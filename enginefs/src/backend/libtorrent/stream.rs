//! File stream implementation for libtorrent backend

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use crate::backend::priorities::{
    BLOCKED_REPLAN_INTERVAL_MS, EngineCacheConfig, MemoryPressure, PlaybackIntent,
    PlaybackPriorityPolicy, PriorityAssignment, PriorityContext,
};

/// Type of seek operation - determines priority behavior
/// Used for DETERMINISTIC seek detection instead of heuristic piece jumps
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum SeekType {
    /// Normal sequential reading (no seek)
    Sequential,
    /// Initial playback (offset=0 on first request)
    InitialPlayback,
    /// User scrubbing to a new position
    UserScrub,
    /// Container metadata read (moov, Cues at end of file)
    ContainerMetadata,
}

type DiskLookupTask = tokio::task::JoinHandle<Option<Arc<Vec<u8>>>>;
type DiskLookup = (i32, DiskLookupTask);

pub(crate) struct LibtorrentFileStream {
    pub(crate) handle: libtorrent_sys::LibtorrentHandle,
    pub(crate) first_piece: i32,
    pub(crate) last_piece: i32,
    pub(crate) piece_length: u64,
    pub(crate) file_offset: u64,
    pub(crate) current_pos: u64,
    pub(crate) is_complete: bool,
    pub(crate) last_priorities_piece: i32,
    pub(crate) cache_config: EngineCacheConfig,
    pub(crate) bitrate: Option<u64>,
    pub(crate) download_speed_ema: f64,
    pub(crate) stream_id: usize,
    /// In-memory piece cache for fast streaming
    pub(crate) piece_cache: Arc<crate::piece_cache::PieceCacheManager>,
    /// Info hash for cache lookups
    pub(crate) info_hash: String,
    /// Currently cached piece data for fast serving
    /// Tuple: (piece_idx, data, file_relative_start)
    pub(crate) cached_piece_data: Option<(i32, Arc<Vec<u8>>, u64)>,
    /// Last piece we triggered prefetch for (to avoid repeated requests)
    pub(crate) last_prefetch_piece: i32,
    /// Last piece for which playback reasserted the next urgent run while serving bytes
    pub(crate) last_served_replan_piece: i32,
    /// Track pieces we've requested via read_piece() API to avoid duplicate requests
    pub(crate) requested_piece_via_api: std::collections::HashMap<i32, Instant>,
    /// Registry of wakers waiting for pieces to finish downloading
    pub(crate) piece_waiter: Arc<crate::piece_waiter::PieceWaiterRegistry>,
    /// Current seek type for DETERMINISTIC priority handling
    pub(crate) seek_type: SeekType,
    /// Playback family used by the centralized priority policy
    pub(crate) playback_intent: PlaybackIntent,
    /// File size for container metadata detection
    pub(crate) file_size: u64,
    /// Stream creation time for startup instrumentation
    pub(crate) created_at: Instant,
    /// Whether we already logged the first successful read
    pub(crate) first_read_logged: bool,
    /// Whether we already logged the first startup wait
    pub(crate) first_wait_logged: bool,
    /// Last time repeated stuck-piece wait state was logged
    pub(crate) last_wait_log: Instant,
    /// Last piece for which direct playback reasserted urgent priority
    pub(crate) last_blocking_piece: i32,
    /// Last time direct playback reasserted urgent priority for a blocked piece
    pub(crate) last_blocking_priority: Instant,
    /// Consecutive pending waits for the current playback piece
    pub(crate) consecutive_waits: u32,
    /// One reusable timer for fallback polling when no piece alert arrives.
    pub(crate) retry_sleep: Option<Pin<Box<tokio::time::Sleep>>>,
    /// At most one optional disk-cache lookup is polled at a time.
    pub(crate) disk_lookup: Option<DiskLookup>,
    /// Avoid repeating a disk miss on every poll for the same piece.
    pub(crate) disk_lookup_miss: Option<i32>,
    pub(crate) playback_permit: super::playback::LibtorrentPlaybackPermit,
}

impl LibtorrentFileStream {
    fn schedule_retry(&mut self, cx: &mut std::task::Context<'_>, delay: Duration) {
        if self.retry_sleep.is_none() {
            self.retry_sleep = Some(Box::pin(tokio::time::sleep(delay)));
        }
        let ready = self
            .retry_sleep
            .as_mut()
            .is_some_and(|sleep| sleep.as_mut().poll(cx).is_ready());
        if ready {
            self.retry_sleep = None;
            cx.waker().wake_by_ref();
        }
    }

    fn priority_replan_intent(&self) -> PlaybackIntent {
        match self.playback_intent {
            PlaybackIntent::ContainerMetadata
            | PlaybackIntent::InternalProbe
            | PlaybackIntent::Background => self.playback_intent,
            intent => intent.seek_for_same_family(),
        }
    }

    fn replace_priority_window<'a>(
        &self, assignments: impl IntoIterator<Item = &'a PriorityAssignment>,
    ) -> usize {
        let non_time_critical = self.playback_permit.is_subordinate()
            || matches!(
                self.playback_intent,
                PlaybackIntent::InternalProbe | PlaybackIntent::Background
            );
        let jitter = (self.stream_id % 10) as i32 * 5;
        let assignments = assignments
            .into_iter()
            .filter(|assignment| {
                assignment.piece_idx >= self.first_piece && assignment.piece_idx <= self.last_piece
            })
            .map(|assignment| {
                let deadline_ms = if non_time_critical {
                    None
                } else if assignment.deadline == 0 || assignment.deadline >= 100_000 {
                    Some(assignment.deadline)
                } else {
                    Some(assignment.deadline.saturating_add(jitter))
                };
                super::playback::LibtorrentPiecePriority {
                    piece: assignment.piece_idx,
                    priority: assignment.piece_priority,
                    deadline_ms,
                }
            })
            .collect::<Vec<_>>();
        let applied = assignments.len();
        self.playback_permit.replace_priority_window(assignments);
        applied
    }

    fn set_priorities(&mut self, pos: u64) {
        // Skip if already complete
        if self.is_complete {
            return;
        }

        if self.piece_length == 0 {
            return;
        }

        // Correct calculation: file_offset is now the TRUE global byte offset of the file start.
        // pos is relative to file start.
        // So (file_offset + pos) is the global byte offset in the torrent.
        let current_piece = ((self.file_offset + pos) / self.piece_length) as i32;

        // Efficient cache check: if we are on the same piece, do nothing
        if current_piece == self.last_priorities_piece {
            return;
        }

        let priority_seek_type = self.seek_type;

        // DETERMINISTIC SEEK HANDLING: Use tracked SeekType instead of piece-jump heuristics
        match priority_seek_type {
            SeekType::Sequential | SeekType::InitialPlayback => {
                // Sequential read or initial playback advances this reader's window.
                tracing::trace!(
                    "set_priorities: {:?} at piece {} - extending window",
                    priority_seek_type,
                    current_piece
                );
            }
            SeekType::ContainerMetadata => {
                // Other readers retain their own head leases while this reader
                // moves to the moov/Cues region.
                tracing::debug!(
                    "set_priorities: ContainerMetadata at piece {} - preserving head priorities",
                    current_piece
                );
            }
            SeekType::UserScrub => {
                tracing::debug!(
                    "set_priorities: UserScrub/probe to piece {} - preserving existing priorities",
                    current_piece
                );
            }
        }

        // After handling the seek, reset to sequential for subsequent reads
        self.seek_type = SeekType::Sequential;

        self.last_priorities_piece = current_piece;

        // Use centralized priorities calculation
        // Calculate dynamic EMA for download speed to avoid priority oscillations
        let status = self.handle.status();
        let _total_pieces = status.num_pieces; // Unused, kept for potential future use
        let current_speed = status.download_rate.max(0) as f64;

        // Alpha of 0.2 means 20% weight to new sample, ~5 samples to converge
        if self.download_speed_ema == 0.0 {
            self.download_speed_ema = current_speed;
        } else {
            self.download_speed_ema = (self.download_speed_ema * 0.8) + (current_speed * 0.2);
        }

        let intent = match priority_seek_type {
            SeekType::ContainerMetadata => PlaybackIntent::ContainerMetadata,
            SeekType::UserScrub => self.priority_replan_intent(),
            SeekType::InitialPlayback | SeekType::Sequential => {
                if self.first_read_logged {
                    self.playback_intent.sequential_after_first_byte()
                } else {
                    self.playback_intent
                }
            }
        };

        let native_memory = libtorrent_sys::memory_storage_stats();
        let memory_pressure = if self.cache_config.size > 0
            && native_memory.total_bytes >= self.cache_config.size.saturating_mul(80) / 100
        {
            MemoryPressure::High
        } else {
            MemoryPressure::Normal
        };

        let decision = PlaybackPriorityPolicy::decide(PriorityContext {
            intent,
            current_piece,
            first_piece: self.first_piece,
            last_piece: self.last_piece,
            piece_length: self.piece_length,
            file_size: self.file_size,
            bitrate_bytes_per_sec: self.bitrate,
            download_rate_bytes_per_sec: self.download_speed_ema as u64,
            peers: status.num_peers as u64,
            cache_size_bytes: self.cache_config.size,
            memory_pressure,
            consecutive_waits: self.consecutive_waits,
            first_byte_sent: self.first_read_logged,
        });

        tracing::debug!(
            intent = ?intent,
            current_piece,
            hot_window = decision.hot_window_pieces,
            warm_window = decision.warm_window_pieces,
            consecutive_waits = self.consecutive_waits,
            speed_mb_s = self.download_speed_ema / 1_000_000.0,
            memory_pressure = ?memory_pressure,
            reason = %decision.reason,
            "priority_decision stream_replan"
        );

        self.replace_priority_window(&decision.assignments);
    }

    fn reassert_serving_window(&mut self, served_piece: i32) {
        if self.is_complete
            || self.playback_permit.is_subordinate()
            || matches!(
                self.playback_intent,
                PlaybackIntent::InternalProbe | PlaybackIntent::Background
            )
            || served_piece < self.first_piece
            || served_piece > self.last_piece
            || self.last_served_replan_piece == served_piece
        {
            return;
        }

        self.last_served_replan_piece = served_piece;

        let next_piece = (served_piece + 1).min(self.last_piece);
        if next_piece < self.first_piece || next_piece > self.last_piece {
            return;
        }

        if matches!(
            self.playback_intent,
            PlaybackIntent::DirectSeek | PlaybackIntent::HlsSeek
        ) {
            self.handle.set_sequential_download(false);
        }

        let status = self.handle.status();
        let native_memory = libtorrent_sys::memory_storage_stats();
        let memory_pressure = if self.cache_config.size > 0
            && native_memory.total_bytes >= self.cache_config.size.saturating_mul(80) / 100
        {
            MemoryPressure::High
        } else {
            MemoryPressure::Normal
        };

        let intent = self.priority_replan_intent();
        let decision = PlaybackPriorityPolicy::decide(PriorityContext {
            intent,
            current_piece: next_piece,
            first_piece: self.first_piece,
            last_piece: self.last_piece,
            piece_length: self.piece_length,
            file_size: self.file_size,
            bitrate_bytes_per_sec: self.bitrate,
            download_rate_bytes_per_sec: status.download_rate.max(0) as u64,
            peers: status.num_peers as u64,
            cache_size_bytes: self.cache_config.size,
            memory_pressure,
            consecutive_waits: self.consecutive_waits,
            first_byte_sent: self.first_read_logged,
        });

        let applied = self.replace_priority_window(&decision.assignments);

        if applied > 0 {
            tracing::info!(
                stream_id = self.stream_id,
                info_hash = %self.info_hash,
                served_piece,
                next_piece,
                applied,
                immediate_pieces = decision.immediate_pieces,
                hot_window = decision.hot_window_pieces,
                peers = status.num_peers,
                speed_mb_s = status.download_rate as f64 / 1_000_000.0,
                reason = %decision.reason,
                "priority_replan_serving_window"
            );
        }
    }

    fn reprioritize_blocking_piece(&mut self, piece: i32) {
        if self.is_complete || piece < self.first_piece || piece > self.last_piece {
            return;
        }

        if matches!(
            self.playback_intent,
            PlaybackIntent::DirectSeek | PlaybackIntent::HlsSeek
        ) {
            self.handle.set_sequential_download(false);
        }

        if self.last_blocking_piece == piece
            && self.last_blocking_priority.elapsed()
                < Duration::from_millis(BLOCKED_REPLAN_INTERVAL_MS)
        {
            return;
        }

        self.last_blocking_piece = piece;
        self.last_blocking_priority = Instant::now();

        let status = self.handle.status();
        let native_memory = libtorrent_sys::memory_storage_stats();
        let memory_pressure = if self.cache_config.size > 0
            && native_memory.total_bytes >= self.cache_config.size.saturating_mul(80) / 100
        {
            MemoryPressure::High
        } else {
            MemoryPressure::Normal
        };

        let intent = self.priority_replan_intent();
        let decision = PlaybackPriorityPolicy::decide(PriorityContext {
            intent,
            current_piece: piece,
            first_piece: self.first_piece,
            last_piece: self.last_piece,
            piece_length: self.piece_length,
            file_size: self.file_size,
            bitrate_bytes_per_sec: self.bitrate,
            download_rate_bytes_per_sec: status.download_rate.max(0) as u64,
            peers: status.num_peers as u64,
            cache_size_bytes: self.cache_config.size,
            memory_pressure,
            consecutive_waits: self.consecutive_waits,
            first_byte_sent: self.first_read_logged,
        });

        self.replace_priority_window(&decision.assignments);

        tracing::debug!(
            intent = ?intent,
            piece,
            stream_id = self.stream_id,
            info_hash = %self.info_hash,
            hot_window = decision.hot_window_pieces,
            warm_window = decision.warm_window_pieces,
            consecutive_waits = self.consecutive_waits,
            speed_mb_s = status.download_rate as f64 / 1_000_000.0,
            reason = %decision.reason,
            "priority_replan_blocked"
        );
    }
}

impl tokio::io::AsyncRead for LibtorrentFileStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.playback_permit.is_cancelled() {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "playback superseded by a newer torrent file",
            )));
        }
        if self.current_pos >= self.file_size || buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let pos = self.current_pos;
        self.set_priorities(pos);

        // Calculate which piece we need
        let piece = self
            .file_offset
            .saturating_add(pos)
            .checked_div(self.piece_length)
            .map_or(-1, |piece| piece as i32);

        if piece >= 0 {
            libtorrent_sys::memory_label_last_unlabeled_storage(&self.info_hash);
        }

        // MEMORY-FIRST READING: Check if we have this piece in our local cache
        if piece >= 0 {
            // Check if we already have the right piece cached locally
            let have_cached = match &self.cached_piece_data {
                Some((cached_piece, _, _)) => *cached_piece == piece,
                None => false,
            };

            if have_cached {
                // Serve from local cache - FASTEST PATH
                let mut served_bytes = false;
                if let Some((_, data, _)) = &self.cached_piece_data {
                    let offset_in_cached = ((self.file_offset + pos) % self.piece_length) as usize;
                    let available = data.len().saturating_sub(offset_in_cached);
                    let remaining_file = self.file_size.saturating_sub(pos) as usize;
                    let to_read = buf.remaining().min(available).min(remaining_file);

                    if to_read > 0 {
                        buf.put_slice(&data[offset_in_cached..offset_in_cached + to_read]);
                        self.current_pos += to_read as u64;
                        self.consecutive_waits = 0;
                        if !self.first_read_logged {
                            self.first_read_logged = true;
                            tracing::debug!(
                                "startup: first direct-stream bytes ready after {:?} (piece={}, source=local-cache)",
                                self.created_at.elapsed(),
                                piece
                            );
                        }

                        if pos.is_multiple_of(1024 * 1024) || pos < 4096 {
                            tracing::debug!(
                                "poll_read: Served {} bytes from MEMORY cache (piece {}, offset_in_cached={})",
                                to_read,
                                piece,
                                offset_in_cached
                            );
                        }
                        served_bytes = true;
                    }
                }
                if served_bytes {
                    self.reassert_serving_window(piece);
                }
                self.requested_piece_via_api.remove(&piece);
                return std::task::Poll::Ready(Ok(()));
            }

            // Try the synchronous Moka hot path. Optional disk persistence is
            // loaded through one stored future below.
            if let Some(piece_data) = self.piece_cache.get_cached_piece(&self.info_hash, piece) {
                self.requested_piece_via_api.remove(&piece);
                self.disk_lookup_miss = None;
                let offset_in_cached = ((self.file_offset + pos) % self.piece_length) as usize;
                self.cached_piece_data = Some((piece, piece_data.clone(), 0));

                let available = piece_data.len().saturating_sub(offset_in_cached);
                let remaining_file = self.file_size.saturating_sub(pos) as usize;
                let to_read = buf.remaining().min(available).min(remaining_file);

                if to_read > 0 {
                    buf.put_slice(&piece_data[offset_in_cached..offset_in_cached + to_read]);
                    self.current_pos += to_read as u64;
                    self.consecutive_waits = 0;
                    if !self.first_read_logged {
                        self.first_read_logged = true;
                        tracing::debug!(
                            "startup: first direct-stream bytes ready after {:?} (piece={}, source=piece-cache)",
                            self.created_at.elapsed(),
                            piece
                        );
                    }

                    tracing::debug!(
                        "poll_read: Served {} bytes from MOKA cache (piece {}, offset_in_cached={})",
                        to_read,
                        piece,
                        offset_in_cached
                    );
                    self.reassert_serving_window(piece);
                }

                // === READ-AHEAD PREFETCH (memory-only) ===
                if piece != self.last_prefetch_piece {
                    self.last_prefetch_piece = piece;
                    let prefetch_cache = self.piece_cache.clone();
                    let prefetch_info_hash = self.info_hash.clone();
                    let prefetch_handle = self.handle.clone();
                    let last_piece = self.last_piece;

                    // ADAPTIVE PREFETCH COUNT
                    let prefetch_count: i32 = if self.download_speed_ema > 10_000_000.0 {
                        8
                    } else if self.download_speed_ema > 5_000_000.0 {
                        5
                    } else if self.download_speed_ema > 1_000_000.0 {
                        3
                    } else {
                        2
                    };

                    // Spawn background prefetch task (memory-only: read directly from storage)
                    tokio::spawn(async move {
                        for i in 1..=prefetch_count {
                            let next_piece = piece + i;
                            if next_piece > last_piece {
                                break;
                            }
                            if prefetch_cache
                                .has_piece(&prefetch_info_hash, next_piece)
                                .await
                            {
                                continue;
                            }
                            if !prefetch_handle.have_piece(next_piece) {
                                continue;
                            }
                            // Read directly from memory storage (no libtorrent read_piece)
                            let data = libtorrent_sys::memory_read_piece_direct(
                                &prefetch_info_hash,
                                next_piece,
                            );
                            if !data.is_empty() {
                                prefetch_cache
                                    .put_piece(&prefetch_info_hash, next_piece, data)
                                    .await;
                                tracing::debug!(
                                    "Read-ahead: cached piece {} directly from memory",
                                    next_piece
                                );
                            }
                        }
                    });
                }

                return std::task::Poll::Ready(Ok(()));
            }

            if self
                .disk_lookup
                .as_ref()
                .is_some_and(|(lookup_piece, _)| *lookup_piece != piece)
                && let Some((_, task)) = self.disk_lookup.take()
            {
                task.abort();
            }
            if self.disk_lookup.is_none() && self.disk_lookup_miss != Some(piece) {
                let cache = self.piece_cache.clone();
                let info_hash = self.info_hash.clone();
                self.disk_lookup = Some((
                    piece,
                    tokio::spawn(async move { cache.get_piece(&info_hash, piece).await }),
                ));
            }
            if let Some((lookup_piece, lookup)) = self.disk_lookup.as_mut() {
                let lookup_piece = *lookup_piece;
                match Pin::new(lookup).poll(cx) {
                    Poll::Ready(Ok(Some(piece_data))) => {
                        self.disk_lookup = None;
                        if lookup_piece == piece {
                            self.disk_lookup_miss = None;
                            self.cached_piece_data = Some((piece, piece_data, 0));
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        }
                    }
                    Poll::Ready(Ok(None)) => {
                        self.disk_lookup = None;
                        self.disk_lookup_miss = Some(lookup_piece);
                    }
                    Poll::Ready(Err(error)) => {
                        self.disk_lookup = None;
                        if !error.is_cancelled() {
                            return Poll::Ready(Err(std::io::Error::other(error)));
                        }
                    }
                    Poll::Pending => {}
                }
            }
        }

        // Not in cache - check if piece is available in libtorrent
        if piece >= 0 && !self.handle.have_piece(piece) {
            self.consecutive_waits = self.consecutive_waits.saturating_add(1);
            self.reprioritize_blocking_piece(piece);

            // NOTIFICATION-BASED WAITING
            self.piece_waiter
                .register(&self.info_hash, piece, self.stream_id, cx.waker().clone());

            self.schedule_retry(cx, Duration::from_millis(50));

            if self.last_wait_log.elapsed() >= std::time::Duration::from_secs(5) {
                self.last_wait_log = Instant::now();
                let status = self.handle.status();
                tracing::info!(
                    "poll_read: still waiting for piece {} (stream_id={}, pos={}, peers={}, speed={:.1}MB/s, paused={}, finished={}, sequential={})",
                    piece,
                    self.stream_id,
                    pos,
                    status.num_peers,
                    status.download_rate as f64 / 1_000_000.0,
                    status.is_paused,
                    status.is_finished,
                    status.sequential_download
                );
            }
            if pos == 0 && !self.first_wait_logged {
                self.first_wait_logged = true;
                let status = self.handle.status();
                tracing::info!(
                    "startup: waiting for first playable piece {} after {:?} (peers={}, paused={}, finished={})",
                    piece,
                    self.created_at.elapsed(),
                    status.num_peers,
                    status.is_paused,
                    status.is_finished
                );
            }

            return std::task::Poll::Pending;
        }

        // Piece is downloaded but not in cache — read directly from memory storage
        if piece >= 0 && !self.requested_piece_via_api.contains_key(&piece) {
            let piece_data = libtorrent_sys::memory_read_piece_direct(&self.info_hash, piece);
            if !piece_data.is_empty() {
                // Got data directly! Cache it and serve immediately on next poll.
                self.requested_piece_via_api.insert(piece, Instant::now());
                tracing::info!(
                    "poll_read: Direct read piece {} from memory storage ({} bytes)",
                    piece,
                    piece_data.len()
                );
                self.reassert_serving_window(piece);
                self.piece_cache
                    .put_piece_now(&self.info_hash, piece, piece_data);
                self.disk_lookup_miss = None;
                self.piece_waiter
                    .notify_piece_finished(&self.info_hash, piece);
            } else {
                self.consecutive_waits = self.consecutive_waits.saturating_add(1);
                tracing::debug!(
                    "poll_read: piece {} downloaded but not yet in memory storage",
                    piece,
                );
                self.piece_waiter.register(
                    &self.info_hash,
                    piece,
                    self.stream_id,
                    cx.waker().clone(),
                );
            }

            self.schedule_retry(cx, Duration::from_millis(10));
            return std::task::Poll::Pending;
        }

        // Piece was requested, waiting for cache to be populated
        if piece >= 0 && self.requested_piece_via_api.contains_key(&piece) {
            self.consecutive_waits = self.consecutive_waits.saturating_add(1);
            let should_rerequest = self
                .requested_piece_via_api
                .get(&piece)
                .map(|requested_at| requested_at.elapsed() > std::time::Duration::from_millis(250))
                .unwrap_or(false);
            if should_rerequest {
                tracing::warn!(
                    "poll_read: piece {} still missing from cache after 250ms, re-reading from memory",
                    piece
                );
                let piece_data = libtorrent_sys::memory_read_piece_direct(&self.info_hash, piece);
                if !piece_data.is_empty() {
                    self.piece_cache
                        .put_piece_now(&self.info_hash, piece, piece_data);
                    self.piece_waiter
                        .notify_piece_finished(&self.info_hash, piece);
                }
                self.requested_piece_via_api.insert(piece, Instant::now());
            }
            self.piece_waiter
                .register(&self.info_hash, piece, self.stream_id, cx.waker().clone());
            tracing::trace!(
                "poll_read: MEMORY-ONLY waiting for piece {} in cache (have_piece={})",
                piece,
                self.handle.have_piece(piece)
            );
            self.schedule_retry(cx, Duration::from_millis(15));
            return std::task::Poll::Pending;
        }

        // Should not reach here
        tracing::error!("poll_read: Unexpected state - piece={}, pos={}", piece, pos);
        std::task::Poll::Ready(Err(std::io::Error::other(
            "Memory-only streaming: unexpected state in poll_read",
        )))
    }
}

impl Drop for LibtorrentFileStream {
    fn drop(&mut self) {
        if let Some((_, task)) = self.disk_lookup.take() {
            task.abort();
        }
        self.piece_waiter.unregister_stream(self.stream_id);
    }
}

impl tokio::io::AsyncSeek for LibtorrentFileStream {
    fn start_seek(
        mut self: std::pin::Pin<&mut Self>, position: std::io::SeekFrom,
    ) -> std::io::Result<()> {
        // Calculate target position
        let new_pos = match position {
            std::io::SeekFrom::Start(pos) => pos,
            std::io::SeekFrom::Current(delta) => (self.current_pos as i64 + delta).max(0) as u64,
            std::io::SeekFrom::End(delta) => (self.file_size as i64 + delta).max(0) as u64,
        };

        // DETERMINISTIC: Preserve route-level metadata classification. Near-tail
        // playback ranges can be real playback, so do not reclassify them here.
        self.seek_type = if matches!(self.playback_intent, PlaybackIntent::ContainerMetadata) {
            SeekType::ContainerMetadata
        } else {
            SeekType::UserScrub
        };

        tracing::debug!(
            "start_seek: {} -> {} ({:?})",
            self.current_pos,
            new_pos,
            self.seek_type
        );

        // Memory-only mode: just update position, no file handle to seek
        self.current_pos = new_pos;
        // Invalidate local cached piece data since position changed
        self.cached_piece_data = None;
        if let Some((_, task)) = self.disk_lookup.take() {
            task.abort();
        }
        self.disk_lookup_miss = None;
        Ok(())
    }

    fn poll_complete(
        mut self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<u64>> {
        // Memory-only mode: position is already set in start_seek
        let pos = self.current_pos;

        let piece_idx = self
            .file_offset
            .saturating_add(pos)
            .checked_div(self.piece_length)
            .map_or(-1, |piece| piece as i32);

        if piece_idx != self.last_priorities_piece {
            self.set_priorities(pos);
        }

        std::task::Poll::Ready(Ok(pos))
    }
}
