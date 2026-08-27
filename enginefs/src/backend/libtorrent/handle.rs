//! Torrent handle implementation for libtorrent backend

use anyhow::{Result, anyhow};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::backend::{
    BackendFileInfo, EngineStats, FileStreamTrait, PieceReadiness, TorrentFilePriorityPlan,
    TorrentHandle as TorrentHandleTrait,
    priorities::{MemoryPressure, PlaybackIntent, PlaybackPriorityPolicy, PriorityContext},
};
use libtorrent_sys::LibtorrentSession;

use super::LibtorrentStorageMode;
use super::disk_stream::LibtorrentDiskFileStream;
use super::helpers::{default_stats, make_engine_stats};
use super::playback::{LibtorrentPlaybackCoordinator, LibtorrentPlaybackStart};
use super::stream::LibtorrentFileStream;

const DISK_READINESS_BUFFER_BYTES: u64 = 8 * 1024 * 1024;
const DISK_READINESS_MAX_PIECES: u32 = 4;
const DISK_READINESS_REASSERT_MS: u64 = 250;

/// Handle to a torrent managed by libtorrent
#[derive(Clone)]
pub struct LibtorrentTorrentHandle {
    pub(crate) session: Arc<RwLock<LibtorrentSession>>,
    pub(crate) info_hash: String,
    pub(crate) save_path: PathBuf,
    pub(crate) config: crate::backend::BackendConfig,
    pub(crate) storage_mode: LibtorrentStorageMode,
    pub(crate) stream_counter: Arc<std::sync::atomic::AtomicUsize>,
    /// In-memory piece cache for fast streaming
    pub(crate) piece_cache: Arc<crate::piece_cache::PieceCacheManager>,
    /// Registry of wakers waiting for pieces to finish downloading
    pub(crate) piece_waiter: Arc<crate::piece_waiter::PieceWaiterRegistry>,
    /// Pinned metadata-critical (Cues/moov) pieces that out-rank the playback head
    pub(crate) metadata_pins: Arc<crate::metadata_pins::MetadataPinRegistry>,
    pub(crate) playback: Arc<LibtorrentPlaybackCoordinator>,
}

impl LibtorrentTorrentHandle {
    fn label_memory_storage(&self) {
        if matches!(self.storage_mode, LibtorrentStorageMode::MemoryOnly) {
            libtorrent_sys::memory_label_last_unlabeled_storage(&self.info_hash);
        }
    }

    fn disk_readiness_target_pieces(
        intent: PlaybackIntent, piece: i32, last_piece: i32, piece_length: u64,
        storage_mode: LibtorrentStorageMode,
    ) -> u32 {
        let remaining = (last_piece.saturating_sub(piece) + 1).max(1) as u32;
        if !matches!(storage_mode, LibtorrentStorageMode::DiskBacked)
            || piece_length == 0
            || !matches!(
                intent,
                PlaybackIntent::DirectInitial
                    | PlaybackIntent::DirectSeek
                    | PlaybackIntent::DirectSequential
                    | PlaybackIntent::HlsInitial
                    | PlaybackIntent::HlsSeek
                    | PlaybackIntent::HlsSequential
            )
        {
            return 1.min(remaining);
        }

        let pieces_for_buffer = DISK_READINESS_BUFFER_BYTES
            .saturating_add(piece_length.saturating_sub(1))
            / piece_length;
        (pieces_for_buffer as u32)
            .clamp(1, DISK_READINESS_MAX_PIECES)
            .min(remaining)
    }
}

#[async_trait::async_trait]
impl TorrentHandleTrait for LibtorrentTorrentHandle {
    fn info_hash(&self) -> String {
        self.info_hash.clone()
    }

    fn name(&self) -> Option<String> {
        // We need to query the session to get the name
        // This is a sync operation wrapped in a blocking task
        let session = self.session.blocking_read();
        match session.find_torrent(&self.info_hash) {
            Ok(handle) => {
                let name = handle.name();
                if name.is_empty() { None } else { Some(name) }
            }
            Err(_) => None,
        }
    }

    async fn stats(&self) -> EngineStats {
        let session = self.session.read().await;

        let handle = match session.find_torrent(&self.info_hash) {
            Ok(h) => h,
            Err(_) => return default_stats(&self.info_hash),
        };

        let status = handle.status();
        let mut stats = make_engine_stats(&status);
        let piece_length = handle.piece_length() as u64;

        // Populate files from the handle
        let files = handle.files();
        let piece_presence = if piece_length > 0 {
            handle.piece_presence(0, handle.num_pieces().saturating_sub(1))
        } else {
            Vec::new()
        };
        let mut current_offset = 0u64;

        stats.files = files
            .iter()
            .map(|f| {
                let file_offset = current_offset;
                current_offset += f.size as u64;

                // Calculate downloaded based on pieces we have (more accurate for streaming)
                // file_progress() returns 0 for files with priority 0 or when streaming
                let downloaded = if f.downloaded > 0 {
                    f.downloaded as u64
                } else if piece_length > 0 {
                    // Count pieces we have in this file's range
                    let piece_bytes = (f.first_piece..=f.last_piece)
                        .filter(|piece| {
                            usize::try_from(*piece)
                                .ok()
                                .and_then(|index| piece_presence.get(index))
                                .is_some_and(|present| *present != 0)
                        })
                        .count() as u64
                        * piece_length;
                    // Cap at file size (last piece may be partial)
                    piece_bytes.min(f.size as u64)
                } else {
                    0
                };

                crate::backend::StatsFile {
                    name: f.path.to_string(),
                    path: f.path.to_string(),
                    length: f.size as u64,
                    offset: file_offset,
                    downloaded,
                    // Use C++ calculated progress which comes from file_progress()
                    progress: f.progress as f64,
                }
            })
            .collect();

        stats
    }

    async fn add_trackers(&self, trackers: Vec<String>) -> Result<()> {
        let session = self.session.read().await;
        let mut handle = session
            .find_torrent(&self.info_hash)
            .map_err(|e| anyhow!("Torrent not found: {}", e))?;
        self.label_memory_storage();

        // Add trackers with tier based on position (faster trackers first get lower tier = higher priority)
        for (idx, tracker) in trackers.iter().enumerate() {
            handle.add_tracker(tracker, idx as i32);
        }
        Ok(())
    }

    async fn is_finished(&self) -> bool {
        let session = self.session.read().await;
        match session.find_torrent(&self.info_hash) {
            Ok(handle) => handle.status().is_finished,
            Err(_) => false,
        }
    }

    fn manages_playback_lifecycle(&self) -> bool {
        true
    }

    async fn refresh_hls_activity(&self, file_idx: usize, source: &'static str) -> Result<()> {
        self.playback.refresh_hls(self, file_idx, source).await?;
        Ok(())
    }

    async fn end_hls_activity(&self, file_idx: usize, reason: &'static str) -> Result<()> {
        self.playback
            .end_hls(&self.info_hash, file_idx, reason)
            .await
    }

    async fn is_file_complete(&self, file_idx: usize) -> bool {
        self.playback
            .is_file_complete(&self.info_hash, file_idx)
            .await
    }

    async fn resume_torrent(&self) -> Result<()> {
        tracing::trace!(
            info_hash = %self.info_hash,
            "Ignoring legacy resume request; playback coordinator owns resume"
        );
        Ok(())
    }

    async fn pause_torrent(&self) -> Result<()> {
        tracing::trace!(
            info_hash = %self.info_hash,
            "Ignoring legacy pause request; playback coordinator owns pause"
        );
        Ok(())
    }

    async fn set_upload_throttled(&self, _throttled: bool) -> Result<()> {
        Ok(())
    }

    async fn reconcile_file_priorities(&self, _plan: TorrentFilePriorityPlan) -> Result<()> {
        // The libtorrent coordinator is the sole owner of acknowledged bulk
        // file priorities and hot-piece ordering. Shared lifecycle calls are
        // intentionally ignored to prevent an unacknowledged second writer.
        Ok(())
    }

    async fn get_file_reader(
        &self, file_idx: usize, start_offset: u64, priority: u8, bitrate: Option<u64>,
        intent: PlaybackIntent,
    ) -> Result<Box<dyn FileStreamTrait>> {
        tracing::debug!("get_file_reader: starting for file {}", file_idx);
        let stream_id = self
            .stream_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let playback_permit = self
            .playback
            .start_playback(
                self,
                LibtorrentPlaybackStart {
                    file_idx,
                    stream_id,
                    start_offset,
                    priority,
                    intent,
                    bitrate_bytes_per_sec: bitrate,
                    source: if priority == 255 {
                        "internal-probe-reader"
                    } else {
                        "file-reader"
                    },
                },
            )
            .await?;
        let layout = self.playback.metadata_layout(&self.info_hash).await?;
        let file_info = layout
            .files
            .get(file_idx)
            .cloned()
            .ok_or_else(|| anyhow!("File index {} out of range", file_idx))?;
        let is_complete = self
            .playback
            .is_file_complete(&self.info_hash, file_idx)
            .await;
        let session = self.session.read().await;
        let handle = session
            .find_torrent(&self.info_hash)
            .map_err(|e| anyhow!("Torrent not found: {}", e))?;
        self.label_memory_storage();

        let first_piece = file_info.first_piece;
        let last_piece = file_info.last_piece;
        let piece_length = layout.piece_length;
        let global_file_offset = file_info.offset as u64;

        tracing::debug!(
            "get_file_reader: file {} is_complete={}",
            file_idx,
            is_complete
        );

        // File size is needed for both prioritization and seek type detection
        let file_size = file_info.size as u64;

        // =========================================================================
        // SEEK TYPE DETECTION with Priority Bands
        // =========================================================================
        // | Band     | Deadline   | Use Case                                     |
        // |----------|------------|----------------------------------------------|
        // | URGENT   | 0-200ms    | Initial playback (first request, offset=0)  |
        // | CRITICAL | 300-500ms  | User-initiated seeks (scrubbing)            |
        // | NORMAL   | 1000-1200ms| Prefetch/buffer expansion                   |
        // | DEFERRED | 2000-3000ms| Container metadata (moov/Cues at end)       |
        // =========================================================================

        #[derive(Debug, Clone, Copy)]
        enum SeekType {
            InitialPlayback,   // offset=0, first request
            ContainerMetadata, // near end of file, seeking for moov/Cues
            UserScrub,         // user is seeking mid-video
        }

        let seek_type = {
            if start_offset == 0 {
                SeekType::InitialPlayback
            } else if matches!(intent, PlaybackIntent::ContainerMetadata) {
                SeekType::ContainerMetadata
            } else {
                SeekType::UserScrub
            }
        };

        let playback_intent = if priority == 255 {
            PlaybackIntent::InternalProbe
        } else {
            match intent {
                PlaybackIntent::DownloadFull | PlaybackIntent::DownloadRange => intent,
                PlaybackIntent::ContainerMetadata => PlaybackIntent::ContainerMetadata,
                PlaybackIntent::Background => PlaybackIntent::Background,
                PlaybackIntent::InternalProbe => PlaybackIntent::InternalProbe,
                PlaybackIntent::HlsInitial
                | PlaybackIntent::HlsSequential
                | PlaybackIntent::HlsSeek => {
                    if start_offset == 0 {
                        PlaybackIntent::HlsInitial
                    } else {
                        PlaybackIntent::HlsSeek
                    }
                }
                _ => {
                    if start_offset == 0 {
                        PlaybackIntent::DirectInitial
                    } else {
                        PlaybackIntent::DirectSeek
                    }
                }
            }
        };

        // The coordinator has already acknowledged the one bulk file-priority
        // update and applied the initial hot window. Polling streams only move
        // that window as bytes are consumed.
        let actual_start_piece: i32 = ((global_file_offset + start_offset) / piece_length) as i32;
        tracing::debug!(
            info_hash = %self.info_hash,
            file_idx,
            priority,
            is_complete,
            actual_start_piece,
            "libtorrent reader opened after coordinated activation"
        );

        if matches!(self.storage_mode, LibtorrentStorageMode::DiskBacked) {
            let file_path = self.save_path.join(&file_info.path);
            let file = match tokio::fs::OpenOptions::new()
                .read(true)
                .open(&file_path)
                .await
            {
                Ok(file) => Some(file),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(error.into()),
            };
            return Ok(Box::new(LibtorrentDiskFileStream::new(
                handle.clone(),
                self.info_hash.clone(),
                file_path,
                file_info.path.clone(),
                first_piece,
                last_piece,
                piece_length,
                global_file_offset,
                file_size,
                file_idx,
                stream_id,
                playback_intent,
                bitrate,
                self.piece_waiter.clone(),
                self.metadata_pins.clone(),
                file,
                self.playback.alerts().clone(),
                playback_permit,
            )));
        }

        // Memory-only mode: no disk files to open or mmap

        // Map local SeekType to stream's SeekType for deterministic handling
        let initial_seek_type = match seek_type {
            SeekType::InitialPlayback => super::stream::SeekType::InitialPlayback,
            SeekType::ContainerMetadata => super::stream::SeekType::ContainerMetadata,
            SeekType::UserScrub => super::stream::SeekType::UserScrub,
        };

        Ok(Box::new(LibtorrentFileStream {
            handle: handle.clone(),
            first_piece,
            last_piece,
            piece_length,
            file_offset: global_file_offset,
            current_pos: 0,
            is_complete,
            last_priorities_piece: if !is_complete { actual_start_piece } else { -1 },
            cache_config: self.config.cache,
            bitrate,
            download_speed_ema: 0.0,
            stream_id,
            piece_cache: self.piece_cache.clone(),
            info_hash: self.info_hash.clone(),
            cached_piece_data: None,
            last_prefetch_piece: -1,
            last_served_replan_piece: -1,
            requested_piece_via_api: std::collections::HashMap::new(),
            piece_waiter: self.piece_waiter.clone(),
            seek_type: initial_seek_type,
            playback_intent,
            file_size,
            created_at: std::time::Instant::now(),
            first_read_logged: false,
            first_wait_logged: false,
            last_wait_log: std::time::Instant::now()
                .checked_sub(std::time::Duration::from_secs(5))
                .unwrap_or_else(std::time::Instant::now),
            last_blocking_piece: -1,
            last_blocking_priority: std::time::Instant::now()
                .checked_sub(std::time::Duration::from_secs(1))
                .unwrap_or_else(std::time::Instant::now),
            consecutive_waits: 0,
            retry_sleep: None,
            disk_lookup: None,
            disk_lookup_miss: None,
            playback_permit,
        }))
    }

    async fn get_files(&self) -> Vec<BackendFileInfo> {
        match self.playback.metadata_layout(&self.info_hash).await {
            Ok(layout) => layout
                .files
                .iter()
                .map(|file| BackendFileInfo {
                    name: file.path.clone(),
                    length: file.size.max(0) as u64,
                })
                .collect(),
            Err(error) => {
                tracing::warn!(
                    info_hash = %self.info_hash,
                    %error,
                    "Failed to resolve torrent file metadata"
                );
                Vec::new()
            }
        }
    }

    async fn get_file_path(&self, file_idx: usize) -> Option<String> {
        let session = self.session.read().await;
        if let Ok(handle) = session.find_torrent(&self.info_hash) {
            let files = handle.files();
            if let Some(file_info) = files.get(file_idx) {
                // Construct full path: save_path + file.path
                let full_path = self.save_path.join(&file_info.path);
                if full_path.is_file() {
                    return Some(full_path.to_string_lossy().to_string());
                }
                tracing::debug!(
                    "get_file_path: No on-disk file available for {} (memory-only mode)",
                    full_path.display()
                );
            }
        }
        None
    }

    async fn prepare_file_for_streaming(&self, file_idx: usize) -> anyhow::Result<()> {
        let layout = self.playback.metadata_layout(&self.info_hash).await?;
        let file = layout
            .files
            .get(file_idx)
            .ok_or_else(|| anyhow!("File index {} out of range", file_idx))?;
        tracing::debug!(
            info_hash = %self.info_hash,
            file_idx,
            first_piece = file.first_piece,
            last_piece = file.last_piece,
            "prepare_file_for_streaming is metadata-only; activation belongs to the coordinator"
        );
        Ok(())
    }

    async fn keep_file_downloading(&self, file_idx: usize) -> anyhow::Result<()> {
        tracing::debug!(
            "keep_file_downloading: coordinator already owns file {} in {}",
            file_idx,
            self.info_hash
        );

        Ok(())
    }

    async fn clear_file_streaming(&self, file_idx: usize) -> anyhow::Result<()> {
        tracing::info!(
            "clear_file_streaming: deferred file {} cleanup to coordinator for {}",
            file_idx,
            self.info_hash
        );

        Ok(())
    }

    async fn wait_for_piece_ready(
        &self, file_idx: usize, offset: u64, timeout: std::time::Duration, intent: PlaybackIntent,
    ) -> anyhow::Result<PieceReadiness> {
        let start = std::time::Instant::now();
        let mut last_peers = 0u64;
        let mut last_rate = 0u64;
        let mut last_readiness_log = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_secs(5))
            .unwrap_or_else(std::time::Instant::now);

        let (piece, first_piece, last_piece, piece_length, file_size) = {
            let session = self.session.read().await;
            let mut handle = session
                .find_torrent(&self.info_hash)
                .map_err(|e| anyhow!("Torrent not found: {}", e))?;
            self.label_memory_storage();
            let files = handle.files();
            let file_info = files
                .get(file_idx)
                .ok_or_else(|| anyhow!("File index {} out of range", file_idx))?;
            let piece_length = handle.piece_length() as u64;
            if piece_length == 0 {
                return Ok(PieceReadiness {
                    ready: false,
                    piece: -1,
                    ready_pieces: 0,
                    target_pieces: 0,
                    elapsed_ms: start.elapsed().as_millis() as u64,
                    peers: 0,
                    download_rate: 0,
                    reason: "zero-piece-length".to_string(),
                });
            }
            let piece = ((file_info.offset as u64 + offset) / piece_length) as i32;
            if piece < file_info.first_piece || piece > file_info.last_piece {
                return Ok(PieceReadiness {
                    ready: false,
                    piece,
                    ready_pieces: 0,
                    target_pieces: 1,
                    elapsed_ms: start.elapsed().as_millis() as u64,
                    peers: 0,
                    download_rate: 0,
                    reason: "piece-out-of-file-range".to_string(),
                });
            }
            let status = handle.status();
            let native_memory = libtorrent_sys::memory_storage_stats();
            let memory_pressure = if self.config.cache.size > 0
                && native_memory.total_bytes >= self.config.cache.size.saturating_mul(80) / 100
            {
                MemoryPressure::High
            } else {
                MemoryPressure::Normal
            };
            let decision = PlaybackPriorityPolicy::decide(PriorityContext {
                intent,
                current_piece: piece,
                first_piece: file_info.first_piece,
                last_piece: file_info.last_piece,
                piece_length,
                file_size: file_info.size as u64,
                bitrate_bytes_per_sec: None,
                download_rate_bytes_per_sec: status.download_rate as u64,
                peers: status.num_peers as u64,
                cache_size_bytes: self.config.cache.size,
                memory_pressure,
                consecutive_waits: 0,
                first_byte_sent: false,
            });
            handle.set_piece_priorities(
                decision
                    .assignments
                    .iter()
                    .map(|assignment| (assignment.piece_idx, assignment.piece_priority)),
            );
            for assignment in &decision.assignments {
                handle.set_piece_deadline(assignment.piece_idx, assignment.deadline);
            }
            tracing::info!(
                intent = ?intent,
                piece,
                hot_window = decision.hot_window_pieces,
                warm_window = decision.warm_window_pieces,
                peers = status.num_peers,
                download_rate = status.download_rate,
                reason = %decision.reason,
                "priority_seek_readiness_begin"
            );
            (
                piece,
                file_info.first_piece,
                file_info.last_piece,
                piece_length,
                file_info.size as u64,
            )
        };

        let target_pieces = Self::disk_readiness_target_pieces(
            intent,
            piece,
            last_piece,
            piece_length,
            self.storage_mode,
        );
        let mut best_ready_pieces = 0u32;
        // Only re-run the priority policy when the swarm picture changes; doing
        // it every tick is wasted work, especially under the parallel-probe
        // burst a player fires at stream start.
        let mut last_decide_peers = u64::MAX;
        let mut last_decide_rate_bucket = u64::MAX;
        // Poll tightly at first so a piece that verifies mid-wait is served
        // almost immediately, then back off to keep idle cost low.
        let mut poll_interval_ms = 5u64;
        let mut last_cluster_reassert = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_millis(DISK_READINESS_REASSERT_MS))
            .unwrap_or_else(std::time::Instant::now);

        while start.elapsed() < timeout {
            // Memory-storage pieces that are downloaded but not yet copied into
            // the read cache. Collected under the session lock and serviced
            // afterwards, since the cache fills are async and must not hold it.
            let mut memory_pieces_to_cache: Vec<i32> = Vec::new();

            // Everything that needs the session is done under a SINGLE read
            // guard per iteration: status, have-piece checks, and piece-window
            // re-prioritization. Previously each of these took the lock
            // separately, multiplying contention across concurrent streams.
            let (ready_pieces_disk, peers, rate) = {
                let session = self.session.read().await;
                let mut handle = session
                    .find_torrent(&self.info_hash)
                    .map_err(|e| anyhow!("Torrent not found: {}", e))?;
                let status = handle.status();
                let peers = status.num_peers as u64;
                let rate = status.download_rate as u64;

                // Count contiguous ready pieces from the target. Disk-backed
                // storage only needs `have_piece`; memory storage also needs the
                // piece copied into the read cache (serviced after unlock).
                let cluster_end = last_piece.min(piece + target_pieces as i32 - 1);
                let cluster_presence = handle.piece_presence(piece, cluster_end);
                let mut ready = 0u32;
                for (offset, present) in cluster_presence.iter().copied().enumerate() {
                    if present == 0 {
                        break;
                    }
                    let ready_piece = piece.saturating_add(offset as i32);
                    if matches!(self.storage_mode, LibtorrentStorageMode::DiskBacked) {
                        ready += 1;
                    } else {
                        memory_pieces_to_cache.push(ready_piece);
                    }
                }

                let disk_satisfied = matches!(self.storage_mode, LibtorrentStorageMode::DiskBacked)
                    && ready >= target_pieces;
                if !disk_satisfied
                    && matches!(self.storage_mode, LibtorrentStorageMode::DiskBacked)
                    && last_cluster_reassert.elapsed()
                        >= std::time::Duration::from_millis(DISK_READINESS_REASSERT_MS)
                {
                    last_cluster_reassert = std::time::Instant::now();
                    let assignments = cluster_presence
                        .iter()
                        .copied()
                        .enumerate()
                        .filter(|(_, present)| *present == 0)
                        .map(|(offset, _)| {
                            let target = piece.saturating_add(offset as i32);
                            (target, target.saturating_sub(piece) * 10)
                        })
                        .collect::<Vec<_>>();
                    handle.set_piece_priorities(assignments.iter().map(|(piece, _)| (*piece, 7)));
                    for (piece, deadline) in assignments {
                        handle.set_piece_deadline(piece, deadline);
                    }
                }
                let rate_bucket = rate / (64 * 1024);
                if !disk_satisfied
                    && (peers != last_decide_peers || rate_bucket != last_decide_rate_bucket)
                {
                    last_decide_peers = peers;
                    last_decide_rate_bucket = rate_bucket;
                    let decision = PlaybackPriorityPolicy::decide(PriorityContext {
                        intent,
                        current_piece: piece,
                        first_piece,
                        last_piece,
                        piece_length,
                        file_size,
                        bitrate_bytes_per_sec: None,
                        download_rate_bytes_per_sec: rate,
                        peers,
                        cache_size_bytes: self.config.cache.size,
                        memory_pressure: MemoryPressure::Normal,
                        consecutive_waits: 1,
                        first_byte_sent: false,
                    });
                    handle.set_piece_priorities(
                        decision
                            .assignments
                            .iter()
                            .map(|assignment| (assignment.piece_idx, assignment.piece_priority)),
                    );
                    for assignment in &decision.assignments {
                        handle.set_piece_deadline(assignment.piece_idx, assignment.deadline);
                    }
                }

                if last_readiness_log.elapsed() >= std::time::Duration::from_secs(5) {
                    last_readiness_log = std::time::Instant::now();
                    let verified_bytes_estimate = u64::try_from(status.total_wanted_done)
                        .unwrap_or(0)
                        .min(file_size);
                    let verified_piece_count = if piece_length == 0 {
                        0
                    } else {
                        verified_bytes_estimate.div_ceil(piece_length) as usize
                    };
                    let request_offset_percent = if file_size > 0 {
                        (offset.min(file_size) as f64 / file_size as f64) * 100.0
                    } else {
                        0.0
                    };
                    tracing::info!(
                        intent = ?intent,
                        storage_mode = ?self.storage_mode,
                        piece,
                        elapsed_ms = start.elapsed().as_millis() as u64,
                        peers = status.num_peers,
                        download_rate = status.download_rate,
                        paused = status.is_paused,
                        auto_managed = status.is_auto_managed,
                        state = status.state,
                        finished = status.is_finished,
                        error = %status.error,
                        verified_piece_count,
                        ready_pieces = ready,
                        target_pieces,
                        request_offset_percent,
                        "direct stream readiness waiting"
                    );
                }

                (ready, peers, rate)
            };
            last_peers = peers;
            last_rate = rate;

            // Service memory-storage cache fills outside the session lock.
            let ready_pieces = if matches!(self.storage_mode, LibtorrentStorageMode::DiskBacked) {
                ready_pieces_disk
            } else {
                let mut ready = 0u32;
                for ready_piece in piece..=last_piece.min(piece + target_pieces as i32 - 1) {
                    if self
                        .piece_cache
                        .has_piece(&self.info_hash, ready_piece)
                        .await
                    {
                        ready += 1;
                        continue;
                    }
                    if memory_pieces_to_cache.contains(&ready_piece) {
                        let piece_data =
                            libtorrent_sys::memory_read_piece_direct(&self.info_hash, ready_piece);
                        if piece_data.is_empty() {
                            break;
                        }
                        self.piece_cache
                            .put_piece(&self.info_hash, ready_piece, piece_data)
                            .await;
                        self.piece_waiter
                            .notify_piece_finished(&self.info_hash, ready_piece);
                        ready += 1;
                    } else {
                        break;
                    }
                }
                ready
            };
            best_ready_pieces = best_ready_pieces.max(ready_pieces);

            if ready_pieces >= target_pieces {
                return Ok(PieceReadiness {
                    ready: true,
                    piece,
                    ready_pieces,
                    target_pieces,
                    elapsed_ms: start.elapsed().as_millis() as u64,
                    peers,
                    download_rate: rate,
                    reason: if target_pieces > 1 {
                        "buffer-ready".to_string()
                    } else {
                        "piece-ready".to_string()
                    },
                });
            }

            tokio::time::sleep(std::time::Duration::from_millis(poll_interval_ms)).await;
            poll_interval_ms = (poll_interval_ms * 2).min(25);
        }

        Ok(PieceReadiness {
            ready: best_ready_pieces > 0,
            piece,
            ready_pieces: best_ready_pieces,
            target_pieces,
            elapsed_ms: start.elapsed().as_millis() as u64,
            peers: last_peers,
            download_rate: last_rate,
            reason: if best_ready_pieces > 0 {
                "partial-buffer-timeout".to_string()
            } else {
                "timeout".to_string()
            },
        })
    }
}
