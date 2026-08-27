use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use crate::backend::priorities::{
    BLOCKED_REPLAN_INTERVAL_MS, PlaybackIntent, disk_backed_forward_window_pieces_for,
    playback_deadline_step_ms,
};
use crate::metadata_pins::MetadataPinRegistry;
use crate::piece_waiter::PieceWaiterRegistry;

const INITIAL_FIRST_BYTE_WINDOW_PIECES: i32 = 3;
type PieceReadTask = tokio::task::JoinHandle<std::io::Result<Arc<Vec<u8>>>>;

fn forward_window_end(piece: i32, last_piece: i32, piece_count: i32) -> i32 {
    last_piece.min(piece.saturating_add(piece_count.saturating_sub(1)))
}

fn broker_slice_bounds(
    piece: i32, piece_length: u64, file_offset: u64, current_pos: u64, file_size: u64,
    piece_bytes: usize, requested_bytes: usize,
) -> Option<(usize, usize)> {
    let piece_start = (piece.max(0) as u64).saturating_mul(piece_length);
    let global_position = file_offset.saturating_add(current_pos);
    let offset = usize::try_from(global_position.saturating_sub(piece_start)).ok()?;
    let available = piece_bytes.saturating_sub(offset);
    let remaining_file =
        usize::try_from(file_size.saturating_sub(current_pos)).unwrap_or(usize::MAX);
    let len = requested_bytes.min(available).min(remaining_file);
    (len != 0).then_some((offset, offset + len))
}

fn piece_is_missing(presence: &[u8], first: i32, piece: i32) -> bool {
    usize::try_from(piece.saturating_sub(first))
        .ok()
        .and_then(|index| presence.get(index))
        .is_none_or(|present| *present == 0)
}

pub(crate) struct LibtorrentDiskFileStream {
    handle: libtorrent_sys::LibtorrentHandle,
    info_hash: String,
    file_path: PathBuf,
    display_path: String,
    first_piece: i32,
    last_piece: i32,
    piece_length: u64,
    file_offset: u64,
    file_size: u64,
    file_idx: usize,
    current_pos: u64,
    stream_id: usize,
    playback_intent: PlaybackIntent,
    bitrate_bytes_per_sec: Option<u64>,
    piece_waiter: Arc<PieceWaiterRegistry>,
    metadata_pins: Arc<MetadataPinRegistry>,
    created_at: Instant,
    first_read_logged: bool,
    last_wait_log: Instant,
    last_prioritized_piece: i32,
    consecutive_waits: u32,
    last_blocked_replan: Instant,
    file: Option<tokio::fs::File>,
    file_cursor: u64,
    seek_pending: bool,
    open_file: Option<tokio::task::JoinHandle<std::io::Result<tokio::fs::File>>>,
    scratch: Vec<u8>,
    verified_piece: Option<i32>,
    broker_piece: Option<(i32, Arc<Vec<u8>>)>,
    piece_read: Option<(i32, PieceReadTask)>,
    piece_read_retry_at: Option<(i32, Instant)>,
    retry_sleep: Option<Pin<Box<tokio::time::Sleep>>>,
    alert_hub: Arc<super::alerts::LibtorrentAlertHub>,
    playback_permit: super::playback::LibtorrentPlaybackPermit,
}

impl LibtorrentDiskFileStream {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        handle: libtorrent_sys::LibtorrentHandle, info_hash: String, file_path: PathBuf,
        display_path: String, first_piece: i32, last_piece: i32, piece_length: u64,
        file_offset: u64, file_size: u64, file_idx: usize, stream_id: usize,
        playback_intent: PlaybackIntent, bitrate_bytes_per_sec: Option<u64>,
        piece_waiter: Arc<PieceWaiterRegistry>, metadata_pins: Arc<MetadataPinRegistry>,
        file: Option<tokio::fs::File>, alert_hub: Arc<super::alerts::LibtorrentAlertHub>,
        playback_permit: super::playback::LibtorrentPlaybackPermit,
    ) -> Self {
        Self {
            handle,
            info_hash,
            file_path,
            display_path,
            first_piece,
            last_piece,
            piece_length,
            file_offset,
            file_size,
            file_idx,
            current_pos: 0,
            stream_id,
            playback_intent,
            bitrate_bytes_per_sec,
            piece_waiter,
            metadata_pins,
            created_at: Instant::now(),
            first_read_logged: false,
            last_wait_log: Instant::now()
                .checked_sub(Duration::from_secs(5))
                .unwrap_or_else(Instant::now),
            last_prioritized_piece: -1,
            consecutive_waits: 0,
            last_blocked_replan: Instant::now(),
            file,
            file_cursor: 0,
            seek_pending: false,
            open_file: None,
            scratch: vec![0; 256 * 1024],
            verified_piece: None,
            broker_piece: None,
            piece_read: None,
            piece_read_retry_at: None,
            retry_sleep: None,
            alert_hub,
            playback_permit,
        }
    }

    fn current_piece(&self) -> std::io::Result<i32> {
        if self.piece_length == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "torrent piece length is zero",
            ));
        }

        Ok(((self.file_offset + self.current_pos) / self.piece_length) as i32)
    }

    fn bytes_available_in_verified_piece(&self, piece: i32) -> usize {
        let current_global = self.file_offset + self.current_pos;
        let piece_end_global = ((piece as u64) + 1).saturating_mul(self.piece_length);
        let remaining_in_piece = piece_end_global.saturating_sub(current_global);
        let remaining_in_file = self.file_size.saturating_sub(self.current_pos);

        remaining_in_piece
            .min(remaining_in_file)
            .min(usize::MAX as u64) as usize
    }

    fn active_forward_window(&self, intent: PlaybackIntent, configured_forward_window: i32) -> i32 {
        if self.first_read_logged {
            return configured_forward_window;
        }

        match intent {
            // Cold starts only need a tiny head cluster before the first byte.
            // A cold seek is even narrower: fetch the exact requested piece,
            // then expand the window as soon as bytes begin flowing.
            PlaybackIntent::DirectInitial | PlaybackIntent::HlsInitial => {
                configured_forward_window.min(INITIAL_FIRST_BYTE_WINDOW_PIECES)
            }
            PlaybackIntent::DirectSeek | PlaybackIntent::HlsSeek => 1,
            _ => configured_forward_window,
        }
    }

    fn priority_intent(&self) -> PlaybackIntent {
        if self.first_read_logged {
            self.playback_intent.sequential_after_first_byte()
        } else {
            self.playback_intent
        }
    }

    fn is_background_reader(&self) -> bool {
        self.playback_permit.is_subordinate()
            || matches!(
                self.playback_intent,
                PlaybackIntent::InternalProbe | PlaybackIntent::Background
            )
    }

    /// Pinned metadata (Cues/moov) pieces for this torrent that are still
    /// missing. Verified pins are dropped as a side effect so the set shrinks
    /// toward empty and the head-window cap lifts automatically.
    fn pinned_metadata_missing(&self) -> Vec<i32> {
        let pinned = self.metadata_pins.pinned(&self.info_hash);
        if pinned.is_empty() {
            return Vec::new();
        }
        let mut verified = Vec::new();
        let mut missing = Vec::new();
        for p in pinned {
            if self.handle.have_piece(p) {
                verified.push(p);
            } else {
                missing.push(p);
            }
        }
        if !verified.is_empty() {
            self.metadata_pins.unpin_pieces(&self.info_hash, &verified);
        }
        missing
    }

    fn extend_with_pinned_metadata(
        &self, assignments: &mut Vec<super::playback::LibtorrentPiecePriority>,
        pinned_missing: &[i32],
    ) {
        for &p in pinned_missing {
            if p >= self.first_piece
                && p <= self.last_piece
                && !self.handle.have_piece(p)
                && !assignments.iter().any(|assignment| assignment.piece == p)
            {
                assignments.push(super::playback::LibtorrentPiecePriority {
                    piece: p,
                    priority: 7,
                    deadline_ms: Some(0),
                });
            }
        }
    }

    fn prioritize_from(&mut self, piece: i32) {
        if piece < self.first_piece || piece > self.last_piece {
            return;
        }

        if self.last_prioritized_piece == piece {
            return;
        }
        self.last_prioritized_piece = piece;

        if self.is_background_reader() {
            let assignments = if self.handle.have_piece(piece) {
                Vec::new()
            } else {
                let priority = if matches!(self.playback_intent, PlaybackIntent::Background) {
                    1
                } else {
                    2
                };
                vec![super::playback::LibtorrentPiecePriority {
                    piece,
                    priority,
                    deadline_ms: None,
                }]
            };
            self.playback_permit.replace_priority_window(assignments);
            return;
        }

        let priority_intent = self.priority_intent();
        let sequential_download = false;
        self.handle.set_sequential_download(false);
        let configured_forward_window =
            disk_backed_forward_window_pieces_for(priority_intent, self.piece_length);
        let forward_window = self.active_forward_window(priority_intent, configured_forward_window);
        let window_end = forward_window_end(piece, self.last_piece, forward_window);
        let presence = self.handle.piece_presence(piece, window_end);

        // A tail-seek (Cues/moov) stream records its window as metadata-critical
        // so concurrent head streams rank it above their own read-ahead until it
        // verifies. The background metadata inspector pins the same region, so a
        // head stream backs off even before the player issues the tail request.
        let is_metadata_stream = matches!(
            priority_intent,
            PlaybackIntent::ContainerMetadata | PlaybackIntent::InternalProbe
        );
        if is_metadata_stream {
            let window: Vec<i32> = (piece..=window_end)
                .filter(|p| piece_is_missing(&presence, piece, *p))
                .collect();
            self.metadata_pins.pin_pieces(&self.info_hash, window);
        }
        let pinned_missing = self.pinned_metadata_missing();
        let cues_pending = !pinned_missing.is_empty();

        // While rare Cues/moov pieces are still missing, a head stream drops its
        // read-ahead window from 7 to 4 so the few peers that hold the tail feed
        // it first. The head's current piece remains priority 7; only read-ahead
        // yields. The metadata stream itself keeps full priority; background
        // reads stay at 1.
        let read_ahead_priority = if matches!(priority_intent, PlaybackIntent::Background) {
            1
        } else if matches!(priority_intent, PlaybackIntent::InternalProbe) {
            2
        } else {
            4
        };
        let deadline_jitter = (self.stream_id % 10) as i32 * 5;
        let status = self.handle.status();
        let deadline_step = playback_deadline_step_ms(
            self.piece_length,
            self.bitrate_bytes_per_sec,
            status.download_rate.max(0) as u64,
        );
        let mut assignments = (piece..=window_end)
            .filter(|p| piece_is_missing(&presence, piece, *p))
            .map(|p| {
                let distance = p - piece;
                let deadline = if distance == 0 {
                    0
                } else {
                    distance * deadline_step + deadline_jitter
                };
                super::playback::LibtorrentPiecePriority {
                    piece: p,
                    priority: if distance == 0 {
                        7
                    } else {
                        read_ahead_priority
                    },
                    deadline_ms: Some(deadline),
                }
            })
            .collect::<Vec<_>>();
        self.extend_with_pinned_metadata(&mut assignments, &pinned_missing);
        self.playback_permit.replace_priority_window(assignments);

        tracing::debug!(
            info_hash = %self.info_hash,
            file_idx = self.file_idx,
            intent = ?self.playback_intent,
            priority_intent = ?priority_intent,
            piece,
            sequential_download,
            forward_window,
            configured_forward_window,
            cues_pending,
            deadline_jitter,
            "disk-backed stream priority window configured"
        );
    }

    /// Replan a blocking piece after the swarm has had time to react. The
    /// coordinator only applies changed leases, so an identical plan does not
    /// push an already-urgent deadline farther into the future.
    fn escalate_blocked_piece(&mut self, piece: i32) {
        if self.last_blocked_replan.elapsed() < Duration::from_millis(BLOCKED_REPLAN_INTERVAL_MS) {
            return;
        }
        self.last_blocked_replan = Instant::now();

        let priority_intent = self.priority_intent();
        let mut forward_window =
            disk_backed_forward_window_pieces_for(priority_intent, self.piece_length);
        forward_window = self.active_forward_window(priority_intent, forward_window);
        let window_end = forward_window_end(piece, self.last_piece, forward_window);
        let presence = self.handle.piece_presence(piece, window_end);
        let status = self.handle.status();
        let deadline_step = playback_deadline_step_ms(
            self.piece_length,
            self.bitrate_bytes_per_sec,
            status.download_rate.max(0) as u64,
        );
        let mut assignments = (piece..=window_end)
            .filter(|p| piece_is_missing(&presence, piece, *p))
            .map(|p| {
                let distance = p - piece;
                super::playback::LibtorrentPiecePriority {
                    piece: p,
                    priority: if distance == 0 { 7 } else { 4 },
                    deadline_ms: Some(distance * deadline_step),
                }
            })
            .collect::<Vec<_>>();
        let pinned_missing = self.pinned_metadata_missing();
        self.extend_with_pinned_metadata(&mut assignments, &pinned_missing);
        self.playback_permit.replace_priority_window(assignments);

        tracing::debug!(
            info_hash = %self.info_hash,
            file_idx = self.file_idx,
            intent = ?priority_intent,
            piece,
            forward_window,
            consecutive_waits = self.consecutive_waits,
            "disk-backed blocked piece re-prioritized"
        );
    }

    fn wait_for_piece(
        &mut self, piece: i32, cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        self.consecutive_waits = self.consecutive_waits.saturating_add(1);
        self.prioritize_from(piece);
        if !self.is_background_reader() {
            self.escalate_blocked_piece(piece);
        }
        self.piece_waiter
            .register(&self.info_hash, piece, self.stream_id, cx.waker().clone());

        self.schedule_retry(cx, Duration::from_millis(50));

        if self.last_wait_log.elapsed() >= Duration::from_secs(5) {
            self.last_wait_log = Instant::now();
            let status = self.handle.status();
            let verified_bytes_estimate = u64::try_from(status.total_wanted_done)
                .unwrap_or(0)
                .min(self.file_size);
            let verified_piece_count = if self.piece_length == 0 {
                0
            } else {
                verified_bytes_estimate.div_ceil(self.piece_length) as usize
            };
            let configured_forward_window =
                disk_backed_forward_window_pieces_for(self.playback_intent, self.piece_length);
            let active_forward_window =
                self.active_forward_window(self.playback_intent, configured_forward_window);
            let cluster_end = forward_window_end(piece, self.last_piece, active_forward_window);
            let active_presence = self.handle.piece_presence(piece, cluster_end);
            let ready_in_active_window = active_presence
                .iter()
                .filter(|present| **present != 0)
                .count();
            let missing_in_active_window =
                (cluster_end.saturating_sub(piece) + 1).max(0) as usize - ready_in_active_window;
            let request_offset_percent = if self.file_size > 0 {
                (self.current_pos.min(self.file_size) as f64 / self.file_size as f64) * 100.0
            } else {
                0.0
            };
            tracing::info!(
                info_hash = %self.info_hash,
                file_idx = self.file_idx,
                file_path = %self.display_path,
                intent = ?self.playback_intent,
                piece,
                pos = self.current_pos,
                request_offset_percent,
                verified_piece_count,
                verified_bytes_estimate,
                active_forward_window,
                ready_in_active_window,
                missing_in_active_window,
                peers = status.num_peers,
                download_rate = status.download_rate,
                paused = status.is_paused,
                auto_managed = status.is_auto_managed,
                state = status.state,
                finished = status.is_finished,
                "disk-backed download waiting for verified piece"
            );
        }

        std::task::Poll::Pending
    }

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

    fn request_piece_from_libtorrent(&mut self, piece: i32) {
        if self
            .piece_read_retry_at
            .is_some_and(|(failed_piece, retry_at)| {
                failed_piece == piece && Instant::now() < retry_at
            })
        {
            return;
        }
        if self
            .piece_read
            .as_ref()
            .is_some_and(|(requested, _)| *requested == piece)
            || self
                .broker_piece
                .as_ref()
                .is_some_and(|(cached, _)| *cached == piece)
        {
            return;
        }

        if let Some((requested, task)) = self.piece_read.take() {
            if requested != piece {
                task.abort();
            } else {
                self.piece_read = Some((requested, task));
                return;
            }
        }
        let alerts = self.alert_hub.clone();
        let handle = self.handle.clone();
        let info_hash = self.info_hash.clone();
        let subordinate = self.is_background_reader();
        let task = tokio::spawn(async move {
            alerts
                .read_piece(
                    handle,
                    &info_hash,
                    piece,
                    if subordinate { 4 } else { 7 },
                    if subordinate { 500 } else { 0 },
                    Duration::from_secs(2),
                )
                .await
        });
        self.piece_read = Some((piece, task));
    }

    fn poll_piece_broker(
        &mut self, cx: &mut std::task::Context<'_>, piece: i32,
    ) -> std::io::Result<()> {
        let Some((requested, task)) = self.piece_read.as_mut() else {
            return Ok(());
        };
        if *requested != piece {
            return Ok(());
        }
        match Pin::new(task).poll(cx) {
            Poll::Ready(Ok(Ok(data))) => {
                self.broker_piece = Some((piece, data));
                self.piece_read = None;
                self.piece_read_retry_at = None;
            }
            Poll::Ready(Ok(Err(error))) => {
                tracing::debug!(
                    info_hash = %self.info_hash,
                    file_idx = self.file_idx,
                    piece,
                    %error,
                    "read_piece broker did not produce bytes"
                );
                self.piece_read = None;
                self.piece_read_retry_at =
                    Some((piece, Instant::now() + Duration::from_millis(250)));
            }
            Poll::Ready(Err(error)) => {
                self.piece_read = None;
                if !error.is_cancelled() {
                    self.piece_read_retry_at =
                        Some((piece, Instant::now() + Duration::from_millis(250)));
                    return Err(std::io::Error::other(error));
                }
            }
            Poll::Pending => {}
        }
        Ok(())
    }

    fn serve_broker_piece(&mut self, piece: i32, buf: &mut tokio::io::ReadBuf<'_>) -> bool {
        let Some((cached_piece, data)) = self.broker_piece.as_ref() else {
            return false;
        };
        if *cached_piece != piece {
            return false;
        }
        let Some((start, end)) = broker_slice_bounds(
            piece,
            self.piece_length,
            self.file_offset,
            self.current_pos,
            self.file_size,
            data.len(),
            buf.remaining(),
        ) else {
            return false;
        };
        buf.put_slice(&data[start..end]);
        self.current_pos = self.current_pos.saturating_add((end - start) as u64);
        self.consecutive_waits = 0;
        self.record_first_read(piece, "read-piece-alert");
        self.prioritize_from(piece.saturating_add(1));
        true
    }

    fn record_first_read(&mut self, piece: i32, source: &'static str) {
        if self.first_read_logged {
            return;
        }
        self.first_read_logged = true;
        tracing::debug!(
            info_hash = %self.info_hash,
            file_idx = self.file_idx,
            intent = ?self.playback_intent,
            piece,
            source,
            elapsed_ms = self.created_at.elapsed().as_millis() as u64,
            stage = "storage_readable",
            "libtorrent playback startup stage"
        );
    }

    fn poll_file_open(&mut self, cx: &mut std::task::Context<'_>) -> Poll<std::io::Result<()>> {
        if self.file.is_some() {
            return Poll::Ready(Ok(()));
        }
        if self.open_file.is_none() {
            let file_path = self.file_path.clone();
            self.open_file = Some(tokio::spawn(async move {
                tokio::fs::OpenOptions::new()
                    .read(true)
                    .open(file_path)
                    .await
            }));
        }
        let Some(open_file) = self.open_file.as_mut() else {
            return Poll::Pending;
        };
        match Pin::new(open_file).poll(cx) {
            Poll::Ready(Ok(Ok(file))) => {
                self.file = Some(file);
                self.file_cursor = 0;
                self.seek_pending = false;
                self.open_file = None;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Ok(Err(error))) if error.kind() == std::io::ErrorKind::NotFound => {
                self.open_file = None;
                Poll::Pending
            }
            Poll::Ready(Ok(Err(error))) => {
                self.open_file = None;
                Poll::Ready(Err(error))
            }
            Poll::Ready(Err(error)) => {
                self.open_file = None;
                Poll::Ready(Err(std::io::Error::other(error)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl tokio::io::AsyncRead for LibtorrentDiskFileStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use tokio::io::AsyncSeek;

        if self.playback_permit.is_cancelled() {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "playback superseded by a newer torrent file",
            )));
        }
        if self.current_pos >= self.file_size || buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        let piece = match self.current_piece() {
            Ok(piece) => piece,
            Err(err) => return Poll::Ready(Err(err)),
        };

        if piece < self.first_piece || piece > self.last_piece {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "read position is outside selected torrent file",
            )));
        }

        if self.verified_piece != Some(piece) && !self.handle.have_piece(piece) {
            self.request_piece_from_libtorrent(piece);
            if let Err(error) = self.poll_piece_broker(cx, piece) {
                return Poll::Ready(Err(error));
            }
            if self.serve_broker_piece(piece, buf) {
                return Poll::Ready(Ok(()));
            }
            return self.wait_for_piece(piece, cx);
        }
        self.verified_piece = Some(piece);

        let verified_available = self.bytes_available_in_verified_piece(piece);
        if verified_available == 0 {
            return Poll::Ready(Ok(()));
        }

        if !self.first_read_logged || self.file.is_none() {
            self.request_piece_from_libtorrent(piece);
        }
        if let Err(error) = self.poll_piece_broker(cx, piece) {
            return Poll::Ready(Err(error));
        }
        if self.serve_broker_piece(piece, buf) {
            return Poll::Ready(Ok(()));
        }

        match self.poll_file_open(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => {
                self.request_piece_from_libtorrent(piece);
                self.schedule_retry(cx, Duration::from_millis(25));
                return Poll::Pending;
            }
        }

        if self.file_cursor != self.current_pos {
            if !self.seek_pending {
                let target = self.current_pos;
                let Some(file) = self.file.as_mut() else {
                    return Poll::Pending;
                };
                if let Err(error) = Pin::new(file).start_seek(std::io::SeekFrom::Start(target)) {
                    return Poll::Ready(Err(error));
                }
                self.seek_pending = true;
            }
            let Some(file) = self.file.as_mut() else {
                return Poll::Pending;
            };
            match Pin::new(file).poll_complete(cx) {
                Poll::Ready(Ok(position)) => {
                    self.file_cursor = position;
                    self.seek_pending = false;
                }
                Poll::Ready(Err(error)) => {
                    self.seek_pending = false;
                    return Poll::Ready(Err(error));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        let to_read = buf
            .remaining()
            .min(verified_available)
            .min(self.scratch.len());
        let read_poll = {
            let this = self.as_mut().get_mut();
            let (file, scratch) = (&mut this.file, &mut this.scratch);
            let Some(file) = file.as_mut() else {
                return Poll::Pending;
            };
            let mut scratch_buf = tokio::io::ReadBuf::new(&mut scratch[..to_read]);
            match Pin::new(file).poll_read(cx, &mut scratch_buf) {
                Poll::Ready(Ok(())) => {
                    let read = scratch_buf.filled().len();
                    Poll::Ready(Ok(read))
                }
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                Poll::Pending => Poll::Pending,
            }
        };
        let read = match read_poll {
            Poll::Ready(Ok(read)) => read,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => return Poll::Pending,
        };
        self.file_cursor = self.file_cursor.saturating_add(read as u64);

        if read == 0
            || (self.current_pos == 0
                && !self.first_read_logged
                && self.scratch[..read].iter().all(|&byte| byte == 0))
        {
            self.request_piece_from_libtorrent(piece);
            self.schedule_retry(cx, Duration::from_millis(25));
            return Poll::Pending;
        }

        buf.put_slice(&self.scratch[..read]);
        self.current_pos = self.current_pos.saturating_add(read as u64);
        self.consecutive_waits = 0;
        self.record_first_read(piece, "async-file");
        self.prioritize_from(piece.saturating_add(1));

        Poll::Ready(Ok(()))
    }
}

impl tokio::io::AsyncSeek for LibtorrentDiskFileStream {
    fn start_seek(
        mut self: std::pin::Pin<&mut Self>, position: std::io::SeekFrom,
    ) -> std::io::Result<()> {
        let new_pos = match position {
            std::io::SeekFrom::Start(pos) => pos,
            std::io::SeekFrom::Current(delta) => {
                (self.current_pos as i64).saturating_add(delta).max(0) as u64
            }
            std::io::SeekFrom::End(delta) => {
                (self.file_size as i64).saturating_add(delta).max(0) as u64
            }
        };

        self.current_pos = new_pos.min(self.file_size);
        self.last_prioritized_piece = -1;
        self.verified_piece = None;
        self.broker_piece = None;
        if let Some((_, task)) = self.piece_read.take() {
            task.abort();
        }
        self.piece_read_retry_at = None;
        Ok(())
    }

    fn poll_complete(
        self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<u64>> {
        std::task::Poll::Ready(Ok(self.current_pos))
    }
}

impl Drop for LibtorrentDiskFileStream {
    fn drop(&mut self) {
        if let Some((_, task)) = self.piece_read.take() {
            task.abort();
        }
        if let Some(task) = self.open_file.take() {
            task.abort();
        }
        self.piece_waiter.unregister_stream(self.stream_id);
    }
}

#[cfg(test)]
mod tests {
    use super::{broker_slice_bounds, forward_window_end};

    #[test]
    fn broker_slices_a_file_that_starts_inside_a_shared_piece() {
        // Piece 2 spans global bytes 32..48. This file starts four bytes into
        // that piece, so only piece bytes 4..16 belong to its first read.
        assert_eq!(broker_slice_bounds(2, 16, 36, 0, 20, 16, 64), Some((4, 16)));
    }

    #[test]
    fn broker_does_not_cross_the_selected_file_boundary() {
        assert_eq!(broker_slice_bounds(2, 16, 32, 0, 5, 16, 64), Some((0, 5)));
    }

    #[test]
    fn forward_window_piece_count_has_no_off_by_one() {
        assert_eq!(forward_window_end(100, 999, 8), 107);
        assert_eq!(forward_window_end(997, 999, 8), 999);
        assert_eq!(forward_window_end(100, 999, 1), 100);
    }
}
