use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Result, anyhow};
use parking_lot::RwLock as SyncRwLock;
use tokio::sync::{Mutex, OnceCell, RwLock, broadcast, mpsc};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::backend::priorities::{
    MemoryPressure, PlaybackIntent, PlaybackPriorityPolicy, PriorityContext,
};

use super::alerts::LibtorrentAlertHub;
use super::{LibtorrentStorageMode, LibtorrentTorrentHandle};

const IDLE_PAUSE_GRACE: Duration = Duration::from_secs(15);
const HLS_ACTIVITY_TTL: Duration = Duration::from_secs(15);
const FILE_PRIORITY_ACK_TIMEOUT: Duration = Duration::from_secs(2);
const PAUSE_CONFIRM_TIMEOUT: Duration = Duration::from_secs(5);
const PAUSE_QUIET_CONFIRMATION: Duration = Duration::from_millis(500);
const PAUSE_RETRY_DELAY: Duration = Duration::from_secs(5);
const EMERGENCY_REANNOUNCE_DELAY: Duration = Duration::from_secs(2);
const EMERGENCY_REANNOUNCE_COOLDOWN: Duration = Duration::from_secs(60);
const METADATA_TIMEOUT: Duration = Duration::from_secs(30);
const HLS_PRIORITY_STREAM_ID: usize = usize::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LibtorrentNetworkPhase {
    Active,
    PausePending,
    PausedIdle,
}

#[derive(Debug, Clone)]
pub struct LibtorrentPlaybackStart {
    pub file_idx: usize,
    pub stream_id: usize,
    pub start_offset: u64,
    pub priority: u8,
    pub intent: PlaybackIntent,
    pub bitrate_bytes_per_sec: Option<u64>,
    pub source: &'static str,
}

pub struct LibtorrentPlaybackPermit {
    pub info_hash: String,
    pub file_idx: usize,
    pub generation: u64,
    stream_id: usize,
    pub cancellation: CancellationToken,
    command_tx: mpsc::UnboundedSender<PlaybackCommand>,
    foreground: bool,
    subordinate: bool,
    released: bool,
}

impl LibtorrentPlaybackPermit {
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    pub(crate) fn is_subordinate(&self) -> bool {
        self.subordinate
    }

    pub(crate) fn replace_priority_window(&self, assignments: Vec<LibtorrentPiecePriority>) {
        if self.released || self.cancellation.is_cancelled() {
            return;
        }
        let _ = self
            .command_tx
            .send(PlaybackCommand::ReplacePriorityWindow {
                info_hash: self.info_hash.clone(),
                file_idx: self.file_idx,
                generation: self.generation,
                stream_id: self.stream_id,
                assignments,
            });
    }
}

impl Drop for LibtorrentPlaybackPermit {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        let _ = self.command_tx.send(PlaybackCommand::ReleasePlayback {
            info_hash: self.info_hash.clone(),
            file_idx: self.file_idx,
            generation: self.generation,
            stream_id: self.stream_id,
            foreground: self.foreground,
        });
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LibtorrentPiecePriority {
    pub(crate) piece: i32,
    pub(crate) priority: i32,
    pub(crate) deadline_ms: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PiecePriorityChange {
    Apply(LibtorrentPiecePriority),
    Reset(i32),
}

#[derive(Default)]
struct ActivePriorityWindows {
    streams: HashMap<usize, HashMap<i32, LibtorrentPiecePriority>>,
    effective: HashMap<i32, LibtorrentPiecePriority>,
}

impl ActivePriorityWindows {
    fn replace(
        &mut self, stream_id: usize, assignments: Vec<LibtorrentPiecePriority>,
    ) -> Vec<PiecePriorityChange> {
        let replacement = assignments
            .into_iter()
            .filter(|assignment| assignment.piece >= 0)
            .map(|assignment| (assignment.piece, assignment))
            .collect::<HashMap<_, _>>();
        let mut affected = replacement.keys().copied().collect::<HashSet<_>>();
        if let Some(previous) = self.streams.insert(stream_id, replacement) {
            affected.extend(previous.keys().copied());
        }
        self.reconcile(affected)
    }

    fn release(&mut self, stream_id: usize) -> Vec<PiecePriorityChange> {
        let Some(previous) = self.streams.remove(&stream_id) else {
            return Vec::new();
        };
        self.reconcile(previous.into_keys().collect())
    }

    fn clear_tracking(&mut self) {
        self.streams.clear();
        self.effective.clear();
    }

    fn mark_piece_verified(&mut self, piece: i32) {
        for window in self.streams.values_mut() {
            window.remove(&piece);
        }
        self.effective.remove(&piece);
    }

    fn reconcile(&mut self, affected: HashSet<i32>) -> Vec<PiecePriorityChange> {
        let mut changes = Vec::with_capacity(affected.len());
        for piece in affected {
            let next = self.effective_for(piece);
            if self.effective.get(&piece).copied() == next {
                continue;
            }
            match next {
                Some(next) => {
                    self.effective.insert(piece, next);
                    changes.push(PiecePriorityChange::Apply(next));
                }
                None => {
                    self.effective.remove(&piece);
                    changes.push(PiecePriorityChange::Reset(piece));
                }
            }
        }
        changes.sort_unstable_by_key(|change| match change {
            PiecePriorityChange::Apply(assignment) => assignment.piece,
            PiecePriorityChange::Reset(piece) => *piece,
        });
        changes
    }

    fn effective_for(&self, piece: i32) -> Option<LibtorrentPiecePriority> {
        self.streams
            .values()
            .filter_map(|window| window.get(&piece).copied())
            .reduce(|left, right| LibtorrentPiecePriority {
                piece,
                priority: left.priority.max(right.priority),
                deadline_ms: match (left.deadline_ms, right.deadline_ms) {
                    (Some(left), Some(right)) => Some(left.min(right)),
                    (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
                    (None, None) => None,
                },
            })
    }
}

struct LibtorrentMetadataPermit {
    info_hash: String,
    generation: u64,
    command_tx: mpsc::UnboundedSender<PlaybackCommand>,
}

impl Drop for LibtorrentMetadataPermit {
    fn drop(&mut self) {
        let _ = self.command_tx.send(PlaybackCommand::ReleaseMetadata {
            info_hash: self.info_hash.clone(),
            generation: self.generation,
        });
    }
}

pub(crate) struct TorrentLayout {
    pub(crate) files: Arc<[libtorrent_sys::FileInfo]>,
    pub(crate) piece_length: u64,
    completion: SyncRwLock<Vec<Option<bool>>>,
}

struct TorrentPlaybackEntry {
    operation: Mutex<()>,
    state: Mutex<TorrentPlaybackState>,
    layout: OnceCell<Arc<TorrentLayout>>,
}

impl TorrentPlaybackEntry {
    fn new() -> Self {
        Self {
            operation: Mutex::new(()),
            state: Mutex::new(TorrentPlaybackState::new()),
            layout: OnceCell::new(),
        }
    }
}

struct TorrentPlaybackState {
    phase: LibtorrentNetworkPhase,
    generation: u64,
    selected_file: Option<usize>,
    selected_priority: i32,
    selected_first_piece: Option<i32>,
    direct_permits: HashMap<u64, usize>,
    foreground_permits: HashMap<u64, usize>,
    metadata_permits: usize,
    hls_last_activity: Option<Instant>,
    idle_deadline: Option<Instant>,
    cancellation: CancellationToken,
    acknowledged_priorities: Option<Vec<i32>>,
    metadata_announced: bool,
    last_emergency_reannounce: Option<Instant>,
    priority_windows: ActivePriorityWindows,
}

impl TorrentPlaybackState {
    fn new() -> Self {
        Self {
            // Every libtorrent add path starts paused and manually managed.
            // Network activation is explicit once metadata or an incomplete
            // playback file actually requires it.
            phase: LibtorrentNetworkPhase::PausedIdle,
            generation: 0,
            selected_file: None,
            selected_priority: 0,
            selected_first_piece: None,
            direct_permits: HashMap::new(),
            foreground_permits: HashMap::new(),
            metadata_permits: 0,
            hls_last_activity: None,
            idle_deadline: None,
            cancellation: CancellationToken::new(),
            acknowledged_priorities: None,
            metadata_announced: false,
            last_emergency_reannounce: None,
            priority_windows: ActivePriorityWindows::default(),
        }
    }

    fn active_playback_permits(&self) -> usize {
        self.direct_permits.values().sum()
    }

    fn active_foreground_permits(&self) -> usize {
        self.foreground_permits
            .get(&self.generation)
            .copied()
            .unwrap_or(0)
    }

    fn hls_is_active(&self, now: Instant) -> bool {
        self.hls_last_activity
            .is_some_and(|last_activity| now.duration_since(last_activity) < HLS_ACTIVITY_TTL)
    }

    fn has_activity(&self, now: Instant) -> bool {
        self.active_playback_permits() > 0 || self.metadata_permits > 0 || self.hls_is_active(now)
    }

    fn schedule_idle_if_needed(&mut self, now: Instant) {
        if self.has_activity(now) {
            self.idle_deadline = None;
            return;
        }

        self.idle_deadline = Some(
            self.hls_last_activity
                .map(|last| last + HLS_ACTIVITY_TTL)
                .unwrap_or(now + IDLE_PAUSE_GRACE)
                .max(now),
        );
    }

    fn select(
        &mut self, file_idx: usize, native_priority: i32, direct_permit: bool, hls_activity: bool,
        network_required: bool, foreground: bool,
    ) -> Selection {
        let subordinate = direct_permit
            && !foreground
            && self.selected_file == Some(file_idx)
            && self.active_foreground_permits() > 0;
        let changed =
            self.selected_file != Some(file_idx) || self.selected_priority != native_priority;
        if changed {
            self.generation = self.generation.saturating_add(1);
            self.cancellation.cancel();
            self.cancellation = CancellationToken::new();
            self.selected_file = Some(file_idx);
            self.selected_priority = native_priority;
            self.selected_first_piece = None;
            self.priority_windows.clear_tracking();
        }

        if direct_permit {
            *self.direct_permits.entry(self.generation).or_default() += 1;
            if foreground {
                *self.foreground_permits.entry(self.generation).or_default() += 1;
            }
        }
        if hls_activity {
            self.hls_last_activity = Some(Instant::now());
        }
        // A complete file is a local-only read. Preserve PausedIdle so repeated
        // MPV Range opens do not manufacture a pause/resume lifecycle or wait
        // for an alert libtorrent will not emit for an already-paused torrent.
        if network_required {
            self.phase = LibtorrentNetworkPhase::Active;
        }
        self.idle_deadline = None;

        Selection {
            generation: self.generation,
            cancellation: self.cancellation.clone(),
            changed,
            subordinate,
            native_priority,
        }
    }

    fn begin_pause(&mut self) -> u64 {
        self.generation = self.generation.saturating_add(1);
        self.cancellation.cancel();
        self.cancellation = CancellationToken::new();
        self.selected_file = None;
        self.selected_priority = 0;
        self.selected_first_piece = None;
        self.priority_windows.clear_tracking();
        self.hls_last_activity = None;
        self.idle_deadline = None;
        self.phase = LibtorrentNetworkPhase::PausePending;
        self.generation
    }

    fn keep_active_for_seeding(&mut self, generation: u64) {
        if self.generation == generation && self.phase == LibtorrentNetworkPhase::PausePending {
            self.phase = LibtorrentNetworkPhase::Active;
        }
    }
}

struct Selection {
    generation: u64,
    cancellation: CancellationToken,
    changed: bool,
    subordinate: bool,
    native_priority: i32,
}

#[derive(Debug, Clone, Copy)]
struct PauseStatusSnapshot {
    is_paused: bool,
    num_peers: i32,
    total_downloaded: i64,
    total_uploaded: i64,
    download_rate: i32,
    upload_rate: i32,
}

#[derive(Default)]
struct PauseSilenceTracker {
    stable_since: Option<Instant>,
    total_downloaded: i64,
    total_uploaded: i64,
}

impl PauseSilenceTracker {
    fn observe(&mut self, status: PauseStatusSnapshot, now: Instant) -> bool {
        if !status.is_paused || status.num_peers != 0 {
            self.stable_since = None;
            return false;
        }

        let totals_changed = self.stable_since.is_some()
            && (self.total_downloaded != status.total_downloaded
                || self.total_uploaded != status.total_uploaded);
        if self.stable_since.is_none() || totals_changed {
            self.stable_since = Some(now);
            self.total_downloaded = status.total_downloaded;
            self.total_uploaded = status.total_uploaded;
            return false;
        }

        now.duration_since(self.stable_since.expect("stable timestamp initialized"))
            >= PAUSE_QUIET_CONFIRMATION
    }
}

enum PlaybackCommand {
    ReplacePriorityWindow {
        info_hash: String,
        file_idx: usize,
        generation: u64,
        stream_id: usize,
        assignments: Vec<LibtorrentPiecePriority>,
    },
    ReleasePlayback {
        info_hash: String,
        file_idx: usize,
        generation: u64,
        stream_id: usize,
        foreground: bool,
    },
    ReleaseMetadata {
        info_hash: String,
        generation: u64,
    },
    SeedingEnabled(bool),
    PieceVerified {
        info_hash: String,
        piece: i32,
    },
    PieceInvalidated {
        info_hash: String,
        piece: i32,
    },
}

pub(crate) struct LibtorrentPlaybackCoordinator {
    session: Arc<RwLock<libtorrent_sys::LibtorrentSession>>,
    alerts: Arc<LibtorrentAlertHub>,
    entries: Mutex<HashMap<String, Arc<TorrentPlaybackEntry>>>,
    command_tx: mpsc::UnboundedSender<PlaybackCommand>,
    seeding_enabled: AtomicBool,
    storage_mode: LibtorrentStorageMode,
    config: crate::backend::BackendConfig,
}

impl LibtorrentPlaybackCoordinator {
    pub(crate) fn new(
        session: Arc<RwLock<libtorrent_sys::LibtorrentSession>>, alerts: Arc<LibtorrentAlertHub>,
        storage_mode: LibtorrentStorageMode, config: crate::backend::BackendConfig,
    ) -> Arc<Self> {
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let coordinator = Arc::new(Self {
            session,
            alerts,
            entries: Mutex::new(HashMap::new()),
            command_tx,
            seeding_enabled: AtomicBool::new(true),
            storage_mode,
            config,
        });
        Self::spawn_worker(&coordinator, command_rx);
        coordinator
    }

    fn spawn_worker(
        coordinator: &Arc<Self>, mut command_rx: mpsc::UnboundedReceiver<PlaybackCommand>,
    ) {
        let coordinator = Arc::downgrade(coordinator);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let Some(coordinator) = coordinator.upgrade() else {
                            return;
                        };
                        coordinator.pause_due_torrents().await;
                    }
                    command = command_rx.recv() => {
                        let Some(command) = command else {
                            return;
                        };
                        let Some(coordinator) = coordinator.upgrade() else {
                            return;
                        };
                        coordinator.handle_command(command).await;
                    }
                }
            }
        });
    }

    pub(crate) fn alerts(&self) -> &Arc<LibtorrentAlertHub> {
        &self.alerts
    }

    async fn entry(&self, info_hash: &str) -> Arc<TorrentPlaybackEntry> {
        let info_hash = info_hash.to_lowercase();
        let mut entries = self.entries.lock().await;
        entries
            .entry(info_hash)
            .or_insert_with(|| Arc::new(TorrentPlaybackEntry::new()))
            .clone()
    }

    pub(crate) async fn register_torrent(&self, info_hash: &str) {
        let _ = self.entry(info_hash).await;
    }

    pub(crate) fn observe_piece_verified(&self, info_hash: &str, piece: i32) {
        let _ = self.command_tx.send(PlaybackCommand::PieceVerified {
            info_hash: info_hash.to_lowercase(),
            piece,
        });
    }

    pub(crate) fn observe_piece_invalidated(&self, info_hash: &str, piece: i32) {
        let _ = self.command_tx.send(PlaybackCommand::PieceInvalidated {
            info_hash: info_hash.to_lowercase(),
            piece,
        });
    }

    pub(crate) async fn remove_torrent(&self, info_hash: &str) {
        let info_hash = info_hash.to_lowercase();
        if let Some(entry) = self.entries.lock().await.remove(&info_hash) {
            entry.state.lock().await.cancellation.cancel();
        }
        self.alerts.remove_torrent(&info_hash);
    }

    pub(crate) fn set_seeding_enabled(&self, enabled: bool) {
        self.seeding_enabled.store(enabled, Ordering::Relaxed);
        let _ = self
            .command_tx
            .send(PlaybackCommand::SeedingEnabled(enabled));
    }

    pub(crate) async fn metadata_layout(
        self: &Arc<Self>, info_hash: &str,
    ) -> Result<Arc<TorrentLayout>> {
        let info_hash = info_hash.to_lowercase();
        let entry = self.entry(&info_hash).await;
        let layout = entry
            .layout
            .get_or_try_init(|| self.load_metadata_layout(&info_hash))
            .await?;
        Ok(layout.clone())
    }

    async fn load_metadata_layout(self: &Arc<Self>, info_hash: &str) -> Result<Arc<TorrentLayout>> {
        let _permit = self.acquire_metadata(info_hash).await?;
        let mut receiver = self.alerts.subscribe(info_hash);
        let started = Instant::now();
        loop {
            if let Some(layout) = self.query_layout(info_hash).await? {
                tracing::info!(
                    info_hash = %info_hash,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    file_count = layout.files.len(),
                    file_count_bucket = file_count_bucket(layout.files.len()),
                    torrent_layout = if layout.files.len() > 1 { "multi" } else { "single" },
                    storage_mode = ?self.storage_mode,
                    stage = "metadata_ready",
                    "libtorrent playback startup stage"
                );
                return Ok(layout);
            }

            let remaining = METADATA_TIMEOUT
                .checked_sub(started.elapsed())
                .ok_or_else(|| anyhow!("Timeout waiting for torrent metadata (30s)"))?;
            let received = tokio::time::timeout(remaining, receiver.recv())
                .await
                .map_err(|_| anyhow!("Timeout waiting for torrent metadata (30s)"))?;
            match received {
                Ok(alert)
                    if alert.alert_type == libtorrent_sys::get_metadata_received_alert_type() => {}
                Ok(alert) if alert.alert_type == libtorrent_sys::get_file_error_alert_type() => {
                    return Err(anyhow!("Torrent metadata file error: {}", alert.message));
                }
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(anyhow!("Libtorrent alert channel closed"));
                }
            }
        }
    }

    async fn query_layout(&self, info_hash: &str) -> Result<Option<Arc<TorrentLayout>>> {
        let session = self.session.read().await;
        let handle = session
            .find_torrent(info_hash)
            .map_err(|error| anyhow!("Torrent not found: {error}"))?;
        if !handle.status().has_metadata {
            return Ok(None);
        }
        let piece_length = handle.piece_length().max(0) as u64;
        let files = Arc::<[libtorrent_sys::FileInfo]>::from(handle.files());
        let completion = files
            .iter()
            .map(|file| (file.size <= 0 || file.downloaded >= file.size).then_some(true))
            .collect();
        Ok(Some(Arc::new(TorrentLayout {
            files,
            piece_length,
            completion: SyncRwLock::new(completion),
        })))
    }

    async fn acquire_metadata(
        self: &Arc<Self>, info_hash: &str,
    ) -> Result<LibtorrentMetadataPermit> {
        let entry = self.entry(info_hash).await;
        let _operation = entry.operation.lock().await;
        let session = self.session.read().await;
        let mut handle = session
            .find_torrent(info_hash)
            .map_err(|error| anyhow!("Torrent not found: {error}"))?;
        let status = handle.status();
        let generation = {
            let mut state = entry.state.lock().await;
            state.metadata_permits = state.metadata_permits.saturating_add(1);
            if !status.has_metadata {
                state.phase = LibtorrentNetworkPhase::Active;
            }
            state.idle_deadline = None;
            state.generation
        };

        let permit = LibtorrentMetadataPermit {
            info_hash: info_hash.to_string(),
            generation,
            command_tx: self.command_tx.clone(),
        };

        if status.has_metadata {
            return Ok(permit);
        }

        let should_announce = {
            let mut state = entry.state.lock().await;
            let should_announce = !state.metadata_announced;
            state.metadata_announced = true;
            should_announce
        };
        if status.is_paused {
            handle.resume();
            tracing::info!(
                info_hash = %info_hash,
                generation,
                stage = "resume_requested",
                reason = "metadata",
                "libtorrent playback startup stage"
            );
        }
        if should_announce {
            handle.force_reannounce_with_flags(true, false);
            handle.force_dht_announce();
        }
        drop(session);
        Ok(permit)
    }

    pub(crate) async fn start_playback(
        self: &Arc<Self>, handle: &LibtorrentTorrentHandle, start: LibtorrentPlaybackStart,
    ) -> Result<LibtorrentPlaybackPermit> {
        let info_hash = handle.info_hash.to_lowercase();
        let layout = self.metadata_layout(&info_hash).await?;
        layout
            .files
            .get(start.file_idx)
            .ok_or_else(|| anyhow!("File index {} out of range", start.file_idx))?;
        let complete = self
            .file_is_complete_with_layout(&info_hash, &layout, start.file_idx)
            .await?;
        let requested_native_priority = if complete {
            0
        } else if matches!(
            start.intent,
            PlaybackIntent::DownloadFull | PlaybackIntent::DownloadRange
        ) {
            7
        } else {
            1
        };
        let foreground = !matches!(
            start.intent,
            PlaybackIntent::InternalProbe | PlaybackIntent::Background
        );
        let entry = self.entry(&info_hash).await;
        let selection = {
            let mut state = entry.state.lock().await;
            if !foreground
                && state.active_foreground_permits() > 0
                && state.selected_file != Some(start.file_idx)
            {
                return Err(anyhow!(
                    "Background probe rejected because a different playback file is active"
                ));
            }
            let native_priority = if !foreground
                && state.active_foreground_permits() > 0
                && state.selected_file == Some(start.file_idx)
            {
                state.selected_priority
            } else {
                requested_native_priority
            };
            state.select(
                start.file_idx,
                native_priority,
                true,
                false,
                !complete || self.seeding_enabled.load(Ordering::Relaxed),
                foreground,
            )
        };
        let permit = LibtorrentPlaybackPermit {
            info_hash: info_hash.clone(),
            file_idx: start.file_idx,
            generation: selection.generation,
            stream_id: start.stream_id,
            cancellation: selection.cancellation.clone(),
            command_tx: self.command_tx.clone(),
            foreground,
            subordinate: selection.subordinate,
            released: false,
        };

        if let Err(error) = self
            .activate_selection(&info_hash, &entry, &layout, &start, &selection, complete)
            .await
        {
            drop(permit);
            return Err(error);
        }
        Ok(permit)
    }

    pub(crate) async fn refresh_hls(
        self: &Arc<Self>, handle: &LibtorrentTorrentHandle, file_idx: usize, source: &'static str,
    ) -> Result<u64> {
        let info_hash = handle.info_hash.to_lowercase();
        let layout = self.metadata_layout(&info_hash).await?;
        layout
            .files
            .get(file_idx)
            .ok_or_else(|| anyhow!("File index {file_idx} out of range"))?;
        let complete = self
            .file_is_complete_with_layout(&info_hash, &layout, file_idx)
            .await?;
        let native_priority = if complete { 0 } else { 1 };
        let entry = self.entry(&info_hash).await;
        let selection = {
            let mut state = entry.state.lock().await;
            state.select(
                file_idx,
                native_priority,
                false,
                true,
                !complete || self.seeding_enabled.load(Ordering::Relaxed),
                false,
            )
        };
        if !selection.changed {
            tracing::debug!(
                info_hash = %info_hash,
                file_idx,
                generation = selection.generation,
                source,
                "HLS playback activity refreshed without native activation"
            );
            return Ok(selection.generation);
        }

        let start = LibtorrentPlaybackStart {
            file_idx,
            stream_id: HLS_PRIORITY_STREAM_ID,
            start_offset: 0,
            priority: 1,
            intent: PlaybackIntent::HlsInitial,
            bitrate_bytes_per_sec: None,
            source,
        };
        self.activate_selection(&info_hash, &entry, &layout, &start, &selection, complete)
            .await?;
        Ok(selection.generation)
    }

    pub(crate) async fn end_hls(
        self: &Arc<Self>, info_hash: &str, file_idx: usize, reason: &'static str,
    ) -> Result<()> {
        let info_hash = info_hash.to_lowercase();
        let entry = self.entry(&info_hash).await;
        let (generation, already_paused) = {
            let mut state = entry.state.lock().await;
            if state.selected_file != Some(file_idx) && state.hls_last_activity.is_none() {
                return Ok(());
            }
            let already_paused = state.phase == LibtorrentNetworkPhase::PausedIdle;
            let generation = state.begin_pause();
            (generation, already_paused)
        };
        tracing::info!(
            info_hash = %info_hash,
            file_idx,
            generation,
            reason,
            "HLS playback generation cancelled"
        );
        if self.seeding_enabled.load(Ordering::Relaxed) {
            entry.state.lock().await.keep_active_for_seeding(generation);
            return Ok(());
        }
        if already_paused {
            entry.state.lock().await.phase = LibtorrentNetworkPhase::PausedIdle;
            return Ok(());
        }
        self.perform_pause(&info_hash, &entry, generation, true)
            .await
    }

    pub(crate) async fn is_file_complete(
        self: &Arc<Self>, info_hash: &str, file_idx: usize,
    ) -> bool {
        let Ok(layout) = self.metadata_layout(info_hash).await else {
            return false;
        };
        if layout.files.get(file_idx).is_none() {
            return false;
        }
        self.file_is_complete_with_layout(info_hash, &layout, file_idx)
            .await
            .unwrap_or(false)
    }

    async fn file_is_complete_with_layout(
        &self, info_hash: &str, layout: &TorrentLayout, file_idx: usize,
    ) -> Result<bool> {
        if let Some(complete) = layout.completion.read().get(file_idx).copied().flatten() {
            return Ok(complete);
        }
        let file = layout
            .files
            .get(file_idx)
            .ok_or_else(|| anyhow!("File index {file_idx} out of range"))?;
        let session = self.session.read().await;
        let handle = session
            .find_torrent(info_hash)
            .map_err(|error| anyhow!("Torrent not found: {error}"))?;
        let presence = handle.piece_presence(file.first_piece, file.last_piece);
        let expected = usize::try_from(file.last_piece - file.first_piece + 1).unwrap_or(0);
        let complete = presence.len() == expected && presence.iter().all(|present| *present != 0);
        if let Some(cached) = layout.completion.write().get_mut(file_idx) {
            *cached = Some(complete);
        }
        Ok(complete)
    }

    async fn activate_selection(
        self: &Arc<Self>, info_hash: &str, entry: &Arc<TorrentPlaybackEntry>,
        layout: &TorrentLayout, start: &LibtorrentPlaybackStart, selection: &Selection,
        complete: bool,
    ) -> Result<()> {
        let activation_started = Instant::now();
        let operation = entry.operation.lock().await;
        if !self
            .selection_is_current(entry, start.file_idx, selection.generation)
            .await
        {
            tracing::info!(
                info_hash = %info_hash,
                file_idx = start.file_idx,
                generation = selection.generation,
                stage = "stale_operation_skipped",
                "libtorrent playback startup stage"
            );
            return Err(anyhow!("Playback request was superseded by a newer file"));
        }

        if selection.subordinate {
            tracing::debug!(
                info_hash = %info_hash,
                file_idx = start.file_idx,
                generation = selection.generation,
                intent = ?start.intent,
                "background reader joined active generation without native activation"
            );
            return Ok(());
        }

        if selection.changed {
            let session = self.session.read().await;
            let mut handle = session
                .find_torrent(info_hash)
                .map_err(|error| anyhow!("Torrent not found: {error}"))?;
            handle.clear_piece_deadlines();
        }

        let desired = desired_priorities(
            layout.files.len(),
            start.file_idx,
            selection.native_priority,
        )?;
        let local_fast_path = complete && !self.seeding_enabled.load(Ordering::Relaxed) && {
            let state = entry.state.lock().await;
            state.phase == LibtorrentNetworkPhase::PausedIdle
                && state.acknowledged_priorities.as_deref() == Some(desired.as_slice())
        };
        if local_fast_path {
            tracing::debug!(
                info_hash = %info_hash,
                file_idx = start.file_idx,
                generation = selection.generation,
                elapsed_ms = activation_started.elapsed().as_millis() as u64,
                source = start.source,
                "serving complete libtorrent file through paused local fast path"
            );
            return Ok(());
        }
        let priorities_changed = {
            let state = entry.state.lock().await;
            state.acknowledged_priorities.as_deref() != Some(desired.as_slice())
        };
        if priorities_changed {
            self.apply_file_priorities(info_hash, &desired).await?;
            let mut state = entry.state.lock().await;
            if state.generation != selection.generation {
                tracing::info!(
                    info_hash = %info_hash,
                    file_idx = start.file_idx,
                    generation = selection.generation,
                    stage = "stale_operation_skipped",
                    "libtorrent playback startup stage"
                );
                return Err(anyhow!(
                    "Playback request was superseded during priority update"
                ));
            }
            state.acknowledged_priorities = Some(desired);
        }

        if complete {
            drop(operation);
            if !self.seeding_enabled.load(Ordering::Relaxed) {
                self.perform_pause(info_hash, entry, selection.generation, false)
                    .await?;
            }
            tracing::debug!(
                info_hash = %info_hash,
                file_idx = start.file_idx,
                generation = selection.generation,
                complete,
                elapsed_ms = activation_started.elapsed().as_millis() as u64,
                source = start.source,
                "libtorrent complete file selected without resume"
            );
            return Ok(());
        }

        let first_piece = self
            .apply_hot_window(info_hash, entry, layout, start, selection.generation)
            .await?;
        {
            let mut state = entry.state.lock().await;
            if state.generation != selection.generation
                || state.selected_file != Some(start.file_idx)
            {
                tracing::info!(
                    info_hash = %info_hash,
                    file_idx = start.file_idx,
                    generation = selection.generation,
                    stage = "stale_operation_skipped",
                    "libtorrent playback startup stage"
                );
                return Err(anyhow!(
                    "Playback request was superseded while applying its hot window"
                ));
            }
            state.selected_first_piece = Some(first_piece);
        }
        let session = self.session.read().await;
        let mut native_handle = session
            .find_torrent(info_hash)
            .map_err(|error| anyhow!("Torrent not found: {error}"))?;
        let state = entry.state.lock().await;
        if state.generation != selection.generation || state.selected_file != Some(start.file_idx) {
            tracing::info!(
                info_hash = %info_hash,
                file_idx = start.file_idx,
                generation = selection.generation,
                stage = "stale_operation_skipped",
                "libtorrent playback startup stage"
            );
            return Err(anyhow!("Playback request was superseded before resume"));
        }
        let status = native_handle.status();
        if status.num_peers > 0 {
            tracing::info!(
                info_hash = %info_hash,
                file_idx = start.file_idx,
                generation = selection.generation,
                peers = status.num_peers,
                elapsed_ms = activation_started.elapsed().as_millis() as u64,
                stage = "first_peer",
                "libtorrent playback startup stage"
            );
        }
        let first_piece_present = native_handle.have_piece(first_piece);
        if status.is_paused {
            native_handle.resume();
            tracing::info!(
                info_hash = %info_hash,
                file_idx = start.file_idx,
                generation = selection.generation,
                elapsed_ms = activation_started.elapsed().as_millis() as u64,
                stage = "resume_requested",
                "libtorrent playback startup stage"
            );
        }
        if selection.changed {
            native_handle.force_reannounce_with_flags(true, false);
            native_handle.force_dht_announce();
        }
        drop(state);
        drop(session);

        if first_piece_present {
            let mut state = entry.state.lock().await;
            if state.generation == selection.generation {
                state.selected_first_piece = None;
            }
            tracing::info!(
                info_hash = %info_hash,
                file_idx = start.file_idx,
                generation = selection.generation,
                piece = first_piece,
                elapsed_ms = activation_started.elapsed().as_millis() as u64,
                stage = "first_piece_verified",
                "libtorrent playback startup stage"
            );
        }

        if selection.changed {
            self.schedule_emergency_reannounce(
                info_hash.to_string(),
                start.file_idx,
                first_piece,
                selection.generation,
                selection.cancellation.clone(),
            );
        }
        Ok(())
    }

    async fn selection_is_current(
        &self, entry: &TorrentPlaybackEntry, file_idx: usize, generation: u64,
    ) -> bool {
        let state = entry.state.lock().await;
        state.generation == generation && state.selected_file == Some(file_idx)
    }

    async fn apply_file_priorities(&self, info_hash: &str, priorities: &[i32]) -> Result<()> {
        let submitted = Instant::now();
        {
            let session = self.session.read().await;
            let handle = session
                .find_torrent(info_hash)
                .map_err(|error| anyhow!("Torrent not found: {error}"))?;
            if handle.status().is_seeding {
                // Libtorrent documents file-priority changes as no-ops for a
                // seed. A normal pause still makes the torrent network-silent,
                // so avoid waiting two seconds for an alert that cannot exist.
                tracing::debug!(
                    info_hash = %info_hash,
                    file_count = priorities.len(),
                    stage = "bulk_priority_skipped_seed",
                    "Skipped file-priority update for complete torrent"
                );
                return Ok(());
            }
        }
        let mut receiver = self.alerts.subscribe(info_hash);
        {
            let session = self.session.read().await;
            let mut handle = session
                .find_torrent(info_hash)
                .map_err(|error| anyhow!("Torrent not found: {error}"))?;
            handle.set_file_priorities(priorities);
        }
        tracing::info!(
            info_hash = %info_hash,
            file_count = priorities.len(),
            elapsed_ms = submitted.elapsed().as_millis() as u64,
            stage = "bulk_priority_submitted",
            "libtorrent playback startup stage"
        );

        let deadline = Instant::now() + FILE_PRIORITY_ACK_TIMEOUT;
        let file_priority_alert_type = libtorrent_sys::get_file_prio_alert_type();
        let file_error_alert_type = libtorrent_sys::get_file_error_alert_type();
        loop {
            let alert = tokio::time::timeout_at(deadline, receiver.recv()).await;
            match alert {
                Ok(Ok(alert)) if alert.alert_type == file_error_alert_type => {
                    return Err(anyhow!("Libtorrent file priority error: {}", alert.message));
                }
                Ok(Ok(alert)) if alert.alert_type == file_priority_alert_type => {
                    if self.file_priorities_match(info_hash, priorities).await? {
                        tracing::info!(
                            info_hash = %info_hash,
                            elapsed_ms = submitted.elapsed().as_millis() as u64,
                            stage = "file_priority_acknowledged",
                            "libtorrent playback startup stage"
                        );
                        return Ok(());
                    }
                }
                Ok(Ok(_)) | Ok(Err(broadcast::error::RecvError::Lagged(_))) => {}
                Ok(Err(broadcast::error::RecvError::Closed)) => {
                    return Err(anyhow!("Libtorrent alert channel closed"));
                }
                Err(_) => {
                    if self.file_priorities_match(info_hash, priorities).await? {
                        tracing::warn!(
                            info_hash = %info_hash,
                            elapsed_ms = submitted.elapsed().as_millis() as u64,
                            stage = "file_priority_verified_after_alert_timeout",
                            "libtorrent playback startup stage"
                        );
                        return Ok(());
                    }
                    return Err(anyhow!(
                        "Libtorrent file priorities did not match after acknowledgement timeout"
                    ));
                }
            }
        }
    }

    async fn file_priorities_match(&self, info_hash: &str, expected: &[i32]) -> Result<bool> {
        let session = self.session.read().await;
        let handle = session
            .find_torrent(info_hash)
            .map_err(|error| anyhow!("Torrent not found: {error}"))?;
        Ok(handle.get_file_priorities() == expected)
    }

    async fn apply_hot_window(
        &self, info_hash: &str, entry: &TorrentPlaybackEntry, layout: &TorrentLayout,
        start: &LibtorrentPlaybackStart, generation: u64,
    ) -> Result<i32> {
        let file = layout
            .files
            .get(start.file_idx)
            .ok_or_else(|| anyhow!("File index {} out of range", start.file_idx))?;
        if layout.piece_length == 0 {
            return Err(anyhow!("Torrent piece length is zero"));
        }
        let file_offset = file.offset.max(0) as u64;
        let start_offset = start.start_offset.min(file.size.max(0) as u64);
        let first_piece = ((file_offset + start_offset) / layout.piece_length) as i32;

        let session = self.session.read().await;
        let mut handle = session
            .find_torrent(info_hash)
            .map_err(|error| anyhow!("Torrent not found: {error}"))?;
        handle.set_sequential_download(false);
        let status = handle.status();
        let native_memory = if matches!(self.storage_mode, LibtorrentStorageMode::MemoryOnly) {
            libtorrent_sys::memory_storage_stats().total_bytes
        } else {
            0
        };
        let memory_pressure = if self.config.cache.size > 0
            && native_memory >= self.config.cache.size.saturating_mul(80) / 100
        {
            MemoryPressure::High
        } else {
            MemoryPressure::Normal
        };
        let decision = PlaybackPriorityPolicy::decide(PriorityContext {
            intent: start.intent,
            current_piece: first_piece,
            first_piece: file.first_piece,
            last_piece: file.last_piece,
            piece_length: layout.piece_length,
            file_size: file.size.max(0) as u64,
            bitrate_bytes_per_sec: start.bitrate_bytes_per_sec,
            download_rate_bytes_per_sec: status.download_rate.max(0) as u64,
            peers: status.num_peers.max(0) as u64,
            cache_size_bytes: self.config.cache.size,
            memory_pressure,
            consecutive_waits: 0,
            first_byte_sent: false,
        });
        let assignments = decision
            .assignments
            .into_iter()
            .filter(|assignment| {
                assignment.piece_idx >= file.first_piece && assignment.piece_idx <= file.last_piece
            })
            .map(|assignment| LibtorrentPiecePriority {
                piece: assignment.piece_idx,
                priority: assignment.piece_priority,
                deadline_ms: if matches!(
                    start.intent,
                    PlaybackIntent::InternalProbe | PlaybackIntent::Background
                ) {
                    None
                } else {
                    Some(assignment.deadline)
                },
            })
            .collect::<Vec<_>>();
        drop(session);
        self.replace_priority_window_locked(
            info_hash,
            entry,
            start.file_idx,
            generation,
            start.stream_id,
            assignments,
        )
        .await?;
        tracing::info!(
            info_hash = %info_hash,
            file_idx = start.file_idx,
            generation,
            piece = first_piece,
            intent = ?start.intent,
            source = start.source,
            "libtorrent hot playback window applied"
        );
        Ok(first_piece)
    }

    async fn replace_priority_window_locked(
        &self, info_hash: &str, entry: &TorrentPlaybackEntry, file_idx: usize, generation: u64,
        stream_id: usize, assignments: Vec<LibtorrentPiecePriority>,
    ) -> Result<bool> {
        let (baseline_priority, changes) = {
            let mut state = entry.state.lock().await;
            if state.generation != generation || state.selected_file != Some(file_idx) {
                return Ok(false);
            }
            let baseline_priority = state.selected_priority;
            let changes = state.priority_windows.replace(stream_id, assignments);
            (baseline_priority, changes)
        };
        self.apply_piece_priority_changes(info_hash, baseline_priority, &changes)
            .await?;
        Ok(true)
    }

    async fn apply_piece_priority_changes(
        &self, info_hash: &str, baseline_priority: i32, changes: &[PiecePriorityChange],
    ) -> Result<()> {
        if changes.is_empty() {
            return Ok(());
        }

        let session = self.session.read().await;
        let mut handle = session
            .find_torrent(info_hash)
            .map_err(|error| anyhow!("Torrent not found: {error}"))?;
        for change in changes {
            match change {
                PiecePriorityChange::Reset(piece)
                | PiecePriorityChange::Apply(LibtorrentPiecePriority {
                    piece,
                    deadline_ms: None,
                    ..
                }) => handle.reset_piece_deadline(*piece),
                PiecePriorityChange::Apply(_) => {}
            }
        }
        handle.set_piece_priorities(changes.iter().map(|change| match change {
            PiecePriorityChange::Apply(assignment) => (assignment.piece, assignment.priority),
            PiecePriorityChange::Reset(piece) => (*piece, baseline_priority),
        }));
        for change in changes {
            if let PiecePriorityChange::Apply(LibtorrentPiecePriority {
                piece,
                deadline_ms: Some(deadline_ms),
                ..
            }) = change
            {
                handle.set_piece_deadline(*piece, *deadline_ms);
            }
        }
        Ok(())
    }

    fn schedule_emergency_reannounce(
        self: &Arc<Self>, info_hash: String, file_idx: usize, piece: i32, generation: u64,
        cancellation: CancellationToken,
    ) {
        let coordinator = Arc::downgrade(self);
        tokio::spawn(async move {
            tokio::select! {
                _ = cancellation.cancelled() => return,
                _ = tokio::time::sleep(EMERGENCY_REANNOUNCE_DELAY) => {}
            }
            let Some(coordinator) = coordinator.upgrade() else {
                return;
            };
            let entry = coordinator.entry(&info_hash).await;
            if !coordinator
                .selection_is_current(&entry, file_idx, generation)
                .await
            {
                return;
            }

            let _operation = entry.operation.lock().await;
            let session = coordinator.session.read().await;
            let Ok(mut handle) = session.find_torrent(&info_hash) else {
                return;
            };
            let status = handle.status();
            if status.num_peers > 0 {
                tracing::info!(
                    info_hash = %info_hash,
                    file_idx,
                    generation,
                    peers = status.num_peers,
                    stage = "first_peer",
                    "libtorrent playback startup stage"
                );
                return;
            }
            if handle.have_piece(piece) {
                return;
            }

            let should_reannounce = {
                let mut state = entry.state.lock().await;
                let now = Instant::now();
                let allowed = state
                    .last_emergency_reannounce
                    .is_none_or(|last| now.duration_since(last) >= EMERGENCY_REANNOUNCE_COOLDOWN);
                if allowed {
                    state.last_emergency_reannounce = Some(now);
                }
                allowed
            };
            if should_reannounce {
                handle.force_reannounce_with_flags(true, true);
                tracing::warn!(
                    info_hash = %info_hash,
                    file_idx,
                    generation,
                    piece,
                    "Emergency tracker reannounce requested after zero-peer startup"
                );
            }
        });
    }

    async fn handle_command(self: &Arc<Self>, command: PlaybackCommand) {
        match command {
            PlaybackCommand::ReplacePriorityWindow {
                info_hash,
                file_idx,
                generation,
                stream_id,
                assignments,
            } => {
                let entry = self.entry(&info_hash).await;
                let _operation = entry.operation.lock().await;
                if let Err(error) = self
                    .replace_priority_window_locked(
                        &info_hash,
                        &entry,
                        file_idx,
                        generation,
                        stream_id,
                        assignments,
                    )
                    .await
                {
                    tracing::warn!(
                        info_hash = %info_hash,
                        file_idx,
                        generation,
                        stream_id,
                        %error,
                        "Failed to reconcile libtorrent playback priority window"
                    );
                }
            }
            PlaybackCommand::ReleasePlayback {
                info_hash,
                file_idx,
                generation,
                stream_id,
                foreground,
            } => {
                let entry = self.entry(&info_hash).await;
                let _operation = entry.operation.lock().await;
                let (baseline_priority, changes, remaining_permits) = {
                    let mut state = entry.state.lock().await;
                    if let Some(count) = state.direct_permits.get_mut(&generation) {
                        *count = count.saturating_sub(1);
                        if *count == 0 {
                            state.direct_permits.remove(&generation);
                        }
                    }
                    if foreground && let Some(count) = state.foreground_permits.get_mut(&generation)
                    {
                        *count = count.saturating_sub(1);
                        if *count == 0 {
                            state.foreground_permits.remove(&generation);
                        }
                    }
                    let changes = if state.generation == generation {
                        state.priority_windows.release(stream_id)
                    } else {
                        Vec::new()
                    };
                    let baseline_priority = state.selected_priority;
                    state.schedule_idle_if_needed(Instant::now());
                    (baseline_priority, changes, state.active_playback_permits())
                };
                if let Err(error) = self
                    .apply_piece_priority_changes(&info_hash, baseline_priority, &changes)
                    .await
                {
                    tracing::warn!(
                        info_hash = %info_hash,
                        file_idx,
                        generation,
                        stream_id,
                        %error,
                        "Failed to retire released libtorrent priority window"
                    );
                }
                tracing::debug!(
                    info_hash = %info_hash,
                    file_idx,
                    generation,
                    stream_id,
                    remaining_permits,
                    "Libtorrent playback permit released"
                );
            }
            PlaybackCommand::ReleaseMetadata {
                info_hash,
                generation,
            } => {
                let entry = self.entry(&info_hash).await;
                let mut state = entry.state.lock().await;
                state.metadata_permits = state.metadata_permits.saturating_sub(1);
                state.schedule_idle_if_needed(Instant::now());
                tracing::debug!(
                    info_hash = %info_hash,
                    generation,
                    metadata_permits = state.metadata_permits,
                    "Libtorrent metadata permit released"
                );
            }
            PlaybackCommand::SeedingEnabled(enabled) => {
                if enabled {
                    self.resume_idle_torrents_for_seeding().await;
                } else {
                    let entries = self
                        .entries
                        .lock()
                        .await
                        .values()
                        .cloned()
                        .collect::<Vec<_>>();
                    for entry in entries {
                        entry
                            .state
                            .lock()
                            .await
                            .schedule_idle_if_needed(Instant::now());
                    }
                }
            }
            PlaybackCommand::PieceVerified { info_hash, piece } => {
                let entry = self.entries.lock().await.get(&info_hash).cloned();
                let Some(entry) = entry else {
                    return;
                };
                if let Some(layout) = entry.layout.get() {
                    let mut completion = layout.completion.write();
                    for (file_idx, file) in layout.files.iter().enumerate() {
                        if piece >= file.first_piece
                            && piece <= file.last_piece
                            && completion[file_idx] != Some(true)
                        {
                            completion[file_idx] = None;
                        }
                    }
                }
                let selected = {
                    let mut state = entry.state.lock().await;
                    state.priority_windows.mark_piece_verified(piece);
                    if state.selected_first_piece == Some(piece) {
                        state.selected_first_piece = None;
                        tracing::info!(
                            info_hash = %info_hash,
                            file_idx = state.selected_file,
                            generation = state.generation,
                            piece,
                            stage = "first_piece_verified",
                            "libtorrent playback startup stage"
                        );
                    }
                    state
                        .selected_file
                        .map(|file_idx| (file_idx, state.generation, state.phase))
                };
                if !self.seeding_enabled.load(Ordering::Relaxed)
                    && let Some((file_idx, generation, LibtorrentNetworkPhase::Active)) = selected
                    && let Some(layout) = entry.layout.get()
                    && self
                        .file_is_complete_with_layout(&info_hash, layout, file_idx)
                        .await
                        .unwrap_or(false)
                {
                    {
                        let mut state = entry.state.lock().await;
                        if state.generation != generation
                            || state.selected_file != Some(file_idx)
                            || state.phase != LibtorrentNetworkPhase::Active
                        {
                            return;
                        }
                        state.phase = LibtorrentNetworkPhase::PausePending;
                    }
                    if let Err(error) = self
                        .perform_pause(&info_hash, &entry, generation, false)
                        .await
                    {
                        tracing::warn!(
                            info_hash = %info_hash,
                            file_idx,
                            generation,
                            %error,
                            "Failed to silence completed playback torrent"
                        );
                        let mut state = entry.state.lock().await;
                        if state.generation == generation {
                            state.phase = LibtorrentNetworkPhase::Active;
                        }
                    }
                }
            }
            PlaybackCommand::PieceInvalidated { info_hash, piece } => {
                let entry = self.entries.lock().await.get(&info_hash).cloned();
                let Some(entry) = entry else {
                    return;
                };
                if let Some(layout) = entry.layout.get() {
                    let mut completion = layout.completion.write();
                    for (file_idx, file) in layout.files.iter().enumerate() {
                        if piece >= file.first_piece && piece <= file.last_piece {
                            completion[file_idx] = None;
                        }
                    }
                }
            }
        }
    }

    async fn pause_due_torrents(self: &Arc<Self>) {
        if self.seeding_enabled.load(Ordering::Relaxed) {
            return;
        }
        let entries = self
            .entries
            .lock()
            .await
            .iter()
            .map(|(hash, entry)| (hash.clone(), entry.clone()))
            .collect::<Vec<_>>();
        let now = Instant::now();
        let mut due_torrents = Vec::new();
        for (info_hash, entry) in entries {
            let generation = {
                let mut state = entry.state.lock().await;
                if state.hls_last_activity.is_some() && !state.hls_is_active(now) {
                    state.hls_last_activity = None;
                    state.idle_deadline = Some(now);
                }
                let due = !state.has_activity(now)
                    && state.idle_deadline.is_some_and(|deadline| deadline <= now)
                    && state.phase != LibtorrentNetworkPhase::PausedIdle;
                if !due {
                    None
                } else if state.phase == LibtorrentNetworkPhase::PausePending {
                    state.idle_deadline = None;
                    Some(state.generation)
                } else {
                    Some(state.begin_pause())
                }
            };
            if let Some(generation) = generation {
                due_torrents.push((info_hash, entry, generation));
            }
        }

        let attempts = due_torrents
            .into_iter()
            .map(|(info_hash, entry, generation)| {
                let coordinator = Arc::clone(self);
                async move {
                    let result = coordinator
                        .perform_pause(&info_hash, &entry, generation, true)
                        .await;
                    (info_hash, entry, generation, result)
                }
            });
        for (info_hash, entry, generation, result) in futures::future::join_all(attempts).await {
            if let Err(error) = result {
                tracing::warn!(
                    info_hash = %info_hash,
                    generation,
                    %error,
                    "Failed to confirm idle libtorrent pause"
                );
                let mut state = entry.state.lock().await;
                if state.generation == generation {
                    state.phase = LibtorrentNetworkPhase::PausePending;
                    state.idle_deadline = Some(Instant::now() + PAUSE_RETRY_DELAY);
                }
            }
        }
    }

    async fn perform_pause(
        &self, info_hash: &str, entry: &TorrentPlaybackEntry, generation: u64,
        clear_selection: bool,
    ) -> Result<()> {
        if self.seeding_enabled.load(Ordering::Relaxed) {
            entry.state.lock().await.keep_active_for_seeding(generation);
            return Ok(());
        }
        let pause_started = Instant::now();
        let _operation = entry.operation.lock().await;
        {
            let state = entry.state.lock().await;
            if state.generation != generation {
                return Ok(());
            }
        }
        let mut receiver = self.alerts.subscribe(info_hash);
        {
            let session = self.session.read().await;
            let mut handle = session
                .find_torrent(info_hash)
                .map_err(|error| anyhow!("Torrent not found: {error}"))?;
            handle.clear_piece_deadlines();
            handle.set_sequential_download(false);
            if !handle.status().is_paused {
                handle.pause();
            }
        }
        tracing::info!(
            info_hash = %info_hash,
            generation,
            stage = "pause_requested",
            "libtorrent playback pause stage"
        );

        self.wait_for_pause_confirmation(info_hash, &mut receiver)
            .await?;
        {
            let state = entry.state.lock().await;
            if state.generation != generation {
                tracing::info!(
                    info_hash = %info_hash,
                    generation,
                    stage = "stale_operation_skipped",
                    operation = "pause-finalization",
                    "libtorrent playback pause stage"
                );
                return Ok(());
            }
        }
        let file_count = entry
            .layout
            .get()
            .map(|layout| layout.files.len())
            .unwrap_or(0);
        let zeros = vec![0; file_count];
        let priorities_changed = {
            let state = entry.state.lock().await;
            !zeros.is_empty() && state.acknowledged_priorities.as_deref() != Some(zeros.as_slice())
        };
        if priorities_changed {
            self.apply_file_priorities(info_hash, &zeros).await?;
        }

        let mut state = entry.state.lock().await;
        if state.generation == generation {
            state.phase = LibtorrentNetworkPhase::PausedIdle;
            state.acknowledged_priorities = (!zeros.is_empty()).then_some(zeros);
            if clear_selection {
                state.selected_file = None;
                state.selected_priority = 0;
            }
        }
        tracing::info!(
            info_hash = %info_hash,
            generation,
            elapsed_ms = pause_started.elapsed().as_millis() as u64,
            stage = "pause_confirmed",
            "libtorrent playback pause stage"
        );
        Ok(())
    }

    async fn wait_for_pause_confirmation(
        &self, info_hash: &str, receiver: &mut broadcast::Receiver<libtorrent_sys::AlertInfo>,
    ) -> Result<()> {
        let paused_alert_type = libtorrent_sys::get_torrent_paused_alert_type();
        let file_error_alert_type = libtorrent_sys::get_file_error_alert_type();
        let deadline = Instant::now() + PAUSE_CONFIRM_TIMEOUT;
        let mut paused_alert_seen = false;
        let mut silence = PauseSilenceTracker::default();
        loop {
            let status = self.pause_status(info_hash).await?;
            if silence.observe(status, Instant::now()) {
                tracing::info!(
                    info_hash = %info_hash,
                    stage = "peers_disconnected",
                    verification = if paused_alert_seen {
                        "alert-and-stable-counters"
                    } else {
                        "stable-counters"
                    },
                    download_rate = status.download_rate,
                    upload_rate = status.upload_rate,
                    "libtorrent playback pause stage"
                );
                return Ok(());
            }

            let now = Instant::now();
            if now >= deadline {
                return Err(anyhow!(
                    "Torrent pause was not silent after {}s (paused={}, peers={}, down={}, up={}, total_down={}, total_up={})",
                    PAUSE_CONFIRM_TIMEOUT.as_secs(),
                    status.is_paused,
                    status.num_peers,
                    status.download_rate,
                    status.upload_rate,
                    status.total_downloaded,
                    status.total_uploaded,
                ));
            }

            let poll_deadline = deadline.min(now + Duration::from_millis(50));
            match tokio::time::timeout_at(poll_deadline, receiver.recv()).await {
                Ok(Ok(alert)) if alert.alert_type == paused_alert_type => {
                    paused_alert_seen = true;
                }
                Ok(Ok(alert)) if alert.alert_type == file_error_alert_type => {
                    return Err(anyhow!(
                        "Libtorrent file error while pausing: {}",
                        alert.message
                    ));
                }
                Ok(Ok(_)) | Ok(Err(broadcast::error::RecvError::Lagged(_))) => {}
                Ok(Err(broadcast::error::RecvError::Closed)) => {
                    return Err(anyhow!("Libtorrent alert channel closed"));
                }
                Err(_) => {}
            }
        }
    }

    async fn pause_status(&self, info_hash: &str) -> Result<PauseStatusSnapshot> {
        let session = self.session.read().await;
        let handle = session
            .find_torrent(info_hash)
            .map_err(|error| anyhow!("Torrent not found: {error}"))?;
        let status = handle.status();
        Ok(PauseStatusSnapshot {
            is_paused: status.is_paused,
            num_peers: status.num_peers,
            total_downloaded: status.total_downloaded,
            total_uploaded: status.total_uploaded,
            download_rate: status.download_rate,
            upload_rate: status.upload_rate,
        })
    }

    async fn resume_idle_torrents_for_seeding(self: &Arc<Self>) {
        let entries = self
            .entries
            .lock()
            .await
            .iter()
            .map(|(hash, entry)| (hash.clone(), entry.clone()))
            .collect::<Vec<_>>();
        for (info_hash, entry) in entries {
            let _operation = entry.operation.lock().await;
            let should_resume = {
                let mut state = entry.state.lock().await;
                state.idle_deadline = None;
                matches!(
                    state.phase,
                    LibtorrentNetworkPhase::PausePending | LibtorrentNetworkPhase::PausedIdle
                )
            };
            if !should_resume {
                continue;
            }
            let session = self.session.read().await;
            if let Ok(mut handle) = session.find_torrent(&info_hash) {
                if handle.status().is_paused {
                    handle.resume();
                    handle.force_reannounce_with_flags(true, false);
                    handle.force_dht_announce();
                }
                entry.state.lock().await.phase = LibtorrentNetworkPhase::Active;
            }
        }
    }
}

fn desired_priorities(file_count: usize, file_idx: usize, priority: i32) -> Result<Vec<i32>> {
    if file_idx >= file_count {
        return Err(anyhow!("File index {file_idx} out of range"));
    }
    let mut priorities = vec![0; file_count];
    priorities[file_idx] = priority;
    Ok(priorities)
}

fn file_count_bucket(file_count: usize) -> &'static str {
    match file_count {
        0 | 1 => "1",
        2..=10 => "2-10",
        11..=100 => "11-100",
        _ => "101+",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn piece_priority(
        piece: i32, priority: i32, deadline_ms: Option<i32>,
    ) -> LibtorrentPiecePriority {
        LibtorrentPiecePriority {
            piece,
            priority,
            deadline_ms,
        }
    }

    #[test]
    fn priority_vector_size_is_constant_operation_for_large_torrent() {
        let priorities = desired_priorities(366, 365, 1).expect("valid file");
        assert_eq!(
            priorities.iter().filter(|&&priority| priority > 0).count(),
            1
        );
    }

    #[test]
    fn switching_file_cancels_previous_generation() {
        let mut state = TorrentPlaybackState::new();
        let first = state.select(1, 1, true, false, true, true);
        let second = state.select(2, 1, true, false, true, true);

        assert!(first.cancellation.is_cancelled());
        assert!(!second.cancellation.is_cancelled());
    }

    #[test]
    fn same_file_range_reuses_generation() {
        let mut state = TorrentPlaybackState::new();
        let first = state.select(7, 1, true, false, true, true);
        let second = state.select(7, 1, true, false, true, true);

        assert_eq!(first.generation, second.generation);
    }

    #[test]
    fn same_file_probe_joins_foreground_without_changing_selection() {
        let mut state = TorrentPlaybackState::new();
        let foreground = state.select(7, 1, true, false, true, true);
        let probe = state.select(7, 1, true, false, true, false);

        assert_eq!(foreground.generation, probe.generation);
        assert!(!probe.changed);
        assert!(probe.subordinate);
        assert_eq!(state.active_foreground_permits(), 1);
    }

    #[test]
    fn complete_file_uses_zero_priority_vector() {
        let priorities = desired_priorities(3, 1, 0).expect("valid file");
        assert_eq!(priorities, vec![0, 0, 0]);
    }

    #[tokio::test(start_paused = true)]
    async fn hls_activity_expires_after_fifteen_seconds() {
        let mut state = TorrentPlaybackState::new();
        state.select(2, 1, false, true, true, false);

        tokio::time::advance(Duration::from_secs(14)).await;
        assert!(state.hls_is_active(Instant::now()));

        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(!state.hls_is_active(Instant::now()));
    }

    #[test]
    fn hls_destroy_does_not_leave_pause_pending_when_seeding() {
        let mut state = TorrentPlaybackState::new();
        state.select(2, 1, false, true, true, false);

        let generation = state.begin_pause();
        state.keep_active_for_seeding(generation);

        assert_eq!(state.phase, LibtorrentNetworkPhase::Active);
        assert!(state.selected_file.is_none());
        assert!(state.hls_last_activity.is_none());
    }

    #[test]
    fn complete_same_file_range_preserves_paused_idle_phase() {
        let mut state = TorrentPlaybackState::new();
        state.phase = LibtorrentNetworkPhase::PausedIdle;
        state.selected_file = Some(4);
        state.selected_priority = 0;

        let selection = state.select(4, 0, true, false, false, true);

        assert!(!selection.changed);
        assert_eq!(state.phase, LibtorrentNetworkPhase::PausedIdle);
        assert_eq!(state.active_playback_permits(), 1);
    }

    #[test]
    fn replacing_a_stream_window_resets_only_retired_pieces() {
        let mut windows = ActivePriorityWindows::default();
        windows.replace(
            1,
            vec![
                piece_priority(10, 7, Some(0)),
                piece_priority(11, 4, Some(500)),
            ],
        );

        let changes = windows.replace(
            1,
            vec![
                piece_priority(11, 4, Some(500)),
                piece_priority(12, 4, Some(1_000)),
            ],
        );

        assert_eq!(
            changes,
            vec![
                PiecePriorityChange::Reset(10),
                PiecePriorityChange::Apply(piece_priority(12, 4, Some(1_000))),
            ]
        );
    }

    #[test]
    fn overlapping_stream_keeps_piece_urgent_until_last_release() {
        let mut windows = ActivePriorityWindows::default();
        windows.replace(1, vec![piece_priority(10, 7, Some(500))]);

        assert_eq!(
            windows.replace(2, vec![piece_priority(10, 4, Some(100))]),
            vec![PiecePriorityChange::Apply(piece_priority(10, 7, Some(100)))]
        );
        assert_eq!(
            windows.release(2),
            vec![PiecePriorityChange::Apply(piece_priority(10, 7, Some(500)))]
        );
        assert_eq!(windows.release(1), vec![PiecePriorityChange::Reset(10)]);
    }

    #[test]
    fn background_lease_does_not_remove_foreground_deadline() {
        let mut windows = ActivePriorityWindows::default();
        windows.replace(1, vec![piece_priority(20, 7, Some(0))]);

        assert!(
            windows
                .replace(2, vec![piece_priority(20, 1, None)])
                .is_empty()
        );
        assert_eq!(windows.release(2), Vec::<PiecePriorityChange>::new());
        assert_eq!(windows.release(1), vec![PiecePriorityChange::Reset(20)]);
    }

    #[tokio::test(start_paused = true)]
    async fn pause_confirmation_ignores_decaying_rate_estimates() {
        let mut tracker = PauseSilenceTracker::default();
        let mut status = PauseStatusSnapshot {
            is_paused: true,
            num_peers: 0,
            total_downloaded: 10_000,
            total_uploaded: 200,
            download_rate: 4_000_000,
            upload_rate: 1_000,
        };
        assert!(!tracker.observe(status, Instant::now()));

        tokio::time::advance(PAUSE_QUIET_CONFIRMATION).await;
        status.download_rate = 10;
        status.upload_rate = 1;
        assert!(tracker.observe(status, Instant::now()));
    }

    #[tokio::test(start_paused = true)]
    async fn pause_confirmation_resets_when_transfer_totals_move() {
        let mut tracker = PauseSilenceTracker::default();
        let mut status = PauseStatusSnapshot {
            is_paused: true,
            num_peers: 0,
            total_downloaded: 10_000,
            total_uploaded: 200,
            download_rate: 0,
            upload_rate: 0,
        };
        assert!(!tracker.observe(status, Instant::now()));

        tokio::time::advance(PAUSE_QUIET_CONFIRMATION).await;
        status.total_downloaded += 1;
        assert!(!tracker.observe(status, Instant::now()));
        tokio::time::advance(PAUSE_QUIET_CONFIRMATION).await;
        assert!(tracker.observe(status, Instant::now()));
    }
}
