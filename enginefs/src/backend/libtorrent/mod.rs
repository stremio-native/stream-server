//! libtorrent-rasterbar backend implementation
//!
//! Uses the libtorrent-sys crate to provide a high-performance native torrent backend.

use anyhow::{Result, anyhow};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::backend::{
    BackendMemoryDiagnostics, TorrentBackend, TorrentMemoryDiagnostics, TorrentSource,
};
use crate::tracker_prober::TrackerProber;

use libtorrent_sys::{LibtorrentSession, SessionSettings};

mod alerts;
mod constants;
mod disk_stream;
mod handle;
mod helpers;
mod playback;
mod stream;

pub use handle::LibtorrentTorrentHandle;
pub use playback::{LibtorrentNetworkPhase, LibtorrentPlaybackPermit, LibtorrentPlaybackStart};
// pub(crate) use stream::LibtorrentFileStream;
// Explicitly re-export read_piece_from_disk for legacy/testing if needed, or just use internally
// Actually mostly internal.

use constants::DEFAULT_TRACKERS;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LibtorrentStorageMode {
    MemoryOnly,
    DiskBacked,
}

/// libtorrent backend implementation
pub struct LibtorrentBackend {
    session: Arc<RwLock<LibtorrentSession>>,
    save_path: PathBuf,
    metadata_path: PathBuf,
    config: crate::backend::BackendConfig,
    storage_mode: LibtorrentStorageMode,
    stream_counter: Arc<std::sync::atomic::AtomicUsize>,
    /// In-memory piece cache for fast streaming
    piece_cache: Arc<crate::piece_cache::PieceCacheManager>,
    /// Registry of wakers waiting for pieces to finish downloading
    piece_waiter: Arc<crate::piece_waiter::PieceWaiterRegistry>,
    /// Pinned metadata-critical (Cues/moov) pieces that out-rank the playback head
    metadata_pins: Arc<crate::metadata_pins::MetadataPinRegistry>,
    alert_hub: Arc<alerts::LibtorrentAlertHub>,
    playback: Arc<playback::LibtorrentPlaybackCoordinator>,
}

impl LibtorrentBackend {
    /// Create a new libtorrent backend
    pub fn new(save_path: PathBuf, config: crate::backend::BackendConfig) -> Result<Self> {
        Self::new_memory_only(save_path, config)
    }

    pub fn new_memory_only(
        save_path: PathBuf,
        config: crate::backend::BackendConfig,
    ) -> Result<Self> {
        Self::new_with_storage_mode(save_path, config, LibtorrentStorageMode::MemoryOnly)
    }

    pub fn new_disk_backed(
        save_path: PathBuf,
        config: crate::backend::BackendConfig,
    ) -> Result<Self> {
        Self::new_with_storage_mode(save_path, config, LibtorrentStorageMode::DiskBacked)
    }

    fn default_session_settings(config: &crate::backend::BackendConfig) -> SessionSettings {
        let download_rate_limit = if config.speed_profile.bt_download_speed_hard_limit > 0.0 {
            config.speed_profile.bt_download_speed_hard_limit as i32
        } else {
            0
        };
        let (max_connections, max_connections_per_torrent, connections_normalized) =
            config.speed_profile.effective_connection_limits();

        if connections_normalized {
            tracing::info!(
                requested_max_connections = config.speed_profile.bt_max_connections,
                effective_max_connections = max_connections,
                effective_max_connections_per_torrent = max_connections_per_torrent,
                "Normalized torrent connection limits for multi-client stability"
            );
        }

        SessionSettings {
            listen_interfaces: config.privacy.bt_listen_interfaces.clone(),
            outgoing_interfaces: config.privacy.bt_outgoing_interfaces.clone(),
            user_agent: "stream-server/1.0".to_string(),
            enable_dht: config.privacy.bt_enable_dht,
            enable_pex: config.privacy.bt_enable_pex,
            enable_lsd: config.privacy.bt_enable_lsd,
            enable_upnp: true,
            enable_natpmp: true,
            encryption_mode: config.privacy.bt_encryption_mode.as_libtorrent_code(),
            max_connections,
            max_connections_per_torrent,
            download_rate_limit,
            upload_rate_limit: 0,
            active_downloads: 8,
            active_seeds: 16,
            active_limit: 24,
            anonymous_mode: config.privacy.bt_anonymous_mode,
            allow_multiple_connections_per_ip: config.privacy.bt_allow_multiple_connections_per_ip,
            outgoing_port: config.privacy.bt_outgoing_port as i32,
            num_outgoing_ports: config.privacy.bt_num_outgoing_ports as i32,
            proxy_host: config.privacy.bt_proxy_host.clone(),
            proxy_port: config.privacy.bt_proxy_port as i32,
            proxy_type: config.privacy.bt_proxy_type.as_libtorrent_code(),
            proxy_username: config.privacy.bt_proxy_username.clone(),
            proxy_password: config.privacy.bt_proxy_password.clone(),
            proxy_hostnames: config.privacy.bt_proxy_hostnames,
            proxy_peer_connections: config.privacy.bt_proxy_peer_connections,
            proxy_tracker_connections: config.privacy.bt_proxy_tracker_connections,
            proxy_send_host_in_connect: config.privacy.bt_proxy_send_host_in_connect,
            validate_https_trackers: config.privacy.bt_validate_https_trackers,
            ssrf_mitigation: config.privacy.bt_ssrf_mitigation,
            announce_to_all_trackers: true,
            announce_to_all_tiers: true,
        }
    }

    fn new_with_storage_mode(
        save_path: PathBuf,
        config: crate::backend::BackendConfig,
        storage_mode: LibtorrentStorageMode,
    ) -> Result<Self> {
        let settings = Self::default_session_settings(&config);

        tracing::info!(
            "LibtorrentBackend: max_connections={}, download_limit={} B/s, dht={}, pex={}, lsd={}, anonymous={}, encryption={}",
            settings.max_connections,
            settings.download_rate_limit,
            settings.enable_dht,
            settings.enable_pex,
            settings.enable_lsd,
            settings.anonymous_mode,
            settings.encryption_mode
        );

        let session = match storage_mode {
            LibtorrentStorageMode::MemoryOnly => {
                let session = LibtorrentSession::new_memory_only(settings).map_err(|e| {
                    anyhow!("Failed to create memory-only libtorrent session: {}", e)
                })?;
                tracing::info!(
                    "LibtorrentBackend: Memory-only mode (streaming-first, cache_size={})",
                    config.cache.size
                );
                session
            }
            LibtorrentStorageMode::DiskBacked => {
                let session = LibtorrentSession::new_disk_backed(settings).map_err(|e| {
                    anyhow!("Failed to create disk-backed libtorrent session: {}", e)
                })?;
                tracing::info!(
                    "LibtorrentBackend: Disk-backed mode (download-first, path={:?})",
                    save_path
                );
                session
            }
        };

        std::fs::create_dir_all(&save_path)?;

        let metadata_path = save_path.join(".metadata");
        let _ = std::fs::create_dir_all(&metadata_path);

        // Create piece cache using existing cache settings
        let piece_cache_config = crate::piece_cache::PieceCacheConfig::from_engine_config(
            &config.cache,
            save_path.join(".piece_cache"),
        );
        let piece_cache = Arc::new(crate::piece_cache::PieceCacheManager::new(
            piece_cache_config,
        ));

        let piece_waiter = Arc::new(crate::piece_waiter::PieceWaiterRegistry::new());
        let metadata_pins = Arc::new(crate::metadata_pins::MetadataPinRegistry::new());

        let session = Arc::new(RwLock::new(session));
        let alert_hub = Arc::new(alerts::LibtorrentAlertHub::new());
        let playback = playback::LibtorrentPlaybackCoordinator::new(
            session.clone(),
            alert_hub.clone(),
            storage_mode,
            config.clone(),
        );
        let backend = Self {
            session,
            save_path,
            metadata_path,
            config,
            storage_mode,
            stream_counter: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            piece_cache,
            piece_waiter,
            metadata_pins,
            alert_hub,
            playback,
        };
        backend.start_monitor_task();
        Ok(backend)
    }

    /// Create with custom settings
    pub fn with_settings(
        save_path: PathBuf,
        settings: SessionSettings,
        config: crate::backend::BackendConfig,
    ) -> Result<Self> {
        Self::with_settings_and_storage_mode(
            save_path,
            settings,
            config,
            LibtorrentStorageMode::MemoryOnly,
        )
    }

    fn with_settings_and_storage_mode(
        save_path: PathBuf,
        settings: SessionSettings,
        config: crate::backend::BackendConfig,
        storage_mode: LibtorrentStorageMode,
    ) -> Result<Self> {
        let session = match storage_mode {
            LibtorrentStorageMode::MemoryOnly => {
                let session = LibtorrentSession::new_memory_only(settings).map_err(|e| {
                    anyhow!("Failed to create memory-only libtorrent session: {}", e)
                })?;
                tracing::info!("LibtorrentBackend: Memory-only mode (streaming-first)");
                session
            }
            LibtorrentStorageMode::DiskBacked => {
                let session = LibtorrentSession::new_disk_backed(settings).map_err(|e| {
                    anyhow!("Failed to create disk-backed libtorrent session: {}", e)
                })?;
                tracing::info!("LibtorrentBackend: Disk-backed mode (download-first)");
                session
            }
        };

        std::fs::create_dir_all(&save_path)?;

        let metadata_path = save_path.join(".metadata");
        let _ = std::fs::create_dir_all(&metadata_path);

        // Create piece cache using existing cache settings
        let piece_cache_config = crate::piece_cache::PieceCacheConfig::from_engine_config(
            &config.cache,
            save_path.join(".piece_cache"),
        );
        let piece_cache = Arc::new(crate::piece_cache::PieceCacheManager::new(
            piece_cache_config,
        ));

        let piece_waiter = Arc::new(crate::piece_waiter::PieceWaiterRegistry::new());
        let metadata_pins = Arc::new(crate::metadata_pins::MetadataPinRegistry::new());

        let session = Arc::new(RwLock::new(session));
        let alert_hub = Arc::new(alerts::LibtorrentAlertHub::new());
        let playback = playback::LibtorrentPlaybackCoordinator::new(
            session.clone(),
            alert_hub.clone(),
            storage_mode,
            config.clone(),
        );
        let backend = Self {
            session,
            save_path,
            metadata_path,
            config,
            storage_mode,
            stream_counter: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            piece_cache,
            piece_waiter,
            metadata_pins,
            alert_hub,
            playback,
        };
        backend.start_monitor_task();
        Ok(backend)
    }

    /// Update session settings dynamically (called when user changes settings)
    pub async fn update_session_settings(
        &self,
        profile: &crate::backend::TorrentSpeedProfile,
        privacy: &crate::backend::TorrentPrivacyConfig,
    ) {
        let mut session = self.session.write().await;

        // Update download rate limit (0 = unlimited)
        let download_limit = if profile.bt_download_speed_hard_limit > 0.0 {
            profile.bt_download_speed_hard_limit as i32
        } else {
            0 // Unlimited
        };
        let (max_connections, max_connections_per_torrent, connections_normalized) =
            profile.effective_connection_limits();
        if connections_normalized {
            tracing::info!(
                requested_max_connections = profile.bt_max_connections,
                effective_max_connections = max_connections,
                effective_max_connections_per_torrent = max_connections_per_torrent,
                "Normalized updated torrent connection limits for multi-client stability"
            );
        }

        // Apply new settings via full settings pack
        let new_settings = libtorrent_sys::SessionSettings {
            listen_interfaces: privacy.bt_listen_interfaces.clone(),
            outgoing_interfaces: privacy.bt_outgoing_interfaces.clone(),
            user_agent: "stream-server/1.0".to_string(),
            enable_dht: privacy.bt_enable_dht,
            enable_pex: privacy.bt_enable_pex,
            enable_lsd: privacy.bt_enable_lsd,
            enable_upnp: true,
            enable_natpmp: true,
            encryption_mode: privacy.bt_encryption_mode.as_libtorrent_code(),
            max_connections,
            max_connections_per_torrent,
            download_rate_limit: download_limit,
            upload_rate_limit: 0,
            active_downloads: 8,
            active_seeds: 16,
            active_limit: 24,
            anonymous_mode: privacy.bt_anonymous_mode,
            allow_multiple_connections_per_ip: privacy.bt_allow_multiple_connections_per_ip,
            outgoing_port: privacy.bt_outgoing_port as i32,
            num_outgoing_ports: privacy.bt_num_outgoing_ports as i32,
            proxy_host: privacy.bt_proxy_host.clone(),
            proxy_port: privacy.bt_proxy_port as i32,
            proxy_type: privacy.bt_proxy_type.as_libtorrent_code(),
            proxy_username: privacy.bt_proxy_username.clone(),
            proxy_password: privacy.bt_proxy_password.clone(),
            proxy_hostnames: privacy.bt_proxy_hostnames,
            proxy_peer_connections: privacy.bt_proxy_peer_connections,
            proxy_tracker_connections: privacy.bt_proxy_tracker_connections,
            proxy_send_host_in_connect: privacy.bt_proxy_send_host_in_connect,
            validate_https_trackers: privacy.bt_validate_https_trackers,
            ssrf_mitigation: privacy.bt_ssrf_mitigation,
            announce_to_all_trackers: true,
            announce_to_all_tiers: true,
        };

        if let Err(e) = session.apply_settings(&new_settings) {
            tracing::error!("Failed to apply session settings: {}", e);
        } else {
            tracing::info!(
                "Updated libtorrent settings: max_connections={} per_torrent={} download_limit={} B/s dht={} pex={} lsd={} anonymous={} encryption={}",
                max_connections,
                max_connections_per_torrent,
                download_limit,
                privacy.bt_enable_dht,
                privacy.bt_enable_pex,
                privacy.bt_enable_lsd,
                privacy.bt_anonymous_mode,
                privacy.bt_encryption_mode.as_libtorrent_code(),
            );
        }
    }
    fn start_monitor_task(&self) {
        // === FAST ALERT PUMP ===
        // Process alerts every 5ms for minimal latency on piece notifications
        // This is CRITICAL for streaming - wakes waiting streams immediately when pieces finish
        let alert_session = self.session.clone();
        let alert_piece_cache = self.piece_cache.clone();
        let alert_piece_waiter = self.piece_waiter.clone();
        let alert_storage_mode = self.storage_mode;
        let alert_hub = self.alert_hub.clone();
        let alert_playback = self.playback.clone();

        tokio::spawn(async move {
            // REDUCED to 5ms for instant piece notifications
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(5));

            // Fetch accurate alert types directly from C++ libtorrent
            let piece_finished_alert_type = libtorrent_sys::get_piece_finished_alert_type();
            let hash_failed_alert_type = libtorrent_sys::get_hash_failed_alert_type();
            let performance_alert_type = libtorrent_sys::get_performance_alert_type();

            loop {
                interval.tick().await;

                let alerts = {
                    let mut session = alert_session.write().await;
                    session.pop_alerts()
                };

                for mut alert in alerts {
                    // Wake priority, pause, and piece-byte waiters before doing
                    // cache or observability work. This path never holds the
                    // native session lock.
                    alert_hub.dispatch(&mut alert);

                    // Hash failures are expected on unhealthy peers; libtorrent re-downloads the piece.
                    if alert.alert_type == hash_failed_alert_type {
                        alert_playback
                            .observe_piece_invalidated(&alert.info_hash, alert.piece_index);
                        tracing::warn!(
                            piece = alert.piece_index,
                            info_hash = %alert.info_hash,
                            alert_message = %alert.message,
                            "piece hash validation failed; libtorrent will retry"
                        );
                    }

                    if alert.alert_type == performance_alert_type {
                        tracing::warn!(
                            info_hash = %alert.info_hash,
                            alert_message = %alert.message,
                            "libtorrent performance warning"
                        );
                    }

                    // Handle piece_finished_alert. Memory sessions copy verified pieces into
                    // the Rust cache; disk sessions only need to wake file readers.
                    if alert.alert_type == piece_finished_alert_type && alert.piece_index >= 0 {
                        tracing::info!(
                            piece = alert.piece_index,
                            info_hash = %alert.info_hash,
                            stage = "piece_verified",
                            "libtorrent verified piece available"
                        );
                        alert_playback.observe_piece_verified(&alert.info_hash, alert.piece_index);

                        if matches!(alert_storage_mode, LibtorrentStorageMode::MemoryOnly) {
                            if !alert.info_hash.is_empty() {
                                libtorrent_sys::memory_label_last_unlabeled_storage(
                                    &alert.info_hash,
                                );
                            }
                            let piece_data = libtorrent_sys::memory_read_piece_direct(
                                &alert.info_hash,
                                alert.piece_index,
                            );
                            if !piece_data.is_empty() {
                                alert_piece_cache.put_piece_now(
                                    &alert.info_hash,
                                    alert.piece_index,
                                    piece_data,
                                );
                                alert_piece_waiter
                                    .notify_piece_finished(&alert.info_hash, alert.piece_index);
                                tracing::info!(
                                    "Direct-read: Cached piece {} for {}",
                                    alert.piece_index,
                                    alert.info_hash
                                );
                            } else {
                                tracing::warn!(
                                    "piece_finished_alert: memory_read_piece_direct returned empty for piece={} info_hash={}",
                                    alert.piece_index,
                                    alert.info_hash,
                                );
                            }
                        }

                        // Still notify waiters so they can retry if the storage read raced.
                        alert_piece_waiter
                            .notify_piece_finished(&alert.info_hash, alert.piece_index);
                    }
                }
            }
        });

        // === SLOW MONITOR ===
        // Handle stats, metadata, peer search, etc. every 2 seconds
        let session = self.session.clone();
        let metadata_path = self.metadata_path.clone();
        let config = self.config.clone();
        let _save_path = self.save_path.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));
            loop {
                interval.tick().await;

                let handles: Vec<_> = {
                    let s = session.read().await;
                    s.get_torrents()
                        .iter()
                        .filter_map(|t| s.find_torrent(&t.info_hash).ok())
                        .collect()
                };

                for mut handle in handles {
                    let status = handle.status();
                    if status.is_paused {
                        continue;
                    }

                    // --- Metadata Initialization Logic ---
                    if status.has_metadata {
                        // NOTE: We no longer reset priorities here!
                        // Priorities are already set to 0 in add_torrent().
                        // Resetting here was causing race conditions with get_file_reader()
                        // which sets priorities for streaming. This was the root cause of
                        // "WAITING for piece 0" with 0 download speed.

                        // Instant Loading Part 3: Save Metadata to Cache
                        let info_hash = handle.info_hash();
                        let cache_file =
                            metadata_path.join(format!("{}.torrent", info_hash.to_lowercase()));
                        if !cache_file.exists() {
                            let metadata = handle.get_metadata();
                            if !metadata.is_empty() && std::fs::write(&cache_file, metadata).is_ok()
                            {
                                tracing::info!(
                                    "Instant Loading: Saved metadata for {} to cache.",
                                    info_hash
                                );
                            }
                        }
                    }

                    // Do not auto-pause "finished" torrents here. With selective file
                    // priorities, libtorrent can report finished when the current wanted
                    // set is complete or temporarily empty, even while HTTP playback or
                    // download readers are about to request more pieces. Cleanup clears
                    // file priorities per stream; pausing here causes slow startup and
                    // stalls when several torrents are active.

                    // Tracker/DHT activation and emergency announces are owned by
                    // the playback coordinator. The session's normal announce
                    // schedule remains active while downloading; this monitor
                    // must not race a generation-scoped pause.

                    // --- SwarmCap Logic ---
                    if let Some(max_speed) = config.swarm_cap.max_speed
                        && (status.download_rate as f64) > max_speed
                    {
                        // Limit handling placeholder
                    }

                    // --- Growler Logic ---
                    let total_downloaded = status.total_downloaded as u64;
                    if total_downloaded > config.growler.flood {
                        if let Some(pulse) = config.growler.pulse {
                            handle.set_download_limit(pulse as i32);
                        }
                    } else {
                        handle.set_download_limit(-1);
                    }
                }
            }
        });
    }

    /// Mark a torrent as latency-sensitive without pausing other active torrents.
    pub async fn focus_torrent(&self, target_info_hash: &str) {
        tracing::trace!(
            info_hash = %target_info_hash,
            "Ignoring legacy focus request; libtorrent playback coordinator owns activation"
        );
    }

    /// Enable or disable streaming mode
    /// Currently a no-op - upload limiting was causing peer deprioritization
    pub async fn set_streaming_mode(&self, _enabled: bool) {
        // DISABLED: Upload limiting causes peers to deprioritize us due to tit-for-tat
        // Even 100KB/s wasn't enough - let uploads run freely
        // The download speed benefit wasn't worth the peer reciprocity cost
    }

    /// Resume all paused torrents (called when streaming ends)
    pub async fn resume_all_torrents(&self) {
        tracing::trace!(
            "Ignoring legacy resume-all request; libtorrent playback coordinator owns activation"
        );
    }

    /// Pause all torrents (called when no active streams remain)
    pub async fn pause_all_torrents(&self) {
        let session = self.session.read().await;
        let torrents = session.get_torrents();

        for status in torrents {
            if !status.is_paused
                && let Ok(mut handle) = session.find_torrent(&status.info_hash)
            {
                tracing::info!("Pausing torrent {} (no active streams)", status.info_hash);
                handle.pause();
            }
        }
    }
}

#[async_trait::async_trait]
impl TorrentBackend for LibtorrentBackend {
    type Handle = LibtorrentTorrentHandle;

    async fn add_torrent(
        &self,
        source: TorrentSource,
        trackers: Vec<String>,
    ) -> Result<Self::Handle> {
        let mut session = self.session.write().await;
        let save_path = self.save_path.to_string_lossy().to_string();

        let mut handle = match source {
            TorrentSource::Url(url) => {
                // Instant Loading Part 1: Check Metadata Cache
                if let Ok(params) = libtorrent_sys::parse_magnet(&url) {
                    let info_hash = params.info_hash.to_lowercase();
                    let cache_file = self.metadata_path.join(format!("{}.torrent", info_hash));

                    if cache_file.exists() {
                        if let Ok(cached_data) = std::fs::read(&cache_file) {
                            tracing::info!(
                                "Instant Loading: Found cached metadata for {}. Skipping magnet resolution.",
                                info_hash
                            );
                            let mut p = params.clone();
                            p.torrent_data = cached_data;
                            p.save_path = save_path;
                            p.paused = true;
                            p.auto_managed = false;
                            // Inject known trackers immediately
                            for &t in DEFAULT_TRACKERS {
                                if !p.trackers.contains(&t.to_string()) {
                                    p.trackers.push(t.to_string());
                                }
                            }
                            session
                                .add_torrent(&p)
                                .map_err(|e| anyhow!("Failed to add torrent from cache: {}", e))?
                        } else {
                            session
                                .add_magnet(&url, &save_path)
                                .map_err(|e| anyhow!("Failed to add magnet: {}", e))?
                        }
                    } else {
                        session
                            .add_magnet(&url, &save_path)
                            .map_err(|e| anyhow!("Failed to add magnet: {}", e))?
                    }
                } else {
                    session
                        .add_magnet(&url, &save_path)
                        .map_err(|e| anyhow!("Failed to add magnet: {}", e))?
                }
            }
            TorrentSource::Bytes(data) => {
                let params = libtorrent_sys::AddTorrentParams {
                    magnet_uri: String::new(),
                    torrent_data: data,
                    save_path,
                    name: String::new(),
                    trackers: trackers.clone(),
                    paused: true,
                    auto_managed: false,
                    upload_limit: 0,
                    download_limit: 0,
                    sequential_download: false,
                    info_hash: String::new(),
                    info_hash_v2: String::new(),
                };
                session
                    .add_torrent(&params)
                    .map_err(|e| anyhow!("Failed to add torrent: {}", e))?
            }
        };

        // Instant Loading Part 2: tracker injection. The playback coordinator
        // owns resume and announce ordering.
        let mut final_trackers: Vec<String> = trackers.clone();
        for &t in DEFAULT_TRACKERS {
            if !final_trackers.iter().any(|existing| existing == t) {
                final_trackers.push(t.to_string());
            }
        }

        for tracker in &final_trackers {
            handle.add_tracker(tracker, 0);
        }

        // Background: Rank trackers and re-apply
        let mut rank_handle = handle.clone();
        tokio::spawn(async move {
            let ranked = TrackerProber::rank_trackers(final_trackers).await;
            if rank_handle.is_valid() {
                rank_handle.replace_trackers(&ranked);
                tracing::debug!(
                    "Trackers ranked and updated for {}",
                    rank_handle.info_hash()
                );
            }
        });

        // Add trackers with tier based on position
        for (idx, tracker) in trackers.iter().enumerate() {
            handle.add_tracker(tracker, idx as i32);
        }

        let info_hash = handle.info_hash();
        self.playback.register_torrent(&info_hash).await;
        Ok(LibtorrentTorrentHandle {
            session: self.session.clone(),
            info_hash,
            save_path: self.save_path.clone(),
            config: self.config.clone(),
            storage_mode: self.storage_mode,
            stream_counter: self.stream_counter.clone(),
            piece_cache: self.piece_cache.clone(),
            piece_waiter: self.piece_waiter.clone(),
            metadata_pins: self.metadata_pins.clone(),
            playback: self.playback.clone(),
        })
    }

    async fn get_torrent(&self, info_hash: &str) -> Option<Self::Handle> {
        let session = self.session.read().await;
        match session.find_torrent(info_hash) {
            Ok(_) => Some(LibtorrentTorrentHandle {
                session: self.session.clone(),
                info_hash: info_hash.to_string(),
                save_path: self.save_path.clone(),
                config: self.config.clone(),
                storage_mode: self.storage_mode,
                stream_counter: self.stream_counter.clone(),
                piece_cache: self.piece_cache.clone(),
                piece_waiter: self.piece_waiter.clone(),
                metadata_pins: self.metadata_pins.clone(),
                playback: self.playback.clone(),
            }),
            Err(_) => None,
        }
    }

    async fn remove_torrent(&self, info_hash: &str) -> Result<()> {
        {
            let mut session = self.session.write().await;
            let handle = session
                .find_torrent(info_hash)
                .map_err(|e| anyhow!("Torrent not found: {}", e))?;
            session
                .remove_torrent(&handle, false)
                .map_err(|e| anyhow!("Failed to remove torrent: {}", e))?;
        }
        self.playback.remove_torrent(info_hash).await;

        self.piece_cache.clear_torrent(info_hash).await;
        self.piece_waiter.clear_torrent(info_hash);
        libtorrent_sys::memory_clear_torrent(info_hash);
        Ok(())
    }
    async fn list_torrents(&self) -> Vec<String> {
        let session = self.session.read().await;
        session
            .get_torrents()
            .iter()
            .map(|t| t.info_hash.to_string())
            .collect()
    }

    async fn memory_diagnostics(&self) -> BackendMemoryDiagnostics {
        let native = if matches!(self.storage_mode, LibtorrentStorageMode::MemoryOnly) {
            libtorrent_sys::memory_storage_stats()
        } else {
            libtorrent_sys::MemoryStorageStats {
                total_bytes: 0,
                total_pieces: 0,
                total_read_bytes: 0,
                total_write_bytes: 0,
                torrents: Vec::new(),
            }
        };
        let (rust_piece_cache_entries, rust_piece_cache_bytes) =
            if matches!(self.storage_mode, LibtorrentStorageMode::MemoryOnly) {
                self.piece_cache.stats()
            } else {
                (0, 0)
            };
        let waiters = self.piece_waiter.stats();

        BackendMemoryDiagnostics {
            native_storage_bytes: native.total_bytes,
            native_storage_pieces: native.total_pieces,
            native_total_read_bytes: native.total_read_bytes,
            native_total_write_bytes: native.total_write_bytes,
            rust_piece_cache_entries,
            rust_piece_cache_bytes,
            waiter_keys: waiters.keys,
            waiter_wakers: waiters.wakers,
            torrents: native
                .torrents
                .into_iter()
                .map(|torrent| TorrentMemoryDiagnostics {
                    info_hash: torrent.info_hash,
                    native_storage_bytes: torrent.bytes,
                    native_storage_pieces: torrent.pieces,
                })
                .collect(),
        }
    }

    async fn update_session_settings(
        &self,
        profile: &crate::backend::TorrentSpeedProfile,
        privacy: &crate::backend::TorrentPrivacyConfig,
    ) {
        LibtorrentBackend::update_session_settings(self, profile, privacy).await;
    }

    fn set_seeding_enabled(&self, enabled: bool) {
        self.playback.set_seeding_enabled(enabled);
    }

    async fn focus_torrent(&self, target_info_hash: &str) {
        LibtorrentBackend::focus_torrent(self, target_info_hash).await;
    }

    async fn resume_all_torrents(&self) {
        LibtorrentBackend::resume_all_torrents(self).await;
    }

    async fn pause_all_torrents(&self) {
        LibtorrentBackend::pause_all_torrents(self).await;
    }

    async fn set_streaming_mode(&self, enabled: bool) {
        LibtorrentBackend::set_streaming_mode(self, enabled).await;
    }
}
