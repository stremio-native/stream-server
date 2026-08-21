use anyhow::Result;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncSeek};

use priorities::PlaybackIntent;

#[cfg(feature = "librqbit")]
pub mod librqbit;

#[cfg(feature = "libtorrent")]
pub mod libtorrent;

pub mod metadata;
pub mod priorities;

pub trait FileStreamTrait: AsyncRead + AsyncSeek + Unpin + Send + Sync {}
impl<T: AsyncRead + AsyncSeek + Unpin + Send + Sync> FileStreamTrait for T {}

#[derive(Debug, Clone)]
pub enum TorrentSource {
    Url(String),
    Bytes(Vec<u8>),
}

#[async_trait::async_trait]
pub trait TorrentBackend: Send + Sync {
    type Handle: TorrentHandle;

    async fn add_torrent(
        &self,
        source: TorrentSource,
        trackers: Vec<String>,
    ) -> Result<Self::Handle>;

    async fn get_torrent(&self, info_hash: &str) -> Option<Self::Handle>;
    async fn remove_torrent(&self, info_hash: &str) -> Result<()>;
    async fn list_torrents(&self) -> Vec<String>;
    async fn memory_diagnostics(&self) -> BackendMemoryDiagnostics;
    async fn update_session_settings(
        &self,
        _profile: &TorrentSpeedProfile,
        _privacy: &TorrentPrivacyConfig,
    ) {
    }
    fn set_seeding_enabled(&self, _enabled: bool) {}
    async fn focus_torrent(&self, _target_info_hash: &str) {}
    async fn resume_all_torrents(&self) {}
    async fn pause_all_torrents(&self) {}
    async fn set_streaming_mode(&self, _enabled: bool) {}
}

#[async_trait::async_trait]
pub trait TorrentHandle: Send + Sync + Clone {
    fn info_hash(&self) -> String;
    fn name(&self) -> Option<String>;

    async fn stats(&self) -> EngineStats;
    async fn add_trackers(&self, trackers: Vec<String>) -> Result<()>;
    /// Cheap check for whether the torrent has finished downloading its wanted
    /// data. Unlike `stats()`, this must not rebuild the full statistics or walk
    /// every piece -- it is called on the hot stream-start path. Defaults to
    /// `false` (treat as still needing the swarm) for backends that cannot tell.
    async fn is_finished(&self) -> bool {
        false
    }
    /// Whether this handle owns file selection, resume, and idle-pause
    /// lifecycle internally.
    fn manages_playback_lifecycle(&self) -> bool {
        false
    }
    /// Record HLS activity without forcing shared backends to implement a
    /// libtorrent-specific lease controller.
    async fn refresh_hls_activity(&self, _file_idx: usize, _source: &'static str) -> Result<()> {
        Ok(())
    }
    /// End HLS activity immediately. Libtorrent overrides this to cancel the
    /// selected generation and confirm a normal torrent pause.
    async fn end_hls_activity(&self, _file_idx: usize, _reason: &'static str) -> Result<()> {
        Ok(())
    }
    /// Cheap per-file completion check used to avoid probing sparse local files.
    async fn is_file_complete(&self, _file_idx: usize) -> bool {
        false
    }
    /// Resume torrent activity after an idle pause.
    async fn resume_torrent(&self) -> Result<()> {
        Ok(())
    }
    /// Pause torrent activity when no stream is currently using it.
    async fn pause_torrent(&self) -> Result<()> {
        Ok(())
    }
    /// Throttle (or restore) the torrent's upload rate to control seeding
    /// WITHOUT disconnecting peers. Pausing a torrent disconnects every peer,
    /// and after a long idle the swarm cannot be reliably re-acquired (tracker
    /// min-announce-intervals reject the reannounce and the DHT routing table
    /// decays), which stalls the next episode's download indefinitely. Clamping
    /// upload instead stops seeding while keeping the torrent connected, so a
    /// newly-requested file downloads immediately from the existing peers.
    /// `true` = clamp upload to a trickle; `false` = restore unlimited upload.
    async fn set_upload_throttled(&self, _throttled: bool) -> Result<()> {
        Ok(())
    }
    /// Keep a file minimally wanted so it continues downloading in the
    /// background while higher-priority playback windows serve the current read.
    async fn keep_file_downloading(&self, _file_idx: usize) -> Result<()> {
        Ok(())
    }
    /// Reconcile wanted files for multi-file torrents. Backends that cannot
    /// apply per-file priorities may leave this as a no-op.
    async fn reconcile_file_priorities(&self, _plan: TorrentFilePriorityPlan) -> Result<()> {
        Ok(())
    }
    async fn get_file_reader(
        &self,
        file_idx: usize,
        start_offset: u64,
        priority: u8,
        bitrate: Option<u64>,
        intent: priorities::PlaybackIntent,
    ) -> Result<Box<dyn FileStreamTrait>>;
    async fn get_files(&self) -> Vec<BackendFileInfo>;
    async fn file_count(&self) -> usize {
        self.get_files().await.len()
    }
    /// Get the local filesystem path for a file (for probing without HTTP loopback)
    async fn get_file_path(&self, file_idx: usize) -> Option<String>;
    /// Prepare a file for streaming by setting its priority and waiting for initial pieces.
    /// This should be called BEFORE probing the file with ffprobe.
    /// Returns Ok(()) when initial pieces are available, or Err on timeout.
    async fn prepare_file_for_streaming(&self, file_idx: usize) -> Result<()>;
    /// Clear streaming state for a file (set priority to 0, clear piece deadlines).
    /// Called when switching to a different file to ensure exclusive downloading.
    async fn clear_file_streaming(&self, file_idx: usize) -> Result<()>;
    /// Wait until the first piece needed for the requested offset is readable.
    async fn wait_for_piece_ready(
        &self,
        file_idx: usize,
        offset: u64,
        timeout: Duration,
        intent: priorities::PlaybackIntent,
    ) -> Result<PieceReadiness>;
}

#[derive(Debug, Clone)]
pub struct TorrentFilePriorityPlan {
    pub active_file: Option<usize>,
    pub hot_file: Option<HotFilePriorityPlan>,
    pub generation: u64,
    pub reason: &'static str,
}

#[derive(Debug, Clone)]
pub struct HotFilePriorityPlan {
    pub file_idx: usize,
    pub start_offset: u64,
    pub priority: u8,
    pub intent: PlaybackIntent,
    pub bitrate_bytes_per_sec: Option<u64>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PieceReadiness {
    pub ready: bool,
    pub piece: i32,
    pub ready_pieces: u32,
    pub target_pieces: u32,
    pub elapsed_ms: u64,
    pub peers: u64,
    pub download_rate: u64,
    pub reason: String,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct BackendMemoryDiagnostics {
    pub native_storage_bytes: u64,
    pub native_storage_pieces: u64,
    pub native_total_read_bytes: u64,
    pub native_total_write_bytes: u64,
    pub rust_piece_cache_entries: u64,
    pub rust_piece_cache_bytes: u64,
    pub waiter_keys: u64,
    pub waiter_wakers: u64,
    pub torrents: Vec<TorrentMemoryDiagnostics>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct TorrentMemoryDiagnostics {
    pub info_hash: String,
    pub native_storage_bytes: u64,
    pub native_storage_pieces: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BackendFileInfo {
    pub name: String,
    pub length: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PeerStat {
    pub ip: String,
    pub down_speed: f64,
    pub up_speed: f64,
    pub rank_score: f64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SubtitleTrack {
    pub id: usize,
    pub name: String,
    pub size: u64,
}

// Stremio-compatible stats structures
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatsFile {
    pub name: String,
    pub path: String,
    pub length: u64,
    pub offset: u64,
    pub downloaded: u64,
    /// Progress 0.0 to 1.0 (from C++ file_progress)
    pub progress: f64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Default)]
pub struct Growler {
    pub flood: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pulse: Option<u64>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerSearch {
    pub max: u64,
    pub min: u64,
    pub sources: Vec<String>,
}

impl Default for PeerSearch {
    fn default() -> Self {
        Self {
            max: 200,
            min: 40,
            sources: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Default)]
pub struct SwarmCap {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_speed: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_peers: Option<u64>,
}

/// Torrent speed profile settings from frontend (stremio-web)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TorrentSpeedProfile {
    /// Hard limit on download speed (bytes/sec)
    pub bt_download_speed_hard_limit: f64,
    /// Soft limit on download speed (bytes/sec)
    pub bt_download_speed_soft_limit: f64,
    /// Handshake timeout (ms)
    pub bt_handshake_timeout: u64,
    /// Maximum connections
    pub bt_max_connections: u64,
    /// Minimum peers for stable
    pub bt_min_peers_for_stable: u64,
    /// Request timeout (ms)
    pub bt_request_timeout: u64,
}

pub const DEFAULT_BT_MAX_CONNECTIONS: u64 = 800;
pub const LEGACY_UNLIMITED_BT_MAX_CONNECTIONS: u64 = 65535;
pub const MAX_EFFECTIVE_BT_CONNECTIONS: u64 = 1200;
pub const MIN_EFFECTIVE_BT_CONNECTIONS: u64 = 80;

impl TorrentSpeedProfile {
    pub fn effective_connection_limits(&self) -> (i32, i32, bool) {
        let requested = self.bt_max_connections;
        let normalized = if requested == 0 || requested >= LEGACY_UNLIMITED_BT_MAX_CONNECTIONS {
            DEFAULT_BT_MAX_CONNECTIONS
        } else {
            requested.clamp(MIN_EFFECTIVE_BT_CONNECTIONS, MAX_EFFECTIVE_BT_CONNECTIONS)
        };

        let per_torrent = (normalized / 4).clamp(40, 200).min(normalized).max(1);

        (
            normalized as i32,
            per_torrent as i32,
            normalized != requested,
        )
    }
}

impl Default for TorrentSpeedProfile {
    fn default() -> Self {
        Self {
            bt_download_speed_hard_limit: 0.0, // 0 = unlimited
            bt_download_speed_soft_limit: 0.0, // 0 = unlimited
            bt_handshake_timeout: 20000,       // 20s - faster failure for dead peers
            bt_max_connections: DEFAULT_BT_MAX_CONNECTIONS,
            bt_min_peers_for_stable: 5, // Lower barrier to entry
            bt_request_timeout: 10000,  // 10s
        }
    }
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[derive(Default)]
pub enum TorrentEncryptionMode {
    #[default]
    Allow,
    Require,
    Disable,
}

impl TorrentEncryptionMode {
    pub fn as_libtorrent_code(self) -> i32 {
        match self {
            Self::Allow => 0,
            Self::Require => 1,
            Self::Disable => 2,
        }
    }
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[derive(Default)]
pub enum TorrentProxyType {
    #[default]
    None,
    Socks4,
    Socks5,
    Socks5Password,
    Http,
    HttpPassword,
}

impl TorrentProxyType {
    pub fn as_libtorrent_code(self) -> i32 {
        match self {
            Self::None => 0,
            Self::Socks4 => 1,
            Self::Socks5 => 2,
            Self::Socks5Password => 3,
            Self::Http => 4,
            Self::HttpPassword => 5,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TorrentPrivacyConfig {
    pub bt_enable_dht: bool,
    pub bt_enable_pex: bool,
    pub bt_enable_lsd: bool,
    pub bt_encryption_mode: TorrentEncryptionMode,
    pub bt_anonymous_mode: bool,
    pub bt_allow_multiple_connections_per_ip: bool,
    pub bt_listen_interfaces: String,
    pub bt_outgoing_interfaces: String,
    pub bt_outgoing_port: u16,
    pub bt_num_outgoing_ports: u16,
    pub bt_proxy_type: TorrentProxyType,
    pub bt_proxy_host: String,
    pub bt_proxy_port: u16,
    pub bt_proxy_username: String,
    pub bt_proxy_password: String,
    pub bt_proxy_hostnames: bool,
    pub bt_proxy_peer_connections: bool,
    pub bt_proxy_tracker_connections: bool,
    pub bt_proxy_send_host_in_connect: bool,
    pub bt_validate_https_trackers: bool,
    pub bt_ssrf_mitigation: bool,
}

impl Default for TorrentPrivacyConfig {
    fn default() -> Self {
        Self {
            bt_enable_dht: true,
            bt_enable_pex: true,
            bt_enable_lsd: true,
            bt_encryption_mode: TorrentEncryptionMode::default(),
            bt_anonymous_mode: false,
            bt_allow_multiple_connections_per_ip: false,
            bt_listen_interfaces: "0.0.0.0:42000-42010,[::]:42000-42010".to_string(),
            bt_outgoing_interfaces: String::new(),
            bt_outgoing_port: 0,
            bt_num_outgoing_ports: 0,
            bt_proxy_type: TorrentProxyType::default(),
            bt_proxy_host: String::new(),
            bt_proxy_port: 0,
            bt_proxy_username: String::new(),
            bt_proxy_password: String::new(),
            bt_proxy_hostnames: true,
            bt_proxy_peer_connections: false,
            bt_proxy_tracker_connections: true,
            bt_proxy_send_host_in_connect: false,
            bt_validate_https_trackers: true,
            bt_ssrf_mitigation: true,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct BackendConfig {
    pub cache: priorities::EngineCacheConfig,
    pub growler: Growler,
    pub peer_search: PeerSearch,
    pub swarm_cap: SwarmCap,
    pub speed_profile: TorrentSpeedProfile,
    pub privacy: TorrentPrivacyConfig,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatsOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connections: Option<u64>,
    pub dht: bool,
    pub growler: Growler,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub handshake_timeout: Option<u64>,
    pub path: String,
    pub peer_search: PeerSearch,
    pub swarm_cap: SwarmCap,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u64>,
    pub tracker: bool,
    pub r#virtual: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Source {
    pub last_started: String,
    pub num_found: u64,
    pub num_found_uniq: u64,
    pub num_requests: u64,
    pub url: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineStats {
    pub name: String,
    pub info_hash: String,
    pub files: Vec<StatsFile>,
    pub sources: Vec<Source>,
    pub opts: StatsOptions,
    pub download_speed: f64,
    pub upload_speed: f64,
    pub downloaded: u64,
    pub uploaded: u64,
    pub unchoked: u64,
    pub peers: u64,
    pub queued: u64,
    pub unique: u64,
    pub connection_tries: u64,
    pub peer_search_running: bool,
    pub stream_len: u64,
    pub stream_name: String,
    pub stream_progress: f64,
    pub swarm_connections: u64,
    pub swarm_paused: bool,
    pub swarm_size: u64,
    /// All wanted pieces are downloaded (libtorrent `is_finished`). A finished
    /// torrent is only seeding and can be paused; an unfinished one still needs
    /// the swarm to download data or fetch metadata.
    pub is_finished: bool,
    /// Torrent metadata is available (false for a freshly added magnet that is
    /// still resolving its info dictionary).
    pub has_metadata: bool,
}
