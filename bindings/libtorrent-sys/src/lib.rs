//! Comprehensive FFI bindings to libtorrent-rasterbar via cxx
//!
//! This crate provides safe Rust bindings to the full libtorrent C++ library API.

#[cxx::bridge(namespace = "libtorrent_wrapper")]
mod ffi {
    // ============================================================================
    // STRUCTS - Data types shared between Rust and C++
    // ============================================================================

    /// Session configuration settings
    #[derive(Debug, Clone, Default)]
    struct SessionSettings {
        /// Listen interfaces (e.g., "0.0.0.0:6881,[::]:6881")
        listen_interfaces: String,
        /// Outgoing interfaces (comma-separated IP addresses or device names)
        outgoing_interfaces: String,
        /// User agent string
        user_agent: String,
        /// Enable DHT
        enable_dht: bool,
        /// Enable Peer Exchange (PeX)
        enable_pex: bool,
        /// Enable LSD (Local Service Discovery)
        enable_lsd: bool,
        /// Enable UPnP
        enable_upnp: bool,
        /// Enable NAT-PMP
        enable_natpmp: bool,
        /// Maximum connections total
        max_connections: i32,
        /// Maximum connections per torrent
        max_connections_per_torrent: i32,
        /// Download rate limit in bytes/sec (0 = unlimited)
        download_rate_limit: i32,
        /// Upload rate limit in bytes/sec (0 = unlimited)
        upload_rate_limit: i32,
        /// Active downloads limit
        active_downloads: i32,
        /// Active seeds limit
        active_seeds: i32,
        /// Active limit (downloads + seeds)
        active_limit: i32,
        /// Encryption mode: 0=allow, 1=require, 2=disable
        encryption_mode: i32,
        /// Enable anonymous mode
        anonymous_mode: bool,
        /// Allow multiple peer connections from the same IP address
        allow_multiple_connections_per_ip: bool,
        /// First local outgoing port (0 = OS chooses)
        outgoing_port: i32,
        /// Number of outgoing ports in the local bind range
        num_outgoing_ports: i32,
        /// Proxy host (empty = no proxy)
        proxy_host: String,
        /// Proxy port
        proxy_port: i32,
        /// Proxy type (0=none, 1=socks4, 2=socks5, 3=socks5_pw, 4=http, 5=http_pw)
        proxy_type: i32,
        /// Proxy username
        proxy_username: String,
        /// Proxy password
        proxy_password: String,
        /// Resolve hostnames through the proxy when supported
        proxy_hostnames: bool,
        /// Proxy BitTorrent peer payload connections
        proxy_peer_connections: bool,
        /// Proxy tracker connections
        proxy_tracker_connections: bool,
        /// Send target hostname in HTTP CONNECT requests
        proxy_send_host_in_connect: bool,
        /// Validate HTTPS tracker certificates
        validate_https_trackers: bool,
        /// Apply SSRF mitigation to tracker and web seed requests
        ssrf_mitigation: bool,
        /// Announce to all trackers in a tier
        announce_to_all_trackers: bool,
        /// Announce to all tiers
        announce_to_all_tiers: bool,
    }

    /// Torrent addition parameters
    #[derive(Debug, Clone, Default)]
    struct AddTorrentParams {
        /// Magnet URI or empty
        magnet_uri: String,
        /// Torrent file data or empty
        torrent_data: Vec<u8>,
        /// Save path on disk
        save_path: String,
        /// Torrent name override
        name: String,
        /// Initial trackers to add
        trackers: Vec<String>,
        /// Start paused
        paused: bool,
        /// Auto-managed
        auto_managed: bool,
        /// Upload limit bytes/sec (0 = unlimited)
        upload_limit: i32,
        /// Download limit bytes/sec (0 = unlimited)
        download_limit: i32,
        /// Sequential download mode
        sequential_download: bool,
        /// Info hash (v1)
        info_hash: String,
        /// Info hash (v2)
        info_hash_v2: String,
    }

    /// Sparse piece-priority update submitted as one libtorrent operation.
    #[derive(Debug, Clone, Copy)]
    struct PiecePriority {
        piece: i32,
        priority: i32,
    }

    /// Torrent status information
    #[derive(Debug, Clone)]
    struct TorrentStatus {
        /// Info hash as hex string (v1)
        info_hash: String,
        /// Info hash v2 (empty if not v2)
        info_hash_v2: String,
        /// Torrent name
        name: String,
        /// Save path on disk
        save_path: String,
        /// State (0=checking_files, 1=downloading_metadata, 2=downloading, 3=finished, 4=seeding, 5=unused, 6=checking_resume_data)
        state: i32,
        /// Total size in bytes
        total_size: i64,
        /// Total done (downloaded and verified)
        total_done: i64,
        /// Total downloaded (including failed)
        total_downloaded: i64,
        /// Total uploaded
        total_uploaded: i64,
        /// Total wanted (selected files)
        total_wanted: i64,
        /// Total wanted done
        total_wanted_done: i64,
        /// Download rate in bytes/sec
        download_rate: i32,
        /// Upload rate in bytes/sec
        upload_rate: i32,
        /// Download payload rate
        download_payload_rate: i32,
        /// Upload payload rate
        upload_payload_rate: i32,
        /// Number of connected peers
        num_peers: i32,
        /// Number of connected seeds
        num_seeds: i32,
        /// Number of peers in swarm (incomplete)
        num_incomplete: i32,
        /// Number of seeds in swarm (complete)
        num_complete: i32,
        /// Progress from 0.0 to 1.0
        progress: f32,
        /// Progress parts per million
        progress_ppm: i32,
        /// Is paused
        is_paused: bool,
        /// Is auto-managed
        is_auto_managed: bool,
        /// Is finished
        is_finished: bool,
        /// Is seeding
        is_seeding: bool,
        /// Has metadata
        has_metadata: bool,
        /// Sequential download enabled
        sequential_download: bool,
        /// Current tracker
        current_tracker: String,
        /// Next announce time in seconds
        next_announce_seconds: i32,
        /// Number of pieces
        num_pieces: i32,
        /// Piece length in bytes
        piece_length: i32,
        /// Added time (unix timestamp)
        added_time: i64,
        /// Completed time (unix timestamp, 0 if not complete)
        completed_time: i64,
        /// Last seen complete (unix timestamp)
        last_seen_complete: i64,
        /// Queue position (-1 if not queued)
        queue_position: i32,
        /// Error string (empty if no error)
        error: String,
    }

    /// File information
    #[derive(Debug, Clone)]
    struct FileInfo {
        /// File index
        index: i32,
        /// File path (relative to save_path)
        path: String,
        /// Absolute path
        absolute_path: String,
        /// File size in bytes
        size: i64,
        /// Downloaded bytes
        downloaded: i64,
        /// Priority (0=skip, 1=low, 4=normal, 7=high)
        priority: i32,
        /// Progress 0.0 to 1.0
        progress: f32,
        /// First piece index
        first_piece: i32,
        /// Last piece index
        last_piece: i32,
        /// File offset in the torrent (global byte offset)
        offset: i64,
    }

    /// Peer information
    #[derive(Debug, Clone)]
    struct PeerInfo {
        /// IP address
        ip: String,
        /// Port
        port: u16,
        /// Client name
        client: String,
        /// Download rate from this peer
        download_rate: i32,
        /// Upload rate to this peer
        upload_rate: i32,
        /// Total downloaded from this peer
        total_download: i64,
        /// Total uploaded to this peer
        total_upload: i64,
        /// Progress 0.0 to 1.0
        progress: f32,
        /// Is seed
        is_seed: bool,
        /// Is connecting
        is_connecting: bool,
        /// Is handshake
        is_handshake: bool,
        /// Connection type (0=bt, 1=web_seed, 2=http_seed)
        connection_type: i32,
        /// Country code (2 chars)
        country: String,
    }

    /// Tracker info
    #[derive(Debug, Clone)]
    struct TrackerInfo {
        /// Tracker URL
        url: String,
        /// Tier
        tier: i32,
        /// Status (0=disabled, 1=enabled)
        status: i32,
        /// Last error
        message: String,
        /// Number of peers from tracker
        num_peers: i32,
        /// Scrape incomplete
        scrape_incomplete: i32,
        /// Scrape complete
        scrape_complete: i32,
        /// Next announce in seconds
        next_announce_seconds: i32,
    }

    /// Alert info
    #[derive(Debug, Clone)]
    struct AlertInfo {
        /// Alert type (numeric)
        alert_type: i32,
        /// Alert category
        category: i32,
        /// What triggered it (usually info_hash)
        what: String,
        /// Human-readable message
        message: String,
        /// Timestamp
        timestamp: i64,
        /// Associated info_hash (if any)
        info_hash: String,
        /// Piece index (for piece_finished_alert and read_piece_alert, -1 otherwise)
        piece_index: i32,
        /// Piece data (for read_piece_alert only, empty otherwise)
        piece_data: Vec<u8>,
    }

    /// DHT stats
    #[derive(Debug, Clone)]
    struct DhtStats {
        /// Nodes in routing table
        dht_nodes: i32,
        /// Global nodes estimate
        dht_node_cache: i32,
        /// Total DHT torrents tracked
        dht_torrents: i32,
        /// Total peers stored
        total_peers: i32,
    }

    /// Session stats
    #[derive(Debug, Clone)]
    struct SessionStats {
        /// Total download rate
        download_rate: i32,
        /// Total upload rate
        upload_rate: i32,
        /// Total payload download
        total_download: i64,
        /// Total payload upload
        total_upload: i64,
        /// Number of torrents
        num_torrents: i32,
        /// Number of peers
        num_peers: i32,
        /// DHT enabled
        dht_enabled: bool,
        /// DHT nodes
        dht_nodes: i32,
    }

    #[derive(Debug, Clone)]
    struct MemoryStorageTorrentStats {
        info_hash: String,
        bytes: u64,
        pieces: u64,
    }

    #[derive(Debug, Clone)]
    struct MemoryStorageStats {
        total_bytes: u64,
        total_pieces: u64,
        total_read_bytes: u64,
        total_write_bytes: u64,
        torrents: Vec<MemoryStorageTorrentStats>,
    }

    // ============================================================================
    // EXTERN C++ - Functions implemented in C++
    // ============================================================================

    unsafe extern "C++" {
        include!("libtorrent-sys/cpp/wrapper.h");

        // Opaque types
        type Session;
        type TorrentHandle;

        // ------------------------------------------------------------------------
        // Session lifecycle
        // ------------------------------------------------------------------------
        fn create_session(settings: &SessionSettings) -> Result<UniquePtr<Session>>;
        /// Create session with normal filesystem-backed storage.
        fn create_session_disk_backed(settings: &SessionSettings) -> Result<UniquePtr<Session>>;
        /// Create session with memory-only storage (no disk writes when cache_size=0)
        fn create_session_memory_only(settings: &SessionSettings) -> Result<UniquePtr<Session>>;
        fn session_abort(session: Pin<&mut Session>);
        fn session_pause(session: Pin<&mut Session>);
        fn session_resume(session: Pin<&mut Session>);
        fn session_is_paused(session: &Session) -> bool;

        // ------------------------------------------------------------------------
        // Session settings
        // ------------------------------------------------------------------------
        fn session_apply_settings(
            session: Pin<&mut Session>, settings: &SessionSettings,
        ) -> Result<()>;
        fn session_get_download_rate_limit(session: &Session) -> i32;
        fn session_get_upload_rate_limit(session: &Session) -> i32;
        fn session_set_download_rate_limit(session: Pin<&mut Session>, limit: i32);
        fn session_set_upload_rate_limit(session: Pin<&mut Session>, limit: i32);

        // ------------------------------------------------------------------------
        // Add/Remove torrents
        // ------------------------------------------------------------------------
        fn session_add_torrent(
            session: Pin<&mut Session>, params: &AddTorrentParams,
        ) -> Result<UniquePtr<TorrentHandle>>;
        fn session_add_magnet(
            session: Pin<&mut Session>, magnet_uri: &str, save_path: &str,
        ) -> Result<UniquePtr<TorrentHandle>>;
        fn session_remove_torrent(
            session: Pin<&mut Session>, handle: &TorrentHandle, delete_files: bool,
        ) -> Result<()>;

        // ------------------------------------------------------------------------
        // Torrent queries
        // ------------------------------------------------------------------------
        fn session_get_torrents(session: &Session) -> Vec<TorrentStatus>;
        fn session_find_torrent(
            session: &Session, info_hash: &str,
        ) -> Result<UniquePtr<TorrentHandle>>;
        fn session_get_torrent_status(session: &Session, info_hash: &str) -> Result<TorrentStatus>;

        // ------------------------------------------------------------------------
        // Session stats
        // ------------------------------------------------------------------------
        fn session_get_stats(session: &Session) -> SessionStats;
        fn session_get_dht_stats(session: &Session) -> DhtStats;

        // ------------------------------------------------------------------------
        // DHT
        // ------------------------------------------------------------------------
        fn session_is_dht_running(session: &Session) -> bool;
        fn session_add_dht_node(session: Pin<&mut Session>, host: &str, port: i32) -> Result<()>;
        fn session_dht_get_peers(session: Pin<&mut Session>, info_hash: &str) -> Result<()>;

        // ------------------------------------------------------------------------
        // Alerts
        // ------------------------------------------------------------------------
        fn session_pop_alerts(session: Pin<&mut Session>) -> Vec<AlertInfo>;
        fn session_wait_for_alert(session: &Session, timeout_ms: i32) -> bool;
        fn session_set_alert_mask(session: Pin<&mut Session>, mask: u32);

        // ------------------------------------------------------------------------
        // State persistence
        // ------------------------------------------------------------------------
        fn session_save_state(session: &Session) -> Vec<u8>;
        fn session_load_state(session: Pin<&mut Session>, state: &[u8]) -> Result<()>;
        fn session_save_dht_state(session: &Session) -> Vec<u8>;
        fn session_load_dht_state(session: Pin<&mut Session>, state: &[u8]) -> Result<()>;

        // ------------------------------------------------------------------------
        // TorrentHandle - Basic info
        // ------------------------------------------------------------------------
        fn handle_is_valid(handle: &TorrentHandle) -> bool;
        fn handle_clone(handle: &TorrentHandle) -> UniquePtr<TorrentHandle>;
        fn handle_get_info_hash(handle: &TorrentHandle) -> String;
        fn handle_get_info_hash_v2(handle: &TorrentHandle) -> String;
        fn handle_get_name(handle: &TorrentHandle) -> String;
        fn handle_get_status(handle: &TorrentHandle) -> TorrentStatus;

        // ------------------------------------------------------------------------
        // TorrentHandle - Control
        // ------------------------------------------------------------------------
        fn handle_pause(handle: Pin<&mut TorrentHandle>);
        fn handle_resume(handle: Pin<&mut TorrentHandle>);
        fn handle_set_upload_limit(handle: Pin<&mut TorrentHandle>, limit: i32);
        fn handle_set_download_limit(handle: Pin<&mut TorrentHandle>, limit: i32);
        fn handle_get_upload_limit(handle: &TorrentHandle) -> i32;
        fn handle_get_download_limit(handle: &TorrentHandle) -> i32;
        fn handle_force_recheck(handle: Pin<&mut TorrentHandle>);
        fn handle_force_reannounce(handle: Pin<&mut TorrentHandle>);
        fn handle_force_reannounce_with_flags(
            handle: Pin<&mut TorrentHandle>, high_priority: bool, ignore_min_interval: bool,
        );
        fn handle_force_dht_announce(handle: Pin<&mut TorrentHandle>);
        fn handle_flush_cache(handle: Pin<&mut TorrentHandle>);

        // ------------------------------------------------------------------------
        // TorrentHandle - Sequential/Streaming
        // ------------------------------------------------------------------------
        fn handle_set_sequential_download(handle: Pin<&mut TorrentHandle>, enable: bool);
        fn handle_is_sequential_download(handle: &TorrentHandle) -> bool;
        fn handle_set_piece_deadline(handle: Pin<&mut TorrentHandle>, piece: i32, deadline_ms: i32);
        fn handle_set_piece_deadline_with_alert(
            handle: Pin<&mut TorrentHandle>, piece: i32, deadline_ms: i32,
            alert_when_available: bool,
        );
        fn handle_reset_piece_deadline(handle: Pin<&mut TorrentHandle>, piece: i32);
        fn handle_clear_piece_deadlines(handle: Pin<&mut TorrentHandle>);

        // ------------------------------------------------------------------------
        // TorrentHandle - Files
        // ------------------------------------------------------------------------
        fn handle_get_files(handle: &TorrentHandle) -> Vec<FileInfo>;
        fn handle_get_file_priorities(handle: &TorrentHandle) -> Vec<i32>;
        fn handle_set_file_priority(handle: Pin<&mut TorrentHandle>, index: i32, priority: i32);
        fn handle_set_file_priorities(handle: Pin<&mut TorrentHandle>, priorities: &[i32]);
        fn handle_rename_file(
            handle: Pin<&mut TorrentHandle>, index: i32, new_name: &str,
        ) -> Result<()>;
        fn handle_move_storage(handle: Pin<&mut TorrentHandle>, new_path: &str) -> Result<()>;

        // ------------------------------------------------------------------------
        // TorrentHandle - Pieces
        // ------------------------------------------------------------------------
        fn handle_num_pieces(handle: &TorrentHandle) -> i32;
        fn handle_piece_length(handle: &TorrentHandle) -> i32;
        fn handle_have_piece(handle: &TorrentHandle, piece: i32) -> bool;
        fn handle_get_piece_presence(handle: &TorrentHandle, first: i32, last: i32) -> Vec<u8>;
        fn handle_get_piece_availability(handle: &TorrentHandle) -> Vec<i32>;
        fn handle_set_piece_priority(handle: Pin<&mut TorrentHandle>, piece: i32, priority: i32);
        fn handle_set_piece_priorities(
            handle: Pin<&mut TorrentHandle>, priorities: &[PiecePriority],
        );
        fn handle_get_piece_priorities(handle: &TorrentHandle) -> Vec<i32>;
        fn handle_read_piece(handle: Pin<&mut TorrentHandle>, piece: i32) -> Result<()>; // Async via alert

        // ------------------------------------------------------------------------
        // TorrentHandle - Peers
        // ------------------------------------------------------------------------
        fn handle_get_peers(handle: &TorrentHandle) -> Vec<PeerInfo>;
        fn handle_connect_peer(handle: Pin<&mut TorrentHandle>, ip: &str, port: u16) -> Result<()>;

        // ------------------------------------------------------------------------
        // TorrentHandle - Trackers
        // ------------------------------------------------------------------------
        fn handle_get_trackers(handle: &TorrentHandle) -> Vec<TrackerInfo>;
        fn handle_add_tracker(handle: Pin<&mut TorrentHandle>, url: &str, tier: i32);
        fn handle_remove_tracker(handle: Pin<&mut TorrentHandle>, url: &str);
        fn handle_replace_trackers(handle: Pin<&mut TorrentHandle>, urls: &[String]);

        // ------------------------------------------------------------------------
        // TorrentHandle - Save/Resume
        // ------------------------------------------------------------------------
        fn handle_save_resume_data(handle: Pin<&mut TorrentHandle>) -> Result<()>; // Async via alert
        fn handle_need_save_resume_data(handle: &TorrentHandle) -> bool;

        // ------------------------------------------------------------------------
        // Utilities
        // ------------------------------------------------------------------------
        fn parse_magnet_uri(uri: &str) -> Result<AddTorrentParams>;
        fn handle_get_metadata(handle: &TorrentHandle) -> Vec<u8>;
        fn make_magnet_uri(handle: &TorrentHandle) -> String;
        fn libtorrent_version() -> String;

        fn get_piece_finished_alert_type() -> i32;
        fn get_read_piece_alert_type() -> i32;
        fn get_metadata_received_alert_type() -> i32;
        fn get_hash_failed_alert_type() -> i32;
        fn get_file_prio_alert_type() -> i32;
        fn get_torrent_paused_alert_type() -> i32;
        fn get_torrent_resumed_alert_type() -> i32;
        fn get_file_error_alert_type() -> i32;
        fn get_performance_alert_type() -> i32;

        /// Read piece data directly from memory storage, bypassing libtorrent's
        /// internal read_piece() which fails with custom disk interfaces.
        fn memory_read_piece_direct(info_hash: &str, piece: i32) -> Vec<u8>;
        fn memory_label_last_unlabeled_storage(info_hash: &str);
        fn memory_clear_torrent(info_hash: &str);
        fn memory_storage_stats() -> MemoryStorageStats;
    }
}

pub use ffi::*;

// ============================================================================
// HIGH-LEVEL RUST WRAPPERS
// ============================================================================

/// High-level wrapper around the FFI session
pub struct LibtorrentSession {
    inner: cxx::UniquePtr<ffi::Session>,
}

unsafe impl Send for LibtorrentSession {}
unsafe impl Sync for LibtorrentSession {}

impl LibtorrentSession {
    /// Create a new libtorrent session with the given settings
    pub fn new(settings: SessionSettings) -> Result<Self, cxx::Exception> {
        let inner = ffi::create_session(&settings)?;
        Ok(Self { inner })
    }

    /// Create a new session with memory-only storage (no disk writes)
    /// Use this when cache_size = 0 to avoid any disk I/O
    pub fn new_memory_only(settings: SessionSettings) -> Result<Self, cxx::Exception> {
        let inner = ffi::create_session_memory_only(&settings)?;
        Ok(Self { inner })
    }

    /// Create a new session with normal filesystem-backed storage.
    pub fn new_disk_backed(settings: SessionSettings) -> Result<Self, cxx::Exception> {
        let inner = ffi::create_session_disk_backed(&settings)?;
        Ok(Self { inner })
    }

    /// Add a torrent using full params
    pub fn add_torrent(
        &mut self, params: &AddTorrentParams,
    ) -> Result<LibtorrentHandle, cxx::Exception> {
        let handle = ffi::session_add_torrent(self.inner.pin_mut(), params)?;
        Ok(LibtorrentHandle { inner: handle })
    }

    /// Add a torrent from a magnet URI
    pub fn add_magnet(
        &mut self, magnet_uri: &str, save_path: &str,
    ) -> Result<LibtorrentHandle, cxx::Exception> {
        let handle = ffi::session_add_magnet(self.inner.pin_mut(), magnet_uri, save_path)?;
        Ok(LibtorrentHandle { inner: handle })
    }

    /// Remove a torrent
    pub fn remove_torrent(
        &mut self, handle: &LibtorrentHandle, delete_files: bool,
    ) -> Result<(), cxx::Exception> {
        ffi::session_remove_torrent(self.inner.pin_mut(), &handle.inner, delete_files)
    }

    /// Get all torrents
    pub fn get_torrents(&self) -> Vec<TorrentStatus> {
        ffi::session_get_torrents(&self.inner)
    }

    /// Find a torrent by info hash
    pub fn find_torrent(&self, info_hash: &str) -> Result<LibtorrentHandle, cxx::Exception> {
        let handle = ffi::session_find_torrent(&self.inner, info_hash)?;
        Ok(LibtorrentHandle { inner: handle })
    }

    /// Get session stats
    pub fn stats(&self) -> SessionStats {
        ffi::session_get_stats(&self.inner)
    }

    /// Get DHT stats
    pub fn dht_stats(&self) -> DhtStats {
        ffi::session_get_dht_stats(&self.inner)
    }

    /// Pop alerts
    pub fn pop_alerts(&mut self) -> Vec<AlertInfo> {
        ffi::session_pop_alerts(self.inner.pin_mut())
    }

    /// Wait for an alert
    pub fn wait_for_alert(&self, timeout_ms: i32) -> bool {
        ffi::session_wait_for_alert(&self.inner, timeout_ms)
    }

    /// Save session state
    pub fn save_state(&self) -> Vec<u8> {
        ffi::session_save_state(&self.inner)
    }

    /// Load session state
    pub fn load_state(&mut self, state: &[u8]) -> Result<(), cxx::Exception> {
        ffi::session_load_state(self.inner.pin_mut(), state)
    }

    /// Pause session
    pub fn pause(&mut self) {
        ffi::session_pause(self.inner.pin_mut())
    }

    /// Resume session
    pub fn resume(&mut self) {
        ffi::session_resume(self.inner.pin_mut())
    }

    /// Is paused
    pub fn is_paused(&self) -> bool {
        ffi::session_is_paused(&self.inner)
    }

    /// Apply new settings
    pub fn apply_settings(&mut self, settings: &SessionSettings) -> Result<(), cxx::Exception> {
        ffi::session_apply_settings(self.inner.pin_mut(), settings)
    }

    /// Set download rate limit (0 = unlimited)
    pub fn set_download_rate_limit(&mut self, limit: i32) {
        ffi::session_set_download_rate_limit(self.inner.pin_mut(), limit)
    }

    /// Set upload rate limit (0 = unlimited)
    pub fn set_upload_rate_limit(&mut self, limit: i32) {
        ffi::session_set_upload_rate_limit(self.inner.pin_mut(), limit)
    }

    /// Get download rate limit
    pub fn get_download_rate_limit(&self) -> i32 {
        ffi::session_get_download_rate_limit(&self.inner)
    }

    /// Get upload rate limit
    pub fn get_upload_rate_limit(&self) -> i32 {
        ffi::session_get_upload_rate_limit(&self.inner)
    }
}

impl Drop for LibtorrentSession {
    fn drop(&mut self) {
        // Abort the session on drop to ensure clean shutdown
        ffi::session_abort(self.inner.pin_mut());
    }
}

/// High-level wrapper around a torrent handle
pub struct LibtorrentHandle {
    inner: cxx::UniquePtr<ffi::TorrentHandle>,
}

unsafe impl Send for LibtorrentHandle {}
unsafe impl Sync for LibtorrentHandle {}

impl Clone for LibtorrentHandle {
    fn clone(&self) -> Self {
        Self {
            inner: ffi::handle_clone(&self.inner),
        }
    }
}

impl LibtorrentHandle {
    /// Is this handle valid
    pub fn is_valid(&self) -> bool {
        ffi::handle_is_valid(&self.inner)
    }

    /// Get info hash (v1)
    pub fn info_hash(&self) -> String {
        ffi::handle_get_info_hash(&self.inner)
    }

    /// Get name
    pub fn name(&self) -> String {
        ffi::handle_get_name(&self.inner)
    }

    /// Get status
    pub fn status(&self) -> TorrentStatus {
        ffi::handle_get_status(&self.inner)
    }

    /// Get files
    pub fn files(&self) -> Vec<FileInfo> {
        ffi::handle_get_files(&self.inner)
    }

    /// Pause
    pub fn pause(&mut self) {
        ffi::handle_pause(self.inner.pin_mut())
    }

    /// Resume
    pub fn resume(&mut self) {
        ffi::handle_resume(self.inner.pin_mut())
    }

    /// Set sequential download mode
    pub fn set_sequential_download(&mut self, enable: bool) {
        ffi::handle_set_sequential_download(self.inner.pin_mut(), enable)
    }

    /// Set piece deadline for streaming
    pub fn set_piece_deadline(&mut self, piece: i32, deadline_ms: i32) {
        ffi::handle_set_piece_deadline(self.inner.pin_mut(), piece, deadline_ms)
    }

    /// Set a streaming deadline and optionally request a `read_piece_alert`
    /// containing the piece bytes as soon as the piece becomes available.
    pub fn set_piece_deadline_with_alert(
        &mut self, piece: i32, deadline_ms: i32, alert_when_available: bool,
    ) {
        ffi::handle_set_piece_deadline_with_alert(
            self.inner.pin_mut(),
            piece,
            deadline_ms,
            alert_when_available,
        );
    }

    /// Reset piece deadline
    pub fn reset_piece_deadline(&mut self, piece: i32) {
        ffi::handle_reset_piece_deadline(self.inner.pin_mut(), piece)
    }

    /// Clear all piece deadlines
    pub fn clear_piece_deadlines(&mut self) {
        ffi::handle_clear_piece_deadlines(self.inner.pin_mut())
    }

    /// Set piece priority (0=skip, 1=low, 4=normal, 7=highest)
    pub fn set_piece_priority(&mut self, piece: i32, priority: i32) {
        ffi::handle_set_piece_priority(self.inner.pin_mut(), piece, priority)
    }

    /// Update selected piece priorities with a single native libtorrent call.
    pub fn set_piece_priorities(&mut self, priorities: impl IntoIterator<Item = (i32, i32)>) {
        let priorities = priorities
            .into_iter()
            .map(|(piece, priority)| ffi::PiecePriority { piece, priority })
            .collect::<Vec<_>>();
        if !priorities.is_empty() {
            ffi::handle_set_piece_priorities(self.inner.pin_mut(), &priorities);
        }
    }

    /// Current availability count for each piece.
    pub fn piece_availability(&self) -> Vec<i32> {
        ffi::handle_get_piece_availability(&self.inner)
    }

    /// Current libtorrent priority for each piece.
    pub fn piece_priorities(&self) -> Vec<i32> {
        ffi::handle_get_piece_priorities(&self.inner)
    }

    /// Check if piece is downloaded
    pub fn have_piece(&self, piece: i32) -> bool {
        ffi::handle_have_piece(&self.inner, piece)
    }

    /// Return one byte per piece in the inclusive range (1 = present).
    ///
    /// Unlike repeated `have_piece` calls, this takes one native status
    /// snapshot and therefore blocks libtorrent's internal thread only once.
    pub fn piece_presence(&self, first: i32, last: i32) -> Vec<u8> {
        ffi::handle_get_piece_presence(&self.inner, first, last)
    }

    /// Number of pieces
    pub fn num_pieces(&self) -> i32 {
        ffi::handle_num_pieces(&self.inner)
    }

    /// Piece length
    pub fn piece_length(&self) -> i32 {
        ffi::handle_piece_length(&self.inner)
    }

    /// Set file priority
    pub fn set_file_priority(&mut self, index: i32, priority: i32) {
        ffi::handle_set_file_priority(self.inner.pin_mut(), index, priority)
    }

    /// Set all file priorities in one asynchronous libtorrent operation.
    ///
    /// Completion is reported through `file_prio_alert`.
    pub fn set_file_priorities(&mut self, priorities: &[i32]) {
        ffi::handle_set_file_priorities(self.inner.pin_mut(), priorities)
    }

    /// Get file priorities
    pub fn get_file_priorities(&self) -> Vec<i32> {
        ffi::handle_get_file_priorities(&self.inner)
    }

    /// Get peers
    pub fn peers(&self) -> Vec<PeerInfo> {
        ffi::handle_get_peers(&self.inner)
    }

    /// Get trackers
    pub fn trackers(&self) -> Vec<TrackerInfo> {
        ffi::handle_get_trackers(&self.inner)
    }

    /// Add tracker
    pub fn add_tracker(&mut self, url: &str, tier: i32) {
        ffi::handle_add_tracker(self.inner.pin_mut(), url, tier)
    }

    /// Replace trackers
    pub fn replace_trackers(&mut self, urls: &[String]) {
        ffi::handle_replace_trackers(self.inner.pin_mut(), urls)
    }

    /// Force recheck
    pub fn force_recheck(&mut self) {
        ffi::handle_force_recheck(self.inner.pin_mut())
    }

    /// Force reannounce
    pub fn force_reannounce(&mut self) {
        ffi::handle_force_reannounce(self.inner.pin_mut())
    }

    /// Force a tracker announce with libtorrent's reannounce flags.
    pub fn force_reannounce_with_flags(&mut self, high_priority: bool, ignore_min_interval: bool) {
        ffi::handle_force_reannounce_with_flags(
            self.inner.pin_mut(),
            high_priority,
            ignore_min_interval,
        )
    }

    /// Force DHT announce
    pub fn force_dht_announce(&mut self) {
        ffi::handle_force_dht_announce(self.inner.pin_mut())
    }

    /// Ask libtorrent to flush its disk cache to the OS files. Verified
    /// pieces can otherwise sit in the write cache while direct file reads
    /// observe preallocated zeros.
    pub fn flush_cache(&mut self) {
        ffi::handle_flush_cache(self.inner.pin_mut())
    }

    /// Move storage to new path
    pub fn move_storage(&mut self, new_path: &str) -> Result<(), cxx::Exception> {
        ffi::handle_move_storage(self.inner.pin_mut(), new_path)
    }

    /// Set upload limit
    pub fn set_upload_limit(&mut self, limit: i32) {
        ffi::handle_set_upload_limit(self.inner.pin_mut(), limit)
    }

    /// Set download limit
    pub fn set_download_limit(&mut self, limit: i32) {
        ffi::handle_set_download_limit(self.inner.pin_mut(), limit)
    }

    /// Get metadata as .torrent bytes
    pub fn get_metadata(&self) -> Vec<u8> {
        ffi::handle_get_metadata(&self.inner)
    }

    /// Make magnet URI
    pub fn make_magnet_uri(&self) -> String {
        ffi::make_magnet_uri(&self.inner)
    }

    /// Request piece data asynchronously (result comes via read_piece_alert)
    /// This is used for memory-first streaming - libtorrent reads the piece
    /// and sends the data via alert before/without disk I/O
    pub fn read_piece(&mut self, piece: i32) -> Result<(), cxx::Exception> {
        ffi::handle_read_piece(self.inner.pin_mut(), piece)
    }
}

/// Get libtorrent version string
pub fn version() -> String {
    ffi::libtorrent_version()
}

/// Parse a magnet URI into AddTorrentParams
pub fn parse_magnet(uri: &str) -> Result<AddTorrentParams, cxx::Exception> {
    ffi::parse_magnet_uri(uri)
}
