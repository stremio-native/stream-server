use crate::routes::compat;
use crate::state::AppState;
use crate::updater::version::UpdateChannel;
use axum::{
    Json,
    extract::{Query, RawQuery, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use enginefs::backend::{
    TorrentEncryptionMode, TorrentHandle, TorrentPrivacyConfig, TorrentProxyType,
};
use serde_json::{Value, json};
use std::collections::HashMap;

async fn combined_engine_stats(
    state: &AppState,
) -> HashMap<String, enginefs::backend::EngineStats> {
    let mut engines = state.engine.get_all_statistics().await;
    let download_engines = state.download_engine.get_all_statistics().await;

    // Direct playback/download requests use download_engine when disk-backed
    // mode is available. Prefer those stats for duplicate info hashes so UI
    // speed/peer counters reflect the active transfer.
    for (hash, stats) in download_engines {
        engines.insert(hash, stats);
    }

    engines
}

#[derive(serde::Deserialize)]
pub struct StatsParams {
    pub sys: Option<String>, // "1"
}

pub async fn get_stats(
    State(state): State<AppState>,
    Query(params): Query<StatsParams>,
) -> impl IntoResponse {
    let engines = combined_engine_stats(&state).await;

    // Convert engines HashMap to Value
    let mut root: serde_json::Map<String, Value> = serde_json::Map::new();

    for (hash, stats) in engines {
        root.insert(hash, serde_json::to_value(stats).unwrap_or(Value::Null));
    }

    if params.sys.as_deref() == Some("1") {
        let mut system = sysinfo::System::new_all();
        system.refresh_all();
        let loadavg = sysinfo::System::load_average();
        root.insert(
            "sys".to_string(),
            json!({
                "loadavg": [loadavg.one, loadavg.five, loadavg.fifteen],
                "cpus": system.cpus().iter().map(|cpu| {
                    json!({
                        "model": cpu.brand(),
                        "speed": cpu.frequency(),
                    })
                }).collect::<Vec<_>>()
            }),
        );
    }

    Json(Value::Object(root))
}

pub async fn heartbeat() -> impl IntoResponse {
    Json(json!({ "success": true }))
}

pub async fn network_info() -> impl IntoResponse {
    let mut interfaces = Vec::new();
    if let Ok(if_addrs) = if_addrs::get_if_addrs() {
        for iface in if_addrs {
            if !iface.is_loopback()
                && let if_addrs::IfAddr::V4(addr) = iface.addr
            {
                interfaces.push(addr.ip.to_string());
            }
        }
    }
    Json(json!({ "availableInterfaces": interfaces }))
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct ServerSettings {
    #[serde(rename = "appPath")]
    pub app_path: String,
    #[serde(rename = "serverVersion")]
    pub server_version: String,
    #[serde(rename = "cacheRoot")]
    pub cache_root: String,
    #[serde(rename = "cacheSize")]
    pub cache_size: f64,
    #[serde(rename = "proxyStreamsEnabled")]
    pub proxy_streams_enabled: bool,
    #[serde(rename = "btMaxConnections")]
    pub bt_max_connections: u64,
    #[serde(rename = "btHandshakeTimeout")]
    pub bt_handshake_timeout: u64,
    #[serde(rename = "btRequestTimeout")]
    pub bt_request_timeout: u64,
    #[serde(rename = "btDownloadSpeedSoftLimit")]
    pub bt_download_speed_soft_limit: f64,
    #[serde(rename = "btDownloadSpeedHardLimit")]
    pub bt_download_speed_hard_limit: f64,
    #[serde(rename = "btMinPeersForStable")]
    pub bt_min_peers_for_stable: u64,
    #[serde(rename = "btEnableDht", default = "default_bt_enable_dht")]
    pub bt_enable_dht: bool,
    #[serde(rename = "btEnablePex", default = "default_bt_enable_pex")]
    pub bt_enable_pex: bool,
    #[serde(rename = "btEnableLsd", default = "default_bt_enable_lsd")]
    pub bt_enable_lsd: bool,
    #[serde(rename = "btEncryptionMode", default)]
    pub bt_encryption_mode: TorrentEncryptionMode,
    #[serde(rename = "btAnonymousMode", default = "default_bt_anonymous_mode")]
    pub bt_anonymous_mode: bool,
    #[serde(
        rename = "btAllowMultipleConnectionsPerIp",
        default = "default_bt_allow_multiple_connections_per_ip"
    )]
    pub bt_allow_multiple_connections_per_ip: bool,
    #[serde(
        rename = "btListenInterfaces",
        default = "default_bt_listen_interfaces"
    )]
    pub bt_listen_interfaces: String,
    #[serde(rename = "btOutgoingInterfaces", default)]
    pub bt_outgoing_interfaces: String,
    #[serde(rename = "btOutgoingPort", default)]
    pub bt_outgoing_port: u16,
    #[serde(rename = "btNumOutgoingPorts", default)]
    pub bt_num_outgoing_ports: u16,
    #[serde(rename = "btProxyType", default)]
    pub bt_proxy_type: TorrentProxyType,
    #[serde(rename = "btProxyHost", default)]
    pub bt_proxy_host: String,
    #[serde(rename = "btProxyPort", default)]
    pub bt_proxy_port: u16,
    #[serde(rename = "btProxyUsername", default)]
    pub bt_proxy_username: String,
    #[serde(rename = "btProxyPassword", default)]
    pub bt_proxy_password: String,
    #[serde(rename = "btProxyHostnames", default = "default_bt_proxy_hostnames")]
    pub bt_proxy_hostnames: bool,
    #[serde(
        rename = "btProxyPeerConnections",
        default = "default_bt_proxy_peer_connections"
    )]
    pub bt_proxy_peer_connections: bool,
    #[serde(
        rename = "btProxyTrackerConnections",
        default = "default_bt_proxy_tracker_connections"
    )]
    pub bt_proxy_tracker_connections: bool,
    #[serde(
        rename = "btProxySendHostInConnect",
        default = "default_bt_proxy_send_host_in_connect"
    )]
    pub bt_proxy_send_host_in_connect: bool,
    #[serde(
        rename = "btValidateHttpsTrackers",
        default = "default_bt_validate_https_trackers"
    )]
    pub bt_validate_https_trackers: bool,
    #[serde(rename = "btSsrfMitigation", default = "default_bt_ssrf_mitigation")]
    pub bt_ssrf_mitigation: bool,
    #[serde(rename = "remoteHttps")]
    pub remote_https: Option<String>,
    #[serde(rename = "transcodeProfile")]
    pub transcode_profile: Option<String>,
    #[serde(rename = "autoUpdateEnabled", default = "default_auto_update_enabled")]
    pub auto_update_enabled: bool,
    #[serde(rename = "updateChannel", default)]
    pub update_channel: UpdateChannel,
    #[serde(
        rename = "updateCheckIntervalHours",
        default = "default_update_check_interval_hours"
    )]
    pub update_check_interval_hours: u64,

    /// Cached list of fastest trackers (ranked by RTT)
    #[serde(rename = "cachedTrackers", default)]
    pub cached_trackers: Vec<String>,

    /// Unix timestamp (seconds) when trackers were last updated
    #[serde(rename = "trackersLastUpdated", default)]
    pub trackers_last_updated: i64,

    /// URL to fetch public tracker list (configurable)
    #[serde(rename = "trackersSourceUrl", default = "default_trackers_url")]
    pub trackers_source_url: String,

    /// When true (default), torrents continue seeding after download
    /// completes, improving swarm health and download speeds from reciprocal
    /// peers.  When false, torrents are paused once their download finishes.
    #[serde(rename = "seedingEnabled", default = "default_seeding_enabled")]
    pub seeding_enabled: bool,
}

pub fn default_trackers_url() -> String {
    "https://raw.githubusercontent.com/ngosang/trackerslist/master/trackers_best.txt".to_string()
}

pub fn default_seeding_enabled() -> bool {
    true
}

pub fn default_auto_update_enabled() -> bool {
    true
}

pub fn default_update_check_interval_hours() -> u64 {
    6
}

pub fn default_bt_enable_dht() -> bool {
    true
}

pub fn default_bt_enable_pex() -> bool {
    true
}

pub fn default_bt_enable_lsd() -> bool {
    true
}

pub fn default_bt_anonymous_mode() -> bool {
    false
}

pub fn default_bt_allow_multiple_connections_per_ip() -> bool {
    false
}

pub fn default_bt_listen_interfaces() -> String {
    enginefs::backend::TorrentPrivacyConfig::default().bt_listen_interfaces
}

pub fn default_bt_proxy_hostnames() -> bool {
    true
}

pub fn default_bt_proxy_peer_connections() -> bool {
    false
}

pub fn default_bt_proxy_tracker_connections() -> bool {
    true
}

pub fn default_bt_proxy_send_host_in_connect() -> bool {
    false
}

pub fn default_bt_validate_https_trackers() -> bool {
    true
}

pub fn default_bt_ssrf_mitigation() -> bool {
    true
}

fn parse_torrent_encryption_mode(value: &Value) -> Option<TorrentEncryptionMode> {
    if let Some(code) = value.as_u64() {
        return match code {
            0 => Some(TorrentEncryptionMode::Allow),
            1 => Some(TorrentEncryptionMode::Require),
            2 => Some(TorrentEncryptionMode::Disable),
            _ => None,
        };
    }

    let raw = value.as_str()?.trim();
    if raw.eq_ignore_ascii_case("allow")
        || raw.eq_ignore_ascii_case("allowEncryption")
        || raw.eq_ignore_ascii_case("enabled")
    {
        Some(TorrentEncryptionMode::Allow)
    } else if raw.eq_ignore_ascii_case("require")
        || raw.eq_ignore_ascii_case("requireEncryption")
        || raw.eq_ignore_ascii_case("forced")
    {
        Some(TorrentEncryptionMode::Require)
    } else if raw.eq_ignore_ascii_case("disable")
        || raw.eq_ignore_ascii_case("disableEncryption")
        || raw.eq_ignore_ascii_case("disabled")
    {
        Some(TorrentEncryptionMode::Disable)
    } else {
        None
    }
}

fn parse_torrent_proxy_type(value: &Value) -> Option<TorrentProxyType> {
    if let Some(code) = value.as_u64() {
        return match code {
            0 => Some(TorrentProxyType::None),
            1 => Some(TorrentProxyType::Socks4),
            2 => Some(TorrentProxyType::Socks5),
            3 => Some(TorrentProxyType::Socks5Password),
            4 => Some(TorrentProxyType::Http),
            5 => Some(TorrentProxyType::HttpPassword),
            _ => None,
        };
    }

    let raw = value.as_str()?.trim();
    if raw.eq_ignore_ascii_case("none") || raw.eq_ignore_ascii_case("disabled") {
        Some(TorrentProxyType::None)
    } else if raw.eq_ignore_ascii_case("socks4") {
        Some(TorrentProxyType::Socks4)
    } else if raw.eq_ignore_ascii_case("socks5") {
        Some(TorrentProxyType::Socks5)
    } else if raw.eq_ignore_ascii_case("socks5Password")
        || raw.eq_ignore_ascii_case("socks5_password")
        || raw.eq_ignore_ascii_case("socks5_pw")
    {
        Some(TorrentProxyType::Socks5Password)
    } else if raw.eq_ignore_ascii_case("http") {
        Some(TorrentProxyType::Http)
    } else if raw.eq_ignore_ascii_case("httpPassword")
        || raw.eq_ignore_ascii_case("http_password")
        || raw.eq_ignore_ascii_case("http_pw")
    {
        Some(TorrentProxyType::HttpPassword)
    } else {
        None
    }
}

fn value_as_u16(value: &Value) -> Option<u16> {
    value.as_u64().and_then(|n| u16::try_from(n).ok())
}

fn update_bool_setting(obj: &serde_json::Map<String, Value>, key: &str, target: &mut bool) {
    if let Some(value) = obj.get(key).and_then(Value::as_bool) {
        *target = value;
    }
}

fn update_u16_setting(obj: &serde_json::Map<String, Value>, key: &str, target: &mut u16) {
    if let Some(value) = obj.get(key).and_then(value_as_u16) {
        *target = value;
    }
}

fn update_string_setting(
    obj: &serde_json::Map<String, Value>,
    key: &str,
    target: &mut String,
    trim: bool,
    allow_empty: bool,
) {
    if let Some(value) = obj.get(key).and_then(Value::as_str) {
        let value = if trim { value.trim() } else { value };
        if allow_empty || !value.is_empty() {
            *target = value.to_string();
        }
    }
}

/// Reads `key` from the environment; falls back to `default` if unset or unparseable.
fn env_bool(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(v) => match v.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => default,
        },
        Err(_) => default,
    }
}

fn env_parsed<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

fn env_string(key: &str, default: String) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or(default)
}

fn env_encryption_mode(key: &str, default: TorrentEncryptionMode) -> TorrentEncryptionMode {
    std::env::var(key)
        .ok()
        .and_then(|v| parse_torrent_encryption_mode(&Value::String(v)))
        .unwrap_or(default)
}

fn env_proxy_type(key: &str, default: TorrentProxyType) -> TorrentProxyType {
    std::env::var(key)
        .ok()
        .and_then(|v| parse_torrent_proxy_type(&Value::String(v)))
        .unwrap_or(default)
}

impl Default for ServerSettings {
    fn default() -> Self {
        let cache_root = std::env::var("STREMIO_CACHE_ROOT")
            .or_else(|_| std::env::var("HOME").map(|h| format!("{}/.cache/stremio-server", h)))
            .unwrap_or_else(|_| {
                std::env::temp_dir()
                    .join("stremio-cache")
                    .to_string_lossy()
                    .to_string()
            });

        // Every field below is overridable via a STREAM_SERVER_* env var, read
        // once here at first startup (before any settings.json exists). Once
        // settings.json exists, it takes over and the /settings API is the
        // way to change things -- these env vars only shape the *initial*
        // defaults for a fresh deployment. Baseline values below are tuned
        // for a single-stream home connection in the ~100Mbps class:
        // bt_max_connections=800 already saturates the per-torrent cap
        // (see enginefs::backend::TorrentSpeedProfile::effective_connection_limits,
        // which clamps per-torrent connections to 200 once the global max
        // reaches 800), so raising it further only helps if you're running
        // several torrents at once -- it doesn't speed up a single stream.
        // bt_download_speed_hard_limit defaults to ~90% of a 100Mbps line
        // (11.25 MB/s) to leave headroom against bufferbloat/self-induced
        // congestion, which otherwise costs more effective throughput (and
        // playback smoothness) than it gains; set it to 0 to go unlimited.
        Self {
            app_path: std::env::current_exe()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|_| "/usr/bin/stremio-server".to_string()),
            server_version: env!("CARGO_PKG_VERSION").to_string(),
            cache_root,
            cache_size: env_parsed("STREAM_SERVER_CACHE_SIZE_BYTES", 10.0 * 1024.0 * 1024.0 * 1024.0), // 10GB
            proxy_streams_enabled: env_bool("STREAM_SERVER_PROXY_STREAMS_ENABLED", false),
            bt_max_connections: env_parsed(
                "STREAM_SERVER_BT_MAX_CONNECTIONS",
                enginefs::backend::DEFAULT_BT_MAX_CONNECTIONS,
            ),
            bt_handshake_timeout: env_parsed("STREAM_SERVER_BT_HANDSHAKE_TIMEOUT_MS", 20000),
            bt_request_timeout: env_parsed("STREAM_SERVER_BT_REQUEST_TIMEOUT_MS", 10000),
            bt_download_speed_soft_limit: env_parsed(
                "STREAM_SERVER_BT_DOWNLOAD_SPEED_SOFT_LIMIT",
                0.0,
            ),
            bt_download_speed_hard_limit: env_parsed(
                "STREAM_SERVER_BT_DOWNLOAD_SPEED_HARD_LIMIT",
                11.25 * 1024.0 * 1024.0, // ~90Mbps of a 100Mbps line, in bytes/sec
            ),
            bt_min_peers_for_stable: env_parsed("STREAM_SERVER_BT_MIN_PEERS_FOR_STABLE", 3),
            bt_enable_dht: env_bool("STREAM_SERVER_BT_ENABLE_DHT", default_bt_enable_dht()),
            bt_enable_pex: env_bool("STREAM_SERVER_BT_ENABLE_PEX", default_bt_enable_pex()),
            bt_enable_lsd: env_bool("STREAM_SERVER_BT_ENABLE_LSD", default_bt_enable_lsd()),
            bt_encryption_mode: env_encryption_mode(
                "STREAM_SERVER_BT_ENCRYPTION_MODE",
                TorrentEncryptionMode::default(),
            ),
            bt_anonymous_mode: env_bool(
                "STREAM_SERVER_BT_ANONYMOUS_MODE",
                default_bt_anonymous_mode(),
            ),
            bt_allow_multiple_connections_per_ip: env_bool(
                "STREAM_SERVER_BT_ALLOW_MULTIPLE_CONNECTIONS_PER_IP",
                true,
            ),
            bt_listen_interfaces: env_string(
                "STREAM_SERVER_BT_LISTEN_INTERFACES",
                default_bt_listen_interfaces(),
            ),
            bt_outgoing_interfaces: env_string("STREAM_SERVER_BT_OUTGOING_INTERFACES", String::new()),
            bt_outgoing_port: env_parsed("STREAM_SERVER_BT_OUTGOING_PORT", 0),
            bt_num_outgoing_ports: env_parsed("STREAM_SERVER_BT_NUM_OUTGOING_PORTS", 0),
            bt_proxy_type: env_proxy_type("STREAM_SERVER_BT_PROXY_TYPE", TorrentProxyType::default()),
            bt_proxy_host: env_string("STREAM_SERVER_BT_PROXY_HOST", String::new()),
            bt_proxy_port: env_parsed("STREAM_SERVER_BT_PROXY_PORT", 0),
            bt_proxy_username: env_string("STREAM_SERVER_BT_PROXY_USERNAME", String::new()),
            bt_proxy_password: env_string("STREAM_SERVER_BT_PROXY_PASSWORD", String::new()),
            bt_proxy_hostnames: env_bool(
                "STREAM_SERVER_BT_PROXY_HOSTNAMES",
                default_bt_proxy_hostnames(),
            ),
            bt_proxy_peer_connections: env_bool(
                "STREAM_SERVER_BT_PROXY_PEER_CONNECTIONS",
                default_bt_proxy_peer_connections(),
            ),
            bt_proxy_tracker_connections: env_bool(
                "STREAM_SERVER_BT_PROXY_TRACKER_CONNECTIONS",
                default_bt_proxy_tracker_connections(),
            ),
            bt_proxy_send_host_in_connect: env_bool(
                "STREAM_SERVER_BT_PROXY_SEND_HOST_IN_CONNECT",
                default_bt_proxy_send_host_in_connect(),
            ),
            bt_validate_https_trackers: env_bool(
                "STREAM_SERVER_BT_VALIDATE_HTTPS_TRACKERS",
                default_bt_validate_https_trackers(),
            ),
            bt_ssrf_mitigation: env_bool(
                "STREAM_SERVER_BT_SSRF_MITIGATION",
                default_bt_ssrf_mitigation(),
            ),
            remote_https: None,
            transcode_profile: None,
            auto_update_enabled: default_auto_update_enabled(),
            update_channel: UpdateChannel::default(),
            update_check_interval_hours: default_update_check_interval_hours(),
            cached_trackers: Vec::new(),
            trackers_last_updated: 0,
            trackers_source_url: default_trackers_url(),
            seeding_enabled: env_bool("STREAM_SERVER_SEEDING_ENABLED", default_seeding_enabled()),
        }
    }
}

/// Returns server settings in the SettingsResponse format expected by stremio-core
/// Response format: { "baseUrl": "http://...", "values": { ...settings } }
pub async fn get_settings(State(state): State<AppState>) -> impl IntoResponse {
    let settings = state.settings.read().await;
    Json(json!({
        "baseUrl": state.base_url.clone(),
        "options": [],
        "values": settings.clone()
    }))
}

pub async fn update_settings(state: &AppState, payload: &Value) -> anyhow::Result<()> {
    tracing::debug!("update_settings: received payload: {:?}", payload);

    // Merge with existing settings
    let mut settings = state.settings.write().await;

    if let Some(obj) = payload.as_object() {
        // Update fields that are present in the payload
        if let Some(v) = obj.get("transcodeProfile") {
            if v.is_null() {
                settings.transcode_profile = None;
            } else if let Some(s) = v.as_str() {
                settings.transcode_profile = Some(s.to_string());
            }
        }
        if let Some(v) = obj.get("cacheSize") {
            if v.is_null() {
                settings.cache_size = 0.0;
            } else if let Some(n) = v.as_f64() {
                settings.cache_size = n;
            }
        }
        if let Some(v) = obj.get("cacheRoot")
            && let Some(s) = v.as_str()
        {
            settings.cache_root = s.to_string();
        }
        if let Some(v) = obj.get("proxyStreamsEnabled")
            && let Some(b) = v.as_bool()
        {
            settings.proxy_streams_enabled = b;
        }
        if let Some(v) = obj.get("btMaxConnections")
            && let Some(n) = v.as_u64()
        {
            settings.bt_max_connections =
                if n == 0 || n >= enginefs::backend::LEGACY_UNLIMITED_BT_MAX_CONNECTIONS {
                    enginefs::backend::DEFAULT_BT_MAX_CONNECTIONS
                } else {
                    n
                };
        }
        if let Some(v) = obj.get("btHandshakeTimeout")
            && let Some(n) = v.as_u64()
        {
            settings.bt_handshake_timeout = n;
        }
        if let Some(v) = obj.get("btRequestTimeout")
            && let Some(n) = v.as_u64()
        {
            settings.bt_request_timeout = n;
        }
        if let Some(v) = obj.get("btDownloadSpeedSoftLimit")
            && let Some(n) = v.as_f64()
        {
            settings.bt_download_speed_soft_limit = n;
        }
        if let Some(v) = obj.get("btDownloadSpeedHardLimit")
            && let Some(n) = v.as_f64()
        {
            settings.bt_download_speed_hard_limit = n;
        }
        if let Some(v) = obj.get("btMinPeersForStable")
            && let Some(n) = v.as_u64()
        {
            settings.bt_min_peers_for_stable = n;
        }
        update_bool_setting(obj, "btEnableDht", &mut settings.bt_enable_dht);
        update_bool_setting(obj, "btEnablePex", &mut settings.bt_enable_pex);
        update_bool_setting(obj, "btEnableLsd", &mut settings.bt_enable_lsd);
        if let Some(mode) = obj
            .get("btEncryptionMode")
            .and_then(parse_torrent_encryption_mode)
        {
            settings.bt_encryption_mode = mode;
        }
        update_bool_setting(obj, "btAnonymousMode", &mut settings.bt_anonymous_mode);
        update_bool_setting(
            obj,
            "btAllowMultipleConnectionsPerIp",
            &mut settings.bt_allow_multiple_connections_per_ip,
        );
        update_string_setting(
            obj,
            "btListenInterfaces",
            &mut settings.bt_listen_interfaces,
            true,
            false,
        );
        update_string_setting(
            obj,
            "btOutgoingInterfaces",
            &mut settings.bt_outgoing_interfaces,
            true,
            true,
        );
        update_u16_setting(obj, "btOutgoingPort", &mut settings.bt_outgoing_port);
        update_u16_setting(
            obj,
            "btNumOutgoingPorts",
            &mut settings.bt_num_outgoing_ports,
        );
        if let Some(proxy_type) = obj.get("btProxyType").and_then(parse_torrent_proxy_type) {
            settings.bt_proxy_type = proxy_type;
        }
        update_string_setting(obj, "btProxyHost", &mut settings.bt_proxy_host, true, true);
        update_u16_setting(obj, "btProxyPort", &mut settings.bt_proxy_port);
        update_string_setting(
            obj,
            "btProxyUsername",
            &mut settings.bt_proxy_username,
            false,
            true,
        );
        update_string_setting(
            obj,
            "btProxyPassword",
            &mut settings.bt_proxy_password,
            false,
            true,
        );
        update_bool_setting(obj, "btProxyHostnames", &mut settings.bt_proxy_hostnames);
        update_bool_setting(
            obj,
            "btProxyPeerConnections",
            &mut settings.bt_proxy_peer_connections,
        );
        update_bool_setting(
            obj,
            "btProxyTrackerConnections",
            &mut settings.bt_proxy_tracker_connections,
        );
        update_bool_setting(
            obj,
            "btProxySendHostInConnect",
            &mut settings.bt_proxy_send_host_in_connect,
        );
        update_bool_setting(
            obj,
            "btValidateHttpsTrackers",
            &mut settings.bt_validate_https_trackers,
        );
        update_bool_setting(obj, "btSsrfMitigation", &mut settings.bt_ssrf_mitigation);
        if let Some(v) = obj.get("remoteHttps") {
            if v.is_null() {
                settings.remote_https = None;
            } else if let Some(s) = v.as_str() {
                settings.remote_https = Some(s.to_string());
            }
        }
        if let Some(v) = obj.get("autoUpdateEnabled")
            && let Some(enabled) = v.as_bool()
        {
            settings.auto_update_enabled = enabled;
        }
        if let Some(v) = obj.get("updateChannel").and_then(|v| v.as_str()) {
            settings.update_channel = if v.eq_ignore_ascii_case("prerelease") {
                UpdateChannel::Prerelease
            } else {
                UpdateChannel::Stable
            };
        }
        if let Some(v) = obj.get("updateCheckIntervalHours")
            && let Some(hours) = v.as_u64()
        {
            settings.update_check_interval_hours = hours.max(1);
        }
        if let Some(v) = obj.get("seedingEnabled")
            && let Some(enabled) = v.as_bool()
        {
            settings.seeding_enabled = enabled;
        }
    }

    let seeding_enabled = settings.seeding_enabled;

    // Build new speed profile from updated settings
    let new_profile = enginefs::backend::TorrentSpeedProfile {
        bt_download_speed_hard_limit: settings.bt_download_speed_hard_limit,
        bt_download_speed_soft_limit: settings.bt_download_speed_soft_limit,
        bt_handshake_timeout: settings.bt_handshake_timeout,
        bt_max_connections: settings.bt_max_connections,
        bt_min_peers_for_stable: settings.bt_min_peers_for_stable,
        bt_request_timeout: settings.bt_request_timeout,
    };
    let new_privacy = TorrentPrivacyConfig {
        bt_enable_dht: settings.bt_enable_dht,
        bt_enable_pex: settings.bt_enable_pex,
        bt_enable_lsd: settings.bt_enable_lsd,
        bt_encryption_mode: settings.bt_encryption_mode,
        bt_anonymous_mode: settings.bt_anonymous_mode,
        bt_allow_multiple_connections_per_ip: settings.bt_allow_multiple_connections_per_ip,
        bt_listen_interfaces: settings.bt_listen_interfaces.clone(),
        bt_outgoing_interfaces: settings.bt_outgoing_interfaces.clone(),
        bt_outgoing_port: settings.bt_outgoing_port,
        bt_num_outgoing_ports: settings.bt_num_outgoing_ports,
        bt_proxy_type: settings.bt_proxy_type,
        bt_proxy_host: settings.bt_proxy_host.clone(),
        bt_proxy_port: settings.bt_proxy_port,
        bt_proxy_username: settings.bt_proxy_username.clone(),
        bt_proxy_password: settings.bt_proxy_password.clone(),
        bt_proxy_hostnames: settings.bt_proxy_hostnames,
        bt_proxy_peer_connections: settings.bt_proxy_peer_connections,
        bt_proxy_tracker_connections: settings.bt_proxy_tracker_connections,
        bt_proxy_send_host_in_connect: settings.bt_proxy_send_host_in_connect,
        bt_validate_https_trackers: settings.bt_validate_https_trackers,
        bt_ssrf_mitigation: settings.bt_ssrf_mitigation,
    };

    // Release the write lock before saving
    drop(settings);

    // Apply updated torrent session settings dynamically.
    state
        .engine
        .update_torrent_settings(&new_profile, &new_privacy)
        .await;
    state
        .download_engine
        .update_torrent_settings(&new_profile, &new_privacy)
        .await;

    state.engine.set_seeding_enabled(seeding_enabled);
    state.download_engine.set_seeding_enabled(seeding_enabled);

    // Save to disk
    state.save_settings().await?;

    Ok(())
}

pub async fn set_settings(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> impl IntoResponse {
    match update_settings(&state, &payload).await {
        Ok(_) => Json(json!({ "success": true })),
        Err(e) => {
            tracing::error!("Failed to save settings: {}", e);
            Json(json!({ "success": false, "error": e.to_string() }))
        }
    }
}
pub async fn get_device_info() -> impl IntoResponse {
    let profiles = probe_hwaccel().await;
    Json(json!({
        "availableHardwareAccelerations": profiles
    }))
}

pub async fn hwaccel_profiler() -> impl IntoResponse {
    let profiles = probe_hwaccel().await;
    Json(json!({
        "success": true,
        "profiles": profiles
    }))
}

static HWACCEL_PROFILES: tokio::sync::OnceCell<Vec<String>> = tokio::sync::OnceCell::const_new();

pub async fn probe_hwaccel() -> Vec<String> {
    HWACCEL_PROFILES
        .get_or_init(probe_hwaccel_uncached)
        .await
        .clone()
}

async fn probe_hwaccel_uncached() -> Vec<String> {
    let mut profiles = Vec::new();
    let output = match tokio::process::Command::new("ffmpeg")
        .args(["-hide_banner", "-encoders"])
        .output()
        .await
    {
        Ok(o) => String::from_utf8_lossy(&o.stdout).to_string(),
        Err(_) => return profiles,
    };

    if output.contains("h264_nvenc") {
        profiles.push("nvenc".to_string());
        if verify_h264_encoder("h264_nvenc").await {
            profiles.push("nvenc:verified".to_string());
        }
    }
    if output.contains("h264_vaapi") {
        profiles.push("vaapi".to_string());
        if verify_h264_encoder("h264_vaapi").await {
            profiles.push("vaapi:verified".to_string());
        }
    }
    if output.contains("h264_vdpau") {
        profiles.push("vdpau".to_string());
    }
    if output.contains("h264_qsv") {
        profiles.push("qsv".to_string());
        if verify_h264_encoder("h264_qsv").await {
            profiles.push("qsv:verified".to_string());
        }
    }
    if output.contains("h264_omx") {
        profiles.push("omx".to_string());
    }
    if output.contains("h264_v4l2m2m") {
        profiles.push("v4l2m2m".to_string());
        if verify_h264_encoder("h264_v4l2m2m").await {
            profiles.push("v4l2m2m:verified".to_string());
        }
    }
    if output.contains("h264_videotoolbox") {
        profiles.push("videotoolbox".to_string());
        if verify_h264_encoder("h264_videotoolbox").await {
            profiles.push("videotoolbox:verified".to_string());
        }
    }
    if output.contains("h264_mediacodec") {
        profiles.push("mediacodec".to_string());
    }

    profiles
}

async fn verify_h264_encoder(encoder: &str) -> bool {
    let mut cmd = tokio::process::Command::new("ffmpeg");
    cmd.args([
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc2=size=64x64:rate=1:duration=1",
        "-frames:v",
        "1",
        "-an",
        "-pix_fmt",
        "yuv420p",
        "-c:v",
        encoder,
        "-f",
        "null",
        "-",
    ]);

    let output = match tokio::time::timeout(std::time::Duration::from_secs(5), cmd.output()).await {
        Ok(Ok(output)) => output,
        Ok(Err(err)) => {
            tracing::debug!(encoder, error = %err, "hardware encoder verification failed to spawn");
            return false;
        }
        Err(_) => {
            tracing::debug!(encoder, "hardware encoder verification timed out");
            return false;
        }
    };

    if output.status.success() {
        tracing::info!(encoder, "hardware encoder verified");
        true
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        tracing::debug!(
            encoder,
            status = ?output.status.code(),
            stderr = %stderr.trim(),
            "hardware encoder listed by FFmpeg but failed verification"
        );
        false
    }
}

pub async fn get_https(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    let ip_address = match params.get("ipAddress") {
        Some(ip) => ip,
        None => return (StatusCode::BAD_REQUEST, "Missing ipAddress").into_response(),
    };
    let auth_key = match params.get("authKey") {
        Some(key) => key,
        None => return (StatusCode::BAD_REQUEST, "Missing authKey").into_response(),
    };

    let client = reqwest::Client::new();
    let api_url = "https://api.strem.io/api/certificateGet";

    let payload = json!({
        "authKey": auth_key,
        "ipAddress": ip_address
    });

    let resp = match client.post(api_url).json(&payload).send().await {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("API error: {}", e),
            )
                .into_response();
        }
    };

    let json: serde_json::Value = match resp.json().await {
        Ok(j) => j,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("JSON error: {}", e),
            )
                .into_response();
        }
    };

    // Parity with http_client_804.js: parse certificate response
    let result = &json["result"];
    if result.is_null() {
        return (StatusCode::NOT_FOUND, "No certificate found in response").into_response();
    }

    let cert_data_str = match result["certificate"].as_str() {
        Some(s) => s,
        None => return (StatusCode::NOT_FOUND, "Certificate field missing").into_response(),
    };

    let cert_data: serde_json::Value = match serde_json::from_str(cert_data_str) {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to parse inner certificate JSON",
            )
                .into_response();
        }
    };

    // Save to disk for main.rs HTTPS listener
    if let (Some(cert), Some(key)) = (
        cert_data["certificate"].as_str(),
        cert_data["privateKey"].as_str(),
    ) {
        let cert_path = state.config_dir.join("https-cert.pem");
        let key_path = state.config_dir.join("https-key.pem");

        if let Err(e) = tokio::fs::write(&cert_path, cert).await {
            tracing::error!("Failed to write https-cert.pem: {}", e);
        }
        if let Err(e) = tokio::fs::write(&key_path, key).await {
            tracing::error!("Failed to write https-key.pem: {}", e);
        }
        tracing::info!("Saved HTTPS certificates to {:?}", state.config_dir);
    }

    let domain = format!(
        "{}-{}",
        ip_address.replace(".", "-"),
        cert_data["commonName"]
            .as_str()
            .unwrap_or("")
            .replace("*", "")
    );

    // We should save this to disk, but for the API response:
    Json(json!({
        "ipAddress": ip_address,
        "domain": domain,
        "port": state.http_addr.port()
    }))
    .into_response()
}

pub async fn get_samples(
    axum::extract::Path(filename): axum::extract::Path<String>,
) -> impl IntoResponse {
    // Parity with /samples/:filename
    (
        StatusCode::NOT_FOUND,
        format!("Sample {} not found", filename),
    )
        .into_response()
}

pub async fn get_engine_stats(
    State(state): State<AppState>,
    axum::extract::Path(info_hash): axum::extract::Path<String>,
) -> Response {
    let info_hash = info_hash.to_lowercase();

    // Try to get existing engine, or auto-create from info hash
    let engine = if let Some(e) = state.download_engine.get_engine(&info_hash).await {
        e
    } else if let Some(e) = state.engine.get_engine(&info_hash).await {
        e
    } else {
        tracing::info!("Auto-creating engine for stats request: {}", info_hash);
        let magnet = format!("magnet:?xt=urn:btih:{}", info_hash);
        let source = enginefs::backend::TorrentSource::Url(magnet);
        match state.download_engine.add_torrent(source, None).await {
            Ok(e) => e,
            Err(e) => {
                tracing::error!("Failed to create engine: {}", e);
                return (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to create engine: {}", e),
                )
                    .into_response();
            }
        }
    };

    let stats = engine.get_statistics().await;
    Json(serde_json::to_value(stats).unwrap()).into_response()
}

pub async fn get_file_stats(
    State(state): State<AppState>,
    axum::extract::Path((info_hash, requested_idx)): axum::extract::Path<(String, String)>,
    RawQuery(query_str): RawQuery,
) -> Response {
    let info_hash = info_hash.to_lowercase();

    // Try to get existing engine, or auto-create from info hash
    let engine = if let Some(e) = state.download_engine.get_engine(&info_hash).await {
        e
    } else if let Some(e) = state.engine.get_engine(&info_hash).await {
        e
    } else {
        tracing::info!("Auto-creating engine for file stats request: {}", info_hash);
        let magnet = format!("magnet:?xt=urn:btih:{}", info_hash);
        let source = enginefs::backend::TorrentSource::Url(magnet);
        match state.download_engine.add_torrent(source, None).await {
            Ok(e) => e,
            Err(e) => {
                tracing::error!("Failed to create engine: {}", e);
                return (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to create engine: {}", e),
                )
                    .into_response();
            }
        }
    };

    let files = engine.handle.get_files().await;
    let candidates = files
        .iter()
        .enumerate()
        .map(|(index, file)| compat::FileCandidate {
            index,
            name: file.name.clone(),
            length: file.length,
        })
        .collect::<Vec<_>>();
    let filters = compat::query_values(query_str.as_deref(), "f");
    let idx = match compat::resolve_file_idx(&requested_idx, &candidates, &filters) {
        Ok(idx) => idx,
        Err(err) => {
            return (axum::http::StatusCode::NOT_FOUND, err).into_response();
        }
    };
    state
        .stream_engine()
        .refresh_existing_hls_playback(&info_hash, idx, "stats-json")
        .await;

    let mut stats = engine.get_statistics().await;
    if idx >= stats.files.len() {
        return (
            axum::http::StatusCode::NOT_FOUND,
            "File index out of bounds",
        )
            .into_response();
    }
    // Report progress for the exact file the client asked about. The guess
    // inside get_statistics can resolve to a different file in a multi-file
    // torrent, and downloaded/length stays stable during cold start (unlike the
    // torrent's total_wanted set, which briefly collapses to the metadata
    // window and spikes the percentage just before playback).
    let file = &stats.files[idx];
    stats.stream_name = file.name.clone();
    stats.stream_len = file.length;
    stats.stream_progress = if file.length > 0 {
        (file.downloaded as f64 / file.length as f64).min(1.0)
    } else {
        0.0
    };
    Json(serde_json::to_value(stats).unwrap()).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_version_default_uses_crate_version() {
        let settings = ServerSettings::default();
        assert_eq!(settings.server_version, env!("CARGO_PKG_VERSION"));
    }
}
