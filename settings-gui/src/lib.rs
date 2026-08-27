use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use slint::{ComponentHandle, ModelRc, SharedString, VecModel, Weak};
use std::{
    collections::{HashMap, HashSet, hash_map::DefaultHasher},
    fs,
    hash::{Hash, Hasher},
    path::PathBuf,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::runtime::{Builder as RuntimeBuilder, Handle};

slint::include_modules!();

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SettingsPayload {
    #[serde(rename = "cacheRoot", default)]
    pub cache_root: String,
    #[serde(rename = "cacheSize", default)]
    pub cache_size: f64,
    #[serde(rename = "proxyStreamsEnabled", default)]
    pub proxy_streams_enabled: bool,
    #[serde(rename = "btMaxConnections", default)]
    pub bt_max_connections: u64,
    #[serde(rename = "btHandshakeTimeout", default)]
    pub bt_handshake_timeout: u64,
    #[serde(rename = "btRequestTimeout", default)]
    pub bt_request_timeout: u64,
    #[serde(rename = "btDownloadSpeedSoftLimit", default)]
    pub bt_download_speed_soft_limit: f64,
    #[serde(rename = "btDownloadSpeedHardLimit", default)]
    pub bt_download_speed_hard_limit: f64,
    #[serde(rename = "btMinPeersForStable", default)]
    pub bt_min_peers_for_stable: u64,
    #[serde(rename = "btEnableDht", default)]
    pub bt_enable_dht: bool,
    #[serde(rename = "btEnablePex", default)]
    pub bt_enable_pex: bool,
    #[serde(rename = "btEnableLsd", default)]
    pub bt_enable_lsd: bool,
    #[serde(rename = "btEncryptionMode", default)]
    pub bt_encryption_mode: String,
    #[serde(rename = "btAnonymousMode", default)]
    pub bt_anonymous_mode: bool,
    #[serde(rename = "btAllowMultipleConnectionsPerIp", default)]
    pub bt_allow_multiple_connections_per_ip: bool,
    #[serde(rename = "btListenInterfaces", default)]
    pub bt_listen_interfaces: String,
    #[serde(rename = "btOutgoingInterfaces", default)]
    pub bt_outgoing_interfaces: String,
    #[serde(rename = "btOutgoingPort", default)]
    pub bt_outgoing_port: u16,
    #[serde(rename = "btNumOutgoingPorts", default)]
    pub bt_num_outgoing_ports: u16,
    #[serde(rename = "btProxyType", default)]
    pub bt_proxy_type: String,
    #[serde(rename = "btProxyHost", default)]
    pub bt_proxy_host: String,
    #[serde(rename = "btProxyPort", default)]
    pub bt_proxy_port: u16,
    #[serde(rename = "btProxyUsername", default)]
    pub bt_proxy_username: String,
    #[serde(rename = "btProxyPassword", default)]
    pub bt_proxy_password: String,
    #[serde(rename = "btProxyHostnames", default)]
    pub bt_proxy_hostnames: bool,
    #[serde(rename = "btProxyPeerConnections", default)]
    pub bt_proxy_peer_connections: bool,
    #[serde(rename = "btProxyTrackerConnections", default)]
    pub bt_proxy_tracker_connections: bool,
    #[serde(rename = "btProxySendHostInConnect", default)]
    pub bt_proxy_send_host_in_connect: bool,
    #[serde(rename = "btValidateHttpsTrackers", default)]
    pub bt_validate_https_trackers: bool,
    #[serde(rename = "btSsrfMitigation", default)]
    pub bt_ssrf_mitigation: bool,
    #[serde(rename = "autoUpdateEnabled", default)]
    pub auto_update_enabled: bool,
    #[serde(rename = "updateChannel", default)]
    pub update_channel: String,
    #[serde(rename = "updateCheckIntervalHours", default)]
    pub update_check_interval_hours: u64,
    #[serde(rename = "seedingEnabled", default)]
    pub seeding_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogsSnapshot {
    #[serde(default, alias = "logDir")]
    pub log_dir: String,
    #[serde(default, alias = "currentHumanLog")]
    pub current_human_log: Option<String>,
    #[serde(default, alias = "currentJsonLog")]
    pub current_json_log: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrentLogTail {
    #[serde(default)]
    pub path: Option<String>,
    pub content: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangelogPayload {
    pub current_version: String,
    pub latest_version: Option<String>,
    pub tag: Option<String>,
    pub release_url: Option<String>,
    pub published_at: Option<String>,
    pub channel: String,
    pub state: String,
    pub sections: Vec<ChangelogSectionPayload>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangelogSectionPayload {
    pub title: String,
    pub items: Vec<String>,
}

#[async_trait::async_trait]
pub trait ServerConnector: Send + Sync + 'static {
    async fn get_settings(&self) -> Result<SettingsPayload>;
    async fn apply_settings(&self, settings: SettingsPayload) -> Result<()>;
    async fn get_logs(&self) -> Result<LogsSnapshot>;
    async fn get_current_log(&self) -> Result<CurrentLogTail>;
    async fn get_changelog(&self, force: bool) -> Result<ChangelogPayload>;
    async fn export_diagnostics(&self) -> Result<Vec<u8>>;
}

pub struct HttpConnector {
    client: Client,
    server_url: String,
}

impl HttpConnector {
    pub fn new(client: Client, server_url: String) -> Self {
        Self { client, server_url }
    }
}

#[async_trait::async_trait]
impl ServerConnector for HttpConnector {
    async fn get_settings(&self) -> Result<SettingsPayload> {
        let res = self
            .client
            .get(format!("{}/settings", self.server_url))
            .send()
            .await?
            .error_for_status()?
            .json::<serde_json::Value>()
            .await?;

        let settings_value = res.get("values").cloned().unwrap_or(res);
        let payload: SettingsPayload = serde_json::from_value(settings_value)?;
        Ok(payload)
    }

    async fn apply_settings(&self, settings: SettingsPayload) -> Result<()> {
        let value = serde_json::to_value(&settings)?;
        self.client
            .post(format!("{}/settings", self.server_url))
            .json(&value)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    async fn get_logs(&self) -> Result<LogsSnapshot> {
        Ok(self
            .client
            .get(format!("{}/diagnostics/logs", self.server_url))
            .send()
            .await?
            .error_for_status()?
            .json::<LogsSnapshot>()
            .await?)
    }

    async fn get_current_log(&self) -> Result<CurrentLogTail> {
        let res = self
            .client
            .get(format!(
                "{}/diagnostics/logs/current?format=json&lines=500",
                self.server_url
            ))
            .send()
            .await?
            .error_for_status()?
            .json::<serde_json::Value>()
            .await?;
        let path = res.get("path").and_then(|p| p.as_str()).map(String::from);
        let content = res
            .get("content")
            .and_then(|c| c.as_str())
            .unwrap_or("")
            .to_string();
        Ok(CurrentLogTail { path, content })
    }

    async fn get_changelog(&self, force: bool) -> Result<ChangelogPayload> {
        Ok(self
            .client
            .get(format!(
                "{}/update/changelog?force={force}",
                self.server_url
            ))
            .timeout(Duration::from_secs(15))
            .send()
            .await?
            .error_for_status()?
            .json::<ChangelogPayload>()
            .await?)
    }

    async fn export_diagnostics(&self) -> Result<Vec<u8>> {
        let bytes = self
            .client
            .get(format!("{}/diagnostics/export", self.server_url))
            .timeout(Duration::from_secs(30))
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        Ok(bytes.to_vec())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogRefreshMode {
    Manual,
    Live,
}

#[derive(Debug, Clone)]
struct LogEntryData {
    id: i32,
    time: String,
    level: String,
    target: String,
    message: String,
    raw: String,
    parsed_json: Option<Value>,
}

#[derive(Debug)]
struct LogState {
    rows: Vec<LogEntryData>,
    query: String,
    expanded_id: Option<i32>,
    live_enabled: bool,
    show_error: bool,
    show_warning: bool,
    show_info: bool,
    show_debug: bool,
    target_filter: Option<String>,
    expanded_tree_nodes: HashMap<i32, HashSet<i32>>,
    content_hash: u64,
    lazy_loaded: bool,
}

impl Default for LogState {
    fn default() -> Self {
        Self {
            rows: Vec::new(),
            query: String::new(),
            expanded_id: None,
            live_enabled: true,
            show_error: true,
            show_warning: true,
            show_info: true,
            show_debug: true,
            target_filter: None,
            expanded_tree_nodes: HashMap::new(),
            content_hash: 0,
            lazy_loaded: false,
        }
    }
}

static SETTINGS_GUI_OPEN: AtomicBool = AtomicBool::new(false);

struct PersistentGuiState {
    weak: Weak<AppWindow>,
    async_handle: Handle,
    connector: Arc<dyn ServerConnector>,
}

static PERSISTENT_GUI: OnceLock<Mutex<PersistentGuiState>> = OnceLock::new();

pub fn run(connector: Arc<dyn ServerConnector>) -> Result<()> {
    // If the event loop is already running from a previous call, just
    // re-show the existing (hidden) window instead of creating a new one.
    if let Some(state_mutex) = PERSISTENT_GUI.get() {
        if SETTINGS_GUI_OPEN.swap(true, Ordering::SeqCst) {
            eprintln!("Settings GUI is already open.");
            return Ok(());
        }

        let state = state_mutex.lock().expect("persistent GUI state poisoned");
        let weak = state.weak.clone();
        let handle = state.async_handle.clone();
        let conn = state.connector.clone();
        drop(state);

        let _ = slint::invoke_from_event_loop(move || {
            if let Some(ui) = weak.upgrade() {
                let _ = ui.show();
                refresh_settings(handle, conn, ui.as_weak());
            }
        });
        return Ok(());
    }

    // First-time initialisation — set up the UI, start the event loop,
    // and keep it alive permanently so subsequent opens can reuse it.
    if std::env::var("SLINT_BACKEND").is_err() {
        unsafe {
            std::env::set_var("SLINT_BACKEND", "winit");
        }
    }

    if SETTINGS_GUI_OPEN.swap(true, Ordering::SeqCst) {
        eprintln!("Settings GUI is already open.");
        return Ok(());
    }

    let async_runtime = RuntimeBuilder::new_multi_thread()
        .worker_threads(2)
        .thread_name("settings-gui-async")
        .enable_all()
        .build()
        .context("failed to start async runtime")?;
    let async_handle = async_runtime.handle().clone();
    let log_state = Arc::new(Mutex::new(LogState::default()));
    let log_refresh_in_flight = Arc::new(AtomicBool::new(false));
    let changelog_loaded = Arc::new(AtomicBool::new(false));
    let ui = AppWindow::new().context("failed to create settings window")?;
    ui.set_server_url("embedded".into());
    ui.set_status_text("Not connected".into());
    ui.set_changelog_status("Changelog not loaded".into());
    ui.set_changelog_sections(ModelRc::new(VecModel::from(Vec::<ChangelogSection>::new())));
    ui.set_log_entries(ModelRc::new(VecModel::from(Vec::<LogEntry>::new())));
    ui.set_log_timeline_bins(ModelRc::new(VecModel::from(vec![1; 32])));
    ui.set_log_target_options(ModelRc::new(VecModel::from(Vec::<FilterOption>::new())));

    // Hide (don't destroy) the window when the user clicks close so
    // the event loop stays alive and the window can be re-shown later.
    ui.window().on_close_requested(|| {
        SETTINGS_GUI_OPEN.store(false, Ordering::SeqCst);
        slint::CloseRequestResponse::HideWindow
    });

    let weak = ui.as_weak();
    ui.on_refresh_settings({
        let weak = weak.clone();
        let handle = async_handle.clone();
        let connector = connector.clone();
        move || refresh_settings(handle.clone(), connector.clone(), weak.clone())
    });

    ui.on_apply_settings({
        let weak = weak.clone();
        let handle = async_handle.clone();
        let connector = connector.clone();
        move || apply_settings(handle.clone(), connector.clone(), weak.clone())
    });

    ui.on_refresh_logs({
        let weak = weak.clone();
        let log_state = Arc::clone(&log_state);
        let in_flight = Arc::clone(&log_refresh_in_flight);
        let handle = async_handle.clone();
        let connector = connector.clone();
        move || {
            refresh_logs(
                handle.clone(),
                connector.clone(),
                weak.clone(),
                Arc::clone(&log_state),
                Arc::clone(&in_flight),
                LogRefreshMode::Manual,
            )
        }
    });

    ui.on_open_logs_folder({
        let weak = weak.clone();
        let handle = async_handle.clone();
        let connector = connector.clone();
        move || open_logs_folder(handle.clone(), connector.clone(), weak.clone())
    });

    ui.on_export_diagnostics({
        let weak = weak.clone();
        let handle = async_handle.clone();
        let connector = connector.clone();
        move || export_diagnostics(handle.clone(), connector.clone(), weak.clone())
    });

    ui.on_refresh_changelog({
        let weak = weak.clone();
        let handle = async_handle.clone();
        let connector = connector.clone();
        let changelog_loaded = Arc::clone(&changelog_loaded);
        move |force| {
            refresh_changelog(
                handle.clone(),
                connector.clone(),
                weak.clone(),
                Arc::clone(&changelog_loaded),
                force,
            )
        }
    });

    ui.on_open_changelog_release({
        let weak = weak.clone();
        let handle = async_handle.clone();
        move || {
            open_changelog_release(handle.clone(), weak.clone());
        }
    });

    let search_debounce_handle = Arc::new(Mutex::new(None::<tokio::task::JoinHandle<()>>));

    ui.on_log_search_edited({
        let weak = weak.clone();
        let log_state = Arc::clone(&log_state);
        let debounce_handle = Arc::clone(&search_debounce_handle);
        let handle = async_handle.clone();
        move |query| {
            {
                let mut state = log_state.lock().expect("log state poisoned");
                state.query = query.to_string();
            }

            let mut guard = debounce_handle.lock().expect("debounce handle poisoned");
            if let Some(prev) = guard.take() {
                prev.abort();
            }

            let weak_clone = weak.clone();
            let log_state_clone = Arc::clone(&log_state);

            *guard = Some(handle.spawn(async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                let _ = slint::invoke_from_event_loop(move || {
                    apply_log_state_to_weak(&weak_clone, &log_state_clone);
                });
            }));
        }
    });

    ui.on_toggle_log_row({
        let weak = weak.clone();
        let log_state = Arc::clone(&log_state);
        move |id| {
            {
                let mut state = log_state.lock().expect("log state poisoned");
                state.expanded_id = if state.expanded_id == Some(id) {
                    None
                } else {
                    Some(id)
                };
            }
            apply_log_state_to_weak(&weak, &log_state);
        }
    });

    ui.on_toggle_tree_node({
        let weak = weak.clone();
        let log_state = Arc::clone(&log_state);
        move |log_id, node_id| {
            {
                let mut state = log_state.lock().expect("log state poisoned");
                let expanded_set = state.expanded_tree_nodes.entry(log_id).or_default();
                if expanded_set.contains(&node_id) {
                    expanded_set.remove(&node_id);
                } else {
                    expanded_set.insert(node_id);
                }
            }
            apply_log_state_to_weak(&weak, &log_state);
        }
    });

    ui.on_toggle_log_live({
        let weak = weak.clone();
        let log_state = Arc::clone(&log_state);
        let in_flight = Arc::clone(&log_refresh_in_flight);
        let handle = async_handle.clone();
        let connector = connector.clone();
        move |enabled| {
            {
                let mut state = log_state.lock().expect("log state poisoned");
                state.live_enabled = enabled;
            }
            apply_log_state_to_weak(&weak, &log_state);
            if enabled {
                refresh_logs(
                    handle.clone(),
                    connector.clone(),
                    weak.clone(),
                    Arc::clone(&log_state),
                    Arc::clone(&in_flight),
                    LogRefreshMode::Live,
                );
            }
        }
    });

    ui.on_set_log_level_filter({
        let weak = weak.clone();
        let log_state = Arc::clone(&log_state);
        move |level, enabled| {
            {
                let mut state = log_state.lock().expect("log state poisoned");
                match level.as_str() {
                    "all" => {
                        state.show_error = enabled;
                        state.show_warning = enabled;
                        state.show_info = enabled;
                        state.show_debug = enabled;
                    }
                    "error" => state.show_error = enabled,
                    "warning" => state.show_warning = enabled,
                    "info" => state.show_info = enabled,
                    "debug" => state.show_debug = enabled,
                    _ => {}
                }
            }
            apply_log_state_to_weak(&weak, &log_state);
        }
    });

    ui.on_set_log_target_filter({
        let weak = weak.clone();
        let log_state = Arc::clone(&log_state);
        move |target, enabled| {
            {
                let mut state = log_state.lock().expect("log state poisoned");
                let target = target.to_string();
                state.target_filter = if enabled && !target.is_empty() {
                    Some(target)
                } else if state.target_filter.as_deref() == Some(target.as_str())
                    || target.is_empty()
                {
                    None
                } else {
                    state.target_filter.clone()
                };
            }
            apply_log_state_to_weak(&weak, &log_state);
        }
    });

    ui.on_section_changed({
        let weak = weak.clone();
        let log_state = Arc::clone(&log_state);
        let in_flight = Arc::clone(&log_refresh_in_flight);
        let changelog_loaded = Arc::clone(&changelog_loaded);
        let handle = async_handle.clone();
        let connector = connector.clone();
        move |index| {
            if index == 5 && !changelog_loaded.load(Ordering::Acquire) {
                refresh_changelog(
                    handle.clone(),
                    connector.clone(),
                    weak.clone(),
                    Arc::clone(&changelog_loaded),
                    false,
                );
            }

            if index == 6 {
                // Logs tab
                let already_loaded = {
                    let mut state = log_state.lock().expect("log state poisoned");
                    let loaded = state.lazy_loaded;
                    if !loaded {
                        state.lazy_loaded = true;
                    }
                    loaded
                };
                if !already_loaded {
                    refresh_logs(
                        handle.clone(),
                        connector.clone(),
                        weak.clone(),
                        Arc::clone(&log_state),
                        Arc::clone(&in_flight),
                        LogRefreshMode::Manual,
                    );
                    start_live_log_refresh(
                        handle.clone(),
                        connector.clone(),
                        weak.clone(),
                        Arc::clone(&log_state),
                        Arc::clone(&in_flight),
                    );
                }
            }
        }
    });

    // Store persistent state so subsequent calls can reuse the window.
    PERSISTENT_GUI.get_or_init(|| {
        Mutex::new(PersistentGuiState {
            weak: ui.as_weak(),
            connector: connector.clone(),
            async_handle: async_handle.clone(),
        })
    });

    refresh_settings(async_handle.clone(), connector.clone(), weak.clone());
    refresh_changelog(
        async_handle.clone(),
        connector.clone(),
        weak.clone(),
        Arc::clone(&changelog_loaded),
        false,
    );

    // Keep the async runtime alive for the lifetime of the event loop.
    // run_event_loop_until_quit blocks forever (even when the window is
    // hidden) so the runtime on the stack is never dropped.
    let _keep_runtime = async_runtime;

    ui.show().context("failed to show settings window")?;
    slint::run_event_loop_until_quit().context("settings window event loop failed")?;
    Ok(())
}

fn start_live_log_refresh(
    handle: Handle, connector: Arc<dyn ServerConnector>, weak: Weak<AppWindow>,
    log_state: Arc<Mutex<LogState>>, in_flight: Arc<AtomicBool>,
) {
    handle.clone().spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            if weak.upgrade().is_none() {
                break;
            }

            let live_enabled = {
                let state = log_state.lock().expect("log state poisoned");
                state.live_enabled
            };

            if live_enabled {
                refresh_logs(
                    handle.clone(),
                    connector.clone(),
                    weak.clone(),
                    Arc::clone(&log_state),
                    Arc::clone(&in_flight),
                    LogRefreshMode::Live,
                );
            }
        }
    });
}

fn refresh_settings(handle: Handle, connector: Arc<dyn ServerConnector>, weak: Weak<AppWindow>) {
    set_status(&weak, "Loading settings...");
    handle.spawn(async move {
        let result = connector.get_settings().await;
        match result {
            Ok(settings) => {
                let _ = slint::invoke_from_event_loop(move || {
                    if let Some(ui) = weak.upgrade() {
                        write_settings_to_ui(&ui, settings);
                        ui.set_status_text("Settings loaded".into());
                    }
                });
            }
            Err(err) => set_status(&weak, format!("Failed to load settings: {err}")),
        }
    });
}

fn apply_settings(handle: Handle, connector: Arc<dyn ServerConnector>, weak: Weak<AppWindow>) {
    let Some(ui) = weak.upgrade() else {
        return;
    };

    let payload = match read_settings_from_ui(&ui) {
        Ok(payload) => payload,
        Err(err) => {
            ui.set_status_text(format!("Invalid settings: {err}").into());
            return;
        }
    };

    set_status(&weak, "Applying settings...");
    handle.spawn(async move {
        let result = connector.apply_settings(payload).await;
        match result {
            Ok(_) => set_status(&weak, "Settings saved"),
            Err(err) => set_status(&weak, format!("Failed to save settings: {err}")),
        }
    });
}

struct LogRefreshGuard(Arc<AtomicBool>);

impl Drop for LogRefreshGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

fn refresh_logs(
    handle: Handle, connector: Arc<dyn ServerConnector>, weak: Weak<AppWindow>,
    log_state: Arc<Mutex<LogState>>, in_flight: Arc<AtomicBool>, mode: LogRefreshMode,
) {
    if in_flight.swap(true, Ordering::AcqRel) {
        if mode == LogRefreshMode::Manual {
            set_log_status(&weak, "Log refresh already running");
        }
        return;
    }

    if mode == LogRefreshMode::Manual {
        set_log_status(&weak, "Loading logs...");
    }

    handle.spawn(async move {
        let _guard = LogRefreshGuard(in_flight);
        let snapshot = if mode == LogRefreshMode::Manual {
            connector.get_logs().await.ok()
        } else {
            None
        };
        let tail = connector.get_current_log().await;

        match tail {
            Ok(tail) => {
                let new_hash = hash_content(&tail.content);
                let hash_changed = {
                    let mut state = log_state.lock().expect("log state poisoned");
                    let changed = state.content_hash != new_hash;
                    if changed {
                        state.content_hash = new_hash;
                    }
                    changed
                };

                if mode == LogRefreshMode::Live && !hash_changed {
                    return; // Content unchanged, skip update
                }

                let entries = parse_jsonl_entries(&tail.content);
                let _ = slint::invoke_from_event_loop(move || {
                    if let Some(ui) = weak.upgrade() {
                        if mode == LogRefreshMode::Manual {
                            let log_dir = snapshot
                                .as_ref()
                                .map(|snapshot| snapshot.log_dir.clone())
                                .unwrap_or_default();
                            let current_path = snapshot
                                .and_then(|snapshot| {
                                    snapshot.current_json_log.or(snapshot.current_human_log)
                                })
                                .or_else(|| tail.path.clone())
                                .unwrap_or_default();
                            ui.set_log_dir(log_dir.into());
                            ui.set_current_log_path(current_path.into());
                        } else if ui.get_current_log_path().is_empty() {
                            ui.set_current_log_path(tail.path.clone().unwrap_or_default().into());
                        }

                        ui.set_logs_text(tail.content.into());
                        {
                            let mut state = log_state.lock().expect("log state poisoned");
                            let expanded_id = state.expanded_id;
                            state.rows = entries;
                            if mode == LogRefreshMode::Manual
                                || expanded_id
                                    .is_some_and(|id| !state.rows.iter().any(|row| row.id == id))
                            {
                                state.expanded_id = None;
                            }
                        }
                        apply_log_state_to_ui(&ui, &log_state);
                        ui.set_log_status(
                            if mode == LogRefreshMode::Live {
                                "Live logs updated"
                            } else {
                                "Logs loaded"
                            }
                            .into(),
                        );
                    }
                });
            }
            Err(err) => {
                let prefix = if mode == LogRefreshMode::Live {
                    "Live log refresh failed"
                } else {
                    "Failed to load logs"
                };
                set_log_status(&weak, format!("{prefix}: {err}"));
            }
        }
    });
}

fn parse_jsonl_entries(content: &str) -> Vec<LogEntryData> {
    content
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() {
                None
            } else {
                Some(parse_jsonl_entry(stable_log_id(line), line))
            }
        })
        .collect()
}

fn stable_log_id(line: &str) -> i32 {
    let mut hasher = DefaultHasher::new();
    line.hash(&mut hasher);
    (hasher.finish() & 0x7fff_ffff) as i32
}

fn parse_jsonl_entry(id: i32, line: &str) -> LogEntryData {
    match serde_json::from_str::<Value>(line) {
        Ok(value) => {
            let fields = value.get("fields").unwrap_or(&Value::Null);
            let time = first_json_string(&value, &["timestamp", "time", "@timestamp"])
                .map(short_time)
                .unwrap_or_else(|| "-".to_string());
            let level = normalize_level(
                first_json_string(&value, &["level", "severity"])
                    .unwrap_or_else(|| "INFO".to_string())
                    .as_str(),
            );
            let target = first_json_string(&value, &["target", "module_path", "module"])
                .or_else(|| first_json_string(fields, &["target", "module_path", "module"]))
                .unwrap_or_else(|| "-".to_string());
            let message = first_json_string(fields, &["message"])
                .or_else(|| first_json_string(&value, &["message"]))
                .unwrap_or_else(|| summarize_json_fields(fields));
            let raw = serde_json::to_string_pretty(&value).unwrap_or_else(|_| line.to_string());

            LogEntryData {
                id,
                time,
                level,
                target,
                message,
                raw,
                parsed_json: Some(value),
            }
        }
        Err(_) => LogEntryData {
            id,
            time: "-".to_string(),
            level: "INFO".to_string(),
            target: "raw".to_string(),
            message: line.to_string(),
            raw: line.to_string(),
            parsed_json: None,
        },
    }
}

fn first_json_string(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        value.get(*key).and_then(|value| match value {
            Value::String(text) => Some(text.clone()),
            Value::Number(number) => Some(number.to_string()),
            Value::Bool(value) => Some(value.to_string()),
            _ => None,
        })
    })
}

fn summarize_json_fields(value: &Value) -> String {
    match value {
        Value::Object(map) if !map.is_empty() => map
            .iter()
            .take(4)
            .map(|(key, value)| format!("{key}={}", compact_json_value(value)))
            .collect::<Vec<_>>()
            .join(" "),
        _ => "-".to_string(),
    }
}

fn compact_json_value(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Null => "null".to_string(),
        _ => serde_json::to_string(value).unwrap_or_else(|_| "-".to_string()),
    }
}

fn normalize_level(level: &str) -> String {
    match level.trim().to_ascii_uppercase().as_str() {
        "WARNING" | "WARN" => "WARN".to_string(),
        "ERROR" => "ERROR".to_string(),
        "DEBUG" => "DEBUG".to_string(),
        "TRACE" => "TRACE".to_string(),
        _ => "INFO".to_string(),
    }
}

fn short_time(time: String) -> String {
    let time = time.trim();
    if let Some((_, rest)) = time.split_once('T') {
        rest.trim_end_matches('Z')
            .split(['+', '-'])
            .next()
            .unwrap_or(rest)
            .to_string()
    } else {
        time.to_string()
    }
}

fn hash_content(s: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

fn flatten_json(
    value: &Value, depth: i32, key: String, expanded_nodes: &HashSet<i32>, node_counter: &mut i32,
    output: &mut Vec<JsonTreeNode>,
) {
    let current_id = *node_counter;
    *node_counter += 1;

    let expandable = value.is_object() || value.is_array();
    let expanded = expandable && expanded_nodes.contains(&current_id);

    let (value_preview, value_type) = match value {
        Value::Null => ("null".to_string(), "null".to_string()),
        Value::Bool(b) => (b.to_string(), "bool".to_string()),
        Value::Number(n) => (n.to_string(), "number".to_string()),
        Value::String(s) => (format!("\"{}\"", s), "string".to_string()),
        Value::Array(arr) => {
            if arr.is_empty() {
                ("[]".to_string(), "array".to_string())
            } else if expanded {
                ("".to_string(), "array".to_string())
            } else {
                (format!("[{} items]", arr.len()), "array".to_string())
            }
        }
        Value::Object(obj) => {
            if obj.is_empty() {
                ("{}".to_string(), "object".to_string())
            } else if expanded {
                ("".to_string(), "object".to_string())
            } else {
                let keys: Vec<&str> = obj.keys().map(|k| k.as_str()).take(3).collect();
                let joined = keys.join(", ");
                let preview = if obj.len() > 3 {
                    format!("{{{}, ...}}", joined)
                } else {
                    format!("{{{}}}", joined)
                };
                (preview, "object".to_string())
            }
        }
    };

    output.push(JsonTreeNode {
        node_id: current_id,
        depth,
        key: key.into(),
        value_preview: value_preview.into(),
        value_type: value_type.into(),
        expandable,
        expanded,
    });

    if expanded {
        match value {
            Value::Array(arr) => {
                for (i, val) in arr.iter().enumerate() {
                    flatten_json(
                        val,
                        depth + 1,
                        i.to_string(),
                        expanded_nodes,
                        node_counter,
                        output,
                    );
                }
            }
            Value::Object(obj) => {
                let mut sorted_keys: Vec<_> = obj.keys().collect();
                sorted_keys.sort();
                for k in sorted_keys {
                    if let Some(val) = obj.get(k) {
                        flatten_json(
                            val,
                            depth + 1,
                            k.clone(),
                            expanded_nodes,
                            node_counter,
                            output,
                        );
                    }
                }
            }
            _ => {}
        }
    }
}

fn apply_log_state_to_weak(weak: &Weak<AppWindow>, log_state: &Arc<Mutex<LogState>>) {
    if let Some(ui) = weak.upgrade() {
        apply_log_state_to_ui(&ui, log_state);
    }
}

fn apply_log_state_to_ui(ui: &AppWindow, log_state: &Arc<Mutex<LogState>>) {
    let snapshot = {
        let state = log_state.lock().expect("log state poisoned");
        build_log_view_snapshot(&state)
    };

    ui.set_log_entries(ModelRc::new(VecModel::from(snapshot.entries)));
    ui.set_log_is_truncated(snapshot.is_truncated);
    ui.set_log_total_count(snapshot.total_count.to_string().into());
    ui.set_log_visible_count(snapshot.visible_count.to_string().into());
    ui.set_log_error_count(snapshot.error_count.to_string().into());
    ui.set_log_warning_count(snapshot.warning_count.to_string().into());
    ui.set_log_info_count(snapshot.info_count.to_string().into());
    ui.set_log_debug_count(snapshot.debug_count.to_string().into());
    ui.set_log_timeline_bins(ModelRc::new(VecModel::from(snapshot.timeline_bins)));
    ui.set_log_target_options(ModelRc::new(VecModel::from(snapshot.target_options)));
    ui.set_log_active_target(snapshot.active_target.into());
    ui.set_log_live_enabled(snapshot.live_enabled);
    ui.set_log_search(snapshot.query.into());
    ui.set_log_show_error(snapshot.show_error);
    ui.set_log_show_warning(snapshot.show_warning);
    ui.set_log_show_info(snapshot.show_info);
    ui.set_log_show_debug(snapshot.show_debug);
}

struct LogViewSnapshot {
    entries: Vec<LogEntry>,
    total_count: usize,
    visible_count: usize,
    is_truncated: bool,
    error_count: usize,
    warning_count: usize,
    info_count: usize,
    debug_count: usize,
    timeline_bins: Vec<i32>,
    target_options: Vec<FilterOption>,
    active_target: String,
    live_enabled: bool,
    query: String,
    show_error: bool,
    show_warning: bool,
    show_info: bool,
    show_debug: bool,
}

fn build_log_view_snapshot(state: &LogState) -> LogViewSnapshot {
    let query = state.query.trim().to_ascii_lowercase();
    let mut error_count = 0;
    let mut warning_count = 0;
    let mut info_count = 0;
    let mut debug_count = 0;
    let target_options = build_target_options(state);
    let timeline_bins = build_timeline_bins(&state.rows, 32);

    let filtered = state
        .rows
        .iter()
        .inspect(|entry| match level_bucket(&entry.level) {
            "error" => error_count += 1,
            "warning" => warning_count += 1,
            "info" => info_count += 1,
            _ => debug_count += 1,
        })
        .filter(|entry| level_is_enabled(entry, state))
        .filter(|entry| {
            state
                .target_filter
                .as_ref()
                .is_none_or(|target| entry.target == *target)
        })
        .filter(|entry| query.is_empty() || entry_matches_query(entry, &query));

    let visible: Vec<_> = filtered.collect();
    let visible_count = visible.len();
    let is_truncated = visible_count > 200;

    let display_entries: Vec<_> = if is_truncated {
        visible
            .into_iter()
            .rev()
            .take(200)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    } else {
        visible
    };

    let entries = display_entries
        .into_iter()
        .map(|entry| {
            let tree_nodes = if state.expanded_id == Some(entry.id) {
                let mut nodes = Vec::new();
                if let Some(ref val) = entry.parsed_json {
                    let expanded_set = state.expanded_tree_nodes.get(&entry.id);
                    let empty_set = HashSet::new();
                    let expanded_nodes = expanded_set.unwrap_or(&empty_set);
                    let mut node_counter = 0;
                    flatten_json(
                        val,
                        0,
                        String::new(),
                        expanded_nodes,
                        &mut node_counter,
                        &mut nodes,
                    );
                }
                nodes
            } else {
                Vec::new()
            };

            LogEntry {
                id: entry.id,
                time: entry.time.as_str().into(),
                level: entry.level.as_str().into(),
                target: entry.target.as_str().into(),
                message: entry.message.as_str().into(),
                expanded: state.expanded_id == Some(entry.id),
                tree_nodes: ModelRc::new(VecModel::from(tree_nodes)),
            }
        })
        .collect::<Vec<_>>();

    LogViewSnapshot {
        visible_count,
        is_truncated,
        entries,
        total_count: state.rows.len(),
        error_count,
        warning_count,
        info_count,
        debug_count,
        timeline_bins,
        target_options,
        active_target: state.target_filter.clone().unwrap_or_default(),
        live_enabled: state.live_enabled,
        query: state.query.clone(),
        show_error: state.show_error,
        show_warning: state.show_warning,
        show_info: state.show_info,
        show_debug: state.show_debug,
    }
}

fn level_bucket(level: &str) -> &'static str {
    match level {
        "ERROR" => "error",
        "WARN" => "warning",
        "INFO" => "info",
        _ => "debug",
    }
}

fn build_target_options(state: &LogState) -> Vec<FilterOption> {
    let mut counts = HashMap::<String, usize>::new();
    for entry in &state.rows {
        if entry.target != "-" && !entry.target.trim().is_empty() {
            *counts.entry(entry.target.clone()).or_default() += 1;
        }
    }

    let mut counts = counts.into_iter().collect::<Vec<_>>();
    counts.sort_by(|(left_target, left_count), (right_target, right_count)| {
        right_count
            .cmp(left_count)
            .then_with(|| left_target.cmp(right_target))
    });
    counts.truncate(8);

    counts
        .into_iter()
        .map(|(target, count)| {
            let checked = state.target_filter.as_deref() == Some(target.as_str());
            FilterOption {
                label: target.into(),
                count: count.to_string().into(),
                checked,
            }
        })
        .collect()
}

fn build_timeline_bins(rows: &[LogEntryData], bin_count: usize) -> Vec<i32> {
    if rows.is_empty() || bin_count == 0 {
        return vec![1; bin_count.max(1)];
    }

    let mut bins = vec![0usize; bin_count];
    for (idx, _) in rows.iter().enumerate() {
        let bin = (idx * bin_count) / rows.len();
        if let Some(value) = bins.get_mut(bin.min(bin_count - 1)) {
            *value += 1;
        }
    }

    let max_count = bins.iter().copied().max().unwrap_or(1).max(1);
    bins.into_iter()
        .map(|count| {
            if count == 0 {
                1
            } else {
                (((count as f32 / max_count as f32) * 18.0).ceil() as i32).max(2)
            }
        })
        .collect()
}

fn level_is_enabled(entry: &LogEntryData, state: &LogState) -> bool {
    match level_bucket(&entry.level) {
        "error" => state.show_error,
        "warning" => state.show_warning,
        "info" => state.show_info,
        _ => state.show_debug,
    }
}

fn entry_matches_query(entry: &LogEntryData, query: &str) -> bool {
    entry.time.to_ascii_lowercase().contains(query)
        || entry.level.to_ascii_lowercase().contains(query)
        || entry.target.to_ascii_lowercase().contains(query)
        || entry.message.to_ascii_lowercase().contains(query)
        || entry.raw.to_ascii_lowercase().contains(query)
}

fn refresh_changelog(
    handle: Handle, connector: Arc<dyn ServerConnector>, weak: Weak<AppWindow>,
    loaded: Arc<AtomicBool>, force: bool,
) {
    if !force && loaded.load(Ordering::Acquire) {
        return;
    }

    set_changelog_status(&weak, "Loading changelog...");
    handle.spawn(async move {
        let result = connector.get_changelog(force).await;
        match result {
            Ok(payload) => {
                loaded.store(true, Ordering::Release);
                let _ = slint::invoke_from_event_loop(move || {
                    if let Some(ui) = weak.upgrade() {
                        apply_changelog_to_ui(&ui, payload);
                    }
                });
            }
            Err(err) => set_changelog_status(&weak, format!("Failed to load changelog: {err}")),
        }
    });
}

fn apply_changelog_to_ui(ui: &AppWindow, payload: ChangelogPayload) {
    let status = changelog_status_text(&payload);
    let sections = payload
        .sections
        .into_iter()
        .map(|section| {
            let items = section
                .items
                .into_iter()
                .map(|text| ChangelogItem { text: text.into() })
                .collect::<Vec<_>>();
            ChangelogSection {
                title: section.title.into(),
                items: ModelRc::new(VecModel::from(items)),
            }
        })
        .collect::<Vec<_>>();

    ui.set_changelog_current_version(payload.current_version.into());
    ui.set_changelog_latest_version(payload.latest_version.unwrap_or_default().into());
    ui.set_changelog_release_url(payload.release_url.unwrap_or_default().into());
    ui.set_changelog_published_at(payload.published_at.unwrap_or_default().into());
    ui.set_changelog_status(status.into());
    ui.set_changelog_sections(ModelRc::new(VecModel::from(sections)));
}

fn changelog_status_text(payload: &ChangelogPayload) -> String {
    let base = match payload.state.as_str() {
        "loaded" => "Latest release changelog loaded",
        "upToDate" => "Latest release changelog loaded",
        "offline" => "GitHub is offline or unreachable",
        "notFound" => "No eligible release changelog found",
        "error" => "Could not load release changelog",
        other => other,
    };

    match payload.error.as_deref() {
        Some(error) if !error.is_empty() => format!("{base}: {error}"),
        _ => base.to_string(),
    }
}

fn open_changelog_release(handle: Handle, weak: Weak<AppWindow>) {
    let Some(ui) = weak.upgrade() else {
        return;
    };
    let url = ui.get_changelog_release_url().to_string();
    if url.trim().is_empty() {
        ui.set_changelog_status("No release link is available".into());
        return;
    }

    set_changelog_status(&weak, "Opening release...");
    handle.spawn(async move {
        let result =
            tokio::task::spawn_blocking(move || open::that(url).context("failed to open release"))
                .await
                .context("failed to join open-release task")
                .and_then(|result| result);

        match result {
            Ok(_) => set_changelog_status(&weak, "Opened release"),
            Err(err) => set_changelog_status(&weak, format!("Failed to open release: {err}")),
        }
    });
}

fn open_logs_folder(handle: Handle, connector: Arc<dyn ServerConnector>, weak: Weak<AppWindow>) {
    set_log_status(&weak, "Opening logs folder...");
    handle.spawn(async move {
        let result = async {
            let snapshot = connector.get_logs().await?;
            tokio::task::spawn_blocking(move || {
                open::that(snapshot.log_dir).context("failed to open log folder")
            })
            .await
            .context("failed to join open-folder task")??;
            Ok::<(), anyhow::Error>(())
        }
        .await;

        match result {
            Ok(_) => set_log_status(&weak, "Opened logs folder"),
            Err(err) => set_log_status(&weak, format!("Failed to open logs folder: {err}")),
        }
    });
}

fn export_diagnostics(handle: Handle, connector: Arc<dyn ServerConnector>, weak: Weak<AppWindow>) {
    set_log_status(&weak, "Exporting diagnostics...");
    handle.spawn(async move {
        let result = export_diagnostics_impl(connector).await;
        match result {
            Ok(path) => {
                set_log_status(&weak, format!("Diagnostics exported to {}", path.display()))
            }
            Err(err) => set_log_status(&weak, format!("Failed to export diagnostics: {err}")),
        }
    });
}

async fn export_diagnostics_impl(connector: Arc<dyn ServerConnector>) -> Result<PathBuf> {
    let bytes = connector.export_diagnostics().await?;
    let dir = dirs::download_dir()
        .or_else(|| std::env::current_dir().ok())
        .unwrap_or_else(std::env::temp_dir);
    let path = dir.join("stream-server-diagnostics.zip");
    let write_path = path.clone();
    tokio::task::spawn_blocking(move || fs::write(&write_path, bytes))
        .await
        .context("failed to join diagnostics export write")??;
    Ok(path)
}

fn set_status(weak: &Weak<AppWindow>, text: impl Into<String>) {
    let text = text.into();
    let weak = weak.clone();
    let _ = slint::invoke_from_event_loop(move || {
        if let Some(ui) = weak.upgrade() {
            ui.set_status_text(SharedString::from(text));
        }
    });
}

fn set_log_status(weak: &Weak<AppWindow>, text: impl Into<String>) {
    let text = text.into();
    let weak = weak.clone();
    let _ = slint::invoke_from_event_loop(move || {
        if let Some(ui) = weak.upgrade() {
            ui.set_log_status(SharedString::from(text));
        }
    });
}

fn set_changelog_status(weak: &Weak<AppWindow>, text: impl Into<String>) {
    let text = text.into();
    let weak = weak.clone();
    let _ = slint::invoke_from_event_loop(move || {
        if let Some(ui) = weak.upgrade() {
            ui.set_changelog_status(SharedString::from(text));
        }
    });
}

fn write_settings_to_ui(ui: &AppWindow, settings: SettingsPayload) {
    ui.set_cache_root(settings.cache_root.into());
    ui.set_cache_size(settings.cache_size.to_string().into());
    ui.set_proxy_streams_enabled(settings.proxy_streams_enabled);
    ui.set_bt_max_connections(settings.bt_max_connections.to_string().into());
    ui.set_bt_handshake_timeout(settings.bt_handshake_timeout.to_string().into());
    ui.set_bt_request_timeout(settings.bt_request_timeout.to_string().into());
    ui.set_bt_download_speed_soft_limit(settings.bt_download_speed_soft_limit.to_string().into());
    ui.set_bt_download_speed_hard_limit(settings.bt_download_speed_hard_limit.to_string().into());
    ui.set_bt_min_peers_for_stable(settings.bt_min_peers_for_stable.to_string().into());
    ui.set_bt_enable_dht(settings.bt_enable_dht);
    ui.set_bt_enable_pex(settings.bt_enable_pex);
    ui.set_bt_enable_lsd(settings.bt_enable_lsd);
    ui.set_bt_encryption_mode(settings.bt_encryption_mode.into());
    ui.set_bt_anonymous_mode(settings.bt_anonymous_mode);
    ui.set_bt_allow_multiple_connections_per_ip(settings.bt_allow_multiple_connections_per_ip);
    ui.set_bt_listen_interfaces(settings.bt_listen_interfaces.into());
    ui.set_bt_outgoing_interfaces(settings.bt_outgoing_interfaces.into());
    ui.set_bt_outgoing_port(settings.bt_outgoing_port.to_string().into());
    ui.set_bt_num_outgoing_ports(settings.bt_num_outgoing_ports.to_string().into());
    ui.set_bt_proxy_type(settings.bt_proxy_type.into());
    ui.set_bt_proxy_host(settings.bt_proxy_host.into());
    ui.set_bt_proxy_port(settings.bt_proxy_port.to_string().into());
    ui.set_bt_proxy_username(settings.bt_proxy_username.into());
    ui.set_bt_proxy_password(settings.bt_proxy_password.into());
    ui.set_bt_proxy_hostnames(settings.bt_proxy_hostnames);
    ui.set_bt_proxy_peer_connections(settings.bt_proxy_peer_connections);
    ui.set_bt_proxy_tracker_connections(settings.bt_proxy_tracker_connections);
    ui.set_bt_proxy_send_host_in_connect(settings.bt_proxy_send_host_in_connect);
    ui.set_bt_validate_https_trackers(settings.bt_validate_https_trackers);
    ui.set_bt_ssrf_mitigation(settings.bt_ssrf_mitigation);
    ui.set_auto_update_enabled(settings.auto_update_enabled);
    ui.set_update_channel(settings.update_channel.into());
    ui.set_update_check_interval_hours(settings.update_check_interval_hours.to_string().into());
    ui.set_seeding_enabled(settings.seeding_enabled);
}

fn read_settings_from_ui(ui: &AppWindow) -> Result<SettingsPayload> {
    Ok(SettingsPayload {
        cache_root: ui.get_cache_root().to_string(),
        cache_size: parse_f64("cacheSize", ui.get_cache_size())?,
        proxy_streams_enabled: ui.get_proxy_streams_enabled(),
        bt_max_connections: parse_u64("btMaxConnections", ui.get_bt_max_connections())?,
        bt_handshake_timeout: parse_u64("btHandshakeTimeout", ui.get_bt_handshake_timeout())?,
        bt_request_timeout: parse_u64("btRequestTimeout", ui.get_bt_request_timeout())?,
        bt_download_speed_soft_limit: parse_f64(
            "btDownloadSpeedSoftLimit",
            ui.get_bt_download_speed_soft_limit(),
        )?,
        bt_download_speed_hard_limit: parse_f64(
            "btDownloadSpeedHardLimit",
            ui.get_bt_download_speed_hard_limit(),
        )?,
        bt_min_peers_for_stable: parse_u64(
            "btMinPeersForStable",
            ui.get_bt_min_peers_for_stable(),
        )?,
        bt_enable_dht: ui.get_bt_enable_dht(),
        bt_enable_pex: ui.get_bt_enable_pex(),
        bt_enable_lsd: ui.get_bt_enable_lsd(),
        bt_encryption_mode: ui.get_bt_encryption_mode().to_string(),
        bt_anonymous_mode: ui.get_bt_anonymous_mode(),
        bt_allow_multiple_connections_per_ip: ui.get_bt_allow_multiple_connections_per_ip(),
        bt_listen_interfaces: ui.get_bt_listen_interfaces().to_string(),
        bt_outgoing_interfaces: ui.get_bt_outgoing_interfaces().to_string(),
        bt_outgoing_port: parse_u16("btOutgoingPort", ui.get_bt_outgoing_port())?,
        bt_num_outgoing_ports: parse_u16("btNumOutgoingPorts", ui.get_bt_num_outgoing_ports())?,
        bt_proxy_type: ui.get_bt_proxy_type().to_string(),
        bt_proxy_host: ui.get_bt_proxy_host().to_string(),
        bt_proxy_port: parse_u16("btProxyPort", ui.get_bt_proxy_port())?,
        bt_proxy_username: ui.get_bt_proxy_username().to_string(),
        bt_proxy_password: ui.get_bt_proxy_password().to_string(),
        bt_proxy_hostnames: ui.get_bt_proxy_hostnames(),
        bt_proxy_peer_connections: ui.get_bt_proxy_peer_connections(),
        bt_proxy_tracker_connections: ui.get_bt_proxy_tracker_connections(),
        bt_proxy_send_host_in_connect: ui.get_bt_proxy_send_host_in_connect(),
        bt_validate_https_trackers: ui.get_bt_validate_https_trackers(),
        bt_ssrf_mitigation: ui.get_bt_ssrf_mitigation(),
        auto_update_enabled: ui.get_auto_update_enabled(),
        update_channel: ui.get_update_channel().to_string(),
        update_check_interval_hours: parse_u64(
            "updateCheckIntervalHours",
            ui.get_update_check_interval_hours(),
        )?,
        seeding_enabled: ui.get_seeding_enabled(),
    })
}

fn parse_u64(field: &str, value: SharedString) -> Result<u64> {
    value
        .trim()
        .parse::<u64>()
        .with_context(|| format!("{field} must be a whole number"))
}

fn parse_u16(field: &str, value: SharedString) -> Result<u16> {
    value
        .trim()
        .parse::<u16>()
        .with_context(|| format!("{field} must be a port number from 0 to 65535"))
}

fn parse_f64(field: &str, value: SharedString) -> Result<f64> {
    value
        .trim()
        .parse::<f64>()
        .with_context(|| format!("{field} must be a number"))
}
