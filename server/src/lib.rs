use anyhow::Context;
use axum::{Router, extract::State, http::StatusCode, response::Redirect, routing::get};
use enginefs::EngineFS;
pub use state::AppState;
use std::{
    future::{IntoFuture, pending},
    io::IsTerminal,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pub const DEFAULT_HTTP_PORT: u16 = 11470;
pub const DEFAULT_HTTPS_PORT: u16 = 12470;

#[cfg(any(target_os = "windows", target_os = "macos", target_os = "linux"))]
pub static GLOBAL_STATE: once_cell::sync::Lazy<std::sync::RwLock<Option<AppState>>> =
    once_cell::sync::Lazy::new(|| std::sync::RwLock::new(None));

#[cfg(any(target_os = "windows", target_os = "macos", target_os = "linux"))]
pub mod tray;

#[cfg(not(any(target_os = "windows", target_os = "macos", target_os = "linux")))]
pub mod tray {
    #[derive(Default)]
    pub struct TrayStats;
    impl TrayStats {
        pub fn new() -> Self {
            Self
        }
        pub fn update(&self, _down: f64, _up: f64, _peers: u64, _active: usize) {}
        pub fn set_auto_update_enabled(&self, _enabled: bool) {}
        pub fn set_seeding_enabled(&self, _enabled: bool) {}
        pub fn update_update_status(&self, _label: String, _install_enabled: bool) {}
        pub fn format_update_line(&self) -> String {
            String::new()
        }
    }
}

pub mod jni;

mod archives;
mod cache_cleaner;
mod diagnostics;
mod ffmpeg_setup;
mod local_addon;
mod routes;
mod ssdp;
mod state;
mod tui;
mod updater;
pub mod ytdlp;

pub use ffmpeg_setup::MissingFfmpegError;

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub http_addr: SocketAddr,
    pub https_addr: Option<SocketAddr>,
    pub public_base_url: Option<String>,
    pub config_dir: Option<PathBuf>,
    pub cache_dir: Option<PathBuf>,
    pub use_tui: bool,
    pub init_logging: bool,
    pub manage_process_globals: bool,
    pub setup_ffmpeg: bool,
    pub listen_for_ctrl_c: bool,
    pub print_startup: bool,
    pub exit_process_on_shutdown_timeout: bool,
    pub enable_update_exit: bool,
    pub enable_cache_cleaner: bool,
    pub enable_memory_sampler: bool,
    pub enable_ssdp_discovery: bool,
    pub enable_background_update_checker: bool,
    pub enable_local_addon_scan: bool,
    pub graceful_shutdown_timeout: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self::embedded()
    }
}

impl ServerConfig {
    pub fn embedded() -> Self {
        Self {
            http_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, DEFAULT_HTTP_PORT)),
            https_addr: None,
            public_base_url: None,
            config_dir: None,
            cache_dir: None,
            use_tui: false,
            init_logging: false,
            manage_process_globals: false,
            setup_ffmpeg: false,
            listen_for_ctrl_c: false,
            print_startup: false,
            exit_process_on_shutdown_timeout: false,
            enable_update_exit: false,
            enable_cache_cleaner: true,
            enable_memory_sampler: false,
            enable_ssdp_discovery: false,
            enable_background_update_checker: false,
            enable_local_addon_scan: false,
            graceful_shutdown_timeout: Duration::from_secs(3),
        }
    }

    pub fn binary_default() -> Self {
        // Overridable so multiple instances (or instances sharing a host with
        // other services on the default ports) can run with --network host,
        // where Docker can no longer remap ports for us.
        let http_port = std::env::var("STREAM_SERVER_HTTP_PORT")
            .ok()
            .and_then(|v| v.parse::<u16>().ok())
            .unwrap_or(DEFAULT_HTTP_PORT);
        let https_port = std::env::var("STREAM_SERVER_HTTPS_PORT")
            .ok()
            .and_then(|v| v.parse::<u16>().ok())
            .unwrap_or(DEFAULT_HTTPS_PORT);
        let public_base_url = std::env::var("STREAM_SERVER_PUBLIC_BASE_URL")
            .ok()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| format!("http://127.0.0.1:{http_port}"));

        Self {
            http_addr: SocketAddr::from(([0, 0, 0, 0], http_port)),
            https_addr: Some(SocketAddr::from(([0, 0, 0, 0], https_port))),
            public_base_url: Some(public_base_url),
            config_dir: None,
            cache_dir: None,
            use_tui: false,
            init_logging: true,
            manage_process_globals: true,
            setup_ffmpeg: true,
            listen_for_ctrl_c: true,
            print_startup: true,
            exit_process_on_shutdown_timeout: true,
            enable_update_exit: true,
            enable_cache_cleaner: true,
            enable_memory_sampler: true,
            enable_ssdp_discovery: true,
            enable_background_update_checker: true,
            enable_local_addon_scan: true,
            graceful_shutdown_timeout: Duration::from_secs(3),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownSource {
    CtrlC,
    Tui,
    External,
}

pub struct ServerHandle {
    http_addr: SocketAddr,
    bound_http_addr: SocketAddr,
    shutdown_tx: tokio::sync::mpsc::Sender<()>,
    join: std::thread::JoinHandle<anyhow::Result<Option<ShutdownSource>>>,
}

impl ServerHandle {
    pub fn http_addr(&self) -> SocketAddr {
        self.http_addr
    }

    pub fn bound_http_addr(&self) -> SocketAddr {
        self.bound_http_addr
    }

    pub fn shutdown(&self) -> anyhow::Result<()> {
        match self.shutdown_tx.try_send(()) {
            Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                Err(anyhow::anyhow!("server is already stopped"))
            }
        }
    }

    pub fn join(self) -> anyhow::Result<Option<ShutdownSource>> {
        self.join
            .join()
            .map_err(|_| anyhow::anyhow!("server thread panicked"))?
    }
}

pub fn start(cfg: ServerConfig) -> anyhow::Result<ServerHandle> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel(1);
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let thread_cfg = cfg.clone();

    let join = std::thread::Builder::new()
        .name("stream-server".to_string())
        .spawn(move || {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(run(thread_cfg, shutdown_rx, Some(ready_tx)))
        })?;

    let bound_http_addr = match ready_rx.blocking_recv() {
        Ok(addr) => addr,
        Err(_) => {
            return match join.join() {
                Ok(result) => match result {
                    Ok(_) => Err(anyhow::anyhow!("server exited before reporting ready")),
                    Err(err) => Err(err),
                },
                Err(_) => Err(anyhow::anyhow!(
                    "server thread panicked before reporting ready"
                )),
            };
        }
    };

    Ok(ServerHandle {
        http_addr: connectable_addr(bound_http_addr),
        bound_http_addr,
        shutdown_tx,
        join,
    })
}

pub async fn run(
    cfg: ServerConfig,
    external_shutdown_rx: tokio::sync::mpsc::Receiver<()>,
    ready_tx: Option<tokio::sync::oneshot::Sender<SocketAddr>>,
) -> anyhow::Result<Option<ShutdownSource>> {
    run_inner(cfg, external_shutdown_rx, None, ready_tx).await
}

pub async fn run_with_tray_stats(
    cfg: ServerConfig,
    external_shutdown_rx: tokio::sync::mpsc::Receiver<()>,
    tray_stats: Arc<tray::TrayStats>,
    ready_tx: Option<tokio::sync::oneshot::Sender<SocketAddr>>,
) -> anyhow::Result<Option<ShutdownSource>> {
    run_inner(cfg, external_shutdown_rx, Some(tray_stats), ready_tx).await
}

async fn run_inner(
    cfg: ServerConfig,
    mut external_shutdown_rx: tokio::sync::mpsc::Receiver<()>,
    tray_stats: Option<Arc<tray::TrayStats>>,
    ready_tx: Option<tokio::sync::oneshot::Sender<SocketAddr>>,
) -> anyhow::Result<Option<ShutdownSource>> {
    let listener = tokio::net::TcpListener::bind(cfg.http_addr)
        .await
        .with_context(|| format!("failed to bind HTTP listener on {}", cfg.http_addr))?;
    let bound_http_addr = listener.local_addr()?;
    let public_http_addr = connectable_addr(bound_http_addr);
    let base_url = cfg
        .public_base_url
        .clone()
        .unwrap_or_else(|| format!("http://{}", public_http_addr));

    let (tui_log_layer, tui_rx) = if cfg.use_tui {
        let (tx, rx) = crossbeam_channel::bounded(1000);
        (Some(tui::log_layer::TuiLogLayer::new(tx)), Some(rx))
    } else {
        (None, None)
    };

    let config_dir = match cfg.config_dir.clone() {
        Some(path) => path,
        None => dirs::config_dir()
            .ok_or_else(|| anyhow::anyhow!("Could not find config directory"))?
            .join("stremio-server"),
    };
    let cache_dir = match cfg.cache_dir.clone() {
        Some(path) => path,
        None => dirs::cache_dir()
            .ok_or_else(|| anyhow::anyhow!("Could not find cache directory"))?
            .join("stremio-server"),
    };
    let log_dir = config_dir.join("logs");

    tokio::fs::create_dir_all(&config_dir).await?;
    tokio::fs::create_dir_all(&cache_dir).await?;
    tokio::fs::create_dir_all(&log_dir).await?;

    diagnostics::logging::init_process_start();
    if cfg.manage_process_globals {
        diagnostics::logging::install_panic_hook();
    }

    let mut startup_log_paths = None;
    if cfg.init_logging {
        let log_writers = diagnostics::logging::open_log_writers(&log_dir)?;
        let human_log_path = log_writers.human_path.clone();
        let archive_log_path = log_writers.archive_path.clone();
        let json_log_path = log_writers.json_path.clone();
        let human_writer = log_writers.human_writer;
        let archive_writer = log_writers.archive_writer;
        let json_writer = log_writers.json_writer;
        let guards = log_writers.guards;

        // Default log directives, applied WITHOUT any environment variable. The
        // application code lives in the `stream_server` lib crate (targets
        // `stream_server::*`); `server` covers the thin `server` bin
        // (src/main.rs). Both must be listed or the lib rename silently filters
        // out every log line. `RUST_LOG` only overrides this when it is set to a
        // non-empty value; an unset or blank `RUST_LOG` keeps the default below.
        const DEFAULT_LOG_FILTER: &str =
            "server=info,stream_server=info,tower_http=info,enginefs=info";
        let log_filter = std::env::var("RUST_LOG")
            .ok()
            .filter(|directives| !directives.trim().is_empty())
            .map(tracing_subscriber::EnvFilter::new)
            .unwrap_or_else(|| tracing_subscriber::EnvFilter::new(DEFAULT_LOG_FILTER));
        let registry = tracing_subscriber::registry().with(log_filter);
        let human_file_layer = tracing_subscriber::fmt::layer()
            .with_writer(human_writer)
            .with_ansi(false);
        let archive_file_layer = tracing_subscriber::fmt::layer()
            .with_writer(archive_writer)
            .with_ansi(false);
        let json_file_layer = tracing_subscriber::fmt::layer()
            .json()
            .with_writer(json_writer)
            .with_ansi(false);

        let init_result = if let Some(layer) = tui_log_layer {
            registry
                .with(human_file_layer)
                .with(archive_file_layer)
                .with(json_file_layer)
                .with(layer)
                .try_init()
        } else if std::io::stdout().is_terminal() {
            registry
                .with(human_file_layer)
                .with(archive_file_layer)
                .with(json_file_layer)
                .with(tracing_subscriber::fmt::layer())
                .try_init()
        } else {
            registry
                .with(human_file_layer)
                .with(archive_file_layer)
                .with(json_file_layer)
                .try_init()
        };

        if init_result.is_ok() {
            diagnostics::logging::store_log_guards(guards);
            startup_log_paths = Some((human_log_path, archive_log_path, json_log_path));
        }
    }

    if cfg.setup_ffmpeg
        && let Err(e) = ffmpeg_setup::setup_ffmpeg().await
    {
        tracing::error!("FFmpeg setup failed: {}", e);
        return Err(e).context("FFmpeg setup failed");
    }

    tracing::info!("Config Dir: {:?}", config_dir);
    tracing::info!("Cache/Download Dir: {:?}", cache_dir);
    tracing::info!("Log Dir: {:?}", log_dir);
    if cfg.manage_process_globals {
        diagnostics::logging::install_native_crash_handler(&log_dir);
    }
    if let Some((human_log_path, archive_log_path, json_log_path)) = startup_log_paths {
        diagnostics::logging::log_startup_context(
            &config_dir,
            &cache_dir,
            &log_dir,
            &human_log_path,
            &archive_log_path,
            &json_log_path,
        );
    }

    // yt-dlp is only needed for YouTube trailers, but downloading it while a
    // hover preview waits is very visible. Warming it here off the startup path
    // means the first trailer usually finds it already installed, and a failure
    // only costs the trailer rather than the server.
    {
        let config_dir = config_dir.clone();
        tokio::spawn(async move {
            if let Err(error) = ytdlp::resolve(&config_dir).await {
                tracing::warn!(%error, "could not prepare yt-dlp; YouTube trailers will be unavailable");
            }
        });
    }

    let default_settings = routes::system::ServerSettings {
        cache_root: cache_dir.to_string_lossy().to_string(),
        ..routes::system::ServerSettings::default()
    };

    let settings = AppState::load_settings(&config_dir, &default_settings);
    let settings_arc = Arc::new(tokio::sync::RwLock::new(settings.clone()));
    let settings_path = config_dir.join("settings.json");
    let tracker_storage = Arc::new(state::TrackerStorageBridge::new(
        settings_arc.clone(),
        settings_path.clone(),
    ));

    let backend_config = enginefs::backend::BackendConfig {
        cache: enginefs::backend::priorities::EngineCacheConfig {
            size: settings.cache_size as u64,
            enabled: true,
        },
        growler: enginefs::backend::Growler::default(),
        peer_search: enginefs::backend::PeerSearch {
            min: settings.bt_min_peers_for_stable,
            ..Default::default()
        },
        swarm_cap: enginefs::backend::SwarmCap::default(),
        speed_profile: enginefs::backend::TorrentSpeedProfile {
            bt_download_speed_hard_limit: settings.bt_download_speed_hard_limit,
            bt_download_speed_soft_limit: settings.bt_download_speed_soft_limit,
            bt_handshake_timeout: settings.bt_handshake_timeout,
            bt_max_connections: settings.bt_max_connections,
            bt_min_peers_for_stable: settings.bt_min_peers_for_stable,
            bt_request_timeout: settings.bt_request_timeout,
        },
        privacy: enginefs::backend::TorrentPrivacyConfig {
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
        },
    };

    let (download_engine, download_engine_disk_backed) = match EngineFS::new_disk_backed(
        cache_dir.clone(),
        backend_config.clone(),
        Some(tracker_storage.clone()),
    )
    .await
    {
        Ok(download_engine_fs) => (Arc::new(download_engine_fs), true),
        Err(err) => {
            tracing::warn!(
                error = %err,
                "Disk-backed download engine unavailable at startup; download=1 will use memory-only mode"
            );
            let engine_fs = EngineFS::new_with_storage(
                cache_dir.clone(),
                backend_config,
                Some(tracker_storage),
            )
            .await?;
            (Arc::new(engine_fs), false)
        }
    };
    let engine = download_engine.clone();

    let mut state = AppState::new_with_shared_settings_log_dir_and_download_engine(
        engine,
        download_engine,
        download_engine_disk_backed,
        settings_arc.clone(),
        config_dir.clone(),
        log_dir.clone(),
    );
    state.base_url = base_url.clone();
    state.http_addr = public_http_addr;
    state.update_install_exit_enabled = cfg.enable_update_exit;

    #[cfg(any(target_os = "windows", target_os = "macos", target_os = "linux"))]
    {
        if let Ok(mut guard) = GLOBAL_STATE.write() {
            *guard = Some(state.clone());
        }
    }

    {
        let settings = settings_arc.read().await;
        state.engine.set_seeding_enabled(settings.seeding_enabled);
        state
            .download_engine
            .set_seeding_enabled(settings.seeding_enabled);
    }

    let mut background_tasks = Vec::new();
    if cfg.enable_cache_cleaner {
        background_tasks.push(cache_cleaner::start(Arc::new(state.clone())));
    }
    if cfg.enable_memory_sampler {
        background_tasks.push(diagnostics::start_memory_sampler(state.clone()));
    }
    if cfg.enable_background_update_checker {
        background_tasks.push(
            state
                .updater
                .clone()
                .spawn_background_checker(state.clone()),
        );
    }
    if cfg.enable_ssdp_discovery {
        background_tasks.push(diagnostics::logging::spawn_logged(
            "ssdp-discovery",
            crate::ssdp::start_discovery(state.devices.clone()),
        ));
    }

    if let Some(stats) = tray_stats {
        let engine_clone = state.engine.clone();
        let download_engine_clone = state.download_engine.clone();
        let updater_clone = state.updater.clone();
        let settings_clone = state.settings.clone();
        background_tasks.push(diagnostics::logging::spawn_logged(
            "tray-stats",
            async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    let mut all_stats = engine_clone.get_all_statistics().await;
                    let download_stats = download_engine_clone.get_all_statistics().await;
                    for (hash, stats) in download_stats {
                        all_stats.insert(hash, stats);
                    }

                    let mut total_down = 0.0;
                    let mut total_up = 0.0;
                    let mut total_peers = 0u64;
                    let active = all_stats.len();

                    for (_, s) in all_stats {
                        total_down += s.download_speed;
                        total_up += s.upload_speed;
                        total_peers += s.peers;
                    }

                    stats.update(total_down, total_up, total_peers, active);

                    let settings = settings_clone.read().await.clone();
                    stats.set_auto_update_enabled(settings.auto_update_enabled);
                    stats.set_seeding_enabled(settings.seeding_enabled);
                    let update_status = updater_clone.status(settings.update_channel).await;
                    let install_enabled = matches!(
                        update_status.state,
                        crate::updater::state::UpdateOperationState::Available
                            | crate::updater::state::UpdateOperationState::Staged
                    );
                    let label = if let Some(version) = update_status.staged_version {
                        format!("Update: v{} staged", version)
                    } else if let Some(version) = update_status.latest_version {
                        format!("Update: v{} available", version)
                    } else {
                        match update_status.state {
                            crate::updater::state::UpdateOperationState::Checking => {
                                "Update: checking".to_string()
                            }
                            crate::updater::state::UpdateOperationState::Downloading => {
                                "Update: downloading".to_string()
                            }
                            crate::updater::state::UpdateOperationState::Installing => {
                                "Update: installing".to_string()
                            }
                            crate::updater::state::UpdateOperationState::Failed => {
                                "Update: failed".to_string()
                            }
                            _ => "Update: up to date".to_string(),
                        }
                    };
                    stats.update_update_status(label, install_enabled);
                }
            },
        ));
    }

    let local_files_dir = config_dir.join("localFiles");
    tokio::fs::create_dir_all(&local_files_dir).await?;
    if cfg.enable_local_addon_scan {
        background_tasks.push(local_addon::scan_background(
            local_files_dir.to_string_lossy().to_string(),
            state.local_index.clone(),
        ));
    }

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel(1);
    if cfg.use_tui
        && let Some(rx) = tui_rx
    {
        tui::start_tui(Arc::new(state.clone()), rx, shutdown_tx);
    }

    let app = build_router(state);

    tracing::info!("listening on {}", bound_http_addr);
    if cfg.print_startup {
        println!("listening on {}", bound_http_addr);
        println!("EngineFS server started at {}", base_url);
    }
    if let Some(ready_tx) = ready_tx {
        let _ = ready_tx.send(bound_http_addr);
    }

    let (shutdown_started_tx, mut shutdown_started_rx) =
        tokio::sync::oneshot::channel::<ShutdownSource>();
    let listen_for_ctrl_c = cfg.listen_for_ctrl_c;
    let shutdown = async move {
        let source = tokio::select! {
            _ = maybe_ctrl_c(listen_for_ctrl_c) => {
                tracing::info!("Ctrl+C received, shutting down");
                ShutdownSource::CtrlC
            }
            _ = shutdown_rx.recv() => {
                tracing::info!("Shutdown signal received from TUI, shutting down");
                ShutdownSource::Tui
            }
            _ = external_shutdown_rx.recv() => {
                tracing::info!("Shutdown signal received from external controller, shutting down");
                ShutdownSource::External
            }
        };

        let _ = shutdown_started_tx.send(source);
    };

    let https_cert_path = config_dir.join("https-cert.pem");
    let https_key_path = config_dir.join("https-key.pem");

    if let Some(https_addr) = cfg.https_addr {
        if https_cert_path.exists() && https_key_path.exists() {
            tracing::info!("Found HTTPS certificates, starting HTTPS server on {https_addr}");
            let https_app = app.clone();
            let https_config = axum_server::tls_rustls::RustlsConfig::from_pem_file(
                https_cert_path,
                https_key_path,
            )
            .await?;

            background_tasks.push(diagnostics::logging::spawn_logged(
                "https-server",
                async move {
                    if let Err(e) = axum_server::bind_rustls(https_addr, https_config)
                        .serve(https_app.into_make_service_with_connect_info::<SocketAddr>())
                        .await
                    {
                        tracing::error!("HTTPS server error: {}", e);
                    }
                },
            ));
        } else {
            tracing::info!(
                "No HTTPS certificates found in {:?}, skipping HTTPS server on {:?}",
                config_dir,
                https_addr
            );
        }
    }

    let server = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown)
    .into_future();

    tokio::pin!(server);

    let shutdown_source = tokio::select! {
        result = &mut server => {
            result?;
            match shutdown_started_rx.try_recv() {
                Ok(source) => Some(source),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => None,
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => None,
            }
        }
        Ok(source) = &mut shutdown_started_rx => {
            match tokio::time::timeout(cfg.graceful_shutdown_timeout, &mut server).await {
                Ok(result) => {
                    result?;
                }
                Err(_) => {
                    if cfg.exit_process_on_shutdown_timeout {
                        tracing::warn!(
                            ?source,
                            timeout_secs = cfg.graceful_shutdown_timeout.as_secs(),
                            "Shutdown taking too long, forcing process exit"
                        );
                        std::process::exit(0);
                    }

                    tracing::warn!(
                        ?source,
                        timeout_secs = cfg.graceful_shutdown_timeout.as_secs(),
                        "Shutdown taking too long, dropping server future so restart can continue"
                    );
                }
            }
            Some(source)
        }
    };

    for task in background_tasks {
        task.abort();
    }

    #[cfg(any(target_os = "windows", target_os = "macos", target_os = "linux"))]
    {
        if let Ok(mut guard) = GLOBAL_STATE.write() {
            *guard = None;
        }
    }

    Ok(shutdown_source)
}

async fn maybe_ctrl_c(enabled: bool) {
    if enabled {
        let _ = tokio::signal::ctrl_c().await;
    } else {
        pending::<()>().await;
    }
}

fn connectable_addr(addr: SocketAddr) -> SocketAddr {
    match addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), addr.port())
        }
        IpAddr::V6(ip) if ip.is_unspecified() => {
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), addr.port())
        }
        _ => addr,
    }
}

pub fn build_router(state: AppState) -> Router {
    fn peer_from_request(req: &axum::extract::Request) -> Option<SocketAddr> {
        req.extensions()
            .get::<axum::extract::ConnectInfo<SocketAddr>>()
            .map(|info| info.0)
    }

    async fn fallback_handler(req: axum::extract::Request) -> impl axum::response::IntoResponse {
        diagnostics::logging::log_unhandled(
            "no matching route (404)",
            StatusCode::NOT_FOUND.as_u16(),
            peer_from_request(&req),
            req.method(),
            req.uri(),
            Some(req.version()),
            req.headers(),
        );
        StatusCode::NOT_FOUND
    }

    async fn method_not_allowed_handler(
        req: axum::extract::Request,
    ) -> impl axum::response::IntoResponse {
        diagnostics::logging::log_unhandled(
            "method not allowed for matched route (405)",
            StatusCode::METHOD_NOT_ALLOWED.as_u16(),
            peer_from_request(&req),
            req.method(),
            req.uri(),
            Some(req.version()),
            req.headers(),
        );
        StatusCode::METHOD_NOT_ALLOWED
    }

    Router::new()
        .route("/", get(root_redirect))
        .route("/heartbeat", get(routes::system::heartbeat))
        .route("/stats.json", get(routes::system::get_stats))
        .route("/network-info", get(routes::system::network_info))
        .nest("/update", routes::update::router())
        .route("/diagnostics/memory", get(diagnostics::memory))
        .route("/diagnostics/streams", get(diagnostics::streams))
        .route("/diagnostics/crashes", get(diagnostics::crashes))
        .route("/diagnostics/logs", get(diagnostics::logs))
        .route("/diagnostics/logs/current", get(diagnostics::current_log))
        .route("/diagnostics/export", get(diagnostics::export))
        .route(
            "/settings",
            get(routes::system::get_settings).post(routes::system::set_settings),
        )
        .route("/list", get(routes::engine::list_engines))
        .route("/removeAll", get(routes::engine::remove_all_engines))
        .route(
            "/create",
            get(routes::engine::create_engine).post(routes::engine::create_engine),
        )
        .route(
            "/{infoHash}/create",
            get(routes::engine::create_magnet_get).post(routes::engine::create_magnet),
        )
        .nest("/{ipc_key}/downloader", routes::downloader::router())
        .route("/{infoHash}/remove", get(routes::engine::remove_engine))
        .route(
            "/{infoHash}/stats.json",
            get(routes::system::get_engine_stats),
        )
        .route(
            "/{infoHash}/{idx}/stats.json",
            get(routes::system::get_file_stats),
        )
        .route("/{infoHash}/peers", get(routes::peers::get_peers))
        .route(
            "/stream/{infoHash}/{fileIdx}",
            get(routes::stream::stream_video).head(routes::stream::head_stream_video),
        )
        .route(
            "/{infoHash}/{fileIdx}",
            get(routes::stream::stream_video).head(routes::stream::head_stream_video),
        )
        .route(
            "/subtitles.vtt",
            get(routes::subtitles::proxy_subtitles_vtt),
        )
        .route(
            "/subtitles.{ext}",
            get(routes::subtitles::proxy_subtitles_ext),
        )
        .route(
            "/{infoHash}/{fileIdx}/subtitles.vtt",
            get(routes::subtitles::get_subtitles_vtt),
        )
        .route("/opensubHash", get(routes::subtitles::opensub_hash))
        .route(
            "/opensubHash/{infoHash}/{fileIdx}",
            get(routes::subtitles::opensub_hash_path),
        )
        .route("/subtitlesTracks", get(routes::subtitles::subtitles_tracks))
        .route("/device-info", get(routes::system::get_device_info))
        .route("/hwaccel-profiler", get(routes::system::hwaccel_profiler))
        .route("/get-https", get(routes::system::get_https))
        .nest("/yt", routes::youtube::router())
        .nest("/rar", routes::archive::router())
        .nest("/zip", routes::archive::router())
        .nest("/7zip", routes::archive::router())
        .nest("/tar", routes::archive::router())
        .nest("/tgz", routes::archive::router())
        .nest("/nzb", routes::nzb::router())
        .nest("/local-addon", local_addon::get_router())
        .nest("/proxy", routes::proxy::router())
        .nest("/ftp", routes::ftp::router())
        .route("/samples/{filename}", get(routes::system::get_samples))
        .route("/hlsv2/status", get(routes::hls::hls_status))
        .route("/hlsv2/{id}/destroy", get(routes::hls::hls_destroy))
        .route("/hlsv2/{id}/burn", get(routes::hls::hls_burn))
        .route(
            "/hlsv2/{hash}/master.m3u8",
            get(routes::hls::master_playlist_by_url),
        )
        .route("/hlsv2/{id}/{resource}", get(routes::hls::hls_v2_resource))
        .route(
            "/hlsv2/{infoHash}/{fileIdx}/master.m3u8",
            get(routes::hls::get_master_playlist),
        )
        .route(
            "/hlsv2/{infoHash}/{fileIdx}/{resource}",
            get(routes::hls::handle_hls_resource),
        )
        .route(
            "/hlsv2/{infoHash}/{fileIdx}/{track}/{segment}",
            get(routes::hls::handle_hls_fmp4_segment),
        )
        .route("/hlsv2/probe", get(routes::hls::probe_by_url))
        .route("/probe", get(routes::hls::probe_by_url))
        .route("/probe/{infoHash}/{fileIdx}", get(routes::hls::get_probe))
        .route("/tracks/{*url}", get(routes::hls::get_tracks_by_url))
        .route(
            "/{first}/{second}/{resource}",
            get(routes::hls::legacy_hls_resource),
        )
        .route(
            "/{first}/{second}/{variant}/{seg}",
            get(routes::hls::legacy_hls_segment),
        )
        .route("/thumb.jpg", get(|| async { StatusCode::NOT_FOUND }))
        .nest("/casting", routes::casting::router())
        .route("/favicon.ico", get(|| async { StatusCode::NOT_FOUND }))
        .fallback(fallback_handler)
        .method_not_allowed_fallback(method_not_allowed_handler)
        .layer(
            TraceLayer::new_for_http().make_span_with(|request: &axum::http::Request<_>| {
                tracing::info_span!(
                    "request",
                    method = %request.method(),
                    path = request.uri().path(),
                )
            }),
        )
        .layer(CorsLayer::permissive())
        .with_state(state)
}

async fn root_redirect(State(state): State<AppState>) -> Redirect {
    let encoded_url = urlencoding::encode(&state.base_url);
    Redirect::temporary(&format!(
        "https://web.stremio.com/#/?streamingServer={}",
        encoded_url
    ))
}
