use crate::routes::system::ServerSettings;
use enginefs::EngineFS;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::local_addon::LocalIndex;

#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<EngineFS>,
    pub download_engine: Arc<EngineFS>,
    pub download_engine_disk_backed: bool,
    pub settings: Arc<RwLock<ServerSettings>>,
    pub settings_path: PathBuf,
    pub config_dir: PathBuf,
    pub log_dir: PathBuf,
    pub base_url: String,
    pub http_addr: SocketAddr,
    pub update_install_exit_enabled: bool,
    pub updater: Arc<crate::updater::UpdateManager>,
    pub local_index: LocalIndex,
    pub archive_cache: Arc<dashmap::DashMap<String, crate::archives::ArchiveSession>>,
    pub nzb_sessions: Arc<dashmap::DashMap<String, crate::archives::nzb::session::NzbSession>>,
    pub devices: Arc<RwLock<Vec<crate::ssdp::Device>>>,
}

impl AppState {
    /// EngineFS that owns stream/HLS torrents: the disk-backed download engine when
    /// available, otherwise the memory-only engine. Mirrors the selection in
    /// `routes::stream` so HLS playback and the `/stream` loopback share one torrent
    /// instead of spinning up a redundant memory-only copy that never evicts pieces.
    pub fn stream_engine(&self) -> Arc<EngineFS> {
        if self.download_engine_disk_backed {
            self.download_engine.clone()
        } else {
            self.engine.clone()
        }
    }

    #[allow(unused)]
    pub fn new(engine: Arc<EngineFS>, settings: ServerSettings, config_dir: PathBuf) -> Self {
        let log_dir = config_dir.join("logs");
        Self::new_with_shared_settings_and_log_dir(
            engine,
            Arc::new(RwLock::new(settings)),
            config_dir,
            log_dir,
        )
    }

    #[allow(unused)]
    pub fn new_with_shared_settings(
        engine: Arc<EngineFS>, settings: Arc<RwLock<ServerSettings>>, config_dir: PathBuf,
    ) -> Self {
        let log_dir = config_dir.join("logs");
        Self::new_with_shared_settings_and_log_dir(engine, settings, config_dir, log_dir)
    }

    pub fn new_with_shared_settings_and_log_dir(
        engine: Arc<EngineFS>, settings: Arc<RwLock<ServerSettings>>, config_dir: PathBuf,
        log_dir: PathBuf,
    ) -> Self {
        Self::new_with_shared_settings_log_dir_and_download_engine(
            engine.clone(),
            engine,
            false,
            settings,
            config_dir,
            log_dir,
        )
    }

    pub fn new_with_shared_settings_log_dir_and_download_engine(
        engine: Arc<EngineFS>, download_engine: Arc<EngineFS>, download_engine_disk_backed: bool,
        settings: Arc<RwLock<ServerSettings>>, config_dir: PathBuf, log_dir: PathBuf,
    ) -> Self {
        let settings_path = config_dir.join("settings.json");
        let updater = Arc::new(crate::updater::UpdateManager::new(config_dir.clone()));

        Self {
            engine,
            download_engine,
            download_engine_disk_backed,
            settings,
            settings_path,
            config_dir,
            log_dir,
            base_url: "http://127.0.0.1:11470".to_string(),
            http_addr: SocketAddr::from(([127, 0, 0, 1], 11470)),
            update_install_exit_enabled: true,
            updater,
            local_index: LocalIndex::new(),
            archive_cache: Arc::new(dashmap::DashMap::new()),
            nzb_sessions: Arc::new(dashmap::DashMap::new()),
            devices: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn save_settings(&self) -> anyhow::Result<()> {
        let settings = self.settings.read().await;
        let json = serde_json::to_string_pretty(&*settings)?;

        // Ensure parent directory exists
        if let Some(parent) = self.settings_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        tokio::fs::write(&self.settings_path, json).await?;
        tracing::info!("Settings saved to {:?}", self.settings_path);
        Ok(())
    }

    pub fn load_settings(
        config_dir: &std::path::Path, defaults: &ServerSettings,
    ) -> ServerSettings {
        let settings_path = config_dir.join("settings.json");

        if settings_path.exists()
            && let Ok(content) = std::fs::read_to_string(&settings_path)
            && let Ok(mut settings) = serde_json::from_str::<ServerSettings>(&content)
        {
            tracing::info!("Loaded settings from {:?}", settings_path);
            // Ensure the cache_root in the loaded settings matches what we expect from runtime?
            // Or do we respect the file?
            // User might have customized it in the file.
            // If it's the default value (empty or old default), maybe update it?
            // For now, let's respect the file, but if missing/empty, use our runtime defaults.
            if settings.cache_root.is_empty() {
                settings.cache_root = defaults.cache_root.clone();
            }
            if settings.bt_max_connections == 0
                || settings.bt_max_connections
                    >= enginefs::backend::LEGACY_UNLIMITED_BT_MAX_CONNECTIONS
            {
                tracing::info!(
                    previous_bt_max_connections = settings.bt_max_connections,
                    normalized_bt_max_connections = enginefs::backend::DEFAULT_BT_MAX_CONNECTIONS,
                    "Normalizing legacy torrent connection setting for multi-client stability"
                );
                settings.bt_max_connections = enginefs::backend::DEFAULT_BT_MAX_CONNECTIONS;
            }
            return settings;
        }

        tracing::info!("Using default settings");
        defaults.clone()
    }
}

/// Wrapper for TrackerStorage that bridges sync trait with async AppState
/// This is created before EngineFS and passed to it for tracker persistence
pub struct TrackerStorageBridge {
    settings: Arc<RwLock<ServerSettings>>,
    settings_path: PathBuf,
}

impl TrackerStorageBridge {
    pub fn new(settings: Arc<RwLock<ServerSettings>>, settings_path: PathBuf) -> Self {
        Self {
            settings,
            settings_path,
        }
    }
}

impl enginefs::TrackerStorage for TrackerStorageBridge {
    fn get_cached_trackers(&self) -> Vec<String> {
        // Use blocking_read for sync access from async context
        // This is safe because we're only reading small data
        let handle = tokio::runtime::Handle::try_current();
        match handle {
            Ok(h) => {
                // We're in an async context, use block_in_place
                tokio::task::block_in_place(|| {
                    h.block_on(async {
                        let settings = self.settings.read().await;
                        settings.cached_trackers.clone()
                    })
                })
            }
            Err(_) => {
                // Not in async context, shouldn't happen but return empty
                Vec::new()
            }
        }
    }

    fn get_last_updated(&self) -> i64 {
        let handle = tokio::runtime::Handle::try_current();
        match handle {
            Ok(h) => tokio::task::block_in_place(|| {
                h.block_on(async {
                    let settings = self.settings.read().await;
                    settings.trackers_last_updated
                })
            }),
            Err(_) => 0,
        }
    }

    fn get_source_url(&self) -> String {
        let handle = tokio::runtime::Handle::try_current();
        match handle {
            Ok(h) => tokio::task::block_in_place(|| {
                h.block_on(async {
                    let settings = self.settings.read().await;
                    settings.trackers_source_url.clone()
                })
            }),
            Err(_) => crate::routes::system::default_trackers_url(),
        }
    }

    fn save_trackers(&self, trackers: Vec<String>, timestamp: i64) {
        let settings = self.settings.clone();
        let settings_path = self.settings_path.clone();

        // Spawn async task to update and save
        tokio::spawn(async move {
            let mut guard = settings.write().await;
            guard.cached_trackers = trackers;
            guard.trackers_last_updated = timestamp;
            let json = match serde_json::to_string_pretty(&*guard) {
                Ok(j) => j,
                Err(e) => {
                    tracing::error!("Failed to serialize settings: {}", e);
                    return;
                }
            };
            drop(guard);

            // Ensure parent directory exists
            if let Some(parent) = settings_path.parent()
                && let Err(e) = tokio::fs::create_dir_all(parent).await
            {
                tracing::error!("Failed to create settings directory: {}", e);
                return;
            }

            if let Err(e) = tokio::fs::write(&settings_path, json).await {
                tracing::error!("Failed to save settings after tracker update: {}", e);
            } else {
                tracing::debug!("Saved cached trackers to settings");
            }
        });
    }
}
