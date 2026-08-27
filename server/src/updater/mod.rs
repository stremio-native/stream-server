pub mod assets;
pub mod changelog;
pub mod error;
pub mod github;
pub mod idle;
pub mod installer;
pub mod staging;
pub mod state;
pub mod version;

use self::{
    error::{UpdateError, UpdateErrorKind},
    github::{fetch_releases, make_client, select_latest_candidate},
    staging::stage_update,
    state::{UpdateInner, UpdateOperationState, UpdateStatus},
    version::{UpdateChannel, normalize_current_version},
};
use crate::{routes::system::ServerSettings, state::AppState};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::{Mutex, RwLock};

#[derive(Clone)]
pub struct UpdateManager {
    inner: Arc<RwLock<UpdateInner>>,
    operation: Arc<Mutex<()>>,
    staging_root: PathBuf,
}

impl UpdateManager {
    pub fn new(config_dir: PathBuf) -> Self {
        let channel = UpdateChannel::default();
        let staging_root = dirs::cache_dir()
            .map(|dir| dir.join("stremio-server").join("updates"))
            .unwrap_or_else(|| config_dir.join("updates"));
        Self {
            inner: Arc::new(RwLock::new(UpdateInner::new(channel))),
            operation: Arc::new(Mutex::new(())),
            staging_root,
        }
    }

    pub async fn status(&self, channel: UpdateChannel) -> UpdateStatus {
        let mut status = self.inner.read().await.status.clone();
        status.channel = channel;
        status
    }

    pub async fn check_for_updates(
        &self, settings: &ServerSettings, force: bool,
    ) -> Result<UpdateStatus, UpdateError> {
        let _guard = self.operation.lock().await;
        let now = unix_now();

        {
            let inner = self.inner.read().await;
            if !force && let Some(last_checked) = inner.status.last_checked_unix_secs {
                let interval_secs = settings.update_check_interval_hours.saturating_mul(3600);
                if interval_secs > 0 && now.saturating_sub(last_checked) < interval_secs as i64 {
                    return Ok(inner.status.clone());
                }
            }
        }

        self.set_state(UpdateOperationState::Checking, None).await;

        let client = make_client()?;
        let releases = match fetch_releases(&client).await {
            Ok(releases) => releases,
            Err(err) => {
                self.fail(&err.to_string()).await;
                return Err(err);
            }
        };

        let selected = match select_latest_candidate(
            &releases,
            settings.update_channel,
            &normalize_current_version(),
        ) {
            Ok(selected) => selected,
            Err(err) => {
                self.fail(&err.to_string()).await;
                return Err(err);
            }
        };

        let mut inner = self.inner.write().await;
        inner.status.channel = settings.update_channel;
        inner.status.last_checked_unix_secs = Some(now);
        inner.status.install_blocked_by.clear();
        inner.status.error = None;

        if let Some(candidate) = selected {
            inner.status.state = UpdateOperationState::Available;
            inner.status.latest_version = Some(candidate.version.clone());
            inner.status.release_url = Some(candidate.release_url.clone());
            inner.candidate = Some(candidate);
        } else {
            inner.status.state = UpdateOperationState::UpToDate;
            inner.status.latest_version = None;
            inner.status.release_url = None;
            inner.candidate = None;
        }

        Ok(inner.status.clone())
    }

    pub async fn download_update(&self) -> Result<UpdateStatus, UpdateError> {
        let _guard = self.operation.lock().await;
        let candidate = {
            let inner = self.inner.read().await;
            inner.candidate.clone().ok_or_else(|| {
                UpdateError::not_found("no eligible update is available to download")
            })?
        };

        self.set_state(UpdateOperationState::Downloading, None)
            .await;

        let client = make_client()?;
        let staged = match stage_update(&client, self.staging_root.clone(), &candidate).await {
            Ok(staged) => staged,
            Err(err) => {
                self.fail(&err.to_string()).await;
                return Err(err);
            }
        };

        let mut inner = self.inner.write().await;
        inner.status.state = UpdateOperationState::Staged;
        inner.status.staged_version = Some(staged.version.clone());
        inner.status.error = None;
        inner.staged = Some(staged);
        Ok(inner.status.clone())
    }

    pub async fn install_update(
        &self, app_state: &AppState, force: bool,
    ) -> Result<UpdateStatus, UpdateError> {
        let _guard = self.operation.lock().await;
        let blockers = idle::install_blockers(app_state).await;
        if !force && !blockers.is_empty() {
            let mut inner = self.inner.write().await;
            inner.status.state = UpdateOperationState::WaitingForIdle;
            inner.status.install_blocked_by = blockers;
            inner.status.error = Some("install blocked while server is active".to_string());
            return Err(UpdateError::conflict(
                "install blocked while server is active",
            ));
        }

        let staged = {
            let inner = self.inner.read().await;
            inner.staged.clone()
        };
        let staged = if let Some(staged) = staged {
            staged
        } else {
            let candidate = {
                let inner = self.inner.read().await;
                inner.candidate.clone().ok_or_else(|| {
                    UpdateError::not_found("no staged update is available to install")
                })?
            };
            self.set_state(UpdateOperationState::Downloading, None)
                .await;
            let client = make_client()?;
            let staged = match stage_update(&client, self.staging_root.clone(), &candidate).await {
                Ok(staged) => staged,
                Err(err) => {
                    self.fail(&err.to_string()).await;
                    return Err(err);
                }
            };
            let mut inner = self.inner.write().await;
            inner.status.staged_version = Some(staged.version.clone());
            inner.staged = Some(staged.clone());
            staged
        };

        let (helper, plan_path) = installer::prepare_apply_plan(&staged, force)?;
        installer::spawn_helper(&helper, &plan_path)?;

        let mut inner = self.inner.write().await;
        inner.status.state = UpdateOperationState::Installing;
        inner.status.install_blocked_by.clear();
        inner.status.error = None;
        Ok(inner.status.clone())
    }

    pub async fn cancel_update(&self) -> Result<UpdateStatus, UpdateError> {
        let _guard = self.operation.lock().await;
        let staged_dir = {
            let inner = self.inner.read().await;
            inner.staged.as_ref().map(|staged| staged.dir.clone())
        };
        if let Some(dir) = staged_dir {
            match tokio::fs::remove_dir_all(&dir).await {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(UpdateError::from(err)),
            }
        }

        let mut inner = self.inner.write().await;
        inner.status.state = UpdateOperationState::Idle;
        inner.status.staged_version = None;
        inner.status.install_blocked_by.clear();
        inner.status.error = None;
        inner.staged = None;
        Ok(inner.status.clone())
    }

    pub fn spawn_background_checker(
        self: Arc<Self>, app_state: AppState,
    ) -> tokio::task::JoinHandle<()> {
        crate::diagnostics::logging::spawn_logged("update-checker", async move {
            tokio::time::sleep(Duration::from_secs(30)).await;
            loop {
                let settings = app_state.settings.read().await.clone();
                if settings.auto_update_enabled
                    && let Err(err) = self.check_for_updates(&settings, false).await
                {
                    tracing::warn!(error = %err, "background update check failed");
                }

                let interval = settings.update_check_interval_hours.max(1);
                tokio::time::sleep(Duration::from_secs(interval.saturating_mul(3600))).await;
            }
        })
    }

    async fn set_state(&self, state: UpdateOperationState, error: Option<String>) {
        let mut inner = self.inner.write().await;
        inner.status.state = state;
        inner.status.error = error;
        inner.status.install_blocked_by.clear();
    }

    async fn fail(&self, message: &str) {
        let mut inner = self.inner.write().await;
        inner.status.state = UpdateOperationState::Failed;
        inner.status.error = Some(message.to_string());
    }
}

pub fn http_status_for_error(kind: UpdateErrorKind) -> axum::http::StatusCode {
    match kind {
        UpdateErrorKind::NotFound => axum::http::StatusCode::NOT_FOUND,
        UpdateErrorKind::Conflict => axum::http::StatusCode::CONFLICT,
        UpdateErrorKind::InvalidRelease => axum::http::StatusCode::UNPROCESSABLE_ENTITY,
        UpdateErrorKind::Unsupported => axum::http::StatusCode::NOT_IMPLEMENTED,
        UpdateErrorKind::Io | UpdateErrorKind::Network | UpdateErrorKind::Unexpected => {
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

pub fn schedule_process_exit_for_update() {
    std::thread::spawn(|| {
        std::thread::sleep(std::time::Duration::from_millis(700));
        std::process::exit(0);
    });
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
