use crate::{
    state::AppState,
    updater::{
        changelog::latest_changelog, http_status_for_error, schedule_process_exit_for_update,
    },
};
use axum::{
    Json, Router,
    extract::{ConnectInfo, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::json;
use std::net::SocketAddr;

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CheckRequest {
    #[serde(default)]
    pub force: bool,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct InstallRequest {
    #[serde(default)]
    pub force: bool,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct ChangelogQuery {
    #[serde(default)]
    force: bool,
}

#[derive(Debug, Clone, Copy)]
struct LocalOnly;

impl IntoResponse for LocalOnly {
    fn into_response(self) -> Response {
        (
            StatusCode::FORBIDDEN,
            Json(json!({
                "success": false,
                "error": "update endpoints are local-only"
            })),
        )
            .into_response()
    }
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/status", get(status))
        .route("/changelog", get(changelog))
        .route("/check", post(check))
        .route("/download", post(download))
        .route("/install", post(install))
        .route("/cancel", post(cancel))
}

async fn status(
    State(state): State<AppState>, ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> Response {
    if let Err(response) = ensure_local(addr) {
        return response.into_response();
    }

    let settings = state.settings.read().await.clone();
    let mut status = state.updater.status(settings.update_channel).await;
    status.install_blocked_by = crate::updater::idle::install_blockers(&state).await;
    Json(status).into_response()
}

async fn changelog(
    State(state): State<AppState>, ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Query(query): Query<ChangelogQuery>,
) -> Response {
    if let Err(response) = ensure_local(addr) {
        return response.into_response();
    }

    let settings = state.settings.read().await.clone();
    Json(latest_changelog(settings.update_channel, query.force).await).into_response()
}

async fn check(
    State(state): State<AppState>, ConnectInfo(addr): ConnectInfo<SocketAddr>,
    payload: Option<Json<CheckRequest>>,
) -> Response {
    if let Err(response) = ensure_local(addr) {
        return response.into_response();
    }

    let request = payload.map(|Json(payload)| payload).unwrap_or_default();
    let settings = state.settings.read().await.clone();
    match state
        .updater
        .check_for_updates(&settings, request.force)
        .await
    {
        Ok(status) => Json(status).into_response(),
        Err(err) => update_error(err),
    }
}

async fn download(
    State(state): State<AppState>, ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> Response {
    if let Err(response) = ensure_local(addr) {
        return response.into_response();
    }

    match state.updater.download_update().await {
        Ok(status) => Json(status).into_response(),
        Err(err) => update_error(err),
    }
}

async fn install(
    State(state): State<AppState>, ConnectInfo(addr): ConnectInfo<SocketAddr>,
    payload: Option<Json<InstallRequest>>,
) -> Response {
    if let Err(response) = ensure_local(addr) {
        return response.into_response();
    }

    let request = payload.map(|Json(payload)| payload).unwrap_or_default();
    match state.updater.install_update(&state, request.force).await {
        Ok(status) => {
            if state.update_install_exit_enabled {
                schedule_process_exit_for_update();
            }
            Json(status).into_response()
        }
        Err(err) => update_error(err),
    }
}

async fn cancel(
    State(state): State<AppState>, ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> Response {
    if let Err(response) = ensure_local(addr) {
        return response.into_response();
    }

    match state.updater.cancel_update().await {
        Ok(status) => Json(status).into_response(),
        Err(err) => update_error(err),
    }
}

fn ensure_local(addr: SocketAddr) -> Result<(), LocalOnly> {
    if addr.ip().is_loopback() {
        Ok(())
    } else {
        Err(LocalOnly)
    }
}

fn update_error(err: crate::updater::error::UpdateError) -> Response {
    let status = http_status_for_error(err.kind);
    (
        status,
        Json(json!({
            "success": false,
            "error": err.message
        })),
    )
        .into_response()
}
