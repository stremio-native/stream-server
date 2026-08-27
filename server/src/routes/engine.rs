use crate::routes::compat;
use crate::state::AppState;
use axum::{
    extract::{Json, State},
    response::IntoResponse,
};
use enginefs::backend::TorrentHandle;
use hex;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct CreateEngineRequest {
    pub from: Option<String>, // Magnet link or URL
    #[serde(alias = "blob")]
    pub torrent: Option<String>, // Torrent blob (hex encoded) - alias "blob" for stremio-core compat
    pub announce: Option<Vec<String>>,
    #[serde(rename = "peerSearch")]
    pub peer_search: Option<PeerSearchBody>,
    #[serde(rename = "fileMustInclude", default)]
    pub file_must_include: Vec<String>,
    #[serde(rename = "guessFileIdx")]
    pub guess_file_idx: Option<serde_json::Value>,
}

#[derive(Deserialize)]
pub struct PeerSearchBody {
    #[serde(default)]
    pub sources: Vec<String>,
}

pub async fn create_engine(
    State(state): State<AppState>, Json(payload): Json<CreateEngineRequest>,
) -> impl IntoResponse {
    let source = if let Some(hex_str) = payload.torrent {
        match hex::decode(hex_str) {
            Ok(bytes) => enginefs::backend::TorrentSource::Bytes(bytes),
            Err(e) => return Json(json!({ "error": format!("Invalid hex blob: {}", e) })),
        }
    } else if let Some(from) = payload.from {
        enginefs::backend::TorrentSource::Url(from)
    } else {
        return Json(json!({ "error": "Missing 'from' or 'torrent' field" }));
    };

    let trackers = merged_trackers(payload.announce, payload.peer_search);
    let file_must_include = payload.file_must_include;
    let should_guess = guess_file_idx_requested(payload.guess_file_idx.as_ref());

    match state
        .stream_engine()
        .add_torrent(source, Some(trackers))
        .await
    {
        Ok(engine) => {
            let stats = stats_with_guess(&engine, &file_must_include, should_guess).await;
            Json(stats)
        }
        Err(e) => Json(json!({ "error": e.to_string() })),
    }
}
pub async fn list_engines(State(state): State<AppState>) -> impl IntoResponse {
    let engines = state.stream_engine().list_engines().await;
    Json(json!(engines))
}

pub async fn remove_engine(
    State(state): State<AppState>, axum::extract::Path(info_hash): axum::extract::Path<String>,
) -> impl IntoResponse {
    state
        .stream_engine()
        .remove_engine(&info_hash.to_lowercase())
        .await;
    Json(json!({}))
}

pub async fn remove_all_engines(State(state): State<AppState>) -> impl IntoResponse {
    let engine_fs = state.stream_engine();
    let engines = engine_fs.list_engines().await;
    for ih in engines {
        engine_fs.remove_engine(&ih).await;
    }
    Json(json!({}))
}

/// Stremio-core /{infoHash}/create endpoint for magnet links
#[derive(Deserialize)]
pub struct CreateMagnetRequest {
    pub stream: Option<CreateMagnetStream>,
    #[serde(rename = "peerSearch")]
    pub peer_search: Option<PeerSearchBody>,
    #[serde(rename = "fileMustInclude", default)]
    pub file_must_include: Vec<String>,
    #[serde(rename = "guessFileIdx")]
    pub guess_file_idx: Option<serde_json::Value>,
}

#[derive(Deserialize)]
pub struct CreateMagnetStream {
    #[serde(rename = "infoHash")]
    pub info_hash: Option<String>,
}

pub async fn create_magnet(
    State(state): State<AppState>, axum::extract::Path(info_hash): axum::extract::Path<String>,
    Json(payload): Json<CreateMagnetRequest>,
) -> impl IntoResponse {
    // Use the info_hash from path or body
    let ih = payload
        .stream
        .as_ref()
        .and_then(|s| s.info_hash.as_ref())
        .map(|s| s.as_str())
        .unwrap_or(&info_hash);

    // Create magnet URL from info hash
    let magnet = format!("magnet:?xt=urn:btih:{}", ih);
    let source = enginefs::backend::TorrentSource::Url(magnet);

    let trackers = merged_trackers(None, payload.peer_search);
    let file_must_include = payload.file_must_include;
    let should_guess = guess_file_idx_requested(payload.guess_file_idx.as_ref());

    match state
        .stream_engine()
        .add_torrent(source, Some(trackers))
        .await
    {
        Ok(engine) => {
            let stats = stats_with_guess(&engine, &file_must_include, should_guess).await;
            Json(stats)
        }
        Err(e) => Json(json!({ "error": e.to_string() })),
    }
}

pub async fn create_magnet_get(
    State(state): State<AppState>, axum::extract::Path(info_hash): axum::extract::Path<String>,
) -> impl IntoResponse {
    let magnet = format!("magnet:?xt=urn:btih:{}", info_hash);
    let source = enginefs::backend::TorrentSource::Url(magnet);

    match state.stream_engine().add_torrent(source, None).await {
        Ok(engine) => {
            let stats = stats_with_guess(&engine, &[], false).await;
            Json(stats)
        }
        Err(e) => Json(json!({ "error": e.to_string() })),
    }
}

fn merged_trackers(
    announce: Option<Vec<String>>, peer_search: Option<PeerSearchBody>,
) -> Vec<String> {
    let mut sources = announce.unwrap_or_default();
    if let Some(peer_search) = peer_search {
        sources.extend(peer_search.sources);
    }
    compat::normalize_tracker_sources(sources)
}

fn guess_file_idx_requested(value: Option<&serde_json::Value>) -> bool {
    match value {
        Some(serde_json::Value::Bool(false)) | None => false,
        Some(serde_json::Value::Null) => false,
        Some(_) => true,
    }
}

async fn stats_with_guess<H>(
    engine: &Arc<enginefs::engine::Engine<H>>, filters: &[String], should_guess: bool,
) -> serde_json::Value
where
    H: TorrentHandle,
{
    let stats = engine.get_statistics().await;
    let mut value = serde_json::to_value(stats).unwrap_or_else(|_| json!({}));

    if filters.is_empty() && !should_guess {
        return value;
    }

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

    if let Ok(idx) = compat::resolve_file_idx("-1", &candidates, filters)
        && let Some(obj) = value.as_object_mut()
    {
        obj.insert("guessedFileIdx".to_string(), json!(idx));
    }

    value
}
