pub mod index;
pub mod manifest;
pub mod parser;
pub mod resolver;
pub mod scanner;
pub mod torrent;

use self::manifest::get_manifest;
use crate::state::AppState;
use axum::{
    Json, Router,
    extract::{Path, State},
    routing::get,
};
use serde_json::{Value, json};

pub use self::index::LocalIndex;
pub use self::scanner::scan_background;

pub fn get_router() -> Router<AppState> {
    Router::new()
        .route("/manifest.json", get(handle_manifest))
        .route("/catalog/{type}/{id}", get(handle_catalog))
        .route("/catalog/{type}/{id}/{extra}", get(handle_catalog_extra))
        .route("/meta/{type}/{id}", get(handle_meta))
        .route("/stream/{type}/{id}", get(handle_stream))
}

async fn handle_manifest() -> Json<manifest::Manifest> {
    Json(get_manifest())
}

async fn handle_catalog(state: State<AppState>, path: Path<(String, String)>) -> Json<Value> {
    let (type_, id_ext) = path.0;
    let id = id_ext.strip_suffix(".json").unwrap_or(&id_ext).to_string();
    _handle_catalog(state, type_, id, None)
}

async fn handle_catalog_extra(
    state: State<AppState>, Path((type_, id, extra_ext)): Path<(String, String, String)>,
) -> Json<Value> {
    let extra = extra_ext
        .strip_suffix(".json")
        .unwrap_or(&extra_ext)
        .to_string();
    _handle_catalog(state, type_, id, Some(extra))
}

fn _handle_catalog(
    State(state): State<AppState>, type_: String, _id: String, _extra: Option<String>,
) -> Json<Value> {
    // Simple catalog: Return everything matching the type
    let items = state.local_index.items.read().unwrap();
    let metas: Vec<Value> = items
        .values()
        .flatten()
        .filter(|item| item.metadata.type_ == type_)
        // De-duplicate by IMDB ID
        .fold(std::collections::HashMap::new(), |mut acc, item| {
            let key = item.imdb_id.clone().unwrap_or_else(|| item.path.clone());
            acc.entry(key).or_insert(item);
            acc
        })
        .values()
        .map(|item| {
            json!({
                "id": item.id.clone(),
                "type": item.metadata.type_,
                "name": item.metadata.name.clone().unwrap_or("Unknown".to_string()),
                "poster": null, // In future: fetch from Cinemeta if imdb_id exists
                "description": format!("Local file: {}", item.path),
            })
        })
        .collect();

    Json(json!({ "metas": metas }))
}

async fn handle_meta(
    State(state): State<AppState>, Path((type_, id_ext)): Path<(String, String)>,
) -> Json<Value> {
    let id = id_ext.strip_suffix(".json").unwrap_or(&id_ext).to_string();

    // Handle bt: prefix - query engine directly
    if id.starts_with("bt:") {
        let info_hash = id.strip_prefix("bt:").unwrap_or(&id).to_lowercase();
        if let Some(engine) = state.stream_engine().get_engine(&info_hash).await {
            let stats = engine.get_statistics().await;
            let videos: Vec<Value> = stats
                .files
                .iter()
                .enumerate()
                .map(|(idx, file)| {
                    json!({
                        "id": format!("{}:{}", id, idx),
                        "title": file.name.clone(),
                        "released": "2020-01-01T00:00:00.000Z",
                        "stream": {
                            "infoHash": info_hash,
                            "fileIdx": idx,
                        }
                    })
                })
                .collect();

            let meta = json!({
                "id": id,
                "type": type_,
                "name": if stats.name.is_empty() { "Torrent".to_string() } else { stats.name.clone() },
                "poster": Value::Null,
                "description": "Torrent stream",
                "showAsVideos": true,
                "videos": videos,
            });
            return Json(json!({ "meta": meta }));
        }
        return Json(json!({ "meta": null }));
    }

    // Handle local: prefix - use local index
    let items_map = state.local_index.items.read().unwrap();
    if let Some(items) = items_map.get(&id)
        && let Some(first) = items.first()
    {
        // For series, we might want to list videos
        let videos: Vec<Value> = items.iter().map(|it| {
                 json!({
                     "id": format!("{}:{}:{}", id, it.metadata.season.unwrap_or(1), it.metadata.episode.as_ref().map(|v| v[0]).unwrap_or(1)),
                     "title": it.metadata.name.clone(),
                     "season": it.metadata.season.unwrap_or(1),
                     "episode": it.metadata.episode.as_ref().map(|v| v[0]).unwrap_or(1),
                     "released": it.metadata.year.unwrap_or(2020).to_string(),
                 })
             }).collect();

        let meta = json!({
            "id": id,
            "type": type_,
            "name": first.metadata.name,
            "videos": videos,
        });
        return Json(json!({ "meta": meta }));
    }
    Json(json!({ "meta": null }))
}

async fn handle_stream(
    State(state): State<AppState>, Path((_type, id_ext)): Path<(String, String)>,
) -> Json<Value> {
    let id = id_ext.strip_suffix(".json").unwrap_or(&id_ext);

    let parts: Vec<&str> = id.split(':').collect();
    if parts.len() < 2 {
        return Json(json!({ "streams": [] }));
    }

    // Handle bt: prefix - query engine directly
    if parts[0] == "bt" {
        let info_hash = parts[1].to_lowercase();
        let file_idx: usize = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);

        if let Some(engine) = state.stream_engine().get_engine(&info_hash).await {
            let stats = engine.get_statistics().await;
            let file_name = stats
                .files
                .get(file_idx)
                .map(|f| f.name.clone())
                .unwrap_or_else(|| "Unknown".to_string());
            return Json(json!({
                "streams": [{
                    "title": file_name,
                    "infoHash": info_hash,
                    "fileIdx": file_idx,
                    "behaviorHints": {
                        "notWebReady": false
                    }
                }]
            }));
        }
        return Json(json!({ "streams": [] }));
    }

    // Handle local: prefix - use local index
    let base_id = format!("{}:{}", parts[0], parts[1]); // local:ttXXXX

    let items_map = state.local_index.items.read().unwrap();
    if let Some(items) = items_map.get(&base_id) {
        // Filter by S:E if present
        let target_season = parts.get(2).and_then(|s| s.parse::<i32>().ok());
        let target_episode = parts.get(3).and_then(|s| s.parse::<i32>().ok());

        let matched_item = items.iter().find(|it| {
            if target_season.is_some() {
                it.metadata.season == target_season
                    && it
                        .metadata
                        .episode
                        .as_ref()
                        .map(|e| e.contains(&target_episode.unwrap_or(1)))
                        .unwrap_or(false)
            } else {
                true // Movie match
            }
        });

        if let Some(item) = matched_item {
            if let (Some(info_hash), Some(file_idx)) = (&item.info_hash, item.file_idx) {
                // Torrent Stream
                return Json(json!({
                    "streams": [{
                        "title": item.metadata.name.clone().unwrap_or("Torrent File".to_string()),
                        "infoHash": info_hash,
                        "fileIdx": file_idx,
                        "behaviorHints": {
                            "notWebReady": false // Can stream via enginefs
                        }
                    }]
                }));
            } else {
                // Check for archive path
                if item.path.starts_with("archive:") {
                    // path format: archive:{abs_path}|{internal_path}
                    let content = item.path.strip_prefix("archive:").unwrap_or(&item.path);
                    if let Some((archive_path, internal_path)) = content.split_once('|') {
                        // Use /zip/stream generic endpoint (works for all supported types via factory)
                        let url = format!(
                            "{}/zip/stream?archive={}&file={}",
                            state.base_url,
                            urlencoding::encode(archive_path),
                            urlencoding::encode(internal_path)
                        );

                        return Json(json!({
                            "streams": [{
                                "title": item.metadata.name.clone().unwrap_or("Archive File".to_string()),
                                "url": url,
                                "behaviorHints": {
                                    "notWebReady": false, // It IS web ready via our HTTP proxy
                                    "bingeGroup": format!("archive:{}", archive_path)
                                }
                            }]
                        }));
                    }
                }

                // Local File Stream
                return Json(json!({
                    "streams": [{
                        "title": "Local File",
                        "url": format!("file://{}", item.path),
                        "behaviorHints": {
                            "notWebReady": true
                        }
                    }]
                }));
            }
        }
    }

    Json(json!({ "streams": [] }))
}
