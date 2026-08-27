use crate::state::AppState;
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use enginefs::backend::{SubtitleTrack, TorrentHandle};
use serde_json::json;
use tokio::io::AsyncReadExt;

#[derive(serde::Deserialize)]
pub struct OpensubHashQuery {
    #[serde(rename = "videoUrl")]
    pub video_url: Option<String>,
}

pub async fn opensub_hash(
    State(state): State<AppState>, Query(query): Query<OpensubHashQuery>,
) -> impl IntoResponse {
    let url = query.video_url.unwrap_or_default();

    // Heuristic: URL like /:infoHash/:fileIdx/...
    let parts: Vec<&str> = url.split('/').collect();
    let mut info_hash = None;
    let mut file_idx = None;

    for (i, part) in parts.iter().enumerate() {
        if part.len() == 40 && hex::decode(part).is_ok() {
            info_hash = Some(part.to_string());
            if i + 1 < parts.len() {
                // Strip query string if present (e.g., "0?tr=..." -> "0")
                let file_part = parts[i + 1].split('?').next().unwrap_or("");
                if let Ok(idx) = file_part.parse::<usize>() {
                    file_idx = Some(idx);
                }
            }
            break;
        }
    }

    if let (Some(info_hash), Some(file_idx)) = (info_hash, file_idx)
        && let Some(engine) = state.stream_engine().get_engine(&info_hash).await
    {
        match engine.get_opensub_hash(file_idx).await {
            Ok(hash) => {
                let size = engine
                    .handle
                    .get_files()
                    .await
                    .get(file_idx)
                    .map(|file| file.length)
                    .unwrap_or(0);
                return Json(json!({ "error": null, "result": { "hash": hash, "size": size } }));
            }
            Err(e) => return Json(json!({ "error": e.to_string(), "result": null })),
        }
    }

    Json(json!({ "error": "Could not identify file from URL", "result": null }))
}

pub async fn opensub_hash_path(
    State(state): State<AppState>, Path((info_hash, file_idx)): Path<(String, usize)>,
) -> impl IntoResponse {
    let info_hash = info_hash.to_lowercase();
    if let Some(engine) = state.stream_engine().get_engine(&info_hash).await {
        match engine.get_opensub_hash(file_idx).await {
            Ok(hash) => {
                let size = engine
                    .handle
                    .get_files()
                    .await
                    .get(file_idx)
                    .map(|file| file.length)
                    .unwrap_or(0);
                return Json(json!({ "error": null, "result": { "hash": hash, "size": size } }));
            }
            Err(e) => return Json(json!({ "error": e.to_string(), "result": null })),
        }
    }
    Json(json!({ "error": "Engine not found", "result": null }))
}

#[derive(serde::Deserialize)]
pub struct SubtitlesTracksQuery {
    #[serde(rename = "subsUrl")]
    pub subs_url: Option<String>,
}

pub async fn subtitles_tracks(
    State(state): State<AppState>, Query(query): Query<SubtitlesTracksQuery>,
) -> impl IntoResponse {
    let url = query.subs_url.unwrap_or_default();
    let mut info_hash = None;

    for part in url.split('/') {
        if part.len() == 40 && hex::decode(part).is_ok() {
            info_hash = Some(part.to_string());
            break;
        }
    }

    if let Some(info_hash) = info_hash
        && let Some(engine) = state.stream_engine().get_engine(&info_hash).await
    {
        let tracks: Vec<SubtitleTrack> = engine.find_subtitle_tracks().await;

        let result: Vec<serde_json::Value> = tracks
            .into_iter()
            .map(|t| {
                json!({
                    "id": t.id,
                    "lang": "Unknown",
                    "label": t.name,
                    "url": format!("/{}/{}/subtitles.vtt", info_hash, t.id)
                })
            })
            .collect();

        return Json(json!({ "error": null, "result": result }));
    }

    Json(json!({ "error": null, "result": [] }))
}

pub async fn get_subtitles_vtt(
    State(state): State<AppState>, Path((info_hash, file_idx)): Path<(String, usize)>,
) -> Response {
    if let Some(engine) = state.stream_engine().get_engine(&info_hash).await {
        // Check if this is an embedded subtitle track ID (>= 1000)
        // For embedded subs, file_idx is actually the ID from find_subtitle_tracks
        // We need to find the main video file index for extraction
        if file_idx >= 1000 {
            // Find the main video file (largest file)
            let files = engine.handle.get_files().await;
            if let Some((main_file_idx, _)) = files.iter().enumerate().max_by_key(|(_, f)| f.length)
            {
                // Determine track ID (offset by 1000)
                let track_id = file_idx;

                match engine
                    .extract_embedded_subtitle(main_file_idx, track_id)
                    .await
                {
                    Ok(content) => {
                        // FFmpeg output is already VTT, but let's run it through our parser
                        // to ensure consistent styling if needed, or just return as is.
                        // For now, return as is since ffmpeg does a decent job.
                        return Response::builder()
                            .header("content-type", "text/vtt")
                            .header("access-control-allow-origin", "*")
                            .body(axum::body::Body::from(content))
                            .unwrap();
                    }
                    Err(e) => {
                        return Response::builder()
                            .status(500)
                            .body(axum::body::Body::from(format!("Extraction failed: {}", e)))
                            .unwrap();
                    }
                }
            }
        }

        // Regular external file handling
        if let Some(mut file) = engine.get_file(file_idx, 0, 0).await {
            let mut content = String::new();
            if file.read_to_string(&mut content).await.is_ok() {
                // Use the new subtitle parser which handles SRT, ASS, and VTT
                // with proper styling preservation
                let vtt_content = enginefs::subtitles::convert_to_vtt(&content);

                return Response::builder()
                    .header("content-type", "text/vtt")
                    .header("access-control-allow-origin", "*")
                    .body(axum::body::Body::from(vtt_content))
                    .unwrap();
            }
        }
    }
    Response::builder()
        .status(404)
        .body(axum::body::Body::empty())
        .unwrap()
}

#[derive(serde::Deserialize)]
pub struct ProxySubtitlesQuery {
    pub from: Option<String>,
    pub offset: Option<i64>,
}

pub async fn proxy_subtitles_vtt(Query(query): Query<ProxySubtitlesQuery>) -> Response {
    proxy_subtitles_response("vtt", query).await
}

pub async fn proxy_subtitles_ext(
    Path(ext): Path<String>, Query(query): Query<ProxySubtitlesQuery>,
) -> Response {
    proxy_subtitles_response(&ext, query).await
}

async fn proxy_subtitles_response(ext: &str, query: ProxySubtitlesQuery) -> Response {
    let from_url = match query.from {
        Some(url) => url,
        None => {
            return Response::builder()
                .status(400)
                .body(axum::body::Body::from("Missing 'from' parameter"))
                .unwrap();
        }
    };

    // Fetch the subtitle from the external URL
    let client = reqwest::Client::new();
    let resp = match client.get(&from_url).send().await {
        Ok(r) => r,
        Err(e) => {
            return Response::builder()
                .status(502)
                .body(axum::body::Body::from(format!(
                    "Failed to fetch subtitles: {}",
                    e
                )))
                .unwrap();
        }
    };

    let content = match resp.text().await {
        Ok(c) => c,
        Err(e) => {
            return Response::builder()
                .status(502)
                .body(axum::body::Body::from(format!(
                    "Failed to read subtitles: {}",
                    e
                )))
                .unwrap();
        }
    };

    let offset = query.offset.unwrap_or(0);
    let ext = ext.trim_start_matches('.').to_ascii_lowercase();
    match ext.as_str() {
        "vtt" => {
            let vtt_content = enginefs::subtitles::convert_to_vtt(&content);
            let shifted = apply_subtitle_offset(&vtt_content, offset);
            Response::builder()
                .header("content-type", "text/vtt")
                .header("access-control-allow-origin", "*")
                .body(axum::body::Body::from(shifted))
                .unwrap()
        }
        "srt" => {
            let shifted = apply_subtitle_offset(&content, offset);
            Response::builder()
                .header("content-type", "application/x-subrip")
                .header("access-control-allow-origin", "*")
                .body(axum::body::Body::from(shifted))
                .unwrap()
        }
        _ => (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            format!("Unsupported subtitle extension: {ext}"),
        )
            .into_response(),
    }
}

fn apply_subtitle_offset(content: &str, offset_ms: i64) -> String {
    if offset_ms == 0 {
        return content.to_string();
    }

    let regex = match regex::Regex::new(r"(\d{2}):(\d{2}):(\d{2})([,.])(\d{3})") {
        Ok(regex) => regex,
        Err(_) => return content.to_string(),
    };

    regex
        .replace_all(content, |caps: &regex::Captures| {
            let hours = caps[1].parse::<i64>().unwrap_or(0);
            let minutes = caps[2].parse::<i64>().unwrap_or(0);
            let seconds = caps[3].parse::<i64>().unwrap_or(0);
            let millis = caps[5].parse::<i64>().unwrap_or(0);
            let total =
                (hours * 3_600_000 + minutes * 60_000 + seconds * 1_000 + millis + offset_ms)
                    .max(0);
            let h = total / 3_600_000;
            let m = (total % 3_600_000) / 60_000;
            let s = (total % 60_000) / 1_000;
            let ms = total % 1_000;
            format!("{h:02}:{m:02}:{s:02}{}{ms:03}", &caps[4])
        })
        .to_string()
}
