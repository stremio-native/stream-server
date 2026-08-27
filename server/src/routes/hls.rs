use crate::routes::compat;
use crate::state::AppState;
use axum::{
    Json,
    body::Body,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use enginefs::backend::TorrentHandle;
use serde::Deserialize;
use tokio_util::io::ReaderStream;

#[derive(Deserialize)]
pub struct ProbeQuery {
    #[serde(rename = "mediaURL")]
    pub media_url: Option<String>,
    pub url: Option<String>,
}

fn stremio_format_name(container: &str) -> String {
    match container {
        "matroska" | "webm" => "matroska,webm".to_string(),
        other => other.to_string(),
    }
}

fn hls_stream_url(base_url: &str, info_hash: &str, file_idx: usize) -> String {
    format!(
        "{}/stream/{}/{}?enginefs-intent=hls",
        base_url.trim_end_matches('/'),
        info_hash,
        file_idx
    )
}

fn stremio_probe_json(
    info_hash: &str, file_idx: usize, probe: &enginefs::hls::ProbeResult,
) -> serde_json::Value {
    serde_json::json!({
        "infoHash": info_hash,
        "fileIdx": file_idx,
        "format": {
            "name": stremio_format_name(&probe.container)
        },
        "duration": probe.duration,
        "streams": probe.streams.iter().map(|stream| {
            serde_json::json!({
                "index": stream.index,
                "track": stream.codec_type,
                "codec": stream.codec_name,
                "channels": if stream.codec_type == "audio" { stream.channels.unwrap_or(2) } else { 0 },
                "width": stream.width,
                "height": stream.height,
                "fps": stream.fps,
                "bitrate": stream.bitrate,
                "lang": stream.lang,
                "default": stream.is_default,
                "profile": stream.profile,
            })
        }).collect::<Vec<_>>()
    })
}

/// Probe endpoint using mediaURL query parameter (for Stremio compatibility)
/// GET /hlsv2/probe?mediaURL=http://127.0.0.1:11470/{infoHash}/{fileIdx}?
pub async fn probe_by_url(
    State(state): State<AppState>, Query(params): Query<ProbeQuery>,
) -> Response {
    let start = std::time::Instant::now();
    let Some(media_url) = params.media_url.or(params.url) else {
        return (StatusCode::BAD_REQUEST, "Missing mediaURL").into_response();
    };
    let media_url = normalize_media_url(&media_url, &state.base_url);
    let (info_hash, requested_idx) = match compat::parse_media_url(&media_url) {
        Ok(parts) => parts,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };

    let engine = match state.stream_engine().get_or_add_engine(&info_hash).await {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to load engine: {}", e),
            )
                .into_response();
        }
    };
    let file_idx = match resolve_hls_file_idx(&engine, &requested_idx, &media_url).await {
        Ok(idx) => idx,
        Err(err) => return (StatusCode::NOT_FOUND, err).into_response(),
    };

    state
        .stream_engine()
        .refresh_hls_playback(&info_hash, file_idx, "hls-probe-url")
        .await;
    let probe = match engine.get_probe_result(file_idx, &media_url).await {
        Ok(p) => p,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Probe failed: {}", e),
            )
                .into_response();
        }
    };

    let elapsed = start.elapsed();
    tracing::info!(
        info_hash = %info_hash,
        file_idx,
        elapsed_ms = elapsed.as_millis() as u64,
        "HLS probe request completed"
    );

    Json(stremio_probe_json(&info_hash, file_idx, &probe)).into_response()
}

/// Query params for HLS endpoints
#[derive(Deserialize)]
pub struct HlsQuery {
    #[serde(rename = "mediaURL")]
    pub media_url: Option<String>,
}

/// Master playlist endpoint using mediaURL query parameter (for Stremio compatibility)
/// GET /hlsv2/{hash}/master.m3u8?mediaURL=http://127.0.0.1:11470/{infoHash}/{fileIdx}?
pub async fn master_playlist_by_url(
    State(state): State<AppState>, Path(_hash): Path<String>, _headers: axum::http::HeaderMap,
    axum::extract::Query(params): axum::extract::Query<HlsQuery>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
) -> Response {
    let Some(media_url) = params.media_url else {
        return (StatusCode::BAD_REQUEST, "Missing mediaURL").into_response();
    };
    let media_url = normalize_media_url(&media_url, &state.base_url);
    let (info_hash, requested_idx) = match compat::parse_media_url(&media_url) {
        Ok(parts) => parts,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };

    let engine = match state.stream_engine().get_or_add_engine(&info_hash).await {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to load engine: {}", e),
            )
                .into_response();
        }
    };
    let file_idx = match resolve_hls_file_idx(&engine, &requested_idx, &media_url).await {
        Ok(idx) => idx,
        Err(err) => return (StatusCode::NOT_FOUND, err).into_response(),
    };

    state
        .stream_engine()
        .refresh_hls_playback(&info_hash, file_idx, "hls-master-url")
        .await;
    if !engine.handle.manages_playback_lifecycle() {
        state.stream_engine().focus_torrent(&info_hash).await;
    }

    let probe = match engine.get_probe_result(file_idx, &media_url).await {
        Ok(p) => p,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Probe failed: {}", e),
            )
                .into_response();
        }
    };

    // Generate master playlist - use the REAL info_hash from mediaURL, not the path hash
    // This ensures subsequent stream/segment requests use the correct torrent identifier
    // Base URL for segments
    let query_str = raw_query.as_deref().unwrap_or("");

    let playlist = enginefs::hls::HlsEngine::get_master_playlist(
        &probe,
        &info_hash,
        file_idx,
        &state.base_url,
        query_str,
        &[],
    );

    (
        [(
            axum::http::header::CONTENT_TYPE,
            "application/vnd.apple.mpegurl",
        )],
        playlist,
    )
        .into_response()
}

pub async fn get_master_playlist(
    State(state): State<AppState>, Path((info_hash, file_idx)): Path<(String, usize)>,
    _headers: axum::http::HeaderMap, axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
) -> Response {
    let request_start = std::time::Instant::now();
    let engine = match state.stream_engine().get_or_add_engine(&info_hash).await {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to load engine: {}", e),
            )
                .into_response();
        }
    };

    state
        .stream_engine()
        .refresh_hls_playback(&info_hash, file_idx, "hls-master")
        .await;
    if !engine.handle.manages_playback_lifecycle() {
        state.stream_engine().focus_torrent(&info_hash).await;
    }

    let stream_url = hls_stream_url(&state.base_url, &info_hash, file_idx);

    let probe = match engine.get_probe_result(file_idx, &stream_url).await {
        Ok(p) => p,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    let query_str = raw_query.as_deref().unwrap_or("");
    let subtitle_tracks = engine.find_subtitle_tracks().await;

    let playlist = enginefs::hls::HlsEngine::get_master_playlist(
        &probe,
        &info_hash,
        file_idx,
        &state.base_url,
        query_str,
        &subtitle_tracks,
    );

    tracing::info!(
        "startup: HLS master playlist ready in {:?} (streams={}, subtitles={})",
        request_start.elapsed(),
        probe.streams.len(),
        subtitle_tracks.len()
    );

    (
        [
            (
                axum::http::header::CONTENT_TYPE,
                "application/vnd.apple.mpegurl",
            ),
            (
                axum::http::header::CACHE_CONTROL,
                "no-cache, no-store, must-revalidate",
            ),
        ],
        playlist,
    )
        .into_response()
}

// Dispatcher for stream playlist or segment to avoid route conflict
// {file_idx} can be numeric (0, 1) or text (subtitle4, audio-1)
pub async fn handle_hls_resource(
    State(state): State<AppState>, _headers: axum::http::HeaderMap,
    Path((info_hash, file_idx_str, resource)): Path<(String, String, String)>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
) -> Response {
    if let Some((real_info_hash, real_file_idx)) = compat::parse_hls_id(&info_hash) {
        if resource == "init.mp4" {
            return compat::unsupported("hlsv2 fMP4 init segments");
        }

        if let Some(segment) = hls_v2_segment_alias(&resource) {
            return get_segment(
                State(state),
                _headers,
                real_info_hash,
                real_file_idx,
                segment,
                raw_query,
            )
            .await;
        }

        if resource.starts_with("segment") {
            return compat::unsupported("hlsv2 fMP4 media segments");
        }

        if resource.ends_with(".m3u8") {
            return get_stream_playlist(
                State(state),
                real_info_hash,
                real_file_idx,
                resource,
                raw_query,
            )
            .await;
        }

        return compat::unsupported("hlsv2 track resource");
    }

    // Parse file_idx - default to 0 if non-numeric (e.g., "subtitle4", "audio-1")
    let file_idx: usize = file_idx_str.parse().unwrap_or(0);

    if resource.ends_with(".m3u8") {
        get_stream_playlist(State(state), info_hash, file_idx, resource, raw_query).await
    } else {
        get_segment(
            State(state),
            _headers,
            info_hash,
            file_idx,
            resource,
            raw_query,
        )
        .await
    }
}

// Handler for fMP4 nested paths like /hlsv2/{infoHash}/{fileIdx}/video0/init.mp4
pub async fn handle_hls_fmp4_segment(
    State(state): State<AppState>, _headers: axum::http::HeaderMap,
    Path((info_hash, file_idx_str, track, segment)): Path<(String, String, String, String)>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
) -> Response {
    let file_idx: usize = file_idx_str.parse().unwrap_or(0);
    // Combine track and segment into resource path format expected by get_segment
    let resource = format!("{}/{}", track, segment);

    get_segment(
        State(state),
        _headers,
        info_hash,
        file_idx,
        resource,
        raw_query,
    )
    .await
}

async fn get_stream_playlist(
    State(state): State<AppState>, info_hash: String, file_idx: usize, playlist_name: String,
    raw_query: Option<String>,
) -> Response {
    let engine = match state.stream_engine().get_or_add_engine(&info_hash).await {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to load engine: {}", e),
            )
                .into_response();
        }
    };

    state
        .stream_engine()
        .refresh_hls_playback(&info_hash, file_idx, "hls-stream-playlist")
        .await;

    let stream_url = hls_stream_url(&state.base_url, &info_hash, file_idx);
    let probe = match engine.get_probe_result(file_idx, &stream_url).await {
        Ok(p) => p,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    let segment_base_url = "./";

    // Check if this is an audio playlist
    // Format: "audio-{idx}.m3u8"
    let audio_track_idx = if playlist_name.starts_with("audio-") {
        playlist_name
            .trim_start_matches("audio-")
            .trim_end_matches(".m3u8")
            .parse::<usize>()
            .ok()
    } else {
        None
    };

    let playlist = enginefs::hls::HlsEngine::get_stream_playlist(
        &probe,
        0,
        segment_base_url,
        audio_track_idx,
        raw_query.as_deref().unwrap_or(""),
    );
    if !probe.is_hls_ready() {
        tracing::warn!(
            info_hash = %info_hash,
            file_idx,
            playlist = %playlist_name,
            streams = probe.streams.len(),
            duration = probe.duration,
            "HLS media playlist returned reloadable empty playlist while probe metadata is incomplete"
        );
    }

    (
        [
            (
                axum::http::header::CONTENT_TYPE,
                "application/vnd.apple.mpegurl",
            ),
            (
                axum::http::header::CACHE_CONTROL,
                "no-cache, no-store, must-revalidate",
            ),
        ],
        playlist,
    )
        .into_response()
}

async fn get_segment(
    State(state): State<AppState>, _headers: axum::http::HeaderMap, info_hash: String,
    file_idx: usize, segment: String, _raw_query: Option<String>,
) -> Response {
    let request_start = std::time::Instant::now();
    let engine = match state.stream_engine().get_or_add_engine(&info_hash).await {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to load engine: {}", e),
            )
                .into_response();
        }
    };

    // Parse segment type from filename
    // Video: "0.ts", "1.ts", etc.
    // Audio: "audio-{track_idx}-{segment_idx}.ts"
    let segment_name = segment.trim_end_matches(".ts");
    let (seg_index, audio_track_idx) = if segment_name.starts_with("audio-") {
        let parts: Vec<&str> = segment_name.split('-').collect();
        if parts.len() >= 3 {
            let track_idx = parts[1].parse::<usize>().ok();
            let seg_idx = parts[2].parse::<usize>().unwrap_or(0);
            (seg_idx, track_idx)
        } else {
            return (StatusCode::BAD_REQUEST, "Invalid audio segment format").into_response();
        }
    } else {
        match segment_name.parse::<usize>() {
            Ok(i) => (i, None),
            Err(_) => return (StatusCode::BAD_REQUEST, "Invalid segment").into_response(),
        }
    };

    state
        .stream_engine()
        .refresh_hls_playback(&info_hash, file_idx, "hls-segment")
        .await;

    let stream_url = hls_stream_url(&state.base_url, &info_hash, file_idx);

    // Use local file path only if the file is fully downloaded.
    // If not fully downloaded, stream via HTTP loopback to trigger piece downloading on demand.
    let is_fully_downloaded = if engine.handle.manages_playback_lifecycle() {
        engine.handle.is_file_complete(file_idx).await
    } else {
        let stats = engine.handle.stats().await;
        stats
            .files
            .get(file_idx)
            .map(|f| f.progress >= 0.99)
            .unwrap_or(false)
    };

    let transcode_input_path = if is_fully_downloaded {
        engine
            .handle
            .get_file_path(file_idx)
            .await
            .unwrap_or_else(|| stream_url.clone())
    } else {
        stream_url.clone()
    };

    let probe = match engine.get_probe_result(file_idx, &stream_url).await {
        Ok(p) => p,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    let segments = enginefs::hls::HlsEngine::get_segments(probe.duration);
    let (start, duration) = match segments.get(seg_index) {
        Some(&s) => s,
        None => return (StatusCode::NOT_FOUND, "Segment out of bounds").into_response(),
    };

    // Get transcode_profile from settings and probe available hardware accelerators
    let transcode_profile = {
        let settings = state.settings.read().await;
        settings.transcode_profile.clone()
    };

    // Probe available hardware accelerators (cached in practice via the system endpoint)
    let available_hwaccels = crate::routes::system::probe_hwaccel().await;
    let mut config = enginefs::hls::TranscodeConfig::with_hwaccel(
        &available_hwaccels,
        transcode_profile.as_deref(),
    );
    config.is_high_bit_depth = probe.has_high_bit_depth_video();
    // Dispatch to video or audio transcoding based on segment type
    let mut child = if let Some(audio_idx) = audio_track_idx {
        // Audio-only segment
        tracing::debug!(
            "Transcoding audio segment: track={}, segment={}, start={:.2}s, input={}",
            audio_idx,
            seg_index,
            start,
            transcode_input_path
        );
        match enginefs::hls::HlsEngine::transcode_audio_segment(
            &transcode_input_path,
            start,
            duration,
            audio_idx,
            &config,
        )
        .await
        {
            Ok(c) => c,
            Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        }
    } else {
        // Video-only segment
        tracing::debug!(
            "Transcoding video segment: segment={}, start={:.2}s",
            seg_index,
            start
        );

        match enginefs::hls::HlsEngine::transcode_video_segment(
            &transcode_input_path,
            start,
            duration,
            &config,
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                if config.uses_hardware_encoder() {
                    tracing::warn!(error = %e, "Hardware transcoding spawn failed, falling back to software");
                    config.force_software_encoder("hardware spawn failed");
                    match enginefs::hls::HlsEngine::transcode_video_segment(
                        &transcode_input_path,
                        start,
                        duration,
                        &config,
                    )
                    .await
                    {
                        Ok(c) => c,
                        Err(e) => {
                            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
                                .into_response();
                        }
                    }
                } else {
                    return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
                }
            }
        }
    };

    // If using hardware encoder, verify the process did not crash immediately during startup
    if audio_track_idx.is_none() && config.uses_hardware_encoder() {
        let mut startup_failed = false;
        for _ in 0..10 {
            match child.inner.try_wait() {
                Ok(Some(status)) => {
                    if !status.success() {
                        startup_failed = true;
                    }
                    break;
                }
                Ok(None) => {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
                Err(_) => {
                    startup_failed = true;
                    break;
                }
            }
        }

        if startup_failed {
            tracing::warn!("Hardware transcoding crashed on startup, retrying with software");
            config.force_software_encoder("hardware process crashed on startup");
            child = match enginefs::hls::HlsEngine::transcode_video_segment(
                &transcode_input_path,
                start,
                duration,
                &config,
            )
            .await
            {
                Ok(c) => c,
                Err(e) => {
                    return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
                }
            };
        }
    }

    let stdout = match enginefs::hls::TranscodeStream::new(child) {
        Some(s) => s,
        None => return (StatusCode::INTERNAL_SERVER_ERROR, "No stdout").into_response(),
    };

    let stream = ReaderStream::new(stdout);
    let body = Body::from_stream(stream);

    tracing::info!(
        "startup: HLS segment response ready in {:?} (segment={}, start={:.2}s, duration={:.2}s)",
        request_start.elapsed(),
        seg_index,
        start,
        duration
    );

    // MPEG-TS content type for HLS segments
    (
        [
            (axum::http::header::CONTENT_TYPE, "video/mp2t"),
            (
                axum::http::header::CACHE_CONTROL,
                "public, max-age=3600, immutable",
            ),
        ],
        body,
    )
        .into_response()
}

pub async fn get_probe(
    State(state): State<AppState>, Path((info_hash, file_idx)): Path<(String, usize)>,
) -> Response {
    let engine = match state.stream_engine().get_engine(&info_hash).await {
        Some(e) => e,
        None => return (StatusCode::NOT_FOUND, "Engine not found").into_response(),
    };

    state
        .stream_engine()
        .refresh_hls_playback(&info_hash, file_idx, "hls-probe")
        .await;
    let stream_url = hls_stream_url(&state.base_url, &info_hash, file_idx);
    let probe = match engine.get_probe_result(file_idx, &stream_url).await {
        Ok(p) => p,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    Json(probe).into_response()
}

fn normalize_media_url(media_url: &str, base_url: &str) -> String {
    if media_url.starts_with('/') {
        format!("{}{}", base_url.trim_end_matches('/'), media_url)
    } else {
        media_url.trim_end_matches('?').to_string()
    }
}

async fn resolve_hls_file_idx<H: TorrentHandle>(
    engine: &std::sync::Arc<enginefs::engine::Engine<H>>, requested_idx: &str, media_url: &str,
) -> Result<usize, String> {
    let filters = url::Url::parse(media_url)
        .ok()
        .map(|url| compat::query_values(url.query(), "f"))
        .unwrap_or_default();
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
    compat::resolve_file_idx(requested_idx, &candidates, &filters)
}

pub async fn get_tracks_by_url(State(state): State<AppState>, Path(url): Path<String>) -> Response {
    let decoded = urlencoding::decode(&url)
        .map(|value| value.into_owned())
        .unwrap_or(url);

    if let Some((info_hash, file_idx)) = parse_tracks_path(&decoded) {
        let engine = match state.stream_engine().get_engine(&info_hash).await {
            Some(engine) => engine,
            None => return (StatusCode::NOT_FOUND, "Engine not found").into_response(),
        };

        state
            .stream_engine()
            .refresh_hls_playback(&info_hash, file_idx, "hls-tracks")
            .await;
        let stream_url = hls_stream_url(&state.base_url, &info_hash, file_idx);
        let probe = match engine.get_probe_result(file_idx, &stream_url).await {
            Ok(probe) => probe,
            Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        };
        return Json(probe.streams).into_response();
    }

    let media_url = normalize_media_url(&decoded, &state.base_url);
    let (info_hash, requested_idx) = match compat::parse_media_url(&media_url) {
        Ok(parts) => parts,
        Err(err) => {
            tracing::warn!(error = %err, path = media_url.split('?').next(), "tracks URL parse failed");
            return Json(Vec::<serde_json::Value>::new()).into_response();
        }
    };

    let engine = match state.stream_engine().get_engine(&info_hash).await {
        Some(engine) => engine,
        None => return Json(Vec::<serde_json::Value>::new()).into_response(),
    };
    let file_idx = match resolve_hls_file_idx(&engine, &requested_idx, &media_url).await {
        Ok(idx) => idx,
        Err(err) => {
            tracing::warn!(error = %err, path = media_url.split('?').next(), "tracks file index resolve failed");
            return Json(Vec::<serde_json::Value>::new()).into_response();
        }
    };
    state
        .stream_engine()
        .refresh_hls_playback(&info_hash, file_idx, "hls-tracks-url")
        .await;
    let probe = match engine.get_probe_result(file_idx, &media_url).await {
        Ok(probe) => probe,
        Err(err) => {
            tracing::warn!(error = %err, path = media_url.split('?').next(), "tracks probe failed");
            return Json(Vec::<serde_json::Value>::new()).into_response();
        }
    };
    Json(probe.streams).into_response()
}

fn parse_tracks_path(path: &str) -> Option<(String, usize)> {
    let path = path.trim_start_matches('/');
    let mut parts = path.split('/');
    let info_hash = parts.next()?;
    let file_idx = parts.next()?;
    if parts.next().is_some() || !compat::is_info_hash(info_hash) {
        return None;
    }
    file_idx
        .parse::<usize>()
        .ok()
        .map(|idx| (info_hash.to_ascii_lowercase(), idx))
}

pub async fn hls_status() -> Response {
    Json(serde_json::json!({})).into_response()
}

pub async fn hls_destroy(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    if let Some((info_hash, file_idx)) = compat::parse_hls_id(&id) {
        state
            .stream_engine()
            .end_hls_playback(&info_hash, file_idx, "hls-destroy")
            .await;
    }
    tracing::info!(id = %id, "hls converter destroy requested; no persistent converter is used");
    compat::empty_ok()
}

pub async fn hls_burn(Path(id): Path<String>) -> Response {
    tracing::warn!(id = %id, "hls subtitle burn-in is not implemented");
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .unwrap()
}

pub async fn hls_v2_resource(
    State(state): State<AppState>, Path((id, resource)): Path<(String, String)>,
    headers: axum::http::HeaderMap, raw_query: axum::extract::RawQuery,
) -> Response {
    let Some((info_hash, file_idx)) = compat::parse_hls_id(&id) else {
        return compat::unsupported("hlsv2 arbitrary converter playlist");
    };

    if resource == "init.mp4" || resource.starts_with("segment") {
        if let Some(segment) = hls_v2_segment_alias(&resource) {
            return get_segment(
                State(state),
                headers,
                info_hash,
                file_idx,
                segment,
                raw_query.0,
            )
            .await;
        }
        return compat::unsupported("hlsv2 fMP4 media segments");
    }

    if !resource.ends_with(".m3u8") {
        return (StatusCode::NOT_FOUND, "HLS resource not found").into_response();
    }

    let track = resource.trim_end_matches(".m3u8");
    if track == "master" || track == "hls" {
        get_master_playlist(
            State(state),
            Path((info_hash, file_idx)),
            headers,
            raw_query,
        )
        .await
    } else {
        get_stream_playlist(State(state), info_hash, file_idx, resource, raw_query.0).await
    }
}

pub async fn legacy_hls_resource(
    State(state): State<AppState>, method: axum::http::Method, uri: axum::http::Uri,
    axum::extract::ConnectInfo(peer): axum::extract::ConnectInfo<std::net::SocketAddr>,
    Path((first, second, resource)): Path<(String, String, String)>,
    headers: axum::http::HeaderMap, raw_query: axum::extract::RawQuery,
) -> Response {
    let Some((info_hash, requested_idx)) = legacy_pair(&first, &second) else {
        crate::diagnostics::logging::log_unhandled(
            "matched legacy HLS resource wildcard but not a valid info-hash/idx pair (404)",
            StatusCode::NOT_FOUND.as_u16(),
            Some(peer),
            &method,
            &uri,
            None,
            &headers,
        );
        return (
            StatusCode::NOT_FOUND,
            "legacy file/url HLS resource not found",
        )
            .into_response();
    };
    let file_idx = match resolve_legacy_file_idx(&state, &info_hash, &requested_idx).await {
        Ok(idx) => idx,
        Err(response) => return *response,
    };

    match resource.as_str() {
        "hls.m3u8" | "master.m3u8" => {
            get_master_playlist(
                State(state),
                Path((info_hash, file_idx)),
                headers,
                raw_query,
            )
            .await
        }
        "stream.m3u8" => {
            get_stream_playlist(
                State(state),
                info_hash,
                file_idx,
                "stream-0.m3u8".to_string(),
                raw_query.0,
            )
            .await
        }
        "thumb.jpg" => (StatusCode::NOT_FOUND, "thumbnail not available").into_response(),
        "dlna" => compat::unsupported("legacy HLS DLNA discovery"),
        resource if legacy_stream_playlist(resource) => {
            get_stream_playlist(
                State(state),
                info_hash,
                file_idx,
                resource.to_string(),
                raw_query.0,
            )
            .await
        }
        resource if resource.starts_with("subs-") => {
            compat::unsupported("legacy HLS subtitle playlist")
        }
        resource if resource.starts_with("mp4stream") => {
            compat::unsupported("legacy HLS MP4 segments")
        }
        resource if resource.ends_with(".ts") => {
            get_segment(
                State(state),
                headers,
                info_hash,
                file_idx,
                resource.to_string(),
                raw_query.0,
            )
            .await
        }
        _ => {
            crate::diagnostics::logging::log_unhandled(
                "legacy HLS resource resolved torrent but resource is unknown (404)",
                StatusCode::NOT_FOUND.as_u16(),
                Some(peer),
                &method,
                &uri,
                None,
                &headers,
            );
            (StatusCode::NOT_FOUND, "legacy HLS resource not found").into_response()
        }
    }
}

pub async fn legacy_hls_segment(
    State(state): State<AppState>, method: axum::http::Method, uri: axum::http::Uri,
    axum::extract::ConnectInfo(peer): axum::extract::ConnectInfo<std::net::SocketAddr>,
    headers: axum::http::HeaderMap,
    Path((first, second, variant, seg)): Path<(String, String, String, String)>,
    raw_query: axum::extract::RawQuery,
) -> Response {
    let Some((info_hash, requested_idx)) = legacy_pair(&first, &second) else {
        crate::diagnostics::logging::log_unhandled(
            "matched legacy HLS segment wildcard but not a valid info-hash/idx pair (404)",
            StatusCode::NOT_FOUND.as_u16(),
            Some(peer),
            &method,
            &uri,
            None,
            &headers,
        );
        return (
            StatusCode::NOT_FOUND,
            "legacy file/url HLS segment not found",
        )
            .into_response();
    };
    let file_idx = match resolve_legacy_file_idx(&state, &info_hash, &requested_idx).await {
        Ok(idx) => idx,
        Err(response) => return *response,
    };

    if variant.starts_with("mp4stream") {
        return compat::unsupported("legacy HLS MP4 segments");
    }

    if variant == "stream" || variant.starts_with("stream-") || variant.starts_with("stream-q-") {
        return get_segment(State(state), headers, info_hash, file_idx, seg, raw_query.0).await;
    }

    compat::unsupported("legacy HLS segment variant")
}

fn legacy_pair(first: &str, second: &str) -> Option<(String, String)> {
    if compat::is_info_hash(first) && (second == "-1" || second.parse::<usize>().is_ok()) {
        Some((first.to_ascii_lowercase(), second.to_string()))
    } else {
        None
    }
}

/// Resolve a legacy path file index, accepting `-1` as "auto-pick the best
/// video file". Autoplay requests the next episode with `-1` because the
/// client does not know the file index before the torrent metadata loads.
async fn resolve_legacy_file_idx(
    state: &AppState, info_hash: &str, requested_idx: &str,
) -> Result<usize, Box<Response>> {
    if let Ok(idx) = requested_idx.parse::<usize>() {
        return Ok(idx);
    }

    let engine = state
        .stream_engine()
        .get_or_add_engine(info_hash)
        .await
        .map_err(|e| {
            Box::new(
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to load engine: {}", e),
                )
                    .into_response(),
            )
        })?;
    resolve_hls_file_idx(&engine, requested_idx, "")
        .await
        .map_err(|err| Box::new((StatusCode::NOT_FOUND, err).into_response()))
}

fn legacy_stream_playlist(resource: &str) -> bool {
    resource.ends_with(".m3u8")
        && (resource.starts_with("stream-") || resource.starts_with("stream-q-"))
}

fn hls_v2_segment_alias(resource: &str) -> Option<String> {
    if !resource.ends_with(".ts") {
        return None;
    }
    let segment = resource.strip_prefix("segment").unwrap_or(resource);
    if segment.trim_end_matches(".ts").parse::<usize>().is_ok() {
        Some(segment.to_string())
    } else {
        None
    }
}
