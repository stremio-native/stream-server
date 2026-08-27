//! Resolves YouTube videos to a directly playable URL.
//!
//! YouTube no longer serves a combined audio+video format to most clients, and
//! the adaptive URLs it does serve are only usable by whichever client fetched
//! them. `yt-dlp` is the only extractor that keeps up with both, so resolution
//! is delegated to it and the result is cached until the signed URLs expire.

use crate::state::AppState;
use crate::ytdlp;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Redirect},
    routing::get,
};
use once_cell::sync::Lazy;
use serde::Deserialize;
use std::time::{Duration, Instant};
use tracing::warn;

/// Signed googlevideo URLs outlive this comfortably, so a cache hit is always
/// still playable; the bound exists to keep hovering over a poster from
/// spawning an extractor process per frame.
const CACHE_TTL: Duration = Duration::from_secs(30 * 60);

/// Prefer a single file that already carries audio and video. `b` falls back to
/// the best progressive format when the mp4 variant is missing; formats that
/// only exist as separate streams are rejected, because the caller expects one
/// URL it can hand straight to a player.
const FORMAT_SELECTOR: &str =
    "b[ext=mp4][vcodec!=none][acodec!=none]/b[vcodec!=none][acodec!=none]";

static CACHE: Lazy<dashmap::DashMap<String, (Instant, ResolvedStream)>> =
    Lazy::new(dashmap::DashMap::new);

pub fn router() -> Router<AppState> {
    Router::new().route("/{id}", get(youtube_handler))
}

pub async fn youtube_handler(
    State(state): State<AppState>, Path(id): Path<String>,
) -> impl IntoResponse {
    let (video_id, as_json) = match id.strip_suffix(".json") {
        Some(stripped) => (stripped.to_owned(), true),
        None => (id, false),
    };

    if !is_video_id(&video_id) {
        return (StatusCode::BAD_REQUEST, "Invalid YouTube ID").into_response();
    }

    let stream = match resolve(&state, &video_id).await {
        Ok(stream) => stream,
        Err(error) => return error.into_response(),
    };

    if as_json {
        Json(stream.to_json()).into_response()
    } else {
        Redirect::temporary(&stream.url).into_response()
    }
}

/// YouTube IDs are 11 URL-safe base64 characters. Validating up front keeps
/// anything caller-controlled out of the extractor's argument list.
fn is_video_id(id: &str) -> bool {
    id.len() == 11
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

async fn resolve(state: &AppState, video_id: &str) -> Result<ResolvedStream, ResolveError> {
    if let Some(entry) = CACHE.get(video_id)
        && entry.0.elapsed() < CACHE_TTL
    {
        return Ok(entry.1.clone());
    }

    let binary = ytdlp::resolve(&state.config_dir).await.map_err(|error| {
        warn!(%error, "yt-dlp is unavailable");
        ResolveError::Unavailable(error.to_string())
    })?;

    let output = ytdlp::command(&binary)
        .args([
            "--quiet",
            "--no-warnings",
            "--no-playlist",
            "--dump-single-json",
            "--format",
            FORMAT_SELECTOR,
            &format!("https://www.youtube.com/watch?v={video_id}"),
        ])
        .output()
        .await
        .map_err(|error| {
            warn!(%error, path = %binary.display(), "could not run yt-dlp");
            ResolveError::Unavailable(format!("could not run yt-dlp: {error}"))
        })?;

    if !output.status.success() {
        let detail = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        warn!(video_id, detail, "yt-dlp could not resolve the video");
        return Err(ResolveError::Extraction(detail));
    }

    let info: YtDlpInfo = serde_json::from_slice(&output.stdout).map_err(|error| {
        warn!(%error, video_id, "could not parse yt-dlp output");
        ResolveError::Extraction(format!("could not parse yt-dlp output: {error}"))
    })?;

    let stream = ResolvedStream::from_info(info).ok_or_else(|| {
        warn!(video_id, "no combined audio+video format is available");
        ResolveError::NoFormat
    })?;

    CACHE.insert(video_id.to_owned(), (Instant::now(), stream.clone()));
    Ok(stream)
}

enum ResolveError {
    /// The extractor itself could not be obtained or started.
    Unavailable(String),
    /// The extractor ran but could not produce stream information.
    Extraction(String),
    /// The video has no format carrying both audio and video.
    NoFormat,
}

impl IntoResponse for ResolveError {
    fn into_response(self) -> axum::response::Response {
        match self {
            Self::Unavailable(detail) => (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("YouTube support is unavailable: {detail}"),
            ),
            Self::Extraction(detail) => (
                StatusCode::BAD_GATEWAY,
                format!("Failed to get video info: {detail}"),
            ),
            Self::NoFormat => (
                StatusCode::NOT_FOUND,
                "No suitable video format found".to_owned(),
            ),
        }
        .into_response()
    }
}

/// The subset of `yt-dlp --dump-single-json` this route depends on. With a
/// single-file `--format`, the selected format is flattened onto the top level.
#[derive(Deserialize)]
struct YtDlpInfo {
    url: Option<String>,
    format_id: Option<String>,
    ext: Option<String>,
    height: Option<u32>,
    format_note: Option<String>,
    vcodec: Option<String>,
    acodec: Option<String>,
    duration: Option<f64>,
    protocol: Option<String>,
    is_live: Option<bool>,
}

#[derive(Clone)]
struct ResolvedStream {
    url: String,
    itag: Option<u64>,
    quality: String,
    container: String,
    mime_type: String,
    has_video: bool,
    has_audio: bool,
    is_live: bool,
    is_hls: bool,
    is_dash: bool,
    duration_ms: Option<u64>,
}

impl ResolvedStream {
    fn from_info(info: YtDlpInfo) -> Option<Self> {
        let url = info.url?;
        let has_video = is_present(info.vcodec.as_deref());
        let has_audio = is_present(info.acodec.as_deref());
        if !has_video || !has_audio {
            return None;
        }

        let container = info.ext.unwrap_or_else(|| "mp4".to_owned());
        let protocol = info.protocol.unwrap_or_default();

        Some(Self {
            url,
            itag: info.format_id.as_deref().and_then(|id| id.parse().ok()),
            quality: info
                .format_note
                .filter(|note| !note.is_empty())
                .or_else(|| info.height.map(|height| format!("{height}p")))
                .unwrap_or_else(|| "unknown".to_owned()),
            mime_type: mime_type(&container, info.vcodec.as_deref(), info.acodec.as_deref()),
            container,
            has_video,
            has_audio,
            is_live: info.is_live.unwrap_or(false),
            is_hls: protocol.contains("m3u8"),
            is_dash: protocol.contains("dash"),
            duration_ms: info.duration.map(|seconds| (seconds * 1000.0) as u64),
        })
    }

    /// Mirrors the response shape the previous implementation returned, so
    /// existing `/yt/<id>.json` callers keep working.
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "url": self.url,
            "itag": self.itag,
            "quality": self.quality,
            "container": self.container,
            "hasVideo": self.has_video,
            "hasAudio": self.has_audio,
            "isLive": self.is_live,
            "isHLS": self.is_hls,
            "isDashMPD": self.is_dash,
            "approxDurationMs": self.duration_ms.map(|ms| ms.to_string()),
            "mimeType": self.mime_type,
        })
    }
}

/// yt-dlp reports a missing track as the literal string `none`.
fn is_present(codec: Option<&str>) -> bool {
    codec.is_some_and(|codec| !codec.is_empty() && codec != "none")
}

fn mime_type(container: &str, vcodec: Option<&str>, acodec: Option<&str>) -> String {
    let base = match container {
        "webm" => "video/webm",
        "3gp" => "video/3gpp",
        _ => "video/mp4",
    };

    let codecs = [vcodec, acodec]
        .into_iter()
        .filter(|codec| is_present(*codec))
        .map(|codec| codec.unwrap_or_default())
        .collect::<Vec<_>>()
        .join(", ");

    if codecs.is_empty() {
        base.to_owned()
    } else {
        format!("{base}; codecs=\"{codecs}\"")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn video_ids_are_validated() {
        assert!(is_video_id("P3uI5sLosKU"));
        assert!(is_video_id("rt-2cxAiPJ_"));
        assert!(!is_video_id("short"));
        assert!(!is_video_id("P3uI5sLosKU_toolong"));
        assert!(!is_video_id("--help; rm -"));
    }

    #[test]
    fn a_progressive_format_resolves() {
        let stream = ResolvedStream::from_info(YtDlpInfo {
            url: Some("https://example.com/video.mp4".to_owned()),
            format_id: Some("18".to_owned()),
            ext: Some("mp4".to_owned()),
            height: Some(360),
            format_note: None,
            vcodec: Some("avc1.42001E".to_owned()),
            acodec: Some("mp4a.40.2".to_owned()),
            duration: Some(157.0),
            protocol: Some("https".to_owned()),
            is_live: Some(false),
        })
        .expect("a progressive format should resolve");

        assert_eq!(stream.itag, Some(18));
        assert_eq!(stream.quality, "360p");
        assert_eq!(stream.duration_ms, Some(157_000));
        assert_eq!(
            stream.mime_type,
            "video/mp4; codecs=\"avc1.42001E, mp4a.40.2\""
        );
        assert!(!stream.is_hls && !stream.is_dash);
    }

    #[test]
    fn a_video_only_format_is_rejected() {
        // Handing an adaptive stream back would give the caller silent video.
        let stream = ResolvedStream::from_info(YtDlpInfo {
            url: Some("https://example.com/video.mp4".to_owned()),
            format_id: Some("137".to_owned()),
            ext: Some("mp4".to_owned()),
            height: Some(1080),
            format_note: None,
            vcodec: Some("avc1.640028".to_owned()),
            acodec: Some("none".to_owned()),
            duration: Some(157.0),
            protocol: Some("https".to_owned()),
            is_live: Some(false),
        });

        assert!(stream.is_none());
    }
}
