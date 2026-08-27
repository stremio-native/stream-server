use crate::state::AppState;
use axum::{
    Json, Router,
    extract::{Path, Query},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::Deserialize;
use serde_json::json;
use std::process::Stdio;
use tokio::process::Command;
use tokio_util::io::ReaderStream;

#[derive(Debug, Deserialize)]
pub struct TranscodeParams {
    pub video: String,
    pub time: Option<f64>,
    #[serde(rename = "audioTrack")]
    pub _audio_track: Option<usize>,
    pub fmp4: Option<String>,
    pub _subtitles: Option<String>,
    #[serde(rename = "subtitlesDelay")]
    pub _subtitles_delay: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct PlayerParams {
    pub source: Option<String>,
    pub paused: Option<String>,
    pub time: Option<f64>,
    pub volume: Option<f32>,
    pub stop: Option<String>,
    #[serde(rename = "audioTrack")]
    pub audio_track: Option<usize>,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(list_devices))
        .route("/transcode", get(transcode))
        .route("/convert", get(transcode))
        .route("/{devID}", get(get_device))
        .route("/{devID}/player", get(player_control).post(player_control))
}

pub async fn list_devices(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> impl IntoResponse {
    let devices = state.devices.read().await;
    Json(devices.clone())
}

pub async fn get_device(Path(dev_id): Path<String>) -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        format!("Device {} not found", dev_id),
    )
        .into_response()
}

pub async fn transcode(Query(params): Query<TranscodeParams>) -> Response {
    let video_url = params.video;
    let offset = params.time.unwrap_or(0.0);
    let is_fmp4 = params.fmp4.is_some();

    let mut args = vec![
        "-copyts".to_string(),
        "-ss".to_string(),
        offset.to_string(),
        "-i".to_string(),
        video_url,
    ];

    args.extend(vec![
        "-c:v".to_string(),
        "libx264".to_string(),
        "-preset".to_string(),
        "ultrafast".to_string(),
        "-tune".to_string(),
        "zerolatency".to_string(),
        "-pix_fmt".to_string(),
        "yuv420p".to_string(),
        "-c:a".to_string(),
        "aac".to_string(),
        "-ac".to_string(),
        "2".to_string(),
        "-threads".to_string(),
        "0".to_string(),
    ]);

    if is_fmp4 {
        args.extend(vec![
            "-movflags".to_string(),
            "frag_keyframe+empty_moov".to_string(),
            "-f".to_string(),
            "mp4".to_string(),
        ]);
    } else {
        args.extend(vec!["-f".to_string(), "matroska".to_string()]);
    }

    args.push("pipe:1".to_string());

    let mut cmd = Command::new("ffmpeg");
    cmd.args(&args).stdout(Stdio::piped()).stderr(Stdio::null());

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to spawn ffmpeg: {}", e),
            )
                .into_response();
        }
    };

    let stdout = child.stdout.take().expect("Failed to open stdout");
    let stream = ReaderStream::new(stdout);

    let content_type = if is_fmp4 {
        "video/mp4"
    } else {
        "video/x-matroska"
    };

    Response::builder()
        .header(header::CONTENT_TYPE, content_type)
        .header(header::TRANSFER_ENCODING, "chunked")
        .header("transferMode.dlna.org", "Streaming")
        .header(
            "contentFeatures.dlna.org",
            "DLNA.ORG_OP=01;DLNA.ORG_CI=1;DLNA.ORG_FLAGS=01300000000000000000000000000000",
        )
        .body(axum::body::Body::from_stream(stream))
        .unwrap()
}

pub async fn player_control(
    method: axum::http::Method, Path(dev_id): Path<String>,
    Query(query_params): Query<PlayerParams>, body: Option<Json<PlayerParams>>,
) -> impl IntoResponse {
    let params = if method == axum::http::Method::POST {
        body.map(|Json(b)| b).unwrap_or(query_params)
    } else {
        query_params
    };

    let response_json = json!({
        "deviceId": dev_id,
        "status": "not_implemented",
        "params": {
            "source": params.source,
            "paused": params.paused,
            "time": params.time,
            "volume": params.volume,
            "stop": params.stop,
            "audio_track": params.audio_track
        }
    });

    Json(response_json)
}
