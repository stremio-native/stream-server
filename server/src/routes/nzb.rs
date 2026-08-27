use crate::archives::nzb::session::{NzbConfig, NzbServerConfig, NzbSession};
use crate::state::AppState;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Redirect, Response},
    routing::get,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
// use std::path::PathBuf;

#[derive(Deserialize, Debug)]
struct NzbServer {
    host: String,
    port: u16,
    user: Option<String>,
    pass: Option<String>,
    ssl: bool,
    connections: u32,
}

#[derive(Deserialize, Debug)]
struct CreateNzbBody {
    servers: Vec<NzbServer>,
    #[serde(rename = "nzbUrl")]
    nzb_url: String,
}

#[derive(Serialize)]
struct CreateResponse {
    key: String,
}

#[derive(Deserialize)]
pub struct NzbCreateQuery {
    pub lz: Option<String>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct NzbLzPayload {
    url: Option<String>,
    #[serde(default)]
    urls: Vec<String>,
    servers: Vec<String>,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/create",
            get(create_session_auto).post(create_session_auto),
        )
        .route(
            "/create/{key}",
            get(create_session_with_key).post(create_session_with_key),
        )
        .route("/stream", get(stream_nzb_query))
        .route("/stream/{key}/{*file}", get(stream_nzb_file))
}

async fn create_session_auto(
    State(state): State<AppState>, method: axum::http::Method, Query(query): Query<NzbCreateQuery>,
    body: axum::body::Bytes,
) -> Response {
    let key = Uuid::new_v4().to_string();
    create_session_internal(state, key, method, query, body).await
}

async fn create_session_with_key(
    State(state): State<AppState>, Path(key): Path<String>, method: axum::http::Method,
    Query(query): Query<NzbCreateQuery>, body: axum::body::Bytes,
) -> Response {
    create_session_internal(state, key, method, query, body).await
}

fn parse_nzb_server_url(url_str: &str) -> Option<NzbServerConfig> {
    let parsed = url::Url::parse(url_str).ok()?;
    let scheme = parsed.scheme();
    if scheme != "news" && scheme != "newss" {
        return None;
    }
    let ssl = scheme == "newss";
    let host = parsed.host_str()?.to_string();
    let port = parsed.port().unwrap_or(if ssl { 563 } else { 119 });

    let user = if parsed.username().is_empty() {
        None
    } else {
        Some(urlencoding::decode(parsed.username()).ok()?.into_owned())
    };

    let pass = parsed
        .password()
        .and_then(|p| urlencoding::decode(p).ok().map(|d| d.into_owned()));

    let path = parsed.path().trim_start_matches('/');
    let connections = path.parse::<u32>().unwrap_or(20);

    Some(NzbServerConfig {
        host,
        port,
        user,
        pass,
        ssl,
        connections,
    })
}

async fn create_session_internal(
    state: AppState, key: String, method: axum::http::Method, query: NzbCreateQuery,
    body: axum::body::Bytes,
) -> Response {
    let config = if let Some(lz) = query.lz {
        let utf16 = match lz_str::decompress_from_encoded_uri_component(&lz) {
            Some(u) => u,
            None => {
                return (StatusCode::BAD_REQUEST, "Failed to decompress lz payload")
                    .into_response();
            }
        };
        let json_str = match String::from_utf16(&utf16) {
            Ok(s) => s,
            Err(_) => {
                return (StatusCode::BAD_REQUEST, "Invalid UTF-16 in lz payload").into_response();
            }
        };
        let payload: NzbLzPayload = match serde_json::from_str(&json_str) {
            Ok(p) => p,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("Invalid LZ JSON payload: {}", e),
                )
                    .into_response();
            }
        };

        let nzb_url = match payload.url.or_else(|| payload.urls.first().cloned()) {
            Some(url) => url,
            None => return (StatusCode::BAD_REQUEST, "No NZB URL provided").into_response(),
        };

        let mut servers = Vec::new();
        for s_url in payload.servers {
            if let Some(cfg) = parse_nzb_server_url(&s_url) {
                servers.push(cfg);
            } else {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("Invalid server URL: {}", s_url),
                )
                    .into_response();
            }
        }

        NzbConfig { nzb_url, servers }
    } else {
        if body.is_empty() {
            return (StatusCode::BAD_REQUEST, "Missing NZB create payload").into_response();
        }
        let payload: CreateNzbBody = match serde_json::from_slice(&body) {
            Ok(p) => p,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("Invalid JSON payload: {}", e),
                )
                    .into_response();
            }
        };

        NzbConfig {
            nzb_url: payload.nzb_url.clone(),
            servers: payload
                .servers
                .into_iter()
                .map(|s| NzbServerConfig {
                    host: s.host,
                    port: s.port,
                    user: s.user,
                    pass: s.pass,
                    ssl: s.ssl,
                    connections: s.connections,
                })
                .collect(),
        }
    };

    // 2. Fetch NZB content
    let nzb_content = match reqwest::get(&config.nzb_url).await {
        Ok(resp) => match resp.text().await {
            Ok(text) => text,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to read NZB content: {}", e),
                )
                    .into_response();
            }
        },
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("Failed to fetch NZB URL: {}", e),
            )
                .into_response();
        }
    };

    // 3. Create Session
    let session = match NzbSession::new(key.clone(), config, nzb_content).await {
        Ok(sess) => sess,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to parse NZB: {}", e),
            )
                .into_response();
        }
    };

    let first_file_subject = session.nzb.files.first().map(|f| f.subject.clone());

    state.nzb_sessions.insert(key.clone(), session);
    tracing::info!("Created NZB session {}", key);

    // 4. Redirect if GET, otherwise return JSON response
    if method == axum::http::Method::GET
        && let Some(subject) = first_file_subject
    {
        return Redirect::temporary(&format!(
            "./stream/{}/{}",
            urlencoding::encode(&key),
            urlencoding::encode(&subject)
        ))
        .into_response();
    }

    Json(CreateResponse { key }).into_response()
}

async fn stream_nzb_file(
    State(state): State<AppState>, Path((key, file)): Path<(String, String)>,
) -> Result<Response, StatusCode> {
    if let Some(session) = state.nzb_sessions.get(&key) {
        match session.stream_file(&file) {
            Ok(stream) => {
                let reader_stream = tokio_util::io::ReaderStream::new(stream);
                return Ok(Response::builder()
                    .header(
                        "transferMode.dlna.org",
                        crate::routes::compat::DLNA_TRANSFER_MODE,
                    )
                    .header(
                        "contentFeatures.dlna.org",
                        crate::routes::compat::DLNA_CONTENT_FEATURES,
                    )
                    .body(axum::body::Body::from_stream(reader_stream))
                    .unwrap());
            }
            Err(e) => {
                tracing::error!(
                    "Failed to find/open file {} in session {}: {}",
                    file,
                    key,
                    e
                );
                return Err(StatusCode::NOT_FOUND);
            }
        }
    }

    Err(StatusCode::NOT_FOUND)
}

#[derive(Deserialize)]
struct NzbStreamQuery {
    key: String,
}

async fn stream_nzb_query(
    State(state): State<AppState>, Query(query): Query<NzbStreamQuery>,
) -> Result<Response, StatusCode> {
    let session = state
        .nzb_sessions
        .get(&query.key)
        .ok_or(StatusCode::NOT_FOUND)?;
    let Some(first_file) = session.nzb.files.first() else {
        return Err(StatusCode::NOT_FOUND);
    };
    Ok(Redirect::temporary(&format!(
        "./stream/{}/{}",
        urlencoding::encode(&query.key),
        urlencoding::encode(&first_file.subject)
    ))
    .into_response())
}
