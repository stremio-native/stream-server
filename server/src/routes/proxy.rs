use crate::state::AppState;
use axum::{
    Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::any,
};
use bytes::Bytes;
use reqwest::{Client, Method};
use std::collections::HashMap;
use std::io;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use url::Url;

/// Stream item type handed from the upstream reader task to the response body.
/// `axum::body::Body::from_stream` expects `Stream<Item = Result<Bytes, E>>`,
/// matching the proven passthrough path (`response.bytes_stream()`).
type ProxyChunk = Result<Bytes, io::Error>;

/// Never buffer more than this; protects the 7.7 GiB-RAM laptop from a stuck
/// upstream feeding unbounded memory. The proxy falls back to a plain
/// pass-through if the body is larger than this.
const MAX_BUFFER_BYTES: usize = 64 * 1024 * 1024;

pub fn router() -> Router<AppState> {
    Router::new()
        // The original JS uses /proxy/:opts/:pathname*
        // We can use a wildcard capturing the whole path.
        .route("/{*rest}", any(proxy_handler))
}

pub async fn proxy_handler(
    State(state): State<AppState>,
    Path(rest): Path<String>, axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    Query(params): Query<HashMap<String, String>>, headers: HeaderMap, method: Method,
) -> impl IntoResponse {
    // Read-ahead window for webstreams, from settings (0 = passthrough).
    let read_ahead_mib = {
        let s = state.settings.read().await;
        s.proxy_read_ahead_mib
    };
    let read_ahead_bytes = (read_ahead_mib as usize) * 1024 * 1024;
    // Porting the logic from express_805.js
    // Format 1: ?d=URL (standard)
    // Format 2: /<query_params>/<path> (Core) where query_params contains d=ORIGIN&h=HEADER&r=RESPONSE_HEADER

    let mut target_url = String::new();
    let mut custom_headers = HashMap::new();
    let mut custom_response_headers = HashMap::new();
    let mut is_path_format = false;

    // Check for standard query param '?d='
    if let Some(d) = params.get("d") {
        target_url = d.clone();
        // Fallback: If rest is not empty and d is just origin, we might need to append rest?
        // But usually ?d=FULL_URL
    } else {
        is_path_format = true;
        // Handle path-based format: /proxy/d=...&h=.../path/to/file
        // Split rest by first slash to get query_segment and path
        let (query_seg, path_seg) = match rest.split_once('/') {
            Some((q, p)) => (q, p),
            None => (rest.as_str(), ""),
        };

        // Parse the query segment
        for (key, val) in url::form_urlencoded::parse(query_seg.as_bytes()) {
            match key.as_ref() {
                "d" => target_url = val.into_owned(),
                "h" => {
                    // Header format "Name:Value"
                    if let Some((name, value)) = val.split_once(':') {
                        custom_headers.insert(name.trim().to_string(), value.trim().to_string());
                    }
                }
                "r" => {
                    // Response header format "Name:Value"
                    if let Some((name, value)) = val.split_once(':') {
                        custom_response_headers
                            .insert(name.trim().to_string(), value.trim().to_string());
                    }
                }
                _ => {}
            }
        }

        // If we found 'd', construct the full URL
        if !target_url.is_empty() {
            // target_url is the origin (e.g. http://example.com)
            // path_seg is the relative path (e.g. video.mp4)
            // Join them carefully
            if !path_seg.is_empty() {
                if !target_url.ends_with('/') {
                    target_url.push('/');
                }
                target_url.push_str(path_seg);
            }
        } else {
            // Fallback: assume whole rest is the URL (legacy/simple proxy)
            target_url = rest.clone();
        }
    }

    let mut url = match Url::parse(&target_url) {
        Ok(u) => u,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid target URL").into_response(),
    };

    if is_path_format && let Some(q) = raw_query {
        url.set_query(Some(&q));
    }

    let client = Client::builder()
        .danger_accept_invalid_certs(true) // Parity with rejectUnauthorized: false
        .pool_max_idle_per_host(8)
        .build()
        .unwrap();

    let mut req_builder = client.request(method, url.clone());

    // Forward standard headers
    let allowed_req_headers = [
        "accept",
        "accept-encoding",
        "accept-language",
        "connection",
        "transfer-encoding",
        "range",
        "if-range",
        "user-agent",
    ];

    for name in allowed_req_headers {
        if let Some(value) = headers.get(name) {
            req_builder = req_builder.header(name, value);
        }
    }

    // Apply custom headers from query params (Core format)
    for (name, value) in custom_headers {
        req_builder = req_builder.header(name, value);
    }

    let response = match req_builder.send().await {
        Ok(resp) => resp,
        Err(e) => return (StatusCode::BAD_GATEWAY, format!("Proxy error: {}", e)).into_response(),
    };

    let status = response.status();
    let mut res_builder = Response::builder().status(status);

    let allowed_res_headers = [
        "accept-ranges",
        "content-type",
        "content-length",
        "content-range",
        "connection",
        "transfer-encoding",
        "last-modified",
        "etag",
        "server",
        "date",
    ];

    let res_headers = response.headers().clone();
    for name in allowed_res_headers {
        if let Some(value) = res_headers.get(name) {
            res_builder = res_builder.header(name, value);
        }
    }

    // Apply custom response headers (Core format)
    for (name, value) in custom_response_headers {
        res_builder = res_builder.header(name, value);
    }

    // CORS headers
    res_builder = res_builder
        .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .header(header::ACCESS_CONTROL_ALLOW_METHODS, "GET, POST, OPTIONS")
        .header(header::ACCESS_CONTROL_ALLOW_HEADERS, "*");

    let content_type = res_headers
        .get(header::CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    let is_playlist = url.path().ends_with(".m3u8")
        || url.path().ends_with(".m3u")
        || content_type.contains("mpegurl");

    if is_playlist {
        // We need to rewrite the playlist.
        let body = response.text().await.unwrap_or_default();
        let rewritten = rewrite_playlist(&body, &url);
        return res_builder
            .body(axum::body::Body::from(rewritten))
            .unwrap()
            .into_response();
    }

    // For non-playlist media (debrid webstreams, subtitles, etc.) we buffer a
    // window ahead of the player so a slow upstream can't stall it.
    // read_ahead_bytes == 0 means the user disabled buffering -> plain passthrough.
    let content_length: Option<u64> = res_headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok());
    if read_ahead_bytes == 0 || content_length.is_some_and(|len| len > MAX_BUFFER_BYTES as u64) {
        let stream = response.bytes_stream();
        return res_builder
            .body(axum::body::Body::from_stream(stream))
            .unwrap()
            .into_response();
    }

    // Spawn a task that drains the upstream into a channel while the response
    // body (built from that channel's receiver) is streamed to the player.
    // The channel capacity is the read-ahead window expressed in 64 KiB
    // chunks: once ~read_ahead_bytes of upstream data are buffered ahead of the
    // player, `tx.send()` blocks and back-pressures the upstream, keeping memory
    // bounded while still staying well ahead of playback. As the player drains
    // the channel, the producer fetches more.
    let chunk_capacity = (read_ahead_bytes / (64 * 1024)).max(1);
    let (tx, rx): (mpsc::Sender<ProxyChunk>, mpsc::Receiver<ProxyChunk>) =
        mpsc::channel(chunk_capacity);
    tokio::spawn(async move {
        let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
        let mut stream = response.bytes_stream();
        use futures_util::StreamExt;
        while let Some(chunk_res) = stream.next().await {
            let chunk: Bytes = match chunk_res {
                Ok(c) => c,
                Err(_) => break,
            };
            buf.extend_from_slice(&chunk);
            // Flush in fixed 64 KiB frames so each channel message is uniform
            // and the capacity math above maps cleanly to the byte window.
            while buf.len() >= 64 * 1024 {
                let tail = buf.split_off(64 * 1024);
                if tx.send(Ok(Bytes::from(buf))).await.is_err() {
                    return; // client disconnected
                }
                buf = tail;
            }
        }
        if !buf.is_empty() {
            let _ = tx.send(Ok(Bytes::from(buf))).await;
        }
    });

    let body_stream = ReceiverStream::new(rx);
    res_builder
        .body(axum::body::Body::from_stream(body_stream))
        .unwrap()
        .into_response()
}

fn rewrite_playlist(body: &str, base_url: &Url) -> String {
    let mut rewritten = String::new();
    for line in body.lines() {
        if line.is_empty() {
            rewritten.push('\n');
            continue;
        }
        if line.starts_with('#') {
            // Handle URI="url" in tags like #EXT-X-MEDIA
            if let Some(start) = line.find("URI=\"") {
                let rest = &line[start + 5..];
                if let Some(end) = rest.find('"') {
                    let uri = &rest[..end];
                    let absolute_uri = if uri.contains("://") {
                        uri.to_string()
                    } else {
                        base_url
                            .join(uri)
                            .map(|u: Url| u.to_string())
                            .unwrap_or_else(|_| uri.to_string())
                    };
                    let proxy_uri = format!("/proxy/?d={}", urlencoding::encode(&absolute_uri));
                    rewritten.push_str(&line[..start + 5]);
                    rewritten.push_str(&proxy_uri);
                    rewritten.push_str(&rest[end..]);
                    rewritten.push('\n');
                    continue;
                }
            }
            rewritten.push_str(line);
            rewritten.push('\n');
        } else {
            // It's a URL
            let absolute_uri = if line.contains("://") {
                line.to_string()
            } else {
                base_url
                    .join(line)
                    .map(|u: Url| u.to_string())
                    .unwrap_or_else(|_| line.to_string())
            };
            let proxy_uri = format!("/proxy/?d={}", urlencoding::encode(&absolute_uri));
            rewritten.push_str(&proxy_uri);
            rewritten.push('\n');
        }
    }
    rewritten
}
