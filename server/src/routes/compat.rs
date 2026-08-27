use axum::{
    body::Body,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use regex::RegexBuilder;
use serde_json::json;

pub const LOCAL_BASE_URL: &str = "http://127.0.0.1:11470";
pub const DLNA_TRANSFER_MODE: &str = "Streaming";
pub const DLNA_CONTENT_FEATURES: &str =
    "DLNA.ORG_OP=01;DLNA.ORG_CI=0;DLNA.ORG_FLAGS=01700000000000000000000000000000";

#[derive(Debug, Clone)]
pub struct FileCandidate {
    pub index: usize,
    pub name: String,
    pub length: u64,
}

pub fn query_values(query: Option<&str>, name: &str) -> Vec<String> {
    query
        .map(|q| {
            q.split('&')
                .filter(|field| field.split_once('=').unwrap_or((field, "")).0 == name)
                .filter_map(|field| {
                    url::form_urlencoded::parse(field.as_bytes())
                        .next()
                        .map(|(_, value)| value.into_owned())
                })
                .collect()
        })
        .unwrap_or_default()
}

pub fn normalize_tracker_sources(sources: Vec<String>) -> Vec<String> {
    sources
        .into_iter()
        .filter_map(|source| {
            let decoded = urlencoding::decode(&source)
                .map(|cow| cow.into_owned())
                .unwrap_or(source);
            let trimmed = decoded.trim();
            if trimmed.is_empty() || trimmed.starts_with("dht:") {
                None
            } else if let Some(tracker) = trimmed.strip_prefix("tracker:") {
                (!tracker.is_empty()).then(|| tracker.to_string())
            } else {
                Some(trimmed.to_string())
            }
        })
        .collect()
}

pub fn basename(path: &str) -> &str {
    path.rsplit(['/', '\\'])
        .find(|part| !part.is_empty() && *part != "." && *part != "..")
        .unwrap_or("download")
}

fn clean_filename_component(name: &str) -> String {
    let cleaned = name
        .chars()
        .filter(|ch| !ch.is_control() && *ch != '"' && *ch != '\r' && *ch != '\n')
        .collect::<String>()
        .trim()
        .trim_matches('.')
        .to_string();

    if cleaned.is_empty() {
        "download".to_string()
    } else {
        cleaned
    }
}

fn ascii_fallback(name: &str) -> String {
    let fallback = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_' | ' ') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim()
        .trim_matches('.')
        .to_string();

    if fallback.is_empty() {
        "download".to_string()
    } else {
        fallback
    }
}

pub fn content_disposition_attachment(path: &str) -> HeaderValue {
    let cleaned = clean_filename_component(basename(path));
    let fallback = ascii_fallback(&cleaned);
    let encoded = urlencoding::encode(&cleaned);
    HeaderValue::from_str(&format!(
        "attachment; filename=\"{}\"; filename*=UTF-8''{}",
        fallback, encoded
    ))
    .unwrap_or_else(|_| HeaderValue::from_static("attachment; filename=\"download\""))
}

pub fn content_disposition_inline(path: &str) -> HeaderValue {
    let cleaned = clean_filename_component(basename(path));
    let fallback = ascii_fallback(&cleaned);
    HeaderValue::from_str(&format!("inline; filename=\"{}\"", fallback))
        .unwrap_or_else(|_| HeaderValue::from_static("inline; filename=\"download\""))
}

pub fn add_dlna_headers(headers: &mut HeaderMap) {
    headers.insert(
        "transferMode.dlna.org",
        HeaderValue::from_static(DLNA_TRANSFER_MODE),
    );
    headers.insert(
        "contentFeatures.dlna.org",
        HeaderValue::from_static(DLNA_CONTENT_FEATURES),
    );
}

pub fn resolve_file_idx(
    requested_idx: &str, files: &[FileCandidate], filters: &[String],
) -> Result<usize, String> {
    if requested_idx != "-1" {
        let idx = requested_idx
            .parse::<usize>()
            .map_err(|_| format!("Invalid file index: {requested_idx}"))?;
        return files
            .iter()
            .any(|file| file.index == idx)
            .then_some(idx)
            .ok_or_else(|| "File index out of bounds".to_string());
    }

    if files.is_empty() {
        return Err("No files available".to_string());
    }

    if !filters.is_empty()
        && let Some(file) = files.iter().find(|file| {
            filters
                .iter()
                .any(|filter| file_matches_filter(&file.name, filter))
        })
    {
        return Ok(file.index);
    }

    files
        .iter()
        .filter(|file| is_video_name(&file.name))
        .max_by_key(|file| file.length)
        .or_else(|| files.iter().max_by_key(|file| file.length))
        .map(|file| file.index)
        .ok_or_else(|| "No playable file found".to_string())
}

pub fn is_video_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    matches!(
        lower.rsplit('.').next(),
        Some("mkv" | "mp4" | "avi" | "webm" | "mov" | "wmv" | "m4v" | "ts")
    )
}

pub fn file_matches_filter(name: &str, filter: &str) -> bool {
    if let Some((pattern, flags)) = parse_regex_filter(filter) {
        return RegexBuilder::new(pattern)
            .case_insensitive(flags.contains('i'))
            .build()
            .map(|regex| regex.is_match(name))
            .unwrap_or(false);
    }

    name.to_ascii_lowercase()
        .contains(&filter.to_ascii_lowercase())
}

fn parse_regex_filter(filter: &str) -> Option<(&str, &str)> {
    if !filter.starts_with('/') {
        return None;
    }
    let last_slash = filter.rfind('/')?;
    (last_slash > 0).then(|| (&filter[1..last_slash], &filter[last_slash + 1..]))
}

pub fn parse_media_url(media_url: &str) -> Result<(String, String), String> {
    let normalized = if media_url.starts_with('/') {
        format!("{}{}", LOCAL_BASE_URL, media_url)
    } else {
        media_url.to_string()
    };

    let parsed = url::Url::parse(normalized.trim_end_matches('?'))
        .map_err(|err| format!("Invalid media URL: {err}"))?;
    let segments = parsed
        .path_segments()
        .map(|segments| segments.collect::<Vec<_>>())
        .unwrap_or_default();

    let info_pos = segments
        .iter()
        .position(|segment| is_info_hash(segment))
        .ok_or_else(|| "No info hash found in media URL".to_string())?;
    let file_idx = segments
        .get(info_pos + 1)
        .ok_or_else(|| "No file index found in media URL".to_string())?;

    Ok((
        segments[info_pos].to_ascii_lowercase(),
        (*file_idx).to_string(),
    ))
}

pub fn is_info_hash(value: &str) -> bool {
    value.len() == 40 && value.chars().all(|ch| ch.is_ascii_hexdigit())
}

pub fn parse_hls_id(id: &str) -> Option<(String, usize)> {
    let decoded = urlencoding::decode(id).ok()?.into_owned();
    let (info_hash, file_idx) = decoded.rsplit_once('-')?;
    if !is_info_hash(info_hash) {
        return None;
    }
    let file_idx = file_idx.parse().ok()?;
    Some((info_hash.to_ascii_lowercase(), file_idx))
}

pub fn unsupported(feature: &str) -> Response {
    tracing::warn!(feature, "Stremio compatibility feature is not implemented");
    (
        StatusCode::NOT_IMPLEMENTED,
        [(header::CONTENT_TYPE, "application/json")],
        Body::from(json!({ "error": format!("{feature} is not implemented") }).to_string()),
    )
        .into_response()
}

pub fn empty_ok() -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .body(Body::empty())
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attachment_header_uses_safe_basename_and_utf8_name() {
        let header = content_disposition_attachment(r#"C:\tmp\..\Movie "Final".mkv"#);
        let value = header.to_str().expect("valid header");

        assert!(value.starts_with("attachment;"));
        assert!(value.contains(r#"filename="Movie Final.mkv""#));
        assert!(value.contains("filename*=UTF-8''Movie%20Final.mkv"));
        assert!(!value.contains(".."));
    }

    #[test]
    fn resolves_minus_one_to_largest_video() {
        let files = vec![
            FileCandidate {
                index: 0,
                name: "sample.txt".to_string(),
                length: 10_000,
            },
            FileCandidate {
                index: 1,
                name: "movie.mkv".to_string(),
                length: 1_000,
            },
            FileCandidate {
                index: 2,
                name: "feature.mp4".to_string(),
                length: 2_000,
            },
        ];

        assert_eq!(resolve_file_idx("-1", &files, &[]).unwrap(), 2);
    }

    #[test]
    fn resolves_minus_one_with_filter() {
        let files = vec![
            FileCandidate {
                index: 0,
                name: "episode.one.mkv".to_string(),
                length: 1,
            },
            FileCandidate {
                index: 1,
                name: "episode.two.mkv".to_string(),
                length: 1,
            },
        ];

        assert_eq!(
            resolve_file_idx("-1", &files, &["/two/i".to_string()]).unwrap(),
            1
        );
    }
}
