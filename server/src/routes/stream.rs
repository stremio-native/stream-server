use crate::routes::compat;
use crate::state::AppState;
use axum::{
    body::Body,
    extract::{Path, RawQuery, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};

use axum::http::HeaderMap;
use enginefs::backend::{HotFilePriorityPlan, TorrentHandle, priorities::PlaybackIntent};
use futures_util::Stream;
use std::path::Path as FsPath;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

static NEXT_STREAM_ID: AtomicU64 = AtomicU64::new(1);

/// Guard that calls on_stream_end when dropped.
struct StreamLifecycleGuard {
    engine: Arc<enginefs::EngineFS>,
    info_hash: String,
    file_idx: usize,
    stream_id: u64,
    notified: bool,
}

impl StreamLifecycleGuard {
    fn new(
        engine: Arc<enginefs::EngineFS>, info_hash: String, file_idx: usize, stream_id: u64,
    ) -> Self {
        crate::diagnostics::logging::direct_stream_started();
        Self {
            engine,
            info_hash,
            file_idx,
            stream_id,
            notified: false,
        }
    }

    fn notify_end(&mut self) {
        if self.notified {
            return;
        }
        self.notified = true;
        crate::diagnostics::logging::direct_stream_ended();

        let engine = self.engine.clone();
        let info_hash = self.info_hash.clone();
        let file_idx = self.file_idx;
        let stream_id = self.stream_id;

        tokio::spawn(async move {
            engine.on_stream_end(&info_hash, file_idx).await;
            tracing::debug!(
                stream_id,
                info_hash = %info_hash,
                file_idx,
                "Stream lifecycle guard notified stream end"
            );
        });
    }
}

impl Drop for StreamLifecycleGuard {
    fn drop(&mut self) {
        self.notify_end();
    }
}

/// Guard that keeps the stream lifecycle alive for the response body.
struct StreamGuard<S> {
    inner: S,
    _lifecycle: StreamLifecycleGuard,
}

impl<S: Stream + Unpin> Stream for StreamGuard<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

/// Counts a request as active while its magnet metadata and automatic file
/// selection are being resolved. A normal `FileHandle` takes over this count
/// once streaming begins.
struct MetadataResolutionGuard {
    active_readers: Arc<AtomicUsize>,
}

impl MetadataResolutionGuard {
    async fn acquire<H: TorrentHandle>(engine: &Arc<enginefs::engine::Engine<H>>) -> Self {
        let active_readers = engine.active_streams.clone();
        active_readers.fetch_add(1, Ordering::SeqCst);
        let guard = Self { active_readers };

        engine.touch();
        if !engine.handle.manages_playback_lifecycle() && engine.idle_paused.load(Ordering::SeqCst)
        {
            if let Err(error) = engine.handle.resume_torrent().await {
                tracing::warn!(
                    info_hash = %engine.info_hash,
                    %error,
                    "Failed to resume torrent while resolving stream metadata"
                );
            } else {
                engine.idle_paused.store(false, Ordering::SeqCst);
                tracing::info!(
                    info_hash = %engine.info_hash,
                    "torrent_resumed_for_metadata_resolution"
                );
            }
        }

        guard
    }
}

impl Drop for MetadataResolutionGuard {
    fn drop(&mut self) {
        let previous = self.active_readers.fetch_sub(1, Ordering::SeqCst);
        debug_assert!(previous > 0, "metadata activity counter underflowed");
    }
}

fn parse_trackers(query_str: Option<&str>) -> Vec<String> {
    compat::normalize_tracker_sources(compat::query_values(query_str, "tr"))
}

#[derive(Default)]
struct PlaybackQuery {
    download: bool,
    hls: bool,
    filters: Vec<String>,
}

impl PlaybackQuery {
    fn parse(query: Option<&str>) -> Self {
        let mut parsed = Self::default();
        let Some(query) = query else {
            return parsed;
        };

        for field in query.split('&') {
            let (key, raw_value) = field.split_once('=').unwrap_or((field, ""));
            match key {
                "download" => parsed.download = query_value_is_true(raw_value),
                "hls" => parsed.hls = query_value_is_true(raw_value),
                "enginefs-intent" if raw_value.eq_ignore_ascii_case("hls") => {
                    parsed.hls = true;
                }
                "f" => {
                    if let Some((_, value)) = url::form_urlencoded::parse(field.as_bytes()).next() {
                        parsed.filters.push(value.into_owned());
                    }
                }
                // Tracker values can be numerous and heavily escaped. They are
                // decoded lazily only if this request must create an engine.
                _ => {}
            }
        }
        parsed
    }
}

fn query_value_is_true(value: &str) -> bool {
    value == "1" || value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("yes")
}

fn playback_intent_for_request(
    is_hls: bool, priority: u8, start: u64, requested_len: u64, file_size: u64, is_download: bool,
    is_partial: bool,
) -> PlaybackIntent {
    if priority == 255 {
        return PlaybackIntent::InternalProbe;
    }
    if priority == 0 {
        return PlaybackIntent::Background;
    }
    if is_download && (!is_partial || requested_len >= file_size.saturating_sub(start)) {
        return PlaybackIntent::DownloadFull;
    }
    if is_download && is_partial {
        return PlaybackIntent::DownloadRange;
    }
    if enginefs::backend::priorities::is_container_metadata_request(start, requested_len, file_size)
    {
        return PlaybackIntent::ContainerMetadata;
    }

    match (is_hls, start == 0) {
        (true, true) => PlaybackIntent::HlsInitial,
        (true, false) => PlaybackIntent::HlsSeek,
        (false, true) => PlaybackIntent::DirectInitial,
        (false, false) => PlaybackIntent::DirectSeek,
    }
}

fn content_type_for_name(name: &str) -> &'static str {
    if name.ends_with(".mp4") {
        "video/mp4"
    } else if name.ends_with(".mkv") {
        "video/x-matroska"
    } else if name.ends_with(".ts") {
        "video/mp2t"
    } else if name.ends_with(".avi") {
        "video/x-msvideo"
    } else if name.ends_with(".mov") {
        "video/quicktime"
    } else if name.ends_with(".wmv") {
        "video/x-ms-wmv"
    } else if name.ends_with(".webm") {
        "video/webm"
    } else if name.ends_with(".mp3") {
        "audio/mpeg"
    } else if name.ends_with(".m4a") {
        "audio/mp4"
    } else if name.ends_with(".aac") {
        "audio/aac"
    } else if name.ends_with(".flac") {
        "audio/flac"
    } else if name.ends_with(".wav") {
        "audio/wav"
    } else if name.ends_with(".ogg") {
        "audio/ogg"
    } else if name.ends_with(".opus") {
        "audio/opus"
    } else if name.ends_with(".ac3") {
        "audio/ac3"
    } else if name.ends_with(".eac3") || name.ends_with(".ec3") {
        "audio/eac3"
    } else {
        "application/octet-stream"
    }
}

// Refreshing the full disk list is a relatively expensive syscall sweep over
// every mounted volume. A player fires a burst of probe requests at stream
// start, each of which would otherwise re-run it on a tokio worker thread.
// Free space does not change meaningfully between those, so cache the result
// per root with a short TTL.
type DiskSpaceCache =
    std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, (Instant, Option<u64>)>>;
static DISK_SPACE_CACHE: std::sync::OnceLock<DiskSpaceCache> = std::sync::OnceLock::new();
const DISK_SPACE_CACHE_TTL: Duration = Duration::from_secs(3);

fn available_space_for_path(path: &FsPath) -> Option<u64> {
    let cache =
        DISK_SPACE_CACHE.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
    if let Ok(map) = cache.lock()
        && let Some((at, value)) = map.get(path)
        && at.elapsed() < DISK_SPACE_CACHE_TTL
    {
        return *value;
    }

    let value = available_space_for_path_uncached(path);
    if let Ok(mut map) = cache.lock() {
        map.insert(path.to_path_buf(), (Instant::now(), value));
    }
    value
}

fn available_space_for_path_uncached(path: &FsPath) -> Option<u64> {
    let disks = sysinfo::Disks::new_with_refreshed_list();
    let mut best_match_len = 0usize;
    let mut best_available = None;

    for disk in disks.list() {
        let mount = disk.mount_point();
        if path.starts_with(mount) {
            let len = mount.as_os_str().len();
            if len >= best_match_len {
                best_match_len = len;
                best_available = Some(disk.available_space());
            }
        }
    }

    best_available
}

fn ensure_download_disk_ready(
    root: &FsPath, file_name: &str, file_size: u64, requested_len: u64, is_partial: bool,
) -> Result<(), String> {
    std::fs::create_dir_all(root).map_err(|e| {
        format!(
            "download cache path is not writable: {} ({})",
            root.display(),
            e
        )
    })?;

    let probe_path = root.join(".write-test");
    std::fs::write(&probe_path, b"ok").map_err(|e| {
        format!(
            "download cache path is not writable: {} ({})",
            root.display(),
            e
        )
    })?;
    let _ = std::fs::remove_file(&probe_path);

    let existing_len = root
        .join(file_name)
        .metadata()
        .map(|metadata| metadata.len().min(file_size))
        .unwrap_or(0);
    let remaining = file_size.saturating_sub(existing_len);
    let safety_margin = 512 * 1024 * 1024u64;
    let required = if is_partial {
        requested_len.min(safety_margin)
    } else {
        remaining.saturating_add(safety_margin)
    };

    let available = available_space_for_path(root).ok_or_else(|| {
        format!(
            "could not determine available disk space for download cache: {}",
            root.display()
        )
    })?;

    if available < required {
        return Err(format!(
            "insufficient download cache space: available={} required={} root={}",
            available,
            required,
            root.display()
        ));
    }

    Ok(())
}

fn disk_space_check_treats_as_partial(is_download: bool, is_partial: bool) -> bool {
    !is_download || is_partial
}

pub async fn head_stream_video(
    State(state): State<AppState>, headers: HeaderMap,
    Path((info_hash, requested_idx)): Path<(String, String)>, RawQuery(query_str): RawQuery,
) -> Response {
    let request_start = Instant::now();
    let info_hash = info_hash.to_lowercase();
    let query = PlaybackQuery::parse(query_str.as_deref());
    let is_download = query.download;
    let prefer_disk_stream = state.download_engine_disk_backed;
    let engine_fs = if prefer_disk_stream {
        state.download_engine.clone()
    } else {
        state.engine.clone()
    };

    let engine = if let Some(e) = engine_fs.get_engine(&info_hash).await {
        e
    } else {
        let trackers = parse_trackers(query_str.as_deref());
        let magnet = format!("magnet:?xt=urn:btih:{}", info_hash);
        let source = enginefs::backend::TorrentSource::Url(magnet);
        match engine_fs.add_torrent(source, Some(trackers.clone())).await {
            Ok(e) => e,
            Err(e) => {
                tracing::error!("head_stream_video: Failed to create engine: {}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to create engine: {}", e),
                )
                    .into_response();
            }
        }
    };

    let _metadata_resolution = MetadataResolutionGuard::acquire(&engine).await;
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
    let idx = match compat::resolve_file_idx(&requested_idx, &candidates, &query.filters) {
        Ok(idx) => idx,
        Err(err) => {
            tracing::warn!(
                info_hash = %info_hash,
                requested_idx = %requested_idx,
                error = %err,
                "head_stream_video could not resolve file index"
            );
            return (StatusCode::NOT_FOUND, err).into_response();
        }
    };
    let Some(file_info) = files.get(idx) else {
        return (StatusCode::NOT_FOUND, "File not found").into_response();
    };
    let size = file_info.length;
    let name = &file_info.name;

    let range_header = headers
        .get(header::RANGE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let (start, end, is_partial) = if let Some(range) = &range_header {
        if let Some((start, end)) = parse_range(range, size) {
            (start, end, true)
        } else {
            return (StatusCode::RANGE_NOT_SATISFIABLE, "Range Not Satisfiable").into_response();
        }
    } else {
        (0, size.saturating_sub(1), false)
    };

    let content_length = end.saturating_sub(start) + 1;
    let mut res_headers = header::HeaderMap::new();
    res_headers.insert(
        header::CONTENT_TYPE,
        content_type_for_name(name).parse().unwrap(),
    );
    res_headers.insert(header::CONTENT_LENGTH, content_length.into());
    res_headers.insert(header::ACCEPT_RANGES, "bytes".parse().unwrap());
    if is_partial {
        res_headers.insert(
            header::CONTENT_RANGE,
            format!("bytes {}-{}/{}", start, end, size).parse().unwrap(),
        );
    }
    if is_download {
        res_headers.insert(
            header::CONTENT_DISPOSITION,
            compat::content_disposition_attachment(name),
        );
    }
    compat::add_dlna_headers(&mut res_headers);

    tracing::debug!(
        "head_stream_video: Responded in {:?} for {} idx={} range {}-{}",
        request_start.elapsed(),
        info_hash,
        idx,
        start,
        end
    );

    if is_partial {
        (StatusCode::PARTIAL_CONTENT, res_headers, Body::empty()).into_response()
    } else {
        (StatusCode::OK, res_headers, Body::empty()).into_response()
    }
}

pub async fn stream_video(
    State(state): State<AppState>, headers: HeaderMap,
    Path((info_hash, requested_idx)): Path<(String, String)>, RawQuery(query_str): RawQuery,
) -> Response {
    let request_start = Instant::now();
    let info_hash = info_hash.to_lowercase();
    let stream_id = NEXT_STREAM_ID.fetch_add(1, Ordering::Relaxed);
    let query = PlaybackQuery::parse(query_str.as_deref());
    let is_download = query.download;
    let prefer_disk_stream = state.download_engine_disk_backed;
    let mut engine_fs = if prefer_disk_stream {
        state.download_engine.clone()
    } else {
        state.engine.clone()
    };
    let mut download_storage_mode = if prefer_disk_stream {
        "diskBacked"
    } else {
        "memoryOnly"
    };

    tracing::debug!(
        stream_id,
        info_hash = %info_hash,
        file_idx = %requested_idx,
        "stream_video request"
    );

    // Try to get existing engine, or auto-create from info hash
    let mut engine = if let Some(e) = engine_fs.get_engine(&info_hash).await {
        tracing::debug!(stream_id, "stream_video engine found in cache");
        e
    } else {
        // Auto-create engine from magnet link
        tracing::debug!(
            stream_id,
            info_hash = %info_hash,
            "stream_video auto-creating engine"
        );
        let trackers = parse_trackers(query_str.as_deref());
        let magnet = format!("magnet:?xt=urn:btih:{}", info_hash);
        // Note: usage of enginefs::backend::TorrentSource requires enginefs dependency or import
        let source = enginefs::backend::TorrentSource::Url(magnet);

        match engine_fs.add_torrent(source, Some(trackers.clone())).await {
            Ok(e) => {
                tracing::debug!(stream_id, "stream_video engine created successfully");
                e
            }
            Err(e) => {
                tracing::error!(stream_id, error = %e, "stream_video failed to create engine");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to create engine: {}", e),
                )
                    .into_response();
            }
        }
    };

    let mut _metadata_resolution = MetadataResolutionGuard::acquire(&engine).await;
    let mut files = engine.handle.get_files().await;
    let candidates = files
        .iter()
        .enumerate()
        .map(|(index, file)| compat::FileCandidate {
            index,
            name: file.name.clone(),
            length: file.length,
        })
        .collect::<Vec<_>>();
    let mut idx = match compat::resolve_file_idx(&requested_idx, &candidates, &query.filters) {
        Ok(idx) => idx,
        Err(err) => {
            tracing::warn!(
                stream_id,
                info_hash = %info_hash,
                requested_idx = %requested_idx,
                error = %err,
                "stream_video could not resolve file index"
            );
            return (StatusCode::NOT_FOUND, err).into_response();
        }
    };
    let Some(file_info) = files.get(idx) else {
        tracing::warn!(
            stream_id,
            info_hash = %info_hash,
            file_idx = idx,
            "stream_video file index not found before stream start"
        );
        return (StatusCode::NOT_FOUND, "File not found").into_response();
    };
    let mut size = file_info.length;
    let mut name = file_info.name.clone();

    let range_header = headers
        .get(header::RANGE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let (mut start, mut end, mut is_partial) = if let Some(range) = &range_header {
        if let Some((start, end)) = parse_range(range, size) {
            (start, end, true)
        } else {
            tracing::warn!(
                stream_id,
                info_hash = %info_hash,
                file_idx = idx,
                range = %range,
                "stream_video invalid range header"
            );
            return (StatusCode::RANGE_NOT_SATISFIABLE, "Range Not Satisfiable").into_response();
        }
    } else {
        (0, size.saturating_sub(1), false)
    };
    let mut requested_content_length = if size == 0 {
        0
    } else {
        end.saturating_sub(start) + 1
    };
    if prefer_disk_stream
        && let Err(err) = ensure_download_disk_ready(
            &engine_fs.download_dir,
            &name,
            size,
            requested_content_length,
            disk_space_check_treats_as_partial(is_download, is_partial),
        )
    {
        tracing::warn!(
            stream_id,
            info_hash = %info_hash,
            file_idx = idx,
            error = %err,
            "disk-backed stream unavailable; switching this request to memory-only mode"
        );
        engine_fs = state.engine.clone();
        download_storage_mode = "memoryOnlyLowDiskFallback";

        engine = if let Some(e) = engine_fs.get_engine(&info_hash).await {
            tracing::debug!(
                stream_id,
                "stream_video fallback memory engine found in cache"
            );
            e
        } else {
            let trackers = parse_trackers(query_str.as_deref());
            let magnet = format!("magnet:?xt=urn:btih:{}", info_hash);
            let source = enginefs::backend::TorrentSource::Url(magnet);
            match engine_fs.add_torrent(source, Some(trackers.clone())).await {
                Ok(e) => {
                    tracing::debug!(
                        stream_id,
                        "stream_video fallback memory engine created successfully"
                    );
                    e
                }
                Err(e) => {
                    tracing::error!(
                        stream_id,
                        error = %e,
                        "stream_video failed to create fallback memory engine"
                    );
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Failed to create fallback memory engine: {}", e),
                    )
                        .into_response();
                }
            }
        };

        _metadata_resolution = MetadataResolutionGuard::acquire(&engine).await;
        files = engine.handle.get_files().await;
        let candidates = files
            .iter()
            .enumerate()
            .map(|(index, file)| compat::FileCandidate {
                index,
                name: file.name.clone(),
                length: file.length,
            })
            .collect::<Vec<_>>();
        idx = match compat::resolve_file_idx(&requested_idx, &candidates, &query.filters) {
            Ok(idx) => idx,
            Err(err) => {
                tracing::warn!(
                    stream_id,
                    info_hash = %info_hash,
                    requested_idx = %requested_idx,
                    error = %err,
                    "stream_video fallback memory engine could not resolve file index"
                );
                return (StatusCode::NOT_FOUND, err).into_response();
            }
        };
        let Some(file_info) = files.get(idx) else {
            tracing::warn!(
                stream_id,
                info_hash = %info_hash,
                file_idx = idx,
                "stream_video fallback memory file index not found before stream start"
            );
            return (StatusCode::NOT_FOUND, "File not found").into_response();
        };
        size = file_info.length;
        name = file_info.name.clone();

        (start, end, is_partial) = if let Some(range) = &range_header {
            if let Some((start, end)) = parse_range(range, size) {
                (start, end, true)
            } else {
                tracing::warn!(
                    stream_id,
                    info_hash = %info_hash,
                    file_idx = idx,
                    range = %range,
                    "stream_video fallback memory invalid range header"
                );
                return (StatusCode::RANGE_NOT_SATISFIABLE, "Range Not Satisfiable")
                    .into_response();
            }
        } else {
            (0, size.saturating_sub(1), false)
        };
        requested_content_length = if size == 0 {
            0
        } else {
            end.saturating_sub(start) + 1
        };
    }
    let start_offset_hint = start;
    // Parse priority from enginefs-prio header
    let priority: u8 = if let Some(prio_val) = headers.get("enginefs-prio") {
        prio_val.to_str().unwrap_or("1").parse().unwrap_or(1)
    } else {
        1
    };
    let playback_intent = playback_intent_for_request(
        query.hls,
        priority,
        start,
        requested_content_length,
        size,
        is_download,
        is_partial,
    );
    let native_lifecycle = engine.handle.manages_playback_lifecycle();
    if !is_download && !is_partial && start == 0 {
        tracing::info!(
            stream_id,
            info_hash = %info_hash,
            file_idx = idx,
            intent = ?playback_intent,
            "playback request without Range; treating as direct initial, not full download"
        );
    }

    // --- Stream Lifecycle: Notify start only after validation has succeeded. ---
    engine_fs.on_stream_start(&info_hash, idx).await;
    if query.hls {
        engine_fs
            .refresh_hls_playback(&info_hash, idx, "hls-loopback")
            .await;
    }
    if !native_lifecycle {
        engine_fs
            .activate_multifile_file_for_playback(
                &info_hash,
                idx,
                Some(HotFilePriorityPlan {
                    file_idx: idx,
                    start_offset: start_offset_hint,
                    priority,
                    intent: playback_intent,
                    bitrate_bytes_per_sec: None,
                }),
                "stream-read",
            )
            .await;
    }
    let lifecycle = StreamLifecycleGuard::new(engine_fs.clone(), info_hash.clone(), idx, stream_id);
    if !native_lifecycle {
        engine_fs.focus_torrent(&info_hash).await;
    }

    // Await the async get_file
    tracing::debug!(
        stream_id,
        info_hash = %info_hash,
        file_idx = idx,
        start_offset = start_offset_hint,
        priority,
        intent = ?playback_intent,
        "stream_video calling get_file"
    );
    if let Some(mut file) = engine
        .get_file_with_intent(idx, start_offset_hint, priority, playback_intent)
        .await
    {
        tracing::debug!(
            stream_id,
            info_hash = %info_hash,
            file_idx = idx,
            size = file.size,
            "stream_video get_file returned success"
        );
        let size = file.size;
        let name = if file.name.is_empty() {
            name
        } else {
            file.name.clone()
        };

        tracing::debug!(
            stream_id,
            info_hash = %info_hash,
            file_idx = idx,
            "stream_video range request: {}-{} (total {})",
            start,
            end,
            size
        );

        if start >= size {
            tracing::warn!(
                stream_id,
                info_hash = %info_hash,
                file_idx = idx,
                start,
                size,
                "stream_video range not satisfiable"
            );
            return (StatusCode::RANGE_NOT_SATISFIABLE, "Range Not Satisfiable").into_response();
        }

        // Seek to the start position
        if start > 0 {
            tracing::debug!(
                stream_id,
                info_hash = %info_hash,
                file_idx = idx,
                start,
                "stream_video seeking"
            );
            if let Err(e) = file.seek(std::io::SeekFrom::Start(start)).await {
                tracing::warn!(
                    stream_id,
                    info_hash = %info_hash,
                    file_idx = idx,
                    error = %e,
                    "stream_video seek error"
                );
                return (StatusCode::INTERNAL_SERVER_ERROR, "Seek failed").into_response();
            }
            tracing::debug!(stream_id, "stream_video seek complete");
        }

        let content_length = requested_content_length;
        let mut res_headers = header::HeaderMap::new();

        let mime = content_type_for_name(&name);

        // Log detected file type
        tracing::debug!(
            stream_id,
            info_hash = %info_hash,
            file_idx = idx,
            content_type = mime,
            file_name = %name,
            "media file detected"
        );

        res_headers.insert(header::CONTENT_TYPE, mime.parse().unwrap());

        res_headers.insert(header::CONTENT_LENGTH, content_length.into());
        res_headers.insert(header::ACCEPT_RANGES, "bytes".parse().unwrap());
        if is_partial {
            res_headers.insert(
                header::CONTENT_RANGE,
                format!("bytes {}-{}/{}", start, end, size).parse().unwrap(),
            );
        }
        if is_download {
            res_headers.insert(
                header::CONTENT_DISPOSITION,
                compat::content_disposition_attachment(&name),
            );
        }
        compat::add_dlna_headers(&mut res_headers);

        // Limit the body to the requested range while the reader waits asynchronously
        // for torrent pieces that have not arrived yet.
        let reader = file.take(content_length);

        // Use ReaderStream to convert AsyncRead to Stream for Axum Body
        // OPTIMIZATION: Use 256KB buffer for improved throughput with large pieces
        // Larger buffer = fewer poll_read calls = less priority calculation overhead
        let base_stream = tokio_util::io::ReaderStream::with_capacity(reader, 262144);

        // Wrap with StreamGuard to notify when stream ends
        let guarded_stream = StreamGuard {
            inner: base_stream,
            _lifecycle: lifecycle,
        };
        let body = Body::from_stream(guarded_stream);

        let response_elapsed_ms = request_start.elapsed().as_millis() as u64;
        if response_elapsed_ms >= 100 {
            tracing::info!(
                stream_id,
                info_hash = %info_hash,
                file_idx = idx,
                elapsed_ms = response_elapsed_ms,
                range_start = start,
                range_end = end,
                partial = is_partial,
                download_storage_mode,
                stage = "http_response_ready",
                "startup: direct stream response ready"
            );
        } else {
            tracing::debug!(
                stream_id,
                info_hash = %info_hash,
                file_idx = idx,
                elapsed_ms = response_elapsed_ms,
                range_start = start,
                range_end = end,
                partial = is_partial,
                download_storage_mode,
                stage = "http_response_ready",
                "startup: direct stream response ready"
            );
        }

        if is_partial {
            (StatusCode::PARTIAL_CONTENT, res_headers, body).into_response()
        } else {
            (StatusCode::OK, res_headers, body).into_response()
        }
    } else {
        tracing::warn!(
            stream_id,
            info_hash = %info_hash,
            file_idx = idx,
            "stream_video get_file returned none after stream start"
        );
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to open stream reader",
        )
            .into_response()
    }
}

fn parse_range(header: &str, size: u64) -> Option<(u64, u64)> {
    let prefix = "bytes=";
    if !header.starts_with(prefix) || size == 0 {
        return None;
    }

    let range_str = &header[prefix.len()..];
    let parts: Vec<&str> = range_str.split('-').collect();
    if parts.len() != 2 {
        return None;
    }

    let start_str = parts[0];
    let end_str = parts[1];

    if start_str.is_empty() {
        let suffix: u64 = end_str.parse().ok()?;
        if suffix == 0 {
            return None;
        }
        let start = size.saturating_sub(suffix);
        return Some((start, size - 1));
    }

    let start: u64 = start_str.parse().ok()?;
    let end = if end_str.is_empty() {
        size - 1
    } else {
        end_str.parse().ok()?
    };

    if start > end || start >= size {
        return None;
    }

    Some((start, end.min(size - 1)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_resolution_guard_balances_the_active_reader_count() {
        let active_readers = Arc::new(AtomicUsize::new(0));
        {
            let _guard = MetadataResolutionGuard {
                active_readers: active_readers.clone(),
            };
            active_readers.fetch_add(1, Ordering::SeqCst);
            assert_eq!(active_readers.load(Ordering::SeqCst), 1);
        }
        assert_eq!(active_readers.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn parses_standard_ranges() {
        assert_eq!(parse_range("bytes=0-0", 10), Some((0, 0)));
        assert_eq!(parse_range("bytes=5-", 10), Some((5, 9)));
        assert_eq!(parse_range("bytes=-4", 10), Some((6, 9)));
    }

    #[test]
    fn rejects_invalid_ranges() {
        assert_eq!(parse_range("items=0-1", 10), None);
        assert_eq!(parse_range("bytes=9-1", 10), None);
        assert_eq!(parse_range("bytes=10-11", 10), None);
        assert_eq!(parse_range("bytes=0-0", 0), None);
    }

    #[test]
    fn accepts_small_partial_download_when_cache_root_is_writable() {
        let temp = tempfile::tempdir().expect("temp dir");
        ensure_download_disk_ready(temp.path(), "movie.mkv", 10 * 1024 * 1024, 1, true)
            .expect("writable temp dir should pass partial request safety check");
    }

    #[test]
    fn normal_playback_uses_partial_disk_space_policy() {
        assert!(disk_space_check_treats_as_partial(false, false));
        assert!(disk_space_check_treats_as_partial(false, true));
        assert!(disk_space_check_treats_as_partial(true, true));
        assert!(!disk_space_check_treats_as_partial(true, false));
    }

    #[test]
    fn full_download_uses_download_full_intent() {
        assert_eq!(
            playback_intent_for_request(false, 1, 0, 10_000, 10_000, true, false),
            PlaybackIntent::DownloadFull
        );
    }

    #[test]
    fn ranged_download_uses_download_range_intent() {
        assert_eq!(
            playback_intent_for_request(false, 1, 500, 1, 10_000, true, true),
            PlaybackIntent::DownloadRange
        );
    }

    #[test]
    fn full_file_range_download_uses_download_full_intent() {
        assert_eq!(
            playback_intent_for_request(false, 1, 0, 10_000, 10_000, true, true),
            PlaybackIntent::DownloadFull
        );
    }

    #[test]
    fn resumed_full_remaining_download_uses_download_full_intent() {
        assert_eq!(
            playback_intent_for_request(false, 1, 5_000, 5_000, 10_000, true, true),
            PlaybackIntent::DownloadFull
        );
    }

    #[test]
    fn playback_without_range_is_direct_initial_not_download() {
        assert_eq!(
            playback_intent_for_request(false, 1, 0, 10_000, 10_000, false, false),
            PlaybackIntent::DirectInitial
        );
    }

    #[test]
    fn tail_playback_range_is_container_metadata() {
        let file_size = 100 * 1024 * 1024;
        let tail = enginefs::backend::priorities::container_metadata_start(file_size);
        assert_eq!(
            playback_intent_for_request(false, 1, tail, 1024, file_size, false, true),
            PlaybackIntent::ContainerMetadata
        );
    }

    #[test]
    fn large_tail_playback_range_stays_direct_seek() {
        let file_size = 100 * 1024 * 1024;
        let tail = enginefs::backend::priorities::container_metadata_start(file_size);
        assert_eq!(
            playback_intent_for_request(
                false,
                1,
                tail,
                enginefs::backend::priorities::MAX_CONTAINER_METADATA_WINDOW_BYTES + 1,
                file_size,
                false,
                true
            ),
            PlaybackIntent::DirectSeek
        );
    }

    #[test]
    fn small_file_non_tail_range_stays_direct_seek() {
        let file_size = 8 * 1024 * 1024;
        assert_eq!(
            playback_intent_for_request(false, 1, 1024 * 1024, 1024, file_size, false, true),
            PlaybackIntent::DirectSeek
        );
    }

    #[test]
    fn playback_query_extracts_controls_without_decoding_trackers() {
        let query = PlaybackQuery::parse(Some(
            "tr=udp%3A%2F%2Fone&tr=https%3A%2F%2Ftwo&f=Episode+02&download=1&hls=true",
        ));

        assert!(query.download);
        assert!(query.hls);
        assert_eq!(query.filters, ["Episode 02"]);
    }
}
