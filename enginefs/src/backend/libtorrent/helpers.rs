//! Helper functions for libtorrent backend

use crate::backend::{EngineStats, Growler, PeerSearch, Source, StatsOptions, SwarmCap};
use libtorrent_sys::TorrentStatus;
use std::path::PathBuf;

/// Cached piece data with position metadata for correct offset calculation
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(super) struct CachedPieceData {
    /// The actual piece data (may be partial for pieces spanning files)
    pub data: Vec<u8>,
    /// File-relative byte offset where this data starts
    /// Used to calculate correct offsets when reading from cache
    pub file_relative_start: u64,
}

/// Read a piece from disk for prefetch caching
/// Correctly handles multi-file torrents by accounting for file_offset
/// Returns CachedPieceData containing both the data and its file-relative start position
#[allow(dead_code)]
pub(super) async fn read_piece_from_disk(
    file_path: &PathBuf, piece_idx: i32, piece_length: u64, file_offset: u64,
) -> std::io::Result<CachedPieceData> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    let mut file = tokio::fs::File::open(file_path).await?;

    // Get file size to calculate valid read range
    let file_metadata = file.metadata().await?;
    let file_size = file_metadata.len();

    // For multi-file torrents:
    // - piece_idx * piece_length gives global torrent byte offset
    // - file_offset is where this file starts in the torrent
    // - We need the offset WITHIN this file
    let piece_global_start = piece_idx as u64 * piece_length;
    let piece_global_end = piece_global_start + piece_length;

    // Calculate file's range in global torrent space
    let file_global_start = file_offset;
    let file_global_end = file_offset + file_size;

    // Check if piece overlaps with this file
    if piece_global_end <= file_global_start || piece_global_start >= file_global_end {
        // Piece doesn't overlap with this file
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Piece does not overlap with this file",
        ));
    }

    // Calculate the overlap range
    let overlap_start = piece_global_start.max(file_global_start);
    let overlap_end = piece_global_end.min(file_global_end);
    let overlap_size = overlap_end - overlap_start;

    // CRITICAL FIX: Only cache FULL pieces.
    // Partial pieces (at file boundaries) cause cache inconsistency because
    // the reader logic assumes cache data represents the starting from piece_global_start.
    // If we cache a partial chunk starting at file_global_start, the offset calculation is wrong.
    // Exception: Last piece of torrent may be smaller naturally.
    if overlap_size < piece_length {
        // Check if this is truly a partial overlap (edge piece)
        // If it's just the last piece of the torrent, it might be fine, but for simplicity
        // and safety, we skip caching any non-full-length chunk.
        // This means we won't prefetch edge pieces, which is acceptable collateral damage.
        return Err(std::io::Error::other(
            "Skipping partial piece caching to preserve consistency",
        ));
    }

    // Convert to file-relative offset
    let file_relative_offset = overlap_start - file_global_start;

    // Seek to the correct position in the file
    file.seek(std::io::SeekFrom::Start(file_relative_offset))
        .await?;

    // Read the overlapping portion
    let mut data = vec![0u8; overlap_size as usize];
    let bytes_read = file.read(&mut data).await?;
    data.truncate(bytes_read);

    Ok(CachedPieceData {
        data,
        file_relative_start: file_relative_offset,
    })
}

pub(super) fn default_stats(info_hash: &str) -> EngineStats {
    EngineStats {
        name: "Unknown".to_string(),
        info_hash: info_hash.to_string(),
        files: vec![],
        sources: vec![],
        opts: StatsOptions {
            connections: None,
            dht: true,
            growler: Growler {
                flood: 0,
                pulse: None,
            },
            handshake_timeout: None,
            path: String::new(),
            peer_search: PeerSearch {
                max: 0,
                min: 0,
                sources: vec![],
            },
            swarm_cap: SwarmCap {
                max_speed: None,
                min_peers: None,
            },
            timeout: None,
            tracker: true,
            r#virtual: false,
        },
        download_speed: 0.0,
        upload_speed: 0.0,
        downloaded: 0,
        uploaded: 0,
        unchoked: 0,
        peers: 0,
        queued: 0,
        unique: 0,
        connection_tries: 0,
        peer_search_running: false,
        stream_len: 0,
        stream_name: String::new(),
        stream_progress: 0.0,
        swarm_connections: 0,
        swarm_paused: false,
        swarm_size: 0,
        is_finished: false,
        has_metadata: false,
    }
}

pub(super) fn make_engine_stats(status: &TorrentStatus) -> EngineStats {
    EngineStats {
        name: status.name.to_string(),
        info_hash: status.info_hash.to_string(),
        files: vec![], // Would need to query files separately
        sources: vec![Source {
            last_started: String::new(),
            num_found: status.num_peers as u64,
            num_found_uniq: status.num_peers as u64,
            num_requests: 0,
            url: status.current_tracker.to_string(),
        }],
        opts: StatsOptions {
            connections: Some(status.num_peers as u64),
            dht: true,
            growler: Growler {
                flood: 0,
                pulse: None,
            },
            handshake_timeout: None,
            path: status.save_path.to_string(),
            peer_search: PeerSearch {
                max: 200,
                min: 0,
                sources: vec![],
            },
            swarm_cap: SwarmCap {
                max_speed: None,
                min_peers: None,
            },
            timeout: None,
            tracker: true,
            r#virtual: false,
        },
        download_speed: status.download_rate as f64,
        upload_speed: status.upload_rate as f64,
        downloaded: status.total_downloaded as u64,
        uploaded: status.total_uploaded as u64,
        unchoked: 0,
        peers: status.num_peers as u64,
        queued: 0,
        unique: status.num_peers as u64,
        connection_tries: 0,
        peer_search_running: !status.is_finished,
        stream_len: status.total_size as u64,
        stream_name: status.name.to_string(),
        stream_progress: if status.total_wanted > 0 {
            (status.total_wanted_done as f64) / (status.total_wanted as f64)
        } else if status.total_size > 0 {
            (status.total_done as f64) / (status.total_size as f64)
        } else {
            0.0
        },
        swarm_connections: status.num_peers as u64,
        swarm_paused: status.is_paused,
        swarm_size: (status.num_complete + status.num_incomplete) as u64,
        is_finished: status.is_finished,
        has_metadata: status.has_metadata,
    }
}
