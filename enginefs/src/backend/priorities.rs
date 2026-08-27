use serde::{Deserialize, Serialize};

/// Startup is gated on the actual first readable bytes. Keep speculative work
/// near MPV's 4 MiB network buffer so rare seek/Cues pieces are not starved by
/// a large urgent head window.
pub const MIN_STARTUP_BYTES: u64 = 4 * 1024 * 1024;
pub const MAX_STARTUP_WINDOW_BYTES: u64 = 4 * 1024 * 1024;
pub const MAX_SEEK_HOT_WINDOW_BYTES: u64 = 128 * 1024 * 1024;
pub const MAX_WARM_WINDOW_BYTES: u64 = 256 * 1024 * 1024;
pub const MAX_CONTAINER_METADATA_WINDOW_BYTES: u64 = 16 * 1024 * 1024;
pub const MAX_DOWNLOAD_RANGE_WINDOW_BYTES: u64 = 32 * 1024 * 1024;
pub const SMALL_FILE_BYTES: u64 = 64 * 1024 * 1024;

/// Maximum pieces to prioritize before first byte is delivered.
pub const MAX_STARTUP_PIECES: i32 = 4;
pub const MAX_SMALL_FILE_STARTUP_PIECES: i32 = 32;

/// Minimum pieces to prioritize before first byte is delivered.
pub const MIN_STARTUP_PIECES: i32 = 2;

/// Aggressive seek/read-ahead defaults. These are internal on purpose: tuning is
/// driven by runtime measurements and logs rather than user-facing settings.
pub const MIN_SEEK_HOT_PIECES: i32 = 24;
pub const SEEK_IMMEDIATE_PIECES: i32 = 12;
pub const MAX_HOT_PIECES: i32 = 96;
pub const MAX_WARM_PIECES: i32 = 192;
pub const BLOCKED_REPLAN_INTERVAL_MS: u64 = 8_000;
const MIN_PIECE_DEADLINE_STEP_MS: u64 = 500;
const MAX_PIECE_DEADLINE_STEP_MS: u64 = 30_000;

/// Start treating reads as "container metadata" when they fall in the last 10MB
/// or the last 5% of the file, whichever starts earlier.
pub fn container_metadata_start(file_size: u64) -> u64 {
    if file_size == 0 {
        0
    } else if file_size < SMALL_FILE_BYTES {
        file_size.saturating_mul(95) / 100
    } else {
        file_size
            .saturating_sub(10 * 1024 * 1024)
            .min(file_size.saturating_mul(95) / 100)
    }
}

pub fn is_container_metadata_request(start: u64, requested_len: u64, file_size: u64) -> bool {
    start > 0
        && file_size > 0
        && requested_len > 0
        && requested_len <= MAX_CONTAINER_METADATA_WINDOW_BYTES
        && start >= container_metadata_start(file_size)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PlaybackIntent {
    DirectInitial,
    DirectSeek,
    DirectSequential,
    HlsInitial,
    HlsSeek,
    HlsSequential,
    DownloadFull,
    DownloadRange,
    ContainerMetadata,
    InternalProbe,
    Background,
}

impl PlaybackIntent {
    pub fn is_hls(self) -> bool {
        matches!(self, Self::HlsInitial | Self::HlsSeek | Self::HlsSequential)
    }

    pub fn sequential_after_first_byte(self) -> Self {
        match self {
            Self::DirectInitial | Self::DirectSeek | Self::DirectSequential => {
                Self::DirectSequential
            }
            Self::HlsInitial | Self::HlsSeek | Self::HlsSequential => Self::HlsSequential,
            Self::DownloadFull | Self::DownloadRange => self,
            other => other,
        }
    }

    pub fn seek_for_same_family(self) -> Self {
        if self.is_hls() {
            Self::HlsSeek
        } else if matches!(self, Self::DownloadFull | Self::DownloadRange) {
            Self::DownloadRange
        } else {
            Self::DirectSeek
        }
    }
}

pub fn disk_backed_sequential_download(intent: PlaybackIntent) -> bool {
    matches!(intent, PlaybackIntent::DownloadFull)
}

pub fn disk_backed_file_baseline_priority(intent: PlaybackIntent) -> i32 {
    match intent {
        PlaybackIntent::DownloadFull | PlaybackIntent::DownloadRange => 7,
        // Every streaming intent keeps the WHOLE file minimally wanted
        // (priority 1). A baseline of 0 leaves only the forward window wanted, so
        // once that small window verifies the torrent reports is_finished and
        // drops to seeding/idle -- the download rate craters, read-ahead stops,
        // and (with seeding disabled) the upload is throttled mid-stream. With
        // baseline 1 the file stays "downloading" until it is actually complete;
        // the forward window (priority 7 + deadlines) still concentrates
        // bandwidth on the requested region, so seek/startup stay fast.
        PlaybackIntent::DirectInitial
        | PlaybackIntent::DirectSeek
        | PlaybackIntent::HlsInitial
        | PlaybackIntent::HlsSeek
        | PlaybackIntent::ContainerMetadata
        | PlaybackIntent::DirectSequential
        | PlaybackIntent::HlsSequential
        | PlaybackIntent::InternalProbe
        | PlaybackIntent::Background => 1,
    }
}

pub fn disk_backed_forward_window_pieces(intent: PlaybackIntent) -> i32 {
    match intent {
        PlaybackIntent::DownloadFull => 64,
        PlaybackIntent::DownloadRange => 8,
        PlaybackIntent::DirectInitial | PlaybackIntent::HlsInitial => MAX_STARTUP_PIECES,
        PlaybackIntent::DirectSeek | PlaybackIntent::DirectSequential => 32,
        PlaybackIntent::HlsSeek | PlaybackIntent::HlsSequential => 16,
        // The container seek index (MKV Cues / MP4 moov) spans several pieces;
        // 16 lets the 16 MB MAX_CONTAINER_METADATA_WINDOW_BYTES cap govern (via
        // cap_pieces_by_bytes) so the whole region downloads in parallel instead
        // of one rare piece at a time.
        PlaybackIntent::ContainerMetadata => 16,
        PlaybackIntent::InternalProbe => 1,
        PlaybackIntent::Background => 1,
    }
}

pub fn disk_backed_forward_window_pieces_for(intent: PlaybackIntent, piece_length: u64) -> i32 {
    let pieces = disk_backed_forward_window_pieces(intent);
    let byte_cap = match intent {
        PlaybackIntent::DownloadFull => MAX_WARM_WINDOW_BYTES,
        PlaybackIntent::DownloadRange => MAX_DOWNLOAD_RANGE_WINDOW_BYTES,
        PlaybackIntent::DirectInitial | PlaybackIntent::HlsInitial => MAX_STARTUP_WINDOW_BYTES,
        PlaybackIntent::DirectSeek
        | PlaybackIntent::HlsSeek
        | PlaybackIntent::DirectSequential
        | PlaybackIntent::HlsSequential => MAX_SEEK_HOT_WINDOW_BYTES,
        PlaybackIntent::ContainerMetadata | PlaybackIntent::InternalProbe => {
            MAX_CONTAINER_METADATA_WINDOW_BYTES
        }
        PlaybackIntent::Background => MAX_CONTAINER_METADATA_WINDOW_BYTES,
    };
    cap_pieces_by_bytes(pieces, piece_length, byte_cap)
}

pub fn playback_deadline_step_ms(
    piece_length: u64, bitrate_bytes_per_sec: Option<u64>, download_rate_bytes_per_sec: u64,
) -> i32 {
    let bitrate = bitrate_bytes_per_sec.filter(|rate| *rate > 0);
    let download_rate = (download_rate_bytes_per_sec > 0).then_some(download_rate_bytes_per_sec);
    let effective_rate = match (bitrate, download_rate) {
        (Some(bitrate), Some(download_rate)) => bitrate.min(download_rate),
        (Some(bitrate), None) => bitrate,
        (None, Some(download_rate)) => download_rate,
        (None, None) => 1024 * 1024,
    };
    let step_ms = piece_length
        .saturating_mul(1_000)
        .div_ceil(effective_rate)
        .clamp(MIN_PIECE_DEADLINE_STEP_MS, MAX_PIECE_DEADLINE_STEP_MS);
    step_ms as i32
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MemoryPressure {
    Normal,
    High,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PriorityBand {
    Immediate,
    Hot,
    Warm,
    Metadata,
    Background,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct EngineCacheConfig {
    pub size: u64,
    pub enabled: bool,
}

impl Default for EngineCacheConfig {
    fn default() -> Self {
        Self {
            size: 10 * 1024 * 1024 * 1024, // 10 GB
            enabled: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PriorityContext {
    pub intent: PlaybackIntent,
    pub current_piece: i32,
    pub first_piece: i32,
    pub last_piece: i32,
    pub piece_length: u64,
    pub file_size: u64,
    pub bitrate_bytes_per_sec: Option<u64>,
    pub download_rate_bytes_per_sec: u64,
    pub peers: u64,
    pub cache_size_bytes: u64,
    pub memory_pressure: MemoryPressure,
    pub consecutive_waits: u32,
    pub first_byte_sent: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorityAssignment {
    pub piece_idx: i32,
    pub piece_priority: i32,
    pub deadline: i32,
    pub band: PriorityBand,
}

pub type PriorityItem = PriorityAssignment;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorityDecision {
    pub assignments: Vec<PriorityAssignment>,
    pub target_window_pieces: i32,
    pub immediate_pieces: i32,
    pub hot_window_pieces: i32,
    pub warm_window_pieces: i32,
    pub reason: String,
}

pub struct PlaybackPriorityPolicy;

impl PlaybackPriorityPolicy {
    pub fn decide(ctx: PriorityContext) -> PriorityDecision {
        if ctx.piece_length == 0
            || ctx.current_piece < ctx.first_piece
            || ctx.last_piece < ctx.first_piece
        {
            return PriorityDecision {
                assignments: Vec::new(),
                target_window_pieces: 0,
                immediate_pieces: 0,
                hot_window_pieces: 0,
                warm_window_pieces: 0,
                reason: "invalid-context".to_string(),
            };
        }

        let max_cache_pieces = if ctx.cache_size_bytes > 0 {
            (ctx.cache_size_bytes / ctx.piece_length).max(1) as i32
        } else {
            MAX_HOT_PIECES
        };
        let remaining_pieces = ctx.last_piece.saturating_sub(ctx.current_piece) + 1;

        let bitrate_ratio = ctx
            .bitrate_bytes_per_sec
            .filter(|bitrate| *bitrate > 0)
            .map(|bitrate| ctx.download_rate_bytes_per_sec as f64 / bitrate as f64);

        let mut reason = match ctx.intent {
            PlaybackIntent::DirectInitial | PlaybackIntent::HlsInitial => "initial".to_string(),
            PlaybackIntent::DirectSeek | PlaybackIntent::HlsSeek => "seek".to_string(),
            PlaybackIntent::DirectSequential | PlaybackIntent::HlsSequential => {
                "sequential".to_string()
            }
            PlaybackIntent::DownloadFull => "download-full".to_string(),
            PlaybackIntent::DownloadRange => "download-range".to_string(),
            PlaybackIntent::ContainerMetadata => "container-metadata".to_string(),
            PlaybackIntent::InternalProbe => "internal-probe".to_string(),
            PlaybackIntent::Background => "background".to_string(),
        };

        let (mut immediate, mut hot, mut warm) = match ctx.intent {
            PlaybackIntent::DirectInitial | PlaybackIntent::HlsInitial if !ctx.first_byte_sent => {
                let target_bytes = match ctx.bitrate_bytes_per_sec {
                    Some(bitrate) => bitrate.saturating_mul(10).max(MIN_STARTUP_BYTES),
                    None if ctx.file_size > 0 && ctx.file_size <= SMALL_FILE_BYTES => {
                        ctx.file_size.min(MAX_STARTUP_WINDOW_BYTES)
                    }
                    None => MIN_STARTUP_BYTES,
                }
                .max(ctx.piece_length)
                .min(MAX_STARTUP_WINDOW_BYTES.max(ctx.piece_length));
                let pieces = target_bytes.saturating_add(ctx.piece_length.saturating_sub(1))
                    / ctx.piece_length;
                let max_startup_pieces =
                    if ctx.bitrate_bytes_per_sec.is_none() && ctx.file_size <= SMALL_FILE_BYTES {
                        pieces_for_bytes(
                            ctx.file_size.min(MAX_STARTUP_WINDOW_BYTES),
                            ctx.piece_length,
                        )
                        .clamp(1, MAX_SMALL_FILE_STARTUP_PIECES)
                    } else {
                        MAX_STARTUP_PIECES
                    };
                let min_startup_pieces = MIN_STARTUP_PIECES.min(max_startup_pieces).max(1);
                let pieces = (pieces as i32).clamp(min_startup_pieces, max_startup_pieces);
                // Keep the immediate band small so the swarm focuses bandwidth
                // on the head pieces the player needs for its first bytes; the
                // rest of the startup window rides in the hot band behind it.
                (pieces.min(MAX_STARTUP_PIECES), pieces, 0)
            }
            PlaybackIntent::DirectInitial
            | PlaybackIntent::DirectSequential
            | PlaybackIntent::HlsInitial
            | PlaybackIntent::HlsSequential => {
                let mut hot = dynamic_hot_window(&ctx, bitrate_ratio);
                if ctx.intent.is_hls() {
                    hot = hot.min(48);
                    reason.push_str("-hls-cap");
                }
                (2, hot, 32)
            }
            PlaybackIntent::DownloadFull => (2, 16, 0),
            PlaybackIntent::DownloadRange => (1, 4, 0),
            PlaybackIntent::DirectSeek | PlaybackIntent::HlsSeek if !ctx.first_byte_sent => {
                reason.push_str("-first-piece-only");
                (1, 1, 0)
            }
            PlaybackIntent::DirectSeek | PlaybackIntent::HlsSeek => {
                let mut hot = dynamic_hot_window(&ctx, bitrate_ratio).max(MIN_SEEK_HOT_PIECES);
                let mut immediate = SEEK_IMMEDIATE_PIECES;
                if ctx.consecutive_waits >= 3 {
                    hot = (hot * 2).min(MAX_HOT_PIECES);
                    immediate = (immediate * 2).min(hot);
                    reason.push_str("-blocked-expand");
                }
                if ctx.intent.is_hls() && ctx.consecutive_waits < 3 {
                    hot = hot.min(48);
                    reason.push_str("-hls-cap");
                }
                (immediate, hot, 32)
            }
            PlaybackIntent::ContainerMetadata => (1, 2, 0),
            PlaybackIntent::InternalProbe => (0, 2, 0),
            PlaybackIntent::Background => (0, 4, 0),
        };

        if matches!(ctx.memory_pressure, MemoryPressure::High) {
            hot = hot.min(if ctx.intent.is_hls() {
                16
            } else {
                MIN_SEEK_HOT_PIECES
            });
            warm = 0;
            reason.push_str("-memory-clamp");
        }

        if matches!(
            ctx.intent,
            PlaybackIntent::Background | PlaybackIntent::InternalProbe
        ) {
            warm = 0;
        }

        let original_hot = hot;
        let original_warm = warm;
        hot = cap_pieces_by_bytes(hot, ctx.piece_length, hot_byte_cap(&ctx));
        warm = cap_pieces_by_bytes(warm, ctx.piece_length, warm_byte_cap(ctx.intent));
        if hot < original_hot || warm < original_warm {
            reason.push_str("-byte-cap");
        }

        hot = hot
            .clamp(0, MAX_HOT_PIECES)
            .min(max_cache_pieces)
            .min(remaining_pieces);
        warm = warm
            .clamp(0, MAX_WARM_PIECES)
            .min(max_cache_pieces.saturating_sub(hot))
            .min(remaining_pieces.saturating_sub(hot));
        immediate = immediate.min(hot).max(0);

        let target_window = hot + warm;
        let mut assignments = Vec::with_capacity(target_window as usize);
        for distance in 0..target_window {
            let piece_idx = ctx.current_piece + distance;
            if piece_idx > ctx.last_piece {
                break;
            }

            let (band, piece_priority, deadline) = assignment_for(&ctx, distance, immediate, hot);
            assignments.push(PriorityAssignment {
                piece_idx,
                piece_priority,
                deadline,
                band,
            });
        }

        PriorityDecision {
            assignments,
            target_window_pieces: target_window,
            immediate_pieces: immediate,
            hot_window_pieces: hot,
            warm_window_pieces: warm,
            reason,
        }
    }
}

fn pieces_for_bytes(bytes: u64, piece_length: u64) -> i32 {
    if bytes == 0 || piece_length == 0 {
        return 0;
    }

    let pieces = bytes.saturating_add(piece_length.saturating_sub(1)) / piece_length;
    pieces.clamp(1, i32::MAX as u64) as i32
}

fn cap_pieces_by_bytes(pieces: i32, piece_length: u64, max_bytes: u64) -> i32 {
    if pieces <= 0 {
        return 0;
    }
    if max_bytes == 0 {
        return 0;
    }

    pieces.min(pieces_for_bytes(max_bytes, piece_length).max(1))
}

fn hot_byte_cap(ctx: &PriorityContext) -> u64 {
    match ctx.intent {
        PlaybackIntent::DirectInitial | PlaybackIntent::HlsInitial if !ctx.first_byte_sent => {
            MAX_STARTUP_WINDOW_BYTES
        }
        PlaybackIntent::DirectInitial | PlaybackIntent::HlsInitial => MAX_SEEK_HOT_WINDOW_BYTES,
        PlaybackIntent::DirectSeek
        | PlaybackIntent::HlsSeek
        | PlaybackIntent::DirectSequential
        | PlaybackIntent::HlsSequential => MAX_SEEK_HOT_WINDOW_BYTES,
        PlaybackIntent::DownloadFull => MAX_WARM_WINDOW_BYTES,
        PlaybackIntent::DownloadRange => MAX_DOWNLOAD_RANGE_WINDOW_BYTES,
        PlaybackIntent::ContainerMetadata | PlaybackIntent::InternalProbe => {
            MAX_CONTAINER_METADATA_WINDOW_BYTES
        }
        PlaybackIntent::Background => MAX_CONTAINER_METADATA_WINDOW_BYTES,
    }
}

fn warm_byte_cap(intent: PlaybackIntent) -> u64 {
    match intent {
        PlaybackIntent::DirectInitial
        | PlaybackIntent::DirectSeek
        | PlaybackIntent::DirectSequential
        | PlaybackIntent::HlsInitial
        | PlaybackIntent::HlsSeek
        | PlaybackIntent::HlsSequential
        | PlaybackIntent::DownloadFull => MAX_WARM_WINDOW_BYTES,
        PlaybackIntent::DownloadRange
        | PlaybackIntent::ContainerMetadata
        | PlaybackIntent::InternalProbe
        | PlaybackIntent::Background => 0,
    }
}

fn dynamic_hot_window(ctx: &PriorityContext, bitrate_ratio: Option<f64>) -> i32 {
    let mut hot = if let Some(ratio) = bitrate_ratio {
        if ratio >= 3.0 {
            96
        } else if ratio >= 1.5 {
            48
        } else if ratio >= 1.0 {
            32
        } else {
            MIN_SEEK_HOT_PIECES
        }
    } else if ctx.download_rate_bytes_per_sec > 10 * 1024 * 1024 {
        96
    } else if ctx.download_rate_bytes_per_sec > 5 * 1024 * 1024 {
        48
    } else if ctx.download_rate_bytes_per_sec > 1024 * 1024 {
        MIN_SEEK_HOT_PIECES
    } else {
        16
    };

    if let Some(bitrate) = ctx.bitrate_bytes_per_sec.filter(|bitrate| *bitrate > 0) {
        let pieces_for_10s = ((bitrate.saturating_mul(10)) / ctx.piece_length).max(1) as i32;
        hot = hot.max(pieces_for_10s);
    }

    if ctx.peers < 3 {
        hot = hot.min(MIN_SEEK_HOT_PIECES);
    }

    hot
}

fn assignment_for(
    ctx: &PriorityContext, distance: i32, immediate_pieces: i32, hot_pieces: i32,
) -> (PriorityBand, i32, i32) {
    let deadline_step = playback_deadline_step_ms(
        ctx.piece_length,
        ctx.bitrate_bytes_per_sec,
        ctx.download_rate_bytes_per_sec,
    );
    match ctx.intent {
        PlaybackIntent::ContainerMetadata => (PriorityBand::Metadata, 7, distance * deadline_step),
        PlaybackIntent::InternalProbe => (
            PriorityBand::Background,
            1,
            10_000 + distance * deadline_step,
        ),
        PlaybackIntent::Background => (
            PriorityBand::Background,
            1,
            30_000 + distance * deadline_step,
        ),
        _ if distance < immediate_pieces => (PriorityBand::Immediate, 7, distance * deadline_step),
        _ if distance < hot_pieces => (PriorityBand::Hot, 4, distance * deadline_step),
        _ => (PriorityBand::Warm, 2, 10_000 + distance * deadline_step),
    }
}

/// Backward-compatible wrapper used by older callers and tests.
pub fn calculate_priorities(
    current_piece: i32, total_pieces: i32, piece_length: u64, config: &EngineCacheConfig,
    priority: u8, download_speed: u64, bitrate: Option<u64>,
) -> Vec<PriorityItem> {
    let intent = if priority >= 250 {
        PlaybackIntent::InternalProbe
    } else if priority >= 100 {
        PlaybackIntent::DirectSeek
    } else if priority == 0 {
        PlaybackIntent::Background
    } else {
        PlaybackIntent::DirectSequential
    };

    PlaybackPriorityPolicy::decide(PriorityContext {
        intent,
        current_piece,
        first_piece: 0,
        last_piece: total_pieces.saturating_sub(1),
        piece_length,
        file_size: total_pieces.max(0) as u64 * piece_length,
        bitrate_bytes_per_sec: bitrate,
        download_rate_bytes_per_sec: download_speed,
        peers: 8,
        cache_size_bytes: if config.enabled { config.size } else { 0 },
        memory_pressure: MemoryPressure::Normal,
        consecutive_waits: 0,
        first_byte_sent: true,
    })
    .assignments
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_context(intent: PlaybackIntent) -> PriorityContext {
        PriorityContext {
            intent,
            current_piece: 100,
            first_piece: 0,
            last_piece: 999,
            piece_length: 1024 * 1024,
            file_size: 1000 * 1024 * 1024,
            bitrate_bytes_per_sec: None,
            download_rate_bytes_per_sec: 2 * 1024 * 1024,
            peers: 10,
            cache_size_bytes: 1024 * 1024 * 1024,
            memory_pressure: MemoryPressure::Normal,
            consecutive_waits: 0,
            first_byte_sent: true,
        }
    }

    #[test]
    fn initial_before_first_byte_is_small() {
        let mut ctx = base_context(PlaybackIntent::DirectInitial);
        ctx.current_piece = 0;
        ctx.first_byte_sent = false;
        let decision = PlaybackPriorityPolicy::decide(ctx);

        assert!(decision.target_window_pieces >= MIN_STARTUP_PIECES);
        assert!(decision.target_window_pieces <= MAX_STARTUP_PIECES);
        assert_eq!(decision.assignments[0].deadline, 0);
        assert_eq!(decision.assignments[0].piece_priority, 7);
    }

    #[test]
    fn initial_after_first_byte_expands() {
        let decision = PlaybackPriorityPolicy::decide(base_context(PlaybackIntent::DirectInitial));

        assert!(decision.hot_window_pieces >= MIN_SEEK_HOT_PIECES);
        assert_eq!(decision.assignments[0].band, PriorityBand::Immediate);
    }

    #[test]
    fn direct_seek_has_minimum_hot_window() {
        let decision = PlaybackPriorityPolicy::decide(base_context(PlaybackIntent::DirectSeek));

        assert!(decision.hot_window_pieces >= MIN_SEEK_HOT_PIECES);
        assert_eq!(decision.immediate_pieces, SEEK_IMMEDIATE_PIECES);
        assert_eq!(decision.assignments[0].piece_priority, 7);
        assert_eq!(decision.assignments[3].piece_priority, 7);
        assert_eq!(decision.assignments[4].piece_priority, 7);
        assert_eq!(
            decision.assignments[SEEK_IMMEDIATE_PIECES as usize - 1].piece_priority,
            7
        );
        assert_eq!(
            decision.assignments[SEEK_IMMEDIATE_PIECES as usize].piece_priority,
            4
        );
    }

    #[test]
    fn fast_swarm_expands_seek_window() {
        let mut ctx = base_context(PlaybackIntent::DirectSeek);
        ctx.download_rate_bytes_per_sec = 12 * 1024 * 1024;
        let decision = PlaybackPriorityPolicy::decide(ctx);

        assert!(decision.hot_window_pieces >= 96);
    }

    #[test]
    fn hls_window_is_capped_before_blocking() {
        let mut ctx = base_context(PlaybackIntent::HlsSeek);
        ctx.download_rate_bytes_per_sec = 12 * 1024 * 1024;
        let decision = PlaybackPriorityPolicy::decide(ctx);

        assert_eq!(decision.hot_window_pieces, 48);
    }

    #[test]
    fn hls_blocking_can_expand_to_aggressive_window() {
        let mut ctx = base_context(PlaybackIntent::HlsSeek);
        ctx.download_rate_bytes_per_sec = 12 * 1024 * 1024;
        ctx.consecutive_waits = 3;
        let decision = PlaybackPriorityPolicy::decide(ctx);

        assert!(decision.hot_window_pieces > 48);
    }

    #[test]
    fn memory_pressure_clamps_window() {
        let mut ctx = base_context(PlaybackIntent::DirectSeek);
        ctx.download_rate_bytes_per_sec = 12 * 1024 * 1024;
        ctx.memory_pressure = MemoryPressure::High;
        let decision = PlaybackPriorityPolicy::decide(ctx);

        assert_eq!(decision.hot_window_pieces, MIN_SEEK_HOT_PIECES);
        assert_eq!(decision.warm_window_pieces, 0);
    }

    #[test]
    fn internal_probe_uses_low_priority() {
        let decision = PlaybackPriorityPolicy::decide(base_context(PlaybackIntent::InternalProbe));

        assert!(
            decision
                .assignments
                .iter()
                .all(|item| item.piece_priority <= 1)
        );
    }

    #[test]
    fn compatibility_wrapper_still_returns_priorities() {
        let config = EngineCacheConfig {
            size: 200 * 1024 * 1024,
            enabled: true,
        };
        let priorities = calculate_priorities(0, 1000, 10 * 1024 * 1024, &config, 1, 0, None);

        assert!(!priorities.is_empty());
        assert_eq!(priorities[0].piece_idx, 0);
    }

    #[test]
    fn disk_backed_sequential_is_only_for_full_downloads() {
        assert!(disk_backed_sequential_download(
            PlaybackIntent::DownloadFull
        ));
        assert!(!disk_backed_sequential_download(
            PlaybackIntent::DownloadRange
        ));
        assert!(!disk_backed_sequential_download(
            PlaybackIntent::DirectInitial
        ));
        assert!(!disk_backed_sequential_download(PlaybackIntent::DirectSeek));
        assert!(!disk_backed_sequential_download(
            PlaybackIntent::ContainerMetadata
        ));
    }

    #[test]
    fn disk_backed_container_metadata_window_covers_cues_region() {
        // The Cues/moov region spans several pieces; the window is wide enough
        // for the 16 MB byte cap (cap_pieces_by_bytes) to govern so the whole
        // index downloads in parallel instead of one rare piece at a time.
        assert_eq!(
            disk_backed_forward_window_pieces(PlaybackIntent::ContainerMetadata),
            16
        );
        assert!(
            disk_backed_forward_window_pieces(PlaybackIntent::DownloadFull)
                > disk_backed_forward_window_pieces(PlaybackIntent::DownloadRange)
        );
    }

    #[test]
    fn disk_backed_streaming_keeps_whole_file_wanted() {
        // Streaming intents keep the whole file minimally wanted (priority 1) so
        // the torrent never reports is_finished after just the forward window
        // completes (which stalls read-ahead / idles the download). Downloads
        // stay at 7.
        assert_eq!(
            disk_backed_file_baseline_priority(PlaybackIntent::DirectInitial),
            1
        );
        assert_eq!(
            disk_backed_file_baseline_priority(PlaybackIntent::DirectSeek),
            1
        );
        assert_eq!(
            disk_backed_file_baseline_priority(PlaybackIntent::ContainerMetadata),
            1
        );
        assert_eq!(
            disk_backed_file_baseline_priority(PlaybackIntent::DirectSequential),
            1
        );
        assert_eq!(
            disk_backed_file_baseline_priority(PlaybackIntent::DownloadFull),
            7
        );
    }

    #[test]
    fn blocking_container_metadata_is_urgent() {
        let decision =
            PlaybackPriorityPolicy::decide(base_context(PlaybackIntent::ContainerMetadata));

        assert_eq!(decision.assignments[0].piece_priority, 7);
        assert_eq!(decision.assignments[0].deadline, 0);
    }

    #[test]
    fn small_file_metadata_starts_at_final_five_percent() {
        let file_size = 8 * 1024 * 1024;
        assert_eq!(container_metadata_start(file_size), file_size * 95 / 100);
        assert!(!is_container_metadata_request(1024 * 1024, 1024, file_size));
        assert!(is_container_metadata_request(
            container_metadata_start(file_size),
            1024,
            file_size
        ));
    }

    #[test]
    fn large_near_end_playback_range_is_not_metadata_when_range_is_large() {
        let file_size = 10 * 1024 * 1024 * 1024;
        let start = container_metadata_start(file_size);

        assert!(is_container_metadata_request(
            start,
            MAX_CONTAINER_METADATA_WINDOW_BYTES,
            file_size
        ));
        assert!(!is_container_metadata_request(
            start,
            MAX_CONTAINER_METADATA_WINDOW_BYTES + 1,
            file_size
        ));
    }

    #[test]
    fn download_range_priority_is_bounded() {
        let decision = PlaybackPriorityPolicy::decide(base_context(PlaybackIntent::DownloadRange));

        assert_eq!(decision.hot_window_pieces, 4);
        assert_eq!(decision.warm_window_pieces, 0);
        assert_eq!(decision.assignments[0].piece_priority, 7);
    }

    #[test]
    fn seek_window_is_capped_by_bytes_for_large_pieces() {
        let mut ctx = base_context(PlaybackIntent::DirectSeek);
        ctx.piece_length = 16 * 1024 * 1024;
        ctx.download_rate_bytes_per_sec = 12 * 1024 * 1024;
        let expected_hot = (MAX_SEEK_HOT_WINDOW_BYTES / ctx.piece_length) as i32;
        let decision = PlaybackPriorityPolicy::decide(ctx);

        assert_eq!(decision.hot_window_pieces, expected_hot);
        assert!(decision.reason.contains("byte-cap"));
    }

    #[test]
    fn startup_window_is_capped_by_bytes_for_huge_pieces() {
        let mut ctx = base_context(PlaybackIntent::DirectInitial);
        ctx.current_piece = 0;
        ctx.first_byte_sent = false;
        ctx.piece_length = 64 * 1024 * 1024;
        let decision = PlaybackPriorityPolicy::decide(ctx);

        assert_eq!(decision.hot_window_pieces, 1);
        assert!(decision.target_window_pieces <= 1);
    }

    #[test]
    fn small_file_startup_respects_mpv_buffer_cap() {
        let mut ctx = base_context(PlaybackIntent::DirectInitial);
        ctx.current_piece = 0;
        ctx.first_byte_sent = false;
        ctx.file_size = 8 * 1024 * 1024;
        ctx.last_piece = 7;
        ctx.piece_length = 1024 * 1024;
        let piece_length = ctx.piece_length;
        let decision = PlaybackPriorityPolicy::decide(ctx);

        assert_eq!(decision.hot_window_pieces, 4);
        assert!(decision.hot_window_pieces as u64 * piece_length <= MAX_STARTUP_WINDOW_BYTES);
        assert_eq!(decision.warm_window_pieces, 0);
        // Immediate band stays focused on the head so the first pieces get
        // all the bandwidth instead of the whole file downloading in parallel.
        assert!(decision.immediate_pieces <= MAX_STARTUP_PIECES);
    }

    #[test]
    fn disk_backed_forward_window_respects_piece_size_byte_cap() {
        assert_eq!(
            disk_backed_forward_window_pieces_for(PlaybackIntent::DownloadRange, 64 * 1024 * 1024),
            1
        );
    }

    #[test]
    fn cold_seek_only_prioritizes_the_requested_piece() {
        let mut ctx = base_context(PlaybackIntent::DirectSeek);
        ctx.first_byte_sent = false;
        ctx.piece_length = 16 * 1024 * 1024;
        let decision = PlaybackPriorityPolicy::decide(ctx);

        assert_eq!(decision.assignments.len(), 1);
        assert_eq!(decision.assignments[0].piece_idx, 100);
        assert_eq!(decision.assignments[0].piece_priority, 7);
        assert!(decision.reason.contains("first-piece-only"));
    }

    #[test]
    fn deadlines_track_the_slower_of_playback_and_download_rates() {
        let mut ctx = base_context(PlaybackIntent::DirectSequential);
        ctx.piece_length = 16 * 1024 * 1024;
        ctx.bitrate_bytes_per_sec = Some(8 * 1024 * 1024);
        ctx.download_rate_bytes_per_sec = 32 * 1024 * 1024;
        let deadline_step = playback_deadline_step_ms(
            ctx.piece_length,
            ctx.bitrate_bytes_per_sec,
            ctx.download_rate_bytes_per_sec,
        );
        let decision = PlaybackPriorityPolicy::decide(ctx);

        assert_eq!(deadline_step, 2_000);
        assert_eq!(decision.assignments[0].deadline, 0);
        assert_eq!(decision.assignments[1].deadline, 2_000);
    }
}
