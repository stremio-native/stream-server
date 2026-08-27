use anyhow::{Context, Result};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::process::Stdio;
use std::task::Poll;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::process::Command;

use crate::backend::SubtitleTrack;
use crate::hwaccel::HwAccelConfig;

// HLS config optimized for browser playback
// Native clients use direct streaming, so HLS is browser-only
#[derive(Debug, Clone)]
pub struct TranscodeConfig {
    pub segment_duration: f64,          // 4.0 seconds (HLS V2 optimization)
    pub video_bitrate: String,          // "15M"
    pub audio_bitrate: String,          // "256k"
    pub gop_frames: u32,                // 96 frames (4s @ 24fps)
    pub hwaccel: Option<HwAccelConfig>, // Hardware acceleration config
    pub is_high_bit_depth: bool,        // Input video has high bit depth (10-bit/12-bit)
}

impl Default for TranscodeConfig {
    fn default() -> Self {
        Self {
            segment_duration: 4.0,
            video_bitrate: "15M".to_string(),
            audio_bitrate: "256k".to_string(),
            gop_frames: 96,
            hwaccel: None, // Will be set based on transcode_profile
            is_high_bit_depth: false,
        }
    }
}

impl TranscodeConfig {
    /// Browser-optimized HLS config for maximum compatibility and quality
    pub fn browser() -> Self {
        Self::default()
    }

    /// Create config with hardware acceleration based on available encoders and transcode_profile
    pub fn with_hwaccel(available_hwaccels: &[String], transcode_profile: Option<&str>) -> Self {
        let hwaccel = HwAccelConfig::from_transcode_profile(available_hwaccels, transcode_profile);
        tracing::info!(
            "Using video encoder: {} (hardware: {})",
            hwaccel.name(),
            hwaccel.is_hardware()
        );
        Self {
            hwaccel: Some(hwaccel),
            ..Self::default()
        }
    }

    pub fn uses_hardware_encoder(&self) -> bool {
        self.hwaccel
            .as_ref()
            .map(|hw| hw.is_hardware())
            .unwrap_or(false)
    }

    pub fn force_software_encoder(&mut self, reason: &str) {
        if let Some(hw) = self.hwaccel.as_ref()
            && hw.is_hardware()
        {
            tracing::warn!(
                encoder = %hw.encoder,
                reason = %reason,
                "Falling back to software HLS encoder"
            );
        }
        self.hwaccel = Some(HwAccelConfig::software());
        self.video_bitrate = "8M".to_string();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoStream {
    pub index: usize,
    pub codec_type: String, // "video" or "audio"
    pub codec_name: String, // "h264", "aac", etc.
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub channels: Option<u8>,
    pub bitrate: Option<u64>,
    pub fps: Option<f64>,
    pub lang: Option<String>,
    pub is_default: bool,
    pub profile: Option<String>,
    #[serde(default)]
    pub pix_fmt: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeResult {
    pub duration: f64, // seconds
    pub container: String,
    pub streams: Vec<VideoStream>,
}

impl VideoStream {
    pub fn is_high_bit_depth_video(&self) -> bool {
        if self.codec_type != "video" {
            return false;
        }

        self.profile
            .as_deref()
            .map(text_mentions_high_bit_depth)
            .unwrap_or(false)
            || self
                .pix_fmt
                .as_deref()
                .map(text_mentions_high_bit_depth)
                .unwrap_or(false)
    }
}

impl ProbeResult {
    pub fn has_high_bit_depth_video(&self) -> bool {
        self.streams
            .iter()
            .any(VideoStream::is_high_bit_depth_video)
    }

    pub fn is_hls_ready(&self) -> bool {
        self.duration.is_finite()
            && self.duration > 0.0
            && self
                .streams
                .iter()
                .any(|stream| stream.codec_type == "video" || stream.codec_type == "audio")
    }
}

#[derive(Clone)]
pub struct HlsEngine;

#[derive(Debug)]
pub struct TranscodeProcess {
    pub inner: tokio::process::Child,
}

impl Drop for TranscodeProcess {
    fn drop(&mut self) {
        // Kill the ffmpeg process when the handle is dropped
        let _ = self.inner.start_kill();
        tracing::debug!("TranscodeProcess dropped, killed ffmpeg process");
    }
}

impl TranscodeProcess {
    pub async fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        self.inner.wait().await
    }
}

pub struct TranscodeStream {
    _process: TranscodeProcess,
    stdout: tokio::process::ChildStdout,
}

impl TranscodeStream {
    pub fn new(mut process: TranscodeProcess) -> Option<Self> {
        let stdout = process.inner.stdout.take()?;
        Some(Self {
            _process: process,
            stdout,
        })
    }
}

impl AsyncRead for TranscodeStream {
    fn poll_read(
        mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>, buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stdout).poll_read(cx, buf)
    }
}

impl HlsEngine {
    pub async fn probe_video(file_path: &str) -> Result<ProbeResult> {
        // Start with a tiny probe budget so startup can happen quickly, then retry only
        // when the first pass does not reveal enough metadata.
        let attempts = [
            (750_000u64, 512_000u64),
            (2_000_000u64, 2_000_000u64),
            (5_000_000u64, 5_000_000u64),
        ];

        for (attempt_idx, (analyzeduration, probesize)) in attempts.iter().copied().enumerate() {
            let probe =
                Self::probe_video_with_limits(file_path, analyzeduration, probesize).await?;
            let has_streams = !probe.streams.is_empty();
            let knows_container = probe.container != "unknown";
            if has_streams && knows_container {
                tracing::info!(
                    "probe_video: resolved media info on attempt {} (analyzeduration={}, probesize={})",
                    attempt_idx + 1,
                    analyzeduration,
                    probesize
                );
                return Ok(probe);
            }
            tracing::debug!(
                "probe_video: escalating probe budget after attempt {} (streams={}, container={})",
                attempt_idx + 1,
                probe.streams.len(),
                probe.container
            );
        }

        Self::probe_video_with_limits(file_path, 5_000_000, 5_000_000).await
    }

    async fn probe_video_with_limits(
        file_path: &str, analyzeduration: u64, probesize: u64,
    ) -> Result<ProbeResult> {
        let mut cmd = Command::new("ffmpeg");
        cmd.arg("-analyzeduration").arg(analyzeduration.to_string());
        cmd.arg("-probesize").arg(probesize.to_string());
        cmd.arg("-i").arg(file_path);
        cmd.stdout(Stdio::null());
        cmd.stderr(Stdio::piped());

        tracing::debug!(
            "Spawning ffmpeg probe command with analyzeduration={} probesize={} path={}",
            analyzeduration,
            probesize,
            file_path
        );
        let mut child = cmd.spawn().context("Failed to spawn ffmpeg")?;
        let stderr = child.stderr.take().context("Failed to capture stderr")?;
        let mut reader = BufReader::new(stderr).lines();

        let mut duration = 0.0;
        let mut container = "unknown".to_string();
        let mut streams = Vec::new();

        let re_duration = Regex::new(r"Duration: (\d{2}):(\d{2}):(\d{2}(\.\d+)?)")?;
        let re_input = Regex::new(r"Input #0, ([^,]+),")?;
        let re_stream = Regex::new(r"Stream #\d+:(\d+)(?:\(([^)]+)\))?: (Video|Audio): ([^,]+)")?;
        let re_dim = Regex::new(r"(\d{3,4})x(\d{3,4})")?;
        let re_fps = Regex::new(r"(\d+(\.\d+)?) fps")?;
        let re_bitrate = Regex::new(r"(\d+) kb/s")?;
        let re_channels = Regex::new(r"(\d+)\s+channels?")?;

        while let Ok(Some(line)) = reader.next_line().await {
            let line = line.trim();

            if let Some(caps) = re_input.captures(line)
                && let Some(formats) = caps.get(1)
            {
                let fmts = formats.as_str().to_lowercase();
                if fmts.contains("mp4") {
                    container = "mp4".to_string();
                } else if fmts.contains("matroska") {
                    container = "matroska".to_string();
                } else {
                    container = fmts.split(',').next().unwrap_or("unknown").to_string();
                }
            }

            if let Some(caps) = re_duration.captures(line) {
                let h: f64 = caps[1].parse().unwrap_or(0.0);
                let m: f64 = caps[2].parse().unwrap_or(0.0);
                let s: f64 = caps[3].parse().unwrap_or(0.0);
                duration = h * 3600.0 + m * 60.0 + s;
            }

            if let Some(caps) = re_stream.captures(line) {
                let index: usize = caps[1].parse().unwrap_or(0);
                let lang = caps.get(2).map(|m| m.as_str().to_string());
                let type_str = caps[3].to_lowercase();
                let raw_codec_desc = caps[4].to_string();
                let codec_name = raw_codec_desc
                    .split_whitespace()
                    .next()
                    .unwrap_or("unknown")
                    .to_lowercase();

                let profile = if let Some(start) = raw_codec_desc.find('(') {
                    if let Some(end) = raw_codec_desc.find(')') {
                        if start < end {
                            Some(raw_codec_desc[start + 1..end].to_string())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                let width = re_dim.captures(line).and_then(|c| c[1].parse().ok());
                let height = re_dim.captures(line).and_then(|c| c[2].parse().ok());
                let fps = re_fps.captures(line).and_then(|c| c[1].parse().ok());
                let channels = parse_audio_channels(line, &re_channels);
                let pix_fmt = if type_str == "video" {
                    parse_video_pixel_format(line)
                } else {
                    None
                };
                let bitrate = re_bitrate
                    .captures(line)
                    .and_then(|c| c[1].parse().ok().map(|kb: u64| kb * 1000));

                streams.push(VideoStream {
                    index,
                    codec_type: type_str,
                    codec_name,
                    width,
                    height,
                    channels,
                    bitrate,
                    fps,
                    lang,
                    is_default: line.contains("(default)"),
                    profile,
                    pix_fmt,
                });
            }
        }

        let status = child.wait().await;
        tracing::debug!("ffmpeg probe finished with status: {:?}", status);

        Ok(ProbeResult {
            duration,
            container,
            streams,
        })
    }

    pub fn get_segments(duration: f64) -> Vec<(f64, f64)> {
        // Longer segments (4.0s) reduce overhead and improve stability
        // Matched HLS V2 implementation
        let segment_duration = 4.0;
        let count = (duration / segment_duration).ceil() as usize;
        let mut segments = Vec::new();
        for i in 0..count {
            let start = i as f64 * segment_duration;
            let dur = if start + segment_duration > duration {
                duration - start
            } else {
                segment_duration
            };
            segments.push((start, dur));
        }
        segments
    }

    pub fn get_master_playlist(
        probe: &ProbeResult, info_hash: &str, file_idx: usize, base_url: &str, query_str: &str,
        subtitle_tracks: &[SubtitleTrack],
    ) -> String {
        let config = TranscodeConfig::browser();
        let mut m3u = String::from("#EXTM3U\n#EXT-X-VERSION:4\n");

        // Subtitles Group
        let subs_group_id = "subs";
        let mut has_subtitles = false;

        for (i, sub) in subtitle_tracks.iter().enumerate() {
            has_subtitles = true;
            let lang = "und"; // We don't have lang in SubtitleTrack yet, default to und
            let name = &sub.name;
            let is_default = if i == 0 { "YES" } else { "NO" };
            let is_autoselect = "YES"; // Always autoselect for now to Ensure player sees them

            // URI for VTT subtitles
            // Uses the existing VTT endpoint: /{infoHash}/{fileIdx}/subtitles.vtt
            // But we need to use the sub.id which is the file_idx for the subtitle file
            let sub_uri = format!(
                "{}/{}/{}/subtitles.vtt?{}",
                base_url, info_hash, sub.id, query_str
            );

            m3u.push_str(&format!(
                "#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID=\"{}\",LANGUAGE=\"{}\",NAME=\"{}\",DEFAULT={},AUTOSELECT={},URI=\"{}\"\n",
                subs_group_id, lang, name, is_default, is_autoselect, sub_uri
            ));
        }

        // Collect audio streams from probe
        let audio_streams: Vec<&VideoStream> = probe
            .streams
            .iter()
            .filter(|s| s.codec_type == "audio")
            .collect();

        // Generate EXT-X-MEDIA entries for each audio track
        let audio_group_id = "audio";
        for (i, audio) in audio_streams.iter().enumerate() {
            let lang = audio.lang.as_deref().unwrap_or("und");
            let fallback_name = format!("Audio {}", i + 1);
            let name = audio.lang.as_deref().unwrap_or(&fallback_name);
            let is_default = if i == 0 || audio.is_default {
                "YES"
            } else {
                "NO"
            };
            let is_autoselect = is_default;

            // Use global stream index (audio.index) in URL for robustness
            m3u.push_str(&format!(
                "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"{}\",LANGUAGE=\"{}\",NAME=\"{}\",DEFAULT={},AUTOSELECT={},URI=\"{}/hlsv2/{}/{}/audio-{}.m3u8?{}\"\n",
                audio_group_id, lang, name, is_default, is_autoselect, base_url, info_hash, file_idx, audio.index, query_str
            ));
        }

        // Video stream with audio group reference
        let bandwidth = if config.video_bitrate.ends_with("M") {
            config
                .video_bitrate
                .trim_end_matches("M")
                .parse::<u64>()
                .unwrap_or(12)
                * 1_000_000
        } else {
            12_000_000
        };

        // High profile codecs for browser
        let codecs = "avc1.640028,mp4a.40.2"; // High Profile Level 4.0

        if !audio_streams.is_empty() {
            let mut line = format!(
                "#EXT-X-STREAM-INF:BANDWIDTH={},CODECS=\"{}\",AUDIO=\"{}\"",
                bandwidth, codecs, audio_group_id
            );
            if has_subtitles {
                line.push_str(&format!(",SUBTITLES=\"{}\"", subs_group_id));
            }
            m3u.push_str(&line);
            m3u.push('\n');
        } else {
            let mut line = format!(
                "#EXT-X-STREAM-INF:BANDWIDTH={},CODECS=\"{}\"",
                bandwidth, codecs
            );
            if has_subtitles {
                line.push_str(&format!(",SUBTITLES=\"{}\"", subs_group_id));
            }
            m3u.push_str(&line);
            m3u.push('\n');
        }

        // Video stream playlist URL
        m3u.push_str(&format!(
            "{}/hlsv2/{}/{}/stream-0.m3u8?{}\n",
            base_url, info_hash, file_idx, query_str
        ));

        m3u
    }

    pub fn get_stream_playlist(
        probe: &ProbeResult, _stream_idx: usize, segment_base_url: &str,
        audio_track_idx: Option<usize>, query_str: &str,
    ) -> String {
        let segments = Self::get_segments(probe.duration);

        if segments.is_empty() {
            // Keep the media playlist reloadable while torrent metadata is still
            // arriving. An empty VOD playlist with ENDLIST makes clients stop.
            let mut m3u = String::from("#EXTM3U\n");
            m3u.push_str("#EXT-X-VERSION:3\n");
            m3u.push_str("#EXT-X-TARGETDURATION:2\n");
            m3u.push_str("#EXT-X-MEDIA-SEQUENCE:0\n");
            return m3u;
        }

        // Calculate max duration for target duration
        let max_duration = segments
            .iter()
            .map(|(_, dur)| dur.ceil() as u32)
            .max()
            .unwrap_or(4);

        let mut m3u = String::from("#EXTM3U\n");
        m3u.push_str("#EXT-X-VERSION:3\n");
        m3u.push_str(&format!("#EXT-X-TARGETDURATION:{}\n", max_duration));
        m3u.push_str("#EXT-X-MEDIA-SEQUENCE:0\n");
        m3u.push_str("#EXT-X-PLAYLIST-TYPE:VOD\n");

        // MPEG-TS segments (no init segment required)
        for (i, (_start, dur)) in segments.iter().enumerate() {
            m3u.push_str(&format!("#EXTINF:{:.6},\n", dur));
            let filename = if let Some(audio_idx) = audio_track_idx {
                format!(
                    "{}audio-{}-{}.ts?{}",
                    segment_base_url, audio_idx, i, query_str
                )
            } else {
                format!("{}{}.ts?{}", segment_base_url, i, query_str)
            };
            m3u.push_str(&format!("{}\n", filename));
        }
        m3u.push_str("#EXT-X-ENDLIST\n");
        m3u
    }

    /// Transcode video-only segment for HLS V2
    /// Implements exact logic from ebml_933.js
    pub async fn transcode_video_segment(
        input_path: &str, start: f64, duration: f64, config: &TranscodeConfig,
    ) -> anyhow::Result<TranscodeProcess> {
        let mut cmd = tokio::process::Command::new("ffmpeg");
        configure_low_impact_ffmpeg(&mut cmd);

        // Reduce FFmpeg verbosity
        cmd.args(["-loglevel", "warning"]);

        // Input flags (Global)
        // +discardcorrupt allows skipping corrupt frames
        // +genpts regenerates presentation timestamps
        cmd.args(["-fflags", "+genpts+discardcorrupt"]);

        // Keep the first segment responsive; the probe path already identifies the streams.
        cmd.args(["-analyzeduration", "2000000", "-probesize", "2000000"]);

        // Hardware acceleration INPUT flags (MUST be before -i)
        // Disable hardware decoding only for native QSV decoding on high-bit-depth streams,
        // as it is prone to driver and format compatibility crashes on Windows.
        // Other methods like d3d11va, cuda, and vaapi are fully compatible and performant.
        let use_hw_decoding = if let Some(ref hw) = config.hwaccel {
            if hw.is_hardware() && config.is_high_bit_depth {
                hw.hwaccel.as_deref() != Some("qsv")
            } else {
                hw.is_hardware()
            }
        } else {
            false
        };

        if use_hw_decoding && let Some(ref hw) = config.hwaccel {
            if let Some(ref accel) = hw.hwaccel {
                cmd.args(["-hwaccel", accel]);
            }
            if let Some(ref device) = hw.device {
                cmd.args(["-hwaccel_device", device]);
            }
        }

        // HYBRID SEEKING APPROACH:
        // 1. Fast input seek to get close (keyframe before target)
        // 2. Output seek to precise position (ensures proper frame decoding)
        // This is much faster than pure output seeking for later segments
        let input_seek_offset = 10.0; // Seek to 10s before target for safety margin
        let input_seek = (start - input_seek_offset).max(0.0);
        let output_seek = start - input_seek;

        // Input seeking (fast, coarse) - BEFORE -i
        if input_seek > 0.0 {
            cmd.arg("-ss").arg(format!("{:.3}", input_seek));
        }

        cmd.arg("-i").arg(input_path);

        // Output seeking (accurate, slower) - AFTER -i
        // This decodes from the input seek point and discards until exact target
        if output_seek > 0.0 {
            cmd.arg("-ss").arg(format!("{:.3}", output_seek));
        }

        // Output timestamp offset - sets segment timestamps correctly
        cmd.arg("-output_ts_offset").arg(format!("{:.3}", start));

        // Duration limit (CRITICAL: prevent transcoding entire file)
        cmd.arg("-t").arg(format!("{:.3}", duration));

        let threads = video_thread_count(config);
        cmd.args(["-threads", &threads]);

        // Metadata cleanup
        cmd.args(["-max_muxing_queue_size", "2048"]);
        cmd.args(["-ignore_unknown"]);
        cmd.args(["-map_metadata", "-1", "-map_chapters", "-1"]);
        cmd.args(["-map", "-0:d?", "-map", "-0:t?"]);

        // Output mapping: video only
        cmd.args(["-map", "0:v:0", "-an", "-sn"]);

        // Video encoding: use hardware acceleration if configured, else software fallback
        let is_hw_encoder = config
            .hwaccel
            .as_ref()
            .map(|hw| hw.is_hardware())
            .unwrap_or(false);

        if let Some(ref hw) = config.hwaccel {
            // Encoder (hwaccel flags already added before -i)
            cmd.args(["-c:v", &hw.encoder]);

            // Encoder-specific extra args (preset, rc, etc.)
            for arg in &hw.extra_args {
                cmd.arg(arg);
            }

            // For software encoders, add video filter and profile settings
            if !hw.is_hardware() {
                let pix_fmt = hw.pix_fmt.as_deref().unwrap_or("yuv420p");
                let filter = software_video_filter(pix_fmt);
                cmd.arg("-vf").arg(&filter);
                cmd.args(["-profile:v", "high", "-level", "51"]);
                tracing::info!(
                    threads = %threads,
                    filter = %filter,
                    "Configured low-impact software HLS video transcode"
                );
            } else {
                // Hardware encoder: force a compatible 8-bit format for high bit depth inputs
                if config.is_high_bit_depth {
                    let pix_fmt = hw.pix_fmt.as_deref().unwrap_or("nv12");
                    cmd.arg("-pix_fmt").arg(pix_fmt);
                } else if let Some(ref pix_fmt) = hw.pix_fmt {
                    cmd.arg("-pix_fmt").arg(pix_fmt);
                }
            }
        } else {
            // Default software encoding fallback
            let filter = software_video_filter("yuv420p");
            cmd.arg("-vf").arg(&filter);
            cmd.args([
                "-c:v",
                "libx264",
                "-preset:v",
                "veryfast",
                "-profile:v",
                "high",
                "-tune:v",
                "zerolatency",
                "-level",
                "51",
            ]);
            tracing::info!(
                threads = %threads,
                filter = %filter,
                "Configured low-impact software HLS video transcode"
            );
        }

        // Fixed GOP for HLS V2 segment alignment
        // Note: sc_threshold only works with software encoders
        let gop = config.gop_frames.to_string();
        if !is_hw_encoder {
            cmd.args(["-sc_threshold", "0"]);
        }
        cmd.args(["-g", &gop, "-keyint_min", &gop]);

        // Bitrate control (can be scaled here if needed, sticking to config for now)
        let video_bitrate_num = config
            .video_bitrate
            .trim_end_matches('M')
            .parse::<u32>()
            .unwrap_or(15);
        let bufsize = video_bitrate_num * 2;
        cmd.args(["-b:v", &format!("{}", video_bitrate_num * 1_000_000)]);
        cmd.args(["-maxrate", &format!("{}", video_bitrate_num * 1_000_000)]);
        cmd.args(["-bufsize", &format!("{}", bufsize * 1_000_000)]);

        // Output format: MPEG-TS for HLS with copyts to preserve timestamps
        cmd.args(["-mpegts_copyts", "1", "-f", "mpegts", "pipe:1"]);

        tracing::debug!("FFmpeg video command (HLS V2): {:?}", cmd);

        #[allow(clippy::zombie_processes)]
        let mut child = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("Failed to spawn ffmpeg for video segment: {:?}", cmd))?;

        // Spawn a task to log stderr in background (for debugging)
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(async move {
                use tokio::io::AsyncBufReadExt;
                let mut reader = tokio::io::BufReader::new(stderr);
                let mut line = String::new();
                while reader.read_line(&mut line).await.unwrap_or(0) > 0 {
                    if !line.trim().is_empty() {
                        tracing::warn!("FFmpeg video stderr: {}", line.trim());
                    }
                    line.clear();
                }
            });
        }

        Ok(TranscodeProcess { inner: child })
    }

    /// Transcode audio-only segment for HLS V2
    /// Implements exact logic from ebml_933.js
    pub async fn transcode_audio_segment(
        input_path: &str, start: f64, duration: f64, audio_stream_index: usize,
        config: &TranscodeConfig,
    ) -> anyhow::Result<TranscodeProcess> {
        let mut cmd = tokio::process::Command::new("ffmpeg");
        configure_low_impact_ffmpeg(&mut cmd);

        cmd.args(["-loglevel", "warning"]);

        // Input flags (Global) - match video transcoding approach
        cmd.args(["-fflags", "+genpts+discardcorrupt"]);

        // Audio segments need only a small startup budget once the stream is identified.
        cmd.args(["-analyzeduration", "1000000", "-probesize", "1000000"]);

        // HYBRID SEEKING APPROACH (same as video):
        // 1. Fast input seek to get close
        // 2. Output seek to precise position
        let input_seek_offset = 10.0;
        let input_seek = (start - input_seek_offset).max(0.0);
        let output_seek = start - input_seek;

        // Input seeking (fast, coarse) - BEFORE -i
        if input_seek > 0.0 {
            cmd.arg("-ss").arg(format!("{:.3}", input_seek));
        }

        cmd.arg("-i").arg(input_path);

        // Output seeking (accurate) - AFTER -i
        if output_seek > 0.0 {
            cmd.arg("-ss").arg(format!("{:.3}", output_seek));
        }

        // Output timestamp offset - REQUIRED to match video timestamps
        cmd.arg("-output_ts_offset").arg(format!("{:.3}", start));

        cmd.args(["-threads", "1"]);

        // Duration limit (CRITICAL)
        cmd.arg("-t").arg(format!("{:.3}", duration));

        // Metadata cleanup
        cmd.args(["-max_muxing_queue_size", "2048"]);
        cmd.args(["-ignore_unknown"]);
        cmd.args(["-map_metadata", "-1", "-map_chapters", "-1"]);
        cmd.args(["-map", "-0:d?", "-map", "-0:t?"]);

        // Output mapping: specific audio track by GLOBAL index
        // Use 0:{index} to be precise, instead of a:{index} which is relative to audio streams
        cmd.arg("-map").arg(format!("0:{}", audio_stream_index));
        cmd.args(["-vn", "-sn"]);

        // Audio encoding HLS V2: resample to keep segment timestamps monotonic,
        // then pad short segments to avoid client-side under-runs.
        cmd.args([
            "-c:a",
            "aac",
            "-af",
            "aresample=async=1:first_pts=0,apad",
            "-ac",
            "2",
            "-b:a",
            &config.audio_bitrate,
        ]);

        // Output format: MPEG-TS for HLS
        cmd.args(["-mpegts_copyts", "1", "-f", "mpegts", "pipe:1"]);

        tracing::debug!("FFmpeg audio command (HLS V2): {:?}", cmd);

        #[allow(clippy::zombie_processes)]
        let mut child = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("Failed to spawn ffmpeg for audio segment: {:?}", cmd))?;

        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(async move {
                use tokio::io::AsyncBufReadExt;
                let mut reader = tokio::io::BufReader::new(stderr);
                let mut line = String::new();
                while reader.read_line(&mut line).await.unwrap_or(0) > 0 {
                    if !line.trim().is_empty() {
                        tracing::warn!("FFmpeg audio stderr: {}", line.trim());
                    }
                    line.clear();
                }
            });
        }

        Ok(TranscodeProcess { inner: child })
    }
}

fn configure_low_impact_ffmpeg(_cmd: &mut Command) {
    #[cfg(windows)]
    {
        const BELOW_NORMAL_PRIORITY_CLASS: u32 = 0x0000_4000;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        _cmd.creation_flags(BELOW_NORMAL_PRIORITY_CLASS | CREATE_NO_WINDOW);
    }
}

fn video_thread_count(config: &TranscodeConfig) -> String {
    if config.uses_hardware_encoder() {
        return "0".to_string();
    }

    let threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2)
        .saturating_sub(2)
        .clamp(1, 4);
    threads.to_string()
}

fn software_video_filter(pix_fmt: &str) -> String {
    format!("scale=w='min(1920,iw)':h=-2:flags=fast_bilinear,format={pix_fmt}")
}

fn parse_audio_channels(line: &str, re_channels: &Regex) -> Option<u8> {
    let line = line.to_lowercase();
    if line.contains("mono") {
        return Some(1);
    }
    if line.contains("stereo") {
        return Some(2);
    }
    if line.contains("5.1") {
        return Some(6);
    }
    if line.contains("7.1") {
        return Some(8);
    }
    re_channels
        .captures(&line)
        .and_then(|caps| caps.get(1))
        .and_then(|m| m.as_str().parse::<u8>().ok())
}

fn parse_video_pixel_format(line: &str) -> Option<String> {
    let (_, video_desc) = line.split_once("Video:")?;
    let pix_fmt = video_desc.split(',').nth(1)?.trim();
    let pix_fmt = pix_fmt
        .split(['(', ' '])
        .next()
        .unwrap_or(pix_fmt)
        .trim()
        .to_ascii_lowercase();
    (!pix_fmt.is_empty()).then_some(pix_fmt)
}

fn text_mentions_high_bit_depth(value: &str) -> bool {
    let value = value.to_ascii_lowercase();
    value.contains("main 10")
        || value.contains("main10")
        || value.contains("high 10")
        || value.contains("high10")
        || value.contains("10 bit")
        || value.contains("10-bit")
        || value.contains("12 bit")
        || value.contains("12-bit")
        || value.contains("p010")
        || value.contains("p016")
        || value.contains("yuv420p10")
        || value.contains("yuv422p10")
        || value.contains("yuv444p10")
        || value.contains("yuv420p12")
        || value.contains("yuv422p12")
        || value.contains("yuv444p12")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn video_stream(profile: Option<&str>, pix_fmt: Option<&str>) -> VideoStream {
        VideoStream {
            index: 0,
            codec_type: "video".to_string(),
            codec_name: "hevc".to_string(),
            width: Some(3840),
            height: Some(2160),
            channels: None,
            bitrate: None,
            fps: Some(23.976),
            lang: None,
            is_default: true,
            profile: profile.map(str::to_string),
            pix_fmt: pix_fmt.map(str::to_string),
        }
    }

    #[test]
    fn parses_ffmpeg_video_pixel_format() {
        let line = "Stream #0:0: Video: hevc (Main 10), yuv420p10le(tv), 3840x2160, 23.98 fps";
        assert_eq!(
            parse_video_pixel_format(line).as_deref(),
            Some("yuv420p10le")
        );
    }

    #[test]
    fn detects_high_bit_depth_from_profile_or_pixel_format() {
        assert!(video_stream(Some("Main 10"), None).is_high_bit_depth_video());
        assert!(video_stream(None, Some("p010le")).is_high_bit_depth_video());
        assert!(!video_stream(Some("High"), Some("yuv420p")).is_high_bit_depth_video());
    }
}
