use crate::backend::{
    BackendFileInfo, BackendMemoryDiagnostics, EngineStats, FileStreamTrait, Growler, PeerSearch,
    PieceReadiness, StatsFile, StatsOptions, SwarmCap, TorrentBackend, TorrentHandle,
    TorrentSource,
};
use anyhow::{Context, Result};
use librqbit::{ManagedTorrent, Session};
use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

const LIBRQBIT_LISTEN_PORTS: RangeInclusive<u16> = 42000..=42009;

fn session_options(download_dir: &std::path::Path, port: u16) -> librqbit::SessionOptions {
    librqbit::SessionOptions {
        listen: Some(librqbit::ListenerOptions {
            listen_addr: ([0, 0, 0, 0], port).into(),
            enable_upnp_port_forwarding: true,
            ..Default::default()
        }),
        persistence: Some(librqbit::SessionPersistenceConfig::Json {
            folder: Some(download_dir.to_path_buf()),
        }),
        connect: Some(librqbit::ConnectionOptions {
            peer_opts: Some(librqbit::PeerConnectionOptions {
                connect_timeout: Some(Duration::from_secs(10)),
                read_write_timeout: Some(Duration::from_secs(30)),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn is_listen_bind_failure(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let is_io_address_error = cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io_error| {
                io_error.kind() == std::io::ErrorKind::AddrInUse
                    || cfg!(windows) && matches!(io_error.raw_os_error(), Some(10013 | 10048))
            });

        // librqbit-dualstack-sockets 0.7 wraps its std::io::Error without
        // exposing it as an Error::source(), so the typed error is not always
        // present in anyhow's chain. Its stable bind-error display prefix lets
        // us distinguish bind failures from unrelated session startup errors.
        is_io_address_error || cause.to_string().starts_with("error binding:")
    })
}

async fn start_session_with_options<F>(
    download_dir: PathBuf,
    listen_ports: RangeInclusive<u16>,
    mut options_for_port: F,
) -> Result<Arc<Session>>
where
    F: FnMut(u16) -> librqbit::SessionOptions,
{
    let mut last_address_error = None;

    for port in listen_ports {
        match Session::new_with_opts(download_dir.clone(), options_for_port(port)).await {
            Ok(session) => return Ok(session),
            Err(error) if is_listen_bind_failure(&error) => {
                warn!(port, error = %error, "librqbit listen port is occupied; trying the next port");
                last_address_error = Some(error);
            }
            Err(error) => return Err(error),
        }
    }

    match last_address_error {
        Some(error) => Err(error).context("all configured librqbit listen ports are occupied"),
        None => anyhow::bail!("librqbit listen port range is empty"),
    }
}

pub struct LibrqbitBackend {
    pub session: Arc<Session>,
}

impl LibrqbitBackend {
    pub async fn new(download_dir: PathBuf) -> Result<(Self, HashMap<String, LibrqbitHandle>)> {
        tokio::fs::create_dir_all(&download_dir).await?;
        debug!(path = ?download_dir, "Storing downloads");

        let options_dir = download_dir.clone();
        let session =
            start_session_with_options(download_dir.clone(), LIBRQBIT_LISTEN_PORTS, |port| {
                session_options(&options_dir, port)
            })
            .await?;
        // Restore from session
        let mut restored_handles = session.with_torrents(|iter| {
            let mut map = HashMap::new();
            for (_id, handle) in iter {
                let info_hash = handle.info_hash().as_string();
                map.insert(
                    info_hash.clone(),
                    LibrqbitHandle {
                        handle: handle.clone(),
                        info_hash,
                    },
                );
            }
            map
        });

        // Restore from .cache directory
        let cache_dir = download_dir.join(".cache");
        if let Ok(mut entries) = tokio::fs::read_dir(&cache_dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                if path.extension().is_some_and(|ext| ext == "torrent")
                    && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
                {
                    let info_hash = stem.to_string();
                    if !restored_handles.contains_key(&info_hash)
                        && let Ok(bytes) = tokio::fs::read(&path).await
                    {
                        let bytes = bytes::Bytes::from(bytes);
                        let add_torrent = librqbit::AddTorrent::from_bytes(bytes);
                        match session.add_torrent(add_torrent, None).await {
                            Ok(response) => {
                                if let librqbit::AddTorrentResponse::Added(_, handle)
                                | librqbit::AddTorrentResponse::AlreadyManaged(_, handle) =
                                    response
                                {
                                    restored_handles.insert(
                                        info_hash.clone(),
                                        LibrqbitHandle { handle, info_hash },
                                    );
                                }
                            }
                            Err(e) => warn!(error = %e, "Failed to add torrent from cache"),
                        }
                    }
                }
            }
        }

        Ok((Self { session }, restored_handles))
    }
}

pub struct LibrqbitHandle {
    pub handle: Arc<ManagedTorrent>,
    pub info_hash: String,
}

#[async_trait::async_trait]
impl TorrentBackend for LibrqbitBackend {
    type Handle = LibrqbitHandle;

    async fn add_torrent(
        &self,
        source: TorrentSource,
        trackers: Vec<String>,
    ) -> Result<Self::Handle> {
        let add_torrent = match source {
            TorrentSource::Url(url) => librqbit::AddTorrent::Url(url.into()),
            TorrentSource::Bytes(bytes) => {
                librqbit::AddTorrent::from_bytes(bytes::Bytes::from(bytes))
            }
        };
        let response = self
            .session
            .add_torrent(
                add_torrent,
                Some(librqbit::AddTorrentOptions {
                    overwrite: true,
                    trackers: Some(trackers),
                    ..Default::default()
                }),
            )
            .await
            .context("Failed to add torrent to librqbit")?;

        let (_id, handle) = match response {
            librqbit::AddTorrentResponse::Added(id, handle)
            | librqbit::AddTorrentResponse::AlreadyManaged(id, handle) => (id, handle),
            _ => return Err(anyhow::anyhow!("Unexpected response from librqbit")),
        };

        Ok(LibrqbitHandle {
            handle,
            info_hash: "".to_string(), // Will be updated by Engine
        })
    }

    async fn get_torrent(&self, _info_hash: &str) -> Option<Self::Handle> {
        None
    }

    async fn remove_torrent(&self, _info_hash: &str) -> Result<()> {
        Ok(())
    }

    async fn list_torrents(&self) -> Vec<String> {
        Vec::new()
    }

    async fn memory_diagnostics(&self) -> BackendMemoryDiagnostics {
        BackendMemoryDiagnostics::default()
    }
}

#[async_trait::async_trait]
impl TorrentHandle for LibrqbitHandle {
    fn info_hash(&self) -> String {
        self.handle.info_hash().as_string()
    }

    fn name(&self) -> Option<String> {
        self.handle
            .metadata
            .load_full()
            .and_then(|m| m.info.name().map(|n| n.into_owned()))
    }

    async fn stats(&self) -> EngineStats {
        let stats = self.handle.stats();
        let (download_speed, upload_speed) = if let Some(ref live) = stats.live {
            (
                live.download_speed.mbps * 1_048_576.0 / 8.0,
                live.upload_speed.mbps * 1_048_576.0 / 8.0,
            )
        } else {
            (0.0, 0.0)
        };

        let (downloaded, uploaded) = if let Some(ref live) = stats.live {
            (live.snapshot.fetched_bytes, live.snapshot.uploaded_bytes)
        } else {
            (0, 0)
        };

        let peers = stats
            .live
            .as_ref()
            .map(|l| l.snapshot.peer_stats.live as u64)
            .unwrap_or(0);

        let mut files = Vec::new();
        let mut total_size = 0u64;
        let mut offset = 0u64;
        if let Some(m) = self.handle.metadata.load_full() {
            for f in m.info.iter_file_details() {
                let filename = f.filename.to_string();
                files.push(StatsFile {
                    name: filename.clone(),
                    path: filename,
                    length: f.len,
                    offset,
                    downloaded: 0, // TODO: Implement per-file progress for librqbit if needed
                    progress: 0.0,
                });
                total_size += f.len;
                offset += f.len;
            }
        }

        EngineStats {
            name: self.name().unwrap_or_else(|| "Unknown".to_string()),
            info_hash: self.info_hash(),
            files,
            sources: vec![],
            opts: StatsOptions {
                dht: true,
                tracker: true,
                path: "".to_string(),
                growler: Growler {
                    flood: 0,
                    pulse: None,
                },
                peer_search: PeerSearch {
                    max: 100,
                    min: 10,
                    sources: vec![],
                },
                swarm_cap: SwarmCap {
                    max_speed: None,
                    min_peers: None,
                },
                connections: None,
                handshake_timeout: None,
                timeout: None,
                r#virtual: false,
            },
            download_speed,
            upload_speed,
            downloaded,
            uploaded,
            peers,
            unchoked: peers,
            queued: 0,
            unique: peers,
            connection_tries: 0,
            peer_search_running: true,
            stream_len: total_size,
            stream_name: "".to_string(),
            stream_progress: if total_size > 0 {
                downloaded as f64 / total_size as f64
            } else {
                0.0
            },
            swarm_connections: peers,
            swarm_paused: false,
            swarm_size: peers,
            is_finished: total_size > 0 && downloaded >= total_size,
            has_metadata: total_size > 0,
        }
    }

    async fn add_trackers(&self, _trackers: Vec<String>) -> Result<()> {
        Ok(())
    }

    async fn get_file_reader(
        &self,
        file_idx: usize,
        _start_offset: u64,
        _priority: u8,
        _bitrate: Option<u64>,
        _intent: crate::backend::priorities::PlaybackIntent,
    ) -> Result<Box<dyn FileStreamTrait>> {
        let stream = self
            .handle
            .clone()
            .stream(file_idx)
            .await
            .context("Failed to stream from librqbit")?;
        Ok(Box::new(stream))
    }

    async fn get_files(&self) -> Vec<BackendFileInfo> {
        let mut files = Vec::new();
        if let Some(m) = self.handle.metadata.load_full() {
            for f in m.info.iter_file_details() {
                files.push(BackendFileInfo {
                    name: f.filename.to_string(),
                    length: f.len,
                });
            }
        }
        files
    }

    async fn get_file_path(&self, _file_idx: usize) -> Option<String> {
        // librqbit doesn't expose local file paths easily
        // Return None to fall back to HTTP URL probing
        None
    }

    async fn prepare_file_for_streaming(&self, _file_idx: usize) -> Result<()> {
        Ok(())
    }

    async fn keep_file_downloading(&self, _file_idx: usize) -> Result<()> {
        Ok(())
    }

    async fn clear_file_streaming(&self, _file_idx: usize) -> Result<()> {
        Ok(())
    }

    async fn wait_for_piece_ready(
        &self,
        _file_idx: usize,
        _offset: u64,
        _timeout: Duration,
        _intent: crate::backend::priorities::PlaybackIntent,
    ) -> Result<PieceReadiness> {
        Ok(PieceReadiness {
            ready: true,
            piece: -1,
            ready_pieces: 1,
            target_pieces: 1,
            elapsed_ms: 0,
            peers: 0,
            download_rate: 0,
            reason: "librqbit-reader".to_string(),
        })
    }
}

impl Clone for LibrqbitHandle {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            info_hash: self.info_hash.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use librqbit::{ListenerOptions, SessionOptions};
    use std::net::{Ipv4Addr, TcpListener};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn adjacent_test_ports() -> (TcpListener, u16, u16) {
        for _ in 0..100 {
            let occupied =
                TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind an ephemeral test port");
            let first_port = occupied.local_addr().expect("test listener address").port();
            let Some(second_port) = first_port.checked_add(1) else {
                continue;
            };

            if let Ok(probe) = TcpListener::bind((Ipv4Addr::LOCALHOST, second_port)) {
                drop(probe);
                return (occupied, first_port, second_port);
            }
        }

        panic!("could not reserve adjacent test ports");
    }

    #[tokio::test]
    async fn session_start_retries_the_next_port_when_the_first_is_occupied() {
        let (_occupied, first_port, second_port) = adjacent_test_ports();
        let test_dir = std::env::temp_dir().join(format!(
            "enginefs-librqbit-port-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time after epoch")
                .as_nanos()
        ));
        tokio::fs::create_dir_all(&test_dir)
            .await
            .expect("create librqbit test directory");

        let session =
            start_session_with_options(test_dir.clone(), first_port..=second_port, |port| {
                SessionOptions {
                    dht: None,
                    listen: Some(ListenerOptions {
                        listen_addr: (Ipv4Addr::LOCALHOST, port).into(),
                        ipv4_only: true,
                        ..Default::default()
                    }),
                    disable_local_service_discovery: true,
                    ..Default::default()
                }
            })
            .await
            .expect("session should retry the next port");

        assert_eq!(
            session
                .listen_addr()
                .expect("session listen address")
                .port(),
            second_port
        );

        session.cancellation_token().cancel();
        drop(session);
        tokio::fs::remove_dir_all(test_dir)
            .await
            .expect("remove librqbit test directory");
    }
}
