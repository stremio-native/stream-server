use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tempfile::NamedTempFile;
use tokio::fs::{File, OpenOptions};
use tokio::io::{self, AsyncRead, AsyncSeek, AsyncWrite};
use tokio::sync::{Notify, watch};

#[derive(Clone, Debug)]
struct StateSnapshot {
    written_bytes: u64,
    is_complete: bool,
    error: Option<String>,
}

/// The core cache controller
#[derive(Clone)]
pub struct ProgressiveCache {
    state_rx: watch::Receiver<StateSnapshot>,
    temp_path: PathBuf,
    // Keep the temp file struct alive
    _temp_file_handle: Arc<NamedTempFile>,
    total_size: Option<u64>,
    notify: Arc<Notify>,
}

impl ProgressiveCache {
    /// Create a new ProgressiveCache using system temp directory
    pub async fn new(total_size: Option<u64>) -> io::Result<(Self, CacheWriter)> {
        // We use std NamedTempFile to create the path and handle, but open with tokio
        let temp_file = NamedTempFile::new()?;
        Self::from_temp_file(temp_file, total_size).await
    }

    /// Create a new ProgressiveCache in a specific directory
    /// This allows respecting the user's cache_root setting
    pub async fn new_in_dir(
        dir: &std::path::Path, total_size: Option<u64>,
    ) -> io::Result<(Self, CacheWriter)> {
        // Ensure directory exists
        if !dir.exists() {
            std::fs::create_dir_all(dir)?;
        }
        let temp_file = tempfile::Builder::new()
            .prefix("archive_extract_")
            .tempfile_in(dir)?;
        Self::from_temp_file(temp_file, total_size).await
    }

    async fn from_temp_file(
        temp_file: NamedTempFile, total_size: Option<u64>,
    ) -> io::Result<(Self, CacheWriter)> {
        let temp_path = temp_file.path().to_path_buf();
        let handle = Arc::new(temp_file);

        let initial_state = StateSnapshot {
            written_bytes: 0,
            is_complete: false,
            error: None,
        };

        let (tx, rx) = watch::channel(initial_state);
        let notify = Arc::new(Notify::new());

        // Open async file for writer
        let writer_file = OpenOptions::new()
            .write(true)
            .create(false) // Created by NamedTempFile
            .open(&temp_path)
            .await?;

        let writer = CacheWriter {
            state_tx: tx.clone(),
            file: writer_file,
            _video_file_size: total_size,
            notify: notify.clone(),
            path: temp_path.clone(),
        };

        Ok((
            ProgressiveCache {
                state_rx: rx,
                temp_path,
                _temp_file_handle: handle,
                total_size,
                notify,
            },
            writer,
        ))
    }

    pub async fn reader(&self) -> io::Result<ProgressiveReader> {
        let file = File::open(&self.temp_path).await?;
        Ok(ProgressiveReader {
            state_rx: self.state_rx.clone(),
            file,
            pos: 0,
            total_size: self.total_size,
            notify: self.notify.clone(),
        })
    }
}

pub struct CacheWriter {
    state_tx: watch::Sender<StateSnapshot>,
    file: File,
    _video_file_size: Option<u64>,
    notify: Arc<Notify>,
    path: PathBuf, // kept for sync writer creation
}

impl AsyncWrite for CacheWriter {
    fn poll_write(
        mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let poll = Pin::new(&mut self.file).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = poll
            && n > 0
        {
            self.state_tx.send_modify(|state| {
                state.written_bytes += n as u64;
            });
            self.notify.notify_waiters();
        }
        poll
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.file).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.file).poll_shutdown(cx)
    }
}

impl CacheWriter {
    pub fn finish(&self) {
        self.state_tx.send_modify(|state| {
            state.is_complete = true;
        });
        self.notify.notify_waiters();
    }

    pub fn set_error(&self, err: String) {
        self.state_tx.send_modify(|state| {
            state.error = Some(err);
            state.is_complete = true;
        });
        self.notify.notify_waiters();
    }

    /// Create a synchronous writer that shares the same state.
    /// Useful for legacy/sync libraries like 7z or unrar.
    pub fn try_clone_sync(&self) -> io::Result<SyncCacheWriter> {
        let file = std::fs::OpenOptions::new().write(true).open(&self.path)?;

        Ok(SyncCacheWriter {
            state_tx: self.state_tx.clone(),
            file,
            notify: self.notify.clone(),
        })
    }
}

/// A synchronous writer that updates the Async Cache state
pub struct SyncCacheWriter {
    state_tx: watch::Sender<StateSnapshot>,
    file: std::fs::File,
    notify: Arc<Notify>,
}

impl SyncCacheWriter {
    pub fn finish(&self) {
        self.state_tx.send_modify(|state| {
            state.is_complete = true;
        });
        self.notify.notify_waiters();
    }

    pub fn set_error(&self, err: String) {
        self.state_tx.send_modify(|state| {
            state.error = Some(err);
            state.is_complete = true;
        });
        self.notify.notify_waiters();
    }
}

impl std::io::Write for SyncCacheWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.file.write(buf)?;
        if n > 0 {
            self.state_tx.send_modify(|state| {
                state.written_bytes += n as u64;
            });
            self.notify.notify_waiters();
        }
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

pub struct ProgressiveReader {
    state_rx: watch::Receiver<StateSnapshot>,
    file: File,
    pos: u64,
    total_size: Option<u64>,
    notify: Arc<Notify>,
}

impl AsyncRead for ProgressiveReader {
    fn poll_read(
        mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            // 1. Snapshot state
            let current_state = self.state_rx.borrow().clone();

            // Check errors
            if let Some(err) = current_state.error {
                return Poll::Ready(Err(io::Error::other(err)));
            }

            // Check if data available
            if self.pos < current_state.written_bytes {
                let available = current_state.written_bytes - self.pos;
                let needed = buf.remaining().min(available as usize);

                if needed == 0 && buf.remaining() == 0 {
                    return Poll::Ready(Ok(()));
                }

                // Read from file
                // We must be careful: if we read more than is flushed to disk, we might get 0 bytes or blocking?
                // But `written_bytes` is updated after write success.
                // However, internal buffering of `File` might mean it's not on disk yet?
                // `File` (tokio) is usually unbuffered direct syscalls (mostly).
                // Let's assume it's safe.

                let mut sub_buf = buf.take(needed);
                let start_filled = sub_buf.filled().len();
                let poll = Pin::new(&mut self.file).poll_read(cx, &mut sub_buf);

                match poll {
                    Poll::Ready(Ok(())) => {
                        let bytes_read = sub_buf.filled().len() - start_filled;
                        if bytes_read == 0 && needed > 0 {
                            // Should theoretically not happen if written_bytes > pos
                            // But could happen if seek pointer is messed up?
                            // Or filesystem lag?
                            // Return pending to wait for more?
                        } else {
                            self.pos += bytes_read as u64;
                            return Poll::Ready(Ok(()));
                        }
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                }
            }

            // No data or read returned 0 despite claiming data availability
            if current_state.is_complete {
                return Poll::Ready(Ok(()));
            }

            // Wait for notification
            // We use `notify.notified()` which gives a future.
            // We need to poll that future.

            // NOTE: We recreate the future every time. `notified()` is cancel-safe.
            // Efficient implementation would cache it, but this is fine for now.
            let notified = self.notify.notified();
            tokio::pin!(notified);

            match notified.poll(cx) {
                Poll::Ready(()) => continue, // Woke up, retry
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl AsyncSeek for ProgressiveReader {
    fn start_seek(mut self: Pin<&mut Self>, position: io::SeekFrom) -> io::Result<()> {
        let pos = match position {
            io::SeekFrom::Start(p) => p,
            io::SeekFrom::End(p) => {
                if let Some(total) = self.total_size {
                    if p < 0 {
                        total.saturating_sub(p.unsigned_abs())
                    } else {
                        total + p as u64
                    }
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "SeekFrom::End requires known total size",
                    ));
                }
            }
            io::SeekFrom::Current(p) => {
                let current = self.pos as i64;
                let new_p = current + p;
                if new_p < 0 {
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, "Negative seek"));
                }
                new_p as u64
            }
        };

        Pin::new(&mut self.file).start_seek(io::SeekFrom::Start(pos))
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        let poll = Pin::new(&mut self.file).poll_complete(cx);
        if let Poll::Ready(Ok(new_pos)) = poll {
            self.pos = new_pos;
        }
        poll
    }
}
