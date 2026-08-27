use super::{ArchiveEntry, ArchiveReader, AsyncSeekableReader};
use anyhow::{Result, anyhow};
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncSeek, AsyncSeekExt};

pub struct TarHandler {
    path: PathBuf,
}

impl TarHandler {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

#[async_trait::async_trait]
impl ArchiveReader for TarHandler {
    async fn list_files(&self) -> Result<Vec<ArchiveEntry>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let file = std::fs::File::open(&path)?;
            let mut archive = tar::Archive::new(file);
            let mut entries = Vec::new();

            for file in archive.entries()? {
                let file = file?;
                entries.push(ArchiveEntry {
                    path: file.path()?.to_string_lossy().to_string(),
                    size: file.size(),
                    is_dir: file.header().entry_type().is_dir(),
                });
            }
            Ok(entries)
        })
        .await?
    }

    async fn open_file(&self, path: &str) -> Result<Box<dyn AsyncSeekableReader>> {
        // For TAR, we need to find the offset and size.
        // We can't jump directly without index.
        // Linear scan is needed (unfortunately, typical for TAR).
        // Optimization: Cache index? For now, scan.

        let path_clone = self.path.clone();
        let target_path = path.to_string();

        // Use spawn_blocking to scan because `tar` crate is sync
        let (offset, size) = tokio::task::spawn_blocking(move || -> Result<(u64, u64)> {
            let file = std::fs::File::open(&path_clone)?;
            let mut archive = tar::Archive::new(file);
            for file in archive.entries()? {
                let file = file?;
                if file.path()?.to_string_lossy() == target_path {
                    return Ok((file.raw_file_position(), file.size()));
                }
            }
            Err(anyhow!("File not found in TAR archive"))
        })
        .await??;

        let slice = AsyncRawFileSlice::new(self.path.clone(), offset, size).await?;
        Ok(Box::new(slice))
    }
}

pub struct AsyncRawFileSlice {
    file: File,
    offset: u64,
    size: u64,
    pos: u64,
}

impl AsyncRawFileSlice {
    pub async fn new(path: PathBuf, offset: u64, size: u64) -> Result<Self> {
        let mut file = File::open(path).await?;
        file.seek(tokio::io::SeekFrom::Start(offset)).await?;
        Ok(Self {
            file,
            offset,
            size,
            pos: 0,
        })
    }
}

impl AsyncRead for AsyncRawFileSlice {
    fn poll_read(
        mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.pos >= self.size {
            return Poll::Ready(Ok(()));
        }

        let remaining = (self.size - self.pos) as usize;
        let want = buf.remaining().min(remaining);

        if want == 0 {
            return Poll::Ready(Ok(()));
        }

        // We need to limit the read.
        // ReadBuf doesn't support `take` easily in poll without wrapper.
        // We can assume file won't read past EOF if we seeked correctly?
        // But the underlying file is larger than our slice.
        // So we MUST limit.

        let mut sub_buf = buf.take(want);
        let start_filled = sub_buf.filled().len();

        let poll = Pin::new(&mut self.file).poll_read(cx, &mut sub_buf);

        match poll {
            Poll::Ready(Ok(())) => {
                let n = sub_buf.filled().len() - start_filled;
                self.pos += n as u64;
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl AsyncSeek for AsyncRawFileSlice {
    fn start_seek(mut self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
        let new_pos = match position {
            std::io::SeekFrom::Start(p) => p,
            std::io::SeekFrom::End(p) => {
                if p < 0 {
                    self.size.saturating_sub(p.unsigned_abs())
                } else {
                    self.size + p as u64
                }
            }
            std::io::SeekFrom::Current(p) => {
                let current = self.pos as i64;
                let new_p = current + p;
                if new_p < 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Negative seek",
                    ));
                }
                new_p as u64
            }
        };

        // Check bounds (optional, but good)
        // Set pos.
        // We need to seek the underlying file to offset + new_pos

        // AsyncSeek works with `start_seek` then `poll_complete`.
        // We calculate target absolute position.
        let target = self.offset + new_pos;
        match Pin::new(&mut self.file).start_seek(std::io::SeekFrom::Start(target)) {
            Ok(()) => {
                // If successful, we update local pos?
                // Wait, logic: `start_seek` prepares. `poll_complete` confirms.
                // We shouldn't update `pos` until `poll_complete` returns `Ready`.
                // But `start_seek` requires we calculated `target` from `position`.
                // `File::start_seek` doesn't know about our "virtual" pos if we used `Current` or `End`.
                // EXCEPT we mapped everything to `Start` above!
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        match Pin::new(&mut self.file).poll_complete(cx) {
            Poll::Ready(Ok(actual_abs_pos)) => {
                // actual_abs_pos is relative to physical file start.
                // We map back to slice relative.
                if actual_abs_pos < self.offset {
                    // This creates a weird state, but ok.
                    self.pos = 0;
                } else {
                    self.pos = actual_abs_pos - self.offset;
                }
                Poll::Ready(Ok(self.pos))
            }
            other => other,
        }
    }
}
