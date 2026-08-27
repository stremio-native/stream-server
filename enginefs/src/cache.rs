use crate::backend::FileStreamTrait;
use moka::future::Cache;
use std::future::Future;
use std::io::{self, SeekFrom};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};
use tokio::sync::Mutex;

const CHUNK_SIZE: u64 = 512 * 1024; // 512KB chunks

pub type DataCache = Cache<(usize, u64), Arc<Vec<u8>>>;
type ReadFuture = Pin<Box<dyn Future<Output = io::Result<Arc<Vec<u8>>>> + Send>>;

struct SyncFuture(ReadFuture);
// SAFETY: We only access the future via &mut self, so no concurrent access is possible.
// The future itself is Send, which is sufficient if we don't share it.
unsafe impl Sync for SyncFuture {}

pub struct CachedStream {
    inner: Arc<Mutex<Box<dyn FileStreamTrait>>>,
    cache: DataCache,
    file_index: usize,
    position: u64,
    file_length: u64,
    current_future: Option<SyncFuture>,
}

impl CachedStream {
    pub fn new(
        inner: Box<dyn FileStreamTrait>, cache: DataCache, file_index: usize, file_length: u64,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
            cache,
            file_index,
            position: 0,
            file_length,
            current_future: None,
        }
    }

    fn prefetch(&self, chunk_index: u64) {
        // Prefetch next 2 chunks
        for i in 1..=2 {
            let next_chunk = chunk_index + i;
            if next_chunk * CHUNK_SIZE >= self.file_length {
                break;
            }

            let key = (self.file_index, next_chunk);
            if self.cache.contains_key(&key) {
                continue;
            }

            let cache = self.cache.clone();
            let inner = self.inner.clone();
            let chunk_start = next_chunk * CHUNK_SIZE;

            // Spawn background task to populate cache
            tokio::spawn(async move {
                // We use try_get_with just to populate. We don't care about the result here.
                // Explicitly type the future's return to help inference
                let _ = cache
                    .try_get_with(key, async move {
                        let mut stream = inner.lock().await;
                        use tokio::io::AsyncReadExt;
                        use tokio::io::AsyncSeekExt;

                        stream.seek(SeekFrom::Start(chunk_start)).await?;

                        let mut buffer = Vec::with_capacity(CHUNK_SIZE as usize);
                        let mut take = (&mut *stream).take(CHUNK_SIZE);
                        take.read_to_end(&mut buffer).await?;

                        let res: Result<Arc<Vec<u8>>, std::io::Error> =
                            Ok(Arc::<Vec<u8>>::new(buffer));
                        res
                    })
                    .await;
            });
        }
    }
}

impl AsyncSeek for CachedStream {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        let new_pos = match position {
            SeekFrom::Start(pos) => pos,
            SeekFrom::Current(diff) => (self.position as i64 + diff) as u64,
            SeekFrom::End(diff) => (self.file_length as i64 + diff) as u64,
        };
        self.position = new_pos;
        self.current_future = None;
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.position))
    }
}

impl AsyncRead for CachedStream {
    fn poll_read(
        mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            // Check if we are done
            if self.position >= self.file_length {
                return Poll::Ready(Ok(()));
            }

            // Trigger prefetch for next chunks if starting a new chunk
            if self.position.is_multiple_of(CHUNK_SIZE) {
                let chunk_index = self.position / CHUNK_SIZE;
                self.prefetch(chunk_index);
            }

            // If we have a pending future, poll it
            if let Some(wrapper) = self.current_future.as_mut() {
                match wrapper.0.as_mut().poll(cx) {
                    Poll::Ready(result) => {
                        self.current_future = None; // clear future
                        match result {
                            Ok(data) => {
                                // We have the chunk data. Copy valid part to buf.
                                let chunk_index = self.position / CHUNK_SIZE;
                                let chunk_start = chunk_index * CHUNK_SIZE;
                                let chunk_offset = (self.position - chunk_start) as usize;

                                if chunk_offset >= data.len() {
                                    continue;
                                }

                                let available = data.len() - chunk_offset;
                                let to_read = std::cmp::min(available, buf.remaining());
                                buf.put_slice(&data[chunk_offset..chunk_offset + to_read]);
                                self.position += to_read as u64;
                                return Poll::Ready(Ok(()));
                            }
                            Err(e) => return Poll::Ready(Err(e)),
                        }
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            // No future, create one
            let chunk_index = self.position / CHUNK_SIZE;
            let key = (self.file_index, chunk_index);
            let chunk_start = chunk_index * CHUNK_SIZE;

            // Trigger prefetch here too in case we jumped into middle of chunk
            self.prefetch(chunk_index);

            let cache = self.cache.clone();
            let inner = self.inner.clone();

            let fut = async move {
                cache
                    .try_get_with(key, async move {
                        let mut stream = inner.lock().await;

                        // Seek to chunk start by using the inner stream's seek
                        use tokio::io::AsyncReadExt;
                        use tokio::io::AsyncSeekExt;

                        // We need to re-seek deeply because other consumers (or previous reads) might have moved it
                        stream.seek(SeekFrom::Start(chunk_start)).await?;

                        let mut buffer = Vec::with_capacity(CHUNK_SIZE as usize);
                        // Take from mutable reference to avoid moving out of MutexGuard
                        let mut take = (&mut *stream).take(CHUNK_SIZE);
                        take.read_to_end(&mut buffer).await?;

                        let res: Result<Arc<Vec<u8>>, std::sync::Arc<io::Error>> =
                            Ok(Arc::<Vec<u8>>::new(buffer));
                        res
                    })
                    .await
                    .map_err(|e| {
                        // Unwrap the Arc error or create generic io error
                        io::Error::other(e.to_string())
                    })
            };

            self.current_future = Some(SyncFuture(Box::pin(fut)));
            // Loop continue to poll the new future immediately
        }
    }
}
