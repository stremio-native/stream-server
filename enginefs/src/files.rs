use crate::backend::{FileStreamTrait, TorrentHandle};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncSeek};

pub struct FileHandle<H: TorrentHandle> {
    pub size: u64,
    pub name: String,
    pub stream: Box<dyn FileStreamTrait>,
    pub engine: Arc<crate::engine::Engine<H>>,
}

impl<H: TorrentHandle> FileHandle<H> {
    pub fn new(
        size: u64, name: String, stream: Box<dyn FileStreamTrait>,
        engine: Arc<crate::engine::Engine<H>>,
    ) -> Self {
        Self {
            size,
            name,
            stream,
            engine,
        }
    }
}

impl<H: TorrentHandle> AsyncRead for FileHandle<H> {
    fn poll_read(
        mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl<H: TorrentHandle> Drop for FileHandle<H> {
    fn drop(&mut self) {
        self.engine.active_streams.fetch_sub(1, Ordering::SeqCst);
    }
}

impl<H: TorrentHandle> AsyncSeek for FileHandle<H> {
    fn start_seek(mut self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
        Pin::new(&mut self.stream).start_seek(position)
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        Pin::new(&mut self.stream).poll_complete(cx)
    }
}
