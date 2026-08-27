use super::parser::NzbFile;
use super::session::NzbSession;
use bytes::Bytes;
use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

pub struct NzbFileStream {
    session: Arc<NzbSession>,
    file: NzbFile,
    current_segment_idx: usize,
    buffer: Bytes, // Decoded data waiting to be read
    fetching: bool,
    // We might need a future to store the pending fetch
    // But since we are in `poll_read`, we need to be careful with async calls.
    // The idiomatic way is to use a State enum or explicit polling of a future.
    // Using `tokio_util::io::ReaderStream` on a stream of chunks might be easier?
    // Let's stick to AsyncRead but maybe loop simpler.
    // actually, let's use a channel or just spawn the fetches ahead?
    // For simplicity: One segment at a time.
    fetch_future: Option<tokio::task::JoinHandle<Result<Vec<u8>>>>,
}

impl NzbFileStream {
    pub fn new(session: Arc<NzbSession>, file: NzbFile) -> Self {
        // Sort segments by number just in case
        let mut file = file;
        file.segments.segments.sort_by_key(|s| s.number);

        Self {
            session,
            file,
            current_segment_idx: 0,
            buffer: Bytes::new(),
            fetching: false,
            fetch_future: None,
        }
    }
}

impl AsyncRead for NzbFileStream {
    fn poll_read(
        mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        loop {
            // 1. If we have buffered data, return it
            if !self.buffer.is_empty() {
                let len = std::cmp::min(buf.remaining(), self.buffer.len());
                buf.put_slice(&self.buffer[..len]);
                self.buffer = self.buffer.slice(len..);
                return Poll::Ready(Ok(()));
            }

            // 2. If no buffer and no more segments, EOF
            if self.current_segment_idx >= self.file.segments.segments.len()
                && self.fetch_future.is_none()
            {
                return Poll::Ready(Ok(()));
            }

            // 3. If we are fetching, poll the future
            if let Some(fut) = &mut self.fetch_future {
                return match Pin::new(fut).poll(cx) {
                    Poll::Ready(result) => {
                        self.fetch_future = None;
                        self.fetching = false;

                        match result {
                            Ok(Ok(raw_body)) => {
                                // Decode yEnc
                                // We'll process raw_body here.
                                // Simple yEnc decoder:
                                // Skip header (=ybegin), decode chars, handle =yend
                                // Using the `yenc` crate: `yenc::decode_buffer`?
                                // If crate is not straightforward, we do simplistic decode for now.
                                // Note: `yenc` crate on crates.io is minimal.
                                // Let's try `yenc::decode`.

                                // Parse raw_body for yEnc
                                let decoded = match decode_yenc(&raw_body) {
                                    Ok(d) => d,
                                    Err(e) => {
                                        return Poll::Ready(Err(Error::new(
                                            ErrorKind::InvalidData,
                                            e.to_string(),
                                        )));
                                    }
                                };

                                self.buffer = Bytes::from(decoded);
                                self.current_segment_idx += 1;
                                continue; // Loop back to write to buf
                            }
                            Ok(Err(e)) => Poll::Ready(Err(Error::other(e.to_string()))),
                            Err(e) => Poll::Ready(Err(Error::other(e.to_string()))), // JoinError
                        }
                    }
                    Poll::Pending => Poll::Pending,
                };
            }

            // 4. Start fetching next segment
            if self.current_segment_idx < self.file.segments.segments.len() {
                let segment = self.file.segments.segments[self.current_segment_idx].clone();
                let session = self.session.clone();
                let fut = tokio::spawn(async move {
                    session
                        .fetch_segment(&segment.id)
                        .await
                        .map_err(|e| std::io::Error::other(e.to_string()))
                });
                self.fetch_future = Some(fut);
                self.fetching = true;
                continue;
            }
        }
    }
}

// Minimal yEnc decoder to avoid complex crate dependency issues if `yenc` crate is weird.
// Legacy JS uses `yenc` module.
fn decode_yenc(input: &[u8]) -> anyhow::Result<Vec<u8>> {
    // Find =ybegin
    // Find =ypart (optional)
    // Decode data
    // Find =yend

    // Naive implementation for MVP
    let start_marker = b"=ybegin";
    let part_marker = b"=ypart";

    // Find start
    let start_idx = input
        .windows(start_marker.len())
        .position(|w| w == start_marker)
        .unwrap_or(0); // If no header, maybe raw? But NNTP usually has header.

    // Find data start (newline after header(s))
    let mut data_start = start_idx;
    // Skip line
    if let Some(pos) = input[start_idx..].iter().position(|&b| b == b'\n') {
        data_start += pos + 1;
    }

    // Check for =ypart
    if input[data_start..].starts_with(part_marker)
        && let Some(pos) = input[data_start..].iter().position(|&b| b == b'\n')
    {
        data_start += pos + 1;
    }

    let mut output = Vec::with_capacity(input.len());
    let mut i = data_start;

    while i < input.len() {
        let b = input[i];

        // Check for end
        if b == b'=' && input[i..].starts_with(b"=yend") {
            break;
        }

        if b == b'=' {
            // Escape next
            i += 1;
            if i >= input.len() {
                break;
            }
            let escaped = input[i];
            output.push((escaped.wrapping_sub(64)).wrapping_sub(42));
        } else if b == b'\r' || b == b'\n' {
            // Ignore newlines in body? yEnc usually ignores them, but they might mean end of line.
            // "The CR/LF pairs at the end of each line are not part of the data"
        } else {
            output.push(b.wrapping_sub(42));
        }
        i += 1;
    }

    Ok(output)
}
