use anyhow::{Result, anyhow};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_native_tls::{TlsConnector, TlsStream};

#[allow(dead_code)]
pub struct NntpClient {
    stream: BufReader<Box<dyn Connection + Send + Unpin>>, // Dynamic dispatch to handle Plain vs TLS or Enum
                                                           // OR use an enum to avoid dyn overhead, but dyn is easier to write initially
}

// Helper trait to unify TcpStream and TlsStream
#[allow(dead_code)]
pub trait Connection: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin> Connection for T {}

enum StreamType {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

// To use BufReader we need strictly AsyncRead + AsyncWrite on the enum or box.
// Let's implement AsyncRead/Write for StreamType wrapper to make things clean.

use std::pin::Pin;
use std::task::{Context, Poll};

struct NntpStream {
    inner: StreamType,
}

impl tokio::io::AsyncRead for NntpStream {
    fn poll_read(
        mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match &mut self.inner {
            StreamType::Plain(s) => Pin::new(s).poll_read(cx, buf),
            StreamType::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl tokio::io::AsyncWrite for NntpStream {
    fn poll_write(
        mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match &mut self.inner {
            StreamType::Plain(s) => Pin::new(s).poll_write(cx, buf),
            StreamType::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut self.inner {
            StreamType::Plain(s) => Pin::new(s).poll_flush(cx),
            StreamType::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut self.inner {
            StreamType::Plain(s) => Pin::new(s).poll_shutdown(cx),
            StreamType::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

pub struct Client {
    stream: BufReader<NntpStream>,
}

impl Client {
    pub async fn connect(host: &str, port: u16, ssl: bool) -> Result<Self> {
        let addr = format!("{}:{}", host, port);
        tracing::debug!("Connecting to NNTP server at {}", addr);

        let tcp = TcpStream::connect(&addr).await?;

        let stream = if ssl {
            let connector = native_tls::TlsConnector::builder().build()?;
            let connector = TlsConnector::from(connector);
            // Verify domain? usually yes, but legacy app might not strict check.
            // Using host as domain.
            let tls_stream = connector.connect(host, tcp).await?;
            NntpStream {
                inner: StreamType::Tls(Box::new(tls_stream)),
            }
        } else {
            NntpStream {
                inner: StreamType::Plain(tcp),
            }
        };

        let mut client = Client {
            stream: BufReader::new(stream),
        };

        // Read initial greeting
        // 200 Service available, posting allowed
        // 201 Service available, posting prohibited
        let response = client.read_response().await?;
        if !response.starts_with("200") && !response.starts_with("201") {
            return Err(anyhow!("Unexpected NNTP greeting: {}", response));
        }

        Ok(client)
    }

    pub async fn authenticate(&mut self, user: &str, pass: &str) -> Result<()> {
        self.send_command(&format!("AUTHINFO USER {}", user))
            .await?;
        let response = self.read_response().await?;

        if response.starts_with("381") {
            // Password required
            self.send_command(&format!("AUTHINFO PASS {}", pass))
                .await?;
            let response = self.read_response().await?;
            if !response.starts_with("281") {
                return Err(anyhow!("NNTP Authentication failed: {}", response));
            }
        } else if !response.starts_with("281") {
            return Err(anyhow!(
                "NNTP Authentication failed (User stage): {}",
                response
            ));
        }

        Ok(())
    }

    // Select group isn't strictly necessary if using message-id, but good practice?
    // "Most servers do not require the GROUP command to be used if the article is being retrieved by message-id"
    // We will implement body fetch by Message-ID.

    pub async fn fetch_body(&mut self, message_id: &str) -> Result<Vec<u8>> {
        // Encapsulate ID in <> if not present
        let msg_id = if message_id.starts_with('<') {
            message_id.to_string()
        } else {
            format!("<{}>", message_id)
        };

        self.send_command(&format!("BODY {}", msg_id)).await?;
        let response = self.read_response().await?;

        if !response.starts_with("222") {
            return Err(anyhow!("Failed to fetch body for {}: {}", msg_id, response));
        }

        // Read until .\r\n
        let mut body = Vec::new();
        loop {
            let mut line = String::new();
            let bytes_read = self.stream.read_line(&mut line).await?;
            if bytes_read == 0 {
                break; // EOF
            }

            if line == ".\r\n" {
                break;
            }

            // Dot-unstuffing: If line starts with .., remove first dot
            if line.starts_with("..") {
                body.extend_from_slice(&line.as_bytes()[1..]);
            } else {
                body.extend_from_slice(line.as_bytes());
            }
        }

        Ok(body)
    }

    async fn send_command(&mut self, cmd: &str) -> Result<()> {
        self.stream.write_all(cmd.as_bytes()).await?;
        self.stream.write_all(b"\r\n").await?;
        self.stream.flush().await?;
        Ok(())
    }

    async fn read_response(&mut self) -> Result<String> {
        let mut line = String::new();
        self.stream.read_line(&mut line).await?;
        // pop \r\n
        Ok(line.trim_end().to_string())
    }
}
