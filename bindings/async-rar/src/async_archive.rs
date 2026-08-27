//! Async RAR archive operations
//!
//! Provides async wrappers using tokio::spawn_blocking for non-blocking operations.

use std::path::Path;
use tokio::sync::mpsc;
use tokio::task;

use crate::archive::{OpenMode, RarArchive, RarEntry};
use crate::error::{RarError, Result};

/// Async handle to a RAR archive
///
/// This type wraps the synchronous `RarArchive` and runs all blocking
/// operations on Tokio's blocking thread pool.
pub struct AsyncRarArchive {
    inner: Option<RarArchive>,
}

impl AsyncRarArchive {
    /// Open a RAR archive asynchronously
    ///
    /// # Example
    /// ```no_run
    /// use async_rar::AsyncRarArchive;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let archive = AsyncRarArchive::open("test.rar").await.unwrap();
    /// }
    /// ```
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_owned();
        let archive =
            task::spawn_blocking(move || RarArchive::open(&path, OpenMode::Extract)).await??;

        Ok(Self {
            inner: Some(archive),
        })
    }

    /// Open a RAR archive for listing only
    pub async fn open_for_list(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_owned();
        let archive =
            task::spawn_blocking(move || RarArchive::open(&path, OpenMode::List)).await??;

        Ok(Self {
            inner: Some(archive),
        })
    }

    /// Set password for encrypted archives
    pub async fn set_password(&mut self, password: String) -> Result<()> {
        let mut archive = self.take_inner()?;

        let (archive, result) = task::spawn_blocking(move || {
            let result = archive.set_password(&password);
            (archive, result)
        })
        .await?;

        self.inner = Some(archive);
        result
    }

    /// List all entries in the archive
    pub async fn list_entries(&mut self) -> Result<Vec<RarEntry>> {
        let mut archive = self.take_inner()?;

        let (archive, entries) = task::spawn_blocking(move || {
            let entries = archive.list_entries();
            // Re-open for further operations since listing consumes the iterator
            (archive, entries)
        })
        .await?;

        self.inner = Some(archive);
        entries
    }

    /// Extract all files to a destination directory
    pub async fn extract_all(&mut self, dest: impl AsRef<Path>) -> Result<()> {
        let mut archive = self.take_inner()?;
        let dest = dest.as_ref().to_owned();

        let (archive, result) = task::spawn_blocking(move || {
            let result = archive.extract_all(&dest);
            (archive, result)
        })
        .await?;

        self.inner = Some(archive);
        result
    }

    /// Get the path to the archive
    pub fn path(&self) -> Option<&str> {
        self.inner.as_ref().map(|a| a.path())
    }

    /// Take the inner archive, returning an error if already taken
    fn take_inner(&mut self) -> Result<RarArchive> {
        self.inner.take().ok_or(RarError::ArchiveClosed)
    }

    /// Stream an entry's data
    pub async fn stream_entry(&mut self, entry_name: String) -> Result<mpsc::Receiver<Vec<u8>>> {
        let mut archive = self.take_inner()?;
        let (tx, rx) = mpsc::channel(32);

        // Spawn blocking task that drives the extraction
        // The sender needs to be able to block, but we are using tokio::sync::mpsc::Sender which is async.
        // But the callback is synchronous.
        // We need bridging.
        // Since we are inside spawn_blocking, we can use blocking_send if using tokio's channel,
        // OR better yet, pass a callback that does blocking send on the channel.

        task::spawn_blocking(move || {
            let _result =
                archive.stream_entry(&entry_name, |data| tx.blocking_send(data.to_vec()).is_ok());
            // Loop finishes when done or error
            // We can drop the archive or re-use it?
            // The original API design consumes 'inner' for each async op to ensure safety.
            // We can't return the archive back easily unless we change the return type.
            // For now, this consumes the archive handle for this operation.
            // (Or we could return it via channel if needed, but simple stream is goal)
            // Actually, ignoring the archive return means it is dropped and closed.
            // This is fine for one-off streaming.
        });

        Ok(rx)
    }
}

/// List entries in a RAR archive without keeping it open
pub async fn list_entries(path: impl AsRef<Path>) -> Result<Vec<RarEntry>> {
    let path = path.as_ref().to_owned();
    task::spawn_blocking(move || {
        let mut archive = RarArchive::open(&path, OpenMode::List)?;
        archive.list_entries()
    })
    .await?
}

/// Extract all files from a RAR archive to a destination
pub async fn extract_all(archive_path: impl AsRef<Path>, dest: impl AsRef<Path>) -> Result<()> {
    let archive_path = archive_path.as_ref().to_owned();
    let dest = dest.as_ref().to_owned();

    task::spawn_blocking(move || {
        let mut archive = RarArchive::open(&archive_path, OpenMode::Extract)?;
        archive.extract_all(&dest)
    })
    .await?
}

/// Extract all files from a password-protected RAR archive
pub async fn extract_all_with_password(
    archive_path: impl AsRef<Path>, dest: impl AsRef<Path>, password: &str,
) -> Result<()> {
    let archive_path = archive_path.as_ref().to_owned();
    let dest = dest.as_ref().to_owned();
    let password = password.to_string();

    task::spawn_blocking(move || {
        let mut archive = RarArchive::open(&archive_path, OpenMode::Extract)?;
        archive.set_password(&password)?;
        archive.extract_all(&dest)
    })
    .await?
}
