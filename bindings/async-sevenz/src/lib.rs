mod ffi;

use crate::ffi::{ReaderRequest, ReaderResponse, RustReaderContext};
use std::ffi::{c_void, CStr};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};
use tokio::sync::mpsc;
use tokio::task;

unsafe impl Send for ffi::SevenZArchive {}
unsafe impl Sync for ffi::SevenZArchive {}

#[derive(Debug, Clone)]
pub struct ArchiveEntry {
    pub name: String,
    pub size: u64,
    pub is_dir: bool,
    pub index: u32,
}

pub struct SevenZ {
    archive: *mut ffi::SevenZArchive,
    // Context must be kept alive as long as archive uses it
    _context: Box<RustReaderContext>,
}

unsafe impl Send for SevenZ {}
unsafe impl Sync for SevenZ {}

impl SevenZ {
    pub async fn open<R>(mut reader: R) -> Result<Self, Box<dyn std::error::Error + Send + Sync>>
    where
        R: AsyncRead + AsyncSeek + Unpin + Send + 'static,
    {
        let (tx, mut rx) =
            mpsc::unbounded_channel::<(ReaderRequest, std::sync::mpsc::Sender<ReaderResponse>)>();

        // Spawn the driver task
        task::spawn(async move {
            while let Some((req, resp_tx)) = rx.recv().await {
                match req {
                    ReaderRequest::Read(size) => {
                        let mut buf = vec![0u8; size as usize];
                        let result = reader.read(&mut buf).await;
                        match result {
                            Ok(n) => {
                                buf.truncate(n);
                                let _ = resp_tx.send(ReaderResponse::Read(buf));
                            }
                            Err(_) => {
                                let _ = resp_tx.send(ReaderResponse::Error);
                            }
                        }
                    }
                    ReaderRequest::Seek(offset, origin) => {
                        let seek_from = match origin {
                            0 => std::io::SeekFrom::Start(offset as u64),
                            1 => std::io::SeekFrom::Current(offset),
                            2 => std::io::SeekFrom::End(offset),
                            _ => {
                                let _ = resp_tx.send(ReaderResponse::Error);
                                continue;
                            }
                        };
                        match reader.seek(seek_from).await {
                            Ok(pos) => {
                                let _ = resp_tx.send(ReaderResponse::Seek(pos));
                            }
                            Err(_) => {
                                let _ = resp_tx.send(ReaderResponse::Error);
                            }
                        }
                    }
                }
            }
        });

        // Create context
        let mut context = Box::new(RustReaderContext { tx });
        let ctx_ptr = &mut *context as *mut RustReaderContext as *mut c_void;
        // Cast to autocxx::c_void pointer type (which wraps system c_void)
        // Since we can't easily construct autocxx::c_void, we assume the FFI function takes *mut autocxx::c_void
        // and we can cast our *mut c_void to it if the layout is compatible.
        // autocxx::c_void is transparently a c_void.
        let ffi_ctx_addr = ctx_ptr as usize;

        // Call OpenArchive in blocking task
        // We cast the result to usize to make it Send-safe to pass back from spawn_blocking
        let archive_addr = task::spawn_blocking(move || unsafe {
            let ptr = ffi::OpenArchive(ffi_ctx_addr as *mut autocxx::c_void);
            ptr as usize
        })
        .await?;

        let archive = archive_addr as *mut ffi::SevenZArchive;

        if archive.is_null() {
            return Err("Failed to open archive".into());
        }

        Ok(Self {
            archive,
            _context: context,
        })
    }

    pub async fn entries(
        &self,
    ) -> Result<Vec<ArchiveEntry>, Box<dyn std::error::Error + Send + Sync>> {
        let archive = self.archive;
        let archive_addr = archive as usize;

        task::spawn_blocking(move || unsafe {
            let archive = archive_addr as *mut ffi::SevenZArchive;
            let count = ffi::GetArchiveItemCount(archive).0;
            let mut entries = Vec::with_capacity(count as usize);

            for i in 0..count {
                let name_ptr = ffi::GetArchiveItemName(archive, autocxx::c_uint(i));
                let name = if !name_ptr.is_null() {
                    let c_str = CStr::from_ptr(name_ptr);
                    let s = c_str.to_string_lossy().to_string();
                    ffi::FreeString(name_ptr);
                    s
                } else {
                    format!("{}", i)
                };

                let is_dir =
                    ffi::IsArchiveItemFolder(archive, autocxx::c_uint(i)) != autocxx::c_int(0);
                let size = ffi::GetArchiveItemSize(archive, autocxx::c_uint(i)).0;

                entries.push(ArchiveEntry {
                    name,
                    size,
                    is_dir,
                    index: i,
                });
            }
            Ok(entries)
        })
        .await?
    }

    pub async fn get_entry_index(
        &self, name: &str,
    ) -> Result<Option<u32>, Box<dyn std::error::Error + Send + Sync>> {
        let archive = self.archive;
        let archive_addr = archive as usize;
        let name_c = std::ffi::CString::new(name)?;

        task::spawn_blocking(move || unsafe {
            let archive = archive_addr as *mut ffi::SevenZArchive;
            let idx = ffi::GetArchiveItemIndex(archive, name_c.as_ptr());
            let idx_val = idx.0;
            if idx_val >= 0 {
                Ok(Some(idx_val as u32))
            } else {
                Ok(None)
            }
        })
        .await?
    }

    /// Get the uncompressed size of an entry by its index
    pub async fn get_entry_size(
        &self, index: u32,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let archive = self.archive;
        let archive_addr = archive as usize;

        task::spawn_blocking(move || unsafe {
            let archive = archive_addr as *mut ffi::SevenZArchive;
            let size = ffi::GetArchiveItemSize(archive, autocxx::c_uint(index));
            Ok(size.0)
        })
        .await?
    }
}

// Structs must be outside impl blocks
struct RustWriterContext {
    writer: Box<dyn std::io::Write + Send>,
}

#[no_mangle]
/// Writes a buffer supplied by the native 7-Zip callback into the Rust writer.
///
/// # Safety
///
/// `ctx` must point to a live [`RustWriterContext`], `buf` must be readable for
/// `size` bytes, and a non-null `processed` pointer must be writable.
pub unsafe extern "C" fn rust_write_cb(
    ctx: *mut c_void, buf: *const c_void, size: autocxx::c_uint, processed: *mut autocxx::c_uint,
) -> autocxx::c_int {
    unsafe {
        let ctx = &mut *(ctx as *mut RustWriterContext);
        let buf_slice = std::slice::from_raw_parts(buf as *const u8, size.0 as usize);
        match ctx.writer.write_all(buf_slice) {
            Ok(_) => {
                if !processed.is_null() {
                    *processed = size;
                }
                autocxx::c_int(0)
            }
            Err(_) => autocxx::c_int(1),
        }
    }
}

impl SevenZ {
    pub async fn extract<W>(
        &self, index: u32, writer: W,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        W: std::io::Write + Send + 'static,
    {
        let archive = self.archive;
        // Since archive is *mut, we cast to usize to pass to blocking task
        let archive_addr = archive as usize;

        task::spawn_blocking(move || unsafe {
            let mut context = RustWriterContext {
                writer: Box::new(writer),
            };
            let callback_ctx = &mut context as *mut RustWriterContext as *mut autocxx::c_void;

            let res = ffi::ExtractEntry(
                archive_addr as *mut ffi::SevenZArchive,
                autocxx::c_uint(index),
                callback_ctx,
            );

            if res != autocxx::c_int(0) {
                Err(format!("Extraction failed with code {:?}", res).into())
            } else {
                Ok(())
            }
        })
        .await?
    }
}

impl Drop for SevenZ {
    fn drop(&mut self) {
        let archive = self.archive;
        if !archive.is_null() {
            unsafe {
                ffi::CloseArchive(archive);
            }
        }
    }
}
