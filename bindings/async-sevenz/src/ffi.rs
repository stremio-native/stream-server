#![allow(clippy::module_inception)]

use autocxx::prelude::*;
use std::ffi::c_void;
use std::sync::mpsc as std_mpsc;
use tokio::sync::mpsc;

include_cpp! {
    #include "cpp/wrappers_api.h"

    // Safety policy
    safety!(unsafe)

    // Allow types
    generate!("SevenZArchive")
    generate!("OpenArchive")
    generate!("CloseArchive")
    generate!("ExtractEntry")
    generate!("GetArchiveItemCount")
    generate!("GetArchiveItemName")
    generate!("FreeString")
    generate!("IsArchiveItemFolder")
    generate!("GetArchiveItemSize")
    generate!("GetArchiveItemIndex")
}

// Re-export generated items so they are directly in crate::ffi
pub use ffi::*;

pub enum ReaderRequest {
    Read(u32),
    Seek(i64, u32),
}

pub enum ReaderResponse {
    Read(Vec<u8>),
    Seek(u64),
    Error,
}

#[repr(C)]
pub struct RustReaderContext {
    // Channel to send requests to the async driver
    pub tx: mpsc::UnboundedSender<(ReaderRequest, std_mpsc::Sender<ReaderResponse>)>,
}

#[no_mangle]
pub unsafe extern "C" fn rust_read_cb(
    ctx: *mut c_void, buf: *mut c_void, size: u32, processed: *mut u32,
) -> i32 {
    let ctx = unsafe { &*(ctx as *mut RustReaderContext) };
    let (resp_tx, resp_rx) = std_mpsc::channel();

    if ctx.tx.send((ReaderRequest::Read(size), resp_tx)).is_err() {
        return -1; // E_FAIL
    }

    match resp_rx.recv() {
        Ok(ReaderResponse::Read(data)) => {
            unsafe {
                let bytes_to_copy = std::cmp::min(size as usize, data.len());
                std::ptr::copy_nonoverlapping(data.as_ptr(), buf as *mut u8, bytes_to_copy);
                *processed = bytes_to_copy as u32;
            }
            0 // S_OK
        }
        _ => -1, // Error
    }
}

#[no_mangle]
pub unsafe extern "C" fn rust_seek_cb(
    ctx: *mut c_void, offset: i64, origin: u32, new_pos: *mut u64,
) -> i32 {
    let ctx = unsafe { &*(ctx as *mut RustReaderContext) };
    let (resp_tx, resp_rx) = std_mpsc::channel();

    if ctx
        .tx
        .send((ReaderRequest::Seek(offset, origin), resp_tx))
        .is_err()
    {
        return -1;
    }

    match resp_rx.recv() {
        Ok(ReaderResponse::Seek(pos)) => {
            unsafe {
                *new_pos = pos;
            }
            0
        }
        _ => -1,
    }
}
