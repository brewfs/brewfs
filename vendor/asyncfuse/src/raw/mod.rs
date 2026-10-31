//! inode based
//!
//! it is not recommend to use this inode based [`Filesystem`] first, you need to handle inode
//! allocate, recycle and sometimes map to the path, [`PathFilesystem`][crate::path::PathFilesystem]
//! helps you do those jobs so you can pay more attention to your filesystem design. However if you
//! want to control the inode or do the path<->inode map on yourself, [`Filesystem`] is the only one
//! choose.

pub use filesystem::Filesystem;
use futures_util::future::Either;
pub use object_safe_filesystem::{DirectoryPlusStream, DirectoryStream, ObjectSafeFilesystem};
pub use request::Request;

/// Optional request lifecycle observer. Implementations must be non-blocking
/// and must not perform filesystem or reply I/O from a callback. The default
/// session has no observer, so callbacks add no state to normal mounts.
pub trait RequestObserver: std::fmt::Debug + Send + Sync + 'static {
    /// Install diagnostic context only for one poll. Implementations must
    /// restore it on return/unwind and invoke `poll` exactly once.
    fn with_request_context(&self, _unique: u64, poll: &mut dyn FnMut()) {
        poll();
    }
    fn request_received(&self, _unique: u64, _opcode: u32) {}
    fn request_received_on_inode(&self, unique: u64, opcode: u32, _inode: u64) {
        self.request_received(unique, opcode);
    }
    fn request_worker_started(&self, _unique: u64) {}
    fn request_handler_started(&self, _unique: u64) {}
    fn request_handler_completed(&self, _unique: u64) {}
    fn request_reply_submitted(&self, _unique: u64) {}
    fn request_reply_submitted_result(&self, unique: u64, _error: i32) {
        self.request_reply_submitted(unique);
    }
    fn request_completed_without_reply(&self, _unique: u64) {}
    fn request_cancelled(&self, _unique: u64) {}
    fn request_failed(&self, _unique: u64) {}
}
#[cfg(any(
    feature = "async-io-runtime",
    feature = "tokio-runtime",
    feature = "io-uring-runtime"
))]
pub use session::{MountHandle, Session};

pub(crate) type FuseData = Either<Vec<u8>, (Vec<u8>, reply::ReplyBytes)>;

pub(crate) mod abi;
pub(crate) mod buffer_pool;
mod connection;
mod filesystem;
pub mod flags;
pub mod logfs;
mod object_safe_filesystem;
pub mod reply;
mod request;
pub(crate) mod session;

pub mod prelude {
    pub use super::reply::FileAttr;
    pub use super::reply::*;
    pub use super::Filesystem;
    pub use super::Request;
    pub use super::Session;
    pub use super::{DirectoryPlusStream, DirectoryStream};
    pub use crate::notify::Notify;
    pub use crate::FileType;
    pub use crate::SetAttr;
}
