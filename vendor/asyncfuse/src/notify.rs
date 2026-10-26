//! notify kernel.

use std::ffi::{OsStr, OsString};
use std::io;
use std::os::unix::ffi::OsStrExt;

use bincode::Options;
use bytes::{Buf, Bytes};
use futures_channel::mpsc::UnboundedSender;
use futures_channel::oneshot;
use futures_util::future::Either;

use crate::helper::get_bincode_config;
use crate::raw::abi::{
    fuse_notify_code, fuse_notify_delete_out, fuse_notify_inval_entry_out,
    fuse_notify_inval_inode_out, fuse_notify_poll_wakeup_out, fuse_notify_retrieve_out,
    fuse_notify_store_out, fuse_out_header, FUSE_NOTIFY_DELETE_OUT_SIZE,
    FUSE_NOTIFY_INVAL_ENTRY_OUT_SIZE, FUSE_NOTIFY_INVAL_INODE_OUT_SIZE,
    FUSE_NOTIFY_POLL_WAKEUP_OUT_SIZE, FUSE_NOTIFY_RETRIEVE_OUT_SIZE, FUSE_NOTIFY_STORE_OUT_SIZE,
    FUSE_OUT_HEADER_SIZE,
};
use crate::raw::FuseData;

#[derive(Debug, Clone)]
/// notify kernel there are something need to handle.
pub struct Notify {
    sender: UnboundedSender<FuseData>,
    completion_sender: UnboundedSender<Notification>,
}

#[derive(Debug)]
pub(crate) struct Notification {
    pub(crate) data: FuseData,
    pub(crate) completion: oneshot::Sender<io::Result<()>>,
}

impl Notify {
    pub(crate) fn new(
        sender: UnboundedSender<FuseData>,
        completion_sender: UnboundedSender<Notification>,
    ) -> Self {
        Self {
            sender,
            completion_sender,
        }
    }

    fn enqueue(
        &self,
        kind: NotifyKind,
        completion: Option<oneshot::Sender<io::Result<()>>>,
    ) -> io::Result<()> {
        let data = match &kind {
            NotifyKind::Wakeup { kh } => {
                let out_header = fuse_out_header {
                    len: (FUSE_OUT_HEADER_SIZE + FUSE_NOTIFY_POLL_WAKEUP_OUT_SIZE) as u32,
                    error: fuse_notify_code::FUSE_POLL as i32,
                    unique: 0,
                };

                let wakeup_out = fuse_notify_poll_wakeup_out { kh: *kh };

                let mut data =
                    Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_NOTIFY_POLL_WAKEUP_OUT_SIZE);

                get_bincode_config()
                    .serialize_into(&mut data, &out_header)
                    .expect("vec size is not enough");
                get_bincode_config()
                    .serialize_into(&mut data, &wakeup_out)
                    .expect("vec size is not enough");

                Either::Left(data)
            }

            NotifyKind::InvalidInode { inode, offset, len } => {
                let out_header = fuse_out_header {
                    len: (FUSE_OUT_HEADER_SIZE + FUSE_NOTIFY_INVAL_INODE_OUT_SIZE) as u32,
                    error: fuse_notify_code::FUSE_NOTIFY_INVAL_INODE as i32,
                    unique: 0,
                };

                let invalid_inode_out = fuse_notify_inval_inode_out {
                    ino: *inode,
                    off: *offset,
                    len: *len,
                };

                let mut data =
                    Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_NOTIFY_INVAL_INODE_OUT_SIZE);

                get_bincode_config()
                    .serialize_into(&mut data, &out_header)
                    .expect("vec size is not enough");
                get_bincode_config()
                    .serialize_into(&mut data, &invalid_inode_out)
                    .expect("vec size is not enough");

                Either::Left(data)
            }

            NotifyKind::InvalidEntry { parent, name } => {
                let name = name.as_bytes();
                if name.is_empty()
                    || name.len() > libc::NAME_MAX as usize
                    || name.contains(&0)
                    || name.contains(&b'/')
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "invalidation name must be one nonempty component without NUL, at most NAME_MAX bytes",
                    ));
                }
                // Linux requires namelen + 1 bytes on the wire; namelen excludes
                // the trailing NUL. Include both in the out-header length.
                let out_header = fuse_out_header {
                    len: (FUSE_OUT_HEADER_SIZE + FUSE_NOTIFY_INVAL_ENTRY_OUT_SIZE + name.len() + 1)
                        as u32,
                    error: fuse_notify_code::FUSE_NOTIFY_INVAL_ENTRY as i32,
                    unique: 0,
                };

                let invalid_entry_out = fuse_notify_inval_entry_out {
                    parent: *parent,
                    namelen: name.len() as _,
                    _padding: 0,
                };

                let mut data = Vec::with_capacity(out_header.len as usize);

                get_bincode_config()
                    .serialize_into(&mut data, &out_header)
                    .expect("vec size is not enough");
                get_bincode_config()
                    .serialize_into(&mut data, &invalid_entry_out)
                    .expect("vec size is not enough");

                data.extend_from_slice(name);
                data.push(0);
                Either::Left(data)
            }

            NotifyKind::Delete {
                parent,
                child,
                name,
            } => {
                let out_header = fuse_out_header {
                    len: (FUSE_OUT_HEADER_SIZE + FUSE_NOTIFY_DELETE_OUT_SIZE) as u32,
                    error: fuse_notify_code::FUSE_NOTIFY_DELETE as i32,
                    unique: 0,
                };

                let delete_out = fuse_notify_delete_out {
                    parent: *parent,
                    child: *child,
                    namelen: name.len() as _,
                    _padding: 0,
                };

                let mut data =
                    Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_NOTIFY_DELETE_OUT_SIZE);

                get_bincode_config()
                    .serialize_into(&mut data, &out_header)
                    .expect("vec size is not enough");
                get_bincode_config()
                    .serialize_into(&mut data, &delete_out)
                    .expect("vec size is not enough");

                // TODO should I add null at the end?

                Either::Right((data, Bytes::copy_from_slice(name.as_bytes()).into()))
            }

            NotifyKind::Store {
                inode,
                offset,
                data,
            } => {
                let out_header = fuse_out_header {
                    len: (FUSE_OUT_HEADER_SIZE + FUSE_NOTIFY_STORE_OUT_SIZE) as u32,
                    error: fuse_notify_code::FUSE_NOTIFY_STORE as i32,
                    unique: 0,
                };

                let store_out = fuse_notify_store_out {
                    nodeid: *inode,
                    offset: *offset,
                    size: data.len() as _,
                    _padding: 0,
                };

                let mut data_buf =
                    Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_NOTIFY_STORE_OUT_SIZE);

                get_bincode_config()
                    .serialize_into(&mut data_buf, &out_header)
                    .expect("vec size is not enough");
                get_bincode_config()
                    .serialize_into(&mut data_buf, &store_out)
                    .expect("vec size is not enough");

                Either::Right((data_buf, data.clone().into()))
            }

            NotifyKind::Retrieve {
                notify_unique,
                inode,
                offset,
                size,
            } => {
                let out_header = fuse_out_header {
                    len: (FUSE_OUT_HEADER_SIZE + FUSE_NOTIFY_RETRIEVE_OUT_SIZE) as u32,
                    error: fuse_notify_code::FUSE_NOTIFY_RETRIEVE as i32,
                    unique: 0,
                };

                let retrieve_out = fuse_notify_retrieve_out {
                    notify_unique: *notify_unique,
                    nodeid: *inode,
                    offset: *offset,
                    size: *size,
                    _padding: 0,
                };

                let mut data =
                    Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_NOTIFY_RETRIEVE_OUT_SIZE);

                get_bincode_config()
                    .serialize_into(&mut data, &out_header)
                    .expect("vec size is not enough");
                get_bincode_config()
                    .serialize_into(&mut data, &retrieve_out)
                    .expect("vec size is not enough");

                Either::Left(data)
            }
        };

        if let Some(completion) = completion {
            self.completion_sender
                .unbounded_send(Notification { data, completion })
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::BrokenPipe, "notification writer stopped")
                })
        } else {
            // Preserve legacy enqueue timing and FIFO ordering with replies.
            self.sender.unbounded_send(data).map_err(|_| {
                io::Error::new(io::ErrorKind::BrokenPipe, "notification writer stopped")
            })
        }
    }

    fn notify(&self, kind: NotifyKind) {
        if let Err(error) = self.enqueue(kind, None) {
            tracing::warn!(%error, "could not enqueue FUSE notification");
        }
    }

    async fn notify_and_wait(&self, kind: NotifyKind) -> io::Result<()> {
        let (completion, result) = oneshot::channel();
        self.enqueue(kind, Some(completion))?;
        result.await.map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "notification writer stopped before completion",
            )
        })?
    }

    /// try to notify kernel the IO is ready, kernel can wakeup the waiting program.
    pub async fn wakeup(self, kh: u64) {
        self.notify(NotifyKind::Wakeup { kh });
    }

    /// Enqueue inode invalidation, logging enqueue or device-write failures.
    /// Retains ordering on the existing request-reply queue. Use
    /// [`Self::try_invalid_inode`] to await the kernel's result.
    pub async fn invalid_inode(self, inode: u64, offset: i64, len: i64) {
        self.notify(NotifyKind::InvalidInode { inode, offset, len });
    }

    /// Invalidate inode attributes and the specified data range, awaiting the
    /// complete `/dev/fuse` write. Returns the kernel errno on rejection (including
    /// `ENOENT` for an inode absent from its cache), or `BrokenPipe` if the writer
    /// stops before acknowledgement. Offset/length semantics match
    /// [`Self::invalid_inode`].
    ///
    /// Issue invalidations outside related filesystem request handlers and
    /// without holding locks they need: the kernel may wait for those requests.
    /// This uses a separate writer and is not ordered with request replies.
    /// Dropping this future after enqueue does not cancel the invalidation.
    pub async fn try_invalid_inode(&self, inode: u64, offset: i64, len: i64) -> io::Result<()> {
        self.notify_and_wait(NotifyKind::InvalidInode { inode, offset, len })
            .await
    }

    /// Enqueue entry invalidation, logging enqueue or device-write failures.
    /// Retains ordering on the existing request-reply queue. Use
    /// [`Self::try_invalid_entry`] to await the kernel's result.
    pub async fn invalid_entry(self, parent: u64, name: OsString) {
        self.notify(NotifyKind::InvalidEntry { parent, name });
    }

    /// Invalidate a cached directory entry, awaiting the complete `/dev/fuse`
    /// write. `name` is a single component; non-UTF-8 names are supported. Empty,
    /// oversized, slash-containing or NUL-containing names return `InvalidInput`.
    /// The wire name includes a trailing NUL, excluded from the ABI's `namelen`.
    ///
    /// Success means the kernel accepted the invalidation, not merely that it
    /// was queued. Kernel errors (including `ENOENT` for an uncached parent or
    /// entry) are returned unchanged. Writer shutdown returns `BrokenPipe`.
    ///
    /// Issue this outside related filesystem request handlers, without holding
    /// locks they need, to avoid deadlock. Dropping this future after enqueue does
    /// not cancel the invalidation. Concurrent lookups can repopulate the cache.
    /// This uses a separate writer and is not ordered with request replies.
    pub async fn try_invalid_entry(&self, parent: u64, name: &OsStr) -> io::Result<()> {
        self.notify_and_wait(NotifyKind::InvalidEntry {
            parent,
            name: name.to_owned(),
        })
        .await
    }

    /// try to notify a directory entry has been deleted.
    pub async fn delete(self, parent: u64, child: u64, name: OsString) {
        self.notify(NotifyKind::Delete {
            parent,
            child,
            name,
        });
    }

    /// try to push the data in an inode for updating the kernel cache.
    pub async fn store(self, inode: u64, offset: u64, mut data: impl Buf) {
        self.notify(NotifyKind::Store {
            inode,
            offset,
            data: data.copy_to_bytes(data.remaining()),
        });
    }

    /// try to retrieve data in an inode from the kernel cache.
    pub async fn retrieve(self, notify_unique: u64, inode: u64, offset: u64, size: u32) {
        self.notify(NotifyKind::Retrieve {
            notify_unique,
            inode,
            offset,
            size,
        });
    }
}

#[derive(Debug)]
/// the kind of notify.
enum NotifyKind {
    /// notify the IO is ready.
    Wakeup { kh: u64 },

    /// notify the cache invalidation about an inode.
    InvalidInode { inode: u64, offset: i64, len: i64 },

    /// notify the invalidation about a directory entry.
    InvalidEntry { parent: u64, name: OsString },

    /// notify a directory entry has been deleted.
    Delete {
        parent: u64,
        child: u64,
        name: OsString,
    },

    /// push the data in an inode for updating the kernel cache.
    Store {
        inode: u64,
        offset: u64,
        data: Bytes,
    },

    /// retrieve data in an inode from the kernel cache.
    Retrieve {
        notify_unique: u64,
        inode: u64,
        offset: u64,
        size: u32,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_channel::mpsc::{unbounded, UnboundedReceiver};
    use futures_util::{FutureExt, StreamExt};

    fn take_notification(receiver: &mut UnboundedReceiver<Notification>) -> Notification {
        receiver.next().now_or_never().unwrap().unwrap()
    }

    fn notify_with_completion(sender: UnboundedSender<Notification>) -> Notify {
        Notify::new(unbounded().0, sender)
    }

    fn packet(notification: &Notification) -> &[u8] {
        match &notification.data {
            Either::Left(data) => data,
            Either::Right(_) => panic!("invalidation must be a complete packet"),
        }
    }

    #[test]
    fn entry_wire_format_and_completion_wait_for_write_acknowledgement() {
        let (sender, mut receiver) = unbounded();
        let notify = notify_with_completion(sender);
        let name = OsStr::from_bytes(b"cached-\xff");
        let pending = notify.try_invalid_entry(42, name);
        futures_util::pin_mut!(pending);
        assert!(pending.as_mut().now_or_never().is_none());
        let notification = take_notification(&mut receiver);
        let bytes = packet(&notification);
        let header: (u32, i32, u64) = get_bincode_config().deserialize(&bytes[..16]).unwrap();
        let entry: (u64, u32, u32) = get_bincode_config().deserialize(&bytes[16..32]).unwrap();
        assert_eq!(header, (bytes.len() as u32, 3, 0));
        assert_eq!(entry, (42, name.len() as u32, 0));
        assert_eq!(&bytes[32..], b"cached-\xff\0");
        // Dequeue alone is not an acknowledgement.
        assert!(pending.as_mut().now_or_never().is_none());
        notification.completion.send(Ok(())).unwrap();
        pending.now_or_never().unwrap().unwrap();
    }

    #[test]
    fn inode_wire_format_is_unchanged_and_kernel_errno_is_preserved() {
        let (sender, mut receiver) = unbounded();
        let notify = notify_with_completion(sender);
        let pending = notify.try_invalid_inode(17, -1, 0);
        futures_util::pin_mut!(pending);
        assert!(pending.as_mut().now_or_never().is_none());
        let notification = take_notification(&mut receiver);
        let bytes = packet(&notification);
        let header: (u32, i32, u64) = get_bincode_config().deserialize(&bytes[..16]).unwrap();
        let inode: (u64, i64, i64) = get_bincode_config().deserialize(&bytes[16..]).unwrap();
        assert_eq!(header, (40, 2, 0));
        assert_eq!(inode, (17, -1, 0));
        notification
            .completion
            .send(Err(io::Error::from_raw_os_error(libc::ENOENT)))
            .unwrap();
        assert_eq!(
            pending.now_or_never().unwrap().unwrap_err().raw_os_error(),
            Some(libc::ENOENT)
        );
    }

    #[test]
    fn stopped_writer_and_dropped_packet_are_observable() {
        let (sender, mut receiver) = unbounded();
        let notify = notify_with_completion(sender);
        let pending = notify.try_invalid_entry(1, OsStr::new("entry"));
        futures_util::pin_mut!(pending);
        assert!(pending.as_mut().now_or_never().is_none());
        drop(take_notification(&mut receiver));
        assert_eq!(
            pending.now_or_never().unwrap().unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        drop(receiver);
        assert_eq!(
            notify
                .try_invalid_inode(2, 0, 0)
                .now_or_never()
                .unwrap()
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            notify
                .try_invalid_entry(1, OsStr::new("entry"))
                .now_or_never()
                .unwrap()
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
    }

    #[test]
    fn invalid_names_are_rejected_before_enqueue() {
        let (sender, mut receiver) = unbounded();
        let notify = notify_with_completion(sender);
        let oversized = vec![b'x'; libc::NAME_MAX as usize + 1];
        for name in [b"".as_slice(), b"a/b", b"a\0b", oversized.as_slice()] {
            assert_eq!(
                notify
                    .try_invalid_entry(1, OsStr::from_bytes(name))
                    .now_or_never()
                    .unwrap()
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
            assert!(receiver.next().now_or_never().is_none());
        }
    }

    #[test]
    fn legacy_methods_keep_enqueue_only_completion_and_reply_queue_ordering() {
        let (sender, mut receiver) = unbounded();
        let (completion_sender, mut completion_receiver) = unbounded();
        let notify = Notify::new(sender.clone(), completion_sender);
        sender.unbounded_send(Either::Left(vec![1])).unwrap();
        assert_eq!(
            notify.clone().invalid_inode(2, 0, -1).now_or_never(),
            Some(())
        );
        sender.unbounded_send(Either::Left(vec![2])).unwrap();
        assert_eq!(
            notify
                .invalid_entry(1, OsString::from("entry"))
                .now_or_never(),
            Some(())
        );
        let packets: Vec<_> = (0..4)
            .map(|_| match receiver.next().now_or_never().unwrap().unwrap() {
                Either::Left(data) => data,
                Either::Right(_) => panic!("unexpected segmented packet"),
            })
            .collect();
        assert_eq!(packets[0], vec![1]);
        assert_eq!(packets[1][4..8], 2_i32.to_le_bytes());
        assert_eq!(packets[2], vec![2]);
        assert_eq!(packets[3][4..8], 3_i32.to_le_bytes());
        // No legacy messages were moved to the independent completion queue.
        assert!(completion_receiver
            .next()
            .now_or_never()
            .is_some_and(|message| message.is_none()));
    }
}
