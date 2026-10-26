//! Socket transport tests for the production notification writer. The mounted
//! example separately verifies that Linux actually invalidates its dentry cache.
use super::*;
use crate::raw::reply::ReplyInit;
use futures_util::FutureExt;
use std::os::unix::net::UnixDatagram;
use std::time::Duration;

struct TestFilesystem;

impl Filesystem for TestFilesystem {
    async fn init(&self, _: Request) -> crate::Result<ReplyInit> {
        Ok(ReplyInit::default())
    }

    async fn destroy(&self, _: Request) {}

    #[cfg(feature = "file-lock")]
    async fn getlk(
        &self,
        _: Request,
        _: crate::Inode,
        _: u64,
        _: u64,
        _: u64,
        _: u64,
        _: u32,
        _: u32,
    ) -> crate::Result<crate::raw::reply::ReplyLock> {
        Err(libc::ENOSYS.into())
    }

    #[cfg(feature = "file-lock")]
    async fn setlk(
        &self,
        _: Request,
        _: crate::Inode,
        _: u64,
        _: u64,
        _: u64,
        _: u64,
        _: u32,
        _: u32,
        _: bool,
    ) -> crate::Result<()> {
        Err(libc::ENOSYS.into())
    }
}

fn connection() -> (Arc<FuseConnection>, UnixDatagram) {
    let (transport, peer) = UnixDatagram::pair().unwrap();
    nix::sys::socket::setsockopt(&transport, nix::sys::socket::sockopt::SndBuf, &4096).unwrap();
    peer.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    (
        Arc::new(FuseConnection::from_test_fd(transport.into())),
        peer,
    )
}

#[tokio::test]
async fn completion_waits_for_the_transport_write() {
    let (connection, peer) = connection();
    let (sender, receiver) = unbounded();
    let notify = Notify::new(unbounded().0, sender);
    let mut invalidation = Box::pin(notify.try_invalid_inode(42, -1, 0));
    assert!(invalidation.as_mut().now_or_never().is_none());

    let writer = tokio::spawn(Session::<TestFilesystem>::notify_fuse(connection, receiver));
    tokio::time::timeout(Duration::from_secs(1), invalidation)
        .await
        .expect("notification never acknowledged its write")
        .unwrap();

    let mut packet = [0u8; 128];
    let written = peer.recv(&mut packet).unwrap();
    assert_eq!(
        written,
        FUSE_OUT_HEADER_SIZE + FUSE_NOTIFY_INVAL_INODE_OUT_SIZE
    );
    assert_eq!(
        u32::from_le_bytes(packet[..4].try_into().unwrap()) as usize,
        written
    );
    assert_eq!(i32::from_le_bytes(packet[4..8].try_into().unwrap()), 2);
    assert_eq!(u64::from_le_bytes(packet[8..16].try_into().unwrap()), 0);
    assert_eq!(u64::from_le_bytes(packet[16..24].try_into().unwrap()), 42);
    assert_eq!(i64::from_le_bytes(packet[24..32].try_into().unwrap()), -1);
    assert_eq!(i64::from_le_bytes(packet[32..40].try_into().unwrap()), 0);
    drop(notify);
    tokio::time::timeout(Duration::from_secs(1), writer)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn terminal_write_error_is_reported_and_does_not_block_later_notifications() {
    let (connection, peer) = connection();
    let (sender, receiver) = unbounded();
    let writer = tokio::spawn(Session::<TestFilesystem>::notify_fuse(connection, receiver));
    let (completion, result) = oneshot::channel();
    // A datagram larger than the socket send buffer deterministically fails
    // writev with EMSGSIZE. It must not enter the reply writer's retry loop.
    sender
        .unbounded_send(Notification {
            data: Either::Left(vec![0; 1024 * 1024]),
            completion,
        })
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(1), result)
        .await
        .expect("terminal write error was retried indefinitely")
        .unwrap()
        .unwrap_err();
    assert_eq!(error.raw_os_error(), Some(libc::EMSGSIZE));

    let notify = Notify::new(unbounded().0, sender);
    tokio::time::timeout(Duration::from_secs(1), notify.try_invalid_inode(2, 0, 0))
        .await
        .expect("failed notification blocked the following one")
        .unwrap();
    let mut packet = [0; 128];
    assert_eq!(peer.recv(&mut packet).unwrap(), 40);
    drop(notify);
    tokio::time::timeout(Duration::from_secs(1), writer)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn canceled_completion_does_not_stop_the_writer() {
    let (connection, peer) = connection();
    let (sender, receiver) = unbounded();
    let notify = Notify::new(unbounded().0, sender);
    let mut canceled = Box::pin(notify.try_invalid_inode(2, 0, 0));
    assert!(canceled.as_mut().now_or_never().is_none());
    drop(canceled);
    let writer = tokio::spawn(Session::<TestFilesystem>::notify_fuse(connection, receiver));
    tokio::time::timeout(Duration::from_secs(1), notify.try_invalid_inode(3, 0, 0))
        .await
        .unwrap()
        .unwrap();
    let mut packet = [0; 128];
    assert_eq!(peer.recv(&mut packet).unwrap(), 40);
    assert_eq!(u64::from_le_bytes(packet[16..24].try_into().unwrap()), 2);
    assert_eq!(peer.recv(&mut packet).unwrap(), 40);
    assert_eq!(u64::from_le_bytes(packet[16..24].try_into().unwrap()), 3);
    drop(notify);
    tokio::time::timeout(Duration::from_secs(1), writer)
        .await
        .unwrap()
        .unwrap();
}
