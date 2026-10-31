//! Exercise observer completion through the production reply pump and transport.
use super::*;
use crate::raw::reply::{own_reply_buffer, ReplyInit};
use crate::raw::RequestObserver;
use std::os::unix::net::UnixDatagram;
use std::sync::Mutex;
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

#[derive(Debug, Default)]
struct Observer {
    submitted: Mutex<Vec<(u64, i32)>>,
}

impl RequestObserver for Observer {
    fn request_reply_submitted_result(&self, unique: u64, error: i32) {
        self.submitted.lock().unwrap().push((unique, error));
    }
}

fn packet(unique: u64, error: i32, payload: &[u8]) -> Vec<u8> {
    let mut packet = Vec::new();
    packet.extend_from_slice(&((FUSE_OUT_HEADER_SIZE + payload.len()) as u32).to_le_bytes());
    packet.extend_from_slice(&error.to_le_bytes());
    packet.extend_from_slice(&unique.to_le_bytes());
    packet.extend_from_slice(payload);
    packet
}

async fn submit(response: FuseData, expected_packet: &[u8]) -> Vec<(u64, i32)> {
    let (transport, peer) = UnixDatagram::pair().unwrap();
    peer.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    let connection = Arc::new(FuseConnection::from_test_fd(transport.into()));
    let observer = Arc::new(Observer::default());
    let (sender, receiver) = unbounded();
    sender.unbounded_send(response).unwrap();
    drop(sender);
    tokio::time::timeout(
        Duration::from_secs(1),
        Session::<TestFilesystem>::reply_fuse_with_observer(
            connection,
            receiver,
            None,
            Some(observer.clone()),
        ),
    )
    .await
    .expect("reply pump did not finish")
    .unwrap();
    let mut received = [0; 1024];
    let size = peer.recv(&mut received).unwrap();
    assert_eq!(&received[..size], expected_packet);
    let submitted = observer.submitted.lock().unwrap().clone();
    submitted
}

#[tokio::test]
async fn guarded_metadata_reply_reports_submission() {
    let packet = packet(41, 0, &[0; FUSE_OPEN_OUT_SIZE]);
    let response = Either::Right((
        Vec::new(),
        own_reply_buffer(packet.clone(), Some(Arc::new(()))),
    ));
    assert_eq!(submit(response, &packet).await, vec![(41, 0)]);
}

#[tokio::test]
async fn guarded_close_error_reply_reports_submission_and_errno() {
    let packet = packet(42, -libc::EIO, &[]);
    let response = Either::Right((
        Vec::new(),
        own_reply_buffer(packet.clone(), Some(Arc::new(()))),
    ));
    assert_eq!(submit(response, &packet).await, vec![(42, -libc::EIO)]);
}

#[tokio::test]
async fn read_reply_reports_one_submission_for_each_header_layout() {
    let packet = packet(43, 0, b"read payload");
    for split in [8, FUSE_OUT_HEADER_SIZE, packet.len()] {
        let response = Either::Right((packet[..split].to_vec(), packet[split..].to_vec().into()));
        assert_eq!(submit(response, &packet).await, vec![(43, 0)]);
    }
    assert_eq!(
        submit(Either::Left(packet.clone()), &packet).await,
        vec![(43, 0)]
    );
}
