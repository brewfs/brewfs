//! Candidate only: real wire parsing, INIT, dispatch admission and workers.
//! Existing tokio from_test_fd uses a Unix datagram socket, not /dev/fuse.
use super::*;
use crate::raw::reply::{ReplyData, ReplyInit};
use futures_util::FutureExt;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

const CONTROL_CAPACITY: usize = 32 * 1024;
const READ_UNIQUE: u64 = 10;
const NODEID: u64 = 7;
const DEADLINE: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CloseKind {
    Flush,
    Release,
    Releasedir,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CloseCall {
    kind: CloseKind,
    unique: u64,
    inode: u64,
    fh: u64,
    flags: u32,
    lock_owner: u64,
    flush: bool,
}

#[derive(Debug, Default)]
struct CloseFs {
    read_cancel: Mutex<Option<oneshot::Sender<()>>>,
    entered: AtomicUsize,
    live: AtomicUsize,
    calls: Mutex<Vec<CloseCall>>,
    errors: BTreeMap<u64, i32>,
    ordinary_reservations: AtomicUsize,
    control_used: Arc<AtomicUsize>,
    control_peak: AtomicUsize,
    control_rejected: AtomicUsize,
    control_charges: Mutex<Vec<usize>>,
}

struct ReadLifetime<'a>(&'a CloseFs);
impl Drop for ReadLifetime<'_> {
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Debug)]
struct ControlOwner {
    used: Arc<AtomicUsize>,
    bytes: usize,
}
impl Drop for ControlOwner {
    fn drop(&mut self) {
        self.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

impl CloseFs {
    fn record(&self, call: CloseCall) -> crate::Result<()> {
        let error = self.errors.get(&call.unique).copied();
        self.calls.lock().unwrap().push(call);
        match error {
            Some(error) => Err(error.into()),
            None => Ok(()),
        }
    }
}

impl Filesystem for CloseFs {
    async fn init(&self, _: Request) -> crate::Result<ReplyInit> {
        Ok(ReplyInit::default())
    }
    async fn destroy(&self, _: Request) {
        let sender = self.read_cancel.lock().unwrap().take();
        if let Some(sender) = sender {
            let _ = sender.send(());
        }
        while self.live.load(Ordering::Acquire) != 0 {
            tokio::task::yield_now().await;
        }
    }
    fn supports_read_cancellation(&self) -> bool {
        true
    }
    fn reserve_request_memory(
        &self,
        _: u64,
    ) -> crate::Result<Option<crate::raw::reply::ReplyMemoryGuard>> {
        self.ordinary_reservations.fetch_add(1, Ordering::AcqRel);
        Ok(Some(Arc::new(())))
    }
    fn reserve_control_memory(
        &self,
        bytes: u64,
    ) -> crate::Result<Option<crate::raw::reply::ReplyMemoryGuard>> {
        let bytes = usize::try_from(bytes).map_err(|_| crate::Errno::from(libc::ENOMEM))?;
        let previous = self
            .control_used
            .try_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
                    .filter(|total| *total <= CONTROL_CAPACITY)
            })
            .map_err(|_| {
                self.control_rejected.fetch_add(1, Ordering::AcqRel);
                crate::Errno::from(libc::ENOMEM)
            })?;
        self.control_peak
            .fetch_max(previous + bytes, Ordering::AcqRel);
        self.control_charges.lock().unwrap().push(bytes);
        Ok(Some(Arc::new(ControlOwner {
            used: self.control_used.clone(),
            bytes,
        })))
    }
    async fn read(
        &self,
        _: Request,
        _: crate::Inode,
        _: u64,
        _: u64,
        _: u32,
    ) -> crate::Result<ReplyData> {
        let (sender, receiver) = oneshot::channel();
        assert!(self.read_cancel.lock().unwrap().replace(sender).is_none());
        self.live.fetch_add(1, Ordering::AcqRel);
        let _lifetime = ReadLifetime(self);
        self.entered.fetch_add(1, Ordering::Release);
        let _ = receiver.await;
        Err(libc::EINTR.into())
    }
    async fn flush(
        &self,
        req: Request,
        inode: crate::Inode,
        fh: u64,
        lock_owner: u64,
    ) -> crate::Result<()> {
        self.record(CloseCall {
            kind: CloseKind::Flush,
            unique: req.unique,
            inode,
            fh,
            flags: 0,
            lock_owner,
            flush: false,
        })
    }
    async fn release(
        &self,
        req: Request,
        inode: crate::Inode,
        fh: u64,
        flags: u32,
        lock_owner: u64,
        flush: bool,
    ) -> crate::Result<()> {
        self.record(CloseCall {
            kind: CloseKind::Release,
            unique: req.unique,
            inode,
            fh,
            flags,
            lock_owner,
            flush,
        })
    }
    async fn releasedir(
        &self,
        req: Request,
        inode: crate::Inode,
        fh: u64,
        flags: u32,
    ) -> crate::Result<()> {
        self.record(CloseCall {
            kind: CloseKind::Releasedir,
            unique: req.unique,
            inode,
            fh,
            flags,
            lock_owner: 0,
            flush: false,
        })
    }
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

fn wire_request(opcode: fuse_opcode, unique: u64, body: &[u8]) -> Vec<u8> {
    let mut wire = Vec::with_capacity(FUSE_IN_HEADER_SIZE + body.len());
    wire.extend_from_slice(&((FUSE_IN_HEADER_SIZE + body.len()) as u32).to_le_bytes());
    wire.extend_from_slice(&(opcode as u32).to_le_bytes());
    wire.extend_from_slice(&unique.to_le_bytes());
    wire.extend_from_slice(&NODEID.to_le_bytes());
    for word in [1000u32, 1001, 42, 0] {
        wire.extend_from_slice(&word.to_le_bytes());
    }
    assert_eq!(wire.len(), FUSE_IN_HEADER_SIZE);
    wire.extend_from_slice(body);
    wire
}

fn close_wire(call: &CloseCall) -> Vec<u8> {
    let mut body = Vec::with_capacity(24);
    body.extend_from_slice(&call.fh.to_le_bytes());
    let opcode = match call.kind {
        CloseKind::Flush => {
            body.extend_from_slice(&0u32.to_le_bytes());
            body.extend_from_slice(&0u32.to_le_bytes());
            body.extend_from_slice(&call.lock_owner.to_le_bytes());
            fuse_opcode::FUSE_FLUSH
        }
        CloseKind::Release | CloseKind::Releasedir => {
            body.extend_from_slice(&call.flags.to_le_bytes());
            let release_flags = if call.flush { FUSE_RELEASE_FLUSH } else { 0 };
            body.extend_from_slice(&release_flags.to_le_bytes());
            body.extend_from_slice(&call.lock_owner.to_le_bytes());
            if call.kind == CloseKind::Release {
                fuse_opcode::FUSE_RELEASE
            } else {
                fuse_opcode::FUSE_RELEASEDIR
            }
        }
    };
    assert_eq!(body.len(), 24);
    wire_request(opcode, call.unique, &body)
}

fn reply_header(packet: &FuseData) -> (u64, i32) {
    let header = match packet {
        Either::Left(data) => data.as_slice(),
        Either::Right((header, body)) if header.is_empty() => body.as_ref(),
        Either::Right((header, _)) => header.as_slice(),
    };
    assert!(header.len() >= FUSE_OUT_HEADER_SIZE);
    (
        u64::from_le_bytes(header[8..16].try_into().unwrap()),
        i32::from_le_bytes(header[4..8].try_into().unwrap()),
    )
}

struct WireSession {
    peer: tokio::net::UnixDatagram,
    replies: UnboundedReceiver<FuseData>,
    task: JoinHandle<(IoResult<()>, Session<CloseFs>)>,
    inflight: Arc<AtomicUsize>,
}

impl WireSession {
    async fn send(&self, wire: &[u8]) {
        assert_eq!(self.peer.send(wire).await.unwrap(), wire.len());
    }
    async fn packet(&mut self) -> FuseData {
        tokio::time::timeout(DEADLINE, self.replies.next())
            .await
            .expect("real dispatch/worker did not publish a response")
            .expect("response channel ended before its expected reply")
    }
    async fn stop(mut self) -> IoResult<()> {
        self.send(&wire_request(fuse_opcode::FUSE_DESTROY, 9000, &[]))
            .await;
        let (result, session) = tokio::time::timeout(DEADLINE, &mut self.task)
            .await
            .expect("real dispatch/worker teardown stalled")
            .unwrap();
        drop(session);
        while let Some(Some(packet)) = self.replies.next().now_or_never() {
            drop(packet);
        }
        result
    }
}

async fn full_ordinary_session(fs: Arc<CloseFs>) -> WireSession {
    let (transport, peer) = std::os::unix::net::UnixDatagram::pair().unwrap();
    peer.set_nonblocking(true).unwrap();
    let peer = tokio::net::UnixDatagram::from_std(peer).unwrap();
    let mut session = Session::new(MountOptions::default()).with_workers(1, 1);
    session.filesystem = Some(fs.clone());
    session.fuse_connection = Some(Arc::new(FuseConnection::from_test_fd(transport.into())));
    let replies = session.response_receivers[0].take().unwrap();
    let inflight = session.inflight.clone();
    let task = tokio::spawn(async move {
        let result = session.dispatch(None).await;
        (result, session)
    });
    let fixture = WireSession {
        peer,
        replies,
        task,
        inflight,
    };
    let init: Vec<u8> = [FUSE_KERNEL_VERSION, FUSE_KERNEL_MINOR_VERSION, 4096, 0]
        .into_iter()
        .flat_map(u32::to_le_bytes)
        .collect();
    fixture
        .send(&wire_request(fuse_opcode::FUSE_INIT, 1, &init))
        .await;
    let mut init_reply = vec![0; FUSE_OUT_HEADER_SIZE + FUSE_INIT_OUT_SIZE];
    let read = tokio::time::timeout(DEADLINE, fixture.peer.recv(&mut init_reply))
        .await
        .expect("real INIT handshake stalled")
        .unwrap();
    assert_eq!(read, init_reply.len());
    assert_eq!(i32::from_le_bytes(init_reply[4..8].try_into().unwrap()), 0);
    let mut body = vec![0; std::mem::size_of::<fuse_read_in>()];
    body[..8].copy_from_slice(&41u64.to_le_bytes());
    body[16..20].copy_from_slice(&4096u32.to_le_bytes());
    fixture
        .send(&wire_request(fuse_opcode::FUSE_READ, READ_UNIQUE, &body))
        .await;
    tokio::time::timeout(DEADLINE, async {
        while fs.entered.load(Ordering::Acquire) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the wire READ did not occupy real ordinary admission");
    assert_eq!(fs.live.load(Ordering::Acquire), 1);
    assert_eq!(fixture.inflight.load(Ordering::Acquire), 1);
    fixture
}

#[tokio::test(flavor = "current_thread")]
async fn full_admission_close_opcodes_preserve_zero_and_nonzero_handles_flags_and_typed_errors() {
    let expected = vec![
        CloseCall {
            kind: CloseKind::Flush,
            unique: 101,
            inode: NODEID,
            fh: 0,
            flags: 0,
            lock_owner: 11,
            flush: false,
        },
        CloseCall {
            kind: CloseKind::Flush,
            unique: 102,
            inode: NODEID,
            fh: 17,
            flags: 0,
            lock_owner: 12,
            flush: false,
        },
        CloseCall {
            kind: CloseKind::Release,
            unique: 103,
            inode: NODEID,
            fh: 0,
            flags: libc::O_RDONLY as u32,
            lock_owner: 13,
            flush: false,
        },
        CloseCall {
            kind: CloseKind::Release,
            unique: 104,
            inode: NODEID,
            fh: 0,
            flags: libc::O_RDONLY as u32,
            lock_owner: 14,
            flush: true,
        },
        CloseCall {
            kind: CloseKind::Release,
            unique: 105,
            inode: NODEID,
            fh: 19,
            flags: libc::O_RDWR as u32,
            lock_owner: 15,
            flush: false,
        },
        CloseCall {
            kind: CloseKind::Release,
            unique: 106,
            inode: NODEID,
            fh: 19,
            flags: libc::O_RDONLY as u32,
            lock_owner: 16,
            flush: true,
        },
        CloseCall {
            kind: CloseKind::Releasedir,
            unique: 107,
            inode: NODEID,
            fh: 0,
            flags: libc::O_RDONLY as u32,
            lock_owner: 0,
            flush: false,
        },
        CloseCall {
            kind: CloseKind::Releasedir,
            unique: 108,
            inode: NODEID,
            fh: 23,
            flags: libc::O_RDONLY as u32,
            lock_owner: 0,
            flush: false,
        },
    ];
    let errors = BTreeMap::from([(102, libc::EIO), (105, libc::EBADF), (108, libc::EACCES)]);
    let fs = Arc::new(CloseFs {
        errors,
        ..CloseFs::default()
    });
    let mut fixture = full_ordinary_session(fs.clone()).await;
    let mut packets = Vec::new();
    let mut responses = Vec::new();
    let mut still_live = Vec::new();
    for call in &expected {
        fixture.send(&close_wire(call)).await;
        let packet = fixture.packet().await;
        responses.push(reply_header(&packet));
        packets.push(packet); // retain every response owner across the next request
        still_live.push(fs.live.load(Ordering::Acquire));
    }
    let used_while_held = fs.control_used.load(Ordering::Acquire);
    let expected_responses: Vec<_> = expected
        .iter()
        .map(|call| {
            (
                call.unique,
                -fs.errors.get(&call.unique).copied().unwrap_or(0),
            )
        })
        .collect();
    fixture
        .stop()
        .await
        .expect("ordinary DESTROY/worker cleanup failed");
    drop(packets);
    // The intended RED is the response/call mismatch, after complete cleanup.
    assert_eq!(fs.live.load(Ordering::Acquire), 0);
    assert_eq!(fs.control_used.load(Ordering::Acquire), 0);
    assert_eq!(still_live, vec![1; expected.len()]);
    assert_eq!(
        responses, expected_responses,
        "close opcodes were rejected by ordinary admission"
    );
    assert_eq!(*fs.calls.lock().unwrap(), expected);
    assert_eq!(
        fs.ordinary_reservations.load(Ordering::Acquire),
        1,
        "close requests must not consume the large ordinary READ request reservation"
    );
    assert!(used_while_held > 0 && used_while_held <= CONTROL_CAPACITY);
    assert!(fs.control_peak.load(Ordering::Acquire) <= CONTROL_CAPACITY);
    assert_eq!(fs.control_rejected.load(Ordering::Acquire), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn close_replies_at_full_admission_are_bounded_by_32k_control_and_reject_before_side_effects()
{
    let fs = Arc::new(CloseFs::default());
    let mut fixture = full_ordinary_session(fs.clone()).await;
    let mut packets = Vec::new();
    let mut responses = Vec::new();
    let mut terminal = None;
    let mut rejected_unique = None;
    // At least one KiB per retained close/control unit is the candidate design
    // floor. 33 attempts expose any uncharged exemption with a 32 KiB pool.
    for index in 0..=CONTROL_CAPACITY / 1024 {
        let call = CloseCall {
            kind: [CloseKind::Flush, CloseKind::Release, CloseKind::Releasedir][index % 3],
            unique: 1000 + index as u64,
            inode: NODEID,
            fh: 31,
            flags: libc::O_RDONLY as u32,
            lock_owner: if index % 3 == 2 { 0 } else { 77 },
            flush: index % 3 == 1,
        };
        fixture.send(&close_wire(&call)).await;
        enum Next {
            Reply(Option<FuseData>),
            Done(Result<(IoResult<()>, Session<CloseFs>), tokio::task::JoinError>),
        }
        loop {
            let event = tokio::time::timeout(DEADLINE, async {
                tokio::select! {
                    packet = fixture.replies.next() => Next::Reply(packet),
                    result = &mut fixture.task => Next::Done(result),
                }
            })
            .await
            .expect("control exhaustion did not reply or terminate the real dispatch");
            match event {
                Next::Reply(Some(packet)) => {
                    let header = reply_header(&packet);
                    if header.0 == READ_UNIQUE {
                        drop(packet);
                        continue;
                    }
                    assert_eq!(
                        header.0, call.unique,
                        "unexpected duplicate/out-of-order close reply"
                    );
                    responses.push(header);
                    packets.push(packet);
                    break;
                }
                Next::Reply(None) => {
                    panic!("response channel closed without a dispatch terminal result")
                }
                Next::Done(result) => {
                    let (result, session) = result.expect("dispatch task panicked");
                    terminal = Some(result);
                    rejected_unique = Some(call.unique);
                    drop(session);
                    break;
                }
            }
        }
        if terminal.is_some() {
            break;
        }
    }
    let used_while_held = fs.control_used.load(Ordering::Acquire);
    let last = packets
        .pop()
        .expect("fixture received no owned close reply");
    let last_clone = last.clone();
    drop(last);
    packets.push(last_clone);
    let used_after_one_reference_drop = fs.control_used.load(Ordering::Acquire);
    if terminal.is_none() {
        fixture
            .stop()
            .await
            .expect("no-terminal fixture cleanup failed");
    } else {
        while let Some(Some(packet)) = fixture.replies.next().now_or_never() {
            drop(packet);
        }
    }
    drop(packets);
    let calls = fs.calls.lock().unwrap().clone();
    let terminal_errno = terminal
        .and_then(|result| result.err())
        .and_then(|error| error.raw_os_error());
    assert_eq!(fs.live.load(Ordering::Acquire), 0);
    assert_eq!(fs.control_used.load(Ordering::Acquire), 0);
    assert!(used_while_held > 0 && used_while_held <= CONTROL_CAPACITY);
    assert_eq!(used_after_one_reference_drop, used_while_held);
    assert!(fs.control_peak.load(Ordering::Acquire) <= CONTROL_CAPACITY);
    assert!(fs
        .control_charges
        .lock()
        .unwrap()
        .iter()
        .all(|bytes| *bytes >= 1024));
    assert_eq!(
        terminal_errno,
        Some(libc::ENOMEM),
        "control exhaustion must fail closed with typed ENOMEM"
    );
    assert_eq!(fs.control_rejected.load(Ordering::Acquire), 1);
    assert_eq!(fs.ordinary_reservations.load(Ordering::Acquire), 1);
    assert!(
        responses.iter().all(|(_, error)| *error == 0),
        "admitted close requests must run their adapters at full ordinary admission"
    );
    assert_eq!(calls.len(), responses.len());
    assert!(!calls.is_empty());
    assert!(
        rejected_unique.is_some_and(|unique| calls.iter().all(|call| call.unique != unique)),
        "exhausted Control admitted a close side effect before rejecting"
    );
}
