//! Mount-local read tokens. This registry never owns kernel reply memory.
use crate::meta::layer::MetadataMemoryGuard;
use asyncfuse::Errno;
use futures_util::future::{AbortHandle, AbortRegistration};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct ReadState {
    requests: BTreeMap<u64, AbortHandle>,
    closed: bool,
    live_registrations: usize,
    clients: Option<Box<ClientLease>>,
    live_clients: usize,
    next_client_generation: u64,
    client_failure: Option<Errno>,
}
type Requests = Arc<Mutex<ReadState>>;

#[derive(Default)]
pub(crate) struct ReadRegistry {
    requests: Requests,
    drained: Arc<tokio::sync::Notify>,
    // Declared last: both initial Arc allocations drop before their final owner.
    _roots: Option<MetadataMemoryGuard>,
}
impl ReadRegistry {
    pub(crate) fn register(
        &self,
        unique: u64,
        owner: Option<MetadataMemoryGuard>,
    ) -> Result<(ReadRegistration, AbortRegistration), Errno> {
        let (handle, registration) = AbortHandle::new_pair();
        let mut state = self.requests.lock().map_err(|_| Errno::from(libc::EIO))?;
        if state.closed {
            return Err(libc::EIO.into());
        }
        if state.requests.contains_key(&unique) {
            return Err(libc::EBUSY.into());
        }
        state.requests.insert(unique, handle.clone());
        state.live_registrations += 1;
        Ok((
            ReadRegistration {
                unique,
                requests: self.requests.clone(),
                drained: self.drained.clone(),
                handle: Some(handle),
                registered: true,
                _owner: owner,
                _roots: self._roots.clone(),
            },
            registration,
        ))
    }

    pub(crate) fn interrupt(&self, unique: u64) -> Result<(), Errno> {
        let state = self.requests.lock().map_err(|_| Errno::from(libc::EIO))?;
        let handle = state
            .requests
            .get(&unique)
            .ok_or(Errno::from(libc::EAGAIN))?;
        // Abort while holding the same lock used by finish so a token cannot
        // be reused while an interrupt targets its previous registration.
        handle.abort();
        Ok(())
    }

    pub(crate) async fn shutdown(&self) {
        {
            let mut state = self
                .requests
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.closed = true;
            for handle in state.requests.values() {
                handle.abort();
            }
        }
        loop {
            let notified = self.drained.notified();
            tokio::pin!(notified);
            // Register before testing emptiness to avoid losing the final drop.
            notified.as_mut().enable();
            if self
                .requests
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .live_registrations
                == 0
            {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(all(test, feature = "workspace"))]
#[path = "fuse_read_cancel/client_fence_tests.rs"]
mod client_fence_tests;

pub(crate) struct ReadRegistration {
    unique: u64,
    requests: Requests,
    drained: Arc<tokio::sync::Notify>,
    handle: Option<AbortHandle>,
    registered: bool,
    _owner: Option<MetadataMemoryGuard>,
    // Declared after requests/drained; preserves the initial Roots allocation
    // until registrations' actual shared-state/Notify Arcs are also dropped.
    _roots: Option<MetadataMemoryGuard>,
}
impl ReadRegistration {
    pub(crate) fn cancellation_errno(&self) -> Errno {
        // A closed mount cannot recover by retrying this syscall. Explicit
        // per-request INTERRUPT remains EINTR while the mount is still open.
        match self.requests.lock() {
            Ok(state) if !state.closed => libc::EINTR.into(),
            _ => libc::EIO.into(),
        }
    }

    pub(crate) fn finish(mut self) -> bool {
        let interrupted = self
            .handle
            .as_ref()
            .is_some_and(|handle| handle.is_aborted());
        self.release();
        interrupted
    }

    fn release(&mut self) {
        if !self.registered {
            return;
        }
        let empty = {
            let mut state = self
                .requests
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.requests.remove(&self.unique);
            self.registered = false;
            state
                .requests
                .is_empty()
                .then(|| std::mem::take(&mut state.requests))
        };
        // A last removal can retain a BTree backing root. It belongs to this
        // READ's existing Control charge, not the initial registry Roots owner.
        drop(empty);
        // Retire token allocations before returning their memory reservation.
        drop(self.handle.take());
        drop(self._owner.take());
        {
            let mut state = self
                .requests
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.live_registrations -= 1;
        }
        self.drained.notify_waiters();
    }
}
impl Drop for ReadRegistration {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
mod tests;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClientKind {
    File,
    Directory,
    Stats,
}

#[repr(u8)]
#[derive(Clone, Copy, Eq, PartialEq)]
enum ClientPhase {
    Opening,
    Live,
    Rollback,
    Releasing,
}

// One requested-layout allocation per admitted client, without BTree root
// retention, hash capacity, or a max_background bound on the number of fhs.
#[repr(C)]
struct ClientLease {
    ino: u64,
    fh: u64,
    generation: u64,
    open_unique: u64,
    next: Option<Box<ClientLease>>,
    owner: Option<MetadataMemoryGuard>,
    kind: ClientKind,
    phase: ClientPhase,
}

pub(crate) struct PendingClientOpen<'a> {
    registry: &'a ReadRegistry,
    generation: u64,
    completed: bool,
}

pub(crate) struct ClientRelease<'a> {
    registry: &'a ReadRegistry,
    generation: u64,
    completed: bool,
}

// These expressions charge Rust requested layouts, not allocator RSS. A
// physical allocator/permit final-deallocation receipt is still OPEN.
impl ReadRegistry {
    pub(crate) fn roots_requested_layout_bytes() -> u64 {
        (std::mem::size_of::<Self>()
            + 2 * std::mem::size_of::<usize>()
            + std::mem::size_of::<Mutex<ReadState>>()
            + 2 * std::mem::size_of::<usize>()
            + std::mem::size_of::<tokio::sync::Notify>()) as u64
    }

    pub(crate) fn client_requested_layout_bytes() -> u64 {
        (std::mem::size_of::<ClientLease>()
            + std::mem::size_of::<PendingClientOpen<'_>>()
            + std::mem::size_of::<ClientRelease<'_>>()) as u64
    }

    // The caller must reserve the initial Roots allocation before invoking
    // this constructor. Registrations clone this same external owner, so it
    // outlives both initial Arcs even when a registration outlives the VFS.
    pub(crate) fn new_owned(owner: Option<MetadataMemoryGuard>) -> Self {
        Self {
            requests: Arc::new(Mutex::new(ReadState::default())),
            drained: Arc::new(tokio::sync::Notify::new()),
            _roots: owner,
        }
    }

    pub(crate) fn check_client_open(&self) -> Result<(), Errno> {
        let state = self.requests.lock().map_err(|_| Errno::from(libc::EIO))?;
        if state.closed || state.client_failure.is_some() {
            Err(libc::ENODEV.into())
        } else {
            Ok(())
        }
    }

    pub(crate) fn begin_client_open(
        &self,
        ino: u64,
        kind: ClientKind,
        open_unique: u64,
        owner: Option<MetadataMemoryGuard>,
    ) -> Result<PendingClientOpen<'_>, Errno> {
        // Admission has already returned the owner; no Box/Arc is allocated
        // until after the same-lock Open check and checked generation update.
        let mut state = self.requests.lock().map_err(|_| Errno::from(libc::EIO))?;
        if state.closed || state.client_failure.is_some() {
            return Err(libc::ENODEV.into());
        }
        let generation = state
            .next_client_generation
            .checked_add(1)
            .ok_or(Errno::from(libc::EOVERFLOW))?;
        let live = state
            .live_clients
            .checked_add(1)
            .ok_or(Errno::from(libc::EOVERFLOW))?;
        let lease = Box::new(ClientLease {
            ino,
            fh: 0,
            generation,
            open_unique,
            next: state.clients.take(),
            owner,
            kind,
            phase: ClientPhase::Opening,
        });
        state.clients = Some(lease);
        state.next_client_generation = generation;
        state.live_clients = live;
        Ok(PendingClientOpen {
            registry: self,
            generation,
            completed: false,
        })
    }

    pub(crate) fn begin_client_release(
        &self,
        ino: u64,
        fh: u64,
        kind: ClientKind,
    ) -> Result<ClientRelease<'_>, Errno> {
        let mut state = self.requests.lock().map_err(|_| Errno::from(libc::EIO))?;
        let mut cursor = state.clients.as_deref_mut();
        while let Some(lease) = cursor {
            if lease.ino == ino && lease.fh == fh && lease.kind == kind {
                if lease.phase != ClientPhase::Live || fh == 0 {
                    return Err(libc::EBADF.into());
                }
                lease.phase = ClientPhase::Releasing;
                return Ok(ClientRelease {
                    registry: self,
                    generation: lease.generation,
                    completed: false,
                });
            }
            cursor = lease.next.as_deref_mut();
        }
        Err(libc::EBADF.into())
    }

    fn fail_client(&self, error: Errno) {
        let mut state = self
            .requests
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.client_failure.is_none() {
            state.client_failure = Some(error);
        }
        // Preserve every unresolved node and its Roots owner. A dropped OPEN,
        // rollback, or RELEASE is never synthesized into successful retirement.
        drop(state);
        self.drained.notify_waiters();
    }

    fn retire_client(&self, generation: u64) -> Result<(), Errno> {
        let mut state = self.requests.lock().map_err(|_| Errno::from(libc::EIO))?;
        let mut cursor = &mut state.clients;
        let mut retired = loop {
            if cursor
                .as_ref()
                .is_some_and(|lease| lease.generation == generation)
            {
                let mut lease = cursor.take().expect("matched client lease");
                *cursor = lease.next.take();
                break lease;
            }
            cursor = &mut cursor.as_mut().ok_or(Errno::from(libc::EBADF))?.next;
        };
        // live_clients deliberately remains nonzero while the detached node
        // and its final owner are being destroyed outside the state lock.
        drop(state);
        let owner = retired.owner.take();
        drop(retired); // actual Box deallocation first; next was already taken
        drop(owner);
        let mut state = self
            .requests
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.live_clients -= 1;
        drop(state);
        self.drained.notify_waiters();
        Ok(())
    }

    // Duplicate the original one-Notify drain shape to avoid retaining a
    // second nested shutdown future. Actual compiled size remains a gate.
    pub(crate) async fn prepare_unmount(&self) -> Result<(), Errno> {
        {
            let mut state = self
                .requests
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.closed = true;
            for handle in state.requests.values() {
                handle.abort();
            }
        }
        loop {
            let notified = self.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let state = self
                    .requests
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if let Some(error) = state.client_failure {
                    return Err(error);
                }
                if state.live_registrations == 0 && state.live_clients == 0 {
                    return Ok(());
                }
            }
            // Typed failure returns before the vendor ordinary helper. A live
            // external client still waits for real RELEASE; deadline policy is
            // a separately admitted driver concern, not an uncharged Timeout.
            notified.await;
        }
    }
}

impl PendingClientOpen<'_> {
    // Ok(true): OPEN committed before Closing; Ok(false): actual fh is retained
    // as rollback until its real close completes. Both decisions share the
    // same mutex used by Closing and new OPEN admission.
    pub(crate) fn commit(&mut self, fh: u64) -> Result<bool, Errno> {
        let mut state = self
            .registry
            .requests
            .lock()
            .map_err(|_| Errno::from(libc::EIO))?;
        if fh == 0 {
            if state.client_failure.is_none() {
                state.client_failure = Some(Errno::from(libc::EOVERFLOW));
            }
            drop(state);
            self.registry.drained.notify_waiters();
            return Err(libc::EOVERFLOW.into());
        }
        let closed = state.closed || state.client_failure.is_some();
        let mut cursor = state.clients.as_deref();
        let mut collision = false;
        while let Some(lease) = cursor {
            if lease.generation != self.generation && lease.fh == fh {
                collision = true;
                break;
            }
            cursor = lease.next.as_deref();
        }
        if collision {
            if state.client_failure.is_none() {
                state.client_failure = Some(Errno::from(libc::EOVERFLOW));
            }
            drop(state);
            self.registry.drained.notify_waiters();
            return Err(libc::EOVERFLOW.into());
        }
        let mut cursor = state.clients.as_deref_mut();
        while let Some(lease) = cursor {
            if lease.generation == self.generation {
                if lease.phase != ClientPhase::Opening {
                    if state.client_failure.is_none() {
                        state.client_failure = Some(Errno::from(libc::EBADF));
                    }
                    drop(state);
                    self.registry.drained.notify_waiters();
                    return Err(libc::EBADF.into());
                }
                lease.fh = fh;
                lease.phase = if closed {
                    ClientPhase::Rollback
                } else {
                    ClientPhase::Live
                };
                tracing::debug!(
                    event = "fuse_client_open_commit",
                    unique = lease.open_unique,
                    ino = lease.ino,
                    fh,
                    generation = lease.generation,
                    closing = closed
                );
                self.completed = !closed;
                return Ok(!closed);
            }
            cursor = lease.next.as_deref_mut();
        }
        if state.client_failure.is_none() {
            state.client_failure = Some(Errno::from(libc::EBADF));
        }
        drop(state);
        self.registry.drained.notify_waiters();
        Err(libc::EBADF.into())
    }

    pub(crate) fn finish_without_handle(mut self) -> Result<(), Errno> {
        if let Err(error) = self.registry.retire_client(self.generation) {
            self.registry.fail_client(error);
            self.completed = true;
            return Err(error);
        }
        self.completed = true;
        Ok(())
    }

    pub(crate) fn finish_rollback(mut self) -> Result<(), Errno> {
        if let Err(error) = self.registry.retire_client(self.generation) {
            self.registry.fail_client(error);
            self.completed = true;
            return Err(error);
        }
        self.completed = true;
        Ok(())
    }

    pub(crate) fn fail(mut self, error: Errno) {
        self.registry.fail_client(error);
        self.completed = true; // retain node/charge, suppress a second Drop error
    }
}

impl Drop for PendingClientOpen<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.registry.fail_client(Errno::from(libc::EIO));
        }
    }
}

impl ClientRelease<'_> {
    pub(crate) fn fail(mut self, error: Errno) {
        self.registry.fail_client(error);
        self.completed = true; // retained unresolved node, never fake retirement
    }

    pub(crate) fn finish(mut self) -> Result<(), Errno> {
        if let Err(error) = self.registry.retire_client(self.generation) {
            self.registry.fail_client(error);
            self.completed = true;
            return Err(error);
        }
        self.completed = true;
        Ok(())
    }
}

impl Drop for ClientRelease<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.registry.fail_client(Errno::from(libc::EIO));
        }
    }
}

impl Drop for ReadState {
    fn drop(&mut self) {
        // Iterative teardown avoids a stack proportional to externally held
        // clients when a failed/disconnected registry itself finally drops.
        while let Some(mut lease) = self.clients.take() {
            self.clients = lease.next.take();
            let owner = lease.owner.take();
            drop(lease);
            drop(owner);
        }
    }
}
