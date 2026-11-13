//! Owned backend drivers retain real pre-response work after caller cancellation.
//! The body owner retires only after actual EOF or userspace body destruction.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures_util::Stream;
use tokio::sync::Notify;

use crate::cadapter::client::ObjectByteStream;
use crate::workspace::packed::snapshot::{
    SnapshotBudgetPool, SnapshotMountBudget, SnapshotOwnedPermit,
};

const MAX_DRIVERS: usize = 128;
const DRIVER_STATE_BYTES: u64 = 4096;

#[derive(Debug, Default)]
struct State {
    closed: bool,
    owners: usize,
    poisoned: bool,
}

#[derive(Default)]
pub(crate) struct ReadonlyTransport {
    state: Mutex<State>,
    changed: Notify,
    reader: Mutex<Option<Arc<dyn crate::workspace::packed_reader_lifecycle::PackedReaderSession>>>,
}

impl std::fmt::Debug for ReadonlyTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReadonlyTransport")
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

struct Owner {
    transport: Arc<ReadonlyTransport>,
    terminal: bool,
    _permit: SnapshotOwnedPermit,
    _reader: Option<Arc<dyn crate::workspace::packed_reader_lifecycle::PackedReaderSession>>,
    _generation: Option<crate::workspace::packed_reader_lifecycle::PackedReaderRequestOwner>,
}

pub(super) struct OwnedTransportResult<T> {
    pub(super) value: T,
    _owner: Owner,
}

impl Drop for Owner {
    fn drop(&mut self) {
        let mut state = self.transport.state.lock().unwrap();
        assert!(state.owners > 0, "readonly transport owner underflow");
        state.owners -= 1;
        state.poisoned |= !self.terminal;
        drop(state);
        self.transport.changed.notify_waiters();
    }
}

impl ReadonlyTransport {
    pub(crate) fn bind_reader(
        &self,
        reader: Arc<dyn crate::workspace::packed_reader_lifecycle::PackedReaderSession>,
    ) -> anyhow::Result<()> {
        let state = self.state.lock().unwrap();
        anyhow::ensure!(
            !state.closed && state.owners == 0,
            "readonly transport already in use"
        );
        let mut slot = self.reader.lock().unwrap();
        if let Some(existing) = slot.as_ref() {
            anyhow::ensure!(
                Arc::ptr_eq(existing, &reader),
                "readonly transport belongs to another reader"
            );
        } else {
            *slot = Some(reader);
        }
        Ok(())
    }

    fn admit(
        self: &Arc<Self>,
        budget: &Arc<SnapshotMountBudget>,
        key_len: usize,
        stored_bytes: u64,
    ) -> anyhow::Result<Owner> {
        anyhow::ensure!(
            key_len > 0 && key_len <= 4096,
            "readonly transport key bound"
        );
        let mut state = self.state.lock().unwrap();
        anyhow::ensure!(
            !state.closed && !state.poisoned,
            "readonly transport is closed"
        );
        anyhow::ensure!(
            state.owners < MAX_DRIVERS,
            "readonly transport driver limit"
        );
        let bytes = DRIVER_STATE_BYTES + 2 * key_len as u64;
        let permit = budget.admit(&[
            (SnapshotBudgetPool::Plans, bytes),
            (SnapshotBudgetPool::Stored, stored_bytes),
        ])?;
        // The actual detached driver owns both its pinned generation and
        // heartbeat session, independently of the cancelled metadata caller.
        let reader = self.reader.lock().unwrap().clone();
        let generation = reader
            .as_ref()
            .map(|reader| reader.retain_request())
            .transpose()?;
        state.owners += 1;
        Ok(Owner {
            transport: self.clone(),
            terminal: false,
            _permit: permit,
            _reader: reader,
            _generation: generation,
        })
    }

    pub(crate) fn stop_admission(&self) {
        self.state.lock().unwrap().closed = true;
        self.changed.notify_waiters();
    }

    pub(crate) async fn drain(&self) -> anyhow::Result<()> {
        self.stop_admission();
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let terminal = {
                let state = self.state.lock().unwrap();
                (state.owners == 0).then_some(state.poisoned)
            };
            if let Some(poisoned) = terminal {
                anyhow::ensure!(
                    !poisoned,
                    "readonly transport driver failed to reach terminal state"
                );
                return Ok(());
            }
            changed.await;
        }
    }

    /// The factory is invoked after admission, before cloning keys/buffers.
    pub(super) async fn run<T, F>(
        self: &Arc<Self>,
        budget: &Arc<SnapshotMountBudget>,
        key_len: usize,
        stored_bytes: u64,
        make: impl FnOnce() -> F,
    ) -> anyhow::Result<OwnedTransportResult<T>>
    where
        T: Send + 'static,
        F: Future<Output = anyhow::Result<T>> + Send + 'static,
    {
        let mut owner = self.admit(budget, key_len, stored_bytes)?;
        let future = make();
        let (send, receive) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let result = future.await;
            owner.terminal = true;
            // The channel/result retains bytes and its admitted owner until
            // the receiver copies them, or a cancelled receiver drops them.
            let result = result.map(|value| OwnedTransportResult {
                value,
                _owner: owner,
            });
            drop(send.send(result));
        });
        receive
            .await
            .map_err(|_| anyhow::anyhow!("readonly transport driver terminated"))?
    }

    pub(crate) async fn open_body<F>(
        self: &Arc<Self>,
        budget: &Arc<SnapshotMountBudget>,
        key_len: usize,
        make: impl FnOnce() -> F,
    ) -> anyhow::Result<ObjectByteStream>
    where
        F: Future<Output = anyhow::Result<ObjectByteStream>> + Send + 'static,
    {
        let mut owner = self.admit(budget, key_len, 0)?;
        let future = make();
        let (send, receive) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let result = match future.await {
                Ok(stream) => Ok(Box::pin(OwnedBody {
                    // Field order and Drop guarantee the actual response is
                    // destroyed before its transport owner announces drain.
                    inner: Some(stream),
                    owner: Some(owner),
                }) as ObjectByteStream),
                Err(error) => {
                    owner.terminal = true;
                    drop(owner);
                    Err(error)
                }
            };
            drop(send.send(result));
        });
        receive
            .await
            .map_err(|_| anyhow::anyhow!("readonly transport body driver terminated"))?
    }
}

struct OwnedBody {
    inner: Option<ObjectByteStream>,
    owner: Option<Owner>,
}

impl Stream for OwnedBody {
    type Item = anyhow::Result<bytes::Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let next = match self.inner.as_mut() {
            Some(inner) => inner.as_mut().poll_next(cx),
            None => return Poll::Ready(None),
        };
        if matches!(next, Poll::Ready(None)) {
            drop(self.inner.take());
            if let Some(mut owner) = self.owner.take() {
                owner.terminal = true;
                drop(owner);
            }
        }
        next
    }
}

impl Drop for OwnedBody {
    fn drop(&mut self) {
        drop(self.inner.take());
        if let Some(mut owner) = self.owner.take() {
            // Dropping a response body is an actual userspace cancellation
            // boundary; a panic while polling/destroying it fails closed.
            owner.terminal = !std::thread::panicking();
            drop(owner);
        }
    }
}
