//! File and directory handle management

use crate::chunk::BlockStore;
use crate::meta::MetaLayer;
use crate::meta::store::FileAttr;
use crate::vfs::fs::DirEntry;
use crate::vfs::io::{FileReader, FileWriter};
use anyhow::anyhow;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::SystemTime;

fn current_time_secs() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
use std::time::Instant;
use tokio::pin;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

/// Maximum entries to return per readdir/readdirplus call
/// The setting should be based on the size of the cache handled by the kernel at one time.
const MAX_READDIR_ENTRIES: usize = 50;

struct GateState {
    readers: u32,
    writers_waiting: u32,
    writing: bool,
}

struct HandleGate {
    state: StdMutex<GateState>,
    notify: Notify,
}

impl HandleGate {
    fn new() -> Self {
        Self {
            state: StdMutex::new(GateState {
                readers: 0,
                writers_waiting: 0,
                writing: false,
            }),
            notify: Notify::new(),
        }
    }

    async fn read_lock(self: &Arc<Self>) -> HandleReadGuard {
        loop {
            // `notified()` does not push us into the waiting queue.
            let notified = self.notify.notified();
            pin!(notified);

            // Critical: mutually register us into the waiting queue.
            // There is a time window between we drop the guard and await the `notified`.
            // After dropping the guard, a writer can `notify_waiters`, but if we aren't waiting for,
            // we will cause a lost wake-up!
            notified.as_mut().enable();
            {
                let mut guard = self.state.lock().unwrap();

                if !guard.writing && guard.writers_waiting == 0 {
                    guard.readers = guard.readers.saturating_add(1);
                    return HandleReadGuard {
                        gate: Arc::clone(self),
                    };
                }
            }

            // If we haven't been registered, we will register here once, but it may be too late!
            notified.await;
        }
    }

    async fn write_lock(self: &Arc<Self>) -> HandleWriteGuard {
        let mut waiter = HandleWriteWaiter::new(self);
        loop {
            let notified = self.notify.notified();
            pin!(notified);

            // Critical: refer to `read_lock`.
            notified.as_mut().enable();

            {
                let mut guard = self.state.lock().unwrap();

                if guard.readers == 0 && !guard.writing {
                    guard.writing = true;
                    guard.writers_waiting = guard.writers_waiting.wrapping_sub(1);
                    waiter.disarm();
                    return HandleWriteGuard {
                        gate: Arc::clone(self),
                    };
                }
            }

            notified.await;
        }
    }

    fn read_unlock(&self) {
        let mut guard = self.state.lock().unwrap();
        guard.readers = guard.readers.saturating_sub(1);

        if guard.readers == 0 {
            self.notify.notify_waiters();
        }
    }

    fn write_unlock(&self) {
        let mut guard = self.state.lock().unwrap();
        guard.writing = false;
        self.notify.notify_waiters();
    }

    fn waiting_dec(&self) {
        let mut guard = self.state.lock().unwrap();
        guard.writers_waiting = guard.writers_waiting.saturating_sub(1);
        self.notify.notify_waiters();
    }
}

struct HandleReadGuard {
    gate: Arc<HandleGate>,
}

impl Drop for HandleReadGuard {
    fn drop(&mut self) {
        self.gate.read_unlock();
    }
}

struct HandleWriteGuard {
    gate: Arc<HandleGate>,
}

impl Drop for HandleWriteGuard {
    fn drop(&mut self) {
        self.gate.write_unlock();
    }
}

struct HandleWriteWaiter<'a> {
    gate: &'a HandleGate,
    active: bool,
}

impl<'a> HandleWriteWaiter<'a> {
    fn new(gate: &'a HandleGate) -> Self {
        {
            let mut guard = gate.state.lock().unwrap();
            guard.writers_waiting = guard.writers_waiting.saturating_add(1);
        }
        Self { gate, active: true }
    }

    fn disarm(&mut self) {
        self.active = false;
    }
}

impl Drop for HandleWriteWaiter<'_> {
    fn drop(&mut self) {
        if self.active {
            self.gate.waiting_dec();
        }
    }
}

struct FileHandleState<B, M>
where
    B: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    attr: FileAttr,
    last_offset: u64,
    last_check: i64,
    reader: Option<Arc<FileReader<B, M>>>,
    writer: Option<Arc<FileWriter<B, M>>>,
}

#[allow(dead_code)]
pub(crate) struct FileHandle<B, M>
where
    B: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    pub(crate) fh: u64,
    pub(crate) ino: i64,
    pub(crate) opened_at: Instant,
    pub(crate) flags: HandleFlags,
    write_dirty: AtomicBool,
    gate: Arc<HandleGate>,
    state: StdMutex<FileHandleState<B, M>>,
}

/// Default attribute cache TTL for open files (seconds).
const ATTR_CACHE_TTL: i64 = 1;

impl<B, M> FileHandle<B, M>
where
    B: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    pub(crate) fn new(fh: u64, ino: i64, attr: FileAttr, flags: HandleFlags) -> Self {
        Self {
            fh,
            ino,
            opened_at: Instant::now(),
            flags,
            write_dirty: AtomicBool::new(false),
            gate: Arc::new(HandleGate::new()),
            state: StdMutex::new(FileHandleState {
                attr,
                last_offset: 0,
                last_check: current_time_secs(),
                reader: None,
                writer: None,
            }),
        }
    }

    pub(crate) fn reader(&self, reader: Arc<FileReader<B, M>>) {
        let mut guard = self.state.lock().unwrap();
        guard.reader = Some(reader);
    }

    pub(crate) fn ensure_reader_with<F>(&self, make_reader: F)
    where
        F: FnOnce() -> Arc<FileReader<B, M>>,
    {
        let mut guard = self.state.lock().unwrap();
        if guard.reader.is_none() {
            guard.reader = Some(make_reader());
        }
    }

    pub(crate) fn writer(&self, writer: Arc<FileWriter<B, M>>) {
        let mut guard = self.state.lock().unwrap();
        guard.writer = Some(writer);
    }

    /// Return the cached attr regardless of TTL (used for handle-level access).
    pub(crate) fn attr(&self) -> FileAttr {
        self.state.lock().unwrap().attr.clone()
    }

    pub(crate) fn update_attr(&self, attr: &FileAttr) {
        let mut guard = self.state.lock().unwrap();
        guard.attr = attr.clone();
        guard.last_check = current_time_secs();
    }

    /// JuiceFS-style Check: returns the cached attr if within TTL.
    /// Used to avoid a metadata-store round-trip for getattr.
    pub(crate) fn check_attr(&self) -> Option<FileAttr> {
        let guard = self.state.lock().unwrap();
        let now = current_time_secs();
        if now - guard.last_check < ATTR_CACHE_TTL {
            Some(guard.attr.clone())
        } else {
            None
        }
    }

    /// Update the cached attr, invalidating if mtime changed.
    /// Returns true if the attr was updated in-place.
    pub(crate) fn update_attr_if_changed(&self, new_attr: &FileAttr) -> bool {
        let mut guard = self.state.lock().unwrap();
        let changed = new_attr.mtime != guard.attr.mtime;
        if changed {
            guard.attr = new_attr.clone();
            guard.last_check = current_time_secs();
            true
        } else if new_attr.size > guard.attr.size {
            guard.attr.size = new_attr.size;
            guard.last_check = current_time_secs();
            true
        } else {
            false
        }
    }

    /// Force the cached size to at least `min_size`.  This is called after
    /// every extending write so that O_APPEND from other handles sees the
    /// most recent file size via check_attr/attr.
    pub(crate) fn extend_size(&self, min_size: u64) {
        let mut guard = self.state.lock().unwrap();
        if min_size > guard.attr.size {
            guard.attr.size = min_size;
        }
        guard.last_check = current_time_secs();
    }

    pub(crate) fn update_offset(&self, offset: u64) {
        self.state.lock().unwrap().last_offset = offset;
    }

    pub(crate) fn mark_write_dirty(&self) {
        self.write_dirty.store(true, Ordering::Release);
    }

    pub(crate) fn take_write_dirty(&self) -> bool {
        self.write_dirty.swap(false, Ordering::AcqRel)
    }

    #[allow(dead_code)]
    pub(crate) fn last_offset(&self) -> u64 {
        self.state.lock().unwrap().last_offset
    }

    #[tracing::instrument(name = "Handle.read", level = "trace", skip(self))]
    pub(crate) async fn read(&self, offset: u64, len: usize) -> anyhow::Result<Vec<u8>> {
        let _guard = self.gate.read_lock().await;
        let reader = {
            let guard = self.state.lock().unwrap();
            guard
                .reader
                .clone()
                .ok_or_else(|| anyhow!("file handle reader not initialized"))?
        };
        let data = reader.read(offset, len).await?;
        self.update_offset(offset + data.len() as u64);
        Ok(data)
    }

    pub(crate) async fn try_read_overlay<F, Fut>(
        &self,
        offset: u64,
        len: usize,
        f: F,
    ) -> anyhow::Result<Option<Vec<u8>>>
    where
        F: FnOnce(u64, usize) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<Option<Vec<u8>>>>,
    {
        let _guard = self.gate.read_lock().await;
        let data = f(offset, len).await?;
        if let Some(data) = &data {
            self.update_offset(offset + data.len() as u64);
        }
        Ok(data)
    }

    pub(crate) async fn write(&self, offset: u64, data: &[u8]) -> anyhow::Result<usize> {
        let _guard = self.gate.write_lock().await;
        self.write_unlocked(offset, data).await
    }

    /// Write while the caller already holds the required handle write locks.
    pub(crate) async fn write_unlocked(&self, offset: u64, data: &[u8]) -> anyhow::Result<usize> {
        let writer = {
            let guard = self.state.lock().unwrap();
            guard
                .writer
                .clone()
                .ok_or_else(|| anyhow!("file handle writer not initialized"))?
        };
        let written = writer.write_at(offset, data).await?;
        self.update_offset(offset + written as u64);
        if written > 0 {
            self.mark_write_dirty();
        }
        Ok(written)
    }

    pub(crate) async fn flush(&self) -> anyhow::Result<()> {
        let writer = {
            let guard = self.state.lock().unwrap();
            guard
                .writer
                .clone()
                .ok_or_else(|| anyhow!("file handle writer not initialized"))?
        };
        writer.flush().await?;
        Ok(())
    }

    pub(crate) async fn lock_write(&self) -> FileHandleWriteGuard {
        let guard = self.gate.write_lock().await;
        FileHandleWriteGuard { _guard: guard }
    }
}

pub(crate) struct FileHandleWriteGuard {
    _guard: HandleWriteGuard,
}

#[allow(dead_code)]
#[derive(Clone, Copy)]
pub(crate) struct HandleFlags {
    pub(crate) read: bool,
    pub(crate) write: bool,
    pub(crate) append: bool,
}

impl HandleFlags {
    pub(crate) const fn new(read: bool, write: bool, append: bool) -> Self {
        Self {
            read,
            write,
            append,
        }
    }
}

/// Directory handle for caching directory listing during opendir-releasedir lifecycle
pub struct DirHandle {
    pub(crate) ino: i64,
    pub(crate) attr: Option<FileAttr>,
    pub(crate) entries: Vec<DirEntry>,
    #[allow(dead_code)]
    pub(crate) opened_at: Instant,
    /// Background task handle for batch attribute prefetching
    pub(crate) prefetch_task: Option<JoinHandle<()>>,
    /// Flag indicating whether prefetch task has completed
    pub(crate) prefetch_done: Arc<AtomicBool>,
}

impl DirHandle {
    #[allow(unused)]
    pub(crate) fn new(ino: i64, entries: Vec<DirEntry>) -> Self {
        Self {
            ino,
            attr: None,
            entries,
            opened_at: Instant::now(),
            prefetch_task: None,
            prefetch_done: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn with_prefetch_task(
        ino: i64,
        entries: Vec<DirEntry>,
        task: JoinHandle<()>,
        done_flag: Arc<AtomicBool>,
    ) -> Self {
        Self {
            ino,
            attr: None,
            entries,
            opened_at: Instant::now(),
            prefetch_task: Some(task),
            prefetch_done: done_flag,
        }
    }

    pub(crate) fn with_attr(mut self, attr: FileAttr) -> Self {
        self.attr = Some(attr);
        self
    }

    /// Get entries starting from offset, limited to MAX_READDIR_ENTRIES
    pub(crate) fn get_entries(&self, offset: u64) -> Vec<DirEntry> {
        let start = offset as usize;
        if start >= self.entries.len() {
            return Vec::new();
        }
        let end = std::cmp::min(start + MAX_READDIR_ENTRIES, self.entries.len());
        self.entries[start..end].to_vec()
    }

    /// Get total number of entries
    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if directory is empty
    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Drop for DirHandle {
    fn drop(&mut self) {
        // Abort prefetch task if still running
        if let Some(task) = self.prefetch_task.take() {
            let is_done = self.prefetch_done.load(Ordering::Acquire);
            if !is_done {
                tracing::trace!("Aborting prefetch task for dir ino={}", self.ino);
                task.abort();
            }
        }
    }
}
