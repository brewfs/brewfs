use super::*;

#[cfg(all(test, feature = "workspace-overlay"))]
mod tests;

pub(super) struct PackedWriterTasks {
    enabled: AtomicBool,
    active: AtomicU64,
    failed: AtomicBool,
    notify: Notify,
}

impl PackedWriterTasks {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            enabled: AtomicBool::new(false),
            active: AtomicU64::new(0),
            failed: AtomicBool::new(false),
            notify: Notify::new(),
        })
    }

    pub(super) fn record_failure(&self) {
        if self.enabled.load(Ordering::Acquire) {
            self.failed.store(true, Ordering::Release);
            self.notify.notify_waiters();
        }
    }

    fn begin(self: &Arc<Self>) -> Option<WriterTaskOwner> {
        if !self.enabled.load(Ordering::Acquire) {
            return None;
        }
        if self
            .active
            .try_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .is_err()
        {
            self.failed.store(true, Ordering::Release);
            self.notify.notify_waiters();
            return None;
        }
        Some(WriterTaskOwner {
            tasks: self.clone(),
            terminal: false,
        })
    }

    #[cfg(feature = "workspace-overlay")]
    async fn wait_idle(&self, deadline: Duration) -> anyhow::Result<()> {
        let wait = async {
            loop {
                let notified = self.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.failed.load(Ordering::Acquire) {
                    anyhow::bail!("packed writer task failed before its terminal boundary");
                }
                if self.active.load(Ordering::Acquire) == 0 {
                    return Ok(());
                }
                notified.await;
            }
        };
        timeout(deadline, wait)
            .await
            .map_err(|_| anyhow::anyhow!("packed writer task drain deadline"))?
    }
}

struct WriterTaskOwner {
    tasks: Arc<PackedWriterTasks>,
    terminal: bool,
}
impl Drop for WriterTaskOwner {
    fn drop(&mut self) {
        if !self.terminal {
            self.tasks.failed.store(true, Ordering::Release);
        }
        self.tasks.active.fetch_sub(1, Ordering::AcqRel);
        self.tasks.notify.notify_waiters();
    }
}

/// Register synchronously, before the spawn can be delayed by the scheduler.
/// A parent registers any child before releasing its own owner, so idle cannot
/// be observed while a follow-on upload/metadata task is merely queued.
pub(super) fn spawn_writer_owned<F>(tasks: &Arc<PackedWriterTasks>, future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    let owner = tasks.begin();
    tokio::spawn(async move {
        let mut owner = owner;
        future.await;
        if let Some(owner) = &mut owner {
            owner.terminal = true;
        }
    });
}

#[cfg(feature = "workspace-overlay")]
pub(crate) struct PackedWriterDrain {
    inodes: Vec<i64>,
    _cleanup: tokio::sync::OwnedMutexGuard<()>,
    _snapshot_owner: Option<crate::meta::layer::MetadataMemoryGuard>,
}
#[cfg(feature = "workspace-overlay")]
impl PackedWriterDrain {
    pub(crate) fn inodes(&self) -> &[i64] {
        &self.inodes
    }
}

#[cfg(feature = "workspace-overlay")]
impl<B, M> FileWriter<B, M>
where
    B: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    async fn validate_publication_slices(&self, max_rows: usize) -> anyhow::Result<()> {
        self.shared.writeback_result()?;
        let inner = self.shared.inner.lock().await;
        let mut rows = 0usize;
        for chunk in inner.chunks.values() {
            for slice in chunk.slices.iter().chain(chunk.recently_committed.iter()) {
                rows = rows
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("slice row overflow"))?;
                if rows > max_rows {
                    anyhow::bail!("packed publication slice row cap");
                }
                let state = slice.lock();
                if !matches!(state.state, SliceStatus::Committed)
                    || !state.upload_complete()
                    || state.in_flight != 0
                    || state.upload_task_active
                    || state.writeback_record_sealing
                    || state.err.is_some()
                {
                    anyhow::bail!(
                        "packed publication has incomplete slice for ino {}",
                        self.shared.inode.ino()
                    );
                }
            }
        }
        Ok(())
    }
}

#[cfg(feature = "workspace-overlay")]
impl<B, M> DataWriter<B, M>
where
    B: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    pub(crate) fn with_packed_publication_tracking(self, enabled: bool) -> Self {
        self.recent_pending_upload
            .packed_tasks
            .enabled
            .store(enabled, Ordering::Release);
        self
    }

    /// Called only after this VFS's admission gate has closed and all admitted
    /// mutations (including mount recovery) have completed. It cannot issue a
    /// native or PM11 proof. Retain its result through final metadata sync.
    pub(crate) async fn drain_for_packed_publication(
        &self,
        max_files: usize,
    ) -> anyhow::Result<PackedWriterDrain> {
        const MAX_SLICE_ROWS: usize = 4096;
        let cleanup = self.publication_cleanup.clone().lock_owned().await;
        if !self
            .recent_pending_upload
            .packed_tasks
            .enabled
            .load(Ordering::Acquire)
            || max_files == 0
            || max_files > 256
            || self.files.len() > max_files
        {
            anyhow::bail!("packed writer publication was not armed or exceeds file cap");
        }
        let bytes = u64::try_from(max_files)?
            .checked_mul(64)
            .and_then(|value| value.checked_add((MAX_SLICE_ROWS * 128 + 4096) as u64))
            .ok_or_else(|| anyhow::anyhow!("packed writer snapshot size overflow"))?;
        let owner = self
            .backend
            .meta()
            .reserve_memory(crate::meta::layer::MetadataMemoryKind::Request, bytes)?;
        let mut writers = Vec::with_capacity(max_files);
        let mut inodes = Vec::with_capacity(max_files);
        for entry in self.files.iter() {
            if writers.len() >= max_files {
                anyhow::bail!("packed writer publication file cap raced");
            }
            let ino = i64::try_from(*entry.key())?;
            writers.push(entry.value().clone());
            inodes.push(ino);
        }
        // Stop every auto-flush producer before waiting. Tracked parent tasks
        // keep ownership while their upload/commit children are registered.
        for writer in &writers {
            writer
                .shared
                .publication_freezing
                .store(true, Ordering::Release);
            let inner = writer.shared.inner.lock().await;
            if inner.chunks.len() > MAX_SLICE_ROWS {
                anyhow::bail!("packed publication chunk row cap before flush");
            }
            let rows = inner
                .chunks
                .values()
                .try_fold(0usize, |total, chunk| {
                    total
                        .checked_add(chunk.slices.len())?
                        .checked_add(chunk.recently_committed.len())
                })
                .ok_or_else(|| anyhow::anyhow!("packed publication slice row overflow"))?;
            if rows > MAX_SLICE_ROWS {
                anyhow::bail!("packed publication slice row cap before flush");
            }
        }
        for writer in &writers {
            writer.flush_with_deadline(FLUSH_DEADLINE).await?;
        }
        // flush() may return at metadata commit. This waits the actual upload
        // and metadata drivers, even when a writer was discarded before close.
        self.recent_pending_upload
            .packed_tasks
            .wait_idle(FLUSH_DEADLINE)
            .await?;
        for writer in &writers {
            writer.validate_publication_slices(MAX_SLICE_ROWS).await?;
        }
        if self.recent_pending_upload.bytes.load(Ordering::Acquire) != 0
            || self
                .recent_pending_upload
                .stage_inflight_bytes
                .load(Ordering::Acquire)
                != 0
            || self
                .recent_pending_upload
                .remote_upload_inflight_bytes
                .load(Ordering::Acquire)
                != 0
            || self
                .write_back
                .as_ref()
                .is_some_and(|cache| cache.has_recoverable_records())
        {
            anyhow::bail!("packed publication retains dirty, remote or recovery work");
        }
        drop(writers);
        Ok(PackedWriterDrain {
            inodes,
            _cleanup: cleanup,
            _snapshot_owner: owner,
        })
    }
}
