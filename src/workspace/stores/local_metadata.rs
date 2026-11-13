//! Private mount metadata journal. Only application CAS can be deferred.
//! Every other CAS and range read is a remote-commit barrier. This deliberately
//! preserves existing ownership, upload, publication and bounded-scan proofs.
use super::kv_backend::{KvCheck, KvEntry, KvReadLimits, KvWrite, WorkspaceKvBackend};
use crate::workspace::error::WorkspaceError;
use async_trait::async_trait;
use bincode::Options;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::Mutex;
mod profile;
use profile::{Profile, Stage};

type Values = BTreeMap<Vec<u8>, Option<Vec<u8>>>;
const MAX_JOURNAL: u64 = 32 << 20;
const MAGIC: &[u8; 8] = b"BWSLMD01";

#[derive(Clone, Debug)]
pub struct LocalMetadataConfig {
    pub directory: PathBuf,
    /// True: ACK after local fsync. False: unsynced updates may be lost on crash.
    pub sync_on_update: bool,
    pub flush_interval: Duration,
    pub max_operations: usize,
    pub max_bytes: usize,
}

impl LocalMetadataConfig {
    /// Remote is the default. Explicit opt-in requires a durable local path.
    pub fn from_env(mount: &Path) -> Result<Option<Self>, WorkspaceError> {
        let sync_on_update = match std::env::var("BREWFS_METADATA_SYNC")
            .as_deref()
            .unwrap_or("remote")
        {
            "remote" => return Ok(None),
            "local-journal" => true,
            "local-buffered" => false,
            _ => {
                return Err(invalid(
                    "BREWFS_METADATA_SYNC must be remote, local-journal or local-buffered",
                ));
            }
        };
        fn number(name: &str, default: usize) -> Result<usize, WorkspaceError> {
            std::env::var(name).map_or(Ok(default), |v| v.parse().map_err(|_| invalid(name)))
        }
        let directory = PathBuf::from(
            std::env::var("BREWFS_METADATA_JOURNAL_DIR")
                .map_err(|_| invalid("BREWFS_METADATA_JOURNAL_DIR is required"))?,
        );
        if !directory.is_absolute() {
            return Err(invalid("journal path must be absolute"));
        }
        std::fs::create_dir_all(&directory)?;
        let directory = directory.canonicalize()?;
        if directory.starts_with(mount.canonicalize()?) {
            return Err(invalid("metadata journal must be outside the FUSE mount"));
        }
        let config = Self {
            directory,
            sync_on_update,
            flush_interval: Duration::from_millis(number("BREWFS_METADATA_FLUSH_MS", 10)? as u64),
            max_operations: number("BREWFS_METADATA_MAX_OPERATIONS", 1024)?,
            max_bytes: number("BREWFS_METADATA_MAX_BYTES", 1 << 20)?,
        };
        config.validate()?;
        Ok(Some(config))
    }

    fn validate(&self) -> Result<(), WorkspaceError> {
        if !self.directory.is_absolute()
            || self.flush_interval.is_zero()
            || self.flush_interval > Duration::from_millis(1000)
            || !(1..=4096).contains(&self.max_operations)
            || !(1024..=4 << 20).contains(&self.max_bytes)
        {
            return Err(invalid(
                "metadata journal limits: interval 1..1000 ms, operations 1..4096, bytes 1024..4194304",
            ));
        }
        Ok(())
    }
}

fn invalid(message: &str) -> WorkspaceError {
    WorkspaceError::InvalidReadPlan(message.into())
}
fn codec(error: impl std::fmt::Display) -> WorkspaceError {
    WorkspaceError::CorruptMetadata(error.to_string())
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct Batch {
    sequence: u64,
    operations: usize,
    checks: Values,
    writes: Values,
    deadline: i64,
}

impl Batch {
    fn merge(&mut self, sequence: u64, checks: &[KvCheck], writes: &[KvWrite], deadline: i64) {
        for check in checks {
            if !self.writes.contains_key(&check.key) {
                self.checks
                    .entry(check.key.clone())
                    .or_insert_with(|| check.expected.clone());
            }
        }
        for write in writes {
            let (key, value) = match write {
                KvWrite::Put { key, value } => (key, Some(value.clone())),
                KvWrite::Delete { key } => (key, None),
            };
            self.writes.insert(key.clone(), value);
        }
        self.sequence = sequence;
        self.operations += 1;
        self.deadline = if self.deadline == 0 {
            deadline
        } else {
            self.deadline.min(deadline)
        };
    }
    fn bytes(&self) -> usize {
        self.checks
            .iter()
            .chain(&self.writes)
            .map(|(k, v)| k.len() + v.as_ref().map_or(0, Vec::len))
            .sum()
    }
    fn receipt(&self) -> Result<Vec<u8>, WorkspaceError> {
        Ok(Sha256::digest(bincode::serialize(self).map_err(codec)?).to_vec())
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct JournalState {
    scope: Vec<u8>,
    sequence: u64,
    confirmed: Option<Vec<u8>>,
    in_flight: Option<Batch>,
    pending: Batch,
}

impl JournalState {
    fn overlay(&self, key: &[u8]) -> Option<&Option<Vec<u8>>> {
        self.pending
            .writes
            .get(key)
            .or_else(|| self.in_flight.as_ref().and_then(|b| b.writes.get(key)))
    }
    fn dirty(&self) -> bool {
        self.in_flight.is_some() || self.pending.operations > 0
    }
}

struct Journal {
    directory: PathBuf,
    profile: Profile,
    // The separate lock inode survives atomic replacement of state.bin.
    _lock: File,
    #[cfg(test)]
    pause: std::sync::Mutex<Option<JournalPause>>,
}

#[cfg(test)]
type JournalPause = (
    tokio::sync::oneshot::Sender<()>,
    std::sync::mpsc::Receiver<()>,
);

impl Journal {
    fn open(
        config: &LocalMetadataConfig,
        scope: &[u8],
    ) -> Result<(Self, JournalState), WorkspaceError> {
        config.validate()?;
        std::fs::create_dir_all(&config.directory)?;
        // Persist newly created directory entries too, not just state.bin.
        for directory in config.directory.ancestors() {
            File::open(directory)?.sync_all()?;
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(config.directory.join("lock"))?;
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            // SAFETY: the descriptor remains owned by Journal until it is dropped.
            if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
                return Err(WorkspaceError::Io(std::io::Error::last_os_error()));
            }
        }
        #[cfg(not(unix))]
        return Err(WorkspaceError::UnsupportedCapability(
            "local metadata journal requires Unix file locking",
        ));
        let journal = Self {
            directory: config.directory.clone(),
            profile: Profile::default(),
            _lock: lock,
            #[cfg(test)]
            pause: std::sync::Mutex::new(None),
        };
        let path = journal.directory.join("state.bin");
        let state = if path.exists() {
            let file = File::open(path)?;
            if file.metadata()?.len() > MAX_JOURNAL {
                return Err(codec("oversized metadata journal"));
            }
            let mut bytes = Vec::new();
            file.take(MAX_JOURNAL + 1).read_to_end(&mut bytes)?;
            if bytes.len() < 40
                || &bytes[..8] != MAGIC
                || Sha256::digest(&bytes[40..]).as_slice() != &bytes[8..40]
            {
                return Err(codec("invalid metadata journal checksum/version"));
            }
            let state: JournalState = bincode::DefaultOptions::new()
                .with_fixint_encoding()
                .with_limit(MAX_JOURNAL)
                .reject_trailing_bytes()
                .deserialize(&bytes[40..])
                .map_err(codec)?;
            if state.scope != scope {
                return Err(invalid(
                    "metadata journal belongs to another catalog/workspace",
                ));
            }
            state
        } else {
            JournalState {
                scope: scope.to_vec(),
                ..Default::default()
            }
        };
        Ok((journal, state))
    }
    fn persist(&self, state: &JournalState) -> Result<(), WorkspaceError> {
        #[cfg(test)]
        {
            let pause = self.pause.lock().unwrap().take();
            if let Some((reached, resume)) = pause {
                let _ = reached.send(());
                resume.recv().map_err(codec)?;
            }
        }
        let encode = self.profile.span(Stage::JournalEncode);
        let payload = bincode::serialize(state).map_err(codec)?;
        if payload.len() as u64 + 40 > MAX_JOURNAL {
            return Err(invalid("metadata journal capacity exceeded"));
        }
        let digest = Sha256::digest(&payload);
        drop(encode);
        let write = self.profile.span(Stage::JournalWrite);
        let path = self.directory.join("state.tmp");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        file.write_all(MAGIC)?;
        file.write_all(&digest)?;
        file.write_all(&payload)?;
        drop(write);
        let sync = self.profile.span(Stage::JournalFileSync);
        file.sync_all()?;
        drop(sync);
        let _rename = self.profile.span(Stage::JournalRenameSync);
        std::fs::rename(path, self.directory.join("state.bin"))?;
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }
}

struct Core {
    remote: Arc<dyn WorkspaceKvBackend>,
    config: LocalMetadataConfig,
    journal: Arc<Journal>,
    state: Arc<Mutex<JournalState>>,
    admission: Mutex<()>,
    flush: Mutex<()>,
    receipt_key: Vec<u8>,
    failed: AtomicBool,
    // Buffered state may advance before its snapshot reaches disk. Only the
    // serialized flush driver clears this, including after an ACK write fails.
    checkpoint_needed: AtomicBool,
}

async fn read_receipt(
    remote: &dyn WorkspaceKvBackend,
    key: &[u8],
) -> Result<Option<Vec<u8>>, WorkspaceError> {
    let keys = [key.to_vec()];
    let (mut values, _) = remote
        .get_many_consistent_with_time_bounded(
            &keys,
            KvReadLimits {
                max_records: 1,
                max_key_bytes: 1024,
                max_value_bytes: 32,
                max_total_bytes: 2048,
                max_response_bytes: 8192,
                max_data_requests: 1,
            },
        )
        .await?;
    if values.len() != 1 || values[0].as_ref().is_some_and(|v| v.len() != 32) {
        return Err(codec("invalid metadata batch receipt"));
    }
    Ok(values.remove(0))
}

impl Core {
    async fn install(
        &self,
        mut state: tokio::sync::OwnedMutexGuard<JournalState>,
        next: JournalState,
        durable: bool,
    ) -> Result<(), WorkspaceError> {
        if !durable {
            // Explicit buffered mode: atomic local visibility, no durability
            // promise until a remote barrier. There is no await in this update.
            *state = next;
            return Ok(());
        }
        let journal = self.journal.clone();
        let queued = journal.profile.start();
        // The blocking job owns the state lock AND its installation. Cancelling
        // an awaiter cannot let a later edit race a still-running fsync/rename,
        // or leave a durable snapshot newer than the in-memory view.
        tokio::task::spawn_blocking(move || {
            journal.profile.record(Stage::BlockingQueue, queued);
            journal.persist(&next)?;
            *state = next;
            Ok(())
        })
        .await
        .map_err(|e| WorkspaceError::Backend(e.to_string()))?
    }

    // Buffered updates do not promise per-operation durability. Publish their
    // snapshot in memory, then persist it without blocking readers/new edits.
    // The flush driver stays serialized across BOTH this I/O and remote CAS.
    async fn checkpoint(
        &self,
        mut state: tokio::sync::OwnedMutexGuard<JournalState>,
        next: JournalState,
    ) -> Result<(), WorkspaceError> {
        if self.config.sync_on_update {
            return self.install(state, next, true).await;
        }
        self.checkpoint_needed.store(true, Ordering::Release);
        *state = next;
        let snapshot = state.clone();
        drop(state);
        let journal = self.journal.clone();
        let queued = journal.profile.start();
        tokio::task::spawn_blocking(move || {
            journal.profile.record(Stage::BlockingQueue, queued);
            journal.persist(&snapshot)
        })
        .await
        .map_err(|e| WorkspaceError::Backend(e.to_string()))??;
        self.checkpoint_needed.store(false, Ordering::Release);
        Ok(())
    }

    async fn flush_to(self: &Arc<Self>, target: u64) -> Result<(), WorkspaceError> {
        if self.config.sync_on_update {
            return self.flush_inner(target).await;
        }
        // Cancellation of an fsync/flush awaiter must not release the driver
        // while its blocking snapshot write is still running. The owned worker
        // completes the checkpoint, and the next driver waits for it.
        let core = self.clone();
        tokio::spawn(async move {
            let result = core.flush_inner(target).await;
            core.failed.store(result.is_err(), Ordering::Release);
            result
        })
        .await
        .map_err(|e| WorkspaceError::Backend(e.to_string()))?
    }

    // Never holds state across remote commit. In buffered mode it also releases
    // state across journal I/O; the immutable flight remains separate from edits.
    async fn flush_inner(&self, target: u64) -> Result<(), WorkspaceError> {
        let wait = self.journal.profile.span(Stage::FlushWait);
        let _driver = self.flush.lock().await;
        drop(wait);
        loop {
            let (batch, confirmed) = {
                let wait = self.journal.profile.span(Stage::FreezeStateWait);
                let state = self.state.clone().lock_owned().await;
                drop(wait);
                if state.in_flight.is_none() {
                    if state.pending.operations == 0 {
                        if self.checkpoint_needed.load(Ordering::Acquire) {
                            let next = state.clone();
                            let _install = self.journal.profile.span(Stage::AckInstall);
                            self.checkpoint(state, next).await?;
                        }
                        return Ok(());
                    }
                    let mut next = state.clone();
                    next.in_flight = Some(std::mem::take(&mut next.pending));
                    let batch = next.in_flight.clone().unwrap();
                    let confirmed = next.confirmed.clone();
                    let _install = self.journal.profile.span(Stage::FreezeInstall);
                    self.checkpoint(state, next).await?;
                    (batch, confirmed)
                } else {
                    let batch = state.in_flight.clone().unwrap();
                    let confirmed = state.confirmed.clone();
                    // A previous failed freeze must reach disk before sending.
                    if self.checkpoint_needed.load(Ordering::Acquire) {
                        let next = state.clone();
                        let _install = self.journal.profile.span(Stage::FreezeInstall);
                        self.checkpoint(state, next).await?;
                    }
                    (batch, confirmed)
                }
            };
            let receipt = batch.receipt()?;
            // A lost commit reply or a crash after remote commit is resolved by
            // its receipt, written in the SAME CAS as the metadata and head.
            let receipt_timer = self.journal.profile.span(Stage::ReceiptRead);
            let remote_receipt = read_receipt(self.remote.as_ref(), &self.receipt_key).await?;
            drop(receipt_timer);
            if remote_receipt != Some(receipt.clone()) {
                let mut checks: Vec<_> = batch
                    .checks
                    .iter()
                    .map(|(key, expected)| KvCheck {
                        key: key.clone(),
                        expected: expected.clone(),
                    })
                    .collect();
                checks.push(KvCheck {
                    key: self.receipt_key.clone(),
                    expected: confirmed,
                });
                let mut writes: Vec<_> = batch
                    .writes
                    .iter()
                    .map(|(key, value)| match value {
                        Some(value) => KvWrite::Put {
                            key: key.clone(),
                            value: value.clone(),
                        },
                        None => KvWrite::Delete { key: key.clone() },
                    })
                    .collect();
                writes.push(KvWrite::Put {
                    key: self.receipt_key.clone(),
                    value: receipt.clone(),
                });
                let commit_timer = self.journal.profile.span(Stage::RemoteCommit);
                let result = self
                    .remote
                    .compare_and_swap_before(&checks, &writes, batch.deadline)
                    .await;
                drop(commit_timer);
                match result {
                    Ok(true) => {}
                    result => {
                        if read_receipt(self.remote.as_ref(), &self.receipt_key).await?
                            != Some(receipt.clone())
                        {
                            return result.and(Err(WorkspaceError::Fenced));
                        }
                    }
                }
            }
            let wait = self.journal.profile.span(Stage::AckStateWait);
            let state = self.state.clone().lock_owned().await;
            drop(wait);
            let mut next = state.clone();
            // Only this batch is retired. Never clear pending or its new values.
            next.in_flight = None;
            next.confirmed = Some(receipt);
            let install = self.journal.profile.span(Stage::AckInstall);
            self.checkpoint(state, next).await?;
            drop(install);
            tracing::debug!(
                sequence = batch.sequence,
                operations = batch.operations,
                keys = batch.writes.len(),
                "local metadata batch committed"
            );
            if batch.sequence >= target {
                return Ok(());
            }
        }
    }

    async fn flush(self: &Arc<Self>) -> Result<(), WorkspaceError> {
        let target = self.state.lock().await.sequence;
        let result = self.flush_to(target).await;
        self.failed.store(result.is_err(), Ordering::Release);
        result
    }
}

/// Wrap the actual backend once, at mount construction. No journal in remote
/// mode; object identity and all native budget registrations stay stable.
pub struct LocalMetadataBackend {
    remote: Arc<dyn WorkspaceKvBackend>,
    core: Option<Arc<Core>>,
}

impl LocalMetadataBackend {
    pub async fn new<B: WorkspaceKvBackend>(
        remote: B,
        config: Option<LocalMetadataConfig>,
        scope: &[u8],
    ) -> Result<Self, WorkspaceError> {
        let remote: Arc<dyn WorkspaceKvBackend> = Arc::new(remote);
        let Some(config) = config else {
            return Ok(Self { remote, core: None });
        };
        let scope = Sha256::digest(scope).to_vec();
        let (journal, mut state) = Journal::open(&config, &scope)?;
        let receipt_key = format!("local-metadata/{}", hex::encode(&scope)).into_bytes();
        // A clean journal may return after this workspace ran on another
        // machine. Its old receipt no longer fences any pending local work.
        // Refresh only when clean; dirty recovery must retain its exact receipt.
        if !state.dirty() {
            state.confirmed = read_receipt(remote.as_ref(), &receipt_key).await?;
        }
        let core = Arc::new(Core {
            remote: remote.clone(),
            config,
            journal: Arc::new(journal),
            state: Arc::new(Mutex::new(state)),
            admission: Mutex::new(()),
            flush: Mutex::new(()),
            receipt_key,
            failed: AtomicBool::new(false),
            checkpoint_needed: AtomicBool::new(false),
        });
        // Recover before lease acquisition/lifecycle mutations. Expired or
        // changed authority fails closed, preserving the journal for recovery.
        core.flush().await?;
        let weak = Arc::downgrade(&core);
        let interval = core.config.flush_interval;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(core) = weak.upgrade() else {
                    break;
                };
                let dirty = core.state.lock().await.dirty()
                    || core.checkpoint_needed.load(Ordering::Acquire);
                if dirty && let Err(error) = core.flush().await {
                    tracing::warn!(%error, "metadata sync retained pending journal; remote barrier will report failure");
                    drop(core);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        });
        Ok(Self {
            remote,
            core: Some(core),
        })
    }

    async fn barrier(&self) -> Result<Option<tokio::sync::MutexGuard<'_, ()>>, WorkspaceError> {
        let Some(core) = &self.core else {
            return Ok(None);
        };
        let admission = core.admission.lock().await;
        core.flush().await?;
        Ok(Some(admission))
    }

    async fn overlay_values(
        &self,
        keys: &[Vec<u8>],
        limits: Option<KvReadLimits>,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        let wait = self
            .core
            .as_ref()
            .map(|c| c.journal.profile.span(Stage::ReadStateWait));
        let state = match &self.core {
            Some(core) => Some(core.state.lock().await),
            None => None,
        };
        drop(wait);
        let read = self
            .core
            .as_ref()
            .map(|c| c.journal.profile.span(Stage::ReadRemote));
        let (mut values, now) = match limits {
            Some(limits) => {
                self.remote
                    .get_many_consistent_with_time_bounded(keys, limits)
                    .await?
            }
            None => self.remote.get_many_consistent_with_time(keys).await?,
        };
        drop(read);
        if values.len() != keys.len() {
            return Err(codec("metadata read cardinality mismatch"));
        }
        if let Some(state) = state {
            for (key, value) in keys.iter().zip(&mut values) {
                if let Some(local) = state.overlay(key) {
                    *value = local.clone();
                }
            }
        }
        if let Some(limits) = limits {
            let mut total = 0usize;
            for (key, value) in keys.iter().zip(&values) {
                let size = value.as_ref().map_or(0, Vec::len);
                total = total
                    .checked_add(key.len() + size)
                    .ok_or_else(|| invalid("local metadata read overflow"))?;
                if size > limits.max_value_bytes || total > limits.max_total_bytes {
                    return Err(invalid("local metadata read exceeds caller byte budget"));
                }
            }
        }
        Ok((values, now))
    }

    // Preserve the caller's read contract. Plain/consistent reads do not ask
    // for a lease clock: on Redis, adding one turns MGET into TIME + MGET inside
    // MULTI/EXEC. Mutation validation and timed reads still use the timed path.
    async fn overlay_untimed(
        &self,
        keys: &[Vec<u8>],
        consistent: bool,
    ) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        let wait = self
            .core
            .as_ref()
            .map(|c| c.journal.profile.span(Stage::ReadStateWait));
        let state = match &self.core {
            Some(core) => Some(core.state.lock().await),
            None => None,
        };
        drop(wait);
        let read = self
            .core
            .as_ref()
            .map(|c| c.journal.profile.span(Stage::ReadRemote));
        let mut values = if consistent {
            self.remote.get_many_consistent(keys).await?
        } else {
            self.remote.get_many(keys).await?
        };
        drop(read);
        if values.len() != keys.len() {
            return Err(codec("metadata read cardinality mismatch"));
        }
        if let Some(state) = state {
            for (key, value) in keys.iter().zip(&mut values) {
                if let Some(local) = state.overlay(key) {
                    *value = local.clone();
                }
            }
        }
        Ok(values)
    }
}

#[async_trait]
impl WorkspaceKvBackend for LocalMetadataBackend {
    fn name(&self) -> &'static str {
        self.remote.name()
    }
    fn supports_consistent_reads(&self) -> bool {
        self.remote.supports_consistent_reads()
    }
    fn short_scan_page_is_end(&self) -> bool {
        self.remote.short_scan_page_is_end()
    }
    fn scan_page_record_limit(&self, limits: KvReadLimits) -> Result<usize, WorkspaceError> {
        self.remote.scan_page_record_limit(limits)
    }
    fn native_gc_metadata_page_quota(&self) -> Option<usize> {
        self.remote.native_gc_metadata_page_quota()
    }
    async fn authenticate_gc_admin(&self) -> Result<(), WorkspaceError> {
        self.remote.authenticate_gc_admin().await
    }
    async fn flush_metadata(&self) -> Result<(), WorkspaceError> {
        let _guard = self.barrier().await?;
        Ok(())
    }
    async fn shutdown_metadata_backend(&self) -> Result<(), WorkspaceError> {
        let _guard = self.barrier().await?;
        if let Some(core) = &self.core {
            core.journal.profile.emit();
        }
        self.remote.shutdown_metadata_backend().await
    }

    async fn compare_and_swap_metadata_before(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        deadline: i64,
    ) -> Result<bool, WorkspaceError> {
        let Some(core) = &self.core else {
            return self
                .remote
                .compare_and_swap_before(checks, writes, deadline)
                .await;
        };
        let _update = core.journal.profile.span(Stage::Update);
        let wait = core.journal.profile.span(Stage::AdmissionWait);
        let _admission = core.admission.lock().await;
        drop(wait);
        if core.failed.load(Ordering::Acquire) {
            core.flush().await?;
        }
        // Retain the existing bounded transport contract. Oversized packets
        // use the original synchronous path rather than an unbounded read.
        if checks.is_empty()
            || checks.len() > 32
            || checks.iter().any(|c| {
                c.key.len() > 1024 || c.expected.as_ref().is_some_and(|v| v.len() > 96 << 10)
            })
        {
            core.flush().await?;
            return self
                .remote
                .compare_and_swap_before(checks, writes, deadline)
                .await;
        }
        loop {
            let wait = core.journal.profile.span(Stage::UpdateStateWait);
            let state = core.state.clone().lock_owned().await;
            drop(wait);
            let merge = core.journal.profile.span(Stage::Merge);
            let sequence = state
                .sequence
                .checked_add(1)
                .ok_or_else(|| invalid("metadata sequence exhausted"))?;
            let mut next = state.clone();
            next.pending.merge(sequence, checks, writes, deadline);
            drop(merge);
            if next.pending.operations > core.config.max_operations
                || next.pending.bytes() > core.config.max_bytes
                || next.pending.checks.len() + next.pending.writes.len() > 8192
            {
                drop(state);
                core.flush().await?;
                // A single large operation stays synchronous; it must not
                // overflow the local queue or relax the catalog packet bound.
                let mut single = Batch::default();
                single.merge(sequence, checks, writes, deadline);
                if single.bytes() > core.config.max_bytes
                    || single.checks.len() + single.writes.len() > 8192
                {
                    return self
                        .remote
                        .compare_and_swap_before(checks, writes, deadline)
                        .await;
                }
                continue;
            }
            let keys: Vec<_> = checks.iter().map(|c| c.key.clone()).collect();
            let validate = core.journal.profile.span(Stage::ValidateRemote);
            let (values, now) = self
                .remote
                .get_many_consistent_with_time_bounded(
                    &keys,
                    KvReadLimits {
                        max_records: 32,
                        max_key_bytes: 1024,
                        max_value_bytes: 96 << 10,
                        max_total_bytes: 4 << 20,
                        max_response_bytes: 4 << 20,
                        max_data_requests: 32,
                    },
                )
                .await?;
            drop(validate);
            if now >= deadline {
                return Err(WorkspaceError::Fenced);
            }
            if values.len() != checks.len() {
                return Err(codec("metadata check cardinality mismatch"));
            }
            for (check, remote) in checks.iter().zip(values) {
                let actual = state.overlay(&check.key).unwrap_or(&remote);
                if actual != &check.expected {
                    return Ok(false);
                }
            }
            next.sequence = sequence;
            // Journal mode ACKs durable local state; buffered mode ACKs memory.
            // Both modes persist the immutable batch BEFORE remote dispatch.
            let _install = core.journal.profile.span(Stage::UpdateInstall);
            core.install(state, next, core.config.sync_on_update)
                .await?;
            return Ok(true);
        }
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        if let Some(core) = &self.core {
            let wait = core.journal.profile.span(Stage::ReadStateWait);
            let state = core.state.lock().await;
            drop(wait);
            if let Some(value) = state.overlay(key) {
                return Ok(value.clone());
            }
            let _remote = core.journal.profile.span(Stage::ReadRemote);
            return self.remote.get(key).await;
        }
        self.remote.get(key).await
    }
    async fn get_many(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        if self.core.is_none() {
            return self.remote.get_many(keys).await;
        }
        self.overlay_untimed(keys, false).await
    }
    async fn get_many_consistent(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        if self.core.is_none() {
            return self.remote.get_many_consistent(keys).await;
        }
        self.overlay_untimed(keys, true).await
    }
    async fn get_many_consistent_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        if self.core.is_none() {
            return self.remote.get_many_consistent_with_time(keys).await;
        }
        self.overlay_values(keys, None).await
    }
    async fn get_many_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        if self.core.is_none() {
            return self.remote.get_many_with_time(keys).await;
        }
        self.overlay_values(keys, None).await
    }
    async fn get_many_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        if self.core.is_none() {
            return self
                .remote
                .get_many_consistent_with_time_bounded(keys, limits)
                .await;
        }
        self.overlay_values(keys, Some(limits)).await
    }
    async fn get_publication_packet_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        let _guard = self.barrier().await?;
        self.remote
            .get_publication_packet_consistent_with_time_bounded(keys, limits)
            .await
    }
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        let _guard = self.barrier().await?;
        self.remote.scan_prefix(prefix).await
    }
    async fn scan_prefix_bounded(
        &self,
        prefix: &[u8],
        max_records: usize,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        let _guard = self.barrier().await?;
        self.remote.scan_prefix_bounded(prefix, max_records).await
    }
    async fn scan_prefix_with_byte_limits(
        &self,
        prefix: &[u8],
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        let _guard = self.barrier().await?;
        self.remote
            .scan_prefix_with_byte_limits(prefix, limits)
            .await
    }
    async fn scan_prefix_page_with_byte_limits(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        let _guard = self.barrier().await?;
        self.remote
            .scan_prefix_page_with_byte_limits(prefix, after, limits)
            .await
    }
    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        let _guard = self.barrier().await?;
        self.remote.compare_and_swap(checks, writes).await
    }
    async fn compare_and_swap_before(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        deadline: i64,
    ) -> Result<bool, WorkspaceError> {
        let _guard = self.barrier().await?;
        self.remote
            .compare_and_swap_before(checks, writes, deadline)
            .await
    }
    async fn compare_and_swap_in_empty_namespace(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        deadline: i64,
    ) -> Result<bool, WorkspaceError> {
        let _guard = self.barrier().await?;
        self.remote
            .compare_and_swap_in_empty_namespace(checks, writes, deadline)
            .await
    }
    async fn compare_and_swap_in_time_window(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        after: Option<i64>,
        before: Option<i64>,
    ) -> Result<bool, WorkspaceError> {
        let _guard = self.barrier().await?;
        self.remote
            .compare_and_swap_in_time_window(checks, writes, after, before)
            .await
    }
    async fn authenticate_checks_before_bounded(
        &self,
        checks: &[KvCheck],
        deadline: i64,
        limits: KvReadLimits,
    ) -> Result<bool, WorkspaceError> {
        if let Some(core) = &self.core {
            let wait = core.journal.profile.span(Stage::AuthStateWait);
            let state = core.state.lock().await;
            drop(wait);
            let mut translated = checks.to_vec();
            let mut complete = true;
            for check in &mut translated {
                if let Some(local) = state.overlay(&check.key) {
                    if local != &check.expected {
                        return Ok(false);
                    }
                    // Authenticate the virtual head against its ORIGINAL remote
                    // head, keeping the lease and authority checks unchanged.
                    // A racing commit may return false; a foreign head or an
                    // expired lease must never authenticate this local view.
                    let first = state
                        .in_flight
                        .as_ref()
                        .filter(|b| b.writes.contains_key(&check.key));
                    let batch = first.unwrap_or(&state.pending);
                    if let Some(expected) = batch.checks.get(&check.key) {
                        check.expected = expected.clone();
                    } else {
                        complete = false;
                        break;
                    }
                }
            }
            if complete {
                super::kv_backend::validate_bounded_authentication_checks(checks, limits)?;
                let _remote = core.journal.profile.span(Stage::AuthRemote);
                return self
                    .remote
                    .authenticate_checks_before_bounded(&translated, deadline, limits)
                    .await;
            }
        }
        // A newly inserted key has no original exact check: commit it before
        // authenticating its remote value.
        let _guard = self.barrier().await?;
        self.remote
            .authenticate_checks_before_bounded(checks, deadline, limits)
            .await
    }
    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        self.remote.server_time_ns().await
    }
}

#[cfg(test)]
mod tests;
