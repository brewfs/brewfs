//! Durable local acknowledgement for entity objects only. Recovery uploads the
//! original immutable object; it never replays an inode, chunk or namespace edit.
//! Remote debt excludes maintenance even after the originating process dies.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::backend::{CommitOutcome, EntityBackend, EntityPlan, RowWrite, TransactionAuthority};
use super::codec::{decode, encode};
use super::data::{MAX_IO_BYTES, ObjectDescriptor};
use super::keys::EntityKeys;
use super::lifecycle::{Lifecycle, NamespaceState, WriterSession};
use super::model::ENTITY_WRITEBACK_FORMAT;
use super::ownership::{SliceOwnership, SliceState};
use super::{EntityError, Result};
use crate::cadapter::client::ObjectBackend;

#[derive(Clone, Debug)]
pub struct WritebackConfig {
    /// Dedicated persistent Linux filesystem directory, retained across restarts.
    pub directory: PathBuf,
    pub capacity_bytes: u64,
    pub min_free_bytes: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct WritebackStatus {
    pub pending_objects: u64,
    pub pending_bytes: u64,
    pub uploaded_bytes: u64,
    pub retries: u64,
    pub last_error: Option<String>,
    pub staging_file_syncs: u64,
    pub staging_io_ns: u64,
    pub stage_debt_attempts: u64,
    pub completion_debt_attempts: u64,
    pub completed_batches: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Debt {
    objects: u64,
    bytes: u64,
}

pub struct EntityWriteback<B, O> {
    backend: Arc<B>,
    objects: Arc<O>,
    keys: EntityKeys,
    config: WritebackConfig,
    // Lifetime lock prevents a second process from replaying/removing this spool.
    _lock: File,
    // Shared by independent staging/read operations; exclusive only for final
    // orphan reconciliation. No directory scan runs in the steady-state loop.
    local: Arc<tokio::sync::RwLock<()>>,
    // Serializes only this process's counter transactions, never filesystem or
    // object transport I/O. Redis CAS still fences other processes/maintenance.
    debt: tokio::sync::Mutex<()>,
    uploader: tokio::sync::Mutex<()>,
    changed: tokio::sync::Notify,
    used: AtomicU64,
    staging_reservations: AtomicU64,
    staging_file_syncs: AtomicU64,
    staging_io_ns: AtomicU64,
    stage_debt_attempts: AtomicU64,
    completion_debt_attempts: AtomicU64,
    completed_batches: AtomicU64,
    uploaded: AtomicU64,
    retries: AtomicU64,
    last_error: Mutex<Option<String>>,
}

fn io(error: impl std::fmt::Display) -> EntityError {
    EntityError::Backend(format!("entity spool: {error}"))
}

struct DiskReservation<'a> {
    used: &'a AtomicU64,
    pending: &'a AtomicU64,
    bytes: u64,
    retain: bool,
}

impl Drop for DiskReservation<'_> {
    fn drop(&mut self) {
        self.pending.fetch_sub(self.bytes, Ordering::AcqRel);
        if !self.retain {
            self.used.fetch_sub(self.bytes, Ordering::AcqRel);
        }
    }
}
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(io)
}

fn authenticate(descriptor: &ObjectDescriptor, bytes: &[u8]) -> Result<()> {
    if descriptor.size == 0
        || descriptor.size > MAX_IO_BYTES as u64
        || bytes.len() as u64 != descriptor.size
        || <[u8; 32]>::from(Sha256::digest(bytes)) != descriptor.sha256
    {
        return Err(EntityError::Corrupt(
            "missing, truncated or corrupt entity staging data".into(),
        ));
    }
    Ok(())
}

impl<B: EntityBackend + 'static, O: ObjectBackend + 'static> EntityWriteback<B, O> {
    pub fn open(
        backend: Arc<B>,
        objects: Arc<O>,
        keys: EntityKeys,
        mut config: WritebackConfig,
    ) -> Result<Arc<Self>> {
        if config.capacity_bytes == 0 || !objects.forbids_mutation_replay() {
            return Err(EntityError::Invalid(
                "entity staging requires capacity and a nonreplaying object transport".into(),
            ));
        }
        std::fs::create_dir_all(&config.directory).map_err(io)?;
        config.directory = std::fs::canonicalize(&config.directory).map_err(io)?;
        // An explicit namespace directory also prevents cross-volume SID collisions.
        config
            .directory
            .push(format!("{}-{}", keys.volume_id(), keys.namespace_id()));
        std::fs::create_dir_all(&config.directory).map_err(io)?;
        // A newly created cache root also needs its entry persisted in every
        // ancestor; syncing only the namespace directory could lose the entire
        // spool hierarchy after a power failure.
        for parent in config.directory.ancestors() {
            sync_directory(parent)?;
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(config.directory.join("lock"))
            .map_err(io)?;
        lock.try_lock().map_err(|_| EntityError::Busy)?;
        let mut used = 0u64;
        for entry in std::fs::read_dir(&config.directory).map_err(io)? {
            used = used
                .checked_add(entry.map_err(io)?.metadata().map_err(io)?.len())
                .ok_or(EntityError::Overflow)?;
        }
        Ok(Arc::new(Self {
            backend,
            objects,
            keys,
            config,
            _lock: lock,
            local: Arc::new(tokio::sync::RwLock::new(())),
            debt: tokio::sync::Mutex::new(()),
            uploader: tokio::sync::Mutex::new(()),
            changed: tokio::sync::Notify::new(),
            used: AtomicU64::new(used),
            staging_reservations: AtomicU64::new(0),
            staging_file_syncs: AtomicU64::new(0),
            staging_io_ns: AtomicU64::new(0),
            stage_debt_attempts: AtomicU64::new(0),
            completion_debt_attempts: AtomicU64::new(0),
            completed_batches: AtomicU64::new(0),
            uploaded: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            last_error: Mutex::new(None),
        }))
    }

    pub fn start(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                let Some(queue) = weak.upgrade() else { break };
                match queue.upload_batch().await {
                    Ok(count) if count != 0 => {
                        // A busy queue must not pay a fixed sleep per 64 objects.
                        tokio::task::yield_now().await;
                        continue;
                    }
                    Err(error) => {
                        queue.retries.fetch_add(1, Ordering::Relaxed);
                        *queue.last_error.lock().unwrap() = Some(error.to_string());
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Ok(_) => {
                        tokio::select! {
                            _ = queue.changed.notified() => {},
                            _ = tokio::time::sleep(Duration::from_millis(100)) => {},
                        }
                    }
                }
                drop(queue);
            }
        });
    }

    /// Upgrades only this opt-in namespace. Old clients fail descriptor decoding
    /// before they can reclaim source-only objects or mount it synchronously.
    pub async fn enable(&self, session: &WriterSession) -> Result<()> {
        let lifecycle = Lifecycle::new(self.backend.clone(), self.keys.clone());
        for _ in 0..32 {
            let authority = lifecycle.authority(session).await?;
            let mut descriptor = lifecycle.descriptor().await?;
            descriptor.format = ENTITY_WRITEBACK_FORMAT.into();
            if self
                .backend
                .commit(
                    &authority,
                    &EntityPlan {
                        checks: vec![],
                        writes: vec![RowWrite::Put {
                            key: self.keys.descriptor(),
                            value: encode(&descriptor)?,
                        }],
                    },
                )
                .await?
                == CommitOutcome::Committed
            {
                return Ok(());
            }
        }
        Err(EntityError::Busy)
    }

    fn path(&self, sid: u64) -> PathBuf {
        self.config.directory.join(format!("{sid}.stage"))
    }
    fn debt_key(&self, sid: u64) -> Vec<u8> {
        [self.keys.prefix(), format!("upload/{sid}").as_bytes()].concat()
    }

    fn load(&self, sid: u64) -> Result<Option<(ObjectDescriptor, Vec<u8>)>> {
        let mut file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(self.path(sid))
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(io(error)),
        };
        if file.metadata().map_err(io)?.len() > MAX_IO_BYTES as u64 + 4096 {
            return Err(EntityError::Corrupt("oversized staging file".into()));
        }
        let mut length = [0; 4];
        file.read_exact(&mut length).map_err(io)?;
        let length = u32::from_le_bytes(length) as usize;
        if length > 4096 {
            return Err(EntityError::Corrupt("staging header bound".into()));
        }
        let mut header = vec![0; length];
        file.read_exact(&mut header).map_err(io)?;
        let descriptor: ObjectDescriptor = decode(&header)?;
        if descriptor.namespace_id != self.keys.namespace_id() || descriptor.slice_id != sid {
            return Err(EntityError::Corrupt("foreign staging identity".into()));
        }
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(io)?;
        authenticate(&descriptor, &bytes)?;
        Ok(Some((descriptor, bytes)))
    }

    async fn load_async(self: &Arc<Self>, sid: u64) -> Result<Option<(ObjectDescriptor, Vec<u8>)>> {
        let local = self.local.clone().read_owned().await;
        let queue = self.clone();
        tokio::task::spawn_blocking(move || {
            let _local = local;
            queue.load(sid)
        })
        .await
        .map_err(io)?
    }

    pub async fn read(self: &Arc<Self>, descriptor: &ObjectDescriptor) -> Result<Option<Vec<u8>>> {
        match self.load_async(descriptor.slice_id).await? {
            Some((stored, bytes)) if stored == *descriptor => Ok(Some(bytes)),
            Some(_) => Err(EntityError::Corrupt("staging descriptor mismatch".into())),
            None => Ok(None),
        }
    }

    pub async fn stage(
        self: &Arc<Self>,
        session: &WriterSession,
        descriptor: &ObjectDescriptor,
        bytes: &[u8],
    ) -> Result<()> {
        if descriptor.namespace_id != self.keys.namespace_id()
            || descriptor.slice_id == 0
            || descriptor.slice_id > i64::MAX as u64
        {
            return Err(EntityError::Fenced);
        }
        let local = self.local.clone().read_owned().await;
        let queue = self.clone();
        let stored = descriptor.clone();
        let bytes = bytes.to_vec();
        // Keep the shared reconciliation guard in the blocking task even if
        // its caller is cancelled. File writes/fsync/hash must not stall the
        // CLI's default single Tokio worker or unrelated inode operations.
        let _local = tokio::task::spawn_blocking(move || {
            let start = Instant::now();
            queue.persist(&stored, &bytes)?;
            queue
                .staging_io_ns
                .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            Ok::<_, EntityError>(local)
        })
        .await
        .map_err(io)??;
        let _debt = self.debt.lock().await;
        // Persist debt before issuing a publication receipt. Ambiguous commits
        // leave both file and marker intact for upload-only recovery.
        let lifecycle = Lifecycle::new(self.backend.clone(), self.keys.clone());
        for _ in 0..64 {
            let mut combined = lifecycle.authority_keys(session)?;
            let authority_count = combined.len();
            let keys = [
                self.keys.remote_pending(),
                self.debt_key(descriptor.slice_id),
                self.keys.slice(descriptor.slice_id),
            ];
            combined.extend(keys.iter().cloned());
            let mut snapshot = self.backend.read(&combined).await?;
            let rows = snapshot.rows.split_off(authority_count);
            let authority = lifecycle.authority_from_snapshot(session, snapshot)?;
            let owner: SliceOwnership = decode(rows[2].as_deref().ok_or(EntityError::Fenced)?)?;
            owner.validate(&self.keys, descriptor.slice_id)?;
            if owner.size != descriptor.size
                || !matches!(owner.state, SliceState::Pending { lease, .. } if lease == session.identity())
            {
                return Err(EntityError::Fenced);
            }
            if let Some(stored) = &rows[1] {
                if decode::<ObjectDescriptor>(stored)? == *descriptor {
                    self.changed.notify_one();
                    return Ok(());
                }
                return Err(EntityError::Corrupt("conflicting upload debt".into()));
            }
            let mut debt: Debt = rows[0].as_deref().map(decode).transpose()?.unwrap_or(Debt {
                objects: 0,
                bytes: 0,
            });
            debt.objects = debt.objects.checked_add(1).ok_or(EntityError::Overflow)?;
            debt.bytes = debt
                .bytes
                .checked_add(descriptor.size)
                .ok_or(EntityError::Overflow)?;
            let plan = EntityPlan {
                checks: super::lifecycle::checked_rows(&keys, rows),
                writes: vec![
                    RowWrite::Put {
                        key: keys[0].clone(),
                        value: encode(&debt)?,
                    },
                    RowWrite::Put {
                        key: keys[1].clone(),
                        value: encode(descriptor)?,
                    },
                ],
            };
            self.stage_debt_attempts.fetch_add(1, Ordering::Relaxed);
            if self.backend.commit(&authority, &plan).await? == CommitOutcome::Committed {
                self.changed.notify_one();
                return Ok(());
            }
        }
        Err(EntityError::Busy)
    }

    fn persist(&self, descriptor: &ObjectDescriptor, bytes: &[u8]) -> Result<()> {
        authenticate(descriptor, bytes)?;
        let header = encode(descriptor)?;
        if header.len() + 4 > 4096 {
            return Err(EntityError::Invalid("staging header bound".into()));
        }
        let required = bytes.len() as u64 + 4096;
        if self
            .used
            .try_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(required)
                    .filter(|next| *next <= self.config.capacity_bytes)
            })
            .is_err()
        {
            return Err(EntityError::Busy);
        }
        let pending = self
            .staging_reservations
            .fetch_add(required, Ordering::AcqRel);
        let mut reservation = DiskReservation {
            used: &self.used,
            pending: &self.staging_reservations,
            bytes: required,
            retain: false,
        };
        let free = nix_free_bytes(&self.config.directory)?;
        // Reserve space for concurrent writes not yet reflected by statvfs.
        if free.saturating_sub(pending.saturating_add(required)) < self.config.min_free_bytes {
            return Err(EntityError::Busy);
        }
        let path = self.path(descriptor.slice_id);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&path)
            .map_err(io)?;
        reservation.retain = true;
        // Once a file exists, errors retain its reservation conservatively.
        // Reopen reconstructs usage from disk, including partial orphan files.
        file.write_all(&(header.len() as u32).to_le_bytes())
            .map_err(io)?;
        file.write_all(&header).map_err(io)?;
        file.write_all(bytes).map_err(io)?;
        file.sync_all().map_err(io)?;
        // No reader/uploader knows this fresh SID until debt is registered.
        // create_new prevents replacing another object's immutable bytes. A
        // pre-debt crash leaves only an unregistered, reclaimable orphan.
        sync_directory(&self.config.directory)?;
        self.used.fetch_sub(
            required - (4 + header.len() + bytes.len()) as u64,
            Ordering::AcqRel,
        );
        self.staging_file_syncs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// The persistent upload row is a capability for this exact immutable PUT,
    /// independent of lease expiry. No inode/dentry/chunk mutation is authorized.
    pub async fn upload_pass(self: &Arc<Self>) -> Result<()> {
        self.upload_batch().await?;
        // Explicit upload-only recovery also reconciles a crash after debt
        // completion but before file removal, including an initially empty
        // queue. The live background loop deliberately calls upload_batch.
        if self.status().await?.pending_objects == 0 {
            let _uploader = self.uploader.lock().await;
            self.cleanup_completed().await?;
        }
        Ok(())
    }

    async fn upload_batch(self: &Arc<Self>) -> Result<usize> {
        let _uploader = self.uploader.lock().await;
        let debt = self.debt.lock().await;
        let prefix = [self.keys.prefix(), b"upload/"].concat();
        let rows = self.backend.scan_prefix_batch(&prefix, 64).await?;
        if rows.is_empty() && self.status().await?.pending_objects != 0 {
            return Err(EntityError::Corrupt(
                "remote debt has no upload records".into(),
            ));
        }
        drop(debt);
        let descriptors = rows
            .into_iter()
            .map(|(_, stored)| decode(&stored))
            .collect::<Result<Vec<ObjectDescriptor>>>()?;
        let count = descriptors.len();
        // At most eight authenticated payloads (8 * MAX_IO_BYTES) and PUTs are
        // retained. Finish every submitted future, even when one object fails.
        let results = stream::iter(descriptors.into_iter().map(|descriptor| async move {
            let result = self.upload_one(&descriptor).await;
            (descriptor, result)
        }))
        .buffer_unordered(8)
        .collect::<Vec<_>>()
        .await;
        let mut completed = Vec::new();
        let mut failure = None;
        for (descriptor, result) in results {
            match result {
                Ok(()) => completed.push(descriptor),
                Err(error) => {
                    failure.get_or_insert(error);
                }
            }
        }
        if !completed.is_empty() {
            self.finish_batch(&completed).await?;
            let local = self.local.clone().read_owned().await;
            let queue = self.clone();
            tokio::task::spawn_blocking(move || {
                let _local = local;
                let mut result = Ok(());
                for descriptor in completed {
                    if let Err(error) = queue.remove_stage(descriptor.slice_id) {
                        result = Err(error);
                        break;
                    }
                    queue.uploaded.fetch_add(descriptor.size, Ordering::Relaxed);
                }
                // Completed objects are already remotely durable. One directory
                // sync persists this entire reclamation batch; a crash earlier
                // leaves only harmless files for final reconciliation.
                sync_directory(&queue.config.directory)?;
                result
            })
            .await
            .map_err(io)??;
            self.completed_batches.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(error) = failure {
            return Err(error);
        }
        *self.last_error.lock().unwrap() = None;
        Ok(count)
    }

    fn remove_stage(&self, sid: u64) -> Result<()> {
        let path = self.path(sid);
        let size = std::fs::metadata(&path).map_err(io)?.len();
        std::fs::remove_file(path).map_err(io)?;
        // Failed staging may have conservatively retained extra reservation;
        // subtracting the actual file length cannot undercount retained files.
        self.used.fetch_sub(size, Ordering::AcqRel);
        Ok(())
    }

    async fn upload_one(self: &Arc<Self>, descriptor: &ObjectDescriptor) -> Result<()> {
        self.validate_upload(descriptor).await?;
        let (local, bytes) = self
            .load_async(descriptor.slice_id)
            .await?
            .ok_or_else(|| EntityError::Corrupt("remote debt has no staging file".into()))?;
        if local != *descriptor {
            return Err(EntityError::Corrupt("upload debt mismatch".into()));
        }
        let key = self.keys.object(descriptor.slice_id);
        // The transport guarantees create-only PUT without hidden replay.
        // Fresh IDs need one request. A collision or lost response must be
        // reconciled by exact size AND digest, never by existence alone.
        let put_error = match self
            .objects
            .put_object_create_only_durable(&key, &bytes)
            .await
        {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };
        match self
            .objects
            .get_object_size_bounded(&key)
            .await
            .map_err(io)?
        {
            Some(size) => {
                if size != descriptor.size {
                    return Err(EntityError::Corrupt("remote upload size mismatch".into()));
                }
                use futures::StreamExt;
                let mut stream = self
                    .objects
                    .get_object_range_stream(&key, 0, descriptor.size + 1)
                    .await
                    .map_err(io)?;
                let mut remote = Vec::new();
                while let Some(part) = stream.next().await {
                    let part = part.map_err(io)?;
                    if remote.len().saturating_add(part.len()) as u64 > descriptor.size {
                        return Err(EntityError::Corrupt("oversized upload verification".into()));
                    }
                    remote.extend_from_slice(&part);
                }
                authenticate(descriptor, &remote)?;
            }
            None => return Err(io(put_error)),
        }
        Ok(())
    }

    /// A crash after removing debt may leave a completed local file. Verify its
    /// original remote bytes before reclaiming it; never upload an unregistered
    /// orphan, since maintenance may already have retired its object identity.
    async fn cleanup_completed(&self) -> Result<()> {
        let _local = self.local.write().await;
        for entry in std::fs::read_dir(&self.config.directory).map_err(io)? {
            let path = entry.map_err(io)?.path();
            if path.extension().and_then(|s| s.to_str()) != Some("stage") {
                continue;
            }
            let sid: u64 = path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| EntityError::Corrupt("invalid staging filename".into()))?;
            let snapshot = self
                .backend
                .read(&[
                    self.debt_key(sid),
                    self.keys.object_descriptor(sid),
                    self.keys.slice(sid),
                ])
                .await?;
            if snapshot.rows[0].is_some() {
                continue;
            }
            let owner: SliceOwnership =
                decode(snapshot.rows[2].as_deref().ok_or(EntityError::Fenced)?)?;
            owner.validate(&self.keys, sid)?;
            if let Some(stored) = &snapshot.rows[1] {
                let descriptor: ObjectDescriptor = decode(stored)?;
                let (local, _) = self.load(sid)?.ok_or(EntityError::NotFound)?;
                if local != descriptor {
                    return Err(EntityError::Corrupt("completed staging descriptor".into()));
                }
                let key = self.keys.object(sid);
                use futures::StreamExt;
                let mut stream = self
                    .objects
                    .get_object_range_stream(&key, 0, descriptor.size + 1)
                    .await
                    .map_err(io)?;
                let mut bytes = Vec::new();
                while let Some(part) = stream.next().await {
                    let part = part.map_err(io)?;
                    if bytes.len().saturating_add(part.len()) as u64 > descriptor.size {
                        return Err(EntityError::Corrupt("completed object bound".into()));
                    }
                    bytes.extend_from_slice(&part);
                }
                authenticate(&descriptor, &bytes)?;
            } else if owner.current_refs != 0
                || !matches!(
                    owner.state,
                    SliceState::Pending { .. } | SliceState::Deleted { .. }
                )
            {
                return Err(EntityError::Corrupt("unexplained staging file".into()));
            }
            self.remove_stage(sid)?;
        }
        sync_directory(&self.config.directory)?;
        Ok(())
    }

    async fn validate_upload(&self, descriptor: &ObjectDescriptor) -> Result<()> {
        let keys = [
            self.keys.descriptor(),
            self.keys.slice(descriptor.slice_id),
            self.debt_key(descriptor.slice_id),
            self.keys.remote_pending(),
        ];
        let snapshot = self.backend.read(&keys).await?;
        let namespace: super::lifecycle::NamespaceDescriptor =
            decode(snapshot.rows[0].as_deref().ok_or(EntityError::Fenced)?)?;
        let owner: SliceOwnership =
            decode(snapshot.rows[1].as_deref().ok_or(EntityError::Fenced)?)?;
        owner.validate(&self.keys, descriptor.slice_id)?;
        if namespace.namespace_id != self.keys.namespace_id()
            || namespace.format != ENTITY_WRITEBACK_FORMAT
            || namespace.state != NamespaceState::Active
            || owner.size != descriptor.size
            || !matches!(
                owner.state,
                SliceState::Pending { .. } | SliceState::Available
            )
            || snapshot.rows[2].as_deref() != Some(encode(descriptor)?.as_slice())
            || snapshot.rows[3].is_none()
        {
            return Err(EntityError::Fenced);
        }
        Ok(())
    }

    async fn finish_batch(&self, descriptors: &[ObjectDescriptor]) -> Result<()> {
        let _debt = self.debt.lock().await;
        if descriptors.is_empty() || descriptors.len() > 64 {
            return Err(EntityError::Invalid("upload completion batch bound".into()));
        }
        let mut keys = vec![self.keys.descriptor(), self.keys.remote_pending()];
        let mut identities = std::collections::BTreeSet::new();
        for descriptor in descriptors {
            if descriptor.namespace_id != self.keys.namespace_id()
                || !identities.insert(descriptor.slice_id)
            {
                return Err(EntityError::Corrupt("upload completion identities".into()));
            }
            keys.push(self.debt_key(descriptor.slice_id));
        }
        for _ in 0..64 {
            let snapshot = self.backend.read(&keys).await?;
            let namespace: super::lifecycle::NamespaceDescriptor =
                decode(snapshot.rows[0].as_deref().ok_or(EntityError::Fenced)?)?;
            if namespace.namespace_id != self.keys.namespace_id()
                || namespace.format != ENTITY_WRITEBACK_FORMAT
                || namespace.state != NamespaceState::Active
            {
                return Err(EntityError::Fenced);
            }
            if snapshot.rows[2..].iter().all(Option::is_none) {
                return Ok(());
            }
            let mut debt: Debt = decode(snapshot.rows[1].as_deref().ok_or(EntityError::Fenced)?)?;
            let mut writes = Vec::new();
            for (index, descriptor) in descriptors.iter().enumerate() {
                let Some(stored) = snapshot.rows[index + 2].as_deref() else {
                    continue;
                };
                if stored != encode(descriptor)?.as_slice() {
                    return Err(EntityError::Corrupt("upload completion identity".into()));
                }
                debt.objects = debt.objects.checked_sub(1).ok_or(EntityError::Overflow)?;
                debt.bytes = debt
                    .bytes
                    .checked_sub(descriptor.size)
                    .ok_or(EntityError::Overflow)?;
                writes.push(RowWrite::Delete {
                    key: keys[index + 2].clone(),
                });
            }
            let checks = super::lifecycle::checked_rows(&keys, snapshot.rows);
            let authority = TransactionAuthority::from_validated(checks, None)?;
            let counter = if debt.objects == 0 {
                if debt.bytes != 0 {
                    return Err(EntityError::Corrupt("upload debt counter mismatch".into()));
                }
                RowWrite::Delete {
                    key: keys[1].clone(),
                }
            } else {
                RowWrite::Put {
                    key: keys[1].clone(),
                    value: encode(&debt)?,
                }
            };
            writes.push(counter);
            self.completion_debt_attempts
                .fetch_add(1, Ordering::Relaxed);
            if self
                .backend
                .commit(
                    &authority,
                    &EntityPlan {
                        checks: vec![],
                        writes,
                    },
                )
                .await?
                == CommitOutcome::Committed
            {
                return Ok(());
            }
        }
        Err(EntityError::Busy)
    }

    pub async fn status(&self) -> Result<WritebackStatus> {
        let snapshot = self.backend.read(&[self.keys.remote_pending()]).await?;
        let debt: Debt = snapshot.rows[0]
            .as_deref()
            .map(decode)
            .transpose()?
            .unwrap_or(Debt {
                objects: 0,
                bytes: 0,
            });
        Ok(WritebackStatus {
            pending_objects: debt.objects,
            pending_bytes: debt.bytes,
            uploaded_bytes: self.uploaded.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            last_error: self.last_error.lock().unwrap().clone(),
            staging_file_syncs: self.staging_file_syncs.load(Ordering::Relaxed),
            staging_io_ns: self.staging_io_ns.load(Ordering::Relaxed),
            stage_debt_attempts: self.stage_debt_attempts.load(Ordering::Relaxed),
            completion_debt_attempts: self.completion_debt_attempts.load(Ordering::Relaxed),
            completed_batches: self.completed_batches.load(Ordering::Relaxed),
        })
    }

    pub async fn drain(self: &Arc<Self>, timeout: Duration) -> Result<()> {
        self.changed.notify_one();
        tokio::time::timeout(timeout, async {
            loop {
                if self.status().await?.pending_objects == 0 {
                    let _uploader = self.uploader.lock().await;
                    if self.status().await?.pending_objects != 0 {
                        continue;
                    }
                    self.cleanup_completed().await?;
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .map_err(|_| EntityError::Busy)?
    }
}

#[cfg(test)]
#[path = "writeback_tests.rs"]
mod tests;

fn nix_free_bytes(path: &Path) -> Result<u64> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(io)?;
    let mut status = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: path is NUL terminated and status points to writable statvfs storage.
    if unsafe { libc::statvfs(path.as_ptr(), status.as_mut_ptr()) } != 0 {
        return Err(io(std::io::Error::last_os_error()));
    }
    // SAFETY: a successful statvfs initialized the entire structure.
    let status = unsafe { status.assume_init() };
    Ok(status.f_bavail.saturating_mul(status.f_frsize))
}
