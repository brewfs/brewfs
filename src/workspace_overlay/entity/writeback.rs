//! Durable local acknowledgement for entity objects only. Recovery uploads the
//! original immutable object; it never replays an inode, chunk or namespace edit.
//! Remote debt excludes maintenance even after the originating process dies.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

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
    local: tokio::sync::Mutex<()>,
    uploader: tokio::sync::Mutex<()>,
    uploaded: AtomicU64,
    retries: AtomicU64,
    last_error: Mutex<Option<String>>,
}

fn io(error: impl std::fmt::Display) -> EntityError {
    EntityError::Backend(format!("entity spool: {error}"))
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
        Ok(Arc::new(Self {
            backend,
            objects,
            keys,
            config,
            _lock: lock,
            local: tokio::sync::Mutex::new(()),
            uploader: tokio::sync::Mutex::new(()),
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
                if let Err(error) = queue.upload_pass().await {
                    queue.retries.fetch_add(1, Ordering::Relaxed);
                    *queue.last_error.lock().unwrap() = Some(error.to_string());
                }
                drop(queue);
                tokio::time::sleep(Duration::from_millis(100)).await;
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

    pub async fn read(&self, descriptor: &ObjectDescriptor) -> Result<Option<Vec<u8>>> {
        let _local = self.local.lock().await;
        match self.load(descriptor.slice_id)? {
            Some((stored, bytes)) if stored == *descriptor => Ok(Some(bytes)),
            Some(_) => Err(EntityError::Corrupt("staging descriptor mismatch".into())),
            None => Ok(None),
        }
    }

    pub async fn stage(
        &self,
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
        authenticate(descriptor, bytes)?;
        let _local = self.local.lock().await;
        let mut used = 0u64;
        for entry in std::fs::read_dir(&self.config.directory).map_err(io)? {
            used = used
                .checked_add(entry.map_err(io)?.metadata().map_err(io)?.len())
                .ok_or(EntityError::Overflow)?;
        }
        let required = bytes.len() as u64 + 4096;
        let free = nix_free_bytes(&self.config.directory)?;
        if used.saturating_add(required) > self.config.capacity_bytes
            || free.saturating_sub(required) < self.config.min_free_bytes
        {
            return Err(EntityError::Busy);
        }
        let path = self.path(descriptor.slice_id);
        let temporary = path.with_extension("tmp");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&temporary)
            .map_err(io)?;
        let header = encode(descriptor)?;
        file.write_all(&(header.len() as u32).to_le_bytes())
            .map_err(io)?;
        file.write_all(&header).map_err(io)?;
        file.write_all(bytes).map_err(io)?;
        file.sync_all().map_err(io)?;
        std::fs::rename(&temporary, &path).map_err(io)?;
        sync_directory(&self.config.directory)?;
        // Persist debt before issuing a publication receipt. Ambiguous commits
        // leave both file and marker intact for upload-only recovery.
        let lifecycle = Lifecycle::new(self.backend.clone(), self.keys.clone());
        for _ in 0..64 {
            let authority = lifecycle.authority(session).await?;
            let keys = [
                self.keys.remote_pending(),
                self.debt_key(descriptor.slice_id),
                self.keys.slice(descriptor.slice_id),
            ];
            let snapshot = self.backend.read(&keys).await?;
            let owner: SliceOwnership =
                decode(snapshot.rows[2].as_deref().ok_or(EntityError::Fenced)?)?;
            owner.validate(&self.keys, descriptor.slice_id)?;
            if owner.size != descriptor.size
                || !matches!(owner.state, SliceState::Pending { lease, .. } if lease == session.identity())
            {
                return Err(EntityError::Fenced);
            }
            if let Some(stored) = &snapshot.rows[1] {
                if decode::<ObjectDescriptor>(stored)? == *descriptor {
                    return Ok(());
                }
                return Err(EntityError::Corrupt("conflicting upload debt".into()));
            }
            let mut debt: Debt = snapshot.rows[0]
                .as_deref()
                .map(decode)
                .transpose()?
                .unwrap_or(Debt {
                    objects: 0,
                    bytes: 0,
                });
            debt.objects = debt.objects.checked_add(1).ok_or(EntityError::Overflow)?;
            debt.bytes = debt
                .bytes
                .checked_add(descriptor.size)
                .ok_or(EntityError::Overflow)?;
            let plan = EntityPlan {
                checks: super::lifecycle::checked_rows(&keys, snapshot.rows),
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
            if self.backend.commit(&authority, &plan).await? == CommitOutcome::Committed {
                return Ok(());
            }
        }
        Err(EntityError::Busy)
    }

    /// The persistent upload row is a capability for this exact immutable PUT,
    /// independent of lease expiry. No inode/dentry/chunk mutation is authorized.
    pub async fn upload_pass(&self) -> Result<()> {
        let _uploader = self.uploader.lock().await;
        let prefix = [self.keys.prefix(), b"upload/"].concat();
        let rows = self.backend.scan_prefix_batch(&prefix, 64).await?;
        if rows.is_empty() && self.status().await?.pending_objects != 0 {
            return Err(EntityError::Corrupt(
                "remote debt has no upload records".into(),
            ));
        }
        for (_, stored) in rows {
            let descriptor: ObjectDescriptor = decode(&stored)?;
            self.validate_upload(&descriptor).await?;
            let bytes = {
                let _local = self.local.lock().await;
                let (local, bytes) = self.load(descriptor.slice_id)?.ok_or_else(|| {
                    EntityError::Corrupt("remote debt has no staging file".into())
                })?;
                if local != descriptor {
                    return Err(EntityError::Corrupt("upload debt mismatch".into()));
                }
                bytes
            };
            let key = self.keys.object(descriptor.slice_id);
            // A successful prior PUT with a lost response is idempotent only
            // after exact size and digest verification, never by existence.
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
                            return Err(EntityError::Corrupt(
                                "oversized upload verification".into(),
                            ));
                        }
                        remote.extend_from_slice(&part);
                    }
                    authenticate(&descriptor, &remote)?;
                }
                None => self
                    .objects
                    .put_object_create_only_durable(&key, &bytes)
                    .await
                    .map_err(io)?,
            }
            self.finish(&descriptor).await?;
            let _local = self.local.lock().await;
            std::fs::remove_file(self.path(descriptor.slice_id)).map_err(io)?;
            sync_directory(&self.config.directory)?;
            self.uploaded.fetch_add(descriptor.size, Ordering::Relaxed);
        }
        *self.last_error.lock().unwrap() = None;
        self.cleanup_completed().await?;
        Ok(())
    }

    /// A crash after removing debt may leave a completed local file. Verify its
    /// original remote bytes before reclaiming it; never upload an unregistered
    /// orphan, since maintenance may already have retired its object identity.
    async fn cleanup_completed(&self) -> Result<()> {
        let _local = self.local.lock().await;
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
            std::fs::remove_file(path).map_err(io)?;
            sync_directory(&self.config.directory)?;
        }
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

    async fn finish(&self, descriptor: &ObjectDescriptor) -> Result<()> {
        for _ in 0..64 {
            let keys = [
                self.keys.descriptor(),
                self.keys.remote_pending(),
                self.debt_key(descriptor.slice_id),
            ];
            let snapshot = self.backend.read(&keys).await?;
            let namespace: super::lifecycle::NamespaceDescriptor =
                decode(snapshot.rows[0].as_deref().ok_or(EntityError::Fenced)?)?;
            if namespace.namespace_id != self.keys.namespace_id()
                || namespace.format != ENTITY_WRITEBACK_FORMAT
                || namespace.state != NamespaceState::Active
            {
                return Err(EntityError::Fenced);
            }
            if snapshot.rows[2].is_none() {
                return Ok(());
            }
            if snapshot.rows[2].as_deref() != Some(encode(descriptor)?.as_slice()) {
                return Err(EntityError::Corrupt("upload completion identity".into()));
            }
            let mut debt: Debt = decode(snapshot.rows[1].as_deref().ok_or(EntityError::Fenced)?)?;
            debt.objects = debt.objects.checked_sub(1).ok_or(EntityError::Overflow)?;
            debt.bytes = debt
                .bytes
                .checked_sub(descriptor.size)
                .ok_or(EntityError::Overflow)?;
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
            if self
                .backend
                .commit(
                    &authority,
                    &EntityPlan {
                        checks: vec![],
                        writes: vec![
                            counter,
                            RowWrite::Delete {
                                key: keys[2].clone(),
                            },
                        ],
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
        })
    }

    pub async fn drain(&self, timeout: Duration) -> Result<()> {
        tokio::time::timeout(timeout, async {
            loop {
                if self.status().await?.pending_objects == 0 {
                    let _uploader = self.uploader.lock().await;
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
