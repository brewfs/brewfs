//! Durable inode-local truncate and quiescent namespace cleanup.
//!
//! Callers retain the inode lock and upload/read admission described by the
//! namespace API. A truncate cut is persistent before bounded cleanup begins;
//! every subsequent write/extension must check that its intent is absent.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::backend::{EntityBackend, EntityPlan, RowCheck, RowWrite, TransactionAuthority};
use super::codec::decode;
use super::keys::EntityKeys;
use super::lifecycle::{Lifecycle, MaintenanceToken, checked_rows, committed, put};
use super::model::{ChunkMap, ChunkSource, EntityDentry, EntityInode, EntityKind, UpperInode};
use super::ownership::{
    DeletionToken, OrphanRecord, Ownership, SliceCensus, SliceState, plan_chunk_membership,
};
use super::{EntityError, Result};

/// Bounded discovery state, separate from the cursor of committed map drains.
/// Decimal keys sort lexically, so keep the two numeric minima across pages.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct ChunkDiscovery {
    after: Option<u64>,
    smallest: Vec<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TruncateIntent {
    pub namespace_id: Uuid,
    pub ino: i64,
    pub operation_id: Uuid,
    pub previous_size: u64,
    pub cut_size: u64,
    pub chunk_size: u64,
    /// Inode-local fence against an upload prepared before the cut.
    pub data_version: u64,
    /// Next chunk that has not yet had its stale mapping removed or masked.
    pub next_chunk: u64,
    #[serde(default)]
    pub(super) discovery: ChunkDiscovery,
}

impl TruncateIntent {
    fn validate(&self, keys: &EntityKeys, ino: i64) -> Result<()> {
        if self.namespace_id != keys.namespace_id()
            || self.ino != ino
            || ino <= 0
            || self.operation_id.is_nil()
            || self.chunk_size == 0
            || self.data_version == 0
            || self.cut_size >= self.previous_size
            || self.previous_size > i64::MAX as u64
            || self.next_chunk < self.cut_size / self.chunk_size
        {
            return Err(EntityError::Corrupt("invalid truncate intent".into()));
        }
        Ok(())
    }
}

pub fn absent_intent_check(keys: &EntityKeys, ino: i64) -> RowCheck {
    RowCheck {
        key: keys.truncate(ino),
        expected: None,
    }
}

/// Prepare under the same inode lock that covers upload and publication. The
/// caller supplies the exact upper row plus its effective (possibly lower)
/// inode and commits this plan with current writer authority. Every cut changes
/// data_version; earlier prepared publications must also compare the inode row.
#[allow(clippy::too_many_arguments)]
pub fn prepare_resize(
    keys: &EntityKeys,
    ino: i64,
    expected_inode: Option<Vec<u8>>,
    effective: &EntityInode,
    new_size: u64,
    chunk_size: u64,
    now_ns: i64,
) -> Result<EntityPlan> {
    effective.validate()?;
    if ino != effective.ino
        || ino <= 0
        || chunk_size == 0
        || now_ns < 0
        || new_size > i64::MAX as u64
        || effective.size > i64::MAX as u64
    {
        return Err(EntityError::Invalid("invalid inode resize".into()));
    }
    if effective.kind != EntityKind::File {
        return Err(if effective.kind == EntityKind::Directory {
            EntityError::IsDirectory
        } else {
            EntityError::Unsupported("truncate special inode")
        });
    }
    if let Some(bytes) = expected_inode.as_deref()
        && decode::<UpperInode>(bytes)? != UpperInode::Present(effective.clone())
    {
        return Err(EntityError::Conflict);
    }
    let mut inode = effective.clone();
    // This reference path has no capability bypass; content/size mutations
    // conservatively drop setid privilege bits, matching ordinary writes.
    inode.mode &= !0o6000;
    inode.size = new_size;
    inode.lower_visible_size = inode.lower_visible_size.min(new_size);
    inode.data_version = inode
        .data_version
        .checked_add(1)
        .ok_or(EntityError::Overflow)?;
    inode.mtime_ns = now_ns;
    inode.ctime_ns = now_ns;
    inode.validate()?;
    let mut plan = EntityPlan {
        checks: vec![
            RowCheck {
                key: keys.inode(ino),
                expected: expected_inode,
            },
            absent_intent_check(keys, ino),
        ],
        writes: vec![put(keys.inode(ino), &UpperInode::Present(inode.clone()))?],
    };
    if new_size < effective.size {
        let intent = TruncateIntent {
            namespace_id: keys.namespace_id(),
            ino,
            operation_id: Uuid::new_v4(),
            previous_size: effective.size,
            cut_size: new_size,
            chunk_size,
            data_version: inode.data_version,
            next_chunk: new_size / chunk_size,
            discovery: ChunkDiscovery::default(),
        };
        intent.validate(keys, ino)?;
        plan.writes.push(put(keys.truncate(ino), &intent)?);
    }
    Ok(plan)
}

pub enum DiscardProgress {
    Pending,
    /// The caller performs idempotent physical DELETE before complete_delete.
    ObjectDeletion(DeletionToken),
    Complete,
}

pub struct Recovery<B> {
    backend: Arc<B>,
    keys: EntityKeys,
    lifecycle: Lifecycle<B>,
    ownership: Ownership<B>,
}

impl<B: EntityBackend> Recovery<B> {
    pub fn new(backend: Arc<B>, keys: EntityKeys) -> Self {
        Self {
            lifecycle: Lifecycle::new(backend.clone(), keys.clone()),
            ownership: Ownership::new(backend.clone(), keys.clone()),
            backend,
            keys,
        }
    }

    /// Save one discovery page, perform one map/ownership/cursor transaction, or
    /// clear a completed intent. Runtime callers hold the inode lock;
    /// maintenance callers have already drained all session operations.
    /// This is not expired-lease rescue.
    pub async fn truncate_step(&self, authority: &TransactionAuthority, ino: i64) -> Result<bool> {
        let keys = vec![self.keys.inode(ino), self.keys.truncate(ino)];
        let snapshot = self.backend.read(&keys).await?;
        let Some(bytes) = snapshot.rows[1].as_deref() else {
            return Ok(true);
        };
        let mut intent: TruncateIntent = decode(bytes)?;
        intent.validate(&self.keys, ino)?;
        let inode: UpperInode = decode(
            snapshot.rows[0]
                .as_deref()
                .ok_or_else(|| EntityError::Corrupt("truncate intent lacks inode".into()))?,
        )?;
        let UpperInode::Present(inode) = inode else {
            return Err(EntityError::Corrupt("truncate inode is deleted".into()));
        };
        inode.validate()?;
        if inode.ino != ino
            || inode.kind != EntityKind::File
            || inode.size != intent.cut_size
            || inode.data_version != intent.data_version
            || inode.lower_visible_size > intent.cut_size
        {
            return Err(EntityError::Corrupt(
                "truncate cut or generation changed during cleanup".into(),
            ));
        }
        let first = intent.cut_size / intent.chunk_size;
        let mut plan = EntityPlan {
            checks: checked_rows(&keys, snapshot.rows),
            writes: Vec::new(),
        };
        let Some(chunks) = self.first_chunks(ino, first, &mut intent.discovery).await? else {
            plan.writes.push(put(self.keys.truncate(ino), &intent)?);
            committed(self.backend.commit(authority, &plan).await?)?;
            return Ok(false);
        };
        let suffix = intent.cut_size % intent.chunk_size;
        let mut candidate = None;
        for (index, key, bytes) in chunks {
            let map: ChunkMap = decode(&bytes)?;
            map.validate()?;
            if map.chunk_size != intent.chunk_size || index.checked_mul(intent.chunk_size).is_none()
            {
                return Err(EntityError::Corrupt(
                    "truncate chunk geometry mismatch".into(),
                ));
            }
            if index < intent.next_chunk {
                if index != first
                    || suffix == 0
                    || map
                        .clip(suffix, intent.chunk_size - suffix)?
                        .iter()
                        .any(|span| span.source != ChunkSource::Hole)
                {
                    return Err(EntityError::Corrupt(
                        "truncate cursor skipped stale bytes".into(),
                    ));
                }
                continue;
            }
            candidate = Some((index, key, bytes, map));
            break;
        }
        if let Some((index, key, bytes, old)) = candidate {
            let keep_boundary = index == first && suffix != 0;
            let next = if keep_boundary {
                old.replace(suffix, intent.chunk_size - suffix, ChunkSource::Hole)?
            } else {
                ChunkMap::holes(intent.chunk_size)?
            };
            let membership =
                plan_chunk_membership(self.backend.as_ref(), &self.keys, ino, index, &old, &next)
                    .await?;
            plan.checks.extend(membership.checks);
            plan.writes.extend(membership.writes);
            plan.checks.push(RowCheck {
                key: key.clone(),
                expected: Some(bytes),
            });
            plan.writes.push(if keep_boundary {
                put(key, &next)?
            } else {
                RowWrite::Delete { key }
            });
            intent.next_chunk = index.checked_add(1).ok_or(EntityError::Overflow)?;
            plan.writes.push(put(self.keys.truncate(ino), &intent)?);
            committed(self.backend.commit(authority, &plan).await?)?;
            return Ok(false);
        }
        plan.writes.push(RowWrite::Delete {
            key: self.keys.truncate(ino),
        });
        committed(self.backend.commit(authority, &plan).await?)?;
        Ok(true)
    }

    pub async fn recover_orphan_step(
        &self,
        maintenance: &MaintenanceToken,
        ino: i64,
    ) -> Result<bool> {
        if ino <= 0 {
            return Err(EntityError::Invalid("invalid orphan inode".into()));
        }
        let authority = self.lifecycle.maintenance_authority(maintenance).await?;
        if !self.truncate_step(&authority, ino).await? {
            return Ok(false);
        }
        self.orphan_step(&authority, ino).await
    }

    /// One atomic orphan cleanup step after truncate recovery and quiescence.
    /// Chunk deletion, membership changes and the durable cursor share a commit;
    /// repeated recovery cannot release an owner twice. Chunk discovery pages
    /// keys without imposing a file-wide row limit. Lower data is never upper-owned.
    async fn orphan_step(&self, authority: &TransactionAuthority, ino: i64) -> Result<bool> {
        let keys = vec![
            self.keys.orphan(ino),
            self.keys.inode(ino),
            self.keys.truncate(ino),
        ];
        let snapshot = self.backend.read(&keys).await?;
        if snapshot.rows[2].is_some() {
            return Err(EntityError::Busy);
        }
        let Some(bytes) = snapshot.rows[0].as_deref() else {
            return Ok(true);
        };
        let mut orphan: OrphanRecord = decode(bytes)?;
        if orphan.namespace_id != self.keys.namespace_id() || orphan.ino != ino {
            return Err(EntityError::Corrupt("invalid orphan identity".into()));
        }
        let inode: UpperInode = decode(
            snapshot.rows[1]
                .as_deref()
                .ok_or_else(|| EntityError::Corrupt("orphan missing inode".into()))?,
        )?;
        let UpperInode::Present(inode) = inode else {
            return Err(EntityError::Corrupt(
                "orphan inode already tombstoned".into(),
            ));
        };
        inode.validate()?;
        if inode.ino != ino || inode.nlink != 0 {
            return Err(EntityError::Corrupt("orphan still linked".into()));
        }
        let mut plan = EntityPlan {
            checks: checked_rows(&keys, snapshot.rows),
            writes: Vec::new(),
        };
        let Some(chunks) = self.first_chunks(ino, 0, &mut orphan.discovery).await? else {
            plan.writes.push(put(self.keys.orphan(ino), &orphan)?);
            committed(self.backend.commit(authority, &plan).await?)?;
            return Ok(false);
        };
        if chunks
            .iter()
            .any(|(index, _, _)| *index < orphan.cleanup_chunk)
        {
            return Err(EntityError::Corrupt(
                "orphan cleanup cursor skipped a chunk".into(),
            ));
        }
        if let Some((index, key, bytes)) = chunks.into_iter().next() {
            let old: ChunkMap = decode(&bytes)?;
            let empty = ChunkMap::holes(old.chunk_size)?;
            let membership =
                plan_chunk_membership(self.backend.as_ref(), &self.keys, ino, index, &old, &empty)
                    .await?;
            plan.checks.extend(membership.checks);
            plan.writes.extend(membership.writes);
            plan.checks.push(RowCheck {
                key: key.clone(),
                expected: Some(bytes),
            });
            plan.writes.push(RowWrite::Delete { key });
            orphan.cleanup_chunk = index.checked_add(1).ok_or(EntityError::Overflow)?;
            plan.writes.push(put(self.keys.orphan(ino), &orphan)?);
            committed(self.backend.commit(authority, &plan).await?)?;
            return Ok(false);
        }
        let xattr_prefix = [self.keys.prefix(), format!("xattr/{ino}/").as_bytes()].concat();
        for (key, value) in self.backend.scan_prefix(&xattr_prefix, 4096).await? {
            plan.checks.push(RowCheck {
                key: key.clone(),
                expected: Some(value),
            });
            plan.writes.push(RowWrite::Delete { key });
        }
        for (key, value) in self
            .backend
            .scan_prefix(&self.keys.dentry_prefix(ino), 4096)
            .await?
        {
            if decode::<EntityDentry>(&value)? != EntityDentry::Whiteout {
                return Err(EntityError::NotEmpty);
            }
            plan.checks.push(RowCheck {
                key: key.clone(),
                expected: Some(value),
            });
            plan.writes.push(RowWrite::Delete { key });
        }
        plan.writes
            .push(put(self.keys.inode(ino), &UpperInode::Deleted)?);
        plan.writes.push(RowWrite::Delete {
            key: self.keys.orphan(ino),
        });
        committed(self.backend.commit(authority, &plan).await?)?;
        Ok(true)
    }

    /// Keep numeric cursor semantics for existing intents and decimal keys.
    /// Two maps suffice: an already-clipped boundary and the next stale map.
    /// Scan exactly one bounded page per step, persisting the exclusive lexical
    /// cursor even when every key is below `first`. None requests a discovery-only
    /// commit. At EOF, load at most two maps and reset discovery in the SAME
    /// transaction as the map/ownership/drain cursor (or completion) update.
    async fn first_chunks(
        &self,
        ino: i64,
        first: u64,
        discovery: &mut ChunkDiscovery,
    ) -> Result<Option<Vec<(u64, Vec<u8>, Vec<u8>)>>> {
        const PAGE_KEYS: usize = 1024;
        let prefix = self.keys.chunk_prefix(ino);
        let after = discovery.after.map(|index| self.keys.chunk(ino, index));
        if discovery.smallest.len() > 2
            || discovery.smallest.windows(2).any(|pair| pair[0] >= pair[1])
            || discovery.smallest.iter().any(|&index| {
                index < first
                    || after
                        .as_ref()
                        .is_none_or(|key| self.keys.chunk(ino, index) > *key)
            })
        {
            return Err(EntityError::Corrupt(
                "invalid chunk discovery cursor".into(),
            ));
        }
        let page = self
            .backend
            .scan_keys_page(&prefix, after.as_deref(), PAGE_KEYS)
            .await?;
        let complete = page.len() < PAGE_KEYS;
        for key in page {
            let index = key
                .strip_prefix(prefix.as_slice())
                .and_then(|suffix| std::str::from_utf8(suffix).ok())
                .and_then(|text| text.parse::<u64>().ok())
                .ok_or_else(|| EntityError::Corrupt("invalid chunk index".into()))?;
            if key != self.keys.chunk(ino, index) {
                return Err(EntityError::Corrupt("noncanonical chunk index".into()));
            }
            discovery.after = Some(index);
            if index >= first {
                discovery.smallest.push(index);
                discovery.smallest.sort_unstable();
                discovery.smallest.truncate(2);
            }
        }
        if !complete {
            return Ok(None);
        }
        let selected = std::mem::take(&mut discovery.smallest);
        discovery.after = None;
        if selected.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let keys = selected
            .iter()
            .map(|&index| self.keys.chunk(ino, index))
            .collect::<Vec<_>>();
        let snapshot = self.backend.read(&keys).await?;
        selected
            .into_iter()
            .zip(keys)
            .zip(snapshot.rows)
            .map(|((index, key), value)| Ok((index, key, value.ok_or(EntityError::Conflict)?)))
            .collect::<Result<Vec<_>>>()
            .map(Some)
    }

    /// Retire one chunk, sweep a bounded metadata batch, or return a durable
    /// object deletion reservation. All I/O admission is already stopped. Lower
    /// ownership is released only after no current rows or live upper objects
    /// remain; publication ownership never gets silently discarded.
    pub async fn discard_step(&self, maintenance: &MaintenanceToken) -> Result<DiscardProgress> {
        if self.lifecycle.discard_completed(maintenance).await? {
            return Ok(DiscardProgress::Complete);
        }
        self.lifecycle.begin_discard(maintenance).await?;
        let authority = self.lifecycle.maintenance_authority(maintenance).await?;
        // Each committed deletion advances the next scan from the beginning.
        // Separate prefix probes preserve the cleanup ordering even when a
        // chunk or membership would fall outside the first metadata batch.
        let chunk_prefix = [self.keys.prefix(), b"chunk/"].concat();
        let chunks = self.backend.scan_prefix_batch(&chunk_prefix, 1).await?;
        if let Some((key, bytes)) = chunks.first() {
            let suffix = std::str::from_utf8(&key[chunk_prefix.len()..])
                .map_err(|_| EntityError::Corrupt("invalid discarded chunk key".into()))?;
            let (ino, index) = suffix
                .split_once('/')
                .ok_or_else(|| EntityError::Corrupt("invalid discarded chunk key".into()))?;
            let ino: i64 = ino
                .parse()
                .map_err(|_| EntityError::Corrupt("invalid discarded inode".into()))?;
            let index: u64 = index
                .parse()
                .map_err(|_| EntityError::Corrupt("invalid discarded chunk index".into()))?;
            if ino <= 0 || *key != self.keys.chunk(ino, index) {
                return Err(EntityError::Corrupt(
                    "invalid discarded chunk identity".into(),
                ));
            }
            let old: ChunkMap = decode(bytes)?;
            let empty = ChunkMap::holes(old.chunk_size)?;
            let mut plan =
                plan_chunk_membership(self.backend.as_ref(), &self.keys, ino, index, &old, &empty)
                    .await?;
            plan.checks.push(RowCheck {
                key: key.clone(),
                expected: Some(bytes.clone()),
            });
            plan.writes.push(RowWrite::Delete { key: key.clone() });
            committed(self.backend.commit(&authority, &plan).await?)?;
            return Ok(DiscardProgress::Pending);
        }
        let owner_prefix = [self.keys.prefix(), b"owner/"].concat();
        if !self
            .backend
            .scan_prefix_batch(&owner_prefix, 1)
            .await?
            .is_empty()
        {
            return Err(EntityError::Corrupt(
                "discard found membership without chunk".into(),
            ));
        }
        let lifecycle_keys = [
            self.keys.descriptor(),
            self.keys.lease(),
            self.keys.lower_pin(),
            self.keys.maintenance(),
            self.keys.counter("inode"),
            self.keys.slice_delete_fence(),
        ];
        // Allow room for every retained lifecycle row, so they cannot hide a
        // sweepable row or turn a partial scan into a false emptiness proof.
        // Deleting at most 128 observed rows makes progress restart-safe
        // without a separately persisted cursor.
        let rows = self
            .backend
            .scan_prefix_batch(self.keys.prefix(), 128 + lifecycle_keys.len())
            .await?;
        let batch = rows
            .into_iter()
            .filter(|(key, _)| !lifecycle_keys.contains(key))
            .take(128)
            .collect::<Vec<_>>();
        if !batch.is_empty() {
            let plan = EntityPlan {
                checks: batch
                    .iter()
                    .map(|(key, value)| RowCheck {
                        key: key.clone(),
                        expected: Some(value.clone()),
                    })
                    .collect(),
                writes: batch
                    .into_iter()
                    .map(|(key, _)| RowWrite::Delete { key })
                    .collect(),
            };
            committed(self.backend.commit(&authority, &plan).await?)?;
            return Ok(DiscardProgress::Pending);
        }
        let mut census = SliceCensus::new(self.backend.as_ref(), &self.keys).await?;
        while let Some(rows) = census.next_page(self.backend.as_ref(), &self.keys).await? {
            for row in rows {
                if matches!(row.state, SliceState::Deleted { .. }) {
                    continue;
                }
                if row.current_refs != 0 {
                    return Err(EntityError::Corrupt(
                        "discard found references without memberships".into(),
                    ));
                }
                if row.publication_refs != 0 {
                    return Err(EntityError::Busy);
                }
                if row.staging_hold {
                    self.ownership
                        .abandon_upload(maintenance, row.slice_id)
                        .await?;
                    return Ok(DiscardProgress::Pending);
                }
                let deletion = self
                    .ownership
                    .reserve_delete(maintenance, row.slice_id)
                    .await?;
                return Ok(DiscardProgress::ObjectDeletion(deletion));
            }
        }
        self.lifecycle.discard_empty(maintenance).await?;
        Ok(DiscardProgress::Complete)
    }
}

#[cfg(test)]
#[path = "recovery_tests.rs"]
mod tests;
