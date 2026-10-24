//! Bounded, atomic current-row transactions. There is no workspace head CAS.

use std::collections::BTreeMap;

use async_trait::async_trait;

use super::{EntityError, Result, codec::MAX_ROW_BYTES};
use super::{codec, keys::EntityKeys, model::*};
use super::{
    lifecycle::LeaseIdentity,
    ownership::{SliceOwnership, SliceState},
};

pub use super::memory::MemoryEntityBackend;

#[cfg(test)]
#[path = "backend_tests.rs"]
mod tests;

pub const MAX_KEY_BYTES: usize = 1024;
pub const MAX_TRANSACTION_ROWS: usize = 4096;
pub const MAX_TRANSACTION_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowCheck {
    pub key: Vec<u8>,
    pub expected: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RowWrite {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl RowWrite {
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Put { key, .. } | Self::Delete { key } => key,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EntityPlan {
    pub checks: Vec<RowCheck>,
    pub writes: Vec<RowWrite>,
}

/// A new inode's fully typed attributes; only its allocator-chosen identity is
/// filled by the transaction. No metadata row is decoded through Lua numbers.
#[derive(Clone, Debug)]
pub struct CreateAllocation {
    pub(super) keys: EntityKeys,
    pub(super) inode: EntityInode,
    pub(super) name: Vec<u8>,
    pub(super) xattrs: Vec<(Vec<u8>, Vec<u8>)>,
    pub(super) lower_max_ino: i64,
}

pub(super) struct PreparedCreate {
    pub namespace_prefix: Vec<u8>,
    pub counter_key: Vec<u8>,
    pub parent_key: Vec<u8>,
    pub dentry_key: Vec<u8>,
    pub inode_suffix: Vec<u8>,
    pub dentry_suffix: Vec<u8>,
    /// Hex-encoded name suffix and canonical EntityValue bytes.
    pub xattrs: Vec<(Vec<u8>, Vec<u8>)>,
}

/// One bounded upload reservation. Every Pending row is durable before any PUT.
#[derive(Clone, Debug)]
pub struct SliceAllocation {
    pub(super) keys: EntityKeys,
    pub(super) sizes: Vec<u64>,
    pub(super) upload_id: uuid::Uuid,
    pub(super) lease: LeaseIdentity,
}

pub(super) struct PreparedSlices {
    pub counter_key: Vec<u8>,
    pub slice_prefix: Vec<u8>,
    pub descriptor_prefix: Vec<u8>,
    pub row_prefix: Vec<u8>,
    pub suffixes: Vec<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowSnapshot {
    pub now_ns: i64,
    /// Positional results, including absent rows, in the requested key order.
    pub rows: Vec<Option<Vec<u8>>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitOutcome {
    Committed,
    Conflict,
}

/// Created by lifecycle validation from exact canonical descriptor/lease rows.
/// Callers outside this module tree cannot invent a deadline or authority.
#[derive(Clone, Debug)]
pub struct TransactionAuthority {
    pub(super) checks: Vec<RowCheck>,
    pub(super) expires_before_ns: Option<i64>,
}

impl TransactionAuthority {
    pub(super) fn from_validated(
        checks: Vec<RowCheck>,
        expires_before_ns: Option<i64>,
    ) -> Result<Self> {
        if checks.is_empty() || expires_before_ns.is_some_and(|value| value < 0) {
            return Err(EntityError::Invalid("invalid transaction authority".into()));
        }
        let authority = Self {
            checks,
            expires_before_ns,
        };
        validate_plan(&authority, &EntityPlan::default())?;
        Ok(authority)
    }

    pub(super) fn allows_time(&self, now_ns: i64) -> bool {
        now_ns >= 0 && self.expires_before_ns.is_none_or(|end| now_ns < end)
    }
}

#[async_trait]
pub trait EntityBackend: Send + Sync {
    async fn read(&self, keys: &[Vec<u8>]) -> Result<RowSnapshot>;
    /// Read new rows and compare earlier observations in the same atomic view.
    /// Only `keys` are returned; the boolean reports whether every check matched,
    /// including absence. A mismatch must not replace the caller's observations.
    /// Backends may override this fallback to avoid returning checked rows.
    async fn read_checked(
        &self,
        keys: &[Vec<u8>],
        checks: &[RowCheck],
    ) -> Result<(RowSnapshot, bool)> {
        validate_checked_read(keys, checks)?;
        let mut combined = keys.to_vec();
        combined.extend(checks.iter().map(|check| check.key.clone()));
        let mut snapshot = self.read(&combined).await?;
        if snapshot.rows.len() != combined.len() {
            return Err(EntityError::Corrupt("short checked read".into()));
        }
        let observed = snapshot.rows.split_off(keys.len());
        let matched = checks
            .iter()
            .zip(observed)
            .all(|(check, value)| check.expected == value);
        Ok((snapshot, matched))
    }
    /// Exact authority-row changes (including heartbeat renewal) are conflicts.
    /// The caller must obtain fresh lifecycle authority before retrying. Backend
    /// time at or past the deadline is fenced, never silently extended.
    async fn commit(
        &self,
        authority: &TransactionAuthority,
        plan: &EntityPlan,
    ) -> Result<CommitOutcome>;
    /// Atomically allocate and publish an inode with the supplied parent/name
    /// predicates. Some is the exact committed identity; None is a conflict
    /// with no allocation or writes. An uncertain error must never be replayed.
    async fn commit_with_inode_allocation(
        &self,
        _authority: &TransactionAuthority,
        _plan: &EntityPlan,
        _create: &CreateAllocation,
    ) -> Result<Option<i64>> {
        Err(EntityError::Unsupported("atomic inode allocation"))
    }
    /// Allocate all IDs and register all Pending owners in one transaction.
    /// None is a definite conflict with no allocation. Errors are not replayed.
    async fn reserve_slices(
        &self,
        _authority: &TransactionAuthority,
        _allocation: &SliceAllocation,
    ) -> Result<Option<Vec<u64>>> {
        Err(EntityError::Unsupported("atomic slice reservation"))
    }
    /// Bounded prefix observation. A caller using this to decide a mutation must
    /// check its parent row, read before the scan, in the resulting transaction.
    async fn scan_prefix(&self, prefix: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
    /// First at most `limit` matching rows in key order. More matching rows are
    /// allowed; only an empty batch proves prefix absence. A quiescent sweeper
    /// can resume from the beginning after deleting each batch, retaining exact
    /// authority and row predicates for every mutation.
    async fn scan_prefix_batch(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
    /// The first `limit` keys in lexical order, strictly after `after`, within
    /// `prefix`. Fewer than `limit` keys proves EOF. Mutating callers must fence the
    /// parent observed before scanning, and read selected values separately.
    async fn scan_keys_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Vec<u8>>>;
}

pub(super) fn validate_key(key: &[u8]) -> Result<()> {
    if key.is_empty() || key.len() > MAX_KEY_BYTES {
        return Err(EntityError::Invalid("entity key exceeds size bound".into()));
    }
    volume_prefix(key)?;
    Ok(())
}

fn volume_prefix(key: &[u8]) -> Result<&[u8]> {
    const START: &[u8] = b"{brewfs-entity:";
    let invalid = || EntityError::Invalid("invalid entity volume domain".into());
    let tail = key.strip_prefix(START).ok_or_else(invalid)?;
    let uuid_text =
        std::str::from_utf8(tail.get(..36).ok_or_else(invalid)?).map_err(|_| invalid())?;
    let volume = uuid::Uuid::parse_str(uuid_text).map_err(|_| invalid())?;
    if uuid_text != volume.hyphenated().to_string() || tail.get(36..38) != Some(b"}/") {
        return Err(invalid());
    }
    Ok(&key[..START.len() + 38])
}

pub(super) fn validate_same_volume<'a>(keys: impl IntoIterator<Item = &'a [u8]>) -> Result<()> {
    let mut domain = None;
    for key in keys {
        let current = volume_prefix(key)?;
        if domain.is_some_and(|previous| previous != current) {
            return Err(EntityError::Invalid(
                "transaction crosses entity volume domains".into(),
            ));
        }
        domain = Some(current);
    }
    Ok(())
}

pub(super) fn add_bytes(total: &mut usize, key: &[u8], value: Option<&[u8]>) -> Result<()> {
    validate_key(key)?;
    let value_len = value.map_or(0, <[u8]>::len);
    if value_len > MAX_ROW_BYTES {
        return Err(EntityError::Invalid("entity row exceeds size bound".into()));
    }
    *total = total
        .checked_add(key.len())
        .and_then(|value| value.checked_add(value_len))
        .ok_or(EntityError::Overflow)?;
    if *total > MAX_TRANSACTION_BYTES {
        return Err(EntityError::Invalid(
            "entity transaction exceeds byte bound".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_checked_read(keys: &[Vec<u8>], checks: &[RowCheck]) -> Result<()> {
    if keys.len().saturating_add(checks.len()) > MAX_TRANSACTION_ROWS {
        return Err(EntityError::Invalid(
            "entity transaction exceeds row bound".into(),
        ));
    }
    validate_same_volume(
        keys.iter()
            .map(Vec::as_slice)
            .chain(checks.iter().map(|check| check.key.as_slice())),
    )?;
    let mut total = 0;
    for key in keys {
        add_bytes(&mut total, key, None)?;
    }
    for check in checks {
        add_bytes(&mut total, &check.key, check.expected.as_deref())?;
    }
    Ok(())
}

pub(super) fn validate_plan(authority: &TransactionAuthority, plan: &EntityPlan) -> Result<()> {
    validate_same_volume(
        authority
            .checks
            .iter()
            .chain(&plan.checks)
            .map(|check| check.key.as_slice())
            .chain(plan.writes.iter().map(RowWrite::key)),
    )?;
    let count = authority
        .checks
        .len()
        .checked_add(plan.checks.len())
        .and_then(|value| value.checked_add(plan.writes.len()))
        .ok_or(EntityError::Overflow)?;
    if count > MAX_TRANSACTION_ROWS {
        return Err(EntityError::Invalid(
            "entity transaction exceeds row bound".into(),
        ));
    }
    let mut total = 0;
    let mut checks = BTreeMap::new();
    for check in authority.checks.iter().chain(&plan.checks) {
        add_bytes(&mut total, &check.key, check.expected.as_deref())?;
        if checks
            .insert(&check.key, &check.expected)
            .is_some_and(|previous| previous != &check.expected)
        {
            return Err(EntityError::Invalid(
                "contradictory entity predicates".into(),
            ));
        }
    }
    let mut writes = std::collections::BTreeSet::new();
    for write in &plan.writes {
        if !writes.insert(write.key()) {
            return Err(EntityError::Invalid("duplicate entity write".into()));
        }
        let value = match write {
            RowWrite::Put { value, .. } => Some(value.as_slice()),
            RowWrite::Delete { .. } => None,
        };
        add_bytes(&mut total, write.key(), value)?;
    }
    Ok(())
}

pub(super) fn prepare_create(
    authority: &TransactionAuthority,
    plan: &EntityPlan,
    create: &CreateAllocation,
) -> Result<PreparedCreate> {
    validate_plan(authority, plan)?;
    create.inode.validate()?;
    validate_name(&create.name)?;
    let parent = create
        .inode
        .parent_hint
        .ok_or_else(|| EntityError::Invalid("create template lacks parent".into()))?;
    if create.inode.ino != 1 || create.lower_max_ino < 1 || create.xattrs.len() > 2 {
        return Err(EntityError::Invalid(
            "invalid create allocation template".into(),
        ));
    }
    let keys = &create.keys;
    let counter_key = keys.counter("inode");
    let parent_key = keys.inode(parent);
    let dentry_key = keys.dentry(parent, &create.name);
    let checks: Vec<_> = authority.checks.iter().chain(&plan.checks).collect();
    if !checks.iter().any(|check| check.key == parent_key)
        || !checks.iter().any(|check| check.key == dentry_key)
        || checks.iter().any(|check| check.key == counter_key)
        || !plan
            .writes
            .iter()
            .any(|write| matches!(write, RowWrite::Put { key, .. } if key == &parent_key))
        || plan
            .writes
            .iter()
            .any(|write| write.key() == counter_key || write.key() == dentry_key)
    {
        return Err(EntityError::Invalid(
            "create requires parent/name predicates and exclusive allocation writes".into(),
        ));
    }
    let dentry = checks.iter().find(|check| check.key == dentry_key).unwrap();
    if let Some(bytes) = &dentry.expected {
        let value: EntityDentry = codec::decode(bytes)?;
        value.validate()?;
        if value != EntityDentry::Whiteout {
            return Err(EntityError::Exists);
        }
        if *bytes != codec::encode(&EntityDentry::Whiteout)? {
            return Err(EntityError::Corrupt("noncanonical create whiteout".into()));
        }
    }
    let inode_bytes = codec::encode(&UpperInode::Present(create.inode.clone()))?;
    let dentry_bytes = codec::encode(&EntityDentry::Present {
        ino: 1,
        kind: create.inode.kind,
    })?;
    let suffix = |bytes: Vec<u8>| -> Result<Vec<u8>> {
        bytes
            .strip_prefix(b"{\"Present\":{\"ino\":1")
            .filter(|tail| tail.starts_with(b","))
            .map(<[u8]>::to_vec)
            .ok_or_else(|| EntityError::Invalid("unsupported create row encoding".into()))
    };
    let inode_suffix = suffix(inode_bytes)?;
    let dentry_suffix = suffix(dentry_bytes)?;
    let mut names = std::collections::BTreeSet::new();
    let mut xattrs = Vec::with_capacity(create.xattrs.len());
    for (name, value) in &create.xattrs {
        validate_xattr_name(name)?;
        if !names.insert(name) || value.len() > MAX_XATTR_BYTES {
            return Err(EntityError::Invalid("invalid create xattrs".into()));
        }
        xattrs.push((
            hex::encode(name).into_bytes(),
            codec::encode(&EntityValue::Present(value.clone()))?,
        ));
    }
    let prepared = PreparedCreate {
        namespace_prefix: keys.prefix().to_vec(),
        counter_key,
        parent_key,
        dentry_key,
        inode_suffix,
        dentry_suffix,
        xattrs,
    };
    // Validate the largest possible rendered decimal identity before transport.
    // The server repeats these bounds for the actual allocation and stored rows.
    let mut bounded = plan.clone();
    bounded.writes.push(RowWrite::Put {
        key: prepared.counter_key.clone(),
        value: i64::MAX.to_string().into_bytes(),
    });
    bounded.writes.extend(prepared.writes(i64::MAX));
    validate_plan(authority, &bounded)?;
    Ok(prepared)
}

impl SliceAllocation {
    pub(super) fn pending(&self, slice_id: u64, position: usize) -> SliceOwnership {
        SliceOwnership {
            namespace_id: self.keys.namespace_id(),
            slice_id,
            size: self.sizes[position],
            state: SliceState::Pending {
                upload_id: self.upload_id,
                lease: self.lease,
            },
            current_refs: 0,
            staging_hold: true,
            retirement_hold: false,
            publication_refs: 0,
        }
    }
}

pub(super) fn prepare_slices(
    authority: &TransactionAuthority,
    allocation: &SliceAllocation,
) -> Result<PreparedSlices> {
    if allocation.sizes.is_empty()
        || allocation.sizes.len() > 64
        || allocation.sizes.contains(&0)
        || allocation.upload_id.is_nil()
        || allocation.lease.lease_id.is_nil()
        || allocation.lease.holder_generation == 0
    {
        return Err(EntityError::Invalid("invalid upload reservation".into()));
    }
    let counter_key = allocation.keys.counter("slice");
    if authority
        .checks
        .iter()
        .any(|check| check.key == counter_key)
    {
        return Err(EntityError::Invalid(
            "slice counter cannot be an authority predicate".into(),
        ));
    }
    let row_prefix = format!(
        "{{\"namespace_id\":\"{}\",\"slice_id\":",
        allocation.keys.namespace_id()
    )
    .into_bytes();
    let template_prefix = [row_prefix.as_slice(), b"1"].concat();
    let mut suffixes = Vec::with_capacity(allocation.sizes.len());
    for position in 0..allocation.sizes.len() {
        let bytes = codec::encode(&allocation.pending(1, position))?;
        suffixes.push(
            bytes
                .strip_prefix(template_prefix.as_slice())
                .filter(|suffix| suffix.starts_with(b","))
                .ok_or_else(|| EntityError::Invalid("unsupported pending row encoding".into()))?
                .to_vec(),
        );
    }
    let prepared = PreparedSlices {
        counter_key,
        slice_prefix: [allocation.keys.volume_prefix(), b"slice/"].concat(),
        descriptor_prefix: [allocation.keys.volume_prefix(), b"object/"].concat(),
        row_prefix,
        suffixes,
    };
    let ids = ((i64::MAX as u64 - allocation.sizes.len() as u64 + 1)..=i64::MAX as u64)
        .collect::<Vec<_>>();
    let mut bounded = EntityPlan {
        checks: Vec::new(),
        writes: prepared.writes(&ids),
    };
    bounded.writes.push(RowWrite::Put {
        key: prepared.counter_key.clone(),
        value: i64::MAX.to_string().into_bytes(),
    });
    for id in ids {
        bounded.checks.push(RowCheck {
            key: prepared.descriptor_key(id),
            expected: None,
        });
    }
    validate_plan(authority, &bounded)?;
    Ok(prepared)
}

impl PreparedSlices {
    pub fn slice_key(&self, id: u64) -> Vec<u8> {
        [self.slice_prefix.as_slice(), id.to_string().as_bytes()].concat()
    }
    pub fn descriptor_key(&self, id: u64) -> Vec<u8> {
        [self.descriptor_prefix.as_slice(), id.to_string().as_bytes()].concat()
    }
    pub fn writes(&self, ids: &[u64]) -> Vec<RowWrite> {
        ids.iter()
            .zip(&self.suffixes)
            .map(|(id, suffix)| RowWrite::Put {
                key: self.slice_key(*id),
                value: [
                    self.row_prefix.as_slice(),
                    id.to_string().as_bytes(),
                    suffix,
                ]
                .concat(),
            })
            .collect()
    }
}

impl PreparedCreate {
    pub fn inode_key(&self, ino: i64) -> Vec<u8> {
        [
            self.namespace_prefix.as_slice(),
            b"inode/",
            ino.to_string().as_bytes(),
        ]
        .concat()
    }

    /// Render by inserting one canonical decimal at a verified structural
    /// boundary. Digits in names, attributes or timestamps are never replaced.
    pub fn writes(&self, ino: i64) -> Vec<RowWrite> {
        let decimal = ino.to_string();
        let value = |suffix: &[u8]| {
            [
                b"{\"Present\":{\"ino\":".as_slice(),
                decimal.as_bytes(),
                suffix,
            ]
            .concat()
        };
        let mut writes = vec![
            RowWrite::Put {
                key: self.inode_key(ino),
                value: value(&self.inode_suffix),
            },
            RowWrite::Put {
                key: self.dentry_key.clone(),
                value: value(&self.dentry_suffix),
            },
        ];
        for (name, value) in &self.xattrs {
            writes.push(RowWrite::Put {
                key: [
                    self.namespace_prefix.as_slice(),
                    b"xattr/",
                    decimal.as_bytes(),
                    b"/",
                    name,
                ]
                .concat(),
                value: value.clone(),
            });
        }
        writes
    }
}

pub(super) fn validate_scan(prefix: &[u8], limit: usize) -> Result<()> {
    validate_key(prefix)?;
    if limit == 0 || limit > MAX_TRANSACTION_ROWS {
        return Err(EntityError::Invalid("invalid entity scan row bound".into()));
    }
    Ok(())
}

pub(super) fn validate_scan_page(prefix: &[u8], after: Option<&[u8]>, limit: usize) -> Result<()> {
    validate_scan(prefix, limit)?;
    if let Some(after) = after {
        validate_key(after)?;
        if !after.starts_with(prefix) {
            return Err(EntityError::Invalid(
                "entity scan cursor is outside prefix".into(),
            ));
        }
    }
    Ok(())
}

pub(super) fn next_counter(bytes: &[u8]) -> Result<u64> {
    let value = std::str::from_utf8(bytes)
        .map_err(|_| EntityError::Corrupt("invalid entity counter".into()))?;
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(EntityError::Corrupt("invalid entity counter".into()));
    }
    let value = value
        .parse::<u64>()
        .map_err(|_| EntityError::Corrupt("invalid entity counter".into()))?;
    if value > i64::MAX as u64 {
        return Err(EntityError::Corrupt(
            "entity counter exceeds signed integer range".into(),
        ));
    }
    value
        .checked_add(1)
        .filter(|next| *next <= i64::MAX as u64)
        .ok_or(EntityError::Exhausted)
}
