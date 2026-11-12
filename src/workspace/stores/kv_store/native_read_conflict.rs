//! Private alignment of complete native read packets across a known root jump.
//! This cannot authorize source access, a mutation, or a publication by itself.

use super::*;

pub(super) const MAX_NATIVE_READ_ATTEMPTS: usize = 3;
pub(super) const NATIVE_READ_REBUILD_BYTES: u64 = 4 << 20;
const MAX_KEYS: usize = 64;
const MAX_VALUE: usize = 48 << 10;
const MAX_PROOF: usize = 2 << 20;

pub(super) struct Alignment {
    pub(super) checks: Vec<KvCheck>,
    pub(super) root_only_conflict: bool,
    pub(super) root_seen: u64,
}

fn root_value(checks: &[KvCheck]) -> Result<u64, WorkspaceError> {
    let raw = checks
        .iter()
        .find(|check| check.key.as_slice() == PACKED_ROOT_GENERATION_KEY)
        .and_then(|check| check.expected.as_deref())
        .ok_or(WorkspaceError::Fenced)?;
    let value: u64 = decode(raw)?;
    if value == 0 || encode(&value)?.as_slice() != raw {
        return Err(WorkspaceError::Fenced);
    }
    Ok(value)
}

pub(super) fn normalize(checks: Vec<KvCheck>) -> Result<Vec<KvCheck>, WorkspaceError> {
    let mut unique: Vec<KvCheck> = Vec::new();
    let mut bytes = 0usize;
    for check in checks {
        let value_bytes = check.expected.as_ref().map_or(0, Vec::len);
        if check.key.len() > 1024 || value_bytes > MAX_VALUE {
            return Err(WorkspaceError::Fenced);
        }
        if let Some(old) = unique.iter().find(|old| old.key == check.key) {
            // Duplicate keys within one actual consistent packet must agree,
            // including root-generation. They are not an inter-read conflict.
            if old.expected != check.expected {
                return Err(WorkspaceError::Fenced);
            }
        } else {
            bytes = bytes
                .checked_add(check.key.len())
                .and_then(|bytes| bytes.checked_add(value_bytes))
                .filter(|bytes| *bytes <= MAX_PROOF)
                .ok_or(WorkspaceError::Busy)?;
            unique.push(check);
            if unique.len() > MAX_KEYS {
                return Err(WorkspaceError::Busy);
            }
        }
    }
    root_value(&unique)?;
    Ok(unique)
}

pub(super) fn align(
    first: Vec<KvCheck>,
    additional: Vec<KvCheck>,
    additional_is_later: bool,
) -> Result<Alignment, WorkspaceError> {
    let mut checks = normalize(first)?;
    let additional = normalize(additional)?;
    let first_root = root_value(&checks)?;
    let additional_root = root_value(&additional)?;
    if (additional_is_later && additional_root < first_root)
        || (!additional_is_later && first_root < additional_root)
    {
        return Err(WorkspaceError::Fenced);
    }
    let root_seen = first_root.max(additional_root);
    let mut root_only_conflict = false;
    // Inspect every overlap. A root mismatch never conceals a later lease,
    // open, source, phase, CONTROL, allocator, or layer-generation mismatch.
    for added in additional {
        if let Some(old) = checks.iter().find(|old| old.key == added.key) {
            if old.expected != added.expected {
                if added.key.as_slice() != PACKED_ROOT_GENERATION_KEY {
                    return Err(WorkspaceError::Busy);
                }
                root_only_conflict = true;
            }
        } else {
            checks.push(added);
        }
    }
    let checks = normalize(checks)?;
    Ok(Alignment {
        checks,
        root_only_conflict,
        root_seen,
    })
}

pub(super) struct FirstRead {
    checks: Option<Vec<KvCheck>>,
    deadline: i64,
    root_floor: u64,
}
impl FirstRead {
    pub(super) fn new() -> Self {
        Self {
            checks: None,
            deadline: i64::MAX,
            root_floor: 0,
        }
    }
    pub(super) fn observe(
        &mut self,
        checks: Vec<KvCheck>,
        deadline: i64,
        root_only_conflict: bool,
        root_seen: u64,
    ) -> Result<(bool, i64), WorkspaceError> {
        let checks = normalize(checks)?;
        let root = root_value(&checks)?;
        if deadline <= 0
            || root_seen < root
            || root < self.root_floor
            || root_seen < self.root_floor
        {
            return Err(WorkspaceError::Fenced);
        }
        if let Some(first) = &self.checks {
            if checks.len() != first.len()
                || first.iter().any(|old| {
                    !checks.iter().any(|fresh| {
                        fresh.key == old.key
                            && (old.key.as_slice() == PACKED_ROOT_GENERATION_KEY
                                || fresh.expected == old.expected)
                    })
                })
            {
                return Err(WorkspaceError::Busy);
            }
        } else {
            self.checks = Some(checks);
        }
        self.deadline = self.deadline.min(deadline);
        self.root_floor = self.root_floor.max(root_seen);
        Ok((!root_only_conflict, self.deadline))
    }
}
