//! Bounded native BWSDELTA hashing. These bytes are native catalog bytes;
//! neither an effective-source SHA256 nor a PM11 digest belongs in this stream.
//!
//! A singleton goes through the existing canonical encoder so there is exactly
//! one definition of row bytes. Only five tiny headers are removed. The caller
//! retains the bounded page owner until this temporary row has been consumed.

use super::{CanonicalLayerDelta, canonical_delta_bytes};
use crate::workspace::error::WorkspaceError;
use crate::workspace::ids::LayerId;
use crate::workspace::model::{
    AclDelta, DataExtentDelta, DentryDelta, InodeDelta, WORKSPACE_SCHEMA_VERSION, XattrDelta,
};

const PREFIX_BYTES: usize = 12;
const TABLE_HEADER_BYTES: usize = 9;

fn invalid(message: &str) -> WorkspaceError {
    WorkspaceError::CorruptMetadata(format!("native delta stream: {message}"))
}

fn signed_key(output: &mut Vec<u8>, value: i64) {
    output.extend_from_slice(&((value as u64) ^ (1_u64 << 63)).to_be_bytes());
}

/// Narrow internal codec contract. These implementations cannot drop Deleted,
/// Whiteout, unreachable, or superseded extent records from the native delta.
pub(crate) trait CanonicalNativeRow {
    const TAG: u8;
    fn layer_id(&self) -> LayerId;
    fn canonical_key(&self) -> Vec<u8>;
    fn variable_bytes(&self) -> Option<usize>;
    fn singleton(&self) -> CanonicalLayerDelta;

    fn canonical_body(&self, max_row_bytes: usize) -> Result<Vec<u8>, WorkspaceError> {
        if !(1..=5).contains(&Self::TAG) {
            return Err(invalid("canonical row table tag"));
        }
        // Check variable fields before the singleton clone/encoder allocation.
        if self
            .variable_bytes()
            .is_none_or(|bytes| bytes > max_row_bytes.saturating_sub(256))
        {
            return Err(invalid("row variable-byte limit"));
        }
        let mut bytes = canonical_delta_bytes(&self.singleton())?;
        let begin = PREFIX_BYTES + TABLE_HEADER_BYTES * usize::from(Self::TAG);
        let tail = TABLE_HEADER_BYTES * usize::from(5 - Self::TAG);
        let end = bytes
            .len()
            .checked_sub(tail)
            .ok_or_else(|| invalid("singleton body layout"))?;
        if end < begin || end - begin > max_row_bytes {
            return Err(invalid("canonical row-byte limit"));
        }
        bytes.truncate(end);
        drop(bytes.drain(..begin));
        Ok(bytes)
    }
}

macro_rules! row {
    ($ty:ty, $tag:literal, $field:ident, $key:expr, $variable:expr) => {
        impl CanonicalNativeRow for $ty {
            const TAG: u8 = $tag;
            fn layer_id(&self) -> LayerId {
                self.layer_id
            }
            fn canonical_key(&self) -> Vec<u8> {
                ($key)(self)
            }
            fn variable_bytes(&self) -> Option<usize> {
                ($variable)(self)
            }
            fn singleton(&self) -> CanonicalLayerDelta {
                CanonicalLayerDelta {
                    $field: vec![self.clone()],
                    ..Default::default()
                }
            }
        }
    };
}

row!(
    DentryDelta,
    1,
    dentries,
    |row: &DentryDelta| {
        let mut key = row.layer_id.as_bytes().to_vec();
        signed_key(&mut key, row.parent_ino);
        key.extend_from_slice(&row.name);
        key
    },
    |row: &DentryDelta| Some(row.name.len())
);
row!(
    InodeDelta,
    2,
    inodes,
    |row: &InodeDelta| {
        let mut key = row.layer_id.as_bytes().to_vec();
        signed_key(&mut key, row.ino);
        key
    },
    |row: &InodeDelta| Some(row.symlink_target.as_ref().map_or(0, Vec::len))
);
row!(
    XattrDelta,
    3,
    xattrs,
    |row: &XattrDelta| {
        let mut key = row.layer_id.as_bytes().to_vec();
        signed_key(&mut key, row.ino);
        key.extend_from_slice(&row.name);
        key
    },
    |row: &XattrDelta| row
        .name
        .len()
        .checked_add(row.value.as_ref().map_or(0, Vec::len))
);
row!(
    AclDelta,
    4,
    acls,
    |row: &AclDelta| {
        let mut key = row.layer_id.as_bytes().to_vec();
        signed_key(&mut key, row.ino);
        key.push(row.acl_type);
        signed_key(&mut key, row.acl_id);
        key
    },
    |row: &AclDelta| Some(row.value.as_ref().map_or(0, Vec::len))
);
row!(
    DataExtentDelta,
    5,
    extents,
    |row: &DataExtentDelta| {
        let mut key = row.layer_id.as_bytes().to_vec();
        signed_key(&mut key, row.ino);
        key.extend_from_slice(&row.chunk_index.to_be_bytes());
        key.extend_from_slice(&row.sequence.to_be_bytes());
        key
    },
    |_: &DataExtentDelta| Some(0)
);

/// Pure codec state, not a seal/drain/publication authority. A trusted capture
/// binds its result to the actual frozen native fence before it can be used.
pub(crate) struct NativeDeltaHasher {
    hasher: blake3::Hasher,
    layer_id: LayerId,
    tag: u8,
    remaining: u64,
    previous: Option<Vec<u8>>,
    bytes: u64,
    max_bytes: u64,
    max_row_bytes: usize,
}

impl NativeDeltaHasher {
    pub(crate) fn new(
        layer_id: LayerId,
        max_bytes: u64,
        max_row_bytes: usize,
    ) -> Result<Self, WorkspaceError> {
        if max_bytes < (PREFIX_BYTES + 5 * TABLE_HEADER_BYTES) as u64 || max_row_bytes < 256 {
            return Err(invalid("invalid codec byte limits"));
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(super::MAGIC);
        hasher.update(&WORKSPACE_SCHEMA_VERSION.to_be_bytes());
        Ok(Self {
            hasher,
            layer_id,
            tag: 0,
            remaining: 0,
            previous: None,
            bytes: PREFIX_BYTES as u64,
            max_bytes,
            max_row_bytes,
        })
    }

    fn add_bytes(&mut self, amount: usize) -> Result<(), WorkspaceError> {
        self.bytes = self
            .bytes
            .checked_add(amount as u64)
            .filter(|bytes| *bytes <= self.max_bytes)
            .ok_or_else(|| invalid("canonical byte quota exceeded"))?;
        Ok(())
    }

    pub(crate) fn begin_table<R: CanonicalNativeRow>(
        &mut self,
        count: u64,
    ) -> Result<(), WorkspaceError> {
        if self.remaining != 0 || R::TAG != self.tag + 1 || R::TAG > 5 {
            return Err(invalid("table order or incomplete preceding table"));
        }
        self.add_bytes(TABLE_HEADER_BYTES)?;
        self.hasher.update(&[R::TAG]);
        self.hasher.update(&count.to_be_bytes());
        self.tag = R::TAG;
        self.remaining = count;
        self.previous = None;
        Ok(())
    }

    pub(crate) fn row<R: CanonicalNativeRow>(&mut self, row: &R) -> Result<(), WorkspaceError> {
        if self.tag != R::TAG || self.remaining == 0 || row.layer_id() != self.layer_id {
            return Err(invalid("wrong table/layer or excess row"));
        }
        let key = row.canonical_key();
        if self
            .previous
            .as_ref()
            .is_some_and(|previous| &key <= previous)
        {
            return Err(invalid("canonical row order or duplicate identity"));
        }
        let body = row.canonical_body(self.max_row_bytes)?;
        self.add_bytes(body.len())?;
        self.hasher.update(&body);
        self.previous = Some(key);
        self.remaining -= 1;
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<([u8; 32], u64), WorkspaceError> {
        if self.tag != 5 || self.remaining != 0 {
            return Err(invalid("incomplete native delta"));
        }
        Ok((*self.hasher.finalize().as_bytes(), self.bytes))
    }
}

#[cfg(test)]
#[path = "native_stream_tests.rs"]
mod tests;
