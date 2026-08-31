use super::error::WorkspaceError;
use super::model::{
    AclDelta, DataExtentDelta, DentryDelta, ExtentKind, InodeDelta, ValueOp,
    WORKSPACE_SCHEMA_VERSION, XattrDelta,
};

const MAGIC: &[u8; 8] = b"BWSDELTA";

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CanonicalLayerDelta {
    pub dentries: Vec<DentryDelta>,
    pub inodes: Vec<InodeDelta>,
    pub xattrs: Vec<XattrDelta>,
    pub acls: Vec<AclDelta>,
    pub extents: Vec<DataExtentDelta>,
}

#[derive(Default)]
struct CanonicalEncoder {
    bytes: Vec<u8>,
}

impl CanonicalEncoder {
    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn i64(&mut self, value: i64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn bytes(&mut self, value: &[u8]) {
        self.u64(value.len() as u64);
        self.bytes.extend_from_slice(value);
    }

    fn optional<T>(&mut self, value: Option<&T>, encode: impl FnOnce(&mut Self, &T)) {
        match value {
            Some(value) => {
                self.u8(1);
                encode(self, value);
            }
            None => self.u8(0),
        }
    }

    fn layer_id(&mut self, id: &super::ids::LayerId) {
        self.bytes.extend_from_slice(id.as_bytes());
    }

    fn table(&mut self, tag: u8, rows: usize) {
        self.u8(tag);
        self.u64(rows as u64);
    }
}

pub fn canonical_delta_bytes(delta: &CanonicalLayerDelta) -> Result<Vec<u8>, WorkspaceError> {
    let mut encoder = CanonicalEncoder::default();
    encoder.bytes.extend_from_slice(MAGIC);
    encoder.u32(WORKSPACE_SCHEMA_VERSION);

    let mut dentries = delta.dentries.iter().collect::<Vec<_>>();
    dentries.sort_by(|left, right| {
        (left.layer_id, left.parent_ino, left.name.as_slice()).cmp(&(
            right.layer_id,
            right.parent_ino,
            right.name.as_slice(),
        ))
    });
    encoder.table(1, dentries.len());
    for row in dentries {
        row.validate()?;
        encoder.layer_id(&row.layer_id);
        encoder.i64(row.parent_ino);
        encoder.bytes(&row.name);
        encoder.u8(row.op.discriminant());
        encoder.optional(row.ino.as_ref(), |encoder, value| encoder.i64(*value));
        encoder.optional(row.entry_type.as_ref(), |encoder, value| encoder.u8(*value));
        encoder.u64(row.sequence);
    }

    let mut inodes = delta.inodes.iter().collect::<Vec<_>>();
    inodes.sort_by_key(|row| (row.layer_id, row.ino));
    encoder.table(2, inodes.len());
    for row in inodes {
        encoder.layer_id(&row.layer_id);
        encoder.i64(row.ino);
        encoder.u8(row.state.discriminant());
        encoder.u8(row.kind);
        encoder.u64(row.size);
        encoder.u32(row.mode);
        encoder.u32(row.uid);
        encoder.u32(row.gid);
        encoder.u32(row.rdev);
        encoder.u32(row.nlink);
        encoder.i64(row.atime_ns);
        encoder.i64(row.mtime_ns);
        encoder.i64(row.ctime_ns);
        encoder.optional(row.symlink_target.as_ref(), |encoder, value| {
            encoder.bytes(value)
        });
        encoder.optional(row.parent_hint.as_ref(), |encoder, value| {
            encoder.i64(*value)
        });
        encoder.u64(row.data_version);
        encoder.u64(row.sequence);
    }

    let mut xattrs = delta.xattrs.iter().collect::<Vec<_>>();
    xattrs.sort_by(|left, right| {
        (left.layer_id, left.ino, left.name.as_slice()).cmp(&(
            right.layer_id,
            right.ino,
            right.name.as_slice(),
        ))
    });
    encoder.table(3, xattrs.len());
    for row in xattrs {
        validate_value(row.op, row.value.as_deref(), "xattr")?;
        encoder.layer_id(&row.layer_id);
        encoder.i64(row.ino);
        encoder.bytes(&row.name);
        encoder.u8(row.op.discriminant());
        encoder.optional(row.value.as_ref(), |encoder, value| encoder.bytes(value));
        encoder.u64(row.sequence);
    }

    let mut acls = delta.acls.iter().collect::<Vec<_>>();
    acls.sort_by_key(|row| (row.layer_id, row.ino, row.acl_type, row.acl_id));
    encoder.table(4, acls.len());
    for row in acls {
        validate_value(row.op, row.value.as_deref(), "acl")?;
        encoder.layer_id(&row.layer_id);
        encoder.i64(row.ino);
        encoder.u8(row.acl_type);
        encoder.i64(row.acl_id);
        encoder.u8(row.op.discriminant());
        encoder.optional(row.value.as_ref(), |encoder, value| encoder.bytes(value));
        encoder.u64(row.sequence);
    }

    let mut extents = delta.extents.iter().collect::<Vec<_>>();
    extents.sort_by_key(|row| (row.layer_id, row.ino, row.chunk_index, row.sequence));
    encoder.table(5, extents.len());
    for row in extents {
        row.validate()?;
        encoder.layer_id(&row.layer_id);
        encoder.i64(row.ino);
        encoder.u64(row.chunk_index);
        encoder.u64(row.logical_offset);
        encoder.u64(row.length);
        encoder.u8(row.kind.discriminant());
        match row.kind {
            ExtentKind::Data {
                slice_id,
                slice_offset,
            } => {
                encoder.optional(Some(&slice_id), |encoder, value| encoder.u64(*value));
                encoder.optional(Some(&slice_offset), |encoder, value| encoder.u64(*value));
            }
            ExtentKind::Hole => {
                encoder.optional(None::<&u64>, |_, _| {});
                encoder.optional(None::<&u64>, |_, _| {});
            }
        }
        encoder.u64(row.sequence);
    }

    Ok(encoder.bytes)
}

fn validate_value(op: ValueOp, value: Option<&[u8]>, kind: &str) -> Result<(), WorkspaceError> {
    match (op, value) {
        (ValueOp::Put, Some(_)) | (ValueOp::Whiteout, None) => Ok(()),
        _ => Err(WorkspaceError::CorruptMetadata(format!(
            "{kind} op/payload mismatch"
        ))),
    }
}

pub fn delta_digest(delta: &CanonicalLayerDelta) -> Result<[u8; 32], WorkspaceError> {
    Ok(*blake3::hash(&canonical_delta_bytes(delta)?).as_bytes())
}

pub fn root_hash(parent_root_hash: [u8; 32], delta_digest: [u8; 32]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&WORKSPACE_SCHEMA_VERSION.to_be_bytes());
    hasher.update(&parent_root_hash);
    hasher.update(&delta_digest);
    *hasher.finalize().as_bytes()
}
