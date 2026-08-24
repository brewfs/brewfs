use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

use super::error::WorkspaceError;
use super::ids::{JournalId, LayerId, LeaseId, SnapshotId, WorkspaceId};
use uuid::Uuid;

pub const WORKSPACE_SCHEMA_VERSION: u32 = 1;
pub const LAYER_CHAIN_HARD_LIMIT: u32 = 32;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VolumeHeader {
    pub volume_format: String,
    pub schema_version: u32,
    pub volume_id: Uuid,
    pub created_at_ns: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BaseRevision {
    pub layer_id: LayerId,
    pub sealed_version: u64,
    pub root_hash: [u8; 32],
}

impl fmt::Display for BaseRevision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}:{}:{}",
            self.layer_id,
            self.sealed_version,
            hex::encode(self.root_hash)
        )
    }
}

impl FromStr for BaseRevision {
    type Err = WorkspaceError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut fields = value.split(':');
        let layer_id = fields
            .next()
            .ok_or_else(|| WorkspaceError::CorruptMetadata("revision layer id is missing".into()))?
            .parse()
            .map_err(|error| {
                WorkspaceError::CorruptMetadata(format!("invalid revision layer id: {error}"))
            })?;
        let sealed_version = fields
            .next()
            .ok_or_else(|| {
                WorkspaceError::CorruptMetadata("revision sealed version is missing".into())
            })?
            .parse()
            .map_err(|error| {
                WorkspaceError::CorruptMetadata(format!("invalid sealed version: {error}"))
            })?;
        let hash = fields.next().ok_or_else(|| {
            WorkspaceError::CorruptMetadata("revision root hash is missing".into())
        })?;
        if fields.next().is_some() {
            return Err(WorkspaceError::CorruptMetadata(
                "revision has extra fields".into(),
            ));
        }
        let decoded = hex::decode(hash).map_err(|error| {
            WorkspaceError::CorruptMetadata(format!("invalid revision root hash: {error}"))
        })?;
        let root_hash: [u8; 32] = decoded.try_into().map_err(|_| {
            WorkspaceError::CorruptMetadata("revision root hash must contain 32 bytes".into())
        })?;
        Ok(Self {
            layer_id,
            sealed_version,
            root_hash,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ViewContext {
    pub workspace_id: WorkspaceId,
    pub head_layer_id: LayerId,
    pub head_epoch: u64,
    pub lease_id: LeaseId,
    pub holder_generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkspaceRecord {
    pub workspace_id: WorkspaceId,
    pub head_layer_id: LayerId,
    pub head_epoch: u64,
    pub fork_base: Option<BaseRevision>,
    pub owner_id: Option<String>,
    pub state: WorkspaceState,
    pub created_at_ns: i64,
    pub updated_at_ns: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotLease {
    pub lease_id: LeaseId,
    pub workspace_id: WorkspaceId,
    pub base_revision: BaseRevision,
    pub holder_generation: u64,
    pub writable: bool,
    pub state: LeaseState,
    pub expires_at_ns: i64,
    pub created_at_ns: i64,
    pub updated_at_ns: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotRecord {
    pub snapshot_id: SnapshotId,
    pub name: Option<String>,
    pub revision: BaseRevision,
    pub owner_id: Option<String>,
    pub created_at_ns: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SealJournal {
    pub journal_id: JournalId,
    pub workspace_id: WorkspaceId,
    pub old_head_layer_id: LayerId,
    pub expected_head_epoch: u64,
    pub phase: SealPhase,
    pub pending_bytes: u64,
    pub delta_digest: Option<[u8; 32]>,
    pub root_hash: Option<[u8; 32]>,
    pub new_head_layer_id: Option<LayerId>,
    pub last_error: Option<String>,
    pub created_at_ns: i64,
    pub updated_at_ns: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SealResult {
    pub revision: BaseRevision,
    pub new_head_layer_id: LayerId,
    pub head_epoch: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommitResult {
    pub revision: BaseRevision,
    pub target_head_layer_id: LayerId,
    pub target_head_epoch: u64,
}

#[cfg(test)]
mod revision_tests {
    use super::*;

    #[test]
    fn external_revision_encoding_round_trips_exact_precondition() {
        let revision = BaseRevision {
            layer_id: LayerId::from_uuid(Uuid::from_u128(91)),
            sealed_version: 12,
            root_hash: [0xab; 32],
        };

        assert_eq!(
            revision.to_string().parse::<BaseRevision>().unwrap(),
            revision
        );
    }

    #[test]
    fn external_revision_rejects_layer_id_only() {
        let error = Uuid::from_u128(91)
            .to_string()
            .parse::<BaseRevision>()
            .unwrap_err();

        assert!(matches!(error, WorkspaceError::CorruptMetadata(_)));
    }
}

macro_rules! persisted_enum {
    ($name:ident { $($variant:ident = $value:expr),+ $(,)? }) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
        #[repr(u8)]
        pub enum $name {
            $($variant = $value),+
        }

        impl $name {
            pub const fn discriminant(self) -> u8 {
                self as u8
            }
        }

        impl TryFrom<u8> for $name {
            type Error = WorkspaceError;

            fn try_from(value: u8) -> Result<Self, WorkspaceError> {
                match value {
                    $($value => Ok(Self::$variant),)+
                    unknown => Err(WorkspaceError::CorruptMetadata(format!(
                        "unknown {} discriminant {unknown}",
                        stringify!($name)
                    ))),
                }
            }
        }
    };
}

persisted_enum!(WorkspaceState {
    Active = 0,
    Quiescing = 1,
    Sealing = 2,
    Deleting = 3,
    Error = 4,
});
persisted_enum!(LayerState {
    Writable = 0,
    Sealing = 1,
    Sealed = 2,
    Deleting = 3,
});
persisted_enum!(LeaseState {
    Active = 0,
    Releasing = 1,
    Released = 2,
    Expired = 3,
});
persisted_enum!(DentryOp { Put = 0, Whiteout = 1 });
persisted_enum!(InodeState { Present = 0, Deleted = 1 });
persisted_enum!(ValueOp { Put = 0, Whiteout = 1 });
persisted_enum!(SealPhase {
    Prepare = 0,
    Quiesced = 1,
    DataDrained = 2,
    Hashed = 3,
    HeadSwitched = 4,
    Completed = 5,
    Aborted = 6,
});

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LayerRecord {
    pub layer_id: LayerId,
    pub parent_layer_id: Option<LayerId>,
    pub state: LayerState,
    pub schema_version: u32,
    pub sealed_version: Option<u64>,
    pub delta_digest: Option<[u8; 32]>,
    pub root_hash: Option<[u8; 32]>,
    pub depth: u32,
    pub owner_workspace_id: Option<WorkspaceId>,
    pub next_sequence: u64,
    pub owned_slice_count: u64,
    pub owned_bytes: u64,
    pub created_at_ns: i64,
    pub sealed_at_ns: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DentryDelta {
    pub layer_id: LayerId,
    pub parent_ino: i64,
    pub name: Vec<u8>,
    pub op: DentryOp,
    pub ino: Option<i64>,
    pub entry_type: Option<u8>,
    pub sequence: u64,
}

impl DentryDelta {
    pub fn put(
        layer_id: LayerId,
        parent_ino: i64,
        name: Vec<u8>,
        ino: i64,
        entry_type: u8,
        sequence: u64,
    ) -> Self {
        Self {
            layer_id,
            parent_ino,
            name,
            op: DentryOp::Put,
            ino: Some(ino),
            entry_type: Some(entry_type),
            sequence,
        }
    }

    pub fn whiteout(layer_id: LayerId, parent_ino: i64, name: Vec<u8>, sequence: u64) -> Self {
        Self {
            layer_id,
            parent_ino,
            name,
            op: DentryOp::Whiteout,
            ino: None,
            entry_type: None,
            sequence,
        }
    }

    pub fn validate(&self) -> Result<(), WorkspaceError> {
        if self.name.is_empty() {
            return Err(WorkspaceError::CorruptMetadata(
                "dentry name must not be empty".into(),
            ));
        }
        match self.op {
            DentryOp::Put if self.ino.is_some() && self.entry_type.is_some() => Ok(()),
            DentryOp::Whiteout if self.ino.is_none() && self.entry_type.is_none() => Ok(()),
            _ => Err(WorkspaceError::CorruptMetadata(
                "dentry op/payload mismatch".into(),
            )),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InodeDelta {
    pub layer_id: LayerId,
    pub ino: i64,
    pub state: InodeState,
    pub kind: u8,
    pub size: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub nlink: u32,
    pub atime_ns: i64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
    pub symlink_target: Option<Vec<u8>>,
    pub parent_hint: Option<i64>,
    pub data_version: u64,
    pub sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct XattrDelta {
    pub layer_id: LayerId,
    pub ino: i64,
    pub name: Vec<u8>,
    pub op: ValueOp,
    pub value: Option<Vec<u8>>,
    pub sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AclDelta {
    pub layer_id: LayerId,
    pub ino: i64,
    pub acl_type: u8,
    pub acl_id: i64,
    pub op: ValueOp,
    pub value: Option<Vec<u8>>,
    pub sequence: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ExtentKind {
    Data { slice_id: u64, slice_offset: u64 },
    Hole,
}

impl ExtentKind {
    pub const fn discriminant(self) -> u8 {
        match self {
            Self::Data { .. } => 0,
            Self::Hole => 1,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LayerExtent {
    pub layer_id: LayerId,
    pub logical_offset: u64,
    pub length: u64,
    pub sequence: u64,
    pub kind: ExtentKind,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DataExtentDelta {
    pub layer_id: LayerId,
    pub ino: i64,
    pub chunk_index: u64,
    pub logical_offset: u64,
    pub length: u64,
    pub kind: ExtentKind,
    pub sequence: u64,
}

impl DataExtentDelta {
    #[allow(clippy::too_many_arguments)]
    pub fn data(
        layer_id: LayerId,
        ino: i64,
        chunk_index: u64,
        logical_offset: u64,
        length: u64,
        slice_id: u64,
        slice_offset: u64,
        sequence: u64,
    ) -> Self {
        Self {
            layer_id,
            ino,
            chunk_index,
            logical_offset,
            length,
            kind: ExtentKind::Data {
                slice_id,
                slice_offset,
            },
            sequence,
        }
    }

    pub fn hole(
        layer_id: LayerId,
        ino: i64,
        chunk_index: u64,
        logical_offset: u64,
        length: u64,
        sequence: u64,
    ) -> Self {
        Self {
            layer_id,
            ino,
            chunk_index,
            logical_offset,
            length,
            kind: ExtentKind::Hole,
            sequence,
        }
    }

    pub fn as_layer_extent(&self) -> LayerExtent {
        LayerExtent {
            layer_id: self.layer_id,
            logical_offset: self.logical_offset,
            length: self.length,
            sequence: self.sequence,
            kind: self.kind,
        }
    }

    pub fn validate(&self) -> Result<(), WorkspaceError> {
        if self.length == 0 {
            return Err(WorkspaceError::CorruptMetadata(
                "zero-length data extent".into(),
            ));
        }
        self.logical_offset
            .checked_add(self.length)
            .ok_or_else(|| {
                WorkspaceError::CorruptMetadata("data extent logical range overflows".into())
            })?;
        if let ExtentKind::Data { slice_offset, .. } = self.kind {
            slice_offset.checked_add(self.length).ok_or_else(|| {
                WorkspaceError::CorruptMetadata("data extent slice range overflows".into())
            })?;
        }
        Ok(())
    }
}
