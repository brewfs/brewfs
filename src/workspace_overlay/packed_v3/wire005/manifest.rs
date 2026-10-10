//! Small authenticated PM10 root with mandatory counted IP06 routing.

use super::{
    V3_FOOTER_LEN, V3_HEADER_LEN, V3ObjectKind, V3ObjectRef, encode_v3_object, read_v3_page,
};
use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::chunk::read_plan::ReadGeneration;
use crate::workspace_overlay::packed_v3::wire::{PackedResult, PackedWireError, Reader, Writer};
use crate::workspace_overlay::packed_v3::{AccessProfile, SizeClassTable};

pub const V3_MANIFEST_BODY_LIMIT: usize = 64 * 1024;
pub const V3_ROOT_COUNT: usize = 7;
pub const V3_MAX_DIRECTORY_ORDINAL: u64 = i64::MAX as u64 - 2;

/// Namespace-independent physical placement input. Native TiKV metadata and
/// packed metadata supply these fields; descriptor routing, authentication,
/// decompression, memory ownership and execution below are identical.
pub(crate) struct V3ReadPlacementInput {
    pub group_id: u64,
    pub container_ordinal: u32,
    pub entry: crate::workspace_overlay::packed_v3::GroupMetaEntry,
    pub placement: Option<super::V3Placement>,
    pub owner: std::sync::Arc<dyn Send + Sync>,
}

#[derive(Clone, Copy, Debug)]
pub struct V3ReadRange {
    pub offset: u64,
    pub length: usize,
}

struct PlanPreparationOwner {
    limit: usize,
    permit: super::V3OwnedPermit,
}

fn read_preparation_limit(allocation_limit: usize, budget: &super::V3MountBudget) -> usize {
    // Index requests and independently owned flight recipes use Plans while
    // this arena is live. Leave room even at the supported 2 MiB capacity.
    // The prepared arena shrinks to actual source/segment/recipe ownership
    // before execution, so a second reader can join its pending frame.
    const RECIPE_HEADROOM: usize = 512 << 10;
    const MAX_PREPARATION: usize = 2 << 20;
    let capacity =
        usize::try_from(budget.capacity(super::V3BudgetPool::Plans)).unwrap_or(usize::MAX);
    allocation_limit
        .min(capacity.saturating_sub(RECIPE_HEADROOM))
        .min(MAX_PREPARATION)
}
const FEATURE_COUNTED_GROUPS: u32 = 1;
const FEATURE_SOURCE_ATTRIBUTES: u32 = 2;
const FEATURE_EXTERNAL_PLACEMENT: u32 = 4;
const FEATURE_BUILD_POLICY: u32 = 8;
const KNOWN_REQUIRED_FEATURES: u32 = FEATURE_COUNTED_GROUPS
    | FEATURE_SOURCE_ATTRIBUTES
    | FEATURE_EXTERNAL_PLACEMENT
    | FEATURE_BUILD_POLICY;
#[cfg(test)]
#[path = "manifest/coordinator_tests.rs"]
mod coordinator_tests;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum V3RootKind {
    Groups = 0,
    Inodes = 1,
    Containers = 2,
    Frames = 3,
    ColdAttributes = 4,
    ReverseNames = 5,
    LargePlacements = 6,
}

impl V3RootKind {
    pub(crate) fn object_kind(self) -> V3ObjectKind {
        match self {
            Self::Groups => V3ObjectKind::GroupIndex,
            Self::Inodes => V3ObjectKind::InodeIndex,
            Self::Containers => V3ObjectKind::ContainerIndex,
            Self::Frames => V3ObjectKind::FrameIndex,
            Self::ColdAttributes => V3ObjectKind::ColdIndex,
            Self::ReverseNames => V3ObjectKind::ReverseIndex,
            Self::LargePlacements => V3ObjectKind::LargeIndex,
        }
    }
}

const ROOT_KINDS: [V3RootKind; V3_ROOT_COUNT] = [
    V3RootKind::Groups,
    V3RootKind::Inodes,
    V3RootKind::Containers,
    V3RootKind::Frames,
    V3RootKind::ColdAttributes,
    V3RootKind::ReverseNames,
    V3RootKind::LargePlacements,
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V3SnapshotManifest {
    pub snapshot_id: [u8; 32],
    pub root_dir_key: [u8; 32],
    pub root_inode: u64,
    pub group_dentry_count: u64,
    pub profile: AccessProfile,
    pub size_classes: SizeClassTable,
    pub build: super::V3BuildProvenance,
    pub roots: [V3ObjectRef; V3_ROOT_COUNT],
    pub source: Option<super::V3SourceAttributes>,
}

/// Only a successfully verified manifest constructs this pinned snapshot.
/// Generation is the manifest content identity, never the reusable snapshot id.
#[derive(Clone, Debug)]
pub struct AuthenticatedV3Snapshot {
    manifest: V3SnapshotManifest,
    reference: V3ObjectRef,
}

impl V3SnapshotManifest {
    pub fn encode(&self) -> PackedResult<Vec<u8>> {
        self.validate()?;
        let mut w = Writer::default();
        w.bytes(b"PM11");
        w.bytes(&self.snapshot_id);
        w.bytes(&self.root_dir_key);
        w.u64(self.root_inode);
        w.u64(self.group_dentry_count);
        let features = FEATURE_COUNTED_GROUPS
            | FEATURE_BUILD_POLICY
            | if self.source.is_some() {
                FEATURE_SOURCE_ATTRIBUTES
            } else {
                0
            }
            | if self
                .source
                .as_ref()
                .is_some_and(|source| source.placement_contract)
            {
                FEATURE_EXTERNAL_PLACEMENT
            } else {
                0
            };
        w.u32(features);
        w.u32(0);
        w.u8(self.profile as u8);
        w.bytes(&[0; 3]);
        w.u64(self.size_classes.min_frame_raw_bytes);
        w.u64(self.size_classes.max_random_frame_raw_bytes);
        w.u64(self.size_classes.max_sequential_frame_raw_bytes);
        self.build.encode(&mut w)?;
        for root in &self.roots {
            w.u8(root.kind as u8);
            w.u8(0);
            w.u16(root.key.len() as u16);
            w.u64(root.object_len);
            w.bytes(&root.digest);
            w.bytes(root.key.as_bytes());
        }
        if let Some(source) = &self.source {
            source.root.encode(&mut w)?;
            let reference = source.allocations.encode_value()?;
            w.u32(reference.len() as u32);
            w.bytes(&reference);
        }
        encode_v3_object(V3ObjectKind::Manifest, &w.finish(), V3_MANIFEST_BODY_LIMIT)
    }

    fn validate(&self) -> PackedResult<()> {
        if self.snapshot_id == [0; 32]
            || self.root_dir_key == [0; 32]
            || self.root_inode == 0
            || self.root_inode > i64::MAX as u64
            || self.group_dentry_count > V3_MAX_DIRECTORY_ORDINAL
        {
            return Err(PackedWireError::Invalid(
                "PM07 namespace identity is invalid".into(),
            ));
        }
        self.build.validate()?;
        let metadata_groups = self.build.metadata_codec_counts[0]
            .checked_add(self.build.metadata_codec_counts[1])
            .ok_or_else(|| {
                PackedWireError::Invalid("PM11 metadata group count overflows".into())
            })?;
        if metadata_groups > self.group_dentry_count {
            return Err(PackedWireError::Invalid(
                "PM11 build metadata groups exceed dentry weight".into(),
            ));
        }
        self.build
            .policy
            .select(1, self.profile, self.size_classes)?;
        self.size_classes
            .validate()
            .map_err(|_| PackedWireError::Invalid("PM07 size table is invalid".into()))?;
        if self.size_classes.min_frame_raw_bytes > 256 * 1024
            || self.size_classes.max_random_frame_raw_bytes > 4 * 1024 * 1024
            || self.size_classes.max_sequential_frame_raw_bytes > 8 * 1024 * 1024
        {
            return Err(PackedWireError::LimitExceeded(
                "PM07 size table exceeds profile limits".into(),
            ));
        }
        for (root, kind) in self.roots.iter().zip(ROOT_KINDS) {
            super::validate_key(&root.key)?;
            if root.kind != kind.object_kind()
                || root.digest == [0; 32]
                || root.object_len < (V3_HEADER_LEN + V3_FOOTER_LEN) as u64
                || root.object_len > (V3_HEADER_LEN + 256 * 1024 + V3_FOOTER_LEN) as u64
            {
                return Err(PackedWireError::Invalid(
                    "PM07 root kind, identity or page bound mismatch".into(),
                ));
            }
        }
        if let Some(source) = &self.source {
            source.validate(self.root_inode)?;
        }
        Ok(())
    }
}

impl AuthenticatedV3Snapshot {
    pub fn decode(reference: &V3ObjectRef, bytes: &[u8]) -> PackedResult<Self> {
        if reference.kind != V3ObjectKind::Manifest {
            return Err(PackedWireError::Invalid(
                "PM07 ref is not a manifest".into(),
            ));
        }
        let body = reference.verify(bytes, V3_MANIFEST_BODY_LIMIT)?;
        let mut r = Reader::new(body);
        let version = r.take(4)?;
        if version != b"PM11" {
            return Err(PackedWireError::UnsupportedFormat(
                "PM11 manifest payload version mismatch".into(),
            ));
        }
        let snapshot_id = r.array::<32>()?;
        let root_dir_key = r.array::<32>()?;
        let root_inode = r.u64()?;
        let group_dentry_count = r.u64()?;
        let features = r.u32()?;
        r.skip_zeroes(4)?;
        if features & !KNOWN_REQUIRED_FEATURES != 0
            || features & FEATURE_BUILD_POLICY == 0
            || features & FEATURE_COUNTED_GROUPS == 0
            || (features & FEATURE_EXTERNAL_PLACEMENT != 0
                && features & FEATURE_SOURCE_ATTRIBUTES == 0)
        {
            return Err(PackedWireError::UnsupportedFormat(
                "PM10 missing or unknown required feature contract".into(),
            ));
        }
        let profile = AccessProfile::from_u8(r.u8()?)
            .map_err(|_| PackedWireError::Invalid("PM07 profile is invalid".into()))?;
        r.skip_zeroes(3)?;
        let size_classes = SizeClassTable {
            min_frame_raw_bytes: r.u64()?,
            max_random_frame_raw_bytes: r.u64()?,
            max_sequential_frame_raw_bytes: r.u64()?,
        };
        let build = super::V3BuildProvenance::decode(&mut r)?;
        let mut roots = Vec::with_capacity(V3_ROOT_COUNT);
        for kind in ROOT_KINDS {
            let object_kind = kind.object_kind();
            if r.u8()? != object_kind as u8 {
                return Err(PackedWireError::UnsupportedFormat(
                    "PM07 required root kind mismatch".into(),
                ));
            }
            r.skip_zeroes(1)?;
            let key_len = r.u16()? as usize;
            if key_len == 0 || key_len > 4096 {
                return Err(PackedWireError::LimitExceeded(
                    "PM07 root key exceeds budget".into(),
                ));
            }
            let object_len = r.u64()?;
            let digest = r.array::<32>()?;
            let key = std::str::from_utf8(r.take(key_len)?)
                .map_err(|_| PackedWireError::Invalid("PM07 object key is not UTF-8".into()))?
                .to_owned();
            roots.push(V3ObjectRef {
                key,
                kind: object_kind,
                object_len,
                digest,
            });
        }
        let source = if features & FEATURE_SOURCE_ATTRIBUTES != 0 {
            let root = super::V3RootAttributes::decode(&mut r)?;
            let reference_len = r.u32()? as usize;
            if reference_len > 8192 {
                return Err(PackedWireError::LimitExceeded(
                    "PM08 allocation ref exceeds budget".into(),
                ));
            }
            let allocations = V3ObjectRef::decode_value(r.take(reference_len)?)?;
            Some(super::V3SourceAttributes {
                root,
                allocations,
                placement_contract: features & FEATURE_EXTERNAL_PLACEMENT != 0,
            })
        } else {
            None
        };
        if !r.is_empty() {
            return Err(PackedWireError::Invalid(
                "PM07 manifest has trailing fields".into(),
            ));
        }
        let roots = roots
            .try_into()
            .map_err(|_| PackedWireError::Invalid("PM07 root count mismatch".into()))?;
        let manifest = V3SnapshotManifest {
            snapshot_id,
            root_dir_key,
            root_inode,
            group_dentry_count,
            profile,
            size_classes,
            build,
            roots,
            source,
        };
        manifest.validate()?;
        Ok(Self {
            manifest,
            reference: reference.clone(),
        })
    }

    pub async fn open<B: ObjectBackend + Clone>(
        client: &ObjectClient<B>,
        reference: &V3ObjectRef,
    ) -> PackedResult<Self> {
        if reference.kind != V3ObjectKind::Manifest {
            return Err(PackedWireError::Invalid(
                "PM07 ref is not a manifest".into(),
            ));
        }
        super::read_v3_page_validated(client, reference, V3_MANIFEST_BODY_LIMIT, |bytes| {
            Self::decode(reference, &bytes)
        })
        .await
    }

    pub fn manifest(&self) -> &V3SnapshotManifest {
        &self.manifest
    }

    pub fn manifest_reference(&self) -> &V3ObjectRef {
        &self.reference
    }

    pub fn read_generation(&self) -> ReadGeneration {
        ReadGeneration::readonly(self.reference.digest)
    }

    pub fn root(&self, kind: V3RootKind) -> &V3ObjectRef {
        &self.manifest.roots[kind as usize]
    }

    /// None only for legacy PM07. Missing PM08 allocation records are corrupt,
    /// never a request to synthesize blocks from logical size.
    pub async fn source_blocks<B: ObjectBackend + Clone + 'static>(
        &self,
        reader: &super::V3IndexReader<B>,
        inode: u64,
    ) -> PackedResult<Option<u64>> {
        reader
            .observe_resolution(
                V3ObjectKind::SourceStatsIndex,
                self.source_blocks_inner(reader, inode),
            )
            .await
    }

    async fn source_blocks_inner<B: ObjectBackend + Clone + 'static>(
        &self,
        reader: &super::V3IndexReader<B>,
        inode: u64,
    ) -> PackedResult<Option<u64>> {
        let Some(source) = &self.manifest.source else {
            return Ok(None);
        };
        if inode == source.root.inode {
            return Ok(Some(source.root.blocks));
        }
        let value = reader
            .lookup(&source.allocations, &inode.to_be_bytes())
            .await?
            .ok_or_else(|| {
                PackedWireError::Invalid("PM08 inode allocation record is missing".into())
            })?;
        super::source_stat::decode_allocation(&value, inode).map(Some)
    }

    pub async fn lookup<B: ObjectBackend + Clone + 'static>(
        &self,
        reader: &super::V3IndexReader<B>,
        kind: V3RootKind,
        key: &[u8],
    ) -> PackedResult<Option<super::V3IndexValueHandle>> {
        reader.lookup(self.root(kind), key).await
    }

    /// Only PM09 opts into the mandatory selector set. A missing record is
    /// corruption; old PM07/PM08 do not acquire a new fallback interpretation.
    pub async fn placement<B: ObjectBackend + Clone + 'static>(
        &self,
        reader: &super::V3IndexReader<B>,
        inode: u64,
        size: u64,
    ) -> PackedResult<Option<super::V3Placement>> {
        reader
            .observe_resolution(
                V3ObjectKind::LargeIndex,
                self.placement_inner(reader, inode, size),
            )
            .await
    }

    async fn placement_inner<B: ObjectBackend + Clone + 'static>(
        &self,
        reader: &super::V3IndexReader<B>,
        inode: u64,
        size: u64,
    ) -> PackedResult<Option<super::V3Placement>> {
        if !self
            .manifest
            .source
            .as_ref()
            .is_some_and(|source| source.placement_contract)
        {
            return Ok(None);
        }
        let bytes = self
            .lookup(reader, V3RootKind::LargePlacements, &inode.to_be_bytes())
            .await?
            .ok_or_else(|| {
                PackedWireError::Invalid(
                    "PM09 regular inode is missing its required selector".into(),
                )
            })?;
        super::V3Placement::decode(&bytes, inode, size).map(Some)
    }

    pub async fn cold_attributes<B: ObjectBackend + Clone + 'static>(
        &self,
        client: &ObjectClient<B>,
        reader: &super::V3IndexReader<B>,
        inode: u64,
    ) -> PackedResult<Option<super::V3ColdAttributes>> {
        Ok(self
            .cold_attributes_owned(client, reader, inode)
            .await?
            .map(|owned| (*owned).clone()))
    }

    pub(crate) async fn cold_attributes_owned<B: ObjectBackend + Clone + 'static>(
        &self,
        client: &ObjectClient<B>,
        reader: &super::V3IndexReader<B>,
        inode: u64,
    ) -> PackedResult<Option<super::budget::V3Owned<super::V3ColdAttributes>>> {
        let Some(value) = self
            .lookup(reader, V3RootKind::ColdAttributes, &inode.to_be_bytes())
            .await?
        else {
            return Ok(None);
        };
        let reference = V3ObjectRef::decode_value(&value)?;
        if reference.kind != V3ObjectKind::ColdAttributes {
            return Err(PackedWireError::Invalid(
                "cold index selected wrong object kind".into(),
            ));
        }
        let mut permit = reader.budget().admit(&[
            (super::V3BudgetPool::Metadata, 1 << 20),
            (
                super::V3BudgetPool::Stored,
                reference.object_len.checked_mul(2).ok_or_else(|| {
                    PackedWireError::LimitExceeded("cold page size overflow".into())
                })?,
            ),
            (super::V3BudgetPool::Control, 2048),
        ])?;
        let (kind, mode) = if inode == self.manifest.root_inode {
            (
                2,
                self.manifest
                    .source
                    .as_ref()
                    .map_or(0o040755, |source| source.root.mode),
            )
        } else {
            let location = self.lookup_inode(reader, inode).await?.ok_or_else(|| {
                PackedWireError::Invalid("cold attributes reference an absent inode".into())
            })?;
            (location.hot.kind, location.hot.mode)
        };
        let attrs = super::read_v3_page_validated(
            client,
            &reference,
            super::cold::V3_COLD_BODY_LIMIT,
            |bytes| {
                let attrs = super::V3ColdAttributes::decode(&reference, &bytes, inode)?;
                attrs.validate_for_inode(kind, mode)?;
                Ok(attrs)
            },
        )
        .await?;
        let weight = std::mem::size_of::<super::V3ColdAttributes>()
            + attrs.symlink_target.as_ref().map_or(0, Vec::capacity)
            + attrs.xattrs.capacity() * std::mem::size_of::<super::cold::V3Xattr>()
            + attrs
                .xattrs
                .iter()
                .map(|attr| attr.name.capacity() + attr.value.capacity())
                .sum::<usize>()
            + attrs.acl.capacity() * std::mem::size_of::<crate::meta::store::AclRule>();
        permit.shrink(super::V3BudgetPool::Metadata, weight as u64)?;
        permit.shrink(super::V3BudgetPool::Stored, 0)?;
        permit.shrink(super::V3BudgetPool::Control, 0)?;
        Ok(Some(super::budget::V3Owned::new(attrs, permit)))
    }

    pub async fn container_ref<B: ObjectBackend + Clone + 'static>(
        &self,
        reader: &super::V3IndexReader<B>,
        ordinal: u32,
    ) -> PackedResult<V3ObjectRef> {
        reader
            .observe_resolution(
                V3ObjectKind::ContainerIndex,
                self.container_ref_inner(reader, ordinal),
            )
            .await
    }

    async fn container_ref_inner<B: ObjectBackend + Clone + 'static>(
        &self,
        reader: &super::V3IndexReader<B>,
        ordinal: u32,
    ) -> PackedResult<V3ObjectRef> {
        let value = self
            .lookup(reader, V3RootKind::Containers, &ordinal.to_be_bytes())
            .await?
            .ok_or_else(|| {
                PackedWireError::Invalid("PM07 referenced container is missing".into())
            })?;
        let reference = V3ObjectRef::decode_value(&value)?;
        if !matches!(
            reference.kind,
            V3ObjectKind::GroupContainer | V3ObjectKind::LargeData
        ) {
            return Err(PackedWireError::Invalid(
                "PM07 container ref has wrong kind".into(),
            ));
        }
        Ok(reference)
    }

    pub async fn lookup_group<B: ObjectBackend + Clone + 'static>(
        &self,
        reader: &super::V3IndexReader<B>,
        parent: [u8; 32],
        name: &[u8],
    ) -> PackedResult<Option<super::V3GroupRef>> {
        reader
            .observe_resolution(
                V3ObjectKind::GroupIndex,
                self.lookup_group_inner(reader, parent, name),
            )
            .await
    }

    async fn lookup_group_inner<B: ObjectBackend + Clone + 'static>(
        &self,
        reader: &super::V3IndexReader<B>,
        parent: [u8; 32],
        name: &[u8],
    ) -> PackedResult<Option<super::V3GroupRef>> {
        crate::workspace_overlay::packed_v3::meta::validate_name(name)?;
        let mut key = parent.to_vec();
        key.extend_from_slice(name);
        let Some(value) = self.lookup(reader, V3RootKind::Groups, &key).await? else {
            return Ok(None);
        };
        let group = super::V3GroupRef::decode_value(&value)?;
        if group.parent_dir_key != parent
            || name < group.first_name.as_slice()
            || name > group.last_name.as_slice()
        {
            return Err(PackedWireError::Invalid(
                "PM07 selected group disagrees with dentry route".into(),
            ));
        }
        Ok(Some(group))
    }

    pub async fn lookup_inode<B: ObjectBackend + Clone + 'static>(
        &self,
        reader: &super::V3IndexReader<B>,
        inode: u64,
    ) -> PackedResult<Option<super::V3InodeLocation>> {
        reader
            .observe_resolution(
                V3ObjectKind::InodeIndex,
                self.lookup_inode_inner(reader, inode),
            )
            .await
    }

    async fn lookup_inode_inner<B: ObjectBackend + Clone + 'static>(
        &self,
        reader: &super::V3IndexReader<B>,
        inode: u64,
    ) -> PackedResult<Option<super::V3InodeLocation>> {
        let Some(value) = self
            .lookup(reader, V3RootKind::Inodes, &inode.to_be_bytes())
            .await?
        else {
            return Ok(None);
        };
        let location = super::V3InodeLocation::decode_value(&value)?;
        if location.hot.inode != inode {
            return Err(PackedWireError::Invalid(
                "PM07 inode index key disagrees with value".into(),
            ));
        }
        Ok(Some(location))
    }

    /// Reverse names are manifest-authenticated pages, not observations from
    /// whichever dentry happened to be looked up first.
    pub async fn reverse_names_page<B: ObjectBackend + Clone + 'static>(
        &self,
        reader: &super::V3IndexReader<B>,
        inode: u64,
        after: Option<&[u8]>,
        limit: usize,
    ) -> PackedResult<Vec<super::V3InodeLocation>> {
        reader
            .observe_resolution(
                V3ObjectKind::ReverseIndex,
                self.reverse_names_page_inner(reader, inode, after, limit),
            )
            .await
    }

    async fn reverse_names_page_inner<B: ObjectBackend + Clone + 'static>(
        &self,
        reader: &super::V3IndexReader<B>,
        inode: u64,
        after: Option<&[u8]>,
        limit: usize,
    ) -> PackedResult<Vec<super::V3InodeLocation>> {
        let lower = inode.to_be_bytes().to_vec();
        let mut upper = lower.clone();
        upper.extend_from_slice(&[0xff; 1033]);
        let rows = reader
            .scan_page(
                self.root(V3RootKind::ReverseNames),
                &lower,
                &upper,
                after,
                limit,
            )
            .await?;
        let master = self.lookup_inode(reader, inode).await?;
        let mut result = Vec::with_capacity(rows.len());
        for row in &rows {
            let super::V3IndexValue::Leaf(value) = &row.value else {
                return Err(PackedWireError::Invalid(
                    "reverse index returned a branch value".into(),
                ));
            };
            let location = super::V3InodeLocation::decode_value(value)?;
            if location.hot.inode != inode
                || location.reverse_key() != row.first_key
                || row.last_key != row.first_key
            {
                return Err(PackedWireError::Invalid(
                    "reverse index key disagrees with inode/dentry".into(),
                ));
            }
            let Some(master) = &master else {
                return Err(PackedWireError::Invalid(
                    "reverse index has no canonical inode".into(),
                ));
            };
            if !location.same_inode_attributes(master) {
                return Err(PackedWireError::Invalid(
                    "reverse alias disagrees with canonical hot attributes".into(),
                ));
            }
            result.push(location);
        }
        Ok(result)
    }

    pub async fn lookup_dentry<B: ObjectBackend + Clone + 'static>(
        &self,
        client: &ObjectClient<B>,
        reader: &super::V3IndexReader<B>,
        parent: [u8; 32],
        name: &[u8],
        allocation_limit: usize,
    ) -> PackedResult<Option<crate::workspace_overlay::packed_v3::GroupMetaEntry>> {
        let Some(group) = self.lookup_group(reader, parent, name).await? else {
            return Ok(None);
        };
        let container = self.container_ref(reader, group.container_ordinal).await?;
        let metadata = group
            .read_metadata_owned(client, &container, allocation_limit, reader.budget())
            .await?;
        Ok(metadata.lookup(name).cloned())
    }

    pub async fn inode_entry<B: ObjectBackend + Clone + 'static>(
        &self,
        client: &ObjectClient<B>,
        reader: &super::V3IndexReader<B>,
        inode: u64,
        allocation_limit: usize,
    ) -> PackedResult<Option<crate::workspace_overlay::packed_v3::GroupMetaEntry>> {
        reader
            .observe_resolution(
                V3ObjectKind::InodeIndex,
                self.inode_entry_inner(client, reader, inode, allocation_limit),
            )
            .await
    }

    async fn inode_entry_inner<B: ObjectBackend + Clone + 'static>(
        &self,
        client: &ObjectClient<B>,
        reader: &super::V3IndexReader<B>,
        inode: u64,
        allocation_limit: usize,
    ) -> PackedResult<Option<crate::workspace_overlay::packed_v3::GroupMetaEntry>> {
        let Some(location) = self.lookup_inode(reader, inode).await? else {
            return Ok(None);
        };
        let container = self
            .container_ref(reader, location.group.container_ordinal)
            .await?;
        let metadata = location
            .group
            .read_metadata_owned(client, &container, allocation_limit, reader.budget())
            .await?;
        let entry = metadata
            .entries()
            .get(location.hot.entry_ordinal as usize)
            .ok_or_else(|| PackedWireError::Invalid("PM07 inode ordinal exceeds group".into()))?;
        location.validate_entry(entry)?;
        Ok(Some(entry.clone()))
    }

    pub async fn readdir_page<B: ObjectBackend + Clone + 'static>(
        &self,
        client: &ObjectClient<B>,
        reader: &super::V3IndexReader<B>,
        parent: [u8; 32],
        mut offset: u64,
        limit: usize,
        allocation_limit: usize,
    ) -> PackedResult<Vec<crate::workspace_overlay::packed_v3::GroupMetaEntry>> {
        if offset > V3_MAX_DIRECTORY_ORDINAL {
            return Err(PackedWireError::LimitExceeded(
                "PM10 directory ordinal exceeds FUSE cookie boundary".into(),
            ));
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        let lower = parent.to_vec();
        let mut upper = parent.to_vec();
        upper.extend_from_slice(&[0xff; 1025]);
        let mut result = Vec::new();
        let mut owned_bytes = 0usize;
        let mut cursor = reader
            .weighted_cursor(
                self.root(V3RootKind::Groups),
                &lower,
                &upper,
                offset,
                self.manifest.group_dentry_count,
            )
            .await?;
        while let Some((record, entry_offset)) = reader
            .next_weighted(self.root(V3RootKind::Groups), &mut cursor)
            .await?
        {
            let super::V3IndexValue::Leaf(value) = &record.value else {
                return Err(PackedWireError::Invalid(
                    "PM07 group scan returned branch value".into(),
                ));
            };
            let group = super::V3GroupRef::decode_value(value)?;
            if group.parent_dir_key != parent {
                return Err(PackedWireError::Invalid(
                    "PM07 group scan crossed parent boundary".into(),
                ));
            }
            let entry_offset = usize::try_from(entry_offset).map_err(|_| {
                PackedWireError::LimitExceeded("PM10 group ordinal exceeds usize".into())
            })?;
            if entry_offset >= group.entry_count as usize {
                return Err(PackedWireError::Invalid(
                    "PM10 selected group ordinal is invalid".into(),
                ));
            }
            let container = self.container_ref(reader, group.container_ordinal).await?;
            let meta = group
                .read_metadata_owned(client, &container, allocation_limit, reader.budget())
                .await?;
            for entry in meta.entries().iter().skip(entry_offset) {
                let weight =
                    std::mem::size_of::<crate::workspace_overlay::packed_v3::GroupMetaEntry>()
                        + entry.name.len()
                        + entry.extents.len()
                            * std::mem::size_of::<
                                crate::workspace_overlay::packed_v3::GroupMetaExtent,
                            >()
                        + entry.inline_data.len();
                if owned_bytes + weight > allocation_limit {
                    if result.is_empty() {
                        return Err(PackedWireError::LimitExceeded(
                            "PM07 directory entry exceeds page budget".into(),
                        ));
                    }
                    return Ok(result);
                }
                owned_bytes += weight;
                result.push(entry.clone());
                offset = offset
                    .checked_add(1)
                    .filter(|offset| *offset <= V3_MAX_DIRECTORY_ORDINAL)
                    .ok_or_else(|| {
                        PackedWireError::LimitExceeded(
                            "PM10 next directory ordinal exceeds FUSE cookie boundary".into(),
                        )
                    })?;
                if result.len() == limit.min(4096) {
                    return Ok(result);
                }
            }
        }
        Ok(result)
    }

    pub async fn read_inode_range<B: ObjectBackend + Clone + 'static>(
        &self,
        client: &ObjectClient<B>,
        reader: &super::V3IndexReader<B>,
        inode: u64,
        offset: u64,
        output: &mut [u8],
        allocation_limit: usize,
    ) -> PackedResult<()> {
        if output.is_empty() {
            return Ok(());
        }
        let prepared = self
            .prepare_inode_read(
                client,
                reader,
                inode,
                offset,
                output.len(),
                allocation_limit,
            )
            .await?;
        crate::chunk::read_plan::execute_unified_into(
            prepared.fetcher.as_ref(),
            offset,
            &prepared.plan,
            output,
        )
        .await
        .map_err(|error| match error {
            crate::chunk::read_plan::ReadPlanError::StaleView(_) => {
                PackedWireError::ReadViewChanged
            }
            crate::chunk::read_plan::ReadPlanError::Invalid(message) => {
                PackedWireError::Invalid(message)
            }
            crate::chunk::read_plan::ReadPlanError::Backend(error) => {
                PackedWireError::Backend(error.to_string())
            }
        })
    }

    pub async fn prepare_inode_read<B: ObjectBackend + Clone + 'static>(
        &self,
        client: &ObjectClient<B>,
        reader: &super::V3IndexReader<B>,
        inode: u64,
        offset: u64,
        length: usize,
        allocation_limit: usize,
    ) -> PackedResult<crate::chunk::read_plan::PreparedUnifiedRead> {
        self.prepare_inode_read_observed(
            client,
            reader,
            inode,
            V3ReadRange { offset, length },
            allocation_limit,
            None,
        )
        .await
    }

    pub async fn prepare_inode_read_observed<B: ObjectBackend + Clone + 'static>(
        &self,
        client: &ObjectClient<B>,
        reader: &super::V3IndexReader<B>,
        inode: u64,
        range: V3ReadRange,
        allocation_limit: usize,
        delivery: Option<std::sync::Arc<crate::cadapter::read_observer::OperationDelivery>>,
    ) -> PackedResult<crate::chunk::read_plan::PreparedUnifiedRead> {
        use super::V3BudgetPool;
        let V3ReadRange { offset, length } = range;
        // Keep bounded preparation room for independent readers and the
        // mount coordinator. Reserving the entire Plans pool for each read
        // prevents a second prepared read from ever joining an in-flight frame.
        let plan_limit = read_preparation_limit(allocation_limit, reader.budget());
        let plan_permit = reader
            .budget()
            .admit(&[(V3BudgetPool::Plans, plan_limit as u64)])?;
        if length > allocation_limit {
            return Err(PackedWireError::LimitExceeded(
                "PM07 output exceeds allocation budget".into(),
            ));
        }
        let location = self
            .lookup_inode(reader, inode)
            .await?
            .ok_or_else(|| PackedWireError::Invalid("PM07 read inode is missing".into()))?;
        let end = offset.checked_add(length as u64).ok_or_else(|| {
            PackedWireError::LimitExceeded("PM07 logical read end overflows".into())
        })?;
        if end > location.hot.size || location.hot.kind != 1 {
            return Err(PackedWireError::Invalid(
                "PM07 read exceeds file size or requires a regular inode".into(),
            ));
        }
        let container = self
            .container_ref(reader, location.group.container_ordinal)
            .await?;
        let metadata = location
            .group
            .read_metadata_owned(client, &container, 512 * 1024, reader.budget())
            .await?;
        let entry = metadata
            .entries()
            .get(location.hot.entry_ordinal as usize)
            .ok_or_else(|| {
                PackedWireError::Invalid("PM07 inode ordinal exceeds GroupMeta".into())
            })?;
        location.validate_entry(entry)?;
        let placement = self.placement(reader, inode, entry.size).await?;
        self.prepare_placement_with_permit(
            client,
            reader,
            range,
            delivery,
            V3ReadPlacementInput {
                group_id: location.group.group_id,
                container_ordinal: location.group.container_ordinal,
                entry: entry.clone(),
                placement,
                owner: metadata,
            },
            PlanPreparationOwner {
                limit: plan_limit,
                permit: plan_permit,
            },
        )
        .await
    }

    pub(crate) async fn prepare_placement_read_observed<B: ObjectBackend + Clone + 'static>(
        &self,
        client: &ObjectClient<B>,
        reader: &super::V3IndexReader<B>,
        range: V3ReadRange,
        allocation_limit: usize,
        delivery: Option<std::sync::Arc<crate::cadapter::read_observer::OperationDelivery>>,
        input: V3ReadPlacementInput,
    ) -> PackedResult<crate::chunk::read_plan::PreparedUnifiedRead> {
        use super::V3BudgetPool;
        let V3ReadRange { length, .. } = range;
        if length > allocation_limit {
            return Err(PackedWireError::LimitExceeded(
                "output exceeds allocation budget".into(),
            ));
        }
        // The Native and Packed shared placement entrance must leave the
        // same bounded preparation room as the inode entrance above.
        let plan_limit = read_preparation_limit(allocation_limit, reader.budget());
        let plan_permit = reader
            .budget()
            .admit(&[(V3BudgetPool::Plans, plan_limit as u64)])?;
        self.prepare_placement_with_permit(
            client,
            reader,
            range,
            delivery,
            input,
            PlanPreparationOwner {
                limit: plan_limit,
                permit: plan_permit,
            },
        )
        .await
    }

    async fn prepare_placement_with_permit<B: ObjectBackend + Clone + 'static>(
        &self,
        client: &ObjectClient<B>,
        reader: &super::V3IndexReader<B>,
        range: V3ReadRange,
        delivery: Option<std::sync::Arc<crate::cadapter::read_observer::OperationDelivery>>,
        input: V3ReadPlacementInput,
        preparation: PlanPreparationOwner,
    ) -> PackedResult<crate::chunk::read_plan::PreparedUnifiedRead> {
        use super::V3BudgetPool;
        use crate::chunk::read_plan::{
            LogicalSegment, ReadSource, UnifiedReadPlan, UnifiedReadSourceFetcher,
        };
        use std::collections::HashMap;
        let V3ReadRange { offset, length } = range;
        let PlanPreparationOwner {
            limit: plan_limit,
            permit: mut plan_permit,
        } = preparation;
        let V3ReadPlacementInput {
            group_id,
            container_ordinal,
            entry,
            placement,
            owner,
        } = input;
        entry.validate_placement()?;
        let inode = entry.inode;
        if self
            .manifest
            .source
            .as_ref()
            .is_some_and(|source| source.placement_contract)
            && placement.is_none()
        {
            return Err(PackedWireError::Invalid(
                "required regular-inode selector is absent".into(),
            ));
        }
        let end = offset
            .checked_add(length as u64)
            .ok_or_else(|| PackedWireError::LimitExceeded("logical read end overflows".into()))?;
        if end > entry.size || entry.kind != 1 {
            return Err(PackedWireError::Invalid(
                "read exceeds size or requires regular inode".into(),
            ));
        }
        if placement
            .as_ref()
            .is_some_and(|placement| placement.inode() != inode || placement.size() != entry.size)
        {
            return Err(PackedWireError::Invalid(
                "native/packed selector disagrees with inode".into(),
            ));
        }
        // The plan arena owns preparation and execution state together. Its
        // original 1 MiB scratch reservation also contains the concrete source
        // object, Arc header, cloned client and the 4096-byte future allowance.
        // Reject before constructing recipes if the generic client cannot fit.
        let source_bytes = std::mem::size_of::<FetchedSources<B>>()
            .checked_add(2 * std::mem::size_of::<usize>())
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(|| PackedWireError::LimitExceeded("source bound overflows".into()))?;
        if source_bytes > 1 << 20 {
            return Err(PackedWireError::LimitExceeded(
                "read source exceeds admitted execution scratch".into(),
            ));
        }
        let mut frame_budget = plan_limit.checked_sub(1 << 20).ok_or_else(|| {
            PackedWireError::LimitExceeded("plan cannot admit bounded prepare state".into())
        })?;
        let external = matches!(placement, Some(super::V3Placement::External { .. }));
        let mut extents = Vec::new();
        if let Some(super::V3Placement::External {
            extents: root,
            extent_count,
            ..
        }) = placement
        {
            if entry.flags != 0 || !entry.inline_data.is_empty() || !entry.extents.is_empty() {
                return Err(PackedWireError::Invalid(
                    "PM09 external inode has conflicting GroupMeta payload".into(),
                ));
            }
            let lower = super::placement::extent_key(inode, offset);
            let upper = super::placement::extent_key(inode, end);
            let mut after: Option<Vec<u8>> = None;
            if length != 0 {
                loop {
                    let records = reader
                        .scan_overlaps_page(&root, &lower, &upper, after.as_deref(), 256)
                        .await?;
                    if records.is_empty() {
                        break;
                    }
                    for record in &records {
                        let extent =
                            super::V3LargeExtent::decode_record(record, inode, entry.size)?;
                        frame_budget = frame_budget
                            .checked_sub(4 * std::mem::size_of::<super::V3LargeExtent>())
                            .ok_or_else(|| {
                                PackedWireError::LimitExceeded(
                                    "PM09 extent/segment bookkeeping exceeds allocation budget"
                                        .into(),
                                )
                            })?;
                        extents.push(extent);
                        if extents.len() as u64 > extent_count {
                            return Err(PackedWireError::Invalid(
                                "PM09 external extent count exceeds selector".into(),
                            ));
                        }
                    }
                    after = records.last().map(|record| record.first_key.clone());
                }
            }
        } else {
            for extent in &entry.extents {
                if extent.file_offset < end
                    && extent.file_offset + u64::from(extent.logical_len) > offset
                {
                    frame_budget = frame_budget
                        .checked_sub(4 * std::mem::size_of::<super::V3LargeExtent>())
                        .ok_or_else(|| {
                            PackedWireError::LimitExceeded(
                                "PM07 extent/segment bookkeeping exceeds allocation budget".into(),
                            )
                        })?;
                    extents.push(super::V3LargeExtent {
                        inode,
                        file_offset: extent.file_offset,
                        logical_len: extent.logical_len,
                        container_ordinal,
                        frame_ordinal: extent.frame_ordinal,
                        raw_offset: extent.raw_offset,
                        raw_len: extent.raw_len,
                    });
                }
            }
        }
        // Every requested byte has an explicit source, including sparse gaps.
        // Reserve the worst-case data/gap/tail count inside the existing arena
        // before allocating; all-hole and empty requests need no frame recipe.
        let segment_capacity = if length == 0 {
            0
        } else if !entry.inline_data.is_empty() {
            1
        } else {
            extents
                .len()
                .checked_mul(2)
                .and_then(|n| n.checked_add(1))
                .ok_or_else(|| {
                    PackedWireError::LimitExceeded("read segment count overflows".into())
                })?
        };
        let segment_bytes = segment_capacity
            .checked_mul(std::mem::size_of::<LogicalSegment>())
            .ok_or_else(|| PackedWireError::LimitExceeded("read segment bytes overflow".into()))?;
        frame_budget = frame_budget.checked_sub(segment_bytes).ok_or_else(|| {
            PackedWireError::LimitExceeded("read segments exceed allocation budget".into())
        })?;
        let mut segments = Vec::with_capacity(segment_capacity);
        let mut decoded = HashMap::new();
        if !entry.inline_data.is_empty() && length != 0 {
            segments.push(LogicalSegment {
                logical_offset: offset,
                length: length as u64,
                source: ReadSource::PackedInline {
                    data: entry.inline_data.clone(),
                    raw_offset: offset as u32,
                },
            });
        } else {
            let mut cursor = offset;
            for extent in &extents {
                let start = offset.max(extent.file_offset);
                let stop = end.min(extent.file_offset + u64::from(extent.logical_len));
                if start >= stop {
                    continue;
                }
                if start < cursor {
                    return Err(PackedWireError::Invalid(
                        "packed read extents overlap or are not sorted".into(),
                    ));
                }
                if cursor < start {
                    segments.push(LogicalSegment {
                        logical_offset: cursor,
                        length: start - cursor,
                        source: ReadSource::Hole,
                    });
                }
                let frame_key = (extent.container_ordinal, extent.frame_ordinal);
                if let std::collections::hash_map::Entry::Vacant(row) = decoded.entry(frame_key) {
                    let frame_container =
                        self.container_ref(reader, extent.container_ordinal).await?;
                    if external && frame_container.kind != V3ObjectKind::LargeData {
                        return Err(PackedWireError::Invalid(
                            "PM09 external extent references non-LargeData container".into(),
                        ));
                    }
                    let page = self
                        .frame_directory(
                            client,
                            reader,
                            extent.container_ordinal,
                            extent.frame_ordinal,
                            &frame_container,
                        )
                        .await?;
                    let descriptor = page
                        .frames
                        .get((extent.frame_ordinal - page.first_ordinal) as usize)
                        .ok_or_else(|| {
                            PackedWireError::Invalid(
                                "PM07 frame index routed to wrong descriptor page".into(),
                            )
                        })?
                        .clone();
                    if descriptor.raw_len != extent.raw_len {
                        return Err(PackedWireError::Invalid(
                            "PM07 placement raw length disagrees with authenticated descriptor"
                                .into(),
                        ));
                    }
                    // Prepare authenticates recipes only. The executor drops
                    // the prior raw frame before admitting its replacement.
                    frame_budget = frame_budget
                        .checked_sub(
                            std::mem::size_of::<(
                                (u32, u32),
                                (
                                    crate::workspace_overlay::packed_v3::PackedFrameDescriptor,
                                    V3ObjectRef,
                                ),
                            )>() * 4
                                + frame_container.key.capacity(),
                        )
                        .ok_or_else(|| {
                            PackedWireError::LimitExceeded(
                                "authenticated frame recipes exceed plan budget".into(),
                            )
                        })?;
                    row.insert((descriptor, frame_container));
                }
                let descriptor = &decoded[&frame_key].0;
                let raw_offset = u64::from(extent.raw_offset) + start - extent.file_offset;
                segments.push(LogicalSegment {
                    logical_offset: start,
                    length: stop - start,
                    source: ReadSource::PackedFrame {
                        group_id,
                        container_ordinal: extent.container_ordinal,
                        frame_ordinal: extent.frame_ordinal,
                        object_offset: descriptor.object_offset,
                        stored_len: descriptor.stored_len,
                        raw_offset: raw_offset as u32,
                        raw_len: extent.raw_len,
                        size_class: descriptor.size_class as u8,
                        codec: descriptor.codec,
                        frame_digest: descriptor.frame_digest,
                    },
                });
                cursor = stop;
            }
            if cursor < end {
                segments.push(LogicalSegment {
                    logical_offset: cursor,
                    length: end - cursor,
                    source: ReadSource::Hole,
                });
            }
        }
        type ActiveFrame = Option<(
            (u32, u32),
            std::sync::Arc<super::budget::V3Owned<super::pipeline::V3SharedFrame>>,
        )>;
        struct FetchedSources<B: ObjectBackend + Clone> {
            group_id: u64,
            frames: HashMap<
                (u32, u32),
                (
                    crate::workspace_overlay::packed_v3::PackedFrameDescriptor,
                    V3ObjectRef,
                ),
            >,
            generation: ReadGeneration,
            client: ObjectClient<B>,
            profile: AccessProfile,
            classes: SizeClassTable,
            frame_policy: super::V3FramePolicy,
            budget: std::sync::Arc<super::V3MountBudget>,
            active: tokio::sync::Mutex<ActiveFrame>,
            delivery: Option<std::sync::Arc<crate::cadapter::read_observer::OperationDelivery>>,
            inline_coverage: tokio::sync::Mutex<Option<crate::cadapter::read_observer::RawLease>>,
            _inline_permit: Option<super::V3OwnedPermit>,
            _plan_permit: super::V3OwnedPermit,
            _pipeline: Option<std::sync::Arc<super::pipeline::V3DemandCoordinator<B>>>,
            _placement_owner: std::sync::Arc<dyn Send + Sync>,
        }
        #[async_trait::async_trait]
        impl<B: ObjectBackend + Clone + 'static> UnifiedReadSourceFetcher for FetchedSources<B> {
            async fn read_source(
                &self,
                source: &ReadSource,
                output: &mut [u8],
            ) -> anyhow::Result<()> {
                match source {
                    ReadSource::PackedFrame {
                        group_id,
                        container_ordinal,
                        frame_ordinal,
                        raw_offset,
                        ..
                    } => {
                        if *group_id != self.group_id {
                            anyhow::bail!("packed source belongs to another group");
                        }
                        let key = (*container_ordinal, *frame_ordinal);
                        let (descriptor, container) = self
                            .frames
                            .get(&(*container_ordinal, *frame_ordinal))
                            .ok_or_else(|| anyhow::anyhow!("missing authenticated frame"))?;
                        if let ReadSource::PackedFrame {
                            object_offset,
                            stored_len,
                            raw_len,
                            size_class,
                            codec,
                            frame_digest,
                            ..
                        } = source
                            && (*object_offset != descriptor.object_offset
                                || *stored_len != descriptor.stored_len
                                || *raw_len != descriptor.raw_len
                                || *size_class != descriptor.size_class as u8
                                || *codec != descriptor.codec
                                || *frame_digest != descriptor.frame_digest)
                        {
                            anyhow::bail!(
                                "packed plan source disagrees with authenticated descriptor"
                            );
                        }
                        let mut active = self.active.lock().await;
                        if active.as_ref().map(|(key, _)| key) != Some(&key) {
                            // Release the previous raw consumer before waiting
                            // for the next frame, including logical A-B-A.
                            *active = None;
                            let class = if container.kind == V3ObjectKind::LargeData {
                                crate::cadapter::read_observer::ReadClass::ExternalPayload
                            } else {
                                crate::cadapter::read_observer::ReadClass::PackedPayload
                            };
                            let context = self.client.read_context(class).unwrap_or(
                                crate::cadapter::read_observer::ReadContext {
                                    engine: crate::cadapter::read_observer::Engine::PackedV3,
                                    phase: crate::cadapter::read_observer::Phase::Runtime,
                                    origin: crate::cadapter::read_observer::Origin::Demand,
                                    class,
                                },
                            );
                            let waiter = self
                                ._pipeline
                                .as_ref()
                                .ok_or_else(|| anyhow::anyhow!("missing mount frame coordinator"))?
                                .submit_with(container.key.len(), || {
                                    super::pipeline::V3FrameDemand {
                                        generation: self.generation,
                                        context,
                                        container: container.clone(),
                                        profile: self.profile,
                                        size_classes: self.classes,
                                        frame_policy: self.frame_policy,
                                        descriptor: descriptor.clone(),
                                        raw_offset: u64::from(*raw_offset),
                                        logical_length: output.len() as u64,
                                    }
                                })?;
                            let frame = waiter
                                .wait()
                                .await
                                .map_err(|error| anyhow::Error::new((*error).clone()))?;
                            drop(waiter);
                            *active = Some((key, frame));
                        }
                        let frame = &active.as_ref().unwrap().1;
                        let start = *raw_offset as usize;
                        let end = start
                            .checked_add(output.len())
                            .ok_or_else(|| anyhow::anyhow!("frame output range overflows"))?;
                        let receipt = if let (Some(coverage), Some(delivery)) =
                            (&frame.coverage, &self.delivery)
                        {
                            let bytes=crate::cadapter::read_observer::SharedRawCoverage::required_receipt_bytes(1)?;
                            let owner = self.budget.admit(&[(V3BudgetPool::Control, bytes)])?;
                            Some(
                                coverage
                                    .attach(
                                        delivery,
                                        &[(start as u64, output.len() as u64)],
                                        bytes,
                                        Box::new(owner),
                                    )
                                    .map_err(|error| {
                                        if error
                                            .is::<crate::cadapter::read_observer::SharedRawLimit>()
                                        {
                                            anyhow::Error::new(PackedWireError::LimitExceeded(
                                                error.to_string(),
                                            ))
                                        } else {
                                            error
                                        }
                                    })?,
                            )
                        } else {
                            None
                        };
                        output.copy_from_slice(
                            frame.raw.get(start..end).ok_or_else(|| {
                                anyhow::anyhow!("frame output range exceeds source")
                            })?,
                        );
                        if let Some(receipt) = receipt {
                            receipt.copied(start as u64, output.len() as u64)?;
                        }
                    }
                    ReadSource::PackedInline { data, raw_offset } => {
                        let start = *raw_offset as usize;
                        let end = start
                            .checked_add(output.len())
                            .ok_or_else(|| anyhow::anyhow!("inline output range overflows"))?;
                        output.copy_from_slice(data.get(start..end).ok_or_else(|| {
                            anyhow::anyhow!("inline output range exceeds source")
                        })?);
                        if let Some(coverage) = self.inline_coverage.lock().await.as_mut() {
                            coverage.copied(start as u64, output.len() as u64)?;
                        }
                    }
                    ReadSource::Hole => output.fill(0),
                    _ => anyhow::bail!("non-packed source in readonly plan"),
                }
                Ok(())
            }
            async fn ensure_generation(&self, generation: ReadGeneration) -> anyhow::Result<()> {
                if generation != self.generation {
                    return Err(crate::chunk::read_plan::ReadViewChanged.into());
                }
                Ok(())
            }
        }
        let generation = self.read_generation();
        let plan = UnifiedReadPlan {
            generation,
            logical_size: entry.size,
            segments,
        };
        plan.validate(offset, length as u64)
            .map_err(|error| PackedWireError::Invalid(error.to_string()))?;
        drop(extents);
        let (inline_coverage, inline_permit) = if !entry.inline_data.is_empty() {
            if let Some(delivery) = &delivery {
                use crate::cadapter::read_observer::{
                    Engine, Origin, Phase, RawCoverage, RawLease, ReadClass, ReadContext,
                };
                let bytes = RawCoverage::required_tracking_bytes(entry.inline_data.len() as u64)
                    .map_err(|error| PackedWireError::LimitExceeded(error.to_string()))?;
                let permit = reader.budget().admit(&[(V3BudgetPool::Workspace, bytes)])?;
                let mut coverage = RawLease::new(
                    entry.inline_data.len() as u64,
                    bytes,
                    delivery.clone(),
                    client
                        .read_context(ReadClass::InlinePayload)
                        .unwrap_or(ReadContext {
                            engine: Engine::PackedV3,
                            phase: Phase::Runtime,
                            origin: Origin::Demand,
                            class: ReadClass::InlinePayload,
                        }),
                )
                .map_err(|error| PackedWireError::LimitExceeded(error.to_string()))?;
                coverage
                    .request(offset, length as u64)
                    .map_err(|error| PackedWireError::Invalid(error.to_string()))?;
                (Some(coverage), Some(permit))
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };
        let pipeline = if decoded.is_empty() {
            None
        } else {
            Some(reader.demand_pipeline().await?)
        };
        let plan_bytes = source_bytes
            + std::mem::size_of::<crate::chunk::read_plan::UnifiedReadPlan>()
            + plan.segments.capacity() * std::mem::size_of::<LogicalSegment>()
            + decoded.capacity()
                * std::mem::size_of::<(
                    (u32, u32),
                    (
                        crate::workspace_overlay::packed_v3::PackedFrameDescriptor,
                        V3ObjectRef,
                    ),
                )>()
                * 2
            + decoded
                .values()
                .map(|(_, reference)| reference.key.capacity())
                .sum::<usize>();
        plan_permit.shrink(V3BudgetPool::Plans, plan_bytes as u64)?;
        Ok(crate::chunk::read_plan::PreparedUnifiedRead {
            plan,
            fetcher: std::sync::Arc::new(FetchedSources {
                group_id,
                frames: decoded,
                generation,
                client: client.clone(),
                profile: self.manifest.profile,
                classes: self.manifest.size_classes,
                frame_policy: self.manifest.build.policy.frames,
                budget: reader.budget().clone(),
                active: tokio::sync::Mutex::new(None),
                delivery,
                inline_coverage: tokio::sync::Mutex::new(inline_coverage),
                _inline_permit: inline_permit,
                _plan_permit: plan_permit,
                _pipeline: pipeline,
                _placement_owner: owner,
            }),
        })
    }

    /// Resolve descriptor and container identities from this pinned manifest.
    /// The roots are immutable; no caller-supplied descriptor is trusted here.
    pub async fn read_frame<B: ObjectBackend + Clone + 'static>(
        &self,
        client: &ObjectClient<B>,
        reader: &super::V3IndexReader<B>,
        container_ordinal: u32,
        frame_ordinal: u32,
        allocation_limit: usize,
    ) -> PackedResult<Vec<u8>> {
        let container_value = self
            .lookup(
                reader,
                V3RootKind::Containers,
                &container_ordinal.to_be_bytes(),
            )
            .await?
            .ok_or_else(|| {
                PackedWireError::Invalid(
                    "PM07 container index is missing a referenced container".into(),
                )
            })?;
        let container = V3ObjectRef::decode_value(&container_value)?;
        let page = self
            .frame_directory(client, reader, container_ordinal, frame_ordinal, &container)
            .await?;
        page.read_frame(client, &container, frame_ordinal, allocation_limit)
            .await
    }

    async fn frame_directory<B: ObjectBackend + Clone + 'static>(
        &self,
        client: &ObjectClient<B>,
        reader: &super::V3IndexReader<B>,
        container_ordinal: u32,
        frame_ordinal: u32,
        container: &V3ObjectRef,
    ) -> PackedResult<super::budget::V3Owned<super::V3FrameDirectoryPage>> {
        let mut key = container_ordinal.to_be_bytes().to_vec();
        key.extend_from_slice(&frame_ordinal.to_be_bytes());
        let directory_value = self
            .lookup(reader, V3RootKind::Frames, &key)
            .await?
            .ok_or_else(|| {
                PackedWireError::Invalid(
                    "PM07 frame index is missing a referenced descriptor page".into(),
                )
            })?;
        let reference = V3ObjectRef::decode_value(&directory_value)?;
        let mut permit = reader.budget().admit(&[
            (super::V3BudgetPool::Metadata, 512 << 10),
            (super::V3BudgetPool::Stored, reference.object_len * 2),
            (super::V3BudgetPool::Control, 2048),
        ])?;
        let page = super::V3FrameDirectoryPage::read(client, &reference, container.digest).await?;
        if page.profile != self.manifest.profile
            || page.size_classes != self.manifest.size_classes
            || page.frame_policy != self.manifest.build.policy.frames
        {
            return Err(PackedWireError::Invalid(
                "PM07 frame directory profile disagrees with manifest".into(),
            ));
        }
        if frame_ordinal < page.first_ordinal
            || frame_ordinal - page.first_ordinal >= page.frames.len() as u32
        {
            return Err(PackedWireError::Invalid(
                "PM07 frame index key disagrees with descriptor page".into(),
            ));
        }
        permit.shrink(super::V3BudgetPool::Metadata,
            (std::mem::size_of::<super::V3FrameDirectoryPage>() + page.frames.capacity()*std::mem::size_of::<crate::workspace_overlay::packed_v3::PackedFrameDescriptor>()) as u64)?;
        permit.shrink(super::V3BudgetPool::Stored, 0)?;
        permit.shrink(super::V3BudgetPool::Control, 0)?;
        Ok(super::budget::V3Owned::new(page, permit))
    }

    /// Authenticates the selected root page but does not eagerly fetch its
    /// children. Page-specific decoding/routing is the catalog's next step.
    pub async fn read_root<B: ObjectBackend + Clone>(
        &self,
        client: &ObjectClient<B>,
        kind: V3RootKind,
    ) -> PackedResult<Vec<u8>> {
        read_v3_page(client, self.root(kind), 256 * 1024).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> V3SnapshotManifest {
        V3SnapshotManifest {
            snapshot_id: [1; 32],
            root_dir_key: [2; 32],
            root_inode: 1,
            group_dentry_count: 0,
            profile: AccessProfile::RandomSmallFile,
            size_classes: SizeClassTable::default(),
            build: Default::default(),
            roots: std::array::from_fn(|i| {
                let kind = ROOT_KINDS[i].object_kind();
                let bytes = super::super::V3IndexPage {
                    kind,
                    height: 0,
                    records: Vec::new(),
                }
                .encode()
                .unwrap();
                V3ObjectRef::from_bytes(format!("roots/{i}"), kind, &bytes).unwrap()
            }),
            source: None,
        }
    }

    fn source_attributes(allocations: V3ObjectRef) -> super::super::V3SourceAttributes {
        super::super::V3SourceAttributes {
            placement_contract: false,
            root: super::super::V3RootAttributes {
                inode: 1,
                size: 4096,
                blocks: 8,
                mode: 0o040750,
                uid: 123,
                gid: 456,
                nlink: 3,
                atime_ns: -4,
                mtime_ns: 5,
                ctime_ns: 6,
            },
            allocations,
        }
    }

    #[test]
    fn pm11_requires_counted_features_and_authenticates_external_contract() {
        let page = super::super::V3IndexPage {
            kind: V3ObjectKind::SourceStatsIndex,
            height: 0,
            records: vec![],
        }
        .encode()
        .unwrap();
        let mut manifest = manifest();
        manifest.source = Some(source_attributes(
            V3ObjectRef::from_bytes("stats".into(), V3ObjectKind::SourceStatsIndex, &page).unwrap(),
        ));
        let without_external = manifest.encode().unwrap();
        manifest.source.as_mut().unwrap().placement_contract = true;
        let with_external = manifest.encode().unwrap();
        assert_eq!(&with_external[super::super::V3_HEADER_LEN..][..4], b"PM11");
        let reference =
            V3ObjectRef::from_bytes("manifest".into(), V3ObjectKind::Manifest, &with_external)
                .unwrap();
        assert_eq!(
            AuthenticatedV3Snapshot::decode(&reference, &with_external)
                .unwrap()
                .manifest(),
            &manifest
        );
        let body = reference
            .verify(&with_external, V3_MANIFEST_BODY_LIMIT)
            .unwrap();
        for case in 0..6 {
            let mut bad = body.to_vec();
            match case {
                0 => bad[..4].copy_from_slice(b"PM09"),
                1 => {
                    bad[84] &= !FEATURE_COUNTED_GROUPS as u8;
                }
                2 => {
                    bad[87] |= 0x80;
                }
                3 => {
                    bad[84] &= !FEATURE_SOURCE_ATTRIBUTES as u8;
                }
                4 => {
                    bad[84] &= !FEATURE_BUILD_POLICY as u8;
                }
                _ => bad.push(0),
            }
            let bytes =
                encode_v3_object(V3ObjectKind::Manifest, &bad, V3_MANIFEST_BODY_LIMIT).unwrap();
            let reference =
                V3ObjectRef::from_bytes("bad".into(), V3ObjectKind::Manifest, &bytes).unwrap();
            assert!(
                AuthenticatedV3Snapshot::decode(&reference, &bytes).is_err(),
                "case {case}"
            );
        }
        manifest.source.as_mut().unwrap().placement_contract = false;
        assert_eq!(manifest.encode().unwrap(), without_external);
        for version in [b"PM07", b"PM08", b"PM09", b"PM10"] {
            let mut bad = body.to_vec();
            bad[..4].copy_from_slice(version);
            let bytes =
                encode_v3_object(V3ObjectKind::Manifest, &bad, V3_MANIFEST_BODY_LIMIT).unwrap();
            let reference =
                V3ObjectRef::from_bytes("old-payload".into(), V3ObjectKind::Manifest, &bytes)
                    .unwrap();
            assert!(matches!(
                AuthenticatedV3Snapshot::decode(&reference, &bytes),
                Err(PackedWireError::UnsupportedFormat(_))
            ));
        }
        manifest.group_dentry_count = V3_MAX_DIRECTORY_ORDINAL + 1;
        assert!(manifest.encode().is_err());
    }

    #[tokio::test]
    async fn pm11_required_external_selector_fails_closed() {
        use crate::cadapter::localfs::LocalFsBackend;
        let temp = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(temp.path()));
        let page = super::super::V3IndexPage {
            kind: V3ObjectKind::LargeIndex,
            height: 0,
            records: vec![],
        }
        .encode()
        .unwrap();
        client.put_object("selectors", &page).await.unwrap();
        let mut manifest = manifest();
        manifest.roots[V3RootKind::LargePlacements as usize] =
            V3ObjectRef::from_bytes("selectors".into(), V3ObjectKind::LargeIndex, &page).unwrap();
        let stats = super::super::V3IndexPage {
            kind: V3ObjectKind::SourceStatsIndex,
            height: 0,
            records: vec![],
        }
        .encode()
        .unwrap();
        manifest.source = Some(source_attributes(
            V3ObjectRef::from_bytes("stats".into(), V3ObjectKind::SourceStatsIndex, &stats)
                .unwrap(),
        ));
        let reader = super::super::V3IndexReader::new(client, 0);
        for required in [false, true] {
            manifest.source.as_mut().unwrap().placement_contract = required;
            let bytes = manifest.encode().unwrap();
            let reference =
                V3ObjectRef::from_bytes("manifest".into(), V3ObjectKind::Manifest, &bytes).unwrap();
            let snapshot = AuthenticatedV3Snapshot::decode(&reference, &bytes).unwrap();
            if required {
                assert!(
                    snapshot
                        .placement(&reader, 9, 72 * 1024 * 1024)
                        .await
                        .is_err()
                );
            } else {
                assert_eq!(
                    snapshot
                        .placement(&reader, 9, 72 * 1024 * 1024)
                        .await
                        .unwrap(),
                    None
                );
            }
        }
    }

    #[test]
    fn pm11_source_contract_roundtrips_and_rejects_legacy_payloads() {
        let mut manifest = manifest();
        let current = manifest.encode().unwrap();
        let current_ref =
            V3ObjectRef::from_bytes("current".into(), V3ObjectKind::Manifest, &current).unwrap();
        assert_eq!(
            AuthenticatedV3Snapshot::decode(&current_ref, &current)
                .unwrap()
                .manifest()
                .encode()
                .unwrap(),
            current
        );
        assert_eq!(&current[super::super::V3_HEADER_LEN..][..4], b"PM11");
        let page = super::super::V3IndexPage {
            kind: V3ObjectKind::SourceStatsIndex,
            height: 0,
            records: vec![],
        }
        .encode()
        .unwrap();
        manifest.source = Some(source_attributes(
            V3ObjectRef::from_bytes("source".into(), V3ObjectKind::SourceStatsIndex, &page)
                .unwrap(),
        ));
        let bytes = manifest.encode().unwrap();
        let reference =
            V3ObjectRef::from_bytes("manifest".into(), V3ObjectKind::Manifest, &bytes).unwrap();
        assert_eq!(&bytes[super::super::V3_HEADER_LEN..][..4], b"PM11");
        assert_eq!(
            AuthenticatedV3Snapshot::decode(&reference, &bytes)
                .unwrap()
                .manifest(),
            &manifest
        );
        // Reauthenticate malformed bodies, so failures prove schema checks,
        // rather than merely the outer checksum rejecting a bit flip.
        let body = reference.verify(&bytes, V3_MANIFEST_BODY_LIMIT).unwrap();
        for case in 0..5 {
            let mut malformed = body.to_vec();
            match case {
                0 => malformed[..4].copy_from_slice(b"PM07"),
                1 => malformed[..4].copy_from_slice(b"PM09"),
                2 => malformed[..4].copy_from_slice(b"PM10"),
                3 => {
                    malformed.pop();
                }
                _ => {
                    malformed.push(0);
                }
            }
            let bytes =
                encode_v3_object(V3ObjectKind::Manifest, &malformed, V3_MANIFEST_BODY_LIMIT)
                    .unwrap();
            let reference =
                V3ObjectRef::from_bytes("invalid".into(), V3ObjectKind::Manifest, &bytes).unwrap();
            assert!(AuthenticatedV3Snapshot::decode(&reference, &bytes).is_err());
        }
        let source = manifest.source.as_mut().unwrap();
        source.root.inode = 2;
        assert!(manifest.encode().is_err());
        manifest.source.as_mut().unwrap().root.inode = 1;
        manifest.source.as_mut().unwrap().root.mode = 0o100750;
        assert!(manifest.encode().is_err());
        manifest.source.as_mut().unwrap().root.mode = 0o040750;
        manifest.source.as_mut().unwrap().allocations.kind = V3ObjectKind::InodeIndex;
        assert!(manifest.encode().is_err());
    }

    #[tokio::test]
    async fn pm11_missing_misbound_or_replaced_allocation_fails_closed() {
        use super::super::{V3IndexPage, V3IndexReader, V3IndexRecord, V3IndexValue};
        use crate::cadapter::localfs::LocalFsBackend;
        let temp = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(temp.path()));
        for case in 0..3 {
            let page = V3IndexPage {
                kind: V3ObjectKind::SourceStatsIndex,
                height: 0,
                records: if case == 0 {
                    vec![]
                } else {
                    vec![V3IndexRecord {
                        first_key: 2u64.to_be_bytes().to_vec(),
                        last_key: 2u64.to_be_bytes().to_vec(),
                        value: V3IndexValue::Leaf(
                            super::super::source_stat::encode_allocation(
                                if case == 1 { 3 } else { 2 },
                                8,
                            )
                            .unwrap(),
                        ),
                    }]
                },
            }
            .encode()
            .unwrap();
            let allocation_ref = V3ObjectRef::from_bytes(
                format!("allocations/{case}"),
                V3ObjectKind::SourceStatsIndex,
                &page,
            )
            .unwrap();
            client.put_object(&allocation_ref.key, &page).await.unwrap();
            let mut manifest = manifest();
            manifest.source = Some(source_attributes(allocation_ref.clone()));
            let bytes = manifest.encode().unwrap();
            let reference =
                V3ObjectRef::from_bytes("manifest".into(), V3ObjectKind::Manifest, &bytes).unwrap();
            let snapshot = AuthenticatedV3Snapshot::decode(&reference, &bytes).unwrap();
            let reader = V3IndexReader::new(client.clone(), 0);
            assert_eq!(snapshot.source_blocks(&reader, 1).await.unwrap(), Some(8));
            if case == 2 {
                assert_eq!(snapshot.source_blocks(&reader, 2).await.unwrap(), Some(8));
                let mut replaced = page.clone();
                replaced[super::super::V3_HEADER_LEN + 2] ^= 1;
                client
                    .put_object(&allocation_ref.key, &replaced)
                    .await
                    .unwrap();
            }
            assert!(snapshot.source_blocks(&reader, 2).await.is_err());
        }
        let legacy = manifest().encode().unwrap();
        let reference =
            V3ObjectRef::from_bytes("legacy".into(), V3ObjectKind::Manifest, &legacy).unwrap();
        let snapshot = AuthenticatedV3Snapshot::decode(&reference, &legacy).unwrap();
        let reader = V3IndexReader::new(client, 0);
        assert_eq!(snapshot.source_blocks(&reader, 2).await.unwrap(), None);
    }

    #[tokio::test]
    async fn manifest_lookup_walks_authenticated_root_without_unrelated_objects() {
        use super::super::{V3IndexPage, V3IndexReader, V3IndexRecord, V3IndexValue};
        use crate::cadapter::localfs::LocalFsBackend;
        let temp = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(temp.path()));
        let mut manifest = manifest();
        let page = V3IndexPage {
            kind: V3ObjectKind::InodeIndex,
            height: 0,
            records: vec![V3IndexRecord {
                first_key: vec![4],
                last_key: vec![4],
                value: V3IndexValue::Leaf(b"inode-four".to_vec()),
            }],
        };
        let page_bytes = page.encode().unwrap();
        let reference = V3ObjectRef::from_bytes("inodes".into(), page.kind, &page_bytes).unwrap();
        manifest.roots[V3RootKind::Inodes as usize] = reference.clone();
        client
            .put_object(&reference.key, &page_bytes)
            .await
            .unwrap();
        let bytes = manifest.encode().unwrap();
        let reference =
            V3ObjectRef::from_bytes("manifest".into(), V3ObjectKind::Manifest, &bytes).unwrap();
        client.put_object(&reference.key, &bytes).await.unwrap();
        let snapshot = AuthenticatedV3Snapshot::open(&client, &reference)
            .await
            .unwrap();
        let reader = V3IndexReader::new(client, 0);
        assert_eq!(
            snapshot
                .lookup(&reader, V3RootKind::Inodes, &[4])
                .await
                .unwrap()
                .map(|value| value.as_ref().to_vec()),
            Some(b"inode-four".to_vec())
        );
        assert_eq!(
            snapshot
                .lookup(&reader, V3RootKind::Inodes, &[5])
                .await
                .unwrap()
                .map(|value| value.as_ref().to_vec()),
            None
        );
    }

    #[tokio::test]
    async fn manifest_builder_to_authenticated_payload_rejects_directory_substitution() {
        use super::super::{
            V3FrameDirectoryPage, V3IndexBuilder, V3IndexReader, V3IndexRecord, V3IndexValue,
        };
        use crate::cadapter::{
            client::{ObjectBackend, ObjectByteStream},
            localfs::LocalFsBackend,
        };
        use crate::workspace_overlay::packed_v3::{
            PackedCodec, PackedFrameDescriptor, SizeClass, encode_block,
        };
        use sha2::{Digest, Sha256};
        use std::sync::{Arc, Mutex};
        #[derive(Clone)]
        struct RecordingBackend {
            inner: LocalFsBackend,
            ranges: Arc<Mutex<Vec<(String, u64, u64)>>>,
        }
        #[async_trait::async_trait]
        impl ObjectBackend for RecordingBackend {
            async fn put_object(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
                self.inner.put_object(key, bytes).await
            }
            async fn put_object_create_only(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
                self.inner.put_object_create_only(key, bytes).await
            }
            async fn get_object(&self, _key: &str) -> anyhow::Result<Option<Vec<u8>>> {
                anyhow::bail!("unexpected full GET")
            }
            async fn get_object_range(
                &self,
                key: &str,
                offset: u64,
                buf: &mut [u8],
            ) -> anyhow::Result<usize> {
                self.inner.get_object_range(key, offset, buf).await
            }
            async fn get_object_range_stream(
                &self,
                key: &str,
                offset: u64,
                length: u64,
            ) -> anyhow::Result<ObjectByteStream> {
                self.ranges
                    .lock()
                    .unwrap()
                    .push((key.to_owned(), offset, length));
                self.inner
                    .get_object_range_stream(key, offset, length)
                    .await
            }
            async fn get_etag(&self, key: &str) -> anyhow::Result<String> {
                self.inner.get_etag(key).await
            }
            async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
                self.inner.delete_object(key).await
            }
        }
        for codec in [PackedCodec::Raw, PackedCodec::Zstd] {
            let root = tempfile::tempdir().unwrap();
            let ranges = Arc::new(Mutex::new(Vec::new()));
            let client = ObjectClient::new(RecordingBackend {
                inner: LocalFsBackend::new(root.path()),
                ranges: Arc::clone(&ranges),
            });
            let raw = vec![0x59u8; 65536];
            let stored = encode_block(codec, &raw, 65536).unwrap();
            let bytes =
                encode_v3_object(V3ObjectKind::GroupContainer, &stored, 65536 + 1024).unwrap();
            let container =
                V3ObjectRef::from_bytes("payload".into(), V3ObjectKind::GroupContainer, &bytes)
                    .unwrap();
            client.put_object(&container.key, &bytes).await.unwrap();
            let directory = V3FrameDirectoryPage {
                container_digest: container.digest,
                container_len: container.object_len,
                profile: AccessProfile::RandomSmallFile,
                size_classes: SizeClassTable::default(),
                frame_policy: Default::default(),
                first_ordinal: 0,
                frames: vec![PackedFrameDescriptor {
                    frame_ordinal: 0,
                    object_offset: V3_HEADER_LEN as u64,
                    stored_len: stored.len() as u32,
                    raw_len: raw.len() as u32,
                    first_file_slot: 0,
                    last_file_slot: 0,
                    size_class: SizeClass::Tiny,
                    codec: codec as u8,
                    frame_digest: Sha256::digest(&stored)[..16].try_into().unwrap(),
                }],
            };
            let bytes = directory.encode().unwrap();
            let directory_ref =
                V3ObjectRef::from_bytes("descriptors".into(), V3ObjectKind::FrameDirectory, &bytes)
                    .unwrap();
            client.put_object(&directory_ref.key, &bytes).await.unwrap();
            let mut manifest = manifest();
            for (kind, key, value) in [
                (
                    V3RootKind::Containers,
                    0u32.to_be_bytes().to_vec(),
                    container.encode_value().unwrap(),
                ),
                (
                    V3RootKind::Frames,
                    vec![0; 8],
                    directory_ref.encode_value().unwrap(),
                ),
            ] {
                let mut builder = V3IndexBuilder::new(
                    client.clone(),
                    kind.object_kind(),
                    "indexes".into(),
                    2,
                    16384,
                )
                .unwrap();
                builder
                    .push(V3IndexRecord {
                        first_key: key.clone(),
                        last_key: key,
                        value: V3IndexValue::Leaf(value),
                    })
                    .await
                    .unwrap();
                manifest.roots[kind as usize] = builder.finish().await.unwrap();
            }
            let bytes = manifest.encode().unwrap();
            let reference =
                V3ObjectRef::from_bytes("manifest".into(), V3ObjectKind::Manifest, &bytes).unwrap();
            client.put_object(&reference.key, &bytes).await.unwrap();
            ranges.lock().unwrap().clear();
            let snapshot = AuthenticatedV3Snapshot::open(&client, &reference)
                .await
                .unwrap();
            let reader = V3IndexReader::new(client.clone(), 0);
            assert_eq!(
                snapshot
                    .read_frame(&client, &reader, 0, 0, 262144)
                    .await
                    .unwrap(),
                raw
            );
            let observed = ranges.lock().unwrap().clone();
            assert_eq!(observed.len(), 5);
            assert_eq!(
                observed
                    .iter()
                    .filter(|(key, _, _)| key == "payload")
                    .cloned()
                    .collect::<Vec<_>>(),
                vec![("payload".into(), V3_HEADER_LEN as u64, stored.len() as u64)]
            );
            ranges.lock().unwrap().clear();
            assert!(matches!(
                snapshot.read_frame(&client, &reader, 0, 0, 1).await,
                Err(PackedWireError::LimitExceeded(_))
            ));
            assert!(
                !ranges
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(key, _, _)| key == "payload")
            );
            let mut replacement = directory.clone();
            replacement.frames[0].frame_digest = [3; 16];
            client
                .put_object(&directory_ref.key, &replacement.encode().unwrap())
                .await
                .unwrap();
            assert!(matches!(
                snapshot.read_frame(&client, &reader, 0, 0, 262144).await,
                Err(PackedWireError::HashMismatch { .. })
            ));
        }
    }

    #[tokio::test]
    async fn published_gc05_group_inode_partial_inline_and_sparse_reads() {
        use super::super::{
            V3IndexBuilder, V3IndexReader, V3IndexRecord, V3IndexValue, V3InodeLocation,
            build_v3_container,
        };
        use crate::cadapter::localfs::LocalFsBackend;
        use crate::workspace_overlay::packed_v3::{
            GroupMeta, PackedCodec, PackedFileInput, PackedInodeIndexEntry, pack_group_files,
        };
        for (size, sparse, codec) in [
            (200 * 1024, false, PackedCodec::Zstd),
            (512 * 1024, true, PackedCodec::Raw),
            (10 * 1024 * 1024, false, PackedCodec::Zstd),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let client = ObjectClient::new(LocalFsBackend::new(temp.path()));
            let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
            let (mut group, frames) = pack_group_files(
                7,
                [2; 32],
                vec![PackedFileInput {
                    name: b"file".to_vec(),
                    inode: 3,
                    kind: 1,
                    mode: 0o100644,
                    uid: 1000,
                    gid: 1000,
                    rdev: 0,
                    nlink: 1,
                    atime_ns: 1,
                    mtime_ns: 2,
                    ctime_ns: 3,
                    flags: 0,
                    data: data.clone(),
                }],
                AccessProfile::RandomSmallFile,
                SizeClassTable::default(),
                None,
            )
            .unwrap();
            if sparse {
                let mut metadata = GroupMeta::decode(&group.metadata).unwrap();
                let entry = &mut metadata.entries_mut()[0];
                entry.size += 8192;
                for extent in &mut entry.extents {
                    extent.file_offset += 4096;
                }
                group.metadata = metadata.encode().unwrap();
            }
            let built = build_v3_container(
                0,
                8,
                AccessProfile::RandomSmallFile,
                SizeClassTable::default(),
                (codec, codec),
                &[group.clone()],
                &frames,
            )
            .unwrap();
            let container = V3ObjectRef::from_bytes(
                "container".into(),
                V3ObjectKind::GroupContainer,
                &built.bytes,
            )
            .unwrap();
            client
                .put_object(&container.key, &built.bytes)
                .await
                .unwrap();
            let original = GroupMeta::decode(&group.metadata).unwrap();
            let entry = &original.entries()[0];
            let reference = built.groups[0].clone();
            let inode = V3InodeLocation {
                hot: PackedInodeIndexEntry {
                    inode: entry.inode,
                    parent_inode: 1,
                    parent_dir_key: [2; 32],
                    group_id: reference.group_id,
                    entry_ordinal: 0,
                    name: entry.name.clone(),
                    kind: entry.kind,
                    mode: entry.mode,
                    uid: entry.uid,
                    gid: entry.gid,
                    rdev: entry.rdev,
                    nlink: entry.nlink,
                    atime_ns: entry.atime_ns,
                    mtime_ns: entry.mtime_ns,
                    ctime_ns: entry.ctime_ns,
                    size: entry.size,
                },
                group: reference.clone(),
            };
            assert_eq!(
                V3InodeLocation::decode_value(&inode.encode_value().unwrap()).unwrap(),
                inode
            );
            let mut rows: Vec<Vec<V3IndexRecord>> = vec![Vec::new(); V3_ROOT_COUNT];
            let mut first = reference.parent_dir_key.to_vec();
            first.extend_from_slice(&reference.first_name);
            let mut last = reference.parent_dir_key.to_vec();
            last.extend_from_slice(&reference.last_name);
            rows[V3RootKind::Groups as usize].push(V3IndexRecord {
                first_key: first,
                last_key: last,
                value: V3IndexValue::Leaf(reference.encode_value().unwrap()),
            });
            rows[V3RootKind::Inodes as usize].push(V3IndexRecord {
                first_key: 3u64.to_be_bytes().to_vec(),
                last_key: 3u64.to_be_bytes().to_vec(),
                value: V3IndexValue::Leaf(inode.encode_value().unwrap()),
            });
            rows[V3RootKind::Containers as usize].push(V3IndexRecord {
                first_key: vec![0; 4],
                last_key: vec![0; 4],
                value: V3IndexValue::Leaf(container.encode_value().unwrap()),
            });
            for (i, page) in built.frame_pages.iter().enumerate() {
                let bytes = page.encode().unwrap();
                let reference = V3ObjectRef::from_bytes(
                    format!("descriptors/{i}"),
                    V3ObjectKind::FrameDirectory,
                    &bytes,
                )
                .unwrap();
                client.put_object(&reference.key, &bytes).await.unwrap();
                let mut first = vec![0; 4];
                first.extend_from_slice(&page.first_ordinal.to_be_bytes());
                let mut last = vec![0; 4];
                last.extend_from_slice(&page.frames.last().unwrap().frame_ordinal.to_be_bytes());
                rows[V3RootKind::Frames as usize].push(V3IndexRecord {
                    first_key: first,
                    last_key: last,
                    value: V3IndexValue::Leaf(reference.encode_value().unwrap()),
                });
            }
            let mut manifest = manifest();
            for (kind, records) in ROOT_KINDS.into_iter().zip(rows) {
                let mut builder = V3IndexBuilder::new(
                    client.clone(),
                    kind.object_kind(),
                    "indexes".into(),
                    2,
                    16384,
                )
                .unwrap();
                for record in records {
                    builder.push(record).await.unwrap();
                }
                manifest.roots[kind as usize] = builder.finish().await.unwrap();
            }
            let bytes = manifest.encode().unwrap();
            let root =
                V3ObjectRef::from_bytes("manifest".into(), V3ObjectKind::Manifest, &bytes).unwrap();
            client.put_object(&root.key, &bytes).await.unwrap();
            let snapshot = AuthenticatedV3Snapshot::open(&client, &root).await.unwrap();
            let reader = V3IndexReader::new(client.clone(), 0);
            let found = snapshot
                .lookup_dentry(&client, &reader, [2; 32], b"file", 512 * 1024)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(&found, entry);
            let mut logical = if sparse { vec![0; 4096] } else { vec![] };
            logical.extend_from_slice(&data);
            if sparse {
                logical.resize(logical.len() + 4096, 0);
            }
            for (offset, length) in [
                (0, 16),
                (4090, 20),
                (logical.len() - 17, 17),
                (4 * 1024 * 1024 - 7, 20),
            ] {
                if offset + length > logical.len() {
                    continue;
                }
                let prepared = snapshot
                    .prepare_inode_read(
                        &client,
                        &reader,
                        3,
                        offset as u64,
                        length,
                        32 * 1024 * 1024,
                    )
                    .await
                    .unwrap();
                let mut cursor = offset as u64;
                for segment in &prepared.plan.segments {
                    assert_eq!(segment.logical_offset, cursor);
                    cursor = segment.end().unwrap();
                }
                assert_eq!(cursor, (offset + length) as u64);
                if sparse && (offset + length <= 4096 || offset >= 4096 + size) {
                    assert_eq!(prepared.plan.segments.len(), 1);
                    assert_eq!(
                        prepared.plan.segments[0].source,
                        crate::chunk::read_plan::ReadSource::Hole
                    );
                }
                drop(prepared);
                let mut output = vec![0xff; length];
                snapshot
                    .read_inode_range(
                        &client,
                        &reader,
                        3,
                        offset as u64,
                        &mut output,
                        32 * 1024 * 1024,
                    )
                    .await
                    .unwrap();
                assert_eq!(&output, &logical[offset..offset + length]);
            }
            let mut output = vec![0xff; 16];
            assert!(
                snapshot
                    .read_inode_range(&client, &reader, 3, 0, &mut output, 1)
                    .await
                    .is_err()
            );
            assert_eq!(output, vec![0xff; 16]);
            if size == 10 * 1024 * 1024 {
                // Eager preparation retains both crossing frames and fails
                // this legal request despite space for output plus one frame.
                let mut bounded_output = vec![0; 4 << 20];
                snapshot
                    .read_inode_range(&client, &reader, 3, 1 << 20, &mut bounded_output, 8 << 20)
                    .await
                    .unwrap();
                assert_eq!(&bounded_output, &logical[1 << 20..5 << 20]);
                // A 4MiB negotiated request crosses two full frames. The
                // caller's output owner and the one active decoder owner
                // survive independently, including a slow reply consumer.
                let mut limits = super::super::V3BudgetLimits::default();
                limits.bytes[super::super::V3BudgetPool::Raw as usize] = 4 << 20;
                let budget = super::super::V3MountBudget::new(limits).unwrap();
                let reader = V3IndexReader::with_budget(client.clone(), 0, budget.clone());
                let offset = 1024 * 1024;
                let length = 4 * 1024 * 1024;
                let output_permit = budget.output(length).unwrap();
                let prepared = snapshot
                    .prepare_inode_read(&client, &reader, 3, offset as u64, length, 32 << 20)
                    .await
                    .unwrap();
                assert_eq!(
                    budget.state().used[super::super::V3BudgetPool::Raw as usize],
                    0
                );
                let mut output = vec![0; length];
                crate::chunk::read_plan::execute_unified_into(
                    prepared.fetcher.as_ref(),
                    offset as u64,
                    &prepared.plan,
                    &mut output,
                )
                .await
                .unwrap();
                assert_eq!(&output, &logical[offset..offset + length]);
                assert_eq!(
                    budget.state().peak[super::super::V3BudgetPool::Raw as usize],
                    4 << 20
                );
                let bytes = bytes::Bytes::from_owner(super::super::V3OwnedBytes {
                    data: output,
                    permit: output_permit,
                });
                let consumer = bytes.clone();
                drop(prepared);
                drop(bytes);
                assert_eq!(
                    budget.state().used[super::super::V3BudgetPool::Raw as usize],
                    0
                );
                assert_eq!(
                    budget.state().used[super::super::V3BudgetPool::Output as usize],
                    length as u64
                );
                assert_eq!(&consumer[..32], &logical[offset..offset + 32]);
                drop(consumer);
                // The reader owns the mount-lifetime lazy coordinator. Join
                // its worker and release that owner before asserting all pools.
                reader.close().await;
                drop(reader);
                assert_eq!(budget.state().used, [0; 8]);
            }
            assert!(
                snapshot
                    .lookup_dentry(&client, &reader, [2; 32], b"missing", 512 * 1024)
                    .await
                    .unwrap()
                    .is_none()
            );
            let mut mismatched = inode.clone();
            mismatched.hot.size += 1;
            assert!(mismatched.validate_entry(entry).is_err());
        }
    }

    #[test]
    fn manifest_roundtrip_pins_generation_to_content_digest() {
        let manifest = manifest();
        let bytes = manifest.encode().unwrap();
        let reference =
            V3ObjectRef::from_bytes("manifest".into(), V3ObjectKind::Manifest, &bytes).unwrap();
        let snapshot = AuthenticatedV3Snapshot::decode(&reference, &bytes).unwrap();
        assert_eq!(snapshot.manifest(), &manifest);
        assert_eq!(
            snapshot.read_generation(),
            ReadGeneration::readonly(reference.digest)
        );
        assert_ne!(
            snapshot.read_generation().lower_snapshot,
            manifest.snapshot_id
        );
        let mut other = manifest.clone();
        other.root_inode = 2;
        let replaced = other.encode().unwrap();
        assert!(matches!(
            AuthenticatedV3Snapshot::decode(&reference, &replaced),
            Err(PackedWireError::HashMismatch { .. })
        ));
    }

    #[test]
    fn manifest_rejects_wrong_root_kind_identity_and_budgets() {
        let valid = manifest();
        for field in 0..5 {
            let mut bad = valid.clone();
            match field {
                0 => bad.roots[0].kind = V3ObjectKind::LargeData,
                1 => bad.roots[0].digest = [0; 32],
                2 => bad.roots[0].object_len = u64::MAX,
                3 => bad.root_inode = 0,
                _ => bad.roots[0].key = "../outside".into(),
            }
            assert!(bad.encode().is_err());
        }
    }

    #[tokio::test]
    async fn pm10_deep_cookie_counts_actual_range_gets_and_fetches_only_selected_metadata() {
        use super::super::{
            V3IndexBuilder, V3IndexPage, V3IndexReader, V3IndexRecord, V3IndexValue,
            build_v3_container,
        };
        use crate::cadapter::{
            client::{ObjectBackend, ObjectByteStream},
            localfs::LocalFsBackend,
        };
        use crate::workspace_overlay::packed_v3::{PackedCodec, PackedFileInput, pack_group_files};
        use std::sync::{Arc, Mutex};
        #[derive(Clone)]
        struct RecordingBackend {
            inner: LocalFsBackend,
            ranges: Arc<Mutex<Vec<(String, u64, u64)>>>,
        }
        #[async_trait::async_trait]
        impl ObjectBackend for RecordingBackend {
            async fn put_object(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
                self.inner.put_object(key, bytes).await
            }
            async fn put_object_create_only(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
                self.inner.put_object_create_only(key, bytes).await
            }
            async fn get_object(&self, _key: &str) -> anyhow::Result<Option<Vec<u8>>> {
                anyhow::bail!("unexpected full GET during counted directory read")
            }
            async fn get_object_range(
                &self,
                key: &str,
                offset: u64,
                buf: &mut [u8],
            ) -> anyhow::Result<usize> {
                self.inner.get_object_range(key, offset, buf).await
            }
            async fn get_object_range_stream(
                &self,
                key: &str,
                offset: u64,
                length: u64,
            ) -> anyhow::Result<ObjectByteStream> {
                self.ranges
                    .lock()
                    .unwrap()
                    .push((key.to_owned(), offset, length));
                self.inner
                    .get_object_range_stream(key, offset, length)
                    .await
            }
            async fn get_etag(&self, key: &str) -> anyhow::Result<String> {
                self.inner.get_etag(key).await
            }
            async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
                self.inner.delete_object(key).await
            }
        }
        for codec in [PackedCodec::Raw, PackedCodec::Zstd] {
            let temp = tempfile::tempdir().unwrap();
            let ranges = Arc::new(Mutex::new(Vec::new()));
            let client = ObjectClient::new(RecordingBackend {
                inner: LocalFsBackend::new(temp.path()),
                ranges: ranges.clone(),
            });
            // Tiny fanout forces multiple actual index levels without a huge
            // fixture. This routing fixture intentionally constructs only the
            // groups/containers closure read by readdir, not a publishable tree.
            let mut groups = V3IndexBuilder::new(
                client.clone(),
                V3ObjectKind::GroupIndex,
                "deep/groups".into(),
                2,
                16 * 1024,
            )
            .unwrap();
            let mut containers = V3IndexBuilder::new(
                client.clone(),
                V3ObjectKind::ContainerIndex,
                "deep/containers".into(),
                2,
                16 * 1024,
            )
            .unwrap();
            let mut oracle = Vec::new();
            let mut references = Vec::new();
            let mut payloads = Vec::new();
            let mut total = 0u64;
            for ordinal in 0u32..38 {
                let parent = if ordinal < 37 { [2; 32] } else { [3; 32] };
                let count = 1 + ordinal % 7;
                let files: Vec<_> = (0..count)
                    .map(|member| {
                        let mut name = format!("{ordinal:05}-{member:03}").into_bytes();
                        if member == 1 {
                            name.push(0xff);
                        }
                        if ordinal == 5 && member == 0 {
                            name.resize(255, b'z');
                        }
                        if parent == [2; 32] {
                            oracle.push(name.clone());
                        }
                        PackedFileInput {
                            name,
                            inode: 10 + u64::from(ordinal) * 8 + u64::from(member),
                            kind: 1,
                            mode: 0o100644,
                            uid: 0,
                            gid: 0,
                            rdev: 0,
                            nlink: 1,
                            atime_ns: 0,
                            mtime_ns: 0,
                            ctime_ns: 0,
                            flags: 0,
                            data: Vec::new(),
                        }
                    })
                    .collect();
                let (group, frames) = pack_group_files(
                    u64::from(ordinal) + 1,
                    parent,
                    files,
                    AccessProfile::RandomSmallFile,
                    SizeClassTable::default(),
                    None,
                )
                .unwrap();
                let built = build_v3_container(
                    ordinal,
                    u64::from(ordinal) + 1,
                    AccessProfile::RandomSmallFile,
                    SizeClassTable::default(),
                    (codec, codec),
                    &[group],
                    &frames,
                )
                .unwrap();
                let payload = V3ObjectRef::from_bytes(
                    format!("payload/{ordinal}"),
                    V3ObjectKind::GroupContainer,
                    &built.bytes,
                )
                .unwrap();
                client.put_object(&payload.key, &built.bytes).await.unwrap();
                let reference = built.groups[0].clone();
                let mut first = parent.to_vec();
                first.extend_from_slice(&reference.first_name);
                let mut last = parent.to_vec();
                last.extend_from_slice(&reference.last_name);
                groups
                    .push(V3IndexRecord {
                        first_key: first,
                        last_key: last,
                        value: V3IndexValue::Leaf(reference.encode_value().unwrap()),
                    })
                    .await
                    .unwrap();
                let key = ordinal.to_be_bytes().to_vec();
                containers
                    .push(V3IndexRecord {
                        first_key: key.clone(),
                        last_key: key,
                        value: V3IndexValue::Leaf(payload.encode_value().unwrap()),
                    })
                    .await
                    .unwrap();
                total += u64::from(count);
                references.push(reference);
                payloads.push((payload, built.bytes));
            }
            let mut data = manifest();
            data.roots[V3RootKind::Groups as usize] = groups.finish().await.unwrap();
            data.roots[V3RootKind::Containers as usize] = containers.finish().await.unwrap();
            data.group_dentry_count = total;
            let bytes = data.encode().unwrap();
            let reference =
                V3ObjectRef::from_bytes("manifest".into(), V3ObjectKind::Manifest, &bytes).unwrap();
            let snapshot = AuthenticatedV3Snapshot::decode(&reference, &bytes).unwrap();
            let mut heights = Vec::new();
            for kind in [V3RootKind::Groups, V3RootKind::Containers] {
                let reference = snapshot.root(kind);
                let bytes =
                    read_v3_page(&client, reference, super::super::index::V3_INDEX_BODY_LIMIT)
                        .await
                        .unwrap();
                heights.push(usize::from(
                    V3IndexPage::decode(reference, &bytes).unwrap().height,
                ));
            }
            assert!(heights.iter().all(|height| *height >= 3));
            for offset in [0, oracle.len() / 2, oracle.len() - 1, oracle.len() - 1] {
                ranges.lock().unwrap().clear();
                let reader = V3IndexReader::new(client.clone(), 0);
                let found = snapshot
                    .readdir_page(&client, &reader, [2; 32], offset as u64, 1, 512 * 1024)
                    .await
                    .unwrap();
                assert_eq!(found.len(), 1);
                assert_eq!(found[0].name, oracle[offset]);
                let requested = ranges.lock().unwrap().clone();
                let group_gets = requested
                    .iter()
                    .filter(|(key, _, _)| key.starts_with("deep/groups/"))
                    .count();
                let container_gets = requested
                    .iter()
                    .filter(|(key, _, _)| key.starts_with("deep/containers/"))
                    .count();
                let metadata_gets: Vec<_> = requested
                    .iter()
                    .filter(|(key, _, _)| key.starts_with("payload/"))
                    .collect();
                assert!(group_gets <= 2 * (heights[0] + 1), "{requested:?}");
                assert!(container_gets <= heights[1] + 1, "{requested:?}");
                assert_eq!(metadata_gets.len(), 1);
                let selected = references
                    .iter()
                    .find(|group| {
                        group.parent_dir_key == [2; 32]
                            && group.first_name <= found[0].name
                            && found[0].name <= group.last_name
                    })
                    .unwrap();
                assert_eq!(
                    metadata_gets[0],
                    &(
                        format!("payload/{}", selected.container_ordinal),
                        selected.meta_offset,
                        u64::from(selected.meta_stored_len)
                    )
                );
                assert_eq!(
                    requested.len(),
                    group_gets + container_gets + metadata_gets.len()
                );
                assert_eq!(reader.resident_bytes(), 0);
            }
            let reader = V3IndexReader::new(client.clone(), 0);
            let second_directory = snapshot
                .readdir_page(&client, &reader, [3; 32], 1, 1, 512 * 1024)
                .await
                .unwrap();
            let mut expected_second = b"00037-001".to_vec();
            expected_second.push(0xff);
            assert_eq!(second_directory.len(), 1);
            assert_eq!(second_directory[0].name, expected_second);
            ranges.lock().unwrap().clear();
            let crossing = snapshot
                .readdir_page(&client, &reader, [2; 32], 0, 3, 512 * 1024)
                .await
                .unwrap();
            assert_eq!(
                crossing
                    .into_iter()
                    .map(|entry| entry.name)
                    .collect::<Vec<_>>(),
                oracle[..3]
            );
            assert_eq!(
                ranges
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(key, _, _)| key.starts_with("payload/"))
                    .count(),
                2
            );
            for offset in [oracle.len() as u64, V3_MAX_DIRECTORY_ORDINAL] {
                ranges.lock().unwrap().clear();
                assert!(
                    snapshot
                        .readdir_page(&client, &reader, [2; 32], offset, 1, 512 * 1024)
                        .await
                        .unwrap()
                        .is_empty()
                );
                assert!(
                    ranges
                        .lock()
                        .unwrap()
                        .iter()
                        .all(|(key, _, _)| key.starts_with("deep/groups/"))
                );
            }
            ranges.lock().unwrap().clear();
            assert!(
                snapshot
                    .readdir_page(
                        &client,
                        &reader,
                        [2; 32],
                        V3_MAX_DIRECTORY_ORDINAL + 1,
                        1,
                        512 * 1024
                    )
                    .await
                    .is_err()
            );
            assert!(ranges.lock().unwrap().is_empty());
            let mut wrong_count = data;
            wrong_count.group_dentry_count += 1;
            let bytes = wrong_count.encode().unwrap();
            let reference =
                V3ObjectRef::from_bytes("wrong-count".into(), V3ObjectKind::Manifest, &bytes)
                    .unwrap();
            let wrong = AuthenticatedV3Snapshot::decode(&reference, &bytes).unwrap();
            assert!(
                wrong
                    .readdir_page(&client, &reader, [2; 32], 0, 1, 512 * 1024)
                    .await
                    .is_err()
            );
            // A deep cookie must not depend on predecessor payloads existing.
            for (payload, _) in &payloads[..36] {
                client.delete_object(&payload.key).await.unwrap();
            }
            assert_eq!(
                snapshot
                    .readdir_page(
                        &client,
                        &reader,
                        [2; 32],
                        oracle.len() as u64 - 1,
                        1,
                        512 * 1024
                    )
                    .await
                    .unwrap()[0]
                    .name,
                *oracle.last().unwrap()
            );
            let (payload, bytes) = &payloads[36];
            let mut corrupt = bytes.clone();
            corrupt[references[36].meta_offset as usize] ^= 1;
            client.put_object(&payload.key, &corrupt).await.unwrap();
            assert!(
                snapshot
                    .readdir_page(
                        &client,
                        &reader,
                        [2; 32],
                        oracle.len() as u64 - 1,
                        1,
                        512 * 1024
                    )
                    .await
                    .is_err()
            );
            client.delete_object(&payload.key).await.unwrap();
            assert!(
                snapshot
                    .readdir_page(
                        &client,
                        &reader,
                        [2; 32],
                        oracle.len() as u64 - 1,
                        1,
                        512 * 1024
                    )
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn manifest_to_root_read_rejects_substitution_with_fresh_internal_hashes() {
        use crate::cadapter::localfs::LocalFsBackend;
        let temp = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(temp.path()));
        let manifest = manifest();
        for (root, kind) in manifest.roots.iter().zip(ROOT_KINDS) {
            client
                .put_object(
                    &root.key,
                    &super::super::V3IndexPage {
                        kind: kind.object_kind(),
                        height: 0,
                        records: Vec::new(),
                    }
                    .encode()
                    .unwrap(),
                )
                .await
                .unwrap();
        }
        let bytes = manifest.encode().unwrap();
        let reference =
            V3ObjectRef::from_bytes("manifest".into(), V3ObjectKind::Manifest, &bytes).unwrap();
        client.put_object(&reference.key, &bytes).await.unwrap();
        let snapshot = AuthenticatedV3Snapshot::open(&client, &reference)
            .await
            .unwrap();
        assert!(
            snapshot
                .read_root(&client, V3RootKind::Frames)
                .await
                .is_ok()
        );
        let root = snapshot.root(V3RootKind::Frames);
        client
            .put_object(
                &root.key,
                &encode_v3_object(root.kind, b"evil-objects", 256 * 1024).unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            snapshot.read_root(&client, V3RootKind::Frames).await,
            Err(PackedWireError::HashMismatch { .. })
        ));
    }
}
