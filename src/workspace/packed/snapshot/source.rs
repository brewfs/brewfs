//! Source entry point: bounded group admission with automatic external routing.

use super::{
    CapturedSourceFile, CapturedSourceLayout, SnapshotColdAttributes, SnapshotSourceFileLimits,
};
use crate::workspace::packed::wire::{PackedResult, PackedWireError};
use crate::workspace::packed::{
    AccessProfile, GroupMeta, GroupMetaEntry, PackedGroupInput, SizeClassTable,
};
use std::path::Path;

pub enum CapturedSource {
    // Both captures are large and short-lived; box them so the enum itself
    // stays pointer-sized whichever route admission selects.
    Group(Box<CapturedSourceFile>),
    External(Box<CapturedSourceLayout>),
}

#[cfg(test)]
#[path = "source/tests.rs"]
mod tests;

impl CapturedSource {
    pub async fn capture(
        path: &Path,
        temporary: &Path,
        inode: u64,
        profile: AccessProfile,
        classes: SizeClassTable,
        limits: SnapshotSourceFileLimits,
    ) -> PackedResult<Self> {
        Self::capture_with_policy(
            path,
            temporary,
            inode,
            profile,
            classes,
            limits,
            super::SnapshotBuildPolicy::default(),
        )
        .await
    }
    pub async fn capture_with_policy(
        path: &Path,
        temporary: &Path,
        inode: u64,
        profile: AccessProfile,
        classes: SizeClassTable,
        limits: SnapshotSourceFileLimits,
        policy: super::SnapshotBuildPolicy,
    ) -> PackedResult<Self> {
        let source_path = path.to_owned();
        let group = tokio::task::spawn_blocking(move || {
            CapturedSourceFile::capture_for_placement(
                &source_path,
                inode,
                profile,
                classes,
                limits,
                policy,
            )
        })
        .await
        .map_err(|_| PackedWireError::Backend("source admission task failed".into()))??;
        match group {
            Some(group) => Ok(Self::Group(Box::new(group))),
            None => Ok(Self::External(Box::new(
                CapturedSourceLayout::capture_with_policy(
                    path, temporary, inode, profile, classes, policy,
                )
                .await?,
            ))),
        }
    }
    pub fn entry(&self) -> &GroupMetaEntry {
        match self {
            Self::Group(source) => source.entry(),
            Self::External(source) => source.entry(),
        }
    }
    pub fn cold_attributes(&self) -> &SnapshotColdAttributes {
        match self {
            Self::Group(source) => source.cold_attributes(),
            Self::External(source) => source.cold_attributes(),
        }
    }
    pub fn source_blocks(&self) -> u64 {
        match self {
            Self::Group(source) => source.source_blocks(),
            Self::External(source) => source.source_blocks(),
        }
    }
    pub fn source_nlink(&self) -> u64 {
        match self {
            Self::Group(source) => source.source_nlink(),
            Self::External(source) => source.source_nlink(),
        }
    }
    pub fn data_bytes(&self) -> u64 {
        match self {
            Self::Group(source) => source
                .frames()
                .iter()
                .map(|frame| frame.raw.len() as u64)
                .sum(),
            Self::External(source) => source.data_bytes(),
        }
    }
    pub fn extent_count(&self) -> u64 {
        match self {
            Self::Group(source) => source.entry().extents.len() as u64,
            Self::External(source) => source.frame_count(),
        }
    }
    pub fn validate_unchanged(&self) -> PackedResult<()> {
        match self {
            Self::Group(source) => source.validate_unchanged(),
            Self::External(source) => source.validate_unchanged(),
        }
    }
    pub fn group(
        &self,
        group_id: u64,
        parent: [u8; 32],
        profile: AccessProfile,
    ) -> PackedResult<PackedGroupInput> {
        match self {
            Self::Group(source) => source.group(group_id, parent, profile),
            Self::External(source) => Ok(PackedGroupInput {
                group_id,
                parent_dir_key: parent,
                layout_profile: profile,
                file_count: 1,
                entry_count: 1,
                frame_ordinals: vec![],
                // Producer input is the canonical GM06 struct encoding; the
                // container builder creates the stored GM07 restart payload.
                metadata: GroupMeta::new(vec![source.entry().clone()])?.encode()?,
            }),
        }
    }
}
