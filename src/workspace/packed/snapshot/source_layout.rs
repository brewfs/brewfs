//! Disk-paged SEEK_DATA/HOLE inventory with one bounded source frame at a time.

use std::fs::{File, Metadata};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::Path;
use std::sync::Arc;

use sea_orm::sqlx::Row;

use super::source_root::{SnapshotSourceEntry, SnapshotSourceLocation};
use super::{SnapshotColdAttributes, SnapshotIndexSpool};
use crate::workspace::packed::wire::{PackedResult, PackedWireError};
use crate::workspace::packed::{
    AccessProfile, GroupMeta, GroupMetaEntry, PackedFrameInput, SizeClass, SizeClassTable,
};

pub struct CapturedSourceLayout {
    file: Arc<File>,
    location: SnapshotSourceLocation,
    before: Metadata,
    entry: GroupMetaEntry,
    cold: SnapshotColdAttributes,
    spool: Box<SnapshotIndexSpool>,
    frame_bytes: u64,
    policy: super::SnapshotBuildPolicy,
    size_class: SizeClass,
    data_bytes: u64,
    frame_count: u64,
    after_run: Option<Vec<u8>>,
    active_run: Option<(u64, u64, u64)>,
}

fn failure(what: &str) -> PackedWireError {
    PackedWireError::Backend(format!("external source {what} failed"))
}

impl CapturedSourceLayout {
    pub async fn capture(
        path: &Path,
        temporary: &Path,
        inode: u64,
        profile: AccessProfile,
        classes: SizeClassTable,
    ) -> PackedResult<Self> {
        Self::capture_with_policy(
            path,
            temporary,
            inode,
            profile,
            classes,
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
        policy: super::SnapshotBuildPolicy,
    ) -> PackedResult<Self> {
        Self::capture_location(
            SnapshotSourceLocation::Path(path.to_owned()),
            temporary,
            inode,
            profile,
            classes,
            policy,
        )
        .await
    }

    pub(super) async fn capture_at(
        entry: SnapshotSourceEntry,
        temporary: &Path,
        inode: u64,
        profile: AccessProfile,
        classes: SizeClassTable,
        policy: super::SnapshotBuildPolicy,
    ) -> PackedResult<Self> {
        Self::capture_location(
            SnapshotSourceLocation::Rooted(entry),
            temporary,
            inode,
            profile,
            classes,
            policy,
        )
        .await
    }

    async fn capture_location(
        location: SnapshotSourceLocation,
        temporary: &Path,
        inode: u64,
        profile: AccessProfile,
        classes: SizeClassTable,
        policy: super::SnapshotBuildPolicy,
    ) -> PackedResult<Self> {
        let name = location.basename()?;
        super::super::meta::validate_name(&name)?;
        if name.len() > crate::posix::NAME_MAX {
            return Err(PackedWireError::LimitExceeded(
                "external source name exceeds NAME_MAX".into(),
            ));
        }
        let opening_location = location.clone();
        let (file, before, cold) = tokio::task::spawn_blocking(move || {
            let file = opening_location.open_regular()?;
            let before = file.metadata().map_err(|_| failure("stat"))?;
            if !before.is_file() || before.len() > i64::MAX as u64 {
                return Err(PackedWireError::UnsupportedFormat(
                    "external source requires a regular file within off_t".into(),
                ));
            }
            let cold = super::source_file::capture_xattrs(&file, inode)?;
            opening_location.validate(&file, &before)?;
            Ok::<_, PackedWireError>((Arc::new(file), before, cold))
        })
        .await
        .map_err(|_| failure("open task"))??;
        let entry = GroupMetaEntry {
            name,
            inode,
            kind: 1,
            mode: before.mode(),
            uid: before.uid(),
            gid: before.gid(),
            rdev: 0,
            nlink: 1,
            atime_ns: super::source_file::ns(before.atime(), before.atime_nsec())?,
            mtime_ns: super::source_file::ns(before.mtime(), before.mtime_nsec())?,
            ctime_ns: super::source_file::ns(before.ctime(), before.ctime_nsec())?,
            size: before.len(),
            flags: 0,
            inline_data: Arc::from([]),
            extents: vec![],
        };
        GroupMeta::new(vec![entry.clone()])?.encode_restart()?;
        let decision = policy.select(before.len(), profile, classes)?;
        let spool = SnapshotIndexSpool::create(temporary).await?;
        sea_orm::sqlx::query(
            "CREATE TABLE source_runs (start BLOB PRIMARY KEY, end BLOB NOT NULL) WITHOUT ROWID",
        )
        .execute(&spool.pool)
        .await
        .map_err(|_| failure("run schema"))?;
        let mut cursor = 0u64;
        let mut data_bytes = 0u64;
        let mut frame_count = 0u64;
        while cursor < before.len() {
            let fd = file.clone();
            let run = tokio::task::spawn_blocking(move || {
                let Some(start) = super::source_file::seek(&fd, cursor, libc::SEEK_DATA)? else {
                    return Ok(None);
                };
                let end = super::source_file::seek(&fd, start, libc::SEEK_HOLE)?
                    .ok_or_else(|| failure("hole query"))?;
                Ok::<_, PackedWireError>(Some((start, end)))
            })
            .await
            .map_err(|_| failure("run task"))??;
            let Some((start, end)) = run else {
                break;
            };
            let end = end.min(before.len());
            if start < cursor || start >= end {
                return Err(PackedWireError::Invalid(
                    "external source run bounds changed".into(),
                ));
            }
            let length = end - start;
            data_bytes = data_bytes
                .checked_add(length)
                .ok_or_else(|| failure("data total overflow"))?;
            frame_count = frame_count
                .checked_add(length.div_ceil(decision.frame_raw_bytes))
                .ok_or_else(|| failure("frame total overflow"))?;
            sea_orm::sqlx::query("INSERT INTO source_runs(start,end) VALUES(?,?)")
                .bind(start.to_be_bytes().as_slice())
                .bind(end.to_be_bytes().as_slice())
                .execute(&spool.pool)
                .await
                .map_err(|_| failure("run insert"))?;
            cursor = end;
        }
        let result = Self {
            file,
            location,
            before,
            entry,
            cold,
            spool: Box::new(spool),
            frame_bytes: decision.frame_raw_bytes,
            policy,
            size_class: decision.size_class,
            data_bytes,
            frame_count,
            after_run: None,
            active_run: None,
        };
        result.validate_unchanged()?;
        Ok(result)
    }

    pub fn entry(&self) -> &GroupMetaEntry {
        &self.entry
    }
    pub fn cold_attributes(&self) -> &SnapshotColdAttributes {
        &self.cold
    }
    pub fn source_metadata(&self) -> &Metadata {
        &self.before
    }
    pub fn source_blocks(&self) -> u64 {
        self.before.blocks()
    }
    pub fn source_nlink(&self) -> u64 {
        self.before.nlink()
    }
    pub fn data_bytes(&self) -> u64 {
        self.data_bytes
    }
    pub fn frame_count(&self) -> u64 {
        self.frame_count
    }
    pub fn build_policy(&self) -> super::SnapshotBuildPolicy {
        self.policy
    }
    pub fn frame_target(&self) -> u64 {
        self.frame_bytes
    }
    pub fn validate_unchanged(&self) -> PackedResult<()> {
        self.location.validate(&self.file, &self.before)
    }

    pub async fn next_frame(&mut self) -> PackedResult<Option<(u64, PackedFrameInput)>> {
        self.validate_unchanged()?;
        if self.active_run.is_none() {
            let row = if let Some(key) = &self.after_run {
                sea_orm::sqlx::query(
                    "SELECT start,end FROM source_runs WHERE start>? ORDER BY start LIMIT 1",
                )
                .bind(key)
                .fetch_optional(&self.spool.pool)
                .await
            } else {
                sea_orm::sqlx::query("SELECT start,end FROM source_runs ORDER BY start LIMIT 1")
                    .fetch_optional(&self.spool.pool)
                    .await
            }
            .map_err(|_| failure("run scan"))?;
            let Some(row) = row else {
                return Ok(None);
            };
            let start: Vec<u8> = row.try_get(0).map_err(|_| failure("run start"))?;
            let end: Vec<u8> = row.try_get(1).map_err(|_| failure("run end"))?;
            let start = u64::from_be_bytes(start.try_into().map_err(|_| failure("run framing"))?);
            let end = u64::from_be_bytes(end.try_into().map_err(|_| failure("run framing"))?);
            self.active_run = Some((start, start, end));
        }
        let (start, offset, end) = self.active_run.unwrap();
        let length = (end - offset).min(self.frame_bytes) as usize;
        let file = self.file.clone();
        let raw = tokio::task::spawn_blocking(move || {
            let mut raw = vec![0; length];
            file.read_exact_at(&mut raw, offset)
                .map_err(|_| failure("payload read"))?;
            Ok::<_, PackedWireError>(raw)
        })
        .await
        .map_err(|_| failure("payload task"))??;
        self.validate_unchanged()?;
        if offset + length as u64 == end {
            self.after_run = Some(start.to_be_bytes().to_vec());
            self.active_run = None;
        } else {
            self.active_run = Some((start, offset + length as u64, end));
        }
        Ok(Some((
            offset,
            PackedFrameInput {
                raw,
                size_class: self.size_class,
                codec: 0,
                first_file_slot: 0,
                last_file_slot: 0,
            },
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom, Write};
    #[tokio::test]
    async fn external_source_pages_many_sparse_runs_without_losing_holes_or_eof() {
        let source = tempfile::tempdir().unwrap();
        let spool = tempfile::tempdir().unwrap();
        let path = source.path().join("sparse");
        let mut file = File::create(&path).unwrap();
        for i in 0..300u64 {
            file.seek(SeekFrom::Start(i * 8192)).unwrap();
            file.write_all(&[i as u8; 4096]).unwrap();
        }
        file.set_len(72 * 1024 * 1024).unwrap();
        // Stabilize the intended immutable fixture before recording st_blocks
        // and stat fences; delayed allocation is itself a source change.
        file.sync_all().unwrap();

        let mut captured = CapturedSourceLayout::capture(
            &path,
            spool.path(),
            2,
            AccessProfile::RandomSmallFile,
            SizeClassTable::default(),
        )
        .await
        .unwrap();
        assert_eq!(captured.entry().size, 72 * 1024 * 1024);
        assert_eq!(captured.frame_count(), 300);
        assert_eq!(captured.data_bytes(), 300 * 4096);
        let mut count = 0;
        while let Some((offset, frame)) = captured.next_frame().await.unwrap() {
            assert_eq!(offset, count * 8192);
            assert_eq!(frame.raw, vec![count as u8; 4096]);
            count += 1;
        }
        assert_eq!(count, 300);
        drop(captured);
        assert_eq!(std::fs::read_dir(spool.path()).unwrap().count(), 0);
    }
    #[tokio::test]
    async fn external_source_all_hole_is_empty_and_mutation_replacement_fail_closed() {
        let source = tempfile::tempdir().unwrap();
        let spool = tempfile::tempdir().unwrap();
        let path = source.path().join("hole");
        File::create(&path)
            .unwrap()
            .set_len(72 * 1024 * 1024)
            .unwrap();
        let mut captured = CapturedSourceLayout::capture(
            &path,
            spool.path(),
            2,
            AccessProfile::RandomSmallFile,
            SizeClassTable::default(),
        )
        .await
        .unwrap();
        assert_eq!(captured.data_bytes(), 0);
        assert_eq!(captured.frame_count(), 0);
        assert!(captured.next_frame().await.unwrap().is_none());
        std::fs::rename(&path, source.path().join("old")).unwrap();
        File::create(&path)
            .unwrap()
            .set_len(72 * 1024 * 1024)
            .unwrap();
        assert!(captured.next_frame().await.is_err());
        drop(captured);
        assert_eq!(std::fs::read_dir(spool.path()).unwrap().count(), 0);
    }
}
