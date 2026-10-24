//! Upper-first current-state resolution over one authenticated immutable lower.
//!
//! Callers supply upper rows from their transaction/read view and retain the
//! namespace lease/lower ownership. This module neither reads an old layer
//! catalog nor turns a failed lower read into absence.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;

use super::EntityError;
use super::model::{
    ChunkMap, ChunkSource, ChunkSpan, EntityDentry, EntityInode, EntityKind, EntityValue,
    LowerBinding, MAX_XATTR_BYTES, UpperInode, validate_name, validate_xattr_name,
};

/// Bound for a lower page and for the complete-directory compatibility API.
pub const MAX_DIRECTORY_ENTRIES: usize = 4096;

#[async_trait]
pub trait ImmutableLower: Send + Sync {
    /// Stable authenticated manifest identity, established by attach. A real
    /// packed adapter must retain its verified manifest and bound lower reads;
    /// implementing this trait alone is not mount/write authority.
    fn binding(&self) -> &LowerBinding;

    async fn inode(&self, ino: i64) -> Result<Option<EntityInode>, EntityError>;
    async fn dentry(&self, parent: i64, name: &[u8]) -> Result<Option<EntityDentry>, EntityError>;
    async fn xattr(&self, ino: i64, name: &[u8]) -> Result<Option<Vec<u8>>, EntityError>;
    /// A complete chunk map containing only LowerRange/Hole, with zero beyond
    /// immutable EOF. None means a known sparse chunk of an existing file;
    /// missing objects, authentication and transport failures return errors.
    async fn chunk(
        &self,
        ino: i64,
        chunk_index: u64,
        chunk_size: u64,
    ) -> Result<Option<ChunkMap>, EntityError>;
    /// Current names only, no whiteouts or duplicates; bounded before materialization.
    async fn readdir(&self, ino: i64) -> Result<Vec<(Vec<u8>, EntityDentry)>, EntityError>;

    /// A bounded page in stable immutable order; a short page means EOF.
    /// Offset counts physical lower names, before upper suppression. Adapters
    /// for large directories override this compatibility implementation to
    /// avoid complete materialization.
    async fn readdir_page(
        &self,
        ino: i64,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, EntityDentry)>, EntityError> {
        validate_directory_page(limit)?;
        let offset = usize::try_from(offset).map_err(|_| EntityError::Overflow)?;
        let entries = self.readdir(ino).await?;
        if entries.len() > MAX_DIRECTORY_ENTRIES {
            return Err(EntityError::Corrupt(
                "immutable directory response exceeds its bound".into(),
            ));
        }
        Ok(entries.into_iter().skip(offset).take(limit).collect())
    }
}

pub(super) fn validate_directory_page(limit: usize) -> Result<(), EntityError> {
    if limit == 0 || limit > MAX_DIRECTORY_ENTRIES {
        return Err(EntityError::Invalid("invalid directory page bound".into()));
    }
    Ok(())
}

pub(super) async fn directory_page<L: ImmutableLower + ?Sized>(
    lower: &L,
    binding: &LowerBinding,
    ino: i64,
    offset: u64,
    limit: usize,
) -> Result<Vec<(Vec<u8>, EntityDentry)>, EntityError> {
    check_binding(lower, binding)?;
    check_ino(ino)?;
    validate_directory_page(limit)?;
    if ino > binding.max_ino {
        return Ok(Vec::new());
    }
    let entries = lower.readdir_page(ino, offset, limit).await?;
    check_binding(lower, binding)?;
    if entries.len() > limit {
        return Err(EntityError::Corrupt(
            "oversized immutable directory page".into(),
        ));
    }
    let mut names = BTreeSet::new();
    for (name, row) in &entries {
        validate_name(name)?;
        check_lower_dentry(row, binding)?;
        if !names.insert(name) {
            return Err(EntityError::Corrupt(
                "duplicate immutable directory name".into(),
            ));
        }
    }
    Ok(entries)
}

/// Implementations return exactly the requested authenticated bytes, or an
/// error. The caller has already clipped the locator to immutable inode EOF.
#[async_trait]
pub trait ImmutableLowerData: ImmutableLower {
    async fn read_range(
        &self,
        ino: i64,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, EntityError>;
}

fn check_binding<L: ImmutableLower + ?Sized>(
    lower: &L,
    expected: &LowerBinding,
) -> Result<(), EntityError> {
    expected.validate()?;
    lower.binding().validate()?;
    if lower.binding() != expected {
        return Err(EntityError::Fenced);
    }
    Ok(())
}

fn check_ino(ino: i64) -> Result<(), EntityError> {
    if ino <= 0 {
        return Err(EntityError::Invalid("invalid inode identity".into()));
    }
    Ok(())
}

fn check_inode(row: &EntityInode, ino: i64, immutable: bool) -> Result<(), EntityError> {
    row.validate()?;
    if row.ino != ino
        || (immutable && row.kind == EntityKind::File && row.lower_visible_size != row.size)
    {
        return Err(EntityError::Corrupt(
            "inode row/key or immutable boundary mismatch".into(),
        ));
    }
    Ok(())
}

pub async fn resolve_inode<L: ImmutableLower + ?Sized>(
    lower: &L,
    binding: &LowerBinding,
    ino: i64,
    upper: Option<&UpperInode>,
) -> Result<Option<EntityInode>, EntityError> {
    check_binding(lower, binding)?;
    check_ino(ino)?;
    match upper {
        Some(UpperInode::Deleted) => Ok(None),
        Some(UpperInode::Present(row)) => {
            check_inode(row, ino, false)?;
            Ok(Some(row.clone()))
        }
        None if ino > binding.max_ino => Ok(None),
        None => {
            let row = lower.inode(ino).await?;
            check_binding(lower, binding)?;
            if let Some(row) = &row {
                check_inode(row, ino, true)?;
            }
            Ok(row)
        }
    }
}

fn check_lower_dentry(row: &EntityDentry, binding: &LowerBinding) -> Result<(), EntityError> {
    row.validate()?;
    match row {
        EntityDentry::Present { ino, .. } if *ino <= binding.max_ino => Ok(()),
        _ => Err(EntityError::Corrupt(
            "invalid immutable lower dentry".into(),
        )),
    }
}

pub async fn resolve_dentry<L: ImmutableLower + ?Sized>(
    lower: &L,
    binding: &LowerBinding,
    parent: i64,
    name: &[u8],
    upper: Option<&EntityDentry>,
) -> Result<Option<EntityDentry>, EntityError> {
    check_binding(lower, binding)?;
    check_ino(parent)?;
    validate_name(name)?;
    match upper {
        Some(EntityDentry::Whiteout) => Ok(None),
        Some(row) => {
            row.validate()?;
            Ok(Some(row.clone()))
        }
        None if parent > binding.max_ino => Ok(None),
        None => {
            let row = lower.dentry(parent, name).await?;
            check_binding(lower, binding)?;
            if let Some(row) = &row {
                check_lower_dentry(row, binding)?;
            }
            Ok(row)
        }
    }
}

pub async fn resolve_xattr<L: ImmutableLower + ?Sized>(
    lower: &L,
    binding: &LowerBinding,
    ino: i64,
    name: &[u8],
    upper: Option<&EntityValue>,
) -> Result<Option<Vec<u8>>, EntityError> {
    check_binding(lower, binding)?;
    check_ino(ino)?;
    validate_xattr_name(name)?;
    match upper {
        Some(EntityValue::Whiteout) => Ok(None),
        Some(row @ EntityValue::Present(value)) => {
            row.validate()?;
            Ok(Some(value.clone()))
        }
        None if ino > binding.max_ino => Ok(None),
        None => {
            let value = lower.xattr(ino, name).await?;
            check_binding(lower, binding)?;
            if value
                .as_ref()
                .is_some_and(|value| value.len() > MAX_XATTR_BYTES)
            {
                return Err(EntityError::Corrupt(
                    "immutable xattr exceeds its byte bound".into(),
                ));
            }
            Ok(value)
        }
    }
}

/// Resolve a complete map. Every inherited span belongs to this same inode
/// and logical position; the durable lower visibility boundary clips its tail.
pub async fn resolve_chunk<L: ImmutableLower + ?Sized>(
    lower: &L,
    binding: &LowerBinding,
    inode: &EntityInode,
    chunk_index: u64,
    chunk_size: u64,
    upper: Option<&ChunkMap>,
) -> Result<ChunkMap, EntityError> {
    check_binding(lower, binding)?;
    inode.validate()?;
    if inode.kind != EntityKind::File {
        return Err(if inode.kind == EntityKind::Directory {
            EntityError::IsDirectory
        } else {
            EntityError::Unsupported("entity chunk map for non-regular inode")
        });
    }
    if chunk_size == 0 {
        return Err(EntityError::Invalid("zero chunk size".into()));
    }
    let chunk_start = chunk_index
        .checked_mul(chunk_size)
        .ok_or(EntityError::Overflow)?;
    chunk_start
        .checked_add(chunk_size)
        .ok_or(EntityError::Overflow)?;
    let inherited = inode
        .lower_visible_size
        .saturating_sub(chunk_start)
        .min(chunk_size);
    let mut original = None;
    let map = match upper {
        Some(map) => map.clone(),
        None if inherited == 0 => return ChunkMap::holes(chunk_size),
        None => {
            let row = resolve_inode(lower, binding, inode.ino, None)
                .await?
                .ok_or_else(|| EntityError::Corrupt("inherited chunk has no lower inode".into()))?;
            if row.kind != EntityKind::File || inode.lower_visible_size > row.size {
                return Err(EntityError::Corrupt(
                    "invalid inherited lower visibility boundary".into(),
                ));
            }
            original = Some(row);
            let map = lower
                .chunk(inode.ino, chunk_index, chunk_size)
                .await?
                .unwrap_or(ChunkMap::holes(chunk_size)?);
            check_binding(lower, binding)?;
            map
        }
    };
    map.validate_binding(binding)?;
    if map.chunk_size != chunk_size {
        return Err(EntityError::Corrupt("chunk size mismatch".into()));
    }
    let mut spans = Vec::with_capacity(map.spans.len());
    for span in map.spans {
        match &span.source {
            ChunkSource::Data { .. } if upper.is_none() => {
                return Err(EntityError::Corrupt(
                    "upper slice in immutable lower map".into(),
                ));
            }
            ChunkSource::LowerRange {
                ino, file_offset, ..
            } => {
                if *ino != inode.ino || *file_offset != chunk_start + span.offset {
                    return Err(EntityError::Corrupt("lower-range identity mismatch".into()));
                }
                if original.is_none() {
                    original = Some(
                        resolve_inode(lower, binding, *ino, None)
                            .await?
                            .ok_or_else(|| {
                                EntityError::Corrupt(
                                    "lower range references a missing inode".into(),
                                )
                            })?,
                    );
                }
                let original = original.as_ref().expect("resolved immutable inode");
                if original.kind != EntityKind::File {
                    return Err(EntityError::Corrupt(
                        "lower range references a non-file inode".into(),
                    ));
                }
                if file_offset
                    .checked_add(span.length)
                    .ok_or(EntityError::Overflow)?
                    > original.size
                {
                    return Err(EntityError::Corrupt(
                        "lower range exceeds immutable EOF".into(),
                    ));
                }
                // Validate the original locator even when truncation masks it.
                let retained = inherited.saturating_sub(span.offset).min(span.length);
                if retained != 0 {
                    spans.push(ChunkSpan {
                        length: retained,
                        ..span.clone()
                    });
                }
                if retained < span.length {
                    spans.push(ChunkSpan {
                        offset: span.offset + retained,
                        length: span.length - retained,
                        source: ChunkSource::Hole,
                    });
                }
            }
            _ => spans.push(span),
        }
    }
    ChunkMap::new(chunk_size, spans)
}
pub async fn merge_directory<L: ImmutableLower + ?Sized>(
    lower: &L,
    binding: &LowerBinding,
    ino: i64,
    upper: &[(Vec<u8>, EntityDentry)],
) -> Result<Vec<(Vec<u8>, EntityDentry)>, EntityError> {
    check_binding(lower, binding)?;
    check_ino(ino)?;
    let mut upper_names = BTreeSet::new();
    for (name, row) in upper {
        validate_name(name)?;
        row.validate()?;
        if !upper_names.insert(name) {
            return Err(EntityError::Corrupt(
                "duplicate upper directory name".into(),
            ));
        }
    }
    let mut merged = BTreeMap::new();
    let mut offset = 0;
    loop {
        let entries = directory_page(lower, binding, ino, offset, MAX_DIRECTORY_ENTRIES).await?;
        let count = entries.len();
        for (name, row) in entries {
            if merged.insert(name, row).is_some() {
                return Err(EntityError::Corrupt(
                    "duplicate immutable directory name".into(),
                ));
            }
        }
        if count < MAX_DIRECTORY_ENTRIES {
            break;
        }
        offset = offset
            .checked_add(count as u64)
            .ok_or(EntityError::Overflow)?;
    }
    for (name, row) in upper {
        match row {
            EntityDentry::Whiteout => {
                merged.remove(name);
            }
            EntityDentry::Present { .. } => {
                merged.insert(name.clone(), row.clone());
            }
        }
    }
    Ok(merged.into_iter().collect())
}

/// Deterministic immutable fixture for namespace/ownership contracts. No
/// production mount can obtain authenticated packed authority from this type.
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    pub(crate) struct MemoryLower {
        pub binding: LowerBinding,
        pub inodes: BTreeMap<i64, EntityInode>,
        pub dentries: BTreeMap<(i64, Vec<u8>), EntityDentry>,
        pub xattrs: BTreeMap<(i64, Vec<u8>), Vec<u8>>,
        pub chunks: BTreeMap<(i64, u64), ChunkMap>,
    }

    pub(crate) fn inode_record(ino: i64, kind: EntityKind, size: u64) -> EntityInode {
        EntityInode {
            ino,
            kind,
            size,
            mode: if kind == EntityKind::Directory {
                0o755
            } else {
                0o644
            },
            uid: 0,
            gid: 0,
            rdev: 0,
            nlink: if kind == EntityKind::Directory { 2 } else { 1 },
            atime_ns: 1,
            mtime_ns: 1,
            ctime_ns: 1,
            symlink_target: (kind == EntityKind::Symlink).then(|| b"target".to_vec()),
            parent_hint: Some(1),
            lower_visible_size: if kind == EntityKind::File { size } else { 0 },
            data_version: 0,
        }
    }

    impl MemoryLower {
        pub(crate) fn empty() -> Self {
            Self {
                binding: LowerBinding {
                    manifest_key: b"test-only/entity-manifest".to_vec(),
                    manifest_digest: [1; 32],
                    root_ino: 1,
                    max_ino: 1,
                },
                inodes: BTreeMap::from([(1, inode_record(1, EntityKind::Directory, 0))]),
                dentries: BTreeMap::new(),
                xattrs: BTreeMap::new(),
                chunks: BTreeMap::new(),
            }
        }

        pub(crate) fn add_file(&mut self, parent: i64, name: &[u8], ino: i64, size: u64) {
            assert!(parent > 0 && ino > 0);
            validate_name(name).unwrap();
            let mut file = inode_record(ino, EntityKind::File, size);
            file.parent_hint = Some(parent);
            self.inodes.insert(ino, file);
            self.dentries.insert(
                (parent, name.to_vec()),
                EntityDentry::Present {
                    ino,
                    kind: EntityKind::File,
                },
            );
            self.binding.max_ino = self.binding.max_ino.max(ino);
        }
    }

    #[async_trait]
    impl ImmutableLower for MemoryLower {
        fn binding(&self) -> &LowerBinding {
            &self.binding
        }
        async fn inode(&self, ino: i64) -> Result<Option<EntityInode>, EntityError> {
            Ok(self.inodes.get(&ino).cloned())
        }
        async fn dentry(
            &self,
            parent: i64,
            name: &[u8],
        ) -> Result<Option<EntityDentry>, EntityError> {
            Ok(self.dentries.get(&(parent, name.to_vec())).cloned())
        }
        async fn xattr(&self, ino: i64, name: &[u8]) -> Result<Option<Vec<u8>>, EntityError> {
            Ok(self.xattrs.get(&(ino, name.to_vec())).cloned())
        }
        async fn chunk(
            &self,
            ino: i64,
            chunk_index: u64,
            chunk_size: u64,
        ) -> Result<Option<ChunkMap>, EntityError> {
            if let Some(map) = self.chunks.get(&(ino, chunk_index)) {
                return Ok(Some(map.clone()));
            }
            let Some(inode) = self.inodes.get(&ino) else {
                return Ok(None);
            };
            let start = chunk_index
                .checked_mul(chunk_size)
                .ok_or(EntityError::Overflow)?;
            ChunkMap::inherited(chunk_size, &self.binding, ino, start, inode.size).map(Some)
        }
        async fn readdir(&self, ino: i64) -> Result<Vec<(Vec<u8>, EntityDentry)>, EntityError> {
            let rows: Vec<_> = self
                .dentries
                .iter()
                .filter(|((parent, _), _)| *parent == ino)
                .take(MAX_DIRECTORY_ENTRIES + 1)
                .map(|((_, name), row)| (name.clone(), row.clone()))
                .collect();
            if rows.len() > MAX_DIRECTORY_ENTRIES {
                return Err(EntityError::Unsupported(
                    "test lower directory requires paging",
                ));
            }
            Ok(rows)
        }

        async fn readdir_page(
            &self,
            ino: i64,
            offset: u64,
            limit: usize,
        ) -> Result<Vec<(Vec<u8>, EntityDentry)>, EntityError> {
            validate_directory_page(limit)?;
            let offset = usize::try_from(offset).map_err(|_| EntityError::Overflow)?;
            Ok(self
                .dentries
                .range((ino, Vec::new())..)
                .take_while(|((parent, _), _)| *parent == ino)
                .skip(offset)
                .take(limit)
                .map(|((_, name), row)| (name.clone(), row.clone()))
                .collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::workspace_overlay::entity::model::ChunkSpan;

    struct Lower {
        binding: LowerBinding,
        calls: AtomicUsize,
        file: EntityInode,
        entries: Vec<(Vec<u8>, EntityDentry)>,
        chunk: Option<ChunkMap>,
    }

    fn inode(ino: i64, size: u64) -> EntityInode {
        EntityInode {
            ino,
            kind: EntityKind::File,
            size,
            mode: 0o644,
            uid: 1,
            gid: 1,
            rdev: 0,
            nlink: 2,
            atime_ns: 1,
            mtime_ns: 1,
            ctime_ns: 1,
            symlink_target: None,
            parent_hint: Some(1),
            lower_visible_size: size,
            data_version: 0,
        }
    }

    impl Lower {
        fn new() -> Self {
            let binding = LowerBinding {
                manifest_key: b"manifest".to_vec(),
                manifest_digest: [1; 32],
                root_ino: 1,
                max_ino: 2,
            };
            let chunk = Some(ChunkMap::inherited(16, &binding, 2, 0, 10).unwrap());
            Self {
                binding,
                calls: AtomicUsize::new(0),
                file: inode(2, 10),
                chunk,
                entries: vec![
                    (
                        b"a".to_vec(),
                        EntityDentry::Present {
                            ino: 2,
                            kind: EntityKind::File,
                        },
                    ),
                    (
                        b"b".to_vec(),
                        EntityDentry::Present {
                            ino: 2,
                            kind: EntityKind::File,
                        },
                    ),
                ],
            }
        }
    }

    #[async_trait]
    impl ImmutableLower for Lower {
        fn binding(&self) -> &LowerBinding {
            &self.binding
        }
        async fn inode(&self, ino: i64) -> Result<Option<EntityInode>, EntityError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok((ino == 2).then(|| self.file.clone()))
        }
        async fn dentry(
            &self,
            _parent: i64,
            name: &[u8],
        ) -> Result<Option<EntityDentry>, EntityError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self
                .entries
                .iter()
                .find(|(candidate, _)| candidate == name)
                .map(|(_, row)| row.clone()))
        }
        async fn xattr(&self, _ino: i64, _name: &[u8]) -> Result<Option<Vec<u8>>, EntityError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some(b"lower-value".to_vec()))
        }
        async fn chunk(
            &self,
            _ino: i64,
            _index: u64,
            _size: u64,
        ) -> Result<Option<ChunkMap>, EntityError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.chunk.clone())
        }
        async fn readdir(&self, _ino: i64) -> Result<Vec<(Vec<u8>, EntityDentry)>, EntityError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.entries.clone())
        }
    }

    #[tokio::test]
    async fn whiteouts_suppress_fallback_while_absence_inherits() {
        let lower = Lower::new();
        let binding = lower.binding();
        assert_eq!(
            resolve_inode(&lower, binding, 2, Some(&UpperInode::Deleted))
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            resolve_dentry(&lower, binding, 1, b"a", Some(&EntityDentry::Whiteout))
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            resolve_xattr(&lower, binding, 2, b"user.x", Some(&EntityValue::Whiteout))
                .await
                .unwrap(),
            None
        );
        assert_eq!(lower.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            resolve_inode(&lower, binding, 2, None).await.unwrap(),
            Some(lower.file.clone())
        );
        assert!(
            resolve_dentry(&lower, binding, 1, b"a", None)
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(
            resolve_xattr(&lower, binding, 2, b"user.x", None)
                .await
                .unwrap(),
            Some(b"lower-value".to_vec())
        );
        assert_eq!(
            resolve_xattr(
                &lower,
                binding,
                2,
                b"user.x",
                Some(&EntityValue::Present(Vec::new()))
            )
            .await
            .unwrap(),
            Some(Vec::new())
        );
    }

    #[tokio::test]
    async fn copy_up_changes_one_inode_shared_by_lower_aliases() {
        let lower = Lower::new();
        let mut copied = lower.file.clone();
        copied.mode = 0o600;
        let upper = UpperInode::Present(copied.clone());
        for name in [b"a".as_slice(), b"b"] {
            let EntityDentry::Present { ino, .. } =
                resolve_dentry(&lower, lower.binding(), 1, name, None)
                    .await
                    .unwrap()
                    .unwrap()
            else {
                panic!("expected a live name")
            };
            assert_eq!(
                resolve_inode(&lower, lower.binding(), ino, Some(&upper))
                    .await
                    .unwrap(),
                Some(copied.clone())
            );
        }
        assert_eq!(lower.file.mode, 0o644);
    }

    #[tokio::test]
    async fn absent_chunk_respects_shrunk_lower_boundary_after_extension() {
        let lower = Lower::new();
        let mut copied = lower.file.clone();
        copied.lower_visible_size = 4;
        copied.size = 16;
        let map = resolve_chunk(&lower, lower.binding(), &copied, 0, 16, None)
            .await
            .unwrap();
        assert_eq!(
            map.spans,
            vec![
                ChunkSpan {
                    offset: 0,
                    length: 4,
                    source: ChunkSource::LowerRange {
                        binding_digest: [1; 32],
                        ino: 2,
                        file_offset: 0
                    }
                },
                ChunkSpan {
                    offset: 4,
                    length: 12,
                    source: ChunkSource::Hole
                },
            ]
        );
        let before = lower.calls.load(Ordering::SeqCst);
        assert_eq!(
            resolve_chunk(&lower, lower.binding(), &copied, 1, 16, None)
                .await
                .unwrap(),
            ChunkMap::holes(16).unwrap()
        );
        let mut new_file = inode(3, 16);
        new_file.lower_visible_size = 0;
        assert_eq!(
            resolve_chunk(&lower, lower.binding(), &new_file, 0, 16, None)
                .await
                .unwrap(),
            ChunkMap::holes(16).unwrap()
        );
        assert_eq!(lower.calls.load(Ordering::SeqCst), before);
    }

    #[tokio::test]
    async fn complete_upper_map_avoids_lower_reads_and_corruption_fails_closed() {
        let lower = Lower::new();
        let upper = ChunkMap::holes(16)
            .unwrap()
            .replace(
                2,
                4,
                ChunkSource::Data {
                    slice_id: 5,
                    slice_offset: 0,
                },
            )
            .unwrap();
        assert_eq!(
            resolve_chunk(&lower, lower.binding(), &lower.file, 0, 16, Some(&upper))
                .await
                .unwrap(),
            upper
        );
        assert_eq!(lower.calls.load(Ordering::SeqCst), 0);
        let mut broken = upper.clone();
        broken.spans.pop();
        assert!(
            resolve_chunk(&lower, lower.binding(), &lower.file, 0, 16, Some(&broken))
                .await
                .is_err()
        );
        assert_eq!(lower.calls.load(Ordering::SeqCst), 0);
        let invalid_lower = ChunkMap::inherited(16, lower.binding(), 2, 16, 32).unwrap();
        assert!(
            resolve_chunk(
                &lower,
                lower.binding(),
                &lower.file,
                0,
                16,
                Some(&invalid_lower)
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn stored_map_cannot_resurrect_lower_tail_or_move_lower_bytes() {
        let lower = Lower::new();
        let mut copied = lower.file.clone();
        copied.lower_visible_size = 4;
        copied.size = 16;
        let old = ChunkMap::inherited(16, lower.binding(), 2, 0, lower.file.size)
            .unwrap()
            .replace(
                8,
                2,
                ChunkSource::Data {
                    slice_id: 7,
                    slice_offset: 0,
                },
            )
            .unwrap();
        let resolved = resolve_chunk(&lower, lower.binding(), &copied, 0, 16, Some(&old))
            .await
            .unwrap();
        let expected = ChunkMap::inherited(16, lower.binding(), 2, 0, 4)
            .unwrap()
            .replace(
                8,
                2,
                ChunkSource::Data {
                    slice_id: 7,
                    slice_offset: 0,
                },
            )
            .unwrap();
        assert_eq!(resolved, expected);

        let moved = ChunkMap::holes(16)
            .unwrap()
            .replace(
                1,
                2,
                ChunkSource::LowerRange {
                    binding_digest: lower.binding().manifest_digest,
                    ino: 2,
                    file_offset: 0,
                },
            )
            .unwrap();
        assert!(matches!(
            resolve_chunk(&lower, lower.binding(), &copied, 0, 16, Some(&moved)).await,
            Err(EntityError::Corrupt(_))
        ));
        let mut other_inode = copied;
        other_inode.ino = 3;
        assert!(matches!(
            resolve_chunk(&lower, lower.binding(), &other_inode, 0, 16, Some(&old)).await,
            Err(EntityError::Corrupt(_))
        ));
    }

    #[tokio::test]
    async fn large_directory_merge_crosses_old_merged_bound() {
        let lower = Lower::new();
        for count in [4095, 4097] {
            let upper: Vec<_> = (0..count)
                .map(|index| {
                    (
                        format!("upper-{index:04}").into_bytes(),
                        EntityDentry::Present {
                            ino: 3,
                            kind: EntityKind::File,
                        },
                    )
                })
                .collect();
            let entries = merge_directory(&lower, lower.binding(), 1, &upper)
                .await
                .unwrap();
            assert_eq!(entries.len(), count + 2);
        }
    }

    #[tokio::test]
    async fn directory_merge_masks_names_and_rejects_ambiguous_rows() {
        let mut lower = Lower::new();
        let replacement = EntityDentry::Present {
            ino: 3,
            kind: EntityKind::File,
        };
        let upper = vec![
            (b"a".to_vec(), EntityDentry::Whiteout),
            (b"c".to_vec(), replacement.clone()),
        ];
        assert_eq!(
            merge_directory(&lower, lower.binding(), 1, &upper)
                .await
                .unwrap(),
            vec![
                (
                    b"b".to_vec(),
                    EntityDentry::Present {
                        ino: 2,
                        kind: EntityKind::File
                    }
                ),
                (b"c".to_vec(), replacement),
            ]
        );
        lower.entries.push(lower.entries[0].clone());
        assert!(
            merge_directory(&lower, lower.binding(), 1, &upper)
                .await
                .is_err()
        );
        assert!(
            merge_directory(
                &lower,
                lower.binding(),
                1,
                &[
                    (b"x".to_vec(), EntityDentry::Whiteout),
                    (b"x".to_vec(), EntityDentry::Whiteout)
                ]
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn binding_or_row_identity_mismatch_is_an_error_not_fallback() {
        let mut lower = Lower::new();
        let mut expected = lower.binding.clone();
        expected.manifest_digest = [9; 32];
        assert_eq!(
            resolve_inode(&lower, &expected, 2, None).await.unwrap_err(),
            EntityError::Fenced
        );
        assert_eq!(lower.calls.load(Ordering::SeqCst), 0);
        lower.file.ino = 3;
        assert!(
            resolve_inode(&lower, lower.binding(), 2, None)
                .await
                .is_err()
        );
        let upper = UpperInode::Present(lower.file.clone());
        assert!(
            resolve_inode(&lower, lower.binding(), 2, Some(&upper))
                .await
                .is_err()
        );
    }
}
