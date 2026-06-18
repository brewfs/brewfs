/// Key for a clean (committed, immutable) block in the read cache.
///
/// Once a slice is committed to the object store, its blocks are immutable.
/// This makes `(slice_id, block_index)` a perfect cache key — no invalidation
/// needed for overwrites (new writes create new slices with new IDs).
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct CleanBlockKey {
    pub slice_id: u64,
    pub block_index: u32,
}

impl CleanBlockKey {
    pub fn new(slice_id: u64, block_index: u32) -> Self {
        Self {
            slice_id,
            block_index,
        }
    }

    pub fn to_cache_path(self) -> String {
        format!("chunks/{}/{}", self.slice_id, self.block_index)
    }
}

/// Key for a dirty (uncommitted) slice in the write-back cache.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DirtySliceKey {
    pub ino: i64,
    pub chunk_id: u64,
    pub local_seq: u64,
    pub epoch: u64,
}

const DIRTY_SLICE_BUCKET_SIZE: u64 = 1024;

impl DirtySliceKey {
    fn bucket(self) -> u64 {
        self.local_seq / DIRTY_SLICE_BUCKET_SIZE
    }

    fn file_stem(self) -> String {
        format!(
            "{}_{}_{}_{}",
            self.ino, self.chunk_id, self.local_seq, self.epoch
        )
    }

    pub fn dir_path(&self, root: &std::path::Path) -> std::path::PathBuf {
        root.join("dirty").join(self.bucket().to_string())
    }

    pub fn slice_path(&self, root: &std::path::Path) -> std::path::PathBuf {
        self.dir_path(root)
            .join(format!("{}.slice", self.file_stem()))
    }

    pub fn meta_path(&self, root: &std::path::Path) -> std::path::PathBuf {
        self.dir_path(root)
            .join(format!("{}.meta", self.file_stem()))
    }

    pub(crate) fn legacy_dir_path(&self, root: &std::path::Path) -> std::path::PathBuf {
        root.join("dirty")
            .join(self.ino.to_string())
            .join(self.chunk_id.to_string())
    }

    pub(crate) fn legacy_slice_path(&self, root: &std::path::Path) -> std::path::PathBuf {
        self.legacy_dir_path(root)
            .join(format!("{}.slice", self.local_seq))
    }

    pub(crate) fn legacy_meta_path(&self, root: &std::path::Path) -> std::path::PathBuf {
        self.legacy_dir_path(root)
            .join(format!("{}.meta", self.local_seq))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn dirty_slice_paths_are_bucketed_by_local_sequence() {
        let root = Path::new("/cache-root");
        let key = DirtySliceKey {
            ino: 42,
            chunk_id: 42_000_000_000,
            local_seq: 2048,
            epoch: 0,
        };
        let sibling = DirtySliceKey {
            local_seq: 2049,
            ..key
        };

        assert_eq!(key.dir_path(root), root.join("dirty").join("2"));
        assert_eq!(sibling.dir_path(root), root.join("dirty").join("2"));
        assert_eq!(
            key.slice_path(root),
            root.join("dirty")
                .join("2")
                .join("42_42000000000_2048_0.slice")
        );
        assert_eq!(
            key.meta_path(root),
            root.join("dirty")
                .join("2")
                .join("42_42000000000_2048_0.meta")
        );
    }
}

/// State machine for a dirty slice in the write-back cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DirtySliceState {
    /// Slice is still being written to in memory.
    Open,
    /// Slice has been sealed (frozen) and persisted to local SSD.
    Sealed,
    /// Slice is being uploaded to the object store.
    Uploading,
    /// Upload complete, awaiting metadata commit.
    Uploaded,
    /// Metadata commit in progress.
    Committing,
    /// Fully committed — globally visible.
    Committed,
    /// Upload or commit failed, eligible for retry.
    Failed,
    /// Invalidated by truncate or overwrite — pending GC.
    Obsolete,
}
