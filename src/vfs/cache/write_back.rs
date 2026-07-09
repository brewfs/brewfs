use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use dashmap::DashSet;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use super::keys::{DirtySliceKey, DirtySliceState};

/// Record describing a dirty slice persisted to local SSD.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DirtySliceRecord {
    pub key: DirtySliceKey,
    pub ino: i64,
    pub chunk_id: u64,
    pub chunk_offset: u64,
    pub length: u64,
    pub remote_slice_id: Option<u64>,
    pub state: DirtySliceState,
    pub path: PathBuf,
    pub retry_count: u32,
    pub last_error: Option<String>,
}

/// Trait for a local SSD write-back cache.
///
/// Sealed (frozen) slices are persisted here before upload to the object store.
/// This provides crash recovery and decouples write latency from upload latency.
#[async_trait::async_trait]
pub trait WriteBackCache: Send + Sync {
    /// Persist a slice data batch to local SSD. Returns the local file path.
    async fn persist_slice_data(
        &self,
        key: DirtySliceKey,
        data: Vec<Bytes>,
        slice_offset: u64,
    ) -> anyhow::Result<PathBuf>;

    /// Publish the recoverable dirty slice record after all data batches are staged.
    async fn seal_slice_record(
        &self,
        key: DirtySliceKey,
        chunk_offset: u64,
        length: u64,
    ) -> anyhow::Result<()>;

    /// Open a persisted slice for reading (used by the uploader).
    async fn open_slice(
        &self,
        key: &DirtySliceKey,
    ) -> anyhow::Result<Box<dyn tokio::io::AsyncRead + Send + Unpin>>;

    /// Update the state of a dirty slice record.
    async fn mark_state(&self, key: &DirtySliceKey, state: DirtySliceState) -> anyhow::Result<()>;

    /// Recover all non-terminal dirty slice records after a crash.
    async fn recover(&self) -> anyhow::Result<Vec<DirtySliceRecord>>;

    /// Remove a committed or obsolete slice from local storage.
    async fn remove(&self, key: &DirtySliceKey) -> anyhow::Result<()>;
}

/// Filesystem-backed write-back cache implementation.
///
/// Directory layout:
///   {root}/dirty/{local_seq/1024}/{ino}_{chunk_id}_{local_seq}_{epoch}.slice  — open raw data
///   {root}/dirty/{local_seq/1024}/{ino}_{chunk_id}_{local_seq}_{epoch}.meta   — legacy JSON metadata
///   {root}/dirty/{local_seq/1024}/{ino}_{chunk_id}_{local_seq}_{epoch}_{off}_{len}.sealed
///     — sealed data and record for high-throughput unsynced writeback
pub struct FsWriteBackCache {
    root: PathBuf,
    seq: AtomicU64,
    sync_on_persist: bool,
    created_dirs: DashSet<PathBuf>,
}

impl FsWriteBackCache {
    pub fn new(root: PathBuf) -> Self {
        Self::new_with_sync(root, true)
    }

    pub fn new_with_sync(root: PathBuf, sync_on_persist: bool) -> Self {
        Self {
            root,
            seq: AtomicU64::new(0),
            sync_on_persist,
            created_dirs: DashSet::new(),
        }
    }

    pub fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    async fn write_meta_at(
        &self,
        meta_path: PathBuf,
        record: &DirtySliceRecord,
    ) -> anyhow::Result<()> {
        if let Some(parent) = meta_path.parent() {
            self.ensure_dir(parent).await?;
        }
        let tmp_path = meta_path.with_extension("meta.tmp");
        let json = serde_json::to_vec(record)?;
        fs::write(&tmp_path, &json).await?;
        fs::rename(&tmp_path, &meta_path).await?;
        Ok(())
    }

    async fn write_meta(
        &self,
        key: &DirtySliceKey,
        record: &DirtySliceRecord,
    ) -> anyhow::Result<()> {
        self.write_meta_at(key.meta_path(&self.root), record).await
    }

    async fn ensure_dir(&self, dir: &Path) -> anyhow::Result<()> {
        if self.created_dirs.contains(dir) {
            return Ok(());
        }
        fs::create_dir_all(dir).await?;
        self.created_dirs.insert(dir.to_path_buf());
        Ok(())
    }

    async fn read_meta(&self, meta_path: &Path) -> anyhow::Result<DirtySliceRecord> {
        let data = fs::read(meta_path).await?;
        let record: DirtySliceRecord = serde_json::from_slice(&data)?;
        Ok(record)
    }

    fn is_meta_path(path: &Path) -> bool {
        path.extension().and_then(|e| e.to_str()) == Some("meta")
    }

    fn is_sealed_path(path: &Path) -> bool {
        path.extension().and_then(|e| e.to_str()) == Some("sealed")
    }

    fn sealed_record_from_path(path: PathBuf) -> Option<DirtySliceRecord> {
        let file_name = path.file_name()?.to_str()?;
        let (key, chunk_offset, length) = DirtySliceKey::parse_sealed_file_name(file_name)?;
        Some(DirtySliceRecord {
            key,
            ino: key.ino,
            chunk_id: key.chunk_id,
            chunk_offset,
            length,
            remote_slice_id: None,
            state: DirtySliceState::Sealed,
            path,
            retry_count: 0,
            last_error: None,
        })
    }

    async fn push_recoverable_meta(
        &self,
        path: &Path,
        records: &mut Vec<DirtySliceRecord>,
    ) -> anyhow::Result<()> {
        match self.read_meta(path).await {
            Ok(record)
                if !matches!(
                    record.state,
                    DirtySliceState::Committed | DirtySliceState::Obsolete
                ) =>
            {
                records.push(record);
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(path = ?path, error = ?e, "corrupt meta");
            }
        }
        Ok(())
    }

    fn push_recoverable_sealed(path: PathBuf, records: &mut Vec<DirtySliceRecord>) {
        match Self::sealed_record_from_path(path.clone()) {
            Some(record) => records.push(record),
            None => {
                tracing::warn!(path = ?path, "invalid sealed writeback record name");
            }
        }
    }

    async fn collect_recoverable_records_in_dir(
        &self,
        dir: &Path,
        records: &mut Vec<DirtySliceRecord>,
    ) -> anyhow::Result<()> {
        let mut entries = fs::read_dir(dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if !entry.file_type().await?.is_file() {
                continue;
            }
            if Self::is_meta_path(&path) {
                self.push_recoverable_meta(&path, records).await?;
            } else if Self::is_sealed_path(&path) {
                Self::push_recoverable_sealed(path, records);
            }
        }
        Ok(())
    }

    async fn sealed_path_for_key(&self, key: &DirtySliceKey) -> anyhow::Result<Option<PathBuf>> {
        let dir = key.dir_path(&self.root);
        if !dir.exists() {
            return Ok(None);
        }

        let prefix = key.sealed_file_prefix();
        let mut entries = fs::read_dir(dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if !entry.file_type().await?.is_file() || !Self::is_sealed_path(&path) {
                continue;
            }
            let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if file_name.starts_with(&prefix) {
                return Ok(Some(path));
            }
        }
        Ok(None)
    }

    async fn remove_sealed_paths(&self, key: &DirtySliceKey) -> anyhow::Result<()> {
        let dir = key.dir_path(&self.root);
        if !dir.exists() {
            return Ok(());
        }

        let prefix = key.sealed_file_prefix();
        let mut entries = fs::read_dir(dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if !entry.file_type().await?.is_file() || !Self::is_sealed_path(&path) {
                continue;
            }
            let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if file_name.starts_with(&prefix) {
                let _ = fs::remove_file(path).await;
            }
        }
        Ok(())
    }

    async fn overlay_from_meta_dir(
        &self,
        dir: &Path,
        ino: i64,
        chunk_id: u64,
        chunk_offset: u64,
        buf: &mut [u8],
    ) -> anyhow::Result<()> {
        let mut entries = fs::read_dir(dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if !entry.file_type().await?.is_file() {
                continue;
            }

            let record = if Self::is_meta_path(&path) {
                match self.read_meta(&path).await {
                    Ok(record) if record.ino == ino && record.chunk_id == chunk_id => record,
                    Ok(_) | Err(_) => continue,
                }
            } else if Self::is_sealed_path(&path) {
                match Self::sealed_record_from_path(path) {
                    Some(record) if record.ino == ino && record.chunk_id == chunk_id => record,
                    Some(_) | None => continue,
                }
            } else {
                continue;
            };

            if !record.path.exists() {
                continue;
            }

            let slice_start = record.chunk_offset;
            let slice_end = slice_start + record.length;
            let buf_end = chunk_offset + buf.len() as u64;

            let overlap_start = chunk_offset.max(slice_start);
            let overlap_end = buf_end.min(slice_end);
            if overlap_start >= overlap_end {
                continue;
            }

            let file_offset = overlap_start - slice_start;
            let dst_start = (overlap_start - chunk_offset) as usize;
            let dst_end = (overlap_end - chunk_offset) as usize;
            let read_len = dst_end - dst_start;

            let mut file = fs::File::open(&record.path).await?;
            file.seek(std::io::SeekFrom::Start(file_offset)).await?;
            file.read_exact(&mut buf[dst_start..dst_start + read_len])
                .await?;
        }
        Ok(())
    }

    async fn read_covered_from_meta_dir(
        &self,
        dir: &Path,
        ino: i64,
        chunk_id: u64,
        chunk_offset: u64,
        buf: &mut [u8],
        covered: &mut Vec<(u64, u64)>,
    ) -> anyhow::Result<()> {
        let mut entries = fs::read_dir(dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if !entry.file_type().await?.is_file() {
                continue;
            }

            let record = if Self::is_meta_path(&path) {
                match self.read_meta(&path).await {
                    Ok(record) if record.ino == ino && record.chunk_id == chunk_id => record,
                    Ok(_) | Err(_) => continue,
                }
            } else if Self::is_sealed_path(&path) {
                match Self::sealed_record_from_path(path) {
                    Some(record) if record.ino == ino && record.chunk_id == chunk_id => record,
                    Some(_) | None => continue,
                }
            } else {
                continue;
            };

            if !record.path.exists() {
                continue;
            }

            let slice_start = record.chunk_offset;
            let slice_end = slice_start + record.length;
            let buf_end = chunk_offset + buf.len() as u64;

            let overlap_start = chunk_offset.max(slice_start);
            let overlap_end = buf_end.min(slice_end);
            if overlap_start >= overlap_end {
                continue;
            }

            let file_offset = overlap_start - slice_start;
            let dst_start = (overlap_start - chunk_offset) as usize;
            let dst_end = (overlap_end - chunk_offset) as usize;

            let mut file = fs::File::open(&record.path).await?;
            file.seek(std::io::SeekFrom::Start(file_offset)).await?;
            file.read_exact(&mut buf[dst_start..dst_end]).await?;
            covered.push((overlap_start - chunk_offset, overlap_end - chunk_offset));
        }
        Ok(())
    }

    async fn write_json_record(
        &self,
        key: DirtySliceKey,
        chunk_offset: u64,
        length: u64,
        path: PathBuf,
    ) -> anyhow::Result<()> {
        let record = DirtySliceRecord {
            key,
            ino: key.ino,
            chunk_id: key.chunk_id,
            chunk_offset,
            length,
            remote_slice_id: None,
            state: DirtySliceState::Sealed,
            path,
            retry_count: 0,
            last_error: None,
        };
        self.write_meta(&key, &record).await
    }

    async fn seal_by_rename(
        &self,
        key: DirtySliceKey,
        chunk_offset: u64,
        length: u64,
        slice_path: PathBuf,
    ) -> anyhow::Result<()> {
        let sealed_path = key.sealed_slice_path(&self.root, chunk_offset, length);
        fs::rename(&slice_path, &sealed_path).await?;
        Ok(())
    }

    #[cfg(test)]
    fn created_dir_count(&self) -> usize {
        self.created_dirs.len()
    }
}

#[async_trait::async_trait]
impl WriteBackCache for FsWriteBackCache {
    async fn persist_slice_data(
        &self,
        key: DirtySliceKey,
        data: Vec<Bytes>,
        slice_offset: u64,
    ) -> anyhow::Result<PathBuf> {
        let dir = key.dir_path(&self.root);
        self.ensure_dir(&dir).await?;

        let slice_path = key.slice_path(&self.root);

        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&slice_path)
            .await?;
        file.seek(std::io::SeekFrom::Start(slice_offset)).await?;
        for chunk in &data {
            file.write_all(chunk).await?;
        }
        file.flush().await?;
        if self.sync_on_persist {
            file.sync_all().await?;
        }
        drop(file);

        if self.sync_on_persist {
            // fsync parent directory to ensure a newly-created slice path is durable.
            let dir_fd = fs::File::open(&dir).await?;
            dir_fd.sync_all().await?;
        }

        Ok(slice_path)
    }

    async fn seal_slice_record(
        &self,
        key: DirtySliceKey,
        chunk_offset: u64,
        length: u64,
    ) -> anyhow::Result<()> {
        let slice_path = key.slice_path(&self.root);
        let file_len = fs::metadata(&slice_path).await?.len();
        anyhow::ensure!(
            file_len >= length,
            "writeback stage incomplete for {:?}: file length {} < sealed length {}",
            key,
            file_len,
            length
        );

        if self.sync_on_persist {
            self.write_json_record(key, chunk_offset, length, slice_path)
                .await?;
        } else {
            self.seal_by_rename(key, chunk_offset, length, slice_path)
                .await?;
        }
        Ok(())
    }

    async fn open_slice(
        &self,
        key: &DirtySliceKey,
    ) -> anyhow::Result<Box<dyn tokio::io::AsyncRead + Send + Unpin>> {
        let path = if key.slice_path(&self.root).exists() {
            key.slice_path(&self.root)
        } else {
            self.sealed_path_for_key(key)
                .await?
                .unwrap_or_else(|| key.slice_path(&self.root))
        };
        let file = fs::File::open(&path).await?;
        Ok(Box::new(file))
    }

    async fn mark_state(&self, key: &DirtySliceKey, state: DirtySliceState) -> anyhow::Result<()> {
        let mut updated_meta = false;
        for meta_path in [key.meta_path(&self.root), key.legacy_meta_path(&self.root)] {
            if meta_path.exists() {
                let mut record = self.read_meta(&meta_path).await?;
                record.state = state;
                self.write_meta_at(meta_path, &record).await?;
                updated_meta = true;
            }
        }
        if !updated_meta
            && matches!(
                state,
                DirtySliceState::Committed | DirtySliceState::Obsolete
            )
        {
            self.remove_sealed_paths(key).await?;
        }
        Ok(())
    }

    async fn recover(&self) -> anyhow::Result<Vec<DirtySliceRecord>> {
        let dirty_root = self.root.join("dirty");
        if !dirty_root.exists() {
            return Ok(Vec::new());
        }

        let mut records = Vec::new();
        let mut entries = fs::read_dir(&dirty_root).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let file_type = entry.file_type().await?;
            if file_type.is_file() {
                if Self::is_meta_path(&path) {
                    self.push_recoverable_meta(&path, &mut records).await?;
                } else if Self::is_sealed_path(&path) {
                    Self::push_recoverable_sealed(path, &mut records);
                }
                continue;
            }

            if !file_type.is_dir() {
                continue;
            }

            // New bucketed layout: dirty/<bucket>/*.meta or *.sealed.
            self.collect_recoverable_records_in_dir(&path, &mut records)
                .await?;

            // Legacy layout: dirty/<ino>/<chunk>/*.meta.
            let mut nested = fs::read_dir(&path).await?;
            while let Some(nested_entry) = nested.next_entry().await? {
                if nested_entry.file_type().await?.is_dir() {
                    self.collect_recoverable_records_in_dir(&nested_entry.path(), &mut records)
                        .await?;
                }
            }
        }
        Ok(records)
    }

    async fn remove(&self, key: &DirtySliceKey) -> anyhow::Result<()> {
        for path in [
            key.slice_path(&self.root),
            key.meta_path(&self.root),
            key.legacy_slice_path(&self.root),
            key.legacy_meta_path(&self.root),
        ] {
            let _ = fs::remove_file(&path).await;
        }
        self.remove_sealed_paths(key).await?;
        Ok(())
    }
}

impl FsWriteBackCache {
    /// Overlay dirty data from SSD onto a read buffer.
    /// Scans dirty slices for the given inode/chunk and copies any
    /// overlapping ranges into `buf`.  Used as a fallback when in-memory
    /// dirty data has been released (e.g., during crash recovery window).
    pub async fn overlay_dirty_range(
        &self,
        ino: i64,
        chunk_id: u64,
        chunk_offset: u64,
        buf: &mut [u8],
    ) -> anyhow::Result<()> {
        let chunk_dir = self
            .root
            .join("dirty")
            .join(ino.to_string())
            .join(chunk_id.to_string());

        let dirty_root = self.root.join("dirty");
        if !dirty_root.exists() {
            return Ok(());
        }

        let mut entries = fs::read_dir(&dirty_root).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                self.overlay_from_meta_dir(&entry.path(), ino, chunk_id, chunk_offset, buf)
                    .await?;
            }
        }

        if chunk_dir.exists() {
            self.overlay_from_meta_dir(&chunk_dir, ino, chunk_id, chunk_offset, buf)
                .await?;
        }

        Ok(())
    }

    pub async fn read_dirty_range_if_fully_covered(
        &self,
        ino: i64,
        chunk_id: u64,
        chunk_offset: u64,
        len: usize,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        if len == 0 {
            return Ok(Some(Vec::new()));
        }

        let dirty_root = self.root.join("dirty");
        if !dirty_root.exists() {
            return Ok(None);
        }

        let mut buf = vec![0; len];
        let mut covered = Vec::new();
        let mut entries = fs::read_dir(&dirty_root).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                self.read_covered_from_meta_dir(
                    &entry.path(),
                    ino,
                    chunk_id,
                    chunk_offset,
                    &mut buf,
                    &mut covered,
                )
                .await?;
            }
        }

        let chunk_dir = dirty_root.join(ino.to_string()).join(chunk_id.to_string());
        if chunk_dir.exists() {
            self.read_covered_from_meta_dir(
                &chunk_dir,
                ino,
                chunk_id,
                chunk_offset,
                &mut buf,
                &mut covered,
            )
            .await?;
        }

        covered.sort_unstable_by_key(|(start, _)| *start);
        let mut end = 0_u64;
        for (start, next_end) in covered {
            if start > end {
                break;
            }
            end = end.max(next_end);
            if end >= len as u64 {
                return Ok(Some(buf));
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unsynced_persist_writes_compact_sealed_record() {
        let temp = tempfile::tempdir().unwrap();
        let cache = FsWriteBackCache::new_with_sync(temp.path().to_path_buf(), false);
        let key = DirtySliceKey {
            ino: 7,
            chunk_id: 11,
            local_seq: 13,
            epoch: 0,
        };

        let path = cache
            .persist_slice_data(key, vec![Bytes::from_static(b"small")], 0)
            .await
            .unwrap();
        cache.seal_slice_record(key, 0, 5).await.unwrap();

        assert!(
            !path.exists(),
            "unsynced seal should rename the open slice into a compact record"
        );
        assert!(
            !key.meta_path(temp.path()).exists(),
            "unsynced seal should not write a JSON sidecar on the hot path"
        );
        let records = cache.recover().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, key);
        assert_eq!(records[0].state, DirtySliceState::Sealed);
        assert_eq!(tokio::fs::read(&records[0].path).await.unwrap(), b"small");
    }

    #[tokio::test]
    async fn synced_persist_keeps_legacy_json_record() {
        let temp = tempfile::tempdir().unwrap();
        let cache = FsWriteBackCache::new_with_sync(temp.path().to_path_buf(), true);
        let key = DirtySliceKey {
            ino: 7,
            chunk_id: 11,
            local_seq: 13,
            epoch: 0,
        };

        let path = cache
            .persist_slice_data(key, vec![Bytes::from_static(b"small")], 0)
            .await
            .unwrap();
        cache.seal_slice_record(key, 0, 5).await.unwrap();

        assert_eq!(tokio::fs::read(&path).await.unwrap(), b"small");
        assert!(key.meta_path(temp.path()).exists());
        let records = cache.recover().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].path, path);
    }

    #[tokio::test]
    async fn persisted_data_is_recoverable_only_after_record_is_sealed() {
        let temp = tempfile::tempdir().unwrap();
        let cache = FsWriteBackCache::new_with_sync(temp.path().to_path_buf(), false);
        let key = DirtySliceKey {
            ino: 7,
            chunk_id: 11,
            local_seq: 17,
            epoch: 0,
        };

        let path = cache
            .persist_slice_data(key, vec![Bytes::from_static(b"payload")], 0)
            .await
            .unwrap();

        assert_eq!(tokio::fs::read(&path).await.unwrap(), b"payload");
        assert!(
            cache.recover().await.unwrap().is_empty(),
            "data batches alone must not publish a recoverable dirty record"
        );

        cache.seal_slice_record(key, 4096, 7).await.unwrap();

        let records = cache.recover().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, key);
        assert_eq!(records[0].chunk_offset, 4096);
        assert_eq!(records[0].length, 7);
        assert_eq!(records[0].state, DirtySliceState::Sealed);
        assert_eq!(tokio::fs::read(&records[0].path).await.unwrap(), b"payload");
    }

    #[tokio::test]
    async fn persist_slice_merges_batches_by_chunk_offset() {
        let temp = tempfile::tempdir().unwrap();
        let cache = FsWriteBackCache::new_with_sync(temp.path().to_path_buf(), false);
        let key = DirtySliceKey {
            ino: 8,
            chunk_id: 12,
            local_seq: 14,
            epoch: 0,
        };

        cache
            .persist_slice_data(key, vec![Bytes::from_static(b"first")], 0)
            .await
            .unwrap();
        cache
            .persist_slice_data(key, vec![Bytes::from_static(b"second")], 5)
            .await
            .unwrap();
        cache.seal_slice_record(key, 0, 11).await.unwrap();

        let records = cache.recover().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].length, 11);
        assert_eq!(
            tokio::fs::read(&records[0].path).await.unwrap(),
            b"firstsecond"
        );
    }

    #[tokio::test]
    async fn overlay_dirty_range_reads_bucketed_record() {
        let temp = tempfile::tempdir().unwrap();
        let cache = FsWriteBackCache::new_with_sync(temp.path().to_path_buf(), false);
        let key = DirtySliceKey {
            ino: 8,
            chunk_id: 12,
            local_seq: 4096,
            epoch: 0,
        };

        let path = cache
            .persist_slice_data(key, vec![Bytes::from_static(b"payload")], 0)
            .await
            .unwrap();
        cache.seal_slice_record(key, 4, 7).await.unwrap();

        assert_eq!(path, key.slice_path(temp.path()));
        assert!(
            !key.legacy_slice_path(temp.path()).exists(),
            "new writes should use the bucketed dirty path"
        );
        assert!(
            !path.exists(),
            "sealed unsynced records should move out of the open slice path"
        );

        let mut buf = vec![0u8; 16];
        cache
            .overlay_dirty_range(key.ino, key.chunk_id, 0, &mut buf)
            .await
            .unwrap();

        assert_eq!(&buf[4..11], b"payload");
    }

    #[tokio::test]
    async fn read_dirty_range_requires_full_local_coverage() {
        let temp = tempfile::tempdir().unwrap();
        let cache = FsWriteBackCache::new_with_sync(temp.path().to_path_buf(), false);
        let key = DirtySliceKey {
            ino: 9,
            chunk_id: 21,
            local_seq: 2,
            epoch: 0,
        };

        cache
            .persist_slice_data(key, vec![Bytes::from_static(b"abcdefghij")], 0)
            .await
            .unwrap();
        cache.seal_slice_record(key, 100, 10).await.unwrap();

        assert_eq!(
            cache
                .read_dirty_range_if_fully_covered(9, 21, 102, 5)
                .await
                .unwrap()
                .unwrap(),
            b"cdefg"
        );
        assert!(
            cache
                .read_dirty_range_if_fully_covered(9, 21, 96, 8)
                .await
                .unwrap()
                .is_none(),
            "a leading gap must not be treated as covered"
        );
        assert!(
            cache
                .read_dirty_range_if_fully_covered(9, 21, 108, 8)
                .await
                .unwrap()
                .is_none(),
            "a trailing gap must not be treated as covered"
        );
    }

    #[tokio::test]
    async fn persist_reuses_created_dirty_directory_for_slice_and_meta() {
        let temp = tempfile::tempdir().unwrap();
        let cache = FsWriteBackCache::new_with_sync(temp.path().to_path_buf(), false);
        let key = DirtySliceKey {
            ino: 8,
            chunk_id: 12,
            local_seq: 31,
            epoch: 0,
        };
        let next_key = DirtySliceKey {
            local_seq: 32,
            ..key
        };

        cache
            .persist_slice_data(key, vec![Bytes::from_static(b"first")], 0)
            .await
            .unwrap();
        cache.seal_slice_record(key, 0, 5).await.unwrap();
        assert_eq!(cache.created_dir_count(), 1);

        cache
            .persist_slice_data(next_key, vec![Bytes::from_static(b"second")], 0)
            .await
            .unwrap();
        cache.seal_slice_record(next_key, 0, 6).await.unwrap();

        assert_eq!(cache.created_dir_count(), 1);
    }

    #[tokio::test]
    async fn remove_keeps_shared_chunk_directory_for_concurrent_slices() {
        let temp = tempfile::tempdir().unwrap();
        let cache = FsWriteBackCache::new_with_sync(temp.path().to_path_buf(), false);
        let old_key = DirtySliceKey {
            ino: 9,
            chunk_id: 13,
            local_seq: 21,
            epoch: 0,
        };
        let new_key = DirtySliceKey {
            local_seq: 22,
            ..old_key
        };

        cache
            .persist_slice_data(old_key, vec![Bytes::from_static(b"old")], 0)
            .await
            .unwrap();
        cache.seal_slice_record(old_key, 0, 3).await.unwrap();
        cache.remove(&old_key).await.unwrap();

        assert!(
            old_key.dir_path(temp.path()).exists(),
            "dirty slice cleanup must not remove the ino/chunk directory shared by newer slices"
        );
        cache
            .persist_slice_data(new_key, vec![Bytes::from_static(b"new")], 0)
            .await
            .unwrap();
        assert_eq!(
            tokio::fs::read(new_key.slice_path(temp.path()))
                .await
                .unwrap(),
            b"new"
        );
    }
}
