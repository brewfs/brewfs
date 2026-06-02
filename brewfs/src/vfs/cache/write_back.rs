use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
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
    /// Persist a sealed slice to local SSD. Returns the local file path.
    async fn persist_slice(
        &self,
        key: DirtySliceKey,
        data: Vec<Bytes>,
        chunk_offset: u64,
    ) -> anyhow::Result<PathBuf>;

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
///   {root}/dirty/{ino}/{chunk_id}/{local_seq}.slice  — raw data
///   {root}/dirty/{ino}/{chunk_id}/{local_seq}.meta   — JSON metadata
pub struct FsWriteBackCache {
    root: PathBuf,
    seq: AtomicU64,
}

impl FsWriteBackCache {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            seq: AtomicU64::new(0),
        }
    }

    pub fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    async fn write_meta(
        &self,
        key: &DirtySliceKey,
        record: &DirtySliceRecord,
    ) -> anyhow::Result<()> {
        let meta_path = key.meta_path(&self.root);
        let json = serde_json::to_vec(record)?;
        fs::write(&meta_path, &json).await?;
        Ok(())
    }

    async fn read_meta(&self, meta_path: &Path) -> anyhow::Result<DirtySliceRecord> {
        let data = fs::read(meta_path).await?;
        let record: DirtySliceRecord = serde_json::from_slice(&data)?;
        Ok(record)
    }
}

#[async_trait::async_trait]
impl WriteBackCache for FsWriteBackCache {
    async fn persist_slice(
        &self,
        key: DirtySliceKey,
        data: Vec<Bytes>,
        chunk_offset: u64,
    ) -> anyhow::Result<PathBuf> {
        let dir = key.dir_path(&self.root);
        fs::create_dir_all(&dir).await?;

        let slice_path = key.slice_path(&self.root);
        let tmp_path = dir.join(format!("{}.tmp", key.local_seq));

        let mut file = fs::File::create(&tmp_path).await?;
        let mut total_len = 0u64;
        for chunk in &data {
            file.write_all(chunk).await?;
            total_len += chunk.len() as u64;
        }
        file.flush().await?;
        file.sync_all().await?;
        drop(file);

        fs::rename(&tmp_path, &slice_path).await?;

        // fsync parent directory to ensure the rename is durable.
        let dir_fd = fs::File::open(&dir).await?;
        dir_fd.sync_all().await?;

        let record = DirtySliceRecord {
            key,
            ino: key.ino,
            chunk_id: key.chunk_id,
            chunk_offset,
            length: total_len,
            remote_slice_id: None,
            state: DirtySliceState::Sealed,
            path: slice_path.clone(),
            retry_count: 0,
            last_error: None,
        };
        self.write_meta(&key, &record).await?;

        Ok(slice_path)
    }

    async fn open_slice(
        &self,
        key: &DirtySliceKey,
    ) -> anyhow::Result<Box<dyn tokio::io::AsyncRead + Send + Unpin>> {
        let path = key.slice_path(&self.root);
        let file = fs::File::open(&path).await?;
        Ok(Box::new(file))
    }

    async fn mark_state(&self, key: &DirtySliceKey, state: DirtySliceState) -> anyhow::Result<()> {
        let meta_path = key.meta_path(&self.root);
        if meta_path.exists() {
            let mut record = self.read_meta(&meta_path).await?;
            record.state = state;
            self.write_meta(key, &record).await?;
        }
        Ok(())
    }

    async fn recover(&self) -> anyhow::Result<Vec<DirtySliceRecord>> {
        let dirty_root = self.root.join("dirty");
        if !dirty_root.exists() {
            return Ok(Vec::new());
        }

        let mut records = Vec::new();
        let mut ino_dirs = fs::read_dir(&dirty_root).await?;
        while let Some(ino_entry) = ino_dirs.next_entry().await? {
            if !ino_entry.file_type().await?.is_dir() {
                continue;
            }
            let mut chunk_dirs = fs::read_dir(ino_entry.path()).await?;
            while let Some(chunk_entry) = chunk_dirs.next_entry().await? {
                if !chunk_entry.file_type().await?.is_dir() {
                    continue;
                }
                let mut files = fs::read_dir(chunk_entry.path()).await?;
                while let Some(file_entry) = files.next_entry().await? {
                    let path = file_entry.path();
                    if path.extension().and_then(|e| e.to_str()) == Some("meta") {
                        match self.read_meta(&path).await {
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
                    }
                }
            }
        }
        Ok(records)
    }

    async fn remove(&self, key: &DirtySliceKey) -> anyhow::Result<()> {
        let slice_path = key.slice_path(&self.root);
        let meta_path = key.meta_path(&self.root);
        let _ = fs::remove_file(&slice_path).await;
        let _ = fs::remove_file(&meta_path).await;
        let dir = key.dir_path(&self.root);
        let _ = fs::remove_dir(&dir).await;
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

        if !chunk_dir.exists() {
            return Ok(());
        }

        let mut entries = fs::read_dir(&chunk_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("meta") {
                continue;
            }

            let record = match self.read_meta(&path).await {
                Ok(r) => r,
                Err(_) => continue,
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
}
