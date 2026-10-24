//! S3 object layer on top of the BrewFS VFS.
//!
//! Implements the `s3s::S3` trait (see `doc/protocols/s3-gateway.md` §4 for
//! the mapping table). All object data lives in the volume as regular files;
//! protocol metadata (etag, content-type, user metadata, directory-object
//! markers) lives in xattrs; multipart state lives under
//! `/.brewfs.sys/s3/`.

use std::collections::{HashMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use dashmap::DashSet;
use futures::{SinkExt, StreamExt};
use s3s::dto::*;
use s3s::{S3, S3Error, S3Request, S3Response, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::chunk::store::BlockStore;
use crate::gateway::s3::list::{ListCollector, ListQuery};
use crate::gateway::s3::multipart::{
    self, UploadMeta, multipart_etag, new_upload_id, parse_part_name,
};
use crate::gateway::s3::path::{self, BucketMode, PathError};
use crate::meta::MetaStore;
use crate::meta::client::MetaClient;
use crate::meta::store::{DirEntry, FileAttr, FileType};
use crate::vfs::fs::VFS;

/// Default content type when none is stored or provided.
const DEFAULT_CONTENT_TYPE: &str = "binary/octet-stream";

/// xattr carrying the object ETag.
pub const XATTR_ETAG: &str = "brewfs.s3.etag";
/// xattr carrying content-type + user metadata as JSON.
pub const XATTR_META: &str = "brewfs.s3.meta";
/// xattr marking an explicit S3 directory object.
pub const XATTR_DIROBJ: &str = "brewfs.s3.dirobj";

/// Read chunk size for streaming copies and GET bodies.
const STREAM_CHUNK: u64 = 4 * 1024 * 1024;

/// Number of fixed shards used to serialize writes to the same object key.
const KEY_LOCK_SHARDS: usize = 256;
/// Number of fixed shards used to serialize bucket lifecycle changes.
const BUCKET_LOCK_SHARDS: usize = 64;
const MAX_MULTIPART_PART_NUMBER: i32 = 10_000;

fn valid_part_number(part_number: i32) -> bool {
    (1..=MAX_MULTIPART_PART_NUMBER).contains(&part_number)
}

fn key_lock_shard(bucket: &str, key: &str) -> usize {
    let mut hasher = DefaultHasher::new();
    bucket.hash(&mut hasher);
    0u8.hash(&mut hasher);
    key.trim_end_matches('/').hash(&mut hasher);
    hasher.finish() as usize % KEY_LOCK_SHARDS
}

fn bucket_lock_shard(bucket: &str) -> usize {
    let mut hasher = DefaultHasher::new();
    bucket.hash(&mut hasher);
    hasher.finish() as usize % BUCKET_LOCK_SHARDS
}

/// Name of the hidden system directory at the volume root.
const SYS_DIR_NAME: &str = ".brewfs.sys";

/// JSON payload stored in the `brewfs.s3.meta` xattr.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ObjectMeta {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    content_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    metadata: Option<HashMap<String, String>>,
}

/// Behavior options of the S3 gateway.
#[derive(Debug, Clone)]
pub struct S3Options {
    pub bucket_mode: BucketMode,
    pub hide_dir_objects: bool,
}

/// The S3 object layer over one BrewFS volume.
pub struct BrewFsS3<S: BlockStore + Send + Sync + 'static> {
    vfs: VFS<S, MetaClient<dyn MetaStore>>,
    opts: S3Options,
    /// Fixed-size write serialization shards within this gateway instance.
    locks: Arc<[Mutex<()>; KEY_LOCK_SHARDS]>,
    /// Fixed-size bucket lifecycle serialization shards within this gateway instance.
    bucket_locks: Arc<[Mutex<()>; BUCKET_LOCK_SHARDS]>,
    active_staging: Arc<DashSet<String>>,
}

impl<S> Clone for BrewFsS3<S>
where
    S: BlockStore + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            vfs: self.vfs.clone(),
            opts: self.opts.clone(),
            locks: self.locks.clone(),
            bucket_locks: self.bucket_locks.clone(),
            active_staging: self.active_staging.clone(),
        }
    }
}

struct ActiveStagingPath {
    path: String,
    active: Arc<DashSet<String>>,
}

impl ActiveStagingPath {
    fn path(&self) -> &str {
        &self.path
    }
}

impl std::ops::Deref for ActiveStagingPath {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

impl Drop for ActiveStagingPath {
    fn drop(&mut self) {
        self.active.remove(&self.path);
    }
}

impl<S> BrewFsS3<S>
where
    S: BlockStore + Send + Sync + 'static,
{
    pub fn new(vfs: VFS<S, MetaClient<dyn MetaStore>>, opts: S3Options) -> Self {
        Self {
            vfs,
            opts,
            locks: Arc::new(std::array::from_fn(|_| Mutex::new(()))),
            bucket_locks: Arc::new(std::array::from_fn(|_| Mutex::new(()))),
            active_staging: Arc::new(DashSet::new()),
        }
    }

    /// Grants access to the underlying VFS (used by the cleanup task).
    pub fn vfs(&self) -> &VFS<S, MetaClient<dyn MetaStore>> {
        &self.vfs
    }

    pub(crate) fn lock_for(&self, bucket: &str, key: &str) -> &Mutex<()> {
        &self.locks[key_lock_shard(bucket, key)]
    }

    fn bucket_lock_for(&self, bucket: &str) -> &Mutex<()> {
        &self.bucket_locks[bucket_lock_shard(bucket)]
    }

    pub(crate) fn staging_path_is_active(&self, path: &str) -> bool {
        self.active_staging.contains(path)
    }

    fn reserve_staging_path(&self) -> ActiveStagingPath {
        let path = format!("{}/{}", multipart::tmp_dir(), new_upload_id());
        self.active_staging.insert(path.clone());
        ActiveStagingPath {
            path,
            active: self.active_staging.clone(),
        }
    }

    // ---- error mapping ----------------------------------------------------

    fn err(e: crate::vfs::error::VfsError) -> S3Error {
        use crate::vfs::error::VfsError;
        match e {
            VfsError::NotFound { .. } => s3_error!(NoSuchKey, "object not found"),
            VfsError::AlreadyExists { .. } => {
                s3_error!(BucketAlreadyExists, "target already exists")
            }
            VfsError::NotADirectory { .. } => {
                s3_error!(InvalidArgument, "a path component is not a directory")
            }
            VfsError::IsADirectory { .. } => s3_error!(NoSuchKey, "is a directory"),
            VfsError::DirectoryNotEmpty { .. } => s3_error!(BucketNotEmpty, "not empty"),
            VfsError::PermissionDenied { .. } => s3_error!(AccessDenied, "permission denied"),
            VfsError::InvalidInput => s3_error!(InvalidArgument, "invalid input"),
            VfsError::ReadOnlyFilesystem { .. } => s3_error!(AccessDenied, "read-only"),
            other => s3_error!(InternalError, "vfs error: {other}"),
        }
    }

    fn path_err(e: PathError) -> S3Error {
        match e {
            PathError::InvalidBucket(b) => s3_error!(NoSuchBucket, "bucket not found: {b}"),
            PathError::InvalidKey(k) => s3_error!(InvalidArgument, "invalid object key: {k}"),
            PathError::ReservedKey(k) => s3_error!(AccessDenied, "reserved object key: {k}"),
        }
    }

    // ---- helpers ------------------------------------------------------------

    async fn stat_bucket_root(&self, bucket: &str) -> S3Result<FileAttr> {
        let root = path::bucket_root(&self.opts.bucket_mode, bucket).map_err(Self::path_err)?;
        let attr = match self.vfs.stat(&root).await {
            Ok(attr) => attr,
            Err(crate::vfs::error::VfsError::NotFound { .. }) => {
                return Err(s3_error!(NoSuchBucket, "bucket not found: {bucket}"));
            }
            Err(e) => return Err(Self::err(e)),
        };
        if attr.kind != FileType::Dir {
            return Err(s3_error!(NoSuchBucket, "bucket not found: {bucket}"));
        }
        Ok(attr)
    }

    async fn read_dir_entries(&self, dir_path: &str) -> S3Result<Vec<DirEntry>> {
        let attr = match self.vfs.stat(dir_path).await {
            Ok(a) => a,
            Err(crate::vfs::error::VfsError::NotFound { .. }) => return Ok(Vec::new()),
            Err(e) => return Err(Self::err(e)),
        };
        if attr.kind != FileType::Dir {
            return Ok(Vec::new());
        }
        let fh = self.vfs.opendir(attr.ino).await.map_err(Self::err)?;
        let mut entries = Vec::new();
        let mut offset = 0;
        loop {
            let page = self
                .vfs
                .readdir(fh, offset)
                .ok_or_else(|| s3_error!(InternalError, "directory handle became unavailable"))?;
            if page.is_empty() {
                break;
            }
            offset += page.len() as u64;
            entries.extend(page);
        }
        self.vfs.closedir(fh).map_err(Self::err)?;
        Ok(entries)
    }

    async fn get_xattr(&self, ino: i64, name: &str) -> Option<String> {
        match self.vfs.get_xattr_ino(ino, name).await {
            Ok(Some(bytes)) => String::from_utf8(bytes).ok(),
            _ => None,
        }
    }

    async fn set_xattr(&self, ino: i64, name: &str, value: &str) -> S3Result<()> {
        self.vfs
            .set_xattr_ino(ino, name, value.as_bytes(), 0)
            .await
            .map_err(Self::err)
    }

    async fn read_etag(&self, ino: i64) -> Option<String> {
        self.get_xattr(ino, XATTR_ETAG).await
    }

    async fn read_object_meta(&self, ino: i64) -> ObjectMeta {
        self.get_xattr(ino, XATTR_META)
            .await
            .and_then(|raw| serde_json::from_str(&raw).ok())
            .unwrap_or_default()
    }

    async fn is_dir_object(&self, ino: i64) -> bool {
        self.get_xattr(ino, XATTR_DIROBJ).await.is_some()
    }

    /// ETag reported for objects written outside the gateway (no stored etag).
    fn fallback_etag(attr: &FileAttr) -> String {
        format!("{:x}-{:x}", attr.ino, attr.mtime)
    }

    fn mtime_timestamp(attr: &FileAttr) -> Option<Timestamp> {
        // VFS file attrs carry mtime in nanoseconds.
        Some(Timestamp::from(
            UNIX_EPOCH + Duration::from_nanos(attr.mtime.max(0) as u64),
        ))
    }

    /// Streams a blob into `dst` (created/truncated), returning size and MD5.
    async fn write_blob(&self, dst: &str, body: &mut StreamingBlob) -> S3Result<(u64, String)> {
        if self.vfs.exists(dst).await {
            self.vfs.unlink(dst).await.map_err(Self::err)?;
        }
        let ino = self.vfs.create_file(dst).await.map_err(Self::err)?;
        let result: S3Result<(u64, String)> = async {
            let attr = self.vfs.stat(dst).await.map_err(Self::err)?;
            let guard = self
                .vfs
                .open_guard(ino, attr, false, true)
                .await
                .map_err(Self::err)?;

            let mut offset: u64 = 0;
            let mut md = md5::Context::new();
            while let Some(chunk) = body.next().await {
                let chunk = chunk.map_err(|e| s3_error!(InternalError, "body read: {e}"))?;
                if !chunk.is_empty() {
                    guard.write(offset, &chunk).await.map_err(Self::err)?;
                    md.consume(&chunk);
                    offset += chunk.len() as u64;
                }
            }
            guard.close().await.map_err(Self::err)?;
            let etag = format!("{:x}", md.compute());
            Ok((offset, etag))
        }
        .await;
        if result.is_err() {
            let _ = self.vfs.unlink(dst).await;
        }
        result
    }

    /// Reads a byte range of a file as a streaming blob. Chunks are produced
    /// by a spawned task and forwarded through an mpsc channel (the channel
    /// receiver is `Send + Sync`, as required by `StreamingBlob::wrap`).
    async fn read_stream(
        &self,
        attr: FileAttr,
        offset: u64,
        length: u64,
    ) -> S3Result<StreamingBlob> {
        let guard = self
            .vfs
            .open_guard(attr.ino, attr, true, false)
            .await
            .map_err(Self::err)?;
        let (mut tx, rx) =
            futures::channel::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(4);
        tokio::spawn(async move {
            let mut offset = offset;
            let mut remaining = length;
            let mut read_error = None;
            while remaining > 0 {
                let len = remaining.min(STREAM_CHUNK) as usize;
                match guard.read(offset, len).await {
                    Ok(data) if data.is_empty() => {
                        read_error = Some(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "object ended before the requested range",
                        ));
                        break;
                    }
                    Ok(data) => {
                        let n = data.len() as u64;
                        if tx.send(Ok(bytes::Bytes::from(data))).await.is_err() {
                            break;
                        }
                        offset += n;
                        remaining -= n;
                    }
                    Err(e) => {
                        read_error = Some(std::io::Error::other(format!("read: {e}")));
                        break;
                    }
                }
            }

            let close_error = guard
                .close()
                .await
                .err()
                .map(|e| std::io::Error::other(format!("close: {e}")));
            if let Some(error) = read_error.or(close_error) {
                let _ = tx.send(Err(error)).await;
            }
        });
        Ok(StreamingBlob::wrap(rx))
    }

    async fn copy_range_exact(
        &self,
        src_ino: i64,
        src_offset: u64,
        dst_ino: i64,
        dst_offset: u64,
        length: u64,
    ) -> S3Result<()> {
        let mut copied = 0;
        while copied < length {
            let chunk_len = (length - copied).min(STREAM_CHUNK);
            let written = self
                .vfs
                .copy_file_range_inodes(
                    src_ino,
                    src_offset + copied,
                    dst_ino,
                    dst_offset + copied,
                    chunk_len,
                )
                .await
                .map_err(Self::err)?;
            if written as u64 != chunk_len {
                return Err(s3_error!(
                    InternalError,
                    "short copy: wrote {written} of {chunk_len} bytes"
                ));
            }
            copied += chunk_len;
        }
        Ok(())
    }

    /// Copies one inode into a fresh tmp file and renames it to `dst`.
    async fn copy_object_data(
        &self,
        src_attr: &FileAttr,
        dst: &str,
        etag: Option<String>,
        meta: &ObjectMeta,
    ) -> S3Result<(u64, FileAttr, String)> {
        let tmp = self.reserve_staging_path();
        if self.vfs.exists(tmp.path()).await {
            self.vfs.unlink(tmp.path()).await.map_err(Self::err)?;
        }
        let tmp_ino = self.vfs.create_file(tmp.path()).await.map_err(Self::err)?;
        let result: S3Result<(FileAttr, String)> = async {
            self.copy_range_exact(src_attr.ino, 0, tmp_ino, 0, src_attr.size)
                .await?;
            let tmp_attr = self.vfs.stat(&tmp).await.map_err(Self::err)?;
            let etag = etag.unwrap_or_else(|| Self::fallback_etag(&tmp_attr));
            self.set_xattr(tmp_ino, XATTR_ETAG, &etag).await?;
            self.set_xattr(tmp_ino, XATTR_META, &serde_json::to_string(meta).unwrap())
                .await?;

            if let Some(parent) = std::path::Path::new(dst).parent() {
                let parent = parent.to_string_lossy().to_string();
                if !parent.is_empty() {
                    self.vfs.mkdir_p(&parent).await.map_err(Self::err)?;
                }
            }
            if self.vfs.exists(dst).await {
                let attr = self.vfs.stat(dst).await.map_err(Self::err)?;
                if attr.kind == FileType::Dir {
                    return Err(s3_error!(InvalidArgument, "target is a directory"));
                }
            }
            self.vfs.rename(&tmp, dst).await.map_err(Self::err)?;
            Ok((tmp_attr, etag))
        }
        .await;

        match result {
            Ok((final_attr, etag)) => Ok((final_attr.size, final_attr, etag)),
            Err(e) => {
                let _ = self.vfs.unlink(&tmp).await;
                Err(e)
            }
        }
    }

    /// Removes empty implicit parent directories left behind after a delete,
    /// stopping at the bucket root, at the volume root, or at the first
    /// directory object / non-empty directory.
    async fn prune_empty_parents(&self, key_path: &str, bucket_root: &str) {
        let mut current = key_path.to_string();
        while let Some(parent) = std::path::Path::new(&current)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
        {
            if parent.is_empty() || parent == "/" || parent == bucket_root {
                return;
            }
            if parent == "/.brewfs.sys" || parent.starts_with("/.brewfs.sys/") {
                return;
            }
            let Ok(attr) = self.vfs.stat(&parent).await else {
                return;
            };
            if attr.kind != FileType::Dir {
                return;
            }
            if self.is_dir_object(attr.ino).await {
                return;
            }
            let Ok(entries) = self.read_dir_entries(&parent).await else {
                return;
            };
            if !entries.is_empty() {
                return;
            }
            if self.vfs.rmdir(&parent).await.is_err() {
                return;
            }
            current = parent;
        }
    }

    /// Deletes one object; shared by DeleteObject and DeleteObjects.
    async fn delete_object_internal(&self, bucket: &str, key: &str) -> S3Result<()> {
        let mode = &self.opts.bucket_mode;
        let bucket_root = path::bucket_root(mode, bucket).map_err(Self::path_err)?;
        let target = path::object_path(mode, bucket, key).map_err(Self::path_err)?;

        if target == bucket_root || target == "/" {
            // Deleting the bucket root via the object API is not allowed.
            return Err(s3_error!(InvalidArgument, "cannot delete bucket root"));
        }

        let attr = match self.vfs.stat(&target).await {
            Ok(a) => a,
            Err(crate::vfs::error::VfsError::NotFound { .. }) => return Ok(()),
            Err(e) => return Err(Self::err(e)),
        };

        if attr.kind == FileType::Dir {
            if !key.ends_with('/') || !self.is_dir_object(attr.ino).await {
                return Ok(());
            }
            let _ = self.vfs.remove_xattr_ino(attr.ino, XATTR_DIROBJ).await;
            let entries = self.read_dir_entries(&target).await?;
            if entries.is_empty() {
                self.vfs.rmdir(&target).await.map_err(Self::err)?;
            }
            return Ok(());
        }
        if key.ends_with('/') {
            return Ok(());
        }

        self.vfs.unlink(&target).await.map_err(Self::err)?;
        self.prune_empty_parents(&target, &bucket_root).await;
        Ok(())
    }

    /// Resolves an object and decides whether it is visible as an object
    /// (file, or directory with the dir-object marker when `allow_dir_obj`).
    async fn resolve_object(
        &self,
        mode: &BucketMode,
        bucket: &str,
        key: &str,
    ) -> S3Result<(String, FileAttr, bool)> {
        self.stat_bucket_root(bucket).await?;
        let target = path::object_path(mode, bucket, key).map_err(Self::path_err)?;
        if target == "/" {
            return Err(s3_error!(NoSuchKey, "no such object"));
        }
        let attr = match self.vfs.stat(&target).await {
            Ok(a) => a,
            Err(e) => return Err(Self::err(e)),
        };
        if attr.kind == FileType::Dir {
            let dirobj = key.ends_with('/') && self.is_dir_object(attr.ino).await;
            if !dirobj {
                return Err(s3_error!(NoSuchKey, "no such object"));
            }
            return Ok((target, attr, true));
        }
        if key.ends_with('/') {
            return Err(s3_error!(NoSuchKey, "no such object"));
        }
        Ok((target, attr, false))
    }

    // ---- listing ------------------------------------------------------------

    async fn walk_dir(
        &self,
        root_path: &str,
        root_key: &str,
        collector: &mut ListCollector,
    ) -> S3Result<()> {
        let mut pending = vec![(root_path.to_string(), root_key.to_string())];
        while let Some((dir_path, parent_key)) = pending.pop() {
            let entries = self.read_dir_entries(&dir_path).await?;
            for entry in entries {
                if dir_path == "/" && entry.name == SYS_DIR_NAME {
                    continue;
                }
                let child_path = format!("{dir_path}/{}", entry.name);
                let is_dir = entry.kind == FileType::Dir;
                let mut dirobj = false;
                let mut etag = None;
                let (size, mtime) = match self.vfs.stat_ino(entry.ino).await {
                    Some(a) => (a.size, a.mtime),
                    None => (0, 0),
                };
                if is_dir {
                    dirobj = self.is_dir_object(entry.ino).await;
                } else {
                    etag = self.read_etag(entry.ino).await;
                }
                collector.push_entry(&parent_key, &entry.name, is_dir, size, mtime, dirobj, etag);
                if is_dir && collector.should_descend(&parent_key, &entry.name, dirobj) {
                    let child_key = format!("{parent_key}{}/", entry.name);
                    pending.push((child_path, child_key));
                }
            }
        }
        Ok(())
    }

    async fn list_keys(
        &self,
        bucket: &str,
        query: ListQuery,
    ) -> S3Result<crate::gateway::s3::list::ListResult> {
        let mode = &self.opts.bucket_mode;
        let root = path::bucket_root(mode, bucket).map_err(Self::path_err)?;
        path::validate_key_namespace(mode, &query.prefix).map_err(Self::path_err)?;
        self.stat_bucket_root(bucket).await?;
        let mut collector = ListCollector::new(query);
        self.walk_dir(&root, "", &mut collector).await?;
        Ok(collector.finish())
    }

    // ---- multipart ------------------------------------------------------------

    pub(crate) async fn read_upload_meta(&self, upload_id: &str) -> S3Result<UploadMeta> {
        if !multipart::is_valid_upload_id(upload_id) {
            return Err(s3_error!(NoSuchUpload, "no such upload"));
        }
        let target = UploadMeta::target_path(upload_id);
        match self.vfs.stat(&target).await {
            Ok(attr) => {
                let size = attr.size;
                let guard = self
                    .vfs
                    .open_guard(attr.ino, attr, true, false)
                    .await
                    .map_err(Self::err)?;
                let data = guard.read(0, size as usize).await.map_err(Self::err)?;
                guard.close().await.map_err(Self::err)?;
                serde_json::from_slice(&data)
                    .map_err(|e| s3_error!(InternalError, "corrupt upload meta: {e}"))
            }
            Err(crate::vfs::error::VfsError::NotFound { .. }) => {
                Err(s3_error!(NoSuchUpload, "no such upload"))
            }
            Err(e) => Err(Self::err(e)),
        }
    }

    async fn read_matching_upload_meta(
        &self,
        upload_id: &str,
        bucket: &str,
        key: &str,
    ) -> S3Result<UploadMeta> {
        let meta = self.read_upload_meta(upload_id).await?;
        if meta.bucket != bucket || meta.key != key {
            return Err(s3_error!(NoSuchUpload, "upload does not match bucket/key"));
        }
        path::object_path(&self.opts.bucket_mode, bucket, key).map_err(Self::path_err)?;
        self.stat_bucket_root(bucket).await?;
        Ok(meta)
    }

    async fn bucket_has_multipart_uploads(&self, bucket: &str) -> S3Result<bool> {
        for hh in self.read_dir_entries(&multipart::uploads_dir()).await? {
            let hh_dir = format!("{}/{}", multipart::uploads_dir(), hh.name);
            for upload in self.read_dir_entries(&hh_dir).await? {
                if let Ok(meta) = self.read_upload_meta(&upload.name).await
                    && meta.bucket == bucket
                {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

#[async_trait]
impl<S> S3 for BrewFsS3<S>
where
    S: BlockStore + Send + Sync + 'static,
{
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let input = req.input;
        let bucket = input.bucket.as_str().to_string();
        let bucket_lock = self.bucket_lock_for(&bucket);
        let _bucket_guard = bucket_lock.lock().await;
        match &self.opts.bucket_mode {
            BucketMode::Single { bucket: expected } => {
                if bucket == *expected {
                    return Err(s3_error!(BucketAlreadyOwnedByYou, "bucket already exists"));
                }
                Err(s3_error!(
                    InvalidBucketName,
                    "this gateway serves a single bucket"
                ))
            }
            BucketMode::Multi => {
                let dir = path::bucket_root(&self.opts.bucket_mode, &bucket)
                    .map_err(|_| s3_error!(InvalidBucketName, "invalid bucket name"))?;
                match self.vfs.mkdir_err(&dir).await {
                    Ok(_) => Ok(S3Response::new(CreateBucketOutput::default())),
                    Err(crate::vfs::error::VfsError::AlreadyExists { .. }) => {
                        Err(s3_error!(BucketAlreadyOwnedByYou, "bucket already exists"))
                    }
                    Err(e) => Err(Self::err(e)),
                }
            }
        }
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        let input = req.input;
        let bucket = input.bucket.as_str().to_string();
        let bucket_lock = self.bucket_lock_for(&bucket);
        let _bucket_guard = bucket_lock.lock().await;
        match &self.opts.bucket_mode {
            BucketMode::Single { .. } => Err(s3_error!(
                MethodNotAllowed,
                "cannot delete the volume bucket in single-bucket mode"
            )),
            BucketMode::Multi => {
                let dir =
                    path::bucket_root(&self.opts.bucket_mode, &bucket).map_err(Self::path_err)?;
                self.stat_bucket_root(&bucket).await?;
                if self.bucket_has_multipart_uploads(&bucket).await? {
                    return Err(s3_error!(
                        BucketNotEmpty,
                        "bucket has active multipart uploads"
                    ));
                }
                match self.vfs.rmdir(&dir).await {
                    Ok(_) => Ok(S3Response::new(DeleteBucketOutput::default())),
                    Err(crate::vfs::error::VfsError::NotFound { .. }) => {
                        Err(s3_error!(NoSuchBucket, "no such bucket"))
                    }
                    Err(crate::vfs::error::VfsError::DirectoryNotEmpty { .. }) => {
                        Err(s3_error!(BucketNotEmpty, "bucket is not empty"))
                    }
                    Err(e) => Err(Self::err(e)),
                }
            }
        }
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        self.stat_bucket_root(req.input.bucket.as_str()).await?;
        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    async fn list_buckets(
        &self,
        _req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let owner = Owner::default();
        let buckets = match &self.opts.bucket_mode {
            BucketMode::Single { bucket } => vec![Bucket {
                name: Some(bucket.clone()),
                creation_date: None,
                bucket_region: None,
            }],
            BucketMode::Multi => {
                let mut out = Vec::new();
                for entry in self.read_dir_entries("/").await? {
                    if entry.name == SYS_DIR_NAME || entry.kind != FileType::Dir {
                        continue;
                    }
                    if path::is_valid_bucket_name(&entry.name) {
                        out.push(Bucket {
                            name: Some(entry.name.clone()),
                            creation_date: None,
                            bucket_region: None,
                        });
                    }
                }
                out
            }
        };
        Ok(S3Response::new(ListBucketsOutput {
            buckets: Some(buckets),
            owner: Some(owner),
            ..Default::default()
        }))
    }

    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        self.stat_bucket_root(req.input.bucket.as_str()).await?;
        Ok(S3Response::new(GetBucketLocationOutput::default()))
    }

    async fn put_object(
        &self,
        mut req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let input = &mut req.input;
        if input.if_match.is_some() || input.if_none_match.is_some() {
            return Err(s3_error!(
                NotImplemented,
                "conditional PUT is not supported"
            ));
        }
        let bucket = input.bucket.as_str().to_string();
        let key = input.key.as_str().to_string();
        let mode = &self.opts.bucket_mode;
        self.stat_bucket_root(&bucket).await?;
        let target = path::object_path(mode, &bucket, &key).map_err(Self::path_err)?;
        if target == "/" {
            return Err(s3_error!(InvalidArgument, "empty key"));
        }

        let content_type = input
            .content_type
            .as_ref()
            .map(|ct| ct.as_str().to_string());
        let metadata: HashMap<String, String> = input
            .metadata
            .as_ref()
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let body = input
            .body
            .take()
            .ok_or_else(|| s3_error!(InvalidArgument, "missing body"))?;
        let mut body = body;

        let lock = self.lock_for(&bucket, &key);
        let _guard = lock.lock().await;

        // Explicit directory object: PUT with a key ending in '/'.
        if key.ends_with('/') {
            while let Some(chunk) = body.next().await {
                let chunk = chunk.map_err(|e| s3_error!(InternalError, "body read: {e}"))?;
                if !chunk.is_empty() {
                    return Err(s3_error!(
                        InvalidArgument,
                        "directory object body must be empty"
                    ));
                }
            }
            let bucket_lock = self.bucket_lock_for(&bucket);
            let _bucket_guard = bucket_lock.lock().await;
            self.stat_bucket_root(&bucket).await?;
            if self.vfs.exists(&target).await {
                let attr = self.vfs.stat(&target).await.map_err(Self::err)?;
                if attr.kind != FileType::Dir {
                    return Err(s3_error!(InvalidArgument, "target is not a directory"));
                }
            }
            self.vfs.mkdir_p(&target).await.map_err(Self::err)?;
            let attr = self.vfs.stat(&target).await.map_err(Self::err)?;
            let meta = ObjectMeta {
                content_type,
                metadata: if metadata.is_empty() {
                    None
                } else {
                    Some(metadata)
                },
            };
            self.set_xattr(attr.ino, XATTR_META, &serde_json::to_string(&meta).unwrap())
                .await?;
            let etag = format!("{:x}", md5::Context::new().compute());
            self.set_xattr(attr.ino, XATTR_ETAG, &etag).await?;
            self.set_xattr(attr.ino, XATTR_DIROBJ, "").await?;
            return Ok(S3Response::new(PutObjectOutput {
                e_tag: Some(ETag::Strong(etag)),
                ..Default::default()
            }));
        }

        let tmp = self.reserve_staging_path();
        let (size, etag) = self.write_blob(tmp.path(), &mut body).await?;
        let meta = ObjectMeta {
            content_type,
            metadata: if metadata.is_empty() {
                None
            } else {
                Some(metadata)
            },
        };
        let bucket_lock = self.bucket_lock_for(&bucket);
        let _bucket_guard = bucket_lock.lock().await;
        let publish_result: S3Result<()> = async {
            self.stat_bucket_root(&bucket).await?;
            let attr = self.vfs.stat(&tmp).await.map_err(Self::err)?;
            self.set_xattr(attr.ino, XATTR_ETAG, &etag).await?;
            self.set_xattr(attr.ino, XATTR_META, &serde_json::to_string(&meta).unwrap())
                .await?;

            if let Some(parent) = std::path::Path::new(&target).parent() {
                let parent = parent.to_string_lossy().to_string();
                if !parent.is_empty() {
                    self.vfs.mkdir_p(&parent).await.map_err(Self::err)?;
                }
            }
            if self.vfs.exists(&target).await {
                let attr = self.vfs.stat(&target).await.map_err(Self::err)?;
                if attr.kind == FileType::Dir {
                    return Err(s3_error!(InvalidArgument, "key maps to a directory"));
                }
            }
            self.vfs.rename(&tmp, &target).await.map_err(Self::err)
        }
        .await;
        if let Err(e) = publish_result {
            let _ = self.vfs.unlink(&tmp).await;
            return Err(e);
        }

        tracing::debug!(bucket = %bucket, key = %key, size, "s3 put_object");
        Ok(S3Response::new(PutObjectOutput {
            e_tag: Some(ETag::Strong(etag)),
            ..Default::default()
        }))
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let input = req.input;
        if input.if_match.is_some()
            || input.if_none_match.is_some()
            || input.if_modified_since.is_some()
            || input.if_unmodified_since.is_some()
        {
            return Err(s3_error!(
                NotImplemented,
                "conditional GET is not supported"
            ));
        }
        let bucket = input.bucket.as_str().to_string();
        let key = input.key.as_str().to_string();
        let lock = self.lock_for(&bucket, &key);
        let _guard = lock.lock().await;
        let mode = &self.opts.bucket_mode;
        let (_target, attr, is_dir_obj) = self.resolve_object(mode, &bucket, &key).await?;

        let meta = self.read_object_meta(attr.ino).await;
        let metadata = meta.metadata.clone().map(|m| {
            m.into_iter()
                .map(|(k, v)| (k, MetadataValue::from(v)))
                .collect::<HashMap<String, MetadataValue>>()
        });
        let etag = self
            .read_etag(attr.ino)
            .await
            .unwrap_or_else(|| Self::fallback_etag(&attr));

        // Directory objects are always empty.
        if is_dir_obj {
            if input.range.is_some() {
                return Err(s3_error!(InvalidRange, "range not satisfiable"));
            }
            return Ok(S3Response::new(GetObjectOutput {
                body: Some(StreamingBlob::from_bytes(Default::default())),
                content_length: Some(0),
                e_tag: Some(ETag::Strong(etag)),
                content_type: Some(
                    meta.content_type
                        .clone()
                        .unwrap_or_else(|| DEFAULT_CONTENT_TYPE.to_string()),
                ),
                last_modified: Self::mtime_timestamp(&attr),
                metadata,
                accept_ranges: Some(AcceptRanges::from("bytes")),
                ..Default::default()
            }));
        }

        // Range handling.
        let (offset, length, content_range) = match input.range.as_ref() {
            Some(range) => {
                let checked = range
                    .check(attr.size)
                    .map_err(|_| s3_error!(InvalidRange, "range not satisfiable"))?;
                if checked.is_empty() {
                    return Err(s3_error!(InvalidRange, "range not satisfiable"));
                }
                let length = checked.end - checked.start;
                (
                    checked.start,
                    length,
                    Some(ContentRange::from(format!(
                        "bytes {}-{}/{}",
                        checked.start,
                        checked.end - 1,
                        attr.size
                    ))),
                )
            }
            None => (0, attr.size, None),
        };

        let body = self.read_stream(attr.clone(), offset, length).await?;
        Ok(S3Response::new(GetObjectOutput {
            body: Some(body),
            content_length: Some(length as i64),
            content_range,
            e_tag: Some(ETag::Strong(etag)),
            content_type: Some(
                meta.content_type
                    .unwrap_or_else(|| DEFAULT_CONTENT_TYPE.to_string()),
            ),
            last_modified: Self::mtime_timestamp(&attr),
            metadata,
            accept_ranges: Some(AcceptRanges::from("bytes")),
            ..Default::default()
        }))
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let input = req.input;
        if input.if_match.is_some()
            || input.if_none_match.is_some()
            || input.if_modified_since.is_some()
            || input.if_unmodified_since.is_some()
        {
            return Err(s3_error!(
                NotImplemented,
                "conditional HEAD is not supported"
            ));
        }
        let bucket = input.bucket.as_str().to_string();
        let key = input.key.as_str().to_string();
        let lock = self.lock_for(&bucket, &key);
        let _guard = lock.lock().await;
        let mode = &self.opts.bucket_mode;
        let (_target, attr, is_dir_obj) = self.resolve_object(mode, &bucket, &key).await?;

        let meta = self.read_object_meta(attr.ino).await;
        let etag = self
            .read_etag(attr.ino)
            .await
            .unwrap_or_else(|| Self::fallback_etag(&attr));
        let metadata = meta.metadata.map(|m| {
            m.into_iter()
                .map(|(k, v)| (k, MetadataValue::from(v)))
                .collect::<HashMap<String, MetadataValue>>()
        });

        Ok(S3Response::new(HeadObjectOutput {
            content_length: Some(if is_dir_obj { 0 } else { attr.size as i64 }),
            e_tag: Some(ETag::Strong(etag)),
            content_type: Some(
                meta.content_type
                    .unwrap_or_else(|| DEFAULT_CONTENT_TYPE.to_string()),
            ),
            last_modified: Self::mtime_timestamp(&attr),
            metadata,
            accept_ranges: Some(AcceptRanges::from("bytes")),
            ..Default::default()
        }))
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let input = req.input;
        if input.if_match.is_some()
            || input.if_match_last_modified_time.is_some()
            || input.if_match_size.is_some()
        {
            return Err(s3_error!(
                NotImplemented,
                "conditional DELETE is not supported"
            ));
        }
        let bucket = input.bucket.as_str().to_string();
        let key = input.key.as_str().to_string();
        let lock = self.lock_for(&bucket, &key);
        let _guard = lock.lock().await;
        self.delete_object_internal(&bucket, &key).await?;
        Ok(S3Response::with_status(
            DeleteObjectOutput::default(),
            axum::http::StatusCode::NO_CONTENT,
        ))
    }

    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let input = req.input;
        let bucket = input.bucket.as_str().to_string();
        let objects = input.delete.objects;

        let mut deleted = Vec::new();
        let mut errors = Vec::new();
        for obj in objects {
            let key = obj.key.as_str().to_string();
            let lock = self.lock_for(&bucket, &key);
            let _guard = lock.lock().await;
            match self.delete_object_internal(&bucket, &key).await {
                Ok(()) => deleted.push(DeletedObject {
                    key: Some(obj.key),
                    ..Default::default()
                }),
                Err(e) => errors.push(s3s::dto::Error {
                    code: Some(e.code().as_str().to_string()),
                    key: Some(obj.key),
                    message: e.message().map(|m| m.to_string()),
                    ..Default::default()
                }),
            }
        }
        Ok(S3Response::new(DeleteObjectsOutput {
            deleted: if deleted.is_empty() {
                None
            } else {
                Some(deleted)
            },
            errors: if errors.is_empty() {
                None
            } else {
                Some(errors)
            },
            ..Default::default()
        }))
    }

    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let input = req.input;
        if input.copy_source_if_match.is_some()
            || input.copy_source_if_none_match.is_some()
            || input.copy_source_if_modified_since.is_some()
            || input.copy_source_if_unmodified_since.is_some()
        {
            return Err(s3_error!(
                NotImplemented,
                "conditional COPY is not supported"
            ));
        }
        let dst_bucket = input.bucket.as_str().to_string();
        let dst_key = input.key.as_str().to_string();
        let mode = &self.opts.bucket_mode;

        // Parse the copy source (bucket/key form; ARN forms unsupported).
        let (src_bucket, src_key) = match input.copy_source {
            CopySource::Bucket { bucket, key, .. } => (bucket.to_string(), key.to_string()),
            _ => {
                return Err(s3_error!(
                    NotImplemented,
                    "only bucket/key copy sources are supported"
                ));
            }
        };

        self.stat_bucket_root(&dst_bucket).await?;
        path::object_path(mode, &src_bucket, &src_key).map_err(Self::path_err)?;
        let dst_path = path::object_path(mode, &dst_bucket, &dst_key).map_err(Self::path_err)?;
        if dst_path == "/" {
            return Err(s3_error!(InvalidArgument, "empty destination key"));
        }

        let src_lock_shard = key_lock_shard(&src_bucket, &src_key);
        let dst_lock_shard = key_lock_shard(&dst_bucket, &dst_key);
        let (first_lock_shard, second_lock_shard) = if src_lock_shard <= dst_lock_shard {
            (src_lock_shard, dst_lock_shard)
        } else {
            (dst_lock_shard, src_lock_shard)
        };
        let _first_guard = self.locks[first_lock_shard].lock().await;
        let _second_guard = if first_lock_shard == second_lock_shard {
            None
        } else {
            Some(self.locks[second_lock_shard].lock().await)
        };
        let (_src_path, src_attr, _) = self.resolve_object(mode, &src_bucket, &src_key).await?;

        let replace = matches!(
            input.metadata_directive.as_ref().map(|d| d.as_str()),
            Some("REPLACE")
        );
        let meta = if replace {
            ObjectMeta {
                content_type: input
                    .content_type
                    .as_ref()
                    .map(|ct| ct.as_str().to_string()),
                metadata: input.metadata.as_ref().map(|m| {
                    m.iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect()
                }),
            }
        } else {
            self.read_object_meta(src_attr.ino).await
        };
        let etag = self
            .read_etag(src_attr.ino)
            .await
            .unwrap_or_else(|| Self::fallback_etag(&src_attr));
        let bucket_lock = self.bucket_lock_for(&dst_bucket);
        let _bucket_guard = bucket_lock.lock().await;
        self.stat_bucket_root(&dst_bucket).await?;

        if dst_key.ends_with('/') {
            if src_attr.size != 0 {
                return Err(s3_error!(
                    InvalidArgument,
                    "directory object source must be empty"
                ));
            }
            if self.vfs.exists(&dst_path).await {
                let attr = self.vfs.stat(&dst_path).await.map_err(Self::err)?;
                if attr.kind != FileType::Dir {
                    return Err(s3_error!(InvalidArgument, "target is not a directory"));
                }
            }
            self.vfs.mkdir_p(&dst_path).await.map_err(Self::err)?;
            let attr = self.vfs.stat(&dst_path).await.map_err(Self::err)?;
            self.set_xattr(attr.ino, XATTR_ETAG, &etag).await?;
            self.set_xattr(attr.ino, XATTR_META, &serde_json::to_string(&meta).unwrap())
                .await?;
            self.set_xattr(attr.ino, XATTR_DIROBJ, "").await?;
            return Ok(S3Response::new(CopyObjectOutput {
                copy_object_result: Some(CopyObjectResult {
                    e_tag: Some(ETag::Strong(etag)),
                    last_modified: Self::mtime_timestamp(&attr),
                    ..Default::default()
                }),
                ..Default::default()
            }));
        }

        let (size, final_attr, etag) = self
            .copy_object_data(&src_attr, &dst_path, Some(etag), &meta)
            .await?;

        let last_modified = Self::mtime_timestamp(&final_attr);
        tracing::debug!(bucket = %dst_bucket, key = %dst_key, size, "s3 copy_object");
        Ok(S3Response::new(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: Some(ETag::Strong(etag)),
                last_modified,
                ..Default::default()
            }),
            ..Default::default()
        }))
    }

    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        let input = req.input;
        let bucket = input.bucket.as_str().to_string();
        let query = ListQuery {
            prefix: input
                .prefix
                .as_ref()
                .map(|p| p.as_str().to_string())
                .unwrap_or_default(),
            delimiter: input.delimiter.as_ref().map(|d| d.as_str().to_string()),
            start_after: input
                .marker
                .as_ref()
                .map(|m| m.as_str().to_string())
                .unwrap_or_default(),
            max_keys: input
                .max_keys
                .map(|m| usize::try_from(m).unwrap_or(1000))
                .unwrap_or(1000),
            hide_dir_objects: self.opts.hide_dir_objects,
        };
        let result = self.list_keys(&bucket, query).await?;

        let contents: Vec<Object> = result
            .objects
            .iter()
            .map(|o| Object {
                key: Some(o.key.clone()),
                size: Some(if o.is_dir_object { 0 } else { o.size as i64 }),
                last_modified: Some(Timestamp::from(
                    UNIX_EPOCH + Duration::from_nanos(o.mtime.max(0) as u64),
                )),
                e_tag: o.etag.clone().map(ETag::Strong),
                ..Default::default()
            })
            .collect();

        Ok(S3Response::new(ListObjectsOutput {
            name: Some(input.bucket),
            prefix: input.prefix,
            marker: input.marker,
            max_keys: input.max_keys,
            is_truncated: Some(result.is_truncated),
            next_marker: if result.is_truncated {
                Some(s3s::dto::NextMarker::from(result.next_marker))
            } else {
                None
            },
            contents: if contents.is_empty() {
                None
            } else {
                Some(contents)
            },
            common_prefixes: prefixes_to_dto(&result.common_prefixes),
            ..Default::default()
        }))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let input = req.input;
        let bucket = input.bucket.as_str().to_string();
        let start_after = input
            .start_after
            .as_ref()
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();
        let continuation = input
            .continuation_token
            .as_ref()
            .map(|t| t.as_str().to_string())
            .unwrap_or_default();
        let query = ListQuery {
            prefix: input
                .prefix
                .as_ref()
                .map(|p| p.as_str().to_string())
                .unwrap_or_default(),
            delimiter: input.delimiter.as_ref().map(|d| d.as_str().to_string()),
            start_after: if continuation.is_empty() {
                start_after
            } else {
                continuation
            },
            max_keys: input
                .max_keys
                .map(|m| usize::try_from(m).unwrap_or(1000))
                .unwrap_or(1000),
            hide_dir_objects: self.opts.hide_dir_objects,
        };
        let result = self.list_keys(&bucket, query).await?;

        let contents: Vec<Object> = result
            .objects
            .iter()
            .map(|o| Object {
                key: Some(o.key.clone()),
                size: Some(if o.is_dir_object { 0 } else { o.size as i64 }),
                last_modified: Some(Timestamp::from(
                    UNIX_EPOCH + Duration::from_nanos(o.mtime.max(0) as u64),
                )),
                e_tag: o.etag.clone().map(ETag::Strong),
                ..Default::default()
            })
            .collect();
        let count = contents.len() + result.common_prefixes.len();

        Ok(S3Response::new(ListObjectsV2Output {
            name: Some(input.bucket),
            prefix: input.prefix,
            max_keys: input.max_keys,
            key_count: Some(s3s::dto::KeyCount::from(count as i32)),
            continuation_token: input.continuation_token,
            start_after: input.start_after,
            is_truncated: Some(result.is_truncated),
            next_continuation_token: if result.is_truncated {
                Some(NextToken::from(result.next_marker))
            } else {
                None
            },
            contents: if contents.is_empty() {
                None
            } else {
                Some(contents)
            },
            common_prefixes: prefixes_to_dto(&result.common_prefixes),
            ..Default::default()
        }))
    }

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let input = req.input;
        let bucket = input.bucket.as_str().to_string();
        let key = input.key.as_str().to_string();
        if key.is_empty() || key.ends_with('/') {
            return Err(s3_error!(
                InvalidArgument,
                "multipart key must not be empty or end with '/'"
            ));
        }
        let bucket_lock = self.bucket_lock_for(&bucket);
        let _bucket_guard = bucket_lock.lock().await;
        self.stat_bucket_root(&bucket).await?;
        path::object_path(&self.opts.bucket_mode, &bucket, &key).map_err(Self::path_err)?;

        let upload_id = new_upload_id();
        let dir = multipart::upload_dir(&upload_id);
        self.vfs.mkdir_p(&dir).await.map_err(Self::err)?;

        let meta = UploadMeta {
            bucket: bucket.clone(),
            key: key.clone(),
            content_type: input
                .content_type
                .as_ref()
                .map(|ct| ct.as_str().to_string()),
            metadata: input.metadata.as_ref().map(|m| {
                m.iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect()
            }),
            initiated: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
        };
        let target_path = UploadMeta::target_path(&upload_id);
        let ino = self
            .vfs
            .create_file(&target_path)
            .await
            .map_err(Self::err)?;
        let attr = self.vfs.stat(&target_path).await.map_err(Self::err)?;
        let guard = self
            .vfs
            .open_guard(ino, attr, false, true)
            .await
            .map_err(Self::err)?;
        guard
            .write(0, serde_json::to_vec(&meta).unwrap().as_slice())
            .await
            .map_err(Self::err)?;
        guard.close().await.map_err(Self::err)?;

        Ok(S3Response::new(CreateMultipartUploadOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            upload_id: Some(MultipartUploadId::from(upload_id)),
            ..Default::default()
        }))
    }

    async fn upload_part(
        &self,
        mut req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let upload_id = req.input.upload_id.clone();
        let part_number = req.input.part_number;
        if !valid_part_number(part_number) {
            return Err(s3_error!(
                InvalidArgument,
                "part number must be between 1 and {MAX_MULTIPART_PART_NUMBER}"
            ));
        }
        let bucket = req.input.bucket.as_str().to_string();
        let key = req.input.key.as_str().to_string();

        {
            let lock = self.lock_for(&bucket, &key);
            let _guard = lock.lock().await;
            self.read_matching_upload_meta(&upload_id, &bucket, &key)
                .await?;
        }

        let dst = multipart::part_path(&upload_id, i64::from(part_number));
        let tmp = self.reserve_staging_path();
        let body = req
            .input
            .body
            .take()
            .ok_or_else(|| s3_error!(InvalidArgument, "missing body"))?;
        let mut body = body;
        let (_size, etag) = self.write_blob(tmp.path(), &mut body).await?;

        let publish_result: S3Result<()> = async {
            let lock = self.lock_for(&bucket, &key);
            let _guard = lock.lock().await;
            self.read_matching_upload_meta(&upload_id, &bucket, &key)
                .await?;
            let attr = self.vfs.stat(&tmp).await.map_err(Self::err)?;
            self.set_xattr(attr.ino, XATTR_ETAG, &etag).await?;
            self.vfs.rename(&tmp, &dst).await.map_err(Self::err)
        }
        .await;
        if let Err(e) = publish_result {
            let _ = self.vfs.unlink(&tmp).await;
            return Err(e);
        }

        Ok(S3Response::new(UploadPartOutput {
            e_tag: Some(ETag::Strong(etag)),
            ..Default::default()
        }))
    }

    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        let input = req.input;
        let upload_id = input.upload_id.clone();
        let bucket = input.bucket.as_str().to_string();
        let key = input.key.as_str().to_string();
        let max_parts = input.max_parts.unwrap_or(1000);
        if !(0..=1000).contains(&max_parts) {
            return Err(s3_error!(
                InvalidArgument,
                "max-parts must be between 0 and 1000"
            ));
        }
        let part_number_marker = input.part_number_marker.unwrap_or(0);
        if !(0..=MAX_MULTIPART_PART_NUMBER).contains(&part_number_marker) {
            return Err(s3_error!(
                InvalidArgument,
                "part-number-marker must be between 0 and {MAX_MULTIPART_PART_NUMBER}"
            ));
        }
        let lock = self.lock_for(&bucket, &key);
        let _guard = lock.lock().await;
        let meta = self
            .read_matching_upload_meta(&upload_id, &bucket, &key)
            .await?;

        let dir = multipart::upload_dir(&upload_id);
        let mut parts: Vec<(i32, u64, i64, Option<String>)> = Vec::new();
        for entry in self.read_dir_entries(&dir).await? {
            if let Some(n) = parse_part_name(&entry.name)
                && let Ok(n) = i32::try_from(n)
                && valid_part_number(n)
                && let Some(attr) = self.vfs.stat_ino(entry.ino).await
            {
                let etag = self.read_etag(entry.ino).await;
                parts.push((n, attr.size, attr.mtime, etag));
            }
        }
        parts.sort_by_key(|p| p.0);
        parts.retain(|part| part.0 > part_number_marker);
        let is_truncated = max_parts > 0 && parts.len() > max_parts as usize;
        parts.truncate(max_parts as usize);
        let next_part_number_marker = if is_truncated {
            Some(
                parts
                    .last()
                    .map(|part| part.0)
                    .unwrap_or(part_number_marker),
            )
        } else {
            None
        };

        Ok(S3Response::new(ListPartsOutput {
            bucket: Some(input.bucket),
            key: Some(ObjectKey::from(meta.key)),
            upload_id: Some(MultipartUploadId::from(upload_id)),
            max_parts: Some(max_parts),
            part_number_marker: Some(part_number_marker),
            is_truncated: Some(is_truncated),
            next_part_number_marker,
            parts: Some(
                parts
                    .into_iter()
                    .map(|(n, size, mtime, etag)| Part {
                        part_number: Some(n),
                        size: Some(size as i64),
                        last_modified: Some(Timestamp::from(
                            UNIX_EPOCH + Duration::from_nanos(mtime.max(0) as u64),
                        )),
                        e_tag: etag.map(ETag::Strong),
                        ..Default::default()
                    })
                    .collect(),
            ),
            ..Default::default()
        }))
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let input = req.input;
        if input.if_match.is_some() || input.if_none_match.is_some() {
            return Err(s3_error!(
                NotImplemented,
                "conditional multipart completion is not supported"
            ));
        }
        let bucket = input.bucket.as_str().to_string();
        let key = input.key.as_str().to_string();
        let upload_id = input.upload_id.clone();

        let completed = input
            .multipart_upload
            .and_then(|m| m.parts)
            .ok_or_else(|| s3_error!(InvalidArgument, "missing parts list"))?;

        let lock = self.lock_for(&bucket, &key);
        let _guard = lock.lock().await;
        let meta = self
            .read_matching_upload_meta(&upload_id, &bucket, &key)
            .await?;

        // Validate part ordering and etags.
        let mut part_etags: Vec<String> = Vec::with_capacity(completed.len());
        let mut part_attrs: Vec<FileAttr> = Vec::with_capacity(completed.len());
        let mut previous_part_number = None;
        for part in &completed {
            let n = part.part_number.unwrap_or(0);
            if !valid_part_number(n) {
                return Err(s3_error!(
                    InvalidArgument,
                    "part number must be between 1 and {MAX_MULTIPART_PART_NUMBER}"
                ));
            }
            if previous_part_number.is_some_and(|previous| n <= previous) {
                return Err(s3_error!(
                    InvalidPartOrder,
                    "parts must be in strictly ascending order"
                ));
            }
            previous_part_number = Some(n);
            let src = multipart::part_path(&upload_id, i64::from(n));
            let attr = match self.vfs.stat(&src).await {
                Ok(a) => a,
                Err(_) => return Err(s3_error!(InvalidPart, "part {n} not uploaded")),
            };
            // Every part except the last must be at least 5 MiB.
            let is_last = std::ptr::eq(part, completed.last().unwrap());
            if !is_last && attr.size < 5 * 1024 * 1024 {
                return Err(s3_error!(EntityTooSmall, "part {n} is smaller than 5 MiB"));
            }
            let stored = self
                .read_etag(attr.ino)
                .await
                .ok_or_else(|| s3_error!(InvalidPart, "part {n} has no etag"))?;
            let requested = part
                .e_tag
                .as_ref()
                .ok_or_else(|| s3_error!(InvalidPart, "part {n} has no etag"))?;
            if requested.value() != stored {
                return Err(s3_error!(InvalidPart, "etag mismatch on part {n}"));
            }
            part_etags.push(stored);
            part_attrs.push(attr);
        }

        // Concatenate parts into a staging file, then publish atomically.
        let tmp = self.reserve_staging_path();
        if self.vfs.exists(tmp.path()).await {
            self.vfs.unlink(tmp.path()).await.map_err(Self::err)?;
        }
        let tmp_ino = self.vfs.create_file(tmp.path()).await.map_err(Self::err)?;
        let target =
            path::object_path(&self.opts.bucket_mode, &bucket, &key).map_err(Self::path_err)?;
        let etag = multipart_etag(&part_etags);
        let object_meta = ObjectMeta {
            content_type: meta.content_type.clone(),
            metadata: meta.metadata.clone(),
        };

        let bucket_lock = self.bucket_lock_for(&bucket);
        let _bucket_guard = bucket_lock.lock().await;
        let publish_result: S3Result<()> = async {
            self.stat_bucket_root(&bucket).await?;
            let mut offset = 0;
            for attr in &part_attrs {
                self.copy_range_exact(attr.ino, 0, tmp_ino, offset, attr.size)
                    .await?;
                offset += attr.size;
            }
            self.set_xattr(tmp_ino, XATTR_ETAG, &etag).await?;
            self.set_xattr(
                tmp_ino,
                XATTR_META,
                &serde_json::to_string(&object_meta).unwrap(),
            )
            .await?;

            if let Some(parent) = std::path::Path::new(&target).parent() {
                let parent = parent.to_string_lossy().to_string();
                if !parent.is_empty() {
                    self.vfs.mkdir_p(&parent).await.map_err(Self::err)?;
                }
            }
            if self.vfs.exists(&target).await {
                let attr = self.vfs.stat(&target).await.map_err(Self::err)?;
                if attr.kind == FileType::Dir {
                    return Err(s3_error!(InvalidArgument, "target is a directory"));
                }
            }
            self.vfs.rename(&tmp, &target).await.map_err(Self::err)
        }
        .await;
        if let Err(e) = publish_result {
            let _ = self.vfs.unlink(&tmp).await;
            return Err(e);
        };

        // Drop the upload state.
        let upload_dir = multipart::upload_dir(&upload_id);
        let _ = super::remove_dir_all_rec(&self.vfs, &upload_dir).await;

        let location = format!("/{bucket}/{key}");
        Ok(S3Response::new(CompleteMultipartUploadOutput {
            bucket: Some(BucketName::from(bucket)),
            key: Some(ObjectKey::from(key)),
            e_tag: Some(ETag::Strong(etag)),
            location: Some(Location::from(location)),
            ..Default::default()
        }))
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let input = req.input;
        if input.if_match_initiated_time.is_some() {
            return Err(s3_error!(
                NotImplemented,
                "conditional multipart abort is not supported"
            ));
        }
        let upload_id = input.upload_id.clone();
        let bucket = input.bucket.as_str().to_string();
        let key = input.key.as_str().to_string();
        let lock = self.lock_for(&bucket, &key);
        let _guard = lock.lock().await;
        self.read_matching_upload_meta(&upload_id, &bucket, &key)
            .await?;
        let dir = multipart::upload_dir(&upload_id);
        super::remove_dir_all_rec(&self.vfs, &dir)
            .await
            .map_err(Self::err)?;
        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }

    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        let input = req.input;
        let bucket = input.bucket.as_str().to_string();
        let prefix = input
            .prefix
            .as_ref()
            .map(|p| p.as_str().to_string())
            .unwrap_or_default();
        let delimiter = input
            .delimiter
            .as_ref()
            .map(|d| d.as_str().to_string())
            .filter(|d| !d.is_empty());
        let key_marker = input
            .key_marker
            .as_ref()
            .map(|marker| marker.as_str().to_string())
            .unwrap_or_default();
        let upload_id_marker = input
            .upload_id_marker
            .as_ref()
            .map(|marker| marker.as_str().to_string())
            .unwrap_or_default();
        let max_uploads = input.max_uploads.unwrap_or(1000);
        if !(1..=1000).contains(&max_uploads) {
            return Err(s3_error!(
                InvalidArgument,
                "max-uploads must be between 1 and 1000"
            ));
        }
        path::validate_key_namespace(&self.opts.bucket_mode, &prefix).map_err(Self::path_err)?;
        self.stat_bucket_root(&bucket).await?;

        let mut uploads = Vec::new();
        let mut common_prefixes = std::collections::BTreeSet::new();
        for hh in self.read_dir_entries(&multipart::uploads_dir()).await? {
            let hh_dir = format!("{}/{}", multipart::uploads_dir(), hh.name);
            for upload in self.read_dir_entries(&hh_dir).await? {
                let upload_id = upload.name;
                let Ok(meta) = self.read_upload_meta(&upload_id).await else {
                    continue;
                };
                if meta.bucket != bucket || !meta.key.starts_with(&prefix) {
                    continue;
                }
                if let Some(delimiter) = delimiter.as_deref()
                    && let Some(relative) = meta.key.strip_prefix(&prefix)
                    && let Some(index) = relative.find(delimiter)
                {
                    let end = prefix.len() + index + delimiter.len();
                    let common_prefix = meta.key[..end].to_string();
                    if key_marker.is_empty() || common_prefix > key_marker {
                        common_prefixes.insert(common_prefix);
                    }
                    continue;
                }
                let after_marker = key_marker.is_empty()
                    || meta.key > key_marker
                    || (meta.key == key_marker
                        && !upload_id_marker.is_empty()
                        && upload_id > upload_id_marker);
                if after_marker {
                    uploads.push((meta.key, upload_id, meta.initiated));
                }
            }
        }
        uploads.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));

        let mut entries = uploads
            .into_iter()
            .map(|upload| (upload.0.clone(), Some(upload)))
            .chain(common_prefixes.into_iter().map(|prefix| (prefix, None)))
            .collect::<Vec<_>>();
        entries.sort_by(|a, b| {
            a.0.cmp(&b.0).then_with(|| match (&a.1, &b.1) {
                (Some(a), Some(b)) => a.1.cmp(&b.1),
                (None, Some(_)) => std::cmp::Ordering::Less,
                (Some(_), None) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            })
        });
        let is_truncated = entries.len() > max_uploads as usize;
        entries.truncate(max_uploads as usize);

        let (next_key_marker, next_upload_id_marker) = if is_truncated {
            entries
                .last()
                .map(|entry| {
                    (
                        Some(NextKeyMarker::from(entry.0.clone())),
                        entry
                            .1
                            .as_ref()
                            .map(|upload| NextUploadIdMarker::from(upload.1.clone())),
                    )
                })
                .unwrap_or_default()
        } else {
            (None, None)
        };
        let mut output_uploads = Vec::new();
        let mut output_prefixes = Vec::new();
        for (entry_key, upload) in entries {
            if let Some((key, upload_id, initiated)) = upload {
                output_uploads.push(MultipartUpload {
                    key: Some(ObjectKey::from(key)),
                    upload_id: Some(MultipartUploadId::from(upload_id)),
                    initiated: Some(Timestamp::from(
                        UNIX_EPOCH + Duration::from_secs(initiated.max(0) as u64),
                    )),
                    storage_class: Some(StorageClass::from_static(StorageClass::STANDARD)),
                    ..Default::default()
                });
            } else {
                output_prefixes.push(CommonPrefix {
                    prefix: Some(Prefix::from(entry_key)),
                });
            }
        }

        Ok(S3Response::new(ListMultipartUploadsOutput {
            bucket: Some(input.bucket),
            delimiter: input.delimiter,
            prefix: input.prefix,
            key_marker: input.key_marker,
            upload_id_marker: input.upload_id_marker,
            max_uploads: Some(max_uploads),
            is_truncated: Some(is_truncated),
            next_key_marker,
            next_upload_id_marker,
            common_prefixes: if output_prefixes.is_empty() {
                None
            } else {
                Some(output_prefixes)
            },
            uploads: if output_uploads.is_empty() {
                None
            } else {
                Some(output_uploads)
            },
            ..Default::default()
        }))
    }
}

fn prefixes_to_dto(prefixes: &std::collections::BTreeSet<String>) -> Option<Vec<CommonPrefix>> {
    if prefixes.is_empty() {
        return None;
    }
    Some(
        prefixes
            .iter()
            .map(|p| CommonPrefix {
                prefix: Some(s3s::dto::Prefix::from(p.as_str())),
            })
            .collect(),
    )
}

#[cfg(test)]
#[path = "tests/conditions.rs"]
mod conditions_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_lock_shards_are_bounded_and_stable() {
        let shard = key_lock_shard("bucket", "key");
        assert!(shard < KEY_LOCK_SHARDS);
        assert_eq!(shard, key_lock_shard("bucket", "key"));
        assert_eq!(
            key_lock_shard("bucket", "dir"),
            key_lock_shard("bucket", "dir//")
        );
    }

    #[test]
    fn bucket_lock_shards_are_bounded_and_stable() {
        let shard = bucket_lock_shard("bucket");
        assert!(shard < BUCKET_LOCK_SHARDS);
        assert_eq!(shard, bucket_lock_shard("bucket"));
    }

    #[test]
    fn multipart_part_numbers_are_bounded() {
        assert!(!valid_part_number(0));
        assert!(valid_part_number(1));
        assert!(valid_part_number(10_000));
        assert!(!valid_part_number(10_001));
    }
}
