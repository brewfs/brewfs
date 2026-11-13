//! Frozen native entity lower: individual authenticated Redis metadata rows and
//! immutable raw S3 objects. This does not use the packed graph or legacy layers.
//! A private source tree is captured before mount; no live native writer is adopted.

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Read;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::backend::{
    CommitOutcome, EntityBackend, EntityPlan, RowCheck, RowWrite, TransactionAuthority,
};
use super::codec::{decode, encode};
use super::model::{ChunkMap, ChunkSource, EntityDentry, EntityInode, EntityKind, LowerBinding};
use super::resolver::{
    ImmutableLower, ImmutableLowerData, MAX_DIRECTORY_ENTRIES, validate_directory_page,
};
use super::{EntityError, Result};
use crate::cadapter::client::ObjectBackend;

const MAX_ROWS: usize = 32768;
const MAX_MANIFEST_BYTES: usize = 4 << 20;

#[cfg(test)]
#[path = "native_lower_tests.rs"]
mod tests;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NativeManifest {
    format: String,
    volume: Uuid,
    chunk_size: u64,
    max_ino: i64,
    rows: BTreeMap<String, [u8; 32]>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NativeObject {
    size: u64,
    sha256: [u8; 32],
}

pub struct NativeLower<B, O> {
    backend: Arc<B>,
    objects: Arc<O>,
    manifest: NativeManifest,
    binding: LowerBinding,
}

fn error(e: impl std::fmt::Display) -> EntityError {
    EntityError::Backend(format!("native entity lower: {e}"))
}
fn row_key(volume: Uuid, digest: &[u8; 32]) -> Vec<u8> {
    format!(
        "{{brewfs-entity:{volume}}}/native/row/{}",
        hex::encode(digest)
    )
    .into_bytes()
}
fn object_key(volume: Uuid, digest: &[u8; 32]) -> String {
    format!("entity-native-v1/{volume}/data/{}", hex::encode(digest))
}

async fn bounded_object<O: ObjectBackend>(
    objects: &O,
    key: &str,
    maximum: usize,
) -> Result<Vec<u8>> {
    use futures::StreamExt;
    let mut stream = objects
        .get_object_range_stream(key, 0, maximum as u64 + 1)
        .await
        .map_err(error)?;
    let mut bytes = Vec::new();
    while let Some(part) = stream.next().await {
        let part = part.map_err(error)?;
        if bytes.len().saturating_add(part.len()) > maximum {
            return Err(EntityError::Corrupt("native object exceeds bound".into()));
        }
        bytes.extend_from_slice(&part);
    }
    Ok(bytes)
}

async fn put_same<O: ObjectBackend>(objects: &O, key: &str, bytes: &[u8]) -> Result<()> {
    match objects.get_object_size_bounded(key).await.map_err(error)? {
        Some(size) if size == bytes.len() as u64 => {
            if bounded_object(objects, key, bytes.len()).await? != bytes {
                return Err(EntityError::Corrupt(
                    "native immutable object collision".into(),
                ));
            }
            Ok(())
        }
        Some(_) => Err(EntityError::Corrupt("native object size collision".into())),
        None => objects
            .put_object_create_only_durable(key, bytes)
            .await
            .map_err(error),
    }
}

impl<B: EntityBackend + 'static, O: ObjectBackend + 'static> NativeLower<B, O> {
    pub async fn open(backend: Arc<B>, objects: Arc<O>, volume: Uuid, key: &str) -> Result<Self> {
        let digest: [u8; 32] = hex::decode(key.rsplit('/').next().unwrap_or_default())
            .ok()
            .and_then(|v| v.try_into().ok())
            .ok_or_else(|| {
                EntityError::Invalid("native manifest key must end in SHA-256".into())
            })?;
        let bytes = bounded_object(objects.as_ref(), key, MAX_MANIFEST_BYTES).await?;
        if <[u8; 32]>::from(Sha256::digest(&bytes)) != digest {
            return Err(EntityError::Corrupt("native manifest digest".into()));
        }
        let manifest: NativeManifest = decode(&bytes)?;
        if manifest.format != "entity-native-lower-v1"
            || manifest.volume != volume
            || manifest.max_ino < 1
            || manifest.rows.len() > MAX_ROWS
            || manifest.chunk_size == 0
            || manifest.chunk_size > super::data::MAX_IO_BYTES as u64
        {
            return Err(EntityError::Corrupt("invalid native manifest".into()));
        }
        let binding = LowerBinding {
            manifest_key: key.as_bytes().to_vec(),
            manifest_digest: digest,
            root_ino: 1,
            max_ino: manifest.max_ino,
        };
        binding.validate()?;
        let lower = Self {
            backend,
            objects,
            manifest,
            binding,
        };
        if lower
            .inode(1)
            .await?
            .is_none_or(|inode| inode.kind != EntityKind::Directory)
        {
            return Err(EntityError::Corrupt("native root missing".into()));
        }
        Ok(lower)
    }

    async fn row<T: serde::de::DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        let Some(digest) = self.manifest.rows.get(key) else {
            return Ok(None);
        };
        let snapshot = self
            .backend
            .read(&[row_key(self.manifest.volume, digest)])
            .await?;
        let bytes = snapshot.rows[0]
            .as_deref()
            .ok_or_else(|| EntityError::Corrupt("native metadata row missing".into()))?;
        if <[u8; 32]>::from(Sha256::digest(bytes)) != *digest {
            return Err(EntityError::Corrupt(
                "native metadata authentication".into(),
            ));
        }
        Ok(Some(decode(bytes)?))
    }

    /// Capture a private, quiescent directory (including a drained native mount).
    /// Stable fingerprints are rechecked after all data and rows have been copied.
    /// Snapshot objects use a separate domain and remain retained; legacy GC has
    /// no authority to remove them. Entity lifecycle registers whole-lower owners.
    pub async fn capture(
        backend: Arc<B>,
        objects: Arc<O>,
        volume: Uuid,
        source: &Path,
        chunk_size: u64,
    ) -> Result<Self> {
        if chunk_size == 0 || chunk_size > super::data::MAX_IO_BYTES as u64 {
            return Err(EntityError::Invalid("native chunk size".into()));
        }
        let root = std::fs::canonicalize(source).map_err(error)?;
        if !root.is_dir() {
            return Err(EntityError::NotDirectory);
        }
        let mut manifest = NativeManifest {
            format: "entity-native-lower-v1".into(),
            volume,
            chunk_size,
            max_ino: 1,
            rows: BTreeMap::new(),
        };
        let mut pending = vec![(root.clone(), 1, None)];
        let mut identities = BTreeMap::new();
        let root_meta = std::fs::symlink_metadata(&root).map_err(error)?;
        identities.insert((root_meta.dev(), root_meta.ino()), 1);
        let mut fingerprints = Vec::new();
        let mut inodes = BTreeMap::new();
        let mut links = BTreeMap::<i64, u32>::new();
        while let Some((path, ino, parent)) = pending.pop() {
            let meta = std::fs::symlink_metadata(&path).map_err(error)?;
            if identities.get(&(meta.dev(), meta.ino())) != Some(&ino) {
                return Err(EntityError::Conflict);
            }
            fingerprints.push((path.clone(), fingerprint(&meta)));
            let kind = kind(meta.mode())?;
            let target = if kind == EntityKind::Symlink {
                Some(
                    std::fs::read_link(&path)
                        .map_err(error)?
                        .as_os_str()
                        .as_bytes()
                        .to_vec(),
                )
            } else {
                None
            };
            let inode = EntityInode {
                ino,
                kind,
                size: if kind == EntityKind::File {
                    meta.len()
                } else {
                    target.as_ref().map_or(0, |t| t.len() as u64)
                },
                mode: meta.mode() & 0o7777,
                uid: meta.uid(),
                gid: meta.gid(),
                rdev: u32::try_from(meta.rdev()).map_err(|_| EntityError::Overflow)?,
                nlink: u32::try_from(meta.nlink()).map_err(|_| EntityError::Overflow)?,
                atime_ns: ns(meta.atime(), meta.atime_nsec())?,
                mtime_ns: ns(meta.mtime(), meta.mtime_nsec())?,
                ctime_ns: ns(meta.ctime(), meta.ctime_nsec())?,
                symlink_target: target,
                parent_hint: parent,
                lower_visible_size: if kind == EntityKind::File {
                    meta.len()
                } else {
                    0
                },
                data_version: 0,
            };
            inode.validate()?;
            inodes.insert(ino, inode);
            for (name, value) in xattrs(&path)? {
                store_row(
                    backend.as_ref(),
                    &mut manifest,
                    format!("xattr/{ino}/{}", hex::encode(name)),
                    &value,
                )
                .await?;
            }
            if kind == EntityKind::Directory {
                let mut entries = Vec::new();
                for entry in std::fs::read_dir(&path).map_err(error)? {
                    let entry = entry.map_err(error)?;
                    if entries.len() == MAX_DIRECTORY_ENTRIES {
                        return Err(EntityError::Invalid(
                            "native capture directory exceeds 4096 names".into(),
                        ));
                    }
                    let child = std::fs::symlink_metadata(entry.path()).map_err(error)?;
                    let identity = (child.dev(), child.ino());
                    let child_ino = match identities.get(&identity) {
                        Some(_) if kind_of(&child)? == EntityKind::Directory => {
                            return Err(EntityError::Invalid(
                                "native capture rejects directory aliases and cycles".into(),
                            ));
                        }
                        Some(ino) => *ino,
                        None => {
                            if identities.len() >= MAX_ROWS {
                                return Err(EntityError::Invalid("native inode quota".into()));
                            }
                            manifest.max_ino += 1;
                            let next = manifest.max_ino;
                            identities.insert(identity, next);
                            pending.push((entry.path(), next, Some(ino)));
                            next
                        }
                    };
                    if kind_of(&child)? == EntityKind::Directory {
                        *links.entry(ino).or_default() += 1;
                    } else {
                        *links.entry(child_ino).or_default() += 1;
                    }
                    let name = entry.file_name().as_bytes().to_vec();
                    super::model::validate_name(&name)?;
                    entries.push((
                        name,
                        EntityDentry::Present {
                            ino: child_ino,
                            kind: kind_of(&child)?,
                        },
                    ));
                }
                entries.sort_by(|a, b| a.0.cmp(&b.0));
                store_row(
                    backend.as_ref(),
                    &mut manifest,
                    format!("directory/{ino}"),
                    &entries,
                )
                .await?;
            } else if kind == EntityKind::File {
                let mut file = OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_NOFOLLOW)
                    .open(&path)
                    .map_err(error)?;
                if fingerprint(&file.metadata().map_err(error)?) != fingerprint(&meta) {
                    return Err(EntityError::Conflict);
                }
                let mut left = meta.len();
                let mut index = 0;
                while left > 0 {
                    let mut bytes = vec![0; left.min(chunk_size) as usize];
                    file.read_exact(&mut bytes).map_err(error)?;
                    let data = NativeObject {
                        size: bytes.len() as u64,
                        sha256: Sha256::digest(&bytes).into(),
                    };
                    put_same(objects.as_ref(), &object_key(volume, &data.sha256), &bytes).await?;
                    store_row(
                        backend.as_ref(),
                        &mut manifest,
                        format!("chunk/{ino}/{index}"),
                        &data,
                    )
                    .await?;
                    left -= bytes.len() as u64;
                    index += 1;
                }
                if fingerprint(&file.metadata().map_err(error)?) != fingerprint(&meta) {
                    return Err(EntityError::Conflict);
                }
            }
        }
        for (path, expected) in fingerprints {
            if fingerprint(&std::fs::symlink_metadata(path).map_err(error)?) != expected {
                return Err(EntityError::Conflict);
            }
        }
        // Links outside the captured tree are not part of the frozen namespace.
        for (ino, mut inode) in inodes {
            inode.nlink = links.get(&ino).copied().unwrap_or(0)
                + if inode.kind == EntityKind::Directory {
                    2
                } else {
                    0
                };
            store_row(
                backend.as_ref(),
                &mut manifest,
                format!("inode/{ino}"),
                &inode,
            )
            .await?;
        }
        let bytes = encode(&manifest)?;
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(EntityError::Invalid("native manifest bound".into()));
        }
        let key = format!(
            "entity-native-v1/{volume}/manifest/{}",
            hex::encode(Sha256::digest(&bytes))
        );
        put_same(objects.as_ref(), &key, &bytes).await?;
        Self::open(backend, objects, volume, &key).await
    }
}

async fn store_row<B: EntityBackend, T: Serialize>(
    backend: &B,
    manifest: &mut NativeManifest,
    name: String,
    row: &T,
) -> Result<()> {
    if manifest.rows.len() >= MAX_ROWS {
        return Err(EntityError::Invalid("native snapshot row quota".into()));
    }
    let bytes = encode(row)?;
    let digest = Sha256::digest(&bytes).into();
    let key = row_key(manifest.volume, &digest);
    let snapshot = backend.read(std::slice::from_ref(&key)).await?;
    if snapshot.rows[0].as_ref().is_some_and(|old| old != &bytes) {
        return Err(EntityError::Corrupt(
            "native metadata hash collision".into(),
        ));
    }
    let authority = TransactionAuthority::from_validated(
        vec![RowCheck {
            key: key.clone(),
            expected: snapshot.rows[0].clone(),
        }],
        None,
    )?;
    if backend
        .commit(
            &authority,
            &EntityPlan {
                checks: vec![],
                writes: vec![RowWrite::Put { key, value: bytes }],
            },
        )
        .await?
        != CommitOutcome::Committed
    {
        return Err(EntityError::Conflict);
    }
    manifest.rows.insert(name, digest);
    Ok(())
}

#[async_trait]
impl<B: EntityBackend + 'static, O: ObjectBackend + 'static> ImmutableLower for NativeLower<B, O> {
    fn binding(&self) -> &LowerBinding {
        &self.binding
    }
    async fn inode(&self, ino: i64) -> Result<Option<EntityInode>> {
        let row: Option<EntityInode> = self.row(&format!("inode/{ino}")).await?;
        if let Some(row) = &row {
            row.validate()?;
            if row.ino != ino {
                return Err(EntityError::Corrupt("native inode identity".into()));
            }
        }
        Ok(row)
    }
    async fn dentry(&self, parent: i64, name: &[u8]) -> Result<Option<EntityDentry>> {
        Ok(self
            .readdir(parent)
            .await?
            .into_iter()
            .find(|(candidate, _)| candidate == name)
            .map(|(_, entry)| entry))
    }
    async fn xattr(&self, ino: i64, name: &[u8]) -> Result<Option<Vec<u8>>> {
        self.row(&format!("xattr/{ino}/{}", hex::encode(name)))
            .await
    }
    async fn chunk(&self, ino: i64, index: u64, chunk_size: u64) -> Result<Option<ChunkMap>> {
        let inode = self.inode(ino).await?.ok_or(EntityError::NotFound)?;
        let offset = index.checked_mul(chunk_size).ok_or(EntityError::Overflow)?;
        let length = inode.size.saturating_sub(offset).min(chunk_size);
        if length == 0 {
            return Ok(None);
        }
        Ok(Some(ChunkMap::holes(chunk_size)?.replace(
            0,
            length,
            ChunkSource::LowerRange {
                binding_digest: self.binding.manifest_digest,
                ino,
                file_offset: offset,
            },
        )?))
    }
    async fn readdir(&self, ino: i64) -> Result<Vec<(Vec<u8>, EntityDentry)>> {
        let rows: Vec<(Vec<u8>, EntityDentry)> = self
            .row(&format!("directory/{ino}"))
            .await?
            .ok_or(EntityError::NotDirectory)?;
        if rows.len() > MAX_DIRECTORY_ENTRIES {
            return Err(EntityError::Corrupt("native directory bound".into()));
        }
        Ok(rows)
    }
    async fn readdir_page(
        &self,
        ino: i64,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, EntityDentry)>> {
        validate_directory_page(limit)?;
        Ok(self
            .readdir(ino)
            .await?
            .into_iter()
            .skip(usize::try_from(offset).map_err(|_| EntityError::Overflow)?)
            .take(limit)
            .collect())
    }
}

#[async_trait]
impl<B: EntityBackend + 'static, O: ObjectBackend + 'static> ImmutableLowerData
    for NativeLower<B, O>
{
    async fn read_range(&self, ino: i64, offset: u64, length: usize) -> Result<Vec<u8>> {
        if length > super::data::MAX_IO_BYTES {
            return Err(EntityError::Invalid("native read bound".into()));
        }
        let inode = self.inode(ino).await?.ok_or(EntityError::NotFound)?;
        if inode.kind != EntityKind::File
            || offset
                .checked_add(length as u64)
                .is_none_or(|end| end > inode.size)
        {
            return Err(EntityError::Invalid("native read outside file".into()));
        }
        let mut output = Vec::with_capacity(length);
        while output.len() < length {
            let position = offset + output.len() as u64;
            let index = position / self.manifest.chunk_size;
            let row: NativeObject = self
                .row(&format!("chunk/{ino}/{index}"))
                .await?
                .ok_or_else(|| EntityError::Corrupt("native data locator missing".into()))?;
            if row.size == 0 || row.size > self.manifest.chunk_size {
                return Err(EntityError::Corrupt("native locator size".into()));
            }
            let bytes = bounded_object(
                self.objects.as_ref(),
                &object_key(self.manifest.volume, &row.sha256),
                row.size as usize,
            )
            .await?;
            if bytes.len() as u64 != row.size
                || <[u8; 32]>::from(Sha256::digest(&bytes)) != row.sha256
            {
                return Err(EntityError::Corrupt("native data authentication".into()));
            }
            let start = (position % self.manifest.chunk_size) as usize;
            let available = bytes
                .get(start..)
                .filter(|v| !v.is_empty())
                .ok_or_else(|| EntityError::Corrupt("native short extent".into()))?;
            output.extend_from_slice(&available[..available.len().min(length - output.len())]);
        }
        Ok(output)
    }
}

fn kind_of(meta: &std::fs::Metadata) -> Result<EntityKind> {
    kind(meta.mode())
}
fn kind(mode: u32) -> Result<EntityKind> {
    Ok(match mode & libc::S_IFMT {
        libc::S_IFREG => EntityKind::File,
        libc::S_IFDIR => EntityKind::Directory,
        libc::S_IFLNK => EntityKind::Symlink,
        libc::S_IFIFO => EntityKind::Fifo,
        libc::S_IFSOCK => EntityKind::Socket,
        libc::S_IFCHR => EntityKind::CharDevice,
        libc::S_IFBLK => EntityKind::BlockDevice,
        _ => return Err(EntityError::Invalid("native inode kind".into())),
    })
}
fn ns(sec: i64, nano: i64) -> Result<i64> {
    sec.checked_mul(1_000_000_000)
        .and_then(|v| v.checked_add(nano))
        .ok_or(EntityError::Overflow)
}
fn fingerprint(meta: &std::fs::Metadata) -> (u64, u64, u64, i64, i64, i64, i64) {
    (
        meta.dev(),
        meta.ino(),
        meta.len(),
        meta.mtime(),
        meta.mtime_nsec(),
        meta.ctime(),
        meta.ctime_nsec(),
    )
}

fn xattrs(path: &Path) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(error)?;
    let mut names = vec![0u8; 64 << 10];
    // SAFETY: valid C path and writable bounded byte buffer; l* does not follow symlinks.
    let count = unsafe { libc::llistxattr(path.as_ptr(), names.as_mut_ptr().cast(), names.len()) };
    if count < 0 {
        return Err(error(std::io::Error::last_os_error()));
    }
    let mut result = Vec::new();
    for name in names[..count as usize]
        .split(|b| *b == 0)
        .filter(|name| !name.is_empty())
    {
        let c_name = std::ffi::CString::new(name).map_err(error)?;
        let mut value = vec![0; super::model::MAX_XATTR_BYTES];
        // SAFETY: both C strings and the destination buffer are valid for the call.
        let length = unsafe {
            libc::lgetxattr(
                path.as_ptr(),
                c_name.as_ptr(),
                value.as_mut_ptr().cast(),
                value.len(),
            )
        };
        if length < 0 {
            return Err(error(std::io::Error::last_os_error()));
        }
        value.truncate(length as usize);
        result.push((name.to_vec(), value));
    }
    Ok(result)
}
