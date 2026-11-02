//! Flat-v1 shared immutable objects. Logical extents and their commit order
//! remain independent; the object never contains synthetic bytes for holes.
use super::*;
use crate::meta::MetaLayer;
use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, oneshot};

pub(crate) const ID_FLAG: u64 = 1 << 52;
pub(crate) const MAX_MEMBER_BYTES: usize = 65536;
const MAX_MEMBERS: usize = 32;
const HEADER: usize = 64;
const ENTRY: usize = 48;
const MAX_OBJECT: usize = 4 << 20;
const MAGIC: &[u8; 8] = b"BFSCOA01";

pub(crate) fn encode_id(group: u64, slot: usize) -> anyhow::Result<u64> {
    anyhow::ensure!(
        (1..1 << 47).contains(&group) && slot < MAX_MEMBERS,
        "coalesced ID exhausted"
    );
    Ok(ID_FLAG | (group << 5) | slot as u64)
}

pub(crate) fn decode_id(id: u64) -> Option<(u64, u8)> {
    (ID_FLAG..ID_FLAG * 2)
        .contains(&id)
        .then_some(((id & !ID_FLAG) >> 5, (id & 31) as u8))
}

pub(crate) fn object_key(group: u64) -> String {
    format!("chunks-coalesced-v1/{group}/0")
}

pub(crate) fn ledger_key(group: u64) -> String {
    format!("write_coalesce/{group}")
}

#[derive(Clone, Debug)]
pub(super) struct IndexEntry {
    offset: usize,
    stored: usize,
    raw: usize,
    digest: [u8; 32],
}

struct Member {
    seed: u64,
    chunk_id: u64,
    raw: usize,
    frame: Bytes,
    meta: Arc<dyn MetaLayer>,
    upload_limit: Arc<Semaphore>,
    result: oneshot::Sender<Result<u64, String>>,
    _memory: OwnedSemaphorePermit,
}

#[derive(Default)]
struct Pending {
    generation: u64,
    raw: usize,
    members: Vec<Member>,
}

pub(super) struct Coalescer {
    pending: Mutex<Pending>,
    memory: Arc<Semaphore>,
    pub(super) index: Cache<u64, Arc<Vec<IndexEntry>>>,
    flight: SingleFlight<u64, Option<Bytes>>,
}

impl Coalescer {
    pub(super) fn new() -> Self {
        Self {
            pending: Mutex::new(Pending::default()),
            memory: Arc::new(Semaphore::new(8 << 20)),
            index: Cache::new(4096),
            flight: SingleFlight::new(),
        }
    }

    fn take(&self, generation: u64) -> Vec<Member> {
        let mut pending = self.pending.lock();
        if pending.generation != generation {
            return Vec::new();
        }
        pending.raw = 0;
        std::mem::take(&mut pending.members)
    }
}

fn encode_object(group: u64, members: &[Member]) -> anyhow::Result<Bytes> {
    anyhow::ensure!(
        !members.is_empty() && members.len() <= MAX_MEMBERS,
        "invalid group size"
    );
    let index_len = HEADER + ENTRY * members.len();
    let total = index_len + members.iter().map(|m| m.frame.len()).sum::<usize>();
    anyhow::ensure!(
        total <= MAX_OBJECT,
        "coalesced object exceeds allocation bound"
    );
    let mut data = vec![0u8; index_len];
    data.reserve_exact(total - index_len);
    data[..8].copy_from_slice(MAGIC);
    data[8..16].copy_from_slice(&group.to_le_bytes());
    data[16..20].copy_from_slice(&(members.len() as u32).to_le_bytes());
    data[20..24].copy_from_slice(&(index_len as u32).to_le_bytes());
    data[24..32].copy_from_slice(&(total as u64).to_le_bytes());
    let mut offset = index_len;
    for (slot, member) in members.iter().enumerate() {
        let start = HEADER + slot * ENTRY;
        data[start..start + 4].copy_from_slice(&(offset as u32).to_le_bytes());
        data[start + 4..start + 8].copy_from_slice(&(member.frame.len() as u32).to_le_bytes());
        data[start + 8..start + 12].copy_from_slice(&(member.raw as u32).to_le_bytes());
        data[start + 16..start + 48].copy_from_slice(&Sha256::digest(&member.frame));
        data.extend_from_slice(&member.frame);
        offset += member.frame.len();
    }
    let mut hash = Sha256::new();
    hash.update(&data[..32]);
    hash.update(&data[HEADER..index_len]);
    data[32..64].copy_from_slice(&hash.finalize());
    Ok(Bytes::from(data))
}

fn parse_index(group: u64, data: &[u8]) -> anyhow::Result<Vec<IndexEntry>> {
    anyhow::ensure!(
        (HEADER..=MAX_OBJECT).contains(&data.len()),
        "invalid coalesced object length"
    );
    anyhow::ensure!(&data[..8] == MAGIC, "invalid coalesced magic");
    anyhow::ensure!(
        u64::from_le_bytes(data[8..16].try_into()?) == group,
        "coalesced group mismatch"
    );
    let count = u32::from_le_bytes(data[16..20].try_into()?) as usize;
    let index_len = u32::from_le_bytes(data[20..24].try_into()?) as usize;
    let total = u64::from_le_bytes(data[24..32].try_into()?);
    anyhow::ensure!(
        count > 0
            && count <= MAX_MEMBERS
            && index_len == HEADER + count * ENTRY
            && index_len <= data.len()
            && total == data.len() as u64,
        "invalid coalesced index bounds"
    );
    let mut hash = Sha256::new();
    hash.update(&data[..32]);
    hash.update(&data[HEADER..index_len]);
    let index_digest: [u8; 32] = hash.finalize().into();
    anyhow::ensure!(
        index_digest == data[32..64],
        "coalesced index digest mismatch"
    );
    let mut result = Vec::with_capacity(count);
    let mut cursor = index_len;
    for slot in 0..count {
        let start = HEADER + slot * ENTRY;
        let offset = u32::from_le_bytes(data[start..start + 4].try_into()?) as usize;
        let stored = u32::from_le_bytes(data[start + 4..start + 8].try_into()?) as usize;
        let raw = u32::from_le_bytes(data[start + 8..start + 12].try_into()?) as usize;
        anyhow::ensure!(
            offset == cursor
                && (PERSISTED_HEADER_LEN..=MAX_MEMBER_BYTES + 1024).contains(&stored)
                && raw > 0
                && raw <= MAX_MEMBER_BYTES
                && data[start + 12..start + 16] == [0; 4],
            "invalid coalesced member"
        );
        cursor = cursor
            .checked_add(stored)
            .context("coalesced extent overflow")?;
        anyhow::ensure!(cursor <= data.len(), "coalesced member exceeds object");
        result.push(IndexEntry {
            offset,
            stored,
            raw,
            digest: data[start + 16..start + 48].try_into()?,
        });
    }
    anyhow::ensure!(cursor == data.len(), "trailing coalesced bytes");
    Ok(result)
}

fn decode_member(entry: &IndexEntry, frame: Bytes) -> anyhow::Result<Bytes> {
    let digest: [u8; 32] = Sha256::digest(&frame).into();
    anyhow::ensure!(
        frame.len() == entry.stored && digest == entry.digest,
        "coalesced member digest mismatch"
    );
    let bytes = decompress_framed_bytes(frame, MAX_MEMBER_BYTES)?;
    anyhow::ensure!(
        bytes.len() == entry.raw,
        "coalesced decoded length mismatch"
    );
    Ok(bytes)
}

impl<B: ObjectBackend + Send + Sync + 'static> ObjectBlockStore<B> {
    pub(super) fn supports_coalesced_objects(&self) -> bool {
        self.legacy_delete_allowed
            && !self.config.create_only_writes
            && !self.config.versioned_objects_only
    }

    pub(super) async fn enqueue_coalesced(
        self: Arc<Self>,
        seed: u64,
        chunk_id: u64,
        parts: Vec<Bytes>,
        meta: Arc<dyn MetaLayer>,
        upload_limit: Arc<Semaphore>,
    ) -> anyhow::Result<u64> {
        encode_id(seed, 0)?;
        let raw = parts.iter().map(Bytes::len).sum::<usize>();
        anyhow::ensure!(
            raw > 0 && raw <= self.write_coalesce_limit(),
            "ineligible coalesced write"
        );
        let memory = self
            .coalescer
            .memory
            .clone()
            .acquire_many_owned((raw * 3 + 4096) as u32)
            .await?;
        let (_, frame) = encode_persisted_block_vectored(&parts, self.config.compression);
        let (tx, rx) = oneshot::channel();
        let (generation, first, ready, previous) = {
            let mut pending = self.coalescer.pending.lock();
            let previous = if pending
                .members
                .first()
                .is_some_and(|m| !Arc::ptr_eq(&m.meta, &meta))
            {
                pending.raw = 0;
                std::mem::take(&mut pending.members)
            } else {
                Vec::new()
            };
            let first = pending.members.is_empty();
            if first {
                pending.generation += 1;
            }
            pending.raw += raw;
            pending.members.push(Member {
                seed,
                chunk_id,
                raw,
                frame,
                meta,
                upload_limit,
                result: tx,
                _memory: memory,
            });
            let ready = if pending.raw >= self.config.write_coalesce_bytes
                || pending.members.len() == MAX_MEMBERS
            {
                pending.raw = 0;
                std::mem::take(&mut pending.members)
            } else {
                Vec::new()
            };
            (pending.generation, first, ready, previous)
        };
        if !previous.is_empty() {
            self.clone().spawn_group(previous);
        }
        if !ready.is_empty() {
            self.clone().spawn_group(ready);
        } else if first {
            let store = self.clone();
            let deadline = if self.config.write_coalesce_ms == 0 {
                100
            } else {
                self.config.write_coalesce_ms
            };
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(deadline)).await;
                let members = store.coalescer.take(generation);
                store.spawn_group(members);
            });
        }
        rx.await
            .context("coalesced upload task disappeared")?
            .map_err(anyhow::Error::msg)
    }

    fn spawn_group(self: Arc<Self>, members: Vec<Member>) {
        if members.is_empty() {
            return;
        }
        tokio::spawn(async move {
            let result = self.upload_group(&members).await;
            for (slot, member) in members.into_iter().enumerate() {
                let answer = match &result {
                    Ok(group) => encode_id(*group, slot).map_err(|e| e.to_string()),
                    Err(error) => Err(format!("coalesced upload failed: {error:#}")),
                };
                let _ = member.result.send(answer);
            }
        });
    }

    async fn upload_group(&self, members: &[Member]) -> anyhow::Result<u64> {
        // Acquire per-file permits only after forming the physical object.
        // Sorting shared semaphore addresses avoids cross-file lock cycles.
        let mut limits: Vec<_> = members.iter().map(|m| m.upload_limit.clone()).collect();
        limits.sort_by_key(|limit| Arc::as_ptr(limit) as usize);
        limits.dedup_by(|a, b| Arc::ptr_eq(a, b));
        let mut permits = Vec::with_capacity(limits.len());
        for limit in limits {
            permits.push(limit.acquire_owned().await?);
        }
        let _upload = crate::chunk::writer::fg_upload_permit().await;
        let group = members[0].seed;
        let descriptors = members
            .iter()
            .enumerate()
            .map(|(slot, m)| Ok((encode_id(group, slot)?, m.chunk_id, m.raw as u64)))
            .collect::<anyhow::Result<Vec<_>>>()?;
        // Registration precedes the PUT, so a crash can leave only tracked
        // orphans. Publication atomically consumes each member reservation.
        members[0]
            .meta
            .register_coalesced_group(group, &descriptors)
            .await?;
        let body = encode_object(group, members)?;
        self.bandwidth.acquire_upload(body.len()).await;
        let started = Instant::now();
        let key = object_key(group);
        for attempt in 0..5 {
            match self
                .client
                .put_object_vectored(&key, vec![body.clone()])
                .await
            {
                Ok(()) => {
                    self.object_metrics
                        .record_put(body.len() as u64, started.elapsed());
                    return Ok(group);
                }
                Err(error) if attempt == 4 => return Err(error),
                Err(_) => tokio::time::sleep(Duration::from_millis(20 << attempt)).await,
            }
        }
        unreachable!()
    }

    pub(super) async fn read_coalesced(
        &self,
        key: BlockKey,
        offset: u64,
        buf: &mut [u8],
    ) -> anyhow::Result<bool> {
        if let Some(bytes) = self.read_cached_range(key, offset, buf.len()).await {
            buf.copy_from_slice(&bytes);
            return Ok(true);
        }
        let (group, slot) = decode_id(key.0).context("not a coalesced ID")?;
        let object = object_key(group);
        let (index, whole) = if let Some(index) = self.coalescer.index.get(&group).await {
            (index, None)
        } else {
            let bytes = self
                .coalescer
                .flight
                .execute(group, || async {
                    let started = Instant::now();
                    let data = self
                        .client
                        .typed_full(
                            ReadClass::NativePayload,
                            &object,
                            None,
                            MAX_OBJECT as u64,
                            |data| {
                                let index = parse_index(group, &data)
                                    .map_err(|e| (FailureClass::Schema, e))?;
                                for entry in &index {
                                    let actual: [u8; 32] = Sha256::digest(
                                        &data[entry.offset..entry.offset + entry.stored],
                                    )
                                    .into();
                                    if actual != entry.digest {
                                        return Err((
                                            FailureClass::Authentication,
                                            anyhow::anyhow!("coalesced payload digest mismatch"),
                                        ));
                                    }
                                }
                                Ok(Bytes::from(data))
                            },
                        )
                        .await?;
                    if let Some(bytes) = &data {
                        self.bandwidth.acquire_download(bytes.len()).await;
                    }
                    self.object_metrics.record_get(
                        data.as_ref().map_or(0, |v| v.len() as u64),
                        started.elapsed(),
                    );
                    Ok::<Option<Bytes>, anyhow::Error>(data)
                })
                .await
                .map_err(|e| anyhow::anyhow!("coalesced GET failed: {e:#}"))?;
            let Some(data) = bytes.as_ref() else {
                return Ok(false);
            };
            let index = Arc::new(parse_index(group, data)?);
            self.coalescer.index.insert(group, index.clone()).await;
            (index, Some(data.clone()))
        };
        anyhow::ensure!(key.1 == 0, "coalesced slice has only one block");
        let entry = index
            .get(slot as usize)
            .context("coalesced slot outside index")?;
        let decoded = if let Some(whole) = whole {
            decode_member(
                entry,
                whole.slice(entry.offset..entry.offset + entry.stored),
            )?
        } else {
            self.bandwidth.acquire_download(entry.stored).await;
            let started = Instant::now();
            let decoded = self
                .client
                .typed_exact(
                    ReadClass::NativePayload,
                    &object,
                    entry.offset as u64,
                    entry.stored as u64,
                    (MAX_MEMBER_BYTES + 1024) as u64,
                    |data| {
                        decode_member(entry, Bytes::from(data))
                            .map_err(|e| (FailureClass::Authentication, e))
                    },
                )
                .await?;
            self.object_metrics
                .record_get(entry.stored as u64, started.elapsed());
            decoded
        };
        let start = usize::try_from(offset)?;
        let end = start
            .checked_add(buf.len())
            .context("coalesced read overflow")?;
        let bytes = decoded.get(start..end);
        require_complete_read(key, offset, buf.len(), bytes.map_or(0, <[u8]>::len))?;
        let bytes = bytes.expect("complete range checked above");
        buf.copy_from_slice(bytes);
        self.block_cache
            .insert_opportunistic(Self::key_for(key), decoded)
            .await;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cadapter::localfs::LocalFsBackend;
    use crate::meta::factory::{MetaHandle, create_meta_store_from_url};
    use crate::meta::store::MetaStore;
    use crate::meta::stores::DatabaseMetaStore;

    struct Fixture {
        root: tempfile::TempDir,
        _cache: tempfile::TempDir,
        store: Arc<ObjectBlockStore<LocalFsBackend>>,
        meta: MetaHandle<DatabaseMetaStore>,
    }

    async fn fixture(bytes: usize, ms: u64) -> Fixture {
        let root = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let store = Arc::new(
            ObjectBlockStore::new_with_configs_async(
                ObjectClient::new(LocalFsBackend::new(root.path())),
                ChunksCacheConfig {
                    disk_storage_dir: Some(cache.path().to_owned()),
                    max_hot_bytes: 0,
                    max_disk_bytes: 0,
                    ..Default::default()
                },
                BlockStoreConfig {
                    write_coalesce_bytes: bytes,
                    write_coalesce_ms: ms,
                    compression: Compression::None,
                    page_cache_capacity: 0,
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
        );
        let meta = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        Fixture {
            root,
            _cache: cache,
            store,
            meta,
        }
    }

    fn enqueue(f: &Fixture, seed: u64, len: usize) -> tokio::task::JoinHandle<anyhow::Result<u64>> {
        enqueue_at(f, seed, 9, len)
    }

    fn enqueue_at(
        f: &Fixture,
        seed: u64,
        chunk_id: u64,
        len: usize,
    ) -> tokio::task::JoinHandle<anyhow::Result<u64>> {
        let store = f.store.clone();
        let meta = f.meta.layer();
        tokio::spawn(async move {
            store
                .write_coalesced(
                    seed,
                    chunk_id,
                    vec![Bytes::from(vec![seed as u8; len])],
                    meta,
                    Arc::new(Semaphore::new(10)),
                )
                .await
        })
    }

    async fn pending_bytes(f: &Fixture, expected: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if f.store.coalescer.pending.lock().raw == expected {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn coalesce_threshold_minus_one_exact_and_over() {
        for last in [1, 2] {
            let f = fixture(65536, 5000).await;
            let first = enqueue(&f, 1, 65535);
            pending_bytes(&f, 65535).await;
            assert_eq!(f.store.object_metrics.snapshot().put_ops, 0);
            assert!(!first.is_finished(), "threshold minus one must wait");
            let second = enqueue(&f, 2, last);
            let (a, b) = tokio::time::timeout(Duration::from_secs(2), async {
                (
                    first.await.unwrap().unwrap(),
                    second.await.unwrap().unwrap(),
                )
            })
            .await
            .unwrap();
            assert_eq!(decode_id(a).unwrap().0, decode_id(b).unwrap().0);
            assert_eq!(f.store.object_metrics.snapshot().put_ops, 1);
            let mut bytes = vec![0; last];
            f.store.read_range((b, 0), 0, &mut bytes).await.unwrap();
            assert_eq!(bytes, vec![2; last]);
        }
    }

    #[tokio::test]
    async fn coalesce_default_deadline_flushes_single_byte_and_off_reader_works() {
        let f = fixture(65536, 0).await;
        let id = tokio::time::timeout(Duration::from_secs(2), enqueue(&f, 1, 1))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let mut reader = ObjectBlockStore::new_with_configs_async(
            ObjectClient::new(LocalFsBackend::new(f.root.path())),
            ChunksCacheConfig {
                disk_storage_dir: Some(f._cache.path().to_owned()),
                max_hot_bytes: 0,
                max_disk_bytes: 0,
                ..Default::default()
            },
            BlockStoreConfig {
                write_coalesce_bytes: 0,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        reader.config.write_coalesce_bytes = 0;
        assert_eq!(reader.write_coalesce_limit(), 0);
        let mut output = [0];
        reader.read_range((id, 0), 0, &mut output).await.unwrap();
        assert_eq!(output, [1]);
        assert!(reader.read_range((id, 0), 1, &mut output).await.is_err());
        assert!(reader.delete_range((id, 0), 1).await.is_err());
    }

    #[tokio::test]
    async fn coalesce_old_high_id_object_is_still_readable() {
        let f = fixture(0, 0).await;
        let id = encode_id(99, 4).unwrap();
        f.store
            .write_fresh_range((id, 0), 0, b"old object")
            .await
            .unwrap();
        let mut output = [0; 10];
        f.store.read_range((id, 0), 0, &mut output).await.unwrap();
        assert_eq!(&output, b"old object");
        f.store.delete_range((id, 0), 1).await.unwrap();
    }

    #[tokio::test]
    async fn coalesce_native_reader_does_not_probe_the_flat_shared_namespace() {
        let mut f = fixture(65536, 0).await;
        let id = encode_id(99, 4).unwrap();
        f.store
            .write_fresh_range((id, 0), 0, b"native object")
            .await
            .unwrap();
        let unrelated = f.root.path().join(object_key(99));
        tokio::fs::create_dir_all(unrelated.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(unrelated, b"not a native object")
            .await
            .unwrap();
        let store = Arc::get_mut(&mut f.store).unwrap();
        store.config.create_only_writes = true;
        store.config.versioned_objects_only = true;
        assert_eq!(store.write_coalesce_limit(), 0);
        let mut output = [0; 13];
        store.read_range((id, 0), 0, &mut output).await.unwrap();
        assert_eq!(&output, b"native object");
    }

    #[tokio::test]
    async fn coalesce_corruption_fails_closed_for_header_and_member() {
        let f = fixture(1, 0).await;
        let id = enqueue(&f, 1, 4096).await.unwrap().unwrap();
        let key = f.root.path().join(object_key(1));
        let original = tokio::fs::read(&key).await.unwrap();
        for position in [0, 8, 16, 24, 32, 64, original.len() - 1] {
            let mut corrupt = original.clone();
            corrupt[position] ^= 1;
            tokio::fs::write(&key, corrupt).await.unwrap();
            f.store.coalescer.index.invalidate(&1).await;
            assert!(
                f.store
                    .read_range((id, 0), 0, &mut [0; 4096])
                    .await
                    .is_err(),
                "byte {position}"
            );
        }
    }

    #[tokio::test]
    async fn coalesce_retirement_keeps_live_members_and_is_idempotent() {
        let f = fixture(8192, 5000).await;
        let a = enqueue(&f, 1, 4096);
        pending_bytes(&f, 4096).await;
        let b = enqueue(&f, 2, 4096);
        let a = a.await.unwrap().unwrap();
        let b = b.await.unwrap().unwrap();
        let raw = f.meta.store();
        let (group, slot_a) = decode_id(a).unwrap();
        let (_, slot_b) = decode_id(b).unwrap();
        assert!(raw.forget_coalesced_group(group).await.is_err());
        assert_eq!(
            raw.retire_coalesced_member(group, slot_a).await.unwrap(),
            Some(false)
        );
        assert_eq!(
            raw.retire_coalesced_member(group, slot_a).await.unwrap(),
            Some(false)
        );
        let mut output = [0; 4096];
        f.store.read_range((b, 0), 0, &mut output).await.unwrap();
        assert_eq!(output, [2; 4096]);
        assert_eq!(
            raw.retire_coalesced_member(group, slot_b).await.unwrap(),
            Some(true)
        );
        f.store.delete_coalesced_group(group).await.unwrap();
        raw.forget_coalesced_group(group).await.unwrap();
        assert_eq!(
            raw.retire_coalesced_member(group, slot_a).await.unwrap(),
            None
        );
        assert!(!f.root.path().join(object_key(group)).exists());
    }

    #[tokio::test]
    async fn coalesce_gc_orphan_then_truncate_preserves_live_bytes_and_deletes_last_member() {
        let f = fixture(8192, 5000).await;
        let raw = f.meta.store();
        let ino = raw.create_file(1, "gc-live".into()).await.unwrap();
        let chunk = crate::vfs::chunk_id_for(ino, 0).unwrap();
        let orphan = enqueue_at(&f, 1, chunk, 4096);
        pending_bytes(&f, 4096).await;
        let live = enqueue_at(&f, 2, chunk, 4096);
        let orphan = orphan.await.unwrap().unwrap();
        let live = live.await.unwrap().unwrap();
        let group = decode_id(orphan).unwrap().0;
        let slice = crate::chunk::SliceDesc {
            slice_id: live,
            chunk_id: chunk,
            offset: 4096,
            length: 4096,
        };
        raw.write_with_coalesced_reservation(ino, chunk, slice, 8192, true)
            .await
            .unwrap();
        let gc = crate::chunk::BlockStoreGC::new(raw.clone(), f.store.clone());
        let config = crate::chunk::BlockGcConfig {
            min_age_secs: -1,
            orphan_cleanup_age_secs: -1,
            ..Default::default()
        };
        gc.run_gc_cycle(&config).await.unwrap();
        assert!(f.root.path().join(object_key(group)).exists());
        let mut output = [0; 4096];
        f.store.read_range((live, 0), 0, &mut output).await.unwrap();
        assert_eq!(output, [2; 4096]);
        raw.truncate(ino, 0, 64 << 20).await.unwrap();
        gc.run_gc_cycle(&config).await.unwrap();
        assert!(!f.root.path().join(object_key(group)).exists());
        assert_eq!(raw.retire_coalesced_member(group, 0).await.unwrap(), None);
        gc.run_gc_cycle(&config).await.unwrap();
    }

    #[tokio::test]
    async fn coalesce_orphan_claim_prevents_late_publication() {
        let f = fixture(1, 0).await;
        let ino = f
            .meta
            .layer()
            .create_file(1, "orphan".into())
            .await
            .unwrap();
        let chunk = crate::vfs::chunk_id_for(ino, 0).unwrap();
        let id = encode_id(1, 0).unwrap();
        let raw = f.meta.store();
        raw.register_coalesced_group(1, &[(id, chunk, 4096)])
            .await
            .unwrap();
        let orphans = raw.cleanup_orphan_uncommitted_slices(-1, 10).await.unwrap();
        assert_eq!(orphans, vec![(id, 4096)]);
        let slice = crate::chunk::SliceDesc {
            slice_id: id,
            chunk_id: chunk,
            offset: 0,
            length: 4096,
        };
        assert!(
            raw.write_with_coalesced_reservation(ino, chunk, slice, 4096, true)
                .await
                .is_err()
        );
        assert!(raw.get_slices(chunk).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn coalesce_concurrent_groups_keep_unique_members_and_return_every_byte() {
        let f = fixture(65536, 5000).await;
        let tasks: Vec<_> = (1..=64).map(|seed| enqueue(&f, seed, 4096)).collect();
        let mut ids = std::collections::HashSet::new();
        for (index, task) in tasks.into_iter().enumerate() {
            let id = task.await.unwrap().unwrap();
            assert!(ids.insert(id));
            let mut output = [0; 4096];
            f.store.read_range((id, 0), 0, &mut output).await.unwrap();
            assert_eq!(output, [index as u8 + 1; 4096]);
        }
        assert_eq!(f.store.object_metrics.snapshot().put_ops, 4);
    }

    #[tokio::test]
    async fn coalesce_sql_restart_preserves_allocator_and_publication() {
        let directory = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}?mode=rwc",
            directory.path().join("meta.db").display()
        );
        let meta = create_meta_store_from_url(&url).await.unwrap();
        let ino = meta
            .layer()
            .create_file(1, "persisted".into())
            .await
            .unwrap();
        let chunk = crate::vfs::chunk_id_for(ino, 0).unwrap();
        let seed = meta
            .layer()
            .next_id(crate::meta::SLICE_ID_KEY)
            .await
            .unwrap() as u64;
        let id = encode_id(seed, 0).unwrap();
        let slice = crate::chunk::SliceDesc {
            slice_id: id,
            chunk_id: chunk,
            offset: 0,
            length: 4096,
        };
        meta.store()
            .register_coalesced_group(seed, &[(id, chunk, 4096)])
            .await
            .unwrap();
        meta.store()
            .write_with_coalesced_reservation(ino, chunk, slice, 4096, true)
            .await
            .unwrap();
        // An ambiguous successful publication retry must not append twice.
        meta.store()
            .write_with_coalesced_reservation(ino, chunk, slice, 4096, true)
            .await
            .unwrap();
        assert_eq!(meta.store().get_slices(chunk).await.unwrap(), vec![slice]);
        drop(meta);
        let restarted = create_meta_store_from_url(&url).await.unwrap();
        let next = restarted
            .layer()
            .next_id(crate::meta::SLICE_ID_KEY)
            .await
            .unwrap() as u64;
        assert!(next > seed && next < 1 << 47);
        assert_eq!(
            restarted.store().get_slices(chunk).await.unwrap(),
            vec![slice]
        );
        assert_eq!(
            restarted
                .store()
                .retire_coalesced_member(seed, 0)
                .await
                .unwrap(),
            Some(true)
        );
    }

    struct FailedPut(Arc<std::sync::atomic::AtomicUsize>);

    #[async_trait]
    impl ObjectBackend for FailedPut {
        async fn put_object(&self, _: &str, _: &[u8]) -> anyhow::Result<()> {
            self.0.fetch_add(1, Ordering::Relaxed);
            anyhow::bail!("injected upload failure")
        }
        async fn get_object(&self, _: &str) -> anyhow::Result<Option<Vec<u8>>> {
            Ok(None)
        }
        async fn get_object_range(&self, _: &str, _: u64, _: &mut [u8]) -> anyhow::Result<usize> {
            Ok(0)
        }
        async fn get_etag(&self, _: &str) -> anyhow::Result<String> {
            anyhow::bail!("missing")
        }
        async fn delete_object(&self, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn coalesce_failed_upload_never_reports_durable_success() {
        let cache = tempfile::tempdir().unwrap();
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let store = Arc::new(
            ObjectBlockStore::new_with_configs_async(
                ObjectClient::new(FailedPut(attempts.clone())),
                ChunksCacheConfig {
                    disk_storage_dir: Some(cache.path().to_owned()),
                    ..Default::default()
                },
                BlockStoreConfig {
                    write_coalesce_bytes: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
        );
        let meta = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        assert!(
            store
                .clone()
                .write_coalesced(
                    1,
                    9,
                    vec![Bytes::from_static(b"x")],
                    meta.layer(),
                    Arc::new(Semaphore::new(10))
                )
                .await
                .is_err()
        );
        assert_eq!(
            attempts.load(Ordering::Relaxed),
            5,
            "shared uploads must retain the ordinary uploader's five attempts"
        );
        assert_eq!(store.object_metrics.snapshot().put_ops, 0);
        assert_eq!(
            meta.store()
                .cleanup_orphan_uncommitted_slices(-1, 10)
                .await
                .unwrap(),
            vec![(encode_id(1, 0).unwrap(), 1)]
        );
        assert_eq!(store.coalescer.memory.available_permits(), 8 << 20);
    }
}
