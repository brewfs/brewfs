//! Store-level evidence: capture the persisted object, not just decoded data.
use super::*;
use crate::cadapter::localfs::LocalFsBackend;
use crate::chunk::compress::copy_probe;
use std::sync::Mutex;

const BLOCK: usize = 4 * 1024 * 1024;

#[derive(Clone)]
struct CapturingBackend {
    local: LocalFsBackend,
    uploaded: Arc<Mutex<Option<Bytes>>>,
}

#[async_trait]
impl ObjectBackend for CapturingBackend {
    async fn put_object_vectored(&self, key: &str, mut chunks: Vec<Bytes>) -> anyhow::Result<()> {
        assert_eq!(chunks.len(), 1);
        let data = chunks.pop().unwrap();
        *self.uploaded.lock().unwrap() = Some(data.clone());
        self.local.put_object_vectored(key, vec![data]).await
    }

    async fn put_object(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
        self.local.put_object(key, data).await
    }

    async fn put_object_create_only(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
        self.local.put_object_create_only(key, data).await
    }

    async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.local.get_object(key).await
    }

    async fn get_object_range(
        &self,
        key: &str,
        offset: u64,
        buf: &mut [u8],
    ) -> anyhow::Result<usize> {
        self.local.get_object_range(key, offset, buf).await
    }

    async fn get_etag(&self, key: &str) -> anyhow::Result<String> {
        self.local.get_etag(key).await
    }

    async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
        self.local.delete_object(key).await
    }
}

async fn store(
    root: &Path,
    compression: Compression,
    cache_enabled: bool,
    create_only: bool,
) -> anyhow::Result<(ObjectBlockStore<CapturingBackend>, CapturingBackend)> {
    let backend = CapturingBackend {
        local: LocalFsBackend::new(root.join("objects")),
        uploaded: Arc::default(),
    };
    let store = ObjectBlockStore::new_with_configs_async(
        ObjectClient::new(backend.clone()),
        ChunksCacheConfig::with_budgets(BLOCK as u64, 0, root.join("cache")),
        BlockStoreConfig {
            compression,
            populate_write_cache_after_upload: cache_enabled,
            create_only_writes: create_only,
            ..Default::default()
        },
    )
    .await?;
    Ok((store, backend))
}

fn payload(len: usize, compressible: bool) -> Bytes {
    // Fixed xorshift stream, independent of rand versions and process seeds.
    let mut state = 0x1234_5678_u32;
    Bytes::from(
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                if compressible { 42 } else { state as u8 }
            })
            .collect::<Vec<_>>(),
    )
}

fn fragments(raw: &Bytes, fragment_len: usize) -> Vec<Bytes> {
    (0..raw.len())
        .step_by(fragment_len)
        .map(|start| raw.slice(start..(start + fragment_len).min(raw.len())))
        .collect()
}

#[tokio::test(flavor = "current_thread")]
async fn persisted_object_copy_evidence() -> anyhow::Result<()> {
    let evidence_dir = std::env::var_os("BREWFS_FRAMED_OBJECT_EVIDENCE_DIR").map(PathBuf::from);
    if let Some(dir) = &evidence_dir {
        fs::create_dir_all(dir)?;
    }
    let mut rows = Vec::new();
    for (name, compression, len, offset, fragment_len, compressible, cache, create_only) in [
        (
            "raw-4m",
            Compression::None,
            BLOCK,
            0,
            128 * 1024,
            false,
            true,
            false,
        ),
        (
            "raw-4m-no-cache",
            Compression::None,
            BLOCK,
            0,
            128 * 1024,
            false,
            false,
            false,
        ),
        (
            "raw-4m-create-only",
            Compression::None,
            BLOCK,
            0,
            128 * 1024,
            false,
            true,
            true,
        ),
        (
            "raw-single",
            Compression::None,
            BLOCK,
            0,
            BLOCK,
            false,
            true,
            false,
        ),
        (
            "raw-sparse-tail",
            Compression::None,
            131_071,
            4097,
            8191,
            false,
            true,
            false,
        ),
        (
            "lz4-fallback",
            Compression::Lz4,
            BLOCK,
            0,
            128 * 1024,
            false,
            true,
            false,
        ),
        (
            "zstd-fallback",
            Compression::Zstd(3),
            BLOCK,
            0,
            128 * 1024,
            false,
            true,
            false,
        ),
        (
            "lz4-compressed",
            Compression::Lz4,
            BLOCK,
            0,
            128 * 1024,
            true,
            true,
            false,
        ),
        (
            "zstd-compressed",
            Compression::Zstd(3),
            BLOCK,
            0,
            128 * 1024,
            true,
            true,
            false,
        ),
    ] {
        let root = tempfile::tempdir()?;
        let (store, backend) = store(root.path(), compression, cache, create_only).await?;
        let raw = payload(len, compressible);
        let parts = fragments(&raw, fragment_len);
        let part_count = parts.len();
        copy_probe::take();
        assert_eq!(
            store
                .write_fresh_vectored((81, 0), offset as u64, parts)
                .await?,
            len as u64
        );
        let (payload_copied, header_copied) = copy_probe::take();
        // Read the actual persisted bytes, including the mandatory frame.
        let object = backend.get_object("chunks-v2/81/0").await?.unwrap();
        let mut expected = vec![0; offset];
        expected.extend_from_slice(&raw);
        let expected_encoding = if compressible {
            compression
        } else {
            Compression::None
        };
        let expected_encoding = match expected_encoding {
            Compression::Zstd(_) => Compression::Zstd(0),
            other => other,
        };
        assert_eq!(
            parse_persisted_header(&object),
            PersistedHeader::Framed(expected_encoding)
        );
        assert_eq!(
            crate::chunk::compress::decompress_framed(&object, BLOCK)?.as_ref(),
            expected
        );
        if expected_encoding == Compression::None {
            assert_eq!(&object[..4], b"SF\0\0");
            assert_eq!(&object[4..], expected);
        }
        let stats = store.block_cache.stats();
        assert_eq!(
            stats.write_hot_bytes,
            if cache { expected.len() as u64 } else { 0 }
        );
        assert_eq!(stats.hot_bytes, 0);
        if cache {
            assert_eq!(
                store
                    .read_cached_range((81, 0), 0, expected.len())
                    .await
                    .unwrap()
                    .as_ref(),
                expected
            );
        }
        let row = serde_json::json!({
            "case": name, "payload_bytes": len, "zero_prefix_bytes": offset,
            "fragments": part_count, "object_bytes": object.len(),
            "header_hex": hex::encode(&object[..4]),
            "sha256": hex::encode(Sha256::digest(&object)),
            "payload_bytes_copied": payload_copied,
            "header_bytes_written": header_copied,
            "write_hot_bytes": stats.write_hot_bytes,
        });
        println!("FRAMED_OBJECT_EVIDENCE {row}");
        if let Some(dir) = &evidence_dir {
            fs::write(dir.join(format!("{name}.object")), &object)?;
        }
        rows.push(row);
    }
    if let Some(dir) = &evidence_dir {
        fs::write(
            dir.join("measurements.json"),
            serde_json::to_vec_pretty(&rows)?,
        )?;
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn fragmented_raw_frame_shares_immutable_cache_and_releases_charge() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let (store, backend) = store(root.path(), Compression::None, true, false).await?;
    let raw = payload(BLOCK, false);
    copy_probe::take();
    store
        .write_fresh_vectored((82, 0), 0, fragments(&raw, 128 * 1024))
        .await?;
    let copied = copy_probe::take();
    let frame = backend.uploaded.lock().unwrap().take().unwrap();
    let cached = store.read_cached_range((82, 0), 0, BLOCK).await.unwrap();
    assert_eq!(
        cached.as_ptr(),
        frame.as_ptr().wrapping_add(PERSISTED_HEADER_LEN)
    );
    assert_eq!(copied, (BLOCK, PERSISTED_HEADER_LEN));
    assert_eq!(cached, raw);
    let frame = frame
        .try_into_mut()
        .expect_err("the raw cache view must keep the upload immutable");
    drop(frame);
    let cached = cached
        .try_into_mut()
        .expect_err("the charged cache entry must retain immutable ownership");
    store
        .block_cache
        .insert_recent_write_hot("chunks/82/0", cached.clone())
        .await;
    assert_eq!(store.block_cache.stats().write_hot_entries, 1);
    assert_eq!(store.block_cache.stats().write_hot_bytes, BLOCK as u64);
    // A second full block evicts the first; outstanding immutable views survive.
    store
        .write_fresh_vectored((82, 1), 0, fragments(&payload(BLOCK, true), 128 * 1024))
        .await?;
    assert!(store.read_cached_range((82, 0), 0, BLOCK).await.is_none());
    assert_eq!(store.block_cache.stats().write_hot_entries, 1);
    assert_eq!(store.block_cache.stats().write_hot_bytes, BLOCK as u64);
    assert_eq!(cached, raw);
    store.block_cache.remove(&"chunks/82/1".to_owned()).await?;
    assert_eq!(store.block_cache.stats().write_hot_bytes, 0);
    assert_eq!(store.block_cache.stats().write_hot_entries, 0);
    assert!(
        cached.try_into_mut().is_ok(),
        "eviction must release the cache's owner"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn fragmented_magic_and_empty_writes_preserve_boundaries() -> anyhow::Result<()> {
    for compression in [Compression::None, Compression::Lz4, Compression::Zstd(3)] {
        let root = tempfile::tempdir()?;
        let (store, backend) = store(root.path(), compression, true, false).await?;
        assert_eq!(
            store
                .write_fresh_vectored((83, 0), 3, vec![Bytes::new()])
                .await?,
            0
        );
        assert!(backend.get_object("chunks-v2/83/0").await?.is_none());
        assert_eq!(store.block_cache.stats().write_hot_bytes, 0);
        let parts = vec![
            Bytes::new(),
            Bytes::from_static(b"SF\0"),
            Bytes::new(),
            Bytes::from_static(b"\0tail"),
            Bytes::new(),
        ];
        assert_eq!(store.write_fresh_vectored((83, 1), 3, parts).await?, 8);
        assert_eq!(
            backend.get_object("chunks-v2/83/1").await?.unwrap(),
            b"SF\0\0\0\0\0SF\0\0tail"
        );
        assert_eq!(
            store
                .read_cached_range((83, 1), 0, 11)
                .await
                .unwrap()
                .as_ref(),
            b"\0\0\0SF\0\0tail"
        );
    }
    Ok(())
}
