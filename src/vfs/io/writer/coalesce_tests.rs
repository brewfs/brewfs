use super::*;
use crate::cadapter::{client::ObjectClient, localfs::LocalFsBackend};
use crate::chunk::cache::ChunksCacheConfig;
use crate::chunk::compress::Compression;
use crate::chunk::store::{BlockStoreConfig, ObjectBlockStore};
use crate::meta::factory::create_meta_store_from_url;
use crate::vfs::config::ReadConfig;
use sha2::{Digest, Sha256};

struct Fixture<M: MetaLayer> {
    objects: tempfile::TempDir,
    _cache: tempfile::TempDir,
    store: Arc<ObjectBlockStore<LocalFsBackend>>,
    reader: Arc<DataReader<ObjectBlockStore<LocalFsBackend>, M>>,
    writer: FileWriter<ObjectBlockStore<LocalFsBackend>, M>,
    inode: Arc<Inode>,
}

async fn fixture() -> Fixture<impl MetaLayer> {
    fixture_with_options(65536, 4096).await
}

async fn fixture_with_options(
    write_coalesce_bytes: usize,
    page_size: u32,
) -> Fixture<impl MetaLayer> {
    let objects = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let store = Arc::new(
        ObjectBlockStore::new_with_configs_async(
            ObjectClient::new(LocalFsBackend::new(objects.path())),
            ChunksCacheConfig {
                disk_storage_dir: Some(cache.path().to_path_buf()),
                max_hot_bytes: 0,
                max_disk_bytes: 0,
                ..Default::default()
            },
            BlockStoreConfig {
                write_coalesce_bytes,
                write_coalesce_ms: 0,
                compression: Compression::None,
                populate_write_cache_after_upload: false,
                page_cache_capacity: 0,
                ..Default::default()
            },
        )
        .await
        .unwrap(),
    );
    let layout = crate::chunk::ChunkLayout::default();
    let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
    let meta = meta_handle.layer();
    let ino = meta.create_file(1, "coalesce".into()).await.unwrap();
    let inode = Inode::new(ino, 0);
    let backend = Arc::new(Backend::new(store.clone(), meta));
    let reader = Arc::new(DataReader::new(
        Arc::new(ReadConfig::new(layout)),
        backend.clone(),
    ));
    let writer = FileWriter::new(
        inode.clone(),
        Arc::new(
            WriteConfig::new(layout)
                .page_size(page_size)
                .freeze_min_bytes(32 << 20)
                .auto_flush_max_age(Duration::from_secs(60)),
        ),
        backend,
        reader.clone(),
        Arc::new(AtomicU64::new(0)),
        None,
    );
    Fixture {
        objects,
        _cache: cache,
        store,
        reader,
        writer,
        inode,
    }
}

#[tokio::test]
async fn isolated_4k_flush_uploads_only_one_framed_payload_with_coalescing_on_or_off() {
    use crate::chunk::compress::{PERSISTED_HEADER_LEN, PersistedHeader, parse_persisted_header};

    for coalesce_bytes in [0, 65536] {
        let f = fixture_with_options(coalesce_bytes, 65536).await;
        let data = (0..4096)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        f.writer.write_at(0, &data).await.unwrap();
        f.writer.flush().await.unwrap();
        let stats = f.store.object_store_metrics().unwrap().snapshot();
        assert_eq!(stats.put_ops, 1);
        assert_eq!(stats.put_bytes, (data.len() + PERSISTED_HEADER_LEN) as u64);
        let objects = std::fs::read_dir(f.objects.path().join("chunks-v2"))
            .unwrap()
            .map(|entry| entry.unwrap().path().join("0"))
            .collect::<Vec<_>>();
        assert_eq!(objects.len(), 1);
        let persisted = std::fs::read(&objects[0]).unwrap();
        assert_eq!(persisted.len(), 4100, "no page or block alignment padding");
        assert_eq!(
            parse_persisted_header(&persisted),
            PersistedHeader::Framed(Compression::None)
        );
        assert_eq!(&persisted[PERSISTED_HEADER_LEN..], data);
        assert!(!f.objects.path().join("chunks-coalesced-v1").exists());
        let reader = f.reader.open_for_handle(f.inode.clone(), 1);
        assert_eq!(reader.read(0, 4096).await.unwrap(), data);
    }
}

// A full permutation of 8192 independent pages, spread over 512 MiB. The
// holes deliberately prevent an adjacent-only merge from passing this gate.
#[tokio::test]
async fn coalesce_8192_random_writes_reduce_physical_puts_and_preserve_every_block() {
    let f = fixture().await;
    let writer = &f.writer;
    let mut expected = BTreeMap::new();
    for index in 0..8192_u64 {
        let page = index * 4051 % 8192;
        let offset = page * 65536;
        let mut bytes = vec![0; 4096];
        for (i, byte) in bytes.iter_mut().enumerate() {
            *byte = ((page * 31 + i as u64 * 17) % 251) as u8;
        }
        writer.write_at(offset, &bytes).await.unwrap();
        assert_eq!(
            writer
                .read_dirty_if_fully_covered(offset, bytes.len())
                .await
                .unwrap(),
            Some(bytes.clone()),
            "read-your-writes must use the pending local data"
        );
        expected.insert(offset, Sha256::digest(&bytes));
        // Bound the fixture's pending metadata without changing the workload's
        // random offsets. Each flush still contains 1 MiB of independent data.
        if (index + 1) % 256 == 0 {
            writer.flush().await.unwrap();
        }
    }
    writer.flush().await.unwrap();
    let puts = f.store.object_store_metrics().unwrap().snapshot().put_ops;
    assert!(
        puts <= 812,
        "32 MiB must use <= 812 PUTs (26000/GiB), got {puts}"
    );
    let committed = f.reader.open_for_handle(f.inode.clone(), 1);
    let zero_digest = Sha256::digest([0u8; 4096]);
    for (offset, digest) in expected {
        let length = (f.inode.file_size() - offset).min(65536) as usize;
        let data = committed.read(offset, length).await.unwrap();
        assert_eq!(data.len(), length);
        assert_eq!(Sha256::digest(&data[..4096]), digest, "block at {offset}");
        for hole in data[4096..].chunks(4096) {
            assert_eq!(Sha256::digest(hole), zero_digest, "hole after {offset}");
        }
    }
}

#[tokio::test]
async fn coalesce_empty_file_and_single_byte_flush() {
    let f = fixture().await;
    f.writer.flush().await.unwrap();
    assert_eq!(
        f.store.object_store_metrics().unwrap().snapshot().put_ops,
        0
    );
    f.writer.write_at(0, b"Z").await.unwrap();
    assert_eq!(
        f.writer.read_dirty_if_fully_covered(0, 1).await.unwrap(),
        Some(b"Z".to_vec())
    );
    f.writer
        .flush_with_deadline(CLOSE_FLUSH_DEADLINE)
        .await
        .unwrap();
    let reader = f.reader.open_for_handle(f.inode.clone(), 1);
    assert_eq!(reader.read(0, 1).await.unwrap(), b"Z");
    assert!(
        !f.objects.path().join("chunks-coalesced-v1").exists(),
        "an explicit flush of one small file must not wait for unrelated writes"
    );
}

#[tokio::test]
async fn coalesce_512_mib_sequential_writes_keep_megabyte_objects() {
    let f = fixture().await;
    let block = vec![0x9e; 1 << 20];
    for index in 0..512 {
        f.writer.write_at(index << 20, &block).await.unwrap();
    }
    f.writer.flush().await.unwrap();
    let stats = f.store.object_store_metrics().unwrap().snapshot();
    assert_eq!(f.inode.file_size(), 512 << 20);
    assert!(stats.put_ops > 0);
    assert!(
        stats.put_bytes / stats.put_ops >= 1 << 20,
        "sequential objects must remain MB-sized"
    );
    assert!(
        !f.objects.path().join("chunks-coalesced-v1").exists(),
        "large sequential writes must bypass aggregation"
    );
}

#[tokio::test]
async fn coalesce_overlapping_small_and_large_writes_preserve_commit_order() {
    let f = fixture().await;
    let mut expected = vec![0u8; 1 << 20];
    for round in 0..3 {
        for index in 0..64_usize {
            let offset = ((index * 37) % 64) * 4096;
            let bytes = vec![index as u8 + round; 4096];
            f.writer.write_at(offset as u64, &bytes).await.unwrap();
            expected[offset..offset + 4096].copy_from_slice(&bytes);
        }
        f.writer.flush().await.unwrap();
        let reader = f.reader.open_for_handle(f.inode.clone(), 1);
        let bytes = reader.read(0, 64 * 4096).await.unwrap();
        assert_eq!(bytes, &expected[..64 * 4096]);
        let large = vec![round + 91; 1 << 20];
        f.writer.write_at(0, &large).await.unwrap();
        f.writer.flush().await.unwrap();
        expected.copy_from_slice(&large);
        let bytes = reader.read(0, 1 << 20).await.unwrap();
        assert_eq!(Sha256::digest(&bytes), Sha256::digest(&expected));
    }
}
