mod read_plan_cache_tests {
    use super::*;
    use crate::chunk::SliceDesc;
    use crate::chunk::read_plan::{ReadPlanSegment, WorkspaceReadPlanProvider};
    use crate::meta::MetaLayer;
    use crate::workspace_overlay::meta_layer::WorkspaceMetaLayer;

    const CHUNK: u64 = 64 << 10;

    async fn fixture() -> (
        Arc<KvWorkspaceStore<MemoryBackend>>,
        WorkspaceMetaLayer<KvWorkspaceStore<MemoryBackend>>,
        i64,
    ) {
        let (store, workspace, lease, _) = initialized().await;
        let store = Arc::new(store);
        let meta = WorkspaceMetaLayer::with_chunk_size(
            store.clone(),
            ViewContext {
                workspace_id: workspace.workspace_id,
                head_layer_id: workspace.head_layer_id,
                head_epoch: workspace.head_epoch,
                lease_id: lease.lease_id,
                holder_generation: lease.holder_generation,
            },
            CHUNK,
        );
        let ino = meta
            .create_file(meta.root_ino(), "random-read".into())
            .await
            .unwrap();
        write(&meta, ino, 0, 0, CHUNK, 700).await;
        (store, meta, ino)
    }

    async fn write(
        meta: &WorkspaceMetaLayer<KvWorkspaceStore<MemoryBackend>>,
        ino: i64,
        chunk_index: u64,
        offset: u64,
        length: u64,
        slice_id: u64,
    ) {
        let chunk_id = crate::vfs::chunk_id_for(ino, chunk_index).unwrap();
        meta.write(
            ino,
            chunk_id,
            SliceDesc {
                slice_id,
                chunk_id,
                offset,
                length,
            },
            (chunk_index + 1) * CHUNK,
        )
        .await
        .unwrap();
    }

    fn data(offset: u64, length: u64, slice_id: u64, slice_offset: u64) -> ReadPlanSegment {
        ReadPlanSegment::Data {
            logical_offset: offset,
            length,
            slice_id,
            slice_offset,
        }
    }

    #[tokio::test]
    async fn random_read_ranges_reuse_one_chunk_extent_scan() {
        let (store, meta, ino) = fixture().await;
        assert_eq!(
            meta.read_plan(ino, 0, 0, 4096).await.unwrap().segments,
            vec![data(0, 4096, 700, 0)]
        );
        let scans = store.backend.scans.load(Ordering::Relaxed);
        // Every range is new to the reader. The stored extent history is
        // identical, so seeking within this chunk must not re-scan Redis.
        for block in (1..16).rev() {
            let offset = block * 4096;
            assert_eq!(
                meta.read_plan(ino, 0, offset, 4096).await.unwrap().segments,
                vec![data(offset, 4096, 700, offset)]
            );
        }
        assert_eq!(
            store.backend.scans.load(Ordering::Relaxed),
            scans,
            "different 4 KiB offsets must reuse the already scanned chunk"
        );
    }

    #[tokio::test]
    async fn cached_chunk_clips_overwrites_holes_and_checks_fresh_inode_version() {
        let (store, meta, ino) = fixture().await;
        // An independent metadata instance keeps the reader's plan cache
        // warm: freshness must follow the inode version, not local eviction.
        let writer = WorkspaceMetaLayer::with_chunk_size(store, meta.view_context().await, CHUNK);
        meta.read_plan(ino, 0, 0, 4096).await.unwrap();
        write(&writer, ino, 0, 4094, 4, 701).await;
        assert_eq!(
            meta.read_plan(ino, 0, 4093, 6).await.unwrap().segments,
            vec![
                data(4093, 1, 700, 4093),
                data(4094, 4, 701, 0),
                data(4098, 1, 700, 4098)
            ]
        );
        assert_eq!(
            meta.read_plan(ino, 0, 4095, 2).await.unwrap().segments,
            vec![data(4095, 2, 701, 1)]
        );
        writer.truncate(ino, 4096, CHUNK).await.unwrap();
        writer.extend_file_size(ino, CHUNK).await.unwrap();
        assert_eq!(
            meta.read_plan(ino, 0, 4095, 3).await.unwrap().segments,
            vec![
                data(4095, 1, 701, 1),
                ReadPlanSegment::Zero {
                    logical_offset: 4096,
                    length: 2
                }
            ]
        );
        // Cache identity includes the chunk, and a sparse tail stays zero.
        write(&writer, ino, 1, 0, 4096, 702).await;
        assert_eq!(
            meta.read_plan(ino, 1, 4094, 4).await.unwrap().segments,
            vec![
                data(4094, 2, 702, 4094),
                ReadPlanSegment::Zero {
                    logical_offset: 4096,
                    length: 2
                }
            ]
        );
        assert_eq!(
            meta.read_plan(ino, 0, 0, 1).await.unwrap().segments,
            vec![data(0, 1, 700, 0)]
        );
        assert!(meta.read_plan(ino, 0, CHUNK - 1, 2).await.is_err());
        assert!(
            meta.read_plan(ino, 0, 0, 0)
                .await
                .unwrap()
                .segments
                .is_empty()
        );
        writer
            .unlink(writer.root_ino(), "random-read")
            .await
            .unwrap();
        assert!(
            meta.read_plan(ino, 0, 0, 1).await.is_err(),
            "a cached plan cannot revive an unlinked inode without an open owner"
        );
    }

    #[tokio::test]
    async fn cached_chunk_survives_open_unlink_until_final_close() {
        let (_, meta, ino) = fixture().await;
        let attr = meta.stat(ino).await.unwrap().unwrap();
        meta.record_open(ino, attr, true, false, false)
            .await
            .unwrap();
        meta.read_plan(ino, 0, 0, 4096).await.unwrap();
        meta.unlink(meta.root_ino(), "random-read").await.unwrap();
        assert_eq!(
            meta.lookup(meta.root_ino(), "random-read").await.unwrap(),
            None
        );
        assert_eq!(
            meta.read_plan(ino, 0, 4096, 4096).await.unwrap().segments,
            vec![data(4096, 4096, 700, 4096)]
        );
        meta.record_close(ino).await.unwrap();
        assert!(
            meta.read_plan(ino, 0, 4096, 4096).await.is_err(),
            "final close must fence the cached unlinked inode"
        );
    }

    #[tokio::test]
    async fn small_and_large_cached_ranges_follow_independent_overwrites() {
        let (store, meta, ino) = fixture().await;
        let writer = WorkspaceMetaLayer::with_chunk_size(store, meta.view_context().await, CHUNK);
        for length in [8192, 4096, CHUNK] {
            assert_eq!(
                meta.read_plan(ino, 0, 0, length).await.unwrap().segments,
                vec![data(0, length, 700, 0)]
            );
        }
        write(&writer, ino, 0, 4094, 4, 703).await;
        assert_eq!(
            meta.read_plan(ino, 0, 0, 8192).await.unwrap().segments,
            vec![
                data(0, 4094, 700, 0),
                data(4094, 4, 703, 0),
                data(4098, 4094, 700, 4098)
            ]
        );
        assert_eq!(
            meta.read_plan(ino, 0, 4095, 2).await.unwrap().segments,
            vec![data(4095, 2, 703, 1)]
        );
        writer.truncate(ino, 4096, CHUNK).await.unwrap();
        writer.extend_file_size(ino, CHUNK).await.unwrap();
        for length in [8192, CHUNK] {
            let expected = vec![
                data(0, 4094, 700, 0),
                data(4094, 2, 703, 0),
                ReadPlanSegment::Zero {
                    logical_offset: 4096,
                    length: length - 4096,
                },
            ];
            for _ in 0..2 {
                assert_eq!(
                    meta.read_plan(ino, 0, 0, length).await.unwrap().segments,
                    expected
                );
            }
        }
    }
}
