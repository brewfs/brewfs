//! Prepared reads use the real authenticated lower and the KV read fences.
use super::*;
use crate::workspace_overlay::meta_layer::packed_lower::with_request_view;

async fn read(f: &Fixture, ino: i64, offset: u64, len: u64) -> Vec<u8> {
    let fence = f
        .meta
        .begin_unified_read_request(ino)
        .await
        .unwrap()
        .unwrap();
    let prepared = with_request_view(
        fence.as_ref(),
        f.meta.prepare_unified_read(ino, 0, offset, len),
    )
    .await
    .unwrap()
    .unwrap();
    let mut bytes = vec![0x55; len as usize];
    execute_unified_into(
        prepared.fetcher.as_ref(),
        offset,
        &prepared.plan,
        &mut bytes,
    )
    .await
    .unwrap();
    fence.ensure_current().await.unwrap();
    bytes
}

fn extent_reads(backend: &PinMemoryBackend) -> [i64; 2] {
    [
        backend.bounded_reads.load(Ordering::SeqCst),
        backend.bounded_scans.load(Ordering::SeqCst),
    ]
}

#[tokio::test]
async fn unified_random_ranges_reuse_chunk_without_backend_reads() {
    let validity = Arc::new(crate::workspace_overlay::lifecycle::LeaseValidity::new(
        std::time::Instant::now(),
        std::time::Duration::from_secs(60),
    ));
    let f = fixture_with_lease(false, packed().await, Some(validity)).await;
    assert_eq!(read(&f, 400, 0, 256).await, f.payload[..256]);
    let before = extent_reads(&f.backend);
    for block in (1..16).rev() {
        let offset = block * 256;
        assert_eq!(
            read(&f, 400, offset, 256).await,
            f.payload[offset as usize..offset as usize + 256],
        );
    }
    assert_eq!(
        extent_reads(&f.backend),
        before,
        "new offsets in a warmed chunk must not issue Redis reads or extent scans",
    );
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn unified_cached_coverage_checks_independent_inode_versions() {
    let f = fixture(false).await;
    f.vfs.chmod(400, 0o644).await.unwrap();
    let old = f
        .meta
        .prepare_unified_read(400, 0, 97, 8)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read(&f, 400, 97, 8).await, f.payload[97..105]);

    // A separate catalog client changes coverage without evicting this
    // metadata instance's cache. Same head epoch, fresh inode data_version.
    let writer =
        KvWorkspaceStore::new(f.backend.clone()).with_packed_reader_pin_budget(f.budget.clone());
    let layers: [LayerRecord; 2] = writer
        .load_layer_chain(f.guard.expected_head_layer_id)
        .await
        .unwrap()
        .try_into()
        .unwrap();
    let (_, rows) = writer
        .read_fenced_inode(f.guard.clone(), layers[1].clone(), 400)
        .await
        .unwrap();
    let mut inode = rows
        .into_iter()
        .find(|row| row.layer_id == layers[0].layer_id)
        .unwrap();
    inode.data_version += 1;
    let mut mutation = VersionedMutation::empty(f.guard.clone(), layers, CHUNK);
    mutation.inodes.push(inode);
    mutation.extents.push(DataExtentDelta::hole(
        f.guard.expected_head_layer_id,
        400,
        0,
        99,
        4,
        0,
    ));
    writer
        .apply_packed_versioned_mutation(mutation, f.binding.binding.clone())
        .await
        .unwrap();

    let mut expected = f.payload[97..105].to_vec();
    expected[2..6].fill(0);
    assert_eq!(read(&f, 400, 97, 8).await, expected);
    assert!(crate::chunk::read_plan::is_read_view_changed(
        &old.fetcher
            .ensure_generation(old.plan.generation)
            .await
            .unwrap_err()
    ));
    assert_eq!(read(&f, 400, 100, 2).await, [0; 2]);
    drop(old);
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn unified_cached_coverage_tracks_writeback_holes_and_truncate_extend() {
    let f = fixture(false).await;
    assert_eq!(read(&f, 400, 0, CHUNK).await, f.payload);
    let fh = f
        .vfs
        .open(
            400,
            f.meta.stat(400).await.unwrap().unwrap(),
            true,
            true,
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        f.vfs.write(fh, 127, OVERWRITE).await.unwrap(),
        OVERWRITE.len()
    );
    f.vfs.flush(fh).await.unwrap();
    let mut expected = f.payload.clone();
    expected[127..127 + OVERWRITE.len()].copy_from_slice(OVERWRITE);
    assert_eq!(read(&f, 400, 0, CHUNK).await, expected);
    f.vfs
        .workspace_hole_fallocate_from_fuse(fh, 400, 129, 4, true)
        .await
        .unwrap();
    expected[129..133].fill(0);
    assert_eq!(read(&f, 400, 126, 20).await, expected[126..146]);
    f.vfs.truncate_inode(400, 132).await.unwrap();
    assert!(f.meta.prepare_unified_read(400, 0, 131, 2).await.is_err());
    f.vfs.truncate_inode(400, CHUNK * 2).await.unwrap();
    expected[132..].fill(0);
    assert_eq!(read(&f, 400, 0, CHUNK).await, expected);
    let tail = f
        .meta
        .prepare_unified_read(400, 1, 0, CHUNK)
        .await
        .unwrap()
        .unwrap();
    let mut bytes = vec![0x55; CHUNK as usize];
    execute_unified_into(tail.fetcher.as_ref(), 0, &tail.plan, &mut bytes)
        .await
        .unwrap();
    assert_eq!(bytes, vec![0; CHUNK as usize]);
    drop(tail);
    f.vfs.close(fh).await.unwrap();
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn unified_cached_coverage_preserves_open_unlink_and_lease_fencing() {
    let f = fixture(false).await;
    let fh = f
        .vfs
        .open(
            400,
            f.meta.stat(400).await.unwrap().unwrap(),
            true,
            true,
            false,
        )
        .await
        .unwrap();
    assert_eq!(read(&f, 400, 0, 256).await, f.payload[..256]);
    f.vfs.unlink("/nonzero").await.unwrap();
    assert_eq!(f.meta.lookup(1, "nonzero").await.unwrap(), None);
    assert_eq!(read(&f, 400, 256, 256).await, f.payload[256..512]);
    f.vfs.close(fh).await.unwrap();
    assert!(f.meta.prepare_unified_read(400, 0, 0, 256).await.is_err());
    f.reader.shutdown().await.unwrap();

    let f = fixture(false).await;
    let prepared = f
        .meta
        .prepare_unified_read(400, 0, 0, 256)
        .await
        .unwrap()
        .unwrap();
    f.store
        .release_lease(crate::workspace_overlay::catalog::ReleaseLease {
            lease_id: f.guard.lease_id,
            holder_generation: f.guard.holder_generation,
        })
        .await
        .unwrap();
    assert!(f.meta.prepare_unified_read(400, 0, 0, 256).await.is_err());
    assert!(
        prepared
            .fetcher
            .ensure_generation(prepared.plan.generation)
            .await
            .is_err()
    );
    drop(prepared);
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn unified_cancelled_fill_does_not_serialize_other_chunks_or_retry() {
    let f = fixture(false).await;
    f.vfs.truncate_inode(400, CHUNK * 2).await.unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    *f.backend.scan_barrier.lock().await = Some((entered.clone(), resume));
    let meta = f.meta.clone();
    let first = tokio::spawn(async move { meta.prepare_unified_read(400, 0, 0, 256).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    let other = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        f.meta.prepare_unified_read(400, 1, 0, 256),
    )
    .await
    .expect("one suspended cache fill must not hold a global read lock")
    .unwrap()
    .unwrap();
    assert!(
        other
            .plan
            .segments
            .iter()
            .all(|segment| matches!(segment.source, ReadSource::Hole))
    );
    first.abort();
    assert!(
        first
            .await
            .err()
            .expect("fill was cancelled")
            .is_cancelled()
    );
    assert_eq!(read(&f, 400, 0, 256).await, f.payload[..256]);
    drop(other);
    f.reader.shutdown().await.unwrap();
}
