//! Exercise negative caching through production metadata mutations and fences.
use super::*;
use crate::workspace::lifecycle::LeaseValidity;

fn lease() -> Arc<LeaseValidity> {
    Arc::new(LeaseValidity::new(
        std::time::Instant::now(),
        std::time::Duration::from_secs(600),
    ))
}

fn metric(f: &Fixture, suffix: &str) -> u64 {
    let stats = crate::vfs::stats::FsStats::new();
    assert!(f.meta.install_row_cache_stats(&stats));
    let prefix = format!("brewfs_workspace_upper_{suffix}_total ");
    stats
        .render()
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .unwrap()
        .parse()
        .unwrap()
}

async fn read(f: &Fixture, offset: u64, length: u64) -> Vec<u8> {
    let prepared = f
        .meta
        .prepare_unified_read(400, 0, offset, length)
        .await
        .unwrap()
        .unwrap();
    let mut bytes = vec![0xff; length as usize];
    execute_unified_into(
        prepared.fetcher.as_ref(),
        offset,
        &prepared.plan,
        &mut bytes,
    )
    .await
    .unwrap();
    bytes
}

async fn names(f: &Fixture) -> Vec<Vec<u8>> {
    f.meta
        .opendir(1)
        .await
        .unwrap()
        .get_entries_page_raw_owned(0, 100)
        .await
        .unwrap()
        .entries
        .into_iter()
        .map(|entry| entry.name)
        .collect()
}

#[tokio::test]
async fn empty_query_cache_hits_are_range_bound_and_revocation_bypasses_them() {
    let validity = lease();
    let f = fixture_with_lease(false, packed().await, Some(validity.clone())).await;
    assert_eq!(read(&f, 0, 64).await, f.payload[..64]);
    let cold = metric(&f, "extent_queries");
    for _ in 0..3 {
        assert_eq!(read(&f, 0, 64).await, f.payload[..64]);
    }
    assert_eq!(metric(&f, "extent_queries"), cold);
    assert_eq!(metric(&f, "extent_empty_cache_hits"), 3);
    assert_eq!(read(&f, 64, 64).await, f.payload[64..128]);
    assert_eq!(metric(&f, "extent_queries"), cold + 1);
    assert_eq!(names(&f).await, vec![b"nonzero".to_vec()]);
    let dirs = metric(&f, "directory_queries");
    assert_eq!(names(&f).await, vec![b"nonzero".to_vec()]);
    assert_eq!(metric(&f, "directory_queries"), dirs);
    assert!(metric(&f, "directory_empty_cache_hits") > 0);
    // Revoked local proof still permits the existing authoritative remote
    // fence when the real lease is alive, but may never use negative rows.
    validity.revoke();
    read(&f, 0, 64).await;
    names(&f).await;
    assert_eq!(metric(&f, "extent_queries"), cold + 2);
    assert_eq!(metric(&f, "directory_queries"), dirs + 2);
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn empty_query_cache_does_not_hide_overwrites_holes_or_truncate_extend() {
    let f = fixture_with_lease(false, packed().await, Some(lease())).await;
    read(&f, 0, 256).await;
    read(&f, 0, 256).await;
    let fh = f
        .vfs
        .open(
            400,
            f.vfs.stat("/nonzero").await.unwrap(),
            true,
            true,
            false,
        )
        .await
        .unwrap();
    f.vfs.write(fh, 32, OVERWRITE).await.unwrap();
    f.vfs.flush(fh).await.unwrap();
    let mut expected = f.payload[..256].to_vec();
    expected[32..32 + OVERWRITE.len()].copy_from_slice(OVERWRITE);
    assert_eq!(read(&f, 0, 256).await, expected);
    let queries = metric(&f, "extent_queries");
    assert_eq!(read(&f, 0, 256).await, expected);
    assert_eq!(
        metric(&f, "extent_queries"),
        queries + 1,
        "positive rows are not negative-cache entries"
    );
    f.vfs
        .workspace_hole_fallocate_from_fuse(fh, 400, 80, 32, true)
        .await
        .unwrap();
    expected[80..112].fill(0);
    assert_eq!(read(&f, 0, 256).await, expected);
    f.vfs.truncate_inode(400, 64).await.unwrap();
    f.vfs.truncate_inode(400, 256).await.unwrap();
    expected[64..].fill(0);
    assert_eq!(read(&f, 0, 256).await, expected);
    f.vfs.close(fh).await.unwrap();
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn empty_query_cache_does_not_hide_directory_edits_or_whiteouts() {
    let f = fixture_with_lease(false, packed().await, Some(lease())).await;
    names(&f).await;
    names(&f).await;
    let ino = f.meta.create_file(1, "new".into()).await.unwrap();
    assert_eq!(names(&f).await, vec![b"new".to_vec(), b"nonzero".to_vec()]);
    f.meta.rename(1, "new", 1, "renamed".into()).await.unwrap();
    f.meta.link(ino, 1, "alias").await.unwrap();
    f.meta.unlink(1, "nonzero").await.unwrap();
    assert_eq!(
        names(&f).await,
        vec![b"alias".to_vec(), b"renamed".to_vec()]
    );
    let queries = metric(&f, "directory_queries");
    assert_eq!(
        names(&f).await,
        vec![b"alias".to_vec(), b"renamed".to_vec()]
    );
    assert_eq!(
        metric(&f, "directory_queries"),
        queries + 2,
        "whiteouts must keep the upper merge active"
    );
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn empty_query_cache_view_replacement_forces_refill() {
    let f = fixture_with_lease(false, packed().await, Some(lease())).await;
    read(&f, 0, 64).await;
    read(&f, 0, 64).await;
    names(&f).await;
    names(&f).await;
    let extent = metric(&f, "extent_queries");
    let directory = metric(&f, "directory_queries");
    f.meta
        .replace_view_context(f.meta.view_context().await)
        .await;
    read(&f, 0, 64).await;
    names(&f).await;
    assert_eq!(metric(&f, "extent_queries"), extent + 1);
    assert_eq!(metric(&f, "directory_queries"), directory + 2);
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn empty_query_cache_requires_an_authoritative_local_lease() {
    let f = fixture(false).await;
    read(&f, 0, 64).await;
    read(&f, 0, 64).await;
    names(&f).await;
    names(&f).await;
    assert_eq!(metric(&f, "extent_queries"), 2);
    assert_eq!(metric(&f, "extent_empty_cache_hits"), 0);
    assert_eq!(metric(&f, "directory_queries"), 4);
    assert_eq!(metric(&f, "directory_empty_cache_hits"), 0);
    f.reader.shutdown().await.unwrap();
}
