use super::*;
#[tokio::test]
async fn single_source_external_admission_uses_producer_input_encoding() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("hole");
    std::fs::File::create(&path)
        .unwrap()
        .set_len(72 * 1024 * 1024)
        .unwrap();
    let source = CapturedSource::capture(
        &path,
        temp.path(),
        2,
        AccessProfile::RandomSmallFile,
        SizeClassTable::default(),
        SnapshotSourceFileLimits::default(),
    )
    .await
    .unwrap();
    assert!(matches!(&source, CapturedSource::External(_)));
    let group = source
        .group(1, [2; 32], AccessProfile::RandomSmallFile)
        .unwrap();
    let metadata = GroupMeta::decode(&group.metadata).unwrap();
    assert_eq!(metadata.entries()[0].size, 72 * 1024 * 1024);
    assert!(metadata.entries()[0].extents.is_empty());
}
