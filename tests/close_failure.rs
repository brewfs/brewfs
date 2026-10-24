use brewfs::chunk::store::{BlockKey, BlockStore};
use brewfs::vfs::error::VfsError;
use brewfs::{ChunkLayout, VFS, create_meta_store_from_url};

struct FailingUploads;

#[async_trait::async_trait]
impl BlockStore for FailingUploads {
    async fn write_fresh_range(&self, _: BlockKey, _: u64, _: &[u8]) -> anyhow::Result<u64> {
        Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "injected upload failure",
        )
        .into())
    }

    async fn read_range(&self, _: BlockKey, _: u64, _: &mut [u8]) -> anyhow::Result<()> {
        anyhow::bail!("failed upload must not be read from the backend")
    }

    async fn delete_range(&self, _: BlockKey, _: u64) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn close_failure_retires_consumed_handle() {
    let metadata = create_meta_store_from_url("sqlite::memory:").await.unwrap();
    let fs = VFS::new(ChunkLayout::default(), FailingUploads, metadata.store())
        .await
        .unwrap();
    let ino = fs.create_file("/failed-close").await.unwrap();
    let attr = fs.stat("/failed-close").await.unwrap();
    let guard = fs.open_guard(ino, attr, true, true).await.unwrap();
    let fh = guard.fh();
    guard.write(0, b"pending data").await.unwrap();

    let error = guard
        .close()
        .await
        .expect_err("close must report the upload failure");
    eprintln!("close error: {error}");
    let read = fs.read(fh, 0, 1).await;
    assert!(
        matches!(read, Err(VfsError::StaleNetworkFileHandle)),
        "a consumed guard must retire its handle even when flush fails; read returned {read:?}"
    );
}
