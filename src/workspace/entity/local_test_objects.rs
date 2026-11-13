//! Test-only real filesystem repository with an explicit directory durability
//! barrier. The production LocalFs adapter's weaker contract is unchanged.

use crate::cadapter::client::{ObjectBackend, ObjectByteStream};
use crate::cadapter::localfs::LocalFsBackend;
use async_trait::async_trait;
use std::path::{Path, PathBuf};

pub(crate) struct DurableLocalObjects {
    inner: LocalFsBackend,
    root: PathBuf,
}
impl DurableLocalObjects {
    pub(crate) fn new(path: impl AsRef<Path>) -> Self {
        Self {
            inner: LocalFsBackend::new(path.as_ref()),
            root: path.as_ref().into(),
        }
    }
}

#[async_trait]
impl ObjectBackend for DurableLocalObjects {
    fn forbids_mutation_replay(&self) -> bool {
        true
    }
    async fn put_object(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
        self.inner.put_object(key, bytes).await
    }
    async fn put_object_create_only_durable(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
        self.inner.put_object_create_only(key, bytes).await?;
        let path = self.root.join(key);
        for parent in path.ancestors().skip(1) {
            std::fs::File::open(parent)?.sync_all()?;
            if parent == self.root {
                break;
            }
        }
        Ok(())
    }
    async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner.get_object(key).await
    }
    async fn get_object_range(
        &self,
        key: &str,
        offset: u64,
        output: &mut [u8],
    ) -> anyhow::Result<usize> {
        self.inner.get_object_range(key, offset, output).await
    }
    async fn get_object_range_stream(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> anyhow::Result<ObjectByteStream> {
        self.inner
            .get_object_range_stream(key, offset, length)
            .await
    }
    async fn get_object_size_bounded(&self, key: &str) -> anyhow::Result<Option<u64>> {
        self.inner.get_object_size_bounded(key).await
    }
    async fn get_etag(&self, key: &str) -> anyhow::Result<String> {
        self.inner.get_etag(key).await
    }
    async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
        self.inner.delete_object(key).await
    }
}
