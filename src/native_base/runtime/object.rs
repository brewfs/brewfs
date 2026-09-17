use async_trait::async_trait;
use sha2::{Digest, Sha256};

use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::native_base::wire::refs::ObjectRef;
use crate::native_base::write::receipts::ObjectSink;

/// Native immutable-object repository backed by BrewFS' production object
/// client. Every PUT is atomic create-only and is followed by exact readback
/// before the write pipeline may produce a receipt.
pub struct BackendObjectRepository<B: ObjectBackend> {
    client: ObjectClient<B>,
}

impl<B: ObjectBackend> BackendObjectRepository<B> {
    pub fn new(client: ObjectClient<B>) -> Self {
        Self { client }
    }

    fn key(object: &ObjectRef) -> anyhow::Result<&str> {
        std::str::from_utf8(&object.key)
            .map_err(|_| anyhow::anyhow!("native object key is not UTF-8"))
    }

    fn verify(object: &ObjectRef, bytes: &[u8]) -> anyhow::Result<()> {
        if bytes.len() as u64 != object.object_len {
            anyhow::bail!(
                "native object length mismatch: expected {}, got {}",
                object.object_len,
                bytes.len()
            )
        }
        let digest: [u8; 32] = Sha256::digest(bytes).into();
        if digest != object.full_hash {
            anyhow::bail!("native object digest mismatch")
        }
        Ok(())
    }
}

#[async_trait]
impl<B> ObjectSink for BackendObjectRepository<B>
where
    B: ObjectBackend + Send + Sync,
{
    async fn put(&self, object: &ObjectRef, bytes: &[u8]) -> anyhow::Result<()> {
        Self::verify(object, bytes)?;
        let key = Self::key(object)?;
        if let Err(error) = self.client.put_object_create_only(key, bytes).await {
            let existing = self.client.get_object(key).await?.ok_or(error)?;
            Self::verify(object, &existing)?;
            if existing != bytes {
                anyhow::bail!("create-only conflict for native object {key}")
            }
        }
        let readback = self
            .client
            .get_object(key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("native object disappeared after PUT: {key}"))?;
        Self::verify(object, &readback)?;
        if readback != bytes {
            anyhow::bail!("native object exact readback differs after PUT: {key}")
        }
        Ok(())
    }

    async fn get(&self, object: &ObjectRef) -> anyhow::Result<Vec<u8>> {
        let key = Self::key(object)?;
        let bytes = self
            .client
            .get_object(key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("native object is missing: {key}"))?;
        Self::verify(object, &bytes)?;
        Ok(bytes)
    }
}
