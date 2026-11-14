//! Monotonic slice ranges retained after a referencing layer is collected.
use super::*;

#[derive(Serialize, Deserialize)]
struct SliceBound {
    slice: u64,
    end: u64,
}

fn key(slice: u64) -> Vec<u8> {
    format!("native/gc-slice-bound/{slice:016x}").into_bytes()
}

fn bound(slice: u64, raw: &[u8]) -> Result<u64, WorkspaceError> {
    let row: SliceBound = decode_open_value(raw, 256)?;
    if row.slice != slice || row.end == 0 {
        return Err(WorkspaceError::CorruptMetadata(
            "invalid GC slice bound".into(),
        ));
    }
    Ok(row.end)
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    pub(super) async fn retain_native_gc_bound(
        &self,
        slice: u64,
        end: u64,
    ) -> Result<(), WorkspaceError> {
        self.require_admin_access()?;
        if slice == 0 || end == 0 {
            return Err(WorkspaceError::CorruptMetadata(
                "invalid GC slice range".into(),
            ));
        }
        let key = key(slice);
        for attempt in 0..CAS_MAX_RETRIES {
            let raw = self.backend.get(&key).await?;
            if let Some(raw) = &raw
                && bound(slice, raw)? >= end
            {
                self.backend.flush_metadata().await?;
                return Ok(());
            }
            let write = put(key.clone(), &SliceBound { slice, end })?;
            if self
                .backend
                .compare_and_swap(
                    &[KvCheck {
                        key: key.clone(),
                        expected: raw,
                    }],
                    &[write],
                )
                .await?
            {
                self.backend.flush_metadata().await?;
                return Ok(());
            }
            retry_backoff(attempt).await;
        }
        Err(WorkspaceError::Busy)
    }

    pub(super) async fn read_native_gc_bound(
        &self,
        slice: u64,
        observed: u64,
    ) -> Result<u64, WorkspaceError> {
        self.require_admin_access()?;
        if slice == 0 || observed == 0 {
            return Err(WorkspaceError::CorruptMetadata(
                "invalid GC slice range".into(),
            ));
        }
        let raw = self.backend.get(&key(slice)).await?.ok_or_else(|| {
            WorkspaceError::CorruptMetadata("GC slice bound has not been retained".into())
        })?;
        let end = bound(slice, &raw)?;
        if end < observed {
            return Err(WorkspaceError::Busy);
        }
        self.backend.flush_metadata().await?;
        Ok(end)
    }
}
