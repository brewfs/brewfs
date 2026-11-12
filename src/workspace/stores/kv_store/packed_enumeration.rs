//! Byte-bounded native pages for packed workspace namespace mask merges.

use super::*;
use crate::workspace::packed::snapshot::{SnapshotBudgetPool, SnapshotMountBudget};
use crate::workspace::stores::kv_backend::KvReadLimits;

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    pub(super) async fn bounded_name_page<T: DeserializeOwned>(
        &self,
        prefix: Vec<u8>,
        after: Option<Vec<u8>>,
        limits: KvReadLimits,
        budget: Arc<SnapshotMountBudget>,
        identity: impl Fn(&T) -> Result<Vec<u8>, WorkspaceError> + Send + Sync,
    ) -> Result<WorkspaceNamePage<T>, WorkspaceError> {
        if self
            .packed_reader_pin_budget
            .get()
            .is_none_or(|owned| !Arc::ptr_eq(owned, &budget))
        {
            return Err(WorkspaceError::InvalidReadPlan(
                "namespace page mount budget mismatch".into(),
            ));
        }
        let permit = budget
            .admit(&[(SnapshotBudgetPool::Metadata, 512 << 10)])
            .map_err(|_| WorkspaceError::Io(std::io::Error::from_raw_os_error(libc::ENOMEM)))?;
        let entries = self
            .backend
            .scan_prefix_page_with_byte_limits(&prefix, after.as_deref(), limits)
            .await?;
        let end = self.backend.short_scan_page_is_end() && entries.len() < limits.max_records;
        let mut rows = Vec::with_capacity(entries.len());
        let mut previous = after.unwrap_or_else(|| prefix.clone());
        for entry in entries {
            if !entry.key.starts_with(&prefix) || entry.key <= previous {
                return Err(WorkspaceError::CorruptMetadata(
                    "namespace page key order/prefix mismatch".into(),
                ));
            }
            let row: T = decode_open_value(&entry.value, limits.max_value_bytes)?;
            if identity(&row)? != entry.key {
                return Err(WorkspaceError::CorruptMetadata(
                    "namespace page row/key mismatch".into(),
                ));
            }
            previous = entry.key;
            rows.push(row);
        }
        Ok(WorkspaceNamePage {
            rows,
            memory_guard: Arc::new(permit),
            end,
        })
    }

    pub(super) async fn bounded_dentry_page(
        &self,
        layer: LayerId,
        parent: i64,
        after_name: Option<&[u8]>,
        budget: Arc<SnapshotMountBudget>,
    ) -> Result<WorkspaceNamePage<DentryDelta>, WorkspaceError> {
        if parent <= 0 || after_name.is_some_and(|name| !valid_dentry_name(name)) {
            return Err(WorkspaceError::InvalidReadPlan(
                "invalid dentry page identity/cursor".into(),
            ));
        }
        self.bounded_name_page(
            dentry_parent_prefix(layer, parent),
            after_name.map(|name| dentry_identity_key(layer, parent, name)),
            KvReadLimits {
                max_records: 32,
                max_key_bytes: 1024,
                max_value_bytes: 4096,
                max_total_bytes: 160 << 10,
                max_response_bytes: 192 << 10,
                max_data_requests: 32,
            },
            budget,
            move |row: &DentryDelta| {
                row.validate()?;
                if row.layer_id != layer
                    || row.parent_ino != parent
                    || !valid_dentry_name(&row.name)
                    || row.ino.is_some_and(|ino| ino <= 0)
                {
                    return Err(WorkspaceError::CorruptMetadata(
                        "invalid bounded dentry row".into(),
                    ));
                }
                Ok(dentry_key(row))
            },
        )
        .await
    }

    pub(super) async fn bounded_xattr_page(
        &self,
        layer: LayerId,
        ino: i64,
        after_name: Option<&[u8]>,
        budget: Arc<SnapshotMountBudget>,
    ) -> Result<WorkspaceNamePage<XattrDelta>, WorkspaceError> {
        if ino <= 0 || after_name.is_some_and(|name| !valid_xattr_name(name)) {
            return Err(WorkspaceError::InvalidReadPlan(
                "invalid xattr page identity/cursor".into(),
            ));
        }
        // One legal 64 KiB xattr per page, including its native envelope.
        self.bounded_name_page(
            xattr_inode_prefix(layer, ino),
            after_name.map(|name| xattr_identity_key(layer, ino, name)),
            KvReadLimits {
                max_records: 1,
                max_key_bytes: 1024,
                max_value_bytes: 96 << 10,
                max_total_bytes: 97 << 10,
                max_response_bytes: 128 << 10,
                max_data_requests: 32,
            },
            budget,
            move |row: &XattrDelta| {
                if row.layer_id != layer
                    || row.ino != ino
                    || !valid_xattr_name(&row.name)
                    || row.value.as_ref().is_some_and(|value| value.len() > 65536)
                    || !matches!(
                        (row.op, row.value.is_some()),
                        (ValueOp::Put, true) | (ValueOp::Whiteout, false)
                    )
                {
                    return Err(WorkspaceError::CorruptMetadata(
                        "invalid bounded xattr row".into(),
                    ));
                }
                Ok(xattr_key(row))
            },
        )
        .await
    }
}

fn valid_dentry_name(name: &[u8]) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && name != b"."
        && name != b".."
        && !name.contains(&0)
        && !name.contains(&b'/')
}
fn valid_xattr_name(name: &[u8]) -> bool {
    !name.is_empty() && name.len() <= 255 && !name.contains(&0)
}
