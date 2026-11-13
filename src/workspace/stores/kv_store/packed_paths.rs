//! Global native delta pages for bounded packed reverse-name reconstruction.

use super::*;
use crate::workspace::packed::snapshot::SnapshotMountBudget;
use crate::workspace::stores::kv_backend::KvReadLimits;

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    pub(super) async fn bounded_layer_dentry_page(
        &self,
        layer: LayerId,
        after: Option<(i64, &[u8])>,
        budget: Arc<SnapshotMountBudget>,
    ) -> Result<WorkspaceNamePage<DentryDelta>, WorkspaceError> {
        if after.is_some_and(|(parent, name)| parent <= 0 || !valid_name(name)) {
            return Err(WorkspaceError::InvalidReadPlan(
                "invalid reverse delta cursor".into(),
            ));
        }
        self.bounded_name_page(
            dentry_layer_prefix(layer),
            after.map(|(parent, name)| dentry_identity_key(layer, parent, name)),
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
                    || row.parent_ino <= 0
                    || !valid_name(&row.name)
                    || row.ino.is_some_and(|ino| ino <= 0)
                {
                    return Err(WorkspaceError::CorruptMetadata(
                        "invalid reverse delta row".into(),
                    ));
                }
                Ok(dentry_key(row))
            },
        )
        .await
    }
}

fn valid_name(name: &[u8]) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && name != b"."
        && name != b".."
        && !name.contains(&0)
        && !name.contains(&b'/')
}
