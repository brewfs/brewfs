//! Slice ownership outlives a fully masked Data row, until layer GC completes.
use super::*;

pub(super) const EXTENT_OWNER_PREFIX: &[u8] = b"delta/extent-owner/";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct ExtentSliceOwner {
    pub layer_id: LayerId,
    pub slice_id: u64,
    pub slice_end: u64,
}

pub(super) fn extent_owner_layer_prefix(layer: LayerId) -> Vec<u8> {
    format!("delta/extent-owner/{}/", id_component(layer)).into_bytes()
}

impl ExtentSliceOwner {
    pub(super) fn key(&self) -> Vec<u8> {
        let mut key = extent_owner_layer_prefix(self.layer_id);
        key.extend_from_slice(u64_component(self.slice_id).as_bytes());
        key
    }

    fn decode(key: &[u8], value: &[u8]) -> Result<Self, WorkspaceError> {
        let row: Self = decode_open_value(value, 512)?;
        if row.key() != key || row.slice_end == 0 || encode(&row)? != value {
            return Err(WorkspaceError::CorruptMetadata(
                "extent ownership key/record identity mismatch".into(),
            ));
        }
        Ok(row)
    }
}

/// Both the observational census and destructive GC proof use this decoder.
pub(super) fn decode_extent_reference(
    key: &[u8],
    value: &[u8],
) -> Result<Option<SliceReference>, WorkspaceError> {
    if key.starts_with(EXTENT_OWNER_PREFIX) {
        let row = ExtentSliceOwner::decode(key, value)?;
        return Ok(Some(SliceReference {
            layer_id: row.layer_id,
            slice_id: row.slice_id,
            slice_end: row.slice_end,
        }));
    }
    let row: DataExtentDelta = decode_open_value(value, 512)?;
    row.validate()?;
    if extent_key(&row) != key {
        return Err(WorkspaceError::CorruptMetadata(
            "extent key/record identity mismatch".into(),
        ));
    }
    Ok(match row.kind {
        ExtentKind::Hole => None,
        ExtentKind::Data {
            slice_id,
            slice_offset,
        } => Some(SliceReference {
            layer_id: row.layer_id,
            slice_id,
            slice_end: slice_offset + row.length,
        }),
    })
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    /// The exact writable-head CAS serializes these monotone ownership rows;
    /// the native extent generation fences their movement against GC scans.
    pub(super) async fn retain_extent_ownership(
        &self,
        head: LayerId,
        slices: BTreeMap<u64, u64>,
        writes: &mut Vec<KvWrite>,
    ) -> Result<(), WorkspaceError> {
        let owners = slices
            .into_iter()
            .map(|(slice_id, slice_end)| ExtentSliceOwner {
                layer_id: head,
                slice_id,
                slice_end,
            })
            .collect::<Vec<_>>();
        for window in owners.chunks(32) {
            let keys = window.iter().map(ExtentSliceOwner::key).collect::<Vec<_>>();
            let (values, _) = self
                .backend
                .get_many_consistent_with_time_bounded(
                    &keys,
                    KvReadLimits {
                        max_records: keys.len(),
                        max_key_bytes: 1024,
                        max_value_bytes: 512,
                        max_total_bytes: 48 << 10,
                        max_response_bytes: 64 << 10,
                        max_data_requests: keys.len(),
                    },
                )
                .await?;
            if values.len() != keys.len() {
                return Err(WorkspaceError::CorruptMetadata(
                    "short extent ownership read".into(),
                ));
            }
            for ((owner, key), raw) in window.iter().zip(keys).zip(values) {
                if let Some(raw) = raw {
                    let old = ExtentSliceOwner::decode(&key, &raw)?;
                    if old.slice_end >= owner.slice_end {
                        continue;
                    }
                }
                writes.push(put(key, owner)?);
            }
        }
        Ok(())
    }
}
