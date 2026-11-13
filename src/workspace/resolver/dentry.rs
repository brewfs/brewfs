use std::collections::BTreeMap;

use crate::workspace::error::WorkspaceError;
use crate::workspace::ids::LayerId;
use crate::workspace::model::{DentryDelta, DentryOp, LayerRecord};

use super::{Resolution, validate_layer_chain};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedDentry {
    pub layer_id: LayerId,
    pub parent_ino: i64,
    pub name: Vec<u8>,
    pub ino: i64,
    pub entry_type: u8,
    pub sequence: u64,
}

fn newest_in_layer<'a>(
    deltas: impl Iterator<Item = &'a DentryDelta>,
) -> Result<Option<&'a DentryDelta>, WorkspaceError> {
    let mut winner: Option<&DentryDelta> = None;
    for delta in deltas {
        delta.validate()?;
        if winner.is_none_or(|current| delta.sequence > current.sequence) {
            winner = Some(delta);
        }
    }
    Ok(winner)
}

fn materialize(delta: &DentryDelta) -> Result<Resolution<ResolvedDentry>, WorkspaceError> {
    match delta.op {
        DentryOp::Whiteout => Ok(Resolution::Masked),
        DentryOp::Put => Ok(Resolution::Present(ResolvedDentry {
            layer_id: delta.layer_id,
            parent_ino: delta.parent_ino,
            name: delta.name.clone(),
            ino: delta.ino.ok_or_else(|| {
                WorkspaceError::CorruptMetadata("put dentry without inode".into())
            })?,
            entry_type: delta.entry_type.ok_or_else(|| {
                WorkspaceError::CorruptMetadata("put dentry without entry type".into())
            })?,
            sequence: delta.sequence,
        })),
    }
}

pub fn resolve_dentry(
    chain: &[LayerRecord],
    deltas: &[DentryDelta],
    parent_ino: i64,
    name: &[u8],
) -> Result<Option<ResolvedDentry>, WorkspaceError> {
    resolve_dentry_state(chain, deltas, parent_ino, name).map(Resolution::into_option)
}

pub fn resolve_dentry_state(
    chain: &[LayerRecord],
    deltas: &[DentryDelta],
    parent_ino: i64,
    name: &[u8],
) -> Result<Resolution<ResolvedDentry>, WorkspaceError> {
    let head = chain
        .first()
        .ok_or_else(|| WorkspaceError::CorruptMetadata("empty layer chain".into()))?;
    validate_layer_chain(head.layer_id, chain)?;
    for layer in chain {
        let winner = newest_in_layer(deltas.iter().filter(|delta| {
            delta.layer_id == layer.layer_id && delta.parent_ino == parent_ino && delta.name == name
        }))?;
        if let Some(winner) = winner {
            return materialize(winner);
        }
    }
    Ok(Resolution::Absent)
}

pub fn resolve_directory(
    chain: &[LayerRecord],
    deltas: &[DentryDelta],
    parent_ino: i64,
) -> Result<Vec<ResolvedDentry>, WorkspaceError> {
    Ok(resolve_directory_state(chain, deltas, parent_ino)?
        .into_values()
        .filter_map(Resolution::into_option)
        .collect())
}

/// Preserve every winning name, including masks, when merging a packed lower
/// directory. A name absent from this map has no upper winner.
pub fn resolve_directory_state(
    chain: &[LayerRecord],
    deltas: &[DentryDelta],
    parent_ino: i64,
) -> Result<BTreeMap<Vec<u8>, Resolution<ResolvedDentry>>, WorkspaceError> {
    let head = chain
        .first()
        .ok_or_else(|| WorkspaceError::CorruptMetadata("empty layer chain".into()))?;
    validate_layer_chain(head.layer_id, chain)?;
    let mut winners = BTreeMap::new();
    for layer in chain {
        let mut layer_rows: BTreeMap<Vec<u8>, &DentryDelta> = BTreeMap::new();
        for delta in deltas
            .iter()
            .filter(|delta| delta.layer_id == layer.layer_id && delta.parent_ino == parent_ino)
        {
            delta.validate()?;
            let replace = layer_rows
                .get(&delta.name)
                .is_none_or(|current| delta.sequence > current.sequence);
            if replace {
                layer_rows.insert(delta.name.clone(), delta);
            }
        }
        for (name, delta) in layer_rows {
            if let std::collections::btree_map::Entry::Vacant(entry) = winners.entry(name) {
                entry.insert(materialize(delta)?);
            }
        }
    }
    Ok(winners)
}
