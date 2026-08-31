use std::collections::BTreeMap;

use crate::workspace_overlay::error::WorkspaceError;
use crate::workspace_overlay::ids::LayerId;
use crate::workspace_overlay::model::{DentryDelta, DentryOp, LayerRecord};

use super::validate_layer_chain;

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

fn materialize(delta: &DentryDelta) -> Result<Option<ResolvedDentry>, WorkspaceError> {
    match delta.op {
        DentryOp::Whiteout => Ok(None),
        DentryOp::Put => Ok(Some(ResolvedDentry {
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
    Ok(None)
}

pub fn resolve_directory(
    chain: &[LayerRecord],
    deltas: &[DentryDelta],
    parent_ino: i64,
) -> Result<Vec<ResolvedDentry>, WorkspaceError> {
    let head = chain
        .first()
        .ok_or_else(|| WorkspaceError::CorruptMetadata("empty layer chain".into()))?;
    validate_layer_chain(head.layer_id, chain)?;
    let mut winners: BTreeMap<Vec<u8>, Option<ResolvedDentry>> = BTreeMap::new();
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
    Ok(winners.into_values().flatten().collect())
}
