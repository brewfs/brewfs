use crate::workspace_overlay::error::WorkspaceError;
use crate::workspace_overlay::ids::LayerId;
use crate::workspace_overlay::model::{AclDelta, LayerRecord, ValueOp, XattrDelta};

use super::validate_layer_chain;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedXattr {
    pub layer_id: LayerId,
    pub value: Vec<u8>,
    pub sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedAcl {
    pub layer_id: LayerId,
    pub value: Vec<u8>,
    pub sequence: u64,
}

pub fn resolve_xattr(
    chain: &[LayerRecord],
    deltas: &[XattrDelta],
    ino: i64,
    name: &[u8],
) -> Result<Option<ResolvedXattr>, WorkspaceError> {
    let head = chain
        .first()
        .ok_or_else(|| WorkspaceError::CorruptMetadata("empty layer chain".into()))?;
    validate_layer_chain(head.layer_id, chain)?;
    for layer in chain {
        let winner = deltas
            .iter()
            .filter(|delta| {
                delta.layer_id == layer.layer_id && delta.ino == ino && delta.name == name
            })
            .max_by_key(|delta| delta.sequence);
        if let Some(winner) = winner {
            return resolve_value(winner.op, winner.value.as_ref()).map(|value| {
                value.map(|value| ResolvedXattr {
                    layer_id: layer.layer_id,
                    value: value.clone(),
                    sequence: winner.sequence,
                })
            });
        }
    }
    Ok(None)
}

pub fn resolve_acl(
    chain: &[LayerRecord],
    deltas: &[AclDelta],
    ino: i64,
    acl_type: u8,
    acl_id: i64,
) -> Result<Option<ResolvedAcl>, WorkspaceError> {
    let head = chain
        .first()
        .ok_or_else(|| WorkspaceError::CorruptMetadata("empty layer chain".into()))?;
    validate_layer_chain(head.layer_id, chain)?;
    for layer in chain {
        let winner = deltas
            .iter()
            .filter(|delta| {
                delta.layer_id == layer.layer_id
                    && delta.ino == ino
                    && delta.acl_type == acl_type
                    && delta.acl_id == acl_id
            })
            .max_by_key(|delta| delta.sequence);
        if let Some(winner) = winner {
            return resolve_value(winner.op, winner.value.as_ref()).map(|value| {
                value.map(|value| ResolvedAcl {
                    layer_id: layer.layer_id,
                    value: value.clone(),
                    sequence: winner.sequence,
                })
            });
        }
    }
    Ok(None)
}

fn resolve_value(op: ValueOp, value: Option<&Vec<u8>>) -> Result<Option<&Vec<u8>>, WorkspaceError> {
    match (op, value) {
        (ValueOp::Put, Some(value)) => Ok(Some(value)),
        (ValueOp::Whiteout, None) => Ok(None),
        _ => Err(WorkspaceError::CorruptMetadata(
            "value op/payload mismatch".into(),
        )),
    }
}
