use crate::workspace::error::WorkspaceError;
use crate::workspace::ids::LayerId;
use crate::workspace::model::{InodeDelta, InodeState, LayerRecord};

use super::{Resolution, validate_layer_chain};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedInode {
    pub layer_id: LayerId,
    pub inode: InodeDelta,
}

pub fn resolve_inode(
    chain: &[LayerRecord],
    deltas: &[InodeDelta],
    ino: i64,
) -> Result<Option<ResolvedInode>, WorkspaceError> {
    resolve_inode_state(chain, deltas, ino).map(Resolution::into_option)
}

pub fn resolve_inode_state(
    chain: &[LayerRecord],
    deltas: &[InodeDelta],
    ino: i64,
) -> Result<Resolution<ResolvedInode>, WorkspaceError> {
    let head = chain
        .first()
        .ok_or_else(|| WorkspaceError::CorruptMetadata("empty layer chain".into()))?;
    validate_layer_chain(head.layer_id, chain)?;
    for layer in chain {
        let winner = deltas
            .iter()
            .filter(|delta| delta.layer_id == layer.layer_id && delta.ino == ino)
            .max_by_key(|delta| delta.sequence);
        if let Some(winner) = winner {
            return match winner.state {
                InodeState::Deleted => Ok(Resolution::Masked),
                InodeState::Present => Ok(Resolution::Present(ResolvedInode {
                    layer_id: layer.layer_id,
                    inode: winner.clone(),
                })),
            };
        }
    }
    Ok(Resolution::Absent)
}
