use crate::workspace_overlay::error::WorkspaceError;
use crate::workspace_overlay::ids::LayerId;
use crate::workspace_overlay::model::{InodeDelta, InodeState, LayerRecord};

use super::validate_layer_chain;

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
                InodeState::Deleted => Ok(None),
                InodeState::Present => Ok(Some(ResolvedInode {
                    layer_id: layer.layer_id,
                    inode: winner.clone(),
                })),
            };
        }
    }
    Ok(None)
}
