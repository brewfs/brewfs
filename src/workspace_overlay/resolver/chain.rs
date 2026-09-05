use std::collections::HashSet;

use crate::workspace_overlay::error::WorkspaceError;
use crate::workspace_overlay::ids::LayerId;
use crate::workspace_overlay::model::{
    LAYER_CHAIN_HARD_LIMIT, LayerRecord, LayerState, WORKSPACE_SCHEMA_VERSION,
};

pub fn validate_layer_chain(
    expected_head: LayerId,
    chain: &[LayerRecord],
) -> Result<(), WorkspaceError> {
    if chain.is_empty() {
        return Err(WorkspaceError::LayerNotFound(expected_head));
    }
    if chain.len() > LAYER_CHAIN_HARD_LIMIT as usize {
        return Err(WorkspaceError::LayerDepthLimit {
            depth: chain.len() as u32,
            hard_limit: LAYER_CHAIN_HARD_LIMIT,
        });
    }
    if chain[0].layer_id != expected_head {
        return Err(WorkspaceError::CorruptMetadata(format!(
            "chain starts at {}, expected {expected_head}",
            chain[0].layer_id
        )));
    }
    if !matches!(chain[0].state, LayerState::Writable | LayerState::Sealed) {
        return Err(WorkspaceError::CorruptMetadata(format!(
            "head layer {} is not resolvable in state {:?}",
            chain[0].layer_id, chain[0].state
        )));
    }

    let mut seen = HashSet::with_capacity(chain.len());
    for (index, layer) in chain.iter().enumerate() {
        if !seen.insert(layer.layer_id) {
            return Err(WorkspaceError::CorruptMetadata(format!(
                "layer chain contains cycle at {}",
                layer.layer_id
            )));
        }
        if layer.schema_version != WORKSPACE_SCHEMA_VERSION {
            return Err(WorkspaceError::UnsupportedSchemaVersion(
                layer.schema_version,
            ));
        }
        let expected_depth = (chain.len() - index) as u32;
        if layer.depth != expected_depth {
            return Err(WorkspaceError::CorruptMetadata(format!(
                "layer {} has depth {}, expected {expected_depth}",
                layer.layer_id, layer.depth
            )));
        }
        if index > 0 && layer.state != LayerState::Sealed {
            return Err(WorkspaceError::CorruptMetadata(format!(
                "non-head layer {} is not sealed",
                layer.layer_id
            )));
        }
        if layer.state == LayerState::Sealed
            && (layer.sealed_version.is_none()
                || layer.delta_digest.is_none()
                || layer.root_hash.is_none())
        {
            return Err(WorkspaceError::CorruptMetadata(format!(
                "sealed layer {} is missing immutable revision fields",
                layer.layer_id
            )));
        }
        match chain.get(index + 1) {
            Some(parent) if layer.parent_layer_id == Some(parent.layer_id) => {}
            Some(parent) => {
                return Err(WorkspaceError::CorruptMetadata(format!(
                    "layer {} points to {:?}, expected parent {}",
                    layer.layer_id, layer.parent_layer_id, parent.layer_id
                )));
            }
            None if layer.parent_layer_id.is_none() => {}
            None => {
                return Err(WorkspaceError::CorruptMetadata(format!(
                    "root layer {} has a parent",
                    layer.layer_id
                )));
            }
        }
    }
    Ok(())
}
