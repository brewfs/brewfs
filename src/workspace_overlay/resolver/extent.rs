use std::collections::HashSet;
use std::ops::Range;

use crate::workspace_overlay::error::WorkspaceError;
use crate::workspace_overlay::model::{DataExtentDelta, ExtentKind, LayerRecord};

use super::validate_layer_chain;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedExtent {
    pub logical_offset: u64,
    pub length: u64,
    pub kind: ExtentKind,
}

impl ResolvedExtent {
    fn end(&self) -> Result<u64, WorkspaceError> {
        self.logical_offset.checked_add(self.length).ok_or_else(|| {
            WorkspaceError::CorruptMetadata("resolved extent range overflows".into())
        })
    }
}

pub fn resolve_extents(
    chain: &[LayerRecord],
    deltas: &[DataExtentDelta],
    ino: i64,
    chunk_index: u64,
    requested: Range<u64>,
) -> Result<Vec<ResolvedExtent>, WorkspaceError> {
    let head = chain
        .first()
        .ok_or_else(|| WorkspaceError::CorruptMetadata("empty layer chain".into()))?;
    validate_layer_chain(head.layer_id, chain)?;
    if requested.start > requested.end {
        return Err(WorkspaceError::InvalidReadPlan(
            "requested range starts after its end".into(),
        ));
    }
    if requested.is_empty() {
        return Ok(Vec::new());
    }

    let chain_ids: HashSet<_> = chain.iter().map(|layer| layer.layer_id).collect();
    for delta in deltas.iter().filter(|delta| {
        chain_ids.contains(&delta.layer_id) && delta.ino == ino && delta.chunk_index == chunk_index
    }) {
        delta.validate()?;
    }

    let mut uncovered = vec![requested.clone()];
    let mut resolved = Vec::new();
    for layer in chain {
        let mut layer_extents = deltas
            .iter()
            .filter(|delta| {
                delta.layer_id == layer.layer_id
                    && delta.ino == ino
                    && delta.chunk_index == chunk_index
            })
            .collect::<Vec<_>>();
        layer_extents.sort_by_key(|extent| std::cmp::Reverse(extent.sequence));

        let mut sequences = HashSet::with_capacity(layer_extents.len());
        for extent in layer_extents {
            if !sequences.insert(extent.sequence) {
                return Err(WorkspaceError::CorruptMetadata(format!(
                    "duplicate extent sequence {} in layer {}",
                    extent.sequence, extent.layer_id
                )));
            }
            if uncovered.is_empty() {
                break;
            }
            let extent_end = extent
                .logical_offset
                .checked_add(extent.length)
                .ok_or_else(|| {
                    WorkspaceError::CorruptMetadata("data extent logical range overflows".into())
                })?;
            let extent_range = extent.logical_offset..extent_end;
            let mut remaining = Vec::with_capacity(uncovered.len() + 1);
            for gap in uncovered {
                let start = gap.start.max(extent_range.start);
                let end = gap.end.min(extent_range.end);
                if start >= end {
                    remaining.push(gap);
                    continue;
                }
                if gap.start < start {
                    remaining.push(gap.start..start);
                }
                if end < gap.end {
                    remaining.push(end..gap.end);
                }

                let kind = match extent.kind {
                    ExtentKind::Data {
                        slice_id,
                        slice_offset,
                    } => {
                        let clipped =
                            start.checked_sub(extent.logical_offset).ok_or_else(|| {
                                WorkspaceError::CorruptMetadata(
                                    "extent intersection precedes logical start".into(),
                                )
                            })?;
                        ExtentKind::Data {
                            slice_id,
                            slice_offset: slice_offset.checked_add(clipped).ok_or_else(|| {
                                WorkspaceError::CorruptMetadata(
                                    "resolved slice offset overflows".into(),
                                )
                            })?,
                        }
                    }
                    ExtentKind::Hole => ExtentKind::Hole,
                };
                resolved.push(ResolvedExtent {
                    logical_offset: start,
                    length: end - start,
                    kind,
                });
            }
            uncovered = remaining;
        }
    }

    resolved.extend(uncovered.into_iter().map(|gap| ResolvedExtent {
        logical_offset: gap.start,
        length: gap.end - gap.start,
        kind: ExtentKind::Hole,
    }));
    resolved.sort_by_key(|extent| extent.logical_offset);
    merge_adjacent(resolved)
}

fn merge_adjacent(extents: Vec<ResolvedExtent>) -> Result<Vec<ResolvedExtent>, WorkspaceError> {
    let mut merged: Vec<ResolvedExtent> = Vec::with_capacity(extents.len());
    for extent in extents {
        if extent.length == 0 {
            return Err(WorkspaceError::CorruptMetadata(
                "resolver emitted a zero-length extent".into(),
            ));
        }
        if let Some(previous) = merged.last_mut() {
            let previous_end = previous.end()?;
            if extent.logical_offset < previous_end {
                return Err(WorkspaceError::InvalidReadPlan(
                    "resolved extents overlap".into(),
                ));
            }
            let can_merge = previous_end == extent.logical_offset
                && match (previous.kind, extent.kind) {
                    (ExtentKind::Hole, ExtentKind::Hole) => true,
                    (
                        ExtentKind::Data {
                            slice_id: left_id,
                            slice_offset: left_offset,
                        },
                        ExtentKind::Data {
                            slice_id: right_id,
                            slice_offset: right_offset,
                        },
                    ) => {
                        left_id == right_id
                            && left_offset
                                .checked_add(previous.length)
                                .is_some_and(|end| end == right_offset)
                    }
                    _ => false,
                };
            if can_merge {
                previous.length = previous.length.checked_add(extent.length).ok_or_else(|| {
                    WorkspaceError::CorruptMetadata("merged extent length overflows".into())
                })?;
                continue;
            }
        }
        merged.push(extent);
    }
    Ok(merged)
}
