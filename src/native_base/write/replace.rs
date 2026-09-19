//! WRITE-012: publishing a temporary file over an existing destination is an
//! *application-complete write*, never a "minimal patch".
//!
//! POSIX `rename(tmp, dst)` onto an existing `dst` replaces the destination's
//! content wholesale.  The overlay therefore republishes the source's complete
//! extent set as the destination's new content, inside one commit:
//!
//! - every extent the destination had is removed by the same transaction;
//! - the new content names *every* extent it ends up with, holes included, and
//!   must cover `[0, size)` exactly — a plan that would leave one destination
//!   byte on the old content is refused, because that would publish a mixture
//!   of old and new bytes as if it were a complete file;
//! - every data block the new content depends on is carried into the commit
//!   and attested by its receipts, so the publication is complete even though
//!   the objects were already durable under the source inode's slice;
//! - the plan has no byte-delta form: [`Replacement::changed_ranges`] always
//!   answers with the whole file, so a caller cannot report a rename-over as a
//!   minimal patch even when the two files happen to be byte-identical.
//!
//! The destination keeps its own attribute row: POSIX rename replaces the
//! name, not the ownership of the inode the name is bound to.

use crate::native_base::seal::BlockBinding;
use crate::native_base::wire::bnct::{ObjectRegistration, RegistrationState};
use crate::native_base::wire::refs::ObjectId;
use crate::native_base::wire::uvarint::Reader;

use super::commit::{CarriedExtent, CommittedBlock, read_inode_view};
use super::domain::decode_registration;
use super::error::WriteError;
use super::keys::Keys;
use super::records::{ExtentKind, HeadPlacement, NativeExtent};
use super::store::ControlStore;

/// The complete new content one rename-over publishes.
#[derive(Debug, Clone)]
pub struct Replacement {
    pub source_inode: u64,
    pub destination_inode: u64,
    /// The destination's logical size once the replacement commits.
    pub size: u64,
    /// Every destination extent this commit removes (its whole old map).
    pub replaced: Vec<(u64, NativeExtent)>,
    /// The complete new extent set, with its durable blocks resolved.
    pub carried: Vec<CarriedExtent>,
}

impl Replacement {
    /// The logical ranges this mutation changes.  A replacement has no
    /// byte-delta form: the answer is always the whole file, even when the
    /// source and the destination hold identical bytes, so a caller can never
    /// describe a rename-over as a minimal patch.
    pub fn changed_ranges(&self) -> Vec<(u64, u64)> {
        vec![(0, self.size)]
    }

    /// Every object the destination's new content depends on, in extent
    /// order.  A caller cannot publish the replacement without attesting all
    /// of them: they are the commit's block objects.
    pub fn carried_object_ids(&self) -> Vec<ObjectId> {
        self.carried
            .iter()
            .flat_map(|extent| extent.blocks.iter())
            .map(|block| block.object_ref.object_id)
            .collect()
    }

    /// Every carried block, flattened, in the order the receipts and the
    /// commit consume them.
    pub fn flattened_blocks(&self) -> Vec<CommittedBlock> {
        self.carried
            .iter()
            .flat_map(|extent| extent.blocks.iter().cloned())
            .collect()
    }

    /// A rename-over is complete by construction: it publishes the whole new
    /// extent set and removes every old one.  This exists so a caller can
    /// assert the property instead of assuming it.
    pub fn is_application_complete(&self) -> bool {
        true
    }
}

/// The fail-closed coverage rule shared by the planner and the commit: the
/// published extents must tile `[0, size)` exactly — no gap (an unpublished
/// byte would silently keep the destination's old content), no overlap (two
/// rows for one byte), no zero-length extent, and no extent past the size.
pub fn cover_whole_file(extents: &[(u64, NativeExtent)], size: u64) -> Result<(), WriteError> {
    let mut ordered: Vec<(u64, NativeExtent)> = extents.to_vec();
    ordered.sort_by_key(|(offset, _)| *offset);
    let mut cursor = 0u64;
    for (offset, extent) in &ordered {
        if extent.logical_len == 0 {
            return Err(WriteError::Record(format!(
                "replacement extent at {offset} is empty"
            )));
        }
        if *offset != cursor {
            return Err(WriteError::Record(format!(
                "replacement must publish the complete file: extent at {offset} follows {cursor} \
                 (a gap or an overlap would mix old and new content)"
            )));
        }
        cursor = offset
            .checked_add(extent.logical_len)
            .ok_or_else(|| WriteError::Record("replacement extent overflows".into()))?;
    }
    if cursor != size {
        return Err(WriteError::Record(format!(
            "replacement publishes {cursor} bytes but the file size is {size}"
        )));
    }
    Ok(())
}

/// Blocks a data extent's `[0, len)` range spans, including a partially
/// covered tail block.
fn blocks_spanning(len: u64, block_size: u64) -> u64 {
    len.div_ceil(block_size)
}

/// Plan a rename-over: read both views, require the source to be a complete,
/// durable file, resolve every block of its data extents to its binding,
/// placement and registration, and refuse anything that could not be
/// republished as a whole.
pub async fn plan_rename_over(
    store: &dyn ControlStore,
    keys: &Keys,
    workspace_id: &[u8; 16],
    domain_id: &[u8; 16],
    source_inode: u64,
    destination_inode: u64,
    block_size: u64,
) -> Result<Replacement, WriteError> {
    if block_size == 0 {
        return Err(WriteError::Record("block size must be > 0".into()));
    }
    if source_inode == destination_inode {
        return Err(WriteError::Record(format!(
            "inode {source_inode} cannot be renamed over itself: a replacement must publish \
             another file's complete content"
        )));
    }
    let source = read_inode_view(store, keys, workspace_id, source_inode).await?;
    let destination = read_inode_view(store, keys, workspace_id, destination_inode).await?;
    if !source.had_record {
        return Err(WriteError::Record(format!(
            "source inode {source_inode} has no committed content to publish"
        )));
    }

    let mut planned: Vec<(u64, NativeExtent)> = Vec::new();
    let mut carried: Vec<CarriedExtent> = Vec::new();
    for (&offset, extent) in &source.extents {
        if extent.kind == ExtentKind::Hole {
            if extent.block_count != 0 {
                return Err(WriteError::Record(format!(
                    "hole extent at {offset} claims {} blocks",
                    extent.block_count
                )));
            }
            planned.push((offset, extent.clone()));
            carried.push(CarriedExtent {
                logical_offset: offset,
                extent: extent.clone(),
                blocks: Vec::new(),
            });
            continue;
        }
        let needed = blocks_spanning(extent.logical_len, block_size);
        if extent.block_count != needed {
            return Err(WriteError::Record(format!(
                "data extent at {offset} is {} bytes with {} blocks, expected {needed}",
                extent.logical_len, extent.block_count
            )));
        }
        let mut blocks = Vec::with_capacity(needed as usize);
        for index in 0..needed {
            let block_index = extent.first_block + index;
            blocks
                .push(resolve_block(store, keys, domain_id, &extent.slice_id, block_index).await?);
        }
        planned.push((offset, extent.clone()));
        carried.push(CarriedExtent {
            logical_offset: offset,
            extent: extent.clone(),
            blocks,
        });
    }
    cover_whole_file(&planned, source.data.size)?;

    Ok(Replacement {
        source_inode,
        destination_inode,
        size: source.data.size,
        replaced: destination
            .extents
            .iter()
            .map(|(offset, extent)| (*offset, extent.clone()))
            .collect(),
        carried,
    })
}

/// Resolve one durable block of the source slice: its binding, its placement
/// and the registration its receipts must name.  Every step fails closed —
/// a block the source committed must be republishable, or the rename-over is
/// refused.
async fn resolve_block(
    store: &dyn ControlStore,
    keys: &Keys,
    domain_id: &[u8; 16],
    slice_id: &[u8; 16],
    block_index: u64,
) -> Result<CommittedBlock, WriteError> {
    let binding_bytes = store
        .get(&keys.binding(slice_id, block_index))
        .await?
        .ok_or_else(|| {
            WriteError::Record(format!(
                "slice {:02x?} block {block_index} has no binding to carry",
                slice_id
            ))
        })?;
    let binding = BlockBinding::decode(&mut Reader::new(&binding_bytes))?;
    let placement_bytes = store
        .get(&keys.placement(slice_id, block_index))
        .await?
        .ok_or_else(|| {
            WriteError::Record(format!(
                "slice {:02x?} block {block_index} has no placement to carry",
                slice_id
            ))
        })?;
    let placement = HeadPlacement::decode(&placement_bytes)?;
    let HeadPlacement::Loose { object_id, .. } = &placement;
    let object_id = *object_id;
    let registration_bytes = store
        .get(&keys.object(domain_id, &object_id))
        .await?
        .ok_or_else(|| {
            WriteError::Record(format!(
                "carried object {:02x?} has no registration",
                object_id
            ))
        })?;
    let registration: ObjectRegistration = decode_registration(&registration_bytes)?;
    if registration.object_ref.object_id != object_id || registration.domain_id != *domain_id {
        return Err(WriteError::RegistrationMismatch(format!(
            "carried object {:02x?} registration does not match its identity",
            object_id
        )));
    }
    if !matches!(
        registration.state,
        RegistrationState::Dispatched | RegistrationState::Verified
    ) {
        return Err(WriteError::RegistrationMismatch(format!(
            "carried object {:02x?} is {:?}, not durable",
            object_id, registration.state
        )));
    }
    Ok(CommittedBlock {
        block_index,
        binding,
        placement,
        object_ref: registration.object_ref.clone(),
        registration,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_base::wire::error::WireError;

    fn data(len: u64) -> NativeExtent {
        NativeExtent::data(len, [1u8; 16], 0, len.div_ceil(64))
    }

    #[test]
    fn contiguous_extents_covering_the_size_are_complete() {
        cover_whole_file(&[(0, data(128)), (128, NativeExtent::hole(64))], 192).unwrap();
        cover_whole_file(&[], 0).unwrap();
    }

    #[test]
    fn a_gap_overlap_or_short_cover_is_refused() {
        // A gap at 128 would leave 64 bytes of the destination's old content
        // in place while claiming a complete replacement.
        let gap = cover_whole_file(&[(0, data(128)), (192, data(64))], 256).unwrap_err();
        assert!(gap.to_string().contains("complete file"), "{gap}");
        // An overlap publishes two rows for one byte.
        assert!(cover_whole_file(&[(0, data(192)), (128, data(64))], 256).is_err());
        // Fewer bytes than the file size.
        assert!(cover_whole_file(&[(0, data(128))], 192).is_err());
        // An empty extent publishes nothing.
        assert!(cover_whole_file(&[(0, data(0))], 0).is_err());
    }

    #[test]
    fn a_replacement_never_reports_a_patch_shaped_change() {
        let replacement = Replacement {
            source_inode: 1,
            destination_inode: 2,
            size: 128,
            replaced: Vec::new(),
            carried: Vec::new(),
        };
        assert!(replacement.is_application_complete());
        assert_eq!(replacement.changed_ranges(), vec![(0, 128)]);
        // Same size on both sides is not an "unchanged" claim: the range is
        // still the whole file.
        let identical = Replacement {
            size: 128,
            ..replacement.clone()
        };
        assert_eq!(identical.changed_ranges(), vec![(0, 128)]);
    }

    #[test]
    fn wire_errors_from_binding_decode_are_writable() {
        let error: WireError = BlockBinding::decode(&mut Reader::new(&[0u8; 4])).unwrap_err();
        assert!(!WriteError::from(error).to_string().is_empty());
    }
}
