//! Physical read plan execution (spec 06, PR03 synchronous subset).
//!
//! The P1 executor here is synchronous and per-call: units are deduplicated
//! by exact immutable frame identity (one fetch per frame slot per read,
//! spec 04 §9), each unit's peak footprint is reserved against the budget
//! before any I/O, cancellation is checked before each unit, and the output
//! is scatter-filled and coverage-verified before returning. The async
//! executor, singleflight waiters, Range GET coalescing and prefetch are
//! PR10 and must pass the same interleaving contracts.
//!
//! Budget semantics for this executor: the whole plan's units are reserved
//! for the duration of the read (all decoded payloads must coexist until the
//! scatter completes), so the reservation is the sum of unit peaks. A single
//! unit exceeding a hard cap is rejected immediately as
//! [`SealError::BudgetUnitTooLarge`] — it never queues forever (spec 06 §7).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use sha2::{Digest, Sha256};

use super::binding::BlockBinding;
use super::builder::NATIVE_LOOSE_KIND;
use super::descriptor::FrameDescriptor;
use super::error::SealError;
use super::placement::{BlockPlacement, validate_native_single_span, validate_spans};
use super::reader::{ResolvedBlock, SealReader};
use crate::chunk::compress::decompress_framed;
use crate::native_base::wire::container::{FOOTER_LEN, HEADER_LEN, ObjectKind};
use crate::native_base::wire::frame::{FRAME_HEADER_LEN, FrameHeader, PayloadFormat};
use crate::native_base::wire::refs::ObjectRef;

/// Upper bound on blocks touched by one read plan (spec 06 §2: plan segment
/// counts must be bounded before task construction).
pub const MAX_PLAN_BLOCKS: u64 = 65_536;

/// Suggested host caps (spec 06 §7; advisory starting points, not wire
/// values).
pub const RECOMMENDED_MAX_INFLIGHT_ENCODED: u64 = 128 * 1024 * 1024;
pub const RECOMMENDED_MAX_INFLIGHT_DECODED: u64 = 256 * 1024 * 1024;

/// Byte budget with in-flight accounting. Tokens are reserved before
/// allocation/decoding and released when the read completes.
#[derive(Debug, Clone)]
pub struct ReadBudget {
    max_inflight_encoded: u64,
    max_inflight_decoded: u64,
    inflight_encoded: u64,
    inflight_decoded: u64,
}

impl Default for ReadBudget {
    fn default() -> Self {
        ReadBudget::recommended()
    }
}

impl ReadBudget {
    pub fn new(max_inflight_encoded: u64, max_inflight_decoded: u64) -> ReadBudget {
        ReadBudget {
            max_inflight_encoded,
            max_inflight_decoded,
            inflight_encoded: 0,
            inflight_decoded: 0,
        }
    }

    pub fn recommended() -> ReadBudget {
        ReadBudget::new(
            RECOMMENDED_MAX_INFLIGHT_ENCODED,
            RECOMMENDED_MAX_INFLIGHT_DECODED,
        )
    }

    pub fn inflight(&self) -> (u64, u64) {
        (self.inflight_encoded, self.inflight_decoded)
    }

    fn reserve(&mut self, encoded: u64, decoded: u64) -> Result<BudgetReservation<'_>, SealError> {
        if encoded > self.max_inflight_encoded {
            return Err(SealError::BudgetUnitTooLarge {
                needed: encoded,
                cap: self.max_inflight_encoded,
            });
        }
        if decoded > self.max_inflight_decoded {
            return Err(SealError::BudgetUnitTooLarge {
                needed: decoded,
                cap: self.max_inflight_decoded,
            });
        }
        self.inflight_encoded = self
            .inflight_encoded
            .checked_add(encoded)
            .ok_or(SealError::PlanLimit("encoded budget overflow".into()))?;
        self.inflight_decoded = self
            .inflight_decoded
            .checked_add(decoded)
            .ok_or(SealError::PlanLimit("decoded budget overflow".into()))?;
        if self.inflight_encoded > self.max_inflight_encoded {
            self.inflight_encoded -= encoded;
            return Err(SealError::BudgetUnitTooLarge {
                needed: self.inflight_encoded + encoded,
                cap: self.max_inflight_encoded,
            });
        }
        if self.inflight_decoded > self.max_inflight_decoded {
            self.inflight_decoded -= decoded;
            return Err(SealError::BudgetUnitTooLarge {
                needed: self.inflight_decoded + decoded,
                cap: self.max_inflight_decoded,
            });
        }
        Ok(BudgetReservation {
            budget: self,
            encoded,
            decoded,
        })
    }
}

/// RAII release of reserved tokens.
pub struct BudgetReservation<'a> {
    budget: &'a mut ReadBudget,
    encoded: u64,
    decoded: u64,
}

impl Drop for BudgetReservation<'_> {
    fn drop(&mut self) {
        self.budget.inflight_encoded -= self.encoded;
        self.budget.inflight_decoded -= self.decoded;
    }
}

/// Cooperative cancellation. Checked between units; a cancelled read
/// returns an error and never a partial result.
#[derive(Debug, Clone, Default)]
pub struct CancelToken(Arc<AtomicBool>);

impl CancelToken {
    pub fn new() -> CancelToken {
        CancelToken::default()
    }

    pub fn cancel(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }

    pub fn check(&self) -> Result<(), SealError> {
        if self.is_cancelled() {
            Err(SealError::Cancelled)
        } else {
            Ok(())
        }
    }
}

/// Read metrics (spec 06 §9). `requested_file_bytes` and
/// `logical_data_bytes` differ only when holes are subtracted upstream; the
/// PR03 executor has no hole ranges, so both track the request.
/// `discarded_prefetch_bytes` and `merged_request_extra_bytes` exist so the
/// accounting shape matches the spec; both stay zero until PR10 adds
/// prefetch and Range GET coalescing.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReadMetrics {
    pub requested_file_bytes: u64,
    pub logical_data_bytes: u64,
    pub fetched_stored_bytes: u64,
    pub decoded_payload_bytes: u64,
    pub native_decoded_bytes: u64,
    pub discarded_prefetch_bytes: u64,
    pub merged_request_extra_bytes: u64,
    pub range_gets: u64,
    pub frames_fetched: u64,
    pub loose_objects_fetched: u64,
}

/// Identity of one schedulable unit: exact immutable frame identity or one
/// loose object. Deduplicated within a plan (spec 04 §9: the same
/// frame_slot is scheduled at most once per read).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum UnitKey {
    Loose(u32),
    Frame(u64),
}

struct PlannedUnit {
    object: ObjectRef,
    descriptor: Option<FrameDescriptor>,
    peak_encoded: u64,
    peak_decoded: u64,
}

struct PlannedBlock {
    block_index: u64,
    in_start: u32,
    in_end: u32,
    binding: BlockBinding,
    placement: BlockPlacement,
    /// The read covers the block's entire decoded domain.
    full_block: bool,
}

impl SealReader<'_> {
    /// Read one whole block by key.
    pub fn read_block(
        &self,
        slice_id: u64,
        block_index: u32,
        block_size: u32,
    ) -> Result<Vec<u8>, SealError> {
        let resolved = self.resolve_block(slice_id, block_index)?;
        if resolved.binding.decoded_len > block_size {
            return Err(SealError::integrity(format!(
                "block ({slice_id},{block_index}) decoded_len {} exceeds block_size {block_size}",
                resolved.binding.decoded_len
            )));
        }
        let offset = (block_index as u64)
            .checked_mul(block_size as u64)
            .ok_or_else(|| SealError::PlanLimit("block offset overflow".into()))?;
        self.read_range(
            slice_id,
            offset,
            resolved.binding.decoded_len as u64,
            block_size,
        )
    }

    /// Read a range with default budget, a fresh cancel token and throwaway
    /// metrics.
    pub fn read_range(
        &self,
        slice_id: u64,
        offset: u64,
        len: u64,
        block_size: u32,
    ) -> Result<Vec<u8>, SealError> {
        let mut budget = ReadBudget::recommended();
        let cancel = CancelToken::new();
        let mut metrics = ReadMetrics::default();
        self.read_range_with(
            slice_id,
            offset,
            len,
            block_size,
            &mut budget,
            &cancel,
            &mut metrics,
        )
    }

    /// The planned read: resolve blocks, deduplicate units, reserve budget,
    /// fetch with cancellation, decode, scatter and verify coverage. Any
    /// failure is an error — a missing frame, short read or hash mismatch
    /// is never converted into zeros.
    // The parameter set mirrors spec 06 §6 (coordinates, budget, cancellation
    // and metrics are all per-call inputs); the PR10 async executor replaces
    // this whole entry point.
    #[allow(clippy::too_many_arguments)]
    pub fn read_range_with(
        &self,
        slice_id: u64,
        offset: u64,
        len: u64,
        block_size: u32,
        budget: &mut ReadBudget,
        cancel: &CancelToken,
        metrics: &mut ReadMetrics,
    ) -> Result<Vec<u8>, SealError> {
        if block_size == 0 {
            return Err(SealError::PlanLimit("block_size must be > 0".into()));
        }
        if len == 0 {
            return Ok(Vec::new());
        }
        metrics.requested_file_bytes += len;
        metrics.logical_data_bytes += len;
        let end = offset
            .checked_add(len)
            .ok_or_else(|| SealError::PlanLimit("offset + len overflow".into()))?;
        let first_block = offset / block_size as u64;
        let last_block = (end - 1) / block_size as u64;
        let block_count = last_block - first_block + 1;
        if block_count > MAX_PLAN_BLOCKS {
            return Err(SealError::PlanLimit(format!(
                "read touches {block_count} blocks, more than {MAX_PLAN_BLOCKS}"
            )));
        }

        // Phase 1: resolve metadata and build the unit set.
        let mut blocks: Vec<PlannedBlock> = Vec::new();
        let mut units: BTreeMap<UnitKey, PlannedUnit> = BTreeMap::new();
        for block_index in first_block..=last_block {
            if block_index > u32::MAX as u64 {
                return Err(SealError::PlanLimit(
                    "block index exceeds the u32 block address space".into(),
                ));
            }
            let resolved: ResolvedBlock = self.resolve_block(slice_id, block_index as u32)?;
            let binding = resolved.binding;
            if binding.decoded_len > block_size {
                return Err(SealError::integrity(format!(
                    "block ({slice_id},{block_index}) decoded_len {} exceeds block_size {block_size}",
                    binding.decoded_len
                )));
            }
            let in_start: u32 = if block_index == first_block {
                (offset % block_size as u64) as u32
            } else {
                0
            };
            let in_end: u32 = if block_index == last_block {
                (((end - 1) % block_size as u64) + 1) as u32
            } else {
                block_size
            };
            if in_end > binding.decoded_len {
                return Err(SealError::RangeBeyondBlock {
                    offset: block_index * block_size as u64 + in_start as u64,
                    end: block_index * block_size as u64 + in_end as u64,
                    decoded_len: binding.decoded_len as u64,
                });
            }
            let full_block = in_start == 0 && in_end == binding.decoded_len;
            match &resolved.placement {
                BlockPlacement::Loose {
                    object_ordinal,
                    native_layout,
                } => {
                    if *native_layout != super::placement::NATIVE_LAYOUT_VERSIONED_FRAMED {
                        return Err(SealError::placement(
                            "loose placement with non-versioned native layout",
                        ));
                    }
                    let object = self.object_for(*object_ordinal)?;
                    if object.kind != NATIVE_LOOSE_KIND {
                        return Err(SealError::placement(format!(
                            "loose placement requires NativeLoose object, kind is {}",
                            object.kind
                        )));
                    }
                    units
                        .entry(UnitKey::Loose(*object_ordinal))
                        .or_insert(PlannedUnit {
                            peak_encoded: object.object_len,
                            peak_decoded: binding.decoded_len as u64,
                            object,
                            descriptor: None,
                        });
                }
                BlockPlacement::Packed { spans } => {
                    validate_spans(spans, binding.decoded_len).map_err(SealError::Wire)?;
                    for span in spans {
                        // Only plan frames whose spans overlap the in-block range.
                        if span.block_offset >= in_end
                            || span.block_offset.saturating_add(span.length) <= in_start
                        {
                            continue;
                        }
                        let descriptor = self.frame_descriptor(span.frame_slot)?;
                        let object = self.object_for(descriptor.object_ordinal)?;
                        if object.kind != ObjectKind::DataPack.as_u8() {
                            return Err(SealError::placement(format!(
                                "frame slot {}: object kind {} is not DataPack",
                                span.frame_slot, object.kind
                            )));
                        }
                        if descriptor.object_offset < HEADER_LEN as u64 {
                            return Err(SealError::placement(format!(
                                "frame slot {}: object_offset enters the container header",
                                span.frame_slot
                            )));
                        }
                        let range_end = descriptor.range_get().1;
                        if range_end > object.object_len.saturating_sub(FOOTER_LEN as u64) {
                            return Err(SealError::placement(format!(
                                "frame slot {}: range get end {range_end} passes the footer \
                                 of object len {}",
                                span.frame_slot, object.object_len
                            )));
                        }
                        if descriptor.payload_format == PayloadFormat::NativeBlockV1 {
                            // Native frames carry the whole block in one span;
                            // frame_raw_offset is not a raw-domain offset.
                            validate_native_single_span(spans, binding.decoded_len)
                                .map_err(SealError::Wire)?;
                        } else if span.frame_raw_offset as u64 + span.length as u64
                            > descriptor.raw_len as u64
                        {
                            return Err(SealError::placement(format!(
                                "span [{}, +{}] exceeds frame raw_len {}",
                                span.frame_raw_offset, span.length, descriptor.raw_len
                            )));
                        }
                        let native_inner =
                            if descriptor.payload_format == PayloadFormat::NativeBlockV1 {
                                binding.decoded_len as u64
                            } else {
                                0
                            };
                        units
                            .entry(UnitKey::Frame(span.frame_slot))
                            .or_insert(PlannedUnit {
                                peak_encoded: FRAME_HEADER_LEN as u64
                                    + descriptor.stored_len as u64,
                                peak_decoded: descriptor.raw_len as u64 + native_inner,
                                object,
                                descriptor: Some(descriptor),
                            });
                    }
                }
            }
            blocks.push(PlannedBlock {
                block_index,
                in_start,
                in_end,
                binding,
                placement: resolved.placement,
                full_block,
            });
        }

        // Phase 2: reserve the plan's peak footprint before any I/O.
        let reservation = {
            let mut encoded = 0u64;
            let mut decoded = 0u64;
            for unit in units.values() {
                if unit.peak_encoded > budget.max_inflight_encoded {
                    return Err(SealError::BudgetUnitTooLarge {
                        needed: unit.peak_encoded,
                        cap: budget.max_inflight_encoded,
                    });
                }
                if unit.peak_decoded > budget.max_inflight_decoded {
                    return Err(SealError::BudgetUnitTooLarge {
                        needed: unit.peak_decoded,
                        cap: budget.max_inflight_decoded,
                    });
                }
                encoded = encoded
                    .checked_add(unit.peak_encoded)
                    .ok_or_else(|| SealError::PlanLimit("encoded budget overflow".into()))?;
                decoded = decoded
                    .checked_add(unit.peak_decoded)
                    .ok_or_else(|| SealError::PlanLimit("decoded budget overflow".into()))?;
            }
            budget.reserve(encoded, decoded)?
        };

        // Phase 3: fetch and decode units.
        let mut frame_raws: BTreeMap<u64, (Vec<u8>, PayloadFormat)> = BTreeMap::new();
        let mut loose_objects: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
        for (key, unit) in &units {
            cancel.check()?;
            match key {
                UnitKey::Loose(ordinal) => {
                    let bytes =
                        self.source
                            .get_range(&unit.object.object_id, 0, unit.object.object_len)?;
                    if bytes.len() as u64 != unit.object.object_len {
                        return Err(SealError::Source(
                            super::source::ObjectSourceError::ShortRead {
                                requested: unit.object.object_len,
                                received: bytes.len() as u64,
                            },
                        ));
                    }
                    let digest: [u8; 32] = Sha256::digest(&bytes).into();
                    if digest != unit.object.full_hash {
                        return Err(SealError::integrity("loose object full hash mismatch"));
                    }
                    metrics.range_gets += 1;
                    metrics.fetched_stored_bytes += bytes.len() as u64;
                    metrics.loose_objects_fetched += 1;
                    loose_objects.insert(*ordinal, bytes);
                }
                UnitKey::Frame(slot) => {
                    let descriptor = unit
                        .descriptor
                        .as_ref()
                        .expect("frame unit carries a descriptor");
                    let (range_start, range_end) = descriptor.range_get();
                    let bytes =
                        self.source
                            .get_range(&unit.object.object_id, range_start, range_end)?;
                    if bytes.len() as u64 != range_end - range_start {
                        return Err(SealError::Source(
                            super::source::ObjectSourceError::ShortRead {
                                requested: range_end - range_start,
                                received: bytes.len() as u64,
                            },
                        ));
                    }
                    metrics.range_gets += 1;
                    metrics.fetched_stored_bytes += bytes.len() as u64;
                    metrics.frames_fetched += 1;
                    let header = FrameHeader::parse(&bytes)?;
                    descriptor
                        .verify_against_header(&header)
                        .map_err(SealError::DescriptorMismatch)?;
                    let raw = header.decode_payload(&bytes[FRAME_HEADER_LEN..])?;
                    metrics.decoded_payload_bytes += raw.len() as u64;
                    frame_raws.insert(*slot, (raw, header.payload_format));
                }
            }
        }

        // Phase 4: scatter into the output buffer and verify coverage.
        let mut out = vec![0u8; len as usize];
        let mut covered: Vec<(u64, u64)> = Vec::with_capacity(blocks.len());
        for block in &blocks {
            let block_start = block.block_index * block_size as u64;
            let out_off = (block_start + block.in_start as u64 - offset) as usize;
            let out_len = (block.in_end - block.in_start) as usize;
            match &block.placement {
                BlockPlacement::Loose { object_ordinal, .. } => {
                    let encoded = &loose_objects[object_ordinal];
                    let decoded = decompress_framed(encoded)
                        .map_err(|e| SealError::Native(format!("loose object: {e}")))?;
                    if decoded.len() as u64 != block.binding.decoded_len as u64 {
                        return Err(SealError::integrity(format!(
                            "loose block decodes to {} bytes, binding says {}",
                            decoded.len(),
                            block.binding.decoded_len
                        )));
                    }
                    metrics.native_decoded_bytes += decoded.len() as u64;
                    out[out_off..out_off + out_len]
                        .copy_from_slice(&decoded[block.in_start as usize..block.in_end as usize]);
                }
                BlockPlacement::Packed { spans } => {
                    for span in spans {
                        if span.block_offset >= block.in_end
                            || span.block_offset.saturating_add(span.length) <= block.in_start
                        {
                            continue;
                        }
                        let (raw, format) = &frame_raws[&span.frame_slot];
                        if *format == PayloadFormat::NativeBlockV1 {
                            // The whole frame is the encoded inner block; the
                            // span's length lives in the decoded domain and
                            // must not be compared with frame.raw_len.
                            let decoded = decompress_framed(raw)
                                .map_err(|e| SealError::Native(format!("inner block: {e}")))?;
                            if decoded.len() as u64 != block.binding.decoded_len as u64 {
                                return Err(SealError::integrity(format!(
                                    "native block decodes to {} bytes, binding says {}",
                                    decoded.len(),
                                    block.binding.decoded_len
                                )));
                            }
                            metrics.native_decoded_bytes += decoded.len() as u64;
                            out[out_off..out_off + out_len].copy_from_slice(
                                &decoded[block.in_start as usize..block.in_end as usize],
                            );
                        } else {
                            let need_start = block.in_start.max(span.block_offset);
                            let need_end = block
                                .in_end
                                .min(span.block_offset.saturating_add(span.length));
                            let raw_start = span.frame_raw_offset as usize
                                + (need_start - span.block_offset) as usize;
                            let raw_end = raw_start + (need_end - need_start) as usize;
                            if raw_end > raw.len() {
                                return Err(SealError::placement(format!(
                                    "span [{}, +{}] exceeds the frame raw payload",
                                    span.frame_raw_offset, span.length
                                )));
                            }
                            let dst_start = (need_start - block.in_start) as usize;
                            let copy_len = (need_end - need_start) as usize;
                            out[out_off + dst_start..out_off + dst_start + copy_len]
                                .copy_from_slice(&raw[raw_start..raw_end]);
                        }
                    }
                }
            }
            // Whole-block reads carry the binding's content hash; verify it.
            // Partial PlainBytes reads are covered by the frame raw_digest
            // (documented limitation: block content hash needs full bytes).
            if block.full_block {
                let digest: [u8; 32] = Sha256::digest(&out[out_off..out_off + out_len]).into();
                if digest != block.binding.content_hash {
                    return Err(SealError::integrity(format!(
                        "block ({slice_id},{}) content hash mismatch",
                        block.block_index
                    )));
                }
            }
            covered.push((out_off as u64, (out_off + out_len) as u64));
        }

        // Coverage: every output byte must have been written exactly once.
        covered.sort_unstable();
        let mut cursor = 0u64;
        for (start, end) in &covered {
            if *start != cursor {
                return Err(SealError::integrity(
                    "read plan left a gap in the output (missing data is an error, not zeros)",
                ));
            }
            cursor = *end;
        }
        if cursor != len {
            return Err(SealError::integrity(
                "read plan left a gap in the output (missing data is an error, not zeros)",
            ));
        }
        drop(reservation);
        Ok(out)
    }
}
