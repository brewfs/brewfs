//! SealBuilder: deterministic `.brfds` assembly (spec 04 §2).
//!
//! Used by tests here and by the seal writer in PR06A. `build()` performs
//! only structural assembly (valid BNPG pages, ascending table directory);
//! referential integrity is *not* silently repaired — writers call
//! [`SealBuilder::validate`] to enforce the binding/placement/frame/object
//! closure, and the reader re-checks everything it consumes. This split is
//! what allows the counterexample tests to construct intentionally broken
//! seals and prove the reader rejects them.
//!
//! Pages are stored uncompressed (codec None) with 8-byte alignment; the
//! table roots are always local children. External table reuse is supported
//! via [`SealBuilder::set_table_override`] for a table the builder itself
//! leaves empty (spec 04 §2: old frame tables may be reused, but slots are
//! never reinterpreted inside one seal).

use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

use super::binding::BlockBinding;
use super::descriptor::FrameDescriptor;
use super::error::SealError;
use super::placement::{BlockPlacement, Span, validate_spans};
use super::tables::{SealRoot, TableId, binding_key, object_key};
use crate::chunk::compress::decompress_framed;
use crate::native_base::wire::container::{
    Codec, ContainerFooter, ContainerHeader, FOOTER_LEN, HEADER_LEN, ObjectKind, features,
};
use crate::native_base::wire::error::WireError;
use crate::native_base::wire::frame::PayloadFormat;
use crate::native_base::wire::index_build::{IndexTreeParams, build_index_tree};
use crate::native_base::wire::refs::{ChildRef, ObjectRef};
use crate::native_base::wire::uvarint::Writer;

/// Default target size for one leaf page's encoded bytes. Pages are capped
/// hard by MAX_RAW_PAGE; this target only controls chunking.
pub const DEFAULT_LEAF_TARGET_BYTES: usize = 16 * 1024;

/// Object kind byte for a versioned native loose object (no container
/// header; kind 6 in the object-kind table).
pub const NATIVE_LOOSE_KIND: u8 = 6;

#[derive(Debug, Clone)]
pub struct SealBuilder {
    block_size: u32,
    leaf_target: usize,
    bindings: BTreeMap<[u8; 12], BlockBinding>,
    placements: BTreeMap<[u8; 12], BlockPlacement>,
    objects: BTreeMap<u32, ObjectRef>,
    frames: BTreeMap<u64, FrameDescriptor>,
    /// Per-table root override (index by TableId::ALL position).
    table_overrides: [Option<ChildRef>; 4],
}

impl Default for SealBuilder {
    fn default() -> Self {
        SealBuilder::new(1024 * 1024)
    }
}

impl SealBuilder {
    pub fn new(block_size: u32) -> SealBuilder {
        SealBuilder {
            block_size,
            leaf_target: DEFAULT_LEAF_TARGET_BYTES,
            bindings: BTreeMap::new(),
            placements: BTreeMap::new(),
            objects: BTreeMap::new(),
            frames: BTreeMap::new(),
            table_overrides: [None, None, None, None],
        }
    }

    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    /// Shrink the leaf page target (tests force multi-level indexes).
    pub fn with_leaf_target(mut self, leaf_target: usize) -> SealBuilder {
        self.leaf_target = leaf_target.max(64);
        self
    }

    pub fn next_object_ordinal(&self) -> u32 {
        self.objects.last_key_value().map_or(0, |(k, _)| k + 1)
    }

    pub fn next_frame_slot(&self) -> u64 {
        self.frames.last_key_value().map_or(0, |(k, _)| k + 1)
    }

    /// Raw table inserts (counterexample construction). Well-formed writers
    /// use the checked helpers below.
    pub fn add_binding(&mut self, slice_id: u64, block_index: u32, binding: BlockBinding) {
        self.bindings
            .insert(binding_key(slice_id, block_index), binding);
    }

    pub fn add_placement(&mut self, slice_id: u64, block_index: u32, placement: BlockPlacement) {
        self.placements
            .insert(binding_key(slice_id, block_index), placement);
    }

    pub fn add_object(&mut self, object_ordinal: u32, object: ObjectRef) {
        self.objects.insert(object_ordinal, object);
    }

    pub fn add_frame(&mut self, frame_slot: u64, descriptor: FrameDescriptor) {
        self.frames.insert(frame_slot, descriptor);
    }

    /// Override a table root with an external child. Only allowed for tables
    /// the builder itself leaves empty.
    pub fn set_table_override(&mut self, table: TableId, child: Option<ChildRef>) {
        self.table_overrides[TableId::ALL.iter().position(|t| *t == table).unwrap()] = child;
    }

    /// Register a versioned-framed loose block: decodes `encoded_object`
    /// to compute the binding (decoded_len + content hash), registers the
    /// object, binding and loose placement. Returns the object ordinal.
    pub fn add_loose_block(
        &mut self,
        slice_id: u64,
        block_index: u32,
        encoded_object: &[u8],
        object: ObjectRef,
    ) -> Result<u32, SealError> {
        if object.kind != NATIVE_LOOSE_KIND {
            return Err(SealError::placement(format!(
                "loose block object kind {} is not NativeLoose ({NATIVE_LOOSE_KIND})",
                object.kind
            )));
        }
        if object.object_len != encoded_object.len() as u64 {
            return Err(SealError::placement(
                "object ref length disagrees with the encoded object bytes",
            ));
        }
        let decoded = decompress_framed(encoded_object)
            .map_err(|e| SealError::Native(format!("loose object: {e}")))?;
        if decoded.is_empty() || decoded.len() as u64 > self.block_size as u64 {
            return Err(SealError::placement(format!(
                "loose block decoded length {} outside 1..={}",
                decoded.len(),
                self.block_size
            )));
        }
        let binding = BlockBinding::of_decoded(&decoded);
        let ordinal = self.next_object_ordinal();
        self.objects.insert(ordinal, object);
        let key = binding_key(slice_id, block_index);
        self.bindings.insert(key, binding);
        self.placements.insert(key, BlockPlacement::loose(ordinal));
        Ok(ordinal)
    }

    /// Register a packed block from its decoded bytes and span list. The
    /// spans must already be validated against the frame descriptors the
    /// caller registered.
    pub fn add_packed_block(
        &mut self,
        slice_id: u64,
        block_index: u32,
        decoded: &[u8],
        spans: Vec<Span>,
    ) -> Result<(), SealError> {
        if decoded.is_empty() || decoded.len() as u64 > self.block_size as u64 {
            return Err(SealError::placement(format!(
                "packed block decoded length {} outside 1..={}",
                decoded.len(),
                self.block_size
            )));
        }
        validate_spans(&spans, decoded.len() as u32).map_err(SealError::Wire)?;
        let binding = BlockBinding::of_decoded(decoded);
        let key = binding_key(slice_id, block_index);
        self.bindings.insert(key, binding);
        self.placements
            .insert(key, BlockPlacement::Packed { spans });
        Ok(())
    }

    /// Referential validation (spec 04 §3/§5/§11): key-set equality,
    /// kind compatibility, span coverage, frame/object closure and object
    /// offset bounds. Writers MUST call this before publishing; the reader
    /// independently enforces the same rules at read time.
    pub fn validate(&self) -> Result<(), SealError> {
        let bindings: Vec<&[u8; 12]> = self.bindings.keys().collect();
        let placements: Vec<&[u8; 12]> = self.placements.keys().collect();
        if bindings != placements {
            let only_bindings = self
                .bindings
                .keys()
                .filter(|k| !self.placements.contains_key(*k))
                .count();
            let only_placements = self
                .placements
                .keys()
                .filter(|k| !self.bindings.contains_key(*k))
                .count();
            return Err(SealError::KeySetMismatch(format!(
                "{} bindings vs {} placements ({} binding-only, {} placement-only)",
                bindings.len(),
                placements.len(),
                only_bindings,
                only_placements
            )));
        }
        for (key, binding) in &self.bindings {
            if binding.decoded_len == 0 || binding.decoded_len > self.block_size {
                return Err(SealError::placement(format!(
                    "block {}: decoded_len {} outside 1..={}",
                    hex::encode(key),
                    binding.decoded_len,
                    self.block_size
                )));
            }
            let placement = &self.placements[key];
            match placement {
                BlockPlacement::Loose {
                    object_ordinal,
                    native_layout,
                } => {
                    if *native_layout != super::placement::NATIVE_LAYOUT_VERSIONED_FRAMED {
                        return Err(SealError::placement(
                            "loose placement with non-versioned native layout",
                        ));
                    }
                    let object = self.objects.get(object_ordinal).ok_or_else(|| {
                        SealError::placement(format!(
                            "block {}: loose object ordinal {object_ordinal} missing",
                            hex::encode(key)
                        ))
                    })?;
                    if object.kind != NATIVE_LOOSE_KIND {
                        return Err(SealError::placement(format!(
                            "block {}: loose placement object kind {} is not NativeLoose",
                            hex::encode(key),
                            object.kind
                        )));
                    }
                }
                BlockPlacement::Packed { spans } => {
                    validate_spans(spans, binding.decoded_len).map_err(SealError::Wire)?;
                    let native = spans.iter().any(|s| {
                        self.frames
                            .get(&s.frame_slot)
                            .is_some_and(|d| d.payload_format == PayloadFormat::NativeBlockV1)
                    });
                    if native {
                        super::placement::validate_native_single_span(spans, binding.decoded_len)
                            .map_err(SealError::Wire)?;
                    }
                    for span in spans {
                        let descriptor = self.frames.get(&span.frame_slot).ok_or_else(|| {
                            SealError::placement(format!(
                                "block {}: frame slot {} missing from frames table",
                                hex::encode(key),
                                span.frame_slot
                            ))
                        })?;
                        let object =
                            self.objects
                                .get(&descriptor.object_ordinal)
                                .ok_or_else(|| {
                                    SealError::placement(format!(
                                        "frame slot {}: object ordinal {} missing",
                                        span.frame_slot, descriptor.object_ordinal
                                    ))
                                })?;
                        if object.kind != ObjectKind::DataPack.as_u8() {
                            return Err(SealError::placement(format!(
                                "frame slot {}: object kind {} is not DataPack",
                                span.frame_slot, object.kind
                            )));
                        }
                        if descriptor.object_offset < HEADER_LEN as u64 {
                            return Err(SealError::placement(format!(
                                "frame slot {}: object_offset {} enters the container header",
                                span.frame_slot, descriptor.object_offset
                            )));
                        }
                        let end = descriptor.range_get().1;
                        if end > object.object_len.saturating_sub(FOOTER_LEN as u64) {
                            return Err(SealError::placement(format!(
                                "frame slot {}: range get end {end} passes the footer of object len {}",
                                span.frame_slot, object.object_len
                            )));
                        }
                        if !native
                            && (span.frame_raw_offset as u64) + (span.length as u64)
                                > descriptor.raw_len as u64
                        {
                            return Err(SealError::placement(format!(
                                "block {}: span [{}, +{}] exceeds frame raw_len {}",
                                hex::encode(key),
                                span.frame_raw_offset,
                                span.length,
                                descriptor.raw_len
                            )));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn table_entries(&self, table: TableId) -> Vec<(Vec<u8>, Vec<u8>)> {
        match table {
            TableId::Bindings => self
                .bindings
                .iter()
                .map(|(k, v)| (k.to_vec(), v.encode()))
                .collect(),
            TableId::Placements => self
                .placements
                .iter()
                .map(|(k, v)| (k.to_vec(), v.encode()))
                .collect(),
            TableId::Objects => self
                .objects
                .iter()
                .map(|(k, v)| {
                    let mut w = Writer::new();
                    v.encode_into(&mut w);
                    (object_key(*k).to_vec(), w.into_bytes())
                })
                .collect(),
            TableId::Frames => self
                .frames
                .iter()
                .map(|(k, v)| (super::tables::frame_key(*k).to_vec(), v.encode()))
                .collect(),
        }
    }

    /// `required_features` for the seal container: propagate the frame
    /// formats referenced by the local frames table (a consumer of this
    /// seal needs those codecs to read the referenced data) and declare
    /// external index children when a table root is external. The seal's
    /// own pages are uncompressed.
    fn required_features(&self) -> u64 {
        let mut bits = 0u64;
        for descriptor in self.frames.values() {
            bits |= descriptor.payload_format.feature_bit();
            if descriptor.codec == Codec::Zstd {
                bits |= features::ZSTD_FRAME_OR_PAGE;
            }
        }
        if self
            .table_overrides
            .iter()
            .flatten()
            .any(|c| matches!(c, ChildRef::External(_)))
        {
            bits |= features::EXTERNAL_INDEX_CHILDREN;
        }
        bits
    }

    /// Assemble the complete `.brfds` object bytes.
    pub fn build(&self) -> Result<Vec<u8>, SealError> {
        let mut root = SealRoot::default();
        // Page region bytes (after the 64-byte header, 8-byte aligned).
        let mut body: Vec<u8> = Vec::new();
        for (i, table) in TableId::ALL.iter().enumerate() {
            let entries = self.table_entries(*table);
            let child = if let Some(override_child) = &self.table_overrides[i] {
                if !entries.is_empty() {
                    return Err(SealError::placement(format!(
                        "table {table:?} has both local entries and a root override"
                    )));
                }
                Some(override_child.clone())
            } else if entries.is_empty() {
                None
            } else {
                Some(build_table_tree(&entries, self.leaf_target, &mut body)?)
            };
            root.set_table(*table, child);
        }
        while !body.len().is_multiple_of(8) {
            body.push(0);
        }
        let root_payload = root.encode();
        let root_offset = (HEADER_LEN + body.len()) as u64;
        let object_len = (HEADER_LEN + body.len() + root_payload.len() + FOOTER_LEN) as u64;
        let header = ContainerHeader {
            kind: ObjectKind::DataSeal,
            required_features: self.required_features(),
            object_len,
            root_offset,
            root_stored_len: root_payload.len() as u32,
            root_raw_len: root_payload.len() as u32,
            hash_id: 1,
            root_codec: Codec::None,
        };
        let footer = ContainerFooter {
            object_len,
            root_stored_digest: Sha256::digest(&root_payload).into(),
        };
        let mut out = Vec::with_capacity(object_len as usize);
        out.extend_from_slice(&header.encode());
        out.extend_from_slice(&body);
        out.extend_from_slice(&root_payload);
        out.extend_from_slice(&footer.encode());
        debug_assert_eq!(out.len() as u64, object_len);
        Ok(out)
    }
}

/// Build a BNPG tree over `entries` (non-empty, key-sorted) and return the
/// root child. Every page of a seal table is a generic key/value page
/// advertising `PageKind::GenericKeyValue` (spec 04 §2). The tree shape
/// itself lives in [`build_index_tree`], shared with the PR06A inventory and
/// RetainBatch indexes.
fn build_table_tree(
    entries: &[(Vec<u8>, Vec<u8>)],
    leaf_target: usize,
    body: &mut Vec<u8>,
) -> Result<ChildRef, WireError> {
    build_index_tree(entries, &IndexTreeParams::generic(leaf_target), body)
}
