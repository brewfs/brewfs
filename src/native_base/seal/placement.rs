//! BlockPlacement (spec 04 §4): where a block's decoded bytes live.
//!
//! There is no `Unknown`, `FallbackToLoose` or `Zero` tag. Logical holes are
//! expressed by metadata, never by a placement kind.
//!
//! Loose: one versioned-framed native loose object. Packed: an ordered span
//! list that must cover `[0, decoded_len)` exactly.

use crate::native_base::wire::error::{WireError, WireResult};
use crate::native_base::wire::uvarint::{Reader, Writer};

/// The only `native_layout` value this version accepts (spec 04 §4).
pub const NATIVE_LAYOUT_VERSIONED_FRAMED: u8 = 1;

/// Upper bound on spans per placement; `decoded_len <= block_size` and each
/// span has length >= 1 bound this naturally, the cap keeps announced counts
/// honest before allocation.
pub const MAX_SPANS: u64 = 65_536;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Span {
    /// Offset in the block's decoded domain.
    pub block_offset: u32,
    /// Length in the block's decoded domain.
    pub length: u32,
    /// Seal-scoped frame slot (not the pack-internal ordinal).
    pub frame_slot: u64,
    /// Offset into the frame's raw (outer-codec-undone) payload. For
    /// NativeBlockV1 frames this must be 0 and is not a byte offset into
    /// the encoded inner block.
    pub frame_raw_offset: u32,
}

impl Span {
    pub fn encode_into(&self, w: &mut Writer) {
        w.u32(self.block_offset);
        w.u32(self.length);
        w.u64(self.frame_slot);
        w.u32(self.frame_raw_offset);
    }

    pub fn decode(r: &mut Reader<'_>) -> WireResult<Span> {
        let what = "placement span";
        Ok(Span {
            block_offset: r.u32(what)?,
            length: r.u32(what)?,
            frame_slot: r.u64(what)?,
            frame_raw_offset: r.u32(what)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockPlacement {
    Loose {
        object_ordinal: u32,
        native_layout: u8,
    },
    Packed {
        spans: Vec<Span>,
    },
}

impl BlockPlacement {
    pub fn loose(object_ordinal: u32) -> BlockPlacement {
        BlockPlacement::Loose {
            object_ordinal,
            native_layout: NATIVE_LAYOUT_VERSIONED_FRAMED,
        }
    }

    pub fn encode_into(&self, w: &mut Writer) {
        match self {
            BlockPlacement::Loose {
                object_ordinal,
                native_layout,
            } => {
                w.u8(0);
                w.u32(*object_ordinal);
                w.u8(*native_layout);
                w.u8(0); // reserved
            }
            BlockPlacement::Packed { spans } => {
                w.u8(1);
                w.uvarint(spans.len() as u64);
                for span in spans {
                    span.encode_into(w);
                }
            }
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        self.encode_into(&mut w);
        w.into_bytes()
    }

    pub fn decode(r: &mut Reader<'_>) -> WireResult<BlockPlacement> {
        let what = "block placement";
        match r.u8(what)? {
            0 => {
                let object_ordinal = r.u32(what)?;
                let native_layout = r.u8(what)?;
                let reserved = r.u8(what)?;
                if native_layout != NATIVE_LAYOUT_VERSIONED_FRAMED {
                    return Err(WireError::invalid(
                        what,
                        format!(
                            "native_layout {native_layout} (only \
                             {NATIVE_LAYOUT_VERSIONED_FRAMED}=VersionedFramed is allowed)"
                        ),
                    ));
                }
                if reserved != 0 {
                    return Err(WireError::invalid(what, "reserved byte must be zero"));
                }
                Ok(BlockPlacement::Loose {
                    object_ordinal,
                    native_layout,
                })
            }
            1 => {
                let count = r.uvarint(what)?;
                if count == 0 {
                    return Err(WireError::invalid(
                        what,
                        "packed placement must have at least one span",
                    ));
                }
                if count > MAX_SPANS {
                    return Err(WireError::LimitExceeded(format!(
                        "span count {count} exceeds {MAX_SPANS}"
                    )));
                }
                let mut spans = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    spans.push(Span::decode(r)?);
                }
                Ok(BlockPlacement::Packed { spans })
            }
            other => Err(WireError::invalid(
                what,
                format!(
                    "placement tag {other} (there is no Unknown/FallbackToLoose/Zero placement)"
                ),
            )),
        }
    }
}

/// Validate a packed span list against the block's `decoded_len`: spans are
/// ordered by `block_offset`, start at 0, are contiguous (no gap, no
/// overlap), have non-zero length, and exactly cover `decoded_len`
/// (spec 04 §4).
pub fn validate_spans(spans: &[Span], decoded_len: u32) -> WireResult<()> {
    let mut covered: u32 = 0;
    for (i, span) in spans.iter().enumerate() {
        if span.length == 0 {
            return Err(WireError::invalid(
                "placement spans",
                format!("span {i}: zero length"),
            ));
        }
        if span.block_offset != covered {
            return Err(WireError::invalid(
                "placement spans",
                format!(
                    "span {i}: block_offset {} does not continue at {covered} \
                     (spans must be contiguous from 0 without gaps or overlaps)",
                    span.block_offset
                ),
            ));
        }
        covered = covered
            .checked_add(span.length)
            .ok_or_else(|| WireError::invalid("placement spans", "coverage overflow"))?;
        if covered > decoded_len {
            return Err(WireError::invalid(
                "placement spans",
                format!("spans cover {covered} bytes, block decoded_len is {decoded_len}"),
            ));
        }
    }
    if covered != decoded_len {
        return Err(WireError::invalid(
            "placement spans",
            format!("spans cover {covered} of {decoded_len} decoded bytes (no zero fill)"),
        ));
    }
    Ok(())
}

/// NativeBlockV1 frames allow exactly one span: `block_offset=0`,
/// `length=decoded_len`, `frame_raw_offset=0` (spec 04 §4). The span's
/// `length` lives in the decoded domain; it must not be compared against the
/// frame's `raw_len`, which describes the encoded inner block.
pub fn validate_native_single_span(spans: &[Span], decoded_len: u32) -> WireResult<()> {
    if spans.len() != 1 {
        return Err(WireError::invalid(
            "placement spans",
            format!(
                "NativeBlockV1 placement must have exactly one span, has {}",
                spans.len()
            ),
        ));
    }
    let span = &spans[0];
    if span.block_offset != 0 || span.frame_raw_offset != 0 || span.length != decoded_len {
        return Err(WireError::invalid(
            "placement spans",
            format!(
                "NativeBlockV1 span must be (0, {decoded_len}, slot, 0), got \
                 (block_offset {}, length {}, frame_raw_offset {})",
                span.block_offset, span.length, span.frame_raw_offset
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn span(block_offset: u32, length: u32, slot: u64, raw_off: u32) -> Span {
        Span {
            block_offset,
            length,
            frame_slot: slot,
            frame_raw_offset: raw_off,
        }
    }

    #[test]
    fn loose_roundtrip_and_rejects_unknown_layout_and_reserved() {
        let p = BlockPlacement::loose(42);
        let enc = p.encode();
        let mut r = Reader::new(&enc);
        assert_eq!(BlockPlacement::decode(&mut r).unwrap(), p);
        assert!(r.is_empty());

        // native_layout = 2 must be rejected, not treated as legacy.
        let mut bad = enc.clone();
        bad[5] = 2;
        assert!(BlockPlacement::decode(&mut Reader::new(&bad)).is_err());
        // reserved != 0.
        let mut bad = enc;
        bad[6] = 1;
        assert!(BlockPlacement::decode(&mut Reader::new(&bad)).is_err());
    }

    #[test]
    fn packed_roundtrip() {
        let p = BlockPlacement::Packed {
            spans: vec![span(0, 16, 7, 0), span(16, 16, 9, 32)],
        };
        let enc = p.encode();
        let mut r = Reader::new(&enc);
        assert_eq!(BlockPlacement::decode(&mut r).unwrap(), p);
        assert!(r.is_empty());
    }

    #[test]
    fn unknown_tag_and_zero_span_count_rejected() {
        let mut w = Writer::new();
        w.u8(2);
        assert!(BlockPlacement::decode(&mut Reader::new(w.as_slice())).is_err());
        let mut w = Writer::new();
        w.u8(1);
        w.uvarint(0);
        assert!(BlockPlacement::decode(&mut Reader::new(w.as_slice())).is_err());
    }

    #[test]
    fn span_validation_counterexamples() {
        let ok = vec![span(0, 10, 1, 0), span(10, 6, 2, 4)];
        assert!(validate_spans(&ok, 16).is_ok());

        // Gap between spans.
        let gap = vec![span(0, 10, 1, 0), span(11, 5, 2, 4)];
        assert!(validate_spans(&gap, 16).is_err());
        // Overlap.
        let overlap = vec![span(0, 10, 1, 0), span(9, 7, 2, 4)];
        assert!(validate_spans(&overlap, 16).is_err());
        // Unsorted (second span earlier).
        let unsorted = vec![span(0, 10, 1, 0), span(2, 14, 2, 4)];
        assert!(validate_spans(&unsorted, 16).is_err());
        // Does not start at 0.
        let shifted = vec![span(1, 15, 1, 0)];
        assert!(validate_spans(&shifted, 16).is_err());
        // Short coverage — the "missing bytes" case that must NOT zero fill.
        let short = vec![span(0, 10, 1, 0)];
        assert!(validate_spans(&short, 16).is_err());
        // Over-coverage.
        let long = vec![span(0, 20, 1, 0)];
        assert!(validate_spans(&long, 16).is_err());
        // Zero-length span.
        let zero = vec![span(0, 0, 1, 0), span(0, 16, 2, 0)];
        assert!(validate_spans(&zero, 16).is_err());
    }

    #[test]
    fn native_single_span_rule() {
        // Correct single span for a 16-byte decoded block.
        assert!(validate_native_single_span(&[span(0, 16, 3, 0)], 16).is_ok());
        // Multi-span over a native frame is forbidden even if contiguous.
        assert!(validate_native_single_span(&[span(0, 8, 3, 0), span(8, 8, 3, 0)], 16).is_err());
        // frame_raw_offset must be 0.
        assert!(validate_native_single_span(&[span(0, 16, 3, 4)], 16).is_err());
        // length must equal decoded_len, not the frame's raw_len.
        assert!(validate_native_single_span(&[span(0, 12, 3, 0)], 16).is_err());
    }
}
