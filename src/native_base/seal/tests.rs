//! PR03 acceptance tests (spec 04 §7, spec 06, spec 15 PR03 row):
//! fake I/O counting, missing-data-is-an-error, and the Native/Plain
//! boundary.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};

use super::builder::SealBuilder;
use super::error::SealError;
use super::placement::Span;
use super::source::{ObjectSource, ObjectSourceError};
use super::tables::{SealRoot, TableId, binding_key};
use super::{
    BlockBinding, BlockPlacement, CancelToken, FrameDescriptor, NATIVE_LOOSE_KIND, ReadBudget,
    ReadMetrics, SealReader, SealSnapshot,
};
use crate::chunk::compress::{Compression, decompress_framed, encode_persisted_block};
use crate::native_base::lifecycle::variant::{
    LayoutIdentity, LayoutVariantRequest, validate_layout_variant,
};
use crate::native_base::wire::container::{Codec, ObjectKind, features, set_declared_features};
use crate::native_base::wire::datapack::{PackBuilder, PackFrame, ScrubbedPack};
use crate::native_base::wire::error::WireError;
use crate::native_base::wire::frame::PayloadFormat;
use crate::native_base::wire::page::IndexPage;
use crate::native_base::wire::refs::{
    ChildRef, ObjectId, ObjectRef, PageAddress, PageKind, RootRef,
};

const BLOCK_SIZE: u32 = 256;
const SLICE: u64 = 7;

const PACK_ID: ObjectId = [0x11; 16];
const LOOSE_ID: ObjectId = [0x33; 16];
const SEAL_A_ID: ObjectId = [0x44; 16];
/// The pack a lossless repack publishes; the source pack stays untouched.
const REPACK_ID: ObjectId = [0x55; 16];

/// Object backend that counts every Range GET. The whole point of PR03's
/// evidence requirements is that these counts are exact: one GET per
/// deduplicated unit, zero GETs when the plan must fail before I/O.
struct CountingSource {
    objects: BTreeMap<ObjectId, Vec<u8>>,
    gets: RefCell<usize>,
    gets_per_object: RefCell<BTreeMap<ObjectId, usize>>,
    bytes_fetched: RefCell<u64>,
    /// Flip one byte of the returned range when it covers `at`.
    corrupt_at: RefCell<Option<(ObjectId, u64)>>,
    /// Return one byte short from every fetch.
    short_reads: bool,
    /// Cancel this token after the first GET (tests that later units are
    /// never fetched once cancellation is observed).
    cancel_after_first_get: Option<CancelToken>,
    /// Serve these replacement bytes from the Nth GET of this object onwards:
    /// a container that changes underneath a reader ("root switch").
    swap_after: RefCell<Option<(ObjectId, usize, Vec<u8>)>>,
}

impl CountingSource {
    fn new(objects: Vec<(ObjectId, Vec<u8>)>) -> CountingSource {
        CountingSource {
            objects: objects.into_iter().collect(),
            gets: RefCell::new(0),
            gets_per_object: RefCell::new(BTreeMap::new()),
            bytes_fetched: RefCell::new(0),
            corrupt_at: RefCell::new(None),
            short_reads: false,
            cancel_after_first_get: None,
            swap_after: RefCell::new(None),
        }
    }

    fn gets(&self) -> usize {
        *self.gets.borrow()
    }

    fn gets_for(&self, id: &ObjectId) -> usize {
        *self.gets_per_object.borrow().get(id).unwrap_or(&0)
    }

    fn bytes_fetched(&self) -> u64 {
        *self.bytes_fetched.borrow()
    }
}

impl ObjectSource for CountingSource {
    fn get_range(
        &self,
        object_id: &ObjectId,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>, ObjectSourceError> {
        *self.gets.borrow_mut() += 1;
        *self
            .gets_per_object
            .borrow_mut()
            .entry(*object_id)
            .or_insert(0) += 1;
        if let Some(token) = &self.cancel_after_first_get {
            token.cancel();
        }
        let object = self
            .objects
            .get(object_id)
            .ok_or(ObjectSourceError::NotFound)?;
        let len = object.len() as u64;
        if start > len || end > len || end < start {
            return Err(ObjectSourceError::Backend(format!(
                "range {start}..{end} invalid for object len {len}"
            )));
        }
        let mut out = object[start as usize..end as usize].to_vec();
        if let Some((id, after, replacement)) = self.swap_after.borrow().as_ref() {
            let served = *self.gets_per_object.borrow().get(object_id).unwrap_or(&0);
            if *id == *object_id && served >= *after && replacement.len() == object.len() {
                out = replacement[start as usize..end as usize].to_vec();
            }
        }
        if self.short_reads && !out.is_empty() {
            out.pop();
        }
        if let Some((id, at)) = *self.corrupt_at.borrow() {
            if id == *object_id && at >= start && at < end {
                let pos = (at - start) as usize;
                out[pos] ^= 0xff;
            }
        }
        *self.bytes_fetched.borrow_mut() += out.len() as u64;
        Ok(out)
    }
}

fn object_ref(id: ObjectId, kind: u8, bytes: &[u8], key: &[u8]) -> ObjectRef {
    ObjectRef {
        object_id: id,
        kind,
        object_len: bytes.len() as u64,
        full_hash: Sha256::digest(bytes).into(),
        key: key.to_vec(),
    }
}

/// Build a DataPack object and its scrubbed view.
fn build_pack(frames: Vec<PackFrame>) -> (Vec<u8>, ScrubbedPack) {
    let mut builder = PackBuilder::new();
    for frame in frames {
        builder.push(frame);
    }
    let bytes = builder.build().unwrap();
    let scrubbed = ScrubbedPack::scrub(&bytes).unwrap();
    (bytes, scrubbed)
}

/// Register every scrubbed frame of a pack in the seal builder, returning
/// the frame slots (seal-scoped, assigned in pack order).
fn register_frames(
    seal: &mut SealBuilder,
    scrubbed: &ScrubbedPack,
    object_ordinal: u32,
) -> Vec<u64> {
    let mut slots = Vec::new();
    for frame in &scrubbed.frames {
        let slot = seal.next_frame_slot();
        seal.add_frame(slot, FrameDescriptor::from_scrubbed(frame, object_ordinal));
        slots.push(slot);
    }
    slots
}

fn span(block_offset: u32, length: u32, slot: u64, raw_off: u32) -> Span {
    Span {
        block_offset,
        length,
        frame_slot: slot,
        frame_raw_offset: raw_off,
    }
}

/// Keep a snapshot alive next to the reader for the test's lifetime.
struct Opened<'a> {
    snapshot: SealSnapshot,
    source: &'a dyn ObjectSource,
}

impl<'a> Opened<'a> {
    fn new(seal_bytes: Vec<u8>, source: &'a dyn ObjectSource) -> Opened<'a> {
        Opened {
            snapshot: SealSnapshot::open(seal_bytes).unwrap(),
            source,
        }
    }

    fn reader(&self) -> SealReader<'_> {
        SealReader::new(&self.snapshot, self.source)
    }
}

/// The mixed fixture: five *short* blocks (each smaller than block_size)
/// over one pack object — plain frame, shared plain frame (two spans),
/// zstd plain frame, native SF-framed frame — plus one loose native object.
/// Short blocks exercise per-block reads; multi-block range reads use the
/// full-size fixture below.
struct MixedFixture {
    seal_bytes: Vec<u8>,
    pack_bytes: Vec<u8>,
    loose_bytes: Vec<u8>,
    blocks: Vec<Vec<u8>>,
    shared_slot: u64,
    zstd_slot: u64,
}

fn mixed_fixture() -> MixedFixture {
    let block0 = b"plain frame block 0: ordinary plain bytes ........".to_vec();
    let block1_a = b"shared frame part A".to_vec();
    let block1_b = b" + shared frame part B".to_vec();
    let block1 = [block1_a.clone(), block1_b.clone()].concat();
    let block2 = b"zstd frame block 2: some moderately compressible \
                   content content content content"
        .to_vec();
    let block3 = b"native frame block 3: stored through the SF codec".to_vec();
    let block4 = b"loose native object block 4: its own versioned object".to_vec();

    let native_inner = encode_persisted_block(&block3, Compression::None).to_vec();
    let shared_raw = [block1_a.clone(), block1_b.clone()].concat();

    let (pack_bytes, scrubbed) = build_pack(vec![
        PackFrame::plain_bytes(&block0).unwrap(),
        PackFrame::plain_bytes(&shared_raw).unwrap(),
        PackFrame::plain_bytes_zstd(&block2, 3).unwrap(),
        PackFrame::native_block_v1(&native_inner).unwrap(),
    ]);

    let loose_bytes = encode_persisted_block(&block4, Compression::Zstd(3)).to_vec();

    let mut seal = SealBuilder::new(BLOCK_SIZE);
    let pack_ordinal = seal.next_object_ordinal();
    seal.add_object(
        pack_ordinal,
        object_ref(
            PACK_ID,
            ObjectKind::DataPack.as_u8(),
            &pack_bytes,
            b"fixture/pack",
        ),
    );
    let slots = register_frames(&mut seal, &scrubbed, pack_ordinal);
    let [plain_slot, shared_slot, zstd_slot, native_slot] =
        [slots[0], slots[1], slots[2], slots[3]];

    seal.add_packed_block(
        SLICE,
        0,
        &block0,
        vec![span(0, block0.len() as u32, plain_slot, 0)],
    )
    .unwrap();
    seal.add_packed_block(
        SLICE,
        1,
        &block1,
        vec![
            span(0, block1_a.len() as u32, shared_slot, 0),
            span(
                block1_a.len() as u32,
                block1_b.len() as u32,
                shared_slot,
                block1_a.len() as u32,
            ),
        ],
    )
    .unwrap();
    seal.add_packed_block(
        SLICE,
        2,
        &block2,
        vec![span(0, block2.len() as u32, zstd_slot, 0)],
    )
    .unwrap();
    seal.add_packed_block(
        SLICE,
        3,
        &block3,
        vec![span(0, block3.len() as u32, native_slot, 0)],
    )
    .unwrap();
    seal.add_loose_block(
        SLICE,
        4,
        &loose_bytes,
        object_ref(LOOSE_ID, NATIVE_LOOSE_KIND, &loose_bytes, b"fixture/loose"),
    )
    .unwrap();

    seal.validate().unwrap();
    let seal_bytes = seal.build().unwrap();
    MixedFixture {
        seal_bytes,
        pack_bytes,
        loose_bytes,
        blocks: vec![block0, block1, block2, block3, block4],
        shared_slot,
        zstd_slot,
    }
}

/// Full-size fixture: five blocks of exactly `BLOCK_SIZE` bytes (a
/// multi-block range read must not cross block padding), three of them
/// sharing one plain frame, one zstd frame, one loose native object.
struct FullFixture {
    seal_bytes: Vec<u8>,
    pack_bytes: Vec<u8>,
    loose_bytes: Vec<u8>,
    blocks: Vec<Vec<u8>>,
}

fn full_blocks_fixture() -> FullFixture {
    let bs = BLOCK_SIZE as usize;
    let block = |seed: u8| vec![seed; bs];
    let b0 = block(0xA0);
    let b1 = block(0xA1);
    let b2 = block(0xA2);
    let b3 = block(0xA3);
    let b4 = block(0xA4);

    let shared_raw = [b0.clone(), b1.clone(), b2.clone()].concat();
    let (pack_bytes, scrubbed) = build_pack(vec![
        PackFrame::plain_bytes(&shared_raw).unwrap(),
        PackFrame::plain_bytes_zstd(&b3, 3).unwrap(),
    ]);
    let loose_bytes = encode_persisted_block(&b4, Compression::Zstd(3)).to_vec();

    let mut seal = SealBuilder::new(BLOCK_SIZE);
    let pack_ordinal = seal.next_object_ordinal();
    seal.add_object(
        pack_ordinal,
        object_ref(
            PACK_ID,
            ObjectKind::DataPack.as_u8(),
            &pack_bytes,
            b"fixture/full-pack",
        ),
    );
    let slots = register_frames(&mut seal, &scrubbed, pack_ordinal);
    let [shared_slot, zstd_slot] = [slots[0], slots[1]];

    let add_plain = |seal: &mut SealBuilder, idx: u32, raw_off: u32, bytes: &Vec<u8>| {
        seal.add_packed_block(
            SLICE,
            idx,
            bytes,
            vec![span(0, bs as u32, shared_slot, raw_off)],
        )
        .unwrap();
    };
    add_plain(&mut seal, 0, 0, &b0);
    add_plain(&mut seal, 1, bs as u32, &b1);
    add_plain(&mut seal, 2, 2 * bs as u32, &b2);
    seal.add_packed_block(SLICE, 3, &b3, vec![span(0, bs as u32, zstd_slot, 0)])
        .unwrap();
    seal.add_loose_block(
        SLICE,
        4,
        &loose_bytes,
        object_ref(
            LOOSE_ID,
            NATIVE_LOOSE_KIND,
            &loose_bytes,
            b"fixture/full-loose",
        ),
    )
    .unwrap();

    seal.validate().unwrap();
    let seal_bytes = seal.build().unwrap();
    FullFixture {
        seal_bytes,
        pack_bytes,
        loose_bytes,
        blocks: vec![b0, b1, b2, b3, b4],
    }
}

// ---------------------------------------------------------------------------
// Happy path: exact byte equality, exact GET counts.
// ---------------------------------------------------------------------------

#[test]
fn mixed_pack_and_loose_blocks_read_back_exactly() {
    let fx = mixed_fixture();
    let source = CountingSource::new(vec![
        (PACK_ID, fx.pack_bytes.clone()),
        (LOOSE_ID, fx.loose_bytes.clone()),
    ]);
    let opened = Opened::new(fx.seal_bytes.clone(), &source);

    for (i, expected) in fx.blocks.iter().enumerate() {
        let got = opened
            .reader()
            .read_block(SLICE, i as u32, BLOCK_SIZE)
            .unwrap();
        assert_eq!(&got, expected, "block {i} mismatch");
    }

    // Partial read inside block 1, which is placed as two spans into the
    // shared frame: the scatter must stitch both spans at the right
    // coordinates.
    let start = BLOCK_SIZE as u64 + 5;
    let got = opened
        .reader()
        .read_range(SLICE, start, 25, BLOCK_SIZE)
        .unwrap();
    assert_eq!(got, fx.blocks[1][5..30]);

    // Empty range is empty without any I/O.
    let before = source.gets();
    assert!(
        opened
            .reader()
            .read_range(SLICE, 0, 0, BLOCK_SIZE)
            .unwrap()
            .is_empty()
    );
    assert_eq!(source.gets(), before);
}

#[test]
fn cross_block_read_over_full_size_blocks() {
    let fx = full_blocks_fixture();
    let source = CountingSource::new(vec![
        (PACK_ID, fx.pack_bytes.clone()),
        (LOOSE_ID, fx.loose_bytes.clone()),
    ]);
    let opened = Opened::new(fx.seal_bytes.clone(), &source);

    // A range starting mid-block-0 through the end of block 3: every
    // covered block is full-size, so the plan never crosses padding.
    let start = 100u64;
    let len = 4 * BLOCK_SIZE as u64 - 100;
    let got = opened
        .reader()
        .read_range(SLICE, start, len, BLOCK_SIZE)
        .unwrap();
    let mut expected = Vec::new();
    expected.extend_from_slice(&fx.blocks[0][100..]);
    expected.extend_from_slice(&fx.blocks[1]);
    expected.extend_from_slice(&fx.blocks[2]);
    expected.extend_from_slice(&fx.blocks[3]);
    assert_eq!(got, expected);

    // The whole 5-block file: 3 shared-frame blocks are ONE frame fetch,
    // plus the zstd frame and the loose object — 3 GETs total.
    let source2 = CountingSource::new(vec![
        (PACK_ID, fx.pack_bytes.clone()),
        (LOOSE_ID, fx.loose_bytes.clone()),
    ]);
    let opened2 = Opened::new(fx.seal_bytes.clone(), &source2);
    let got = opened2
        .reader()
        .read_range(SLICE, 0, 5 * BLOCK_SIZE as u64, BLOCK_SIZE)
        .unwrap();
    let expected = fx.blocks.concat();
    assert_eq!(got, expected);
    assert_eq!(source2.gets(), 3, "fake counting: 3 units, 3 GETs");
    assert_eq!(source2.gets_for(&PACK_ID), 2);
    assert_eq!(source2.gets_for(&LOOSE_ID), 1);
}

#[test]
fn shared_frame_is_fetched_once_per_read() {
    let fx = mixed_fixture();
    let source = CountingSource::new(vec![
        (PACK_ID, fx.pack_bytes.clone()),
        (LOOSE_ID, fx.loose_bytes.clone()),
    ]);
    let opened = Opened::new(fx.seal_bytes.clone(), &source);

    // Block 1 is placed as two spans into one shared frame: exactly one
    // Range GET against the pack object.
    opened.reader().read_block(SLICE, 1, BLOCK_SIZE).unwrap();
    assert_eq!(
        source.gets_for(&PACK_ID),
        1,
        "fake counting: shared frame must be one GET"
    );

    // Per-call dedup (spec 04 §9): a fresh read plans and fetches again.
    opened.reader().read_block(SLICE, 1, BLOCK_SIZE).unwrap();
    assert_eq!(source.gets_for(&PACK_ID), 2);
}

#[test]
fn metrics_report_each_component_separately() {
    let fx = full_blocks_fixture();
    let source = CountingSource::new(vec![
        (PACK_ID, fx.pack_bytes.clone()),
        (LOOSE_ID, fx.loose_bytes.clone()),
    ]);
    let opened = Opened::new(fx.seal_bytes.clone(), &source);
    let mut budget = ReadBudget::recommended();
    let cancel = CancelToken::new();
    let mut metrics = ReadMetrics::default();
    let len = 5 * BLOCK_SIZE as u64;
    opened
        .reader()
        .read_range_with(
            SLICE,
            0,
            len,
            BLOCK_SIZE,
            &mut budget,
            &cancel,
            &mut metrics,
        )
        .unwrap();

    assert_eq!(metrics.requested_file_bytes, len);
    assert_eq!(metrics.logical_data_bytes, len);
    assert_eq!(metrics.range_gets, 3);
    assert_eq!(metrics.frames_fetched, 2);
    assert_eq!(metrics.loose_objects_fetched, 1);
    assert_eq!(metrics.fetched_stored_bytes, source.bytes_fetched());
    // decoded payload bytes: the outer-codec-undone raw of every frame
    // (768 shared + 256 zstd).
    assert_eq!(metrics.decoded_payload_bytes, 4 * BLOCK_SIZE as u64);
    // native decoded bytes: the loose block, decoded from SF framing once.
    assert_eq!(metrics.native_decoded_bytes, BLOCK_SIZE as u64);
    assert_eq!(metrics.discarded_prefetch_bytes, 0);
    assert_eq!(metrics.merged_request_extra_bytes, 0);
    // Budget tokens are returned after the read.
    assert_eq!(budget.inflight(), (0, 0));
}

// ---------------------------------------------------------------------------
// Missing data is an error, never zeros.
// ---------------------------------------------------------------------------

#[test]
fn missing_frame_descriptor_is_an_error_not_zeros() {
    let fx = mixed_fixture();
    let scrubbed = ScrubbedPack::scrub(&fx.pack_bytes).unwrap();
    // The frames table exists (slot 0 registered) but the placement
    // references slot 999, which has no descriptor.
    let mut seal = SealBuilder::new(BLOCK_SIZE);
    seal.add_object(
        0,
        object_ref(PACK_ID, ObjectKind::DataPack.as_u8(), &fx.pack_bytes, b"k"),
    );
    seal.add_frame(0, FrameDescriptor::from_scrubbed(&scrubbed.frames[0], 0));
    seal.add_packed_block(
        SLICE,
        0,
        &fx.blocks[0],
        vec![span(0, fx.blocks[0].len() as u32, 999, 0)],
    )
    .unwrap();
    let seal_bytes = seal.build().unwrap();

    let source = CountingSource::new(vec![(PACK_ID, fx.pack_bytes.clone())]);
    let opened = Opened::new(seal_bytes, &source);
    let err = opened
        .reader()
        .read_block(SLICE, 0, BLOCK_SIZE)
        .unwrap_err();
    assert!(
        matches!(
            err,
            SealError::KeyNotFound {
                table: TableId::Frames,
                ..
            }
        ),
        "unexpected error: {err:?}"
    );
    assert_eq!(source.gets(), 0, "no I/O before metadata resolution");
}

#[test]
fn source_not_found_is_an_error_not_zeros() {
    let fx = mixed_fixture();
    // The source does not hold the pack object at all.
    let source = CountingSource::new(vec![(LOOSE_ID, fx.loose_bytes.clone())]);
    let opened = Opened::new(fx.seal_bytes.clone(), &source);
    let err = opened
        .reader()
        .read_block(SLICE, 0, BLOCK_SIZE)
        .unwrap_err();
    assert!(
        matches!(err, SealError::Source(ObjectSourceError::NotFound)),
        "{err:?}"
    );
}

#[test]
fn short_read_is_an_error_not_zeros() {
    let fx = mixed_fixture();
    let mut source = CountingSource::new(vec![
        (PACK_ID, fx.pack_bytes.clone()),
        (LOOSE_ID, fx.loose_bytes.clone()),
    ]);
    source.short_reads = true;
    let opened = Opened::new(fx.seal_bytes.clone(), &source);
    let err = opened
        .reader()
        .read_block(SLICE, 0, BLOCK_SIZE)
        .unwrap_err();
    assert!(
        matches!(err, SealError::Source(ObjectSourceError::ShortRead { .. })),
        "{err:?}"
    );
}

#[test]
fn corrupted_frame_payload_is_an_error_not_zeros() {
    let fx = mixed_fixture();
    // Corrupt one byte inside frame 0's stored payload on the source side.
    let scrubbed = ScrubbedPack::scrub(&fx.pack_bytes).unwrap();
    let frame0 = &scrubbed.frames[0];
    let corrupt_at = frame0.object_offset + 80 + 2;
    let source = CountingSource::new(vec![
        (PACK_ID, fx.pack_bytes.clone()),
        (LOOSE_ID, fx.loose_bytes.clone()),
    ]);
    *source.corrupt_at.borrow_mut() = Some((PACK_ID, corrupt_at));
    let opened = Opened::new(fx.seal_bytes.clone(), &source);
    let err = opened
        .reader()
        .read_block(SLICE, 0, BLOCK_SIZE)
        .unwrap_err();
    // The frame's raw digest or the block content hash must catch it.
    assert!(
        matches!(
            err,
            SealError::Wire(crate::native_base::wire::error::WireError::HashMismatch { .. })
                | SealError::Integrity(_)
        ),
        "{err:?}"
    );
}

#[test]
fn binding_without_placement_is_an_error() {
    let fx = mixed_fixture();
    let mut seal = SealBuilder::new(BLOCK_SIZE);
    seal.add_binding(SLICE, 9, BlockBinding::of_decoded(&fx.blocks[0]));
    let seal_bytes = seal.build().unwrap();

    let source = CountingSource::new(vec![]);
    let opened = Opened::new(seal_bytes, &source);
    assert!(matches!(
        opened.reader().verify_key_sets().unwrap_err(),
        SealError::KeySetMismatch(_)
    ));
    assert!(matches!(
        opened.reader().resolve_block(SLICE, 9).unwrap_err(),
        SealError::Placement(_)
    ));
}

#[test]
fn read_beyond_decoded_len_is_an_error_not_zeros() {
    let fx = mixed_fixture();
    let source = CountingSource::new(vec![
        (PACK_ID, fx.pack_bytes.clone()),
        (LOOSE_ID, fx.loose_bytes.clone()),
    ]);
    let opened = Opened::new(fx.seal_bytes.clone(), &source);
    // Block 0 is shorter than block_size; reading into the padding is an
    // error, not a zero-filled tail.
    let short = fx.blocks[0].len() as u64;
    let err = opened
        .reader()
        .read_range(SLICE, 0, BLOCK_SIZE as u64, BLOCK_SIZE)
        .unwrap_err();
    assert!(
        matches!(err, SealError::RangeBeyondBlock { decoded_len, .. } if decoded_len == short),
        "{err:?}"
    );
}

// ---------------------------------------------------------------------------
// Native/Plain boundary.
// ---------------------------------------------------------------------------

#[test]
fn plain_bytes_starting_with_the_sf_magic_stay_raw() {
    // A PlainBytes frame whose decoded content coincidentally starts with
    // the SF framing magic must be copied verbatim — never routed through
    // the native codec. The Native/Plain boundary is the payload format,
    // not a content sniff.
    let block: Vec<u8> = [0x53u8, 0x46, 0x00, 0x00]
        .into_iter()
        .chain(std::iter::repeat(b'\xAB').take(60))
        .collect();
    let (pack_bytes, scrubbed) = build_pack(vec![PackFrame::plain_bytes(&block).unwrap()]);

    let mut seal = SealBuilder::new(BLOCK_SIZE);
    seal.add_object(
        0,
        object_ref(PACK_ID, ObjectKind::DataPack.as_u8(), &pack_bytes, b"k"),
    );
    let slots = register_frames(&mut seal, &scrubbed, 0);
    seal.add_packed_block(
        SLICE,
        0,
        &block,
        vec![span(0, block.len() as u32, slots[0], 0)],
    )
    .unwrap();
    let seal_bytes = seal.build().unwrap();

    let source = CountingSource::new(vec![(PACK_ID, pack_bytes.clone())]);
    let opened = Opened::new(seal_bytes, &source);
    let got = opened.reader().read_block(SLICE, 0, BLOCK_SIZE).unwrap();
    assert_eq!(got, block);
}

#[test]
fn native_multi_span_placement_rejected_at_read_time() {
    let fx = mixed_fixture();
    let native_inner = encode_persisted_block(&fx.blocks[3], Compression::None).to_vec();
    let (pack_bytes, scrubbed) = build_pack(vec![
        PackFrame::plain_bytes(&fx.blocks[0]).unwrap(),
        PackFrame::native_block_v1(&native_inner).unwrap(),
    ]);
    let mut seal = SealBuilder::new(BLOCK_SIZE);
    seal.add_object(
        0,
        object_ref(PACK_ID, ObjectKind::DataPack.as_u8(), &pack_bytes, b"k"),
    );
    let slots = register_frames(&mut seal, &scrubbed, 0);
    let native_slot = slots[1];
    let decoded_len = fx.blocks[3].len() as u32;

    // Two contiguous spans over one native frame: valid coverage, invalid
    // for NativeBlockV1 (exactly one span required). The builder's
    // validate() catches it; here we skip validate and prove the reader
    // rejects it too.
    seal.add_packed_block(
        SLICE,
        0,
        &fx.blocks[3],
        vec![
            span(0, decoded_len / 2, native_slot, 0),
            span(
                decoded_len / 2,
                decoded_len - decoded_len / 2,
                native_slot,
                0,
            ),
        ],
    )
    .unwrap();
    assert!(matches!(seal.validate().unwrap_err(), SealError::Wire(_)));
    let seal_bytes = seal.build().unwrap();

    let source = CountingSource::new(vec![(PACK_ID, pack_bytes.clone())]);
    let opened = Opened::new(seal_bytes, &source);
    let err = opened
        .reader()
        .read_block(SLICE, 0, BLOCK_SIZE)
        .unwrap_err();
    assert!(matches!(err, SealError::Wire(_)), "{err:?}");
    assert_eq!(source.gets(), 0, "rejected before any I/O");
}

#[test]
fn native_span_length_must_not_borrow_frame_raw_len() {
    // The span length lives in the decoded domain. A writer that uses the
    // frame's raw_len (the encoded inner length) as the block length
    // produces a self-consistent-looking seal whose binding describes the
    // encoded bytes; the read must fail the length cross-check, never
    // return the encoded bytes as if they were the block.
    let block = vec![0xC7u8; 100];
    let native_inner = encode_persisted_block(&block, Compression::None).to_vec();
    assert_eq!(native_inner.len(), block.len() + 4);
    let (pack_bytes, scrubbed) =
        build_pack(vec![PackFrame::native_block_v1(&native_inner).unwrap()]);

    let mut seal = SealBuilder::new(BLOCK_SIZE);
    seal.add_object(
        0,
        object_ref(PACK_ID, ObjectKind::DataPack.as_u8(), &pack_bytes, b"k"),
    );
    let slots = register_frames(&mut seal, &scrubbed, 0);
    // "Decoded" content = the encoded inner bytes (the classic confusion).
    let raw_len = native_inner.len() as u32;
    seal.add_packed_block(SLICE, 0, &native_inner, vec![span(0, raw_len, slots[0], 0)])
        .unwrap();
    let seal_bytes = seal.build().unwrap();

    let source = CountingSource::new(vec![(PACK_ID, pack_bytes.clone())]);
    let opened = Opened::new(seal_bytes, &source);
    let err = opened
        .reader()
        .read_block(SLICE, 0, BLOCK_SIZE)
        .unwrap_err();
    assert!(
        matches!(err, SealError::Integrity(_)),
        "raw_len-as-decoded_len must fail the length cross-check: {err:?}"
    );
}

#[test]
fn sf_framed_native_block_roundtrips_through_the_native_path() {
    // Sanity in the codec direction: a genuine native frame decodes and
    // yields the original block (the loose path shares this framing).
    let fx = mixed_fixture();
    let decoded: Vec<u8> =
        decompress_framed(&encode_persisted_block(&fx.blocks[3], Compression::Zstd(3)))
            .unwrap()
            .to_vec();
    assert_eq!(decoded, fx.blocks[3]);
}

// ---------------------------------------------------------------------------
// Descriptor/verification boundaries.
// ---------------------------------------------------------------------------

#[test]
fn frame_descriptor_mismatch_with_actual_header_is_an_error() {
    let fx = mixed_fixture();
    let mut seal = SealBuilder::new(BLOCK_SIZE);
    seal.add_object(
        0,
        object_ref(PACK_ID, ObjectKind::DataPack.as_u8(), &fx.pack_bytes, b"k"),
    );
    let scrubbed = ScrubbedPack::scrub(&fx.pack_bytes).unwrap();
    let mut descriptor = FrameDescriptor::from_scrubbed(&scrubbed.frames[0], 0);
    // Announce the wrong ordinal: the fetched header disagrees.
    descriptor.frame_ordinal += 1;
    seal.add_frame(0, descriptor);
    seal.add_packed_block(
        SLICE,
        0,
        &fx.blocks[0],
        vec![span(0, fx.blocks[0].len() as u32, 0, 0)],
    )
    .unwrap();
    let seal_bytes = seal.build().unwrap();

    let source = CountingSource::new(vec![(PACK_ID, fx.pack_bytes.clone())]);
    let opened = Opened::new(seal_bytes, &source);
    let err = opened
        .reader()
        .read_block(SLICE, 0, BLOCK_SIZE)
        .unwrap_err();
    assert!(matches!(err, SealError::DescriptorMismatch(_)), "{err:?}");
}

#[test]
fn descriptor_pointing_at_a_non_frame_offset_is_an_error() {
    let fx = mixed_fixture();
    let scrubbed = ScrubbedPack::scrub(&fx.pack_bytes).unwrap();
    let mut seal = SealBuilder::new(BLOCK_SIZE);
    seal.add_object(
        0,
        object_ref(PACK_ID, ObjectKind::DataPack.as_u8(), &fx.pack_bytes, b"k"),
    );
    let mut descriptor = FrameDescriptor::from_scrubbed(&scrubbed.frames[0], 0);
    // Shift the object offset by one: no frame header lives there.
    descriptor.object_offset += 1;
    seal.add_frame(0, descriptor);
    seal.add_packed_block(
        SLICE,
        0,
        &fx.blocks[0],
        vec![span(0, fx.blocks[0].len() as u32, 0, 0)],
    )
    .unwrap();
    let seal_bytes = seal.build().unwrap();

    let source = CountingSource::new(vec![(PACK_ID, fx.pack_bytes.clone())]);
    let opened = Opened::new(seal_bytes, &source);
    assert!(opened.reader().read_block(SLICE, 0, BLOCK_SIZE).is_err());
}

#[test]
fn loose_object_corruption_is_an_error() {
    let fx = mixed_fixture();
    let source = CountingSource::new(vec![
        (PACK_ID, fx.pack_bytes.clone()),
        (LOOSE_ID, fx.loose_bytes.clone()),
    ]);
    *source.corrupt_at.borrow_mut() = Some((LOOSE_ID, 3));
    let opened = Opened::new(fx.seal_bytes.clone(), &source);
    let err = opened
        .reader()
        .read_block(SLICE, 4, BLOCK_SIZE)
        .unwrap_err();
    assert!(
        matches!(err, SealError::Integrity(_) | SealError::Native(_)),
        "{err:?}"
    );
}

// ---------------------------------------------------------------------------
// Budget and cancellation.
// ---------------------------------------------------------------------------

#[test]
fn undersized_budget_rejects_before_any_io() {
    let fx = mixed_fixture();
    let source = CountingSource::new(vec![
        (PACK_ID, fx.pack_bytes.clone()),
        (LOOSE_ID, fx.loose_bytes.clone()),
    ]);
    let opened = Opened::new(fx.seal_bytes.clone(), &source);
    let mut budget = ReadBudget::new(1, 1);
    let cancel = CancelToken::new();
    let mut metrics = ReadMetrics::default();
    let block_len = fx.blocks[0].len() as u64;
    let err = opened
        .reader()
        .read_range_with(
            SLICE,
            0,
            block_len,
            BLOCK_SIZE,
            &mut budget,
            &cancel,
            &mut metrics,
        )
        .unwrap_err();
    assert!(
        matches!(err, SealError::BudgetUnitTooLarge { .. }),
        "{err:?}"
    );
    assert_eq!(source.gets(), 0, "budget rejection happens before I/O");
    assert_eq!(
        budget.inflight(),
        (0, 0),
        "failed reservation leaks nothing"
    );
}

#[test]
fn budget_that_fits_only_some_units_rejects_the_whole_plan() {
    let fx = full_blocks_fixture();
    let source = CountingSource::new(vec![
        (PACK_ID, fx.pack_bytes.clone()),
        (LOOSE_ID, fx.loose_bytes.clone()),
    ]);
    let opened = Opened::new(fx.seal_bytes.clone(), &source);
    // Cap sized to the largest single unit (the shared frame: 80-byte
    // header + 768 stored bytes) so each unit fits alone, but the sum of
    // the plan's three units exceeds it.
    let shared_unit = 80 + 3 * BLOCK_SIZE as u64;
    let mut budget = ReadBudget::new(shared_unit, 3 * BLOCK_SIZE as u64);
    let cancel = CancelToken::new();
    let mut metrics = ReadMetrics::default();
    let err = opened
        .reader()
        .read_range_with(
            SLICE,
            0,
            5 * BLOCK_SIZE as u64,
            BLOCK_SIZE,
            &mut budget,
            &cancel,
            &mut metrics,
        )
        .unwrap_err();
    assert!(
        matches!(err, SealError::BudgetUnitTooLarge { .. }),
        "{err:?}"
    );
    assert_eq!(source.gets(), 0);
    assert_eq!(budget.inflight(), (0, 0));
}

/// RES-002: the plan's segment count is bounded before any unit is resolved
/// or any task/future is constructed, so a million-segment request cannot
/// allocate an unbounded plan.
#[test]
fn a_plan_beyond_the_segment_bound_is_refused_before_any_io() {
    let fx = mixed_fixture();
    let source = CountingSource::new(vec![
        (PACK_ID, fx.pack_bytes.clone()),
        (LOOSE_ID, fx.loose_bytes.clone()),
    ]);
    let opened = Opened::new(fx.seal_bytes.clone(), &source);
    let mut budget = ReadBudget::recommended();
    let cancel = CancelToken::new();
    let mut metrics = ReadMetrics::default();
    let err = opened
        .reader()
        .read_range_with(
            SLICE,
            0,
            (super::MAX_PLAN_BLOCKS + 1) * BLOCK_SIZE as u64,
            BLOCK_SIZE,
            &mut budget,
            &cancel,
            &mut metrics,
        )
        .unwrap_err();
    assert!(matches!(err, SealError::PlanLimit(_)), "{err:?}");
    assert_eq!(source.gets(), 0, "the bound is checked before any fetch");
    assert_eq!(budget.inflight(), (0, 0));
}

#[test]
fn pre_cancelled_read_does_no_io() {
    let fx = mixed_fixture();
    let source = CountingSource::new(vec![
        (PACK_ID, fx.pack_bytes.clone()),
        (LOOSE_ID, fx.loose_bytes.clone()),
    ]);
    let opened = Opened::new(fx.seal_bytes.clone(), &source);
    let cancel = CancelToken::new();
    cancel.cancel();
    let mut budget = ReadBudget::recommended();
    let mut metrics = ReadMetrics::default();
    let block_len = fx.blocks[0].len() as u64;
    let err = opened
        .reader()
        .read_range_with(
            SLICE,
            0,
            block_len,
            BLOCK_SIZE,
            &mut budget,
            &cancel,
            &mut metrics,
        )
        .unwrap_err();
    assert!(matches!(err, SealError::Cancelled), "{err:?}");
    assert_eq!(source.gets(), 0);
}

#[test]
fn cancellation_between_units_stops_fetching() {
    let fx = full_blocks_fixture();
    let cancel = CancelToken::new();
    let source = CountingSource {
        objects: vec![
            (PACK_ID, fx.pack_bytes.clone()),
            (LOOSE_ID, fx.loose_bytes.clone()),
        ]
        .into_iter()
        .collect(),
        gets: RefCell::new(0),
        gets_per_object: RefCell::new(BTreeMap::new()),
        bytes_fetched: RefCell::new(0),
        corrupt_at: RefCell::new(None),
        short_reads: false,
        cancel_after_first_get: Some(cancel.clone()),
        swap_after: RefCell::new(None),
    };
    let opened = Opened::new(fx.seal_bytes.clone(), &source);
    // Three units planned (loose object first in unit order); the first GET
    // cancels the token, so the read fails and the remaining units are
    // never fetched.
    let mut budget = ReadBudget::recommended();
    let mut metrics = ReadMetrics::default();
    let err = opened
        .reader()
        .read_range_with(
            SLICE,
            0,
            5 * BLOCK_SIZE as u64,
            BLOCK_SIZE,
            &mut budget,
            &cancel,
            &mut metrics,
        )
        .unwrap_err();
    assert!(matches!(err, SealError::Cancelled), "{err:?}");
    assert_eq!(source.gets(), 1, "only the in-flight unit was fetched");
}

/// RES-003: every budget token follows the real buffer lifetime. A completed
/// read, a failed read and a cancelled read must each release exactly what
/// they reserved: no double release and no leak.
#[test]
fn budget_tokens_follow_the_buffer_lifetime_on_success_error_and_cancel() {
    let fx = full_blocks_fixture();
    let plan_len = 5 * BLOCK_SIZE as u64;

    // Success.
    let source = CountingSource::new(vec![
        (PACK_ID, fx.pack_bytes.clone()),
        (LOOSE_ID, fx.loose_bytes.clone()),
    ]);
    let opened = Opened::new(fx.seal_bytes.clone(), &source);
    let mut budget = ReadBudget::recommended();
    let cancel = CancelToken::new();
    let mut metrics = ReadMetrics::default();
    opened
        .reader()
        .read_range_with(
            SLICE,
            0,
            plan_len,
            BLOCK_SIZE,
            &mut budget,
            &cancel,
            &mut metrics,
        )
        .unwrap();
    assert_eq!(budget.inflight(), (0, 0), "a completed read leaks nothing");

    // Error: a short read fails the plan and must still release the whole
    // reservation before the error is returned.
    let source = CountingSource {
        objects: vec![
            (PACK_ID, fx.pack_bytes.clone()),
            (LOOSE_ID, fx.loose_bytes.clone()),
        ]
        .into_iter()
        .collect(),
        gets: RefCell::new(0),
        gets_per_object: RefCell::new(BTreeMap::new()),
        bytes_fetched: RefCell::new(0),
        corrupt_at: RefCell::new(None),
        short_reads: true,
        cancel_after_first_get: None,
        swap_after: RefCell::new(None),
    };
    let opened = Opened::new(fx.seal_bytes.clone(), &source);
    let mut budget = ReadBudget::recommended();
    let mut metrics = ReadMetrics::default();
    assert!(
        opened
            .reader()
            .read_range_with(
                SLICE,
                0,
                plan_len,
                BLOCK_SIZE,
                &mut budget,
                &cancel,
                &mut metrics,
            )
            .is_err()
    );
    assert_eq!(budget.inflight(), (0, 0), "a failed read leaks nothing");

    // Cancellation: the in-flight unit's tokens are released with its buffer
    // even though the plan stops early.
    let cancel = CancelToken::new();
    let source = CountingSource {
        objects: vec![
            (PACK_ID, fx.pack_bytes.clone()),
            (LOOSE_ID, fx.loose_bytes.clone()),
        ]
        .into_iter()
        .collect(),
        gets: RefCell::new(0),
        gets_per_object: RefCell::new(BTreeMap::new()),
        bytes_fetched: RefCell::new(0),
        corrupt_at: RefCell::new(None),
        short_reads: false,
        cancel_after_first_get: Some(cancel.clone()),
        swap_after: RefCell::new(None),
    };
    let opened = Opened::new(fx.seal_bytes.clone(), &source);
    let mut budget = ReadBudget::recommended();
    let mut metrics = ReadMetrics::default();
    assert!(
        opened
            .reader()
            .read_range_with(
                SLICE,
                0,
                plan_len,
                BLOCK_SIZE,
                &mut budget,
                &cancel,
                &mut metrics,
            )
            .is_err()
    );
    assert_eq!(budget.inflight(), (0, 0), "a cancelled read leaks nothing");
}

// ---------------------------------------------------------------------------
// Index structure.
// ---------------------------------------------------------------------------

#[test]
fn multi_level_index_lookup_hits_and_absent_keys_miss() {
    let block = vec![0x5Au8; 64];
    let (pack_bytes, scrubbed) = build_pack(vec![PackFrame::plain_bytes(&block).unwrap()]);
    // Leaf target 64 bytes: each binding record (~58 encoded bytes) lands
    // on its own leaf, forcing internal levels.
    let mut seal = SealBuilder::new(BLOCK_SIZE).with_leaf_target(64);
    seal.add_object(
        0,
        object_ref(PACK_ID, ObjectKind::DataPack.as_u8(), &pack_bytes, b"k"),
    );
    let slots = register_frames(&mut seal, &scrubbed, 0);
    const N: u32 = 40;
    for i in 0..N {
        seal.add_packed_block(
            SLICE,
            i,
            &block,
            vec![span(0, block.len() as u32, slots[0], 0)],
        )
        .unwrap();
    }
    let seal_bytes = seal.build().unwrap();

    let source = CountingSource::new(vec![(PACK_ID, pack_bytes.clone())]);
    let snapshot = SealSnapshot::open(seal_bytes).unwrap();
    assert!(
        matches!(snapshot.table(TableId::Bindings), Some(ChildRef::Local(addr)) if addr.level >= 2),
        "fixture must build at least two internal levels"
    );
    let reader = SealReader::new(&snapshot, &source);

    for i in [0u32, 1, 17, N - 1] {
        let got = reader.read_block(SLICE, i, BLOCK_SIZE).unwrap();
        assert_eq!(got, block, "block {i}");
    }
    // Absent keys return None, never an error and never fabricated data.
    let missing = binding_key(SLICE, 10_000);
    assert!(
        reader
            .lookup(TableId::Bindings, &missing)
            .unwrap()
            .is_none()
    );
    // A slice with no bindings at all.
    let other_slice = binding_key(SLICE + 1, 0);
    assert!(
        reader
            .lookup(TableId::Bindings, &other_slice)
            .unwrap()
            .is_none()
    );
}

#[test]
fn empty_table_reports_missing_table_on_query() {
    // Only a binding: every other table is None (spec 04 §2).
    let mut seal = SealBuilder::new(BLOCK_SIZE);
    seal.add_binding(SLICE, 0, BlockBinding::of_decoded(b"x"));
    let bytes = seal.build().unwrap();
    let source = CountingSource::new(vec![]);
    let snapshot = SealSnapshot::open(bytes).unwrap();
    let reader = SealReader::new(&snapshot, &source);
    assert!(snapshot.table(TableId::Objects).is_none());
    assert!(matches!(
        reader.object_for(0).unwrap_err(),
        SealError::MissingTable(TableId::Objects)
    ));
}

// ---------------------------------------------------------------------------
// External table reuse.
// ---------------------------------------------------------------------------

#[test]
fn external_frames_table_reuse_reads_through_the_old_seal() {
    // Seal A holds the frames and objects; seal B (a later seal reusing the
    // old frame table, spec 04 §2) holds its own bindings/placements for a
    // new slice and points its Frames/Objects tables at A.
    let block = b"reused frame table block".to_vec();
    let (pack_bytes, scrubbed) = build_pack(vec![PackFrame::plain_bytes(&block).unwrap()]);

    let mut seal_a = SealBuilder::new(BLOCK_SIZE);
    let ordinal = seal_a.next_object_ordinal();
    seal_a.add_object(
        ordinal,
        object_ref(
            PACK_ID,
            ObjectKind::DataPack.as_u8(),
            &pack_bytes,
            b"reuse/pack",
        ),
    );
    let slots = register_frames(&mut seal_a, &scrubbed, ordinal);
    seal_a
        .add_packed_block(
            SLICE,
            0,
            &block,
            vec![span(0, block.len() as u32, slots[0], 0)],
        )
        .unwrap();
    seal_a.validate().unwrap();
    let seal_a_bytes = seal_a.build().unwrap();

    let snapshot_a = SealSnapshot::open(seal_a_bytes.clone()).unwrap();
    let frames_root = snapshot_a.table(TableId::Frames).unwrap().clone();
    let objects_root = snapshot_a.table(TableId::Objects).unwrap().clone();
    let seal_a_ref = object_ref(
        SEAL_A_ID,
        ObjectKind::DataSeal.as_u8(),
        &seal_a_bytes,
        b"reuse/seal-a",
    );

    let external = |root: ChildRef| match root {
        ChildRef::Local(addr) => ChildRef::External(RootRef {
            object: seal_a_ref.clone(),
            address: addr,
        }),
        ChildRef::External(_) => unreachable!("A builds local roots"),
    };
    let mut seal_b = SealBuilder::new(BLOCK_SIZE);
    seal_b.set_table_override(TableId::Frames, Some(external(frames_root)));
    seal_b.set_table_override(TableId::Objects, Some(external(objects_root)));
    // B's own slice reuses A's frame slots without renumbering them.
    seal_b
        .add_packed_block(
            SLICE + 1,
            0,
            &block,
            vec![span(0, block.len() as u32, slots[0], 0)],
        )
        .unwrap();
    let seal_b_bytes = seal_b.build().unwrap();

    let snapshot_b = SealSnapshot::open(seal_b_bytes).unwrap();
    assert!(
        snapshot_b.header().required_features & features::EXTERNAL_INDEX_CHILDREN != 0,
        "external children must be declared in required_features"
    );
    assert!(matches!(
        snapshot_b.table(TableId::Frames),
        Some(ChildRef::External(_))
    ));

    let source = CountingSource::new(vec![
        (PACK_ID, pack_bytes.clone()),
        (SEAL_A_ID, seal_a_bytes.clone()),
    ]);
    let reader = SealReader::new(&snapshot_b, &source);
    let got = reader.read_block(SLICE + 1, 0, BLOCK_SIZE).unwrap();
    assert_eq!(got, block);
    // The frame descriptor and object were read through A's pages.
    assert!(source.gets_for(&SEAL_A_ID) >= 2, "B must read A's tables");
    // And the data itself from the pack.
    assert_eq!(source.gets_for(&PACK_ID), 1);
}

/// GATE-002/GATE-003 on the seal side: a seal whose header stops declaring
/// the external index children it actually points at is refused by the full
/// verify, before any page or block is read. The unpatched object opens and
/// reports a closure whose content bits are already declared.
#[test]
fn undeclared_external_child_is_refused_before_the_seal_is_opened() {
    let block = b"closure smoke block".to_vec();
    let (pack_bytes, scrubbed) = build_pack(vec![PackFrame::plain_bytes(&block).unwrap()]);

    let mut seal_a = SealBuilder::new(BLOCK_SIZE);
    let ordinal = seal_a.next_object_ordinal();
    seal_a.add_object(
        ordinal,
        object_ref(
            PACK_ID,
            ObjectKind::DataPack.as_u8(),
            &pack_bytes,
            b"closure/pack",
        ),
    );
    let slots = register_frames(&mut seal_a, &scrubbed, ordinal);
    seal_a
        .add_packed_block(
            SLICE,
            0,
            &block,
            vec![span(0, block.len() as u32, slots[0], 0)],
        )
        .unwrap();
    seal_a.validate().unwrap();
    let seal_a_bytes = seal_a.build().unwrap();

    let snapshot_a = SealSnapshot::open(seal_a_bytes.clone()).unwrap();
    let frames_root = snapshot_a.table(TableId::Frames).unwrap().clone();
    let seal_a_ref = object_ref(
        SEAL_A_ID,
        ObjectKind::DataSeal.as_u8(),
        &seal_a_bytes,
        b"closure/seal-a",
    );
    let external = |root: ChildRef| match root {
        ChildRef::Local(addr) => ChildRef::External(RootRef {
            object: seal_a_ref.clone(),
            address: addr,
        }),
        ChildRef::External(_) => unreachable!("A builds local roots"),
    };
    let mut seal_b = SealBuilder::new(BLOCK_SIZE);
    seal_b.set_table_override(TableId::Frames, Some(external(frames_root)));
    seal_b
        .add_packed_block(
            SLICE + 1,
            0,
            &block,
            vec![span(0, block.len() as u32, slots[0], 0)],
        )
        .unwrap();
    let honest = seal_b.build().unwrap();

    let snapshot_b = SealSnapshot::open(honest.clone()).unwrap();
    let closure = snapshot_b.feature_closure();
    assert_eq!(closure.undeclared(), 0);
    assert!(
        closure.closure() & features::EXTERNAL_INDEX_CHILDREN != 0,
        "the external root is part of the closure"
    );

    let mut lying = honest;
    set_declared_features(
        &mut lying,
        closure.declared & !features::EXTERNAL_INDEX_CHILDREN,
    );
    let err = SealSnapshot::open(lying).unwrap_err();
    assert!(
        matches!(
            err,
            SealError::Wire(crate::native_base::wire::error::WireError::UnsupportedFormat(_))
        ),
        "{err}"
    );
    assert!(err.to_string().contains("0x4"), "{err}");
}

#[test]
fn table_override_rejects_local_entries() {
    let fx = mixed_fixture();
    let scrubbed = ScrubbedPack::scrub(&fx.pack_bytes).unwrap();
    let mut seal = SealBuilder::new(BLOCK_SIZE);
    let bogus = ChildRef::External(RootRef {
        object: object_ref(
            SEAL_A_ID,
            ObjectKind::DataSeal.as_u8(),
            &fx.pack_bytes,
            b"k",
        ),
        address: PageAddress {
            offset: 64,
            stored_len: 8,
            raw_len: 8,
            codec: Codec::None,
            page_kind: PageKind::GenericKeyValue,
            level: 0,
            entry_count: 1,
            stored_digest: [0; 32],
        },
    });
    seal.set_table_override(TableId::Frames, Some(bogus));
    // Local frame entries plus an override: build must refuse.
    seal.add_frame(0, FrameDescriptor::from_scrubbed(&scrubbed.frames[0], 0));
    assert!(seal.build().is_err());
}

// ---------------------------------------------------------------------------
// Builder-level validation.
// ---------------------------------------------------------------------------

#[test]
fn builder_validate_rejects_missing_frame_and_kind_mismatch() {
    let fx = mixed_fixture();
    let mut seal = SealBuilder::new(BLOCK_SIZE);
    // Placement references slot 5 which was never registered.
    seal.add_packed_block(
        SLICE,
        0,
        &fx.blocks[0],
        vec![span(0, fx.blocks[0].len() as u32, 5, 0)],
    )
    .unwrap();
    assert!(matches!(
        seal.validate().unwrap_err(),
        SealError::Placement(_)
    ));

    // Objects-table kind mismatch: a Packed placement whose object is a
    // NativeLoose object, not DataPack.
    let mut seal = SealBuilder::new(BLOCK_SIZE);
    seal.add_object(
        0,
        object_ref(PACK_ID, NATIVE_LOOSE_KIND, &fx.pack_bytes, b"k"),
    );
    let scrubbed = ScrubbedPack::scrub(&fx.pack_bytes).unwrap();
    seal.add_frame(0, FrameDescriptor::from_scrubbed(&scrubbed.frames[0], 0));
    seal.add_packed_block(
        SLICE,
        0,
        &fx.blocks[0],
        vec![span(0, fx.blocks[0].len() as u32, 0, 0)],
    )
    .unwrap();
    assert!(matches!(
        seal.validate().unwrap_err(),
        SealError::Placement(_)
    ));
}

#[test]
fn builder_add_packed_block_rejects_short_span_coverage() {
    let mut seal = SealBuilder::new(BLOCK_SIZE);
    let err = seal
        .add_packed_block(SLICE, 0, &[1u8; 32], vec![span(0, 16, 0, 0)]) // covers only half
        .unwrap_err();
    assert!(matches!(err, SealError::Wire(_)), "{err:?}");
}

#[test]
fn resolved_placements_and_descriptors_roundtrip_through_the_seal() {
    let fx = mixed_fixture();
    let source = CountingSource::new(vec![]);
    let opened = Opened::new(fx.seal_bytes.clone(), &source);
    let resolved = opened.reader().resolve_block(SLICE, 1).unwrap();
    match &resolved.placement {
        BlockPlacement::Packed { spans } => {
            assert_eq!(spans.len(), 2);
            assert_eq!(spans[0].frame_slot, fx.shared_slot);
            assert_eq!(spans[1].frame_slot, fx.shared_slot);
        }
        other => panic!("expected packed placement, got {other:?}"),
    }
    let resolved = opened.reader().resolve_block(SLICE, 4).unwrap();
    assert!(matches!(resolved.placement, BlockPlacement::Loose { .. }));
    let descriptor = opened.reader().frame_descriptor(fx.zstd_slot).unwrap();
    assert_eq!(descriptor.payload_format, PayloadFormat::PlainBytes);
    assert_eq!(descriptor.codec, Codec::Zstd);
}

#[test]
fn seal_root_table_directory_shape_is_preserved_end_to_end() {
    let fx = mixed_fixture();
    let source = CountingSource::new(vec![]);
    let opened = Opened::new(fx.seal_bytes.clone(), &source);
    let root: &SealRoot = opened.snapshot.root();
    // All four tables present and local in the fixture.
    for table in TableId::ALL {
        assert!(
            matches!(root.table(table), Some(ChildRef::Local(_))),
            "table {table:?} should be a local child"
        );
    }
    // Objects table holds exactly the pack + loose object.
    let mut objects = 0;
    opened
        .reader()
        .for_each_entry(TableId::Objects, |_, _| {
            objects += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(objects, 2);
}

/// Byte-stability guard for the shared index builder (PR06A).
///
/// The BNPG tree writer used by every seal table moved into
/// `wire::index_build` so the inventory and RetainBatch indexes (PR06A) can
/// reuse it. These two digests were captured from the pre-refactor
/// implementation (commit 7291196, via a throwaway probe in a detached
/// worktree) and pin the seal object bytes: the mixed Loose/Packed fixture
/// exercises all four tables, and the second build forces multi-level
/// internal pages with a 64-byte leaf target.
#[test]
fn seal_bytes_are_unchanged_by_the_shared_index_builder() {
    use sha2::Digest;

    let mixed = mixed_fixture();
    assert_eq!(
        hex::encode(sha2::Sha256::digest(&mixed.seal_bytes)),
        "8c361922a9b647f57b282364456c664b3631d9940e1a0eac9335ce909addb8b0"
    );

    let block = vec![0x5Au8; 64];
    let (pack_bytes, scrubbed) = build_pack(vec![PackFrame::plain_bytes(&block).unwrap()]);
    let mut seal = SealBuilder::new(BLOCK_SIZE).with_leaf_target(64);
    seal.add_object(
        0,
        object_ref(PACK_ID, ObjectKind::DataPack.as_u8(), &pack_bytes, b"k"),
    );
    let slots = register_frames(&mut seal, &scrubbed, 0);
    for i in 0..40u32 {
        seal.add_packed_block(
            SLICE,
            i,
            &block,
            vec![span(0, block.len() as u32, slots[0], 0)],
        )
        .unwrap();
    }
    let bytes = seal.build().unwrap();
    assert!(
        matches!(
            SealSnapshot::open(bytes.clone()).unwrap().table(TableId::Bindings),
            Some(ChildRef::Local(addr)) if addr.level >= 2
        ),
        "the second fixture must reach multi-level internal pages"
    );
    assert_eq!(
        hex::encode(sha2::Sha256::digest(&bytes)),
        "a0723bc338b23c90c44641c68c937ab84228e7ea2e482e774b50609be03ef298"
    );
}

const REMOTE_SEAL_ID: ObjectId = [0x77; 16];
const NESTED_PACK_ID: ObjectId = [0x66; 16];
const REUSED_SLICE: u64 = 0x21;

/// READ-005/READ-006 fixture: a remote seal whose Frames table needs more
/// than one index level, plus a local seal that re-uses remote frame slots
/// through external table roots.  Reading the local seal therefore has to
/// locate the remote root page *and* resolve a local child of that external
/// page inside the remote container.
struct NestedExternalFixture {
    remote_bytes: Vec<u8>,
    local_bytes: Vec<u8>,
    pack_bytes: Vec<u8>,
    blocks: Vec<Vec<u8>>,
    /// `(offset, stored_len)` of the remote Frames root page.
    frames_root_page: (u64, u32),
    /// Height of the remote Frames tree (root level + 1).
    frames_tree_levels: usize,
}

fn nested_external_fixture() -> NestedExternalFixture {
    let blocks: Vec<Vec<u8>> = (0..96u32)
        .map(|index| {
            let mut block = vec![0x5A; BLOCK_SIZE as usize];
            block[..4].copy_from_slice(&index.to_be_bytes());
            block
        })
        .collect();
    let (pack_bytes, scrubbed) = build_pack(
        blocks
            .iter()
            .map(|block| PackFrame::plain_bytes(block).unwrap())
            .collect(),
    );

    // A leaf target of 1024 bytes over 96 descriptors of 96 bytes each gives
    // a two-level Frames tree: one internal root page and its local leaves.
    let mut remote = SealBuilder::new(BLOCK_SIZE).with_leaf_target(1024);
    let pack_ordinal = remote.next_object_ordinal();
    remote.add_object(
        pack_ordinal,
        object_ref(
            NESTED_PACK_ID,
            ObjectKind::DataPack.as_u8(),
            &pack_bytes,
            b"nested/pack",
        ),
    );
    let slots = register_frames(&mut remote, &scrubbed, pack_ordinal);
    for (index, block) in blocks.iter().enumerate() {
        remote
            .add_packed_block(
                SLICE,
                index as u32,
                block,
                vec![span(0, block.len() as u32, slots[index], 0)],
            )
            .unwrap();
    }
    remote.validate().unwrap();
    let remote_bytes = remote.build().unwrap();

    let snapshot = SealSnapshot::open(remote_bytes.clone()).unwrap();
    let frames_root = snapshot.table(TableId::Frames).unwrap().clone();
    let ChildRef::Local(frames_addr) = frames_root.clone() else {
        panic!("the remote seal builds local roots");
    };
    // Prove the fixture is what it claims to be: the remote Frames root is an
    // internal page, so its children are local pages of the remote container.
    let stored = &remote_bytes[frames_addr.offset as usize
        ..(frames_addr.offset + frames_addr.stored_len as u64) as usize];
    assert_eq!(
        <[u8; 32]>::from(Sha256::digest(stored)),
        frames_addr.stored_digest
    );
    let root_page = IndexPage::decode(stored).unwrap();
    assert!(
        root_page.level >= 1,
        "the remote Frames table must be multi-level, got level {}",
        root_page.level
    );
    // One page per level, plus the single Objects page.
    let frames_tree_levels = root_page.level as usize + 1;

    let remote_ref = object_ref(
        REMOTE_SEAL_ID,
        ObjectKind::DataSeal.as_u8(),
        &remote_bytes,
        b"nested/remote-seal",
    );
    let external = |root: ChildRef| match root {
        ChildRef::Local(addr) => ChildRef::External(RootRef {
            object: remote_ref.clone(),
            address: addr,
        }),
        ChildRef::External(_) => unreachable!("the remote seal builds local roots"),
    };
    let objects_root = snapshot.table(TableId::Objects).unwrap().clone();
    let mut local = SealBuilder::new(BLOCK_SIZE);
    local.set_table_override(TableId::Frames, Some(external(frames_root)));
    local.set_table_override(TableId::Objects, Some(external(objects_root)));
    for block_index in 0..3u32 {
        local
            .add_packed_block(
                REUSED_SLICE,
                block_index,
                &blocks[block_index as usize],
                vec![span(0, BLOCK_SIZE, slots[block_index as usize], 0)],
            )
            .unwrap();
    }
    let local_bytes = local.build().unwrap();

    NestedExternalFixture {
        remote_bytes,
        local_bytes,
        pack_bytes,
        blocks,
        frames_root_page: (frames_addr.offset, frames_addr.stored_len),
        frames_tree_levels,
    }
}

/// READ-005: a page that is not loaded yet is fetched through its locator ?
/// including the local children of an external page, which are addressed
/// inside the external container.  One fetch per distinct page per read; a
/// page that cannot be loaded at all is an error, never an inferred absence.
#[test]
fn nested_external_index_pages_are_located_and_loaded() {
    let fx = nested_external_fixture();
    let source = CountingSource::new(vec![
        (NESTED_PACK_ID, fx.pack_bytes.clone()),
        (REMOTE_SEAL_ID, fx.remote_bytes.clone()),
    ]);
    let opened = Opened::new(fx.local_bytes.clone(), &source);
    let reader = opened.reader();

    // The key is present in the remote table but its page is not loaded: the
    // read must locate and load it instead of reporting ENOENT or a hole.
    assert_eq!(
        reader.read_block(REUSED_SLICE, 0, BLOCK_SIZE).unwrap(),
        fx.blocks[0]
    );
    assert_eq!(
        source.gets_for(&REMOTE_SEAL_ID),
        fx.frames_tree_levels + 1,
        "one fetch per distinct remote page: the whole Frames path plus Objects"
    );

    // The same pages serve the whole range: they are never fetched twice.
    let oracle: Vec<u8> = fx.blocks[..3].concat();
    assert_eq!(
        reader
            .read_range(REUSED_SLICE, 0, 3 * BLOCK_SIZE as u64, BLOCK_SIZE)
            .unwrap(),
        oracle
    );
    assert_eq!(
        source.gets_for(&REMOTE_SEAL_ID),
        fx.frames_tree_levels + 1,
        "no page is fetched twice"
    );

    // A page that cannot be loaded is an error, never an inferred absence and
    // never zeros.
    let empty = CountingSource::new(Vec::new());
    let unloadable = Opened::new(fx.local_bytes.clone(), &empty);
    let error = unloadable
        .reader()
        .read_block(REUSED_SLICE, 0, BLOCK_SIZE)
        .unwrap_err();
    assert!(matches!(error, SealError::Source(_)), "{error:?}");
}

/// READ-006: a pinned view never mixes revisions.  Once the remote container
/// changes underneath the reader, the already-pinned reader keeps serving the
/// revision its pages were authenticated against, and a view opened after the
/// switch fails closed instead of combining old and new bytes.
#[test]
fn a_pinned_view_never_mixes_revisions_across_a_root_switch() {
    let fx = nested_external_fixture();
    let (offset, stored_len) = fx.frames_root_page;
    let mut switched = fx.remote_bytes.clone();
    // Flip a byte inside the remote Frames root page: the same offsets, a
    // different revision behind the very same locator.
    switched[offset as usize + stored_len as usize / 2] ^= 0xff;

    let source = CountingSource::new(vec![
        (NESTED_PACK_ID, fx.pack_bytes.clone()),
        (REMOTE_SEAL_ID, fx.remote_bytes.clone()),
    ]);
    *source.swap_after.borrow_mut() = Some((REMOTE_SEAL_ID, 3, switched));
    let opened = Opened::new(fx.local_bytes.clone(), &source);
    let reader = opened.reader();

    assert_eq!(
        reader.read_block(REUSED_SLICE, 0, BLOCK_SIZE).unwrap(),
        fx.blocks[0]
    );
    assert_eq!(source.gets_for(&REMOTE_SEAL_ID), fx.frames_tree_levels + 1);

    let oracle: Vec<u8> = fx.blocks[..3].concat();
    assert_eq!(
        reader
            .read_range(REUSED_SLICE, 0, 3 * BLOCK_SIZE as u64, BLOCK_SIZE)
            .unwrap(),
        oracle,
        "the pinned view keeps the revision it was opened with"
    );
    assert_eq!(
        source.gets_for(&REMOTE_SEAL_ID),
        fx.frames_tree_levels + 1,
        "the pinned view fetches no page from the switched container"
    );

    // A view opened after the switch fails closed: it never mixes the two
    // revisions and never silently reads a short or zero-filled range.
    let after = Opened::new(fx.local_bytes.clone(), &source);
    let error = after
        .reader()
        .read_block(REUSED_SLICE, 0, BLOCK_SIZE)
        .unwrap_err();
    assert!(
        matches!(error, SealError::Wire(WireError::HashMismatch { .. })),
        "{error:?}"
    );
}

// ---------------------------------------------------------------------------
// OPT-007 / INV-10: a lossless repack keeps the logical identity.
// ---------------------------------------------------------------------------

/// The canonical digest of one seal table.  A repack that leaves the Bindings
/// table byte-identical has not moved the logical revision, while new
/// Placements/Objects/Frames tables are exactly what makes it a repack.
fn table_digest(snapshot: &SealSnapshot, id: TableId) -> [u8; 32] {
    match snapshot.table(id) {
        Some(ChildRef::Local(address)) => address.stored_digest,
        other => panic!("{id:?} is not a local table: {other:?}"),
    }
}

/// OPT-007 / INV-10: a lossless repack moves one logical block from a pack
/// into a NEW pack with a different frame split.  The Bindings table -- the
/// seal's logical revision: every BlockKey, `decoded_len` and `content_hash`
/// -- is byte-identical, both views serve the same bytes, the old object is
/// neither read nor rewritten by the repack, and the variant is booked as new
/// space on top of the untouched source instead of replacing it.
///
/// The negative half is the point of the spec's "not only compare frame
/// checksums": a placement that keeps the declared binding but decodes to
/// different bytes passes every structural and frame-level check (the seal
/// validates and builds), and the read is still refused because the decoded
/// block does not hash to the binding's `content_hash`.
#[test]
fn lossless_repack_keeps_bindings_and_logical_revision() {
    let block: Vec<u8> = (0..BLOCK_SIZE).map(|i| (i % 251) as u8).collect();
    let (first, second) = block.split_at(BLOCK_SIZE as usize / 2);

    // Source layout: the whole block is one frame of one pack object.
    let (source_pack, source_scrubbed) = build_pack(vec![PackFrame::plain_bytes(&block).unwrap()]);
    let mut source_builder = SealBuilder::new(BLOCK_SIZE);
    let source_ordinal = source_builder.next_object_ordinal();
    source_builder.add_object(
        source_ordinal,
        object_ref(
            PACK_ID,
            ObjectKind::DataPack.as_u8(),
            &source_pack,
            b"repack/source",
        ),
    );
    let source_slots = register_frames(&mut source_builder, &source_scrubbed, source_ordinal);
    source_builder
        .add_packed_block(
            SLICE,
            0,
            &block,
            vec![span(0, block.len() as u32, source_slots[0], 0)],
        )
        .unwrap();
    source_builder.validate().unwrap();
    let source_seal = source_builder.build().unwrap();

    // Repacked layout: the same block, split over two frames of a NEW pack.
    let (repacked_pack, repacked_scrubbed) = build_pack(vec![
        PackFrame::plain_bytes(first).unwrap(),
        PackFrame::plain_bytes(second).unwrap(),
    ]);
    let mut repacked_builder = SealBuilder::new(BLOCK_SIZE);
    let repacked_ordinal = repacked_builder.next_object_ordinal();
    repacked_builder.add_object(
        repacked_ordinal,
        object_ref(
            REPACK_ID,
            ObjectKind::DataPack.as_u8(),
            &repacked_pack,
            b"repack/candidate",
        ),
    );
    let repacked_slots =
        register_frames(&mut repacked_builder, &repacked_scrubbed, repacked_ordinal);
    repacked_builder
        .add_packed_block(
            SLICE,
            0,
            &block,
            vec![
                span(0, first.len() as u32, repacked_slots[0], 0),
                span(
                    first.len() as u32,
                    second.len() as u32,
                    repacked_slots[1],
                    0,
                ),
            ],
        )
        .unwrap();
    repacked_builder.validate().unwrap();
    let repacked_seal = repacked_builder.build().unwrap();

    let source_view = SealSnapshot::open(source_seal.clone()).unwrap();
    let repacked_view = SealSnapshot::open(repacked_seal.clone()).unwrap();

    // Logical identity: the Bindings table -- and with it every BlockKey,
    // decoded_len and content_hash -- is byte-identical across the repack.
    assert_eq!(
        table_digest(&source_view, TableId::Bindings),
        table_digest(&repacked_view, TableId::Bindings),
        "a lossless repack must not move the bindings"
    );
    // Physical identity: everything that says *where* the bytes live moved,
    // which is what makes this a repack instead of a no-op.
    for physical in [TableId::Placements, TableId::Objects, TableId::Frames] {
        assert_ne!(
            table_digest(&source_view, physical),
            table_digest(&repacked_view, physical),
            "{physical:?} must describe the new layout"
        );
    }
    assert_ne!(source_seal, repacked_seal, "the seal object itself is new");

    // Both views serve the same bytes, each verified against the same
    // binding: the source from its own pack, the repack from the new one.
    let objects = CountingSource::new(vec![
        (PACK_ID, source_pack.clone()),
        (REPACK_ID, repacked_pack.clone()),
    ]);
    let source_reader = SealReader::new(&source_view, &objects);
    let repacked_reader = SealReader::new(&repacked_view, &objects);
    // The repacked view first: it fetches one range per frame of the NEW pack
    // and never touches the object the source view reads.
    assert_eq!(
        repacked_reader.read_block(SLICE, 0, BLOCK_SIZE).unwrap(),
        block
    );
    assert_eq!(
        objects.gets_for(&REPACK_ID),
        2,
        "one fetch per frame of the repacked object"
    );
    assert_eq!(
        objects.gets_for(&PACK_ID),
        0,
        "the repacked view never re-reads the old object"
    );
    // The source view still reads its own object and serves the same bytes,
    // so the repack left the published layout intact.
    assert_eq!(
        source_reader.read_block(SLICE, 0, BLOCK_SIZE).unwrap(),
        block
    );
    assert_eq!(
        objects.gets_for(&PACK_ID),
        1,
        "the source view still reads its own object"
    );
    assert_eq!(
        objects.gets(),
        3,
        "two new-pack frames plus one old-object read, nothing else"
    );

    // The variant validator agrees with the seal: same logical identity, and
    // the new space is booked on top of the untouched source rather than
    // replacing it (spec 10 §9: no in-place repack, no net-space claim).
    let logical = table_digest(&source_view, TableId::Bindings);
    let candidate = table_digest(&repacked_view, TableId::Bindings);
    let plan = validate_layout_variant(&LayoutVariantRequest {
        source: LayoutIdentity {
            logical_revision: logical,
            binding_digest: logical,
        },
        candidate: LayoutIdentity {
            logical_revision: candidate,
            binding_digest: candidate,
        },
        source_objects: BTreeSet::from([PACK_ID]),
        candidate_objects: BTreeSet::from([PACK_ID, REPACK_ID]),
        source_bytes: source_seal.len() as u64,
        candidate_new_bytes: repacked_seal.len() as u64,
        max_extra_bytes: repacked_seal.len() as u64,
        apply: true,
        expected_view_matches: true,
        head_has_visible_delta: false,
        open_orphan_count: 0,
        valid_writer_present: false,
    })
    .unwrap();
    assert!(plan.permanent_objects.contains(&PACK_ID));
    assert!(plan.added_objects.contains(&REPACK_ID));
    assert_eq!(plan.permanent_bytes, plan.source_bytes + plan.added_bytes);

    // The negative half: the declared binding is the real one, the placement
    // decodes to different bytes.  Every frame is intact and the seal builds,
    // so only a full decode can catch this -- and it does, on the content
    // hash rather than on a frame checksum.
    let mut tampered = block.clone();
    tampered[0] ^= 0x5A;
    let (tampered_first, tampered_second) = tampered.split_at(BLOCK_SIZE as usize / 2);
    let (tampered_pack, tampered_scrubbed) = build_pack(vec![
        PackFrame::plain_bytes(tampered_first).unwrap(),
        PackFrame::plain_bytes(tampered_second).unwrap(),
    ]);
    let mut tampered_builder = SealBuilder::new(BLOCK_SIZE);
    let tampered_ordinal = tampered_builder.next_object_ordinal();
    tampered_builder.add_object(
        tampered_ordinal,
        object_ref(
            REPACK_ID,
            ObjectKind::DataPack.as_u8(),
            &tampered_pack,
            b"repack/tampered",
        ),
    );
    let tampered_slots =
        register_frames(&mut tampered_builder, &tampered_scrubbed, tampered_ordinal);
    // The builder never re-reads the frames, so declaring the untampered
    // binding here is exactly the "same binding, other bytes" case.
    tampered_builder.add_binding(SLICE, 0, BlockBinding::of_decoded(&block));
    tampered_builder.add_placement(
        SLICE,
        0,
        BlockPlacement::Packed {
            spans: vec![
                span(0, tampered_first.len() as u32, tampered_slots[0], 0),
                span(
                    tampered_first.len() as u32,
                    tampered_second.len() as u32,
                    tampered_slots[1],
                    0,
                ),
            ],
        },
    );
    tampered_builder.validate().unwrap();
    let tampered_seal = tampered_builder.build().unwrap();
    let tampered_view = SealSnapshot::open(tampered_seal).unwrap();
    let tampered_objects = CountingSource::new(vec![(REPACK_ID, tampered_pack)]);
    let error = SealReader::new(&tampered_view, &tampered_objects)
        .read_block(SLICE, 0, BLOCK_SIZE)
        .unwrap_err();
    assert!(
        matches!(&error, SealError::Integrity(detail) if detail.contains("content hash mismatch")),
        "{error:?}"
    );
}
