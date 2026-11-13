//! Bounded LD05 external payload chunks with independently authenticated FD05.

use super::{
    SNAPSHOT_HEADER_LEN, SnapshotFrameDirectoryPage, SnapshotObjectKind, encode_snapshot_object,
};
use crate::workspace::packed::wire::{PackedResult, PackedWireError, Writer};
use crate::workspace::packed::{
    AccessProfile, PackedCodec, PackedFrameDescriptor, PackedFrameInput, SizeClassTable,
};
use sha2::{Digest, Sha256};

pub const SNAPSHOT_LARGE_CHUNK_RAW_LIMIT: usize = 16 * 1024 * 1024;
pub const SNAPSHOT_LARGE_CHUNK_BODY_LIMIT: usize = 24 * 1024 * 1024;
pub const SNAPSHOT_LARGE_CHUNK_FRAME_LIMIT: usize = 1024;

pub(super) struct BuiltLargeChunk {
    pub bytes: Vec<u8>,
    pub directory: SnapshotFrameDirectoryPage,
}

#[cfg(test)]
pub(super) fn build_large_chunk(
    inode: u64,
    chunk_id: u64,
    profile: AccessProfile,
    classes: SizeClassTable,
    codec: PackedCodec,
    frames: &[PackedFrameInput],
) -> PackedResult<BuiltLargeChunk> {
    build_large_chunk_with_policy(
        inode,
        chunk_id,
        profile,
        classes,
        codec,
        frames,
        super::SnapshotBuildPolicy::default(),
    )
}

pub(super) fn build_large_chunk_with_policy(
    inode: u64,
    chunk_id: u64,
    profile: AccessProfile,
    classes: SizeClassTable,
    codec: PackedCodec,
    frames: &[PackedFrameInput],
    policy: super::SnapshotBuildPolicy,
) -> PackedResult<BuiltLargeChunk> {
    let invalid =
        || PackedWireError::LimitExceeded("LD05 inode/count/raw/stored budget is invalid".into());
    let raw_bytes = frames
        .iter()
        .try_fold(0usize, |sum, frame| sum.checked_add(frame.raw.len()))
        .ok_or_else(invalid)?;
    if inode == 0
        || inode > i64::MAX as u64
        || frames.is_empty()
        || frames.len() > SNAPSHOT_LARGE_CHUNK_FRAME_LIMIT
        || raw_bytes > SNAPSHOT_LARGE_CHUNK_RAW_LIMIT
        || frames
            .iter()
            .any(|f| f.raw.is_empty() || f.raw.len() > 8 * 1024 * 1024 || f.codec != 0)
    {
        return Err(invalid());
    }
    let mut prefix = Writer::default();
    prefix.bytes(b"LD05");
    prefix.u64(inode);
    prefix.u64(chunk_id);
    prefix.u32(frames.len() as u32);
    prefix.u64(raw_bytes as u64);
    let mut body = prefix.finish();
    let mut descriptors = Vec::with_capacity(frames.len());
    for (ordinal, frame) in frames.iter().enumerate() {
        let encoded = super::super::codec::encode_block(codec, &frame.raw, 8 * 1024 * 1024)?;
        let (stored, frame_codec) =
            if codec == PackedCodec::Zstd && encoded.len() >= frame.raw.len() {
                (frame.raw.as_slice(), PackedCodec::Raw)
            } else {
                (encoded.as_slice(), codec)
            };
        if body
            .len()
            .checked_add(stored.len())
            .is_none_or(|length| length > SNAPSHOT_LARGE_CHUNK_BODY_LIMIT)
        {
            return Err(invalid());
        }
        descriptors.push(PackedFrameDescriptor {
            frame_ordinal: ordinal as u32,
            size_class: frame.size_class,
            codec: frame_codec as u8,
            object_offset: (SNAPSHOT_HEADER_LEN + body.len()) as u64,
            stored_len: stored.len() as u32,
            raw_len: frame.raw.len() as u32,
            first_file_slot: 0,
            last_file_slot: 0,
            frame_digest: Sha256::digest(stored)[..16].try_into().unwrap(),
        });
        body.extend_from_slice(stored);
    }
    let bytes = encode_snapshot_object(
        SnapshotObjectKind::LargeData,
        &body,
        SNAPSHOT_LARGE_CHUNK_BODY_LIMIT,
    )?;
    let directory = SnapshotFrameDirectoryPage {
        container_digest: Sha256::digest(&bytes).into(),
        container_len: bytes.len() as u64,
        profile,
        size_classes: classes,
        frame_policy: policy.frames,
        first_ordinal: 0,
        frames: descriptors,
    };
    directory.encode()?;
    Ok(BuiltLargeChunk { bytes, directory })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cadapter::{client::ObjectClient, localfs::LocalFsBackend};
    use crate::workspace::packed::SizeClass;
    #[test]
    fn ld05_incompressible_sequential_frame_keeps_strict_range_bound() {
        let mut state = 0x9e3779b97f4a7c15u64;
        let raw = (0..8 * 1024 * 1024)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect();
        let built = build_large_chunk(
            9,
            0,
            AccessProfile::SequentialSmallFile,
            SizeClassTable::default(),
            PackedCodec::Zstd,
            &[PackedFrameInput {
                raw,
                size_class: SizeClass::Large,
                codec: 0,
                first_file_slot: 0,
                last_file_slot: 0,
            }],
        )
        .unwrap();
        assert_eq!(built.directory.frames[0].codec, PackedCodec::Raw as u8);
        assert_eq!(built.directory.frames[0].stored_len, 8 * 1024 * 1024);
    }
    #[tokio::test]
    async fn ld05_raw_zstd_chunks_roundtrip_and_reject_replaced_payload() {
        let temp = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(temp.path()));
        let frames = [
            PackedFrameInput {
                raw: vec![37; 4096],
                size_class: SizeClass::Large,
                codec: 0,
                first_file_slot: 0,
                last_file_slot: 0,
            },
            PackedFrameInput {
                raw: vec![91; 8192],
                size_class: SizeClass::Large,
                codec: 0,
                first_file_slot: 0,
                last_file_slot: 0,
            },
        ];
        for codec in [PackedCodec::Raw, PackedCodec::Zstd] {
            let built = build_large_chunk(
                9,
                0,
                AccessProfile::RandomSmallFile,
                SizeClassTable::default(),
                codec,
                &frames,
            )
            .unwrap();
            let reference = super::super::SnapshotObjectRef::from_bytes(
                "chunk".into(),
                SnapshotObjectKind::LargeData,
                &built.bytes,
            )
            .unwrap();
            client
                .put_object(&reference.key, &built.bytes)
                .await
                .unwrap();
            for ordinal in 0..2 {
                assert_eq!(
                    built
                        .directory
                        .read_frame(&client, &reference, ordinal, 1024 * 1024)
                        .await
                        .unwrap(),
                    frames[ordinal as usize].raw
                );
            }
            let mut bad = built.bytes;
            bad[built.directory.frames[0].object_offset as usize] ^= 1;
            client.put_object(&reference.key, &bad).await.unwrap();
            assert!(
                built
                    .directory
                    .read_frame(&client, &reference, 0, 1024 * 1024)
                    .await
                    .is_err()
            );
        }
        assert!(
            build_large_chunk(
                0,
                0,
                AccessProfile::RandomSmallFile,
                SizeClassTable::default(),
                PackedCodec::Raw,
                &frames
            )
            .is_err()
        );
        assert!(
            build_large_chunk(
                9,
                0,
                AccessProfile::RandomSmallFile,
                SizeClassTable::default(),
                PackedCodec::Raw,
                &[]
            )
            .is_err()
        );
    }
}
