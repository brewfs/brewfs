//! v2 DataPack (`BRFDP002`) adapter.
//!
//! The frame and container primitives already used by the native packed base
//! are deliberately reused here.  The v2 namespace is a distinct publication
//! format, so the adapter changes only the object magic and re-seals the fixed
//! container-header CRC; frame layout, independent codecs, padding, and frame
//! authentication remain exactly the shared wire contract.

use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::native_base::wire::container::crc32c;
use crate::native_base::wire::container::{ContainerHeader, ObjectKind, features};
use crate::native_base::wire::datapack::{PackBuilder, PackFrame, ScrubbedFrame, ScrubbedPack};
use crate::native_base::wire::error::{WireError, WireResult};
use crate::native_base::wire::frame::{FRAME_HEADER_LEN, FrameHeader};
use sha2::{Digest, Sha256};

use super::data_seal::FrameDescriptor;

pub const DATA_PACK_MAGIC: &[u8; 8] = b"BRFDP002";
const LEGACY_DATA_PACK_MAGIC: &[u8; 8] = b"BRFDP003";
const HEADER_LEN: usize = 64;

/// A deterministic v2 DataPack producer. Frames are appended in input order;
/// use the deterministic upload plan to decide object and frame ordinals.
#[derive(Debug, Default)]
pub struct DataPackBuilder {
    inner: PackBuilder,
}

impl DataPackBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, frame: PackFrame) {
        self.inner.push(frame);
    }

    pub fn frame_count(&self) -> usize {
        self.inner.frame_count()
    }

    /// Build an immutable `BRFDP002` object.
    pub fn build(&self) -> WireResult<Vec<u8>> {
        let mut bytes = self.inner.build()?;
        rewrite_magic(&mut bytes, DATA_PACK_MAGIC)?;
        Ok(bytes)
    }
}

/// Authenticated view of a complete v2 DataPack.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataPackSnapshot {
    bytes: Vec<u8>,
    frames: Vec<ScrubbedFrame>,
    full_hash: [u8; 32],
}

/// Header-only remote view of a `BRFDP002` object. The Data Seal supplies
/// frame offsets, so reading one logical range needs one bounded object-store
/// range request and never scans or downloads preceding frames.
pub struct RemoteDataPack<B: ObjectBackend + Clone> {
    client: ObjectClient<B>,
    object_key: String,
    object_len: u64,
    header: ContainerHeader,
}

impl<B: ObjectBackend + Clone> RemoteDataPack<B> {
    pub async fn open(
        client: &ObjectClient<B>,
        object_key: &str,
        object_len: u64,
    ) -> WireResult<Self> {
        if object_key.is_empty() || object_key.len() > 1024 || object_key.contains('\0') {
            return Err(WireError::invalid(
                "remote data pack",
                "object key must be non-empty, bounded, and contain no NUL",
            ));
        }
        if object_len < 128 {
            return Err(WireError::Truncated {
                what: "remote data pack",
                need: 128,
                have: object_len as usize,
            });
        }
        let mut bytes = vec![0u8; 64];
        let actual = client
            .get_object_range(object_key, 0, &mut bytes)
            .await
            .map_err(|error| WireError::invalid("remote data pack header", error.to_string()))?;
        if actual != bytes.len() {
            return Err(WireError::Truncated {
                what: "remote data pack header",
                need: bytes.len(),
                have: actual,
            });
        }
        let header = parse_v2_header(&bytes, object_len)?;
        if header.kind != ObjectKind::DataPack {
            return Err(WireError::invalid(
                "remote data pack",
                "container kind is not DataPack",
            ));
        }
        if header.required_features & !features::KNOWN != 0 {
            return Err(WireError::UnsupportedFormat(
                "remote data pack requires unsupported features".into(),
            ));
        }
        Ok(Self {
            client: client.clone(),
            object_key: object_key.to_owned(),
            object_len,
            header,
        })
    }

    pub fn object_len(&self) -> u64 {
        self.object_len
    }

    pub fn header(&self) -> &ContainerHeader {
        &self.header
    }

    /// Read and authenticate one Data Seal frame descriptor.
    pub async fn read_frame(&self, descriptor: &FrameDescriptor) -> WireResult<ScrubbedFrame> {
        let frame_span = (FRAME_HEADER_LEN as u64)
            .checked_add(u64::from(descriptor.stored_len))
            .ok_or_else(|| WireError::LimitExceeded("data pack frame span overflows".into()))?
            .div_ceil(8)
            .checked_mul(8)
            .ok_or_else(|| WireError::LimitExceeded("data pack frame span overflows".into()))?;
        let end = descriptor
            .object_offset
            .checked_add(frame_span)
            .ok_or_else(|| WireError::LimitExceeded("data pack frame end overflows".into()))?;
        let footer_offset =
            self.object_len
                .checked_sub(64)
                .ok_or_else(|| WireError::Truncated {
                    what: "remote data pack footer",
                    need: 64,
                    have: self.object_len as usize,
                })?;
        if descriptor.object_offset < 64 || end > footer_offset {
            return Err(WireError::Truncated {
                what: "remote data pack frame",
                need: frame_span as usize,
                have: self.object_len.saturating_sub(descriptor.object_offset) as usize,
            });
        }
        let range_len = u32::try_from(frame_span)
            .map_err(|_| WireError::LimitExceeded("data pack frame range exceeds u32".into()))?;
        let mut bytes = vec![0u8; range_len as usize];
        let actual = self
            .client
            .get_object_range(&self.object_key, descriptor.object_offset, &mut bytes)
            .await
            .map_err(|error| WireError::invalid("remote data pack frame", error.to_string()))?;
        if actual != bytes.len() {
            return Err(WireError::Truncated {
                what: "remote data pack frame",
                need: bytes.len(),
                have: actual,
            });
        }
        let header = FrameHeader::parse(&bytes)?;
        if header.ordinal != u64::from(descriptor.frame_ordinal)
            || header.stored_len != descriptor.stored_len
            || header.raw_len != descriptor.raw_len
            || header.payload_format.as_u8() != descriptor.payload_format
            || header.codec.as_u8() != descriptor.codec
        {
            return Err(WireError::invalid(
                "remote data pack frame",
                "frame header disagrees with Data Seal descriptor",
            ));
        }
        let stored_end = FRAME_HEADER_LEN
            .checked_add(header.stored_len as usize)
            .ok_or_else(|| WireError::LimitExceeded("data pack payload offset overflows".into()))?;
        let span = usize::try_from(header.frame_span())
            .map_err(|_| WireError::LimitExceeded("data pack frame span exceeds usize".into()))?;
        if span != bytes.len() || bytes[stored_end..].iter().any(|byte| *byte != 0) {
            return Err(WireError::invalid(
                "remote data pack frame",
                "frame padding or range length is non-canonical",
            ));
        }
        let stored = bytes[FRAME_HEADER_LEN..stored_end].to_vec();
        let checksum = header.stored_digest(&stored);
        if checksum[..16] != descriptor.frame_checksum {
            return Err(WireError::HashMismatch {
                what: "remote data pack frame",
                stored: hex::encode(descriptor.frame_checksum),
                computed: hex::encode(&checksum[..16]),
            });
        }
        let raw = header.decode_payload(&stored)?;
        Ok(ScrubbedFrame {
            header,
            object_offset: descriptor.object_offset,
            stored,
            raw,
        })
    }
}

fn parse_v2_header(bytes: &[u8], object_len: u64) -> WireResult<ContainerHeader> {
    if bytes.len() < 64 || &bytes[..8] != DATA_PACK_MAGIC {
        return Err(WireError::UnsupportedFormat(
            "not a BRFDP002 data pack".into(),
        ));
    }
    let mut legacy = bytes[..64].to_vec();
    rewrite_magic(&mut legacy, LEGACY_DATA_PACK_MAGIC)?;
    let header = ContainerHeader::parse(&legacy)?;
    if header.object_len != object_len
        || header.root_offset != 0
        || header.root_stored_len != 0
        || header.root_raw_len != 0
    {
        return Err(WireError::invalid(
            "remote data pack header",
            "object length or root fields are invalid",
        ));
    }
    Ok(header)
}

impl DataPackSnapshot {
    /// Scrub all frames and retain the immutable bytes for later seal
    /// descriptor generation. No caller-provided offset is trusted.
    pub fn open(bytes: Vec<u8>) -> WireResult<Self> {
        if bytes.len() < HEADER_LEN || &bytes[..8] != DATA_PACK_MAGIC {
            return Err(WireError::UnsupportedFormat(
                "not a BRFDP002 data pack".into(),
            ));
        }
        let mut legacy = bytes.clone();
        rewrite_magic(&mut legacy, LEGACY_DATA_PACK_MAGIC)?;
        let scrubbed = ScrubbedPack::scrub(&legacy)?;
        let full_hash: [u8; 32] = Sha256::digest(&bytes).into();
        Ok(Self {
            bytes,
            frames: scrubbed.frames,
            full_hash,
        })
    }

    pub fn object_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn frames(&self) -> &[ScrubbedFrame] {
        &self.frames
    }

    pub fn frame(&self, ordinal: u64) -> Option<&ScrubbedFrame> {
        self.frames
            .get(usize::try_from(ordinal).ok()?)
            .filter(|frame| frame.header.ordinal == ordinal)
    }

    pub fn full_hash(&self) -> [u8; 32] {
        self.full_hash
    }
}

fn rewrite_magic(bytes: &mut [u8], magic: &[u8; 8]) -> WireResult<()> {
    if bytes.len() < HEADER_LEN {
        return Err(WireError::Truncated {
            what: "data pack header",
            need: HEADER_LEN,
            have: bytes.len(),
        });
    }
    bytes[..8].copy_from_slice(magic);
    let crc = crc32c(&bytes[..60]);
    bytes[60..64].copy_from_slice(&crc.to_le_bytes());
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;

    use super::*;
    use crate::cadapter::client::ObjectBackend;
    use crate::native_base::wire::datapack::PackFrame;

    #[derive(Clone, Default)]
    struct CountingBackend {
        state: Arc<Mutex<State>>,
    }

    #[derive(Default)]
    struct State {
        objects: HashMap<String, Vec<u8>>,
        ranges: Vec<(u64, usize)>,
    }

    impl CountingBackend {
        fn insert(&self, key: &str, bytes: Vec<u8>) {
            self.state
                .lock()
                .expect("backend mutex")
                .objects
                .insert(key.to_owned(), bytes);
        }

        fn ranges(&self) -> Vec<(u64, usize)> {
            self.state.lock().expect("backend mutex").ranges.clone()
        }
    }

    #[async_trait]
    impl ObjectBackend for CountingBackend {
        async fn put_object(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
            self.insert(key, data.to_vec());
            Ok(())
        }

        async fn get_object(&self, _key: &str) -> anyhow::Result<Option<Vec<u8>>> {
            Ok(None)
        }

        async fn get_object_range(
            &self,
            key: &str,
            offset: u64,
            buf: &mut [u8],
        ) -> anyhow::Result<usize> {
            let mut state = self.state.lock().expect("backend mutex");
            let Some(object) = state.objects.get(key) else {
                return Ok(0);
            };
            let start = usize::try_from(offset)?;
            if start >= object.len() {
                return Ok(0);
            }
            let count = buf.len().min(object.len() - start);
            buf[..count].copy_from_slice(&object[start..start + count]);
            state.ranges.push((offset, count));
            Ok(count)
        }

        async fn get_etag(&self, _key: &str) -> anyhow::Result<String> {
            Ok(String::new())
        }

        async fn delete_object(&self, _key: &str) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn v2_datapack_is_deterministic_and_independently_scrubbable() {
        let mut builder = DataPackBuilder::new();
        builder.push(PackFrame::plain_bytes(b"alpha").unwrap());
        builder.push(PackFrame::plain_bytes_zstd(&vec![b'x'; 4096], 3).unwrap());
        let first = builder.build().unwrap();
        let second = builder.build().unwrap();
        assert_eq!(first, second);
        assert_eq!(&first[..8], DATA_PACK_MAGIC);

        let snapshot = DataPackSnapshot::open(first.clone()).unwrap();
        assert_eq!(snapshot.frames().len(), 2);
        assert_eq!(snapshot.frames()[0].raw, b"alpha");
        assert_eq!(snapshot.object_bytes(), first.as_slice());
        let expected_hash: [u8; 32] = Sha256::digest(&first).into();
        assert_eq!(snapshot.full_hash(), expected_hash);
    }

    #[test]
    fn v2_datapack_rejects_tampering_and_legacy_magic() {
        let mut builder = DataPackBuilder::new();
        builder.push(PackFrame::plain_bytes(b"payload").unwrap());
        let mut bytes = builder.build().unwrap();
        bytes[64 + 80] ^= 1;
        assert!(matches!(
            DataPackSnapshot::open(bytes),
            Err(WireError::HashMismatch { .. })
        ));

        let legacy = PackBuilder::new().build().unwrap();
        assert!(matches!(
            DataPackSnapshot::open(legacy),
            Err(WireError::UnsupportedFormat(_))
        ));
    }

    #[tokio::test]
    async fn remote_datapack_reads_one_sealed_frame_by_range() {
        let mut builder = DataPackBuilder::new();
        builder.push(PackFrame::plain_bytes(b"first").unwrap());
        builder.push(PackFrame::plain_bytes_zstd(&vec![b'z'; 4096], 3).unwrap());
        let bytes = builder.build().unwrap();
        let snapshot = DataPackSnapshot::open(bytes.clone()).unwrap();
        let descriptors =
            super::super::data_seal::frame_descriptors_from_pack(7, &snapshot).unwrap();

        let backend = CountingBackend::default();
        backend.insert("packs/7.brfdp", bytes);
        let client = ObjectClient::new(backend.clone());
        let remote = RemoteDataPack::open(
            &client,
            "packs/7.brfdp",
            snapshot.object_bytes().len() as u64,
        )
        .await
        .unwrap();
        assert_eq!(backend.ranges().len(), 1);
        let frame = remote.read_frame(&descriptors[1]).await.unwrap();
        assert_eq!(frame.header.ordinal, 1);
        assert_eq!(frame.raw, vec![b'z'; 4096]);
        assert_eq!(backend.ranges().len(), 2);
        let (offset, len) = backend.ranges()[1];
        assert_eq!(offset, descriptors[1].object_offset);
        assert_eq!(len, snapshot.frames()[1].header.frame_span() as usize);
    }
}
