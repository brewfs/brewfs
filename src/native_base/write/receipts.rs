//! Durable receipts: the auditable record of every object a commit depends
//! on, stored as a type 3 (FrozenMetadata) single-value container.
//!
//! Spec 07 §2 walks a commit through `UPLOAD_INTENT_DURABLE →
//! NATIVE_OBJECTS_REMOTE_VERIFIED → RECEIPT_COMPLETE →
//! EXTENT_AND_PLACEMENT_COMMITTED`; the receipt set is the durable evidence
//! of the middle step. It is persisted as a kind 1 generic key/value
//! single-value page — fixed empty key, exactly one record (spec 05 §50) —
//! which is the only root a non-DataPack container may carry (spec 02 §7:
//! root non-empty, offset ≥ 64 and 8-aligned, stored range ending at most
//! `object_len − 64`).
//!
//! The container bytes must be uploaded (put) before the commit transaction
//! references it; `ObjectSink` is the upload seam the PR05/PR07 backends
//! plug into. Tests use an in-memory sink.

use async_trait::async_trait;
use sha2::{Digest, Sha256};

use crate::native_base::wire::container::{
    Codec, ContainerFooter, ContainerHeader, HEADER_LEN, ObjectKind,
};
use crate::native_base::wire::error::{WireError, WireResult};
use crate::native_base::wire::page::{BnpgKind, IndexPage, LeafEntry, PageBody};
use crate::native_base::wire::refs::{Hash32, ObjectId, RootRef};
use crate::native_base::wire::uvarint::{Reader, Writer};

/// Where uploaded control/metadata objects land before a commit references
/// them. The PR05 ingest path and the PR07 S3 adapter implement this; the
/// commit itself only records the [`RootRef`], it never uploads.
#[async_trait]
pub trait ObjectSink: Send + Sync {
    async fn put(
        &self,
        object: &crate::native_base::wire::refs::ObjectRef,
        bytes: &[u8],
    ) -> anyhow::Result<()>;

    /// Fetch the complete immutable object and verify its declared identity.
    async fn get(
        &self,
        object: &crate::native_base::wire::refs::ObjectRef,
    ) -> anyhow::Result<Vec<u8>>;
}

/// In-memory [`ObjectSink`] for tests and the reference pipeline.
#[derive(Default)]
pub struct MemorySink(pub std::sync::Mutex<std::collections::HashMap<ObjectId, Vec<u8>>>);

#[async_trait]
impl ObjectSink for MemorySink {
    async fn put(
        &self,
        object: &crate::native_base::wire::refs::ObjectRef,
        bytes: &[u8],
    ) -> anyhow::Result<()> {
        let mut objects = self.0.lock().unwrap();
        match objects.get(&object.object_id) {
            Some(existing) if existing != bytes => {
                anyhow::bail!("object identity already exists with different bytes")
            }
            Some(_) => {}
            None => {
                objects.insert(object.object_id, bytes.to_vec());
            }
        }
        Ok(())
    }

    async fn get(
        &self,
        object: &crate::native_base::wire::refs::ObjectRef,
    ) -> anyhow::Result<Vec<u8>> {
        let bytes = self
            .0
            .lock()
            .unwrap()
            .get(&object.object_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("object is missing"))?;
        if bytes.len() as u64 != object.object_len
            || <[u8; 32]>::from(Sha256::digest(&bytes)) != object.full_hash
        {
            anyhow::bail!("object bytes do not match the declared identity")
        }
        Ok(bytes)
    }
}

/// One receipt entry: the object identity and the registration facts the
/// commit verified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiptEntry {
    pub object_id: ObjectId,
    pub domain_id: [u8; 16],
    pub attempt_generation: u64,
    pub registration_seq: u64,
    pub full_hash: Hash32,
}

/// The receipt set carried by one commit.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReceiptSet {
    pub entries: Vec<ReceiptEntry>,
}

impl ReceiptSet {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.uvarint(self.entries.len() as u64);
        for e in &self.entries {
            w.put(&e.object_id);
            w.put(&e.domain_id);
            w.u64(e.attempt_generation);
            w.u64(e.registration_seq);
            w.put(&e.full_hash);
        }
        w.into_bytes()
    }

    pub fn decode(bytes: &[u8]) -> WireResult<ReceiptSet> {
        let mut r = Reader::new(bytes);
        let count = r.uvarint("receipt set")? as usize;
        let mut entries = Vec::with_capacity(count.min(1024));
        for _ in 0..count {
            let object_id: ObjectId = r.take(16, "receipt entry")?.try_into().unwrap();
            let domain_id: [u8; 16] = r.take(16, "receipt entry")?.try_into().unwrap();
            let attempt_generation = r.u64("receipt entry")?;
            let registration_seq = r.u64("receipt entry")?;
            let full_hash: Hash32 = r.take(32, "receipt entry")?.try_into().unwrap();
            entries.push(ReceiptEntry {
                object_id,
                domain_id,
                attempt_generation,
                registration_seq,
                full_hash,
            });
        }
        if !r.is_empty() {
            return Err(WireError::invalid("receipt set", "trailing bytes"));
        }
        Ok(ReceiptSet { entries })
    }
}

/// A built receipts container: the object bytes plus the [`RootRef`] the
/// mutation result carries.
#[derive(Debug, Clone)]
pub struct ReceiptsObject {
    pub root: RootRef,
    pub bytes: Vec<u8>,
}

/// Build the type 3 single-value container holding `set`.
///
/// Layout: 64-byte header, the BNPG kind 1 leaf page at offset 64 (8-aligned
/// by construction), then the 64-byte footer. `object_id` must be unique per
/// operation — the caller passes the operation's identity-derived id.
pub fn build_receipts_container(
    volume_id: &[u8; 16],
    object_id: &ObjectId,
    set: &ReceiptSet,
) -> ReceiptsObject {
    let page = IndexPage {
        kind: BnpgKind::GenericKeyValue,
        level: 0,
        body: PageBody::Leaf(vec![LeafEntry {
            key: Vec::new(),
            value: set.encode(),
        }]),
    };
    let raw = page.encode();
    let stored = raw.clone();
    let root_offset = HEADER_LEN as u64; // 64: the minimum, and 8-aligned.
    let object_len = HEADER_LEN as u64 + stored.len() as u64 + HEADER_LEN as u64;

    let header = ContainerHeader {
        kind: ObjectKind::FrozenMetadata,
        required_features: 0,
        object_len,
        root_offset,
        root_stored_len: stored.len() as u32,
        root_raw_len: raw.len() as u32,
        hash_id: 1,
        root_codec: Codec::None,
    };
    let footer = ContainerFooter {
        object_len,
        root_stored_digest: Sha256::digest(&stored).into(),
    };

    let mut bytes = Vec::with_capacity(object_len as usize);
    bytes.extend_from_slice(&header.encode());
    bytes.extend_from_slice(&stored);
    bytes.extend_from_slice(&footer.encode());
    debug_assert_eq!(bytes.len() as u64, object_len);

    let full_hash: Hash32 = Sha256::digest(&bytes).into();
    let key = object_key(volume_id, "frozen", object_id, &full_hash);
    let root = RootRef {
        object: crate::native_base::wire::refs::ObjectRef {
            object_id: *object_id,
            kind: ObjectKind::FrozenMetadata.as_u8(),
            object_len,
            full_hash,
            key,
        },
        address: crate::native_base::wire::refs::PageAddress {
            offset: root_offset,
            stored_len: stored.len() as u32,
            raw_len: raw.len() as u32,
            codec: Codec::None,
            page_kind: crate::native_base::wire::refs::PageKind::GenericKeyValue,
            level: 0,
            entry_count: 1,
            stored_digest: Sha256::digest(&stored).into(),
        },
    };
    ReceiptsObject { root, bytes }
}

/// Object key layout (spec 01 §6): `native-base/v3/{volume}/{kind}/{object
/// -id}/{full-hash}.{suffix}`.
pub fn object_key(
    volume_id: &[u8; 16],
    kind_dir: &str,
    object_id: &ObjectId,
    full_hash: &Hash32,
) -> Vec<u8> {
    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
    format!(
        "native-base/v3/{}/{}/{}/{}.{}",
        hex(volume_id),
        kind_dir,
        hex(object_id),
        hex(full_hash),
        object_kind_suffix(kind_dir)
    )
    .into_bytes()
}

fn object_kind_suffix(kind_dir: &str) -> &'static str {
    match kind_dir {
        "data" => "brfds",
        "pack" => "brfdp",
        "frozen" => "brfc",
        "manifest" => "brfsm",
        "inventory" => "brfin",
        // Kind 6 has no container magic; the bytes are the SF-framed
        // persisted block, so the existing native suffix is `.sf`
        // (spec 02 §2: loose objects keep the existing native suffix).
        "loose" => "sf",
        other => unreachable!("unknown kind dir {other}"),
    }
}

/// The stable error recorded for a failed mutation, carried in
/// `NativeMutationResult.stable_error`.
pub fn stable_error_text(err: &crate::native_base::write::error::WriteError) -> Vec<u8> {
    err.to_string().into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receipts_container_is_a_valid_single_value_frozen_metadata_object() {
        let set = ReceiptSet {
            entries: vec![ReceiptEntry {
                object_id: [1u8; 16],
                domain_id: [2u8; 16],
                attempt_generation: 1,
                registration_seq: 7,
                full_hash: [3u8; 32],
            }],
        };
        let built = build_receipts_container(&[9u8; 16], &[0xa5; 16], &set);

        // Container-level structure.
        let header = ContainerHeader::parse(&built.bytes).unwrap();
        assert_eq!(header.kind, ObjectKind::FrozenMetadata);
        assert_eq!(header.root_offset, 64);
        assert_eq!(header.object_len as usize, built.bytes.len());
        // Spec 02 §7 root bounds for non-DataPack containers.
        assert!(header.root_stored_len > 0);
        assert_eq!(header.root_offset % 8, 0);
        assert!(header.root_offset + header.root_stored_len as u64 <= header.object_len - 64);
        let footer = ContainerFooter::parse(&built.bytes[built.bytes.len() - 64..]).unwrap();
        let stored_digest: Hash32 =
            Sha256::digest(&built.bytes[64..64 + header.root_stored_len as usize]).into();
        assert_eq!(footer.root_stored_digest, stored_digest);

        // RootRef points at a single-record empty-key kind 1 leaf.
        let page_bytes = &built.bytes[built.root.address.offset as usize
            ..(built.root.address.offset as usize + built.root.address.stored_len as usize)];
        let page = IndexPage::decode(page_bytes).unwrap();
        match page.body {
            PageBody::Leaf(entries) => {
                assert_eq!(entries.len(), 1);
                assert!(entries[0].key.is_empty());
                assert_eq!(ReceiptSet::decode(&entries[0].value).unwrap(), set);
            }
            _ => panic!("root must be a leaf"),
        }
        // Full hash covers the whole object.
        let full_hash: Hash32 = Sha256::digest(&built.bytes).into();
        assert_eq!(built.root.object.full_hash, full_hash);
        assert_eq!(built.root.object.kind, 3);
        // Spec 01 §6 object key layout, with the volume and object id in
        // full hex and the kind 3 `.brfc` suffix.
        let hex = |bytes: &[u8]| -> String { bytes.iter().map(|b| format!("{b:02x}")).collect() };
        let expected_prefix = format!(
            "native-base/v3/{}/frozen/{}/",
            hex(&[9u8; 16]),
            hex(&[0xa5; 16])
        );
        assert!(
            built
                .root
                .object
                .key
                .starts_with(expected_prefix.as_bytes())
        );
        assert!(built.root.object.key.ends_with(b".brfc"));
    }

    #[test]
    fn receipt_set_roundtrip_and_rejects_trailing() {
        let set = ReceiptSet {
            entries: vec![
                ReceiptEntry {
                    object_id: [1u8; 16],
                    domain_id: [2u8; 16],
                    attempt_generation: 1,
                    registration_seq: 1,
                    full_hash: [4u8; 32],
                },
                ReceiptEntry {
                    object_id: [5u8; 16],
                    domain_id: [2u8; 16],
                    attempt_generation: 3,
                    registration_seq: 2,
                    full_hash: [6u8; 32],
                },
            ],
        };
        assert_eq!(ReceiptSet::decode(&set.encode()).unwrap(), set);
        let mut trailing = set.encode();
        trailing.push(0);
        assert!(ReceiptSet::decode(&trailing).is_err());
    }
}
