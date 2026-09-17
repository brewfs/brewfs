//! P1 snapshot manifest (`.brfsm`, object kind 4; spec 05 §5).

use sha2::{Digest, Sha256};

use crate::native_base::wire::container::{
    Codec, ContainerFooter, ContainerHeader, FOOTER_LEN, HEADER_LEN, ObjectKind, parse_footer,
};
use crate::native_base::wire::refs::{Hash32, ObjectRef, PageAddress, PageKind, RootRef};
use crate::native_base::wire::uvarint::{Reader, Writer};

use super::{LifecycleError, LifecycleResult};

pub const MANIFEST_MAGIC: &[u8; 4] = b"BNSM";
pub const MANIFEST_VERSION: u16 = 1;
pub const MAX_MANIFEST_LEN: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvNamespace {
    pub layer_id: [u8; 16],
    pub sealed_version: u64,
}

/// P1 manifest.  P2's Frozen namespace mode intentionally remains outside
/// PR06A; decoding any namespace mode other than 1 fails closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotManifest {
    pub volume_id: [u8; 16],
    pub storage_namespace_id: [u8; 16],
    pub chunk_size: u64,
    pub block_size: u32,
    pub required_features: u64,
    pub logical_revision: Hash32,
    pub namespace_digest: Hash32,
    pub binding_digest: Hash32,
    pub namespace: KvNamespace,
    pub data_seal: RootRef,
    pub physical_inventory: RootRef,
    pub file_count: u64,
    pub directory_count: u64,
    pub total_logical_bytes: u64,
    pub created_at_ns: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuiltManifest {
    pub manifest: SnapshotManifest,
    pub bytes: Vec<u8>,
    pub object: ObjectRef,
    pub root_address: PageAddress,
}

impl SnapshotManifest {
    pub fn encode_payload(&self) -> LifecycleResult<Vec<u8>> {
        if self.chunk_size == 0 || self.block_size == 0 {
            return Err(LifecycleError::Record(
                "manifest chunk_size and block_size must be non-zero".into(),
            ));
        }
        if self.data_seal.object.kind != ObjectKind::DataSeal.as_u8() {
            return Err(LifecycleError::Record(
                "manifest data_seal is not an object-kind 2 container".into(),
            ));
        }
        if self.physical_inventory.object.kind != ObjectKind::PagedInventory.as_u8() {
            return Err(LifecycleError::Record(
                "manifest physical_inventory is not an object-kind 5 container".into(),
            ));
        }
        let mut w = Writer::new();
        w.put(MANIFEST_MAGIC);
        w.u16(MANIFEST_VERSION);
        w.u16(0);
        w.put(&self.volume_id);
        w.put(&self.storage_namespace_id);
        w.u64(self.chunk_size);
        w.u32(self.block_size);
        w.u64(self.required_features);
        w.put(&self.logical_revision);
        w.put(&self.namespace_digest);
        w.put(&self.binding_digest);
        w.u8(1); // P1 KV namespace mode.
        w.put(&self.namespace.layer_id);
        w.u64(self.namespace.sealed_version);
        self.data_seal.encode_into(&mut w);
        self.physical_inventory.encode_into(&mut w);
        w.u64(self.file_count);
        w.u64(self.directory_count);
        w.u64(self.total_logical_bytes);
        w.i64(self.created_at_ns);
        Ok(w.into_bytes())
    }

    pub fn decode_payload(payload: &[u8]) -> LifecycleResult<Self> {
        let what = "snapshot manifest";
        let mut r = Reader::new(payload);
        if r.take(4, what)? != MANIFEST_MAGIC {
            return Err(LifecycleError::Record("manifest magic is not BNSM".into()));
        }
        let version = r.u16(what)?;
        if version != MANIFEST_VERSION {
            return Err(LifecycleError::Record(format!(
                "unsupported manifest version {version}"
            )));
        }
        if r.u16(what)? != 0 {
            return Err(LifecycleError::Record(
                "manifest reserved field is non-zero".into(),
            ));
        }
        let volume_id = r.take(16, what)?.try_into().unwrap();
        let storage_namespace_id = r.take(16, what)?.try_into().unwrap();
        let chunk_size = r.u64(what)?;
        let block_size = r.u32(what)?;
        let required_features = r.u64(what)?;
        let logical_revision = r.take(32, what)?.try_into().unwrap();
        let namespace_digest = r.take(32, what)?.try_into().unwrap();
        let binding_digest = r.take(32, what)?.try_into().unwrap();
        let namespace = match r.u8(what)? {
            1 => KvNamespace {
                layer_id: r.take(16, what)?.try_into().unwrap(),
                sealed_version: r.u64(what)?,
            },
            mode => {
                return Err(LifecycleError::Record(format!(
                    "namespace mode {mode} is not P1 KV mode 1"
                )));
            }
        };
        let data_seal = RootRef::decode(&mut r)?;
        let physical_inventory = RootRef::decode(&mut r)?;
        let file_count = r.u64(what)?;
        let directory_count = r.u64(what)?;
        let total_logical_bytes = r.u64(what)?;
        let created_at_ns = r.i64(what)?;
        if !r.is_empty() {
            return Err(LifecycleError::Record(
                "manifest payload has trailing bytes".into(),
            ));
        }
        let manifest = SnapshotManifest {
            volume_id,
            storage_namespace_id,
            chunk_size,
            block_size,
            required_features,
            logical_revision,
            namespace_digest,
            binding_digest,
            namespace,
            data_seal,
            physical_inventory,
            file_count,
            directory_count,
            total_logical_bytes,
            created_at_ns,
        };
        // Reuse the writer-side invariant checks without accepting a second
        // representation of the same payload.
        let _ = manifest.encode_payload()?;
        Ok(manifest)
    }

    pub fn build(&self, object_id: [u8; 16], key: Vec<u8>) -> LifecycleResult<BuiltManifest> {
        let payload = self.encode_payload()?;
        let object_len = HEADER_LEN + payload.len() + FOOTER_LEN;
        if object_len > MAX_MANIFEST_LEN {
            return Err(LifecycleError::LimitExceeded(format!(
                "manifest object is {object_len} bytes (maximum {MAX_MANIFEST_LEN})"
            )));
        }
        let digest: Hash32 = Sha256::digest(&payload).into();
        let address = PageAddress {
            offset: HEADER_LEN as u64,
            stored_len: payload.len() as u32,
            raw_len: payload.len() as u32,
            codec: Codec::None,
            page_kind: PageKind::ManifestPayload,
            level: 0,
            entry_count: 1,
            stored_digest: digest,
        };
        let header = ContainerHeader {
            kind: ObjectKind::SnapshotManifest,
            required_features: self.required_features,
            object_len: object_len as u64,
            root_offset: address.offset,
            root_stored_len: address.stored_len,
            root_raw_len: address.raw_len,
            hash_id: 1,
            root_codec: Codec::None,
        };
        let footer = ContainerFooter {
            object_len: object_len as u64,
            root_stored_digest: digest,
        };
        let mut bytes = Vec::with_capacity(object_len);
        bytes.extend_from_slice(&header.encode());
        bytes.extend_from_slice(&payload);
        bytes.extend_from_slice(&footer.encode());
        let object = ObjectRef {
            object_id,
            kind: ObjectKind::SnapshotManifest.as_u8(),
            object_len: object_len as u64,
            full_hash: Sha256::digest(&bytes).into(),
            key,
        };
        crate::native_base::wire::refs::validate_object_key(&object.key)?;
        Ok(BuiltManifest {
            manifest: self.clone(),
            bytes,
            object,
            root_address: address,
        })
    }
}

/// Fully verify and decode a manifest object.  The complete-object hash is
/// checked before any embedded RootRef is trusted.
pub fn open_manifest(object: &ObjectRef, bytes: &[u8]) -> LifecycleResult<BuiltManifest> {
    if object.kind != ObjectKind::SnapshotManifest.as_u8() {
        return Err(LifecycleError::Record(
            "manifest ObjectRef has wrong kind".into(),
        ));
    }
    if bytes.len() > MAX_MANIFEST_LEN {
        return Err(LifecycleError::LimitExceeded(format!(
            "manifest object is {} bytes (maximum {MAX_MANIFEST_LEN})",
            bytes.len()
        )));
    }
    if object.object_len != bytes.len() as u64
        || object.full_hash != <[u8; 32]>::from(Sha256::digest(bytes))
    {
        return Err(LifecycleError::Record(
            "manifest ObjectRef length/hash mismatch".into(),
        ));
    }
    let header = ContainerHeader::parse(bytes)?;
    header.ensure_supported_features()?;
    if header.kind != ObjectKind::SnapshotManifest
        || header.object_len != bytes.len() as u64
        || header.root_codec != Codec::None
        || header.root_raw_len != header.root_stored_len
        || header.root_stored_len == 0
        || header.root_offset % 8 != 0
    {
        return Err(LifecycleError::Record(
            "invalid manifest container header".into(),
        ));
    }
    let footer = parse_footer(bytes)?;
    let start = usize::try_from(header.root_offset)
        .map_err(|_| LifecycleError::Record("manifest root offset overflow".into()))?;
    let end = start
        .checked_add(header.root_stored_len as usize)
        .ok_or_else(|| LifecycleError::Record("manifest root range overflow".into()))?;
    if start != HEADER_LEN || end != bytes.len() - FOOTER_LEN {
        return Err(LifecycleError::Record(
            "manifest root is not the sole container payload".into(),
        ));
    }
    let payload = &bytes[start..end];
    let digest: Hash32 = Sha256::digest(payload).into();
    if footer.root_stored_digest != digest {
        return Err(LifecycleError::Record(
            "manifest root digest mismatch".into(),
        ));
    }
    let manifest = SnapshotManifest::decode_payload(payload)?;
    if manifest.required_features != header.required_features {
        return Err(LifecycleError::Record(
            "manifest feature aggregate disagrees with container header".into(),
        ));
    }
    Ok(BuiltManifest {
        manifest,
        bytes: bytes.to_vec(),
        object: object.clone(),
        root_address: PageAddress {
            offset: header.root_offset,
            stored_len: header.root_stored_len,
            raw_len: header.root_raw_len,
            codec: header.root_codec,
            page_kind: PageKind::ManifestPayload,
            level: 0,
            entry_count: 1,
            stored_digest: digest,
        },
    })
}
