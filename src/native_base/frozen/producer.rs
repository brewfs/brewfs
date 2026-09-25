//! Frozen snapshot production boundary.
//!
//! The producer turns a complete, already-consistent row inventory into
//! immutable authenticated objects.  It deliberately does not read a
//! control store itself: callers must provide a complete snapshot captured
//! under their own consistency protocol.  This keeps partial scans from
//! becoming mountable metadata.

use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

use crate::native_base::wire::container::{
    Codec, ContainerFooter, ContainerHeader, FOOTER_LEN, HEADER_LEN, ObjectKind, features,
};
use crate::native_base::wire::error::{WireError, WireResult};
use crate::native_base::wire::index_build::{
    IndexTreeParams, build_index_tree, build_index_tree_with_subtree_counts,
};
use crate::native_base::wire::refs::{
    ChildRef, ObjectId, ObjectRef, PageAddress, PageKind, RootRef,
};
use crate::native_base::write::receipts::{ObjectSink, object_key};

use super::{
    FrozenInodeRecord, FrozenRow, SnapshotManifest, canonical_table_digest,
    decode_canonical_attributes, decode_extent_slice_value, dentry_prefix, extent_prefix,
    inode_key,
};

const DEFAULT_LEAF_TARGET: usize = 16 * 1024;

#[derive(Clone, Debug)]
pub struct FrozenSnapshotInput {
    pub volume_id: [u8; 16],
    pub storage_namespace_id: [u8; 16],
    pub chunk_size: u64,
    pub block_size: u32,
    pub required_features: u64,
    pub logical_revision: [u8; 32],
    pub namespace_rows: Vec<FrozenRow>,
    pub data_rows: Vec<FrozenRow>,
    pub inventory_rows: Vec<FrozenRow>,
    pub file_count: u64,
    pub directory_count: u64,
    pub total_logical_bytes: u64,
    pub created_at_ns: i64,
    pub namespace_object_id: ObjectId,
    pub data_object_id: ObjectId,
    pub inventory_object_id: ObjectId,
    pub manifest_object_id: ObjectId,
}

#[derive(Clone, Debug)]
pub struct BuiltFrozenSnapshot {
    pub manifest: SnapshotManifest,
    pub manifest_object: ObjectRef,
    pub manifest_bytes: Vec<u8>,
    pub metadata: Vec<(ObjectRef, Vec<u8>)>,
}

fn validate_rows(name: &'static str, rows: &[FrozenRow]) -> WireResult<()> {
    if rows.is_empty() {
        return Err(WireError::invalid(name, "table must not be empty"));
    }
    for pair in rows.windows(2) {
        if pair[0].key >= pair[1].key {
            return Err(WireError::invalid(
                name,
                "rows must be strictly ordered by unique key",
            ));
        }
    }
    Ok(())
}

fn decode_be_u64(bytes: &[u8], what: &'static str) -> WireResult<u64> {
    bytes
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| WireError::invalid(what, "invalid u64 key"))
}

/// Validate the complete cross-table contract before constructing any object.
///
/// This is intentionally stricter than the page builder: a sorted B-tree can
/// still describe a namespace with missing inodes, dangling dentries, or data
/// rows that point at a different chunk. Such a snapshot must never become a
/// published read-only revision.
fn validate_snapshot_input(input: &FrozenSnapshotInput) -> WireResult<()> {
    validate_rows("frozen namespace", &input.namespace_rows)?;
    validate_rows("frozen data", &input.data_rows)?;
    validate_rows("frozen inventory", &input.inventory_rows)?;

    let mut inodes = BTreeMap::<u64, FrozenInodeRecord>::new();
    let mut dentries = Vec::new();
    for row in &input.namespace_rows {
        if row.key.first() == Some(&b'i') {
            if row.key.len() != inode_key(0).len() {
                return Err(WireError::invalid("frozen namespace", "invalid inode key"));
            }
            let inode = decode_be_u64(&row.key[1..], "frozen inode key")?;
            let attr = decode_canonical_attributes(&row.value)?;
            if inodes.insert(inode, attr).is_some() {
                return Err(WireError::invalid("frozen namespace", "duplicate inode"));
            }
            continue;
        }
        if row.key.first() == Some(&b'd') {
            let prefix_len = dentry_prefix(0).len();
            if row.key.len() <= prefix_len || row.key[9] != 0 {
                return Err(WireError::invalid("frozen namespace", "invalid dentry key"));
            }
            if row.value.len() != inode_key(0).len() || row.value[0] != b'i' {
                return Err(WireError::invalid(
                    "frozen namespace",
                    "invalid dentry value",
                ));
            }
            let parent = decode_be_u64(&row.key[1..9], "frozen dentry key")?;
            let target = decode_be_u64(&row.value[1..], "frozen dentry value")?;
            dentries.push((parent, target));
            continue;
        }
        return Err(WireError::invalid(
            "frozen namespace",
            "unknown row key family",
        ));
    }
    let root = inodes
        .get(&1)
        .ok_or_else(|| WireError::invalid("frozen namespace", "root inode is missing"))?;
    if root.kind != 2 {
        return Err(WireError::invalid(
            "frozen namespace",
            "root inode is not a directory",
        ));
    }
    for (parent, target) in dentries {
        let parent_attr = inodes
            .get(&parent)
            .ok_or_else(|| WireError::invalid("frozen namespace", "dentry parent is missing"))?;
        if parent_attr.kind != 2 {
            return Err(WireError::invalid(
                "frozen namespace",
                "dentry parent is not a directory",
            ));
        }
        if !inodes.contains_key(&target) {
            return Err(WireError::invalid(
                "frozen namespace",
                "dentry target is missing",
            ));
        }
    }

    let mut logical_bytes = 0u64;
    let mut file_count = 0u64;
    let mut directory_count = 0u64;
    for attr in inodes.values() {
        if attr.kind == 2 {
            directory_count = directory_count.checked_add(1).ok_or_else(|| {
                WireError::invalid("frozen namespace", "directory count overflow")
            })?;
        } else {
            file_count = file_count
                .checked_add(1)
                .ok_or_else(|| WireError::invalid("frozen namespace", "file count overflow"))?;
            logical_bytes = logical_bytes.checked_add(attr.size).ok_or_else(|| {
                WireError::invalid("frozen namespace", "logical byte count overflow")
            })?;
        }
    }
    if input.file_count != file_count
        || input.directory_count != directory_count
        || input.total_logical_bytes != logical_bytes
    {
        return Err(WireError::invalid(
            "frozen snapshot",
            "manifest counts do not match namespace inodes",
        ));
    }

    for row in &input.data_rows {
        let prefix_len = extent_prefix(0, 0).len();
        if row.key.len() != prefix_len + 8 || row.key.first() != Some(&b'e') {
            return Err(WireError::invalid("frozen data", "invalid extent key"));
        }
        let inode = decode_be_u64(&row.key[1..9], "frozen extent key")?;
        let chunk_index = decode_be_u64(&row.key[9..17], "frozen extent key")?;
        if !inodes.contains_key(&inode) {
            return Err(WireError::invalid("frozen data", "extent inode is missing"));
        }
        let offset = decode_be_u64(&row.key[17..], "frozen extent key")?;
        let (length, _, chunk_id, value_offset) = decode_extent_slice_value(&row.value)?;
        if length == 0 || value_offset != offset {
            return Err(WireError::invalid("frozen data", "extent range is invalid"));
        }
        let expected_chunk_id = crate::vfs::chunk_id_for(inode as i64, chunk_index)
            .map_err(|_| WireError::invalid("frozen data", "chunk id overflows"))?;
        if crate::vfs::extract_ino_and_chunk_index(expected_chunk_id) != (inode as i64, chunk_index)
            || chunk_id != expected_chunk_id
        {
            return Err(WireError::invalid(
                "frozen data",
                "extent chunk id mismatch",
            ));
        }
        offset
            .checked_add(length)
            .ok_or_else(|| WireError::invalid("frozen data", "extent range overflows"))?;
    }
    Ok(())
}

fn build_metadata_object(
    volume_id: &[u8; 16],
    object_id: ObjectId,
    rows: &[FrozenRow],
    with_subtree_counts: bool,
) -> WireResult<(ObjectRef, Vec<u8>, RootRef)> {
    validate_rows("frozen metadata", rows)?;
    let entries = rows
        .iter()
        .map(|row| (row.key.clone(), row.value.clone()))
        .collect::<Vec<_>>();
    let mut body = Vec::new();
    let params = IndexTreeParams::generic_with_codec(DEFAULT_LEAF_TARGET, Codec::Zstd);
    let child = if with_subtree_counts {
        build_index_tree_with_subtree_counts(&entries, &params, &mut body)?
    } else {
        build_index_tree(&entries, &params, &mut body)?
    };
    let ChildRef::Local(address) = child else {
        return Err(WireError::invalid(
            "frozen metadata",
            "root unexpectedly external",
        ));
    };
    while !(HEADER_LEN + body.len()).is_multiple_of(8) {
        body.push(0);
    }
    let object_len = HEADER_LEN + body.len() + FOOTER_LEN;
    let header = ContainerHeader {
        kind: ObjectKind::FrozenMetadata,
        required_features: features::ZSTD_FRAME_OR_PAGE,
        object_len: object_len as u64,
        root_offset: address.offset,
        root_stored_len: address.stored_len,
        root_raw_len: address.raw_len,
        hash_id: 1,
        root_codec: address.codec,
    };
    let footer = ContainerFooter {
        object_len: object_len as u64,
        root_stored_digest: address.stored_digest,
    };
    let mut bytes = Vec::with_capacity(object_len);
    bytes.extend_from_slice(&header.encode());
    bytes.extend_from_slice(&body);
    bytes.extend_from_slice(&footer.encode());
    let full_hash: [u8; 32] = Sha256::digest(&bytes).into();
    let object = ObjectRef {
        object_id,
        kind: ObjectKind::FrozenMetadata.as_u8(),
        object_len: object_len as u64,
        full_hash,
        key: object_key(volume_id, "frozen", &object_id, &full_hash),
    };
    let root = RootRef {
        object: object.clone(),
        address,
    };
    Ok((object, bytes, root))
}

fn ensure_distinct_object_ids(input: &FrozenSnapshotInput) -> WireResult<()> {
    let ids = [
        input.namespace_object_id,
        input.data_object_id,
        input.inventory_object_id,
        input.manifest_object_id,
    ];
    for (index, id) in ids.iter().enumerate() {
        if ids[..index].contains(id) {
            return Err(WireError::invalid(
                "frozen snapshot",
                "metadata and manifest object IDs must be distinct",
            ));
        }
    }
    Ok(())
}

fn build_manifest_object(
    volume_id: &[u8; 16],
    object_id: ObjectId,
    manifest: &SnapshotManifest,
) -> WireResult<(ObjectRef, Vec<u8>)> {
    let payload = manifest.encode()?;
    let digest: [u8; 32] = Sha256::digest(&payload).into();
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
    let object_len = HEADER_LEN + payload.len() + FOOTER_LEN;
    let header = ContainerHeader {
        kind: ObjectKind::SnapshotManifest,
        required_features: manifest.required_features,
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
    let full_hash: [u8; 32] = Sha256::digest(&bytes).into();
    let object = ObjectRef {
        object_id,
        kind: ObjectKind::SnapshotManifest.as_u8(),
        object_len: object_len as u64,
        full_hash,
        key: object_key(volume_id, "manifest", &object_id, &full_hash),
    };
    Ok((object, bytes))
}

pub fn build_snapshot(input: FrozenSnapshotInput) -> WireResult<BuiltFrozenSnapshot> {
    if input.chunk_size == 0 || input.block_size == 0 {
        return Err(WireError::invalid(
            "frozen snapshot",
            "chunk and block sizes must be non-zero",
        ));
    }
    ensure_distinct_object_ids(&input)?;
    validate_snapshot_input(&input)?;
    let (namespace_object, namespace_bytes, namespace_root) = build_metadata_object(
        &input.volume_id,
        input.namespace_object_id,
        &input.namespace_rows,
        true,
    )?;
    let (data_object, data_bytes, data_root) = build_metadata_object(
        &input.volume_id,
        input.data_object_id,
        &input.data_rows,
        false,
    )?;
    let (inventory_object, inventory_bytes, inventory_root) = build_metadata_object(
        &input.volume_id,
        input.inventory_object_id,
        &input.inventory_rows,
        false,
    )?;
    let manifest = SnapshotManifest {
        volume_id: input.volume_id,
        storage_namespace_id: input.storage_namespace_id,
        chunk_size: input.chunk_size,
        block_size: input.block_size,
        required_features: input.required_features,
        logical_revision: input.logical_revision,
        namespace_digest: canonical_table_digest(1, &input.namespace_rows),
        binding_digest: canonical_table_digest(2, &input.data_rows),
        namespace_mode: 2,
        kv_layer_id: None,
        kv_sealed_version: None,
        namespace_root: Some(namespace_root),
        data_root,
        inventory_root,
        file_count: input.file_count,
        directory_count: input.directory_count,
        total_logical_bytes: input.total_logical_bytes,
        created_at_ns: input.created_at_ns,
    };
    let (manifest_object, manifest_bytes) =
        build_manifest_object(&input.volume_id, input.manifest_object_id, &manifest)?;
    Ok(BuiltFrozenSnapshot {
        manifest,
        manifest_object,
        manifest_bytes,
        metadata: vec![
            (namespace_object, namespace_bytes),
            (data_object, data_bytes),
            (inventory_object, inventory_bytes),
        ],
    })
}

/// Upload metadata first and publish the manifest last.  A successful return
/// is the only point at which the caller may record the manifest identity.
pub async fn upload_snapshot(
    sink: &dyn ObjectSink,
    snapshot: &BuiltFrozenSnapshot,
) -> anyhow::Result<ObjectRef> {
    for (object, bytes) in &snapshot.metadata {
        sink.put(object, bytes).await?;
        let verified = sink.get(object).await?;
        if verified != *bytes {
            anyhow::bail!("frozen metadata readback differs from uploaded bytes")
        }
    }
    sink.put(&snapshot.manifest_object, &snapshot.manifest_bytes)
        .await?;
    let verified = sink.get(&snapshot.manifest_object).await?;
    if verified != snapshot.manifest_bytes {
        anyhow::bail!("frozen manifest readback differs from uploaded bytes")
    }
    Ok(snapshot.manifest_object.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_base::write::receipts::MemorySink;

    fn row(key: &[u8], value: &[u8]) -> FrozenRow {
        FrozenRow {
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    fn input() -> FrozenSnapshotInput {
        FrozenSnapshotInput {
            volume_id: [1; 16],
            storage_namespace_id: [2; 16],
            chunk_size: 4096,
            block_size: 512,
            required_features: 0,
            logical_revision: [3; 32],
            namespace_rows: vec![
                row(
                    &super::super::dentry_key(1, b"file"),
                    &super::super::inode_key(2),
                ),
                row(
                    &super::super::inode_key(1),
                    &super::super::FrozenInodeRecord {
                        kind: 2,
                        mode: 0o040755,
                        uid: 0,
                        gid: 0,
                        rdev: 0,
                        nlink: 2,
                        size: 0,
                        atime_ns: 0,
                        mtime_ns: 0,
                        ctime_ns: 0,
                        parent_hint: None,
                        symlink_target: None,
                    }
                    .encode(),
                ),
                row(
                    &super::super::inode_key(2),
                    &super::super::FrozenInodeRecord {
                        kind: 1,
                        mode: 0o100644,
                        uid: 0,
                        gid: 0,
                        rdev: 0,
                        nlink: 1,
                        size: 7,
                        atime_ns: 0,
                        mtime_ns: 0,
                        ctime_ns: 0,
                        parent_hint: Some(1),
                        symlink_target: None,
                    }
                    .encode(),
                ),
            ],
            data_rows: vec![row(
                &super::super::extent_key(2, 0, 0),
                &super::super::encode_extent_slice_value(
                    7,
                    8,
                    crate::vfs::chunk_id_for(2, 0).unwrap(),
                    0,
                ),
            )],
            inventory_rows: vec![row(b"i", b"object")],
            file_count: 1,
            directory_count: 1,
            total_logical_bytes: 7,
            created_at_ns: 4,
            namespace_object_id: [10; 16],
            data_object_id: [11; 16],
            inventory_object_id: [12; 16],
            manifest_object_id: [13; 16],
        }
    }

    #[test]
    fn producer_rejects_unsorted_or_empty_tables() {
        let mut bad = input();
        bad.data_rows.clear();
        assert!(build_snapshot(bad).is_err());
        let mut bad = input();
        bad.data_rows = vec![row(b"b", b"1"), row(b"a", b"2")];
        assert!(build_snapshot(bad).is_err());
        let mut bad = input();
        bad.manifest_object_id = bad.data_object_id;
        assert!(build_snapshot(bad).is_err());
    }

    #[test]
    fn producer_rejects_dangling_namespace_and_misaligned_extent_rows() {
        let mut dangling = input();
        dangling.namespace_rows[0].value = inode_key(999);
        assert!(build_snapshot(dangling).is_err());

        let mut wrong_count = input();
        wrong_count.file_count = 2;
        assert!(build_snapshot(wrong_count).is_err());

        let mut wrong_extent = input();
        wrong_extent.data_rows[0].value = super::super::encode_extent_slice_value(
            7,
            8,
            crate::vfs::chunk_id_for(2, 1).unwrap(),
            0,
        );
        assert!(build_snapshot(wrong_extent).is_err());
    }

    #[test]
    fn packed_metadata_pages_use_declared_zstd_codec() {
        let snapshot = build_snapshot(input()).unwrap();
        let (object, bytes) = &snapshot.metadata[0];
        let header = ContainerHeader::parse(bytes).unwrap();
        assert_eq!(header.kind, ObjectKind::FrozenMetadata);
        assert_eq!(header.required_features, features::ZSTD_FRAME_OR_PAGE);
        assert_eq!(header.root_codec, Codec::Zstd);
        assert_eq!(header.object_len, object.object_len);
        assert!(header.root_raw_len > 0);
    }

    #[tokio::test]
    async fn metadata_is_uploaded_before_manifest_and_roundtrips() {
        let snapshot = build_snapshot(input()).unwrap();
        let sink = MemorySink::default();
        let manifest = upload_snapshot(&sink, &snapshot).await.unwrap();
        assert_eq!(manifest, snapshot.manifest_object);
        assert!(sink.get(&manifest).await.is_ok());
        for (object, _) in &snapshot.metadata {
            assert!(sink.get(object).await.is_ok());
        }
    }
}
