//! Independently decodable cold-attribute batches for metadata v2.
//!
//! Namespace batches keep hot inode fields small.  Symlink targets, xattrs,
//! and ACL bytes live here in LocalNodeId order and can be fetched/evicted
//! independently of namespace and extent batches.

use crate::native_base::wire::error::{WireError, WireResult};
use crate::native_base::wire::uvarint::{Reader, Writer};

use super::batch::{BatchKind, EncodedBatch, encode_batch};

const MAX_GROUPS: usize = 1 << 20;
const MAX_XATTRS_PER_GROUP: usize = 1 << 16;
const MAX_SYMLINK_BYTES: usize = 16 * 1024 * 1024;
const MAX_XATTR_NAME_BYTES: usize = 255;
const MAX_XATTR_VALUE_BYTES: usize = 16 * 1024 * 1024;
const MAX_ACL_BYTES: usize = 16 * 1024 * 1024;

/// Canonical key used by the cold attribute index.
pub fn attribute_index_key(local_node_id: u32) -> [u8; 4] {
    local_node_id.to_be_bytes()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct XattrRecord {
    pub name: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttributeGroup {
    pub local_node_id: u32,
    pub symlink_target: Option<Vec<u8>>,
    pub xattrs: Vec<XattrRecord>,
    pub acl: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttributeBatch {
    pub cluster_id: [u8; 16],
    pub batch_id: u32,
    pub stream_ordinal: u32,
    pub predecessor_ordinal: u32,
    pub groups: Vec<AttributeGroup>,
}

impl AttributeBatch {
    pub fn encode(&self) -> WireResult<EncodedBatch> {
        validate_groups(&self.groups)?;
        let mut payload = Writer::new();
        for group in &self.groups {
            encode_group(group, &mut payload)?;
        }
        let first_key = self
            .groups
            .first()
            .map(|group| attribute_index_key(group.local_node_id))
            .unwrap_or([0; 4]);
        encode_batch(
            BatchKind::Attribute,
            0,
            self.cluster_id,
            self.batch_id,
            self.stream_ordinal,
            self.predecessor_ordinal,
            self.groups.len() as u32,
            0,
            0,
            &first_key,
            payload.as_slice(),
        )
    }

    pub fn decode(encoded: &EncodedBatch) -> WireResult<Self> {
        if encoded.header.kind != BatchKind::Attribute {
            return Err(WireError::invalid(
                "attribute batch",
                "batch kind is not attribute",
            ));
        }
        if encoded.header.first_new_node_id != 0 || encoded.header.new_node_count != 0 {
            return Err(WireError::invalid(
                "attribute batch",
                "attribute batch cannot introduce namespace nodes",
            ));
        }
        let expected_count = usize::try_from(encoded.header.record_count)
            .map_err(|_| WireError::LimitExceeded("attribute group count exceeds usize".into()))?;
        if expected_count == 0 || expected_count > MAX_GROUPS {
            return Err(WireError::LimitExceeded(
                "attribute group count is outside bounds".into(),
            ));
        }
        let mut reader = Reader::new(&encoded.raw_payload);
        let mut groups = Vec::with_capacity(expected_count);
        for _ in 0..expected_count {
            groups.push(decode_group(&mut reader)?);
        }
        if !reader.is_empty() {
            return Err(WireError::invalid(
                "attribute batch",
                "trailing bytes after groups",
            ));
        }
        validate_groups(&groups)?;
        if encoded.header.cluster_id == [0; 16] {
            return Err(WireError::invalid(
                "attribute batch",
                "cluster id must be non-zero",
            ));
        }
        Ok(Self {
            cluster_id: encoded.header.cluster_id,
            batch_id: encoded.header.batch_id,
            stream_ordinal: encoded.header.stream_ordinal,
            predecessor_ordinal: encoded.header.predecessor_ordinal,
            groups,
        })
    }
}

fn encode_group(group: &AttributeGroup, writer: &mut Writer) -> WireResult<()> {
    validate_group(group)?;
    writer.uvarint(u64::from(group.local_node_id));
    match &group.symlink_target {
        Some(target) => {
            writer.option_tag(true);
            writer.bytes(target);
        }
        None => writer.option_tag(false),
    }
    writer.uvarint(group.xattrs.len() as u64);
    for xattr in &group.xattrs {
        writer.bytes(&xattr.name);
        writer.bytes(&xattr.value);
    }
    match &group.acl {
        Some(acl) => {
            writer.option_tag(true);
            writer.bytes(acl);
        }
        None => writer.option_tag(false),
    }
    Ok(())
}

fn decode_group(reader: &mut Reader<'_>) -> WireResult<AttributeGroup> {
    let local_node_id = u32::try_from(reader.uvarint("attribute group")?)
        .map_err(|_| WireError::LimitExceeded("attribute local node id exceeds u32".into()))?;
    let symlink_target = if reader.option_tag("attribute symlink target")? {
        let target = reader.bytes("attribute symlink target")?;
        if target.len() > MAX_SYMLINK_BYTES {
            return Err(WireError::LimitExceeded(
                "symlink target exceeds configured bound".into(),
            ));
        }
        Some(target.to_vec())
    } else {
        None
    };
    let xattr_count = usize::try_from(reader.uvarint("attribute xattrs")?)
        .map_err(|_| WireError::LimitExceeded("xattr count exceeds usize".into()))?;
    if xattr_count > MAX_XATTRS_PER_GROUP {
        return Err(WireError::LimitExceeded(
            "xattr count exceeds configured bound".into(),
        ));
    }
    let mut xattrs = Vec::with_capacity(xattr_count);
    for _ in 0..xattr_count {
        let name = reader.bytes("xattr name")?.to_vec();
        let value = reader.bytes("xattr value")?.to_vec();
        if name.len() > MAX_XATTR_NAME_BYTES || value.len() > MAX_XATTR_VALUE_BYTES {
            return Err(WireError::LimitExceeded(
                "xattr name or value exceeds configured bound".into(),
            ));
        }
        xattrs.push(XattrRecord { name, value });
    }
    let acl = if reader.option_tag("attribute acl")? {
        let acl = reader.bytes("attribute acl")?;
        if acl.len() > MAX_ACL_BYTES {
            return Err(WireError::LimitExceeded(
                "ACL exceeds configured bound".into(),
            ));
        }
        Some(acl.to_vec())
    } else {
        None
    };
    let group = AttributeGroup {
        local_node_id,
        symlink_target,
        xattrs,
        acl,
    };
    validate_group(&group)?;
    Ok(group)
}

fn validate_groups(groups: &[AttributeGroup]) -> WireResult<()> {
    if groups.is_empty() || groups.len() > MAX_GROUPS {
        return Err(WireError::LimitExceeded(
            "attribute group count is outside bounds".into(),
        ));
    }
    let mut previous = 0u32;
    for (index, group) in groups.iter().enumerate() {
        validate_group(group)?;
        if index > 0 && group.local_node_id <= previous {
            return Err(WireError::invalid(
                "attribute groups",
                "local node ids are not strictly sorted",
            ));
        }
        previous = group.local_node_id;
    }
    Ok(())
}

fn validate_group(group: &AttributeGroup) -> WireResult<()> {
    if group.local_node_id == 0 {
        return Err(WireError::invalid(
            "attribute group",
            "local node id must be non-zero",
        ));
    }
    if group
        .symlink_target
        .as_ref()
        .is_some_and(|target| target.len() > MAX_SYMLINK_BYTES)
    {
        return Err(WireError::LimitExceeded(
            "symlink target exceeds configured bound".into(),
        ));
    }
    if group.xattrs.len() > MAX_XATTRS_PER_GROUP {
        return Err(WireError::LimitExceeded(
            "xattr count exceeds configured bound".into(),
        ));
    }
    let mut previous_name: Option<&[u8]> = None;
    for xattr in &group.xattrs {
        if xattr.name.is_empty() || xattr.name.len() > MAX_XATTR_NAME_BYTES {
            return Err(WireError::invalid(
                "xattr name",
                "name must be non-empty and within bounds",
            ));
        }
        if xattr.value.len() > MAX_XATTR_VALUE_BYTES {
            return Err(WireError::LimitExceeded(
                "xattr value exceeds configured bound".into(),
            ));
        }
        if let Some(previous_name) = previous_name
            && xattr.name.as_slice() <= previous_name
        {
            return Err(WireError::invalid(
                "attribute xattrs",
                "xattr names are not strictly sorted",
            ));
        }
        previous_name = Some(&xattr.name);
    }
    if group
        .acl
        .as_ref()
        .is_some_and(|acl| acl.len() > MAX_ACL_BYTES)
    {
        return Err(WireError::LimitExceeded(
            "ACL exceeds configured bound".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace_overlay::clustered_snapshot::batch::BatchHeader;

    fn batch() -> AttributeBatch {
        AttributeBatch {
            cluster_id: [1; 16],
            batch_id: 2,
            stream_ordinal: 4,
            predecessor_ordinal: BatchHeader::NO_PREDECESSOR,
            groups: vec![
                AttributeGroup {
                    local_node_id: 7,
                    symlink_target: None,
                    xattrs: vec![XattrRecord {
                        name: b"user.author".to_vec(),
                        value: b"brewfs".to_vec(),
                    }],
                    acl: Some(b"acl-bytes".to_vec()),
                },
                AttributeGroup {
                    local_node_id: 9,
                    symlink_target: Some(b"../target".to_vec()),
                    xattrs: Vec::new(),
                    acl: None,
                },
            ],
        }
    }

    #[test]
    fn attribute_batch_round_trips_deterministically() {
        let expected = batch();
        let first = expected.encode().unwrap();
        let second = expected.encode().unwrap();
        assert_eq!(first.bytes, second.bytes);
        let decoded = AttributeBatch::decode(&EncodedBatch::decode(&first.bytes).unwrap()).unwrap();
        assert_eq!(decoded, expected);
        assert_eq!(first.header.kind, BatchKind::Attribute);
        assert_eq!(first.header.record_count, 2);
    }

    #[test]
    fn attribute_batch_rejects_unsorted_or_duplicate_xattrs() {
        let mut invalid = batch();
        invalid.groups[0].xattrs.push(XattrRecord {
            name: b"user.author".to_vec(),
            value: Vec::new(),
        });
        assert!(invalid.encode().is_err());
        let mut invalid = batch();
        invalid.groups.swap(0, 1);
        assert!(invalid.encode().is_err());
    }

    #[test]
    fn attribute_index_key_is_big_endian() {
        assert_eq!(attribute_index_key(0x0102_0304), [1, 2, 3, 4]);
    }
}
