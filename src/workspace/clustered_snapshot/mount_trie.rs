//! Typed, page-oriented mount trie payload for `BRFSM002`.
//!
//! The manifest envelope authenticates the complete section, while this
//! payload adds a small directory of independently checked pages.  Mount
//! lookup can therefore keep the root page resident and fetch only the page
//! containing a deeper trie node.

use std::collections::BTreeMap;

use crate::native_base::wire::error::{WireError, WireResult};
use crate::native_base::wire::uvarint::{Reader, Writer};

use super::name::NameBytes;

pub const MOUNT_TRIE_MAGIC: &[u8; 8] = b"BRFMTI02";
pub const MOUNT_TRIE_HEADER_LEN: usize = 64;
pub const MOUNT_TRIE_PAGE_REF_LEN: usize = 64;
const MOUNT_TRIE_VERSION: u16 = 1;
const PAGE_RECORD_LIMIT: usize = 256;
const PAGE_TARGET_BYTES: usize = 64 * 1024;
const MAX_NODES: usize = (1 << 29) - 1;
const MAX_CHILDREN_PER_NODE: usize = 1 << 20;
const MAX_CLUSTER_SLOTS: usize = (1 << 29) - 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MountTrieChild {
    pub name: NameBytes,
    pub node_id: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MountTrieNode {
    pub node_id: u32,
    pub parent_id: u32,
    pub component: Option<NameBytes>,
    pub cluster_slots: Vec<u32>,
    pub children: Vec<MountTrieChild>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MountTriePageRef {
    pub first_node_id: u32,
    pub last_node_id: u32,
    pub offset: u64,
    pub len: u32,
    pub count: u32,
    pub digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MountTrieIndex {
    pub node_count: u64,
    pub page_record_limit: u32,
    pub pages: Vec<MountTriePageRef>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MountTrie {
    pub nodes: Vec<MountTrieNode>,
}

impl MountTrie {
    pub fn encode(&self) -> WireResult<Vec<u8>> {
        validate_nodes(&self.nodes)?;
        let (index, directory, pages) = build_pages(&self.nodes)?;
        let mut header = [0u8; MOUNT_TRIE_HEADER_LEN];
        header[..8].copy_from_slice(MOUNT_TRIE_MAGIC);
        header[8..10].copy_from_slice(&MOUNT_TRIE_VERSION.to_le_bytes());
        header[12..16].copy_from_slice(&(index.pages.len() as u32).to_le_bytes());
        header[16..24].copy_from_slice(&(self.nodes.len() as u64).to_le_bytes());
        header[24..28].copy_from_slice(&(directory.len() as u32).to_le_bytes());
        header[28..32].copy_from_slice(&(index.page_record_limit).to_le_bytes());
        header[32..64].copy_from_slice(blake3::hash(&directory).as_bytes());

        let mut payload = Vec::with_capacity(
            MOUNT_TRIE_HEADER_LEN
                .checked_add(directory.len())
                .and_then(|length| {
                    pages
                        .iter()
                        .map(Vec::len)
                        .try_fold(length, |total, page_len| total.checked_add(page_len))
                })
                .ok_or_else(|| WireError::LimitExceeded("mount trie payload overflows".into()))?,
        );
        payload.extend_from_slice(&header);
        payload.extend_from_slice(&directory);
        for page in pages {
            payload.extend_from_slice(&page);
        }
        if payload.len() > 64 * 1024 * 1024 {
            return Err(WireError::LimitExceeded(
                "mount trie payload exceeds 64 MiB".into(),
            ));
        }
        Ok(payload)
    }

    pub fn decode(bytes: &[u8]) -> WireResult<Self> {
        if bytes.len() > 64 * 1024 * 1024 {
            return Err(WireError::LimitExceeded(
                "mount trie payload exceeds 64 MiB".into(),
            ));
        }
        let index = decode_index(bytes)?;
        let mut nodes = Vec::with_capacity(index.node_count as usize);
        for page in &index.pages {
            let page_bytes = page_bytes(bytes, page)?;
            verify_page(page_bytes, &page.digest)?;
            nodes.extend(decode_page(
                page_bytes,
                page.count,
                page.first_node_id,
                page.last_node_id,
            )?);
        }
        validate_nodes(&nodes)?;
        Ok(Self { nodes })
    }

    pub fn index(bytes: &[u8]) -> WireResult<MountTrieIndex> {
        if bytes.len() > 64 * 1024 * 1024 {
            return Err(WireError::LimitExceeded(
                "mount trie payload exceeds 64 MiB".into(),
            ));
        }
        decode_index(bytes)
    }

    /// Decode the fixed header and page directory when the page bodies are
    /// still remote. `payload_len` is the complete typed payload length, not
    /// the length of the prefix supplied in `bytes`.
    pub fn index_from_directory(bytes: &[u8], payload_len: usize) -> WireResult<MountTrieIndex> {
        if payload_len > 64 * 1024 * 1024 || bytes.len() > payload_len {
            return Err(WireError::LimitExceeded(
                "mount trie payload exceeds 64 MiB".into(),
            ));
        }
        decode_index_with_payload_len(bytes, payload_len)
    }

    pub fn decode_page_at(bytes: &[u8], page: &MountTriePageRef) -> WireResult<Vec<MountTrieNode>> {
        let page_bytes = page_bytes(bytes, page)?;
        verify_page(page_bytes, &page.digest)?;
        decode_page(
            page_bytes,
            page.count,
            page.first_node_id,
            page.last_node_id,
        )
    }

    /// Verify and decode a page fetched as a standalone range.
    pub fn decode_page_bytes(
        bytes: &[u8],
        page: &MountTriePageRef,
    ) -> WireResult<Vec<MountTrieNode>> {
        if bytes.len() != page.len as usize {
            return Err(WireError::Truncated {
                what: "mount trie page",
                need: page.len as usize,
                have: bytes.len(),
            });
        }
        verify_page(bytes, &page.digest)?;
        decode_page(bytes, page.count, page.first_node_id, page.last_node_id)
    }
}

fn validate_nodes(nodes: &[MountTrieNode]) -> WireResult<()> {
    if nodes.len() > MAX_NODES || nodes.is_empty() {
        return Err(WireError::invalid(
            "mount trie",
            "node count is outside bounds",
        ));
    }
    if nodes[0].node_id != 1 || nodes[0].parent_id != 0 || nodes[0].component.is_some() {
        return Err(WireError::invalid(
            "mount trie",
            "root node must be node 1 with no parent or component",
        ));
    }
    let mut seen = BTreeMap::new();
    for (position, node) in nodes.iter().enumerate() {
        if node.node_id == 0 || seen.insert(node.node_id, position).is_some() {
            return Err(WireError::invalid("mount trie", "node ids are not unique"));
        }
        if position > 0 && node.node_id <= nodes[position - 1].node_id {
            return Err(WireError::invalid(
                "mount trie",
                "nodes are not sorted by id",
            ));
        }
        if node.node_id != position as u32 + 1 {
            return Err(WireError::invalid("mount trie", "node ids are not dense"));
        }
        if position > 0
            && (node.parent_id == 0
                || node.parent_id >= node.node_id
                || node.parent_id > u32::try_from(nodes.len()).unwrap_or(u32::MAX))
        {
            return Err(WireError::invalid(
                "mount trie",
                "nodes are not parent-first",
            ));
        }
        if node.component.is_none() && position != 0 {
            return Err(WireError::invalid(
                "mount trie",
                "only the root may omit a component",
            ));
        }
        if node.cluster_slots.len() > MAX_CLUSTER_SLOTS {
            return Err(WireError::LimitExceeded(
                "mount trie cluster slot list exceeds limit".into(),
            ));
        }
        if node
            .cluster_slots
            .windows(2)
            .any(|pair| pair[0] >= pair[1] || pair[0] >= (1 << 29))
            || node
                .cluster_slots
                .last()
                .is_some_and(|slot| *slot >= (1 << 29))
        {
            return Err(WireError::invalid(
                "mount trie",
                "cluster slots are not sorted or exceed the snapshot limit",
            ));
        }
        if node.children.len() > MAX_CHILDREN_PER_NODE {
            return Err(WireError::LimitExceeded(
                "mount trie child list exceeds limit".into(),
            ));
        }
        for pair in node.children.windows(2) {
            if pair[0].name >= pair[1].name || pair[0].node_id >= pair[1].node_id {
                return Err(WireError::invalid(
                    "mount trie",
                    "children are not sorted and duplicate-free",
                ));
            }
        }
        if node.children.iter().any(|child| {
            child.node_id == 0 || child.node_id > u32::try_from(nodes.len()).unwrap_or(0)
        }) {
            return Err(WireError::invalid(
                "mount trie",
                "child references an unknown node",
            ));
        }
    }
    for node in nodes {
        if node.parent_id != 0 {
            let parent = &nodes[node.parent_id as usize - 1];
            if !parent.children.iter().any(|child| {
                child.node_id == node.node_id && node.component.as_ref() == Some(&child.name)
            }) {
                return Err(WireError::invalid(
                    "mount trie",
                    "parent/child references disagree",
                ));
            }
        }
    }
    Ok(())
}

fn encode_node(node: &MountTrieNode) -> WireResult<Vec<u8>> {
    let mut writer = Writer::new();
    writer.u32(node.node_id);
    writer.u32(node.parent_id);
    writer.option_tag(node.component.is_some());
    writer.u8(0);
    writer.u16(0);
    if let Some(component) = &node.component {
        writer.bytes(component.as_bytes());
    }
    writer.uvarint(node.cluster_slots.len() as u64);
    for slot in &node.cluster_slots {
        writer.u32(*slot);
    }
    writer.uvarint(node.children.len() as u64);
    for child in &node.children {
        writer.bytes(child.name.as_bytes());
        writer.u32(child.node_id);
    }
    Ok(writer.into_bytes())
}

fn decode_node(reader: &mut Reader<'_>) -> WireResult<MountTrieNode> {
    let node_id = reader.u32("mount trie node")?;
    let parent_id = reader.u32("mount trie node")?;
    let has_component = reader.option_tag("mount trie node component")?;
    if reader.u8("mount trie node")? != 0 || reader.u16("mount trie node")? != 0 {
        return Err(WireError::invalid(
            "mount trie node",
            "reserved bytes are non-zero",
        ));
    }
    let component = if has_component {
        Some(
            NameBytes::new(reader.bytes("mount trie component")?.to_vec())
                .map_err(|error| WireError::invalid("mount trie component", error.to_string()))?,
        )
    } else {
        None
    };
    let slot_count = usize::try_from(reader.uvarint("mount trie slots")?)
        .map_err(|_| WireError::LimitExceeded("mount trie slot count exceeds usize".into()))?;
    if slot_count > MAX_CLUSTER_SLOTS || slot_count.saturating_mul(4) > reader.remaining() {
        return Err(WireError::LimitExceeded(
            "mount trie slot count exceeds bounded input".into(),
        ));
    }
    let mut cluster_slots = Vec::with_capacity(slot_count);
    for _ in 0..slot_count {
        cluster_slots.push(reader.u32("mount trie slot")?);
    }
    let child_count = usize::try_from(reader.uvarint("mount trie children")?)
        .map_err(|_| WireError::LimitExceeded("mount trie child count exceeds usize".into()))?;
    if child_count > MAX_CHILDREN_PER_NODE {
        return Err(WireError::LimitExceeded(
            "mount trie child count exceeds limit".into(),
        ));
    }
    let mut children = Vec::with_capacity(child_count);
    for _ in 0..child_count {
        let name = NameBytes::new(reader.bytes("mount trie child name")?.to_vec())
            .map_err(|error| WireError::invalid("mount trie child name", error.to_string()))?;
        children.push(MountTrieChild {
            name,
            node_id: reader.u32("mount trie child")?,
        });
    }
    Ok(MountTrieNode {
        node_id,
        parent_id,
        component,
        cluster_slots,
        children,
    })
}

fn build_pages(nodes: &[MountTrieNode]) -> WireResult<(MountTrieIndex, Vec<u8>, Vec<Vec<u8>>)> {
    let mut pages = Vec::new();
    let mut refs = Vec::new();
    let mut current = Vec::new();
    let mut first = 0u32;
    let mut last = 0u32;
    let mut count = 0usize;
    for node in nodes {
        let record = encode_node(node)?;
        if !current.is_empty()
            && (count >= PAGE_RECORD_LIMIT
                || current
                    .len()
                    .checked_add(record.len())
                    .is_none_or(|length| length > PAGE_TARGET_BYTES))
        {
            pages.push(std::mem::take(&mut current));
            refs.push((first, last, count as u32));
            count = 0;
        }
        if current.is_empty() {
            first = node.node_id;
        }
        last = node.node_id;
        count += 1;
        current.extend_from_slice(&record);
    }
    if !current.is_empty() {
        pages.push(current);
        refs.push((first, last, count as u32));
    }
    let directory_len = pages
        .len()
        .checked_mul(MOUNT_TRIE_PAGE_REF_LEN)
        .ok_or_else(|| WireError::LimitExceeded("mount trie directory overflows".into()))?;
    let mut directory = Vec::with_capacity(directory_len);
    let mut offset = u64::try_from(MOUNT_TRIE_HEADER_LEN + directory_len)
        .map_err(|_| WireError::LimitExceeded("mount trie page offset overflows".into()))?;
    let mut page_refs = Vec::with_capacity(pages.len());
    for (index, page) in pages.iter().enumerate() {
        let len = u32::try_from(page.len())
            .map_err(|_| WireError::LimitExceeded("mount trie page exceeds u32".into()))?;
        let (first_node_id, last_node_id, count) = refs[index];
        let digest = *blake3::hash(page).as_bytes();
        directory.extend_from_slice(&first_node_id.to_le_bytes());
        directory.extend_from_slice(&last_node_id.to_le_bytes());
        directory.extend_from_slice(&[0; 8]);
        directory.extend_from_slice(&offset.to_le_bytes());
        directory.extend_from_slice(&len.to_le_bytes());
        directory.extend_from_slice(&count.to_le_bytes());
        directory.extend_from_slice(&digest);
        page_refs.push(MountTriePageRef {
            first_node_id,
            last_node_id,
            offset,
            len,
            count,
            digest,
        });
        offset = offset
            .checked_add(u64::from(len))
            .ok_or_else(|| WireError::LimitExceeded("mount trie page offset overflows".into()))?;
    }
    Ok((
        MountTrieIndex {
            node_count: nodes.len() as u64,
            page_record_limit: PAGE_RECORD_LIMIT as u32,
            pages: page_refs,
        },
        directory,
        pages,
    ))
}

fn decode_index(bytes: &[u8]) -> WireResult<MountTrieIndex> {
    decode_index_with_payload_len(bytes, bytes.len())
}

fn decode_index_with_payload_len(bytes: &[u8], payload_len: usize) -> WireResult<MountTrieIndex> {
    if bytes.len() < MOUNT_TRIE_HEADER_LEN || &bytes[..8] != MOUNT_TRIE_MAGIC {
        return Err(WireError::UnsupportedFormat(
            "not a BRFMTI02 mount trie payload".into(),
        ));
    }
    if u16::from_le_bytes(bytes[8..10].try_into().unwrap()) != MOUNT_TRIE_VERSION
        || bytes[10..12].iter().any(|byte| *byte != 0)
    {
        return Err(WireError::UnsupportedFormat(
            "unsupported mount trie payload version".into(),
        ));
    }
    let page_count = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
    let node_count = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
    if node_count == 0 || node_count > MAX_NODES as u64 {
        return Err(WireError::LimitExceeded(
            "mount trie node count exceeds limit".into(),
        ));
    }
    let directory_len = u32::from_le_bytes(bytes[24..28].try_into().unwrap()) as usize;
    let expected_directory_len = (page_count as usize)
        .checked_mul(MOUNT_TRIE_PAGE_REF_LEN)
        .ok_or_else(|| WireError::LimitExceeded("mount trie directory overflows".into()))?;
    if directory_len != expected_directory_len {
        return Err(WireError::invalid(
            "mount trie index",
            "directory length is not canonical",
        ));
    }
    let page_record_limit = u32::from_le_bytes(bytes[28..32].try_into().unwrap());
    if page_record_limit == 0 || page_record_limit as usize > PAGE_RECORD_LIMIT {
        return Err(WireError::invalid(
            "mount trie index",
            "page record limit is outside bounds",
        ));
    }
    let directory_end = MOUNT_TRIE_HEADER_LEN
        .checked_add(directory_len)
        .ok_or_else(|| WireError::LimitExceeded("mount trie directory overflows".into()))?;
    if directory_end > bytes.len()
        || blake3::hash(&bytes[MOUNT_TRIE_HEADER_LEN..directory_end]).as_bytes() != &bytes[32..64]
    {
        return Err(WireError::HashMismatch {
            what: "mount trie directory",
            stored: hex::encode(&bytes[32..64]),
            computed: hex::encode(
                blake3::hash(&bytes[MOUNT_TRIE_HEADER_LEN..directory_end]).as_bytes(),
            ),
        });
    }
    let mut pages = Vec::with_capacity(page_count as usize);
    let mut expected_offset = directory_end as u64;
    let mut total_count = 0u64;
    let mut previous_last = 0u32;
    for index in 0..page_count as usize {
        let start = MOUNT_TRIE_HEADER_LEN + index * MOUNT_TRIE_PAGE_REF_LEN;
        let entry = &bytes[start..start + MOUNT_TRIE_PAGE_REF_LEN];
        if entry[8..16].iter().any(|byte| *byte != 0) {
            return Err(WireError::invalid(
                "mount trie index",
                "reserved page bytes are non-zero",
            ));
        }
        let first_node_id = u32::from_le_bytes(entry[..4].try_into().unwrap());
        let last_node_id = u32::from_le_bytes(entry[4..8].try_into().unwrap());
        let offset = u64::from_le_bytes(entry[16..24].try_into().unwrap());
        let len = u32::from_le_bytes(entry[24..28].try_into().unwrap());
        let count = u32::from_le_bytes(entry[28..32].try_into().unwrap());
        let digest = entry[32..64].try_into().unwrap();
        let end = offset
            .checked_add(u64::from(len))
            .ok_or_else(|| WireError::LimitExceeded("mount trie page end overflows".into()))?;
        if first_node_id == 0
            || first_node_id > last_node_id
            || (index > 0 && first_node_id <= previous_last)
            || offset != expected_offset
            || len == 0
            || count == 0
            || count > page_record_limit
            || end > payload_len as u64
        {
            return Err(WireError::invalid(
                "mount trie index",
                "page ordering or bounds are invalid",
            ));
        }
        total_count = total_count
            .checked_add(u64::from(count))
            .ok_or_else(|| WireError::LimitExceeded("mount trie node count overflows".into()))?;
        previous_last = last_node_id;
        expected_offset = end;
        pages.push(MountTriePageRef {
            first_node_id,
            last_node_id,
            offset,
            len,
            count,
            digest,
        });
    }
    if total_count != node_count || expected_offset != payload_len as u64 {
        return Err(WireError::invalid(
            "mount trie index",
            "page counts do not cover the complete payload",
        ));
    }
    Ok(MountTrieIndex {
        node_count,
        page_record_limit,
        pages,
    })
}

fn page_bytes<'a>(bytes: &'a [u8], page: &MountTriePageRef) -> WireResult<&'a [u8]> {
    let start = usize::try_from(page.offset)
        .map_err(|_| WireError::LimitExceeded("mount trie page offset exceeds usize".into()))?;
    let end = start
        .checked_add(page.len as usize)
        .ok_or_else(|| WireError::LimitExceeded("mount trie page end overflows".into()))?;
    bytes.get(start..end).ok_or(WireError::Truncated {
        what: "mount trie page",
        need: page.len as usize,
        have: bytes.len().saturating_sub(start),
    })
}

fn verify_page(bytes: &[u8], expected: &[u8; 32]) -> WireResult<()> {
    let actual = blake3::hash(bytes);
    if expected != actual.as_bytes() {
        return Err(WireError::HashMismatch {
            what: "mount trie page",
            stored: hex::encode(expected),
            computed: hex::encode(actual.as_bytes()),
        });
    }
    Ok(())
}

fn decode_page(
    bytes: &[u8],
    count: u32,
    first_node_id: u32,
    last_node_id: u32,
) -> WireResult<Vec<MountTrieNode>> {
    let mut reader = Reader::new(bytes);
    let mut nodes = Vec::with_capacity(count as usize);
    let mut previous = 0u32;
    for _ in 0..count {
        let node = decode_node(&mut reader)?;
        if node.node_id <= previous {
            return Err(WireError::invalid(
                "mount trie page",
                "node ids are not sorted",
            ));
        }
        previous = node.node_id;
        nodes.push(node);
    }
    if !reader.is_empty()
        || nodes.first().map(|node| node.node_id) != Some(first_node_id)
        || nodes.last().map(|node| node.node_id) != Some(last_node_id)
    {
        return Err(WireError::invalid(
            "mount trie page",
            "page record coverage is not canonical",
        ));
    }
    Ok(nodes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trie() -> MountTrie {
        let root = MountTrieNode {
            node_id: 1,
            parent_id: 0,
            component: None,
            cluster_slots: vec![0, 2],
            children: vec![
                MountTrieChild {
                    name: NameBytes::new(b"data".to_vec()).unwrap(),
                    node_id: 2,
                },
                MountTrieChild {
                    name: NameBytes::new(b"src".to_vec()).unwrap(),
                    node_id: 3,
                },
            ],
        };
        MountTrie {
            nodes: vec![
                root,
                MountTrieNode {
                    node_id: 2,
                    parent_id: 1,
                    component: Some(NameBytes::new(b"data".to_vec()).unwrap()),
                    cluster_slots: vec![0],
                    children: Vec::new(),
                },
                MountTrieNode {
                    node_id: 3,
                    parent_id: 1,
                    component: Some(NameBytes::new(b"src".to_vec()).unwrap()),
                    cluster_slots: vec![2],
                    children: Vec::new(),
                },
            ],
        }
    }

    #[test]
    fn mount_trie_round_trips_and_is_deterministic() {
        let trie = trie();
        let first = trie.encode().unwrap();
        assert_eq!(first, trie.encode().unwrap());
        assert_eq!(MountTrie::decode(&first).unwrap(), trie);
        let index = MountTrie::index(&first).unwrap();
        assert_eq!(index.node_count, 3);
        assert_eq!(index.pages.len(), 1);
    }

    #[test]
    fn mount_trie_rejects_tampered_page_and_parent_mismatch() {
        let trie = trie();
        let mut bytes = trie.encode().unwrap();
        let page_offset = MountTrie::index(&bytes).unwrap().pages[0].offset as usize;
        bytes[page_offset] ^= 1;
        assert!(matches!(
            MountTrie::decode(&bytes),
            Err(WireError::HashMismatch { .. })
        ));

        let mut invalid = trie;
        invalid.nodes[1].parent_id = 3;
        assert!(invalid.encode().is_err());
    }
}
