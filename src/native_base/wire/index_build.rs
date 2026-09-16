//! The shared BNPG index-tree builder (spec 02 §7/§8).
//!
//! Every authenticated index in this format — the four Data Seal tables, the
//! physical inventory, the RetainBatch indexes — is a BNPG tree stored inside
//! one container: 8-byte-aligned digest-authenticated pages appended to a body
//! region, with the root referenced from the container header. The tree shape
//! (leaf chunking, bottom-up internal levels, closed `[min_key, max_key]`
//! ranges) is identical everywhere; only the leaf target and the *page kind*
//! the RootRef advertises differ.
//!
//! This module owns that one implementation so the seal writer (PR03) and the
//! inventory/retention writers (PR06A) cannot drift apart. It writes only
//! structurally valid pages — referential integrity is the caller's job.

use sha2::{Digest, Sha256};

use super::error::{WireError, WireResult};
use super::page::{
    BnpgKind, IndexPage, InternalEntry, LeafEntry, MAX_PAGE_ENTRIES, MAX_RAW_PAGE, PageBody,
};
use super::refs::{ChildRef, MAX_INDEX_LEVEL, PageAddress, PageKind};
use super::container::HEADER_LEN;

/// Shape parameters of one index tree.
#[derive(Debug, Clone, Copy)]
pub struct IndexTreeParams {
    /// Target encoded bytes per leaf page; pages are still capped hard by
    /// [`MAX_RAW_PAGE`], this only controls chunking.
    pub leaf_target: usize,
    /// BNPG kind of every page in the tree (kind 1 today).
    pub leaf_kind: BnpgKind,
    /// Page kind advertised by the resulting [`PageAddress`]es. This is what
    /// binds a tree to the container kind that may reference it (spec 02 §10).
    pub page_kind: PageKind,
}

impl IndexTreeParams {
    /// Seal tables (spec 04) and every other generic key/value index.
    pub fn generic(leaf_target: usize) -> Self {
        IndexTreeParams {
            leaf_target,
            leaf_kind: BnpgKind::GenericKeyValue,
            page_kind: PageKind::GenericKeyValue,
        }
    }
}

/// Append one page to the body region and return its [`PageAddress`]. Pages
/// are stored uncompressed, 8-byte aligned and digest-authenticated.
pub fn place_page(
    body: &mut Vec<u8>,
    page: IndexPage,
    page_kind: PageKind,
) -> WireResult<PageAddress> {
    let raw = page.encode();
    if raw.len() > MAX_RAW_PAGE {
        return Err(WireError::LimitExceeded(format!(
            "page {} bytes exceeds {MAX_RAW_PAGE}",
            raw.len()
        )));
    }
    if page.entry_count() > MAX_PAGE_ENTRIES {
        return Err(WireError::LimitExceeded(format!(
            "page entry count {} exceeds {MAX_PAGE_ENTRIES}",
            page.entry_count()
        )));
    }
    while !(HEADER_LEN + body.len()).is_multiple_of(8) {
        body.push(0);
    }
    let addr = PageAddress {
        offset: (HEADER_LEN + body.len()) as u64,
        stored_len: raw.len() as u32,
        raw_len: raw.len() as u32,
        codec: super::container::Codec::None,
        page_kind,
        level: page.level,
        entry_count: page.entry_count(),
        stored_digest: Sha256::digest(&raw).into(),
    };
    body.extend_from_slice(&raw);
    Ok(addr)
}

fn flush_leaf(
    entries: &mut Vec<LeafEntry>,
    children: &mut Vec<(Vec<u8>, Vec<u8>, ChildRef)>,
    body: &mut Vec<u8>,
    params: &IndexTreeParams,
) -> WireResult<()> {
    let first = entries.first().unwrap().key.clone();
    let last = entries.last().unwrap().key.clone();
    let page = IndexPage {
        kind: params.leaf_kind,
        level: 0,
        body: PageBody::Leaf(std::mem::take(entries)),
    };
    let addr = place_page(body, page, params.page_kind)?;
    children.push((first, last, ChildRef::Local(addr)));
    Ok(())
}

fn flush_internal(
    entries: &mut Vec<InternalEntry>,
    first: &mut Option<Vec<u8>>,
    last: Vec<u8>,
    level: u8,
    children: &mut Vec<(Vec<u8>, Vec<u8>, ChildRef)>,
    body: &mut Vec<u8>,
    params: &IndexTreeParams,
) -> WireResult<()> {
    let first = first.take().unwrap();
    let page = IndexPage {
        kind: params.leaf_kind,
        level,
        body: PageBody::Internal(std::mem::take(entries)),
    };
    let addr = place_page(body, page, params.page_kind)?;
    children.push((first, last, ChildRef::Local(addr)));
    Ok(())
}

/// Build a BNPG tree over `entries` (non-empty, key-sorted, unique keys) and
/// return the root child. Leaf pages are chunked to `leaf_target` encoded
/// bytes; internal levels are built bottom-up until a single root remains.
pub fn build_index_tree(
    entries: &[(Vec<u8>, Vec<u8>)],
    params: &IndexTreeParams,
    body: &mut Vec<u8>,
) -> WireResult<ChildRef> {
    let mut children: Vec<(Vec<u8>, Vec<u8>, ChildRef)> = Vec::new();
    let mut current: Vec<LeafEntry> = Vec::new();
    let mut current_est = 0usize;
    for (key, value) in entries {
        // Upper bound on the encoded record: varint lengths + payload.
        let est = 10 + key.len() + value.len();
        if !current.is_empty() && current_est + est > params.leaf_target {
            flush_leaf(&mut current, &mut children, body, params)?;
            current_est = 0;
        }
        current.push(LeafEntry {
            key: key.clone(),
            value: value.clone(),
        });
        current_est += est;
    }
    if !current.is_empty() {
        flush_leaf(&mut current, &mut children, body, params)?;
    }

    let mut level: u8 = 1;
    while children.len() > 1 {
        if level > MAX_INDEX_LEVEL {
            return Err(WireError::LimitExceeded(format!(
                "index height exceeds {MAX_INDEX_LEVEL}"
            )));
        }
        let mut next: Vec<(Vec<u8>, Vec<u8>, ChildRef)> = Vec::new();
        let mut current_internal: Vec<InternalEntry> = Vec::new();
        let mut current_est = 0usize;
        let mut first_key: Option<Vec<u8>> = None;
        let mut last_key: Vec<u8> = Vec::new();
        for (min_key, max_key, child) in std::mem::take(&mut children) {
            // Upper bound: two length-prefixed keys + child ref (Local = 57).
            let est = 2 * (10 + min_key.len().max(max_key.len())) + 72;
            // At least two entries per internal page so each level strictly
            // reduces the page count (a target smaller than one entry must
            // not stall the tree at constant width).
            if current_internal.len() >= 2 && current_est + est > params.leaf_target {
                flush_internal(
                    &mut current_internal,
                    &mut first_key,
                    std::mem::take(&mut last_key),
                    level,
                    &mut next,
                    body,
                    params,
                )?;
                current_est = 0;
            }
            if first_key.is_none() {
                first_key = Some(min_key.clone());
            }
            last_key = max_key.clone();
            current_internal.push(InternalEntry {
                min_key,
                max_key,
                child,
            });
            current_est += est;
        }
        if !current_internal.is_empty() {
            flush_internal(
                &mut current_internal,
                &mut first_key,
                last_key,
                level,
                &mut next,
                body,
                params,
            )?;
        }
        children = next;
        level += 1;
    }
    Ok(children.pop().expect("non-empty tree").2)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entries(n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..n)
            .map(|i| {
                (
                    (i as u32).to_be_bytes().to_vec(),
                    vec![b'v'; 40 + (i % 7)],
                )
            })
            .collect()
    }

    #[test]
    fn single_leaf_and_multi_level_shapes() {
        let mut body = Vec::new();
        let root = build_index_tree(&entries(1), &IndexTreeParams::generic(16 * 1024), &mut body)
            .unwrap();
        match root {
            ChildRef::Local(addr) => {
                assert_eq!(addr.level, 0);
                assert_eq!(addr.entry_count, 1);
                assert_eq!(addr.page_kind, PageKind::GenericKeyValue);
                assert_eq!(addr.offset, 64);
            }
            _ => panic!("local root expected"),
        }

        // A tiny target forces several internal levels.
        let mut body = Vec::new();
        let root =
            build_index_tree(&entries(200), &IndexTreeParams::generic(96), &mut body).unwrap();
        match root {
            ChildRef::Local(addr) => {
                assert!(addr.level >= 2, "expected a multi-level tree");
                // PageAddress offsets are object-relative (header included),
                // so the stored range must land inside header + body.
                assert!(addr.offset + addr.stored_len as u64 <= HEADER_LEN as u64 + body.len() as u64);
            }
            _ => panic!("local root expected"),
        }
    }

    #[test]
    fn every_reachable_page_is_aligned_and_digest_authenticated() {
        let mut body = Vec::new();
        let params = IndexTreeParams::generic(128);
        let root = build_index_tree(&entries(64), &params, &mut body).unwrap();

        // Walk the tree the way a reader does: every node's declared page
        // range must be 8-aligned, in bounds, and match its stored digest.
        fn walk(
            child: &ChildRef,
            body: &[u8],
            pages: &mut usize,
            levels: &mut u8,
        ) -> Vec<ChildRef> {
            let ChildRef::Local(addr) = child else {
                panic!("the builder only produces local children")
            };
            assert_eq!(addr.offset % 8, 0, "page offset must be 8-aligned");
            let start = addr.offset as usize - HEADER_LEN;
            let raw = &body[start..start + addr.stored_len as usize];
            let digest: [u8; 32] = Sha256::digest(raw).into();
            assert_eq!(digest, addr.stored_digest);
            let page = IndexPage::decode(raw).expect("valid page");
            assert_eq!(page.level, addr.level);
            assert_eq!(page.entry_count(), addr.entry_count);
            *pages += 1;
            *levels = (*levels).max(addr.level);
            match page.body {
                PageBody::Leaf(entries) => {
                    assert!(!entries.is_empty());
                    Vec::new()
                }
                PageBody::Internal(entries) => {
                    entries.iter().map(|e| e.child.clone()).collect()
                }
            }
        }

        let mut pages = 0;
        let mut levels = 0;
        let mut stack = vec![root];
        while let Some(child) = stack.pop() {
            stack.extend(walk(&child, &body, &mut pages, &mut levels));
        }
        assert!(pages >= 2, "expected a multi-page tree, got {pages}");
        assert!(levels >= 1, "expected at least one internal level");
    }

    #[test]
    fn oversized_page_is_rejected_by_the_leaf_target_cap() {
        // One entry larger than the hard page cap must fail rather than
        // produce an unreadable page.
        let big = vec![(b"k".to_vec(), vec![0u8; MAX_RAW_PAGE + 1])];
        let mut body = Vec::new();
        assert!(
            build_index_tree(&big, &IndexTreeParams::generic(16 * 1024), &mut body).is_err()
        );
    }
}
