//! Build-time directory folding and the bounded read-side merge primitive.
//!
//! A clustered snapshot may contain the same ancestor directory in many
//! clusters.  Those physical skeletons are useful for self-description, but
//! they must not become one runtime contributor per cluster.  This module
//! folds the skeletons first, then plans disjoint raw-name windows.  The
//! reader-side helper only merges the sources referenced by one window and
//! therefore has a fixed fan-out and output bound.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};
use std::fmt;

use super::directory::{
    DirectoryEntry, DirectoryIdentity, DirectoryPage, MAX_RANGE_SOURCES, MAX_WINDOW_ENTRIES,
    NodeRef, RangeRouteError, RangeWindow, ReadDirLimit, WindowSource, WindowSourceKind,
};
use super::identity::DirKey;
use super::name::NameBytes;

/// One physical directory contribution before canonical folding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryContribution {
    pub dir_key: DirKey,
    pub node: NodeRef,
    /// Digest of the complete directory hot/cold attributes.  The producer
    /// computes this over the canonical encoded attributes, so equality is a
    /// cheap validation here and does not require carrying a large value.
    pub attributes_digest: [u8; 32],
    pub entries: Vec<ContributionEntry>,
}

/// One raw-name entry in a physical contribution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContributionEntry {
    pub name: NameBytes,
    pub inode: u64,
    pub kind: u8,
    /// Directory entries carry the derived child `DirKey`; regular entries
    /// must leave it unset.
    pub child_dir_key: Option<DirKey>,
    /// Digest of the child inode's directory attributes.  It is compared when
    /// duplicate directory skeletons are folded.
    pub attributes_digest: [u8; 32],
}

impl ContributionEntry {
    pub fn is_directory(&self) -> bool {
        self.kind == 2
    }
}

/// A canonical visible entry after physical directory skeletons have been
/// folded.  `source_id` identifies the physical file source for non-directory
/// entries; directory entries use the canonical projection source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalEntry {
    pub entry: DirectoryEntry,
    pub source_id: u32,
    pub child_dir_key: Option<DirKey>,
    pub attributes_digest: [u8; 32],
}

/// Folded logical directory and the source streams used to build its route.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalDirectory {
    pub dir_key: DirKey,
    pub canonical_node: NodeRef,
    pub attributes_digest: [u8; 32],
    pub entries: Vec<CanonicalEntry>,
    pub sources: Vec<SourceEntries>,
}

/// Sorted entries from one physical source.  This is a producer/reference
/// representation; an on-disk route replaces the vector with a batch locator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceEntries {
    pub source_id: u32,
    pub kind: WindowSourceKind,
    /// Physical contributor that owns this source stream.  The planner-local
    /// source id is not enough to address a cluster after publication.
    pub contributor: NodeRef,
    pub entries: Vec<CanonicalEntry>,
}

impl SourceEntries {
    fn validate(&self) -> Result<(), MergeError> {
        if self.entries.is_empty() {
            return Err(MergeError::EmptySource {
                source_id: self.source_id,
            });
        }
        for pair in self.entries.windows(2) {
            if pair[0].entry.name >= pair[1].entry.name {
                return Err(MergeError::SourceNotSorted {
                    source_id: self.source_id,
                });
            }
        }
        Ok(())
    }
}

/// An authenticated route plus its reference source streams.  The source
/// vectors are intentionally kept in this build/test representation; the
/// production format stores an object offset and bounded length instead.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannedDirectory {
    pub identity: DirectoryIdentity,
    pub canonical_node: NodeRef,
    pub visible_entry_count: u64,
    pub windows: Vec<RangeWindow>,
    pub sources: Vec<SourceEntries>,
}

impl PlannedDirectory {
    /// Resolve a stable child ordinal without retaining a cookie map.  Only
    /// windows intersecting the requested page are merged.
    pub fn read_page(
        &self,
        next_ordinal: u64,
        limit: ReadDirLimit,
    ) -> Result<DirectoryPage, MergeError> {
        let limit = limit.validate().map_err(MergeError::Route)?;
        if next_ordinal > self.visible_entry_count {
            return Err(MergeError::OrdinalOutOfRange {
                ordinal: next_ordinal,
                visible: self.visible_entry_count,
            });
        }
        if next_ordinal == self.visible_entry_count {
            return Ok(DirectoryPage {
                identity: self.identity,
                next_ordinal,
                entries: Vec::new(),
                end: true,
            });
        }

        let mut before = 0u64;
        let mut output = Vec::with_capacity(limit.max_entries.min(MAX_WINDOW_ENTRIES));
        let mut owned_bytes = 0usize;
        for window in &self.windows {
            let count = u64::from(window.visible_entry_count);
            if next_ordinal >= before.saturating_add(count) {
                before = before.saturating_add(count);
                continue;
            }

            let skip = usize::try_from(next_ordinal.saturating_sub(before)).map_err(|_| {
                MergeError::OrdinalOutOfRange {
                    ordinal: next_ordinal,
                    visible: self.visible_entry_count,
                }
            })?;
            let remaining = limit.max_entries.saturating_sub(output.len());
            if remaining == 0 || owned_bytes >= limit.max_owned_bytes {
                break;
            }
            let page_limit = ReadDirLimit {
                max_entries: remaining,
                max_owned_bytes: limit.max_owned_bytes.saturating_sub(owned_bytes),
            };
            let page = merge_window_page(window, &self.sources, skip, remaining, page_limit)?;
            owned_bytes = owned_bytes.saturating_add(
                page.entries
                    .iter()
                    .map(|entry| page_limit.owned_bytes(entry))
                    .sum::<usize>(),
            );
            let budget_exhausted = page.budget_exhausted;
            output.extend(page.entries);
            before = before.saturating_add(count);
            if output.len() >= limit.max_entries || owned_bytes >= limit.max_owned_bytes {
                break;
            }
            // A short page caused by the owned-byte limit is a complete
            // response. Continuing into the next range would spend the same
            // request's remaining budget on later names and can turn a valid
            // short page into PageTooLarge for an unrelated entry.
            if budget_exhausted {
                break;
            }
        }

        let next = next_ordinal.saturating_add(output.len() as u64);
        Ok(DirectoryPage {
            identity: self.identity,
            next_ordinal: next,
            end: next >= self.visible_entry_count,
            entries: output,
        })
    }
}

/// Fold all physical contributions for one `DirKey` and plan its raw-name
/// route.  Directory skeletons become one canonical source; file entries
/// remain attached to their source cluster.
pub fn build_directory_plan(
    identity: DirectoryIdentity,
    contributions: Vec<DirectoryContribution>,
) -> Result<PlannedDirectory, MergeError> {
    let canonical = fold_directory_contributions(contributions)?;
    let mut windows = plan_windows(&canonical.sources)?;
    // The planner is also the producer-side proof that the route covers the
    // complete logical name space.  An empty directory has no windows but is
    // still represented by visible_entry_count=0 in the view.
    let visible_entry_count = canonical.entries.len() as u64;
    let route_count = windows
        .iter()
        .map(|window| u64::from(window.visible_entry_count))
        .sum::<u64>();
    if route_count != visible_entry_count {
        return Err(MergeError::RouteCoverage {
            expected: visible_entry_count,
            actual: route_count,
        });
    }
    // Keep route windows deterministic even if the caller supplied sources in
    // a different physical order.
    windows.sort_by(|left, right| left.lower.cmp(&right.lower));
    Ok(PlannedDirectory {
        identity,
        canonical_node: canonical.canonical_node,
        visible_entry_count,
        windows,
        sources: canonical.sources,
    })
}

/// Validate and fold physical contributions into one logical directory.
pub fn fold_directory_contributions(
    mut contributions: Vec<DirectoryContribution>,
) -> Result<CanonicalDirectory, MergeError> {
    if contributions.is_empty() {
        return Err(MergeError::NoContributions);
    }
    contributions.sort_by_key(|contribution| contribution.node);
    let dir_key = contributions[0].dir_key;
    let attributes_digest = contributions[0].attributes_digest;
    for contribution in &contributions {
        if contribution.dir_key != dir_key {
            return Err(MergeError::DirectoryKeyMismatch);
        }
        if contribution.attributes_digest != attributes_digest {
            return Err(MergeError::DirectoryAttributesMismatch);
        }
        validate_contribution(contribution)?;
    }
    let canonical_node = contributions[0].node;

    let mut by_name = BTreeMap::<NameBytes, Vec<(usize, ContributionEntry)>>::new();
    for (index, contribution) in contributions.iter().enumerate() {
        for entry in &contribution.entries {
            by_name
                .entry(entry.name.clone())
                .or_default()
                .push((index, entry.clone()));
        }
    }

    let mut entries = Vec::with_capacity(by_name.len());
    for (name, candidates) in by_name {
        let first = &candidates[0].1;
        if first.is_directory() {
            let Some(child_dir_key) = first.child_dir_key else {
                return Err(MergeError::MissingChildDirectoryKey { name });
            };
            for (_, candidate) in &candidates {
                if !candidate.is_directory()
                    || candidate.child_dir_key != Some(child_dir_key)
                    || candidate.attributes_digest != first.attributes_digest
                {
                    return Err(MergeError::NameCollision { name });
                }
            }
            let (_index, selected) = candidates
                .iter()
                .min_by_key(|(index, candidate)| (contributions[*index].node, candidate.inode))
                .expect("directory candidate list is non-empty");
            entries.push(CanonicalEntry {
                entry: DirectoryEntry {
                    name,
                    inode: selected.inode,
                    kind: selected.kind,
                },
                source_id: u32::MAX,
                child_dir_key: Some(child_dir_key),
                attributes_digest: selected.attributes_digest,
            });
        } else {
            if candidates.len() != 1 || candidates[0].1.child_dir_key.is_some() {
                return Err(MergeError::NameCollision { name });
            }
            let (index, selected) = &candidates[0];
            entries.push(CanonicalEntry {
                entry: DirectoryEntry {
                    name,
                    inode: selected.inode,
                    kind: selected.kind,
                },
                source_id: contributions[*index].node.cluster_slot,
                child_dir_key: None,
                attributes_digest: selected.attributes_digest,
            });
        }
    }

    let mut grouped = BTreeMap::<(u32, WindowSourceKind), Vec<CanonicalEntry>>::new();
    for entry in &entries {
        let kind = if entry.entry.kind == 2 {
            WindowSourceKind::CanonicalDirectoryEntries
        } else {
            WindowSourceKind::FileEntries
        };
        grouped
            .entry((entry.source_id, kind))
            .or_default()
            .push(entry.clone());
    }
    let sources = grouped
        .into_iter()
        .map(|((source_id, kind), entries)| {
            let contributor = if kind == WindowSourceKind::CanonicalDirectoryEntries {
                canonical_node
            } else {
                contributions
                    .iter()
                    .find(|contribution| contribution.node.cluster_slot == source_id)
                    .map(|contribution| contribution.node)
                    .ok_or(MergeError::MissingSource { source_id })?
            };
            Ok(SourceEntries {
                source_id,
                kind,
                contributor,
                entries,
            })
        })
        .collect::<Result<Vec<_>, MergeError>>()?;

    Ok(CanonicalDirectory {
        dir_key,
        canonical_node,
        attributes_digest,
        entries,
        sources,
    })
}

fn validate_contribution(contribution: &DirectoryContribution) -> Result<(), MergeError> {
    for pair in contribution.entries.windows(2) {
        if pair[0].name >= pair[1].name {
            return Err(MergeError::ContributionNotSorted {
                source_id: contribution.node.cluster_slot,
            });
        }
    }
    for entry in &contribution.entries {
        if entry.is_directory() != entry.child_dir_key.is_some() {
            return Err(MergeError::InvalidEntry {
                source_id: contribution.node.cluster_slot,
                name: entry.name.clone(),
            });
        }
    }
    Ok(())
}

fn plan_windows(sources: &[SourceEntries]) -> Result<Vec<RangeWindow>, MergeError> {
    for source in sources {
        source.validate()?;
    }
    if sources.is_empty() {
        return Ok(Vec::new());
    }
    let mut names = BTreeMap::<NameBytes, ()>::new();
    for source in sources {
        for entry in &source.entries {
            names.insert(entry.entry.name.clone(), ());
        }
    }
    let names = names.into_keys().collect::<Vec<_>>();
    let mut windows = Vec::new();
    let mut start = 0usize;
    while start < names.len() {
        // Start with the largest legal window, then move its upper boundary
        // left until the source fan-out is admissible.  This keeps a source
        // that ends early from unnecessarily poisoning later windows.
        let mut end = (start + MAX_WINDOW_ENTRIES).min(names.len());
        let (lower, window_sources) = loop {
            let lower = names[start].clone();
            let upper = names.get(end).cloned();
            let window_sources = route_sources(sources, Some(&lower), upper.as_ref());
            if window_sources.len() <= MAX_RANGE_SOURCES {
                break (lower, (upper, window_sources));
            }
            if end == start + 1 {
                return Err(MergeError::TooManySources {
                    count: window_sources.len(),
                });
            }
            end -= 1;
        };
        let (upper, window_sources) = window_sources;
        // The planner uses concrete names to select source slices, but the
        // persisted route covers the complete byte-name space.  Therefore
        // only interior windows carry finite bounds.
        let route_lower = (start != 0).then_some(lower);
        let route_upper = (end != names.len()).then_some(upper).flatten();
        windows.push(
            RangeWindow::new(
                route_lower,
                route_upper,
                window_sources,
                (end - start) as u32,
            )
            .map_err(MergeError::Route)?,
        );
        start = end;
    }
    Ok(windows)
}

fn route_sources(
    sources: &[SourceEntries],
    lower: Option<&NameBytes>,
    upper: Option<&NameBytes>,
) -> Vec<WindowSource> {
    let mut window_sources = Vec::new();
    for source in sources {
        let selected = source
            .entries
            .iter()
            .filter(|entry| {
                lower.is_none_or(|bound| entry.entry.name >= *bound)
                    && upper.is_none_or(|bound| entry.entry.name < *bound)
            })
            .collect::<Vec<_>>();
        if selected.is_empty() {
            continue;
        }
        let first_name = selected[0].entry.name.clone();
        let last_name = selected
            .last()
            .expect("selected source is non-empty")
            .entry
            .name
            .clone();
        window_sources.push(WindowSource {
            source_id: source.source_id,
            kind: source.kind,
            first_name,
            last_name,
            entry_count: selected.len() as u32,
        });
    }
    window_sources
}

#[derive(Clone, Eq, PartialEq)]
struct HeapItem {
    name: NameBytes,
    source: usize,
    position: usize,
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max heap; invert the name ordering for a min heap,
        // then use source/position as deterministic tie breakers.
        other
            .name
            .cmp(&self.name)
            .then_with(|| other.source.cmp(&self.source))
            .then_with(|| other.position.cmp(&self.position))
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct MergedWindowPage {
    entries: Vec<DirectoryEntry>,
    budget_exhausted: bool,
}

fn merge_window_page(
    window: &RangeWindow,
    sources: &[SourceEntries],
    skip: usize,
    limit: usize,
    byte_limit: ReadDirLimit,
) -> Result<MergedWindowPage, MergeError> {
    if window.sources.len() > MAX_RANGE_SOURCES {
        return Err(MergeError::TooManySources {
            count: window.sources.len(),
        });
    }
    let mut heap = BinaryHeap::new();
    let mut source_indices = Vec::new();
    for route_source in &window.sources {
        let Some((source_index, source)) = sources.iter().enumerate().find(|(_, source)| {
            source.source_id == route_source.source_id && source.kind == route_source.kind
        }) else {
            return Err(MergeError::MissingSource {
                source_id: route_source.source_id,
            });
        };
        source.validate()?;
        let start = source
            .entries
            .partition_point(|entry| entry.entry.name < route_source.first_name);
        let end = source
            .entries
            .partition_point(|entry| entry.entry.name <= route_source.last_name);
        if end <= start || end - start != route_source.entry_count as usize {
            return Err(MergeError::RouteSourceMismatch {
                source_id: route_source.source_id,
            });
        }
        source_indices.push((source_index, start, end));
        heap.push(HeapItem {
            name: source.entries[start].entry.name.clone(),
            source: source_index,
            position: start,
        });
    }

    let mut output = Vec::new();
    let mut output_bytes = 0usize;
    let mut logical_index = 0usize;
    let mut budget_exhausted = false;
    while let Some(item) = heap.pop() {
        let name = item.name.clone();
        let mut same = vec![item];
        while heap.peek().is_some_and(|next| next.name == name) {
            same.push(heap.pop().expect("peeked heap item"));
        }
        for item in &same {
            if let Some((_, _, end)) = source_indices
                .iter()
                .find(|(source_index, _, _)| *source_index == item.source)
            {
                if item.position + 1 < *end {
                    let source = &sources[item.source];
                    heap.push(HeapItem {
                        name: source.entries[item.position + 1].entry.name.clone(),
                        source: item.source,
                        position: item.position + 1,
                    });
                }
            }
        }

        let candidates = same
            .iter()
            .map(|item| sources[item.source].entries[item.position].clone())
            .collect::<Vec<_>>();
        let selected = merge_candidates(&name, candidates)?;
        if logical_index >= skip && output.len() < limit {
            let entry_bytes = byte_limit.owned_bytes(&selected.entry);
            if entry_bytes > byte_limit.max_owned_bytes {
                if output.is_empty() {
                    return Err(MergeError::Route(RangeRouteError::PageTooLarge));
                }
                budget_exhausted = true;
                break;
            }
            if output_bytes.saturating_add(entry_bytes) > byte_limit.max_owned_bytes {
                budget_exhausted = true;
                break;
            }
            output_bytes = output_bytes.saturating_add(entry_bytes);
            output.push(selected.entry);
        }
        logical_index += 1;
        if output.len() >= limit {
            break;
        }
    }
    Ok(MergedWindowPage {
        entries: output,
        budget_exhausted,
    })
}

fn merge_candidates(
    name: &NameBytes,
    candidates: Vec<CanonicalEntry>,
) -> Result<CanonicalEntry, MergeError> {
    let first = candidates
        .first()
        .ok_or_else(|| MergeError::NameCollision { name: name.clone() })?;
    if first.entry.kind == 2 {
        let Some(child_dir_key) = first.child_dir_key else {
            return Err(MergeError::MissingChildDirectoryKey { name: name.clone() });
        };
        if candidates.iter().any(|candidate| {
            candidate.entry.kind != 2
                || candidate.child_dir_key != Some(child_dir_key)
                || candidate.attributes_digest != first.attributes_digest
        }) {
            return Err(MergeError::NameCollision { name: name.clone() });
        }
        return Ok(candidates
            .into_iter()
            .min_by_key(|candidate| (candidate.source_id, candidate.entry.inode))
            .expect("candidate list is non-empty"));
    }
    if candidates.len() != 1 {
        return Err(MergeError::NameCollision { name: name.clone() });
    }
    Ok(first.clone())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MergeError {
    NoContributions,
    DirectoryKeyMismatch,
    DirectoryAttributesMismatch,
    ContributionNotSorted { source_id: u32 },
    SourceNotSorted { source_id: u32 },
    InvalidEntry { source_id: u32, name: NameBytes },
    MissingChildDirectoryKey { name: NameBytes },
    NameCollision { name: NameBytes },
    EmptySource { source_id: u32 },
    TooManySources { count: usize },
    MissingSource { source_id: u32 },
    RouteSourceMismatch { source_id: u32 },
    RouteCoverage { expected: u64, actual: u64 },
    OrdinalOutOfRange { ordinal: u64, visible: u64 },
    Route(RangeRouteError),
}

impl fmt::Display for MergeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoContributions => f.write_str("directory has no physical contributions"),
            Self::DirectoryKeyMismatch => {
                f.write_str("directory contributions have different DirKey")
            }
            Self::DirectoryAttributesMismatch => {
                f.write_str("directory contribution attributes differ")
            }
            Self::ContributionNotSorted { source_id } => {
                write!(f, "contribution {source_id} is not raw-name sorted")
            }
            Self::SourceNotSorted { source_id } => {
                write!(f, "route source {source_id} is not raw-name sorted")
            }
            Self::InvalidEntry { source_id, name } => write!(
                f,
                "source {source_id} has invalid entry {:?}",
                name.as_bytes()
            ),
            Self::MissingChildDirectoryKey { name } => write!(
                f,
                "directory entry {:?} has no child DirKey",
                name.as_bytes()
            ),
            Self::NameCollision { name } => {
                write!(f, "committed name collision at {:?}", name.as_bytes())
            }
            Self::EmptySource { source_id } => write!(f, "route source {source_id} is empty"),
            Self::TooManySources { count } => write!(
                f,
                "route window has {count} sources, maximum is {MAX_RANGE_SOURCES}"
            ),
            Self::MissingSource { source_id } => {
                write!(f, "route references missing source {source_id}")
            }
            Self::RouteSourceMismatch { source_id } => {
                write!(f, "route source {source_id} does not match its locator")
            }
            Self::RouteCoverage { expected, actual } => {
                write!(f, "route covers {actual} names, expected {expected}")
            }
            Self::OrdinalOutOfRange { ordinal, visible } => {
                write!(f, "directory ordinal {ordinal} exceeds {visible}")
            }
            Self::Route(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for MergeError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(bytes: &[u8]) -> NameBytes {
        NameBytes::new(bytes.to_vec()).unwrap()
    }

    fn entry(bytes: &[u8], inode: u64, kind: u8, child: Option<DirKey>) -> ContributionEntry {
        ContributionEntry {
            name: name(bytes),
            inode,
            kind,
            child_dir_key: child,
            attributes_digest: [kind; 32],
        }
    }

    fn contribution(slot: u32, entries: Vec<ContributionEntry>) -> DirectoryContribution {
        DirectoryContribution {
            dir_key: DirKey::new([7; 16]),
            node: NodeRef {
                cluster_slot: slot,
                local_node_id: 1,
            },
            attributes_digest: [3; 32],
            entries,
        }
    }

    #[test]
    fn duplicate_directory_skeletons_fold_to_one_canonical_entry() {
        let folded = fold_directory_contributions(vec![
            contribution(2, vec![entry(b"child", 20, 2, Some(DirKey::new([9; 16])))]),
            contribution(1, vec![entry(b"child", 10, 2, Some(DirKey::new([9; 16])))]),
        ])
        .unwrap();
        assert_eq!(folded.canonical_node.cluster_slot, 1);
        assert_eq!(folded.entries.len(), 1);
        assert_eq!(folded.entries[0].entry.inode, 10);
        assert_eq!(folded.entries[0].source_id, u32::MAX);
        assert_eq!(
            folded.sources[0].kind,
            WindowSourceKind::CanonicalDirectoryEntries
        );
    }

    #[test]
    fn file_file_and_file_directory_collisions_are_rejected() {
        assert!(matches!(
            fold_directory_contributions(vec![
                contribution(1, vec![entry(b"same", 1, 1, None)]),
                contribution(2, vec![entry(b"same", 2, 1, None)]),
            ]),
            Err(MergeError::NameCollision { .. })
        ));
        assert!(matches!(
            fold_directory_contributions(vec![
                contribution(1, vec![entry(b"same", 1, 1, None)]),
                contribution(2, vec![entry(b"same", 2, 2, Some(DirKey::new([1; 16])))]),
            ]),
            Err(MergeError::NameCollision { .. })
        ));
    }

    #[test]
    fn planner_splits_large_raw_name_space_and_replays_ordinals() {
        let entries = (0..(MAX_WINDOW_ENTRIES * 2 + 7))
            .map(|index| {
                entry(
                    format!("f{index:05}").as_bytes(),
                    index as u64 + 10,
                    1,
                    None,
                )
            })
            .collect::<Vec<_>>();
        let plan = build_directory_plan(
            DirectoryIdentity {
                snapshot: [1; 32],
                dir_key: [7; 16],
            },
            vec![contribution(4, entries)],
        )
        .unwrap();
        assert_eq!(plan.windows.len(), 3);
        assert_eq!(
            plan.visible_entry_count,
            (MAX_WINDOW_ENTRIES * 2 + 7) as u64
        );
        let first = plan
            .read_page(
                0,
                ReadDirLimit {
                    max_entries: 17,
                    max_owned_bytes: 64 * 1024,
                },
            )
            .unwrap();
        assert_eq!(first.entries.len(), 17);
        let replay = plan
            .read_page(
                first.next_ordinal,
                ReadDirLimit {
                    max_entries: 17,
                    max_owned_bytes: 64 * 1024,
                },
            )
            .unwrap();
        assert_eq!(replay.entries[0].name.as_bytes(), b"f00017");
        let tail = plan
            .read_page(
                plan.visible_entry_count - 3,
                ReadDirLimit {
                    max_entries: 17,
                    max_owned_bytes: 64 * 1024,
                },
            )
            .unwrap();
        assert!(tail.end);
        assert_eq!(tail.entries.len(), 3);

        let one_entry = std::mem::size_of::<DirectoryEntry>() + 6;
        let short = plan
            .read_page(
                0,
                ReadDirLimit {
                    max_entries: 256,
                    max_owned_bytes: one_entry * 2 - 1,
                },
            )
            .unwrap();
        assert_eq!(short.entries.len(), 1);
        assert_eq!(short.next_ordinal, 1);
        assert!(!short.end);
    }

    #[test]
    fn too_many_overlapping_sources_are_rejected_before_runtime_merge() {
        let contributions = (0..=MAX_RANGE_SOURCES as u32)
            .map(|slot| contribution(slot, vec![entry(b"name", slot as u64, 1, None)]))
            .collect::<Vec<_>>();
        assert!(matches!(
            fold_directory_contributions(contributions),
            Err(MergeError::NameCollision { .. })
        ));
    }

    #[test]
    fn planner_moves_boundaries_to_keep_source_fanout_bounded() {
        let contributions = (0..(MAX_RANGE_SOURCES as u32 + 1))
            .map(|slot| {
                let name = [b'a' + slot as u8];
                contribution(slot, vec![entry(&name, slot as u64 + 1, 1, None)])
            })
            .collect::<Vec<_>>();
        let plan = build_directory_plan(
            DirectoryIdentity {
                snapshot: [4; 32],
                dir_key: [7; 16],
            },
            contributions,
        )
        .unwrap();
        assert_eq!(plan.visible_entry_count, (MAX_RANGE_SOURCES + 1) as u64);
        assert!(plan.windows.iter().all(|window| {
            window.sources.len() <= MAX_RANGE_SOURCES
                && window.visible_entry_count <= MAX_WINDOW_ENTRIES as u32
        }));
    }
}
