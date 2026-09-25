use super::identity::DirKey;
use super::name::NameBytes;
use std::fmt;

pub const MAX_RANGE_SOURCES: usize = 8;
pub const MAX_WINDOW_ENTRIES: usize = 4096;

/// Snapshot-local identity of the canonical contributor for a directory.
/// The pair is a locator, not a process-local pointer.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodeRef {
    pub cluster_slot: u32,
    pub local_node_id: u32,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct DirectoryIdentity {
    pub snapshot: [u8; 32],
    pub dir_key: [u8; 16],
}

impl DirectoryIdentity {
    pub const fn from_dir_key(snapshot: [u8; 32], dir_key: DirKey) -> Self {
        Self {
            snapshot,
            dir_key: dir_key.into_bytes(),
        }
    }

    pub const fn dir_key(&self) -> DirKey {
        DirKey::new(self.dir_key)
    }
}

/// Authenticated logical directory metadata. Loading a view does not imply
/// that any entry batch is resident in memory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryView {
    pub identity: DirectoryIdentity,
    pub canonical_node: NodeRef,
    pub visible_entry_count: u64,
    pub entry_index_root: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryEntry {
    pub name: NameBytes,
    pub inode: u64,
    pub kind: u8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadDirLimit {
    pub max_entries: usize,
    pub max_owned_bytes: usize,
}

impl ReadDirLimit {
    pub const DEFAULT: Self = Self {
        max_entries: 256,
        max_owned_bytes: 1024 * 1024,
    };

    pub fn validate(self) -> Result<Self, RangeRouteError> {
        if self.max_entries == 0 || self.max_entries > MAX_WINDOW_ENTRIES {
            return Err(RangeRouteError::InvalidLimit {
                max_entries: self.max_entries,
            });
        }
        if self.max_owned_bytes == 0 {
            return Err(RangeRouteError::InvalidByteBudget);
        }
        Ok(self)
    }

    pub fn fits(&self, entry: &DirectoryEntry) -> bool {
        // Keep the accounting conservative.  The output page owns the name
        // bytes and fixed scalar fields; container capacity is charged by the
        // caller's reservation separately.
        self.owned_bytes(entry) <= self.max_owned_bytes
    }

    pub fn owned_bytes(&self, entry: &DirectoryEntry) -> usize {
        entry
            .name
            .as_bytes()
            .len()
            .saturating_add(std::mem::size_of::<DirectoryEntry>())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryPage {
    pub identity: DirectoryIdentity,
    /// Stable ordinal of the next logical child.  `.` and `..` occupy ordinals
    /// 0 and 1 in the FUSE adapter; this page is the child portion.
    pub next_ordinal: u64,
    pub entries: Vec<DirectoryEntry>,
    pub end: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadDirCursor {
    pub identity: DirectoryIdentity,
    /// The next child ordinal to emit.  It is recomputed from the immutable
    /// range index after an eviction; it is never a pointer into a batch.
    pub next_ordinal: u64,
}

impl ReadDirCursor {
    pub const fn start(identity: DirectoryIdentity) -> Self {
        Self {
            identity,
            next_ordinal: 0,
        }
    }

    pub fn advance(&mut self, page: &DirectoryPage) -> Result<(), RangeRouteError> {
        if page.identity != self.identity || page.next_ordinal < self.next_ordinal {
            return Err(RangeRouteError::CursorMismatch);
        }
        self.next_ordinal = page.next_ordinal;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeRoute {
    pub lower: Option<NameBytes>,
    pub upper: Option<NameBytes>,
    pub sources: Vec<RangeSource>,
    pub visible_entry_count: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeSource {
    pub source_id: u32,
    pub first_name: NameBytes,
    pub last_name: NameBytes,
    pub entry_count: u32,
}

/// One bounded raw-name window in a directory view. Sources may overlap while
/// a k-way merge validates duplicate names; windows themselves are disjoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeWindow {
    pub lower: Option<NameBytes>,
    pub upper: Option<NameBytes>,
    pub visible_entry_count: u32,
    pub sources: Vec<WindowSource>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum WindowSourceKind {
    FileEntries,
    CanonicalDirectoryEntries,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WindowSource {
    pub source_id: u32,
    pub kind: WindowSourceKind,
    pub first_name: NameBytes,
    pub last_name: NameBytes,
    pub entry_count: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RangeRouteError {
    TooManySources { count: usize, max: usize },
    InvalidWindow,
    InvalidSource,
    TooManyEntries { count: usize, max: usize },
    InvalidLimit { max_entries: usize },
    InvalidByteBudget,
    CursorMismatch,
    PageTooLarge,
}

impl fmt::Display for RangeRouteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManySources { count, max } => {
                write!(f, "range window has {count} sources, maximum is {max}")
            }
            Self::InvalidWindow => f.write_str("range window bounds are invalid"),
            Self::InvalidSource => f.write_str("range window source is invalid"),
            Self::TooManyEntries { count, max } => {
                write!(f, "range window has {count} entries, maximum is {max}")
            }
            Self::InvalidLimit { max_entries } => {
                write!(f, "invalid readdir entry limit {max_entries}")
            }
            Self::InvalidByteBudget => f.write_str("readdir byte budget must be non-zero"),
            Self::CursorMismatch => f.write_str("readdir cursor does not match its page"),
            Self::PageTooLarge => f.write_str("readdir page exceeds its owned-byte budget"),
        }
    }
}

impl std::error::Error for RangeRouteError {}

impl RangeRoute {
    pub fn new(
        lower: Option<NameBytes>,
        upper: Option<NameBytes>,
        sources: Vec<RangeSource>,
        visible_entry_count: u32,
    ) -> Result<Self, RangeRouteError> {
        if sources.len() > MAX_RANGE_SOURCES {
            return Err(RangeRouteError::TooManySources {
                count: sources.len(),
                max: MAX_RANGE_SOURCES,
            });
        }
        if let (Some(lower), Some(upper)) = (&lower, &upper)
            && lower >= upper
        {
            return Err(RangeRouteError::InvalidWindow);
        }
        Ok(Self {
            lower,
            upper,
            sources,
            visible_entry_count,
        })
    }

    pub fn contains(&self, name: &[u8]) -> bool {
        self.lower
            .as_ref()
            .is_none_or(|lower| name >= lower.as_bytes())
            && self
                .upper
                .as_ref()
                .is_none_or(|upper| name < upper.as_bytes())
    }
}

impl RangeWindow {
    pub fn new(
        lower: Option<NameBytes>,
        upper: Option<NameBytes>,
        sources: Vec<WindowSource>,
        visible_entry_count: u32,
    ) -> Result<Self, RangeRouteError> {
        if sources.len() > MAX_RANGE_SOURCES {
            return Err(RangeRouteError::TooManySources {
                count: sources.len(),
                max: MAX_RANGE_SOURCES,
            });
        }
        if visible_entry_count as usize > MAX_WINDOW_ENTRIES {
            return Err(RangeRouteError::TooManyEntries {
                count: visible_entry_count as usize,
                max: MAX_WINDOW_ENTRIES,
            });
        }
        if let (Some(lower), Some(upper)) = (&lower, &upper)
            && lower >= upper
        {
            return Err(RangeRouteError::InvalidWindow);
        }
        for source in &sources {
            if source.entry_count == 0 || source.first_name > source.last_name {
                return Err(RangeRouteError::InvalidSource);
            }
            if lower
                .as_ref()
                .is_some_and(|bound| source.first_name < *bound)
                || upper
                    .as_ref()
                    .is_some_and(|bound| source.last_name >= *bound)
            {
                return Err(RangeRouteError::InvalidSource);
            }
        }
        Ok(Self {
            lower,
            upper,
            visible_entry_count,
            sources,
        })
    }

    pub fn contains(&self, name: &[u8]) -> bool {
        self.lower
            .as_ref()
            .is_none_or(|lower| name >= lower.as_bytes())
            && self
                .upper
                .as_ref()
                .is_none_or(|upper| name < upper.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(value: &[u8]) -> NameBytes {
        NameBytes::new(value.to_vec()).unwrap()
    }

    #[test]
    fn route_enforces_bounded_fanout_and_raw_name_ranges() {
        let route = RangeRoute::new(
            Some(name(b"a")),
            Some(name(b"m")),
            vec![RangeSource {
                source_id: 7,
                first_name: name(b"a"),
                last_name: name(b"l"),
                entry_count: 12,
            }],
            12,
        )
        .unwrap();
        assert!(route.contains(b"a"));
        assert!(route.contains(b"l"));
        assert!(!route.contains(b"m"));

        let sources = (0..=MAX_RANGE_SOURCES)
            .map(|source_id| RangeSource {
                source_id: source_id as u32,
                first_name: name(b"a"),
                last_name: name(b"z"),
                entry_count: 1,
            })
            .collect();
        assert!(matches!(
            RangeRoute::new(None, None, sources, 1),
            Err(RangeRouteError::TooManySources { .. })
        ));
    }

    #[test]
    fn cursor_is_snapshot_stable_and_pages_are_bounded() {
        let identity = DirectoryIdentity {
            snapshot: [1; 32],
            dir_key: [2; 16],
        };
        let mut cursor = ReadDirCursor::start(identity);
        let page = DirectoryPage {
            identity,
            next_ordinal: 3,
            entries: vec![DirectoryEntry {
                name: name(b"x"),
                inode: 9,
                kind: 1,
            }],
            end: false,
        };
        cursor.advance(&page).unwrap();
        assert_eq!(cursor.next_ordinal, 3);
        let other = DirectoryPage {
            identity: DirectoryIdentity {
                snapshot: [3; 32],
                ..identity
            },
            ..page
        };
        assert_eq!(cursor.advance(&other), Err(RangeRouteError::CursorMismatch));
    }

    #[test]
    fn range_window_rejects_out_of_range_sources_and_unbounded_counts() {
        let source = WindowSource {
            source_id: 1,
            kind: WindowSourceKind::FileEntries,
            first_name: name(b"a"),
            last_name: name(b"z"),
            entry_count: 1,
        };
        assert!(matches!(
            RangeWindow::new(Some(name(b"m")), None, vec![source.clone()], 1,),
            Err(RangeRouteError::InvalidSource)
        ));
        assert!(matches!(
            RangeWindow::new(None, None, vec![source], (MAX_WINDOW_ENTRIES + 1) as u32),
            Err(RangeRouteError::TooManyEntries { .. })
        ));
    }
}
