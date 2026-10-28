//! Complete reverse paths from exact per-inode native and packed indexes.
//! No lower namespace walk and no partial result on quota, cycle or view drift.

use super::*;
use crate::meta::layer::{MetadataMemoryGuard, OwnedPaths};
use crate::workspace_overlay::packed_v3::wire005::{V3BudgetPool, V3OwnedPermit};

const MAX_PAGES: usize = 256;
const MAX_WORK: usize = 4096;
const MAX_DEPTH: usize = 1024;
const MAX_OUTPUT_BYTES: usize = 256 << 10;
const MAX_PENDING_BYTES: usize = 2 << 20;
// Covers the 256 KiB bytes, 4096 Vec headers and the final consumer's bounded
// ancestor component vector. Admission precedes construction and follows the
// returned paths through every awaited permission lookup.
const PATHS_OUTPUT_BYTES: u64 = 1 << 20;
const WALK_ARENA_BYTES: u64 = 16 << 20;
// Metadata kept free for non-waiting FUSE request (2 MiB), frozen mutation
// version (2 MiB) and commit-bound (~2 MiB) charges while walks wait: about
// ten such operations in flight, a quarter of the default 256 MiB pool.
const WALK_HEADROOM_BYTES: u64 = 64 << 20;

struct PathsOutputOwner {
    _permit: V3OwnedPermit,
    _reader: Option<crate::workspace_overlay::packed_reader_lifecycle::PackedReaderRequestOwner>,
}

impl std::fmt::Debug for PathsOutputOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PathsOutputOwner").finish_non_exhaustive()
    }
}

type Name = (i64, Vec<u8>);

struct Limits {
    pages: usize,
    work: usize,
}
impl Limits {
    fn page(&mut self) -> Result<(), MetaError> {
        self.pages += 1;
        if self.pages > MAX_PAGES {
            return Err(exhausted());
        }
        Ok(())
    }
    fn work(&mut self) -> Result<(), MetaError> {
        self.work += 1;
        if self.work > MAX_WORK {
            return Err(exhausted());
        }
        Ok(())
    }
}

struct PathWork {
    inode: i64,
    components: Vec<Vec<u8>>,
    seen: Vec<i64>,
    bytes: usize,
}

fn exhausted() -> MetaError {
    MetaError::Io(std::io::Error::from_raw_os_error(libc::E2BIG))
}

impl<W: WorkspaceStore + 'static> WorkspaceMetaLayer<W> {
    async fn packed_reverse_aliases(
        &self,
        lower: &WorkspacePackedLower,
        authority: &crate::workspace_overlay::catalog::WorkspaceNativeReverseAuthority,
        inode: i64,
        limits: &mut Limits,
    ) -> Result<Vec<Name>, MetaError> {
        let cache = self.namespace_row_cache();
        if let Some(super::row_cache::RowValue::Aliases(aliases)) =
            cache.and_then(|cache| cache.get(&super::row_cache::RowKey::Aliases(inode)))
        {
            for _ in aliases.iter() {
                limits.work()?;
            }
            return Ok((*aliases).clone());
        }
        let key = super::row_cache::RowKey::Aliases(inode);
        let ticket = cache.map(|cache| cache.ticket(&key));
        let effective = self
            .packed_reverse_aliases_uncached(lower, authority, inode, limits)
            .await?;
        if let (Some(cache), Some(ticket)) = (cache, ticket) {
            cache.insert(
                ticket,
                key,
                super::row_cache::RowValue::Aliases(Arc::new(effective.clone())),
            );
        }
        Ok(effective)
    }

    async fn packed_reverse_aliases_uncached(
        &self,
        lower: &WorkspacePackedLower,
        authority: &crate::workspace_overlay::catalog::WorkspaceNativeReverseAuthority,
        inode: i64,
        limits: &mut Limits,
    ) -> Result<Vec<Name>, MetaError> {
        let mut candidates = BTreeSet::new();
        for layer in self.chain().await? {
            let mut cursor: Option<Name> = None;
            loop {
                limits.page()?;
                let page = self
                    .store
                    .get_native_reverse_dentry_page(
                        authority,
                        layer.layer_id,
                        inode,
                        cursor
                            .as_ref()
                            .map(|(parent, name)| (*parent, name.as_slice())),
                        lower.budget.clone(),
                    )
                    .await
                    .map_err(workspace_to_meta)?;
                if page.rows.is_empty() {
                    break;
                }
                for row in &page.rows {
                    limits.work()?;
                    let next = (row.parent_ino, row.name.clone());
                    if cursor.as_ref().is_some_and(|old| &next <= old) {
                        return Err(MetaError::Internal(
                            "native reverse cursor did not advance".into(),
                        ));
                    }
                    candidates.insert(next.clone());
                    cursor = Some(next);
                }
                // A backend-proven short page needs no empty confirmation
                // page; completeness is still re-authenticated at the end.
                if page.end {
                    break;
                }
            }
        }
        // The immutable lower's complete alias list is memoized
        // for the binding lifetime; page limits apply on the first load.
        let lower_aliases = lower.reverse_aliases(inode, || limits.page()).await?;
        for (parent, name) in lower_aliases.iter() {
            limits.work()?;
            candidates.insert((*parent, name.clone()));
        }
        let mut effective = Vec::new();
        for (parent, name) in candidates {
            limits.work()?;
            if self
                .resolve_dentry_entry(parent, &name)
                .await?
                .is_some_and(|entry| entry.ino == inode)
            {
                let parent_attr = self
                    .stat(parent)
                    .await?
                    .ok_or(MetaError::NotFound(parent))?;
                if parent_attr.kind != FileType::Dir {
                    return Err(MetaError::NotDirectory(parent));
                }
                effective.push((parent, name));
            }
        }
        Ok(effective)
    }

    pub(super) async fn packed_paths_bytes(&self, target: i64) -> Result<Vec<Vec<u8>>, MetaError> {
        // Compatibility callers take the bytes only. Consumers that await more
        // metadata work must retain the owned form instead.
        Ok(self.packed_paths_bytes_owned(target).await?.paths)
    }

    /// A path walk spans many reads under one head/lease view. A concurrent
    /// local commit (for example another file's create) advances the head
    /// and fences the walk with Busy; restart it from a fresh view a bounded
    /// number of times, like every mutation retry. Other errors stay fatal.
    pub(super) async fn packed_paths_bytes_owned(
        &self,
        target: i64,
    ) -> Result<OwnedPaths, MetaError> {
        // The path walk reads many upper rows; the opt-in namespace cache
        // memoizes its complete result. Only namespace-changing commits
        // (dentry writes, inode deletion) invalidate it.
        let cache = self.namespace_row_cache();
        if let Some(super::row_cache::RowValue::Paths(paths)) =
            cache.and_then(|cache| cache.get(&super::row_cache::RowKey::Paths(target)))
        {
            return Ok(OwnedPaths {
                paths: (*paths).clone(),
                guard: None,
            });
        }
        let key = super::row_cache::RowKey::Paths(target);
        let ticket = cache.map(|cache| cache.ticket(&key));
        const ATTEMPTS: usize = 64;
        for attempt in 1..=ATTEMPTS {
            match self.packed_paths_bytes_owned_attempt(target).await {
                Ok(owned) => {
                    if let (Some(cache), Some(ticket)) = (cache, ticket) {
                        cache.insert(
                            ticket,
                            key,
                            super::row_cache::RowValue::Paths(Arc::new(owned.paths.clone())),
                        );
                    }
                    return Ok(owned);
                }
                Err(MetaError::Io(ref error))
                    if error.raw_os_error() == Some(libc::EBUSY) && attempt < ATTEMPTS =>
                {
                    tokio::task::yield_now().await;
                }
                result => return result,
            }
        }
        unreachable!("bounded path walk retry always returns")
    }

    async fn packed_paths_bytes_owned_attempt(&self, target: i64) -> Result<OwnedPaths, MetaError> {
        let lower = self
            .packed_lower()
            .ok_or_else(|| MetaError::Internal("missing packed path lower".into()))?;
        // A bounded number of walks own their 16 MiB arena at once; others
        // queue here instead of failing with ENOMEM. The FUSE request charge
        // a caller may hold is never part of this wait.
        let _walk = lower
            .path_walks
            .acquire()
            .await
            .map_err(|_| MetaError::Io(std::io::Error::from_raw_os_error(libc::ENOMEM)))?;
        // Waits (bounded by ADMISSION_WAIT) while the pool is busy and only
        // takes the arena if WALK_HEADROOM_BYTES stay free, so the small
        // non-waiting per-request and commit charges in the same pool are
        // not starved into ENOMEM. The only Metadata a waiting walk holds is
        // its caller's 2 MiB request charge; arenas are released when walks
        // finish, so waiters make progress or fail closed at the deadline.
        let _permit: V3OwnedPermit = lower
            .budget
            .admit_waiting_with_headroom(
                V3BudgetPool::Metadata,
                WALK_ARENA_BYTES,
                WALK_HEADROOM_BYTES,
            )
            .await
            .map_err(packed_lower::budget_to_meta)?;
        let output_permit = lower
            .budget
            .admit(&[(V3BudgetPool::Output, PATHS_OUTPUT_BYTES)])
            .map_err(packed_lower::budget_to_meta)?;
        let output_reader = lower
            .authority
            .retain_reader_request()
            .map_err(workspace_to_meta)?;
        let guard: MetadataMemoryGuard = Arc::new(PathsOutputOwner {
            _permit: output_permit,
            _reader: output_reader,
        });
        let fence = self.packed_enumeration_fence().await?;
        let mut limits = Limits { pages: 0, work: 0 };
        if self.stat(target).await?.is_none() {
            lower
                .budget
                .admit(&[])
                .map_err(packed_lower::budget_to_meta)?;
            self.validate_packed_metadata_fence(fence).await?;
            lower
                .budget
                .admit(&[])
                .map_err(packed_lower::budget_to_meta)?;
            return Ok(OwnedPaths {
                paths: Vec::new(),
                guard: Some(guard),
            });
        }
        if target == self.root_ino() {
            lower
                .budget
                .admit(&[])
                .map_err(packed_lower::budget_to_meta)?;
            self.validate_packed_metadata_fence(fence).await?;
            lower
                .budget
                .admit(&[])
                .map_err(packed_lower::budget_to_meta)?;
            return Ok(OwnedPaths {
                paths: vec![b"/".to_vec()],
                guard: Some(guard),
            });
        }
        let authority = self
            .store
            .get_native_reverse_authority(
                fence
                    .as_ref()
                    .ok_or_else(|| MetaError::Internal("missing packed path fence".into()))?
                    .1
                    .as_slice(),
                lower.budget.clone(),
            )
            .await
            .map_err(workspace_to_meta)?;
        let mut cached = BTreeMap::<i64, Vec<Name>>::new();
        let mut pending = vec![PathWork {
            inode: target,
            components: Vec::new(),
            seen: Vec::new(),
            bytes: 128,
        }];
        let mut pending_bytes = 128usize;
        let mut output = BTreeSet::new();
        let mut output_bytes = 0usize;
        while let Some(work) = pending.pop() {
            limits.work()?;
            pending_bytes -= work.bytes;
            if work.seen.contains(&work.inode) || work.seen.len() >= MAX_DEPTH {
                return Err(MetaError::Internal(
                    "packed effective ancestor chain is cyclic/overlong".into(),
                ));
            }
            if let std::collections::btree_map::Entry::Vacant(entry) = cached.entry(work.inode) {
                let aliases = self
                    .packed_reverse_aliases(lower, &authority, work.inode, &mut limits)
                    .await?;
                entry.insert(aliases);
            }
            for (parent, name) in &cached[&work.inode] {
                limits.work()?;
                let path_bytes = name.len()
                    + 1
                    + work
                        .components
                        .iter()
                        .map(|part| part.len() + 1)
                        .sum::<usize>();
                if path_bytes > MAX_OUTPUT_BYTES {
                    return Err(exhausted());
                }
                if *parent == self.root_ino() {
                    if output.len() >= MAX_WORK || output_bytes + path_bytes > MAX_OUTPUT_BYTES {
                        return Err(exhausted());
                    }
                    let mut path = Vec::with_capacity(path_bytes);
                    path.push(b'/');
                    path.extend_from_slice(name);
                    for part in work.components.iter().rev() {
                        path.push(b'/');
                        path.extend_from_slice(part);
                    }
                    if output.insert(path) {
                        output_bytes += path_bytes;
                    }
                } else {
                    let bytes = work.bytes
                        + name.len()
                        + 1
                        + std::mem::size_of::<Vec<u8>>()
                        + std::mem::size_of::<i64>();
                    if pending_bytes + bytes > MAX_PENDING_BYTES {
                        return Err(exhausted());
                    }
                    let mut components = work.components.clone();
                    components.push(name.clone());
                    let mut seen = work.seen.clone();
                    seen.push(work.inode);
                    pending.push(PathWork {
                        inode: *parent,
                        components,
                        seen,
                        bytes,
                    });
                    pending_bytes += bytes;
                }
            }
        }
        lower
            .budget
            .admit(&[])
            .map_err(packed_lower::budget_to_meta)?;
        self.validate_packed_metadata_fence(fence).await?;
        self.store
            .confirm_native_reverse_authority(&authority, lower.budget.clone())
            .await
            .map_err(workspace_to_meta)?;
        lower
            .budget
            .admit(&[])
            .map_err(packed_lower::budget_to_meta)?;
        Ok(OwnedPaths {
            paths: output.into_iter().collect(),
            guard: Some(guard),
        })
    }
}
