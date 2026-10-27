//! Bounded three-way merge of head/base raw deltas and immutable packed lower.
//! Cookies remain visible child ordinals. A lease-proven local namespace
//! generation tolerates data-only commits; otherwise use the remote head fence.
//! Namespace changes fence the handle; FUSE rewind opens a new generation.

use std::collections::VecDeque;

use super::*;
use crate::meta::layer::OwnedXattrNames;
use crate::vfs::handles::{DirectoryPageSource, OwnedDirectoryPage, RawDirEntry};
use crate::workspace_overlay::catalog::WorkspaceNamePage;
use crate::workspace_overlay::model::DentryOp;
use crate::workspace_overlay::packed_reader_lifecycle::PackedReaderRequestOwner;
use crate::workspace_overlay::packed_v3::wire005::{V3BudgetPool, V3OwnedPermit};

const PAGE_ENTRIES: usize = 256;
const CHECKPOINTS: usize = 16;
const MAX_COOKIE: u64 = i64::MAX as u64 - 2;

struct EnumerationOutputOwner {
    _permit: V3OwnedPermit,
    _reader: Option<PackedReaderRequestOwner>,
}
impl std::fmt::Debug for EnumerationOutputOwner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("EnumerationOutputOwner")
    }
}

#[derive(Clone, Default)]
struct MergeCursor {
    visible: u64,
    lower: u64,
    after: [Option<Vec<u8>>; 2],
}

struct EnumerationView<W> {
    store: Arc<W>,
    lower: Arc<WorkspacePackedLower>,
    guard: HeadGuard,
    layers: [LayerRecord; 2],
    local_namespace: Option<(
        namespace_epoch::NamespaceSnapshot,
        Arc<crate::workspace_overlay::lifecycle::LeaseValidity>,
    )>,
}
impl<W: WorkspaceStore + 'static> EnumerationView<W> {
    async fn validate(&self) -> Result<(), MetaError> {
        if let Some((namespace, lease)) = &self.local_namespace {
            if !lease.authoritative() {
                return Err(workspace_to_meta(WorkspaceError::Fenced));
            }
            return if namespace.unchanged() {
                Ok(())
            } else {
                Err(workspace_to_meta(WorkspaceError::Busy))
            };
        }
        // The immutable lower needs no binding authentication. The writable
        // head/lease fence (one MGET per page) detects same-epoch upper mutation.
        self.store
            .validate_read_fence(self.guard.clone(), self.layers.clone())
            .await
            .map_err(workspace_to_meta)
    }
}

struct WorkspaceDirectory<W> {
    view: EnumerationView<W>,
    lower_dir: Option<DirHandle>,
    native_empty: bool,
    checkpoints: Mutex<VecDeque<MergeCursor>>,
    _reader: Option<PackedReaderRequestOwner>,
    _permit: V3OwnedPermit,
}

struct NativePage<T> {
    page: Option<WorkspaceNamePage<T>>,
    index: usize,
    eof: bool,
}
impl<T> Default for NativePage<T> {
    fn default() -> Self {
        Self {
            page: None,
            index: 0,
            eof: false,
        }
    }
}
impl<T> NativePage<T> {
    fn front(&self) -> Option<&T> {
        self.page
            .as_ref()
            .and_then(|page| page.rows.get(self.index))
    }
    fn needs_page(&self) -> bool {
        !self.eof && self.front().is_none()
    }
    fn replace(&mut self, page: WorkspaceNamePage<T>) {
        self.eof = page.rows.is_empty();
        self.page = Some(page);
        self.index = 0;
    }
}

struct DirectoryMerge<'a, W> {
    source: &'a WorkspaceDirectory<W>,
    ino: i64,
    cursor: MergeCursor,
    native: [NativePage<DentryDelta>; 2],
    lower: Option<OwnedDirectoryPage>,
    lower_index: usize,
    lower_eof: bool,
    examined: u64,
}
impl<W: WorkspaceStore + 'static> DirectoryMerge<'_, W> {
    async fn next_visible(&mut self) -> Result<Option<RawDirEntry>, MetaError> {
        loop {
            if self.examined.is_multiple_of(32) {
                if self.source.view.lower.budget.state().closed {
                    return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                        libc::ENOMEM,
                    )));
                }
                self.source.view.validate().await?;
                // A memory backend can complete every page synchronously;
                // still provide a cancellation/scheduling boundary while
                // skipping a long sequence of whiteouts.
                tokio::task::yield_now().await;
            }
            for ordinal in 0..2 {
                if self.native[ordinal].needs_page() {
                    // Do not retain an exhausted page while acquiring its replacement.
                    self.native[ordinal].page = None;
                    let page = self
                        .source
                        .view
                        .store
                        .get_dentry_delta_page(
                            self.source.view.layers[ordinal].layer_id,
                            self.ino,
                            self.cursor.after[ordinal].as_deref(),
                            self.source.view.lower.budget.clone(),
                        )
                        .await
                        .map_err(workspace_to_meta)?;
                    self.native[ordinal].replace(page);
                }
            }
            if !self.lower_eof
                && self
                    .lower
                    .as_ref()
                    .is_none_or(|page| self.lower_index == page.entries.len())
            {
                self.lower = None;
                self.lower_index = 0;
                self.lower = match &self.source.lower_dir {
                    Some(dir) => Some(
                        dir.get_entries_page_raw_owned(self.cursor.lower, 32)
                            .await?,
                    ),
                    None => None,
                };
                self.lower_eof = self
                    .lower
                    .as_ref()
                    .is_none_or(|page| page.entries.is_empty());
            }
            let lower = self
                .lower
                .as_ref()
                .and_then(|page| page.entries.get(self.lower_index));
            let name = self
                .native
                .iter()
                .filter_map(|page| page.front().map(|row| row.name.as_slice()))
                .chain(lower.map(|row| row.name.as_slice()))
                .min()
                .map(<[u8]>::to_vec);
            let Some(name) = name else {
                return Ok(None);
            };
            self.examined = (self.examined + 1) % 32;
            let mut winner = None;
            let mut native_winner = false;
            for ordinal in 0..2 {
                if let Some(row) = self.native[ordinal].front().filter(|row| row.name == name) {
                    if !native_winner {
                        native_winner = true;
                        if row.op == DentryOp::Put {
                            winner = Some(RawDirEntry {
                                name: name.clone(),
                                ino: row.ino.ok_or_else(|| {
                                    MetaError::Internal("dentry missing inode".into())
                                })?,
                                kind: file_type_from_code(row.entry_type.ok_or_else(|| {
                                    MetaError::Internal("dentry missing kind".into())
                                })?)?,
                            });
                        }
                    }
                    self.cursor.after[ordinal] = Some(name.clone());
                    self.native[ordinal].index += 1;
                }
            }
            if lower.is_some_and(|row| row.name == name) {
                if !native_winner {
                    winner = lower.cloned();
                }
                self.cursor.lower = self.cursor.lower.checked_add(1).ok_or_else(|| {
                    MetaError::Internal("lower directory ordinal overflow".into())
                })?;
                self.lower_index += 1;
            }
            if let Some(winner) = winner {
                if self.cursor.visible >= MAX_COOKIE {
                    return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                        libc::EOVERFLOW,
                    )));
                }
                self.cursor.visible += 1;
                return Ok(Some(winner));
            }
        }
    }
}

#[async_trait]
impl<W: WorkspaceStore + 'static> DirectoryPageSource for WorkspaceDirectory<W> {
    async fn read_page_owned(
        &self,
        ino: i64,
        offset: u64,
        max_entries: usize,
    ) -> Result<OwnedDirectoryPage, MetaError> {
        if offset > MAX_COOKIE {
            return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                libc::EOVERFLOW,
            )));
        }
        let permit = self
            .view
            .lower
            .budget
            .admit(&[
                (V3BudgetPool::Output, 128 << 10),
                (V3BudgetPool::Control, 8192),
            ])
            .map_err(packed_lower::budget_to_meta)?;
        let reader = self
            .view
            .lower
            .authority
            .retain_reader_request()
            .map_err(workspace_to_meta)?;
        let output_owner: crate::meta::layer::MetadataMemoryGuard =
            Arc::new(EnumerationOutputOwner {
                _permit: permit,
                _reader: reader,
            });
        self.view.validate().await?;
        let limit = max_entries.min(PAGE_ENTRIES);
        if limit == 0 {
            return Ok(OwnedDirectoryPage {
                entries: Vec::new(),
                guard: Some(output_owner),
            });
        }
        if self.native_empty {
            let page = match &self.lower_dir {
                Some(dir) => dir.get_entries_page_raw_owned(offset, limit).await?,
                None => OwnedDirectoryPage::from(Vec::new()),
            };
            self.view.validate().await?;
            // Moving the entries transfers their charge to our output permit.
            return Ok(OwnedDirectoryPage {
                entries: page.entries,
                guard: Some(output_owner),
            });
        }
        // A bounded checkpoint cache is an accelerator, never cookie identity.
        // Cancellation commits no checkpoint and drops all transient pages.
        let cursor = self
            .checkpoints
            .lock()
            .await
            .iter()
            .filter(|cursor| cursor.visible <= offset)
            .max_by_key(|cursor| cursor.visible)
            .cloned()
            .unwrap_or_default();
        let mut merge = DirectoryMerge {
            source: self,
            ino,
            cursor,
            native: [NativePage::default(), NativePage::default()],
            lower: None,
            lower_index: 0,
            lower_eof: self.lower_dir.is_none(),
            examined: 0,
        };
        while merge.cursor.visible < offset {
            if merge.next_visible().await?.is_none() {
                break;
            }
        }
        let mut entries = Vec::with_capacity(limit);
        if merge.cursor.visible == offset {
            while entries.len() < limit {
                let Some(entry) = merge.next_visible().await? else {
                    break;
                };
                entries.push(entry);
            }
        }
        self.view.validate().await?;
        let mut checkpoints = self.checkpoints.lock().await;
        if checkpoints.len() == CHECKPOINTS {
            checkpoints.pop_front();
        }
        checkpoints.push_back(merge.cursor.clone());
        Ok(OwnedDirectoryPage {
            entries,
            guard: Some(output_owner),
        })
    }

    async fn read_page(
        &self,
        ino: i64,
        offset: u64,
        max_entries: usize,
    ) -> Result<Vec<RawDirEntry>, MetaError> {
        Ok(self
            .read_page_owned(ino, offset, max_entries)
            .await?
            .entries)
    }
}

impl<W: WorkspaceStore + 'static> WorkspaceMetaLayer<W> {
    pub(super) async fn directory_is_empty(&self, ino: i64) -> Result<bool, MetaError> {
        if self.packed_lower().is_some() {
            let directory = self.packed_opendir(ino).await?;
            return Ok(directory
                .get_entries_page_raw_owned(0, 1)
                .await?
                .entries
                .is_empty());
        }
        Ok(self.readdir(ino).await?.is_empty())
    }

    pub(super) async fn packed_opendir(&self, ino: i64) -> Result<DirHandle, MetaError> {
        // No handle/cookie has escaped yet. A concurrent upper commit can
        // invalidate any of the preparation reads, so rebuild the whole view
        // rather than exposing a transient version conflict as opendir EBUSY.
        // Published handles still fence changes in read_page_owned.
        for _ in 0..64 {
            match self.packed_opendir_once(ino).await {
                Err(MetaError::Io(ref error)) if error.raw_os_error() == Some(libc::EBUSY) => {
                    tokio::task::yield_now().await;
                }
                result => return result,
            }
        }
        Err(MetaError::Io(std::io::Error::from_raw_os_error(
            libc::EBUSY,
        )))
    }

    async fn packed_opendir_once(&self, ino: i64) -> Result<DirHandle, MetaError> {
        // Capture before any preparation await. Every local namespace commit
        // and view replacement fences this token; data/attribute writes do
        // not change the directory's names or ordinal cookies. The exclusive
        // lease rules out another upper writer for the token's whole lifetime.
        let local_namespace = self.lease_validity.as_ref().and_then(|lease| {
            if !lease.authoritative() {
                return None;
            }
            self.namespace_epoch
                .capture()
                .map(|namespace| (namespace, lease.clone()))
        });
        let lower = self
            .packed_lower()
            .ok_or_else(|| MetaError::Internal("packed directory without lower".into()))?
            .clone();
        // Includes the source, lower handle, fixed two-layer fence and 16
        // checkpoints, before allocating any of those owners.
        let permit = lower
            .budget
            .admit(&[(V3BudgetPool::Metadata, 32 << 10)])
            .map_err(packed_lower::budget_to_meta)?;
        let (guard, layers, reader) = self
            .packed_enumeration_fence()
            .await?
            .ok_or_else(|| MetaError::Internal("packed directory missing fence".into()))?;
        let attr = self.stat(ino).await?.ok_or(MetaError::NotFound(ino))?;
        if attr.kind != FileType::Dir {
            return Err(MetaError::NotDirectory(ino));
        }
        let lower_dir = if lower.metadata.stat(ino).await?.is_some() {
            Some(lower.metadata.opendir(ino).await?)
        } else {
            None
        };
        let mut native_empty = true;
        for layer in &layers {
            let page = self
                .store
                .get_dentry_delta_page(layer.layer_id, ino, None, lower.budget.clone())
                .await
                .map_err(workspace_to_meta)?;
            native_empty &= page.rows.is_empty();
        }
        let view = EnumerationView {
            store: self.store.clone(),
            lower,
            guard,
            layers,
            local_namespace,
        };
        view.validate().await?;
        let source = WorkspaceDirectory {
            view,
            lower_dir,
            native_empty,
            checkpoints: Mutex::new(VecDeque::with_capacity(CHECKPOINTS)),
            _reader: reader,
            _permit: permit,
        };
        Ok(DirHandle::new_paged(ino, Arc::new(source)).with_attr(attr))
    }

    pub(super) async fn packed_list_xattr_bytes_owned(
        &self,
        ino: i64,
    ) -> Result<OwnedXattrNames, MetaError> {
        let lower = self
            .packed_lower()
            .ok_or_else(|| MetaError::Internal("packed xattrs without lower".into()))?;
        // Up to 64 KiB Linux name list plus Vec/name capacity and temporary
        // winner copies. This owner follows the returned names into FUSE.
        let permit = lower
            .budget
            .admit(&[(V3BudgetPool::Output, 2 << 20)])
            .map_err(packed_lower::budget_to_meta)?;
        let reader = lower
            .authority
            .retain_reader_request()
            .map_err(workspace_to_meta)?;
        let output_owner: crate::meta::layer::MetadataMemoryGuard =
            Arc::new(EnumerationOutputOwner {
                _permit: permit,
                _reader: reader,
            });
        let fence = self.packed_enumeration_fence().await?;
        if self.stat(ino).await?.is_none() {
            return Err(MetaError::NotFound(ino));
        }
        let layers = &fence
            .as_ref()
            .ok_or_else(|| MetaError::Internal("packed xattrs missing fence".into()))?
            .1;
        let cold = lower.metadata.frozen_cold_attributes_owned(ino).await?;
        let lower_names = cold
            .as_ref()
            .map(|attrs| attrs.xattrs.as_slice())
            .unwrap_or_default();
        let mut lower_index = 0usize;
        let mut native: [NativePage<XattrDelta>; 2] =
            [NativePage::default(), NativePage::default()];
        let mut after: [Option<Vec<u8>>; 2] = [None, None];
        let mut names = Vec::new();
        let mut bytes = 0usize;
        let mut examined = 0u64;
        loop {
            if examined.is_multiple_of(32) && lower.budget.state().closed {
                return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                    libc::ENOMEM,
                )));
            }
            if examined.is_multiple_of(32) {
                let (guard, layers, _) = fence
                    .as_ref()
                    .ok_or_else(|| MetaError::Internal("packed xattrs missing fence".into()))?;
                self.store
                    .validate_read_fence(guard.clone(), layers.clone())
                    .await
                    .map_err(workspace_to_meta)?;
                tokio::task::yield_now().await;
            }
            for ordinal in 0..2 {
                if native[ordinal].needs_page() {
                    native[ordinal].page = None;
                    let page = self
                        .store
                        .get_xattr_delta_page(
                            layers[ordinal].layer_id,
                            ino,
                            after[ordinal].as_deref(),
                            lower.budget.clone(),
                        )
                        .await
                        .map_err(workspace_to_meta)?;
                    native[ordinal].replace(page);
                }
            }
            let name = native
                .iter()
                .filter_map(|page| page.front().map(|row| row.name.as_slice()))
                .chain(lower_names.get(lower_index).map(|row| row.name.as_slice()))
                .min()
                .map(<[u8]>::to_vec);
            let Some(name) = name else {
                break;
            };
            examined = (examined + 1) % 32;
            let mut winner = None;
            for ordinal in 0..2 {
                if let Some(row) = native[ordinal].front().filter(|row| row.name == name) {
                    if winner.is_none() {
                        winner = Some(row.op == ValueOp::Put);
                    }
                    after[ordinal] = Some(name.clone());
                    native[ordinal].index += 1;
                }
            }
            if lower_names
                .get(lower_index)
                .is_some_and(|row| row.name == name)
            {
                if winner.is_none() {
                    winner = Some(true);
                }
                lower_index += 1;
            }
            if winner == Some(true) {
                bytes = bytes
                    .checked_add(name.len() + 1)
                    .ok_or_else(|| MetaError::Io(std::io::Error::from_raw_os_error(libc::E2BIG)))?;
                if bytes > 65536 {
                    return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                        libc::E2BIG,
                    )));
                }
                names.push(name);
            }
        }
        self.validate_packed_metadata_fence(fence).await?;
        Ok(OwnedXattrNames {
            names,
            guard: Some(output_owner),
        })
    }
}
