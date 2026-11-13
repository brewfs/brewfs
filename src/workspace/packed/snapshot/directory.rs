//! Names-only reads retain no file payload or placement records. GM07 stores
//! names beside inline data, so its whole range must still be authenticated.

use std::{borrow::Cow, sync::Arc};

use super::{
    SnapshotBudgetPool, SnapshotGroupRef, SnapshotMountBudget, SnapshotObjectRef, SnapshotOwned,
};
use crate::cadapter::{
    client::{ObjectBackend, ObjectClient},
    read_observer::{ReadClass, ReadWork},
};
use crate::workspace::packed::{
    PackedCodec, decode_block,
    meta::directory::GroupDirectory,
    wire::{PackedResult, PackedWireError},
};

pub(super) type OwnedGroupDirectory = Arc<SnapshotOwned<GroupDirectory>>;

const DIRECTORY_DECODE_BYTES: u64 = 6 << 20;

/// Parallel enumeration must not require eight decoders to fit on a mount
/// that can admit one. Leave ordinary admission to arbitrate concurrent users.
fn directory_batch_limit(budget: &SnapshotMountBudget) -> PackedResult<usize> {
    let state = budget.state();
    let workspace = super::super::codec::decode_workspace_bytes(PackedCodec::Zstd)? as u64;
    let mut limit = 8;
    for (pool, bytes) in [
        (
            SnapshotBudgetPool::Metadata,
            DIRECTORY_DECODE_BYTES + (16 << 10),
        ),
        (SnapshotBudgetPool::Plans, 16 << 10),
        (SnapshotBudgetPool::Control, 4096),
        (SnapshotBudgetPool::Stored, 512 << 10),
        (SnapshotBudgetPool::Raw, 256 << 10),
        (SnapshotBudgetPool::Workspace, workspace),
    ] {
        let available = budget
            .capacity(pool)
            .saturating_sub(state.used[pool as usize]);
        limit = limit.min(available / bytes);
    }
    Ok(limit.max(1) as usize)
}

struct DirectoryGroup {
    group: SnapshotGroupRef,
    container: SnapshotObjectRef,
    generation: crate::chunk::read_plan::ReadGeneration,
    epoch: u64,
    names: tokio::sync::OnceCell<OwnedGroupDirectory>,
}

/// One charged group remains available even when the mount cache is disabled.
/// Concurrent readers retain their own owners when this handle moves forward.
pub(crate) struct SnapshotDirectoryNames {
    current: std::sync::Mutex<Option<Arc<SnapshotOwned<DirectoryGroup>>>>,
    budget: Arc<SnapshotMountBudget>,
    _permit: super::SnapshotOwnedPermit,
}

impl SnapshotDirectoryNames {
    pub(crate) fn new(budget: Arc<SnapshotMountBudget>) -> PackedResult<Self> {
        let permit = budget.admit(&[(SnapshotBudgetPool::Roots, 256)])?;
        Ok(Self {
            current: std::sync::Mutex::new(None),
            budget,
            _permit: permit,
        })
    }

    pub(super) async fn read<B: ObjectBackend + Clone + 'static>(
        &self,
        group: &SnapshotGroupRef,
        container: &SnapshotObjectRef,
        reader: &super::SnapshotIndexReader<B>,
        generation: crate::chunk::read_plan::ReadGeneration,
        allocation_limit: usize,
    ) -> PackedResult<OwnedGroupDirectory> {
        let _request = self.budget.admit(&[(SnapshotBudgetPool::Control, 512)])?;
        if !Arc::ptr_eq(reader.budget(), &self.budget) {
            return Err(PackedWireError::Invalid(
                "directory belongs to a different mount budget".into(),
            ));
        }
        reader.ensure_open().await?;
        group.validate_metadata_read(container, allocation_limit)?;
        let epoch = self.budget.read_cache_epoch();
        let slot = {
            let mut current = self.current.lock().unwrap();
            if let Some(slot) = current.as_ref().filter(|slot| {
                slot.group == *group
                    && slot.container == *container
                    && slot.generation == generation
                    && slot.epoch == epoch
            }) {
                slot.clone()
            } else {
                *current = None;
                let bytes =
                    512 + group.first_name.len() + group.last_name.len() + container.key.len();
                let permit = self
                    .budget
                    .admit(&[(SnapshotBudgetPool::Metadata, bytes as u64)])?;
                let slot = Arc::new(SnapshotOwned::new(
                    DirectoryGroup {
                        group: group.clone(),
                        container: container.clone(),
                        generation,
                        epoch,
                        names: tokio::sync::OnceCell::new(),
                    },
                    permit,
                ));
                *current = Some(slot.clone());
                slot
            }
        };
        let names = slot
            .names
            .get_or_try_init(|| {
                reader.group_directory(generation, group, container, allocation_limit)
            })
            .await?
            .clone();
        if epoch != self.budget.read_cache_epoch() || self.budget.state().closed {
            return Err(PackedWireError::ReadViewChanged);
        }
        Ok(names)
    }
}

impl SnapshotGroupRef {
    pub(super) async fn read_directory_owned<B: ObjectBackend + Clone>(
        &self,
        client: &ObjectClient<B>,
        container: &SnapshotObjectRef,
        allocation_limit: usize,
        budget: &Arc<SnapshotMountBudget>,
    ) -> PackedResult<OwnedGroupDirectory> {
        self.validate_metadata_read(container, allocation_limit)?;
        let workspace = super::super::codec::decode_workspace_bytes(self.meta_codec)?;
        // Covers worst-case front-coded name expansion before any allocation.
        let mut permit = budget.admit(&[
            (SnapshotBudgetPool::Metadata, DIRECTORY_DECODE_BYTES),
            (
                SnapshotBudgetPool::Stored,
                u64::from(self.meta_stored_len) * 2,
            ),
            (SnapshotBudgetPool::Raw, u64::from(self.meta_raw_len)),
            (SnapshotBudgetPool::Workspace, workspace as u64),
        ])?;
        let directory = client
            .typed_exact(
                ReadClass::GroupMetadata,
                &container.key,
                self.meta_offset,
                u64::from(self.meta_stored_len),
                allocation_limit as u64,
                |stored| {
                    let value = (|| -> PackedResult<GroupDirectory> {
                        let timer = client
                            .measure_read_work(ReadClass::GroupMetadata, ReadWork::Authentication);
                        let digest = ring::digest::digest(&ring::digest::SHA256, &stored);
                        drop(timer);
                        if digest.as_ref() != self.meta_digest.as_slice() {
                            return Err(PackedWireError::HashMismatch {
                                what: "wire 005 authenticated GroupMeta",
                                expected: hex::encode(self.meta_digest),
                                computed: hex::encode(digest.as_ref()),
                            });
                        }
                        let _timer =
                            client.measure_read_work(ReadClass::GroupMetadata, ReadWork::Decode);
                        let raw = if self.meta_codec == PackedCodec::Raw {
                            Cow::Borrowed(stored.as_slice())
                        } else {
                            Cow::Owned(decode_block(
                                self.meta_codec,
                                &stored,
                                self.meta_raw_len as usize,
                                256 << 10,
                                self.meta_raw_len as usize,
                            )?)
                        };
                        let directory = GroupDirectory::decode(&raw)?;
                        if directory.entries.len() != self.entry_count as usize
                            || directory.entries.first().map(|e| e.name.as_slice())
                                != Some(self.first_name.as_slice())
                            || directory.entries.last().map(|e| e.name.as_slice())
                                != Some(self.last_name.as_slice())
                        {
                            return Err(PackedWireError::Invalid(
                                "wire 005 group count/name fences disagree with directory".into(),
                            ));
                        }
                        Ok(directory)
                    })();
                    value.map_err(super::observer_validation_error)
                },
            )
            .await
            .map_err(super::observer_backend_error)?;
        permit.shrink(SnapshotBudgetPool::Metadata, directory.owned_memory_bytes())?;
        for pool in [
            SnapshotBudgetPool::Stored,
            SnapshotBudgetPool::Raw,
            SnapshotBudgetPool::Workspace,
        ] {
            permit.shrink(pool, 0)?;
        }
        Ok(Arc::new(SnapshotOwned::new(directory, permit)))
    }
}

impl super::AuthenticatedSnapshot {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn readdir_names_page<B: ObjectBackend + Clone + 'static>(
        &self,
        reader: &super::SnapshotIndexReader<B>,
        parent: [u8; 32],
        offset: u64,
        limit: usize,
        allocation_limit: usize,
        directory: &SnapshotDirectoryNames,
    ) -> PackedResult<Vec<super::super::meta::directory::GroupDirectoryEntry>> {
        use super::super::meta::directory::GroupDirectoryEntry;
        use super::manifest::SNAPSHOT_MAX_DIRECTORY_ORDINAL;
        if offset > SNAPSHOT_MAX_DIRECTORY_ORDINAL {
            return Err(PackedWireError::LimitExceeded(
                "PM10 directory ordinal exceeds FUSE cookie boundary".into(),
            ));
        }
        let limit = limit.min(4096);
        if limit == 0 {
            return Ok(Vec::new());
        }
        reader.ensure_open().await?;
        let epoch = reader.budget().read_cache_epoch();
        // Up to eight exact group/container references and futures, independent
        // of the directory size. The caller owns the bounded output reservation.
        let _request = reader.budget().admit(&[
            (SnapshotBudgetPool::Plans, 64 << 10),
            (SnapshotBudgetPool::Control, 1024),
        ])?;
        let lower = parent.to_vec();
        let mut upper = lower.clone();
        upper.extend_from_slice(&[0xff; 1025]);
        let root = self.root(super::SnapshotRootKind::Groups);
        let mut cursor = reader
            .weighted_cursor(
                root,
                &lower,
                &upper,
                offset,
                self.manifest().group_dentry_count,
            )
            .await?;
        let mut result = Vec::new();
        let mut owned_bytes = 0usize;
        'pages: loop {
            let mut wanted = limit - result.len();
            let batch_limit = directory_batch_limit(reader.budget())?;
            let mut groups = Vec::with_capacity(8);
            while wanted != 0 && groups.len() < batch_limit {
                let Some((record, entry_offset)) = reader.next_weighted(root, &mut cursor).await?
                else {
                    break;
                };
                let super::SnapshotIndexValue::Leaf(value) = &record.value else {
                    return Err(PackedWireError::Invalid(
                        "PM07 group scan returned branch value".into(),
                    ));
                };
                let group = SnapshotGroupRef::decode_value(value)?;
                if group.parent_dir_key != parent || entry_offset >= u64::from(group.entry_count) {
                    return Err(PackedWireError::Invalid(
                        "PM10 selected group parent/ordinal is invalid".into(),
                    ));
                }
                wanted = wanted.saturating_sub(group.entry_count as usize - entry_offset as usize);
                let container = self.container_ref(reader, group.container_ordinal).await?;
                groups.push((group, container, entry_offset as usize));
            }
            if groups.is_empty() {
                break;
            }
            let pages = futures::future::try_join_all(groups.iter().map(
                |(group, container, start)| async move {
                    let page = directory
                        .read(
                            group,
                            container,
                            reader,
                            self.read_generation(),
                            allocation_limit,
                        )
                        .await?;
                    Ok::<_, PackedWireError>((page, *start))
                },
            ))
            .await?;
            for (page, start) in pages {
                for entry in page.entries.iter().skip(start).take(limit - result.len()) {
                    let weight = std::mem::size_of::<GroupDirectoryEntry>() + entry.name.len();
                    if owned_bytes + weight > allocation_limit {
                        if result.is_empty() {
                            return Err(PackedWireError::LimitExceeded(
                                "PM07 directory entry exceeds page budget".into(),
                            ));
                        }
                        break 'pages;
                    }
                    owned_bytes += weight;
                    if offset + result.len() as u64 >= SNAPSHOT_MAX_DIRECTORY_ORDINAL {
                        return Err(PackedWireError::LimitExceeded(
                            "PM10 next directory ordinal exceeds FUSE cookie boundary".into(),
                        ));
                    }
                    result.push(entry.clone());
                }
            }
            if result.len() == limit {
                break;
            }
        }
        if epoch != reader.budget().read_cache_epoch() || reader.budget().state().closed {
            return Err(PackedWireError::ReadViewChanged);
        }
        Ok(result)
    }
}
