use super::*;
use crate::workspace::publish::merge::{MergeConflict, MergeRequest, MergeResult};

fn rows(delta: &CanonicalLayerDelta) -> usize {
    delta.dentries.len()
        + delta.inodes.len()
        + delta.xattrs.len()
        + delta.acls.len()
        + delta.extents.len()
}

fn directory_attributes(row: &InodeDelta) -> (u32, u32, u32, u32, Option<i64>) {
    (row.mode, row.uid, row.gid, row.rdev, row.parent_hint)
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    async fn merge_base_inode(
        &self,
        base: LayerId,
        ino: i64,
        cache: &mut BTreeMap<i64, Option<InodeDelta>>,
    ) -> Result<Option<InodeDelta>, WorkspaceError> {
        if let Some(value) = cache.get(&ino) {
            return Ok(value.clone());
        }
        let row = self
            .get_inode_deltas(InodeQuery {
                layer_ids: vec![base],
                ino,
            })
            .await?
            .into_iter()
            .max_by_key(|row| row.sequence);
        cache.insert(ino, row.clone());
        Ok(row)
    }

    /// Both workspaces must be unmounted. Exact workspace/head checks reject
    /// concurrent lease acquisition, deletion, mutation and head replacement.
    pub(super) async fn merge_native_offline(
        &self,
        request: MergeRequest,
    ) -> Result<MergeResult, WorkspaceError> {
        self.require_admin_access()?;
        if request.child == request.target || request.operation.is_nil() {
            return Err(conflict(
                "merge requires distinct workspaces and a nonzero operation ID",
            ));
        }
        let receipt_key =
            format!("native/merge/{}/{}", request.target, request.operation).into_bytes();
        if let Some(raw) = self.backend.get(&receipt_key).await? {
            let result: MergeResult = decode(&raw)?;
            return if result.request == request {
                Ok(result)
            } else {
                Err(conflict("merge operation ID belongs to another request"))
            };
        }
        let mut checks = vec![KvCheck {
            key: receipt_key.clone(),
            expected: None,
        }];
        let mut workspaces = Vec::new();
        let mut heads = Vec::new();
        for id in [request.child, request.target] {
            let key = hot_workspace_key(id);
            let raw = self.backend.get(&key).await?;
            let workspace: WorkspaceRecord = decode_required(&raw)?;
            if workspace.state != WorkspaceState::Active || workspace.active_lease.is_some() {
                return Err(conflict("merge requires active, unmounted workspaces"));
            }
            checks.push(KvCheck { key, expected: raw });
            let key = hot_layer_key(workspace.head_layer_id);
            let raw = self.backend.get(&key).await?;
            let head: LayerRecord = decode_required(&raw)?;
            if head.state != LayerState::Writable
                || head.depth != 2
                || head.owner_workspace_id != Some(id)
            {
                return Err(conflict("merge requires a Native Overlay writable head"));
            }
            checks.push(KvCheck { key, expected: raw });
            workspaces.push(workspace);
            heads.push(head);
        }
        if workspaces[0].upper_bucket != workspaces[1].upper_bucket {
            return Err(conflict(
                "native reference merge requires the same object bucket",
            ));
        }
        let base_id = heads[0].parent_layer_id.ok_or(WorkspaceError::Fenced)?;
        if heads[1].parent_layer_id != Some(base_id) {
            return Err(conflict("workspaces have different bases"));
        }
        let base_key = hot_layer_key(base_id);
        let base_raw = self.backend.get(&base_key).await?;
        let base: LayerRecord = decode_required(&base_raw)?;
        if base.state != LayerState::Sealed || base.depth != 1 || base.parent_layer_id.is_some() {
            return Err(conflict("merge base must be an immutable flat layer"));
        }
        for workspace in &workspaces {
            if workspace.fork_base.as_ref().is_none_or(|revision| {
                revision.layer_id != base_id || Some(revision.sealed_version) != base.sealed_version
            }) {
                return Err(conflict("workspace fork version differs from merge base"));
            }
        }
        checks.push(KvCheck {
            key: base_key,
            expected: base_raw,
        });
        // Native bases have native rows; packed manifests require their own
        // authenticated resolver and must not enter this metadata-only path.
        let now = self.now_ns().await?;
        for workspace in &workspaces {
            let key = packed_current_key(workspace.workspace_id);
            let raw = self.backend.get(&key).await?;
            if raw.is_some() {
                return Err(conflict("merge requires native base metadata"));
            }
            checks.push(KvCheck { key, expected: raw });
            let key = open_snapshot_key(workspace.workspace_id);
            let raw = self.backend.get(&key).await?;
            if let Some(raw) = &raw {
                let open: SnapshotOpenRecord = decode(raw)?;
                if open.expires_at_ns > now || open.recovery_required {
                    return Err(conflict(
                        "workspace has an open or recovering mount session",
                    ));
                }
            }
            checks.push(KvCheck { key, expected: raw });
            let key = open_snapshot_recovery_key(workspace.workspace_id);
            let raw = self.backend.get(&key).await?;
            if let Some(raw) = &raw {
                let recovery: SnapshotRecoveryRecord = decode(raw)?;
                if recovery.incomplete {
                    return Err(conflict("workspace mount recovery is incomplete"));
                }
            }
            checks.push(KvCheck { key, expected: raw });
        }
        let mut child = self.load_layer_delta(heads[0].layer_id).await?;
        let target = self.load_layer_delta(heads[1].layer_id).await?;
        let mut result = MergeResult {
            request,
            committed: false,
            target_head: heads[1].layer_id,
            target_sequence: heads[1].next_sequence,
            scanned_records: rows(&child) + rows(&target),
            applied_records: 0,
            conflicts: Vec::new(),
        };
        let mut cache = BTreeMap::new();
        for (side, other) in [(&child, &target), (&target, &child)] {
            for row in &side.extents {
                if other.extents.iter().any(|other| other.ino == row.ino)
                    || other.inodes.iter().any(|other| other.ino == row.ino)
                    || other.xattrs.iter().any(|other| other.ino == row.ino)
                    || other.acls.iter().any(|other| other.ino == row.ino)
                {
                    result.conflicts.push(MergeConflict {
                        ino: row.ino,
                        name: None,
                        reason: "file data overlaps another inode change".into(),
                    });
                }
            }
            for ino in side
                .xattrs
                .iter()
                .map(|row| row.ino)
                .chain(side.acls.iter().map(|row| row.ino))
            {
                if other.inodes.iter().any(|row| {
                    row.ino == ino && (row.kind != 1 || row.state != InodeState::Present)
                }) {
                    result.conflicts.push(MergeConflict {
                        ino,
                        name: None,
                        reason: "attribute change overlaps an inode change".into(),
                    });
                }
            }
        }
        for row in &child.xattrs {
            if target
                .xattrs
                .iter()
                .any(|other| row.ino == other.ino && row.name == other.name)
            {
                result.conflicts.push(MergeConflict {
                    ino: row.ino,
                    name: Some(row.name.clone()),
                    reason: "both workspaces changed this extended attribute".into(),
                });
            }
        }
        for row in &child.acls {
            if target.acls.iter().any(|other| {
                row.ino == other.ino && row.acl_type == other.acl_type && row.acl_id == other.acl_id
            }) {
                result.conflicts.push(MergeConflict {
                    ino: row.ino,
                    name: None,
                    reason: "both workspaces changed this ACL".into(),
                });
            }
        }
        for row in &child.dentries {
            if target
                .dentries
                .iter()
                .any(|other| row.parent_ino == other.parent_ino && row.name == other.name)
            {
                result.conflicts.push(MergeConflict {
                    ino: row.parent_ino,
                    name: Some(row.name.clone()),
                    reason: "both workspaces changed this name".into(),
                });
            }
        }
        for row in &mut child.inodes {
            if let Some(other) = target.inodes.iter().find(|other| other.ino == row.ino) {
                let original = self.merge_base_inode(base_id, row.ino, &mut cache).await?;
                if let Some(original) = original.filter(|original| {
                    original.kind == 1
                        && original.state == InodeState::Present
                        && row.kind == 1
                        && row.state == InodeState::Present
                        && other.kind == 1
                        && other.state == InodeState::Present
                        && directory_attributes(row) == directory_attributes(original)
                        && directory_attributes(other) == directory_attributes(original)
                }) {
                    row.nlink = (i128::from(other.nlink) + i128::from(row.nlink)
                        - i128::from(original.nlink))
                    .try_into()
                    .map_err(|_| WorkspaceError::Fenced)?;
                    row.size = (i128::from(other.size) + i128::from(row.size)
                        - i128::from(original.size))
                    .try_into()
                    .map_err(|_| WorkspaceError::Fenced)?;
                    row.mtime_ns = row.mtime_ns.max(other.mtime_ns);
                    row.ctime_ns = row.ctime_ns.max(other.ctime_ns);
                    row.atime_ns = row.atime_ns.max(other.atime_ns);
                } else {
                    result.conflicts.push(MergeConflict {
                        ino: row.ino,
                        name: None,
                        reason: "both workspaces changed this inode".into(),
                    });
                }
            }
        }
        for (side, other) in [(&child, &target), (&target, &child)] {
            let mut structural = BTreeSet::new();
            for row in &side.inodes {
                if let Some(original) = self.merge_base_inode(base_id, row.ino, &mut cache).await?
                    && original.kind == 1
                    && (row.state != InodeState::Present
                        || row.kind != 1
                        || row.parent_hint != original.parent_hint)
                {
                    structural.insert(row.ino);
                }
            }
            let touched = other
                .inodes
                .iter()
                .map(|row| row.ino)
                .chain(other.dentries.iter().map(|row| row.parent_ino))
                .chain(other.extents.iter().map(|row| row.ino))
                .chain(other.xattrs.iter().map(|row| row.ino))
                .chain(other.acls.iter().map(|row| row.ino))
                .collect::<BTreeSet<_>>();
            for start in touched {
                let mut ino = start;
                let mut visited = BTreeSet::new();
                while visited.insert(ino) {
                    if structural.contains(&ino) {
                        result.conflicts.push(MergeConflict {
                            ino: start,
                            name: None,
                            reason: "directory replacement or move overlaps a descendant change"
                                .into(),
                        });
                        break;
                    }
                    let row = match other.inodes.iter().find(|row| row.ino == ino) {
                        Some(row) => Some(row.clone()),
                        None => self.merge_base_inode(base_id, ino, &mut cache).await?,
                    };
                    let Some(parent) = row.and_then(|row| row.parent_hint) else {
                        break;
                    };
                    if parent == ino {
                        break;
                    }
                    ino = parent;
                }
            }
        }
        if !result.conflicts.is_empty() {
            return Ok(result);
        }
        let mut head = heads[1].clone();
        for extent in &child.extents {
            if matches!(extent.kind, ExtentKind::Data { .. }) {
                head.owned_slice_count = head
                    .owned_slice_count
                    .checked_add(1)
                    .ok_or(WorkspaceError::Fenced)?;
                head.owned_bytes = head
                    .owned_bytes
                    .checked_add(extent.length)
                    .ok_or(WorkspaceError::Fenced)?;
            }
        }
        let mut writes = Vec::new();
        child.extents.sort_by_key(|row| row.sequence);
        macro_rules! append {
            ($rows:expr, $key:ident) => {
                for mut row in $rows {
                    row.layer_id = head.layer_id;
                    row.sequence = head.next_sequence;
                    head.next_sequence = head
                        .next_sequence
                        .checked_add(1)
                        .ok_or(WorkspaceError::Fenced)?;
                    writes.push(put($key(&row), &row)?);
                }
            };
        }
        result.applied_records = rows(&child);
        append!(child.dentries, dentry_key);
        append!(child.inodes, inode_key);
        append!(child.xattrs, xattr_key);
        append!(child.acls, acl_key);
        append!(child.extents, extent_key);
        let _compaction = self
            .compact_extent_history(&head, None, DELTA_SCAN_MAX_ROWS, &mut writes)
            .await?;
        result.committed = true;
        result.target_sequence = head.next_sequence;
        writes.push(put(hot_layer_key(head.layer_id), &head)?);
        writes.push(put(receipt_key, &result)?);
        let packet = self.prepare_topology_packet(checks, writes, None).await?;
        if !self.commit_prepared_topology_packet(&packet).await? {
            return Err(WorkspaceError::Busy);
        }
        self.backend.flush_metadata().await?;
        Ok(result)
    }
}
