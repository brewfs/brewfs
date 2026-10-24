//! Per-operation observed rows. Advisory topology chooses a bounded read set;
//! exact observations and lifecycle authority authorize reads and mutation commits.

use super::*;
use crate::workspace_overlay::entity::backend::{MAX_TRANSACTION_ROWS, RowSnapshot, add_bytes};

impl<B: EntityBackend + 'static, L: ImmutableLower + 'static> EntityNamespace<B, L> {
    pub(in crate::workspace_overlay::entity) async fn snapshot(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<(TransactionAuthority, Prepared<'_, B, L>)> {
        if !self.session.matches_keys(&self.keys)
            || self.session.lower_binding() != self.lower.binding()
        {
            return Err(EntityError::Fenced);
        }
        let authority_keys = self.lifecycle.authority_keys(&self.session)?;
        let mut read_keys = authority_keys.clone();
        read_keys.extend(
            keys.into_iter()
                .filter(|key| !authority_keys.contains(key))
                .collect::<BTreeSet<_>>(),
        );
        let mut snapshot = self.backend.read(&read_keys).await?;
        if snapshot.rows.len() != read_keys.len() {
            return Err(EntityError::Corrupt("short operation snapshot".into()));
        }
        let rows = snapshot.rows.split_off(authority_keys.len());
        let now = snapshot.now_ns;
        let authority = self.lifecycle.authority_from_snapshot(
            &self.session,
            RowSnapshot {
                now_ns: now,
                rows: snapshot.rows,
            },
        )?;
        let mut prepared = Prepared::new(self);
        prepared.now = now;
        for (key, expected) in read_keys.into_iter().skip(authority_keys.len()).zip(rows) {
            prepared.add(RowCheck { key, expected })?;
        }
        prepared.read_covered = true;
        Ok((authority, prepared))
    }

    /// Hints only nominate rows to fetch. The actual parent walk resolves the
    /// observed rows and fetches any newly discovered ancestors before validation.
    pub(super) async fn parent_keys(
        &self,
        parent: i64,
        keys: &mut BTreeSet<Vec<u8>>,
    ) -> Result<()> {
        let mut current = parent;
        let mut seen = BTreeSet::new();
        while current > 0 && seen.insert(current) && seen.len() <= MAX_ANCESTORS {
            keys.insert(self.keys.inode(current));
            keys.insert(self.keys.xattr(current, ACCESS_XATTR));
            if current == self.lower.binding().root_ino {
                break;
            }
            if let Some(parent) = self.directory_hints.get(&current) {
                current = parent;
            } else if current <= self.lower.binding().max_ino {
                match self.lower.inode(current).await? {
                    Some(inode) if inode.kind == EntityKind::Directory => {
                        if let Some(parent) = inode.parent_hint {
                            self.directory_hints.insert(current, parent);
                            current = parent;
                        } else {
                            break;
                        }
                    }
                    _ => break,
                }
            } else {
                break;
            }
        }
        Ok(())
    }

    pub(super) async fn path_keys(&self, parent: i64, name: &[u8]) -> Result<Vec<Vec<u8>>> {
        let mut keys = BTreeSet::from([self.keys.dentry(parent, name)]);
        if let Some(ino) = self.dentry_hints.get(&(parent, name.to_vec())) {
            keys.insert(self.keys.inode(ino));
            keys.insert(self.keys.xattr(ino, ACCESS_XATTR));
        }
        self.parent_keys(parent, &mut keys).await?;
        Ok(keys.into_iter().collect())
    }

    pub(super) async fn operation_keys(
        &self,
        request: &Request,
        ids: &BTreeSet<i64>,
    ) -> Result<Vec<Vec<u8>>> {
        let mut keys = BTreeSet::new();
        for ino in ids {
            keys.insert(self.keys.inode(*ino));
            if !matches!(request, Request::Stat(Target::Inode(_))) {
                keys.insert(self.keys.xattr(*ino, ACCESS_XATTR));
            }
            if request.mutation() {
                keys.insert(self.keys.truncate(*ino));
            }
            if matches!(request, Request::Unlink { .. } | Request::Rename { .. }) {
                keys.insert(self.keys.orphan(*ino));
            }
        }
        for (parent, name) in request.paths() {
            keys.extend(self.path_keys(parent, name).await?);
        }
        match request {
            Request::Create(request, _) => {
                keys.insert(self.keys.xattr(request.parent, DEFAULT_XATTR));
            }
            Request::Readdir { ino } => self.parent_keys(*ino, &mut keys).await?,
            Request::Xattr { ino, name, .. } => {
                keys.insert(self.keys.xattr(*ino, name));
            }
            _ => {}
        }
        Ok(keys.into_iter().collect())
    }
}

impl<B: EntityBackend + 'static, L: ImmutableLower + 'static> Prepared<'_, B, L> {
    pub(in crate::workspace_overlay::entity) fn now_ns(&self) -> i64 {
        self.now
    }

    pub(in crate::workspace_overlay::entity) fn observed(
        &self,
        key: &[u8],
    ) -> Option<&Option<Vec<u8>>> {
        self.rows.get(key)
    }

    pub(in crate::workspace_overlay::entity) fn observations(
        &self,
    ) -> &BTreeMap<Vec<u8>, Option<Vec<u8>>> {
        &self.rows
    }

    /// Drop only advisory rows that the freshly decoded read plan does not use.
    /// Atomic coverage of the remaining observations is unchanged.
    pub(in crate::workspace_overlay::entity) fn discard_unused_hints(
        &mut self,
        hints: &[Vec<u8>],
        required: &BTreeSet<Vec<u8>>,
    ) {
        for key in hints {
            if !required.contains(key)
                && let Some(value) = self.rows.remove(key)
            {
                self.bytes -= key.len() + value.as_ref().map_or(0, Vec::len);
            }
        }
    }

    /// Never replace an observation: a plan assembled from incompatible views
    /// is a conflict, including when one of those observations was absence.
    pub(in crate::workspace_overlay::entity) fn add(&mut self, check: RowCheck) -> Result<()> {
        if let Some(previous) = self.rows.get(&check.key) {
            return if previous == &check.expected {
                Ok(())
            } else {
                Err(EntityError::Conflict)
            };
        }
        if self.rows.len() >= MAX_TRANSACTION_ROWS {
            return Err(EntityError::Invalid(
                "operation read set exceeds row bound".into(),
            ));
        }
        let mut bytes = self.bytes;
        add_bytes(&mut bytes, &check.key, check.expected.as_deref())?;
        self.bytes = bytes;
        self.rows.insert(check.key, check.expected);
        self.read_covered = false;
        Ok(())
    }

    pub(in crate::workspace_overlay::entity) async fn prefetch(
        &mut self,
        keys: Vec<Vec<u8>>,
    ) -> Result<()> {
        let keys: Vec<_> = keys
            .into_iter()
            .filter(|key| !self.rows.contains_key(key) && !self.writes.contains_key(key))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        if keys.is_empty() {
            return Ok(());
        }
        if self.rows.len().saturating_add(keys.len()) > MAX_TRANSACTION_ROWS {
            return Err(EntityError::Invalid(
                "operation read set exceeds row bound".into(),
            ));
        }
        let (_, mut snapshot) = self
            .ns
            .snapshot(
                self.rows
                    .keys()
                    .cloned()
                    .chain(keys.iter().cloned())
                    .collect(),
            )
            .await?;
        let covered = self
            .rows
            .iter()
            .all(|(key, expected)| snapshot.rows.get(key) == Some(expected));
        self.now = snapshot.now;
        // Keep old observations even when the fresh batch disagrees. The
        // unchanged mutation commit or final read validation must reject that
        // mixed view; preparation does not introduce a new conflict exit.
        for key in keys {
            let expected = snapshot
                .rows
                .remove(&key)
                .ok_or_else(|| EntityError::Corrupt("missing prefetched row".into()))?;
            self.add(RowCheck { key, expected })?;
        }
        self.read_covered = covered;
        Ok(())
    }

    /// Complete a read from its atomic batch; force a new batch after object
    /// transport. This only validates observations and never publishes writes.
    pub(in crate::workspace_overlay::entity) async fn validate_read(
        &mut self,
        force: bool,
    ) -> Result<()> {
        if !force && self.read_covered {
            return Ok(());
        }
        self.read_covered = false;
        // Validate the entire view together, including fresh authority and
        // backend TIME. Mixed dependent reads must never authorize a result.
        let (_, snapshot) = self
            .ns
            .snapshot(self.rows.keys().cloned().collect())
            .await?;
        if self.rows != snapshot.rows {
            return Err(EntityError::Conflict);
        }
        self.now = snapshot.now;
        self.read_covered = true;
        Ok(())
    }

    pub(in crate::workspace_overlay::entity) fn plan_with_writes(
        &self,
        writes: Vec<RowWrite>,
    ) -> EntityPlan {
        let mut plan = self.plan();
        plan.writes.extend(writes);
        plan
    }

    pub(in crate::workspace_overlay::entity) async fn chunk(
        &mut self,
        inode: &EntityInode,
        index: u64,
        chunk_size: u64,
    ) -> Result<ChunkMap> {
        let row = self.raw(self.ns.keys.chunk(inode.ino, index)).await?;
        let upper: Option<ChunkMap> = row.as_deref().map(codec::decode).transpose()?;
        resolver::resolve_chunk(
            self.ns.lower.as_ref(),
            self.ns.lower.binding(),
            inode,
            index,
            chunk_size,
            upper.as_ref(),
        )
        .await
    }
}
