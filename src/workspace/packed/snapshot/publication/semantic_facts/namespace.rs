//! Candidate private namespace relation audit. No GM07/FD/codec/content proof,
//! durable pin, or catalog authority is constructed by this module.
use super::super::super::{
    AuthenticatedSnapshot, SnapshotGroupRef, SnapshotInodeLocation, SnapshotPlacement,
};
use super::*;
use crate::workspace::packed::{directory_key, snapshot::SnapshotBudgetPool};

#[derive(Clone, Copy)]
pub(in super::super) struct NamespaceContexts {
    pub groups: ContextId,
    pub inodes: ContextId,
    pub reverse: ContextId,
    pub selectors: ContextId,
    pub source: Option<ContextId>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in super::super) struct NamespaceSummary {
    pub highest_inode: u64,
    pub canonical_inodes: u64,
    pub aliases: u64,
    pub groups: u64,
    /// Includes the separate manifest root.
    pub directories: u64,
    pub root_child_directories: u64,
    pub source_records: u64,
    pub selector_records: u64,
}

const NAMESPACE_SCHEMA: &[&str] = &[
    "CREATE TABLE ns_root (id INTEGER PRIMARY KEY CHECK(id=1), inode BLOB NOT NULL UNIQUE CHECK(length(inode)=8), dir_key BLOB NOT NULL CHECK(length(dir_key)=32), nlink INTEGER CHECK(nlink IS NULL OR nlink BETWEEN 1 AND 4294967295))",
    "CREATE TABLE ns_groups (group_key BLOB PRIMARY KEY NOT NULL CHECK(length(group_key) BETWEEN 33 AND 1056), value BLOB NOT NULL CHECK(length(value)<=4096), entry_count INTEGER NOT NULL CHECK(entry_count BETWEEN 1 AND 4096), observed INTEGER NOT NULL DEFAULT 0 CHECK(observed BETWEEN 0 AND 4096)) WITHOUT ROWID",
    "CREATE TABLE ns_canonical (inode BLOB PRIMARY KEY NOT NULL CHECK(length(inode)=8), value BLOB NOT NULL CHECK(length(value)<=8192), kind INTEGER NOT NULL CHECK(kind BETWEEN 1 AND 7), nlink INTEGER NOT NULL CHECK(nlink BETWEEN 1 AND 4294967295), alias_count BLOB NOT NULL CHECK(length(alias_count)=8)) WITHOUT ROWID",
    "CREATE TABLE ns_aliases (reverse_key BLOB PRIMARY KEY NOT NULL CHECK(length(reverse_key) BETWEEN 17 AND 1040), inode BLOB NOT NULL CHECK(length(inode)=8), parent BLOB NOT NULL CHECK(length(parent)=8), parent_dir_key BLOB NOT NULL CHECK(length(parent_dir_key)=32), name BLOB NOT NULL CHECK(length(name) BETWEEN 1 AND 1024), group_key BLOB NOT NULL CHECK(length(group_key) BETWEEN 33 AND 1056), ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 4095), value BLOB NOT NULL CHECK(length(value)<=8192)) WITHOUT ROWID",
    "CREATE UNIQUE INDEX ns_dentries ON ns_aliases(parent,name)",
    "CREATE UNIQUE INDEX ns_group_ordinals ON ns_aliases(group_key,ordinal)",
    "CREATE INDEX ns_inode_aliases ON ns_aliases(inode,reverse_key)",
    "CREATE INDEX ns_master_aliases ON ns_aliases(inode,parent_dir_key,name)",
    "CREATE TABLE ns_directories (inode BLOB PRIMARY KEY NOT NULL CHECK(length(inode)=8), dir_key BLOB NOT NULL UNIQUE CHECK(length(dir_key)=32), child_dirs INTEGER NOT NULL DEFAULT 0 CHECK(child_dirs BETWEEN 0 AND 4294967295), reached INTEGER NOT NULL DEFAULT 0 CHECK(reached IN(0,1)), expanded INTEGER NOT NULL DEFAULT 0 CHECK(expanded IN(0,1))) WITHOUT ROWID",
    "CREATE INDEX ns_directory_work ON ns_directories(reached,expanded,inode)",
    "CREATE TABLE ns_directory_edges (parent BLOB NOT NULL CHECK(length(parent)=8), child BLOB PRIMARY KEY NOT NULL CHECK(length(child)=8)) WITHOUT ROWID",
    "CREATE INDEX ns_directory_children ON ns_directory_edges(parent,child)",
];

fn leaf_value(record: &SnapshotIndexRecord) -> PackedResult<&[u8]> {
    match &record.value {
        SnapshotIndexValue::Leaf(value) => Ok(value),
        _ => Err(invalid("namespace relation requires an authenticated leaf")),
    }
}
fn inode_key(record: &SnapshotIndexRecord) -> PackedResult<u64> {
    if record.first_key != record.last_key {
        return Err(invalid("canonical namespace inode has interval fences"));
    }
    let inode = be8(&record.first_key)?;
    if inode == 0 || inode > i64::MAX as u64 {
        return Err(invalid("namespace inode outside supported range"));
    }
    Ok(inode)
}

impl SemanticFacts {
    /// The snapshot and all context leaves already have admitted owners.
    /// Work stays private and disposable; interruption poisons the entire facts
    /// instance through the existing compound-operation guard.
    pub(in super::super) async fn audit_namespace_relations(
        &mut self,
        snapshot: &AuthenticatedSnapshot,
        contexts: NamespaceContexts,
    ) -> PackedResult<NamespaceSummary> {
        self.begin()?;
        let result = self.namespace_inner(snapshot, contexts).await;
        self.end(result)
    }

    async fn namespace_context(
        &mut self,
        context: ContextId,
        role: SemanticRole,
        reference: &SnapshotObjectRef,
    ) -> PackedResult<()> {
        let length = reference.object_len.to_le_bytes();
        if !self.exists(|| sea_orm::sqlx::query("SELECT 1 FROM contexts c JOIN objects o ON o.key=c.root_key WHERE c.id=? AND c.role=? AND c.finished=1 AND c.root_key=? AND o.kind=? AND o.object_len=? AND o.digest=? AND o.authenticated=1 LIMIT 1")
            .bind(context.0).bind(role.tag()).bind(reference.key.as_bytes()).bind(reference.kind as i64).bind(length.as_slice()).bind(reference.digest.as_slice())).await? {
            return Err(invalid("namespace audit context does not bind its completed authenticated manifest root"));
        }
        Ok(())
    }
    async fn namespace_leaf(
        &mut self,
        context: ContextId,
        after: &[u8],
    ) -> PackedResult<Option<OwnedSemanticRow<SnapshotIndexRecord>>> {
        self.row(|| sea_orm::sqlx::query("SELECT first_key,last_key,value FROM context_leaves WHERE context_id=? AND first_key>? ORDER BY first_key LIMIT 1").bind(context.0).bind(after), |row| {
            Ok(SnapshotIndexRecord { first_key: column(row, 0)?, last_key: column(row, 1)?, value: SnapshotIndexValue::Leaf(column(row, 2)?) })
        }).await
    }
    async fn namespace_inner(
        &mut self,
        snapshot: &AuthenticatedSnapshot,
        contexts: NamespaceContexts,
    ) -> PackedResult<NamespaceSummary> {
        let manifest = snapshot.manifest();
        // Binding comes from the authenticated header, including its optional
        // source/placement contract. Never infer a contract from existing rows.
        for (context, role, ordinal) in [
            (contexts.groups, SemanticRole::Groups, 0usize),
            (contexts.inodes, SemanticRole::Inodes, 1),
            (contexts.reverse, SemanticRole::Reverse, 5),
            (contexts.selectors, SemanticRole::NamespaceSelectors, 6),
        ] {
            self.namespace_context(context, role, &manifest.roots[ordinal])
                .await?;
        }
        match (&manifest.source, contexts.source) {
            (Some(source), Some(context)) => {
                self.namespace_context(
                    context,
                    SemanticRole::SourceAllocations,
                    &source.allocations,
                )
                .await?
            }
            (None, None) => {}
            _ => {
                return Err(invalid(
                    "namespace source context disagrees with authenticated manifest",
                ));
            }
        }
        for statement in NAMESPACE_SCHEMA {
            self.execute(|| sea_orm::sqlx::query(statement)).await?;
        }
        let root = manifest.root_inode.to_be_bytes();
        let root_nlink = manifest
            .source
            .as_ref()
            .map(|source| i64::from(source.root.nlink));
        self.execute(|| {
            sea_orm::sqlx::query("INSERT INTO ns_root(id,inode,dir_key,nlink) VALUES(1,?,?,?)")
                .bind(root.as_slice())
                .bind(manifest.root_dir_key.as_slice())
                .bind(root_nlink)
        })
        .await?;
        self.execute(|| {
            sea_orm::sqlx::query("INSERT INTO ns_directories(inode,dir_key,reached) VALUES(?,?,1)")
                .bind(root.as_slice())
                .bind(manifest.root_dir_key.as_slice())
        })
        .await?;
        let mut summary = NamespaceSummary {
            highest_inode: manifest.root_inode,
            directories: 1,
            ..NamespaceSummary::default()
        };
        // One <=2048-byte keyset cursor is reused, never a namespace collection.
        let _cursor_owner = self.budget.admit(&[(SnapshotBudgetPool::Metadata, 4096)])?;
        let mut cursor = Vec::with_capacity(MAX_FENCE_BYTES);
        while let Some(record) = self.namespace_leaf(contexts.groups, &cursor).await? {
            let _decoded = self
                .budget
                .admit(&[(SnapshotBudgetPool::Metadata, 32 << 10)])?;
            let raw = leaf_value(&record)?;
            let group = SnapshotGroupRef::decode_value(raw)?;
            // IP06 group weight validation also checks both complete fences.
            record.subtree_weight(SnapshotObjectKind::GroupIndex)?;
            self.execute(|| {
                sea_orm::sqlx::query(
                    "INSERT INTO ns_groups(group_key,value,entry_count) VALUES(?,?,?)",
                )
                .bind(record.first_key.as_slice())
                .bind(raw)
                .bind(i64::from(group.entry_count))
            })
            .await?;
            summary.groups = summary
                .groups
                .checked_add(1)
                .ok_or_else(|| limit("namespace group count overflow"))?;
            cursor.clear();
            cursor.extend_from_slice(&record.first_key);
        }
        cursor.clear();
        while let Some(record) = self.namespace_leaf(contexts.inodes, &cursor).await? {
            summary.highest_inode = summary.highest_inode.max(inode_key(&record)?);
            let _decoded = self
                .budget
                .admit(&[(SnapshotBudgetPool::Metadata, 32 << 10)])?;
            let inode = inode_key(&record)?;
            let raw = leaf_value(&record)?;
            let location = SnapshotInodeLocation::decode_value(raw)?;
            if location.hot.inode != inode || inode == manifest.root_inode {
                return Err(invalid("canonical index contains root or mismatched inode"));
            }
            let zero = 0u64.to_be_bytes();
            self.execute(|| sea_orm::sqlx::query("INSERT INTO ns_canonical(inode,value,kind,nlink,alias_count) VALUES(?,?,?,?,?)")
                .bind(record.first_key.as_slice()).bind(raw).bind(i64::from(location.hot.kind)).bind(i64::from(location.hot.nlink)).bind(zero.as_slice())).await?;
            if location.hot.kind == 2 {
                let key = directory_key(manifest.snapshot_id, inode);
                if key == manifest.root_dir_key {
                    return Err(invalid(
                        "non-root directory collides with actual manifest root key",
                    ));
                }
                self.execute(|| {
                    sea_orm::sqlx::query("INSERT INTO ns_directories(inode,dir_key) VALUES(?,?)")
                        .bind(record.first_key.as_slice())
                        .bind(key.as_slice())
                })
                .await?;
                summary.directories = summary
                    .directories
                    .checked_add(1)
                    .ok_or_else(|| limit("namespace directory count overflow"))?;
            }
            summary.canonical_inodes = summary
                .canonical_inodes
                .checked_add(1)
                .ok_or_else(|| limit("namespace inode count overflow"))?;
            cursor.clear();
            cursor.extend_from_slice(&record.first_key);
        }
        cursor.clear();
        while let Some(record) = self.namespace_leaf(contexts.reverse, &cursor).await? {
            // Own both typed decodes and their group/reverse-key encodings
            // before allocation. SQL rows retain their independent owners.
            let _decoded = self
                .budget
                .admit(&[(SnapshotBudgetPool::Metadata, 64 << 10)])?;
            let raw = leaf_value(&record)?;
            let alias = SnapshotInodeLocation::decode_value(raw)?;
            let key = alias.reverse_key();
            let inode = alias.hot.inode.to_be_bytes();
            let parent = alias.hot.parent_inode.to_be_bytes();
            if alias.hot.inode == manifest.root_inode
                || record.first_key != key
                || record.last_key != key
            {
                return Err(invalid(
                    "Reverse index contains root or mismatched alias key",
                ));
            }
            let canonical = self
                .row(
                    || {
                        sea_orm::sqlx::query(
                            "SELECT value,alias_count FROM ns_canonical WHERE inode=? LIMIT 1",
                        )
                        .bind(inode.as_slice())
                    },
                    |row| {
                        let raw: Vec<u8> = column(row, 0)?;
                        let count: Vec<u8> = column(row, 1)?;
                        Ok((raw, be8(&count)?))
                    },
                )
                .await?
                .ok_or_else(|| invalid("Reverse alias has no canonical inode"))?;
            let canonical_location = SnapshotInodeLocation::decode_value(&canonical.0)?;
            if !alias.same_inode_attributes(&canonical_location) {
                return Err(invalid(
                    "canonical and Reverse alias hot attributes disagree",
                ));
            }
            let links = canonical
                .1
                .checked_add(1)
                .ok_or_else(|| limit("namespace alias count overflow"))?;
            let links_blob = links.to_be_bytes();
            if alias.hot.kind == 2 && links != 1 {
                return Err(invalid("directory has multiple visible parents"));
            }
            if alias.hot.kind != 2 && links > u64::from(alias.hot.nlink) {
                return Err(invalid("visible aliases exceed inode nlink"));
            }
            drop(canonical);
            if !self
                .exists(|| {
                    sea_orm::sqlx::query(
                        "SELECT 1 FROM ns_directories WHERE inode=? AND dir_key=? LIMIT 1",
                    )
                    .bind(parent.as_slice())
                    .bind(alias.hot.parent_dir_key.as_slice())
                })
                .await?
            {
                return Err(invalid(
                    "alias parent is missing/non-directory or has wrong actual directory key",
                ));
            }
            let mut group_key = alias.group.parent_dir_key.to_vec();
            group_key.extend_from_slice(&alias.group.first_name);
            let group_raw = alias.group.encode_value()?;
            let group = self.row(|| sea_orm::sqlx::query("SELECT entry_count,observed FROM ns_groups WHERE group_key=? AND value=? LIMIT 1").bind(group_key.as_slice()).bind(group_raw.as_slice()), |row| {
                Ok((column::<i64>(row, 0)?, column::<i64>(row, 1)?))
            }).await?.ok_or_else(|| invalid("IL05 GroupRef differs from Groups index"))?;
            if i64::from(alias.hot.entry_ordinal) >= group.0 || group.1 >= group.0 {
                return Err(invalid(
                    "alias group ordinal/count exceeds authenticated group",
                ));
            }
            let observed = group.1 + 1;
            drop(group);
            if self
                .exists(|| {
                    sea_orm::sqlx::query(
                        "SELECT 1 FROM ns_aliases WHERE parent=? AND name=? LIMIT 1",
                    )
                    .bind(parent.as_slice())
                    .bind(alias.hot.name.as_slice())
                })
                .await?
            {
                return Err(invalid("directory/name has multiple inode aliases"));
            }
            if self
                .exists(|| {
                    sea_orm::sqlx::query(
                        "SELECT 1 FROM ns_aliases WHERE group_key=? AND ordinal=? LIMIT 1",
                    )
                    .bind(group_key.as_slice())
                    .bind(i64::from(alias.hot.entry_ordinal))
                })
                .await?
            {
                return Err(invalid("authenticated group ordinal has multiple aliases"));
            }
            self.execute(|| sea_orm::sqlx::query("INSERT INTO ns_aliases(reverse_key,inode,parent,parent_dir_key,name,group_key,ordinal,value) VALUES(?,?,?,?,?,?,?,?)")
                .bind(key.as_slice()).bind(inode.as_slice()).bind(parent.as_slice()).bind(alias.hot.parent_dir_key.as_slice()).bind(alias.hot.name.as_slice()).bind(group_key.as_slice()).bind(i64::from(alias.hot.entry_ordinal)).bind(raw)).await?;
            self.execute(|| {
                sea_orm::sqlx::query("UPDATE ns_groups SET observed=? WHERE group_key=?")
                    .bind(observed)
                    .bind(group_key.as_slice())
            })
            .await?;
            self.execute(|| {
                sea_orm::sqlx::query("UPDATE ns_canonical SET alias_count=? WHERE inode=?")
                    .bind(links_blob.as_slice())
                    .bind(inode.as_slice())
            })
            .await?;
            if alias.hot.kind == 2 {
                self.execute(|| {
                    sea_orm::sqlx::query("INSERT INTO ns_directory_edges(parent,child) VALUES(?,?)")
                        .bind(parent.as_slice())
                        .bind(inode.as_slice())
                })
                .await?;
                self.execute(|| {
                    sea_orm::sqlx::query(
                        "UPDATE ns_directories SET child_dirs=child_dirs+1 WHERE inode=?",
                    )
                    .bind(parent.as_slice())
                })
                .await?;
            }
            summary.aliases = summary
                .aliases
                .checked_add(1)
                .ok_or_else(|| limit("namespace alias count overflow"))?;
            cursor.clear();
            cursor.extend_from_slice(&record.first_key);
        }
        if summary.aliases != manifest.group_dentry_count {
            return Err(invalid(
                "Reverse alias count differs from authenticated Groups dentry weight",
            ));
        }
        // Both directions are checked against actual staged facts. Mutable
        // counters are internal summaries, never producer-supplied authority.
        for statement in [
            "SELECT 1 FROM ns_groups WHERE observed<>entry_count LIMIT 1",
            "SELECT 1 FROM ns_canonical c WHERE c.value IS NOT (SELECT a.value FROM ns_aliases a INDEXED BY ns_master_aliases WHERE a.inode=c.inode ORDER BY a.parent_dir_key,a.name LIMIT 1) LIMIT 1",
            "SELECT 1 FROM ns_aliases a LEFT JOIN ns_canonical c ON c.inode=a.inode WHERE c.inode IS NULL LIMIT 1",
            "SELECT 1 FROM ns_canonical c WHERE c.kind=2 AND c.alias_count<>x'0000000000000001' LIMIT 1",
            "SELECT 1 FROM ns_directories d LEFT JOIN ns_canonical c ON c.inode=d.inode LEFT JOIN ns_root r ON r.inode=d.inode WHERE (r.inode IS NULL AND (c.inode IS NULL OR c.kind<>2)) OR (r.inode IS NOT NULL AND c.inode IS NOT NULL) LIMIT 1",
        ] {
            if self.exists(|| sea_orm::sqlx::query(statement)).await? {
                return Err(invalid(
                    "canonical/group/directory namespace relation failed",
                ));
            }
        }
        // Directory/root nlink is preserved from authenticated attributes;
        // wire 005 does not declare a universal 2+children policy. Exactly one
        // visible incoming alias is checked separately for non-root directories.
        // Non-directory nlink uses u64 blobs to avoid SQLite integer narrowing.
        cursor.clear();
        loop {
            let next = self.row(|| sea_orm::sqlx::query("SELECT inode,nlink,alias_count FROM ns_canonical WHERE kind<>2 AND inode>? ORDER BY inode LIMIT 1").bind(cursor.as_slice()), |row| {
                let inode: Vec<u8> = column(row, 0)?; let nlink: i64 = column(row, 1)?; let aliases: Vec<u8> = column(row, 2)?;
                Ok((inode, nlink, be8(&aliases)?))
            }).await?;
            let Some(next) = next else {
                break;
            };
            if next.1 as u64 != next.2 {
                return Err(invalid(
                    "snapshot nlink differs from visible Reverse aliases",
                ));
            }
            cursor.clear();
            cursor.extend_from_slice(&next.0);
        }
        if let Some(source) = contexts.source {
            for statement in [
                "SELECT 1 FROM ns_canonical c LEFT JOIN context_leaves s ON s.context_id=? AND s.first_key=c.inode WHERE s.first_key IS NULL LIMIT 1",
                "SELECT 1 FROM context_leaves s LEFT JOIN ns_canonical c ON c.inode=s.first_key WHERE s.context_id=? AND c.inode IS NULL LIMIT 1",
            ] {
                if self
                    .exists(|| sea_orm::sqlx::query(statement).bind(source.0))
                    .await?
                {
                    return Err(invalid(
                        "SI05 does not exactly cover non-root canonical inodes",
                    ));
                }
            }
            cursor.clear();
            while let Some(record) = self.namespace_leaf(source, &cursor).await? {
                let _decoded = self
                    .budget
                    .admit(&[(SnapshotBudgetPool::Metadata, 32 << 10)])?;
                let inode = inode_key(&record)?;
                super::super::super::source_stat::decode_allocation(leaf_value(&record)?, inode)?;
                summary.source_records = summary
                    .source_records
                    .checked_add(1)
                    .ok_or_else(|| limit("namespace allocation count overflow"))?;
                cursor.clear();
                cursor.extend_from_slice(&record.first_key);
            }
            if summary.source_records != summary.canonical_inodes {
                return Err(invalid(
                    "typed SI05 count differs from canonical non-root inode count",
                ));
            }
        }
        let placement_required = manifest
            .source
            .as_ref()
            .is_some_and(|source| source.placement_contract);
        if placement_required {
            if self.exists(|| sea_orm::sqlx::query("SELECT 1 FROM ns_canonical c LEFT JOIN context_leaves s ON s.context_id=? AND s.first_key=c.inode WHERE c.kind=1 AND s.first_key IS NULL LIMIT 1").bind(contexts.selectors.0)).await? { return Err(invalid("PS09 is missing a canonical regular inode")); }
            if self.exists(|| sea_orm::sqlx::query("SELECT 1 FROM context_leaves s LEFT JOIN ns_canonical c ON c.inode=s.first_key WHERE s.context_id=? AND (c.inode IS NULL OR c.kind<>1) LIMIT 1").bind(contexts.selectors.0)).await? { return Err(invalid("PS09 contains a non-regular or non-canonical inode")); }
            cursor.clear();
            while let Some(record) = self.namespace_leaf(contexts.selectors, &cursor).await? {
                let _decoded = self
                    .budget
                    .admit(&[(SnapshotBudgetPool::Metadata, 64 << 10)])?;
                let inode = inode_key(&record)?;
                let canonical = self
                    .row(
                        || {
                            sea_orm::sqlx::query(
                                "SELECT value FROM ns_canonical WHERE inode=? AND kind=1 LIMIT 1",
                            )
                            .bind(record.first_key.as_slice())
                        },
                        |row| column::<Vec<u8>>(row, 0),
                    )
                    .await?
                    .ok_or_else(|| invalid("typed PS09 selector has no canonical regular inode"))?;
                let location = SnapshotInodeLocation::decode_value(&canonical)?;
                if location.hot.inode != inode || location.hot.kind != 1 {
                    return Err(invalid("typed PS09 canonical identity or kind disagrees"));
                }
                SnapshotPlacement::decode(leaf_value(&record)?, inode, location.hot.size)?;
                summary.selector_records = summary
                    .selector_records
                    .checked_add(1)
                    .ok_or_else(|| limit("namespace selector count overflow"))?;
                cursor.clear();
                cursor.extend_from_slice(&record.first_key);
            }
        } else if self
            .exists(|| {
                sea_orm::sqlx::query("SELECT 1 FROM context_leaves WHERE context_id=? LIMIT 1")
                    .bind(contexts.selectors.0)
            })
            .await?
        {
            return Err(invalid(
                "PS09 leaves exist without authenticated placement contract",
            ));
        }
        // Indexed BFS reads one child at a time and marks only actual canonical
        // directory edges. Local joins alone cannot exclude an orphan cycle.
        loop {
            let work = self.row(|| sea_orm::sqlx::query("SELECT inode FROM ns_directories INDEXED BY ns_directory_work WHERE reached=1 AND expanded=0 ORDER BY inode LIMIT 1"), |row| column::<Vec<u8>>(row, 0)).await?;
            let Some(work) = work else {
                break;
            };
            let mut after = 0u64.to_be_bytes();
            loop {
                let child = self.row(|| sea_orm::sqlx::query("SELECT e.child,d.reached FROM ns_directory_edges e INDEXED BY ns_directory_children LEFT JOIN ns_directories d ON d.inode=e.child WHERE e.parent=? AND e.child>? ORDER BY e.child LIMIT 1").bind(work.as_slice()).bind(after.as_slice()), |row| {
                    let child: Vec<u8> = column(row, 0)?; let reached: Option<i64> = column(row, 1)?;
                    Ok((child, reached))
                }).await?;
                let Some(child) = child else {
                    break;
                };
                if child.1 != Some(0) {
                    return Err(invalid(
                        "directory reachability revisits a node or has missing child facts",
                    ));
                }
                let affected = self
                    .execute(|| {
                        sea_orm::sqlx::query(
                            "UPDATE ns_directories SET reached=1 WHERE inode=? AND reached=0",
                        )
                        .bind(child.0.as_slice())
                    })
                    .await?;
                if affected != 1 {
                    return Err(invalid("directory reachability lost its child row"));
                }
                after = child
                    .0
                    .as_slice()
                    .try_into()
                    .map_err(|_| invalid("directory child inode blob is not BE8"))?;
            }
            let affected = self.execute(|| sea_orm::sqlx::query("UPDATE ns_directories SET expanded=1 WHERE inode=? AND reached=1 AND expanded=0").bind(work.as_slice())).await?;
            if affected != 1 {
                return Err(invalid("directory expansion lost its row"));
            }
        }
        if self
            .exists(|| {
                sea_orm::sqlx::query(
                    "SELECT 1 FROM ns_directories WHERE reached=0 OR expanded=0 LIMIT 1",
                )
            })
            .await?
        {
            return Err(invalid(
                "directory is unreachable from authenticated root (orphan/cycle)",
            ));
        }
        let root_dirs = self
            .row(
                || {
                    sea_orm::sqlx::query(
                        "SELECT child_dirs FROM ns_directories WHERE inode=? LIMIT 1",
                    )
                    .bind(root.as_slice())
                },
                |row| column::<i64>(row, 0),
            )
            .await?
            .ok_or_else(|| invalid("root directory facts missing"))?;
        summary.root_child_directories = u64::try_from(*root_dirs)
            .map_err(|_| invalid("root child directory count negative"))?;
        self.live()?;
        Ok(summary)
    }
}
