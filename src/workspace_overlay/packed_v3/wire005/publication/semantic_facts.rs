//! Private, disposable index facts. These facts never grant catalog authority.
//! Physical identities are deduplicated; incoming semantic occurrences are not.

mod group_content;
pub(super) use group_content::{ContentContexts, GroupContentLimits};

mod container_occurrences;
pub(super) use container_occurrences::{ContainerContexts, ContainerOccurrenceLimits};

mod namespace;
pub(super) use namespace::NamespaceContexts;

use super::super::index::{IndexPageClaim, validate_child_claim};
use super::inventory::{
    InventoryLimits, InventoryStats, OwnedSqlRow, PhysicalInventory, Registration, SemanticVmWork,
};
use crate::workspace_overlay::packed_v3::wire::{PackedResult, PackedWireError};
use crate::workspace_overlay::packed_v3::wire005::{
    V3IndexPage, V3IndexRecord, V3IndexValue, V3MountBudget, V3ObjectKind, V3ObjectRef,
    V3OwnedPermit,
};
use sea_orm::sqlx::{
    Row, Sqlite,
    query::Query,
    sqlite::{SqliteArguments, SqliteRow},
};
use std::ops::Deref;
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio_util::sync::CancellationToken;

const MAX_OWNED_ROWS: usize = 4;
const MAX_FENCE_BYTES: usize = 2048;
const MAX_VALUE_BYTES: usize = 8192;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SemanticRole {
    Groups,
    Inodes,
    Containers,
    Frames,
    Cold,
    Reverse,
    NamespaceSelectors,
    SourceAllocations,
    ExternalExtents { inode: u64, eof: u64 },
}

impl SemanticRole {
    fn tag(self) -> i64 {
        match self {
            Self::Groups => 0,
            Self::Inodes => 1,
            Self::Containers => 2,
            Self::Frames => 3,
            Self::Cold => 4,
            Self::Reverse => 5,
            Self::NamespaceSelectors => 6,
            Self::SourceAllocations => 7,
            Self::ExternalExtents { .. } => 8,
        }
    }
    pub(super) fn kind(self) -> V3ObjectKind {
        match self {
            Self::Groups => V3ObjectKind::GroupIndex,
            Self::Inodes => V3ObjectKind::InodeIndex,
            Self::Containers => V3ObjectKind::ContainerIndex,
            Self::Frames => V3ObjectKind::FrameIndex,
            Self::Cold => V3ObjectKind::ColdIndex,
            Self::Reverse => V3ObjectKind::ReverseIndex,
            Self::NamespaceSelectors | Self::ExternalExtents { .. } => V3ObjectKind::LargeIndex,
            Self::SourceAllocations => V3ObjectKind::SourceStatsIndex,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ContextId(pub(super) i64);
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct VisitId(pub(super) i64);

#[derive(Debug, Eq, PartialEq)]
pub(super) struct ParentClaim {
    pub height: u8,
    pub first: Vec<u8>,
    pub last: Vec<u8>,
    pub weight: u64,
}
#[derive(Debug, Eq, PartialEq)]
pub(super) struct PageVisit {
    pub id: VisitId,
    pub context: ContextId,
    pub role: SemanticRole,
    pub reference: V3ObjectRef,
    pub parent: Option<(VisitId, u16)>,
    pub expected: Option<ParentClaim>,
}
#[derive(Debug, Eq, PartialEq)]
pub(super) struct PageFact {
    pub kind: V3ObjectKind,
    pub height: u8,
    pub count: u16,
    pub first: Option<Vec<u8>>,
    pub last: Option<Vec<u8>>,
    pub weight: u64,
}
#[derive(Debug, Eq, PartialEq)]
pub(super) struct PageRecord {
    pub slot: u16,
    pub record: V3IndexRecord,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct ContextStats {
    pub leaf_count: u64,
    pub leaf_weight: u64,
    pub finished: bool,
}

struct RowSlot(Arc<AtomicUsize>);
impl Drop for RowSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(super) struct OwnedSemanticRow<T> {
    value: T,
    _permit: V3OwnedPermit,
    _slot: RowSlot,
}
impl<T> Deref for OwnedSemanticRow<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct SemanticFactLimits {
    pub inventory: InventoryLimits,
    pub max_contexts: u64,
    pub max_visits: u64,
    pub max_leaves: u64,
    pub max_page_records: u64,
    pub max_sql_operations: u64,
    pub max_sql_vm_steps: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct SemanticSummary {
    pub contexts: u64,
    pub visits: u64,
    pub leaves: u64,
    pub page_records: u64,
    pub sql_operations: u64,
    pub sql_vm_steps: u64,
    pub inventory: InventoryStats,
}

pub(super) struct SemanticFacts {
    inventory: PhysicalInventory,
    budget: Arc<V3MountBudget>,
    cancel: CancellationToken,
    limits: SemanticFactLimits,
    summary: SemanticSummary,
    vm: Arc<SemanticVmWork>,
    rows: Arc<AtomicUsize>,
    busy: bool,
    tainted: bool,
}

fn invalid(message: &str) -> PackedWireError {
    PackedWireError::Invalid(message.into())
}
fn limit(message: &str) -> PackedWireError {
    PackedWireError::LimitExceeded(message.into())
}
fn column<T>(row: &SqliteRow, index: usize) -> PackedResult<T>
where
    for<'r> T: sea_orm::sqlx::Decode<'r, Sqlite> + sea_orm::sqlx::Type<Sqlite>,
{
    row.try_get(index)
        .map_err(|_| invalid("invalid semantic facts row"))
}
fn be8(value: &[u8]) -> PackedResult<u64> {
    Ok(u64::from_be_bytes(
        value
            .try_into()
            .map_err(|_| invalid("invalid semantic u64 blob"))?,
    ))
}
fn role(row: &SqliteRow, start: usize) -> PackedResult<SemanticRole> {
    let tag: i64 = column(row, start)?;
    let inode: Option<Vec<u8>> = column(row, start + 1)?;
    let eof: Option<Vec<u8>> = column(row, start + 2)?;
    if tag != 8 && (inode.is_some() || eof.is_some()) {
        return Err(invalid("namespace context carries External identity"));
    }
    Ok(match tag {
        0 => SemanticRole::Groups,
        1 => SemanticRole::Inodes,
        2 => SemanticRole::Containers,
        3 => SemanticRole::Frames,
        4 => SemanticRole::Cold,
        5 => SemanticRole::Reverse,
        6 => SemanticRole::NamespaceSelectors,
        7 => SemanticRole::SourceAllocations,
        8 => SemanticRole::ExternalExtents {
            inode: be8(&inode.ok_or_else(|| invalid("External context has no inode"))?)?,
            eof: be8(&eof.ok_or_else(|| invalid("External context has no EOF"))?)?,
        },
        _ => return Err(invalid("unknown semantic context role")),
    })
}
fn object(row: &SqliteRow, start: usize) -> PackedResult<V3ObjectRef> {
    let key: Vec<u8> = column(row, start)?;
    let kind: i64 = column(row, start + 1)?;
    let length: Vec<u8> = column(row, start + 2)?;
    let digest: Vec<u8> = column(row, start + 3)?;
    let reference = V3ObjectRef {
        key: String::from_utf8(key).map_err(|_| invalid("semantic object key is not UTF-8"))?,
        kind: V3ObjectKind::from_u8(
            kind.try_into()
                .map_err(|_| invalid("semantic object kind"))?,
        )?,
        object_len: u64::from_le_bytes(
            length
                .try_into()
                .map_err(|_| invalid("semantic physical length"))?,
        ),
        digest: digest
            .try_into()
            .map_err(|_| invalid("semantic object digest"))?,
    };
    reference.encode_value()?;
    Ok(reference)
}
fn fact(row: &SqliteRow) -> PackedResult<PageFact> {
    let kind: i64 = column(row, 0)?;
    let height: i64 = column(row, 1)?;
    let count: i64 = column(row, 2)?;
    let weight: Vec<u8> = column(row, 5)?;
    Ok(PageFact {
        kind: V3ObjectKind::from_u8(kind.try_into().map_err(|_| invalid("semantic page kind"))?)?,
        height: height
            .try_into()
            .map_err(|_| invalid("semantic page height"))?,
        count: count
            .try_into()
            .map_err(|_| invalid("semantic page count"))?,
        first: column(row, 3)?,
        last: column(row, 4)?,
        weight: be8(&weight)?,
    })
}
fn page_record(row: &SqliteRow) -> PackedResult<PageRecord> {
    let slot: i64 = column(row, 0)?;
    let first_key = column(row, 1)?;
    let last_key = column(row, 2)?;
    let leaf: Option<Vec<u8>> = column(row, 3)?;
    let child: Option<Vec<u8>> = column(row, 4)?;
    let weight: Vec<u8> = column(row, 5)?;
    let value = match (leaf, child) {
        (Some(value), None) => V3IndexValue::Leaf(value),
        (None, Some(_)) => V3IndexValue::Child {
            reference: object(row, 6)?,
            subtree_weight: be8(&weight)?,
        },
        _ => return Err(invalid("semantic record has no unique leaf/child shape")),
    };
    Ok(PageRecord {
        slot: slot
            .try_into()
            .map_err(|_| invalid("semantic record slot"))?,
        record: V3IndexRecord {
            first_key,
            last_key,
            value,
        },
    })
}
fn visit(row: &SqliteRow) -> PackedResult<PageVisit> {
    let parent: Option<i64> = column(row, 2)?;
    let slot: Option<i64> = column(row, 3)?;
    let height: Option<i64> = column(row, 4)?;
    let first: Option<Vec<u8>> = column(row, 5)?;
    let last: Option<Vec<u8>> = column(row, 6)?;
    let weight: Option<Vec<u8>> = column(row, 7)?;
    let (parent, expected) = match (parent, slot, height, first, last, weight) {
        (None, None, None, None, None, None) => (None, None),
        (Some(parent), Some(slot), Some(height), Some(first), Some(last), Some(weight)) => (
            Some((
                VisitId(parent),
                slot.try_into()
                    .map_err(|_| invalid("semantic parent slot"))?,
            )),
            Some(ParentClaim {
                height: height
                    .try_into()
                    .map_err(|_| invalid("semantic parent height"))?,
                first,
                last,
                weight: be8(&weight)?,
            }),
        ),
        _ => return Err(invalid("semantic visit parent claim is incomplete")),
    };
    Ok(PageVisit {
        id: VisitId(column(row, 0)?),
        context: ContextId(column(row, 1)?),
        parent,
        expected,
        role: role(row, 8)?,
        reference: object(row, 11)?,
    })
}

const SCHEMA: &[&str] = &[
    "CREATE TABLE contexts (id INTEGER PRIMARY KEY, role INTEGER NOT NULL CHECK(role BETWEEN 0 AND 8), root_key BLOB NOT NULL, inode BLOB, eof BLOB, expected_weight BLOB CHECK(expected_weight IS NULL OR length(expected_weight)=8), leaf_count BLOB NOT NULL CHECK(length(leaf_count)=8), leaf_weight BLOB NOT NULL CHECK(length(leaf_weight)=8), finished INTEGER NOT NULL DEFAULT 0 CHECK(finished IN(0,1)), CHECK((role=8 AND inode IS NOT NULL AND eof IS NOT NULL AND length(inode)=8 AND length(eof)=8) OR (role<>8 AND inode IS NULL AND eof IS NULL)))",
    "CREATE UNIQUE INDEX namespace_context ON contexts(role) WHERE role<>8",
    "CREATE UNIQUE INDEX external_context ON contexts(inode) WHERE role=8",
    "CREATE INDEX unfinished_contexts ON contexts(finished,id)",
    "CREATE TABLE visits (id INTEGER PRIMARY KEY, context_id INTEGER NOT NULL, object_key BLOB NOT NULL, parent_id INTEGER, parent_slot INTEGER, expected_height INTEGER, first_key BLOB, last_key BLOB, expected_weight BLOB, checked INTEGER NOT NULL DEFAULT 0 CHECK(checked IN(0,1)), processed INTEGER NOT NULL DEFAULT 0 CHECK(processed IN(0,1)), CHECK((parent_id IS NULL AND parent_slot IS NULL AND expected_height IS NULL AND first_key IS NULL AND last_key IS NULL AND expected_weight IS NULL) OR (parent_id IS NOT NULL AND parent_slot IS NOT NULL AND parent_slot BETWEEN 0 AND 1023 AND expected_height IS NOT NULL AND expected_height BETWEEN 0 AND 15 AND first_key IS NOT NULL AND last_key IS NOT NULL AND length(first_key) BETWEEN 1 AND 2048 AND length(last_key) BETWEEN 1 AND 2048 AND expected_weight IS NOT NULL AND length(expected_weight)=8)))",
    "CREATE UNIQUE INDEX root_visit ON visits(context_id) WHERE parent_id IS NULL",
    "CREATE UNIQUE INDEX incoming_visit ON visits(parent_id,parent_slot) WHERE parent_id IS NOT NULL",
    "CREATE INDEX pending_visits ON visits(processed,id)",
    "CREATE INDEX context_visits ON visits(context_id,processed,id)",
    "CREATE TABLE page_facts (object_key BLOB PRIMARY KEY NOT NULL, kind INTEGER NOT NULL CHECK(kind BETWEEN 5 AND 12), height INTEGER NOT NULL CHECK(height BETWEEN 0 AND 16), record_count INTEGER NOT NULL CHECK(record_count BETWEEN 0 AND 1024), first_key BLOB, last_key BLOB, weight BLOB NOT NULL CHECK(length(weight)=8), complete INTEGER NOT NULL DEFAULT 0 CHECK(complete IN(0,1)), CHECK((record_count=0 AND height=0 AND first_key IS NULL AND last_key IS NULL) OR (record_count>0 AND first_key IS NOT NULL AND last_key IS NOT NULL AND length(first_key) BETWEEN 1 AND 2048 AND length(last_key) BETWEEN 1 AND 2048))) WITHOUT ROWID",
    "CREATE TABLE page_records (object_key BLOB NOT NULL, slot INTEGER NOT NULL CHECK(slot BETWEEN 0 AND 1023), first_key BLOB NOT NULL CHECK(length(first_key) BETWEEN 1 AND 2048), last_key BLOB NOT NULL CHECK(length(last_key) BETWEEN 1 AND 2048), leaf BLOB, child_key BLOB, weight BLOB NOT NULL CHECK(length(weight)=8), PRIMARY KEY(object_key,slot), CHECK((leaf IS NOT NULL AND length(leaf)<=8192 AND child_key IS NULL) OR (leaf IS NULL AND child_key IS NOT NULL AND length(child_key) BETWEEN 1 AND 4096))) WITHOUT ROWID",
    "CREATE TABLE context_leaves (context_id INTEGER NOT NULL, first_key BLOB NOT NULL CHECK(length(first_key) BETWEEN 1 AND 2048), last_key BLOB NOT NULL CHECK(length(last_key) BETWEEN 1 AND 2048), value BLOB NOT NULL CHECK(length(value)<=8192), visit_id INTEGER NOT NULL, slot INTEGER NOT NULL CHECK(slot BETWEEN 0 AND 1023), weight BLOB NOT NULL CHECK(length(weight)=8), PRIMARY KEY(context_id,first_key), UNIQUE(visit_id,slot)) WITHOUT ROWID",
];

impl SemanticFacts {
    pub(super) async fn create(
        parent: &Path,
        budget: Arc<V3MountBudget>,
        limits: SemanticFactLimits,
        cancel: CancellationToken,
    ) -> PackedResult<Self> {
        if limits.max_contexts == 0
            || limits.max_visits == 0
            || limits.max_leaves == 0
            || limits.max_page_records == 0
            || limits.max_sql_operations == 0
            || limits.max_sql_vm_steps == 0
            || limits.max_contexts > i64::MAX as u64
            || limits.max_visits > i64::MAX as u64
        {
            return Err(limit("invalid semantic facts quotas"));
        }
        let mut inventory =
            PhysicalInventory::create(parent, budget.clone(), limits.inventory, cancel.clone())
                .await?;
        let vm = match inventory
            .semantic_set_vm_limit(limits.max_sql_vm_steps)
            .await
        {
            Ok(vm) => vm,
            Err(error) => {
                inventory.close().await?;
                return Err(error);
            }
        };
        let mut facts = Self {
            inventory,
            budget,
            cancel,
            limits,
            summary: SemanticSummary::default(),
            vm,
            rows: Arc::new(AtomicUsize::new(0)),
            busy: false,
            tainted: false,
        };
        for statement in SCHEMA {
            if let Err(error) = facts.execute(|| sea_orm::sqlx::query(statement)).await {
                facts.close().await?;
                return Err(error);
            }
        }
        Ok(facts)
    }

    fn live(&self) -> PackedResult<()> {
        if self.tainted {
            return Err(invalid(
                "semantic facts are unusable after an interrupted operation",
            ));
        }
        if self.cancel.is_cancelled() {
            return Err(PackedWireError::Backend(
                "semantic verification cancelled".into(),
            ));
        }
        if self.budget.state().closed {
            return Err(limit("semantic mount budget closed"));
        }
        if self.vm.exhausted() {
            return Err(limit("semantic SQL VM-work quota exceeded"));
        }
        Ok(())
    }
    fn begin(&mut self) -> PackedResult<()> {
        self.live()?;
        if self.busy {
            self.tainted = true;
            return Err(invalid(
                "semantic facts contain an unfinished compound operation",
            ));
        }
        self.busy = true;
        Ok(())
    }
    fn end<T>(&mut self, result: PackedResult<T>) -> PackedResult<T> {
        if result.is_ok() {
            self.busy = false;
        } else {
            self.tainted = true;
        }
        self.map_error(result)
    }
    fn map_error<T>(&self, result: PackedResult<T>) -> PackedResult<T> {
        result.map_err(|error| {
            if self.vm.exhausted() {
                limit("semantic SQL VM-work quota exceeded")
            } else {
                error
            }
        })
    }
    fn charge_sql(&mut self, count: u64) -> PackedResult<()> {
        self.live()?;
        let next = self
            .summary
            .sql_operations
            .checked_add(count)
            .ok_or_else(|| limit("semantic SQL operation count overflow"))?;
        if next > self.limits.max_sql_operations {
            return Err(limit("semantic SQL operation quota exceeded"));
        }
        self.summary.sql_operations = next;
        Ok(())
    }
    async fn execute<'q>(
        &mut self,
        query: impl FnOnce() -> Query<'q, Sqlite, SqliteArguments<'q>>,
    ) -> PackedResult<u64> {
        self.charge_sql(1)?;
        let result = self.inventory.semantic_execute(query).await;
        self.map_error(result)
    }
    async fn row<'q, T>(
        &mut self,
        query: impl FnOnce() -> Query<'q, Sqlite, SqliteArguments<'q>>,
        decode: impl FnOnce(&SqliteRow) -> PackedResult<T>,
    ) -> PackedResult<Option<OwnedSemanticRow<T>>> {
        self.charge_sql(1)?;
        if self
            .rows
            .try_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < MAX_OWNED_ROWS).then_some(count + 1)
            })
            .is_err()
        {
            return Err(limit("semantic owned row slots exhausted"));
        }
        let slot = RowSlot(self.rows.clone());
        let result = self.inventory.semantic_fetch_optional(query).await;
        let Some(OwnedSqlRow { row, permit }) = self.map_error(result)? else {
            return Ok(None);
        };
        let decoded = decode(&row);
        drop(row);
        let value = decoded?;
        Ok(Some(OwnedSemanticRow {
            value,
            _permit: permit,
            _slot: slot,
        }))
    }
    async fn exists<'q>(
        &mut self,
        query: impl FnOnce() -> Query<'q, Sqlite, SqliteArguments<'q>>,
    ) -> PackedResult<bool> {
        Ok(self
            .row(query, |row| column::<i64>(row, 0))
            .await?
            .is_some())
    }
    async fn register_inner(&mut self, reference: &V3ObjectRef) -> PackedResult<Registration> {
        // Existing registry methods issue at most two SQL statements. Charge
        // that upper bound here; the VM callback meters their exact work.
        self.charge_sql(2)?;
        let result = self.inventory.register(reference).await;
        self.map_error(result)
    }
    pub(super) async fn register_object(
        &mut self,
        reference: &V3ObjectRef,
    ) -> PackedResult<Registration> {
        self.begin()?;
        let result = self.register_inner(reference).await;
        self.end(result)
    }
    pub(super) async fn next_unauthenticated_object(
        &mut self,
    ) -> PackedResult<Option<super::inventory::OwnedInventoryReference>> {
        self.begin()?;
        let result = async {
            self.charge_sql(1)?;
            self.inventory.next_pending().await
        }
        .await;
        self.end(result)
    }
    pub(super) async fn mark_authenticated(&mut self, reference: &V3ObjectRef) -> PackedResult<()> {
        self.begin()?;
        let result = async {
            self.charge_sql(2)?;
            self.inventory.mark_authenticated(reference).await
        }
        .await;
        self.end(result)
    }

    pub(super) async fn register_context(
        &mut self,
        context_role: SemanticRole,
        root: &V3ObjectRef,
        expected_weight: Option<u64>,
    ) -> PackedResult<ContextId> {
        self.begin()?;
        let result = self
            .register_context_inner(context_role, root, expected_weight)
            .await;
        self.end(result)
    }
    async fn register_context_inner(
        &mut self,
        context_role: SemanticRole,
        root: &V3ObjectRef,
        expected_weight: Option<u64>,
    ) -> PackedResult<ContextId> {
        if root.kind != context_role.kind() {
            return Err(invalid("context root kind disagrees with semantic role"));
        }
        let (inode, eof) = match context_role {
            SemanticRole::ExternalExtents { inode, eof } if inode != 0 => {
                (Some(inode.to_be_bytes()), Some(eof.to_be_bytes()))
            }
            SemanticRole::ExternalExtents { .. } => {
                return Err(invalid("External context inode is zero"));
            }
            _ => (None, None),
        };
        let contexts = self
            .summary
            .contexts
            .checked_add(1)
            .filter(|n| *n <= self.limits.max_contexts)
            .ok_or_else(|| limit("semantic context quota exceeded"))?;
        let visits = self
            .summary
            .visits
            .checked_add(1)
            .filter(|n| *n <= self.limits.max_visits)
            .ok_or_else(|| limit("semantic visit quota exceeded"))?;
        self.register_inner(root).await?;
        let duplicate = if let Some(inode) = inode {
            self.exists(|| {
                sea_orm::sqlx::query("SELECT 1 FROM contexts WHERE role=8 AND inode=? LIMIT 1")
                    .bind(inode.as_slice())
            })
            .await?
        } else {
            self.exists(|| {
                sea_orm::sqlx::query("SELECT 1 FROM contexts WHERE role=? AND role<>8 LIMIT 1")
                    .bind(context_role.tag())
            })
            .await?
        };
        if duplicate {
            return Err(invalid("duplicate semantic context"));
        }
        let id = ContextId(contexts as i64);
        let expected = expected_weight.map(u64::to_be_bytes);
        let zero = 0u64.to_be_bytes();
        self.execute(|| sea_orm::sqlx::query("INSERT INTO contexts(id,role,root_key,inode,eof,expected_weight,leaf_count,leaf_weight) VALUES(?,?,?,?,?,?,?,?)")
            .bind(id.0).bind(context_role.tag()).bind(root.key.as_bytes()).bind(inode.as_ref().map(|v| v.as_slice())).bind(eof.as_ref().map(|v| v.as_slice())).bind(expected.as_ref().map(|v| v.as_slice())).bind(zero.as_slice()).bind(zero.as_slice())).await?;
        self.execute(|| {
            sea_orm::sqlx::query("INSERT INTO visits(id,context_id,object_key) VALUES(?,?,?)")
                .bind(visits as i64)
                .bind(id.0)
                .bind(root.key.as_bytes())
        })
        .await?;
        self.summary.contexts = contexts;
        self.summary.visits = visits;
        Ok(id)
    }

    pub(super) async fn next_visit(&mut self) -> PackedResult<Option<OwnedSemanticRow<PageVisit>>> {
        self.begin()?;
        let result = self.row(|| sea_orm::sqlx::query("SELECT v.id,v.context_id,v.parent_id,v.parent_slot,v.expected_height,v.first_key,v.last_key,v.expected_weight,c.role,c.inode,c.eof,o.key,o.kind,o.object_len,o.digest FROM visits v INDEXED BY pending_visits JOIN contexts c ON c.id=v.context_id JOIN objects o ON o.key=v.object_key WHERE v.processed=0 ORDER BY v.id LIMIT 1"), visit).await;
        self.end(result)
    }
    async fn visit_inner(&mut self, id: VisitId) -> PackedResult<OwnedSemanticRow<PageVisit>> {
        self.row(|| sea_orm::sqlx::query("SELECT v.id,v.context_id,v.parent_id,v.parent_slot,v.expected_height,v.first_key,v.last_key,v.expected_weight,c.role,c.inode,c.eof,o.key,o.kind,o.object_len,o.digest FROM visits v JOIN contexts c ON c.id=v.context_id JOIN objects o ON o.key=v.object_key WHERE v.id=? AND v.processed=0 AND c.finished=0 LIMIT 1").bind(id.0), visit).await?.ok_or_else(|| invalid("unknown or completed semantic visit"))
    }
    async fn fact_inner(
        &mut self,
        reference: &V3ObjectRef,
    ) -> PackedResult<Option<OwnedSemanticRow<PageFact>>> {
        let length = reference.object_len.to_le_bytes();
        self.row(|| sea_orm::sqlx::query("SELECT p.kind,p.height,p.record_count,p.first_key,p.last_key,p.weight FROM page_facts p JOIN objects o ON o.key=p.object_key WHERE p.object_key=? AND p.complete=1 AND o.authenticated=1 AND o.kind=? AND o.object_len=? AND o.digest=? LIMIT 1")
            .bind(reference.key.as_bytes()).bind(reference.kind as i64).bind(length.as_slice()).bind(reference.digest.as_slice()), fact).await
    }
    pub(super) async fn page_fact(
        &mut self,
        reference: &V3ObjectRef,
    ) -> PackedResult<Option<OwnedSemanticRow<PageFact>>> {
        self.begin()?;
        let result = self.fact_inner(reference).await;
        self.end(result)
    }

    pub(super) async fn store_authenticated_page(
        &mut self,
        reference: &V3ObjectRef,
        page: &V3IndexPage,
    ) -> PackedResult<()> {
        self.begin()?;
        let result = self.store_page_inner(reference, page).await;
        self.end(result)
    }
    async fn store_page_inner(
        &mut self,
        reference: &V3ObjectRef,
        page: &V3IndexPage,
    ) -> PackedResult<()> {
        if page.kind != reference.kind
            || page.height > 16
            || page.records.len() > 1024
            || (page.height > 0 && page.records.is_empty())
        {
            return Err(invalid("semantic page shape/kind mismatch"));
        }
        if !self.register_inner(reference).await?.authenticated {
            return Err(invalid(
                "semantic page has not been physically authenticated",
            ));
        }
        if self.fact_inner(reference).await?.is_some() {
            return Err(invalid("authenticated page facts already exist"));
        }
        let records = self
            .summary
            .page_records
            .checked_add(page.records.len() as u64)
            .filter(|n| *n <= self.limits.max_page_records)
            .ok_or_else(|| limit("semantic page-record quota exceeded"))?;
        let mut previous: Option<&[u8]> = None;
        for record in &page.records {
            if record.first_key.is_empty()
                || record.first_key.len() > MAX_FENCE_BYTES
                || record.last_key.len() > MAX_FENCE_BYTES
                || record.first_key > record.last_key
                || previous.is_some_and(|last| last >= record.first_key.as_slice())
            {
                return Err(invalid("semantic page fences are invalid or overlap"));
            }
            match &record.value {
                V3IndexValue::Leaf(value) if page.height == 0 && value.len() <= MAX_VALUE_BYTES => {
                }
                V3IndexValue::Child {
                    reference: child,
                    subtree_weight,
                } if page.height > 0 && child.kind == page.kind && *subtree_weight > 0 => {
                    self.register_inner(child).await?;
                }
                _ => return Err(invalid("semantic page record shape disagrees with height")),
            }
            previous = Some(&record.last_key);
        }
        let weight = page.total_weight()?.to_be_bytes();
        let bounds = page.bounds();
        self.execute(|| sea_orm::sqlx::query("INSERT INTO page_facts(object_key,kind,height,record_count,first_key,last_key,weight) VALUES(?,?,?,?,?,?,?)")
            .bind(reference.key.as_bytes()).bind(page.kind as i64).bind(i64::from(page.height)).bind(page.records.len() as i64).bind(bounds.map(|v| v.0)).bind(bounds.map(|v| v.1)).bind(weight.as_slice())).await?;
        for (slot, record) in page.records.iter().enumerate() {
            let (leaf, child) = match &record.value {
                V3IndexValue::Leaf(v) => (Some(v.as_slice()), None),
                V3IndexValue::Child { reference, .. } => (None, Some(reference.key.as_bytes())),
            };
            let weight = record.subtree_weight(page.kind)?.to_be_bytes();
            self.execute(|| sea_orm::sqlx::query("INSERT INTO page_records(object_key,slot,first_key,last_key,leaf,child_key,weight) VALUES(?,?,?,?,?,?,?)")
                .bind(reference.key.as_bytes()).bind(slot as i64).bind(record.first_key.as_slice()).bind(record.last_key.as_slice()).bind(leaf).bind(child).bind(weight.as_slice())).await?;
        }
        self.execute(|| {
            sea_orm::sqlx::query(
                "UPDATE page_facts SET complete=1 WHERE object_key=? AND complete=0",
            )
            .bind(reference.key.as_bytes())
        })
        .await?;
        self.summary.page_records = records;
        Ok(())
    }

    async fn record_inner(
        &mut self,
        reference: &V3ObjectRef,
        after: i64,
        exact: bool,
    ) -> PackedResult<Option<OwnedSemanticRow<PageRecord>>> {
        let length = reference.object_len.to_le_bytes();
        let statement = if exact {
            "SELECT r.slot,r.first_key,r.last_key,r.leaf,r.child_key,r.weight,o.key,o.kind,o.object_len,o.digest FROM page_records r JOIN page_facts p ON p.object_key=r.object_key JOIN objects own ON own.key=r.object_key LEFT JOIN objects o ON o.key=r.child_key WHERE r.object_key=? AND r.slot=? AND p.complete=1 AND own.authenticated=1 AND own.kind=? AND own.object_len=? AND own.digest=? LIMIT 1"
        } else {
            "SELECT r.slot,r.first_key,r.last_key,r.leaf,r.child_key,r.weight,o.key,o.kind,o.object_len,o.digest FROM page_records r JOIN page_facts p ON p.object_key=r.object_key JOIN objects own ON own.key=r.object_key LEFT JOIN objects o ON o.key=r.child_key WHERE r.object_key=? AND r.slot>? AND p.complete=1 AND own.authenticated=1 AND own.kind=? AND own.object_len=? AND own.digest=? ORDER BY r.slot LIMIT 1"
        };
        self.row(
            || {
                sea_orm::sqlx::query(statement)
                    .bind(reference.key.as_bytes())
                    .bind(after)
                    .bind(reference.kind as i64)
                    .bind(length.as_slice())
                    .bind(reference.digest.as_slice())
            },
            page_record,
        )
        .await
    }
    pub(super) async fn next_page_record(
        &mut self,
        reference: &V3ObjectRef,
        after_slot: Option<u16>,
    ) -> PackedResult<Option<OwnedSemanticRow<PageRecord>>> {
        self.begin()?;
        let result = self
            .record_inner(reference, after_slot.map_or(-1, i64::from), false)
            .await;
        self.end(result)
    }

    pub(super) async fn enqueue_child(
        &mut self,
        parent: &PageVisit,
        slot: u16,
        record: &V3IndexRecord,
    ) -> PackedResult<VisitId> {
        self.begin()?;
        let result = self.enqueue_inner(parent, slot, record).await;
        self.end(result)
    }
    async fn enqueue_inner(
        &mut self,
        parent: &PageVisit,
        slot: u16,
        record: &V3IndexRecord,
    ) -> PackedResult<VisitId> {
        let actual = self.visit_inner(parent.id).await?;
        if *actual != *parent {
            return Err(invalid("parent visit differs from stored occurrence"));
        }
        drop(actual);
        let fact = self
            .fact_inner(&parent.reference)
            .await?
            .ok_or_else(|| invalid("parent has no authenticated page facts"))?;
        if fact.height == 0 {
            return Err(invalid("leaf page cannot enqueue a child"));
        }
        let height = fact.height - 1;
        drop(fact);
        let stored = self
            .record_inner(&parent.reference, i64::from(slot), true)
            .await?
            .ok_or_else(|| invalid("unknown parent slot"))?;
        if stored.record != *record {
            return Err(invalid(
                "child record differs from authenticated parent slot",
            ));
        }
        drop(stored);
        let V3IndexValue::Child {
            reference,
            subtree_weight,
        } = &record.value
        else {
            return Err(invalid("leaf cannot enqueue child"));
        };
        if self
            .exists(|| {
                sea_orm::sqlx::query(
                    "SELECT 1 FROM visits WHERE parent_id=? AND parent_slot=? LIMIT 1",
                )
                .bind(parent.id.0)
                .bind(i64::from(slot))
            })
            .await?
        {
            return Err(invalid("parent slot was already enqueued"));
        }
        let next = self
            .summary
            .visits
            .checked_add(1)
            .filter(|n| *n <= self.limits.max_visits)
            .ok_or_else(|| limit("semantic visit quota exceeded"))?;
        let weight = subtree_weight.to_be_bytes();
        self.execute(|| sea_orm::sqlx::query("INSERT INTO visits(id,context_id,object_key,parent_id,parent_slot,expected_height,first_key,last_key,expected_weight) VALUES(?,?,?,?,?,?,?,?,?)")
            .bind(next as i64).bind(parent.context.0).bind(reference.key.as_bytes()).bind(parent.id.0).bind(i64::from(slot)).bind(i64::from(height)).bind(record.first_key.as_slice()).bind(record.last_key.as_slice()).bind(weight.as_slice())).await?;
        self.summary.visits = next;
        Ok(VisitId(next as i64))
    }

    pub(super) async fn insert_leaf(
        &mut self,
        page_visit: &PageVisit,
        slot: u16,
        record: &V3IndexRecord,
    ) -> PackedResult<()> {
        self.begin()?;
        let result = self.insert_leaf_inner(page_visit, slot, record).await;
        self.end(result)
    }
    async fn insert_leaf_inner(
        &mut self,
        page_visit: &PageVisit,
        slot: u16,
        record: &V3IndexRecord,
    ) -> PackedResult<()> {
        let actual = self.visit_inner(page_visit.id).await?;
        if *actual != *page_visit {
            return Err(invalid("leaf visit differs from stored occurrence"));
        }
        drop(actual);
        let fact = self
            .fact_inner(&page_visit.reference)
            .await?
            .ok_or_else(|| invalid("leaf has no authenticated page facts"))?;
        if fact.height != 0 {
            return Err(invalid("branch page cannot insert a leaf"));
        }
        drop(fact);
        let stored = self
            .record_inner(&page_visit.reference, i64::from(slot), true)
            .await?
            .ok_or_else(|| invalid("unknown leaf slot"))?;
        if stored.record != *record {
            return Err(invalid("leaf differs from authenticated slot"));
        }
        drop(stored);
        let V3IndexValue::Leaf(value) = &record.value else {
            return Err(invalid("child cannot insert a leaf"));
        };
        let leaves = self
            .summary
            .leaves
            .checked_add(1)
            .filter(|n| *n <= self.limits.max_leaves)
            .ok_or_else(|| limit("semantic leaf quota exceeded"))?;
        if self
            .exists(|| {
                sea_orm::sqlx::query(
                    "SELECT 1 FROM context_leaves WHERE context_id=? AND first_key=? LIMIT 1",
                )
                .bind(page_visit.context.0)
                .bind(record.first_key.as_slice())
            })
            .await?
        {
            return Err(invalid("semantic context contains duplicate leaf key"));
        }
        let stats = self.context_stats_inner(page_visit.context).await?;
        if stats.finished {
            return Err(invalid("cannot insert into completed context"));
        }
        let weight = record.subtree_weight(page_visit.role.kind())?;
        let count = stats
            .leaf_count
            .checked_add(1)
            .ok_or_else(|| limit("context leaf count overflow"))?
            .to_be_bytes();
        let total = stats
            .leaf_weight
            .checked_add(weight)
            .ok_or_else(|| limit("context leaf weight overflow"))?
            .to_be_bytes();
        drop(stats);
        let weight = weight.to_be_bytes();
        self.execute(|| sea_orm::sqlx::query("INSERT INTO context_leaves(context_id,first_key,last_key,value,visit_id,slot,weight) VALUES(?,?,?,?,?,?,?)")
            .bind(page_visit.context.0).bind(record.first_key.as_slice()).bind(record.last_key.as_slice()).bind(value.as_slice()).bind(page_visit.id.0).bind(i64::from(slot)).bind(weight.as_slice())).await?;
        self.execute(|| {
            sea_orm::sqlx::query(
                "UPDATE contexts SET leaf_count=?,leaf_weight=? WHERE id=? AND finished=0",
            )
            .bind(count.as_slice())
            .bind(total.as_slice())
            .bind(page_visit.context.0)
        })
        .await?;
        self.summary.leaves = leaves;
        Ok(())
    }

    async fn context_stats_inner(
        &mut self,
        id: ContextId,
    ) -> PackedResult<OwnedSemanticRow<ContextStats>> {
        self.row(
            || {
                sea_orm::sqlx::query(
                    "SELECT leaf_count,leaf_weight,finished FROM contexts WHERE id=? LIMIT 1",
                )
                .bind(id.0)
            },
            |row| {
                let count: Vec<u8> = column(row, 0)?;
                let weight: Vec<u8> = column(row, 1)?;
                let finished: i64 = column(row, 2)?;
                Ok(ContextStats {
                    leaf_count: be8(&count)?,
                    leaf_weight: be8(&weight)?,
                    finished: finished == 1,
                })
            },
        )
        .await?
        .ok_or_else(|| invalid("unknown semantic context"))
    }
    pub(super) async fn context_stats(
        &mut self,
        id: ContextId,
    ) -> PackedResult<OwnedSemanticRow<ContextStats>> {
        self.begin()?;
        let result = self.context_stats_inner(id).await;
        self.end(result)
    }
    pub(super) async fn next_context_leaf(
        &mut self,
        id: ContextId,
        after: Option<&[u8]>,
    ) -> PackedResult<Option<OwnedSemanticRow<V3IndexRecord>>> {
        self.begin()?;
        let result = self.row(|| sea_orm::sqlx::query("SELECT first_key,last_key,value FROM context_leaves WHERE context_id=? AND first_key>? ORDER BY first_key LIMIT 1").bind(id.0).bind(after.unwrap_or(&[])), |row| {
            Ok(V3IndexRecord { first_key: column(row, 0)?, last_key: column(row, 1)?, value: V3IndexValue::Leaf(column(row, 2)?) })
        }).await;
        self.end(result)
    }
    pub(super) async fn context_leaf(
        &mut self,
        id: ContextId,
        key: &[u8],
    ) -> PackedResult<Option<OwnedSemanticRow<V3IndexRecord>>> {
        self.begin()?;
        let result = self.row(|| sea_orm::sqlx::query("SELECT first_key,last_key,value FROM context_leaves WHERE context_id=? AND first_key=? LIMIT 1").bind(id.0).bind(key), |row| {
            Ok(V3IndexRecord { first_key: column(row, 0)?, last_key: column(row, 1)?, value: V3IndexValue::Leaf(column(row, 2)?) })
        }).await;
        self.end(result)
    }

    pub(super) async fn finish_visit(&mut self, id: VisitId) -> PackedResult<()> {
        self.begin()?;
        let result = self.finish_visit_inner(id).await;
        self.end(result)
    }
    async fn finish_visit_inner(&mut self, id: VisitId) -> PackedResult<()> {
        let page_visit = self.visit_inner(id).await?;
        let page = self
            .fact_inner(&page_visit.reference)
            .await?
            .ok_or_else(|| invalid("visited page lacks authenticated facts"))?;
        if page.kind != page_visit.role.kind() {
            return Err(invalid("visit role disagrees with authenticated page kind"));
        }
        if let Some(expected) = &page_visit.expected {
            validate_child_claim(
                IndexPageClaim {
                    kind: page_visit.role.kind(),
                    height: expected.height,
                    bounds: Some((&expected.first, &expected.last)),
                    weight: expected.weight,
                },
                IndexPageClaim {
                    kind: page.kind,
                    height: page.height,
                    bounds: page.first.as_deref().zip(page.last.as_deref()),
                    weight: page.weight,
                },
            )?;
        }
        if let Some((parent, slot)) = page_visit.parent {
            if self.exists(|| sea_orm::sqlx::query("SELECT 1 FROM visits child LEFT JOIN visits parent ON parent.id=child.parent_id LEFT JOIN page_facts p ON p.object_key=parent.object_key LEFT JOIN page_records r ON r.object_key=parent.object_key AND r.slot=child.parent_slot WHERE child.id=? AND (parent.id IS NULL OR parent.context_id<>child.context_id OR p.complete IS NOT 1 OR r.child_key IS NULL OR r.child_key<>child.object_key OR p.height<>child.expected_height+1 OR r.first_key<>child.first_key OR r.last_key<>child.last_key OR r.weight<>child.expected_weight) LIMIT 1").bind(id.0)).await? { return Err(invalid("incoming edge does not bind its actual same-context parent record")); }
            if parent.0 <= 0 || slot >= 1024 {
                return Err(invalid("incoming edge id/slot invalid"));
            }
        } else if !self
            .exists(|| {
                sea_orm::sqlx::query("SELECT 1 FROM contexts WHERE id=? AND root_key=? LIMIT 1")
                    .bind(page_visit.context.0)
                    .bind(page_visit.reference.key.as_bytes())
            })
            .await?
        {
            return Err(invalid("root visit differs from context root"));
        }
        let missing = if page.height == 0 {
            self.exists(|| sea_orm::sqlx::query("SELECT 1 FROM page_records r LEFT JOIN context_leaves l ON l.visit_id=? AND l.slot=r.slot WHERE r.object_key=? AND (l.visit_id IS NULL OR l.context_id<>? OR l.first_key<>r.first_key OR l.last_key<>r.last_key OR l.value<>r.leaf OR l.weight<>r.weight) LIMIT 1")
                .bind(id.0).bind(page_visit.reference.key.as_bytes()).bind(page_visit.context.0)).await?
        } else {
            self.exists(|| sea_orm::sqlx::query("SELECT 1 FROM page_records r LEFT JOIN visits v ON v.parent_id=? AND v.parent_slot=r.slot WHERE r.object_key=? AND (v.id IS NULL OR v.context_id<>? OR v.object_key<>r.child_key OR v.expected_height<>? OR v.first_key<>r.first_key OR v.last_key<>r.last_key OR v.expected_weight<>r.weight) LIMIT 1")
                .bind(id.0).bind(page_visit.reference.key.as_bytes()).bind(page_visit.context.0).bind(i64::from(page.height - 1))).await?
        };
        if missing {
            return Err(invalid(
                "visit did not process every authenticated page record",
            ));
        }
        let affected = self
            .execute(|| {
                sea_orm::sqlx::query(
                    "UPDATE visits SET checked=1,processed=1 WHERE id=? AND processed=0",
                )
                .bind(id.0)
            })
            .await?;
        if affected != 1 {
            return Err(invalid("semantic visit completion lost its row"));
        }
        Ok(())
    }

    pub(super) async fn finish_context(&mut self, id: ContextId) -> PackedResult<()> {
        self.begin()?;
        let result = self.finish_context_inner(id).await;
        self.end(result)
    }
    async fn finish_context_inner(&mut self, id: ContextId) -> PackedResult<()> {
        let stats = self.context_stats_inner(id).await?;
        if stats.finished {
            return Err(invalid("semantic context completed twice"));
        }
        if self.exists(|| sea_orm::sqlx::query("SELECT 1 FROM visits INDEXED BY context_visits WHERE context_id=? AND processed=0 LIMIT 1").bind(id.0)).await? { return Err(invalid("semantic context still has pending visits")); }
        let root = self.row(|| sea_orm::sqlx::query("SELECT p.weight,c.expected_weight FROM contexts c LEFT JOIN visits v ON v.context_id=c.id AND v.parent_id IS NULL LEFT JOIN page_facts p ON p.object_key=v.object_key WHERE c.id=? AND v.checked=1 AND v.processed=1 AND p.complete=1 LIMIT 1").bind(id.0), |row| {
            let actual: Vec<u8> = column(row, 0)?; let expected: Option<Vec<u8>> = column(row, 1)?;
            Ok((be8(&actual)?, expected.as_deref().map(be8).transpose()?))
        }).await?.ok_or_else(|| invalid("semantic context lacks exactly one completed authenticated root"))?;
        if stats.leaf_weight != root.0
            || root.1.is_some_and(|expected| expected != stats.leaf_weight)
        {
            return Err(invalid(
                "context observed leaves disagree with root/declared weight",
            ));
        }
        drop(root);
        drop(stats);
        let affected = self
            .execute(|| {
                sea_orm::sqlx::query("UPDATE contexts SET finished=1 WHERE id=? AND finished=0")
                    .bind(id.0)
            })
            .await?;
        if affected != 1 {
            return Err(invalid("semantic context completion lost its row"));
        }
        Ok(())
    }

    pub(super) async fn finish_all(&mut self) -> PackedResult<SemanticSummary> {
        self.begin()?;
        let result = async {
            for statement in [
                "SELECT 1 FROM contexts INDEXED BY unfinished_contexts WHERE finished=0 LIMIT 1",
                "SELECT 1 FROM visits WHERE processed=0 OR checked=0 LIMIT 1",
                "SELECT 1 FROM visits v LEFT JOIN contexts c ON c.id=v.context_id LEFT JOIN page_facts p ON p.object_key=v.object_key LEFT JOIN objects o ON o.key=v.object_key WHERE c.id IS NULL OR p.complete IS NOT 1 OR o.authenticated IS NOT 1 OR p.kind<>o.kind LIMIT 1",
                "SELECT 1 FROM page_facts WHERE complete=0 LIMIT 1",
                "SELECT 1 FROM objects INDEXED BY pending_objects WHERE authenticated=0 LIMIT 1",
            ] {
                if self.exists(|| sea_orm::sqlx::query(statement)).await? { return Err(invalid("semantic facts contain unfinished or missing graph records")); }
            }
            self.live()?;
            let mut summary = self.summary;
            summary.sql_vm_steps = self.vm.used(); summary.inventory = self.inventory.stats();
            Ok(summary)
        }.await;
        self.end(result)
    }
    pub(super) async fn close(self) -> PackedResult<()> {
        self.inventory.close().await
    }

    pub(super) async fn authenticated_inventory_digest(&mut self) -> PackedResult<[u8; 32]> {
        self.begin()?;
        let result = async {
            self.charge_sql(self.inventory.stats().registered_objects.saturating_add(1))?;
            self.inventory.final_digest().await
        }
        .await;
        self.end(result)
    }

    pub(super) async fn next_authenticated_reference(
        &mut self,
        after: &[u8],
    ) -> PackedResult<Option<super::inventory::OwnedInventoryReference>> {
        self.begin()?;
        let result = async {
            self.charge_sql(1)?;
            self.inventory.next_authenticated(after).await
        }
        .await;
        self.end(result)
    }
}

#[cfg(test)]
mod tests;
