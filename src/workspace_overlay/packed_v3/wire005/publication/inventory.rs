//! Private, disposable physical-object registry. Authentication is an explicit
//! caller action; this registry does not prove typed graph relations or provide
//! a durable publication journal, retention root, or catalog authority.

use crate::workspace_overlay::packed_v3::wire::{PackedResult, PackedWireError};
use crate::workspace_overlay::packed_v3::wire005::{
    V3BudgetPool, V3MountBudget, V3ObjectKind, V3ObjectRef, V3OwnedPermit,
};
use futures::channel::oneshot;
use sea_orm::sqlx::{
    Connection, Row, SqliteConnection,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteRow, SqliteSynchronous},
};
use sha2::{Digest, Sha256};
use std::future::Future;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const PAGE_BYTES: u64 = 4096;
const MIN_CACHE_BYTES: u64 = 64 << 10;
const CACHE_WRITE_ALLOWANCE_BYTES: u64 = 256 << 10;
const CONNECTION_CONTROL_BYTES: u64 = 8 << 10;
const CONNECTION_METADATA_BYTES: u64 = 24 << 10;
const SQLITE_WORKER_STACK_ALLOWANCE_BYTES: u64 = 2 << 20;
const CLEANUP_STACK_BYTES: usize = 128 << 10;
const QUERY_MEMORY_BYTES: u64 = 32 << 10;
const INVENTORY_DOMAIN: &[u8] = b"BrewFS packed v3 physical inventory\0";
const INVENTORY_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug)]
pub(super) struct InventoryLimits {
    pub max_objects: u64,
    pub max_declared_bytes: u64,
    pub max_disk_bytes: u64,
    pub sqlite_cache_bytes: u64,
}

impl InventoryLimits {
    fn validate(self) -> PackedResult<()> {
        if self.max_objects == 0
            || self.max_declared_bytes == 0
            || self.max_disk_bytes < 4 * PAGE_BYTES
            || self.max_disk_bytes / PAGE_BYTES >= u32::MAX as u64
            || self.sqlite_cache_bytes < MIN_CACHE_BYTES
            || self.sqlite_cache_bytes / PAGE_BYTES > i32::MAX as u64
        {
            return Err(PackedWireError::LimitExceeded(
                "invalid physical inventory quotas".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct InventoryStats {
    pub registered_objects: u64,
    pub declared_bytes: u64,
    pub authenticated_objects: u64,
    pub authenticated_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Registration {
    pub is_new: bool,
    pub authenticated: bool,
}

pub(super) struct OwnedInventoryReference {
    reference: V3ObjectRef,
    _permit: V3OwnedPermit,
}

/// Private one-row SQL result; its arguments, row and decoded copies remain
/// admitted until the semantic caller consumes/drops this owner.
pub(super) struct OwnedSqlRow {
    pub(super) row: SqliteRow,
    pub(super) permit: V3OwnedPermit,
}

pub(super) struct SemanticVmWork {
    remaining: AtomicU64,
    used: AtomicU64,
    exhausted: AtomicBool,
}

impl SemanticVmWork {
    pub(super) fn used(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }

    pub(super) fn exhausted(&self) -> bool {
        self.exhausted.load(Ordering::Acquire)
    }

    fn step(&self) -> bool {
        if self
            .remaining
            .try_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .is_err()
        {
            self.exhausted.store(true, Ordering::Release);
            return false;
        }
        self.used.fetch_add(1, Ordering::AcqRel);
        true
    }
}

impl Deref for OwnedInventoryReference {
    type Target = V3ObjectRef;

    fn deref(&self) -> &Self::Target {
        &self.reference
    }
}

struct PrivateDirectory {
    path: PathBuf,
}

impl Drop for PrivateDirectory {
    fn drop(&mut self) {
        // Only this exclusively created directory is owned. Its parent and
        // sibling evidence directories are never cleanup targets.
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

struct InventoryStorage {
    connection: Option<SqliteConnection>,
    directory: Option<PrivateDirectory>,
    permit: Option<V3OwnedPermit>,
    active_query: Option<V3OwnedPermit>,
}

impl InventoryStorage {
    fn take_cleanup(&mut self) -> Option<Cleanup> {
        Some(Cleanup {
            connection: self.connection.take()?,
            directory: self.directory.take().expect("private inventory directory"),
            permit: self.permit.take().expect("private inventory admission"),
            active_query: self.active_query.take(),
        })
    }

    fn close_on_current_thread(mut self) -> PackedResult<()> {
        if let Some(cleanup) = self.take_cleanup() {
            cleanup.run()
        } else {
            Ok(())
        }
    }

    async fn close(mut self) -> PackedResult<()> {
        let Some(cleanup) = self.take_cleanup() else {
            return Ok(());
        };
        start_cleanup(cleanup).await.map_err(|_| {
            PackedWireError::Backend("physical inventory cleanup worker stopped".into())
        })?
    }
}

impl Drop for InventoryStorage {
    fn drop(&mut self) {
        if let Some(cleanup) = self.take_cleanup() {
            // SQLx Drop alone does not confirm its SQLite worker has released
            // the page cache. The cleanup worker retains all owners until the
            // shutdown acknowledgment, even if the Tokio runtime has stopped.
            drop(start_cleanup(cleanup));
        }
    }
}

struct Cleanup {
    connection: SqliteConnection,
    directory: PrivateDirectory,
    permit: V3OwnedPermit,
    active_query: Option<V3OwnedPermit>,
}

impl Cleanup {
    fn run(self) -> PackedResult<()> {
        let Self {
            connection,
            directory,
            permit,
            active_query,
        } = self;
        // optimize_on_close is disabled. SQLx's shutdown acknowledgment follows
        // release of its database handle and cached statements; this future
        // uses a channels-only worker and needs no Tokio runtime.
        let result = futures::executor::block_on(connection.close()).map_err(sql_error);
        drop(directory);
        drop(active_query);
        drop(permit);
        result
    }
}

fn start_cleanup(cleanup: Cleanup) -> oneshot::Receiver<PackedResult<()>> {
    let (sender, receiver) = oneshot::channel();
    let retained = Arc::new(Mutex::new(Some((cleanup, sender))));
    let worker_retained = retained.clone();
    let result = std::thread::Builder::new()
        .name("brewfs-wire005-inventory-close".into())
        .stack_size(CLEANUP_STACK_BYTES)
        .spawn(move || {
            if let Some((cleanup, sender)) = worker_retained.lock().unwrap().take() {
                let _ = sender.send(cleanup.run());
            }
        });
    if result.is_err()
        && let Some((cleanup, sender)) = retained.lock().unwrap().take()
    {
        // At OS thread exhaustion there is no nonblocking way to confirm the
        // SQLite worker's shutdown. Conservatively retain the private files
        // and memory owners rather than report an unproved release.
        std::mem::forget(cleanup);
        let _ = sender.send(Err(PackedWireError::Backend(
            "cannot start private inventory cleanup worker".into(),
        )));
    }
    receiver
}

fn cancelled() -> PackedWireError {
    PackedWireError::Backend("physical inventory cancelled".into())
}

fn closed() -> PackedWireError {
    PackedWireError::LimitExceeded("physical inventory mount budget closed".into())
}

fn sql_error(error: sea_orm::sqlx::Error) -> PackedWireError {
    if error
        .as_database_error()
        .and_then(|error| error.code())
        .is_some_and(|code| code == "13")
    {
        PackedWireError::LimitExceeded("physical inventory SQLite disk quota exceeded".into())
    } else {
        PackedWireError::Backend("private physical inventory SQLite operation failed".into())
    }
}

async fn sql_await<T>(
    future: impl Future<Output = Result<T, sea_orm::sqlx::Error>>,
    budget: &V3MountBudget,
    cancel: &CancellationToken,
) -> PackedResult<T> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(cancelled()),
        _ = budget.wait_closed() => Err(closed()),
        result = future => result.map_err(sql_error),
    }
}

fn decode_row(row: SqliteRow) -> PackedResult<(V3ObjectRef, bool)> {
    let invalid = || PackedWireError::Invalid("invalid private physical inventory row".into());
    let key: Vec<u8> = row.try_get(0).map_err(|_| invalid())?;
    let kind: i64 = row.try_get(1).map_err(|_| invalid())?;
    let length: Vec<u8> = row.try_get(2).map_err(|_| invalid())?;
    let digest: Vec<u8> = row.try_get(3).map_err(|_| invalid())?;
    let authenticated: i64 = row.try_get(4).map_err(|_| invalid())?;
    if key.is_empty() || key.len() > 4096 || !matches!(authenticated, 0 | 1) {
        return Err(invalid());
    }
    let reference = V3ObjectRef {
        key: String::from_utf8(key).map_err(|_| invalid())?,
        kind: V3ObjectKind::from_u8(kind.try_into().map_err(|_| invalid())?)?,
        object_len: u64::from_le_bytes(length.try_into().map_err(|_| invalid())?),
        digest: digest.try_into().map_err(|_| invalid())?,
    };
    reference.encode_value()?;
    Ok((reference, authenticated == 1))
}

pub(super) struct PhysicalInventory {
    storage: Option<InventoryStorage>,
    budget: Arc<V3MountBudget>,
    cancel: CancellationToken,
    limits: InventoryLimits,
    stats: InventoryStats,
    tainted: bool,
}

impl PhysicalInventory {
    pub(super) async fn create(
        parent: &Path,
        budget: Arc<V3MountBudget>,
        limits: InventoryLimits,
        cancel: CancellationToken,
    ) -> PackedResult<Self> {
        limits.validate()?;
        if cancel.is_cancelled() {
            return Err(cancelled());
        }
        // SQLx starts one standard Rust thread without an explicit stack size.
        // Reserve its usual 2 MiB, or a larger inherited RUST_MIN_STACK, plus
        // both possible simultaneous open/close worker stacks. These admissions
        // cover configured owners; they are not a measured SQLite RSS ceiling.
        let sqlite_stack = std::env::var("RUST_MIN_STACK")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(SQLITE_WORKER_STACK_ALLOWANCE_BYTES)
            .max(SQLITE_WORKER_STACK_ALLOWANCE_BYTES);
        let workspace = limits
            .sqlite_cache_bytes
            .checked_add(CACHE_WRITE_ALLOWANCE_BYTES)
            .and_then(|bytes| bytes.checked_add(2 * CLEANUP_STACK_BYTES as u64))
            .and_then(|bytes| bytes.checked_add(sqlite_stack))
            .ok_or_else(|| {
                PackedWireError::LimitExceeded("inventory cache charge overflow".into())
            })?;
        let permit = budget.admit(&[
            (V3BudgetPool::Workspace, workspace),
            (V3BudgetPool::Control, CONNECTION_CONTROL_BYTES),
            (V3BudgetPool::Metadata, CONNECTION_METADATA_BYTES),
        ])?;
        let directory_path =
            parent.join(format!("brewfs-wire005-inventory-{}", uuid::Uuid::new_v4()));
        let mut directory_builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            directory_builder.mode(0o700);
        }
        directory_builder.create(&directory_path).map_err(|_| {
            PackedWireError::Backend("cannot create private physical inventory directory".into())
        })?;
        let directory = PrivateDirectory {
            path: directory_path,
        };
        let path = directory.path.join("inventory.sqlite");
        let mut open = std::fs::OpenOptions::new();
        open.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            open.mode(0o600);
        }
        open.open(&path).map_err(|_| {
            PackedWireError::Backend("cannot create private physical inventory database".into())
        })?;
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(false)
            .in_memory(false)
            .page_size(PAGE_BYTES as u32)
            .journal_mode(SqliteJournalMode::Off)
            .synchronous(SqliteSynchronous::Off)
            .statement_cache_capacity(8)
            .command_buffer_size(1)
            .row_buffer_size(1)
            .busy_timeout(Duration::from_millis(50))
            .optimize_on_close(false, None)
            .pragma(
                "cache_size",
                (limits.sqlite_cache_bytes / PAGE_BYTES).to_string(),
            )
            .pragma(
                "max_page_count",
                (limits.max_disk_bytes / PAGE_BYTES).to_string(),
            )
            .pragma("mmap_size", "0")
            .pragma("temp_store", "MEMORY");
        let storage = InventoryStorage {
            connection: None,
            directory: Some(directory),
            permit: Some(permit),
            active_query: None,
        };
        let (sender, receiver) = oneshot::channel();
        let worker_budget = budget.clone();
        let worker_cancel = cancel.clone();
        std::thread::Builder::new()
            .name("brewfs-wire005-inventory-open".into())
            .stack_size(CLEANUP_STACK_BYTES)
            .spawn(move || {
                let result = futures::executor::block_on(Self::initialize(
                    storage,
                    options,
                    worker_budget,
                    limits,
                    worker_cancel,
                ));
                if let Err(Ok(mut inventory)) = sender.send(result)
                    && let Some(storage) = inventory.storage.take()
                {
                    let _ = storage.close_on_current_thread();
                }
            })
            .map_err(|_| {
                PackedWireError::Backend("cannot start physical inventory initialization".into())
            })?;
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(cancelled()),
            _ = budget.wait_closed() => Err(closed()),
            result = receiver => result.map_err(|_| PackedWireError::Backend("physical inventory initialization worker stopped".into()))?,
        }
    }

    async fn initialize(
        mut storage: InventoryStorage,
        options: SqliteConnectOptions,
        budget: Arc<V3MountBudget>,
        limits: InventoryLimits,
        cancel: CancellationToken,
    ) -> PackedResult<Self> {
        let connecting = SqliteConnection::connect_with(&options);
        tokio::pin!(connecting);
        let aborted = tokio::select! {
            biased;
            _ = cancel.cancelled() => Some(cancelled()),
            _ = budget.wait_closed() => Some(closed()),
            result = &mut connecting => {
                storage.connection = Some(result.map_err(sql_error)?);
                None
            },
        };
        if let Some(error) = aborted {
            // Connection establishment owns a second SQLx worker. Retain all
            // permits until that future settles, then confirm its shutdown.
            if let Ok(connection) = connecting.await {
                storage.connection = Some(connection);
            }
            let _ = storage.close().await;
            return Err(error);
        }
        let schema = async {
            for statement in [
                "CREATE TABLE objects (key BLOB PRIMARY KEY NOT NULL CHECK(length(key) BETWEEN 1 AND 4096), kind INTEGER NOT NULL CHECK(kind BETWEEN 0 AND 12), object_len BLOB NOT NULL CHECK(length(object_len)=8), digest BLOB NOT NULL CHECK(length(digest)=32), authenticated INTEGER NOT NULL DEFAULT 0 CHECK(authenticated IN (0,1))) WITHOUT ROWID",
                "CREATE INDEX pending_objects ON objects(authenticated, key)",
            ] {
                sql_await(
                    sea_orm::sqlx::query(statement)
                        .execute(storage.connection.as_mut().expect("inventory connection")),
                    &budget,
                    &cancel,
                )
                .await?;
            }
            Ok(())
        }
        .await;
        if let Err(error) = schema {
            let _ = storage.close().await;
            return Err(error);
        }
        Ok(Self {
            storage: Some(storage),
            budget,
            cancel,
            limits,
            stats: InventoryStats::default(),
            tainted: false,
        })
    }

    pub(super) fn stats(&self) -> InventoryStats {
        self.stats
    }

    fn live(&mut self) -> PackedResult<()> {
        if self.tainted {
            return Err(PackedWireError::Invalid(
                "physical inventory is unusable after an interrupted SQL operation".into(),
            ));
        }
        if self.cancel.is_cancelled() {
            self.tainted = true;
            return Err(cancelled());
        }
        if self.budget.state().closed {
            self.tainted = true;
            return Err(closed());
        }
        Ok(())
    }

    fn start_query(&mut self) -> PackedResult<()> {
        self.start_sized_query(QUERY_MEMORY_BYTES)
    }

    fn start_sized_query(&mut self, bytes: u64) -> PackedResult<()> {
        if !(QUERY_MEMORY_BYTES..=2 << 20).contains(&bytes) {
            return Err(PackedWireError::LimitExceeded(
                "private scratch query admission is outside its fixed bounds".into(),
            ));
        }
        if self.storage.as_ref().unwrap().active_query.is_some() {
            self.tainted = true;
            return Err(PackedWireError::Invalid(
                "physical inventory has an unfinished query owner".into(),
            ));
        }
        self.live()?;
        let result = self.budget.admit(&[(V3BudgetPool::Metadata, bytes)]);
        match result {
            Ok(permit) => {
                // SQLx may retain owned arguments after the caller's future is
                // dropped. Keep the query reservation with its connection;
                // another operation cannot replace this unfinished owner.
                self.storage.as_mut().unwrap().active_query = Some(permit);
                Ok(())
            }
            Err(error) => {
                self.tainted = true;
                Err(error)
            }
        }
    }

    fn finish_query(&mut self) {
        if !self.tainted {
            // All awaits completed. Recoverable identity/quota rejections
            // also release ownership: they did not leave SQL work pending.
            drop(self.storage.as_mut().unwrap().active_query.take());
        }
    }

    fn checked_sql<T>(&mut self, result: PackedResult<T>) -> PackedResult<T> {
        if result.is_err() {
            self.tainted = true;
        }
        result
    }

    async fn existing(&mut self, key: &[u8]) -> PackedResult<Option<(V3ObjectRef, bool)>> {
        let connection = self.storage.as_mut().unwrap().connection.as_mut().unwrap();
        let result = sql_await(
            sea_orm::sqlx::query(
                "SELECT key, kind, object_len, digest, authenticated FROM objects WHERE key=?",
            )
            .bind(key)
            .fetch_optional(connection),
            &self.budget,
            &self.cancel,
        )
        .await
        .and_then(|row| row.map(decode_row).transpose());
        self.checked_sql(result)
    }

    pub(super) async fn register(&mut self, reference: &V3ObjectRef) -> PackedResult<Registration> {
        self.start_query()?;
        let result = self.register_inner(reference).await;
        self.finish_query();
        result
    }

    async fn register_inner(&mut self, reference: &V3ObjectRef) -> PackedResult<Registration> {
        reference.encode_value()?;
        if let Some((existing, authenticated)) = self.existing(reference.key.as_bytes()).await? {
            if existing != *reference {
                return Err(PackedWireError::Invalid(
                    "one physical object key has conflicting kind, length or digest".into(),
                ));
            }
            return Ok(Registration {
                is_new: false,
                authenticated,
            });
        }
        let count = self
            .stats
            .registered_objects
            .checked_add(1)
            .ok_or_else(|| {
                PackedWireError::LimitExceeded("physical inventory object count overflow".into())
            })?;
        let bytes = self
            .stats
            .declared_bytes
            .checked_add(reference.object_len)
            .ok_or_else(|| {
                PackedWireError::LimitExceeded("physical inventory byte count overflow".into())
            })?;
        if count > self.limits.max_objects || bytes > self.limits.max_declared_bytes {
            return Err(PackedWireError::LimitExceeded(
                "physical inventory object or declared-byte quota exceeded".into(),
            ));
        }
        let length = reference.object_len.to_le_bytes();
        let connection = self.storage.as_mut().unwrap().connection.as_mut().unwrap();
        let result = sql_await(
            sea_orm::sqlx::query(
                "INSERT INTO objects(key,kind,object_len,digest,authenticated) VALUES(?,?,?,?,0)",
            )
            .bind(reference.key.as_bytes())
            .bind(reference.kind as i64)
            .bind(length.as_slice())
            .bind(reference.digest.as_slice())
            .execute(connection),
            &self.budget,
            &self.cancel,
        )
        .await;
        self.checked_sql(result)?;
        self.stats.registered_objects = count;
        self.stats.declared_bytes = bytes;
        Ok(Registration {
            is_new: true,
            authenticated: false,
        })
    }

    pub(super) async fn next_pending(&mut self) -> PackedResult<Option<OwnedInventoryReference>> {
        self.start_query()?;
        let connection = self.storage.as_mut().unwrap().connection.as_mut().unwrap();
        let result = sql_await(
            sea_orm::sqlx::query(
                "SELECT key,kind,object_len,digest,authenticated FROM objects INDEXED BY pending_objects WHERE authenticated=0 ORDER BY key LIMIT 1",
            )
            .fetch_optional(connection),
            &self.budget,
            &self.cancel,
        )
        .await
        .and_then(|row| row.map(decode_row).transpose());
        let row = self.checked_sql(result)?;
        let permit = self
            .storage
            .as_mut()
            .unwrap()
            .active_query
            .take()
            .expect("completed inventory query owner");
        Ok(row.map(|(reference, _)| OwnedInventoryReference {
            reference,
            _permit: permit,
        }))
    }

    pub(super) async fn next_authenticated(
        &mut self,
        after: &[u8],
    ) -> PackedResult<Option<OwnedInventoryReference>> {
        self.start_query()?;
        let connection = self.storage.as_mut().unwrap().connection.as_mut().unwrap();
        let result = sql_await(
            sea_orm::sqlx::query("SELECT key,kind,object_len,digest,authenticated FROM objects WHERE key>? ORDER BY key LIMIT 1")
                .bind(after).fetch_optional(connection),
            &self.budget,
            &self.cancel,
        ).await.and_then(|row| row.map(decode_row).transpose());
        let row = self.checked_sql(result)?;
        if row
            .as_ref()
            .is_some_and(|(_, authenticated)| !authenticated)
        {
            self.tainted = true;
            return Err(PackedWireError::Invalid(
                "unfinished physical inventory member".into(),
            ));
        }
        let permit = self
            .storage
            .as_mut()
            .unwrap()
            .active_query
            .take()
            .expect("completed inventory member owner");
        Ok(row.map(|(reference, _)| OwnedInventoryReference {
            reference,
            _permit: permit,
        }))
    }

    pub(super) async fn mark_authenticated(&mut self, reference: &V3ObjectRef) -> PackedResult<()> {
        self.start_query()?;
        let result = self.mark_authenticated_inner(reference).await;
        self.finish_query();
        result
    }

    async fn mark_authenticated_inner(&mut self, reference: &V3ObjectRef) -> PackedResult<()> {
        reference.encode_value()?;
        let Some((existing, authenticated)) = self.existing(reference.key.as_bytes()).await? else {
            return Err(PackedWireError::Invalid(
                "cannot authenticate an unregistered physical object".into(),
            ));
        };
        if existing != *reference {
            return Err(PackedWireError::Invalid(
                "authentication identity disagrees with physical inventory".into(),
            ));
        }
        if authenticated {
            return Ok(());
        }
        let count = self
            .stats
            .authenticated_objects
            .checked_add(1)
            .ok_or_else(|| {
                PackedWireError::LimitExceeded("authenticated inventory count overflow".into())
            })?;
        let bytes = self
            .stats
            .authenticated_bytes
            .checked_add(reference.object_len)
            .ok_or_else(|| {
                PackedWireError::LimitExceeded("authenticated inventory bytes overflow".into())
            })?;
        let connection = self.storage.as_mut().unwrap().connection.as_mut().unwrap();
        let result = sql_await(
            sea_orm::sqlx::query(
                "UPDATE objects SET authenticated=1 WHERE key=? AND authenticated=0",
            )
            .bind(reference.key.as_bytes())
            .execute(connection),
            &self.budget,
            &self.cancel,
        )
        .await;
        let changed = self.checked_sql(result)?;
        if changed.rows_affected() != 1 {
            self.tainted = true;
            return Err(PackedWireError::Invalid(
                "physical inventory authentication update lost its row".into(),
            ));
        }
        self.stats.authenticated_objects = count;
        self.stats.authenticated_bytes = bytes;
        Ok(())
    }

    pub(super) async fn final_digest(&mut self) -> PackedResult<[u8; 32]> {
        self.start_query()?;
        let result = self.final_digest_inner().await;
        self.finish_query();
        result
    }

    async fn final_digest_inner(&mut self) -> PackedResult<[u8; 32]> {
        if self.stats.authenticated_objects != self.stats.registered_objects
            || self.stats.authenticated_bytes != self.stats.declared_bytes
        {
            return Err(PackedWireError::Invalid(
                "physical inventory still contains unauthenticated objects".into(),
            ));
        }
        let mut hash = Sha256::new();
        hash.update(INVENTORY_DOMAIN);
        hash.update(INVENTORY_VERSION.to_le_bytes());
        hash.update(self.stats.registered_objects.to_le_bytes());
        let mut cursor = Vec::new();
        let mut observed_objects = 0u64;
        let mut observed_bytes = 0u64;
        loop {
            self.live()?;
            let connection = self.storage.as_mut().unwrap().connection.as_mut().unwrap();
            let result = sql_await(
                sea_orm::sqlx::query(
                    "SELECT key,kind,object_len,digest,authenticated FROM objects WHERE key>? ORDER BY key LIMIT 1",
                )
                .bind(cursor.as_slice())
                .fetch_optional(connection),
                &self.budget,
                &self.cancel,
            )
            .await
            .and_then(|row| row.map(decode_row).transpose());
            let Some((reference, authenticated)) = self.checked_sql(result)? else {
                break;
            };
            if !authenticated {
                self.tainted = true;
                return Err(PackedWireError::Invalid(
                    "physical inventory authentication count disagrees with rows".into(),
                ));
            }
            observed_objects = observed_objects.checked_add(1).ok_or_else(|| {
                PackedWireError::LimitExceeded("inventory digest object count overflow".into())
            })?;
            observed_bytes = observed_bytes
                .checked_add(reference.object_len)
                .ok_or_else(|| {
                    PackedWireError::LimitExceeded("inventory digest byte count overflow".into())
                })?;
            hash.update((reference.key.len() as u32).to_le_bytes());
            hash.update(reference.key.as_bytes());
            hash.update([reference.kind as u8]);
            hash.update(reference.object_len.to_le_bytes());
            hash.update(reference.digest);
            cursor.clear();
            cursor.extend_from_slice(reference.key.as_bytes());
        }
        self.live()?;
        if observed_objects != self.stats.registered_objects
            || observed_bytes != self.stats.declared_bytes
        {
            self.tainted = true;
            return Err(PackedWireError::Invalid(
                "physical inventory digest counts disagree with authenticated rows".into(),
            ));
        }
        Ok(hash.finalize().into())
    }

    /// Narrow sibling-module hook. Construct bound arguments only after the
    /// existing query owner is installed, and retain it on error/cancel/drop.
    pub(super) async fn semantic_execute<'q>(
        &mut self,
        make_query: impl FnOnce() -> sea_orm::sqlx::query::Query<
            'q,
            sea_orm::sqlx::Sqlite,
            sea_orm::sqlx::sqlite::SqliteArguments<'q>,
        >,
    ) -> PackedResult<u64> {
        self.start_query()?;
        let connection = self.storage.as_mut().unwrap().connection.as_mut().unwrap();
        let result = sql_await(make_query().execute(connection), &self.budget, &self.cancel).await;
        let affected = self.checked_sql(result)?.rows_affected();
        self.finish_query();
        Ok(affected)
    }

    /// Native source cold rows use this bounded sibling hook. The reservation
    /// remains in storage while SQLx owns arguments, including cancellation.
    pub(super) async fn semantic_execute_sized<'q>(
        &mut self,
        bytes: u64,
        make_query: impl FnOnce() -> sea_orm::sqlx::query::Query<
            'q,
            sea_orm::sqlx::Sqlite,
            sea_orm::sqlx::sqlite::SqliteArguments<'q>,
        >,
    ) -> PackedResult<u64> {
        self.start_sized_query(bytes)?;
        let connection = self.storage.as_mut().unwrap().connection.as_mut().unwrap();
        let result = sql_await(make_query().execute(connection), &self.budget, &self.cancel).await;
        let affected = self.checked_sql(result)?.rows_affected();
        self.finish_query();
        Ok(affected)
    }

    pub(super) async fn semantic_fetch_optional_sized<'q>(
        &mut self,
        bytes: u64,
        make_query: impl FnOnce() -> sea_orm::sqlx::query::Query<
            'q,
            sea_orm::sqlx::Sqlite,
            sea_orm::sqlx::sqlite::SqliteArguments<'q>,
        >,
    ) -> PackedResult<Option<OwnedSqlRow>> {
        self.start_sized_query(bytes)?;
        let connection = self.storage.as_mut().unwrap().connection.as_mut().unwrap();
        let result = sql_await(
            make_query().fetch_optional(connection),
            &self.budget,
            &self.cancel,
        )
        .await;
        let row = self.checked_sql(result)?;
        let permit = self
            .storage
            .as_mut()
            .unwrap()
            .active_query
            .take()
            .expect("completed native scratch query owner");
        Ok(row.map(|row| OwnedSqlRow { row, permit }))
    }

    /// The caller may create only owned source files inside this exclusively
    /// created directory. Inventory cleanup keeps ownership through SQLx close.
    pub(super) fn private_directory(&self) -> PackedResult<&Path> {
        self.storage
            .as_ref()
            .and_then(|storage| storage.directory.as_ref())
            .map(|directory| directory.path.as_path())
            .ok_or_else(|| PackedWireError::Invalid("private scratch storage is closed".into()))
    }

    pub(super) async fn semantic_fetch_optional<'q>(
        &mut self,
        make_query: impl FnOnce() -> sea_orm::sqlx::query::Query<
            'q,
            sea_orm::sqlx::Sqlite,
            sea_orm::sqlx::sqlite::SqliteArguments<'q>,
        >,
    ) -> PackedResult<Option<OwnedSqlRow>> {
        self.start_query()?;
        let connection = self.storage.as_mut().unwrap().connection.as_mut().unwrap();
        let result = sql_await(
            make_query().fetch_optional(connection),
            &self.budget,
            &self.cancel,
        )
        .await;
        let row = self.checked_sql(result)?;
        let permit = self
            .storage
            .as_mut()
            .unwrap()
            .active_query
            .take()
            .expect("completed semantic query owner");
        Ok(row.map(|row| OwnedSqlRow { row, permit }))
    }

    /// Only semantic scratch storage opts into this callback. Existing physical
    /// inventories and their accepted behavior remain unchanged. The callback
    /// itself stays with SQLx until its real connection-close acknowledgment.
    pub(super) async fn semantic_set_vm_limit(
        &mut self,
        max_steps: u64,
    ) -> PackedResult<Arc<SemanticVmWork>> {
        if max_steps == 0 {
            return Err(PackedWireError::LimitExceeded(
                "semantic SQL VM quota is zero".into(),
            ));
        }
        self.start_query()?;
        let work = Arc::new(SemanticVmWork {
            remaining: AtomicU64::new(max_steps),
            used: AtomicU64::new(0),
            exhausted: AtomicBool::new(false),
        });
        let worker_work = work.clone();
        let worker_budget = self.budget.clone();
        let worker_cancel = self.cancel.clone();
        let result = {
            let connection = self.storage.as_mut().unwrap().connection.as_mut().unwrap();
            match sql_await(connection.lock_handle(), &self.budget, &self.cancel).await {
                Ok(mut handle) => {
                    // Quantum one bounds short statements too; no uncharged
                    // sub-quantum work can accumulate across repeated queries.
                    handle.set_progress_handler(1, move || {
                        !worker_cancel.is_cancelled()
                            && !worker_budget.state().closed
                            && worker_work.step()
                    });
                    Ok(())
                }
                Err(error) => Err(error),
            }
        };
        self.checked_sql(result)?;
        self.finish_query();
        Ok(work)
    }

    pub(super) async fn close(mut self) -> PackedResult<()> {
        let Some(storage) = self.storage.take() else {
            return Ok(());
        };
        // Cleanup deliberately survives verification cancellation and runtime
        // teardown. No evidence is returned until worker shutdown is confirmed.
        storage.close().await
    }

    #[cfg(test)]
    fn directory_path(&self) -> &Path {
        &self
            .storage
            .as_ref()
            .unwrap()
            .directory
            .as_ref()
            .unwrap()
            .path
    }
}

#[cfg(test)]
struct QueryOwnerTestBarrierState {
    entered: tokio::sync::Notify,
    released: Mutex<bool>,
    release_changed: std::sync::Condvar,
    blocked: AtomicBool,
}

#[cfg(test)]
impl QueryOwnerTestBarrierState {
    fn block_worker(&self) -> bool {
        self.blocked.store(true, Ordering::Release);
        self.entered.notify_one();
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.release_changed.wait(released).unwrap();
        }
        self.blocked.store(false, Ordering::Release);
        // Interrupt this real sqlite3_step once the test permits cleanup.
        false
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.release_changed.notify_all();
    }
}

/// Test-only ownership probe on the real SQLite worker. Dropping the guard
/// releases the worker even if setup, a timeout or an assertion fails.
#[cfg(test)]
pub(super) struct QueryOwnerTestBarrier {
    state: Arc<QueryOwnerTestBarrierState>,
}

#[cfg(test)]
impl QueryOwnerTestBarrier {
    pub(super) async fn entered(&self) {
        self.state.entered.notified().await;
    }

    pub(super) fn is_blocked(&self) -> bool {
        self.state.blocked.load(Ordering::Acquire)
    }

    pub(super) fn release(&self) {
        self.state.release();
    }
}

#[cfg(test)]
impl Drop for QueryOwnerTestBarrier {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
impl PhysicalInventory {
    pub(super) async fn install_query_owner_test_barrier(
        &mut self,
    ) -> PackedResult<QueryOwnerTestBarrier> {
        // Warm this exact statement before replacing the test connection's
        // progress callback, so the barrier stops sqlite3_step, not preparation.
        self.semantic_execute(|| sea_orm::sqlx::query("SELECT 1"))
            .await?;
        let barrier = QueryOwnerTestBarrier {
            state: Arc::new(QueryOwnerTestBarrierState {
                entered: tokio::sync::Notify::new(),
                released: Mutex::new(false),
                release_changed: std::sync::Condvar::new(),
                blocked: AtomicBool::new(false),
            }),
        };
        let connection = self.storage.as_mut().unwrap().connection.as_mut().unwrap();
        let mut handle = connection.lock_handle().await.map_err(sql_error)?;
        let worker_barrier = barrier.state.clone();
        let mut first_callback = true;
        // Only this disposable test connection replaces its VM callback; the
        // caller releases the barrier and closes it before checking observations.
        handle.set_progress_handler(1, move || {
            if first_callback {
                first_callback = false;
                worker_barrier.block_worker()
            } else {
                true
            }
        });
        Ok(barrier)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace_overlay::packed_v3::wire005::{
        V3BudgetPool, V3MountBudget, V3ObjectKind, V3ObjectRef,
    };
    use tokio_util::sync::CancellationToken;

    fn limits() -> InventoryLimits {
        InventoryLimits {
            max_objects: 256,
            max_declared_bytes: 4 << 20,
            max_disk_bytes: 256 << 10,
            sqlite_cache_bytes: 64 << 10,
        }
    }

    fn reference(key: &str) -> V3ObjectRef {
        V3ObjectRef {
            key: key.into(),
            kind: V3ObjectKind::InodeIndex,
            object_len: 8192,
            digest: [7; 32],
        }
    }

    #[tokio::test]
    async fn inventory_exact_identity_reuses_only_the_same_physical_key() {
        let parent = tempfile::tempdir().unwrap();
        let mut inventory = PhysicalInventory::create(
            parent.path(),
            V3MountBudget::defaults(),
            limits(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let first = reference("packed/a");
        assert_eq!(
            inventory.register(&first).await.unwrap(),
            Registration {
                is_new: true,
                authenticated: false,
            }
        );
        assert_eq!(inventory.stats().authenticated_objects, 0);
        assert!(inventory.final_digest().await.is_err());
        inventory.mark_authenticated(&first).await.unwrap();
        assert_eq!(
            inventory.register(&first).await.unwrap(),
            Registration {
                is_new: false,
                authenticated: true,
            }
        );
        inventory.mark_authenticated(&first).await.unwrap();
        let second = reference("packed/b");
        inventory.register(&second).await.unwrap();
        assert_eq!(inventory.stats().registered_objects, 2);
        assert_eq!(inventory.stats().declared_bytes, 16384);
        assert_eq!(inventory.stats().authenticated_objects, 1);
        assert_eq!(inventory.stats().authenticated_bytes, 8192);
        assert!(inventory.final_digest().await.is_err());
        inventory.mark_authenticated(&second).await.unwrap();
        assert_eq!(inventory.stats().authenticated_objects, 2);
        inventory.final_digest().await.unwrap();
        inventory.close().await.unwrap();
    }

    #[tokio::test]
    async fn inventory_rejects_each_same_key_identity_conflict_and_unknown_authentication() {
        for identity_part in 0..3 {
            let parent = tempfile::tempdir().unwrap();
            let mut inventory = PhysicalInventory::create(
                parent.path(),
                V3MountBudget::defaults(),
                limits(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
            let first = reference("packed/identity");
            assert!(inventory.mark_authenticated(&first).await.is_err());
            inventory.register(&first).await.unwrap();
            let mut conflict = first.clone();
            match identity_part {
                0 => conflict.kind = V3ObjectKind::ColdIndex,
                1 => conflict.object_len += 1,
                _ => conflict.digest[0] ^= 1,
            }
            assert!(inventory.register(&conflict).await.is_err());
            assert!(inventory.mark_authenticated(&conflict).await.is_err());
            assert_eq!(inventory.stats().registered_objects, 1);
            assert_eq!(inventory.stats().authenticated_objects, 0);
            inventory.mark_authenticated(&first).await.unwrap();
            inventory.final_digest().await.unwrap();
            inventory.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn inventory_digest_is_order_independent_and_binds_physical_key() {
        async fn digest(keys: &[&str]) -> [u8; 32] {
            let parent = tempfile::tempdir().unwrap();
            let mut inventory = PhysicalInventory::create(
                parent.path(),
                V3MountBudget::defaults(),
                limits(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
            for key in keys {
                let object = reference(key);
                inventory.register(&object).await.unwrap();
                inventory.mark_authenticated(&object).await.unwrap();
            }
            let result = inventory.final_digest().await.unwrap();
            inventory.close().await.unwrap();
            result
        }
        assert_eq!(
            digest(&["packed/z", "packed/a", "packed/m"]).await,
            digest(&["packed/m", "packed/z", "packed/a"]).await
        );
        assert_ne!(digest(&["packed/a"]).await, digest(&["packed/b"]).await);
        assert_ne!(digest(&[]).await, digest(&["packed/a"]).await);
    }

    #[tokio::test]
    async fn inventory_pending_reference_owns_admission_until_consumer_releases_it() {
        let parent = tempfile::tempdir().unwrap();
        let budget = V3MountBudget::defaults();
        let mut inventory = PhysicalInventory::create(
            parent.path(),
            budget.clone(),
            limits(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let first = reference("packed/a");
        let second = reference("packed/z");
        inventory.register(&second).await.unwrap();
        inventory.register(&first).await.unwrap();
        let pending = inventory.next_pending().await.unwrap().unwrap();
        assert_eq!(&*pending, &first);
        assert!(budget.state().used[V3BudgetPool::Metadata as usize] > 0);
        inventory.mark_authenticated(&pending).await.unwrap();
        assert_eq!(&*inventory.next_pending().await.unwrap().unwrap(), &second);
        inventory.mark_authenticated(&second).await.unwrap();
        assert!(inventory.next_pending().await.unwrap().is_none());
        inventory.close().await.unwrap();
        assert!(budget.state().used[V3BudgetPool::Metadata as usize] > 0);
        assert_eq!(budget.state().used[V3BudgetPool::Workspace as usize], 0);
        drop(pending);
        assert_eq!(budget.state().used[V3BudgetPool::Metadata as usize], 0);
    }

    #[tokio::test]
    async fn inventory_quota_rejects_before_counting_an_unregistered_object() {
        for object_quota in [true, false] {
            let parent = tempfile::tempdir().unwrap();
            let mut bounded = limits();
            if object_quota {
                bounded.max_objects = 1;
            } else {
                bounded.max_declared_bytes = 8192;
            }
            let mut inventory = PhysicalInventory::create(
                parent.path(),
                V3MountBudget::defaults(),
                bounded,
                CancellationToken::new(),
            )
            .await
            .unwrap();
            let first = reference("packed/a");
            inventory.register(&first).await.unwrap();
            inventory.mark_authenticated(&first).await.unwrap();
            assert!(inventory.register(&reference("packed/b")).await.is_err());
            assert_eq!(inventory.stats().registered_objects, 1);
            assert_eq!(inventory.stats().authenticated_objects, 1);
            inventory.final_digest().await.unwrap();
            inventory.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn inventory_disk_quota_failure_taints_inventory_and_preserves_siblings() {
        let parent = tempfile::tempdir().unwrap();
        let sentinel = parent.path().join("existing-evidence");
        std::fs::write(&sentinel, b"keep").unwrap();
        let mut bounded = limits();
        bounded.max_disk_bytes = 16 << 10;
        let mut inventory = PhysicalInventory::create(
            parent.path(),
            V3MountBudget::defaults(),
            bounded,
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let directory = inventory.directory_path().to_path_buf();
        let mut hit_disk_limit = false;
        for ordinal in 0..100 {
            let object = reference(&format!("packed/{ordinal:03}/{}", "k".repeat(3000)));
            match inventory.register(&object).await {
                Ok(_) => inventory.mark_authenticated(&object).await.unwrap(),
                Err(_) => {
                    hit_disk_limit = true;
                    break;
                }
            }
        }
        assert!(hit_disk_limit);
        assert!(
            std::fs::metadata(directory.join("inventory.sqlite"))
                .unwrap()
                .len()
                <= 16 << 10
        );
        assert!(inventory.final_digest().await.is_err());
        inventory.close().await.unwrap();
        assert!(!directory.exists());
        assert_eq!(std::fs::read(sentinel).unwrap(), b"keep");
    }

    #[tokio::test]
    async fn inventory_cancellation_and_budget_close_release_owners_without_closing_shared_budget()
    {
        for close_budget in [false, true] {
            let parent = tempfile::tempdir().unwrap();
            let budget = V3MountBudget::defaults();
            let cancel = CancellationToken::new();
            let mut inventory =
                PhysicalInventory::create(parent.path(), budget.clone(), limits(), cancel.clone())
                    .await
                    .unwrap();
            let directory = inventory.directory_path().to_path_buf();
            assert!(budget.state().used[V3BudgetPool::Workspace as usize] > 0);
            if close_budget {
                budget.close();
            } else {
                cancel.cancel();
            }
            assert!(inventory.register(&reference("packed/a")).await.is_err());
            assert!(inventory.final_digest().await.is_err());
            assert_eq!(budget.state().closed, close_budget);
            inventory.close().await.unwrap();
            assert!(!directory.exists());
            assert!(budget.state().used.iter().all(|used| *used == 0));
        }
    }

    #[tokio::test]
    async fn inventory_precancelled_creation_leaves_no_directory_or_budget_owner() {
        let parent = tempfile::tempdir().unwrap();
        let budget = V3MountBudget::defaults();
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(
            PhysicalInventory::create(parent.path(), budget.clone(), limits(), cancel)
                .await
                .is_err()
        );
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
        assert!(budget.state().used.iter().all(|used| *used == 0));
        assert!(!budget.state().closed);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn inventory_private_directory_and_database_permissions_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let parent = tempfile::tempdir().unwrap();
        let inventory = PhysicalInventory::create(
            parent.path(),
            V3MountBudget::defaults(),
            limits(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            std::fs::metadata(inventory.directory_path())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(inventory.directory_path().join("inventory.sqlite"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        inventory.close().await.unwrap();
    }

    #[tokio::test]
    async fn inventory_drop_cleans_only_owned_files_after_sqlite_worker_shutdown() {
        let parent = tempfile::tempdir().unwrap();
        let budget = V3MountBudget::defaults();
        let sentinel = parent.path().join("keep-evidence");
        std::fs::write(&sentinel, b"keep").unwrap();
        let inventory = PhysicalInventory::create(
            parent.path(),
            budget.clone(),
            limits(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let directory = inventory.directory_path().to_path_buf();
        drop(inventory);
        tokio::time::timeout(Duration::from_secs(2), async {
            while directory.exists() || budget.state().used.iter().any(|bytes| *bytes != 0) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(std::fs::read(sentinel).unwrap(), b"keep");
        assert!(!budget.state().closed);
    }

    #[tokio::test]
    async fn inventory_keeps_payload_control_admission_available_at_minimum_capacity() {
        use crate::workspace_overlay::packed_v3::wire005::V3BudgetLimits;

        let parent = tempfile::tempdir().unwrap();
        let mut budgets = V3BudgetLimits::default();
        budgets.bytes[V3BudgetPool::Control as usize] = 32 << 10;
        let budget = V3MountBudget::new(budgets).unwrap();
        let inventory = PhysicalInventory::create(
            parent.path(),
            budget.clone(),
            limits(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let payload_control = budget.admit(&[(V3BudgetPool::Control, 16 << 10)]).unwrap();
        inventory.close().await.unwrap();
        assert_eq!(
            budget.state().used[V3BudgetPool::Control as usize],
            16 << 10
        );
        drop(payload_control);
        assert!(budget.state().used.iter().all(|used| *used == 0));
    }

    #[tokio::test]
    async fn inventory_sql_wait_cancellation_taints_state_until_confirmed_close() {
        let parent = tempfile::tempdir().unwrap();
        let budget = V3MountBudget::defaults();
        let cancel = CancellationToken::new();
        let mut inventory =
            PhysicalInventory::create(parent.path(), budget.clone(), limits(), cancel.clone())
                .await
                .unwrap();
        let directory = inventory.directory_path().to_path_buf();
        let mut blocker = SqliteConnection::connect_with(
            &SqliteConnectOptions::new()
                .filename(directory.join("inventory.sqlite"))
                .create_if_missing(false),
        )
        .await
        .unwrap();
        sea_orm::sqlx::query("BEGIN EXCLUSIVE")
            .execute(&mut blocker)
            .await
            .unwrap();
        let object = reference("packed/blocked");
        {
            let registration = inventory.register(&object);
            tokio::pin!(registration);
            assert!(futures::poll!(&mut registration).is_pending());
            cancel.cancel();
            assert!(registration.await.is_err());
        }
        assert_eq!(inventory.stats().registered_objects, 0);
        assert_eq!(inventory.stats().authenticated_objects, 0);
        assert!(inventory.final_digest().await.is_err());
        assert!(budget.state().used[V3BudgetPool::Workspace as usize] > 0);
        sea_orm::sqlx::query("ROLLBACK")
            .execute(&mut blocker)
            .await
            .unwrap();
        blocker.close().await.unwrap();
        inventory.close().await.unwrap();
        assert!(!directory.exists());
        assert!(budget.state().used.iter().all(|used| *used == 0));
        assert!(!budget.state().closed);
    }

    #[test]
    fn inventory_drop_cleanup_survives_tokio_runtime_exit() {
        let parent = tempfile::tempdir().unwrap();
        let budget = V3MountBudget::defaults();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let inventory = runtime
            .block_on(PhysicalInventory::create(
                parent.path(),
                budget.clone(),
                limits(),
                CancellationToken::new(),
            ))
            .unwrap();
        let directory = inventory.directory_path().to_path_buf();
        drop(runtime);
        drop(inventory);
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while directory.exists() || budget.state().used.iter().any(|used| *used != 0) {
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        }
        assert!(!budget.state().closed);
    }

    #[derive(Clone, Copy, Debug)]
    enum QueryOwnerInterruption {
        TokenCancel,
        BudgetClose,
        CallerFutureDrop,
    }

    struct QueryOwnerProgressBarrier {
        entered: tokio::sync::Notify,
        released: std::sync::Mutex<bool>,
        release_changed: std::sync::Condvar,
        blocked: std::sync::atomic::AtomicBool,
    }

    impl QueryOwnerProgressBarrier {
        fn new() -> Self {
            Self {
                entered: tokio::sync::Notify::new(),
                released: std::sync::Mutex::new(false),
                release_changed: std::sync::Condvar::new(),
                blocked: std::sync::atomic::AtomicBool::new(false),
            }
        }

        fn block_worker(&self) -> bool {
            use std::sync::atomic::Ordering;

            self.blocked.store(true, Ordering::Release);
            self.entered.notify_one();
            let mut released = self.released.lock().unwrap();
            while !*released {
                released = self.release_changed.wait(released).unwrap();
            }
            self.blocked.store(false, Ordering::Release);
            // Interrupt this real sqlite3_step once the test permits cleanup.
            false
        }

        fn release(&self) {
            *self.released.lock().unwrap() = true;
            self.release_changed.notify_all();
        }
    }

    struct QueryOwnerBarrierRelease(std::sync::Arc<QueryOwnerProgressBarrier>);

    impl Drop for QueryOwnerBarrierRelease {
        fn drop(&mut self) {
            // Setup/time-out failures must never leave the SQLite worker blocked.
            self.0.release();
        }
    }

    async fn query_owner_survives_interruption_until_worker_close(
        interruption: QueryOwnerInterruption,
    ) {
        use std::sync::atomic::Ordering;

        let parent = tempfile::tempdir().unwrap();
        let budget = V3MountBudget::defaults();
        let cancel = CancellationToken::new();
        let mut inventory =
            PhysicalInventory::create(parent.path(), budget.clone(), limits(), cancel.clone())
                .await
                .unwrap();
        let directory = inventory.directory_path().to_path_buf();
        // OR05 accepts 1..=4096 UTF-8 key bytes with nonempty, non-dot components.
        // This fixture exercises SQLx's owned 4096-byte bind, without backend I/O.
        let key = format!("packed/{}", "a".repeat(4096 - "packed/".len()));
        let object = reference(&key);
        let encoded_reference = object.encode_value().unwrap();
        let key_is_valid = object.key.len() == 4096;
        drop(encoded_reference);
        // Warm the parameterized existing-row SELECT in SQLx's statement cache,
        // so the progress callback below stops sqlite3_step, not preparation.
        inventory.register(&object).await.unwrap();
        let baseline_metadata = budget.state().used[V3BudgetPool::Metadata as usize];

        let barrier = std::sync::Arc::new(QueryOwnerProgressBarrier::new());
        let release_on_drop = QueryOwnerBarrierRelease(barrier.clone());
        {
            let connection = inventory
                .storage
                .as_mut()
                .unwrap()
                .connection
                .as_mut()
                .unwrap();
            let mut handle = connection.lock_handle().await.unwrap();
            let worker_barrier = barrier.clone();
            let mut first_callback = true;
            handle.set_progress_handler(1, move || {
                if first_callback {
                    first_callback = false;
                    worker_barrier.block_worker()
                } else {
                    true
                }
            });
        }

        let reached_worker_barrier;
        let interruption_completed;
        {
            let registration = inventory.register(&object);
            tokio::pin!(registration);
            reached_worker_barrier = tokio::time::timeout(Duration::from_secs(5), async {
                tokio::select! {
                    _ = barrier.entered.notified() => true,
                    _ = &mut registration => false,
                }
            })
            .await
            .is_ok_and(|entered| entered);
            interruption_completed = if reached_worker_barrier {
                match interruption {
                    QueryOwnerInterruption::TokenCancel => {
                        cancel.cancel();
                        tokio::time::timeout(Duration::from_secs(2), &mut registration)
                            .await
                            .is_ok_and(|result| result.is_err())
                    }
                    QueryOwnerInterruption::BudgetClose => {
                        budget.close();
                        tokio::time::timeout(Duration::from_secs(2), &mut registration)
                            .await
                            .is_ok_and(|result| result.is_err())
                    }
                    QueryOwnerInterruption::CallerFutureDrop => true,
                }
            } else {
                false
            };
            // Drop the pinned future itself at the scope boundary. Dropping only
            // a Pin<&mut _> would keep the owned registration future alive.
        }
        let worker_still_blocked = barrier.blocked.load(Ordering::Acquire);
        let held_metadata = budget.state().used[V3BudgetPool::Metadata as usize];
        let directory_still_owned = directory.exists();
        let (caller_drop_reuse_rejected, caller_drop_reuse_preserved_metadata) =
            if matches!(interruption, QueryOwnerInterruption::CallerFutureDrop) {
                // Reusing a dropped operation must not enqueue another SQL command
                // or replace the still-live worker's query reservation.
                let rejected =
                    tokio::time::timeout(Duration::from_millis(200), inventory.register(&object))
                        .await
                        .is_ok_and(|result| result.is_err());
                let preserved_metadata =
                    budget.state().used[V3BudgetPool::Metadata as usize] == held_metadata;
                (rejected, preserved_metadata)
            } else {
                (true, true)
            };

        // Capture the RED observation first; release and close before asserting
        // it, so the intentional failure cannot leave a test-owned worker alive.
        barrier.release();
        let close_result = inventory.close().await;
        drop(release_on_drop);
        let after_close = budget.state();
        let directory_removed = !directory.exists();
        assert!(close_result.is_ok(), "{interruption:?}: {close_result:?}");
        assert!(
            directory_removed,
            "{interruption:?}: scratch directory remains"
        );
        assert!(
            after_close.used.iter().all(|bytes| *bytes == 0),
            "{interruption:?}: owners remain after acknowledged close: {:?}",
            after_close.used
        );
        assert!(key_is_valid, "fixture must bind a valid 4096-byte OR05 key");
        assert!(
            reached_worker_barrier,
            "{interruption:?}: real worker never entered barrier"
        );
        assert!(
            interruption_completed,
            "{interruption:?}: foreground interruption timed out"
        );
        assert!(
            worker_still_blocked,
            "{interruption:?}: worker finished before ownership capture"
        );
        assert!(
            directory_still_owned,
            "{interruption:?}: scratch released before worker close"
        );
        assert!(
            held_metadata >= baseline_metadata + QUERY_MEMORY_BYTES,
            "{interruption:?}: blocked SQLx worker lost query ownership: baseline={baseline_metadata}, held={held_metadata}, query={QUERY_MEMORY_BYTES}"
        );
        assert!(
            caller_drop_reuse_rejected,
            "caller future drop: reuse did not promptly reject an unfinished worker"
        );
        assert!(
            caller_drop_reuse_preserved_metadata,
            "caller future drop: reuse changed the unfinished query's Metadata reservation"
        );
    }

    #[tokio::test]
    async fn inventory_query_owner_token_cancel_retains_metadata_until_worker_close() {
        query_owner_survives_interruption_until_worker_close(QueryOwnerInterruption::TokenCancel)
            .await;
    }

    #[tokio::test]
    async fn inventory_query_owner_budget_close_retains_metadata_until_worker_close() {
        query_owner_survives_interruption_until_worker_close(QueryOwnerInterruption::BudgetClose)
            .await;
    }

    #[tokio::test]
    async fn inventory_query_owner_caller_future_drop_retains_metadata_until_worker_close() {
        query_owner_survives_interruption_until_worker_close(
            QueryOwnerInterruption::CallerFutureDrop,
        )
        .await;
    }
}
