//! TiKV transactional substrate for the workspace catalog.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use rand::{RngCore, rng};
use tikv_client::{BoundRange, CheckLevel, Key, KvPair, TransactionClient, TransactionOptions};

use super::kv_backend::{
    KvCheck, KvEntry, KvReadLimits, KvWrite, WorkspaceKvBackend, validate_cas_time_window,
};
use crate::workspace_overlay::error::WorkspaceError;
use crate::workspace_overlay::packed_v3::wire005::{V3BudgetPool, V3MountBudget};

const SCAN_BATCH_LIMIT: u32 = 1024;
const TXN_MAX_RETRIES: usize = 10;
// Only namespaces without CONTROL serialize here. The key is locked but
// never stored; established catalogs retain their independent entity CAS.
const BOOTSTRAP_BARRIER: &[u8] = b"\0manager-bootstrap";
// A scan response may carry a lock error in one KvPair while the transaction
// remains read-only and safe to retry at its fixed timestamp. Keep this
// continuation budget separate from region/page admission: exhausting it
// returns Busy so the caller can rebuild the bounded read transaction.
const BOUNDED_SCAN_LOCK_RETRIES: usize = 20;
const BOUNDED_SCAN_LOCK_BACKOFF: Duration = Duration::from_millis(20);
// This same configured timeout derives the fixed data Get-batch deadline.
const BOUNDED_CLIENT_RPC_TIMEOUT: Duration = Duration::from_secs(2);
// Concurrent mounts can open several PD channels at once.  A transient
// transport failure while establishing a client is safe to retry because no
// transaction has been started yet; request deadlines remain unchanged.
const CLIENT_CONNECT_RETRIES: usize = 6;
const CLIENT_CONNECT_BACKOFF: Duration = Duration::from_millis(50);
#[path = "tikv_bounded_read.rs"]
mod bounded_read;
#[path = "tikv_tls.rs"]
mod tls;
pub use tls::TiKvTlsConfig;
/// This is a protobuf/gRPC message envelope limit, not a stored-value limit.
pub const BOUNDED_RECEIPT_MESSAGE_BYTES: usize = 8 << 10;
pub const BOUNDED_READ_MESSAGE_BYTES: usize = 16 << 10;
pub const BOUNDED_JOURNAL_MESSAGE_BYTES: usize = 64 << 10;
/// A full Linux xattr plus its native delta envelope exceeds 64 KiB.
pub const BOUNDED_XATTR_VALUE_BYTES: usize = 96 << 10;
pub const BOUNDED_XATTR_MESSAGE_BYTES: usize = 128 << 10;
/// Generic catalog pages admit larger records than native delta pages while
/// remaining within a fixed decoder envelope.
pub const BOUNDED_GENERIC_VALUE_BYTES: usize = 256 << 10;
pub const BOUNDED_GENERIC_MESSAGE_BYTES: usize = 512 << 10;
pub const BOUNDED_READ_MAX_POINT_KEYS: usize = 32;
pub const BOUNDED_READ_MAX_SCAN_PAGES: usize = 1024;
const CLIENT_RESIDENT_METADATA_BYTES: u64 = 4 << 20;
const CLIENT_RESIDENT_ROOTS_BYTES: u64 = 64 << 10;

fn authentication_rebuild_bytes_fit(total: usize, checks: &[KvCheck], limit: usize) -> bool {
    checks
        .iter()
        .try_fold(total, |bytes, check| {
            bytes
                .checked_add(check.key.len())?
                .checked_add(check.expected.as_ref().map_or(0, Vec::len))
        })
        .is_some_and(|bytes| bytes <= limit)
}

fn pessimistic_checks_in_key_order(checks: &[KvCheck]) -> Vec<&KvCheck> {
    let mut ordered = checks.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| left.key.cmp(&right.key));
    ordered
}

fn pessimistic_lock_keys<'a>(checks: &'a [KvCheck], writes: &'a [KvWrite]) -> Vec<&'a [u8]> {
    let mut keys = checks
        .iter()
        .map(|check| check.key.as_slice())
        .chain(writes.iter().map(|write| match write {
            KvWrite::Put { key, .. } | KvWrite::Delete { key } => key.as_slice(),
        }))
        .collect::<Vec<_>>();
    keys.sort_unstable();
    keys.dedup();
    keys
}

#[derive(Default)]
struct ClientSlots {
    main: Option<TransactionClient>,
    receipt: Option<TransactionClient>,
    small: Option<TransactionClient>,
    journal: Option<TransactionClient>,
    xattr: Option<TransactionClient>,
    generic: Option<TransactionClient>,
}

impl Drop for ClientSlots {
    fn drop(&mut self) {
        // Startup failure or cancelled administration still begins cancellation.
        // This is not a join receipt; SDK tasks retain the resident lease until
        // their terminal state. Normal mounts use shutdown().await explicitly.
        for client in [
            &self.main,
            &self.receipt,
            &self.small,
            &self.journal,
            &self.xattr,
            &self.generic,
        ]
        .into_iter()
        .flatten()
        {
            client.cancel_background_tasks();
        }
    }
}

#[derive(Default)]
struct OperationState {
    closed: bool,
    active: usize,
}

#[derive(Default)]
struct Operations {
    state: Mutex<OperationState>,
    changed: tokio::sync::Notify,
}

struct OperationOwner(Arc<Operations>);

impl Operations {
    fn enter(self: &Arc<Self>) -> Result<OperationOwner, WorkspaceError> {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(backend("TiKV workspace backend is shut down"));
        }
        state.active = state
            .active
            .checked_add(1)
            .ok_or_else(|| backend("TiKV operation owner overflow"))?;
        Ok(OperationOwner(self.clone()))
    }
    async fn close_and_drain(&self) {
        self.state.lock().unwrap().closed = true;
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.state.lock().unwrap().active == 0 {
                break;
            }
            changed.await;
        }
    }
}

impl Drop for OperationOwner {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap();
        state.active -= 1;
        drop(state);
        self.0.changed.notify_waiters();
    }
}

// The receipt caller's 8 KiB wire contract is real: choose a distinct SDK
// decoder cap, never substitute the larger 16 KiB small-record envelope.
fn bounded_read_message_bytes(limits: KvReadLimits) -> Result<usize, WorkspaceError> {
    limits.validate()?;
    let message_bytes = if limits.max_value_bytes <= 4 << 10 {
        BOUNDED_RECEIPT_MESSAGE_BYTES
    } else if limits.max_value_bytes <= 12 << 10 {
        BOUNDED_READ_MESSAGE_BYTES
    } else if limits.max_value_bytes <= 48 << 10 {
        BOUNDED_JOURNAL_MESSAGE_BYTES
    } else if limits.max_value_bytes <= BOUNDED_XATTR_VALUE_BYTES {
        BOUNDED_XATTR_MESSAGE_BYTES
    } else if limits.max_value_bytes <= BOUNDED_GENERIC_VALUE_BYTES {
        BOUNDED_GENERIC_MESSAGE_BYTES
    } else {
        return Err(WorkspaceError::UnsupportedCapability(
            "TiKV bounded read record schema larger than 256 KiB",
        ));
    };
    if limits.max_response_bytes < message_bytes {
        return Err(WorkspaceError::InvalidReadPlan(
            "TiKV bounded read envelope exceeds caller hard response limit".into(),
        ));
    }
    Ok(message_bytes)
}

fn client_config_with_tls(
    budget: &Arc<V3MountBudget>,
    message_bytes: usize,
    tls: Option<&TiKvTlsConfig>,
) -> Result<tikv_client::Config, WorkspaceError> {
    // PD materialization has an independent 16 KiB wire cap. This reservation
    // covers retained membership, connector/config copies and protobuf capacity
    // slack; data response ownership remains in the admitted caller operation.
    let permit = budget
        .admit(&[
            (
                V3BudgetPool::Metadata,
                CLIENT_RESIDENT_METADATA_BYTES
                    + tls.map_or(0, |_| tls::TLS_RESIDENT_METADATA_BYTES),
            ),
            (V3BudgetPool::Roots, CLIENT_RESIDENT_ROOTS_BYTES),
        ])
        .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
    let config = tikv_client::Config::default()
        .with_timeout(BOUNDED_CLIENT_RPC_TIMEOUT)
        .with_grpc_max_decoding_message_size(message_bytes)
        .with_grpc_accept_gzip(false)
        .with_bounded_get_scan_schema(matches!(
            message_bytes,
            BOUNDED_RECEIPT_MESSAGE_BYTES
                | BOUNDED_READ_MESSAGE_BYTES
                | BOUNDED_JOURNAL_MESSAGE_BYTES
                | BOUNDED_XATTR_MESSAGE_BYTES
                | BOUNDED_GENERIC_MESSAGE_BYTES
        ))
        .with_bounded_read_lock_conflicts(
            if matches!(
                message_bytes,
                BOUNDED_RECEIPT_MESSAGE_BYTES
                    | BOUNDED_READ_MESSAGE_BYTES
                    | BOUNDED_JOURNAL_MESSAGE_BYTES
                    | BOUNDED_XATTR_MESSAGE_BYTES
                    | BOUNDED_GENERIC_MESSAGE_BYTES
            ) {
                4096
            } else {
                0
            },
        )
        .with_bounded_resources(tikv_client::ClientResourceOwner::new(Arc::new((
            permit,
            tls.cloned(),
        ))));
    match tls {
        Some(tls) => tls.apply(config),
        None => Ok(config),
    }
}

async fn connect_transaction_client(
    pd_endpoints: &[String],
    config: tikv_client::Config,
) -> Result<TransactionClient, WorkspaceError> {
    for attempt in 0..=CLIENT_CONNECT_RETRIES {
        match TransactionClient::new_with_config(pd_endpoints.to_vec(), config.clone()).await {
            Ok(client) => return Ok(client),
            Err(error @ tikv_client::Error::Grpc(_)) if attempt < CLIENT_CONNECT_RETRIES => {
                tokio::time::sleep(CLIENT_CONNECT_BACKOFF * (attempt as u32 + 1)).await;
                log::debug!(
                    "retrying TiKV client connection after transport failure (attempt {})",
                    attempt + 1
                );
                drop(error);
            }
            Err(error) => return Err(backend(error)),
        }
    }
    unreachable!("TiKV client connection loop always returns")
}

#[derive(Clone)]
pub struct TiKvWorkspaceBackend {
    clients: Arc<tokio::sync::Mutex<ClientSlots>>,
    prefix: Vec<u8>,
    namespace: String,
    pd_endpoints: std::sync::Arc<Vec<String>>,
    mount_budget: Arc<V3MountBudget>,
    tls: Option<TiKvTlsConfig>,
    operator_admin: Option<TiKvOperatorAdminIdentity>,
    operations: Arc<Operations>,
    established_namespace: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(test)]
    test_control: std::sync::Arc<tests::commit_failure_candidates::CasTestControl>,
}

#[derive(Clone)]
struct TiKvOperatorAdminIdentity {
    admin_leaf: [u8; 32],
    runtime_leaf: [u8; 32],
}

impl TiKvWorkspaceBackend {
    pub async fn connect(
        pd_endpoints: Vec<String>,
        namespace: &str,
    ) -> Result<Self, WorkspaceError> {
        Self::connect_with_budget(pd_endpoints, namespace, V3MountBudget::defaults()).await
    }

    /// All production catalog clients share the mount/session/GC ledger.
    pub async fn connect_with_budget(
        pd_endpoints: Vec<String>,
        namespace: &str,
        mount_budget: Arc<V3MountBudget>,
    ) -> Result<Self, WorkspaceError> {
        Self::connect_with_optional_tls(pd_endpoints, namespace, mount_budget, None).await
    }

    /// Authenticated transport for both runtime and operator connections. This
    /// constructor alone does not grant any administrator/GC capability.
    pub async fn connect_with_tls_and_budget(
        pd_endpoints: Vec<String>,
        namespace: &str,
        mount_budget: Arc<V3MountBudget>,
        tls: TiKvTlsConfig,
    ) -> Result<Self, WorkspaceError> {
        Self::connect_with_optional_tls(pd_endpoints, namespace, mount_budget, Some(tls)).await
    }

    /// The trusted operator Secret resolver assigns the two credential roles.
    /// Both distinct certificate identities must authenticate to the actual
    /// cluster; possession of a runtime certificate alone cannot enter this
    /// path. This is an application TCB boundary, not native TiKV RPC ACL.
    pub async fn connect_operator_admin_with_budget(
        pd_endpoints: Vec<String>,
        namespace: &str,
        mount_budget: Arc<V3MountBudget>,
        admin_tls: TiKvTlsConfig,
        runtime_tls: TiKvTlsConfig,
    ) -> Result<Self, WorkspaceError> {
        let identity = TiKvOperatorAdminIdentity {
            admin_leaf: admin_tls.leaf_fingerprint()?,
            runtime_leaf: runtime_tls.leaf_fingerprint()?,
        };
        if identity.admin_leaf == identity.runtime_leaf {
            return Err(WorkspaceError::UnsupportedCapability(
                "operator and runtime TiKV certificate identities must differ",
            ));
        }
        let runtime = Self::connect_with_tls_and_budget(
            pd_endpoints.clone(),
            namespace,
            mount_budget.clone(),
            runtime_tls,
        )
        .await?;
        let authenticated = runtime.probe_authenticated_connection().await;
        let drained = runtime.shutdown().await;
        authenticated?;
        drained?;
        let mut admin =
            Self::connect_with_tls_and_budget(pd_endpoints, namespace, mount_budget, admin_tls)
                .await?;
        if let Err(error) = admin.probe_authenticated_connection().await {
            admin.shutdown().await?;
            return Err(error);
        }
        admin.operator_admin = Some(identity);
        Ok(admin)
    }

    async fn probe_authenticated_connection(&self) -> Result<(), WorkspaceError> {
        // A PD connection alone cannot prove a TiKV store accepted this client
        // identity. This bounded read is deliberately free of writes/locks.
        let (values, _) = self
            .get_many_consistent_with_time_bounded(
                &[b"__operator/admin-auth-probe/v3".to_vec()],
                KvReadLimits {
                    max_records: 1,
                    max_key_bytes: 256,
                    max_value_bytes: 1024,
                    max_total_bytes: 1280,
                    max_response_bytes: BOUNDED_RECEIPT_MESSAGE_BYTES,
                    max_data_requests: 1,
                },
            )
            .await?;
        if values.len() != 1 {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }

    async fn connect_with_optional_tls(
        pd_endpoints: Vec<String>,
        namespace: &str,
        mount_budget: Arc<V3MountBudget>,
        tls: Option<TiKvTlsConfig>,
    ) -> Result<Self, WorkspaceError> {
        if pd_endpoints.is_empty() {
            return Err(WorkspaceError::Backend(
                "TiKV workspace catalog requires at least one PD endpoint".into(),
            ));
        }
        validate_namespace(namespace)?;
        let config = client_config_with_tls(&mount_budget, 4 << 20, tls.as_ref())?;
        let client = connect_transaction_client(&pd_endpoints, config).await?;
        let prefix = format!("{namespace}/ws:v1/").into_bytes();
        Ok(Self {
            clients: Arc::new(tokio::sync::Mutex::new(ClientSlots {
                main: Some(client),
                receipt: None,
                small: None,
                journal: None,
                xattr: None,
                generic: None,
            })),
            prefix,
            namespace: namespace.to_owned(),
            pd_endpoints: std::sync::Arc::new(pd_endpoints),
            mount_budget,
            tls,
            operator_admin: None,
            operations: Arc::default(),
            established_namespace: Arc::default(),
            #[cfg(test)]
            test_control: std::sync::Arc::default(),
        })
    }

    fn scoped(&self, key: &[u8]) -> Vec<u8> {
        let mut scoped = Vec::with_capacity(self.prefix.len() + key.len());
        scoped.extend_from_slice(&self.prefix);
        scoped.extend_from_slice(key);
        scoped
    }

    pub(crate) async fn load_manager_native_locator(
        &self,
    ) -> Result<Option<Vec<u8>>, WorkspaceError> {
        let _operation = self.operations.enter()?;
        let mut transaction = self
            .main_client()
            .await?
            .begin_with_options(TransactionOptions::new_optimistic().drop_check(CheckLevel::Warn))
            .await
            .map_err(backend)?;
        let value = transaction
            .get(format!("nb2/header/{}", self.namespace))
            .await;
        transaction.rollback().await.map_err(backend)?;
        value.map_err(backend)
    }

    async fn main_client(&self) -> Result<TransactionClient, WorkspaceError> {
        self.clients
            .lock()
            .await
            .main
            .as_ref()
            .cloned()
            .ok_or_else(|| backend("TiKV workspace backend is shut down"))
    }

    pub async fn shutdown(&self) -> Result<(), WorkspaceError> {
        self.operations.close_and_drain().await;
        let mut clients = self.clients.lock().await;
        // Keep slots while awaiting joins so a cancelled shutdown can resume.
        for client in [
            &clients.main,
            &clients.receipt,
            &clients.small,
            &clients.journal,
            &clients.xattr,
            &clients.generic,
        ]
        .into_iter()
        .flatten()
        {
            client.shutdown().await.map_err(backend)?;
        }
        clients.main.take();
        clients.receipt.take();
        clients.small.take();
        clients.journal.take();
        clients.xattr.take();
        clients.generic.take();
        Ok(())
    }

    async fn retry_delay(attempt: usize) {
        let bound = ((attempt + 1) * (attempt + 1)).max(1) as u64;
        let jitter = rng().next_u64() % bound;
        tokio::time::sleep(Duration::from_millis(20 + jitter)).await;
    }

    async fn bounded_authentication_transaction(
        &self,
        limits: KvReadLimits,
    ) -> Result<tikv_client::Transaction, WorkspaceError> {
        limits.validate()?;
        if limits.max_value_bytes > 48 << 10
            || limits.max_response_bytes < BOUNDED_JOURNAL_MESSAGE_BYTES
            || limits.max_data_requests > BOUNDED_READ_MAX_POINT_KEYS
        {
            return Err(WorkspaceError::InvalidReadPlan(
                "TiKV bounded authentication exceeds its 48 KiB schema/64 KiB envelope".into(),
            ));
        }
        let mut clients = self.clients.lock().await;
        let slot = &mut clients.journal;
        if slot.is_none() {
            let config = client_config_with_tls(
                &self.mount_budget,
                BOUNDED_JOURNAL_MESSAGE_BYTES,
                self.tls.as_ref(),
            )?;
            *slot = Some(connect_transaction_client(self.pd_endpoints.as_ref(), config).await?);
        }
        let client = slot.as_ref().unwrap().clone();
        drop(clients);
        let options = TransactionOptions::new_pessimistic()
            .no_resolve_locks()
            .no_resolve_regions()
            .wait_for_secondary_commit()
            .drop_check(CheckLevel::Warn);
        client.begin_with_options(options).await.map_err(backend)
    }

    async fn bounded_read_transaction(
        &self,
        limits: KvReadLimits,
    ) -> Result<tikv_client::Transaction, WorkspaceError> {
        let message_bytes = bounded_read_message_bytes(limits)?;
        let mut clients = self.clients.lock().await;
        let slot = if message_bytes == BOUNDED_RECEIPT_MESSAGE_BYTES {
            &mut clients.receipt
        } else if message_bytes == BOUNDED_READ_MESSAGE_BYTES {
            &mut clients.small
        } else if message_bytes == BOUNDED_JOURNAL_MESSAGE_BYTES {
            &mut clients.journal
        } else if message_bytes == BOUNDED_XATTR_MESSAGE_BYTES {
            &mut clients.xattr
        } else {
            &mut clients.generic
        };
        if slot.is_none() {
            let config =
                client_config_with_tls(&self.mount_budget, message_bytes, self.tls.as_ref())?;
            *slot = Some(connect_transaction_client(self.pd_endpoints.as_ref(), config).await?);
        }
        let client = slot.as_ref().unwrap().clone();
        drop(clients);
        let options = TransactionOptions::new_optimistic()
            .read_only()
            .drop_check(CheckLevel::Warn);
        client.begin_with_options(options).await.map_err(backend)
    }

    async fn scan_prefix_with_byte_plan(
        &self,
        prefix: &[u8],
        after_key_exclusive: Option<&[u8]>,
        limits: KvReadLimits,
        allow_short_page: bool,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        limits.validate()?;
        let _operation = self.operations.enter()?;
        if prefix.len() > limits.max_key_bytes
            || limits.max_data_requests > BOUNDED_READ_MAX_SCAN_PAGES
        {
            return Err(WorkspaceError::InvalidReadPlan(
                "TiKV bounded scan exceeds prefix or page limit".into(),
            ));
        }
        let mut transaction = self.bounded_read_transaction(limits).await?;
        let mut start = match after_key_exclusive {
            // TiKV scans an inclusive start. Appending NUL is the immediate
            // successor in the variable-length byte-key ordering.
            Some(after) => {
                let mut successor = self.scoped(after);
                successor.push(0);
                Key::from(successor)
            }
            None => Key::from(self.scoped(prefix)),
        };
        let upper = prefix_range_end(&self.scoped(prefix))
            .map(Key::from)
            .map(Bound::Excluded)
            .unwrap_or(Bound::Unbounded);
        let mut entries: Vec<KvEntry> = Vec::new();
        let mut total = 0usize;
        let mut lock_retries = 0usize;
        let mut data_requests = 0usize;
        while data_requests < limits.max_data_requests {
            let range = BoundRange::new(Bound::Included(start.clone()), upper.clone());
            // Exactly one region and one row per data RPC. Empty regions count
            // against max_data_requests too; no unbounded seek through sparse PD
            // maps. The SDK's ordinary scan(limit) is intentionally not used.
            data_requests += 1;
            let (page, continuation) = match transaction.scan_single_region_page(range, 1).await {
                Ok(result) => result,
                Err(error)
                    if error.is_bounded_read_retryable_key_lock()
                        || error.is_bounded_read_retryable_region() =>
                {
                    // Scan errors can be embedded in a returned KvPair. A
                    // key lock and an EpochNotMatch region refresh are both
                    // safe to retry while this transaction remains read-only;
                    // the explicit budget prevents an unbounded wait.
                    if lock_retries >= BOUNDED_SCAN_LOCK_RETRIES
                        || data_requests >= limits.max_data_requests
                    {
                        // A failed row cannot be treated as a short page: doing
                        // so could advance the caller cursor past the locked
                        // key and silently omit it. Surface Busy so the
                        // enclosing topology operation rebuilds its read
                        // packet after the finite refresh budget is spent.
                        return Err(WorkspaceError::Busy);
                    }
                    lock_retries += 1;
                    tokio::time::sleep(BOUNDED_SCAN_LOCK_BACKOFF).await;
                    continue;
                }
                Err(error) => return Err(backend(error)),
            };
            for pair in page {
                let (key, value): (Key, Vec<u8>) = pair.into();
                let key: Vec<u8> = key.into();
                let logical = key
                    .strip_prefix(self.prefix.as_slice())
                    .ok_or_else(|| backend("bounded scan returned out-of-namespace key"))?;
                total = total
                    .checked_add(logical.len())
                    .and_then(|sum| sum.checked_add(value.len()))
                    .ok_or_else(|| backend("bounded scan byte count overflow"))?;
                let previous = entries
                    .last()
                    .map(|entry| entry.key.as_slice())
                    .or(after_key_exclusive);
                if !logical.starts_with(prefix)
                    || previous.is_some_and(|last| logical <= last)
                    || logical.len() > limits.max_key_bytes
                    || value.len() > limits.max_value_bytes
                    || total > limits.max_total_bytes
                {
                    return Err(backend("bounded scan key order or decoded bytes exceeded"));
                }
                entries.push(KvEntry {
                    key: logical.to_vec(),
                    value,
                });
                if entries.len() == limits.max_records {
                    return Ok(entries);
                }
            }
            match continuation {
                None => return Ok(entries),
                Some(next) if next > start => start = next,
                Some(_) => return Err(backend("bounded scan continuation did not advance")),
            }
        }
        if allow_short_page && !entries.is_empty() {
            // Cursor advances to the last returned row. The caller continues
            // even after a short page, preserving all unvisited region ranges.
            return Ok(entries);
        }
        Err(WorkspaceError::InvalidReadPlan(
            "TiKV bounded scan exhausted page/RPC count limit".into(),
        ))
    }
}

impl TiKvWorkspaceBackend {
    async fn scan_prefix_with_limit(
        &self,
        prefix: &[u8],
        max_records: Option<usize>,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        let _operation = self.operations.enter()?;
        if max_records == Some(0) {
            return Err(WorkspaceError::InvalidReadPlan(
                "bounded prefix scan requires a positive row limit".into(),
            ));
        }
        let scoped_prefix = self.scoped(prefix);
        let upper = prefix_range_end(&scoped_prefix)
            .map(Key::from)
            .map(Bound::Excluded)
            .unwrap_or(Bound::Unbounded);

        // A full layer scan is used during seal/hash and can observe a
        // read-only key lock in one returned KvPair.  Restart the complete
        // read transaction a finite number of times; no mutation has been
        // submitted, so this is safe and lets the lock's secondary commit
        // finish.  Exhaustion becomes Busy for the caller's outer CAS retry.
        for lock_attempt in 0..=BOUNDED_SCAN_LOCK_RETRIES {
            let options = TransactionOptions::new_optimistic().drop_check(CheckLevel::Warn);
            let mut transaction = self
                .main_client()
                .await?
                .begin_with_options(options)
                .await
                .map_err(backend)?;
            let mut lower = Bound::Included(Key::from(scoped_prefix.clone()));
            let mut entries = Vec::new();
            let mut retry = false;

            loop {
                if max_records.is_some_and(|limit| entries.len() >= limit) {
                    break;
                }
                let batch_limit = max_records
                    .map(|limit| {
                        let remaining = u32::try_from(limit - entries.len()).unwrap_or(u32::MAX);
                        SCAN_BATCH_LIMIT.min(remaining)
                    })
                    .unwrap_or(SCAN_BATCH_LIMIT);
                let range = BoundRange::new(lower.clone(), upper.clone());
                let batch: Vec<KvPair> = match transaction.scan(range, batch_limit).await {
                    Ok(batch) => batch.collect(),
                    Err(error)
                        if error.is_bounded_read_retryable_key_lock()
                            || error.is_bounded_read_retryable_region() =>
                    {
                        if lock_attempt < BOUNDED_SCAN_LOCK_RETRIES {
                            retry = true;
                            break;
                        }
                        return Err(WorkspaceError::Busy);
                    }
                    Err(error) => return Err(backend(error)),
                };
                let batch_len = batch.len();
                if batch_len == 0 {
                    break;
                }
                for pair in batch {
                    let last_key: Vec<u8> = pair.key().clone().into();
                    lower = Bound::Excluded(Key::from(last_key.clone()));
                    let logical =
                        last_key
                            .strip_prefix(self.prefix.as_slice())
                            .ok_or_else(|| {
                                WorkspaceError::Backend(
                                    "TiKV workspace scan returned an out-of-namespace key".into(),
                                )
                            })?;
                    entries.push(KvEntry {
                        key: logical.to_vec(),
                        value: pair.value().to_vec(),
                    });
                }
                if batch_len < batch_limit as usize {
                    break;
                }
            }

            if retry {
                if let Err(error) = transaction.rollback().await {
                    log::debug!("TiKV workspace scan retry rollback failed: {error}");
                }
                tokio::time::sleep(BOUNDED_SCAN_LOCK_BACKOFF).await;
                continue;
            }
            if let Err(error) = transaction.rollback().await {
                log::debug!("TiKV workspace scan rollback failed: {error}");
            }
            return Ok(entries);
        }
        Err(WorkspaceError::Busy)
    }
}

#[async_trait]
impl WorkspaceKvBackend for TiKvWorkspaceBackend {
    async fn authenticate_gc_admin(&self) -> Result<(), WorkspaceError> {
        let identity =
            self.operator_admin
                .as_ref()
                .ok_or(WorkspaceError::UnsupportedCapability(
                    "independent operator TiKV credentials",
                ))?;
        let tls = self.tls.as_ref().ok_or(WorkspaceError::Fenced)?;
        if tls.leaf_fingerprint()? != identity.admin_leaf
            || identity.admin_leaf == identity.runtime_leaf
        {
            return Err(WorkspaceError::Fenced);
        }
        self.probe_authenticated_connection().await
    }
    async fn shutdown_metadata_backend(&self) -> Result<(), WorkspaceError> {
        self.shutdown().await
    }

    fn supports_consistent_reads(&self) -> bool {
        true
    }
    fn name(&self) -> &'static str {
        "workspace-tikv"
    }
    fn native_gc_metadata_page_quota(&self) -> Option<usize> {
        // TiKV scan pages are bounded before values are materialized, so use
        // the same durable cursor finalization as the operator adapter.
        Some(32)
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        let _operation = self.operations.enter()?;
        let options = TransactionOptions::new_optimistic().drop_check(CheckLevel::Warn);
        let mut transaction = self
            .main_client()
            .await?
            .begin_with_options(options)
            .await
            .map_err(backend)?;
        let value = transaction.get(self.scoped(key)).await.map_err(backend)?;
        if let Err(error) = transaction.rollback().await {
            log::debug!("TiKV workspace get rollback failed: {error}");
        }
        Ok(value)
    }

    async fn get_many(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        Ok(self.get_many_with_time(keys).await?.0)
    }

    async fn get_many_consistent(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        // get_many_with_time uses one TiKV read transaction/start timestamp.
        self.get_many(keys).await
    }

    async fn get_many_consistent_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        self.get_many_with_time(keys).await
    }

    async fn get_many_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        limits.validate_keys(keys)?;
        let _operation = self.operations.enter()?;
        if keys.len() > BOUNDED_READ_MAX_POINT_KEYS || keys.len() > limits.max_data_requests {
            return Err(WorkspaceError::InvalidReadPlan(
                "TiKV bounded point read exceeds RPC count limit".into(),
            ));
        }
        let mut transaction = self.bounded_read_transaction(limits).await?;
        let now = pd_physical_ms_to_ns(transaction.start_timestamp().physical)?;
        // Create one data-batch context only after this transaction's original
        // read-only timestamp is fixed. Startup/initial TSO are outside it.
        let values = bounded_read::read_batch(
            &mut transaction,
            &self.prefix,
            keys,
            limits,
            BOUNDED_CLIENT_RPC_TIMEOUT,
        )
        .await?;
        // A read-only transaction owns no locks or mutations; Drop is complete.
        Ok((values, now))
    }

    async fn get_publication_packet_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        super::kv_backend::validate_publication_packet_limits(keys, limits)?;
        let _operation = self.operations.enter()?;
        // Exactly one start timestamp and one owner survive all <=32-key
        // windows. The private driver shares bytes, attempts and deadline.
        let mut transaction = self.bounded_read_transaction(limits).await?;
        let now = pd_physical_ms_to_ns(transaction.start_timestamp().physical)?;
        let values = bounded_read::read_publication_packet(
            &mut transaction,
            &self.prefix,
            keys,
            limits,
            BOUNDED_CLIENT_RPC_TIMEOUT,
        )
        .await?;
        Ok((values, now))
    }

    async fn get_many_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        let _operation = self.operations.enter()?;
        if keys.is_empty() {
            let timestamp = self
                .main_client()
                .await?
                .current_timestamp()
                .await
                .map_err(backend)?;
            return Ok((Vec::new(), pd_physical_ms_to_ns(timestamp.physical)?));
        }
        let options = TransactionOptions::new_optimistic().drop_check(CheckLevel::Warn);
        let mut transaction = self
            .main_client()
            .await?
            .begin_with_options(options)
            .await
            .map_err(backend)?;
        let now = pd_physical_ms_to_ns(transaction.start_timestamp().physical)?;
        let scoped = keys.iter().map(|key| self.scoped(key)).collect::<Vec<_>>();
        let found = transaction
            .batch_get(scoped.clone())
            .await
            .map_err(backend)?
            .map(|pair| {
                let key: Vec<u8> = pair.key().clone().into();
                (key, pair.value().to_vec())
            })
            .collect::<BTreeMap<_, _>>();
        if let Err(error) = transaction.rollback().await {
            log::debug!("TiKV workspace batch get rollback failed: {error}");
        }
        Ok((
            scoped.iter().map(|key| found.get(key).cloned()).collect(),
            now,
        ))
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.scan_prefix_with_limit(prefix, None).await
    }

    async fn scan_prefix_with_byte_limits(
        &self,
        prefix: &[u8],
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.scan_prefix_with_byte_plan(prefix, None, limits, false)
            .await
    }

    async fn scan_prefix_page_with_byte_limits(
        &self,
        prefix: &[u8],
        after_key_exclusive: Option<&[u8]>,
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        limits.validate_scan_page(prefix, after_key_exclusive)?;
        self.scan_prefix_with_byte_plan(prefix, after_key_exclusive, limits, true)
            .await
    }

    async fn scan_prefix_bounded(
        &self,
        prefix: &[u8],
        max_records: usize,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.scan_prefix_with_limit(prefix, Some(max_records)).await
    }

    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        self.cas(checks, writes, None, None, false).await
    }

    async fn compare_and_swap_in_empty_namespace(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        before_ns: i64,
    ) -> Result<bool, WorkspaceError> {
        validate_cas_time_window(None, Some(before_ns))?;
        self.cas(checks, writes, None, Some(before_ns), true).await
    }

    async fn authenticate_checks_before_bounded(
        &self,
        checks: &[KvCheck],
        expires_at_ns: i64,
        limits: KvReadLimits,
    ) -> Result<bool, WorkspaceError> {
        super::kv_backend::validate_bounded_authentication_checks(checks, limits)?;
        validate_cas_time_window(None, Some(expires_at_ns))?;
        let _operation = self.operations.enter()?;
        let ordered_checks = pessimistic_checks_in_key_order(checks);
        let mut transaction = self.bounded_authentication_transaction(limits).await?;
        let mut context = bounded_read::BoundedPointReadContext::new(
            limits.max_data_requests,
            checks.len(),
            BOUNDED_CLIENT_RPC_TIMEOUT,
        )?;
        let mut total = 0usize;
        // At most two complete authentication attempts. Only a request-certified
        // Lock WriteConflict may reach the second, after successful rollback.
        // Neither deadline, request slots, bytes nor original checks are reset.
        'authentication: for authentication_attempt in 0..2 {
            #[cfg(test)]
            self.test_control.begin_attempt();
            for check in &ordered_checks {
                let mut write_conflict_bytes = None;
                let read = async {
                    context.admit_attempt()?;
                    let current = match transaction
                        .get_for_update_uncached_single_region_until(
                            self.scoped(&check.key),
                            context.deadline(),
                        )
                        .await
                    {
                        Ok(current) => current,
                        Err(error) => {
                            write_conflict_bytes =
                                error.bounded_authentication_write_conflict_response_bytes();
                            return Err(backend(error));
                        }
                    };
                    #[cfg(test)]
                    self.test_control.record_lock(
                        transaction.start_timestamp(),
                        &self.scoped(&check.key),
                        true,
                    );
                    let bytes = current.as_ref().map_or(0, Vec::len);
                    total = total
                        .checked_add(check.key.len())
                        .and_then(|sum| sum.checked_add(bytes))
                        .ok_or_else(|| backend("bounded authentication byte count overflow"))?;
                    if bytes > limits.max_value_bytes || total > limits.max_total_bytes {
                        return Err(backend(
                            "bounded authentication decoded value bytes exceeded",
                        ));
                    }
                    if tokio::time::Instant::now() >= context.deadline() {
                        return Err(backend("bounded authentication locking deadline exhausted"));
                    }
                    context.record_success()?;
                    Ok::<_, WorkspaceError>(current)
                }
                .await;
                let current = match read {
                    Ok(current) => current,
                    Err(error) => {
                        // Also clean possible locks registered before dispatch.
                        // Failed cleanup, unknown transport and mixed/region
                        // errors never authorize another authentication attempt.
                        if let Err(cleanup) = transaction.rollback().await {
                            return Err(backend(format!(
                                "{error}; bounded authentication rollback failed: {cleanup}"
                            )));
                        }
                        if let Some(response_bytes) = write_conflict_bytes {
                            total = total
                                .checked_add(check.key.len())
                                .and_then(|sum| sum.checked_add(response_bytes))
                                .ok_or_else(|| {
                                    backend("bounded authentication byte count overflow")
                                })?;
                            if total > limits.max_total_bytes {
                                return Ok(false);
                            }
                            if authentication_attempt == 0
                                && authentication_rebuild_bytes_fit(
                                    total,
                                    checks,
                                    limits.max_total_bytes,
                                )
                                && context.reset_authentication_originals(checks.len())
                            {
                                transaction =
                                    self.bounded_authentication_transaction(limits).await?;
                                continue 'authentication;
                            }
                            return Ok(false);
                        }
                        return Err(error);
                    }
                };
                if current.as_deref() != check.expected.as_deref() {
                    transaction.rollback().await.map_err(backend)?;
                    return Ok(false);
                }
            }
            #[cfg(test)]
            self.test_control
                .before_commit(transaction.start_timestamp())
                .await;
            let now = match self.server_time_ns().await {
                Ok(now) => now,
                Err(error) => {
                    transaction.rollback().await.map_err(backend)?;
                    return Err(error);
                }
            };
            if now >= expires_at_ns {
                transaction.rollback().await.map_err(backend)?;
                return Err(WorkspaceError::Fenced);
            }
            if tokio::time::Instant::now() >= context.deadline() {
                transaction.rollback().await.map_err(backend)?;
                return Err(backend("bounded authentication locking deadline exhausted"));
            }
            // Commit the same lock mutations as the existing empty-write CAS.
            // Pessimistic prewrite verifies that no early lock expired after a
            // failed heartbeat; rollback alone cannot supply that proof.
            let result = transaction.commit().await;
            #[cfg(test)]
            self.test_control.record_commit_result(&result);
            return match result {
                Ok(_) => Ok(true),
                // No loop continuation after prewrite/commit submission.
                Err(error) => Err(backend(format!(
                    "{error}; commit cause={}",
                    commit_failure_kind(&error)
                ))),
            };
        }
        Ok(false)
    }

    async fn compare_and_swap_before(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        expires_at_ns: i64,
    ) -> Result<bool, WorkspaceError> {
        self.cas(checks, writes, None, Some(expires_at_ns), false)
            .await
    }

    async fn compare_and_swap_in_time_window(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        not_before_ns: Option<i64>,
        before_ns: Option<i64>,
    ) -> Result<bool, WorkspaceError> {
        validate_cas_time_window(not_before_ns, before_ns)?;
        self.cas(checks, writes, not_before_ns, before_ns, false)
            .await
    }

    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        let _operation = self.operations.enter()?;
        let timestamp = self
            .main_client()
            .await?
            .current_timestamp()
            .await
            .map_err(backend)?;
        pd_physical_ms_to_ns(timestamp.physical)
    }
}

impl TiKvWorkspaceBackend {
    async fn cas(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        not_before_ns: Option<i64>,
        before_ns: Option<i64>,
        empty_namespace: bool,
    ) -> Result<bool, WorkspaceError> {
        let _operation = self.operations.enter()?;
        if writes.iter().any(|write| match write {
            KvWrite::Put { key, .. } | KvWrite::Delete { key } => {
                key.as_slice() == BOOTSTRAP_BARRIER
            }
        }) {
            return Err(WorkspaceError::Fenced);
        }
        // A range read alone has no phantom protection under snapshot
        // isolation. Every pre-initialization CAS owns this same barrier.
        // CONTROL is immutable as a namespace-lifetime marker: replacement or
        // manual raw-key deletion is not supported catalog initialization.
        let preinitialization = if empty_namespace {
            true
        } else if self
            .established_namespace
            .load(std::sync::atomic::Ordering::Acquire)
        {
            false
        } else if self.get(b"control").await?.is_some() {
            self.established_namespace
                .store(true, std::sync::atomic::Ordering::Release);
            false
        } else {
            true
        };
        let ordered_checks = pessimistic_checks_in_key_order(checks);
        let lock_keys = pessimistic_lock_keys(checks, writes);
        let native_locator =
            empty_namespace.then(|| format!("nb2/header/{}", self.namespace).into_bytes());
        'attempt: for attempt in 0..TXN_MAX_RETRIES {
            // A following bounded read cannot resolve an ordinary 2PC
            // secondary lock. Success therefore includes this transaction's
            // owned secondary completion; unknown errors retain no-replay.
            let options = TransactionOptions::new_pessimistic()
                .wait_for_secondary_commit()
                .drop_check(CheckLevel::Warn);
            let mut transaction = self
                .main_client()
                .await?
                .begin_with_options(options)
                .await
                .map_err(backend)?;

            if preinitialization {
                #[cfg(test)]
                self.test_control
                    .before_bootstrap_lock(transaction.start_timestamp());
                let barrier = self.scoped(BOOTSTRAP_BARRIER);
                match transaction.get_for_update(barrier.clone()).await {
                    Ok(None) => {}
                    Ok(Some(_)) => {
                        transaction.rollback().await.map_err(backend)?;
                        return Err(WorkspaceError::Fenced);
                    }
                    Err(error) => {
                        transaction.rollback().await.map_err(backend)?;
                        if is_retryable(&error.to_string()) && attempt + 1 < TXN_MAX_RETRIES {
                            Self::retry_delay(attempt).await;
                            continue 'attempt;
                        }
                        return Err(backend(error));
                    }
                }
                if empty_namespace {
                    // The transaction start timestamp may precede a wait for
                    // the barrier. Read a fresh snapshot *after* owning it so
                    // a previous writer's committed rows cannot be missed.
                    // Exclude our own absent-key lock from the read ranges.
                    let client = self.main_client().await?;
                    let stamp = client.current_timestamp().await.map_err(backend)?;
                    let mut snapshot = client.snapshot(stamp, TransactionOptions::new_optimistic());
                    let end = prefix_range_end(&self.prefix)
                        .map(Key::from)
                        .map(Bound::Excluded)
                        .unwrap_or(Bound::Unbounded);
                    let ranges = [
                        BoundRange::new(
                            Bound::Included(Key::from(self.prefix.clone())),
                            Bound::Excluded(Key::from(barrier.clone())),
                        ),
                        BoundRange::new(Bound::Excluded(Key::from(barrier)), end),
                    ];
                    let owner = self.scoped(super::manager_guard::MANAGER_CONTROLLER_KEY);
                    let owner_checked = checks.iter().any(|check| {
                        check.key.as_slice() == super::manager_guard::MANAGER_CONTROLLER_KEY
                            && check.expected.is_some()
                    });
                    for range in ranges {
                        let keys = match snapshot.scan_keys(range, 2).await {
                            Ok(keys) => keys.collect::<Vec<_>>(),
                            Err(error) => {
                                transaction.rollback().await.map_err(backend)?;
                                return Err(backend(error));
                            }
                        };
                        if keys.into_iter().any(|key| {
                            let key: Vec<u8> = key.into();
                            !owner_checked || key != owner
                        }) {
                            transaction.rollback().await.map_err(backend)?;
                            return Ok(false);
                        }
                    }
                }
            }

            #[cfg(test)]
            self.test_control.begin_attempt();
            // The native locator is outside the carrier namespace, so it must
            // be checked under its own lock through commit. Its key sorts
            // entirely before or after the scoped packet; keep that order to
            // agree with guarded native transactions that also lock authority.
            let locator_first = native_locator
                .as_ref()
                .is_some_and(|key| key.as_slice() < self.prefix.as_slice());
            if locator_first {
                match transaction
                    .get_for_update(native_locator.as_ref().unwrap().clone())
                    .await
                {
                    Ok(None) => {}
                    Ok(Some(_)) => {
                        transaction.rollback().await.map_err(backend)?;
                        return Ok(false);
                    }
                    Err(error) => {
                        transaction.rollback().await.map_err(backend)?;
                        return Err(backend(error));
                    }
                }
            }
            let mut matched = true;
            let mut check_index = 0;
            // First acquisition of every checked or written key follows one
            // byte order across both CAS and bounded authentication. The SDK
            // may later re-lock an owned key while applying an original write.
            for key in &lock_keys {
                let check_begin = check_index;
                while check_index < ordered_checks.len()
                    && ordered_checks[check_index].key.as_slice() == *key
                {
                    check_index += 1;
                }
                let lock_result = if check_begin == check_index {
                    // Pure locking avoids fetching an unchecked existing value.
                    transaction
                        .lock_keys(std::iter::once(self.scoped(key)))
                        .await
                        .map(|()| None)
                } else {
                    transaction.get_for_update(self.scoped(key)).await
                };
                let current = match lock_result {
                    Ok(current) => current,
                    Err(error) => {
                        // TiKV can return the certified bounded-read key-lock
                        // envelope from get_for_update as well as from a
                        // read-only scan. It is pre-submit and safe to retry,
                        // but its diagnostic does not contain the legacy
                        // "PessimisticLock" text used by is_retryable().
                        // Classify it before mapping to a backend error so a
                        // transient lock cannot escape the CAS retry budget.
                        let retryable_key_lock = error.is_bounded_read_retryable_key_lock();
                        let retryable_region = error.is_bounded_read_retryable_region();
                        let retryable = retryable_key_lock
                            || retryable_region
                            || is_retryable(&error.to_string());
                        if let Err(rollback_error) = transaction.rollback().await {
                            return Err(backend(format!(
                                "{error}; CAS check rollback failed: {rollback_error}"
                            )));
                        }
                        if retryable && attempt + 1 < TXN_MAX_RETRIES {
                            Self::retry_delay(attempt).await;
                            continue 'attempt;
                        }
                        if is_definite_precommit_write_conflict(&error) {
                            // No commit was submitted and rollback completed.
                            // The catalog must reread its version and rebuild
                            // sequence/policy input after sustained contention.
                            return Ok(false);
                        }
                        if retryable_key_lock || retryable_region {
                            // These bounded pre-submit read conditions are
                            // transient even when TXN_MAX_RETRIES is
                            // exhausted. Let the topology caller rebuild its
                            // complete read packet and preserve Busy
                            // semantics for the outer CAS retry budget.
                            return Err(WorkspaceError::Busy);
                        }
                        return Err(backend(error));
                    }
                };
                #[cfg(test)]
                self.test_control.record_lock(
                    transaction.start_timestamp(),
                    self.scoped(key).as_slice(),
                    check_begin != check_index,
                );
                // Duplicate checks share one locking read, but every original
                // expected value must match. Conflicting duplicates are false.
                if ordered_checks[check_begin..check_index]
                    .iter()
                    .any(|check| current.as_deref() != check.expected.as_deref())
                {
                    matched = false;
                    break;
                }
            }
            if !matched {
                transaction.rollback().await.map_err(backend)?;
                return Ok(false);
            }
            if let Some(locator) = &native_locator
                && !locator_first
            {
                match transaction.get_for_update(locator.clone()).await {
                    Ok(None) => {}
                    Ok(Some(_)) => {
                        transaction.rollback().await.map_err(backend)?;
                        return Ok(false);
                    }
                    Err(error) => {
                        transaction.rollback().await.map_err(backend)?;
                        return Err(backend(error));
                    }
                }
            }

            for write in writes {
                let result = match write {
                    KvWrite::Put { key, value } => {
                        transaction.put(self.scoped(key), value.clone()).await
                    }
                    KvWrite::Delete { key } => transaction.delete(self.scoped(key)).await,
                };
                #[cfg(test)]
                if result.is_ok() {
                    let key = match write {
                        KvWrite::Put { key, .. } | KvWrite::Delete { key } => key,
                    };
                    self.test_control.record_lock(
                        transaction.start_timestamp(),
                        &self.scoped(key),
                        false,
                    );
                }
                if let Err(error) = result {
                    let retryable = is_retryable(&error.to_string());
                    if let Err(rollback_error) = transaction.rollback().await {
                        return Err(backend(format!(
                            "{error}; CAS rollback failed: {rollback_error}"
                        )));
                    }
                    if retryable && attempt + 1 < TXN_MAX_RETRIES {
                        Self::retry_delay(attempt).await;
                        continue 'attempt;
                    }
                    if is_definite_precommit_write_conflict(&error) {
                        return Ok(false);
                    }
                    return Err(backend(error));
                }
            }

            #[cfg(test)]
            self.test_control
                .before_commit(transaction.start_timestamp())
                .await;
            // All guard/base/binding/allocator keys are locked. Sampling the
            // transaction's start timestamp here would be stale after a lock
            // wait, so request a fresh PD TSO immediately before commit.
            if not_before_ns.is_some() || before_ns.is_some() {
                let now = match self.server_time_ns().await {
                    Ok(now) => now,
                    Err(error) => {
                        transaction.rollback().await.map_err(backend)?;
                        return Err(error);
                    }
                };
                if not_before_ns.is_some_and(|lower| now < lower) {
                    transaction.rollback().await.map_err(backend)?;
                    return Err(WorkspaceError::Busy);
                }
                if before_ns.is_some_and(|upper| now >= upper) {
                    transaction.rollback().await.map_err(backend)?;
                    return Err(WorkspaceError::Fenced);
                }
            }
            let commit_result = transaction.commit().await;
            #[cfg(test)]
            self.test_control.record_commit_result(&commit_result);
            match commit_result {
                Ok(_) => return Ok(true),
                // The server may already have prewritten or committed. A new
                // CAS transaction cannot reconcile that outcome and can hide
                // the original error behind a later value mismatch.
                Err(error) => {
                    return Err(backend(format!(
                        "{error}; commit cause={}",
                        commit_failure_kind(&error)
                    )));
                }
            }
        }
        Err(WorkspaceError::Backend(
            "TiKV workspace catalog exhausted transaction retries".into(),
        ))
    }
}

fn pd_physical_ms_to_ns(physical_ms: i64) -> Result<i64, WorkspaceError> {
    physical_ms
        .checked_mul(1_000_000)
        .ok_or_else(|| WorkspaceError::Backend("PD TSO physical time overflows nanoseconds".into()))
}

fn validate_namespace(namespace: &str) -> Result<(), WorkspaceError> {
    if namespace.is_empty()
        || !namespace
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(WorkspaceError::Backend(
            "TiKV workspace namespace must contain only ASCII letters, digits, '-', '_' or '.'"
                .into(),
        ));
    }
    Ok(())
}

fn prefix_range_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for index in (0..end.len()).rev() {
        if end[index] != 0xff {
            end[index] += 1;
            end.truncate(index + 1);
            return Some(end);
        }
    }
    None
}

fn is_retryable(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("write conflict")
        || message.contains("pessimisticlock")
        || message.contains("lock conflict")
        || message.contains("txnlock")
}

/// Use only before submitting commit and after successful pessimistic rollback.
/// Transport/undetermined errors and error text cannot establish a conflict.
fn is_definite_precommit_write_conflict(error: &tikv_client::Error) -> bool {
    use tikv_client::Error;
    match error {
        Error::PessimisticLockError { inner, .. } => is_definite_precommit_write_conflict(inner),
        Error::MultipleKeyErrors(errors) | Error::ExtractedErrors(errors) => {
            !errors.is_empty() && errors.iter().all(is_definite_precommit_write_conflict)
        }
        Error::KeyError(key) => {
            key.conflict.is_some()
                && key.locked.is_none()
                && key.abort.is_empty()
                && key.already_exist.is_none()
                && key.deadlock.is_none()
                && key.commit_ts_expired.is_none()
                && key.txn_not_found.is_none()
                && key.commit_ts_too_large.is_none()
                && key.assertion_failed.is_none()
                && key.primary_mismatch.is_none()
        }
        _ => false,
    }
}

fn backend(error: impl std::fmt::Display) -> WorkspaceError {
    WorkspaceError::Backend(format!("TiKV workspace catalog: {error}"))
}

/// Retain a useful diagnosis when the SDK's undetermined Display hides its
/// cause. Only fixed categories are emitted; keys and RPC messages stay private.
fn commit_failure_kind(error: &tikv_client::Error) -> &'static str {
    use tikv_client::Error;
    let mut cause = error;
    for _ in 0..8 {
        cause = match cause {
            Error::UndeterminedError(inner) | Error::PessimisticLockError { inner, .. } => inner,
            Error::MultipleKeyErrors(errors) | Error::ExtractedErrors(errors)
                if errors.len() == 1 =>
            {
                &errors[0]
            }
            Error::KeyError(key) => {
                return if key.conflict.is_some() {
                    "key-write-conflict"
                } else if key.commit_ts_expired.is_some() {
                    "key-commit-timestamp-expired"
                } else if key.txn_not_found.is_some() {
                    "key-transaction-not-found"
                } else if key.locked.is_some() {
                    "key-locked"
                } else if !key.abort.is_empty() {
                    "key-abort"
                } else {
                    "key-other"
                };
            }
            Error::GrpcAPI(status) => {
                // gRPC's stable numeric status codes avoid a separate runtime
                // dependency on the SDK's transport crate.
                return match status.code() as i32 {
                    9 => "rpc-failed-precondition",
                    8 => "rpc-resource-exhausted",
                    14 => "rpc-unavailable",
                    4 => "rpc-deadline-exceeded",
                    _ => "rpc-other",
                };
            }
            Error::InternalError { message } => {
                return if message.contains("SDK task owner closed or full") {
                    "sdk-task-owner-unavailable"
                } else if message.contains("owned secondary commit result receiver closed") {
                    "secondary-receiver-closed"
                } else if message.contains("owned secondary commit wait exceeded its deadline") {
                    "secondary-wait-deadline"
                } else {
                    "internal"
                };
            }
            Error::RegionError(_) => return "region",
            Error::JoinError(_) | Error::Channel(_) | Error::Canceled(_) => {
                return "task-or-channel";
            }
            Error::MultipleKeyErrors(_) | Error::ExtractedErrors(_) => return "multiple-causes",
            _ => return "other",
        };
    }
    "nested-cause-limit"
}

#[cfg(test)]
#[path = "tikv_admin_tls_tests.rs"]
mod admin_tls_tests;

#[cfg(test)]
#[path = "tikv_secondary_join_tests.rs"]
mod secondary_join_tests;

#[cfg(test)]
mod tests {
    use super::pd_physical_ms_to_ns;

    #[test]
    fn authentication_rebuild_preserves_consumed_bytes_and_full_snapshot_reservation() {
        let checks = vec![
            super::KvCheck {
                key: b"first".to_vec(),
                expected: Some(vec![1; 7]),
            },
            super::KvCheck {
                key: b"second".to_vec(),
                expected: None,
            },
        ];
        assert!(super::authentication_rebuild_bytes_fit(20, &checks, 38));
        assert!(!super::authentication_rebuild_bytes_fit(20, &checks, 37));
        assert!(!super::authentication_rebuild_bytes_fit(
            usize::MAX,
            &checks,
            usize::MAX
        ));
    }

    #[test]
    fn precommit_conflict_classification_rejects_uncertain_and_mixed_errors() {
        use tikv_client::{Error, ProtoKeyError};
        let conflict = || {
            let key = ProtoKeyError {
                conflict: Some(Default::default()),
                ..Default::default()
            };
            Error::KeyError(Box::new(key))
        };
        let nested = Error::PessimisticLockError {
            inner: Box::new(Error::MultipleKeyErrors(vec![conflict()])),
            success_keys: Vec::new(),
        };
        assert!(super::is_definite_precommit_write_conflict(&nested));
        assert!(!super::is_definite_precommit_write_conflict(
            &Error::UndeterminedError(Box::new(conflict()))
        ));
        assert!(!super::is_definite_precommit_write_conflict(
            &Error::MultipleKeyErrors(vec![
                conflict(),
                Error::StringError("PessimisticLock WriteConflict".into()),
            ])
        ));
        assert!(!super::is_definite_precommit_write_conflict(
            &Error::MultipleKeyErrors(Vec::new())
        ));
        assert!(!super::is_definite_precommit_write_conflict(
            &Error::KeyError(Box::new(ProtoKeyError {
                conflict: Some(Default::default()),
                abort: "transaction aborted".into(),
                ..Default::default()
            }))
        ));
    }

    #[tokio::test]
    async fn backend_shutdown_stops_admission_and_keeps_drain_obligation_on_cancellation() {
        let operations = std::sync::Arc::new(super::Operations::default());
        let active = operations.enter().unwrap();
        let mut shutdown = Box::pin(operations.close_and_drain());
        assert!(futures::poll!(&mut shutdown).is_pending());
        assert!(operations.enter().is_err());
        drop(shutdown);
        assert_eq!(operations.state.lock().unwrap().active, 1);
        drop(active);
        operations.close_and_drain().await;
        assert_eq!(operations.state.lock().unwrap().active, 0);
    }

    #[test]
    fn client_resident_owner_pre_admits_and_rejects_a_closed_mount_ledger() {
        use crate::workspace_overlay::packed_v3::wire005::{V3BudgetPool, V3MountBudget};
        let budget = V3MountBudget::defaults();
        let config =
            super::client_config_with_tls(&budget, super::BOUNDED_READ_MESSAGE_BYTES, None)
                .unwrap();
        assert_eq!(config.max_cached_kv_clients, 1);
        assert_eq!(
            budget.state().used[V3BudgetPool::Metadata as usize],
            super::CLIENT_RESIDENT_METADATA_BYTES
        );
        let retained = config.clone();
        drop(config);
        assert_eq!(
            budget.state().used[V3BudgetPool::Metadata as usize],
            super::CLIENT_RESIDENT_METADATA_BYTES
        );
        drop(retained);
        assert_eq!(budget.state().used, [0; 8]);
        budget.close();
        assert!(
            super::client_config_with_tls(&budget, super::BOUNDED_READ_MESSAGE_BYTES, None)
                .is_err()
        );
        assert_eq!(budget.state().used, [0; 8]);
    }

    #[tokio::test]
    #[ignore = "requires BREWFS_TEST_TIKV_PD_ENDPOINTS"]
    async fn real_tikv_bounded_xattr_reads_preserve_64k_value_and_shutdown_owners() {
        use super::*;
        use crate::workspace_overlay::ids::LayerId;
        use crate::workspace_overlay::model::{ValueOp, XattrDelta};
        use futures::FutureExt;

        let endpoints = std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
            .unwrap()
            .split(',')
            .map(str::to_owned)
            .collect();
        let budget = V3MountBudget::defaults();
        let backend = TiKvWorkspaceBackend::connect_with_budget(
            endpoints,
            &format!("kv-xattr-{}", uuid::Uuid::new_v4().simple()),
            budget.clone(),
        )
        .await
        .unwrap();
        let key = b"xattr/user.large".to_vec();
        let delta = XattrDelta {
            layer_id: LayerId::new(),
            ino: 2,
            name: b"user.large".to_vec(),
            op: ValueOp::Put,
            value: Some(vec![0x53; 64 << 10]),
            sequence: 1,
        };
        let mut stored = b"BWSKV001".to_vec();
        stored.extend(bincode::serialize(&delta).unwrap());
        assert!(stored.len() > 64 << 10);
        let limits = KvReadLimits {
            max_records: 1,
            max_key_bytes: 1024,
            max_value_bytes: 96 << 10,
            max_total_bytes: (96 << 10) + 1024,
            max_response_bytes: 128 << 10,
            max_data_requests: 16,
        };
        let result = std::panic::AssertUnwindSafe(async {
            assert!(
                backend
                    .compare_and_swap(
                        &[],
                        &[KvWrite::Put {
                            key: key.clone(),
                            value: stored.clone(),
                        }],
                    )
                    .await?
            );
            let keys = [key.clone()];
            let (values, now) = backend
                .get_many_consistent_with_time_bounded(&keys, limits)
                .await?;
            assert!(now > 0);
            assert_eq!(values, vec![Some(stored.clone())]);
            let decoded: XattrDelta =
                bincode::deserialize(&values[0].as_ref().unwrap()[8..]).unwrap();
            assert_eq!(decoded, delta);
            let page = backend
                .scan_prefix_page_with_byte_limits(b"xattr/", None, limits)
                .await?;
            assert_eq!(
                page,
                vec![KvEntry {
                    key: key.clone(),
                    value: stored.clone()
                }]
            );
            assert!(
                backend
                    .scan_prefix_page_with_byte_limits(b"xattr/", Some(&key), limits)
                    .await?
                    .is_empty()
            );
            let too_small = KvReadLimits {
                max_total_bytes: key.len() + stored.len() - 1,
                ..limits
            };
            assert!(
                backend
                    .get_many_consistent_with_time_bounded(&keys, too_small)
                    .await
                    .is_err()
            );
            assert!(
                backend
                    .scan_prefix_page_with_byte_limits(b"xattr/", None, too_small)
                    .await
                    .is_err()
            );
            let too_small = KvReadLimits {
                max_value_bytes: stored.len() - 1,
                ..limits
            };
            assert!(
                backend
                    .get_many_consistent_with_time_bounded(&keys, too_small)
                    .await
                    .is_err()
            );
            assert!(
                backend
                    .scan_prefix_page_with_byte_limits(b"xattr/", None, too_small)
                    .await
                    .is_err()
            );
            let too_small = KvReadLimits {
                max_response_bytes: (128 << 10) - 1,
                ..limits
            };
            assert!(matches!(
                backend
                    .get_many_consistent_with_time_bounded(&keys, too_small)
                    .await,
                Err(WorkspaceError::InvalidReadPlan(_))
            ));
            // Visit all fixed clients, sharing the same canonical ledger.
            for (max_value_bytes, max_response_bytes) in
                [(12 << 10, 16 << 10), (48 << 10, 64 << 10)]
            {
                let missing = [b"missing".to_vec()];
                backend
                    .get_many_consistent_with_time_bounded(
                        &missing,
                        KvReadLimits {
                            max_value_bytes,
                            max_response_bytes,
                            ..limits
                        },
                    )
                    .await?;
            }
            assert_eq!(
                budget.state().used[V3BudgetPool::Metadata as usize],
                4 * CLIENT_RESIDENT_METADATA_BYTES
            );
            backend
                .compare_and_swap(
                    &[],
                    &[KvWrite::Put {
                        key: key.clone(),
                        value: vec![1; 256 << 10],
                    }],
                )
                .await?;
            for error in [
                backend
                    .get_many_consistent_with_time_bounded(&keys, limits)
                    .await
                    .unwrap_err(),
                backend
                    .scan_prefix_page_with_byte_limits(b"xattr/", None, limits)
                    .await
                    .unwrap_err(),
            ] {
                assert!(error.to_string().contains("131072"), "{error}");
            }
            Ok::<(), WorkspaceError>(())
        })
        .catch_unwind()
        .await;
        backend
            .compare_and_swap(&[], &[KvWrite::Delete { key: key.clone() }])
            .await
            .unwrap();
        backend.shutdown().await.unwrap();
        assert_eq!(budget.state().used, [0; 8]);
        assert!(
            backend
                .get_many_consistent_with_time_bounded(&[key], limits)
                .await
                .is_err()
        );
        result.unwrap().unwrap();
    }

    #[tokio::test]
    #[ignore = "requires BREWFS_TEST_TIKV_PD_ENDPOINTS"]
    async fn real_tikv_bounded_values_use_hard_cap_for_corrupt_oversized_point_and_scan() {
        use super::*;
        use futures::FutureExt;
        let endpoints = std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
            .unwrap()
            .split(',')
            .map(str::to_owned)
            .collect();
        let namespace = format!("kv-bytes-{}", uuid::Uuid::new_v4().simple());
        let budget = V3MountBudget::defaults();
        let backend =
            TiKvWorkspaceBackend::connect_with_budget(endpoints, &namespace, budget.clone())
                .await
                .unwrap();
        let keys = [b"pin/a".to_vec(), b"pin/b".to_vec()];
        let limits = KvReadLimits {
            max_records: 2,
            max_key_bytes: 32,
            max_value_bytes: 4,
            max_total_bytes: 18,
            max_response_bytes: BOUNDED_READ_MESSAGE_BYTES,
            max_data_requests: 8,
        };
        let journal_limits = KvReadLimits {
            max_value_bytes: 48 << 10,
            max_response_bytes: BOUNDED_JOURNAL_MESSAGE_BYTES,
            ..limits
        };
        let result = std::panic::AssertUnwindSafe(async {
            backend
                .compare_and_swap(
                    &[],
                    &[
                        KvWrite::Put {
                            key: keys[0].clone(),
                            value: vec![1; 4],
                        },
                        KvWrite::Put {
                            key: keys[1].clone(),
                            value: vec![2; 4],
                        },
                    ],
                )
                .await?;
            let (values, now) = backend
                .get_many_consistent_with_time_bounded(&keys, limits)
                .await?;
            assert_eq!(values, vec![Some(vec![1; 4]), Some(vec![2; 4])]);
            assert!(now > 0);
            assert_eq!(
                backend
                    .get_many_consistent_with_time_bounded(&keys, journal_limits)
                    .await?
                    .0,
                vec![Some(vec![1; 4]), Some(vec![2; 4])]
            );
            assert_eq!(
                budget.state().used[V3BudgetPool::Metadata as usize],
                3 * CLIENT_RESIDENT_METADATA_BYTES
            );
            assert_eq!(
                backend
                    .scan_prefix_with_byte_limits(b"pin/", limits)
                    .await?
                    .len(),
                2
            );
            let aggregate_too_small = KvReadLimits {
                max_total_bytes: 17,
                ..limits
            };
            assert!(
                backend
                    .get_many_consistent_with_time_bounded(&keys, aggregate_too_small)
                    .await
                    .is_err()
            );
            assert!(
                backend
                    .scan_prefix_with_byte_limits(b"pin/", aggregate_too_small)
                    .await
                    .is_err()
            );
            backend
                .compare_and_swap(
                    &[],
                    &[KvWrite::Put {
                        key: keys[0].clone(),
                        value: vec![3; 2 << 20],
                    }],
                )
                .await?;
            let point = backend
                .get_many_consistent_with_time_bounded(&keys, limits)
                .await
                .unwrap_err();
            let scan = backend
                .scan_prefix_with_byte_limits(b"pin/", limits)
                .await
                .unwrap_err();
            // This must be the actual tonic envelope rejection, before the
            // smaller semantic 4-byte value limit can observe the payload.
            assert!(point.to_string().contains("16384"), "{point}");
            assert!(scan.to_string().contains("16384"), "{scan}");
            let journal = backend
                .get_many_consistent_with_time_bounded(&keys, journal_limits)
                .await
                .unwrap_err();
            assert!(journal.to_string().contains("65536"), "{journal}");
            Ok::<(), WorkspaceError>(())
        })
        .catch_unwind()
        .await;
        backend
            .compare_and_swap(
                &[],
                &[
                    KvWrite::Delete {
                        key: keys[0].clone(),
                    },
                    KvWrite::Delete {
                        key: keys[1].clone(),
                    },
                ],
            )
            .await
            .unwrap();
        backend.shutdown().await.unwrap();
        assert_eq!(budget.state().used, [0; 8]);
        assert!(
            backend
                .get_many_consistent_with_time_bounded(&keys, limits)
                .await
                .is_err()
        );
        result.unwrap().unwrap();
    }

    #[tokio::test]
    #[ignore = "requires BREWFS_TEST_TIKV_PD_ENDPOINTS"]
    async fn real_tikv_bounded_keyset_pages_complete_large_prefix() {
        let endpoints = std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
            .unwrap()
            .split(',')
            .map(str::to_owned)
            .collect();
        let backend = super::TiKvWorkspaceBackend::connect(
            endpoints,
            &format!("kv-pages-{}", uuid::Uuid::new_v4().simple()),
        )
        .await
        .unwrap();
        super::super::kv_backend::scan_page_contract::assert_real_pages(&backend).await;
        backend.shutdown().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires BREWFS_TEST_TIKV_PD_ENDPOINTS"]
    async fn real_tikv_clock_window_fences_reap_and_advances_generation_atomically() {
        let endpoints = std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
            .unwrap()
            .split(',')
            .map(str::to_owned)
            .collect();
        let backend = super::TiKvWorkspaceBackend::connect(
            endpoints,
            &format!("g12-pin-clock-{}", uuid::Uuid::new_v4().simple()),
        )
        .await
        .unwrap();
        super::super::kv_backend::time_window_contract::assert_real_window(&backend).await;
    }

    pub(super) mod commit_failure_candidates {
        include!("tikv_commit_failure_tests.rs");
        include!("tikv_commit_proxy_tests.rs");
        include!("tikv_whole_auth_postsubmission_tests.rs");
        include!("tikv_lock_order_tests.rs");
        include!("tikv_manager_bootstrap_tests.rs");
    }

    #[test]
    fn pd_tso_physical_milliseconds_are_converted_to_nanoseconds() {
        assert_eq!(
            pd_physical_ms_to_ns(1_725_000_000_123).unwrap(),
            1_725_000_000_123_000_000
        );
        assert!(pd_physical_ms_to_ns(i64::MAX).is_err());
    }
}

#[cfg(test)]
mod receipt_read_plan_tests {
    use super::*;

    fn limits(value: usize, response: usize) -> KvReadLimits {
        KvReadLimits {
            max_records: 1,
            max_key_bytes: 1024,
            max_value_bytes: value,
            max_total_bytes: value,
            max_response_bytes: response,
            max_data_requests: 3,
        }
    }

    #[test]
    fn receipt_uses_actual_eight_kib_decoder_and_keeps_caller_bound_hard() {
        assert_eq!(
            bounded_read_message_bytes(limits(4 << 10, 8 << 10)).unwrap(),
            8 << 10
        );
        assert!(matches!(
            bounded_read_message_bytes(limits(4 << 10, (8 << 10) - 1)),
            Err(WorkspaceError::InvalidReadPlan(_))
        ));
        assert!(matches!(
            bounded_read_message_bytes(limits((4 << 10) + 1, 8 << 10)),
            Err(WorkspaceError::InvalidReadPlan(_))
        ));
        assert_eq!(
            bounded_read_message_bytes(limits(12 << 10, 16 << 10)).unwrap(),
            16 << 10
        );
        assert_eq!(
            bounded_read_message_bytes(limits(48 << 10, 64 << 10)).unwrap(),
            64 << 10
        );
        assert_eq!(
            bounded_read_message_bytes(limits(96 << 10, 128 << 10)).unwrap(),
            128 << 10
        );
        assert_eq!(
            bounded_read_message_bytes(limits(256 << 10, 512 << 10)).unwrap(),
            512 << 10
        );
        assert!(matches!(
            bounded_read_message_bytes(limits((256 << 10) + 1, 512 << 10)),
            Err(WorkspaceError::UnsupportedCapability(_))
        ));
    }
}
