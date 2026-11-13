//! Central packed GC. Kubernetes election only schedules backend-authenticated GC.
//!
//! A Lease response does not grant backend deletion authority. Every catalog
//! scope must authenticate through WorkspaceAdmin::packed_gc_admin, and every
//! tick retains its backend-owned transactional fences. Loss of the scheduling
//! lease cancels admission and drains the owned work before another election.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context as _};
use async_trait::async_trait;
use brewfs::workspace::packed_admin::{
    PackedGcAdmin, PackedGcCursor, PackedGcPolicy, PackedGcTickRequest,
};
use chrono::Utc;
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use kube::api::{Api, ListParams, PostParams};
use kube::{Client, ResourceExt};
use tokio::time::{sleep, sleep_until, timeout, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use uuid::Uuid;

use crate::crd::BrewFSCluster;

use super::admin::{catalog_namespace, connect_workspace_admin_for_cluster};

const LEASE_NAME: &str = "brewfs-packed-gc";
const LEASE_SECONDS: i32 = 30;
const SAFETY_MARGIN: Duration = Duration::from_secs(5);
const API_TIMEOUT: Duration = Duration::from_secs(5);
const RENEW_EVERY: Duration = Duration::from_secs(5);
const TICK_EVERY: Duration = Duration::from_secs(2);
const CLUSTER_PAGE_SIZE: u32 = 16;
const CURSOR_KEY: &str = "packed-gc-cursor";
const CATALOG_KEY: &str = "catalog-namespace";
const CLUSTER_UID_KEY: &str = "cluster-uid";
const MAX_CURSOR_BYTES: usize = 1024;

#[async_trait]
trait LeaseApi: Send + Sync {
    async fn get(&self) -> anyhow::Result<Option<Lease>>;
    async fn create(&self, lease: &Lease) -> anyhow::Result<Lease>;
    async fn replace(&self, lease: &Lease) -> anyhow::Result<Lease>;
}

struct KubernetesLeaseApi(Api<Lease>);

#[async_trait]
impl LeaseApi for KubernetesLeaseApi {
    async fn get(&self) -> anyhow::Result<Option<Lease>> {
        Ok(self.0.get_opt(LEASE_NAME).await?)
    }

    async fn create(&self, lease: &Lease) -> anyhow::Result<Lease> {
        Ok(self.0.create(&PostParams::default(), lease).await?)
    }

    async fn replace(&self, lease: &Lease) -> anyhow::Result<Lease> {
        // Kubernetes enforces metadata.resourceVersion (and UID) on replace.
        Ok(self
            .0
            .replace(LEASE_NAME, &PostParams::default(), lease)
            .await?)
    }
}

#[derive(Clone)]
struct Claim {
    lease: Lease,
    deadline: Instant,
}

struct Observation {
    uid: String,
    resource_version: String,
    since: Instant,
    duration: Duration,
}

struct Election<A> {
    api: A,
    holder: String,
    observed: Option<Observation>,
}

impl<A: LeaseApi> Election<A> {
    fn new(api: A, holder: String) -> Self {
        Self {
            api,
            holder,
            observed: None,
        }
    }

    async fn acquire(&mut self) -> anyhow::Result<Option<Claim>> {
        let current = timeout(API_TIMEOUT, self.api.get()).await??;
        let Some(current) = current else {
            self.observed = None;
            return self.write(None).await.map(Some);
        };
        let (uid, rv, spec) = lease_identity(&current)?;
        if spec.holder_identity.as_deref() == Some(self.holder.as_str())
            || spec.holder_identity.as_deref().is_none_or(str::is_empty)
        {
            return self.write(Some(current)).await.map(Some);
        }
        let seconds = spec
            .lease_duration_seconds
            .ok_or_else(|| anyhow!("GC Lease has no TTL"))?;
        if !(10..=300).contains(&seconds) {
            bail!("GC Lease TTL is outside the supported range");
        }
        let duration = Duration::from_secs(seconds as u64) + SAFETY_MARGIN;
        let unchanged = self.observed.as_ref().is_some_and(|last| {
            last.uid == uid && last.resource_version == rv && last.duration == duration
        });
        if !unchanged {
            self.observed = Some(Observation {
                uid: uid.to_owned(),
                resource_version: rv.to_owned(),
                since: Instant::now(),
                duration,
            });
            return Ok(None);
        }
        // Wait a full observed TTL on our monotonic clock. Wall-clock renewal
        // timestamps are diagnostic only; clock skew never shortens this wait.
        if self
            .observed
            .as_ref()
            .is_some_and(|last| last.since.elapsed() >= last.duration)
        {
            self.write(Some(current)).await.map(Some)
        } else {
            Ok(None)
        }
    }

    async fn renew(&self, claim: &Claim) -> anyhow::Result<Claim> {
        if Instant::now() >= claim.deadline {
            bail!("GC scheduling authority already expired");
        }
        self.write(Some(claim.lease.clone())).await
    }

    async fn write(&self, old: Option<Lease>) -> anyhow::Result<Claim> {
        let previous = old.as_ref().map(lease_identity).transpose()?;
        let mut lease = old.clone().unwrap_or_else(|| Lease {
            metadata: ObjectMeta {
                name: Some(LEASE_NAME.into()),
                ..Default::default()
            },
            ..Default::default()
        });
        let started = Instant::now();
        let changed_holder = old
            .as_ref()
            .and_then(|old| old.spec.as_ref())
            .and_then(|spec| spec.holder_identity.as_deref())
            != Some(self.holder.as_str());
        lease.spec = Some(LeaseSpec {
            holder_identity: Some(self.holder.clone()),
            lease_duration_seconds: Some(LEASE_SECONDS),
            acquire_time: old
                .as_ref()
                .filter(|_| !changed_holder)
                .and_then(|old| old.spec.as_ref())
                .and_then(|s| s.acquire_time.clone())
                .or_else(|| Some(MicroTime(Utc::now()))),
            renew_time: Some(MicroTime(Utc::now())),
            lease_transitions: Some(
                old.as_ref()
                    .and_then(|old| old.spec.as_ref())
                    .and_then(|s| s.lease_transitions)
                    .unwrap_or(0)
                    .checked_add(i32::from(changed_holder))
                    .ok_or_else(|| anyhow!("GC Lease transition overflow"))?,
            ),
        });
        let response = if old.is_some() {
            timeout(API_TIMEOUT, self.api.replace(&lease)).await??
        } else {
            timeout(API_TIMEOUT, self.api.create(&lease)).await??
        };
        let (uid, rv, spec) = lease_identity(&response)?;
        if spec.holder_identity.as_deref() != Some(self.holder.as_str())
            || spec.lease_duration_seconds != Some(LEASE_SECONDS)
            || previous.is_some_and(|(old_uid, old_rv, _)| uid != old_uid || rv == old_rv)
        {
            bail!("GC Lease write response did not authenticate the requested claim");
        }
        let deadline = started + Duration::from_secs(LEASE_SECONDS as u64) - SAFETY_MARGIN;
        if Instant::now() >= deadline {
            bail!("GC Lease write response arrived after the local authority deadline");
        }
        Ok(Claim {
            lease: response,
            deadline,
        })
    }
}

fn lease_identity(lease: &Lease) -> anyhow::Result<(&str, &str, &LeaseSpec)> {
    if lease.metadata.name.as_deref() != Some(LEASE_NAME)
        || lease.metadata.deletion_timestamp.is_some()
    {
        bail!("unexpected or deleting GC Lease");
    }
    let uid = lease
        .metadata
        .uid
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("GC Lease UID is missing"))?;
    let rv = lease
        .metadata
        .resource_version
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("GC Lease resourceVersion is missing"))?;
    let spec = lease
        .spec
        .as_ref()
        .ok_or_else(|| anyhow!("GC Lease spec is missing"))?;
    Ok((uid, rv, spec))
}

#[async_trait]
trait OwnedWork: Send + Sync {
    /// Returning means every tick/backend scope owned by this call has drained.
    async fn run(&self, cancel: CancellationToken) -> anyhow::Result<()>;
}

async fn leader_session<A: LeaseApi, W: OwnedWork>(
    election: &Election<A>,
    mut claim: Claim,
    work: &W,
    stop: &CancellationToken,
) -> anyhow::Result<()> {
    let cancel = stop.child_token();
    let work_future = work.run(cancel.clone());
    tokio::pin!(work_future);
    let mut next_renew = Instant::now() + RENEW_EVERY;
    let outcome = loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => break Ok(()),
            _ = sleep_until(claim.deadline) => break Err(anyhow!("GC scheduling Lease expired")),
            result = &mut work_future => return result,
            _ = sleep_until(next_renew) => {
                let renewed = {
                    let renewal = election.renew(&claim);
                    tokio::pin!(renewal);
                    tokio::select! {
                        biased;
                        _ = stop.cancelled() => break Ok(()),
                        _ = sleep_until(claim.deadline) => break Err(anyhow!("GC Lease expired during renewal")),
                        result = &mut work_future => return result,
                        result = &mut renewal => result,
                    }
                };
                match renewed {
                    Ok(new_claim) if Instant::now() < claim.deadline => claim = new_claim,
                    Ok(_) => break Err(anyhow!("late GC Lease renewal rejected")),
                    Err(error) => break Err(error.context("GC Lease renewal failed")),
                }
                next_renew = Instant::now() + RENEW_EVERY;
            }
        }
    };
    // Do not abort/drop a tick on election loss. The backend may have dispatched
    // a transaction/DELETE already. Its owner must observe cancellation and drain.
    cancel.cancel();
    let drained = work_future.await;
    outcome.and(drained)
}

struct CatalogWork {
    client: Client,
}

#[derive(Default)]
struct ClusterQueue {
    continuation: Option<String>,
    pending: VecDeque<BrewFSCluster>,
}

impl ClusterQueue {
    fn accept_page(
        &mut self,
        continuation: Option<String>,
        items: Vec<BrewFSCluster>,
    ) -> anyhow::Result<()> {
        if !self.pending.is_empty() || items.len() > CLUSTER_PAGE_SIZE as usize {
            bail!("Kubernetes GC discovery exceeded its bounded pending page");
        }
        self.continuation = continuation.filter(|s| !s.is_empty());
        self.pending = items.into();
        Ok(())
    }
}

struct CatalogCursor {
    name: String,
    uid: String,
    catalog_namespace: String,
    current: Option<ConfigMap>,
    cursor: PackedGcCursor,
}

#[async_trait]
impl OwnedWork for CatalogWork {
    async fn run(&self, cancel: CancellationToken) -> anyhow::Result<()> {
        let mut queue = ClusterQueue::default();
        loop {
            if cancel.is_cancelled() {
                return Ok(());
            }
            if queue.pending.is_empty() {
                let mut params = ListParams::default().limit(CLUSTER_PAGE_SIZE);
                if let Some(token) = queue.continuation.as_deref() {
                    params = params.continue_token(token);
                }
                let clusters = Api::<BrewFSCluster>::all(self.client.clone());
                let page = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Ok(()),
                    result = timeout(API_TIMEOUT, clusters.list(&params)) => result??,
                };
                queue.accept_page(page.metadata.continue_, page.items)?;
            }
            if let Some(cluster) = queue.pending.pop_front() {
                if cluster.metadata.deletion_timestamp.is_none()
                    && cluster
                        .spec
                        .workspace
                        .as_ref()
                        .is_some_and(|spec| spec.enabled)
                {
                    if let Err(error) = self.tick_cluster(&cluster, &cancel).await {
                        warn!(cluster = %cluster.name_any(), %error, "packed GC catalog deferred");
                    }
                }
            }
            // At most one cluster dispatch per scheduling slice; empty pages,
            // disabled catalogs and credential errors cannot create a busy loop.
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(()),
                _ = sleep(TICK_EVERY) => {}
            }
        }
    }
}

impl CatalogWork {
    async fn tick_cluster(
        &self,
        cluster: &BrewFSCluster,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        let Some(spec) = cluster.spec.workspace.as_ref() else {
            return Ok(());
        };
        spec.validate().map_err(anyhow::Error::msg)?;
        let namespace = cluster
            .namespace()
            .ok_or_else(|| anyhow!("GC cluster namespace is missing"))?;
        let uid = cluster
            .uid()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow!("GC cluster UID is missing"))?;
        let cursor_name = cursor_name(&uid)?;
        let cursor_api = Api::<ConfigMap>::namespaced(self.client.clone(), &namespace);
        let current = timeout(API_TIMEOUT, cursor_api.get_opt(&cursor_name)).await??;
        let catalog = catalog_namespace(&cluster.name_any(), &namespace, spec);
        let saved = load_cursor(cursor_name, uid, catalog, current)?;
        let prepare = async {
            let admin =
                connect_workspace_admin_for_cluster(&self.client, cluster, &namespace, spec)
                    .await?;
            admin.packed_gc_admin().await
        };
        let policy = PackedGcPolicy {
            lease_ttl_seconds: u64::from(spec.lease_ttl_seconds),
            grace_seconds: u64::from(spec.gc_grace_seconds),
            max_scans: 16,
            max_operations: 1,
            max_protective_rows: 64,
        };
        // Do not drop a partially constructed GC owner on cancellation. The
        // facade returns only authenticated scopes; cancellation is checked
        // again after preparation and before any tick is dispatched.
        let outcome =
            authenticated_tick(prepare, policy, saved.cursor.clone(), cancel.clone()).await;
        match outcome {
            Ok(Some(cursor)) if !cancel.is_cancelled() => {
                // The cursor is a routing hint, never GC authority. Lost/failed
                // writes retain the old hint; backend recovery is idempotent.
                save_cursor(&cursor_api, cluster, saved, &cursor).await?;
            }
            Ok(Some(_)) => {}
            Ok(None) => {}
            Err(error) => {
                warn!(cluster = %cluster.name_any(), namespace, %error, "packed GC tick deferred")
            }
        }
        Ok(())
    }
}

fn cursor_name(uid: &str) -> anyhow::Result<String> {
    // UID is cluster-incarnation identity, never user-supplied metadata.name.
    let parsed = Uuid::parse_str(uid).context("invalid Kubernetes cluster UID")?;
    Ok(format!("brewfs-gc-{}", parsed.simple()))
}

fn load_cursor(
    name: String,
    uid: String,
    catalog_namespace: String,
    current: Option<ConfigMap>,
) -> anyhow::Result<CatalogCursor> {
    let cursor = if let Some(ref map) = current {
        if map.metadata.name.as_deref() != Some(&name)
            || map.metadata.uid.as_deref().is_none_or(str::is_empty)
            || map
                .metadata
                .resource_version
                .as_deref()
                .is_none_or(str::is_empty)
            || map.metadata.deletion_timestamp.is_some()
        {
            bail!("GC cursor ConfigMap identity is incomplete or deleting");
        }
        let data = map
            .data
            .as_ref()
            .ok_or_else(|| anyhow!("GC cursor ConfigMap has no data"))?;
        if data.get(CLUSTER_UID_KEY) != Some(&uid)
            || data.get(CATALOG_KEY) != Some(&catalog_namespace)
            || map.metadata.owner_references.as_ref().is_none_or(|owners| {
                !owners.iter().any(|owner| {
                    owner.uid == uid
                        && owner.kind == "BrewFSCluster"
                        && owner.api_version == "storage.brewfs.io/v1alpha1"
                })
            })
        {
            bail!("GC cursor belongs to another cluster/catalog incarnation");
        }
        let encoded = data
            .get(CURSOR_KEY)
            .ok_or_else(|| anyhow!("GC cursor value is missing"))?;
        if encoded.len() > MAX_CURSOR_BYTES {
            bail!("GC cursor exceeds its byte limit");
        }
        PackedGcCursor::decode(encoded.as_bytes())?
    } else {
        PackedGcCursor::default()
    };
    Ok(CatalogCursor {
        name,
        uid,
        catalog_namespace,
        current,
        cursor,
    })
}

async fn save_cursor(
    api: &Api<ConfigMap>,
    cluster: &BrewFSCluster,
    saved: CatalogCursor,
    cursor: &PackedGcCursor,
) -> anyhow::Result<()> {
    let bytes = cursor.encode()?;
    if bytes.len() > MAX_CURSOR_BYTES {
        bail!("GC cursor encoding exceeded its byte limit");
    }
    let value = String::from_utf8(bytes).context("GC cursor encoding is not UTF-8")?;
    let existed = saved.current.is_some();
    let mut map = saved.current.unwrap_or_else(|| ConfigMap {
        metadata: ObjectMeta {
            name: Some(saved.name),
            owner_references: Some(vec![
                k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
                    api_version: "storage.brewfs.io/v1alpha1".into(),
                    kind: "BrewFSCluster".into(),
                    name: cluster.name_any(),
                    uid: saved.uid.clone(),
                    controller: Some(false),
                    block_owner_deletion: Some(false),
                },
            ]),
            ..Default::default()
        },
        ..Default::default()
    });
    map.data = Some(BTreeMap::from([
        (CURSOR_KEY.into(), value),
        (CLUSTER_UID_KEY.into(), saved.uid),
        (CATALOG_KEY.into(), saved.catalog_namespace),
    ]));
    if existed {
        timeout(
            API_TIMEOUT,
            api.replace(&map.name_any(), &PostParams::default(), &map),
        )
        .await??;
    } else {
        timeout(API_TIMEOUT, api.create(&PostParams::default(), &map)).await??;
    }
    Ok(())
}

async fn run_tick(
    admin: Arc<dyn PackedGcAdmin>,
    policy: PackedGcPolicy,
    cursor: PackedGcCursor,
    cancel: CancellationToken,
) -> anyhow::Result<Option<PackedGcCursor>> {
    policy.validate()?;
    if cancel.is_cancelled() {
        return Ok(None);
    }
    let report = admin
        .tick(PackedGcTickRequest {
            policy,
            cursor,
            cancel,
        })
        .await?;
    info!(
        scanned = report.scanned,
        attempted = report.attempted,
        deferred = report.deferred,
        deleted_objects = report.deleted_objects,
        native_deleted_layers = report.native_deleted_layers,
        "packed central GC tick complete"
    );
    Ok(Some(report.next_cursor))
}

async fn authenticated_tick(
    authenticated: impl std::future::Future<Output = anyhow::Result<Arc<dyn PackedGcAdmin>>>,
    policy: PackedGcPolicy,
    cursor: PackedGcCursor,
    cancel: CancellationToken,
) -> anyhow::Result<Option<PackedGcCursor>> {
    // Only a successfully authenticated factory result reaches dispatch. Do not
    // convert auth errors into a permissive backend or a default admin role.
    let admin = authenticated.await?;
    let result = run_tick(admin.clone(), policy, cursor, cancel).await;
    let drained = admin.shutdown().await;
    // Always perform shutdown, including malformed policy/failed tick/cancel.
    drained?;
    result
}

pub async fn run(client: Client, stop: CancellationToken) -> anyhow::Result<()> {
    let namespace = client.default_namespace().to_owned();
    let api = KubernetesLeaseApi(Api::namespaced(client.clone(), &namespace));
    let mut election = Election::new(api, format!("brewfs-gc-{}", Uuid::new_v4()));
    let work = CatalogWork { client };
    info!(
        namespace,
        lease = LEASE_NAME,
        "starting leader-elected packed GC worker"
    );
    while !stop.is_cancelled() {
        let acquire = tokio::select! {
            biased;
            _ = stop.cancelled() => return Ok(()),
            result = election.acquire() => result,
        };
        match acquire {
            Ok(Some(claim)) => {
                if let Err(error) = leader_session(&election, claim, &work, &stop).await {
                    warn!(%error, "packed GC leader stopped and owned work drained");
                }
            }
            Ok(None) => {}
            Err(error) => warn!(%error, "packed GC election refused; no work admitted"),
        }
        tokio::select! {
            biased;
            _ = stop.cancelled() => break,
            _ = sleep(RENEW_EVERY) => {}
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "gc_tests.rs"]
mod tests;
