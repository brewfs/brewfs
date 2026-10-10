//! Bounded authenticated IP06 routing with mandatory subtree weights.

use super::budget::V3Owned;
use super::{V3_FOOTER_LEN, V3_HEADER_LEN, V3ObjectKind, V3ObjectRef, encode_v3_object};
use super::{V3BudgetPool, V3MountBudget};
use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::workspace_overlay::packed_v3::wire::{PackedResult, PackedWireError, Reader, Writer};
use moka::future::Cache;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub const V3_INDEX_BODY_LIMIT: usize = 256 * 1024;
const MAX_RECORDS: usize = 1024;
const MAX_KEY_BYTES: usize = 2048;
const MAX_VALUE_BYTES: usize = 8192;
const MAX_HEIGHT: u8 = 16;

/// Immutable record view. Its page keeps the actual decoded allocation charged
/// even after cache eviction, reader close, and cloning this view.
#[derive(Clone, Debug)]
pub struct V3IndexRecordHandle {
    page: Arc<V3Owned<V3IndexPage>>,
    ordinal: usize,
}
impl std::ops::Deref for V3IndexRecordHandle {
    type Target = V3IndexRecord;
    fn deref(&self) -> &Self::Target {
        &self.page.records[self.ordinal]
    }
}
impl PartialEq for V3IndexRecordHandle {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}
impl Eq for V3IndexRecordHandle {}

/// Leaf bytes borrowed from an admitted immutable page, never an unguarded
/// post-await Vec copy. Clones share that page's one actual allocation owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V3IndexValueHandle {
    record: V3IndexRecordHandle,
}
impl std::ops::Deref for V3IndexValueHandle {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match &self.record.value {
            V3IndexValue::Leaf(bytes) => bytes,
            V3IndexValue::Child { .. } => unreachable!("leaf handle constructed from branch"),
        }
    }
}
impl AsRef<[u8]> for V3IndexValueHandle {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

/// Fixed-capacity, admitted result slots. Iteration borrows the rows; there is
/// no conversion that detaches the Vec from its reservation.
#[derive(Debug)]
pub struct V3IndexRows<T> {
    rows: Vec<T>,
    _permit: super::V3OwnedPermit,
}
impl<T> V3IndexRows<T> {
    fn new(budget: &Arc<V3MountBudget>, capacity: usize) -> PackedResult<Self> {
        let bytes = capacity
            .checked_mul(std::mem::size_of::<T>())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Vec<T>>()))
            .ok_or_else(|| PackedWireError::LimitExceeded("index result slots overflow".into()))?;
        let permit = budget.admit(&[(V3BudgetPool::Metadata, bytes as u64)])?;
        Ok(Self {
            rows: Vec::with_capacity(capacity),
            _permit: permit,
        })
    }
}
impl<T> std::ops::Deref for V3IndexRows<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        &self.rows
    }
}
impl<'a, T> IntoIterator for &'a V3IndexRows<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.rows.iter()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum V3IndexValue {
    Leaf(Vec<u8>),
    Child {
        reference: V3ObjectRef,
        subtree_weight: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V3IndexRecord {
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
    pub value: V3IndexValue,
}

impl V3IndexRecord {
    /// Groups count visible dentries; every other index counts leaf records.
    /// The leaf weight is derived from its authenticated value, never supplied
    /// independently by the producer.
    pub(crate) fn subtree_weight(&self, kind: V3ObjectKind) -> PackedResult<u64> {
        match &self.value {
            V3IndexValue::Child { subtree_weight, .. } if *subtree_weight > 0 => {
                Ok(*subtree_weight)
            }
            V3IndexValue::Child { .. } => Err(PackedWireError::Invalid(
                "IP06 internal subtree has zero weight".into(),
            )),
            V3IndexValue::Leaf(value) if kind == V3ObjectKind::GroupIndex => {
                let group = super::V3GroupRef::decode_value(value)?;
                let mut first = group.parent_dir_key.to_vec();
                first.extend_from_slice(&group.first_name);
                let mut last = group.parent_dir_key.to_vec();
                last.extend_from_slice(&group.last_name);
                if self.first_key != first || self.last_key != last {
                    return Err(PackedWireError::Invalid(
                        "IP06 group fences disagree with authenticated group".into(),
                    ));
                }
                Ok(u64::from(group.entry_count))
            }
            V3IndexValue::Leaf(_) => Ok(1),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V3IndexPage {
    pub kind: V3ObjectKind,
    pub height: u8,
    pub records: Vec<V3IndexRecord>,
}

fn index_kind(kind: V3ObjectKind) -> bool {
    matches!(
        kind,
        V3ObjectKind::GroupIndex
            | V3ObjectKind::InodeIndex
            | V3ObjectKind::ContainerIndex
            | V3ObjectKind::FrameIndex
            | V3ObjectKind::ColdIndex
            | V3ObjectKind::ReverseIndex
            | V3ObjectKind::LargeIndex
            | V3ObjectKind::SourceStatsIndex
    )
}

impl V3IndexPage {
    pub fn encode(&self) -> PackedResult<Vec<u8>> {
        self.validate()?;
        let mut w = Writer::default();
        w.bytes(b"IP06");
        w.u8(self.height);
        w.bytes(&[0; 3]);
        w.u32(self.records.len() as u32);
        for record in &self.records {
            w.u16(record.first_key.len() as u16);
            w.u16(record.last_key.len() as u16);
            w.bytes(&record.first_key);
            w.bytes(&record.last_key);
            match &record.value {
                V3IndexValue::Leaf(value) => {
                    w.u32(value.len() as u32);
                    w.bytes(value);
                }
                V3IndexValue::Child {
                    reference,
                    subtree_weight,
                } => {
                    w.u64(*subtree_weight);
                    w.u8(reference.kind as u8);
                    w.u8(0);
                    w.u16(reference.key.len() as u16);
                    w.u64(reference.object_len);
                    w.bytes(&reference.digest);
                    w.bytes(reference.key.as_bytes());
                }
            }
        }
        encode_v3_object(self.kind, &w.finish(), V3_INDEX_BODY_LIMIT)
    }

    pub fn decode(reference: &V3ObjectRef, bytes: &[u8]) -> PackedResult<Self> {
        if !index_kind(reference.kind) {
            return Err(PackedWireError::Invalid(
                "wire 005 index ref has a non-index kind".into(),
            ));
        }
        let body = reference.verify(bytes, V3_INDEX_BODY_LIMIT)?;
        let mut r = Reader::new(body);
        if r.take(4)? != b"IP06" {
            return Err(PackedWireError::UnsupportedFormat(
                "wire 005 index payload mismatch".into(),
            ));
        }
        let height = r.u8()?;
        r.skip_zeroes(3)?;
        let count = r.u32()? as usize;
        if height > MAX_HEIGHT || count > MAX_RECORDS || count > body.len().saturating_sub(12) / 10
        {
            return Err(PackedWireError::LimitExceeded(
                "wire 005 index count/height exceeds page budget".into(),
            ));
        }
        let mut records = Vec::with_capacity(count);
        for _ in 0..count {
            let first_len = r.u16()? as usize;
            let last_len = r.u16()? as usize;
            if first_len == 0
                || last_len == 0
                || first_len > MAX_KEY_BYTES
                || last_len > MAX_KEY_BYTES
            {
                return Err(PackedWireError::LimitExceeded(
                    "wire 005 index fence bytes exceed budget".into(),
                ));
            }
            let first_key = r.take(first_len)?.to_vec();
            let last_key = r.take(last_len)?.to_vec();
            let value = if height == 0 {
                let length = r.u32()? as usize;
                if length > MAX_VALUE_BYTES {
                    return Err(PackedWireError::LimitExceeded(
                        "wire 005 leaf value exceeds budget".into(),
                    ));
                }
                V3IndexValue::Leaf(r.take(length)?.to_vec())
            } else {
                let subtree_weight = r.u64()?;
                if r.u8()? != reference.kind as u8 {
                    return Err(PackedWireError::Invalid(
                        "wire 005 index child kind mismatch".into(),
                    ));
                }
                r.skip_zeroes(1)?;
                let key_len = r.u16()? as usize;
                if key_len == 0 || key_len > 4096 {
                    return Err(PackedWireError::LimitExceeded(
                        "wire 005 child object key exceeds budget".into(),
                    ));
                }
                let object_len = r.u64()?;
                let digest = r.array::<32>()?;
                let key = std::str::from_utf8(r.take(key_len)?)
                    .map_err(|_| {
                        PackedWireError::Invalid("wire 005 child object key is not UTF-8".into())
                    })?
                    .to_owned();
                V3IndexValue::Child {
                    reference: V3ObjectRef {
                        key,
                        kind: reference.kind,
                        object_len,
                        digest,
                    },
                    subtree_weight,
                }
            };
            records.push(V3IndexRecord {
                first_key,
                last_key,
                value,
            });
        }
        if !r.is_empty() {
            return Err(PackedWireError::Invalid(
                "wire 005 index has trailing bytes".into(),
            ));
        }
        let page = Self {
            kind: reference.kind,
            height,
            records,
        };
        page.validate()?;
        Ok(page)
    }

    fn validate(&self) -> PackedResult<()> {
        if !index_kind(self.kind)
            || self.height > MAX_HEIGHT
            || self.records.len() > MAX_RECORDS
            || (self.height > 0 && self.records.is_empty())
        {
            return Err(PackedWireError::LimitExceeded(
                "wire 005 index kind/height/count exceeds limits".into(),
            ));
        }
        let mut bytes = 12usize;
        let mut previous: Option<&[u8]> = None;
        for record in &self.records {
            if record.first_key.is_empty()
                || record.first_key.len() > MAX_KEY_BYTES
                || record.last_key.len() > MAX_KEY_BYTES
                || record.first_key > record.last_key
                || previous.is_some_and(|last| last >= record.first_key.as_slice())
            {
                return Err(PackedWireError::Invalid(
                    "wire 005 index fences overlap or are invalid".into(),
                ));
            }
            bytes += 4 + record.first_key.len() + record.last_key.len();
            match &record.value {
                V3IndexValue::Leaf(value) if self.height == 0 && value.len() <= MAX_VALUE_BYTES => {
                    bytes += 4 + value.len()
                }
                V3IndexValue::Child { reference, .. } if self.height > 0 => {
                    super::validate_key(&reference.key)?;
                    if reference.kind != self.kind
                        || reference.digest == [0; 32]
                        || reference.object_len < (V3_HEADER_LEN + V3_FOOTER_LEN) as u64
                        || reference.object_len
                            > (V3_HEADER_LEN + V3_INDEX_BODY_LIMIT + V3_FOOTER_LEN) as u64
                    {
                        return Err(PackedWireError::Invalid(
                            "wire 005 index child identity/budget mismatch".into(),
                        ));
                    }
                    bytes += 52 + reference.key.len();
                }
                _ => {
                    return Err(PackedWireError::Invalid(
                        "wire 005 index value does not match page height".into(),
                    ));
                }
            }
            if bytes > V3_INDEX_BODY_LIMIT {
                return Err(PackedWireError::LimitExceeded(
                    "wire 005 index encoded bytes exceed page budget".into(),
                ));
            }
            previous = Some(&record.last_key);
        }
        self.total_weight()?;
        Ok(())
    }

    pub fn total_weight(&self) -> PackedResult<u64> {
        self.records.iter().try_fold(0u64, |sum, record| {
            sum.checked_add(record.subtree_weight(self.kind)?)
                .ok_or_else(|| {
                    PackedWireError::LimitExceeded("IP06 subtree weight overflows u64".into())
                })
        })
    }

    pub fn bounds(&self) -> Option<(&[u8], &[u8])> {
        Some((
            &self.records.first()?.first_key,
            &self.records.last()?.last_key,
        ))
    }

    pub fn lookup(&self, key: &[u8]) -> Option<&V3IndexRecord> {
        let index = self
            .records
            .partition_point(|record| record.last_key.as_slice() < key);
        self.records
            .get(index)
            .filter(|record| record.first_key.as_slice() <= key)
    }

    fn weight(&self) -> u32 {
        // Arc header, owned permit, cache identity and page wrapper are kept
        // with the backing Vecs even after the cache drops its reference.
        let bytes = std::mem::size_of::<Self>()
            + 256
            + self
                .records
                .iter()
                .map(|r| {
                    std::mem::size_of::<V3IndexRecord>()
                        + r.first_key.capacity()
                        + r.last_key.capacity()
                        + match &r.value {
                            V3IndexValue::Leaf(value) => value.capacity(),
                            V3IndexValue::Child { reference, .. } => reference.key.capacity(),
                        }
                })
                .sum::<usize>();
        (bytes
            + self.records.capacity().saturating_sub(self.records.len())
                * std::mem::size_of::<V3IndexRecord>())
        .min(u32::MAX as usize) as u32
    }
}

/// Unlike Moka's deferred policy counters, every cache value takes this charge
/// before it can become visible. The charge lasts through the final cache-value
/// owner, including a removed entry still awaiting actual destruction.
struct V3IndexCacheOwnership {
    capacity: u64,
    owned: AtomicU64,
    peak: AtomicU64,
}

impl V3IndexCacheOwnership {
    fn new(capacity: u64) -> Self {
        Self {
            capacity,
            owned: AtomicU64::new(0),
            peak: AtomicU64::new(0),
        }
    }

    fn try_admit(self: &Arc<Self>, bytes: u64) -> Option<V3IndexCacheLease> {
        let previous = self
            .owned
            .try_update(Ordering::AcqRel, Ordering::Acquire, |owned| {
                owned.checked_add(bytes).filter(|sum| *sum <= self.capacity)
            })
            .ok()?;
        self.peak.fetch_max(previous + bytes, Ordering::AcqRel);
        Some(V3IndexCacheLease {
            ownership: self.clone(),
            bytes,
        })
    }
}

struct V3IndexCacheLease {
    ownership: Arc<V3IndexCacheOwnership>,
    bytes: u64,
}

impl Drop for V3IndexCacheLease {
    fn drop(&mut self) {
        self.ownership.owned.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

/// A cursor receives only the inner page Arc. Its mount Metadata permit remains
/// alive after cache eviction; it does not pin this separate retention charge.
struct V3RetainedIndexPage {
    page: Arc<V3Owned<V3IndexPage>>,
    _cache_lease: V3IndexCacheLease,
}

impl std::ops::Deref for V3RetainedIndexPage {
    type Target = V3Owned<V3IndexPage>;

    fn deref(&self) -> &Self::Target {
        &self.page
    }
}

/// Moka shares fallible initializer outcomes without inserting Err into its
/// value map. A valid unretained page uses that existing singleflight channel;
/// it is returned as success to callers, never as a wire/backend failure.
enum V3IndexFetchError {
    Wire(PackedWireError),
    Unretained(Arc<V3Owned<V3IndexPage>>),
}

pub struct V3IndexReader<B: ObjectBackend + Clone> {
    client: ObjectClient<B>,
    pages: Cache<([u8; 32], V3ObjectKind, u64), Arc<V3RetainedIndexPage>>,
    cache_ownership: Arc<V3IndexCacheOwnership>,
    configured_bytes: u64,
    budget: Arc<V3MountBudget>,
    demand_pipeline: tokio::sync::Mutex<V3ReaderRuntime<B>>,
}

struct V3ReaderRuntime<B: ObjectBackend + Clone> {
    closed: bool,
    coordinator: Option<Arc<super::pipeline::V3DemandCoordinator<B>>>,
}

pub(crate) struct V3IndexCacheStats {
    pages: Cache<([u8; 32], V3ObjectKind, u64), Arc<V3RetainedIndexPage>>,
    cache_ownership: Arc<V3IndexCacheOwnership>,
    configured_bytes: u64,
}
impl std::fmt::Debug for V3IndexCacheStats {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        output
            .debug_struct("V3IndexCacheStats")
            .field("configured_bytes", &self.configured_bytes)
            .finish()
    }
}
impl V3IndexCacheStats {
    pub(crate) fn render_into(&self, output: &mut dyn std::fmt::Write) {
        let _ = writeln!(
            output,
            "brewfs_packed_v3_index_cache_configured_bytes {}",
            self.configured_bytes
        );
        let _ = writeln!(
            output,
            "brewfs_packed_v3_index_cache_retained_weighted_bytes {}",
            self.pages.weighted_size()
        );
        let _ = writeln!(
            output,
            "brewfs_packed_v3_index_cache_retained_pages {}",
            self.pages.entry_count()
        );
        let _ = writeln!(
            output,
            "brewfs_packed_v3_index_cache_owned_bytes {}",
            self.cache_ownership.owned.load(Ordering::Acquire)
        );
        let _ = writeln!(
            output,
            "brewfs_packed_v3_index_cache_peak_owned_bytes {}",
            self.cache_ownership.peak.load(Ordering::Acquire)
        );
    }
}

#[derive(Clone)]
struct ChildExpectation {
    kind: V3ObjectKind,
    height: u8,
    first: Vec<u8>,
    last: Vec<u8>,
    weight: u64,
}
impl ChildExpectation {
    fn validate(&self, child: &V3IndexPage) -> PackedResult<()> {
        validate_child_claim(
            IndexPageClaim {
                kind: self.kind,
                height: self.height,
                bounds: Some((&self.first, &self.last)),
                weight: self.weight,
            },
            IndexPageClaim {
                kind: child.kind,
                height: child.height,
                bounds: child.bounds(),
                weight: child.total_weight()?,
            },
        )
    }
}

/// Shared parent-edge rule for bounded readers and complete occurrence audits.
/// Borrowing the fields avoids detaching fences from their admitted page/row.
pub(super) struct IndexPageClaim<'a> {
    pub kind: V3ObjectKind,
    pub height: u8,
    pub bounds: Option<(&'a [u8], &'a [u8])>,
    pub weight: u64,
}

pub(super) fn validate_child_claim(
    expected: IndexPageClaim<'_>,
    actual: IndexPageClaim<'_>,
) -> PackedResult<()> {
    if actual.kind != expected.kind
        || actual.height != expected.height
        || actual.bounds != expected.bounds
        || actual.weight != expected.weight
    {
        return Err(PackedWireError::Invalid(
            "IP06 child kind/height/fences/weight disagree with parent".into(),
        ));
    }
    Ok(())
}

pub(crate) struct V3WeightedCursor {
    root_digest: [u8; 32],
    lower: Vec<u8>,
    upper: Vec<u8>,
    stack: Vec<(Arc<V3Owned<V3IndexPage>>, usize)>,
    record_offset: u64,
    _permit: super::V3OwnedPermit,
}

impl<B: ObjectBackend + Clone + 'static> V3IndexReader<B> {
    pub fn new(client: ObjectClient<B>, cache_bytes: u64) -> Self {
        Self::with_budget(client, cache_bytes, V3MountBudget::defaults())
    }
    pub fn with_budget(
        client: ObjectClient<B>,
        cache_bytes: u64,
        budget: Arc<V3MountBudget>,
    ) -> Self {
        Self {
            client,
            pages: Cache::builder()
                .max_capacity(cache_bytes.max(1))
                .weigher(
                    |_: &([u8; 32], V3ObjectKind, u64), page: &Arc<V3RetainedIndexPage>| {
                        page.weight()
                    },
                )
                .build(),
            cache_ownership: Arc::new(V3IndexCacheOwnership::new(cache_bytes)),
            configured_bytes: cache_bytes,
            budget,
            demand_pipeline: tokio::sync::Mutex::new(V3ReaderRuntime {
                closed: false,
                coordinator: None,
            }),
        }
    }

    pub fn budget(&self) -> &Arc<V3MountBudget> {
        &self.budget
    }
    /// Reader shutdown is a hard admission boundary. The mount budget is
    /// closed by the outer session after transport drain, so checking it
    /// alone leaves a window in which an already-closed reader can issue new
    /// index GETs. Keep the runtime close bit as the authoritative reader
    /// fence and check it before every page/cache operation.
    async fn ensure_open(&self) -> PackedResult<()> {
        let runtime = self.demand_pipeline.lock().await;
        if runtime.closed || self.budget.state().closed {
            return Err(PackedWireError::LimitExceeded(
                "index reader is closed".into(),
            ));
        }
        Ok(())
    }
    pub(super) async fn demand_pipeline(
        &self,
    ) -> PackedResult<Arc<super::pipeline::V3DemandCoordinator<B>>> {
        let mut runtime = self.demand_pipeline.lock().await;
        if runtime.closed || self.budget.state().closed {
            return Err(PackedWireError::LimitExceeded(
                "index reader is closed".into(),
            ));
        }
        if let Some(coordinator) = &runtime.coordinator {
            return Ok(coordinator.clone());
        }
        let coordinator = super::pipeline::V3DemandCoordinator::new(
            self.client.clone(),
            self.budget.clone(),
            super::pipeline::V3PipelineLimits::default(),
        )?;
        runtime.coordinator = Some(coordinator.clone());
        Ok(coordinator)
    }
    pub(crate) fn cache_stats(&self) -> V3IndexCacheStats {
        V3IndexCacheStats {
            pages: self.pages.clone(),
            cache_ownership: self.cache_ownership.clone(),
            configured_bytes: self.configured_bytes,
        }
    }

    async fn load(&self, reference: &V3ObjectRef) -> PackedResult<Arc<V3Owned<V3IndexPage>>> {
        self.load_expected(reference, None).await
    }

    async fn load_expected(
        &self,
        reference: &V3ObjectRef,
        expected: Option<ChildExpectation>,
    ) -> PackedResult<Arc<V3Owned<V3IndexPage>>> {
        self.ensure_open().await?;
        // Cache hits and followers are mount work too. Admit before either
        // Moka operation or cloning the authenticated reference into a fetch
        // future. This request owner is deliberately independent of the
        // decoded page owner and lasts until this lookup future retires.
        // Moka's delayed table/policy storage and returned Vec copies remain
        // separate ownership work; this charge cannot stand in for either.
        let recipe = (reference.key.len() as u64)
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(512))
            .ok_or_else(|| {
                PackedWireError::LimitExceeded("index request recipe overflows".into())
            })?;
        let _request = self
            .budget
            .admit(&[(V3BudgetPool::Control, 1024), (V3BudgetPool::Plans, recipe)])?;
        super::validate_key(&reference.key)?;
        if !index_kind(reference.kind) {
            return Err(PackedWireError::Invalid(
                "wire 005 index ref has a non-index kind".into(),
            ));
        }
        use crate::cadapter::read_observer::ReadEvent;
        let class = super::page_read_class(reference.kind)?;
        if self.configured_bytes == 0 {
            self.client.read_event(class, ReadEvent::CacheDisabled);
            self.client.read_event(class, ReadEvent::FetchLeader);
            return Self::decode_owned(&self.client, reference, &self.budget, expected).await;
        }
        let key = (reference.digest, reference.kind, reference.object_len);
        if let Some(page) = self.pages.get(&key).await {
            self.client.read_event(class, ReadEvent::CacheHit);
            if let Some(expected) = &expected {
                expected.validate(&page)?;
            }
            return Ok(page.page.clone());
        }
        self.client.read_event(class, ReadEvent::CacheLookupMiss);
        for attempt in 0..2 {
            let client = self.client.clone();
            let reference = reference.clone();
            let budget = self.budget.clone();
            let fetch_expected = expected.clone();
            let pages = self.pages.clone();
            let cache_ownership = self.cache_ownership.clone();
            let elected = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let factory_elected = Arc::clone(&elected);
            let result = self
                .pages
                .try_get_with(key, async move {
                    factory_elected.store(true, std::sync::atomic::Ordering::Relaxed);
                    client.read_event(class, ReadEvent::FetchLeader);
                    let page = Self::decode_owned(&client, &reference, &budget, fetch_expected)
                        .await
                        .map_err(V3IndexFetchError::Wire)?;
                    let bytes = u64::from(page.weight());
                    if bytes > cache_ownership.capacity {
                        return Err(V3IndexFetchError::Unretained(page));
                    }
                    let mut lease = cache_ownership.try_admit(bytes);
                    if lease.is_none() {
                        // Drop only cache references. Any cursor still owns
                        // the page's original mount Metadata permit.
                        pages.invalidate_all();
                        pages.run_pending_tasks().await;
                        lease = cache_ownership.try_admit(bytes);
                    }
                    match lease {
                        Some(lease) => Ok(Arc::new(V3RetainedIndexPage {
                            page,
                            _cache_lease: lease,
                        })),
                        None => Err(V3IndexFetchError::Unretained(page)),
                    }
                })
                .await;
            match result {
                Ok(page) => {
                    if !elected.load(std::sync::atomic::Ordering::Relaxed) {
                        self.client
                            .read_event(class, ReadEvent::SharedResultAfterMiss);
                    }
                    if let Some(expected) = &expected {
                        expected.validate(&page)?;
                    }
                    return Ok(page.page.clone());
                }
                Err(error) => match error.as_ref() {
                    V3IndexFetchError::Unretained(page) => {
                        if !elected.load(std::sync::atomic::Ordering::Relaxed) {
                            self.client
                                .read_event(class, ReadEvent::SharedResultAfterMiss);
                        }
                        if let Some(expected) = &expected {
                            expected.validate(page)?;
                        }
                        return Ok(page.clone());
                    }
                    V3IndexFetchError::Wire(error)
                        if attempt == 0 && matches!(error, PackedWireError::LimitExceeded(_)) =>
                    {
                        // Cache eviction drops only cache references; cursor
                        // pins retain permits through their last consumer.
                        self.pages.invalidate_all();
                        self.pages.run_pending_tasks().await;
                    }
                    V3IndexFetchError::Wire(error) => return Err(error.clone()),
                },
            }
        }
        unreachable!("bounded cache admission attempts")
    }

    async fn decode_owned(
        client: &ObjectClient<B>,
        reference: &V3ObjectRef,
        budget: &Arc<V3MountBudget>,
        expected: Option<ChildExpectation>,
    ) -> PackedResult<Arc<V3Owned<V3IndexPage>>> {
        let mut permit = budget.admit(&[
            (
                V3BudgetPool::Stored,
                reference.object_len.checked_mul(2).ok_or_else(|| {
                    PackedWireError::LimitExceeded("index stream budget overflow".into())
                })?,
            ),
            (V3BudgetPool::Metadata, 2 << 20),
            (V3BudgetPool::Control, 2048),
        ])?;
        super::read_v3_page_validated(client, reference, V3_INDEX_BODY_LIMIT, |bytes| {
            let page = V3IndexPage::decode(reference, &bytes)?;
            if let Some(expected) = &expected {
                expected.validate(&page)?;
            }
            drop(bytes);
            permit.shrink(V3BudgetPool::Stored, 0)?;
            permit.shrink(V3BudgetPool::Control, 0)?;
            permit.shrink(V3BudgetPool::Metadata, page.weight() as u64)?;
            Ok(Arc::new(V3Owned::new(page, permit)))
        })
        .await
    }

    pub(crate) async fn observe_resolution<T>(
        &self,
        kind: V3ObjectKind,
        future: impl std::future::Future<Output = PackedResult<T>>,
    ) -> PackedResult<T> {
        let guard = self.client.begin_validation(super::page_read_class(kind)?);
        let result = future.await;
        if let Some(guard) = guard {
            match &result {
                Ok(_) => guard.succeed(),
                Err(error) => guard.fail(super::observer_validation_error(error.clone()).0),
            }
        }
        result
    }

    pub async fn lookup(
        &self,
        root: &V3ObjectRef,
        key: &[u8],
    ) -> PackedResult<Option<V3IndexValueHandle>> {
        if key.is_empty() || key.len() > MAX_KEY_BYTES {
            return Err(PackedWireError::LimitExceeded(
                "wire 005 index lookup key exceeds budget".into(),
            ));
        }
        let mut page = self.load(root).await?;
        for _ in 0..=MAX_HEIGHT {
            let Some(record) = page.lookup(key) else {
                return Ok(None);
            };
            match &record.value {
                V3IndexValue::Leaf(_) => {
                    let ordinal = page
                        .records
                        .partition_point(|record| record.last_key.as_slice() < key);
                    return Ok(Some(V3IndexValueHandle {
                        record: V3IndexRecordHandle { page, ordinal },
                    }));
                }
                V3IndexValue::Child { .. } => {
                    page = self.load_child(&page, record).await?;
                }
            }
        }
        Err(PackedWireError::LimitExceeded(
            "wire 005 index route exceeds maximum depth".into(),
        ))
    }

    async fn load_child(
        &self,
        parent: &V3IndexPage,
        record: &V3IndexRecord,
    ) -> PackedResult<Arc<V3Owned<V3IndexPage>>> {
        let V3IndexValue::Child {
            reference,
            subtree_weight,
        } = &record.value
        else {
            return Err(PackedWireError::Invalid("IP06 leaf is not a child".into()));
        };
        let height = parent
            .height
            .checked_sub(1)
            .ok_or_else(|| PackedWireError::Invalid("IP06 leaf contains a child".into()))?;
        // The request keeps one expectation and the elected initializer
        // may clone it once. Admit both fence buffers before the first clone.
        let fence_bytes = (record.first_key.len() as u64)
            .checked_add(record.last_key.len() as u64)
            .and_then(|bytes| bytes.checked_mul(2))
            .ok_or_else(|| {
                PackedWireError::LimitExceeded("index expectation fences overflow".into())
            })?;
        let _expectation = self
            .budget
            .admit(&[(V3BudgetPool::Metadata, fence_bytes)])?;
        let expected = ChildExpectation {
            kind: parent.kind,
            height,
            first: record.first_key.clone(),
            last: record.last_key.clone(),
            weight: *subtree_weight,
        };
        self.observe_resolution(
            reference.kind,
            self.load_expected(reference, Some(expected)),
        )
        .await
    }

    pub async fn total_weight(&self, root: &V3ObjectRef) -> PackedResult<u64> {
        self.load(root).await?.total_weight()
    }

    /// Authenticate only the rightmost IP06 path. No namespace scan, caller
    /// supplied ceiling or uncharged returned locator is used for allocation.
    pub(crate) async fn maximum_inode(&self, root: &V3ObjectRef) -> PackedResult<Option<u64>> {
        if root.kind != V3ObjectKind::InodeIndex {
            return Err(PackedWireError::Invalid(
                "inode ceiling requires IP06 inode root".into(),
            ));
        }
        let _decode = self.budget.admit(&[
            (V3BudgetPool::Metadata, 32 * 1024),
            (V3BudgetPool::Control, 1024),
        ])?;
        self.observe_resolution(root.kind, async {
            let mut page = self.load(root).await?;
            for _ in 0..=MAX_HEIGHT {
                let Some(record) = page.records.last() else {
                    return Ok(None);
                };
                match &record.value {
                    V3IndexValue::Child { .. } => page = self.load_child(&page, record).await?,
                    V3IndexValue::Leaf(value) => {
                        if record.first_key != record.last_key || record.last_key.len() != 8 {
                            return Err(PackedWireError::Invalid(
                                "IP06 maximum inode key is not exact u64".into(),
                            ));
                        }
                        let inode =
                            u64::from_be_bytes(record.last_key.as_slice().try_into().map_err(
                                |_| PackedWireError::Invalid("IP06 inode key length".into()),
                            )?);
                        let location = super::V3InodeLocation::decode_value(value)?;
                        if location.hot.inode != inode {
                            return Err(PackedWireError::Invalid(
                                "IP06 maximum inode key/value disagree".into(),
                            ));
                        }
                        return Ok(Some(inode));
                    }
                }
            }
            Err(PackedWireError::LimitExceeded(
                "IP06 maximum inode route exceeds depth".into(),
            ))
        })
        .await
    }

    async fn rank_lower_bound(
        &self,
        mut page: Arc<V3Owned<V3IndexPage>>,
        key: &[u8],
    ) -> PackedResult<u64> {
        let mut rank = 0u64;
        for _ in 0..=MAX_HEIGHT {
            let selected = page.records.partition_point(|record| {
                if page.height == 0 {
                    record.first_key.as_slice() < key
                } else {
                    record.last_key.as_slice() < key
                }
            });
            for record in &page.records[..selected] {
                rank = rank
                    .checked_add(record.subtree_weight(page.kind)?)
                    .ok_or_else(|| {
                        PackedWireError::LimitExceeded("IP06 prefix rank overflows u64".into())
                    })?;
            }
            if page.height == 0 {
                return Ok(rank);
            }
            let Some(record) = page.records.get(selected) else {
                return Ok(rank);
            };
            if record.first_key.as_slice() >= key {
                return Ok(rank);
            }
            page = self.load_child(&page, record).await?;
        }
        Err(PackedWireError::LimitExceeded(
            "IP06 rank route exceeds maximum depth".into(),
        ))
    }

    /// Select a weighted ordinal within [lower, upper), then return bounded
    /// adjacent leaves. The first tuple carries the ordinal within that leaf
    /// record (a dentry ordinal for Groups); all following offsets are zero.
    /// A retained authenticated path resumes adjacent leaves without rescanning
    /// the directory prefix or retaining an unbounded cookie-to-page map.
    pub async fn scan_weighted_page(
        &self,
        root: &V3ObjectRef,
        lower: &[u8],
        upper: &[u8],
        offset: u64,
        limit: usize,
        expected_total: u64,
    ) -> PackedResult<V3IndexRows<(V3IndexRecordHandle, u64)>> {
        if limit > MAX_RECORDS {
            return Err(PackedWireError::LimitExceeded(
                "IP06 weighted scan bounds exceed budget".into(),
            ));
        }
        if limit == 0 {
            return V3IndexRows::new(&self.budget, 0);
        }
        let mut found = V3IndexRows::new(&self.budget, limit)?;
        let mut cursor = self
            .weighted_cursor(root, lower, upper, offset, expected_total)
            .await?;
        let mut owned_bytes = 0usize;
        while let Some((record, entry_offset)) = self.next_weighted(root, &mut cursor).await? {
            let V3IndexValue::Leaf(value) = &record.value else {
                return Err(PackedWireError::Invalid(
                    "IP06 weighted cursor returned a branch".into(),
                ));
            };
            let weight = record.first_key.len()
                + record.last_key.len()
                + value.len()
                + std::mem::size_of::<V3IndexRecord>();
            if !found.is_empty() && owned_bytes + weight > V3_INDEX_BODY_LIMIT {
                break;
            }
            owned_bytes += weight;
            found.rows.push((record, entry_offset));
            if found.len() == limit {
                break;
            }
        }
        Ok(found)
    }

    pub(crate) async fn weighted_cursor(
        &self,
        root: &V3ObjectRef,
        lower: &[u8],
        upper: &[u8],
        offset: u64,
        expected_total: u64,
    ) -> PackedResult<V3WeightedCursor> {
        if lower.is_empty()
            || lower.len() > MAX_KEY_BYTES
            || upper.len() > MAX_KEY_BYTES
            || lower >= upper
        {
            return Err(PackedWireError::LimitExceeded(
                "IP06 weighted cursor bounds exceed budget".into(),
            ));
        }
        let permit = self.budget.admit(&[
            (V3BudgetPool::Control, 8192),
            (V3BudgetPool::Metadata, (lower.len() + upper.len()) as u64),
        ])?;
        let mut cursor = V3WeightedCursor {
            root_digest: root.digest,
            lower: lower.to_vec(),
            upper: upper.to_vec(),
            stack: Vec::new(),
            record_offset: 0,
            _permit: permit,
        };
        let root_page = self.load(root).await?;
        if root_page.total_weight()? != expected_total {
            return Err(PackedWireError::Invalid(
                "IP06 root weight disagrees with manifest".into(),
            ));
        }
        let prefix_rank = self.rank_lower_bound(root_page.clone(), lower).await?;
        let mut remaining = prefix_rank.checked_add(offset).ok_or_else(|| {
            PackedWireError::LimitExceeded("IP06 directory ordinal overflows u64".into())
        })?;
        if remaining >= expected_total {
            return Ok(cursor);
        }
        let mut page = root_page;
        let mut stack = Vec::with_capacity(MAX_HEIGHT as usize + 1);
        loop {
            let mut selected = None;
            for (index, record) in page.records.iter().enumerate() {
                let weight = record.subtree_weight(page.kind)?;
                if remaining < weight {
                    selected = Some(index);
                    break;
                }
                remaining = remaining.checked_sub(weight).ok_or_else(|| {
                    PackedWireError::Invalid("IP06 select weight underflows".into())
                })?;
            }
            let index = selected.ok_or_else(|| {
                PackedWireError::Invalid("IP06 ordinal exceeds authenticated page weight".into())
            })?;
            if page.height == 0 {
                stack.push((page, index));
                break;
            }
            let child = self.load_child(&page, &page.records[index]).await?;
            stack.push((page, index + 1));
            if stack.len() > MAX_HEIGHT as usize {
                return Err(PackedWireError::LimitExceeded(
                    "IP06 select route exceeds maximum depth".into(),
                ));
            }
            page = child;
        }
        cursor.stack = stack;
        cursor.record_offset = remaining;
        Ok(cursor)
    }

    pub(crate) async fn next_weighted(
        &self,
        root: &V3ObjectRef,
        cursor: &mut V3WeightedCursor,
    ) -> PackedResult<Option<(V3IndexRecordHandle, u64)>> {
        if root.digest != cursor.root_digest {
            return Err(PackedWireError::Invalid(
                "IP06 weighted cursor generation changed".into(),
            ));
        }
        while let Some((page, index)) = cursor.stack.last_mut() {
            if *index >= page.records.len() {
                cursor.stack.pop();
                continue;
            }
            let ordinal = *index;
            let record = &page.records[ordinal];
            *index += 1;
            if record.first_key.as_slice() >= cursor.upper.as_slice() {
                cursor.stack.clear();
                return Ok(None);
            }
            match &record.value {
                V3IndexValue::Leaf(_) => {
                    if record.first_key.as_slice() < cursor.lower.as_slice() {
                        return Err(PackedWireError::Invalid(
                            "IP06 selected record precedes directory prefix".into(),
                        ));
                    }
                    let record = V3IndexRecordHandle {
                        page: page.clone(),
                        ordinal,
                    };
                    return Ok(Some((record, std::mem::take(&mut cursor.record_offset))));
                }
                V3IndexValue::Child { .. } => {
                    let child = self.load_child(page, record).await?;
                    if cursor.stack.len() > MAX_HEIGHT as usize {
                        return Err(PackedWireError::LimitExceeded(
                            "IP06 weighted scan exceeds maximum depth".into(),
                        ));
                    }
                    cursor.stack.push((child, 0));
                }
            }
        }
        Ok(None)
    }

    /// Bounded lexicographic pagination. `after` is the last returned first
    /// key. Retain at most one authenticated page per tree level while walking.
    pub async fn scan_page(
        &self,
        root: &V3ObjectRef,
        lower: &[u8],
        upper: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> PackedResult<V3IndexRows<V3IndexRecordHandle>> {
        self.scan_page_inner(root, lower, upper, after, limit, false)
            .await
    }

    /// Return leaves whose inclusive fences overlap [lower, upper). The
    /// cursor remains the last returned first key, including a leaf that
    /// starts before lower. This is required for reads starting inside data.
    pub async fn scan_overlaps_page(
        &self,
        root: &V3ObjectRef,
        lower: &[u8],
        upper: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> PackedResult<V3IndexRows<V3IndexRecordHandle>> {
        self.scan_page_inner(root, lower, upper, after, limit, true)
            .await
    }

    async fn scan_page_inner(
        &self,
        root: &V3ObjectRef,
        lower: &[u8],
        upper: &[u8],
        after: Option<&[u8]>,
        limit: usize,
        overlaps: bool,
    ) -> PackedResult<V3IndexRows<V3IndexRecordHandle>> {
        if lower.is_empty()
            || lower.len() > MAX_KEY_BYTES
            || upper.len() > MAX_KEY_BYTES
            || lower >= upper
            || after.is_some_and(|key| key.len() > MAX_KEY_BYTES)
            || limit > MAX_RECORDS
        {
            return Err(PackedWireError::LimitExceeded(
                "wire 005 index scan bounds exceed budget".into(),
            ));
        }
        if limit == 0 {
            return V3IndexRows::new(&self.budget, 0);
        }
        let _stack = self.budget.admit(&[(
            V3BudgetPool::Control,
            ((MAX_HEIGHT as usize + 1) * std::mem::size_of::<(Arc<V3Owned<V3IndexPage>>, usize)>())
                as u64,
        )])?;
        let mut found = V3IndexRows::new(&self.budget, limit)?;
        let page = self.load(root).await?;
        let mut stack = Vec::with_capacity(MAX_HEIGHT as usize + 1);
        stack.push((page, 0usize));
        let mut owned_bytes = 0usize;
        while let Some((page, index)) = stack.last_mut() {
            if *index >= page.records.len() {
                stack.pop();
                continue;
            }
            let ordinal = *index;
            let record = &page.records[ordinal];
            *index += 1;
            if record.last_key.as_slice() < lower
                || after.is_some_and(|key| record.last_key.as_slice() <= key)
            {
                continue;
            }
            if record.first_key.as_slice() >= upper {
                stack.pop();
                continue;
            }
            match &record.value {
                V3IndexValue::Leaf(value) => {
                    if (!overlaps && record.first_key.as_slice() < lower)
                        || after.is_some_and(|key| record.first_key.as_slice() <= key)
                    {
                        continue;
                    }
                    let weight = record.first_key.len()
                        + record.last_key.len()
                        + value.len()
                        + std::mem::size_of::<V3IndexRecord>();
                    if !found.is_empty() && owned_bytes + weight > V3_INDEX_BODY_LIMIT {
                        break;
                    }
                    owned_bytes += weight;
                    found.rows.push(V3IndexRecordHandle {
                        page: page.clone(),
                        ordinal,
                    });
                    if found.len() == limit {
                        break;
                    }
                }
                V3IndexValue::Child { .. } => {
                    let child = self.load_child(page, record).await?;
                    if stack.len() > MAX_HEIGHT as usize {
                        return Err(PackedWireError::LimitExceeded(
                            "wire 005 scan exceeds maximum depth".into(),
                        ));
                    }
                    stack.push((child, 0));
                }
            }
        }
        Ok(found)
    }

    pub fn resident_bytes(&self) -> u64 {
        // Managed cache-owner charge; process RSS is measured separately.
        self.cache_ownership.owned.load(Ordering::Acquire)
    }
    pub(crate) async fn close(&self) {
        let mut runtime = self.demand_pipeline.lock().await;
        runtime.closed = true;
        if let Some(pipeline) = &runtime.coordinator {
            pipeline.shutdown().await;
        }
        drop(runtime.coordinator.take());
        self.pages.invalidate_all();
        self.pages.run_pending_tasks().await;
    }
}

#[cfg(test)]
#[path = "index_retention_tests.rs"]
mod cache_retention_tests;

#[cfg(test)]
#[path = "index_request_admission_tests.rs"]
mod request_admission_tests;

#[cfg(test)]
#[path = "index_result_contract_tests.rs"]
mod result_contract_tests;

#[cfg(test)]
#[path = "index_owned_lifecycle_tests.rs"]
mod owned_lifecycle_tests;

#[cfg(test)]
#[path = "index_runtime_close_tests.rs"]
mod runtime_close_tests;

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn budget_owned_page_survives_cache_eviction_until_last_cursor_owner_leaves() {
        let temp = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(crate::cadapter::localfs::LocalFsBackend::new(temp.path()));
        let bytes = V3IndexPage {
            kind: V3ObjectKind::InodeIndex,
            height: 0,
            records: vec![V3IndexRecord {
                first_key: vec![1],
                last_key: vec![1],
                value: V3IndexValue::Leaf(vec![9; 8192]),
            }],
        }
        .encode()
        .unwrap();
        let reference =
            V3ObjectRef::from_bytes("budget-index".into(), V3ObjectKind::InodeIndex, &bytes)
                .unwrap();
        client.put_object(&reference.key, &bytes).await.unwrap();
        let budget = V3MountBudget::defaults();
        let reader = V3IndexReader::with_budget(client, 1 << 20, budget.clone());
        let page = reader.load(&reference).await.unwrap();
        let held = budget.state().used[V3BudgetPool::Metadata as usize];
        assert!(held >= 8192);
        reader.pages.invalidate_all();
        reader.pages.run_pending_tasks().await;
        assert_eq!(budget.state().used[V3BudgetPool::Metadata as usize], held);
        assert_eq!(page.records[0].first_key, [1]);
        drop(page);
        reader.pages.run_pending_tasks().await;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
        for _ in 0..32 {
            if budget.state().used == [0; 8] {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "owned cache release exceeded 1 second"
            );
            reader.pages.run_pending_tasks().await;
            tokio::task::yield_now().await;
        }
        assert_eq!(budget.state().used, [0; 8]);
    }

    #[tokio::test]
    async fn budget_pressure_evicts_retained_pages_but_cannot_drop_cursor_pins() {
        let temp = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(crate::cadapter::localfs::LocalFsBackend::new(temp.path()));
        let mut limits = super::super::V3BudgetLimits::default();
        limits.bytes[V3BudgetPool::Metadata as usize] = 2 << 20;
        let budget = V3MountBudget::new(limits).unwrap();
        let reader = V3IndexReader::with_budget(client.clone(), 1 << 20, budget.clone());
        let mut references = Vec::new();
        for i in 1..=2 {
            let bytes = leaf(i, i).encode().unwrap();
            let reference =
                V3ObjectRef::from_bytes(format!("pressure-{i}"), V3ObjectKind::InodeIndex, &bytes)
                    .unwrap();
            client.put_object(&reference.key, &bytes).await.unwrap();
            references.push(reference);
        }
        let pin = reader.load(&references[0]).await.unwrap();
        assert!(matches!(
            reader.load(&references[1]).await,
            Err(PackedWireError::LimitExceeded(_))
        ));
        assert_eq!(pin.records[0].first_key, [1]);
        drop(pin);
        let second = reader.load(&references[1]).await.unwrap();
        assert_eq!(second.records[0].first_key, [2]);
        drop(second);
        reader.pages.run_pending_tasks().await;
        reader.pages.invalidate_all();
        reader.pages.run_pending_tasks().await;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
        for _ in 0..32 {
            if budget.state().used == [0; 8] {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "owned cache release exceeded 1 second"
            );
            reader.pages.run_pending_tasks().await;
            tokio::task::yield_now().await;
        }
        assert_eq!(budget.state().used, [0; 8]);
        assert!(budget.state().rejections >= 1);
    }

    fn leaf(first: u8, last: u8) -> V3IndexPage {
        V3IndexPage {
            kind: V3ObjectKind::InodeIndex,
            height: 0,
            records: (first..=last)
                .map(|i| V3IndexRecord {
                    first_key: vec![i],
                    last_key: vec![i],
                    value: V3IndexValue::Leaf(vec![i + 10]),
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn overlap_scan_keeps_mid_extent_leaf_and_stable_pagination() {
        let temp = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(crate::cadapter::localfs::LocalFsBackend::new(temp.path()));
        let mut builder = super::super::V3IndexBuilder::new(
            client.clone(),
            V3ObjectKind::LargeIndex,
            "index".into(),
            2,
            16 * 1024,
        )
        .unwrap();
        for start in [1, 11, 21, 31, 41] {
            builder
                .push(V3IndexRecord {
                    first_key: vec![start],
                    last_key: vec![start + 8],
                    value: V3IndexValue::Leaf(vec![start]),
                })
                .await
                .unwrap();
        }
        let root = builder.finish().await.unwrap();
        let reader = V3IndexReader::new(client, 0);
        assert_eq!(
            reader
                .scan_page(&root, &[15], &[38], None, 10)
                .await
                .unwrap()
                .iter()
                .map(|r| r.first_key[0])
                .collect::<Vec<_>>(),
            [21, 31]
        );
        let first = reader
            .scan_overlaps_page(&root, &[15], &[38], None, 1)
            .await
            .unwrap();
        assert_eq!(first[0].first_key, [11]);
        let second = reader
            .scan_overlaps_page(&root, &[15], &[38], Some(&first[0].first_key), 1)
            .await
            .unwrap();
        assert_eq!(second[0].first_key, [21]);
        let third = reader
            .scan_overlaps_page(&root, &[15], &[38], Some(&second[0].first_key), 1)
            .await
            .unwrap();
        assert_eq!(third[0].first_key, [31]);
        assert!(
            reader
                .scan_overlaps_page(&root, &[15], &[38], Some(&third[0].first_key), 1)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn index_roundtrip_and_invalid_fence_rejection() {
        let page = leaf(1, 3);
        let bytes = page.encode().unwrap();
        let reference = V3ObjectRef::from_bytes("index".into(), page.kind, &bytes).unwrap();
        assert_eq!(V3IndexPage::decode(&reference, &bytes).unwrap(), page);
        let mut overlap = page.clone();
        overlap.records[1].first_key = vec![1];
        assert!(overlap.encode().is_err());
        let mut oversized = page.clone();
        oversized.records[0].value = V3IndexValue::Leaf(vec![0; MAX_VALUE_BYTES + 1]);
        assert!(oversized.encode().is_err());
        let empty = V3IndexPage {
            kind: page.kind,
            height: 0,
            records: vec![],
        };
        assert!(empty.encode().is_ok());
    }

    #[tokio::test]
    async fn index_scan_pages_continue_without_fetching_unrelated_subtrees() {
        use crate::cadapter::localfs::LocalFsBackend;
        let dir = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(dir.path()));
        let mut builder = super::super::V3IndexBuilder::new(
            client.clone(),
            V3ObjectKind::InodeIndex,
            "scan".into(),
            2,
            16384,
        )
        .unwrap();
        for i in 1u8..=17 {
            builder
                .push(V3IndexRecord {
                    first_key: vec![i],
                    last_key: vec![i],
                    value: V3IndexValue::Leaf(vec![i]),
                })
                .await
                .unwrap();
        }
        let root = builder.finish().await.unwrap();
        let reader = V3IndexReader::new(client, 0);
        let mut after = None;
        let mut found = Vec::new();
        loop {
            let rows = reader
                .scan_page(&root, &[4], &[12], after.as_deref(), 3)
                .await
                .unwrap();
            if rows.is_empty() {
                break;
            }
            assert!(rows.len() <= 3);
            after = Some(rows.last().unwrap().first_key.clone());
            found.extend(rows.iter().map(|row| row.first_key[0]));
        }
        assert_eq!(found, (4u8..12).collect::<Vec<_>>());
        assert_eq!(reader.resident_bytes(), 0);
        assert!(
            reader
                .scan_page(&root, &[4], &[12], None, 0)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(reader.scan_page(&root, &[12], &[4], None, 3).await.is_err());
    }

    #[tokio::test]
    async fn root_routes_only_selected_authenticated_child_and_rejects_fence_lies() {
        use crate::cadapter::localfs::LocalFsBackend;
        let temp = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(temp.path()));
        let mut records = Vec::new();
        for (i, page) in [leaf(1, 2), leaf(3, 4)].into_iter().enumerate() {
            let bytes = page.encode().unwrap();
            let reference =
                V3ObjectRef::from_bytes(format!("child/{i}"), page.kind, &bytes).unwrap();
            if i == 1 {
                client.put_object(&reference.key, &bytes).await.unwrap();
            }
            let (first, last) = page.bounds().unwrap();
            records.push(V3IndexRecord {
                first_key: first.to_vec(),
                last_key: last.to_vec(),
                value: V3IndexValue::Child {
                    reference,
                    subtree_weight: page.total_weight().unwrap(),
                },
            });
        }
        let mut root = V3IndexPage {
            kind: V3ObjectKind::InodeIndex,
            height: 1,
            records,
        };
        let bytes = root.encode().unwrap();
        let reference = V3ObjectRef::from_bytes("root".into(), root.kind, &bytes).unwrap();
        client.put_object(&reference.key, &bytes).await.unwrap();
        let reader = V3IndexReader::new(client.clone(), 0);
        assert_eq!(
            reader
                .lookup(&reference, &[4])
                .await
                .unwrap()
                .map(|value| value.as_ref().to_vec()),
            Some(vec![14])
        );
        assert_eq!(
            reader
                .lookup(&reference, &[5])
                .await
                .unwrap()
                .map(|value| value.as_ref().to_vec()),
            None
        );
        assert_eq!(reader.resident_bytes(), 0);
        root.records[1].last_key = vec![5];
        let bad = root.encode().unwrap();
        let bad_ref = V3ObjectRef::from_bytes("bad-root".into(), root.kind, &bad).unwrap();
        client.put_object(&bad_ref.key, &bad).await.unwrap();
        assert!(reader.lookup(&bad_ref, &[4]).await.is_err());

        let mut bad_weight = root.clone();
        bad_weight.records[1].last_key = vec![4];
        let V3IndexValue::Child { subtree_weight, .. } = &mut bad_weight.records[1].value else {
            unreachable!()
        };
        *subtree_weight += 1;
        let bytes = bad_weight.encode().unwrap();
        let reference = V3ObjectRef::from_bytes("bad-weight".into(), root.kind, &bytes).unwrap();
        client.put_object(&reference.key, &bytes).await.unwrap();
        assert!(reader.lookup(&reference, &[4]).await.is_err());
        let V3IndexValue::Child { subtree_weight, .. } = &mut bad_weight.records[1].value else {
            unreachable!()
        };
        *subtree_weight = 0;
        assert!(bad_weight.encode().is_err());
        let mut overflow = root;
        for (record, weight) in overflow.records.iter_mut().zip([u64::MAX, 1]) {
            let V3IndexValue::Child { subtree_weight, .. } = &mut record.value else {
                unreachable!()
            };
            *subtree_weight = weight;
        }
        assert!(overflow.encode().is_err());
    }

    #[test]
    fn ip06_explicitly_rejects_authenticated_ip05_payload() {
        let page = leaf(1, 2);
        let bytes = page.encode().unwrap();
        let reference = V3ObjectRef::from_bytes("new".into(), page.kind, &bytes).unwrap();
        let mut body = reference
            .verify(&bytes, V3_INDEX_BODY_LIMIT)
            .unwrap()
            .to_vec();
        body[..4].copy_from_slice(b"IP05");
        let bytes = encode_v3_object(page.kind, &body, V3_INDEX_BODY_LIMIT).unwrap();
        let reference = V3ObjectRef::from_bytes("old".into(), page.kind, &bytes).unwrap();
        assert!(matches!(
            V3IndexPage::decode(&reference, &bytes),
            Err(PackedWireError::UnsupportedFormat(_))
        ));
    }

    #[tokio::test]
    async fn counted_scan_reconstructs_evicted_cursor_and_rejects_rank_overflow_or_wrong_root_total()
     {
        let temp = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(crate::cadapter::localfs::LocalFsBackend::new(temp.path()));
        let mut builder = super::super::V3IndexBuilder::new(
            client.clone(),
            V3ObjectKind::InodeIndex,
            "counted".into(),
            2,
            16 * 1024,
        )
        .unwrap();
        for value in 1u8..=37 {
            builder
                .push(V3IndexRecord {
                    first_key: vec![value],
                    last_key: vec![value],
                    value: V3IndexValue::Leaf(vec![value]),
                })
                .await
                .unwrap();
        }
        let root = builder.finish().await.unwrap();
        for offset in [0, 10, 33, 36, 37, 500] {
            let reader = V3IndexReader::new(client.clone(), 0);
            let found = reader
                .scan_weighted_page(&root, &[1], &[38], offset, 3, 37)
                .await
                .unwrap();
            let expected: Vec<u8> = (1u8..=37).skip(offset as usize).take(3).collect();
            assert_eq!(
                found
                    .iter()
                    .map(|(row, within)| {
                        assert_eq!(*within, 0);
                        row.first_key[0]
                    })
                    .collect::<Vec<_>>(),
                expected
            );
            assert_eq!(reader.resident_bytes(), 0);
        }
        let reader = V3IndexReader::new(client, 0);
        assert!(
            reader
                .scan_weighted_page(&root, &[1], &[38], 0, 1, 38)
                .await
                .is_err()
        );
        assert!(
            reader
                .scan_weighted_page(&root, &[2], &[38], u64::MAX, 1, 37)
                .await
                .is_err()
        );
        let mut cursor = reader
            .weighted_cursor(&root, &[1], &[38], 0, 37)
            .await
            .unwrap();
        let mut changed = root.clone();
        changed.digest[0] ^= 1;
        assert!(reader.next_weighted(&changed, &mut cursor).await.is_err());
    }
}
