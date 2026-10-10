use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::chunk::cache::ChunksCache;
use crate::chunk::read_plan::{ReadGeneration, ReadSource, UnifiedReadSourceFetcher};
use crate::chunk::singleflight::SingleFlight;

use super::group::{
    PackedFrameDescriptor, frame_directory_body_len, frame_table_body_offset,
    group_container_counts, parse_frame_descriptor_range,
};
use super::metrics::PackedRuntimeMetrics;
use super::wire::{
    PACKED_FOOTER_LEN, PACKED_HEADER_LEN, PackedHeader, PackedObjectKind, PackedResult,
    PackedWireError,
};

pub const MAX_PACKED_STREAM_RANGE_BYTES: u64 = 8 * 1024 * 1024 + 64;
const PACKED_FRAME_READ_WINDOW_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct RangeFlightKey {
    offset: u64,
    length: u64,
}

/// One byte budget for every container opened by a packed mount. Container
/// identity is part of the key so equal offsets in different objects cannot
/// alias each other.
pub(crate) struct PackedWindowCache {
    cache: moka::future::Cache<(String, u64, u64), Bytes>,
}

impl PackedWindowCache {
    pub(crate) fn new(max_bytes: u64) -> Self {
        Self {
            cache: moka::future::Cache::builder()
                .max_capacity(max_bytes)
                .weigher(|_key: &(String, u64, u64), value: &Bytes| {
                    value.len().min(u32::MAX as usize) as u32
                })
                .build(),
        }
    }

    pub(crate) fn entry_count(&self) -> u64 {
        self.cache.entry_count()
    }

    pub(crate) fn weighted_size(&self) -> u64 {
        self.cache.weighted_size()
    }

    #[cfg(test)]
    pub(crate) async fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks().await;
    }
}

/// Counters for the optional in-process group-window cache.  The cache starts
/// empty for every mount and is never persisted, so these counters describe
/// request coalescing during the current read session rather than warm-cache
/// reuse across benchmark runs.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PackedWindowCacheStats {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub remote_fetches: u64,
}

/// Consume exactly one bounded range from an object backend without retaining the
/// complete range. The consumer receives the relative offset of each chunk and
/// owns any bytes it needs to retain.
async fn consume_exact_range<B, F>(
    client: &ObjectClient<B>,
    key: &str,
    offset: u64,
    length: u64,
    mut consume: F,
) -> PackedResult<()>
where
    B: ObjectBackend + Clone,
    F: FnMut(u64, Bytes) -> PackedResult<()>,
{
    if length > MAX_PACKED_STREAM_RANGE_BYTES {
        return Err(PackedWireError::LimitExceeded(
            "packed stream range exceeds 8 MiB budget".into(),
        ));
    }
    offset
        .checked_add(length)
        .ok_or_else(|| PackedWireError::LimitExceeded("packed stream range overflows".into()))?;
    let mut stream = client
        .get_object_range_stream(key, offset, length)
        .await
        .map_err(|error| PackedWireError::Backend(error.to_string()))?;
    let mut consumed = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| PackedWireError::Backend(error.to_string()))?;
        let chunk_len = u64::try_from(chunk.len()).map_err(|_| {
            PackedWireError::LimitExceeded("packed stream chunk exceeds u64".into())
        })?;
        if chunk_len > length - consumed {
            return Err(PackedWireError::Invalid(
                "packed stream returned more bytes than requested".into(),
            ));
        }
        consume(consumed, chunk)?;
        consumed += chunk_len;
    }
    if consumed != length {
        return Err(PackedWireError::Truncated {
            what: "packed streamed range",
            need: usize::try_from(length).unwrap_or(usize::MAX),
            have: usize::try_from(consumed).unwrap_or(usize::MAX),
        });
    }
    Ok(())
}

/// Consume exactly one bounded range from an object backend. The backend may
/// yield any chunk sizes; a short response or a chunk that crosses the
/// requested bound is rejected before callers can treat the bytes as valid.
pub async fn read_exact_range<B: ObjectBackend + Clone>(
    client: &ObjectClient<B>,
    key: &str,
    offset: u64,
    length: u64,
) -> PackedResult<Vec<u8>> {
    if length > MAX_PACKED_STREAM_RANGE_BYTES {
        return Err(PackedWireError::LimitExceeded(
            "packed stream range exceeds 8 MiB budget".into(),
        ));
    }
    let expected = usize::try_from(length)
        .map_err(|_| PackedWireError::LimitExceeded("packed stream length exceeds usize".into()))?;
    let mut output = Vec::with_capacity(expected);
    consume_exact_range(client, key, offset, length, |_, chunk| {
        output.extend_from_slice(&chunk);
        Ok(())
    })
    .await?;
    Ok(output)
}

#[derive(Clone)]
pub struct RemotePackedObject<B: ObjectBackend + Clone> {
    client: ObjectClient<B>,
    object_key: String,
    object_len: u64,
    header: PackedHeader,
    /// Optional immutable container cache. Once enabled, the first payload
    /// read downloads the complete container into the local cache and later
    /// frame reads are local slices instead of OSS range requests.
    payload_cache: Option<Arc<ChunksCache>>,
    payload_cache_key: Option<String>,
    full_object_flight: Arc<SingleFlight<(), Vec<u8>>>,
    /// Coalesce overlapping FUSE reads for the same immutable frame without
    /// retaining payload bytes after the callers leave the flight.
    range_flight: Arc<SingleFlight<RangeFlightKey, Bytes>>,
    /// Optional bounded cache of aligned group windows.  This is deliberately
    /// independent from the persistent payload cache: it is an in-process
    /// request pipeline cache and is empty at mount startup.
    window_cache: Option<Arc<PackedWindowCache>>,
    window_cache_hits: Arc<AtomicU64>,
    window_cache_misses: Arc<AtomicU64>,
    window_remote_fetches: Arc<AtomicU64>,
    runtime_metrics: Arc<PackedRuntimeMetrics>,
}

impl<B: ObjectBackend + Clone> RemotePackedObject<B> {
    pub async fn open(
        client: &ObjectClient<B>,
        object_key: &str,
        object_len: u64,
        expected_kind: PackedObjectKind,
    ) -> PackedResult<Self> {
        if object_key.is_empty() || object_key.len() > 4096 || object_key.contains('\0') {
            return Err(PackedWireError::Invalid(
                "packed object key is empty, too long, or contains NUL".into(),
            ));
        }
        let header_bytes =
            read_exact_range(client, object_key, 0, PACKED_HEADER_LEN as u64).await?;
        let header = PackedHeader::parse(&header_bytes)?;
        if header.kind != expected_kind {
            return Err(PackedWireError::Invalid(
                "packed object kind does not match the requested reader".into(),
            ));
        }
        if header.object_len != object_len {
            return Err(PackedWireError::Invalid(
                "packed object length disagrees with manifest".into(),
            ));
        }
        Ok(Self {
            client: client.clone(),
            object_key: object_key.to_owned(),
            object_len,
            header,
            payload_cache: None,
            payload_cache_key: None,
            full_object_flight: Arc::new(SingleFlight::new()),
            range_flight: Arc::new(SingleFlight::new()),
            window_cache: None,
            window_cache_hits: Arc::new(AtomicU64::new(0)),
            window_cache_misses: Arc::new(AtomicU64::new(0)),
            window_remote_fetches: Arc::new(AtomicU64::new(0)),
            runtime_metrics: Arc::new(PackedRuntimeMetrics::default()),
        })
    }

    /// Enable a bounded in-process cache for aligned payload windows.  A zero
    /// budget leaves the strict per-range path unchanged.  The cache is
    /// intentionally not backed by the SSD cache and has no on-disk identity.
    pub fn with_frame_window_cache_bytes(mut self, max_bytes: u64) -> Self {
        if max_bytes == 0 {
            return self;
        }
        self.window_cache = Some(Arc::new(PackedWindowCache::new(max_bytes)));
        self
    }

    pub(crate) fn with_shared_frame_window_cache(
        mut self,
        cache: Option<Arc<PackedWindowCache>>,
    ) -> Self {
        self.window_cache = cache;
        self
    }

    pub(crate) fn with_runtime_metrics(mut self, metrics: Arc<PackedRuntimeMetrics>) -> Self {
        self.runtime_metrics = metrics;
        self
    }

    pub fn window_cache_stats(&self) -> PackedWindowCacheStats {
        PackedWindowCacheStats {
            cache_hits: self.window_cache_hits.load(Ordering::Relaxed),
            cache_misses: self.window_cache_misses.load(Ordering::Relaxed),
            remote_fetches: self.window_remote_fetches.load(Ordering::Relaxed),
        }
    }

    #[cfg(test)]
    pub(crate) async fn run_window_cache_maintenance(&self) {
        if let Some(cache) = &self.window_cache {
            cache.run_pending_tasks().await;
        }
    }

    #[cfg(test)]
    pub(crate) fn window_cache_weighted_size(&self) -> u64 {
        self.window_cache
            .as_ref()
            .map_or(0, |cache| cache.weighted_size())
    }

    /// Best-effort read-ahead for the next aligned payload window.
    ///
    /// This is deliberately separate from `read_frame`: strict cold reads
    /// must issue exactly the requested frame range. Callers that have
    /// explicitly enabled the ephemeral window cache may use this helper for
    /// a sequential scanner. The returned boolean tells the caller whether a
    /// window was actually scheduled/read; a disabled cache is a no-op.
    pub async fn prefetch_next_payload_window(
        &self,
        offset: u64,
        length: u64,
    ) -> PackedResult<bool> {
        let Some(_) = &self.window_cache else {
            return Ok(false);
        };
        if length == 0 || length > PACKED_FRAME_READ_WINDOW_BYTES {
            return Ok(false);
        }
        let (_, footer_offset) = self.validate_body_range(offset, length)?;
        let end = offset.checked_add(length).ok_or_else(|| {
            PackedWireError::LimitExceeded("packed prefetch range overflows".into())
        })?;
        let next_offset = end
            .saturating_add(PACKED_FRAME_READ_WINDOW_BYTES - 1)
            .checked_div(PACKED_FRAME_READ_WINDOW_BYTES)
            .and_then(|value| value.checked_mul(PACKED_FRAME_READ_WINDOW_BYTES))
            .unwrap_or(footer_offset)
            .max(PACKED_HEADER_LEN as u64);
        if next_offset >= footer_offset {
            return Ok(false);
        }
        let next_length = (footer_offset - next_offset).min(PACKED_FRAME_READ_WINDOW_BYTES);
        if next_length == 0 {
            return Ok(false);
        }
        self.read_windowed_payload_range(next_offset, next_length)
            .await
            .map(|_| true)
    }

    pub async fn open_with_payload_cache(
        client: &ObjectClient<B>,
        object_key: &str,
        object_len: u64,
        expected_kind: PackedObjectKind,
        payload_cache: Arc<ChunksCache>,
        payload_cache_key: String,
    ) -> PackedResult<Self> {
        if let Some(object) = payload_cache.get(&payload_cache_key).await {
            if object.len() as u64 == object_len {
                let envelope = super::wire::PackedEnvelope::parse(object.to_vec())?;
                if envelope.header.kind != expected_kind {
                    return Err(PackedWireError::Invalid(
                        "packed cached object kind does not match the requested reader".into(),
                    ));
                }
                return Ok(Self {
                    client: client.clone(),
                    object_key: object_key.to_owned(),
                    object_len,
                    header: envelope.header,
                    payload_cache: Some(payload_cache),
                    payload_cache_key: Some(payload_cache_key),
                    full_object_flight: Arc::new(SingleFlight::new()),
                    range_flight: Arc::new(SingleFlight::new()),
                    window_cache: None,
                    window_cache_hits: Arc::new(AtomicU64::new(0)),
                    window_cache_misses: Arc::new(AtomicU64::new(0)),
                    window_remote_fetches: Arc::new(AtomicU64::new(0)),
                    runtime_metrics: Arc::new(PackedRuntimeMetrics::default()),
                });
            }
            let _ = payload_cache.remove(&payload_cache_key).await;
        }
        let mut object = Self::open(client, object_key, object_len, expected_kind).await?;
        object.payload_cache = Some(payload_cache);
        object.payload_cache_key = Some(payload_cache_key);
        Ok(object)
    }

    pub fn header(&self) -> &PackedHeader {
        &self.header
    }

    pub fn object_len(&self) -> u64 {
        self.object_len
    }

    pub(crate) fn object_key(&self) -> &str {
        &self.object_key
    }

    pub async fn read_range(&self, offset: u64, length: u64) -> PackedResult<Vec<u8>> {
        let (end, _) = self.validate_body_range(offset, length)?;
        if let (Some(cache), Some(cache_key)) = (&self.payload_cache, &self.payload_cache_key)
            && let Some(object) = cache.get(cache_key).await
            && object.len() as u64 == self.object_len
        {
            return Ok(object[offset as usize..end as usize].to_vec());
        }
        read_exact_range(&self.client, &self.object_key, offset, length).await
    }

    fn validate_body_range(&self, offset: u64, length: u64) -> PackedResult<(u64, u64)> {
        let footer_offset = self
            .object_len
            .checked_sub(PACKED_FOOTER_LEN as u64)
            .ok_or_else(|| {
                PackedWireError::Invalid("packed object is shorter than footer".into())
            })?;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| PackedWireError::LimitExceeded("packed range overflows".into()))?;
        if offset < PACKED_HEADER_LEN as u64 || end > footer_offset {
            return Err(PackedWireError::Invalid(
                "packed range is outside the object body".into(),
            ));
        }
        Ok((end, footer_offset))
    }

    /// Read frame payload through the local whole-container cache. Metadata
    /// and frame-directory reads remain bounded ranges; the first actual frame
    /// read materializes this immutable container locally.
    pub async fn read_payload_range(&self, offset: u64, length: u64) -> PackedResult<Vec<u8>> {
        let (end, _) = self.validate_body_range(offset, length)?;
        let (Some(cache), Some(cache_key)) = (&self.payload_cache, &self.payload_cache_key) else {
            self.runtime_metrics.record_data_range(length);
            return read_exact_range(&self.client, &self.object_key, offset, length).await;
        };
        if !cache.has_read_capacity() || !cache.can_retain_read_bytes(self.object_len) {
            self.runtime_metrics.record_data_range(length);
            return read_exact_range(&self.client, &self.object_key, offset, length).await;
        }
        if let Some(object) = cache.get(cache_key).await
            && object.len() as u64 == self.object_len
        {
            self.runtime_metrics.record_data_cache_hit();
            return Ok(object[offset as usize..end as usize].to_vec());
        }

        let client = self.client.clone();
        let object_key = self.object_key.clone();
        let object_len = self.object_len;
        let expected_kind = self.header.kind;
        let (is_leader, result) = self
            .full_object_flight
            .execute_with_status((), || async move {
                let object = client
                    .get_object(&object_key)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("packed object is missing"))?;
                if object.len() as u64 != object_len {
                    anyhow::bail!(
                        "packed object length mismatch: expected {object_len}, got {}",
                        object.len()
                    );
                }
                let envelope = super::wire::PackedEnvelope::parse(object.clone())
                    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
                if envelope.header.kind != expected_kind {
                    anyhow::bail!("packed object kind changed while caching");
                }
                Ok::<_, anyhow::Error>(object)
            })
            .await;
        let object = result.map_err(|error| PackedWireError::Backend(error.to_string()))?;
        if is_leader {
            self.runtime_metrics.record_data_range(object_len);
            if let Err(error) = cache.insert(cache_key, object.as_ref()).await {
                tracing::warn!(error = ?error, "packed container local cache insert failed");
            }
        }
        Ok(object[offset as usize..end as usize].to_vec())
    }

    /// Read a payload range through an aligned group window when the bounded
    /// window cache is enabled.  A window is fetched at most once for the
    /// lifetime of this object, then serves adjacent frames from memory.  The
    /// returned bytes are still copied to the caller, so the cached value can
    /// be evicted without borrowing a range buffer.
    pub async fn read_windowed_payload_range(
        &self,
        offset: u64,
        length: u64,
    ) -> PackedResult<Vec<u8>> {
        let (end, footer_offset) = self.validate_body_range(offset, length)?;
        let Some(cache) = &self.window_cache else {
            self.runtime_metrics.record_data_range(length);
            return read_exact_range(&self.client, &self.object_key, offset, length).await;
        };
        if length == 0 {
            return Ok(Vec::new());
        }
        if length > PACKED_FRAME_READ_WINDOW_BYTES {
            self.runtime_metrics.record_data_range(length);
            return read_exact_range(&self.client, &self.object_key, offset, length).await;
        }
        let aligned = (offset / PACKED_FRAME_READ_WINDOW_BYTES)
            .saturating_mul(PACKED_FRAME_READ_WINDOW_BYTES);
        let window_offset = aligned.max(PACKED_HEADER_LEN as u64);
        let window_end = end
            .saturating_add(PACKED_FRAME_READ_WINDOW_BYTES - 1)
            .checked_div(PACKED_FRAME_READ_WINDOW_BYTES)
            .and_then(|value| value.checked_mul(PACKED_FRAME_READ_WINDOW_BYTES))
            .unwrap_or(footer_offset)
            .min(footer_offset);
        if window_end <= window_offset || end > window_end {
            self.runtime_metrics.record_data_range(length);
            return read_exact_range(&self.client, &self.object_key, offset, length).await;
        }
        let key = RangeFlightKey {
            offset: window_offset,
            length: window_end - window_offset,
        };
        let shared_key = (self.object_key.clone(), key.offset, key.length);
        if let Some(window) = cache.cache.get(&shared_key).await {
            self.window_cache_hits.fetch_add(1, Ordering::Relaxed);
            self.runtime_metrics.record_window_hit();
            let start = usize::try_from(offset - window_offset).map_err(|_| {
                PackedWireError::LimitExceeded("packed window offset exceeds usize".into())
            })?;
            let end = start
                .checked_add(usize::try_from(length).map_err(|_| {
                    PackedWireError::LimitExceeded("packed window length exceeds usize".into())
                })?)
                .ok_or_else(|| {
                    PackedWireError::LimitExceeded("packed window slice overflows".into())
                })?;
            if end > window.len() {
                return Err(PackedWireError::Truncated {
                    what: "packed cached frame window",
                    need: end,
                    have: window.len(),
                });
            }
            return Ok(window[start..end].to_vec());
        }
        self.window_cache_misses.fetch_add(1, Ordering::Relaxed);
        self.runtime_metrics.record_window_miss();
        let client = self.client.clone();
        let object_key = self.object_key.clone();
        let cache_key = key.clone();
        let window_fetch_bytes = key.length;
        let (is_leader, result) = self
            .range_flight
            .execute_with_status(key, || async move {
                let bytes =
                    read_exact_range(&client, &object_key, cache_key.offset, cache_key.length)
                        .await?;
                Ok::<_, PackedWireError>(Bytes::from(bytes))
            })
            .await;
        let window = result.map_err(|error| PackedWireError::Backend(error.to_string()))?;
        if is_leader {
            self.window_remote_fetches.fetch_add(1, Ordering::Relaxed);
            self.runtime_metrics.record_window_fetch(window_fetch_bytes);
            cache.cache.insert(shared_key, (*window).clone()).await;
        }
        let start = usize::try_from(offset - window_offset).map_err(|_| {
            PackedWireError::LimitExceeded("packed window offset exceeds usize".into())
        })?;
        let end = start
            .checked_add(usize::try_from(length).map_err(|_| {
                PackedWireError::LimitExceeded("packed window length exceeds usize".into())
            })?)
            .ok_or_else(|| {
                PackedWireError::LimitExceeded("packed window slice overflows".into())
            })?;
        if end > window.len() {
            return Err(PackedWireError::Truncated {
                what: "packed frame window",
                need: end,
                have: window.len(),
            });
        }
        Ok(window[start..end].to_vec())
    }

    pub async fn read_frame(&self, frame: &PackedFrameDescriptor) -> PackedResult<Bytes> {
        if self.window_cache.is_some() {
            let bytes = self
                .read_windowed_payload_range(frame.object_offset, u64::from(frame.stored_len))
                .await?;
            let digest: [u8; 16] = Sha256::digest(&bytes)[..16]
                .try_into()
                .expect("sha256 prefix has 16 bytes");
            if digest != frame.frame_digest {
                return Err(PackedWireError::Invalid(
                    "packed frame digest mismatch".into(),
                ));
            }
            return Ok(Bytes::from(bytes));
        }
        // Strict reads must request exactly the frame bytes.  The aligned
        // window path is opt-in above; retaining a hidden 4 MiB overscan here
        // would make callers that do not use the coordinator look like a
        // warm/prefetched workload and inflate OSS egress for small files.
        let offset = frame.object_offset;
        let length = u64::from(frame.stored_len);
        let key = RangeFlightKey { offset, length };
        let bytes = self
            .range_flight
            .execute(key, || async move {
                self.read_payload_range(offset, length)
                    .await
                    .map(Bytes::from)
                    .map_err(|error| anyhow::anyhow!(error.to_string()))
            })
            .await
            .map_err(|error| PackedWireError::Backend(error.to_string()))?;
        let frame_offset = usize::try_from(frame.object_offset - offset).map_err(|_| {
            PackedWireError::LimitExceeded("packed frame offset exceeds usize".into())
        })?;
        let frame_end = frame_offset
            .checked_add(frame.stored_len as usize)
            .ok_or_else(|| PackedWireError::LimitExceeded("packed frame slice overflows".into()))?;
        if frame_end > bytes.len() {
            return Err(PackedWireError::Truncated {
                what: "packed frame window",
                need: frame_end,
                have: bytes.len(),
            });
        }
        let bytes = bytes.slice(frame_offset..frame_end);
        let digest: [u8; 16] = Sha256::digest(&bytes)[..16]
            .try_into()
            .expect("sha256 prefix has 16 bytes");
        if digest != frame.frame_digest {
            return Err(PackedWireError::Invalid(
                "packed frame digest mismatch".into(),
            ));
        }
        Ok(bytes)
    }

    /// Stream one bounded physical range and retain only the requested frame
    /// payloads. Strict-cold callers use this path so coalesced gap bytes never
    /// become a second full-range allocation.
    pub(crate) async fn read_frames_in_range(
        &self,
        offset: u64,
        length: u64,
        frames: &[PackedFrameDescriptor],
    ) -> PackedResult<BTreeMap<u32, Bytes>> {
        let (end, _) = self.validate_body_range(offset, length)?;
        if frames.is_empty() {
            return Ok(BTreeMap::new());
        }
        let mut descriptors = frames.to_vec();
        descriptors.sort_by_key(|frame| frame.object_offset);
        for pair in descriptors.windows(2) {
            if pair[0].frame_ordinal == pair[1].frame_ordinal {
                return Err(PackedWireError::Invalid(
                    "duplicate frame ordinal in streamed range".into(),
                ));
            }
        }
        for frame in &descriptors {
            let frame_end = frame
                .object_offset
                .checked_add(u64::from(frame.stored_len))
                .ok_or_else(|| PackedWireError::LimitExceeded("frame range overflows".into()))?;
            if frame.object_offset < offset || frame_end > end {
                return Err(PackedWireError::Invalid(
                    "requested frame lies outside streamed range".into(),
                ));
            }
        }

        if self.window_cache.is_some() && length <= PACKED_FRAME_READ_WINDOW_BYTES {
            let bytes = self.read_windowed_payload_range(offset, length).await?;
            return decode_frame_range(offset, &bytes, &descriptors);
        }

        let mut buffers: Vec<(PackedFrameDescriptor, BytesMut)> = descriptors
            .iter()
            .map(|frame| {
                (
                    frame.clone(),
                    BytesMut::with_capacity(frame.stored_len as usize),
                )
            })
            .collect();
        self.runtime_metrics.record_data_range(length);
        consume_exact_range(
            &self.client,
            &self.object_key,
            offset,
            length,
            |relative, chunk| {
                let chunk_start = offset.checked_add(relative).ok_or_else(|| {
                    PackedWireError::LimitExceeded("stream offset overflows".into())
                })?;
                let chunk_end = chunk_start.checked_add(chunk.len() as u64).ok_or_else(|| {
                    PackedWireError::LimitExceeded("stream chunk overflows".into())
                })?;
                for (frame, output) in &mut buffers {
                    let frame_end = frame
                        .object_offset
                        .checked_add(u64::from(frame.stored_len))
                        .ok_or_else(|| {
                            PackedWireError::LimitExceeded("frame range overflows".into())
                        })?;
                    let copy_start = chunk_start.max(frame.object_offset);
                    let copy_end = chunk_end.min(frame_end);
                    if copy_start < copy_end {
                        let chunk_offset =
                            usize::try_from(copy_start - chunk_start).map_err(|_| {
                                PackedWireError::LimitExceeded(
                                    "frame chunk offset exceeds usize".into(),
                                )
                            })?;
                        let copy_len = usize::try_from(copy_end - copy_start).map_err(|_| {
                            PackedWireError::LimitExceeded(
                                "frame chunk length exceeds usize".into(),
                            )
                        })?;
                        output.extend_from_slice(&chunk[chunk_offset..chunk_offset + copy_len]);
                    }
                }
                Ok(())
            },
        )
        .await?;

        let mut output = BTreeMap::new();
        for (frame, bytes) in buffers {
            if bytes.len() != frame.stored_len as usize {
                return Err(PackedWireError::Truncated {
                    what: "streamed packed frame",
                    need: frame.stored_len as usize,
                    have: bytes.len(),
                });
            }
            let digest: [u8; 16] = Sha256::digest(&bytes)[..16]
                .try_into()
                .expect("sha256 prefix has 16 bytes");
            if digest != frame.frame_digest {
                return Err(PackedWireError::Invalid(
                    "packed frame digest mismatch".into(),
                ));
            }
            output.insert(frame.frame_ordinal, bytes.freeze());
        }
        Ok(output)
    }

    /// request fetches only the 24-byte body prefix; descriptor records are
    /// then fetched in chunks no larger than the streaming range budget.  No
    /// metadata, frame-list or frame payload bytes are touched.
    pub async fn read_frame_directory(&self) -> PackedResult<Vec<PackedFrameDescriptor>> {
        let prefix = self.read_range(PACKED_HEADER_LEN as u64, 24).await?;
        let required_len = frame_directory_body_len(&prefix)?;
        if required_len > usize::try_from(self.header.body_stored_len).unwrap_or(usize::MAX) {
            return Err(PackedWireError::Truncated {
                what: "packed group frame directory",
                need: required_len,
                have: usize::try_from(self.header.body_stored_len).unwrap_or(usize::MAX),
            });
        }
        let (_, frame_count) = group_container_counts(&prefix)?;
        let table_offset = frame_table_body_offset(&prefix)?;
        let records_per_range = usize::try_from(MAX_PACKED_STREAM_RANGE_BYTES)
            .unwrap_or(usize::MAX)
            .saturating_div(super::group::FRAME_RECORD_LEN);
        if records_per_range == 0 {
            return Err(PackedWireError::LimitExceeded(
                "packed frame directory range budget is too small".into(),
            ));
        }
        let mut frames = Vec::with_capacity(frame_count as usize);
        let mut first = 0u32;
        while first < frame_count {
            let count = usize::try_from(frame_count - first)
                .unwrap_or(usize::MAX)
                .min(records_per_range);
            let length = count
                .checked_mul(super::group::FRAME_RECORD_LEN)
                .ok_or_else(|| {
                    PackedWireError::LimitExceeded("frame directory range overflows".into())
                })?;
            let body_offset = table_offset
                .checked_add(
                    usize::try_from(first)
                        .ok()
                        .and_then(|value| value.checked_mul(super::group::FRAME_RECORD_LEN))
                        .ok_or_else(|| {
                            PackedWireError::LimitExceeded(
                                "frame directory offset overflows".into(),
                            )
                        })?,
                )
                .ok_or_else(|| {
                    PackedWireError::LimitExceeded("frame directory offset overflows".into())
                })?;
            let object_offset = (PACKED_HEADER_LEN as u64)
                .checked_add(body_offset as u64)
                .ok_or_else(|| {
                    PackedWireError::LimitExceeded("frame directory object offset overflows".into())
                })?;
            let bytes = self.read_range(object_offset, length as u64).await?;
            frames.extend(parse_frame_descriptor_range(
                &bytes,
                self.object_len,
                first,
            )?);
            first = first
                .checked_add(u32::try_from(count).map_err(|_| {
                    PackedWireError::LimitExceeded("frame directory count exceeds u32".into())
                })?)
                .ok_or_else(|| {
                    PackedWireError::LimitExceeded("frame directory ordinal overflows".into())
                })?;
        }
        validate_descriptor_order(&frames)?;
        Ok(frames)
    }

    /// Read only the requested frame descriptors.  Ordinals are sorted and
    /// contiguous runs are merged, with every object-store range bounded by
    /// `MAX_PACKED_STREAM_RANGE_BYTES`.  This avoids loading a million-frame
    /// table when a single GroupMeta entry references only a few frames.
    pub async fn read_frame_descriptors(
        &self,
        ordinals: impl IntoIterator<Item = u32>,
    ) -> PackedResult<Vec<PackedFrameDescriptor>> {
        let mut ordinals: Vec<u32> = ordinals.into_iter().collect();
        if ordinals.is_empty() {
            return Ok(Vec::new());
        }
        ordinals.sort_unstable();
        ordinals.dedup();
        let prefix = self.read_range(PACKED_HEADER_LEN as u64, 24).await?;
        let (_, frame_count) = group_container_counts(&prefix)?;
        if ordinals.iter().any(|ordinal| *ordinal >= frame_count) {
            return Err(PackedWireError::Invalid(
                "requested frame ordinal exceeds container frame count".into(),
            ));
        }
        let table_offset = frame_table_body_offset(&prefix)?;
        let max_records = usize::try_from(MAX_PACKED_STREAM_RANGE_BYTES)
            .unwrap_or(usize::MAX)
            .saturating_div(super::group::FRAME_RECORD_LEN)
            .max(1);
        let mut output = Vec::with_capacity(ordinals.len());
        let mut index = 0usize;
        while index < ordinals.len() {
            let start = ordinals[index];
            let mut end = start.checked_add(1).ok_or_else(|| {
                PackedWireError::LimitExceeded("frame ordinal range overflows".into())
            })?;
            let mut next = index + 1;
            while next < ordinals.len()
                && ordinals[next] == end
                && usize::try_from(end - start).unwrap_or(usize::MAX) < max_records
            {
                end = end.checked_add(1).ok_or_else(|| {
                    PackedWireError::LimitExceeded("frame ordinal range overflows".into())
                })?;
                next += 1;
            }
            let count = usize::try_from(end - start).map_err(|_| {
                PackedWireError::LimitExceeded("frame descriptor count exceeds usize".into())
            })?;
            let body_offset = table_offset
                .checked_add(
                    usize::try_from(start)
                        .ok()
                        .and_then(|value| value.checked_mul(super::group::FRAME_RECORD_LEN))
                        .ok_or_else(|| {
                            PackedWireError::LimitExceeded(
                                "frame directory offset overflows".into(),
                            )
                        })?,
                )
                .ok_or_else(|| {
                    PackedWireError::LimitExceeded("frame directory offset overflows".into())
                })?;
            let object_offset = (PACKED_HEADER_LEN as u64)
                .checked_add(body_offset as u64)
                .ok_or_else(|| {
                    PackedWireError::LimitExceeded("frame directory object offset overflows".into())
                })?;
            let length = count
                .checked_mul(super::group::FRAME_RECORD_LEN)
                .ok_or_else(|| {
                    PackedWireError::LimitExceeded("frame directory range overflows".into())
                })?;
            let bytes = self.read_range(object_offset, length as u64).await?;
            output.extend(parse_frame_descriptor_range(
                &bytes,
                self.object_len,
                start,
            )?);
            index = next;
        }
        validate_descriptor_order(&output)?;
        Ok(output)
    }
}

fn validate_descriptor_order(frames: &[PackedFrameDescriptor]) -> PackedResult<()> {
    let mut previous = None;
    for frame in frames {
        let end = frame
            .object_offset
            .checked_add(u64::from(frame.stored_len))
            .ok_or_else(|| PackedWireError::LimitExceeded("frame range overflows".into()))?;
        if let Some(previous_end) = previous
            && frame.object_offset < previous_end
        {
            return Err(PackedWireError::Invalid(
                "frame payload ranges overlap or are not canonical".into(),
            ));
        }
        previous = Some(end);
    }
    Ok(())
}

fn decode_frame_range(
    offset: u64,
    bytes: &[u8],
    frames: &[PackedFrameDescriptor],
) -> PackedResult<BTreeMap<u32, Bytes>> {
    let mut output = BTreeMap::new();
    for frame in frames {
        let relative = frame
            .object_offset
            .checked_sub(offset)
            .ok_or_else(|| PackedWireError::Invalid("frame is outside range".into()))?;
        let start = usize::try_from(relative)
            .map_err(|_| PackedWireError::LimitExceeded("frame offset exceeds usize".into()))?;
        let end = start
            .checked_add(frame.stored_len as usize)
            .ok_or_else(|| PackedWireError::LimitExceeded("frame slice overflows".into()))?;
        if end > bytes.len() {
            return Err(PackedWireError::Truncated {
                what: "frame in streamed range",
                need: end,
                have: bytes.len(),
            });
        }
        let payload = &bytes[start..end];
        let digest: [u8; 16] = Sha256::digest(payload)[..16]
            .try_into()
            .expect("sha256 prefix has 16 bytes");
        if digest != frame.frame_digest {
            return Err(PackedWireError::Invalid(
                "packed frame digest mismatch".into(),
            ));
        }
        output.insert(frame.frame_ordinal, Bytes::copy_from_slice(payload));
    }
    Ok(output)
}

/// The map is populated from the pinned manifest; no metadata service is
/// consulted while a frame is being delivered.
#[derive(Clone)]
pub struct PackedFrameSourceFetcher<B: ObjectBackend + Clone> {
    objects: Arc<HashMap<u32, RemotePackedObject<B>>>,
    prefetched: Arc<HashMap<(u32, u32), Bytes>>,
    /// The immutable generation captured while resolving the unified plan.
    ///
    /// A frame source is often built after the plan has been prepared (for
    /// example, once the coordinator has decoded its frames).  Keeping the
    /// generation on the source lets the executor reject a plan assembled
    /// for another snapshot before it copies even inline bytes.  The plain
    /// constructor remains useful for isolated source tests; production
    /// readers bind it with [`Self::with_generation`].
    generation: Option<ReadGeneration>,
}

impl<B: ObjectBackend + Clone> PackedFrameSourceFetcher<B> {
    pub fn new(objects: HashMap<u32, RemotePackedObject<B>>) -> Self {
        Self {
            objects: Arc::new(objects),
            prefetched: Arc::new(HashMap::new()),
            generation: None,
        }
    }

    /// Bind this source to the generation used to resolve its plan.
    ///
    /// The binding is immutable for the lifetime of the fetcher.  A mismatch
    /// is reported as [`crate::chunk::read_plan::ReadViewChanged`], which is
    /// the only typed error that permits the caller to discard the complete
    /// output and resolve again.
    pub fn with_generation(mut self, generation: ReadGeneration) -> Self {
        self.generation = Some(generation);
        self
    }

    pub fn from_object(object: RemotePackedObject<B>, container_ordinal: u32) -> Self {
        let mut objects = HashMap::new();
        objects.insert(container_ordinal, object);
        Self::new(objects)
    }

    pub(crate) fn with_prefetched_frame_map(
        container_ordinal: u32,
        frames: BTreeMap<u32, Bytes>,
    ) -> Self {
        let prefetched = frames
            .into_iter()
            .map(|(frame_ordinal, bytes)| ((container_ordinal, frame_ordinal), bytes))
            .collect();
        Self {
            objects: Arc::new(HashMap::new()),
            prefetched: Arc::new(prefetched),
            generation: None,
        }
    }

    /// complete raw frame payloads for one read operation; it is intentionally
    /// short lived and is not a hidden cache.
    pub fn with_prefetched_frames(
        object: RemotePackedObject<B>,
        container_ordinal: u32,
        frames: BTreeMap<u32, Bytes>,
    ) -> Self {
        let mut fetcher = Self::from_object(object, container_ordinal);
        let prefetched = frames
            .into_iter()
            .map(|(frame_ordinal, bytes)| ((container_ordinal, frame_ordinal), bytes))
            .collect();
        fetcher.prefetched = Arc::new(prefetched);
        fetcher
    }
}

#[async_trait]
impl<B: ObjectBackend + Clone + 'static> UnifiedReadSourceFetcher for PackedFrameSourceFetcher<B> {
    async fn read_source(&self, source: &ReadSource, output: &mut [u8]) -> anyhow::Result<()> {
        match source {
            ReadSource::Hole => {
                output.fill(0);
                Ok(())
            }
            ReadSource::PackedFrame {
                container_ordinal,
                frame_ordinal,
                object_offset,
                stored_len,
                raw_offset,
                raw_len,
                size_class,
                codec,
                frame_digest,
                ..
            } => {
                if *codec != 0 || *stored_len != *raw_len {
                    anyhow::bail!("unsupported packed frame codec or lengths")
                }
                let frame = if let Some(frame) =
                    self.prefetched.get(&(*container_ordinal, *frame_ordinal))
                {
                    frame.clone()
                } else {
                    let object = self.objects.get(container_ordinal).ok_or_else(|| {
                        anyhow::anyhow!("packed frame container {container_ordinal} is not pinned")
                    })?;
                    let descriptor = PackedFrameDescriptor {
                        frame_ordinal: *frame_ordinal,
                        object_offset: *object_offset,
                        stored_len: *stored_len,
                        raw_len: *raw_len,
                        first_file_slot: 0,
                        last_file_slot: 0,
                        size_class: super::layout::SizeClass::from_u8(*size_class)
                            .map_err(|error| anyhow::anyhow!(error.to_string()))?,
                        codec: *codec,
                        frame_digest: *frame_digest,
                    };
                    object
                        .read_frame(&descriptor)
                        .await
                        .map_err(|error| anyhow::anyhow!("packed frame read failed: {error}"))?
                };
                let start = usize::try_from(*raw_offset)
                    .map_err(|_| anyhow::anyhow!("packed raw offset exceeds usize"))?;
                let end = start
                    .checked_add(output.len())
                    .ok_or_else(|| anyhow::anyhow!("packed output range overflows"))?;
                if end > frame.len() {
                    anyhow::bail!("packed source range exceeds frame raw length")
                }
                output.copy_from_slice(&frame[start..end]);
                Ok(())
            }
            ReadSource::PackedInline { data, raw_offset } => {
                let start = usize::try_from(*raw_offset)
                    .map_err(|_| anyhow::anyhow!("packed inline offset exceeds usize"))?;
                let end = start
                    .checked_add(output.len())
                    .ok_or_else(|| anyhow::anyhow!("packed inline range overflows"))?;
                if end > data.len() {
                    anyhow::bail!("packed inline range exceeds payload length");
                }
                output.copy_from_slice(&data[start..end]);
                Ok(())
            }
            ReadSource::UpperBlock { .. } | ReadSource::LegacySlice { .. } => {
                anyhow::bail!("packed frame fetcher cannot read mutable or legacy sources")
            }
        }
    }

    async fn ensure_generation(&self, generation: ReadGeneration) -> anyhow::Result<()> {
        if let Some(expected) = self.generation {
            if expected != generation {
                return Err(crate::chunk::read_plan::ReadViewChanged.into());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cadapter::client::{ObjectBackend, ObjectByteStream};
    use crate::cadapter::localfs::LocalFsBackend;
    use crate::chunk::cache::{ChunksCache, ChunksCacheConfig};
    use crate::workspace_overlay::packed_v3::{
        AccessProfile, GroupMeta, GroupMetaEntry, GroupMetaExtent, PackedFrameInput,
        PackedGroupContainer, PackedGroupInput, SizeClass,
    };
    use anyhow::Result;
    use async_trait::async_trait;
    use std::sync::Mutex;
    use tempfile::tempdir;

    #[derive(Clone)]
    struct ScriptedStreamBackend {
        chunks: Arc<Vec<Vec<u8>>>,
        error_after_chunks: Option<usize>,
    }

    #[async_trait]
    impl ObjectBackend for ScriptedStreamBackend {
        async fn put_object(&self, _key: &str, _data: &[u8]) -> Result<()> {
            anyhow::bail!("scripted stream backend is read-only")
        }

        async fn get_object(&self, _key: &str) -> Result<Option<Vec<u8>>> {
            Ok(Some(self.chunks.iter().flatten().copied().collect()))
        }

        async fn get_object_range(&self, _key: &str, offset: u64, buf: &mut [u8]) -> Result<usize> {
            let object: Vec<u8> = self.chunks.iter().flatten().copied().collect();
            let start = usize::try_from(offset).unwrap_or(usize::MAX);
            if start >= object.len() {
                return Ok(0);
            }
            let length = buf.len().min(object.len() - start);
            buf[..length].copy_from_slice(&object[start..start + length]);
            Ok(length)
        }

        async fn get_object_range_stream(
            &self,
            _key: &str,
            _offset: u64,
            _length: u64,
        ) -> Result<ObjectByteStream> {
            let mut items: Vec<Result<Bytes>> = self
                .chunks
                .iter()
                .cloned()
                .map(|chunk| Ok(Bytes::from(chunk)))
                .collect();
            if let Some(index) = self.error_after_chunks {
                items.truncate(index);
                items.push(Err(anyhow::anyhow!("injected stream failure")));
            }
            Ok(Box::pin(futures_util::stream::iter(items)))
        }

        async fn get_etag(&self, _key: &str) -> Result<String> {
            Ok(String::new())
        }

        async fn delete_object(&self, _key: &str) -> Result<()> {
            Ok(())
        }
    }

    #[derive(Clone)]
    struct RecordingBackend {
        inner: LocalFsBackend,
        ranges: Arc<Mutex<Vec<(u64, u64)>>>,
    }

    #[async_trait]
    impl ObjectBackend for RecordingBackend {
        async fn put_object(&self, key: &str, data: &[u8]) -> Result<()> {
            self.inner.put_object(key, data).await
        }

        async fn get_object(&self, key: &str) -> Result<Option<Vec<u8>>> {
            self.inner.get_object(key).await
        }

        async fn get_object_range(&self, key: &str, offset: u64, buf: &mut [u8]) -> Result<usize> {
            self.ranges
                .lock()
                .expect("range recorder mutex poisoned")
                .push((offset, buf.len() as u64));
            self.inner.get_object_range(key, offset, buf).await
        }

        async fn get_etag(&self, key: &str) -> Result<String> {
            self.inner.get_etag(key).await
        }

        async fn delete_object(&self, key: &str) -> Result<()> {
            self.inner.delete_object(key).await
        }
    }

    #[tokio::test]
    async fn remote_reader_consumes_a_bounded_stream_range() {
        let temp = tempdir().unwrap();
        let backend = LocalFsBackend::new(temp.path());
        let client = ObjectClient::new(backend.clone());
        let object = PackedGroupContainer::build(
            7,
            AccessProfile::RandomSmallFile,
            vec![PackedGroupInput {
                group_id: 1,
                parent_dir_key: [0; 32],
                metadata: GroupMeta::new(vec![GroupMetaEntry {
                    name: b"payload".to_vec(),
                    inode: 1,
                    kind: 1,
                    mode: 0o100644,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    nlink: 1,
                    atime_ns: 0,
                    mtime_ns: 0,
                    ctime_ns: 0,
                    size: 7,
                    flags: 0,
                    inline_data: Arc::from([]),
                    extents: vec![GroupMetaExtent {
                        file_offset: 0,
                        logical_len: 7,
                        frame_ordinal: 0,
                        raw_offset: 0,
                        raw_len: 7,
                    }],
                }])
                .unwrap()
                .encode()
                .unwrap(),
                frame_ordinals: vec![0],
                entry_count: 1,
                file_count: 1,
                layout_profile: AccessProfile::RandomSmallFile,
            }],
            vec![PackedFrameInput {
                raw: b"payload".to_vec(),
                size_class: SizeClass::Tiny,
                codec: 0,
                first_file_slot: 0,
                last_file_slot: 0,
            }],
        )
        .unwrap();
        client.put_object("group", &object).await.unwrap();
        let opened = PackedGroupContainer::open(object).unwrap();
        let remote = RemotePackedObject::open(
            &client,
            "group",
            opened.object_len(),
            PackedObjectKind::GroupContainer,
        )
        .await
        .unwrap();
        let frame = remote.read_frame(&opened.frames()[0]).await.unwrap();
        assert_eq!(frame.as_ref(), b"payload");
        let directory = remote.read_frame_directory().await.unwrap();
        assert_eq!(directory, opened.frames());
        let selected = remote.read_frame_descriptors([0]).await.unwrap();
        assert_eq!(selected, opened.frames());
    }

    #[tokio::test]
    async fn exact_range_consumes_multiple_chunks() {
        let client = ObjectClient::new(ScriptedStreamBackend {
            chunks: Arc::new(vec![b"ab".to_vec(), b"c".to_vec(), b"def".to_vec()]),
            error_after_chunks: None,
        });

        assert_eq!(
            read_exact_range(&client, "object", 0, 6).await.unwrap(),
            b"abcdef"
        );
    }

    #[tokio::test]
    async fn exact_range_rejects_an_interrupted_stream() {
        let client = ObjectClient::new(ScriptedStreamBackend {
            chunks: Arc::new(vec![b"ab".to_vec(), b"cd".to_vec()]),
            error_after_chunks: Some(1),
        });

        assert!(matches!(
            read_exact_range(&client, "object", 0, 4).await,
            Err(PackedWireError::Backend(_))
        ));
    }

    #[tokio::test]
    async fn exact_range_rejects_a_chunk_crossing_the_declared_bound() {
        let client = ObjectClient::new(ScriptedStreamBackend {
            chunks: Arc::new(vec![b"abcdefg".to_vec()]),
            error_after_chunks: None,
        });

        assert!(matches!(
            read_exact_range(&client, "object", 0, 6).await,
            Err(PackedWireError::Invalid(_))
        ));
    }

    #[tokio::test]
    async fn exact_range_rejects_trailing_chunks_after_exact_length() {
        let client = ObjectClient::new(ScriptedStreamBackend {
            chunks: Arc::new(vec![b"abcdef".to_vec(), b"g".to_vec()]),
            error_after_chunks: None,
        });

        assert!(matches!(
            read_exact_range(&client, "object", 0, 6).await,
            Err(PackedWireError::Invalid(_))
        ));
    }

    #[tokio::test]
    async fn exact_range_rejects_a_failure_after_exact_length() {
        let client = ObjectClient::new(ScriptedStreamBackend {
            chunks: Arc::new(vec![b"abcdef".to_vec()]),
            error_after_chunks: Some(1),
        });

        assert!(matches!(
            read_exact_range(&client, "object", 0, 6).await,
            Err(PackedWireError::Backend(_))
        ));
    }

    #[tokio::test]
    async fn exact_range_rejects_short_streams() {
        let client = ObjectClient::new(ScriptedStreamBackend {
            chunks: Arc::new(vec![b"abc".to_vec()]),
            error_after_chunks: None,
        });

        assert!(matches!(
            read_exact_range(&client, "object", 0, 6).await,
            Err(PackedWireError::Truncated { .. })
        ));
    }

    #[tokio::test]
    async fn streamed_frame_range_delivers_each_frame_without_a_range_buffer() {
        let temp = tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(temp.path()));
        let object = PackedGroupContainer::build(
            71,
            AccessProfile::SequentialSmallFile,
            vec![PackedGroupInput {
                group_id: 1,
                parent_dir_key: [0; 32],
                metadata: GroupMeta::new(Vec::new()).unwrap().encode().unwrap(),
                frame_ordinals: vec![0, 1],
                entry_count: 0,
                file_count: 0,
                layout_profile: AccessProfile::SequentialSmallFile,
            }],
            vec![
                PackedFrameInput {
                    raw: vec![1; 24 * 1024],
                    size_class: SizeClass::Tiny,
                    codec: 0,
                    first_file_slot: 0,
                    last_file_slot: 0,
                },
                PackedFrameInput {
                    raw: vec![2; 24 * 1024],
                    size_class: SizeClass::Tiny,
                    codec: 0,
                    first_file_slot: 1,
                    last_file_slot: 1,
                },
            ],
        )
        .unwrap();
        client.put_object("streamed-group", &object).await.unwrap();
        let opened = PackedGroupContainer::open(object).unwrap();
        let remote = RemotePackedObject::open(
            &client,
            "streamed-group",
            opened.object_len(),
            PackedObjectKind::GroupContainer,
        )
        .await
        .unwrap();
        let first = &opened.frames()[0];
        let second = &opened.frames()[1];
        let start = first.object_offset;
        let end = second.object_offset + u64::from(second.stored_len);

        let frames = remote
            .read_frames_in_range(start, end - start, &[first.clone(), second.clone()])
            .await
            .unwrap();

        assert_eq!(frames[&0].as_ref(), vec![1; 24 * 1024]);
        assert_eq!(frames[&1].as_ref(), vec![2; 24 * 1024]);
    }

    #[tokio::test]
    async fn strict_read_frame_does_not_overscan_small_payloads() {
        let temp = tempdir().unwrap();
        let ranges = Arc::new(Mutex::new(Vec::new()));
        let backend = RecordingBackend {
            inner: LocalFsBackend::new(temp.path()),
            ranges: Arc::clone(&ranges),
        };
        let client = ObjectClient::new(backend);
        let object = PackedGroupContainer::build(
            8,
            AccessProfile::RandomSmallFile,
            vec![PackedGroupInput {
                group_id: 1,
                parent_dir_key: [0; 32],
                metadata: GroupMeta::new(Vec::new()).unwrap().encode().unwrap(),
                frame_ordinals: vec![0],
                entry_count: 0,
                file_count: 0,
                layout_profile: AccessProfile::RandomSmallFile,
            }],
            vec![PackedFrameInput {
                raw: vec![7; 1024],
                size_class: SizeClass::Tiny,
                codec: 0,
                first_file_slot: 0,
                last_file_slot: 0,
            }],
        )
        .unwrap();
        client.put_object("group", &object).await.unwrap();
        let opened = PackedGroupContainer::open(object).unwrap();
        let frame = &opened.frames()[0];
        let remote = RemotePackedObject::open(
            &client,
            "group",
            opened.object_len(),
            PackedObjectKind::GroupContainer,
        )
        .await
        .unwrap();

        remote.read_frame(frame).await.unwrap();

        {
            let recorded = ranges.lock().unwrap();
            assert_eq!(
                recorded.last().copied(),
                Some((frame.object_offset, u64::from(frame.stored_len),))
            );
        }

        // A zero-budget cache may still be supplied by shared mount plumbing,
        // but it must not switch the strict reader to whole-container GETs.
        let cache = Arc::new(
            ChunksCache::new_with_config(ChunksCacheConfig::with_budgets(
                0,
                0,
                temp.path().join("zero-cache"),
            ))
            .await
            .unwrap(),
        );
        let cached_remote = RemotePackedObject::open_with_payload_cache(
            &client,
            "group",
            opened.object_len(),
            PackedObjectKind::GroupContainer,
            cache,
            "zero-budget-container".into(),
        )
        .await
        .unwrap();
        ranges.lock().unwrap().clear();
        cached_remote.read_frame(frame).await.unwrap();
        {
            let recorded = ranges.lock().unwrap();
            assert_eq!(
                recorded.last().copied(),
                Some((frame.object_offset, u64::from(frame.stored_len),))
            );
        }

        let tiny_cache = Arc::new(
            ChunksCache::new_with_config(ChunksCacheConfig::with_budgets(
                1,
                0,
                temp.path().join("tiny-cache"),
            ))
            .await
            .unwrap(),
        );
        let tiny_cached_remote = RemotePackedObject::open_with_payload_cache(
            &client,
            "group",
            opened.object_len(),
            PackedObjectKind::GroupContainer,
            tiny_cache,
            "tiny-budget-container".into(),
        )
        .await
        .unwrap();
        ranges.lock().unwrap().clear();
        tiny_cached_remote.read_frame(frame).await.unwrap();
        let recorded = ranges.lock().unwrap();
        assert_eq!(
            recorded.last().copied(),
            Some((frame.object_offset, u64::from(frame.stored_len),))
        );
    }

    #[tokio::test]
    async fn aligned_window_cache_reuses_adjacent_payload_reads() {
        let temp = tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(temp.path()));
        let object = PackedGroupContainer::build(
            17,
            AccessProfile::SequentialSmallFile,
            vec![PackedGroupInput {
                group_id: 1,
                parent_dir_key: [0; 32],
                metadata: GroupMeta::new(Vec::new()).unwrap().encode().unwrap(),
                frame_ordinals: vec![0, 1],
                entry_count: 0,
                file_count: 0,
                layout_profile: AccessProfile::SequentialSmallFile,
            }],
            vec![
                PackedFrameInput {
                    raw: b"first-payload".to_vec(),
                    size_class: SizeClass::Tiny,
                    codec: 0,
                    first_file_slot: 0,
                    last_file_slot: 0,
                },
                PackedFrameInput {
                    raw: b"second-payload".to_vec(),
                    size_class: SizeClass::Tiny,
                    codec: 0,
                    first_file_slot: 1,
                    last_file_slot: 1,
                },
            ],
        )
        .unwrap();
        client.put_object("group", &object).await.unwrap();
        let opened = PackedGroupContainer::open(object).unwrap();
        let remote = RemotePackedObject::open(
            &client,
            "group",
            opened.object_len(),
            PackedObjectKind::GroupContainer,
        )
        .await
        .unwrap()
        .with_frame_window_cache_bytes(4 * 1024 * 1024);

        let first = &opened.frames()[0];
        let second = &opened.frames()[1];
        assert_eq!(
            remote
                .read_windowed_payload_range(first.object_offset, u64::from(first.stored_len),)
                .await
                .unwrap(),
            b"first-payload"
        );
        assert_eq!(
            remote
                .read_windowed_payload_range(second.object_offset, u64::from(second.stored_len),)
                .await
                .unwrap(),
            b"second-payload"
        );
        let stats = remote.window_cache_stats();
        assert_eq!(stats.remote_fetches, 1);
        assert_eq!(stats.cache_hits, 1);
        assert_eq!(stats.cache_misses, 1);
    }

    #[tokio::test]
    async fn sequential_prefetch_populates_the_next_payload_window() {
        let temp = tempdir().unwrap();
        let ranges = Arc::new(Mutex::new(Vec::new()));
        let backend = RecordingBackend {
            inner: LocalFsBackend::new(temp.path()),
            ranges: Arc::clone(&ranges),
        };
        let client = ObjectClient::new(backend);
        let metadata = GroupMeta::new(Vec::new()).unwrap().encode().unwrap();
        let first_len = (4 * 1024 * 1024) - (64 * 1024);
        let object = PackedGroupContainer::build(
            23,
            AccessProfile::SequentialSmallFile,
            vec![PackedGroupInput {
                group_id: 1,
                parent_dir_key: [0; 32],
                metadata,
                frame_ordinals: vec![0, 1],
                entry_count: 0,
                file_count: 0,
                layout_profile: AccessProfile::SequentialSmallFile,
            }],
            vec![
                PackedFrameInput {
                    raw: vec![1; first_len],
                    size_class: SizeClass::Small,
                    codec: 0,
                    first_file_slot: 0,
                    last_file_slot: 0,
                },
                PackedFrameInput {
                    raw: vec![2; 256 * 1024],
                    size_class: SizeClass::Tiny,
                    codec: 0,
                    first_file_slot: 1,
                    last_file_slot: 1,
                },
            ],
        )
        .unwrap();
        client.put_object("sequential", &object).await.unwrap();
        let opened = PackedGroupContainer::open(object).unwrap();
        let remote = RemotePackedObject::open(
            &client,
            "sequential",
            opened.object_len(),
            PackedObjectKind::GroupContainer,
        )
        .await
        .unwrap()
        .with_frame_window_cache_bytes(8 * 1024 * 1024);

        let first = &opened.frames()[0];
        assert!(
            remote
                .prefetch_next_payload_window(first.object_offset, u64::from(first.stored_len))
                .await
                .unwrap()
        );
        let stats = remote.window_cache_stats();
        assert_eq!(stats.remote_fetches, 1);
        assert_eq!(stats.cache_hits, 0);
        assert_eq!(stats.cache_misses, 1);
    }

    #[tokio::test]
    async fn inline_source_reads_without_an_object_backend_request() {
        let fetcher = PackedFrameSourceFetcher::<LocalFsBackend>::new(HashMap::new());
        let source = ReadSource::PackedInline {
            data: Arc::from(b"0123456789".as_slice()),
            raw_offset: 3,
        };
        let mut output = [0u8; 4];
        fetcher.read_source(&source, &mut output).await.unwrap();
        assert_eq!(&output, b"3456");
    }

    #[tokio::test]
    async fn bound_source_rejects_a_plan_from_another_generation_before_copying() {
        let expected = ReadGeneration::readonly([7; 32]);
        let fetcher = PackedFrameSourceFetcher::<LocalFsBackend>::new(HashMap::new())
            .with_generation(expected);
        let plan = crate::chunk::read_plan::UnifiedReadPlan {
            generation: ReadGeneration::readonly([8; 32]),
            logical_size: 4,
            segments: vec![crate::chunk::read_plan::LogicalSegment {
                logical_offset: 0,
                length: 4,
                source: ReadSource::PackedInline {
                    data: Arc::from(b"stale".as_slice()),
                    raw_offset: 0,
                },
            }],
        };
        let mut output = [0xa5; 4];
        let error = crate::chunk::read_plan::execute_unified_into(&fetcher, 0, &plan, &mut output)
            .await
            .expect_err("a source bound to another generation must fence the read");
        assert!(matches!(
            error,
            crate::chunk::read_plan::ReadPlanError::StaleView(_)
        ));
        assert_eq!(
            output, [0xa5; 4],
            "fenced reads must not partially write output"
        );
    }
}
