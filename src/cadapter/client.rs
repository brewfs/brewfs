//! High-level object client wrapping backend put/get operations.

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::Stream;
use std::pin::Pin;
use std::sync::Arc;

use super::read_observer::{
    Engine, FailureClass, Origin, Phase, ReadClass, ReadContext, ReadObserver,
};

pub type ObjectByteStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>;

#[async_trait]
pub trait ObjectBackend: Send + Sync {
    /// Switch the writable object bucket for a workspace mount. Backends that
    /// do not support bucket selection keep their existing behavior.
    fn set_bucket(&self, _bucket: &str) -> anyhow::Result<()> {
        anyhow::bail!("object backend does not support bucket selection")
    }

    /// Coalesce cold block reads when the backend layout favors whole objects.
    fn prefers_full_object_reads(&self) -> bool {
        false
    }

    /// True only when the concrete transport/client cannot automatically
    /// resend a physical PUT/DELETE after an uncertain mutation outcome.
    fn forbids_mutation_replay(&self) -> bool {
        false
    }
    async fn put_object_vectored(&self, key: &str, chunks: Vec<Bytes>) -> Result<()> {
        let data = chunks
            .into_iter()
            .flat_map(|e| e.to_vec())
            .collect::<Vec<_>>();
        self.put_object(key, &data).await
    }

    async fn put_object(&self, key: &str, data: &[u8]) -> Result<()>;

    /// Atomically create an object and fail if the key already exists with
    /// different content. An identical object may be accepted as an
    /// idempotent retry after an ambiguous network result. Backends must not
    /// emulate this with a racy HEAD followed by PUT.
    async fn put_object_create_only(&self, _key: &str, _data: &[u8]) -> Result<()> {
        anyhow::bail!("object backend does not support atomic create-only PUT")
    }

    /// Strict create-only publication with the backend's durability barrier.
    /// No overwrite fallback is allowed. A buffered write or an unsynced local
    /// directory entry is not sufficient to publish a durable entity map.
    async fn put_object_create_only_durable(&self, _key: &str, _data: &[u8]) -> Result<()> {
        anyhow::bail!("object backend does not support durable atomic create-only PUT")
    }

    async fn get_object(&self, key: &str) -> Result<Option<Vec<u8>>>;

    /// Preserve a whole GET's physical request shape while streaming its body.
    /// The buffered compatibility route does not enable HTTP-attempt capability.
    async fn get_object_stream(&self, key: &str) -> Result<Option<ObjectByteStream>> {
        Ok(self.get_object(key).await?.map(|bytes| {
            Box::pin(futures_util::stream::once(
                async move { Ok(Bytes::from(bytes)) },
            )) as ObjectByteStream
        }))
    }
    async fn get_object_stream_observed(
        &self,
        key: &str,
        _expected: Option<u64>,
        _context: ReadContext,
        _observer: Arc<ReadObserver>,
    ) -> Result<Option<ObjectByteStream>> {
        self.get_object_stream(key).await
    }

    /// Get a range of bytes from an object.
    ///
    /// Returns the number of bytes actually read into `buf`, which may be less than `buf.len()`
    /// if the object is smaller than the requested range. Returns `Ok(0)` when the object is
    /// missing or `offset` is beyond the end. Partial reads are allowed.
    /// Used for small range reads in intelligent read strategy.
    async fn get_object_range(&self, key: &str, offset: u64, buf: &mut [u8]) -> Result<usize>;

    /// Stream a bounded object range without requiring the backend to
    /// materialize the complete response first.  The compatibility default
    /// keeps older test backends correct; S3 and LocalFS override it with
    /// genuine response/file streams.
    async fn get_object_range_stream(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<ObjectByteStream> {
        let length = usize::try_from(length)
            .map_err(|_| anyhow::anyhow!("object range length exceeds usize"))?;
        let mut bytes = vec![0u8; length];
        let actual = self.get_object_range(key, offset, &mut bytes).await?;
        bytes.truncate(actual);
        Ok(Box::pin(futures_util::stream::once(async move {
            Ok(Bytes::from(bytes))
        })))
    }

    /// Common typed context handoff for transport adapters. The default
    /// preserves existing backends and enables no HTTP-attempt capability.
    async fn get_object_range_stream_observed(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        _context: ReadContext,
        _observer: Arc<ReadObserver>,
    ) -> Result<ObjectByteStream> {
        self.get_object_range_stream(key, offset, length).await
    }

    /// Return an object's length without downloading its payload when the
    /// backend supports a metadata request.  The compatibility default keeps
    /// existing test backends correct by falling back to one complete read.
    async fn get_object_size(&self, key: &str) -> Result<Option<u64>> {
        Ok(self.get_object(key).await?.map(|bytes| bytes.len() as u64))
    }

    /// Obtain physical length using metadata only. A missing object is None;
    /// a present object without a valid length is an error. This strict route
    /// must never download object bytes or fall back to the legacy size API.
    async fn get_object_size_bounded(&self, _key: &str) -> Result<Option<u64>> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "object backend does not support bounded size metadata",
        )
        .into())
    }

    async fn get_object_size_bounded_observed(
        &self,
        key: &str,
        _context: ReadContext,
        _observer: Arc<ReadObserver>,
    ) -> Result<Option<u64>> {
        self.get_object_size_bounded(key).await
    }

    #[allow(dead_code)]
    async fn get_etag(&self, key: &str) -> Result<String>;

    /// Typed metadata route for an ETag HEAD request. Backends with a
    /// transport observer should override this so the physical HTTP attempt
    /// is attributed to the supplied read scope. The default keeps existing
    /// test and local backends source-compatible.
    async fn get_etag_observed(
        &self,
        key: &str,
        _context: ReadContext,
        _observer: Arc<ReadObserver>,
    ) -> Result<String> {
        self.get_etag(key).await
    }

    #[allow(dead_code)]
    async fn delete_object(&self, key: &str) -> Result<()>;
}

#[cfg(test)]
mod bounded_size_tests {
    use super::super::read_observer::Ledger;
    use super::*;

    struct NoMetadataBackend;

    #[async_trait]
    impl ObjectBackend for NoMetadataBackend {
        async fn put_object(&self, _: &str, _: &[u8]) -> Result<()> {
            panic!("readonly")
        }
        async fn get_object(&self, _: &str) -> Result<Option<Vec<u8>>> {
            panic!("bounded metadata must never fall back to whole GET")
        }
        async fn get_object_range(&self, _: &str, _: u64, _: &mut [u8]) -> Result<usize> {
            panic!("bounded metadata must not fetch object bytes")
        }
        async fn get_etag(&self, _: &str) -> Result<String> {
            panic!("no etag fallback")
        }
        async fn delete_object(&self, _: &str) -> Result<()> {
            panic!("readonly")
        }
    }

    struct MetadataBackend;

    #[async_trait]
    impl ObjectBackend for MetadataBackend {
        async fn put_object(&self, _: &str, _: &[u8]) -> Result<()> {
            panic!("readonly")
        }
        async fn get_object(&self, _: &str) -> Result<Option<Vec<u8>>> {
            panic!("no GET")
        }
        async fn get_object_range(&self, _: &str, _: u64, _: &mut [u8]) -> Result<usize> {
            panic!("no range")
        }
        async fn get_etag(&self, _: &str) -> Result<String> {
            panic!("no etag")
        }
        async fn delete_object(&self, _: &str) -> Result<()> {
            panic!("readonly")
        }
        async fn get_object_size_bounded(&self, key: &str) -> Result<Option<u64>> {
            match key {
                "present" => Ok(Some(1_u64 << 40)),
                "missing" => Ok(None),
                "corrupt" => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid length",
                )
                .into()),
                "denied" => {
                    Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied").into())
                }
                "pending" => futures_util::future::pending().await,
                _ => panic!("unexpected key"),
            }
        }
    }

    fn observed<B: ObjectBackend>(backend: B) -> (ObjectClient<B>, Arc<ReadObserver>, ReadContext) {
        let observer = Arc::new(ReadObserver::default());
        let client = ObjectClient::new(backend).with_read_observer(
            Arc::clone(&observer),
            Engine::PackedV3,
            Phase::Startup,
            Origin::Demand,
        );
        let context = client.read_context(ReadClass::ContainerIndex).unwrap();
        (client, observer, context)
    }

    #[tokio::test]
    async fn bounded_size_default_is_unsupported_without_whole_get() {
        let (client, observer, context) = observed(NoMetadataBackend);
        let error = client
            .typed_object_size(ReadClass::ContainerIndex, "present")
            .await
            .unwrap_err();
        assert_eq!(
            error.downcast_ref::<std::io::Error>().unwrap().kind(),
            std::io::ErrorKind::Unsupported
        );
        let snapshot = observer.snapshot();
        for ledger in [Ledger::ValidatedFetch, Ledger::BackendBody] {
            let row = &snapshot.rows[&(ledger, context)];
            assert_eq!(
                (row.started, row.failed, row.inflight, row.received),
                (1, 1, 0, 0)
            );
            assert!(row.conserved());
        }
    }

    #[tokio::test]
    async fn bounded_size_observer_preserves_presence_errors_and_zero_payload_bytes() {
        let (client, observer, context) = observed(MetadataBackend);
        assert_eq!(
            client
                .typed_object_size(ReadClass::ContainerIndex, "present")
                .await
                .unwrap(),
            Some(1_u64 << 40)
        );
        assert_eq!(
            client
                .typed_object_size(ReadClass::ContainerIndex, "missing")
                .await
                .unwrap(),
            None
        );
        for (key, kind) in [
            ("corrupt", std::io::ErrorKind::InvalidData),
            ("denied", std::io::ErrorKind::PermissionDenied),
        ] {
            let error = client
                .typed_object_size(ReadClass::ContainerIndex, key)
                .await
                .unwrap_err();
            assert_eq!(error.downcast_ref::<std::io::Error>().unwrap().kind(), kind);
        }
        let snapshot = observer.snapshot();
        assert!(!snapshot.http_observed);
        for ledger in [Ledger::ValidatedFetch, Ledger::BackendBody] {
            let row = &snapshot.rows[&(ledger, context)];
            assert_eq!(
                (
                    row.started,
                    row.success,
                    row.failed,
                    row.cancelled,
                    row.inflight
                ),
                (4, 2, 2, 0, 0)
            );
            assert_eq!(
                (row.requested, row.received, row.logical_delivered),
                (0, 0, 0)
            );
            assert_eq!(row.failure_reasons[&FailureClass::Schema], 1);
            assert_eq!(row.failure_reasons[&FailureClass::Backend], 1);
            assert!(row.conserved());
        }
    }

    #[tokio::test]
    async fn bounded_size_cancelled_future_retires_both_owned_guards() {
        let (client, observer, context) = observed(MetadataBackend);
        let future = client.typed_object_size(ReadClass::ContainerIndex, "pending");
        let mut future = Box::pin(future);
        assert!(futures_util::poll!(future.as_mut()).is_pending());
        drop(future);
        let snapshot = observer.snapshot();
        for ledger in [Ledger::ValidatedFetch, Ledger::BackendBody] {
            let row = &snapshot.rows[&(ledger, context)];
            assert_eq!((row.started, row.cancelled, row.inflight), (1, 1, 0));
            assert!(row.conserved());
        }
    }
}

#[derive(Clone)]
pub struct ObjectClient<B: ObjectBackend> {
    backend: B,
    read_scope: Option<(Arc<ReadObserver>, ReadContext)>,
    phase_control: Option<Arc<std::sync::atomic::AtomicU8>>,
}

impl<B: ObjectBackend> ObjectClient<B> {
    pub fn set_bucket(&self, bucket: &str) -> anyhow::Result<()> {
        self.backend.set_bucket(bucket)
    }
    pub(crate) fn prefers_full_object_reads(&self) -> bool {
        self.backend.prefers_full_object_reads()
    }

    pub fn forbids_mutation_replay(&self) -> bool {
        self.backend.forbids_mutation_replay()
    }
    #[cfg(all(target_os = "linux", feature = "workspace-overlay"))]
    pub(crate) fn map_backend<C: ObjectBackend>(self, map: impl FnOnce(B) -> C) -> ObjectClient<C> {
        ObjectClient {
            backend: map(self.backend),
            read_scope: self.read_scope,
            phase_control: self.phase_control,
        }
    }

    pub fn new(backend: B) -> Self {
        Self {
            backend,
            read_scope: None,
            phase_control: None,
        }
    }

    pub fn with_read_observer(
        mut self,
        observer: Arc<ReadObserver>,
        engine: Engine,
        phase: Phase,
        origin: Origin,
    ) -> Self {
        self.read_scope = Some((
            observer,
            ReadContext {
                engine,
                phase,
                origin,
                class: ReadClass::LogicalRead,
            },
        ));
        self
    }

    /// Mount-local phase transition shared only by clones from this mount.
    pub fn with_phase_control(mut self, phase: Arc<std::sync::atomic::AtomicU8>) -> Self {
        self.phase_control = Some(phase);
        self
    }
    fn active_context(&self, context: ReadContext) -> ReadContext {
        use std::sync::atomic::Ordering;
        let phase = match &self.phase_control {
            Some(phase) if phase.load(Ordering::Acquire) != 0 => Phase::Runtime,
            Some(_) => Phase::Startup,
            None => context.phase,
        };
        ReadContext { phase, ..context }
    }

    pub fn read_observer(&self) -> Option<Arc<ReadObserver>> {
        self.read_scope
            .as_ref()
            .map(|(observer, _)| Arc::clone(observer))
    }

    pub fn without_read_observer(mut self) -> Self {
        self.read_scope = None;
        self.phase_control = None;
        self
    }

    pub fn read_context(&self, class: ReadClass) -> Option<ReadContext> {
        self.read_scope
            .as_ref()
            .map(|(_, context)| self.active_context(ReadContext { class, ..*context }))
    }
    /// A queued demand retains the attribution of its submission even if the
    /// mount moves from startup to runtime during the collection interval.
    pub(crate) fn with_fixed_read_context(mut self, context: ReadContext) -> Self {
        if let Some((_, current)) = self.read_scope.as_mut() {
            *current = context;
        }
        self.phase_control = None;
        self
    }

    pub(crate) fn measure_read_work_with_origin(
        &self,
        class: ReadClass,
        origin: Origin,
        work: crate::cadapter::read_observer::ReadWork,
    ) -> Option<crate::cadapter::read_observer::WorkTimer> {
        let (observer, context) = self.read_scope.as_ref()?;
        Some(observer.work(
            self.active_context(ReadContext {
                class,
                origin,
                ..*context
            }),
            work,
        ))
    }

    pub(crate) fn begin_validation_with_origin(
        &self,
        class: ReadClass,
        origin: Origin,
    ) -> Option<crate::cadapter::read_observer::TerminalGuard> {
        let (observer, context) = self.read_scope.as_ref()?;
        Some(observer.start(
            crate::cadapter::read_observer::Ledger::SemanticValidation,
            self.active_context(ReadContext {
                class,
                origin,
                ..*context
            }),
            0,
        ))
    }

    pub(crate) fn read_event_with_origin(
        &self,
        class: ReadClass,
        origin: Origin,
        event: crate::cadapter::read_observer::ReadEvent,
    ) {
        if let Some((observer, context)) = &self.read_scope {
            observer.event(
                self.active_context(ReadContext {
                    class,
                    origin,
                    ..*context
                }),
                event,
            );
        }
    }

    pub(crate) fn measure_read_work(
        &self,
        class: ReadClass,
        work: crate::cadapter::read_observer::ReadWork,
    ) -> Option<crate::cadapter::read_observer::WorkTimer> {
        let (observer, context) = self.read_scope.as_ref()?;
        Some(observer.work(self.active_context(ReadContext { class, ..*context }), work))
    }

    pub(crate) fn read_event(
        &self,
        class: ReadClass,
        event: crate::cadapter::read_observer::ReadEvent,
    ) {
        if let Some((observer, context)) = &self.read_scope {
            observer.event(
                self.active_context(ReadContext { class, ..*context }),
                event,
            );
        }
    }

    pub(crate) fn begin_validation(
        &self,
        class: ReadClass,
    ) -> Option<crate::cadapter::read_observer::TerminalGuard> {
        let (observer, context) = self.read_scope.as_ref()?;
        Some(observer.start(
            crate::cadapter::read_observer::Ledger::SemanticValidation,
            self.active_context(ReadContext { class, ..*context }),
            0,
        ))
    }

    /// The common observer owns backend body lifetime; it calls this private
    /// route so the typed frontend never double-counts one backend invocation.
    pub(crate) async fn backend_range_stream(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<ObjectByteStream> {
        self.backend
            .get_object_range_stream(key, offset, length)
            .await
    }

    pub(crate) async fn backend_range_stream_observed(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        context: ReadContext,
        observer: Arc<ReadObserver>,
    ) -> Result<ObjectByteStream> {
        self.backend
            .get_object_range_stream_observed(key, offset, length, context, observer)
            .await
    }

    pub(crate) async fn backend_etag_observed(
        &self,
        key: &str,
        context: ReadContext,
        observer: Arc<ReadObserver>,
    ) -> Result<String> {
        self.backend.get_etag_observed(key, context, observer).await
    }

    pub(crate) async fn backend_object_stream(
        &self,
        key: &str,
        expected: Option<u64>,
        context: Option<ReadContext>,
    ) -> Result<Option<ObjectByteStream>> {
        match &self.read_scope {
            Some((observer, base_context)) => {
                self.backend
                    .get_object_stream_observed(
                        key,
                        expected,
                        context.unwrap_or_else(|| self.active_context(*base_context)),
                        Arc::clone(observer),
                    )
                    .await
            }
            None => self.backend.get_object_stream(key).await,
        }
    }

    /// Native blocks lack a stored length descriptor. `None` records an
    /// unknown request length explicitly; it never turns a whole GET into Range.
    pub async fn typed_full<T, Verify>(
        &self,
        class: ReadClass,
        key: &str,
        expected: Option<u64>,
        allocation_limit: u64,
        verify: Verify,
    ) -> Result<Option<T>>
    where
        Verify: FnOnce(Vec<u8>) -> std::result::Result<T, (FailureClass, anyhow::Error)>,
    {
        let scope = self.read_scope.as_ref().map(|(observer, context)| {
            (
                Arc::clone(observer),
                self.active_context(ReadContext { class, ..*context }),
            )
        });
        super::read_observer::whole_verified(self, scope, key, expected, allocation_limit, verify)
            .await
    }

    /// Attribute one whole GET without cloning or mutating the shared client.
    pub async fn typed_full_with_origin<T, Verify>(
        &self,
        class: ReadClass,
        origin: Origin,
        key: &str,
        expected: Option<u64>,
        allocation_limit: u64,
        verify: Verify,
    ) -> Result<Option<T>>
    where
        Verify: FnOnce(Vec<u8>) -> std::result::Result<T, (FailureClass, anyhow::Error)>,
    {
        let scope = self.read_scope.as_ref().map(|(observer, context)| {
            (
                Arc::clone(observer),
                self.active_context(ReadContext {
                    class,
                    origin,
                    ..*context
                }),
            )
        });
        super::read_observer::whole_verified(self, scope, key, expected, allocation_limit, verify)
            .await
    }

    /// A bounded probe preserves legacy short-read/EOF semantics. The caller
    /// performs its format validation when it has assembled the complete header.
    pub async fn typed_bounded_range(
        &self,
        class: ReadClass,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>> {
        let scope = self.read_scope.as_ref().map(|(observer, context)| {
            (
                Arc::clone(observer),
                self.active_context(ReadContext { class, ..*context }),
            )
        });
        super::read_observer::bounded_range(self, scope, key, offset, length).await
    }

    pub fn with_read_origin(mut self, origin: Origin) -> Self {
        if let Some((_, context)) = &mut self.read_scope {
            context.origin = origin;
        }
        self
    }

    /// The class comes from an authenticated format caller, never the key.
    pub async fn typed_exact<T, Verify>(
        &self,
        class: ReadClass,
        key: &str,
        offset: u64,
        length: u64,
        allocation_limit: u64,
        verify: Verify,
    ) -> Result<T>
    where
        Verify: FnOnce(Vec<u8>) -> std::result::Result<T, (FailureClass, anyhow::Error)>,
    {
        if let Some((observer, context)) = &self.read_scope {
            return super::read_observer::exact_verified(
                self,
                observer,
                self.active_context(ReadContext { class, ..*context }),
                super::read_observer::VerifiedReadRequest {
                    key,
                    offset,
                    length,
                    allocation_limit,
                },
                verify,
            )
            .await;
        }
        if length > allocation_limit || offset.checked_add(length).is_none() {
            return Err(super::read_observer::ReadBoundaryError::Admission.into());
        }
        use futures_util::StreamExt;
        let expected = usize::try_from(length)?;
        let mut bytes = Vec::with_capacity(expected);
        let mut body = self.backend_range_stream(key, offset, length).await?;
        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            if chunk.len() > expected - bytes.len() {
                return Err(super::read_observer::ReadBoundaryError::Excess.into());
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.len() != expected {
            return Err(super::read_observer::ReadBoundaryError::Short {
                expected: length,
                received: bytes.len() as u64,
            }
            .into());
        }
        verify(bytes).map_err(|(_, error)| error)
    }

    #[allow(dead_code)]
    pub async fn put_object(&self, key: &str, data: &[u8]) -> Result<()> {
        self.backend.put_object(key, data).await
    }

    pub async fn put_object_vectored(&self, key: &str, chunks: Vec<Bytes>) -> Result<()> {
        self.backend.put_object_vectored(key, chunks).await
    }

    pub async fn put_object_create_only(&self, key: &str, data: &[u8]) -> Result<()> {
        self.backend.put_object_create_only(key, data).await
    }

    pub async fn get_object(&self, key: &str) -> Result<Option<Vec<u8>>> {
        if self.read_scope.is_some() {
            anyhow::bail!("observed readonly path forbids unclassified whole-object GET");
        }
        self.backend.get_object(key).await
    }

    /// Get a range of bytes from an object.
    ///
    /// Returns the number of bytes actually read into `buf`, which may be less than `buf.len()`
    /// if the object is smaller than the requested range. Returns `Ok(0)` when the object is
    /// missing or `offset` is beyond the end. Partial reads are allowed.
    /// Used for small range reads in intelligent read strategy.
    pub async fn get_object_range(&self, key: &str, offset: u64, buf: &mut [u8]) -> Result<usize> {
        if self.read_scope.is_some() {
            anyhow::bail!("observed readonly range requires a typed class");
        }
        self.backend.get_object_range(key, offset, buf).await
    }

    pub async fn get_object_range_stream(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<ObjectByteStream> {
        if self.read_scope.is_some() {
            anyhow::bail!("observed readonly stream requires a typed class");
        }
        self.backend
            .get_object_range_stream(key, offset, length)
            .await
    }

    pub async fn get_object_size(&self, key: &str) -> Result<Option<u64>> {
        if self.read_scope.is_some() {
            anyhow::bail!(
                "observed readonly path requires a typed HEAD observer before get_object_size"
            );
        }
        self.backend.get_object_size(key).await
    }

    /// Typed metadata query for exact physical-length validation. Object
    /// length is metadata, not received payload bytes. Missing objects remain
    /// explicit so the graph caller can reject a referenced absent object.
    pub async fn typed_object_size(&self, class: ReadClass, key: &str) -> Result<Option<u64>> {
        use super::read_observer::Ledger;
        let scope = self.read_scope.as_ref().map(|(observer, context)| {
            (
                Arc::clone(observer),
                self.active_context(ReadContext { class, ..*context }),
            )
        });
        let fetch = scope
            .as_ref()
            .map(|(observer, context)| observer.start(Ledger::ValidatedFetch, *context, 0));
        let backend = scope
            .as_ref()
            .map(|(observer, context)| observer.start(Ledger::BackendBody, *context, 0));
        let result = match scope {
            Some((observer, context)) => {
                self.backend
                    .get_object_size_bounded_observed(key, context, observer)
                    .await
            }
            None => self.backend.get_object_size_bounded(key).await,
        };
        for guard in [backend, fetch].into_iter().flatten() {
            match &result {
                Ok(_) => guard.succeed(),
                Err(error) => {
                    let class = if error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|error| error.kind() == std::io::ErrorKind::InvalidData)
                    {
                        FailureClass::Schema
                    } else {
                        FailureClass::Backend
                    };
                    guard.fail(class);
                }
            }
        }
        result
    }

    /// Typed metadata query for an object's ETag. An observed client must use
    /// this route; the unclassified `get_etag` method remains rejected while a
    /// read observer is attached.
    pub async fn typed_etag(&self, class: ReadClass, key: &str) -> Result<String> {
        use super::read_observer::Ledger;
        let scope = self.read_scope.as_ref().map(|(observer, context)| {
            (
                Arc::clone(observer),
                self.active_context(ReadContext { class, ..*context }),
            )
        });
        let fetch = scope
            .as_ref()
            .map(|(observer, context)| observer.start(Ledger::ValidatedFetch, *context, 0));
        let backend = scope
            .as_ref()
            .map(|(observer, context)| observer.start(Ledger::BackendBody, *context, 0));
        let result = match scope {
            Some((observer, context)) => {
                self.backend.get_etag_observed(key, context, observer).await
            }
            None => self.backend.get_etag(key).await,
        };
        for guard in [backend, fetch].into_iter().flatten() {
            match &result {
                Ok(_) => guard.succeed(),
                Err(_) => guard.fail(FailureClass::Backend),
            }
        }
        result
    }

    #[allow(dead_code)]
    pub async fn get_etag(&self, key: &str) -> Result<String> {
        if self.read_scope.is_some() {
            anyhow::bail!("observed readonly path requires a typed HEAD observer before get_etag");
        }
        self.backend.get_etag(key).await
    }

    #[allow(dead_code)]
    pub async fn delete_object(&self, key: &str) -> Result<()> {
        self.backend.delete_object(key).await
    }
}
