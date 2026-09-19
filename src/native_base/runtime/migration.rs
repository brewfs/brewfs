use super::header::load_volume_header;
use super::{NativeRuntimeCapabilities, NativeVolumeHeader, RuntimeAdmissionError};
use crate::native_base::write::keys::Keys;
use crate::native_base::write::store::{ControlStore, StoreError, Txn};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrationMode {
    NewNamespace,
    OfflineCopy,
    InPlaceAdopt,
    OnlineHeaderRewrite,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationRequest<'a> {
    pub mode: MigrationMode,
    pub source_namespace: Option<&'a str>,
    pub target_namespace: &'a str,
    pub source_quiesced: bool,
    pub header: &'a NativeVolumeHeader,
}

#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    #[error("offline copy requires a quiesced source")]
    SourceNotQuiesced,
    #[error("source and target namespaces must be different")]
    NamespaceReuse,
    #[error("offline copy requires an explicit source namespace")]
    MissingSource,
    #[error("in-place adoption is not implemented; use a new namespace or offline copy")]
    InPlaceAdoptUnsupported,
    #[error("online native header rewrite is forbidden")]
    OnlineHeaderRewriteForbidden,
    #[error("a logical copy requires the offline-copy migration mode")]
    CopyRequiresOfflineCopy,
    #[error("a logical copy requires a target volume id different from the source's")]
    VolumeIdReuse,
    #[error("the control store returned a row outside the scanned volume prefix")]
    ScanOutsideVolumePrefix,
    #[error("the target volume already holds control rows")]
    TargetVolumeNotEmpty,
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Admission(#[from] RuntimeAdmissionError),
}

pub fn validate_migration(request: &MigrationRequest<'_>) -> Result<(), MigrationError> {
    request
        .header
        .validate(NativeRuntimeCapabilities::compiled())?;
    match request.mode {
        MigrationMode::NewNamespace => {
            if request.source_namespace.is_some() {
                return Err(MigrationError::NamespaceReuse);
            }
            Ok(())
        }
        MigrationMode::OfflineCopy => {
            let source = request
                .source_namespace
                .ok_or(MigrationError::MissingSource)?;
            if source == request.target_namespace {
                return Err(MigrationError::NamespaceReuse);
            }
            if !request.source_quiesced {
                return Err(MigrationError::SourceNotQuiesced);
            }
            Ok(())
        }
        MigrationMode::InPlaceAdopt => Err(MigrationError::InPlaceAdoptUnsupported),
        MigrationMode::OnlineHeaderRewrite => Err(MigrationError::OnlineHeaderRewriteForbidden),
    }
}

/// What a logical volume copy published.
///
/// This is a *logical* migration: native data objects are content-addressed
/// and therefore shared, so the copy republishes control rows only.  It has no
/// object sink at all, which is what makes "no byte is uploaded, re-read or
/// re-verified by the copy" a property of the design rather than a promise.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationReport {
    pub source_namespace: String,
    pub target_namespace: String,
    pub source_volume_id: [u8; 16],
    pub target_volume_id: [u8; 16],
    /// Control rows republished under the target volume, including the
    /// target's locator header.
    pub copied_rows: usize,
    /// Payload bytes of those rows.
    pub copied_bytes: usize,
}

/// Copy a native volume logically: every control row of the source volume is
/// republished verbatim under the target volume id, in one conditional
/// transaction together with the target's locator header.
///
/// The source is *retained*: the copy publishes only under the target prefix,
/// so it never deletes or rewrites a source row, and the test asserts the
/// source is byte-identical afterwards.  Because every target row is written
/// with `check_absent`, the target namespace either appears complete or not at
/// all -- a half-migrated volume is not observable.
///
/// Rows are copied unchanged, so inode attributes, extents, heads, bindings
/// and the workspace view are identical on both sides.  The copy cannot move
/// the on-disk format: both headers pass the same admission gate, which pins
/// format, schema, control and wire version to the compiled constants, and a
/// version move is refused there (REGRESS-002) before a single row is read.
pub async fn copy_volume_logically(
    store: &dyn ControlStore,
    request: &MigrationRequest<'_>,
) -> Result<MigrationReport, MigrationError> {
    validate_migration(request)?;
    if request.mode != MigrationMode::OfflineCopy {
        return Err(MigrationError::CopyRequiresOfflineCopy);
    }
    let source_namespace = request
        .source_namespace
        .ok_or(MigrationError::MissingSource)?;
    let source_header = load_volume_header(
        store,
        source_namespace,
        NativeRuntimeCapabilities::compiled(),
    )
    .await?;
    let target_header = request.header;
    if target_header.volume_id == source_header.volume_id {
        return Err(MigrationError::VolumeIdReuse);
    }

    let header_key = Keys::volume_header(request.target_namespace);
    if store.get(&header_key).await?.is_some() {
        return Err(RuntimeAdmissionError::NamespaceExists.into());
    }

    let source_prefix = Keys::volume_prefix(&source_header.volume_id);
    let target_prefix = Keys::volume_prefix(&target_header.volume_id);
    let rows = store.scan(&source_prefix).await?;
    let mut txn = Txn::new();
    let mut copied_bytes = 0usize;
    for (key, value) in &rows {
        let suffix = key
            .strip_prefix(source_prefix.as_slice())
            .ok_or(MigrationError::ScanOutsideVolumePrefix)?;
        let mut target_key = target_prefix.clone();
        target_key.extend_from_slice(suffix);
        copied_bytes += value.len();
        txn = txn
            .check_absent(target_key.clone())
            .put(target_key, value.clone());
    }
    let header_value = target_header.encode();
    copied_bytes += header_value.len();
    let txn = txn
        .check_absent(header_key.clone())
        .put(header_key, header_value);
    store.run(txn).await.map_err(|error| match error {
        StoreError::Conflict => MigrationError::TargetVolumeNotEmpty,
        other => MigrationError::Store(other),
    })?;

    Ok(MigrationReport {
        source_namespace: source_namespace.to_owned(),
        target_namespace: request.target_namespace.to_owned(),
        source_volume_id: source_header.volume_id,
        target_volume_id: target_header.volume_id,
        copied_rows: rows.len() + 1,
        copied_bytes,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use super::*;
    use crate::native_base::runtime::{NativeDataRuntime, ZeroBaseDataSource, initialize_volume};
    use crate::native_base::write::memory::MemoryControlStore;
    use crate::native_base::write::overlay::{OverlayParams, WriteOverlay};
    use crate::native_base::write::receipts::MemorySink;

    const INODE: u64 = 9;
    const BLOCK_SIZE: u64 = 64;

    fn all_capabilities() -> NativeRuntimeCapabilities {
        NativeRuntimeCapabilities {
            native_packed_base: true,
            frozen_base_metadata: true,
        }
    }

    fn overlay(
        store: Arc<dyn ControlStore>,
        sink: Arc<MemorySink>,
        volume_id: [u8; 16],
    ) -> Arc<WriteOverlay> {
        Arc::new(WriteOverlay::new(
            store,
            sink,
            OverlayParams {
                volume_id,
                workspace_id: [2; 16],
                domain_id: [3; 16],
                writer_generation: 1,
                block_size: BLOCK_SIZE,
            },
        ))
    }

    /// A writing runtime on `volume_id`, initialized so it may publish.
    async fn writer_runtime(
        store: Arc<dyn ControlStore>,
        sink: Arc<MemorySink>,
        volume_id: [u8; 16],
    ) -> NativeDataRuntime {
        let runtime = NativeDataRuntime::new(
            overlay(store, sink, volume_id),
            Arc::new(ZeroBaseDataSource),
        );
        runtime.initialize([4; 16], 1).await.unwrap();
        runtime
    }

    /// A read-only runtime on `volume_id`.  It is never initialized, so every
    /// byte it serves comes from the rows the copy published.
    fn reader_runtime(
        store: Arc<dyn ControlStore>,
        sink: Arc<MemorySink>,
        volume_id: [u8; 16],
    ) -> NativeDataRuntime {
        NativeDataRuntime::new(
            overlay(store, sink, volume_id),
            Arc::new(ZeroBaseDataSource),
        )
    }

    async fn snapshot(store: &dyn ControlStore) -> BTreeMap<Vec<u8>, Vec<u8>> {
        store.scan(b"nb2/").await.unwrap().into_iter().collect()
    }

    fn request(mode: MigrationMode) -> MigrationRequest<'static> {
        let header = Box::leak(Box::new(NativeVolumeHeader::p1([1; 16], [2; 16])));
        MigrationRequest {
            mode,
            source_namespace: Some("source"),
            target_namespace: "target",
            source_quiesced: true,
            header,
        }
    }

    #[cfg(feature = "native-packed-base")]
    #[test]
    fn only_new_namespace_and_quiesced_offline_copy_are_admitted() {
        let mut new = request(MigrationMode::NewNamespace);
        new.source_namespace = None;
        validate_migration(&new).unwrap();
        validate_migration(&request(MigrationMode::OfflineCopy)).unwrap();

        let mut live = request(MigrationMode::OfflineCopy);
        live.source_quiesced = false;
        assert!(matches!(
            validate_migration(&live),
            Err(MigrationError::SourceNotQuiesced)
        ));
        assert!(matches!(
            validate_migration(&request(MigrationMode::InPlaceAdopt)),
            Err(MigrationError::InPlaceAdoptUnsupported)
        ));
        assert!(matches!(
            validate_migration(&request(MigrationMode::OnlineHeaderRewrite)),
            Err(MigrationError::OnlineHeaderRewriteForbidden)
        ));
    }

    #[cfg(feature = "native-packed-base")]
    #[tokio::test]
    async fn a_logical_copy_retains_the_source_and_reproduces_attributes_and_content() {
        let store = Arc::new(MemoryControlStore::new());
        let source_header = NativeVolumeHeader::p1([7; 16], [8; 16]);
        initialize_volume(&*store, "source", &source_header, all_capabilities())
            .await
            .unwrap();

        let sink = Arc::new(MemorySink::default());
        let payload: Vec<u8> = (0..(BLOCK_SIZE * 3)).map(|i| (i % 251) as u8).collect();
        let writer = writer_runtime(store.clone(), sink.clone(), source_header.volume_id).await;
        writer.write(INODE, 0, &payload).await.unwrap();
        writer.fsync(INODE).await.unwrap();
        assert_eq!(writer.pending_count(INODE).await, 0);
        assert_eq!(writer.read(INODE, 0, payload.len()).await.unwrap(), payload);
        let size = writer.size(INODE).await.unwrap();
        assert_eq!(size, BLOCK_SIZE * 3);
        drop(writer);

        let before = snapshot(store.as_ref()).await;

        let target_header = NativeVolumeHeader::p1([9; 16], [8; 16]);
        let copy = MigrationRequest {
            mode: MigrationMode::OfflineCopy,
            source_namespace: Some("source"),
            target_namespace: "target",
            source_quiesced: true,
            header: &target_header,
        };
        let report = copy_volume_logically(store.as_ref(), &copy).await.unwrap();
        assert_eq!(report.source_namespace, "source");
        assert_eq!(report.target_namespace, "target");
        assert_eq!(report.source_volume_id, [7; 16]);
        assert_eq!(report.target_volume_id, [9; 16]);

        let after = snapshot(store.as_ref()).await;
        let source_prefix = Keys::volume_prefix(&source_header.volume_id);
        let target_prefix = Keys::volume_prefix(&target_header.volume_id);
        let source_rows = before
            .keys()
            .filter(|key| key.starts_with(source_prefix.as_slice()))
            .count();
        assert!(source_rows > 1, "the source volume only had a header");
        assert_eq!(report.copied_rows, source_rows + 1);

        // The source is retained: every row it held before the copy -- the
        // locator header included -- still holds exactly the same payload.
        for (key, value) in before.iter() {
            assert_eq!(after.get(key), Some(value), "source row changed: {key:?}");
        }
        // Attributes and layout are verbatim: the inode row, the extents, the
        // head, the registry and the bindings all have an identical twin under
        // the target volume id.
        for (key, value) in before.iter() {
            if !key.starts_with(source_prefix.as_slice()) {
                continue;
            }
            let suffix = key.strip_prefix(source_prefix.as_slice()).unwrap();
            let mut twin = target_prefix.clone();
            twin.extend_from_slice(suffix);
            assert_eq!(after.get(&twin), Some(value), "target row differs: {key:?}");
        }
        assert_eq!(
            after.get(&Keys::volume_header("target")),
            Some(&target_header.encode())
        );

        // Content: a reader that was never initialized on the target namespace
        // (no domain and no head of its own) serves the same size and the same
        // bytes, from the content-addressed objects the source uploaded.
        let reader = reader_runtime(store.clone(), sink, target_header.volume_id);
        assert_eq!(reader.size(INODE).await.unwrap(), size);
        assert_eq!(reader.read(INODE, 0, payload.len()).await.unwrap(), payload);
    }

    #[cfg(feature = "native-packed-base")]
    #[tokio::test]
    async fn a_logical_copy_refuses_a_reused_id_a_moving_mode_and_a_taken_target() {
        let store = Arc::new(MemoryControlStore::new());
        let source_header = NativeVolumeHeader::p1([7; 16], [8; 16]);
        initialize_volume(&*store, "source", &source_header, all_capabilities())
            .await
            .unwrap();

        // Reusing the source volume id would make every target row collide
        // with a row the copy is reading, so it is refused up front.
        let same_id = NativeVolumeHeader::p1([7; 16], [8; 16]);
        let mut copy = MigrationRequest {
            mode: MigrationMode::OfflineCopy,
            source_namespace: Some("source"),
            target_namespace: "target",
            source_quiesced: true,
            header: &same_id,
        };
        assert!(matches!(
            copy_volume_logically(store.as_ref(), &copy).await,
            Err(MigrationError::VolumeIdReuse)
        ));

        // A mode that creates or rewrites instead of copying moves no row.
        let target_header = NativeVolumeHeader::p1([9; 16], [8; 16]);
        copy.mode = MigrationMode::NewNamespace;
        copy.source_namespace = None;
        copy.header = &target_header;
        assert!(matches!(
            copy_volume_logically(store.as_ref(), &copy).await,
            Err(MigrationError::CopyRequiresOfflineCopy)
        ));
        copy.mode = MigrationMode::OfflineCopy;
        copy.source_namespace = Some("source");

        // An occupied target is refused by name, first as an existing locator
        // header for the target namespace...
        initialize_volume(&*store, "target", &target_header, all_capabilities())
            .await
            .unwrap();
        assert!(matches!(
            copy_volume_logically(store.as_ref(), &copy).await,
            Err(MigrationError::Admission(
                RuntimeAdmissionError::NamespaceExists
            ))
        ));

        // ...and then as pre-existing rows under the target volume id, which
        // the conditional transaction catches even though the header key is
        // free.  Both volumes carry the same workspace shape, so the target
        // keys the copy would publish really are occupied.
        let occupied = Arc::new(MemoryControlStore::new());
        initialize_volume(&*occupied, "source", &source_header, all_capabilities())
            .await
            .unwrap();
        initialize_volume(&*occupied, "occupied", &target_header, all_capabilities())
            .await
            .unwrap();
        let sink = Arc::new(MemorySink::default());
        let payload = [5u8; BLOCK_SIZE as usize];
        let source_writer =
            writer_runtime(occupied.clone(), sink.clone(), source_header.volume_id).await;
        source_writer.write(INODE, 0, &payload).await.unwrap();
        source_writer.fsync(INODE).await.unwrap();
        drop(source_writer);
        let target_writer = writer_runtime(occupied.clone(), sink, target_header.volume_id).await;
        target_writer.write(INODE, 0, &payload).await.unwrap();
        target_writer.fsync(INODE).await.unwrap();
        drop(target_writer);

        let target_prefix = Keys::volume_prefix(&target_header.volume_id);
        let rows_before = snapshot(occupied.as_ref())
            .await
            .keys()
            .filter(|key| key.starts_with(target_prefix.as_slice()))
            .count();
        assert!(rows_before > 0, "the occupied volume published nothing");
        assert!(matches!(
            copy_volume_logically(occupied.as_ref(), &copy).await,
            Err(MigrationError::TargetVolumeNotEmpty)
        ));
        let rows_after = snapshot(occupied.as_ref())
            .await
            .keys()
            .filter(|key| key.starts_with(target_prefix.as_slice()))
            .count();
        assert_eq!(rows_before, rows_after, "the refused copy published rows");
    }

    /// A store that answers a scan with one row outside the requested prefix.
    struct LyingStore {
        inner: Arc<MemoryControlStore>,
    }

    #[async_trait::async_trait]
    impl ControlStore for LyingStore {
        async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
            self.inner.get(key).await
        }

        async fn run(&self, txn: Txn) -> Result<(), StoreError> {
            self.inner.run(txn).await
        }

        async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
            let mut rows = self.inner.scan(prefix).await?;
            rows.push((b"nb2/elsewhere/row".to_vec(), b"foreign".to_vec()));
            Ok(rows)
        }
    }

    #[cfg(feature = "native-packed-base")]
    #[tokio::test]
    async fn a_logical_copy_fails_closed_on_a_store_that_leaves_the_volume_prefix() {
        let store = Arc::new(MemoryControlStore::new());
        let source_header = NativeVolumeHeader::p1([7; 16], [8; 16]);
        initialize_volume(&*store, "source", &source_header, all_capabilities())
            .await
            .unwrap();
        let target_header = NativeVolumeHeader::p1([9; 16], [8; 16]);
        let copy = MigrationRequest {
            mode: MigrationMode::OfflineCopy,
            source_namespace: Some("source"),
            target_namespace: "target",
            source_quiesced: true,
            header: &target_header,
        };
        let lying = LyingStore {
            inner: store.clone(),
        };
        assert!(matches!(
            copy_volume_logically(&lying, &copy).await,
            Err(MigrationError::ScanOutsideVolumePrefix)
        ));
        assert!(
            store
                .get(&Keys::volume_header("target"))
                .await
                .unwrap()
                .is_none(),
            "the refused copy installed a target header"
        );
    }
}
