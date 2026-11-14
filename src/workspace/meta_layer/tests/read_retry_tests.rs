//! Inject view changes at final delivery; the KV catalog and Packed reads are
//! real. No SQL backend, stale-view success, or backend-error retry is allowed.
use super::*;
use crate::chunk::read_plan::{
    PreparedUnifiedRead, ReadViewChanged, ResolvedReadPlan, UnifiedReadRequestFence,
    WorkspaceReadPlanProvider,
};
use crate::meta::store::MetaError;

type Meta = WorkspaceMetaLayer<
    KvWorkspaceStore<crate::workspace::stores::local_metadata::LocalMetadataBackend>,
>;
struct Provider {
    meta: Arc<Meta>,
    begins: AtomicUsize,
    successes: AtomicUsize,
    conflicts: usize,
    terminal_error: u8,
}
struct Fence {
    inner: Arc<dyn UnifiedReadRequestFence>,
    checks: AtomicUsize,
    conflict: bool,
    terminal_error: u8,
}
#[async_trait]
impl UnifiedReadRequestFence for Fence {
    fn file_size(&self) -> u64 {
        self.inner.file_size()
    }
    fn request_view(&self) -> Option<crate::workspace::meta_layer::packed_lower::RequestView> {
        self.inner.request_view()
    }
    async fn ensure_current(&self) -> anyhow::Result<()> {
        self.inner.ensure_current().await?;
        if self.checks.fetch_add(1, Ordering::SeqCst) == 1 {
            if self.conflict {
                return Err(ReadViewChanged.into());
            }
            match self.terminal_error {
                1 => return Err(WorkspaceError::Fenced.into()),
                2 => anyhow::bail!("injected read backend failure"),
                _ => {}
            }
        }
        Ok(())
    }
}
#[async_trait]
impl WorkspaceReadPlanProvider for Provider {
    fn requires_unified_read_request_fence(&self) -> bool {
        true
    }
    fn supports_prepared_unified_read(&self) -> bool {
        true
    }
    fn max_read_bytes(&self) -> Option<usize> {
        self.meta.max_read_bytes()
    }
    fn reserve_read_output(&self, n: usize) -> Result<Option<Box<dyn Send + Sync>>, MetaError> {
        self.meta.reserve_read_output(n)
    }
    async fn begin_unified_read_request(
        &self,
        ino: i64,
    ) -> anyhow::Result<Option<Arc<dyn UnifiedReadRequestFence>>> {
        let attempt = self.begins.fetch_add(1, Ordering::SeqCst);
        let inner = self.meta.begin_unified_read_request(ino).await?.unwrap();
        Ok(Some(Arc::new(Fence {
            inner,
            checks: AtomicUsize::new(0),
            conflict: attempt < self.conflicts,
            terminal_error: self.terminal_error,
        })))
    }
    async fn read_plan(
        &self,
        i: i64,
        c: u64,
        o: u64,
        n: u64,
    ) -> Result<ResolvedReadPlan, MetaError> {
        self.meta.read_plan(i, c, o, n).await
    }
    async fn prepare_unified_read(
        &self,
        i: i64,
        c: u64,
        o: u64,
        n: u64,
    ) -> Result<Option<PreparedUnifiedRead>, MetaError> {
        self.meta.prepare_unified_read(i, c, o, n).await
    }
    async fn range_has_data(&self, i: i64, o: u64, n: u64) -> Result<bool, MetaError> {
        self.meta.range_has_data(i, o, n).await
    }
    fn record_unified_read_success(&self, bytes: u64) {
        self.successes.fetch_add(bytes as usize, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn repeated_read_view_changes_retry_without_delivering_stale_bytes() {
    check(5, 0).await;
}
#[tokio::test]
async fn read_view_retry_does_not_retry_fencing_or_backend_errors() {
    check(0, 1).await;
    check(0, 2).await;
    check(5, 1).await;
}
#[tokio::test]
async fn read_view_retry_has_a_wall_clock_deadline() {
    check(usize::MAX, 0).await;
}

async fn check(conflicts: usize, terminal_error: u8) {
    let f = fixture_with_cache(64 << 20).await;
    let meta = Arc::new(f.meta);
    let provider = Arc::new(Provider {
        meta: meta.clone(),
        begins: AtomicUsize::new(0),
        successes: AtomicUsize::new(0),
        conflicts,
        terminal_error,
    });
    let vfs = crate::vfs::fs::VFS::from_readonly_components_with_provider(
        crate::vfs::config::VFSConfig::new(ChunkLayout {
            chunk_size: 4096,
            block_size: 4096,
        }),
        Arc::new(crate::chunk::store::InMemoryBlockStore::new()),
        meta.clone(),
        provider.clone(),
    )
    .unwrap();
    let fh = vfs.open_fresh_ino(400, true, false, false).await.unwrap();
    let started = std::time::Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(13), vfs.read(fh, 0, 4096))
        .await
        .expect("retry must terminate within its deadline");
    vfs.close(fh).await.unwrap();
    f.authority.close_renewals_and_drain().await.unwrap();
    meta.shutdown_session().await.unwrap();
    if conflicts == usize::MAX {
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("stable Workspace read view")
        );
        assert!(started.elapsed() >= Duration::from_secs(10));
        assert_eq!(provider.successes.load(Ordering::SeqCst), 0);
    } else {
        assert_eq!(provider.begins.load(Ordering::SeqCst), conflicts + 1);
        if terminal_error == 0 {
            let expected: Vec<u8> = (0..4096).map(|i| (i % 251 + 1) as u8).collect();
            assert_eq!(result.unwrap(), expected);
            assert_eq!(provider.successes.load(Ordering::SeqCst), 4096);
        } else {
            assert!(result.is_err());
            assert_eq!(provider.successes.load(Ordering::SeqCst), 0);
        }
    }
}
