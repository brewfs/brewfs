//! Opt-in cumulative read-path wall spans. Spans can nest and must not be added.
//!
//! `BREWFS_STAGE_TIMING=1` enables clocks/counters. `SIGUSR1` and normal CLI
//! shutdown atomically write `BREWFS_STAGE_TIMING_PATH` (default stage-timing.json).

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static ENABLED: OnceLock<bool> = OnceLock::new();
static COUNTERS: [Counter; 6] = [const { Counter::new() }; 6];
const NAMES: [&str; 6] = [
    "fuse_dispatch",
    "resolve",
    "row_cache",
    "meta_backend",
    "extent_plan",
    "data_path",
];

#[derive(Clone, Copy)]
pub(crate) enum Stage {
    FuseDispatch,
    Resolve,
    RowCache,
    MetaBackend,
    ExtentPlan,
    DataPath,
}

struct Counter {
    count: AtomicU64,
    sum_ns: AtomicU64,
    max_ns: AtomicU64,
}

impl Counter {
    const fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            sum_ns: AtomicU64::new(0),
            max_ns: AtomicU64::new(0),
        }
    }
}

fn enabled() -> bool {
    *ENABLED.get_or_init(|| {
        std::env::var("BREWFS_STAGE_TIMING")
            .is_ok_and(|value| matches!(value.trim(), "1" | "true" | "yes" | "on"))
    })
}

pub(crate) struct Span {
    stage: Stage,
    start: Option<Instant>,
}

impl Span {
    #[inline]
    pub(crate) fn new(stage: Stage) -> Self {
        Self {
            stage,
            start: enabled().then(Instant::now),
        }
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        if let Some(start) = self.start {
            let elapsed = start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            let counter = &COUNTERS[self.stage as usize];
            counter.sum_ns.fetch_add(elapsed, Ordering::Relaxed);
            counter.max_ns.fetch_max(elapsed, Ordering::Relaxed);
            counter.count.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn write_snapshot() -> std::io::Result<()> {
    let stages = NAMES
        .iter()
        .zip(&COUNTERS)
        .map(|(name, counter)| {
            (
                *name,
                serde_json::json!({
                    "count": counter.count.load(Ordering::Relaxed),
                    "sum_us": counter.sum_ns.load(Ordering::Relaxed) as f64 / 1000.0,
                    "max_us": counter.max_ns.load(Ordering::Relaxed) as f64 / 1000.0,
                }),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let path = std::path::PathBuf::from(
        std::env::var_os("BREWFS_STAGE_TIMING_PATH").unwrap_or_else(|| "stage-timing.json".into()),
    );
    let temporary = path.with_extension(format!("json.{}.tmp", std::process::id()));
    let bytes = serde_json::to_vec_pretty(&serde_json::json!({
        "enabled": true,
        "stages": stages,
    }))?;
    std::fs::write(&temporary, bytes)?;
    std::fs::rename(temporary, path)
}

pub(crate) struct Reporter {
    #[cfg(unix)]
    task: tokio::task::JoinHandle<()>,
}

pub(crate) fn install() -> std::io::Result<Option<Reporter>> {
    if !enabled() {
        return Ok(None);
    }
    #[cfg(unix)]
    {
        let mut signal =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1())?;
        let task = tokio::spawn(async move {
            while signal.recv().await.is_some() {
                if let Err(error) = write_snapshot() {
                    eprintln!("stage timing snapshot failed: {error}");
                }
            }
        });
        Ok(Some(Reporter { task }))
    }
    #[cfg(not(unix))]
    Ok(Some(Reporter {}))
}

impl Drop for Reporter {
    fn drop(&mut self) {
        #[cfg(unix)]
        self.task.abort();
        if let Err(error) = write_snapshot() {
            eprintln!("stage timing final snapshot failed: {error}");
        }
    }
}
