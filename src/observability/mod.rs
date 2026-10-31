//! Optional process-scoped instrumentation for the CLI's single mount.
//! This entire module is absent unless `observability` is compiled in.
pub(crate) mod fuse;
mod histogram;
mod operations;
pub mod stats;

use crate::vfs::stats::FsStats;
use axum::{Router, extract::State, http::header, routing::get};
use histogram::{Histogram, META, READ, WRITE};
pub(crate) use operations::MetadataOp;
use operations::{FUSE_OPS, META_OPS};
use parking_lot::Mutex;
use std::{
    collections::BTreeMap,
    fmt::Write,
    net::SocketAddr,
    sync::{
        Arc, OnceLock, Weak,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::Instant,
};

static ACTIVE: OnceLock<Arc<Metrics>> = OnceLock::new();

pub(crate) struct Metrics {
    fuse: Vec<Histogram>,
    errors: Mutex<BTreeMap<(usize, i32), u64>>,
    metadata: Vec<Vec<Histogram>>,
    object: [Histogram; 2],
    get_count: [AtomicU64; 3],
    get_bytes: [AtomicU64; 3],
    put_count: AtomicU64,
    put_bytes: AtomicU64,
    stats: Mutex<Option<Weak<FsStats>>>,
    profile: String,
    backend: String,
    start: Instant,
}

const BACKENDS: &[&str] = &["sqlx", "redis", "etcd", "tikv", "other"];
const KINDS: &[&str] = &["upper", "packed", "large"];

impl Metrics {
    fn record_fuse(&self, op: usize, elapsed: std::time::Duration, errno: Option<i32>) {
        self.fuse[op].observe(elapsed);
        if let Some(errno) = errno {
            *self.errors.lock().entry((op, errno.abs())).or_default() += 1;
        }
    }
    fn new(profile: String, backend: String) -> Self {
        Self {
            fuse: FUSE_OPS
                .iter()
                .map(|op| {
                    Histogram::new(match *op {
                        "write" | "flush" | "fsync" | "release" | "fallocate"
                        | "copy_file_range" => WRITE,
                        "read" | "readlink" | "readdir" | "readdirplus" => READ,
                        _ => META,
                    })
                })
                .collect(),
            errors: Mutex::new(BTreeMap::new()),
            metadata: BACKENDS
                .iter()
                .map(|_| META_OPS.iter().map(|_| Histogram::new(META)).collect())
                .collect(),
            object: [Histogram::new(READ), Histogram::new(WRITE)],
            get_count: std::array::from_fn(|_| AtomicU64::new(0)),
            get_bytes: std::array::from_fn(|_| AtomicU64::new(0)),
            put_count: AtomicU64::new(0),
            put_bytes: AtomicU64::new(0),
            stats: Mutex::new(None),
            profile,
            backend,
            start: Instant::now(),
        }
    }

    fn render(&self) -> String {
        let mut out = String::with_capacity(48 * 1024);
        descriptor(
            &mut out,
            "brewfs_fuse_op_total",
            "counter",
            "Completed FUSE handler requests including errors.",
        );
        descriptor(
            &mut out,
            "brewfs_fuse_op_latency_seconds",
            "histogram",
            "FUSE handler duration, including failed requests.",
        );
        for (index, op) in FUSE_OPS.iter().enumerate() {
            let labels = format!("op=\"{op}\"");
            let (buckets, sum) = self.fuse[index].snapshot();
            sample(
                &mut out,
                "brewfs_fuse_op_total",
                &labels,
                *buckets.last().unwrap(),
            );
            histogram(
                &mut out,
                "brewfs_fuse_op_latency_seconds",
                &labels,
                &self.fuse[index],
                &buckets,
                sum,
            );
        }
        descriptor(
            &mut out,
            "brewfs_fuse_op_errors_total",
            "counter",
            "Failed FUSE handlers by positive errno.",
        );
        for ((op, errno), count) in self.errors.lock().iter() {
            sample(
                &mut out,
                "brewfs_fuse_op_errors_total",
                &format!("op=\"{}\",errno=\"{errno}\"", FUSE_OPS[*op]),
                count,
            );
        }
        for name in [
            "brewfs_object_get_total",
            "brewfs_object_get_bytes_total",
            "brewfs_object_put_total",
            "brewfs_object_put_bytes_total",
        ] {
            descriptor(
                &mut out,
                name,
                "counter",
                "Object client requests or successfully transferred body bytes.",
            );
        }
        for (i, kind) in KINDS.iter().enumerate() {
            let labels = format!("kind=\"{kind}\"");
            sample(
                &mut out,
                "brewfs_object_get_total",
                &labels,
                self.get_count[i].load(Relaxed),
            );
            sample(
                &mut out,
                "brewfs_object_get_bytes_total",
                &labels,
                self.get_bytes[i].load(Relaxed),
            );
        }
        sample(
            &mut out,
            "brewfs_object_put_total",
            "",
            self.put_count.load(Relaxed),
        );
        sample(
            &mut out,
            "brewfs_object_put_bytes_total",
            "",
            self.put_bytes.load(Relaxed),
        );
        descriptor(
            &mut out,
            "brewfs_object_op_latency_seconds",
            "histogram",
            "Object call duration; streaming GET includes body consumption or cancellation.",
        );
        for (i, op) in ["get", "put"].iter().enumerate() {
            let (buckets, sum) = self.object[i].snapshot();
            histogram(
                &mut out,
                "brewfs_object_op_latency_seconds",
                &format!("op=\"{op}\""),
                &self.object[i],
                &buckets,
                sum,
            );
        }
        descriptor(
            &mut out,
            "brewfs_metadata_op_total",
            "counter",
            "Metadata store API calls completed, including errors and cancellation.",
        );
        descriptor(
            &mut out,
            "brewfs_metadata_op_latency_seconds",
            "histogram",
            "Metadata store API duration, including transaction and network waits.",
        );
        for (backend, histograms) in BACKENDS.iter().zip(&self.metadata) {
            for (op, hist) in META_OPS.iter().zip(histograms) {
                let (buckets, sum) = hist.snapshot();
                if *buckets.last().unwrap() == 0 && *backend != self.backend {
                    continue;
                }
                let labels = format!("op=\"{op}\",backend=\"{backend}\"");
                sample(
                    &mut out,
                    "brewfs_metadata_op_total",
                    &labels,
                    *buckets.last().unwrap(),
                );
                histogram(
                    &mut out,
                    "brewfs_metadata_op_latency_seconds",
                    &labels,
                    hist,
                    &buckets,
                    sum,
                );
            }
        }
        descriptor(
            &mut out,
            "brewfs_cache_hits_total",
            "counter",
            "Userspace cache hits; page means the BrewFS read-page cache.",
        );
        descriptor(
            &mut out,
            "brewfs_cache_misses_total",
            "counter",
            "Userspace cache misses.",
        );
        let snapshot = self
            .stats
            .lock()
            .as_ref()
            .and_then(Weak::upgrade)
            .map(|s| s.snapshot());
        if let Some(s) = snapshot {
            for (cache, hits, misses) in [
                ("block", s.cache_hits, s.cache_misses),
                ("page", s.read_page_cache_hits, s.read_page_cache_misses),
                (
                    "meta",
                    s.meta_stat_cache_hit
                        + s.meta_lookup_cache_hit
                        + s.meta_get_slices_cache_hit
                        + s.meta_open_file_cache_hit,
                    s.meta_stat_cache_miss
                        + s.meta_lookup_cache_miss
                        + s.meta_get_slices_cache_miss
                        + s.meta_open_file_cache_miss,
                ),
            ] {
                let labels = format!("cache=\"{cache}\"");
                sample(&mut out, "brewfs_cache_hits_total", &labels, hits);
                sample(&mut out, "brewfs_cache_misses_total", &labels, misses);
            }
            for (name, value) in [
                ("brewfs_writeback_dirty_bytes", s.buf_dirty_bytes),
                (
                    "brewfs_writeback_pending_upload_bytes",
                    s.writeback_recent_pending_upload_bytes,
                ),
                (
                    "brewfs_writeback_inflight_bytes",
                    s.writeback_remote_upload_inflight_bytes,
                ),
            ] {
                descriptor(
                    &mut out,
                    name,
                    "gauge",
                    "Writeback debt bytes; categories can overlap and must not be summed.",
                );
                sample(&mut out, name, "", value);
            }
        }
        descriptor(
            &mut out,
            "brewfs_mount_info",
            "gauge",
            "Mount build, volume profile and metadata backend.",
        );
        sample(
            &mut out,
            "brewfs_mount_info",
            &format!(
                "version=\"{}\",profile=\"{}\",backend=\"{}\"",
                env!("CARGO_PKG_VERSION"),
                escape(&self.profile),
                escape(&self.backend)
            ),
            1,
        );
        descriptor(
            &mut out,
            "brewfs_mount_uptime_seconds",
            "gauge",
            "Seconds since observability startup.",
        );
        sample(
            &mut out,
            "brewfs_mount_uptime_seconds",
            "",
            self.start.elapsed().as_secs_f64(),
        );
        out
    }
}

fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}
fn descriptor(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} {kind}");
}
fn sample(out: &mut String, name: &str, labels: &str, value: impl std::fmt::Display) {
    if labels.is_empty() {
        let _ = writeln!(out, "{name} {value}");
    } else {
        let _ = writeln!(out, "{name}{{{labels}}} {value}");
    }
}
fn histogram(
    out: &mut String,
    name: &str,
    labels: &str,
    hist: &Histogram,
    buckets: &[u64],
    sum: f64,
) {
    for (bound, count) in hist.bounds.iter().zip(buckets) {
        sample(
            out,
            &format!("{name}_bucket"),
            &format!("{labels},le=\"{}\"", *bound as f64 / 1e6),
            count,
        );
    }
    sample(
        out,
        &format!("{name}_bucket"),
        &format!("{labels},le=\"+Inf\""),
        buckets.last().unwrap(),
    );
    sample(
        out,
        &format!("{name}_count"),
        labels,
        buckets.last().unwrap(),
    );
    sample(out, &format!("{name}_sum"), labels, sum);
}

pub struct FuseTimer {
    metrics: &'static Metrics,
    op: usize,
    start: Instant,
}
impl FuseTimer {
    pub fn finish(self, errno: Option<i32>) {
        self.metrics
            .record_fuse(self.op, self.start.elapsed(), errno);
    }
}
#[inline]
pub(crate) fn fuse_timer(op: operations::FuseOp) -> Option<FuseTimer> {
    ACTIVE.get().map(|metrics| FuseTimer {
        metrics,
        op: op as usize,
        start: Instant::now(),
    })
}
pub struct MetadataTimer {
    histogram: &'static Histogram,
    start: Instant,
}
impl Drop for MetadataTimer {
    fn drop(&mut self) {
        self.histogram.observe(self.start.elapsed());
    }
}
#[inline]
pub(crate) fn metadata_timer(op: MetadataOp, backend: &str) -> Option<MetadataTimer> {
    let backend = match backend {
        "database" => 0,
        "redis-meta-store" => 1,
        "etcd" => 2,
        "tikv" => 3,
        _ => 4,
    };
    ACTIVE.get().map(|metrics| MetadataTimer {
        histogram: &metrics.metadata[backend][op as usize],
        start: Instant::now(),
    })
}

pub(crate) fn attach(stats: &Arc<FsStats>) {
    if let Some(metrics) = ACTIVE.get() {
        *metrics.stats.lock() = Some(Arc::downgrade(stats));
    }
}

pub(crate) struct ObjectTimer {
    metrics: Arc<Metrics>,
    op: usize,
    kind: usize,
    start: Instant,
}
impl ObjectTimer {
    pub(crate) fn start(put: bool, kind: usize) -> Option<Self> {
        ACTIVE
            .get()
            .map(|metrics| Self::new(Arc::clone(metrics), put, kind))
    }
    fn new(metrics: Arc<Metrics>, put: bool, kind: usize) -> Self {
        if put {
            metrics.put_count.fetch_add(1, Relaxed);
        } else {
            metrics.get_count[kind].fetch_add(1, Relaxed);
        }
        Self {
            metrics,
            op: usize::from(put),
            kind,
            start: Instant::now(),
        }
    }
    pub(crate) fn bytes(&self, bytes: usize) {
        if self.op == 1 {
            self.metrics.put_bytes.fetch_add(bytes as u64, Relaxed);
        } else {
            self.metrics.get_bytes[self.kind].fetch_add(bytes as u64, Relaxed);
        }
    }
}
impl Drop for ObjectTimer {
    fn drop(&mut self) {
        self.metrics.object[self.op].observe(self.start.elapsed());
    }
}

/// Holds the timer through EOF, a body error, or cancellation, without buffering.
pub(crate) fn observe_stream(
    stream: crate::cadapter::client::ObjectByteStream,
    timer: Option<ObjectTimer>,
) -> crate::cadapter::client::ObjectByteStream {
    use futures_util::StreamExt;
    let Some(timer) = timer else {
        return stream;
    };
    Box::pin(futures_util::stream::unfold(
        Some((stream, timer)),
        |state| async move {
            let (mut stream, timer) = state?;
            match stream.next().await {
                Some(Ok(bytes)) => {
                    timer.bytes(bytes.len());
                    Some((Ok(bytes), Some((stream, timer))))
                }
                Some(Err(error)) => Some((Err(error), Some((stream, timer)))),
                None => None,
            }
        },
    ))
}

pub struct Server {
    task: tokio::task::JoinHandle<()>,
    pub address: SocketAddr,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn serve(address: SocketAddr, metrics: Arc<Metrics>) -> anyhow::Result<Server> {
    let listener = tokio::net::TcpListener::bind(address).await?;
    let address = listener.local_addr()?;
    let app = Router::new()
        .route(
            "/metrics",
            get(|State(metrics): State<Arc<Metrics>>| async move {
                (
                    [(
                        header::CONTENT_TYPE,
                        "text/plain; version=0.0.4; charset=utf-8",
                    )],
                    metrics.render(),
                )
            }),
        )
        .with_state(metrics);
    let task = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            tracing::error!(%error, "metrics server stopped");
        }
    });
    Ok(Server { task, address })
}

pub async fn start(
    enabled: bool,
    address: SocketAddr,
    profile: String,
    backend: String,
) -> anyhow::Result<Option<Server>> {
    if !enabled {
        return Ok(None);
    }
    let metrics = Arc::new(Metrics::new(profile, backend));
    let server = serve(address, metrics.clone()).await?;
    ACTIVE
        .set(metrics)
        .map_err(|_| anyhow::anyhow!("observability already initialized for this process"))?;
    tracing::info!(address = %server.address, "Prometheus metrics enabled");
    Ok(Some(server))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    #[tokio::test]
    async fn stream_preserves_chunks_errors_and_counts_consumed_bytes_on_cancel() {
        use bytes::Bytes;
        use futures_util::StreamExt;
        let metrics = Arc::new(Metrics::new("flatv1".into(), "redis".into()));
        let timer = ObjectTimer::new(metrics.clone(), false, 2);
        let input = futures_util::stream::iter(vec![
            Ok(Bytes::from_static(b"abc")),
            Err(anyhow::anyhow!("body error")),
            Ok(Bytes::from_static(b"defg")),
        ]);
        let mut stream = observe_stream(Box::pin(input), Some(timer));
        assert_eq!(metrics.get_bytes[2].load(Relaxed), 0);
        assert_eq!(stream.next().await.unwrap().unwrap(), "abc");
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().to_string(),
            "body error"
        );
        assert_eq!(stream.next().await.unwrap().unwrap(), "defg");
        assert!(stream.next().await.is_none());
        assert_eq!(metrics.get_count[2].load(Relaxed), 1);
        assert_eq!(metrics.get_bytes[2].load(Relaxed), 7);
        assert_eq!(metrics.object[0].snapshot().0.last(), Some(&1));

        let timer = ObjectTimer::new(metrics.clone(), false, 2);
        let input = futures_util::stream::iter(vec![Ok(Bytes::from_static(b"hi"))])
            .chain(futures_util::stream::pending());
        let mut stream = observe_stream(Box::pin(input), Some(timer));
        assert_eq!(stream.next().await.unwrap().unwrap(), "hi");
        drop(stream);
        assert_eq!(metrics.get_count[2].load(Relaxed), 2);
        assert_eq!(metrics.get_bytes[2].load(Relaxed), 9);
        assert_eq!(metrics.object[0].snapshot().0.last(), Some(&2));
    }
    #[test]
    fn exposition_has_counters_errors_and_exact_histogram_counts() {
        let metrics = Metrics::new("flatv1".into(), "redis".into());
        let read = FUSE_OPS.iter().position(|op| *op == "read").unwrap();
        for i in 0..100 {
            metrics.record_fuse(
                read,
                std::time::Duration::from_micros(100),
                (i < 7).then_some(libc::EIO),
            );
        }
        let rendered = metrics.render();
        assert!(rendered.contains("brewfs_fuse_op_total{op=\"read\"} 100\n"));
        assert!(rendered.contains("brewfs_fuse_op_errors_total{op=\"read\",errno=\"5\"} 7\n"));
        assert!(
            rendered
                .contains("brewfs_fuse_op_latency_seconds_bucket{op=\"read\",le=\"0.0001\"} 100\n")
        );
        assert!(
            rendered
                .contains("brewfs_fuse_op_latency_seconds_bucket{op=\"read\",le=\"+Inf\"} 100\n")
        );
        assert!(rendered.contains("brewfs_fuse_op_latency_seconds_sum{op=\"read\"} 0.01\n"));
        assert!(rendered.contains("brewfs_metadata_op_total{op=\"stat\",backend=\"redis\"} 0\n"));
        let mut samples = std::collections::BTreeSet::new();
        for line in rendered.lines().filter(|line| !line.starts_with('#')) {
            assert!(
                samples.insert(line.rsplit_once(' ').unwrap().0),
                "duplicate sample: {line}"
            );
        }
    }
    #[tokio::test]
    async fn disabled_does_not_bind_and_enabled_serves_prometheus() {
        let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        assert!(
            start(
                false,
                occupied.local_addr().unwrap(),
                "flat".into(),
                "redis".into()
            )
            .await
            .unwrap()
            .is_none()
        );
        let metrics = Arc::new(Metrics::new("flat\"\\\n".into(), "redis".into()));
        let server = serve("127.0.0.1:0".parse().unwrap(), metrics)
            .await
            .unwrap();
        let mut stream = tokio::net::TcpStream::connect(server.address)
            .await
            .unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("text/plain; version=0.0.4"));
        assert!(response.contains("# TYPE brewfs_fuse_op_latency_seconds histogram"));
        assert!(
            response.contains("brewfs_fuse_op_latency_seconds_bucket{op=\"read\",le=\"+Inf\"} 0")
        );
        assert!(response.contains("profile=\"flat\\\"\\\\\\n\""));
        let address = server.address;
        drop(server);
        tokio::task::yield_now().await;
        assert!(tokio::net::TcpListener::bind(address).await.is_ok());
    }
}
