//! Terminal rates use deltas, including histogram buckets, between scrapes.
use std::{
    collections::BTreeMap,
    fmt::Write,
    io::IsTerminal,
    net::SocketAddr,
    time::{Duration, Instant},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, clap::Args)]
pub struct StatsArgs {
    /// Address of the mount's enabled /metrics endpoint.
    #[arg(long, default_value = "127.0.0.1:9567")]
    pub address: SocketAddr,
    /// Seconds between updates; must be finite and greater than zero.
    #[arg(long, short = 'i', default_value_t = 1.0)]
    pub interval: f64,
    /// Exit after this many updates (otherwise run until Ctrl-C).
    #[arg(long)]
    pub count: Option<usize>,
}

type Snapshot = BTreeMap<String, f64>;
fn parse(text: &str) -> Snapshot {
    text.lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| {
            let (key, value) = line.rsplit_once(' ')?;
            Some((key.to_owned(), value.parse().ok()?))
        })
        .collect()
}
fn delta(previous: &Snapshot, current: &Snapshot, key: &str) -> f64 {
    let current = current.get(key).copied().unwrap_or(0.0);
    let previous = previous.get(key).copied().unwrap_or(0.0);
    (current - previous).max(0.0)
}
fn total_delta(previous: &Snapshot, current: &Snapshot, prefix: &str) -> f64 {
    current
        .keys()
        .filter(|key| *key == prefix || key.starts_with(&format!("{prefix}{{")))
        .map(|key| delta(previous, current, key))
        .sum()
}

fn quantile(buckets: &[(f64, f64)], q: f64) -> Option<f64> {
    let count = buckets.last()?.1;
    if count <= 0.0 {
        return None;
    }
    let rank = count * q;
    let (mut lower, mut previous) = (0.0, 0.0);
    for &(upper, cumulative) in buckets {
        if cumulative >= rank {
            if !upper.is_finite() {
                return Some(lower);
            }
            if cumulative <= previous {
                return Some(upper);
            }
            return Some(lower + (upper - lower) * (rank - previous) / (cumulative - previous));
        }
        lower = upper;
        previous = cumulative;
    }
    None
}

fn latency(previous: &Snapshot, current: &Snapshot, op: &str, q: f64) -> String {
    let count_key = format!("brewfs_fuse_op_latency_seconds_count{{op=\"{op}\"}}");
    if current.get(&count_key) < previous.get(&count_key) {
        return "--".into();
    }
    let prefix = format!("brewfs_fuse_op_latency_seconds_bucket{{op=\"{op}\",le=\"");
    let mut buckets: Vec<_> = current
        .keys()
        .filter_map(|key| {
            let bound = key.strip_prefix(&prefix)?.strip_suffix("\"}")?;
            let upper = if bound == "+Inf" {
                f64::INFINITY
            } else {
                bound.parse().ok()?
            };
            Some((upper, delta(previous, current, key)))
        })
        .collect();
    buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
    quantile(&buckets, q).map_or_else(
        || "--".into(),
        |seconds| format!("{:.3}", seconds * 1_000.0),
    )
}

fn table(previous: &Snapshot, current: &Snapshot, elapsed: f64) -> String {
    let mut out = String::from("OP                       QPS     P50 ms~     P99 ms~\n");
    for op in super::operations::FUSE_OPS {
        let key = format!("brewfs_fuse_op_total{{op=\"{op}\"}}");
        let count = delta(previous, current, &key);
        if count > 0.0 {
            let _ = writeln!(
                out,
                "{op:<22} {:>7.1} {:>11} {:>11}",
                count / elapsed,
                latency(previous, current, op, 0.5),
                latency(previous, current, op, 0.99)
            );
        }
    }
    for op in ["get", "put"] {
        let requests =
            total_delta(previous, current, &format!("brewfs_object_{op}_total")) / elapsed;
        let mib = total_delta(
            previous,
            current,
            &format!("brewfs_object_{op}_bytes_total"),
        ) / elapsed
            / 1_048_576.0;
        let _ = writeln!(
            out,
            "Object {op:<3}: {requests:>10.1} req/s {mib:>10.2} MiB/s"
        );
    }
    for cache in ["block", "page", "meta"] {
        let hits = delta(
            previous,
            current,
            &format!("brewfs_cache_hits_total{{cache=\"{cache}\"}}"),
        );
        let misses = delta(
            previous,
            current,
            &format!("brewfs_cache_misses_total{{cache=\"{cache}\"}}"),
        );
        let ratio = if hits + misses > 0.0 {
            format!("{:.1}%", 100.0 * hits / (hits + misses))
        } else {
            "--".into()
        };
        let _ = writeln!(out, "Cache {cache:<5}: {ratio:>8} hit");
    }
    for name in ["dirty", "pending_upload", "inflight"] {
        let bytes = current
            .get(&format!("brewfs_writeback_{name}_bytes"))
            .copied()
            .unwrap_or(0.0);
        let _ = writeln!(out, "Debt {name:<14}: {bytes:.0} bytes");
    }
    out
}

async fn scrape(address: SocketAddr) -> anyhow::Result<Snapshot> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut stream = tokio::net::TcpStream::connect(address).await?;
        stream
            .write_all(b"GET /metrics HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await?;
        let mut text = String::new();
        stream
            .take(4 * 1024 * 1024)
            .read_to_string(&mut text)
            .await?;
        let (headers, body) = text
            .split_once("\r\n\r\n")
            .ok_or_else(|| anyhow::anyhow!("invalid HTTP metrics response"))?;
        anyhow::ensure!(
            headers.starts_with("HTTP/1.0 200 ") || headers.starts_with("HTTP/1.1 200 "),
            "metrics endpoint returned {}",
            headers.lines().next().unwrap_or("invalid status")
        );
        anyhow::ensure!(
            text.len() < 4 * 1024 * 1024,
            "metrics response exceeds 4 MiB"
        );
        let snapshot = parse(body);
        anyhow::ensure!(
            snapshot
                .keys()
                .any(|key| key.starts_with("brewfs_mount_info{")),
            "endpoint does not contain BrewFS metrics"
        );
        Ok(snapshot)
    })
    .await?
}

pub async fn run(args: StatsArgs) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.interval.is_finite() && args.interval > 0.0 && args.interval <= 86_400.0,
        "interval must be in (0, 86400] seconds"
    );
    if args.count == Some(0) {
        return Ok(());
    }
    let interval = Duration::from_secs_f64(args.interval);
    let mut previous = scrape(args.address).await?;
    let mut last = Instant::now();
    let mut updates = 0;
    let signal = tokio::signal::ctrl_c();
    tokio::pin!(signal);
    loop {
        tokio::select! { result = &mut signal => { result?; return Ok(()); }, _ = tokio::time::sleep(interval) => {} }
        let current = tokio::select! { result = &mut signal => { result?; return Ok(()); }, snapshot = scrape(args.address) => snapshot? };
        let now = Instant::now();
        if std::io::stdout().is_terminal() {
            print!("\x1b[2J\x1b[H");
        }
        println!(
            "{}",
            table(&previous, &current, now.duration_since(last).as_secs_f64())
        );
        previous = current;
        last = now;
        updates += 1;
        if args.count.is_some_and(|count| updates >= count) {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn quantiles_use_interval_buckets_and_handle_overflow_and_idle() {
        assert_eq!(
            quantile(&[(0.001, 50.0), (0.01, 100.0), (f64::INFINITY, 100.0)], 0.5),
            Some(0.001)
        );
        assert!(
            (quantile(
                &[(0.001, 50.0), (0.01, 100.0), (f64::INFINITY, 100.0)],
                0.99
            )
            .unwrap()
                - 0.00982)
                .abs()
                < 1e-12
        );
        assert_eq!(
            quantile(&[(0.01, 0.0), (f64::INFINITY, 100.0)], 0.99),
            Some(0.01)
        );
        assert_eq!(quantile(&[(0.01, 0.0), (f64::INFINITY, 0.0)], 0.5), None);
        let before = parse(
            "brewfs_object_get_total{kind=\"upper\"} 10\nbrewfs_object_get_total{kind=\"packed\"} 20\n",
        );
        let after = parse(
            "brewfs_object_get_total{kind=\"upper\"} 15\nbrewfs_object_get_total{kind=\"packed\"} 25\n",
        );
        assert_eq!(
            total_delta(&before, &after, "brewfs_object_get_total"),
            10.0
        );
        assert_eq!(total_delta(&after, &before, "brewfs_object_get_total"), 0.0);
    }
}
