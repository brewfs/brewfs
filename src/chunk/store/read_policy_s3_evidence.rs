//! Opt-in physical HTTP evidence, using the dedicated four-drive compose fixture.
use super::*;
use crate::cadapter::read_observer::{Engine, Ledger, Phase, ReadObserver};
use crate::cadapter::s3::{RustfsEcBlockSizeHint, S3Backend, S3Config};
use crate::chunk::compress::encode_persisted_block;

fn http_totals(observer: &ReadObserver) -> (u64, u64) {
    let snapshot = observer.snapshot();
    assert!(!snapshot.overflowed);
    snapshot
        .rows
        .iter()
        .filter(|((ledger, _), _)| *ledger == Ledger::HttpAttempt)
        .fold((0, 0), |(gets, bytes), (_, row)| {
            assert!(row.conserved());
            assert_eq!((row.failed, row.cancelled, row.inflight), (0, 0, 0));
            (gets + row.started, bytes + row.received)
        })
}

#[tokio::test]
#[ignore = "requires dedicated RustFS compose fixture and BREWFS_EC_SPARSE_EVIDENCE output path"]
async fn ec_sparse_http_evidence() -> anyhow::Result<()> {
    let output = std::path::PathBuf::from(std::env::var("BREWFS_EC_SPARSE_EVIDENCE")?);
    let endpoint = std::env::var("BREWFS_S3_ENDPOINT")?;
    let bucket = std::env::var("BREWFS_S3_BUCKET")?;
    let backend = S3Backend::with_static_credentials(
        S3Config {
            bucket,
            endpoint: Some(endpoint),
            region: Some("us-east-1".into()),
            force_path_style: true,
            rustfs_ec_block_size_hint: Some(RustfsEcBlockSizeHint::Fixed(262_144)),
            ..Default::default()
        },
        "rustfsadmin".into(),
        "rustfsadmin".into(),
    )
    .await?;
    let block_size = 4 << 20;
    let mut state = 0x12345678u32;
    let raw: Vec<u8> = (0..block_size)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state as u8
        })
        .collect();
    let compressed = vec![37; block_size];
    let fixtures = [
        (8101, raw.as_slice(), Compression::None),
        (8102, compressed.as_slice(), Compression::Lz4),
        (8103, compressed.as_slice(), Compression::Zstd(1)),
        (8104, &raw[..block_size - 17], Compression::None),
    ];
    for (id, bytes, compression) in fixtures {
        backend
            .put_object(
                &format!("chunks-v2/{id}/0"),
                &encode_persisted_block(bytes, compression),
            )
            .await?;
    }
    // Fresh store/data caches per sample; server disk/page caches are warm.
    // The warm-format arm resolves only the four-byte header before timing.
    let cases = [
        ("known-4k", 8101, 128 << 10, 4096, true),
        ("cold-4k", 8101, 128 << 10, 4096, false),
        ("known-aligned-64k", 8101, 128 << 10, 65536, true),
        ("complete-object", 8101, 0, block_size, false),
        ("lz4-4k", 8102, 128 << 10, 4096, true),
        ("zstd-4k", 8103, 128 << 10, 4096, true),
        ("tail-4k", 8104, block_size - 17 - 4096, 4096, true),
    ];
    let mut rows = Vec::new();
    for trial in 0..3 {
        let policies = if trial == 1 {
            [ReadFetchPolicy::Sparse, ReadFetchPolicy::BackendDefault]
        } else {
            [ReadFetchPolicy::BackendDefault, ReadFetchPolicy::Sparse]
        };
        for policy in policies {
            for (name, id, offset, size, known) in cases {
                let mut latencies = Vec::new();
                let (mut gets, mut body_bytes, mut hits, mut repeat_gets) = (0, 0, 0, 0);
                let (mut miss_hits, mut repeat_body_bytes) = (0, 0);
                for _ in 0..64 {
                    let cache = tempfile::tempdir()?;
                    let observer = Arc::new(ReadObserver::default());
                    let client = ObjectClient::new(backend.clone()).with_read_observer(
                        observer.clone(),
                        Engine::Native,
                        Phase::Runtime,
                        Origin::Demand,
                    );
                    let store = ObjectBlockStore::new_with_configs_async(
                        client,
                        ChunksCacheConfig::with_budgets(8 << 20, 0, cache.path().to_path_buf()),
                        BlockStoreConfig {
                            compression: Compression::None,
                            ..Default::default()
                        },
                    )
                    .await?
                    .with_read_fetch_policy(policy);
                    if known {
                        store.resolve_object_layout((id, 0)).await?;
                    }
                    let before = http_totals(&observer);
                    let before_stats = store.object_metrics.snapshot();
                    let mut bytes = vec![0; size];
                    let started = Instant::now();
                    store.read_range((id, 0), offset as u64, &mut bytes).await?;
                    latencies.push(started.elapsed().as_secs_f64());
                    let expected = if id == 8102 || id == 8103 {
                        &compressed
                    } else {
                        &raw
                    };
                    assert_eq!(bytes, expected[offset..offset + size]);
                    let after = http_totals(&observer);
                    assert!(observer.snapshot().http_observed);
                    gets += after.0 - before.0;
                    body_bytes += after.1 - before.1;
                    let after_stats = store.object_metrics.snapshot();
                    miss_hits += after_stats.read_block_cache_hits
                        + after_stats.read_page_cache_hits
                        - before_stats.read_block_cache_hits
                        - before_stats.read_page_cache_hits;
                    assert_eq!(
                        store.object_metrics.snapshot().read_background_prefetches,
                        0
                    );
                    store.read_range((id, 0), offset as u64, &mut bytes).await?;
                    assert_eq!(bytes, expected[offset..offset + size]);
                    let stats = store.object_metrics.snapshot();
                    hits += stats.read_block_cache_hits + stats.read_page_cache_hits
                        - after_stats.read_block_cache_hits
                        - after_stats.read_page_cache_hits;
                    repeat_gets += http_totals(&observer).0 - after.0;
                    repeat_body_bytes += http_totals(&observer).1 - after.1;
                }
                let seconds: f64 = latencies.iter().sum();
                latencies.sort_by(f64::total_cmp);
                let row = serde_json::json!({
                    "trial": trial + 1, "policy": format!("{policy:?}"), "case": name,
                    "samples": latencies.len(), "logical_bytes_per_read": size,
                    "http_body_bytes": body_bytes, "http_gets": gets,
                    "http_body_bytes_per_miss": body_bytes / 64, "http_gets_per_miss": gets / 64,
                    "miss_cache_hits": miss_hits, "repeat_cache_hits": hits, "repeat_gets": repeat_gets,
                    "repeat_http_body_bytes": repeat_body_bytes,
                    "logical_mib_s": (size * 64) as f64 / seconds / 1048576.0,
                    "p95_ms": latencies[(latencies.len() * 95).div_ceil(100) - 1] * 1000.0,
                    "p99_ms": latencies[(latencies.len() * 99).div_ceil(100) - 1] * 1000.0,
                    "miss_latency_seconds": latencies,
                });
                println!(
                    "{name} {policy:?} trial {}: {} bytes, {} GETs",
                    trial + 1,
                    body_bytes,
                    gets
                );
                rows.push(row);
                std::fs::write(&output, serde_json::to_vec_pretty(&rows)?)?;
            }
        }
    }
    Ok(())
}
