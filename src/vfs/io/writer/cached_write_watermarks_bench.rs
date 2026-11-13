//! Explicit local comparison of the actual FileWriter cached-write path.
//! The old algorithm is test-only, so both arms run in one binary with the
//! same data, store, flush configuration and memory resources.
use super::*;
use crate::chunk::ChunkLayout;
use crate::chunk::store::InMemoryBlockStore;
use crate::meta::factory::create_meta_store_from_url;
use crate::vfs::config::ReadConfig;

#[derive(Clone, Copy)]
pub(super) struct LegacyRange {
    start: u64,
    end: u64,
    max_unique: u64,
}

pub(super) fn legacy_allow_and_record(
    ranges: &mut Vec<LegacyRange>,
    start: u64,
    end: u64,
    unique: u64,
) -> Vec<(u64, usize)> {
    let mut allowed = crate::utils::Intervals::new(start, end);
    for range in ranges.iter() {
        if range.max_unique > unique {
            allowed.cut(range.start.max(start), range.end.min(end));
        }
    }
    let allowed = allowed
        .collect()
        .into_iter()
        .map(|(start, end)| (start, (end - start).as_usize()))
        .collect();
    record_legacy_watermark(ranges, start, end, unique);
    allowed
}

fn record_legacy_watermark(ranges: &mut Vec<LegacyRange>, start: u64, end: u64, unique: u64) {
    if unique == 0 || start >= end {
        return;
    }

    let mut boundaries = vec![start, end];
    for range in ranges.iter() {
        if range.end <= start || range.start >= end {
            continue;
        }
        boundaries.push(range.start.max(start));
        boundaries.push(range.end.min(end));
    }
    boundaries.sort_unstable();
    boundaries.dedup();

    let mut next = Vec::with_capacity(ranges.len() + boundaries.len());
    for range in ranges.iter().copied() {
        if range.end <= start || range.start >= end {
            next.push(range);
            continue;
        }
        if range.start < start {
            next.push(LegacyRange {
                start: range.start,
                end: start,
                max_unique: range.max_unique,
            });
        }
        if range.end > end {
            next.push(LegacyRange {
                start: end,
                end: range.end,
                max_unique: range.max_unique,
            });
        }
    }

    for window in boundaries.windows(2) {
        let seg_start = window[0];
        let seg_end = window[1];
        if seg_start >= seg_end {
            continue;
        }
        let mut max_unique = unique;
        for range in ranges.iter() {
            if range.start < seg_end && seg_start < range.end {
                max_unique = max_unique.max(range.max_unique);
            }
        }
        next.push(LegacyRange {
            start: seg_start,
            end: seg_end,
            max_unique,
        });
    }

    next.sort_by_key(|range| (range.start, range.end));
    let mut merged: Vec<LegacyRange> = Vec::with_capacity(next.len());
    for range in next {
        if range.start >= range.end {
            continue;
        }
        if let Some(last) = merged.last_mut()
            && last.end == range.start
            && last.max_unique == range.max_unique
        {
            last.end = range.end;
            continue;
        }
        merged.push(range);
    }
    *ranges = merged;
}

fn process_cpu_ns() -> u128 {
    let mut value = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // This explicitly invoked benchmark is Linux-only; the clock includes all
    // worker threads, unlike a timer that only accounts for the caller thread.
    assert_eq!(
        unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut value) },
        0
    );
    value.tv_sec as u128 * 1_000_000_000 + value.tv_nsec as u128
}

#[tokio::test]
#[ignore = "explicit same-binary FileWriter timing comparison; run with --ignored --nocapture --test-threads=1"]
async fn cached_watermark_filewriter_comparison() {
    let layout = ChunkLayout {
        chunk_size: 64 << 20,
        block_size: 4 << 20,
    };
    for count in [128usize, 1024, 4096] {
        // ABBAAB: each mode appears three times; stores/caches are fresh per arm.
        for (repeat, indexed) in [false, true, true, false, false, true]
            .into_iter()
            .enumerate()
        {
            let store = Arc::new(InMemoryBlockStore::new());
            let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
            let meta = meta_handle.layer();
            let backend = Arc::new(Backend::new(store, meta.clone()));
            let ino = meta
                .create_file(1, "watermark-comparison".into())
                .await
                .unwrap();
            let inode = Inode::new(ino, 0);
            inode.set_size((count * 4096) as u64);
            let reader = Arc::new(DataReader::new(
                Arc::new(ReadConfig::new(layout)),
                backend.clone(),
            ));
            let config = Arc::new(
                WriteConfig::new(layout)
                    .page_size(4096)
                    .freeze_min_bytes(32 << 20)
                    .auto_flush_max_age(Duration::from_secs(600))
                    .writeback_mode(WriteBackMode::UploadBeforeCommit)
                    .cached_block_assembler(false),
            );
            let writer = DataWriter::new(config, backend, reader.clone(), None);
            let file = writer.ensure_file(inode.clone());
            if !indexed {
                file.shared
                    .inner
                    .lock()
                    .await
                    .legacy_cached_write_watermarks = Some(BTreeMap::new());
            }
            let before_cpu = process_cpu_ns();
            let begin = Instant::now();
            for i in 0..count {
                file.write_at_cached((i * 4096) as u64, &[(i % 251) as u8; 4096], i as u64 + 1)
                    .await
                    .unwrap();
            }
            let write_ns = begin.elapsed().as_nanos();
            let after_write_cpu = process_cpu_ns();
            let begin_flush = Instant::now();
            file.flush().await.unwrap();
            let flush_ns = begin_flush.elapsed().as_nanos();
            let after_flush_cpu = process_cpu_ns();
            let bytes = reader
                .open_for_handle(inode, 42)
                .read(0, count * 4096)
                .await
                .unwrap();
            assert_eq!(bytes.len(), count * 4096);
            for (i, page) in bytes.as_chunks::<4096>().0.iter().enumerate() {
                assert!(page.iter().all(|&b| b == (i % 251) as u8));
            }
            println!(
                "WATERMARK_BENCH {}",
                serde_json::json!({
                    "mode":if indexed {"indexed"} else {"baseline"},"writes":count,
                    "request_bytes":4096,"repeat":repeat,"write_ns":write_ns,"flush_ns":flush_ns,
                    "write_cpu_ns":after_write_cpu-before_cpu,"flush_cpu_ns":after_flush_cpu-after_write_cpu,
                    "verified_bytes":bytes.len(),"backend":"in-memory blocks and sqlite metadata",
                    "chunk_bytes":64<<20,"block_bytes":4<<20,"page_bytes":4096,
                    "freeze_min_bytes":32<<20,"cached_block_assembler":false,
                    "debug_assertions":cfg!(debug_assertions)
                })
            );
            file.clear().await;
            writer.clear(u64::try_from(ino).unwrap()).await;
        }
    }
}
