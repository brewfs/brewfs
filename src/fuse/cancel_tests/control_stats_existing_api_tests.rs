//! Existing production adapter/VFS APIs; LocalFS body gate, not HTTP/kernel proof.
use super::*;

fn limits() -> V3BudgetLimits {
    let mut limits = V3BudgetLimits::default();
    limits.bytes[V3BudgetPool::Control as usize] = 32 << 10;
    limits.bytes[V3BudgetPool::Metadata as usize] = 64 << 20;
    limits
}

#[tokio::test]
async fn minimum_control_existing_api_pending_read_coexists_with_complete_stats_and_cancel() {
    let budget = V3MountBudget::new(limits()).unwrap();
    budget.validate_frame_capability(8 << 20).unwrap();
    let f = fixture_with_budget(budget).await;
    assert!(f.fs.meta_layer().install_v3_stats(f.fs.stats()));
    let idle = f.budget.state().used;
    let memo_bytes =
        crate::workspace_overlay::packed_v3::wire005::verify_memo_reservation_bytes().unwrap();
    assert!(memo_bytes <= 2 << 20, "verification index exceeds 2 MiB");

    // These are the original Session adapter hooks, with the actual Linux
    // open/read request body sizes. Guards span their real handler futures.
    let open_owner = Filesystem::reserve_request_memory(&f.fs, 8)
        .unwrap()
        .unwrap();
    let opened = Filesystem::open(&f.fs, request(1501), f.ino, libc::O_RDONLY as u32)
        .await
        .unwrap();
    drop(open_owner);
    // OPEN owns one client lease. Capture it before the first payload read
    // can initialize the independent mount-owned demand coordinator.
    let file_client_roots = f.budget.state().used[V3BudgetPool::Roots as usize]
        .checked_sub(idle[V3BudgetPool::Roots as usize])
        .expect("file OPEN reduced idle Roots");
    let requested_client_roots =
        crate::vfs::fuse_read_cancel::ReadRegistry::client_requested_layout_bytes()
            + (2 * std::mem::size_of::<usize>()
                + std::mem::size_of::<crate::workspace_overlay::packed_v3::wire005::V3OwnedPermit>(
                )) as u64;
    assert_eq!(file_client_roots, requested_client_roots);
    assert!(file_client_roots > 0);
    let read_owner = Filesystem::reserve_request_memory(&f.fs, 40)
        .unwrap()
        .unwrap();
    let fs = f.fs.clone();
    let ino = f.ino;
    let fh = opened.fh;
    f.gate.mode.store(1, Ordering::SeqCst);
    let reader = tokio::spawn(async move {
        let result = Filesystem::read(&fs, request(1502), ino, fh, 0, 8192).await;
        drop(read_owner);
        result
    });
    tokio::time::timeout(Duration::from_secs(2), f.gate.entered.notified())
        .await
        .expect("actual packed payload body did not become pending");
    assert!(!reader.is_finished());
    assert_eq!(f.gate.payload_ranges.lock().unwrap().len(), 1);
    assert_eq!(f.gate.dropped.load(Ordering::SeqCst), 0);
    let observer = f.observer.snapshot();
    for (ledger, class) in [
        (Ledger::BackendBody, ReadClass::PackedPayload),
        (Ledger::LogicalOperation, ReadClass::LogicalRead),
    ] {
        assert!(
            observer.rows.iter().any(|((kind, context), row)| {
                *kind == ledger && context.class == class && row.inflight > 0
            }),
            "the real adapter body/logical read was not inflight"
        );
    }

    // The production failure occurred before os.open could obtain its stats
    // snapshot. Do not bypass the real incoming request admission hook.
    let stats_roots_before_open = f.budget.state().used[V3BudgetPool::Roots as usize];
    let mount_roots_after_payload_init = stats_roots_before_open
        .checked_sub(file_client_roots)
        .expect("pending payload lost its live file client Roots");
    assert!(mount_roots_after_payload_init > idle[V3BudgetPool::Roots as usize]);
    eprintln!(
        "actual_controlstats_roots idle={} file_client={} initialized_mount={}",
        idle[V3BudgetPool::Roots as usize],
        file_client_roots,
        mount_roots_after_payload_init
    );
    let stats_open_owner = Filesystem::reserve_request_memory(&f.fs, 8)
        .expect("32 KiB Control cannot admit stats while the real read body is pending")
        .unwrap();
    let stats = Filesystem::open(&f.fs, request(1503), STATS_INODE, libc::O_RDONLY as u32)
        .await
        .expect("complete stats snapshot cannot coexist with a normal pending read");
    drop(stats_open_owner);
    let expected = f.fs.virtual_stats_snapshot(stats.fh).unwrap();
    let expected_text = std::str::from_utf8(expected.as_ref()).unwrap();
    assert!(expected_text.ends_with('\n'));
    assert!(expected_text.contains("brewfs_packed_v3_budget_control_capacity_bytes 32768\n"));
    assert!(
        expected_text.contains("brewfs_packed_v3_budget_metadata_owned_capacity_bytes 67108864\n")
    );
    assert!(expected_text.contains("brewfs_object_read_inflight{layer=\"backend_body\""));
    let attr = Filesystem::getattr(&f.fs, request(1504), STATS_INODE, Some(stats.fh), 0)
        .await
        .unwrap();
    assert_eq!(attr.attr.size, expected.len() as u64);
    let mut full = Vec::new();
    let mut offset = 0;
    let mut last_reply = None;
    loop {
        let owner = Filesystem::reserve_request_memory(&f.fs, 40)
            .unwrap()
            .unwrap();
        let reply = Filesystem::read(&f.fs, request(1505), STATS_INODE, stats.fh, offset, 4096)
            .await
            .expect("complete statistics were not readable to actual EOF");
        drop(owner);
        if reply.data.is_empty() {
            break;
        }
        offset += reply.data.len() as u64;
        full.extend_from_slice(&reply.data);
        last_reply = Some(reply.data.clone());
    }
    assert_eq!(full.as_slice(), expected.as_ref());
    assert_eq!(offset, attr.attr.size);
    assert!(!reader.is_finished());
    assert_eq!(f.gate.dropped.load(Ordering::SeqCst), 0);
    let held_snapshot = f.budget.state().used;
    let retained = last_reply.unwrap();
    drop(expected);
    let release_owner = Filesystem::reserve_request_memory(&f.fs, 24)
        .unwrap()
        .unwrap();
    Filesystem::release(&f.fs, request(1506), STATS_INODE, stats.fh, 0, 0, false)
        .await
        .unwrap();
    drop(release_owner);
    assert!(f.fs.virtual_stats_snapshot(stats.fh).is_none());
    let mut after_stats_release = held_snapshot;
    after_stats_release[V3BudgetPool::Roots as usize] = stats_roots_before_open;
    assert_eq!(f.budget.state().used, after_stats_release);
    assert!(!retained.is_empty());

    let interrupt_owner = Filesystem::reserve_control_memory(&f.fs, 1024)
        .unwrap()
        .unwrap();
    Filesystem::interrupt(&f.fs, request(1507), 1502)
        .await
        .unwrap();
    drop(interrupt_owner);
    let cancelled = tokio::time::timeout(Duration::from_secs(2), reader)
        .await
        .expect("same unique did not cancel the real body")
        .unwrap();
    assert_eq!(cancelled.err(), Some(Errno::from(libc::EINTR)));
    assert_eq!(f.gate.dropped.load(Ordering::SeqCst), 1);
    assert_cancelled_read(&f.observer);
    assert_eq!(f.fs.open_file_handle_count(), 1);
    let before_last_consumer = f.budget.state().used;
    assert_eq!(before_last_consumer[V3BudgetPool::Control as usize], 4096);
    // The lazy coordinator owns a payload-free verification index until
    // shutdown. Subtract only its declared reservation, never a measured
    // delta that could hide retained payload or other unexpected owners.
    // The original 24 KiB is request/handle bookkeeping, not cached payload.
    let request_handle_bytes = before_last_consumer[V3BudgetPool::Metadata as usize]
        .checked_sub(idle[V3BudgetPool::Metadata as usize] + memo_bytes)
        .expect("missing idle metadata or memo reservation");
    assert_eq!(request_handle_bytes, 24576);
    for pool in [
        V3BudgetPool::Stored,
        V3BudgetPool::Raw,
        V3BudgetPool::Workspace,
    ] {
        assert_eq!(before_last_consumer[pool as usize], 0, "{pool:?}");
    }
    assert!(before_last_consumer[V3BudgetPool::Output as usize] > 0);
    drop(retained);
    let after_last_consumer = f.budget.state().used;
    assert_eq!(after_last_consumer[V3BudgetPool::Control as usize], 0);
    assert_eq!(after_last_consumer[V3BudgetPool::Output as usize], 0);
    assert_eq!(
        after_last_consumer[V3BudgetPool::Metadata as usize],
        idle[V3BudgetPool::Metadata as usize] + memo_bytes + 16384
    );
    f.gate.mode.store(0, Ordering::SeqCst);
    let recovery_owner = Filesystem::reserve_request_memory(&f.fs, 40)
        .unwrap()
        .unwrap();
    let recovery = Filesystem::read(&f.fs, request(1508), f.ino, fh, 0, 8192)
        .await
        .unwrap();
    assert_eq!(recovery.data.as_ref(), &[0x75; 8192]);
    drop(recovery);
    drop(recovery_owner);
    assert_eq!(
        f.budget.state().used[V3BudgetPool::Roots as usize],
        mount_roots_after_payload_init + file_client_roots
    );
    Filesystem::release(&f.fs, request(1509), f.ino, fh, 0, 0, false)
        .await
        .unwrap();
    // RELEASE retires exactly the admitted client lease. The lazy pipeline
    // remains owned by V3IndexReader until actual metadata-session shutdown.
    assert_eq!(
        f.budget.state().used[V3BudgetPool::Roots as usize],
        mount_roots_after_payload_init
    );
    assert_eq!(f.fs.open_file_handle_count(), 0);
    assert_eq!(f.budget.state().used[V3BudgetPool::Control as usize], 0);
    assert_eq!(
        f.budget.state().used[V3BudgetPool::Metadata as usize],
        idle[V3BudgetPool::Metadata as usize] + memo_bytes
    );
    assert!(f.budget.state().peak[V3BudgetPool::Control as usize] <= 32768);
    assert_eq!(f.budget.capacity(V3BudgetPool::Metadata), 64 << 20);
    tokio::time::timeout(
        Duration::from_secs(2),
        MetaLayer::shutdown_session(f.fs.meta_layer()),
    )
    .await
    .expect("actual metadata shutdown did not retire the mount coordinator")
    .expect("actual metadata shutdown failed");
    assert_eq!(
        f.budget.state().used[V3BudgetPool::Roots as usize],
        idle[V3BudgetPool::Roots as usize]
    );
    assert!(f.budget.state().closed);
    assert_eq!(f.budget.state().used[V3BudgetPool::Control as usize], 0);
    assert_eq!(f.budget.state().used[V3BudgetPool::Output as usize], 0);
    assert_eq!(
        f.budget.state().used[V3BudgetPool::Metadata as usize],
        idle[V3BudgetPool::Metadata as usize]
    );
}

#[tokio::test]
async fn packed_existing_handle_guards_charge_full_metadata_until_last_consumer() {
    use crate::meta::layer::{MetaLayer, MetadataMemoryKind};
    let f = fixture_with_budget(V3MountBudget::new(limits()).unwrap()).await;
    let baseline = f.budget.state().used;
    for bytes in [1u64, 8192, 16384] {
        let guard =
            f.fs.meta_layer()
                .reserve_memory(MetadataMemoryKind::Handle, bytes)
                .unwrap()
                .unwrap();
        let mut expected = baseline;
        expected[V3BudgetPool::Metadata as usize] += bytes.max(8192);
        assert_eq!(f.budget.state().used, expected);
        let last_consumer = guard.clone();
        drop(guard);
        assert_eq!(f.budget.state().used, expected);
        drop(last_consumer);
        assert_eq!(f.budget.state().used, baseline);
    }
}

#[tokio::test]
async fn packed_metadata_exhaustion_rejects_handles_and_preserves_other_kind_charges() {
    use crate::meta::layer::{MetaLayer, MetadataMemoryKind};
    let f = fixture_with_budget(V3MountBudget::new(limits()).unwrap()).await;
    let baseline = f.budget.state().used;
    let held = f
        .budget
        .admit(&[(
            V3BudgetPool::Metadata,
            f.budget.capacity(V3BudgetPool::Metadata) - baseline[V3BudgetPool::Metadata as usize],
        )])
        .unwrap();
    let full = f.budget.state().used;
    assert!(
        f.fs.meta_layer()
            .reserve_memory(MetadataMemoryKind::Handle, 16384)
            .is_err()
    );
    assert_eq!(f.budget.state().used, full);
    drop(held);
    assert_eq!(f.budget.state().used, baseline);
    for (kind, charges) in [
        (MetadataMemoryKind::Roots, vec![(V3BudgetPool::Roots, 513)]),
        (
            MetadataMemoryKind::Request,
            vec![
                (V3BudgetPool::Control, 8192),
                (V3BudgetPool::Metadata, (2 << 20) + 513),
            ],
        ),
        (
            MetadataMemoryKind::Reply,
            vec![
                (V3BudgetPool::Output, 513 + (1 << 20)),
                (V3BudgetPool::Control, 4096),
            ],
        ),
        (
            MetadataMemoryKind::Control,
            vec![(V3BudgetPool::Control, 513)],
        ),
    ] {
        let guard =
            f.fs.meta_layer()
                .reserve_memory(kind, 513)
                .unwrap()
                .unwrap();
        let mut expected = baseline;
        for (pool, bytes) in charges {
            expected[pool as usize] += bytes;
        }
        assert_eq!(f.budget.state().used, expected);
        let consumer = guard.clone();
        drop(guard);
        assert_eq!(f.budget.state().used, expected);
        drop(consumer);
        assert_eq!(f.budget.state().used, baseline);
    }
}
