//! Run only against the dedicated TLS runner's owned PD/TiKV cluster.
use super::*;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Weak;
use tokio::time::{Instant, timeout};

fn private_tls(
    source: &Path,
    role: &str,
    wrong_ca: bool,
    extra_whitespace: bool,
) -> (TiKvTlsConfig, Weak<tempfile::TempDir>, PathBuf) {
    let owner = Arc::new(tempfile::tempdir().expect("private test TLS directory"));
    let mut paths = Vec::new();
    for (input, output) in [
        (
            if wrong_ca {
                "wrong-ca.crt".to_owned()
            } else {
                "ca.crt".to_owned()
            },
            "ca.crt",
        ),
        (format!("{role}.crt"), "tls.crt"),
        (format!("{role}.key"), "tls.key"),
    ] {
        let mut bytes = std::fs::read(source.join(input)).expect("TLS fixture input");
        if extra_whitespace && output == "tls.crt" {
            bytes.insert(0, b'\n');
        }
        let path = owner.path().join(output);
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        options
            .open(&path)
            .expect("private TLS file")
            .write_all(&bytes)
            .expect("copy TLS fixture");
        paths.push(path);
    }
    let weak = Arc::downgrade(&owner);
    let directory = owner.path().to_owned();
    let tls = TiKvTlsConfig::from_paths_with_owner(
        paths[0].clone(),
        paths[1].clone(),
        paths[2].clone(),
        owner,
    )
    .expect("bounded owned TLS configuration");
    (tls, weak, directory)
}

fn limits(value: usize, response: usize) -> KvReadLimits {
    KvReadLimits {
        max_records: 1,
        max_key_bytes: 256,
        max_value_bytes: value,
        max_total_bytes: value + 256,
        max_response_bytes: response,
        max_data_requests: 1,
    }
}

async fn drained_ledger(budget: &Arc<V3MountBudget>) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if budget.state().used.iter().all(|used| *used == 0) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "TLS SDK resource owners failed to drain"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
#[ignore = "requires the dedicated owner-isolated PD/TiKV TLS runner"]
async fn real_tikv_dual_tls_operator_gate_and_all_client_drain() {
    let Ok(source) = std::env::var("BREWFS_TEST_TIKV_TLS_DIR") else {
        eprintln!("skip: BREWFS_TEST_TIKV_TLS_DIR is unset");
        return;
    };
    let Ok(endpoint) = std::env::var("BREWFS_TEST_TIKV_TLS_PD_ENDPOINT") else {
        eprintln!("skip: BREWFS_TEST_TIKV_TLS_PD_ENDPOINT is unset");
        return;
    };
    let source = PathBuf::from(source);
    let namespace = format!("dual-tls-{}", uuid::Uuid::new_v4().simple());

    let budget = V3MountBudget::defaults();
    let (admin_tls, admin_owner, admin_directory) = private_tls(&source, "admin", false, false);
    let (runtime_tls, runtime_owner, runtime_directory) =
        private_tls(&source, "runtime", false, false);
    let admin = timeout(
        Duration::from_secs(45),
        TiKvWorkspaceBackend::connect_operator_admin_with_budget(
            vec![endpoint.clone()],
            &namespace,
            budget.clone(),
            admin_tls,
            runtime_tls,
        ),
    )
    .await
    .expect("dual TLS constructor deadline")
    .expect("independent TLS identities must authenticate to PD and TiKV");
    assert!(
        runtime_owner.upgrade().is_none(),
        "temporary runtime proof connection must release TLS ownership"
    );
    assert!(!runtime_directory.exists());
    assert!(admin_owner.upgrade().is_some());
    admin
        .authenticate_gc_admin()
        .await
        .expect("authenticated operator GC capability");
    for (value, response) in [
        (4 << 10, 8 << 10),
        (12 << 10, 16 << 10),
        (48 << 10, 64 << 10),
        (96 << 10, 128 << 10),
    ] {
        let (values, _) = admin
            .get_many_consistent_with_time_bounded(
                &[b"tls-gate/missing".to_vec()],
                limits(value, response),
            )
            .await
            .expect("each bounded decoder must retain the configured TLS identity");
        assert_eq!(values, vec![None]);
    }
    {
        let clients = admin.clients.lock().await;
        assert!(
            clients.main.is_some()
                && clients.receipt.is_some()
                && clients.small.is_some()
                && clients.journal.is_some()
                && clients.xattr.is_some()
        );
    }
    admin
        .shutdown()
        .await
        .expect("join all five TLS client owners");
    assert!(admin.server_time_ns().await.is_err());
    {
        let clients = admin.clients.lock().await;
        assert!(
            clients.main.is_none()
                && clients.receipt.is_none()
                && clients.small.is_none()
                && clients.journal.is_none()
                && clients.xattr.is_none()
        );
    }
    drained_ledger(&budget).await;
    drop(admin);
    assert!(admin_owner.upgrade().is_none());
    assert!(!admin_directory.exists());

    // A real runtime TLS connection can read the backend, but has no private
    // operator-session proof. Certificate presence cannot grant GC authority.
    let runtime_budget = V3MountBudget::defaults();
    let (runtime_tls, runtime_owner, _) = private_tls(&source, "runtime", false, false);
    let runtime = TiKvWorkspaceBackend::connect_with_tls_and_budget(
        vec![endpoint.clone()],
        &namespace,
        runtime_budget.clone(),
        runtime_tls,
    )
    .await
    .expect("runtime TLS transport");
    runtime
        .probe_authenticated_connection()
        .await
        .expect("runtime actual store authentication");
    assert!(matches!(
        runtime.authenticate_gc_admin().await,
        Err(WorkspaceError::UnsupportedCapability(_))
    ));
    runtime.shutdown().await.expect("runtime scope drain");
    drained_ledger(&runtime_budget).await;
    drop(runtime);
    assert!(runtime_owner.upgrade().is_none());

    // Distinct files/PEM whitespace cannot disguise reuse of one leaf identity.
    let same_budget = V3MountBudget::defaults();
    let (admin_tls, _, _) = private_tls(&source, "admin", false, false);
    let (same_leaf, _, _) = private_tls(&source, "admin", false, true);
    let same = TiKvWorkspaceBackend::connect_operator_admin_with_budget(
        vec![endpoint.clone()],
        &namespace,
        same_budget.clone(),
        admin_tls,
        same_leaf,
    )
    .await;
    assert!(matches!(
        same,
        Err(WorkspaceError::UnsupportedCapability(_))
    ));
    drained_ledger(&same_budget).await;

    let wrong_budget = V3MountBudget::defaults();
    let (wrong_admin, wrong_owner, _) = private_tls(&source, "admin", true, false);
    let (runtime_tls, _, _) = private_tls(&source, "runtime", false, false);
    let wrong = timeout(
        Duration::from_secs(45),
        TiKvWorkspaceBackend::connect_operator_admin_with_budget(
            vec![endpoint.clone()],
            &namespace,
            wrong_budget.clone(),
            wrong_admin,
            runtime_tls,
        ),
    )
    .await
    .expect("incorrect CA must terminate with an authentication error");
    assert!(
        wrong.is_err(),
        "incorrect server trust root must not authenticate an admin session"
    );
    drop(wrong);
    drained_ledger(&wrong_budget).await;
    assert!(wrong_owner.upgrade().is_none());

    // A final positive control distinguishes CA rejection from a dead service.
    let control_budget = V3MountBudget::defaults();
    let (admin_tls, _, _) = private_tls(&source, "admin", false, false);
    let (runtime_tls, _, _) = private_tls(&source, "runtime", false, false);
    let control = TiKvWorkspaceBackend::connect_operator_admin_with_budget(
        vec![endpoint],
        &namespace,
        control_budget.clone(),
        admin_tls,
        runtime_tls,
    )
    .await
    .expect("positive TLS control after the negative cases");
    control
        .authenticate_gc_admin()
        .await
        .expect("final authenticated admin control");
    control.shutdown().await.expect("final control drain");
    drained_ledger(&control_budget).await;
    drop(control);
    eprintln!("dual TLS gate: both identities, five clients, negative controls and drains passed");
}
