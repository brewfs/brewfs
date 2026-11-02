//! Actual CLI catalog selection, mounted mutation, process death and PVC recovery.
//! Ignored Linux tests require real Redis/TiKV and /dev/fuse. The subprocess
//! runs BREWFS_TEST_BREWFS_BIN; every mount and recovery uses its real CLI parser.

use super::packed_mount_cutoff::packed_original_shutdown_tests::{
    ActualObjects, ObjectProbe, selected_cli_objects,
};
use super::*;
use crate::workspace_overlay::catalog::{
    HeadGuard, PermissionSnapshotQuery, ReleaseLease, VersionedMutation,
};
use crate::workspace_overlay::ids::LeaseId;
use crate::workspace_overlay::model::{LeaseState, SnapshotLease, WorkspaceRecord};
use crate::workspace_overlay::packed_v3::wire005::V3MountBudget;
use crate::workspace_overlay::stores::binding_tests::{packed_for_process_restart, request};
use crate::workspace_overlay::stores::kv_backend::{KvReadLimits, WorkspaceKvBackend};
use crate::workspace_overlay::stores::kv_store::packed_admin::{
    PackedCleanAdmission, PackedReleasedMountReference,
};
use std::fs::OpenOptions;
use std::future::Future;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::{Child, Command as ProcessCommand, ExitStatus, Stdio};
use uuid::Uuid;

const WAIT: Duration = Duration::from_secs(90);
const PAYLOAD: &[u8] = b"packed-v3 actual CLI upper payload survives release and process recovery";
type Connection<B> =
    Pin<Box<dyn Future<Output = Result<B, workspace_overlay::error::WorkspaceError>> + Send>>;

fn report_smallfile_hashes(stage: &str, hashes: &std::collections::BTreeMap<String, String>) {
    use sha2::{Digest, Sha256};
    let artifact = PathBuf::from(
        std::env::var_os("BREWFS_TEST_SMALLFILE_ARTIFACT_DIR")
            .expect("explicit writable directory for full crash SHA-256 manifests"),
    );
    let aggregate = hex::encode(Sha256::digest(serde_json::to_vec(hashes).unwrap()));
    std::fs::write(
        artifact.join(format!("{stage}.json")),
        serde_json::to_vec_pretty(&serde_json::json!({
            "files": hashes.len(), "bytes_per_file": 4096, "mismatches": 0,
            "aggregate_sha256": aggregate, "sha256": hashes,
        }))
        .unwrap(),
    )
    .unwrap();
    eprintln!(
        "smallfile-crash stage={stage} files={} sha256={aggregate}",
        hashes.len()
    );
}

fn write_smallfile_crash_corpus(
    mount: &Path,
    count: usize,
) -> std::collections::BTreeMap<String, String> {
    use sha2::{Digest, Sha256};
    let directory = mount.join("smallfile-crash-corpus");
    std::fs::create_dir(&directory).unwrap();
    let mut hashes = std::collections::BTreeMap::new();
    for index in 0..count {
        let seed = (index as u64 + 1).to_le_bytes();
        let payload = (0..4096)
            .map(|offset| seed[offset % 8] ^ if offset % 2 == 0 { 0 } else { 0x80 })
            .collect::<Vec<_>>();
        let name = format!("f{index:06}");
        let path = directory.join(&name);
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.write_all(&payload).unwrap();
        file.sync_all().unwrap();
        drop(file);
        let expected = hex::encode(Sha256::digest(&payload));
        assert_eq!(
            hex::encode(Sha256::digest(std::fs::read(path).unwrap())),
            expected
        );
        hashes.insert(name, expected);
    }
    report_smallfile_hashes("before-sigkill", &hashes);
    hashes
}

fn measure_smallfile_write_rename(mount: &Path) {
    let path = mount.join("smallfile-operation-guard");
    let renamed = mount.join("smallfile-operation-guard-renamed");
    let payload = [0x5a; 4096];
    let mut result = serde_json::Map::new();
    for mode in ["write", "rename"] {
        let mut rounds = Vec::new();
        for _ in 0..7 {
            let start = std::time::Instant::now();
            for _ in 0..256 {
                if mode == "write" {
                    let mut file = OpenOptions::new()
                        .create(true)
                        .truncate(true)
                        .write(true)
                        .open(&path)
                        .unwrap();
                    file.write_all(&payload).unwrap();
                    file.sync_all().unwrap();
                    drop(file);
                } else {
                    std::fs::rename(&path, &renamed).unwrap();
                    std::fs::rename(&renamed, &path).unwrap();
                }
            }
            let operations = if mode == "write" { 256.0 } else { 512.0 };
            rounds.push(start.elapsed().as_secs_f64() * 1000.0 / operations);
        }
        let mut ordered = rounds.clone();
        ordered.sort_by(f64::total_cmp);
        result.insert(
            mode.into(),
            serde_json::json!({"ms_per_op": rounds, "median_ms": ordered[3]}),
        );
    }
    assert_eq!(std::fs::read(path).unwrap(), payload);
    let artifact = std::env::var_os("BREWFS_TEST_SMALLFILE_ARTIFACT_DIR").unwrap();
    std::fs::write(
        PathBuf::from(artifact).join("write-rename-guards.json"),
        serde_json::to_vec_pretty(&result).unwrap(),
    )
    .unwrap();
}

fn verify_smallfile_crash_corpus(
    mount: &Path,
    expected: &std::collections::BTreeMap<String, String>,
) {
    use sha2::{Digest, Sha256};
    let directory = mount.join("smallfile-crash-corpus");
    assert_eq!(
        std::fs::read_dir(&directory).unwrap().count(),
        expected.len()
    );
    let mut actual = std::collections::BTreeMap::new();
    for (name, digest) in expected {
        let bytes = std::fs::read(directory.join(name)).unwrap();
        assert_eq!(bytes.len(), 4096);
        let hash = hex::encode(Sha256::digest(bytes));
        assert_eq!(&hash, digest, "recovered content differs: {name}");
        actual.insert(name.clone(), hash);
    }
    report_smallfile_hashes("after-recovery", &actual);
}

// Fault the real LocalFS PUT at the filesystem boundary while keeping the
// original PVC writable. Restoring exact permissions lets real recovery retry.
struct DenyObjectWrites {
    root: PathBuf,
    modes: Vec<(PathBuf, std::fs::Permissions)>,
    objects: Vec<(PathBuf, Vec<u8>)>,
}
impl DenyObjectWrites {
    fn new(root: &Path) -> Self {
        fn visit(path: &Path, rows: &mut Vec<(PathBuf, std::fs::Permissions)>) {
            assert!(rows.len() < 128, "small fault fixture object cap");
            let metadata = std::fs::symlink_metadata(path).unwrap();
            assert!(!metadata.file_type().is_symlink());
            rows.push((path.to_owned(), metadata.permissions()));
            if metadata.is_dir() {
                for entry in std::fs::read_dir(path).unwrap() {
                    visit(&entry.unwrap().path(), rows);
                }
            }
        }
        let mut rows = Vec::new();
        visit(root, &mut rows);
        let objects = Self::snapshot(&rows);
        let guard = Self {
            root: root.to_owned(),
            modes: rows,
            objects,
        };
        for (path, _) in &guard.modes {
            let mode = if path.is_dir() { 0o555 } else { 0o444 };
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
        }
        let probe = root.join(format!("denied-put-{}", Uuid::new_v4()));
        match OpenOptions::new().write(true).create_new(true).open(&probe) {
            Ok(file) => {
                drop(file);
                std::fs::remove_file(probe).unwrap();
                panic!("actual hxy process can bypass the object PUT permission fault");
            }
            Err(error) => assert_eq!(
                error.kind(),
                std::io::ErrorKind::PermissionDenied,
                "object PUT fault must fail at the real filesystem boundary"
            ),
        }
        guard
    }
    fn snapshot(rows: &[(PathBuf, std::fs::Permissions)]) -> Vec<(PathBuf, Vec<u8>)> {
        let mut bytes = 0usize;
        let mut objects = rows
            .iter()
            .filter(|(path, _)| path.is_file())
            .map(|(path, _)| {
                let data = std::fs::read(path).unwrap();
                bytes = bytes.checked_add(data.len()).unwrap();
                assert!(bytes <= 8 << 20, "small object fixture byte cap");
                (path.clone(), data)
            })
            .collect::<Vec<_>>();
        objects.sort_by(|left, right| left.0.cmp(&right.0));
        objects
    }
    fn assert_unchanged(&self) {
        fn paths(root: &Path, rows: &mut Vec<(PathBuf, std::fs::Permissions)>) {
            let metadata = std::fs::symlink_metadata(root).unwrap();
            assert!(!metadata.file_type().is_symlink());
            assert!(rows.len() < 128);
            rows.push((root.to_owned(), metadata.permissions()));
            if metadata.is_dir() {
                for entry in std::fs::read_dir(root).unwrap() {
                    paths(&entry.unwrap().path(), rows);
                }
            }
        }
        let mut rows = Vec::new();
        paths(&self.root, &mut rows);
        assert_eq!(
            Self::snapshot(&rows),
            self.objects,
            "a real object PUT succeeded while original PVC upload was pending"
        );
    }
}
impl Drop for DenyObjectWrites {
    fn drop(&mut self) {
        for (path, mode) in &self.modes {
            std::fs::set_permissions(path, mode.clone()).unwrap();
        }
    }
}

fn cli_object_case(backend: &str, killed: bool, pending: bool) -> &'static str {
    match (backend, killed, pending) {
        ("redis", false, false) => "rc",
        ("tikv", false, false) => "tc",
        ("redis", true, false) => "rk",
        ("tikv", true, false) => "tk",
        ("redis", true, true) => "rp",
        ("tikv", true, true) => "tp",
        _ => panic!("invalid actual CLI object case"),
    }
}

fn cli_data_config(objects: &Path) -> serde_json::Value {
    match std::env::var("BREWFS_TEST_ORIGINAL_OBJECT_BACKEND").as_deref() {
        Err(std::env::VarError::NotPresent) | Ok("localfs") => {
            serde_json::json!({"backend": "local-fs", "localfs": {"data_dir": objects}})
        }
        Ok("rustfs") => {
            // selected_cli_objects has already authenticated the exact owned
            // loopback endpoint, run UUID, per-case bucket and static identity.
            for (aws, runtime) in [
                (
                    "AWS_ACCESS_KEY_ID",
                    "BREWFS_TEST_ORIGINAL_RUSTFS_ACCESS_KEY",
                ),
                (
                    "AWS_SECRET_ACCESS_KEY",
                    "BREWFS_TEST_ORIGINAL_RUSTFS_SECRET_KEY",
                ),
            ] {
                assert!(
                    std::env::var(aws).unwrap() == std::env::var(runtime).unwrap(),
                    "CLI AWS identity must match the owned runtime identity"
                );
            }
            serde_json::json!({"backend": "s3", "s3": {
                "bucket": std::env::var("BREWFS_TEST_ORIGINAL_RUSTFS_BUCKET").unwrap(),
                "endpoint": std::env::var("BREWFS_TEST_ORIGINAL_RUSTFS_ENDPOINT").unwrap(),
                "region": "us-east-1", "force_path_style": true,
                "max_concurrency": 2, "disable_payload_checksum": true,
            }})
        }
        _ => panic!("invalid actual CLI object selection"),
    }
}

struct RustfsPutDenial {
    socket: PathBuf,
    token: String,
    case: String,
}
impl RustfsPutDenial {
    fn call(&self, action: &str) -> std::io::Result<()> {
        use std::io::Read;
        use std::os::unix::net::UnixStream;
        assert!(matches!(
            action,
            "deny-put" | "assert-unchanged" | "restore-put"
        ));
        let mut socket = UnixStream::connect(&self.socket)?;
        socket.set_read_timeout(Some(Duration::from_secs(10)))?;
        socket.set_write_timeout(Some(Duration::from_secs(10)))?;
        let mut request = serde_json::to_vec(&serde_json::json!({
            "action": action, "case": self.case, "token": self.token,
        }))?;
        assert!(request.len() <= 512);
        request.push(b'\n');
        socket.write_all(&request)?;
        socket.shutdown(std::net::Shutdown::Write)?;
        let mut bytes = Vec::new();
        socket.take(4097).read_to_end(&mut bytes)?;
        assert!(bytes.len() <= 4096, "bounded IAM fault control reply");
        let reply: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(
            reply["ok"], true,
            "actual owned RustFS IAM fault operation failed"
        );
        assert_eq!(reply["action"], action);
        assert_eq!(reply["case"], self.case);
        Ok(())
    }
    fn new() -> Self {
        let guard = Self {
            socket: std::env::var_os("BREWFS_TEST_CLI_FAULT_SOCKET")
                .unwrap()
                .into(),
            token: std::env::var("BREWFS_TEST_CLI_FAULT_TOKEN").unwrap(),
            case: std::env::var("BREWFS_TEST_CLI_RUSTFS_CASE").unwrap(),
        };
        assert!(matches!(guard.case.as_str(), "rp" | "tp"));
        guard.call("deny-put").unwrap();
        guard
    }
}
impl Drop for RustfsPutDenial {
    fn drop(&mut self) {
        let result = self.call("restore-put");
        if !std::thread::panicking() {
            result.unwrap();
        }
    }
}

enum DeniedObjectWrites {
    Local(DenyObjectWrites),
    Rustfs(RustfsPutDenial),
}
impl DeniedObjectWrites {
    fn new(root: &Path) -> Self {
        if std::env::var("BREWFS_TEST_ORIGINAL_OBJECT_BACKEND").as_deref() == Ok("rustfs") {
            Self::Rustfs(RustfsPutDenial::new())
        } else {
            Self::Local(DenyObjectWrites::new(root))
        }
    }
    fn assert_unchanged(&self) {
        match self {
            Self::Local(guard) => guard.assert_unchanged(),
            Self::Rustfs(guard) => guard.call("assert-unchanged").unwrap(),
        }
    }
}

fn executable() -> PathBuf {
    let path = PathBuf::from(
        std::env::var_os("BREWFS_TEST_BREWFS_BIN")
            .expect("explicit path to the separately built production brewfs executable"),
    )
    .canonicalize()
    .unwrap();
    let metadata = std::fs::metadata(&path).unwrap();
    assert!(metadata.is_file() && metadata.permissions().mode() & 0o111 != 0);
    path
}

fn write_mount_config(
    root: &Path,
    objects: &Path,
    backend: &str,
    namespace: &str,
    workspace: WorkspaceId,
    operator_managed: bool,
) -> PathBuf {
    let meta = if backend == "redis" {
        serde_json::json!({"backend": "redis", "redis": {
            "url": std::env::var("BREWFS_TEST_REDIS_URL").unwrap(),
        }})
    } else {
        assert_eq!(backend, "tikv");
        serde_json::json!({"backend": "tikv", "tikv": {
            "pd_endpoints": std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS").unwrap()
                .split(',').map(str::to_owned).collect::<Vec<_>>(),
        }})
    };
    let value = serde_json::json!({
        "mount_point": root.join("mount"), "volume_format": "workspace-v1",
        "workspace": workspace, "workspace_namespace": namespace,
        "workspace_operator_managed": operator_managed,
        "data": cli_data_config(objects),
        "meta": meta, "layout": {"chunk_size": 4096, "block_size": 4096},
        "fuse": {"workers": 2, "max_background": 4, "privileged": false},
        "cache": {"cache_root": root.join("pvc-cache"), "read_memory_bytes": 0,
            "read_ssd_bytes": 0, "write_memory_bytes": 1 << 20,
            "write_ssd_bytes": 16 << 20, "memory_budget_bytes": 8 << 20,
            "prefetch_enabled": false, "range_background_prefetch": false,
            "populate_write_cache_after_upload": false, "persist_write_cache_after_upload": true,
            "writeback_mode": "upload_before_commit", "writeback_persist_sync": true,
            "writeback_require_stage_before_commit": true,
            "dirty_slice_target_size": 4096, "dirty_slice_max_age_ms": 20,
            "upload_concurrency": 1, "compression": "none"},
    });
    // JSON is YAML-compatible. The config may contain the supplied Redis URL;
    // keep it private and never echo it to process logs or test output.
    let path = root.join(format!("mount-{}.json", Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)
        .unwrap();
    file.write_all(&serde_json::to_vec_pretty(&value).unwrap())
        .unwrap();
    file.sync_all().unwrap();
    path
}

fn present_mount(path: &Path) -> Option<u64> {
    let path = path.to_str().unwrap();
    assert!(
        !path
            .as_bytes()
            .iter()
            .any(|byte| byte.is_ascii_whitespace() || *byte == b'\\')
    );
    let mut matches = std::fs::read_to_string("/proc/self/mountinfo")
        .unwrap()
        .lines()
        .filter_map(|line| {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            (fields.get(4).copied() == Some(path)).then(|| fields[0].parse::<u64>().unwrap())
        })
        .collect::<Vec<_>>();
    assert!(matches.len() <= 1, "stacked mount at owned test directory");
    matches.pop()
}

fn cleanup_dead_mount(path: &Path, expected: Option<u64>) {
    if let Some(actual) = present_mount(path) {
        assert_eq!(
            Some(actual),
            expected,
            "owned mount identity changed before cleanup"
        );
        let status = if unsafe { libc::geteuid() } == 0 {
            ProcessCommand::new("umount").arg(path).status().unwrap()
        } else {
            ProcessCommand::new("fusermount3")
                .args(["-u", "--"])
                .arg(path)
                .status()
                .unwrap()
        };
        assert!(status.success());
        assert!(
            present_mount(path).is_none(),
            "dead original kernel mount remained"
        );
    }
}

struct CliMountIdentity {
    workspace: WorkspaceId,
    mount_uid: Uuid,
    pod_uid: Uuid,
}

struct OwnedCliChild {
    child: Child,
    config: PathBuf,
    log: PathBuf,
    mount: PathBuf,
    original_mount_id: Option<u64>,
    terminal: bool,
    started: std::time::Instant,
    diagnostics_emitted: bool,
}
impl OwnedCliChild {
    fn spawn(
        root: &Path,
        objects: &Path,
        backend: &str,
        namespace: &str,
        identity: CliMountIdentity,
        killed: bool,
        _pending: bool,
    ) -> Self {
        let CliMountIdentity {
            workspace,
            mount_uid,
            pod_uid,
        } = identity;
        let config = write_mount_config(root, objects, backend, namespace, workspace, killed);
        let log_path = root.join(format!("cli-{}.log", Uuid::new_v4()));
        let log = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&log_path)
            .unwrap();
        let mut command = ProcessCommand::new(executable());
        command
            .arg("mount")
            .arg("--config")
            .arg(&config)
            .env("BREWFS_PACKED_V3_MOUNT_UID", mount_uid.to_string())
            .env("BREWFS_PACKED_V3_POD_UID", pod_uid.to_string())
            // Periodic stats would evict early heartbeat failures from the
            // bounded tail; retain all warning/error events in this module.
            .env("RUST_LOG", "brewfs=info,brewfs::vfs::fs=warn")
            .env_remove("BREWFS_LOG_FILE")
            .env_remove("BREWFS_FUSE_LOG_FILE")
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log));
        let child = command.spawn().unwrap();
        Self {
            child,
            config,
            log: log_path,
            mount: root.join("mount"),
            original_mount_id: None,
            terminal: false,
            started: std::time::Instant::now(),
            diagnostics_emitted: false,
        }
    }
    fn stage(&self, phase: &str) {
        eprintln!(
            "actual-cli diagnostic phase={phase} elapsed_ms={}",
            self.started.elapsed().as_millis()
        );
    }
    fn diagnostics(&mut self, phase: &str, status: Option<ExitStatus>) {
        if self.diagnostics_emitted {
            return;
        }
        self.diagnostics_emitted = true;
        self.stage(phase);
        if let Some(status) = status {
            eprintln!(
                "actual-cli diagnostic exit_code={:?} signal={:?} core_dumped={}",
                status.code(),
                status.signal(),
                status.core_dumped()
            );
        }
        // Capture before TempDir unwinding removes the private child log.
        // Fail closed if a bounded, complete UTF-8 tail cannot be redacted.
        if let Some(tail) = sanitized_cli_log_tail(&self.log) {
            eprintln!("actual-cli diagnostic child_log_tail_begin\n{tail}");
            eprintln!("actual-cli diagnostic child_log_tail_end");
        } else {
            eprintln!("actual-cli diagnostic child_log_tail_unavailable");
        }
    }
    async fn mounted(&mut self) {
        tokio::time::timeout(WAIT, async {
            loop {
                assert!(
                    self.child.try_wait().unwrap().is_none(),
                    "actual CLI exited before mounted state"
                );
                if let Some(id) = present_mount(&self.mount) {
                    let status =
                        std::fs::read_to_string(format!("/proc/{}/status", self.child.id()))
                            .unwrap();
                    let uid = status
                        .lines()
                        .find(|line| line.starts_with("Uid:"))
                        .unwrap()
                        .split_whitespace()
                        .skip(1)
                        .map(|value| value.parse::<u32>().unwrap())
                        .collect::<Vec<_>>();
                    assert_eq!(uid.len(), 4);
                    assert!(
                        uid.iter().all(|uid| *uid == unsafe { libc::geteuid() }),
                        "actual CLI must preserve the hxy uid across exec"
                    );
                    let capabilities = status
                        .lines()
                        .find(|line| line.starts_with("CapEff:"))
                        .unwrap()
                        .split_whitespace()
                        .nth(1)
                        .unwrap();
                    assert_eq!(
                        u64::from_str_radix(capabilities, 16).unwrap() & 0b110,
                        0,
                        "actual CLI must not bypass the real object PUT permission fault"
                    );
                    self.original_mount_id = Some(id);
                    self.stage("mounted");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    async fn wait(&mut self) -> ExitStatus {
        let status = tokio::time::timeout(WAIT, async {
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("actual CLI child did not terminate");
        self.terminal = true;
        status
    }
    async fn terminate(&mut self, killed: bool) {
        assert!(self.child.try_wait().unwrap().is_none());
        self.stage(if killed { "sigkill" } else { "sigterm" });
        if killed {
            self.child.kill().unwrap();
        } else {
            assert_eq!(
                unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGTERM) },
                0
            );
        }
        let status = self.wait().await;
        self.stage("terminal");
        if status.success() == killed {
            self.diagnostics("unexpected-terminal", Some(status));
        }
        assert_eq!(status.success(), !killed, "actual CLI terminal status");
        cleanup_dead_mount(&self.mount, self.original_mount_id);
    }
}
impl Drop for OwnedCliChild {
    fn drop(&mut self) {
        if !self.terminal {
            let _ = self.child.kill();
            let status = self.child.wait().ok();
            if std::thread::panicking() {
                self.diagnostics("panic-cleanup", status);
            }
            self.terminal = true;
        } else if std::thread::panicking() {
            let status = self.child.try_wait().ok().flatten();
            self.diagnostics("panic-cleanup", status);
        }
        cleanup_dead_mount(&self.mount, self.original_mount_id);
    }
}

fn sanitized_cli_log_tail(path: &Path) -> Option<String> {
    const MAX_BYTES: u64 = 64 << 10;
    let mut file = std::fs::File::open(path).ok()?;
    let length = file.metadata().ok()?.len();
    let offset = length.saturating_sub(MAX_BYTES);
    file.seek(SeekFrom::Start(offset)).ok()?;
    let mut bytes = Vec::new();
    file.take(MAX_BYTES).read_to_end(&mut bytes).ok()?;
    if offset != 0 {
        // The first line may contain only a suffix of a credential; omit it.
        let end = bytes.iter().position(|byte| *byte == b'\n')?;
        bytes.drain(..=end);
    }
    // A crashed child can end mid-line, including mid-credential.
    let end = bytes.iter().rposition(|byte| *byte == b'\n')?;
    bytes.truncate(end + 1);
    let mut tail = String::from_utf8(bytes).ok()?;
    let mut secrets = Vec::new();
    let mut add_secret = |value: &str| -> Option<()> {
        if value.is_empty() {
            return Some(());
        }
        // A multi-line value may overlap either discarded boundary line.
        if value.contains(['\n', '\r']) {
            return None;
        }
        secrets.push(value.to_owned());
        let debug = format!("{value:?}");
        secrets.push(debug[1..debug.len() - 1].to_owned());
        let json = serde_json::to_string(value).ok()?;
        secrets.push(json[1..json.len() - 1].to_owned());
        Some(())
    };
    for (name, value) in std::env::vars_os() {
        let name = name.to_str()?.to_ascii_uppercase();
        let sensitive = [
            "SECRET",
            "TOKEN",
            "PASSWORD",
            "ACCESS_KEY",
            "CREDENTIAL",
            "REDIS_URL",
        ]
        .iter()
        .any(|part| name.contains(part));
        let Some(value) = value.to_str() else {
            if sensitive {
                return None;
            }
            continue;
        };
        if sensitive && !value.is_empty() {
            add_secret(value)?;
        }
        if let Ok(url) = url::Url::parse(value) {
            for credential in [Some(url.username()), url.password()].into_iter().flatten() {
                if !credential.is_empty() {
                    add_secret(credential)?;
                    let encoded = format!("v={credential}");
                    let (_, decoded) = url::form_urlencoded::parse(encoded.as_bytes()).next()?;
                    add_secret(&decoded)?;
                }
            }
        }
    }
    secrets.sort_unstable_by_key(|value| std::cmp::Reverse(value.len()));
    secrets.dedup();
    for secret in secrets {
        tail = tail.replace(&secret, "[REDACTED]");
    }
    while tail.len() > MAX_BYTES as usize {
        let end = tail.find('\n')?;
        tail.drain(..=end);
    }
    Some(tail)
}

async fn read_rows<B: WorkspaceKvBackend>(
    backend: &B,
    keys: &[Vec<u8>],
) -> (Vec<Option<Vec<u8>>>, i64) {
    let (values, now) = backend
        .get_many_consistent_with_time_bounded(
            keys,
            KvReadLimits {
                max_records: keys.len(),
                max_key_bytes: 256,
                max_value_bytes: 12 << 10,
                max_total_bytes: 64 << 10,
                max_response_bytes: 64 << 10,
                max_data_requests: keys.len(),
            },
        )
        .await
        .unwrap();
    assert_eq!(values.len(), keys.len());
    assert!(now > 0);
    (values, now)
}

fn native_record<T: serde::de::DeserializeOwned>(raw: &[u8]) -> T {
    // WorkspaceRecord carries the writable-bucket schema envelope; leases
    // and identifiers continue to use the ordinary native record envelope.
    let magic: &[u8] = if std::any::type_name::<T>() == std::any::type_name::<WorkspaceRecord>() {
        b"BWSWR002"
    } else {
        b"BWSKV001"
    };
    bincode::deserialize(raw.strip_prefix(magic).expect("native record envelope")).unwrap()
}

async fn mounted_lease<B: WorkspaceKvBackend>(
    backend: &B,
    workspace: WorkspaceId,
) -> (SnapshotLease, serde_json::Value) {
    let key = format!("packed-v3/writer/{workspace}").into_bytes();
    let (values, _) = read_rows(backend, &[key]).await;
    let raw = values[0].as_deref().unwrap();
    assert!(raw.starts_with(b"PWA3\x01"));
    let writer: serde_json::Value = serde_json::from_slice(&raw[5..]).unwrap();
    let owner = &writer["owner"]["Mounted"];
    let lease_id: LeaseId = serde_json::from_value(owner["lease_id"].clone()).unwrap();
    let generation = owner["holder_generation"].as_u64().unwrap();
    let keys = [
        format!("lease/{workspace}/{lease_id}").into_bytes(),
        format!("open/v3/{workspace}").into_bytes(),
        format!("ws/{workspace}").into_bytes(),
        format!("lease-id/{lease_id}").into_bytes(),
    ];
    let (values, now) = read_rows(backend, &keys).await;
    let lease: SnapshotLease = native_record(values[0].as_deref().unwrap());
    assert_eq!(lease.lease_id, lease_id);
    assert_eq!(lease.holder_generation, generation);
    assert_eq!(lease.workspace_id, workspace);
    assert_eq!(lease.state, LeaseState::Active);
    assert!(lease.writable && lease.expires_at_ns > now);
    assert!(
        values[1].is_some(),
        "joint grant must include actual open record"
    );
    let entity: WorkspaceRecord = native_record(values[2].as_deref().unwrap());
    assert_eq!(entity.workspace_id, workspace);
    assert_eq!(entity.active_lease, Some(lease_id));
    let indexed_workspace: WorkspaceId = native_record(values[3].as_deref().unwrap());
    assert_eq!(indexed_workspace, workspace);
    (lease, writer)
}

async fn assert_idle<B: WorkspaceKvBackend>(
    backend: &B,
    workspace: WorkspaceId,
    leases: &[LeaseId],
) {
    let mut keys = vec![
        format!("packed-v3/writer/{workspace}").into_bytes(),
        format!("ws/{workspace}").into_bytes(),
    ];
    for lease in leases {
        keys.push(format!("lease/{workspace}/{lease}").into_bytes());
        keys.push(format!("lease-id/{lease}").into_bytes());
    }
    let (values, _) = read_rows(backend, &keys).await;
    let raw = values[0].as_deref().unwrap();
    assert!(raw.starts_with(b"PWA3\x01"));
    let writer: serde_json::Value = serde_json::from_slice(&raw[5..]).unwrap();
    assert!(
        writer["owner"].is_null(),
        "terminal CAS must retire packed writer"
    );
    let entity: WorkspaceRecord = native_record(values[1].as_deref().unwrap());
    assert_eq!(entity.workspace_id, workspace);
    assert!(
        entity.active_lease.is_none(),
        "terminal CAS must retire active lease pointer"
    );
    for (rows, id) in values[2..].as_chunks::<2>().0.iter().zip(leases) {
        let lease: SnapshotLease = native_record(rows[0].as_deref().unwrap());
        assert_eq!(lease.lease_id, *id);
        assert_eq!(lease.workspace_id, workspace);
        assert_eq!(lease.state, LeaseState::Released);
        let indexed_workspace: WorkspaceId = native_record(rows[1].as_deref().unwrap());
        assert_eq!(indexed_workspace, workspace);
    }
}

async fn publish_recovered_cli_source<B, F>(
    connect: Arc<F>,
    root: &Path,
    client: ObjectClient<ActualObjects>,
    released: PackedReleasedMountReference,
) -> crate::workspace_overlay::stores::kv_store::packed_admin::PackedSnapshotResult
where
    B: WorkspaceKvBackend,
    F: Fn(Arc<V3MountBudget>) -> Connection<B> + Send + Sync + 'static,
{
    use crate::workspace_overlay::ids::{JournalId, LayerId, SnapshotId};
    use crate::workspace_overlay::packed_v3::wire005::V3IndexAuditLimits;
    use crate::workspace_overlay::stores::kv_store::packed_admin::{
        PackedHeadlessSnapshotDescription, PackedHeadlessSnapshotRequest,
    };

    let budget = V3MountBudget::defaults();
    let backend = Arc::new(connect(budget.clone()).await.unwrap());
    let scratch = Arc::new(tempfile::tempdir_in(root).unwrap());
    let snapshot_id = SnapshotId::new();
    let next_head = LayerId::new();
    let publication = async {
        let publisher = Arc::new(
            KvWorkspaceStore::from_arc(backend.clone())
                .with_packed_reader_pin_budget(budget.clone()),
        );
        let client = client.with_read_observer(
            budget.read_observer(None).unwrap(),
            crate::cadapter::read_observer::Engine::PackedV3,
            crate::cadapter::read_observer::Phase::Startup,
            crate::cadapter::read_observer::Origin::Demand,
        );
        let upper = Arc::new(
            ObjectBlockStore::new_with_configs_async(
                client.clone(),
                ChunksCacheConfig::with_budgets(0, 0, scratch.path().join("publication-cache")),
                BlockStoreConfig {
                    block_size: 4096,
                    compression: crate::chunk::compress::Compression::None,
                    populate_write_cache_after_upload: false,
                    persist_write_cache_after_upload: false,
                    range_background_prefetch: false,
                    page_cache_capacity: 0,
                    create_only_writes: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
        );
        let mut request = PackedHeadlessSnapshotRequest::bounded_operator(
            PackedHeadlessSnapshotDescription {
                snapshot_id,
                snapshot_name: format!("actual-cli-pmr-source-{snapshot_id}"),
                owner_id: None,
            },
            LeaseId::new(),
            JournalId::new(),
            next_head,
            300_000_000_000,
            scratch.path().to_path_buf(),
        )
        .with_temporary_owner(scratch.clone())
        .unwrap();
        // The two-file fixture uses the existing small operation tier. These
        // limits also bound real capture and publication scratch on disk.
        request.max_rows = 512;
        request.max_logical_bytes = 8 << 20;
        request.max_data_bytes = 8 << 20;
        request.scratch_disk_bytes = 8 << 20;
        request.graph_limits = V3IndexAuditLimits {
            max_objects: 4096,
            max_authenticated_bytes: 16 << 20,
            max_requested_bytes: 16 << 20,
            max_decoded_bytes: 16 << 20,
            max_frame_validation_steps: 4096,
            max_logical_hash_bytes: 8 << 20,
            max_contexts: 4096,
            max_visits: 16_384,
            max_leaf_records: 4096,
            max_page_records: 4096,
            max_disk_bytes: 8 << 20,
            sqlite_cache_bytes: 64 << 10,
            max_sql_operations: 1_000_000,
            max_sql_vm_steps: 4_000_000,
            chunk_bytes: 4096,
        };
        publisher
            .publish_recovered_packed_snapshot(
                released,
                client,
                upper,
                ChunkLayout {
                    chunk_size: 4096,
                    block_size: 4096,
                },
                request,
            )
            .await
    }
    .await;
    // This operation closes its own ledger. No later observation or mount
    // admission can reuse that ledger or its actual metadata connection.
    backend.shutdown_metadata_backend().await.unwrap();
    let published = match publication {
        Ok(published) => published,
        Err(failure) => panic!(
            "actual recovered source publication failed: {}",
            failure.error()
        ),
    };
    assert!(
        budget.state().closed,
        "actual publisher must close its canonical ledger"
    );
    tokio::time::timeout(WAIT, async {
        while budget.state().used != [0; 8] {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("actual recovered publisher retained operation owners");
    assert_eq!(budget.state().used, [0; 8]);
    assert_eq!(published.snapshot_id, snapshot_id);
    assert_eq!(published.binding.head_layer_id, next_head);
    assert_ne!(
        published.packed_carrier_revision,
        published.native_sealed_source_revision
    );
    published
}

async fn actual_cli_chain<B, F>(
    backend_name: &str,
    namespace: &str,
    connect: Arc<F>,
    killed: bool,
    pending: bool,
    smallfile_count: usize,
) where
    B: WorkspaceKvBackend,
    F: Fn(Arc<V3MountBudget>) -> Connection<B> + Send + Sync + 'static,
{
    assert!(
        Path::new("/dev/fuse").exists(),
        "real CLI test requires FUSE"
    );
    assert_ne!(
        unsafe { libc::geteuid() },
        0,
        "run the real binary fixture as hxy; root can bypass the object PUT fault"
    );
    let _ = executable();
    assert!(!pending || killed);
    assert!(smallfile_count == 0 || (killed && !pending));
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("mount")).unwrap();
    let observation_budget = V3MountBudget::defaults();
    let backend = Arc::new(connect(observation_budget.clone()).await.unwrap());
    let store = Arc::new(
        KvWorkspaceStore::from_arc(backend.clone())
            .with_packed_reader_pin_budget(observation_budget.clone()),
    );
    let (objects, _, fixture_snapshot, _, lower_payload) =
        packed_for_process_restart(root.path()).await;
    let case = cli_object_case(backend_name, killed, pending);
    let actual_client = selected_cli_objects(
        &objects.path().join("objects"),
        Arc::new(ObjectProbe::default()),
        case,
    )
    .await;
    let snapshot = crate::workspace_overlay::packed_v3::wire005::AuthenticatedV3Snapshot::open(
        &actual_client,
        fixture_snapshot.manifest_reference(),
    )
    .await
    .unwrap();
    let proof = crate::workspace_overlay::publish::binding::VerifiedPackedLower::from_authenticated_snapshot(
        &snapshot,
        &crate::workspace_overlay::packed_v3::wire005::V3IndexReader::new(actual_client.clone(), 0),
    ).await.unwrap();
    if std::env::var("BREWFS_TEST_ORIGINAL_OBJECT_BACKEND").as_deref() == Ok("rustfs") {
        assert_eq!(std::env::var("BREWFS_TEST_CLI_RUSTFS_CASE").unwrap(), case);
        eprintln!("actual-cli object_backend=rustfs case={case}");
    }
    let install = request(store.as_ref(), proof).await;
    let binding = store
        .install_packed_lower_binding(install.clone())
        .await
        .unwrap();
    // This CLI deliberately runs without root privileges. Assign its disposable
    // upper root to that user through the real guarded copy-up path, preserving
    // the fixture's 0755 permissions and all writer/binding/version checks.
    let fixture_uid = unsafe { libc::geteuid() };
    let fixture_gid = unsafe { libc::getegid() };
    let setup_guard = HeadGuard {
        expected_head_layer_id: binding.head_layer_id,
        expected_head_epoch: binding.head_epoch,
        ..install.guard.clone()
    };
    let permissions = store
        .read_packed_permission_snapshot(
            setup_guard.clone(),
            binding.binding.clone(),
            PermissionSnapshotQuery {
                layer_ids: [binding.head_layer_id, binding.base_revision.layer_id],
                inodes: vec![1],
                dentry: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(permissions.inodes.len(), 1);
    let mut fixture_root = permissions.inodes.into_iter().next().unwrap();
    assert_eq!(fixture_root.ino, 1);
    assert_eq!((fixture_root.uid, fixture_root.gid), (0, 0));
    assert_eq!(fixture_root.mode, 0o755);
    fixture_root.layer_id = binding.head_layer_id;
    fixture_root.uid = fixture_uid;
    fixture_root.gid = fixture_gid;
    let mut setup = VersionedMutation::empty(setup_guard, permissions.layers, 4096);
    setup.inodes.push(fixture_root);
    store
        .apply_packed_versioned_mutation(setup, binding.binding.clone())
        .await
        .unwrap();
    store
        .release_lease(ReleaseLease {
            lease_id: install.guard.lease_id,
            holder_generation: install.guard.holder_generation,
        })
        .await
        .unwrap();
    let header = store.load_volume_header().await.unwrap().unwrap();
    let mount_uid = Uuid::new_v4();
    let pod_uid = Uuid::new_v4();
    let mut child = OwnedCliChild::spawn(
        root.path(),
        &objects.path().join("objects"),
        backend_name,
        namespace,
        CliMountIdentity {
            workspace: binding.workspace_id,
            mount_uid,
            pod_uid,
        },
        killed,
        pending,
    );
    child.mounted().await;
    let (lease, _writer) = mounted_lease(backend.as_ref(), binding.workspace_id).await;
    assert!(
        matches!(
            store
                .release_lease(ReleaseLease {
                    lease_id: lease.lease_id,
                    holder_generation: lease.holder_generation
                })
                .await,
            Err(workspace_overlay::error::WorkspaceError::Fenced)
        ),
        "plain release must not retire a CLI packed mount"
    );
    let mounted_root = root.path().join("mount");
    assert_eq!(
        tokio::task::spawn_blocking(move || {
            let metadata = std::fs::metadata(&mounted_root).unwrap();
            assert!(metadata.is_dir());
            assert_eq!((metadata.uid(), metadata.gid()), (fixture_uid, fixture_gid));
            assert_eq!(metadata.mode() & 0o177777, 0o040755);
            let mut lower = std::fs::File::open(mounted_root.join("nonzero"))
                .expect("actual CLI lower OPEN failed");
            let mut bytes = Vec::new();
            lower
                .read_to_end(&mut bytes)
                .expect("actual CLI lower READ failed");
            bytes
        })
        .await
        .unwrap(),
        lower_payload
    );
    child.stage("lower-read-complete");
    let object_write_fault =
        pending.then(|| DeniedObjectWrites::new(&objects.path().join("objects")));
    let expected_payload = if pending {
        PAYLOAD
            .iter()
            .copied()
            .cycle()
            .take(4096)
            .collect::<Vec<_>>()
    } else {
        PAYLOAD.to_vec()
    };
    let upper_path = root.path().join("mount/cli-upper");
    let io_payload = expected_payload.clone();
    let mut io = Some(tokio::task::spawn_blocking(
        move || -> std::io::Result<()> {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&upper_path)?;
            file.write_all(&io_payload)?;
            if !pending {
                file.sync_all()?;
            }
            drop(file);
            if !pending {
                assert_eq!(std::fs::read(upper_path)?, io_payload);
            }
            Ok(())
        },
    ));
    let writeback_root = workspace_overlay::cache_scope::writeback_root(
        &root.path().join("pvc-cache"),
        header.volume_id,
        binding.workspace_id,
        binding.head_epoch,
    )
    .unwrap();
    let mut pending_inode = None;
    if pending {
        let cache = crate::vfs::cache::write_back::FsWriteBackCache::new_with_sync(
            writeback_root.clone(),
            true,
        );
        let pending_record = tokio::time::timeout(WAIT, async {
            'found: loop {
                if let Ok(rows) = cache.recover_packed_publication().await {
                    for record in rows {
                        if record.key.epoch != lease.holder_generation || record.length != expected_payload.len() as u64 {
                            continue;
                        }
                        assert!(matches!(record.state,
                            crate::vfs::cache::keys::DirtySliceState::Sealed
                                | crate::vfs::cache::keys::DirtySliceState::Failed
                                | crate::vfs::cache::keys::DirtySliceState::Uploading));
                        let (ino, chunk_index) = crate::vfs::extract_ino_and_chunk_index(record.chunk_id);
                        assert_eq!(ino, record.ino);
                        let rows = store.get_extent_deltas(workspace_overlay::catalog::ExtentQuery {
                            layer_ids: vec![binding.head_layer_id], ino: record.ino,
                            chunk_index, range_start: record.chunk_offset,
                            range_end: record.chunk_offset + record.length,
                        }).await.unwrap();
                        assert!(!rows.iter().any(|row| matches!(&row.kind,
                            workspace_overlay::model::ExtentKind::Data { .. })),
                            "upload-before-commit must not expose an extent while real PUT is denied");
                        assert_eq!(std::fs::read(&record.path).unwrap(), expected_payload);
                        break 'found record;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("no actual persisted pending-upload PVC slice before crash");
        let record = pending_record;
        pending_inode = Some(record.ino);
        object_write_fault.as_ref().unwrap().assert_unchanged();
    } else {
        io.take().unwrap().await.unwrap().unwrap();
        child.stage("upper-sync-read-complete");
    }
    let smallfile_hashes = if smallfile_count != 0 {
        let mount = root.path().join("mount");
        Some(
            tokio::task::spawn_blocking(move || {
                measure_smallfile_write_rename(&mount);
                write_smallfile_crash_corpus(&mount, smallfile_count)
            })
            .await
            .unwrap(),
        )
    } else {
        None
    };
    child.terminate(killed).await;
    if pending {
        let _ = tokio::time::timeout(WAIT, io.take().unwrap())
            .await
            .expect("dead CLI left kernel write caller blocked");
    }
    drop(object_write_fault);
    if !killed {
        assert_idle(backend.as_ref(), binding.workspace_id, &[lease.lease_id]).await;
        let reference = store
            .inspect_original_clean_packed_mount(binding.workspace_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reference.guard.lease_id, lease.lease_id);
        assert_eq!(reference.guard.holder_generation, lease.holder_generation);
        assert!(
            matches!(
                store
                    .admit_clean_packed_source(reference, observation_budget.clone())
                    .await
                    .unwrap(),
                PackedCleanAdmission::Ready(_)
            ),
            "normal CLI must write genuine PCR"
        );
    } else {
        let reference = PackedReleasedMountReference {
            guard: HeadGuard {
                workspace_id: binding.workspace_id,
                expected_head_layer_id: binding.head_layer_id,
                expected_head_epoch: binding.head_epoch,
                lease_id: lease.lease_id,
                holder_generation: lease.holder_generation,
            },
            mount_uid,
            pod_uid,
        };
        assert!(
            matches!(
                store
                    .admit_clean_packed_source(reference.clone(), observation_budget.clone())
                    .await
                    .unwrap(),
                PackedCleanAdmission::RequiresRecovery
            ),
            "kill -9 must not manufacture original clean release"
        );
        tokio::time::timeout(WAIT, async {
            loop {
                let (values, now) = read_rows(
                    backend.as_ref(),
                    &[format!("lease/{}/{}", binding.workspace_id, lease.lease_id).into_bytes()],
                )
                .await;
                let current: SnapshotLease = native_record(values[0].as_deref().unwrap());
                if now >= current.expires_at_ns {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        // Expiry alone grants no Ready writer. Actual CLI retry must fail before FUSE.
        let retry_config = write_mount_config(
            root.path(),
            &objects.path().join("objects"),
            backend_name,
            namespace,
            binding.workspace_id,
            false,
        );
        let mut retry = tokio::process::Command::new(executable());
        retry
            .arg("mount")
            .arg("--config")
            .arg(retry_config)
            .kill_on_drop(true)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        assert!(
            !tokio::time::timeout(WAIT, retry.status())
                .await
                .unwrap()
                .unwrap()
                .success(),
            "expired original mount must reject actual binary normal remount"
        );
        assert!(present_mount(&root.path().join("mount")).is_none());
        assert!(
            writeback_root.is_dir(),
            "recovery must reuse the actual original PVC root"
        );
        let recovery_lease = LeaseId::new();
        let recovery_pod = Uuid::new_v4();
        let generation = lease.holder_generation.checked_add(1).unwrap();
        let mut command = tokio::process::Command::new(executable());
        command
            .args(["workspace", "recover-packed-mount", "--config"])
            .arg(&child.config)
            .args([
                "--workspace",
                &binding.workspace_id.to_string(),
                "--head-layer",
                &binding.head_layer_id.to_string(),
                "--head-epoch",
                &binding.head_epoch.to_string(),
                "--original-lease",
                &lease.lease_id.to_string(),
                "--original-generation",
                &lease.holder_generation.to_string(),
                "--mount-uid",
                &mount_uid.to_string(),
                "--original-pod-uid",
                &pod_uid.to_string(),
                "--recovery-lease",
                &recovery_lease.to_string(),
                "--recovery-generation",
                &generation.to_string(),
                "--recovery-pod-uid",
                &recovery_pod.to_string(),
                "--ttl-seconds",
                "300",
            ])
            .env("RUST_LOG", "off")
            .kill_on_drop(true)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let output = tokio::time::timeout(WAIT, command.output())
            .await
            .unwrap()
            .unwrap();
        assert!(
            output.status.success(),
            "actual binary original PVC recovery failed"
        );
        assert!(output.stdout.len() <= 64 << 10);
        let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(report["format"], "packed-v3");
        assert_eq!(report["recovery_completed"], true);
        let result = store
            .inspect_recovered_packed_mount(binding.workspace_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.original, reference);
        assert_eq!(result.released.guard.lease_id, recovery_lease);
        assert_eq!(result.released.guard.holder_generation, generation);
        assert_eq!(result.released.pod_uid, recovery_pod);
        assert_idle(
            backend.as_ref(),
            binding.workspace_id,
            &[reference.guard.lease_id, result.released.guard.lease_id],
        )
        .await;
        assert!(matches!(
            store
                .admit_clean_packed_source(reference.clone(), observation_budget.clone())
                .await
                .unwrap(),
            PackedCleanAdmission::RequiresRecovery
        ));
        let pmr_key = format!(
            "packed-v3/mount-recovery/{}/{}",
            binding.workspace_id, reference.guard.lease_id
        )
        .into_bytes();
        let (values, _) = read_rows(backend.as_ref(), &[pmr_key]).await;
        let pmr = values[0].as_deref().unwrap();
        assert!(pmr.starts_with(b"PMR3\x01"));
        let completed: serde_json::Value = serde_json::from_slice(&pmr[5..]).unwrap();
        assert_eq!(completed["completed"], true);
        if let Some(ino) = pending_inode {
            let cache = crate::vfs::cache::write_back::FsWriteBackCache::new_with_sync(
                writeback_root,
                true,
            );
            assert!(
                cache.recover_packed_publication().await.unwrap().is_empty(),
                "PMR completion left the actual dirty PVC inventory pending"
            );
            let rows = store
                .get_extent_deltas(workspace_overlay::catalog::ExtentQuery {
                    layer_ids: vec![binding.head_layer_id],
                    ino,
                    chunk_index: 0,
                    range_start: 0,
                    range_end: expected_payload.len() as u64,
                })
                .await
                .unwrap();
            let latest = rows.iter().max_by_key(|row| row.sequence).unwrap();
            let workspace_overlay::model::ExtentKind::Data {
                slice_id,
                slice_offset,
            } = &latest.kind
            else {
                panic!("recovered pending data extent disappeared");
            };
            let probe_cache = tempfile::tempdir_in(root.path()).unwrap();
            let probe = ObjectBlockStore::new_with_configs_async(
                actual_client.clone(),
                ChunksCacheConfig::with_budgets(0, 0, probe_cache.path().to_path_buf()),
                BlockStoreConfig {
                    block_size: 4096,
                    compression: crate::chunk::compress::Compression::None,
                    populate_write_cache_after_upload: false,
                    range_background_prefetch: false,
                    page_cache_capacity: 0,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            let mut bytes = vec![0; expected_payload.len()];
            probe
                .read_range((*slice_id, 0), *slice_offset, &mut bytes)
                .await
                .unwrap();
            assert_eq!(
                bytes, expected_payload,
                "real recovery PUT did not preserve pending payload"
            );
        }
        let expected_reopened_carrier = if !pending && smallfile_count == 0 {
            // Quiesced kill/recovery exercises the real PMR publisher. The
            // pending-PUT fault pair keeps its independent replay scope.
            let original_pmr_bytes = pmr.to_vec();
            let published = publish_recovered_cli_source(
                connect.clone(),
                root.path(),
                actual_client.clone(),
                result.released.clone(),
            )
            .await;
            assert_eq!(published.binding.workspace_id, binding.workspace_id);
            assert!(published.binding.head_epoch > binding.head_epoch);
            assert_ne!(published.binding.head_layer_id, binding.head_layer_id);
            assert_eq!(
                published.binding.base_revision,
                published.packed_carrier_revision
            );
            assert!(
                !observation_budget.state().closed,
                "publication must not close the independent observation ledger"
            );
            let workspace = store.load_workspace(binding.workspace_id).await.unwrap();
            assert_eq!(workspace.head_layer_id, published.binding.head_layer_id);
            assert_eq!(workspace.head_epoch, published.binding.head_epoch);
            let snapshot = store.load_snapshot(published.snapshot_id).await.unwrap();
            assert_eq!(snapshot.revision, published.packed_carrier_revision);
            let view = store
                .inspect_clean_published_view(binding.workspace_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(view.snapshot_id, published.snapshot_id);
            assert_eq!(view.binding, published.binding);

            let keys = [
                format!(
                    "packed-v3/mount-recovery/{}/{}",
                    binding.workspace_id, reference.guard.lease_id
                )
                .into_bytes(),
                format!(
                    "packed-v3/recovered-source/{}/{}",
                    binding.workspace_id, result.released.guard.lease_id
                )
                .into_bytes(),
            ];
            let (rows, _) = read_rows(backend.as_ref(), &keys).await;
            assert_eq!(
                rows[0].as_deref(),
                Some(original_pmr_bytes.as_slice()),
                "source consumption must not rewrite immutable PMR completion"
            );
            let claimed = rows[1].as_deref().unwrap();
            assert!(claimed.starts_with(b"PRS3\x01"));
            let claimed: serde_json::Value = serde_json::from_slice(&claimed[5..]).unwrap();
            assert_eq!(claimed["completion"], completed);
            assert!(
                claimed["first_admin_claim"].is_object(),
                "actual recovered publisher did not consume the source"
            );
            assert!(
                claimed["first_admin_claim"]["open_owner"]["owner_id"]
                    .as_str()
                    .unwrap()
                    .starts_with("packed-v3-recovered/")
            );
            assert!(
                store
                    .inspect_recovered_packed_mount(binding.workspace_id)
                    .await
                    .unwrap()
                    .is_none(),
                "consumed PMR source must not be admitted as a fresh publication source"
            );
            let cleanup = store
                .inspect_packed_mount_recovery_for_cleanup(reference.clone())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(cleanup.original, reference);
            assert_eq!(cleanup.released, result.released);
            assert!(
                matches!(
                    store
                        .admit_clean_packed_source(reference.clone(), observation_budget.clone())
                        .await
                        .unwrap(),
                    PackedCleanAdmission::RequiresRecovery
                ),
                "publishing a recovered source must never create an original PCR"
            );
            Some(published.packed_carrier_revision)
        } else {
            None
        };
        // Read through a second genuine CLI mount after the recovery terminal
        // CAS. This also proves that normal admission resumes only after replay.
        let mut reopened = OwnedCliChild::spawn(
            root.path(),
            &objects.path().join("objects"),
            backend_name,
            namespace,
            CliMountIdentity {
                workspace: binding.workspace_id,
                mount_uid: Uuid::new_v4(),
                pod_uid: Uuid::new_v4(),
            },
            false,
            false,
        );
        reopened.mounted().await;
        if let Some(expected_carrier) = expected_reopened_carrier {
            let (fresh_lease, _) = mounted_lease(backend.as_ref(), binding.workspace_id).await;
            assert_eq!(
                fresh_lease.base_revision, expected_carrier,
                "actual CLI remount must select the newly published carrier"
            );
        }
        let upper_path = root.path().join("mount/cli-upper");
        let lower_path = root.path().join("mount/nonzero");
        let (upper_bytes, lower_bytes) = tokio::task::spawn_blocking(move || {
            (
                std::fs::read(upper_path).unwrap(),
                std::fs::read(lower_path).unwrap(),
            )
        })
        .await
        .unwrap();
        assert_eq!(upper_bytes, expected_payload);
        assert_eq!(lower_bytes, lower_payload);
        if let Some(expected) = smallfile_hashes {
            let mount = root.path().join("mount");
            tokio::task::spawn_blocking(move || verify_smallfile_crash_corpus(&mount, &expected))
                .await
                .unwrap();
        }
        reopened.terminate(false).await;
        drop(reopened);
        // The immutable original PMR remains cleanup evidence after a fresh
        // real mount advances the open owner, lease and writer incarnation.
        let cleanup = store
            .inspect_packed_mount_recovery_for_cleanup(reference.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cleanup.original, reference);
        assert_eq!(cleanup.released, result.released);
    }
    drop(child);
    drop(store);
    backend.shutdown_metadata_backend().await.unwrap();
    observation_budget.close();
    assert!(
        observation_budget
            .state()
            .used
            .iter()
            .all(|used| *used == 0)
    );
}

async fn redis_with_smallfiles(killed: bool, pending: bool, smallfile_count: usize) {
    let namespace = format!("packed-v3-cli-route-{}", Uuid::new_v4());
    let url = std::env::var("BREWFS_TEST_REDIS_URL").unwrap();
    let connect_namespace = namespace.clone();
    let connect = Arc::new(move |_budget: Arc<V3MountBudget>| {
        let url = url.clone();
        let namespace = connect_namespace.clone();
        Box::pin(async move { RedisWorkspaceBackend::connect(&url, &namespace).await })
            as Connection<RedisWorkspaceBackend>
    });
    actual_cli_chain(
        "redis",
        &namespace,
        connect,
        killed,
        pending,
        smallfile_count,
    )
    .await;
}
async fn redis(killed: bool, pending: bool) {
    redis_with_smallfiles(killed, pending, 0).await;
}
async fn tikv(killed: bool, pending: bool) {
    let namespace = format!("packed-v3-cli-route-{}", Uuid::new_v4());
    let endpoints = std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let connect_namespace = namespace.clone();
    let connect = Arc::new(move |budget: Arc<V3MountBudget>| {
        let endpoints = endpoints.clone();
        let namespace = connect_namespace.clone();
        Box::pin(async move {
            TiKvWorkspaceBackend::connect_with_budget(endpoints, &namespace, budget).await
        }) as Connection<TiKvWorkspaceBackend>
    });
    actual_cli_chain("tikv", &namespace, connect, killed, pending, 0).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real Redis, Linux FUSE and owned CLI subprocess"]
async fn real_redis_cli_mount_mutation_clean_release() {
    redis(false, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real TiKV, Linux FUSE and owned CLI subprocess"]
async fn real_tikv_cli_mount_mutation_clean_release() {
    tikv(false, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real Redis, Linux FUSE, SIGKILL expiry and original PVC recovery"]
async fn real_redis_cli_kill_expiry_pvc_recovery() {
    redis(true, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real Redis, FUSE, 8192 fsynced files, SIGKILL and per-file SHA-256 manifests"]
async fn real_redis_cli_smallfiles_kill_expiry_pvc_recovery() {
    redis_with_smallfiles(true, false, 8192).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real Redis, FUSE and isolated write/rename operation measurements"]
async fn real_redis_cli_smallfile_operation_guards() {
    redis_with_smallfiles(true, false, 1).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real TiKV, Linux FUSE, SIGKILL expiry and original PVC recovery"]
async fn real_tikv_cli_kill_expiry_pvc_recovery() {
    tikv(true, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real Redis, FUSE, denied real object PUT, SIGKILL and pending PVC replay"]
async fn real_redis_cli_pending_upload_kill_pvc_recovery() {
    redis(true, true).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real TiKV, FUSE, denied real object PUT, SIGKILL and pending PVC replay"]
async fn real_tikv_cli_pending_upload_kill_pvc_recovery() {
    tikv(true, true).await;
}
