//! Opt-in Linux FUSE traces, with an isolated Redis backend when configured.
use super::*;
use asyncfuse::raw::reply::{ReplyAttr, ReplyEntry, ReplyInit, ReplyLock, ReplyOpen};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;

type Fs = crate::vfs::fs::VFS<crate::chunk::store::InMemoryBlockStore, Meta>;

struct KernelFixture {
    _objects: tempfile::TempDir,
    meta: Meta,
    control: Arc<ValidationControl>,
    authority: Arc<dyn PackedMountAuthority>,
}

impl<B: WorkspaceKvBackend> From<Fixture<B>> for KernelFixture {
    fn from(f: Fixture<B>) -> Self {
        Self {
            _objects: f._objects,
            meta: f.meta,
            control: f.control,
            authority: f.authority,
        }
    }
}

async fn kernel_fixture(cache_bytes: u64) -> KernelFixture {
    if let Ok(url) = std::env::var("BREWFS_ENOENT_REDIS_URL") {
        assert!(
            url.starts_with("redis://127.0.0.1:"),
            "use an isolated loopback Redis"
        );
        let backend = crate::workspace::stores::redis::RedisWorkspaceBackend::connect(
            &url,
            &format!("enoent-kernel-{}", uuid::Uuid::new_v4().simple()),
        )
        .await
        .unwrap();
        eprintln!("KERNEL_BACKEND redis cache={cache_bytes}");
        fixture_with_backend(cache_bytes, Arc::new(backend))
            .await
            .into()
    } else {
        eprintln!("KERNEL_BACKEND memory cache={cache_bytes}");
        fixture_with_cache(cache_bytes).await.into()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum At {
    LookupStat,
    Getattr,
    GetattrAgain,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Change {
    Replace,
    Unlink,
    Deny,
}

struct ControlledFs {
    fs: Fs,
    meta: Arc<Meta>,
    control: Arc<ValidationControl>,
    armed: AtomicBool,
    at: At,
    change: Change,
    old: std::sync::atomic::AtomicU64,
    replacement: u64,
    changes_left: AtomicUsize,
    events: Arc<std::sync::Mutex<Vec<String>>>,
}

async fn change_name(meta: &Meta, change: Change) -> Result<(), crate::meta::store::MetaError> {
    match change {
        Change::Unlink => meta.unlink(1, "target").await,
        Change::Replace | Change::Deny => meta.rename(1, "next", 1, "target".into()).await,
    }
}

impl ControlledFs {
    fn event(&self, event: String) {
        self.events.lock().unwrap().push(event);
    }
}

impl Filesystem for ControlledFs {
    async fn init(&self, req: Request) -> asyncfuse::Result<ReplyInit> {
        Filesystem::init(&self.fs, req).await
    }
    async fn destroy(&self, req: Request) {
        Filesystem::destroy(&self.fs, req).await;
    }
    async fn lookup(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
    ) -> asyncfuse::Result<ReplyEntry> {
        let mut mutation = None;
        if name == OsStr::new("target")
            && self.at == At::LookupStat
            && self.armed.swap(false, Ordering::SeqCst)
        {
            self.meta.invalidate_upper_rows();
            self.control
                .enoent_f24
                .pause_dentry
                .store(true, Ordering::SeqCst);
            let (meta, control, events, change) = (
                self.meta.clone(),
                self.control.clone(),
                self.events.clone(),
                self.change,
            );
            mutation = Some(tokio::spawn(async move {
                let permit = tokio::time::timeout(
                    Duration::from_secs(5),
                    control.enoent_f24.reached.acquire(),
                )
                .await
                .map_err(|_| Errno::from(libc::ETIMEDOUT))?
                .map_err(|_| Errno::from(libc::EIO))?;
                permit.forget();
                let result = change_name(&meta, change).await;
                events.lock().unwrap().push(format!(
                    "change LookupStat {change:?} error={:?}",
                    result.as_ref().err()
                ));
                control.enoent_f24.resume.add_permits(1);
                result.map_err(|_| Errno::from(libc::EIO))
            }));
        }
        let result = Filesystem::lookup(&self.fs, req, parent, name).await;
        if let Some(mutation) = mutation {
            mutation.await.map_err(|_| Errno::from(libc::EIO))??;
        }
        self.event(format!(
            "lookup uid={} parent={parent} name={name:?} result={:?}",
            req.uid,
            result.as_ref().map(|reply| reply.attr.ino)
        ));
        result
    }
    async fn getattr(
        &self,
        req: Request,
        ino: u64,
        fh: Option<u64>,
        flags: u32,
    ) -> asyncfuse::Result<ReplyAttr> {
        if ino == self.old.load(Ordering::SeqCst)
            && fh.is_none()
            && matches!(self.at, At::Getattr | At::GetattrAgain)
            && self.armed.swap(false, Ordering::SeqCst)
        {
            let left = self.changes_left.fetch_sub(1, Ordering::SeqCst);
            let result = if self.at == At::GetattrAgain && left == 1 {
                self.meta.rename(1, "later", 1, "target".into()).await
            } else {
                change_name(&self.meta, self.change).await
            };
            self.event(format!(
                "change {:?} {:?} error={:?}",
                self.at,
                self.change,
                result.as_ref().err()
            ));
            result.map_err(|_| Errno::from(libc::EIO))?;
            if left > 1 {
                self.old.store(self.replacement, Ordering::SeqCst);
                self.armed.store(true, Ordering::SeqCst);
            }
        }
        let result = Filesystem::getattr(&self.fs, req, ino, fh, flags).await;
        self.event(format!(
            "getattr uid={} ino={ino} fh={fh:?} result={:?}",
            req.uid,
            result
                .as_ref()
                .map(|reply| (reply.attr.ino, reply.attr.nlink))
        ));
        result
    }
    async fn open(&self, req: Request, ino: u64, flags: u32) -> asyncfuse::Result<ReplyOpen> {
        let result = Filesystem::open(&self.fs, req, ino, flags).await;
        self.event(format!(
            "open uid={} ino={ino} result={:?}",
            req.uid,
            result.as_ref().map(|reply| reply.fh)
        ));
        result
    }
    async fn flush(&self, req: Request, ino: u64, fh: u64, owner: u64) -> asyncfuse::Result<()> {
        Filesystem::flush(&self.fs, req, ino, fh, owner).await
    }
    async fn release(
        &self,
        req: Request,
        ino: u64,
        fh: u64,
        flags: u32,
        owner: u64,
        flush: bool,
    ) -> asyncfuse::Result<()> {
        Filesystem::release(&self.fs, req, ino, fh, flags, owner, flush).await
    }
    async fn getlk(
        &self,
        req: Request,
        ino: u64,
        fh: u64,
        owner: u64,
        start: u64,
        end: u64,
        kind: u32,
        pid: u32,
    ) -> asyncfuse::Result<ReplyLock> {
        Filesystem::getlk(&self.fs, req, ino, fh, owner, start, end, kind, pid).await
    }
    async fn setlk(
        &self,
        req: Request,
        ino: u64,
        fh: u64,
        owner: u64,
        start: u64,
        end: u64,
        kind: u32,
        pid: u32,
        block: bool,
    ) -> asyncfuse::Result<()> {
        Filesystem::setlk(&self.fs, req, ino, fh, owner, start, end, kind, pid, block).await
    }
}

const CLIENT: &str = r#"
import json, os, sys
assert os.getuid() == 1000
assert os.getgid() == 1000 and os.getgroups() == []
try:
    if sys.argv[2] == 'stat':
        attr = os.stat(sys.argv[1])
    else:
        fd = os.open(sys.argv[1], os.O_RDONLY)
        try: attr = os.fstat(fd)
        finally: os.close(fd)
    print(json.dumps({'ino': attr.st_ino, 'nlink': attr.st_nlink}))
except OSError as error:
    print(json.dumps({'errno': error.errno}))
"#;

async fn run_case(at: At, change: Change, stat_only: bool, cache_bytes: u64) -> Result<(), String> {
    assert_eq!(
        unsafe { libc::geteuid() },
        0,
        "run in isolated mount/network namespaces as root; the client drops to uid 1000"
    );
    let f = kernel_fixture(cache_bytes).await;
    let meta = cached_meta(f.meta, cache_bytes);
    let old = create(&meta, "target").await;
    let new = create(&meta, "next").await;
    let later = if at == At::GetattrAgain {
        Some(create(&meta, "later").await)
    } else {
        None
    };
    if change == Change::Deny {
        meta.set_attr(
            new,
            &crate::meta::store::SetAttrRequest {
                mode: Some(0),
                ..Default::default()
            },
            crate::meta::store::SetAttrFlags::empty(),
        )
        .await
        .unwrap();
    }
    let fs = vfs(meta.clone());
    fs.set_fuse_cache_ttl(Duration::ZERO);
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let wrapper = ControlledFs {
        fs,
        meta: meta.clone(),
        control: f.control.clone(),
        armed: AtomicBool::new(true),
        at,
        change,
        old: std::sync::atomic::AtomicU64::new(old as u64),
        replacement: new as u64,
        changes_left: AtomicUsize::new(if later.is_some() { 2 } else { 1 }),
        events: events.clone(),
    };
    let directory = tempfile::tempdir().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut options = asyncfuse::MountOptions::default();
    options
        .fs_name("brewfs-enoent-isolated-test")
        .default_permissions(true)
        .allow_other(true);
    let mount = asyncfuse::raw::Session::new(options)
        .mount(wrapper, directory.path())
        .await
        .unwrap();
    let path = directory.path().join("target");
    // One userspace open/fstat or path stat. All retries must come from Linux.
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new("python3")
            .args(["-c", CLIENT])
            .arg(path)
            .arg(if stat_only { "stat" } else { "open" })
            .uid(1000)
            .gid(1000)
            .output()
    })
    .await
    .unwrap()
    .unwrap();
    let final_target = meta.lookup(1, "target").await;
    mount.unmount().await.unwrap();
    f.authority.close_renewals_and_drain().await.unwrap();
    meta.shutdown_session().await.unwrap();
    let events = events.lock().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    eprintln!(
        "KERNEL_TRACE at={at:?} change={change:?} stat={stat_only} cache={cache_bytes} old={old} new={new} status={} stdout={stdout} stderr={} events={events:?}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    if !output.status.success() {
        return Err("client failed before an assertion result".into());
    }
    let observed: serde_json::Value = serde_json::from_str(&stdout).map_err(|e| e.to_string())?;
    let expected = if at == At::GetattrAgain {
        // Linux retries ESTALE once with LOOKUP_REVAL; a second replacement
        // may exhaust that retry, but must not report a nonexistent pathname.
        serde_json::json!({"errno": libc::ESTALE})
    } else {
        match change {
            Change::Replace => serde_json::json!({"ino": new, "nlink": 1}),
            Change::Unlink => serde_json::json!({"errno": libc::ENOENT}),
            Change::Deny => serde_json::json!({"errno": libc::EACCES}),
        }
    };
    if observed != expected {
        return Err(format!("expected={expected} observed={observed}"));
    }
    if !events
        .iter()
        .any(|e| e.starts_with(&format!("change {at:?} {change:?} error=None")))
    {
        return Err("the deterministic interleaving was not reached".into());
    }
    if matches!(at, At::Getattr | At::GetattrAgain) {
        let expected = format!(
            "getattr uid=1000 ino={old} fh=None result=Err({:?})",
            Errno::from(libc::ESTALE)
        );
        if !events.contains(&expected) {
            return Err(format!("missing stale-node revalidation: {expected}"));
        }
        if events
            .iter()
            .any(|e| e.starts_with(&format!("open uid=1000 ino={old} ")))
        {
            return Err("GETATTR must fail before any OPEN of the retired inode".into());
        }
    }
    if let Some(later) = later {
        if final_target.map_err(|e| e.to_string())? != Some(later) {
            return Err("the second replacement was not published".into());
        }
        let second = format!(
            "getattr uid=1000 ino={new} fh=None result=Err({:?})",
            Errno::from(libc::ESTALE)
        );
        if !events.contains(&second)
            || events
                .iter()
                .any(|e| e.starts_with(&format!("open uid=1000 ino={new} ")))
            || events
                .iter()
                .filter(|e| e.starts_with("change GetattrAgain Replace error=None"))
                .count()
                != 2
        {
            return Err(format!(
                "missing bounded second GETATTR revalidation: {second}"
            ));
        }
    }
    if !meta.open_counts.is_empty() {
        return Err("leaked retained metadata handles".into());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated Linux mount/net namespaces, root, /dev/fuse; client runs as uid 1000"]
async fn lookup_stat_kernel_revalidates_without_userspace_retry() {
    let mut failures = Vec::new();
    for cache in [0, 64 << 20] {
        for change in [Change::Replace, Change::Unlink, Change::Deny] {
            for stat in [false, true] {
                if stat && change == Change::Deny {
                    continue;
                }
                if let Err(error) = run_case(At::LookupStat, change, stat, cache).await {
                    failures.push(format!(
                        "cache={cache} change={change:?} stat={stat}: {error}"
                    ));
                }
            }
        }
    }
    assert!(failures.is_empty(), "KERNEL_LOOKUP_STAT: {failures:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated Linux mount/net namespaces, root, /dev/fuse; client runs as uid 1000"]
async fn retired_getattr_kernel_revalidates_before_open_and_stat() {
    let mut failures = Vec::new();
    for cache in [0, 64 << 20] {
        for change in [Change::Replace, Change::Unlink, Change::Deny] {
            for stat in [false, true] {
                if stat && change == Change::Deny {
                    continue;
                }
                if let Err(error) = run_case(At::Getattr, change, stat, cache).await {
                    failures.push(format!(
                        "cache={cache} change={change:?} stat={stat}: {error}"
                    ));
                }
            }
        }
    }
    assert!(failures.is_empty(), "KERNEL_RETIRED_GETATTR: {failures:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated Linux mount/net namespaces, root, /dev/fuse; client runs as uid 1000"]
async fn repeated_getattr_replacement_exhausts_kernel_retry_as_stale() {
    let mut failures = Vec::new();
    for cache in [0, 64 << 20] {
        for stat in [false, true] {
            if let Err(error) = run_case(At::GetattrAgain, Change::Replace, stat, cache).await {
                failures.push(format!("cache={cache} stat={stat}: {error}"));
            }
        }
    }
    assert!(failures.is_empty(), "KERNEL_REPEATED_GETATTR: {failures:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated Linux mount/net namespaces, root, /dev/fuse; client runs as uid 1000"]
async fn kernel_retained_handle_preserves_absence_permissions_and_backend_errors() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    const CONTROL_CLIENT: &str = r#"
import json, os, sys
assert os.getuid() == 1000 and os.getgid() == 1000 and os.getgroups() == []
path = sys.argv[1]
fd = os.open(path, os.O_RDONLY)
print(json.dumps({'ready': True}), flush=True)
for command in sys.stdin:
    try:
        if command.strip() == 'fstat':
            attr = os.fstat(fd)
        elif command.strip() == 'absent':
            attr = os.stat(path + '-absent')
        else:
            other = os.open(path, os.O_RDONLY)
            try: attr = os.fstat(other)
            finally: os.close(other)
        result = {'ino': attr.st_ino, 'nlink': attr.st_nlink}
    except OSError as error:
        result = {'errno': error.errno}
    print(json.dumps(result), flush=True)
os.close(fd)
"#;
    for cache_bytes in [0, 64 << 20] {
        let f = kernel_fixture(cache_bytes).await;
        let meta = cached_meta(f.meta, cache_bytes);
        let old = create(&meta, "target").await;
        let new = create(&meta, "next").await;
        let fs = vfs(meta.clone());
        fs.set_fuse_cache_ttl(Duration::ZERO);
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let wrapper = ControlledFs {
            fs,
            meta: meta.clone(),
            control: f.control.clone(),
            armed: AtomicBool::new(false),
            at: At::Getattr,
            change: Change::Replace,
            old: std::sync::atomic::AtomicU64::new(old as u64),
            replacement: new as u64,
            changes_left: AtomicUsize::new(0),
            events: events.clone(),
        };
        let directory = tempfile::tempdir().unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut options = asyncfuse::MountOptions::default();
        options
            .fs_name("brewfs-enoent-control-test")
            .default_permissions(true)
            .allow_other(true);
        let mount = asyncfuse::raw::Session::new(options)
            .mount(wrapper, directory.path())
            .await
            .unwrap();
        let mut client = tokio::process::Command::new("python3");
        client
            .args(["-c", CONTROL_CLIENT])
            .arg(directory.path().join("target"))
            .uid(1000)
            .gid(1000)
            .kill_on_drop(true)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped());
        let mut client = client.spawn().unwrap();
        let mut input = client.stdin.take().unwrap();
        let mut output = BufReader::new(client.stdout.take().unwrap()).lines();
        let ready = tokio::time::timeout(Duration::from_secs(5), output.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&ready).unwrap(),
            serde_json::json!({"ready": true})
        );
        meta.rename(1, "next", 1, "target".into()).await.unwrap();
        meta.set_attr(
            new,
            &crate::meta::store::SetAttrRequest {
                mode: Some(0),
                ..Default::default()
            },
            crate::meta::store::SetAttrFlags::empty(),
        )
        .await
        .unwrap();
        let cases = [
            ("fstat", 0, serde_json::json!({"ino": old, "nlink": 0})),
            ("absent", 0, serde_json::json!({"errno": libc::ENOENT})),
            ("open", 0, serde_json::json!({"errno": libc::EACCES})),
            (
                "fstat",
                libc::EACCES,
                serde_json::json!({"errno": libc::EACCES}),
            ),
            ("fstat", libc::EIO, serde_json::json!({"errno": libc::EIO})),
            (
                "fstat",
                libc::ENOENT,
                serde_json::json!({"errno": libc::ENOENT}),
            ),
            ("fstat", 0, serde_json::json!({"ino": old, "nlink": 0})),
        ];
        let mut failures = Vec::new();
        for (command, error, expected) in cases {
            meta.invalidate_upper_rows();
            f.control
                .enoent_f24
                .inode_error
                .store(error, Ordering::SeqCst);
            input
                .write_all(format!("{command}\n").as_bytes())
                .await
                .unwrap();
            input.flush().await.unwrap();
            let line = tokio::time::timeout(Duration::from_secs(5), output.next_line())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let observed: serde_json::Value = serde_json::from_str(&line).unwrap();
            eprintln!(
                "KERNEL_CONTROL cache={cache_bytes} command={command} injected={error} expected={expected} observed={observed}"
            );
            if observed != expected {
                failures.push(format!("{command} error={error}: {observed}"));
            }
        }
        f.control.enoent_f24.inode_error.store(0, Ordering::SeqCst);
        drop(input);
        let status = tokio::time::timeout(Duration::from_secs(5), client.wait())
            .await
            .unwrap()
            .unwrap();
        mount.unmount().await.unwrap();
        f.authority.close_renewals_and_drain().await.unwrap();
        meta.shutdown_session().await.unwrap();
        eprintln!(
            "KERNEL_CONTROL_TRACE cache={cache_bytes} events={:?}",
            events.lock().unwrap()
        );
        assert!(status.success());
        assert!(meta.open_counts.is_empty());
        assert!(failures.is_empty(), "KERNEL_CONTROLS: {failures:?}");
    }
}
