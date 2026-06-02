use brewfs::{
    ChunkLayout, ObjectBlockStore, ObjectClient, S3Backend, S3Config, VFS,
    create_redis_meta_store_from_url,
};
use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

const OP_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
struct StressRng(u64);

impl StressRng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }

    fn range(&mut self, end: u64) -> u64 {
        if end == 0 { 0 } else { self.next() % end }
    }
}

struct DockerStack {
    network: String,
    containers: Vec<String>,
}

impl DockerStack {
    fn new(name: String) -> Self {
        Self {
            network: name,
            containers: Vec::new(),
        }
    }

    fn run(&self, args: &[String]) {
        let status = Command::new("docker")
            .args(args)
            .stdin(Stdio::null())
            .status()
            .expect("failed to run docker");
        assert!(
            status.success(),
            "docker command failed: docker {}",
            args.join(" ")
        );
    }

    fn output(&self, args: &[String]) -> std::process::Output {
        Command::new("docker")
            .args(args)
            .stdin(Stdio::null())
            .output()
            .expect("failed to run docker")
    }

    fn create_network(&self) {
        self.run(&["network".into(), "create".into(), self.network.clone()]);
    }

    fn start_redis(&mut self, name: &str, host_port: u16) {
        self.containers.push(name.to_string());
        self.run(&[
            "run".into(),
            "-d".into(),
            "--rm".into(),
            "--name".into(),
            name.into(),
            "--network".into(),
            self.network.clone(),
            "-p".into(),
            format!("127.0.0.1:{host_port}:6379"),
            "docker.io/library/redis:7.2-alpine".into(),
            "redis-server".into(),
            "--appendonly".into(),
            "yes".into(),
            "--appendfsync".into(),
            "everysec".into(),
        ]);
    }

    fn start_rustfs(&mut self, name: &str, host_port: u16) {
        self.containers.push(name.to_string());
        self.run(&[
            "run".into(),
            "-d".into(),
            "--rm".into(),
            "--name".into(),
            name.into(),
            "--network".into(),
            self.network.clone(),
            "--network-alias".into(),
            "rustfs".into(),
            "-p".into(),
            format!("127.0.0.1:{host_port}:9000"),
            "-e".into(),
            "RUSTFS_ACCESS_KEY=rustfsadmin".into(),
            "-e".into(),
            "RUSTFS_SECRET_KEY=rustfsadmin".into(),
            "rustfs/rustfs:latest".into(),
            "--address".into(),
            ":9000".into(),
            "--access-key".into(),
            "rustfsadmin".into(),
            "--secret-key".into(),
            "rustfsadmin".into(),
            "/data".into(),
        ]);
    }

    fn wait_redis(&self, name: &str) {
        let deadline = Instant::now() + Duration::from_secs(60);
        while Instant::now() < deadline {
            let out = self.output(&[
                "exec".into(),
                name.into(),
                "redis-cli".into(),
                "ping".into(),
            ]);
            if out.status.success() && String::from_utf8_lossy(&out.stdout).contains("PONG") {
                return;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        panic!("redis container did not become ready");
    }

    fn create_bucket(&self, bucket: &str) {
        let deadline = Instant::now() + Duration::from_secs(180);
        while Instant::now() < deadline {
            for op in ["create-bucket", "head-bucket"] {
                let status = Command::new("docker")
                    .args([
                        "run",
                        "--rm",
                        "--network",
                        &self.network,
                        "-e",
                        "AWS_ACCESS_KEY_ID=rustfsadmin",
                        "-e",
                        "AWS_SECRET_ACCESS_KEY=rustfsadmin",
                        "-e",
                        "AWS_DEFAULT_REGION=us-east-1",
                        "-e",
                        "AWS_EC2_METADATA_DISABLED=true",
                        "amazon/aws-cli:latest",
                        "--endpoint-url",
                        "http://rustfs:9000",
                        "s3api",
                        op,
                        "--bucket",
                        bucket,
                    ])
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()
                    .expect("failed to run aws-cli container");
                if status.success() {
                    return;
                }
            }
            std::thread::sleep(Duration::from_secs(2));
        }
        panic!("rustfs bucket was not ready");
    }
}

impl Drop for DockerStack {
    fn drop(&mut self) {
        for container in self.containers.iter().rev() {
            let _ = Command::new("docker")
                .args(["rm", "-f", container])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        let _ = Command::new("docker")
            .args(["network", "rm", &self.network])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local addr")
        .port()
}

async fn op_timeout<T>(label: &'static str, fut: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(OP_TIMEOUT, fut)
        .await
        .unwrap_or_else(|_| panic!("redis/rustfs fsstress operation timed out: {label}"))
}

async fn run_phase(
    fs: Arc<VFS<ObjectBlockStore<S3Backend>, brewfs::MetaClient<Arc<brewfs::RedisMetaStore>>>>,
    worker_count: usize,
    ops_per_worker: usize,
    seed: u64,
) {
    fs.mkdir_p("/stress").await.unwrap();
    for i in 0..64 {
        let _ = fs.create_file(&format!("/stress/f{i}")).await;
    }
    for i in 0..16 {
        let _ = fs.mkdir_p(&format!("/stress/d{i}")).await;
    }

    let mut handles = Vec::with_capacity(worker_count);
    for worker in 0..worker_count {
        let fs = fs.clone();
        handles.push(tokio::spawn(async move {
            let worker_seed = (worker as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            let mut rng = StressRng::new(seed ^ worker_seed);

            for iter in 0..ops_per_worker {
                let slot = rng.range(128) as usize;
                let path = format!("/stress/f{slot}");
                let aux = format!("/stress/x{}_{}", worker, rng.range(128));
                let dir = format!("/stress/d{}", rng.range(32));

                match rng.range(100) {
                    0..=9 => {
                        op_timeout("mkdir_p", fs.mkdir_p(&dir)).await.ok();
                    }
                    10..=19 => {
                        op_timeout("create_file", fs.create_file(&path)).await.ok();
                    }
                    20..=29 => {
                        op_timeout("link", fs.link(&path, &aux)).await.ok();
                    }
                    30..=49 => {
                        op_timeout("rename", fs.rename(&path, &aux)).await.ok();
                        if rng.range(4) == 0 {
                            op_timeout("rename back", fs.rename(&aux, &path)).await.ok();
                        }
                    }
                    50..=59 => {
                        op_timeout("unlink", fs.unlink(&path)).await.ok();
                    }
                    60..=69 => {
                        op_timeout("rmdir", fs.rmdir(&dir)).await.ok();
                    }
                    70..=79 => {
                        let size = rng.range(256 * 1024);
                        op_timeout("truncate", fs.truncate(&path, size)).await.ok();
                    }
                    80..=89 => {
                        let data = vec![worker as u8; (rng.range(4096) + 1) as usize];
                        let offset = rng.range(128 * 1024);
                        if let Ok(attr) = op_timeout("stat before write_ino", fs.stat(&path)).await
                        {
                            op_timeout("write_ino", fs.write_ino(attr.ino, offset, &data))
                                .await
                                .ok();
                        }
                    }
                    _ => {
                        op_timeout("stat", fs.stat(&path)).await.ok();
                    }
                }

                if iter % 100 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        }));
    }

    for handle in handles {
        handle.await.expect("redis/rustfs fsstress worker panicked");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
async fn test_native_fsstress_013_redis_rustfs_docker() {
    if std::env::var("BREWFS_REDIS_RUSTFS_DOCKER_TEST")
        .ok()
        .as_deref()
        != Some("1")
    {
        eprintln!("skip redis/rustfs docker test: set BREWFS_REDIS_RUSTFS_DOCKER_TEST=1");
        return;
    }

    let suffix = format!("{}-{}", std::process::id(), free_port());
    let redis_name = format!("brewfs-redis-{suffix}");
    let rustfs_container_name = format!("brewfs-rustfs-{suffix}");
    let network = format!("brewfs-fsstress-{suffix}");
    let redis_port = free_port();
    let rustfs_port = free_port();
    let bucket = format!("brewfs-fsstress-{suffix}");

    let mut docker = DockerStack::new(network);
    docker.create_network();
    docker.start_redis(&redis_name, redis_port);
    docker.start_rustfs(&rustfs_container_name, rustfs_port);
    docker.wait_redis(&redis_name);
    docker.create_bucket(&bucket);

    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "rustfsadmin");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "rustfsadmin");
        std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");
        std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
    }

    let meta_handle =
        create_redis_meta_store_from_url(&format!("redis://127.0.0.1:{redis_port}/0"))
            .await
            .unwrap();

    let s3_backend = S3Backend::with_config(S3Config {
        bucket,
        region: Some("us-east-1".to_string()),
        endpoint: Some(format!("http://127.0.0.1:{rustfs_port}")),
        force_path_style: true,
        part_size: 8 * 1024 * 1024,
        max_concurrency: 4,
        ..Default::default()
    })
    .await
    .unwrap();
    let block_store = ObjectBlockStore::new(ObjectClient::new(s3_backend));
    let fs = Arc::new(
        VFS::new(ChunkLayout::default(), block_store, meta_handle.store())
            .await
            .unwrap(),
    );

    run_phase(fs.clone(), 1, 1000, 0x0131_0001).await;
    run_phase(fs.clone(), 20, 1000, 0x0131_0002).await;
    run_phase(fs, 4, 1000, 0x0131_0003).await;
}
