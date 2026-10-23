#![cfg(feature = "workspace-operator")]

use std::fs::{self, File};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct StopOnDrop(Child);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn startup_selects_crypto_provider_before_constructing_kubernetes_client() {
    let directory = tempfile::tempdir().unwrap();
    // Reserve a local endpoint. No real Kubernetes cluster is contacted.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let config = serde_json::json!({
        "apiVersion": "v1", "kind": "Config", "current-context": "test",
        "clusters": [{"name": "test", "cluster": {
            "server": format!("https://{}", listener.local_addr().unwrap()),
            "insecure-skip-tls-verify": true
        }}],
        "contexts": [{"name": "test", "context": {"cluster": "test", "user": "test"}}],
        "users": [{"name": "test", "user": {}}]
    });
    let config_path = directory.path().join("config.json");
    fs::write(&config_path, config.to_string()).unwrap();
    let log_path = directory.path().join("startup.log");
    let output = File::create(&log_path).unwrap();
    let mut child = StopOnDrop(
        Command::new(env!("CARGO_BIN_EXE_brewfs-operator"))
            .arg("run")
            .env("KUBECONFIG", config_path)
            .env_remove("KUBERNETES_SERVICE_HOST")
            .env("NO_PROXY", "*")
            .env("no_proxy", "*")
            .env("RUST_LOG", "info")
            .stdin(Stdio::null())
            .stdout(output.try_clone().unwrap())
            .stderr(output)
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let log = fs::read_to_string(&log_path).unwrap();
        if log.contains("starting BrewFS controllers") {
            assert!(!log.contains("panicked"), "{log}");
            return;
        }
        assert!(
            child.0.try_wait().unwrap().is_none() && Instant::now() < deadline,
            "operator did not construct its Kubernetes client: {log}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}
