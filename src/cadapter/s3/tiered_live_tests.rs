//! Explicit upload and readback steps. The caller owns service preparation,
//! restart, shard inspection and retention; these tests never delete evidence.

use super::*;
use serde_json::{Value, json};
use std::sync::Mutex;

#[derive(Debug, Clone, Default)]
struct WireEvidence(Arc<Mutex<Vec<Value>>>);

impl aws_sdk_s3::config::Intercept for WireEvidence {
    fn name(&self) -> &'static str {
        "TieredWireEvidence"
    }

    fn read_before_transmit(
        &self,
        context: &aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextRef<'_>,
        _: &aws_sdk_s3::config::RuntimeComponents,
        _: &mut aws_sdk_s3::config::ConfigBag,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let request = context.request();
        self.0.lock().unwrap().push(json!({
            "event": "request", "method": request.method(),
            "uri": request.uri().to_string(),
            "hint": request.headers().get("x-amz-meta-rustfs-ec-block-size-hint"),
            "content_length": request.headers().get("content-length"),
            "range": request.headers().get("range"),
        }));
        Ok(())
    }

    fn read_before_deserialization(
        &self,
        context: &aws_sdk_s3::config::interceptors::BeforeDeserializationInterceptorContextRef<'_>,
        _: &aws_sdk_s3::config::RuntimeComponents,
        _: &mut aws_sdk_s3::config::ConfigBag,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let response = context.response();
        self.0.lock().unwrap().push(json!({
            "event": "response", "http_status": response.status().as_u16(),
            "applied_block": response.headers().get("x-rustfs-effective-ec-block-size"),
            "hint_status": response.headers().get("x-rustfs-layout-hint-status"),
            "hint_reason": response.headers().get("x-rustfs-layout-hint-reason"),
        }));
        Ok(())
    }
}

async fn backend() -> (S3Backend, WireEvidence, std::path::PathBuf) {
    let output =
        std::env::var("BREWFS_TIERED_EVIDENCE").expect("dedicated existing evidence directory");
    let bucket = std::env::var("BREWFS_S3_BUCKET").expect("dedicated existing bucket");
    let endpoint = std::env::var("BREWFS_S3_ENDPOINT").expect("isolated RustFS endpoint");
    let config = S3Config {
        bucket,
        endpoint: Some(endpoint),
        region: Some("us-east-1".into()),
        force_path_style: true,
        part_size: 5 * 1024 * 1024,
        max_concurrency: 1,
        max_retries: 2,
        rustfs_ec_block_size_hint: Some("tiered".parse().unwrap()),
        ..Default::default()
    };
    let mut backend = S3Backend::with_static_credentials(
        config,
        std::env::var("AWS_ACCESS_KEY_ID").expect("test access key"),
        std::env::var("AWS_SECRET_ACCESS_KEY").expect("test secret key"),
    )
    .await
    .unwrap();
    let capture = WireEvidence::default();
    backend.client = Client::from_conf(
        backend
            .client
            .config()
            .to_builder()
            .interceptor(capture.clone())
            .build(),
    );
    (backend, capture, output.into())
}

fn payload(length: usize) -> Bytes {
    use rand::{RngCore, SeedableRng};
    let mut data = vec![0; length];
    rand::rngs::StdRng::seed_from_u64(length as u64 + 31).fill_bytes(&mut data);
    Bytes::from(data)
}

async fn verify_reads(backend: &S3Backend, key: &str, data: &[u8]) {
    assert_eq!(
        backend.get_object(key).await.unwrap().as_deref(),
        Some(data)
    );
    for offset in [
        0,
        16_383,
        32_767,
        65_535,
        262_143,
        524_287,
        1_048_575,
        data.len().saturating_sub(1),
    ] {
        if offset >= data.len() {
            continue;
        }
        let length = 4096.min(data.len() - offset);
        let mut actual = vec![0; length];
        assert_eq!(
            backend
                .get_object_range(key, offset as u64, &mut actual)
                .await
                .unwrap(),
            length
        );
        assert_eq!(
            actual,
            data[offset..offset + length],
            "{key} Range {offset}"
        );
    }
}

fn write_new(path: &std::path::Path, value: &Value) {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    serde_json::to_writer_pretty(&mut file, value).unwrap();
    writeln!(file).unwrap();
}

#[tokio::test]
#[ignore = "step 1: isolated RustFS, all four gates enabled, existing bucket and BREWFS_TIERED_EVIDENCE"]
async fn tiered_live_upload_evidence() {
    let (mut backend, capture, output) = backend().await;
    assert!(
        !output.join("objects.json").exists(),
        "use a fresh owned prefix and evidence directory"
    );
    let mut rows = Vec::new();
    for (length, expected) in [
        (0, None),
        (32_767, None),
        (32_768, None),
        (32_769, None),
        (262_143, None),
        (262_144, None),
        (262_145, Some(65_536)),
        (1_048_575, Some(65_536)),
        (1_048_576, Some(65_536)),
        (1_048_577, Some(262_144)),
        (4_194_303, Some(262_144)),
        (4_194_304, Some(262_144)),
        (4_194_305, Some(1_048_576)),
        (16_777_215, Some(1_048_576)),
        (16_777_216, Some(1_048_576)),
        (16_777_217, Some(4_194_304)),
        (17, None),
    ] {
        let data = payload(length);
        for mode in [
            "simple",
            "vectored",
            "create-only",
            "create-only-durable",
            "multipart",
            "multipart-vectored",
        ] {
            if mode.starts_with("multipart") && length <= 5 * 1024 * 1024 {
                continue;
            }
            let key = format!("tiered-v1/{length}/{mode}");
            capture.0.lock().unwrap().clear();
            match mode {
                "simple" => backend.put_object_simple(&key, &data).await.unwrap(),
                "vectored" => backend
                    .put_object_vectored_simple(
                        &key,
                        vec![data.slice(..length / 2), data.slice(length / 2..)],
                    )
                    .await
                    .unwrap(),
                "create-only" => backend.put_object_create_only(&key, &data).await.unwrap(),
                "create-only-durable" => backend
                    .put_object_create_only_durable(&key, &data)
                    .await
                    .unwrap(),
                "multipart" => backend.multipart_upload(&key, &data).await.unwrap(),
                "multipart-vectored" => backend
                    .multipart_upload_vectored(
                        &key,
                        vec![data.slice(..length / 2), data.slice(length / 2..)],
                    )
                    .await
                    .unwrap(),
                _ => unreachable!(),
            }
            let uploads = capture.0.lock().unwrap().clone();
            if let Some(expected) = expected {
                let expected_hint = expected.to_string();
                assert!(uploads.iter().any(|e| e["event"] == "request"
                    && e["hint"].as_str() == Some(expected_hint.as_str())));
                assert!(uploads.iter().any(|e| e["hint_status"] == "applied"));
            } else {
                assert!(uploads.iter().all(|e| e["hint"].is_null()));
            }
            capture.0.lock().unwrap().clear();
            let head = backend
                .client
                .head_object()
                .bucket(backend.bucket_name())
                .key(&key)
                .send()
                .await
                .unwrap();
            assert_eq!(head.content_length(), Some(length as i64));
            let heads = capture.0.lock().unwrap().clone();
            let expected_block = expected.unwrap_or(1_048_576).to_string();
            assert!(
                heads
                    .iter()
                    .any(|e| e["applied_block"].as_str() == Some(expected_block.as_str()))
            );
            verify_reads(&backend, &key, &data).await;
            rows.push(json!({"key": key, "object_bytes": length, "policy": "tiered-v1", "mode": mode,
                "requested_hint": expected, "reason": if expected.is_some() {"size-tier"} else {"small-default"},
                "upload": uploads, "head": heads, "get_range_ok": true}));
            // Preserve completed evidence if a later probe fails.
            write_new(
                &output.join(format!("object-{}.json", rows.len())),
                rows.last().unwrap(),
            );
        }
    }
    // Old-policy objects in the same enabled backend must remain readable.
    for policy in ["off", "auto", "65536", "262144", "1048576", "4194304"] {
        let length = 1_048_577;
        let key = format!("compat/{policy}");
        backend.config.rustfs_ec_block_size_hint = Some(policy.parse().unwrap());
        let data = payload(length);
        backend.put_object(&key, &data).await.unwrap();
        verify_reads(&backend, &key, &data).await;
        rows.push(json!({"key":key,"object_bytes":length,"policy":policy,"get_range_ok":true}));
    }
    write_new(
        &output.join("objects.json"),
        &json!({"bucket":backend.bucket_name(),"objects":rows}),
    );
}

#[tokio::test]
#[ignore = "step 2: caller restarts RustFS first, then reuses BREWFS_TIERED_EVIDENCE from step 1"]
async fn tiered_live_readback_evidence() {
    let (mut backend, capture, output) = backend().await;
    backend.config.rustfs_ec_block_size_hint = Some(RustfsEcBlockSizeHint::Off);
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(output.join("objects.json")).unwrap()).unwrap();
    assert_eq!(manifest["bucket"], backend.bucket_name());
    for row in manifest["objects"].as_array().unwrap() {
        let data = payload(row["object_bytes"].as_u64().unwrap() as usize);
        verify_reads(&backend, row["key"].as_str().unwrap(), &data).await;
    }
    assert!(
        capture
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e["event"] == "request")
            .all(|e| e["method"] == "GET")
    );
    write_new(
        &output.join("readback.json"),
        &json!({"all_content_verified":true, "requests":*capture.0.lock().unwrap()}),
    );
}
