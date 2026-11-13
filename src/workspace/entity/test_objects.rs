//! Shared real RustFS transport for explicitly selected entity integration tests.
//!
//! This follows the packed graph audit's loopback endpoint, credential and fresh
//! UUID bucket contract. The gate owns the disposable RustFS service and its
//! cleanup. Each fixture must use unique object keys; no test deletes the shared
//! bucket while other tests can still be using it.

use tokio::sync::OnceCell;

use crate::cadapter::client::ObjectBackend;
use crate::cadapter::s3::{S3Backend, S3Config};

static BUCKET_CREATED: OnceCell<()> = OnceCell::const_new();

/// Creates one isolated bucket per test process and returns the production S3
/// adapter with both SDK and adapter mutation replay disabled. Missing endpoint
/// configuration is a test failure, never a successful skipped assertion.
/// Tests using this helper are individually ignored unless selected explicitly.
pub(crate) async fn backend() -> S3Backend {
    let endpoint = std::env::var("BREWFS_PACKED_GRAPH_TEST_S3_ENDPOINT")
        .expect("explicit BREWFS_PACKED_GRAPH_TEST_S3_ENDPOINT is required for entity tests");
    let bucket = std::env::var("BREWFS_PACKED_GRAPH_TEST_S3_BUCKET")
        .expect("explicit BREWFS_PACKED_GRAPH_TEST_S3_BUCKET is required for entity tests");
    let url = url::Url::parse(&endpoint).expect("entity test endpoint must be a URL");
    assert!(
        endpoint.starts_with("http://127.0.0.1:"),
        "entity tests require an explicit loopback HTTP port"
    );
    assert_eq!(url.scheme(), "http");
    assert_eq!(url.host_str(), Some("127.0.0.1"));
    assert!(url.port().is_some());
    assert!(url.username().is_empty() && url.password().is_none());
    assert!(url.path() == "/" && url.query().is_none() && url.fragment().is_none());
    let suffix = bucket
        .strip_prefix("brewfs-graph-test-")
        .expect("entity test bucket must use the isolated test prefix");
    let uuid = uuid::Uuid::parse_str(suffix).expect("entity test bucket must contain a fresh UUID");
    assert_eq!(suffix, uuid.to_string());
    assert!(!uuid.is_nil());
    let access_key =
        std::env::var("AWS_ACCESS_KEY_ID").expect("runner AWS_ACCESS_KEY_ID is required");
    let secret_key =
        std::env::var("AWS_SECRET_ACCESS_KEY").expect("runner AWS_SECRET_ACCESS_KEY is required");
    assert!(!access_key.is_empty() && !secret_key.is_empty());

    // Use explicit runner credentials for the bucket bootstrap as well as the
    // actual adapter. A local fixture must not consult profiles or cloud IMDS.
    BUCKET_CREATED
        .get_or_init(|| async {
            let credentials = aws_sdk_s3::config::Credentials::new(
                access_key.clone(),
                secret_key.clone(),
                None,
                None,
                "brewfs-entity-rustfs-test",
            );
            let shared = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .credentials_provider(credentials)
                .load()
                .await;
            let sdk = aws_sdk_s3::Client::from_conf(
                aws_sdk_s3::config::Builder::from(&shared)
                    .endpoint_url(endpoint.clone())
                    .force_path_style(true)
                    .retry_config(
                        aws_sdk_s3::config::retry::RetryConfig::standard().with_max_attempts(1),
                    )
                    .request_checksum_calculation(
                        aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired,
                    )
                    .response_checksum_validation(
                        aws_sdk_s3::config::ResponseChecksumValidation::WhenRequired,
                    )
                    .build(),
            );
            sdk.create_bucket()
                .bucket(&bucket)
                .send()
                .await
                .expect("isolated RustFS bucket creation failed; entity fixture upload aborted");
        })
        .await;
    // Each #[tokio::test] owns its runtime. Share the bucket bootstrap only;
    // sharing a pooled SDK client could retain connections on a finished test's
    // runtime and turn the next test's nonreplaying request into a false failure.
    let backend = S3Backend::with_gc_static_credentials(
        S3Config {
            bucket,
            region: Some("us-east-1".into()),
            endpoint: Some(endpoint),
            force_path_style: true,
            max_retries: 1,
            ..Default::default()
        },
        access_key,
        secret_key,
    )
    .await
    .expect("real entity RustFS adapter initialization failed");
    assert!(backend.forbids_mutation_replay());
    backend
}
