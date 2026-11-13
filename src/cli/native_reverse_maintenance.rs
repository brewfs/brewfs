//! Explicit, bounded reverse-index maintenance through authenticated catalogs.

use super::*;
use crate::workspace::error::WorkspaceError;
use crate::workspace::packed::snapshot::{SnapshotBudgetPool, SnapshotMountBudget};
use crate::workspace::stores::kv_backend::WorkspaceKvBackend;
use crate::workspace::stores::tikv::TiKvTlsConfig;
use serde::Deserialize;
use std::io::Read;
use std::path::Path;

const CREDENTIAL_BYTES: u64 = 16 << 10;

// No Debug implementation: connection URLs can contain passwords.
#[derive(Deserialize)]
#[serde(tag = "backend", rename_all = "lowercase", deny_unknown_fields)]
enum Credentials {
    Redis {
        admin_url: String,
        admin_principal: String,
        runtime_url: String,
        runtime_principal: String,
    },
    Tikv {
        admin_tls: crate::config::TiKvTlsFileConfig,
        runtime_tls: crate::config::TiKvTlsFileConfig,
    },
}

fn credentials(path: &Path) -> anyhow::Result<Credentials> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options
        .open(path)
        .map_err(|_| anyhow::anyhow!("administrator credentials are unavailable"))?;
    let metadata = file.metadata()?;
    anyhow::ensure!(
        metadata.is_file() && metadata.len() > 0 && metadata.len() <= CREDENTIAL_BYTES,
        "administrator credentials must be a bounded regular file"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        anyhow::ensure!(
            metadata.permissions().mode() & 0o077 == 0,
            "administrator credentials must be private to their owner"
        );
    }
    let mut bytes = Vec::new();
    file.take(CREDENTIAL_BYTES + 1).read_to_end(&mut bytes)?;
    anyhow::ensure!(
        bytes.len() as u64 <= CREDENTIAL_BYTES,
        "administrator credentials exceed the bounded schema"
    );
    // Do not include serde's input-dependent error text in a credential error.
    serde_json::from_slice(&bytes)
        .map_err(|_| anyhow::anyhow!("invalid administrator credential schema"))
}

fn tls(config: crate::config::TiKvTlsFileConfig) -> anyhow::Result<TiKvTlsConfig> {
    config.validate()?;
    Ok(TiKvTlsConfig::from_paths(
        config.ca_path,
        config.cert_path,
        config.key_path,
    )?)
}

fn catalog_error(error: WorkspaceError) -> anyhow::Error {
    // SDK transport failures may contain endpoint URLs or credential paths.
    // Preserve typed catalog refusal reasons without forwarding SDK text.
    match error {
        WorkspaceError::Backend(_) => anyhow::anyhow!("native reverse catalog operation failed"),
        error => error.into(),
    }
}

pub(super) async fn run(
    arguments: NativeReverseIndexArgs,
    backend: WorkspaceMetaBackendKind,
    meta_url: String,
    endpoints: Vec<String>,
    tls_override: Option<crate::config::TiKvTlsFileConfig>,
    namespace: String,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        (1..=256).contains(&arguments.max_pages),
        "native reverse maintenance page limit must be between 1 and 256"
    );
    let budget = SnapshotMountBudget::from_env()?;
    let _credentials_owner = budget
        .admit(&[(SnapshotBudgetPool::Metadata, 64 << 10)])
        .map_err(|_| anyhow::anyhow!("administrator credential budget exhausted"))?;
    match (backend, credentials(&arguments.admin_credentials)?) {
        (
            WorkspaceMetaBackendKind::Redis,
            Credentials::Redis {
                admin_url,
                admin_principal,
                runtime_url,
                runtime_principal,
            },
        ) => {
            anyhow::ensure!(
                tls_override.is_none() && endpoints.is_empty(),
                "Redis reverse maintenance cannot use TiKV routing or TLS arguments"
            );
            anyhow::ensure!(
                meta_url == DEFAULT_META_URL || meta_url == admin_url,
                "metadata URL disagrees with administrator credentials"
            );
            let backend = RedisWorkspaceBackend::connect_operator_admin(
                &admin_url,
                &admin_principal,
                &runtime_url,
                &runtime_principal,
                &namespace,
            )
            .await
            .map_err(|_| {
                anyhow::anyhow!("Redis administrator authentication or connection failed")
            })?;
            maintain(backend, arguments, budget).await
        }
        (
            WorkspaceMetaBackendKind::TiKv,
            Credentials::Tikv {
                admin_tls,
                runtime_tls,
            },
        ) => {
            anyhow::ensure!(!endpoints.is_empty(), "TiKV PD endpoints are required");
            anyhow::ensure!(
                meta_url == DEFAULT_META_URL,
                "TiKV reverse maintenance cannot use a metadata URL override"
            );
            anyhow::ensure!(
                tls_override
                    .as_ref()
                    .is_none_or(|value| value == &admin_tls),
                "TiKV TLS override disagrees with administrator credentials"
            );
            let backend = TiKvWorkspaceBackend::connect_operator_admin_with_budget(
                endpoints,
                &namespace,
                budget.clone(),
                tls(admin_tls)?,
                tls(runtime_tls)?,
            )
            .await
            .map_err(|_| {
                anyhow::anyhow!("TiKV administrator authentication or connection failed")
            })?;
            maintain(backend, arguments, budget).await
        }
        _ => anyhow::bail!(
            "native reverse maintenance requires matching Redis or TiKV administrator credentials"
        ),
    }
}

async fn maintain<B: WorkspaceKvBackend>(
    backend: B,
    arguments: NativeReverseIndexArgs,
    budget: Arc<SnapshotMountBudget>,
) -> anyhow::Result<()> {
    let backend = Arc::new(backend);
    let store =
        KvWorkspaceStore::from_arc(backend.clone()).with_packed_reader_pin_budget(budget.clone());
    let result: anyhow::Result<serde_json::Value> = async {
        backend
            .authenticate_gc_admin()
            .await
            .map_err(catalog_error)?;
        let header = store
            .load_volume_header()
            .await
            .map_err(catalog_error)?
            .ok_or_else(|| {
                anyhow::anyhow!("native reverse maintenance requires an initialized volume")
            })?;
        anyhow::ensure!(
            header.volume_format == "workspace"
                && header.schema_version == WORKSPACE_SCHEMA_VERSION
                && !header.volume_id.is_nil(),
            "native reverse maintenance volume format is invalid"
        );
        if arguments.initialize_gc_incarnation {
            store
                .initialize_native_reverse_deleting_identity(arguments.layer, budget.clone())
                .await
                .map_err(catalog_error)?;
            return Ok(serde_json::json!({
                "format": "packed", "layer": arguments.layer,
                "gc_incarnation_initialized": true, "ready": false,
                "pages_completed": 0
            }));
        }
        if arguments.start {
            store
                .start_native_reverse_index(arguments.layer, budget.clone())
                .await
                .map_err(catalog_error)?;
        }
        let mut ready = false;
        let mut pages = 0;
        while pages < arguments.max_pages {
            ready = store
                .advance_native_reverse_index(arguments.layer, budget.clone())
                .await
                .map_err(catalog_error)?;
            pages += 1;
            if ready {
                break;
            }
        }
        Ok(
            serde_json::json!({"format": "packed", "layer": arguments.layer,
            "ready": ready, "pages_completed": pages}),
        )
    }
    .await;
    // Always drain the backend, including a rejected or uncertain work result.
    // A success report is emitted only after shutdown has completed.
    let shutdown = store
        .shutdown_metadata_backend()
        .await
        .map_err(catalog_error);
    let report = result?;
    shutdown?;
    println!("{report}");
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn maintenance_credentials_are_bounded_private_and_redacted() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("credentials.json");
        let sentinel = "must-not-appear-in-error";
        std::fs::write(&path, format!("{{\"backend\":\"{sentinel}\"}}")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let error = credentials(&path).err().unwrap().to_string();
        assert!(!error.contains(sentinel));
        std::fs::write(&path, vec![b' '; CREDENTIAL_BYTES as usize + 1]).unwrap();
        assert!(credentials(&path).is_err());
        std::fs::write(&path, b"{}").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(credentials(&path).is_err());
        let link = directory.path().join("link.json");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert!(credentials(&link).is_err());
    }

    #[test]
    fn maintenance_cli_requires_explicit_routing_and_bounds_pages() {
        let layer = uuid::Uuid::new_v4().to_string();
        let parse = |pages: &str| {
            Cli::try_parse_from([
                "brewfs",
                "workspace",
                "--meta-backend",
                "redis",
                "index-native-reverse",
                "--admin-credentials",
                "/private/admin.json",
                "--layer",
                &layer,
                "--max-pages",
                pages,
            ])
        };
        assert!(parse("0").is_err());
        assert!(parse("257").is_err());
        let parsed = parse("1").unwrap();
        let Command::Workspace(args) = parsed.cmd else {
            panic!("workspace command")
        };
        let WorkspaceCommand::IndexNativeReverse(args) = args.command else {
            panic!("reverse command")
        };
        assert_eq!(args.max_pages, 1);
        assert!(!args.start);
        assert!(!args.initialize_gc_incarnation);
        let options = [
            "brewfs",
            "workspace",
            "index-native-reverse",
            "--admin-credentials",
            "/private/admin.json",
            "--layer",
            &layer,
            "--initialize-gc-incarnation",
        ];
        let parsed = Cli::try_parse_from(options).unwrap();
        let Command::Workspace(args) = parsed.cmd else {
            panic!("workspace command")
        };
        let WorkspaceCommand::IndexNativeReverse(args) = args.command else {
            panic!("reverse command")
        };
        assert!(args.initialize_gc_incarnation);
        let mut conflicting = options.to_vec();
        conflicting.push("--start");
        assert!(Cli::try_parse_from(conflicting).is_err());
    }

    #[test]
    fn maintenance_transport_errors_do_not_expose_connection_material() {
        let secret = "private-credential-must-not-appear";
        let error = catalog_error(WorkspaceError::Backend(format!(
            "connect redis://operator:{secret}@host"
        )));
        assert!(!format!("{error:#}").contains(secret));
        assert!(matches!(
            catalog_error(WorkspaceError::Busy).downcast_ref::<WorkspaceError>(),
            Some(WorkspaceError::Busy)
        ));
    }
}
