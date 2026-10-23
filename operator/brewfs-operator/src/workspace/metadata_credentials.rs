//! Namespace-local metadata credentials for the trusted operator/sidecar boundary.
//! Secret references are configuration; only authenticated backend constructors
//! establish an operator session. Agent containers must never receive these values.
use std::fs::OpenOptions;
use std::io::Write as _;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context as _};
use brewfs::workspace_overlay::packed_v3::wire005::V3MountBudget;
use brewfs::workspace_overlay::stores::redis::RedisWorkspaceBackend;
use brewfs::workspace_overlay::stores::tikv::{TiKvTlsConfig, TiKvWorkspaceBackend};
use k8s_openapi::api::core::v1::Secret;
use kube::{Api, Client};
use sha2::{Digest as _, Sha256};

use super::crd::{WorkspaceCatalogBackend, WorkspaceClusterSpec};

const MAX_PEM_BYTES: usize = 64 * 1024;
const RUNTIME_COMMANDS: &str = "+auth +ping +select +client|setinfo +acl|whoami +get +mget +set +del +exists +strlen +type +time +multi +exec +zadd +zrangebylex +zrem +scan +eval +evalsha +script|load";

// Deliberately no Debug/Serialize: neither raw credentials nor TLS paths belong
// in logs, status, or agent-visible objects.
#[derive(Clone)]
pub(crate) struct OperatorMetadataCredentials {
    roles: MetadataRoles,
}

#[derive(Clone)]
enum MetadataRoles {
    Redis {
        runtime: RedisPrincipal,
        admin: RedisPrincipal,
    },
    TiKv {
        runtime: TiKvTlsConfig,
        admin: TiKvTlsConfig,
    },
}

#[derive(Clone)]
struct RedisPrincipal {
    username: String,
    password: String,
}

pub(crate) async fn load_metadata_credentials(
    client: &Client,
    namespace: &str,
    spec: &WorkspaceClusterSpec,
) -> anyhow::Result<OperatorMetadataCredentials> {
    let (runtime_name, admin_name) = spec.metadata_secret_names().map_err(anyhow::Error::msg)?;
    let secrets = Api::<Secret>::namespaced(client.clone(), namespace);
    let runtime = secrets
        .get(runtime_name)
        .await
        .context("load runtime metadata Secret")?;
    let admin = secrets
        .get(admin_name)
        .await
        .context("load operator metadata Secret")?;
    credentials_from_secrets(spec.catalog_backend, &runtime, &admin)
}

fn credentials_from_secrets(
    backend: WorkspaceCatalogBackend,
    runtime: &Secret,
    admin: &Secret,
) -> anyhow::Result<OperatorMetadataCredentials> {
    let roles = match backend {
        WorkspaceCatalogBackend::Redis => {
            let runtime = redis_principal(runtime)?;
            let admin = redis_principal(admin)?;
            if runtime.username == admin.username || runtime.password == admin.password {
                bail!("runtime and operator Redis usernames and passwords must be independent");
            }
            MetadataRoles::Redis { runtime, admin }
        }
        WorkspaceCatalogBackend::TiKv => {
            // Validate all six bounded fields before writing any private file.
            let runtime_material = tls_material(runtime)?;
            let admin_material = tls_material(admin)?;
            MetadataRoles::TiKv {
                runtime: private_tls_config(runtime_material)?,
                admin: private_tls_config(admin_material)?,
            }
        }
    };
    Ok(OperatorMetadataCredentials { roles })
}

impl OperatorMetadataCredentials {
    /// Derived server configuration contains only hashes, never raw passwords.
    /// EVAL/CAS commands serve both roles. The trusted BrewFS process and typed
    /// admin entry points enforce snapshot/discard/GC authority, not command ACLs.
    pub(crate) fn redis_acl_file(&self) -> anyhow::Result<String> {
        let MetadataRoles::Redis { runtime, admin } = &self.roles else {
            bail!("Redis ACL configuration requires Redis metadata credentials");
        };
        let runtime_hash = hex::encode(Sha256::digest(runtime.password.as_bytes()));
        let admin_hash = hex::encode(Sha256::digest(admin.password.as_bytes()));
        Ok(format!(
            "user default off -@all\nuser {} reset on #{} ~* -@all {}\nuser {} reset on #{} ~* +@all\n",
            runtime.username, runtime_hash, RUNTIME_COMMANDS, admin.username, admin_hash,
        ))
    }

    pub(crate) async fn connect_redis(
        &self,
        host: &str,
        port: u16,
        namespace: &str,
    ) -> anyhow::Result<RedisWorkspaceBackend> {
        let MetadataRoles::Redis { runtime, admin } = &self.roles else {
            bail!("Redis connection requires Redis metadata credentials");
        };
        if port == 0
            || host.is_empty()
            || !host
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.'))
        {
            bail!("invalid Redis metadata endpoint");
        }
        // Validation limits credentials to URL-safe ASCII, shared by the
        // sidecar's CLI URL construction and this authenticated operator path.
        let admin_url = format!(
            "redis://{}:{}@{host}:{port}/",
            admin.username, admin.password
        );
        let runtime_url = format!(
            "redis://{}:{}@{host}:{port}/",
            runtime.username, runtime.password
        );
        RedisWorkspaceBackend::connect_operator_admin(
            &admin_url,
            &admin.username,
            &runtime_url,
            &runtime.username,
            namespace,
        )
        .await
        .context("authenticate independent Redis metadata roles")
    }

    pub(crate) async fn connect_tikv(
        &self,
        endpoints: Vec<String>,
        namespace: &str,
        budget: Arc<V3MountBudget>,
    ) -> anyhow::Result<TiKvWorkspaceBackend> {
        let MetadataRoles::TiKv { runtime, admin } = &self.roles else {
            bail!("TiKV connection requires TiKV metadata credentials");
        };
        // Both TLS owners survive async connection, SDK lazy slots, and drain.
        // The library compares canonical leaf identities and performs bounded
        // real reads using each identity before retaining only the admin session.
        TiKvWorkspaceBackend::connect_operator_admin_with_budget(
            endpoints,
            namespace,
            budget,
            admin.clone(),
            runtime.clone(),
        )
        .await
        .context("authenticate independent TiKV metadata roles")
    }
}

fn secret_bytes<'a>(secret: &'a Secret, key: &str, max: usize) -> anyhow::Result<&'a [u8]> {
    let value = secret
        .data
        .as_ref()
        .and_then(|data| data.get(key))
        .ok_or_else(|| anyhow!("metadata Secret is missing a required key"))?;
    if value.0.is_empty() || value.0.len() > max {
        bail!("metadata Secret field has invalid size");
    }
    Ok(&value.0)
}

fn redis_principal(secret: &Secret) -> anyhow::Result<RedisPrincipal> {
    let username = url_safe_field(secret, "username", 1, 64)?;
    let password = url_safe_field(secret, "password", 16, 256)?;
    if username == "default" {
        bail!("default Redis principal cannot hold a metadata role");
    }
    Ok(RedisPrincipal { username, password })
}

fn url_safe_field(secret: &Secret, key: &str, min: usize, max: usize) -> anyhow::Result<String> {
    let bytes = secret_bytes(secret, key, max)?;
    if bytes.len() < min
        || !bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'-' | b'_'))
    {
        bail!(
            "Redis metadata credentials require bounded URL-safe ASCII letters, digits, '-' or '_'"
        );
    }
    // ASCII validation above also excludes controls, whitespace, ACL syntax,
    // URL delimiters, and invalid UTF-8 without exposing any rejected value.
    String::from_utf8(bytes.to_vec()).context("invalid Redis credential encoding")
}

fn tls_material(secret: &Secret) -> anyhow::Result<[&[u8]; 3]> {
    let ca = secret_bytes(secret, "ca.crt", MAX_PEM_BYTES)?;
    let cert = secret_bytes(secret, "tls.crt", MAX_PEM_BYTES)?;
    let key = secret_bytes(secret, "tls.key", MAX_PEM_BYTES)?;
    for (value, marker) in [(ca, "CERTIFICATE"), (cert, "CERTIFICATE")] {
        let pem = std::str::from_utf8(value).context("metadata certificate is not PEM text")?;
        if !pem.contains(&format!("-----BEGIN {marker}-----"))
            || !pem.contains(&format!("-----END {marker}-----"))
        {
            bail!("metadata certificate is missing its PEM envelope");
        }
    }
    let key_text = std::str::from_utf8(key).context("metadata private key is not PEM text")?;
    if !["PRIVATE KEY", "RSA PRIVATE KEY", "EC PRIVATE KEY"]
        .iter()
        .any(|marker| {
            key_text.contains(&format!("-----BEGIN {marker}-----"))
                && key_text.contains(&format!("-----END {marker}-----"))
        })
    {
        bail!("metadata private key is missing a supported PEM envelope");
    }
    Ok([ca, cert, key])
}

fn private_tls_config(material: [&[u8]; 3]) -> anyhow::Result<TiKvTlsConfig> {
    let owner = Arc::new(tempfile::tempdir().context("create private metadata TLS directory")?);
    let paths = [
        owner.path().join("ca.crt"),
        owner.path().join("tls.crt"),
        owner.path().join("tls.key"),
    ];
    for (path, contents) in paths.iter().zip(material) {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let mut file = options
            .open(path)
            .context("create private metadata TLS file")?;
        file.write_all(contents)
            .context("write private metadata TLS file")?;
    }
    TiKvTlsConfig::from_paths_with_owner(
        paths[0].clone(),
        paths[1].clone(),
        paths[2].clone(),
        owner,
    )
    .context("validate bounded private metadata TLS files")
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::ByteString;
    use std::collections::BTreeMap;

    fn redis_secret(username: &[u8], password: &[u8]) -> Secret {
        Secret {
            data: Some(BTreeMap::from([
                ("username".into(), ByteString(username.to_vec())),
                ("password".into(), ByteString(password.to_vec())),
            ])),
            ..Default::default()
        }
    }

    #[test]
    fn redis_roles_reject_default_shared_or_injectable_credentials() {
        let admin = redis_secret(b"operator", b"operator-value-1234");
        let runtime = redis_secret(b"runtime", b"runtime-value-12345");
        assert!(credentials_from_secrets(WorkspaceCatalogBackend::Redis, &runtime, &admin).is_ok());
        for username in [
            b"default".as_slice(),
            b"operator",
            b"bad user",
            b"bad@host",
            b"bad\nuser",
        ] {
            assert!(credentials_from_secrets(
                WorkspaceCatalogBackend::Redis,
                &redis_secret(username, b"runtime-value-12345"),
                &admin
            )
            .is_err());
        }
        for password in [
            b"short".as_slice(),
            b"operator-value-1234",
            b"runtime-value@12345",
            b"runtime-value\n12345",
            &[0xff; 16],
        ] {
            assert!(credentials_from_secrets(
                WorkspaceCatalogBackend::Redis,
                &redis_secret(b"runtime", password),
                &admin
            )
            .is_err());
        }
        assert!(redis_principal(&redis_secret(&[b'x'; 65], b"runtime-value-12345")).is_err());
        assert!(redis_principal(&redis_secret(b"runtime", &[b'x'; 257])).is_err());
        assert!(redis_principal(&Secret::default()).is_err());
    }

    #[test]
    fn generated_redis_acl_has_only_hashes_and_minimum_runtime_commands() {
        let credentials = credentials_from_secrets(
            WorkspaceCatalogBackend::Redis,
            &redis_secret(b"runtime", b"runtime-value-12345"),
            &redis_secret(b"operator", b"operator-value-1234"),
        )
        .unwrap();
        let acl = credentials.redis_acl_file().unwrap();
        assert!(!acl.contains("runtime-value-12345"));
        assert!(!acl.contains("operator-value-1234"));
        let rows: Vec<_> = acl.lines().collect();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], "user default off -@all");
        assert!(rows[1].contains(" +acl|whoami "));
        assert!(rows[1].contains(" +eval +evalsha +script|load"));
        // The shared runtime reads TIME/MGET in an atomic Redis pipeline.
        assert!(rows[1]
            .split_whitespace()
            .any(|command| command == "+multi"));
        assert!(rows[1].split_whitespace().any(|command| command == "+exec"));
        assert!(!rows[1].contains("+@all"));
        assert!(!rows[1].contains("+acl|setuser"));
        for (row, password) in [
            (rows[1], "runtime-value-12345"),
            (rows[2], "operator-value-1234"),
        ] {
            assert!(row.contains(&format!(
                "#{}",
                hex::encode(Sha256::digest(password.as_bytes()))
            )));
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    #[ignore = "requires redis-server; runs an isolated Unix-socket Redis fixture"]
    async fn recovered_runtime_acl_allows_atomic_metadata_reads() {
        use brewfs::workspace_overlay::stores::kv_backend::{KvWrite, WorkspaceKvBackend};
        use std::process::{Child, Command, Stdio};
        use std::time::{Duration, Instant};

        struct StopOnDrop(Child);
        impl Drop for StopOnDrop {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }

        let credentials = credentials_from_secrets(
            WorkspaceCatalogBackend::Redis,
            &redis_secret(b"runtime", b"runtime-value-12345"),
            &redis_secret(b"operator", b"operator-value-1234"),
        )
        .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("redis.sock");
        let acl = directory.path().join("users.acl");
        std::fs::write(&acl, credentials.redis_acl_file().unwrap()).unwrap();
        let mut server = StopOnDrop(
            Command::new("redis-server")
                .args([
                    "--port",
                    "0",
                    "--save",
                    "",
                    "--appendonly",
                    "no",
                    "--unixsocket",
                ])
                .arg(&socket)
                .arg("--aclfile")
                .arg(acl)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .spawn()
                .expect("install redis-server to run this regression"),
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        while !socket.exists() {
            assert!(
                server.0.try_wait().unwrap().is_none(),
                "Redis fixture exited"
            );
            assert!(Instant::now() < deadline, "Redis fixture startup timed out");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let backend = RedisWorkspaceBackend::connect(
            &format!(
                "redis+unix://{}?user=runtime&pass=runtime-value-12345",
                socket.display()
            ),
            "acl-regression",
        )
        .await
        .unwrap();
        let key = b"test-key".to_vec();
        assert!(backend
            .compare_and_swap(
                &[],
                &[KvWrite::Put {
                    key: key.clone(),
                    value: b"value".to_vec()
                }]
            )
            .await
            .unwrap());
        let (values, now) = backend
            .get_many_with_time(std::slice::from_ref(&key))
            .await
            .unwrap();
        assert_eq!(values, vec![Some(b"value".to_vec())]);
        assert!(now > 0);
        let (values, later) = backend.get_many_consistent_with_time(&[key]).await.unwrap();
        assert_eq!(values, vec![Some(b"value".to_vec())]);
        assert!(later >= now);
    }

    #[test]
    fn tikv_material_requires_all_bounded_pem_fields() {
        let valid = Secret {
            data: Some(BTreeMap::from([
                (
                    "ca.crt".into(),
                    ByteString(
                        b"-----BEGIN CERTIFICATE-----\neA==\n-----END CERTIFICATE-----\n".to_vec(),
                    ),
                ),
                (
                    "tls.crt".into(),
                    ByteString(
                        b"-----BEGIN CERTIFICATE-----\neA==\n-----END CERTIFICATE-----\n".to_vec(),
                    ),
                ),
                (
                    "tls.key".into(),
                    ByteString(
                        b"-----BEGIN PRIVATE KEY-----\neA==\n-----END PRIVATE KEY-----\n".to_vec(),
                    ),
                ),
            ])),
            ..Default::default()
        };
        assert!(tls_material(&valid).is_ok());
        for key in ["ca.crt", "tls.crt", "tls.key"] {
            let mut incomplete = valid.clone();
            incomplete.data.as_mut().unwrap().remove(key);
            assert!(tls_material(&incomplete).is_err());
            for value in [
                vec![],
                vec![b'x'; MAX_PEM_BYTES + 1],
                vec![0xff],
                b"plaintext".to_vec(),
            ] {
                let mut invalid = valid.clone();
                invalid
                    .data
                    .as_mut()
                    .unwrap()
                    .insert(key.into(), ByteString(value));
                assert!(tls_material(&invalid).is_err());
            }
        }
    }
}
