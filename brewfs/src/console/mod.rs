pub mod acl;
pub mod api;
pub mod csi;
pub mod registry;
pub mod server;
pub mod trash;

use crate::config::ConsoleArgs;
use crate::control::runtime::RuntimeRegistry;
use registry::VolumeRegistry;
use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum AuthMode {
    Disabled,
    Token,
}

#[derive(Clone, PartialEq, Eq)]
pub enum AuthConfig {
    Disabled,
    Token { token: Arc<str> },
}

impl AuthConfig {
    pub fn mode(&self) -> AuthMode {
        match self {
            Self::Disabled => AuthMode::Disabled,
            Self::Token { .. } => AuthMode::Token,
        }
    }

    pub fn accepts_bearer(&self, candidate: &str) -> bool {
        match self {
            Self::Disabled => true,
            Self::Token { token } => token.as_ref() == candidate,
        }
    }
}

impl fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disabled => f.write_str("Disabled"),
            Self::Token { .. } => f
                .debug_struct("Token")
                .field("token", &"<redacted>")
                .finish(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConsoleConfig {
    pub listen: SocketAddr,
    pub state_dir: PathBuf,
    pub runtime_dir: PathBuf,
    pub static_dir: PathBuf,
    pub auth: AuthConfig,
    pub csi: ConsoleCsiConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsoleCsiConfig {
    pub enabled: bool,
    pub kubeconfig: Option<PathBuf>,
    pub driver_name: String,
}

#[derive(Debug, Clone)]
pub struct ConsoleState {
    pub auth: AuthConfig,
    pub static_dir: PathBuf,
    pub registry: VolumeRegistry,
    pub runtime_registry: RuntimeRegistry,
    pub csi_dashboard: bool,
    pub csi_adapter: Arc<dyn csi::CsiAdapter>,
    pub trash_adapter: Arc<dyn trash::TrashAdapter>,
    pub acl_adapter: Arc<dyn acl::AclAdapter>,
}

impl ConsoleConfig {
    pub fn from_args(args: ConsoleArgs) -> anyhow::Result<Self> {
        let auth = if args.dev_no_auth {
            ensure_loopback(args.listen)?;
            AuthConfig::Disabled
        } else {
            read_token_auth(args.auth_token_file.as_ref())?
        };

        Ok(Self {
            listen: args.listen,
            state_dir: args.state_dir.unwrap_or_else(default_state_dir),
            runtime_dir: args
                .runtime_dir
                .unwrap_or_else(RuntimeRegistry::default_root),
            static_dir: args
                .static_dir
                .unwrap_or_else(|| PathBuf::from("brewfs/web/console/dist")),
            auth,
            csi: ConsoleCsiConfig {
                enabled: args.enable_csi_dashboard,
                kubeconfig: args.kubeconfig,
                driver_name: args.csi_driver_name,
            },
        })
    }
}

pub async fn serve_cmd(args: ConsoleArgs) -> anyhow::Result<()> {
    let config = ConsoleConfig::from_args(args)?;
    server::serve(config).await
}

fn ensure_loopback(listen: SocketAddr) -> anyhow::Result<()> {
    let is_loopback = match listen.ip() {
        IpAddr::V4(ip) => ip.is_loopback(),
        IpAddr::V6(ip) => ip.is_loopback(),
    };
    if is_loopback {
        Ok(())
    } else {
        anyhow::bail!("--dev-no-auth is only allowed with loopback listen addresses")
    }
}

fn read_token_auth(token_file: Option<&PathBuf>) -> anyhow::Result<AuthConfig> {
    let token = if let Some(path) = token_file {
        std::fs::read_to_string(path).map_err(|err| {
            anyhow::anyhow!("failed to read auth token file {}: {err}", path.display())
        })?
    } else {
        std::env::var("BREWFS_CONSOLE_TOKEN").map_err(|_| {
            anyhow::anyhow!(
                "console auth requires --auth-token-file or BREWFS_CONSOLE_TOKEN; use --dev-no-auth only on loopback listeners"
            )
        })?
    };
    let token = token.trim().to_owned();
    if token.is_empty() {
        anyhow::bail!("console auth token must not be empty");
    }
    Ok(AuthConfig::Token {
        token: Arc::from(token),
    })
}

fn default_state_dir() -> PathBuf {
    if unsafe { libc::geteuid() } == 0 {
        return PathBuf::from("/var/lib/brewfs/console");
    }
    dirs::state_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("brewfs")
        .join("console")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConsoleArgs;
    use std::net::SocketAddr;
    use tempfile::tempdir;

    fn console_args() -> ConsoleArgs {
        ConsoleArgs {
            listen: SocketAddr::from(([127, 0, 0, 1], 8080)),
            state_dir: None,
            runtime_dir: None,
            static_dir: None,
            auth_token_file: None,
            kubeconfig: None,
            csi_driver_name: "csi.brewfs.io".to_string(),
            dev_no_auth: false,
            enable_csi_dashboard: false,
        }
    }

    #[test]
    fn reads_token_from_auth_token_file() {
        let dir = tempdir().unwrap();
        let token_file = dir.path().join("token");
        std::fs::write(&token_file, "secret-token\n").unwrap();
        let mut args = console_args();
        args.auth_token_file = Some(token_file);

        let config = ConsoleConfig::from_args(args).unwrap();

        assert_eq!(config.auth.mode(), AuthMode::Token);
        assert!(config.auth.accepts_bearer("secret-token"));
    }

    #[test]
    fn rejects_dev_no_auth_on_non_loopback_listener() {
        let mut args = console_args();
        args.dev_no_auth = true;
        args.listen = SocketAddr::from(([0, 0, 0, 0], 8080));

        let err = ConsoleConfig::from_args(args).unwrap_err();

        assert!(err.to_string().contains("loopback"));
    }

    #[test]
    fn preserves_csi_kubeconfig_for_adapter_setup() {
        let mut args = console_args();
        args.dev_no_auth = true;
        args.enable_csi_dashboard = true;
        args.kubeconfig = Some(PathBuf::from("/tmp/kubeconfig"));

        let config = ConsoleConfig::from_args(args).unwrap();

        assert!(config.csi.enabled);
        assert_eq!(
            config.csi.kubeconfig,
            Some(PathBuf::from("/tmp/kubeconfig"))
        );
        assert_eq!(config.csi.driver_name, "csi.brewfs.io");
    }
}
