//! Bounded TLS material shared by every TiKV catalog client.
//!
//! TLS authenticates a connection. It does not grant a GC role or provide
//! TiKV namespace/RPC authorization. The trusted operator factory must still
//! keep administrator credentials separate from runtime credentials.

use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

use base64::Engine as _;
use sha2::{Digest, Sha256};

use crate::workspace::error::WorkspaceError;

const MAX_TLS_PEM_BYTES: u64 = 64 << 10;
const MAX_TLS_PATH_BYTES: usize = 4096;
pub(super) const TLS_RESIDENT_METADATA_BYTES: u64 = 1 << 20;

/// Three immutable, bounded PEM files. This type carries no administrative
/// authority and deliberately has no serialization implementation.
#[derive(Clone)]
pub struct TiKvTlsConfig {
    ca_path: PathBuf,
    cert_path: PathBuf,
    key_path: PathBuf,
    // A Secret resolver can retain its temporary directory through every
    // backend clone and the SDK's resource owner, including cancellation drain.
    _path_owner: Arc<dyn Send + Sync>,
}

impl std::fmt::Debug for TiKvTlsConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("TiKvTlsConfig { material: [REDACTED] }")
    }
}

impl TiKvTlsConfig {
    /// Use persistent, immutable mounted Secret files.
    pub fn from_paths(
        ca_path: impl Into<PathBuf>,
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
    ) -> Result<Self, WorkspaceError> {
        Self::from_paths_with_owner(ca_path, cert_path, key_path, Arc::new(()))
    }

    /// Retain the owner of immutable PEM files created by a trusted Secret
    /// resolver. Callers must not edit these files after construction.
    pub fn from_paths_with_owner(
        ca_path: impl Into<PathBuf>,
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
        path_owner: Arc<dyn Send + Sync>,
    ) -> Result<Self, WorkspaceError> {
        let config = Self {
            ca_path: ca_path.into(),
            cert_path: cert_path.into(),
            key_path: key_path.into(),
            _path_owner: path_owner,
        };
        config.validate_files()?;
        Ok(config)
    }

    fn validate_files(&self) -> Result<(), WorkspaceError> {
        for path in [&self.ca_path, &self.cert_path, &self.key_path] {
            if path.as_os_str().is_empty() || path.as_os_str().len() > MAX_TLS_PATH_BYTES {
                return Err(WorkspaceError::InvalidReadPlan(
                    "TiKV TLS path is empty or exceeds its bounded schema".into(),
                ));
            }
            let metadata = std::fs::metadata(path)
                .map_err(|_| WorkspaceError::Backend("TiKV TLS material is unavailable".into()))?;
            if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_TLS_PEM_BYTES {
                return Err(WorkspaceError::InvalidReadPlan(
                    "TiKV TLS material is empty or exceeds its bounded schema".into(),
                ));
            }
        }
        Ok(())
    }

    pub(super) fn apply(
        &self,
        config: tikv_client::Config,
    ) -> Result<tikv_client::Config, WorkspaceError> {
        // In particular, never let an empty CA select the SDK's plaintext path.
        self.validate_files()?;
        Ok(config.with_security(
            self.ca_path.clone(),
            self.cert_path.clone(),
            self.key_path.clone(),
        ))
    }

    pub(super) fn leaf_fingerprint(&self) -> Result<[u8; 32], WorkspaceError> {
        self.validate_files()?;
        let file = std::fs::File::open(&self.cert_path).map_err(|_| {
            WorkspaceError::Backend("TiKV client certificate is unavailable".into())
        })?;
        let mut pem = Vec::new();
        file.take(MAX_TLS_PEM_BYTES + 1)
            .read_to_end(&mut pem)
            .map_err(|_| {
                WorkspaceError::Backend("TiKV client certificate could not be read".into())
            })?;
        if pem.len() as u64 > MAX_TLS_PEM_BYTES {
            return Err(WorkspaceError::InvalidReadPlan(
                "TiKV client certificate exceeds its bounded schema".into(),
            ));
        }
        leaf_fingerprint(&pem)
    }
}

fn leaf_fingerprint(pem: &[u8]) -> Result<[u8; 32], WorkspaceError> {
    let invalid = || WorkspaceError::InvalidReadPlan("invalid TiKV leaf certificate PEM".into());
    let pem = std::str::from_utf8(pem).map_err(|_| invalid())?;
    let mut lines = pem.lines().map(str::trim);
    if !lines
        .by_ref()
        .any(|line| line == "-----BEGIN CERTIFICATE-----")
    {
        return Err(invalid());
    }
    let mut encoded = String::new();
    for line in lines {
        if line == "-----END CERTIFICATE-----" {
            let der = base64::engine::general_purpose::STANDARD
                .decode(encoded.as_bytes())
                .map_err(|_| invalid())?;
            if der.is_empty() {
                return Err(invalid());
            }
            return Ok(Sha256::digest(der).into());
        }
        if line.bytes().any(|byte| {
            !(byte.is_ascii_alphanumeric() || byte == b'+' || byte == b'/' || byte == b'=')
        }) {
            return Err(invalid());
        }
        encoded.push_str(line);
    }
    Err(invalid())
}

#[cfg(test)]
mod tests {
    #[test]
    fn tls_file_owner_and_budget_survive_config_clone_until_terminal_drop() {
        use crate::workspace::packed::snapshot::{SnapshotBudgetPool, SnapshotMountBudget};
        let directory = std::sync::Arc::new(tempfile::tempdir().unwrap());
        let paths = ["ca.crt", "tls.crt", "tls.key"].map(|name| directory.path().join(name));
        for path in &paths {
            std::fs::write(path, b"fixture").unwrap();
        }
        let tls = super::TiKvTlsConfig::from_paths_with_owner(
            paths[0].clone(),
            paths[1].clone(),
            paths[2].clone(),
            directory.clone(),
        )
        .unwrap();
        let budget = SnapshotMountBudget::defaults();
        let config = super::super::client_config_with_tls(
            &budget,
            super::super::BOUNDED_RECEIPT_MESSAGE_BYTES,
            Some(&tls),
        )
        .unwrap();
        let retained = config.clone();
        drop(config);
        drop(tls);
        drop(directory);
        assert!(paths[2].exists());
        assert_eq!(
            budget.state().used[SnapshotBudgetPool::Metadata as usize],
            super::super::CLIENT_RESIDENT_METADATA_BYTES + super::TLS_RESIDENT_METADATA_BYTES
        );
        drop(retained);
        assert!(!paths[2].exists());
        assert_eq!(budget.state().used, [0; 8]);
    }

    #[test]
    fn empty_ca_is_rejected_before_sdk_plaintext_selection() {
        let directory = tempfile::tempdir().unwrap();
        let paths = ["ca.crt", "tls.crt", "tls.key"].map(|name| directory.path().join(name));
        std::fs::write(&paths[0], b"").unwrap();
        std::fs::write(&paths[1], b"fixture").unwrap();
        std::fs::write(&paths[2], b"fixture").unwrap();
        assert!(super::TiKvTlsConfig::from_paths(&paths[0], &paths[1], &paths[2]).is_err());
    }

    #[test]
    fn leaf_identity_cannot_change_with_pem_whitespace_or_appended_chain() {
        let first = b"-----BEGIN CERTIFICATE-----\nAQIDBA==\n-----END CERTIFICATE-----\n";
        let reformatted = b"\r\n-----BEGIN CERTIFICATE-----\r\n  AQID\r\nBA==  \r\n-----END CERTIFICATE-----\r\n-----BEGIN CERTIFICATE-----\nBQYH\n-----END CERTIFICATE-----\n";
        assert_eq!(
            super::leaf_fingerprint(first).unwrap(),
            super::leaf_fingerprint(reformatted).unwrap()
        );
        assert!(
            super::leaf_fingerprint(b"-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----")
                .is_err()
        );
    }
}
