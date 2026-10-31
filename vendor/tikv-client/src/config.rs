// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::PathBuf;
use std::time::Duration;

use serde_derive::Deserialize;
use serde_derive::Serialize;

/// The configuration for either a [`RawClient`](crate::RawClient) or a
/// [`TransactionClient`](crate::TransactionClient).
///
/// See also [`TransactionOptions`](crate::TransactionOptions) which provides more ways to configure
/// requests.
///
/// This struct is marked `#[non_exhaustive]` to allow adding new configuration options in the
/// future without breaking downstream code. Construct it via [`Config::default`] and then use the
/// `with_*` methods (or field assignment) to customize it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub struct Config {
    pub ca_path: Option<PathBuf>,
    pub cert_path: Option<PathBuf>,
    pub key_path: Option<PathBuf>,
    pub timeout: Duration,
    pub grpc_max_decoding_message_size: usize,
    /// Accept gzip responses. Disable for callers that require the wire cap
    /// to bound decoded protobuf materialization under tonic 0.10.
    pub grpc_accept_gzip: bool,
    pub bounded_get_scan_schema: bool,
    /// Interpret a single CheckTxnStatus TxnNotFound response using the
    /// resolver's existing TTL-aware recovery. Explicit opt-in for new callers;
    /// omitted/serialized configurations retain the legacy error behavior.
    #[serde(skip)]
    pub resolve_txn_not_found: bool,
    /// Classify strictly validated Get lock conflicts for an explicit caller
    /// policy. This does not dispatch retries or resolve locks.
    #[serde(skip)]
    pub bounded_read_lock_conflict_key_bytes: Option<usize>,
    /// Independent bound for PD topology and timestamp responses.
    pub pd_max_decoding_message_size: usize,
    /// Maximum retained region, store and endpoint-client records. Zero disables
    /// retention rather than silently permitting an unbounded cache.
    pub max_cached_regions: usize,
    pub max_cached_stores: usize,
    pub max_cached_kv_clients: usize,
    pub max_cache_entry_bytes: usize,
    pub tso_max_pending_groups: usize,
    pub tso_max_batch_size: usize,
    /// Request one timestamp in the caller future, without an SDK TSO task.
    pub tso_on_demand: bool,
    #[serde(skip)]
    pub resource_owner: Option<crate::ClientResourceOwner>,
    pub keyspace: Option<String>,
}

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_GRPC_MAX_DECODING_MESSAGE_SIZE: usize = 4 * 1024 * 1024; // 4MB

impl Default for Config {
    fn default() -> Self {
        Config {
            ca_path: None,
            cert_path: None,
            key_path: None,
            timeout: DEFAULT_REQUEST_TIMEOUT,
            grpc_max_decoding_message_size: DEFAULT_GRPC_MAX_DECODING_MESSAGE_SIZE,
            grpc_accept_gzip: true,
            bounded_get_scan_schema: false,
            resolve_txn_not_found: false,
            bounded_read_lock_conflict_key_bytes: None,
            pd_max_decoding_message_size: DEFAULT_GRPC_MAX_DECODING_MESSAGE_SIZE,
            max_cached_regions: usize::MAX,
            max_cached_stores: usize::MAX,
            max_cached_kv_clients: usize::MAX,
            max_cache_entry_bytes: usize::MAX,
            tso_max_pending_groups: 1 << 16,
            tso_max_batch_size: 64,
            tso_on_demand: false,
            resource_owner: None,
            keyspace: None,
        }
    }
}

impl Config {
    /// Set the certificate authority, certificate, and key locations for clients.
    ///
    /// By default, this client will use an insecure connection over instead of one protected by
    /// Transport Layer Security (TLS). Your deployment may have chosen to rely on security measures
    /// such as a private network, or a VPN layer to provide secure transmission.
    ///
    /// To use a TLS secured connection, use the `with_security` function to set the required
    /// parameters.
    ///
    /// TiKV does not currently offer encrypted storage (or encryption-at-rest).
    ///
    /// # Examples
    /// ```rust
    /// # use tikv_client::Config;
    /// let config = Config::default().with_security("root.ca", "internal.cert", "internal.key");
    /// ```
    #[must_use]
    pub fn with_security(
        mut self,
        ca_path: impl Into<PathBuf>,
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
    ) -> Self {
        self.ca_path = Some(ca_path.into());
        self.cert_path = Some(cert_path.into());
        self.key_path = Some(key_path.into());
        self
    }

    /// Set the timeout for clients.
    ///
    /// The timeout is used for all requests when using or connecting to a TiKV cluster (including
    /// PD nodes). If the request does not complete within timeout, the request is cancelled and
    /// an error returned to the user.
    ///
    /// The default timeout is two seconds.
    ///
    /// # Examples
    /// ```rust
    /// # use tikv_client::Config;
    /// # use std::time::Duration;
    /// let config = Config::default().with_timeout(Duration::from_secs(10));
    /// ```
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the maximum decoding message size for gRPC.
    #[must_use]
    pub fn with_grpc_max_decoding_message_size(mut self, size: usize) -> Self {
        self.grpc_max_decoding_message_size = size;
        self.pd_max_decoding_message_size = size;
        self
    }

    #[must_use]
    pub fn with_grpc_accept_gzip(mut self, accept: bool) -> Self {
        self.grpc_accept_gzip = accept;
        self
    }

    #[must_use]
    pub fn with_bounded_get_scan_schema(mut self, enabled: bool) -> Self {
        self.bounded_get_scan_schema = enabled;
        self
    }

    /// Enable local classification of strictly bounded Get lock conflicts.
    /// The schema guard must also be enabled. Unsupported key limits disable
    /// classification; ordinary SDK retry and lock resolution remain unchanged.
    #[must_use]
    pub fn with_bounded_read_lock_conflicts(mut self, max_key_bytes: usize) -> Self {
        self.bounded_read_lock_conflict_key_bytes =
            (1..=4096).contains(&max_key_bytes).then_some(max_key_bytes);
        self
    }

    /// Bound retained SDK state for callers with a mount memory ledger.
    /// Metadata responses are still bounded independently by the gRPC cap.
    #[must_use]
    pub fn with_bounded_resources(mut self, owner: crate::ClientResourceOwner) -> Self {
        self.pd_max_decoding_message_size = 16 << 10;
        self.max_cached_regions = 0;
        self.max_cached_stores = 0;
        // Keep one live RPC channel in the bounded store endpoint cache. A zero cache avoids
        // retained SDK state but opens a fresh TCP connection for every region
        // request; concurrent workspace writers can then exhaust ephemeral
        // ports before TiKV has time to recycle the sockets. The endpoint map
        // remains explicitly bounded and retained clients share this resource
        // owner.
        self.max_cached_kv_clients = 1;
        self.max_cache_entry_bytes = 16 << 10;
        self.tso_max_pending_groups = 1;
        self.tso_max_batch_size = 1;
        self.tso_on_demand = true;
        self.resource_owner = Some(owner);
        self
    }

    /// Set to use default keyspace.
    ///
    /// Server should enable `storage.api-version = 2` to use this feature.
    #[must_use]
    pub fn with_default_keyspace(self) -> Self {
        self.with_keyspace("DEFAULT")
    }

    /// Set the use keyspace for the client.
    ///
    /// Server should enable `storage.api-version = 2` to use this feature.
    #[must_use]
    pub fn with_keyspace(mut self, keyspace: &str) -> Self {
        self.keyspace = Some(keyspace.to_owned());
        self
    }
}
