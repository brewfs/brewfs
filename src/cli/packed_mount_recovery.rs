//! Production same-PVC packed recovery. The command accepts identity hints;
//! only the private scoped metadata/PVC driver can create recovery authority.

use super::*;
use crate::workspace::lifecycle::PackedMountCatalog;
use crate::workspace::packed::snapshot::SnapshotMountBudget;
use crate::workspace::stores::kv_backend::WorkspaceKvBackend;
use crate::workspace::stores::kv_store::packed_admin::{
    PackedMountedRecoveryRequest, PackedReleasedMountReference,
};

pub(super) async fn run(
    arguments: PackedMountRecoveryArgs,
    meta_url_override: String,
    tls_override: Option<crate::config::TiKvTlsFileConfig>,
) -> anyhow::Result<()> {
    let cli = Cli::try_parse_from([
        std::ffi::OsStr::new("brewfs"),
        std::ffi::OsStr::new("mount"),
        std::ffi::OsStr::new("--config"),
        arguments.config.as_os_str(),
    ])?;
    let Command::Mount(mount_args) = cli.cmd else {
        unreachable!("internal mount configuration parser");
    };
    let mut config = MountConfig::from_sources(*mount_args)?;
    if let Some(tls) = tls_override
        && (!matches!(config.meta_backend, MetaBackendKind::TiKv)
            || config.meta_tikv_tls.as_ref() != Some(&tls))
    {
        anyhow::bail!("packed recovery TLS paths must match the original mount configuration");
    }
    if meta_url_override != DEFAULT_META_URL {
        if !matches!(config.meta_backend, MetaBackendKind::Redis) {
            anyhow::bail!(
                "packed recovery metadata URL override requires the original Redis configuration"
            );
        }
        config.meta_url = meta_url_override;
    }
    if config.volume_format != VolumeFormat::Workspace
        || config.workspace != Some(*arguments.workspace.as_uuid())
        || arguments.recovery_generation <= arguments.original_generation
        || arguments.recovery_lease == arguments.original_lease
        || arguments.recovery_pod_uid == arguments.original_pod_uid
        || arguments.ttl_seconds == 0
        || arguments.ttl_seconds > 900
    {
        anyhow::bail!("packed recovery identities or original mount configuration disagree");
    }
    namespace_flat_volume_cache(&mut config)?;
    let budget = SnapshotMountBudget::from_env()?;
    match config.meta_backend {
        MetaBackendKind::Redis => {
            let backend =
                RedisWorkspaceBackend::connect(&config.meta_url, &config.workspace_namespace)
                    .await?;
            let catalog = Arc::new(
                KvWorkspaceStore::new(backend).with_packed_reader_pin_budget(budget.clone()),
            );
            let result = dispatch(catalog.clone(), budget, config, arguments).await;
            let shutdown = catalog.shutdown_metadata_backend().await;
            result?;
            shutdown?;
            Ok(())
        }
        MetaBackendKind::TiKv => {
            let backend = connect_workspace_tikv(
                config.meta_tikv_pd_endpoints.clone(),
                &config.workspace_namespace,
                budget.clone(),
                config.meta_tikv_tls.as_ref(),
            )
            .await?;
            let catalog = Arc::new(
                KvWorkspaceStore::new(backend).with_packed_reader_pin_budget(budget.clone()),
            );
            let result = dispatch(catalog.clone(), budget, config, arguments).await;
            let shutdown = catalog.shutdown_metadata_backend().await;
            result?;
            shutdown?;
            Ok(())
        }
        MetaBackendKind::Sqlx | MetaBackendKind::Etcd => {
            anyhow::bail!("packed mounted recovery requires Redis or TiKV metadata");
        }
    }
}

async fn dispatch<B: WorkspaceKvBackend>(
    catalog: Arc<KvWorkspaceStore<B>>,
    budget: Arc<SnapshotMountBudget>,
    config: MountConfig,
    arguments: PackedMountRecoveryArgs,
) -> anyhow::Result<()> {
    match config.data_backend {
        DataBackendKind::LocalFs => {
            let client = create_localfs_client(&config)?;
            recover_with_client(catalog, budget, config, arguments, client.clone(), client).await
        }
        DataBackendKind::S3 => {
            let client = create_s3_client(&config).await?;
            let mut upper_config = config.clone();
            if let Some(bucket) = catalog
                .load_workspace(arguments.workspace)
                .await?
                .upper_bucket
            {
                upper_config.s3_bucket = Some(bucket);
            }
            // Use an independent backend: selecting the upper on a cloned
            // S3 client would also redirect the shared packed-lower reader.
            let upper_client = create_s3_client(&upper_config).await?;
            recover_with_client(catalog, budget, config, arguments, client, upper_client).await
        }
    }
}

async fn recover_with_client<B, O>(
    catalog: Arc<KvWorkspaceStore<B>>,
    budget: Arc<SnapshotMountBudget>,
    config: MountConfig,
    arguments: PackedMountRecoveryArgs,
    client: ObjectClient<O>,
    upper_client: ObjectClient<O>,
) -> anyhow::Result<()>
where
    B: WorkspaceKvBackend,
    O: ObjectBackend + Clone + Send + Sync + 'static,
{
    let header = catalog
        .packed_mount_header(arguments.workspace)
        .await?
        .ok_or_else(|| anyhow::anyhow!("packed recovery binding is absent"))?;
    if header.volume_format != "workspace" || header.schema_version != WORKSPACE_SCHEMA_VERSION {
        anyhow::bail!("packed recovery volume marker is invalid");
    }
    let layout = ChunkLayout {
        chunk_size: config.chunk_size,
        block_size: config.block_size,
    };
    let writeback_root = workspace::cache_scope::writeback_root(
        &config.cache.cache_root,
        header.volume_id,
        arguments.workspace,
        arguments.head_epoch,
    )?;
    // Verification of the existing directory and exact original marker is in
    // the private driver, before takeover. This command never initializes it.
    let vfs_config =
        crate::vfs::config::VFSConfig::new_with_cache_config(layout, config.cache.clone())
            .workspace_writeback_root(writeback_root)
            .workspace_writer_epoch(arguments.recovery_generation);
    let observer = budget.read_observer(client.read_observer())?;
    let client = client.with_read_observer(
        observer,
        crate::cadapter::read_observer::Engine::Packed,
        crate::cadapter::read_observer::Phase::Startup,
        crate::cadapter::read_observer::Origin::Demand,
    );
    let mut block_cache = config.cache.clone();
    block_cache.cache_root = block_cache
        .cache_root
        .join("workspace-blocks")
        .join(arguments.workspace.to_string());
    let upper = Arc::new(create_object_store(upper_client, layout, &block_cache, true).await?);
    let original = PackedReleasedMountReference {
        guard: workspace::catalog::HeadGuard {
            workspace_id: arguments.workspace,
            expected_head_layer_id: arguments.head_layer,
            expected_head_epoch: arguments.head_epoch,
            lease_id: arguments.original_lease,
            holder_generation: arguments.original_generation,
        },
        mount_uid: arguments.mount_uid,
        pod_uid: arguments.original_pod_uid,
    };
    let result = catalog
        .recover_packed_mounted_session(
            PackedMountedRecoveryRequest {
                original,
                lease_id: arguments.recovery_lease,
                recovery_pod_uid: arguments.recovery_pod_uid,
                ttl_ns: arguments
                    .ttl_seconds
                    .checked_mul(1_000_000_000)
                    .ok_or_else(|| anyhow::anyhow!("packed recovery TTL overflow"))?,
            },
            client,
            upper,
            vfs_config,
        )
        .await?;
    let report = |reference: &PackedReleasedMountReference| {
        serde_json::json!({
            "workspace_id": reference.guard.workspace_id,
            "head_layer_id": reference.guard.expected_head_layer_id,
            "head_epoch": reference.guard.expected_head_epoch,
            "lease_id": reference.guard.lease_id,
            "holder_generation": reference.guard.holder_generation,
            "mount_uid": reference.mount_uid, "pod_uid": reference.pod_uid,
        })
    };
    println!(
        "{}",
        serde_json::json!({
            "format": "packed", "recovery_completed": true,
            "original": report(&result.original), "released": report(&result.released),
        })
    );
    Ok(())
}
