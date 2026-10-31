use std::path::Path;
use std::sync::Arc;

use clap::Parser;

use crate::cadapter::s3::S3Backend;
use crate::config::{Cli, Command, DataBackendKind, MountConfig};
use crate::workspace_overlay::catalog::{CreateWorkspace, WorkspaceStore};
use crate::workspace_overlay::ids::WorkspaceId;
use crate::workspace_overlay::model::{BaseRevision, WorkspaceRecord};

pub fn config(path: &Path) -> anyhow::Result<MountConfig> {
    let cli = Cli::try_parse_from([
        std::ffi::OsStr::new("brewfs"),
        std::ffi::OsStr::new("mount"),
        std::ffi::OsStr::new("--config"),
        path.as_os_str(),
        std::ffi::OsStr::new("/nonexistent-workspace-bucket"),
    ])?;
    let Command::Mount(args) = cli.cmd else {
        anyhow::bail!("workspace bucket config did not parse as a mount configuration");
    };
    MountConfig::from_sources(*args)
}

pub async fn create(
    config: &MountConfig,
    workspace_id: WorkspaceId,
) -> anyhow::Result<Option<String>> {
    if !matches!(config.data_backend, DataBackendKind::S3) {
        return Ok(None);
    }
    let base = config
        .s3_bucket
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("S3 bucket is required for workspace upper buckets"))?;
    let name = render(&config.workspace_bucket_template, base, workspace_id)?;
    let backend = S3Backend::with_config(crate::cadapter::s3::S3Config {
        bucket: name.clone(),
        region: config.s3_region.clone(),
        endpoint: config.s3_endpoint.clone(),
        part_size: config.s3_part_size,
        max_concurrency: config.s3_max_concurrency,
        force_path_style: config.s3_force_path_style,
        disable_payload_checksum: config.s3_disable_payload_checksum,
        rustfs_ec_block_size_hint: config.s3_rustfs_ec_block_size_hint,
        ..Default::default()
    })
    .await?;
    backend.create_bucket_idempotent().await?;
    tracing::info!(workspace = %workspace_id, bucket = %name, "workspace upper bucket ready");
    Ok(Some(name))
}

pub async fn fork<W: WorkspaceStore + 'static>(
    store: Arc<W>,
    revision: BaseRevision,
    count: usize,
    owner: Option<String>,
    data_config: &Path,
) -> anyhow::Result<Vec<WorkspaceRecord>> {
    let config = config(data_config)?;
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let workspace_id = WorkspaceId::new();
        let upper_bucket = create(&config, workspace_id).await?;
        rows.push(
            store
                .create_workspace(CreateWorkspace {
                    workspace_id,
                    head_layer_id: crate::workspace_overlay::ids::LayerId::new(),
                    base_revision: revision.clone(),
                    owner_id: owner.clone(),
                    upper_bucket,
                })
                .await?,
        );
    }
    Ok(rows)
}

pub async fn drop_bucket<W: WorkspaceStore + 'static>(
    store: Arc<W>,
    workspace: WorkspaceId,
    data_config: &Path,
    force: bool,
) -> anyhow::Result<()> {
    if !force {
        anyhow::bail!("dropping an upper bucket requires --force");
    }
    let record = store.load_workspace(workspace).await?;
    if record.state != crate::workspace_overlay::model::WorkspaceState::Deleting {
        anyhow::bail!("discard workspace {workspace} before deleting its upper bucket");
    }
    let Some(bucket) = record.upper_bucket else {
        anyhow::bail!("workspace {workspace} uses the legacy shared bucket; refusing to delete it");
    };
    let config = config(data_config)?;
    if !matches!(config.data_backend, DataBackendKind::S3) {
        anyhow::bail!("drop-upper-bucket currently requires data.backend=s3");
    }
    if config.s3_bucket.as_deref() == Some(bucket.as_str()) {
        anyhow::bail!("refusing to delete the configured shared lower bucket {bucket:?}");
    }
    let backend = S3Backend::with_config(crate::cadapter::s3::S3Config {
        bucket: bucket.clone(),
        region: config.s3_region,
        endpoint: config.s3_endpoint,
        force_path_style: config.s3_force_path_style,
        ..Default::default()
    })
    .await?;
    backend.empty_and_delete_bucket().await.map_err(|error| {
        anyhow::anyhow!(
            "failed to delete upper bucket {bucket:?}; versioned or protected objects may require explicit S3 administration: {error}"
        )
    })?;
    println!("deleted upper bucket {bucket} for workspace {workspace}");
    Ok(())
}

fn render(template: &str, base: &str, workspace_id: WorkspaceId) -> anyhow::Result<String> {
    if !template.contains("{workspace_id}") {
        anyhow::bail!(
            "workspace bucket template must contain {{workspace_id}} to isolate workspaces"
        );
    }
    let value = template
        .replace("{bucket}", base)
        .replace("{workspace_id}", &workspace_id.to_string());
    if value.len() < 3
        || value.len() > 63
        || !value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
        || value.split('.').any(|part| {
            part.is_empty()
                || !part.as_bytes()[0].is_ascii_alphanumeric()
                || !part.as_bytes()[part.len() - 1].is_ascii_alphanumeric()
        })
        || value.parse::<std::net::Ipv4Addr>().is_ok()
    {
        anyhow::bail!("workspace bucket template rendered invalid S3 bucket name {value:?}");
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::render;
    use crate::workspace_overlay::ids::WorkspaceId;

    #[test]
    fn renders_default_workspace_bucket_name() {
        let workspace = WorkspaceId::from_uuid(uuid::Uuid::from_u128(7));
        assert_eq!(
            render("{bucket}-{workspace_id}", "brewfs-data", workspace).unwrap(),
            "brewfs-data-00000000-0000-0000-0000-000000000007"
        );
    }

    #[test]
    fn renders_custom_template_and_rejects_invalid_names() {
        let workspace = WorkspaceId::from_uuid(uuid::Uuid::from_u128(8));
        assert_eq!(
            render("upper.{workspace_id}", "ignored", workspace).unwrap(),
            "upper.00000000-0000-0000-0000-000000000008"
        );
        assert!(render("Upper-{workspace_id}", "brewfs-data", workspace).is_err());
        assert!(render("{bucket}", "", workspace).is_err());
    }
}
