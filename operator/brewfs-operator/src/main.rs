mod crd;
mod reconciler;
#[cfg(feature = "workspace-operator")]
mod workspace;

use std::sync::Arc;

use anyhow::Context as _;
use clap::{Parser, Subcommand};
use futures::StreamExt;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{ConfigMap, PersistentVolumeClaim, Secret, Service};
use kube::api::Api;
use kube::runtime::reflector::ObjectRef;
use kube::runtime::watcher;
use kube::runtime::Controller;
use kube::Client;
use kube::CustomResourceExt;
use kube::ResourceExt;
use tracing::{error, info};

use crate::crd::{BrewFSCluster, BrewFSMount};
use crate::reconciler::OperatorContext;
#[cfg(feature = "workspace-operator")]
use crate::workspace::crd::{BrewFSWorkspace, BrewFSWorkspaceMount, BrewFSWorkspaceSnapshot};

#[derive(Parser, Debug)]
#[command(
    name = "brewfs-operator",
    version,
    about = "Independent BrewFS Kubernetes operator"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the controller loop.
    Run,
    /// Print the CRD YAML to stdout.
    Crdgen,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    match Cli::parse().command {
        Command::Run => run_controller().await,
        Command::Crdgen => print_crd(),
    }
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::new(
        std::env::var("RUST_LOG")
            .unwrap_or_else(|_| "brewfs_operator=info,brewfs_operator=debug".to_string()),
    );

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}

fn print_crd() -> anyhow::Result<()> {
    let cluster_crd = serde_yaml::to_string(&BrewFSCluster::crd())
        .context("serialize BrewFSCluster CRD to YAML")?;
    let mount_crd =
        serde_yaml::to_string(&BrewFSMount::crd()).context("serialize BrewFSMount CRD to YAML")?;
    print!("{cluster_crd}---\n{mount_crd}");
    #[cfg(feature = "workspace-operator")]
    {
        let workspace_crd = serde_yaml::to_string(&BrewFSWorkspace::crd())
            .context("serialize BrewFSWorkspace CRD to YAML")?;
        let workspace_mount_crd = serde_yaml::to_string(&BrewFSWorkspaceMount::crd())
            .context("serialize BrewFSWorkspaceMount CRD to YAML")?;
        let workspace_snapshot_crd = serde_yaml::to_string(&BrewFSWorkspaceSnapshot::crd())
            .context("serialize BrewFSWorkspaceSnapshot CRD to YAML")?;
        print!("---\n{workspace_crd}---\n{workspace_mount_crd}---\n{workspace_snapshot_crd}");
    }
    Ok(())
}

async fn run_controller() -> anyhow::Result<()> {
    let client = Client::try_default()
        .await
        .context("build kubernetes client from current environment")?;
    let context = Arc::new(OperatorContext {
        client: client.clone(),
    });
    let cluster_api: Api<BrewFSCluster> = Api::all(client.clone());
    let mount_api: Api<BrewFSMount> = Api::all(client.clone());

    info!("starting BrewFS controllers");

    let cluster_controller = Controller::new(cluster_api, watcher::Config::default())
        .owns::<Deployment>(Api::all(client.clone()), watcher::Config::default())
        .owns::<PersistentVolumeClaim>(Api::all(client.clone()), watcher::Config::default())
        .owns::<Job>(Api::all(client.clone()), watcher::Config::default())
        .owns::<Service>(Api::all(client.clone()), watcher::Config::default())
        .owns::<Secret>(Api::all(client.clone()), watcher::Config::default())
        .owns::<ConfigMap>(Api::all(client.clone()), watcher::Config::default())
        .run(
            reconciler::reconcile_cluster,
            reconciler::error_policy_cluster,
            context.clone(),
        )
        .for_each(|result| async move {
            match result {
                Ok((object_ref, action)) => {
                    info!(name = %object_ref.name, ?action, "reconciled BrewFSCluster");
                }
                Err(error) => {
                    error!(?error, "BrewFSCluster reconcile loop error");
                }
            }
        });

    let mount_controller_builder = Controller::new(mount_api, watcher::Config::default());
    let mount_store = mount_controller_builder.store();
    let mount_controller = mount_controller_builder
        .watches(
            Api::<BrewFSCluster>::all(client.clone()),
            watcher::Config::default(),
            move |cluster: BrewFSCluster| {
                mount_store
                    .state()
                    .into_iter()
                    .filter(|mount| {
                        mount.namespace() == cluster.namespace()
                            && mount.spec.cluster_ref.name == cluster.name_any()
                    })
                    .map(|mount| ObjectRef::from_obj(mount.as_ref()))
                    .collect::<Vec<_>>()
            },
        )
        .run(
            reconciler::reconcile_mount,
            reconciler::error_policy_mount,
            context.clone(),
        )
        .for_each(|result| async move {
            match result {
                Ok((object_ref, action)) => {
                    info!(name = %object_ref.name, ?action, "reconciled BrewFSMount");
                }
                Err(error) => {
                    error!(?error, "BrewFSMount reconcile loop error");
                }
            }
        });

    #[cfg(feature = "workspace-operator")]
    {
        let workspace_controller = Controller::new(
            Api::<BrewFSWorkspace>::all(client.clone()),
            watcher::Config::default(),
        )
        .run(
            workspace::controller::reconcile_workspace,
            workspace::controller::error_policy_workspace,
            context.clone(),
        )
        .for_each(|result| async move {
            match result {
                Ok((object_ref, action)) => {
                    info!(name = %object_ref.name, ?action, "reconciled BrewFSWorkspace");
                }
                Err(error) => error!(?error, "BrewFSWorkspace reconcile loop error"),
            }
        });

        let workspace_mount_controller = Controller::new(
            Api::<BrewFSWorkspaceMount>::all(client.clone()),
            watcher::Config::default(),
        )
        .run(
            workspace::controller::reconcile_workspace_mount,
            workspace::controller::error_policy_mount,
            context.clone(),
        )
        .for_each(|result| async move {
            match result {
                Ok((object_ref, action)) => {
                    info!(name = %object_ref.name, ?action, "reconciled BrewFSWorkspaceMount");
                }
                Err(error) => error!(?error, "BrewFSWorkspaceMount reconcile loop error"),
            }
        });

        let workspace_snapshot_controller = Controller::new(
            Api::<BrewFSWorkspaceSnapshot>::all(client),
            watcher::Config::default(),
        )
        .run(
            workspace::controller::reconcile_workspace_snapshot,
            workspace::controller::error_policy_snapshot,
            context,
        )
        .for_each(|result| async move {
            match result {
                Ok((object_ref, action)) => {
                    info!(name = %object_ref.name, ?action, "reconciled BrewFSWorkspaceSnapshot");
                }
                Err(error) => error!(?error, "BrewFSWorkspaceSnapshot reconcile loop error"),
            }
        });

        tokio::join!(
            cluster_controller,
            mount_controller,
            workspace_controller,
            workspace_mount_controller,
            workspace_snapshot_controller
        );
    }

    #[cfg(not(feature = "workspace-operator"))]
    tokio::join!(cluster_controller, mount_controller);

    Ok(())
}
