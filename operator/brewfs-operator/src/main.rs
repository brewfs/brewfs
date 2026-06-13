mod crd;
mod reconciler;

use std::sync::Arc;

use anyhow::Context as _;
use clap::{Parser, Subcommand};
use futures::StreamExt;
use kube::api::Api;
use kube::runtime::watcher;
use kube::runtime::Controller;
use kube::Client;
use kube::CustomResourceExt;
use tracing::{error, info};

use crate::crd::{BrewFSCluster, BrewFSMount};
use crate::reconciler::OperatorContext;

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
    println!("{cluster_crd}---\n{mount_crd}");
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
    let mount_api: Api<BrewFSMount> = Api::all(client);

    info!("starting BrewFS controllers");

    let cluster_controller = Controller::new(cluster_api, watcher::Config::default())
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

    let mount_controller = Controller::new(mount_api, watcher::Config::default())
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

    tokio::join!(cluster_controller, mount_controller);

    Ok(())
}
