//! Read-only JSON-lines helper for isolated entity integration/performance tests.
//! Keep the client alive so drain observations do not repeatedly initialize PD.
use std::{collections::BTreeMap, sync::Arc};

use anyhow::{Context, Result, bail};
use brewfs::workspace_overlay::entity::{
    backend::EntityBackend, codec, keys::EntityKeys, redis::RedisEntityBackend,
    tikv::TiKvEntityBackend,
};
use clap::{Parser, ValueEnum};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use uuid::Uuid;

#[derive(Clone, ValueEnum)]
enum Backend {
    Redis,
    Tikv,
}

#[derive(Parser)]
struct Args {
    #[arg(long, value_enum)]
    backend: Backend,
    /// Redis URL or comma-separated PD endpoints of an owned test service.
    #[arg(long)]
    endpoint: String,
    #[arg(long)]
    volume: Uuid,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    op: String,
    namespace: Uuid,
}

async fn query(backend: &dyn EntityBackend, keys: EntityKeys, op: &str) -> Result<Value> {
    match op {
        "status" => {
            let rows = backend
                .read(&[keys.descriptor(), keys.remote_pending()])
                .await?
                .rows;
            let descriptor: Value =
                codec::decode(rows[0].as_deref().context("missing namespace descriptor")?)?;
            let debt = rows[1]
                .as_deref()
                .map(codec::decode::<Value>)
                .transpose()?
                .unwrap_or(json!({"objects":0,"bytes":0}));
            Ok(json!({"descriptor":descriptor,"debt":debt}))
        }
        "snapshot" | "objects" => {
            // A multi-page snapshot is intended only for quiescent test cells.
            let prefix = if op == "objects" {
                [keys.volume_prefix(), b"object/"].concat()
            } else {
                keys.prefix().to_vec()
            };
            let upload = [keys.prefix(), b"upload/"].concat();
            let mut after: Option<Vec<u8>> = None;
            let mut result = BTreeMap::new();
            loop {
                let rows = backend
                    .scan_prefix_page(&prefix, after.as_deref(), 128)
                    .await?;
                if rows.is_empty() {
                    break;
                }
                after = rows.last().map(|row| row.0.clone());
                for (key, value) in rows {
                    if op == "snapshot"
                        && (key.starts_with(&upload) || key == keys.remote_pending())
                    {
                        continue;
                    }
                    if result.len() >= 100_000 {
                        bail!("test snapshot exceeds 100000 rows");
                    }
                    result.insert(String::from_utf8(key)?, hex::encode(value));
                }
            }
            Ok(serde_json::to_value(result)?)
        }
        _ => bail!("unknown read-only probe operation"),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let backend: Arc<dyn EntityBackend> = match args.backend {
        Backend::Redis => Arc::new(RedisEntityBackend::connect(&args.endpoint, args.volume).await?),
        Backend::Tikv => Arc::new(
            TiKvEntityBackend::connect(
                args.endpoint.split(',').map(str::to_owned).collect(),
                args.volume,
                None,
            )
            .await?,
        ),
    };
    let result = async {
        let mut input = BufReader::new(tokio::io::stdin()).lines();
        let mut output = tokio::io::stdout();
        output.write_all(b"{\"ready\":true}\n").await?;
        output.flush().await?;
        while let Some(line) = input.next_line().await? {
            let request: Request = serde_json::from_str(&line)?;
            let value = query(
                backend.as_ref(),
                EntityKeys::new(args.volume, request.namespace),
                &request.op,
            )
            .await?;
            output.write_all(&serde_json::to_vec(&value)?).await?;
            output.write_all(b"\n").await?;
            output.flush().await?;
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;
    let shutdown = backend.shutdown().await;
    result?;
    shutdown?;
    Ok(())
}
