use crate::control::protocol::{ControlRequest, ControlResponse};
#[cfg(unix)]
use anyhow::Context;
use std::path::Path;
#[cfg(unix)]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(unix)]
use tokio::net::UnixStream;

#[cfg(unix)]
pub async fn send_request(
    socket_path: impl AsRef<Path>,
    request: &ControlRequest,
) -> anyhow::Result<ControlResponse> {
    let socket_path = socket_path.as_ref();
    let mut stream = UnixStream::connect(socket_path)
        .await
        .with_context(|| format!("connect {}", socket_path.display()))?;

    let payload = serde_json::to_vec(request)?;
    stream.write_all(&payload).await?;
    stream.shutdown().await?;

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await?;

    serde_json::from_slice(&buf).context("decode control response")
}

/// Windows transport: connect to the named pipe stored in the instance record
/// and exchange a length-prefixed JSON frame (see `crate::control::pipe`).
///
/// The mount process creates the pipe during startup, so a short retry loop
/// absorbs the race between `brewfs mount` returning and an immediate
/// `brewfs gc`/`brewfs info` invocation.
#[cfg(windows)]
pub async fn send_request(
    socket_path: impl AsRef<Path>,
    request: &ControlRequest,
) -> anyhow::Result<ControlResponse> {
    use std::time::Duration;

    use tokio::io::AsyncWriteExt;
    use tokio::net::windows::named_pipe::ClientOptions;

    let pipe_name = socket_path.as_ref();

    let mut client = None;
    for attempt in 0..20usize {
        match ClientOptions::new().open(pipe_name) {
            Ok(c) => {
                client = Some(c);
                break;
            }
            Err(_e) if attempt + 1 < 20 => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "connect control plane pipe {}: {e}",
                    pipe_name.display()
                ));
            }
        }
    }
    let mut client = client.expect("retry loop always returns or breaks");

    let payload = serde_json::to_vec(request)?;
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&payload);
    client.write_all(&frame).await?;

    let mut response = Vec::new();
    crate::control::pipe::read_frame(&mut client, &mut response).await?;
    serde_json::from_slice(&response).map_err(anyhow::Error::from)
}
