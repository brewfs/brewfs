use crate::control::protocol::{
    CONTROL_IO_TIMEOUT, CONTROL_MAX_REQUEST_BYTES, CONTROL_MAX_RESPONSE_BYTES, ControlRequest,
    ControlResponse,
};
use anyhow::Context;
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::time::timeout;

pub async fn send_request(
    socket_path: impl AsRef<Path>,
    request: &ControlRequest,
) -> anyhow::Result<ControlResponse> {
    let socket_path = socket_path.as_ref();
    let mut stream = UnixStream::connect(socket_path)
        .await
        .with_context(|| format!("connect {}", socket_path.display()))?;

    let payload = serde_json::to_vec(request)?;
    if payload.len() > CONTROL_MAX_REQUEST_BYTES {
        anyhow::bail!(
            "control request exceeds {} bytes",
            CONTROL_MAX_REQUEST_BYTES
        );
    }
    timeout(CONTROL_IO_TIMEOUT, stream.write_all(&payload)).await??;
    timeout(CONTROL_IO_TIMEOUT, stream.shutdown()).await??;

    let mut buf = Vec::new();
    timeout(
        CONTROL_IO_TIMEOUT,
        (&mut stream)
            .take((CONTROL_MAX_RESPONSE_BYTES + 1) as u64)
            .read_to_end(&mut buf),
    )
    .await??;
    if buf.len() > CONTROL_MAX_RESPONSE_BYTES {
        anyhow::bail!(
            "control response exceeds {} bytes",
            CONTROL_MAX_RESPONSE_BYTES
        );
    }

    serde_json::from_slice(&buf).context("decode control response")
}
