//! Windows named-pipe transport helpers for the control plane.
//!
//! Unix builds use Unix domain sockets with an EOF-delimited framing (client
//! writes a JSON payload, shuts down the write half, reads the response to
//! EOF). Named pipes have no half-close, so the Windows transport uses a
//! length-prefixed framing (u32 LE byte count + JSON payload) in both
//! directions. The wire protocol (`ControlRequest`/`ControlResponse`) is
//! identical on both platforms.

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::control::protocol::ControlResponse;

/// Read one length-prefixed frame into `buf`.
pub(crate) async fn read_frame(
    stream: &mut (impl AsyncRead + Unpin),
    buf: &mut Vec<u8>,
) -> anyhow::Result<()> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;
    buf.clear();
    buf.resize(len, 0);
    stream.read_exact(buf).await?;
    Ok(())
}

/// Write a length-prefixed frame for a control response.
pub(crate) async fn write_frame(
    stream: &mut (impl AsyncWrite + Unpin),
    response: &ControlResponse,
) -> anyhow::Result<()> {
    let payload = serde_json::to_vec(response)?;
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&payload);
    stream.write_all(&frame).await?;
    Ok(())
}
