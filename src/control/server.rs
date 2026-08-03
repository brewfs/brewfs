use crate::control::protocol::{ControlRequest, ControlResponse};
use anyhow::Context;
use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(unix)]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::task::JoinHandle;

#[async_trait]
pub trait ControlHandler: Send + Sync + 'static {
    async fn handle(&self, request: ControlRequest) -> ControlResponse;
}

pub struct ControlServer {
    #[cfg(unix)]
    socket_path: PathBuf,
    #[cfg(unix)]
    task: JoinHandle<()>,
    #[cfg(windows)]
    task: JoinHandle<()>,
}

impl ControlServer {
    #[cfg(unix)]
    pub async fn bind<H>(socket_path: PathBuf, handler: H) -> anyhow::Result<Self>
    where
        H: ControlHandler,
    {
        if let Some(parent) = socket_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create {}", parent.display()))?;
        }

        if socket_path.exists() {
            std::fs::remove_file(&socket_path)?;
        }

        let listener = UnixListener::bind(&socket_path)
            .with_context(|| format!("bind {}", socket_path.display()))?;
        let handler = Arc::new(handler);

        let task = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };

                let handler = Arc::clone(&handler);
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let response = match stream.read_to_end(&mut buf).await {
                        Ok(_) => match serde_json::from_slice::<ControlRequest>(&buf) {
                            Ok(request) => handler.handle(request).await,
                            Err(err) => ControlResponse::Error {
                                code: "invalid_request".to_string(),
                                message: err.to_string(),
                            },
                        },
                        Err(err) => ControlResponse::Error {
                            code: "read_failed".to_string(),
                            message: err.to_string(),
                        },
                    };

                    if let Ok(payload) = serde_json::to_vec(&response) {
                        let _ = stream.write_all(&payload).await;
                    }
                    let _ = stream.shutdown().await;
                });
            }
        });

        Ok(Self { socket_path, task })
    }

    /// Windows transport: bind a named pipe (`\\.\pipe\brewfs-<pid>`) and
    /// serve length-prefixed JSON frames (see `crate::control::pipe`).
    ///
    /// A fresh pipe instance is created for every connection. Reusing a single
    /// instance via `disconnect` + `connect` has a tokio/Win32 race where the
    /// disconnected handle becomes immediately writable and `connect` returns
    /// before a real client is attached (ERROR_PIPE_NOT_CONNECTED on read).
    /// Create-per-connection avoids that and keeps the pipe always available:
    /// clients retry briefly on ERROR_FILE_NOT_FOUND / ERROR_PIPE_BUSY while
    /// the next instance is being created.
    #[cfg(windows)]
    pub async fn bind<H>(socket_path: PathBuf, handler: H) -> anyhow::Result<Self>
    where
        H: ControlHandler,
    {
        use std::time::Duration;

        use tokio::net::windows::named_pipe::ServerOptions;

        let handler = Arc::new(handler);

        let task = tokio::spawn(async move {
            loop {
                let mut server = match ServerOptions::new()
                    .first_pipe_instance(true)
                    .max_instances(1)
                    .create(&socket_path)
                {
                    Ok(server) => server,
                    Err(err) => {
                        // Stale/busy pipe (e.g. previous instance not yet
                        // released after a crash): retry shortly.
                        tracing::warn!(
                            error = ?err,
                            "control plane named pipe create failed, retrying"
                        );
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };

                if let Err(err) = server.connect().await {
                    tracing::warn!(error = ?err, "control plane named pipe connect failed");
                    break;
                }

                let mut buf = Vec::new();
                let response = match crate::control::pipe::read_frame(&mut server, &mut buf).await {
                    Ok(()) => match serde_json::from_slice::<ControlRequest>(&buf) {
                        Ok(request) => handler.handle(request).await,
                        Err(err) => ControlResponse::Error {
                            code: "invalid_request".to_string(),
                            message: err.to_string(),
                        },
                    },
                    Err(err) => ControlResponse::Error {
                        code: "read_failed".to_string(),
                        message: err.to_string(),
                    },
                };

                if let Err(err) = crate::control::pipe::write_frame(&mut server, &response).await {
                    tracing::warn!(error = ?err, "control plane named pipe write failed");
                }
                // Dropping `server` closes this pipe instance.
            }
        });

        Ok(Self { task })
    }
}

#[cfg(unix)]
impl Drop for ControlServer {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

#[cfg(windows)]
impl Drop for ControlServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
