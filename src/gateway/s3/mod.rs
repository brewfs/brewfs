//! S3-compatible protocol gateway.
//!
//! Serves one BrewFS volume over the S3 API on top of the `s3s` service
//! framework. See `doc/protocols/s3-gateway.md`.

pub mod list;
pub mod multipart;
pub mod path;
mod store;

pub use path::BucketMode;
pub use store::{BrewFsS3, S3Options};

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use tower::Service;

use crate::chunk::store::BlockStore;
use crate::meta::MetaStore;
use crate::meta::client::MetaClient;
use crate::meta::config::{CompactConfig, MetaClientConfig};
use crate::meta::layer::MetaLayer;
use crate::vfs::cache::config::CacheConfig as VfsCacheConfig;
use crate::vfs::fs::VFS;

/// Options for the S3 gateway server.
#[derive(Debug, Clone)]
pub struct S3GatewayOptions {
    /// Listen address of the S3 endpoint.
    pub listen_addr: SocketAddr,
    /// Static access key for SigV4 authentication.
    pub access_key: String,
    /// Static secret key for SigV4 authentication.
    pub secret_key: String,
    /// Bucket exposure model.
    pub bucket_mode: BucketMode,
    /// Hide explicit directory objects in listings.
    pub hide_dir_objects: bool,
}

/// Runs the S3 gateway until the process is stopped.
///
/// `store` is the block store of the volume (same construction as a FUSE
/// mount), `meta` the metadata store. The gateway builds its own `VFS` stack
/// on top of them and registers a control-plane instance so `brewfs info`
/// can see it.
pub async fn serve<S>(
    store: S,
    meta: Arc<dyn MetaStore>,
    layout: crate::chunk::ChunkLayout,
    compact: CompactConfig,
    cache: VfsCacheConfig,
    opts: S3GatewayOptions,
) -> anyhow::Result<()>
where
    S: BlockStore + Send + Sync + 'static,
{
    let mut config = MetaClientConfig::default();
    config.options.mount_point = Some("brewfs-gateway-s3".to_string());
    let meta_client = MetaClient::with_options(
        meta,
        config.capacity.clone(),
        config.effective_ttl(),
        config.options,
    );
    meta_client
        .initialize()
        .await
        .map_err(anyhow::Error::from)?;
    meta_client
        .start_control_plane()
        .await
        .map_err(anyhow::Error::from)?;

    let vfs = VFS::with_meta_layer_with_cache_config(
        layout,
        Arc::new(store),
        meta_client,
        compact,
        cache,
    )
    .map_err(|e| anyhow::anyhow!("create VFS: {e}"))?;

    let s3_options = S3Options {
        bucket_mode: opts.bucket_mode.clone(),
        hide_dir_objects: opts.hide_dir_objects,
    };

    let s3 = BrewFsS3::new(vfs, s3_options);
    s3.vfs()
        .mkdir_p(&multipart::uploads_dir())
        .await
        .map_err(|e| anyhow::anyhow!("create uploads dir: {e}"))?;
    s3.vfs()
        .mkdir_p(&multipart::tmp_dir())
        .await
        .map_err(|e| anyhow::anyhow!("create tmp dir: {e}"))?;

    // Periodic cleanup of stale multipart state (best effort).
    {
        let cleanup_s3 = s3.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3600));
            loop {
                interval.tick().await;
                if let Err(e) = cleanup_stale_uploads(&cleanup_s3).await {
                    tracing::warn!(error = %e, "s3 gateway cleanup task failed");
                }
            }
        });
    }

    let mut builder = S3ServiceBuilder::new(s3);
    builder.set_auth(SimpleAuth::from_single(
        opts.access_key.clone(),
        opts.secret_key.clone(),
    ));
    let service = builder.build();

    let app = axum::Router::new().fallback_service(S3Fallback(service));
    let listener = tokio::net::TcpListener::bind(opts.listen_addr).await?;
    tracing::info!(
        listen = %opts.listen_addr,
        bucket_mode = %opts.bucket_mode,
        "brewfs s3 gateway listening"
    );
    axum::serve(listener, app).await?;
    Ok(())
}

/// Removes multipart uploads older than 24h and staging files older than 24h.
async fn cleanup_stale_uploads<S>(s3: &BrewFsS3<S>) -> anyhow::Result<()>
where
    S: BlockStore + Send + Sync + 'static,
{
    let vfs = s3.vfs();
    let cutoff = chrono::Utc::now().timestamp() - 24 * 3600;
    let cutoff_ns = cutoff * 1_000_000_000; // VFS attrs carry mtime in nanos.
    let uploads = multipart::uploads_dir();
    for hh in read_children(vfs, &uploads).await {
        let hh_dir = format!("{uploads}/{}", hh.name);
        for upload in read_children(vfs, &hh_dir).await {
            let upload_id = upload.name;
            let dir = format!("{hh_dir}/{upload_id}");
            let Ok(meta) = s3.read_upload_meta(&upload_id).await else {
                if let Ok(attr) = vfs.stat(&dir).await
                    && attr.mtime < cutoff_ns
                    && remove_dir_all_rec(vfs, &dir).await.is_ok()
                {
                    tracing::info!(dir = %dir, "cleaned unreadable stale multipart upload");
                }
                continue;
            };
            if meta.initiated >= cutoff {
                continue;
            }
            let lock = s3.lock_for(&meta.bucket, &meta.key);
            let _guard = lock.lock().await;
            let Ok(meta) = s3.read_upload_meta(&upload_id).await else {
                continue;
            };
            if meta.initiated < cutoff {
                let dir = multipart::upload_dir(&upload_id);
                if remove_dir_all_rec(vfs, &dir).await.is_ok() {
                    tracing::info!(dir = %dir, "cleaned stale multipart upload");
                }
            }
        }
    }
    let tmp = multipart::tmp_dir();
    for entry in read_children(vfs, &tmp).await {
        let path = format!("{tmp}/{}", entry.name);
        if s3.staging_path_is_active(&path) {
            continue;
        }
        if let Ok(attr) = vfs.stat(&path).await
            && attr.mtime < cutoff_ns
        {
            let _ = vfs.unlink(&path).await;
        }
    }
    Ok(())
}

async fn read_children<S>(
    vfs: &VFS<S, MetaClient<dyn MetaStore>>,
    dir: &str,
) -> Vec<crate::meta::store::DirEntry>
where
    S: BlockStore + Send + Sync + 'static,
{
    let Ok(attr) = vfs.stat(dir).await else {
        return Vec::new();
    };
    if attr.kind != crate::meta::store::FileType::Dir {
        return Vec::new();
    }
    let Ok(fh) = vfs.opendir(attr.ino).await else {
        return Vec::new();
    };
    let mut entries = Vec::new();
    let mut offset = 0;
    while let Some(page) = vfs.readdir(fh, offset) {
        if page.is_empty() {
            break;
        }
        offset += page.len() as u64;
        entries.extend(page);
    }
    let _ = vfs.closedir(fh);
    entries
}

/// Recursively removes a directory tree over the VFS (VFS has no native
/// `remove_dir_all`).
pub(crate) async fn remove_dir_all_rec<S>(
    vfs: &VFS<S, MetaClient<dyn MetaStore>>,
    path: &str,
) -> Result<(), crate::vfs::error::VfsError>
where
    S: BlockStore + Send + Sync + 'static,
{
    let attr = vfs.stat(path).await?;
    if attr.kind == crate::meta::store::FileType::Dir {
        if let Ok(fh) = vfs.opendir(attr.ino).await {
            let mut entries = Vec::new();
            let mut offset = 0;
            while let Some(page) = vfs.readdir(fh, offset) {
                if page.is_empty() {
                    break;
                }
                offset += page.len() as u64;
                entries.extend(page);
            }
            let _ = vfs.closedir(fh);
            for entry in entries {
                let child = format!("{path}/{}", entry.name);
                Box::pin(remove_dir_all_rec(vfs, &child)).await?;
            }
        }
        vfs.rmdir(path).await
    } else {
        vfs.unlink(path).await
    }
}

/// axum adapter: `S3Service` fails with `HttpError`, but axum routers require
/// infallible services. Errors are converted into plain 500 responses here.
#[derive(Clone)]
struct S3Fallback(s3s::service::S3Service);

impl Service<axum::http::Request<axum::body::Body>> for S3Fallback {
    type Response = axum::http::Response<axum::body::Body>;
    type Error = std::convert::Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        // `S3Service` is always ready; its own poll_ready reports an
        // `HttpError` which cannot cross the infallible axum boundary.
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: axum::http::Request<axum::body::Body>) -> Self::Future {
        let mut service = self.0.clone();
        Box::pin(async move {
            match tower::Service::call(&mut service, req).await {
                Ok(resp) => {
                    let (parts, body) = resp.into_parts();
                    Ok(axum::http::Response::from_parts(
                        parts,
                        axum::body::Body::new(body),
                    ))
                }
                Err(e) => {
                    let err: Box<dyn std::error::Error + Send + Sync> = e.into();
                    Ok(axum::http::Response::builder()
                        .status(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
                        .body(axum::body::Body::from(format!("s3 gateway error: {err}")))
                        .unwrap())
                }
            }
        })
    }
}
