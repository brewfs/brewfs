use brewfs_csi::{
    CsiError, DRIVER_NAME, MountManager, MountSpec, SystemMountOps, VolumeId, lock_state_dir, proto,
};
use clap::Parser;
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::net::{TcpListener, UnixListener};
use tokio_stream::wrappers::{TcpListenerStream, UnixListenerStream};
use tonic::{Request, Response, Status};

use proto::controller_server::{Controller, ControllerServer};
use proto::identity_server::{Identity, IdentityServer};
use proto::node_server::{Node, NodeServer};
use proto::volume_capability::AccessType;

#[derive(Debug, Parser)]
#[command(
    name = "brewfs-csi",
    about = "BrewFS Container Storage Interface plugin"
)]
struct Args {
    /// Unix socket used by the node plugin and local CSI probes.
    #[arg(
        long,
        env = "BREWFS_CSI_SOCKET",
        default_value = "/var/lib/kubelet/plugins/csi.brewfs.io/csi.sock"
    )]
    socket: PathBuf,
    /// Optional TCP endpoint for CSI diagnostics. Dynamic provisioning is
    /// unsupported; kubelet uses the Unix socket above.
    #[arg(long, env = "BREWFS_CSI_LISTEN")]
    listen: Option<std::net::SocketAddr>,
    /// Durable node mount state root, preserved across restarts.
    #[arg(
        long,
        env = "BREWFS_CSI_STATE_DIR",
        default_value = "/var/lib/brewfs-csi/mounts"
    )]
    state_dir: PathBuf,
    /// Node identity returned by NodeGetInfo.
    #[arg(long, env = "BREWFS_CSI_NODE_ID", default_value = "unknown")]
    node_id: String,
}

#[derive(Debug, Clone, Copy, Default)]
struct IdentityService;

#[tonic::async_trait]
impl Identity for IdentityService {
    async fn get_plugin_info(
        &self,
        _request: Request<proto::GetPluginInfoRequest>,
    ) -> Result<Response<proto::GetPluginInfoResponse>, Status> {
        Ok(Response::new(proto::GetPluginInfoResponse {
            name: DRIVER_NAME.to_owned(),
            vendor_version: env!("CARGO_PKG_VERSION").to_owned(),
            manifest: HashMap::new(),
        }))
    }

    async fn get_plugin_capabilities(
        &self,
        _request: Request<proto::GetPluginCapabilitiesRequest>,
    ) -> Result<Response<proto::GetPluginCapabilitiesResponse>, Status> {
        Ok(Response::new(proto::GetPluginCapabilitiesResponse {
            // Workspaces are provisioned by the operator. Without a volume
            // inventory or enforced capacity, we cannot offer a CSI controller.
            capabilities: Vec::new(),
        }))
    }

    async fn probe(
        &self,
        _request: Request<proto::ProbeRequest>,
    ) -> Result<Response<proto::ProbeResponse>, Status> {
        Ok(Response::new(proto::ProbeResponse { ready: Some(true) }))
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct ControllerService;

#[tonic::async_trait]
impl Controller for ControllerService {
    async fn create_volume(
        &self,
        _request: Request<proto::CreateVolumeRequest>,
    ) -> Result<Response<proto::CreateVolumeResponse>, Status> {
        Err(Status::unimplemented(
            "dynamic provisioning is unsupported; use a static PV for an operator-provisioned workspace",
        ))
    }

    async fn delete_volume(
        &self,
        _request: Request<proto::DeleteVolumeRequest>,
    ) -> Result<Response<proto::DeleteVolumeResponse>, Status> {
        Err(Status::unimplemented(
            "workspace lifecycle belongs to the BrewFS operator",
        ))
    }

    async fn controller_publish_volume(
        &self,
        _request: Request<proto::ControllerPublishVolumeRequest>,
    ) -> Result<Response<proto::ControllerPublishVolumeResponse>, Status> {
        Err(Status::unimplemented(
            "attachRequired=false; controller publish is not used",
        ))
    }

    async fn controller_unpublish_volume(
        &self,
        _request: Request<proto::ControllerUnpublishVolumeRequest>,
    ) -> Result<Response<proto::ControllerUnpublishVolumeResponse>, Status> {
        Err(Status::unimplemented(
            "attachRequired=false; controller unpublish is not used",
        ))
    }

    async fn validate_volume_capabilities(
        &self,
        _request: Request<proto::ValidateVolumeCapabilitiesRequest>,
    ) -> Result<Response<proto::ValidateVolumeCapabilitiesResponse>, Status> {
        Err(Status::unimplemented(
            "controller validation is unsupported for operator-provisioned workspaces",
        ))
    }

    async fn list_volumes(
        &self,
        _request: Request<proto::ListVolumesRequest>,
    ) -> Result<Response<proto::ListVolumesResponse>, Status> {
        Err(Status::unimplemented("volume inventory is not supported"))
    }

    async fn get_capacity(
        &self,
        _request: Request<proto::GetCapacityRequest>,
    ) -> Result<Response<proto::GetCapacityResponse>, Status> {
        Err(Status::unimplemented("capacity reporting is not supported"))
    }

    async fn controller_get_capabilities(
        &self,
        _request: Request<proto::ControllerGetCapabilitiesRequest>,
    ) -> Result<Response<proto::ControllerGetCapabilitiesResponse>, Status> {
        Ok(Response::new(proto::ControllerGetCapabilitiesResponse {
            capabilities: Vec::new(),
        }))
    }

    async fn create_snapshot(
        &self,
        _request: Request<proto::CreateSnapshotRequest>,
    ) -> Result<Response<proto::CreateSnapshotResponse>, Status> {
        Err(Status::unimplemented("snapshots are not supported"))
    }

    async fn delete_snapshot(
        &self,
        _request: Request<proto::DeleteSnapshotRequest>,
    ) -> Result<Response<proto::DeleteSnapshotResponse>, Status> {
        Err(Status::unimplemented("snapshots are not supported"))
    }

    async fn list_snapshots(
        &self,
        _request: Request<proto::ListSnapshotsRequest>,
    ) -> Result<Response<proto::ListSnapshotsResponse>, Status> {
        Err(Status::unimplemented("snapshots are not supported"))
    }

    async fn controller_expand_volume(
        &self,
        _request: Request<proto::ControllerExpandVolumeRequest>,
    ) -> Result<Response<proto::ControllerExpandVolumeResponse>, Status> {
        Err(Status::unimplemented("volume expansion is not supported"))
    }

    async fn controller_get_volume(
        &self,
        _request: Request<proto::ControllerGetVolumeRequest>,
    ) -> Result<Response<proto::ControllerGetVolumeResponse>, Status> {
        Err(Status::unimplemented("volume lookup is not supported"))
    }
}

#[derive(Debug, Clone)]
struct NodeService {
    manager: MountManager<SystemMountOps>,
    node_id: String,
}

impl NodeService {
    fn spec(
        &self,
        volume_id: &str,
        context: &HashMap<String, String>,
    ) -> Result<MountSpec, Status> {
        let pairs = context
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        MountSpec::from_context(volume_id, &pairs).map_err(status_for)
    }
}

#[tonic::async_trait]
impl Node for NodeService {
    async fn node_stage_volume(
        &self,
        request: Request<proto::NodeStageVolumeRequest>,
    ) -> Result<Response<proto::NodeStageVolumeResponse>, Status> {
        let request = request.into_inner();
        if request.staging_target_path.is_empty() {
            return Err(Status::invalid_argument("staging_target_path is required"));
        }
        let capability = request
            .volume_capability
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("volume_capability is required"))?;
        validate_capabilities(std::slice::from_ref(capability))?;
        let spec = self.spec(&request.volume_id, &request.volume_context)?;
        self.manager.stage(&spec).await.map_err(status_for)?;
        Ok(Response::new(proto::NodeStageVolumeResponse {}))
    }

    async fn node_unstage_volume(
        &self,
        request: Request<proto::NodeUnstageVolumeRequest>,
    ) -> Result<Response<proto::NodeUnstageVolumeResponse>, Status> {
        let request = request.into_inner();
        if request.staging_target_path.is_empty() {
            return Err(Status::invalid_argument("staging_target_path is required"));
        }
        VolumeId::parse(&request.volume_id).map_err(status_for)?;
        self.manager
            .unstage_by_id(&request.volume_id)
            .await
            .map_err(status_for)?;
        Ok(Response::new(proto::NodeUnstageVolumeResponse {}))
    }

    async fn node_publish_volume(
        &self,
        request: Request<proto::NodePublishVolumeRequest>,
    ) -> Result<Response<proto::NodePublishVolumeResponse>, Status> {
        let request = request.into_inner();
        if request.target_path.is_empty() {
            return Err(Status::invalid_argument("target_path is required"));
        }
        let capability = request
            .volume_capability
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("volume_capability is required"))?;
        validate_capabilities(std::slice::from_ref(capability))?;
        let spec = self.spec(&request.volume_id, &request.volume_context)?;
        self.manager
            .publish(
                &spec,
                PathBuf::from(request.target_path).as_path(),
                request.readonly,
            )
            .await
            .map_err(status_for)?;
        Ok(Response::new(proto::NodePublishVolumeResponse {}))
    }

    async fn node_unpublish_volume(
        &self,
        request: Request<proto::NodeUnpublishVolumeRequest>,
    ) -> Result<Response<proto::NodeUnpublishVolumeResponse>, Status> {
        let request = request.into_inner();
        if request.target_path.is_empty() {
            return Err(Status::invalid_argument("target_path is required"));
        }
        VolumeId::parse(&request.volume_id).map_err(status_for)?;
        self.manager
            .unpublish_by_id(
                &request.volume_id,
                PathBuf::from(request.target_path).as_path(),
            )
            .await
            .map_err(status_for)?;
        Ok(Response::new(proto::NodeUnpublishVolumeResponse {}))
    }

    async fn node_get_volume_stats(
        &self,
        _request: Request<proto::NodeGetVolumeStatsRequest>,
    ) -> Result<Response<proto::NodeGetVolumeStatsResponse>, Status> {
        Err(Status::unimplemented("volume statistics are not supported"))
    }

    async fn node_expand_volume(
        &self,
        _request: Request<proto::NodeExpandVolumeRequest>,
    ) -> Result<Response<proto::NodeExpandVolumeResponse>, Status> {
        Err(Status::unimplemented("volume expansion is not supported"))
    }

    async fn node_get_capabilities(
        &self,
        _request: Request<proto::NodeGetCapabilitiesRequest>,
    ) -> Result<Response<proto::NodeGetCapabilitiesResponse>, Status> {
        Ok(Response::new(proto::NodeGetCapabilitiesResponse {
            capabilities: vec![proto::NodeServiceCapability {
                r#type: Some(proto::node_service_capability::Type::Rpc(
                    proto::node_service_capability::Rpc {
                        r#type: proto::node_service_capability::rpc::Type::StageUnstageVolume
                            as i32,
                    },
                )),
            }],
        }))
    }

    async fn node_get_info(
        &self,
        _request: Request<proto::NodeGetInfoRequest>,
    ) -> Result<Response<proto::NodeGetInfoResponse>, Status> {
        Ok(Response::new(proto::NodeGetInfoResponse {
            node_id: self.node_id.clone(),
            max_volumes_per_node: 0,
            accessible_topology: None,
        }))
    }
}

fn validate_capabilities(capabilities: &[proto::VolumeCapability]) -> Result<(), Status> {
    if capabilities.is_empty() {
        return Err(Status::invalid_argument(
            "at least one volume capability is required",
        ));
    }
    for capability in capabilities {
        if !matches!(capability.access_type, Some(AccessType::Mount(_))) {
            return Err(Status::invalid_argument(
                "BrewFS supports filesystem mounts only",
            ));
        }
        let mode = capability
            .access_mode
            .as_ref()
            .and_then(|mode| proto::volume_capability::access_mode::Mode::try_from(mode.mode).ok())
            .ok_or_else(|| Status::invalid_argument("volume access mode is required"))?;
        if !matches!(
            mode,
            proto::volume_capability::access_mode::Mode::SingleNodeWriter
                | proto::volume_capability::access_mode::Mode::SingleNodeReaderOnly
        ) {
            return Err(Status::failed_precondition(
                "BrewFS permits one writable lease; multi-writer access is unsupported",
            ));
        }
    }
    Ok(())
}

fn status_for(error: impl Into<CsiError>) -> Status {
    match error.into() {
        CsiError::InvalidVolumeId(error) => Status::invalid_argument(error.to_string()),
        CsiError::InvalidContext(message) => Status::invalid_argument(message),
        CsiError::Busy(message) => Status::aborted(message),
        CsiError::Published(message) => Status::failed_precondition(message),
        CsiError::MountConflict(message) => Status::failed_precondition(message),
        CsiError::Io(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
            Status::aborted(error.to_string())
        }
        CsiError::Io(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Status::not_found(error.to_string())
        }
        CsiError::Io(error) => Status::internal(error.to_string()),
        CsiError::MountCommand(message) => Status::internal(message),
    }
}

async fn serve_unix(
    socket: PathBuf,
    identity: IdentityService,
    controller: ControllerService,
    node: NodeService,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(parent) = socket.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    match tokio::fs::remove_file(&socket).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let listener = UnixListener::bind(&socket)?;
    tonic::transport::Server::builder()
        .add_service(IdentityServer::new(identity))
        .add_service(ControllerServer::new(controller))
        .add_service(NodeServer::new(node))
        .serve_with_incoming(UnixListenerStream::new(listener))
        .await?;
    Ok(())
}

async fn serve_tcp(
    address: std::net::SocketAddr,
    identity: IdentityService,
    controller: ControllerService,
    node: NodeService,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(address).await?;
    tonic::transport::Server::builder()
        .add_service(IdentityServer::new(identity))
        .add_service(ControllerServer::new(controller))
        .add_service(NodeServer::new(node))
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    let _lock = lock_state_dir(&args.state_dir)?;
    let manager = MountManager::new_with_root(SystemMountOps, args.state_dir.clone());
    let identity = IdentityService;
    let controller = ControllerService;
    let node = NodeService {
        manager,
        node_id: args.node_id,
    };

    if let Some(address) = args.listen {
        let unix = serve_unix(args.socket, identity, controller, node.clone());
        let tcp = serve_tcp(address, IdentityService, ControllerService, node);
        tokio::try_join!(unix, tcp)?;
    } else {
        serve_unix(args.socket, identity, controller, node).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn static_volumes_do_not_advertise_controller_service() {
        let response = IdentityService
            .get_plugin_capabilities(Request::new(proto::GetPluginCapabilitiesRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert!(response.capabilities.is_empty());
    }

    #[tokio::test]
    async fn static_volumes_do_not_advertise_controller_rpcs() {
        let response = ControllerService
            .controller_get_capabilities(Request::new(proto::ControllerGetCapabilitiesRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert!(response.capabilities.is_empty());
    }

    fn single_node_mount() -> proto::VolumeCapability {
        proto::VolumeCapability {
            access_type: Some(AccessType::Mount(Default::default())),
            access_mode: Some(proto::volume_capability::AccessMode {
                mode: proto::volume_capability::access_mode::Mode::SingleNodeWriter as i32,
            }),
        }
    }

    #[tokio::test]
    async fn static_volumes_reject_create_requests_without_promising_capacity() {
        for required_bytes in [1 << 30, 2 << 30] {
            let request = proto::CreateVolumeRequest {
                name: "same-name".to_owned(),
                capacity_range: Some(proto::CapacityRange {
                    required_bytes,
                    limit_bytes: 0,
                }),
                volume_capabilities: vec![single_node_mount()],
                parameters: HashMap::from([
                    ("cluster".to_owned(), "cluster-a".to_owned()),
                    ("workspace".to_owned(), uuid::Uuid::from_u128(7).to_string()),
                    ("config".to_owned(), "/etc/brewfs/config.yaml".to_owned()),
                ]),
                ..Default::default()
            };
            assert_eq!(
                ControllerService
                    .create_volume(Request::new(request))
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::Unimplemented
            );
        }
    }

    #[tokio::test]
    async fn static_volumes_do_not_confirm_unknown_volumes() {
        let request = proto::ValidateVolumeCapabilitiesRequest {
            volume_id: VolumeId::new("cluster-a", uuid::Uuid::from_u128(8))
                .unwrap()
                .as_string(),
            volume_capabilities: vec![single_node_mount()],
            ..Default::default()
        };
        assert_eq!(
            ControllerService
                .validate_volume_capabilities(Request::new(request))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unimplemented
        );
    }

    #[tokio::test]
    async fn static_volumes_do_not_offer_controller_deletion() {
        let request = proto::DeleteVolumeRequest {
            volume_id: VolumeId::new("cluster-a", uuid::Uuid::from_u128(8))
                .unwrap()
                .as_string(),
            ..Default::default()
        };
        assert_eq!(
            ControllerService
                .delete_volume(Request::new(request))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unimplemented
        );
    }

    #[test]
    fn maps_driver_errors_to_csi_statuses() {
        assert_eq!(
            status_for(CsiError::Io(std::io::Error::from(
                std::io::ErrorKind::WouldBlock,
            )))
            .code(),
            tonic::Code::Aborted
        );
        assert_eq!(
            status_for(CsiError::MountConflict("foreign mount".to_owned())).code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            status_for(CsiError::Busy("lease held".to_owned())).code(),
            tonic::Code::Aborted
        );
        assert_eq!(
            status_for(CsiError::Published("target".to_owned())).code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            status_for(CsiError::InvalidContext("config".to_owned())).code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            status_for(CsiError::Io(std::io::Error::from(
                std::io::ErrorKind::NotFound,
            )))
            .code(),
            tonic::Code::NotFound
        );
    }

    #[test]
    fn rejects_multi_writer_and_multi_node_read_modes() {
        let mount = proto::VolumeCapability {
            access_type: Some(AccessType::Mount(proto::volume_capability::MountVolume {
                fs_type: String::new(),
                mount_flags: Vec::new(),
                volume_mount_group: String::new(),
            })),
            access_mode: Some(proto::volume_capability::AccessMode {
                mode: proto::volume_capability::access_mode::Mode::MultiNodeMultiWriter as i32,
            }),
        };
        assert_eq!(
            validate_capabilities(&[mount]).unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
    }
}
