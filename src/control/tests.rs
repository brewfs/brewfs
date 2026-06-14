use super::job::{GcJobResult, JobManager, JobOutcome, JobState};
use super::protocol::{ControlAclEntry, ControlRequest, ControlResponse, ControlTrashEntry};
use super::runtime::{InstanceRecord, RuntimeRegistry};
use super::server::{ControlHandler, ControlServer};
use async_trait::async_trait;
use tempfile::tempdir;

#[test]
fn protocol_roundtrip_preserves_gc_request() {
    let req = ControlRequest::RunGc { dry_run: true };
    let raw = serde_json::to_vec(&req).expect("serialize request");
    let decoded: ControlRequest = serde_json::from_slice(&raw).expect("deserialize request");

    assert_eq!(decoded, req);
}

#[test]
fn protocol_roundtrip_preserves_directory_listing_request() {
    let req = ControlRequest::ListDirectory {
        path: "/projects".to_string(),
    };
    let raw = serde_json::to_vec(&req).expect("serialize request");
    let decoded: ControlRequest = serde_json::from_slice(&raw).expect("deserialize request");

    assert_eq!(decoded, req);

    let response = ControlResponse::DirectoryListing {
        path: "/projects".to_string(),
        entries: vec![super::protocol::ControlDirectoryEntry {
            name: "readme.md".to_string(),
            inode: 42,
            kind: super::protocol::ControlFileKind::File,
            size: 128,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            mtime_ns: 1_786_000_000_000_000_000,
            has_acl: true,
        }],
    };
    let raw = serde_json::to_vec(&response).expect("serialize response");
    let decoded: ControlResponse = serde_json::from_slice(&raw).expect("deserialize response");

    assert_eq!(decoded, response);
}

#[test]
fn protocol_roundtrip_preserves_path_metadata_and_readlink_requests() {
    let stat_req = ControlRequest::StatPath {
        path: "/projects/readme.md".to_string(),
    };
    let raw = serde_json::to_vec(&stat_req).expect("serialize stat request");
    let decoded: ControlRequest = serde_json::from_slice(&raw).expect("deserialize stat request");
    assert_eq!(decoded, stat_req);

    let readlink_req = ControlRequest::ReadLink {
        path: "/latest".to_string(),
    };
    let raw = serde_json::to_vec(&readlink_req).expect("serialize readlink request");
    let decoded: ControlRequest =
        serde_json::from_slice(&raw).expect("deserialize readlink request");
    assert_eq!(decoded, readlink_req);

    let metadata = ControlResponse::PathMetadata {
        path: "/projects/readme.md".to_string(),
        metadata: super::protocol::ControlPathMetadata {
            inode: 42,
            kind: super::protocol::ControlFileKind::File,
            size: 128,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            mtime_ns: 1_786_000_000_000_000_000,
        },
    };
    let raw = serde_json::to_vec(&metadata).expect("serialize metadata response");
    let decoded: ControlResponse =
        serde_json::from_slice(&raw).expect("deserialize metadata response");
    assert_eq!(decoded, metadata);

    let target = ControlResponse::SymlinkTarget {
        path: "/latest".to_string(),
        target: "/projects/readme.md".to_string(),
    };
    let raw = serde_json::to_vec(&target).expect("serialize symlink response");
    let decoded: ControlResponse =
        serde_json::from_slice(&raw).expect("deserialize symlink response");
    assert_eq!(decoded, target);
}

#[test]
fn protocol_roundtrip_preserves_acl_requests_and_responses() {
    let entry = ControlAclEntry {
        scope: "access".to_string(),
        tag: "user_obj".to_string(),
        id: None,
        perm: "rwx".to_string(),
    };
    let get_req = ControlRequest::GetAcl {
        path: "/docs".to_string(),
    };
    let raw = serde_json::to_vec(&get_req).expect("serialize get acl request");
    let decoded: ControlRequest =
        serde_json::from_slice(&raw).expect("deserialize get acl request");
    assert_eq!(decoded, get_req);

    let put_req = ControlRequest::PutAcl {
        path: "/docs".to_string(),
        entries: vec![entry.clone()],
    };
    let raw = serde_json::to_vec(&put_req).expect("serialize put acl request");
    let decoded: ControlRequest =
        serde_json::from_slice(&raw).expect("deserialize put acl request");
    assert_eq!(decoded, put_req);

    let delete_req = ControlRequest::DeleteAcl {
        path: "/docs".to_string(),
    };
    let raw = serde_json::to_vec(&delete_req).expect("serialize delete acl request");
    let decoded: ControlRequest =
        serde_json::from_slice(&raw).expect("deserialize delete acl request");
    assert_eq!(decoded, delete_req);

    let response = ControlResponse::Acl {
        path: "/docs".to_string(),
        entries: vec![entry],
    };
    let raw = serde_json::to_vec(&response).expect("serialize acl response");
    let decoded: ControlResponse = serde_json::from_slice(&raw).expect("deserialize acl response");
    assert_eq!(decoded, response);

    let deleted = ControlResponse::AclDeleted {
        path: "/docs".to_string(),
    };
    let raw = serde_json::to_vec(&deleted).expect("serialize acl delete response");
    let decoded: ControlResponse =
        serde_json::from_slice(&raw).expect("deserialize acl delete response");
    assert_eq!(decoded, deleted);
}

#[test]
fn protocol_roundtrip_preserves_trash_requests_and_responses() {
    let entry = ControlTrashEntry {
        id: "trash-1".to_string(),
        original_path: "/docs/report.txt".to_string(),
        size: Some(42),
        deleted_at: Some("2026-06-11T12:00:00Z".to_string()),
    };
    let list_req = ControlRequest::ListTrash;
    let raw = serde_json::to_vec(&list_req).expect("serialize list trash request");
    let decoded: ControlRequest =
        serde_json::from_slice(&raw).expect("deserialize list trash request");
    assert_eq!(decoded, list_req);

    let restore_req = ControlRequest::RestoreTrashEntry {
        entry_id: "trash-1".to_string(),
    };
    let raw = serde_json::to_vec(&restore_req).expect("serialize restore trash request");
    let decoded: ControlRequest =
        serde_json::from_slice(&raw).expect("deserialize restore trash request");
    assert_eq!(decoded, restore_req);

    let delete_req = ControlRequest::DeleteTrashEntry {
        entry_id: "trash-1".to_string(),
    };
    let raw = serde_json::to_vec(&delete_req).expect("serialize delete trash request");
    let decoded: ControlRequest =
        serde_json::from_slice(&raw).expect("deserialize delete trash request");
    assert_eq!(decoded, delete_req);

    let response = ControlResponse::Trash {
        entries: vec![entry],
    };
    let raw = serde_json::to_vec(&response).expect("serialize trash response");
    let decoded: ControlResponse =
        serde_json::from_slice(&raw).expect("deserialize trash response");
    assert_eq!(decoded, response);

    let restored = ControlResponse::TrashRestored {
        entry_id: "trash-1".to_string(),
    };
    let raw = serde_json::to_vec(&restored).expect("serialize trash restore response");
    let decoded: ControlResponse =
        serde_json::from_slice(&raw).expect("deserialize trash restore response");
    assert_eq!(decoded, restored);

    let deleted = ControlResponse::TrashDeleted {
        entry_id: "trash-1".to_string(),
    };
    let raw = serde_json::to_vec(&deleted).expect("serialize trash delete response");
    let decoded: ControlResponse =
        serde_json::from_slice(&raw).expect("deserialize trash delete response");
    assert_eq!(decoded, deleted);
}

#[tokio::test]
async fn runtime_registry_auto_selects_single_live_instance() {
    let dir = tempdir().expect("tempdir");
    let registry = RuntimeRegistry::new(dir.path().to_path_buf());

    let record = InstanceRecord::new(
        std::process::id(),
        "/mnt/slayer".to_string(),
        registry.socket_path(std::process::id()),
        chrono::Utc::now(),
    );

    registry.write_record(&record).await.expect("write record");

    let selected = registry
        .select_instance(None)
        .await
        .expect("select instance");

    assert_eq!(selected.mount_point, "/mnt/slayer");
    assert_eq!(selected.pid, std::process::id());
}

#[tokio::test]
async fn job_manager_tracks_gc_job_lifecycle() {
    let jobs = JobManager::default();
    let job_id = jobs.create_gc_job(true).await;

    let pending = jobs.get(&job_id).await.expect("pending job");
    assert_eq!(pending.state, JobState::Pending);
    assert_eq!(pending.detail.as_deref(), Some("dry-run"));

    jobs.mark_running(&job_id).await.expect("mark running");

    let running = jobs.get(&job_id).await.expect("running job");
    assert_eq!(running.state, JobState::Running);

    jobs.finish(
        &job_id,
        GcJobResult {
            dry_run: true,
            orphan_slice_count: 2,
            orphan_object_count: 4,
            deleted_object_count: 0,
            error_count: 0,
            detail: Some("dry-run".to_string()),
        },
    )
    .await
    .expect("finish job");

    let finished = jobs.get(&job_id).await.expect("finished job");
    assert_eq!(finished.state, JobState::Succeeded);

    match finished.outcome {
        Some(JobOutcome::Gc(result)) => {
            assert_eq!(result.orphan_slice_count, 2);
            assert_eq!(result.orphan_object_count, 4);
            assert_eq!(result.deleted_object_count, 0);
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

struct FakeHandler;

#[async_trait]
impl ControlHandler for FakeHandler {
    async fn handle(&self, request: ControlRequest) -> ControlResponse {
        match request {
            ControlRequest::Ping => ControlResponse::Pong,
            _ => ControlResponse::Error {
                code: "unsupported".to_string(),
                message: "unsupported".to_string(),
            },
        }
    }
}

#[tokio::test]
async fn uds_server_handles_single_request_response() {
    let dir = tempdir().expect("tempdir");
    let socket_path = dir.path().join("control.sock");
    let _server = ControlServer::bind(socket_path.clone(), FakeHandler)
        .await
        .expect("bind server");

    let response = super::client::send_request(&socket_path, &ControlRequest::Ping)
        .await
        .expect("send request");

    assert_eq!(response, ControlResponse::Pong);
}

#[tokio::test]
async fn uds_server_creates_parent_directory() {
    let dir = tempdir().expect("tempdir");
    let socket_path = dir.path().join("nested").join("control.sock");
    let _server = ControlServer::bind(socket_path.clone(), FakeHandler)
        .await
        .expect("bind server");

    assert!(socket_path.exists());
}
