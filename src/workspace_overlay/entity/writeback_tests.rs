use super::*;
use crate::workspace_overlay::entity::backend::MemoryEntityBackend;
use crate::workspace_overlay::entity::local_test_objects::DurableLocalObjects;
use crate::workspace_overlay::entity::model::LowerBinding;
use crate::workspace_overlay::entity::ownership::Ownership;
use uuid::Uuid;

type Queue = EntityWriteback<MemoryEntityBackend, DurableLocalObjects>;

async fn queue(spool: &Path, remote: &Path, capacity: u64) -> (Arc<Queue>, WriterSession) {
    let backend = Arc::new(MemoryEntityBackend::new(1000));
    let keys = EntityKeys::new(Uuid::new_v4(), Uuid::new_v4());
    let lifecycle = Lifecycle::new(backend.clone(), keys.clone());
    lifecycle
        .provision(&LowerBinding {
            manifest_key: b"test-lower".to_vec(),
            manifest_digest: [1; 32],
            root_ino: 1,
            max_ino: 1,
        })
        .await
        .unwrap();
    let session = lifecycle.acquire(Uuid::new_v4(), 1_000_000).await.unwrap();
    let queue = EntityWriteback::open(
        backend,
        Arc::new(DurableLocalObjects::new(remote)),
        keys,
        WritebackConfig {
            directory: spool.into(),
            capacity_bytes: capacity,
            min_free_bytes: 0,
        },
    )
    .unwrap();
    queue.enable(&session).await.unwrap();
    (queue, session)
}

async fn reserve(
    queue: &Arc<Queue>,
    session: &WriterSession,
    count: usize,
) -> Vec<ObjectDescriptor> {
    let authority = Lifecycle::new(queue.backend.clone(), queue.keys.clone())
        .authority(session)
        .await
        .unwrap();
    Ownership::new(queue.backend.clone(), queue.keys.clone())
        .reserve_uploads(session, &authority, &vec![8; count])
        .await
        .unwrap()
        .into_iter()
        .map(|token| ObjectDescriptor {
            namespace_id: queue.keys.namespace_id(),
            slice_id: token.slice_id(),
            size: 8,
            sha256: Sha256::digest(b"original").into(),
        })
        .collect()
}

async fn stage(queue: &Arc<Queue>, session: &WriterSession, count: usize) -> Vec<ObjectDescriptor> {
    let descriptors = reserve(queue, session, count).await;
    for descriptor in &descriptors {
        queue.stage(session, descriptor, b"original").await.unwrap();
    }
    descriptors
}

#[tokio::test]
async fn completion_batch_checks_every_identity_before_atomic_counter_update() {
    let spool = tempfile::tempdir().unwrap();
    let remote = tempfile::tempdir().unwrap();
    let (queue, session) = queue(spool.path(), remote.path(), 1 << 20).await;
    let descriptors = stage(&queue, &session, 4).await;
    let mut wrong = descriptors.clone();
    wrong[3].sha256 = [0; 32];
    assert!(matches!(
        queue.finish_batch(&wrong).await,
        Err(EntityError::Corrupt(_))
    ));
    assert_eq!(queue.status().await.unwrap().pending_objects, 4);
    assert_eq!(queue.status().await.unwrap().completion_debt_attempts, 0);
    queue.upload_pass().await.unwrap();
    let status = queue.status().await.unwrap();
    assert_eq!((status.pending_objects, status.pending_bytes), (0, 0));
    assert_eq!(status.completion_debt_attempts, 1);
    assert_eq!(status.stage_debt_attempts, 4);
    assert_eq!(status.staging_file_syncs, 4);
    assert_eq!(queue.used.load(Ordering::Acquire), 0);
    // A reconciled/lost completion reply cannot decrement the counter twice.
    queue.finish_batch(&descriptors).await.unwrap();
    assert_eq!(queue.status().await.unwrap().completion_debt_attempts, 1);
}

#[tokio::test]
async fn failed_payload_keeps_its_debt_while_successful_peers_are_reclaimed() {
    let spool = tempfile::tempdir().unwrap();
    let remote = tempfile::tempdir().unwrap();
    let (queue, session) = queue(spool.path(), remote.path(), 1 << 20).await;
    let descriptors = stage(&queue, &session, 4).await;
    std::fs::remove_file(queue.path(descriptors[1].slice_id)).unwrap();
    assert!(queue.upload_pass().await.is_err());
    let status = queue.status().await.unwrap();
    assert_eq!((status.pending_objects, status.pending_bytes), (1, 8));
    assert_eq!(status.uploaded_bytes, 24);
    for (index, descriptor) in descriptors.iter().enumerate() {
        let remote = queue
            .objects
            .get_object(&queue.keys.object(descriptor.slice_id))
            .await
            .unwrap();
        if index == 1 {
            assert!(remote.is_none());
        } else {
            assert_eq!(remote.unwrap(), b"original");
        }
    }
    assert!(
        queue
            .backend
            .read(&[queue.debt_key(descriptors[1].slice_id)])
            .await
            .unwrap()
            .rows[0]
            .is_some()
    );
}

#[tokio::test]
async fn parallel_staging_reserves_capacity_and_reuses_space_after_upload() {
    let spool = tempfile::tempdir().unwrap();
    let remote = tempfile::tempdir().unwrap();
    let (queue, session) = queue(spool.path(), remote.path(), 5000).await;
    let descriptors = reserve(&queue, &session, 32).await;
    let results = futures::future::join_all(
        descriptors
            .iter()
            .map(|descriptor| queue.stage(&session, descriptor, b"original")),
    )
    .await;
    let successful = results.iter().filter(|result| result.is_ok()).count();
    assert!(successful > 0 && successful < 32);
    assert!(
        results
            .iter()
            .all(|result| result.is_ok() || matches!(result, Err(EntityError::Busy)))
    );
    assert!(queue.used.load(Ordering::Acquire) <= 5000);
    assert_eq!(
        queue.status().await.unwrap().pending_objects,
        successful as u64
    );
    queue.upload_pass().await.unwrap();
    assert_eq!(queue.used.load(Ordering::Acquire), 0);
    stage(&queue, &session, 1).await;
    queue.upload_pass().await.unwrap();
    assert_eq!(queue.status().await.unwrap().pending_objects, 0);
}

#[tokio::test]
async fn create_only_collision_cannot_clear_debt_for_different_remote_bytes() {
    let spool = tempfile::tempdir().unwrap();
    let remote = tempfile::tempdir().unwrap();
    let (queue, session) = queue(spool.path(), remote.path(), 1 << 20).await;
    let descriptors = stage(&queue, &session, 1).await;
    let key = queue.keys.object(descriptors[0].slice_id);
    queue
        .objects
        .put_object_create_only_durable(&key, b"conflict")
        .await
        .unwrap();
    assert!(queue.upload_pass().await.is_err());
    assert_eq!(queue.status().await.unwrap().pending_objects, 1);
    assert_eq!(
        queue.objects.get_object(&key).await.unwrap().unwrap(),
        b"conflict"
    );
    assert!(queue.path(descriptors[0].slice_id).exists());
}

#[tokio::test]
async fn final_reconciliation_never_uploads_unregistered_staging_files() {
    let spool = tempfile::tempdir().unwrap();
    let remote = tempfile::tempdir().unwrap();
    let (queue, session) = queue(spool.path(), remote.path(), 1 << 20).await;
    let descriptors = reserve(&queue, &session, 1).await;
    // Crash boundary after durable file creation, before debt registration.
    queue.persist(&descriptors[0], b"original").unwrap();
    // recover_uploads explicitly calls upload_pass even for initially zero debt.
    queue.upload_pass().await.unwrap();
    assert_eq!(queue.used.load(Ordering::Acquire), 0);
    assert!(!queue.path(descriptors[0].slice_id).exists());
    assert!(
        queue
            .objects
            .get_object(&queue.keys.object(descriptors[0].slice_id))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn duplicate_stage_cannot_replace_previously_acknowledged_bytes() {
    let spool = tempfile::tempdir().unwrap();
    let remote = tempfile::tempdir().unwrap();
    let (queue, session) = queue(spool.path(), remote.path(), 1 << 20).await;
    let descriptors = stage(&queue, &session, 1).await;
    let mut conflict = descriptors[0].clone();
    conflict.sha256 = Sha256::digest(b"conflict").into();
    assert!(queue.stage(&session, &conflict, b"conflict").await.is_err());
    assert_eq!(
        queue.read(&descriptors[0]).await.unwrap().unwrap(),
        b"original"
    );
    assert_eq!(queue.status().await.unwrap().pending_bytes, 8);
    queue.upload_pass().await.unwrap();
    assert_eq!(
        queue
            .objects
            .get_object(&queue.keys.object(conflict.slice_id))
            .await
            .unwrap()
            .unwrap(),
        b"original"
    );
}

#[tokio::test]
async fn explicit_recovery_authenticates_completed_leftovers_before_reclaiming() {
    let spool = tempfile::tempdir().unwrap();
    let remote = tempfile::tempdir().unwrap();
    let (queue, session) = queue(spool.path(), remote.path(), 1 << 20).await;
    let descriptors = stage(&queue, &session, 1).await;
    let descriptor = &descriptors[0];
    queue.upload_one(descriptor).await.unwrap();
    queue.finish_batch(&descriptors).await.unwrap();
    // Model a published descriptor and a crash before local file reclamation.
    let authority = Lifecycle::new(queue.backend.clone(), queue.keys.clone())
        .authority(&session)
        .await
        .unwrap();
    queue
        .backend
        .commit(
            &authority,
            &EntityPlan {
                checks: vec![],
                writes: vec![RowWrite::Put {
                    key: queue.keys.object_descriptor(descriptor.slice_id),
                    value: encode(descriptor).unwrap(),
                }],
            },
        )
        .await
        .unwrap();
    let key = queue.keys.object(descriptor.slice_id);
    queue.objects.put_object(&key, b"tampered").await.unwrap();
    assert!(queue.upload_pass().await.is_err());
    assert!(queue.path(descriptor.slice_id).exists());
    queue.objects.put_object(&key, b"original").await.unwrap();
    queue.upload_pass().await.unwrap();
    assert!(!queue.path(descriptor.slice_id).exists());
    assert_eq!(queue.used.load(Ordering::Acquire), 0);
}
