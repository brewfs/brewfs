//! O(1) fork and guarded fast-forward over a permanently retained snapshot.

use sha2::{Digest, Sha256};

use crate::native_base::wire::bnct::{
    ControlRecord, DomainKind, DomainState, Id16, NativeWorkspaceHead, OwnershipDomain,
    PublishedRevision, SnapshotRef, WorkspaceHeadState,
};
use crate::native_base::wire::refs::Hash32;
use crate::native_base::wire::uvarint::Writer;
use crate::native_base::write::keys::Keys;
use crate::native_base::write::records::HeadState;
use crate::native_base::write::store::{ControlStore, Txn};

use super::seal::next_head_ref;
use super::{LifecycleError, LifecycleResult, map_conflict};

#[derive(Debug, Clone)]
pub struct ForkRequest {
    pub operation_id: Id16,
    pub workspace_id: Id16,
    pub owner_id: Id16,
    pub owner_generation: u64,
    pub new_head_id: Id16,
    pub new_domain_id: Id16,
    pub namespace_id: Id16,
    pub source: PublishedRevision,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForkOutcome {
    pub workspace_id: Id16,
    pub domain_id: Id16,
    pub head: HeadState,
    pub view: NativeWorkspaceHead,
}

fn source_snapshot(source: &PublishedRevision) -> SnapshotRef {
    SnapshotRef {
        volume_id: source.volume_id,
        logical_revision: source.logical_revision,
        manifest: source.manifest.clone(),
    }
}

fn fork_digest(request: &ForkRequest) -> Hash32 {
    let mut w = Writer::new();
    w.put(b"BrewFS.Fork.v1\0");
    w.put(&request.operation_id);
    w.put(&request.workspace_id);
    w.put(&request.owner_id);
    w.u64(request.owner_generation);
    w.put(&request.new_head_id);
    w.put(&request.new_domain_id);
    w.put(&request.namespace_id);
    w.bytes(&ControlRecord::PublishedRevision(request.source.clone()).encode());
    Sha256::digest(w.as_slice()).into()
}

pub async fn fork_from_published(
    store: &dyn ControlStore,
    keys: &Keys,
    request: &ForkRequest,
) -> LifecycleResult<ForkOutcome> {
    let op_key = keys.fork_operation(&request.operation_id);
    let digest = fork_digest(request);
    if let Some(existing) = store.get(&op_key).await? {
        if existing.len() != 32 + 16 + 16 || existing[..32] != digest[..] {
            return Err(LifecycleError::OperationIdMismatch(
                "fork OperationId was used with another payload".into(),
            ));
        }
        let domain_id: Id16 = existing[48..64].try_into().unwrap();
        let head_bytes = store
            .get(&keys.head(&request.workspace_id))
            .await?
            .ok_or_else(|| LifecycleError::Record("fork operation has no head".into()))?;
        let head = HeadState::decode(&head_bytes)?;
        let view_bytes = store
            .get(&keys.workspace_view(&request.workspace_id))
            .await?
            .ok_or_else(|| LifecycleError::Record("fork operation has no view".into()))?;
        let view = super::seal::decode_workspace_view(&view_bytes)?;
        if view.write_domain_id != domain_id {
            return Err(LifecycleError::Record(
                "fork operation domain mismatch".into(),
            ));
        }
        return Ok(ForkOutcome {
            workspace_id: request.workspace_id,
            domain_id,
            head,
            view,
        });
    }
    if request.source.volume_id != keys.volume_id()
        || request.source.storage_view_id != request.source.manifest.full_hash
        || request.source.retention_policy
            != crate::native_base::wire::bnct::RetentionPolicy::Forever
    {
        return Err(LifecycleError::Retention(
            "fork source is not a forever-retained revision of this volume".into(),
        ));
    }
    let source_key = keys.published_revision(&request.source.storage_view_id);
    let source_bytes = store.get(&source_key).await?.ok_or_else(|| {
        LifecycleError::Retention("fork source PublishedRevision not found".into())
    })?;
    if super::publish::decode_published_revision(&source_bytes)? != request.source {
        return Err(LifecycleError::Retention(
            "fork source record does not match the requested immutable revision".into(),
        ));
    }
    if request.new_domain_id == [0u8; 16]
        || request.workspace_id == [0u8; 16]
        || request.new_head_id == [0u8; 16]
        || request.owner_generation == 0
    {
        return Err(LifecycleError::InvalidState(
            "fork workspace/domain/head ids and owner generation must be non-zero".into(),
        ));
    }
    let snapshot = source_snapshot(&request.source);
    let head = HeadState {
        head: crate::native_base::wire::bnct::HeadRef {
            head_id: request.new_head_id,
            epoch: 1,
            commit_seq: 0,
        },
        writer_generation: request.owner_generation,
        write_domain_id: request.new_domain_id,
    };
    let view = NativeWorkspaceHead {
        workspace_id: request.workspace_id,
        head: head.head.clone(),
        base: snapshot.clone(),
        writer_generation: request.owner_generation,
        write_domain_id: request.new_domain_id,
        visible_delta_count: 0,
        open_orphan_count: 0,
        orphan_carry_digest: None,
        state: WorkspaceHeadState::Running,
        entity_version: 1,
    };
    let domain = OwnershipDomain {
        domain_id: request.new_domain_id,
        volume_id: keys.volume_id(),
        namespace_id: request.namespace_id,
        domain_kind: DomainKind::Workspace,
        owner_id: request.owner_id,
        owner_generation: request.owner_generation,
        state: DomainState::Active,
        entity_version: 1,
        inventory_seq: 0,
        retention_seq: 0,
        outstanding_attempts: 0,
        open_operations: 0,
        accepted_ticket_end: None,
        close_ref: None,
    };
    let mut op_value = Vec::with_capacity(64);
    op_value.extend_from_slice(&digest);
    op_value.extend_from_slice(&request.workspace_id);
    op_value.extend_from_slice(&request.new_domain_id);
    let head_key = keys.head(&request.workspace_id);
    let view_key = keys.workspace_view(&request.workspace_id);
    let domain_key = keys.domain(&request.new_domain_id);
    let mut fork_base_writer = Writer::new();
    snapshot.encode_into(&mut fork_base_writer);
    store
        .run(
            Txn::new()
                .check_bytes(source_key, source_bytes)
                .check_absent(op_key.clone())
                .check_absent(head_key.clone())
                .check_absent(view_key.clone())
                .check_absent(domain_key.clone())
                .check_absent(keys.fork_base(&request.workspace_id))
                .put(domain_key, ControlRecord::OwnershipDomain(domain).encode())
                .put(head_key, head.encode())
                .put(
                    view_key,
                    ControlRecord::NativeWorkspaceHead(view.clone()).encode(),
                )
                .put(
                    keys.fork_base(&request.workspace_id),
                    fork_base_writer.into_bytes(),
                )
                .put(op_key, op_value),
        )
        .await
        .map_err(|e| map_conflict(e, "fork raced with workspace/domain creation"))?;
    Ok(ForkOutcome {
        workspace_id: request.workspace_id,
        domain_id: request.new_domain_id,
        head,
        view,
    })
}

#[derive(Debug, Clone)]
pub struct FastForwardRequest {
    pub operation_id: Id16,
    pub workspace_id: Id16,
    pub expected_head: HeadState,
    pub expected_view: NativeWorkspaceHead,
    pub expected_fork_base: SnapshotRef,
    pub source: PublishedRevision,
    pub new_head_id: Id16,
}

fn fast_forward_digest(request: &FastForwardRequest) -> Hash32 {
    let mut w = Writer::new();
    w.put(b"BrewFS.FastForward.v1\0");
    w.put(&request.operation_id);
    w.put(&request.workspace_id);
    w.bytes(&request.expected_head.encode());
    w.bytes(&ControlRecord::NativeWorkspaceHead(request.expected_view.clone()).encode());
    request.expected_fork_base.encode_into(&mut w);
    w.bytes(&ControlRecord::PublishedRevision(request.source.clone()).encode());
    w.put(&request.new_head_id);
    Sha256::digest(w.as_slice()).into()
}

pub async fn fast_forward(
    store: &dyn ControlStore,
    keys: &Keys,
    request: &FastForwardRequest,
) -> LifecycleResult<ForkOutcome> {
    let operation_key = keys.fork_operation(&request.operation_id);
    let operation_digest = fast_forward_digest(request);
    if let Some(existing) = store.get(&operation_key).await? {
        if existing.len() != 64 || existing[..32] != operation_digest[..] {
            return Err(LifecycleError::OperationIdMismatch(
                "fast-forward OperationId was used with another payload".into(),
            ));
        }
        let domain_id: Id16 = existing[48..64].try_into().unwrap();
        let head = HeadState::decode(
            &store
                .get(&keys.head(&request.workspace_id))
                .await?
                .ok_or_else(|| LifecycleError::Record("fast-forward result head missing".into()))?,
        )?;
        let view = super::seal::decode_workspace_view(
            &store
                .get(&keys.workspace_view(&request.workspace_id))
                .await?
                .ok_or_else(|| LifecycleError::Record("fast-forward result view missing".into()))?,
        )?;
        if head.write_domain_id != domain_id || view.write_domain_id != domain_id {
            return Err(LifecycleError::Record(
                "fast-forward operation result domain mismatch".into(),
            ));
        }
        return Ok(ForkOutcome {
            workspace_id: request.workspace_id,
            domain_id,
            head,
            view,
        });
    }
    let head_key = keys.head(&request.workspace_id);
    let view_key = keys.workspace_view(&request.workspace_id);
    let head_bytes = store
        .get(&head_key)
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("fast-forward target head missing".into()))?;
    let view_bytes = store
        .get(&view_key)
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("fast-forward target view missing".into()))?;
    let current_head = HeadState::decode(&head_bytes)?;
    let current_view = super::seal::decode_workspace_view(&view_bytes)?;
    if current_head != request.expected_head || current_view != request.expected_view {
        return Err(LifecycleError::Conflict(
            "fast-forward target head/view is stale".into(),
        ));
    }
    if current_view.visible_delta_count != 0
        || current_view.open_orphan_count != 0
        || current_view.state != WorkspaceHeadState::Running
        || store
            .get(&keys.writer_lease(&request.workspace_id))
            .await?
            .is_some()
    {
        return Err(LifecycleError::Conflict(
            "fast-forward target has visible changes, orphans, or a valid writer".into(),
        ));
    }
    let fork_base_bytes = store
        .get(&keys.fork_base(&request.workspace_id))
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("fast-forward fork_base is missing".into()))?;
    let mut fork_base_reader = crate::native_base::wire::uvarint::Reader::new(&fork_base_bytes);
    let fork_base = SnapshotRef::decode(&mut fork_base_reader)?;
    if !fork_base_reader.is_empty() {
        return Err(LifecycleError::Record(
            "fast-forward fork_base has trailing bytes".into(),
        ));
    }
    if fork_base != request.expected_fork_base || current_view.base != fork_base {
        return Err(LifecycleError::Conflict(
            "fast-forward fork_base does not exactly match target base".into(),
        ));
    }
    let source_key = keys.published_revision(&request.source.storage_view_id);
    let source_bytes = store
        .get(&source_key)
        .await?
        .ok_or_else(|| LifecycleError::Retention("fast-forward source not retained".into()))?;
    if super::publish::decode_published_revision(&source_bytes)? != request.source {
        return Err(LifecycleError::Retention(
            "fast-forward source changed".into(),
        ));
    }
    if request.source.volume_id != keys.volume_id()
        || request.source.storage_view_id != request.source.manifest.full_hash
    {
        return Err(LifecycleError::Retention(
            "fast-forward source volume mismatch".into(),
        ));
    }
    if request.new_head_id == [0u8; 16] {
        return Err(LifecycleError::InvalidState(
            "fast-forward head id must be non-zero".into(),
        ));
    }
    let new_head_ref = next_head_ref(&current_head.head, request.new_head_id)?;
    let new_head = HeadState {
        head: new_head_ref.clone(),
        writer_generation: current_head.writer_generation,
        write_domain_id: current_head.write_domain_id,
    };
    let mut new_view = current_view;
    new_view.head = new_head_ref;
    new_view.base = source_snapshot(&request.source);
    new_view.entity_version = new_view
        .entity_version
        .checked_add(1)
        .ok_or_else(|| LifecycleError::LimitExceeded("workspace entity version overflow".into()))?;
    let result = ForkOutcome {
        workspace_id: request.workspace_id,
        domain_id: current_head.write_domain_id,
        head: new_head.clone(),
        view: new_view.clone(),
    };
    let mut operation_value = Vec::with_capacity(64);
    operation_value.extend_from_slice(&operation_digest);
    operation_value.extend_from_slice(&request.workspace_id);
    operation_value.extend_from_slice(&current_head.write_domain_id);
    store
        .run(
            Txn::new()
                .check_absent(operation_key.clone())
                .check_bytes(head_key.clone(), head_bytes)
                .check_bytes(view_key.clone(), view_bytes)
                .check_bytes(source_key, source_bytes)
                .put(head_key, new_head.encode())
                .put(
                    view_key,
                    ControlRecord::NativeWorkspaceHead(new_view).encode(),
                )
                .put(operation_key, operation_value),
        )
        .await
        .map_err(|e| map_conflict(e, "fast-forward target changed during switch"))?;
    Ok(result)
}
