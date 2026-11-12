use super::*;

fn quiesced() -> SealJournal {
    SealJournal {
        journal_id: JournalId::from_uuid(uuid::Uuid::from_u128(1)),
        workspace_id: WorkspaceId::from_uuid(uuid::Uuid::from_u128(2)),
        old_head_layer_id: LayerId::from_uuid(uuid::Uuid::from_u128(3)),
        expected_head_epoch: 37,
        phase: SealPhase::Quiesced,
        pending_bytes: 8192,
        delta_digest: None,
        root_hash: None,
        new_head_layer_id: Some(LayerId::from_uuid(uuid::Uuid::from_u128(4))),
        last_error: Some("prior drain transport uncertainty".into()),
        created_at_ns: 103,
        updated_at_ns: 107,
    }
}

#[test]
fn two_native_journal_steps_keep_identity_and_use_native_root_contract() {
    let initial = quiesced();
    let digest = crate::workspace::digest::delta_digest(
        &crate::workspace::digest::CanonicalLayerDelta::default(),
    )
    .unwrap();
    let root = crate::workspace::digest::root_hash([31; 32], digest);
    let drained = successor(&initial, SealPhase::DataDrained, 109, digest, root).unwrap();
    assert_eq!(drained.journal_id, initial.journal_id);
    assert_eq!(drained.workspace_id, initial.workspace_id);
    assert_eq!(drained.old_head_layer_id, initial.old_head_layer_id);
    assert_eq!(drained.expected_head_epoch, initial.expected_head_epoch);
    assert_eq!(drained.new_head_layer_id, initial.new_head_layer_id);
    assert_eq!(drained.created_at_ns, initial.created_at_ns);
    assert_eq!(
        (
            drained.pending_bytes,
            drained.delta_digest,
            drained.root_hash
        ),
        (0, None, None)
    );
    assert_eq!(drained.last_error, None);
    let hashed = successor(&drained, SealPhase::Hashed, 113, digest, root).unwrap();
    assert_eq!(
        (hashed.delta_digest, hashed.root_hash),
        (Some(digest), Some(root))
    );
    assert_eq!(hashed.old_head_layer_id, initial.old_head_layer_id);
    assert_eq!(hashed.new_head_layer_id, initial.new_head_layer_id);
    assert_eq!(hashed.created_at_ns, initial.created_at_ns);
    assert_eq!(
        initial,
        quiesced(),
        "successors do not modify immutable predecessor receipt"
    );
}

#[test]
fn native_journal_rejects_skipped_phase_and_preexisting_hashes() {
    let initial = quiesced();
    assert!(successor(&initial, SealPhase::Hashed, 109, [1; 32], [2; 32]).is_err());
    assert!(successor(&initial, SealPhase::DataDrained, 0, [1; 32], [2; 32]).is_err());
    let mut damaged = initial.clone();
    damaged.delta_digest = Some([1; 32]);
    assert!(successor(&damaged, SealPhase::DataDrained, 109, [1; 32], [2; 32]).is_err());
    let mut drained = successor(&initial, SealPhase::DataDrained, 109, [1; 32], [2; 32]).unwrap();
    drained.root_hash = Some([2; 32]);
    assert!(successor(&drained, SealPhase::Hashed, 113, [1; 32], [2; 32]).is_err());
}
