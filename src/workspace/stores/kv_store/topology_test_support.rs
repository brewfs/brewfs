//! Entity catalog fixtures and observations, compiled only for tests.

use super::*;

fn fixture_entity_key(key: &[u8]) -> bool {
    [
        HOT_WORKSPACE_PREFIX,
        HOT_LAYER_PREFIX,
        HOT_LEASE_PREFIX,
        HOT_JOURNAL_PREFIX,
        HOT_SNAPSHOT_PREFIX,
        HOT_ALLOCATOR_PREFIX,
        LEASE_INDEX_PREFIX,
        JOURNAL_INDEX_PREFIX,
        SNAPSHOT_NAME_PREFIX,
    ]
    .iter()
    .any(|prefix| key.starts_with(prefix))
}

pub(super) fn test_topology_from_rows(rows: &BTreeMap<Vec<u8>, Vec<u8>>) -> ControlState {
    let checks = rows
        .iter()
        .filter(|(key, _)| key.as_slice() == CONTROL_KEY || fixture_entity_key(key))
        .map(|(key, value)| KvCheck {
            key: key.clone(),
            expected: Some(value.clone()),
        })
        .collect::<Vec<_>>();
    topology_state_from_checks(&checks).expect("valid fixture entity catalog")
}

pub(super) fn test_write_topology_rows(
    rows: &mut BTreeMap<Vec<u8>, Vec<u8>>,
    state: &ControlState,
) {
    let previous = rows
        .iter()
        .filter(|(key, _)| fixture_entity_key(key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    rows.retain(|key, _| !fixture_entity_key(key));
    let KvWrite::Put { key, value } = put_control(&ControlHeader {
        schema_version: state.schema_version,
        header: state.header.clone(),
        catalog_format: CATALOG_FORMAT,
    })
    .unwrap() else {
        unreachable!()
    };
    rows.insert(key, value);
    let mut writes = Vec::new();
    append_hot_diff(&ControlState::default(), state, &mut writes).unwrap();
    append_entity_indexes(&ControlState::default(), state, &mut writes).unwrap();
    for write in writes {
        match write {
            KvWrite::Put { key, value } => {
                rows.insert(key, value);
            }
            KvWrite::Delete { key } => {
                rows.remove(&key);
            }
        }
    }
    let next = rows
        .iter()
        .filter(|(key, _)| fixture_entity_key(key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    if previous != next {
        let raw = rows.get(TOPOLOGY_GENERATION_KEY).cloned();
        rows.insert(
            TOPOLOGY_GENERATION_KEY.to_vec(),
            encode(&next_layer_inventory_generation(&raw).unwrap()).unwrap(),
        );
    }
}

pub(super) fn test_entity_packet(
    before: &ControlState,
    after: &ControlState,
) -> (Vec<KvCheck>, Vec<KvWrite>) {
    let mut rows = BTreeMap::new();
    test_write_topology_rows(&mut rows, before);
    let mut writes = Vec::new();
    append_hot_diff(before, after, &mut writes).unwrap();
    append_entity_indexes(before, after, &mut writes).unwrap();
    let checks = writes
        .iter()
        .map(|write| {
            let key = match write {
                KvWrite::Put { key, .. } | KvWrite::Delete { key } => key,
            };
            KvCheck {
                key: key.clone(),
                expected: rows.get(key).cloned(),
            }
        })
        .collect();
    (checks, writes)
}

pub(super) async fn test_entity_state<B: WorkspaceKvBackend + ?Sized>(backend: &B) -> ControlState {
    let mut rows = BTreeMap::new();
    rows.insert(
        CONTROL_KEY.to_vec(),
        backend.get(CONTROL_KEY).await.unwrap().unwrap(),
    );
    for prefix in [
        HOT_WORKSPACE_PREFIX,
        HOT_LAYER_PREFIX,
        HOT_LEASE_PREFIX,
        HOT_JOURNAL_PREFIX,
        HOT_SNAPSHOT_PREFIX,
        HOT_ALLOCATOR_PREFIX,
    ] {
        let mut after = None;
        loop {
            let page = backend
                .scan_prefix_page_with_byte_limits(
                    prefix,
                    after.as_deref(),
                    topology_point_limits(32),
                )
                .await
                .unwrap();
            if page.is_empty() {
                break;
            }
            for entry in &page {
                rows.insert(entry.key.clone(), entry.value.clone());
            }
            after = page.last().map(|entry| entry.key.clone());
        }
    }
    test_topology_from_rows(&rows)
}
