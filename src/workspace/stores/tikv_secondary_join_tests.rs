use super::*;
use futures::FutureExt;

#[tokio::test]
#[ignore = "requires isolated BREWFS_TEST_TIKV_PD_ENDPOINTS"]
async fn real_tikv_secondary_join_multikey_cas_allows_immediate_bounded_point_and_scan() {
    let Some(endpoints) = crate::test_support::tikv_endpoints() else {
        eprintln!("skip: BREWFS_TEST_TIKV_PD_ENDPOINTS is unset or empty");
        return;
    };
    let namespace = format!("secondary-join-{}", uuid::Uuid::new_v4().simple());
    let budget = SnapshotMountBudget::defaults();
    let backend = TiKvWorkspaceBackend::connect_with_budget(endpoints, &namespace, budget.clone())
        .await
        .unwrap();
    let keys = [b"secondary/a".to_vec(), b"secondary/b".to_vec()];
    let limits = KvReadLimits {
        max_records: 2,
        max_key_bytes: 64,
        max_value_bytes: 128,
        max_total_bytes: 512,
        max_response_bytes: BOUNDED_READ_MESSAGE_BYTES,
        max_data_requests: 8,
    };
    let result = std::panic::AssertUnwindSafe(async {
        let mut previous: [Option<Vec<u8>>; 2] = [None, None];
        for round in 0u64..32 {
            let current = [
                round.to_le_bytes().to_vec(),
                (round + 100).to_le_bytes().to_vec(),
            ];
            let checks = keys
                .iter()
                .zip(&previous)
                .map(|(key, expected)| KvCheck {
                    key: key.clone(),
                    expected: expected.clone(),
                })
                .collect::<Vec<_>>();
            let writes = keys
                .iter()
                .zip(&current)
                .map(|(key, value)| KvWrite::Put {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect::<Vec<_>>();
            assert!(backend.compare_and_swap(&checks, &writes).await.unwrap());
            // Neither read may resolve locks or wait on a caller-invented timer.
            let (values, now) = backend
                .get_many_consistent_with_time_bounded(&keys, limits)
                .await
                .unwrap();
            assert!(now > 0);
            assert_eq!(
                values,
                current.iter().cloned().map(Some).collect::<Vec<_>>()
            );
            let page = backend
                .scan_prefix_page_with_byte_limits(b"secondary/", None, limits)
                .await
                .unwrap();
            assert_eq!(page.len(), 2);
            for (index, entry) in page.into_iter().enumerate() {
                assert_eq!(entry.key, keys[index]);
                assert_eq!(entry.value, current[index]);
            }
            previous = current.map(Some);
        }
    })
    .catch_unwind()
    .await;
    let cleanup = backend
        .compare_and_swap(
            &[],
            &keys
                .iter()
                .cloned()
                .map(|key| KvWrite::Delete { key })
                .collect::<Vec<_>>(),
        )
        .await;
    backend.shutdown().await.unwrap();
    drop(backend);
    assert_eq!(budget.state().used, [0; 8]);
    assert!(cleanup.unwrap());
    result.unwrap();
}
