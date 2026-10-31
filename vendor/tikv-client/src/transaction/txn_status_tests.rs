// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.
//! Exercise the complete request-plan error wrapper and lock-resolution flow.
use super::*;
use crate::mock::{MockKvClient, MockPdClient};
use std::any::Any;
use std::sync::Mutex;

fn missing_primary_client(
    enabled: bool,
    malformed: bool,
) -> (
    Arc<MockPdClient>,
    Arc<Mutex<Vec<bool>>>,
    Arc<Mutex<Vec<u64>>>,
) {
    let checks = Arc::new(Mutex::new(Vec::new()));
    let resolutions = Arc::new(Mutex::new(Vec::new()));
    let observed_checks = checks.clone();
    let observed_resolutions = resolutions.clone();
    let client = MockPdClient::new(MockKvClient::with_dispatch_hook(
        move |request: &dyn Any| {
            if let Some(request) = request.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                observed_checks
                    .lock()
                    .unwrap()
                    .push(request.rollback_if_not_exist);
                let response = if request.rollback_if_not_exist {
                    kvrpcpb::CheckTxnStatusResponse {
                        action: kvrpcpb::Action::LockNotExistRollback as i32,
                        ..Default::default()
                    }
                } else {
                    kvrpcpb::CheckTxnStatusResponse {
                        error: Some(kvrpcpb::KeyError {
                            txn_not_found: Some(kvrpcpb::TxnNotFound {
                                start_ts: request.lock_ts,
                                primary_key: request.primary_key.clone(),
                            }),
                            abort: if malformed {
                                "do not hide this error".into()
                            } else {
                                String::new()
                            },
                            ..Default::default()
                        }),
                        ..Default::default()
                    }
                };
                return Ok(Box::new(response) as Box<dyn Any>);
            }
            if let Some(request) = request.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                assert_eq!(request.start_version, 1);
                observed_resolutions
                    .lock()
                    .unwrap()
                    .push(request.commit_version);
                return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
            }
            panic!("unexpected request: {:?}", request.type_id());
        },
    ));
    let client = if enabled {
        client.with_txn_not_found_resolution()
    } else {
        client
    };
    (Arc::new(client), checks, resolutions)
}

fn lock(ttl: u64) -> kvrpcpb::LockInfo {
    kvrpcpb::LockInfo {
        key: vec![2],
        primary_lock: vec![1],
        lock_version: 1,
        lock_ttl: ttl,
        lock_type: kvrpcpb::Op::PessimisticLock as i32,
        ..Default::default()
    }
}

#[tokio::test]
async fn missing_primary_expired_lock_is_resolved_only_when_opted_in() {
    assert!(!crate::Config::default().resolve_txn_not_found);
    for enabled in [false, true] {
        let (client, checks, resolutions) = missing_primary_client(enabled, false);
        let result = resolve_locks(
            vec![lock(0)],
            Timestamp::default(),
            client,
            Keyspace::Disable,
        )
        .await;
        if enabled {
            assert!(result.unwrap().is_empty());
            assert_eq!(*checks.lock().unwrap(), [false, true]);
            assert_eq!(*resolutions.lock().unwrap(), [0]);
        } else {
            assert!(matches!(result, Err(Error::MultipleKeyErrors(_))));
            assert_eq!(*checks.lock().unwrap(), [false]);
            assert!(resolutions.lock().unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn missing_primary_live_pessimistic_lock_is_not_rolled_back() {
    let (client, checks, resolutions) = missing_primary_client(true, false);
    let original = lock(100);
    let live = resolve_locks(
        vec![original.clone()],
        Timestamp::default(),
        client,
        Keyspace::Disable,
    )
    .await
    .unwrap();
    assert_eq!(live, [original]);
    assert_eq!(*checks.lock().unwrap(), [false]);
    assert!(resolutions.lock().unwrap().is_empty());
}

#[tokio::test]
async fn missing_primary_mixed_error_is_not_reclassified() {
    let (client, checks, resolutions) = missing_primary_client(true, true);
    let error = resolve_locks(
        vec![lock(0)],
        Timestamp::default(),
        client,
        Keyspace::Disable,
    )
    .await
    .unwrap_err();
    assert!(matches!(error, Error::MultipleKeyErrors(_)));
    assert_eq!(*checks.lock().unwrap(), [false]);
    assert!(resolutions.lock().unwrap().is_empty());
}
