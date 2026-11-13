//! Actual ACL authentication and trusted-factory admission, not a Redis
//! business-transition ACL test. Both fixture users are temporary and scoped.

use super::*;
use futures::FutureExt;

fn credential_url(base: &str, username: &str, password: &str) -> String {
    let mut url = url::Url::parse(base).unwrap();
    url.set_username(username).unwrap();
    url.set_password(Some(password)).unwrap();
    url.into()
}

#[tokio::test]
#[ignore = "requires BREWFS_TEST_REDIS_URL with ACL SETUSER/DELUSER permission"]
async fn real_redis_operator_admin_requires_two_actual_distinct_acl_identities() {
    let root_url = std::env::var("BREWFS_TEST_REDIS_URL").unwrap();
    let mut control = ConnectionManager::new(redis::Client::open(root_url.as_str()).unwrap())
        .await
        .unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let namespace = format!("operator-auth-{suffix}");
    let rejected_namespace = format!("operator-rejected-{suffix}");
    let admin_name = format!("brewfs_operator_{suffix}");
    let runtime_name = format!("brewfs_runtime_{suffix}");
    let admin_password = uuid::Uuid::new_v4().simple().to_string();
    let runtime_password = uuid::Uuid::new_v4().simple().to_string();
    let admin_url = credential_url(&root_url, &admin_name, &admin_password);
    let runtime_url = credential_url(&root_url, &runtime_name, &runtime_password);
    let prefix = format!("{{brewfs-ws-v1}}:{namespace}:ws:v1/");
    let rejected_prefix = format!("{{brewfs-ws-v1}}:{rejected_namespace}:ws:v1/");

    // Establish ownership before the first mutation. Never delete a preexisting
    // ACL principal, even in the unlikely event of a random fixture collision.
    for name in [&admin_name, &runtime_name] {
        let existing: Option<redis::Value> = redis::cmd("ACL")
            .arg("GETUSER")
            .arg(name)
            .query_async(&mut control)
            .await
            .unwrap();
        assert!(
            existing.is_none(),
            "random ACL fixture identity already exists"
        );
    }

    let result = std::panic::AssertUnwindSafe(async {
        for (name, password) in [
            (&admin_name, &admin_password),
            (&runtime_name, &runtime_password),
        ] {
            let status: String = redis::cmd("ACL")
                .arg("SETUSER")
                .arg(name)
                .arg("reset")
                .arg("on")
                .arg(format!(">{password}"))
                .arg(format!("~{prefix}*"))
                .arg("+@all")
                .query_async(&mut control)
                .await
                .unwrap();
            assert_eq!(status, "OK");
        }

        let runtime = RedisWorkspaceBackend::connect(&runtime_url, &namespace)
            .await
            .unwrap();
        assert!(matches!(
            runtime.authenticate_gc_admin().await,
            Err(WorkspaceError::UnsupportedCapability(_))
        ));
        let admin = RedisWorkspaceBackend::connect_operator_admin(
            &admin_url,
            &admin_name,
            &runtime_url,
            &runtime_name,
            &namespace,
        )
        .await
        .unwrap();
        admin.authenticate_gc_admin().await.unwrap();

        assert!(
            RedisWorkspaceBackend::connect_operator_admin(
                &runtime_url,
                &runtime_name,
                &runtime_url,
                &runtime_name,
                &rejected_namespace,
            )
            .await
            .is_err()
        );
        assert!(
            RedisWorkspaceBackend::connect_operator_admin(
                &admin_url,
                "wrong-expected-principal",
                &runtime_url,
                &runtime_name,
                &rejected_namespace,
            )
            .await
            .is_err()
        );
        let bad_password_url = credential_url(&root_url, &admin_name, "wrong-fixture-password");
        assert!(
            RedisWorkspaceBackend::connect_operator_admin(
                &bad_password_url,
                &admin_name,
                &runtime_url,
                &runtime_name,
                &rejected_namespace,
            )
            .await
            .is_err()
        );
        let shared_password_url = credential_url(&root_url, &runtime_name, &admin_password);
        assert!(
            RedisWorkspaceBackend::connect_operator_admin(
                &admin_url,
                &admin_name,
                &shared_password_url,
                &runtime_name,
                &rejected_namespace,
            )
            .await
            .is_err()
        );

        // All rejected identities must fail before the key-index bootstrap.
        let rejected: usize = redis::cmd("EXISTS")
            .arg(format!(
                "{rejected_prefix}{}",
                String::from_utf8_lossy(KEY_INDEX)
            ))
            .arg(format!(
                "{rejected_prefix}{}",
                String::from_utf8_lossy(KEY_INDEX_READY)
            ))
            .query_async(&mut control)
            .await
            .unwrap();
        assert_eq!(rejected, 0);

        // Removing the actual server principal revokes an already-constructed
        // admin handle. A retained private marker cannot stand in for auth.
        let removed: usize = redis::cmd("ACL")
            .arg("DELUSER")
            .arg(&admin_name)
            .query_async(&mut control)
            .await
            .unwrap();
        assert_eq!(removed, 1);
        assert!(admin.authenticate_gc_admin().await.is_err());
        drop(admin);
        drop(runtime);
    })
    .catch_unwind()
    .await;

    // Cleanup runs after a failed assertion as well. Verify actual server
    // absence independently rather than assuming successful DEL is sufficient.
    let mut exact_keys = Vec::new();
    for fixture_prefix in [&prefix, &rejected_prefix] {
        for key in [KEY_INDEX, KEY_INDEX_READY] {
            let mut scoped = fixture_prefix.as_bytes().to_vec();
            scoped.extend_from_slice(key);
            exact_keys.push(scoped);
        }
    }
    let deleted_keys = redis::cmd("DEL")
        .arg(&exact_keys)
        .query_async::<usize>(&mut control)
        .await;
    let deleted_users = redis::cmd("ACL")
        .arg("DELUSER")
        .arg(&admin_name)
        .arg(&runtime_name)
        .query_async::<usize>(&mut control)
        .await;
    let remaining_keys = redis::cmd("EXISTS")
        .arg(&exact_keys)
        .query_async::<usize>(&mut control)
        .await;
    let mut principals_absent = true;
    for name in [&admin_name, &runtime_name] {
        let principal = redis::cmd("ACL")
            .arg("GETUSER")
            .arg(name)
            .query_async::<Option<redis::Value>>(&mut control)
            .await;
        principals_absent &= matches!(principal, Ok(None));
    }
    assert!(
        deleted_keys.is_ok() && deleted_users.is_ok(),
        "Redis auth fixture cleanup failed"
    );
    assert!(
        matches!(remaining_keys, Ok(0)) && principals_absent,
        "Redis auth fixture independent absence check failed"
    );
    assert!(
        result.is_ok(),
        "Redis independent admin authentication case failed"
    );
}
