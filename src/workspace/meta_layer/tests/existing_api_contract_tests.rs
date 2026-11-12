//! Behavior contract expressed entirely through green03's existing MetaLayer API.
use super::*;
use crate::meta::posix_acl::PosixAcl;

#[tokio::test]
async fn g04_existing_api_default_acl_inheritance_and_chmod_stay_in_sync() {
    let meta = test_meta().await;
    let parent = meta.mkdir(1, "default-acl-parent".into()).await.unwrap();
    let mut default_acl = 2u32.to_le_bytes().to_vec();
    for (tag, permissions, id) in [
        (1u16, 7u16, u32::MAX),
        (2, 4, 1234),
        (4, 5, u32::MAX),
        (16, 5, u32::MAX),
        (32, 0, u32::MAX),
    ] {
        default_acl.extend_from_slice(&tag.to_le_bytes());
        default_acl.extend_from_slice(&permissions.to_le_bytes());
        default_acl.extend_from_slice(&id.to_le_bytes());
    }
    PosixAcl::decode(&default_acl).unwrap();
    meta.set_xattr(parent, "system.posix_acl_default", &default_acl, 0)
        .await
        .unwrap();
    let child = meta.create_file(parent, "inherited".into()).await.unwrap();
    let inherited = meta
        .get_xattr(child, "system.posix_acl_access")
        .await
        .unwrap()
        .expect("created child did not inherit the parent's valid default ACL");
    let acl = PosixAcl::decode(&inherited).unwrap();
    let attr = meta.stat(child).await.unwrap().unwrap();
    assert_eq!(attr.mode & 0o777, 0o640);
    assert_eq!(acl.mode_bits(), attr.mode & 0o777);
    assert_eq!(acl.user_access_mode(attr.uid, 1234), Some(4));

    let changed = meta
        .set_attr(
            child,
            &SetAttrRequest {
                mode: Some(0o600),
                ..Default::default()
            },
            SetAttrFlags::empty(),
        )
        .await
        .unwrap();
    let after = meta
        .get_xattr(child, "system.posix_acl_access")
        .await
        .unwrap()
        .expect("chmod silently removed an extended ACL");
    assert_eq!(PosixAcl::decode(&after).unwrap().mode_bits(), 0o600);
    assert_eq!(changed.mode & 0o777, 0o600);
    assert_eq!(
        meta.get_xattr(parent, "system.posix_acl_default")
            .await
            .unwrap(),
        Some(default_acl),
        "child chmod changed the parent's inheritance policy"
    );
}
