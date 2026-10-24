#![cfg(feature = "gateway-s3")]

use std::sync::Arc;

use crate::chunk::layout::ChunkLayout;
use crate::chunk::store::InMemoryBlockStore;
use crate::gateway::s3::path::BucketMode;
use crate::gateway::s3::{BrewFsS3, S3Options};
use crate::meta::MetaStore;
use crate::meta::client::{MetaClient, MetaClientOptions};
use crate::meta::config::{CompactConfig, MetaClientConfig};
use crate::meta::factory::create_meta_store_from_url;
use crate::vfs::cache::config::CacheConfig;
use crate::vfs::fs::VFS;
use bytes::Bytes;
use futures::StreamExt;
use s3s::dto::*;
use s3s::{S3, S3ErrorCode, S3Request, S3Result};

fn request<T>(input: T) -> S3Request<T> {
    S3Request {
        input,
        method: Default::default(),
        uri: Default::default(),
        headers: Default::default(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

async fn gateway() -> BrewFsS3<InMemoryBlockStore> {
    let handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
    let store: Arc<dyn MetaStore> = handle.store();
    let config = MetaClientConfig::default();
    let meta = MetaClient::with_options(
        store,
        config.capacity.clone(),
        config.effective_ttl(),
        MetaClientOptions::default(),
    );
    let vfs = VFS::with_meta_layer_with_cache_config(
        ChunkLayout::default(),
        Arc::new(InMemoryBlockStore::new()),
        meta,
        CompactConfig::default(),
        CacheConfig::default(),
    )
    .unwrap();
    vfs.mkdir_p("/.brewfs.sys/s3/tmp").await.unwrap();
    BrewFsS3::new(
        vfs,
        S3Options {
            bucket_mode: BucketMode::Single {
                bucket: "bucket".into(),
            },
            hide_dir_objects: false,
        },
    )
}

fn put(bytes: &'static [u8]) -> PutObjectInput {
    PutObjectInput {
        bucket: "bucket".into(),
        key: "key".into(),
        body: Some(StreamingBlob::from(Bytes::from_static(bytes))),
        ..Default::default()
    }
}

fn unsupported<T>(result: S3Result<T>) {
    let error = match result {
        Ok(_) => panic!("conditional operation incorrectly succeeded"),
        Err(error) => error,
    };
    assert_eq!(error.code(), &S3ErrorCode::NotImplemented);
}

#[tokio::test]
async fn conditional_delete_preserves_object() {
    let s3 = gateway().await;
    s3.put_object(request(put(b"new"))).await.unwrap();
    for field in 0..3 {
        let mut input = DeleteObjectInput {
            bucket: "bucket".into(),
            key: "key".into(),
            ..Default::default()
        };
        match field {
            0 => input.if_match = Some(ETagCondition::Any),
            1 => input.if_match_last_modified_time = Some(Timestamp::from(std::time::UNIX_EPOCH)),
            _ => input.if_match_size = Some(0),
        }
        unsupported(s3.delete_object(request(input)).await);
        assert!(s3.vfs().exists("/key").await);
    }
}

#[tokio::test]
async fn conditional_multipart_operations_are_rejected_before_upload_lookup() {
    let s3 = gateway().await;
    for field in 0..2 {
        let mut builder = CompleteMultipartUploadInput::builder();
        builder
            .set_bucket("bucket".into())
            .set_key("key".into())
            .set_upload_id("missing".into());
        let mut input = builder.build().unwrap();
        if field == 0 {
            input.if_match = Some(ETagCondition::Any);
        } else {
            input.if_none_match = Some(ETagCondition::Any);
        }
        unsupported(s3.complete_multipart_upload(request(input)).await);
    }
    let mut builder = AbortMultipartUploadInput::builder();
    builder
        .set_bucket("bucket".into())
        .set_key("key".into())
        .set_upload_id("missing".into());
    let mut input = builder.build().unwrap();
    input.if_match_initiated_time = Some(Timestamp::from(std::time::UNIX_EPOCH));
    unsupported(s3.abort_multipart_upload(request(input)).await);
}

#[tokio::test]
async fn conditional_put_preserves_newer_bytes() {
    let s3 = gateway().await;
    let old = s3
        .put_object(request(put(b"old")))
        .await
        .unwrap()
        .output
        .e_tag
        .unwrap();
    s3.put_object(request(put(b"new"))).await.unwrap();
    let mut conditional = put(b"stale replacement");
    conditional.if_match = Some(ETagCondition::ETag(old));
    unsupported(s3.put_object(request(conditional)).await);
    let mut conditional = put(b"create-only replacement");
    conditional.if_none_match = Some(ETagCondition::Any);
    unsupported(s3.put_object(request(conditional)).await);
    let mut body = s3
        .get_object(request(GetObjectInput {
            bucket: "bucket".into(),
            key: "key".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .body
        .unwrap();
    let mut bytes = Vec::new();
    while let Some(chunk) = body.next().await {
        bytes.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(bytes, b"new");
}

#[tokio::test]
async fn conditional_get_and_head_are_explicitly_rejected() {
    let s3 = gateway().await;
    s3.put_object(request(put(b"new"))).await.unwrap();
    for field in 0..4 {
        let mut get = GetObjectInput {
            bucket: "bucket".into(),
            key: "key".into(),
            ..Default::default()
        };
        let mut head = HeadObjectInput {
            bucket: "bucket".into(),
            key: "key".into(),
            ..Default::default()
        };
        match field {
            0 => {
                get.if_match = Some(ETagCondition::ETag(ETag::Strong("stale".into())));
                head.if_match = get.if_match.clone();
            }
            1 => {
                get.if_none_match = Some(ETagCondition::Any);
                head.if_none_match = get.if_none_match.clone();
            }
            2 => {
                get.if_modified_since = Some(Timestamp::from(std::time::SystemTime::now()));
                head.if_modified_since = get.if_modified_since.clone();
            }
            _ => {
                get.if_unmodified_since = Some(Timestamp::from(std::time::UNIX_EPOCH));
                head.if_unmodified_since = get.if_unmodified_since.clone();
            }
        }
        unsupported(s3.get_object(request(get)).await);
        unsupported(s3.head_object(request(head)).await);
    }
}

#[tokio::test]
async fn conditional_copy_never_publishes_destination() {
    let s3 = gateway().await;
    s3.put_object(request(put(b"new"))).await.unwrap();
    for field in 0..4 {
        let mut builder = CopyObjectInput::builder();
        builder
            .set_bucket("bucket".into())
            .set_key("copy".into())
            .set_copy_source(CopySource::Bucket {
                bucket: "bucket".into(),
                key: "key".into(),
                version_id: None,
            });
        let mut input = builder.build().unwrap();
        match field {
            0 => {
                input.copy_source_if_match = Some(ETagCondition::ETag(ETag::Strong("stale".into())))
            }
            1 => input.copy_source_if_none_match = Some(ETagCondition::Any),
            2 => {
                input.copy_source_if_modified_since =
                    Some(Timestamp::from(std::time::SystemTime::now()))
            }
            _ => {
                input.copy_source_if_unmodified_since = Some(Timestamp::from(std::time::UNIX_EPOCH))
            }
        }
        unsupported(s3.copy_object(request(input)).await);
        assert!(!s3.vfs().exists("/copy").await);
    }
}
