use super::*;
use crate::chunk::{ChunkLayout, store::InMemoryBlockStore};
use crate::vfs::{config::VFSConfig, fs::VFS};
use asyncfuse::SetAttr;
use asyncfuse::raw::{Filesystem, Request};
use std::ffi::OsStr;
use std::time::Duration;

#[tokio::test]
async fn writable_acl_fuse_replies_use_only_explicit_cache_ttl() {
    for configured in [None, Some(0), Some(7), Some(1000)] {
        let meta = Arc::new(test_meta().await);
        assert_eq!(
            meta.posix_acl_capability(),
            crate::meta::layer::PosixAclCapability::ReadWrite
        );
        let root = meta.root_ino() as u64;
        let fs = VFS::from_workspace_components(
            VFSConfig::new(ChunkLayout::default()),
            Arc::new(InMemoryBlockStore::new()),
            meta.clone(),
        )
        .unwrap();
        let cloned = fs.clone();
        if let Some(ms) = configured {
            fs.set_fuse_cache_ttl(Duration::from_millis(ms));
        }
        let ttl = Duration::from_millis(configured.unwrap_or(0));
        let req = Request {
            pid: std::process::id(),
            ..Request::default()
        };

        let virtual_stats = Filesystem::lookup(&cloned, req, root, OsStr::new(".stats"))
            .await
            .unwrap();
        assert_eq!(virtual_stats.ttl, Duration::ZERO);
        for name in ["file", ".stats"] {
            meta.create_file(root as i64, name.into()).await.unwrap();
            let lookup = Filesystem::lookup(&cloned, req, root, OsStr::new(name))
                .await
                .unwrap();
            assert_eq!(lookup.ttl, ttl, "LOOKUP {name}, {configured:?}");
            let ino = lookup.attr.ino;
            assert_eq!(
                Filesystem::getattr(&cloned, req, ino, None, 0)
                    .await
                    .unwrap()
                    .ttl,
                ttl
            );
            assert_eq!(
                Filesystem::setattr(&cloned, req, ino, None, SetAttr::default())
                    .await
                    .unwrap()
                    .ttl,
                ttl
            );
            assert_eq!(
                Filesystem::setattr(
                    &cloned,
                    req,
                    ino,
                    None,
                    SetAttr {
                        mode: Some(0o600),
                        ..SetAttr::default()
                    }
                )
                .await
                .unwrap()
                .ttl,
                ttl
            );
            assert_eq!(
                Filesystem::setattr(
                    &cloned,
                    req,
                    ino,
                    None,
                    SetAttr {
                        size: Some(0),
                        ..SetAttr::default()
                    }
                )
                .await
                .unwrap()
                .ttl,
                Duration::ZERO
            );
        }
        let created = Filesystem::create(
            &cloned,
            req,
            root,
            OsStr::new("created"),
            0o644,
            libc::O_RDWR as u32,
        )
        .await
        .unwrap();
        assert_eq!(created.ttl, Duration::ZERO);
        Filesystem::release(
            &cloned,
            req,
            created.attr.ino,
            created.fh,
            libc::O_RDWR as u32,
            0,
            false,
        )
        .await
        .unwrap();
    }
}
