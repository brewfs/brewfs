//! `ossmount` — mount an S3-compatible bucket (Aliyun OSS, MinIO, ...) as a
//! local filesystem with **no local metadata database**.
//!
//! The bucket is the single source of truth: paths are encoded into object
//! keys, so any number of machines can mount the same bucket and see the same
//! tree. Consistency is weak (no locks / no atomic rename) — it is a "cloud
//! drive", not a multi-writer POSIX filesystem.
//!
//! Credentials come from the environment (`AWS_ACCESS_KEY_ID`,
//! `AWS_SECRET_ACCESS_KEY`), matching how the BrewFS tray app spawns mounts.
//!
//! Windows requires WinFsp 2.x. Linux/macOS builds only print a message.

use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use brewfs::ossfs::{ObjectFs, OssConfig};

fn usage() -> ! {
    eprintln!(
        "usage: ossmount --bucket BUCKET [--endpoint URL] [--region REGION]\n\
                 [--prefix PREFIX] [--force-path-style] MOUNT_POINT\n\
         env:  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY"
    );
    std::process::exit(2);
}

fn parse_args() -> (OssConfig, PathBuf) {
    let mut bucket = String::new();
    let mut endpoint: Option<String> = None;
    let mut region = "us-east-1".to_string();
    let mut prefix = String::new();
    let mut force_path_style = false;
    let mut mount_point: Option<PathBuf> = None;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--bucket" => bucket = args.next().unwrap_or_else(|| usage()),
            "--endpoint" => endpoint = Some(args.next().unwrap_or_else(|| usage())),
            "--region" => region = args.next().unwrap_or_else(|| usage()),
            "--prefix" => prefix = args.next().unwrap_or_else(|| usage()),
            "--force-path-style" => force_path_style = true,
            other if other.starts_with("--") => usage(),
            other => mount_point = Some(PathBuf::from(other)),
        }
    }
    let mount_point = mount_point.unwrap_or_else(|| usage());
    if bucket.is_empty() {
        usage();
    }
    (
        OssConfig {
            bucket,
            region,
            endpoint,
            force_path_style,
            prefix,
        },
        mount_point,
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    #[cfg(windows)]
    {
        let (cfg, mount_point) = parse_args();
        let fs = Arc::new(ObjectFs::connect(cfg).await?);
        brewfs::ossfs::winfsp::mount_oss_winfsp(fs, &mount_point).await
    }
    #[cfg(not(windows))]
    {
        eprintln!("ossmount requires Windows + WinFsp (this build is not Windows)");
        std::process::exit(2)
    }
}
