//! Build and publish a deterministic packed-metadata read-only fixture.
//!
//! The fixture deliberately uses one shared immutable block for the small
//! files and a separately named set of blocks for the fio file.  This keeps
//! object PUT count small while exercising the real `chunks-v2` read path.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use clap::Parser;
use sha2::{Digest, Sha256};

use brewfs::cadapter::client::ObjectClient;
use brewfs::cadapter::s3::{S3Backend, S3Config};
use brewfs::chunk::Compression;
use brewfs::chunk::compress::encode_persisted_block;
use brewfs::native_base::frozen::producer::{FrozenSnapshotInput, build_snapshot, upload_snapshot};
use brewfs::native_base::frozen::{
    FrozenInodeRecord, FrozenRow, dentry_key, encode_extent_slice_value, extent_key, inode_key,
};
use brewfs::native_base::runtime::BackendObjectRepository;
use brewfs::native_base::wire::refs::ObjectId;
use brewfs::vfs::chunk_id_for;

#[derive(Debug, Parser)]
#[command(about = "publish a packed-metadata benchmark fixture")]
struct Args {
    #[arg(long, default_value = "brewfs-data")]
    bucket: String,
    #[arg(long, default_value = "http://127.0.0.1:19000")]
    endpoint: String,
    #[arg(long, default_value = "us-east-1")]
    region: String,
    #[arg(long, default_value = "packed-fixture")]
    prefix: String,
    #[arg(long, default_value_t = 8)]
    dirs: u64,
    /// Number of directory levels below the root. Zero preserves the legacy
    /// flat layout and uses `dirs` as the number of leaf directories.
    #[arg(long, default_value_t = 0)]
    dir_levels: u32,
    /// Number of child directories at every level in hierarchical mode.
    #[arg(long, default_value_t = 10)]
    dirs_per_level: u64,
    #[arg(long, default_value_t = 4500)]
    files_per_dir: u64,
    #[arg(long, default_value_t = 4096)]
    small_file_size: u64,
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    fio_file_size: u64,
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    chunk_size: u64,
    #[arg(long, default_value_t = 4 * 1024 * 1024)]
    block_size: u32,
    #[arg(long, default_value = "packed-manifest-key.txt")]
    manifest_output: PathBuf,
}

#[derive(Debug, Clone)]
struct LeafDirectory {
    inode: u64,
    components: Vec<String>,
}

fn attr(kind: u8, mode: u32, size: u64, parent_hint: Option<u64>, nlink: u64) -> Vec<u8> {
    FrozenInodeRecord {
        kind,
        mode,
        uid: 0,
        gid: 0,
        rdev: 0,
        nlink,
        size,
        atime_ns: 0,
        mtime_ns: 0,
        ctime_ns: 0,
        parent_hint,
        symlink_target: None,
    }
    .encode()
}

fn object_id(seed: &[u8], label: &str) -> ObjectId {
    let mut hasher = Sha256::new();
    hasher.update(seed);
    hasher.update(label.as_bytes());
    hasher.finalize()[..16].try_into().expect("sha256 prefix")
}

fn revision_id(seed: &[u8], label: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(seed);
    hasher.update(label.as_bytes());
    hasher.finalize().into()
}

fn row(map: &mut BTreeMap<Vec<u8>, Vec<u8>>, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
    if map.insert(key, value).is_some() {
        bail!("duplicate fixture row")
    }
    Ok(())
}

fn build_directory_tree(
    namespace: &mut BTreeMap<Vec<u8>, Vec<u8>>,
    next_inode: &mut u64,
    parent_inode: u64,
    level: u32,
    levels: u32,
    fanout: u64,
    components: &mut Vec<String>,
    leaves: &mut Vec<LeafDirectory>,
) -> Result<()> {
    if level == levels {
        leaves.push(LeafDirectory {
            inode: parent_inode,
            components: components.clone(),
        });
        return Ok(());
    }

    for child_index in 0..fanout {
        let inode = *next_inode;
        *next_inode = next_inode
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("fixture inode counter overflow"))?;
        let name = format!("d{child_index:03}");
        row(
            namespace,
            inode_key(inode),
            attr(2, 0o040755, 0, Some(parent_inode), 2),
        )?;
        row(
            namespace,
            dentry_key(parent_inode, name.as_bytes()),
            inode_key(inode),
        )?;
        components.push(name);
        build_directory_tree(
            namespace,
            next_inode,
            inode,
            level + 1,
            levels,
            fanout,
            components,
            leaves,
        )?;
        components.pop();
    }
    Ok(())
}

async fn put_block<B: brewfs::ObjectBackend>(
    client: &ObjectClient<B>,
    slice_id: u64,
    block_index: u32,
    data: &[u8],
) -> Result<()> {
    let key = format!("chunks-v2/{slice_id}/{block_index}");
    let framed = encode_persisted_block(data, Compression::None);
    client.put_object(&key, &framed).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.chunk_size < u64::from(args.block_size)
        || !args.fio_file_size.is_multiple_of(args.block_size as u64)
        || !args.fio_file_size.is_multiple_of(args.chunk_size)
    {
        bail!("fio-file-size must be a multiple of block-size and chunk-size must not be smaller");
    }
    if args.small_file_size == 0 || args.files_per_dir == 0 {
        bail!("files-per-dir and small-file-size must be non-zero");
    }
    let (dir_levels, dirs_per_level) = if args.dir_levels == 0 {
        if args.dirs == 0 {
            bail!("dirs must be non-zero in flat directory mode");
        }
        (1, args.dirs)
    } else {
        if args.dirs_per_level == 0 {
            bail!("dirs-per-level must be non-zero in hierarchical mode");
        }
        (args.dir_levels, args.dirs_per_level)
    };
    if dir_levels > 32 {
        bail!("dir-levels must be at most 32");
    }
    if args.small_file_size > u64::from(args.block_size) || args.small_file_size > 4 * 1024 * 1024 {
        bail!("small-file-size must fit in one block and be at most 4 MiB");
    }

    let volume_id = object_id(args.prefix.as_bytes(), "volume");
    let storage_namespace_id = object_id(args.prefix.as_bytes(), "storage");
    let slice_base = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before unix epoch")?
        .as_secs()
        .saturating_mul(100_000);
    let small_slice_id = slice_base + 1;
    let seed = vec![0x5a_u8; 4 * 1024 * 1024];

    let mut namespace = BTreeMap::new();
    row(&mut namespace, inode_key(1), attr(2, 0o040755, 0, None, 2))?;
    let mut next_inode = 2_u64;
    let mut leaf_dirs = Vec::new();
    build_directory_tree(
        &mut namespace,
        &mut next_inode,
        1,
        0,
        dir_levels,
        dirs_per_level,
        &mut Vec::new(),
        &mut leaf_dirs,
    )?;
    if leaf_dirs.is_empty() {
        bail!("directory tree produced no leaf directories");
    }
    let corpus_directory_count = next_inode - 2;
    let bench_dir = next_inode;
    next_inode += 1;
    row(
        &mut namespace,
        inode_key(bench_dir),
        attr(2, 0o040755, 0, Some(1), 2),
    )?;
    row(
        &mut namespace,
        dentry_key(1, b"bench"),
        inode_key(bench_dir),
    )?;

    let verify_dir = next_inode;
    next_inode += 1;
    row(
        &mut namespace,
        inode_key(verify_dir),
        attr(2, 0o040755, 0, Some(1), 2),
    )?;
    row(
        &mut namespace,
        dentry_key(1, b"verify"),
        inode_key(verify_dir),
    )?;

    let mut data = BTreeMap::new();
    let small_chunk = args.small_file_size.div_ceil(args.chunk_size);
    if small_chunk != 1 {
        bail!("small-file-size must fit in one chunk for this fixture")
    }
    let mut file_count = 1_u64; // the fio file below
    let mut total_logical_bytes = args.fio_file_size;
    let first_small_inode = next_inode;
    for (dir_index, leaf) in leaf_dirs.iter().enumerate() {
        let dir_inode = leaf.inode;
        for file_index in 0..args.files_per_dir {
            let inode = next_inode;
            next_inode += 1;
            let name = format!("f{file_index:05}");
            let nlink = if inode == first_small_inode { 2 } else { 1 };
            row(
                &mut namespace,
                inode_key(inode),
                attr(1, 0o100644, args.small_file_size, Some(dir_inode), nlink),
            )?;
            row(
                &mut namespace,
                dentry_key(dir_inode, name.as_bytes()),
                inode_key(inode),
            )?;
            let chunk_id = chunk_id_for(inode as i64, 0)?;
            row(
                &mut data,
                extent_key(inode, 0, 0),
                encode_extent_slice_value(args.small_file_size, small_slice_id, chunk_id, 0),
            )?;
            file_count += 1;
            total_logical_bytes += args.small_file_size;
        }
        eprintln!(
            "prepared leaf directory {} ({}/{})",
            dir_index,
            dir_index + 1,
            leaf_dirs.len()
        );
    }

    // Keep a compact set of POSIX shape checks outside the throughput scan.
    // The hardlink points at the first small file; all other special inodes
    // are metadata-only and therefore need no data extent.
    row(
        &mut namespace,
        dentry_key(verify_dir, b"hardlink"),
        inode_key(first_small_inode),
    )?;
    let symlink_inode = next_inode;
    next_inode += 1;
    let first_leaf = &leaf_dirs[0].components;
    let symlink_target = format!("../{}/f00000", first_leaf.join("/")).into_bytes();
    row(
        &mut namespace,
        inode_key(symlink_inode),
        FrozenInodeRecord {
            kind: 3,
            mode: 0o120777,
            uid: 0,
            gid: 0,
            rdev: 0,
            nlink: 1,
            size: symlink_target.len() as u64,
            atime_ns: 0,
            mtime_ns: 0,
            ctime_ns: 0,
            parent_hint: Some(verify_dir),
            symlink_target: Some(symlink_target.clone()),
        }
        .encode(),
    )?;
    row(
        &mut namespace,
        dentry_key(verify_dir, b"symlink"),
        inode_key(symlink_inode),
    )?;

    for (name, kind, mode, rdev) in [
        (b"fifo".as_slice(), 4_u8, 0o010644_u32, 0_u64),
        (b"socket".as_slice(), 5_u8, 0o140755_u32, 0_u64),
        (b"char".as_slice(), 6_u8, 0o020600_u32, 0x1234_u64),
        (b"block".as_slice(), 7_u8, 0o060600_u32, 0x5678_u64),
    ] {
        let inode = next_inode;
        next_inode += 1;
        row(
            &mut namespace,
            inode_key(inode),
            FrozenInodeRecord {
                kind,
                mode,
                uid: 0,
                gid: 0,
                rdev,
                nlink: 1,
                size: 0,
                atime_ns: 0,
                mtime_ns: 0,
                ctime_ns: 0,
                parent_hint: Some(verify_dir),
                symlink_target: None,
            }
            .encode(),
        )?;
        row(
            &mut namespace,
            dentry_key(verify_dir, name),
            inode_key(inode),
        )?;
    }

    let bench_inode = next_inode;
    let bench_name = b"read.bin";
    row(
        &mut namespace,
        inode_key(bench_inode),
        attr(1, 0o100644, args.fio_file_size, Some(bench_dir), 1),
    )?;
    row(
        &mut namespace,
        dentry_key(bench_dir, bench_name),
        inode_key(bench_inode),
    )?;
    let block_count = args.fio_file_size / u64::from(args.block_size);
    for block_index in 0..block_count {
        let offset = block_index * u64::from(args.block_size);
        let chunk_index = offset / args.chunk_size;
        let chunk_offset = offset % args.chunk_size;
        let chunk_id = chunk_id_for(bench_inode as i64, chunk_index)?;
        let slice_id = slice_base + 100 + block_index;
        row(
            &mut data,
            extent_key(bench_inode, chunk_index, chunk_offset),
            encode_extent_slice_value(u64::from(args.block_size), slice_id, chunk_id, chunk_offset),
        )?;
    }

    let inventory = vec![FrozenRow {
        key: b"fixture-version".to_vec(),
        value: b"packed-metadata-v1".to_vec(),
    }];
    let namespace_rows = namespace
        .into_iter()
        .map(|(key, value)| FrozenRow { key, value })
        .collect::<Vec<_>>();
    let data_rows = data
        .into_iter()
        .map(|(key, value)| FrozenRow { key, value })
        .collect::<Vec<_>>();
    file_count += 5; // symlink + fifo/socket/char/block; the hardlink is not a new inode.
    total_logical_bytes += symlink_target.len() as u64;
    let input = FrozenSnapshotInput {
        volume_id,
        storage_namespace_id,
        chunk_size: args.chunk_size,
        block_size: args.block_size,
        required_features: 0,
        logical_revision: revision_id(args.prefix.as_bytes(), "revision"),
        namespace_rows,
        data_rows,
        inventory_rows: inventory,
        file_count,
        directory_count: corpus_directory_count + 3,
        total_logical_bytes,
        created_at_ns: 0,
        namespace_object_id: object_id(args.prefix.as_bytes(), "namespace"),
        data_object_id: object_id(args.prefix.as_bytes(), "data"),
        inventory_object_id: object_id(args.prefix.as_bytes(), "inventory"),
        manifest_object_id: object_id(args.prefix.as_bytes(), "manifest"),
    };
    let snapshot = build_snapshot(input).map_err(|error| anyhow::anyhow!(error))?;

    let config = S3Config {
        bucket: args.bucket.clone(),
        region: Some(args.region.clone()),
        endpoint: Some(args.endpoint.clone()),
        force_path_style: true,
        part_size: 16 * 1024 * 1024,
        max_concurrency: 16,
        disable_payload_checksum: true,
        ..Default::default()
    };
    let backend = S3Backend::with_config(config).await?;
    let client = ObjectClient::new(backend.clone());
    put_block(
        &client,
        small_slice_id,
        0,
        &seed[..args.small_file_size as usize],
    )
    .await?;
    for block_index in 0..block_count {
        let mut block = vec![0_u8; args.block_size as usize];
        for (index, byte) in block.iter_mut().enumerate() {
            *byte = (index as u64 + block_index) as u8;
        }
        put_block(&client, slice_base + 100 + block_index, 0, &block).await?;
    }
    let repository = BackendObjectRepository::new(client);
    let manifest = upload_snapshot(&repository, &snapshot).await?;
    let manifest_key = String::from_utf8(manifest.key.clone()).context("manifest key is utf8")?;
    std::fs::write(&args.manifest_output, format!("{manifest_key}\n"))?;
    println!("manifest_key={manifest_key}");
    println!(
        "files={file_count} directories={} leaf_directories={} dir_levels={} dirs_per_level={} logical_bytes={total_logical_bytes}",
        corpus_directory_count + 3,
        leaf_dirs.len(),
        dir_levels,
        dirs_per_level
    );
    println!("first_leaf_path={}/", first_leaf.join("/"));
    println!("small_slice_id={small_slice_id} fio_blocks={block_count}");
    Ok(())
}
