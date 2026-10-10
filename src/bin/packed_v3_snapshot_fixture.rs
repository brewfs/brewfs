//! Build a local packed-metadata v3 snapshot for cold-read tests.
//!
//! The fixture shards large directories into bounded immutable groups and
//! publishes pageable group/inode indexes, matching the read path used by
//! large snapshots.

use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result, bail};
use clap::Parser;
use sha2::{Digest, Sha256};
use tokio::{sync::Semaphore, task::JoinSet};

use brewfs::cadapter::client::{ObjectBackend, ObjectClient};
use brewfs::cadapter::localfs::LocalFsBackend;
use brewfs::cadapter::s3::{S3Backend, S3Config};
use brewfs::workspace_overlay::packed_v3::{
    AccessProfile, GroupMeta, PackedFileInput, PackedGroupInput, SizeClassTable, directory_key,
};

#[derive(Debug, Parser)]
#[command(about = "build a local packed-metadata v3 fixture")]
struct Args {
    #[arg(long, default_value = "packed-v3-objects")]
    output_dir: PathBuf,
    /// Capture one Linux regular-file dentry, preserving SEEK_DATA/SEEK_HOLE
    /// placement and cold xattrs. Visible nlink is one; directory/root inventory
    /// and atomic source-snapshot guarantees are separate production work.
    #[arg(long, conflicts_with = "source_directory")]
    source_file: Option<PathBuf>,
    /// Inventory a complete Linux directory with disk-backed raw-name sorting.
    #[arg(long, conflicts_with = "source_file")]
    source_directory: Option<PathBuf>,
    /// Select stat detection, or a verified existing readonly Btrfs snapshot.
    #[arg(long, value_parser = ["best-effort-detected", "snapshot-backed"], requires = "source_directory")]
    source_consistency: Option<String>,
    /// Preserve snapshot-visible nlink, or refuse any alias outside this tree.
    #[arg(long, value_parser = ["visible-links", "reject-external"], requires = "source_directory")]
    source_hardlink_policy: Option<String>,
    /// Current v3 contract: wire 005 with PM11/IP06 counted directory routing.
    #[arg(long,hide=true,default_value_t=5,value_parser=clap::value_parser!(u8).range(5..=5))]
    wire_version: u8,
    /// Packed-v3 metadata codec; omitted preserves zstd.
    #[arg(long,value_parser=["raw","zstd"])]
    metadata_codec: Option<String>,
    /// Packed-v3 data codec; omitted preserves zstd.
    #[arg(long,value_parser=["raw","zstd"])]
    data_codec: Option<String>,
    /// Fixed targets or deterministic size-only selection, applied at build time.
    #[arg(long, default_value = "size-only", value_parser = ["size-only", "static-256kib", "static-1mib", "static-4mib", "p90-training"])]
    frame_policy: String,
    /// Authenticated offline p90 policy JSON. Required by p90-training.
    #[arg(long, requires = "frame_policy")]
    p90_policy: Option<PathBuf>,
    /// Off keeps every nonempty payload out of metadata, including tiny files.
    #[arg(long, default_value = "on", value_parser = ["on", "off"])]
    inline_data: String,
    #[arg(long,default_value_t=false,action=clap::ArgAction::Set)]
    cold_corpus: bool,
    #[arg(long,default_value_t=false,action=clap::ArgAction::Set)]
    hardlink_corpus: bool,

    #[arg(long, default_value_t = 2)]
    dir_levels: u32,
    #[arg(long, default_value_t = 10)]
    dirs_per_level: u64,
    #[arg(long, default_value_t = 1000)]
    files_per_dir: u64,
    #[arg(long, default_value_t = 102_400)]
    small_file_size: u64,
    /// Optional deterministic size range. When omitted, `small_file_size`
    /// keeps the historical fixed-size fixture behavior.
    #[arg(long)]
    small_file_min_size: Option<u64>,
    #[arg(long)]
    small_file_max_size: Option<u64>,
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    chunk_size: u64,
    #[arg(long, default_value_t = 4 * 1024 * 1024)]
    block_size: u32,
    #[arg(long, default_value = "packed-v3-manifest-key.txt")]
    manifest_output: PathBuf,
    #[arg(long)]
    bucket: Option<String>,
    #[arg(long)]
    endpoint: Option<String>,
    #[arg(long)]
    region: Option<String>,
    #[arg(long, default_value = "")]
    prefix: String,
    /// Packing profile for the immutable payload layout. Keep the default
    /// random profile for compatibility; sequential scans should opt into
    /// `sequential-small-file` so containers are grouped for read-ahead.
    #[arg(long, default_value = "random-small-file")]
    access_profile: String,
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    force_path_style: bool,
    /// Upload the deterministic file tree as individual raw objects instead
    /// of building packed metadata. This is the JuiceFS comparison source.
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    raw_only: bool,
}

#[derive(Clone)]
struct Directory {
    inode: u64,
    parent_inode: u64,
    name: Vec<u8>,
}

fn fill_pattern(size: usize, file_index: u64) -> Vec<u8> {
    let mut data = vec![0u8; size];
    let seed = file_index.wrapping_add(1).to_le_bytes();
    for (index, byte) in data.iter_mut().enumerate() {
        *byte = seed[index % seed.len()] ^ (index as u64).rotate_left(7) as u8;
    }
    data
}

fn file_size_for_index(file_index: u64, min_size: u64, max_size: u64) -> usize {
    let span = max_size.saturating_sub(min_size).saturating_add(1);
    let mixed = file_index
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    min_size.saturating_add(if span == 0 { 0 } else { mixed % span }) as usize
}

fn file_size_range(args: &Args) -> Result<(u64, u64)> {
    let min_size = args.small_file_min_size.unwrap_or(args.small_file_size);
    let max_size = args.small_file_max_size.unwrap_or(args.small_file_size);
    if min_size == 0 || min_size > max_size || max_size > 4 * 1024 * 1024 {
        bail!("small file size range must satisfy 0 < min <= max <= 4 MiB");
    }
    Ok((min_size, max_size))
}

fn fixture_codec(value: Option<&str>) -> Result<brewfs::workspace_overlay::packed_v3::PackedCodec> {
    use brewfs::workspace_overlay::packed_v3::PackedCodec;
    match value.unwrap_or("zstd") {
        "raw" => Ok(PackedCodec::Raw),
        "zstd" => Ok(PackedCodec::Zstd),
        _ => bail!("unsupported fixture codec"),
    }
}

fn fixture_build_policy(
    args: &Args,
) -> Result<brewfs::workspace_overlay::packed_v3::wire005::V3BuildPolicy> {
    use brewfs::workspace_overlay::packed_v3::wire005::{V3BuildPolicy, V3FramePolicy};
    let frames = match args.frame_policy.as_str() {
        "size-only" => V3FramePolicy::SizeOnly,
        "static-256kib" => V3FramePolicy::Static256Kib,
        "static-1mib" => V3FramePolicy::Static1Mib,
        "static-4mib" => V3FramePolicy::Static4Mib,
        "p90-training" => V3FramePolicy::P90Training,
        _ => bail!("unsupported frame policy"),
    };
    let p90 = match frames {
        V3FramePolicy::P90Training => {
            Some(read_p90_policy(args.p90_policy.as_deref().ok_or_else(
                || anyhow::anyhow!("p90-training requires --p90-policy"),
            )?)?)
        }
        _ => {
            if args.p90_policy.is_some() {
                bail!("--p90-policy requires --frame-policy p90-training");
            }
            None
        }
    };
    Ok(V3BuildPolicy {
        frames,
        inline_data: args.inline_data == "on",
        p90,
    })
}

fn read_p90_policy(
    path: &std::path::Path,
) -> Result<brewfs::workspace_overlay::packed_v3::wire005::V3P90Policy> {
    const MAX_P90_SAMPLES: u64 = 10_000_000;
    const MAX_REQUESTED_RANGE: u64 = 1 << 63;
    use brewfs::workspace_overlay::packed_v3::wire005::V3P90Policy;
    let value: serde_json::Value = serde_json::from_slice(
        &std::fs::read(path).with_context(|| format!("read p90 policy {}", path.display()))?,
    )?;
    let object = value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("p90 policy must be a JSON object"))?;
    if object.get("schema").and_then(serde_json::Value::as_str) != Some("packed-v3-p90-policy-v1") {
        bail!("unsupported p90 policy schema");
    }
    let hex_digest = |name: &str| -> Result<[u8; 32]> {
        let text = object
            .get(name)
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("p90 policy missing {name}"))?;
        let bytes = hex::decode(text).with_context(|| format!("decode p90 {name}"))?;
        bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("p90 {name} must be a SHA-256 digest"))
    };
    let trace_digest = hex_digest("trace_sha256")?;
    let policy_digest = hex_digest("policy_sha256")?;
    let mut policy_payload = object.clone();
    policy_payload.remove("policy_sha256");
    let calculated_policy: [u8; 32] = Sha256::digest(serde_json::to_vec(&policy_payload)?).into();
    if calculated_policy != policy_digest {
        bail!("p90 policy digest does not match its contents");
    }
    for field in ["source", "captured_at_utc"] {
        let text = object
            .get(field)
            .and_then(serde_json::Value::as_str)
            .filter(|text| !text.trim().is_empty() && !text.chars().any(char::is_whitespace))
            .ok_or_else(|| anyhow::anyhow!("p90 policy {field} is malformed"))?;
        let _ = text;
    }
    let sample_count = object
        .get("sample_count")
        .and_then(serde_json::Value::as_u64)
        .filter(|count| *count > 0 && *count <= MAX_P90_SAMPLES)
        .ok_or_else(|| anyhow::anyhow!("p90 sample_count is invalid"))?;
    let histogram = object
        .get("histogram")
        .and_then(serde_json::Value::as_array)
        .filter(|entries| !entries.is_empty())
        .ok_or_else(|| anyhow::anyhow!("p90 histogram is empty"))?;
    let mut total = 0u64;
    let mut previous = 0u64;
    let rank = sample_count
        .checked_mul(90)
        .and_then(|value| value.checked_add(99))
        .map(|value| value / 100)
        .ok_or_else(|| anyhow::anyhow!("p90 sample_count rank overflows"))?;
    let mut p90_from_histogram = None;
    for entry in histogram {
        let item = entry
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("p90 histogram entry is malformed"))?;
        let range = item
            .get("range_bytes")
            .and_then(serde_json::Value::as_u64)
            .filter(|range| *range > previous && *range <= MAX_REQUESTED_RANGE)
            .ok_or_else(|| anyhow::anyhow!("p90 histogram is not strictly ordered"))?;
        let count = item
            .get("count")
            .and_then(serde_json::Value::as_u64)
            .filter(|count| *count > 0)
            .ok_or_else(|| anyhow::anyhow!("p90 histogram count is invalid"))?;
        total = total
            .checked_add(count)
            .ok_or_else(|| anyhow::anyhow!("p90 histogram count overflows"))?;
        if total > MAX_P90_SAMPLES {
            bail!("p90 histogram exceeds the bounded sample limit");
        }
        if p90_from_histogram.is_none() && total >= rank {
            p90_from_histogram = Some(range);
        }
        previous = range;
    }
    if total != sample_count {
        bail!("p90 histogram total disagrees with sample_count");
    }
    let requested_range = object
        .get("p90_bytes")
        .and_then(serde_json::Value::as_u64)
        .filter(|value| Some(*value) == p90_from_histogram)
        .ok_or_else(|| anyhow::anyhow!("p90 value disagrees with histogram"))?;
    // Bind the exact histogram bytes into BP12. serde_json's map is sorted by
    // key in this build, matching the canonical JSON used by the Python
    // policy builder.
    let histogram_digest: [u8; 32] = Sha256::digest(serde_json::to_vec(histogram)?).into();
    Ok(V3P90Policy {
        trace_digest,
        policy_digest,
        histogram_digest,
        sample_count,
        requested_range,
    })
}

async fn authenticated_build<B: ObjectBackend + Clone>(
    client: &ObjectClient<B>,
    reference: &brewfs::workspace_overlay::packed_v3::wire005::V3ObjectRef,
) -> Result<brewfs::workspace_overlay::packed_v3::wire005::V3BuildProvenance> {
    let snapshot = brewfs::workspace_overlay::packed_v3::wire005::AuthenticatedV3Snapshot::open(
        client, reference,
    )
    .await?;
    Ok(snapshot.manifest().build.clone())
}

fn access_profile(value: &str) -> Result<AccessProfile> {
    match value.trim().to_ascii_lowercase().as_str() {
        "random" | "random-small-file" | "random_small_file" => Ok(AccessProfile::RandomSmallFile),
        "sequential" | "sequential-small-file" | "sequential_small_file" => {
            Ok(AccessProfile::SequentialSmallFile)
        }
        "mixed" => Ok(AccessProfile::Mixed),
        other => bail!(
            "unsupported access profile {other}; expected random-small-file, sequential-small-file, or mixed"
        ),
    }
}

fn leaf_path(leaf_index: u64, levels: u32, fanout: u64) -> String {
    let mut components = Vec::with_capacity(levels as usize);
    let mut value = leaf_index;
    for _ in 0..levels {
        components.push(format!("d{:03}", value % fanout));
        value /= fanout;
    }
    components.reverse();
    components.join("/")
}

fn build_tree(
    levels: u32,
    fanout: u64,
    next_inode: &mut u64,
    parent: Directory,
    output: &mut Vec<Directory>,
) {
    output.push(parent.clone());
    if levels == 0 {
        return;
    }
    for index in 0..fanout {
        let inode = *next_inode;
        *next_inode += 1;
        let name = format!("d{index:03}").into_bytes();
        let child = Directory {
            inode,
            parent_inode: parent.inode,
            name,
        };
        build_tree(levels - 1, fanout, next_inode, child, output);
    }
}

fn object_key(prefix: &str, relative: &str) -> String {
    if prefix.is_empty() {
        relative.to_owned()
    } else {
        format!("{prefix}/{relative}")
    }
}

/// Upload the same deterministic tree as one object per file. The bounded
/// JoinSet keeps SDK upload memory independent of the total dataset size.
async fn build_raw_fixture<B>(args: &Args, client: ObjectClient<B>, prefix: &str) -> Result<()>
where
    B: ObjectBackend + Clone + Send + Sync + 'static,
{
    let (min_file_size, max_file_size) = file_size_range(args)?;
    let leaf_count = args
        .dirs_per_level
        .checked_pow(args.dir_levels)
        .context("directory count overflows u64")?;
    let file_count = leaf_count
        .checked_mul(args.files_per_dir)
        .context("file count overflows u64")?;
    let semaphore = Arc::new(Semaphore::new(64));
    let mut uploads = JoinSet::new();
    let mut manifest = Vec::with_capacity(file_count as usize * 64);
    let mut logical_bytes = 0u64;

    for leaf_index in 0..leaf_count {
        let directory = leaf_path(leaf_index, args.dir_levels, args.dirs_per_level);
        for file_index in 0..args.files_per_dir {
            let global_file_index = leaf_index
                .checked_mul(args.files_per_dir)
                .and_then(|value| value.checked_add(file_index))
                .context("file index overflows u64")?;
            let size = file_size_for_index(global_file_index, min_file_size, max_file_size);
            let key = object_key(prefix, &format!("{directory}/f{file_index:06}"));
            manifest.extend_from_slice(format!("{key}\t{global_file_index}\t{size}\n").as_bytes());
            logical_bytes = logical_bytes.saturating_add(size as u64);
            let permit = semaphore.clone().acquire_owned().await?;
            let upload_client = client.clone();
            uploads.spawn(async move {
                let _permit = permit;
                let data = fill_pattern(size, global_file_index);
                upload_client.put_object(&key, &data).await
            });
            if uploads.len() >= 64 {
                let _ = uploads
                    .join_next()
                    .await
                    .context("raw SDK upload task disappeared")??;
            }
        }
    }
    while let Some(result) = uploads.join_next().await {
        result??;
    }

    let manifest_key = object_key(prefix, "raw-manifest.tsv");
    client.put_object(&manifest_key, &manifest).await?;
    std::fs::write(&args.manifest_output, format!("{manifest_key}\n"))?;
    println!(
        "raw_manifest_key={manifest_key} files={file_count} min_file_size={min_file_size} max_file_size={max_file_size} logical_bytes={logical_bytes} manifest_bytes={} prefix={prefix}",
        manifest.len()
    );
    Ok(())
}

fn apply_cold_targets(
    group: &mut PackedGroupInput,
    attributes: &[brewfs::workspace_overlay::packed_v3::wire005::V3ColdAttributes],
) -> Result<()> {
    if attributes.is_empty() {
        return Ok(());
    }
    let metadata = GroupMeta::decode(&group.metadata)?;
    let mut entries = metadata.entries().to_vec();
    for entry in &mut entries {
        if let Some(target) = attributes
            .iter()
            .find(|attrs| attrs.inode == entry.inode)
            .and_then(|attrs| attrs.symlink_target.as_ref())
        {
            entry.size = target.len() as u64;
        }
    }
    group.metadata = GroupMeta::new(entries)?.encode()?;
    Ok(())
}

async fn build_v3_fixture<B: ObjectBackend + Clone + Send + Sync + 'static>(
    args: &Args,
    client: ObjectClient<B>,
    prefix: &str,
) -> Result<()> {
    use brewfs::workspace_overlay::packed_v3::wire005::{V3ProducerOptions, V3SnapshotProducer};
    let provenance_client = client.clone();
    let build_policy = fixture_build_policy(args)?;
    let (minimum, maximum) = file_size_range(args)?;
    let profile = access_profile(&args.access_profile)?;
    let snapshot_id: [u8; 32] = Sha256::digest(b"brewfs-packed-v3-fixture").into();
    let root_key = directory_key(snapshot_id, 1);
    let options = V3ProducerOptions {
        snapshot_id,
        root_dir_key: root_key,
        root_inode: 1,
        profile,
        size_classes: SizeClassTable::default(),
        build_policy,
        metadata_codec: fixture_codec(args.metadata_codec.as_deref())?,
        data_codec: fixture_codec(args.data_codec.as_deref())?,
    };
    let prefix = if prefix.is_empty() {
        "packed-wire005-fixture"
    } else {
        prefix
    };
    if let Some(source) = &args.source_directory {
        #[cfg(target_os = "linux")]
        {
            use brewfs::workspace_overlay::packed_v3::wire005::{
                V3SourceConsistency, V3SourceFileLimits, V3SourceHardlinkPolicy,
                V3SourceNamespaceInventory, V3SourceNamespaceOptions,
            };
            let policy = match args.source_hardlink_policy.as_deref() {
                Some("visible-links") => V3SourceHardlinkPolicy::VisibleLinks,
                Some("reject-external") => V3SourceHardlinkPolicy::RejectExternal,
                _ => bail!("source-directory requires an explicit source-hardlink-policy"),
            };
            let consistency = match args.source_consistency.as_deref() {
                Some("best-effort-detected") => V3SourceConsistency::BestEffortDetected,
                Some("snapshot-backed") => V3SourceConsistency::SnapshotBacked,
                _ => bail!("source-directory requires an explicit source-consistency"),
            };
            let inventory = V3SourceNamespaceInventory::capture(
                source,
                &std::env::temp_dir(),
                V3SourceNamespaceOptions {
                    root_inode: 1,
                    consistency,
                    hardlink_policy: policy,
                    file_limits: V3SourceFileLimits::default(),
                },
            )
            .await?;
            let snapshot = inventory
                .build_snapshot(client, prefix.into(), options)
                .await?;
            let build = authenticated_build(&provenance_client, &snapshot.reference).await?;
            let provenance = serde_json::json!({
                "build_provenance": build,
                "capture_scope": "complete-directory-inventory",
                "source_consistency": snapshot.provenance.consistency,
                "source_view": snapshot.provenance,
                "source_hardlink_policy": policy.as_str(),
                "manifest_payload": "PM11", "index_payload": "IP06",
                "st_blocks_wire_preserved": true, "root_source_attributes_preserved": true,
                "posix_acl_scope": "linux-access-readonly-default-preserved",
                "source_report": snapshot.report,
                "group_logical_limit": 64 * 1024 * 1024,
                "group_data_limit": 16 * 1024 * 1024, "group_extent_limit": 256,
                "external_logical_limit": i64::MAX as u64,
                "manifest_key": snapshot.reference.key,
                "manifest_digest": hex::encode(snapshot.reference.digest),
            });
            std::fs::write(
                args.manifest_output.with_extension("source.json"),
                serde_json::to_vec_pretty(&provenance)?,
            )?;
            std::fs::write(
                &args.manifest_output,
                format!("{}\n", snapshot.reference.key),
            )?;
            println!(
                "packed_version=v3 source_entries={} unique_inodes={} groups={} frames={}",
                snapshot.report.source_entries,
                snapshot.report.unique_inodes,
                snapshot.report.groups,
                snapshot.report.frames
            );
            return Ok(());
        }
        #[cfg(not(target_os = "linux"))]
        bail!("source-directory capture requires Linux");
    }
    let mut producer =
        V3SnapshotProducer::new(client, &std::env::temp_dir(), prefix.into(), options).await?;
    if let Some(path) = &args.source_file {
        #[cfg(target_os = "linux")]
        {
            use brewfs::workspace_overlay::packed_v3::wire005::{
                CapturedV3Source, CapturedV3SourceRoot, V3SourceFileLimits,
            };
            // Capture disk I/O off the runtime. Its buffers are bounded and
            // cancellation cannot publish a partial source group.
            let path = path.clone();
            let root_path_for_capture = path.clone();
            let source_root = tokio::task::spawn_blocking(move || {
                let path = root_path_for_capture;
                let root_path = path
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or(std::path::Path::new("."));
                CapturedV3SourceRoot::capture(root_path, 1)
            })
            .await??;
            let mut captured = CapturedV3Source::capture_with_policy(
                &path,
                &std::env::temp_dir(),
                2,
                profile,
                SizeClassTable::default(),
                V3SourceFileLimits::default(),
                build_policy,
            )
            .await?;
            producer.set_root_attributes(source_root.attributes().clone())?;
            producer
                .set_inode_blocks(2, captured.source_blocks())
                .await?;
            let group = captured.group(1, root_key, profile)?;
            let external_chunks = match &mut captured {
                CapturedV3Source::External(source) => producer.add_external_source(source).await?,
                CapturedV3Source::Group(_) => 0,
            };
            let frames = match &captured {
                CapturedV3Source::Group(source) => source.frames(),
                CapturedV3Source::External(_) => &[],
            };
            producer.add_container(1, &[group], frames, &[1]).await?;
            producer
                .add_cold_attributes(captured.cold_attributes())
                .await?;
            producer
                .add_cold_attributes(source_root.cold_attributes())
                .await?;
            captured.validate_unchanged()?;
            source_root.validate_unchanged()?;
            let reference = producer.finish().await?;
            let build = authenticated_build(&provenance_client, &reference).await?;
            // A late mutation never yields a published manifest-output file.
            // Uploaded unreachable objects remain the caller's cleanup domain.
            captured.validate_unchanged()?;
            source_root.validate_unchanged()?;
            let provenance = serde_json::json!({
                "build_provenance": build,
                "capture_scope": "single-regular-file-dentry",
                "source_consistency": "stat-revalidated-not-atomic-directory-snapshot",
                "visible_nlink": captured.entry().nlink,
                "source_nlink": captured.source_nlink(),
                "source_blocks": captured.source_blocks(),
                "size": captured.entry().size,
                "mode": captured.entry().mode,
                "uid": captured.entry().uid, "gid": captured.entry().gid,
                "mtime_ns": captured.entry().mtime_ns, "ctime_ns": captured.entry().ctime_ns,
                "data_bytes": captured.data_bytes(),
                "extent_count": captured.extent_count(),
                "external_chunks": external_chunks,
                "xattr_count": captured.cold_attributes().xattrs.len(),
                "manifest_payload": "PM11", "index_payload": "IP06",
                "root_capture_scope": "source-parent-attributes-only-not-full-inventory",
                "st_blocks_wire_preserved": true,
                "root_source_attributes_preserved": true,
                "root_size": source_root.attributes().size,
                "root_blocks": source_root.attributes().blocks,
                "root_mode": source_root.attributes().mode,
                "root_uid": source_root.attributes().uid, "root_gid": source_root.attributes().gid,
                "root_nlink": source_root.attributes().nlink,
                "manifest_key": reference.key, "manifest_digest": hex::encode(reference.digest),
            });
            let provenance_path = args.manifest_output.with_extension("source.json");
            std::fs::write(provenance_path, serde_json::to_vec_pretty(&provenance)?)?;
            std::fs::write(&args.manifest_output, format!("{}\n", reference.key))?;
            println!(
                "packed_version=v3 files=1 logical_bytes={} source_data_bytes={} extents={}",
                captured.entry().size,
                provenance["data_bytes"],
                captured.extent_count()
            );
            return Ok(());
        }
        #[cfg(not(target_os = "linux"))]
        bail!("source-file capture requires Linux");
    }
    let mut next_inode = 2;
    let mut directories = Vec::new();
    build_tree(
        args.dir_levels,
        args.dirs_per_level,
        &mut next_inode,
        Directory {
            inode: 1,
            parent_inode: 0,
            name: Vec::new(),
        },
        &mut directories,
    );
    let mut cold_attributes = Vec::new();
    if args.cold_corpus {
        use brewfs::workspace_overlay::packed_v3::wire005::{V3ColdAttributes, V3Xattr};
        let inode = next_inode;
        next_inode += 1;
        cold_attributes.push(V3ColdAttributes {
            inode,
            symlink_target: Some(b"raw-\xff-target".to_vec()),
            xattrs: Vec::new(),
            acl: Vec::new(),
        });
        let inode = next_inode;
        next_inode += 1;
        cold_attributes.push(V3ColdAttributes {
            inode,
            symlink_target: None,
            xattrs: vec![V3Xattr {
                name: b"user.brewfs.test".to_vec(),
                value: b"\0\xffcold".to_vec(),
            }],
            acl: vec![brewfs::meta::store::AclRule {
                acl_type: 1,
                qualifier: 0,
                permissions: 7,
            }],
        });
    }
    let hardlink_inode = if args.hardlink_corpus {
        let inode = next_inode;
        next_inode += 1;
        Some(inode)
    } else {
        None
    };
    let hardlink_parent = directories
        .iter()
        .find(|directory| directory.parent_inode == 1)
        .map(|directory| directory.inode)
        .unwrap_or(1);
    let mut file_index = 0u64;
    let mut group_count = 0u64;
    let mut logical_bytes = 0u64;
    for directory in &directories {
        let key = if directory.inode == 1 {
            root_key
        } else {
            directory_key(snapshot_id, directory.inode)
        };
        let mut files = Vec::new();
        let mut pending_bytes = 0usize;
        let mut shard = 0u64;
        for child in directories
            .iter()
            .filter(|child| child.parent_inode == directory.inode)
        {
            files.push(PackedFileInput {
                name: child.name.clone(),
                inode: child.inode,
                kind: 2,
                mode: 0o040755,
                uid: 0,
                gid: 0,
                rdev: 0,
                nlink: 2,
                atime_ns: 0,
                mtime_ns: 0,
                ctime_ns: 0,
                flags: 0,
                data: Vec::new(),
            });
        }
        if directory.inode == 1 && args.cold_corpus {
            for attributes in &cold_attributes {
                let symlink = attributes.symlink_target.is_some();
                files.push(PackedFileInput {
                    name: if symlink {
                        b".cold-link".to_vec()
                    } else {
                        b".cold-file".to_vec()
                    },
                    inode: attributes.inode,
                    kind: if symlink { 3 } else { 1 },
                    // Writable permission bits let the diagnostic reach the
                    // readonly filesystem instead of stopping at DAC denial.
                    mode: if symlink { 0o120777 } else { 0o100666 },
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    nlink: 1,
                    atime_ns: 0,
                    mtime_ns: 0,
                    ctime_ns: 0,
                    flags: 0,
                    data: if symlink {
                        Vec::new()
                    } else {
                        b"cold\n".to_vec()
                    },
                });
            }
        }
        if let Some(inode) = hardlink_inode {
            let mut names = Vec::new();
            if directory.inode == 1 {
                names.push(b".hardlink-a".to_vec());
            }
            if directory.inode == hardlink_parent {
                names.push(b".hardlink-b".to_vec());
            }
            for name in names {
                files.push(PackedFileInput {
                    name,
                    inode,
                    kind: 1,
                    mode: 0o100644,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    nlink: 2,
                    atime_ns: 0,
                    mtime_ns: 0,
                    ctime_ns: 0,
                    flags: 0,
                    data: b"hardlink\n".to_vec(),
                });
            }
        }
        if !directories
            .iter()
            .any(|child| child.parent_inode == directory.inode)
        {
            for index in 0..args.files_per_dir {
                let size = file_size_for_index(file_index, minimum, maximum);
                files.push(PackedFileInput {
                    name: format!("f{index:06}").into_bytes(),
                    inode: next_inode,
                    kind: 1,
                    mode: 0o100644,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    nlink: 1,
                    atime_ns: 0,
                    mtime_ns: 0,
                    ctime_ns: 0,
                    flags: 0,
                    data: fill_pattern(size, file_index),
                });
                next_inode += 1;
                file_index += 1;
                logical_bytes += size as u64;
                pending_bytes += size;
                if pending_bytes >= 8 * 1024 * 1024 || files.len() >= 256 {
                    let id = (directory.inode << 32) | shard;
                    let (mut group, frames) =
                        brewfs::workspace_overlay::packed_v3::pack_group_files_with_policy(
                            id,
                            key,
                            std::mem::take(&mut files),
                            profile,
                            SizeClassTable::default(),
                            build_policy,
                        )?;
                    apply_cold_targets(&mut group, &cold_attributes)?;
                    producer
                        .add_container(id, &[group], &frames, &[directory.inode])
                        .await?;
                    pending_bytes = 0;
                    shard += 1;
                    group_count += 1;
                }
            }
        }
        if !files.is_empty() {
            let id = (directory.inode << 32) | shard;
            let (mut group, frames) =
                brewfs::workspace_overlay::packed_v3::pack_group_files_with_policy(
                    id,
                    key,
                    files,
                    profile,
                    SizeClassTable::default(),
                    build_policy,
                )?;
            apply_cold_targets(&mut group, &cold_attributes)?;
            producer
                .add_container(id, &[group], &frames, &[directory.inode])
                .await?;
            group_count += 1;
        }
    }
    for attrs in &cold_attributes {
        producer.add_cold_attributes(attrs).await?;
    }
    let reference = producer.finish().await?;
    let build = authenticated_build(&provenance_client, &reference).await?;
    let provenance = serde_json::json!({
        "capture_scope": "generated-deterministic-corpus",
        "manifest_payload": "PM11", "index_payload": "IP06",
        "manifest_key": reference.key, "manifest_digest": hex::encode(reference.digest),
        "build_provenance": build, "logical_bytes": logical_bytes, "files": file_index,
    });
    std::fs::write(
        args.manifest_output.with_extension("source.json"),
        serde_json::to_vec_pretty(&provenance)?,
    )?;
    std::fs::write(&args.manifest_output, format!("{}\n", reference.key))?;
    println!(
        "packed_version=v3 manifest_key={} manifest_digest={} files={} groups={} logical_bytes={}",
        reference.key,
        hex::encode(reference.digest),
        file_index,
        group_count,
        logical_bytes
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Args::parse();
    if args.raw_only && (args.frame_policy != "size-only" || args.inline_data != "on") {
        bail!("packed frame/inline controls require a packed v3 corpus");
    }
    if (args.source_file.is_some() || args.source_directory.is_some())
        && (args.wire_version != 5 || args.raw_only || args.cold_corpus || args.hardlink_corpus)
    {
        bail!(
            "source capture requires the current packed-v3 encoding and cannot combine generated/raw corpora"
        );
    }
    if let Some(source) = &args.source_directory {
        if !matches!(
            args.source_consistency.as_deref(),
            Some("best-effort-detected" | "snapshot-backed")
        ) || args.source_hardlink_policy.is_none()
        {
            bail!(
                "source-directory requires explicit source-consistency and source-hardlink-policy"
            );
        }
        let output = resolve_destination(&args.output_dir)?;
        let prefix = args.prefix.trim_matches('/');
        let objects = resolve_destination(&output.join(if prefix.is_empty() {
            "packed-wire005-fixture"
        } else {
            prefix
        }))?;
        let manifest = resolve_destination(&args.manifest_output)?;
        let provenance = resolve_destination(&manifest.with_extension("source.json"))?;
        #[cfg(target_os = "linux")]
        {
            let mut destinations = vec![manifest.clone(), provenance];
            if args.bucket.is_none() {
                destinations.extend([output.clone(), objects]);
            }
            brewfs::workspace_overlay::packed_v3::wire005::V3SourceNamespaceInventory::validate_output_paths(source, &destinations)?;
        }
        #[cfg(not(target_os = "linux"))]
        bail!("source namespace import requires Linux");
        // Use the resolved destination, so a missing component followed by ..
        // cannot create a scratch directory inside the read-only source.
        args.output_dir = output;
        args.manifest_output = manifest;
    }
    if (args.cold_corpus || args.hardlink_corpus) && (args.wire_version != 5 || args.raw_only) {
        bail!("cold-corpus requires the current packed-v3 encoding");
    }
    if args.dir_levels > 8 || args.dirs_per_level == 0 || args.files_per_dir == 0 {
        bail!("dir-levels must be <= 8 and fanout/files-per-dir must be non-zero");
    }
    file_size_range(&args)?;
    access_profile(&args.access_profile)?;
    if args.chunk_size < u64::from(args.block_size) || args.block_size == 0 {
        bail!("chunk-size must be >= block-size and block-size must be non-zero");
    }
    let prefix = args.prefix.trim_matches('/');
    if let Some(bucket) = args.bucket.as_deref() {
        let backend = S3Backend::with_config(S3Config {
            bucket: bucket.to_owned(),
            endpoint: args.endpoint.clone(),
            region: args.region.clone(),
            force_path_style: args.force_path_style,
            part_size: 16 * 1024 * 1024,
            max_concurrency: 32,
            disable_payload_checksum: true,
            ..Default::default()
        })
        .await?;
        let client = ObjectClient::new(backend);
        if args.raw_only {
            build_raw_fixture(&args, client, prefix).await
        } else {
            build_v3_fixture(&args, client, prefix).await
        }
    } else {
        std::fs::create_dir_all(&args.output_dir)?;
        let backend = LocalFsBackend::new(&args.output_dir);
        let client = ObjectClient::new(backend);
        if args.raw_only {
            build_raw_fixture(&args, client, prefix).await
        } else {
            build_v3_fixture(&args, client, prefix).await
        }
    }
}

fn resolve_destination(path: &std::path::Path) -> Result<PathBuf> {
    use std::path::Component;
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut result = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::RootDir => result.push("/"),
            Component::CurDir => {}
            Component::ParentDir => {
                result.pop();
            }
            Component::Normal(part) => {
                result.push(part);
                match std::fs::symlink_metadata(&result) {
                    Ok(_) => {
                        result = std::fs::canonicalize(&result)
                            .context("resolve destination component")?
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
            Component::Prefix(_) => bail!("unsupported destination prefix"),
        }
    }
    Ok(result)
}

#[cfg(test)]
mod codec_control_tests {
    #[test]
    fn p90_policy_rejects_sample_count_above_bound_before_rank_calculation() {
        let mut policy = serde_json::json!({
            "schema": "packed-v3-p90-policy-v1",
            "trace_sha256": "00".repeat(32),
            "source": "fixture-trace",
            "captured_at_utc": "2026-10-10T00:00:00Z",
            "sample_count": 10_000_001u64,
            "histogram": [{"range_bytes": 1u64, "count": 1u64}],
            "p90_bytes": 1u64,
        });
        let digest = Sha256::digest(serde_json::to_vec(&policy).unwrap());
        policy["policy_sha256"] = serde_json::Value::String(hex::encode(digest));
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("p90-policy.json");
        std::fs::write(&path, serde_json::to_vec(&policy).unwrap()).unwrap();

        let error =
            read_p90_policy(&path).expect_err("oversized p90 sample count must fail closed");
        assert!(error.to_string().contains("p90 sample_count is invalid"));
    }

    #[test]
    fn g15_cli_accepts_actual_static_and_inline_off_controls() {
        assert!(
            Args::try_parse_from([
                "fixture",
                "--frame-policy",
                "static-1mib",
                "--inline-data",
                "off",
                "--metadata-codec",
                "raw",
                "--data-codec",
                "zstd",
            ])
            .is_ok(),
            "required actual layout controls are unavailable"
        );
    }

    use super::*;
    #[test]
    fn namespace_cli_accepts_explicit_snapshot_provider_selection() {
        let args = Args::try_parse_from([
            "fixture",
            "--source-directory",
            "readonly-snapshot",
            "--source-consistency",
            "snapshot-backed",
            "--source-hardlink-policy",
            "visible-links",
        ])
        .unwrap();
        assert_eq!(args.source_consistency.as_deref(), Some("snapshot-backed"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn destination_preflight_rejects_existing_final_source_file_symlink() {
        use brewfs::workspace_overlay::packed_v3::wire005::V3SourceNamespaceInventory;
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        std::fs::create_dir(&source).unwrap();
        let original = source.join("original");
        std::fs::write(&original, b"source-must-not-change").unwrap();
        let destination = root.path().join("outside-manifest");
        std::os::unix::fs::symlink(&original, &destination).unwrap();
        // This is the actual fixture preflight sequence: the final link is
        // resolved first, then containment checks the resulting source path.
        let resolved = resolve_destination(&destination).unwrap();
        assert_eq!(resolved, original);
        assert!(V3SourceNamespaceInventory::validate_output_paths(&source, &[resolved]).is_err());
        assert_eq!(std::fs::read(&original).unwrap(), b"source-must-not-change");
        assert_eq!(std::fs::read_dir(&source).unwrap().count(), 1);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn destination_preflight_rejects_dangling_final_source_file_symlink() {
        use brewfs::workspace_overlay::packed_v3::wire005::V3SourceNamespaceInventory;
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        std::fs::create_dir(&source).unwrap();
        let absent = source.join("must-not-be-created");
        let destination = root.path().join("outside-manifest");
        std::os::unix::fs::symlink(&absent, &destination).unwrap();
        let accepted = resolve_destination(&destination).and_then(|resolved| {
            V3SourceNamespaceInventory::validate_output_paths(&source, &[resolved])
                .map_err(anyhow::Error::from)
        });
        assert!(accepted.is_err());
        assert!(!absent.exists());
        assert_eq!(std::fs::read_dir(&source).unwrap().count(), 0);
    }
    #[cfg(unix)]
    #[test]
    fn destination_guard_resolves_aliases_and_missing_tail_without_creating_source_paths() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        std::fs::create_dir(&source).unwrap();
        let alias = root.path().join("alias");
        std::os::unix::fs::symlink(&source, &alias).unwrap();
        assert_eq!(
            resolve_destination(&alias.join("objects")).unwrap(),
            source.join("objects")
        );
        assert_eq!(
            resolve_destination(&source.join("missing/../../objects")).unwrap(),
            root.path().join("objects")
        );
        assert_eq!(std::fs::read_dir(&source).unwrap().count(), 0);
        std::os::unix::fs::symlink(root.path().join("absent"), root.path().join("dangling"))
            .unwrap();
        assert!(resolve_destination(&root.path().join("dangling/objects")).is_err());
    }

    #[test]
    fn namespace_cli_requires_directory_for_source_policies_and_rejects_mixed_sources() {
        assert!(
            Args::try_parse_from(["fixture", "--source-hardlink-policy", "visible-links"]).is_err()
        );
        assert!(
            Args::try_parse_from(["fixture", "--source-consistency", "best-effort-detected"])
                .is_err()
        );
        assert!(
            Args::try_parse_from([
                "fixture",
                "--source-file",
                "one",
                "--source-directory",
                "tree"
            ])
            .is_err()
        );
    }

    #[test]
    fn wire005_codec_controls_preserve_defaults_and_reject_unknown_codecs() {
        use brewfs::workspace_overlay::packed_v3::PackedCodec;
        assert_eq!(fixture_codec(None).unwrap(), PackedCodec::Zstd);
        assert_eq!(fixture_codec(Some("raw")).unwrap(), PackedCodec::Raw);
        assert!(fixture_codec(Some("invalid")).is_err());
        assert!(Args::try_parse_from(["fixture", "--data-codec", "invalid"]).is_err());
        let args = Args::try_parse_from([
            "fixture",
            "--wire-version",
            "5",
            "--data-codec",
            "raw",
            "--metadata-codec",
            "raw",
        ])
        .unwrap();
        assert_eq!(args.data_codec.as_deref(), Some("raw"));
        assert_eq!(args.metadata_codec.as_deref(), Some("raw"));
    }

    #[test]
    fn fixture_defaults_to_current_v3_contract_and_rejects_legacy_writer() {
        assert_eq!(Args::try_parse_from(["fixture"]).unwrap().wire_version, 5);
        for version in ["4", "6"] {
            assert!(Args::try_parse_from(["fixture", "--wire-version", version]).is_err());
        }
    }
}
