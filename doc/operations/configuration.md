# 配置指南

BrewFS 是单二进制程序。挂载时主要通过 `brewfs mount` 的命令行参数和 YAML 配置文件控制运行行为；命令行参数优先级高于 YAML。日志、FUSE 调试和部分实验性开关通过环境变量控制。

相关代码入口：

- `src/config.rs`: CLI 参数、YAML schema、默认值合并逻辑。
- `src/main.rs`: 后端创建、control plane、mount 生命周期。
- `src/vfs/cache/config.rs`: VFS 读写缓存默认值。
- `src/meta/config.rs`: meta client cache 和 compaction 默认值。

## Single-node installer

单机部署可以直接使用 `scripts/install_brewfs_single_node.sh`。这个脚本会安装并
维护三个 systemd 服务：

| 服务 | 作用 |
|---|---|
| `brewfs-redis.service` | Redis 元数据服务 |
| `brewfs-rustfs.service` | RustFS S3 兼容对象存储 |
| `brewfs.service` | BrewFS FUSE 挂载 |

当前分支合并到 main 后，可以通过 GitHub raw URL 一行安装：

```bash
curl -fsSL https://raw.githubusercontent.com/brewfs/brewfs/main/scripts/install_brewfs_single_node.sh | sudo bash -s -- install
```

指定挂载点和调优档位：

```bash
curl -fsSL https://raw.githubusercontent.com/brewfs/brewfs/main/scripts/install_brewfs_single_node.sh \
  | sudo env MOUNT_POINT=/mnt/brewfs BREWFS_TUNING_PROFILE=balanced bash -s -- install
```

常用动作：

```bash
sudo /usr/local/bin/brewfs --version
sudo systemctl status brewfs.service brewfs-redis.service brewfs-rustfs.service
sudo bash scripts/install_brewfs_single_node.sh status
sudo bash scripts/install_brewfs_single_node.sh restart
sudo bash scripts/install_brewfs_single_node.sh upgrade
sudo bash scripts/install_brewfs_single_node.sh uninstall
```

安装脚本默认写入：

| 路径 | 内容 |
|---|---|
| `/usr/local/bin/brewfs` | BrewFS 二进制 |
| `/usr/local/bin/rustfs` | RustFS 二进制 |
| `/etc/brewfs/mount.yaml` | BrewFS 挂载配置 |
| `/etc/default/brewfs*` | systemd 环境文件 |
| `/etc/systemd/system/brewfs*.service` | systemd unit |
| `/var/lib/brewfs` | Redis、RustFS、BrewFS cache 状态 |
| `/var/log/brewfs` | 服务日志 |

挂载点必须是空目录。默认是 `/mnt/brewfs`，如果该目录已有内容，请使用：

```bash
curl -fsSL https://raw.githubusercontent.com/brewfs/brewfs/main/scripts/install_brewfs_single_node.sh \
  | sudo env MOUNT_POINT=/mnt/brewfs-single bash -s -- install
```

调优档位：

| `BREWFS_TUNING_PROFILE` | 用途 |
|---|---|
| `compat` | 尽量接近旧配置，适合排查回归 |
| `balanced` | 默认值，适合单机 Redis + RustFS 的通用负载 |
| `throughput` | 更激进的吞吐配置，适合专门压读或随机混合 IO |

关键参数都可以用环境变量覆盖，例如：

```bash
curl -fsSL https://raw.githubusercontent.com/brewfs/brewfs/main/scripts/install_brewfs_single_node.sh \
  | sudo env \
      MOUNT_POINT=/mnt/brewfs \
      BREWFS_TUNING_PROFILE=balanced \
      BREWFS_FUSE_WORKERS=4 \
      BREWFS_UPLOAD_CONCURRENCY=16 \
      BREWFS_MEMORY_BUDGET_BYTES=2147483648 \
      bash -s -- install
```

如果只想安装指定 BrewFS 版本：

```bash
curl -fsSL https://raw.githubusercontent.com/brewfs/brewfs/main/scripts/install_brewfs_single_node.sh \
  | sudo env BREWFS_VERSION=v0.0.1 bash -s -- install
```

脚本会优先选择可下载的 BrewFS release artifact；当最新 tag 没有对应 R2 二进制时，
会跳过该 tag 并继续查找可用版本。

RustFS 默认使用官方安装脚本 `https://rustfs.com/install_rustfs.sh` 对应的
`https://dl.rustfs.com/artifacts/rustfs/release` 二进制镜像源。官方脚本本身是交互式的，
并会创建独立 `rustfs.service`；BrewFS 单机安装脚本只复用它的二进制来源，
再由 `brewfs-rustfs.service` 统一维护服务生命周期。

## 快速模板

最小配置只需要挂载点。其余字段使用默认值：本地对象目录 `./data`、SQLx 元数据、内存 SQLite、64 MiB chunk、4 MiB block。

```yaml
mount_point: /mnt/brewfs
```

本地开发常用配置：

```yaml
mount_point: /mnt/brewfs

data:
  backend: local-fs
  localfs:
    data_dir: /var/lib/brewfs/data

meta:
  backend: sqlx
  sqlx:
    url: "sqlite:///var/lib/brewfs/meta.db?mode=rwc"

cache:
  root: /var/cache/brewfs
```

Redis + RustFS/MinIO/S3 兼容对象存储配置：

```yaml
mount_point: /mnt/brewfs

data:
  backend: s3
  s3:
    bucket: brewfs-data
    endpoint: http://127.0.0.1:9000
    region: us-east-1
    force_path_style: true
    disable_payload_checksum: true
    part_size: 16777216
    max_concurrency: 32

meta:
  backend: redis
  redis:
    url: "redis://127.0.0.1:6379/0"
  open_file_cache_ttl_ms: 30000
  open_file_cache_capacity: 65536
  allow_write_open_cache: false

cache:
  root: /var/cache/brewfs
  writeback_mode: upload_before_commit

fuse:
  workers: 1
  max_background: 512
  privileged: false
```

运行：

```bash
brewfs mount --config /etc/brewfs/mount.yaml
brewfs info /mnt/brewfs
brewfs gc /mnt/brewfs --dry-run
```

如果同时传入 CLI 参数和 YAML，例如：

```bash
brewfs mount --config /etc/brewfs/mount.yaml --s3-bucket other-bucket
```

则 `--s3-bucket` 会覆盖 YAML 中的 `data.s3.bucket`。

## CLI

```bash
brewfs mount [OPTIONS] [MOUNT_POINT]
brewfs info [MOUNT_POINT]
brewfs gc [MOUNT_POINT] [--dry-run]
brewfs console [OPTIONS]
```

主要 `mount` 参数：

| 参数 | 说明 |
|---|---|
| `--config <FILE>` | YAML 配置文件。 |
| `[MOUNT_POINT]` | 挂载点；覆盖 `mount_point`。 |
| `--data-backend <local-fs|s3>` | 对象数据后端。 |
| `--data-dir <DIR>` | `local-fs` 数据目录。 |
| `--s3-bucket <BUCKET>` | S3 bucket。 |
| `--s3-endpoint <URL>` | S3 兼容端点；AWS S3 可不填。 |
| `--s3-region <REGION>` | S3 region。 |
| `--s3-part-size <BYTES>` | multipart part 大小，默认 16 MiB。 |
| `--s3-max-concurrency <N>` | S3 multipart 最大并发，默认 32。 |
| `--s3-force-path-style <true|false>` | path-style S3 访问。MinIO/RustFS 通常设为 `true`。 |
| `--s3-disable-payload-checksum <true|false>` | 禁用 SigV4 payload SHA-256，默认 `true`。 |
| `--meta-backend <sqlx|redis|etcd|tikv>` | 元数据后端。 |
| `--meta-url <URL>` | SQLx 或 Redis URL。 |
| `--meta-etcd-urls <URLS>` | 逗号分隔的 Etcd endpoint。 |
| `--meta-tikv-pd-endpoints <URLS>` | 逗号分隔的 TiKV PD endpoint。 |
| `--meta-tikv-namespace <NAMESPACE>` | TiKV key namespace，默认 `brewfs`。 |
| `--chunk-size <BYTES>` | chunk 大小，默认 64 MiB。 |
| `--block-size <BYTES>` | block 大小，默认 4 MiB。 |
| `--fuse-workers <N>` | FUSE worker 数；`0` 或 `1` 使用低开销 session dispatch。 |
| `--fuse-max-background <N>` | FUSE 最大 in-flight 请求数，默认 512。 |
| `--privileged` | 直接打开 `/dev/fuse`，适合 systemd/root/container。 |

`console` 子命令：

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `--listen <ADDR>` | `127.0.0.1:8080` | HTTP listen 地址。 |
| `--state-dir <DIR>` | 自动 | console 状态目录。 |
| `--runtime-dir <DIR>` | 自动 | BrewFS runtime registry 目录。 |
| `--static-dir <DIR>` | 自动 | 预构建前端静态资源目录。 |
| `--auth-token-file <FILE>` | 无 | Bearer token 文件。 |
| `--dev-no-auth` | `false` | 本地开发免认证；只允许 loopback listener。 |
| `--enable-csi-dashboard` | `false` | 开启只读 Kubernetes CSI dashboard API。 |
| `--kubeconfig <FILE>` | 自动 | Kubernetes config。 |
| `--csi-driver-name <NAME>` | `csi.brewfs.io` | CSI driver 名称。 |

## YAML Schema

顶层字段：

```yaml
mount_point: /mnt/brewfs
data: {}
meta: {}
layout: {}
fuse: {}
cache: {}
compact: {}
```

### data

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `data.backend` | `local-fs` | `local-fs` 或 `s3`。 |
| `data.localfs.data_dir` | `./data` | 本地对象数据目录。 |
| `data.s3.bucket` | 无 | S3 bucket；`backend=s3` 时必填。 |
| `data.s3.endpoint` | 无 | S3 兼容端点。 |
| `data.s3.region` | 无 | S3 region；自建对象存储常用 `us-east-1`。 |
| `data.s3.part_size` | `16777216` | multipart part 大小，单位 bytes。 |
| `data.s3.max_concurrency` | `32` | S3 multipart 最大并发。 |
| `data.s3.force_path_style` | `false` | MinIO/RustFS/Ceph RGW 常需要 `true`。 |
| `data.s3.disable_payload_checksum` | `true` | 自建 S3 通常建议 `true` 以降低写路径 CPU。 |

S3 凭据走 AWS SDK 标准环境变量和配置文件，例如：

```bash
export AWS_ACCESS_KEY_ID=rustfsadmin
export AWS_SECRET_ACCESS_KEY=rustfsadmin
export AWS_DEFAULT_REGION=us-east-1
export AWS_EC2_METADATA_DISABLED=true
```

### meta

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `meta.backend` | `sqlx` | `sqlx`、`redis`、`etcd` 或 `tikv`。 |
| `meta.sqlx.url` | `sqlite::memory:` | SQLite 或 PostgreSQL URL。 |
| `meta.redis.url` | 无 | Redis URL；`backend=redis` 时必须显式配置。 |
| `meta.etcd.urls` | `[]` | Etcd endpoint 列表。 |
| `meta.tikv.pd_endpoints` | `[]` | TiKV PD endpoint 列表。 |
| `meta.tikv.namespace` | `brewfs` | TiKV key namespace。 |
| `meta.open_file_cache_ttl_ms` | 关闭 | 只读 open 文件属性缓存 TTL，单位 ms。 |
| `meta.open_file_cache_capacity` | 默认值 | open file cache 容量。 |
| `meta.allow_write_open_cache` | `false` | 允许写 open 复用属性缓存。仅建议在单客户端或可接受跨客户端 close-to-open 新鲜度减弱的性能场景中启用。 |

示例：

```yaml
meta:
  backend: sqlx
  sqlx:
    url: "postgres://brewfs:secret@postgres:5432/brewfs"
```

```yaml
meta:
  backend: etcd
  etcd:
    urls:
      - "http://10.0.0.11:2379"
      - "http://10.0.0.12:2379"
```

```yaml
meta:
  backend: tikv
  tikv:
    pd_endpoints:
      - "10.0.0.21:2379"
    namespace: tenant-a
```

内部 meta client 的 inode/path cache 会按后端选择默认 TTL：SQLite 10s、PostgreSQL 500ms、Redis 500ms、Etcd 2s、TiKV 1s。FUSE 返回给内核的 attr/entry TTL 是独立配置，见下方环境变量 `BREWFS_CACHE_TTL_MS`。

### layout

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `layout.chunk_size` | `67108864` | chunk 大小，默认 64 MiB。 |
| `layout.block_size` | `4194304` | block/object 粒度，默认 4 MiB。 |

`chunk_size` 必须大于等于 `block_size`。提高 `chunk_size` 可以减少大文件元数据提交频率，但会增加单个 chunk 的管理跨度。

### fuse

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `fuse.workers` | `1` | `0` 或 `1` 使用低开销 session dispatch；大于 `1` 开启 worker pool。 |
| `fuse.max_background` | `512` | asyncfuse worker 模式下最大 queued/running 请求数。 |
| `fuse.privileged` | `false` | 直接使用 `/dev/fuse`，通常用于 root/systemd/container。 |

非特权挂载依赖 `fusermount3`；特权挂载依赖进程有权限访问 `/dev/fuse`。

### cache

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `cache.root` / `cache.cache_root` | `$XDG_CACHE_HOME/brewfs` 或 `/tmp/brewfs` | 本地缓存根目录。 |
| `cache.read_memory_bytes` | `4294967296` | 读缓存内存预算，默认 4 GiB。 |
| `cache.read_ssd_bytes` | `21474836480` | 读缓存磁盘预算，默认 20 GiB。 |
| `cache.write_memory_bytes` | `402653184` | 写缓存内存预算，默认 384 MiB。 |
| `cache.write_ssd_bytes` | `21474836480` | 写缓存磁盘预算，默认 20 GiB。 |
| `cache.dirty_slice_target_size` | `33554432` | 脏 slice 聚合目标，默认 32 MiB。 |
| `cache.dirty_slice_max_age_ms` | `2000` | 脏 slice 最大聚合时间。 |
| `cache.upload_concurrency` | `10` | 单 writer 内 block upload 并发。 |
| `cache.prefetch_enabled` | `true` | VFS 顺序预取开关。 |
| `cache.prefetch_max_bytes` | `67108864` | 最大预读距离，默认 64 MiB。 |
| `cache.prefetch_concurrency` | `64` | 预取并发。 |
| `cache.range_background_prefetch` | `true` | range miss 后后台补全 block。 |
| `cache.populate_write_cache_after_upload` | `true` | 上传后把写入 block 放入读缓存。 |
| `cache.persist_write_cache_after_upload` | `false` | 上传后是否持久化到磁盘读缓存。 |
| `cache.memory_budget_bytes` | `1342177280` | VFS reader/writer buffer 总预算，默认 1280 MiB。 |
| `cache.compression` | `lz4` | 对象压缩：`none`、`lz4`、`zstd`。 |
| `cache.zstd_level` | `3` | `compression=zstd` 时的 level。 |
| `cache.verify_cache_checksum` | `full` | 本地缓存校验：`full` 或 `none`。 |
| `cache.writeback_mode` | `upload_before_commit` | 可选 `upload_before_commit` 或 `commit_before_upload`。 |
| `cache.writeback_persist_sync` | `true` | staged writeback 同步落盘后再继续。 |
| `cache.writeback_require_stage_before_commit` | `true` | 发布元数据前要求本地 stage sealed。 |
| `cache.writeback_recent_pending_soft_bytes` | `0` | commit-before-upload 待上传软限制，0 为关闭。 |
| `cache.writeback_recent_pending_hard_bytes` | `0` | commit-before-upload 待上传硬限制，0 跟随软限制。 |
| `cache.bandwidth.upload_limit_mibps` | 无 | 上传限速，单位 MiB/s。 |
| `cache.bandwidth.download_limit_mibps` | 无 | 下载限速，单位 MiB/s。 |

`writeback_mode` 语义：

- `upload_before_commit`: 默认安全模式。先上传对象，再提交元数据；`fsync`/`close` 成功后数据已在对象存储和元数据中可见。
- `commit_before_upload`: 高性能 S3 writeback 模式。先提交元数据，再异步上传对象；只允许 `data.backend=s3`。本地缓存盘损坏或进程异常退出时，已发布但未上传的对象可能丢失，其他客户端也可能暂时看到缺失对象。

### compact

`compact` 控制 slice compaction 和锁 TTL：

```yaml
compact:
  min_slice_count: 3
  min_fragment_ratio: 0.1
  async_threshold: 100
  sync_threshold: 200
  interval:
    secs: 600
    nanos: 0
  max_chunks_per_run: 1000
  max_concurrent_tasks: 4
  light_enabled: true
  light_threshold: 2
  heavy_enabled: true
  heavy_fragment_threshold: 0.3
  heavy_slice_threshold: 30
  heavy_force_fragment_threshold: 0.5
  lock_ttl:
    async_ttl_secs: 10
    sync_ttl_secs: 30
    ttl_per_slice_ms: 50
    min_ttl_secs: 5
    max_ttl_secs: 300
```

`interval` 使用 Rust `Duration` 的 YAML 表示，即 `secs` 和 `nanos`。

## FUSE 和运行时环境变量

| 环境变量 | 默认值 | 说明 |
|---|---:|---|
| `RUST_LOG` | `brewfs=info` | tracing filter，例如 `brewfs=debug,asyncfuse=info`。 |
| `BREWFS_LOG_FILE` | stderr | 主日志输出文件。 |
| `BREWFS_FUSE_LOG_FILE` | 无 | 单独输出 asyncfuse op 日志；会屏蔽主日志里的 `asyncfuse::raw::logfs`。 |
| `BREWFS_NOFILE_LIMIT` | `1048576` | 启动时尝试提升 `RLIMIT_NOFILE`。 |
| `BREWFS_CACHE_TTL_MS` | `1000` | FUSE attr/entry TTL。设为 `0` 可严格绕过内核 attr/entry cache。新建文件 create reply 的 attr TTL 固定为 0。 |
| `BREWFS_FUSE_DIRECT_IO` | 无 | 对所有 open reply 启用或关闭 direct IO。 |
| `BREWFS_FUSE_READ_DIRECT_IO` | `false` | 只对只读 open 启用 direct IO。 |
| `BREWFS_FUSE_WRITE_DIRECT_IO` | `false` | 只对只写 open 启用 direct IO。 |
| `BREWFS_FUSE_KEEP_CACHE` | `false` | open reply 加 `FOPEN_KEEP_CACHE`。 |
| `BREWFS_FUSE_COPY_FILE_RANGE` | `true` | 控制 FUSE `copy_file_range` 支持。 |
| `BREWFS_CACHED_BLOCK_ASSEMBLER` | `false` | 实验性 cached block assembler。 |
| `BREWFS_TRACE_CHROME` | 无 | profiling build 下输出 Chrome trace。 |
| `BREWFS_TRACE_FLAME` | 无 | profiling build 下输出 tracing-flame folded stack。 |
| `TOKIO_CONSOLE` | 无 | profiling build 下启用 tokio-console。 |
| `BREWFS_CONSOLE_TOKEN` | 无 | console API bearer token；也可用 `--auth-token-file`。 |

## 部署建议

单机开发：

- `data.backend=local-fs`
- `meta.backend=sqlx`
- SQLite URL 使用持久文件，例如 `sqlite:///var/lib/brewfs/meta.db?mode=rwc`

单机生产或性能测试：

- `data.backend=s3` 指向本机 RustFS/MinIO
- `meta.backend=redis`
- `data.s3.force_path_style=true`
- `data.s3.disable_payload_checksum=true`
- cache root 放到可靠 SSD
- 如启用 `commit_before_upload`，必须理解本地写回缓存丢失风险
- 大读吞吐测试可用 `docker/compose-xfstests/run_redis_perf.sh --read-throughput-profile`，该 profile 会设置 `BREWFS_FUSE_READ_DIRECT_IO=1`，减少 buffered FUSE 大读拆分；它只适合性能 profile，因为对应只读 fd 会禁用 mmap。
- 大写吞吐测试可用 `docker/compose-xfstests/run_redis_perf.sh --bigwrite-throughput-profile --tools fio-bigwrite`。该 profile 是 `--writeback-throughput-profile` 的明确别名，会启用 `commit_before_upload`、更大的本地读写缓存和写回队列，并在 fio 后等待写回 drain；它适合验证 `fio-bigwrite` 上限，不建议替代默认 `upload_before_commit` 语义。
- 在本机 Redis + RustFS 对照中，默认 `fio-bigwrite` 约 528 MiB/s，JuiceFS 约 703 MiB/s，`--bigwrite-throughput-profile` 约 774 MiB/s。这个结果说明 BrewFS 写路径有足够吞吐空间，但收益来自显式写回 profile，而不是无风险默认参数调整。

多节点或分布式部署：

- 使用 Redis、Etcd 或 TiKV 作为元数据后端。
- 所有客户端必须使用同一个对象 bucket 和元数据 namespace。
- 默认 FUSE TTL 为 1s。需要更强多客户端可见性时，把 `BREWFS_CACHE_TTL_MS=0` 放进 systemd environment；需要更高 metadata-heavy 性能时保留默认值。
- 每个客户端使用独立 `cache.root`，不要共享本地缓存目录。

更多可直接运行的样例见 `examples/mount-config*.yaml`。
