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
  slice_version_check_interval_ms: 1000

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
| `--s3-rustfs-ec-block-size-hint <auto或BYTES>` | 需要编译 feature `rustfs-ec-block-size-hint`；`auto` 按最终对象大小选择，或固定为 `65536`、`262144`、`1048576`、`4194304`，默认关闭，只用于 S3 后端。 |
| `--meta-backend <sqlx|redis|etcd|tikv>` | 元数据后端。 |
| `--meta-url <URL>` | SQLx 或 Redis URL。 |
| `--meta-etcd-urls <URLS>` | 逗号分隔的 Etcd endpoint。 |
| `--meta-tikv-pd-endpoints <URLS>` | 逗号分隔的 TiKV PD endpoint。 |
| `--meta-tikv-namespace <NAMESPACE>` | TiKV key namespace，默认 `brewfs`。 |
| `--chunk-size <BYTES>` | chunk 大小，默认 64 MiB。 |
| `--block-size <BYTES>` | block 大小，默认 4 MiB。 |
| `--fuse-workers <N>` | FUSE worker 数；默认 `clamp(2 × available_parallelism, 1, 64)`，CPU 探测失败时为 `1`；显式 `0` 或 `1` 使用低开销 session dispatch。 |
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
| `data.s3.rustfs_ec_block_size_hint` | 关闭 | 需要编译 feature `rustfs-ec-block-size-hint`；支持 `auto` 或固定 bytes：`65536`、`262144`、`1048576`、`4194304`。 |

S3 凭据走 AWS SDK 标准环境变量和配置文件，例如：

```bash
export AWS_ACCESS_KEY_ID=rustfsadmin
export AWS_SECRET_ACCESS_KEY=rustfsadmin
export AWS_DEFAULT_REGION=us-east-1
export AWS_EC2_METADATA_DISABLED=true
```

#### RustFS EC 块大小提示

该功能由 Cargo feature `rustfs-ec-block-size-hint` 控制，默认编译关闭。需要先构建启用该 feature 的 BrewFS：

```bash
cargo build --release --features rustfs-ec-block-size-hint
```

若部署内核不支持默认 io_uring FUSE 路径所要求的同步取消（本次 ACS 5.10 内核返回 `Invalid argument`），使用已有的 Tokio 运行时：

```bash
cargo build --release --no-default-features --features fuse-tokio-runtime,rustfs-ec-block-size-hint
```

需要网关时再同时启用 `gateway-s3,gateway-webdav`。这是运行时兼容选择，自动 hint 规则相同。

未启用 feature 的二进制不包含提示发送逻辑，CLI 帮助中隐藏该参数；显式使用 CLI、YAML 或 SDK 配置提示会报 `feature not compiled: rustfs-ec-block-size-hint`，不会静默忽略。启用 feature 后，仍需显式配置 `auto` 或固定提示；省略时不发送提示。

在支持 Dynamic Block Size 扩展的 RustFS 上，可为新上传的 S3 对象启用自动选择。例如：

```yaml
data:
  backend: s3
  s3:
    bucket: brewfs-data
    endpoint: http://127.0.0.1:9000
    region: us-east-1
    force_path_style: true
    rustfs_ec_block_size_hint: auto
```

CLI 的 `--s3-rustfs-ec-block-size-hint` 优先于 YAML。未配置时不发送提示；关闭时删除 YAML 字段并省略 CLI 参数。`0` 和其他未列出的尺寸会被拒绝，`local-fs` 后端也不接受此字段。

提示通过 S3 用户元数据头 `x-amz-meta-rustfs-ec-block-size-hint` 随普通 PUT、create-only PUT 和 multipart initiation 发送。它是 RustFS 扩展，不是 S3 标准的 EC 布局控制参数；只有支持且启用了提示处理的服务端才会解释它。服务端决定实际采用的布局，已有对象不会因为挂载配置变化而重新编码。

`auto` 是显式启用的整对象读写策略：最终 S3 对象 **≤ 1 MiB 不发送 hint**，由 RustFS 保留默认及内联策略；**> 1 MiB 请求 1 MiB EC 块**。ACS 上相同客户端与服务端的整文件对照没有支持旧策略自动选择 4 MiB EC，故按实测修正。不会自动选择 64/256 KiB 小块；固定值仍可用于显式覆盖和实验。该策略主要配合完整 GET 合并与缓存路径，不代表改变 EC 数值本身就能提升所有负载。技术细节及证据见 [Innovations](../../innovations/README.md)。

此字段与 BrewFS 的 `layout.block_size`、`data.s3.part_size` 和服务端校验片大小分别控制不同层次。自动选择使用上传入口已经定型的对象字节数，包括压缩结果、补零和 4 字节持久化格式头；普通上传、后台上传与恢复上传共用此入口，不读取正在增长的 inode 长度、SSD 预分配空间或全局缓存占用，也不等待整个文件关闭。multipart 在创建时使用整个对象大小，part 请求不重复发送 hint。选择固定模式时不再按对象大小推断；CLI 始终优先于 YAML。

例如，不压缩的 1 MiB 数据块带格式头后会超过阈值；4 MiB 的高可压缩数据若压缩到 1 MiB 以下则不发送 hint；一个大文件的短尾块可使用不同于前面整块的 hint。BrewFS 文件切分和压缩格式不变。完整设计与测试标准见 [自动提示设计](../testing/rustfs-auto-object-hint.md)。

直接上传诊断可绕过 FUSE 检查 S3 写入路径：

```bash
brewfs object-put-bench --s3-bucket brewfs-hint-test \
  --s3-endpoint http://127.0.0.1:9000 \
  --s3-rustfs-ec-block-size-hint 1048576 \
  --object-size 4194304 --workers 1 --objects 20 --duration-secs 0
```

该命令上传测试对象，不能替代文件系统正确性或端到端性能测试。另有显式启用的实时回归，覆盖上传、HEAD 中的实际 EC 块大小及完整/Range 读取。运行前需要隔离的 Dynamic Block RustFS、已创建的测试 bucket，以及已启用的动态块和客户端提示开关；凭据使用上方 AWS SDK 配置：

```bash
BREWFS_S3_ENDPOINT=http://127.0.0.1:9000 \
BREWFS_S3_BUCKET=brewfs-hint-test \
  cargo test --features rustfs-ec-block-size-hint --lib cadapter::s3::tests::rustfs_ec_hint_live_upload_head_and_range \
  -- --ignored --exact
```

### meta

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `meta.backend` | `sqlx` | `sqlx`、`redis`、`etcd` 或 `tikv`。 |
| `meta.sqlx.url` | `sqlite::memory:` | SQLite 或 PostgreSQL URL。 |
| `meta.redis.url` | 无 | 单一 Redis 7+ endpoint URL；`backend=redis` 时必须显式配置。原子命名空间操作需要 Lua ACL 预检，启动时拒绝旧版本。当前元数据后端依赖跨 key Lua 原子事务，不支持 Redis Cluster。 |
| `meta.etcd.urls` | `[]` | Etcd endpoint 列表。 |
| `meta.tikv.pd_endpoints` | `[]` | TiKV PD endpoint 列表。 |
| `meta.tikv.namespace` | `brewfs` | TiKV key namespace。 |
| `meta.open_file_cache_ttl_ms` | 关闭 | 只读 open 文件属性缓存 TTL，单位 ms。 |
| `meta.open_file_cache_capacity` | 默认值 | open file cache 容量。 |
| `meta.allow_write_open_cache` | `false` | 允许写 open 复用属性缓存。仅建议在单客户端或可接受跨客户端 close-to-open 新鲜度减弱的性能场景中启用。 |
| `meta.slice_version_check_interval_ms` | `1000` | Redis slice-list 缓存的后端版本复检间隔。远端 compact/write 最多可在该窗口内暂未被当前客户端看到；设为 `0` 会在每次缓存命中时复检。与复检重叠的变更可能在下一次复检时可见。 |

Redis 后端的 `statfs`/`df` 是每个客户端独立维护的近似快照，固定缓存 5 秒。`create`、`unlink`、`write`、`truncate` 等本地或远端变更可能要到该窗口过期后的第一次查询才会反映到 `used_space` / `used_inodes`；这是为了避免重复 SCAN 全 inode 空间以及在每次元数据变更上增加全局 epoch 写入。

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
| `fuse.workers` | `clamp(2 × available_parallelism, 1, 64)` | 根据 CPU affinity / cgroup quota 可用并行度计算，探测失败时为 `1`；显式 `0` 或 `1` 使用低开销 session dispatch。 |
| `fuse.max_background` | `512` | asyncfuse worker 模式下最大 queued/running 请求数。 |
| `fuse.cache_ttl_ms` | `0` | 显式放宽可写 POSIX ACL 挂载的内核权限/属性/目录项缓存 TTL；CLI `--fuse-cache-ttl-ms` 优先，含显式 `0`。详见 [CONFIG.md](../../CONFIG.md) 中的性能和权限陈旧窗口。 |
| `fuse.privileged` | `false` | 直接使用 `/dev/fuse`，通常用于 root/systemd/container。 |

优先级为 `--fuse-workers` > YAML `fuse.workers` > 自动计算的默认值。perf runner 和 installer 将 `BREWFS_FUSE_WORKERS` 写入 YAML；挂载程序不直接读取这个环境变量。显式配置的 worker 数不受默认值的 `64` 上限限制。

非特权挂载依赖 `fusermount3`；特权挂载依赖进程有权限访问 `/dev/fuse`。

The `brewfs` executable uses one Tokio worker thread by default. Set
`TOKIO_WORKER_THREADS` to a positive integer (for example, `8`) to override
the executor size. This is separate from `fuse.workers`; blocking I/O still
uses Tokio's blocking pool. See [the measured scope and caveats](../testing/tokio-worker-default.md).

### cache

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `cache.root` / `cache.cache_root` | `$XDG_CACHE_HOME/brewfs` 或 `/tmp/brewfs` | 本地缓存根目录。 |
| `cache.read_memory_bytes` | `8589934592` | 读缓存内存预算，默认 8 GiB。 |
| `cache.read_ssd_bytes` | `68719476736` | 读缓存磁盘预算，默认 64 GiB。 |
| `cache.write_memory_bytes` | `805306368` | 写缓存内存预算，默认 768 MiB。 |
| `cache.write_ssd_bytes` | `68719476736` | 写缓存磁盘预算，默认 64 GiB。 |
| `cache.dirty_slice_target_size` | `33554432` | 脏 slice 聚合目标，默认 32 MiB。 |
| `cache.dirty_slice_max_age_ms` | `2000` | 脏 slice 最大聚合时间。 |
| `cache.upload_concurrency` | `10` | 单 writer 内 block upload 并发。 |
| `cache.prefetch_enabled` | `true` | VFS 顺序预取开关。 |
| `cache.prefetch_max_bytes` | `134217728` | 最大预读距离，默认 128 MiB。 |
| `cache.prefetch_concurrency` | `64` | 预取并发。 |
| `cache.range_background_prefetch` | `true` | range miss 后后台补全 block。 |
| `cache.populate_write_cache_after_upload` | `true` | 上传后把写入 block 放入读缓存。 |
| `cache.persist_write_cache_after_upload` | `false` | 上传后是否持久化到磁盘读缓存。 |
| `cache.memory_budget_bytes` | `2147483648` | VFS reader/writer buffer 总预算，默认 2 GiB。 |
| `BREWFS_PACKED_METADATA_CACHE_BYTES` | `268435456` | packed v3 解码元数据总预算；按 index page、GroupMeta、frame directory、inode 热项和 locator 分层，不写入 `cache.root`。 |
| `BREWFS_PACKED_METADATA_PREFETCH` | `auto` | packed v3 挂载前预热模式：默认 `auto` 按独立 index 子预算预热稳定页前缀，再按 GroupMeta 和 frame-directory byte budget 预热可驻留的 group/container；`eager`/`true` 并行预热指定 GroupMeta，`off` 保留严格 metadata-cold 请求图。超预算的尾部页/group/container 留给按需 LRU；数据 frame 仍按请求读取。 |
| `BREWFS_PACKED_METADATA_PREFETCH_CONCURRENCY` | `8` | metadata warm-up 最大并发。 |
| `BREWFS_PACKED_METADATA_PREFETCH_MAX_GROUPS` | 未设置 | 预热的 GroupMeta 数量上限；未设置表示按索引预热全部 group，`0` 表示只预热 index。 |
| block page cache | `32768 x 65536` | 普通小范围数据读的 page cache，默认约 2 GiB；冷读 profile 会显式禁用。 |
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
