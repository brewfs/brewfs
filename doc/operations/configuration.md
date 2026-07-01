# 配置与部署

BrewFS 是单二进制程序，通过 CLI 参数、YAML 配置文件和环境变量控制所有行为。

源码位置：`src/config.rs`、`src/main.rs`

## CLI 命令

```
brewfs <SUBCOMMAND>
```

三个子命令：

| 命令 | 说明 |
|---|---|
| `brewfs mount [OPTIONS] [MOUNT_POINT]` | FUSE 挂载 |
| `brewfs gc [MOUNT_POINT] [--dry-run]` | 触发 GC（与运行中的 daemon 通信） |
| `brewfs info [MOUNT_POINT]` | 查看挂载信息 |

### mount 参数

```
brewfs mount [MOUNT_POINT] \
  --config <FILE>                     # YAML 配置文件
  --data-backend <local-fs|s3>        # 数据后端
  --data-dir <DIR>                    # LocalFS 数据目录
  --s3-bucket <BUCKET>                # S3 桶
  --s3-endpoint <URL>                 # S3 兼容端点
  --s3-region <REGION>                # S3 区域
  --s3-part-size <BYTES>             # S3 multipart 分片大小 (默认 16 MiB)
  --s3-max-concurrency <N>           # S3 最大并发 (默认 32)
  --s3-force-path-style              # 强制 path-style 访问
  --s3-disable-payload-checksum      # 禁用 S3 payload SHA-256 签名
  --meta-backend <sqlx|etcd|redis>   # 元数据后端
  --meta-url <URL>                    # 元数据 URL
  --meta-etcd-urls <URL1,URL2,...>   # Etcd 端点
  --chunk-size <BYTES>               # Chunk 大小 (默认 64 MiB)
  --block-size <BYTES>               # Block 大小 (默认 4 MiB)
  --fuse-workers <N>                  # FUSE worker 数 (默认 1)
  --fuse-max-background <N>          # FUSE 最大排队请求 (默认 512)
  --privileged                       # 特权挂载模式 (使用 /dev/fuse)
```

### mount YAML 配置

所有 CLI 参数都可以通过 YAML 文件配置：

```yaml
mount_point: /mnt/brewfs

data:
  backend: local-fs    # 或 s3
  localfs:
    data_dir: /data/brewfs
  s3:
    bucket: my-bucket
    endpoint: https://s3.example.com
    region: us-east-1
    part_size: 16777216        # 16 MiB
    max_concurrency: 32
    force_path_style: true
    disable_payload_checksum: false

meta:
  backend: sqlx         # 或 etcd / redis
  sqlx:
    url: "sqlite:///var/lib/brewfs/meta.db"
  # 或:
  # etcd:
  #   urls:
  #     - http://10.0.0.1:2379
  #     - http://10.0.0.2:2379
  # 或:
  # redis:
  #   url: "redis://127.0.0.1:6379/0"

layout:
  chunk_size: 67108864          # 64 MiB
  block_size: 4194304            # 4 MiB

fuse:
  workers: 4
  max_background: 64
  privileged: false

cache:
  root: /var/cache/brewfs
  read_memory_bytes: 4294967296       # 4 GiB
  read_ssd_bytes: 10737418240         # 10 GiB (磁盘冷缓存)
  write_memory_bytes: 1073741824      # 1 GiB
  write_ssd_bytes: 0
  dirty_slice_target_size: 67108864   # 64 MiB
  dirty_slice_max_age_ms: 500
  prefetch_enabled: true
  prefetch_max_bytes: 268435456       # 256 MiB
  prefetch_concurrency: 2
  memory_budget_bytes: 8589934592     # 8 GiB
  compression: none                   # none / lz4 / zstd
  zstd_level: 3
  bandwidth:
    upload_limit_mibps: 100
    download_limit_mibps: 200
```

CLI 参数优先级高于 YAML 配置。例如配置文件指定了 `chunk_size: 67108864`，但 CLI 传了 `--chunk-size 134217728`，则使用 128 MiB。

## 数据后端

### LocalFS

以本地目录模拟对象存储，适用于单机开发和测试：

```bash
brewfs mount /mnt/brewfs \
  --data-backend local-fs \
  --data-dir /tmp/brewfs-data \
  --meta-url sqlite:///tmp/brewfs/meta.db
```

Block 数据以 `chunks/{slice_id}/{block_index}` 的目录结构存储在 `data-dir` 下。

### S3

接入任何 S3 兼容的对象存储（AWS S3、MinIO、Ceph RGW 等）：

```bash
brewfs mount /mnt/brewfs \
  --data-backend s3 \
  --s3-bucket my-bucket \
  --s3-endpoint https://s3.example.com \
  --meta-backend redis \
  --meta-url redis://10.0.0.1:6379/0
```

S3 相关参数说明：

| 参数 | 默认值 | 说明 |
|---|---|---|
| `s3-part-size` | 16 MiB | Multipart upload 的 part 大小 |
| `s3-max-concurrency` | 32 | Multipart upload 的并发 part 数 |
| `s3-force-path-style` | false | MinIO 等需要 path-style 访问时必须开启 |
| `s3-disable-payload-checksum` | true | 禁用 SigV4 payload SHA-256 签名（减少 ~20% CPU） |

## 元数据后端

### SQLx (DatabaseMetaStore)

支持 SQLite 和 PostgreSQL：

```bash
# SQLite (开发/单机)
--meta-backend sqlx --meta-url "sqlite:///path/to/meta.db"
# SQLite 内存 (测试)
--meta-backend sqlx --meta-url "sqlite::memory:"
# PostgreSQL (生产)
--meta-backend sqlx --meta-url "postgresql://user:pass@host:5432/meta"
```

### Etcd (EtcdMetaStore)

用于分布式部署，提供 KV 存储 + watch + 事务：

```bash
--meta-backend etcd --meta-etcd-urls http://etcd-1:2379,http://etcd-2:2379
```

需要至少一个 etcd 3.x 集群。

### Redis (RedisMetaStore)

高性能 KV 存储，适合对延迟敏感的元数据操作：

```bash
--meta-backend redis --meta-url redis://127.0.0.1:6379/0
```

使用 Lua CAS 保证并发安全（详见 `doc/architecture/redis-version-cas.md`）。

## FUSE 并发配置

| 参数 | 默认值 | 说明 |
|---|---|---|
| `fuse-workers` | 1 | asyncfuse worker pool 大小。0 或 1 使用低开销 session dispatch |
| `fuse-max-background` | 512 | 排队 + 执行中的 FUSE 请求最大数 |

worker pool 模式（`workers > 1`）下，FUSE 请求由 worker 线程池并发处理，适合需要额外 FUSE 并发的 IO 场景；metadata-heavy workload 默认保留低调度开销路径。

## 挂载模式

### 非特权模式（默认）

使用 `fusermount3`，要求用户在 `fuse` 组中：

```bash
brewfs mount /mnt/brewfs ...
```

### 特权模式

直接使用 `/dev/fuse`，需要 root 权限：

```bash
sudo brewfs mount /mnt/brewfs --privileged ...
```

## 环境变量

| 变量 | 说明 |
|---|---|
| `RUST_LOG` | 日志级别控制（`brewfs=info`, `brewfs=trace` 等） |
| `BREWFS_LOG_FILE` | 主日志输出文件路径 |
| `BREWFS_FUSE_LOG_FILE` | FUSE 操作日志单独输出路径 |
| `BREWFS_TRACE_CHROME` | Chrome trace 输出路径 |
| `BREWFS_TRACE_FLAME` | tracing-flame 输出路径 |
| `TOKIO_CONSOLE` | 设为任意值启用 tokio-console |
| `BREWFS_NOFILE_LIMIT` | 自定义 nofile 限制（默认 1,048,576） |

## 元数据后端测试环境搭建

`doc/testing/docker-compose-test-guide.md` 提供了 Docker Compose 快速搭建测试环境的说明：

```bash
# 使用仓库中的 docker-compose.yml 启动所有服务
docker compose up -d

# 运行各后端测试
cargo test --lib meta::stores::redis_store -- --nocapture
cargo test --lib meta::stores::etcd_store -- --nocapture
cargo test --lib meta::stores::database_store -- --nocapture

# 停止
docker compose down
```

测试服务使用默认凭据，仅适用于本地开发，不可用于生产环境。

## 构建特性 (Features)

| Feature | 说明 |
|---|---|
| `jemalloc` | 使用 jemalloc 作为全局内存分配器（Linux only） |
| `jemalloc-profiling` | 启用 jemalloc heap profiling |
| `profiling` | 启用 tracing-flame + tracing-chrome 支持 |

```bash
# 生产构建（含 jemalloc）
cargo build --release --features jemalloc

# 性能分析构建
cargo build --release --features jemalloc-profiling
```

## Daemon 控制面 (Control Plane)

运行中的 brewfs 进程通过 Unix domain socket 提供控制面接口：

```
/run/brewfs/<mount_hash>/control.sock
```

`brewfs gc` 和 `brewfs info` 命令通过此 socket 与 daemon 通信：

```bash
# 查看挂载信息
brewfs info /mnt/brewfs

# 触发 GC (扫描)
brewfs gc /mnt/brewfs --dry-run

# 触发 GC (实际删除)
brewfs gc /mnt/brewfs
```

源码：`src/control/` — 定义了请求/响应协议、任务生命周期管理和 socket 通信。
