<div align="center">
  <img src="doc/assets/brewfs.png" alt="BrewFS" width="366" height="167" />
  <p><strong>BrewFS：基于 Rust 的高性能分布式存储</strong></p>

  <p>
    <a href="https://github.com/brewfs/brewfs/actions/workflows/ci.yml"><img src="https://github.com/brewfs/brewfs/actions/workflows/ci.yml/badge.svg" alt="CI" /></a>
    <a href="https://github.com/brewfs/brewfs/releases"><img src="https://img.shields.io/github/v/release/brewfs/brewfs" alt="Release" /></a>
    <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/language-Rust-orange.svg" alt="Rust" /></a>
    <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT license" /></a>
  </p>
  <p>
    <a href="#quick-start">安装</a> ·
    <a href="#performance-vs-juicefs">性能对比</a> ·
    <a href="doc/architecture/arch.md">架构</a> ·
    <a href="doc/README.md">文档</a> ·
    <a href="README.md">English</a>
  </p>
</div>

BrewFS 是一个面向容器、AI 与对象存储密集场景的独立分布式文件系统。它结合了类 POSIX 的 FUSE 接口、可插拔的事务型元数据后端，以及兼容 S3 的对象数据存储。BrewFS 与 [RustFS 社区](https://github.com/rustfs/rustfs) 共同开发，持续保持对以对象存储为基础的分布式工作负载的兼容性。

<p align="center">
  <a href="https://github.com/rustfs/rustfs">
    <img src="doc/assets/rustfs.png" alt="RustFS" width="220" height="60" />
  </a>
  <a href="https://github.com/rustfs/rustfs">
    <img src="https://img.shields.io/github/stars/rustfs/rustfs?style=flat-square" alt="RustFS GitHub stars" />
  </a>
</p>

在当前 Redis + RustFS 基准测试下，BrewFS 对比 [JuiceFS](https://juicefs.com/) ([GitHub](https://github.com/juicedata/juicefs)) 实现了 **2.45x 随机读吞吐**、**2.62x 文件创建吞吐**、以及约 **12x 混合随机 I/O 吞吐**。完整结果包含双方持平或 JuiceFS 优势场景，见下方。

| **2.45x** | **2.62x** | **12.09x** | **1.17x** |
| :---: | :---: | :---: | :---: |
| random read | file create | mixed random read | tuned large write |

![Selected BrewFS benchmark wins relative to JuiceFS](doc/assets/performance-vs-juicefs.svg)

## 为什么选择 BrewFS

- **快速数据面：** 分块 I/O、内存与 SSD 缓存、预读、writeback，以及大块写入聚合。
- **Rust 全栈：** 从 FUSE/VFS 到元数据与对象存储，全栈使用 Rust 构建。
- **存储灵活性：** 元数据可选 Redis、TiKV、etcd、PostgreSQL 或 SQLite；对象数据支持兼容 S3 的对象存储或本地后端。
- **可测性：** 仓库内置 xfstests、pjdfstest、LTP、stress-ng、fio、元数据基准、fuzz 和 Docker Compose 运行脚本，可直接复现覆盖。

## Performance vs JuiceFS

以下快照采集于同一台主机，使用 Redis 元数据与 RustFS S3 兼容对象存储，关闭压缩，并保持 fio 负载形态一致。fio 使用 buffered I/O（`direct=0`），BrewFS 在读重场景启用了 direct-I/O FUSE handle。

### 测试环境（本机）

- **CPU：** Intel Xeon Platinum（x86_64，1 路 / 8 vCPU，2 threads per core）
- **内存：** 16 GiB
- **内核：** Linux 6.8.0-117-generic
- **系统：** Ubuntu-based Linux 内核镜像（GNU/Linux）
- **存储：** 150GB aliyun ESSD AutoPL（8300 IOPS）

每次运行的环境文件与完整日志保存在 `docker/compose-xfstests/artifacts/<run>/`，包含 profile 原始配置与生成报告。

### 数据吞吐

| 负载 | BrewFS | JuiceFS | BrewFS / JuiceFS |
| --- | ---: | ---: | ---: |
| Large write | **820.5 MiB/s** | 702.8 MiB/s | **1.17x** |
| Large read | **2.36 GiB/s** | 1.03 GiB/s | **2.29x** |
| Sequential read | **1.77 GiB/s** | 1.12 GiB/s | **1.57x** |
| Sequential write | 198.5 MiB/s | **204.7 MiB/s** | 0.97x |
| Random read | **2.81 GiB/s** | 1.15 GiB/s | **2.45x** |
| Random write | 200.9 MiB/s | **205.2 MiB/s** | 0.98x |
| Mixed random read | **392.4 MiB/s** | 32.5 MiB/s | **12.09x** |
| Mixed random write | **174.5 MiB/s** | 14.7 MiB/s | **11.86x** |

### 元数据吞吐

| 操作 | BrewFS | JuiceFS | BrewFS / JuiceFS |
| --- | ---: | ---: | ---: |
| Create | **992.7 ops/s** | 378.8 ops/s | **2.62x** |
| Open | 5,185.2 ops/s | **6,025.0 ops/s** | 0.86x |
| Stat | **684,626.9 ops/s** | 683,779.5 ops/s | 1.00x |
| Readdir | **34,624.8 ops/s** | 21,420.4 ops/s | **1.62x** |
| Rename | 1,014.9 ops/s | **1,341.6 ops/s** | 0.76x |

<details>
<summary><strong>延迟、时长与基准细节</strong></summary>

| 负载 | BrewFS wall / active | JuiceFS wall / active | BrewFS p99 | JuiceFS p99 |
| --- | ---: | ---: | ---: | ---: |
| Large write | 2s / 1.248s | 3s / 1.457s | W 67.6ms | W 65.3ms |
| Large read | 1s / 0.424s | 2s / 0.973s | R 0.0ms | R 97.0ms |
| Sequential read | 60s / 60.001s | 61s / 60.001s | R 0.0ms | R 4.4ms |
| Sequential write | 63s / 60.002s | 62s / 60.051s | W 42.2ms | W 120.1ms |
| Random read | 60s / 60.004s | 60s / 60.008s | R 0.0ms | R 51.6ms |
| Random write | 65s / 60.016s | 62s / 60.091s | W 254.8ms | W 215.0ms |
| Mixed random I/O | 65s / 60.034s | 64s / 61.734s | R 107.5ms / W 219.2ms | R 3170.9ms / W 346.0ms |

| 元数据操作 | BrewFS 延迟 | JuiceFS 延迟 |
| --- | ---: | ---: |
| Create | **1,007.4 us/op** | 2,639.9 us/op |
| Open | 192.9 us/op | **166.0 us/op** |
| Stat | **1.5 us/op** | **1.5 us/op** |
| Readdir | **28.9 us/op** | 46.7 us/op |
| Rename | 985.3 us/op | **745.4 us/op** |

| 工具 | BrewFS wall | JuiceFS wall | 结果 |
| --- | ---: | ---: | --- |
| `dirstress` | **1s** | 3s | pass / pass |
| `dirperf` | 16s | **14s** | pass / pass |
| `metaperf` | 205s | **193s** | pass / pass |
| `looptest` | 2s | **1s** | pass / pass |

BrewFS 的 large-write 数据来自可选 `--bigwrite-throughput-profile`，该 profile 会开启 `commit_before_upload`；默认更稳妥配置为 `upload_before_commit`。JuiceFS 使用 `JFS_WRITEBACK=false`。JuiceFS 的 mixed random-I/O 结果已重跑两次，保持在相似区间内。

这是 2026 年 7 月 9-10 日的本地工程级快照，不可视作全部部署场景下的普适结论。每次运行都会将 fio JSON、环境信息、日志与生成报告写入 `.gitignore` 的 `docker/compose-xfstests/artifacts/` 目录，便于本地复核。

</details>

可按以下命令复现实验：

```bash
BREWFS_COMPRESSION=none \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 --read-throughput-profile
bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --bigwrite-throughput-profile --tools fio-bigwrite
JFS_COMPRESS=none JFS_WRITEBACK=false \
  bash docker/compose-xfstests/run_juicefs_perf.sh
```

## Quick Start

在 Linux 单机环境可使用脚本一键安装包含 Redis、RustFS、systemd 与 BrewFS FUSE 挂载的完整栈：

```bash
curl -fsSL https://raw.githubusercontent.com/brewfs/brewfs/main/scripts/install_brewfs_single_node.sh \
  | sudo bash -s -- install
```

或使用源码构建（Rust 1.85+、`fuse3`）：

```bash
cargo build -p brewfs --release

mkdir -p /tmp/brewfs-mnt /tmp/brewfs-data
target/release/brewfs mount /tmp/brewfs-mnt \
  --data-backend local-fs \
  --data-dir /tmp/brewfs-data \
  --meta-backend sqlx \
  --meta-url sqlite:///tmp/brewfs-meta.db
```

生产部署、调优 profile、升级与卸载参数请参考：[二进制部署文档](doc/operations/binary-deployment.md) 与 [配置说明](doc/operations/configuration.md)。

## Architecture

BrewFS 将文件系统接口、元数据与对象数据通道解耦：

- **FUSE + VFS** 提供基于 inode 的 POSIX 语义。
- **元数据层**：在 Redis、TiKV、etcd、PostgreSQL 或 SQLite 中维护 namespace、属性、切片、session 与事务。
- **Chunk + 缓存层**：按 64 MiB chunk / 4 MiB block 组织数据，支持内存与 SSD 缓存、compaction 与垃圾回收。
- **对象后端**：将块持久化到 RustFS、MinIO、AWS S3、Ceph RGW 等兼容 S3 的对象系统，或本地存储。

核心能力包括创建、读、写、truncate、稀疏文件、重命名、硬链接、符号链接、字节区锁（byte-range lock）、compaction、延迟删除，以及运行期 `info` / `gc` 管控命令。

## Test It

```bash
cargo test -p brewfs

cd docker
bash compose-xfstests/run_redis_xfstests.sh --cases "generic/001"
bash compose-xfstests/run_redis_pjdfstest.sh
```

测试说明见 [Docker Compose 测试手册](doc/testing/docker-compose-test-guide.md)，包含 Redis、TiKV、RustFS、xfstests、pjdfstest、LTP、stress-ng 与性能跑分脚本。

## Documentation

- [文档索引](doc/README.md)
- [架构设计](doc/architecture/arch.md)
- [配置说明](doc/operations/configuration.md)
- [二进制部署](doc/operations/binary-deployment.md)
- [基准测试指南](doc/testing/bench.md)
- [BrewFS/JuiceFS 差距分析](doc/gap/README.md)

## Contributing

欢迎提交 Issue 与 PR。建议将行为变更、测试与文档一并提交，以降低回归风险。
