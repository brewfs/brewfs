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

在当前与 [JuiceFS](https://juicefs.com/)（[GitHub](https://github.com/juicedata/juicefs)）同机、同后端且完整排空的对比中，BrewFS 的数据面无权几何平均为 **1.74x**，全部 12 项数据与元数据操作的无权几何平均为 **1.38x**。其中完整排空的大文件写入为 **3.18x**、随机读取为 **3.10x**、完整排空的混合 I/O 为 **3.75x**、readdir 为 **1.96x**；下方也完整保留了 JuiceFS 更快的项目。

![BrewFS 与 JuiceFS 的 12 项同配置性能对比](doc/assets/performance-vs-juicefs.svg)

## 为什么选择 BrewFS

- **快速数据面：** 分块 I/O、内存与 SSD 缓存、预读、writeback，以及大块写入聚合。
- **Rust 全栈：** 从 FUSE/VFS 到元数据与对象存储，全栈使用 Rust 构建。
- **存储灵活性：** 元数据可选 Redis、TiKV、etcd、PostgreSQL 或 SQLite；对象数据支持兼容 S3 的对象存储或本地后端。
- **可测性：** 仓库内置 xfstests、pjdfstest、LTP、stress-ng、fio、元数据基准、fuzz 和 Docker Compose 运行脚本，可直接复现覆盖。

## Performance vs JuiceFS

以下快照采集于同一台主机，双方都使用 Redis 元数据、RustFS S3 兼容对象存储、关闭压缩，并启用各自 runner 的 `--writeback-throughput-profile`。fio 使用 buffered I/O（`direct=0`）、`io_uring`、4 MiB block 与 `iodepth=1`；large read/write 使用 8 个 128 MiB job（合计 1 GiB）。读取测试在预填充并排空后重新挂载和清理文件系统缓存，写入测试结束后显式等待完整排空。BrewFS 在该 profile 中为读取保留 direct-I/O FUSE handle，这项实现差异没有被隐藏。

### 测试环境（本机）

- **CPU：** Intel Xeon Platinum（x86_64，1 路 / 8 vCPU，2 threads per core）
- **内存：** 基准主机可用 14 GiB
- **内核：** Linux 6.8.0-117-generic
- **系统：** Ubuntu-based Linux 内核镜像（GNU/Linux）
- **存储：** 130 GiB 虚拟块设备

完整运行 artifact 分别为 `perf-run-1784459867-21061`（BrewFS 数据面）、`perf-run-1784461564-23252`（BrewFS 元数据）和 `juicefs-perf-run-1784386826-2853`（JuiceFS）。优化后的 large-read 行取 focused rerun `perf-run-1784469566-30242` 与 `perf-run-1784469601-26934` 的均值；large-write 行使用 `perf-run-1784473152-32564` 与 `perf-run-1784473176-9768`。每份 artifact 都在 `docker/compose-xfstests/artifacts/<run>/` 中保留 profile 环境、fio JSON 或工具日志、诊断信息、警告和生成报告。

实际生效参数如下：

| 系统 | 实际配置 |
| --- | --- |
| BrewFS | `commit_before_upload`；读写各 4 GiB 内存/SSD cache；12 GiB 内存预算；6 个 writeback upload worker；S3 concurrency 16；upload concurrency 32；1 秒 / 65,536 项 metadata open cache，关闭可写 handle 复用；16 个 FUSE worker；`max_background=512`；启用 async-fuse request buffer pool |
| JuiceFS 1.3.1 | `writeback=true`；8 GiB buffer；4 GiB 本地 cache；4 个 upload worker；1 秒 / 65,536 项 open cache；关闭 metadata backup 和 usage reporting。该版本不支持请求的 `max-downloads=16`，因此没有传给 mount。 |

对于单客户端 build 或频繁以 `O_RDWR` 打开文件的元数据负载，可为 BrewFS 命令增加 `--metadata-throughput-profile`。它会在 1 秒 open cache 上启用 `BREWFS_METADATA_ALLOW_WRITE_OPEN_CACHE=true`；最近一次匹配的 `metaperf` 中 open 从 4,989.0 提升到 5,589.9 ops/s（+12%）。这会削弱多客户端 close-to-open 新鲜度，因此没有用于下方对比。

### 数据吞吐

前台表记录 workload 正在提交 I/O 时 fio 看到的带宽，表示应用可见的接收速度；fio 结束时，双方都可能仍有数据位于 writeback 队列。

| 负载 | BrewFS | JuiceFS | BrewFS / JuiceFS |
| --- | ---: | ---: | ---: |
| Large write | 912.3 MiB/s | **1.07 GiB/s** | 0.83x |
| Large read | 743.7 MiB/s | **936.9 MiB/s** | 0.79x |
| Sequential read | **1.57 GiB/s** | 1.04 GiB/s | **1.52x** |
| Sequential write | 146.5 MiB/s | **280.6 MiB/s** | 0.52x |
| Random read | **3.75 GiB/s** | 1.21 GiB/s | **3.10x** |
| Random write | 127.9 MiB/s | **312.9 MiB/s** | 0.41x |
| Mixed random read | **237.7 MiB/s** | 119.3 MiB/s | **1.99x** |
| Mixed random write | **108.1 MiB/s** | 55.7 MiB/s | **1.94x** |

对于写入负载，端到端吞吐按“fio 实际字节数 /（`active_io_runtime + post_write_drain`）”计算，将清空文件系统 writeback 队列的时间纳入统计，避免更大的未落盘积压反而获得更高分数。

| 完整排空负载 | BrewFS | JuiceFS | BrewFS / JuiceFS |
| --- | ---: | ---: | ---: |
| Large write | **327.9 MiB/s** | 103.1 MiB/s | **3.18x** |
| Sequential write | **103.4 MiB/s** | 99.7 MiB/s | **1.04x** |
| Random write | **104.4 MiB/s** | 97.8 MiB/s | **1.07x** |
| Mixed random I/O total | **278.0 MiB/s** | 74.2 MiB/s | **3.75x** |

在 large read/write、sequential read/write、random read/write 和 mixed random I/O 七项数据面负载上，BrewFS 的无权几何平均为 **1.74x**。加入下方五项元数据操作后，全部 12 项的无权几何平均为 **1.38x**；mixed I/O 在汇总中按总字节数只计算一次。

### 元数据吞吐

| 操作 | BrewFS | JuiceFS | BrewFS / JuiceFS |
| --- | ---: | ---: | ---: |
| Create | **1,054.5 ops/s** | 651.5 ops/s | **1.62x** |
| Open | 5,018.6 ops/s | **12,027.2 ops/s** | 0.42x |
| Stat | **686,751.8 ops/s** | 683,792.0 ops/s | 1.00x |
| Readdir | **34,480.7 ops/s** | 17,580.2 ops/s | **1.96x** |
| Rename | 965.7 ops/s | **1,306.8 ops/s** | 0.74x |

<details>
<summary><strong>延迟、时长与基准细节</strong></summary>

| 负载 | BrewFS wall / active | JuiceFS wall / active | BrewFS p99 | JuiceFS p99 |
| --- | ---: | ---: | ---: | ---: |
| Large write | 2s / 1.123s | 2s / 0.935s | W 65.1ms | W 46.9ms |
| Large read | 2s / 1.377s | 1s / 1.093s | R 0.1ms | R 137.4ms |
| Sequential read | 61s / 60.001s | 61s / 60.001s | R 0.0ms | R 4.8ms |
| Sequential write | 62s / 60.019s | 96s / 60.067s | W 124.3ms | W 90.7ms |
| Random read | 60s / 60.003s | 60s / 60.009s | R 0.0ms | R 21.6ms |
| Random write | 64s / 62.210s | 103s / 60.011s | W 5335.2ms | W 248.5ms |
| Mixed random I/O | 68s / 65.707s | 61s / 60.304s | R 89.7ms / W 3238.0ms | R 1249.9ms / W 34.3ms |

| 元数据操作 | BrewFS 延迟 | JuiceFS 延迟 |
| --- | ---: | ---: |
| Create | **948 us/op** | 1,535 us/op |
| Open | 199 us/op | **83 us/op** |
| Stat | **1 us/op** | **1 us/op** |
| Readdir | **29 us/op** | 57 us/op |
| Rename | 1,036 us/op | **765 us/op** |

| 工具 | BrewFS wall | JuiceFS wall | 结果 |
| --- | ---: | ---: | --- |
| `dirstress` | **1s** | 3s | pass / pass |
| `dirperf` | 16s | **14s** | pass / pass |
| `metaperf` | 207s | **194s** | pass / pass |
| `looptest` | **1s** | **1s** | pass / pass |

双方完整运行中的全部 11 个工具均通过；随后又以同一 profile 分别重复了优化后的 large-read 与 large-write 行。这是一组面向吞吐的 profile，不是 durability 等价性声明。JuiceFS 在完整运行中产生了本地 cache `flushPage` 慢操作警告；其 large、sequential、random 和 mixed 写入的排空时间分别为 9/109/132/82 秒，BrewFS 完整运行对应为 2/25/14/16 秒，两次优化 large-write rerun 均为 2 秒。原始日志保留全部警告和排空诊断，生成报告同时将推导结果写入 `fully-drained-throughput.tsv`。

这是 2026 年 7 月 19 日的本地工程级快照，不可视作全部部署场景下的普适结论。每次运行都会将 fio JSON、环境信息、日志与生成报告写入 `.gitignore` 的 `docker/compose-xfstests/artifacts/` 目录，便于本地复核。

</details>

可按以下命令复现实验：

```bash
BREWFS_COMPRESSION=none \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 --writeback-throughput-profile
JFS_COMPRESS=none \
  bash docker/compose-xfstests/run_juicefs_perf.sh --writeback-throughput-profile
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
