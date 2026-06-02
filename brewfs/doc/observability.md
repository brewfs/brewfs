# 可观测性

BrewFS 提供了多层性能分析和监控手段：运行时统计（.stats 虚拟文件）、分布式追踪（tracing）、CPU 火焰图、tokio 异步任务分析和堆内存分析。

## brewfs-stats（实时监控）

类似 `juicefs stats` 的终端实时监控工具。

```bash
# 编译
cargo build -p brewfs-stats

# 默认 1s 刷新
brewfs-stats /mnt/brewfs

# 自定义刷新间隔
brewfs-stats /mnt/brewfs -i 2
```

展示内容：

| 模块 | 指标 | 说明 |
|---|---|---|
| FUSE | ops, read, write, r_lat, w_lat | FUSE 层吞吐与延迟 |
| META | ops, txn, lat | 元数据操作与事务 |
| OBJECT | get, get/s, put, put/s, del | S3 对象存储流量 |
| CACHE | hit, miss, dirty | 块缓存命中率与脏数据量 |

### .stats 虚拟文件

挂载点根目录下的 `.stats` 文件（inode: `0x7FFF_FFFF_0000_0003`，mode 0444）提供 Prometheus 格式的原始指标：

```
brewfs_uptime_seconds 3600
brewfs_fuse_read_ops_total 123456
brewfs_fuse_read_bytes_total 1073741824
brewfs_fuse_read_lat_us_total 5000000
brewfs_fuse_write_ops_total 45678
brewfs_fuse_write_bytes_total 536870912
brewfs_fuse_write_lat_us_total 8000000
brewfs_fuse_lookup_ops_total 789012
...
```

实现位于 `src/vfs/stats.rs`，使用 `AtomicU64` + `Relaxed` 顺序做无竞争计数更新。`OpTimer` RAII 结构在 drop 时自动记录延迟，无需手动埋点。

目前已打点的 FUSE 操作：read、write、lookup。其余操作（getattr、open、create、unlink 等）以及 meta/S3 层指标待补充。

## 性能基准测试

`benches/` 目录下的 Criterion benchmark，复现 `juicefs bench` 的工作负载模式。

```bash
cargo bench --bench brewfs_bench
```

### 测试场景

| 阶段 | 说明 | 指标 |
|---|---|---|
| 大文件读写 | 多线程顺序写/读 | GiB/s 吞吐 |
| 小文件读写 | 每线程大量 128 KiB 文件 | 文件数/秒 |
| stat 压测 | 反复 stat 同一批小文件 | 操作数/秒 |

### 环境变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `BREWFS_BENCH_THREADS` | 4 | 并发线程数 |
| `BREWFS_BENCH_BLOCK_MB` | 1 | 单次 IO 块大小 (MiB) |
| `BREWFS_BENCH_BIG_FILE_MB` | 512 | 大文件逻辑大小 (MiB) |
| `BREWFS_BENCH_SMALL_FILE_KB` | 128 | 小文件大小 (KiB) |
| `BREWFS_BENCH_SMALL_FILE_COUNT` | 100 | 每线程小文件数 |
| `BREWFS_BENCH_SAMPLE_SIZE` | ≥10 | Criterion 样本数 |
| `BREWFS_BENCH_MODE` | direct | `direct` 直连 VFS，`fuse` 通过 FUSE |
| `BREWFS_BENCH_BACKEND` | local | `local` 或 `s3` |
| `BREWFS_BENCH_META_BACKEND` | sqlx | `sqlx`、`redis`、`etcd` |
| `BREWFS_BENCH_FLAMEGRAPH` | 未设置 | 设任意值采集火焰图 |
| `BREWFS_BENCH_CHROME` | 未设置 | Chrome trace 输出路径 |

## Tracing（分布式追踪）

### tracing-chrome (Perfetto)

最通用的方案，可看到包含 on-cpu 和 off-cpu 的完整时间线：

**Bench 模式：**
```bash
BREWFS_BENCH_CHROME=/tmp/brewfs_trace.json \
RUST_LOG=brewfs=trace \
cargo bench --bench brewfs_bench -- brewfs_big_file/write
```

**主程序模式：**
```bash
sudo RUST_LOG=brewfs=trace \
     BREWFS_TRACE_CHROME=/tmp/brewfs.trace.json \
     ./target/release/brewfs mount /mnt/brewfs \
       --meta-backend etcd \
       --meta-etcd-urls http://127.0.0.1:2379
```

查看：
- Perfetto UI (`https://ui.perfetto.dev`)
- Chrome: `chrome://tracing`

### tracing-flame

运行时生成火焰图（需要手动添加 `#[tracing::instrument]` span）：

```bash
BREWFS_TRACE_FLAME=/tmp/brewfs.folded \
RUST_LOG=brewfs=trace \
./target/release/brewfs mount /mnt/brewfs \
  --meta-backend etcd \
  --meta-etcd-urls http://127.0.0.1:2379
```

停止进程后生成 SVG：
```bash
inferno-flamegraph /tmp/brewfs.folded > /tmp/brewfs_flame.svg
```

### 添加 Span

```rust
// 函数级
#[tracing::instrument(level = "trace", skip(self, buf), fields(offset, len = buf.len()))]
async fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> { ... }

// 代码块级
let _span = tracing::trace_span!("read_at.split_spans", offset, len).entered();
let spans = split_chunk_spans(self.config.layout, offset, len);

// async 片段
let out = async { ... }
    .instrument(tracing::trace_span!("fetch.read_range", start, len))
    .await?;
```

建议大对象使用 `skip(...)`，只记录关键字段，避免 trace 文件过大。

## tokio-console（异步任务分析）

专用于分析 tokio 运行时内的任务调度和等待时间：

```bash
# 编译时启用
RUSTFLAGS="--cfg tokio_unstable" \
TOKIO_CONSOLE=1 \
RUST_LOG=brewfs=info \
./target/release/brewfs mount /mnt/brewfs \
  --meta-backend etcd \
  --meta-etcd-urls http://127.0.0.1:2379
```

另开终端：
```bash
tokio-console    # 默认连接 http://127.0.0.1:6669
```

## perf 火焰图（CPU Sampling）

### 使用 run_perf.sh 自动化

```bash
./tools/perf/run_perf.sh              # 完整流程（构建 → 基础设施 → 挂载 → fio → perf → 火焰图）
./tools/perf/run_perf.sh --quick      # 短时间压测（15s）
./tools/perf/run_perf.sh --no-build   # 跳过编译
./tools/perf/run_perf.sh --skip-offcpu # 跳过 off-CPU 分析
```

产物：
- `/tmp/brewfs-perf/flame/oncpu-flame.svg`
- `/tmp/brewfs-perf/flame/offcpu-flame.svg`
- `/tmp/brewfs-perf/results/fio-*.json`
- `/tmp/brewfs-perf/llm-report.txt`

注意：脚本使用 `--call-graph fp`（frame pointers）而非 `dwarf`，因为 release binary 含 735MB+ 调试信息，`addr2line` 无法处理。

### 手动操作

```bash
# 编译（带符号 + frame pointers）
RUSTFLAGS="-C force-frame-pointers=yes" CARGO_PROFILE_RELEASE_DEBUG=true \
  cargo build --release

# 记录 perf 数据
sudo perf record -F 99 -g -- ./target/release/brewfs mount /mnt/brewfs ...

# 另一个终端运行工作负载
fio --directory=/mnt/brewfs --rw=read --size=4G --bs=1M --direct=1 ...

# Ctrl+C 停止 perf，生成火焰图
sudo perf script | inferno-collapse-perf > out.folded
inferno-flamegraph out.folded > flame.svg
```

### Criterion 自带的 Flamegraph

最简单的 on-cpu 火焰图方式：

```bash
BREWFS_BENCH_FLAMEGRAPH=1 \
cargo bench --bench brewfs_bench -- brewfs_big_file/write
```

输出：`target/criterion/brewfs_big_file/write/1/profile/flamegraph.svg`

局限：仅 on-cpu，看不到 off-cpu 等待时间。

## jemalloc Heap Profiling

分析内存分配热点：

```bash
# 编译（需要 jemalloc-profiling feature）
cargo build --release --features jemalloc-profiling

# 启动并采集 heap profile
sudo RUST_LOG=brewfs::vfs::io::reader=trace --preserve-env=_RJEM_MALLOC_CONF \
  _RJEM_MALLOC_CONF="prof:true,prof_active:true,lg_prof_interval:30,lg_prof_sample:21,prof_prefix:/tmp/brewfs" \
  ./target/release/brewfs mount /mnt/brewfs ...
```

生成报告：
```bash
jeprof --show_bytes --svg target/release/brewfs /tmp/brewfs_heap.*.heap > /tmp/brewfs_heap.svg
```

## 日志

日志使用 `tracing-subscriber`，通过 `RUST_LOG` 环境变量控制：

```bash
# Info 级别（默认）
RUST_LOG=brewfs=info

# Trace 级别（所有 span）
RUST_LOG=brewfs=trace

# 分级控制
RUST_LOG=brewfs::vfs::io::reader=trace,brewfs::chunk=debug,brewfs=info
```

### 日志文件分离

```bash
# FUSE 操作日志单独输出
BREWFS_FUSE_LOG_FILE=/var/log/brewfs/fuse.log

# 主日志输出到文件
BREWFS_LOG_FILE=/var/log/brewfs/brewfs.log
```

FUSE 操作日志仅包含 `rfuse3::raw::logfs` 的 TRACE 级别事件（每个 FUSE 请求/响应的详细信息），主日志输出其余所有事件。两者互不重复。

## 工具速查

| 工具 | 适用场景 | 是否需编译特性 |
|---|---|---|
| `brewfs-stats` | 实时吞吐/延迟监控 | 否 |
| Criterion flamegraph | 快速 on-cpu 热点 | `BENCH_FLAMEGRAPH=1` |
| tracing-chrome | 完整 on+off-cpu 时间线 | `profiling` feature |
| tracing-flame | 运行时 trace → 火焰图 | `profiling` feature |
| tokio-console | 异步任务/等待分析 | `RUSTFLAGS="--cfg tokio_unstable"` |
| perf | CPU sampling 火焰图 | `force-frame-pointers=yes` |
| jemalloc profiling | 堆内存热点 | `jemalloc-profiling` feature |
| `run_perf.sh` | 一键 perf+火焰图 | 自动处理编译 |
