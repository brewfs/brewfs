# Profiling

本文档汇总 BrewFS 的常用性能分析方法，包括：
- tracing-chrome（Perfetto/Chrome trace）
- Criterion 自带 flamegraph
- tracing-flame（运行时 trace -> flamegraph）
- tokio-console（异步任务/等待分析）
- jemalloc heap profiling（内存占用热点）

> 建议只在分析时开启 profiling，避免影响结果。

## Tracing Chrome（Bench）

```
BREWFS_BENCH_CHROME=/tmp/brewfs_trace.json \
RUST_LOG=brewfs=trace \
cargo bench --bench brewfs_bench -- brewfs_big_file/write
```

打开 trace 文件：
- Perfetto（推荐）：`https://ui.perfetto.dev`
- Chrome：`chrome://tracing`

相比之下tracing-chrome最通用，它可以看到一个包含on-cpu和off-cpu的完整时间线，并且可以用SQL查询关键指标。但是它只能看到被`tracing`打出的span，需要手动补充span和instrument，用起来较为繁琐。

### 常用的 span 添加方式

#### 1) 给函数加 `#[tracing::instrument]`

适合快速覆盖函数整体耗时。

```rust
#[tracing::instrument(level = "trace", skip(self, buf), fields(offset, len = buf.len()))]
async fn read_at(&self, offset: u64, buf: &mut [u8]) -> anyhow::Result<usize> {
    // ...
    Ok(buf.len())
}
```

#### 2) 只包一段逻辑（子阶段）

适合细分内部步骤，比如锁等待、IO、拷贝等。

```rust
let _span = tracing::trace_span!("read_at.split_spans", offset, len = actual_len).entered();
let spans = split_chunk_spans(self.config.layout, offset, actual_len);
```

#### 3) 给 async 片段打 span

```rust
use tracing::Instrument;

let out = async {
    fetcher.prepare_slices().await?;
    fetcher.read_at(start, len).await
}
.instrument(tracing::trace_span!("fetch.read_range", start, len))
.await?;
```

#### 4) 动态记录字段

当字段只有在运行时才能确定，可以在 span 内部补充：

```rust
let span = tracing::trace_span!("read_range", key = %key_str, offset, len);
let _enter = span.enter();
span.record("read_len", read_len);
```

> 实践建议：大对象用 `skip(...)`，只记录关键字段，避免 trace 文件过大。比如对`data: &[u8]`使用`skip(data)`，另加一个`fields(len = data.len())`记录长度即可。

## Tracing Chrome（主程序）

```
sudo RUST_LOG=brewfs=trace \
     BREWFS_TRACE_CHROME=/tmp/brewfs.trace.json \
     ./target/release/brewfs mount /mnt/brewfs \
       --meta-backend etcd \
       --meta-etcd-urls http://127.0.0.1:2379
```

用主程序进行tracing的好处是可以使用Fio等工具进行压测，得到压测结果的分析。

## Criterion 自带 Flamegraph（Bench）

```
BREWFS_BENCH_FLAMEGRAPH=1 \
cargo bench --bench brewfs_bench -- brewfs_big_file/write
```

输出位置示例：
```
target/criterion/brewfs_big_file/write/1/profile/flamegraph.svg
```

Criterion自带的Flamegraph最为懒人，只需要一条环境变量即可开启，但是它是on-cpu的火焰图，看不到off-cpu的结果，这导致了一些问题:

曾经 VFS 每次调用 write 都会执行 `extend_file_size`，实际等待接近 4 秒，但火焰图中看不到这部分等待，导致看起来像是内存拷贝和`writev`各占一半（约0.5秒），从而导致误判瓶颈。

## tracing-flame（运行时 trace -> flamegraph）

主程序内置 `tracing-flame` 开关：

```
BREWFS_TRACE_FLAME=/tmp/brewfs.folded \
RUST_LOG=brewfs=trace \
./target/release/brewfs mount /mnt/brewfs \
  --meta-backend etcd \
  --meta-etcd-urls http://127.0.0.1:2379
```

停止进程后生成 `/tmp/brewfs.folded`，再用 flamegraph 工具生成图：

```
inferno-flamegraph /tmp/brewfs.folded > /tmp/brewfs_flame.svg
```

> 需要关键路径存在 `tracing::instrument` 或显式 span 才有价值。

能看到整体时间分布，输出简单，易于对比不同版本，但是只能看到被`tracing::instrument`的函数，看不到函数内部的细粒度span。

## tokio-console（任务/等待分析）

主程序已集成 `console-subscriber`，通过环境变量开启：

```
RUSTFLAGS="--cfg tokio_unstable" \
TOKIO_CONSOLE=1 \
RUST_LOG=brewfs=info \
./target/release/brewfs mount /mnt/brewfs \
  --meta-backend etcd \
  --meta-etcd-urls http://127.0.0.1:2379
```

另起终端运行：
```
tokio-console
```

默认连接 `http://127.0.0.1:6669`。若提示“不支持 state streaming”，请确认二进制使用
`RUSTFLAGS="--cfg tokio_unstable"` 编译。

tokio-console 专注于 tokio 任务/等待时间，定位异步瓶颈很有效，但是它只覆盖 tokio 运行时层面，缺少完整的调用栈视图。

## jemalloc heap profiling（内存热点）

需要使用

### 1) 采集 heap 文件

需要使用`cargo build --release --features jemalloc-profiling`构建。

```
sudo RUST_LOG=brewfs::vfs::io::reader=trace --preserve-env=_RJEM_MALLOC_CONF \
              _RJEM_MALLOC_CONF="prof:true,prof_active:true,prof_final:true,lg_prof_interval:30,lg_prof_sample:21,prof_prefix:/tmp/brewfs,confirm_conf:true" \
              ../target/release/brewfs mount /mnt/brewfs \
              --meta-backend etcd \
              --meta-etcd-urls http://127.0.0.1:2379
```

会生成类似：
```
/tmp/brewfs_heap.<pid>.<seq>.heap
```

### 2) 生成 SVG/PDF

```
BIN=$(ls target/release/deps/brewfs_bench-* | head -n 1)
jeprof --show_bytes --svg "$BIN" /tmp/brewfs_heap.*.heap > /tmp/brewfs_heap.svg
```

> 如果符号不完整，可用 `RUSTFLAGS="-g"` 重新编译再采集。

jemalloc heap profiling专用于堆内存分析，不反应 CPU/IO。

## run_perf.sh（自动化性能分析 + 火焰图）

`tools/perf/run_perf.sh` 提供一键性能分析流程：构建 → 启动基础设施 → 挂载 → fio 压测 → perf 采样 → 生成火焰图。

```bash
cd project/brewfs
./tools/perf/run_perf.sh              # 完整流程
./tools/perf/run_perf.sh --quick      # 短时间压测（15s）
./tools/perf/run_perf.sh --no-build   # 跳过编译
./tools/perf/run_perf.sh --skip-offcpu # 跳过 off-CPU 分析
PERF_FIO_WORKLOADS="randrw" PERF_FIO_DIRECT=1 ./tools/perf/run_perf.sh --quick
```

输出产物：
- `tools/perf/results/<timestamp>/flame/oncpu-flame.svg` — On-CPU 火焰图
- `tools/perf/results/<timestamp>/flame/offcpu-flame.svg` — Off-CPU 火焰图
- `tools/perf/results/<timestamp>/fio/fio-*.json` — fio 原始数据
- `tools/perf/results/<timestamp>/llm-report.txt` — LLM 可读的分析报告
- `tools/perf/results/<timestamp>/report.md` — Markdown 报告

常用环境变量：
- `PERF_FIO_WORKLOADS="seqwrite seqread randwrite randread randrw"` 控制 fio workload 集合。
- `PERF_FIO_DIRECT=0|1` 显式选择 buffered 或 direct I/O；默认仍为 `0`，便于观察 Linux page cache/FUSE writeback 的影响。
- `PERF_RECORD_FREQ=19` 可降低 perf 采样频率，减少 `perf.data` 体积；默认 `49`。
- `KEEP_PERF_DATA=1` 保留原始 `perf.data`；默认生成火焰图和报告后删除，避免结果目录膨胀。
- `BREWFS_UPLOAD_CONCURRENCY=32` 控制单个 writer 内并发 block PUT 上限；它会与 `BREWFS_WRITEBACK_UPLOAD_CONCURRENCY` 的全局 writeback 上传池共同生效，较低者更容易成为瓶颈。
- `BREWFS_RANGE_BACKGROUND_PREFETCH=false` 只关闭 range miss 后的 full-block 后台预取，保留 VFS 层顺序预取；适合诊断 randrw 下后台 GET/PUT 竞争。

Compose perf runner 的 `report.md` 会把脚本 wall seconds、fio `job_runtime_ms`、active IO runtime、writeback dirty/recent pending/uploaded 和 S3 GET/PUT 平均延迟放在同一份报告里。对 `direct=0` 结果尤其要看这些字段，因为 page cache/FUSE writeback 可能让 fio 运行期吞吐看起来很好，但 close/flush 或后台上传拖尾仍然很重。

> **注意**：脚本使用 `--call-graph fp`（frame pointers）而非 `dwarf`，因为 brewfs
> release binary 含有 735MB+ 的调试信息，`addr2line` 无法处理如此大的 DWARF section，
> 会报 "could not read first record" 错误。Frame pointer 方式更快更可靠。

## brewfs-stats（实时性能监控）

类似 `juicefs stats`，通过读取挂载点下的 `.stats` 虚拟文件实时展示性能指标。

```bash
# 编译
cargo build -p brewfs-stats

# 使用（默认 1s 刷新）
brewfs-stats /mnt/brewfs

# 自定义刷新间隔
brewfs-stats /mnt/brewfs -i 2

# 直接查看原始指标
cat /mnt/brewfs/.stats
```

展示内容：
| 模块 | 指标 | 说明 |
|------|------|------|
| FUSE | ops, read, write, r_lat, w_lat | FUSE 层吞吐与延迟 |
| META | ops, txn, lat | 元数据操作与事务 |
| OBJECT | get, get/s, put, put/s, del | S3 对象存储流量 |
| CACHE | hit, miss, dirty | 缓存命中率与脏数据量 |

详细文档见 `doc/stats-tool.md`。
