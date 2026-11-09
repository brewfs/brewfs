# BrewFS vs JuiceFS 同机对照（PR #441 修复后，2026-11-09）

> 本文档是一次**只测量、不改产品代码**的对照实验记录。
>
> - 被测 BrewFS 二进制：`origin/main` = `622736b`（PR #441「fix(chunk): cut 4 KiB random-read
>   amplification 303x -> 0.8x, +53.2x read throughput, clat p99 -3.3x」）
> - 被测 JuiceFS：`1.3.1+2025-12-02.e0032b2`
> - 对照场景：`fio-seqwrite` / `fio-randwrite` / `fio-randread` × bs ∈ {4 MiB, 1 MiB}，3 轮独立重复
> - 完整报告（含进度日志、5 张表、逐轮原始数字、11-08 C1-v2 对比）见 `/home/hxy/JUICEFS-VS-BREWFS-REPORT.md`

## 1. 这次测量回答什么、不回答什么

| 问题 | 本次能否回答 |
| --- | --- |
| 修复后两侧在**对称参数**下的写/随机写吞吐对比 | ✅ 能（表 2、表 3） |
| 排空后（含写回落盘）的端到端吞吐对比 | ✅ 能（表 3） |
| 延迟对比（clat mean / p99） | ⚠️ 写侧同口径；**读侧不同口径**，见 §2.3 |
| 「PR #441 是否修好了冷读差距」 | ❌ **主矩阵不能回答**，必须看冷读格（表 5） |
| 冷读对比 | ✅ 能（表 5，与 11-08 C1-v2 的 coldread 行对照） |

### 1.1 为什么主矩阵回答不了冷读

主矩阵的数据集只有 **512 MiB**，而两侧的读缓存分别是 BrewFS 4 GiB 内存 + 8 GiB SSD、
JuiceFS buffer 4096 MiB + cache 8192 MiB。**数据全落在本地缓存里**，两侧都证实了这一点：

| 计数器 | BrewFS | JuiceFS | 含义 |
| --- | ---: | ---: | --- |
| `brewfs_read_range_gets_total` | **0** | — | BrewFS 一次 S3 range GET 都没发 |
| `brewfs_read_block_cache_hits_total` | 33,968 | — | 全部命中本地块缓存 |
| `juicefs_blockcache_hit_bytes` | — | **41.7 GB** | 41.7 GB 从本地缓存读出 |

这不是不对称（两侧同样命中本地缓存），但它使 `fio-randread` 一格**测的是本地缓存带宽**，
而不是对象存储的读吞吐。因此额外加了**冷读格**：`PERF_FIO_COLD_READ=1` +
清 cache root + remount + `drop_caches`。BrewFS 侧还带 `assert_cold_read_no_hits` 硬校验
（缓存命中即判失败），JuiceFS 侧走 `clear_juicefs_cache_if_requested` + `drop_caches`。

### 1.2 为什么 PR #441 在主矩阵下不会改变数字

`src/chunk/store.rs:allow_range_background_prefetch()` 的抑制条件必须同时满足：

1. `range_read_threshold = 0.25`（`src/chunk/cache.rs:606`）——只有**读长度 < 25% × 4 MiB = 1 MiB**
   的 miss 才走「整块后台预取」路径。bs=4 MiB 远超阈值，bs=1 MiB 恰在阈值边缘，**两者都不触发**。
2. `estimated_working_set_bytes > max_hot_bytes × factor` = `read_memory_bytes × 4`
   = 4 GiB × 4 = **16 GiB**。主矩阵数据集 512 MiB，**工作集不可能超过 16 GiB**。

⇒ 主表回答的是「**修复后两侧对称对照**」，不是「PR #441 修好了冷读差距」。
验证 #441 本身需要 **bs=4 KiB + 缩小读缓存**的诊断格
（`read_memory_bytes` 降到 64 MiB ⇒ 阈值 256 MiB < 512 MiB 数据集，
`brewfs_read_background_prefetch_suppressed_total` 会 >0，作为修复被触发的直接证据）。

### 1.3 两侧无法消除的不对称

| 参数 | BrewFS（harness 设的值） | JuiceFS（harness 设的值） | JuiceFS 官方默认 |
| --- | --- | --- | --- |
| 对象上传并发 | `BREWFS_S3_MAX_CONCURRENCY=16`、`BREWFS_UPLOAD_CONCURRENCY=32` | `JFS_MAX_UPLOADS=4` | **20** |
| 写回 staging 并发 | `BREWFS_WRITEBACK_UPLOAD_CONCURRENCY=6` | `JFS_MAX_STAGE_WRITE=4` | **1000** |
| 下载并发 | `BREWFS_S3_MAX_CONCURRENCY=16` | `JFS_MAX_DOWNLOADS=8` → **1.3.1 无此选项** | — |

两点必须写进结论：

1. JuiceFS 被 harness **主动降到 4/4**，远低于它自己的默认（20/1000）。
   即 **BrewFS 侧拿到了更有利的并发配置**——这让「JuiceFS 写更快」这个结果**更难**成立，
   而不是更容易。
2. `--max-stage-write=4` 限制的是「把暂存数据写成对象的线程数」，而 JuiceFS 的 writeback 模式
   本身允许「超过阈值的请求直接上传」，所以实际并发并不严格等于 4。
   合起来意味着：**写路径的并发不对称既存在、方向又对 BrewFS 不利，但幅度无法从本表精确量化**。

### 1.4 读路径的 FUSE direct I/O 不对称

`--writeback-throughput-profile` 在 BrewFS 侧**隐含** `READ_THROUGHPUT_PROFILE=true`
（`run_redis_perf.sh:177-180`），后者会设 `BREWFS_FUSE_READ_DIRECT_IO=1`
（`run_redis_perf.sh:218-221`）。该开关在 `src/fuse/mod.rs:145-160` 只对**只读句柄**生效：

```rust
let read_only = read && !write;
let direct_io = ... || (read_only && fuse_read_direct_io_enabled());
if (read || write) && direct_io { flags |= FOPEN_DIRECT_IO; }
```

`fio-randread` 的文件正是以**只读**方式打开的 ⇒ **BrewFS 的 randread 走 FUSE direct I/O，
不经过内核页缓存**；而 JuiceFS 1.3.1 没有 per-handle direct I/O，它的读默认经内核页缓存。

⇒ **`fio-randread` 一格的延迟对比不是同口径的**，吞吐对比也部分受益于
「BrewFS 少一层页缓存拷贝」。本轮 artifact 佐证：BrewFS `perf-profile.env` 里
`BREWFS_FUSE_READ_DIRECT_IO=1`。

## 2. 方法学

### 2.1 隔离（与同机另一 agent 会话共存）

同机还有另一个 agent 会话（cfio），共用同一台机器、同一个 docker daemon、同一个 `/home/hxy`。
它的 harness 会 `docker compose down -v --remove-orphans`、按**写死的端口** 16379/19000/19001
`docker stop` 占用者，并且所有 compose 文件同目录 ⇒ **compose project 名同名、数据卷名字面量共享**。

因此本次测量做了三层隔离（全部是 harness/脚本层改动，不碰产品代码）：

| 隔离项 | 本测量的做法 |
| --- | --- |
| compose project | 独立 compose 文件带 `name: brewfs-jfs-perf` / `name: juicefs-jfs-perf`；每轮再用 `COMPOSE_PROJECT_NAME=jfs-<side>-<pid>` 覆盖 |
| 容器名 / 数据卷名 | 加 `-jfs` 后缀，且每轮再带 `$$` 后缀 |
| 宿主机端口 | 每轮用 `$$` 派生 17400+ / 17500+ / 17600+（JuiceFS 侧 17700+ / 17800+ / 17900+） |
| 镜像 tag | 独立 tag `brewfs-jfs-perf:local` / `juicefs-jfs-perf:local`，不与另一会话共享 |
| 空闲门 | 每轮前三次 15 s 间隔的空闲确认 |
| watchdog | 轮次中检测到外部 fio **或 cargo/rustc** 就标记 tainted 并中止 |
| 互斥锁 | `flock /home/hxy/.jfs-perf.lock` |

> **watchdog 的一次修正**：初版 watchdog 只检测 fio 进程，结果 2026-11-09 13:51:39–14:04:09
> 另一会话的 `cargo build`（写入 6050 个构建产物、load1 达 11，而本机只有 8 vCPU）
> 污染了 3 轮数据，事后才发现。现已把 `rustc|cargo` 加入检测条件。

### 2.2 每轮的配置

```bash
PERF_FIO_SIZE=512m  PERF_FIO_RUNTIME=20
PERF_FIO_SEQWRITE_BS=<4m|1m>  PERF_FIO_RANDWRITE_BS=<4m|1m>  PERF_FIO_RANDREAD_BS=<4m|1m>
  bash docker/compose-xfstests/run_redis_perf.sh   --s3 \
    --writeback-throughput-profile --tools "fio-seqwrite fio-randwrite fio-randread"
PERF_FIO_SIZE=512m  PERF_FIO_RUNTIME=20
PERF_FIO_SEQWRITE_BS=<4m|1m>  PERF_FIO_RANDWRITE_BS=<4m|1m>  PERF_FIO_RANDREAD_BS=<4m|1m>
  bash docker/compose-xfstests/run_juicefs_perf.sh \
    --writeback-throughput-profile --tools "fio-seqwrite fio-randwrite fio-randread"
```

每轮：全新 compose project + 清空数据卷 + 三次空闲确认 + 中途 watchdog。
per-scene BS 覆盖走 `PERF_FIO_{SEQWRITE,RANDWRITE,RANDREAD}_BS`，两侧 env 透传白名单都含这三个变量
（`run_redis_perf.sh:660,669,678` / `run_juicefs_perf.sh:305,314,323`）。

### 2.3 两侧对称性核对

| 维度 | BrewFS | JuiceFS | 对称？ |
| --- | --- | --- | --- |
| harness | `run_perf_in_container.sh` | `run_juicefs_perf_in_container.sh` | 场景参数表逐项一致 |
| fio 场景默认值 | bs/size/numjobs/ioengine/iodepth/direct | 同 | ✅ |
| fio 观测参数 | `--lat_percentiles=1 --clat_percentiles=1 --slat_percentiles=1 --percentile_list=50:95:99:99.9 --randseed=20261031 --randrepeat=1` | 完全相同 | ✅ |
| prefill | `--rw=write --bs=<同 bs> --size=<同 size> --end_fsync=1` | 同 | ✅ |
| 缓存预算 | read-mem 4 GiB + read-SSD 8 GiB + write-mem 4 GiB + write-SSD 4 GiB，memory budget 12 GiB | buffer 4096 MiB + cache 8192 MiB + cache-large-write | ✅（总量对齐） |
| 排空后吞吐指标 | `fully-drained-throughput.tsv`，公式 `(read+write bytes)/(active_seconds+drain_seconds)` | **同一公式、同一文件名** | ✅ 同口径 |
| 排空判定 | `brewfs_writeback_live_dirty_bytes==0` 且 put/backend 计数连续稳定 | `juicefs_staging_blocks==0 && staging_bytes<=0 && uploading==0` | 各自严格排空，语义等价 |
| 对象存储 | RustFS `rustfs/rustfs`（同一 digest `ed9210d6`），同一 S3 桶 `brewfs-data` | 同 | ✅ |
| 元数据库 | Redis 7.2-alpine | Redis 7.2-alpine | ✅ |
| 写回并发 | S3 max 16 / writeback upload 6 / upload 32 | max-uploads 4 / max-stage-write 4 | ⚠️ 不同，见 §1.3 |
| 下载并发 | S3 max concurrency 16 | `--max-downloads` **1.3.1 不存在** → 用默认 | ❌ 见 §1.3 |
| 读 direct I/O | FUSE per-handle direct I/O（只读句柄） | 无 per-handle direct I/O | ❌ 见 §1.4 |

## 3. 对仓库的改动（全部非产品代码）

| 文件 | 改动 | 为什么 |
| --- | --- | --- |
| `docker/compose-xfstests/run_juicefs_perf.sh` | ①新增 `JUICEFS_PERF_COMPOSE_FILE` 覆盖开关（+8 行，与既有 `BREWFS_PERF_COMPOSE_FILE` 约定一致）；②`preclean_ports()` 改为读 `REDIS_HOST_PORT`/`RUSTFS_S3_HOST_PORT`/`RUSTFS_CONSOLE_HOST_PORT`（+3/-3 行） | ①否则 JuiceFS 侧无法用独立 compose 文件；②原代码**写死** 16379/19000/19001 并 `docker stop` 占用者，会**杀掉同机另一会话的容器** |
| `docker/compose-xfstests/docker-compose.redis-perf-jfs.yml` | 新增（从 `docker-compose.redis-perf.yml` 派生）：compose project 名 `brewfs-jfs-perf`、容器名/卷名加 `-jfs`、网络名 `brewfs-jfs`、镜像 tag `brewfs-jfs-perf:local` | 与同机另一会话完全隔离 |
| `docker/compose-xfstests/docker-compose.juicefs-perf-jfs.yml` | 新增（从 `docker-compose.juicefs-perf.yml` 派生）：compose project 名 `juicefs-jfs-perf`、容器名/卷名加 `-jfs`、网络名 `juicefs-jfs`、镜像 tag `juicefs-jfs-perf:local` | 同上 |

**产品代码（`src/`）零改动。**

## 4. 结果

表格与结论见 `/home/hxy/JUICEFS-VS-BREWFS-REPORT.md`（数据尚未随本 PR 入库；本 PR 只归档方法学与
隔离改动，避免把本机原始数据塞进产品仓库）。

## 5. 与 11-08 C1-v2 的协议差异（对比时必须说明）

C1-v2 基线取自 PR #438 的 `doc/experiments/2026-11-08-campaign/reports/METRICS.md`。
两次测量的**协议不同**，绝对数字不可直接比大小：

| 维度 | 11-08 C1-v2 | 本次（修复后） |
| --- | --- | --- |
| 拓扑 | **4 台云主机**（a-client 客户端 / a-meta TiKV / a-obj RustFS） | **单机**，RustFS 与客户端同机 |
| 元数据 | TiKV（a-meta） | **Redis** |
| fio direct | `--direct=1` | `--direct=0`（走页缓存，与 harness 默认一致） |
| ioengine / iodepth | `libaio` / 16 | `io_uring` / 1 |
| 每 cell 大小 | 1 GiB，新文件 create-new | **512 MiB 数据集，size=512m**，覆盖写 |
| 冷读做法 | **重启 RustFS** + drop_caches | 清 cache root + remount + drop_caches（BrewFS 侧另有 `assert_cold_read_no_hits` 硬校验） |
| 分位数 | p50/p95/p99 | p50/p95/p99/p99.9 + clat |
| 重复 | 3 轮中位数 | 3 轮中位数 + min/max + spread |

⇒ 对比只做**方向性**结论（谁快、快多少倍），不声称「#441 让冷读从 X 变成 Y」这种绝对值断言。
