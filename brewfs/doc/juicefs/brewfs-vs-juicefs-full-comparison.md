# BrewFS vs JuiceFS 全模块对比分析

审查日期: 2026-05-23  
范围: 读路径、写路径、缓存体系、事务引擎  
依据: `doc/juicefs-architecture.html`, `doc/juicefs/*.md`, BrewFS 当前源码

## 结论摘要

BrewFS 的写入并发、epoch 防陈旧提交、dirty overlay 和 Rust async pipeline 已经强于 JuiceFS 的部分实现；主要性能缺口集中在读路径缓存命中率、元数据缓存和磁盘缓存鲁棒性。现有 benchmark 显示:

| 指标 | JuiceFS | BrewFS | 差距 |
| --- | ---: | ---: | ---: |
| `stat` | 1,089,199 ops/s | 3,061 ops/s | 355x |
| `randread` | 1.3 GiB/s | 29 MiB/s | 42x |
| `seqread` | 1.1 GiB/s | 134 MiB/s | 8x |

读差距的主因不是对象存储单次吞吐，而是 BrewFS 本地缓存链条仍不够闭环: 小范围读、压缩块读、写后读、失败重试、尾部预取和磁盘缓存健康状态还没有全部达到 JuiceFS 的成熟度。

## 27 维度差异矩阵

| # | 模块 | 维度 | JuiceFS | BrewFS 当前状态 | 优势/缺口 |
| ---: | --- | --- | --- | --- | --- |
| 1 | 读路径 | 读前一致性 | `Read()` 前调用 writer flush，读己之所写 | dirty overlay + commit invalidate，避免每次读被 flush 阻塞 | BrewFS 延迟更低，需继续守住覆盖正确性 |
| 2 | 读路径 | handle 锁 | 写优先 R/W lock | FUSE handle 层已有互斥与 dirty overlay | 大体相当 |
| 3 | 读路径 | slice reader 状态机 | NEW/BUSY/REFRESH/BREAK/READY/INVALID | New/Busy/Ready/Invalid/Refresh，缺 BREAK | JuiceFS 更完整 |
| 4 | 读路径 | slice 失败重试 | 失败后 invalidate + 指数退避，最多 `maxRetries` | 当前失败会落 Invalid，重试能力不足 | 待修复 P2-6 |
| 5 | 读路径 | range read 条件 | `offset > 0 && len <= blockSize/4` | 已对齐，并在压缩时走 full-block 解压缓存 | 已对齐 |
| 6 | 读路径 | range 后整块预取 | `loadRange()` 成功后异步 prefetch full block | 已实现后台 full-block prefetch，并受并发限制 | 已优化 |
| 7 | 读路径 | full block singleflight | 同 key 并发 GET 合并 | `read_flight` 合并 full-block GET，小读可 piggyback | 已优化 |
| 8 | 读路径 | page 粒度复用 | 主要依赖 block cache | 64KB `ReadPageCache` 覆盖小范围随机读 | BrewFS 更细粒度 |
| 9 | 读路径 | 自适应预读 | 2 session，窗口翻倍/减半 | 2 session + MemoryBudget readahead_factor | 相当，阈值需调优 P1-5 |
| 10 | 读路径 | tail prefetch | 文件尾部 32KB 预取 | 未实现 | 待修复 P2-7 |
| 11 | 写路径 | 数据先后顺序 | data first, meta later，孤儿 block 可 GC | safe 模式同 data-first；也支持 writeback 风险模式 | 相当 |
| 12 | 写路径 | slice 提交线程 | per chunk commitThread 顺序提交 | per chunk commit task + creation_unique 排序 | BrewFS 更抗 FUSE 并发乱序 |
| 13 | 写路径 | 跨 chunk 依赖 | growing slice 等待前一 chunk dep committed | 无显式 dep chain | JuiceFS 更严格 |
| 14 | 写路径 | block 上传并发 | `currentUpload` semaphore | foreground/background 双 semaphore + pipeline | BrewFS 更强 |
| 15 | 写路径 | 写后缓存 | 上传后缓存 page/block | hot cache 已同步插入，disk cache best-effort | 已优化 P0-2 |
| 16 | 写路径 | flush 触发 | block 满、slice 满、idle、age、fsync/close | 类似，且有 size/pressure proactive flush | BrewFS 更细 |
| 17 | 写路径 | dirty 读覆盖 | flush 前读通过 writer flush 保证 | dirty overlay 可直接覆盖读缓冲 | BrewFS 延迟更优 |
| 18 | 缓存 | metadata CSC | Redis RESP3 client-side caching | MetaClient TTL/local cache，未完整 CSC | JuiceFS 明显领先，影响 stat 355x |
| 19 | 缓存 | open file chunk cache | openfiles 缓存 attr + chunk slices | inode cache 下缓存 slices | 相当，需继续校验失效 |
| 20 | 缓存 | hot memory cache | 内存 pages/pending | moka byte-weighted hot cache | BrewFS 更现代 |
| 21 | 缓存 | page cache | pending pages + disk cache | 64KB page_cache，压缩 full read 已填充 page_cache | 已优化 P0-3 |
| 22 | 缓存 | disk cache 文件格式 | data + CRC32C + tierID | `SFC1`/CRC32C framing | 相当 |
| 23 | 缓存 | disk cache 写入竞争 | busy map 去重 | `insert_opportunistic` 仍需 per-key CAS 去重 | 待修复 P1-4 |
| 24 | 缓存 | 磁盘健康状态 | Normal/Unstable/Down 自动降级恢复 | 缺健康状态机 | 待修复 P3-8 |
| 25 | 事务 | Redis 写事务 | WATCH/MULTI/EXEC，最多 50 retries | Lua + local lock + version counter | BrewFS RTT 更低 |
| 26 | 事务 | etcd 写事务 | 不适用 | EtcdTxn + lock stripes + jitter retry | BrewFS 已增强 |
| 27 | 事务 | slice 引用/GC | `sliceRefs` refcount | delayed slices/uncommitted slices | JuiceFS refcount 更完整 |

## Benchmark 解释

| Workload | 现象 | 主要原因 | 对应修复 |
| --- | --- | --- | --- |
| `stat` 355x gap | 元数据小请求大量穿透 Redis/后端 | Redis CSC/openfile attr cache 不完整 | 优先项 #2 |
| `randread` 42x gap | 4KB/小范围读无法稳定命中本地 block | page_cache 不升 block、压缩块不铺 page、磁盘缓存竞争 | #1, #3, #4 |
| `seqread` 8x gap | 顺序读仍有 S3 GET 和预读窗口不足 | write 后 cache warming、readahead/tail prefetch 不完整 | #1, #5 |

## 核心架构瓶颈: 双层缓存冲突 (实验确认 2026-05-24)

经过 5 轮 benchmark 和 9 项代码修复后，读性能 (bigread 226→230, randread 29→54) 远未达到 JuiceFS 水平 (4-25x 差距)。根因是 **VFS SliceState.page 与 ChunksCache 双层缓存架构冲突**:

### BrewFS 双层缓存

```
                      ┌─ VFS 层 ──────────────────────┐
FUSE read ──→ FileReader.read_at()                     │
                │                                       │
                ├─ prepare_slices()                     │
                │   └─ SliceState (per-range, per-fh)   │ ← 优先服务
                │       .page: Vec<u8>  (内存)          │
                │       TTL: 30s idle                   │
                │                                       │
                └─ background_fetch()                   │
                    └─ DataFetcher                      │
                        └─ ObjectBlockStore             │
                            └─ ChunksCache              │ ← 仅在 SliceState
                                ├─ hot (moka, 4GB)      │   缺失时查询
                                └─ disk (20GB, CRC32C)  │
                                                         └──────────────┘
```

### 冲突机制

1. 首次读: SliceState 创建 → `background_fetch` → S3 → ChunksCache 填充 → SliceState.page 拿到数据
2. 后续读同一范围 (30s 内): **SliceState.page 直接返回，完全绕过 ChunksCache**
3. 后续读不同范围: 新的 SliceState → 新的 background_fetch → ChunksCache check (hit or miss)

在 benchmark 的 60s 时间窗口内，每个 fio 作业的读写范围固定，SliceState 永不过期 → **ChunksCache 仅在首次访问每个 4MB 范围时被读取 1 次，之后永不被使用**。写入路径填充的 4GB 热缓存 + 20GB 磁盘缓存在读路径上形同虚设。

### 证据

| 指标 | BrewFS | JuiceFS | 说明 |
|------|----------|---------|------|
| bigread P50 | 135ms | 31ms | BrewFS 每读延迟 4.3x |
| seqread P50 | 16ms | 0.6ms | 即使"命中"也是内存拷贝延迟 |
| randread P99 | 1518ms | 24ms | 随机读几乎全 miss |

JuiceFS 无 VFS 层缓存: `rSlice.ReadAt()` 直接查 `bcache.load()` (3 级: memory → disk → S3)，所有读取统一走同一个缓存层，预填后所有读命中 disk cache。

### 修复方向

去除 VFS 层 `SliceState.page` 数据缓存，让所有读统一经过 `ObjectBlockStore.read_range` → `ChunksCache.get`。SliceState 退化为纯元数据记录 (范围覆盖 + 引用计数)，数据始终从 ChunksCache 服务。这需要重构 `FileReader.read_at`、`DataFetcher` 和 `SliceState` 的生命周期。

相关任务: `doc/superpowers/plans/2026-05-23-read-cache-optimization.md` Phase 4 (待规划)。

---

## 7 项优先修复排序

| 优先级 | 修复项 | 影响 | 状态 |
| ---: | --- | --- | --- |
| 1 | 小范围读后组装/预取 full block 并进入 block_cache | randread 直接收益 | 已完成: 后台 full-block prefetch + 全页齐备组装 |
| 2 | 写路径同步 hot cache 插入 | 消除空缓存启动与 read-after-write 惩罚 | 已完成 |
| 3 | 压缩 full-block 解压后填充 page_cache | 压缩默认启用时避免重复 full-block 解压 | 已完成 |
| 4 | `insert_opportunistic` per-key 去重/CAS | 避免并发磁盘缓存双写与统计偏差 | 待做 |
| 5 | 自适应晋升阈值调优 | 提升 hot cache 命中率 | 待做 |
| 6 | slice 读失败指数退避重试 | 降低瞬时 S3 错误导致的永久失败 | 待做 |
| 7 | 磁盘缓存健康状态机 + tail prefetch | 生产鲁棒性与顺序读尾延迟 | 待做 |

## 当前建议

短期聚焦 `doc/superpowers/plans/2026-05-23-read-cache-optimization.md` 的 P0/P1。P0 已经能提升读后命中和写后命中；P1 解决并发插入和晋升策略后，再用 `fio-randread`/`fio-seqread` 验证是否接近目标: `randread 29 -> 200 MiB/s`, `seqread 134 -> 500 MiB/s`, cache hit rate `~20% -> 80%+`。
