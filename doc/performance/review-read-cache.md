# BrewFS 读路径与缓存模块审查

## 现状摘要

本次审查范围覆盖 `src/vfs/io/reader.rs`、`src/vfs/cache/*`、`src/chunk/cache.rs`、`src/chunk/page_cache.rs`、`src/chunk/slice.rs`，并只读对照了 `brewfs/juicefs/pkg/vfs/reader.go` 与 `brewfs/juicefs/pkg/chunk/cached_store.go`。

当前 BrewFS 读路径大致为：

1. `VFS::read` 先尝试 dirty/recently committed overlay 快路径，未覆盖时进入 handle reader。
2. `FileReader::read_at` 按 chunk 拆分 span，维护 per-handle `SliceState` 和 `chunk_slices` 元数据缓存，但数据面不再由 `SliceState.page` 承载。
3. `read_chunk_span` 通过 `DataFetcher::with_slices/read_at` 解析 slice 覆盖关系，再按 block span 调用 `BlockStore::read_range`。
4. `ObjectBlockStore::read_range` 先查 `ChunksCache` full block，再根据压缩和 range 阈值选择 64KiB `ReadPageCache` + Range GET，或 full GET + decompression + block cache insert。
5. `GlobalPrefetcher` 在每次成功 VFS read 后提交用户态顺序预读；小 range miss 还会在 `ObjectBlockStore` 内触发 full-block 后台预取。
6. FUSE 默认 `FOPEN_KEEP_CACHE`、`write_back(true)`、`max_readahead=16MiB`；只有设置 `BREWFS_FUSE_READ_DIRECT_IO=1` 且只读 handle 时才会加 `FOPEN_DIRECT_IO`。

这套架构已经把数据缓存统一到了 `BlockStore` 层，方向比早期 per-handle reader page cache 更好；但当前还有明显的调度、统计和读放大问题。已知 perf 现象也吻合：BrewFS `randread` 曾达到 701 MiB/s，说明热 full-block cache/并发读路径并非完全失效；但 `randrw` read p99 可到秒级，且低水位配置下 cache hit 96.8%、S3 GET avg 7.65ms 仍有 read/write tail，说明长尾更可能来自后台预取、range/full GET、磁盘 cache 写入、writer upload、kernel page cache/readahead 之间的资源竞争，以及统计口径无法定位具体策略。

## 具体问题 / 风险 / 性能瓶颈

### 1. P0: 读侧前台 I/O 与后台预取缺少统一优先级和预算

- 位置：`src/vfs/io/reader.rs::DataReader::submit_prefetch` 约 138-166 行；`src/vfs/cache/prefetch.rs::GlobalPrefetcher::worker_loop` 约 135-199 行；`src/chunk/store.rs::prefetch_full_block_background` 约 521-583 行。
- 原因：VFS 层每次 read 成功都会提交顺序预读，range miss 又会触发 full-block 后台预取。两套预取分别受 `prefetch_concurrency` 与 `range_prefetch_limit` 控制，但没有和前台 `read_range`、writer upload、S3 bandwidth limiter、disk cache 写入形成统一队列。`GlobalPrefetcher` 只在批内 sort priority，实际 `PrefetchPriority::Demand` 目前没有前台 demand 入口，且 submit 队列满时直接丢任务，不反馈压力。
- 建议改法：引入读侧 I/O scheduler 或至少共享的前台/后台令牌：foreground full/range GET 最高优先级，dirty overlay 和 user read 不被后台预取阻塞；range-triggered full prefetch 和 VFS readahead 在 S3 in-flight、writeback dirty、disk write permit、recent read p99 升高时降速或停发。把 `submit_prefetch` 的随机读识别做成显式状态，randrw 下禁止或极限收缩 ahead。
- 验证方式：增加 `.stats` 指标：foreground full/range GET in-flight、background prefetch in-flight、dropped by pressure、queue wait p50/p99。跑 `fio randrw --bs=4k/64k/1m --direct=1/0`，对比 read p99/p999、S3 GET ops、background prefetch ops；预期 p99 明显下降，吞吐最多小幅回退。

### 2. P0: cache hit 统计不能解释“S3 GET 仍多”

- 位置：`src/chunk/cache.rs::ChunksCache::get` 约 1665-1710 行；`src/chunk/store.rs::ObjectStoreMetrics` 约 95-222 行；`src/vfs/stats.rs` 约 80-89、553-574、907-950 行。
- 原因：`cache_hits/cache_misses` 只统计 `ChunksCache` full-block 层，page cache 命中、piggyback、range GET、full GET、background prefetch 是另一组 counter。低水位下 “cache hit 96.8%” 可能只是 block cache 请求命中率，仍可能存在大量小 range page miss、后台 full GET、写后缓存异步持久化失败后的冷 miss。当前 `read_range_gets` 在任意 page miss 后只加 1，无法表达一次 read 命中了多少页、发了多少 Range GET，也无法区分前台和后台 GET 的 latency。
- 建议改法：将读策略统计改为互斥且可分层：每次 `ObjectBlockStore::read_range` 记录 primary outcome（block_hit/page_all_hit/page_partial_miss/range_get/full_get/piggyback/zero_or_missing），同时记录 object GET source（foreground_full、foreground_range、background_prefetch、writeback_cache_fill）。Range GET 计数按实际 page GET 数和字节数记录。
- 验证方式：压测后用 `.stats` 计算 `S3 GET ops == foreground_range + foreground_full + background_prefetch + other`，并能解释 cache hit 96.8% 时剩余 3.2% miss 是否足以造成 observed S3 GET 和 tail。

### 3. P0: range miss 后 full-block 后台预取可能放大 randrw tail

- 位置：`src/chunk/store.rs::read_range` 约 697-812 行，尤其 806-810 行；`prefetch_full_block_background` 约 521-583 行。
- 原因：未压缩且 `offset > 0 && len <= block_size*0.25` 时走 64KiB page range read；只要 `range_missed && total_read > 0` 且无法由 page cache 组装 full block，就触发 full-block 后台预取。randrw 小随机读会产生大量不连续 page miss，随后又异步拉 4MiB full block，前台 GET avg 只有 7.65ms 时，额外后台 GET 更容易把 S3/网络/tokio 调度拖成长尾。
- 建议改法：把 range 后 full-block prefetch 变成自适应 admission：同一 block 的 page 覆盖率超过阈值、连续访问次数达到阈值、或检测到顺序/局部性时才拉 full block；randrw 或 write pressure 高时只保留 page cache，不做 full prefetch。参考 JuiceFS `loadRange` 成功后会 `fetcher.fetch(key)`，但它有独立 prefetcher 和 cache manager 约束，BrewFS 需要补齐压力门控。
- 验证方式：做 A/B：关闭 range-triggered full prefetch、覆盖率阈值 25%/50%/75%。观察 randrw read p99、S3 GET bytes、read_background_prefetch_total、cache hit ratio。理想结果是 tail 下降，randread 热读不明显退化。

### 4. P0: 压缩默认 LZ4 导致小读无法走 Range GET，full GET 可能隐藏在高 hit 之后

- 位置：`src/vfs/cache/config.rs::CacheConfig::default` 约 69-95 行默认 `compression=Lz4`；`src/chunk/store.rs::read_range` 约 697 行要求 `Compression::None` 才走 range；约 821-876 行 full GET + decompression + cache insert。
- 原因：默认 LZ4 时，小范围读全部绕过 Range GET，走 full object GET 后解压完整 block。热 cache 下吞吐可高，但混合读写、cache 被挤压或新 slice 读取时，每次 4KiB/64KiB 读都可能转化为 4MiB full GET/decompress/cache insert。S3 GET avg 7.65ms 不高，但 full GET 的网络字节、解压 CPU 和 cache 插入会把 p99 拉长。
- 建议改法：短期为性能测试提供 `compression=None` 明确 profile，并在 `.stats` 输出 compression mode；中期记录压缩块的原始大小和压缩大小，根据对象大小/请求长度决定 full GET 是否值得；长期可考虑块内分片压缩或压缩索引，避免小读必须全块解压。
- 验证方式：同一数据集分别以 LZ4/None 格式跑 randread/randrw，比较 `read_full_gets_total`、S3 GET bytes、CPU、read p99。若 LZ4 下 full_get 占主导，应优先优化压缩小读策略。

### 5. P1: `DataFetcher::read_at` 为每个 block span 创建无上限并发 future

- 位置：`src/chunk/reader.rs::DataFetcher::read_at` 约 139-204 行；`read_at_into` 约 228-285 行。
- 原因：每个需要读取的 slice/block span 都 push 到 `FuturesUnordered`，没有 per-read 或全局并发上限。slice 碎片多时，一个 FUSE read 可展开成大量 `BlockStore::read_range`，这些 read 又各自可能触发 full GET/range GET/prefetch。randrw 与写入造成 slice 增多时，这会放大调度竞争和 S3 burst。
- 建议改法：引入每个 `DataFetcher` 的并发上限，并按 block key 合并同一 block 的多个 sub-range；或者先构建 resolved extents，再按 block key 去重后读取，多个覆盖片段在内存中裁剪。
- 验证方式：构造一个 chunk 内 100/1000 个重叠小 slice 的读测试，记录单次 read 的 `need_reads`、`read_block` future 数、S3 GET ops 和 p99。优化后 future 数应随 block key 数而不是 slice fragment 数增长。

### 6. P1: `get_slices` 返回整 chunk slice 列表，碎片化时读路径 CPU/元数据成本随历史写入放大

- 位置：`src/meta/client/mod.rs::MetaLayer for MetaClient::get_slices` 约 2317-2348 行；Redis backend `get_slices` 约 2980-2996 行；database backend 约 2759-2770 行；etcd backend 约 2533-2542 行；`DataFetcher::read_at` 约 120-133 行倒序 cut。
- 原因：每次 chunk miss 都拿完整 slice 列表，然后在 reader 侧倒序做 interval cut。元数据 cache 命中能省 RTT，但不能省 slice 列表复制、反序扫描和覆盖解析。randrw 写入越碎，read tail 越容易受 slice_count 影响。
- 建议改法：短期在 `DataFetcher` 增加 slice_count/need_reads histogram 和大 slice_count warning；中期在 meta 层提供 `get_slices_for_range(chunk_id, offset, len)` 或返回已裁剪的 visible extents；长期把 compaction 与读侧阈值联动，slice_count 超阈值优先 light compact。
- 验证方式：用人工构造重叠 slice 的 microbench，比较 get_slices cache hit 下纯 CPU read latency；压测中关联 `meta_get_slices_cache_hit/miss`、slice_count p99 与 read p99。

### 7. P1: `FileReader` 的 `chunk_slices` 是 per-handle cache，跨 handle/process 复用有限且失效依赖写路径

- 位置：`src/vfs/io/reader.rs::FileReader` 字段 `chunk_slices` 约 492-495 行；`read_chunk_span` 约 858-879 行；`invalidate` 约 981-994 行；MetaClient inode cache slice map 约 `src/meta/client/cache.rs` 342-400 行。
- 原因：同一个 inode 的不同 file handle 会各自维护 `chunk_slices`，而 MetaClient 也有 inode-level slice cache。双层缓存降低 RTT，但也增加一致性边界。当前本进程写入通过 `invalidate` 清 per-handle cache，MetaClient `invalidate_chunk_slices` 清 inode cache；跨客户端依赖 backend/watch/TTL，读路径没有 chunk version 校验。
- 建议改法：去掉或弱化 per-handle `chunk_slices`，统一使用 MetaClient inode-level cache + version/epoch；或者在 `chunk_slices` value 中带 chunk version，写入/compaction 后版本不一致时自动 miss。至少把 per-handle cache hit/miss 暴露出来。
- 验证方式：多 handle、多客户端、compaction 后读一致性测试；并记录 per-handle cache hit 是否真的带来收益。如果 MetaClient hit 已高，删除 per-handle cache 应不显著降低吞吐。

### 8. P1: full-block cache 插入和磁盘 cache 写入仍可能影响前台 tail

- 位置：`src/chunk/store.rs::read_range` 约 871-876 行；`src/chunk/cache.rs::insert_opportunistic` 约 1753-1818 行；`insert_hot` 约 1746-1751 行；`DiskStorage::store_with_permit` 约 374-436 行。
- 原因：full GET 路径 copy 给前台后仍 await `insert_opportunistic`，其中 `insert_hot` 会 `run_pending_tasks().await` 并更新 moka eviction。磁盘写是 opportunistic spawn，但获取 permit、cold cache 判断和 hot cache eviction 仍在前台 await 链上。高并发读写下，这部分可能把“GET avg 7.65ms”之后的用户可见 latency 拉长。
- 建议改法：前台只做 O(1) hot insert 或返回后异步缓存；把 cache insert latency 单独计入 `read_cache_insert_lat_us`。对已读到的数据可先返回，再由 detached task 写 hot/disk cache，但要评估下一次同 block 读的命中窗口。
- 验证方式：在 `read_range` 增加分段 timing：cache_lookup、object_get、decompress、copy、hot_insert、disk_schedule。对比同步/异步 hot insert 下 read p99 和后续 cache hit。

### 9. P1: `ReadPageCache` 容量按页数而非字节权重，TTL/TTI 固定，缺少压力反馈

- 位置：`src/chunk/page_cache.rs::ReadPageCache::new` 约 49-55 行；`ObjectBlockStore::new*` 约 376、446 行。
- 原因：默认 4096 页约 256MiB，和 `CacheConfig.read_memory_bytes`、`memory_budget_bytes`、`ChunksCache.max_hot_bytes` 没有统一预算。页面 TTL 120s、TTI 30s 固定，randrw 下可能缓存大量低复用 page，同时 full-block cache 又占 4GiB 默认热内存。
- 建议改法：把 page cache capacity 纳入 `CacheConfig`，按 `read_memory_bytes` 分配给 block/page cache，并在 memory pressure 高时缩短 TTI 或停止 page admission。page cache 应暴露 entry/bytes/eviction 指标。
- 验证方式：在低水位和默认配置下跑 randrw，观测 RSS、page cache entries、block hot bytes、eviction。调低 page cache 后 read p99 不应恶化，RSS 和后台 I/O 应下降。

### 10. P1: `direct=0` 与 kernel page cache/readahead 会干扰读缓存判断

- 位置：`src/fuse/mod.rs::fuse_open_reply_flags` 约 62-79 行；`src/fuse/mount.rs::default_mount_options` 约 27-45 行。
- 原因：默认 `FOPEN_KEEP_CACHE` + kernel `max_readahead=16MiB` + writeback cache。fio `direct=0` 时，部分 read 根本不会进入 BrewFS 用户态，另一些 read 会被 kernel readahead 改造成更大/更顺序的请求。这样用户态 cache hit 96.8%、S3 GET avg 7.65ms 与 fio tail 不再一一对应。
- 建议改法：性能结论必须分开 `direct=1`、`direct=0`；压测脚本输出 mount flags、`BREWFS_FUSE_READ_DIRECT_IO`、kernel readahead、fio direct。必要时新增运行时开关：禁用 `KEEP_CACHE` 或降低 `max_readahead` 做诊断。
- 验证方式：同一 workload 分别跑 `direct=1`、`direct=0`、`BREWFS_FUSE_READ_DIRECT_IO=1`，对比 FUSE read ops、bytes、avg size、read p99。若 direct=0 FUSE ops 显著少且 avg size 变大，说明内核 cache 已参与。

### 11. P2: `vfs/cache/read_cache.rs` 与 `lru_cache.rs` 仍像遗留接口，容易误导配置和诊断

- 位置：`src/vfs/cache/read_cache.rs` 约 1-28 行注释 “currently bypassed by ObjectBlockStore”；`src/vfs/cache/mod.rs` 约 1-7 行。
- 原因：`ReadCache` trait 表示 VFS 全局 read-through block cache，但主读路径实际使用 `ObjectBlockStore` 内部 `ChunksCache`。保留未接入接口会让 reviewer/配置使用者误以为有两套 read cache 同时工作，甚至误判 hit/miss 指标。
- 建议改法：文档和代码注释明确 `vfs/cache/read_cache.rs` 暂未接入；后续要么删除/feature-gate，要么把它适配到 `BlockStore` 层，避免两套同名 read cache。
- 验证方式：`rg "ReadCache|get_block|put_block"` 确认无主路径调用；补一个架构文档段落说明实际缓存层次。

### 12. P2: `DiskStorage::evict_lru` 扫目录和 atime 更新在高 churn 下可能产生本地 IO 抖动

- 位置：`src/chunk/cache.rs::DiskStorage::load` 约 510-566 行；`evict_lru` 约 455-508 行；`store_with_permit` 约 390-436 行。
- 原因：disk cache 命中会 `utimensat` 更新 atime，插入超预算时扫整个 cache dir 并按 atime 排序。低水位配置或热集大于 SSD budget 时，后台 disk cache 写入可能形成本地 IO 抖动，间接影响前台 read/write tail。
- 建议改法：把 eviction 改为增量索引或分桶抽样；atime 更新做节流（例如同 key 1s 内只更新一次）或只维护内存 LRU metadata。低水位 profile 下可默认跳过 disk cache store 或提高 skip aggressiveness。
- 验证方式：用小 `read_ssd_bytes` 跑长时间 randrw，采集 disk write/read IOPS、eviction duration、read p99。优化后 eviction spikes 应减少。

## 最值得优先尝试的 3 个读侧优化方案

### 方案 A: 给前台读建立硬优先级，动态关停后台预取

- 内容：把 VFS readahead 和 range-triggered full-block prefetch 纳入同一读侧预算；当前台 full/range GET in-flight、writer dirty/upload、read p99 或 S3 latency 超阈值时，暂停后台预取。randrw 默认只保留 page cache，不主动 full prefetch。
- 预期收益：最直接针对 `randrw read p99 秒级`。在 S3 GET avg 7.65ms 的情况下，减少排队和背景 GET 抢占比继续提高 cache hit 更有价值。预计 read p99/p999 明显下降，write tail 也可能改善。
- 回退风险：顺序读和热身速度可能下降，`randread` 701 MiB/s 这类高吞吐场景可能轻微回退。可通过开关和阈值快速回退。

### 方案 B: 补齐读策略可观测性，先把 96.8% hit 拆成可解释账本

- 内容：新增互斥策略 counter 和 latency：block_hit、page_all_hit、page_partial_miss、range_get_pages、foreground_full_get、background_full_get、piggyback、cache_insert_lat、prefetch_queue_wait、slice_count/need_reads histogram。
- 预期收益：能准确解释 “cache hit 高但 S3 GET 仍多” 是 page miss、full prefetch、压缩 full GET、disk cache churn 还是元数据 slice 解析导致。后续优化不会盲调。
- 回退风险：指标增加有少量原子计数开销。可先放 `.stats` 和 tracing span，不改变行为。

### 方案 C: 元数据与 slice 解析裁剪，降低碎片化读放大

- 内容：短期在 `DataFetcher` 内按 block key 合并读取并限制并发；中期让 `MetaClient` 提供 range-aware visible extents，避免每次读取完整 chunk slice 列表；slice_count 超阈值时触发/提示 compaction。
- 预期收益：对 randrw、small overwrite、copy/truncate 后读 tail 更稳定，减少 `get_slices` cache hit 下仍有的 CPU 和对象 GET fan-out。也能降低 `DataFetcher` 无上限 future burst。
- 回退风险：range-aware extents 涉及元数据语义，正确性风险高于方案 A/B；建议先以 reader 侧只读裁剪和 microbench 验证开始。

## 建议的验证矩阵

- fio：`randread`、`randrw`，bs=4KiB/64KiB/1MiB/4MiB，`direct=1` 与 `direct=0` 分开。
- cache profile：默认配置、低水位配置、禁用 range full prefetch、compression=None、compression=Lz4。
- 观测指标：fio read/write p50/p95/p99/p999、BrewFS `.stats`、S3 GET ops/bytes/avg/p99、foreground/background GET 拆分、page/block cache hit、slice_count/need_reads、FUSE read ops/avg size、RSS、本地 cache disk IOPS。
- 正确性：多 handle 读写一致性、跨客户端写后读、compaction 后读、压缩/非压缩小 range、hole/overwrite overlay。

## 并行 agent 补充审查

### P0 补充：后台预取 worker 等 permit 时会阻塞队列推进

- 位置：`DataReader::submit_prefetch`、`GlobalPrefetcher::worker_loop`、`ObjectBlockStore::read_range` range miss 后的 full-block prefetch。
- 发现：VFS 顺序预读和 range-triggered full-block prefetch 是两套后台 I/O，但都没有与前台 read 建立硬优先级。`GlobalPrefetcher` 只在当前 batch 内排序，等待 semaphore permit 时会阻塞 worker 继续处理后续取消/高优先级任务；range miss 还会额外排入 4MiB full GET。
- 改动建议：先做低风险门控：randrw/随机读关闭 range full-block prefetch，或者在前台 read p99、S3 in-flight、writeback dirty 超阈值时暂停后台预取。随后把后台 prefetch permit 改成 try-acquire + requeue/drop low priority，避免 worker 卡在低优先级任务上。
- 验证：`BREWFS_RANGE_BACKGROUND_PREFETCH=false` 与默认对比 `fio-randrw fio-randread`，观察 read p99/p99.9、S3 GET bytes、background prefetch/drop 计数、randread 热身吞吐是否明显回退。

### P1 补充：`chunk_slices` 命中后仍 clone 整个 slice list

- 位置：`FileReader::read_chunk_span` 的 `chunk_slices` hit 路径、`DataFetcher::with_slices`。
- 发现：缓存值是 `Arc<Vec<SliceDesc>>`，但命中后仍 clone 成新的 `Vec` 传给 `DataFetcher`。当 direct=0 写入导致 slice list 很长时，per-handle cache 只省掉 metadata RTT，不能省每次 read 的 Vec clone 与倒序扫描成本。
- 改动建议：新增 `DataFetcher::with_slices_arc` 或让 `DataFetcher` 持有 `Arc<[SliceDesc]>`；短期先记录 per-read slice_count、clone bytes、visible extent count，用数据确认是否进入热路径。
- 验证：构造 100/1000 个 slice 的 chunk read microbench；优化后 get_slices cache hit 下 CPU/read latency 应下降，S3 GET 计数不变。

### P1 补充：读路径仍有多层分配，`read_at_into` 没有用于热路径

- 位置：`FileReader::read_at`、`read_chunk_span`、`chunk/reader.rs::DataFetcher::read_at/read_at_into`。
- 发现：每个 chunk span 先由 `DataFetcher::read_at` 分配 Vec，再转 Bytes，最后 `FileReader` 再 assemble 到新的 Vec。跨 chunk 大读至少多一次内存带宽消耗。
- 改动建议：在 `FileReader::read_at` 预分配最终输出 buffer，按 span 调 `DataFetcher::read_at_into`；保留现有 `read_at` 作为兼容 wrapper。
- 验证：`fio-randread` 与顺序大读对比 read BW、CPU、alloc profile、read p50/p99。

### P1 补充：压缩模式下 page cache 写入和读取策略不一致

- 位置：`ObjectBlockStore::read_range` range gate 与 `populate_page_cache_from_block`。
- 发现：range/page-cache path 只允许 `Compression::None`，但 full GET 后只有压缩模式会 populate page cache。默认 LZ4 下会填 256MiB page cache，却很难在 block miss 后走小读 page cache，可能浪费内存和 CPU。
- 改动建议：二选一：压缩模式不填 page cache；或小读先查 page cache，miss 后再因压缩退回 full GET。后者收益更大，但必须保证 page 对应的是已解压明文 offset。
- 验证：LZ4 下 A/B `populate page cache`，看 RSS、page cache hit、full GET 次数、read p99。

### P1 补充：SliceState refresh 会发真实 I/O 但丢弃数据

- 位置：`SliceState::background_fetch`、失效 refresh 调用点。
- 发现：数据缓存已经统一到 `BlockStore`，但 `background_fetch` 仍会读对象/块后只把状态置为 `Ready`，数据本身不进入 `SliceState`。这会制造非用户请求的真实 I/O。
- 改动建议：删除这类数据 fetch，改为 metadata-only 状态转换；需要验证数据时由下一次 demand read 走 `BlockStore`。
- 验证：失效/refresh 场景下统计 S3 GET ops，删除后非 demand GET 应下降，读正确性不变。

## 核心结论

当前 BrewFS 读路径的主问题不是“完全没有 cache”，而是 cache 层次和 I/O 调度还没有把前台读保护起来，同时局部热路径仍有“命中后 clone/拷贝”和“后台 I/O 抢前台”的成本。`randread` 能到 701 MiB/s 说明 full-block cache 热路径可用；但 `randrw` read p99 到秒级、低水位下 cache hit 96.8% 仍有 S3 GET 与 read/write tail，优先应从后台预取限流、读策略指标拆账、range/full GET admission、slice 解析放大和读路径拷贝减少五个方向收敛。
