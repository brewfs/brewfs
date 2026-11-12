# Packed v3 Metadata Cache Analysis (2026-09-29)

## 结论

二进制编码本身只减少解析、分配和元数据请求次数，不能自动消除对象存储
HTTP RTT。之前的 packed 小文件结果还混入了一个更严重的读路径问题：严格
冷读使用 `read_memory_bytes=0`、`read_ssd_bytes=0` 时，mount 仍创建并传入
`ChunksCache`。`RemotePackedObject` 只要看到 cache 对象就尝试整 container
下载；空预算不能保留它，于是后续每个 frame miss 都可能重新整对象下载。
这既不是严格 cold，也会把 GroupMeta 的收益淹没在数据传输中。

## 多维度瓶颈拆分

| 维度 | packed v3 当前路径 | JuiceFS/Redis 参考 | 结论 |
| --- | --- | --- | --- |
| wire/解析 | 一个有界 GroupMeta，前缀压缩，二进制固定字段 | 多条 KV value/命令解析 | packed 有优势，但只影响 CPU 和 metadata bytes |
| metadata RTT | inode index、group index、GroupMeta 需要 OSS range | Redis 查询在内存完成 | cold、低 RTT 场景 Redis 可能更快；需要 warm-up 或持久 metadata cache |
| metadata 驻留 | 原来主要按请求被动填充；stat 的 II05 与 locator 没有完全共享 | Redis 元数据常驻内存 | 需要 inode 热项、GroupMeta singleflight 和显式 warm-up |
| data 读 | 0 budget 仍误走 whole-container materialization | JuiceFS 可用本地 cache 或并发 block read | 这是此前结果失真的主要原因；0 budget 必须精确 frame range |
| page 查找 CPU | inode page 每次线性扫描最多 4096 条 | Redis key lookup 近似 O(1) | II05 页内应二分，热点 inode 记录应有 byte-budget cache |
| 目录扫描 | pageable page 已有 cache，但 warm-up 前首次扫描仍串行触发 miss | Redis children/attr cache | mount 级 metadata warm-up 可把可预测的只读扫描移到启动阶段 |

## 本轮修改

1. `RemoteGroupCatalog` 增加 16 MiB inode-entry byte budget，并按 inode 使用
   二分查找；`with_metadata_cache_bytes` 现在把总预算同时分给 pageable
   index page、GroupMeta、inode 热项和 file locator，避免 index page 在总预算
   之外无限增长。
2. `lookup_entry*` 成功后立即写入 locator cache，使 lookup/open/read 复用同一
   `PackedFileLocator`，不再重复从 GroupMeta 克隆 extents/inline bytes。
3. 增加 `prefetch_metadata(concurrency, max_groups)`：并行预热有界 inode/group
   index，并按预算预热 GroupMeta。它不读取 data frame，耗时和页数单独记录。
   readonly mount 默认使用 `auto`；显式设置 `off` 才关闭这一步。手工覆盖时可用：

   ```text
   BREWFS_PACKED_METADATA_PREFETCH=true
   BREWFS_PACKED_METADATA_PREFETCH_CONCURRENCY=8
   BREWFS_PACKED_METADATA_PREFETCH_MAX_GROUPS=<optional>
   ```

4. index page 改为一次有界整页 range stream，避免 header/body/footer 三个
   metadata RTT；仍受 `MAX_PACKED_STREAM_RANGE_BYTES` 限制并校验 envelope/digest。
5. 0 data-cache budget 时不构造 packed payload cache；即使调用方误传入空
   `ChunksCache`，reader 也会检测 `has_read_capacity()` 并回退到精确 frame range。
6. metadata warm-up 改为分页流式处理，不再先把全部 group descriptor 收集到
   临时 `Vec`。`auto` 会分别用 manifest 的 index object 长度估算 inode/group
   index 子预算，在各自预算内预热稳定页前缀；即使百万级索引整体超过预算，也不会
   整批放弃。随后按稳定 group 顺序以保守的 2x decoded-footprint 估算填充 GroupMeta
   子预算，超出的尾部 group 留给按需 LRU，避免下载后立即淘汰。目录页返回前还会把
   其中的 inode/group locator 有界 admission 到同一 cache，使随后的
   getattr/get_slices/read 复用已解码 entry。卸载时日志还会输出各类 metadata
   remote GET/bytes，便于把“解析快”与“仍然付出了 OSS RTT”区分开。
7. frame directory 也纳入 metadata budget。`auto` 会按 container 的固定宽度
   frame table 估算稳定前缀并提前读取；后续首个文件 read 只需拉 payload，不再把
   frame-directory prefix/table RTT 混在数据延迟中。新增
   `packed_frame_directory_remote_gets/bytes` 和驻留条目统计，预算不足时尾部仍按
   demand single-flight 读取。
8. demand read 不再因为一个文件缺少 descriptor 就加载整个 frame table。catalog
   现在调用 `read_frame_descriptors`，只合并请求文件实际引用的连续 ordinal，且每个
   range 受 8 MiB 上限约束。完整 table 只由显式 metadata warm-up 读取；日志另外输出
   `packed_frame_descriptor_remote_gets/bytes`，因此可以区分“按需 descriptor 成本”和
   “预热目录成本”。
9. 单条 descriptor cache 也纳入总 metadata byte budget（默认最多 4 MiB），不再只按
   65,536 条计数。卸载统计同时报告 descriptor 的驻留条目和字节，避免完整目录 cache
   与按需 descriptor cache 的重复占用变成未观测的内存。
10. metadata warm-up 现在按 `container_ordinal` 对相邻 GroupMeta 做有界范围合并：
    合并范围最多 8 MiB，允许的空洞最多 64 KiB；收到后仍按每个 group 的精确
    `meta_offset/meta_len` 切片，分别校验 digest、解码 GM06 并 admission 到同一个
    byte-budget cache。这样一个 container 中的多个目录 shard 不再各自承担一次
    OSS RTT，而随机 demand read 仍保持精确 group range，不会因为 warm-up 优化而
    偷读无关文件。`group_meta_remote_gets/bytes` 现在反映合并后的实际 range 请求。

## 本轮追加修正（2026-09-29）

11. adaptive warm-up 原来每收集 `concurrency` 个 group 就提交一批。默认并发为
    8 时，同一个 GroupContainer 内的连续 GroupMeta 会被人为拆成多批，仍然产生
    许多重复 RTT。现在先在 GroupMeta byte budget 内选出稳定 group 前缀，再由一个
    bounded range planner 跨全部已选 group 按 container/offset 合并；range 仍限制
    为最多 8 MiB、空洞最多 64 KiB，in-flight 数量仍由 `concurrency` 限制。
12. 目录分页跳过前置 group 时不再下载并解码每个 GroupMeta。`PackedGroupRef.entry_count`
    是 manifest 的认证页长度提示，`readdir_page(child_offset, ...)` 先用它扣减
    cursor，只读取真正包含返回项的 group。目标 GroupMeta 仍做 digest、GM06 解码和
    `entry_count` 一致性校验；这只减少深分页的 metadata RTT，不改变返回顺序。
13. 预热结束前显式等待所有 Moka metadata tiers 的 pending maintenance 完成，避免
    mount 已开始服务但刚下载的 page 尚未完成 admission/eviction。统计因此更接近
    FUSE 首个请求看到的实际驻留状态。

## 本轮追加修正（locator admission 与 ordinal fast path）

之前的 warm-up 只把完整 GroupMeta page 放入缓存；首个 `get_slices/read` 仍会
再次走 inode-page 命中、GroupMeta name 二分和 `GroupMetaEntry` clone。现在 warm-up
在独立的 locator byte 子预算内按 GroupMeta 的稳定顺序 admission 热点
`PackedFileLocator`。这个 tier 不读取数据，也不改变 GroupMeta 的认证边界；预算耗尽
后剩余 entry 仍走原来的按需路径，并在 warm-up 日志中单独报告 admitted/skipped。

II05 已经认证每个 inode 的 `entry_ordinal`，读路径现在直接按 ordinal 访问 GroupMeta
entry，再校验 name 和 inode 一致性。这样 metadata-warm/cold-data 的首读不再为同一
记录做第二次 name search；恶意或损坏的 index 仍会 fail closed。

这项修改只降低本地 metadata CPU、分配和 FUSE 首读的 lookup 层开销；它不能消除
严格冷读中每个独立 payload 的 OSS range RTT。因此新结果必须同时报告
`locator_entries`、`metadata_group_meta_*` 和 payload/window 请求，不能把 locator
命中写成 data-cache 命中。

## 验证口径

必须至少分开跑以下两个 profile：

* `strict-cold`: 两个 data cache budget 为零、清理 OS page cache、关闭 data
  prefetch；报告精确 range GET/file 和 metadata cache hit/miss。
* `metadata-warm-cold-data`: data cache 仍为零，只打开 metadata warm-up；把
  warm-up 耗时计入 mount/setup 总时间，同时单独报告 FUSE 扫描阶段吞吐。

不能把 metadata warm-up 或 data frame window 命中写成 data-cache hit，也不能
把带有本地 payload cache 的 JuiceFS 结果与 strict-cold packed 结果直接比较。

本轮局部验证：packed-v3 定向测试 52 个用例通过；新增的 zero-budget payload
测试和 pageable metadata warm-up 测试通过。Aliyun 端到端结果需在同一数据集、
同一 OSS、相同并发和相同缓存预算下重新采集后才能更新性能表。

本轮新增回归：同一 GroupContainer 的两个相邻 GroupMeta 在 warm-up 中只产生一个
metadata range GET，且每个 GM06 block 的 digest/边界校验仍单独执行；完整
`packed_v3::catalog` 测试 13/13 通过。该优化只降低 metadata warm-up 的请求 RTT，
不等价于 payload/data cache 命中，也不改变 strict-cold 的按需 descriptor/data 读取。

本轮另加目录深分页回归：跳过第一个 group 直接读取第二个 group 时，记录到的 range
只包含第二个 GroupMeta；该路径不再为 cursor 前的 group 支付远程 metadata GET。

## 本轮追加修正（II05 inode-entry admission，2026-09-30）

之前的 warm-up 已经读取并解码 II05 inode index page，但页内记录只在第一次
`stat/get_names/get_paths` 时才被二分查找并复制到 `inode_entries` 热缓存。对百万级
只读扫描，这会把每个首访文件的 page lookup、记录 clone 和异步 cache admission
重新放回 FUSE 请求路径；它没有增加 OSS GET，却会放大 CPU、分配和调度噪声，让“二进制
metadata 已预热”的指标不完整。

现在 adaptive/eager warm-up 在独立的 inode-entry byte budget 内按 II05 页和 inode
顺序 admission 记录，预算耗尽后保持稳定前缀，尾部仍按需加载。启动日志新增：

* `inode_entries`：预热时可直接命中的 II05 记录数；
* `inode_entry_bytes`：这些记录的保守解码权重；
* `inode_entries_skipped`：由于 entry 子预算不足而留给 demand path 的记录数。

该 tier 只复制固定属性和文件名，不读 GroupMeta、frame descriptor 或 payload；因此它
不会把元数据命中伪装成数据缓存命中。它也不改变 page cache 的认证边界：II05 page
仍是 canonical source，entry cache 只是 bounded hot view。新增回归确认预热后 inode
lookup 命中 entry cache，并且 inode index object 只远程读取一次。

### 为什么这仍不能单独击败 JuiceFS

二进制 metadata 的优势需要拆成四个独立量：wire bytes/解析 CPU、metadata RTT、
本地驻留命中率，以及 payload/FUSE 请求数。Redis 在同机时 metadata RTT 约几十微秒；
packed 即使把索引页常驻内存，冷读仍要承担 OSS range RTT 和每文件 FUSE read。因而：

1. tree-only 或 metadata-warm 的扫描才是比较编码与 KV 的实验；
2. 完整 payload 扫描必须另外报告 frame/window GET 数和 overscan；
3. 只有远端 KV RTT 明显高、packed metadata 真正 warm、并且 group window/并发读取
   合并了相邻 payload 时，二进制格式的理论优势才会传导到端到端吞吐。

## 本轮候选（coordinator batching delay，2026-09-30）

### 假设

`SharedGroupReadCoordinator` 原来在每次收到 pending read 后固定等待 1 ms，给相邻
FUSE 请求留下合并机会。对单文件冷读或低并发随机扫描，如果没有第二个请求，这个等待
不会产生任何 range 合并收益，却直接增加每个读请求的端到端延迟。将收集窗口缩短到
250 us 应保留同一调度 tick 内的合并能力，同时减少无合并收益的固定等待。

### 变更与边界

变更仅在 `src/workspace_overlay/packed_v3/coordinator.rs`，不改变 range planner、
digest/CRC/长度/边界校验、共享 byte/range budget 或窗口 cache。新增单元回归固定该
收集窗口小于 1 ms；没有 benchmark-only 分支，也没有改变 strict-cold 的对象布局。

### 验证记录

本候选尚未获得端到端性能验收数字。目标实验应使用已有的 10k/100k OSS fixture，
保持 `BREWFS_PACKED_FRAME_WINDOW_CACHE_BYTES=0`、payload memory/SSD cache 为 0、
metadata cache/prefetch 设置与匹配基线一致，并在 `sync; echo 3 >/proc/sys/vm/drop_caches`
后分别运行 packed 与 JuiceFS；报告 FUSE active bandwidth、request/GET 数、对象放大和
`effective_active_plus_drain_bw_mib_s`。现有匹配参考证据为：

* packed：`docker/compose-xfstests/artifacts/aliyun-packed-v3-vs-juicefs-100k-20260928-r3/`
* JuiceFS：同一目录下的 `juicefs/` strict-cold 结果


## 2026-09-30 strict streaming and runtime metrics update

本轮在保持 `BRFPM004/BRFGC004`、PM06/GC04/GM06/II05 不变的前提下补齐了两项可
独立验证的读路径能力：

1. strict/coalesced payload range 现在通过 bounded chunk consumer 逐块消费，按 frame
   descriptor 把 chunk 分发到独立 `BytesMut`，完成后逐 frame 校验 16-byte digest；coalesced
   gap bytes 不再先 materialize 成完整 range `Vec`。window cache 仍是显式 opt-in 的完整
   对齐窗口，metadata `read_exact_range` 仍保留 bounded `Vec` 兼容 wrapper。
2. 新增 mount-scoped `PackedRuntimeMetrics`，在实际 coordinator/remote 边界统计 data
   range GET/bytes、logical/overscan、decoded frames/size class、coalesced/singleflight、
   pipeline current/peak、window hit/miss/fetch 和 data-cache hit；packed mount 卸载日志会
   输出 snapshot，runner 改为 `RUST_LOG=info` 以保留这些字段。`.stats`/Prometheus sink、
   cold attributes、完整 overlay lower binding 和压缩/restart 新 wire 仍未宣称完成。

本地验证通过：packed-v3 focused suite `64 passed`；workspace hard gate
`CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins`
为 `1097 passed, 0 failed, 225 ignored`；workspace check/build/clippy、fmt、脚本门禁及
fuse-tokio/io-uring feature checks 均通过。

本轮两次 10k Aliyun runner 尝试均在执行前被 Claude Code 安全策略以
`Data Exfiltration` 拒绝，未创建 ECS、挂载、OSS 对象或临时凭据；因此没有新的云端
吞吐结论，也没有更新 README 性能表。已有 matched JuiceFS reference 仍是性能基线。

## 显式 decoded-frame warm cache（2026-10-01）

为重复 epoch/训练集扫描增加了独立的、byte-budgeted decoded-frame cache。它只在
`BREWFS_PACKED_DECODED_FRAME_CACHE_BYTES > 0` 时启用；默认值为 0，因此
`strict-cold` 和 `cold-pipelined` 请求图不变。cache key 是
`(container_ordinal, frame_ordinal)`，只 admission 已完成长度和 frame digest 校验的
payload；Moka weigher 按实际 frame bytes 计费，日志单独报告 configured/resident bytes、
hit/miss/eviction，命中同时计为明确的 data-cache hit。

本机 RustFS/S3-compatible HTTP、1,000 个独立 100 KiB 文件、16 workers、direct I/O、
persistent payload cache=0、window cache=0 的两遍完整读取 A/B：

| profile | pass 1 | pass 2 | two-pass total | backend GET | fetched bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| decoded cache 0 | 2.412 s / 414.55 files/s | 2.776 s / 360.20 files/s | 5.188 s | 714 | 294,912,000 |
| decoded cache 64 MiB | 2.778 s / 359.99 files/s | 1.175 s / 851.33 files/s | 3.952 s | 414 | 167,321,600 |

两边 checksum 均为 `124948`、errors=0。候选第二遍 files/s 为 baseline 的 `2.36x`，
两遍总耗时改善约 `1.31x`；代价是首遍约 15% admission 开销和 64 MiB 显式内存预算。
因此该能力只接受为 `warm-frame-cache` profile，不作为 strict-cold 默认值，也不能替代
metadata-only/TiKV 对照或已有 full-payload cold 结论。
