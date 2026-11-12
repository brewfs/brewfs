# Aliyun packed-metadata-v3 冷读边界（2026-09-28）

## 本轮有效结果

测试使用 Aliyun `ecs.u1-c1m4.2xlarge`（32 GiB、100 GiB ESSD）和同一 OSS
区域；没有 Docker。两边都使用 1,000 个 100 KiB 文件、两级目录、10x10 叶目录、
每叶 10 个文件，完整读取并校验每个文件内容。两套 fixture 使用相同的文件序号
payload 模式，扫描 checksum 均为 `124948`。BrewFS 的 `read_memory_bytes=0`、
`read_ssd_bytes=0`、prefetch 全关闭，挂载前执行
`sync; echo 3 >/proc/sys/vm/drop_caches`；JuiceFS strict 使用
`cache-size=0`、`prefetch=0`、`attr/entry/dir-entry/open-cache=0`。

| profile | 端到端耗时 | 扫描器耗时 | 结果 |
| --- | ---: | ---: | --- |
| packed v3 strict cold | 29.043 s | 28.282 s，35.36 files/s | 1,000/1,000，102,400,000 payload bytes，缓存命中 0 |
| JuiceFS strict cold | 21.583 s | 21.524 s，46.46 files/s | 1,000/1,000，102,400,000 payload bytes，缓存关闭 |
| JuiceFS prefetch（补充） | 21.121 s | 21.062 s，47.48 files/s | 非 strict 对照，不用于冷读结论 |

此前 `aliyun-v3-tree` 中的 JuiceFS 数字（0.179794 s）只是 namespace/tree 扫描，
没有打开并读取文件 payload，不能与本表相除。上表是本轮在同一 ECS 上完成的
matched payload A/B；packed v3 在该低 RTT 场景为 JuiceFS strict 的 `0.743x`
（约慢 1.35 倍）。

索引页、GroupMeta 和 frame descriptor 的有界挂载期缓存把 packed 从约 127 秒降到
29 秒；但每个文件仍会触发独立的 OSS range 读取，且当前 scanner 是串行的。这个
固定的对象存储往返成本在同机 Redis metadata 基线下仍高于 JuiceFS，当前不能把
packed 表述为默认更快。

证据：

- `docker/compose-xfstests/artifacts/aliyun-v3-jfs-ab-20260928-packed/remote-output.log`
- `docker/compose-xfstests/artifacts/aliyun-v3-jfs-ab-20260928-jfs/remote-output.log`

测试结束后两次临时 ECS、所有本轮 OSS 前缀、OSS bucket 和临时凭据均已删除。

## 本轮读路径修正（2026-09-29）

这轮没有重新申请云资源，先修正一个会影响下一轮 A/B 的读路径问题：packed
已经有 4 MiB 的进程内窗口缓存，但此前没有顺序读的窗口 read-ahead。现在增加了
一个有界、显式开启的下一窗口预取：

```text
BREWFS_PACKED_FRAME_WINDOW_CACHE_BYTES=67108864
BREWFS_PACKED_FRAME_WINDOW_PREFETCH=true
```

只有同时满足以下条件才会触发：窗口预算非零、GroupContainer 的发布 profile 是
`SequentialSmallFile`、并且预取信号量还有空位。每个 foreground read 最多提交一个
相邻 4 MiB 窗口，后台任务不阻塞 foreground；窗口缓存从挂载时为空，也不写入 SSD。
严格 cold（窗口预算为 0，或 `...PREFETCH=false`）的精确 frame range 行为没有改变。

新增回归测试验证：

- strict frame read 仍只请求精确的 `(offset, stored_len)`；
- 相邻窗口命中只产生一次远程窗口拉取；
- sequential read-ahead 在窗口预算启用时确实填充下一个窗口。

该改动只解决跨文件顺序扫描的请求流水线，不改变“单线程严格禁止预取时每个独立
文件至少一次 payload RTT”的理论下界。下一次云端对照必须单独报告 strict 和
pipelined 两行，并记录 window hit/miss、远程窗口数、overscan bytes 和实际 payload
bytes，不能把 pipelined 数字写成 strict cold 结果。

## Inline payload 修复后的 10k 复测

随后使用修复后的 Linux release binary，在同类 Aliyun
`ecs.u1-c1m4.2xlarge`（32 GiB、100 GiB ESSD）上复用同一份 10,000 文件
fixture，单独运行 `packed-smallfiles`。fixture 为两级目录、10x10 叶目录、每叶
100 个 100 KiB 文件；`read_memory_bytes=0`、`read_ssd_bytes=0`、prefetch 和
range background prefetch 均关闭，读取模式为完整 payload 冷读。

结果为 `10000/10000`，`errors=0`、`walk_errors=0`，读取
`1,024,000,000` 字节，checksum `1273096`；扫描器耗时 `249.549116 s`，端到端
工具耗时 `250.359 s`，约 `40.07 files/s`。指标中
`brewfs_cache_hit_ratio=0`、`brewfs_read_block_cache_hits_total=0`、
`brewfs_read_page_cache_hits_total=0`，确认没有以缓存命中换取结果。

证据：

- `docker/compose-xfstests/artifacts/aliyun-v3-inline-retest-20260928/remote-output.log`

本次复测结束后删除了新旧 OSS 测试前缀和 ECS；`DescribeInstances` 返回
`TotalCount=0`，两个前缀的对象数均为 0。该结果只证明 inline payload 路径的
正确性和当前 10k 冷读基线，不宣称在低 RTT、同机 Redis 对照下已经超过 JuiceFS。

## Matched 10k strict cold A/B（同一 ECS，2026-09-28）

为回应“二进制 metadata 应该比 Redis 快”的疑问，在同一台
`ecs.u1-c1m4.2xlarge`（32 GiB、100 GiB ESSD）、同一 OSS bucket、同一目录树和
同一 10,000 个 100 KiB payload 上重新做了完整端到端读取。两边每轮开始前都执行
`sync; echo 3 >/proc/sys/vm/drop_caches`；packed 的内存/SSD cache、prefetch 和
range background prefetch 全关，JuiceFS 的 `cache-size=0`、`prefetch=0`、
`attr/entry/dir-entry/open-cache=0` 全关。两边均读取并校验全部 payload，checksum
均为 `1273096`，没有错误。

| profile | 端到端/扫描器耗时 | 速度 | 说明 |
| --- | ---: | ---: | --- |
| packed v3 strict cold | 239.344 s / 239.148 s | 41.82 files/s | 10,000/10,000，1,024,000,000 bytes |
| JuiceFS strict cold | 199.017 s / 198.954 s | 50.26 files/s | Redis loopback，payload 完整读取 |
| JuiceFS prefetch 补充 | 203.792 s / 203.725 s | 49.09 files/s | 非 strict，只作诊断 |

因此本轮 packed 是 JuiceFS strict 的 `0.832x`（慢约 `20.3%`）。这不是元数据
编码结论：packed 的 lookup 总延迟约 3.1 s，但 read 指标显示 20,000 次 FUSE
read、平均 10.96 ms；每个小文件仍承担独立 OSS range GET，且 scanner 串行。Redis
在 ECS loopback 上的 ping 只有约 44 µs，所以“少做 Redis 查询”被对象存储往返和
FUSE read 往返淹没。要验证二进制 metadata 的优势，下一轮必须单独测 tree-only
扫描，并实现跨文件 group window/并发读取后再做完整 payload A/B。

证据：

- `docker/compose-xfstests/artifacts/aliyun-ab-packed-10k-20260928/remote-output.log`
- `docker/compose-xfstests/artifacts/aliyun-ab-jfs-10k-20260928/remote-output.log`

本轮结束后已删除三个临时 OSS 前缀；每个前缀查询均为 `Object Number is: 0`，
并删除 ECS `i-bp1efmn5fano9515s0bx`，`DescribeInstances` 返回 `TotalCount=0`。

## 适用场景与验收矩阵

同机 Redis 是 JuiceFS 的有利基线，不是 packed 的目标优势场景。packed v3 应在
以下条件下验收：

1. JuiceFS 元数据服务位于跨主机/跨可用区网络，metadata RTT 和 p99 明确记录；
2. 数据访问是只读、冷启动、命名空间很大，且一个 packed group 能覆盖同一目录的
   多个文件；
3. 读取器具有并发 worker 或显式 group-level prefetch，使一个 OSS range 覆盖多个
   相邻动态 frame；
4. 对照两边使用相同 worker 数、相同 OSS、相同 drop-caches 流程，且不计入数据
   准备时间。

本轮增加了 `-MetadataLatencyMs`/`JFS_METADATA_LATENCY_MS` 诊断开关，在 JuiceFS
Redis 的 loopback 上注入固定 RTT，OSS 数据路径不注入延迟。10 ms 诊断点运行超过
约 5 分钟后主动停止，未产生可接受的 payload 数字；它只能说明当前 JuiceFS
metadata 往返放大值得单独优化，不能作为 packed 的胜利证据。要证明 packed 在此
场景领先，必须先实现跨文件 group prefetch/并发读取，再用一个有明确完成时间的
高 RTT matched A/B 重测。

下一轮只有在实现跨文件 group prefetch、记录 OSS range GET 数和并发度后，才扩大到
百万级文件。若 packed 仍无法把每个文件一次 OSS GET 合并成 group 级请求，则应接受
其更适合远端元数据高延迟和顺序训练数据，而不是同机 Redis 小文件基线。

## 100k matched SDK-upload/FUSE-read 对照（2026-09-28/29）

正式大规模对照使用同一台 `ecs.u1-c1m4.2xlarge`（32 GiB、100 GiB ESSD）、同一
OSS 内网端点、同一 100,000 文件树（2 层、10x10 叶目录、每叶 1,000 文件）、
100–1,000 KiB 随机文件和 16 个读取 worker。两边读取前均执行 `sync` 与
`drop_caches=3`，本地文件缓存预算为 0；packed 的内存/SSD cache、prefetch 和
range background prefetch 关闭，JuiceFS 的 `cache-size=0`、`prefetch=0`、
`attr/entry/dir-entry/open-cache=0`。

上传路径也分开记录：packed 由 `packed_v3_snapshot_fixture` SDK 发布 3,319 个
`.brfgc` container 及索引/manifest；JuiceFS 由同一 fixture SDK 上传 100,000 个
原始文件到 `juicefs/raw/`，再在准备阶段用 `juicefs sync` 导入卷，正式性能阶段
两边都只经 FUSE 读取。上传和导入时间不计入读取吞吐。

| profile | 完整 payload 扫描秒数 | files/s | MiB/s | 校验 |
| --- | ---: | ---: | ---: | --- |
| packed v3 cold | 3384.746851 | 29.54 | 16.22 | 100,000/100,000，errors=0，checksum=12742480 |
| JuiceFS strict cold | 351.235225 | 284.71 | 156.27 | 100,000/100,000，errors=0，checksum=12742480 |

因此 packed 为 JuiceFS 的 `0.1038x`，约慢 `9.64x`。packed stats 显示缓存命中率、
block/page cache hits 均为 0，但有 200,332 次 FUSE read、平均约 104.8 ms；当前
瓶颈是每个小文件的独立 OSS range/data 读取和 FUSE 往返，不能归因于二进制 metadata
解析，也不能通过这轮结果声称 packed 已优于 JuiceFS。下一步需要记录 OSS GET/Range
请求并实现跨文件 group window/异步读取，然后再补普通 BrewFS 三方对照。

证据：

- `docker/compose-xfstests/artifacts/aliyun-packed-v3-vs-juicefs-100k-20260928-r3/packed-run.log`
- `docker/compose-xfstests/artifacts/aliyun-packed-v3-vs-juicefs-100k-20260928-r3/packed/remote-output.log`
- `docker/compose-xfstests/artifacts/aliyun-packed-v3-vs-juicefs-100k-20260928-r3/juicefs/remote-output.log`

清理核验：ECS `i-bp10wczp9w20g7yjqizf` `TotalCount=0`，OSS 测试前缀对象数为 0。

## JuiceFS 本地缓存复核（2026-09-29）

针对“100k JuiceFS 速度可能复用了本地 SSD cache”的疑问，使用同一 Aliyun
区域、32 GiB/100 GiB ECS 和同样的 SDK-upload/FUSE-read 流程做了 10,000 文件
复核。该轮在扫描前删除 JuiceFS cache 目录并执行 `sync; echo 3 >/proc/sys/vm/drop_caches`，
挂载参数为 `cache-size=0`、`prefetch=0`、`max-fuse-io=4M`、`max-readahead=16M`，
同时关闭 attr/entry/dir-entry/open cache。

JuiceFS 完整 payload 扫描为 18.300526 s（10,000/10,000，5,759,098,576 bytes，
checksum `1273096`）。实际落盘证明为：

```text
cache_dir=/opt/juicefs-native/jfs-cache
cache_bytes_before=0 cache_bytes_after=0
cache_files_before=0 cache_files_after=0
```

因此该轮没有本地 SSD 数据缓存命中；旧 100k 结果虽然只保留了 `cache-size=0` 和
清理/drop-caches 配置，没有保存目录字节数，但其 57.5 GiB 工作集也不可能由默认的
本地缓存解释。当前 100k 的 JuiceFS 优势应归因于其 FUSE/块读取并发和数据路径，
而不是复用本地 SSD cache。后续报告应同时保留 `cache-proof.env`，避免只凭挂载参数
推断冷读。

证据：

- `docker/compose-xfstests/artifacts/aliyun-cache-proof-10k-20260929-r4/juicefs/remote-output.log`
- `docker/compose-xfstests/artifacts/aliyun-cache-proof-10k-20260929-r4/juicefs-run.log`

本轮结束后 ECS `i-bp19aylqsh27cn50y5st` 查询为 `TotalCount=0`，测试 bucket 对象数
为 0。

## 读路径修正（2026-09-29，尚未作为云端验收结果）

代码审查发现，之前的 `GroupReadCoordinator` 虽然支持合并 range，但真实
`read_inode_range` 一次只提交当前 FUSE read 所涉及的 frame；因此主路径实际仍是
每个 frame 一个精确 OSS Range GET，`read_frame` 中的 4 MiB 对齐窗口没有被使用。

本轮修正将 coordinator 的 payload 读取接到一个显式配置的、进程内 byte-budgeted
group-window cache：窗口在 mount 时为空，不落 SSD、不跨进程，命中只表示本次扫描中
相邻 frame 共享了同一个已下载的 OSS window。`BREWFS_PACKED_FRAME_WINDOW_CACHE_BYTES=0`
保持原来的严格逐 range 行为；非零值才启用窗口流水线。每个对象同时记录
`cache_hits`、`cache_misses` 和实际 `remote_fetches`，后续报告必须一起给出，不能把
窗口共享写成 warm-cache 结果。

对应的 focused 单元测试覆盖了：两个相邻 frame、一次远程窗口读取、一次窗口命中，且
校验 frame payload 未改变。云端重新对照前必须先用同一份 fixture 分别跑：

1. strict cold：窗口预算为 0；
2. pipelined cold：窗口预算非零但 mount 后从空 cache 开始；
3. JuiceFS strict/prefetch：保持 cache-size=0 的独立对照。

只有第二组能够显示 packed 的跨文件请求优势，不能用它替代第一组的 strict cold
基线。若 OSS 请求数明显下降但端到端仍落后，瓶颈就位于 FUSE scanner 并发或 OSS
带宽，而不是 packed metadata 编码。

## 严格 frame 读取修正（2026-09-29）

补充审查发现，`RemotePackedObject::read_frame` 在窗口缓存关闭时仍会把小
frame 扩成 4 MiB 对齐 Range。虽然当前主 FUSE coordinator 通常走精确 range，
这个备用读取路径仍会制造不必要的 OSS overscan。现在 strict 路径固定请求
`[object_offset, stored_len]`，只有显式配置
`BREWFS_PACKED_FRAME_WINDOW_CACHE_BYTES` 才允许 4 MiB 窗口。该行为由
`strict_read_frame_does_not_overscan_small_payloads` 回归测试锁定。

## 1,000 文件顺序窗口复测（2026-09-29）

为验证 descriptor/cache 修正后的顺序路径，在同一 Aliyun
`ecs.u1-c1m4.2xlarge`（32 GiB、100 GiB ESSD）上运行了 1,000 个
100 KiB--1 MiB 文件、两级 `10 x 10` 目录、每叶 10 个文件的 matched
payload 对照。两边均通过 FUSE 读取完整内容并校验 checksum `124948`；packed
使用 `SequentialSmallFile`、64 MiB 进程内窗口预算和顺序 read-ahead，持久
cache 仍为 0；JuiceFS 使用 Redis metadata、`cache-size=0`、`prefetch=0`。

| profile | scanner seconds | files/s | logical bytes | result |
| --- | ---: | ---: | ---: | --- |
| packed v3 cold-pipelined | 6.992579 | 143.01 | 586,260,935 | 1,000/1,000 |
| JuiceFS strict cold | 1.663303 | 601.21 | 586,260,935 | 1,000/1,000 |

packed 是 JuiceFS 的约 `0.238x`，即慢约 `4.2x`。这次比此前 packed strict
小文件读的几十秒级结果明显改善，说明 container frame-directory 缓存和对齐
window 在顺序 workload 上有效；但它仍不能在低 RTT Redis metadata 基线下抵消
FUSE read 往返和 OSS payload 请求成本。该轮的 read-ahead 仍在本次云端 binary
构建时序优化之前，不能把数字当作提前启动 read-ahead 的收益证明。

证据：

- `docker/compose-xfstests/artifacts/aliyun-packed-v3-descriptor-cache-1000-20260929/packed/remote-output.log`
- `docker/compose-xfstests/artifacts/aliyun-packed-v3-descriptor-cache-1000-20260929/juicefs/remote-output.log`

ECS `i-bp16u86uk6a59pcrnb0g` 已删除，测试 OSS 前缀对象数为 0。

## Descriptor fan-out 修正（2026-09-29）

代码审查进一步发现，单纯的 payload window 不能消除 descriptor fan-out：此前
每个尚未命中的 frame ordinal 都会触发一次 frame-table prefix probe 和一条小的
descriptor range。16 个 FUSE worker 同时打开同一批文件时，GroupMeta、inode/index
page 也可能在第一次填充完成前被重复下载。

当前实现做了三项有界优化：

1. GroupMeta、inode-index page 和 group-index page 使用 Moka `try_get_with`，同一
   immutable page 的并发冷缺失合并为一个远程加载；
2. frame descriptor 改为按 container 缓存完整 frame directory。容器目录的范围
   只有 prefix 加 descriptor records，远小于 payload，不进入 payload cache；
3. fixture 增加 `--access-profile random-small-file|sequential-small-file|mixed`。
   顺序训练数据必须显式发布 `sequential-small-file`，它才会启用相应的 group、
   container 和窗口 read-ahead 策略；默认仍是 random，避免改变既有 fixture 的
   语义。

新增回归测试 `catalog_reads_a_container_frame_directory_once_for_multiple_files`，
在同一 container 内读取两个不同 frame，录制到的请求固定为 header、GroupMeta、
frame-directory prefix、descriptor records 四条，不再随文件数线性增加。该测试
只验证请求合并，不代表 warm cache；payload window 仍由独立的显式预算控制。

理论边界保持不变：packed 的优势来自把 `N` 个元数据 KV/extent 查询压缩成有限的
索引页、GroupMeta 和 frame directory 读取；它不能把每个独立 payload 的 OSS/FUSE
往返凭空消除。要在低 RTT Redis 的 JuiceFS 基线之上领先，必须同时满足远端 metadata
RTT 较高、读取有顺序性、以及 group window/并发读取确实降低 payload GET 数。后续云端
对照应分别跑 random strict、sequential pipelined 和 JuiceFS `cache-size=0`，并记录
frame-directory 请求、window hit/miss、实际 payload bytes 与 overscan bytes。
