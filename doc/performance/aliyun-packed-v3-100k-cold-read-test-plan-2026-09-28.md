# Aliyun Packed v3 10 万随机小文件冷读测试计划（草案）

状态：已执行双侧对照；普通 BrewFS 对照尚未执行。本文件的结果只覆盖 packed v3 与 JuiceFS，
不把未完成的三方对照写成最终架构结论。

## 1. 目标

验证 packed-metadata-v3 在只读、无本地缓存、100–1000 KiB 随机小文件场景下的：

1. fixture 发布是否真正流式，内存不随总逻辑数据量增长；
2. group/container 分块是否满足设计上限并减少元数据请求；
3. tree scan 与完整 payload 冷读的正确性、OSS 请求形状和吞吐；
4. 为后续与 JuiceFS、无缓存 BrewFS 的匹配对照提供可复现基线。

本轮不宣称 packed 已经优于任何竞争对手；只有在相同机器、缓存、数据量、OSS 端点和读取语义下完成对照后才形成结论。

## 2. 固定实验条件

| 项目 | 值 |
| --- | --- |
| ECS | `ecs.u1-c1m4.2xlarge`，目标 32 GiB 内存 |
| 系统盘 | 100 GiB ESSD PL1 |
| Region/Zone | `cn-hangzhou` / `cn-hangzhou-h` |
| 对象端点 | `oss-cn-hangzhou-internal.aliyuncs.com` |
| 文件数 | 100,000 |
| 文件大小 | 102,400–1,048,576 bytes，确定性 LCG 随机分布 |
| 目录布局 | 2 层、每层 10 个目录、每个叶目录 1,000 文件 |
| 预计逻辑数据 | 约 55–60 GiB，以 fixture 输出为准 |
| 元数据后端 | 无 Redis/TiKV；manifest、index、container 全在 OSS |
| BrewFS 缓存 | `read_memory_bytes=0`、`read_ssd_bytes=0`、prefetch=false、range background prefetch=false、`keep_cache=0` |
| 每个工具前 | 删除测试 cache root、`sync`、`drop_caches=3`；失败时不报告为冷读 |

ECS 只通过 Cloud Assistant `RunCommand` 执行原生脚本；不使用 Docker、SSH 或宿主机缓存。

## 3. 测试阶段

### A. 本地门禁

在接受云端数据前必须通过：

```bash
cargo fmt --all -- --check
bash -n docker/compose-xfstests/aliyun/run_aliyun_packed_native.sh
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins
```

release binary 使用 `--features workspace-overlay` 构建；上传前可去掉 ELF 调试段，但不得改变源码或运行参数。

### B. 流式 fixture 发布

使用 packed v3 publisher（当前为 `packed_v3_snapshot_fixture` 原型）：

1. 遍历目录并按 `RandomSmallFile` profile 生成 group；
2. GroupMeta 单组硬上限 256 KiB；group target/hard limit 为 16/32 MiB；
3. container target 为 32 MiB，wire hard limit 仍为 64 MiB；
4. 达到 container target 后立即上传并释放 frame payload；
5. 所有 group/inode index page 上传完成后才发布 manifest；
6. manifest key 作为唯一可读提交点，上传失败不得留下“已发布”状态。

发布阶段记录：文件数、目录数、groups、containers、逻辑字节、manifest 大小、container 对象数/大小分布、index page 数和最大 GroupMeta 大小。

### C. Packed v3 tree scan（无 payload）

每次重新挂载并清缓存，只遍历目录和目录项，不读取文件内容。验证：

- 文件数、叶目录数、目录 fanout 与 fixture 完全一致；
- 不能出现额外 OSS metadata backend 请求；
- 记录 wall time、directories/s、files/s、OSS GET 次数和 range 字节；
- 记录 BrewFS stats 中 catalog/index/container 命中与 miss。

### D. Packed v3 full payload cold read

重新挂载并清缓存，按确定性目录顺序读取每个文件全部内容，校验文件长度和前缀模式。验证：

- `files=100000`、`errors=0`、`walk_errors=0`；
- 逻辑字节与 fixture manifest 一致；
- 记录 wall time、有效 MiB/s、files/s、OSS GET/Range 数、请求字节和读错误；
- 对比 data container 数量与独立对象读取次数，确认没有误报“缓存命中”。

主结果至少执行 1 次严格冷读；若无异常，再执行 2 次重复冷读用于观察方差。重复前必须重新删除 cache root 和 drop caches。

### E. 对照实验（单独批准后执行）

对照必须保持同一 ECS 规格、OSS region/端点、文件树、文件内容、读取顺序和缓存清理策略：

1. JuiceFS：使用其原生 block/object layout，元数据后端和缓存预算显式记录；
2. 普通 BrewFS：使用无缓存原始格式，禁止复用 packed container 或任何跨文件共享缓存；
3. 可选 TiKV/Redis：只作为 metadata backend 对照，不与 packed 的无 KV 设计混合比较。

若对照无法使用同一 payload 物理布局，报告必须同时给出“逻辑吞吐”和“OSS 请求/传输放大”，不能只比较单一 wall time。

## 4. 统一采集指标

| 类别 | 指标 |
| --- | --- |
| 正确性 | files、directories、logical bytes、checksum、errors、walk errors |
| 延迟/吞吐 | wall seconds、files/s、MiB/s、每文件读取延迟（可选 p50/p95） |
| OSS | GET/HEAD/Range 次数、请求字节、实际 payload 字节、metadata/data 请求比例 |
| 分块 | group/container 数、对象大小 min/p50/p95/max、GroupMeta 最大值、index page 数 |
| 缓存 | read memory/SSD budget、cache hit/miss、drop_caches 结果、cache root 大小 |
| 资源 | ECS 内存峰值、系统盘使用率、CPU、测试时长、实例释放时间 |

所有原始日志保存在 `docker/compose-xfstests/artifacts/aliyun-packed-v3-100k-*`，结果表中区分 fixture 发布时间和冷读时间。

## 5. 验收规则

- 任何校验错误、文件数不一致、manifest 不可读或 drop-caches 失败：该轮无效，不进入性能表；
- GroupMeta、container wire hard limit 或 index 边界违反：构建失败，不通过；
- 不能把 fixture 发布时间计入 read throughput，也不能把缓存命中结果当冷读；
- 云端 invocation 超时或 ECS 异常时，保留诊断日志并立即停止/删除资源；
- 只有完整 packed、JuiceFS、普通 BrewFS 三者的匹配结果，才允许写入“谁更快”的结论。

## 6. 资源和清理

- ECS 设置最多 180 分钟自动释放；runner 结束时停止并删除新建实例；
- fixture 使用唯一 OSS prefix；测试结束删除该 prefix，不删除 bucket 或其他用户对象；
- invocation 被停止后把 `Timeout`、`Terminated` 等状态视为终态，避免无限轮询；
- 运行前后检查 `DescribeInstances` 和 OSS prefix，确认无残留实例、凭据对象或测试对象。

## 7. 计划命令（确认后执行）

```powershell
pwsh -NoProfile -File docker/compose-xfstests/aliyun/run_aliyun_packed_million.ps1 `
  -Action run -RegionId cn-hangzhou -ZoneId cn-hangzhou-h `
  -VSwitchId <vswitch> -SecurityGroupId <security-group> `
  -InstanceType ecs.u1-c1m4.2xlarge -SystemDiskSizeGiB 100 `
  -SmallFileCount 100000 -SmallFileSizeBytes 102400 `
  -SmallFileMinSizeBytes 102400 -SmallFileMaxSizeBytes 1048576 `
  -DirLevels 2 -DirsPerLevel 10 -FilesPerLeaf 1000 `
  -VolumeFormat packed-metadata-v3 `
  -PerfTools "packed-tree packed-smallfiles" -ReadMode full `
  -ToolTimeoutSeconds 7200 -AutoReleaseMinutes 180 `
  -S3Bucket <bucket> -S3Endpoint https://oss-cn-hangzhou-internal.aliyuncs.com `
  -ObjectPrefix <unique-prefix> -SkipBuild
```

本草案待确认的唯一实验选择是：先只跑 packed v3 基线，还是确认后紧接着在同一 ECS 规格上追加 JuiceFS/普通 BrewFS 对照。

## 8. 已完成的 100k 对照（2026-09-28/29）

已在同一台 `ecs.u1-c1m4.2xlarge`、同一 OSS bucket/内网端点、同一目录树和同一
确定性 payload 上完成 packed v3 与 JuiceFS 对照。文件为 100,000 个、大小
102,400–1,048,576 bytes，逻辑字节 `57,552,185,440`，两边均使用 16 个扫描 worker，
读取通过 FUSE，读取前执行 `sync` 与 `drop_caches=3`。

两边的准备和读取路径保持以下边界：

- packed 使用 `packed_v3_snapshot_fixture` SDK 将 group/container/index/manifest 发布到
  `packed/` 前缀；观测到 3,319 个 `.brfgc` container 对象。读取只使用 packed FUSE。
- JuiceFS 使用同一个 fixture SDK 将 100,000 个原始文件上传到 `juicefs/raw/`，随后用
  `juicefs sync --threads 32 --list-threads 4 --check-new` 在不挂载 FUSE 的准备阶段导入
  JuiceFS 卷；正式读取只使用 JuiceFS FUSE。上传、sync 和读取时间分开记录，未将准备时间
  混入读取吞吐。
- packed 的内存/SSD cache、prefetch、range background prefetch 均为 0/关闭；JuiceFS 的
  `cache-size=0`、`prefetch=0`、`attr/entry/dir-entry/open-cache=0`。packed stats 的
  `brewfs_cache_hit_ratio=0`、block/page cache hits 均为 0。

| profile | 扫描器秒数 | files/s | 有效 MiB/s | 校验 |
| --- | ---: | ---: | ---: | --- |
| packed v3 full cold | 3384.746851 | 29.54 | 16.22 | 100,000/100,000，errors=0，checksum=12742480 |
| JuiceFS strict full cold | 351.235225 | 284.71 | 156.27 | 100,000/100,000，errors=0，checksum=12742480 |

本轮 packed 的有效吞吐是 JuiceFS 的 `0.1038x`，即约慢 `9.64x`；这不是可接受的
“packed 已领先”结果。packed stats 同时显示 200,332 次 FUSE read、平均 read latency
约 104.8 ms，以及 114,462 次 lookup；因此当前主要瓶颈是每个小文件仍触发独立的
OSS range/data 读取和 FUSE 往返，而不是 Redis/KV metadata。JuiceFS 的 loopback Redis
metadata 不构成网络瓶颈，且其读取路径的块/请求合并更有效。

本轮只证明了匹配条件下 packed v3 当前实现明显落后，不能据此否定 packed metadata
格式本身。要验证 v3 的设计目标，下一轮必须记录两边 OSS GET/Range 数和请求字节，
实现跨文件 group window/异步读取后，再补普通 BrewFS 三方对照。

证据：

- `docker/compose-xfstests/artifacts/aliyun-packed-v3-vs-juicefs-100k-20260928-r3/packed-run.log`
- `docker/compose-xfstests/artifacts/aliyun-packed-v3-vs-juicefs-100k-20260928-r3/packed/remote-output.log`
- `docker/compose-xfstests/artifacts/aliyun-packed-v3-vs-juicefs-100k-20260928-r3/juicefs/remote-output.log`

测试结束后的独立核验：ECS `i-bp10wczp9w20g7yjqizf` 不存在（`TotalCount=0`），
OSS 前缀 `brewfs-v3-jfs-20260928-100k-r3/` 对象数为 0。
