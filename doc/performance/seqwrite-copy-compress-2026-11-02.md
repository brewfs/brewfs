# 顺序写拷贝与压缩实验

任务开始：2026-11-02 07:48:24 UTC。截止：13:48:24 UTC。
工作树：`/home/hxy/brewfs-main`，分支 `perf/seqwrite-copy-compress`。
当前 main：`1bd4d75f8c7c77188f52e025dcd315520353cd65`（含 #404、#405）。
实验与原始日志：`/home/hxy/perf-seqwrite-20261102/`。

## 状态与证据边界

当前 main 的 30 秒、499 Hz 全线程调用栈采样和拷贝成本表已于 09:16:18 UTC 完成，
早于产品代码修改，并在 90 分钟定位时限内。完整 workspace 测试、默认与
overlay 的严格 all-targets clippy 均已通过；候选 release 与完整 perf 已完成。
候选 perf 的 CPU 与 seqwrite 指标达到阈值，正式三对交错 A/B 于 11:43 UTC
完成，满足四小时出数时限。randread、seqread 与 randrw 的 95% CI 尚不能
排除超过 3% 的下降，未通过全部验收，PR 保持 draft。完整内容与崩溃读回已通过；
broad 两臂各 10/10 工具成功退出，但单对 seqread -32.86%、bigread -9.23%、
randrw -5.76% 尚未解释。完整 overlay 二进制重跑及其余门禁已于 13:40:25 UTC
前完成；首轮 overlay 失败与原始日志缺口公开列于后文，不能称为首轮全绿。
历史基线 `/home/hxy/perf-full-20261101/` 保留且不重测其历史数值：
active 122.91 MiB/s、wall+drain 108.67 MiB/s、p99 173.0 ms、
memmove self 24.74%、原口径压缩 inclusive 22.19%。
历史环境与当前 main 不相同，历史数值不能替代新优化的配对控制组。

独立基线工作树 `/home/hxy/brewfs-seqwrite-baseline` 固定上述提交；
独立 target `/home/hxy/brewfs-seqwrite-baseline-target`。
候选使用 `/home/hxy/brewfs-pkv-target`；任一时刻只运行一条编译命令，
长编译在 tmux 中执行，不用 timeout，保留 3072 MiB 内存 watchdog。

此前误把 #404 二进制当作当前 main，已撤销该判断：两份旧二进制哈希相同
不能证明包含 #405。`profile-01-baseline` 明确标记为 reference-only，
不可用于当前 main 的验收或配对基线。该采样、被中断的构建日志均保留。
正确独立 main 构建于 08:21:05 UTC 启动、09:14:53 UTC 完成，随后采集新 profile-02。

## 固定实验条件

flat-v1、Redis、独立 RustFS S3 bucket；zstd level 1；块 4 MiB、chunk 64 MiB；
coalesce 64 KiB/0 ms；Tokio workers 1；指标开启、1 Hz 采样，request trace 关闭；
read RAM/SSD 0/0、write RAM 512 MiB、总内存预算 2 GiB、prefetch 关闭；
upload-before-commit、upload concurrency 10、S3 concurrency 8、multipart 16 MiB。
seqwrite 为 1 MiB、1 job、depth 8、buffered；random 为 4 KiB、4 jobs、depth 32；
randread direct=1；512 MiB/job、seed 20261101，写入 refill/scramble。
正式 A/B 使用仓库 compose perf runner，20 秒、A/B→B/A→A/B 三对。
runner 副本在生成 YAML 时补一行固定 `zstd_level: 1`，并在恢复受控 fio 附加
参数时去掉第二次 `--name`，避免 fio 启动第二组任务；完整补丁和哈希在 harness
中留档。其余工作负载、排空、断言和卸载逻辑使用仓库 runner。

同时报告 fio active、active+drain 与 wall+drain。严格排空要求 dirty、live dirty、
pending 和 inflight 全部归零且 PUT 计数稳定；不允许把 close/flush 成本移到统计窗外。
95% CI 使用配对 log(B/A) 的 Student t 区间，n=3、df=2、t=4.30265273；
不能排除 -3% 的区间记为未证明不回归。

正式测量前已审计 runner 计时：`perf-summary.tsv` 的 wall 在 post-write drain
之前结束，二者不重复。但原生 wall/drain 秒数为整数，可能带来取整误差。
因此同时保留原生生成指标，并从 `phase-boundaries.tsv` 纳秒时间戳另算
`tool_start → post-write_extended_drained` 的完整连续墙钟：包含 fio close/flush、
采样器停止、观察/报告间隙及严格排空。读场景终点为 `tool_return`。精确口径
与原生 wall+drain 均要求 seqwrite ≥130 MiB/s，不能用取整或观察间隙制造达标。

## 源码审计

默认构建同时启用 io-uring、tokio 和 buffer-pool。正常 FUSE_WRITE 用
`Bytes::from_owner(PooledRequestBuffer)` 转移请求缓冲所有权，payload slice 不复制；
只有池获取失败的 fallback 才复制完整请求。不能把 io-uring 一概计为一次 payload 拷贝。
CacheSlice append 把请求写入可变页一次；freeze、Bytes slice 和 uploader 分片只共享所有权。
对象准备把多个页片段拼成一块连续的 raw SF frame 一次，raw cache 是这块内存的 slice。
完整压缩后若有效，还会把压缩 body 复制进 SF header 后的最终 frame。
当前没有熵探测或局部试压跳过：lz4/zstd 都先完整编码，再比较长度。
单 Bytes 的 S3 PUT 共享所有权，多 chunks 的 simple PUT 当前会 flatten 一次。
4 KiB 独立小 extent 还可能进入 #404 coalesced object，另有一次成员 frame 到聚合对象的复制。
连续 4 KiB 写填满 4 MiB 块与每次 4 KiB 后 flush/fsync 必须分开计数。

采样百分比是 on-CPU period 占比，不等于数据字节复制次数；
内核 FUSE/TCP copy 与编解码内部的数据依赖拷贝另外列示。

## 验收与发布规则

所有门禁与硬指标满足才转 ready/merge；否则保持 draft 并逐项列差距。
未通过验收不更新 README 的已接受性能比较表。
保留原始 fio JSON、perf data/folded/SVG、指标、SHA-256 列表、SIGKILL 和卸载记录。

## 当前 main 的完整诊断（先于产品修改）

表格生成：2026-11-02T09:16:18.649843+00:00。
采样命令：`perf record -F 499 -g --call-graph dwarf,8192 -e cpu-clock -z -p <mount-pid>`；fio 实际 active 30.058 秒，perf 窗口 31.512 秒。
样本 11038，lost=0，unknown self=0.299%。
新 main 诊断：active **145.885 MiB/s**，wall+严格 drain **138.660 MiB/s**，p99 **152.044 ms**。
memmove self **23.156%**；含 kernel memcpy 的合计 **29.734%**。

拷贝次数 × 每处成本（前台 payload 按一次连续写最终填满 4 MiB 块计）：

| 调用链 / 阶段 | 完整复制次数 / 量 | self CPU | 占所有 memmove | CPU ms/MiB |
|---|---|---:|---:|---:|
| 页片段 → 连续 raw SF 帧；cache 共享其 body | 1 × raw bytes | 7.175% | 30.986% | 0.3620 |
| Zstd literal/seqstore/raw-block 等内部复制 | 依赖内容，不能由采样反推固定次数 | 6.795% | 29.343% | 0.3428 |
| 仅压缩被采用时，编码结果 → 带 SF 头的最终帧 | 0 或 1 × compressed bytes | 3.878% | 16.745% | 0.1956 |
| 请求中的借用 payload → 可变 64 KiB 页 | 1 × raw bytes | 3.651% | 15.767% | 0.1842 |
| 本行还含 worker/future 小结构移动，不能视为整块复制 | 正常 pooled 请求 payload 为 0；fallback 为 1 | 1.350% | 5.829% | 0.0681 |
| SDK/HTTP future 与控制结构移动；内核 TCP copy 另计 | 当前单 Bytes body 的 payload 为 0 | 0.299% | 1.291% | 0.0151 |
| 保留归属不确定性 | 未解析，不编造次数 | 0.009% | 0.039% | 0.0005 |
| freeze、Bytes slice/clone、uploader 分片 | 0 | 包含在调用方，不另估算 | — | — |
| 独立 4 KiB extent 的 #404 聚合成员 → group body | 另 1 次 encoded member copy | 本 seqwrite 场景未单独量到 | — | — |

4 KiB 的答案：从正常 pooled FUSE 用户态请求开始，连续写填满块时有 **2 次固定 raw payload 拷贝**（页写入、对象连续化）；压缩有效时另有 **1 次压缩结果拷贝**，再加编码器内部的数据依赖复制。每次 4 KiB 后独立 flush/fsync 的小 extent 可能再经过 coalesced group 的成员复制。FUSE 请求池 fallback 会另复制一次。内核应用 page cache、FUSE 传输、TCP/对象服务器端复制不包含在这两个用户态次数中，也不能由挂载进程的 perf 采样量出全部成本。

压缩归属（inclusive 组有重叠，禁止相加）：

| 口径 | CPU |
|---|---:|
| Zstd 编码（历史 `ZSTD_compress` 口径） | 27.179% |
| Zstd Rust/C 编码链 | 27.242% |
| LZ4 编码链 | 0.000% |
| Rust 压缩策略、codec 与压缩 SF 封装 | 31.346% |
| 全进程校验链 | 0.118% |
| 上述压缩链与校验链的 union | 31.464% |

此配置选用 zstd level 1，LZ4 为零说明没有走该算法，不能推断 LZ4 的性能。源码没有不可压缩数据预检测：先完整编码，再判断 `encoded_len + 4 < raw_len`。校验开关和 S3 签名保持固定。

原始完整调用栈及所有链的百分比：`/home/hxy/perf-seqwrite-20261102/rounds/seqwrite/profile-02-baseline/results/baseline/seqwrite/measure-2/copy-cost.json`；完整火焰图：`cpu.svg`；原始 `cpu.data`、frame-preserving `cpu.folded`、perf 命令和采样日志均在同目录。

## A–E 选择与风险

下列成本是当前采样中可以针对的 CPU 上限，非带宽提升承诺。压缩与 memmove 重叠，不能叠加收益。

| 方向 | 预期收益与选择依据 | 风险 | 对读路径影响 |
|---|---|---|---|
| A 消除整块连续化 | raw frame 拼装占 7.175% CPU；少一次完整 raw copy。cache 关闭时可考察共享 Bytes 分片上传 | 必须保留 SF header、长度、重试和不可变缓冲生命周期；create-only/cache 需要原路径 | 格式不变；需验证冷读与写后读，压缩策略变化另计 |
| B 合并小写直接写目标缓冲 | CacheSlice append 占 3.651% CPU，是更小的单项上限；#404 group copy 不在本场景主成本中 | 重叠写、冻结、COW 与预算计费复杂 | 最容易伤及 read-your-writes 和并发读 |
| C 大块低收益压缩探测 | 压缩策略/codec/封装占 31.346% CPU；低收益块可避开完整编码和压缩结果封装，首选单独验证 | 抽样有误判，可能增加存储/传输量；必须覆盖可压缩尾部、重复高熵数据和小块开销 | 保留现有 raw/压缩帧解码；需实测 IO 放大与读回归 |
| D vectored body | 当前上传已经是单 Bytes，无额外 payload flatten 可单独消除；是 A 保持分片到 S3 的配套能力 | HTTP 长度提示、重试、checksum 和 RustFS 兼容性 | 不直接改读；共享 SDK/网络资源仍需混合读写门禁 |
| E 排空与写入重叠 | 本次严格排空 1.042 秒；先看实际债务，不能把稳定采样等待当作可隐藏的上传成本 | fsync/close 持久性与后台债务、混合读写争用 | IO 竞争可拖慢 randread/randrw |

先用生产压缩模块的失败测试及隔离诊断检验 C；隔离模块数字不作为性能验收。只有已有证据表明下一步必要时才增加第二个方向，最终候选必须通过完整 compose 配对与门禁。

## 候选范围与定向验证

首先单独验证 C：只对至少 1 MiB 的 Zstd 输入取首部、中部、尾部各 8 KiB，
总试压输入不超过 24 KiB。平均节省不足 5% 时使用现有 raw SF 帧；相同样本
保留全量压缩，避免重复高熵区域被当作随机数据；任一样本压缩至 80% 以下则
提前保留全量编码。小块与 LZ4 不改变决策，试压错误保留原来的全量编码机会。
抽样无法保证发现样本以外的冗余，这是明确的压缩率/CPU 取舍。

原实现的失败测试记录于 `build/focused-c-red-test.log`：1 MiB 随机块仍向 codec
提交 1 MiB。C 模块的 17 个测试随后通过，覆盖 1/4 MiB 随机块、1/3 级 Zstd、
小块、全零、文本、随机前缀/可压缩尾部和重复随机区域，全部完整解码核对。
隔离生产模块对六份实际低收益输入均选择 raw，减少全量编码；这些是诊断，不能
替代挂载性能验收。可压缩数据隔离微基准观察到约 15–30% 的吞吐下降，包含
全零输入；独立上下文没有消除该现象，原因未定位，保留全部测量，安排额外
三对交错的全零数据 compose 挂载对照，不能宣称已排除可压缩数据风险。

C 仍会保留每块一次连续 raw frame 拼装，占原采样 CPU 的 7.175%，其余 CPU
消失后这个比例可能升高。下一步因此只增加 A 所需的分片路径及 D 运输配套：
大块 raw、关闭写后缓存且非 create-only 时，共享输入 Bytes，单独附加静态
4 字节 SF 头；采样最多复制 24 KiB，不再先拼整个 raw block。S3 simple PUT
仅在至少 1 MiB、多分片且已关闭 payload checksum/signing 的既有配置下使用
精确长度、可重试的共享分片 body。其余路径保留原来的连续帧。没有实施 B/E。

A 的旧实现失败证据为 `build/focused-a-red-test.log`（4,194,317 字节 payload
仍被复制）；最终 19 个压缩/组帧测试全部通过。新 body 的失败测试证明旧 flatten
不能保留原始所有权；修复后三个测试验证逐帧指针、部分消费后的完整重试、精确
递减长度、空分片及 drop 后所有权释放。另加入真实 mock S3 的 4 MiB PUT 首次
500 重试，要求两次 body 完整一致、Content-Length/MD5 正确、无 chunked 传输。
原缓存共享与计费回归测试继续执行，create-only 保留连续缓冲和条件写入语义。

完整 workspace 通过后，复用同一测试二进制单独捕获实际持久化对象，九个案例
全部通过。4 MiB/32 分片 raw 写的准备阶段实测：关闭缓存为 **0 字节 payload
复制**；缓存开启与 create-only 均为 **4,194,304 字节**，三者 wire object 都是
4,194,308 字节且 SHA-256 都为
`322bdf05f5cb1bec9bfee06baa029a94983c7f9f34da9ad6c127ac49752f1e3f`。
这仅计同步对象准备中的显式复制，不包含 VFS、codec 或传输内核复制。
捕获对象及计数在 `correctness/framed-object-captures/`，测试日志在
`validation/framed-object-evidence.log`。Zstd 无缓存探测的分片测试另验证复制上限
24 KiB 和逐片指针相同。最终挂载性能只比较整体候选；没有 C-only 挂载对照，
不能把整体收益拆分归因给 C 或 A。

24 KiB 是一次三点探测的 codec 输入上限，也是共享 raw 分支的拷贝上限。
共享输入第一次探测后若决定保留编码，会进入连续帧编码器，后者再次进行
自己的探测；这种可压缩路径最坏有 48 KiB 试压输入，再加完整编码。
本轮没有为消除这项重复判断再扩大生产修改，额外全零挂载 guard 会单独报告。

初次 standalone body 诊断错选同版本 bytes 的 no-std rlib，造成类型不匹配。
按 Cargo fingerprint 选 std/default 构件后红/绿测试正常；这属于隔离诊断选库问题，
不能作为生产依赖冲突证据。完整 workspace 首次编译另发现 coalesce 对父模块
隐式导入的依赖，改为显式导入；失败日志保留于 `validation-attempt-01-import-failure/`。

## 本轮完整 workspace 门禁

`CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0`
下执行 `cargo test --workspace --lib --bins`，仅以 codegen-units=256 限制 LLVM
单元的内存；没有测试过滤或新增 ignore。09:53:25 UTC 完成，library **1325 passed,
0 failed, 264 既有 ignored**，binary **2 passed, 0 failed**。全部原始命令、输出、
开始/结束时间与退出码位于 `validation/test-workspace.*`。默认严格 all-targets
clippy 于 09:57:22 UTC 通过，overlay/observability 同口径于 10:05:52 UTC 通过。
overlay 全套和其余 CI 仍需分别通过，不能把部分门禁通过当作最终全绿。

## 候选完整 perf（正式配对验收前）

候选 release 于 10:05:54–11:00:03 UTC 单独构建，退出码 0；构建前后
源文件 manifest 完全一致。候选二进制 SHA-256 为
`24bb3d0c46a416c2e697f219fe64905677845c83e16edc5b3a5e03bc41b902cc`；
独立 main 为 `bb99101bbc7edc3b60f3a24db672e7618a23a118996da275ce057a05c21e3ef3`。
两者使用同样 release flags 与 workspace-overlay/observability features。

候选采用同样预热和 `-F 499 -g --call-graph dwarf,8192 -e cpu-clock`：
fio active 30.004 秒、wall 33.117578 秒，perf 窗口 33.950722 秒；9026 个样本，
lost=0，unknown self=0.7201%。本表只说明完整采样前后结果，配对结论另列。

| 指标 | 当前 main 完整采样 | 候选完整采样 |
|---|---:|---:|
| memmove self | 23.156% | **8.553%** |
| memmove + kernel memcpy self | 29.734% | 19.411% |
| 历史口径 Zstd inclusive | 27.179% | 0.632% |
| LZ4 inclusive | 0% | 0% |
| 压缩策略/codec/SF 封装 inclusive | 31.346% | **0.676%** |
| 压缩与校验 union | 31.464% | 0.920% |
| 整个 compression 模块 conservative inclusive 上界（另含 raw 拼装） | 38.585% | 0.875% |
| active MiB/s | 145.885 | **194.141** |
| wall + strict drain MiB/s | 138.660 | **170.510** |
| p99 total ms | 152.044 | 112.722 |
| 平均 PUT bytes | 4,169,027 | 4,192,149 |

以逻辑写入 MiB 归一化的 on-CPU 成本，避免仅因分母变化而夸大百分比收益：

| CPU ms/MiB | 当前 main | 候选 |
|---|---:|---:|
| 挂载进程总 CPU | 5.044525 | 3.105266 |
| memmove | 1.168129 | 0.265596 |
| memmove + memcpy | 1.499921 | 0.602751 |
| 其中单独 memcpy | 0.331792 | 0.337155 |
| 压缩策略/codec/SF 封装 | 1.581270 | 0.020986 |

memcpy 的绝对成本基本持平，说明 memmove 比例下降同时伴随其每 MiB 绝对
CPU 成本下降；内核传输复制仍存在，并未在报告中被归零。

候选压缩与校验 union 比纯压缩模块上界更高，是因为 union 包含模块外校验；
各 inclusive 口径重叠，不相加。新 raw 路径放弃少量原编码收益，对象大小上升
约 0.55%；必须结合正式读、混合读写及对象放大门禁判断，不能只报写入增益。

| 候选剩余 memmove 调用链 | self CPU | 占全部 memmove |
|---|---:|---:|
| FUSE payload → CacheSlice 页 | 5.794% | 67.746% |
| FUSE/session/worker future 结构移动 | 2.293% | 26.813% |
| S3/HTTP（含 future 小结构） | 0.377% | 4.404% |
| 采样与分片句柄 | 0.044% | 0.518% |
| codec 内部 | 0.033% | 0.389% |
| 其他 | 0.011% | 0.130% |

候选严格排空 1.044600 秒，首次所有债务清零在 0.014424 秒；连续三个采样稳定，
最终 dirty/live dirty/pending/inflight 全为零。GET=0，PUT=1457，
上传 6,107,961,028 字节，正常卸载。完整证据位于
`rounds/seqwrite/profile-01-candidate/results/candidate/seqwrite/measure-1/`。
两次启动脚本权限失败（实验目录所有权、辅助脚本缺 executable mode）均发生在
对应测量之前，已隔离保留；仅修正外部实验脚本，未改变产品源码或二进制。
辅助脚本采用内容相同、chmod 755 的外部副本。正式 A/B 准备曾于 11:12:42 UTC
开始，但可选 prefill 全文件 sha256sum 的小读在无读缓存时反复解码压缩对象，
准备时间过长，停止并整臂归档，没有混入三对正式结果。后续统一关闭这项可选
准备散列；完整逐块 SHA-256 和崩溃恢复检查仍由独立正确性门禁完成。

第一次重启的退出 trap 使 CI 提前开始 overlay 编译；SIGSTOP 没有使进程保持
停止，因而改为单独冻结仅含 CI bash/cargo/rustc 的 cgroup（11:19 UTC）。
受影响的未完成 baseline arm 同样整臂归档，正式六臂全部重新开始，避免将
有编译 CPU 竞争的 baseline 与无竞争的 candidate 对比。CI 未被杀死，完整
测量后解除 cgroup freeze 继续。暂停时间单独记录，不能当作编译性能数据。

11:21 UTC 核对 JSON 发现仓库 runner 在 `PERF_FIO_*_ARGS` 非空时再次传入
`--name`，fio 因而产生两组独立 job。尚未完成的 baseline arm 整体归档于
`rejected-harness/ab-01-baseline-duplicate-fio-name/`；正式 runner 副本移除重复
job 声明后重新开始六臂。分析器要求只出现一个汇总组、seqwrite/seqread
numjobs=1、random numjobs=4，所有 fio errors=0。该修复只在实验副本中，
不把两组 job 的吞吐与用户要求的 1 job 指标混算。

六个正式 arm 均在 11:42:26 UTC 前完成，11:43:08 UTC 输出三对统计。
测量完成后修改了仍运行的 shell 驱动，这是调度错误：其文件读取位置使它
再次调用 A/B，被 `exist_ok=False` 保护挡住，原六臂没有被覆盖，也没有运行
第二次负载。失败日志保留；已改用新的独立驱动从分析与正确性阶段继续。

首个崩溃用例在成功 fsync 后立即 SIGKILL 挂载进程，随后关闭仍打开的 fd
返回 Linux `ECONNABORTED`（103），使旧 helper 在重新挂载之前退出。
该次没有产生读回结论，已清理挂载并完整归档。helper 将此明确的断连错误
与既有 ENOTCONN/ENODEV/EIO 一并记录后继续卸载；重启和逐块/整文件 SHA-256
断言全部保留，四个场景从头执行。测试对象为挂载进程，Redis/RustFS 在恢复
期间继续运行。

## 自我 review（源码冻结后）

- 所有持久化对象仍使用 chunks-v2 与四字节 SF 头；raw magic collision、空写、
  稀疏前缀、压缩/非压缩对象的读取由现有帧解析器和完整 workspace 测试覆盖。
  新路径按原顺序共享不可变 Bytes，采样不会改写 payload。
- 仅上传成功后发布 format cache；带宽额度按实际 wire bytes 申请，写回/metadata
  提交顺序保持 upload-before-commit。写后缓存和 create-only 请求强制连续帧。
- S3 新 body 每次 SDK/外层 retry 都从完整分片重建，Content-Length 和递减
  size_hint 精确；4 MiB mock 首次 500 后验证了两次完整内容、MD5 和非 chunked。
  payload checksum/signing 开启、multipart、小请求仍使用既有 body。
- 独立小 extent 的 #404 聚合成员至多 64 KiB，未进入新的 ≥1 MiB 探测分支；
  import 显式化没有改变聚合策略。读取实现未修改，但对象编码决策可能改变
  传输量，因此读、混合读写和可压缩数据实测仍是必须的验收门禁。
- 采样可能漏掉未采样区域的冗余；此策略不保证与完整编码相同的压缩率。
  可压缩数据微基准的 15–30% 下降尚不能解释，必须结合额外挂载对照公开判断。
- 生产输入逐文件 SHA-256 与 release 构建前后及同轮 CI manifest 一致；
  未跟踪的 `tools/perf/measure_observability.py` 不属于本变更，也不加入提交。

## Overlay 首轮失败与日志保管缺口

首轮未过滤的 overlay/observability 全套于 13:15 UTC 前结束，**2751 passed、
1 failed、475 ignored**，exit 101，失败为
`workspace_overlay::meta_layer::tests::writable_acl_independent_sqlite_instances_keep_mode_acl_during_atime_writes`
在 `src/workspace_overlay/meta_layer/tests.rs:1477` 的 `EBUSY`。主分支 #404 的
报告与实施日志记录了同一未修改测试的并发失败；这不豁免本轮全套零失败门禁。

重跑驱动写入同名日志前没有先归档，导致首轮完整 `overlay-full.log` 被覆盖。
只保留 exec 工具捕获的失败尾部、计数、退出码与命令，存于
`validation-attempt-02-overlay-ebusy/failure-excerpt.txt`；**不声称完整首轮原始日志
仍然存在**。该证据缺口与失败本身均公开保留。驱动另外先后因 sudo PATH 缺少
cargo、root 未配置 rustup toolchain 而启动失败，均未执行测试。修正外部驱动的
PATH/RUSTUP_HOME/CARGO_HOME 后，使用同一稳定版编译器、参数、特性和
`RUST_TEST_THREADS=2` 完整重跑，没有过滤、跳过、修改断言或产品源码。

修正后的 cargo 重跑因 Git index 更新触发 `build.rs`，再次开始编译库；该次
编译在测试执行前由代理中断，不能算作测试失败或通过。随后直接执行首轮本次
源码生成的三个完整测试可执行文件：overlay/observability library、main 和 stats，
使用相同 `RUST_TEST_THREADS=2`、无任何过滤。其 SHA-256 与命令单独记录在
`validation/overlay-test-binaries.sha256` 和 `harness/overlay-binary-full.sh`。
普通 workspace 的 cargo 测试门禁已通过；后续 overlay 结果明确来自完整二进制
重跑，不声称第二次 cargo test 命令已完成。剩余 check/build/runtime/clippy
顺序执行，任一时刻仍只运行一条编译命令。原始首轮失败和中断的事实保留。

完整二进制重跑最终 **2752 library tests + 2 binary tests passed，0 failed、
475 既有 ignored**，library 用时 461.31 秒。workspace check/build、两个 runtime
check、workspace clippy 和最终 diff-check 均通过，恢复脚本于 13:40:25 UTC
退出 0。严格 all-targets clippy（默认及 overlay/observability）先前已通过，
产品源码 SHA-256 始终一致；没有因这次失败调整断言或新增 ignore。
本轮专用 Redis/RustFS compose 服务、容器和网络已正常清理，raw perf 证据目录保留。

## 最终验收数据（自动汇总，无轮次筛选）

汇总时间：2026-11-02T13:43:43.195833+00:00。

### 三对正式 compose A/B

| Scene | Baseline active | Candidate active | Change (95% CI) | Baseline wall+drain | Candidate wall+drain |
|---|---:|---:|---|---:|---:|
| seqwrite | 135.680 | 184.472 | +37.51% [+30.10, +45.33]% | 114.708 | 153.750 |
| randwrite | 4.849 | 4.933 | +1.63% [+0.33, +2.95]% | 4.042 | 4.185 |
| randread | 0.267 | 0.298 | +16.26% [-9.67, +49.65]% | 0.267 | 0.298 |
| seqread | 22.372 | 23.381 | +1.39% [-53.34, +120.34]% | 22.400 | 23.400 |
| randrw | 0.386 | 0.400 | +3.74% [-7.19, +15.97]% | 0.328 | 0.348 |

单位 MiB/s；变化与 CI 是配对 log(B/A) 的几何均值及 Student t 95% 区间，
表内绝对值各自取中位数，不能用中位数比值代替配对估计。

| 场景 | A/B 第 1 对 | A/B 第 2 对 | A/B 第 3 对 |
|---|---|---|---|
| seqwrite | 137.423251 / 184.472329 | 135.679647 / 190.321451 | 132.333898 / 182.722591 |
| randwrite | 4.917955 / 5.021317 | 4.849467 / 4.933285 | 4.820175 / 4.871440 |
| randread | 0.266902 / 0.282691 | 0.260091 / 0.297854 | 0.295996 / 0.383512 |
| seqread | 32.269343 / 23.381294 | 22.372034 / 30.066926 | 21.752145 / 23.283701 |
| randrw | 0.386197 / 0.399815 | 0.377017 / 0.409469 | 0.402259 / 0.399436 |

| 场景 | A/B 精确 wall+drain | 候选 active+drain | ≤3% 不退化已证明 |
|---|---|---:|---|
| seqwrite | 114.713 / 154.404 | 160.414 | 是 |
| randwrite | 4.054 / 4.130 | 4.366 | 是 |
| randread | 0.260 / 0.290 | 0.298 | 否 |
| seqread | 21.793 / 22.742 | 23.381 | 否 |
| randrw | 0.325 / 0.337 | 0.363 | 否 |

| 场景/操作 | A/B p99 total，ms | 配对变化与 95% CI |
|---|---|---|
| seqwrite/write | 162.529 / 126.353 | -22.99% [-39.97, -1.22]% |
| randwrite/write | 206.569 / 214.958 | -2.01% [-26.66, +30.93]% |
| randread/read | 2122.318 / 1887.437 | -12.88% [-23.55, -0.74]% |
| seqread/read | 404.750 / 429.916 | +11.95% [-29.66, +78.16]% |
| randrw/read | 1585.447 / 1602.224 | -0.36% [-4.31, +3.75]% |
| randrw/write | 1568.670 / 1551.892 | -1.08% [-3.73, +1.64]% |

randrw 写入每轮不足 1000 次 IO，其 p99 的尾部样本有限；原始总 IO 数与
p99_sample_sufficient 标记保留在逐臂 JSON，不能据此声称尾延迟已充分验证。

### 对象放大与挂载资源

| 场景/版本 | PUT/GiB 写入 | 平均 PUT bytes | wire/raw | partial-tail/batch | RSS 峰值 MiB |
|---|---:|---:|---:|---:|---:|
| seqwrite/baseline | 256.289 | 4168232.1 | 0.994920 | 0.011905 | 186.55 |
| seqwrite/candidate | 256.070 | 4193160.8 | 1.000001 | 0.008696 | 214.53 |
| randwrite/baseline | 16387.319 | 66240.6 | 1.010955 | 1.000000 | 294.31 |
| randwrite/candidate | 16383.363 | 66257.5 | 1.010959 | 1.000000 | 322.00 |
| randrw/baseline | 52844.902 | 20592.9 | 1.013495 | 1.000000 | 104.39 |
| randrw/candidate | 50103.976 | 21717.8 | 1.013418 | 1.000000 | 105.44 |

各值分别取三轮中位数；对象窗口包括 close 和严格 drain，扣除准备阶段。
最终所有 debt gauge 为零，所有实际 fusermount3 均 status=0，退出进程表无 brewfs mount。

### 可压缩全零数据追加对照

| Scene | Baseline active | Candidate active | Change (95% CI) | Baseline wall+drain | Candidate wall+drain |
|---|---:|---:|---|---:|---:|
| seqwrite | 203.067 | 203.770 | -0.35% [-6.27, +5.95]% | 174.208 | 184.273 |

同样三对交错、512 MiB、20 s；zero_buffers=1、scramble_buffers=0、refill_buffers=1。
与不可压缩主测分开报告；隔离微基准与挂载测量各自保留。

### Direct IO guard

| 场景/版本 | active MiB/s | wall+drain MiB/s | read/write p99，ms | strict drain，s |
|---|---:|---:|---|---:|
| randrw-direct-baseline | 3.334 | 3.077 | 210.764 / 208.667 | 1.047 |
| randrw-direct-candidate | 3.291 | 3.047 | 200.278 / 198.181 | 1.041 |
| randwrite-direct-baseline | 5.377 | 4.863 | 0.000 / 219.152 | 1.046 |
| randwrite-direct-candidate | 5.264 | 4.808 | 0.000 / 181.404 | 1.047 |
| seqwrite-direct-baseline | 157.842 | 146.443 | 0.000 / 156.238 | 1.042 |
| seqwrite-direct-candidate | 205.959 | 169.799 | 0.000 / 95.945 | 1.042 |

以上为相同挂载配置的 host guard，每场景仅一对；另以仓库 compose runner
执行 direct=1 的 seqwrite/randwrite/randrw，结果如下。两组各自报告，均不作为
独立统计显著性证据。

| compose direct 场景 | A/B active MiB/s | A/B 精确 wall+drain MiB/s |
|---|---|---|
| seqwrite | 157.376 / 203.973 | 133.965 / 156.330 |
| randwrite | 4.918 / 4.934 | 4.093 / 4.128 |
| randrw | 0.386 / 0.431 | 0.325 / 0.365 |

### Broad compose：完整十场景单对

两臂均 10/10 工具成功退出、无 invalid 标记、排空后债务为零且正常卸载。
这只能证明运行完成；以下单对性能下降仍是未解决的风险。bigread/bigwrite
为 8 jobs × 512 MiB 的完整 4 GiB 一次性传输，其余 fio 为 20 秒。

| 场景 | A/B active MiB/s | 变化 | A/B 精确 wall+drain MiB/s |
|---|---|---:|---|
| seqread | 31.844 / 21.379 | -32.86% | 30.936 / 20.767 |
| seqwrite | 137.033 / 177.697 | +29.67% | 116.000 / 149.784 |
| randread | 0.308 / 0.322 | +4.45% | 0.301 / 0.313 |
| randwrite | 4.586 / 4.863 | +6.04% | 3.846 / 4.058 |
| randrw | 0.413 / 0.389 | -5.76% | 0.347 / 0.328 |
| bigread | 36.592 / 33.215 | -9.23% | 36.367 / 33.065 |
| bigwrite | 169.980 / 179.712 | +5.73% | 145.931 / 154.344 |

seqread、bigread 与 randrw 的下降尚未定位，不能由先前三对中位数上升
来抵消，也不能归因于噪声后丢弃。bigread p99 命中 fio 直方图上限
17,112.760 ms，不能据两臂 p99 相同推断真实尾延迟相同。

| 冷读场景/版本 | GET/GiB 逻辑读 | GET bytes / 逻辑读 bytes | mount 平均 CPU 核 |
|---|---:|---:|---:|
| seqread/baseline | 2258.587 | 7.144 | 0.603 |
| seqread/candidate | 2385.346 | 6.851 | 0.747 |
| bigread/baseline | 2328.750 | 7.062 | 1.055 |
| bigread/candidate | 2430.750 | 6.735 | 1.051 |
| randread/baseline | 343887.828 | 904.313 | 1.003 |
| randread/candidate | 340739.566 | 867.742 | 1.002 |
| randrw/baseline | 350189.601 | 903.172 | 1.005 |
| randrw/candidate | 354733.492 | 859.778 | 1.006 |

在无读缓存配置下，顺序读仍有约 7 倍传输放大、随机读约 900 倍；
候选 GET 字节放大稍降，但 seqread/bigread 的每 GiB 请求数上升。新的 raw
选择会影响现有 range/full-decode 分支，尚需分离编码布局、FUSE 分片与后端
状态的贡献。Broad bigread 两臂 host IO PSI some 均约 63%，full 约 41–42%；
这些是诊断线索，不能据此豁免读回归或认定其根因。

| metaperf 操作 | A/B ops/s | 变化 |
|---|---|---:|
| create | 142.940 / 139.754 | -2.23% |
| open | 1228.577 / 1253.164 | +2.00% |
| stat | 545690.460 / 549253.749 | +0.65% |
| readdir | 8720.206 / 9344.801 | +7.16% |
| rename | 338.240 / 346.489 | +2.44% |

metaperf create 单对 -2.23%，其余操作上升；这里只报告观察值，没有
把单对当作 ≤3% 的统计证明。metadata cache 与 Redis commandstats 保留于
guard-review.json 和原始 diagnostics；完整对照为 broad-comparison.txt。

### 完整内容与崩溃恢复

| 场景 | 校验字节 | 4 KiB 块数 | 整文件 SHA-256 |
|---|---:|---:|---|
| candidate-direct0-fsync | 536870912 | 131072 | `e0e6080a86ab10c34f2a11256aab0383ff57c0625908d8c0a717fcf34e3c8789` |
| candidate-direct0-close | 536870912 | 131072 | `e0e6080a86ab10c34f2a11256aab0383ff57c0625908d8c0a717fcf34e3c8789` |
| candidate-direct1-fsync | 536870912 | 131072 | `e0e6080a86ab10c34f2a11256aab0383ff57c0625908d8c0a717fcf34e3c8789` |
| candidate-direct1-close | 536870912 | 131072 | `e0e6080a86ab10c34f2a11256aab0383ff57c0625908d8c0a717fcf34e3c8789` |

四个场景分别写入 512 MiB，全部逐块 SHA-256 和整文件 SHA-256 一致，
总计 2 GiB、524288 块；buffered/direct × fsync/close，持久性边界后立即
SIGKILL，不插入 drain、等待或验证读取。重启使用新挂载和新缓存目录。
空文件、单字节文件同时验证；八次清理均无残留挂载。散列列表与 SIGKILL/卸载
日志保留在原始 correctness 目录，列表哈希见 guard-review.json。

### 最低本地 CI 与 overlay 全套

| 门禁 | exit | 命令 |
|---|---:|---|
| build-workspace | 0 | `cargo --config /home/hxy/perf-seqwrite-20261102/harness/cargo-ci.toml build --workspace` |
| check-io-uring | 0 | `cargo --config /home/hxy/perf-seqwrite-20261102/harness/cargo-ci.toml check -p brewfs --no-default-features --features fuse-io-uring-runtime` |
| check-tokio | 0 | `cargo --config /home/hxy/perf-seqwrite-20261102/harness/cargo-ci.toml check -p brewfs --no-default-features --features fuse-tokio-runtime` |
| check-workspace | 0 | `cargo --config /home/hxy/perf-seqwrite-20261102/harness/cargo-ci.toml check --workspace` |
| ci | 0 | `完整 CI 流水线` |
| clippy-all-targets | 0 | `cargo --config /home/hxy/perf-seqwrite-20261102/harness/cargo-ci.toml clippy --workspace --all-targets -- -D warnings` |
| clippy-overlay | 0 | `cargo --config /home/hxy/perf-seqwrite-20261102/harness/cargo-ci.toml clippy --workspace --all-targets --features workspace-overlay\,observability -- -D warnings` |
| clippy-workspace | 0 | `cargo --config /home/hxy/perf-seqwrite-20261102/harness/cargo-ci.toml clippy --workspace` |
| diff-check | 0 | `bash -c git\ diff\ --check\ \&\&\ git\ diff\ --cached\ --check` |
| fmt | 0 | `cargo fmt --all --check` |
| framed-object-evidence | 0 | `/home/hxy/brewfs-pkv-target/ci/debug/deps/brewfs-7625f77511eb5045 --exact chunk::store::framed_write_tests::persisted_object_copy_evidence --nocapture` |
| juicefs-direct-matrix | 0 | `bash docker/compose-xfstests/test_juicefs_direct_matrix.sh` |
| juicefs-perf-report | 0 | `bash docker/compose-xfstests/test_juicefs_perf_report.sh` |
| overlay-full | 0 | `bash /home/hxy/perf-seqwrite-20261102/harness/overlay-binary-full.sh (all original overlay library and binary tests, no filters)` |
| perf-report-delta | 0 | `bash docker/compose-xfstests/test_perf_report_delta.sh` |
| syntax-run_juicefs_perf | 0 | `bash -n docker/compose-xfstests/run_juicefs_perf.sh` |
| syntax-run_juicefs_perf_in_container | 0 | `bash -n docker/compose-xfstests/run_juicefs_perf_in_container.sh` |
| syntax-run_perf_in_container | 0 | `bash -n docker/compose-xfstests/run_perf_in_container.sh` |
| syntax-run_redis_perf | 0 | `bash -n docker/compose-xfstests/run_redis_perf.sh` |
| test-workspace | 0 | `cargo --config /home/hxy/perf-seqwrite-20261102/harness/cargo-ci.toml test --workspace --lib --bins` |

### 硬指标逐项判断

| 检查 | 达标 | 实测/限制 |
|---|---|---|
| memmove_self | 是 | `{"value": 8.553068912031907, "limit": 10}` |
| compression_inclusive | 是 | `{"value": 0.9195656990915134, "limit": 15, "note": "Uses policy/codec/framing plus checksum union; also retain raw codec and full module bounds."}` |
| profile_valid | 是 | `{"lost_samples": 0, "unknown_self_pct": 0.7201418125415466}` |
| seqwrite_active_mib_s | 是 | `{"value": 184.47232913970947, "minimum": 150}` |
| seqwrite_wall_plus_drain_mib_s | 是 | `{"value": 153.75, "minimum": 130}` |
| seqwrite_precise_wall_plus_drain_mib_s | 是 | `{"value": 154.40387101196572, "minimum": 130}` |
| randwrite_nonregression | 是 | `{"geomean_change_pct": 1.6303056454196558, "95_ci_pct": [0.33146441251731956, 2.9459610308728923]}` |
| randread_nonregression | 否 | `{"geomean_change_pct": 16.2636842517338, "95_ci_pct": [-9.674834154093439, 49.65092119340338]}` |
| seqread_nonregression | 否 | `{"geomean_change_pct": 1.3920648318899431, "95_ci_pct": [-53.34237014622902, 120.33589882498698]}` |
| randrw_nonregression | 否 | `{"geomean_change_pct": 3.740971028381771, "95_ci_pct": [-7.194761215264557, 15.965318454433039]}` |
| randrw_read_p99_nonregression | 否 | `{"geomean_change_pct": -0.3615042275532412, "95_ci_pct": [-4.306882389664817, 3.746539852764198]}` |
| randrw_write_p99_nonregression | 是 | `{"geomean_change_pct": -1.0811657845912979, "95_ci_pct": [-3.730808167011468, 1.641403404640513]}` |
| content_and_crash | 是 | `{"bytes": 2147483648, "cases": 4}` |
| local_ci | 是 | `{"missing_or_failed": []}` |
| source_identity | 是 | `{"mismatches": []}` |
| broad_complete | 是 | `{"arms": 2}` |
| compressible_complete | 是 | `{"arms": 6}` |
| broad_observed_nonregression | 否 | `{"unresolved_regressions": {"seqread": {"baseline_mib_s": 31.84363079071045, "candidate_mib_s": 21.378621101379395, "change_pct": -32.86374521206906}, "randrw": {"baseline_mib_s": 0.41255760192871094, "candidate_mib_s": 0.3887901306152344, "change_pct": -5.7610067545388555}, "bigread": {"baseline_mib_s": 36.5923376083374, "candidate_mib_s": 33.215209007263184, "change_pct": -9.22905947474849}}, "note": "One pair only; tool exit success does not establish performance acceptance."}` |
| compose_direct_guard_complete | 是 | `{"arms": 2}` |
| direct_guard_complete | 是 | `{"arms": 6}` |
| crash_teardowns | 是 | `{"count": 8}` |

完整原始目录：`/home/hxy/perf-seqwrite-20261102/`；提交保留紧凑证据、
两份火焰图和 folded、原始 fio JSON、精确 phase 时间戳、协议和复现脚本。
脚本中的绝对路径与独立服务端口需按环境调整，依赖 Python PyYAML/boto3、
Docker Compose、fio、perf 与 inferno。历史基线和已接受 README 表均未改写。

### 发布判断

**保持 draft，不转 ready、不合并。** CPU、顺序写 active 和包含严格排空的
墙钟带宽已达标，完整内容与 SIGKILL 后重启读回通过；正式三对的 randread、
seqread、randrw 95% CI 仍不能排除超过 3% 的吞吐回归，randrw read p99
95% CI 上界 +3.75% 也超过 3%。Broad 单对的 seqread -32.86%、bigread
-9.23%、randrw -5.76% 需要进一步诊断。样本量、冷读/后端状态和可压缩
数据微基准风险尚未充分解释；本轮保留候选供 review，不宣称优化已接受。
