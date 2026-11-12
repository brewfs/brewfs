# 小随机写对象聚合验收报告

## 第二轮最终源码验收（2026-11-02）

**状态：第二轮合并门禁通过。** 下述原始失败、复测及测量限制均保留。

本轮起点为 `d5e53e1ca532762cf7ffb361430d939a0b97eb7c`，生产代码与测试断言
均未修改。下面的数据全部来自本轮新编译的 release；旧 v1 测量仅保留在文末
历史记录中。升级、默认开启行为和回滚限制见[运维说明](../operations/write-coalescing.md)。

### 二进制与逐文件源码身份

| 项目 | 值 |
|---|---|
| release SHA-256 | `e2f12ee08c23f29369be8982ecf87b416d2f190a8cbf85e652c773dfffb32313` |
| source manifest SHA-256 | `5e954a6ff76ac0c6fa3828d2148620c32d7eb16022d718a8f9a2f82c8fde26bf` |
| `src/vfs/io/writer.rs` SHA-256 | `a42b77d9d0ccee4e67ced80b3d688cc02ad2d6dfe86f21565ec00b59ba2d5fee` |
| `src/meta/stores/redis/mod.rs` SHA-256 | `ee457f68cc752e70a75fa7bdd6e4a317bb7d12b015fff521bad028fa8dd942ff` |

[source-manifest.json](evidence/write-coalesce/r2/source-manifest.json) 记录全部
1,858 个 tracked 文件各自的 SHA-256。overlay、release 构建前后、同轮 CI、测量
起点使用相同 manifest。`--version` 中的 dirty=true 来自既有未跟踪文件
`tools/perf/measure_observability.py`，该文件未改动且不是编译输入。

构建命令为 `cargo build --locked --release --bin brewfs --features workspace,observability`，
默认 features 开启，incremental=0、debug=0、codegen-units=256、
`RUSTFLAGS=-C debuginfo=0`，与 v1 的优化配置一致。tmux 后台构建未加 timeout，
主机 3072 MiB watchdog 保持运行。library 用 1 job 完成后，刚开始的 main.rs
编译主动中断并以 4 jobs 重启，复用同源码 library；第一次 Cargo 130 和调度原因
均已归档，不算成功构建。最终 Cargo exit 0，总耗时 102m42s（含调度切换）。
完整配置见 [build-identity.json](evidence/write-coalesce/r2/build-identity.json)。

### Overlay 与同轮 CI

两次未筛选的 `cargo test -p brewfs --lib --features workspace` 均为
**2729 passed / 0 failed / 474 既有 ignored**，第二次于 03:36:06 UTC 完成，
距本轮开始 47m58s，满足 90 分钟定位时限。内存配置为 1 个编译 job、
`RUST_TEST_THREADS=2`；没有修改测试选择、断言或 ignore。

已知偶发用例为
`workspace::meta_layer::tests::writable_acl_independent_sqlite_instances_keep_mode_acl_during_atime_writes`。
五次单测重跑结果依次是 **pass / fail / pass / pass / pass（4/5）**。
失败在 `meta_layer/tests.rs:1477:57` 的
`observer.open(ino, OpenFlags::RDONLY).await.unwrap()`，错误为
`Io(Os { code: 16, kind: ResourceBusy, message: "Device or resource busy" })`。
测试、meta_layer 实现与 SQLite store 文件均与 PR base `a41b126` 一致，保留为
既有并发 EBUSY flaky 证据。门禁依据是两次完整套件零失败，**不声称 focused 5/5 通过**。
命令、时间和失败日志哈希见 [overlay-summary.json](evidence/write-coalesce/r2/overlay-summary.json)。

同一源码迭代的最低本地 CI 与追加检查共 **17 个独立检查全部 exit 0**：fmt、4 个 shell
语法检查、3 个 perf report 测试、Redis 故障注入、workspace check/build、
tokio/io-uring 配置、workspace 测试、clippy workspace、diff check，及 workflow 的
`clippy --workspace --all-targets -- -D warnings`。
其中 `CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins`
通过 **1315 库测试 + 2 二进制测试，0 failed，264 既有 ignored**。
rfuse3 不在 workspace，条件检查不适用；既有依赖 warning 保留，clippy exit 0。
完整命令和日志哈希见 [ci-summary.json](evidence/write-coalesce/r2/ci-summary.json)。

### 冻结协议与最终 release 配对结果

同一个最终 release，`BREWFS_WRITE_COALESCE_BYTES=0/65536` 交错 off/on。
沿用 v1 顺序 randwrite → seqwrite → randrw，每场景 **2 对预热 + 3 对正式测量**，
每轮 20 s。随机场景 4 KiB、4 jobs、iodepth=32、512 MiB/job；顺序写
1 MiB、1 job、iodepth=8、512 MiB。fio buffered libaio、seed=20261101；
混合读写 70% read / 30% write。direct guard 单独用 direct=1，不混入主 CI。

隔离 compose Redis/RustFS、flat、upload-before-commit、zstd、同一不可压缩
语料；读内存/SSD cache=0、写内存=512 MiB、写 SSD=0、总预算=2 GiB。
每 arm 清空自己的后端，重新挂载、完整预填充并严格 drain 后测量；没有全局
drop_caches。严格 drain 要求所有写回 debt gauge 为 0 且 PUT 次数/字节连续
3 次稳定，间隔 0.5 s、180 s 上限。下表计入 close/wall 和 drain 的结果用于
排除把前台成本转移到尾部的假收益。完整环境与 v1 差异见
[protocol.json](evidence/write-coalesce/r2/protocol.json)。

| 场景 | off / on MiB/s，中位数 | 变化 | 配对差值 95% CI，MiB/s | off / on wall+drain MiB/s |
|---|---|---:|---|---|
| randwrite | 2.5057 / 5.2202 | +108.33% | [+2.5726, +2.7899] | 2.2368 / 4.7519 |
| randrw，总吞吐 | 2.7811 / 3.4273 | +23.23% | [+0.5266, +0.7963] | 2.5546 / 3.1644 |
| seqwrite | 135.8728 / 134.4866 | −1.02% | [−9.4185, +10.4877] | 125.9439 / 125.1858 |

CI 用三对 on-minus-off 差值计算 Student t 95% 区间，自由度 2。
有限样本和固定 off/on 顺序仍是本结果的限制；不声称 seqwrite 加速。

| 场景 | off / on PUT 数 | off / on PUT 字节 | off / on avg PUT，B | off / on PUT/GiB |
|---|---|---|---|---|
| randwrite | 12812 / 1670 | 52518793 / 110687311 | 4099.19 / 66272.36 | 261490.89 / 16379.61 |
| randrw | 4317 / 333 | 17660679 / 21944261 | 4090.96 / 65959.41 | 262022.61 / 16459.04 |
| seqwrite | 680 / 673 | 2835573644 / 2806367937 | 4169996.99 / 4169937.50 | 256.19 / 256.19 |

以上各列独立取三轮中位数，不能用中位 PUT bytes 除中位 PUT count 反推平均值。
PUT/GiB 使用 fio 逻辑写入字节归一化，排除 prefill、包含 drain；on 在固定时间内
写入更多数据，所以物理字节总量可增加。randwrite 三个 on 轮次的 PUT/GiB 均
≤16388.87，avg PUT 均 ≥66235.45 B，分别满足 ≤26000 和 ≥40000 B 门槛。

| 场景 | off p50 / p95 / p99，ms | on p50 / p95 / p99，ms | p99 变化 | off / on RSS 峰值中位数，MiB |
|---|---|---|---:|---|
| randwrite | 206.569 / 329.253 / 387.973 | 91.750 / 145.752 / 235.930 | −39.19% | 292.86 / 313.75 |
| randrw，写 | 177.209 / 227.541 / 256.901 | 143.655 / 179.306 / 198.181 | −22.86% | 142.34 / 146.02 |
| seqwrite | 32.899 / 149.946 / 170.918 | 33.161 / 149.946 / 170.918 | 0.00% | 195.02 / 185.68 |

randrw 读 p99 为 270.533 → 200.278 ms（−25.97%）。RSS 为 1 Hz 采样峰值。
randwrite active+drain 吞吐为 2.3817 → 4.9612 MiB/s；wall+drain 提升
112.44%，未观察到收益转移到 close/drain。每轮原始 fio、PUT、分位数和 RSS
见 [comparison.json](evidence/write-coalesce/r2/comparison.json) 与
[逐轮表](evidence/write-coalesce/r2/comparison-table.md)。

### Direct guard

每场景单对 off/on，均使用最终 release 和 fio direct=1；不作为独立显著性证明。

| 场景 | off / on MiB/s | off / on 写 p99，ms | off / on wall+drain MiB/s | on avg PUT，B | on PUT/GiB |
|---|---|---|---|---:|---:|
| randwrite-direct | 2.6187 / 5.2049 | 387.973 / 177.209 | 2.3291 / 4.7443 | 66245.06 | 16386.46 |
| seqwrite-direct | 160.3759 / 156.5843 | 145.752 / 156.238 | 145.9903 / 145.3636 | 4172815.64 | 256.00 |
| randrw-direct | 2.8091 / 3.4543 | 278.921 / 196.084 | 2.5237 / 3.1922 | 65914.37 | 16469.78 |

randwrite-direct 满足 PUT/GiB、avg PUT 和 p99 门槛；额外 seqwrite-direct 带宽
−2.36%、p99 +7.19%，也在 −3% / +10% 范围内。randrw-direct 读 p99 为
278.921 → 198.181 ms。全部 **36 个主测/预热/direct arm** 严格 drain 通过，
正常 unmount exit 0、daemon exit 0、无残留挂载。部分 direct arm 的 drain 为
1.56–1.57 s，其成本已计入表中。

### 完整 broad compose 与读性能追加取证

实际运行仓库 `run_perf_in_container.sh`，包含七个 fio 场景及 metaperf、
dirstress、dirperf，**off 10/10、on 第二次完整运行 10/10**。保留 v1 预先
准备的负载、参数和断言；入口包装仅记录真实卸载命令与退出后挂载/进程表。
三个 Python helper 使用内容逐字节相同的可执行副本，避免原仓库 0644 模式
不能在镜像中直接执行的问题。没有使用 fallback 元数据探针替代真实工具。

首次 on 运行不能接受：bigread 预填充第 74 秒的一个 `.stats` 样本为空，
触发 `writeback-samples.invalid`；bigread 预填充及 bigwrite 的 fio 错误码均为
0，但正式 bigread 未执行，后续工具因该持久无效标记而失败。stats 读取默认
超时为 5 秒，runner 没有保存该次读取的退出码，不能把空样本的具体原因
写成已经证实的超时。失败现场在 `broad-attempts/on-attempt1/`，原始 stage
exit 1 在 `broad-attempts/initial-stage/`。随后保持相同断言、采样、超时和
二进制，重新运行完整 on 组；不是删标记或只重跑 bigwrite。

有效的 off/on 两组各发生 5 次普通 `fusermount3 -u`，全部 exit 0，
退出后均无残留 FUSE 挂载；失败的第一次 on 也正常卸载。所有最终 fio
错误码为 0，prefill、写入与元数据严格 drain 均通过，最终债务全部为 0。
完整对比见 [broad-comparison.md](evidence/write-coalesce/r2/broad-comparison.md)，
逐项状态、缓存、Redis commandstats 与尾部债务见
[broad-summary.json](evidence/write-coalesce/r2/broad-summary.json)。

| broad 场景（单次完整运行） | off / on MiB/s | 变化 |
|---|---|---:|
| seqread | 31.495 / 21.534 | −31.63% |
| seqwrite | 38.596 / 38.891 | +0.76% |
| randread | 1.696 / 1.605 | −5.37% |
| randwrite | 2.388 / 4.858 | +103.42% |
| randrw，总吞吐 | 1.587 / 1.777 | +11.99% |
| bigread，完整 4 GiB | 37.594 / 31.795 | −15.42% |
| bigwrite，完整 4 GiB | 178.646 / 178.872 | +0.13% |

broad randrw 读/写 p99 从 434.110 / 425.722 降至 371.196 / 371.196 ms。
broad 采用原 runner 的数据生成与准备流程，其绝对吞吐不与主配对的预填充
语料混算。以上单对数据明确显示两个读吞吐差异，所以工具返回成功后并未
立即接受性能；追加了事先记录顺序与门槛的读复测。

对比脚本同时保留整数秒计时口径：randwrite active+drain 为 2.077 → 4.225、
randrw 为 1.380 → 1.545 MiB/s。bigwrite 的 wall / active+drain 口径分别显示
−4.2% / −3.7%，不能只报告 active 的 +0.13%。进一步核对原始纳秒 phase
boundaries，bigwrite 从 tool_start 到严格 drain 完成为 26.689880 →
26.827351 s，精确 wall+drain 为 **153.466 → 152.680 MiB/s（−0.51%）**；
整数秒的 23/24 s tool wall 与 2/3 s drain 放大了显示差值。原始计时和计算
同时保存在 [broad-exact-wall.json](evidence/write-coalesce/r2/broad-exact-wall.json)。

seqread 的三对顺序为 off/on、on/off、off/on，其他参数及 prefill/drain/remount
完全不变。三对 off/on MiB/s 分别为 **22.824/21.248、30.874/30.615、
30.728/31.105**；中位数 **30.728 → 30.615（−0.37%）**，p99 中位数
**408.945 → 438.305 ms（+7.18%）**，满足预先记录的 −3% / +10% 门槛。
配对差值 95% CI 为 [−2.960, +1.987] MiB/s，不声称顺序读加速或严格等价。
六臂每次卸载后均列出了完整对象清单：129 个对象、0 个共享对象，表明本次
seqread 数据未走共享对象格式。首次 broad on 尝试（后来因空 stats 样本失效）
中的 seqread 只有 21.472 MiB/s，
当时 host CPU pressure 从 6.95% 升至 14.66%，S3 GET 平均延迟从 13.265 升至
19.366 ms；这些是同时观测到的差异，不能当作已经证明的根因。
全部原始样本与限制见 [seqread-recheck-summary.json](evidence/write-coalesce/r2/seqread-recheck-summary.json)。

bigread 另补一对同配置、完整 4 GiB 的 off/on：**33.030 → 32.416 MiB/s
（−1.86%）**，均无 fio 错误、无无效采样、正常卸载；对象清单中均无共享对象。
平均延迟为 26918.105 → 27408.963 ms（+1.82%），最大延迟为
32999.612 → 34191.678 ms（+3.61%）。该场景的 fio 延迟直方图在约 17.1 s
饱和，报告的 p99 均为 17112.760 ms，低于实际均值；**不能据此证明真实
bigread p99 没有回退**。这限制了额外读 guard 的延迟结论，不替代也不改变
主写入矩阵已有效测得的 p99 门槛。保留一对复测的局限，不声称统计等价。
见 [bigread-recheck-summary.json](evidence/write-coalesce/r2/bigread-recheck-summary.json)。

| metaperf 操作，ops/s | off | on | 变化 |
|---|---:|---:|---:|
| create | 149.97 | 142.22 | −5.16% |
| open | 1178.78 | 1287.35 | +9.21% |
| stat | 544857.18 | 569347.42 | +4.49% |
| readdir | 8876.67 | 9341.37 | +5.23% |
| rename | 335.49 | 350.63 | +4.51% |

元数据表也是单次 broad 观测，不声称统计显著性。明确保留 create 的下降；
open/stat/readdir/rename 没有观察到吞吐回退。dirstress 的预期并发 errno
原样保留，未发现 EIO 或 ENOSYS。Redis commandstats 也保留真实 failed_calls，
不把成功完成的 workload 等同于 Redis 服务器零错误回复。

### 最终 release 的内容与崩溃恢复

8192 次 4 KiB 稀疏随机写产生 **512 PUT / 34,013,184 PUT bytes**，
平均 **66,432 B**、**16,384 PUT/GiB**；所有立即读回和最终块哈希一致。
off/on 分别在 direct 与 buffered FUSE 下运行 fsync、close 后立即 SIGKILL，
重新挂载使用新 cache：共八个场景，每个场景全量验证 **8192 块 SHA-256**，
空文件和单字节文件也正确。所有块哈希列表摘要一致：
`6b63e88a9ea558535b198d528970a12eba20412ee1653a5ffb1e4222ce1a4f90`。
内容测试与崩溃测试合计 17 次卸载均成功，死亡守护进程退出 −9、正常退出为 0。
此证据限定于挂载进程崩溃，没有扩大为 Redis 或对象服务器断电证明。
见 [correctness-summary.json](evidence/write-coalesce/r2/correctness-summary.json)。

### 验收范围与证据保存

本轮没有新增生产代码或放宽测试断言。最终源码的 overlay、最低 CI、release
配对矩阵、direct guard、完整 broad 与挂载进程崩溃恢复均完成。
主 randwrite 的 PUT/GiB、avg PUT、配对置信区间、p99 和 seqwrite 回归门槛全部
满足；randrw 吞吐、读写 p99 和计入尾部成本的指标也通过。
完整 broad 的首次无效观测、原始读下降和追加复测均保留，未删除不利样本。

本结果支持保留默认 `BREWFS_WRITE_COALESCE_BYTES=65536`，不是多窗口寻优结论。
范围仍是 flat + Redis/SQL + UploadBeforeCommit；真实 S3/FUSE 性能使用
Redis/RustFS，SQL 覆盖来自 SQLite 正确性测试。native-v2、packed、etcd/TiKV
和 CommitBeforeUpload 不适用。0 关闭新聚合但保留 reader；**旧二进制不能读取
新共享对象，写入新格式后不能直接降级旧二进制**。

原始目录：`/home/hxy/perf-write-coalesce-r2-20261102/`。
仓库证据见 [r2/README.md](evidence/write-coalesce/r2/README.md)、
[acceptance.json](evidence/write-coalesce/r2/acceptance.json) 和
[supplemental-review.json](evidence/write-coalesce/r2/supplemental-review.json)。
压缩包保存原始 fio、PUT、RSS、drain、CI/overlay 日志、协议与 harness；大体积
release 二进制、CPU profile 和 FUSE trace 保留在主机原始目录。任务 compose
服务与 volumes 已清理，没有任务挂载或 D-state 遗留；既有 JuiceFS 挂载未动。
后续提交仅更新文档和证据，编译输入逐文件哈希继续与被测 d5e53e1 一致。

---

<details>
<summary>第一轮交付与自审历史（旧 v1 数字不作为最终源码验收）</summary>


升级、默认开启行为、混合版本挂载限制与回滚步骤见
[运维说明](../operations/write-coalescing.md)。下文保留原交付快照；后续自审
发现 Redis 非规范 chunk version 可导致部分发布，修复与验证状态见
[实验计划的自审记录](../superpowers/plans/2026-11-02-write-coalesce.md#pr-404-adversarial-review)。

开始：2026-11-02 04:48:18 Asia/Shanghai。截止：09:48:18。
代码基点：`a41b126`；分支：`perf/write-coalesce`。
证据目录：`/home/hxy/perf-write-coalesce-20261102/`。

当前状态：**未达到完整验收，保持 draft，不合并。** 被测候选 v1 的主要 A/B
数值及内容/崩溃验证通过，但完整 workspace 测试发现了跨块配置回归。最小修复后
workspace 和最终 debug 构建的真实 FUSE 内容/崩溃验证均通过。overlay 编译被
主机内存 watchdog 终止，已用 1 个编译 job 重跑；最终 clippy 尚未执行。
主要 A/B 属于修复前 release，最终 release 尚未构建和重测。README 性能表未更新。
旧状态报告已保存到新证据目录的
`environment/previous-report.md`，历史完整基线目录保持不变。

## 已取得的证据（尚未验收）

- 先实现断言再改上传路径。FUSE 红灯在 2026-11-02 05:24 完成：
  8192 个 4 KiB 随机写、32 MiB 数据产生 **8192 PUT / 33,587,200 PUT bytes**，
  平均 **4100 B**，**262144 PUT/GiB**，远超上限 812 PUT。
  每次写后的立即读回和最后 8192 块 SHA-256 全部正确。
- 红灯使用上一轮已验收 main 对应的未改动二进制，SHA-256
  `c0629e69b37a3ed2beaccca3386f6a8b2748855b8bce2db4e3a42febbf0c8064`。
  这是功能红灯，**不是新性能基线**；独立构建后的新基线见下表。
- 旧实现崩溃对照通过：fsync 和仅 close 两种情况下，成功返回后立即 SIGKILL，
  重启后各 8192 块 SHA-256 全部一致，空文件及单字节文件正确。
  SIGKILL 返回码均为 -9；卸载已用 findmnt 确认。第一次脚本未先关闭死亡
  FUSE 的 fd，造成卸载失败；两个 harness 失败日志保留，未计入正确性通过。
- 直接针对候选源码内嵌 Lua 的独立 Redis DB 1 测试通过：注册冲突、幂等发布、
  消费预留、回收阻止迟到发布、禁止删除仍有活成员的 ledger、重复回收、
  删除后不复活 ledger、32 个并发回收。
- Rust 首轮专项测试：19 passed / 3 failed。8192 次随机写的 PUT 上限、每块及
  空洞内容校验、512 MiB 顺序写绕过聚合、交叠写、GC/截断和 SQL 重启均通过。
  三个额外回归断言真实失败后已修复：单字节完整文件显式 flush 不应等待聚合，
  native 读不应探测 flat 共享对象，失败上传应保持原有五次尝试。
  修复后的重跑为 **22 passed / 0 failed / 0 ignored**，127.26 秒；其中新增
  测试 15 个，另有 7 个现有匹配测试。最终完整门禁结果见末尾状态表。
- 自我 review 的 Redis 故障注入还复现了两类问题：cjson 在标记发布成功后因
  非有限数字序列化失败，以及 ACL 拒绝导致部分注册/回收。现已将序列化和
  写权限检查放在修改前；完整故障测试通过，红/绿日志均保留在 validation。

## 协议与进度

- 当前 main 二进制使用独立 worktree 和独立 CARGO_TARGET_DIR，tmux 后台单次编译。
- 对应 Rust 测试及边界、失败上传、覆盖写、旧对象读取、回收和 SQL 重启测试已通过
  专项检查；完整本地 CI 于 08:48 Asia/Shanghai 开始串行执行。
- 已固定 protocol.json：20 秒，4 jobs/depth 32，两次预热，三对 off/on，
  配对 Student t 95% CI；采集 PUT、有效带宽、分位数、RSS、perf、严格 drain。
- fsync/close 后立即 SIGKILL、重新挂载、全量逐块 SHA-256 的 off/on 对照均通过。
- v1 三个负载的主 A/B、direct guard 和 perf 采样已完成；最终源码的 workspace
  门禁通过，overlay 正在重试，clippy 与最终 release/broad 验收仍缺。

90 分钟节点（06:18:18 Asia/Shanghai）未完成：功能红灯已于 05:24 完成，
但独立 worktree 的 release 构建仍在编译最终二进制，因此新性能基线延期。
06:19 检查 rustc 仍运行，未见内存 watchdog 杀进程；未替换成历史性能结果。

3 小时节点（07:48:18 Asia/Shanghai）也未完成：候选 release 于 07:28:47 开始，
该节点仍在构建，尚无候选 A/B 数字。专项测试及提前运行的严格 clippy 已通过。
编译仍按单个 Cargo 流水线执行，watchdog 保持运行；没有用旧数据代替本次测量。

## 新 main 基线（本次实测）

独立 worktree `a41b1266`，release 构建成功，用时 108m 06s；二进制 SHA-256：
`8b6c188d6588e0f68813a2b271ef177e3e81f01e15ab8fd8d689e991565519cb`。
两次预热后，三次正式测量于 06:42:22 Asia/Shanghai 完成。

| 轮次 | MiB/s | p99 ms | 平均 PUT B | PUT/GiB | 含完整 wall 与 drain 的 MiB/s |
|---|---:|---:|---:|---:|---:|
| 1 | 2.6438 | 387.973 | 4099.858 | 261448.145 | 2.3435 |
| 2 | 2.5828 | 476.054 | 4099.821 | 261450.288 | 2.2741 |
| 3 | 2.5722 | 425.722 | 4098.909 | 261509.077 | 2.2684 |
| 中位数 | 2.5828 | 425.722 | 4099.821 | 261450.288 | 2.2741 |

全部轮次严格 drain 通过、正常卸载；原始计数、p50/p95、RSS 见
`baseline-comparison.json` 和 `rounds/randwrite/measure-*-main/`。
这是新基线证据，不代表候选优化已通过验收。

## 随机写 A/B 原始结果（完整 CI 前不验收）

同一个候选二进制，off/on 交错，两对预热后测量三对，每轮 20 秒。
正式随机写于 08:31:46 Asia/Shanghai 完成，较 3 小时节点晚约 43 分钟。

| 对 | off MiB/s | on MiB/s | 差值 MiB/s | off p99 ms | on p99 ms |
|---|---:|---:|---:|---:|---:|
| 1 | 2.4302 | 5.0878 | +2.6576 | 471.859 | 200.278 |
| 2 | 2.5094 | 5.0270 | +2.5176 | 438.305 | 219.152 |
| 3 | 2.4651 | 5.1553 | +2.6903 | 497.025 | 175.112 |

配对差值均值 **+2.6218 MiB/s**，Student t、自由度 2 的 95% CI 为
**[+2.3940, +2.8497] MiB/s**，不含零。带宽中位数提升 **106.40%**。
候选 off 中位数为 2.4651 MiB/s，低于之前新鲜 main 的 2.5828 MiB/s；
本报告分别保留这两组数据，配对 CI 只使用同一二进制的 off/on 正式轮次。

| 指标（三轮中位数） | off | on |
|---|---:|---:|
| PUT/GiB（以逻辑 fio 写入字节归一化） | 261512.07 | 16385.24 |
| 平均 PUT，B | 4099.04 | 66249.75 |
| p50 / p95 / p99，ms | 204.472 / 371.196 / 471.859 | 92.799 / 162.529 / 200.278 |
| RSS 峰值，MiB | 287.01 | 300.70 |
| active + drain，MiB/s | 2.3413 | 4.8372 |
| 完整 wall + drain，MiB/s | 2.2000 | 4.6484 |
| drain，s | 1.0502 | 1.0440 |

| 对/模式 | PUT 次数 | PUT 字节 | PUT/GiB |
|---|---:|---:|---:|
| 1/off | 12415 | 50889520 | 261512.07 |
| 1/on | 1633 | 108160462 | 16389.02 |
| 2/off | 12820 | 52551282 | 261491.29 |
| 2/on | 1611 | 106740434 | 16383.36 |
| 3/off | 12620 | 51722241 | 261542.99 |
| 3/on | 1650 | 109312092 | 16385.24 |

全部正式轮次严格 drain 通过。物理 PUT/GiB 减少约 **15.96×**，p99 中位数降低
57.56%，RSS 峰值中位数增加 13.68 MiB。含完整 wall 和 drain 的有效带宽提高
111.29%，没有把本项收益移到关闭/drain。逻辑 slice 仍独立，减少的是物理对象数。
原始 p50/p95/p99、RSS 采样、fio JSON、perf、PUT 计数均保存在每轮目录。
混合读写和 direct guard 结果见下文；这些是 v1 观测值，不能代替最终 release 验收。

## 大文件顺序写回归（原始结果）

512 MiB 工作集，1 MiB 写、1 job/depth 8，同样两对预热、三对正式测量。

| 对 | off MiB/s | on MiB/s | off p99 ms | on p99 ms |
|---|---:|---:|---:|---:|
| 1 | 129.6870 | 132.6378 | 181.404 | 168.821 |
| 2 | 134.2433 | 134.9365 | 168.821 | 166.724 |
| 3 | 133.9744 | 133.3800 | 164.626 | 170.918 |

带宽中位数 **133.9744 → 133.3800 MiB/s，-0.44%**，满足不回退超过 3%。
配对差值均值 +1.0165 MiB/s，95% CI [-3.4414, +5.4744] MiB/s；不声称顺序写加速。
平均 PUT 中位数 4,168,391 → 4,171,525 B，仍为约 4 MiB；512 MiB Rust 测试还
明确断言未创建共享对象目录。p99 中位数均为 168.821 ms，RSS 峰值中位数
191.05 → 187.91 MiB，wall+drain 有效带宽 124.57 → 124.12 MiB/s（-0.36%）。
全部正式轮次严格 drain 通过。

## 混合读写回归（原始结果）

70% 读 / 30% 写，4 KiB、4 jobs/depth 32，两对预热、三对正式测量。

| 对 | off MiB/s | on MiB/s | off 读/写 p99 ms | on 读/写 p99 ms |
|---|---:|---:|---|---|
| 1 | 2.7750 | 3.3830 | 258.998 / 258.998 | 233.832 / 227.541 |
| 2 | 2.8406 | 3.3017 | 252.707 / 250.610 | 200.278 / 198.181 |
| 3 | 2.6137 | 3.3475 | 283.116 / 278.921 | 206.569 / 204.472 |

总带宽中位数 **2.7750 → 3.3475 MiB/s（+20.63%）**。配对差值均值
**+0.6010 MiB/s**，95% CI **[+0.2618, +0.9401] MiB/s**。
读 p99 中位数 258.998 → 206.569 ms（-20.24%），写 p99 中位数
258.998 → 204.472 ms（-21.05%）。wall+drain 有效带宽 2.5487 → 3.0945 MiB/s
（+21.42%），RSS 峰值中位数 145.49 → 142.73 MiB。PUT/GiB 按 fio 的写入字节
归一化为 262022.33 → 16525.57，平均 PUT 4090.97 → 65694.36 B。
全部正式轮次严格 drain 通过，未观察到 mixed-I/O 吞吐或尾延迟回归。

## direct-I/O guard

每场景单对 off/on，fio direct=1；不混入主 A/B 的置信区间。

| 场景 | off / on MiB/s | off / on 写 p99 ms | off / on wall+drain MiB/s | on 平均 PUT B | on PUT/GiB |
|---|---|---|---|---:|---:|
| randwrite | 2.5947 / 5.4005 | 400.556 / 227.541 | 2.3024 / 4.8141 | 66238.28 | 16388.14 |
| seqwrite | 162.2338 / 164.2996 | 147.849 / 139.461 | 150.8451 / 152.9180 | 4168226.07 | 256.23 |
| randrw | 2.7463 / 3.4599 | 283.116 / 198.181 | 2.5224 / 3.1968 | 65951.40 | 16460.55 |

randrw 读 p99 为 278.921 → 193.987 ms。全部严格 drain 与卸载通过。
randwrite 的 on drain 为 1.560 s（off 1.048 s）；计入此代价后有效带宽仍提高约
109.1%，没有忽略尾部成本。单对 guard 只能排查明显回归，不作为独立统计显著性证明。

## perf 对象上传热点

从三个正式 randwrite 轮次的 cpu-clock profile 汇总 self CPU period，先 Rust
demangle 并移除泛型参数再按符号归类；使用精确 period，不累加四舍五入的百分比。
以下为各轮归一化值的中位数；完整命令、样本、top symbols 见 cpu-comparison.json
及每轮 cpu-report.txt。九份 main/off/on 正式 profile 均无丢样。

| 指标 | off | on |
|---|---:|---:|
| SDK/HTTP self CPU，% | 11.693 | 2.584 |
| SDK/HTTP 采样 CPU，ms/逻辑写 MiB | 49.179 | 5.558 |
| 总采样 CPU，ms/逻辑写 MiB | 421.840 | 218.193 |
| 聚合相关 self CPU，% | 0.264 | 0.596 |
| 校验 self CPU，% | 2.176 | 6.127 |
| 压缩 self CPU，% | 1.452 | 1.871 |

SDK/HTTP 的归一化采样成本下降 88.70%，符合减少请求数的假设。新索引/成员 SHA-256
带来额外校验成本；压缩所占比例增加不等于其绝对成本增加。此分类只解释采样 CPU，不代表 off-CPU 等待、远端服务 CPU
或精确的每次 PUT 成本，也不替代带宽和延迟验收。

自查纠正了初版分类器把 sha2::sha256::soft::compress 计入压缩的问题，并加入
SHA-1 符号。现先识别校验再识别压缩；旧分类输出保留为
validation/profile-classifier-before.json，不纳入结论。SDK/HTTP 和总 CPU 结果不受影响。

## 被测候选 v1 功能与崩溃验证

候选 release 构建成功（54m 13s），SHA-256：
`6e3e9d0ac6bae33aba4a8d3c1b1a22984e77600c711bcebaf48b5ba7276b34a7`。
测量时的生产源码、runner 和测试源码与构建快照一致；随后完整 CI 触发了下述
资格检查修复，因此该二进制不代表最终源码。

真实 FUSE 的 8192 次 4 KiB 随机写通过：**512 PUT / 34,013,184 PUT bytes**，
平均 **66,432 B**，**16,384 PUT/GiB**。与原实现的 8192 PUT 相比减少 **16×**。
8192 次立即读回及最后 8192 块 SHA-256 全部一致，严格 drain 和卸载通过。

候选的 fsync、仅 close 两种崩溃测试均通过：操作成功返回后立即 SIGKILL，
不插入 drain 或等待；fsync 场景保留打开的 fd。新 cache 重新挂载后，各全量
读取 32 MiB、验证全部 8192 块 SHA-256，零错误；空文件、单字节文件也正确。
两个被杀进程退出码均为 -9，四次卸载均成功，未残留挂载。

**在本次验证的 FUSE 守护进程崩溃范围内，聚合没有改变崩溃语义**：成功返回的
fsync/close 保持内容可恢复；未 fsync 数据仍可丢失。此证据针对挂载进程 SIGKILL，
没有将其扩大为对象存储或 Redis 服务器断电的证明。对照实现使用同一脚本也通过。

## 最终源码的追加正确性验证

修复后的 workspace 调试二进制 SHA-256 为
`fbe0f9f64c6718fa8e8f988b6ae33e3be96f857fa692843a1687ee742fe64e91`。
使用 dev profile、默认 features、debug=0、incremental=0；对应源码与第二轮 CI
快照一致。它仅用于正确性验证，没有用调试构建替代 release 性能样本。

真实 Redis/RustFS + FUSE 补测通过：fsync、仅 close 两场景各随机写满 32 MiB，
每次立即读回；成功返回后立即 SIGKILL，重挂载后读取整个文件，分别验证全部
8192 个 SHA-256 块。空文件、单字节文件均正确，两个死亡进程均退出 -9，四次
卸载均成功，无残留挂载。off、v1 release、最终 debug 三者的块哈希列表摘要相同：
`6b63e88a9ea558535b198d528970a12eba20412ee1653a5ffb1e4222ce1a4f90`。
因此最终源码在这些挂载进程崩溃用例中也保持 fsync/close 持久性语义。

证据为 correctness/final-debug-on-summary.json、对应 before-kill/after-restart
目录及 build/final-debug.json。补测最初沿用 `--metrics`，但默认 features 的
调试构建没有此 CLI 参数，挂载在启动阶段退出；该 harness 失败日志保留在
validation/final-functional-cli-attempt1/。改为不启用 HTTP metrics 后原内容、
SIGKILL、卸载断言完整通过。此调试补测没有提供新的物理 PUT 性能计数。

第二轮完整 workspace 测试通过：**1315 库测试 + 2 二进制测试，0 failed**；
264 个既有 ignored 未变，包括首次失败的 pwrite 断言现已通过。新增 8192 随机写
PUT 上限/全量块与空洞检查、512 MiB 顺序写绕过聚合等测试也包含在这一轮中。
overlay 和严格 clippy 的最终结果见下方本地门禁表。

## 实现边界与默认值依据

现有 slice 元数据只表示连续范围。实现保留独立逻辑 slice，通过一个物理对象保存
多个小 slice，避免补洞读取和重写原文件数据。读者仍按原有 slice 覆盖顺序解析。
共享对象须等全部成员不再可达才能删除，由元数据后端维护持久回收状态。
注册预留先于 PUT，元数据发布必须原子消费该预留，GC 已认领的成员不能迟到发布。
每个成员保存原有 framed 编码，索引和成员均有 SHA-256；旧 chunks/chunks-v2
对象保留读取回退。新对象位于 chunks-coalesced-v1。

初版范围为 flat + Redis/SQL + UploadBeforeCommit；native-v2、packed、
etcd/TiKV 和 CommitBeforeUpload 的独立发布/回收协议目前未覆盖，不能把结果推广
到它们。SQL 的专项验证使用 SQLite，真实性能与 SIGKILL 验证使用 Redis/RustFS。
候选默认值为 65536 个未压缩字节：16 个 4 KiB 写可组成
一个对象，理论上约 16384 PUT/GiB，给 10× 指标留出低流量尾组余量；最终默认值
仍须经最终 release 复测与完整 CI 后才能接受。v1 主 A/B 的实际平均 PUT 为
66.25 KB、约 16,385 PUT/GiB，三个配对均满足对象数和尾延迟要求，为该默认值提供
初步依据。本次未做多窗口寻优，不声称
64 KiB 是所有负载的最优值。0 字节关闭新聚合，但新对象始终可读。
强压缩数据或低流量超时尾组的物理对象可能小于 40 KB；本次硬指标限定于冻结协议
中的不可压缩 4 KiB 随机写，不将样本平均值扩展为每个对象的最小尺寸保证。

BREWFS_WRITE_COALESCE_MS=0 表示不指定额外的时间窗口；为满足“不能无限推迟”，
实现仍保留强制 100 ms 队列安全期限。显式非零值替换该期限，上限 5000 ms。
每组最多 32 个成员；额外编码/组装内存以 8 MiB semaphore 预算约束，索引缓存
最多 4096 组。顺序大 slice 不进入聚合。上传沿用原有全局/每文件并发限制。
连续写仍由既有 slice 缓冲合并；聚合器接收冻结后的 Bytes 页引用，每个成员沿用
原有 framed 编码，最终对象组装复制一次，不读取空洞或反复拼接整个待聚合缓冲。
逻辑 slice 数量未减少，跨文件或随机稀疏范围共享的是物理对象。
既有 upload batch/partial-tail 计数描述逻辑 slice，仍可能显示单块尾批；物理聚合
收益使用对象层 PUT 次数和字节核算，不能把逻辑 batch 数误当作新物理对象数。

兼容方向：新二进制能读取旧 chunks/chunks-v2 对象；关闭聚合仍能读新共享对象。
**旧二进制不识别新共享对象，不能在写入新格式后直接降级二进制。**
新 reader 冷读一个组时至多获取 4 MiB，缓存索引后只读对应成员。此初版可能增加
冷随机读的取数放大，须结合 broad guard 评估，不能只从 PUT 数量推断读性能。

新逻辑 ID 使用 [2^52, 2^53) 的保留空间，保持 Redis/Lua 数字精确。普通分配器
若触及保留空间则明确拒绝新写，避免碰撞；旧高 ID 对象仍可读取。组 seed 必须
小于 2^47，超过时不再创建新聚合。SQL 重启恢复不能把编码后的 ID 当作普通计数器。

## 验收状态

09:03 Asia/Shanghai 首次完整 workspace 测试结束：**1314 passed / 1 failed /
264 既有 ignored**。失败为
vfs::fs::tests::io_tests::test_fs_regression_fs_ops_pwrite_slice_relative_upload_offset，
报错 `coalesced slice has only one block`。该测试的 VFS 块大小为 64 字节，而
对象存储配置沿用默认 4 MiB；原聚合资格仅检查后者，错误地接纳跨 VFS 多块的 slice。

修复在入口增加 `s.data.len() <= s.data.block_size()` 限制，使这种 slice 回到
既有多块上传路径。保留原测试和所有断言，未新增 ignore。失败日志及第一次 CI
源码快照完整保存在 validation/full-ci-attempt1/。第二次完整 CI 于 09:04 开始。

此修复改变生产源码，因此现有 release SHA 与最终源码不再匹配。下表只记录
候选 v1 的观测值；最终版本还需要新 release 的同协议性能、FUSE 和崩溃验证，
不能因为修复只增加一条资格检查就免除重测。

| 硬指标 | 本次结果 | 状态 |
|---|---|---|
| PUT/GiB ≤ 26000 | v1 为 16385.24，降低 15.96× | v1 数值通过；最终版本未测 |
| 平均 PUT ≥ 40 KB | v1 为 66.25 KB | v1 数值通过；最终版本未测 |
| randwrite 配对 95% CI 不含 0 | v1 为 [+2.3940, +2.8497] MiB/s | v1 数值通过；最终版本未测 |
| p99 不恶化超过 10% | v1 为 −57.56% | v1 数值通过；最终版本未测 |
| 内容逐块 SHA-256 | v1 release 和最终 debug 均逐块零错误 | 最终源码正确性通过 |
| fsync / close 后 kill -9 | 最终 debug 两场景各全量 8192 块正确 | 最终源码正确性通过 |
| seqwrite ≥ −3% | v1 中位数 −0.44% | v1 数值通过；最终版本未测 |
| randrw 无实质回归 | v1 带宽 +20.63%，读写 p99 均下降 | v1 数值通过；最终版本未测 |
| 完整 workspace / overlay / clippy | workspace 通过；overlay 被 watchdog 终止后重跑；最终 clippy 未执行 | 未通过验收 |
| broad compose 及元数据回归 | 最终源码尚无新 release；未执行 | 未通过验收 |

主 A/B 使用 compose 管理隔离的 Redis/RustFS，并复用基线 fio、采样、严格 drain
代码；broad guard 使用仓库实际 run_perf_in_container.sh。protocol.json 冻结完整
环境、缓存、压缩、并发、种子和统计口径。两对预热后固定 off/on 顺序，各三对，
未将预热或 direct guard 当作独立正式配对；有限样本和固定顺序仍存在统计限制。
baseline 构建使用 2 jobs、候选使用 4 jobs，代码生成参数一致，只有编译调度不同。

v1 提前执行的严格 overlay clippy 已通过（项目零 warning）；工具链仍报告 30 条既有
依赖诊断及一条 future-compatibility 提示，不能写作整个依赖树零诊断。

## 最终本地门禁与明确差距

| 检查 | 最终源码结果 |
|---|---|
| fmt | 通过 |
| workspace check / build | 通过 |
| tokio / io-uring 配置检查 | 通过 |
| rfuse3 三配置 | 不适用；该包不在 workspace，按条件门禁跳过 |
| workspace `--lib --bins` | 1315 + 2 passed，0 failed，264 既有 ignored |
| `--lib --features workspace` | 编译被 watchdog SIGTERM；正在 1 job 重试，尚无测试结果 |
| 最终 workspace / overlay clippy `-D warnings` | 待 overlay 重跑后串行执行；v1 的提前 clippy 结果不冒充最终结果 |
| 四个 runner 的 bash 语法 | 通过 |
| perf-report-delta / JuiceFS direct matrix / JuiceFS report | 三项通过 |
| Redis 注册/发布/回收故障测试 | 最终源码重跑通过 |
| diff / cached diff 检查 | 通过 |
| 最终 debug 的 FUSE 内容 / SIGKILL 恢复 | 两场景通过，块哈希和卸载完整验证 |

watchdog 的原始记录：`2026-11-02T01:34:04Z avail=1775MiB < 3072 -> killing rustc`。
overlay Cargo 退出 101，rustc 收到 SIGTERM；测试尚未开始，不能记作 0 failed 的通过。
完整失败记录保存在 validation/overlay-watchdog-attempt1/。没有关闭 watchdog、
修改 Rust 代码、删断言或增加 ignore；重试仅将 CARGO_BUILD_JOBS 从 2 改为 1，
其余 features、codegen profile 和测试选择不变。没有用 timeout 包 cargo build。

截至本报告交付，最终 release 的 randwrite、seqwrite、randrw 正式配对均为
**0/3 对**，最终 release 的 direct guard 为 **0/3 场景**，broad compose 的
off/on 为 **0/2 arm**；overlay 完整测试和最终两项 clippy 也没有通过结果。
v1 数值达到目标，但 v1 未通过完整正确性门禁；最终源码虽修复了该问题，不能把
旧性能结果转移给新二进制。因此 performance_acceptance_passed=false，PR 保持 draft。

90 分钟基线节点、3 小时 A/B 节点均未按时完成。新基线 release 构建耗时
108m 06s，v1 release 耗时 54m 13s；在五小时时限内交付代码、未验收数据与明确
差距，没有把未完成门禁写成全绿。详细计时均在 build/ 和 validation/ 原始记录。

后台串行重试位于 tmux `coalesce-gates`，入口 validation/resume-ci.sh；日志为
validation/test-overlay.log，随后继续最终 clippy 和收尾检查。编译期间仍保留
内存 watchdog。任务专用 compose 的清理在这些后台检查结束后执行，并保留所有
基线、失败和已测产物。报告状态是交付快照，后续日志不自动等于 PR 已验收。

</details>
