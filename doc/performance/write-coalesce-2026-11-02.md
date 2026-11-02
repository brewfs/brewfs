# 小随机写对象聚合验收报告

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

初版范围为 flat-v1 + Redis/SQL + UploadBeforeCommit；native-v2、packed-v3、
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
| `--lib --features workspace-overlay` | 编译被 watchdog SIGTERM；正在 1 job 重试，尚无测试结果 |
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
