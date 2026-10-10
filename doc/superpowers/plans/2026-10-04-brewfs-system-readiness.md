# BrewFS 三创新系统验收与实验进入条件（2026-10-04）

用户最新范围仅 v3/005；v2独立产品及v1/v2/旧wire/flat/CR兼容移出出口。本文所有
“五份SPEC独立出口”历史要求由[当前v3清单](2026-10-04-brewfs-all-spec-completion.md)
替代，三创新需要的source/POSIX/read/budget/workspace/operator/GC仍全部验收。

**当前结论：未达到完整三创新系统验收条件，也未达到主性能矩阵的进入条件。**

2026-10-08 当前主线为 Redis/TiKV 元数据管理，产品名称仅 packed-v3。
完整真实元数据 attempt12 已通过26阶段/64项，源码、配置与 Git 不变；原子发布、
恢复、真实进程退出、历史回收、pins、有限读取和 no-replay 均已实际执行，
自有服务与代理已独立核对清理。完整 stores 为289通过/0失败。
剩余系统出口是 packed carrier/fork 引用、原 mount clean shutdown、headless snapshot
及其崩溃恢复、first bootstrap、实际 mounted/operator 生命周期和同迭代完整门禁。
这些候选与集成状态以[当前差距清单](2026-10-03-packed-v3-spec-gap-audit.md)和
[元数据验证记录](../../performance/packed-v3-metadata-closeout-validation-2026-10-07.md)为准；
下文表格保留首次审计的代码事实，不能用于描述当前实现或签收未验证出口。

用户要求先确定系统完善，再仔细规划实验。本文把“完善”转成可核验的系统出口，
防止只读的小规模成功被解读成完整 workspace 生命周期完成。已有
[三创新实验方案](2026-10-03-brewfs-three-innovations-experiment-plan.md)保留为候选设计；
系统验收通过后才冻结最终运行清单、样本量和发布结论。当前不启动性能 campaign。

最新用户将“完成所有SPEC”的持续目标收敛为v3；相关operator/large-directory契约
纳入[全部SPEC清单](2026-10-04-brewfs-all-spec-completion.md)。本文保留首次readiness
复核的哈希与范围记录；后续PM08 root/allocated-block代码与验收见
[source-stat checkpoint](../../performance/packed-v3-source-stat-validation-2026-10-04.md)。
后续目录批次已通过40项最终gate与raw/zstd各540 entries真实挂载：
[namespace checkpoint](../../performance/packed-v3-namespace-validation-2026-10-04.md)。
随后byte xattrs/源.stats/removexattr通过独立40项gate与四次真实FUSE，见
[名称边界checkpoint](../../performance/packed-v3-namespace-posix-validation-2026-10-04.md)。
PM09 external随后也已签收：40项最终gate、6项CLI、raw/zstd各525-entry外部大文件挂载
和540-entry兼容回归全部通过，见[external checkpoint](../../performance/packed-v3-external-validation-2026-10-04.md)。
S/X仍未通过，不因这些子项补齐而签收系统。

后续root-FD/Btrfs冻结source、PM10/IP06分页及SQL seek已完成独立46项gate、8次FUSE、
3项真实Btrfs测试；5,474-byte深路径roundtrip和最终guard已验证，source/binary/cleanup
统一核对见[当前checkpoint](../../performance/packed-v3-frozen-source-seek-validation-2026-10-04.md)。
36k导入仍两次600秒超时、未挂载，G05规模出口开放；G06/G07、可写ACL、G08–G17继续
按清单推进，历史S1/S3表记录的是首次复核状态，以当前gap audit及checkpoint为准。

最新[spool/readonly checkpoint](../../performance/packed-v3-spool-readonly-validation-2026-10-04.md)
已核对47项完整gate、8次真实FUSE、3项真实Btrfs及366源码/固定binary身份与cleanup。
第三次36k仍600秒未挂载，库存完整36,003项但编号只完成29,571项，producer尚未开始。
source capture/assignment有界事务为下一批；三次失败都保留。S/X仍未签收。

## 本次复核事实

后续当前源库存checkpoint已通过47项gate02、14次固定binary实挂载身份核对及新库的3项
真实Btrfs重验，见[源库存报告](../../performance/packed-v3-source-batch-validation-2026-10-04.md)。
36k raw/zstd导入/分页/重启/正常卸载已通过；原三次失败临时work在环境恢复时已缺失，
持久诊断仍在。该项只关闭有界库存与此规模正确性；完整typed观测、物理eviction、原卸载
hang、mount预算、mutable packed/lifecycle/operator和S/X仍开放。下文历史事实保留首次
复核范围，不把其source hash表述成当前所有改动均已验收。

仓库 `/home/hxy/brewfs`，分支 `codex/packed-metadata-aliyun-20260930`，HEAD
`a429b0e1bc1c158af06ecf738e552062123d6e00`，005 改动仍未提交。复核依据为实际代码、
[SPEC差距清单](2026-10-03-packed-v3-spec-gap-audit.md)和
[上一轮源码绑定的正确性证据](../../performance/packed-v3-source-validation-2026-10-03.md)。

本次将当前 `src`、Cargo 配置、workflow、vendor 和 perf 工具文件集合及哈希与上一轮
final gate 逐项比较：新增/删除/变更均为空；brewfs 和 fixture 二进制哈希相同。
上一轮32项本地 gate、10次真实源 FUSE、100/1,000文件 RustFS full 回读仍适用于其原有
验证范围；本次没有将它们重新表述成新测试，也没有重复 unchanged-code benchmark。
当前 owned mounts/daemons/test containers/test volumes 和 D-state processes 均为空。

机器可读复核记录为
`docker/compose-xfstests/artifacts/packed-v3-readiness-20261004/readiness.json`；
checkpoint 保存 source/binary hashes、git status 和证据路径。
两个 readiness 布尔值来自人工代码/SPEC审查，文件哈希相同只证明证据身份，不能自动
证明缺失功能存在。本次不修改 Rust 实现，不覆盖继承工作树或旧实验结果。

## 验收范围与两个出口

当前范围包含三个创新点的核心存储系统及关联v3完成出口：readonly packed
metadata、自适应 immutable data placement、可修改 overlay workspace，以及v3 source/
recovery、large-directory、operator lifecycle。G14及005 pipeline均是全部SPEC目标中的
必需能力；即使某个实验格不使用它们，也不能从系统完成清单中省略。
已有 native workspace 的测试不能替代 packed lower 的测试，v3和operator出口
见[全部SPEC清单](2026-10-04-brewfs-all-spec-completion.md)。

| 出口 | 必须证明 | 不作为此前提的事项 |
| --- | --- | --- |
| S：系统完整性 | 一致源→S0→fork A/B→修改A→durable drain→seal/repack→原子发布S1→remount/next-fork→GC；源、权限、数据、隔离、恢复、资源均正确；核对全部v3关联出口 | 是否快于 JuiceFS；所有 workload 都获益；未纳入的可选 p90 训练参数 |
| X：主实验可执行性 | S已通过；native/packed 同 placement/executor；static/dynamic 和 inline-off 参数可证明生效；release/provenance、固定trace、缓存、计数与计时可比较 | 预先获得性能胜出；把 G17 的性能接受结论当作进入实验的前提 |

S和X分别签收，任何未知证据状态记 unverified。通用成功条件是零数据/namespace/权限
错误、零未解释泄漏/挂起、相同源码身份对应完整AGENTS gate、失败恢复具有精确结果。
统计显著性或性能阈值不能弥补任何正确性错误。

## S出口的必需补全项

| 批次 / 对应差距 | 当前代码事实 | 必须增加的验收证据 |
| --- | --- | --- |
| S1 源与POSIX：G02–G04 | PM08有界目录inventory、hot/cold/blocks、raw paths/byte xattrs、special/rdev、visible-links/reject-external及源.stats优先已验证；best-effort明确非原子 | frozen-view协议、PATH_MAX边界、ACL应用/继承、其他raw namespace mutation；继续实际FUSE与mutation拒绝，不能将名称checkpoint写成完整POSIX |
| S2 稀疏/大文件：G03 | 已签收PM09 required selectors、SEEK_DATA/HOLE磁盘runs、分页LE09、bounded LD05/FD05及同executor读取；有界group cap保留，source超限自动external | raw/zstd >64MiB dense/all-hole、300段sparse、跨chunk/frame/page、EOF/holes、source attrs/blocks、hardlink复用、EROFS和正常卸载通过；source/binary与40项gate证据见external checkpoint。G07共享预算和G12/G13发布/GC仍独立开放 |
| S3 读取与预算：G05–G07 | readdir深cookie从起点扫描refs；005预算局限于单次prepare；observing backend只覆盖runtime | 深cookie数量/顺序/GET证据；跨请求queue/pin/stored/raw/decoder/output/cache/慢consumer的全生命周期预算；cancel/failure/eviction/shutdown回收；完整流量和计数守恒 |
| S4 Packed binding/fallback：G10 | `WorkspaceRecord/BaseRevision`仍是native层；extent resolver最终缺口直接变Hole；packed provider只用于readonly | 独立版本化manifest-digest binding；namespace/hot/cold fallback；upper Absent与explicit Hole区分；upper full-cover零lower GET；partial只补缺口；真实A/B挂载隔离 |
| S5 Mutation fence：G11 | `ReadGeneration`含head epoch、mutation sequence和lower digest；005 `FetchedSources`与旧helper均返回typed stale | 同epoch data_version/sequence、head/binding/lease变更可检测；typed stale使整个输出丢弃并有限重新resolve；无混合generation bytes |
| S6 发布与恢复：G12 | producer返回verified manifest ref；原有seal journal只有native层CAS | 一致effective view→依赖对象闭合→manifest→atomic head+binding；每个持久边界注入崩溃；旧或新可见版本精确；幂等remount recovery；fsync不暗中全量repack |
| S7 可达性GC：G13 | `gc.rs`追踪native layer与slice；005对象图未加入 | S0/S1/workspaces/leases/journals/active readers为roots；container/IP05/FD05/CA05/large依赖闭合；grace；reader pin；活跃对象不误删；失败上传orphan可收 |

G01语义校验已完成，本次没有重新打开。G09的005 singleflight/coalescing/cache不是
核心正确性的替代品；全部SPEC目标须补实现和相应预算/取消验证，然后再决定实验行。
不允许用004的coordinator证明005已具备同等能力。

### 最小端到端语料与不可省略的断言

先用总量有界的真实source corpus完成以下验收，不立即扩大云资源。

1. S0至少包含浅/深目录、raw-byte目录/文件名、0B、inline边界、普通file、8MiB sparse、
   all-hole、跨目录hardlink、raw symlink、binary xattrs、权限受限目录。
   >64MiB和>256extents作为独立有界large场景，不能因小语料通过而跳过。
2. immutable输入应来自可冻结源，或明确可证明的一致view协议。普通stat复查只承诺
   检测普通mutation，不称原子目录快照；注入rename/replacement/xattr/content变化须拒绝
   发布混合源版本。对源子树外hardlinks，选择visible nlink策略或拒绝，并记录provenance。
3. fork A/B固定同一个manifest内容digest。A执行create/rename/unlink/link、full/partial
   overwrite、hole、truncate→extend，S0/B及其旧handles逐字节与namespace oracle不变。
4. 对full-cover与partial分别计lower请求；explicit Hole不能fallback，Absent必须fallback。
   upper/hot/cold mutation不能在后续lookup中从lower恢复已删除的数据或属性。
5. resolve/fetch/交付前分别注入同epoch变更和lease/head/binding变化。输出只允许全旧或
   全新一致generation，typed stale不能交付部分成功buffer；retry耗尽有确定错误。
6. durable drain后seal为S1，remount/next-fork逐字节复查；对象上传/verify/index/manifest/
   head+binding各边界崩溃后执行恢复，检查依赖图和版本可见性。恢复重复执行必须幂等。
7. GC前保持S0/B及一个活跃reader，确认对象仍在；释放最后引用与grace后只删除不可达
   对象。失败上传orphan有单独回收证据；不能通过一次目录清理模拟对象图GC。
8. 成功/失败/取消/超时均正常unmount，确认无D-state任务、无预算permit/队列残留、
   无本次owned容器/volume/临时目录。保留失败产物和oracle，禁止覆盖失败证据。

每个 accepted Rust batch 按AGENTS运行完整本地门禁并增加实际受影响features/vendor。
底层read/mutable共享路径改变时，额外执行仓库randrw/direct/writeback/metadata guards。
已知generic/075/iogen01限制保持现有边界，不擅自重新启用或用direct I/O掩盖mmap缺口。

## X出口与实验草案的冻结顺序

| 条件 | 必须有的输出 | 相关差距 |
| --- | --- | --- |
| 相同物理读路径 | 同一D下native/packed引用相同immutable placement与descriptor身份、同executor、同认证要求；两arm真实完整/partial回读 | G08 |
| 实际构建控制 | static1MiB/dynamic size-only、inline-off、独立raw metadata/data codec；实际frame/inline/layout分布和policy provenance证明生效 | G15 |
| 训练参数边界 | 只有纳入p90行才要求训练/test trace隔离、冻结p90与错配测试；不能用test结果反调构建参数 | G15条件扩展 |
| 应用请求独立 | 在被测arm外生成path/operation/range trace；fresh mount开始，不由每个arm的discovery重新生成请求 | G16 |
| 统一环境 | release binary/vendor/source哈希、client+metadata+object server资源、CPU/limits/network/storage、concurrency、cache/TTL/prefetch记录 | G16 |
| 明确计数 | metadata/inline/payload/descriptor/startup/failed/retry的归属；received与raw decoded分开；重复fetch与请求union的raw overfetch可验证 | G06 |
| 全程计时 | source/import/upload/verify、mount/warmup/discovery、active、close/drain/unmount、seal/publish/first-consume/GC与外层wall | G16 |
| 可复现资源管理 | 唯一owned资源域与durable journal、deadline、成功/失败cleanup独立核验；artifact不含凭据；未测字段为null | G16 |

2026-10-10 G16 checkpoint: the local packed-v3 runner now emits and finalizes
a durable owned-resource journal. Manifest finalization validates the terminal
cleanup state for both successful and failed runs, including mount, worker,
Compose, and temporary-directory ownership. This is only the local
cleanup-proof checkpoint; cloud dispatch, real FUSE failure recovery, and
paired performance acceptance remain open.

源码冻结不要求在旧004 magic下扩展字段；root/blocks/large/binding均先给独立payload/
schema版本和明确拒绝历史payload的规则，再接真实源和workspace；不要求旧reader兼容。
G17的coherent delivery/provenance部分属于可复现冻结；paired性能接受部分在实验完成后
判断，不形成“先实验胜出才允许实验”的循环。

S与X通过后，才将现有候选方案具体化为以下最终运行包：

- 主元数据×布局四格，先raw/inline-off与同executor，分离I1/I2和interaction。
- 固定D后的inline、metadata/data codec、可选pipeline/cache分别单因素比较。
- 双workspace正确性oracle已通过后，比较eager/lazy、native lower、packed static/dynamic
  的fork→modify→publish→首次消费全生命周期成本；给出reuse/private bytes及GC成本。
- primary workload、system baselines、局部/完整读、trace seeds、运行顺序、paired次数、
  bootstrap单位、latency/randrw guard、失败/噪声/无收益的报告方式逐项预注册。

这些条目目前仅确定最终规划需要回答的问题，运行格与样本数继续以候选方案为草案。
所有现有debug correctness结果保留原口径，不填入release performance矩阵；未实现格
保持blocked，不能用合成数值、native workspace结果或历史百万扫描替代。

## 本次签收

已完成当前证据身份与代码缺口复核、系统验收条件和实验进入条件整理；**没有签收S或X**。
完整系统实现与验收仍需继续，source root/blocks与有界namespace子项已有上述checkpoint；
继续S1名称/ACL/view边界与S2 external-large，按S1→S2→S3、S4→S5→S6→S7依赖推进。G08/G15/G16可在接口稳定后
准备，但不以实验设施存在为由跳过核心系统验证。
