# Packed-v3 接手审计：当前进度与 SPEC 差距（2026-10-03）

2026-10-08 收尾复测 diagnostic05 已终态：8项全部执行，5通过、3失败；
source/config/Git不变，自有服务和代理清理及独立absence检查通过。
Redis fork/initial composer/取消后恢复与TiKV两项postsubmission故障通过。
剩余为TiKV fork在Quiesced前Busy，以及Redis/TiKV新borrowed fork首次pin快照Fenced。
对象runtime/admin凭据分离12文件及absence修正已精确接入，尚未编译/真实权限验证。
生产CLI6、operator/中央GC及完整同迭代门禁仍开放，全部SPEC目标继续active。

2026-10-08 最新终态：仅packed-v3；stores05为328通过、0失败、101 ignored；
SDK03为122通过、0失败、1 ignored，均source/Git不变。
同源真实diagnostic04的46项全部执行：38通过、8失败、0未执行，source/config/Git不变，
自有Redis/PD/TiKV与代理清理及独立absence核验通过。original shutdown18/0、route2/0；
fork0/2、initial composer1/1、headless0/2、native resume11/1、specialized auth0/2。
BuildingEmpty已通过；native失败改为Redis readback-cancelled的journal_not_future守卫。
fork缺失目录修复与initial/native时钟固定阶段诊断已在终态后接入，相关hot lease/CONTROL
hydration及故障测试契约修复集中准备中，未签新一轮验证。下文保留各历史范围。

2026-10-08 最新增量：仅packed-v3；普通stores04终态为326通过、0失败、99 ignored。
同源真实Redis/TiKV diagnostic03矩阵44项全部执行，28通过、16失败、0未执行；
source/config/Git不变，自有服务与代理已清理并独立确认消失。阶段为fork0/2、
bootstrap2/0、initial composer2/0、clean-source负例4/0、original shutdown8/10、
headless0/2、headless route1/1、native resume11/1；Redis BuildingEmpty在after-page
authority失败，旧native12/0只保留历史范围。原attempt12的64项通过仍不替代新生命周期出口。
TiKV透明Lock probe实际0/1，七事件中三次明确返回
非空retryable+WriteConflict，并非空KeyError；源和库测试binary不变且独立清理通过。
完整PWA handoff packet、ordinary journal同holder夹具、native-rebind共享预算及
hot-head/PWA scoped discovery修正的stores03已终态：323通过、0失败、99 ignored，
source/Git不变；原28个普通失败（含26项root故障控制）均通过。其后followup04精确接入
严格TiKV冲突处理、typed fork mount/clean publication及两项真实discovery竞态回归，
原三处格式修正已完成。SDK01仅编译类型错误、0测试；借用Key转换修正后SDK02已终态
119通过、0失败、1 ignored，source/Git不变；stores04已完成上述326/0。
reason04已确认三次冲突的key/primary/TS全匹配且含四类ExecDetailsV2；该测试仍0/1。
metadata-followup05六文件已精确接入且fmt/diff通过，尚未编译/真实验收。
详见[当前元数据增量](../../performance/packed-v3-metadata-followup-validation-2026-10-08.md)。
FUSE后端重验待执行；operator中央leader GC仍是独立生产缺口。
真实生产CLI/FUSE/operator及完整门禁未通过，全部SPEC及S/X仍开放。
最新计数与边界见[carrier/runtime验证](../../performance/packed-v3-carrier-fork-bootstrap-validation-2026-10-08.md)。

2026-10-08 当前收尾重点是 Redis/TiKV 的发布、恢复、reader pins 和 GC。
此前真实后端 26 阶段的 64 项测试全部执行，56 通过、8 失败；失败均遇到
TiKV Get 锁错误，部分尚未到达目标故障注入断言，不能签收这些故障语义。
严格本地锁分类和固定 data deadline 已接入，SDK 普通测试 113 通过、0 失败、1 ignored。
同事务、有请求额度和截止时间限制的 point-read batch 已接入，16 项测试全通过；
旧夹具修正已通过。initial1 删除回收及3项精确修正后，完整 stores 回归为
289 通过、0 失败、73 ignored，source/Git 不变。真实 focused19 曾复现未分类锁错误；
受控 Get 观测确认的附加耗时字段已严格校验并通过 SDK 回归，真实 focused25 已
25 通过、0 失败、0 未执行；source/config/Git 不变，自有服务及代理独立确认已清理。
原26阶段64项 attempt12 已终态 exit0：64通过、0失败、0未执行；每阶段源码、
配置和 Git 不变，自有 Redis/PD/TiKV 与代理独立确认已清理，所有日志逐项绑定。
最新证据和边界见[元数据收尾记录](../../performance/packed-v3-metadata-closeout-validation-2026-10-07.md)。
产品、API、CLI 和实验名称统一使用 packed-v3；下述条目保留各自历史验证范围。

当前 Redis/TiKV 收尾清单（覆盖下方历史审计表的当前状态）：

| 必需契约 | 当前代码与证据 | 剩余出口 |
| --- | --- | --- |
| 固定 TS 的有限元数据读取 | 严格 Get 锁认证、固定 data deadline、共享请求额度和两次 continuation 已接入；SDK 最新 113/0，BrewFS batch16、全 stores 289/0、真实 focused25 与完整 attempt12 的64项已通过 | 当前源码完整仓库门禁 |
| 原子发布与恢复：G12 | 实际 VFS native capture、持久 journal、依赖图审计、carrier/head/PWB3/registry/native holds 同 Redis/TiKV CAS 已接入；原26阶段64项修复后完整重跑全通过 | 新 carrier/fork/admin 接线尚待集成及实际验证 |
| Pins、共享引用和回收：G13 | 持久 reader pins、canonical native holds、registry memberships、grace、后续 history 退休已接入；Deleting-only initial1 全 stores 289/0；真实 reaper4 与 initial adoption→删除→grace→对象及 native GC2 已通过 | 新 fork alias 的逻辑退休和 source 物理保留候选尚待集成与实际验证 |
| Packed workspace 完整生命周期：S4/S6/S7 | CLI 有 Redis/TiKV mount 接线；开发树仅在consistent backend与canonical预算具备时报告mount capability，尚未验收 | 公开管理接口、准确 carrier revision、fork 引用认证及 alias 退休、真实 mounted fork→mutation→seal→remount→GC与故障恢复验收 |
| Operator：G14 | 能力/status/CRD 基础已有；实际 shutdown 会等待 mount/worker、drain，再释放 lease | 原 mount 的 clean drain 与 Released 同 CAS 的持久证明、packed facade/consumer、真实 operator 生命周期；Expired 不能当 clean release |

系统 S 与主实验 X 尚未通过。三创新实验在系统完成后冻结；旧表和历史 gate
只说明对应的源码与验证范围，不能作为当前树或全 SPEC 的签收。

2026-10-07 最新签收子项为 G12 物理依赖审计，见
[physical dependency validation](../../performance/packed-v3-physical-dependency-validation-2026-10-07.md)。
bounded HEAD、GC05/LD05 完整物理认证、SQLite 库存及七根/全部子页遍历已实现。
gate13 实际结束 48/49、564 输入不变，唯一严格 Clippy 的两处问题已修；独立复核
另发现 SQLx 查询取消后预算提前释放，三个真实后台 barrier RED 均复现。修复后的
publication 模块 37 通过/0 失败/1 ignored。gate14 实际结束 48/49、564 输入/config/Git
不变，仅新测试 late-init 写法被严格 Clippy 拒绝；保持断言与生产逻辑的修正已应用，
gate15 完整门禁实际 exit0：49/49、564 输入/config/Git 不变，stores108、default1169、
overlay1830+fixture8、all-features library1845、operator28 均零失败；严格 Clippy 与
CRD 生成/diff 通过。随后同源真实 RustFS 显式执行1项测试通过：12对象/55228字节，
48 HTTP attempts、24 ranges、最大4096字节，HEAD零payload、三账本守恒及本地/远端
库存一致；missing cold/GC suffix均拒绝，自有bucket/容器/进程组清理确认。
该审计不能构造完整发布 authority；parent context/namespace/codec/External digest、
journal/原子旋转/pins/packed GC 仍开放，不关闭完整 G12/G13 或全部 SPEC/S/X。
下一批每入边context/semantic facts的具体SQL schema/API设计及非最大路径weight测试
候选仅在仓库外准备，未应用/运行，不把设计或DDL检查记为完整graph proof。

2026-10-07 当前源码批次见
[G12/G13 catalog safety](../../performance/packed-v3-g12-g13-catalog-safety-validation-2026-10-07.md)。
G10旧successor02反向应用无需恢复；现v3 bounded-open/attach及G11/G13保留。
新publication retry、首次扩张inode预留检查、PWB3根代数与finalize预扫/层集合代数
正在同源完整门禁验证；开发期117项stores中105通过、3个新增finalize竞态按预期RED、
9个真实服务用例ignored。修复后同源完整gate的stores阶段108通过/0失败/9ignored，
gate11最终45/49、557源码不变；三处Clippy与实际operator CRD生成panic已修复，
schema新测试已证明RED→2项GREEN，生成manifest已核对更新。gate12最终实际exit0：
49/49、558源码/config不变，stores108通过/0失败/12ignored，default1164，
overlay1787+fixture8，all-features library1802，operator28，均0失败。
随后同源真实Redis4filters/8tests实际通过（含新增3个G12发布/allocator用例），
自有容器和进程组清理已核对；不推导G13真实竞态、TiKV/Kubernetes或journal完成。
G12完整依赖图与packed journal/旋转，
G13持久pins/history退休/对象图GC，以及全部SPEC/S/X仍开放。

2026-10-04 用户已收敛为仅 v3，不需要 v2 等兼容。下表 G01–G17 按 v3/005 和三创新
关联 workspace/operator 契约推进；历史五份SPEC独立出口、v2产品及旧wire/flat/CR兼容
不再必需。当前范围以[全部SPEC清单](2026-10-04-brewfs-all-spec-completion.md)为准。

2026-10-04后续复核与系统验收出口见
[系统就绪清单](2026-10-04-brewfs-system-readiness.md)。首次复核的source/binary与10月3日
最终gate一致；后续代码按各自冻结checkpoint核对，历史正确性结果保持原范围。最新源
库存批次47项gate02与36k raw/zstd分页/重启已通过，完整typed观测/eviction、预算与
workspace/operator出口仍开放。核心系统与主实验进入条件均未通过。

## 目标与审计口径

按用户当前目标，先核对实现、规范和证据，再设计 readonly-packed metadata、dynamic
data blocks、overlay workspace 三个创新点的实验。本文是完成清单，不把设计或已有
接口记为端到端实现。对应实验见
[三创新实验方案](2026-10-03-brewfs-three-innovations-experiment-plan.md)。

主规范为 [packed-v3 SPEC](../specs/2026-09-27-brewfs-packed-metadata-v3-readonly-smallfiles.md)，
关联large-directory、workspace/operator的v3完成契约也在审计范围。独立v2产品与
旧wire/bincode/CR兼容不再是出口；历史版本记录不能约束当前v3实现。

状态分为：代码存在、库回归、真实 FUSE 验证、生命周期验收、匹配性能验收。
缺少最后一层不否认前面已完成；前面的通过也不能推导后面通过。不按行数给完成百分比。

## 实际仓库与证据基线

- 指定仓库 `/home/hxy/brewfs`，分支 `codex/packed-metadata-aliyun-20260930`。
- 接手 HEAD `a429b0e1bc1c158af06ecf738e552062123d6e00`，不是最初粘贴文本中的 `3a1d43d`。
- 24 个已跟踪文件有既有修改，另有 005 模块、vendor、runner、性能记录等未跟踪文件；
  `git diff --stat` 不含这些未跟踪实现，不能用它估算全部工作量。
- `.claude/`、生成物、缓存和凭据不属于交付源码。保留既有工作树；审计不重置或清理它。
- 初始 status、源文件哈希与诊断保存在
  `docker/compose-xfstests/artifacts/packed-v3-takeover-20261003/`。
- 审计开始时继承的最新完整本地 gate 是
  `packed-v3-completion-20261003-cold-hardlink-gate/`：workspace lib 1,015 passed/225 ignored，
  brewfs bin 1,099 passed/225 ignored，packed 105 passed，overlay 262 passed/2 ignored，
  vendor 13 passed；fmt/scripts/check/build/runtime/clippy/diff 均记录通过。
  这是历史 worktree gate，不是本轮重新执行完整 CI，也不是 all-feature/operator/`-D warnings` 通过。
- 005 的 10k 完整回读证据为 `packed-local-20261002T133706Z-1359072/`；
  cold/hardlink 实挂载为 `packed-local-20261003T021043Z-1555949/`。
  原始目录在本机存在；分别适用其阶段，不能假设所有后来修改都在 10k 上重新验证过。

更多阶段证据见 [wire/producer/FUSE checkpoint](../../performance/packed-v3-completion-correctness-2026-10-02.md)
和 [cold/hardlink checkpoint](../../performance/packed-v3-cold-hardlink-validation-2026-10-03.md)。

## 后续代码完善与小规模验证（同日）

用户随后要求按差距继续完善代码并进行小规模验证。本批新增 005 专用 GM07/IL05
语义校验、有界 Linux regular-file 真实捕获和 fixture `--source-file` 接线；实挂载
发现并修复 raw-name open 的 String 反向路径权限检查问题。G01 已补齐；G02–G04
仅推进单文件源语义、真实稀疏、binary xattr、raw filename 和祖先 DAC 的具体子项。

最新验证及源码身份见
[source correctness checkpoint](../../performance/packed-v3-source-validation-2026-10-03.md)。
真实源 raw/zstd 各5例和 RustFS 100/1,000 文件 full 回读均零错误并完成清理；最终
同迭代完整门禁状态以该 checkpoint 与 `packed-v3-completion-20261003-source/final-gate/`
为准。未提交继承修改保持原样；不将 debug 正确性换算成性能接受。

## 已经完成的能力及适用边界

2026-10-04 source-stat批次已通过40项最终本地gate与10次真实源FUSE；PM08认证根属性
及SI05 allocated blocks不再仅在provenance中保存，见
[source-stat checkpoint](../../performance/packed-v3-source-stat-validation-2026-10-04.md)。
下表G02/G03仅关闭这一子项，完整namespace/POSIX/external-large仍开放。全部SPEC目标
及v2/operator独立出口见[扩展清单](2026-10-04-brewfs-all-spec-completion.md)。

| 能力 | 当前证据 | 尚不能据此宣称的能力 |
| --- | --- | --- |
| 历史004与250µs demand coordinator（移出范围） | 历史格式/目录/范围/缓存扫描记录；当前readonly入口拒绝004 | 250µs优于1ms；005也有同样coordinator；旧回读属于当前完成条件 |
| PM07 / IP05 / FD05 / GC05 / GM07 | manifest 信任锚、分页认证、独立 raw/zstd、restart、stream/upload/cancel 回归 | 完整 source snapshot 或可恢复 workspace 原子发布 |
| 流式 producer / 私有磁盘排序 | 有界 group 输入、SQLite spool、CAS 对象和 upload verify | 真实源遍历、源一致性、外部大文件、全部 inode 属性 |
| 有界 Linux 源捕获 | PM08 namespace与PM09 external；root/blocks/raw names/specials/alias策略及byte xattrs/源.stats；最新40项gate、6项CLI、raw/zstd external和旧namespace四次FUSE通过 | 原子frozen-view协议、POSIX ACL/其他raw mutation/PATH_MAX与mutable packed生命周期 |
| 005 readonly VFS | prepared unified plan 直接接入 reader；100/1k/10k 阶段性完整字节回读 | native/packed 四格同 executor 或 mutable packed lower |
| cold attrs / hardlinks | raw readlink、byte xattr names/binary values、EROFS、共享 inode/nlink、反向分页及显式子树alias策略与实挂载 | POSIX ACL 权限应用/继承；其他raw namespace mutation边界 |
| `.stats` | runtime backend range/requested/received/failure/logical bytes | 初始 probe/manifest、SDK attempts、全部类型分解、完整 raw overscan |
| workspace-v1 原有生命周期 | native delta、lease、seal journal、recovery/GC 原有实现 | 005 binding、manifest refs、共享对象图已纳入这些机制 |
| 历史 JuiceFS 对照 | 10k、ordered/shuffled 1M 的已有记录和原始产物 | 新 005 性能接受、对所有 workload 胜出或 TTL 已匹配 |

## SPEC 差距清单

优先级：阻断正确性/可信验收为 A，阻断完整生命周期为 B，阻断因果实验为 C。
每项必须有实际测试/产物再关闭，不能通过移除 SPEC 的未完成标记关闭。

| ID / 阶段 | 优先级与缺口 | 代码入口 / 现状依据 | 完成条件 |
| --- | --- | --- | --- |
| G01 / P1 | 已补齐：005 负向语义校验 | GM07 writer/decoder、IL05共用kind/mode/rdev/identity校验；当前PM10/IP06明确拒绝旧格式 | 行为red/green及完整gate见各checkpoint；后续只验收v3，不要求旧004回归 |
| G02 / P1 | rooted/frozen目录source子项已签收 | 同root FD no-follow按组件inventory/stat/cold/payload；真实Btrfs readonly snapshot lease及最终source guard；46项gate、8次FUSE、3项真实Btrfs与identity/cleanup核对 | best-effort明确非原子；single-file API较弱；workspace publication/recovery仍独立开放；证据见frozen-source checkpoint |
| G03 / P1 | 已签收：source sparse/allocated blocks及external placement | PM09/EX09/PS09/LE09/LD05 producer与同executor reader；required selector闭合、磁盘runs/extent分页、bounded chunks、跨目录hardlink复用；raw/zstd各525-entry真实FUSE | 最新40项gate、6项CLI、>64MiB dense/all-hole、300段sparse、跨chunk/page/EOF/holes、blocks/attrs/EROFS、source/binary身份和正常卸载均通过；不替代G02/G04/G07或workspace发布/GC |
| G04 / P1–P2 | A：readonly ACL/typed errno子项签收，可写原子ACL仍开放 | raw names/DAC/special/hardlink/xattr与Linux readonly ACL通过；45项gate、overlay1,344与8次FUSE回归身份核对见deep-cookie/checkpoint.json | 同版本mode+ACL读取、可写set/remove/chmod/create继承、后端独立实例并发与事务故障；完整POSIX继续逐项验收 |
| G05 / P2 | C：rank/select、SQL seek及有界库存已签收，完整规模出口开放 | source-batch checkpoint02核对47项gate、14次固定binary挂载、新库3项Btrfs；36k raw/zstd各72groups完整导入/分页cookie/active fd/fresh restart/20秒卸载通过；旧失败日志保留但临时数据库已缺失 | 实际typed GET图、物理eviction/refetch、更大规模/hot protection与完整内存证据；原20秒teardown根因仍开放 |
| G06 / P0 | A：完整传输与流量分类（HTTP status-drop terminal 子契约已补） | observer 已对 backend/validated/logical/http-attempt/semantic-validation 维持 terminal 守恒；非2xx body 在 EOF 前被 SDK/调用方丢弃时按 HttpStatus failed 记账并保留实际 received bytes（focused GREEN 见 2026-10-06 evidence）；完整 startup/类型/retry 请求图、多路径 raw/decoded 仍开放 | probe/manifest/IP06/GM07/inline/FD/CA/payload/failed/SDK retry 分类；terminal 守恒、实际 received、raw decoded/union 放大；logical 只在成功交付边界 commit；本次只闭合 HTTP status-drop 子项 |
| G07 / P3 | A：mount-wide 全生命周期预算（V3 admission shutdown 子契约已补） | V3MountBudget 已覆盖多池 owned permits；本轮验证 Stored/Raw 阻塞 admission 在 close 时唤醒、无 partial charge（1 passed） | 跨 read/queue/pin/stored/raw/decoder/output 持有至最后 consumer 释放；取消/失败/慢 consumer/eviction/shutdown 回收；不会互等死锁；v3 关联 native 与真实 FUSE/RSS 仍开放；004 兼容不属于出口 |
| G08 / P3 | C：native/packed 统一实验执行路径（same-snapshot unified executor 子契约已补） | NativePackedPlacementProvider 与 PackedV3ReadonlyMeta 共享 UnifiedReadPlan/Fetcher；同 snapshot 计划/字节回读与真实 adapter read 各 1 passed | native与packed metadata可引用同一个immutable placement，并进入同executor；完整四格、真实FUSE/外部后端和实验矩阵仍开放 |
| G09 / P4 | C：005跨请求pipeline（coordinator shutdown/JoinError 子契约已补） | V3 coordinator 已有共享有界flight、同帧唯一物理fetch、取消和慢消费者背压；本轮补真实 worker handle 关闭等待、取消后恢复 join 与 JoinError 留存测试（15 passed） | 005接共享有界singleflight/coalescing、signal、取消/卸载；支持profile矩阵和同frame唯一fetch验证；仍缺完整生产/FUSE 与 G08 统一路径 |
| G10 / P5 | B：binding/open/attach基础已实现，完整生命周期开放 | PWB3独立版本化manifest、SQLite/KV初次install、catalog authority与缺失binding拒绝、workspace-scoped bounded open；已有lower fallback/Absent与explicit Hole库回归 | 全量native/packed namespace、hot/cold与full/partial覆盖实挂载；fork→mutation→seal→remount及真实Redis/TiKV组合验收；不把基础attach当完整发布 |
| G11 / P5 | A：可见sequence与typed stale子契约已实现 | ReadGeneration新增workspace_mutation_sequence；旧004 helper与当前005 `FetchedSources`均在计划执行前后绑定generation，跨代映射typed `ReadViewChanged`/`StaleView`，readonly FUSE adapter保留retryable error | 真实并发FUSE与native/packed生命周期验证有限整体retry、输出全丢弃且无混合视图；完整S/X仍开放 |
| G12 / P5 | B：same-head原子发布与精确重试已实现，seal/recovery开放 | SQLite事务/KV timed CAS同时更新PWB3 history/current、epoch/sequence/allocator；本轮补提交后重试与首次inode扩张碰撞检查；producer依赖先行、readback与manifest-last已有 | 全部IP06/FD/cold/container/external图证明、durable candidate journal与staging roots、effective view捕获及head/base旋转原子提交、崩溃/reopen恢复；fsync不隐式repack |
| G13 / P5 | B：native层PWB根保护已实现，packed对象图GC开放 | 既有grace/fork phantom回归；本轮新增PWB root generation、SQLite finalize同事务复检、KV扫描前Deleting与层集合generation，含byte-identical ABA RED→GREEN；真实G13竞态后端仍待验证 | durable reader pins、受保护history退休、snapshots/workspaces/leases/journals/staging roots闭合；005 container/index/cold/descriptor/external共享对象mark/sweep与上传orphan回收；无误删 |
| G14 / P5 | B：能力/status基础与CRD生成修复已有，真实operator生命周期开放 | 显式v3 capability/binding条件，缺失能力不得推导Ready；本轮修实际binding-status结构化schema panic并重生成CRD，GREEN与完整新gate状态见checkpoint | Kubernetes API-server接受新schema/CEL、Redis/TiKV E2E、conditions/finalizer/lease与真实packed binding；Ready不能替代storage验收；旧CR兼容移出范围 |
| G15 / P1,P6 | C：static/dynamic/inline/p90实验控制窄契约已补 | fixture支持005 raw/zstd、static frame、inline-off和带认证训练trace的p90 policy；policy输入有边界与溢出拒绝，runner传递并绑定manifest | offline builder完整分布/provenance；同内容/group/container规则；p90只用训练trace；真实输出统计证明参数生效 |
| G16 / P0,P6 | C：runner与工具契约窄契约已补 | local runner记录toolchain、固定trace控制及durable owned-resource journal；partial scanner有discovery；cloud Python控制面尚非完整已验收dispatch | release构建/新wire/metadata codec/data codec传递、三端同trace；阶段计时与未测字段=null；10k有界真实FUSE、cloud dispatch和paired acceptance仍开放 |
| G17 / P6 | C：新的匹配性能与交付闭合 | 历史1M有TTL/inline口径限制；005 debug/zstd正确性没有raw paired性能对照；代码仍未提交 | 同迭代完整AGENTS gate、消融/生命周期/paired对照；接受/拒绝记录；源码vendor可复现；最后coherent commit/push并更新交接 |

### 审计阶段诊断的精确范围（历史，已被后续修复接续）

`gm07-red.log`：5个定向测试中3通过、2失败。第一组在 `kind=0` 的encode拒绝断言失败，
第二组在 `inode=0` 的encode拒绝断言失败。未知flags、超范围inode、nlink=0、非regular
extents仍缺校验，但该fail-fast运行没有逐项动态证明，不能把它们写成全部已复现。
decoder拒绝也仍待单独red/green验证。完整拟议regressions保存在
`gm07-proposed-regressions.rs.txt`。审计阶段只保留诊断，没有接受修复；当时临时测试已移出源码，
初始文件哈希比较证明原有源码内容恢复。不能把这份失败日志算作现有suite回归通过。

后续代码批次的 `packed-v3-completion-20261003-source/gm07-red.log` 为1 passed/4 failed，
逐项收集并分别证明 encoder/decoder 的错误接受；正式回归已留在源码并修复。
raw-name 实挂载初次失败和修复后的10次通过分别保留，不以覆盖失败日志关闭缺口。

## 补全依赖与每阶段出口

2026-10-04目录checkpoint见[namespace报告](../../performance/packed-v3-namespace-validation-2026-10-04.md)：
7项namespace回归、40项最终checks、raw/zstd各540 entries和正常卸载通过。
只关闭inventory/specials/alias/只读修复子项，G02–G04全项继续开放。
后续[名称边界批次](../../performance/packed-v3-namespace-posix-validation-2026-10-04.md)已签收
raw xattr/.stats/removexattr子项，仍不关闭G04全项。
G03 external已按[实施契约](2026-10-04-packed-v3-external-placement-plan.md)签收，证据见
[external报告](../../performance/packed-v3-external-validation-2026-10-04.md)：40项最终checks、
overlay1,328、6项CLI与4次FUSE通过，源/binary身份与清理核对完成。G04 ACL的named-user
grant/EOPNOTSUPP已实挂载复现，待修复；全SPEC与S/X仍开放。

| 次序 | 必须完成 | 可打开的实验 |
| --- | --- | --- |
| 1 | G01负向语义；G02–G04一致source/POSIX/sparse/large；保留已有认证/stream回归 | tiny correctness；真实source corpus |
| 2 | G06实际流量、G07完整预算、G05分页guard、G15真实控制 | packed内static/dynamic/inline/codec消融 |
| 3 | G08 native/packed同executor桥；G09需要测试的005pipeline | metadata×frame四格；独立pipeline/cache实验 |
| 4 | G10binding/fallback→G11mutation fence→G12publication/recovery→G13GC；G14 operator为全部SPEC必需出口 | 两workspace实挂载→生命周期总成本 |
| 5 | G16可重复runner与G17当前CI/paired acceptance | 10k匹配系统对照；有依据才扩大100k/1M |

完整任务最终出口还包括同一真实source的S0→forkA/B→mutate→seal→S1→remount/next-fork→GC。
只读001/10k扫描和composer单测均不能替代这一出口。阶段2的局部测量可辅助开发；
完整三创新性能主张必须等待阶段4的生命周期正确性，且发布成本计入总wall。

## 本轮验证与环境记录

审计检查日志为 `packed-v3-takeover-20261003/audit-status.txt`，另保存toolchain版本。
首次非login shell误用了系统Cargo 1.75，因edition2024失败；原始失败保存于
`fmt-system-toolchain-failed.log`。使用用户既有Rust工具链后再执行fmt/packed和runner检查，
不改Cargo manifest以迁就错误工具链。最终结果以本轮状态日志及交接补充为准。

审计阶段没有接受Rust行为变更、执行云campaign或重写历史性能表。完整CI历史结果和当时
定向检查分别记录。AGENTS引用的`performance-agent-guide.md`在本checkout缺失；当前有效
门禁以AGENTS和现有CI文件为准，后续补文档链接时保留这个审计事实。
后续代码批次已推进上述修复与小规模验证，最终门禁/剩余差距以 source checkpoint 为准。

## 2026-10-06 delta: G09 wire-005 pipeline narrow contract

005 现已由 `V3DemandCoordinator`/`V3FlightRegistry` 实际接入有界跨请求
singleflight、submitted-demand coalescing、body cancellation 和 shutdown/join
signal。flight key 固定包含 ReadGeneration、attribution、container identity、
profile、frame policy、descriptor identity 及 size-class limits；因此同一认证
frame 在同一 profile 下只产生一个实际 range GET，而 generation/profile/descriptor
变化不会错误复用。新增 profile 矩阵回归覆盖 `RandomSmallFile`、
`SequentialSmallFile`、`Mixed`，并对真实 coordinator backend probe 验证每个
profile 的两次提交只发生一次 GET、两个 waiter 收到同一 `Arc`。

005 pipeline 定向套件现为 **22 passed, 0 failed**；`fmt --check` 与
`git diff --check` 通过。证据见
[G09 pipeline checkpoint](../../performance/packed-v3-g09-pipeline-validation-2026-10-06.md)。
本批只新增测试和证据文档，没有修改 production pipeline code。

这只关闭 G09 的窄 singleflight/profile/cancel/join 子契约。完整 G09 仍需真实
FUSE 跨请求 trace、`.stats` 全请求图、eviction/refetch、cold-pipelined/warm
区分和匹配性能接受；不能用004缓存结果替代。


## 2026-10-06 delta: G12 narrow publication

G12 now has a real same-head versioned PWB3 publication sub-contract in
`PublishPackedLowerBinding`. SQLite uses one transaction and KV uses one
timed CAS to advance history/current, workspace head epoch, writable sequence,
and inode floor under old head/base/binding guards. Four publication-specific
tests cover SQLite/KV success and stale/failure preservation; the
`publication` filter is 13 passed and 0 failed.

This changes the implementation evidence only for same-head publication.
Manifest dependency closure, manifest-last ordering, seal/head-layer rotation,
journal crash recovery and idempotent resume/abort, old-reader pins, packed
object graph GC, operator runtime lifecycle and the S/X exits remain open.
See `doc/performance/packed-v3-g12-publication-validation-2026-10-06.md`.


## 2026-10-06 delta: G06 HTTP terminal classification

The observer now closes one narrow HTTP-attempt accounting hole: when a known
non-2xx response body is retired before EOF by the SDK retry path or caller,
ObservedHttpBody records an HttpStatus terminal failure and keeps the bytes
already received in the failed-byte ledger. A successful-status body dropped
before EOF remains a cancellation. The focused regression passed with
failed=1, cancelled=0, received_failed=10, and conserved counters.

This does not close the G06 request graph, startup/object-class attribution,
full retry accounting, raw/decoded/union amplification, or real FUSE evidence.
See G06 HTTP evidence in the performance directory.

## 2026-10-10 delta: G16 runner module invocation

The bounded Python performance helpers now support both package-qualified invocation from the repository root and direct invocation from tools/perf. tools/perf/__init__.py marks the helper directory as a package; packed_partial_scan.py and its focused tests use relative imports with a direct-execution fallback. The repository-root suite (python3 -m unittest tools.perf.test_packed_local_runner tools.perf.test_packed_run_manifest tools.perf.test_packed_partial_scan tools.perf.test_smallfiles_scan) passes 18 tests, and the direct tools/perf runner-manifest/partial-scan suite passes 9 tests. This closes only the import/reproducibility sub-contract; cloud dispatch, release/toolchain provenance, resource journals, cleanup proof, and G17 paired acceptance remain open.

## 2026-10-10 delta: G16 local toolchain provenance

The packed-v3 local RustFS runner now emits `toolchain.json` and the manifest
requires it for a successful run. The record binds `rustc -Vv`, Cargo version,
host target, binary profile, Git revision, and the SHA-256 of the complete dirty
diff. Malformed or incomplete provenance fails manifest finalization, while
failed runs remain recordable without claiming measurements. The focused Python
runner/manifest suite passes 19 tests (and 7 direct-invocation tests).

This closes only the local build-provenance sub-contract. It does not certify a
release build, cloud dispatch, Redis/TiKV lifecycle, resource journals, cleanup
proof, or G17 paired acceptance.

## 2026-10-10 delta: G16 owned-resource journal

`tools/perf/run_packed_local.sh` now creates a durable
`packed-v3-resource-journal-v1` beside every run manifest and records the owned
Compose project, mount, worker process, and temporary work directory through
init, startup, mount-ready, unmount, service-stop, work-removal, and final
events. `packed_run_manifest.py finalize` validates the journal for both
successful and failed runs; a successful artifact is rejected if any owned
resource remains live, while a failed artifact must still contain a terminal
cleanup decision. The focused resource-journal, manifest, and runner suite
passes 18 tests and shell/Python syntax checks pass.

This closes only the local resource-accounting and cleanup-proof sub-contract.
It does not close cloud dispatch, Redis/TiKV lifecycle, real FUSE failure
recovery, release reproducibility, or G17 paired acceptance.
Evidence: [`packed-v3-g16-resource-journal-validation-2026-10-10.md`](../../performance/packed-v3-g16-resource-journal-validation-2026-10-10.md).

## 2026-10-10 delta: G11 wire-005 generation fence

The production packed-v3/005 path now binds `FetchedSources` to the
manifest-derived `ReadGeneration`. The executor rejects a plan from another
generation before filling the caller buffer, and both the pre/post checks and
the readonly FUSE adapter preserve typed `ReadViewChanged` through
`PackedWireError` and `anyhow`, so retry classification remains available.
The older 004 helper keeps the same fence, but it is not used as v3 evidence.

Formatting, diff checks, and the focused typed retry test pass (`1 passed` with
`CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0`). The broader feature test
binary did not reach a runtime result in this WSL session after two
resource-bounded compile attempts; no full FUSE generation claim is made here.

2026-10-10 incremental: G06 typed ETag HEAD observation is wired through ObjectClient, S3 ObservedHttpClient, and packed-v3 readonly transport; scripted success/403 ledger tests are included. fmt, cargo check --tests, and diff check passed; full focused test-binary codegen was stopped after exceeding the resource window, so its runtime result is not claimed. Remaining startup/retry/union/raw/decoded and FUSE evidence stays open.
