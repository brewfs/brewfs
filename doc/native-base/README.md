# Native Base 实现跟踪（workspace-native-v2）

依据规格包 `BrewFS_Native_Packed_Base_Specs_v1.2_Final_2026-09-15`（1.2-consolidated），
代码参考 `8ff73d9c18c0f81e88c183383b00d2ba86bd42ba`。实现按
spec15 的 13 组 PR（06 拆 06A/06B，共 14 步）推进，每步工作流：
反例测试 → 最小实现 → focused tests → AGENTS 完整 CI gate →
保存 commit/命令/exit code/raw 日志/fixture → 更新验收矩阵。

## 文件

- [acceptance-matrix.json](acceptance-matrix.json) — 173 项工程验收矩阵的仓库内跟踪副本。
  每项 `implementation_status` 从 `SPECIFIED_NOT_IMPLEMENTED` 起步；每项 PASS 必须附
  `evidence`（真实 commit/命令/exit code/日志路径）。无环境如实标 `NOT_RUN`。
- [implementation-report.md](implementation-report.md) — 按 spec 包模板维护的实现报告。
- [pr06a-handoff.md](pr06a-handoff.md) — PR06A 开发期交接记录（历史交接点
  `e19c990`）；最终实现与证据以本页和 implementation-report 为准。
- `pr01-baseline-audit.md` — PR01 基线调用者与旧 GC delete 入口审计。

## PR 状态

| PR | 范围 | 状态 |
|---|---|---|
| 01 | 基线调用者审计、成本指标盘点、反例测试 | 完成（门禁全绿） |
| 02 | wire3 codec + BNCT 控制记录 v2 骨架 | 完成（门禁全绿, WIRE 10/12 项 PASS） |
| 03 | Pack/Seal reader、view-bound placement、budget/cancel | 完成（门禁全绿, WIRE 12/12 + READ 3 项 PASS） |
| 04 | receipt、按 inode 顺序 commit、dirty 交接 | 完成（门禁全绿, WRITE/KV/ORD 共 12 项 PASS） |
| 05 | ingest、上传验证/断点恢复 | 完成（门禁全绿, INGEST 10/10 项 PASS，上传侧为合同级证据） |
| 06A | PublishedRevision、RetainBatch、seal/fork/recovery | 完成（代码 `ea373a9`；exact-SHA 完整门禁全绿） |
| 06B | close 证书、终结私有 cleaner、旧 GC 隔离 | 组件交付完成（`48966c9`/`ea86664`；门禁全绿） |
| 06C | 空域 Option None 语义（CTRL-003） | 完成（[pr06c-ctrl003.log](logs/pr06c-ctrl003.log)） |
| 06D | close 副本/权威证书 hash 一致性（CTRL-004） | 完成（[pr06d-ctrl004.log](logs/pr06d-ctrl004.log)） |
| 07C | fsync 依赖链不跳过（ORD-005/ORD-008） | 完成（[pr07c-ord005-008.log](logs/pr07c-ord005-008.log)） |
| 07D | 发布/准入门拒绝不可发布配置（WRITE-013/GATE-001/GATE-004/RET-021/CLN-024） | 完成（[pr07d-admission-gates.log](logs/pr07d-admission-gates.log)） |
| 07E | 读取资源边界：unit/plan/取消/预算/64MiB 信封（RES-001…005） | 完成（[pr07e-resource-bounds.log](logs/pr07e-resource-bounds.log)） |
| 07F | ingest 验证证据：composite≠full_hash、resume 变更拒绝（VFY-001/004） | 完成（[pr07f-ingest-verification.log](logs/pr07f-ingest-verification.log)） |
| 07G | 读取摊薄与元数据专用路径：100 并发共享一次 fetch/decode、stat/readdir 不拉 data（OPT-002/OPT-004） | 完成（[pr07g-read-amortization.log](logs/pr07g-read-amortization.log)） |
| 07H | 能力闭包：required_features 必须覆盖内容真实依赖，谎报一律拒绝（GATE-002/GATE-003） | 完成（[pr07h-feature-closure.log](logs/pr07h-feature-closure.log)） |
| 07I | 私有清理判定与记账：ACTIVE 域无候选、冻结库存、丢失回包只释放一次、逐对象核对（CLN-001/004/014/015） | 完成（[pr07i-private-cleanup.log](logs/pr07i-private-cleanup.log)） |
| 07J | 保留/清理局部性：drain 未完成不关闭、alias 删除不撤销旧版本、cleaner 不扫全局历史、证据局部可枚举（CLN-007/RET-002/020/023） | 完成（[pr07j-retention-locality.log](logs/pr07j-retention-locality.log)） |
| 07K | 上传校验能力探测：只回显 metadata hash / 未做 part 校验的后端一律拒绝（VFY-002/VFY-003） | 完成（[pr07k-checksum-capability.log](logs/pr07k-checksum-capability.log)） |
| 07M | 读路径组合与并发身份：真实 packed baseline + loose + hole 逐字节 oracle、namespace 参与身份、并发取消无悬挂引用（READ-001/007/008） | 完成（[pr07m-runtime-read-composition.log](logs/pr07m-runtime-read-composition.log)） |
| 07L | 密封读视图定位与固定：外部索引页按 locator 解析、同一视图不混用 revision（READ-005/READ-006） | 完成（[pr07l-seal-view-pinning.log](logs/pr07l-seal-view-pinning.log)） |
| 07N | 保留闭包与私有配额：manifest 与全部传递容器永久保留、固定只读基线读写计数、alias 删除不扩大候选集、配额 fence 只停新准入（RET-008/RET-011/CLN-017/CLN-021） | 完成（[pr07n-retention-quota.log](logs/pr07n-retention-quota.log)） |
| 07O | 显式 repack 变体记账与 close 证据证书：新增空间单列、未发布变体只清自身未保留输出、C(d) 证据 quota 与“数据不得伪装证据”（CLN-022/CLN-023/CLN-025） | 完成（[pr07o-variant-evidence-close.log](logs/pr07o-variant-evidence-close.log)） |
| 07P | Frozen 元数据：冷属性驱逐后重载一致、COW 页复用与旧快照可读、摘要扫描与重写分别计数（FROZEN-005/FROZEN-006/FROZEN-008） | 完成（[pr07p-frozen-cold-cow-scan.log](logs/pr07p-frozen-cold-cow-scan.log)） |
| 07Q | Frozen 索引三缝：内联→外部 ValueRef 的逐字节搬迁、repack 后稀疏 ordinal/历史 Objects 条目剪除边界、已签发 readdir cookie 的 spool 驱逐与重放（IDX-003/IDX-004/IDX-005） | 完成（[pr07q-index-relocation-prune-cookies.log](logs/pr07q-index-relocation-prune-cookies.log)） |
| 07R | 无名 inode 跨 seal/fork 携带与回包丢失恢复：open-unlink 句柄跨 seal、fork 看不到无名 inode、lost-reply 按 OperationId 恢复唯一 head switch（ORD-009/ORD-010/IDX-006/LIFE-006/WRITE-011） | 完成（[pr07r-orphan-carry-seal-fork.log](logs/pr07r-orphan-carry-seal-fork.log)） |
| 07S | writer lease fence / backend lease time / durability 边界：过期或被顶替的 lease 不产生 guard 且不落部分 metadata、lease 只按 backend 时间判定、四种 profile 报告 confirmed 与 may_be_lost 切分（WRITE-007/KV-004/KV-005） | 完成（[pr07s-lease-fence-durability.log](logs/pr07s-lease-fence-durability.log)） |
| 07T | 上传回执保护、chmod 元数据专用路径与 rename 覆盖的应用完整写语义：上传成功但 KV 失败的对象与 receipts 进入受保护 orphan（WRITE-006）、GiB 文件 chmod 只写属性行且不产生任何数据对象或数据路径行、裸权限位在准入即拒绝（WRITE-010）、rename 覆盖发布源文件完整 extent 集（含 hole）、同事务删除目标全部旧 extent、receipts 覆盖全部被携带 block，缺段/重叠/短覆盖与手写部分替换在提交边界一律拒绝（WRITE-012） | 完成（[pr07t-write-receipts-attributes-replace.log](logs/pr07t-write-receipts-attributes-replace.log)） |
| 07U | 单次请求一次有界且同代的 metadata 捕获：capture 中落入的提交不服务陈旧字节、dirty→committed 交接在提交确认后且回包丢失可重试、一次读跨多 chunk 复用同一 capture、metadata 持续变化/超时只在有界预算内重试（CONS-001/CONS-002/CONS-003/CONS-006） | 完成（[pr07u-capture-consistency.log](logs/pr07u-capture-consistency.log)） |
| 07V | 失效租约与 authority 回滚下的私有状态收口：过期/被顶替的 lease 在注册与提交前 fence 且不写任何行、dispatch 中途失效时保护已落盘块、rollback 停止发布并清空 dirty/pending/inode 本地镜像、运行时 fsync 返回 LeaseFence 且 head 不动（CONS-004）；绕过本地门的另一写者被持久 head guard 拒绝、其上传受保护、重新派生 head 的写者仍可发布（CONS-005） | 完成（[pr07v-fenced-writer-rollback.log](logs/pr07v-fenced-writer-rollback.log)） |
| 07W | 写路径的 copy-up 与持久化验收：GiB 文件首次 4096B 覆盖只读/写它替换的一个块（无前置复制）、跨 native block 的局部写只发布被覆盖两块的 fresh slice 且不重写其余基线、write 返回后的本地读与提交后读到同一字节（交接无 gap）、fsync 后清 cache 重挂（同 store/sink/baseline 新建 runtime）字节与 size 均一致且不新上传（WRITE-001/002/004/005） | 完成（[pr07w-write-path-durability.log](logs/pr07w-write-path-durability.log)） |
| 07X | feature 关闭时的旧路径回归与版本准入拒绝：native 特性关闭配置下旧（flat/chunk）路径的整个 lib 套件在同一提交上通过（934 passed/0 failed/225 ignored），native 开/关两种配置的 check/build 由同一 gate 覆盖（REGRESS-001）；未编译 native 支持的旧二进制与 schema/wire/volume_format 前进的新卷都按名拒绝，且不存在把 native 卷当 flat 卷读的回退（REGRESS-002）；buffered/direct/mmap 三形态在 doc/native-base/known-gaps.md 记录为 KnownGap 而非 PASS（REGRESS-005） | 完成（REGRESS-001/002 PASS；REGRESS-005 记为 KnownGap，状态 NOT_RUN；[pr07x-compat-known-gaps.log](logs/pr07x-compat-known-gaps.log)） |
| 07Y | 逻辑迁移 roundtrip：离线拷贝模式下一次条件事务把源卷全部控制行原样搬到新 volume id 下并安装目标 locator header——源卷逐行逐字节保留、属性与布局在目标侧都有 payload 完全相同的孪生行、从未 initialize 过的目标 reader 读回相同 size 与同样的字节（数据对象按 content-addressed 身份共享，copy 函数不持有 ObjectSink 故结构上不可能上传/重读任何数据字节）；复用源 id、不搬行的模式、已占用目标（locator header 或同形状控制行）与越出被扫描前缀的 store 全部按名拒绝（REGRESS-003） | 完成（[pr07y-logical-migration-roundtrip.log](logs/pr07y-logical-migration-roundtrip.log)） |
| 07Z | lossless repack 保持逻辑身份：把同一逻辑块从旧 Pack 搬到新 Pack 的不同 frame 切分后，Bindings 表（每个 BlockKey 的 decoded_len/content_hash，即该 seal 的逻辑 revision）逐字节不变，而 Placements/Objects/Frames 三张表全部改变（所以它确实是一次 repack 而不是空操作）；两个视图都读出同样字节，repack 既不重读也不改写旧对象，变体按“在未改动的源之上新增空间”记账；反向用例里刻意声明**未篡改**的 binding 却让 placement 解码出不同字节——结构校验与 build 全部通过、每个 frame 都完好，读时仍被“解码块对 binding content_hash 的完整校验”拒绝（OPT-007） | 完成（[pr07z-lossless-repack.log](logs/pr07z-lossless-repack.log)） |
| 08A | 训练模式 sampler hints：新增 `runtime::plan_sample_issue_order`——hints 只能重排“底层请求发出顺序”，sampler 的样本集合与分布（含重复抽样的重数）以及“应用看到的语义顺序”逐项不变；字节区间 hint 必须与某个抽样完全一致（三个坐标差一个即 `HintNotASample`），越界 draw 即 `HintOutOfRange`，因此为顺序 I/O 做优化不可能增删、合并或改写样本集合（OPT-006） | 完成（[pr08a-training-sampler-hints.log](logs/pr08a-training-sampler-hints.log)） |
| 07 | P1 FUSE 接入、初始化命令与运行时准入 | 组件级完成（`762aa76`；[pr07-focused.log](logs/pr07-focused.log)） |
| 08/09 | Frozen reader、固定 revision 零 KV 读取 | 组件级完成（11 项聚焦测试；[pr07b-baseline-overlay.log](logs/pr07b-baseline-overlay.log)） |
| 10/11/12 | 读取 planner、共享缓存 preview、布局变体 | 组件级完成（`762aa76`/`8bcb106`） |
| 13 | 性能实验、能力发布与运维文档 | 未开始（A-F 全部 NOT_RUN） |

当前验收进度（2026-09-19）：173 项矩阵中 **169 PASS**、3
`SPECIFIED_NOT_IMPLEMENTED` 与 1 项 `NOT_RUN`（REGRESS-005 的 buffered/direct/mmap 形态按该项要求记录为
KnownGap，见 [known-gaps.md](known-gaps.md)）（截至本提交）。每个 PASS 都附仓库内命令、
exit code 与日志；缺环境或只做到组件级的项不虚标为完成。队列中的主要工作：
真实 FUSE 挂载 + Redis/TiKV + S3 的 READ/WRITE/fsync 端到端集成、P2 Frozen
COW/驱逐、P3 cache/repack 与性能 A-F，以及 cleanup/retention/ordered-commit
剩余条目。发布/准入类配置拒绝已由 PR07D 收口（WRITE-013/GATE-001/GATE-004/
RET-021/CLN-024）；GATE-002/GATE-003 的 required_features 依赖闭包已由 PR07H 收口
（DataPack scrub 与 Data Seal open 都在任何字节被读出前拒绝谎报的声明）；密封读视图的 locator 解析与固定已由 PR07L 收口（READ-005/READ-006）；Packed+Loose+Hole 组合读、namespace 级身份与并发取消的引用/等待者清理已由 PR07M 收口（READ-001/007/008）；保留闭包（manifest 自身 + 外部 meta 索引页 + 嵌套 inventory 容器）、
固定只读基线的读写计数与私有配额 fence 已由 PR07N 收口（RET-008/RET-011/CLN-017/CLN-021）；
显式 repack 变体的新增空间记账、未发布变体的清理边界与 close 证据证书的 quota/kind
校验已由 PR07O 收口（CLN-022/CLN-023/CLN-025）；Frozen 侧的冷属性驱逐、
COW 页复用与摘要扫描/重写计数已由 PR07P 收口（FROZEN-005/FROZEN-006/FROZEN-008）。writer lease 的 fence 语义、backend 时间判定与 durability 丢失边界已由 PR07S 收口（WRITE-007/KV-004/KV-005）：过期或被顶替的 lease 不签发 commit guard，fenced 写入前后整个卷命名空间快照相同（无部分 metadata），lease 有效性只认 backend 时钟（客户端时钟一律拒绝）、generation 先于时钟判定，四种 durability profile 分别报告 confirmed 与 may_be_lost 且 confirmed 必须是阶段前缀、未知持久化 code 拒绝。

上传回执保护、chmod 与 rename 覆盖的写语义已由 PR07T 收口（WRITE-006/WRITE-010/WRITE-012）：数据对象与 receipts 已上传而 commit 事务失败时，operation 折叠为受保护 orphan receipt 并保持本域注册，cleaner 不能回收、resolve 后恰好释放一次；对 1 GiB 文件的 chmod 只写 attr/ 行且新增对象仅 receipts 控制容器，无文件类型位的裸权限位在准入即拒绝（不占用该 inode 的 mutation_order），内容提交对 attr 行做 check_bytes；rename 覆盖把源文件完整 extent 集（含 Hole）发布为目标内容，同一事务删除目标全部旧 extent，commit 的 durable_receipts 恰好覆盖全部被携带 block，同尺寸覆盖仍是整文件重发与版本 +1，缺段/重叠/短覆盖或手写的部分 ReplaceInode 在提交边界被拒且卷命名空间逐字节不变。

训练模式的 sampler hints 已由 PR08A 收口（OPT-006/INV-02）：新增 `src/native_base/runtime/sampler.rs`（`Sample`/`SampleHint`/`SampleIssuePlan`/`plan_sample_issue_order`）。契约是hints 只能重排“底层请求发出顺序”，其余一律是输入：`semantic_order` 恒为 `0..n`（应用看到的就是 sampler 的顺序），`issue_order` 是它的排列（`covers_every_draw` 在每个用例里都成立），`SampleHint::ByteRange` 必须与某个抽样在三个坐标上完全一致（否则 `HintNotASample`，绝不会被当成一个新样本），`Prioritise`/`SequentialFile` 只改次序且对重复 hint 幂等。验收用例特意用了一个“file 1 逆序抽样 + file 2 同一 range 抽到两次”的批次：`SequentialFile{file:1}` 只把 file 1 占用的槽位按字节序重排（其他文件不动），随后 `applied_samples()` 仍逐项等于 sampler 的 draws、发出与应用的样本多重集合完全相同、`issued_bytes` 不变，重复抽样仍是两个独立槽位（不被合并），越界 draw 与任何坐标对不上的 range 都被拒绝——这正是规范“不得为了顺序 I/O 改变样本集合或分布”，同时保留“应用最终读取顺序与底层请求发出顺序可不同”的自由。

lossless repack 的逻辑身份保持已由 PR07Z 收口（OPT-007/INV-10）：验收把同一个逻辑块从旧 Pack 搬到一个**新** Pack 的不同 frame 切分里（源是单帧单 span，候选是两帧两 span），然后逐表比 digest——Bindings 表（每个 BlockKey 的 `decoded_len`/`content_hash`，也就是这个 seal 的逻辑 revision）在两侧逐字节相同，而 Placements/Objects/Frames 三张表全部不同，seal 对象本身也是新的，所以这确实是一次 repack 而不是空操作；两个视图（源视图读旧对象、候选视图读新对象）读出的字节完全相同，且候选视图只对新 Pack 做按 frame 的 2 次 range 取读、完全不碰旧对象，源视图随后仍能从自己的对象读出同样字节——旧对象既没被重读也没被改写。变体校验器与 seal 结论一致：同一逻辑身份被接受，新空间按“叠加在未改动的源之上”记账（`permanent_bytes == source_bytes + added_bytes`，不报告净回收），新对象进 `added_objects`、旧对象仍在 `permanent_objects`。反向用例正是规范里“不能只比较 frame checksum”那一条：刻意声明**未篡改**的 binding、却把 placement 指向解码出不同字节的 frame——seal 结构校验与 build 全部通过、每个 frame 自身完好，读取时仍然失败，失败点是“解码块对 binding `content_hash` 的完整校验”（`Integrity("block (...) content hash mismatch")`）。

逻辑迁移 roundtrip 已由 PR07Y 收口（REGRESS-003）：新增 `runtime::copy_volume_logically`——离线拷贝模式把源卷的全部控制行在**一个**条件事务里原样重发到目标 volume id 下，并安装目标的 locator header（`Keys::volume_prefix` 成为卷前缀的唯一来源）。验收用真实写入（write+fsync、pending_count 归零）建立含 inode 属性行、extent、head、registry、binding 的源卷，再逐字节断言：源卷在拷贝前后每一行（含 locator header）的 payload 都不变，即 source 保留；源卷每一行在目标前缀下都有一份 payload 完全相同的孪生行，即属性与布局两侧一致；一个**从未 initialize** 过的目标 reader（没有自己的 domain/head）读回相同的 size 与相同的字节，而那些对象就是源卷上传的 content-addressed 对象——copy 函数根本不持有 `ObjectSink`，因此“不上传、不重读、不重校验任何数据字节”是结构性质而不是承诺。反向路径同样按名拒绝：复用源 volume id 会以 `VolumeIdReuse` 先拒（否则目标行会与正在读取的行重叠）、把不搬行的 `NewNamespace` 模式交给拷贝以 `CopyRequiresOfflineCopy` 拒、目标 namespace 已有 locator header 以 `NamespaceExists` 拒、目标 volume id 下已有同形状控制行由事务 `check_absent` 以 `TargetVolumeNotEmpty` 拒且目标前缀行数不变、store 返回越出被扫描前缀的行以 `ScanOutsideVolumePrefix` 失败关闭且不安装目标 header——失败路径不会留下半个目标卷。

feature 关闭时的旧路径回归与版本准入拒绝已由 PR07X 收口（REGRESS-001/REGRESS-002）：把 brewfs 的整个 lib 套件放在 `--no-default-features --features fuse-tokio-runtime`（不含 native 代码的“旧二进制”配置）下运行，同一提交上仍是 934 passed/0 failed/225 ignored，说明 native 特性是纯增量、旧 flat/chunk 路径无回归，且 native 开/关两种配置的 `cargo check` 在同一 CI gate 内各自 exit 0。版本准入侧新增 `src/native_base/runtime/header.rs::tests::an_older_binary_or_a_newer_volume_is_refused_without_a_flat_fallback`：未编译 native 支持的读者拿到 header 行后仍被 `FeatureNotCompiled("native-packed-base")` 拒绝；`schema_version` 与 `wire_major` 各前进一格分别以 `UnsupportedSchemaVersion` / `UnsupportedWireVersion` 按名拒绝；`volume_format` 不是本格式（`workspace-flat-v1`）以 `UnsupportedVolumeFormat` 拒绝——三者都发生在任何字节被读出之前，且没有任何“按 flat 卷解释”的回退；同一记录在能力匹配的二进制下 `validate` 通过，说明拒绝来自读者与版本而不是损坏的 header，并且该测试在 native 开启与关闭两种配置下都通过。buffered/direct/mmap 三种 FUSE 访问形态未在本环境验证（无真实挂载 + xfstests/LTP harness），因此按 REGRESS-005 的要求只在 `doc/native-base/known-gaps.md` 逐形态记录为 KnownGap（表现、为什么不是 PASS、记录位置、关闭条件），矩阵中没有任何一行把它们标成 PASS，REGRESS-005 保持 `NOT_RUN`。

写路径的 copy-up 边界与持久化已由 PR07W 收口（WRITE-001/WRITE-002/WRITE-004/WRITE-005）：1 GiB 稀疏基线（不可变快照）上的首次 4096B 覆盖只从基线读取它替换的那一个块、只上传一个数据对象与其 receipts 容器并只发布一条 extent，文件长度与尾部基线都不动，因此没有前置 copy-up；跨 block 边界的 10B 局部写只读/只写被覆盖的两个 block（fresh slice 各自成 mutation），其余基线既不被读也不被重写；write 返回后 pending_count=1 且本地读立即返回 dirty 字节，fsync 后 pending_count=0 且读回同一字节，两轮交接后前一轮字节仍在，说明 dirty→committed 的交接点就是提交确认点；fsync 后清 cache 并用同一 store/sink/baseline 重建 runtime（重挂类比）时补丁、size 与 truncate 结果逐字节一致，且重挂过程中没有任何新上传。

失效租约与 authority 回滚的私有状态已由 PR07V 收口（CONS-004/CONS-005）：overlay 现在按 WriterLease 发布，租约在 backend 时间过期或被顶替时注册与提交都在事务之前被拒（整个卷命名空间逐字节不变），dispatch 中途失效时已落盘的块被折成一条 step=DataUploaded 的 orphan receipt 精确覆盖，rollback 停止发布（published_rows==0）并清空 pending/dirty 与 inode 本地镜像；authority 把域置为 Quarantined 后同样收口，运行时的 fsync 在租约失效时返回 LeaseFence 且 head/extent 行不动。绕过本地门的另一写者由其不可见的持久 head guard 拒绝：先写者提交推进 head 后，旁路写者的提交报告 stale head guard、head 与 extent 行不变、上传对象受保护，而重新从 store 派生 head 的写者仍能用相同字节提交成功——本地 Mutex 只是同进程优化，跨写者的栅栏是 store 事务里的 head 比较。

读捕获的一致性已由 PR07U 收口（CONS-001/CONS-002/CONS-003/CONS-006）：一次请求先取 boundary 内的 pending 快照、再读已提交视图，并用 inode 行 token 复核（扫描前后逐字节相同），因此既不会把已接受但未提交的尾部读成 0，也不会把一代的 size 与另一代的 extent 混在一起；提交事务落盘但回包丢失时按已记录结果重试，pending overlay 只在确认后释放（字节在交接窗口内不消失）；一次读跨 3 个 chunk/extent 只消耗 1 次 capture 与 1 次 extent 扫描，两个 syscall 明确是两次捕获；metadata 持续变化或超过 attempt_timeout 时只在 max_attempts 内有界重试后以 MetadataUnstable 失败，重试与捕获都在状态门之外进行（并发提交不会被读阻塞）。

open-unlink 无名 inode 的跨 seal/fork 携带与回包丢失恢复已由 PR07R 收口（ORD-009/ORD-010/IDX-006/LIFE-006/WRITE-011）：seal 的新私有 head 携带完整 attrs/extent（不 copy-up、不改写域），fork listing 与 fork view 都不得出现无名 inode 或私有 carry 状态，seal 后立即 fork 可完整读取 Loose+Packed，lost-reply 只能按 OperationId 重放唯一 head switch（换 head/异 payload/陈旧前态/非零 visible_delta_count 一律拒绝）。

Frozen 索引三缝已由 PR07Q 收口（IDX-003/IDX-004/IDX-005）：内联 attrs 搬进外部 ValueRef 必须逐字节等同 canonical bytes（resolved 后 revision digest 不变，非 canonical/缩短/异对象一律拒绝）、repack 后稀疏 ordinal 只剪除本 Pack 死映射而不是重解释旧 slot（pinned 保留、外 Pack 行不删不认领）、readdir cookie 锚定最后返回条目因此 spool 驱逐后可重放且 cookie 表满时先失败不发坏 cookie。

## 环境说明（2026-09-15 探测更新）

开发在 Windows 侧（git 操作、文件编辑），验证全部在 WSL Ubuntu-24.04
（12 核 / 15GB 内存，kernel 6.18.35.2-microsoft-standard-WSL2）执行：

- **cargo 门**：`CARGO_TARGET_DIR=$HOME/brewfs-target-vigorous-solomon`（ext4，
  避开 /mnt/d 的 9p 慢 IO 与 D 盘空间限制）。
- **docker 门**：WSL 内 docker daemon 可用（29.1.3），镜像已缓存
  redis:7-alpine、redis:7.2-alpine、pingcap/tikv:v8.5.0 + pd、rustfs/rustfs:latest
  （S3）、postgres:16、xfstests/pjdfstest/ltp/workspace-overlay 系列。
  docker compose 脚本门（redis/tikv/sqlite/etcd）真实可跑；CRLF 问题用
  /tmp LF 归一化副本解决（Linux CI 等价）。
- **FUSE**：`/dev/fuse` 存在，fusermount3 3.14.0，真实挂载测试可执行。
- **git**：只能从 Windows 侧跑（WSL git 无法解析 worktree 的 Windows 路径）。

因此 Redis/TiKV/S3(rustfs)/SQLite/FUSE/docker compose 相关集成验收在本机
**可真实执行**，不再默认 NOT_RUN。无环境如实 NOT_RUN 的原则不变；
PR01 未涉及后端行为，其记录不受影响。
