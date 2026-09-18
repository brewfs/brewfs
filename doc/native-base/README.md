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
| 07 | P1 FUSE 接入、初始化命令与运行时准入 | 组件级完成（`762aa76`；[pr07-focused.log](logs/pr07-focused.log)） |
| 08/09 | Frozen reader、固定 revision 零 KV 读取 | 组件级完成（11 项聚焦测试；[pr07b-baseline-overlay.log](logs/pr07b-baseline-overlay.log)） |
| 10/11/12 | 读取 planner、共享缓存 preview、布局变体 | 组件级完成（`762aa76`/`8bcb106`） |
| 13 | 性能实验、能力发布与运维文档 | 未开始（A-F 全部 NOT_RUN） |

当前验收进度（2026-09-18）：173 项矩阵中 **125 PASS**、48
`SPECIFIED_NOT_IMPLEMENTED`（截至本提交）。每个 PASS 都附仓库内命令、
exit code 与日志；缺环境或只做到组件级的项不虚标为完成。队列中的主要工作：
真实 FUSE 挂载 + Redis/TiKV + S3 的 READ/WRITE/fsync 端到端集成、P2 Frozen
COW/驱逐、P3 cache/repack 与性能 A-F，以及 cleanup/retention/ordered-commit
剩余条目。发布/准入类配置拒绝已由 PR07D 收口（WRITE-013/GATE-001/GATE-004/
RET-021/CLN-024）；GATE-002/GATE-003 的 required_features 依赖闭包已由 PR07H 收口
（DataPack scrub 与 Data Seal open 都在任何字节被读出前拒绝谎报的声明）。

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
