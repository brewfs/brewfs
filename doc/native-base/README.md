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
- `pr01-baseline-audit.md` — PR01 基线调用者与旧 GC delete 入口审计。

## PR 状态

| PR | 范围 | 状态 |
|---|---|---|
| 01 | 基线调用者审计、成本指标盘点、反例测试 | 完成（门禁全绿） |
| 02 | wire3 codec + BNCT 控制记录 v2 骨架 | 完成（门禁全绿, WIRE 10/12 项 PASS） |
| 03 | Pack/Seal reader、view-bound placement | 未开始（含 FrameDescriptor96 与索引 lookup, 补齐 WIRE-001/010） |
| 04 | receipt、按 inode 顺序 commit、dirty 交接 | 未开始 |
| 05 | ingest、上传验证/断点恢复 | 未开始 |
| 06A | PublishedRevision、RetainBatch、seal/fork/recovery | 未开始 |
| 06B | close 证书、终结私有 cleaner、旧 GC 隔离 | 未开始 |
| 07+ | P1 接入、P2 Frozen、P3 优化 | 未开始 |

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
