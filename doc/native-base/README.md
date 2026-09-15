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
| 02 | wire3 codec + BNCT 控制记录 v2 骨架 | 未开始 |
| 03 | Pack/Seal reader、view-bound placement | 未开始 |
| 04 | receipt、按 inode 顺序 commit、dirty 交接 | 未开始 |
| 05 | ingest、上传验证/断点恢复 | 未开始 |
| 06A | PublishedRevision、RetainBatch、seal/fork/recovery | 未开始 |
| 06B | close 证书、终结私有 cleaner、旧 GC 隔离 | 未开始 |
| 07+ | P1 接入、P2 Frozen、P3 优化 | 未开始 |

## 环境说明

本实现工作在 Windows 开发机上进行。Redis/TiKV/S3/FUSE/docker compose 集成
环境不可用，相关验收项标 `NOT_RUN` 并关闭对应能力声明；可本机验证的是
Rust 单元/golden/模型测试与 `cargo fmt/check/test --workspace --lib --bins/clippy`
CI 门。AGENTS.md 中的 bash/docker 脚本门在本机标 NOT_RUN。
