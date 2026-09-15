# Native Base 实现及实验报告

状态：进行中（PR01）。本报告按规格包模板维护，逐 PR 追加真实证据；
不把参考模型 PASS 抄成产品验收 PASS，无环境项如实 NOT_RUN。

## 身份与范围

- 规格：BrewFS Native Packed Base 1.2-consolidated（2026-09-15），只读 wire 3/0，
  控制契约 native_control_version=2。
- 基准 commit：`8ff73d9c18c0f81e88c183383b00d2ba86bd42ba`（实现开始时 HEAD 与其一致）。
- 阶段：P1（PR01 基线审计）。
- 已读：仓库 AGENTS.md；规格包 README、CODEX_TASK、00/01/09/10/15/18/20。
- 关联不变量：本轮全部（INV-01..INV-24），PR01 重点核 INV-03（一致捕获）、
  INV-11/INV-21（永久保留/清理集合）、INV-22（提交顺序）。
- 关联发现：FNL-03/FNL-04/FNL-08/FNL-10/FNL-11（保留精确性/原子性/顺序/证据）。

## 真实测试证据

### PR01 · 基线调用者审计与反例

工具链：WSL Ubuntu-24.04，rustc 1.98.1（Windows 侧无法编译 Linux-only 依赖，
见 README.md 环境说明）。raw 日志均在 `doc/native-base/logs/`。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR01-AUDIT | （只读代码审查，三路并行审计） | — | DONE | [pr01-baseline-audit.md](pr01-baseline-audit.md) |
| PR01-CEX | `cargo test -p brewfs --lib native_base` | 0 | PASS（10 passed / 0 failed） | 见 pr01-gate.log 的 test 段 |
| PR01-FMT | `cargo fmt --all --check` | 0 | PASS | [pr01-gate.log](logs/pr01-gate.log) |
| PR01-BASHN | `bash -n run_{perf_in_container,redis_perf,juicefs_perf_in_container,juicefs_perf}.sh` | 0×4 | PASS（LF 归一化副本，原件未动） | [pr01-bash-gates.log](logs/pr01-bash-gates.log) |
| PR01-BASHTEST | `bash test_{perf_report_delta,juicefs_direct_matrix,juicefs_perf_report}.sh` | 0×3 | PASS（LF 归一化副本） | [pr01-bash-gates.log](logs/pr01-bash-gates.log) |
| PR01-CHECK | `cargo check --workspace` | 0 | PASS | [pr01-gate.log](logs/pr01-gate.log) |
| PR01-BUILD | `cargo build --workspace` | 0 | PASS | [pr01-gate.log](logs/pr01-gate.log) |
| PR01-FEAT-TOKIO | `cargo check -p brewfs --no-default-features --features fuse-tokio-runtime` | 0 | PASS | [pr01-gate.log](logs/pr01-gate.log) |
| PR01-FEAT-URING | `cargo check -p brewfs --no-default-features --features fuse-io-uring-runtime` | 0 | PASS | [pr01-gate.log](logs/pr01-gate.log) |
| PR01-GATE | `CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins` | 0 | PASS（655+721 passed, 0 failed, 185 ignored） | [pr01-gate.log](logs/pr01-gate.log) |
| PR01-CLIPPY | `cargo clippy --workspace` | 0 | PASS | [pr01-gate.log](logs/pr01-gate.log) |
| PR01-GITDIFF | `git diff --check`（Windows 侧；WSL git 无法解析 worktree 路径） | 0 | PASS | — |

说明：
- 门禁脚本 [pr01-gate.sh](logs/pr01-gate.sh) / [pr01-bash-gates.sh](logs/pr01-bash-gates.sh)
  可复现全部命令并逐项记录 exit code。
- docker 脚本门在 Windows checkout 的 CRLF 副本上会假失败；bash 门在
  /tmp 的 LF 归一化副本执行（Linux CI 等价环境），仓库原件未改动。
- Redis/TiKV/S3/FUSE/docker compose 真实后端集成：NOT_RUN（本机无环境），
  对应能力不声明。AGENTS.md 提及的 compose 性能门不适用于 PR01（无行为变更）。

## 一致读取、写入与发布

PR01：审计现有写/读路径的捕获与顺序机制，结论见 pr01-baseline-audit.md。
反例模型测试记录三个弱协议失败模式（torn capture、乱序重叠提交、先切 head
后补保留），作为 PR03/PR04/PR06A 实现的对照契约。

## 永久保留与私有清理

PR01：盘点全部现有 delete/GC 入口及其触发条件，形成隔离清单
（pr01-baseline-audit.md）。PR06B 将按此清单逐入口接入卷格式隔离，
不实现时默认拒绝。

## 后端、故障与准入

尚未涉及（PR04/06B 起）。Redis/TiKV/S3/FUSE 真实后端门在当前环境 NOT_RUN。

## 性能与成本

PR01：盘点现有数据/元数据成本指标（见审计文档），后续 PR 按 A–F 消融补充。

## 风险、兼容与回滚

- 本机无集成环境：所有真实后端验收保持 NOT_RUN，对应能力不声明。
- wire 3/0 与 4 个 DataPack goldens 兼容性在 PR02 建立 Rust golden 后验证。
