# Native Base 实现及实验报告

状态：进行中（PR02 完成）。本报告按规格包模板维护，逐 PR 追加真实证据；
不把参考模型 PASS 抄成产品验收 PASS，无环境项如实 NOT_RUN。

## 身份与范围

- 规格：BrewFS Native Packed Base 1.2-consolidated（2026-09-15），只读 wire 3/0，
  控制契约 native_control_version=2。
- 基准 commit：`8ff73d9c18c0f81e88c183383b00d2ba86bd42ba`（实现开始时 HEAD 与其一致）。
- 阶段：P1（PR01 基线审计 → PR02 wire codec）。
- 已读：仓库 AGENTS.md；规格包 README、CODEX_TASK、00/01/02/03/09/10/15/18/20。
- 关联不变量：本轮全部（INV-01..INV-24），PR02 重点核 INV-01/04/05/12/13/14
  （字节精确/校验失败拒绝/索引序/结构限制/不猜编码）。
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
- PR01 未涉及后端行为变更，Redis/TiKV/S3/FUSE/docker compose 集成门对本 PR
  不适用。环境探测（2026-09-15 更新）确认本机 WSL 具备 docker daemon
  （redis/tikv/pd/rustfs 镜像已缓存）、`/dev/fuse`、fusermount3，后续 PR 的
  后端集成验收将真实执行，见 README.md 环境说明。

### PR02 · wire 3/0 codec 与 BNCT 控制记录 v2

工具链：同 PR01（WSL Ubuntu-24.04，rustc 1.98.1）。raw 日志均在
`doc/native-base/logs/`。实现位于 `src/native_base/wire/`（8 个模块）：
`uvarint`（canonical LEB128 + checked Reader/Writer）、`container`
（Header64/Footer64/CRC32C/features/codec）、`frame`（FrameHeader80 + payload
codec）、`refs`（PageAddress56/ObjectRef/RootRef/ChildRef）、`page`
（BNPG 索引页编解码与全结构校验）、`datapack`（顺序 scrub + 确定性
PackBuilder）、`bnct`（envelope + 11 kind 控制记录 v2）。fixtures 从规格包
examples/ 原样复制到 `src/native_base/testdata/`。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR02-FOCUSED | `cargo test -p brewfs --lib native_base` | 0 | PASS（73 passed / 0 failed） | [pr02-focused.log](logs/pr02-focused.log) |
| PR02-GOLDEN-BRFDP | （上项内）4 个 .brfdp fixture byte-identical 重建 | 0 | PASS（empty/two_plain_frames/native_none/plain_magic_prefix） | [testdata/](../src/native_base/testdata/) |
| PR02-GOLDEN-BNCT | （上项内）kind7 envelope 104 字节 hex golden | 0 | PASS | bnct.rs tests |
| PR02-FMT | `cargo fmt --all --check` | 0 | PASS | [pr02-gate.log](logs/pr02-gate.log) |
| PR02-BASHN | `bash -n run_{perf_in_container,redis_perf,juicefs_perf_in_container,juicefs_perf}.sh` | 0×4 | PASS（LF 归一化副本，原件未动） | [pr02-bash-gates.log](logs/pr02-bash-gates.log) |
| PR02-BASHTEST | `bash test_{perf_report_delta,juicefs_direct_matrix,juicefs_perf_report}.sh` | 0×3 | PASS（LF 归一化副本） | [pr02-bash-gates.log](logs/pr02-bash-gates.log) |
| PR02-CHECK | `cargo check --workspace` | 0 | PASS | [pr02-gate.log](logs/pr02-gate.log) |
| PR02-BUILD | `cargo build --workspace` | 0 | PASS | [pr02-gate.log](logs/pr02-gate.log) |
| PR02-FEAT-TOKIO | `cargo check -p brewfs --no-default-features --features fuse-tokio-runtime` | 0 | PASS | [pr02-gate.log](logs/pr02-gate.log) |
| PR02-FEAT-URING | `cargo check -p brewfs --no-default-features --features fuse-io-uring-runtime` | 0 | PASS | [pr02-gate.log](logs/pr02-gate.log) |
| PR02-GATE | `CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins` | 0 | PASS（718+721 passed, 0 failed, 185 ignored） | [pr02-gate.log](logs/pr02-gate.log) |
| PR02-CLIPPY | `cargo clippy --workspace` | 0 | PASS（native_base 0 警告） | [pr02-gate.log](logs/pr02-gate.log) |
| PR02-GITDIFF | `git diff --check`（Windows 侧；WSL git 无法解析 worktree 路径） | 0 | PASS | — |

验收矩阵：WIRE-002..009、011、012 共 10 项 PASS（见
[acceptance-matrix.json](acceptance-matrix.json)）。两项如实保留待 PR03：
- WIRE-001：Header64/Footer64/Frame80/PageAddress56 已断言，但
  FrameDescriptor96 属 Data Seal（spec 04 §5），随 PR03 reader 实现。
- WIRE-010：decode 全结构校验（重复/乱序/越界 bounds 拒绝）已过，
  "lookup 不误报缺失" 需要 PR03 的索引查找 API。

说明：
- 门禁脚本 [pr02-gate.sh](logs/pr02-gate.sh)（cargo 门）/
  [pr02-bash-gates.sh](logs/pr02-bash-gates.sh)（bash 门）/ 
  [pr02-focused.sh](logs/pr02-focused.sh) 可复现并逐项记录 exit code。
- writer 只声明实际使用的 required_features 位（从帧推导）；未知位、未知
  codec/格式/kind/枚举值全部 fail-closed `UnsupportedFormat`。
- PR02 为纯编解码层，未触后端；Data Seal/Frozen/Manifest payload 与 KV 布局
  明确不在本 PR 范围（见 `src/native_base/wire/mod.rs` 模块文档）。

## 一致读取、写入与发布

PR01：审计现有写/读路径的捕获与顺序机制，结论见 pr01-baseline-audit.md。
反例模型测试记录三个弱协议失败模式（torn capture、乱序重叠提交、先切 head
后补保留），作为 PR03/PR04/PR06A 实现的对照契约。

## 永久保留与私有清理

PR01：盘点全部现有 delete/GC 入口及其触发条件，形成隔离清单
（pr01-baseline-audit.md）。PR06B 将按此清单逐入口接入卷格式隔离，
不实现时默认拒绝。

## 后端、故障与准入

尚未涉及（PR04/06B 起）。后端集成环境已确认可用（WSL docker +
redis/tikv/rustfs 缓存镜像，FUSE 设备可用），相应验收将按 PR 真实执行。

## 性能与成本

PR01：盘点现有数据/元数据成本指标（见审计文档），后续 PR 按 A–F 消融补充。

## 风险、兼容与回滚

- 集成环境已确认可用（见 README.md 环境说明），后端相关风险从“无环境”
  转为按 PR 逐项验证。
- wire 3/0 与 4 个 DataPack goldens 兼容性：PR02 已验证 —— Rust
  `PackBuilder` 重建与规格包 fixture 逐字节一致（WIRE-002 PASS），且全部
  fixture 经 scrub/decode 无警告。BNCT kind7 golden 逐字节一致。
