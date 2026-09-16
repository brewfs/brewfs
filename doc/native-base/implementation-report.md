# Native Base 实现及实验报告

状态：进行中（PR03 完成）。本报告按规格包模板维护，逐 PR 追加真实证据；
不把参考模型 PASS 抄成产品验收 PASS，无环境项如实 NOT_RUN。

## 身份与范围

- 规格：BrewFS Native Packed Base 1.2-consolidated（2026-09-15），只读 wire 3/0，
  控制契约 native_control_version=2。
- 基准 commit：`8ff73d9c18c0f81e88c183383b00d2ba86bd42ba`（实现开始时 HEAD 与其一致）。
- 阶段：P1（PR01 基线审计 → PR02 wire codec → PR03 seal reader）。
- 已读：仓库 AGENTS.md；规格包 README、CODEX_TASK、00/01/02/03/04/06/09/10/15/18/20。
- 关联不变量：本轮全部（INV-01..INV-24），PR03 重点核 INV-03/04/05/13/14
  （固定视图/缺失即错误/索引序/预算取消/不猜编码）。
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

### PR03 · Data Seal 精确 reader（spec 04 §7 + spec 06 同步执行器）

工具链：同 PR01/02（WSL Ubuntu-24.04，rustc 1.98.1）。raw 日志均在
`doc/native-base/logs/`。实现位于 `src/native_base/seal/`（11 个模块，
约 4100 行）：`tables`（BNSD 根目录 + 4 表 key 编码，BE 序=数值序）、
`binding`（BlockBinding 36B：decoded_len + SHA256 完整解码域）、
`placement`（Loose/Packed tag + Span 精确覆盖校验，无任何 Fallback/Zero
标签）、`descriptor`（FrameDescriptor 96B + range_get + 逐字段
verify_against_header）、`builder`（确定性 seal 组装 + 引用完整性校验 +
BNPG 表树构建，供测试与 PR06A 复用）、`source`（ObjectSource trait +
内存实现）、`error`（SealError 分类）、`reader`（SealSnapshot 鉴权打开 +
页走查 exact-key lookup + binding/placement 共解析）、`plan`（同步
planned read：块分解→unit 去重→全 plan 一次性预算预约→带取消逐 unit
GET→scatter→覆盖合并校验）。

证据要点（对应 spec15 PR03 验收关键词）：
- **fake 计数**：CountingSource 记录每对象 GET 次数与字节数；共享 frame
  每 read 每 frame_slot 恰一次 GET；整 5 块文件（3 块共享一个 plain
  frame + 1 zstd + 1 loose）全文件读 = 3 GET；metrics 全分项
  （requested_file_bytes/logical_data_bytes/fetched_stored_bytes/
  decoded_payload_bytes/native_decoded_bytes + range_gets/frames_fetched/
  loose_objects_fetched）逐项断言，budget inflight 读后归零。
- **缺失不填零**：frame descriptor 缺失、对象 404、短读、payload 损坏、
  loose 损坏、binding 无 placement、读入短块 padding 域、span 覆盖不满
  —— 全部返回错误，输出从不补零（missing_frame_descriptor_…0 GET、
  source_not_found/short_read/corrupted_frame_payload/…_not_zeros、
  read_beyond_decoded_len_is_an_error_not_zeros）。
- **Native/Plain 边界**：边界 = payload format 字节而非内容嗅探；plain
  frame 解码域以 SF 魔数开头时原样返回
  （plain_bytes_starting_with_the_sf_magic_stay_raw）；native 块走
  decompress_framed 且 span length 禁止借用 frame.raw_len
  （native_span_length_must_not_borrow_frame_raw_len）；NativeBlockV1
  多 span 在读取时拒绝。
- 预算/取消：单元超 cap → BudgetUnitTooLarge 且 0 GET；全 plan 一次性
  预约不部分执行；预取消 0 IO；首 unit GET 后取消即停止后续 unit。
- 索引：多层树（root level≥2）命中与缺失 key 判定；页读取/解析失败
  为错误（绝不缓存为缺失）；外部表复用（seal B 通过 External RootRef
  复用 seal A 的 Frames/Objects 表，required_features 传播
  EXTERNAL_INDEX_CHILDREN）。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR03-FOCUSED | `cargo test -p brewfs --lib native_base` | 0 | PASS（117 passed / 0 failed：wire 73 + seal 44） | [pr03-focused.log](logs/pr03-focused.log) |
| PR03-FMT | `cargo fmt --all --check` | 0 | PASS | [pr03-gate.log](logs/pr03-gate.log) |
| PR03-BASHN | `bash -n run_{perf_in_container,redis_perf,juicefs_perf_in_container,juicefs_perf}.sh` | 0×4 | PASS（LF 归一化副本，原件未动） | [pr03-bash-gates.log](logs/pr03-bash-gates.log) |
| PR03-BASHTEST | `bash test_{perf_report_delta,juicefs_direct_matrix,juicefs_perf_report}.sh` | 0×3 | PASS（LF 归一化副本） | [pr03-bash-gates.log](logs/pr03-bash-gates.log) |
| PR03-CHECK | `cargo check --workspace` | 0 | PASS | [pr03-gate.log](logs/pr03-gate.log) |
| PR03-BUILD | `cargo build --workspace` | 0 | PASS | [pr03-gate.log](logs/pr03-gate.log) |
| PR03-FEAT-TOKIO | `cargo check -p brewfs --no-default-features --features fuse-tokio-runtime` | 0 | PASS | [pr03-gate.log](logs/pr03-gate.log) |
| PR03-FEAT-URING | `cargo check -p brewfs --no-default-features --features fuse-io-uring-runtime` | 0 | PASS | [pr03-gate.log](logs/pr03-gate.log) |
| PR03-GATE | `CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins` | 0 | PASS（762+721 passed, 0 failed, 185 ignored） | [pr03-gate.log](logs/pr03-gate.log) |
| PR03-CLIPPY | `cargo clippy --workspace` | 0 | PASS（native_base 0 警告；src/main.rs 的 CacheTtl 未用警告为存量，非本 PR 引入） | [pr03-gate.log](logs/pr03-gate.log) |
| PR03-GITDIFF | `git diff --check`（Windows 侧；WSL git 无法解析 worktree 路径） | 0 | PASS | — |

验收矩阵：PR03 后 WIRE 12/12 全 PASS；READ-002/003/004 PASS（seal reader
组件级，证据中注明层级与后续接入点）。如实保留：
- READ-001（Packed+Loose+Hole 混合）：Hole 语义在 extent/head KV 层
  （PR04+），seal 层不合成 hole。
- READ-005..008：页缓存/视图固定于更高层（PR04/PR07/PR10）。

说明：
- 门禁脚本 [pr03-gate.sh](logs/pr03-gate.sh)（cargo 门）/
  [pr03-bash-gates.sh](logs/pr03-bash-gates.sh)（bash 门）/
  [pr03-focused.sh](logs/pr03-focused.sh) 可复现并逐项记录 exit code。
- read_range 块编址语义：块按 block_index 固定 block_size 步进编址，
  短块 padding 域不可读（in_end > decoded_len → RangeBeyondBlock），
  多块 range 读要求覆盖块全为满尺寸（末块除外）；partial plain 读不重复
  验块 content_hash（frame raw_digest 已覆盖，全块读才验）。
- 空表语义（spec 04 §2）：verify_key_sets 把 absent 表当空集（报
  key-set 不对称）、resolve_block 把 absent Placements 表映射为
  placement 错误，两者都不借 MissingTable 逃避契约。
- 明确不在 PR03 范围（见 `src/native_base/seal/mod.rs`）：head KV 表
  （PR04）、Frozen extents（PR08）、真实对象后端（PR05/07）、async
  singleflight/prefetch/Range 合并（PR10）。

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
