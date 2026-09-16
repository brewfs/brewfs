# Native Base 实现及实验报告

状态：进行中（PR04 完成）。本报告按规格包模板维护，逐 PR 追加真实证据；
不把参考模型 PASS 抄成产品验收 PASS，无环境项如实 NOT_RUN。

## 身份与范围

- 规格：BrewFS Native Packed Base 1.2-consolidated（2026-09-15），只读 wire 3/0，
  控制契约 native_control_version=2。
- 基准 commit：`8ff73d9c18c0f81e88c183383b00d2ba86bd42ba`（实现开始时 HEAD 与其一致）。
- 阶段：P1（PR01 基线审计 → PR02 wire codec → PR03 seal reader →
  PR04 写管线与控制面事务）。
- 已读：仓库 AGENTS.md；规格包 README、CODEX_TASK、00/01/02/03/04/06/09/10/15/18/20。
- 关联不变量：本轮全部（INV-01..INV-24），PR03 重点核 INV-03/04/05/13/14
  （固定视图/缺失即错误/索引序/预算取消/不猜编码）；PR04 重点核
  INV-03/04/06/08/22（按 inode 有序提交/截断与 hole 语义/单事务原子性/
  独立写者 fencing/receipt 证据绑定）。
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

### PR04 · 真实 receipt、按 inode 顺序 commit、dirty 交接、ownership inventory

工具链：同 PR01-03（WSL Ubuntu-24.04，rustc 1.98.1；git 在 Windows 侧）。
raw 日志均在 `doc/native-base/logs/`（直接写仓库路径——WSL 空闲重启会清空
/tmp，曾两次吞掉 /tmp 中的门禁日志）。实现位于 `src/native_base/write/`
（15 个模块，约 4400 行）：`store`/`memory`（OCC 事务抽象
`Txn{checks,writes}` 与内存后端）、`redis`（单 Lua 脚本原子 check+write，
sentinel 错误映射 Conflict；SCAN+MATCH glob 转义扫描）、`tikv`（悲观事务
`get_for_update` 锁后验证，锁竞争/写冲突/锁过期统一映射 Conflict）、
`keys`/`records`（`nb2/{volume}/` 布局与紧凑记录：head state、inode data、
extents、head placements）、`domain`（ownership domain 与
register→dispatch 上传预留，spec 20 §6）、`receipts`（type 3 单值
receipt 容器 `.brfc`）、`commit`（单事务 extent+binding+placement+inode+
head 全有或全无 + 每 inode 排序门（spec 18 §10）+ OperationId 幂等
（spec 07 §2））、`overlay`（admission ticket、dirty 跟踪与交接、按序
drain，spec 18 §5/§10）。

测试为后端无关场景集：16 个 `pub(crate) async fn(Arc<dyn ControlStore>)`
场景 + 1 个双 store 竞争场景 + 3 个纯 extent 代数测试；
`memory_backend` 模块常规测试跑全量，`tests_redis`/`tests_tikv` 以
`--ignored` 集成门对真实实例跑同一场景清单（每场景随机
volume/workspace/domain id，共享后端互不干扰；snapshot 只扫本 volume
前缀）。

证据要点（对应 spec15 PR04 验收关键词「Redis/TiKV 原子
extent+binding+placement；重排上传反例」）：
- **原子提交**：单事务同时落 extent/binding(36B)/placement/inode head，
  head commit_seq 恰 +1；两条失败路径（stale guard、篡改 registration）
  前后整卷 KV snapshot 逐字节相等——无任何子集落盘。
- **重排上传反例**：同 inode 两写 admission 序 A(1)/B(2)，仅 B 上传完成
  时 drain 0 提交（committed_order 保持 0、无 inode 行）；A 迟到补齐后
  按 A 后 B 两笔齐落。排序门对早到（缺前序）与迟到（slot 已过）均永久
  OutOfOrder（非 retryable）。
- **同 head 竞争恰一次**：两个独立连接的 store（Redis/TiKV 各两个真实
  连接）从同一 HeadGuard 并发 commit 不同 order-1 truncate，恰一个
  Committed，落败方 Conflict/StaleHeadGuard 且不留 mutation result——
  fencing 来自事务而非进程内状态。
- **receipt 证据绑定**：commit 重读 registration；内容寻址去重下同内容
  对象可能已被另一操作置 Verified，容忍 Dispatched→Verified 状态推进，
  但 registration facts（object_ref/domain_id/upload_plan_hash/
  registration_seq/attempt_generation）任一与 receipt 捕获值不符即永久
  RegistrationMismatch。
- **dirty 交接**：仅被提交 operation 的 dirty 引用被移除（非范围清除），
  未提交 sibling 的 dirty 保留，更早 capture 的 Arc 克隆不被收缩。
- **ownership inventory**：domain 非 Active 拒绝 register 与 commit；
  registry 一 (namespace, object key) 恒绑一 ObjectId（永久冲突）。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR04-FOCUSED | `cargo test -p brewfs --lib native_base::write` | 0 | PASS（29 passed / 0 failed, 34 ignored） | [pr04-focused.log](logs/pr04-focused.log) |
| PR04-REDIS | `bash .claude/pr04-redis-test.sh`（scratch redis:7.4-alpine @127.0.0.1:16379，`--ignored`） | 0 | PASS（17 passed / 0 failed：16 场景 + 独立连接竞争） | [pr04-redis-test.log](logs/pr04-redis-test.log) |
| PR04-TIKV | `bash .claude/pr04-tikv-test.sh`（scratch pd+tikv v8.5.0 docker network；测试二进制挂载进 ubuntu:24.04 容器执行，`--ignored`） | 0 | PASS（17 passed / 0 failed） | [pr04-tikv-test.log](logs/pr04-tikv-test.log) |
| PR04-FMT | `cargo fmt --all --check` | 0 | PASS | [pr04-gate.log](logs/pr04-gate.log) |
| PR04-BASHN | `bash -n run_{perf_in_container,redis_perf,juicefs_perf_in_container,juicefs_perf}.sh` | 0×4 | PASS（LF 归一化 scratch 副本，原件未动） | [pr04-gate.log](logs/pr04-gate.log) |
| PR04-BASHTEST | `bash test_{perf_report_delta,juicefs_direct_matrix,juicefs_perf_report}.sh` | 0×3 | PASS（LF 归一化 scratch 副本） | [pr04-gate.log](logs/pr04-gate.log) |
| PR04-CHECK | `cargo check --workspace` | 0 | PASS | [pr04-gate.log](logs/pr04-gate.log) |
| PR04-BUILD | `cargo build --workspace` | 0 | PASS | [pr04-gate.log](logs/pr04-gate.log) |
| PR04-FEAT-TOKIO | `cargo check -p brewfs --no-default-features --features fuse-tokio-runtime` | 0 | PASS | [pr04-gate.log](logs/pr04-gate.log) |
| PR04-FEAT-URING | `cargo check -p brewfs --no-default-features --features fuse-io-uring-runtime` | 0 | PASS | [pr04-gate.log](logs/pr04-gate.log) |
| PR04-GATE | `CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins` | 0 | PASS（791+721 passed, 0 failed, 219+185 ignored） | [pr04-gate.log](logs/pr04-gate.log) |
| PR04-CLIPPY | `cargo clippy --workspace` | 0 | PASS（native_base/write 0 警告；src/main.rs 存量警告非本 PR 引入） | [pr04-gate.log](logs/pr04-gate.log) |
| PR04-GITDIFF | `git diff --check`（Windows 侧；WSL git 无法解析 worktree 路径） | 0 | PASS | — |

验收矩阵：PR04 后 WRITE-003/008/009、KV-001/002/003、
ORD-001/002/003/004/006/007 共 12 项 PASS（ORD-001/002 证据中注明
组件级：commit/overlay 层）。如实保留待后续 PR：
- WRITE-004（write 返回后本地读）：dirty 交接机制已测（handoff 场景），
  但 overlay 的 dirty+committed 合并读路径属 PR05/PR10。
- WRITE-005/006/007（fsync 回执/上传成功 KV 失败/lease）：PR05 ingest。
- WRITE-013（CommitBeforeUpload）：上传与 commit 之间的故障注入需真实
  对象后端（PR05/PR07）。
- ORD-005（fsync 边界）：阻塞不跳过机制已测（failed_predecessor 场景），
  fsync barrier API 属 PR05。
- ORD-008（seal 冻结后 drain）：PR06A。
- DRAIN-005：OperationId 幂等半边已测（same_operation_id 场景），有界
  batch 属 drain 批处理（PR05/PR06）。
- KV-004/005（时钟偏移/profile）：P2。

说明：
- 门禁脚本 [pr04-gate.sh](logs/pr04-gate.sh)（bash 门 + cargo 门一体，
  docker 脚本门在 LF 归一化 scratch 副本执行）/
  [pr04-redis-test.sh](logs/pr04-redis-test.sh) /
  [pr04-tikv-test.sh](logs/pr04-tikv-test.sh) 可复现并逐项记录 exit
  code。
- TiKV 集成门执行方式：WSL docker 的 host-port hairpin 使 PD advertise
  端口从宿主不可达（gRPC preface EOF），故 pd/tikv 全部放进自建 docker
  network 用容器内地址互通，已编译测试二进制（仅依赖 libc/libgcc/libm）
  挂载进 ubuntu:24.04 容器加入同一网络执行。
- TiKV 悲观锁语义：锁竞争可能发生在 `get_for_update`（锁获取）而非
  commit（锁过期）——两者都映射 Conflict（事务未应用任何写入，调用方
  re-derive），该映射由独立连接竞争场景在真实集群上验证。
- mark_failed 只改状态（Failed + 后继 Blocked），上报由 drain Phase 1
  pop 终态时统一负责（单次上报）。

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
