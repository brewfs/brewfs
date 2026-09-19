# Native Base 实现及实验报告

状态：PR01–PR12 全部交付并通过 focused unit tests 与 workspace CI gate；PR13 文档与能力矩阵已更新，性能实验 A–F 因缺少 FUSE 集成环境标记为 NOT_RUN。本报告按规格包模板维护，逐 PR 追加真实证据；
不把参考模型 PASS 抄成产品验收 PASS，无环境项如实 NOT_RUN。

## 身份与范围

- 规格：BrewFS Native Packed Base 1.2-consolidated（2026-09-15），只读 wire 3/0，
  控制契约 native_control_version=2。
- 基准 commit：`8ff73d9c18c0f81e88c183383b00d2ba86bd42ba`（实现开始时 HEAD 与其一致）。
- 阶段：P1（PR01 基线审计 → PR02 wire codec → PR03 seal reader →
  PR04 写管线与控制面事务 → PR05 ingest 与上传验证/resume → PR06A
  永久发布、精确保留、seal/fork/recovery → PR06B close/cleanup/旧GC隔离 → PR07 P1 FUSE 挂载与初始化 → PR08 Frozen Metadata 格式与索引 → PR09 固定 revision 零 KV 读取 → PR10 读取 planner 与预算/singleflight → PR12 lossless 布局变体校验）。
- 已读：仓库 AGENTS.md；规格包 README、CODEX_TASK、00/01/02/03/04/06/09/10/15/18/20。
- 关联不变量：本轮全部（INV-01..INV-24），PR03 重点核 INV-03/04/05/13/14
  （固定视图/缺失即错误/索引序/预算取消/不猜编码）；PR04 重点核
  INV-03/04/06/08/22（按 inode 有序提交/截断与 hole 语义/单事务原子性/
  独立写者 fencing/receipt 证据绑定）；PR05 重点核 INV-01/02/04/07/12
  （同内容幂等/身份保留不跟随链接/不得仅凭 HEAD 通过验证/源一致性与有界
  恢复/逃逸输入拒绝）。
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
| PR04-BASHN | `bash -n run_{perf_in_container,redis_perf,juicefs_perf_in_container,juicefs_perf}.sh`（LF 归一化 scratch 副本） | 0×4 | PASS（**记录值**：逐脚本 exit=0） | [pr04-bash-gates.log](logs/pr04-bash-gates.log) |
| PR04-BASHTEST | `bash test_{perf_report_delta,juicefs_direct_matrix,juicefs_perf_report}.sh`（LF 归一化 scratch 副本） | 0×3 | PASS（**记录值**：逐脚本 exit=0） | [pr04-bash-gates.log](logs/pr04-bash-gates.log) |
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
  [pr04-tikv-test.sh](logs/pr04-tikv-test.sh) 可复现；其中 redis/tikv
  两个测试脚本逐项记录 exit code。
- **证据更正（2026-09-16）**：PR04 表格里各行的退出码来源此前未写清，
  现逐条说明日志到底记录了什么：
  - PR04-FOCUSED / PR04-REDIS / PR04-TIKV：三个日志都含 cargo 自己的
    判定行（`test result: ok. … 0 failed`），与表中 “0” 一致，但**没有**
    单独的退出码记录行；表中 “0” 与该判定行相符，不是逐进程捕获的值。
  - PR04-FMT / CHECK / BUILD / FEAT-TOKIO / FEAT-URING / GATE / CLIPPY：
    [pr04-gate.log](logs/pr04-gate.log) 末行是 `=== GATE: ALL PASSED ===`，
    该行只有在脚本里所有 `|| exit 1` 守卫都未触发时才会打印，因此这些
    步骤的 “0” 由日志的**终态标记**支撑（聚合层面），日志内没有逐步
    退出码行。
  - PR04-BASHN / PR04-BASHTEST：这是真正的缺口——脚本里这些是
    `bash -n … || exit 1` / `(cd … && bash …) || exit 1`，成功时脚本自身
    **静默**，pr04-gate.log 对应段落只有 step 标题，连 cargo 那样的判定行
    都没有，原表 “0×4 / 0×3” 由控制流推断。此外 pr04-gate.sh 原版本
    `cd "$(dirname "$0")/.."` 从其提交位置 `doc/native-base/logs` 只上溯到
    `doc/native-base`，提交的副本不能复现自己的日志（原运行用的是高一层
    的副本，这也是 pr04-gate.log 里 cargo 路径显示 worktree 的原因）。
    现已修正 cd 为上溯三级并打印解析出的根（见脚本头部 NOTE），并用
    [pr04-bash-gates.sh](logs/pr04-bash-gates.sh) 在当前 commit 重跑这七个
    脚本，逐项打印 `bashn:<name>:exit=$?` / `test:<name>:exit=$?` 到
    [pr04-bash-gates.log](logs/pr04-bash-gates.log)（7 行全部 exit=0，
    脚本 overall=0）。上表两行因此改引该日志：现在有记录值支撑，状态仍为
    PASS。cargo 侧各行未改动，仍以 pr04-gate.log 的原始运行为准。
- TiKV 集成门执行方式：WSL docker 的 host-port hairpin 使 PD advertise
  端口从宿主不可达（gRPC preface EOF），故 pd/tikv 全部放进自建 docker
  network 用容器内地址互通，已编译测试二进制（仅依赖 libc/libgcc/libm）
  挂载进 ubuntu:24.04 容器加入同一网络执行。
- TiKV 悲观锁语义：锁竞争可能发生在 `get_for_update`（锁获取）而非
  commit（锁过期）——两者都映射 Conflict（事务未应用任何写入，调用方
  re-derive），该映射由独立连接竞争场景在真实集群上验证。
- mark_failed 只改状态（Failed + 后继 Blocked），上报由 drain Phase 1
  pop 终态时统一负责（单次上报）。

### PR05 · local/seekable-tar ingest、稳定计划、上传验证与 resume

工具链：同 PR01-04（WSL Ubuntu-24.04，rustc 1.98.1；git 在 Windows 侧）。
raw 日志在 `doc/native-base/logs/`。实现位于 `src/native_base/ingest/`（9 个模块，
约 4900 行，54 个测试）：

- `source`：统一输入契约 `IngestSource{policy, inventory, read_range, revalidate}`。
  `LocalDirSource` 以 `symlink_metadata` 遍历（不跟随 symlink，symlink 目标
  作为内容）、hardlink 按 `(dev, ino)` 归组、设备/管道/套接字成员拒绝、
  身份 = SHA-256(dev‖ino‖size‖mtime)；`SeekableTarSource` 解析 ustar，拒绝
  绝对路径/`..` 逃逸/重复路径/设备成员/压缩档（xz、gz magic），支持 old-GNU
  sparse（固定头最多 4 extent；带 extension header 超出 P1 界则拒绝），并校验
  存储序前缀和 `Σextent_len == stored size`。
- `wal`：BNWL 记录 `"BNWL"[4] + record_len:u32 + sequence:u64 + kind:u16 +
  reserved:u16 + payload + crc32c:u32`（最小 24B、最大 16MiB），sequence 自 1
  严格 +1；`ReplayOutcome::{Complete, DroppedTail}` 只允许丢弃截断或 CRC 错的
  **最终**记录，中间 CRC 错/坏 magic/坏长度/序号缺口一律 `CorruptJournal`；
  `WalWriter::open` 就地修复损坏尾；checkpoint 走
  temp→fsync→rename→fsync(parent)。
- `plan`：`PLAN_ALGORITHM_VERSION=1`，uvarint 规范编码，`digest()` =
  SHA-256(版本 LE ‖ 编码)，与主计划同为 temp→fsync→rename→fsync(parent)。
  resume 时版本或 digest 不符 → `PlanMismatch`：不静默重新分区，也不把上一轮
  的 receipt 混进改动过的计划。
- `session`：布局 `session.json`/`inventory.bin`/`upload.plan`/`journal.wal`/
  `checkpoint.bin`/`objects/`/`spool/` + 单 owner 独占锁 `owner.lock`
  （`flock(LOCK_EX|LOCK_NB)`）；10 态状态机（NEW→INVENTORIED→PLAN_FROZEN→
  METADATA_STAGED→DATA_UPLOADING→DATA_VERIFIED→SEAL_VERIFIED→
  MANIFEST_VERIFIED→PUBLISHED，外加 BUILT_UNPUBLISHED 显式停点；
  Published/BuiltUnpublished 为终态）默认停 BUILT_UNPUBLISHED；journal 权威
  （先 WAL，再 session.json/checkpoint）；`artifact_view()` 对任何非 Published
  状态返回 `NotPublished`。
- `backend`：比既有 `ObjectBackend` 更严的上传合同 —— 原子 create-only PUT
  （不支持且无等价验证路径则拒绝启用可写发布，E02）、`range_get_exact` 全填充
  失败即错（绝不补零）、multipart 结果分类；配套内存实现与全套故障注入。
- `upload`：两验证 profile —— `ExactReadback`（默认，整对象回读比对）与
  `ServiceValidatedChecksums`（显式，用服务端事实）。CreateOnly 分支 412 →
  inspect 同 len+hash 视为幂等、异则 `PreconditionMismatch`；Multipart 分支
  part 计划先持久化再上传，part 超时 → `multipart_status` 按已接收 part 恢复
  （不重发、不换号），complete 超时 → 查状态，`NoSuchUpload` → 核对最终对象，
  HTTP 200 内嵌错误 → `Failed`；重试有界（part 4 / complete 4 / 整体重启 3）。
- `build`：inventory → revalidate → 打包（目录跳过、symlink 取内容、hardlink
  按组去重、`PackFrame::plain_bytes`、`object_key` 命名、按 content budget 切
  pack 与 part）→ 冻结计划 → 上传（resume 先比对 plan digest，跳过已 verified
  对象）→ 停 BUILT_UNPUBLISHED；`build_to_unpublished` 按相位恢复（New 全跑 /
  Inventoried 只冻结 / 其余续传 / BuiltUnpublished 幂等返回）。

证据要点（对应 spec15 PR05 验收关键词「源一致性、模糊结果、未发布产物不可见」）：

- **源一致性**：`LocalDirSource::revalidate` 逐项重算 identity（dev/ino/size/
  mtime），tar 源重算整档摘要；inventory 之后源被改动 → `SourceChanged` 且
  发布停止（场景：inventory 后改写文件 → 报错，session 连同状态与计划留在
  磁盘上供诊断）。不跟随 symlink 由 `symlink_metadata` 保证；hardlink 共享
  `hardlink_group`，身份保留而数据只落一份。
- **模糊结果**：4 类歧义都以「查询同一身份」收敛而非盲目重发 —— part 响应
  丢失但服务端已落盘 → 按计划身份恢复该 part（序号不变）；part 超时且未落盘
  → 同号重试；complete 响应丢失 → `multipart_status` 报 Completed → 按服务端
  事实收敛；complete 报 NoSuchUpload 而已组装 → 核对最终对象内容后收敛。
  HTTP 200 内嵌错误与 409 都永不变成 remote-verified。
- **未发布产物不可见**：`Session::artifact_view()` 对任何非 Published 状态
  （含 BUILT_UNPUBLISHED）返回 `NotPublished`，P1 构建的本地对象与计划在发布
  路径上不可见；磁盘预算门在**任何字节上传之前**失败（`InsufficientDisk`），
  不留半成品。
- **resume 不静默改计划**：篡改 `upload.plan` 后 resume → `PlanMismatch`；已
  verified 的对象在续传时被跳过（不重复上传），权威状态由 journal 重放得出，
  截断尾有界恢复、中间损坏停止。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR05-FOCUSED | `cargo test --lib -j 4 native_base::ingest -- --test-threads 4` | 0 | PASS（54 passed / 0 failed, 1010 filtered） | [pr05-gate.log](logs/pr05-gate.log) |
| PR05-FMT | `cargo fmt --all --check` | 0 | PASS | [pr05-gate.log](logs/pr05-gate.log) |
| PR05-BASHN | `bash -n run_{perf_in_container,redis_perf,juicefs_perf_in_container,juicefs_perf}.sh` | 0×4 | PASS（LF 归一化 scratch 副本，仓库原件未动） | [pr05-gate.log](logs/pr05-gate.log) |
| PR05-BASHTEST | `bash test_{perf_report_delta,juicefs_direct_matrix,juicefs_perf_report}.sh` | 0×3 | PASS（LF 归一化 scratch 副本） | [pr05-gate.log](logs/pr05-gate.log) |
| PR05-CHECK | `cargo check --workspace` | 0 | PASS | [pr05-gate.log](logs/pr05-gate.log) |
| PR05-BUILD | `cargo build --workspace` | 0 | PASS | [pr05-gate.log](logs/pr05-gate.log) |
| PR05-FEAT-TOKIO | `cargo check -p brewfs --no-default-features --features fuse-tokio-runtime` | 0 | PASS | [pr05-gate.log](logs/pr05-gate.log) |
| PR05-FEAT-URING | `cargo check -p brewfs --no-default-features --features fuse-io-uring-runtime` | 0 | PASS | [pr05-gate.log](logs/pr05-gate.log) |
| PR05-GATE | `CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins` | 0 | PASS（845+721 passed, 0 failed, 219+185 ignored） | [pr05-gate.log](logs/pr05-gate.log) |
| PR05-CLIPPY | `cargo clippy --workspace` | 0 | PASS（native_base/ingest 0 警告；src/main.rs 存量 `unused import: CacheTtl` 非本 PR 引入） | [pr05-gate.log](logs/pr05-gate.log) |
| PR05-GITDIFF | `git diff --check`（Windows 侧；WSL git 无法解析 worktree 路径） | 0 | PASS | — |

验收矩阵：PR05 后 INGEST-001..010 共 10 项 PASS。如实说明范围：
- 上传侧证据全部来自带故障注入的内存 `MemoryUploadBackend` —— 被验证的是
  `UploadBackend` 合同语义（PR07 的真实对象后端必须满足同一合同）；真实
  后端集成（rustfs/S3 语义、服务端 multipart 并发交叠）属 PR07，本 PR 不主张。
- INGEST-010 的「磁盘耗尽」在 P1 以 build 的 disk budget 门实现
  （`InsufficientDisk`，上传前失败）；后端侧配额/507 的真实注入属 PR07。
- 未做项（如实保留）：跨 session 的真实后端 lease 回收与 runtime 私有清理接线属 PR07；manifest
  层（SealVerified→ManifestVerified→Published）属 PR06A。

说明：
- 门禁脚本 [pr05-gate.sh](logs/pr05-gate.sh) 与 [pr05-gate.log](logs/pr05-gate.log)
  可复现（脚本从自身位置回溯到仓库根，先打印 `gate root:` 供核对；docker 脚本门
  在 LF 归一化 scratch 副本执行）。
- 本轮修复的两个真实缺陷（PR05 开发中自查 + 焦点测试暴露）：多 extent 成员的
  存储偏移必须累加前序 extent 长度（原实现会让稀疏成员读错位）；常规 tar 成员
  必须携带覆盖整个逻辑尺寸的 extent（原实现让常规成员整段走 hole 分支读零）。
- `cargo clippy` 曾就新增代码报 6 条风格警告（`new_without_default`、
  `collapsible_if`×2、`needless_borrows_for_generic_args`×2、
  `manual_div_ceil`），已全部修掉；门禁日志为修后运行。

PR01：审计现有写/读路径的捕获与顺序机制，结论见 pr01-baseline-audit.md。
反例模型测试记录三个弱协议失败模式（torn capture、乱序重叠提交、先切 head
后补保留），作为 PR03/PR04/PR06A 实现的对照契约。

### PR06A · 永久 PublishedRevision、精确 RetainBatch、seal/fork/recovery（完成）

实现提交：`ea373a9b8af5187c01b16b65c46a16c56ddd9ee2`。实现位于
`src/native_base/lifecycle/`：kind-4 manifest、kind-5 physical inventory 与
RetainBatch 索引、精确 origin partition、八阶段 seal journal、认证有界 drain、
PublishedRevision/RetentionReceipt/KvBaseRetention/head 的单事务发布，以及只允许
PublishedRevision 源的 fork/fast-forward。PR04 的紧凑 `HeadState` 字节保持不变，
生命周期事务与 kind-16 `NativeWorkspaceHead` 同步双写。

关键反例证据：

- 混合 Loose/Packed 候选按实际闭包切为 workspace/build 两个 RetainBatch；已覆盖
  的 Loose A 不进入永久集合，Loose B、整 Pack、Seal、inventory 与 manifest
  精确进入所属域；任一域先进入 DRAINING 时事务全不生效。
- admission ticket 大于当前 commit sequence 的写仍进入冻结计划；伪造条目、未
  fsync payload、Noop remote barrier 与 stale head 全部被拒绝；重复 batch 与
  phase transition 幂等。
- 发布成功、OperationId 回包丢失重试、误 abort 和 complete 都返回同一固定结果；
  两域 receipt、PublishedRevision、KV base 与 head switch 全有全无。
- 100 个 fork 只新增每个 workspace 的 5 条有界控制记录，不复制 metadata/data；
  有 visible delta 的目标明确拒绝 fast-forward。

| 测试项 | 原样命令 | 退出码 | 结果 | raw 证据 |
|---|---|---:|---|---|
| PR06A-FOCUSED | `cargo test -p brewfs --lib -j 4 -- native_base::lifecycle:: --test-threads 4` | 0 | PASS（8 passed / 0 failed） | [pr06a-focused.log](logs/pr06a-focused.log) |
| PR06A-NATIVE | `cargo test -p brewfs --lib -j 4 -- native_base:: --test-threads 4` | 0 | PASS（212 passed / 0 failed / 34 ignored） | 同上 |
| PR06A-FMT/BASH/CHECK/BUILD/FEATURE | `bash doc/native-base/logs/pr06a-final-run.sh` 内逐步执行 AGENTS 门禁 | 0 | PASS（每个步骤独立 `:exit=0`） | [pr06a-final-gate.log](logs/pr06a-final-gate.log) |
| PR06A-WORKSPACE-TEST | `CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins -j 4 -- --test-threads 4` | 0 | PASS（857 + 721 passed；0 failed） | 同上 |
| PR06A-CLIPPY | `CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo clippy --workspace` | 0 | PASS（无新增警告） | 同上 |
| PR06A-GITDIFF | `git diff --check`（Windows 侧） | 0 | PASS | 本报告更新前复核 |

完整门禁日志第 2 行固定上述实现 SHA，末尾为 `=== GATE: ALL PASSED ===`；WSL
包装的流水线退出码为 0。共享 index builder 的字节稳定 golden 仍钉住重构前
`7291196` 的两个 seal 摘要，证明 06A 没有改写既有 Data Seal 字节。

本阶段未虚标的范围：`LIFE-006` 需要 PR07 FUSE/SDK 真读 mixed base；`RET-006`
多次 seal 并集、`RET-020/023` 与全部 `CLN-*` 归 06B；`RET-008` 的 Frozen
metadata 外部页归 PR08/09；`RET-011` 是 PR07 runtime 读计数；`RET-012` 是 P2；
`RET-021` 已有纯拒绝函数但必须等 PR07 配置加载器接线后才能 PASS。06A 证据是
内存控制后端的确定性事务/故障注入；Redis/TiKV/S3/FUSE 真后端门禁不在此冒充。

## 永久保留与私有清理

### PR06B · ownership close 证书、终结私有 cleaner、旧 GC 隔离（组件交付完成）

实现提交：`48966c9b0b5e6754d163a35631bc25e83d08a10e`；门禁脚本修订与最终
精确 SHA 为 `ea86664da47123837a79976f95c293c7e307acc7`。实现位于
`src/native_base/lifecycle/cleanup.rs`，并扩展 `Keys` 的 inventory/object/close/
cleanup journal 命名空间。关闭只接受同一 volume/domain 的连续库存、终态
registration、连续 RetentionReceipt 和认证索引根；未知 attempt 会原子进入
`QUARANTINED`，不会靠 lease expiry 或取消 HTTP future 伪造终结。

私有计划绑定固定 close certificate、`I(d)−K(d)−C(d)`、seq/root/digest 与
有界 128-object/1MiB journal batch；apply 先将 registration 置
`DELETE_PENDING`，再在 KV 锁外逐对象删除，按 `Deleted`/`AlreadyAbsent` 记录
实际结果。重复 operation/batch、部分失败重试、两个 cleaner 竞争和
`RECOVERY_RETAIN_ALL` 均 fail-closed。`CLEANED` 只表示计划候选处理完，保留
对象继续存在。

旧删除入口增加 native-v2 隔离：`ObjectBlockStore::delete_range`、
`WorkspaceGc`、`BlockStoreGC` 与 mark-sweep 配置均拒绝
`workspace-native-v2`，后续 runtime 构造器必须显式绑定新 cleaner。

| 测试项 | 原样命令 | 退出码 | 结果 | raw 证据 |
|---|---|---:|---|---|
| PR06B-FOCUSED | `bash doc/native-base/logs/pr06b-focused.sh` | 0 | PASS（cleanup 11、lifecycle 19、legacy-delete guard 1；0 failed；clippy -D warnings） | [pr06b-focused.log](logs/pr06b-focused.log) |
| PR06B-FMT/BASH/CHECK/BUILD/FEATURE | `bash doc/native-base/logs/pr06b-final-run.sh` | 0 | PASS（每个步骤独立 `exit=0`；shell 使用 LF scratch 副本） | [pr06b-final-gate.log](logs/pr06b-final-gate.log) |
| PR06B-WORKSPACE-TEST | `CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins` | 0 | PASS（869 + 722 passed；0 failed；219 + 185 ignored） | 同上 |
| PR06B-CLIPPY | `CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo clippy --workspace` | 0 | PASS（无 PR06B 新警告；仅 feature 组合下存量 `CacheTtl` warning 已知） | 同上 |
| PR06B-GITDIFF | `git diff --check`（Windows 侧） | 0 | PASS（提交后工作树仅保留用户 `.claude/`） | 本报告更新前复核 |
| PR06C-CTRL003 | `bash doc/native-base/logs/pr06c-ctrl003.sh` | 0 | PASS（lifecycle 22 passed/0 failed；native lib 960 passed, 221 ignored, 0 failed；clippy 无新增 warning）：空域不构造 RetainBatch、空对象索引不可构建、close 证书三个可选 root 均为 `None`、对空集携带 artifact 与非空集缺失 artifact 均报错 | [pr06c-ctrl003.log](logs/pr06c-ctrl003.log) |
| PR06D-CTRL004 | `bash doc/native-base/logs/pr06d-ctrl004.sh` | 0 | PASS（ctrl_004 2 passed/0 failed；lifecycle 24 passed/0 failed；native lib 962 passed, 221 ignored, 0 failed；clippy 无新增 warning）：close 副本 hash 与权威 KV 证书不一致时拒绝写入；权威证书被置换时删除前停删 | [pr06d-ctrl004.log](logs/pr06d-ctrl004.log) |
| PR06D-CI-GATE | `bash doc/native-base/logs/ci-gate.sh` | 0 | PASS（fmt、workspace check/build、feature checks、workspace lib+bins 907+726 passed/0 failed、clippy 全 `exit=0`） | [pr06d-ci-gate.log](logs/pr06d-ci-gate.log) |

补充（PR06C）：CTRL-003 的空域语义已在组件级闭环。`build_retain_batch` 拒绝
空候选集，`build_object_index` 拒绝空对象列表，close 证书的 `inventory` /
`retained_union` / `control_evidence` 用 `Option::None` 表达空集
（`option_tag` 只写 1 字节标签，不会产生零长度 `RootRef`），
`require_optional_artifact` 对"空集带 artifact"与"非空集缺 artifact"都报错。

补充（PR06D）：CTRL-004 的副本/权威一致性已在组件级闭环。
`build_single_value_record` 按 `(object_id, key, record_bytes)` 生成规范的 type-3
单值容器，因此 close 证据对象可被任意一方重算并与权威证书比对：

- close 阶段：`close_domain_commit` 从权威证书重建规范副本，要求调用方提供的
  `certificate_ref` 完全相等（`full_hash` / `stored_digest` / `object_len` /
  `key` / 页地址）；不一致直接返回 `Durability` 错误，且不写证书行、不改域状态
  （域仍为 `Draining`）。未提供副本时由权威派生，`close_ref` 永不为 `None`。
- cleanup 阶段：`load_cleanup_authority` 在任何删除前用 KV 权威证书重建副本并
  要求等于 `plan.close_ref`；权威证书被置换/回滚时立即停删并报
  authority/evidence 错误，deleter 不会收到一次调用。

聚焦反例覆盖：publish-before-close 与 close-before-publish、UNKNOWN/quarantine、
lease 不足、多个 RetainBatch 并集、I/K/C 精确差集、证据缺失与 authority
rollback retain-all、版本化/ObjectLock 能力拒绝、两个 cleaner 竞争、部分删除
重试与 CLEANED 后 retained 可读；PR06C 追加空域 `Option::None` 语义，PR06D
追加 close 副本与权威 KV 证书的 hash 一致性（close 拒绝写入 + cleanup 停删）。
验收矩阵从 65 项 PASS 增至 80 项 PASS，PR06C/PR06D 后为 97 项 PASS。

仍如实保留的范围：cleanup 目前在内存 `ControlStore`/故障注入 deleter 上验证，
Redis/TiKV durability、真实 S3 per-object delete 响应分类、native-v2 runtime/FUSE
接线、quota hard-limit、独立 ControlEvidence 引导对象和 operator 实际删除路由
属于后续 PR07/运维集成；因此本节不宣称完整 P1 已开放。


### PR07 · P1 FUSE 接入、初始化命令与运行时准入

工具链：同 PR01（WSL Ubuntu-24.04，rustc 1.98.1）。实现分布：

- `src/main.rs`：`mount_native_with_client` / `mount_native_with_catalog` /
  `init_native_volume_with_catalog` 接线；Redis/TiKV 分别创建独立
  `ControlStore` 与 workspace catalog；拒绝 SQLite/etcd。
- `src/native_base/runtime/header.rs`：`NativeVolumeHeader` 编码/校验、
  `initialize_volume`（create-only 事务）、`load_volume_header`（重新校验）。
- `src/native_base/runtime/io.rs`：`NativeDataRuntime` / `BaseDataSource` /
  `ZeroBaseDataSource` 写入边界与 fsync 语义；baseline、loose extent 与
  Hole 的范围合成，以及纯 punch-hole 的直接提交。
- `src/native_base/write/commit.rs` / `overlay.rs`：commit planner 携带
  immutable baseline 尺寸，首次 mutation 不把未修改 baseline 物化成 Hole。
- `src/native_base/runtime/workspace.rs`：`WorkspaceBaseDataSource` 桥接
  workspace-v1 meta layer 与 native baseline reads（feature-gated）。
- `src/native_base/runtime/migration.rs`：迁移模式与模式 0（拒绝）。
- `src/native_base/runtime/object.rs`：`BackendObjectRepository` 对象后端。
- `src/vfs/fs/mod.rs`：`attach_native_runtime` 与 13 处 VFS 操作派发点。

校验点：
- volume_format / schema_version 失败关闭（不隐式升级旧 workspace-v1）。
- `workspace-v1` catalog 必须先存在；native header create-only。
- `volume_id` 与 `storage_namespace_id` 双校验，不匹配即拒绝挂载。
- Redis / TiKV control + catalog 双后端矩阵：2 类控制面 × 2 类元数据面。
- 新卷 `workspace init-native` 命令：仅创建 native header，不改动 catalog。
- remount writer generation 更新与 head 持久域复用。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR07-FOCUSED | `bash doc/native-base/logs/pr07-focused.sh` | 0 | PASS（942 passed, 221 ignored, 0 failed；clippy/format/default-feature-check 均通过） | [pr07-focused.log](logs/pr07-focused.log) |
| PR07-FMT | `cargo fmt --all --check` | 0 | PASS（同上） | 同上 |
| PR07-NATIVE-CHECK | `cargo check -p brewfs --features native-packed-base` | 0 | PASS（同上） | 同上 |
| PR07-DEFAULT-CHECK | `cargo check -p brewfs`（无 native feature） | 0 | PASS（不破坏旧构建） | 同上 |
| PR07-TEST | `cargo test -p brewfs --features native-packed-base --lib` | 0 | PASS（942 passed, 0 failed） | 同上 |
| PR07-BASELINE-OVERLAY | `bash doc/native-base/logs/pr07b-baseline-overlay.sh` | 0 | PASS（runtime io 11 passed/0 failed；native lib 959 passed, 221 ignored, 0 failed）：Packed-like baseline + loose patch + Hole 逐字节 oracle；fresh punch-hole 保留 baseline 尺寸与尾部；truncate-down 后 extend/far-write 不复活旧尾部；跨 block 局部写与多 extent 非连续写逐字节回读 | [pr07b-baseline-overlay.log](logs/pr07b-baseline-overlay.log) |
| PR07-DIRECT-HOLE | `cargo test -p brewfs --features native-packed-base --lib native_base::runtime::io::tests` | 0 | PASS（11 passed, 0 failed）：不与 pending 写、也不与已提交 Data extent 重叠的 punch-hole 直接提交为 Hole extent；会切开已提交 Data extent 的孔回落块物化，避免留下非块对齐 extent 片段 | 同上 |
| PR07-BASELINE-SIZE | 同上 | 0 | PASS（11 passed, 0 failed）：commit planner 携带 immutable baseline 尺寸，首次 mutation / truncate / punch-hole 不再把未修改 baseline 物化成 Hole，也不复活截断尾部 | 同上 |
| PR07C-ORD005 | `bash doc/native-base/logs/pr07c-ord005-008.sh` | 0 | PASS（ord_005 1 passed/0 failed；runtime io 12 passed/0 failed；write 29 passed/0 failed；native lib 963 passed, 221 ignored, 0 failed） | [pr07c-ord005-008.log](logs/pr07c-ord005-008.log) |
| PR07C-ORD008 | 同上 | 0 | PASS（reordered_upload 1 passed/0 failed；write 29 passed/0 failed；native lib 963 passed/0 failed） | 同上 |

补充（PR07C · ORD-005/ORD-008）：fsync 边界此前存在一个真实的静默丢弃缺陷。
`NativeDataRuntime::fsync` 会在结尾把 `ticket <= boundary` 的 pending mutation 全部出队，
但 overlay 的 `drain()` 遇到前序操作仍是 `Accepted`（例如上传失败后停留在该状态）时
只会在队首 `break`，既不提交也不报错；`commit_ticket` 因此看到空报告并返回成功，
于是 fsync 返回 Ok 却把这条写入丢掉了，同时该 inode 之后的所有操作都被卡在
队首永远无法提交。修复分两点：

- `WriteOverlay::retry_incomplete(inode)`：重试半途操作（`Accepted` 重新 dispatch、
  缺 receipts 的补 `complete_upload`）。注册与上传对同一身份幂等，且复用原 ticket，
  因此 inode 的注册顺序不变——这就是"等待依赖链"而不是"跳过"。
- `WriteOverlay::incomplete_count(inode)`：fsync 在消费 pending 之前断言该 inode
  没有 `Accepted`/`Uploaded` 残留；仍存在时返回 `Fsync` 错误并保留 pending，
  重试可继续（`ord_005_incomplete_predecessor_fails_fsync_and_is_not_skipped`
  验证失败时 pending 仍为 2、committed view 为空，修好 sink 后重试逐字节一致）。

ORD-008 由既有 `reordered_upload_cannot_overtake_its_predecessor` 与
`failed_predecessor_blocks_successors_instead_of_skipping_them` 覆盖：乱序完成的
上传不得抢跑，drain 只按 admission order 提交且不扩张计划（committed=2、
committed_order=2、两个 extent）。
| PR07-CLIPPY | `cargo clippy -p brewfs --features native-packed-base --lib` | 0 | PASS（仅 4 条存量 warning，无新增） | 同上 |
| PR07-GITDIFF | `git diff --check`（Windows 侧） | 0 | PASS | — |

聚焦反例覆盖：header roundtrip 与 CRC fail-closed、未知 control_version /
required_features 拒绝、namespace 路径逃逸拒绝、create-only 幂等性、
默认 feature 下 native 模块不编译。

仍如实 NOT_RUN：真实 FUSE 挂载测试（READ-001/WRITE-001/002/004/005/010 等需
真实后端 + FUSE 设备的集成场景）、Redis/TiKV durability、S3 实际上传。
`native_base::runtime::io::tests` 目前只覆盖组件级语义（内存 control store +
内存 sink + immutable baseline）：WRITE-002/004/005 与 READ-001 的逻辑已被 11
个场景逐字节验证，但没有真实 FUSE/Redis/TiKV/S3 参与，因此这些条目在验收矩阵
中保持 `SPECIFIED_NOT_IMPLEMENTED`，待接入真实后端后补测。

### PR07D · 发布/准入门：不可发布配置的明确拒绝

工具链：同 PR01（WSL Ubuntu-24.04，rustc 1.98.1）。实现分布：

- `src/config.rs`：`NativeBaseFileConfig::validate_p1` 是 native 卷的配置准入门
  （release_profile / format_version / namespace_mode / packing / reader /
  cache / prefetch / durability / retention / cleanup / repack / limits），
  配合 `#[serde(deny_unknown_fields)]` 让旧选项与隐藏开关直接失败关闭。
- `src/native_base/lifecycle/options.rs`：`reject_legacy_retention_options`
  对 `retention_ttl_seconds` / `published_gc` / `read_retention_leases`
  返回 `InvalidState`。
- `src/native_base/lifecycle/seal.rs`：`mark_candidate_verified` 在
  `contains_new_remote_writes=true` 时拒绝 `RemoteDurability::NoopTestBarrier`。
- `src/native_base/lifecycle/cleanup.rs`：`CleanupCapabilities::validate`
  要求已验证的 unversioned / 未上锁 delete 能力（私有清理准入）。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR07D-FOCUSED | `bash doc/native-base/logs/pr07d-admission-gates.sh` | 0 | PASS（config::tests::native 7 passed/0 failed；bounded_drain_rejects_forgery_and_noop_write_barrier_then_recovers 1 passed/0 failed；retired_retention_options_fail_closed_without_claiming_runtime_wiring 1 passed/0 failed） | [pr07d-admission-gates.log](logs/pr07d-admission-gates.log) |
| PR07D-FMT | `cargo fmt --all --check` | 0 | PASS（同上） | 同上 |
| PR07D-WRITE013 | `cargo test -p brewfs --bins config::tests::native` | 0 | PASS：`native_volume_rejects_commit_before_upload_even_for_s3` —— 即使 `data.backend=s3`，native 卷的 `cache.writeback_mode=commit_before_upload` 在挂载配置校验即被拒 | 同上 |
| PR07D-GATE001 | 同上 | 0 | PASS：`native_release_profile_gate_admits_only_the_shipped_p1_profiles` —— 未发布 profile 被拒；`p1-retained-trial` 打开 cleanup 被拒；limits 层级不一致被拒；合法 retained-trial 配置仍可解析 | 同上 |
| PR07D-GATE004 | 同上 + `cargo test -p brewfs --features native-packed-base --lib native_base::lifecycle::tests::bounded_drain_rejects_forgery_and_noop_write_barrier_then_recovers` | 0 | PASS：`native_durability_profile_must_be_an_explicit_verified_contract` 覆盖 4 类非 verified 耐久性声明；发布路径对 Noop barrier 返回 Durability 错误 | 同上 |
| PR07D-RET021 | 同上 + `cargo test -p brewfs --features native-packed-base --lib native_base::lifecycle::tests::retired_retention_options_fail_closed_without_claiming_runtime_wiring` | 0 | PASS：`native_retention_rejects_retired_ttl_gc_and_lease_options` 证明 `published_gc`/`read_retention_leases`/`retention_ttl_seconds` 均失败关闭 | 同上 |
| PR07D-CLN024 | `cargo test -p brewfs --bins config::tests::native` | 0 | PASS：`native_cleanup_rejects_purge_force_and_age_rules` —— `purge`/`force`/`by-age` 与隐藏的 `by_age_days` 字段全部被拒 | 同上 |

反例覆盖要点：这一组不是"实现了功能"而是"拒绝了未实现的功能"，因此每项都必须
证明拒绝确实发生且不可绕过——`deny_unknown_fields` 让隐藏开关变成硬错误，
`validate_p1` 让品牌式耐久性声明（`trust-the-brand`）与 Noop barrier 无法蒙混，
cleanup 只接受 close 证书 + 已验证 unversioned delete 能力。

仍如实 NOT_RUN：GATE-002/GATE-003 需要的 `required_features` 依赖闭包汇总尚未实现
（当前只拒绝未知位与运行时不支持的位）；真实后端 durability 仍需 compose 环境实测。

### PR07E · 读取资源边界（RES-001…RES-005）

工具链：同 PR01（WSL Ubuntu-24.04，rustc 1.98.1）。这一组证明的是"边界先于
分配/IO 生效"，而不是吞吐优化：

- `src/native_base/seal/plan.rs`：`ReadBudget::reserve` 对单个 unit 的
  encoded/decoded 峰值超出硬上限立即返回 `SealError::BudgetUnitTooLarge`；
  `MAX_PLAN_BLOCKS = 65_536` 在解析任何 unit 之前用 `PlanLimit` 拒绝；
  `BudgetReservation` 的 `Drop` 是唯一的释放路径（成功、失败、取消三条
  路径都走它）。
- `src/native_base/runtime/planner.rs`：`ReadBudget` 的 demand reserve 与
  `FrameSingleflight`（waiter 取消不取消共享 loader）。
- `src/native_base/wire/frame.rs`：`MAX_PLAIN_PAYLOAD = 64 MiB`（decoded）与
  `MAX_NATIVE_OUTER_PAYLOAD = 65 MiB`（encoded 外壳，含 4B persisted header）。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR07E-FOCUSED | `bash doc/native-base/logs/pr07e-resource-bounds.sh` | 0 | PASS（seal::tests 32 passed/0 failed；planner::tests 3 passed/0 failed；frame::tests 9 passed/0 failed） | [pr07e-resource-bounds.log](logs/pr07e-resource-bounds.log) |
| PR07E-FMT | `cargo fmt --all --check` | 0 | PASS（同上） | 同上 |
| PR07E-RES001 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::seal::tests` | 0 | PASS：`undersized_budget_rejects_before_any_io`（单 unit 超预算 → `BudgetUnitTooLarge{needed,cap}`，0 次 GET） | 同上 |
| PR07E-RES002 | 同上 | 0 | PASS：`a_plan_beyond_the_segment_bound_is_refused_before_any_io`（`MAX_PLAN_BLOCKS+1` 个 block → `PlanLimit`，0 次 GET、0 令牌占用） | 同上 |
| PR07E-RES003 | 同上 + `-- native_base::runtime::planner::tests` | 0 | PASS：`budget_tokens_follow_the_buffer_lifetime_on_success_error_and_cancel` 与 `singleflight_cancelled_waiter_does_not_cancel_loader` | 同上 |
| PR07E-RES004 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::planner::tests` | 0 | PASS：`prefetch_cannot_consume_demand_reserve` | 同上 |
| PR07E-RES005 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::wire::frame::tests` | 0 | PASS：`native_block_envelope_carries_64mib_decoded_plus_the_persisted_header` | 同上 |

范围说明：这些是同步执行器（PR03/PR10 核心算法）上的组件级证据。PR10 的异步
执行器、Range GET 合并与预取尚未接入真实 reader，因此矩阵中与"真实 FUSE
读取"绑定的条目（READ-001/005/006/007/008、CONS-003 等）仍保持未实现。

### PR07F · ingest 上传验证证据（VFY-001/VFY-004）

工具链：同 PR01（WSL Ubuntu-24.04，rustc 1.98.1）。这一组把 PR05 上传侧的两条
证据契约从"合同级"推进到可执行断言：

- `src/native_base/ingest/upload.rs`：`VerificationEvidence::ServiceValidated
  { composite_sha256 }` 与 `PlannedObject::full_hash` 是两个独立事实。服务端
  composite 只用于确认，写入 `ObjectRef.full_hash` 的永远是本地 sealed hash；
  两者不一致时 `verify_remote` 返回 `RemoteVerificationFailed`。
- `src/native_base/ingest/plan.rs` + `build.rs`：plan 在冻结时持久化 digest，
  resume 时 `plan.digest()` 必须等于 journal 中记录的值，否则
  `IngestError::PlanMismatch`；新 plan 必须由 `Session::record_plan_digest`
  显式登记后才会被接受（旧回执不会混入不同分区的 attempt）。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR07F-FOCUSED | `bash doc/native-base/logs/pr07f-ingest-verification.sh` | 0 | PASS（native_base::ingest 56 passed/0 failed） | [pr07f-ingest-verification.log](logs/pr07f-ingest-verification.log) |
| PR07F-FMT | `cargo fmt --all --check` | 0 | PASS（同上） | 同上 |
| PR07F-VFY001 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::ingest` | 0 | PASS：`service_composite_that_differs_from_the_local_hash_is_refused`、`service_validated_profile_uses_service_facts_not_readback`、`part_checksum_mismatch_is_refused_server_side` | 同上 |
| PR07F-VFY004 | 同上 | 0 | PASS：`resume_with_changed_part_boundaries_is_refused_until_registered`（改写 plan → PlanMismatch、0 对象上传；显式登记后才放行）、`changed_plan_on_resume_is_refused` | 同上 |

仍如实 NOT_RUN：VFY-002/VFY-003 需要的能力探测（服务端只回显 metadata hash、
Create 未指定算法或错误 part 校验）尚未实现——当前 `RemoteVerificationProfile`
的正确性依赖调用方显式选择，尚无"探测未校验即拒绝 ServiceValidatedChecksums"
的运行时门。此外 `MemoryUploadBackend::corrupt_payload` 目前只写入
`corrupt_keys`、没有任何读取点（存量缺口，本轮未改）。

### PR07G · 读取摊薄与元数据专用路径（OPT-002/OPT-004）

工具链：同 PR01（WSL Ubuntu-24.04，rustc 1.98.1）。这一组把 P3 的两条读取路径
契约从"计划"推进到可执行断言，复用已 PASS 的 OPT-003/OPT-005 所在的
`runtime::planner` 基元：

- `src/native_base/runtime/planner.rs`：`FrameSingleflight::get_or_load` 在 100 个
  并发读者请求同一 frame key 时只调用 loader 一次，并让每个等待者
  `Arc::ptr_eq` 到同一个 buffer（OPT-002）。
- `src/native_base/runtime/io.rs`：`Runtime::size`（getattr/readdir 背后的元数据
  入口）只走控制面与基线长度，不触发对象 GET/PUT，也不复制基线 data（OPT-004）。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR07G-FOCUSED | `bash doc/native-base/logs/pr07g-read-amortization.sh` | 0 | PASS（planner 4 passed/0 failed、io 13 passed/0 failed） | [pr07g-read-amortization.log](logs/pr07g-read-amortization.log) |
| PR07G-FMT | `cargo fmt --all --check` | 0 | PASS（同上） | 同上 |
| PR07G-OPT002 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::planner::tests` | 0 | PASS：`one_hundred_concurrent_readers_share_one_fetch_and_decode`（loader 调用次数 == 1；100 个返回 buffer 两两 `Arc::ptr_eq`） | 同上 |
| PR07G-OPT004 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::io::tests` | 0 | PASS：`metadata_only_operations_never_fetch_file_data`（`size` 返回 4096*3；对象 GET/PUT 均为 0，基线 `bytes_read` 为 0） | 同上 |

范围说明（不虚标）：OPT-002 的证据是进程内 singleflight 基元——跨进程
Unix-socket 共享服务仍是 PR11 preview 骨架（`probe_shared_cache()` 恒为 `None`），
因此本项只覆盖"同一消费者进程内的并发读只调度一次 fetch/decode"。

### PR07H · 能力闭包：required_features 必须覆盖内容真实依赖（GATE-002/GATE-003）

工具链：同 PR01（WSL Ubuntu-24.04，rustc 1.98.1）。此前 reader 只拒绝
"本构建不认识的位"，从不校验声明是否覆盖内容：一个 CRC/digest 全部合法、
但 header 只声明 None/plain 的对象，仍会被读出它实际依赖的能力。

- `src/native_base/wire/container.rs`：新增 `FeatureClosure { declared, content }`
  统一汇总依赖闭包——`closure() = declared | content`、`undeclared() = content & !declared`、
  `ensure_declared()` 在未声明时 fail closed。
- `src/native_base/wire/datapack.rs`：`ScrubbedPack::scrub` 在逐帧走查时累计
  content（`payload_format.feature_bit()`、`Codec::Zstd`），走查结束后
  `ensure_declared()`；汇总值挂在 `ScrubbedPack::feature_closure`。
- `src/native_base/seal/reader.rs`：`SealSnapshot::open` 按 `root_codec`（Zstd）与
  external table roots 累计 content 并 `ensure_declared()`，汇总值由
  `SealSnapshot::feature_closure()` 暴露。两者都在任何字节被读出前完成。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR07H-FOCUSED | `bash doc/native-base/logs/pr07h-feature-closure.sh` | 0 | PASS（container 11 / datapack 15 / seal 48 passed，0 failed） | [pr07h-feature-closure.log](logs/pr07h-feature-closure.log) |
| PR07H-FMT | `cargo fmt --all --check` | 0 | PASS（同上） | 同上 |
| PR07H-GATE002 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::wire::datapack::tests` | 0 | PASS：`declared_plain_header_cannot_hide_a_zstd_frame`（合法 CRC/digest 但声明仅 PLAIN 的 Zstd pack 被 UnsupportedFormat 拒绝，报文含 0x2）、`shipped_goldens_declare_their_content_feature_closure`（4 个 golden 的 declared==content==closure、undeclared==0） | 同上 |
| PR07H-GATE002-SEAL | 同上（seal 套件） | 0 | PASS：`undeclared_external_child_is_refused_before_the_seal_is_opened`（去掉 EXTERNAL_INDEX_CHILDREN 声明的 external-root seal 在 `SealSnapshot::open` 处被拒，去掉的位 0x4 出现在报文里） | 同上 |
| PR07H-GATE003 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::wire::container::tests` | 0 | PASS：`feature_closure_requires_the_declaration_to_cover_the_content`（覆盖即放行并汇总为并集；未覆盖时报缺失位）、`set_declared_features_rewrites_the_header_and_repairs_its_crc` | 同上 |

仍未覆盖（不虚标）：`FixedRevisionReader::open_manifest`（Frozen Metadata）与
SnapshotManifest 的 reader 目前只做 `ensure_supported_features()`；其中
SnapshotManifest 强制 `root_codec == None`、PagedInventory 强制
`required_features == 0`，闭包是平凡成立的，而 Frozen Metadata 的 Zstd root
尚未走到 `FeatureClosure` 校验（当前测试夹具只产出 None root）。此缺口留待
后续增量补齐，不并入本项 PASS。

### PR07I · 私有清理判定与记账（CLN-001/CLN-004/CLN-014/CLN-015）

工具链：同 PR01（WSL Ubuntu-24.04，rustc 1.98.1）。这一组不新增机制，而是把
`lifecycle::cleanup` 已有的判定与记账契约钉成可执行断言：

- 候选集只来自 CLOSED 域的冻结库存：ACTIVE 域即使存在被覆盖/失败的对象也不产生
  DELETE 候选（无证书或域行非 CLOSED 都是 `InvalidState`）。
- 冻结库存之后的合法 dispatch（`registration_seq > final_inventory_seq`）被
  `Retention` 拒绝，不会被静默跳过或当成候选。
- 丢失回包（后端 `AlreadyAbsent`）时对象登记为 Deleted 但 `deleted_bytes` 不增加，
  batch journal 保留，重复 apply 返回 `AlreadyComplete` 且不再释放统计。
- 批量删除逐对象核对：部分失败时 `Blocked` + `failed_objects=1`，仅成功对象计入
  `deleted_bytes`；重试只处理失败对象并只计它的字节。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR07I-FOCUSED | `bash doc/native-base/logs/pr07i-private-cleanup.sh` | 0 | PASS（native_base::lifecycle::cleanup 18 passed/0 failed） | [pr07i-private-cleanup.log](logs/pr07i-private-cleanup.log) |
| PR07I-FMT | `cargo fmt --all --check` | 0 | PASS（同上） | 同上 |
| PR07I-CLN001 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::lifecycle::cleanup` | 0 | PASS：`an_active_domain_never_exposes_delete_candidates` | 同上 |
| PR07I-CLN004 | 同上 | 0 | PASS：`a_dispatch_after_the_frozen_inventory_is_refused_not_cleaned` | 同上 |
| PR07I-CLN014 | 同上 | 0 | PASS：`a_lost_delete_response_is_counted_once_and_keeps_its_journal` | 同上 |
| PR07I-CLN015 | 同上 | 0 | PASS：`a_partially_failed_batch_counts_each_object_separately` | 同上 |

仍未覆盖（不虚标）：CLN-007（discard 时仍有 open/read/decoder 任务的 Busy/deadline
排空）、CLN-017（正式记录永久保留但 alias 删除）、CLN-021（hard-limit 前的准入停止）
仍待补；CLN-022/023 属 repack/变体记账，见 PR12 相关条目。


### PR07J · 保留/清理局部性（CLN-007/RET-002/RET-020/RET-023）

工具链：同 PR01（WSL Ubuntu-24.04，rustc 1.98.1）。这一组把"cleaner 只在自己的域内
工作"和"永久保留不因 alias 消失而撤销"钉成可执行断言：

- `CLN-007`：discard 期间域内仍有 open operation 时 close 被 `Durability` 拒绝且域停在
  Draining；缺少 terminal drain proof 同样被拒。没有 close 证书就没有 plan，
  私有对象保持 `Verified`。
- `RET-002`：新 revision 经 `fast_forward` 接管 workspace base 指针后，删除 head/view
  （native 模型中的 alias）不改变任何一条 PublishedRevision 行：两条都逐字节不变且
  仍是 `Forever`。
- `RET-020`：新增 `RecordingStore` 审计夹具。在 400 行无关 fork/head/lease/PublishedRevision
  存在时，`plan_private_cleanup` 的触达恰好是 `close_certificate(domain)`、
  `domain(domain)` 两次 get 与 `objects_prefix(domain)` 一次 scan，零写入、零 `pub/` 读取。
- `RET-023`：两个 origin 域的 receipt.evidence_root 各自等于本域 RetainBatch 根，
  只用本域字节即可枚举本域对象，不含对方的对象，也不含自身索引对象；verified_subset_digest
  绑定本域子集。证据对象不被删除由同一日志内的
  `close_freezes_domain_and_cleanup_protects_retained_object` 覆盖。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR07J-FOCUSED | `bash doc/native-base/logs/pr07j-retention-locality.sh` | 0 | PASS（native_base::lifecycle:: 32 passed/0 failed） | [pr07j-retention-locality.log](logs/pr07j-retention-locality.log) |
| PR07J-FMT | `cargo fmt --all --check` | 0 | PASS（同上） | 同上 |
| PR07J-CLN007 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::lifecycle::` | 0 | PASS：`an_open_operation_blocks_the_close_before_any_private_delete` | 同上 |
| PR07J-RET002 | 同上 | 0 | PASS：`replacing_the_latest_alias_keeps_the_old_published_revision` | 同上 |
| PR07J-RET020 | 同上 | 0 | PASS：`cleanup_never_scans_the_published_history` | 同上 |
| PR07J-RET023 | 同上 | 0 | PASS：`each_domain_receipt_enumerates_only_its_own_evidence` | 同上 |

范围说明（不虚标）：native 模型没有旧版 `latest`/`alias` KV 行，RET-002 的
"latest/alias" 落在 workspace 的 head+view 指针上，测试以删除该指针模拟 alias
消失；CLN-022/023 的 repack 记账仍待 PR12 侧补齐。


### PR07K · 上传校验能力探测（VFY-002/VFY-003）

工具链：同 PR01（WSL Ubuntu-24.04，rustc 1.98.1）。此前
`RemoteVerificationProfile::ServiceValidatedChecksums` 的正确性完全依赖调用方自觉：
没有运行时门证明后端**真的**校验过校验和。

- `src/native_base/ingest/backend.rs`：新增 `ChecksumValidation{Verified,Unverified,Inconclusive}`
  与 `UploadBackend::probe_checksum_validation`（默认 `Inconclusive`，即 fail closed）。
  `MemoryUploadBackend::without_checksum_validation()` 模拟"Create 未指定算法/不校验 part"
  且回显声明 hash 的后端（part 校验被跳过、complete 报告 `spec.sha256`、inspect 返回声明值）。
- `src/native_base/ingest/upload.rs`：`UploadExecutor` 在 service-validated profile 下先探测
  （探测用 `key + "\0brewfs-checksum-probe"` 的 scratch 身份，永不占用真实对象键），
  非 `Verified` 一律返回既有的 `IngestError::UnprovableChecksum`；结果按 executor 记忆化，
  一个 executor 只探测一次。`ExactReadback` 不受影响。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR07K-FOCUSED | `bash doc/native-base/logs/pr07k-checksum-capability.sh` | 0 | PASS（native_base::ingest 59 passed/0 failed） | [pr07k-checksum-capability.log](logs/pr07k-checksum-capability.log) |
| PR07K-FMT | `cargo fmt --all --check` | 0 | PASS（同上） | 同上 |
| PR07K-VFY002 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::ingest` | 0 | PASS：`echoing_metadata_hash_is_detected_and_never_becomes_remote_verified` | 同上 |
| PR07K-VFY003 | 同上 | 0 | PASS：`service_validated_profile_is_refused_without_checksum_validation`、`honest_backend_probe_is_verified_and_memoized` | 同上 |

范围说明（不虚标）：这是组件级的能力门；真实 S3/RustFS 的能力探测（例如对 S3 的
`x-amz-checksum-*` 与 CreateMultipartUpload 算法的实测）仍需真实对象后端，未在
本项冒充。`MemoryUploadBackend::corrupt_keys` 仍然只有写入点、没有读取点（存量缺口）。


### PR07L · 密封读视图的 locator 解析与固定（READ-005/READ-006）

工具链：同 PR01（WSL Ubuntu-24.04，rustc 1.98.1）。此前 seal reader 有两个真实缺口：
`ChildRef::Local` 一律按"当前 seal 自己的字节"解析，因此**外部页的 local child**
（多级外部索引）会被错误地按本地容器寻址；同时每次 lookup 都重新取页，同一个读视图
在容器字节变化后可能把两个 revision 混进一次读。

- `src/native_base/seal/reader.rs`：`read_page` 现在返回"该页 local child 所在容器"
  （`None` = 本 seal，`Some(ObjectRef)` = 经 locator 到达的外部容器），`lookup` 与
  `for_each_entry` 逐级传递该上下文；寻址与边界检查在查缓存之前完成，以 stored digest
  为键的 `PageKey` 缓存保证同一视图内**每页只取一次**，容器被替换时要么命中已认证的旧页、
  要么在 digest 处失败关闭。
- `src/native_base/seal/tests.rs`：新增 2 层外部 Frames 表 fixture（96 个 block、
  `with_leaf_target(1024)`），并给计数后端加"第 N 次 GET 起换成另一份字节"的容器切换钩子。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR07L-FOCUSED | `bash doc/native-base/logs/pr07l-seal-view-pinning.sh` | 0 | PASS（native_base::seal::tests 35 passed/0 failed） | [pr07l-seal-view-pinning.log](logs/pr07l-seal-view-pinning.log) |
| PR07L-FMT | `cargo fmt --all --check` | 0 | PASS（同上） | 同上 |
| PR07L-READ005 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::seal::tests` | 0 | PASS：`nested_external_index_pages_are_located_and_loaded`（外部页 local child 按 locator 解析；每页只取一次；不可加载的页是错误而非 Ok(None)/零填充） | 同上 |
| PR07L-READ006 | 同上 | 0 | PASS：`a_pinned_view_never_mixes_revisions_across_a_root_switch`（切换后固定视图仍返回全旧字节且不再取页；切换后新开的视图以 HashMismatch 失败关闭） | 同上 |

范围说明（不虚标）：页缓存是**读视图级**的（`SealReader` 每次读新建），不是跨请求缓存；
跨请求 shared cache 仍是 PR11 preview（`probe_shared_cache()` 恒为 `None`）。READ-005/006
的验收是组件级（seal reader + 进程内对象源），真实对象后端的分片/延迟 GET 等价性仍需 P3 集成门。


### PR07M · 读路径组合与并发身份（READ-001/READ-007/READ-008）

工具链：同 PR01（WSL Ubuntu-24.04，rustc 1.98.1）。本组把三条此前只有"合同级"的
读取契约变成可执行断言：

- `src/native_base/runtime/io.rs`：测试新增 `PackedSealBase`——baseline 是真实
  `.brfds`（`SealBuilder` + `DataPack` + `SealReader::read_range`）而不是字节数组；
  在同一 inode 上叠 loose patch 与 hole，全量与跨边界局部读都与 oracle 逐字节一致
  （READ-001）。
- `src/native_base/runtime/planner.rs`：新增 `ReadUnit::flight_key()` / `FrameKey`，
  把 namespace 显式写进可缓存身份；`coalesce_ranges` 与 singleflight 都以该身份为准
  （READ-007）；并新增"并发 scatter 中取消 4/8 个等待者"的测试，证明被取消者既不
  持有 buffer 引用、也不会成为 loader，已完成条目继续服务后续读者（READ-008）。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR07M-FOCUSED | `bash doc/native-base/logs/pr07m-runtime-read-composition.sh` | 0 | PASS（runtime io 14 passed/0 failed、planner 6 passed/0 failed） | [pr07m-runtime-read-composition.log](logs/pr07m-runtime-read-composition.log) |
| PR07M-FMT | `cargo fmt --all --check` | 0 | PASS（同上） | 同上 |
| PR07M-READ001 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::io` | 0 | PASS：`packed_baseline_loose_patch_and_hole_read_back_byte_exact` | 同上 |
| PR07M-READ007 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::planner` | 0 | PASS：`the_same_object_id_in_two_namespaces_shares_nothing` | 同上 |
| PR07M-READ008 | 同上 | 0 | PASS：`a_cancelled_scatter_waiter_leaves_no_buffer_reference_or_waiter` | 同上 |

范围说明（不虚标）：READ-001 的 baseline 是进程内 `.brfds` + 进程内 ObjectSource，
没有真实对象后端的延迟/分片 GET；READ-007 的 namespace 隔离在 planner 身份层完成，
跨进程共享缓存（PR11 preview）仍未启用；READ-008 的取消是 tokio 任务级取消，
FUSE 层 request 级取消（D-state/teardown）不在本项证据内。


### PR07N · 保留闭包、固定只读基线与私有配额（RET-008/RET-011/CLN-017/CLN-021）

工具链：同 PR01（WSL Ubuntu-24.04，rustc 1.98.1）。本组把"谁被永久保留、只读基线花了
多少次读写、alias 消失后还能删谁、配额到顶后还能准入什么"四件事变成可执行断言：

- `src/native_base/lifecycle/retention.rs`：`CandidateClosure` 新增
  `transitive_containers`，`prepare_retention` 新增四类 fail-closed 校验（容器必须声明、
  必须属于 I(d)、必须是容器 kind、manifest 不得重复声明），并把 manifest、inventory 容器、
  外部 meta 索引页、嵌套 inventory 容器一起计入 RetainBatch（RET-008）。
- `src/native_base/frozen/mod.rs`：`FixedRevisionReader` 新增真实
  `object_read_count()`（每层恰好 1 页；每页 = header + stored 两次 range 读），
  测试用计数 `ObjectSource` 证明三个独立 reader 的计数完全相同且 `metadata_rpc_count()==0`；
  P1 侧用计数 `ControlStore` 证明固定只读基线每次 head 解析恰好 1 次 KV namespace 读、
  0 事务、0 租约 RPC，legacy `read_retention_leases` 在任何 RPC 构造前被拒（RET-011）。
- `src/native_base/lifecycle/cleanup.rs`：新增 `PrivateQuota`/`QuotaFence::plan_fence`
  纯函数；测试证明 hard limit 附近 headroom=0 立即 fenced、边界不前移、已接受 ticket 仍通过
  `seal::validate_plan`（`pub(crate)`）而超界 ticket 被拒、已接受 publish 仍能完成 close
  commit（CLN-021）；同模块新增 alias 删除测试，删除 workspace head/view 前后
  `plan_digest` 与候选集逐字节相同（CLN-017）。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR07N-FOCUSED | `bash doc/native-base/logs/pr07n-retention-quota.sh` | 0 | PASS（5 个聚焦测试各 1 passed/0 failed） | [pr07n-retention-quota.log](logs/pr07n-retention-quota.log) |
| PR07N-FMT | `cargo fmt --all --check` | 0 | PASS（同上） | 同上 |
| PR07N-RET008 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::lifecycle::tests::retention_keeps` | 0 | PASS：`retention_keeps_the_manifest_and_every_transitive_container_permanently` | 同上 |
| PR07N-RET011 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::frozen::tests::fixed_readonly_baseline` | 0 | PASS：`fixed_readonly_baseline_page_counts_are_fixed_and_lease_free` | 同上 |
| PR07N-RET011-P1 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::lifecycle::tests::fixed_readonly_baseline` | 0 | PASS：`fixed_readonly_baseline_counters_are_fixed_and_lease_free` | 同上 |
| PR07N-CLN017 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::lifecycle::cleanup::tests::a_deleted_alias` | 0 | PASS：`a_deleted_alias_never_widens_the_cleanup_candidate_set` | 同上 |
| PR07N-CLN021 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::lifecycle::cleanup::tests::quota_fence` | 0 | PASS：`quota_fence_stops_new_admission_and_keeps_the_accepted_allowance` | 同上 |

范围说明（不虚标）：`transitive_containers` 目前由调用方（未来的 build/ingest 侧）
声明，`prepare_retention` 尚未接入真实 publish 事务；`PrivateQuota::plan_fence` 是纯函数，
volume 级 used/reserved 字节的采集与 runtime 常量接入属于后续集成；CLN-017 的 "alias"
在本地测试中是 workspace head + view 记录，真实 KV 命名空间（Redis/TiKV）上的 alias
删除走同一 `plan_private_cleanup` 入口，但未在本机多后端复跑。

### PR07O · 显式变体记账与 close 证据证书（CLN-022/CLN-023/CLN-025）

工具链：同 PR01（WSL Ubuntu-24.04，rustc 1.98.1）。

- `src/native_base/lifecycle/variant.rs`：`LayoutVariantRequest` 新增 `source_bytes`，
  `LayoutVariantPlan` 新增 `source_bytes`/`permanent_bytes`，使新增空间与旧正式 Pack
  足迹分开列账（`permanent_bytes = source_bytes + added_bytes`，溢出即
  `LimitExceeded`）；新增 `abandoned_variant_outputs`，把"未发布变体的 build 域
  只能清自身未保留输出"编码为集合运算（CLN-022/CLN-023）。
- `src/native_base/lifecycle/cleanup.rs`：新增 `CloseEvidenceBudget` /
  `CloseEvidenceUsage` / `account_close_evidence`，并在 `close_domain_commit`
  计算完 C(d) 后、写证书前调用：证据对象必须是容器 kind（3..=5），
  DataPack/DataSeal 一律拒绝，objects/bytes 逐项累计并施加证据 quota
  （默认 4096 objects / 64 MiB）（CLN-025）。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR07O-FOCUSED | `bash doc/native-base/logs/pr07o-variant-evidence-close.sh` | 0 | PASS（variant 4 passed/0 failed、close_evidence 1 passed/0 failed） | [pr07o-variant-evidence-close.log](logs/pr07o-variant-evidence-close.log) |
| PR07O-FMT | `cargo fmt --all --check` | 0 | PASS（同上） | 同上 |
| PR07O-CLN022 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::lifecycle::variant` | 0 | PASS：`explicit_variant_books_new_space_separately_and_keeps_old_packs` | 同上 |
| PR07O-CLN023 | 同上 | 0 | PASS：`an_unpublished_variant_cleans_only_its_unretained_outputs` | 同上 |
| PR07O-CLN025 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::lifecycle::cleanup::tests::close_evidence` | 0 | PASS：`close_evidence_is_independent_quota_checked_and_never_data` | 同上 |

范围说明（不虚标）：`source_bytes` 由调用方提供（尚无真实字节统计接入），
`abandoned_variant_outputs` 是纯函数，未接入真实 build 域终结事务；证据 quota
的默认值是可配置常量，接入运维配置（spec 15 的运行时常量面）属于后续工作；
close 证据端到端路径在内存后端验证，未在 Redis/TiKV 上复跑。

### PR07P · Frozen 冷属性驱逐、COW 页复用与摘要扫描计数（FROZEN-005/006/008）

工具链：同 PR01（WSL Ubuntu-24.04，rustc 1.98.1）。实现位于
`src/native_base/frozen/mod.rs`。

- `FrozenAttributeCache`：有界 LRU 属性缓存，可观测 hits/misses/evictions；
  冷条目被驱逐后从已认证页重载，结果与驱逐前逐字段一致（含 mode/uid/gid
  与权限位），负查询不缓存，capacity=0 时不缓存但答案不变（FROZEN-005）。
- `PageSlot` + `plan_page_reuse` + `ensure_page_overwrite_is_safe`：把 COW
  元数据重写拆成"复用页 / 必须新写页 / 仅旧快照仍引用页"三组，新快照的页
  闭包必须被"复用 ∪ 新写"完整覆盖；对仍被旧快照引用的页做原地覆盖直接拒绝
  （FROZEN-006）。
- `MetaScanCounters` + `account_meta_scan`：摘要扫描分别统计扫描量与重写量
  （页数与字节数），重写必须是扫描过的页的子集，否则失败关闭（FROZEN-008）。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR07P-FOCUSED | `bash doc/native-base/logs/pr07p-frozen-cold-cow-scan.sh` | 0 | PASS（frozen 15 passed/0 failed，含 3 个新增测试） | [pr07p-frozen-cold-cow-scan.log](logs/pr07p-frozen-cold-cow-scan.log) |
| PR07P-FMT | `cargo fmt --all --check` | 0 | PASS（同上） | 同上 |
| PR07P-FROZEN005 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::frozen::tests::cold_attribute_eviction` | 0 | PASS：`cold_attribute_eviction_reloads_identical_attributes_and_permissions` | 同上 |
| PR07P-FROZEN006 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::frozen::tests::cow_page_reuse` | 0 | PASS：`cow_page_reuse_keeps_the_old_snapshot_readable_and_the_new_closure_complete` | 同上 |
| PR07P-FROZEN008 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::frozen::tests::summary_meta_scan` | 0 | PASS：`summary_meta_scan_counts_scanned_and_rewritten_volume_separately` | 同上 |

范围说明（不虚标）：`FrozenAttributeCache` 是进程内 LRU，未接入真实内存
预算/全局缓存服务；`plan_page_reuse` 的页槽由调用方枚举（测试里从真实
BNPG 树遍历得到），尚未接入真正的 COW 写入事务；`account_meta_scan` 是
纯记账函数，摘要扫描的触发时机与重写决策属于后续 P2 集成。

### PR08B · Plain 共享 frame：切片逐文件正确、decoded 预算按 frame 只算一次（OPT-001/INV-04）

工具链同 PR07P。本步**没有改动产品代码**：spec 03 §3 的“两个小文件共享 frame”与 spec 06 §7 的
“已解码 Bytes 由 inflight 转入 cache 时转移同一份 accounting，不可漏算或双算”已由既有 seal 计划器
（`UnitKey::Frame` 去重、`decoded_payload_bytes` 记账）实现，缺的是把它放到“一个小文件聚合 frame
真的被多个**文件**共享”的形状上验证。新增 `src/native_base/seal/tests.rs` 的
`shared_plain_frame_fixture()` 与两个用例。

核心改动（测试）：
- `shared_plain_frame_fixture()`：一个 PlainBytes frame 的 raw 把两个小文件的字节**交错**排列
  （`A0|B0|A1|B1`）；file A（slice `0x31`）用两个 span 指向该 frame 的 `[0,|A0|)` 与
  `|A0|+|B0|` 起的区间，file B（slice `0x32`）用另外两个 span 指向剩下两段。span 在 frame 内的
  raw offset 刻意不相邻，所以“把 block offset 当 raw offset”的实现必然读出错误字节。
- `one_plain_frame_serves_two_files_without_mixing_their_slices`：file A 的整块读逐字节等于
  `A0||A1`（不含 B 的字节），file B 同理；file A 内偏移 3/长度 6 的部分读落在第一个 span 内、
  偏移 11/长度 12 的读跨越 span 边界，都与文件字节逐字节一致；4 次读共 7 个 span，只有 4 次
  Range GET（每次读 1 次，从不按 span 计）。
- `decoded_budget_for_a_shared_plain_frame_counts_the_frame_once`：`decoded_payload_bytes` 每次读
  恰好等于该 frame 的 `raw_len`（不按 span 累加、不按引用文件重复计入）；`ReadBudget::new(
  FRAME_HEADER_LEN + stored_len, raw_len)` 恰好放行且读后 `inflight() == (0, 0)`；decoded 上限
  少 1 字节时在任何 I/O 之前 `BudgetUnitTooLarge`，`source.gets()` 不变、令牌不泄漏。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR08B-FOCUSED | `bash doc/native-base/logs/pr08b-plain-shared-frame.sh` | 0 | PASS（两个用例各 1 passed/0 failed） | [pr08b-plain-shared-frame.log](logs/pr08b-plain-shared-frame.log) |
| PR08B-FMT | `cargo fmt --all --check` | 0 | PASS（同上） | 同上 |
| PR08B-OPT001 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::seal::tests` | 0 | PASS（seal 38 passed/0 failed） | 同上 |
| PR08B-GATE | `bash doc/native-base/logs/ci-gate.sh` | 0 | PASS（workspace 988 + 730 passed；clippy/fmt/feature checks 全绿） | [pr08b-ci-gate.log](logs/pr08b-ci-gate.log) |

范围说明（诚实）：本步验证的是 **read plan 的切片与解码记账**（fake counting backend，无真实
S3/网络），不是 spec 06 §5 里“跨进程共享缓存服务”的端到端启用：`cache::probe_shared_cache` 仍是
preview-only 占位（返回 `None`），`shared-host-cache` feature 未启用。因此本项**不**声称同机
Unix-socket 共享服务已上线；它证明的是 P3 “Plain shared frame”本身——一个 frame 被多个文件共享时
切片正确、解码字节按 frame 只计一次，也就是那个服务将来要复用的同一份 plan 语义。

### PR08A · 训练模式 sampler hints 保持样本集合与顺序契约（OPT-006/INV-02）

工具链同 PR07P。新增 `src/native_base/runtime/sampler.rs`（约 370 行，含 5 个测试）并在
`src/native_base/runtime/mod.rs` 导出 `Sample`、`SampleHint`、`SampleIssuePlan`、`SamplerError`、
`plan_sample_issue_order`。规范依据是 spec 06 §8：训练模式可接受显式 sample/byte-range hints，
但必须保持 sampler 的语义顺序，不得为了顺序 I/O 改变样本集合或分布；应用最终读取顺序与底层请求
发出顺序可以不同。

核心改动：
- `Sample { file, offset, len }` 是一次抽样；`plan_sample_issue_order(draws, hints)` 返回
  `SampleIssuePlan { samples, semantic_order, issue_order, hinted_draws }`，其中 `semantic_order`
  **恒为 `0..n`**（应用看到的就是 sampler 的顺序），`issue_order` 是它的一个排列。hints 只能重排。
- `SampleHint::SequentialFile { file }`：只重排该文件占用的那些槽位（按 `(offset, len)` 升序），
  其他文件的槽位不动；同一文件的重复 hint 幂等。
- `SampleHint::Prioritise { draw }`：把指定抽样按 hint 顺序提到队首，其余保持相对顺序；重复 hint 幂等；
  `draw >= n` 直接 `SamplerError::HintOutOfRange`。
- `SampleHint::ByteRange { file, offset, len }`：对样本集合的**断言**——必须与某个抽样在三个坐标上
  完全一致，否则 `SamplerError::HintNotASample`；它不会新增、裁剪或替换任何样本。
- `SampleIssuePlan::covers_every_draw()`（每个抽样恰好在 issue order 里出现一次）在构造处
  `debug_assert!`，并在全部用例中显式断言；`issued_bytes()` 统计发出字节。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR08A-FOCUSED | `bash doc/native-base/logs/pr08a-training-sampler-hints.sh` | 0 | PASS（sampler 5 passed/0 failed） | [pr08a-training-sampler-hints.log](logs/pr08a-training-sampler-hints.log) |
| PR08A-FMT | `cargo fmt --all --check` | 0 | PASS（同上） | 同上 |
| PR08A-OPT006 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::sampler::tests` | 0 | PASS（重排/幂等/多重集不变/重复抽样不合并/两类拒绝） | 同上 |
| PR08A-GATE | `bash doc/native-base/logs/ci-gate.sh` | 0 | PASS（workspace 986 + 730 passed；clippy/fmt/feature checks 全绿） | [pr08a-ci-gate.log](logs/pr08a-ci-gate.log) |

范围说明（诚实）：本步交付的是**计划器**，不是端到端的训练读取模式：`plan_sample_issue_order` 只决定
“哪些抽样以什么次序发出”，没有接入真实 FUSE/后端的预取执行器，也没有测量顺序化带来的吞吐收益（那属于
性能 A–F / PR13，仍未开始）。`Semaphore`/字节预算与“预取只能用剩余额度”的背压（spec 06 §7）由既有
planner 预算记账覆盖，本步不重复实现；`hotset` 提示（绑定 LogicalRevision/StorageView）仍未实现。

### PR07Z · lossless repack 保持逻辑身份（OPT-007/INV-10）

工具链同 PR07P。本步**没有改动产品代码**：spec 10 §9 的“保持 BlockKey、decoded_len/content_hash、LogicalRevision”
在 `lifecycle::variant::validate_layout_variant`（身份不等即拒绝）与 seal reader（整块读校验 binding 的 content_hash）
里已经成立，缺的是把这两端钉在一次真实 repack 上的显式证据。因此在 `src/native_base/seal/tests.rs` 新增
`lossless_repack_keeps_bindings_and_logical_revision`（含 `table_digest` 辅助函数）。

核心证据：
- 物理上真的是 repack：源布局是“整块 = 一个 Pack 的一个 frame、一个 span”，候选布局是“同一个块 = **新** Pack 的两个
  frame、两个 span”。`TableId::Placements/Objects/Frames` 三张表在两侧的 digest 全部不同，seal 对象本身也不同。
- 逻辑身份不变：`TableId::Bindings` 的 digest 两侧逐字节相同，即每个 `(slice_id, block_index)` 的
  `decoded_len`/`content_hash` 未动——这是 seal 逻辑 revision 的定义面。
- 内容一致且旧对象不受影响：候选视图读新 Pack（按 frame 各一次 range 取读，共 2 次），同一块字节与源视图一致；
  候选视图对旧对象的 GET 计数为 0（既不重读也不改写），源视图随后仍能从自己的对象读出同样字节。
- 变体记账一致（spec 10 §9）：用真实的 Bindings digest 作为 `LayoutIdentity` 交给 `validate_layout_variant`，
  同一身份被接受，`permanent_objects` 含旧 Pack、`added_objects` 含新 Pack，`permanent_bytes == source_bytes +
  added_bytes`，即新空间叠加在未改动的源之上、不报告净回收。
- 反向用例（规范“不能只比较 frame checksum”）：故意 `add_binding` 声明**未篡改**块的 binding，却把 placement 指向
  解码出**篡改后**字节的 frame。seal 的 `validate()`/`build()` 全部通过（结构、span 覆盖、frame/object 闭包都没问题，
  且每个 frame 自身完好），读取仍在 `SealError::Integrity("block (7,0) content hash mismatch")` 处失败——证明校验是
  对“解码后的整块”做的，而不是对 frame checksum 做的。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR07Z-FOCUSED | `bash doc/native-base/logs/pr07z-lossless-repack.sh` | 0 | PASS（seal lossless_repack 1 passed/0 failed） | [pr07z-lossless-repack.log](logs/pr07z-lossless-repack.log) |
| PR07Z-FMT | `cargo fmt --all --check` | 0 | PASS（同上） | 同上 |
| PR07Z-OPT007 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::seal::tests::lossless_repack_keeps_bindings_and_logical_revision` | 0 | PASS（Bindings 不变、物理三表改变、旧对象 0 GET、变体记账、content-hash 反向拒绝） | 同上 |
| PR07Z-GATE | `bash doc/native-base/logs/ci-gate.sh` | 0 | PASS（workspace 981 + 730 passed；clippy/fmt/feature checks 全绿） | [pr07z-ci-gate.log](logs/pr07z-ci-gate.log) |

范围说明（诚实）：本步是 **component-level** 的证据，不是一次真实的多 Pack 离线 repack 作业：测试里源/候选两个 seal 都是
由 `SealBuilder` 在内存中构造的（块很小、单块、无压缩策略变化），没有覆盖多块/大 Pack 的构建成本、`--max-extra-bytes`
预算下的真实构建、以及“显式 apply 与 expected StorageView 冲突”的并发路径（后者见 OPT-010 的既有条目）。
按 spec 10 §9，本步只是证明“lossless 变体的逻辑身份不变、旧对象不被改写、校验靠完整解码”这一组契约；真实空间回收
从来不是本版承诺（新旧依赖永久保留）。

### PR07Y · 逻辑迁移 roundtrip：源卷保留、属性与内容一致（REGRESS-003）

工具链同 PR07P。改动集中在 `src/native_base/runtime/migration.rs`（新增 `MigrationReport` 与
`copy_volume_logically`，以及 4 个测试）、`src/native_base/runtime/mod.rs`（导出新 API）与
`src/native_base/write/keys.rs`（新增 `Keys::volume_prefix` 并让 `Keys::new` 复用它，使卷前缀只有一处定义）。

核心改动：
- `copy_volume_logically(store, request)`：先跑既有的 `validate_migration` 准入，再要求 `OfflineCopy`
  模式与显式源 namespace；读取源卷 locator header 得到源 volume id，并拒绝「目标 volume id 等于源 id」
  （否则目标行会与正在被读取的行重叠）。随后 `scan(source_prefix)` 取源卷全部控制行，把每行后缀原样接到
  `target_prefix` 上，与目标 locator header 一起放进**同一个**条件事务（每行 `check_absent`），所以目标
  命名空间要么完整出现、要么完全不出现。函数不持有 `ObjectSink`：逻辑迁移不上传、不重读、不重校验任何
  数据字节，数据对象靠 content-addressed 身份共享。
- `MigrationReport`：源/目标 namespace 与 volume id、`copied_rows`、`copied_bytes`（仅控制面字节）。
- 新增错误：`CopyRequiresOfflineCopy`、`VolumeIdReuse`、`ScanOutsideVolumePrefix`、`TargetVolumeNotEmpty`
  （`StoreError` 直接 `#[from]`；事务里的 `Conflict` 映射为 `TargetVolumeNotEmpty`）。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR07Y-FOCUSED | `bash doc/native-base/logs/pr07y-logical-migration-roundtrip.sh` | 0 | PASS（migration 4 passed/0 failed） | [pr07y-logical-migration-roundtrip.log](logs/pr07y-logical-migration-roundtrip.log) |
| PR07Y-FMT | `cargo fmt --all --check` | 0 | PASS（同上） | 同上 |
| PR07Y-ROUNDTRIP | `cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::migration::tests::a_logical_copy_retains_the_source_and_reproduces_attributes_and_content` | 0 | PASS（源保留 + 属性/布局孪生行 + 目标 reader 同 size/字节） | 同上 |
| PR07Y-REFUSALS | `cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::migration::tests::a_logical_copy_refuses_a_reused_id_a_moving_mode_and_a_taken_target` | 0 | PASS（VolumeIdReuse / CopyRequiresOfflineCopy / NamespaceExists / TargetVolumeNotEmpty） | 同上 |
| PR07Y-FAILCLOSED | `cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::migration::tests::a_logical_copy_fails_closed_on_a_store_that_leaves_the_volume_prefix` | 0 | PASS（ScanOutsideVolumePrefix，且不安装目标 header） | 同上 |
| PR07Y-GATE | `bash doc/native-base/logs/ci-gate.sh` | 0 | PASS（workspace 980 + 730 passed；clippy/fmt/feature checks 全绿） | [pr07y-ci-gate.log](logs/pr07y-ci-gate.log) |

范围说明（诚实）：这是 **component-level** 的逻辑迁移，不是挂载级迁移工具：`copy_volume_logically` 在
一个事务里重发全部控制行，因此大卷的批处理/续传、迁移进度与限流不在本步范围内；数据对象不搬，所以
「迁移后源与目标共享同一批对象」是本设计的语义而不是缺陷（spec 10 §9：lossless 变体不改旧对象、新旧
依赖永久保留，且不承诺净空间节省）。拷贝不改变 on-disk 格式：两侧 header 都过同一个准入门
（format/schema/control/wire 被钉在编译期常量上），版本前进会在读任何一行之前按 REGRESS-002 拒绝。
测试使用内存控制存储（`MemoryControlStore`），Redis/TiKV 上的同一事务形状未在本环境验证。

### PR07X · feature 关闭的旧路径回归、版本准入拒绝与 KnownGap 记录（REGRESS-001/002/005）

工具链同 PR07P。本步只新增一个测试、一份 KnownGap 记录与一个证据脚本，**没有改动产品代码**：
REGRESS-001 断言的是“native 特性关闭时旧路径不变”，REGRESS-002 断言的是既有准入拒绝
（`RuntimeAdmissionError::FeatureNotCompiled` / `UnsupportedSchemaVersion` / `UnsupportedWireVersion` /
`UnsupportedVolumeFormat`）已经覆盖“旧二进制读新卷”的形状，REGRESS-005 要求的是把 buffered/direct/mmap
记录为 KnownGap 而不是标成 PASS。

核心证据：
- REGRESS-001：`cargo test -p brewfs --no-default-features --features fuse-tokio-runtime --lib` 在同一提交上
  934 passed / 0 failed / 225 ignored，即旧（flat/chunk）路径可独立编译并通过自己的整套 lib 测试；native 开/关
  两种配置的 `cargo check` 由同一 CI gate 覆盖（均 exit 0），因此 native 特性是纯增量而不是替换。
- REGRESS-002：新增 `src/native_base/runtime/header.rs::tests::an_older_binary_or_a_newer_volume_is_refused_without_a_flat_fallback`
  （`NativeVolumeHeader::p1` + `MemoryControlStore`）：① 旧二进制（`native_packed_base: false`）读到 header 行后仍被
  `FeatureNotCompiled("native-packed-base")` 拒绝；② `schema_version += 1` 与 `wire_major += 1` 分别以
  `UnsupportedSchemaVersion(_)` / `UnsupportedWireVersion { .. }` 拒绝；③ `volume_format = "workspace-flat-v1"` 以
  `UnsupportedVolumeFormat(_)` 拒绝，而不是被当作 flat 卷解释；④ 同一记录在 `all_capabilities()` 下 `validate` 通过，
  证明上述拒绝来自读者与版本而不是损坏的 header。该测试在 native 开启与关闭两种配置下都通过。
- REGRESS-005：新增 `doc/native-base/known-gaps.md`，逐形态（buffered page cache / direct O_DIRECT / mmap）给出表现、
  为什么不是 PASS、记录位置与关闭条件，并明确本仓库没有任何一行把它们标为 PASS；形态本身的事实依据是 `AGENTS.md` 的
  Known POSIX And FUSE Limitations（`generic/075` 默认排除、mmap 形状在 direct I/O 下 `ENODEV`、`iogen01` 留在 skip
  list、post-reply invalidation 顺序实验会阻塞 `fsx`）。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR07X-FOCUSED | `bash doc/native-base/logs/pr07x-compat-known-gaps.sh` | 0 | PASS（focused 1 passed；feature-off header 5 passed；feature-off lib 934 passed/0 failed） | [pr07x-compat-known-gaps.log](logs/pr07x-compat-known-gaps.log) |
| PR07X-FMT | `cargo fmt --all --check` | 0 | PASS（同上） | 同上 |
| PR07X-REGRESS001 | `cargo test -p brewfs --no-default-features --features fuse-tokio-runtime --lib` | 0 | PASS（934 passed；0 failed；225 ignored） | 同上 |
| PR07X-REGRESS002 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::header::tests::an_older_binary_or_a_newer_volume_is_refused_without_a_flat_fallback` | 0 | PASS（1 passed；1261 filtered out） | 同上 |
| PR07X-REGRESS002-OFF | `cargo test -p brewfs --no-default-features --features fuse-tokio-runtime --lib -- native_base::runtime::header::tests::` | 0 | PASS（5 passed；1154 filtered out） | 同上 |
| PR07X-REGRESS005 | `grep -n 'generic/075\|iogen01\|ENODEV' AGENTS.md; grep -n 'KnownGap' doc/native-base/known-gaps.md` | 0 | PASS（KnownGap 记录存在且注明非 PASS） | [known-gaps.md](known-gaps.md) / 同上 |
| PR07X-GATE | `bash doc/native-base/logs/ci-gate.sh` | 0 | PASS（workspace 980 + 730 passed；clippy/fmt/feature checks 全绿） | [pr07x-ci-gate.log](logs/pr07x-ci-gate.log) |

范围说明（诚实）：REGRESS-005 在矩阵中保持 **`NOT_RUN`**，不是 PASS——本步交付的是“如实记录为 KnownGap”这件事，
而不是 buffered/direct/mmap 形态本身通过：这三种形状需要真实 FUSE 挂载加 xfstests/LTP harness
（`docker/compose-xfstests/`），本环境无法运行，关闭条件写在 `known-gaps.md`。REGRESS-001 的“无回归”是**测试套件
层面**的证据（旧配置整套 lib 测试通过 + 两种配置 check 通过），不是字节级 dump 对比；REGRESS-002 是 component-level
的准入证据，没有跑真实挂载下的跨版本卷。

### PR07W · 写路径 copy-up 边界、跨块 fresh slice 与重挂持久化（WRITE-001/002/004/005）

工具链同 PR07P。本步**没有改动产品代码**：这四项的语义在 PR07J/PR07U 的运行时与 overlay 里已经成立，缺的是它们各自的显式验收证据。
因此在 `src/native_base/runtime/io.rs` 的测试模块新增 4 个场景（并复用两个既有场景），把它们钉在可复现的断言上。

核心证据：
- WRITE-001：新增 `SparseBase`（size 可设、字节全零、按请求长度记账的不可变稀疏基线）与 `runtime_with_block_size`，在 1 GiB 基线上
  写 4096B 后断言：fsync 只从基线读 4096B、只 put 2 次（1 数据块 + 1 receipts 容器）、get 0 次、只发布 1 条 extent，且 1 GiB 长度、
  头块与尾块都正确。若实现前置复制整文件，读数会接近 1 GiB，测试立即失败。
- WRITE-002：1 MiB 基线上在 block 边界处写 10B，断言提交只读 2 个块、只发布 2 条 extent（runtime 目前按被覆盖块逐个 materialize，
  每块一个 mutation + 各自 receipts 容器，故 put 为 4）、覆盖范围外的块仍读回基线，并做 3 块范围的字节 oracle 比对。
- WRITE-004：两轮 write→read→fsync→read，断言 `pending_count` 1→0 与读回字节在交接前后完全相同；配合既有的
  `patch_is_dirty_visible_and_fsync_reads_only_the_touched_block` 与 PR07U 的 `a_lost_commit_reply_hands_off_without_a_gap`
  （掉包只在确认后释放 pending）。
- WRITE-005：写补丁 + truncate 后各自 fsync、clear_cache，再用同一 store/sink/baseline 构造新 runtime（重挂类比），断言补丁字节、
  两个 size 与 truncate 结果一致，且重挂期间 sink.puts 不增加（只是重新读取卷）。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR07W-FOCUSED | `bash doc/native-base/logs/pr07w-write-path-durability.sh` | 0 | PASS（focused 6 passed/0 failed；runtime::io 24 passed） | [pr07w-write-path-durability.log](logs/pr07w-write-path-durability.log) |
| PR07W-FMT | `cargo fmt --all --check` | 0 | PASS（同上） | 同上 |
| PR07W-WRITE001 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::io::tests::a_first_4k_overwrite_of_a_gib_file_touches_one_block_and_no_more` | 0 | PASS（1 GiB 基线首写只读/写 1 块，无 copy-up） | 同上 |
| PR07W-WRITE002 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::io::tests::a_write_straddling_a_block_boundary_rewrites_only_those_two_blocks` | 0 | PASS（只读 2 块、只发布 2 条 extent、范围外仍读基线） | 同上 |
| PR07W-WRITE004 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::io::tests::a_local_read_sees_the_dirty_bytes_and_then_the_committed_ones` | 0 | PASS（两轮交接 pending 1→0，字节不变） | 同上 |
| PR07W-WRITE005 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::io::tests::fsync_then_cache_clear_and_remount_serves_the_same_bytes_and_sizes` | 0 | PASS（重挂后字节/size 一致，重挂不新上传） | 同上 |

范围说明（诚实）：本项仍是 **component-level** 运行时证据。「重挂」是新建 `NativeDataRuntime`（同一 store/sink/baseline），
不是内核 FUSE unmount/mount；「GiB 文件」是 1 GiB 稀疏基线而不是真实 1 GiB 已填充对象（copy-up 结论只依赖读/写字节数，与基线内容无关）；
WRITE-001 的 4096B 与 WRITE-002 的跨块 10B 都是 block_size=4096 下的单请求形状，没有覆盖多请求并发或 fio 级吞吐验证；
本步未新增产品代码，矩阵状态变化来自「语义已存在 + 本步补上显式证据」，不是新实现。

### PR07V · 失效租约与 authority 回滚下的私有状态收口（CONS-004/005）

工具链同 PR07P。改动集中在 `src/native_base/write/overlay.rs`（发布路径与 `RollbackReport`）、
`src/native_base/write/lease.rs`（`WriterLease`、`StepClock`）、`src/native_base/runtime/io.rs`（运行时栅栏入口），
新增 `src/native_base/write/tests_fence.rs`（4 个显式场景）。

核心改动：
- `WriterLease { grant, clock }`：`WriteOverlay::with_writer_lease` 后，**每一个会发布的步骤**（`dispatch` 里的对象注册与
  `complete_upload` 里的 receipts 容器注册、`drain` 里的提交）都改为 `publish_guard(head)` 取 guard，而不是直接用手上
  的 head 造 guard。租约在 backend 时间过期、或被顶替（`owner_generation != head.writer_generation`）、或时钟源不是
  backend 时，`LeaseFence` 在事务**之前**返回：没有 registry 行、没有 inventory 行、没有 domain 计数推进、没有 head 行。
  无租约的调用方保持 PR04 行为（本地 head guard），因此既有调用点不受影响。
- `RollbackReport` + `WriteOverlay::rollback(last_rollback())`：把写者的私有状态整体收口——在飞的 operation 全部停止、
  它们的 dirty 引用被丢弃（已持有的 capture 仍持有自己的 `Arc` 克隆，因此不受影响）、已落盘但未发布的块折成
  `OrphanReceipt`（`step=DataUploaded` 或 `step=Commit`，WRITE-006 的可回收保护）、inode 本地镜像被丢弃，
  使下一次准入必须从 store 重新派生。`published_rows` 恒为 0：rollback 不写任何 head/inode/extent/binding/placement/mut 行。
- dispatch 中途失效：`dispatch` 的每块上传路径记录已落盘块（`PendingOp.partial`），因此租约在两块之间失效时，第一块既不会
  被重解释成部分提交（operation 仍是 `Accepted`，重试重新派生整块集），也不会失去保护（rollback 把它按 `DataUploaded` 保护）。
- `fenced()`：只有栅栏类错误（`LeaseFence`/`StaleHeadGuard`/`DomainNotActive`）触发自动 rollback，返回的错误变体保持不变
  （调用方仍可按变体分类，清理结果由 `last_rollback()` 观测）；清理本身失败会折进错误文本，因为把持久上传留在无保护状态
  绝不能静默（WRITE-006）。`drain` 在 commit 被租约拒绝时同样收口其余在飞 operation（`DrainReport.rolled_back`）。
- 运行时入口：`NativeDataRuntime::{writer_lease, rollback_writer, last_writer_rollback}` 把同一栅栏暴露给 FUSE 侧的协调器；
  运行时 `fsync` 在租约失效时返回 `LeaseFence`、`head.commit_seq` 与 extent 行不变，失败的 pending 仍归调用方（可 discard）。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR07V-FOCUSED | `bash doc/native-base/logs/pr07v-fenced-writer-rollback.sh` | 0 | PASS（focused 4 passed/0 failed；write 45 passed、runtime 31 passed） | [pr07v-fenced-writer-rollback.log](logs/pr07v-fenced-writer-rollback.log) |
| PR07V-FMT | `cargo fmt --all --check` | 0 | PASS（同上） | 同上 |
| PR07V-CONS004a | `cargo test -p brewfs --features native-packed-base --lib -- native_base::write::tests_fence::an_expired_or_superseded_lease_stops_a_dispatch_before_any_row_is_written` | 0 | PASS（过期与被顶替两种租约都整卷逐字节不变） | 同上 |
| PR07V-CONS004b | `cargo test -p brewfs --features native-packed-base --lib -- native_base::write::tests_fence::a_fence_between_two_blocks_protects_the_uploads_that_already_landed` | 0 | PASS（落盘块 = 1 条 DataUploaded receipt，dirty 被清除） | 同上 |
| PR07V-CONS004c | `cargo test -p brewfs --features native-packed-base --lib -- native_base::write::tests_fence::an_authority_rollback_stops_the_writer_and_cleans_all_private_state` | 0 | PASS（域 Quarantined + rollback：stopped_publishing、3 对象受保护、域行冻结后不再发布） | 同上 |
| PR07V-CONS004d | `cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::io::tests::a_lapsed_writer_lease_fences_the_runtime_and_cleans_its_private_state` | 0 | PASS（运行时 fsync 返回 LeaseFence、head 不动、pending 归调用方） | 同上 |
| PR07V-CONS005 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::write::tests_fence::another_writer_bypassing_the_local_gate_is_refused_by_the_head_guard` | 0 | PASS（stale head guard 拒绝旁路写者；重新派生 head 的写者提交成功） | 同上 |

范围说明（诚实）：本项同样是 **component-level**（真实 overlay + 真实控制平面 store），没有真实 FUSE 挂载、没有 Redis/TiKV
真后端上的租约对象存储（`LeaseGrant` 目前由调用方构造，`StepClock` 是测试用的可控 backend 时钟，真实 fork 的
`sleep`/续租循环未接入）；「另一个 writer」在测试里是同一进程内的第二个 overlay 实例，它证明的是**跨实例不可见的本地门
不能替代持久栅栏**，分布式多进程写者的真实网络分区/时钟漂移仍未覆盖；`fenced` 的自动 rollback 只在栅栏类错误上触发，
普通 `Conflict` 仍交由调用方重试。

### PR07U · 单次请求一致且有界的读捕获（CONS-001/002/003/006）

工具链同 PR07P。改动集中在 `src/native_base/runtime/io.rs`（PR07 的 component-level 读写运行时）。

核心改动：
- `NativeDataRuntime::snapshot_metadata`：一次请求只做一次 metadata 捕获，且顺序固定为先取 `pending_through(boundary)` 快照、
  再读已提交视图。顺序是正确性的一部分：先读 committed 再读 pending 时，落在两步之间的提交既已 handoff（不在 pending）
  又未被第一步看到（committed 视图早于事务），read 会把已返回给客户端的写入读成陈旧内容/空洞；反过来该提交的 extent
  已在 store 里，第二步必定读到（CONS-001/CONS-002）。
- token 复核：`capture_stable_view` 在 extent 扫描**前后**各读一次 inode 行并要求逐字节相同（每次提交都会推进该行，
  因此行相同即窗口内没有提交落地）。不一致即判定为跨代视图并重读；`read_at_boundary` 的 size、committed extents 与
  pending overlay 现在全部来自同一个 `MetadataSnapshot`，size 与 extent 不可能来自两代（CONS-001）。
- `MetadataCapturePolicy { max_attempts, attempt_timeout }` + `NativeIoError::MetadataUnstable`：重试只发生在
  token 变化与 metadata 读超时两种可重试情形，且既有次数上限又有单次墙钟上限，超限后请求失败而不是无限重试或给出跨代结果。
  整个捕获与重试都在 runtime 状态门之外进行（读不会阻塞并发提交）（CONS-006）。
- 观测：`capture_count()`（每请求一次）与 `capture_attempt_count()`（含复核重试），用于证明「一次读 = 一次捕获」与
  「重试有界」；`size()` 也走同一捕获路径，因此 size 与数据一致。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR07U-FOCUSED | `bash doc/native-base/logs/pr07u-capture-consistency.sh` | 0 | PASS（runtime::io 19 passed/0 failed，每 ID 1 passed，CONS-006 2 passed） | [pr07u-capture-consistency.log](logs/pr07u-capture-consistency.log) |
| PR07U-FMT | `cargo fmt --all --check` | 0 | PASS（同上） | 同上 |
| PR07U-CONS001 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::io::tests::a_commit_inside_a_read_capture_is_never_served_stale` | 0 | PASS（故障注入 store 在 extent 扫描中提交并 handoff） | 同上 |
| PR07U-CONS002 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::io::tests::a_lost_commit_reply_hands_off_without_a_gap` | 0 | PASS（事务已应用但回包丢失 → 重试回答 AlreadyCommitted） | 同上 |
| PR07U-CONS003 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::io::tests::one_read_request_reuses_a_single_bounded_capture` | 0 | PASS（3 chunk 跨读 = 1 capture / 1 scan） | 同上 |
| PR07U-CONS006 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::io::tests::an_inode_row_that_never_settles_fails_within_the_bounded_budget native_base::runtime::io::tests::a_metadata_read_beyond_the_attempt_timeout_is_bounded` | 0 | PASS（2 passed：不稳定行与超时均按 max_attempts 有界失败） | 同上 |

范围说明（诚实）：本项是 **component-level** 运行时（`NativeDataRuntime`）而非真实 FUSE 挂载路径上的证据；
一次 read 的边界是「一个内部请求」，不是内核 syscall 级原子（跨 syscall 复用同一 capture 仍属未实现）；
CRITICAL/`attempt_timeout` 的默认值（4 次 / 2s）是策略默认，不是压测结论；metadata plane 的真实超时（Redis/TiKV 客户端级）
仍由后端自身超时与本文的有界预算叠加，未做端到端超时归因。
### PR07T · 上传回执保护、chmod 元数据专用路径与 rename 覆盖（WRITE-006/010/012）

工具链同 PR07P。新增实现位于 `src/native_base/write/orphan_receipt.rs` 与 `src/native_base/write/replace.rs`
（`write/mod.rs` 导出），`write/records.rs` 增加 `InodeAttributes`，`write/keys.rs` 增加 `attr/` 与 `ufo/` 记录键，
`write/commit.rs`/`write/overlay.rs` 扩展 `Mutation`/`MutationSpec`，`write/tests.rs` 增加 store-agnostic 场景并登记到
memory/redis/tikv 三个 lists。

核心组件：
- `OrphanReceipt`（`orphan_receipt.rs`）：`after_kv_failure` 只在确有持久化对象（数据块或 receipts 根）时产出 receipt，
  记录 OperationId / workspace / domain / `KvStep` / 截断后的原因 / receipts 根 / 全部对象；`plan_collection` +
  `ensure_not_collected` 让 cleaner 无法回收未提交对象的任何字节，`resolve_orphan_receipt` 只在状态允许时把它移出受保护集合
  且恰好一次，`record_orphan_receipt` 按 digest 幂等写入 `ufo/<operation_id>`（WRITE-006）。
- `WriteOverlay` 的 drain 失败路径：commit 事务失败或重试后仍失败时，`protect_uncommitted_upload` 把该 operation 折叠为受保护
  orphan receipt（`DrainReport.orphaned`），并保证“保护失败”不会被静默吞掉。回归测试用故障注入 store（拦截含 `mut/`
  写入的事务）走真实 overlay 路径（WRITE-006）。
- `InodeAttributes`（`records.rs`）：`mode/uid/gid/rdev/ctime_ns` 独立成 `attr/<workspace>/<inode>` 行，`validate()` 要求
  `mode` 必须带文件类型位，`permissions()` 只取低 12 位；`Mutation::SetAttributes` 不携带任何 block、`planned_size` 返回
  `None`（不是 0），因此 metadata-only 变更不可能顺手改 size 或 extent（WRITE-010）。
- commit 侧：属性行与内容变更的并发冲突用 `check_bytes`/`check_absent` 显式表达（内容提交会锁住 attr 行），
  非法 mode 在 `plan_mutation` 再次校验；准入侧在分配 `mutation_order` 之前就拒绝裸权限位，
  避免一个被拒输入毒化该 inode 的后续顺序槽（WRITE-010）。
- `Replacement`（`replace.rs`）：`plan_rename_over` 读源/目标两个 view，要求源有持久记录、每个 data extent 的
  `block_count` 与长度自洽，并把每个 block 通过 `bnd/`、`plc/`、`obj/` 解析成携带用的 `CommittedBlock`；
  `cover_whole_file` 要求新 extent 集合无缝隙、无重叠、无零长且恰好铺满 `[0, size)`——缺一段就会让目标残留旧字节，
  因此一律拒绝。`changed_ranges()` 恒为整文件 `(0, size)`，`is_application_complete()` 恒真：rename 覆盖没有
  “最小 patch” 形式，即使两侧同尺寸同内容也一样（WRITE-012）。
- commit 侧 `Mutation::ReplaceInode`：同一事务删除目标全部旧 extent 行（逐行 `check_bytes`）、写入完整携带集合、
  对每个携带 block 的 `bnd/`/`plc/` 行做 compare-then-put、把所有携带 block 纳入 registration 校验与
  `durable_receipts`；`planned_size` 返回完整新 size（对比 `SetAttributes` 的 `None`），因此同尺寸覆盖仍会 +1 `data_version`。
  行为测试断言：目标 extent 集合与源逐条相同（含 Hole）、旧 `ext64` 行消失、目标引用对象 = 源对象且与自身旧对象不相交、
  receipts 解出的条目恰好覆盖全部携带对象、源 inode 完全不变；手写的“缺一段”`ReplaceInode` 在提交边界被拒且整卷快照不变。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR07T-FOCUSED | `bash doc/native-base/logs/pr07t-write-receipts-attributes-replace.sh` | 0 | PASS（write 41 passed/0 failed，每 ID 1 passed + replace 单测 4 passed） | [pr07t-write-receipts-attributes-replace.log](logs/pr07t-write-receipts-attributes-replace.log) |
| PR07T-FMT | `cargo fmt --all --check` | 0 | PASS（同上） | 同上 |
| PR07T-WRITE006 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::write::tests::orphan_protection::a_kv_failure_after_a_successful_upload_protects_the_receipt` | 0 | PASS（故障注入 store，走真实 overlay drain 路径） | 同上 |
| PR07T-WRITE010 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::write::tests::memory_backend::chmod_on_a_large_file_changes_metadata_and_uploads_no_data` | 0 | PASS（场景与 memory/redis/tikv 共享） | 同上 |
| PR07T-WRITE012 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::write::tests::memory_backend::rename_over_publishes_the_complete_file_and_never_a_patch` | 0 | PASS（场景与 memory/redis/tikv 共享） | 同上 |
| PR07T-REPLACE-UNIT | `cargo test -p brewfs --features native-packed-base --lib -- native_base::write::replace::tests` | 0 | PASS（4 passed：覆盖律 gap/overlap/短覆盖/零长一律拒绝、patch 形式不存在） | 同上 |

范围说明（诚实）：rename 覆盖目前发布的是**内容与大小**（extent 集合 + size + data_version），目标 inode 的属性行保持不变
（POSIX rename 替换的是名字而不是 inode 归属）；源 inode 的删除仍由 VFS/lifecycle 侧负责，本轮没有把 unlink 与 replace 合并成一个事务。
rename 覆盖不产生新的数据 PUT（对象已按内容寻址持久化在源 slice 下），但 receipts 会逐个证明这些对象，因此“零上传”不等于“空发布”。
### PR07S · writer lease fence、backend lease time 与 durability 边界

工具链同 PR07P。新增实现位于 `src/native_base/write/lease.rs`（`write/mod.rs` 导出），`write/error.rs` 增加 `LeaseFence` 变体，
`write/tests.rs` 增加 store-agnostic 场景并登记到 memory/redis/tikv 三个 lists。

核心组件：
- `LeaseClock` / `TimeSource` / `FixedClock`：lease 判定必须读取声明为 `Backend` 的时钟；`skewed_client()` 用来表达任意客户端偏差。
  `LeaseGrant::evaluate` 先判 generation（不等即 `Superseded`，不被时钟掩盖），再按 backend 时间判 `now >= deadline`；
  传入非 backend 时钟直接 `LeaseFence` 拒绝（KV-004）。
- `LeaseGrant::guard`：只有 `Held` 才签发 `HeadGuard`；`Superseded`/`Expired` 不产生 guard，因此被 fence 的写入根本进不到 commit（WRITE-007）。
  集成测试走真实 `commit_uploaded_slice` 路径，并断言失败前后整卷快照逐字节相同（无 extent/inode/registration/mutation/head 残留）。
- `DurabilityProfile` + `DurabilityBoundary`：四种 profile 报告 confirmed / may_be_lost 的精确切分；
  `verify()` 要求两半恰好划分全部阶段且 confirmed 必须是阶段前缀（越级确认即拒绝），`durability_boundary_for_code` 对未知持久化 code 拒绝（KV-005）。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR07S-FOCUSED | `bash doc/native-base/logs/pr07s-lease-fence-durability.sh` | 0 | PASS（write 23 passed/0 failed，每 ID 1 passed） | [pr07s-lease-fence-durability.log](logs/pr07s-lease-fence-durability.log) |
| PR07S-FMT | `cargo fmt --all --check` | 0 | PASS（同上） | 同上 |
| PR07S-WRITE007 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::write::tests::memory_backend::an_expired_or_superseded_lease_is_fenced_without_partial_metadata` | 0 | PASS（场景与 memory/redis/tikv 共享） | 同上 |
| PR07S-KV004 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::write::lease::tests::lease_validity_comes_from_the_backend_clock` | 0 | PASS（`lease_validity_comes_from_the_backend_clock_and_ignores_client_skew`） | 同上 |
| PR07S-KV005 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::write::lease::tests::durability_profiles_report_their_lossy_boundary` | 0 | PASS（`durability_profiles_report_their_lossy_boundary_and_verify`） | 同上 |

范围说明（诚实）：`LeaseGrant` 目前是调用方持有的结构，生产 lease 获取/续租（Redis `TIME`、TiKV TSO）尚未接入；
`DurabilityProfile` 是报告与校验层，真实后端 profile 的探测（fsync/TiKV 持久化语义）仍未实现；
WRITE-007 的 fence 由 PR04 的原子事务保证，本轮把它与 lease 语义显式绑定并加了整卷快照断言。
### PR07R · 无名 inode 跨 seal/fork 携带与回包丢失恢复

工具链同 PR07P。新增实现位于 `src/native_base/lifecycle/orphan.rs`（`lifecycle/mod.rs` 导出，`write/keys.rs` 增加 `oc/<operation_id>` 记录键）。

核心组件：
- `OrphanExtent` / `OrphanInode` / `OrphanCarry`：无名 inode 的 placement（chunk/offset/length + payload hash）与必须 canonical 的属性字节。
  `validate()` 要求私有 inode 号非零、inode 按号严格升序、extent 按 (chunk, offset) 严格升序且长度非零，属性必须被
  `frozen::decode_canonical_attributes` 逐字节还原——carry 不得有损重建它声称保存的 inode 身份。`encode`/`decode` 带 magic、
  计数上限（4096 inode / 1<<20 extent / 64 KiB attrs）与尾随字节检查，`digest()` 覆盖整段编码并写进 workspace head。
- `OrphanLedger`：`open_unlinked`（重复或已关闭的私有 inode 号拒绝）、`verify_read`（读必须命中已携带 placement 且内容 hash 一致，
  缺一即拒绝，且不产生 copy-up）、`append_write`（只在同私有 head 追加 placement，与既有 placement 重叠即冲突）、
  `close`（最后一个句柄关闭后释放 placement、离开 carry，inode 号不复用）、`carry`（空集合不产生 carry）、
  `ensure_fork_invisible`（fork listing 出现无名 inode 即报错）。
- `seal_head_with_orphans`：只允许 Running head，`visible_delta_count` 必须为 0（seal 不能隐藏可见 delta），workspace/domain 必须匹配；
  保持 base 与 `write_domain_id` 不变，写入 `open_orphan_count` 与 `orphan_carry_digest`，`entity_version` +1。
- `ensure_fork_view_has_no_carry` + `plan_fork_readable_base`：fork view 带私有 orphan 状态即拒绝；fork 立即可读集合 = Loose ∪ Packed
  （manifest 必须在基座内，同一对象不得同时被 Loose/Packed 认领）。
- `resume_orphan_carry`：lost-reply 恢复以 OperationId 为键；记录存在时要求记录字节逐字节相同，并重新计算「唯一那次 head switch」的
  结果 head/view 做比较——不同 head、不同 payload、陈旧前态、非零 `visible_delta_count` 全部拒绝，不会 fast-forward 到别的 head。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR07R-FOCUSED | `bash doc/native-base/logs/pr07r-orphan-carry-seal-fork.sh` | 0 | PASS（orphan 3 passed/0 failed，每 ID 1 passed） | [pr07r-orphan-carry-seal-fork.log](logs/pr07r-orphan-carry-seal-fork.log) |
| PR07R-FMT | `cargo fmt --all --check` | 0 | PASS（同上） | 同上 |
| PR07R-ORD009 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::lifecycle::orphan::tests::open_unlinked_inode_survives_seal` | 0 | PASS（`open_unlinked_inode_survives_seal_and_never_enters_the_visible_baseline`） | 同上 |
| PR07R-ORD010 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::lifecycle::orphan::tests::a_lost_seal_reply` | 0 | PASS（`a_lost_seal_reply_is_recovered_by_operation_id_without_wrong_fast_forward`） | 同上 |
| PR07R-IDX006-LIFE006 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::lifecycle::orphan::tests::a_fork_after_seal` | 0 | PASS（`a_fork_after_seal_reads_loose_and_packed_and_never_sees_the_carry`） | 同上 |

范围说明（诚实）：本轮交的是生命周期 seam。`OrphanLedger` 是进程内状态机，尚未接到真实 VFS open/unlink 路径；
`resume_orphan_carry` 走真实 `ControlStore` 事务（与 Redis/TiKV 共用同一 trait），但真实 seal 调用点仍在 PR07 运行时；
carry 只记录 placement 与 payload hash，不复制数据本体。
### PR07Q · Frozen 索引三缝：属性搬迁、ordinal 剪除、cookie 驱逐

工具链同 PR07P。实现位于 `src/native_base/frozen/mod.rs`。

核心组件：
- `AttributePlacement` + `ValueRef` + `relocate_attributes` / `resolve_external_attributes`：命名空间行值仍是
  「内联 canonical bytes」（首字节是 inode kind 1..=7），搬到外部对象时改为 `0xff` 标记 +
  `object_id/offset/stored_len`；`decode_row` 对两种形态都要求「decode 后再 encode 必须与入参逐字节相同」，
  否则拒绝（`attribute bytes are not canonical`），所以搬迁只能是移动，
  `logical_revision`/`canonical_table_digest` 不会变化。`FixedRevisionReader::resolve_inode_attributes`
  （`lookup_inode` 直接复用）走同一条认证页链读取外部字节。
- `InventoryOrdinal` + `plan_inventory_prune`：repack 前的 ordinal 映射必须严格升序；`pinned`/`live` 里的 ordinal
  必须已映射在**本 Pack** 内，否则报 `is not mapped` / `maps into another pack`；本 Pack 且既不 pinned
  也不 live 的行输出为 `pruned_keys`（`BE32(ordinal)` 删除列表），外 Pack 行原样列入 `foreign`——既不删也不当作本 Pack 映射。
- `DirectoryCursor`（锚点化）：cookie 从 `cookie -> spool index` 改为 `cookie -> DirectoryAnchor`（最后返回条目的 name+inode），
  新增 `evict_entries`/`respool`/`cookie_count`/`entries_resident`；`with_cookie_budget` 让 cookie 表独立受限：
  需要新锚点而表已满时整次 `page` 失败（`directory cookie table budget`），不返回页也不发坏 cookie；
  锚点条目消失或同名换成别的 inode 时失败而不是猜位置。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR07Q-FOCUSED | `bash doc/native-base/logs/pr07q-index-relocation-prune-cookies.sh` | 0 | PASS（frozen 18 passed/0 failed，3 项各 1 passed） | [pr07q-index-relocation-prune-cookies.log](logs/pr07q-index-relocation-prune-cookies.log) |
| PR07Q-FMT | `cargo fmt --all --check` | 0 | PASS（同上） | 同上 |
| PR07Q-IDX003 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::frozen::tests::attribute_relocation` | 0 | PASS（`attribute_relocation_from_inline_to_external_keeps_the_canonical_bytes`） | 同上 |
| PR07Q-IDX004 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::frozen::tests::inventory_prune` | 0 | PASS（`inventory_prune_removes_only_dead_slots_of_the_repacked_pack`） | 同上 |
| PR07Q-IDX005 | `cargo test -p brewfs --features native-packed-base --lib -- native_base::frozen::tests::issued_directory_cookies` | 0 | PASS（`issued_directory_cookies_survive_spool_eviction_and_the_table_fails_first`） | 同上 |

范围说明（诚实）：本轮交的是纯索引/游标 seam。`plan_inventory_prune` 是纯函数，尚未接到真实 repack 的
inventory 页重写；外部 attrs 的 ValueRef 解析已由 reader 走通，但没有真实 ingest/发布路径写入这种行；
`DirectoryCursor` 的驱逐/重放由调用方触发，尚未接入 VFS readdir 的 RAM 预算回收器；
`MAX_DIRECTORY_COOKIES` 默认 `1 << 20` 是进程内硬上限。P2 完整交付仍需与 published revision 流程打通。
### PR08 · Frozen Metadata 格式、索引与目录游标

工具链：同上。实现位于 `src/native_base/frozen/mod.rs`（单文件，约 380 行
结构 + 编解码 + 约 280 行 reader + 约 260 行测试）。

核心结构：`FrozenInodeRecord`（11 字段 + parent_hint + symlink_target）、
`SnapshotManifest`（namespace_mode 1/2 + data/inventory/namespace root）、
`FrozenExtent`、`DirectoryCursor`（bounded cookie spool + replay）。游标拒绝
零大小分页，并按页端点复用 continuation cookie，避免重复 readdir 请求导致
cookie 表无界增长。

BNPG 索引用共享 `wire::index_build` 模块构建；reader 使用 `ObjectSource`
trait 做按需页加载，配合 `sha2` 做 digest 验证。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR08-MANIFEST | manifest roundtrip + mode shape fail-closed | 0 | PASS（单元测试） | frozen::tests |
| PR08-EXTENT-PREFIX | extent predecessor 查询不跨 inode/chunk | 0 | PASS（单元测试） | frozen::tests |
| PR08-DIR-COOKIE | directory cursor cookie replay + unknown cookie 拒绝 | 0 | PASS（单元测试） | frozen::tests |
| PR08-DIR-COOKIE-BOUND | 同一页端点复用 continuation cookie（重复分页不再分配新 cookie）；`limit=0` 拒绝 | 0 | PASS（新增聚焦测试） | `directory_cookie_survives_page_replay` |
| PR08-MILLION-DIR | 1,000,000 条目按序分页，64 MiB spool 上限与未知 cookie 拒绝 | 0 | PASS（新增聚焦测试） | `one_million_directory_entries_page_in_order` |
| PR08-NON-UTF8 | 非 UTF-8 dentry 名称按原始 bytes 排序、分页和返回 | 0 | PASS（新增聚焦测试） | `non_utf8_dentry_names_preserve_original_bytes` |
| PR08-HARDLINK-PAGE | 跨页 dentry 仍解析到同一 inode 记录，缺失名称返回 None | 0 | PASS（新增聚焦测试） | `hardlinked_names_across_pages_share_one_inode_record` |
| PR08-RENAME | rename 后新路径复用原目录 inode，旧 dentry 消失且子树身份不变 | 0 | PASS（新增聚焦测试） | `directory_rename_keeps_inode_and_descendant_identity` |
| PR08-PARTIAL-NAMESPACE | manifest 到达但 namespace 对象缺失时 fail-closed | 0 | PASS（新增聚焦测试） | `partial_namespace_arrival_fails_closed` |
| PR08-MULTI-PAGE | FixedRevisionReader 多级 BNPG 索引遍历（150 条目 ≥ 2 层） | 0 | PASS（新增集成测试） | frozen::tests |
| PR08-NEGATIVE | negative lookup 返回 None 且不触发 KV RPC | 0 | PASS（新增集成测试） | frozen::tests |
| PR08-CORRUPT | 页损坏 fail-closed（digest 不匹配） | 0 | PASS（新增集成测试） | frozen::tests |
| PR08-FULL-TEST | `cargo test -p brewfs --features native-packed-base --lib native_base::frozen` | 0 | PASS（11 passed, 0 failed） | [pr07b-baseline-overlay.log](logs/pr07b-baseline-overlay.log) |

修复的 bug：`read_page` 中 `ObjectKind::from_magic` 传入整个 header（64B）
而非 magic（8B），导致所有 FrozenMetadata 对象被判为未知格式。

### PR09 · P2 Frozen + KV head、固定 revision 零 KV 读取

工具链：同上。实现为 `FixedRevisionReader<'a, S>` 结构（同 frozen 模块）。

核心保证：
- 零 KV RPC：reader 不持有任何 KV client，所有查找来自已认证 manifest 与页。
- `metadata_rpc_count()` 计数器永久为 0，用于 observability 证明。
- 命名空间、数据、inventory 三棵树各自独立 root。
- prefix-scoped extent 查询（predecessor + successor 不跨 inode/chunk）。
- 页级 digest 认证；子页损坏即失败，不降级。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR09-ZERO-KV | lookup_inode / lookup_data / lookup_inventory 全程 0 KV RPC | 0 | PASS（`metadata_rpc_count() == 0` 断言） | frozen::tests |
| PR09-FIXED-REV | manifest 验证后 reader 仅引用已包含 root 的对象 | 0 | PASS | frozen::tests |
| PR09-MODE-1 | namespace_mode=1（KV-layer 模式）打开成功但 frozen lookup 返回 None | 0 | PASS（`namespace_root: None`） | frozen::tests |

注意：PR09 目前只提供只读 P2 路径，不包含增量页重建、seal 扫描成本计算、
或 cold lookup 性能指标。这些属于 P2 完整交付的后续工作。

### PR10 · 读取 planner、Range 合并与预算

工具链：同上。实现位于 `src/native_base/runtime/planner.rs`（约 370 行）。

核心组件：
- `ReadUnit` + `CoalescedRange`：identity 精确合并，去重 duplicate，
  gap / physical size / amplification 三层限制。
- `MergePolicy`：`max_coalesced_get`、`max_gap_bytes`、`max_merge_amplification`。
- `ReadBudget` + `BudgetPermit`：demand reserve、prefetch 不得占用、RAII 释放。
- `FrameSingleflight<K, V>`：后台 loader、waiter cancel 不取消共享 loader、
  loader 失败不永久缓存。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR10-COALESCE | coalesce 去重 + amplification 限制 | 0 | PASS（单元测试） | planner::tests |
| PR10-BUDGET | prefetch 不能消耗 demand reserve | 0 | PASS（单元测试） | planner::tests |
| PR10-SINGLEFLIGHT | cancel waiter 不取消 loader | 0 | PASS（单元测试） | planner::tests |

注意：planner 尚未接入真实 reader 与 VFS 读取路径；当前为独立可测试的
核心算法模块。接入 PR07 运行时读取路径属于后续整合工作。


### PR11 · 同机共享基线缓存服务（P3 preview）

工具链：同上。实现位于 `src/native_base/cache/mod.rs`（约 150 行）。

当前状态：**preview 骨架，未默认启用**。提供统一的类型接口和
进程内 fallback，真实 Unix socket 服务待 P3 阶段实现。

核心组件：
- `FrameCacheKey`：namespace + object_id + full_hash + offset + len + digest，
  确保跨租户隔离（spec 06 §5）。
- `CacheLookupResult`：Hit / Miss / Unavailable 三态。
- `InProcessFrameCache`：有界 HashMap fallback，用于无服务场景。
- `probe_shared_cache()`：始终返回 `None`（preview），调用者自动 fallback。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR11-FALLBACK-HIT | in-process cache hit/miss | 0 | PASS（单元测试） | cache::tests |
| PR11-BOUNDED | cache 容量上限不超 | 0 | PASS（单元测试） | cache::tests |
| PR11-NS-ISOLATE | 不同 namespace 不共享 key | 0 | PASS（单元测试） | cache::tests |
| PR11-PROBE-OFF | preview 模式 probe 返回 None | 0 | PASS（单元测试） | cache::tests |

注意：真实 Unix socket 服务、跨进程 singleflight、LRU byte budget、
服务退出 fallback、TLS/认证等均未实现。这些属于完整 P3 交付，
不在本轮 P1/P2 范围内。
### PR12 · 显式 lossless 布局变体

工具链：同上。实现位于 `src/native_base/lifecycle/variant.rs`（约 136 行）。

核心保证：
- 永远不删除旧正式对象：`permanent_objects = source ∪ candidate`。
- 不改变 logical revision 或 block bindings（semantic identity 校验）。
- `max_extra_bytes` 预算超支即拒绝。
- apply 需要 clean head + 0 open orphan + 无有效 writer + view 匹配。
- `added_objects` 精确列出新增对象集合，便于保留追踪。

| ID | 原样命令 | 退出码 | 结果 | raw日志/fixture路径 |
|---|---|---:|---|---|
| PR12-UNION | 新旧对象并集永久保留 | 0 | PASS（单元测试） | variant::tests |
| PR12-SEMANTIC | logical/binding 变化拒绝 | 0 | PASS（单元测试） | variant::tests |
| PR12-BUDGET | 预算超支拒绝 | 0 | PASS（单元测试） | variant::tests |
| PR12-BUSY | busy apply 拒绝（open orphan） | 0 | PASS（单元测试） | variant::tests |

注意：variant 尚未接入真实 publish/retain 事务路径；当前为纯函数验证模块。
与 PublishedRevision 的集成在完整 P2/P3 阶段推进。


### PR13 · 性能实验、能力发布与运维文档

当前状态：代码实现部分（PR07–PR12）完成 focused unit test 验证；性能
实验与完整集成门尚未运行。本节说明各 profile 的验证状态与计划。

**性能 A–F 对照（当前状态）**：

| Profile | 描述 | 状态 | 说明 |
|---|---|---|---|
| A | P1 retained-trial baseline | NOT_RUN | 需 docker compose + Redis + FUSE + fio |
| B | P1 + zstd compression | NOT_RUN | 待 A 通过后启用 |
| C | P2 frozen-read baseline | NOT_RUN | 需 P1 + frozen snapshot 生成工具 |
| D | P3 shared-cache warm | NOT_RUN | 需 PR11 Unix socket cache service |
| E | P3 repacked layout | NOT_RUN | 需 PR12 variant apply + repack 工具 |
| F | 对照 JuiceFS writeback | NOT_RUN | 需 compose-xfstests runner + JuiceFS 镜像 |

**能力发布矩阵**：

| 能力 | P1 | P2 | P3 |
|---|---|---|---|
| Packed/Loose mixed 与范围 COW | 代码已写，集成未验 | — | — |
| receipt、ordered commit、seal/fork/recovery | 已验证（PR06A） | — | — |
| PublishedRevision 永久保留 | 已验证（PR06A） | — | — |
| 终结私有域清理 | 已验证（PR06B，内存后端） | — | — |
| Frozen 只读按需元数据 | 代码已写，单元验证 | 集成未验 | — |
| shared-frame、跨请求合并、host cache | planner 算法已写 | 未接入 reader | 服务未实现 |
| 历史 sealed 自动删除/TTL | 不支持（设计如此） | 不支持 | 不支持 |
| 自动 repack 释放 published 空间 | 不支持（variant 纯加法） | 不支持 | 不支持 |

**回滚与运维**：

- 回滚：native header 是 create-only 的 KV 记录；回滚到
  `workspace-v1` 只需不加载 native feature，旧路径完全不受影响。
- 数据安全：所有 native 对象写入前计算 full_hash，读取时校验；
  控制面事务原子（Redis Lua / TiKV txn）。
- 容量：native 对象仅追加，不删除已发布基线；私有域在 close 后清理。
- 监控：`brewfs_writeback_*`、`brewfs_native_*` 计数器（逐步接入）。
- 已知限制：见 `doc/native-base/README.md` 与 `acceptance-matrix.json`
  中所有 NOT_RUN / SPECIFIED_NOT_IMPLEMENTED 条目。

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
