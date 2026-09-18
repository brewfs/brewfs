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
