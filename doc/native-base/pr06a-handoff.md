# PR06A 开发期交接记录（历史交接点 commit `e19c990`）

> PR06A 已在实现提交 `ea373a9b8af5187c01b16b65c46a16c56ddd9ee2` 完成并通过
> exact-SHA 完整门禁。本文件保留开发期上下文；最终状态与证据见
> `README.md` 和 `implementation-report.md`，后续顺序从 PR06B 继续。

面向接手本机后续工作的 agent。目标：在**不重新推导已有结论**的前提下继续
PR06A → PR06B → PR07+。所有事实性陈述都对应到本仓库的真实 commit/文件；凡
“未验证”都明确标注。

---

## 1. 交接点状态（先看这里）

| 项 | 值 |
|---|---|
| worktree | `D:\code\brewfs\.claude\worktrees\vigorous-solomon-7caff5` |
| 分支 | `claude/vigorous-solomon-7caff5` |
| 交接 commit | `e19c990` refactor(native-base): extract the shared BNPG index-tree builder |
| 前序 commit | `7291196`（PR04 证据更正）· `7b6ddcf` PR05 · `59df362` PR04 · `63e4ec2` PR03 · `87e3aca` PR02 |
| 远端 | **未 push**（用户未授权 push；不要把该分支推走） |
| 规格包 | `D:\code\brewfs\BrewFS_Native_Packed_Base_Specs_v1.2_Final_2026-09-15\brewfs-native-base-specs-2026-09-15-v1.2-final` |
| 入口 | 该包 `CODEX_TASK.md`；顺序由 `specs/15-implementation-plan.md` §2 的 14 步表决定 |
| 工作副本 | 未提交文件：`.claude/`（临时脚本、提交信息、`gate-commit.txt`，未跟踪；勿清理他人内容） |

验证状态：`e19c990` 上 `cargo test -p brewfs --lib -- native_base::` = **204 passed / 0 failed / 34 ignored**（ignored 是需要 scratch Redis/TiKV 的用例）。
完整 AGENTS 门禁在交接点重跑：全部步骤 `:exit=0`、无 `:exit=<非0>`、
`849 + 721 passed / 0 failed`、`=== GATE: ALL PASSED ===`、`GATE_PIPELINE_EXIT=0`，
日志 [logs/pr06a-checkpoint-gate.log](logs/pr06a-checkpoint-gate.log)。

## 2. 环境与命令（照抄即可）

开发在 Windows 侧（git、文件编辑），**编译与测试全部在 WSL Ubuntu-24.04**
（cargo 1.98.1，target dir `/home/luxian/brewfs-target-vigorous-solomon-7caff5`）。

完整门禁（脚本已提交，可复现本次交接点的日志）：

```bash
cd "D:/code/brewfs/.claude/worktrees/vigorous-solomon-7caff5" && git rev-parse HEAD > .claude/gate-commit.txt
```

```bash
MSYS_NO_PATHCONV=1 wsl.exe -d Ubuntu-24.04 bash -lc 'bash /mnt/d/code/brewfs/.claude/worktrees/vigorous-solomon-7caff5/doc/native-base/logs/pr06a-checkpoint-run.sh'
```

两条命令的分工：`pr06a-checkpoint-run.sh`（WSL 包装：设 `PATH=$HOME/.cargo/bin`、
`CARGO_TARGET_DIR`、`CARGO_INCREMENTAL=0`、`CARGO_PROFILE_DEV_DEBUG=0`，并把输出
tee 到 `doc/native-base/logs/pr06a-checkpoint-gate.log`）在 WSL 里读不到本 worktree
的 git（Windows 路径），所以提交号由 Windows 侧先写进 `.claude/gate-commit.txt`，
包装脚本读出后作为 `GATE_COMMIT` 传给被调用的 `pr06a-checkpoint-gate.sh`（门禁本体，
逐步骤记录 `:exit=`）。单跑 focused test：

```bash
MSYS_NO_PATHCONV=1 wsl.exe -d Ubuntu-24.04 bash -lc 'cd /mnt/d/code/brewfs/.claude/worktrees/vigorous-solomon-7caff5 && CARGO_TARGET_DIR=$HOME/brewfs-target-vigorous-solomon-7caff5 cargo test -p brewfs --lib -j 4 -- native_base:: --test-threads 4'
```

**踩坑清单（都已实际踩过）**

1. Write/Edit 产出 CRLF → 提交前必须 `sed -i 's/\r$//' <file>`，否则
   `git diff --check` 报 “new blank line at EOF”/CR 告警。
2. `git diff --check`、`git commit` 只能在 Windows 侧跑（WSL git 解析不了
   worktree 的 Windows 路径）。
3. `*.log` 被 `.gitignore` 忽略 → 证据日志要 `git add -f`。
4. 嵌套引号会吞变量：需要 `$VAR` 的命令一律写成脚本文件再 `bash <file>` 调用。
5. docker compose 脚本在 Windows checkout 是 CRLF → 复制到 `/tmp` 归一化后跑。
6. 测试并发：`-j 4 -- --test-threads 4`（默认并发会 OOM/超时）。
7. `doc/native-base/logs/*.sh` 里引用仓库根需**上溯三级**（`cd ../../..`）。
8. 读 `acceptance-matrix.json` 用 `io.open(..., encoding='utf-8')`；字段名是
   `test_id`；控制台 GBK 会乱码中文 → 先写 UTF-8 文件再读。

## 3. 红线（用户逐字要求，必须继续遵守）

- 没有明确授权：**不 push 远端、不部署、不删除用户数据**。
- **不直接操作现有用户 bucket**；不编造 PR/issue 链接（本包没有创建任何远端 PR）。
- coordinator durability 占位值未验证前不能替换。
- 无环境如实 `NOT_RUN`；不把参考模型 PASS 抄成产品测试结果。
- 验收矩阵 `implementation_status` 只允许
  `SPECIFIED_NOT_IMPLEMENTED` / `PASS` / `NOT_RUN`；**PASS 必须附真实命令、
  退出码、产物路径**，不能由控制流推断（PR04 已因此被更正一次，见 §5）。
- spec 06/15 §6 的捷径禁令：不 404 补零、不整文件加载、缺失不补零、
  不 HEAD+PUT 模拟原子 create、不能晚到旧写覆盖新写、不能 lease 过期直接删私有
  数据、不能整写域永久化省掉对象选择、不能用 direct I/O 掩盖 mmap 缺口、
  **不能把 retention 批次登记留到 publish 之后**。
- commit 结尾 `Co-Authored-By: Claude Code <noreply@anthropic.com>`；PR 描述结尾
  `🤖 Generated with [Claude Code](https://claude.com/claude-code)`。
- 不使用裸 `git stash`（worktree 共享 stash 栈）。需要暂存用临时 WIP commit。

## 4. 已完成部分（PR01–PR05 + 06A 第一步）

代码全部在 `src/native_base/`（新增目录，不动既有 v1 路径）：

| 模块 | 内容 | 关键入口 |
|---|---|---|
| `wire/` | 3/0 codec：container(64B 头尾/kind/magic/CRC32C)、frame、page(BNPG)、refs(PageAddress 56B / ObjectRef / RootRef / ChildRef)、uvarint、bnct(kind 1–7、16–19, envelope v2)、datapack(scrub+builder)、**index_build（本次新增）** | `bnct::ControlRecord`、`refs::PageAddress`、`index_build::build_index_tree` |
| `seal/` | SealBuilder（BNSD root + 4 表）、严格 reader、plan/binding/placement/descriptor | `SealBuilder::build`、`SealSnapshot::open` |
| `write/` | `ControlStore/Txn/StoreError::Conflict`；keys 前缀 `nb2/{volume}/`；records(`HeadState`)；domain；receipts(`ObjectSink`/`object_key`)；commit；overlay(`WriteOverlay`)；memory/redis/tikv 后端 | `WriteOverlay::{accept,capture,dispatch,drain}`、`commit_uploaded_slice` |
| `ingest/` | local/seekable-tar 源、稳定计划、上传验证与 resume | PR05 |

验收矩阵 173 项中 **37 项 PASS**（WIRE 10/12、READ、WRITE/KV/ORD 共 12、INGEST 10），其余 136 项
`SPECIFIED_NOT_IMPLEMENTED`。

**06A 第一步（本次完成）**：把 BNPG 索引树 writer 从 `seal/builder.rs` 抽到
`wire/index_build.rs`（`IndexTreeParams{leaf_target, leaf_kind, page_kind}`、
`place_page(body, page, page_kind)`、`build_index_tree(entries, params, body)`），
供 Inventory / RetainBatch 索引复用。字节不变性**不是假设而是被钉住的**：
`seal/tests.rs::seal_bytes_are_unchanged_by_the_shared_index_builder` 用
`7291196` 上的 detached worktree 探针取到的两个摘要做 golden（混合
Loose/Packed fixture + 64B 叶子多层树）。

## 5. PR06A 剩余工作（下一步做什么）

spec15 行 06A：**永久 PublishedRevision、精确 RetainBatch、seal/fork/recovery**；
依赖 04/05；必需证据“新基线混合 Loose/Packed，批次与切 head 同事务”。

### 5.1 已定设计（可直接实施，无需重新推导）

新模块 `src/native_base/lifecycle/`（建议 `mod.rs` / `manifest.rs` / `index.rs` /
`retention.rs` / `publish.rs` / `seal.rs` / `fork.rs` / `options.rs` / `tests.rs`）：

- **manifest**：kind 4 容器 + 裸 `BNSM` root（`PageKind::ManifestPayload`，无 BNPG
  前缀）。P1 走 `namespace_mode=1`（`kv_layer_id`+`kv_sealed_version`），故 06A
  **不需要** namespace root dir。`manifest full_hash` = `StorageViewId`；
  inventory 不含 manifest 自身与其索引对象。
- **inventory**：kind 5 容器 + `PageKind::InventoryIndex` 页，
  key=`ObjectId(16B)` → value=`ObjectRef`，复用 `index_build`。
- **RetainBatch**：按 origin 域切分；只列“本次候选实际依赖且 origin 在本域”的对象
  （candidate manifest、physical inventory 索引容器与全部传递依赖）；已由旧
  PublishedRevision 保留者可省；**索引对象自身不入自己列表**（无自引用 hash）；
  `evidence_root` 由 receipt 根隐式保留。
- **publish 单次原子事务**（≤2 个自有域：workspace + build）：追加 RetentionReceipt
  → 递增各域 `retention_seq` → 登记 PublishedRevision → KvBaseRetention（kind 7，
  create-only，重复必须内容一致、永不过期）→ 切 base/head/epoch → 记 OperationId
  结果。同 id 同 payload 返回同 result；同 id 异 payload 拒绝。
- **seal 状态机**：`LOCAL_FREEZING → PREPARE → QUIESCED → DATA_DRAINED →
  CANDIDATE_VERIFIED → RETENTION_PREPARED → PUBLISHED_RETAINED → COMPLETED`，
  每阶段崩溃后幂等恢复（重载 journal 的逐阶段恢复表见 spec09）。验证证书固定
  candidate hash / required closure digest / source PublishedRevision IDs / 自有域
  与 RetainBatch 根 / namespace version / drain final seq。
- **fork**：只从 `PUBLISHED_RETAINED` 建新 head + 新写域，O(N) 控制记录、不复制
  metadata/data；fast-forward 要求 target head 无可见增量/无 open-orphan/无有效
  writer/`fork_base` 精确匹配，并比较 `LogicalRevision`、`StorageViewId`、
  `head_epoch`。discard/detach **不解除** PublishedRevision 保留。
- **写域跨 seal 稳定**：`PrivateWriteDomain` 贯穿 workspace 生命周期；一次 seal 的
  manifest/Seal/metadata 归**本次** BuildDomain。
- **head 记录策略**：保留 PR04 的 `head/{ws}` `HeadState`（写路径 token），另加
  kind 16 视图记录，二者在同一原子事务内更新 —— 目的是**不改动 PR04 的字节格式**
  （改了会让 PR04 的既有证据失效）。

### 5.2 验收项映射（P0 优先）

- `LIFE-001..009`：seal 保留 mixed 布局、freeze 期 drain 不死锁、逐 phase 崩溃幂等
  恢复、root switch 回包丢失后同 operation 同 result、100 fork 不复制 N 倍数据、
  seal 后立即 fork 可读 Loose+Packed、fast-forward 目标已修改须 Conflict、
  Noop remote barrier 拒绝真实 write publication、stale 客户端不能改新 head。
- `DRAIN-001..005`：冻结计划须包含“已接受但无 commit sequence”的 ticket、
  冻结前接收/冻结后分配 slice 的合法 drain、伪造 ticket 拒绝、未 fsync 不得宣称
  完成、有界 batch + OperationId 幂等。
- `RET-001..019/022`：见 `.claude/pr06a-matrix.txt`（已从矩阵摘出，含
  scenario/expected）。
- **不属于 06A**：`RET-020`（私有 cleaner 不扫全局历史）、`RET-023` 的清理侧、
  `CLN-*` → 06B；`RET-012`（P2 trusted 固定只读）→ P2；
  `RET-021`（拒绝 TTL/published_gc/read_retention_leases 配置）**本仓库尚无
  native-base 配置加载器（配置绑定属 PR07）** → 06A 只实现纯校验函数并**不**标
  PASS，保持 `SPECIFIED_NOT_IMPLEMENTED`。

### 5.3 工作流（每步都要）

反例测试 → 最小实现 → focused tests → AGENTS 完整门禁 → 保存
commit/命令/退出码/raw 日志/fixture → 更新
`doc/native-base/implementation-report.md`、`acceptance-matrix.json`、`README.md`
的 PR 状态表 → 提交（**不 push**）。

## 6. 既有代码面要点（改之前先读）

- `write/keys.rs`：`nb2/{volume}/` 下已有 dom/reg/obj/inv/head/mut/ino/ext/bnd/plc
  前缀；06A 新 key 加在这里。
- `write/store.rs`：所有控制面写入走 `Txn{check_absent,check_bytes,put,delete}` +
  `ControlStore::run`，冲突返回 `StoreError::Conflict`（原子事务就用它）。
- `write/records.rs`：`HeadState{head, writer_generation, write_domain_id}` +
  `next_commit`；`next_commit` 是数据面 commit sequence 的水位。
- `wire/refs.rs::ensure_object_kind_allows_page` 决定哪个容器 kind 能引用哪种
  page kind —— 新增 `PageKind::InventoryIndex` 用法前先看它。
- `IndexPage::decode` **强制精确消费**（不接受尾随字节）；校验页时用
  `PageAddress{offset, stored_len}` 切片，不要逐字节扫 body。
- `MAX_RAW_PAGE` = 1 MiB、`MAX_PAGE_ENTRIES` 有硬上限，超限是
  `WireError::LimitExceeded`（不是 panic）。

## 7. 交接点自查（接手第一步就能跑）

```bash
MSYS_NO_PATHCONV=1 wsl.exe -d Ubuntu-24.04 bash -lc 'cd /mnt/d/code/brewfs/.claude/worktrees/vigorous-solomon-7caff5 && CARGO_TARGET_DIR=$HOME/brewfs-target-vigorous-solomon-7caff5 cargo test -p brewfs --lib -j 4 -- native_base:: --test-threads 4'
```

期望：`204 passed; 0 failed; 34 ignored`。若数字不符，先看是否有未跟踪改动
（`git status`）与 CRLF 问题（§2.1）。

## 8. 未决/需用户确认的点

1. **是否 push 分支 / 开 PR**：本分支一直只在本地。用户未授权，接手前先问。
2. **`RET-021` 的配置拒绝**：等 PR07 配置加载器落地再接线，否则只能纯函数级证据
   （已在 §5.2 说明，勿标 PASS）。
3. **coordinator durability 占位值**：仓库内仍是占位，未验证前不要当作真实保证。
4. **head 视图记录（kind 16）与 PR04 `HeadState` 双写**：若后续决定只保留一份，
   需要重跑 PR04 证据（会使其现有日志失效），成本已在 §5.1 标注。
