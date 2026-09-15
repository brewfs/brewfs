# PR01 · 基线调用者审计（workspace-native-v2 实现前置）

审计基线：`8ff73d9c18c0f81e88c183383b00d2ba86bd42ba`（= 实现开始时 HEAD）。
方法：对 `src/workspace_overlay`、`src/chunk`、`src/meta`、`src/cadapter`、
`src/console`、`src/control`、`src/daemon`、`operator/` 做只读代码审查，
核对数据结构、写/读路径、生命周期与全部删除入口。本文是 PR01 的交付证据，
对应的反例模型测试在 `src/native_base/counterexamples.rs`。

## 1. 结论摘要

1. **仓库中不存在任何 native packed base 代码**（brfdp/DataPack/Data Seal/BNCT/
   workspace-native-v2 均无匹配）。新实现是绿地，但必须嵌入现有
   `workspace_overlay`（catalog/lifecycle/meta_layer）与 `chunk`（BlockStore）体系。
2. `VolumeFormat` 目前只有 `FlatV1 | WorkspaceV1`（[config.rs:285](../../src/config.rs)）。
   **没有任何代码按 volume_format 为 v2 分支路由**；现有隔离是"路径分叉"
   （workspace 挂载不启动 flat GC）而非格式判断。
3. 与新协议的五个关键差距（详见 §5）：每次 seal 更换写域、发布与保留不原子、
   无 published 状态区分、GC 是隐式根推导 mark-sweep、open-unlink 依赖内存计数。
4. 反例模型测试（§6）记录三个弱协议失败模式，作为 PR03/PR04/PR06A 的对照契约。

## 2. 数据路径现状

### 2.1 数据结构（与新规格的对接点）

| 结构 | 位置 | 说明 |
|---|---|---|
| `SliceDesc { slice_id, chunk_id, offset, length }` | [slice.rs:95](../../src/chunk/slice.rs) | flat 路径；`offset` 是 chunk 内逻辑偏移 |
| `ExtentKind::Data { slice_id, slice_offset } \| Hole` | [model.rs:366](../../src/workspace_overlay/model.rs) | workspace 路径；**与新规格语义一致**（逻辑寻址，物理位置另查） |
| `BlockKey = (slice_id, block_index)` | [store.rs:100](../../src/chunk/store.rs) | BlockStore 对象键 `chunks-v2/{chunk_id}/{block_index}` |
| `ReadPlanSegment::Data/Zero` | [read_plan.rs:12](../../src/chunk/read_plan.rs) | 工作区中立读计划 |
| `ChunkLayout { chunk_size=64MiB, block_size=4MiB }` | [layout.rs:32](../../src/chunk/layout.rs) | 与 P1 试运行配置（67108864/4194304）一致 |
| `LayerExtent { layer_id, logical_offset, length, sequence, kind }` | [model.rs:381](../../src/workspace_overlay/model.rs) | 层内 sequence 即新规格 head_commit_seq 的前身 |

### 2.2 写路径

- flat 与 workspace 共用 FileWriter；slice 在 `find_slice_or_create`
  （[writer.rs:1310](../../src/vfs/io/writer.rs)）按 `write_order` 插入 chunk deque，
  `try_commit` 只允许 deque 前端提交（[writer.rs:1161](../../src/vfs/io/writer.rs)，
  注释明言防止乱序 append 让旧 slice 获胜）——**单 writer 单 inode 内顺序有保证**。
- 风险点：(a) `write_order` 是 writer-local 计数器，跨挂载写同一 inode 无全局序；
  (b) TiKV 租约式 slice_id 批量分配使 slice_id 与提交顺序无关
  （`remove_fully_covered` 视高 slice_id 为新只是启发式，[slice.rs:161](../../src/chunk/slice.rs)）；
  (c) commit 失败重试耗尽后 slice 被 `mark_failed` 丢弃（数据丢失而非阻塞）。
- workspace 写：`apply_data_mutation` 在同一 CAS 事务为 inode+extents 分配连续
  sequence，受 HeadGuard fence（[kv_store.rs:1014](../../src/workspace_overlay/stores/kv_store.rs)）。
  truncate 写 Hole extents 覆盖；`visible_length = min(slice.length, new_size - slice_start)`
  截去 EOF 外尾巴（[meta_layer/mod.rs:1310](../../src/workspace_overlay/meta_layer/mod.rs)）。
- **无 ingest/导入预约机制**：slice_id 一律上传开始时惰性分配，无"先预约 id 后填数据"接口。

### 2.3 读路径

- flat 读捕获 = inode 缓存 size + slices 缓存 + dirty overlay 三者**无原子快照/token**；
  代码用 snapshot-patch-before-backend-read（[fs/mod.rs:3436](../../src/vfs/fs/mod.rs)）
  与 commit 前失效 reader cache 弥补窄窗口。
- workspace 读：inode.size 与 extents 同事务变更、`data_version` 参与
  ReadPlanCacheKey（[cache.rs:14](../../src/workspace_overlay/cache.rs)）；但
  `read_plan` 内 inode 解析与 extent 查询是**两次独立 store 读**
  （[meta_layer/mod.rs:1711/1741](../../src/workspace_overlay/meta_layer/mod.rs)），
  无读事务包裹——正是 spec18 要求 ViewGate+token 校验消除的 torn-view 暴露面。

## 3. workspace 生命周期现状

- **seal 编排**（[lifecycle/mod.rs:62-125](../../src/workspace_overlay/lifecycle/mod.rs)）：
  begin_seal → Quiesced → `DurableRemoteBarrier.drain()` → DataDrained → hash →
  `commit_seal`（[kv_store.rs:1241](../../src/workspace_overlay/stores/kv_store.rs)，
  单 CAS 事务：旧 head Sealed+sealed_version、新 writable layer、head_epoch+1、
  lease rebasing）→ `flatten_sealed_workspace` → `install_compaction`
  （又一次 head 切换+全量命名空间物化，O(全部元数据)/次，旧 head→Deleting）。
- **生产 `DurableRemoteBarrier` 只有 Noop 实现**（[main.rs:1563](../../src/main.rs)）——
  seal 的 Quiesced→DataDrained 并不真正保证写回缓存已上传（spec18 的受限 drain 要替换的空操作）。
- **无 `head_commit_seq`**：跨 revision 全序只能拼 head_epoch + 全局 sealed_version
  分配器 + 层内 sequence。
- **fork 零复制**（[kv_store.rs:494-556](../../src/workspace_overlay/stores/kv_store.rs)）：
  只写 workspace 记录+空 writable head；但**没有 published 概念**——任何
  compaction 产物都可作 fork base。
- **open-unlink 跨 seal**：`open_counts` 是纯内存 DashMap
  （[meta_layer/mod.rs:49](../../src/workspace_overlay/meta_layer/mod.rs)）；客户端
  崩溃未 close 则 unlinked inode 永久泄漏在 sealed base，slice 永不可回收
  （新规格 OrphanCarry + 显式 empty-head 判定要解决的缺口，FNL-06）。
- schema 路由：`volume_format = "workspace-v1"` 硬校验于
  [main.rs:1230/1533](../../src/main.rs)、[meta_layer/mod.rs:524](../../src/workspace_overlay/meta_layer/mod.rs)、
  [config.rs:626](../../src/config.rs)——新格式需在这些路由点接入 `workspace-native-v2`。

## 4. 删除/GC 入口清单（PR06B 收口点）

### 4.1 远端对象删除的汇聚点

| # | 入口 | 位置 | 删除什么 | guard | 上游 |
|---|---|---|---|---|---|
| D1 | `ObjectBlockStore::delete_range` | [store.rs:1230](../../src/chunk/store.rs) | 远端 block 对象（chunks/ 与 chunks-v2/） | **无任何格式判断** | 所有下列调用方 |
| D2 | `WorkspaceGc::run_at` | [gc.rs:54](../../src/workspace_overlay/gc.rs) | 不可达层元数据行 + 孤儿 slice 远端块 | 可达性闭包 + grace 窗口 | 挂载进程内定时任务（operator-managed 时禁用） |
| D3 | `BlockStoreGC::run_gc_cycle` | [compact/gc.rs:68](../../src/chunk/compact/gc.rs) | delayed slice 到期块 + orphan 未提交块 | TTL | CompactionWorker（**仅 flat 挂载启动**） |
| D4 | `cleanup_uncommitted_slice` | [compact/compactor.rs:347](../../src/chunk/compact/compactor.rs) | heavy 压缩失败时新写 slice 的块 | 压缩冲突分支 | 压缩循环 |
| D5 | `MarkBasedGarbageCollector::delete_objects` | [daemon/worker.rs:204](../../src/daemon/worker.rs) | 按 inode 整文件远端对象 | **无格式 guard；当前 dead_code 未接线** | 无（接线前必须加 guard） |
| D6 | operator `cleanup_workspace` / `cleanup_snapshot` | [controller.rs:857/945](../../operator/brewfs-operator/src/workspace/controller.rs) | → `mark_workspace_deleting` / `delete_snapshot`（移除 GC 根） | deletion_policy/lease/finalizer | K8s CR finalizer |

### 4.2 元数据删除例程

| # | 入口 | 位置 | 说明 |
|---|---|---|---|
| M1 | `delete_layer_metadata` / `finalize_layer_metadata_deletion` | [kv_store.rs:1671/1701](../../src/workspace_overlay/stores/kv_store.rs) | 层置 Deleting → 硬删 5 张 delta 表；唯一调用方 WorkspaceGc |
| M2 | `delete_snapshot` | [kv_store.rs:746](../../src/workspace_overlay/stores/kv_store.rs) | 删 snapshot 行=移除 GC 根；无 refcount |
| M3 | `mark_workspace_deleting` | [kv_store.rs:1523](../../src/workspace_overlay/stores/kv_store.rs) | discard/operator 删除的入口 |
| M4 | `record_orphan_slice` | [kv_store.rs:1565](../../src/workspace_overlay/stores/kv_store.rs) | 已上传未提交 slice 交 GC 回收 |
| M5 | `prune_slices_for_truncate` | [database/mod.rs:786](../../src/meta/stores/database/mod.rs) | flat truncate 直接 DELETE slice_meta 行 |
| M6 | `process_delayed_slices` / `cleanup_orphan_uncommitted_slices` / `remove_file_metadata` | [store.rs:906-1407](../../src/meta/store.rs) | flat 元数据 GC 例程（各 store 实现） |
| M7 | `remove_file_metadata`（workspace 侧） | [meta_layer/mod.rs:1334](../../src/workspace_overlay/meta_layer/mod.rs) | **已 NotSupported**（"lifecycle-managed"）——现有路径分叉隔离的范例 |

### 4.3 隔离状况判定

- workspace 挂载通过 `from_workspace_components(background_tasks=None)`
  （[fs/mod.rs:985](../../src/vfs/fs/mod.rs)）不启动 flat GC/压缩——结构隔离；
- workspace-v1 header 硬校验构成"非 workspace-v1 一律拒绝挂载"的闸门
  （database.rs:2868 同样）——**v2 复用该 catalog 时必须新增路由而非被拒绝**；
- `enqueue_gc_job`/RunGc 是 no-op stub（无风险）；trash delete 只删元数据行。

**PR06B 收口顺序（按风险）**：① WorkspaceGc 全链路（不区分 sealed 是否已发布）；
② operator 的 mark_workspace_deleting/delete_snapshot；③ `delete_range` 作为
统一 choke point 加卷格式 guard；④ dead 的 D5 接线前必须加 guard。

## 5. 与新协议（1.2-consolidated）的关键差距

| # | 差距 | 现状 | 新协议要求 | 涉及 spec |
|---|---|---|---|---|
| G1 | 写域 | 每次 seal 铸造新 LayerId、epoch 双跳（commit_seal+install_compaction） | 一个 workspace 一个贯穿生命周期的 PrivateWriteDomain | 09§2 |
| G2 | 发布原子性 | `seal_and_snapshot` 两连调；fast_forward 完全不登记保留 | PublishedRevision+RetainBatch+head 切换单事务 | 09§4/20§5 |
| G3 | published 状态 | 任何扁平 sealed revision 都可 fork | 仅 PUBLISHED_RETAINED 可 fork | 09§6 |
| G4 | GC | 隐式根推导（snapshots+leases+journals）单进程 mark-sweep | 无全局 GC；只清理 CLOSED 域 I−K−C | 10§2 |
| G5 | open-unlink | 内存 open_counts，崩溃即泄漏 | OrphanCarry 元数据引用 + 显式 empty-head 判定 | 09§7/20§14 |
| G6 | 一致捕获 | read_plan 两次独立 store 读，无 token | ViewGate + HeadCommitToken 校验 | 18§4 |
| G7 | drain | NoopDurableRemoteBarrier | 受限 drain：journal+DrainGuard+按 ticket 边界 | 18§6 |
| G8 | 提交序号 | writer-local write_order + 层内 sequence | admission_ticket/dirty_generation/head_commit_seq 三分离，同 inode 按 ticket 序提交 | 18§2/§10 |

## 6. 反例记录（`src/native_base/counterexamples.rs`）

三个确定性交错模型，每个都先跑弱协议复现危害（原始反例），再跑强协议验证安全：

| 模型 | 弱协议危害 | 强协议契约 | 对应验收 |
|---|---|---|---|
| CEX-CONS | 读捕获 EOF 与 extents 非原子：已接受追加的尾部读零（INV-04 违例）；handoff 后读旧内容 | ViewGate 单次捕获固定同代 EOF+extents；token 变化即丢弃重试 | CONS-001..006 |
| CEX-ORD | 上传完成序提交：重叠写旧遮新（`AAAAAAAA` 覆盖 `BBBB`）；迟到写复活 truncate 后文件；handoff 按范围误删新 dirty | 同 inode 按 admission_ticket 序提交；handoff 按 operation 身份释放 | ORD-001/002/004/008 |
| CEX-RET | 先切 head 后补保留：cleaner 在窗口内删除已发布对象（FNL-04） | 发布事务原子登记 RetainBatch；close 先赢则拒绝发布 | RET-001/005/006/016 |

现有产品代码的对应暴露面：G6（read_plan 两次读）、G8 风险点 (a)(b)、
D2+G2 组合（GC 不区分已发布）。这些暴露面即后续 PR 的最小实现靶点。

## 7. 成本指标盘点（性能 A–F 消融的现有基础）

- [stats.rs](../../src/vfs/stats.rs)：s3_put/get/del ops+bytes+lat、
  writeback dirty 族（live/pending/uploaded 细分）、flush_fragmentation、
  slice_create/reuse/reject、upload_batch、partial_tail——数据面成本基础良好；
  **无显式写放大率**（可由 s3_put_bytes/fuse_write_bytes 推导）、无元数据事务计数。
- [metrics.rs](../../src/workspace_overlay/metrics.rs)：private_bytes_written、
  parent_bytes_read、resolver 步数、extent_plan_segments、seal_pending_bytes、
  gc_orphan_bytes、fenced_writes。
- 新协议需补：seal 元数据扫描成本、验证回读、drain、追加保留空间四类独立统计
  （spec19/性能 A–F 要求，PR06A 起实现）。

## 8. 对后续 PR 的输入

- PR02 起的 wire codec 放 `src/native_base/`（本 PR 已建模块）；
- PR04 的提交管道需将 `write_order`/层内 sequence 对接到 admission_ticket；
- PR06A 的发布事务以 `commit_seal` 的单 CAS 事务模式为底座扩展 RetainBatch；
- PR06B 按 §4.3 的收口顺序逐入口加 `workspace-native-v2` 卷格式隔离。
