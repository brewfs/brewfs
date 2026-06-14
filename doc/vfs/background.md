# 后台任务

## 1. 为什么 VFS 需要后台任务

VFS 并不只处理前台请求。

为了让系统长期稳定运行，它还需要一组持续推进的后台逻辑，例如：

- writer 定期 flush
- chunk compaction
- block gc
- 目录项预取任务
- 修改标记清理

这些逻辑有一个共同点：

- 它们不直接由某次用户请求驱动
- 但会持续影响性能、内存占用和系统一致性体验

因此，理解 VFS 不能只看前台 `read/write/rename`，还要看后台怎样“替前台提前做事”。

## 2. 两类后台任务

VFS 当前的后台工作大致可以分成两类。

### 2.1 VFS 级后台任务

由 `VFS` 在构造时统一启动，主要包括：

- compaction worker
- block gc worker

这些任务更偏“存储整理和回收”。

### 2.2 writer 级后台任务

由 `DataWriter` / `FileWriter` 自己启动，主要包括：

- `DataWriter::start_flush_background()`
- `FileWriter::auto_flush()`
- `commit_chunk()` 后台提交循环
- 单 slice upload task

这些任务更偏“把脏写持续推进到稳定状态”。

## 3. `VfsBackgroundConfig`

VFS 使用 `VfsBackgroundConfig` 来统一描述后台任务配置。

它包含：

- `compaction: CompactionWorkerConfig`
- `gc: BlockGcConfig`
- `compact_config: CompactConfig`
- `enabled: bool`

这个结构的意义是：

- 把 `meta` 层 compact 配置转换为 VFS 可直接消费的后台配置
- 让 VFS 构造流程能清晰决定“是否启后台任务”

### 3.1 `from_compact_config()`

这个辅助函数会把更高层的 `CompactConfig` 转成：

- compaction 的扫描间隔与每轮上限
- gc 的 interval 与 block size

也就是说，VFS 并不发明一套完全独立的后台配置，而是把已有 compact 配置映射到后台任务模型中。

## 4. `VfsBackgroundTasks`

`VfsBackgroundTasks` 很小，只持有两个 `JoinHandle`：

- `compaction_handle`
- `gc_handle`

它的定位不是“任务实现”，而是：

- VFS 对后台任务生命周期的持有者

这表示 VFS 至少知道：

- 哪两个后台主任务已经启动
- 它们作为运行时资源被当前实例持有

## 5. VFS 构造阶段如何启动后台任务

在 `VFS::with_meta_layer_with_compact_config()` 中，VFS 会：

1. 基于配置判断后台任务是否启用
2. 调 `start_background_tasks(...)`
3. 把返回的 `VfsBackgroundTasks` 放进 `background_tasks`

这说明后台任务是：

- 随 VFS 实例一起创建
- 而不是全局单例

## 6. `start_background_tasks()`

这个函数是 VFS 级后台任务的总入口。

它会做几件关键事情：

### 6.1 判断是否启用

若 `config.enabled == false`，直接返回 `None`。

这保证：

- 开发/测试场景可以明确关闭后台作业
- VFS 构造逻辑不需要到处散落 enable 判断

### 6.2 创建 `CompactionWorker`

VFS 会基于：

- `meta_store`
- `block_store`
- `layout`
- `compact_config`

构造一个 `CompactionWorker`。

这里说明 VFS 级后台任务已经跨越了：

- 元数据层
- 数据块层

它不是单纯某一层自己的 housekeeping。

### 6.3 为 database store 安装 compaction hook

当底层元数据后端是 database store 时，VFS 会额外挂一个 compaction hook：

- 当某个 chunk 被 compact 完成
- 通过 `MetaClient` 异步 `invalidate_chunk_slices(chunk_id)`

这个设计非常关键，因为 compaction 改变的不只是块数据布局，也可能影响元数据缓存的正确性。

所以 VFS 在这里承担的是：

- 把 chunk 层后台事件转译成 meta client 缓存失效动作

### 6.4 启动 compaction 与 gc

最后通过 `worker.start(...)` 一次性拿到：

- compaction 后台任务句柄
- gc 后台任务句柄

并封装进 `VfsBackgroundTasks`。

## 7. writer 背景任务概览

相比 VFS 级后台任务，writer 的后台任务更分散，也更贴近前台 I/O。

主要有 4 类：

1. 全局 writer 周期 flush
2. 单文件 auto flush
3. 单 slice upload task
4. 单 chunk commit loop

它们共同构成了“把脏写往后端持续推进”的后台机制。

## 8. `DataWriter::start_flush_background()`

这是 writer 体系中的一个周期性全局后台循环。

### 8.1 它做什么

它会按 `flush_all_interval` 定时醒来，并执行：

- `flush_once()`

`flush_once()` 会：

- 遍历当前所有 `FileWriter`
- 对仍有 pending 数据的 writer 执行一次 `flush()`

### 8.2 它的意义

这个循环的价值在于：

- 就算没有显式 `fsync/close`
- 系统也会周期性尝试把脏写推进

它属于典型的“全局兜底推进机制”。

### 8.3 为什么用 `Weak`

实现里使用 `Arc::downgrade(self)`，后台循环每次 tick 后都会尝试 upgrade。

这样当 `DataWriter` 生命周期结束时：

- 后台任务会自然退出
- 不会因为任务自己持有强引用而造成资源泄漏

这是 VFS 后台任务中非常典型的生命周期控制手法。

## 9. `FileWriter::auto_flush()`

这是每个文件自己的后台 flush 循环。

### 9.1 它的目标

`auto_flush()` 的目标不是“替代 fsync”，而是：

- 提前冻结足够老或足够空闲的 Writable slice
- 尽早把它们送去 upload

这样显式 `flush/fsync` 到来时，很多工作已经在后台进行中。

### 9.2 当前依据

它会扫描所有 Writable slice，并根据：

- `age`
- `idle_time`
- 当前 slice 总量
- 随机选半边 chunk 做额外压力释放

来决定是否 freeze。

### 9.3 为什么它重要

没有 auto flush 时，前台 `fsync` 会变成：

- 先冻结
- 再启动 upload
- 再等 commit

也就是承担完整推进成本。

有了 auto flush 后，很多 slice 在前台显式 flush 前就已经进入：

- `Readonly`
- upload in flight

从而降低显式同步操作的尾延迟。

## 10. 单 slice upload task

当一个 slice 被 freeze 后，writer 会通过 `spawn_flush_slice()` 启动后台 upload 任务。

这个后台任务会：

1. 调 `prepare_upload()` 算出该传哪些 block
2. 获取或申请 `slice_id`
3. 用 `DataUploader` 把数据写入块存储
4. 成功则推进上传位置
5. 失败则标记 `Failed`

这类任务非常细粒度：

- 粒度是单个 slice
- 生命周期取决于该 slice 是否完成上传

## 11. `commit_chunk()` 后台循环

这是 writer 里最关键的后台推进器之一。

它的职责是：

- 按 chunk 观察队头 slice
- 等待其 upload 完成
- 将 `SliceDesc` 持久化到元数据层
- 标记 `Committed`
- 失效 reader 缓存
- 弹出已完成 slice

### 11.1 为什么按 chunk 启一个提交循环

因为 chunk 内 slice 之间的顺序和覆盖关系最重要。

按 chunk 独立推进能保证：

- 同一 chunk 上的提交顺序更清晰
- 不同 chunk 之间又能并发推进

这是一种天然和 `chunk_id` 对齐的后台并行模型。

### 11.2 后台循环与前台 flush 的关系

前台 `flush()` 并不自己直接写元数据，它更多是：

- 冻结
- 等待这些 slice 被后台推进到 `Committed`

因此 `commit_chunk()` 实际上是前台 flush 能成功返回的关键后端动力。

## 12. 目录句柄预取任务

后台任务并不只有写路径。

`DirHandle` 也可能带一个目录属性预取任务：

- `prefetch_task`
- `prefetch_done`

它的作用是：

- opendir 拿到目录项后
- 异步预取目录项相关属性
- 为后续 `readdirplus` 或相关访问减少等待

### 12.1 生命周期管理

当 `DirHandle` drop 时：

- 若预取任务仍未完成，会被主动 `abort()`

这让目录预取不会脱离句柄生命周期无限运行。

## 13. `ModifiedTracker` 清理

`ModifiedTracker` 本身不是一个独立后台线程，但它有典型的后台维护语义：

- 通过 `cleanup_older_than(ttl)` 清理过旧修改标记

这类逻辑虽然通常由上层或周期性路径触发，但本质上属于“后台维护本地状态规模”的一部分。

## 14. 后台任务与缓存失效

VFS 后台任务的一个重要共性是：

- 它们不仅做工作推进，还会触发缓存失效

典型例子有：

- compaction 完成后失效 meta client 的 chunk slice 缓存
- writer commit 完成后失效 reader 的范围缓存

所以后台任务不是简单“慢慢整理数据”，而是：

- 改变系统真实状态
- 并同步调整本地缓存视图

## 15. 后台任务与前台语义的边界

一个很重要的原则是：

- 后台任务负责“提前推进”
- 前台强语义路径负责“确保完成”

例如：

- auto flush 提前 freeze 和 upload
- 但 `flush_required()` / `fsync()` 仍必须真正等到需要的数据稳定

这条边界非常关键。

如果把前台强语义也偷偷交给后台“最好努力一下”，就会破坏文件系统语义。

## 16. 生命周期与退出

VFS 后台任务目前主要通过以下方式与对象生命周期绑定：

- VFS 级任务句柄存放在 `background_tasks`
- writer 全局 flush 通过 `Weak<DataWriter>` 自然退出
- 文件级 auto flush 通过 `Weak<Shared>` 自然退出
- 目录预取任务由 `DirHandle::drop()` 中止

这种设计避免了两类常见问题：

- 后台任务永远不退出
- 后台任务过早结束导致前台对象悬空

## 17. 为什么后台任务分散在多个层次

从代码结构看，后台任务并没有集中在单一模块中，而是分散在：

- `fs/mod.rs`
- `io/writer.rs`
- `handles.rs`

这不是坏事，反而反映了任务的归属关系：

- compaction/gc 属于 VFS 级依赖编排
- auto flush / commit 属于 writer 级推进逻辑
- prefetch 属于目录句柄级优化逻辑

把它们硬塞进一个“统一 scheduler”反而会掩盖对象边界。

## 18. 总结

VFS 的后台任务体系可以概括为两条主线：

```text
VFS 级后台:
  compaction + gc

writer 级后台:
  flush background + auto flush + upload + commit
```

它们共同完成的目标是：

- 降低前台请求必须立刻承担的工作量
- 保持缓存、块数据、元数据三者持续向“稳定状态”推进
- 让系统在长时间运行下仍能维持可控的内存和数据布局

所以从设计上看，后台任务不是附属功能，而是 BrewFS 的 VFS 成为“可持续运行系统”的必要组成部分。
