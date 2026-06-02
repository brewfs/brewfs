# 写路径

## 1. 总览

`src/vfs/io/writer.rs` 实现了 BrewFS 的写入引擎。

它并不把每次写调用直接同步落到元数据层和对象存储，而是采用：

- 本地缓冲
- slice 追加
- 后台 upload
- 元数据 commit

的分阶段模型。

一个典型写路径如下：

```text
VFS / FileHandle::write
  -> FileWriter::write_at
  -> 按 chunk 切分
  -> 写入 Writable slice
  -> 背景 upload
  -> commit_chunk 写入元数据
  -> reader 缓存失效
```

这使得 BrewFS 可以把“文件系统写入”拆成：

- 前台尽快返回
- 后台逐步把数据推进到稳定状态

## 2. 两层对象

writer 体系同样分成两层：

- `DataWriter`
  - inode 级 writer 注册表
  - 管理所有 `FileWriter`
- `FileWriter`
  - 单文件写缓冲、flush 和 commit 协调者

可以把它理解为：

- `DataWriter` 管“有哪些文件正在写”
- `FileWriter` 管“某一个文件如何写”

## 3. `DataWriter`

`DataWriter` 持有：

- `config`
- `backend`
- `reader`
- `files`
- `buffer_usage`

职责主要有 5 类：

- 为 inode `ensure_file(...)`
- 提供 `flush_if_exists(...)`
- 提供 `flush_required(...)`
- 提供 `overlay_dirty_if_exists(...)`
- 提供 `clear(...)` / `release(...)`

这里最重要的一点是：writer 并不是孤立存在的。

`DataWriter` 同时持有 `reader`，因为写入提交后必须主动失效读缓存。

## 4. `FileWriter`

`FileWriter` 是单文件写路径的核心对象。

它内部主要围绕一个 `Shared` 结构运行，后者持有：

- `inode`
- `config`
- `buffer_usage`
- `inner`
- `write_notify`
- `flush_notify`
- `backend`
- `reader`
- `write_gen`
- `last_flushed_gen`

其中：

- `inner`
  - 保存真正的 chunk / slice 运行时状态
- `write_notify`
  - 用于 flush 阻塞写入时唤醒写方
- `flush_notify`
  - 用于 commit / flush 之间同步
- `write_gen` / `last_flushed_gen`
  - 用于快速判断是否还有“新写入未被 flush”

## 5. writer 中的核心状态对象

### 5.1 `ChunkState`

writer 按 chunk 维度组织写入。

`ChunkState` 内部主要维护该 chunk 上的 slice 列表。

一个 chunk 中可能同时存在多个 slice，因为：

- 多次写入可能命中同一 chunk
- 新写可能覆盖旧写
- flush / commit 是逐 slice 推进的

### 5.2 `SliceState`

`SliceState` 是 writer 的最核心状态单元。

它包含：

- `state`
- `chunk_id`
- `slice_id`
- `offset`
- `uploaded`
- `uploading`
- `data`
- `usage`
- `err`
- `notify`
- `started`
- `last_mod`

它描述的是：

- 一段属于某个 chunk 的 append-only 脏数据
- 当前写到了哪里
- 已上传到了哪里
- 是否已只读 / 已提交 / 失败

## 6. writer 的状态机

writer 的 slice 状态机是整个写路径的关键。

当前主要状态包括：

- `Writable`
  - 仍可继续追加写入
- `Readonly`
  - 已冻结，不再允许新写入
- `Uploaded`
  - 数据已成功上传到块存储
- `Failed`
  - 上传失败，等待重试或错误处理
- `Committed`
  - 对应 slice 元数据已经写入元数据层，对 reader 可见

最重要的转换路径是：

```text
Writable
  -> Readonly
  -> Uploaded
  -> Committed
```

其中：

- `Writable -> Readonly`
  - 由 `flush()` 或 `auto_flush` 触发冻结
- `Readonly -> Uploaded`
  - 由后台 upload 任务完成
- `Uploaded -> Committed`
  - 由 `commit_chunk` 把 slice 元数据写入 meta 后完成

只有 `Committed` 的 slice 才是读路径真正稳定可见的数据。

## 7. 为什么 slice 是 append-only

writer 当前采用 append-only slice 策略，而不是原地修改旧 slice。

这么做有几个明显好处：

- 状态机清晰，已上传区域不再被覆盖
- upload / commit 可以按 slice 线性推进
- 新旧写覆盖关系可以靠 slice 顺序表达
- 与对象存储更匹配，不需要随机覆盖远端对象

代价是：

- 同一 chunk 可能积累多个 slice
- 需要 compaction 或后台整理

但从分布式文件系统和对象存储语义来看，这是更现实的权衡。

## 8. `write_at()` 主流程

`FileWriter::write_at()` 是写路径的前台入口。

它的主要流程如下。

### 8.1 内存 back-pressure

写入前先执行 `back_pressure()`。

逻辑和 reader 类似：

- 低于软限制：直接继续
- 高于软限制：短暂等待
- 高于硬限制：持续等待直到下降
- 超时则报错

这保证 writer 不会因为大量脏写无限占用内存。

### 8.2 与 flush 串行化

writer 在真正写入前会检查：

- 是否存在进行中的 flush

如果 `flush_waiting > 0`，新写会挂起，直到 flush 完成。

这一步非常重要，因为 flush 的语义是：

- 冻结当前一批 slice
- 等待它们推进到稳定状态

如果 flush 中间还让新写任意混入，语义会变得很难界定。

### 8.3 切分 chunk span

writer 同样使用 `split_chunk_spans(...)` 把文件偏移范围拆成若干 chunk 内区间。

随后对每个 chunk span：

- 取出或创建对应 `ChunkState`
- 通过 `ChunkHandle::write_at(...)` 追加进某个 slice

### 8.4 触发 upload / commit

`ChunkHandle::write_at(...)` 返回一个 `WriteAction`，其中可能包含：

- 哪些 slice 需要立即触发 upload
- 是否需要启动 `commit_chunk`

于是前台写入结束前，会把这些后台动作异步 kick 出去：

- `spawn_flush_slice(...)`
- `tokio::spawn(commit_chunk(...))`

### 8.5 更新 inode size

写入完成后，writer 会立刻更新本地 `inode.file_size()`。

这里强调的是“本地可见 size”：

- 不等元数据写入完成
- 先保证当前进程视角能看到新长度

这对 O_APPEND、后续读边界判断、FUSE getattr 等场景都很重要。

## 9. `ChunkHandle::write_at()` 在做什么

虽然 `FileWriter::write_at()` 是入口，但真正决定“写到哪个 slice”的是 `ChunkHandle::write_at()`。

它会尝试：

- 找到仍然可写的 slice
- 检查是否允许在当前 offset 追加
- 如不能写，则创建新 slice
- 必要时决定是否应该冻结某些 slice

所以它既是数据布局逻辑，也是 slice 生命周期入口。

## 10. `overlay_dirty()`

`overlay_dirty()` 是 writer 和 reader/FUSE writeback 协作的重要接口。

它的作用是：

- 把仍停留在本地脏缓冲里的最新数据叠加到一个读缓冲区上

这样即使某些新写还没有 commit 到元数据层，也能在需要时提供“更接近最新状态”的可见性。

这里有两个重要点：

- slice 按创建顺序遍历
- 后写的 slice 会覆盖前写的脏数据

这正好符合“后写覆盖前写”的文件语义。

## 11. `flush()`

`flush()` 是 writer 里最关键的强语义操作之一。

### 11.1 它的语义

`flush()` 的目标不是“把当前 chunk 清空”，而是：

- 冻结 flush 开始时存在的那一批 slice
- 等待它们都进入 `Committed` 或 `Failed`

这个设计非常重要，因为在持续写流量下：

- 如果按“等 chunk 为空”判断 flush 完成
- 同一 chunk 上源源不断的新 slice 会让 flush 永远不返回

所以当前实现采用了“按快照等待”的方式：

1. 抓取当前存在的所有 slice 快照
2. 冻结其中仍可写的 slice
3. 启动这些 slice 的 upload
4. 等待这些特定 slice 进入终态

新产生的 slice 不属于本次 flush 范围，而是留给下一次。

### 11.2 flush 为什么会阻塞新写

在 flush 期间，writer 会增加 `flush_waiting`，让新写挂起。

这是因为 flush 的语义需要一个稳定边界：

- 哪些 slice 属于“flush 前的数据”
- 哪些 slice 属于“flush 后的新数据”

这个边界必须清晰，否则显式 `fsync/flush/truncate` 会失去意义。

## 12. 上传阶段：`spawn_flush_slice()`

当某个 slice 被冻结后，就有资格进入 upload。

`spawn_flush_slice()` 会异步启动上传任务，大致流程是：

1. 调 `prepare_upload()` 得到待上传块
2. 为 slice 获取或分配 `slice_id`
3. 用 `DataUploader::write_at_vectored(...)` 把数据写入块存储
4. 成功后推进 `uploaded`
5. 失败则标记 `Failed`

这里 writer 并不是一次只传整个 slice，而是按内部 block 组织上传。

## 13. `commit_chunk()`

`commit_chunk()` 是后台提交线程，是 writer 最关键的“数据变可见”阶段。

它的职责是：

- 观察某个 chunk 的最前 slice
- 等它上传完成
- 把对应 `SliceDesc` 写入元数据层
- 成功后标记为 `Committed`
- 失效 reader 缓存
- 从 chunk 队列头部弹出已完成 slice

### 13.1 为什么按 chunk、按 front slice 提交

同一个 chunk 上的 slice 按顺序推进有两个好处：

- 覆盖关系和提交顺序更容易维护
- chunk 队列天然形成一个前进方向

因此 `commit_chunk()` 更像“按 chunk 排队消费已上传 slice”的后台线程。

### 13.2 commit 成功后还会做什么

成功后还会额外做两件关键事情：

- `inode.add_committed_bytes(...)`
  - 让 `st_blocks` 统计更准确
- `reader.invalidate(...)`
  - 让读缓存意识到对应范围已经被新数据覆盖

这说明 commit 不只是“写元数据”那么简单，而是：

- 持久化状态推进点
- 读写协同点

## 14. `auto_flush()`

`auto_flush()` 是每个 `FileWriter` 自带的后台循环。

它的任务不是直接 commit，而是：

- 定期扫描 Writable slice
- 对满足条件的 slice 做 freeze
- 启动它们的 upload

### 14.1 为什么需要 auto flush

如果没有它，系统就会过度依赖前台路径：

- 只有显式 `flush/fsync` 才冻结 slice
- 前台第一次要求稳定可见时，必须自己承担整轮 upload + commit 启动成本

这会导致：

- `fsync` 延迟明显变大
- 后台几乎不主动推进写入

auto flush 的价值就是“提前做事”。

### 14.2 当前触发依据

当前 auto flush 会综合考虑：

- slice 年龄
- 空闲时间
- slice 总量是否过多

特别是较短的 `AUTO_FLUSH_MAX_AGE`，其目标是让背景线程更早把 Writable slice 冻结并送进 upload 阶段，从而让后续 `fsync` 更多是在等待已经开始的工作，而不是从零启动。

## 15. `flush_required()` 与 `flush_if_exists()`

`DataWriter` 暴露了两个常用入口：

- `flush_if_exists()`
  - 最多尝试刷新，不强制向上传播错误
- `flush_required()`
  - 要求有 pending 就真正 flush，并向上传播错误

这两个接口会被上层 VFS 用在不同场景：

- 读前可见性整理
- `truncate`
- `setattr(size)`
- `flush`
- `fsync`
- `close`

因此它们是 writer 与 `fs/mod.rs` 之间的重要桥梁。

## 16. writer 与 VFS 的关系

writer 本身不理解：

- 路径
- 权限
- 目录树
- FUSE 协议

这些都由 VFS 负责。

VFS 负责把合适的 inode、offset、data 交给 writer，并在需要时调用：

- `flush_required()`
- `clear()`
- `release()`
- `overlay_dirty_if_exists()`

可以理解为：

- writer 负责“如何把一堆脏字节推进成可见 slice”
- VFS 负责“什么时候应该推进，以及推进后谁需要同步”

## 17. writer 与 reader 的关系

writer 与 reader 的交互非常紧密，主要有两条线。

### 17.1 写后失效读缓存

当 commit 成功后，writer 会通知 reader 对应区间失效。

这样下次读不会继续复用旧 slice。

### 17.2 读时叠加 dirty 数据

对于尚未 commit、但已经进入本地写缓冲的数据，writer 可以通过 `overlay_dirty()` 提供最新覆盖内容。

这使 reader / VFS 有机会在特定路径下观察到“尚未完全持久化但本地已经写入”的内容。

## 18. 为什么 writer 复杂

writer 复杂的根本原因不是代码量，而是它要同时满足几种彼此拉扯的目标：

- 前台写不能太慢
- `fsync/flush` 又必须有明确语义
- 后端是块存储 + 元数据分离
- 同一 chunk 上可能存在连续覆盖
- reader 还要与未完成写入协作

所以 writer 不得不同时具备：

- 缓冲能力
- 状态机
- 后台任务推进
- 强语义操作边界
- 读写协同能力

## 19. 设计收益

当前 writer 设计带来的主要收益有：

- 把前台写与后端提交解耦
- 允许 slice append-only，适配对象存储
- 通过 chunk 级提交线程维持明确推进顺序
- 通过 auto flush 提前把脏数据推到后台
- 通过 `flush()` 快照等待机制保证前进性
- 通过 reader 失效和 dirty overlay 与读路径协作

总结起来，writer 是 VFS 里最像“引擎”的部分。

它不只是缓存写数据，而是在持续推动一批本地脏字节沿着：

```text
Writable -> Readonly -> Uploaded -> Committed
```

这条路径前进，直到它们真正成为对整个系统稳定可见的数据。
