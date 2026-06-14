# 句柄管理

## 1. 为什么 VFS 需要句柄层

在 BrewFS 中，“文件存在”与“文件被打开”是两回事。

元数据层只负责命名空间和持久化属性，而 VFS 还必须管理：

- 一次 `open()` 产生的会话状态
- 读写并发控制
- 打开文件上的局部属性缓存
- 目录流生命周期
- `fh` 到 inode 的映射关系

这些职责集中在 `src/vfs/handles.rs` 和 `src/vfs/fs/mod.rs` 里的 `HandleRegistry` 上。

可以把这一层理解为：

- 面向 VFS 的运行时句柄系统
- 不是内核 FUSE handle 的简单镜像
- 而是“文件会话状态 + 并发门禁 + 缓存入口”的组合

## 2. 句柄体系概览

VFS 里有两类句柄：

- `FileHandle`
  - 表示一个已打开文件
- `DirHandle`
  - 表示一个已打开目录

同时，`HandleRegistry` 负责维护：

- `fh -> FileHandle`
- `fh -> DirHandle`
- `ino -> [fh, fh, ...]`

的运行时映射。

整体关系可以表示为：

```text
open/opendir
   |
   v
HandleRegistry 分配 fh
   |
   +--> FileHandle
   |      - attr cache
   |      - last_offset
   |      - reader/writer 绑定
   |      - HandleGate
   |
   `--> DirHandle
          - entries
          - attr
          - prefetch task
```

## 3. `HandleRegistry`

`HandleRegistry` 定义在 `src/vfs/fs/mod.rs`，是所有句柄的中心注册表。

它内部主要维护 4 个字段：

- `handles: DashMap<u64, Arc<FileHandle<...>>>`
- `inode_handles: DashMap<i64, Vec<u64>>`
- `dir_handles: DashMap<u64, Arc<DirHandle>>`
- `next_fh: AtomicU64`

职责如下：

- 分配新的文件句柄号 `fh`
- 分配新的目录句柄号 `fh`
- 根据 `fh` 找回对应句柄对象
- 根据 inode 找到所有打开句柄
- 判断某个 inode 是否还有写句柄或是否已无句柄
- 在 inode 属性变化后把新 attr 分发给所有已打开句柄

### 3.1 为什么需要 `inode_handles`

只维护 `fh -> handle` 不够，因为很多操作需要 inode 维度视角，例如：

- `truncate` 前拿到同 inode 的所有写句柄并加锁
- `close` 时判断这是不是最后一个打开句柄
- 判断某 inode 当前是否还有写句柄
- 批量更新同 inode 所有关联句柄的 attr

因此 `inode_handles` 是一个反向索引，让 VFS 能快速从 inode 找到所有活跃句柄。

## 4. `FileHandle`

`FileHandle` 表示一次打开文件后的运行时对象。

它包含：

- `fh`
- `ino`
- `opened_at`
- `flags`
- `gate`
- `state`

其中 `state` 内部又维护：

- `attr`
- `last_offset`
- `last_check`
- `reader`
- `writer`

也就是说，`FileHandle` 不只是“一个编号”，而是一个带状态的会话对象。

## 5. `HandleFlags`

`HandleFlags` 是 VFS 对打开语义的归纳，包含：

- `read`
- `write`
- `append`

它来自上层打开请求，但在 VFS 中会继续影响很多运行时行为，例如：

- 是否要给 handle 绑定 writer
- `close` 时是否需要触发 flush / 更新时间戳
- 是否参与“是否还有写句柄”的判断
- O_APPEND 行为是否需要读本地最新 size

## 6. `FileHandleState`

`FileHandleState` 是 `FileHandle` 的内部可变状态，使用 `StdMutex` 保护。

### 6.1 `attr`

这里缓存的是句柄看到的文件属性视图。

用途包括：

- `getattr` 快速返回
- O_APPEND 读取最新 size
- 减少频繁元数据 round-trip

### 6.2 `last_offset`

记录最近一次读/写后的偏移，主要用于：

- 顺序访问跟踪
- 调试和后续优化

### 6.3 `last_check`

用于配合 `ATTR_CACHE_TTL` 做一个很短的 attr TTL 缓存。

当前语义比较简单：

- 如果句柄 attr 距离上次检查还在 TTL 内
- 就优先返回句柄缓存

这使得一些高频 `getattr` 不必每次都访问元数据层。

### 6.4 `reader` / `writer`

这两个字段把句柄和 I/O 对象关联起来：

- 读句柄需要能拿到 `FileReader`
- 写句柄需要能拿到 `FileWriter`

这样 `FileHandle::read()` / `write()` / `flush()` 就能直接转发给底层 I/O 对象。

## 7. `HandleGate`

`HandleGate` 是句柄层最重要的并发控制机制。

它内部维护一个 `GateState`：

- `readers`
- `writers_waiting`
- `writing`

并提供：

- `read_lock()`
- `write_lock()`
- `read_unlock()`
- `write_unlock()`

### 7.1 它解决什么问题

同一个打开文件实例上，读写不能完全放任并发执行，否则会出现：

- 读到同一 handle 上未稳定的写状态
- 写和写之间交错破坏偏移或局部状态
- writer 长期饥饿

`HandleGate` 的作用就是在“同一 handle 内”建立一个轻量门禁。

### 7.2 writer 优先

从实现可以看到，这个 gate 并不是普通读写锁，而是带 writer 优先倾向：

- 当存在 `writers_waiting > 0` 时，新 reader 不再进入
- 这样可以避免读流量把 writer 长期饿死

这对文件系统场景很重要，因为：

- 写往往会改变 size、mtime、本地缓存状态
- 很多后续操作依赖写尽快完成

### 7.3 为什么实现这么小心

`read_lock()` 和 `write_lock()` 都特别注意 `Notify` 的使用顺序。

核心点在于：

- 先拿到 `notified()`
- 先 `enable()`
- 再检查状态

这样做是为了避免经典的 lost wake-up：

- 如果先看状态再等待通知
- 可能刚放下锁，另一边已经 `notify_waiters()`
- 但此时自己尚未真正进入等待队列
- 最后就会永远错过那次唤醒

这部分实现虽然不长，但属于并发正确性的关键。

## 8. `FileHandle` 的主要方法

### 8.1 `reader()` / `writer()`

这两个方法用于把 I/O 对象绑定到 handle 上。

通常发生在 `VFS::open()` 期间：

- 对读句柄绑定 `FileReader`
- 对写句柄绑定 `FileWriter`

### 8.2 `attr()` / `update_attr()`

这组方法用于读取和更新句柄的 attr 缓存。

它们不直接访问元数据层，而是维护“这个已打开句柄当前掌握的属性副本”。

### 8.3 `check_attr()`

用于做一个短 TTL 的 attr 命中判断。

若命中 TTL，则句柄层可直接复用缓存属性，减少元数据请求。

### 8.4 `update_attr_if_changed()`

这是一个“按变化更新”的接口。

当前策略主要看：

- `mtime` 是否变化
- 或者 `size` 是否增大

其目的是在尽量少刷新的前提下，让 handle attr 保持基本新鲜。

### 8.5 `extend_size(min_size)`

每次写入扩展文件时，VFS 会尽量把句柄 attr 中的 size 至少扩到 `min_size`。

它的价值在于：

- 同一进程内其他 O_APPEND 操作能尽快看到新大小
- 不必等完整元数据往返后才更新句柄可见 size

### 8.6 `read()`

`FileHandle::read()` 的逻辑很直接：

1. 先拿 `read_lock`
2. 拿到绑定的 `FileReader`
3. 调 reader 完成读取
4. 更新 `last_offset`

### 8.7 `write()`

`FileHandle::write()` 逻辑类似：

1. 先拿 `write_lock`
2. 调 `write_unlocked()`
3. 底层转给 `FileWriter`
4. 更新 `last_offset`

这保证了同一 handle 上不会出现读写随意交错。

### 8.8 `flush()`

`FileHandle::flush()` 只是句柄侧的转发层：

- 从内部状态取出 writer
- 调 `writer.flush()`

真正的写缓冲冻结、上传和 commit 仍由 writer 子系统完成。

## 9. `DirHandle`

`DirHandle` 表示 opendir 到 releasedir 生命周期内的目录句柄。

它主要保存：

- `ino`
- `attr`
- `entries`
- `opened_at`
- `prefetch_task`
- `prefetch_done`

相比文件句柄，目录句柄更偏“结果缓存对象”。

### 9.1 为什么缓存 `entries`

目录读取往往是分页式的：

- 第一次 `opendir` 拿到目录内容
- 后续多次 `readdir` 根据 offset 分批返回

因此 `DirHandle` 把目录项列表缓存下来，后续 `get_entries(offset)` 按窗口切片返回。

### 9.2 `MAX_READDIR_ENTRIES`

目录句柄内部会限制单次返回项数，默认 `50`。

这个限制的意义是：

- 避免一次返回过多目录项
- 与内核侧 `readdir` 缓冲处理更好配合
- 保持单次响应大小可控

### 9.3 预取任务

`DirHandle` 还可以带一个目录属性预取任务：

- `prefetch_task`
- `prefetch_done`

用途是：

- 当目录项已经拿到后
- 后台异步预取子项 attr
- 让后续 `readdirplus` 或相关访问更快

## 10. `DirHandle` 的 Drop 语义

`DirHandle` 的一个重要点是：

- 在 drop 时，如果预取任务仍未完成，会主动 `abort()`

这说明目录句柄不仅缓存数据，也负责持有一个后台任务生命周期。

这样做可以避免：

- 目录句柄已经释放
- 后台预取还在继续消耗资源

也就是把目录句柄做成一个真正的资源拥有者，而不只是纯数据结构。

## 11. 句柄生命周期

### 11.1 文件句柄

典型流程如下：

1. `VFS::open()` 检查 inode 和 attr
2. `HandleRegistry::allocate()` 分配 `fh`
3. 创建 `FileHandle`
4. 绑定 reader / writer
5. 返回 `fh`
6. 使用期间参与读写和 flush
7. `VFS::close()` 调用 `HandleRegistry::release()`
8. 如果这是该 inode 最后一个句柄，可能进一步回收 inode / writer 状态

### 11.2 目录句柄

典型流程如下：

1. `VFS::opendir()` 获取目录项
2. 创建 `DirHandle`
3. `HandleRegistry::allocate_dir()` 分配目录 `fh`
4. `readdir` / `readdirplus` 使用 `entries`
5. `releasedir` 时释放句柄
6. Drop 时自动处理预取任务清理

## 12. `FileGuard`

在 `src/vfs/fs/mod.rs` 中还有一个辅助对象 `FileGuard`。

它本质上是一个 RAII 包装：

- 内部持有 `VFS + fh`
- 在显式 `close()` 或 drop 时确保句柄关闭

它的价值不是替代 `FileHandle`，而是让某些调用路径在 Rust 风格下更容易做到“忘记 close 也不会泄漏”。

## 13. 这一层的设计价值

句柄层把 VFS 中最容易散掉的几类运行时状态收拢起来：

- open 会话状态
- 短期 attr 缓存
- 同一 handle 内的读写并发门禁
- inode 到句柄的反向索引
- 目录流分页与预取生命周期

如果没有这层，很多逻辑就会被迫分散到：

- `fs/mod.rs`
- `reader.rs`
- `writer.rs`
- `fuse/mod.rs`

最终会让“一个 open 文件”的语义无法收束在单一对象里。

因此，`handles` 子模块的真正作用不是“保存 fh 编号”，而是给 VFS 提供一个稳定的运行时会话模型。
