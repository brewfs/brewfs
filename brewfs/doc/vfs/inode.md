# Inode 管理

## 1. 作用

`src/vfs/inode.rs` 中的 `Inode` 不是元数据层里“完整 inode 记录”的镜像，而是 VFS 在本地维护的一个轻量运行态对象。

它主要承担两类职责：

- 在当前进程内缓存某个文件 inode 的“本地可见大小”
- 维护一个独立于逻辑文件长度的 `committed_bytes` 计数，用于反映真实已提交的数据量

因此，`Inode` 的定位更接近：

- 运行时 inode 视图
- 本地协作对象
- reader / writer / handle 之间共享的文件状态

而不是：

- 元数据持久化实体
- 目录树中的命名节点

## 2. 数据结构

`Inode` 当前包含 4 个字段：

```rust
pub(crate) struct Inode {
    ino: i64,
    length_rx: watch::Receiver<u64>,
    length_tx: watch::Sender<u64>,
    committed_bytes: Arc<AtomicU64>,
}
```

字段含义如下：

- `ino`
  - inode 编号
- `length_tx` / `length_rx`
  - 一个 `tokio::sync::watch` 通道
  - 用来广播当前文件的本地可见大小
- `committed_bytes`
  - 当前已经成功提交到元数据层的数据量
  - 不是逻辑大小，而是“已落稳”的字节统计

## 3. 为什么用 `watch`

VFS 没有把大小缓存成一个普通 `AtomicU64`，而是用了 `watch` 通道。

原因是 `watch` 同时满足两件事：

- 读取方能随时看到“最近一次发布的值”
- 更新方可以把新 size 广播给所有持有 receiver 的对象

对 VFS 来说，这很适合描述“文件大小”这种状态：

- 它是单值，不是日志流
- 只关心最新状态，不关心历史
- 可能被多个 reader / writer / handle 同时观察

这比手工维护一组订阅者简单很多，也比把 size 到处拷贝一份更一致。

## 4. 文件大小的两种含义

理解 `Inode` 的关键，在于区分两种“大小”。

### 4.1 逻辑文件大小

逻辑大小由 `file_size()` 表示，对应：

- 当前进程内应该看到的文件长度
- 写入扩容后立即对本地路径可见的 size
- `truncate` / `setattr(size)` 后更新的 size

它不要求每个字节都已经真正提交到元数据层。

### 4.2 已提交字节数

`committed_bytes()` 用于表示：

- 已经 upload 完成
- 且对应 slice 元数据已经成功 commit

的字节总量。

这部分主要用于估算 `st_blocks`，尤其是稀疏文件场景。

如果只拿逻辑 size 计算 `st_blocks`，会把大量逻辑洞也算成“已占用块数”，结果偏大；因此 VFS 引入了单独的 committed-bytes 计数。

## 5. 生命周期

### 5.1 创建

`Inode::new(ino, size)` 会创建一个新的运行时 inode：

- watch 通道初始值设为 `size`
- `committed_bytes` 初始值也设为 `size`

这是一种保守初始化策略：

- 对新建文件，`size=0`，天然正确
- 对第一次打开的已有文件，VFS 默认认为现有字节都已经是已提交状态

这不意味着系统证明了每个字节都真实存在，而是为了让“已有文件初次接入本地运行态”有一个合理起点。

### 5.2 注册

`VFS` 不会为所有 inode 预先创建本地对象，而是在需要时通过 `ensure_inode_registered()` 注册：

- 先检查 `state.inodes` 中是否已有对象
- 如果没有，就从元数据层取 `attr`
- 只对普通文件创建 `Inode`
- 放入 `DashMap<i64, Arc<Inode>>`

这说明 `Inode` 是懒创建的运行态缓存，而不是全局常驻索引。

### 5.3 删除

当某个 inode 不再有打开句柄，而且本地状态允许回收时，`VFS` 会把它从 `state.inodes` 中移除。

因此 `Inode` 生命周期通常和“当前进程内是否正在使用这个文件”相关，而不是和文件在元数据层是否存在完全绑定。

## 6. 关键接口

### 6.1 `ino()`

返回 inode 编号，用于：

- chunk id 计算
- handle / writer / reader 关联
- 日志与状态映射

### 6.2 `file_size()`

读取 watch 中的当前值，表示 VFS 本地视角下的逻辑大小。

这通常用于：

- 读路径裁剪
- 写路径决定是否扩容
- FUSE / SDK 返回属性时的本地 size 参考

### 6.3 `update_size(new_size)`

通过 `watch::Sender` 广播新的文件大小。

这个更新通常发生在：

- 写路径扩容
- `truncate`
- `setattr(size)`
- 需要把本地句柄和 inode size 统一起来的场景

### 6.4 `committed_bytes()`

返回当前已提交字节数。

它的典型用途是：

- 估算 `st_blocks`
- 让稀疏文件的空间统计更接近真实后端占用

### 6.5 `add_committed_bytes(n)`

当某个 slice 成功写入元数据层后，writer 会调用这个方法累加已提交字节数。

它表达的是：

- “这批数据现在对 reader 可见了”
- “这批数据可以计入真实已落稳数据量”

### 6.6 `reset_committed_bytes(size)`

这个方法主要出现在 `truncate` 之后。

因为 truncate 会让旧的提交统计失去意义，所以需要把 committed-bytes 重置到新的目标大小。

典型场景：

- truncate 到 0，重置为 0
- 缩短文件，重置为保留部分大小
- 扩展文件，保留逻辑大小，但新扩出的洞不会自动增加真实提交量

## 7. `Inode` 与句柄缓存的关系

`Inode` 不替代句柄自己的属性缓存。

两者职责不同：

- `Inode`
  - 表示 inode 级共享状态
  - 特别关心 size 和 committed-bytes
- `FileHandle`
  - 表示某个打开文件实例的局部状态
  - 还会缓存 attr、last_offset、reader / writer 绑定关系

可以把它们理解为：

- `Inode` 是跨句柄共享的文件运行态
- `FileHandle` 是每次 open 产生的会话级视图

## 8. `Inode` 与 reader / writer 的关系

`Inode` 是 reader 和 writer 协作的一个公共锚点。

### 对 writer 来说

writer 会在以下场景更新 inode：

- 写入扩展后更新本地 size
- commit 成功后增加 committed-bytes
- truncate 后重置 committed-bytes

### 对 reader 来说

reader 依赖 inode 的本地 size 来判断：

- 当前文件长度
- 读请求边界
- 某些本地状态是否已经变化

因此，虽然 `Inode` 结构简单，但它是读写路径之间共享的最小状态单元。

## 9. 为什么不把所有属性都放进 `Inode`

当前 `Inode` 非常克制，只保留：

- `ino`
- `size`
- `committed_bytes`

原因是 VFS 更关心：

- 哪些状态需要高频读取
- 哪些状态需要多对象共享
- 哪些状态需要本地快速更新

相比之下，mode/uid/gid/mtime 等属性：

- 变更频率更低
- 更适合放在句柄 attr 缓存或元数据层查询结果中
- 不一定值得为每个 inode 长期维护一份高频共享运行态

这种设计让 `Inode` 保持轻量，也降低了本地状态同步复杂度。

## 10. 设计收益

`Inode` 这个对象虽然不大，但为 VFS 提供了几个关键收益：

- 让本地 size 可以被多个组件一致地观察
- 让写后扩容不必每次都立刻依赖元数据层往返
- 让 `st_blocks` 的估算能区分逻辑长度与真实提交量
- 为 truncate / commit / close 等复杂路径提供一个共享状态载体

总结起来，`Inode` 是 VFS 的“最小共享文件状态对象”。

它把真正高频、真正需要跨组件共享的那部分信息提炼了出来，从而让 `handles`、`reader`、`writer` 和 `fs/mod.rs` 之间能以较低成本协作。
