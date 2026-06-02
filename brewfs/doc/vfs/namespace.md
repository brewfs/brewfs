# 名字空间与属性操作

## 1. 作用范围

`src/vfs/fs/mod.rs` 中除了读写句柄管理，还承担了名字空间相关操作。

这里的“名字空间”包括：

- 路径规范化
- 父目录解析
- 目录项创建与删除
- 重命名
- 硬链接 / 符号链接
- `stat` / `readlink`
- `truncate` / `set_attr` / `fallocate`

这些操作的共同特点是：

- 输入通常是路径、inode 或目录项名
- 真正持久化依赖 `MetaLayer`
- 但 VFS 需要补上本地状态同步和并发协调

## 2. 三层职责划分

理解名字空间操作时，可以把职责拆成三层：

### 2.1 路径层

负责：

- 规范化路径
- 拆出 parent 与 basename
- 判断是否是根目录等特殊路径

主要辅助函数：

- `norm_path()`
- `split_dir_file()`
- `resolve_parent_inode()`

### 2.2 元数据层

负责：

- `lookup`
- `mkdir`
- `create_file`
- `unlink`
- `rename`
- `set_attr`
- `stat`

VFS 最终通过 `meta_*` 辅助函数落到 `MetaLayer`。

### 2.3 本地状态层

负责：

- 更新 `ModifiedTracker`
- 更新本地 inode size
- 更新已打开句柄的 attr 缓存
- 失效 reader / writer 本地状态
- 在危险操作前协调 flush 和句柄锁

真正让这些元数据操作表现得像“一个文件系统”的，是这一层。

## 3. 路径规范化

### 3.1 `norm_path()`

VFS 先把输入路径规整成统一形式：

- 空路径变成 `/`
- 连续 `/` 被压缩
- 中间空段被去掉

这样做的意义是：

- 避免同一路径出现多种字符串表示
- 让后续缓存、日志和元数据解析都更稳定

### 3.2 `split_dir_file()`

这个函数把规范化路径拆成：

- 父目录路径
- basename

例如：

```text
/a/b/c.txt
  -> parent: /a/b
  -> name: c.txt
```

这是大多数目录项操作的共同入口。

### 3.3 `resolve_parent_inode()`

当上层给的是路径时，很多操作最终都需要：

- 先解析父目录
- 再在父目录下对名字做操作

`resolve_parent_inode()` 封装了这一步，并保证：

- 根目录 `/` 直接映射到 root inode
- 非目录父节点返回 `NotADirectory`

## 4. 目录创建

### 4.1 `mkdir_p()`

`mkdir_p()` 提供递归建目录语义，特点是：

- 已存在目录直接复用
- 中间节点若是文件则报 `NotADirectory`
- 缺失节点则逐级创建

它的主流程是：

1. 规范化路径
2. 如果目标已存在，直接返回 inode
3. 从 root 开始逐段 `lookup`
4. 缺失就 `meta_mkdir`
5. 每创建一级后更新 `modified`

因此它是“沿路径逐层推进”的过程，而不是一次性黑盒调用。

### 4.2 `mkdir_err()` / `mkdir_at()`

这组接口提供更接近非递归 `mkdir` 的行为：

- 父目录必须已存在
- 目标已存在且是目录时返回现有 inode
- 目标已存在但不是目录时返回 `AlreadyExists`

其中 `mkdir_at()` 是 inode 直达版本，更适合 FUSE 这类已拿到 parent inode 的路径。

## 5. 文件创建

### 5.1 `create_file()`

`create_file()` 的语义是：

- 先确保父目录存在，相当于隐式 `mkdir_p(parent)`
- 再在父目录下创建文件
- 如果同名文件已存在，直接返回现有 inode

这更像 SDK 友好的便捷接口。

### 5.2 `create_file_in_existing_dir_err()` / `create_file_at()`

这组接口更接近系统调用语义：

- 父目录必须存在
- 可选择 `create_new`
- 同名目录存在时返回 `IsADirectory`
- `create_new=true` 时若文件存在则返回 `AlreadyExists`

和目录创建一样，VFS 提供了：

- 路径版本
- parent inode + name 的直达版本

这样可以同时服务：

- SDK 路径接口
- FUSE 已解析好的请求

## 6. 链接操作

### 6.1 硬链接：`link()` / `link_by_ino()`

硬链接创建流程的关键是：

- 目标必须不是目录
- 新父目录必须存在
- 链接名必须合法

其中 `link_by_ino()` 是更底层也更推荐的路径，因为 FUSE 场景下常常已经拿到了源 inode 和目标 parent inode，不需要再做路径反解。

成功后，VFS 会：

- touch 目标父目录
- touch 源 inode

这说明“修改目录项”和“修改被链接对象”都会留下本地修改痕迹。

### 6.2 符号链接：`create_symlink()` / `create_symlink_at()`

符号链接的处理与硬链接不同：

- 目标字符串不要求在创建时存在
- VFS 只负责创建一条 symlink 目录项和其 inode

成功后同样会更新 parent inode 与新建 symlink inode 的修改时间痕迹。

## 7. 删除操作

### 7.1 `unlink()` / `unlink_at()`

`unlink` 只允许删除：

- 普通文件
- 符号链接

如果目标是目录，则返回 `IsADirectory`。

主流程是：

1. 解析父目录 inode
2. 在父目录下 `lookup_required`
3. `stat` 判断类型
4. 调 `meta_unlink`
5. touch parent 和目标 inode

### 7.2 `rmdir()` / `rmdir_at()`

`rmdir` 的额外约束更多：

- 根目录不能删
- 目标必须是目录
- 目录必须为空

因此 VFS 不会直接盲删，而是先做：

- `lookup_required`
- `stat`
- `readdir`

只有确认空目录后才调用 `meta_rmdir`。

## 8. 重命名

重命名是名字空间里最复杂的一类操作。

### 8.1 `rename()`

路径版 `rename()` 的主要工作其实很简单：

- 规范化 old/new
- 解析 old/new 父目录 inode
- 转给 `rename_at()`

复杂逻辑都在 `rename_at()`。

### 8.2 `rename_at()` 的核心检查

`rename_at()` 会依次处理：

- 名字合法性校验
- 同目录同名 rename 的空操作快速返回
- 源项存在性检查
- 目标 parent 是否为目录
- 如果源是目录，检查是否会把目录移入自己的后代目录，避免形成环

这个“目标 parent 是否是源目录后代”的检查通过 `parent_is_descendant_of()` 实现。

这是目录 rename 正确性的关键。

### 8.3 rename 成功后的本地同步

成功后会：

- touch old parent
- old/new parent 不同则 touch new parent
- touch 源 inode

也就是说，VFS 至少会记录：

- 两边目录树发生了变化
- 被移动对象本身也发生了名字空间变化

### 8.4 `rename_with_flags()`

VFS 还支持接近 `renameat2` 的扩展语义：

- `noreplace`
- `exchange`
- `whiteout` 占位

目前真正实现的重点是：

- `rename_noreplace()`
- `rename_exchange()`

其中：

- `rename_noreplace()` 会先检查目标是否存在
- `rename_exchange()` 要求源和目标都存在，并走元数据层的交换接口

### 8.5 `can_rename()`

这个接口用于“只检查，不执行”。

它会帮助判断：

- 源是否存在
- 目标父目录是否存在
- 目标若存在，类型是否允许被替换
- 目录替换目录时是否为空

这相当于把 rename 的前置校验单独暴露出来。

## 9. 属性查询

### 9.1 `stat()`

`stat()` 的关键特点不是“查元数据”，而是：

- 以元数据结果为基础
- 再用本地 inode size 覆盖 size 字段

这体现了 BrewFS 的 close-to-open 本地视角：

- 如果当前进程内有更新过的本地 size
- 它应该被认为比元数据层返回值更新

### 9.2 `stat_ino()`

按 inode 查询时也采用相同策略：

- 先查 meta
- 再用 `inode_size_cached()` 覆盖 size

这让 FUSE 场景可以避免路径回溯，同时保持本地可见 size 一致。

### 9.3 `blocks_for_attr()`

这个辅助函数不是普通 `stat` 字段拷贝，而是：

- 优先使用本地 `inode.committed_bytes()`
- 回退到 `attr.size`

用于估算更接近真实占用的 `st_blocks`。

## 10. `readlink()`

符号链接读取比较直接：

- 路径版先解析 path -> ino + kind
- 必须确认 kind 是 `Symlink`
- 再调用 `readlink_ino()`

这里 VFS 的价值主要在于类型检查和路径解析，而真正目标字符串仍来自元数据层。

## 11. `truncate()`

`truncate` 是名字空间操作里和 writer/reader 交互最重的一类。

### 11.1 为什么它复杂

`truncate` 不能只改元数据层 size，因为还必须处理：

- 本地脏写是否会丢失
- reader 是否还缓存旧内容
- 已打开句柄是否立即看到新 size
- `st_blocks` 统计是否要重置

### 11.2 `truncate_inode()` 的主流程

它的大致流程是：

1. 先 `writer.flush_required()`
   - 避免已有脏数据在后续 `clear()` 时无声丢失
2. 获取 inode 级 `append_lock`
3. 获取该 inode 所有打开句柄的写锁
4. 执行 `meta_truncate(...)`
5. `reader.invalidate_all(...)`
6. `writer.clear(...)`
7. 更新本地 `Inode` size
8. `reset_committed_bytes(size)`
9. 更新句柄 attr cache 中的 size
10. touch inode

这里最关键的点是：

- 先 flush，再拿 mutation lock
- 再把 truncate 变成一个“清晰边界”

这样可以避免 truncate 和并发写入把本地状态撕裂。

## 12. `fallocate_ino()`

当前 `fallocate_ino()` 提供的是一个最小可用语义：

- 不做真实预分配
- 但保证 `mode=0` 下文件逻辑大小至少扩到 `offset + length`

这主要是为了支持像 `generic/438` 这类依赖 `posix_fallocate` 的 mmap 测试。

因此它的定位不是完整 fallocate 实现，而是：

- 先补文件系统基本可用语义
- 明确拒绝更复杂的 punch/collapse/zero-range 模式

## 13. `set_attr()`

`set_attr()` 是另一个和本地状态同步强相关的接口。

### 13.1 size 变更与非 size 变更分开处理

当前实现中，`size` 变更会走更重的逻辑：

- 先 flush writer
- 再拿 inode mutation lock
- 再锁住相关 handle
- 再执行 `meta_truncate`
- 再清理 reader/writer 本地状态

而非 size 变更则相对简单，只走元数据更新。

### 13.2 为什么要这样拆

因为 `size` 是最容易影响：

- page cache
- mmap
- writer 脏数据
- handle attr

的一类属性。

如果它和普通 mode/uid/gid/mtime 一样处理，会很容易出现并发可见性问题。

## 14. touch 与 `ModifiedTracker`

多数名字空间操作完成后都会 `touch(...)`。

它的作用不是持久化时间戳，而是：

- 在 VFS 本地记录“这个 inode 最近修改过”

这个信息可用于：

- 判断某 inode 自某时刻后是否变动
- 辅助缓存一致性和上层逻辑

因此 `ModifiedTracker` 是名字空间操作和后台状态之间的一条弱耦合通道。

## 15. 为什么名字空间逻辑放在 VFS

这些操作虽然最终都落到元数据层，但它们不能只停在 `MetaLayer`，原因有 4 点：

- 路径规范化是 VFS 责任
- 本地 inode / handle / reader / writer 状态要同步
- 并发危险操作要和写路径协调
- 返回给 FUSE/SDK 的语义要更接近文件系统，而不是原始存储接口

因此，VFS 的名字空间层本质上是：

- 路径语义层
- 本地状态协调层
- 元数据操作编排层

## 16. 总结

VFS 名字空间操作的共性可以概括为：

```text
输入路径/ino
  -> 做路径与类型校验
  -> 调用 meta_* 真正执行
  -> 同步本地 inode/handle/reader/writer 状态
  -> 记录 modified
```

它们表面上是一个个独立 API，但本质上共享同一套设计原则：

- 元数据持久化由 `MetaLayer` 完成
- 文件系统语义由 VFS 补齐
- 本地状态一致性由 VFS 维护

这也是为什么 `fs/mod.rs` 中名字空间逻辑虽然分散，但读起来会反复看到相似的结构。
