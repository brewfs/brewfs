# VFS 模块总览

## 1. 定位

`src/vfs/` 是 BrewFS 的虚拟文件系统层，位于：

- 上层：`src/fuse/`、`src/vfs/sdk.rs`
- 下层：`src/meta/`、`src/chunk/`

它的核心职责不是“直接存数据”，而是把文件系统视角的操作组织成可执行的数据与元数据流程，并补上本地状态、缓存、一致性控制和句柄生命周期管理。

如果把 BrewFS 看成一个分层系统，可以粗略理解为：

```text
FUSE / SDK
    |
    v
VFS
    |------> MetaLayer / MetaClient
    |
    `------> DataReader / DataWriter -> BlockStore
```

VFS 是“语义层”和“编排层”：

- 对上提供接近 POSIX 的接口，例如 `open`、`read`、`write`、`rename`、`truncate`
- 对下协调元数据、对象块、缓存和后台任务
- 在本地维护足够的运行态，使单次文件操作不必每一步都直接访问后端

## 2. 模块拆分

`src/vfs/` 主要由以下几部分构成：

```text
src/vfs/
├── mod.rs
├── inode.rs
├── handles.rs
├── backend.rs
├── config.rs
├── error.rs
├── meta_ops.rs
├── fs/mod.rs
└── io/
    ├── reader.rs
    └── writer.rs
```

职责划分如下：

- `mod.rs`
  - 模块入口
  - 公开基础子模块
  - 提供 `chunk_id_for()` / `extract_ino_and_chunk_index()` 这类公共辅助函数
- `inode.rs`
  - 定义运行时 `Inode` 对象
  - 维护本地可见 size 和 committed-bytes 计数
- `handles.rs`
  - 管理 `FileHandle`、`DirHandle`
  - 定义句柄级并发控制 `HandleGate`
- `fs/mod.rs`
  - VFS 主实现
  - 负责对象组装、命名空间操作、句柄生命周期和后台任务接入
- `io/reader.rs`
  - 读路径：根据 slice 元数据组装并读取数据
- `io/writer.rs`
  - 写路径：缓冲、切片、上传、提交元数据
- `meta_ops.rs`
  - VFS 对元数据层的辅助封装
- `backend.rs`
  - 将 `BlockStore + MetaLayer` 组合为 reader / writer 可复用的后端对象

## 3. 三个核心对象

### 3.1 `VFS`

`VFS<S, M>` 是外部真正持有并调用的入口对象，定义在 `src/vfs/fs/mod.rs`。

它内部主要持有三部分：

- `core: Arc<VfsCore<S, M>>`
- `state: Arc<VfsState<S, M>>`
- `background_tasks: Option<VfsBackgroundTasks>`

其中：

- `core` 保存相对稳定、偏配置和依赖注入的部分
- `state` 保存运行时会不断变化的本地状态
- `background_tasks` 保存 compaction / gc 等后台任务句柄

这种拆分让 `VFS` 在 clone 时只复制核心依赖与状态引用，不复制后台任务本身。

### 3.2 `VfsCore`

`VfsCore` 表示 VFS 的“静态核心上下文”，包含：

- `layout: ChunkLayout`
- `backend: Arc<Backend<S, M>>`
- `meta_layer: Arc<M>`
- `root: i64`

它回答的是“VFS 依赖什么”：

- 文件布局参数是什么
- 数据层如何访问
- 元数据层怎么访问
- 根 inode 是多少

### 3.3 `VfsState`

`VfsState` 表示 VFS 的“运行时状态”，包含：

- `handles: HandleRegistry<S, M>`
- `inodes: DashMap<i64, Arc<Inode>>`
- `reader: Arc<DataReader<S, M>>`
- `writer: Arc<DataWriter<S, M>>`
- `modified: ModifiedTracker`
- `append_locks: DashMap<i64, Arc<Mutex<()>>>`

它回答的是“VFS 当前正在处理什么”：

- 哪些 inode 在本地有运行态缓存
- 哪些文件/目录句柄处于打开状态
- 当前 writer / reader 对象是什么
- 哪些 inode 最近被修改过
- 哪些 inode 的写操作需要串行化

## 4. 构造流程

VFS 的典型构造流程如下：

1. 创建 `BlockStore`
2. 创建或包装 `MetaStore`
3. 构造 `MetaClient`
4. 初始化 `MetaClient`
5. 创建 `Backend`
6. 创建 `VfsState`
   - 内部同时构造 `DataReader`
   - 内部同时构造 `DataWriter`
   - 启动 writer 的后台 flush 循环
7. 创建 `VfsCore`
8. 视配置决定是否启动 compaction / gc 后台任务

可以把这个过程理解为：VFS 把底层依赖注入进来，再把“局部运行时”一起搭起来。

## 5. VFS 管什么

VFS 主要负责 5 类事情。

### 5.1 命名空间与属性

VFS 对外提供：

- `lookup`
- `mkdir_p`
- `create_file`
- `unlink`
- `rmdir`
- `rename`
- `link`
- `symlink`
- `truncate`
- `set_attr`
- `stat`
- `stat_fs`

这些操作最终仍要落到元数据层，但 VFS 会负责：

- 路径规范化
- 目录 / 文件类型检查
- 本地运行态同步
- 本地 reader / writer 缓存失效
- 与已打开句柄之间的协同

### 5.2 读路径组织

VFS 不直接读块，而是把请求交给 `DataReader` / `FileReader`。

它负责：

- 打开句柄时关联 reader
- 读前检查本地状态
- 必要时触发 pending writer 数据刷新
- 在 inode / handle 层维护可见 size

### 5.3 写路径组织

VFS 不直接上传数据，而是把请求交给 `DataWriter` / `FileWriter`。

它负责：

- 把写请求路由到正确 inode 的 writer
- 管理写后 reader 缓存失效
- 本地扩展 size
- 在 `truncate`、`setattr(size)`、`close`、`fsync` 等场景下协调 writer 状态

### 5.4 句柄生命周期

VFS 负责统一管理：

- 文件句柄 `FileHandle`
- 目录句柄 `DirHandle`

包括：

- 分配 `fh`
- 绑定到 inode
- 维护句柄属性缓存
- 跟踪是否存在写句柄
- 在 `close` / `releasedir` 时做清理

### 5.5 后台任务接入

VFS 既不是 compaction 的实现者，也不是 gc 的实现者，但它负责：

- 按配置启动后台任务
- 持有任务句柄
- 在生命周期结束时停止这些任务

## 6. 数据流

### 6.1 读路径

一个典型的读路径大致如下：

```text
FUSE/SDK read
  -> VFS::read / FileHandle::read
  -> FileReader / DataReader
  -> MetaLayer 查询 slice
  -> BlockStore 读取数据
  -> 返回拼装后的字节流
```

VFS 在这里的关键价值是：

- 通过 handle 和 inode 保留本地运行态
- 和 writer 交互，避免读到过旧数据
- 为后续读取复用 reader 对象和缓存

### 6.2 写路径

一个典型的写路径大致如下：

```text
FUSE/SDK write
  -> VFS::write / write_ino / write_cached_ino
  -> FileWriter::write_at
  -> 生成 slice
  -> 后台 upload
  -> commit_chunk 写元数据
  -> reader 缓存失效 / inode size 更新
```

这里 VFS 的关键职责是：

- 确保同一 inode 的某些修改互斥执行
- 保持本地 size 尽快可见
- 让显式 `flush/fsync/close` 能驱动 writer 进入稳定状态

## 7. 一致性与边界

VFS 保证的是“当前进程视角下的文件系统语义协调”，不是跨所有客户端的强全局一致调度器。

当前可以把它的边界理解为：

- 句柄内并发通过 `HandleGate` 做协调
- inode 级危险写操作通过 `append_lock` 串行化
- 读写真正可见性仍取决于 writer 是否已经把 slice commit 到元数据层
- 元数据的持久化与目录项变更最终由 `MetaLayer` 保证
- 块数据上传和读取最终由 `BlockStore` / `chunk` 子系统保证

也就是说，VFS 的角色更接近：

- 语义协调器
- 本地状态缓存
- 调度与编排层

而不是：

- 终极持久化层
- 单独的事务系统

## 8. 为什么 VFS 复杂

VFS 的复杂度主要来自三个方向：

### 8.1 文件系统语义本身复杂

即使底层只有对象存储和元数据存储，VFS 仍要回答：

- 打开文件后谁能看到最新大小
- 写入还未提交时读应该看到什么
- truncate 和并发写入如何避免交错破坏
- rename/unlink 与已打开句柄如何共存

### 8.2 数据与元数据分离

BrewFS 的数据与元数据走不同通道：

- 数据写入 `chunk` / `BlockStore`
- 元数据走 `MetaLayer`

VFS 必须把这两条链重新编排成“像一个文件系统”的体验。

### 8.3 需要本地运行态

如果每次读写都完全依赖远端后端，性能和语义都难以接受，因此 VFS 必须维护：

- inode 本地 size
- handle attr 缓存
- reader / writer 生命周期
- 最近修改痕迹
- 后台 flush 状态

## 9. 阅读建议

如果你第一次读 VFS 代码，建议：

1. 先看 `VFS / VfsCore / VfsState` 的对象关系
2. 再看 `inode.rs` 和 `handles.rs`
3. 然后分别阅读 `io/reader.rs` 与 `io/writer.rs`
4. 最后再看 `fs/mod.rs` 中的命名空间方法和生命周期方法

这样更容易把 VFS 看成“由若干对象共同组成的系统”，而不是一个超长的 `fs/mod.rs` 文件。
