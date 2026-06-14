# VFS 模块文档索引

本文档集用于说明 `src/vfs/` 的职责边界、核心抽象以及关键读写路径。

`vfs` 是 BrewFS 的中间层，位于 FUSE/SDK 与 `meta`、`chunk` 之间，负责把面向文件系统的操作翻译为：

- 命名空间和属性操作
- 文件句柄与目录句柄生命周期管理
- 读缓存与写缓冲协调
- 后台 flush / compaction / gc 等任务接入
- 一组接近 POSIX 语义的高层接口

## 文档范围

本文档集聚焦 `src/vfs/` 模块本身，不单独覆盖以下内容：

- `src/sdk_fs.rs` 和外部 SDK 封装
- `src/meta/` 的后端实现细节
- `src/chunk/` 的块存储和 compaction 细节
- `src/fuse/` 的协议适配细节

这些主题分别由现有专题文档或后续文档说明。

## 目录结构

当前规划拆分为 7 篇文档：

1. `vfs-overview.md`
   - 说明 VFS 在系统中的位置、核心类型、整体数据流和一致性边界
2. `inode.md`
   - 说明本地 `Inode` 缓存对象、size 广播和 committed-bytes 计数
3. `handles.md`
   - 说明文件/目录句柄、句柄注册表、HandleGate 与 RAII 生命周期
4. `reader.md`
   - 说明读路径、Reader 缓存、预读和与 writer 的交互
5. `writer.md`
   - 说明写路径、slice 状态机、flush、auto flush 与 back-pressure
6. `namespace.md`
   - 说明 lookup/create/unlink/rename/truncate/setattr 等命名空间操作
7. `background.md`
   - 说明后台 compaction、gc、writer 后台 flush 以及相关调度逻辑

## 推荐阅读顺序

建议按以下顺序阅读：

1. `vfs-overview.md`
2. `inode.md`
3. `handles.md`
4. `reader.md`
5. `writer.md`
6. `namespace.md`
7. `background.md`

这样的顺序先建立整体模型，再进入基础抽象，最后展开 I/O 主路径和后台逻辑。

## 源码映射

VFS 相关源码主要分布在以下位置：

```text
src/vfs/
├── mod.rs         # 模块入口、chunk_id 辅助函数
├── inode.rs       # Inode 本地状态
├── handles.rs     # FileHandle / DirHandle / HandleGate
├── error.rs       # VfsError 与 PathHint
├── config.rs      # VFS / reader / writer 配置
├── backend.rs     # 对 chunk + meta 的统一后端封装
├── meta_ops.rs    # 元数据访问辅助逻辑
├── fs/
│   └── mod.rs     # VFS 主实现、命名空间操作、句柄生命周期
└── io/
    ├── reader.rs  # DataReader / FileReader
    └── writer.rs  # DataWriter / FileWriter
```

## 源码跳转索引

下面这组索引按文件列出最值得先看的类型和函数，适合把文档阅读和源码阅读配合起来。

### `src/vfs/mod.rs`

- [chunk_id_for](file:///mnt/rk8s/src/vfs/mod.rs#L38-L58): 根据 `ino + chunk_index` 计算全局 chunk id
- [extract_ino_and_chunk_index](file:///mnt/rk8s/src/vfs/mod.rs#L61-L80): 从 chunk id 反解 inode 与 chunk 下标

### `src/vfs/inode.rs`

- [Inode](file:///mnt/rk8s/src/vfs/inode.rs#L8-L58): VFS 本地 inode 运行态，维护 size 广播与 `committed_bytes`

### `src/vfs/handles.rs`

- [HandleGate](file:///mnt/rk8s/src/vfs/handles.rs#L35-L171): 同一文件句柄上的读写门禁，带 writer 优先
- [FileHandle](file:///mnt/rk8s/src/vfs/handles.rs#L187-L347): 单个打开文件的会话状态、attr cache 和 reader/writer 绑定
- [DirHandle](file:///mnt/rk8s/src/vfs/handles.rs#L371-L463): 目录句柄、目录项缓存和预取任务生命周期

### `src/vfs/fs/mod.rs`

- [VfsState](file:///mnt/rk8s/src/vfs/fs/mod.rs#L244-L287): VFS 运行时状态，汇总 handle registry、inode 表、reader、writer 和修改跟踪
- [VfsCore](file:///mnt/rk8s/src/vfs/fs/mod.rs#L289-L320): VFS 核心依赖与稳定上下文
- [VFS](file:///mnt/rk8s/src/vfs/fs/mod.rs#L322-L418): 对外主入口对象
- [start_background_tasks](file:///mnt/rk8s/src/vfs/fs/mod.rs#L420-L456): 启动 compaction 与 gc 后台任务
- [from_components_with_background](file:///mnt/rk8s/src/vfs/fs/mod.rs#L465-L483): 组装 `core/state/background_tasks`
- [stat_ino](file:///mnt/rk8s/src/vfs/fs/mod.rs#L569-L579): 基于 meta 结果叠加本地 size 的 inode 属性查询
- [mkdir_p](file:///mnt/rk8s/src/vfs/fs/mod.rs#L685-L720): 递归建目录
- [create_file](file:///mnt/rk8s/src/vfs/fs/mod.rs#L769-L778): 创建文件的便捷路径接口
- [rename](file:///mnt/rk8s/src/vfs/fs/mod.rs#L1197-L1211): 路径版 rename 入口
- [truncate](file:///mnt/rk8s/src/vfs/fs/mod.rs#L1370-L1379): 路径版截断入口
- [fallocate_ino](file:///mnt/rk8s/src/vfs/fs/mod.rs#L1437-L1468): 最小可用的 fallocate 语义
- [set_attr](file:///mnt/rk8s/src/vfs/fs/mod.rs#L1470-L1588): 属性更新与 size 变更协调逻辑
- [read](file:///mnt/rk8s/src/vfs/fs/mod.rs#L1595-L1622): 按文件句柄读取
- [write](file:///mnt/rk8s/src/vfs/fs/mod.rs#L1624-L1691): 按文件句柄写入与 writeback 路由
- [open](file:///mnt/rk8s/src/vfs/fs/mod.rs#L1869-L1926): 打开文件并绑定 reader/writer
- [close](file:///mnt/rk8s/src/vfs/fs/mod.rs#L1928-L2010): 关闭文件句柄并处理 flush/release
- [fsync](file:///mnt/rk8s/src/vfs/fs/mod.rs#L2012-L2028): 显式同步入口
- [opendir](file:///mnt/rk8s/src/vfs/fs/mod.rs#L2030-L2036): 打开目录句柄
- [readdir](file:///mnt/rk8s/src/vfs/fs/mod.rs#L2063-L2068): 分页读取目录项

### `src/vfs/io/reader.rs`

- [DataReader](file:///mnt/rk8s/src/vfs/io/reader.rs#L33-L404): reader 注册表与缓存失效入口
- [FileReader](file:///mnt/rk8s/src/vfs/io/reader.rs#L405-L1122): 单文件读引擎
- [check_session](file:///mnt/rk8s/src/vfs/io/reader.rs#L519-L579): 双 Session 读模式判断与 readahead 决策
- [read_from_slice](file:///mnt/rk8s/src/vfs/io/reader.rs#L802-L860): 等待 slice 就绪并把数据拷贝到用户缓冲区
- [prepare_slices](file:///mnt/rk8s/src/vfs/io/reader.rs#L862-L960): 为请求区间准备 slice 并启动后台 fetch

### `src/vfs/io/writer.rs`

- [FileWriter](file:///mnt/rk8s/src/vfs/io/writer.rs#L671-L1349): 单文件写入引擎
- [write_at](file:///mnt/rk8s/src/vfs/io/writer.rs#L728-L837): 前台写入入口，负责 back-pressure、chunk 切分和动作下发
- [flush](file:///mnt/rk8s/src/vfs/io/writer.rs#L839-L960): 冻结当前写入快照并等待其推进到稳定状态
- [spawn_flush_slice](file:///mnt/rk8s/src/vfs/io/writer.rs#L961-L1070): 单 slice 上传后台任务
- [commit_chunk](file:///mnt/rk8s/src/vfs/io/writer.rs#L1072-L1268): chunk 级后台提交循环
- [auto_flush](file:///mnt/rk8s/src/vfs/io/writer.rs#L1270-L1349): 文件级后台自动冻结与推进逻辑
- [DataWriter](file:///mnt/rk8s/src/vfs/io/writer.rs#L1351-L1479): writer 注册表与全局后台 flush 协调
- [ensure_file](file:///mnt/rk8s/src/vfs/io/writer.rs#L1378-L1391): 为 inode 获取或创建 `FileWriter`
- [start_flush_background](file:///mnt/rk8s/src/vfs/io/writer.rs#L1393-L1407): 周期性遍历所有 writer 执行 flush
- [flush_if_exists](file:///mnt/rk8s/src/vfs/io/writer.rs#L1409-L1416): 机会性 flush 入口
- [overlay_dirty_if_exists](file:///mnt/rk8s/src/vfs/io/writer.rs#L1418-L1431): 将本地脏写叠加到读缓冲
- [flush_required](file:///mnt/rk8s/src/vfs/io/writer.rs#L1433-L1443): 强语义 flush 入口
- [flush_once](file:///mnt/rk8s/src/vfs/io/writer.rs#L1465-L1478): 一轮全局 writer 扫描与推进

## 编写原则

VFS 文档按以下原则组织：

- 先讲抽象，再讲实现细节
- 尽量围绕“对象职责 + 状态变化 + 关键调用链”展开
- 明确哪些语义由 VFS 保证，哪些依赖 `meta` / `chunk` / `fuse`
- 对复杂并发点给出“为什么这么设计”，而不仅是“代码做了什么”

## 当前状态

当前已完成：

- `vfs-overview.md`
- `inode.md`
- `handles.md`
- `reader.md`
- `writer.md`
- `namespace.md`
- `background.md`
