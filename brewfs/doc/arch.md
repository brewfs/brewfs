# BrewFS 架构概述

BrewFS 是一个用 Rust 实现的分布式文件系统，设计思路受 JuiceFS 启发。计算与存储分离，元数据与数据分流到不同后端，对外通过 FUSE 挂载提供 POSIX 兼容的文件系统接口。

## 分层结构

整个系统从上到下分为 6 层：

```
┌──────────────────────────────────────────┐
│  CLI / Daemon  (main.rs)                 │  ← 入口、配置解析、信号处理
├──────────────────────────────────────────┤
│  FUSE 适配层  (fuse/)                     │  ← rfuse3, io_uring, 请求分发
├──────────────────────────────────────────┤
│  VFS 层  (vfs/)                          │  ← POSIX 语义, 句柄管理, 缓存
├──────────────────────────────────────────┤
│  MetaClient / MetaLayer  (meta/)         │  ← 元数据缓存, 会话, 事务封装
│  MetaStore  (meta/store.rs trait)        │  ← 元数据存储抽象
│  ├── DatabaseMetaStore (SQLite/Postgres) │
│  ├── EtcdMetaStore                       │
│  └── RedisMetaStore                      │
├──────────────────────────────────────────┤
│  数据层  (chunk/)                         │  ← Chunk/Block/Slice 管理
│  ├── Writer (上传, 提交)                  │
│  ├── Reader (定位, 读取, 拼装)            │
│  ├── BlockStore (对象存储适配)             │
│  └── Compaction/GC                       │
├──────────────────────────────────────────┤
│  对象后端  (cadapter/)                    │
│  ├── LocalFsBackend (本地磁盘)            │
│  └── S3Backend (S3 兼容存储)              │
└──────────────────────────────────────────┘
```

### 1. CLI / Daemon 层

`main.rs` 是整个系统的入口，负责：

- 解析 CLI 参数（支持 YAML 配置文件 + 命令行覆盖）
- 初始化 tracing 日志系统（支持 log 文件分离、chrome trace、flamegraph、tokio-console）
- 创建 MetaStore 和 BlockStore 实例
- 构造 VFS 实例并通过 FUSE 挂载
- 信号处理（SIGINT 卸载）

提供三个子命令：`mount`、`gc`、`info`。其中 `gc` 和 `info` 通过 Unix domain socket 与已运行的 daemon 通信（control plane）。

源码入口：`src/main.rs`，库入口：`src/lib.rs`

### 2. FUSE 适配层

`src/fuse/` 基于 `rfuse3` 实现 FUSE 协议，关键文件：

- `mod.rs`：实现 `fuser::Filesystem` trait，将 FUSE 请求翻译为 VFS 调用
- `mount.rs`：挂载逻辑，支持特权模式（直接 `/dev/fuse`）和非特权模式（`fusermount3`）
- `adapter.rs`：FUSE 与 VFS 之间的类型适配

FUSE 层通过 `OpTimer` RAII 结构记录每个操作的计数、字节数和延迟，写入 `.stats` 虚拟文件供 `brewfs-stats` 读取。

### 3. VFS 层

`src/vfs/` 是核心逻辑层，实现 POSIX 语义：

- `fs/mod.rs`：`VFS` 结构体，是所有文件系统操作的入口（open/read/write/mkdir/unlink/rename/truncate 等）
- `handles.rs`：文件和目录句柄管理，每个打开的文件持有 `FileHandle`
- `io/reader.rs`：`FileReader`，读取路径，包含预读（readahead）逻辑
- `io/writer.rs`：`FileWriter`，写入路径，管理 Slice 状态机和异步上传
- `inode.rs`：inode 号分配与管理
- `cache/`：VFS 层的缓存子系统（page cache、read cache、write-back cache、prefetch）
- `meta_ops.rs`：元数据操作的 VFS 封装
- `sdk.rs`：SDK 客户端封装（`VfsClient`）
- `stats.rs`：`.stats` 虚拟文件的实现
- `memory.rs`：全局内存预算管理

### 4. 元数据层

`src/meta/` 管理所有文件系统命名空间和布局元数据：

- `store.rs`：`MetaStore` trait，定义了 70+ 个元数据操作接口（stat、lookup、mkdir、create、unlink、rename、link、symlink、xattr、quota、lock 等）
- `client/mod.rs`：`MetaClient`，在 MetaStore 之上提供缓存层（InodeCache、PathTrie、path_cache）
- `layer.rs`：`MetaLayer`，组合 MetaClient，为 VFS 提供更高层语义
- `stores/database/mod.rs`：`DatabaseMetaStore`，基于 SeaORM，支持 SQLite 和 PostgreSQL
- `stores/etcd/mod.rs`：`EtcdMetaStore`，基于 etcd-client，支持分布式 KV、watch、事务
- `stores/redis/mod.rs`：`RedisMetaStore`，基于 redis-rs，支持 Version + Lua CAS 并发控制
- `entities/`：SeaORM 实体定义（`file_meta`、`content_meta`、`access_meta`、`slice_meta` 等）
- `factory.rs`：`MetaStoreFactory`，工厂模式创建不同后端
- `migrations.rs`：数据库 schema 迁移

### 5. 数据层（Chunk 子系统）

`src/chunk/` 实现了 JuiceFS 风格的 Chunk → Block 两级数据布局：

- `layout.rs`：`ChunkLayout`，定义 chunk_size（默认 64MiB）和 block_size（默认 4MiB），提供全套偏移换算
- `span.rs`：泛型 `Span<T>` 结构，用编译期 marker（ChunkTag/BlockTag/PageTag）区分层级
- `slice.rs`：`SliceDesc` — 一次写操作在 Chunk 内产生的连续区间；`block_span_iter_slice` — slice 到 block 的映射
- `writer.rs`：`DataUploader`，并发上传 slice 的各 block 数据
- `reader.rs`：`DataFetcher`，按 SliceDesc 加载 block 并拼装
- `store.rs`：`BlockStore` trait + `ObjectBlockStore` 实现，封装对象读写
- `cache.rs`：`ChunksCache`，双层缓存（热内存 + 冷磁盘）
- `compact/`：压缩与 GC（Compactor、CompactionWorker、BlockStoreGC）

### 6. 对象后端

`src/cadapter/` 抽象对象存储访问：

- `client.rs`：`ObjectBackend` trait + `ObjectClient<B>` 泛型封装
- `localfs.rs`：`LocalFsBackend`，以本地目录模拟对象存储
- `s3.rs`：`S3Backend`，接入 S3 兼容存储，支持 multipart upload、path-style、checksum 控制

## 数据流转

### 写路径

```
FUSE write(ino, offset, data)
  → VFS::write()
    → FileWriter::write_at(offset, data)
      → 按 Chunk 边界切分为 ChunkSpan 列表
      → 每个 Chunk：追加到 SliceState（Writable 状态）
      → auto_flush 定时器触发 spawn_flush_slice
        → SliceState: Writable → Readonly → Uploading
        → DataUploader::write_at_vectored()
          → block_span_iter_slice 拆分为 BlockSpan 列表
          → 并发：BlockStore::write_fresh_vectored(key=(slice_id, block_index), data)
        → SliceState: Uploaded
        → commit_chunk()
          → MetaLayer::append_slice(chunk_id, SliceDesc)
        → SliceState: Committed（对读可见）
```

### 读路径

```
FUSE read(ino, offset, len)
  → VFS::read()
    → FileReader::read_at(offset, len)
      → 按 Chunk 边界切分
      → 每个 Chunk：
        → DataFetcher::prepare_slices(chunk_id)
          → MetaLayer::get_slices(chunk_id) 加载全部 SliceDesc
        → DataFetcher::read_at(offset, len)
          → Intervals 反向扫描（最新 slice 优先）构建 need_read 列表
          → 对每条 need_read：
            → block_span_iter_slice 枚举 BlockSpan 列表
            → 并发：BlockStore::read_range(key, offset, buf)
          → 空洞区间填 0
```

## 模块间依赖

```
fuse ──→ vfs ──→ meta/layer ──→ meta/client ──→ meta/store (trait)
                │                                    ├── database
                │                                    ├── etcd
                │                                    └── redis
                │
                ├──→ chunk/writer ──→ chunk/store ──→ cadapter
                │                                ├── localfs
                ├──→ chunk/reader ──→ chunk/store ─┘
                │                   └── chunk/cache
                │
                └──→ chunk/compact ──→ chunk/store
                         └── chunk/compact/gc
```

## 进程模型

BrewFS 以单进程 daemon 方式运行。一个进程内包含：

- FUSE dispatch loop（可选 worker pool 模式）
- MetaClient（含 cache、session heartbeat）
- VFS（含 reader/writer cache）
- 后台任务线程（compaction worker、GC worker）
- Control plane server（Unix domain socket，处理 gc/info 命令）

多进程部署时，每个进程独立挂载同一个文件系统，共享元数据后端和对象存储。一致性依赖 close-to-open 语义（当前为尽力而为）和全局锁（compaction 场景）。
