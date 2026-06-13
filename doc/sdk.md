# SDK 与 API

BrewFS 提供两种使用方式：FUSE 挂载（POSIX 文件系统）和 SDK（编程接口）。SDK 允许在不挂载 FUSE 的情况下直接通过路径操作文件。

## 公开 API

`src/lib.rs` 的 `pub use` 导出了以下公共 API。

### 核心类型

```rust
// 数据布局
pub use brewfs::ChunkLayout;

// 对象存储后端
pub use brewfs::cadapter::localfs::LocalFsBackend;
pub use brewfs::cadapter::s3::{S3Backend, S3Config};
pub use brewfs::cadapter::client::{ObjectBackend, ObjectClient};

// Block 存储
pub use brewfs::chunk::store::{BlockKey, BlockStore, InMemoryBlockStore, ObjectBlockStore};

// Compaction / GC
pub use brewfs::chunk::{BlockGcConfig, BlockStoreGC};
pub use brewfs::chunk::{CompactResult, Compactor, CompactorError};

// 元数据
pub use brewfs::meta::MetaStore;
pub use brewfs::meta::MetaHandle;
pub use brewfs::meta::client::MetaClient;
pub use brewfs::meta::factory::MetaStoreFactory;
pub use brewfs::meta::stores::{DatabaseMetaStore, EtcdMetaStore, RedisMetaStore};
pub use brewfs::meta::config::{
    CacheConfig, ClientOptions, CompactConfig, Config, DatabaseConfig, DatabaseType,
};
pub use brewfs::meta::store::{
    DirEntry as VfsDirEntry, FileAttr as VfsFileAttr, FileType as VfsFileType,
    SetAttrFlags, SetAttrRequest, StatFsSnapshot,
};
pub use brewfs::meta::file_lock::{
    FileLockInfo, FileLockQuery, FileLockRange, FileLockType,
};
pub use brewfs::meta::{create_meta_store_from_url, create_redis_meta_store_from_url};

// VFS
pub use brewfs::vfs::fs::{RenameFlags, VFS};

// SDK 客户端
pub use brewfs::vfs::sdk::{LocalClient, VfsClient};
```

### SDK 类型

```rust
pub use brewfs::sdk_fs::{
    AccessMode, Client, ClientBackend, DirEntry as SdkDirEntry,
    File, FileType as SdkFileType, Metadata, OpenOptions, ReadDir,
};
```

## VfsClient（推荐使用方式）

`VfsClient<S, M>` 是一个泛型结构体，参数化为 `BlockStore` 和 `MetaClient`。使用时通过 `LocalClient` 便捷构造器创建。

### LocalClient

```rust
use brewfs::{ChunkLayout, LocalClient};

#[tokio::main]
async fn main() {
    let layout = ChunkLayout::default();       // 64 MiB chunk / 4 MiB block
    let root = "/tmp/brewfs-objroot";
    let mut cli = LocalClient::new_local(root, layout).await.unwrap();

    // 创建目录和文件
    cli.mkdir_p("/a/b").await.unwrap();
    cli.create_file("/a/b/hello.txt", false).await.unwrap();

    // 跨块写入
    let half = (layout.block_size / 2) as usize;
    let len = layout.block_size as usize + half;
    let mut data = vec![0u8; len];
    for i in 0..len { data[i] = (i % 251) as u8; }
    cli.write_at("/a/b/hello.txt", half as u64, &data).await.unwrap();

    // 读取并校验
    let out = cli.read_at("/a/b/hello.txt", half as u64, len).await.unwrap();
    assert_eq!(out, data);
}
```

### 自定义后端

```rust
use brewfs::{
    LocalFsBackend, ObjectClient, ObjectBlockStore,
    DatabaseMetaStore, MetaClient, VfsClient, ChunkLayout,
};

let layout = ChunkLayout::default();

// 对象后端
let backend = LocalFsBackend::new("/tmp/data");
let client = ObjectClient::new(backend);
let store = ObjectBlockStore::new_with_configs(client, cache_cfg, store_cfg).await?;

// 元数据后端
let meta_store = DatabaseMetaStore::from_config(db_config).await?;
let meta_client = MetaClient::new(meta_store, ...);

// VFS 客户端
let vfs = VfsClient::new(layout, store, meta_client)?;
```

## API 速览

所有路径 API 返回 `io::Result`，错误映射到标准 errno（ENOENT、EEXIST、ENOTDIR、EISDIR、ENOTEMPTY 等）。

| 方法 | 说明 |
|---|---|
| `mkdir_p(path)` | 递归创建目录。中间路径若是文件 → `ENOTDIR` |
| `create_file(path, create_new)` | 创建文件。`create_new=true` 时已存在 → `EEXIST` |
| `write_at(path, offset, &[u8])` | 按文件偏移写入，跨 Chunk/Block 自动拆分，返回写入字节数 |
| `read_at(path, offset, len)` | 按偏移读取。未写入区域 → 零填充（稀疏文件语义） |
| `readdir(path)` | 列目录，不含 `.` 和 `..` |
| `stat(path)` | 获取文件属性（ino、size、kind、mode、uid、gid、时间戳） |
| `unlink(path)` | 删除文件。目录 → `EISDIR` |
| `rmdir(path)` | 删除空目录。根目录不可删除。非空 → `ENOTEMPTY` |
| `rename(old, new)` | 重命名文件。目标父目录不存在时自动创建 |
| `rename_with_flags(old, new, flags)` | 带标志的重命名（`RENAME_NOREPLACE` / `RENAME_EXCHANGE`） |
| `truncate(path, size)` | 截断文件。收缩不立即清理块数据（由 GC 回收） |

### RenameFlags

```rust
pub struct RenameFlags {
    pub noreplace: bool,    // RENAME_NOREPLACE: 目标存在时失败
    pub exchange: bool,     // RENAME_EXCHANGE: 原子交换两个文件
}
```

### 类型定义

```rust
// 目录项
pub struct VfsDirEntry {
    pub name: String,
    pub ino: i64,
    pub kind: VfsFileType,   // File / Dir / Symlink
}

// 文件属性
pub struct VfsFileAttr {
    pub ino: i64,
    pub size: u64,
    pub blocks: u64,     // 分配的 512-byte 块数
    pub kind: VfsFileType,
    pub mode: u32,       // 权限位 (0o777)
    pub uid: u32,
    pub gid: u32,
    pub atime: i64,      // Unix 时间戳
    pub mtime: i64,
    pub ctime: i64,
    pub nlink: u32,      // 硬链接计数
}
```

## FS 层（内部使用）

`src/fs.rs` 实现了一个基础的基于路径的 FileSystem（非 pub），提供了 `mkdir`、`mkdir_all`、`create`、`read`、`write`、`readdir`、`stat`、`unlink`、`rmdir`、`rename`、`truncate` 等操作。它使用单个互斥锁保护命名空间，避免多锁死锁。VFS 层（`src/vfs/fs.rs`）在此基础上实现了完整的 POSIX 语义和 FUSE 集成。

## Daemon

`src/daemon/` 提供了 daemon 进程管理：

- `supervisor.rs` — 进程监控和生命周期管理
- `worker.rs` — 后台 worker 任务管理

## VFS 构造

VFS 的完整构造链在 `main.rs:mount_with_store` 函数中：

1. 创建 `BlockStore`（`ObjectBlockStore` 或 `InMemoryBlockStore`）
2. 创建 `MetaStore`（根据后端类型）
3. 创建 `MetaClient`（包裹 MetaStore，带缓存层）
4. 调用 `MetaClient::initialize()` 初始化根 inode
5. 调用 `MetaClient::start_control_plane()` 启动控制面
6. 构造 `VFS::with_meta_layer_with_cache_config(layout, store, meta_client, compact_config, cache_config)`
7. 通过 FUSE 挂载（`mount_vfs_privileged` 或 `mount_vfs_unprivileged`）

```rust
let fs = VFS::with_meta_layer_with_cache_config(
    layout,
    store,
    meta_client.clone(),
    compact_config,
    cache_config,
)?;
```

内部创建的组件：
- `MetaLayer`（包装 MetaClient，提供高层语义）
- `FileWriter`（写路径，Slice 状态机）
- `FileReader`（读路径，预读管理）
- `MemoryBudget`（全局内存预算）
- `FileHandles`（句柄管理）
- `Stats`（.stats 虚拟文件）

## Posix 层

`src/posix.rs` 提供 POSIX 标准的辅助实现（如路径解析、特殊文件名处理等）。

## 工具模块

`src/utils/` 包含通用工具：

- `intervals.rs` — 区间集合，支持 cut/add/merge 操作，用于读路径的 Slice 覆盖计算
- `num.rs` — 数值转换辅助
- `usage.rs` — 磁盘使用量统计
- `zero.rs` — 零填充优化（检测全零缓冲区，跳过写入）
