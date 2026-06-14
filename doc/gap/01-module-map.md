# 模块映射与架构边界

## 源码规模对照

BrewFS Rust 侧当前核心目录：

| BrewFS 模块 | 文件数 | 主要职责 |
|---|---:|---|
| `src/meta` | 38 | 元数据 trait、client cache、session、Database/Redis/Etcd store |
| `src/vfs` | 21 | inode/handle、读写编排、POSIX 操作、缓存接入 |
| `src/chunk` | 15 | layout、slice、block store、reader/writer、compact/gc |
| `src/cadapter` | 5 | LocalFS/S3 object backend wrapper |
| `src/fuse` | 3 | asyncfuse 适配与挂载 |
| `src/control` | 7 | mount 实例注册、Unix socket 控制面、job |
| `src/daemon` | 3 | daemon/worker 雏形 |

JuiceFS Go 侧当前核心目录：

| JuiceFS 模块 | 文件数 | 主要职责 |
|---|---:|---|
| `pkg/meta` | 50 | 元数据完整生命周期、多后端、quota/ACL/xattr/dump/load/fsck |
| `pkg/object` | 65 | 大量对象存储后端、multipart、restore、加密包装 |
| `pkg/chunk` | 21 | page/chunk cache、writeback、prefetch、rate limit |
| `pkg/vfs` | 19 | 完整 VFS 语义、内部控制节点、backup/compact/fill |
| `pkg/fuse` | 9 | 平台相关 FUSE glue |
| `pkg/sync` | 9 | 数据同步工具 |
| `pkg/acl` | 3 | ACL 缓存与规则 |
| `pkg/compress` | 2 | 压缩抽象 |
| `pkg/gateway` | 2 | S3 gateway |

## 模块一一映射

| 能力域 | BrewFS | JuiceFS | 差异 |
|---|---|---|---|
| CLI 入口 | `src/main.rs`、`src/config.rs` | `cmd/*.go`、`cmd/main.go` | BrewFS 当前命令面很窄；JuiceFS 的命令即产品控制面 |
| 元数据抽象 | `src/meta/store.rs` | `pkg/meta/interface.go` | 两者都很大；BrewFS 许多方法仍是默认未实现 |
| 元数据实现 | `src/meta/stores/{database,redis,etcd}` | `pkg/meta/{redis,sql,tkv,...}` | JuiceFS 后端更广，且围绕同一语义长期打磨 |
| 元数据客户端缓存 | `src/meta/client/*` | `pkg/meta/base.go`、openfile/cache 相关 | BrewFS 有 inode/path cache 与 Etcd watch；JuiceFS 有 open-cache、session、quota、ACL 等完整整合 |
| 数据布局 | `src/chunk/layout.rs`、`slice.rs` | `pkg/meta/interface.go` constants、`pkg/chunk` | 默认 64MiB chunk 类似；对象 key 和 slice 生命周期细节不同 |
| 对象存储抽象 | `src/cadapter/client.rs`、`src/chunk/store.rs` | `pkg/object/interface.go` | BrewFS 抽象简洁；JuiceFS object interface 包含 List/Head/Multipart/Restore 等生产能力 |
| 读写路径 | `src/vfs/io/{reader,writer}.rs` | `pkg/vfs/{reader,writer}.go`、`pkg/chunk/cached_store.go` | BrewFS 状态机清晰但仍需语义验证；JuiceFS 缓存/限速/回写成熟 |
| FUSE | `src/fuse/*` 使用 `asyncfuse` | `pkg/fuse/*` | BrewFS FUSE 层薄；JuiceFS mount option 与平台行为更多 |
| 后台维护 | `src/chunk/compact/*`、`src/control/*` | `cmd/gc.go`、`cmd/compact.go`、`pkg/vfs/compact.go`、`pkg/meta` cleanup | BrewFS 已有 compact/gc 内核，但 CLI 与可观测性不够 |
| K8s | `operator/brewfs-operator` | JuiceFS CSI 独立生态 | BrewFS operator 是雏形；CSI 能力缺失 |

## 架构边界差异

### BrewFS 更偏“库内强类型分层”

BrewFS 的抽象非常 Rust 化：

- `BlockStore` 只关心 block range 读写与删除。
- `ObjectBackend` 只关心 put/get/range/delete。
- `MetaLayer` 和 `MetaStore` 把缓存层与后端层分开。
- `VFS` 显式持有 `Backend<BlockStore, MetaLayer>` 并编排 reader/writer。

这个设计利于单元测试和模块替换，但也带来一个风险：很多 JuiceFS 中集中在 `baseMeta` 或 `VFS` 的语义，现在散落在多个 trait 与状态机中，必须靠契约文档和集成测试保证一致。

### JuiceFS 更偏“产品闭环优先”

JuiceFS 的 `meta.Meta` 接口非常庞大，包含：

- format/load/session/lock/statfs
- lookup/getattr/setattr/open/close
- read/write/truncate/fallocate/copy_file_range
- xattr/ACL/flock/plock
- compact/list slices/remove/summary/clone/check
- dump/load/quota/changelog/token

它的好处是：每个元数据后端最终要向同一个产品语义收敛。缺点是接口大、实现复杂。BrewFS 已经开始走类似路线，但还没有完成“所有后端都承诺同一语义”的阶段。

## BrewFS 当前模块亮点

### 1. `MetaClient` 缓存结构明确

`src/meta/client/mod.rs` 有 inode cache、path cache、path trie、inode-to-path reverse index，并对 Etcd backend 预留 watch invalidation。这比单纯 TTL cache 更接近分布式一致性所需结构。

### 2. `DataWriter` 状态机可审计

`src/vfs/io/writer.rs` 把写入生命周期写得很明确：

```text
Writable -> Readonly -> Uploaded/Failed -> Committed
```

这是后续做 fsync 语义、故障注入和 crash recovery 的基础。

### 3. compact/gc 已不只是占位

`src/chunk/compact` 已包含 light/heavy compaction、delayed slice、uncommitted slice cleanup、chunk 全局锁与 TTL。这是比许多 MVP 更进一步的地方。

## 结构性缺口

### 1. 缺少统一 volume format

JuiceFS 的 `meta.Format` 记录 name、UUID、storage、bucket、block size、compression、hash prefix、capacity、inodes、encryption、trash days、quota、ACL、client version 等。BrewFS 当前主要通过 mount config 拼出运行参数，缺少“格式化后的卷不变量”。

建议新增：

- `brewfs format <meta-url> <name>`
- `volume_format` 元数据记录
- layout/object/backend/feature/client-version 校验
- 不兼容字段禁止 mount 或要求显式迁移

### 2. 模块边界缺少能力发现

`MetaStore` 很多方法是 optional，但上层没有统一 capability 查询。建议引入：

```rust
struct StoreCapabilities {
    xattr: bool,
    acl: bool,
    quota: bool,
    dump_load: bool,
    compact: bool,
    global_lock: bool,
    watch_invalidation: bool,
}
```

并让 mount 阶段根据目标功能做 fail-fast。

### 3. 后台任务与控制面未完全产品化

BrewFS control plane 目前支持 `Ping/GetInfo/RunGc/GetJob`。建议将 compact、cache warmup/evict、stats、debug dump、session list、fsck 都纳入同一异步 job 框架。

