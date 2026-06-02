# JuiceFS 架构总览

> 基于 vendored juicefs 源码 (`juicefs/pkg/`) 分析, `54439a2`, `2026-05-21`

## I/O 栈全景

```
┌─ Layer 0: User Application ─────────────────────────────────┐
│  fio / dirstress / metaperf → read()/write()/stat()         │
├─ Layer 1: Linux Kernel VFS → FUSE (/dev/fuse) ──────────────┤
│  Kernel page cache → FUSE protocol → userspace daemon       │
├─ Layer 2: JuiceFS VFS (vfs/vfs.go) ─────────────────────────┤
│  VFS.Read/Write/Flush + handle.Wlock/Rlock + Internal Nodes │
├─ Layer 3a: Data Layer (vfs/reader.go, writer.go) ───────────┤
│  fileReader (预读) / fileWriter (切片缓冲) / commitThread    │
├─ Layer 3b: Metadata Layer (meta/) ──────────────────────────┤
│  Redis WATCH/EXEC txn + CSC + OpenFile Cache                │
├─ Layer 4a: Chunk Store (chunk/cached_store.go) ─────────────┤
│  wSlice/rSlice → block upload/download → compress → retry   │
├─ Layer 4b: Disk Cache (chunk/disk_cache.go) ────────────────┤
│  multi-dir consistent hashing → pending→flush pipeline      │
├─ Layer 5a: Object Storage ──────────────────────────────────┤
│  S3 / GCS / OSS → blob GET/PUT                              │
└─ Layer 5b: Redis Cluster ───────────────────────────────────┘
│  Standalone / Sentinel / Cluster → 28 key patterns          │
```

## 核心设计原则

### Data-First, Metadata-Later
数据先上传到 S3，确认成功后，再通过 Redis 事务提交切片元数据。崩溃场景下最多留下孤儿 block，由 sliceRefs GC 清理。

### Append-Only Slice Log
每个 64MB chunk 是一个 Redis List，新切片永远 RPUSH 到末尾。从不修改已有元素。压缩通过 LTRIM+LPUSH 原子替换整个列表。

### 5-Level Cache Hierarchy
Redis CSC → OpenFile → Memory Pages → Disk Cache Files → Object Store。每层独立管理，上层命中率决定性能天花板。

---

## Redis 元数据引擎 (meta/redis.go, 6249 行)

### 连接管理

三种模式自动检测 (`newRedisMeta`, line 110):

| 模式 | 检测方式 | 前缀 |
|------|---------|------|
| Standalone | 默认 `redis.NewClient` | 空 |
| Sentinel | Host 含 `,` 且第一逗号在 `:` 前 | 空 |
| Cluster | 尝试 `CLUSTER INFO` 检测 | `{db}` |

URL query 参数: `client-cache`, `client-cache-size`(12800), `client-cache-expire`(1m), `route-read`, `read-timeout`(30s), `write-timeout`(5s).

### 28 个 Key Pattern

所有 key 可带 `{DB}` 前缀用于 Cluster hash-tag:

| Key Pattern | 类型 | 内容 | getter |
|-------------|------|------|--------|
| `i{inode}` | String | Attr 二进制 (67B) | `inodeKey()` |
| `d{parent}` | Hash | `name → type+inode` (9B/条) | `entryKey()` |
| `p{inode}` | Hash | `parent → count` (硬链接) | `parentKey()` |
| `c{inode}_{indx}` | List | slice 二进制 (24B/条) | `chunkKey()` |
| `s{inode}` | String | symlink target | `symKey()` |
| `x{inode}` | Hash | `name → value` (xattr) | `xattrKey()` |
| `lockf{inode}` | Hash | `{sid}_{owner} → ltype` | `flockKey()` |
| `lockp{inode}` | Hash | `{sid}_{owner} → Plock` | `plockKey()` |
| `allSessions` | ZSet | `sid → heartbeat_expire` | `allSessions()` |
| `sessionInfos` | Hash | `sid → JSON info` | `sessionInfos()` |
| `session{sid}` | Set | sustained inodes | `sustained()` |
| `locked{sid}` | Set | lock inodes | `lockedKey()` |
| `setting` | String | JSON `Format` | `setting()` |
| `sliceRef` | Hash | `k{id}_{size} → refcount` | `sliceRefs()` |
| `delfiles` | ZSet | `{inode}:{length} → expire` | `delfiles()` |
| `detachedNodes` | ZSet | `inode → seconds` | `detachedNodes()` |
| `delSlices` | Hash | `{id}_{ts} → bytes` | `delSlices()` |
| `dirDataLength` | Hash | `inode → length` | `dirDataLengthKey()` |
| `dirUsedSpace` | Hash | `inode → usedSpace` | `dirUsedSpaceKey()` |
| `dirUsedInodes` | Hash | `inode → usedInodes` | `dirUsedInodesKey()` |
| `dirQuota` | Hash | `inode → quota` (16B) | `dirQuotaKey()` |
| `totalInodes` | String | counter | `totalInodesKey()` |
| `usedSpace` | String | counter | `usedSpaceKey()` |
| `nextInode/nextChunk/nextSession` | String | counter | `counterKey()` |
| `txnLog` | List | changelog entries | `txnLogKey()` |
| `txnLastLog` | String | log version | `txnLastLog()` |
| `acl` | Hash | `acl_id → rule` | `aclKey()` |
| `krbToken` | Hash | `token_id → token` | `krbTokenKey()` |

### Binary Encoding

```go
packEntry(type uint8, inode Ino) []byte  // 9B: 1B type + 8B BE inode
packQuota(space, inodes int64) []byte    // 16B: 8B space + 8B inodes
marshalSlice(pos, id, size, off, len)    // 24B: 4+8+4+4+4
```

---

## Mount 生命周期 (cmd/mount.go:533)

```
Stage 0: prepareMp() → NewClient() → load Format
Stage 0-3: NewReloadableStorage() → object store + connectivity test
Stage <3: daemonRun() → background → launchMount() → fork
Stage 3: NewCachedStore() → NewSession() → NewVFS() → mountMain()
  [Running...]
Signals → FlushAll() → CloseSession() → Shutdown(blob)
```

### 关键默认配置

| Flag | Default | 说明 |
|------|---------|------|
| `--heartbeat` | 12s | 心跳间隔，5x 即 60s 超时 |
| `--attr-cache` | 1.0s | FUSE attr 缓存超时 |
| `--entry-cache` | 1.0s | FUSE entry 缓存超时 |
| `--open-cache` | 0s | open file 缓存 TTL |
| `--buffer-size` | 300M | 写缓冲 + 读预读缓冲总量 |
| `--cache-size` | 100G | 本地磁盘缓存容量 |
| `--max-uploads` | 20 | 并发上传数 |
| `--max-downloads` | 200 | 并发下载数 |
| `--prefetch` | 1 | 预取并发数 |
| `--block-size` | 4M (KiB) | 对象存储 block 大小 |
| `--compress` | none | 压缩算法: none/lz4/zstd |
| `--writeback` | false | 写回模式 |
| `--upload-delay` | 0s | 延迟上传 (写回模式) |
| `--cache-eviction` | 2-random | 淘汰策略: none/2-random/lru |

---

## Format 流程 (cmd/format.go:410)

**新建**:
```
validate name (3-63 chars) → createStorage() → test connectivity
→ check bucket empty → write juicefs_uuid → encrypt → m.Init(format, force)
```

**更新**:
```
load existing format → apply flag overrides → decrypt keys → m.Init(format, force)
```

格式信息持久化在 Redis `setting` key 中 (JSON `Format` struct)，包含 storage class, compression, block size, encryption, trash days 等配置。
