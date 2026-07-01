# Perf agent 4: metadata cache / Redis path explorer

日期：2026-06-11
分支：`codex/perf-tune-meta`
基线：`a28239a` (`codex/writeback-backpressure-drain`)

本文对比 BrewFS 当前 metadata cache 热路径与 JuiceFS 的 lookup/open/readlink/readdir 机制，重点找出 Redis metadata 模式下可能造成 perf 差距的具体原因，并给出下一步可实施的最小 patch 计划。

说明：BrewFS 代码引用来自本 worktree。JuiceFS 源码从 `/mnt/slayerfs/brewfs/juicefs` 只读查看，因为该 worktree 中没有 `juicefs/` 目录。

## 结论摘要

BrewFS 不是“没有 metadata cache”。它已经有 `MetaClient` inode/children cache、path cache、可选 open-file cache、Redis store 内部 `node_cache`，以及 Redis `lookup_with_attr` 融合路径。更可能的 perf 差距在于：已有 cache 的数据没有一路传到 FUSE 热路径，或者 freshness 语义不够清晰导致缓存不能放心放大。

最值得先做的三个点：

1. 先把 open-file cache 收窄为只服务 read-only open，避免写路径依赖 stale attr。
2. 增加 `MetaClient` symlink target cache，减少重复 `readlink` 后端访问。
3. 打通 Redis `readdir_plus` 到 FUSE `readdirplus` 的 attr-carrying 路径，避免 `HGETALL/MGET` 后又逐 child `stat_ino`。

## 当前 BrewFS 元数据缓存路径

### 公共缓存层

- `MetaClient` 持有：
  - `InodeCache`：缓存 inode attr、parent、目录 children 状态和 slice metadata，见 `src/meta/client/cache.rs`。
  - `path_cache`、`path_trie`、`inode_to_paths`：缓存 path 到 inode 的解析结果，见 `src/meta/client/mod.rs`。
  - 可选 `OpenFileCache`：只有配置了 `open_file_cache_ttl_ms` 和 capacity 才启用。
  - watch worker：目前只有 etcd 路径具备 watch invalidation；Redis capability 标记为 `watch_invalidation: false`。
- Redis store 自己还有一层 `node_cache`，默认 capacity `100000`、TTL `30s`，见 `src/meta/stores/redis/mod.rs`。因此 `MetaClient::stat_fresh` 绕过了 MetaClient inode cache，但在 Redis backend 下仍可能命中 Redis store 的本地 node cache。
- perf 脚本已经打开 open-file cache：`tools/perf/run_perf.sh` 中 Redis profile 设置 `open_file_cache_ttl_ms: 1000`、capacity `65536`。

### lookup / path resolve

- `MetaClient::resolve_path` 先查 `path_cache`，miss 后逐 segment 调 `cached_stat` 与 `cached_lookup`。
- `cached_lookup` 可从 `InodeCache` 的完整 children 状态返回 positive hit 和 complete-negative hit。
- plain miss 时，`cached_lookup` 会先 `store.lookup(parent, name)`，再 `store.stat(child)`，即 backend 层两步。
- `cached_lookup_with_attr` 在可能时走融合路径：
  - 如果完整 children 与 child attr 都在 `InodeCache` 中，直接内存返回。
  - 否则调用 `store.lookup_with_attr`。
- Redis `lookup_with_attr` 用 Lua 把目录 `HGET` 与 child node `GET` 合并，并把 node 写入 Redis store `node_cache`。

### open

- VFS `open_fresh_ino` 调 `meta_stat_for_open`，校验类型，刷新 VFS inode 状态，然后 `record_open`。
- `MetaClient::stat_for_open` 在 `open_file_cache_eligible(read, write, append)` 通过时使用 `OpenFileCache`，否则走 `stat_fresh`。
- 当前 eligibility 是 `(read || write) && !append`，所以 write-only 和 RDWR open 都可能命中 open-file attr cache。
- `OpenFileCache` 只缓存 `FileAttr`、refs、`last_check`；不缓存 chunk/slice metadata。Moka 使用 `time_to_live`，所以 `last_check` 也不会延长底层 cache entry 生命周期。

### readlink

- FUSE `readlink` 调 VFS `readlink_ino`。
- VFS `readlink_ino` 先 `meta_stat_required` 校验 `FileType::Symlink`，再调用 `meta_read_symlink`。
- `MetaClient::read_symlink` 直接委托 `store.read_symlink`，没有 symlink target cache。
- Redis 把 symlink target 存在 node JSON 的 `symlink_target` 中；`read_symlink` 通过 `get_node` 读取。它可能命中 Redis store `node_cache`，但仍没有 MetaClient 级 target cache，且 VFS 形态仍是 stat-before-read。

### readdir / readdirplus

- `MetaClient::readdir` 只有在目录 children complete 且每个 child attr 都还在 cache 时，才从 `InodeCache` 返回。
- miss 时，`MetaClient::readdir` 调 `store.readdir`，排序后只把 `(name, ino)` children 写入 `InodeCache`。
- 代码注释明确不在 `readdir` 内同步 prefetch attrs；`opendir` 构造 `DirHandle` 后才启动 `spawn_batch_prefetch`。
- Redis `store.readdir` 为了得到每个 child 的 `kind`，已经做了 `HGETALL` children 和 `get_nodes(child_inodes)`。但公开返回类型 `DirEntry` 只有 `name`、`ino`、`kind`，完整 attrs 在 store 边界被丢弃。
- FUSE `readdirplus` 复用普通目录 handle，然后对每个 child 调一次 `stat_ino(e.ino)`。如果 async batch prefetch 尚未完成，冷目录首个 `readdirplus` 会退化为逐 child stat。
- `MetaStore` trait 已经有 `DirEntryPlus { entry, attr }` 和 `readdir_plus` 扩展点，但当前 FUSE 路径没有使用它。

## JuiceFS 对应机制

### Redis client-side cache 与 lookup

- JuiceFS Redis 有可选 client-side cache，见 `pkg/meta/redis_csc.go`。它缓存 inode key 与 directory-entry key，使用 Redis client tracking / invalidation notification，并为 directory entry 维护本地 term。
- `doLookup` 位于 `pkg/meta/redis.go`，会检查 entry cache term，可用 Lua 合并 entry+inode 读取，并把 attr 作为 lookup 结果返回。
- FUSE 层还暴露并应用 attr cache、entry cache、dir-entry cache、negative-entry cache 等 TTL，见 `cmd/mount_unix.go` 与 `pkg/fuse/fuse.go`。

### Open-file cache

- JuiceFS `pkg/meta/openfile.go` 的 open-file cache 不只保存 attr，还保存 refs、last check、first chunk 和 per-chunk slice list。
- `OpenCheck` 可在 `OpenCache` 窗口内满足重复 open。
- `GetAttr` 会先查 open files；`Read` 会先查 `openfiles.ReadChunk`。
- 当 attr mtime 改变时，JuiceFS 会失效 chunk cache；mtime 不变时可以保留 cache。

### readlink

- JuiceFS metadata 层有 symlink target map (`m.symlinks`)。
- `ReadLink` 先查该 map，再按 atime/noatime 策略决定是否访问 backend。
- JuiceFS VFS `Readlink` 直接调用 `Meta.ReadLink`，类型检查和 target 读取收敛在 metadata 层。

### readdir / readdirplus

- JuiceFS metadata `Readdir` 支持 plus-style attr filling。
- Redis `doReaddir` 扫目录 entries；plus 请求下批量填充 attrs，而不是由 FUSE/VFS 对每个 child 单独 stat。
- JuiceFS VFS directory handler 持有 listing 状态，FUSE reply 一致应用 entry/attr TTL。

## 具体差距

1. Redis `readdir` 已经取到 child nodes，但 BrewFS 没有把 attrs 传上去。
   - Redis 为了计算 `kind` 已经 `get_nodes(child_inodes)`。
   - `DirEntry` 丢弃 `FileAttr` 后，MetaClient 无法填充 child attr cache。
   - `readdirplus` 随后又对每个 child 走 `stat_ino`。

2. async prefetch 和首个 `readdirplus` 存在竞态。
   - `opendir` 在 `readdir` 后才 spawn batch prefetch。
   - FUSE `readdirplus` 立即遍历 handle 并逐项 stat。
   - 冷目录下，用户可见的第一个 plus 调用可能已经付出了 per-child stat 成本。

3. `readlink` 没有 target cache，且 VFS 是 stat-before-read。
   - path-heavy 或 package tree 工作负载中，重复读 symlink 很常见。
   - JuiceFS 有 metadata symlink cache；BrewFS 目前主要依赖 Redis store `node_cache`。
   - 即使命中 Redis store node cache，也仍有额外的 VFS/MetaClient 调用与类型校验步骤。

4. Redis 下 `stat_fresh` 并不等价于真正 backend fresh。
   - 它绕过 MetaClient inode cache，但 Redis `get_node` 可以命中 30s `node_cache`。
   - 对 open-time freshness、多 client 场景、以及 open-file cache 评估都需要明确这个语义。

5. BrewFS open-file cache 比 JuiceFS 的安全边界更宽。
   - BrewFS 当前允许 write/RDWR open 命中 attr cache。
   - JuiceFS open-file cache 同时管理 chunk metadata，并在 mtime/chunk 变化时失效。
   - BrewFS 当前是 attr-only open cache，且 Redis 没有 watch invalidation；写 open 命中更容易掩盖真实 freshness 问题。

6. Redis 缺少 JuiceFS 风格 client tracking invalidation。
   - BrewFS Redis capability 标记 `watch_invalidation: false`。
   - 没有跨 client invalidation 时，本地 inode/path/children/open cache 只能依赖 TTL 或本 client mutation invalidation。
   - 这限制了 BrewFS 在多 client 部署中提高 TTL 的空间。

## 建议改动

### Patch 1：open-file cache 默认只服务 read-only open

目标：保留重复 read open 的 benchmark 收益，同时去掉最高风险的 stale write-open 路径。

改动：

- 将 `open_file_cache_eligible(read, write, append)` 改为 `read && !write && !append`。
- 更新现有 open-file-cache 测试：read-only open 命中，write-only/RDWR open miss 并走 fresh stat。
- 维持当前 perf profile 的 TTL 与 capacity。
- 如后续确有需求，可新增 `allow_write_open_cache` 配置，默认 false。

收益：

- patch 小，主要是正确性收口。
- 让后续 metadata cache 扩张不依赖写路径 stale attr。
- 更接近 JuiceFS 有 chunk/mtime invalidation 保护的 open-cache 语义。

### Patch 2：增加 MetaClient symlink target cache

目标：减少重复 `readlink` 与 symlink path traversal 的 Redis/backend 访问。

改动：

- 增加一个按 inode key 的小型 Moka cache，value 为 target `String`，TTL 绑定现有 inode cache TTL。
- `MetaClient::read_symlink` backend 成功后写入 cache。
- 如果已有 cached attr 且 kind 不是 symlink，直接返回对应错误，避免 backend 访问。
- 在本 client 的 unlink、rename replacement、delete 等移除或替换 entry 的操作中失效。
- `symlink(parent, name, target)` 成功后可直接插入 cache。

收益：

- 与文件 data/chunk cache 解耦，风险较低。
- symlink target 对同一个 inode 通常不可变；替换路径会创建/移除 entry，而不是原地修改 target。
- 对齐 JuiceFS `m.symlinks` 的具体机制。

### Patch 3：打通 attr-carrying readdirplus

目标：避免 Redis `HGETALL/MGET` 后 FUSE `readdirplus` 再 N 次 child stat。

改动：

- 为 Redis 实现 `MetaStore::readdir_plus`：复用当前 `HGETALL` + `get_nodes(child_inodes)` 流程，返回 `DirEntryPlus { entry, attr: Some(attr) }`。
- 在 `MetaClient` 增加 plus 路径：排序、写入 children，并把 child attrs 写入 `InodeCache`。children complete 写入仍必须保留现有 generation guard。
- FUSE `readdirplus` 在 `offset == 0` 或 refresh handle 时使用该 plus 路径。
- 对未实现 `readdir_plus` 的 store，fallback 到当前 `readdir` + `batch_stat`。
- 普通 `readdir` 行为先不变，避免扩大 blast radius。

收益：

- trait 和 `DirEntryPlus` 已存在，落点明确。
- Redis 当前已经获取了大部分所需数据，主要问题是数据没有向上传递。
- 直接改善目录型 workload 和 package tree 中 `readdirplus` 的 metadata 调用数量。

## 预期测试场景

Unit tests：

- `OpenFileCache`：
  - read-only open 在 TTL 内命中；
  - write-only/RDWR open 不命中，并增加 fresh stat 计数；
  - 本地 mutation 后 cached attr 被失效。
- Symlink cache：
  - 连续两次 `read_symlink` 第二次命中 cache；
  - 非 symlink inode 返回预期错误；
  - unlink/rename replacement 后旧 target 不再返回；
  - 新建 symlink 后可直接命中刚插入的 target。
- Readdirplus：
  - Redis `readdir_plus` 返回稳定排序后的 entries 和 attrs；
  - MetaClient 把 child attrs 写入 `InodeCache`；
  - FUSE `readdirplus` 在 plus 路径成功时不逐 child 调 `stat_ino`；
  - 未实现 plus 的 store 仍走 fallback。

Integration / perf：

- Redis two-client 场景：
  - client A 预热 lookup/readdir/readlink；
  - client B create/unlink/rename/replace；
  - client A 的行为符合 TTL 与本地 invalidation 预期。
- FUSE 重复 `readlink` microbenchmark。
- 冷目录 `readdirplus` benchmark：记录 Redis `cmdstat_get`、`cmdstat_hgetall`、`cmdstat_mget` 与 BrewFS metadata counters。
- 现有 perf 脚本：
  - `tools/perf/run_perf.sh` metadata profile；
  - Redis meta 下 small-file create/read profile；
  - 如存在，补跑 `dirperf`/`dirstress`。

本文档自查命令：

- `rg -n "当前 BrewFS|JuiceFS 对应机制|具体差距|建议改动|预期测试场景|风险" doc/performance/perf-agent-metadata-cache.md`
- `git diff --check -- doc/performance/perf-agent-metadata-cache.md`

## 风险

- open-file cache 改成 read-only 后，某些反复 RDWR open 但不改 metadata 的 benchmark 可能下降；这是为了避免写路径 stale attr，除非后续有更强 freshness 保护。
- symlink cache 必须覆盖本 client 的 unlink/rename replacement invalidation；跨 client stale target 在 Redis client tracking 落地前仍只能依赖 TTL。
- `readdir_plus` 会更积极携带 attrs，大目录下可能增加内存压力；应使用现有 `limit` 参数，避免一次加载超过 handle/page 需要的数据。
- children complete 状态必须继续受 generation guard 保护，否则本地并发 mutation 可能安装 stale complete-negative lookup。
- Redis store `node_cache` 仍会让 `stat_fresh` 只是在 MetaClient 层 fresh。后续应明确命名/文档，或增加真正 bypass store cache 的 stat API。
- JuiceFS 风格 Redis client-side invalidation 牵涉连接初始化、reconnect、key prefix、notification 语义与多 client 正确性，不建议作为第一批 perf patch。

## Top recommendations

1. 先落 read-only open-file-cache eligibility，避免 perf 优化依赖 stale write opens。
2. 增加 symlink target cache，这是低风险 metadata hot-path win。
3. 打通 Redis `readdir_plus` 到 FUSE `readdirplus` 的 attr 传递，避免已取到的 child attrs 被丢弃后再逐项 stat。
