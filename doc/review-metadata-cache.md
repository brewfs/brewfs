# BrewFS 元数据层/元数据缓存 Review

审查范围：`src/meta/layer.rs`、`src/meta/client/mod.rs`、`src/meta/client/cache.rs`、`src/meta/store.rs`、`src/meta/stores/redis/mod.rs`，并抽查 sqlite/database、etcd、tikv 实现以及只读参考 `juicefs/pkg/meta`。

## 模块现状摘要

BrewFS 现在的元数据层分成两层：`MetaStore` 是后端抽象，`MetaLayer`/`MetaClient` 是 VFS 使用的高层 facade。`MetaClient` 已经有 inode attr cache、children/readdir cache、path cache、per-inode per-chunk slice cache，以及可配置但默认关闭的 open-file scoped attr cache。VFS open 路径会走 `stat_for_open`/`record_open`/`record_close`，读路径还有 `FileReader` 内部 per-chunk slice metadata cache。

Redis 后端已经针对单个 slice 写入做了重要优化：`write` 使用 `WRITE_SLICE_LUA` 把 `RPUSH slice list`、`INCR chunk version`、`extend file size` 合并到一次 Redis Lua 往返里，这是当前写路径最确定的收益点之一。但这个优化仍是单 slice 粒度，writer 层在 direct=0、randrw 或 buffered 小写场景仍会产生大量独立 slice 和大量 `meta.write()` 调用。

各后端对 slice list 的存储模型不一致：Redis 是每个 chunk 一个 Redis list，`get_slices` 用 `LRANGE 0 -1`；database 是 `slice_meta` 每 slice 一行；etcd/TiKV 把整个 chunk 的 `Vec<SliceDesc>` 作为一个值读改写。compaction API 已存在，包括 `replace_slices_for_compact(_with_version)`、delayed slice、uncommitted slice 清理等，但 write path 本身不会主动控制 slice list 长度。

当前已知性能事实需要纳入后续设计：Redis 单 slice `write` 已用 Lua 合并 append+size；metadata batch write 在 `tools/perf` 中看似提升，但 compose `randrw` 出现拖尾/失败，不能直接合入为默认路径；`direct=0` 会产生大量小 slice，slice list 增长和 compaction 很可能成为后续瓶颈。

## 具体问题、风险与建议

### 1. P0 Redis `WRITE_SLICE_LUA` 先写 slice 后校验 inode，失败会留下脏 slice metadata

- 位置：`src/meta/stores/redis/mod.rs:415` `WRITE_SLICE_LUA`，`src/meta/stores/redis/mod.rs:3030` `RedisMetaStore::write`
- 原因：Lua 脚本第一个动作是 `RPUSH` slice list 和 `INCR` version，然后才 `GET`/decode inode。如果 inode 不存在或 node JSON 损坏，脚本返回错误，但 Redis Lua 不会回滚已经执行的写入，导致 chunk list 出现不可达 slice，并且版本被推进。
- 建议改法：把 inode `GET`、decode、类型/size 校验全部放到 `RPUSH` 前；只有校验成功后再 append slice、更新 version、更新 node。最好补充 `node.kind`/file type 校验，避免目录 inode 被写入。脚本返回错误时应保证 chunk list/version 不变。
- 验证方式：Redis 后端单测：对不存在 inode 调 `write(ino, chunk_id, slice, new_size)`，断言返回 `NotFound` 且 `get_slices(chunk_id)` 为空、version key 未创建或未变化；再覆盖 corrupt node JSON 场景。

### 2. P1 `stat_fresh` 对 Redis 并不真正 fresh，close-to-open 语义容易被 30 秒 store cache 混淆

- 位置：`src/meta/client/mod.rs:1446` `MetaClient::stat_fresh`；`src/meta/stores/redis/mod.rs:1309` `node_cache` TTL 30s；`src/meta/stores/redis/mod.rs:1546` `get_node`
- 原因：`stat_fresh` 绕过了 `MetaClient` 的 inode cache，但调用的是 `store.stat()`；Redis store 的 `get_node` 会先查本地 `node_cache`。测试 `src/meta/stores/redis/tests.rs:2416` 也明确验证 hot `stat_fresh` 不发 Redis GET。这对性能有利，但名字和上层 open 语义会让人误以为每次 open 都强制刷新后端。
- 建议改法：新增后端能力或方法，例如 `stat_no_cache`/`stat_consistent`，让 close-to-open 严格模式真正绕过 Redis store cache；或者把 `stat_fresh` 改名/注释为 “fresh from MetaClient cache only”。open-file cache 开启时也应在配置文档中明确一致性边界。
- 验证方式：双客户端 Redis 集成测试：client A 缓存 stat，client B 修改 size/mtime，client A 调 `stat_fresh`；严格模式应看到新值，性能模式可允许 TTL 内旧值但必须有指标和文档说明。

### 3. P1 Redis/普通 cache 缺少跨客户端失效，lookup/stat/get_slices 都可能读到远端旧值

- 位置：`src/meta/client/mod.rs:1056` `cached_stat`、`:1092` `cached_lookup`、`:2317` `get_slices`；`src/meta/stores/redis/mod.rs:2055` capabilities `watch_invalidation: false`
- 原因：MetaClient 只在本客户端 mutation 后局部失效；etcd 有可选 watch worker，但 Redis capabilities 明确没有 watch invalidation。complete children cache 会缓存 negative lookup，slice cache 会缓存完整 chunk list。多挂载或多进程并发写/rename 后，另一个客户端可能在 TTL 内读旧 namespace、旧 attr 或旧 slice list。
- 建议改法：短期把强一致场景的 Redis TTL/open-file cache 默认保持保守，并在 `.stats` 暴露 stale-risk 配置；中期实现 JuiceFS Redis CSC 类似的 inode/entry invalidation，至少对 write/truncate/rename/unlink/create 发布 invalidation；slice cache 建议使用 chunk version key 作为 cache token。
- 验证方式：两个 MetaClient 指向同一 Redis：A readdir/lookup/get_slices 预热，B create/write/rename/unlink/truncate，A 在 TTL 内重复 lookup/read/stat，断言强一致模式不返回旧结果；同时记录 cache hit/miss 和 Redis QPS。

### 4. P1 open-file cache 目前只缓存 attr，未覆盖 JuiceFS 的 open chunk/slice cache 语义

- 位置：`src/meta/client/cache.rs:416` `OpenFileEntry`、`:457` `OpenFileCache`；`src/meta/client/mod.rs:1470` `stat_for_open`；JuiceFS 对照 `juicefs/pkg/meta/openfile.go:195` `ReadChunk`、`:210` `CacheChunk`、`:227` `InvalidateChunk`
- 原因：BrewFS open-file cache 只复用 `FileAttr`，而 JuiceFS openfiles 同时缓存 attr 和打开文件的 chunk slices，并在 mtime 变化、写入、compact 时细粒度失效。BrewFS 的 slice cache 在 `InodeCache` 和 `FileReader` 中，和 open refs 没有绑定，不能直接复用 JuiceFS “open file 生命周期内减少 get_slices” 的完整收益。
- 建议改法：先保持 attr cache 的小步收益；下一步把 open-file cache 扩展为可选的 per-open chunk slice cache，key 为 `(ino, chunk_index, chunk_version/mtime)`，并把 write/truncate/compact/read retry 的失效统一到一个接口。不要只扩大 TTL。
- 验证方式：小文件重复 open/read benchmark，对比 `open_file_cache_hit`、`get_slices_cache_hit/miss`、Redis `LRANGE/GET` 次数；并跑 write-after-read、rename-over-open-file、truncate-after-open 的一致性测试。

### 5. P1 open-file cache 对 `write=true && append=false` 也命中，写打开可能复用过旧 size/mtime

- 位置：`src/meta/client/mod.rs:539` `open_file_cache_eligible`，`:1470` `stat_for_open`，`:1492` `record_open`
- 原因：`open_file_cache_eligible` 当前条件是 `(read || write) && !append`，读写打开都可复用 attr。对只读重复打开，这是合理的性能优化；但写打开通常需要更谨慎的 close-to-open 判断，特别是跨客户端写入后再本客户端以 write 打开，旧 size 可能影响 append 之外的定位、truncate 前判断或 VFS 句柄初始 attr。
- 建议改法：默认只让 read-only open 命中 open-file cache；read-write/write-only 需要配置开关或先做轻量版本校验。至少要把 write-open 命中从现有指标中单独统计出来。
- 验证方式：双客户端：A 关闭一个文件后保留 open cache，B 扩大文件，A 以 write 或 rdwr 重新 open，断言严格模式下 attr size 为新值；append/open_trunc 路径也应覆盖。

### 6. P1 get_slices cache 没有版本 token，append-only 更新无法覆盖 compact/truncate/远端改写

- 位置：`src/meta/client/cache.rs:342` `append_slice`，`:368` `cache_slices_if_absent`，`:392` `get_slices`；`src/meta/client/mod.rs:2317` `get_slices`
- 原因：缓存的是 `Vec<SliceDesc>` 本身，命中时没有校验 Redis chunk version 或后端 revision。当前本地 `write/append_slice` 只在已有缓存上 append；truncate 会 invalidate inode；compact 走 raw store 时不一定经过 MetaClient，所以 MetaClient/reader 内部 cache 可能不知道 slice list 被替换。Redis version key 已存在，但只用于 CAS，不参与读取缓存判断。
- 建议改法：把 slice cache value 改成 `{version, slices}`。Redis 读时用 pipeline `GET version + LRANGE`；cache hit 前可按配置检查 version。database 可用 chunk row version 或 max updated_at；etcd/TiKV 可用 KV mod_revision/txn version。compactor 完成后要广播或回调 `invalidate_chunk_slices`。
- 验证方式：预热 `get_slices` 后直接通过 store `replace_slices_for_compact_with_version` 替换 slice list，再通过 MetaClient `get_slices` 读取，应返回新 slice。Redis 版本检查应增加一次轻量 GET，但避免 LRANGE。

### 7. P1 direct=0 小写会让 slice list 快速增长，读/写/compact 都会退化

- 位置：`src/vfs/io/writer.rs:729`、`:2389` 每个 frozen slice 调一次 `meta.write`；`src/meta/stores/redis/mod.rs:2980` `LRANGE 0 -1`；`src/meta/stores/etcd/mod.rs:2598` write 读改写整 Vec；`src/meta/stores/tikv/mod.rs:2507` write 读改写整 Vec
- 原因：buffered/direct=0 randrw 会产生大量小 slice。Redis 读 chunk metadata 要拉全 list；etcd/TiKV 每次 append 都读写整个 Vec，复杂度接近随 slice 数线性上升；database 虽是逐行插入，但 get_slices 也要返回全部历史 slice。JuiceFS 有 `buildSlice`/`compactChunk` 逻辑来把历史 slice 解释和压缩，BrewFS 虽有 compactor，但 write path 没有 slice 数阈值和背压。
- 建议改法：增加 per-chunk slice count/bytes 指标和阈值：超过阈值触发异步 compact，或者对同一 chunk 的连续小写做短窗口 coalescing。不要把 direct=0 下所有小写立即暴露为永久 slice list。
- 验证方式：fio `randrw --direct=0` 结束后统计每个 chunk 的 slice count p50/p95/max、`get_slices` latency、compaction 前后读放大；同时复跑 compose randrw，确认没有拖尾失败。

### 8. P1 metadata batch write 还没有安全的一等接口，之前 perf 改动不能直接复用为默认路径

- 位置：`src/meta/store.rs:575` 只有单 slice `write`；`src/vfs/io/writer.rs:729`、`:2249`、`:2389` 多处逐 slice commit；已知事实：`tools/perf` batch write 看似提升，但 compose `randrw` 拖尾失败
- 原因：现有接口只表达“一个 slice + 一个 new_size”的原子更新。直接在 writer 层攒 batch 容易破坏 commit-before-upload、retry、stale epoch、reader invalidation、writeback record 删除等状态机。之前 `tools/perf` 结果说明减少 RTT 有收益，但 compose `randrw` 失败说明尾部 flush/重试/顺序语义还没被证明。
- 建议改法：设计 `write_batch(ino, chunk_id, Vec<SliceDesc>, max_new_size, expected_epoch)`，仅允许同 inode/同 chunk 或明确排序的批次；每个后端实现真正原子提交，VFS 层在 flush 阶段按 chunk 小批量提交。先以 feature flag 在 perf 环境启用。
- 验证方式：除 tools/perf 外，必须跑 compose `fio-randrw direct=0`、随机 truncate+write、kill -9/restart writeback 恢复测试；指标包含尾延迟、失败率、未提交 slice 清理数。

### 9. P1 Redis compaction 的 CAS 替换和 delayed record 创建不是一个原子单元，崩溃会丢 GC 账本

- 位置：`src/meta/stores/redis/mod.rs:3171` `replace_slices_for_compact`，`:3231` CAS 替换 list，`:3246` 后续创建 delayed records；`:3290` versioned variant 同类
- 原因：CAS 成功后旧 slice 已从 chunk list 移除；随后才 `INCRBY ds_counter` 并写 delayed slice hash/zset。如果进程或 Redis 连接在两步之间失败，元数据不再引用旧对象，但 delayed GC 记录没有落地，形成对象泄漏。database/etcd/TiKV 更容易把替换和 delayed 记录放进同一事务，Redis 这里需要 Lua 合并。
- 建议改法：把 CAS 检查、list 替换、delayed id 分配、delayed hash/zset 写入、uncommitted cleanup 合并进一个 Lua 脚本。脚本参数传 old delayed tuples 和 new slice bytes，返回 compact conflict 或成功。
- 验证方式：用故障注入在 CAS 成功后、delayed pipe 前中断，重启后运行 GC，确认旧 slice 要么仍在 chunk list，要么存在 delayed 记录；不能出现两边都没有。

### 10. P2 Redis `stat_fs` 使用 `KEYS node*` 和全量 MGET，会在大规模 inode 下阻塞 Redis

- 位置：`src/meta/stores/redis/mod.rs:2731` `stat_fs`
- 原因：`KEYS` 是阻塞式全库扫描；随后 MGET 所有 node 并解析 JSON。随着 inode 数增长，statfs 会成为 Redis 延迟尖刺，并影响正常 lookup/write。
- 建议改法：维护 used_space/used_inodes 计数器，write/create/unlink/remove/compact 时增量更新；临时方案可用 SCAN 分页并限制调用频率，但长期应避免 statfs 全量扫。
- 验证方式：造 10 万/100 万 inode，压测 statfs 并观察 Redis `commandstats`、p99 延迟和业务 QPS；验证计数器与离线 scan 差异在可接受范围。

### 11. P2 `append_slice` 是公开 MetaLayer/MetaStore 能力，但不更新 file size，容易被误用

- 位置：`src/meta/layer.rs:298`、`src/meta/store.rs:573` `append_slice`；`src/meta/client/mod.rs:2361` `MetaClient::append_slice`
- 原因：`append_slice` 只追加 slice metadata，不更新 inode size/mtime。测试和 helper 大量使用它构造 slice list，但生产调用者如果误用，会得到有 slice 但 stat size 不变的文件。相比之下 `write` 表达了 append slice + size 的一致提交。
- 建议改法：将 `append_slice` 标注为测试/内部工具，生产写路径只允许 `write` 或未来 `write_batch`；如果保留公开接口，命名为 `append_slice_without_attr_update` 并在 trait 文档中写清楚语义。
- 验证方式：静态搜索生产路径只允许 compactor/test/reader helper 使用；加 lint 或单测确认普通 write API 后 size 和 slices 同步。

## 最值得优先尝试的 3 个优化方案

### A. 修正 Redis write Lua 原子性，并把 chunk version 纳入 get_slices cache

- 预期收益：先消除 P0 脏 slice 风险，再用 version token 让 MetaClient slice cache 在 Redis 上可控地减少 `LRANGE 0 -1`。对 direct=0 randrw，能降低重复读同一 chunk metadata 的开销，同时避免 compact/write 后读旧 list。
- 回退风险：每次 cache hit 如需校验 version 会多一次 Redis GET；可用配置分为 `strict` 和 `ttl-only` 两档。如果 GET 开销抵消收益，可只在打开文件/reader 生命周期内校验。
- 建议验证：Redis unit + two-client stale test + fio randrw direct=0，比较 `LRANGE` 次数、get_slices p95、读正确性。

### B. 做受限的 per-chunk metadata batch write，而不是泛化全局 batch

- 预期收益：减少 direct=0 小写的 Redis Lua/SQL txn/etcd txn 次数，避免每个 slice 都独立提交元数据。比全局 batch 更容易保证顺序、epoch、reader invalidation 和失败恢复。
- 回退风险：已有事实显示 tools/perf batch write 有提升但 compose randrw 拖尾失败，说明尾部 flush 和异常恢复是主要风险。必须 feature flag 默认关闭，并限制同 inode/同 chunk、小批量、短时间窗口。
- 建议验证：先只在 Redis 实现 `write_batch_same_chunk` Lua，VFS 层按 chunk 聚合；通过 compose `fio-randrw`、truncate race、writeback crash recovery 后再考虑 database/etcd/TiKV。

### C. 建立 slice list 增长控制：指标、阈值、异步 compact/coalescing

- 预期收益：direct=0 大量小 slice 是当前最可能的下一阶段瓶颈。给每个 chunk 暴露 slice count、metadata bytes、compact attempts/conflicts 后，可以按阈值触发 compaction 或写前 coalescing，降低 get_slices 读放大和 etcd/TiKV 整值重写成本。
- 回退风险：compaction 会引入额外对象写、delayed GC、CAS conflict 和读写竞争。阈值过低会拖慢写入，过高又无法控制尾延迟。建议先只观测和后台 compact，不阻塞前台 write。
- 建议验证：fio direct=0 randrw 后统计 slice count 分布；打开后台 compact 后比较读 p99、Redis list 长度、delayed slice 积压、对象泄漏检查结果。

## 并行 agent 补充审查

### P2 补充：open-file cache TTL 不是 TTI，`last_check` 不会延长寿命

- 位置：`src/meta/client/cache.rs::OpenFileCache` 与 `OpenFileEntry::last_check`。
- 发现：open-file cache entry 内部会更新 `last_check`，但 Moka 使用的是 `time_to_live`，不是 `time_to_idle`。这意味着反复 open/read 不能自然延长缓存寿命，`last_check` 更像一个未被过期策略消费的字段。
- 改动建议：如果目标是 JuiceFS open-cache 类似的“打开文件活跃期缓存”，应改为 `time_to_idle` 或在每次 `record_open/record_close/stat_for_open` 后 refresh/replace entry。若坚持 TTL，应删除或重新定义 `last_check`，避免误导。
- 验证：设置短 TTL，连续 open 同一 inode；TTI 模式下热文件不应过期，TTL 模式下应按固定时间过期，指标里分别记录 open cache hit/miss。

### P2 补充：`cache.enabled=false` 没有明确绕过 MetaClient cache

- 位置：`src/meta/config.rs::CacheConfig.enabled`、`src/meta/factory.rs` 构造 MetaClient 的路径。
- 发现：配置层有 `enabled` 开关，但 factory 仍会构造带默认容量/TTL 的 `MetaClient` cache，容易让测试者以为已经关闭元数据缓存，实际还在命中 attr/path/slice cache。
- 改动建议：factory 在 disabled 时使用 zero TTL/zero capacity，或显式构造 no-cache facade；启动日志和 `.stats` 输出最终 cache enabled、TTL、capacity，便于性能 profile 复现。
- 验证：`cache.enabled=false` 下跑 lookup/stat/get_slices，Redis commandstats 应明显增加，MetaClient cache hit counter 应为 0。

### P1 补充：open-file cache 对写打开命中需要单独指标和开关

- 位置：`open_file_cache_eligible`、`stat_for_open`、`record_open`。
- 发现：当前 `(read || write) && !append` 让 `O_WRONLY/O_RDWR` 也可复用旧 attr。只读重复 open 命中是合理优化，但写打开通常需要更严格的 close-to-open 判断，尤其跨客户端写入后本客户端再以 write 打开。
- 改动建议：默认只让 read-only open 命中 open-file cache；write-open 命中需要单独配置开关或轻量 version 校验，并把 read-open/write-open hit 分开统计。
- 验证：双客户端：A 预热 open cache，B 扩大文件，A 以 write/rdwr 重新 open；严格模式下 size/mtime 必须是新值，性能模式必须在文档和指标中标注 stale-risk。

### P1 补充：JuiceFS 对照说明 open cache 不只缓存 attr

- 位置：BrewFS `OpenFileEntry` 与 JuiceFS `openfile.go` 的 `ReadChunk/CacheChunk/InvalidateChunk`。
- 发现：BrewFS open-file cache 目前只是 attr cache；JuiceFS 的 openfiles 还缓存打开文件的 chunk/slice 信息，并在写入、truncate、mtime 变化时失效。继续只扩大 BrewFS open attr TTL，不会拿到 JuiceFS open-cache 的主要 metadata read 收益。
- 改动建议：下一步如果要追 JuiceFS，应扩展为 per-open chunk slice cache，key 包含 `(ino, chunk_index, chunk_version/mtime)`，并复用 writer/truncate/compact/read retry 的统一失效接口。
- 验证：小文件反复 open/read，比较 Redis `LRANGE` 次数、get_slices cache hit、open cache hit；再跑 write-after-open/truncate-after-open 一致性。

## 核心结论

元数据缓存方向是对的，但现在最大的风险不是“缓存不够多”，而是缓存一致性边界和配置语义不够显式：`stat_fresh` 在 Redis 下并不真正 fresh，slice cache 没有 version token，Redis 也没有跨客户端 invalidation；同时 open-file cache 的 TTL/写打开命中和 `cache.enabled` 开关都需要让行为可验证。性能上，Redis 单 slice Lua 已经解决了一次明显 RTT，但 direct=0 会制造大量小 slice，后续瓶颈会转移到 slice list 增长、`get_slices` 全量读取和 compaction/GC 账本。建议优先修 Redis write 原子性，再做版本化 slice cache，最后谨慎推进同 chunk batch write 和 slice list 增长控制。
