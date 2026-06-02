# Redis Chunk 操作：从 WATCH 乐观锁迁移到 Version + Lua CAS

## 背景

`generic/075` 在 xfstests 中概率性卡死。排查发现 `brewfs.log` 中 Redis 连接数在 2 秒内达到 170 条，大量重复的 `redis connection established` 日志。根因是 chunk slice list 的并发修改使用了 `WATCH + MULTI + EXEC`，每次重试都调用 `create_connection` 新建一条 TCP 连接。在 fsx 两进程并发 truncate 的高冲突场景下，WATCH 频繁失败 → 重试 → 新建连接，形成连接风暴，最终 Redis 被打满、文件系统卡死。

## 问题分析

`RedisMetaStore` 中三个函数使用 `WATCH` 对 chunk list (`c{inode}_{idx}`) 做乐观并发控制：

1. **`rewrite_trimmed_slices`** — truncate 时裁剪 cutoff chunk 的 slice
2. **`replace_slices_for_compact`** — compact 时替换 chunk slice 列表
3. **`replace_slices_for_compact_with_version`** — compact 时做带版本校验的替换

它们共享同一个问题模式：

```rust
for _ in 0..MAX_RETRIES {
    let mut conn = Self::create_connection(&self._config).await?; // 每次新建 TCP 连接

    WATCH key
    LRANGE key 0 -1        // 读取当前数据
    // ... 修改 ...
    MULTI/DEL+RPUSH/EXEC   // 原子替换
    if success { break; }
    // 失败 → 重试 → 又建新连接
}
```

**问题要点：**

- `create_connection` 每次都走 DNS 解析 + TCP 握手 + Redis AUTH
- WATCH 连接不可被其他操作复用（WATCH 是连接级别的状态）
- 两个 fsx 进程并发 truncate 同一文件 → 高冲突 → 大量重试 → 连接风暴
- 满负荷的 Redis 响应变慢 → 更长的 WATCH 窗口 → 更多冲突 → 恶性循环

## 方案：Version 字段 + Lua CAS

### 原理

版本号 CAS 是分布式文件系统的标准方案（etcd revision / ZK zxid / MVCC）：

- 每个 chunk list 配套一个 version key：`c{inode}_{idx}:v`
- 所有修改 chunk list 的操作必须递增 version
- 使用 Lua 脚本做原子 CAS：仅当 version 匹配时才替换数据并递增 version

相比 WATCH 的关键优势：

| | WATCH（旧） | Version + Lua CAS（新） |
|---|---|---|
| 连接 | 每重试新建 TCP | 复用 `self.conn.clone()` |
| 每次重试网络往返 | 3 次（WATCH + LRANGE + EXEC） | 1 次（首次读取 2 次 pipe） |
| 高冲突表现 | 连接风暴 → 系统卡死 | 可控重试，复用连接 |
| FUSE cache 可用 | 否 | 是（version 可传播到上层） |
| 多 key 原子操作 | 麻烦 | 简单（Lua 内多 key） |

### 新增数据结构

```
key: c{inode}_{chunk_index}      type: LIST   (不变)
key: c{inode}_{chunk_index}:v    type: string (新增, version 编号)
```

### 新增 Lua 脚本：`CHUNK_CAS_LUA`

```lua
-- KEYS[1] = chunk list key
-- KEYS[2] = chunk version key
-- ARGV[1] = expected version
-- ARGV[2] = new version
-- ARGV[3..N] = serialized slice data (可为空)

local expected = tonumber(ARGV[1])
local new_ver  = tonumber(ARGV[2])

local current = redis.call('GET', KEYS[2])
local current_ver = current and tonumber(current) or 0

if current_ver ~= expected then
    return 0  -- CAS 失败
end

redis.call('DEL', KEYS[1])
for i = 3, #ARGV do
    redis.call('RPUSH', KEYS[1], ARGV[i])
end

if new_ver > 0 then
    redis.call('SET', KEYS[2], new_ver)
else
    redis.call('DEL', KEYS[2])  -- 空列表时清理 version key
end

return 1
```

### Rust 侧调用模式

```rust
async fn rewrite_trimmed_slices(&self, chunk_id: u64, cutoff_offset: u64) -> Result<()> {
    for _ in 0..MAX_RETRIES {
        let mut conn = self.conn.clone();  // 复用 multiplexed 连接

        // 一次 pipe 读取 version + 当前数据
        let (version, raw): (Option<i64>, Vec<Vec<u8>>) = redis::pipe()
            .cmd("GET").arg(&version_key)
            .cmd("LRANGE").arg(&chunk_key).arg(0).arg(-1)
            .query_async(&mut conn).await?;

        let current_version = version.unwrap_or(0);
        // ... 计算新的 slice 列表 ...
        let new_version = current_version + 1;

        // 原子 CAS
        let ok: i32 = redis::Script::new(CHUNK_CAS_LUA)
            .key(&chunk_key)
            .key(&version_key)
            .arg(current_version)
            .arg(new_version)
            .arg(&new_data)
            .invoke_async(&mut conn).await?;

        if ok == 1 { return Ok(()); }
    }
    Err(...)
}
```

## 修改清单

### `src/meta/stores/redis/mod.rs`

1. **新增 `CHUNK_CAS_LUA` 常量** — CAS Lua 脚本
2. **新增 `chunk_version_key()` helper** — 生成 version key: `c{ino}_{idx}:v`
3. **`append_slice`** — 用 pipe 原子执行 `RPUSH` + `INCR version_key`，保证首次写入即初始化 version 并保持 invariant "每次内容变更 version 递增"
4. **`rewrite_trimmed_slices`** — WATCH 重试循环替换为 version 读取 + Lua CAS
5. **`replace_slices_for_compact`** — 同上；延迟记录创建移到 CAS 成功后，计数器分配改为一次 `INCRBY n` + 批量 pipe
6. **`replace_slices_for_compact_with_version`** — 同上；`expected_slices` 逐条比对替换为 version 号比对（version 单调递增保证内容对应）
7. **`prune_slices_for_truncate` full-chunk-delete 路径** — delete 时同步 `DEL version_key`
8. **GC `process_delayed_slices` LREM 路径** — LREM 时同步 `INCR version_key`

## 验证

**xfstests generic/075**：

| | 修复前 | 修复后 |
|---|---|---|
| Redis 连接数 | 170（2 秒内） | 1 |
| 测试耗时 | 超时卡死（14+ 分钟） | 6~7 秒通过 |
| 日志 | `unmount failed: Device or resource busy` | 干净卸载，无错误 |

## 关于 `create_connection`

修复后 `create_connection` 仅在一处调用：`RedisMetaStore::new` 初始化时创建初始 `ConnectionManager`。所有运行时操作均使用 `self.conn.clone()`（共享 multiplexed connection）。
