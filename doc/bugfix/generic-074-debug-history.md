# generic/074 调试与修复全记录

## 1. 文档范围

本文档只记录这一轮 `xfstests generic/074` 调试过程中我实际做过、并且与问题定位和修复直接相关的修改。

不包含工作区中其它并行开发项，也不把无关改动混入本次问题分析。

最终结果：

```text
generic/074 Passed all 1 tests
artifacts/run-1777738548-15565
```

---

## 2. 问题背景

`generic/074` 主要覆盖 `fstest` 的几类场景：

1. 普通写入与校验
2. 单进程 mmap 写入与校验
3. 多进程普通写入与校验
4. 多进程 mmap 写入与校验
5. 多进程 mmap + sync 写入与校验

本轮 BrewFS 的实际问题演化是：

1. 最开始卡在 `fstest.2`
2. 修掉卡死后，推进到 `fstest.3`
3. `fstest.3` 持续出现 mmap 写入后的读零数据损坏
4. 最后通过 `FUSE_WRITE_CACHE` 可见性修复，整个 `generic/074` 通过

---

## 3. 修复总览

这一轮真正解决问题的修改可以归为四类：

1. 调试基础设施：把日志拆开，并修正容器 mount helper 的环境变量丢失问题
2. `fstest.2` 卡死修复：避免在 `truncate/setattr(size)` 路径里持锁等待长时间 flush
3. 属性正确性修复：让 `size` / `blocks` 在 writeback-cache 与 sparse file 场景下更符合内核预期
4. `fstest.3` mmap 可见性修复：修正 userspace writer 异步提交与 `FUSE_WRITE_CACHE` 返回时机之间的竞态

---

## 4. Bug 点 1：FUSE 操作日志最初并没有真正落盘

### 现象

一开始需要靠 FUSE op log 分析 `generic/074`，但容器跑完后只有主日志，没有拿到预期的 FUSE 操作日志，导致无法判断 `WRITE`、`READ`、`FLUSH` 的实际顺序。

### 根因

`docker/compose-xfstests/run_xfstests_in_container.sh` 在生成 mount helper 时使用了会阻止变量展开的 heredoc 写法，导致 `BREWFS_FUSE_LOG_FILE` 在 helper 脚本里没有被正确带入。

结果是：

1. mount helper 实际启动时丢失了 FUSE 日志路径
2. `BREWFS_FUSE_OP_LOG=1` 虽然看起来设置了，但日志文件并没有按预期写入到 artifact 目录

### 修改

做了两层修复：

1. 在 `src/main.rs` 中增加分离日志能力：
   - 主日志走 `BREWFS_LOG_FILE`
   - FUSE op 日志走 `BREWFS_FUSE_LOG_FILE`
2. 在 `docker/compose-xfstests/run_xfstests_in_container.sh` 中把运行时确定的路径直接 baked 进 helper，避免 mount 时环境被清空或丢失

### 相关文件

1. `src/main.rs`
2. `docker/compose-xfstests/run_xfstests_in_container.sh`
3. `docker/compose-xfstests/docker-compose.redis.yml`
4. `docker/compose-xfstests/docker-compose.etcd.yml`
5. `docker/compose-xfstests/docker-compose.sqlite.yml`
6. `docker/compose-xfstests/docker-compose.redis-perf.yml`
7. `docker/compose-xfstests/docker-compose.etcd-perf.yml`

### 结果

后续 run 成功拿到了 `brewfs_fuse_ops.log`，才得以继续分析 `WRITE_CACHE`、`READ`、`FLUSH` 的相对顺序。

---

## 5. Bug 点 2：`fstest.2` 卡死，根因是 truncate/setattr(size) 持锁等待 flush

### 现象

`generic/074` 最早不是数据损坏，而是卡死在 `fstest.2`。目录和文件已经创建出来了，但测试长时间不前进，最后被 xfstests 超时杀掉。

### 根因

核心问题在 `src/vfs/fs/mod.rs` 的 `truncate_inode()` 和 `set_attr()` 的 size 修改路径：

1. 先拿 inode 级别互斥
2. 再调用 `flush_required()`
3. `flush_required()` 可能等待很久，因为它会等 writer upload/commit 排空

而此时：

1. FUSE 后续的 `WRITE` 还在继续进来
2. 这些 `WRITE` 又需要同一个 inode 的互斥锁
3. 于是形成长时间互相等待

这在 `write_back=true` 的 FUSE writeback-cache 模式下尤其容易把内核 writeback 一起拖住，最终表现为 `pwrite` 长时间阻塞。

### 修改

把控制顺序改成：

1. 先 `flush_required()`
2. 再获取 inode 级别锁
3. 再执行 truncate / setattr(size)
4. 获锁后调用 `writer.clear()`，清掉 pre-flush 与加锁之间新落进来的脏 slice

这个修改同时做在：

1. `truncate_inode()`
2. `set_attr()` 的 `req.size.is_some()` 分支

### 相关文件

1. `src/vfs/fs/mod.rs`

### 结果

修复后：

1. `fstest.2` 不再卡死
2. `generic/074` 开始稳定推进到 `fstest.3`

这说明最初的 hang 主因已经被清掉。

---

## 6. Bug 点 3：`st_blocks` 以前按逻辑大小算，稀疏文件会报错

### 现象

在 sparse file / hole 场景里，`stat(2)` 看到的 `st_blocks` 不应该简单按 `size / 512` 推出来，因为逻辑大小和真实已提交数据量并不相同。

如果继续按逻辑大小算：

1. 带 hole 的文件会显得“占用太多 blocks”
2. mmap / truncate / sparse 校验类测试更容易把 BrewFS 识别成语义错误

### 根因

FUSE 回复属性时原先直接按：

```text
blocks = size.div_ceil(512)
```

这对密集文件勉强成立，对 sparse file 明显不成立。

### 修改

最终采用的是“只在 VFS/FUSE 视图层推导 blocks，而不是把 blocks 存进持久化 attr”这条设计：

1. `FileAttr` 不新增持久化 `blocks` 字段
2. `src/vfs/inode.rs` 增加 `committed_bytes`
3. `commit_chunk` 成功后累加 committed bytes
4. truncate 时重置 committed bytes
5. `VFS::blocks_for_attr()` 用 committed bytes 计算 blocks
6. `src/fuse/mod.rs` 的 `vfs_to_fuse_attr()` 改为显式接收 blocks 参数

### 相关文件

1. `src/vfs/inode.rs`
2. `src/vfs/io/writer.rs`
3. `src/vfs/fs/mod.rs`
4. `src/fuse/mod.rs`

### 结果

`st_blocks` 从“逻辑大小近似值”改成“已提交数据量近似值”，避免把 sparse file 错误描述成 fully allocated file。

---

## 7. Bug 点 4：非 size 的 setattr 可能把内核看到的文件大小回退成旧值

### 现象

在 writeback-cache + mmap 路径里，内核可能发出只更新时间戳的 `setattr(size=None)`。如果这时 BrewFS 回复的 attr.size 还是元数据层旧值，而不是本地已扩展的新值，会让内核误以为文件大小退回了旧状态。

这类问题会放大 mmap 读零或 page cache 错乱的风险。

### 根因

`set_attr()` 处理非 size 请求时，之前直接返回 meta 层 attr，而 meta 层 size 未必已经追上本地 writer 的最新扩展结果。

### 修改

在 `src/vfs/fs/mod.rs` 中增加逻辑：

1. 如果 `req.size.is_none()`
2. 且本地 inode cache 里有更大的 size
3. 则用本地 size 覆盖返回 attr.size

### 相关文件

1. `src/vfs/fs/mod.rs`

### 结果

这个修改是一次重要的 correctness hardening。后续日志确认：`setattr(size=None)` 的回复 size 已不再退回到 0。

它不是最后解决 `fstest.3` 的唯一根因，但属于必须保留的正确性修复。

---

## 8. Bug 点 5：读路径原先靠“先 flush 再 read”，但对 mmap/writeback 并不可靠

### 现象

在 `fstest.3` 里，mmap 写入之后的读校验持续读到 `00 00 00 ...`，而不是预期的数据模式。最典型的损坏是：

1. `file0`
2. 小 offset，比如 `4096` 或 `32768`
3. 预期是某个固定字节值重复
4. 实际全是零

### 根因

原读路径在 `src/vfs/fs/mod.rs` 里是：

1. 读之前尝试 `flush_if_exists()`
2. 然后直接 `handle.read()`

这条路径有两个问题：

1. flush 是 best-effort，不是强保证
2. 即便 writer 内部仍然持有比底层持久层更新的数据，读路径也没有把这些 dirty slices 覆盖到 read buffer 上

### 修改

把读路径改成：

1. 先执行底层 `handle.read()`
2. 再调用 `writer.overlay_dirty_if_exists()`，把尚未完全通过正常读路径可见的 dirty data 盖回到结果 buffer

同时新增：

1. `CacheSlice::copy_into()`
2. `Page::copy_slice()`
3. `FileWriter::overlay_dirty()`
4. `FileWriters::overlay_dirty_if_exists()`

### 相关文件

1. `src/vfs/fs/mod.rs`
2. `src/vfs/io/writer.rs`
3. `src/vfs/cache/page.rs`

### 结果

这一步把问题从“完全看不到 writer 内存态”推进成“能看到一部分 dirty view，但仍然存在特定窗口下的零读”。

也就是说，它是必要修复，但还不够。

---

## 9. Bug 点 6：`overlay_dirty` 最初忽略了 `Uploaded/Committed` 切片

### 现象

即使已经做了读路径 overlay，`fstest.3` 仍然失败。进一步排查发现，writer 中并不是只有 `Writable/Readonly` 切片，很多 mmap 写回切片已经推进到了：

1. `Uploaded`
2. `Committed`

但它们在真正从 chunk slice 队列里移除前，仍然可能代表“比底层读路径更新的数据”。

### 根因

`src/vfs/io/writer.rs` 中 `can_overlay_read()` 原先只允许：

1. `Writable`
2. `Readonly`
3. `Failed`

这意味着一旦切片推进到 `Uploaded/Committed`，读覆盖逻辑就会把它们忽略掉。

### 修改

把可参与读覆盖的状态扩展为：

1. `Writable`
2. `Readonly`
3. `Uploaded`
4. `Failed`
5. `Committed`

### 相关文件

1. `src/vfs/io/writer.rs`

### 结果

这一步修复了一个清晰的状态判断错误，但仍没有完全消除 `fstest.3` 的 mmap 零读。

说明问题不只是“状态不参与 overlay”，还有更深一层的时序问题。

---

## 10. Bug 点 7：uploaded page 在 metadata commit 前被提前释放，导致 overlay 读到的仍是零

### 现象

继续往下挖后发现，即使切片状态允许 overlay，也不代表切片里还保留着真正的数据页。

### 根因

`src/vfs/io/writer.rs` 的 `advance_upload()` 在 upload 成功后会立刻：

1. 增加 `uploaded` 偏移
2. 调用 `release_block()` 释放已上传 block 的 page
3. 而 metadata commit 还没完成

于是出现一个危险窗口：

1. upload 已成功
2. metadata 还没 commit
3. read 想靠 overlay 看到最新数据
4. 但内存页已经提前释放
5. 结果 overlay 拷出来的又是零页

### 修改

把这一步改成“延后释放”：

1. upload 成功后不再提前 `release_block()`
2. 把 page 保留到 commit 后 slice 被真正 pop/remove 为止

### 相关文件

1. `src/vfs/io/writer.rs`

### 结果

这一步清掉了 “uploaded but not yet committed” 窗口里的零读问题，但 `fstest.3` 仍然还有最后一个竞态没有解决。

---

## 11. Bug 点 8：最终根因，`FUSE_WRITE_CACHE` 返回成功时数据对 close 后重开读仍不可见

### 现象

这是最后真正打穿 `generic/074` 的根因。

结合 `fstest.c` 源码，`fstest.3` 的关键顺序是：

1. `open(O_RDWR|O_TRUNC)`
2. `ftruncate(file_size)`
3. `mmap(MAP_SHARED)`
4. 直接写映射内存
5. `munmap()`
6. `close(fd)`
7. 重新 `open(O_RDONLY)`
8. `pread()` 校验所有块

所以最终失败点已经被缩小成：

```text
close(fd) 返回以后，下一次 open + pread 仍然可能看到旧零块
```

### 根因

`FUSE_WRITE_CACHE` 请求在 BrewFS 里之前是这样处理的：

1. 落进 userspace writer buffer
2. 立即给 FUSE `ReplyWrite { written: ... }`
3. 实际 upload / commit 继续异步进行

这就留下了一个最后的可见性竞态：

1. 内核已经认为 cached page writeback 成功了
2. `close(fd)` 返回
3. 测试马上 `open + pread`
4. 但 BrewFS userspace 里的 writer 还没把数据真正 flush/commit 到可读路径
5. 于是读回旧的零块

这也是为什么前面只靠 overlay 修修补补还不够，因为这里已经进入了“close 后重开读”的阶段。

### 修改

在 `src/vfs/fs/mod.rs` 中，把 `write_cached_ino()` 改成：

1. 先执行 `write_ino_inner()`
2. 再立刻 `writer.flush_required(ino)`
3. 只有 flush/commit 真正完成后，才向 FUSE 返回 cached write 成功

也就是把 `FUSE_WRITE_CACHE` 从“异步可见”改成“返回成功前同步可见”。

### 相关文件

1. `src/vfs/fs/mod.rs`
2. `src/fuse/mod.rs`

### 结果

这是最终让 `generic/074` 通过的决定性修复。

验证结果：

```text
generic/074 656s
Passed all 1 tests
```

artifact：

```text
docker/compose-xfstests/artifacts/run-1777738548-15565
```

---

## 12. Bug 点 9：FUSE op log 默认开启会严重放大排查成本与产物体积

### 现象

在问题已经定位完成后，继续默认开启 FUSE op log 会带来两个副作用：

1. 日志体积非常大
2. 后续每次回归验证都更慢、更难读

### 根因

`run_xfstests_in_container.sh` 曾经是默认在 helper 启动时直接硬编码：

```text
BREWFS_FUSE_OP_LOG=1
```

这意味着后续每次 xfstests run 都会开 FUSE op log，即使只是做回归验证。

### 修改

把策略改成：

1. 默认关闭
2. 只有显式设置 `BREWFS_FUSE_OP_LOG=1|true|yes|on` 才开启
3. compose 文件保留开关透传，方便以后需要时重新抓日志

### 相关文件

1. `docker/compose-xfstests/run_xfstests_in_container.sh`
2. `docker/compose-xfstests/docker-compose.redis.yml`
3. `docker/compose-xfstests/docker-compose.etcd.yml`
4. `docker/compose-xfstests/docker-compose.sqlite.yml`
5. `docker/compose-xfstests/docker-compose.redis-perf.yml`
6. `docker/compose-xfstests/docker-compose.etcd-perf.yml`

### 结果

后续回归跑 `generic/074` 时默认只保留主日志，必要时再显式打开 FUSE op log。

---

## 13. 其它与本次问题相关的辅助修复

这些修改不是最终单点根因，但属于同一调试链路中必要的正确性补强：

### 13.1 `release` 路径改为在 `_flush=true` 时先显式 flush 再 close

目的：

1. 减少 close/release 语义不一致
2. 让 FUSE 释放路径更贴近内核预期

相关文件：

1. `src/fuse/mod.rs`

### 13.2 `flush_and_sync_handle()` 不再只依赖 handle 是否可写

目的：

1. mmap 写回可以通过共享 writer 落在 inode 上
2. 即使当前 handle 不是 write handle，`fsync/flush` 也应该能把共享 writer 排空

相关文件：

1. `src/vfs/fs/mod.rs`

### 13.3 `HandleWriteGate` 去掉“每次 write 后强制同步 flush”

目的：

1. 避免每个 write 都走完整 upload/commit
2. 降低 writer 状态机被小写放大的概率

相关文件：

1. `src/vfs/handles.rs`

### 13.4 `commit_chunk` 增加 upload re-kick

目的：

1. 避免 frozen slice 存在但 uploader 没继续推进时，commit 一直空等
2. 这是先前 `generic/013 --s3` hang 分析里也验证过的重要修复点

相关文件：

1. `src/vfs/io/writer.rs`

---

## 14. 这轮实际修改过的关键文件

与 `generic/074` 调试和修复直接相关、并且在这一轮被改动的核心文件如下：

1. `src/main.rs`
2. `src/fuse/mod.rs`
3. `src/vfs/fs/mod.rs`
4. `src/vfs/io/writer.rs`
5. `src/vfs/cache/page.rs`
6. `src/vfs/inode.rs`
7. `src/vfs/handles.rs`
8. `docker/compose-xfstests/run_xfstests_in_container.sh`
9. `docker/compose-xfstests/docker-compose.redis.yml`
10. `docker/compose-xfstests/docker-compose.etcd.yml`
11. `docker/compose-xfstests/docker-compose.sqlite.yml`
12. `docker/compose-xfstests/docker-compose.redis-perf.yml`
13. `docker/compose-xfstests/docker-compose.etcd-perf.yml`

---

## 15. 最终结论

这轮 `generic/074` 的问题并不是单一 bug，而是一串彼此叠加的问题链：

1. 先是调试信息拿不到
2. 再是 `fstest.2` 因为 truncate/flush 锁顺序问题卡死
3. 推进到 `fstest.3` 后，又暴露出 mmap/writeback-cache 可见性问题
4. 中间还夹杂 `size` / `blocks` / dirty overlay / uploaded page 生命周期等 correctness 细节
5. 最后真正决定测试成败的根因，是 `FUSE_WRITE_CACHE` 在返回成功时数据还停留在 userspace 异步 writer 中，导致 close 后重开读与 commit 可见性之间存在竞态

最终修复思路可以概括为一句话：

```text
把 mmap/writeback-cache 路径上的“异步最终一致”收紧成 close 后读所需要的“同步可见”
```

也正是这个收口，最终让 `generic/074` 从：

1. hang
2. `fstest.3` 读零损坏
3. 输出不匹配

推进到了完全通过。

---

## 16. 第二轮回归：generic/074 耗时过长 / flush timeout

### 触发现象

在后续回归测试中，`generic/074` 出现以下行为：

1. `fstest.0`、`fstest.1` 几秒内通过
2. `fstest.2`（单进程 mmap 写入与校验）运行 4+ 分钟后仍未完成
3. 日志末尾出现 `flush timeout` 错误：

```text
2026-05-19T06:38:22 ERROR brewfs::vfs::io::writer: flush timeout, ino: 37856,
elapsed_ms: 6145, pending_slices: 1, pending_states: ["Uploaded@14520320"]
```

4. 测试被手动中断（Ctrl+C）

artifact：`docker/compose-xfstests/artifacts/run-1779170794-23960`

### 分析路径

从日志确认测试并未真正死锁——写操作持续推进（auto_flush 冻结、upload 完成、compaction 运行），但整体吞吐显著低于预期。

关键线索是 flush timeout 报出的状态：slice 在 `Uploaded` 但从未进入 `Committed`，说明 `commit_chunk` 背景任务已退出但 slice 仍在等待 metadata commit。

---

## 17. Bug 点 10：commit_chunk 退出后新 slice 无 committer（竞态）

### 现象

`flush_for_close`（CLOSE_FLUSH_DEADLINE = 5s）或 `flush_required`（FLUSH_DEADLINE = 300s）等待某个 slice 从 `Uploaded` 变为 `Committed`，但永远等不到。

### 根因

`commit_chunk` 的退出逻辑存在竞态：

```rust
// commit_chunk 发现 slices.front() == None
let Some(slice) = slice else {
    let mut guard = shared.inner.lock().await;
    let keep = !recently_committed.is_empty();
    if !keep { guard.chunks.remove(&chunk_id); }
    // ↑ keep == true 时：chunk 留在 map，commit_started 仍为 true
    return; // ← commit_chunk 退出
};
```

时序：

1. `commit_chunk` 把最后一个 slice 从 `slices` 移到 `recently_committed`
2. 下一次循环：`slices.front()` → None
3. `recently_committed` 非空 → chunk 不被删除，`commit_started` 保持 `true`
4. `commit_chunk` **return**
5. 新的 cached write 到来，`get_or_create_chunk` 发现 chunk 已存在
6. `find_slice_or_create` 检查 `chunk.commit_started` → `true` → **不 spawn 新 commit_chunk**
7. 新 slice 被 upload 后进入 `Uploaded` 状态，但无人推进到 `Committed`
8. flush 等待超时

### 修改

在 `commit_chunk` 的"slices 为空但 recently_committed 非空"退出路径中，在持锁状态下：

1. 检查是否有新 slices 出现（有则 `continue` 继续处理）
2. 若无新 slices，重置 `chunk.commit_started = false`

这保证下一次写入必然 spawn 新的 `commit_chunk` 任务。

同时在 `flush_with_deadline` 的等待循环中增加安全网：如果发现有 `Uploaded` 状态 slice 但 chunk 的 `commit_started == false`，主动重新 spawn `commit_chunk`。

### 相关文件

1. `src/vfs/io/writer.rs`

### 结果

消除了 slice 在 `Uploaded` 状态永远无法 commit 的窗口。flush/close 不再因为等待死去的 committer 而超时。

---

## 18. 性能点 1：write_cached_ino 持 per-inode 互斥锁导致 mmap writeback 串行化

### 现象

`generic/074` 的 `fstest.2`～`fstest.5` 均涉及 mmap 写入。内核 writeback-cache 模式下，多个 dirty page 可能并发通过 `FUSE_WRITE_CACHE` 发送。但原有实现每次 cached write 都获取 per-inode `mutation_lock`，所有并发 page flush 串行化。

### 根因

原 `write_cached_ino` 直接调用 `write_ino_inner`，后者的控制流：

```text
1. append_lock.lock()           ← 全部 cached write 在此排队
2. meta_stat_required()         ← 每次都查 metadata 确认 inode 类型
3. ensure_inode_registered()
4. writer.write_at_cached()     ← 真正的写入（内部有 slice 级锁）
5. reader.invalidate()          ← 失效读缓存
6. extend_local_file_size()
7. modified.touch()             ← 全局 mutex
```

对于 4KB page writeback，步骤 1/2/5/7 都是不必要的开销：

- `mutation_lock`：writer 内部已有 slice 级锁，truncate 也走 flush-before-lock
- `meta_stat_required`：FUSE_WRITE_CACHE 只对已打开的文件触发，类型不可能变
- `reader.invalidate`：`commit_chunk` 在 commit 成功后已做 invalidate
- `modified.touch`：flush 路径覆盖此标记，不需要每个 page 写一次

### 修改

将 `write_cached_ino` 从委托 `write_ino_inner` 改为独立快速路径：

```rust
pub async fn write_cached_ino(&self, ino, offset, data) -> Result<usize> {
    let inode = self.ensure_inode_registered(ino).await?;
    let writer = self.state.writer.ensure_file(inode.clone());
    let written = writer.write_at_cached(offset, data).await?;
    let new_end = offset + written as u64;
    if new_end > inode.file_size() {
        self.extend_local_file_size(ino, new_end);
    }
    Ok(written)
}
```

关键差异：

| 步骤 | 原路径 | 新路径 |
|------|--------|--------|
| mutation_lock | 每次获取 | 不获取 |
| meta_stat_required | 每次查询 | 跳过 |
| reader.invalidate | 每次调用 | 跳过（commit 时做） |
| modified.touch | 每次更新 | 跳过（flush 时做） |
| extend_size | 比较 attr.size | 比较 inode 原子 size |

### 正确性论证

1. **truncate 安全**：truncate_inode / set_attr 先调用 `flush_before_truncate`（等所有 pending writes 完成），再获取 mutation_lock，再调用 `writer.clear()`。不需要 cached write 侧持 mutation_lock。
2. **并发写安全**：writer.write_at_cached 内部通过 `shared.inner.lock()` 保证 slice-level 互斥。
3. **size 安全**：`inode.extend_size()` 使用 CAS 原子操作，多个并发写收敛到最大值。
4. **类型安全**：FUSE_WRITE_CACHE 只在内核已确认为 regular file 时发送，不存在类型变化可能。

### 相关文件

1. `src/vfs/fs/mod.rs`

### 预期效果

mmap writeback 吞吐从"串行 per-page mutex + metadata lookup"降低到"仅 slice 级锁 + 原子 size 更新"。对 `generic/074` 的 fstest.2～5（大量 4KB page 并发写）应有显著提速。

---

## 19. 性能分析：其它观察到的特征（未在本轮修复）

### 19.1 auto_flush INFO 日志量

每个 slice 冻结时都输出 `INFO` 级别日志。在 mmap 写入峰值时，500ms 内积累的 slice 在同一时刻全部冻结，产生数百条 INFO 消息。日志 I/O 本身成为可测量开销。

**建议**：将 `auto_flush: freezing slice` 降为 `DEBUG` 级别或加 rate-limit。

### 19.2 inner lock 竞争热点

writer 的 `shared.inner.lock()` 被以下路径共用：

- `write_at_cached`（每次 cached write）
- `overlay_dirty`（每次 read）
- `flush_with_deadline`（flush 启动 + 等待循环）
- `commit_chunk`（commit 循环）
- `auto_flush`（定时扫描）
- `has_pending`（flush/overlay 前置检查）

在重度 mmap 写入 + 读验证并发场景下，所有路径竞争同一 tokio Mutex。长期方向可考虑：

- 读路径（overlay_dirty）使用 snapshot / 无锁遍历
- 将 chunks map 改为 per-chunk 独立锁，降低锁粒度

### 19.3 compaction 与前台 IO 带宽竞争

`generic/074` 运行期间，后台 heavy compaction 持续执行（从日志可见每 2-3 秒完成一次 compaction，每次读取 10-20 slices 合并上传）。compaction 与前台 write upload 共享 S3 带宽（max_concurrency=8），在高写入场景下可能抢占上传带宽。

**建议**：考虑在检测到活跃 flush/fsync 时暂停或降低 compaction 并发。

### 19.4 back_pressure 10ms sleep

当 writer buffer usage > soft_limit (300MB) 时，每次 write 固定 sleep 10ms。对于 4KB page 的 cached write，单个 10ms sleep 对应 ~400KB/s 吞吐上限。实际测试中 300MB 阈值可能不容易触达，但值得关注。

---

## 20. 第二轮修改的关键文件

1. `src/vfs/io/writer.rs` — commit_chunk 竞态修复 + flush 安全网
2. `src/vfs/fs/mod.rs` — write_cached_ino 去锁快速路径

---

## 21. 第二轮结论

本轮问题的核心是：

1. **正确性 bug**：`commit_chunk` 退出时留下 `commit_started=true` 的 orphan chunk，后续 slice 永远无法 commit，导致 flush 等待 300s 超时（表现为测试"卡死"）
2. **性能缺陷**：`write_cached_ino` 持 per-inode 互斥锁 + 冗余 metadata 查询 + 冗余 reader invalidation，使 mmap writeback 吞吐远低于 writer 实际能力

修复思路可以概括为：

```text
正确性：确保 chunk 生命周期内始终有活跃的 committer 或可重新 spawn committer 的条件
性能：把 cached writeback 热路径从"per-page 全局串行"改为"slice 级并发 + 原子 size"
```

---

## 22. 第三轮优化：inner lock 竞争缓解

基于 §19.2 分析出的 inner lock 热点，本轮针对三个高频获取锁的路径进行优化，减少锁持有时间和获取频率。

### 22.1 flush_with_deadline 安全网锁获取优化

**问题**：safety-net 代码（检查 orphan Uploaded slices 并 re-spawn commit_chunk）在每次 notify 唤醒后都获取 inner lock，但 notify 唤醒说明正在有进展，不需要安全网介入。

**修复**：重构条件判断，仅当 `FLUSH_WAIT`（3 秒）超时触发（即无进展）时才获取 inner lock 执行安全网检查。正常 notify 唤醒（有 commit 完成）直接回到循环顶部重新检查 all_done，无需加锁。

```rust
// 修改前：无论 notify 还是 timeout 都执行安全网（获取 inner lock）
// 修改后：
if timeout(FLUSH_WAIT, notify).await.is_err() {
    // 仅超时路径才获取 inner lock 做安全网检查
    let mut guard = self.shared.inner.lock().await;
    // ... re-spawn orphan commit_chunk
}
```

**效果**：高频 commit 场景下（每秒数十次 notify），inner lock 的获取从"每次唤醒"降为"每 3 秒超时一次"。

### 22.2 has_pending() 无锁快速路径

**问题**：`has_pending()` 被 `auto_flush`（每 500ms）、`flush_required`（每次 close/fsync）等多处调用，每次都获取 inner lock 仅为检查 `has_chunks()`。

**修复**：利用已有的 `write_gen` / `last_flushed_gen` 原子计数器（flush 成功后会同步两者），添加快速路径：当 `write_gen == last_flushed_gen` 时直接返回 false，跳过锁获取。

```rust
pub(crate) async fn has_pending(&self) -> bool {
    // ... error check ...
    let gen = self.shared.write_gen.load(Ordering::Acquire);
    let flushed = self.shared.last_flushed_gen.load(Ordering::Acquire);
    if gen == flushed {
        return false;  // 无锁快速返回
    }
    let guard = self.shared.inner.lock().await;
    guard.has_chunks()
}
```

**效果**：文件空闲时（已 flush 完成、无新写入），`auto_flush` 每 500ms 的轮询不再获取 inner lock。

### 22.3 overlay_dirty 缩短锁持有时间

**问题**：读路径 `overlay_dirty` 获取 inner lock 后遍历所有相关 slices 并执行数据复制（`copy_into`），锁持有期间写路径完全阻塞。对于跨多 slice 的大范围读取，持锁时间可达微秒到毫秒级。

**修复**：将操作拆分为两阶段：
1. 持锁阶段：仅 snapshot 相关 slice 的 `Arc` 引用（O(n) clone Arc）
2. 无锁阶段：遍历 snapshot，通过 ParkingMutex 逐个锁定 slice 进行数据复制

```rust
// 持锁：仅获取 Arc 引用
let slice_refs: Vec<Option<Vec<Arc<ParkingMutex<SliceState>>>>> = {
    let guard = self.shared.inner.lock().await;
    // ... collect Arc clones ...
};
// 无锁：数据复制不再阻塞 write 路径
for (span, slices_opt) in spans.iter().zip(slice_refs.iter()) { ... }
```

**效果**：inner lock 持有时间从"扫描 + 复制全部数据"缩短为"扫描 + clone Arc 指针"。写路径在读覆盖期间不再被长时间阻塞。

### 22.4 本轮修改文件

1. `src/vfs/io/writer.rs` — flush_with_deadline 安全网重构、has_pending 快速路径、overlay_dirty 锁拆分

### 22.5 优化总结

| 路径 | 优化前 | 优化后 |
|------|--------|--------|
| flush 安全网 | 每次 notify/timeout 获取 inner lock | 仅 3s 超时获取 |
| has_pending | 每次调用获取 inner lock | write_gen==last_flushed_gen 时无锁返回 |
| overlay_dirty | 持 inner lock 期间完成全部数据复制 | 持锁仅 snapshot Arc，数据复制在锁外 |

这三个优化共同减少了 inner lock 的竞争压力，尤其在 generic/074 的"高频 mmap 写 + 并发读验证"场景下，读写路径的相互阻塞大幅降低。

---

## 23. 第四轮优化：带宽竞争与背压调优

### 23.1 compaction 写入纳入全局上传信号量

**问题**：前台 flush 通过 `DataUploader` 使用全局 `UPLOAD_SEM`（256 permits）控制并发上传。但 compaction 的 `write_merged_data` 直接调用 `block_store.write_fresh_range()` 绕过信号量，在重度 compaction 期间可以不受限地发起 S3 写请求，与前台 flush 上传竞争带宽。

**修复**：在 `compactor.rs` 的 `write_merged_data` 中，每次写 block 前获取 `upload_permit()`。这使 compaction 和前台 flush 共享同一个并发池，前台繁忙时 compaction 自然退让。

```rust
// src/chunk/compact/compactor.rs
async fn write_merged_data(&self, slice_id: u64, data: &[u8]) -> ... {
    for span in spans {
        let _permit = upload_permit().await;  // 新增：共享信号量
        self.block_store.write_fresh_range(key, ...).await?;
    }
}
```

**新增公开函数**：`src/chunk/writer.rs` 中暴露 `pub(crate) async fn upload_permit()` 供 compact 模块调用。

**效果**：当前台有 200+ 并发上传时，compaction 的写入会被信号量排队，自动降速。空闲时信号量余量充足，compaction 不受影响。

### 23.2 back_pressure soft-limit 改为 yield

**问题**：当 `buffer_usage > soft_limit`（默认 300MB）时，每次 `write_at_inner` 固定 sleep 10ms。对于 4KB 的 mmap writeback 页面，单次 10ms 对应最多 ~400KB/s 的写入吞吐，严重限制 mmap writeback 速率。

**根因**：soft limit 的目的是给 flush/upload 任务运行机会来释放 buffer。固定 10ms sleep 过于保守——在 tokio 运行时中，yield 即可让出执行权给同优先级的 flush 任务。

**修复**：将 soft limit 路径的 `sleep(10ms)` 改为 `tokio::task::yield_now()`。Hard limit（2x soft）仍保留 100ms sleep 作为 OOM 防护。

```rust
// 修改前：
tokio::time::sleep(Duration::from_millis(10)).await;

// 修改后：
tokio::task::yield_now().await;
```

**效果**：soft limit 触发时，写入任务仅让出一次调度轮次（微秒级），而非固定等待 10ms。在 flush 任务能及时消耗 buffer 的正常场景下，mmap writeback 吞吐不再被人为限制。

### 23.3 本轮修改文件

1. `src/chunk/writer.rs` — 新增 `upload_permit()` 公开函数
2. `src/chunk/compact/compactor.rs` — `write_merged_data` 每次写 block 前获取信号量
3. `src/vfs/io/writer.rs` — `back_pressure` soft limit 从 sleep(10ms) 改为 yield_now()

### 23.4 综合影响

| 瓶颈 | 修改 | 预期效果 |
|------|------|----------|
| compaction 抢带宽 | 纳入 UPLOAD_SEM | 前台 flush 优先级自然提升 |
| back_pressure 过度限流 | yield 代替 sleep | mmap writeback 吞吐从 ~400KB/s 恢复到线速 |

两项改动共同确保：在 generic/074 的高频 mmap 写入 + fsync 场景下，前台 IO 获得更多 S3 带宽和 CPU 时间，测试整体耗时应有明显下降。
