# BrewFS 写路径 / 写回模块 Review

日期：2026-06-10  
范围：`src/vfs/io/writer.rs`、`src/vfs/cache/write_back.rs`、`src/vfs/cache/page.rs`、`WriteConfig` 与 `.stats` 写回指标。

## 模块现状摘要

当前写路径以 `FileWriter` 为核心：`write_at`/`write_at_cached` 把写请求按 chunk 拆分，追加到每个 chunk 的 append-only `SliceState`。slice 从 `Writable` 冻结为 `Readonly` 后由 `spawn_upload_task` 上传对象；安全模式是 `UploadBeforeCommit`，对象上传完成后再由 `commit_chunk` 写元数据；高吞吐模式是 `CommitBeforeUpload`，`commit_chunk` 可以在对象上传完成前先写元数据，把 slice 移入 `recently_committed`，依赖 dirty overlay 在本机继续服务读。

这个设计已经具备较强的流水线能力：slice 内按 block batch 并发上传，`try_commit` 缩短上传后到 metadata commit 的等待，`auto_flush` 周期性冻结老 slice，`recent_pending_upload_bytes` 用来给 `CommitBeforeUpload` 增加 backlog 背压。但现有状态机把“flush/close 成功”和“对象上传完成”拆开了，且 pending accounting、失败路径、small write 合并和全局上传限流还不够闭环。

需要特别结合当前 perf 现象来看：`randrw direct=0` 的 close/flush 拖尾，本质上不是单纯 metadata 慢，而是大量 metadata-visible、object-not-yet-uploaded 的 slice 在 close 后继续排队；`recent_pending` 残留说明 backlog 统计和清理还有失败/拖尾窗口；降低 pending 低水位能减少 close 拖尾，但会把等待前移到 write 路径导致 write p99 恶化；此前 metadata batch 方向已经失败，不能再把写回瓶颈误判为单纯 metadata 批处理问题。

## 具体问题 / 风险 / 性能瓶颈

### 1. P0：`CommitBeforeUpload` 下上传失败会让 `recent_pending_upload_bytes` 残留

- 位置：`src/vfs/io/writer.rs`，`SliceHandle::mark_failed` 约 580 行、`clear_recent_pending_if_complete` 约 546 行、`spawn_upload_task` 约 2009 行。
- 原因：`recent_pending_upload_bytes` 只在 `upload_complete()` 时扣减。若 slice 已 early commit 并被计入 pending，随后对象上传失败，`mark_failed` 会把状态改成 `Failed`、清空 `in_flight`，但不会扣减 `recent_pending_accounted`。由于 failed slice 通常 `has_idle_block()` 仍可能为真，`upload_complete()` 不成立，pending 可能永久残留，并持续触发 backpressure 或污染 `.stats`。
- 建议改法：在 `mark_failed` 中调用一个统一的 `clear_recent_pending_accounting(reason)`，无论成功、失败、丢弃、`clear()` 都保证只扣一次；把 `recent_pending_accounted` 和 accounted bytes 绑定，避免用当前 `alloc_bytes()` 做后验扣减。
- 验证方式：新增单测：`CommitBeforeUpload` + blocking/failing store，flush 返回后 pending > 0，然后上传失败，断言 `recent_pending_upload_bytes == 0` 且后续 write 不再因 pending gate 卡死。再跑 `fio-randrw direct=0`，确认 close 后 pending 不再长期残留。

### 2. P1：flush/close 成功语义在 `CommitBeforeUpload` 下只等 metadata，不等对象上传

- 位置：`writer.rs`，`flush_with_deadline` 约 1588-1718 行、`commit_chunk` early commit 约 2217-2294 行、`flush_for_close` 约 2987 行；上层 `src/vfs/fs/mod.rs::close` 约 2669-2733 行。
- 原因：`flush_with_deadline` 等待捕获的 slice 状态到 `Committed`。在 `CommitBeforeUpload` 下，`Committed` 可以发生在对象上传前，之后 slice 被移入 `recently_committed`，`has_pending()` 也会认为 live pending 已清空。`close()` 再调用 `flush_for_close()` 时看不到这些 committed-but-uploading slice，于是 close 可以很快返回，但后台仍有大量 S3 PUT 拖尾。
- 建议改法：明确两套语义并用配置暴露：高吞吐 close 只保证本机 overlay + metadata；强语义 `fsync/close` 可选择等待 `recent_pending_upload_bytes` 降到 0 或低水位。至少应在 `.stats` 和日志里把 close 返回时的 pending bytes 打出来，避免 benchmark 把尾部上传当成“消失”。
- 验证方式：构造 blocked object store 单测，分别验证 `flush()` 快速返回、`fsync_strict` 或新配置 close 等待上传完成。perf 用相同 `fio-randrw direct=0` 比较 close 后 pending drain 时间、write p99、总体吞吐。

### 3. P1：低水位背压是单次 sleep / hard wait，容易在“减少拖尾”和“write p99 恶化”之间震荡

- 位置：`writer.rs`，`decide_writeback_backpressure` 约 86 行、`wait_for_writeback_backpressure` 约 1261-1294 行；`WriteConfig` 约 75-78 行。
- 原因：soft 区间只 sleep 1-6ms 后直接放行，不再次检查 backlog；hard 区间无限等 notify。低水位越小，close 拖尾越少，但 write path 进入 hard wait 的概率越高，符合已知现象“低水位减少拖尾但 write p99 恶化”。当前没有 per-writer wait 次数、wait 时长、soft/hard hit 指标，调参基本靠 fio 结果倒推。
- 建议改法：改成 hysteresis admission control：超过 high watermark 停，低于 low watermark 放；soft 区间采用 token/速率估计而不是固定 sleep。增加 `writeback_backpressure_wait_ops/us`、`soft_sleep_ops/us`、`hard_wait_ops/us`。
- 验证方式：用 3 组水位跑 `fio-seqwrite` 和 `fio-randrw direct=0`，要求 pending 峰值降低时 write p99 不超过基线 25%；同时检查新增 wait 指标与 p99 spike 对齐。

### 4. P1：`recent_pending` 统计使用 slice 级 bool + 当前 `alloc_bytes()`，可观测值不够精确

- 位置：`writer.rs`，`recent_pending_accounted` 字段约 229 行、`account_recent_pending_if_needed` 约 2061 行、`dirty_breakdown` 约 2821 行。
- 原因：计入和扣减都取 `state.data.alloc_bytes()`，这是内存页分配量，不是实际上传剩余量，也不是对象字节数。partial block、64KiB page 分配、未来 page release 或 SSD staging 都会让该数值偏离真正的 not-yet-uploaded bytes。stats-only staged 方案失败也说明仅加派生字段但状态机不闭环没有帮助。
- 建议改法：在计入时保存 `recent_pending_accounted_bytes`，并区分 `logical_len`、`allocated_bytes`、`remaining_upload_bytes` 三类指标。backpressure 应基于 remaining upload bytes 或上传队列成本，memory pressure 应基于 allocated bytes。
- 验证方式：新增 partial block、小 page、failed upload、release cleanup 单测，断言三类指标分别符合预期。perf 中采集 `brewfs_writeback_recent_pending_upload_bytes` 与 `brewfs_s3_put_bytes_total` 的差值趋势。

### 5. P1：`CommitBeforeUpload` 把跨客户端可见性建立在对象尚不存在的 metadata 上

- 位置：`src/vfs/cache/config.rs::WriteBackMode` 约 14-18 行、`writer.rs::can_overlay_read` 约 328 行、`DataWriter::overlay_dirty_if_exists` 约 2881 行。
- 原因：本机读可以通过 dirty overlay 补齐，但其他客户端只看到 metadata slice，可能在对象还没上传时读到缺失对象或 EIO。`WriteBackMode` 注释已经承认风险，但 `flush/close` 成功语义没有把这种弱一致边界显式返回给调用者。
- 建议改法：把 `CommitBeforeUpload` 标为弱 close-to-open 模式，并提供强语义开关：跨客户端可见前必须上传完成，或让 reader 遇到 missing object 时通过 metadata 的 dirty/staging 标记等待/重试。
- 验证方式：双挂载集成测试：客户端 A 写入并 close，客户端 B 立即 open/read；分别在 weak/strict 模式下验证行为，weak 模式至少不能产生永久 EIO。

### 6. P0：本地 SSD persist 不可靠，early commit 前可能没有完整 durable staging

- 位置：`writer.rs::spawn_upload_task` 约 1887-1941 行、`write_back.rs::persist_slice` 约 103-150 行。
- 原因：上传 subtask 同时跑 `persist_slice` 和 S3 upload；persist 失败只 debug 记录“SSD persist skipped”。更严重的是 pipeline 可能把同一 slice 拆成多个 upload batch，而 dirty key 只包含 `slice_id`，`FsWriteBackCache::persist_slice` 会反复写同一个 `{local_seq}.slice`。后一个 batch 可能覆盖前一个 batch，`.meta` 也只记录最后一次 `chunk_offset/length`。在 `CommitBeforeUpload` 模式中，metadata 可能已经提交，若随后进程崩溃或对象上传失败，本地 staging 未必能恢复完整 slice。
- 建议改法：在 `CommitBeforeUpload` 中把 persist 从 best-effort 提升为 admission 前置条件：冻结后先把完整 logical slice durable stage 一次，成功后才允许 early metadata commit；persist 失败时降级为 `UploadBeforeCommit` 或返回错误。若保留分批 persist，dirty key 必须扩展为 `(slice_id, batch_offset)`，recovery/overlay 按 range 合并。
- 验证方式：注入 persist 失败 + blocked upload，断言不会 early commit；构造多 batch slice，断言 dirty staging 文件覆盖完整 range；崩溃恢复测试中扫描 dirty records 后能恢复上传。

### 7. P1：`randrw direct=0` 小写会放大 slice/object/metadata 数量

- 位置：`writer.rs::find_slice_or_create` 约 842-956 行、`SliceState::can_write` 约 296-314 行、`CacheSlice::collect_pages` 约 261-308 行。
- 原因：direct=0 通过内核 page cache/writeback 往往产生较小、乱序、重叠写。当前 slice 只能 append 或在未上传区间内 overlap，随机 offset 很容易新建 slice。每个小 slice 都需要 slice id、对象 PUT、metadata write、dirty overlay 生命周期管理；metadata batch 方案失败后，应优先减少 slice 数和 PUT 数，而不是继续堆 metadata 批处理。
- 建议改法：增加 per-chunk small-write coalescer：对 4KiB/64KiB dirty page 先进入 chunk-local dirty map，按短窗口或达到阈值后合并成较大的 contiguous/extent slice；对重叠写按 FUSE unique 保留 last-writer-wins。
- 验证方式：给 `fio-randrw direct=0 bs=4k/64k/4m` 记录 `slice_count`、`s3_put_ops`、`meta_txn_ops`、write p99；目标是小写场景 PUT/meta 数明显下降，4MiB 场景无回退。

### 8. P2：upload 并发已接入 `upload_concurrency`，但仍缺全局优先级调度

- 位置：`src/vfs/cache/config.rs` 约 41-43 行定义 `upload_concurrency`；`writer.rs::Shared::upload_limit` 与 `chunk/writer.rs::DataUploader` 的 local upload limit。
- 原因：当前代码已经把 `CacheConfig::upload_concurrency` 贯通到每个 writer 的上传 semaphore，并有单测验证 local limit 生效；这修正了“配置存在但不生效”的问题。但该限制仍是 per-writer 维度，和前台读、后台预取、writeback upload 之间没有统一优先级，也没有全局 S3 PUT/GET in-flight 预算。低并发实测会降低 randrw 吞吐，高并发又可能抢读请求连接池。
- 建议改法：保留 per-writer `upload_concurrency` 作为局部保护，再在 object store 或调度层增加全局 GET/PUT semaphore，区分 foreground read、writeback upload、background prefetch 的优先级。`CommitBeforeUpload` 可用低优先级后台 token，flush/strict close 可临时提权。
- 验证方式：压测 `fio-randrw` 同时采集 S3 PUT/GET in-flight、GET p99、write p99、pending drain。已知 `upload_concurrency=2` 会回退，默认 32 未见明显回退；下一轮要验证全局优先级是否能降 read tail。

### 9. P2：`auto_flush` 10ms 全量扫描 + 随机半区策略会制造抖动

- 位置：`writer.rs::auto_flush` 约 2585-2747 行。
- 原因：后台任务每 10ms 取一次 writer lock，遍历所有 chunk/slice，并用随机 half 处理 too_many slices。slice 多时这会增加锁竞争和不可重复的尾延迟；slice 少时 500ms age 又可能让 close 承担最后一段上传。
- 建议改法：维护按 deadline 排序的 flush heap/queue；写入或 slice 状态变化时更新下次 wakeup，而不是固定 10ms 扫描。too_many 时按 oldest/bytes 明确排序，不用随机。
- 验证方式：新增 tracing 统计 auto_flush scan duration、frozen slice 数、lock wait；对比 randrw write p99 和 flush tail 的波动。

### 10. P2：`write_back.rs` 的 metadata record 写入不是原子 durable

- 位置：`write_back.rs::persist_slice` 约 115-148 行、`write_meta` 约 83-91 行、`recover` 约 171-210 行。
- 原因：slice 数据文件使用 tmp + rename + fsync dir，但 `.meta` 直接 `fs::write`，没有 tmp rename，也没有 fsync meta 文件和父目录。崩溃可能留下有 slice 无 meta、meta 半写或状态落后；recover 只扫描 meta，数据孤儿不会被恢复。
- 建议改法：meta 也使用 tmp + flush/sync + rename + fsync dir；recover 增加 orphan slice 清理/告警；`mark_state` 同样做原子更新。
- 验证方式：用故障注入在 data rename 后、meta write 中、meta rename 后 crash，验证 recover 能恢复或安全清理，不产生 metadata-visible missing object。

### 11. P2：`flush_inode` 是 best-effort，rename/copy 等路径可能吞掉写回错误

- 位置：`src/vfs/fs/mod.rs::flush_inode` 约 2781-2785 行；`writer.rs::flush_if_exists` 约 2872-2878 行。
- 原因：`flush_if_exists` 丢弃 `writer.flush()` 错误，`flush_inode` 也无返回值。对于 rename 等 metadata 操作，这可能让写回错误被后续元数据操作覆盖，用户只看到成功的 rename，却不知道此前 dirty data 失败。
- 建议改法：把需要一致性的 metadata 操作切到 `flush_required` 并传播错误；保留 best-effort 版本只给明确不要求强语义的后台维护任务。
- 验证方式：注入 object upload failure 后执行 rename/flush_inode 路径，断言用户操作返回错误，且 handle 后续 close/fsync 也能观察到 async writeback error。

### 12. P2：现有 `.stats` 不能定位 close/flush 拖尾的来源

- 位置：`src/vfs/stats.rs::sync_writeback_dirty_breakdown` 约 516-527 行、Prometheus 输出约 887-900 行；`DataWriter::dirty_breakdown` 约 2821-2854 行。
- 原因：现在只有 dirty/live/recent_pending/recent_uploaded 字节，没有 slice 数、object 数、oldest age、backpressure 等待、flush/close drain 时间。面对“recent_pending 残留”和“低水位 p99 恶化”，只能从 fio 延迟和单点 stats 推断。
- 建议改法：增加 `writeback_recent_pending_slices`、`oldest_pending_age_ms`、`upload_inflight_batches`、`flush_wait_slices`、`close_pending_bytes_at_return`、`backpressure_wait_us`。
- 验证方式：perf runner 在每个 fio 前后保存 stats，并在 report 中列出 pending age 和 wait us；要求每次调参都能解释 p99 spike 属于 write admission、metadata commit 还是 S3 PUT drain。

## 最值得优先尝试的 3 个优化方案

### A. 修正 pending accounting，并做 hysteresis 背压

- 预期收益：直接解决 `recent_pending` 残留；把“减少 close 拖尾”从粗暴低水位变成可控的 high/low watermark，降低 write p99 被硬等待打爆的概率。
- 具体方向：保存 accounted bytes；成功、失败、清理统一扣减；增加 low/high watermark 和 wait 指标；默认只在 `CommitBeforeUpload` 生效。
- 回退风险：如果水位过低，seqwrite/randrw write p99 会回退；应保留 env 开关，默认先使用保守水位。

### B. 增加 direct=0 小写合并层，减少 slice/PUT/meta 放大

- 预期收益：`randrw direct=0` 的小写会从“很多小 slice + 很多 PUT/meta”变成 per-chunk 合并提交，预计改善 write p99、close 拖尾和 S3 PUT ops。
- 具体方向：chunk 内 dirty page map + 5-20ms 或 size threshold 合并；按 FUSE unique 保序；flush/fsync/close 强制 seal。
- 回退风险：状态机复杂度增加，最容易出错的是 overlap last-writer-wins、truncate epoch 和 mmap writeback；建议先 behind feature flag，只对小于 1MiB 的 cached writes 启用。

### C. 明确 close/fsync 语义，给强一致 drain 一个可配置路径

- 预期收益：把 benchmark 和用户语义从“close 返回但后台还有大量上传”中解耦；强语义模式能消除跨客户端 missing object 风险，弱语义模式继续服务高吞吐。
- 具体方向：保留 `CommitBeforeUpload` 的快速 flush，但新增 strict close/fsync 等待 pending drain 到 0 或 low watermark；同时将本地 persist 成功作为 early commit 前置条件。
- 回退风险：严格等待会降低 seqwrite 吞吐并抬高 close latency；需要通过配置和文档让用户显式选择。

## 并行 agent 补充审查

### P0 补充：`upload_complete()` 可能把失败 upload 误判为完成

- 位置：`SliceState::upload_complete`、`has_idle_block`、`mark_failed`、`clear_recent_pending_if_complete`、`auto_flush` 清理 `recently_committed` 的 retain 分支。
- 发现：`prepare_upload` 推进的是 dispatched frontier；失败时 `mark_failed` 只清 `in_flight`，没有回滚 dispatched，也没有记录“已成功上传”的独立边界。若整段已 dispatched 但 PUT 失败，`upload_complete()` 可能因为“无 idle block + 无 in_flight”返回 true，导致 failed-but-committed slice 被 grace 清理，本机 overlay 消失，`recent_pending` 也可能无法可信扣减。
- 改动建议：把 dispatched 与 uploaded-success frontier 拆开。`upload_complete()` 必须基于成功上传的 block/range；失败的 committed-but-not-uploaded slice 不应进入 completed cleanup。新增 `recent_pending_accounted_bytes`，成功、失败、clear、drop recently_committed 都走统一扣减函数，只扣一次。
- 验证：mock object store 在指定 block PUT 失败；`CommitBeforeUpload` 已 early commit 后触发失败，断言 overlay 不被误清、pending accounting 归零或进入可解释错误态，后续 `fsync/close` 可观察到 writeback error。

### P1 补充：early commit 后后台 upload 缺 watchdog

- 位置：`COMMIT_UPLOAD_MAX_WAIT`、`commit_chunk` early commit 分支、`spawn_upload_task` 的 upload future。
- 发现：`COMMIT_UPLOAD_MAX_WAIT` 只保护 upload-before-commit 路径。`CommitBeforeUpload` 一旦把 slice 移入 `recently_committed`，后台 upload 如果 hang 住，pending/overlay 可能长期残留，且不一定触发 durable writeback error。
- 改动建议：给每个 upload batch 加 timeout，或给 `recently_committed` pending slice 增加 watchdog 扫描。超时后记录可查询的 writeback error、唤醒 pending gate，并保留 overlay/SSD staging 直到恢复或明确丢弃。
- 验证：blocking store + early commit，确认 watchdog 后 `.stats` 有 oldest pending age/error，write admission 不会永久等待，严格 close/fsync 能返回错误或超时。

### P2 补充：异步写路径里同步 `yield_now` 和随机 auto-flush 会放大抖动

- 位置：`ChunkHandle::write_at` 的 retry/yield；`auto_flush` 10ms 扫描和随机 half flush。
- 发现：同步 `std::thread::yield_now()` 运行在 Tokio worker 上，不能让异步任务公平推进；auto-flush 的固定 10ms 全量扫描与随机 half flush 会让 perf 重现性变差。
- 改动建议：失败重试释放 `inner` lock 后使用 `tokio::task::yield_now().await`，或将冻结竞态转为一次性新建 slice。auto-flush 先补 scan duration、frozen count、lock wait 指标，再考虑 deadline heap 或 oldest-first 队列。
- 验证：高并发 randwrite/randrw 记录 writer lock wait、auto_flush scan us、frozen slice count；确认 tail spike 能被指标解释。

## 核心结论

当前写回瓶颈的主因不是 metadata batch 不够，而是 `CommitBeforeUpload` 把可见性提前到 metadata，导致大量 committed-but-not-uploaded slice 进入后台上传拖尾。并行审查后需要把两个正确性风险排到性能优化之前：本地 durable staging 不能被分批覆盖，`upload_complete()` 不能用 dispatched frontier 误判成功。`recent_pending` 是正确的观测方向，但 accounting、失败清理和 watchdog 还不闭环；低水位背压能压缩 close tail，却会把等待转移到 write path，解释了 write p99 恶化。优先级最高的是先让 pending accounting 和 staging 可信，再做有 hysteresis 的背压；随后针对 `direct=0 randrw` 做小写合并，减少 slice/PUT/meta 放大。
