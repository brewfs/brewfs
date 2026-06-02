# 写路径卡顿修复 Todo

本文根据 `doc/bugfix/write-path-stall-retrospective.md` 拆分后续工作。目标是把写路径从“无限等待、无限重试、前台强同步”收敛到“有预算、可观测、错误可传播”的行为。

## P0：停止伪死锁和错误吞没

- [x] 给 `ChunkHandle::write_at` 的 slice 竞争重试增加上限，避免在 `auto_flush`/`commit_chunk` 竞争下无让步忙等。
  - 代码：`src/vfs/io/writer.rs`
  - 当前策略：最多重试 `WRITE_SLICE_MAX_RETRIES` 次，超过后返回错误。
- [x] 上传失败不再由 `commit_chunk` 无限重新 `spawn_flush_slice`。
  - 代码：`src/vfs/io/writer.rs`
  - 当前策略：上传任务内部仍使用 `UPLOAD_MAX_RETRIES`，耗尽后记录 writeback error，`commit_chunk` 丢弃失败 front slice 并继续处理后续 slice。
- [x] `flush/fsync/close` 不再把失败 slice 当作成功。
  - 代码：`src/vfs/io/writer.rs`
  - 当前策略：后台上传或 metadata commit 失败会记录到 writer，后续 `flush_required` 能观察到错误。
- [x] 给 `commit_chunk` 的 metadata write 增加最大重试预算。
  - 代码：`src/vfs/io/writer.rs`
  - 当前策略：连续失败达到 `COMMIT_META_MAX_RETRIES` 后记录 writeback error，避免前台无限等待。
- [x] 把 writeback failure 从 `VfsError::Other` 改为保留根因的错误传播，避免 FUSE 层丢失 message。
  - 当前策略：VFS 写回路径用 `VfsError::Anyhow` 保留后台错误，FUSE 仍映射为 `EIO`。

## P1：降低前台同步成本

- [x] 改造 `VFS.read`，默认不再先执行 `writer.flush_if_exists`。
  - 代码：`src/vfs/fs/mod.rs`
  - 当前策略：读已提交快照，再叠加本地 dirty overlay；用 `recently_committed` 列表避免 overlay 窗口竞态。
- [x] 重新定义 close 语义，拆分 handle close、dirty data 可见性和持久化。
  - 代码：`src/vfs/fs/mod.rs`、`src/vfs/io/writer.rs`
  - 当前策略：close 使用 `CLOSE_FLUSH_DEADLINE = 5s` 短超时；FUSE 已在 release 前调用 flush()，close 只做残余收敛。
- [x] 给 `flush` 增加更短的前台 deadline 或按调用方区分 deadline。
  - 代码：`src/vfs/io/writer.rs`
  - 当前策略：`flush_with_deadline(deadline)` 参数化；close 用 5s，fsync/truncate 用 300s。

## P2：错误分类和事务退避

- [x] 拆分 `MetaError::ContinueRetry`。
  - 代码：`src/meta/store.rs`
  - 当前策略：`ContinueRetry(RetryReason)` 区分 VersionConflict、CompactConflict、TransactionConflict、LockContention。
- [ ] 让 `commit_chunk` 根据错误类型选择重试、快速失败或降级。
  - 当前状态：已有最大次数预算和 reason 日志，但还不能根据 reason 选择不同退避策略。
- [x] metadata write 遇到非 `ContinueRetry` 错误时快速失败。
  - 当前策略：`MetaError::ContinueRetry`、数据库 deadlock/locked/busy/serialization/timeout、部分瞬时 IO 错误会进入重试预算；永久错误直接记录 writeback failure。
- [ ] 为数据库/etcd/Redis backend 统一 rename、write、compact 冲突的条件事务错误语义。

## P3：可观测性和回归测试

- [x] 增加上传失败时 `flush` 返回错误的单元测试。
  - 代码：`src/vfs/io/writer.rs`
- [x] 增加 metadata commit 失败的单元测试。
  - 当前覆盖：非重试 metadata 错误会让 `flush` 返回 writeback failure。
- [x] 增加 `should_retry_meta_write` 分类正确性测试。
  - 代码：`src/vfs/io/writer.rs`
  - 覆盖所有 RetryReason 变体和非重试错误。
- [ ] 增加 metadata commit 连续 `ContinueRetry` 的单元测试。
  - 建议：用测试 MetaLayer 注入 `ContinueRetry`，验证超过预算后 `flush` 返回错误。
  - 阻塞：需要 mock MetaLayer 基础设施。
- [ ] 增加 close/fsync 错误传播测试。
  - 建议：覆盖 VFS 层，确认用户可见错误不会被 close 或 fsync 吞掉。
- [x] 增加慢 flush 日志字段。
  - 代码：`src/vfs/io/writer.rs`
  - 当前策略：flush timeout 时记录 ino、elapsed_ms、pending_slices 数量和各 slice 状态。
  - commit_chunk 重试时记录 retry_reason、retry_failures、backoff_ms。
- [ ] 在 xfstests 产物中自动抓取 `auto_flush: alive` 前后的前台等待状态。

## 验证清单

- [x] `cargo test -p brewfs vfs::io::writer`
- [x] `cargo test -p brewfs vfs::fs::tests`
- [ ] Redis backend 运行 `generic/001`
- [ ] Redis backend 运行 `generic/100`
- [ ] Redis backend 运行 `generic/127`
- [ ] 数据库 backend 注入 `ContinueRetry`，确认不会无限卡在 `commit_chunk`
