# xfstests generic/013 -> generic/014 顺序卡住分析

## 背景

`docker/compose-xfstests` 中观察到一个容易误判的问题：

- 单独运行 `generic/014` 可以通过。
- 按顺序运行早期 generic 用例时，常见表现是跑完 `generic/013` 后卡在 `generic/014`。
- 进一步复现后，问题有时会更早暴露为 `generic/013` 自身卡住。

这说明 `generic/014` 不是唯一触发点。更准确的判断是：前序用例，尤其是 `generic/013` 的 fsstress 和 sync 路径，会留下未正确收敛的 FUSE/writeback 状态；后续 `generic/014` 的 truncate 压力只是更容易把这个状态放大成卡死。

## 相关 artifact

### 单跑 generic/014 通过

`docker/compose-xfstests/artifacts/run-1779032064-29722`

结果：

```text
generic/014 35
Ran: generic/014
Passed all 1 tests
```

这说明 `generic/014` 在干净 mount/session 下可以完成。

### 顺序跑时 generic/014 卡住

`docker/compose-xfstests/artifacts/run-1779032172-30220`

结果：

```text
generic/001 23
generic/002 1
generic/005 1
generic/006 26
generic/007 64
generic/011 34
generic/013 64
Ran: generic/001 ... generic/014
Interrupted!
```

`check.console.log` 停在：

```text
generic/014
```

`brewfs.log` 显示进入 `generic/014` 前有大量 write-back recovery：

```text
recovered dirty slices from previous session, count: 5370
re-uploading recovered slice ...
recovery commit success ...
recovery metadata commit failed ... NotFound(...)
```

随后进入 014 后只看到：

```text
fuse.lookup ENOENT, parent: 1, name: truncfile.5935.0
MetaClient: create_file operation for (1, 'truncfile.5935.0')
```

之后只剩 `auto_flush: alive` 心跳，没有继续前台 FUSE 操作。

### 当前复现中 generic/013 自身卡住

`docker/compose-xfstests/artifacts/run-1779033626-8029`

命令：

```bash
timeout 900 bash docker/compose-xfstests/run_redis_xfstests.sh \
  --cases 'generic/001 generic/002 generic/003 generic/004 generic/005 generic/006 generic/007 generic/008 generic/009 generic/010 generic/011 generic/012 generic/013 generic/014'
```

卡点：

```text
generic/013
```

容器内进程显示：

```text
fsstress ... D wait_sb_inodes
```

`/proc/<pid>/stack`：

```text
wait_sb_inodes
sync_inodes_sb
sync_inodes_one_sb
iterate_supers
ksys_sync
__do_sys_sync
```

这说明 `generic/013` 的 fsstress 子进程已经进入 `sync()`，但内核仍在等待 superblock inode writeback 完成。对 FUSE writeback-cache 文件系统来说，这通常表示某些脏 inode 没有被正确清理或某些写回请求没有完成。

## generic/013 和 generic/014 覆盖内容

### generic/013

`tests/scripts/xfstests-prebuilt/xfstests/tests/generic/013`

核心内容：

```bash
count=1000
procs=20

_do_test 1 "-r" $count
_do_test 2 "-p $procs -r" $count
_do_test 3 "-p 4 -z -f rmdir=10 -f link=10 -f creat=10 -f mkdir=10 -f rename=30 -f stat=30 -f unlink=30 -f truncate=20" $count
```

每轮 `_do_test` 后都会执行 `_check_test_fs`。在当前复现中，卡住发生在第一轮 `fsstress -r -n 1000` 后的 `sync()` 路径。

这个用例覆盖：

- 大量 create/unlink/mkdir/rmdir/rename/truncate 混合操作。
- FUSE writeback-cache 下的脏页写回收敛。
- close/flush/fsync/sync 语义。
- 多 inode、多目录、多短生命周期文件的后台写回清理。

### generic/014

`tests/scripts/xfstests-prebuilt/xfstests/tests/generic/014`

核心内容：

```bash
$here/src/truncfile -c 10000 $TEST_DIR/truncfile.$$.0
```

这个用例覆盖：

- 密集 truncate。
- sparse file 读写语义。
- truncate 和 dirty writeback 的同步关系。
- inode size、page cache、metadata size 的一致性。

单跑 014 能通过，说明干净状态下 truncate 主路径基本可用；顺序跑卡住说明前序用例留下的 writeback 状态会污染后续 truncate 用例。

## 已发现的问题点

### 1. asyncfuse max_background 协商不一致

`asyncfuse::Session::with_workers(_, max_background)` 配置了用户态 worker/backpressure，但 FUSE INIT 回复仍固定：

```rust
max_background: DEFAULT_MAX_BACKGROUND // 12
```

在 writeback-cache 和 fsstress 场景下，内核最多只投递 12 个后台请求，容易被等待写回或 flush 的请求占满，后续能推进状态的请求进不来。

已尝试修复：

- FUSE INIT 中使用 session 的 `self.max_background`。
- BrewFS 默认 `DEFAULT_FUSE_MAX_BACKGROUND` 从 12 提到 128。

效果：

- 单跑 `generic/014` 仍通过。
- 但顺序跑目前仍可能卡在 `generic/013`，说明 max_background 不是唯一根因。

### 2. write-back cache dirty record 清理 key 不一致

上传前持久化 dirty slice 时使用：

```rust
local_seq: wb.next_seq()
```

提交成功后清理时使用：

```rust
local_seq: desc.slice_id
```

两者通常不相等，因此 `.slice/.meta` 文件没有被清理。后续 remount 或新 session 会把已经提交过的 slice 当成未完成 dirty slice recovery，导致大量：

```text
recovered dirty slices from previous session
re-uploading recovered slice
```

已尝试修复：

- 上传前先分配最终 `slice_id`。
- dirty record 的 `local_seq` 使用 `slice_id`。
- `persist_slice` 记录正确的 `chunk_offset`，避免部分上传重试后的 recovery 偏移错误。

状态：

- `cargo check` 已通过。
- 顺序回归仍需要继续验证，目前复现先卡在 `generic/013` 的 `sync()`，需要进一步确认是否还有 FUSE writeback 完成/清脏页问题。

### 3. generic/013 sync 卡住是当前主要阻塞点

当前最强证据来自容器内 `fsstress`：

```text
D wait_sb_inodes
```

内核栈：

```text
wait_sb_inodes
sync_inodes_sb
ksys_sync
```

这意味着用户态 BrewFS 可能已经没有明显前台日志，但内核仍认为 FUSE superblock 上存在未完成 inode writeback。需要继续调查：

- 是否存在 FUSE WRITE/FLUSH/FSYNC reply 丢失或未返回。
- 是否有 writeback-cache 脏页在 release/flush 后没有完成清理。
- 是否 `FileWriter::flush()` 返回了，但内核没有收到对应写回完成。
- 是否 asyncfuse worker/reply 队列在某种顺序下停住。

## 为什么 013 和 014 都应该放进回归集合

只跑 `generic/014` 不够，因为单跑通过不能覆盖前序污染。

必须覆盖三种组合：

1. `generic/014`
   - 快速验证 truncate 主路径。
2. `generic/013`
   - 验证 fsstress 后 `sync()` 和 writeback 收敛。
3. `generic/013 generic/014`
   - 验证 fsstress 后的残留状态不会污染后续 truncate。

更完整的早期 gate 可以跑：

```text
generic/001 generic/002 generic/003 generic/004 generic/005 generic/006 generic/007 generic/008 generic/009 generic/010 generic/011 generic/012 generic/013 generic/014
```

这与当前复现路径一致，能覆盖顺序污染问题。

## 建议新增测试集合

建议在 `docker/compose-xfstests` 层增加命名集合，供本地和 CI 复用：

```text
writeback-sync:
  generic/013

truncate:
  generic/014

writeback-truncate-sequence:
  generic/013 generic/014

early-generic-gate:
  generic/001 generic/002 generic/003 generic/004 generic/005 generic/006 generic/007 generic/008 generic/009 generic/010 generic/011 generic/012 generic/013 generic/014
```

短期最重要的是 `writeback-truncate-sequence`。它比单跑 014 更能防止当前问题回归。

## 下一步调试建议

1. 对 `generic/013` 开启 FUSE op log，定位最后一个未返回的 opcode。
2. 在 `flush/fsync/release/write_cached_ino` 加 request-level tracing，记录开始和完成。
3. 在 asyncfuse reply path 记录 reply unique、opcode、errno、写入 `/dev/fuse` 结果。
4. 在 `FileWriter::flush()` 成功返回时记录 pending slice 数、chunk 数、writeback error 状态。
5. 在 `sync()` 卡住时抓：

```bash
ps -eo pid,ppid,stat,wchan:32,comm,args
cat /proc/<fsstress-pid>/stack
cat /proc/<fsstress-pid>/syscall
```

6. 修复后必须至少验证：

```bash
bash docker/compose-xfstests/run_redis_xfstests.sh --cases "generic/013"
bash docker/compose-xfstests/run_redis_xfstests.sh --cases "generic/014"
bash docker/compose-xfstests/run_redis_xfstests.sh --cases "generic/013 generic/014"
```

## 当前结论

`generic/014` 单跑通过不代表顺序路径安全。当前问题的核心更接近 FUSE writeback/sync 收敛问题，`generic/013` 是主要触发器，`generic/014` 是后续放大器。

因此：

- `generic/013` 必须进入 BrewFS 的回归测试集合。
- `generic/014` 必须进入 BrewFS 的回归测试集合。
- `generic/013 generic/014` 的顺序组合也必须作为独立回归集合保留。
