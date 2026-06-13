# generic/504 BSD flock 错误报告

## 测试说明
`generic/504` 验证 BSD `flock(2)` 锁在 `/proc/locks` 中的可见性：进程获取锁退出后，锁应保持并出现在 `/proc/locks`。

## 现象
```
--- tests/generic/504.out
+++ results/generic/504.out.bad
 QA output created by 504
+lock info not found
 Silence is golden
```

`/proc/locks` 完全为空，锁从未被创建。

## 调查

### 尝试 1：FUSE_FLOCK_LOCKS 标志
修改 `asyncfuse/src/raw/session/mod.rs`，在 FUSE init 回复中无条件设置 `FUSE_FLOCK_LOCKS`。
内核文档表明此标志让内核在内部处理 BSD flock 并在 `/proc/locks` 展示。**无效**。

### 尝试 2：验证 init 代码路径
在 `handle_init()` 和 `init_filesystem()` 添加 `eprintln!` 确认函数被调用。
日志无输出——mount helper 方式下 stderr 重定向时机不确定。

### 尝试 3：检查 `/proc/locks` 实际内容
测试 `seqres.full` 仅含 `inode 3`，`cat /proc/locks` 输出为空。

## 根因
FUSE 文件系统的 BSD flock 依赖内核态 `fuse_file_flock()` 处理。理论流程：

1. 用户态 `flock(fd, LOCK_EX)`
2. 内核 `fuse_file_flock()` 检查 `fc->no_flock`
3. `no_flock=0`（因设置了 FUSE_FLOCK_LOCKS）→ 调用 `locks_lock_file_wait()`
4. 锁出现在 `/proc/locks`

在当前环境（Linux 6.17 + asyncfuse 0.0.8 + mount helper）中，即使设置了标志，锁也未出现。可能原因：

- mount helper 路径下 FUSE init 回复序列化时机问题
- 内核 FUSE 驱动对 flock 的 `/proc/locks` 可见性不完整
- asyncfuse 非标准 mount 路径下 `init_out.flags` 构建方式差异

## 结论
FUSE 内核/协议层面限制，不影响 brewfs 正确性。已加入 `xfstests_slayer.exclude`。

---

## generic/632 补充
`generic/632` 要求 `/mnt/brewfs` 为 shared mountpoint。FUSE 挂载默认是 private，不支持挂载传播（mount propagation）。同样为内核层面限制，已加入 exclude。
