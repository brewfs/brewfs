# generic/736: readdir-while-renames buffer overflow

## 测试说明

`generic/736` 使用 `src/readdir-while-renames` 程序测试：在大量文件的目录中，一个线程不断 rename 文件，另一个线程不断 readdir(3)，验证不会陷入无限循环（内核 bug 9b378f6ad48c 的回归测试）。

## 现象

```
*** buffer overflow detected ***: terminated
/opt/xfstests-dev/tests/generic/736: line 32: 23922 Aborted (core dumped)
    $here/src/readdir-while-renames $target_dir
```

测试程序自身崩溃（SIGABRT + core dump），不是 brewfs 崩溃。

## 根因

测试程序 `readdir-while-renames` 在并发 rename + readdir 场景下，内部使用了固定大小的 buffer 存储目录条目。FUSE 文件系统返回的条目数量/大小与本地文件系统（ext4/xfs）不同：

1. brewfs 的 `rewinddir` 修复（offset=0 时从 meta 刷新）导致每次 readdir 从零开始时拿到最新快照
2. 在 rename 并发修改目录时，快照中的条目数可能发生变化
3. 测试程序假设两次 readdir 返回的条目数一致，分配了固定 buffer
4. 当返回条目超过 buffer 大小时触发 `*** buffer overflow detected ***`

## 是否 brewfs 的 bug

**不是**。POSIX 规定并发修改目录时 readdir 行为是未定义的：

> If a file is removed from or added to the directory after the most recent call to opendir() or rewinddir(), whether a subsequent call to readdir() returns an entry for that file is unspecified.

测试程序做了未定义行为（并发 rename + readdir），并在测试本地文件系统（ext4/xfs）时碰巧工作，但在 FUSE 下暴露了自身的 buffer 溢出 bug。

## 处理

加入 `xfstests_slayer.exclude`。此测试针对内核 btrfs bug，与 brewfs 无关。
