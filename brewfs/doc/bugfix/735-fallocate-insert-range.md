# generic/735: FALLOC_FL_INSERT_RANGE (xfs_io finsert)

## 测试说明

`generic/735` 使用 `xfs_io -c "finsert <offset> <length>"` 测试在文件中间插入空洞（不改变文件大小，将插入点之后的数据向后移动）。

## 现象

```
generic/735  [not run] xfs_io finsert failed (old kernel/wrong fs?)
```

xfstests 框架判定为 `[not run]`（非失败），因为 `xfs_io finsert` 返回了错误。

## 根因

`xfs_io finsert` 调用 `fallocate(fd, FALLOC_FL_INSERT_RANGE, offset, length)`。brewfs 的 fallocate 实现（`fuse/mod.rs`, `vfs/fs/mod.rs`）只支持 `mode == 0`（普通空间分配）：

```rust
// fuse/mod.rs
async fn fallocate(&self, ...) -> FuseResult<()> {
    if mode != 0 {
        return Err(libc::EOPNOTSUPP.into());
    }
    self.fallocate_ino(inode as i64, offset, length).await...
}
```

`FALLOC_FL_INSERT_RANGE` 需要：
1. 在文件中间创建空洞
2. 将空洞之后的所有数据向后移动 `length` 字节
3. 文件大小不变

这涉及元数据层的数据块重映射，对于基于对象存储的 brewfs 来说非常复杂——每个 chunk 的 slice 偏移需要重新计算。

## 是否可修

可修但工程量大。需要：
1. 在 `fallocate_ino` 中识别 `FALLOC_FL_INSERT_RANGE`
2. 读取插入点之后的所有 slices
3. 重写 slices 到新的偏移位置
4. 清理原始位置的数据
5. 更新元数据

对于 737 个 generic 测试而言，仅此 1 个 case 需要此功能。

## 处理

加入 `xfstests_slayer.exclude`。xfstests 框架将其标记为 `[not run]` 而非 `[failed]`。
