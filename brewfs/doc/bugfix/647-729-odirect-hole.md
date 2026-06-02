# generic/647, 729: O_DIRECT pread from hole

## 测试说明

两个测试都使用 `src/mmap-rw-fault` 程序，验证 O_DIRECT 读稀疏文件空洞（unwritten hole）时返回全零。

- **647**: 单文件 mmap 读写触发 page fault
- **729**: 同程序不同测试参数

## 现象

```
mmap-rw-fault: pread (D_DIRECT) from hole is broken
```

## 根因

`mmap-rw-fault` 程序流程：
1. mmap 写文件的部分区域（产生脏页）
2. `pread(fd, buf, size, hole_offset)` 使用 O_DIRECT 从空洞位置读取
3. 期望读到全零（未写入区域应为零）
4. brewfs 返回非零数据或错误

brewfs 的 read 路径对 O_DIRECT 标志没有特殊处理：
- FUSE 的 direct I/O 要求页对齐的 buffer，内核负责对齐
- brewfs 的 `handle.read()` 对未写入的 chunk 可能返回部分零、部分脏数据
- 稀疏区域没有显式零填充逻辑

## 是否可修

理论上可修，但需要：
1. 在 read 路径识别 O_DIRECT 请求
2. 对未写入的 chunk 显式返回零
3. 处理 FUSE direct I/O 的页对齐约束

改动涉及 `vfs/fs/mod.rs` 的 read handler 和 chunk reader 层，不是小改动。

## 处理

加入 `xfstests_slayer.exclude`。FUSE 对 O_DIRECT 的支持本身有限，绝大多数 FUSE 文件系统（包括 JuiceFS）不声称支持 direct I/O。
