# VFS/FUSE 与 POSIX 语义差异

## 对照对象

- BrewFS：`src/vfs/*`、`src/fuse/*`、`src/posix.rs`
- JuiceFS：`juicefs/pkg/vfs/*`、`juicefs/pkg/fuse/*`

## 总体判断

BrewFS 的 VFS 结构已经比较完整：有 inode runtime、handle registry、reader/writer、append locks、modified tracker、background tasks，并通过 asyncfuse 暴露 FUSE。差距主要不在“有没有函数”，而在 POSIX 边界是否被完整定义、跨客户端是否一致、错误是否按系统调用语义传播。

## 已有能力

BrewFS 当前 VFS/Meta 层已覆盖或正在覆盖：

- lookup/stat/readdir
- open/create/read/write/flush/fsync/release
- mkdir/rmdir/unlink/rename
- truncate/set_attr
- link/symlink/readlink
- xattr 接口
- flock/plock 接口
- fallocate 相关接口
- FUSE worker pool 与 max background 配置

## 核心语义 gap

### 1. close-to-open 需要明确成硬保证或软保证

JuiceFS 对 close-to-open、缓存例外、open-cache 等边界有长期实践。BrewFS 文档中也提到跨客户端缓存一致性仍是弱项。

建议定义最小目标：

```text
客户端 A:
  write -> fsync/close 成功

客户端 B:
  open 同一路径成功后 read

结果:
  必须看到 A 已成功 fsync/close 的数据，除非挂载配置显式启用弱一致缓存。
```

落地需要：

- open 时绕过或校验 attr cache。
- chunk/slice version 参与 reader cache invalidation。
- rename/unlink/truncate/setattr 后发布失效。
- Redis/SQL/Etcd 都有一致的失效策略。

### 2. fsync/flush/close 的成功条件必须收紧

对象存储文件系统最容易出错的是“用户以为落盘，实际只是进了后台队列”。BrewFS 写路径有异步上传和 commit，因此必须规定：

- `flush` 返回成功是否要求所有 dirty slice 上传完成？
- `fsync` 返回成功是否要求对象数据与元数据都已持久化？
- `close` 是否返回后台错误？
- 多次 write 中任意一次异步失败，后续 fsync 是否必定返回错误？

建议：

- `fsync` 必须等待并检查 Uploaded + Committed。
- `flush` 至少在 FUSE flush 语义下传播已知写错误。
- `release/close` 不应静默吞掉 dirty write 失败。
- 对每个 file handle 保存 first async write error，直到用户观察到。

### 3. rename 原子替换语义要统一

POSIX `rename(old, new)` 在 `new` 是普通文件时应原子替换。BrewFS 历史 bug 文档已提到相关风险。JuiceFS `Rename` 接口还支持 flags，包括 no-replace/exchange/whiteout 等语义。

提升方向：

- `rename` 默认支持原子覆盖普通文件。
- `rename_exchange` 和 no-replace 明确通过 flag 或独立接口暴露。
- 不同 MetaStore 后端必须同测。
- atomic-save 模式加入回归：写 temp、fsync temp、rename target、fsync parent。

### 4. truncate/write 需要 per-inode 线性化

BrewFS 当前 writer 有 per-handle gate、append locks 和 flush 等机制，但 truncate 与 in-flight write 的全局顺序必须非常明确。

建议目标：

- 同一 inode 的 size-changing operation 必须进入同一个序列化通道。
- truncate 前先 drain 或标记 epoch；旧 epoch slice 不得 commit。
- 已返回成功的 write 不应被后续 truncate 的内部清理静默丢失，除非从全局顺序看 truncate 在其后。
- 并发 truncate/write 加入 stress 测试。

### 5. 权限检查路径需要集中化

JuiceFS `Access`、`Open`、`CheckSetAttr`、ACL 等在 meta 层形成统一入口。BrewFS 目前权限能力正在补，但需要避免每个 VFS 操作自行判断。

提升方向：

- 抽象 `PermissionEngine` 或集中在 `MetaClient`。
- 所有 create/mkdir/open/link/rename/unlink/setattr 统一调用。
- root squash/all squash、umask、setuid/setgid/sticky bit 行为明确。
- 不支持 ACL 时清楚返回 `ENOTSUP` 或按基础 mode 处理。

### 6. readdir 与 dentry cache 行为需要固定

JuiceFS 有 readdir cache、readdir plus、entry timeout 等配置。BrewFS 有 batch prefetch 和 dir handle，但需要明确：

- opendir 后目录快照是否固定？
- rewinddir 是否刷新？
- 并发 rename/unlink/create 下行为边界是什么？
- readdirplus 是否支持，attr cache 如何失效？

建议对齐 FUSE 常见实践：默认允许一定缓存，但在 xfstests 相关场景下通过 TTL/显式刷新满足期望。

## FUSE 层差异

BrewFS 使用 `asyncfuse`，FUSE 层较薄，优势是复杂度低。JuiceFS mount 侧包含大量 mount options：

- attr/entry/dir-entry cache timeout
- open-cache
- writeback
- max uploads/downloads
- upload/download limit
- xattr/acl/locks/ioctl
- direct mount
- debug/profile/log

BrewFS 当前暴露的核心是 data/meta/layout/fuse workers/max-background。

提升方向：

- 逐步增加面向语义和性能的 mount options，而不是只暴露底层参数。
- 将选项写入 `brewfs info/status`，便于排查。
- FUSE 错误码建立映射测试，避免内部错误都变成 EIO。

## 文件锁差异

BrewFS 已有 flock/plock 数据结构和测试文档。JuiceFS 的 session 会携带锁信息，并在 stale session cleanup 时处理。

提升方向：

- 锁生命周期绑定 session 和 owner。
- client crash 后锁释放必须可验证。
- flock/plock 与 fork、dup fd、close 行为需要测试。
- `status` 能显示锁持有者。

## POSIX 验收建议

优先回归以下模式：

- atomic save：`write temp -> fsync temp -> rename -> fsync dir`
- 多客户端 close-to-open：A close 后 B open/read
- truncate/write race：随机 pwrite + truncate + fsync
- unlink-open：unlink 后已打开 fd 继续读写，close 后清理
- hardlink/symlink：nlink、parent/path、readlink、rename link
- xattr/ACL：支持与不支持两种挂载都要稳定
- lock：flock/plock 阻塞、非阻塞、owner crash

