# XATTR backend support

本文记录的是 BrewFS 当前的扩展属性（xattr）支持边界。这里的“后端”有
两层含义，不能混为一谈：

* **元数据后端（`MetaStore`）**保存 inode、目录项和文件元数据。挂载点的
  `setxattr(2)`、`getxattr(2)`、`listxattr(2)`、`removexattr(2)`最终调用这一层。
* **workspace overlay KV 存储**保存 overlay 的增量记录。TiKV 在这一层和
  `MetaStore` 层都有独立的 `xattr/` 记录读写实现；两者的键空间和测试不同。

## 支持矩阵

表中 `✅` 表示该操作有实现并有测试证据，`❌` 表示明确未实现，`↪` 表示
能力由下层 `MetaStore` 继承，`—` 表示该层没有对应的 POSIX xattr API。

### MetaStore 和网关层

| 后端/层 | `set_xattr` | `get_xattr` | `list_xattr` | `remove_xattr` | 证据 |
| --- | --- | --- | --- | --- | --- |
| SQLite（`DatabaseMetaStore`） | ✅ | ✅ | ✅ | ✅ | `src/meta/stores/database/mod.rs:1673-1688` 将 `xattr` 能力置为 true；四个方法在 `src/meta/stores/database/mod.rs:4121-4229`，其中 `XATTR_CREATE`/`XATTR_REPLACE` 语义在 `set_xattr` 中处理。`src/meta/stores/database/tests.rs:602-620` 验证 xattr 变更的 ctime。 |
| Redis（`RedisMetaStore`） | ✅ | ✅ | ✅ | ✅ | `src/meta/stores/redis/mod.rs:3018-3041` 声明能力并实现四个方法（实现从 `:3041` 开始）；`test_redis_xattr_crud_flags_and_binary_values`（`src/meta/stores/redis/tests.rs:5560-5645`）覆盖 CRUD、二进制值、空值和 create/replace 标志，另有 ctime 和并发 create 测试。 |
| TiKV 元数据后端（`TiKvMetaStore`） | ✅ | ✅ | ✅ | ✅ | `src/meta/stores/tikv/mod.rs:1437` 将 `xattr` 能力置为 true；`xattr/<ino>/<name>` key 由 `src/meta/stores/tikv/mod.rs:185-193` 构造，四个方法在 `src/meta/stores/tikv/mod.rs:2838-2995`。`real_tikv_meta_store_xattr_crud_flags_and_binary_values`（`src/meta/stores/tikv/tests.rs:374`）覆盖 CRUD、二进制/空值、`XATTR_CREATE`/`XATTR_REPLACE`、ctime 和缺失 inode；`real_tikv_meta_store_xattr_inode_cleanup`（`src/meta/stores/tikv/tests.rs:481`）覆盖 `rmdir`、unlink 后 GC、rename 覆盖目录三条清理路径。 |
| etcd 元数据后端（`EtcdMetaStore`） | ❌ `NotImplemented` | ❌ `NotImplemented` | ❌ `NotImplemented` | ❌ `NotImplemented` | `src/meta/stores/etcd/mod.rs:1697-1717` 将 `xattr` 能力置为 false，且没有四个方法的实现；因此同样落到 `MetaStore` 默认实现 `src/meta/store.rs:1364-1388`。 |
| S3 gateway（网关层，不是 `MetaStore`） | ↪ | ↪ | — | ↪ | gateway 的对象 ETag、对象 metadata 和目录对象标记分别通过 `src/gateway/s3/store.rs:253-283` 的 get/set helper 读写下层 xattr；`src/gateway/s3/store_xattr_tests.rs` 的错误注入测试验证这些读取错误不会被当成“没有 xattr”。网关是否可用取决于所选 `MetaStore`，不会增加 TiKV/etcd 的能力。 |

S3 gateway 的 `—` 只表示它没有向 S3 客户端暴露 POSIX `listxattr(2)`；它的
内部元数据仍然依赖下层的 get/set/remove。使用 SQLite 或 Redis 时这些内部
属性可用；TiKV-MetaStore 现在也支持这些内部 get/set/remove 操作。使用
etcd 时，涉及这些属性的网关操作仍会沿着下层的 unsupported 错误失败。

### workspace overlay 的 TiKV KV 存储

| 存储层 | 写入（Put/CAS） | 读取（bounded get） | 列举（prefix scan） | 删除（Delete/CAS） | 证据 |
| --- | --- | --- | --- | --- | --- |
| TiKV overlay KV（`TiKvWorkspaceBackend`） | ✅ | ✅ | ✅ | ✅ | `src/workspace_overlay/stores/tikv.rs:1712-1890` 的 `real_tikv_bounded_xattr_reads_preserve_64k_value_and_shutdown_owners` 使用真实 TiKV 写入 `xattr/user.large`、读取 64 KiB 值、扫描 `xattr/` 前缀、检查边界错误并删除记录。 |

这个表中的 TiKV 是 overlay 的 KV 存储，不是上一张表的
`TiKvMetaStore`。两者使用不同的接口、键空间和测试。

## 本地复现

以下命令均使用 debug 测试二进制；`CARGO_TARGET_DIR` 指向独立目录，避免
与其他编译会话的 target 发生权限或增量编译冲突。Redis 测试会清空所选
Redis DB，因此应使用专用 DB。

### Redis MetaStore

Redis xattr 集成测试是 ignored 测试，Redis 必须监听在本机 `127.0.0.1:6379`
（或把 URL 改成专用实例）：

```bash
BREWFS_TEST_REDIS_URL=redis://127.0.0.1:6379/0 \
CARGO_TARGET_DIR=/home/hxy/xattr-target \
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
cargo test --no-default-features \
  --features fuse-tokio-runtime,workspace-overlay \
  --lib -- redis_xattr
```

测试 helper 同时接受历史变量 `BREWFS_REDIS_TEST_URL`。命令中的
`BREWFS_TEST_REDIS_URL` 会优先使用。

### TiKV overlay 和 TiKV MetaStore

PD 必须可从本机访问，且至少有一个 store 已注册。只验证 TiKV-MetaStore
xattr 时可使用下面的过滤词（它会选中 CRUD 和 inode 清理两条 ignored 测试）：

```bash
BREWFS_TEST_TIKV_PD_ENDPOINTS=127.0.0.1:2379 \
CARGO_TARGET_DIR=/home/hxy/xattr-target \
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
cargo test -p brewfs --lib \
  meta::stores::tikv::tests::real_tikv_meta_store_xattr -- --ignored
```

TiKV 相关测试都是 ignored 集成测试，并使用随机 namespace/前缀，避免相互
污染。题目中给出的 `-- --ignored real_tikv xattr` 在 Rust libtest 中是
多个 OR 过滤词，会把所有 `real_tikv` 测试也选进来；需要只验证 xattr 时
应使用上面的单一 `xattr` 过滤词，或分别传入两个完整测试名。

SQLite 的 xattr 单元测试不需要外部服务；可以用同一组 feature 和 target
运行 `cargo test --no-default-features --features fuse-tokio-runtime,workspace-overlay --lib xattr`，但这会同时选择本地 xattr 单元测试和 ignored 测试筛选结果。

## 挂载点上的错误码和限制

etcd 仍未实现 xattr；对普通 `user.*` xattr，它的调用路径是：

1. `MetaStore` 默认方法返回 `MetaError::NotImplemented`
   （`src/meta/store.rs:1364-1388`）。
2. VFS 将它转换为 `VfsError::Unsupported`
   （`src/vfs/error.rs:197-218`）。
3. FUSE 的 set/get/list/remove xattr handler 将 `Unsupported` 映射为
   `ENOSYS`（`src/fuse/mod.rs:2273-2282`、`:2315-2323`、`:2352-2358`、
   `:2442-2449`）。

因此在 etcd 挂载点上执行
`setfattr -n user.test -v value <path>`，预期失败并返回 errno **38
（`ENOSYS`，通常显示为 “Function not implemented”）**；get/list/remove 的
普通 user xattr 也走同一错误码。`system.posix_acl_*` 是单独的 ACL 路径，
其 capability 检查可能返回 `EOPNOTSUPP`，不要用它替代普通 user xattr 的
结论。

TiKV-MetaStore 已不再走上述 `NotImplemented` 路径。普通 `user.*` xattr
缺失时 `get_xattr` 返回 `Ok(None)`，FUSE 层映射为 `ENODATA`；
`remove_xattr` 和 `XATTR_REPLACE` 针对缺失属性返回 `MetaError::NotFound`，
FUSE 层同样映射为 `ENODATA`；`XATTR_CREATE` 针对已有属性返回
`MetaError::AlreadyExists`，映射为 `EEXIST`。

## 本次实测摘要

本分支实测摘要：

* TiKV 模块常规测试：`test result: ok. 13 passed; 0 failed; 15 ignored`。
* TiKV-MetaStore 真集群 xattr（本地 PD/TiKV 8.5.0）：
  `test result: ok. 2 passed; 0 failed; 0 ignored`，测试名为
  `real_tikv_meta_store_xattr_crud_flags_and_binary_values` 和
  `real_tikv_meta_store_xattr_inode_cleanup`。

## JuiceFS（外部 baseline）的 xattr 与 attr cache 配置

本节回答 C2/C3 对比里「JuiceFS 那边 xattr 能不能用、attr cache 怎么配」，
避免把 BrewFS 的 TTL 语义与 JuiceFS 的挂载默认值混为一谈。

### 1. xattr 能力（从 JuiceFS 源码核实，不是猜）

来源：JuiceFS `pkg/vfs/vfs.go`（本地镜像 `research/juicefs-ttl/vfs_vfs.go`）

| 项 | 事实 |
|---|---|
| 单值上限 | **65536 字节（64 KiB）**，超过返回 `ERANGE`（Linux） |
| 名字上限 | **255 字节** |
| `user.*` / `trusted.*` | 默认可用（`isXattrEnabled` 直接返回 true） |
| `security.capability` | 需 `Security.EnableCap` 能力开关，否则不可用 |
| `security.selinux` | 需 `Security.EnableSELinux` |
| `system.posix_acl_access` / `_default` | 需 `Format.EnableACL` |
| 全局关闭 | `DisableXAttrs` 可整体关闭 xattr |

**对比要点**：JuiceFS 的 xattr 单值上限是 **64 KiB**。BrewFS 的 64 KiB+ 分页读取测试
（`real_tikv_bounded_xattr_reads_preserve_64k_value_and_shutdown_owners`）覆盖的正是这个边界。
做 C3 xattr 对比时必须用 ≤64 KiB 的值，否则 JuiceFS 侧会 `ERANGE`，两边不可比。

### 2. attr / entry cache 旋钮（JuiceFS）

| 旋钮 | 默认 | 影响 |
|---|---|---|
| `--attr-cache` | 1 s | `getattr`/`stat` 的属性缓存 |
| `--entry-cache` | 1 s | 文件 `lookup`（路径→inode）缓存 |
| `--dir-entry-cache` | 1 s | 目录项枚举（readdir 结果）缓存 |
| `--negative-entry-cache` | 0 s | 不存在路径的负缓存（默认关） |
| `--open-cache` | 0（关） | 打开句柄复用；破坏 close-to-open，属激进选项 |

**核实方式（部署时必做，本表不能当运行时证据）**：

```bash
juicefs version
juicefs mount --help | grep -E 'attr-cache|entry-cache|dir-entry-cache|negative-entry-cache|open-cache'
```

本轮 campaign 使用的 JuiceFS 版本为 **1.4.2**；上表是文档默认值，实际生效值以挂载命令
与 `/proc/mounts` 记录为准。

### 3. 可运行验证脚本

```bash
#!/bin/bash
# 用法: verify-juicefs-xattr.sh <meta-url> <mountpoint>
# 例:  ATTR_CACHE=0 verify-juicefs-xattr.sh redis://127.0.0.1:6379/9 /mnt/jfs-xattr
set -euo pipefail
META=${1:?meta url}
MP=${2:?mount point}
ATTR=${ATTR_CACHE:-1}
ENTRY=${ENTRY_CACHE:-1}
DIRENTRY=${DIR_ENTRY_CACHE:-1}
NEG=${NEG_ENTRY_CACHE:-0}

juicefs format --storage file --bucket /tmp/jfs-bucket "$META" jfs-xattr >/dev/null
mkdir -p /tmp/jfs-bucket "$MP"
juicefs mount -d \
  --attr-cache="$ATTR" --entry-cache="$ENTRY" \
  --dir-entry-cache="$DIRENTRY" --negative-entry-cache="$NEG" \
  "$META" "$MP"
sleep 2
trap 'juicefs umount "$MP" 2>/dev/null || true' EXIT

f="$MP/probe"
echo hello > "$f"

# a) user.* 往返
setfattr -n user.demo -v alpha "$f"
printf 'getfattr user.demo: '; getfattr --only-values -n user.demo "$f"; echo

# b) listxattr 覆盖
printf 'listxattr: '; getfattr -d "$f" 2>/dev/null | tr '\n' ' '; echo

# c) 边界：64 KiB 通过、64 KiB+1 应被拒
python3 - "$f" <<'PY'
import os, sys
p = sys.argv[1]
os.setxattr(p, "user.big", b"x" * 65536)
print("  64KiB setxattr: ok, read back", len(os.getxattr(p, "user.big")))
try:
    os.setxattr(p, "user.toobig", b"x" * 65537)
    print("  WARN: 65537 bytes accepted (source says 64KiB limit; re-check version)")
except OSError as e:
    print("  65537 bytes rejected:", e)
PY

# d) 缓存语义：改属性后立即读，受 --attr-cache 影响
setfattr -n user.demo -v beta "$f"
printf 'after set, immediate read: '; getfattr --only-values -n user.demo "$f"; echo
echo "attr-cache=${ATTR}s entry-cache=${ENTRY}s dir-entry-cache=${DIRENTRY}s negative=${NEG}s"
echo "（用 ATTR_CACHE=0 再跑一次，对比 d) 的时序差异即为缓存效应）"
```

### 4. 与 BrewFS 旋钮的对照（C2/C3 对比必须对齐）

| 语义 | JuiceFS | BrewFS |
|---|---|---|
| 属性缓存（stat） | `--attr-cache`（1 s） | `fuse.cache_ttl_ms`（实验 0/1000）→ 待拆 attr TTL（#422） |
| 路径 lookup | `--entry-cache`（1 s） | 同上，待拆 entry TTL |
| 目录枚举 | `--dir-entry-cache`（1 s） | 同上，待拆 dir-entry TTL |
| 负缓存 | `--negative-entry-cache`（0） | 同上，待拆 negative TTL |
| 打开复用 | `--open-cache`（默认关） | 无对应；BrewFS 不做破坏 close-to-open 的缓存 |

**结论（对比口径）**：JuiceFS 默认 4 个缓存里 3 个是 1 s、负缓存 0；BrewFS 当前是单一
`cache_ttl_ms`。严格对齐的做法：BrewFS 侧 `cache_ttl_ms=1000` 对齐 JuiceFS 的
attr/entry/dir-entry=1 s，negative 两边都为 0；若研究差异，必须显式记录各自四个旋钮值，
不能只写「都用默认」。
