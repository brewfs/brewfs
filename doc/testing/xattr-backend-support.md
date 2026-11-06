# XATTR backend support

本文记录的是 BrewFS 当前的扩展属性（xattr）支持边界。这里的“后端”有
两层含义，不能混为一谈：

* **元数据后端（`MetaStore`）**保存 inode、目录项和文件元数据。挂载点的
  `setxattr(2)`、`getxattr(2)`、`listxattr(2)`、`removexattr(2)`最终调用这一层。
* **workspace overlay KV 存储**保存 overlay 的增量记录。TiKV 在这一层有
  `xattr/` 记录的读写实现；这不代表 TiKV 作为 `MetaStore` 也支持 xattr。

## 支持矩阵

表中 `✅` 表示该操作有实现并有测试证据，`❌` 表示明确未实现，`↪` 表示
能力由下层 `MetaStore` 继承，`—` 表示该层没有对应的 POSIX xattr API。

### MetaStore 和网关层

| 后端/层 | `set_xattr` | `get_xattr` | `list_xattr` | `remove_xattr` | 证据 |
| --- | --- | --- | --- | --- | --- |
| SQLite（`DatabaseMetaStore`） | ✅ | ✅ | ✅ | ✅ | `src/meta/stores/database/mod.rs:1673-1688` 将 `xattr` 能力置为 true；四个方法在 `src/meta/stores/database/mod.rs:4121-4229`，其中 `XATTR_CREATE`/`XATTR_REPLACE` 语义在 `set_xattr` 中处理。`src/meta/stores/database/tests.rs:602-620` 验证 xattr 变更的 ctime。 |
| Redis（`RedisMetaStore`） | ✅ | ✅ | ✅ | ✅ | `src/meta/stores/redis/mod.rs:3018-3041` 声明能力并实现四个方法（实现从 `:3041` 开始）；`test_redis_xattr_crud_flags_and_binary_values`（`src/meta/stores/redis/tests.rs:5560-5645`）覆盖 CRUD、二进制值、空值和 create/replace 标志，另有 ctime 和并发 create 测试。 |
| TiKV 元数据后端（`TiKvMetaStore`） | ❌ `NotImplemented` | ❌ `NotImplemented` | ❌ `NotImplemented` | ❌ `NotImplemented` | `src/meta/stores/tikv/mod.rs:1380-1391` 没有打开 `xattr` 能力，且文件中没有四个方法的实现，因此使用 `MetaStore` 默认实现 `src/meta/store.rs:1364-1388`。新增的 ignored 真集群测试 `real_tikv_meta_store_xattr_is_not_implemented`（`src/meta/stores/tikv/tests.rs:336-379`）直接断言四个调用均返回 `MetaError::NotImplemented`。 |
| etcd 元数据后端（`EtcdMetaStore`） | ❌ `NotImplemented` | ❌ `NotImplemented` | ❌ `NotImplemented` | ❌ `NotImplemented` | `src/meta/stores/etcd/mod.rs:1697-1717` 将 `xattr` 能力置为 false，且没有四个方法的实现；因此同样落到 `MetaStore` 默认实现 `src/meta/store.rs:1364-1388`。 |
| S3 gateway（网关层，不是 `MetaStore`） | ↪ | ↪ | — | ↪ | gateway 的对象 ETag、对象 metadata 和目录对象标记分别通过 `src/gateway/s3/store.rs:253-283` 的 get/set helper 读写下层 xattr；`src/gateway/s3/store_xattr_tests.rs` 的错误注入测试验证这些读取错误不会被当成“没有 xattr”。网关是否可用取决于所选 `MetaStore`，不会增加 TiKV/etcd 的能力。 |

S3 gateway 的 `—` 只表示它没有向 S3 客户端暴露 POSIX `listxattr(2)`；它的
内部元数据仍然依赖下层的 get/set/remove。使用 SQLite 或 Redis 时这些内部
属性可用；使用 TiKV-MetaStore 或 etcd 时，涉及这些属性的网关操作会沿着
下层的 unsupported 错误失败。

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

PD 必须可从本机访问，且至少有一个 store 已注册。用单个 `xattr` 过滤词
会运行 Redis xattr 测试、overlay 的真实 TiKV xattr 测试和新增的
TiKV-MetaStore unsupported 断言：

```bash
BREWFS_TEST_TIKV_PD_ENDPOINTS=172.24.86.193:2379 \
CARGO_TARGET_DIR=/home/hxy/xattr-target \
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
cargo test --no-default-features \
  --features fuse-tokio-runtime,workspace-overlay \
  --lib -- --ignored xattr
```

TiKV 相关测试都是 ignored 集成测试，并使用随机 namespace/前缀，避免相互
污染。题目中给出的 `-- --ignored real_tikv xattr` 在 Rust libtest 中是
多个 OR 过滤词，会把所有 `real_tikv` 测试也选进来；需要只验证 xattr 时
应使用上面的单一 `xattr` 过滤词，或分别传入两个完整测试名。

SQLite 的 xattr 单元测试不需要外部服务；可以用同一组 feature 和 target
运行 `cargo test --no-default-features --features fuse-tokio-runtime,workspace-overlay --lib xattr`，但这会同时选择本地 xattr 单元测试和 ignored 测试筛选结果。

## 挂载点上的错误码和限制

对普通 `user.*` xattr，TiKV-MetaStore 或 etcd 的调用路径是：

1. `MetaStore` 默认方法返回 `MetaError::NotImplemented`
   （`src/meta/store.rs:1364-1388`）。
2. VFS 将它转换为 `VfsError::Unsupported`
   （`src/vfs/error.rs:197-218`）。
3. FUSE 的 set/get/list/remove xattr handler 将 `Unsupported` 映射为
   `ENOSYS`（`src/fuse/mod.rs:2273-2282`、`:2315-2323`、`:2352-2358`、
   `:2442-2449`）。

因此在 TiKV-MetaStore 或 etcd 挂载点上执行
`setfattr -n user.test -v value <path>`，预期失败并返回 errno **38
（`ENOSYS`，通常显示为 “Function not implemented”）**；get/list/remove 的
普通 user xattr 也走同一错误码。`system.posix_acl_*` 是单独的 ACL 路径，
其 capability 检查可能返回 `EOPNOTSUPP`，不要用它替代普通 user xattr 的
结论。

overlay TiKV 的成功测试只证明 overlay KV 的 bounded 记录读写和清理，不能
改变 TiKV-MetaStore 的上述结果。当前工作范围也不包含为 TiKV-MetaStore
新增 xattr 持久化实现。

## 本次实测摘要

本分支实测摘要：

* Redis：`test result: ok. 5 passed; 0 failed; 0 ignored`（5 个
  `redis_xattr` 测试）。
* TiKV/overlay 与 TiKV-MetaStore：`test result: ok. 9 passed; 0 failed;
  0 ignored`（单一 `xattr` 过滤词；其中 Redis 的 7 个 xattr 清理/CRUD
  测试也会被该过滤词选中）。overlay 测试名为
  `real_tikv_bounded_xattr_reads_preserve_64k_value_and_shutdown_owners`，
  MetaStore 测试名为 `real_tikv_meta_store_xattr_is_not_implemented`。
