# HDFS / Hadoop Java SDK

状态：H0 native C ABI 已实现；Hadoop Java adapter 为实验性实现

## 1. 目标边界

BrewFS 的 HDFS 方向采用 JuiceFS 的 Hadoop Java SDK 路线：Hadoop 应用通过 `FileSystem` API 访问 BrewFS，而不是让 BrewFS 冒充 HDFS NameNode/DataNode。

当前交付物：

- `hdfs-sdk` Rust feature；
- 版本化 `brewfs_v1_*` C ABI；
- LocalFS data backend + SQLite metadata 的 native contract；
- `sdk/java` 中的 Hadoop `FileSystem` adapter 原型（当前通过 JNA 调用 C ABI，属于实验性 Java FFI bridge），默认 scheme 为 `jfs`/`brewfs`（需显式配置）；
-真实 VFS 文件句柄的 `read/write/flush/fsync/close` 错误传播；
- path、stat、目录列举、rename、delete、truncate、xattr 和 POSIX metadata 的基础映射。

明确不支持：Hadoop RPC、NameNode、DataNode、block transfer pipeline、replication、block locality、HDFS-compatible checksum、lease recovery、HA、delegation token、Kerberos/RPCSEC_GSS、ACL 完整兼容和跨进程 writer fencing。调用这些语义必须返回不支持或使用文档化的兼容 fallback，不能伪造成功。

## 2. Native ABI

头文件：[sdk/c/include/brewfs.h](../../sdk/c/include/brewfs.h)。

- ABI 版本为 `major << 16 | minor`，当前为 `1.0`；
- C 字符串和路径使用带长度的 UTF-8 byte buffer，不依赖 NUL 终止；
- C client/file/directory 使用 opaque handles，通过各自的 `*_close` 释放（不提供通用 free 函数）；
- `struct_size` 是每个 options/output 结构的第一个字段：入口先只读取该字段并校验，`0` 或小于 ABI v1 结构大小返回 `BREWFS_INVALID_ARGUMENT`；更大的值允许（向后兼容），只读/写 ABI v1 字段。输出结构（`brewfs_stat_v1`/`brewfs_statfs_v1`）由调用方预先填写 `struct_size`；
- 所有入口捕获 Rust panic，并将其转换为稳定状态码；
- error message 使用 thread-local last-error buffer；
- `pread/pwrite` 使用 caller-owned buffer 和显式 offset，单次 I/O 上限 16 MiB（超出返回 `BREWFS_INVALID_ARGUMENT`）；
- `OPEN_APPEND` 只保证可写语义，写入位置由调用方负责：打开后先 stat 取当前 size 作为续写 offset；
- `delete(recursive=1)` 对普通文件同样成功（HDFS 语义），仅对目录递归展开；
- `flush`、`fsync(data_only)`、`close` 经过 VFS 的真实 writeback/metadata 路径，错误向调用方返回；
- capabilities 用 bitset 报告 xattr/statfs/append 能力。

当前 native factory 使用 flat-v1 的 LocalFS + SQLite 组合。未提供 `metadata_url` 时，默认为 data dir 下的持久化 SQLite 文件（`sqlite://<data_dir>/metadata.db?mode=rwc`），而不是 in-memory 数据库：共享同一 data dir 的多个客户端（或重启后的进程）因此能看到同一份元数据。配置结构已经为 S3-compatible data backend 和 Redis metadata URL 保留扩展位；后端矩阵在独立的 endpoint E2E 完成前不宣称通过。

## 3. Java adapter

`sdk/java` 是 Hadoop 3.3 API、JDK 8 字节码基线的实验模块。配置示例：

```xml
<property>
  <name>fs.jfs.impl</name>
  <value>io.brewfs.BrewFsFileSystem</value>
</property>
<property>
  <name>brewfs.data.dir</name>
  <value>/var/lib/brewfs/data</value>
</property>
<property>
  <name>brewfs.metadata.url</name>
  <value>sqlite:///var/lib/brewfs/metadata.db</value>
</property>
```

`hflush()` 映射为 data-only fsync，`hsync()` 映射为 full fsync；output stream close 先执行 hsync，再 close，第一处错误不会被吞掉。`append()` 打开后以当前文件长度作为流起点，超过 native `max_io`（16 MiB）的单次读写会在 Java 侧分块。`rename`/`delete` 在源不存在（native `NOT_FOUND`）时按 Hadoop 契约返回 `false` 而不是抛异常。`brewfs.metadata.url` 未配置时默认为 data dir 下的持久化 SQLite 文件（`<data.dir>/metadata.db`）；Hadoop replication 参数目前只作为兼容输入，不改变 BrewFS 的内部 slice/block 数据布局。

## 4. 构建与测试

Rust native ABI：

```bash
cargo check --no-default-features --features fuse-tokio-runtime,hdfs-sdk --lib
cargo test --no-default-features --features fuse-tokio-runtime,hdfs-sdk hdfs::tests --lib
```

Java adapter：

```bash
cd sdk/java
mvn -DskipTests package
```

H0 已覆盖：ABI version、LocalFS/SQLite 初始化、open/create、pwrite、fsync、close、pread、重开读取。后续必须增加 GCC/Clang C11 contract、ABI header drift 检查、Hadoop contract tests 和 FUSE/S3 cross-entry verification。

## 5. 后续里程碑

- H0：稳定 C ABI、opaque lifecycle、native contract tests；
- H1：JNI/JNA 发布打包、Hadoop contract test、多平台 native loader；
- H2：Redis + RustFS cross-entry、权限/identity hardening、append lease/fencing 评估；
- 远期：仅在明确需求下评估原生 HDFS wire protocol；它与 Java SDK 是不同项目，不应混用验收标准。
