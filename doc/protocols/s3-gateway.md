# S3 Gateway Spec

状态：M1 MVP 已实现（PR #83），进入兼容性与生产化迭代
依赖：`s3s = "0.15"`（feature `gateway-s3`）
CLI：`brewfs gateway s3 --listen <addr> --access-key <ak> --secret-key <sk> [后端参数...]`

## 1. 目标与非目标

### 目标

- 以 S3 兼容协议暴露一个 BrewFS volume；当前已用 boto3 验证，目标兼容 `aws cli`、`mc`、s3fs 与各类 AWS SDK；
- 覆盖单 bucket 与多 bucket 两种模式（见 §3）；
- 覆盖核心对象操作与 multipart 上传（见 §4 的 M1 操作）；
- 与 FUSE 挂载数据互通：S3 写入的对象可在 FUSE 下按路径读取，反之亦然；
- 流式数据路径：GET/PUT 不整对象入内存，range 请求走 `read_at`。

### 非目标（显式排除，调用返回 NotImplemented）

以下能力不属于当前 M1 MVP，除非另有说明，调用应返回 `NotImplemented` 或协议对应的明确“不支持”错误：

- Bucket/Object versioning（`ListObjectVersions`）；
- Object Lock / Retention / Legal Hold；
- SSE（服务端加密）与对象级压缩；
- Bucket policy / ACL / IAM / STS；M1 只有单一静态密钥对（见 §6）；
- Event notification、Lifecycle、Replication、Inventory；
- SelectObjectContent；
- Bucket tagging、Website、Logging、Notification 等桶级配置接口；
- 完整的 `encoding-type`、`fetch-owner` 及 V1 `NextMarker` 列表扩展语义；
- ARN 形式的 `x-amz-copy-source`；M1 仅支持 `bucket/key`；
- 跨网关实例的分布式 key/bucket 锁；M1 仅提供进程内互斥；
- 原生 TLS；M1 使用 HTTP，生产环境需由反向代理终结 TLS。

Presigned URL 不需要服务端新增 API，但仍须验证 s3s SigV4 query authentication 与 BrewFS 路由的组合行为，列入 M3 客户端兼容性矩阵。

## 2. 架构

```
aws cli / mc / SDK
      │ HTTP + SigV4
      ▼
┌─────────────┐   S3 trait (s3s)   ┌──────────────┐   inode/path operations
│ s3s::service │ ─────────────────▶ │ BrewFsS3      │ ───────────────────────▶ VFS ─▶ meta/data
│ (axum/tokio) │                   │ (src/gateway/s3)│
└─────────────┘                   └──────────────┘
                                         │ xattr: brewfs.s3.{etag,meta,tags,dirobj}
                                         │ 内部状态: /.brewfs.sys/s3/{uploads,tmp}
```

- HTTP/SigV4/路由/序列化全部交给 s3s；我们只实现 `s3s::S3` trait 的方法；
- 存储映射层（`BrewFsS3`）直接使用进程内 `VFS`，不依赖 FUSE；
- 大对象分段流式转发：s3s 的请求 body 是 `Stream<Byte>`，写侧以固定窗口（默认 4 MiB，
  对齐 chunk block 大小）写入 staging 文件；读侧用 s3s 的 `StreamingBlob` 包装固定 inode 的
  分块读取流，并通过 bounded channel 提供背压。

## 3. Bucket 模型

| 模式 | 开启方式 | 映射 | 说明 |
|---|---|---|---|
| 单 bucket（默认） | `--bucket <name>`（缺省为 `brewfs`） | 整个 volume = 一个 bucket；`/` 是 bucket 根 | 与 JuiceFS gateway 默认行为一致 |
| 多 bucket | `--multi-buckets` | 顶层目录即 bucket：`/<bucket>/<key>` | `CreateBucket` = mkdir 顶层目录；`ListBuckets` = readdir `/` 并过滤合法 bucket 名与 `.brewfs.sys` |

约束：

- bucket 名合法性按 S3 规则（3-63 字符、小写、无连续点等）校验，不合法目录在多 bucket 模式下不出现在 ListBuckets；
- 两种模式下 object key → 路径的映射都是直接的 `/<key>`（单 bucket）或 `/<bucket>/<key>`（多 bucket），**不做 key 编码**；
  key 中的 `..`、以 `/` 开头等畸形输入在入口校验拒绝（`InvalidArgument`），防止逃逸 bucket 根；
- `HeadBucket` = stat bucket 根目录；`DeleteBucket` = rmdir 空目录（非空返回 `BucketNotEmpty`）。

## 4. 操作映射表

状态含义：`M1` = 已随 PR #83 实现；`M3` = S3 兼容性补全计划；`M5/远期` = 生产化或长期能力。

| S3 操作 | 状态 | BrewFS 映射或计划 |
|---|---|---|
| PutObject | M1 | 流式写 staging 文件，写完数据与 xattr 后 `rename` 到目标路径；覆盖发布保持原子性 |
| GetObject | M1 | `stat` + 固定 inode 分块流式返回；支持完整对象与单 Range 读取 |
| HeadObject | M1 | `stat` + xattr 还原 ETag、Content-Type 和 user metadata；目录对象判定见 §5 |
| DeleteObject | M1 | `unlink`；随后自底向上回收变空的隐式父目录，直到 bucket 根或显式目录对象 |
| DeleteObjects（批量） | M1 | 逐项删除并返回每项结果 |
| ListObjectsV1 / V2 | M1 | 前缀树遍历；支持 prefix、delimiter、marker/start-after、continuation-token 和 max-keys；在分页计数前过滤不可见目录 |
| CreateBucket / DeleteBucket / HeadBucket / ListBuckets / GetBucketLocation | M1 | 见 §3；进程内 bucket lifecycle lock 防止删桶与迟到发布竞态 |
| CopyObject | M1 | VFS 内部分块复制到 staging 后原子发布；支持 `bucket/key` copy source 和 metadata copy/replace |
| CreateMultipartUpload | M1 | 建目录 `/.brewfs.sys/s3/uploads/<hh>/<upload-id>/`，`.target` 记录目标 bucket/key 和 metadata |
| UploadPart | M1 | 流式写 `part-<n>`，part ETag 记入 `brewfs.s3.etag` xattr |
| ListParts / ListMultipartUploads | M1 | 分页扫描内部 upload 状态并验证 upload ID、bucket、key 归属 |
| CompleteMultipartUpload | M1 | 校验严格递增的 part 序号与 ETag（允许序号有空档）；除最后一片外检查 5 MiB 下限；分块合并 staging 后原子发布 |
| AbortMultipartUpload | M1 | 验证归属后递归删除 upload 目录 |
| Put/Get/DeleteObjectTagging | M3 | 以 `brewfs.s3.tags` xattr 持久化，并补充与 COPY、multipart completion 的继承规则 |
| GetObjectAttributes | M3 | 组合 `stat`、ETag xattr 和 multipart 信息 |
| 条件请求（If-Match、If-None-Match、If-Modified-Since 等） | M3 | 为 GET/HEAD/COPY/PUT 补齐 S3 precondition 顺序和错误码 |
| List 扩展（`encoding-type`、`fetch-owner`、完整 V1 `NextMarker`） | M3 | 扩展现有 list collector 与响应字段，不改变现有 continuation token |
| CopySource 扩展 | M3 | 支持 URL 编码细节及 ARN 等当前 `bucket/key` 之外的形式 |
| CORS、Bucket Tagging | M3 | 优先补齐常用浏览器与运维客户端场景，并纳入兼容性矩阵 |
| Website / Logging 等其余桶级配置 | M5/远期 | 按客户端需求逐项实现，未实现时返回明确错误而非静态成功 |
| Bucket/Object ACL、Bucket Policy | M5 | 在多身份与授权模型确定后实现；M1 不伪造权限语义 |
| 多密钥、IAM/STS 兼容层 | M5 | 与 console 身份体系整合，定义 principal 到 BrewFS identity 的映射 |
| 匿名只读与 virtual-host addressing | M5 | 作为显式 policy/domain 配置实现，并覆盖 host/path-style 路由隔离 |
| 原生 TLS | M5 | rustls 监听与证书热更新；此前由反向代理终结 TLS |
| Versioning | M5/远期 | 需先定义版本数据布局、删除标记、LIST 语义和跨协议可见性 |
| SSE-S3 / SSE-KMS / SSE-C | M5/远期 | 需定义密钥管理、加密 metadata 与 range/multipart 数据路径；禁止静默忽略加密请求头 |
| Object Lock / Retention / Legal Hold | 远期 | 依赖 versioning、不可变策略和绕过权限模型 |
| Lifecycle / Notification / Replication / Inventory | 远期 | 依赖持久任务、事件日志、幂等重试与运维控制面 |
| SelectObjectContent | 远期评估 | 仅在有明确需求和可维护的执行引擎后立项 |

### 条件请求

M1 仅承诺已验证的基础读写与 Range 语义，不承诺完整条件头行为。所有未实现的条件头必须明确拒绝，不能静默表现为已满足；完整 precondition 语义列入 M3。

PUT、GET、HEAD、COPY、DELETE，以及 multipart complete/abort 的条件字段目前统一返回 `NotImplemented`，并在消费请求体、访问上传状态或修改对象之前拒绝请求。

## 5. 目录语义（S3 扁平空间 ↔ POSIX 层级）

这是 S3 网关最难做对的部分，规则如下（与 JuiceFS gateway 行为对齐，但用 xattr 代替 atime 技巧）：

1. **隐式目录**：`PUT a/b/c.txt` 自动 `mkdir_p a/b`；list `a/` 时 `a/b/` 作为 CommonPrefix 出现。
   这些目录没有 xattr 标记，DeleteObject 后被回收（逐级向上，遇非空停）。
2. **显式目录对象**：`PUT a/b/`（key 以 `/` 结尾，零长度 body）创建真实目录并打
   `brewfs.s3.dirobj` xattr；它在 list 中作为一条零字节对象出现（key 带尾 `/`），
   DeleteObject 父级回收遇到它停止；`--hide-dir-objects` 选项可在 list 中隐藏。
3. **文件与目录同名冲突**：POSIX 下不可能共存；若 FUSE 侧手工构造出与对象同前缀的结构，
   list 结果以目录优先（与 JuiceFS 一致），文档声明避免混用。
4. `HEAD key/`（带尾斜杠）命中目录对象返回 200；命中隐式目录返回 404（S3 真实行为）。

## 6. 认证与安全

- M1：单一静态密钥对。`--access-key/--secret-key` 或环境变量 `BREWFS_S3_ACCESS_KEY/BREWFS_S3_SECRET_KEY`；
  SigV4 校验由 s3s 的 auth 组件完成；
- 未配置密钥时拒绝启动；M1 不支持匿名访问、多密钥、IAM、STS 或 ACL/policy 授权；
- TLS：M1 为 HTTP；生产部署应前置反向代理（nginx/caddy）终结 TLS；
- M3/M5：先定义多 principal 与 BrewFS identity 的映射，再实现多密钥、ACL/policy、STS 和原生 rustls，
  避免在没有权限模型时仅返回看似成功的 S3 配置响应。

## 7. 一致性与并发

- **写-写冲突**：单实例内使用固定数量的 `(bucket,key)` key-lock 分片，同一对象的
  Put/Complete/Delete/Copy 发布串行化；COPY 涉及两个分片时按固定顺序加锁；
- **bucket 生命周期**：固定 bucket-lock 分片串行化 Create/DeleteBucket 与 multipart 创建、
  PUT/COPY/Complete 的最终发布；发布前重新验证 bucket，避免删桶后被在途写入重建；
- **写-读冲突**：GET 在返回流前获取 VFS read guard，固定源 inode；覆盖写通过 staging + rename
  发布，因此慢消费者读取完整旧版本或完整新版本，不会读到混合数据；
- **multipart 并发**：操作以 upload ID + bucket + key 校验归属；同一 part number 的替换受 key
  互斥保护；活动 multipart 会阻止 multi-bucket 模式删除对应 bucket；
- **多网关实例**：M1 的 key-lock 与 bucket-lock 都是进程内固定分片，不保证跨实例串行化；
  分布式锁、fencing 和失败恢复协议列入 M3，不能只增加远程 mutex 而缺少租约失效保护；
- **list 一致性**：单次列表先按可见 S3 entry 排序再分页，但分页期间的并发增删仍不提供跨请求快照；
  continuation token 只表示字典序位置。

## 8. 错误码映射

| BrewFS 错误 | S3 响应 |
|---|---|
| `NotFound`（对象） | 404 `NoSuchKey` |
| `NotFound`（bucket 根） | 404 `NoSuchBucket` |
| `AlreadyExists`（bucket） | 409 `BucketAlreadyExists` / `BucketAlreadyOwnedByYou` |
| `DirectoryNotEmpty` | 409 `BucketNotEmpty` |
| `PermissionDenied` | 403 `AccessDenied` |
| `InvalidInput`（key 畸形/part 序号错） | 400 `InvalidArgument` / `InvalidPartOrder` |
| 空间不足 / 配额 | 507 `InsufficientStorage`（自定义 message） |
| 其他内部错误 | 500 `InternalError` + request id |

etag 不匹配的 part 校验 → 400 `InvalidPart`；upload-id 不存在 → 404 `NoSuchUpload`。

## 9. 性能目标与要求

- PUT/GET 大对象（≥1 GiB）稳态吞吐不低于同机 FUSE 挂载直写的 80%（无内核路径，理论上应更高）；
- 全程流式：单连接内存占用有界（≤ 2 × 4 MiB 缓冲 + 协议栈开销），与对象大小无关；
- 并发：默认 tokio worker 全核；单 key 串行不成为全局瓶颈；
- list 每页 max-keys=1000 的 p99 延迟 < 200ms（sqlite meta，10 万对象规模内）。

## 10. CLI 参数

```
brewfs gateway s3 \
  --listen 0.0.0.0:9000            # 监听地址
  --access-key / --secret-key      # 静态密钥（或 env）
  [--bucket <name>]                # 单 bucket 名（默认 brewfs）
  [--multi-buckets]                # 多 bucket 模式
  [--hide-dir-objects]             # list 隐藏目录对象
  <与 mount 相同的后端参数>
```

`--anonymous-read`、virtual-host `--domain` 与原生 TLS 均为后续计划，不属于 M1 CLI。

## 11. 测试与后端验证矩阵

### 11.1 当前结果（2026-09-09）

| Meta backend | Data backend | S3 gateway 结果 | 说明 |
|---|---|---|---|
| SQLite | LocalFS | 已验证 | fresh volume 上 boto3 E2E：76 passed / 0 failed；网关单元测试 22 passed；包含大对象慢速 GET、并发覆盖、路径边界、list 分页、multipart 和 bucket lifecycle 竞态 |
| Redis | RustFS（S3 data backend） | **尚未验证** | 仓库已有该组合的 FUSE/POSIX、xfstests 与性能测试，但这些结果不经过 `brewfs gateway s3`，不能视为 S3 网关验证 |
| 其他 MetaStore | LocalFS/S3-compatible | 尚未形成网关矩阵 | 编译和通用 VFS 测试通过不等价于真实协议 E2E |

CI 中名为 Redis/TiKV workspace-overlay、pjdfstest、xfstests 或 stress-ng 的 job 验证的是 FUSE/VFS
数据路径，而不是 S3 网关端点。报告 S3 网关兼容性时必须单独标明 gateway endpoint、meta backend、
data backend 和客户端，不能用底层后端测试代替。

### 11.2 Redis + RustFS 验收计划（M3）

1. 通过 Docker Compose 或本地 WSL 启动 Redis 与 RustFS，为 RustFS 创建独立的数据 bucket；
2. 使用不同端口启动 RustFS 和 BrewFS S3 gateway，避免二者默认 `9000` 端口冲突；
3. 以 `--meta-backend redis --meta-url redis://... --data-backend s3` 和 RustFS endpoint 启动网关，
   在 single-bucket fresh volume 上运行与 SQLite + LocalFS 相同的 76 项 boto3 E2E；
4. 重启网关、Redis 和 RustFS 后复查对象、xattr metadata、ETag、目录对象和未完成 multipart 状态，
   验证持久化与 stale-upload cleanup；
5. 运行 multi-bucket 的 Create/DeleteBucket、active multipart、在途 PUT/COPY/Complete 与 namespace
   边界测试；
6. 同一 Redis + RustFS volume 同时由 FUSE 与 S3 gateway 访问，双向验证对象数据、rename、删除和
   metadata 可见性；
7. 将最小 boto3 smoke 纳入普通 CI；完整 76 项、重启与故障恢复场景可作为定时或手动 workflow，
   并上传 gateway、Redis、RustFS 日志。

验收门槛：功能断言 0 failure、服务重启后无数据/metadata 丢失、失败发布无泄漏可见对象、
active multipart 不被 cleanup 或 DeleteBucket 误删。多网关实例并发不在本阶段宣称通过，须等 §12
的分布式锁方案落地。

### 11.3 客户端兼容性计划

- 当前：boto3 端到端测试；
- M3：AWS CLI、MinIO Client (`mc`)、presigned URL、s3fs 的核心读写/列表/multipart 冒烟；
- 每个客户端记录版本、addressing style、签名模式与已知差异，避免仅以单一 SDK 代表 S3 兼容性。

## 12. 后续里程碑

### M1：核心 S3 网关（已完成，PR #83）

- 单桶与多桶、核心对象 CRUD/COPY/LIST、multipart 全流程、静态 SigV4 密钥、流式 GET/PUT；
- staging + rename 原子发布、固定 key/bucket lock 分片、内部 namespace 与 multipart ownership 防护；
- SQLite + LocalFS 的 76 项 boto3 E2E、22 项单元测试和 CI 编译/Clippy/测试矩阵。

### M3：兼容性与后端矩阵

- 完成 Redis + RustFS 验收计划并接入 CI smoke；
- Object Tagging、GetObjectAttributes、完整条件请求；
- `encoding-type`、`fetch-owner`、V1 `NextMarker` 和 CopySource 扩展；
- AWS CLI、`mc`、s3fs、presigned URL 兼容性矩阵；
- 常用 CORS、bucket tagging 等桶级配置；
- 设计并实现带租约/fencing 的跨实例 key/bucket 锁，增加双网关故障注入回归。

### M5：安全与生产化

- 多密钥、ACL/policy、IAM/STS 兼容层及其 BrewFS identity 映射；
- 原生 rustls、证书轮换、请求审计、Prometheus 指标、限流与配额错误映射；
- Redis + RustFS 大对象吞吐、10 万/100 万对象 list、multipart 并发和 24h soak 基线。

### 远期：高级 S3 数据语义

- Versioning 与 delete marker；
- SSE-S3/SSE-KMS/SSE-C；
- Object Lock / Retention / Legal Hold；
- Lifecycle、Notification、Replication、Inventory；
- SelectObjectContent 按实际需求再评估。

这些能力必须先给出持久化布局、跨协议可见性、升级/回滚和故障恢复设计，不以返回静态成功响应
冒充兼容支持。

## 13. 参考

- JuiceFS gateway：`pkg/gateway/gateway.go`（tmp+rename、`/.sys`、xattr etag/meta、
  DeleteObject 父目录回收、multipart 目录模型、cleanup 周期任务）；
  文档 `juicefs/docs/en/guide/gateway.md`；
- s3s crate：<https://crates.io/crates/s3s>（RustFS 同源 S3 服务框架）；
- 本目录 [README.md](README.md) §3 共用约定。
