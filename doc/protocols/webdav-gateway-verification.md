# WebDAV 网关本地验证说明

本文档描述如何在本地（WSL Ubuntu-22.04）验证 BrewFS WebDAV 网关（`brewfs gateway webdav`）。
对应的协议范围见 WebDAV 网关 spec；本 MVP 面向单用户 Class 2 WebDAV 客户端，并与 FUSE/S3 共享同一 volume。

## 1. 前置条件

- Linux（WSL Ubuntu-22.04 已验证）+ Rust 工具链
- Python 3（端到端脚本只使用标准库）
- `protoc`、OpenSSL（仅 HTTPS 验证需要）
- WSL 中代理变量不可用时，构建前执行 `unset http_proxy https_proxy all_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY`
- SQLite 元数据后端和 LocalFS 数据后端可用于最小本地验证

## 2. 构建

```bash
cargo build --no-default-features --features "gateway-webdav,fuse-tokio-runtime"
```

仓库默认 features 同时包含 S3 和 WebDAV；使用默认配置时也可以直接执行：

```bash
cargo build
```

WebDAV-only 构建需要显式启用一个 asyncfuse runtime feature，即上例中的 `fuse-tokio-runtime`。

## 3. 启动 HTTP 网关（LocalFS + SQLite）

默认要求 Basic Auth，并默认使用原子 PUT/PATCH：

```bash
mkdir -p /tmp/brewfs-webdav-e2e/data
cargo run --no-default-features --features "gateway-webdav,fuse-tokio-runtime" -- \
  gateway webdav \
  --listen 127.0.0.1:19102 \
  --user testuser --password testpass \
  --data-backend local-fs --data-dir /tmp/brewfs-webdav-e2e/data \
  --meta-backend sqlx \
  --meta-url "sqlite:///tmp/brewfs-webdav-e2e/meta.db?mode=rwc"
```

常用参数：

| 参数 | 说明 |
|---|---|
| `--listen` | HTTP/HTTPS 监听地址，默认 `0.0.0.0:9001` |
| `--user` / `--password` | 成对的 Basic Auth 凭据，也可用 `BREWFS_WEBDAV_USER` / `BREWFS_WEBDAV_PASSWORD` |
| `--allow-anonymous` | 显式允许匿名读写；不能与 `--user/--password` 同时使用 |
| `--atomic-put <true\|false>` | 原子发布 PUT/PATCH，默认 `true`；关闭后为直接写回模式 |
| `--tls-cert` / `--tls-key` | 成对的 PEM 证书链和私钥，启用原生 HTTPS |
| 其余 | 与 `brewfs mount` 相同的 volume、数据后端和元数据后端参数 |

未提供凭据且未传 `--allow-anonymous` 会在监听前失败。用户名/密码不完整、为空，或 TLS 证书/私钥不完整也会在监听前失败。配置 Basic Auth 时，非 loopback 监听地址必须同时配置 TLS；loopback HTTP 仅适合本地验证。

启动成功的标志是日志出现 `brewfs webdav gateway listening` 或其 TLS 版本。

## 4. 运行端到端测试

在网关保持运行时执行：

```bash
python3 scripts/e2e_webdav_gateway.py
```

脚本默认连接 `http://127.0.0.1:19102`、凭据 `testuser/testpass`。可用以下环境变量覆盖：

- `GW_ENDPOINT`
- `GW_USER` / `GW_PASSWORD`
- `GW_AUTH_REQUIRED=0`（匿名模式）
- `GW_ATOMIC_PUT=0`（仅改变失败 body 后的断言）
- `GW_INSECURE_TLS=1`（自签名 HTTPS）

**2026-09-10 在 WSL Ubuntu-22.04 的实际结果：**

- 认证 + 原子 PUT/PATCH + HTTP：`RESULT: 79 passed, 0 failed`
- 认证 + 直接写回（`--atomic-put false`）：`RESULT: 79 passed, 0 failed`
- 显式匿名 + 原子 PUT/PATCH：`RESULT: 77 passed, 0 failed`；认证拒绝相关的 2 项按匿名模式跳过
- 认证 + 自签名 HTTPS：`RESULT: 79 passed, 0 failed`；随后发送 SIGINT，TLS listener 优雅退出、端口关闭且退出码为 0

此次共享 metadata store 生命周期修复还做了跨协议回归：

- S3-only 构建 + fresh LocalFS/SQLite：`scripts/e2e_s3_gateway.py` 实测 `RESULT: 76 passed, 0 failed`
- flat FUSE + fresh LocalFS/SQLite：root 权限下文件创建、读取、目录创建、移动和卸载通过；mount 进程退出码为 0
- WebDAV HTTP/TLS 实例的日志均未出现 `sid has been set`、`No session_id found during shutdown` 或 panic

覆盖内容包括：

- Basic Auth 缺失/错误/正确凭据，匿名访问分支
- OPTIONS、DAV Class 1/2 和资源类型相关 Allow 能力
- GET、HEAD、Range GET、PUT、PATCH（range/append）
- MKCOL、Depth 0/1/infinity PROPFIND、PROPPATCH 和 dead-property round-trip
- Unicode 路径、保留 `.brewfs.sys` 命名空间拒绝、根目录边界
- 文件 COPY/MOVE、Overwrite `T/F`、递归目录 COPY
- LOCK/UNLOCK、锁定写入、lock-null resource
- 原子模式的短 body/超长 body 拒绝及目标保留
- 超过 256 个子项的递归 DELETE 和完整目录分页

## 5. 单元测试与 CLI 检查

```bash
cargo test --no-default-features --features "gateway-webdav,fuse-tokio-runtime" webdav --lib
```

当前 WebDAV 聚焦测试为 19 个用例，覆盖路径映射、顶层 Destination 的内部空父路径、保留命名空间、时间转换、seek 边界、认证、请求方法作用域、`Content-Range` 和 body 长度、ETag/条件写，以及属性上限/事务性更新。

在低内存、单任务构建参数（`CARGO_BUILD_JOBS=1`、`CARGO_INCREMENTAL=0`、关闭 debug info）下还完成了：

- `cargo test --workspace --lib --bins -- --test-threads=1`：`679 passed, 0 failed, 175 ignored`
- `cargo test --workspace --all-features -- --test-threads=1`：`753 passed, 0 failed, 177 ignored`
- WebDAV-only `cargo clippy ... -- -D warnings`：通过
- Redis（无 xattr）能力冒烟：OPTIONS、读写、活属性和基本 WebDAV 操作通过；脚本结果为 `65 passed, 4 failed`，4 项均为 dead-property 持久化断言，`PROPPATCH` 按后端能力正确返回 507
- 使用可写 SQLite、无效 PEM 证书/私钥启动：在 TLS 加载阶段失败，无监听端口、无 orphan gateway 进程

查看 CLI：

```bash
cargo run --no-default-features --features "gateway-webdav,fuse-tokio-runtime" -- gateway webdav --help
```

## 6. HTTPS 验证

生成短期自签名证书并启动：

```bash
mkdir -p /tmp/brewfs-webdav-tls/data
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout /tmp/brewfs-webdav-tls/key.pem \
  -out /tmp/brewfs-webdav-tls/cert.pem \
  -days 1 -subj /CN=localhost \
  -addext subjectAltName=DNS:localhost,IP:127.0.0.1
```

```bash
cargo run --no-default-features --features "gateway-webdav,fuse-tokio-runtime" -- \
  gateway webdav --listen 127.0.0.1:19105 \
  --user testuser --password testpass \
  --tls-cert /tmp/brewfs-webdav-tls/cert.pem \
  --tls-key /tmp/brewfs-webdav-tls/key.pem \
  --data-backend local-fs --data-dir /tmp/brewfs-webdav-tls/data \
  --meta-backend sqlx \
  --meta-url "sqlite:///tmp/brewfs-webdav-tls/meta.db?mode=rwc"
```

```bash
GW_ENDPOINT=https://127.0.0.1:19105 GW_INSECURE_TLS=1 \
  python3 scripts/e2e_webdav_gateway.py
```

生产环境不应使用自签名证书；Basic Auth 应通过 HTTPS 传输。

## 7. 已知限制（MVP 范围外）

- DeltaV、CalDAV、CardDAV、DASL/SEARCH、ACL 和多用户授权不在范围内。
- WebDAV 锁使用进程内 `MemLs`，重启丢失，跨网关实例不互斥，也不映射 POSIX/FUSE 锁。
- 进程内路径锁仅用于同一 WebDAV 实例；路径操作使用 parent-inode 解析并拒绝中间 symlink，避免通过路径重解析进入保留命名空间；跨进程条件写仍依赖共享 metadata backend 的原子边界。
- WebDAV 使用与 mount/S3 相同的 flat cache namespace，并按 metadata backend 使用相应的 MetaClient TTL。
- 原子模式保证成功 flush 后发布；直接模式可能在客户端断连或 body 长度错误时留下部分目标内容，适合需要传统直接写回语义的客户端，不适合要求全有或全无发布的场景。
- 原子模式会校验 `Content-Length`、`X-Expected-Entity-Length` 和 `Content-Range` 声明的 body 长度；完全不带长度声明的 chunked 上传目前没有可配置的请求体上限，生产部署应在前置代理限制请求大小。
- staging 清理的 active 集合是进程内状态；多个网关实例共享同一 staging 目录时，应避免并发运行清理任务，或在外部调度时协调实例生命周期。
- 明文 HTTP 的优雅关闭会等待在途请求自然结束；TLS listener 额外提供最长 30 秒的关闭期限。
- dead properties 依赖元数据后端的 xattr 能力。无 xattr 的后端仍提供活属性和基本读写，但不宣告 dead-property 支持，PROPPATCH 不提供持久化 dead properties。
- `dav-server` 的递归目录 COPY 通过 `create_dir` 创建集合，当前 adapter 无法从该回调获得源集合路径，因此文件属性可复制，但源目录的 dead properties 不保证复制。
- `Depth: infinity` 由网关适配 `dav-server` 的 litmus gate 后执行递归遍历；大型树应谨慎使用，并考虑客户端分页/有限深度行为。
- 当前未自动化验证 WebDAV litmus、rclone、davfs2、Windows WebClient、Finder 或其他外部客户端兼容性；上述 E2E 使用 Python 标准库直接发送 HTTP/WebDAV 请求。
- 网关通过 Ctrl-C 执行优雅关闭：停止接收新请求、等待在途请求（TLS listener 最长 30 秒）、停止 staging 清理任务并关闭 MetaClient control plane/runtime。
