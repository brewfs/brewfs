# WebDAV 网关使用指南

本文面向部署 BrewFS WebDAV 网关的用户。协议设计细节见
[WebDAV Gateway Spec](../protocols/webdav.md)，可复现的开发者验证步骤见
[WebDAV Gateway Verification](../protocols/webdav-gateway-verification.md)。

WebDAV 网关直接访问 BrewFS 的 metadata/data backend，**不需要先挂载 FUSE**。
同一个 volume 也可以由 FUSE 或 S3 gateway 访问，但应阅读本文的缓存、锁和并发限制。

## 1. 支持范围

当前 MVP 是单用户 WebDAV Class 2 网关，支持：

- Basic Auth，或显式启用的匿名读写；
- 原生 HTTP 或 PEM 证书 HTTPS；
- GET、HEAD、Range GET、PUT、PATCH；
- MKCOL、递归 DELETE、Depth 0/1 PROPFIND（`Depth: infinity` 一律拒绝，返回 501）；
- PROPPATCH dead properties；
- COPY、MOVE、Overwrite `T/F`；
- LOCK、UNLOCK 和 lock-null resource；
- 原子 PUT/PATCH（默认开启）和 direct-write 模式；
- LocalFS 或 S3-compatible data backend，以及 SQLite、PostgreSQL、Redis、Etcd、TiKV metadata backend（具体能力取决于 backend）。

不在 MVP 范围内：DeltaV、CalDAV、CardDAV、DASL/SEARCH、ACL、多用户授权、Bearer
Token、mTLS 授权模型和跨网关实例 WebDAV lock 协调。

## 2. 快速开始：LocalFS + SQLite

### 2.1 从源码构建

默认 feature 已包含 S3 和 WebDAV。只构建 WebDAV 时，必须显式启用一个 asyncfuse
runtime feature：

```bash
cargo build --release --no-default-features \
  --features "gateway-webdav,fuse-tokio-runtime"
```

如果 WSL 中设置了不可用的代理，先清理代理变量：

```bash
unset http_proxy https_proxy all_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY
```

### 2.2 启动一个本地实例

下面的命令使用 LocalFS 保存数据、SQLite 保存 metadata，并启用认证和原子写入：

```bash
mkdir -p /var/lib/brewfs/webdav/data

brewfs gateway webdav \
  --listen 127.0.0.1:9001 \
  --user testuser \
  --password testpass \
  --data-backend local-fs \
  --data-dir /var/lib/brewfs/webdav/data \
  --meta-backend sqlx \
  --meta-url "sqlite:///var/lib/brewfs/webdav/meta.db?mode=rwc"
```

本地验证也可以使用临时目录和端口：

```bash
mkdir -p /tmp/brewfs-webdav/data

cargo run --no-default-features \
  --features "gateway-webdav,fuse-tokio-runtime" -- \
  gateway webdav \
  --listen 127.0.0.1:19102 \
  --user testuser --password testpass \
  --data-backend local-fs \
  --data-dir /tmp/brewfs-webdav/data \
  --meta-backend sqlx \
  --meta-url "sqlite:///tmp/brewfs-webdav/meta.db?mode=rwc"
```

看到以下日志之一表示 listener 已开始监听：

```text
brewfs webdav gateway listening
brewfs webdav gateway listening with TLS
```

### 2.3 第一个请求

使用 `curl` 检查认证、创建目录、上传和读取：

```bash
curl -i -u testuser:testpass -X OPTIONS http://127.0.0.1:9001/
```

```bash
curl -i -u testuser:testpass -X MKCOL http://127.0.0.1:9001/docs/
```

```bash
curl -i -u testuser:testpass --upload-file ./README.md \
  http://127.0.0.1:9001/docs/README.md
```

```bash
curl -i -u testuser:testpass \
  http://127.0.0.1:9001/docs/README.md
```

URL 根目录建议保留结尾 `/`。路径中的非 ASCII 字符应使用 URL percent-encoding；
`dav-server` 会负责标准 URL 解码和规范化。

## 3. 认证、监听和 TLS

### 3.1 Basic Auth

默认必须同时提供非空用户名和密码：

```bash
--user testuser --password testpass
```

也可以使用环境变量，避免把密码直接写入 shell history：

```bash
export BREWFS_WEBDAV_USER=testuser
export BREWFS_WEBDAV_PASSWORD='change-this-password'
```

用户名和密码必须成对出现；不能只提供其中一个。未提供凭据时，必须显式传入
`--allow-anonymous`，否则网关在监听前失败：

```bash
brewfs gateway webdav \
  --allow-anonymous \
  --listen 127.0.0.1:9001 \
  ...
```

匿名模式是无授权的读写访问，只适合本机或完全隔离的可信网络。

### 3.2 监听地址安全规则

- 默认监听地址是 `0.0.0.0:9001`；
- 带 Basic Auth 的非 loopback 明文 HTTP 会被拒绝，必须同时配置 TLS；
- loopback 明文 HTTP 仅适合本地开发和验证；
- 生产部署建议使用 HTTPS，或在可信反向代理后运行 HTTP，并限制网关监听地址。

### 3.3 原生 HTTPS

证书和私钥必须成对提供：

```bash
export BREWFS_WEBDAV_TLS_CERT=/etc/brewfs/tls/fullchain.pem
export BREWFS_WEBDAV_TLS_KEY=/etc/brewfs/tls/private.key

brewfs gateway webdav \
  --listen 0.0.0.0:9001 \
  --user testuser --password 'change-this-password' \
  --tls-cert "$BREWFS_WEBDAV_TLS_CERT" \
  --tls-key "$BREWFS_WEBDAV_TLS_KEY" \
  ...
```

证书的 SAN 必须包含客户端实际使用的 DNS 名称或 IP。私钥应只允许服务账号读取，
并通过证书续期工具在更新后重启或平滑重启网关。

诊断 TLS 握手：

```bash
openssl s_client -connect webdav.example.com:9001 \
  -servername webdav.example.com -showcerts
```

自签名证书只适合测试。客户端需要显式信任测试 CA；不要把
`GW_INSECURE_TLS=1` 或等效的跳过证书校验设置用于生产。

## 4. 配置参考

### 4.1 WebDAV 专属参数

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `--listen ADDR` | `0.0.0.0:9001` | HTTP/HTTPS 监听地址 |
| `--user USER` | 无 | Basic Auth 用户名，也可用 `BREWFS_WEBDAV_USER` |
| `--password PASSWORD` | 无 | Basic Auth 密码，也可用 `BREWFS_WEBDAV_PASSWORD` |
| `--allow-anonymous` | `false` | 显式允许匿名读写，不能和用户名/密码组合 |
| `--atomic-put true\|false` | `true` | 成功 flush 后再发布 PUT/PATCH；`false` 为直接写回 |
| `--tls-cert FILE` | 无 | HTTPS PEM 证书链，也可用 `BREWFS_WEBDAV_TLS_CERT` |
| `--tls-key FILE` | 无 | HTTPS PEM 私钥，也可用 `BREWFS_WEBDAV_TLS_KEY` |

查看当前二进制实际接受的参数：

```bash
brewfs gateway webdav --help
```

### 4.2 共享 volume 参数

WebDAV 的 volume 参数与 `brewfs mount` 共用同一组定义，常用参数包括：

- `--data-backend local-fs|s3`；
- `--data-dir PATH`（LocalFS）；
- `--s3-bucket`、`--s3-endpoint`、`--s3-region`、`--s3-part-size` 等 S3 参数；
- `--meta-backend sqlx|redis|etcd|tikv`；
- `--meta-url`、`--meta-etcd-urls`、`--meta-tikv-pd-endpoints` 等 metadata 参数；
- `--block-size`、`--chunk-size`、缓存和压缩参数；
- `--config FILE` 以及 `mount` 支持的其他公共 volume 参数。

使用 `--config` 时，配置文件提供公共 volume/backend 设置；WebDAV 用户、密码、TLS
证书等网关专属设置仍应使用对应 flag 或环境变量。WebDAV MVP 只支持 `flat` volume，
不支持 `workspace`。

启动多个前端访问同一 volume 时，必须使用完全一致的 backend identity。WebDAV 当前会
使用与 mount/S3 相同的 flat cache namespace，并按 metadata backend 使用对应的
MetaClient TTL。

## 5. 客户端连接示例

### 5.1 curl：诊断和脚本化操作

```bash
curl --fail-with-body -u testuser:testpass \
  -X PROPFIND -H 'Depth: 1' \
  -H 'Content-Type: application/xml' \
  --data '<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:allprop/></D:propfind>' \
  https://webdav.example.com:9001/docs/
```

Range 读取：

```bash
curl -i -u testuser:testpass \
  -H 'Range: bytes=0-1023' \
  https://webdav.example.com:9001/docs/large.bin
```

### 5.2 davfs2（Linux）

安装客户端：

```bash
sudo apt-get install davfs2
sudo mkdir -p /mnt/brewfs-webdav
sudo sh -c 'printf "%s %s\\n" "https://webdav.example.com:9001" "testuser" >> /etc/davfs2/secrets'
sudo chmod 600 /etc/davfs2/secrets
```

更安全的做法是把密码作为 secrets 文件的第三列：

```text
https://webdav.example.com:9001 testuser change-this-password
```

挂载和卸载：

```bash
sudo mount -t davfs https://webdav.example.com:9001 /mnt/brewfs-webdav
sudo sh -c 'printf "use_locks 1\\n" >> /etc/davfs2/davfs2.conf'
sudo umount /mnt/brewfs-webdav
```

davfs2 可能在 close 时才提交写入；网关会传播 flush 错误。本文档没有把 davfs2
列为已自动化验证客户端，建议先用小文件手动 smoke test。

### 5.3 rclone

创建 remote：

```bash
rclone config create brewfs-webdav webdav \
  url https://webdav.example.com:9001 \
  vendor other \
  user testuser \
  pass 'change-this-password'
```

复制文件：

```bash
rclone copy ./local-dir brewfs-webdav:/docs --progress
```

挂载时建议使用写缓存：

```bash
rclone mount brewfs-webdav:/ /mnt/brewfs-rclone \
  --vfs-cache-mode writes
```

rclone、davfs2、litmus、Finder 和 Windows WebClient 尚未纳入本项目自动化 E2E；
使用前请在目标平台完成小文件、重命名、断线恢复和大文件测试。

### 5.4 Windows WebClient

使用 HTTPS 时，可以在资源管理器中选择“映射网络驱动器”，或在管理员终端执行：

```powershell
net use Z: https://webdav.example.com:9001/ /user:testuser *
```

断开：

```powershell
net use Z: /delete
```

Windows WebClient 对 Basic Auth、证书信任和单文件大小有系统限制。常见部署需要：

- 使用受系统信任的 HTTPS 证书；
- 确认 WebClient 服务已启动；
- 留意 Windows 默认约 50 MiB 的 `FileSizeLimitInBytes` 限制；
- 只在明确隔离的环境中考虑修改 Basic Auth 注册表策略，优先使用 HTTPS 而不是放宽明文认证。

Windows WebClient 尚未在本项目 CI 中自动化验证。

### 5.5 macOS Finder

Finder 中选择“前往 → 连接服务器”，输入：

```text
https://webdav.example.com:9001/
```

然后输入 Basic Auth 凭据。Finder 可能创建 `.DS_Store`、`._*` 和 AppleDouble 文件；
网关不会把这些普通用户文件自动过滤掉。

## 6. systemd 部署示例

创建 `/etc/brewfs/webdav.env`，并限制权限：

```bash
sudo install -o root -g brewfs -m 0640 /dev/null /etc/brewfs/webdav.env
sudoedit /etc/brewfs/webdav.env
```

示例环境文件：

```text
BREWFS_WEBDAV_USER=testuser
BREWFS_WEBDAV_PASSWORD=change-this-password
BREWFS_WEBDAV_TLS_CERT=/etc/brewfs/tls/fullchain.pem
BREWFS_WEBDAV_TLS_KEY=/etc/brewfs/tls/private.key
RUST_LOG=brewfs=info
```

创建 `/etc/systemd/system/brewfs-webdav.service`：

```ini
[Unit]
Description=BrewFS WebDAV gateway
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=brewfs
Group=brewfs
EnvironmentFile=/etc/brewfs/webdav.env
ExecStart=/usr/local/bin/brewfs gateway webdav \
  --listen 0.0.0.0:9001 \
  --tls-cert ${BREWFS_WEBDAV_TLS_CERT} \
  --tls-key ${BREWFS_WEBDAV_TLS_KEY} \
  --data-backend local-fs \
  --data-dir /var/lib/brewfs/webdav/data \
  --meta-backend sqlx \
  --meta-url sqlite:///var/lib/brewfs/webdav/meta.db?mode=rwc
Restart=on-failure
RestartSec=5
LimitNOFILE=1048576
ReadWritePaths=/var/lib/brewfs/webdav

[Install]
WantedBy=multi-user.target
```

启用、查看和重启：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now brewfs-webdav
sudo systemctl status brewfs-webdav
sudo journalctl -u brewfs-webdav -f
sudo systemctl restart brewfs-webdav
```

服务账号必须能读写 data、metadata、cache 和 staging 所在目录，并且只能读取 TLS
私钥。防火墙只应开放实际需要的 TCP 端口。

## 7. 健康检查和排障

### 7.1 健康检查

WebDAV 没有独立的 health API。使用带认证的 OPTIONS 作为 readiness 检查：

```bash
curl --fail -u testuser:testpass -X OPTIONS \
  -D - -o /dev/null https://127.0.0.1:9001/
```

响应应包含 `DAV: 1,2,3,sabredav-partialupdate`（dav-server 默认头）和资源对应的 `Allow` 方法列表。

### 7.2 常见启动问题

| 现象 | 常见原因和处理 |
|---|---|
| `unrecognized subcommand 'webdav'` | 使用了不含 `gateway-webdav` feature 的二进制；重新构建或使用默认 features |
| 缺少凭据错误 | 带认证启动时同时提供 `--user` 和 `--password`；匿名场景显式加 `--allow-anonymous` |
| TLS certificate/key 不完整 | `--tls-cert` 和 `--tls-key` 必须成对提供 |
| 非 loopback HTTP 被拒绝 | Basic Auth 的非 loopback listener 必须启用 TLS |
| `workspace` 被拒绝 | 当前 WebDAV MVP 只支持 `flat` |
| `address already in use` | 检查端口占用：`ss -ltnp | grep 9001`，或更换 `--listen` |
| TLS 握手失败 | 检查证书 SAN、私钥权限、证书链和客户端 CA 信任；用 `openssl s_client` 诊断 |

### 7.3 常见 HTTP 状态码

- `401 Unauthorized`：缺少或错误的 Basic Auth；检查用户名、密码和 Authorization header。
- `403 Forbidden`：权限不足、根目录 mutation，或访问内部 `/.brewfs.sys` namespace。
- `404 Not Found`：路径不存在，或 parent component 不存在。
- `405 Method Not Allowed`：方法不适用于资源类型，或资源已存在的特定 MKCOL/条件场景。
- `409 Conflict`：parent 不存在、目录非空删除或 collection 冲突。
- `412 Precondition Failed`：`Overwrite: F`、stale `If-Match` 或其他条件失败。
- `423 Locked`：资源被 WebDAV lock 保护且请求未携带 token。
- `507 Insufficient Storage`：metadata xattr 不支持 dead properties，或 backend 空间/配额不足。
- `500 Internal Server Error`：VFS、metadata、flush 或数据 backend 错误；查看 gateway 日志。

### 7.4 数据和客户端问题

- `PROPPATCH` 在无 xattr backend 上返回 `507` 是预期能力限制；普通读写仍可工作。
- 默认原子写入在 body 校验或 flush 失败时保留旧目标；`--atomic-put false` 可能留下部分目标内容。
- 完全没有长度声明的 chunked 上传没有网关级可配置上限，生产环境应在反向代理限制请求体大小。
- `Depth: infinity` 的 PROPFIND/REPORT 会被网关直接拒绝（`501 Not Implemented`，`propfind-finite-depth` 错误体，RFC 4918 §9.1 允许），请使用 Depth 0/1。
- metadata cache 提供 close-to-open 语义；其他协议刚写入的数据可能在 TTL 内短暂不可见。
- WebDAV lock 只在当前进程有效，重启、另一个 gateway 实例和 FUSE/POSIX lock 不共享状态。

## 8. 生产限制和兼容性声明

- 只有 `flat` volume；不支持 workspace overlay。
- Basic Auth 是单用户模型；没有 Bearer、ACL、多用户授权或 mTLS 身份映射。
- dead properties 依赖 metadata backend 的 xattr capability。
- 递归 directory COPY 无法从 dav-server callback 得到源 collection 路径，因此源目录 dead properties 不保证复制。
- `.brewfs.sys` 是内部 namespace，不应由客户端直接访问。
- staging active 集合是进程内状态；多个实例共享 staging 目录需要外部协调。
- 当前未自动化验证 WebDAV litmus、rclone、davfs2、Windows WebClient 或 Finder。
- 文档中的性能目标是规划目标，不是所有客户端和 backend 组合的验收承诺。

## 9. 可复现验证

开发者和发布前验证请执行：

```bash
python3 scripts/e2e_webdav_gateway.py
```

完整 WSL 构建、HTTP/HTTPS 启动、环境变量、测试结果和已知限制见
[WebDAV Gateway Verification](../protocols/webdav-gateway-verification.md)。
