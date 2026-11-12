# WebDAV Gateway Spec（网盘接入）

状态：M2 MVP 已实现；真实客户端兼容性（litmus、davfs2、Windows WebClient、Finder、rclone）仍待独立验收
依赖：`dav-server = "0.11"`（feature `gateway-webdav`）
CLI：`brewfs gateway webdav --listen <addr> [--user u --password p | --allow-anonymous] [后端参数...]`

## 1. 目标与非目标

### 目标

- 以 WebDAV（RFC 4918）暴露 BrewFS volume，覆盖"网盘"核心场景：
  - Windows「映射网络驱动器」/ `net use`；
  - macOS Finder「连接服务器」；
  - Linux `davfs2` 内核挂载；
  - rclone、cadaver、各移动端 WebDAV 客户端；
- 协议方法与文件系统语义忠实映射（§3），不丢数据、不产生半文件；
- 与 S3 网关 / FUSE 挂载数据互通（路径即路径，xattr 约定见总设计 §3.2）。

### 非目标

- DeltaV / 版本控制（RFC 3253）、CalDAV/CardDAV；
- 全文搜索（DASL）；
- 多用户权限体系（MVP 单用户 Basic auth）；
- Web 界面（网盘 UI 不属于协议层；console 模块是未来 Web 文件管理器的落点）。

## 2. 架构

```
Windows 网络驱动器 / Finder / davfs2 / rclone
      │ HTTP(S) WebDAV
      ▼
┌──────────────┐  DavFileSystem/DavFile traits  ┌────────────────────┐  parent-inode operations
│ dav-server    │ ─────────────────────────────▶ │ BrewFsDavFs          │ ───────────────────▶ VFS
│ (axum 集成)   │                                │ (src/gateway/webdav)│
└──────────────┘                                └────────────────────┘
        │ lock system: memls (MVP) → VFS plock (后续)
        │ dead props: xattr brewfs.dav.deadprops
```

- dav-server 负责 HTTP 方法解析、PROPFIND XML、lock 语义框架；我们实现
  `DavFileSystem`（路径级操作）与 `DavFile`（读写句柄）两个 trait；
- 基于 VFS 的 parent-inode/path component 操作，拒绝中间 symlink 和内部 namespace 逃逸；条件写使用 ETag/ctime 重新检查。
- flat cache 使用 volume namespace；metadata client 使用 backend-specific TTL。

## 3. 方法映射表

| WebDAV 方法 | BrewFS 映射 | 说明 |
|---|---|---|
| `GET` / `HEAD` | `stat` + 流式 `read_at` | 支持 `Range`；目录 GET 返回 405（dav-server 行为） |
| `PUT` | `create_file` + 流式写 + `flush` | MVP 直接写最终路径；`--atomic-put` 选项切换为 tmp+rename（总设计 §3.3），默认开启见 §5 |
| `MKCOL` | `mkdir`（父不存在 → 409 `Conflict`；已存在 → 405） | |
| `DELETE` | 文件 `unlink`；目录 `remove_dir_all`（递归，RFC 要求） | 递归删除限并发（默认 8，防大目录惊群） |
| `PROPFIND` | depth=0 `stat`；depth=1 `readdir` + bounded concurrent `stat_ino`；depth=infinity 一律拒绝（`501` + `propfind-finite-depth`） | 属性集：`displayname/getcontentlength/getlastmodified/creationdate/resourcetype/getetag`（inode+mtime+ctime+size 合成 etag）+ dead props |
| `PROPPATCH` | dead properties 读写 xattr `brewfs.dav.deadprops`（JSON map） | 活属性（getcontentlength 等）set → 409；xattr 不可用时整请求 507 |
| `COPY` | 流式服务端复制（read→write），目录递归；`Overwrite: F` → create_new | 后续可用 chunk 级克隆优化 |
| `MOVE` | `rename`（同 volume 内恒真）；`Overwrite: F` → `RENAME_NOREPLACE` | |
| `LOCK` / `UNLOCK` | MVP：dav-server memls（进程内存） | 语义与限制见 §4 |
| `OPTIONS` | dav-server 自动应答 | `DAV: 1,2` 头 |

错误码映射（dav-server 已处理骨架，存储层错误按下表汇入）：

| BrewFS 错误 | HTTP |
|---|---|
| NotFound | 404 |
| PermissionDenied | 403 |
| AlreadyExists | 412 / 405（MKCOL） |
| DirectoryNotEmpty | 409（DELETE 非递归时）/ 424 |
| 父路径不存在 | 409 Conflict |
| 空间不足 | 507 Insufficient Storage |
| 其他 | 500 |

## 4. 锁语义

- MVP：`memls`（进程内内存锁，等价 JuiceFS webdav 的 `webdav.NewMemLS()`）。
  明示限制：锁只在单网关实例内有效，且与 FUSE 侧 POSIX lock / 其他网关互不感知；
- 后续：实现 dav-server 的 `DavLockSystem` trait，落到 `/.brewfs.sys/locks/` + meta flock，
  达成跨实例可见的 WebDAV 锁；**不**做 WebDAV lock ↔ POSIX lock 双向映射
  （语义模型不同：WebDAV 锁是路径+token+超时，POSIX 是句柄+字节区间），
  仅在文档中声明两者互不影响；
- davfs2/Windows redirector 会先发 `LOCK`；不支持时部分客户端退化行为差，故 memls 必须可用
  （不能返回 501），这是 dav-server 默认能力，直接启用。

## 5. PUT 的原子性与"半文件"问题

网盘客户端（尤其 Windows redirector 与部分同步工具）的行为特点：

- 先 `PUT` 零字节占位、再 `LOCK`、再多次 `PUT` 覆盖写内容；
- 大文件分段 `PUT`（Content-Range）或直接整传。

约定：

1. 默认 `--atomic-put=true`（原子发布）：成功 flush 后整请求才可见，适合归档类一次性写入；
2. `--atomic-put=false` 时启用直接写最终路径：与依赖创建后分段写、传统同步工具的行为兼容最好，但客户端断连或 body 长度错误时可能留下部分目标内容；
3. 原子模式会保留旧目标直到完整请求成功，且会重新检查 `If-Match`；无论哪种模式，`flush` 失败必须返回错误而不是静默成功；

## 6. 认证与安全

- `--user/--password`（或 env `BREWFS_WEBDAV_USER/BREWFS_WEBDAV_PASSWORD`）启用 HTTP Basic；
  未配置凭据时必须显式传 `--allow-anonymous`，否则启动失败；匿名模式仅限可信网络；
- 非 loopback listener 使用 Basic Auth 时必须同时启用 TLS；loopback 明文 HTTP 仅用于本地验证；
- **Windows 网盘映射的现实约束**：Windows WebClient 默认仅允许 HTTPS 上的 Basic auth
  （否则需改注册表 `BasicAuthLevel=2`）。因此：
  - MVP 提供 `--tls-cert/--tls-key`（rustls）原生 HTTPS —— 这是 Windows 免注册表映射的前提；
  - 使用指南给出 `net use Z: https://host:port/` 与证书信任步骤；
- 后续：Bearer token（复用 console `AuthConfig` 模式）、多用户。

## 7. 各客户端兼容性注意点（写入部署文档）

| 客户端 | 注意点 |
|---|---|
| Windows redirector | 大文件默认限 50MB（注册表 `FileSizeLimitInBytes`）；会先发 OPTIONS/PROPFIND 探测根路径；中文路径需 UTF-8 percent-encoding；HTTPS 是推荐配置 |
| macOS Finder | 会创建 `.DS_Store`、`._*`（AppleDouble）文件；建议文档说明而非过滤 |
| davfs2 | 依赖 LOCK 可用；`use_locks 1` 默认；大文件写入走内核页缓存，close 时才 flush——flush 错误必须如实返回 |
| rclone | 默认 chunked 上传关闭，行为良好；`--vfs-cache-mode writes` 时接近本地盘语义 |

## 8. 性能目标

- GET/PUT 流式，单连接内存有界；
- PROPFIND depth=1 在万级条目目录下 p99 < 500ms（sqlite meta）；
- davfs2 挂载下顺序读写吞吐 ≥ 同机 FUSE 直挂的 70%。

## 9. 测试计划

1. **单元测试**：dead props xattr 编解码、路径解码（percent-encoding、尾斜杠归一）、
   `.brewfs.sys` 在 PROPFIND 中的过滤；
2. **集成测试**（`tests/webdav_gateway_test.rs`）：进程内起服务 + `reqwest` 手工构造
   PROPFIND/PUT/MKCOL/MOVE/COPY/LOCK 请求断言；
3. **litmus 测试**（e2e 脚本）：`litmus http://host:port/ user pass`，basic/copymove/props/locks 四组全过；
4. **真实客户端冒烟**（WSL/手动）：davfs2 挂载读写、rclone copy、Windows 映射网络驱动器拖放文件；
5. **交叉一致性**：WebDAV 写入 → S3 GET / FUSE 读取验证。

## 10. CLI 参数

```
brewfs gateway webdav \
  --listen 0.0.0.0:9001 \
  [--user u --password p | --allow-anonymous] \
  [--tls-cert c.pem --tls-key k.pem] \
  [--atomic-put true|false] \
  <与 mount 相同的后端参数>
```

## 11. 里程碑

- **M2（实现完成）**：§3 全表 + memls + Basic auth + TLS + 原子 PUT/PATCH + dead properties；本地协议 E2E 已通过。
- **M2 外部客户端验收（待完成）**：litmus basic/copymove/props/locks、davfs2 挂载读写、Windows 网络驱动器、Finder 和 rclone 冒烟；当前仅有协议级 Python E2E，不能替代这些客户端验收。
- **后续**：分布式锁、Bearer token、多用户和 Web 文件管理器（console 侧）。

## 12. 参考

- JuiceFS webdav：`pkg/fs/http.go`（davFS over pkg/fs、MemLS、dead props in xattr、
  Basic auth、litmus 兼容）；
- dav-server crate：<https://crates.io/crates/dav-server>；
- RFC 4918；litmus <http://www.webdav.org/neon/litmus/>。
