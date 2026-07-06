# 二进制部署与安装

本文面向不从源码仓库直接运行的部署方式：下载或构建 `brewfs` 二进制，然后用 systemd、手工命令或容器运行挂载进程。

## 支持范围

- 操作系统：Linux。
- 架构：release installer 目前按 `linux-amd64` 和 `linux-arm64` 解析二进制。
- 运行依赖：FUSE 3、`/dev/fuse`、对象存储、元数据后端。
- 推荐单机栈：Redis 作为元数据后端，RustFS 作为 S3 兼容对象存储。

Debian/Ubuntu 常用依赖：

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl fuse3 util-linux
```

RHEL/CentOS/Fedora 常用依赖：

```bash
sudo dnf install -y ca-certificates curl fuse3 util-linux
```

非特权挂载需要 `fusermount3`；systemd/root 部署通常直接使用 `--privileged` 访问 `/dev/fuse`。

## 方式一：单机 installer

仓库提供 `scripts/install_brewfs_single_node.sh`，会安装并维护三个 systemd 服务：

| 服务 | 作用 |
|---|---|
| `brewfs-redis.service` | Redis 元数据服务。 |
| `brewfs-rustfs.service` | RustFS S3 兼容对象存储。 |
| `brewfs.service` | BrewFS FUSE 挂载进程。 |

默认路径：

| 路径 | 说明 |
|---|---|
| `/usr/local/bin/brewfs` | BrewFS 二进制。 |
| `/usr/local/bin/rustfs` | RustFS 二进制。 |
| `/etc/brewfs/mount.yaml` | BrewFS mount 配置。 |
| `/etc/default/brewfs*` | systemd 环境变量文件。 |
| `/var/lib/brewfs` | Redis/RustFS/BrewFS 状态目录。 |
| `/var/lib/brewfs/cache` | BrewFS 本地缓存目录。 |
| `/var/log/brewfs` | 服务日志。 |
| `/mnt/brewfs` | 默认挂载点。 |

安装前需要满足下面任一条件：

- 机器上已有 `redis-server`。
- 或设置 `REDIS_DOWNLOAD_URL` 指向 redis-server 二进制或压缩包。

同时需要满足下面任一条件：

- `/usr/local/bin/rustfs` 已存在且可执行。
- 或设置 `RUSTFS_DOWNLOAD_URL` 指向 rustfs 二进制或压缩包。

安装示例：

```bash
sudo BREWFS_VERSION=v0.1.1 \
  RUSTFS_DOWNLOAD_URL=https://download.example.invalid/rustfs-linux-amd64 \
  scripts/install_brewfs_single_node.sh install
```

如果已经把 RustFS 放在 `/usr/local/bin/rustfs`：

```bash
sudo BREWFS_VERSION=v0.1.1 \
  scripts/install_brewfs_single_node.sh install
```

installer 会从 GitHub release 检测最新 BrewFS 版本；如果环境不能访问 GitHub，显式传 `--version` 或设置 `BREWFS_VERSION`：

```bash
sudo scripts/install_brewfs_single_node.sh --version v0.1.1 install
```

tag release 的 GitHub Actions 会把每个产物的下载链接写入 workflow summary。链接规则是：

```text
https://download.brewfs.ai/brewfs/releases/<version>/brewfs-<os>-<arch>
https://download.brewfs.ai/brewfs/releases/<version>/brewfs-<os>-<arch>.sha256
```

Linux installer 会自动选择 `brewfs-linux-amd64` 或 `brewfs-linux-arm64`。

也可以完全指定下载地址：

```bash
sudo BREWFS_DOWNLOAD_URL=https://download.brewfs.ai/brewfs/releases/v0.1.1/brewfs-linux-amd64 \
  RUSTFS_DOWNLOAD_URL=https://download.example.invalid/rustfs-linux-amd64 \
  scripts/install_brewfs_single_node.sh install
```

常用环境覆盖：

| 变量 | 默认值 | 说明 |
|---|---:|---|
| `BREWFS_VERSION` | 最新 release | BrewFS release tag，可写 `v0.1.1` 或 `0.1.1`。 |
| `BREWFS_BASE_URL` | `https://download.brewfs.ai/brewfs/releases` | BrewFS release mirror。 |
| `BREWFS_DOWNLOAD_URL` | 自动拼接 | 显式 BrewFS 二进制或压缩包 URL。 |
| `BREWFS_REQUIRE_CHECKSUM` | `0` | 设为 `1` 时要求 `<url>.sha256` 存在且匹配。 |
| `RUSTFS_DOWNLOAD_URL` | 空 | RustFS 二进制或压缩包 URL。 |
| `REDIS_DOWNLOAD_URL` | 空 | redis-server 二进制或压缩包 URL。 |
| `MOUNT_POINT` | `/mnt/brewfs` | 挂载点。 |
| `STATE_DIR` | `/var/lib/brewfs` | 状态目录。 |
| `LOG_DIR` | `/var/log/brewfs` | 日志目录。 |
| `BREWFS_BUCKET` | `brewfs-data` | RustFS/S3 bucket。 |
| `REDIS_PORT` | `6379` | Redis 端口。 |
| `RUSTFS_PORT` | `9000` | RustFS S3 API 端口。 |
| `RUSTFS_CONSOLE_PORT` | `9001` | RustFS console 端口。 |
| `RUSTFS_ACCESS_KEY` | `rustfsadmin` | RustFS access key。生产环境必须修改。 |
| `RUSTFS_SECRET_KEY` | `rustfsadmin` | RustFS secret key。生产环境必须修改。 |
| `BREWFS_S3_PART_SIZE` | `16777216` | BrewFS S3 multipart part 大小。 |
| `BREWFS_S3_MAX_CONCURRENCY` | `32` | BrewFS S3 multipart 最大并发。 |
| `BREWFS_S3_FORCE_PATH_STYLE` | `true` | S3 path-style 访问。 |
| `BREWFS_S3_DISABLE_PAYLOAD_CHECKSUM` | `true` | 禁用 S3 payload checksum。 |
| `BREWFS_META_OPEN_FILE_CACHE_TTL_MS` | `30000` | open file attr cache TTL。 |
| `BREWFS_META_OPEN_FILE_CACHE_CAPACITY` | `65536` | open file attr cache 容量。 |
| `BREWFS_WRITEBACK_MODE` | `commit_before_upload` | 可改为 `upload_before_commit` 获得更保守的崩溃语义。 |
| `BREWFS_WRITEBACK_PERSIST_SYNC` | `false` | 是否同步落盘本地 writeback stage。 |
| `BREWFS_CACHE_TTL_MS` | `1000` | FUSE attr/entry TTL，单位 ms。 |
| `BREWFS_FUSE_WORKERS` | `1` | FUSE worker 数。 |
| `BREWFS_FUSE_MAX_BACKGROUND` | `512` | FUSE 最大 in-flight 请求数。 |
| `BREWFS_FUSE_PRIVILEGED` | `true` | 生成 `fuse.privileged`。 |
| `BREWFS_LOG_LEVEL` | `RUST_LOG` 或 `brewfs=info` | 写入 systemd 环境文件里的 `RUST_LOG`。 |
| `BREWFS_USER` | `root` | systemd 服务用户。 |
| `BREWFS_GROUP` | `root` | systemd 服务组。 |

installer 生成的 BrewFS 配置使用：

```yaml
data:
  backend: s3
  s3:
    part_size: 16777216
    max_concurrency: 32
    force_path_style: true
    disable_payload_checksum: true
meta:
  backend: redis
  open_file_cache_ttl_ms: 30000
  open_file_cache_capacity: 65536
cache:
  writeback_mode: commit_before_upload
  writeback_persist_sync: false
fuse:
  workers: 1
  max_background: 512
  privileged: true
```

这是面向单机 Redis + RustFS 的高吞吐配置。`commit_before_upload` 会先发布元数据再异步上传对象，依赖本机缓存盘可靠性；如果更关注崩溃后一致性，请把 `/etc/brewfs/mount.yaml` 改为：

```yaml
cache:
  writeback_mode: upload_before_commit
  writeback_persist_sync: true
```

修改后重启：

```bash
sudo systemctl restart brewfs.service
```

维护命令：

```bash
sudo scripts/install_brewfs_single_node.sh status
sudo scripts/install_brewfs_single_node.sh restart
sudo scripts/install_brewfs_single_node.sh upgrade
sudo scripts/install_brewfs_single_node.sh uninstall
```

`uninstall` 只删除 unit 和配置文件，保留 `/var/lib/brewfs` 和 `/var/log/brewfs`。

## 方式二：手工安装二进制

从源码构建：

```bash
cargo build -p brewfs --release
sudo install -m 0755 target/release/brewfs /usr/local/bin/brewfs
```

如果用于容器镜像上下文，可运行：

```bash
bash docker/build_brewfs_host_binary.sh
```

该脚本会构建 release 二进制，strip 后同步到 `target/docker/brewfs`。

创建目录：

```bash
sudo install -d -m 0755 /etc/brewfs /var/lib/brewfs/data /var/cache/brewfs /var/log/brewfs /mnt/brewfs
```

写入本地 SQLite 配置 `/etc/brewfs/mount.yaml`：

```yaml
mount_point: /mnt/brewfs

data:
  backend: local-fs
  localfs:
    data_dir: /var/lib/brewfs/data

meta:
  backend: sqlx
  sqlx:
    url: "sqlite:///var/lib/brewfs/meta.db?mode=rwc"

cache:
  root: /var/cache/brewfs

fuse:
  privileged: true
```

前台验证：

```bash
sudo RUST_LOG=brewfs=info brewfs mount --config /etc/brewfs/mount.yaml
```

另一个终端检查：

```bash
mount | grep /mnt/brewfs
brewfs info /mnt/brewfs
```

卸载：

```bash
sudo fusermount3 -u /mnt/brewfs || sudo umount /mnt/brewfs
```

## 手工 systemd unit

写入 `/etc/systemd/system/brewfs.service`：

```ini
[Unit]
Description=BrewFS Mount
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
Environment=RUST_LOG=brewfs=info
Environment=BREWFS_LOG_FILE=/var/log/brewfs/brewfs.log
ExecStart=/usr/local/bin/brewfs mount --config /etc/brewfs/mount.yaml
ExecStop=/bin/sh -c 'if command -v fusermount3 >/dev/null 2>&1; then fusermount3 -u /mnt/brewfs; else umount /mnt/brewfs; fi'
Restart=always
RestartSec=5s
LimitNOFILE=1048576
TasksMax=infinity
ReadWritePaths=/var/lib/brewfs /var/cache/brewfs /var/log/brewfs /mnt/brewfs

[Install]
WantedBy=multi-user.target
```

启用：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now brewfs.service
sudo systemctl status brewfs.service --no-pager
```

日志：

```bash
sudo journalctl -u brewfs.service -f
sudo tail -f /var/log/brewfs/brewfs.log
```

## Redis + S3 手工配置

Redis + RustFS/MinIO/S3 配置示例：

```yaml
mount_point: /mnt/brewfs

data:
  backend: s3
  s3:
    bucket: brewfs-data
    endpoint: http://127.0.0.1:9000
    region: us-east-1
    force_path_style: true
    disable_payload_checksum: true
    part_size: 16777216
    max_concurrency: 32

meta:
  backend: redis
  redis:
    url: "redis://127.0.0.1:6379/0"
  open_file_cache_ttl_ms: 30000
  open_file_cache_capacity: 65536

cache:
  root: /var/cache/brewfs
  writeback_mode: upload_before_commit

fuse:
  privileged: true
```

systemd 环境文件 `/etc/default/brewfs` 可放 S3 凭据：

```bash
AWS_ACCESS_KEY_ID=rustfsadmin
AWS_SECRET_ACCESS_KEY=rustfsadmin
AWS_DEFAULT_REGION=us-east-1
AWS_EC2_METADATA_DISABLED=true
RUST_LOG=brewfs=info
BREWFS_LOG_FILE=/var/log/brewfs/brewfs.log
```

然后在 unit 中加入：

```ini
EnvironmentFile=/etc/default/brewfs
```

## 容器运行

构建 runtime 镜像：

```bash
docker build -f docker/Dockerfile.runtime -t brewfs:runtime .
```

容器内挂载需要 `/dev/fuse` 和合适权限，通常至少需要：

```bash
docker run --rm -it \
  --device /dev/fuse \
  --cap-add SYS_ADMIN \
  --security-opt apparmor=unconfined \
  -e BREWFS_DATA_BACKEND=local-fs \
  -e BREWFS_META_BACKEND=sqlite \
  -v /var/lib/brewfs:/var/lib/brewfs \
  -v /mnt/brewfs:/mnt/brewfs:rshared \
  brewfs:runtime
```

容器 entrypoint 会根据 `BREWFS_*` 环境变量生成 `/run/brewfs/config.yaml` 并执行：

```bash
brewfs mount --privileged --config /run/brewfs/config.yaml /mnt/brewfs
```

## 升级与回滚

systemd 部署建议保留上一个二进制：

```bash
sudo cp -a /usr/local/bin/brewfs /usr/local/bin/brewfs.prev
sudo install -m 0755 ./brewfs-linux-amd64 /usr/local/bin/brewfs
sudo systemctl restart brewfs.service
```

回滚：

```bash
sudo install -m 0755 /usr/local/bin/brewfs.prev /usr/local/bin/brewfs
sudo systemctl restart brewfs.service
```

升级后检查：

```bash
brewfs --version
brewfs info /mnt/brewfs
cat /mnt/brewfs/.stats 2>/dev/null || true
```

## 常见问题

| 现象 | 处理 |
|---|---|
| `fusermount3: command not found` | 安装 `fuse3`，或使用 root/systemd 加 `--privileged`。 |
| `permission denied: /dev/fuse` | 检查 `/dev/fuse` 权限、容器 `--device /dev/fuse`、systemd service 用户。 |
| `mount point must be a directory` | 创建挂载目录并确保不是普通文件。 |
| S3 bucket 不存在 | 先用对象存储工具创建 bucket；installer 在有 AWS CLI 时会尝试自动创建。 |
| Redis 连接失败 | 检查 `meta.redis.url`、Redis bind/protected-mode、防火墙和 systemd 启动顺序。 |
| `mmap` 或文件访问报 `No such device` | 通常表示 FUSE 挂载进程已退出或 mount 已失效；先 `fusermount3 -u /mnt/brewfs`，再检查日志并重新挂载。 |
| 读到旧属性或目录项 | 默认 FUSE attr/entry TTL 为 1s；强一致调试可设置 `BREWFS_CACHE_TTL_MS=0` 后重启。 |

更完整的配置项见 [configuration.md](configuration.md)。
