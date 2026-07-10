<div align="center">
	<img src="doc/icon.png" alt="BrewFS icon" width="96" height="96" />
</div>

<h1 align="center">BrewFS</h1>
<p align="center"><strong>高性能 Rust &amp; 层感知分布式文件系统</strong></p>
<p align="center"><a href="README.md">English</a> | <a href="README_CN.md"><b>中文</b></a></p>

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](../LICENSE)
[![Rust](https://img.shields.io/badge/language-Rust-orange.svg)](https://www.rust-lang.org/)


## ✨ 项目概览

BrewFS 是一个使用 Rust 构建、面向容器、AI 与对象存储密集场景的分布式文件系统。它采用 chunk/block 的数据布局，支持 LocalFS 与 S3 兼容对象存储后端，并通过可插拔事务元数据后端维护命名空间与 slice 信息。

核心理念：计算与存储解耦。应用通过 POSIX 风格接口访问数据，由 BrewFS 负责 chunk 布局、对象 IO、缓存、元数据事务、compaction 与 GC。

BrewFS 不是 JuiceFS fork，但 JuiceFS 是当前最重要的生产级参考基线。性能优化和差距分析会持续对照 JuiceFS 的元数据缓存、读写缓存、writeback 语义、对象存储放大、compaction 与测试覆盖，避免只在单一场景里“看起来变快”。

BrewFS 与 [RustFS 社区](https://github.com/rustfs/rustfs) 共建生态，旨在更好地服务 RustFS 等对象存储场景下的分布式数据与 POSIX 语义交付。

<p align="center">
  <a href="https://github.com/rustfs/rustfs">
    <img src="https://github.com/rustfs.png" alt="RustFS" width="220" height="220" />
  </a>
  <a href="https://github.com/rustfs/rustfs">
    <img src="https://img.shields.io/github/stars/rustfs/rustfs?style=flat-square" alt="RustFS GitHub stars" />
  </a>
</p>

## 🖼 架构

组件概览：
- chunk：ChunkLayout、ChunkReader/Writer，负责将文件偏移映射到 chunk/block，处理跨块 IO 与洞零填充；
- cadapter：对象后端抽象与实现，支持 LocalFS 与 S3 兼容服务；
- meta：元数据客户端、事务后端、session、控制面、compaction hook 与 GC 元数据；
- fs：基于路径的 FileSystem（mkdir/mkdir_all/create/read/write/readdir/stat/unlink/rmdir/rename/truncate）；
- vfs：面向 FUSE 的 inode-based VFS；
- sdk：面向应用的轻量客户端封装（基于 FileSystem，提供 LocalClient 便捷构造）。

## 🚀 快速开始

### 环境要求

- Rust: >= 1.85.0
- 操作系统：Linux (Ubuntu 20.04+, CentOS 8+)

### 单机二进制安装

在一台 Linux 机器上，可以用一个脚本安装并由 systemd 维护完整单机栈：
Redis 作为元数据服务，RustFS 作为 S3 兼容对象存储，BrewFS 通过 FUSE 挂载。

当前分支合并到 main 后，一行安装命令如下：

```bash
curl -fsSL https://raw.githubusercontent.com/brewfs/brewfs/main/scripts/install_brewfs_single_node.sh | sudo bash -s -- install
```

常用覆盖参数：

```bash
curl -fsSL https://raw.githubusercontent.com/brewfs/brewfs/main/scripts/install_brewfs_single_node.sh \
  | sudo env MOUNT_POINT=/mnt/brewfs BREWFS_TUNING_PROFILE=balanced bash -s -- install
```

挂载点必须为空。脚本会生成并启用 `brewfs.service`、`brewfs-redis.service`
和 `brewfs-rustfs.service`。状态检查：

```bash
sudo systemctl status brewfs.service
```

RustFS 会使用 `https://rustfs.com/install_rustfs.sh` 对应的官方二进制镜像源安装；
BrewFS 脚本自己生成并维护 `brewfs-rustfs.service`，避免和独立 `rustfs.service`
抢占端口或生命周期。

更多安装、升级、卸载和调优参数见
`doc/operations/configuration.md#single-node-installer`。

### 从源码运行 SDK demo

```bash
cargo run -q --bin sdk_demo -- /tmp/brewfs-objroot
```
示例将会：
- 创建多级目录/文件，进行跨 block/chunk 写入与读回校验；
- 执行重命名、截断（收缩/扩展）、列目录与删除；
- 打印预期错误场景，并输出 "sdk demo: OK"。

### FUSE 挂载并发配置

`brewfs mount` 默认会启用 `asyncfuse` 的 worker pool，也支持显式覆盖：

```bash
brewfs mount /mnt/slayer \
  --meta-url sqlite:///tmp/brewfs.db \
  --data-dir /tmp/brewfs-data \
  --fuse-workers 4 \
  --fuse-max-background 64
```

说明：
- 默认会按机器可用并发度自动选择 worker 数，且至少为 `2`
- `--fuse-workers 0` 或 `1`：保持 `asyncfuse` 旧的 legacy session dispatch
- `--fuse-workers > 1`：启用 `asyncfuse` worker pool
- `--fuse-max-background`：限制排队中和执行中的 FUSE 请求总数
- YAML 配置也支持：

```yaml
fuse:
  workers: 4
  max_background: 64
```

---

## 🌟 当前能力

### 基于路径的 FileSystem
- mkdir/mkdir_all/create/read/write/readdir/stat/exists/unlink/rmdir/rename/truncate
- 使用单把互斥锁保护命名空间（避免多锁死锁）；热点路径避免持锁 await

### 分块 IO + 洞零填充
- 默认 64MiB chunk + 4MiB block（可配置）
- 写路径按 block 拆分；读路径对未写区域返回 0

### 对象存储 BlockStore
- LocalFS 用于测试/示例
- S3 兼容后端支持 RustFS、MinIO、Ceph RGW 等服务

### 带事务的元数据
- 支持 SQLite/PostgreSQL、Redis、etcd、TiKV 等元数据后端
- Redis 后端使用 Lua/CAS 保护 chunk slice 更新

更多细节：参见 `doc/operations/sdk.md` 与源码注释。

---

## 📚 文档
- 文档索引：`doc/README.md`
- 架构设计：`doc/architecture/arch.md`
- 配置说明：`doc/operations/configuration.md`
- VFS 内部实现：`doc/vfs/README.md`
- 测试与 CI：`doc/README.md#testing-and-ci`
- 性能与 JuiceFS 对比：`doc/README.md#performance-and-juicefs-comparison`
- JuiceFS 内部机制分析：`doc/juicefs/README.md`
- BrewFS/JuiceFS 差距分析：`doc/gap/README.md`
- SDK 使用：`doc/operations/sdk.md`

---

## 🤝 参与贡献

欢迎通过 Issue/PR 参与改进架构、实现与文档。
