# ACK/Aliyun Kubernetes E2E

`ack-e2e.ps1` 将之前在 Aliyun ACK 上验证 BrewFS Operator 的流程串起来：

1. 可选创建一个单节点 ACK Managed Basic 集群（首尔、`ecs.e-c1m2.large`）。
2. 获取临时 kubeconfig，并设置 `KUBECONFIG`。
3. 创建 GHCR 拉取 Secret，部署 Operator。
4. 将 ACK 的默认云盘 StorageClass 切到 `alicloud-disk-essd`，避免 `ap-northeast-2a` 不支持 `alicloud-disk-efficiency`。
5. 创建 `BrewFSCluster/demo` 与 `BrewFSMount/demo-mount`。
6. 执行写入、读取、复制、移动、硬链接、软链接、权限检查等冒烟测试。
7. 使用 `-RunRestartTest` 时，删除挂载 Pod，清理节点上的旧 FUSE 挂载，再验证重启后的数据持久性。
8. `-Action all` 默认清理 CR 并提交 ACK 集群删除任务。

## 前置条件

- Windows PowerShell 5.1+ 或 PowerShell 7+
- Aliyun CLI，且已通过 `aliyun configure` 配置有 ACK/ECS 权限的 Profile
- `kubectl`
- GitHub CLI（`gh auth login`），Token 需要 `read:packages` 权限；也可以通过环境变量 `GHCR_TOKEN` 传入
- 当前分支的 Operator 镜像和 BrewFS 镜像已经发布到 GHCR

## 使用方式

在仓库根目录执行：

```powershell
# 使用已有 ACK 集群，仅部署并测试
& .\operator\brewfs-operator\scripts\ack-e2e.ps1 `
  -Action test `
  -ClusterId cxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx `
  -RegionId ap-northeast-2 `
  -RunRestartTest
```

创建临时集群、测试并自动提交删除：

```powershell
& .\operator\brewfs-operator\scripts\ack-e2e.ps1 `
  -Action all `
  -RunRestartTest
```

仅创建集群并输出 kubeconfig：

```powershell
& .\operator\brewfs-operator\scripts\ack-e2e.ps1 -Action create
```

查询状态或销毁指定集群：

```powershell
& .\operator\brewfs-operator\scripts\ack-e2e.ps1 -Action status -ClusterId cxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
& .\operator\brewfs-operator\scripts\ack-e2e.ps1 -Action destroy -ClusterId cxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

如需保留临时集群供人工检查，给 `all` 加 `-NoCleanup`。脚本不会删除 GHCR、Aliyun 账号密钥或集群外的云资源；集群删除是否释放其附属资源由 ACK 的 `retain_all_resources=false` 控制。

## 已知限制

- 集群创建请求使用单节点、单 Zone、自动 VPC/安全组的测试参数；生产集群应显式传入网络和节点池配置。
- `BrewFSMount` 的 host FUSE 挂载在 Pod 强制删除后可能残留，脚本的重启测试会使用临时特权 Pod 在宿主机命名空间执行 lazy unmount。
- 该脚本只做 Operator 和 POSIX 冒烟验证；xfstests、LTP、pjdfstest 继续使用仓库中的 Docker/Kubernetes 测试入口。
