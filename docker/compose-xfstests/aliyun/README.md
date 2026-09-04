# Aliyun ECS 性能测试

这个目录把现有 Redis/TiKV Docker Compose 性能测试迁移到 Aliyun。推荐使用 ACK/Kubernetes runner：镜像在本地构建并推送，测试在 K8s Job 中运行；ECS/Cloud Assistant runner 仅作为没有 ACK 集群时的 fallback。

## ACK/Kubernetes 主流程

性能测试的推荐路径是本地构建镜像后交给 ACK 运行，避免在临时 ECS 上冷编译。`run_aliyun_perf_k8s.ps1` 使用 `Dockerfile.perf-local` 在本地 Docker builder 中构建 Linux BrewFS 镜像，推送到 GHCR（或其他可访问 registry），然后在已有 ACK 集群中创建 Redis/TiKV 依赖和特权 FUSE Job，并把 `/artifacts` 拷回本地。

```powershell
.\docker\compose-xfstests\aliyun\run_aliyun_perf_k8s.ps1 `
  -KubeconfigPath $env:KUBECONFIG `
  -RegistryImage ghcr.io/ivanbeethoven/brewfs-perf `
  -GhcrToken $env:GHCR_TOKEN `
  -Backend redis -DataBackend local-fs `
  -ArtifactDirectory .\docker\compose-xfstests\artifacts\ack-redis
```

测试完成后脚本会在本地输出两个结果：完整结果目录和同名 `.zip` 归档。归档包含性能报告、原始日志、BrewFS 日志、后端诊断和性能统计，便于上传或脱离集群查看（xfstests/LTP runner 的 artifacts 也使用同样的目录结构）。脚本会在容器中先生成单个 `tar.gz` 再下载，避免逐文件复制时出现 `unexpected EOF`。

若希望在测试进行时从另一终端手动导出，保留 Job 并延长结果保留窗口：

```powershell
$tag = 'aliyun-20260904-redis'
.\docker\compose-xfstests\aliyun\run_aliyun_perf_k8s.ps1 `
  -KubeconfigPath $env:KUBECONFIG -ImageTag $tag -Backend redis `
  -KeepJob -ArtifactHoldSeconds 1800

.\docker\compose-xfstests\aliyun\run_aliyun_perf_k8s.ps1 `
  -Action export -JobName "brewfs-perf-$tag" `
  -KubeconfigPath $env:KUBECONFIG -ArtifactDirectory .\artifacts\manual
```

`-Action export` 只能在 Pod 仍处于 Running 且 `perf.complete` 已出现的 hold 窗口内执行；默认 `emptyDir` 随 Pod 结束而消失。因此正常使用应直接等待 `-Action test` 自动导出。若需要测试结束后仍可导出，应为 Job 改用持久化卷（后续可增加 `-ArtifactPvc` 参数）。

ACK 集群本身可使用 `operator/brewfs-operator/scripts/ack-e2e.ps1` 创建/销毁；K8s runner 不创建 VPC、节点或账号级网络资源。`run_aliyun_perf.ps1` 保留为 ECS/Cloud Assistant fallback，适合没有 ACK 集群的故障诊断，不是主性能测试路径。

## 前置条件

- Aliyun CLI 已配置，并具备 ECS、VPC 查询、RunCommand 权限。
- 目标地域已有可用的 VPC vSwitch 和安全组；脚本不会自动创建或删除账号网络资源。
- ECS 镜像内置 Cloud Assistant Agent，且能访问软件源和 GitHub/GHCR。
- 目标镜像在该地域可用。默认值是 `ubuntu_24_04_x64_20G_alibase_20260522.vhd`，可用 `ecs DescribeImages` 查询并通过 `-ImageId` 覆盖。

## 使用方式

```powershell
# 创建临时 ECS，跑 Redis + RustFS/S3 性能测试，然后自动释放 ECS
.\docker\compose-xfstests\aliyun\run_aliyun_perf.ps1 `
  -Action run `
  -VSwitchId vsw-xxxxxxxx `
  -SecurityGroupId sg-xxxxxxxx `
  -RegionId ap-northeast-2 `
  -ZoneId ap-northeast-2a `
  -Backend redis `
  -DataBackend s3 `
  -Ref main

# TiKV 场景，并保留 ECS 方便检查日志
.\docker\compose-xfstests\aliyun\run_aliyun_perf.ps1 `
  -Action run -InstanceId i-xxxxxxxx `
  -RegionId ap-northeast-2 -Backend tikv `
  -DataBackend local-fs -KeepInstance

# 单独创建、查看和销毁
.\docker\compose-xfstests\aliyun\run_aliyun_perf.ps1 -Action create `
  -VSwitchId vsw-xxxxxxxx -SecurityGroupId sg-xxxxxxxx
.\docker\compose-xfstests\aliyun\run_aliyun_perf.ps1 -Action status `
  -InstanceId i-xxxxxxxx -RegionId ap-northeast-2
.\docker\compose-xfstests\aliyun\run_aliyun_perf.ps1 -Action destroy `
  -InstanceId i-xxxxxxxx -RegionId ap-northeast-2
```

## 参数映射

| ECS 脚本参数 | Compose 等价行为 |
| --- | --- |
| `-Backend redis` | 调用 `run_redis_perf.sh`，启动 Redis、RustFS/MinIO 和 perf 容器 |
| `-Backend tikv` | 调用 `run_tikv_perf.sh`，启动 PD、TiKV、RustFS 和 perf 容器 |
| `-DataBackend s3` | 传递 `--s3`，使用 Compose 内的 RustFS |
| `-DataBackend local-fs` | 传递 `--local-fs` |
| `-PerfTools` | 传递给现有 runner 的 `--tools`，保持本地与云端测试矩阵一致 |
| `-RunBench` | 传递 `--brewfs-bench` |

默认 ECS 为按量付费，并设置四小时自动释放时间；`run` 结束后还会主动释放实例，除非指定 `-KeepInstance` 或 `-NoCleanup`。脚本不会删除快照、VPC、vSwitch、安全组或其他账号资源。
