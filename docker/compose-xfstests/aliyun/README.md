# Aliyun 云端性能测试

这个目录把现有 Redis/TiKV Docker Compose 性能测试迁移到 Aliyun。百万级 packed 小文件测试使用 ECS/Cloud Assistant runner：默认 32 GiB 内存、100 GiB ESSD、100 万个 100 KiB 文件和三级目录树。ACK runner 仍用于已有集群上的通用矩阵。

## 百万级 packed 小文件测试

`run_aliyun_packed_million.ps1` 是专用入口，目录布局默认是：

```text
root/
  d000..d009/
    d000..d009/
      d000..d009/
        f00000..f00999  (100 KiB each)
```

也就是 `10 x 10 x 10 x 1,000 = 1,000,000` 个文件。packed fixture 的小文件数据共享一个不可变 block，因此不会在 ECS 本地落下约 100 GiB 的重复 payload；全文件读模式仍会实际读取约 100 GiB 的逻辑数据并记录 payload bytes。

```powershell
# 先用 -DryRun 检查参数；需要已有 vSwitch 和安全组。
.\docker\compose-xfstests\aliyun\run_aliyun_packed_million.ps1 `
  -DryRun `
  -VSwitchId vsw-xxxxxxxx `
  -SecurityGroupId sg-xxxxxxxx

# 创建 ECS，构建指定 ref，发布 packed fixture，冷读扫描后自动释放 ECS。
.\docker\compose-xfstests\aliyun\run_aliyun_packed_million.ps1 `
  -VSwitchId vsw-xxxxxxxx `
  -SecurityGroupId sg-xxxxxxxx `
  -RegionId cn-hangzhou `
  -ZoneId cn-hangzhou-h `
  -ImageId ubuntu_24_04_x64_20G_alibase_20260916.vhd `
  -Ref codex/packed-million
```

默认使用 `ReadMode=full`，会读取每个 100 KiB 文件；若只想先验证元数据路径，可使用 `-ReadMode prefix`。测试固定关闭 BrewFS 数据缓存和预取，并要求 `drop_caches` 成功；结果不会把缓存命中当成冷读性能。`-KeepInstance` 可保留现场，`-NoCleanup` 禁止自动释放，完成后使用原 ECS runner 的 `-Action destroy` 清理。

当前工作树的代码尚未提交到远端时，`-Ref` 必须指向已推送的分支或 commit；ECS runner 会在远端重新 clone 该 ref，不会自动上传未提交修改。

## ACK/Kubernetes 主流程

性能测试的推荐路径是本地构建镜像后交给 ACK 运行，避免在临时 ECS 上冷编译。`run_aliyun_perf_k8s.ps1` 使用 `Dockerfile.perf-local` 在本地 Docker builder 中构建 Linux BrewFS 镜像，推送到 GHCR（或其他可访问 registry），然后在已有 ACK 集群中创建 Redis/TiKV 依赖和特权 FUSE Job，并把 `/artifacts` 拷回本地。

```powershell
$env:BREWFS_RESULTS_URL = 'https://results.example.com'

.\docker\compose-xfstests\aliyun\run_aliyun_perf_k8s.ps1 `
  -KubeconfigPath $env:KUBECONFIG `
  -RegistryImage ghcr.io/ivanbeethoven/brewfs-perf `
  -GhcrUsername Ivanbeethoven `
  -GhcrToken $env:GHCR_TOKEN `
  -Backend redis -DataBackend local-fs `
  -ArtifactDirectory .\docker\compose-xfstests\artifacts\ack-redis
```

测试完成后脚本会在本地输出两个结果：完整结果目录和同名 `.zip` 归档。设置 `BREWFS_RESULTS_URL` 后，脚本还会自动把同一个 ZIP POST 到网站；`-ResultVaultUrl` 可临时覆盖环境变量，未配置时只保存在本地。网站不可用时不会丢弃本地结果，只会发出警告。归档包含性能报告、原始日志、BrewFS 日志、后端诊断和性能统计，便于上传或脱离集群查看（xfstests/LTP runner 的 artifacts 也使用同样的目录结构）。脚本会在容器中先生成单个 `tar.gz` 再下载，避免逐文件复制时出现 `unexpected EOF`。

默认情况下，无论测试成功还是失败，runner 都会清理本轮带有 `app.kubernetes.io/managed-by=brewfs-perf-runner` 标签的 Job、Redis/TiKV、RustFS、ConfigMap 和镜像拉取 Secret，避免共享 ACK 集群上留下持续占用节点的资源。需要保留现场或手工导出时使用 `-KeepJob`；之后通过 `-Action destroy` 清理。

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

`-Action export` 只能在 Pod 仍处于 Running 且 `perf.complete` 已出现的 hold 窗口内执行；默认 `emptyDir` 随 Pod 结束而消失。因此正常使用应直接等待 `-Action test` 自动导出。若需要测试结束后仍可导出，应为 Job 改用持久化卷（后续可增加 `-ArtifactPvc` 参数）。不带 `-KeepJob` 时，自动导出完成后会立即清理测试资源，不再等待 hold 窗口。

ACK 集群本身可使用 `operator/brewfs-operator/scripts/ack-e2e.ps1` 创建/销毁；K8s runner 不创建 VPC、节点或账号级网络资源。`run_aliyun_perf.ps1` 保留为 ECS/Cloud Assistant fallback，适合没有 ACK 集群的故障诊断，不是主性能测试路径。

## 前置条件

- Aliyun CLI 已配置，并具备 ECS、VPC 查询、RunCommand 权限。
- 目标地域已有可用的 VPC vSwitch 和安全组；脚本不会自动创建或删除账号网络资源。
- ECS 镜像内置 Cloud Assistant Agent，且能访问软件源和 GitHub/GHCR。
- 目标镜像在该地域可用。百万级入口默认使用 `ubuntu_24_04_x64_20G_alibase_20260916.vhd`，可用 `ecs DescribeImages` 查询并通过 `-ImageId` 覆盖；通用 runner 仍可单独传入 `-ImageId`。

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

默认 ECS 为按量付费，并设置八小时自动释放时间；`run` 结束后还会主动释放实例，除非指定 `-KeepInstance` 或 `-NoCleanup`。脚本不会删除快照、VPC、vSwitch、安全组或其他账号资源。创建前会在实例内校验内存至少 30,000,000 KiB、工作盘至少 90,000,000,000 字节，并把实际值写入 `aliyun-resource-proof.env`。
