# Aliyun ECS 性能测试

这个目录把现有 Redis/TiKV Docker Compose 性能测试迁移到 Aliyun ECS：脚本创建或复用一台 ECS，通过 ECS Cloud Assistant 在远端安装 Docker Compose，拉取仓库并直接调用现有的 `run_redis_perf.sh` 或 `run_tikv_perf.sh`。测试产物仍写入 ECS 上的 `docker/compose-xfstests/artifacts/`，命令完成时会把最新 `perf-summary.tsv` 输出到调用结果。

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
