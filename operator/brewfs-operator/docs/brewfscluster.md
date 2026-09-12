# BrewFSCluster

## 资源定位

`BrewFSCluster` 表示一套可供 BrewFS 挂载平面使用的后端依赖。

它不是“一个已经挂载好的文件系统实例”，而是：

- Redis 元数据后端
- RustFS S3 兼容对象存储
- BrewFS 运行配置

的统一声明入口。

## 字段结构

`BrewFSCluster.spec` 当前包含三个子块：

- `redis`
- `rustfs`
- `mountConfig`

## `redis`

字段：

- `image`
- `port`
- `storageSize`
- `storageClassName`（可选）

默认值：

- `image: redis:7.2-alpine`
- `port: 6379`
- `storageSize: 1Gi`
- `storageClassName`: 未设置时使用集群默认 StorageClass

当前实现会创建：

- 一个 Redis `Service`
- 一个 Redis 数据 `PVC`
- 一个 Redis `Deployment`

Redis `Deployment` 会以：

- `appendonly yes`
- `appendfsync everysec`
- 将 `/data` 挂载到 PVC，AOF 因而可在 Pod 替换后保留

启动，优先保证较合理的持久化表现。当前仍是单副本 Deployment：PVC 能覆盖 Pod
重建和重新调度造成的容器文件系统丢失；Deployment 使用 `Recreate` 策略，避免
新旧 Pod 同时争用单写卷，但会产生短暂不可用，也不提供 Redis 高可用或跨故障域复制。

从未使用 Redis PVC 的旧版 operator 升级时，现有 AOF 仍位于旧 Pod 的临时文件系统，
operator 无法自动迁移它。升级前应先备份 Redis 数据，并在新 PVC 上恢复后再继续写入。
`storageClassName` 应在 PVC 首次创建时选定；Kubernetes 不允许原地修改该字段。

## `rustfs`

字段：

- `image`
- `bucket`
- `region`
- `accessKey`
- `secretKey`
- `port`
- `consolePort`
- `storageSize`

默认值：

- `image: rustfs/rustfs:latest`
- `bucket: brewfs-data`
- `region: us-east-1`
- `accessKey: rustfsadmin`
- `secretKey: rustfsadmin`
- `port: 9000`
- `consolePort: 9001`
- `storageSize: 20Gi`

当前实现会创建：

- 一个凭据 `Secret`
- 一个存储 `PVC`
- 一个 RustFS `Service`
- 一个 RustFS `Deployment`
- 一个 bucket 初始化 `Job`

bucket 初始化 `Job` 会在 RustFS 可达后循环尝试：

- `create-bucket`
- `head-bucket`

直到 bucket 已存在或创建成功。

## `mountConfig`

字段：

- `mountPoint`
- `chunkSize`
- `blockSize`
- `partSize`
- `maxConcurrency`
- `forcePathStyle`

这部分不会直接创建独立 workload，而是被渲染进 `ConfigMap` 中的 `config.yaml`。

该配置当前主要服务于 `BrewFSMount`。

## 生成的 `ConfigMap`

`BrewFSCluster` reconcile 的关键产物之一是：

- `<cluster-name>-brewfs-config`

其中包含 `config.yaml`，内容包括：

- `mount_point`
- `data.backend = s3`
- S3 endpoint / bucket / region
- `meta.backend = redis`
- Redis URL
- layout 参数

这意味着：

- mount workload 不需要自己拼配置
- consumer workload 更不需要知道 Redis / S3 细节

## 状态字段

当前 `status` 包含：

- `observedGeneration`
- `phase`
- `message`
- `redisService`
- `rustfsService`
- `bucket`
- `configMap`
- `lastReconciledAt`

常见理解方式：

- `phase: Ready`
  - 说明 operator 已完成本轮 reconcile
- `redisService`
  - 挂载平面可以用这个服务名访问 Redis
- `rustfsService`
  - 挂载平面可以用这个服务名访问 RustFS
- `configMap`
  - `BrewFSMount` 应引用的配置资源名

## 最小示例

```yaml
apiVersion: storage.brewfs.io/v1alpha1
kind: BrewFSCluster
metadata:
  name: demo
spec:
  redis: {}
  rustfs:
    bucket: brewfs-data
  mountConfig:
    mountPoint: /mnt/brewfs
```

## 典型修改点

最常见的自定义方向：

- 改 RustFS 镜像
- 改 bucket 名
- 改 RustFS 存储大小
- 改 Redis 存储大小或 StorageClass
- 改 chunk / block / part 参数
- 改 path style / concurrency 行为

## 已知限制

- Redis / RustFS 当前都是单实例思路，不是高可用编排
- bucket 初始化逻辑是最小实现，不含复杂租户管理
- `ConfigMap` 是静态配置产物，不含按节点动态渲染逻辑
- 当前不直接支持多对象存储后端编排策略切换
