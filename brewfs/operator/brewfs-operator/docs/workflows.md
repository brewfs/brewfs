# Workflows And Deployment Modes

## 目标

这篇文档关注“怎么用”，而不是“字段是什么”。

它描述几种典型组合：

- 只有后端
- 后端 + 挂载 workload
- 后端 + 宿主机挂载暴露
- 后端 + 宿主机挂载暴露 + consumer workload

## 流程 1：先创建后端

第一步总是先创建 `BrewFSCluster`：

```bash
kubectl apply -f manifests/example-cluster.yaml
```

建议先确认：

- Redis Pod 已启动
- RustFS Pod 已启动
- bucket 初始化 `Job` 已完成
- `BrewFSCluster.status.configMap` 已出现

如果这一步没有完成，`BrewFSMount` 只会不断等待，不会真正进入运行态。

## 流程 2：仅创建挂载 workload

适合：

- 调试 `brewfs mount` 本身
- 验证 FUSE 与后端链路

配置特点：

- 不写 `hostMountPath`
- 不写 `consumer`

结果：

- 只创建挂载 workload
- 挂载点只在挂载容器内部可见

## 流程 3：把挂载点暴露到宿主机

适合：

- 想让其他 Pod 通过 hostPath 使用挂载点
- 想做节点级实验

推荐配置：

```yaml
spec:
  workloadKind: DaemonSet
  mountPath: /mnt/brewfs
  hostMountPath: /var/lib/brewfs/mounts/demo
  mountPropagation: Bidirectional
```

建议原因：

- `DaemonSet`
  - 每个匹配节点都可有一个挂载实例
- `hostMountPath`
  - 把挂载点落到宿主机目录
- `Bidirectional`
  - 更适合传播容器内的挂载动作

## 流程 4：让 operator 自动创建 consumer workload

适合：

- 需要一个最小业务容器验证挂载点内容
- 不想手写 hostPath Deployment

推荐配置：

```yaml
spec:
  hostMountPath: /var/lib/brewfs/mounts/demo
  consumer:
    workloadKind: Deployment
    image: busybox:1.36
    mountPath: /data
    command:
      - /bin/sh
      - -ec
    args:
      - "while true; do ls -al /data; sleep 30; done"
```

结果：

- operator 创建挂载 workload
- operator 再创建 consumer workload
- consumer 容器在 `/data` 看见相同宿主机目录

## 生命周期顺序

推荐理解为以下链路：

1. cluster reconcile
2. cluster 生成 config
3. mount reconcile
4. mount workload 启动
5. 如果配置 consumer 且 `hostMountPath` 存在
6. consumer workload 启动

因此：

- cluster 是前置
- config 是桥梁
- mount 是运行时核心
- consumer 是可选附加层

## 推荐部署模式

## 模式 A：最小开发模式

特点：

- 单实例
- 易调试
- 适合本地或测试集群

建议：

- `BrewFSCluster`
- `BrewFSMount.workloadKind = Deployment`
- 不开 `hostMountPath`
- 不开 `consumer`

## 模式 B：节点实验模式

特点：

- 更贴近节点级挂载
- 适合验证 hostPath 消费链路

建议：

- `BrewFSMount.workloadKind = DaemonSet`
- 开 `hostMountPath`
- `mountPropagation = Bidirectional`

## 模式 C：演示/验收模式

特点：

- 能直接展示业务侧可见结果
- 不需要额外手写业务 YAML

建议：

- 模式 B 的基础上
- 开启 `consumer`

## 常见故障点

## 1. `BrewFSMount` 一直 `Pending`

优先检查：

- `clusterRef.name` 是否存在
- `BrewFSCluster` 是否已产生 `ConfigMap`
- namespace 是否一致

## 2. 挂载 workload 一直不 ready

优先检查：

- 节点是否有 `/dev/fuse`
- 集群是否允许 `privileged`
- FUSE 相关运行时是否可用
- RustFS / Redis 服务是否真的可达

## 3. 配了 `consumer` 但没有 workload

优先检查：

- 是否设置了 `hostMountPath`
- `status.message` 是否提示等待宿主机路径暴露

## 4. consumer workload ready，但看不到挂载内容

优先检查：

- `hostMountPath` 是否与挂载 workload 使用的相同
- `mountPropagation` 是否设置合理
- 节点安全策略是否屏蔽传播行为

## 排障建议

推荐按这个顺序看：

1. `kubectl get brewfscluster,brewfsmount -o yaml`
2. `status.phase` / `status.message`
3. mount workload Pod 日志
4. consumer workload Pod 日志
5. Redis / RustFS Pod 状态

## 当前最佳实践

- 如果只是验证后端，不要急着开 `consumer`
- 如果要做节点级消费，优先使用 `DaemonSet + hostMountPath + Bidirectional`
- 如果要演示业务读取链路，开启一个最小 `busybox` consumer 即可
- 如果要进入生产方向，建议把下一步重点放在：
  - `NodeRuntime`
  - 完整 workload template
  - 更强的状态与恢复策略
