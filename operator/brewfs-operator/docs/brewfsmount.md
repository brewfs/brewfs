# BrewFSMount

## 资源定位

`BrewFSMount` 表示一个“挂载平面实例”。

它解决的问题不是“如何定义后端”，而是：

- 如何运行 `brewfs mount`
- 如何把挂载点暴露到宿主机目录
- 如何为业务侧创建一个消费该宿主机目录的 workload
- 如何让 operator 生成更贴近已有业务 Deployment / StatefulSet 模板的 consumer workload

换句话说：

- `BrewFSCluster` 更偏后端编排
- `BrewFSMount` 更偏运行时编排

## 与 `BrewFSCluster` 的关系

`BrewFSMount.spec.clusterRef.name` 必须指向同 namespace 下的一个 `BrewFSCluster`。

当前 reconcile 流程会依次检查：

1. `BrewFSCluster` 是否存在
2. 该 cluster 对应的 `ConfigMap` 是否存在
3. 满足后才创建挂载 workload

如果这些前置条件不满足：

- `BrewFSMount` 不会直接失败
- 会停留在 `Pending`
- `status.message` 会说明在等待哪一个资源

## 字段总览

`spec` 当前可分成四组字段：

1. 挂载 workload 本身
2. 宿主机挂载暴露
3. 调度与运行控制
4. consumer workload

## 1. 挂载 workload 字段

核心字段：

- `workloadKind`
- `image`
- `imagePullPolicy`
- `replicas`
- `mountPath`
- `configPath`
- `statePath`
- `logLevel`

含义：

- `workloadKind`
  - 挂载 workload 类型
  - 支持 `Deployment` / `DaemonSet`
- `image`
  - 挂载容器镜像
- `replicas`
  - 仅对 `Deployment` 生效
- `mountPath`
  - `brewfs mount` 在容器内看到的挂载点
- `configPath`
  - cluster 生成的 `config.yaml` 在容器内的挂载路径
- `statePath`
  - BrewFS 本地状态目录
- `logLevel`
  - 传给容器环境变量 `RUST_LOG`

默认值：

- `workloadKind: Deployment`
- `image: brewfs:local`
- `imagePullPolicy: IfNotPresent`
- `replicas: 1`
- `mountPath: /mnt/brewfs`
- `configPath: /run/brewfs/config.yaml`
- `statePath: /var/lib/brewfs`
- `logLevel: brewfs=info`

## 2. 宿主机挂载暴露字段

字段：

- `hostMountPath`
- `mountPropagation`

含义：

- `hostMountPath`
  - 可选
  - 如果设置，operator 会把宿主机目录挂到容器内的 `mountPath`
  - 这样挂载结果就有机会暴露给宿主机命名空间
- `mountPropagation`
  - 控制 `mountPath` 这个 `VolumeMount` 的传播模式
  - 支持：
    - `None`
    - `HostToContainer`
    - `Bidirectional`

推荐理解：

- 不设置 `hostMountPath`
  - 挂载点只存在于挂载 Pod 内
- 设置 `hostMountPath`
  - 挂载点可落到宿主机目录
- `Bidirectional`
  - 更适合节点级挂载传播场景

注意：

- 没有 `hostMountPath` 时，`mountPropagation` 几乎没有实际意义
- 该能力依赖运行环境允许 `privileged + FUSE + hostPath`

## 3. 调度与运行控制

字段：

- `serviceAccountName`
- `nodeSelector`
- `tolerations`
- `statePvcName`

含义：

- `serviceAccountName`
  - 指定挂载 workload 的 ServiceAccount
- `nodeSelector`
  - 约束调度节点
- `tolerations`
  - 允许调度到带特定 taint 的节点
- `statePvcName`
  - 如果指定，则 `statePath` 使用已有 PVC
  - 如果不指定，则退回到 `emptyDir`

## 4. Consumer workload

字段：

- `consumer.workloadKind`
- `consumer.workloadLabels`
- `consumer.workloadAnnotations`
- `consumer.podLabels`
- `consumer.podAnnotations`
- `consumer.image`
- `consumer.imagePullPolicy`
- `consumer.replicas`
- `consumer.mountPath`
- `consumer.command`
- `consumer.args`
- `consumer.env`
- `consumer.initContainers`
- `consumer.containers`
- `consumer.volumes`
- `consumer.serviceAccountName`
- `consumer.nodeSelector`
- `consumer.tolerations`

`consumer` 的作用是：

- 自动创建一个业务侧 workload
- 把与挂载 workload 相同的 `hostMountPath` 挂进去
- 业务容器在 `consumer.mountPath` 下直接访问数据
- 并允许把已有业务模板中的 metadata/container/volume 结构迁移进来

当前 `consumer` 已经支持一组更接近 Pod 模板的能力：

- workload 级 labels / annotations
- Pod labels / annotations
- init containers
- 多个普通 containers
- 额外挂载 volumes
- 每个 container 自己的 env / command / args / workingDir / volumeMounts
- 每个 container 的 `resources` / `ports` / `envFrom` / `securityContext` / `livenessProbe` / `readinessProbe`
- 每个 container 的 `startupProbe` / `lifecycle` / `terminationMessagePolicy` / `stdin` / `tty`
- PodSpec 级的 `imagePullSecrets` / `priorityClassName` / `hostNetwork` / `dnsPolicy` / `terminationGracePeriodSeconds` / `podSecurityContext`

默认值：

- `consumer.workloadKind: Deployment`
- `consumer.image: busybox:1.36`
- `consumer.imagePullPolicy: IfNotPresent`
- `consumer.replicas: 1`
- `consumer.mountPath: /data`
- `consumer.command: ["/bin/sh", "-ec"]`
- `consumer.args: ["sleep infinity"]`

兼容性说明：

- 如果 `consumer.containers` 为空
- operator 会根据 `consumer.image` / `consumer.command` / `consumer.args` / `consumer.env`
  自动合成一个默认 container
- 如果 `consumer.containers` 不为空
- 简化字段仍可保留，但不再用于生成主 containers 列表

重要前提：

- 如果配置了 `consumer`
- 但没有配置 `hostMountPath`
- operator 不会创建 consumer workload
- `BrewFSMount` 会显示为 `Progressing`
- `status.message` 会提示 consumer 依赖 `hostMountPath`

### `consumer.workloadKind`

当前支持：

- `Deployment`
- `DaemonSet`
- `StatefulSet`

推荐理解：

- `Deployment`
  - 适合无状态业务副本
- `DaemonSet`
  - 适合每节点都消费同一个宿主机挂载路径
- `StatefulSet`
  - 适合原本就是有序身份/稳定网络标识的业务模板

当选择 `StatefulSet` 时：

- operator 会自动生成同名 headless `Service`
- `consumer.replicas` 会写入 `StatefulSet.spec.replicas`
- 共享 BrewFS 挂载仍来自同一个 `hostMountPath`

### `consumer.workloadLabels` / `consumer.workloadAnnotations`

这两个字段作用在生成出的 workload 对象本身，而不是 Pod 模板：

- `consumer.workloadLabels`
  - 写到 `Deployment.metadata.labels` / `StatefulSet.metadata.labels`
- `consumer.workloadAnnotations`
  - 写到 workload `metadata.annotations`

适用场景：

- 把已有业务 Deployment / StatefulSet 的顶层 metadata 迁移进来
- 保留原有应用识别标签
- 让外部工具继续按原先标签发现 workload

### `consumer.containers`

每个 container 当前支持：

- `name`
- `image`
- `imagePullPolicy`
- `command`
- `args`
- `env`
- `envFrom`
- `workingDir`
- `mountPath`
- `volumeMounts`
- `ports`
- `resources`
- `securityContext`
- `livenessProbe`
- `readinessProbe`
- `startupProbe`
- `lifecycle`
- `terminationMessagePolicy`
- `stdin`
- `tty`

其中：

- `mountPath`
- 表示该 container 看到共享 BrewFS 数据的路径
- 如果未设置，则回退到 `consumer.mountPath`
- `volumeMounts`
- 用于额外挂载 `consumer.volumes`
- 共享 BrewFS 挂载本身由 operator 自动注入，不需要手写

新增能力说明：

- `envFrom`
  - 支持从 `ConfigMap` 或 `Secret` 批量注入环境变量
  - 可选 `prefix`
- `ports`
  - 渲染到容器 `ports`
  - 支持 `name` / `containerPort` / `protocol`
- `resources`
  - 支持 `requests` / `limits`
  - 值使用标准 Kubernetes 资源字符串，如 `100m`、`256Mi`
- `securityContext`
  - 当前支持 `runAsUser`、`runAsGroup`、`runAsNonRoot`
  - 以及 `readOnlyRootFilesystem`、`allowPrivilegeEscalation`、`privileged`
  - 和 `capabilitiesAdd` / `capabilitiesDrop`
- `livenessProbe` / `readinessProbe`
  - 当前支持三类探针动作：
  - `execCommand`
  - `httpGet`
  - `tcpSocket`
  - 以及常见时序字段，如 `initialDelaySeconds`、`periodSeconds`、`timeoutSeconds`
- `startupProbe`
  - 与 `livenessProbe` / `readinessProbe` 结构相同
  - 适合启动较慢的业务容器
- `lifecycle`
  - 当前支持 `postStart` / `preStop`
  - 每个 hook 都支持 `execCommand` / `httpGet` / `tcpSocket`
- `terminationMessagePolicy`
  - 直接映射到容器终止消息策略
- `stdin` / `tty`
  - 适合交互式调试容器或 shell sidecar

### PodSpec 级字段

`consumer` 当前还支持一批直接作用在 Pod 模板上的字段：

- `serviceAccountName`
- `imagePullSecrets`
- `nodeSelector`
- `tolerations`
- `priorityClassName`
- `hostNetwork`
- `dnsPolicy`
- `terminationGracePeriodSeconds`
- `podSecurityContext`

其中：

- `imagePullSecrets`
  - 用于拉取私有镜像
- `priorityClassName`
  - 让业务 Pod 参与集群优先级调度
- `hostNetwork`
  - 直接控制 Pod 是否使用宿主机网络命名空间
- `dnsPolicy`
  - 适合和 `hostNetwork` 联动调整 DNS 行为
- `terminationGracePeriodSeconds`
  - 控制 Pod 优雅退出窗口
- `podSecurityContext`
  - 当前支持 `runAsUser`、`runAsGroup`、`runAsNonRoot`、`fsGroup`、`supplementalGroups`

### `consumer.initContainers`

结构与 `consumer.containers` 相同，适合：

- 启动前检查共享挂载目录
- 做初始化任务
- 预热应用环境

### `consumer.volumes`

当前支持的附加卷类型：

- `emptyDir`
- `configMapName`
- `secretName`
- `hostPath`

每个卷至少需要：

- `name`

然后再选择一个卷源字段。

## 生成的 Kubernetes 资源

### 挂载 workload

可能是：

- `<mount-name>-mount` `Deployment`
- `<mount-name>-mount` `DaemonSet`

它包含：

- `/dev/fuse` hostPath
- 配置 `ConfigMap`
- 状态卷
- 可选的 `mountPath` hostPath

并运行：

```bash
brewfs mount --config <configPath> <mountPath>
```

### Consumer workload

如果启用 `consumer`，可能是：

- `<mount-name>-consumer` `Deployment`
- `<mount-name>-consumer` `DaemonSet`
- `<mount-name>-consumer` `StatefulSet`

它是一个普通 workload，不带特权设置，不运行 BrewFS，只消费宿主机路径。

如果是 `StatefulSet`，还会额外生成：

- `<mount-name>-consumer` headless `Service`

## 状态字段

当前 `status` 包括两部分观测信息。

### 挂载 workload 状态

- `workloadKind`
- `workloadName`
- `desiredReplicas`
- `readyReplicas`
- `hostMountPath`
- `mountPropagation`

### Consumer workload 状态

- `consumerWorkloadKind`
- `consumerWorkloadName`
- `consumerMountPath`
- `consumerDesiredReplicas`
- `consumerReadyReplicas`

### 通用状态

- `observedGeneration`
- `phase`
- `message`
- `cluster`
- `configMap`
- `lastReconciledAt`

## 推荐模式

### 模式 A：仅容器内挂载

适合：

- 调试
- 单 Pod 测试
- 不需要把挂载暴露给业务侧

做法：

- 不设置 `hostMountPath`
- 不设置 `consumer`

### 模式 B：节点级挂载暴露

适合：

- 希望把挂载点落到宿主机
- 允许其他 workload 通过 hostPath 消费

做法：

- `workloadKind: DaemonSet`
- 配置 `hostMountPath`
- `mountPropagation: Bidirectional`

### 模式 C：自动 consumer

适合：

- 想验证挂载点是否能被业务容器访问
- 想避免手写 hostPath 工作负载

做法：

- 开启 `hostMountPath`
- 开启 `consumer`

## 示例

```yaml
apiVersion: storage.brewfs.io/v1alpha1
kind: BrewFSMount
metadata:
  name: demo-mount
spec:
  clusterRef:
    name: demo
  workloadKind: DaemonSet
  image: brewfs:local
  mountPath: /mnt/brewfs
  hostMountPath: /var/lib/brewfs/mounts/demo
  mountPropagation: Bidirectional
  nodeSelector:
    kubernetes.io/os: linux
  tolerations:
    - operator: Exists
  consumer:
    workloadKind: Deployment
    mountPath: /data
    podLabels:
      app.kubernetes.io/part-of: brewfs-demo
    initContainers:
      - name: init-check
        image: busybox:1.36
        command:
          - /bin/sh
          - -ec
        args:
          - "echo preparing consumer; ls -al /data || true"
    containers:
      - name: app
        image: busybox:1.36
        command:
          - /bin/sh
          - -ec
        args:
          - "while true; do date; ls -al /data; sleep 30; done"
        env:
          APP_MODE: demo
        volumeMounts:
          - name: scratch
            mountPath: /scratch
      - name: sidecar
        image: busybox:1.36
        command:
          - /bin/sh
          - -ec
        args:
          - "while true; do echo sidecar alive; sleep 60; done"
        mountPath: /brewfs
    volumes:
      - name: scratch
        emptyDir: true
```

## 已知限制

- consumer 目前是 operator 自己创建的新 workload，不会改写集群里现存的已有业务对象
- consumer 现在支持更完整的模板，但仍不是完整 PodSpec 透传
- 所谓“接管已有 workload 模板”，当前含义是把模板结构迁移到 `BrewFSMount.spec.consumer`，由 operator 再生成新的 Deployment / StatefulSet
- `hostPath`/FUSE/`mountPropagation` 是否有效仍取决于集群安全策略
- `DaemonSet + hostMountPath` 更接近节点级消费，但还不是完整 node runtime 方案
