# BrewFS Operator

这是一个独立于 `brewfs` 主二进制的 Kubernetes operator 工程。

当前这版 operator 仍然不是完整 CSI，但已经拆成两层 CR：

- `BrewFSCluster`：管理 Redis、RustFS 和 BrewFS 配置
- `BrewFSMount`：专门管理挂载工作负载

详细文档入口：

- `docs/README.md`
- `docs/architecture.md`
- `docs/brewfscluster.md`
- `docs/brewfsmount.md`
- `docs/workflows.md`

## 当前范围

`BrewFSCluster` 的 reconcile 逻辑负责创建和维护：

- Redis `Deployment` + `Service`
- RustFS `Deployment` + `Service`
- RustFS 凭据 `Secret`
- RustFS 数据 `PersistentVolumeClaim`
- RustFS bucket 初始化 `Job`
- BrewFS 配置 `ConfigMap`

`BrewFSMount` 的 reconcile 逻辑负责创建和维护：

- BrewFS 挂载 `Deployment` 或 `DaemonSet`
- 可选的消费工作负载 `Deployment` 或 `DaemonSet`
- 挂载状态 `status`
- 对 `BrewFSCluster`/ConfigMap 就绪性的等待与关联

这意味着第一版 operator 主要解决的是：

- 后端依赖栈一致性
- BrewFS 挂载配置的标准化下发
- 把“后端栈”和“挂载工作负载”拆成两个 CR 分别管理

当前还**不**包含：

- CSI 驱动
- 在任意已有业务工作负载上做原地注入
- CSI 生命周期集成

## 目录结构

```text
operator/brewfs-operator/
├── Cargo.toml
├── README.md
├── src/
│   ├── main.rs
│   ├── crd.rs
│   └── reconciler.rs
└── manifests/
    ├── namespace.yaml
    ├── rbac.yaml
    ├── deployment.yaml
    ├── example-cluster.yaml
    └── example-mount.yaml
```

## CRD

自定义资源名称：

- Kind: `BrewFSCluster`
- Kind: `BrewFSMount`
- Group: `storage.brewfs.io`
- Version: `v1alpha1`

`BrewFSCluster` 最小示例：

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

`BrewFSMount` 最小示例：

```yaml
apiVersion: storage.brewfs.io/v1alpha1
kind: BrewFSMount
metadata:
  name: demo-mount
spec:
  clusterRef:
    name: demo
  workloadKind: Deployment
  image: brewfs:local
```

## 工作方式

`BrewFSMount` 会引用一个同 namespace 下的 `BrewFSCluster`，并复用该 cluster 生成的 `ConfigMap`。

当前挂载工作负载可以声明为 `Deployment` 或 `DaemonSet`，其特点是：

- 使用 `privileged` + `SYS_ADMIN`
- 挂载宿主机 `/dev/fuse`
- 在容器内运行 `brewfs mount --config ...`
- 默认用 `emptyDir` 作为 `/var/lib/brewfs` 状态目录，也可以通过 `statePvcName` 绑定已有 PVC
- 支持 `serviceAccountName`、`nodeSelector`、`tolerations`
- 可选把 `mountPath` 绑定到宿主机 `hostMountPath`，并通过 `mountPropagation` 控制传播

字段建议：

- `workloadKind: Deployment` 适合单实例或受控副本数的挂载 Pod
- `workloadKind: DaemonSet` 适合希望每个匹配节点都运行一个挂载 Pod 的场景
- `replicas` 仅对 `Deployment` 生效
- `hostMountPath` 打开后，挂载点会落到宿主机目录，其他 Pod 可以通过相同 `hostPath` 消费
- `mountPropagation: Bidirectional` 更适合节点级挂载传播；未配置 `hostMountPath` 时该字段不会产生实际效果
- `consumer` 打开后，operator 会额外创建一个消费 `hostMountPath` 的 workload，省去手写业务侧 `hostPath`
- `consumer.workloadLabels` / `consumer.workloadAnnotations` 可把已有业务 workload 的顶层 metadata 平移进来

`consumer` 的工作方式：

- `consumer.workloadKind` 支持 `Deployment`、`DaemonSet` 和 `StatefulSet`
- `consumer.mountPath` 是默认共享挂载路径
- `consumer.containers` / `consumer.initContainers` / `consumer.volumes` 可定义更完整的 Pod 模板
- `consumer.workloadLabels` / `consumer.workloadAnnotations` 可描述生成出来的 Deployment / StatefulSet 自身 metadata
- `consumer.containers[*]` 现在还支持 `resources`、`ports`、`envFrom`、`securityContext`、`livenessProbe`、`readinessProbe`、`startupProbe`、`lifecycle`、`terminationMessagePolicy`、`stdin/tty`
- `consumer` 还支持一批 PodSpec 级字段，如 `imagePullSecrets`、`priorityClassName`、`dnsPolicy`、`terminationGracePeriodSeconds`、`podSecurityContext`
- 如果 `consumer.containers` 为空，仍会回退到 `consumer.image` / `consumer.command` / `consumer.args` / `consumer.env`
- `consumer` 依赖 `hostMountPath`；未配置 `hostMountPath` 时不会创建 consumer workload

这意味着当前版本已经可以把挂载点落到宿主机路径，并由 operator 自动创建消费 workload，但还**没有**做到：

- 跨节点统一抽象消费方式
- 对集群里现存 Deployment/StatefulSet 做原地修改

一个 `DaemonSet` 风格示例：

```yaml
apiVersion: storage.brewfs.io/v1alpha1
kind: BrewFSMount
metadata:
  name: demo-node-mount
spec:
  clusterRef:
    name: demo
  workloadKind: DaemonSet
  image: brewfs:local
  hostMountPath: /var/lib/brewfs/mounts/demo
  mountPropagation: Bidirectional
  nodeSelector:
    kubernetes.io/os: linux
  tolerations:
    - operator: Exists
  consumer:
    workloadKind: StatefulSet
    workloadLabels:
      app.kubernetes.io/name: demo-app
      app.kubernetes.io/component: api
    mountPath: /data
    containers:
      - name: app
        image: busybox:1.36
        ports:
          - name: http
            containerPort: 8080
        resources:
          requests:
            cpu: 100m
            memory: 128Mi
        command:
          - /bin/sh
          - -ec
        args:
          - "while true; do ls -al /data; sleep 30; done"
```

这样会生成两类 workload：

- 一个挂载 workload，把 BrewFS 挂到宿主机 `/var/lib/brewfs/mounts/demo`
- 一个 consumer workload，把相同宿主机路径挂到容器内 `/data`
- 如果 `consumer.workloadKind: StatefulSet`，operator 还会自动创建同名 headless Service 供 StatefulSet 绑定

## 运行

开发模式运行 controller：

```bash
cd operator/brewfs-operator
cargo run -- run
```

打印 CRD YAML：

```bash
cd operator/brewfs-operator
cargo run -- crdgen
```

应用示例资源：

```bash
kubectl apply -f manifests/crd.yaml
kubectl apply -f manifests/example-cluster.yaml
kubectl apply -f manifests/example-mount.yaml
```

## 后续演进方向

这版 operator 目前是“后端栈 + 挂载工作负载”两层模型。后续如果继续扩展，比较自然的方向是：

- 让 `BrewFSMount` 管理 sidecar / 业务 Pod 模板
- 让 `BrewFSMount` 直接接收更完整的 Deployment / StatefulSet 模板片段
- 增加 `BrewFSNodeRuntime` 做节点级挂载
- 或者演化成 CSI 相关的控制面组件
