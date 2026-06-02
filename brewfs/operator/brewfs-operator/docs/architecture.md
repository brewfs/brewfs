# Operator Architecture

## 目标

`brewfs-operator` 是一个独立于 `brewfs` 主程序的控制面工程。

它的目标不是把 Kubernetes 能力硬塞进 `brewfs mount` 二进制，而是提供一个单独的 operator 来管理：

- 后端依赖栈
- BrewFS 配置下发
- FUSE 挂载工作负载
- 挂载点暴露给业务工作负载的消费链路

## 资源分层

当前资源模型拆成两层：

1. `BrewFSCluster`
2. `BrewFSMount`

职责分工如下：

- `BrewFSCluster`
  - 负责 Redis
  - 负责 RustFS
  - 负责 bucket 初始化
  - 负责生成 BrewFS `config.yaml`
- `BrewFSMount`
  - 负责启动真正执行 `brewfs mount` 的 workload
  - 可选把挂载点暴露到宿主机目录
  - 可选创建 consumer workload 去消费该宿主机目录

这个拆分的核心原因是把“后端栈生命周期”和“挂载生命周期”分开：

- 后端资源变更频率更低
- 挂载 workload 更偏运行时
- 业务消费模式可能一套后端对应多个挂载点或多个挂载副本

## 控制循环

当前 `main.rs` 中会同时启动两个 controller：

- 一个 watch `BrewFSCluster`
- 一个 watch `BrewFSMount`

每个 controller 都有独立的：

- reconcile 入口
- error policy
- requeue 周期

实现位置：

- `src/main.rs`
- `src/reconciler.rs`

## `BrewFSCluster` reconcile 产物

一个 `BrewFSCluster` 当前会生成以下资源：

- Redis `Service`
- Redis `Deployment`
- RustFS 凭据 `Secret`
- RustFS 数据 `PersistentVolumeClaim`
- RustFS `Service`
- RustFS `Deployment`
- RustFS bucket 初始化 `Job`
- BrewFS 配置 `ConfigMap`

其中 `ConfigMap` 是整个 mount 平面的关键桥梁。

`BrewFSMount` 不会自己拼 Redis/S3 配置，而是引用 `BrewFSCluster` 生成的配置。

## `BrewFSMount` reconcile 产物

一个 `BrewFSMount` 当前最多会生成两类 workload：

1. 挂载 workload
2. consumer workload

### 挂载 workload

挂载 workload 负责执行：

```bash
brewfs mount --config <configPath> <mountPath>
```

其工作负载类型可选：

- `Deployment`
- `DaemonSet`

它具备以下运行时特征：

- `privileged: true`
- `SYS_ADMIN`
- 宿主机 `/dev/fuse`
- ConfigMap 形式注入 `config.yaml`
- 本地状态目录
- 可选的宿主机挂载导出目录

### Consumer workload

consumer workload 是一个普通业务 workload，由 operator 自动创建。

它不会运行 BrewFS 本身，而是：

- 直接消费 `hostMountPath`
- 在容器内把该路径映射到 `consumer.mountPath`
- 用 `consumer.command` / `consumer.args` / `consumer.env` 定义业务行为

它的作用是把“业务 Pod 自己写 `hostPath`”这件事，从手写 YAML 变成 CR 配置。

## 依赖关系

资源依赖顺序如下：

1. `BrewFSCluster` 创建 Redis / RustFS / ConfigMap
2. `BrewFSMount` 等待对应 cluster 和 config 就绪
3. operator 创建挂载 workload
4. 如果配置了 `consumer` 且存在 `hostMountPath`
5. operator 再创建 consumer workload

如果任一前置条件未满足：

- controller 不会盲目失败退出
- 会把状态写成 `Pending` / `Progressing`
- 并重新入队等待下次 reconcile

## 状态模型

当前状态模型以“观测结果”为主，不是复杂状态机。

`BrewFSCluster.status` 主要回答：

- cluster 是否已 reconcile
- Redis / RustFS 服务名是什么
- bucket 是什么
- 配置 ConfigMap 名是什么

`BrewFSMount.status` 主要回答：

- 挂载 workload 是哪种类型
- 挂载 workload 名称是什么
- 挂载副本是否 ready
- 是否配置了 `hostMountPath`
- 是否启用了 `consumer`
- consumer workload 是否 ready

## 设计边界

当前 operator 刻意不做以下事情：

- 不实现 CSI
- 不修改已有业务工作负载模板
- 不自动把已有 Pod 接到新的挂载点
- 不做复杂节点驱逐恢复策略
- 不做 FUSE 生命周期之外的存储编排

这是为了先把控制面边界收敛在“独立可运行、可观察、可声明式复用”的范围内。

## 后续可能演进

比较自然的后续方向：

- 增加 `BrewFSNodeRuntime`
- 增加业务 workload template 注入能力
- 为 consumer 支持更丰富的 PodSpec 片段
- 演进到 CSI controller / node plugin 结构
