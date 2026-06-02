# BrewFS Operator Docs

这组文档用于说明独立 `brewfs-operator` 的职责、资源模型、reconcile 行为和部署方式。

## 文档导航

- `architecture.md`
  - 说明 operator 的整体分层、控制循环、资源关系和设计边界。
- `brewfscluster.md`
  - 说明 `BrewFSCluster` 的字段、默认值、创建出的 Kubernetes 资源和状态语义。
- `brewfsmount.md`
  - 说明 `BrewFSMount` 的字段、挂载工作负载、宿主机挂载暴露、consumer 工作负载和状态语义。
- `workflows.md`
  - 说明常见部署模式、推荐组合、生命周期顺序和排障建议。

## 推荐阅读顺序

1. `architecture.md`
2. `brewfscluster.md`
3. `brewfsmount.md`
4. `workflows.md`

## 适用范围

当前文档描述的是仓库中的独立 Rust operator：

- 路径：`operator/brewfs-operator`
- 语言：Rust
- 控制器框架：`kube-rs`
- 当前 CR：
  - `BrewFSCluster`
  - `BrewFSMount`

## 当前能力边界

当前 operator 已经能够：

- 管理 Redis、RustFS、bucket 初始化任务和 BrewFS 配置 `ConfigMap`
- 管理 BrewFS 挂载 workload
- 通过 `hostMountPath` 把挂载点暴露到宿主机目录
- 可选自动创建一个 consumer workload 来消费相同宿主机目录

当前 operator 还没有做到：

- 自动改写已有业务 Deployment / StatefulSet
- 提供标准 CSI 接口
- 处理节点级回收、污点驱逐、跨节点数据消费一致性等更强的运行时问题
