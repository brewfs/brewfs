# k8s xfstests

本目录提供一套与 `docker/compose-xfstests` 对齐的 Kubernetes 版本编排，用于在集群里运行 BrewFS 的 xfstests。

当前目标是把 compose 里的核心对象一比一拆成 Kubernetes 资源：

- `redis`
- `rustfs`
- `rustfs-init`
- `xfstests`

## 目录说明

```text
k8s/run_xfstests/
├── README.md
├── kustomization.yaml
├── namespace.yaml
├── pvc.yaml
├── redis.yaml
├── rustfs.yaml
├── rustfs-init-job.yaml
├── xfstests-job.yaml
└── run_redis_xfstests.sh
```

## 资源对应关系

- `redis.yaml`
  - 对应 compose 里的 `redis`
  - 提供 Redis 元数据库
- `rustfs.yaml`
  - 对应 compose 里的 `rustfs`
  - 提供 S3 兼容对象存储
- `rustfs-init-job.yaml`
  - 对应 compose 里的 `rustfs-init`
  - 负责确保 `brewfs-data` bucket 存在
- `xfstests-job.yaml`
  - 对应 compose 里的 `xfstests`
  - 负责实际挂载 BrewFS 并运行 xfstests
- `pvc.yaml`
  - 提供与 compose volume 对应的持久卷：
    - `brewfs-state`
    - `rustfs-data`
    - `artifacts`

## 前置条件

运行这套清单前，需要满足：

- 集群节点允许 `privileged` Pod
- 节点存在 `/dev/fuse`
- 集群有默认 `StorageClass`，或你手动修改 `pvc.yaml`
- `xfstests` 镜像已经可被集群拉取

`xfstests` Job 默认使用镜像占位符 `__XFSTESTS_IMAGE__`，由脚本在运行时替换。

## 运行方式

推荐通过脚本启动：

```bash
bash k8s/run_xfstests/run_redis_xfstests.sh --image <your-registry>/brewfs-xfstests:tag
```

当前脚本固定使用 `rustfs` 作为对象存储，也就是固定 `BREWFS_DATA_BACKEND=s3`。

常用参数：

- `--image <image>`
  - 指定 xfstests runner 镜像
- `--namespace <ns>`
  - 指定命名空间，默认 `brewfs-xfstests`
- `--cases "<case...>"`
  - 只跑指定用例
- `--skip-cases <N>`
  - 跳过默认全量序列前 N 个用例
- `--check-args "<args...>"`
  - 原样透传给 `./check`
- `--keep`
  - 保留 namespace 中的资源，便于调试

## 运行流程

脚本执行顺序与 compose 版本尽量保持一致：

1. 渲染一份临时 kustomize 目录
2. 应用 `namespace + pvc + redis + rustfs`
3. 等待 `redis` 和 `rustfs` ready
4. 创建并等待 `rustfs-init` Job 完成
5. 创建并等待 `xfstests` Job 完成
6. 打印 `xfstests` Job 日志

## 产物

`xfstests` Job 把产物写入 `/artifacts`，对应 PVC `artifacts`。

Job 内部仍然沿用 `run_xfstests_in_container.sh` 的逻辑，因此产物目录结构与 compose 版本保持一致，例如：

```text
/artifacts/run-<timestamp>-<rand>/
├── brewfs.log
├── local.config
├── backend.yml
└── results/
```

## 已知限制

- 当前脚本不负责构建或推送 `xfstests` 镜像，只负责消费一个已经可拉取的镜像。
- `pvc.yaml` 默认依赖集群的默认 `StorageClass`。
- `xfstests` Pod 需要宿主机 `/dev/fuse`，因此不适合受限沙箱集群。
- 当前只先维护 Redis 版元数据库流程；若后续需要，可以再补 etcd / sqlite 版本。
