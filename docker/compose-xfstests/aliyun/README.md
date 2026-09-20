# Aliyun 云端性能测试

这个目录使用预制的 Ubuntu 24.04 ECS 虚拟机镜像运行 native 性能测试。镜像内只预装 apt 依赖、fio、xfstests、FUSE、AWS CLI、JuiceFS 最新稳定版和 native runner；不安装 Docker、Redis Server 或 RustFS。每轮测试在本地 WSL 构建 Linux `brewfs`，只上传一个 BrewFS 二进制到临时 ECS；对象数据使用 Aliyun OSS，元数据使用一台临时 Aliyun Redis/Tair 实例。BrewFS 和 JuiceFS 完整矩阵测试结束后，脚本自动删除 ECS、数据盘、OSS bucket、Redis/Tair 实例和二进制临时上传 run，只保留 Result Vault 最终报告和长期 VM 镜像。

## ECS native managed-backend 流程

### 1. 创建并长期保留 Ubuntu VM 镜像

镜像 builder ECS 会在脚本的 `finally` 中删除，只有生成的自定义 VM 镜像会保留：

```powershell
$env:BREWFS_PERF_VSWITCH_ID = 'vsw-xxxxxxxx'
$env:BREWFS_PERF_SECURITY_GROUP_ID = 'sg-xxxxxxxx'

.\docker\compose-xfstests\aliyun\maintain_aliyun_perf_image.ps1 `
  -Action prepare `
  -ImageName brewfs-perf-native-managed `
  -RegionId cn-hangzhou `
  -ZoneId cn-hangzhou-h `
  -VSwitchId $env:BREWFS_PERF_VSWITCH_ID `
  -SecurityGroupId $env:BREWFS_PERF_SECURITY_GROUP_ID
```

该镜像固定内置 JuiceFS `v1.4.1`，不包含任何 OSS/Redis 密钥。查看或删除镜像必须显式执行：

```powershell
$env:BREWFS_PERF_IMAGE_ID = 'm-xxxxxxxx'
.\docker\compose-xfstests\aliyun\maintain_aliyun_perf_image.ps1 -Action status -ImageId $env:BREWFS_PERF_IMAGE_ID
# 只有确认不再使用时才删除：
.\docker\compose-xfstests\aliyun\maintain_aliyun_perf_image.ps1 -Action delete -ImageId $env:BREWFS_PERF_IMAGE_ID
```

### 2. 一次性运行完整 BrewFS + JuiceFS 对比

先在 WSL 本地构建并确认目标 commit，再设置结果服务和网络资源：

```powershell
$env:BREWFS_RESULTS_URL = 'https://<your-result-vault>'
$env:BREWFS_PERF_IMAGE_ID = 'm-xxxxxxxx'
$env:BREWFS_PERF_VSWITCH_ID = 'vsw-xxxxxxxx'
$env:BREWFS_PERF_SECURITY_GROUP_ID = 'sg-xxxxxxxx'

wsl.exe bash -lc 'cd /path/to/brewfs && cargo build --workspace --bin brewfs --release'

.\docker\compose-xfstests\aliyun\run_aliyun_managed_perf.ps1 `
  -RegionId cn-hangzhou `
  -ZoneId cn-hangzhou-h `
  -VSwitchId $env:BREWFS_PERF_VSWITCH_ID `
  -SecurityGroupId $env:BREWFS_PERF_SECURITY_GROUP_ID `
  -ImageId $env:BREWFS_PERF_IMAGE_ID `
  -BinaryPath '\\wsl.localhost\Ubuntu\home\user\brewfs\target\docker\brewfs' `
  -ResultVaultUrl $env:BREWFS_RESULTS_URL
```

脚本会自动：

- 创建两个临时私有 OSS bucket（BrewFS/JuiceFS 各一个）；
- 创建一台临时、非 TLS 的 Aliyun Redis/Tair 实例，并把 vSwitch CIDR 加入白名单；
- 为 BrewFS 和 JuiceFS 分别创建临时 ECS，使用同一 VM 镜像、规格、网络、数据盘和完整工具集合；
- 每轮只上传本地 BrewFS 二进制，JuiceFS 客户端来自镜像；
- 上传完整性能归档到 Result Vault；
- 无论成功、失败或超时都清理 ECS、数据盘、OSS bucket/对象、Redis/Tair 和二进制临时 run。

OSS/Redis 凭证优先通过环境变量传入：

```powershell
$env:BREWFS_PERF_OSS_ACCESS_KEY_ID = '...'
$env:BREWFS_PERF_OSS_SECRET_ACCESS_KEY = '...'
$env:BREWFS_PERF_REDIS_PASSWORD = '...' # 不设置则由脚本随机生成
```

如果没有显式 OSS 凭证，脚本会从当前 Aliyun CLI profile 读取，只在内存中使用，不写入镜像或结果归档。当前实现默认使用 `https://oss-cn-hangzhou.aliyuncs.com`、`cn-hangzhou` 和 1G 级临时 Redis/Tair；这些资源都标记为 temporary 并在 `finally` 中删除。

### 3. 单 workload 调试入口

需要只调试一套 workload 时，可以直接调用底层 runner，但必须显式提供 managed OSS/Redis 参数；测试 ECS 和二进制临时 run 仍会自动清理：

```powershell
.\docker\compose-xfstests\aliyun\run_aliyun_perf.ps1 `
  -Action run -UsePerfImage -ManagedBackend `
  -Workload brewfs `
  -BinaryPath '\\wsl.localhost\Ubuntu\home\user\brewfs\target\docker\brewfs' `
  -ImageId $env:BREWFS_PERF_IMAGE_ID `
  -MetaUrl 'redis://:password@redis-private-endpoint:6379/0' `
  -S3Endpoint 'https://oss-cn-hangzhou.aliyuncs.com' `
  -S3Bucket brewfs-perf-example `
  -S3Region cn-hangzhou `
  -S3AccessKey $env:BREWFS_PERF_OSS_ACCESS_KEY_ID `
  -S3SecretKey $env:BREWFS_PERF_OSS_SECRET_ACCESS_KEY
```

`-UsePerfImage -ManagedBackend` 模式不在 ECS 上执行 apt、Rust 编译、Docker build、Docker pull、Redis Server 或 RustFS 启动。最终归档包含完整 fio/metadata 结果、commit、binary SHA-256、镜像 ID、OSS/Redis 脱敏配置摘要和清理日志。
### 4. native managed 模式已知坑

这三条都是 2026-09-17 实测中真实踩到并已修复的问题，回归时优先检查：

- **BrewFS 与 JuiceFS 不能共用同一个 Redis 逻辑库。** 两者的 key 布局一致（`i<inode>`、`d<inode>`、`c<inode>_<chunk>`），共用 `db 0` 时 JuiceFS 会把 BrewFS 的目录哈希当成自己的条目解析，在 `meta.parseEntry` 里 panic（`invalid entry`），表现为挂载后 lookup 全部失败。`run_aliyun_managed_perf.ps1` 现在给 BrewFS 用 `db 0`、JuiceFS 用 `db 1`，仍然只需要一台 Redis/Tair。
- **Aliyun OSS 必须走 virtual-hosted 寻址，JuiceFS 侧要用 `--storage oss`。** 路径风格请求会被 OSS 拒绝（`SecondLevelDomainForbidden`）；JuiceFS 的通用 `--storage s3` 对 `*.aliyuncs.com` endpoint 仍会退回 path-style，只有专用 `oss` 存储类型才会使用 `https://<bucket>.<endpoint>`。`run_juicefs_perf_in_container.sh` 现在按 bucket URL 自动选择存储类型，内网 endpoint（`oss-<region>-internal.aliyuncs.com`）同样适用。
- **产物目录由 runner 自己命名。** native runner 会在 `BREWFS_ARTIFACT_ROOT` 下创建 `perf-run-<ts>` 并写入 `perf-summary.tsv`；如果外部把 `BREWFS_ARTIFACT_DIR` 指向公共父目录，报告文件就会直接落在父目录里，归档步骤再也找不到 `*perf-run-*` 目录，整轮测试会在最后一步以 `no performance artifact directory found` 失败。归档逻辑现在同时搜索源树与 `BREWFS_ARTIFACT_DIR`，并且不再覆盖 runner 的目录命名。

### 5. native managed 实测结果（2026-09-17）

同一 VM 镜像、同规格 ECS、同 OSS bucket 形态、同 Tair 实例（BrewFS `db 0` / JuiceFS `db 1`）、非 TLS、完整工具矩阵；每条归档 166 个文件、11 项工具全部 `pass`。

| 项目 | BrewFS | JuiceFS |
| --- | --- | --- |
| 被测版本 | `3e71f64`（= `origin/main`），二进制 sha256 `194b9c84f4d5df431a50829cb0a7c080174c93a3a20462cbdb8e95e8219548a3` | 镜像内置客户端 1.4.1 |
| ECS 规格 | `ecs.u1-c1m2.2xlarge`（8 vCPU / 16 GiB） | 同左，独立临时 ECS，同镜像同数据盘 |
| Result Vault run | `run-20260917-185840-0203fe6c`（`perf-run-1789637682-brewfs-16g`） | `run-20260917-185841-63fd0acb`（`perf-run-1789640165-juicefs-16g`） |
| fio-bigread | 1858.4 MiB/s（p99 25.3 ms） | 2392.5 MiB/s（p99 38.0 ms） |
| fio-seqread | 1177.1 MiB/s（p99 3.9 ms） | 1193.7 MiB/s（p99 3.3 ms） |
| fio-randread | 1399.3 MiB/s（p99 14.6 ms） | 1333.9 MiB/s（p99 15.9 ms） |
| fio-bigwrite | 283.0 MiB/s（p99 238.0 ms） | 196.7 MiB/s（p99 41.2 ms） |
| fio-seqwrite | 321.4 MiB/s（p99 23.7 ms） | 186.1 MiB/s（p99 26.6 ms） |
| fio-randwrite | 320.5 MiB/s（p99 221.3 ms） | 187.9 MiB/s（p99 92.8 ms） |
| fio-randrw 读/写 | 705.4 / 315.7 MiB/s（读 p99 77.1 ms） | 132.7 / 59.3 MiB/s（读 p99 379.6 ms） |
| metaperf / dirperf / dirstress / looptest | 全部 `pass`（202 s / 20 s / 0 s / 7 s） | 全部 `pass`（209 s / 30 s / 6 s / 1 s） |

BrewFS 这条归档是由一次 salvage 规范化而来：native invocation 的测试本体已经跑完并产出完整结果，只有归档步骤因第 4 节第一条目录缺陷以 exit 1 收尾；缺陷修复后 JuiceFS 那条是端到端干净通过的。两条都已在收尾时删除临时 ECS、数据盘、OSS bucket/对象、Tair 实例和二进制中继 run。

归档命名约定：`perf-run-<ts>-<workload>[-<label>]`，例如 `perf-run-1789640165-juicefs-16g`。BrewFS 与 JuiceFS 的报告在 Result Vault 里必须能一眼区分，否则两轮指标几乎同形；run 名取自上传 zip 的文件名，所以改名要连 zip 文件名一起改（`docker/compose-xfstests/run_perf_in_container.sh`、`run_juicefs_perf_in_container.sh` 已按 workload 命名产物目录，`run_aliyun_perf.ps1` 再用 `-RunLabel` 补规格标签，并在归档前兜底重命名）。

**口径警告（2026-09-17 复核，先看这段再看上表）：** 这一轮两条归档不是同口径，写侧和 `fio-bigread` 都不能直接下结论。缺陷已在当晚修复（见本节末尾「对齐修复」），上表只应作为旧归档的历史数字引用。

- `run_aliyun_perf.ps1` 的 `Get-RemoteCommand` 只在 `-Workload juicefs` 时传 `--writeback-throughput-profile`（`$juicefsArgs`，模板变量 `JUICEFS_ARGS`），BrewFS 那轮因此跑的是**默认配置**而非该 profile。归档里的 `perf-profile.env` 可直接验证：BrewFS 侧 `PERF_FIO_POST_WRITE_DRAIN=false`、`PERF_FIO_PREFILL_DRAIN=false`、`PERF_FIO_PREFILL_REMOUNT=false`，且 `BREWFS_WRITEBACK_MODE`、`BREWFS_READ_MEMORY_BYTES`、`BREWFS_S3_MAX_CONCURRENCY`、`BREWFS_FUSE_WORKERS` 等全部为空；JuiceFS 侧对应项为 `true`/已设置。
- JuiceFS 归档的写侧带宽取自 `fully-drained-throughput.tsv`（fio 字节 ÷（active I/O + 写回 drain），drain 92–124 s），而它的 IOPS 取自 fio 前台 JSON；BrewFS 归档没有 drain 行，两侧都是前台数字。数值上可验：BrewFS `70.74 IOPS × 4 MiB = 283 MiB/s` 与图上吞吐自洽，JuiceFS `212.45 × 4 MiB = 850 MiB/s` 却对应图上的 `196.7 MiB/s`。**图上「IOPS 与吞吐互相打架」是口径混用，不是文件系统行为差异。**
- 结论：写场景与 `fio-bigread` 必须在两边都带 `--writeback-throughput-profile` 的情况下重跑后才可作为对比基线；在此之前只应引用读侧的 `fio-seqread`/`fio-randread`（两边都是前台、无 drain 参与）。

**对齐修复（2026-09-17 晚，`run_aliyun_perf.ps1`）：**

- 拆开「workload 选择」与「profile 开关」这两个职责。新增独立模板变量 `WORKLOAD`（只取 `brewfs`/`juicefs`，同时决定 runner 与归档标签），profile 单独走 `PROFILE_ARGS`；native 两条腿现在拿到同形参数 `run_native_perf.sh <workload> --tools "<tools>" --s3 --writeback-throughput-profile`。归档标签改为直接取 `$WORKLOAD`，不再用「profile 参数非空」反推，BrewFS 归档不会再被打成 `juicefs` 标签。
- `PROFILE_ARGS` 支持多个 flag（按空白拆词，例如 `--writeback-throughput-profile --metadata-throughput-profile`）；`args` 从空数组起建，空值不再作为空参数透传给 runner。
- 参数解析层的实测效果：两条腿都拿到 `PERF_FIO_PREFILL_DRAIN=true`、`PERF_FIO_PREFILL_REMOUNT=true`、`PERF_FIO_POST_WRITE_DRAIN=true`；BrewFS 侧 `BREWFS_READ_MEMORY_BYTES`/`BREWFS_WRITE_MEMORY_BYTES` 各 4 GiB、`BREWFS_S3_MAX_CONCURRENCY=16`、`BREWFS_WRITEBACK_UPLOAD_CONCURRENCY=6`，JuiceFS 侧 `JFS_WRITEBACK=true`、`JFS_BUFFER_SIZE_MIB=4096`、`JFS_MAX_UPLOADS=4`、`JFS_MAX_DOWNLOADS=8`。fio 参数本身两个 runner 的默认值逐项相同（`1g`/`512m`/`128m`、numjobs `1`/`4`/`8`、bs `4m`、iodepth `1`、runtime `60`、rwmixread `70`、`io_uring`、direct `0`），本轮没有改动。
- 每次归档新增两份自证文件：`config-parity.txt`（`workload`、`run_label`、`native_mode`、`managed_backend`、`profile_args`、`data_args`、`bench_args`、`perf_tools`、S3 endpoint/bucket/region、脱敏后的 meta url）与 `host-samples.txt`（`stdbuf -oL vmstat -t -w 5` 采样，每 5 秒一行，含 CPU `us/sy/id/wa`、内存 `free/cache`、swap `si/so`、run-queue 与时间戳）。此前 JuiceFS 归档有进程级 CPU/RSS、BrewFS 只有写回计数器，宿主机 CPU/内存数据两边都缺；现在两侧各有一份同格式采样，`vmstat` 缺失时自动跳过而不是失败。
- 自检入口：`bash docker/compose-xfstests/test_native_perf_memory_guard.sh`，以及 `target/perf-main-build/cigate.sh`（归一化 CRLF 后跑三个 report 测试、内存预检与 `bash -n`）。VM 镜像里烘焙的 harness 每轮都会被 Result Vault 上的最新副本覆盖（`run_aliyun_perf.ps1` 的 `refresh_harness`），因此这轮修复不需要重建镜像。
- 需要「默认配置 vs profile」对照时，用 `-WorkloadProfileArgs ''` 让两条腿同时禁用 profile；不要只改一侧，否则又会得到两条不可比的归档。

### 5.1 8 GiB 规格不可行（2026-09-17 实测，无归档产出）

同镜像、同 profile、同工具矩阵，只把 ECS 换成 `ecs.u1-c1m2.xlarge`（4 vCPU / **8 GiB**），BrewFS 那一段没有跑完，也没有产出任何归档。它不是"慢"，是**换页活锁**，现场证据如下：

| 观测项 | 8 GiB 轮（`ecs.u1-c1m2.xlarge`） | 16 GiB 轮（`ecs.u1-c1m2.2xlarge`） |
| --- | --- | --- |
| BrewFS 段耗时 | 45 分钟后仍未结束，只能终止 | 9 分钟 |
| 80 GiB 系统盘（`/dev/xvda`） | 11:12–11:48 UTC 持续 **170.0 MB/s 读**、约 2200 IOPS、读延迟 **55–72 ms**、写流量 0 | 无此形态 |
| 512 GiB 数据盘（`/dev/xvdb`） | 11:13 之后**完全空闲**（整段只有 0.4 GB 写） | 承载 fio 预填充与 BrewFS 缓存 |
| 实例 CPU | 4–10%（阻塞在 I/O，不是计算瓶颈） | 16–61% |
| OSS 对象写入 | 最后一笔 19:06:52，之后 36 分钟无新对象 | 持续写入 |
| Cloud Assistant | 主 invocation 一直 `Running`，新命令一律 `ClientNotRunning`（`aliyun.service` 已无响应） | 正常 |

诊断路径（不依赖登入实例）：`DescribeInstanceMonitorData` 看到实例级读带宽被打满且 CPU 掉到个位数，`DescribeDiskMonitorData` 再按盘拆开，证明压力全在 80 GiB **系统盘**上、数据盘闲置，同时 OSS 无流量——即热点页落在系统盘上反复换入，而不是对象存储或数据盘慢。控制台截图显示 guest 仍停在 login 提示符，说明内核没崩，只是被换页拖住。

机理：`--writeback-throughput-profile` 给 BrewFS 配的是 `BREWFS_READ_MEMORY_BYTES=4GiB` + `BREWFS_WRITE_MEMORY_BYTES=4GiB` + `BREWFS_MEMORY_BUDGET_BYTES=12GiB`，harness 还要额外做 4 GiB fio 预填充。这套内存在 8 GiB 机器上放不下，测试不会收敛。

结论：

- **`--writeback-throughput-profile` 至少需要 16 GiB**；8 GiB 实例不要再用于该 profile 的任何一侧。
- 若确实要在小规格上取数，必须按内存缩放 profile（读写内存缓存与 `BREWFS_MEMORY_BUDGET_BYTES` 同步下调），并且 16 GiB 基线也要用同一份缩放后的 profile 重跑，否则两侧不可比。
- 防呆：`run_native_perf.sh` 现在在**参数解析阶段**做内存预检，`MemTotal < 读缓存 + 写缓存 + 4GiB 预填充 + 2GiB 余量`（BrewFS）或 `< JuiceFS 写回缓冲 + 4GiB 预填充 + 2GiB 余量`（JuiceFS）时直接拒绝启动并打印所需内存，不再产生"跑几十分钟、什么都没有"的账单。2 GiB 余量是给内核、页缓存与 harness 本身留的，见 5.2；没有它的话第 5.2 节对齐后的 8 GiB 预算会正好卡在 8 GiB 机器的边界上被放行。逃生口只有显式的 `BREWFS_PERF_ALLOW_UNDERSIZED=1`。
- 回归脚本：`bash docker/compose-xfstests/test_native_perf_memory_guard.sh`（`bash -n` + 单元用例 + 走真实参数解析的集成用例，覆盖 8 GiB 拒绝与 16 GiB 放行两条路径）。
- 本轮临时资源（ECS、数据盘、两个 OSS bucket、Tair、二进制中继 run）已在终止时全部删除，账号里只剩长期保留的镜像与快照。

### 5.2 预算对齐后的 BrewFS 重测（2026-09-17 晚）

起因是第 5 节「对齐修复」只统一了 profile 开关，没有统一**缓存预算**：BrewFS 腿把 4 GiB 读缓存 + 4 GiB 写缓存常驻内存（`BREWFS_MEMORY_BUDGET_BYTES=12GiB`）再配 8 GiB 读 SSD + 4 GiB 写 SSD，JuiceFS 腿则是 4 GiB 内存 buffer + 8 GiB 磁盘 cache。归档里能直接对比：BrewFS `BREWFS_READ_MEMORY_BYTES=4294967296` / `BREWFS_WRITE_MEMORY_BYTES=4294967296`，JuiceFS `JFS_BUFFER_SIZE_MIB=4096` / `JFS_CACHE_SIZE_MIB=8192`。

修复在 `run_native_perf.sh` 的 `--writeback-throughput-profile` brewfs 分支，把同一档预算拆到 BrewFS 的两层缓存里：

| 层 | BrewFS（本轮） | JuiceFS（本轮） |
| --- | --- | --- |
| 内存缓存 | read 2 GiB + write 2 GiB = **4 GiB** | `--buffer-size 4096` = **4 GiB** |
| 磁盘缓存 | read 4 GiB + write 4 GiB = **8 GiB** | `--cache-size 8192` = **8 GiB** |
| 内存预算 | `BREWFS_MEMORY_BUDGET_BYTES=8589934592` | — |

配套改动：

- `config-parity.txt` 增加 `BREWFS_(READ|WRITE)_(MEMORY|SSD)_BYTES`、`BREWFS_MEMORY_BUDGET_BYTES`、`JFS_BUFFER_SIZE_MIB`、`JFS_CACHE_SIZE_MIB`，归档自带生效预算；`s3_endpoint` 拆成 `s3_endpoint_arg`（driver 入参，公网）与 `s3_endpoint_effective`（`backend.yml`/`juicefs-profile.env` 里实际用的内网 endpoint）。
- 新增 `bash docker/compose-xfstests/test_native_perf_budget_parity.sh`：从 runner 抽出真实参数解析分支跑两条腿，断言内存层与磁盘层预算相等，并要求内存预检值等于"缓存 + 4 GiB 预填充"，防止下次只改一侧。
- `run_aliyun_managed_perf.ps1` 新增 `-Workloads both|brewfs|juicefs`。单条腿只需一台 ECS、一个 Tair、一个 bucket，本次只用 `-Workloads brewfs` 重测 BrewFS。
- 内存预检加 2 GiB 固定余量：预算降到 8 GiB 后，8 GiB 机器会正好落在边界上被放行。

本轮归档：`perf-run-1789651880-8869-brewfs-brewfs-16g-budget`（Result Vault run `run-20260917-214108-83d4527a`），`ecs.u1-c1m2.2xlarge`，11 项工具全部 `pass`，结束后 ECS/Tair/bucket 已删除（复查：账号里只剩用户自己的 ECS，Tair 0 个，bucket 0 个）。

与上一轮 BrewFS 归档（`perf-run-1789647853-17597-brewfs-brewfs-16g`）逐项对比：

| 场景 | run8（4+4 GiB 内存） | run9（2+2 GiB 内存） | 变化 |
| --- | ---: | ---: | ---: |
| fio-bigread | 1712.4 MiB/s（p99 31.6 ms） | 1812.4 MiB/s（p99 25.8 ms） | +5.8% |
| fio-seqread | 679.3 MiB/s（p99 7.0 ms） | 786.7 MiB/s（p99 5.7 ms） | +15.8% |
| fio-randread | 800.1 MiB/s（p99 105.4 ms） | 794.0 MiB/s（p99 103.3 ms） | -0.8% |
| fio-bigwrite | 847.0 MiB/s | 1135.3 MiB/s | +34.0% |
| fio-seqwrite 前台 / drained | 302.0 / 292.2 MiB/s | 317.7 / 307.4 MiB/s | +5.2% |
| fio-randwrite 前台 / drained | 225.3 / 225.3 MiB/s（p99 1250 ms） | 344.1 / 333.0 MiB/s（p99 489 ms） | +52.7% |
| fio-randrw 读/写 drained | 529.2 / 235.1 MiB/s | 465.2 / 207.1 MiB/s | -12.1% |

**上一轮"BrewFS 读侧数字被内存压力污染"的说法站不住，这里撤回。** 预算砍半后宿主机形态几乎没变：`free` 最低点 175 MiB → 185 MiB，`wa` 平均 32.1% → 31.1%（峰值 95% → 82%），两侧 swap 都是 0。原因是 `free` 低本来就不是缓存预算造成的：跑测期间 `vmstat` 的 `cache` 列涨到 7–10 GiB，那是内核对 4 GiB fio 工作集和 FUSE 读数据做的页缓存，Linux 把空闲内存填成页缓存属正常行为。真正异常的是**写阶段** `bo ≈ 256000 块/s` 时 `wa` 冲到 70–80%（读阶段只有 20–29%）。

对齐后的结论：

- **为什么内存减半反而更快：多出来的缓存在这两组 workload 里本来就没干活，只是把队列拉长了。** 归档里的计数器能直接看出来：
  - 读场景的工作集是 fio 反复读的固定文件（`fio-seqread` 1 GiB、`fio-bigread` 1 GiB、`fio-randread` 2 GiB，都是 `size × numjobs`），两侧几乎全命中本地块缓存：run8/run9 的 `brewfs_read_block_cache_hits_total` 分别是 163057/188817（seqread）和 383737/380857（randread），`brewfs_s3_get_ops_total` 是 0 和 192/160。缓存容量在这两组里不是瓶颈，所以砍掉一半内存也伤不到读。
  - 读侧那点差距体现在**单次 FUSE 读延迟**：seqread 334 µs → 278 µs（-17%），正好对应 +16% 吞吐；randread 反而从 298 µs 涨到 353 µs。这是延迟层面的抖动，不是容量效应。
  - 写场景能看出机制。上传并发是固定旋钮（S3 16 / writeback 6 / upload 32），4 GiB 写缓存 + 12 GiB 预算让 run8 把脏数据和 inflight 上传堆到约 2 倍，却没有更多上传工位去消化：`fio-randwrite` 峰值 `max_buffer_dirty` 8196 → 4100 MiB、`max_live_slices` 1322 → 715、`max_stage_inflight` 7359 → 3044 MiB、`max_remote_inflight` 5440 → 3244 MiB。同 60 s 内 run9 发出 6152 个 PUT / 25.8 GB（run8 4051 个 / 16.9 GB），PUT prepare 延迟 5.1 → 3.2 ms，p99 1250 → 489 ms。队列变长只是增加切片合并、staging 拷贝和尾延迟，不增加带宽——`max_remote_inflight` 5.4 GiB 意味着九成 inflight 容量是排队等待而不是在传。
  - 次要因素：读 SSD 缓存同时从 8 GiB 降到 4 GiB，而读缓存与写缓存共用同一块数据盘，常驻读缓存少一半就少了与之争抢的淘汰/写回流量。
  - 口径提醒：每个配置只有一次运行，没有重复样本。能明确超出噪声的是 `fio-randwrite`（吞吐 +53% 且 PUT 数 +52%、prepare 延迟 -38%）；读侧是 -0.8% ~ +15.8%，`fio-randrw` 还退了 12%，所以这不是"内存越少越快"的规律，而是"多余的缓存容量等于多余的队列深度"。
- 预算对齐本身要保留（两条腿现在同量级，且归档可自证），但它解释不了读数差距：读侧只动了 -0.8% ~ +15.8%，写侧反而全面变好，`fio-randrw` 退 12%。单次运行里这是噪声到小效应的量级，不能据此宣称收益。
- BrewFS 对 JuiceFS 的**读侧差距是真实的、不是口径问题**：对齐预算后 `fio-seqread` 786.7 对 1186.8 MiB/s、`fio-randread` 794.0 对 1332.4 MiB/s、`fio-bigread` 1812.4 对 2343.2 MiB/s（BrewFS 的 p99 更好：25.8 对 36.4 ms）。要解释它得看读路径本身（读 SSD cache 落盘、`BREWFS_VERIFY_CACHE_CHECKSUM=full`、下载并发），不能再归因到内存预算。
- 两侧都按 drained 口径比，写侧仍是 BrewFS 领先：`fio-seqwrite` 307.4 对 186.1、`fio-randwrite` 333.0 对 185.0、`fio-randrw` 读 465.2 对 130.5 / 写 207.1 对 58.5 MiB/s；JuiceFS 这三项 drain 要 176–185 s，BrewFS 是 62 s。
- 顺带发现 JuiceFS 1.4.1 上 `max-downloads` 没生效：归档 `juicefs-profile.env` 里同时写着 `JFS_MAX_DOWNLOADS=8` 和 `JFS_MAX_DOWNLOADS_EFFECTIVE=unsupported`，读侧下载并发并没有真拉到 8。下一轮要对齐读侧并发时先解决这个开关。

### 5.3 修好遥测链路后用当前 main 重测（2026-09-19）

第 5.2 节的归档是在 `3e71f64` 上跑的。这轮的目标是：修掉 PR #115 上暴出的问题，再在**当前 main**
（`4c0bae9`）上重跑一次 BrewFS 单腿，数字进同一个 Result Vault。

CI 侧只有一个错误，来自 PR 基线自带的 `reconciler.rs`（`E0425: cannot find function
patch_cluster_status_phase`），该函数在后续 main 提交里已改名 `patch_cluster_status`；把分支 rebase 到
`4c0bae9` 后 CI 全绿。

真正花钱的是遥测链路：native 腿在开机后几十秒内就以 `ExitCode 1` 结束，两轮各烧掉一台 ECS + 一个 Tair +
一个 bucket 的时间。两次都是"镜像里烘焙的 runner 和每轮覆盖上传的 runner 对不上"：

| 轮次 | 云端报错 | 根因 |
| --- | --- | --- |
| run10 | `python3: can't open file '/usr/local/bin/perf_manifest.py'` | `run_perf_in_container.sh` 已经会写 `run-manifest.json`，但 helper 只存在于引入它的那个 checkout 里；镜像烘焙和每轮 harness refresh 都只带了 runner |
| run11 | `/opt/brewfs-perf/native/run_native_perf.sh: line 2: $'\r': command not found` | 所有 payload 都是从 Windows checkout 读的原字节；`core.autocrlf=true` 让新 worktree 里的脚本变成 CRLF，上传 19648 B 的 runner，bash 在第一行就拒绝 |

修复：

- 新增 `tools/perf/perf_manifest.py`，并接进四条供给路径：镜像烘焙（`prepare_native_perf_vm.sh`）、镜像维护
（`maintain_aliyun_perf_image.ps1`）、每轮上传 + refresh（`run_aliyun_perf.ps1`）、compose 镜像
（`Dockerfile`）。
- 三个 payload 构造器统一把 CRLF 归一成 LF 再装箱（`run_aliyun_perf.ps1`、`invoke_native_vm_prepare.ps1`、
`maintain_aliyun_perf_image.ps1`）。run9 之所以没踩到，只是那个 checkout 里脚本碰巧是 LF；同一 commit 的新
worktree 就会失败。
- 新增 `bash docker/compose-xfstests/test_native_perf_harness_payload.sh`：从两个 runner 里抽出所有
`/usr/local/bin/*.py` 引用，断言每个 helper 都有仓库源文件、都随二进制上传、都被两条镜像路径安装、都进 compose
镜像，并要求三个构造器仍然做 CRLF 归一。上面两种失败形态都验证过会被它拦下。顺带把两个一直没进 CI 的 native
测试（`test_native_perf_budget_parity.sh`、`test_native_perf_memory_guard.sh`）接进 `Check perf scripts`。

本轮归档：`perf-run-1789786937-918-brewfs-brewfs-main-4c0bae9-lfharness`（Result Vault run
`run-20260919-111203-cc2b8fc7`），二进制是 `4c0bae9` 的干净构建
（`sha256 52519bf3…ec09`），规格、预算、11 项工具与第 5.2 节完全一致（`config-parity.txt` 可自证：read 2 GiB
+ write 2 GiB 内存、read 4 GiB + write 4 GiB SSD、budget 8 GiB），11 项全部 `pass`，跑完 ECS/Tair/bucket
已删除（复查：账号里只剩用户自己的 ECS，Tair 0 个，bucket 0 个）。

与第 5.2 节（`3e71f64`，同预算同规格）逐项对比：

| 场景 | run9（3e71f64） | run12（4c0bae9） | 变化 |
| --- | ---: | ---: | ---: |
| fio-bigread | 1812.4 MiB/s（p99 25.8 ms） | 2137.8 MiB/s（p99 21.4 ms） | +18.0% |
| fio-seqread | 786.7 MiB/s（p99 5.7 ms） | 906.3 MiB/s（p99 5.0 ms） | +15.2% |
| fio-randread | 794.0 MiB/s（p99 103.3 ms） | 896.4 MiB/s（p99 98.0 ms） | +12.9% |
| fio-bigwrite | 1135.3 MiB/s（p99 42.2 ms） | 1267.3 MiB/s（p99 36.4 ms） | +11.6% |
| fio-seqwrite 前台 / drained | 317.7 / 307.4 MiB/s | 320.2 / 320.2 MiB/s | +0.8% / +4.2% |
| fio-randwrite 前台 / drained | 344.1 / 333.0 MiB/s（p99 489 ms） | 324.7 / 314.2 MiB/s（p99 541 ms） | -5.6% / -5.7% |
| fio-randrw 读/写 drained | 465.2 / 207.1 MiB/s | 491.0 / 217.4 MiB/s | +5.5% / +5.0% |

口径提醒：`3e71f64 → 4c0bae9` 之间只有 operator 调度、TiKV 尾部 GC、trusted xattr 这几个提交，没有碰读路径，
所以读侧 +13% ~ +18% **不能算成代码收益**，更可能是宿主机抖动——本轮宿主机其实更紧（`free` 最低点 149 MiB
对 185 MiB，`wa` 平均 34.5% 对 31.1%，两侧 swap 都是 0）。写侧 `fio-randwrite` 退 5.6% 同理。要结论就得加重复
样本，单轮不够。

与 JuiceFS 的 drained 口径（第 5.2 节的 JuiceFS 腿，`perf-run-1789648512-6450-juicefs-juicefs-16g`）：

| 场景 | BrewFS（4c0bae9） | JuiceFS 1.4.1 | 差距 |
| --- | ---: | ---: | ---: |
| fio-bigwrite drained | 213.0 MiB/s | 190.9 MiB/s | +11.6% |
| fio-seqwrite drained | 320.2 MiB/s | 186.1 MiB/s | +72.1% |
| fio-randwrite drained | 314.2 MiB/s | 185.0 MiB/s | +69.8% |
| fio-randrw drained 总吞吐 | 708.4 MiB/s | 189.0 MiB/s | +274.8% |

读侧差距仍在，且这轮没有针对它做任何改动：`fio-bigread` 2137.8 对 2343.2、`fio-seqread` 906.3 对 1186.8、
`fio-randread` 896.4 对 1332.4 MiB/s。下一轮优化应该从这里入手，不要再拿内存预算解释。

### 5.4 合并全部读路径 PR 后的完整两腿重测（2026-09-19 晚）

第 5.3 节的 `4c0bae9` 只是遥测修复。之后用户把 PR 全部合并，`origin/main` 前进到 **`de551df`**，含三项与本轮
相关的改动：#115 的 native managed harness、#121 cold-read v2 full-GET reuse、#122 readdirplus 批量属性。
本轮在 `de551df` 上重跑完整两腿，确认这些改动在真实云端环境里的表现。

二进制来自 `brewfs-aliyun-perf` 的干净检出（commit `de551df248f4927c714f7e844841d8c42d94b3b1`，`dirty=0`，
68,277,064 B，`sha256 d2d7ef5de23851f705fd221ff7524c2c7e2b266a229a456c89e9d0fcc5646784`），由
`target/build-commit.sh` 在 WSL 里 release 构建后上传；云端不编译。两腿规格、预算、11 项工具与第 5.2/5.3 节
一致（`config-parity.txt` 可自证：BrewFS read 2 GiB + write 2 GiB 内存、read 4 GiB + write 4 GiB SSD、
`BREWFS_MEMORY_BUDGET_BYTES=8589934592`；JuiceFS `JFS_BUFFER_SIZE_MIB=4096` + `JFS_CACHE_SIZE_MIB=8192`），
同一个 Tair 实例上 BrewFS 用 db0、JuiceFS 用 db1，各自独立 bucket。

归档：

| 腿 | run 名 | Result Vault | 文件数 | 状态 |
| --- | --- | --- | ---: | --- |
| BrewFS `de551df` | `perf-run-1789790119-14469-brewfs-brewfs-main-de551df` | `run-20260919-120458-3e41c29a` | 193 | pass |
| JuiceFS 1.4.1 | `perf-run-1789790756-6109-juicefs-juicefs-main-de551df` | `run-20260919-122228-f982b4a7` | 168 | pass |

两腿 11 项全部 `pass`；日志里唯一的 ERROR 是既有的「当前 JuiceFS 不支持 `--max-downloads`」，属已知项。
跑完 ECS/Tair/bucket 已删除，复查账号只剩用户自己的 ECS、Tair 0 个、bucket 0 个。

与第 5.3 节（`4c0bae9`，同预算同规格）逐项对比：

| 场景 | 4c0bae9 | de551df | 变化 |
| --- | ---: | ---: | ---: |
| fio-bigread | 2137.8 MiB/s（p99 21.4 ms） | 2240.7 MiB/s（p99 21.1 ms） | +4.8% |
| fio-seqread | 906.3 MiB/s（p99 5.0 ms） | 984.3 MiB/s（p99 4.6 ms） | +8.6% |
| fio-randread | 896.4 MiB/s（p99 98.0 ms） | 940.4 MiB/s（p99 80.2 ms） | +4.9% |
| fio-bigwrite | 1267.3 MiB/s（p99 36.4 ms） | 1442.3 MiB/s（p99 29.8 ms） | +13.8% |
| fio-seqwrite 前台 / drained | 320.2 / 320.2 MiB/s | 322.9 / 312.5 MiB/s | +0.8% / -2.4% |
| fio-randwrite 前台 / drained | 324.7 / 314.2 MiB/s（p99 541 ms） | 324.1 / 313.6 MiB/s（p99 549 ms） | -0.2% / -0.2% |
| fio-randrw 读/写 drained | 491.0 / 217.4 MiB/s | 517.4 / 229.9 MiB/s | +5.4% / +5.8% |

#121 的效果有硬证据，不靠吞吐：`fio-randread`、`fio-randwrite`、`fio-randrw-prefill` 三条场景的
`s3_get_ops` 从 **194 → 96（-50.5%）**，即每个 512 MiB 工作集的回源 GET 直接砍半。吞吐侧的 +4.8% ~ +13.8%
则要和宿主机抖动一起看：本轮 `free` 最低点 173 MiB、`wa` 平均 33.8%，上一轮 149 MiB / 34.5%，两侧 swap 都是
0，属同一噪声量级；`fio-randrw` 写 p99 从 64.8 ms 涨到 147.8 ms 是这里唯一的负面信号，绝对值基数小（写侧
drained 仍 +5.8%），先记为待观察项。

与同轮 JuiceFS 1.4.1 的对比（前台带宽 / p99）：

| 场景 | JuiceFS 1.4.1 | BrewFS de551df | BrewFS 相对 |
| --- | ---: | ---: | ---: |
| fio-bigread | 3056.7 MiB/s（p99 32.6 ms） | 2240.7 MiB/s（p99 21.1 ms） | -26.7% |
| fio-seqread | 1551.0 MiB/s（p99 2.6 ms） | 984.3 MiB/s（p99 4.6 ms） | -36.5% |
| fio-randread | 1798.8 MiB/s（p99 12.0 ms） | 940.4 MiB/s（p99 80.2 ms） | -47.7% |
| fio-bigwrite | 885.0 MiB/s（p99 31.6 ms） | 1442.3 MiB/s（p99 29.8 ms） | +63.0% |
| fio-seqwrite | 554.5 MiB/s（p99 35.4 ms） | 322.9 MiB/s（p99 33.4 ms） | -41.8% |
| fio-randwrite | 574.5 MiB/s（p99 99.1 ms） | 324.1 MiB/s（p99 549.5 ms） | -43.6% |
| fio-randrw 读/写 | 343.6 / 153.3 MiB/s | 534.7 / 237.6 MiB/s | +55.6% / +55.0% |

写侧必须按 drained 口径读，否则结论会反（数据来自 `fully-drained-throughput.tsv`）：

| 场景 | BrewFS de551df | JuiceFS 1.4.1 | BrewFS 相对 |
| --- | ---: | ---: | ---: |
| fio-bigwrite drained | 217.4 MiB/s（drain 4 s） | 246.3 MiB/s（drain 3 s） | -11.7% |
| fio-seqwrite drained | 312.5 MiB/s（drain 2 s） | 180.8 MiB/s（drain 124 s） | +72.8% |
| fio-randwrite drained | 313.6 MiB/s（drain 2 s） | 188.4 MiB/s（drain 123 s） | +66.4% |
| fio-randrw drained 读/写 | 517.4 / 229.9 MiB/s（drain 2 s） | 133.2 / 59.4 MiB/s（drain 95 s） | +288% / +287% |

也就是说前台 JuiceFS 的写吞吐优势全部来自「写内存 + 慢慢回源」：它每条写场景要 95 ~ 124 s 才排空，BrewFS 只要
2 ~ 4 s。按真正落地完成的口径，BrewFS 除 `fio-bigwrite` 外全面领先。读侧差距比第 5.3 节更大（JuiceFS 这轮读的
绝对值显著高于上一轮：bigread 2343→3057、seqread 1187→1551、randread 1332→1799 MiB/s），且只有
`fio-randread` 的 p99 是 BrewFS 明显更差（80.2 对 12.0 ms）。宿主机这边 JuiceFS 腿反而更宽裕（`free` 最低点
186 MiB、`wa` 平均 14.8%，199 个采样是因为要等长 drain），所以读侧差距不能归因于宿主机被拖累，下一轮优化重点
仍在这里。

口径提醒：JuiceFS 每轮只跑一次，本节这组数字就是后续的 JuiceFS 基准；之后不再重复测 JuiceFS，常规迭代只跑
BrewFS 单腿（第 3 节入口），需要刷新 JuiceFS 基准时再单独说明。

### 6. 成本控制

默认策略是「用完即删」。2026-09-17 一轮结束后，账号里只应剩用户自己的 ECS、它的系统盘，以及一块长期 VM 镜像和它的快照。

- 每轮创建的 ECS、随实例删除的数据盘、临时 OSS bucket/对象、临时 Tair 实例和二进制中继 run 都在 `finally` 中无条件删除，失败、超时、Cloud Assistant 中断同样走清理路径；ECS 还会设置 `AutoReleaseTime`（`-AutoReleaseMinutes`，默认 240 分钟）作为驱动脚本被强杀时的兜底。
- 默认规格为 `ecs.u1-c1m2.2xlarge`（8 vCPU / 16 GiB）+ 80 GiB 系统盘 + 512 GiB `cloud_essd` PL2 数据盘 + 20 Mbps 按量带宽。只调试一个 workload 时用第 3 节的单 workload 入口，不要为了省事重跑两遍全量矩阵。
- 长期保留的只有一份 VM 镜像 `<image-id>` 及其系统快照 `<snapshot-id>`（账号私有资源，不必写进仓库），它们是每轮仅上传二进制的必要条件。镜像维护过程中产生的旧基础镜像/快照属于迭代产物，用完立即删除。
- `aliyun ecs DeleteImage --Force true` 实测**不会**连带删除镜像对应的快照，删完镜像必须再显式 `aliyun ecs DeleteSnapshot --SnapshotId <id>`，否则快照继续计费。

收尾检查清单：

```powershell
aliyun ecs DescribeInstances    --RegionId cn-hangzhou
aliyun ecs DescribeDisks        --RegionId cn-hangzhou
aliyun ecs DescribeImages       --RegionId cn-hangzhou --ImageOwnerAlias self
aliyun ecs DescribeSnapshots    --RegionId cn-hangzhou
aliyun vpc DescribeEipAddresses --RegionId cn-hangzhou
aliyun r-kvstore DescribeInstances --RegionId cn-hangzhou
aliyun oss ls
```

## ACK/Kubernetes 主流程

性能测试的推荐路径是本地构建镜像后交给 ACK 运行，避免在临时 ECS 上冷编译。`run_aliyun_perf_k8s.ps1` 在本地 Docker builder 中构建 BrewFS 或 JuiceFS 镜像，推送到 registry，然后在已有 ACK 集群中创建特权 FUSE Job。默认 `-ServiceMode managed` 不在 ACK 内创建 Redis、TiKV 或 RustFS；请传入托管 Redis/TiKV 和 OSS endpoint。`-ServiceMode embedded` 仅用于兼容旧的自包含测试。

```powershell
$env:BREWFS_RESULTS_URL = 'https://results.example.com'

.\docker\compose-xfstests\aliyun\run_aliyun_perf_k8s.ps1 `
  -KubeconfigPath $env:KUBECONFIG `
  -RegistryImage ghcr.io/ivanbeethoven/brewfs-perf `
  -GhcrUsername Ivanbeethoven `
  -GhcrToken $env:GHCR_TOKEN `
  -Backend redis -MetaUrl 'rediss://:password@r-xxxxx.redis.rds.aliyuncs.com:6379/0' `
  -S3Endpoint 'https://oss-cn-hangzhou.aliyuncs.com' -S3Bucket brewfs-data `
  -S3Region cn-hangzhou -S3AccessKey $env:OSS_ACCESS_KEY_ID `
  -S3SecretKey $env:OSS_SECRET_ACCESS_KEY `
  -ArtifactDirectory .\docker\compose-xfstests\artifacts\ack-redis
```

JuiceFS 使用相同的 ACK Job、OSS、Redis endpoint、fio 工具矩阵和结果上传路径，只切换 workload 与镜像：

```powershell
.\docker\compose-xfstests\aliyun\run_aliyun_perf_k8s.ps1 `
  -Workload juicefs -RegistryImage ghcr.io/ivanbeethoven/juicefs-perf `
  -GhcrUsername Ivanbeethoven -GhcrToken $env:GHCR_TOKEN `
  -Backend redis -MetaUrl 'rediss://:password@r-xxxxx.redis.rds.aliyuncs.com:6379/0' `
  -S3Endpoint 'https://oss-cn-hangzhou.aliyuncs.com' -S3Bucket juicefs-data `
  -S3Region cn-hangzhou -S3AccessKey $env:OSS_ACCESS_KEY_ID `
  -S3SecretKey $env:OSS_SECRET_ACCESS_KEY `
  -ArtifactDirectory .\docker\compose-xfstests\artifacts\ack-juicefs
```

测试完成后脚本会在本地输出两个结果：完整结果目录和同名 `.zip` 归档。设置 `BREWFS_RESULTS_URL` 后，脚本会自动把同一个 ZIP POST 到网站；`-ResultVaultUrl` 可临时覆盖环境变量。Result Vault URL 缺失、无效或上传失败都会使任务失败，但本地归档仍会保留，避免静默丢结果。归档包含性能报告、原始日志、BrewFS/JuiceFS 日志、后端诊断和性能统计，便于上传或脱离集群查看。脚本会在容器中先生成单个 `tar.gz` 再下载，避免逐文件复制时出现 `unexpected EOF`。

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
- ECS 镜像内置 Cloud Assistant Agent，且能访问软件源和 GitHub/GHCR；使用预制镜像时，这些依赖只在镜像维护阶段安装。
- 预制镜像由 `maintain_aliyun_perf_image.ps1` 创建；原始 Ubuntu 镜像默认是 `ubuntu_24_04_x64_20G_alibase_20260522.vhd`。

## 使用方式

```powershell
# 传统 source 模式仍可用：创建临时 ECS，跑测试后自动释放 ECS
.\docker\compose-xfstests\aliyun\run_aliyun_perf.ps1 `
  -Action run `
  -VSwitchId vsw-xxxxxxxx `
  -SecurityGroupId sg-xxxxxxxx `
  -RegionId cn-hangzhou `
  -ZoneId cn-hangzhou-h `
  -ZoneId ap-northeast-2a `
  -Backend redis `
  -DataBackend s3 `
  -Ref main

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

默认 ECS 为按量付费，并设置四小时自动释放时间；`run` 无论成功、失败、超时还是 Cloud Assistant 重启，都会主动释放本轮实例及其随实例删除的数据盘。脚本不会删除预制 VM 镜像、快照、VPC、vSwitch、安全组或其他账号级长期资源；Result Vault 临时上传 run 会在同一个 `finally` 中删除。
