# 已合并 PR #407 / #409 回退复查（2026-11-04）

本报告审计已合并改动，执行「负优化不要 merge」的配对 A/B + 95% CI 要求。
生产代码不变，不自行 revert，不合并本报告 PR。原始证据根目录：
`/mnt/brewfs-data/home/hxy/regress-20261104/`（下文路径均相对此目录）。

**结论（primary cohort）：#407 的 B/C/D/E 未触发预设回退门；#409 的 E
纯 `readdir` 触发真回退。** #409-E 的 main 中位数为 1,601.369900 ms，父提交
为 929.291745 ms，配对变化 `+68.53%`，95% CI `[+44.42%, +96.66%]`，
配对 log t=14.548、p=0.004692；三对的 main 都更慢。该结果应交 #391 编排者
决策，本文不自行回退 main。A/B 比较包含父提交与候选之间的其他提交，因此这
是合并差异的回退证据，不能单独证明回退只由 #409 引入。

测量于 2026-11-04 17:37:50–20:47:40 UTC 完成，60/60 轮有效、没有失败或无效
性能轮次、无编译交叠、无卸载挂起或排空超时。任务指定的六小时时限已超出，本文
不声称按时完成。

## 比较范围

- 候选固定为 `d1160dc67cc083e5ab3279be8cc34c57eb026254`。
- #407 父提交：`1bd4d75f8c7c77188f52e025dcd315520353cd65`。
- #409 父提交：`e7135c541a6a3ae3824205200123a55a9afad3d5`。
- A 臂为对应父提交，B 臂为 main；每场景严格 A/B/A/B/A/B，三对独立新挂载。
  两组比较共需三份源版本，各版本二进制先构建并归档，再统一测量。
- 这是用户指定的 main 对历史父提交审计，包含区间内其他提交；不能仅凭该比较
  把所有差异归因给 #407 或 #409。#407 实际修改大块压缩探测、组帧复制及 S3
  vectored body，4 KiB 小写对象聚合属于其父版本已含的 #404。

## 固定方法

同一主机、固定 Compose 镜像、同一套配置和构建参数。每轮新容器、独立空本地
状态目录及新的 FUSE 进程，保存挂载 PID、mountinfo 和卸载结果。flat 每轮使用
独立 S3 bucket 并清空本任务 Redis；packed 每个版本单独生成其格式布局。
不触碰 #402/#406/#414/#416 或工作树已有未提交文件，不依赖 GitHub Actions。

构建经 `/home/hxy/shim/cargo`：MemoryMax=26 GiB、MemorySwapMax=8 GiB、
CPUQuota=700%、IOWeight=50，shim 注入 `CARGO_BUILD_JOBS=3`。启动前清理
可回收宿主缓存并要求 MemFree ≥26 GiB。各 cargo 调用及源版本顺序执行。

```bash
cargo +1.99.0 rustc --locked --release --no-default-features \
  --features fuse-tokio-runtime,workspace-overlay,observability \
  --bin brewfs -- -C opt-level=1 -C debuginfo=0
# main 与 #409 父提交的 packed_v3_snapshot_fixture 只需替换 --bin。
```

实际需要三份 CLI，以及 main / #409 父提交的两份 packed fixture。
#407 只运行 flat-v1 场景，不使用 packed fixture。

库保留 release opt-level=3，最终 bin opt-level=1；`RUSTFLAGS=-C debuginfo=0`、
release debug=0、incremental=0。此口径沿用 #409 的匹配构建。
原始命令、退出码、rustc 版本、源码提交和二进制 SHA-256 保存在 `build/`。
固定 Rust 1.99.0 (`b940084d7`, LLVM 23.1.1)，并逐次核对 rustc、driver 和
LLVM 共享库的构建前后 SHA-256，防止宿主工具链维护改变比较条件。
每秒记录测量期间的 cargo/rustc/编译器进程，存在交叠则该轮无效。

每轮为一个统计样本，单轮内 4,096 次 fsync 或 8,192 个文件不能充当独立重复。
报告两臂中位数。对每对 `log(main/parent)` 做双侧配对 t 检验，n=3、df=2，
95% CI 临界值 4.30265273；转换为百分比报告几何平均配对差值与区间。
同时保留未取对数的配对 t、均值差和绝对单位 CI。

判定遵循预设规则：B/C/D/E 中任一预设指标的回退点估计 >5%，且 95% CI
排除 0，即触发回退；不以未测收益抵消。带宽下降为坏，时延/耗时上升为坏。
区间跨 0 是证据不足，不能改写成「无回退」；只有区间也排除 >5% 回退时
才能在该指标上给出相应界限。区间为逐指标区间，未做多重比较校正。
失败和无效轮次保留，完整有效配对才进入统计；修复 harness 后使用新 cohort。

写入同时报告 active、active+严格 drain、完整 wall+严格 drain 的带宽。
严格 drain 要求 dirty/live dirty/pending/inflight/metadata 等债务归零，PUT
计数稳定三个样本；延迟关闭、排空超时、卸载挂起均不能作为有效性能数字。

## #407：五场景

flat-v1 + Redis + RustFS，zstd level 1，4 MiB block / 64 MiB chunk，
coalesce 64 KiB / 0 ms，Tokio worker=1、FUSE workers=16，S3/upload 并发=8/10。
read RAM/SSD=0/0，write RAM=512 MiB、write SSD=0，总预算=2 GiB，
prefetch/keep-cache/writeback cache 关闭，upload-before-commit。

| 场景和指标 | 父提交中位数 | main 中位数 | 配对变化 [95% CI] | 配对 t / p | 判定 |
| --- | ---: | ---: | --- | --- | --- |
| A 顺序写，4 MiB，1 job × depth 8，buffered，60 s；active / wall+drain MiB/s | 151.993203<br>142.866928 | 183.272243<br>165.731315 | +19.78% [+15.96, +23.72]<br>+15.20% [+11.19, +19.36] | 23.959 / 0.001737<br>17.178 / 0.003372 | 对照：改善 |
| B 随机写，4 KiB，4 jobs × depth 32，direct=1；active / wall+drain MiB/s；write p50 / p99 ms | 4.798014<br>4.494312<br>95.944704<br>256.901120 | 4.863321<br>4.542393<br>91.750400<br>274.726912 | +0.84% [-3.07, +4.90]<br>+0.66% [-2.42, +3.84]<br>-3.27% [-10.02, +3.99]<br>+9.11% [-2.93, +22.65] | 0.907 / 0.4603<br>0.909 / 0.4591<br>-1.978 / 0.1866<br>3.208 / 0.08499 | 未触发；write p99 区间跨 0，不能排除 >5% 回退 |
| C 随机混合 70/30，4 KiB，4 jobs × depth 32，direct=1；active / wall+drain MiB/s；read/write p50 / p99 ms | 0.354539<br>0.332608<br>1400.897536<br>1837.105152<br>1367.343104<br>1786.773504 | 0.358206<br>0.336686<br>1400.897536<br>1786.773504<br>1350.565888<br>1736.441856 | +0.44% [-16.56, +20.90]<br>+0.56% [-16.60, +21.24]<br>+0.38% [-19.11, +24.55]<br>-0.60% [-9.56, +9.24]<br>-0.41% [-18.70, +21.99]<br>-0.32% [-9.79, +10.13] | 0.102 / 0.9282<br>0.128 / 0.9101<br>0.075 / 0.9473<br>-0.276 / 0.8086<br>-0.087 / 0.9384<br>-0.140 / 0.9014 | 未触发；区间较宽，证据不足以排除小幅变化 |
| D 8,192 × 4 KiB 顺序创建/关闭/严格排空；durable / create-close / p99 ms/file | 20.773844<br>20.391152<br>35.163632 | 20.540089<br>20.167512<br>33.654859 | -0.99% [-3.89, +2.00]<br>-1.02% [-3.88, +1.93]<br>-3.84% [-7.92, +0.42] | -1.443 / 0.2859<br>-1.505 / 0.2712<br>-3.887 / 0.06026 | 各指标区间排除 >5% 回退 |
| E 4,096 次 4 KiB write 后逐次 fsync；p50 / p99 ms；总耗时 s | 115.630352<br>128.925787<br>482.550255 | 115.387246<br>129.985574<br>481.243940 | -0.13% [-0.82, +0.56]<br>+0.33% [-3.90, +4.74]<br>-0.20% [-0.90, +0.51] | -0.817 / 0.4996<br>0.326 / 0.7756<br>-1.196 / 0.3544 | 各指标区间排除 >5% 回退 |

fio 每 job 工作集 512 MiB，libaio，refill/scramble buffers；三对 seed 分别
20261104/20261105/20261106，两臂相同。C 先完整预填充、严格 drain 并重挂载。
D 的主耗时包含 close 后严格排空，随后逐字节验证全部文件；E 单独计 fsync
系统调用的 p50/p99，并保留 write/fsync/close 总耗时和全量内容校验。

历史 +37.51% 使用 1 MiB fio 块及另一套环境。本次按要求用 4 MiB，历史数字
只是参考值，不是本轮对照样本。实际是否重现相同幅度由本表决定。

## #409：五场景

8,192 × 4 KiB，wire005、size-only、sequential-small-file、inline on，
metadata/data codec 均 raw，metadata cache=256 MiB，payload cache=0，
FUSE read direct I/O=1、keep-cache=0、TTL=0、payload prefetch=off。
两臂分别用自身 fixture 生成快照，因为本 PR 改变了打包边界，需重新打包。

历史 #409 PR 正文其实已经披露 stat median/p99、首文件读与混合写尾延迟
问题：其中三对 mixed fio 的 write p99 为 +42.94%，且明确写了
「Keep draft; do not merge with the current failures/gaps」。本轮指定的 D
是顺序扫描后随机读 10%，并非 mixed fio 写尾测试；这五项复查即使通过也不能
消除该历史风险。历史热态对照的 2.9738 → 0.5756 ms/file（约 -80.64%）与本轮
固定 wire005、8,192 文件 fixture 的 A 不是同一测量口径；本轮 A 为 -51.34%，
不能要求两者数值完全相等。历史正文原样保留于 `analysis/pr-409-historical.json`。

| 场景和指标 | 父提交中位数 | main 中位数 | 配对变化 [95% CI] | 配对 t / p | 判定 |
| --- | ---: | ---: | --- | --- | --- |
| A 目录遍历预热后的顺序全量扫描；scan / 预热+扫描 ms/file | 10.317406<br>10.427731 | 5.083283<br>5.266224 | -51.34% [-56.32, -45.79]<br>-50.23% [-55.40, -44.45] | -28.692 / 0.001213<br>-27.337 / 0.001335 | 对照：改善 |
| B drop caches + remount 后首轮全量读；scan / mount+scan ms/file | 10.837187<br>11.326394 | 5.404388<br>5.897360 | -51.21% [-55.77, -46.17]<br>-48.97% [-53.20, -44.36] | -31.463 / 0.001009<br>-33.461 / 0.0008920 | 各指标区间排除 >5% 回退 |
| C 新快照发布后立即全量读回 verify，ms/file | 9.684921 | 4.298459 | -55.68% [-57.64, -53.61] | -77.049 / 0.0001684 | 各指标区间排除 >5% 回退 |
| D 先顺序全扫，再随机读 10%（820 文件）；p50 / p99 ms/file | 11.364997<br>21.730176 | 4.101648<br>7.399032 | -63.51% [-66.15, -60.66]<br>-65.16% [-68.38, -61.61] | -57.741 / 0.0002998<br>-46.785 / 0.0004566 | 各指标区间排除 >5% 回退 |
| E 只 readdir，无文件 stat/open/read，ms | 929.291745 | 1601.369900 | **+68.53% [+44.42, +96.66]** | **14.548 / 0.004692** | **触发回退** |

A 沿用历史 `packed-tree` → `packed-smallfiles` 定义，并另外计入预热成本。
B 强制成功执行 `echo 3 > /proc/sys/vm/drop_caches`，确认旧进程退出和新的
挂载 PID，再执行首读；单独报告挂载耗时及挂载+扫描。C 不在发布与校验之间
人为清缓存：不可变 packed 挂载只读，所谓写完是该版本完成新快照对象发布；
本项不替代可写 workspace upper 的持久性验证。D 随机子集及顺序按配对 seed
固定，保留 p99；E 用 `os.scandir` 的名字枚举。全部场景测后逐字节与 SHA-256
验证完整语料，测前不以完整校验意外预热。

父提交 fixture 生成 8,192 个文件、32 个 groups，main fixture 生成 8,192 个
文件、147 个 groups；两臂均为 32 MiB 逻辑数据。每个 #409 轮都在独立挂载中
完成 8,192 个文件的内容和 SHA-256 校验；E 的 `readdir.json` 明确记录
`operations=1`、`content_sha256={}`，没有打开或读取文件。

## 测量结论与回退处置

主 cohort 的 60 个 status 全部 `valid=true`：每轮编译器采样为空，卸载返回 0
且进程已退出。#407 的 B/C/D/E 均没有出现「点估计回退超过 5% 且 95% CI
排除 0」的指标；B 的 write p99 点估计为 +9.11%，但 CI 为 `[-2.93%,
+22.65%]`，因此按预设规则只能记为未触发、证据不足。#409 的 A/B/C/D 均为
显著改善，E 则三对逐一变慢（+74.92%、+56.86%、+74.44%），触发回退门。

完整三对原始值、log 配对 t、原单位配对 t 和区间见
`analysis/primary-summary.json`；渲染表为 `analysis/primary-tables.md` 和
`analysis/primary-all-statistics.md`。#407 的 fio 补充比较也显示三对均无
`post_write_drain_timeout`，active、active+drain、wall+drain 结果方向一致；
这些补充值没有替代上表的配对判定。

复现 #409-E（先启动本任务 Redis/RustFS 服务，使用已归档二进制，不要编译）：

```bash
audit=/mnt/brewfs-data/home/hxy/regress-20261104
docker compose -p regress20261104 \
  -f /home/hxy/brewfs-main/docker/compose-xfstests/docker-compose.redis-perf.yml \
  -f "$audit/harness/compose.yml" up -d --wait redis rustfs
python3 "$audit/harness/campaign.py" measure \
  --cohort confirm-409-e --scenes 409-E --pairs 3
python3 "$audit/harness/analyze.py" confirm-409-e
```

对 #409-E 有三种处置建议，交由 #391 编排者选择：

1. **加开关**：给 packed 小文件目录枚举路径增加运行时开关，默认使用已知不回退
   的旧枚举路径；待修复并重新通过配对 CI 后再打开。
2. **限制形态**：仅在已验证的顺序打开/verify 形态启用新布局；纯 `readdir` 或
   groups 很多的目录继续走旧路径，避免把本轮 147 groups 的回退暴露给用户。
3. **完全回退**：由编排者决定是否回退 #409 的 packed 小文件变更；本审计不
   直接改动或 revert main。

## Harness 审计及失败记录

测量基于仓库 Compose runner 的固定副本，并用 runtime bind mount 指向各臂
已编好的二进制；不调用会重新编译的宿主包装器。原 runner 与补丁保存在
`harness/upstream-runner.sh`、`harness/runner.sh` 和 `analysis/runner.patch`。
补丁明确 zstd level 1，并去掉 profile 参数恢复时重复的第二个 `--name`，避免
fio 意外创建第二组任务。自定义 workload 和挂载清理 wrapper 位于 `harness/`，
源码哈希固定于 `protocol.json`；本 PR 仅提交文档，不修改生产或仓库 harness。

- 前一启动留下的构建被 SIGTERM 中断，没有完成的二进制或测量轮次，原样保留于
  `build-attempt-01/`。
- 本次首个 shim 构建发现 root 执行 Git 因工作树所有权得到 `commit=unknown`。
  主动停止，保留 `build-attempt-02/`；只将本次隔离工作树加入 Git safe.directory，
  随后重新构建并核对嵌入提交身份。产品源码字节未变化。
- 20:49–21:17（北京时间）的下一次构建在运行约 27 分 43 秒后收到 SIGBUS。
  同机另一个维护脚本 `disk-opt-3.sh` 对正在使用的 Rust 工具链 ELF 执行 strip；
  其后 `fix-toolchain.sh` 重装 stable，使版本从 1.99.0 变成 1.99.1。
  本次 cgroup 峰值 20.42 GiB、OOM/max 事件均为 0，无完成的二进制或性能轮次。
  诊断和维护脚本副本在 `build-attempt-03/`；审计单独安装并固定原 1.99.0，
  所有版本重新使用同一编译器。该失败不能记为代码测试失败或性能回退。
- 自定义七种 workload 在普通临时目录的正确性 smoke 均通过，等长内容损坏被
  verify 拒绝；这些耗时不进入性能统计。记录：`analysis/harness-checks.json`。
- 23:49（北京时间）主动停止未被 #407 任何场景使用的父臂 packed fixture
  编译，以保留测量时间。发现 shim 外层在 SIGTERM 后仍返回 0，driver 因而
  复制了 target 中旧的 main fixture；该复制品立即隔离并从有效二进制清单排除。
  原始退出码和误导性的 driver `FINISH` 行原样保留，明确标记为无效；它从未
  生成 fixture 或参加测量。所有实际使用的五份二进制除哈希/提交检查外，还必须
  有真实 Cargo `Finished` 记录。证据：`build/unused-fixture-cancellation.json`。
- 最后一个实际使用的 fixture 在 17:32:56 UTC 复制完成后，构建 driver 报告
  `harness/build.sh: line 64` 的 shell 引号未闭合，原始 `build/driver.exit=2`
  保留。随后逐个核对五份实际使用二进制的 Cargo
  `Finished`、Exit 0、提交身份、SHA-256 和工具链前后哈希，全部通过；
  `build/driver-postcheck.json` 只接受这一条尾部错误，未重编或修改二进制，
  `build/complete` 明确记录该 driver exit 2 被保留。
- #423 既有本地测试日志保留于 `analysis/pr423-existing-gate/`：1,325 个库测试
  和 2 个 binary 测试通过，0 failed；其测试提交 `ff9f06f` 与 `d1160dc` 的
  Git tree 相同。这是历史门禁证据，不冒充本轮新跑的全量 CI。本任务只提交
  审计文档，没有新 Rust 改动或新优化的接受决定；性能测量阶段不运行 cargo。

## 复现与证据

```bash
audit=/mnt/brewfs-data/home/hxy/regress-20261104
# 本轮执行记录：先完成所有版本构建；性能测量期间禁止编译。
bash "$audit/harness/build.sh"
python3 "$audit/harness/campaign.py" bootstrap
python3 "$audit/harness/campaign.py" measure --cohort primary --pairs 3
python3 "$audit/harness/analyze.py" primary
```

重复测量使用已归档二进制和新的 cohort 名，不在原目录重新运行 build/bootstrap：

```bash
docker compose -p regress20261104 \
  -f /home/hxy/brewfs-main/docker/compose-xfstests/docker-compose.redis-perf.yml \
  -f "$audit/harness/compose.yml" up -d --wait redis rustfs
python3 "$audit/harness/campaign.py" measure --cohort repeat-01 --pairs 3
python3 "$audit/harness/analyze.py" repeat-01
```

`campaign.py one --scenes <407-B 等> --pair <1..3> --arm <parent407|parent409|main>
--cohort <新的名称>` 可复现单轮。每个 `artifacts/<cohort>-<scene>-r<pair>-<arm>/`
保留解析后的 Compose、完整环境/argv、原始 fio JSON/逐次延迟、1 Hz 主机与
writeback 采样、阶段纳秒时间戳、校验摘要、挂载与卸载记录和 `status.json`。
完整统计在 `analysis/<cohort>-summary.json`，包含原始三对数值、两种配对 t
检验及 CI，不能只从摘要表反推原始数据。

## 后续决策

#409-E 的回退幅度、CI、复现命令和三种处置路径已在「测量结论与回退处置」中
列出；结论提交 #391 由编排者决定，不自行更改或 revert main。
