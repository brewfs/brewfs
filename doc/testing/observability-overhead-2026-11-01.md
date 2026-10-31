# 默认构建 2.45% 差异判定

更新时间：2026-10-31T21:15:31.082037+00:00

**结论：噪声占主导：默认构建在测量噪声内（无可检出开销）。** 同一个二进制的 A/B 中位数差为 -2.858%，差异幅度超过原来的 -2.447%；实际 baseline 与 default 构建中位数差为 +0.633%，没有复现稳定的 -2.45%。实际配对差异正负跳动，配对几何均值差为 +0.932%，95% CI 为 [-3.212%, +5.254%]，包含 0。

噪声结论记录于 2026-10-31T20:37:30.654916+00:00，任务第 69.24 分钟；其后的 guard 与 feature 门禁结果补充在本报告中。

原三轮 randread 的 -2.446887% 保留为历史观测；验收口径更新为「在测量噪声内」。这里的「无可检出开销」不表示数学上证明开销恰好等于零，也不把置信区间当作等效性检验。

## 实验设计与构建

- main 基线：`cba857b567fa6e94d50edd1d2e1c47c25c7b2e50`；rebase 后候选：`b78ffd3896a593a0616e6965aae040d5509facc9`。
- 每个场景在同构建、不同构建两组中各固定 5 对；顺序均为 A1 B1 A2 B2 A3 B3 A4 B4 A5 B5，共 40 次 compose、60 个 fio 场景。
- 单任务 release 编译耗时超过半小时；在任何测量开始前将 randread 单独优先执行两个矩阵，再执行 seqread+randrw 的两个矩阵，以满足 90 分钟内回答 randread 核心问题的时限。没有增加或筛选任一场景的轮次。
- 第一组两个 arm 使用同一个 rebased default 可执行文件；第二组 A 为 clean main 默认构建，B 为 rebased default 默认构建。两者均未启用 observability。
- 两构建 Cargo.lock SHA256 相同；同一 rustc、release 参数与默认 feature 集。没有根据测量结果修改产品代码、追加轮次或选最好结果。
- compose + Redis 元数据 + LocalFS 对象；upload_before_commit、无压缩。512 MiB 固定 pattern `0x0123456789abcdef`，每场景预填充后完整 SHA256 校验并预热。
- fio 30 秒、direct=1、libaio、1 job、iodepth=16；randread/randrw 4 KiB，seqread 1 MiB；randrw 读占 70%。randseed=20261031、randrepeat=1。
- 读缓存 1 GiB、SSD 读缓存关闭、挂载内存预算 2 GiB；request tracing 关闭，两个 arm 均关闭 HTTP scrape 和周期性 writeback sampler。
- 每次 fresh mount、fresh compose volumes；验证容器实际二进制 SHA256、无残留 FUSE mount/进程、无残留 compose 容器/卷。测量期间无 Cargo/rustc。
- P99 为 fio completion latency；未执行的写方向记 N/A。样本标准差使用 n-1；极差=max-min。
- 95% CI 对五个配对 log(B/A) 均值使用 Student-t（df=4，t=2.7764451052），再变回百分比；它不是中位数比值的 CI。小样本、固定 AB 顺序和时间相关性限制精度。
- LocalFS 暖缓存结论仅适用于此 profile；active+drain 未采集，明确记 N/A。同步上传完成及 effective wall 带宽用于检查收尾成本。

编译器：`rustc 1.99.0 (b940084d7 2026-09-28)`。构建环境：`{"CARGO_INCREMENTAL": "0", "CARGO_PROFILE_RELEASE_DEBUG": "0", "CARGO_PROFILE_RELEASE_CODEGEN_UNITS": "256", "RUSTFLAGS": "-C debuginfo=0"}`。

构建调度：baseline 使用 1 个 Cargo build job，default 使用 4 个；候选尚未开始编译时为满足时限调整。这只改变编译并行度，优化选项/codegen-units 不变；原排队日志保留。每组内执行的二进制 SHA256 固定。

| 构建 | SHA256 |
|---|---|
| baseline | `9c5e07a8a397e6b449ba908db060ab538835ca998e779fa76ad34226d54ef18a` |
| default | `e3661e92989729d229657f425bc0cc1c22c7cd24b95de2b91eb5c7127c815540` |

默认构建审计：指标名字符串 0；`brewfs::observability::` 符号 0。main 已有的独立 request observer 保留其真实时钟/原子语义，关闭 tracing；零符号结论仅针对 feature 模块。

## 每轮原始值

MiB/s 为读写带宽之和；下表 P99 单位 ms。每一轮都保留，不按性能筛选。

### 同构建 randread

| 场景 | 轮 | A MiB/s | B MiB/s | B/A Δ% | A 读P99 | B 读P99 | A 写P99 | B 写P99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| fio-randread | 1 | 15.642 | 15.935 | +1.873 | 6.521 | 6.390 | N/A | N/A |
| fio-randread | 2 | 15.552 | 15.583 | +0.200 | 6.849 | 6.521 | N/A | N/A |
| fio-randread | 3 | 15.602 | 15.107 | -3.170 | 6.390 | 7.111 | N/A | N/A |
| fio-randread | 4 | 14.896 | 14.965 | +0.463 | 7.045 | 6.849 | N/A | N/A |
| fio-randread | 5 | 15.062 | 15.101 | +0.259 | 6.717 | 6.849 | N/A | N/A |

### 不同构建 randread

| 场景 | 轮 | A MiB/s | B MiB/s | B/A Δ% | A 读P99 | B 读P99 | A 写P99 | B 写P99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| fio-randread | 1 | 15.170 | 15.361 | +1.259 | 7.045 | 6.586 | N/A | N/A |
| fio-randread | 2 | 15.264 | 15.492 | +1.495 | 6.586 | 6.783 | N/A | N/A |
| fio-randread | 3 | 15.367 | 15.610 | +1.585 | 6.849 | 6.586 | N/A | N/A |
| fio-randread | 4 | 15.509 | 14.826 | -4.404 | 6.521 | 7.111 | N/A | N/A |
| fio-randread | 5 | 14.525 | 15.244 | +4.954 | 7.242 | 6.980 | N/A | N/A |

### 同构建 guards

| 场景 | 轮 | A MiB/s | B MiB/s | B/A Δ% | A 读P99 | B 读P99 | A 写P99 | B 写P99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| fio-seqread | 1 | 1041.699 | 1010.366 | -3.008 | 22.151 | 23.200 | N/A | N/A |
| fio-seqread | 2 | 995.100 | 1023.099 | +2.814 | 24.510 | 22.938 | N/A | N/A |
| fio-seqread | 3 | 1027.599 | 1008.966 | -1.813 | 22.675 | 23.986 | N/A | N/A |
| fio-seqread | 4 | 1059.698 | 1026.899 | -3.095 | 21.627 | 24.248 | N/A | N/A |
| fio-seqread | 5 | 1051.165 | 1054.796 | +0.345 | 22.413 | 21.627 | N/A | N/A |
| fio-randrw | 1 | 5.797 | 5.827 | +0.528 | 17.957 | 17.170 | 18.481 | 17.433 |
| fio-randrw | 2 | 5.757 | 5.867 | +1.900 | 18.481 | 18.743 | 19.005 | 19.005 |
| fio-randrw | 3 | 5.610 | 5.799 | +3.370 | 17.433 | 19.530 | 17.957 | 20.054 |
| fio-randrw | 4 | 5.816 | 5.948 | +2.279 | 18.219 | 17.433 | 18.481 | 17.695 |
| fio-randrw | 5 | 5.879 | 5.952 | +1.255 | 17.695 | 17.170 | 17.433 | 17.433 |

### 不同构建 guards

| 场景 | 轮 | A MiB/s | B MiB/s | B/A Δ% | A 读P99 | B 读P99 | A 写P99 | B 写P99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| fio-seqread | 1 | 1026.566 | 1073.498 | +4.572 | 23.462 | 21.365 | N/A | N/A |
| fio-seqread | 2 | 1096.097 | 1086.297 | -0.894 | 20.840 | 22.151 | N/A | N/A |
| fio-seqread | 3 | 1033.432 | 1007.700 | -2.490 | 22.938 | 23.462 | N/A | N/A |
| fio-seqread | 4 | 1079.364 | 1026.532 | -4.895 | 21.627 | 22.413 | N/A | N/A |
| fio-seqread | 5 | 1060.165 | 1096.030 | +3.383 | 22.151 | 20.578 | N/A | N/A |
| fio-randrw | 1 | 5.923 | 6.020 | +1.634 | 17.695 | 17.170 | 17.695 | 17.170 |
| fio-randrw | 2 | 6.061 | 5.918 | -2.347 | 16.908 | 16.908 | 17.433 | 17.170 |
| fio-randrw | 3 | 5.803 | 5.878 | +1.292 | 18.481 | 18.219 | 18.743 | 19.005 |
| fio-randrw | 4 | 5.726 | 5.900 | +3.033 | 18.743 | 17.433 | 19.005 | 17.695 |
| fio-randrw | 5 | 5.823 | 5.947 | +2.138 | 17.170 | 17.433 | 17.433 | 17.433 |

## 中位数、极差和标准差

| 组 | 场景 | arm | 指标 | 中位数 | 最小值 | 最大值 | 极差 | 样本标准差 |
|---|---|---|---|---:|---:|---:|---:|---:|
| 同构建 randread | fio-randread | baseline | mib_s | 15.552 | 14.896 | 15.642 | 0.746 | 0.346 |
| 同构建 randread | fio-randread | baseline | read_p99_ms | 6.717 | 6.390 | 7.045 | 0.655 | 0.260 |
| 同构建 randread | fio-randread | baseline | effective_wall_bw_mib_s | 15.099 | 14.416 | 15.642 | 1.227 | 0.555 |
| 同构建 randread | fio-randread | default | mib_s | 15.107 | 14.965 | 15.935 | 0.970 | 0.408 |
| 同构建 randread | fio-randread | default | read_p99_ms | 6.849 | 6.390 | 7.111 | 0.721 | 0.288 |
| 同构建 randread | fio-randread | default | effective_wall_bw_mib_s | 15.101 | 14.965 | 15.421 | 0.456 | 0.170 |
| 不同构建 randread | fio-randread | baseline | mib_s | 15.264 | 14.525 | 15.509 | 0.984 | 0.380 |
| 不同构建 randread | fio-randread | baseline | read_p99_ms | 6.849 | 6.521 | 7.242 | 0.721 | 0.304 |
| 不同构建 randread | fio-randread | baseline | effective_wall_bw_mib_s | 14.772 | 14.057 | 15.367 | 1.310 | 0.482 |
| 不同构建 randread | fio-randread | default | mib_s | 15.361 | 14.826 | 15.610 | 0.784 | 0.302 |
| 不同构建 randread | fio-randread | default | read_p99_ms | 6.783 | 6.586 | 7.111 | 0.524 | 0.234 |
| 不同构建 randread | fio-randread | default | effective_wall_bw_mib_s | 14.866 | 14.349 | 15.493 | 1.144 | 0.424 |
| 同构建 guards | fio-seqread | baseline | mib_s | 1041.699 | 995.100 | 1059.698 | 64.598 | 25.313 |
| 同构建 guards | fio-seqread | baseline | read_p99_ms | 22.413 | 21.627 | 24.510 | 2.884 | 1.097 |
| 同构建 guards | fio-seqread | baseline | effective_wall_bw_mib_s | 1008.129 | 994.484 | 1025.548 | 31.065 | 13.624 |
| 同构建 guards | fio-seqread | default | mib_s | 1023.099 | 1008.966 | 1054.796 | 45.830 | 18.480 |
| 同构建 guards | fio-seqread | default | read_p99_ms | 23.200 | 21.627 | 24.248 | 2.621 | 1.032 |
| 同构建 guards | fio-seqread | default | effective_wall_bw_mib_s | 990.129 | 976.452 | 1054.867 | 78.415 | 34.435 |
| 同构建 guards | fio-randrw | baseline | mib_s | 5.797 | 5.610 | 5.879 | 0.269 | 0.101 |
| 同构建 guards | fio-randrw | baseline | read_p99_ms | 17.957 | 17.433 | 18.481 | 1.049 | 0.414 |
| 同构建 guards | fio-randrw | baseline | write_p99_ms | 18.481 | 17.433 | 19.005 | 1.573 | 0.598 |
| 同构建 guards | fio-randrw | baseline | effective_wall_bw_mib_s | 5.797 | 5.572 | 5.879 | 0.307 | 0.135 |
| 同构建 guards | fio-randrw | default | mib_s | 5.867 | 5.799 | 5.952 | 0.154 | 0.070 |
| 同构建 guards | fio-randrw | default | read_p99_ms | 17.433 | 17.170 | 19.530 | 2.359 | 1.071 |
| 同构建 guards | fio-randrw | default | write_p99_ms | 17.695 | 17.433 | 20.054 | 2.621 | 1.166 |
| 同构建 guards | fio-randrw | default | effective_wall_bw_mib_s | 5.757 | 5.639 | 5.799 | 0.160 | 0.066 |
| 不同构建 guards | fio-seqread | baseline | mib_s | 1060.165 | 1026.566 | 1096.097 | 69.531 | 29.571 |
| 不同构建 guards | fio-seqread | baseline | read_p99_ms | 22.151 | 20.840 | 23.462 | 2.621 | 1.039 |
| 不同构建 guards | fio-seqread | baseline | effective_wall_bw_mib_s | 1026.600 | 1000.129 | 1079.400 | 79.271 | 31.381 |
| 不同构建 guards | fio-seqread | default | mib_s | 1073.498 | 1007.700 | 1096.030 | 88.330 | 38.754 |
| 不同构建 guards | fio-seqread | default | read_p99_ms | 22.151 | 20.578 | 23.462 | 2.884 | 1.090 |
| 不同构建 guards | fio-seqread | default | effective_wall_bw_mib_s | 1073.533 | 975.226 | 1096.067 | 120.841 | 50.310 |
| 不同构建 guards | fio-randrw | baseline | mib_s | 5.823 | 5.726 | 6.061 | 0.335 | 0.129 |
| 不同构建 guards | fio-randrw | baseline | read_p99_ms | 17.695 | 16.908 | 18.743 | 1.835 | 0.799 |
| 不同构建 guards | fio-randrw | baseline | write_p99_ms | 17.695 | 17.433 | 19.005 | 1.573 | 0.755 |
| 不同构建 guards | fio-randrw | baseline | effective_wall_bw_mib_s | 5.803 | 5.541 | 6.061 | 0.519 | 0.187 |
| 不同构建 guards | fio-randrw | default | mib_s | 5.918 | 5.878 | 6.020 | 0.142 | 0.055 |
| 不同构建 guards | fio-randrw | default | read_p99_ms | 17.433 | 16.908 | 18.219 | 1.311 | 0.490 |
| 不同构建 guards | fio-randrw | default | write_p99_ms | 17.433 | 17.170 | 19.005 | 1.835 | 0.764 |
| 不同构建 guards | fio-randrw | default | effective_wall_bw_mib_s | 5.826 | 5.688 | 5.947 | 0.259 | 0.118 |

## 配对差异和置信区间

| 组 | 场景 | 中位数比值 Δ% | 配对差异 min…max % | 配对差异标准差 pp | 配对几何均值 Δ% | 95% CI % | 正负跳动 |
|---|---|---:|---|---:|---:|---|---|
| 同构建 randread | fio-randread | -2.858 | -3.170 … +1.873 | 1.861 | -0.089 | [-2.391, +2.268] | 是 |
| 不同构建 randread | fio-randread | +0.633 | -4.404 … +4.954 | 3.372 | +0.932 | [-3.212, +5.254] | 是 |
| 同构建 guards | fio-seqread | -1.785 | -3.095 … +2.814 | 2.521 | -0.977 | [-4.037, +2.181] | 是 |
| 同构建 guards | fio-randrw | +1.211 | +0.528 … +3.370 | 1.072 | +1.862 | [+0.540, +3.201] | 否 |
| 不同构建 guards | fio-seqread | +1.258 | -4.895 … +4.572 | 3.977 | -0.128 | [-4.942, +4.930] | 是 |
| 不同构建 guards | fio-randrw | +1.645 | -2.347 … +3.033 | 2.062 | +1.133 | [-1.421, +3.753] | 是 |

命中预先写定的规则 1 和 3：同构建出现更大的中位数差，且实际跨构建配对差异正负跳动。原「固定绝对差 ≤1%」读门槛更新为「在测量噪声内」；未发现稳定的 default 开销，因此没有进入性能调参或修改产品代码的路径。结论来自完整固定五对，所有原始值均保留。

同构建 randread 的两臂中位数比值为 -2.858%，而配对几何均值仅 -0.089%。两种统计量回答不同问题：前者与旧三轮报告口径一致，后者利用相邻 A/B 的配对关系。因此同时报告两者，避免把小样本中位数差误读成稳定的逐轮损失。

同构建 randrw 的配对几何均值差为 +1.862%，95% CI [+0.540%, +3.201%]，尽管两边执行的是相同二进制。这显示固定 AB 顺序下还存在顺序/时间偏差；CI 不含 0 也不能单独证明代码导致了差异。本报告不把同构对照的正差异当作产品性能提升。

补充 guard 未发现稳定退化：实际构建 seqread 中位数差 +1.258%、读 P99 中位数相同；randrw 中位数差 +1.645%、读/写 P99 中位数均下降 1.481%，effective wall 中位数差 +0.398%。seqread/randrw 配对差异均正负跳动，CI 均包含 0。40 次运行全部成功并清理完挂载/进程/卷，未观察到收尾挂起。

effective wall 使用 runner 的整秒 wall time（本次约 30/31 秒），量化误差约为一个运行时长的 3.3%；不能据此把亚百分点差异解释为性能提升。active+drain 为 N/A，未将缺失值当作 0，也不宣称 strict-drain 验收。

## rebase 与门禁

冲突解决保留 main 的 request observer、写回 metadata-inflight 和 runner 观测改动；保留 feature 分支的 Prometheus 门控与确定性预填充。没有按性能数据修改缓存、I/O、调度或 codegen 设置。

- workspace: `test result: ok. 1296 passed; 0 failed; 264 ignored; 0 measured; 0 filtered out; finished in 75.94s`; `test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s`; `test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s`
- observability: `test result: ok. 1301 passed; 0 failed; 264 ignored; 0 measured; 0 filtered out; finished in 69.09s`; `test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s`; `test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s`
- production_noop: `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s`
- default / observability 两种 `cargo clippy --workspace --all-targets -- -D warnings` 均退出 0，workspace 0 warning。
- vendored tikv-client / asyncfuse 的既有依赖编译警告及 proc-macro-error2 future-incompatibility 提示保留在日志中；没有通过降低 workspace lint 强度隐藏警告。
- fmt、workspace check/build、tokio/io-uring 无默认 feature 检查、生产 no-op 测试、4 项 shell 语法检查、3 项 runner 回归检查、Python 语法和 git diff --check 通过。rfuse3 不是 workspace member。
- enabled feature 的旧性能结果继续保留为历史数据；本次测量结论针对 default，未扩展为 enabled 的重测结论。

可选 packed/native 的旧 read observers 尚未全面门控；普通 default 的零符号证据不扩展到这些可选配置。

## 证据与复现

- 预先设计、构建/门禁日志、全部运行记录：`/tmp/brewfs-obs-overhead/`。
- 同构建：`/tmp/brewfs-obs-overhead/same-binary/noise-analysis.json`。
- 不同构建：`/tmp/brewfs-obs-overhead/different-binaries/noise-analysis.json`。
- 每次 compose artifact 绝对路径在各组 `runs.json` 及仓库 evidence JSON 中；每对另有 compare_artifacts.py 报告。
- 原九次性能证据保留在 `/tmp/brewfs-observability-gated/perf-final-retry2/`。
- 初次容器预检因自定义 Dockerfile 未给 perf_manifest.py 执行权限，在挂载和 fio 之前退出；没有 fio 结果。修复 COPY --chmod=755 后执行完整矩阵。失败日志/空产物保留，见 `preflight-failure.json` 和 `same-binary-preflight-failed/`；没有按性能剔除数据。
- 启动实验：`bash /tmp/brewfs-obs-overhead/run-experiments.sh`（已有输出目录禁止覆盖；复现时选择新目录）。
- 同构建命令：`python3 tools/perf/measure_feature_observability.py DEFAULT DEFAULT --two-arm --rounds 5 --runtime 30 --tools "fio-randread" --dockerfile target/Dockerfile.observability-noise --output NEW_DIR`。随后在新目录以 `--tools "fio-seqread fio-randrw"` 执行 guard 矩阵。
- 不同构建命令：将第一个 DEFAULT 替换为 BASELINE；分析：`python3 tools/perf/summarize_observability_noise.py NEW_DIR`。
