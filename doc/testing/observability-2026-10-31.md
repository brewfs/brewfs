# Observability acceptance evidence: noise calibration update

**默认构建读性能：在测量噪声内，无可检出的稳定开销。** 原三轮 randread 的 -2.45% 不再按脱离噪声底噪的固定 1% 门槛判定。同一二进制的五对交错对照复现了 -2.858% 的两组中位数差；实际 main/default 五对中位数差为 +0.633%，配对几何均值差 +0.932%，95% CI [-3.212%, +5.254%]，差异正负跳动。

结论在任务第 69.24 分钟记录。全部轮次均保留，没有根据性能增删轮次，也没有为凑数字修改产品代码。

[完整实验设计、原始值、P99、中位数/极差/标准差及 CI](observability-overhead-2026-11-01.md) · [机器可读新旧证据](observability-2026-10-31.json) · [指标与构建指南](../operations/prometheus.md)

## Rebased experiment

基线为 clean main `cba857b5`，候选为 rebased `b78ffd3`，均未启用 observability。每个场景在同构建和不同构建对照中各跑五对，均为 A1 B1 … A5 B5。randread 优先执行，随后 seqread/randrw；共 40 次 compose、60 个 fio 场景。512 MiB 固定校验数据、30 秒、direct=1、libaio、单 job、iodepth=16、固定种子 20261031；Redis + LocalFS、upload_before_commit、无压缩。每次彻底卸载并删除临时卷。运行时无编译器。

| 场景 | 同构建中位数差 % | 不同构建中位数差 % | 不同构建配对几何均值 95% CI % |
|---|---:|---:|---|
| fio-randread | -2.858 | +0.633 | [-3.212, +5.254] |
| fio-seqread | -1.785 | +1.258 | [-4.942, +4.930] |
| fio-randrw | +1.211 | +1.645 | [-1.421, +3.753] |

CI 对配对 log(B/A) 均值使用 Student-t（df=4），不是中位数比值的 CI。小样本未检出稳定开销不等于证明精确零效应。

默认二进制指标字符串和 Prometheus observability 符号计数均为 0；实际 FUSE smoke 读写/卸载通过，无 TCP 监听。main 的独立 request observer 保留真实时钟和原子语义，本实验关闭 tracing。

## Local gates after rebase

- workspace: `test result: ok. 1296 passed; 0 failed; 264 ignored; 0 measured; 0 filtered out; finished in 75.94s`; `test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s`; `test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s`
- observability: `test result: ok. 1301 passed; 0 failed; 264 ignored; 0 measured; 0 filtered out; finished in 69.09s`; `test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s`; `test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s`
- production_noop: `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s`
- default 和 observability 两种 all-targets Clippy `-D warnings` 均通过，workspace 0 warning；vendored 依赖既有编译警告保留在日志中。
- fmt、workspace check/build、tokio/io-uring runtime 检查、生产 no-op 测试、shell 和 runner 报告回归检查、Python 语法和 diff 检查通过。

## Historical three-round observations

以下为 `060db0d` 上的旧数据，保留用于追溯；默认构建的验收解释已由以上噪声实验更新。enabled 的旧性能结果不能当作 rebase 后的重测结果。普通 default 的结论不扩展到尚未全面门控的可选 packed/native 旧 read observers。

| 场景 | 模式 | 第1轮 | 第2轮 | 第3轮 | 中位数 MiB/s |
|---|---|---:|---:|---:|---:|
| fio-randread | baseline | 15.267 | 14.978 | 15.607 | 15.267 |
| fio-randread | default | 14.547 | 15.031 | 14.893 | 14.893 |
| fio-randread | enabled | 14.763 | 14.469 | 15.171 | 14.763 |
| fio-seqread | baseline | 1056.198 | 1050.165 | 1061.898 | 1056.198 |
| fio-seqread | default | 1013.633 | 1069.664 | 1035.899 | 1035.899 |
| fio-seqread | enabled | 1045.132 | 1022.033 | 1078.764 | 1045.132 |
| fio-randrw | baseline | 5.639 | 5.858 | 5.842 | 5.842 |
| fio-randrw | default | 5.812 | 5.793 | 5.862 | 5.812 |
| fio-randrw | enabled | 5.587 | 5.449 | 5.813 | 5.587 |

旧九次原始产物保留在 `/tmp/brewfs-observability-gated/perf-final-retry2/`；新产物、配置、每对比较和全部门禁日志在 `/tmp/brewfs-obs-overhead/`。第一次镜像预检失败发生在挂载/fio 前，没有性能值，修复执行权限后才开始完整矩阵。
