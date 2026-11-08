# -*- coding: utf-8 -*-
import io, os

base = r'C:\Users\HXY\Documents\deepseek-harness\experiment-data-20261108'
readme = u'''# 实验数据包（2026-11-08 导出）

来源：阿里云 a-client `/srv/ax-data/...`（实验机已停机，原始大文件不在此包内）
导出时间：2026-11-08 14:10 CST
本地大小：约 11 MB（2,599 个文件）

## 目录结构

```
export-lean/
  c1fio/            C1 fio 全矩阵：c1.tsv（72 cell 原始行）、summary.md（中位数汇总）、96 个 fio JSON
  c2b2/             C2 在 BrewFS 上的尝试：brew-pytest-full-02.json/out（失败）、xattr-pytest.*
  c2/               C2 历史（JuiceFS 侧）：strict-pytest*.out、*replay*.json/out
  c2b-reports/      rust trace 的报告与 JSON
  c3-results/       C3 百万小文件：verify/pilot 的 result.json、inventory、日志片段
  c4ws/             C4 overlay fork：fork.tsv（62/443/4146 ms）、e2e.log（305 ms 端到端）、fork-*.log
  redistest/        Redis 元数据后端：mount2.log、gen10k 结果、redis 日志
  scripts/          c1fio4.py、c1cold.py、agg.py、gen10k.py、redis-test2.sh、xattr.patch
reports/
  METRICS.md        指标台账（权威数字，含作废声明）
  DASHBOARD.md      看板（实验状态/成本）
  DECISION_LOG.md   决策日志（含历次纠错与踩坑）
  C3_RESULTS.md     C3 专项结果
experiment-data-20261108.tar.gz   上述内容的原始压缩包
```

## 关键数字速查（细节见 reports/METRICS.md）

**C1 fio（72 cell，3 轮中位数；bw = MiB/s，延迟 = ms）**

| op | bs | JuiceFS bw | BrewFS bw | J p50 | B p50 | J p95 | B p95 |
|---|---|---:|---:|---:|---:|---:|---:|
| write | 1M | 515.6 | 414.2 | 25.8 | 6.5 | 37.5 | 39.6 |
| randwrite | 1M | 231.6 | 113.0 | 57.4 | 13.0 | 99.1 | 80.2 |
| randrw | 1M | 37.7 | 322.3 | 375.4 | 30.0 | 683.7 | 166.7 |
| coldread | 1M | 2043.9 | 302.4 | 5.7 | 33.8 | 7.5 | 175.1 |

（4M / 64M 见 c1fio/summary.md）

**C2**：JuiceFS 侧 6/6 PASS（pytest 1,747,109 ops / django 57,787,980 callbacks / rust 3,122,195 callbacks，
errno 差异 0）；BrewFS 侧**未完成** —— pytest 第 203 事件 `getxattr` 期望 ENODATA(61)、实得 EOPNOTSUPP(95)，
根因 TiKV MetaStore 无 xattr。

**C3**：JuiceFS verify 2,587.57 s（1M 文件 / 4,096,000,000 B）；BrewFS verify 1,510 s / RSS 1,785 MB / 31 次重挂；
pilot 热态 JuiceFS 4.4466 s vs BrewFS 8.9613 s（冷 23.4057 s）。

**C4**：`workspace fork` 62 ms（count=1）、443 ms（count=10）、4,146 ms（count=100 ⇒ 41.5 ms/个）；
fork+mount 端到端 305 ms；对比 `juicefs clone` 97,600 文件 20,020 ms。

**Redis 后端**：挂载 1 s；10,000 × 4 KiB 文件 54.7 s（183 文件/s）；Redis 30,359 keys / 7.90 MB。

## 已知缺口（详见 reports/DECISION_LOG.md 的 Round 1–4）

```
1. C2 的 BrewFS 数据缺失：xattr 补丁已写好（scripts/xattr.patch，+426/−56），但编进 4.3 GB 二进制耗时 2–3 小时未完成
2. C3 pilot 3 次重复未做（需每次重建 1M workspace，30–40 分钟/次）
3. C4 「fork 后能看到 base 文件」未验证（live workspace 的 1,026 文件未 seal 成 revision；CLI 无 seal 入口）
4. Redis pilot 被 smallfiles 的 `os error 2` 挡住
```

## 复现

脚本都在 scripts/：
- `c1fio4.py`：C1 主矩阵（3 bs × 3 op × 3 轮 × 2 系统）
- `c1cold.py`：C1 冷读（含 RustFS 重启，注意用 `pkill -x rustfs`）
- `agg.py`：从 c1.tsv 汇总中位数
- `xattr.patch`：TiKV MetaStore xattr 实现补丁
'''

with io.open(os.path.join(base, 'README.md'), 'w', encoding='utf-8') as f:
    f.write(readme)
print('README written, bytes =', len(readme.encode('utf-8')))
