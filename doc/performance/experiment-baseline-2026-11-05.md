# 实验基线（2026-11-05 冻结）

## 基线标识

| 项 | 值 |
|---|---|
| commit | 3fc7db90e8f374899470e7f02bd89c46169187ee（短 3fc7db9，2026-11-05 17:28:14 +0800） |
| Rust | rustc 1.99.1 (58cf8f9c3 2026-10-13) |
| 编译配置 | release、-C opt-level=3、debug=line-tables-only、CARGO_INCREMENTAL=0 |
| 测量主机 | brewfs-singapore-v2（8 vCPU / 32 GB，47.84.198.14） |
| 编译主机 | brewfs-build（8 vCPU / 32 GB，47.237.195.102） |
| TiKV 主机 | tikv-xattr-test（4 vCPU / 8 GB，内网 172.24.86.193，按需开机） |

## 已合入的性能改动（本基线的构成）

| PR | 内容 | 实测 |
|---|---|---|
| #407 | 写路径批量合并 | 顺序写 +20.6%（三级一致）；随机写 +1.2%（噪声内）；混合 0% |
| #409 | packed 小文件内联布局 | 温态 -51%、冷启动 -50%、verify -50.5%、随机混合 -63.5%（p99 -65.2%） |
| #426/#427 | 惰性目录名枚举（修 #409 引入的 readdir 回退） | readdir 912.23 → 772.68 ms（-13.83%），CI [-22.52, -4.16]，9/9 指标零回退 |
| #404 | PUT 合并 | PUT/GiB 261,500 → 16,384（-93.7%） |
| #420 | BREWFS_CACHE_TTL_MS 优先级 | 配置正确性 |

## 已知残差（实验要打的点）

| 指标 | 现状 | 目标/差距 |
|---|---|---|
| 小文件温态 | 3.27 ms/文件（1.19 MiB/s） | native 0.27 ms/文件（15.33 MiB/s）⇒ 差 12 倍 |
| 纯 readdir | 772.68 ms / 8,192 文件 ≈ 0.094 ms/文件 | 主因：147 groups 串行拉取（+551~687 ms） |
| SHA-256 | 占端到端 61% | 待评估是否可跳过/并行 |
| 写路径 close tail | 5.68 s（#414） | 目标 < 5 s |
| 大文件 4 KiB 随机读 | 0.180 MiB/s（4 MiB 帧） | 帧大小与预取策略 |

## 强制实验方法

1. 测量只在 brewfs，禁止编译（编译全部去 brewfs-build）
2. 每结论 ≥3 对交替测量；报中位数 + 配对 t + 95% CI
3. 两臂必须同 rustc、同 opt-level、同 debuginfo
4. 每轮独立挂载 + drop_caches + 校验 status=valid
5. 报告写进 doc/performance/，附 artifacts 路径

## harness 位置与用法

    /mnt/brewfs-data/home/hxy/regress-20261104/
      harness/campaign.py measure --cohort NAME --pairs 3
      harness/analyze.py NAME
      protocol.json   场景定义
      artifacts/      每轮 report.md / fio json / fully-drained-throughput.tsv

## 禁止合并清单

- #402  EC range-heavy 提示：已知 seqread/randrw/verify/cold-start 回退
- #414  write-pressure：close tail 5.68 s 未达标（draft）
- #416  小文件归因：证据类（draft）
- #406  第三方 PR（不碰）

## 冻结声明

自本基线起，除实验 PR 外不合入其他改动；
任何实验 PR 必须附「同一 harness、配对 3 轮、95% CI」的数字，否则不合。
