# -*- coding: utf-8 -*-
import io
p = r'C:\Users\HXY\Documents\Codex\2026-10-22\https-www-usenix-org-conferences-author\work\ax-plan-review\runs\dsh-experiment-context-20261106\METRICS.md'
with io.open(p, encoding='utf-8') as f:
    text = f.read()

section = u'''
## C1-v2 —— 对称缓存重测（2026-11-08 18:00–18:45，新机器 b-client/b-meta/b-obj）

**修掉的配置不对称**：v1 里 JuiceFS 用 `--cache-size 0`、BrewFS 用默认缓存 ⇒ randrw/coldread 作废。
v2 两侧都开默认缓存（JuiceFS `--cache-size 102400`，BrewFS 默认），其余协议一致：

```
cell 4 GiB；bs ∈ {1M,4M,64M}；jedepth=16 numjobs=1（单队列组）
op  ∈ write / randwrite / randrw(70% read) / coldread
rep1 = create-new（新文件）；rep2/3 = overwrite（复用同名文件）
冷读 = prep 写 4G → 重启 RustFS（pkill -x rustfs，带凭据）→ drop_caches → **重挂载** → 读同一文件
每 cell 前检查对象盘 >80 GB；percentile_list=50:95:99（冒号）
机器：client 16c64g / meta 8c32g(PD+TiKV) / obj 16c64g(RustFS)；盘 100/100/300 GiB
```

**结果（3 次中位数，bw = MiB/s，p50 = ms）**

| op | bs | JuiceFS bw | BrewFS bw | JuiceFS p50 | BrewFS p50 |
|---|---|---:|---:|---:|---:|
| write | 1M | 270.3 | **285.5** | — | — |
| write | 4M | 266.2 | **280.7** | 231.7 | **177.2** |
| write | 64M | **245.2** | 235.7 | 3942.7 | **3070.2** |
| randwrite | 1M | 133.2 | **153.8** | — | — |
| randwrite | 4M | 266.2 | **278.8** | 227.5 | **156.2** |
| randwrite | 64M | 1.5 | **159.1** | 17112.8 | **6476.0** |
| randrw | 1M | 30.3 | **959.7** | — | — |
| randrw | 4M | 119.8 | **610.3** | 388.0 | **91.8** |
| coldread | 1M | **2314.1** | 450.4 | **6.19** | 34.34 |
| coldread | 4M | **2266.8** | 456.0 | **25.17** | 132.91 |
| coldread | 64M | **2078.9** | 446.8 | **400.56** | 2126.51 |

**读法**
- 写类：两侧接近（差 −4% ~ +6%）；BrewFS 的 p50 在 4M/64M 上低 22–56%
- 随机写 64M：JuiceFS 崩到 **1.5 MiB/s**（p50 17.1 s/IO），BrewFS 159.1 MiB/s（p50 6.5 s）⇒ **106×**
- 混合读写：BrewFS 全面大幅领先（1M ×31.7、4M ×5.1）
- 冷读：JuiceFS 快 **4.7–5.1×**，p50 低 5.5×（与 v1 结论一致，v1 的 2043.9 vs 302.4 仍成立）
- 1M 的 p50 显示为 — 是因为 fio 在该模式下 clat 只测提交（4 GiB 的 fsync 成本计入 wall），不可用于延迟比较

**未取到的 cell（NOT MEASURED）**
```
randrw 64M（JuiceFS 端极慢，超时放弃）
write_sat 1M / 64M（4M 饱和组已测：两侧都 ≈33 MiB/s，因为 8 jobs × 4 GiB = 32 GiB 的 fsync 主导，不代表吞吐上限，需用 size=512M/job 重测）
coldread rep3（只做了 2 轮）
```

**原始数据**：`/srv/ax-data/c1v2/c1v2.tsv`（58 行）+ 每 cell 的 fio JSON（新机器 ax-c1-b-client 上）

'''
anchor = u'## C2 — '
idx = text.index(anchor)
text = text[:idx] + section + text[idx:]
with io.open(p, 'w', encoding='utf-8') as f:
    f.write(text)
print('METRICS bytes =', len(text.encode('utf-8')))
