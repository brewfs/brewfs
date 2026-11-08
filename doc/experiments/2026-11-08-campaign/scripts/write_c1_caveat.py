# -*- coding: utf-8 -*-
import io, os

base = r'C:\Users\HXY\Documents\Codex\2026-10-22\https-www-usenix-org-conferences-author\work\ax-plan-review\runs\dsh-experiment-context-20261106'
mp = os.path.join(base, 'METRICS.md')
dp = os.path.join(base, 'DECISION_LOG.md')

banner = u'''## ⚠️ C1 数据有效性警告（2026-11-08 17:10 追加）

**下面这组 C1 数字来自「配置不对称」的挂载，其中 randrw 与 coldread 两组不可用于横向对比：**

```
JuiceFS 挂载：juicefs mount -d --cache-size 0      ← 本地缓存被完全关闭
BrewFS  挂载：默认配置（chunk cache + write-back 开启）
```

证据（同一 cell 两边读写字节完全相同，都是读 707 MiB + 写 317 MiB）：

| randrw 1M | 耗时 | p50 | iops |
|---|---:|---:|---:|
| JuiceFS（cache=0） | 29–31 s | 375 ms | 38 |
| BrewFS（有 cache） | 6.8–7.5 s | 30 ms | 285 |

⇒ JuiceFS 在 70/30 混合读写里每次读都打到对象存储，p50 375 ms；这不是文件系统差异，是**缓存被关掉**的结果。
⇒ `coldread` 组同样受影响（JuiceFS 2 GB/s 是顺序直读对象存储，BrewFS 300 MiB/s 含分块 GET 开销）。

**仍然可用的部分**：`write` / `randwrite` 两组（只写不读，缓存策略对写路径影响小，但 `create-new` 语义仍混入 inode 创建与 fsync 成本）。

**另外两个设计缺陷**
```
① 每 cell 仅 1 GiB：bs=64M 时只有 16 个 IO ⇒ p95=p99=最大值，分位数无统计意义
② 每 cell 结束即 unlink：把删除成本混进带宽；且 randrw 的总带宽我把读写两方向相加，应分方向报
```

**C1-v2 重测协议（下次按此跑）**
```
· 缓存对称：JuiceFS 默认缓存（--cache-size 102400），BrewFS 默认；
  冷读改为「两侧 drop_caches + 重启 RustFS」，不靠关缓存
· cell 放大到 4 GiB（64M bs → 64 个 IO，分位数可用）
· rep1 = create-new（单独报），rep2/3 = overwrite（覆盖写）
· randrw 分方向报 read/write 带宽与各自 p50
· 增加 numjobs=8 / iodepth=32 的饱和吞吐组；单队列组只用于延迟
```

---

'''

with io.open(mp, encoding='utf-8') as f:
    text = f.read()
anchor = u'## C1 — fio 性能矩阵（2026-11-08 重做，v5 最终协议）'
idx = text.index(anchor)
text = text[:idx] + banner + text[idx:]
with io.open(mp, 'w', encoding='utf-8') as f:
    f.write(text)

entry = u'''
---

## 2026-11-08 17:10 C1 数据有效性复核（重要修正）

复核 c1.tsv 后发现三类配置/设计问题，已在 METRICS 顶部加警告：

1. **缓存不对称（最严重）**：JuiceFS 挂载用了 `--cache-size 0`（我为了冷读干净而关掉），BrewFS 用默认缓存。
   randrw 1M 两边读写字节完全相同（707/317 MiB），JuiceFS 29–31 s、p50 375 ms；BrewFS 6.8–7.5 s、p50 30 ms。
   ⇒ 「BrewFS 混合读写快 8.5×」的结论**作废**，那是缓存配置差异。
2. **64M bs 的 cell 只有 16 个 IO**：p95=p99=max，分位数无意义（如 write 64M p95=p99=1451 ms）。
3. **口径问题**：每 cell create-new + 结束 unlink 把删除/inode 成本混入带宽；randrw 总量把读写两方向相加。

保留可用的：write / randwrite 两组（仅作指示性，仍需按 v2 重测）。
C1-v2 协议已写入 METRICS（缓存对称、4 GiB cell、create/overwrite 分开、分方向报、补 numjobs=8 饱和组）。
'''
with io.open(dp, encoding='utf-8') as f:
    d = f.read()
d = d.rstrip() + u'\n' + entry
with io.open(dp, 'w', encoding='utf-8') as f:
    f.write(d)

print('METRICS bytes =', len(text.encode('utf-8')), 'DECISION_LOG bytes =', len(d.encode('utf-8')))
