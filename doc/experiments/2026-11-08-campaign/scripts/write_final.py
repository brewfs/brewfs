# -*- coding: utf-8 -*-
import io
p = r'C:\Users\HXY\Documents\Codex\2026-10-22\https-www-usenix-org-conferences-author\work\ax-plan-review\runs\dsh-experiment-context-20261106\DECISION_LOG.md'
with io.open(p, encoding='utf-8') as f:
    d = f.read()

entry = u'''
---

## 2026-11-08 21:40 收尾：能做的做完、数据归档、资源清零

### 本轮完成

1. **任务 A（C1-v2）** ✅ 完成：新开 3 台小盘机器（100/100/300 GiB，替代旧的 2048 GiB×3），
   对称缓存协议跑出 58 行数据（11 行汇总），冷读 12 cell；已归档。
2. **任务 C（C4 seal）** 🟡 大部分：seal 子命令实现并编出二进制（62m37s，BUILD_RC=0），
   实测 `seal` 8 ms、`fork` 9 ms；但 **fork 挂载后看不到 base 数据（0 文件）**，
   `workspace diff` 返回 [] 说明元数据一致、缺口在挂载侧不接 lower。已记为产品缺口。
3. **任务 B（C2 trace）** ❌ 无法做：trace 原始文件（1.7M/57.8M 事件）随旧 a-client 释放而丢失，
   本机归档只有 result JSON，不含 events/blobs；重采集需重跑 pytest/django 的 capture，成本远超预算。
4. **任务 D（C3 pilot×3）** ❌ 未做：需每次重建 1M workspace（30–40 分钟/次）；本轮优先做 A 和 C。
5. **任务 E（Redis pilot）** ❌ 未做：smallfiles `os error 2` 根因未定位。

### 数据归档

```
本地：C:\\Users\\HXY\\Documents\\deepseek-harness\\experiment-data-20261108\\
  export-lean/c1v2-20261108/c1v2-data.tar.gz       （C1-v2 原始 58 行 + JSON）
  export-lean/c4-seal-20261108/seal.patch           （seal 实现，14 文件 + seal.rs）
  export-lean/c4-seal-20261108/seal-evidence.tar.gz （meta.db、fork.json、mount/构建日志）
  README.md 已追加 C1-v2 与 C4-seal 两节（含完整表格与结论）
  reports/{METRICS,DASHBOARD,DECISION_LOG}.md 已同步
```

### 资源清零

```
删除：ax-c1-b-client / b-meta / b-obj / c-build（4 台）+ 各自的盘；旧 a-* 更早已删
现存：ax-c1-jump（¥0.3/h，唯一公网入口）+ brewfs-singapore-v2（包年 ¥0）
磁盘：仅 jump 40 GiB ⇒ ≈¥40/月（此前旧配置 3×2048 GiB ≈¥6,144/月）
当前成本 ≈ ¥0.3/h
```

### 遗留（下一轮可直接接手）

```
① C4 fork 可见性：查 mount 侧 lower 附着逻辑（fork 记录 base_revision 已正确，diff=[] 也正常）
② C3 pilot 重复：需 1M workspace 重建；建议复用旧的 c3-base 桶（若还在）或写更快的小文件生成器
③ Redis pilot：smallfiles os error 2 需读源码定位
④ C2：需要重新采集 trace（pytest/django）才能补 BrewFS 侧
```
'''
with io.open(p, 'w', encoding='utf-8') as f:
    f.write(d.rstrip() + u'\n' + entry)

p2 = r'C:\Users\HXY\Documents\Codex\2026-10-22\https-www-usenix-org-conferences-author\work\ax-plan-review\runs\dsh-experiment-context-20261106\DASHBOARD.md'
with io.open(p2, encoding='utf-8') as f:
    lines = f.read().split('\n')
lines[3] = u'> 最后更新：**2026-11-08 21:40 CST**（收尾：C1-v2 完成、C4 seal 编出二进制但 fork 可见性缺口、数据已归档、资源清零 ⇒ ¥0.3/h）'
lines[75] = u'**当前合计 ≈ ¥0.3/h**（jump 0.3；brewfs 包年 ¥0）。21:40 已删除 ax-c1-b-*（3 台）与 ax-c1-c-build（1 台）及其盘；现存仅 jump 40 GiB ⇒ 磁盘月费 ≈¥40（旧配置 ≈¥6,144/月）。'
with io.open(p2, 'w', encoding='utf-8') as f:
    f.write('\n'.join(lines))
print('ok')
