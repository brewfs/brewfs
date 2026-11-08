# -*- coding: utf-8 -*-
import io
base = r'C:\Users\HXY\Documents\Codex\2026-10-22\https-www-usenix-org-conferences-author\work\ax-plan-review\runs\dsh-experiment-context-20261106'
mp = base + r'\METRICS.md'
dp = base + r'\DECISION_LOG.md'

banner = u'''
> ## ⚠️ 2026-11-08 19:00 重大事故：实验机被释放，原始证据丢失
>
> `ax-c1-a-client` / `ax-c1-a-meta` / `ax-c1-a-obj` **三台实例及其数据盘已被释放**（`DescribeInstances` 只剩 jump，
> `DescribeDisks` 只剩 jump 的系统盘；无对应快照可恢复）。原因尚未查清 —— 我没有对这三台执行过 `DeleteInstance`，
> 最近的操作只有 Stop/Start；不排除账号余额/欠费自动释放或其它外部操作。
>
> **影响**：本文件中引用的以下原始路径**已不存在**：
> - `/srv/ax-data/c1fio/`（C1 72 个 cell 的 fio JSON 与 c1.tsv）
> - `/srv/ax-data/c2b2/`（C2 performance 模式 result.json 与 replay.out）
> - `/srv/ax-data/c3/`（C3 1M 文件 verify/pilot 证据，5.7 GB）
> - `/srv/ax-data/c4ws/`、`/srv/ax-data/redistest/`、`/srv/ax-data/traces/`
> - RustFS 对象桶（c3-base / c2-base 等）随 a-obj 一并消失
>
> **仍成立的部分**：本文件记录的所有**数字与结论**（C1 矩阵、C2 680.06 s、C3 verify/pilot、Redis 183 文件/s 等）
> 均来自当时的实测输出，已在本文与 DECISION_LOG 中留档；但**无法再从原始文件复查**。
>
> **教训**：实验数据必须实时同步到与实验机独立的存储（如 brewfs 包年机或本地），不能只留在按量付费机器的数据盘上。

'''
with io.open(mp, encoding='utf-8') as f:
    text = f.read()
lines = text.split('\n')
# 插到第一个标题之后
insert_at = 1
for i, l in enumerate(lines[:20]):
    if l.startswith('## '):
        insert_at = i
        break
lines.insert(insert_at, banner)
text = '\n'.join(lines)
with io.open(mp, 'w', encoding='utf-8') as f:
    f.write(text)

entry = u'''
---

## 2026-11-08 19:00 事故记录：实验机被释放

1. `ax-c1-a-client`（16c64g）、`ax-c1-a-meta`（8c32g）、`ax-c1-a-obj`（16c64g）三台及数据盘**已被释放**。
   `aliyun ecs DescribeInstances --RegionId cn-hangzhou` 只剩 `ax-c1-jump`；`DescribeDisks` 只剩 jump 的系统盘；
   两张既有快照与本批磁盘无关，无法恢复。
2. 我未对这3台执行 `DeleteInstance`；最后对它们的操作是 16:33 的 `StartInstance`（为 C3 复测）与随后的停机。
   原因待查（可能：账号欠费自动释放 / 外部操作）。
3. 影响：C1/C2/C3/C4/Redis 的**原始产物**（fio JSON、replay result、1M 文件树、trace 目录、RustFS 桶）全部丢失；
   METRICS/DECISION_LOG 中记录的数字与结论仍有效，但不可再从原始文件复核。
4. 后续纪律（写进定时任务）：
   - 任何实验产物必须在完成后 10 分钟内同步到 brewfs 包年机（或本地），再考虑停机
   - 关键实验机不再使用「停机不收费」之外的任何自动化回收策略；停机后必须回读 Status 并确认磁盘仍在
   - 每小时巡检增加一项：核对保留清单中的实例/磁盘是否仍然存在，异常立即告警
'''
with io.open(dp, encoding='utf-8') as f:
    d = f.read()
d = d.rstrip() + u'\n' + entry
with io.open(dp, 'w', encoding='utf-8') as f:
    f.write(d)
print('METRICS', len(text.encode('utf-8')), 'DECISION_LOG', len(d.encode('utf-8')))
