# -*- coding: utf-8 -*-
import io
base = r'C:\Users\HXY\Documents\Codex\2026-10-22\https-www-usenix-org-conferences-author\work\ax-plan-review\runs\dsh-experiment-context-20261106'
mp = base + r'\METRICS.md'
dp = base + r'\DECISION_LOG.md'

with io.open(mp, encoding='utf-8') as f:
    text = f.read()

add = u'''
### C3 pilot 3 次重复：尝试失败（环境不稳）

```
尝试 1（10 万文件 @ flat BrewFS，bucket c1fio-20261108-brew）：
  生成阶段 python 报 ConnectionAbortedError: [Errno 103] Software caused connection abort
  只建成 1,000 文件（1 个目录）后中断；mount 仍在（procs=2），但批量写入被中断
  原因：BrewFS flat 挂载在「连续小文件创建 + 每个 create 打 INFO 日志（mount.log 暴涨）」下不稳
尝试 2（1 万文件 × 3 轮，RUST_LOG=warn，逐目录重试）：
  脚本刚起（mount=y）后 a-client 的 SSH 被远端关闭，实例随后变为 Stopping/Stopped
  （本轮还发现：先前一次 StopInstance 的回显不代表真的停了，必须回读 Status 校验）
⇒ C3 pilot 方差（3 次重复）**仍未取得**；已有数据仍是最初的「1 次热态 8.9613 s + 1 次冷态 23.4057 s」
```

'''
anchor = u'## C4 — fork / 派生成本'
idx = text.index(anchor)
text = text[:idx] + add + text[idx:]
with io.open(mp, 'w', encoding='utf-8') as f:
    f.write(text)

entry = u'''
---

## 2026-11-08 Round 5–6：C2 突破 / C3 失败 / C4 等 seal

1. **C2 突破（重要）**：用 replay 的 `--performance` 模式绕开 xattr 阻塞，
   pytest-full-02 在 BrewFS 上完整回放：1,747,109 ops / 680.06 s / probe_errno_diff 23,929 / handles 0。
   JuiceFS 同 trace strict 模式 109.7 s ⇒ **BrewFS 6.2× 慢**。django 外推 6.2 h，记为 NOT MEASURED。

2. **C3 失败（环境不稳）**：两次尝试均中断 ——
   ① 10 万文件批量写时 FUSE 连接中断（ConnectionAborted Error 103）；
   ② 换 1 万文件 + RUST_LOG=warn 后，a-client SSH 被关闭、实例转 Stopping。
   另发现操作失误：StopInstance 的回显不能作为停机成功的依据，必须回读 Status（本轮因此空转约 1 小时 ≈¥7.5）。

3. **C4 推进方式**：派 agent（brewfs 包年机，¥0）实现 `workspace seal`，已完成代码改动
   （cli.rs/config.rs/workspace_overlay 多处 + 测试），但因 brewfs 内存被 cargo test 拖死、
   SSH 全部被拒，已 RebootInstance 一次；代码在磁盘上保留，现正在重编 release 做端到端验证。

4. **成本纪律**：现在只有 jump（¥0.3/h）+ brewfs（¥0）在跑；a-* 已停。
'''
with io.open(dp, encoding='utf-8') as f:
    d = f.read()
d = d.rstrip() + u'\n' + entry
with io.open(dp, 'w', encoding='utf-8') as f:
    f.write(d)
print('METRICS', len(text.encode('utf-8')), 'DECISION_LOG', len(d.encode('utf-8')))
