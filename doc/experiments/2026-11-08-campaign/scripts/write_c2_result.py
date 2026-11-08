# -*- coding: utf-8 -*-
import io
path = r'C:\Users\HXY\Documents\Codex\2026-10-22\https-www-usenix-org-conferences-author\work\ax-plan-review\runs\dsh-experiment-context-20261106\METRICS.md'
with io.open(path, encoding='utf-8') as f:
    text = f.read()

old = u'| **pytest-full-02** | **BrewFS（flat/TiKV）** | — | — | — | — | ❌ **第 203 事件失败** |'
new = (u'| **pytest-full-02** | **BrewFS（flat/TiKV，performance 模式）** | **1,747,109 ops** | 读 281,774,624 B / 写 194,706,661 B '
       u'| **680.06 s** | **23,929 次 errno 差异（全部 xattr/ioctl 路径）** | ✅ **完成** |')

assert old in text, "anchor not found"
text = text.replace(old, new)

addendum = u'''
### C2 的 BrewFS 侧：如何绕开 xattr 阻塞（2026-11-08 突破）

```
strict 模式永远过不去：第 203 事件 getxattr 期望 ENODATA(61)、BrewFS 返回 ENOTSUPP(95) → 直接中止
正解 = replay 工具自带的 `--performance` 模式：
  src/main.rs:398 明确对 getxattr/listxattr/ioctl 的 errno 差异容错并累加 probe_errno_differences
  ⇒ trace 不修改、事件不丢弃，完整跑完 1,747,109 个回调

pytest-full-02 on BrewFS（performance 模式）：
  operations            1,747,109（与 JuiceFS 完全一致）
  active_replay_seconds 680.06 s   （JuiceFS strict 模式 109.7 s ⇒ BrewFS 慢 6.2×）
  actual_read_bytes     281,774,624
  actual_write_bytes    194,706,661
  expected_errors       45,525
  probe_errno_differences 23,929   （22,881 个 xattr 事件 + ioctl 差异，数量级吻合）
  remaining_handles     0          （无 fd 泄漏）
  completed             true
```

**意义**：C2 的 BrewFS 数据不再是空白 —— 同一 trace、同一初始树、同一台机器，BrewFS 完成时间 680 s vs JuiceFS 109.7 s（**6.2×**），
且首次量化了 xattr 缺失的语义偏差规模（23,929 次）。

'''
anchor = u'**BrewFS 失败根因（已定位到具体事件）**'
idx = text.index(anchor)
text = text[:idx] + addendum + text[idx:]

with io.open(path, 'w', encoding='utf-8') as f:
    f.write(text)
print('C2 updated, bytes =', len(text.encode('utf-8')))
