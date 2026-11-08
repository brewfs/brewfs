# -*- coding: utf-8 -*-
import io
base = r'C:\Users\HXY\Documents\Codex\2026-10-22\https-www-usenix-org-conferences-author\work\ax-plan-review\runs\dsh-experiment-context-20261106'
mp = base + r'\METRICS.md'
dp = base + r'\DECISION_LOG.md'

with io.open(mp, encoding='utf-8') as f:
    text = f.read()

add = u'''
### C4 seal：代码已写，编译/验证未完成

```
agent gseal（brewfs 包年机，¥0）已完成代码改动：14 个文件
  src/cli.rs / src/config.rs / src/workspace_overlay/{catalog,mod}.rs
  src/workspace_overlay/stores/{database,kv_store}.rs + 测试文件
  目标命令：brewfs workspace seal <WORKSPACE_ID> → 输出可被 fork 接受的 revision

阻塞：
  ① 该 agent 跑 `cargo test --workspace` 把 30 GB 内存的 brewfs 拖死，SSH 全部被拒
     ⇒ RebootInstance 一次（代码保留、agent 上下文丢失）
  ② 重启后手动重编 release：brewfs 主 crate 在 30 GB 机器上跑 ~50 分钟仍未产出，
     且内存 19/30 GB、磁盘一度只剩 14 GB（已清 14 GB 旧 target 缓解）
  ③ 因主 crate 长时间无进展，已 kill；seal 代码仍在磁盘（14 个改动文件），但**没有可用二进制**
⇒ C4 的「seal → fork 可见性 → 写隔离」闭环**仍未验证**；下一步需要一个能编出二进制且内存充足的机器
   （a-client 61 GB 更合适，但需 2–3 小时编译预算）
```

'''
anchor = u'## C3 — 百万小文件'
idx = text.index(anchor)
text = text[:idx] + add + text[idx:]
with io.open(mp, 'w', encoding='utf-8') as f:
    f.write(text)

entry = u'''
---

## 2026-11-08 Round 6 收尾：C4 seal 卡在编译

1. agent gseal 已完成 `workspace seal` 的代码（14 个文件改动，含 CLI/config/workspace_overlay/测试），
   但**没有产出可用二进制**：
   - 它自己跑 `cargo test --workspace` 时把 30 GB 的 brewfs 内存打满，SSH 全部被拒 → RebootInstance 一次
   - 重启后手动重编 release，主 crate 在 30 GB 机器上 ~50 分钟无进展（内存 19/30、磁盘一度 14 GB）
   - 已清掉 14 GB 旧编译产物（txn-target/redis-xattr-target 等），仍选择 kill 掉编译
2. **结论**：seal 代码在 `/home/hxy/brewfs-main`（14 个改动文件），可复用；
   但要拿到二进制，最好在 a-client（61 GB + 64 GB swap）上编，预算 2–3 小时。
3. 本轮其它结论：C2 pytest 已在 BrewFS 跑通（680.06 s vs JuiceFS 109.7 s）；C3 两次尝试均因挂载/SSH 不稳失败。
4. 成本：仅 jump（¥0.3/h）+ brewfs（¥0）；a-* 全部 Stopped（已回读 Status 校验）。
'''
with io.open(dp, encoding='utf-8') as f:
    d = f.read()
d = d.rstrip() + u'\n' + entry
with io.open(dp, 'w', encoding='utf-8') as f:
    f.write(d)
print('METRICS', len(text.encode('utf-8')), 'DECISION_LOG', len(d.encode('utf-8')))
