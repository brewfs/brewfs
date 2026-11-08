# -*- coding: utf-8 -*-
import io, os
base = r'C:\Users\HXY\Documents\Codex\2026-10-22\https-www-usenix-org-conferences-author\work\ax-plan-review\runs\dsh-experiment-context-20261106'
dp = os.path.join(base, 'DECISION_LOG.md')

entry = u'''
---

## 2026-11-08 17:10 C4 阻塞定点：seal 只有库函数，没有 CLI 入口

排查结论（在 brewfs 包年机上只读源码得到）：

```
· src/workspace_overlay/seal.rs 有完整实现：
    pub struct WorkspaceSealer<W>
    pub fn new(store: Arc<W>) -> Self
    pub async fn seal(&self, workspace_id: WorkspaceId) -> Result<SealResult, WorkspaceError>
· 全仓库检索：除 `pub mod seal;` 外**没有任何 CLI / 控制面调用它**
· 因此 `workspace fork --from <revision>` 永远只能拿到 init 时的空 sealed base；
  写进 live workspace 的文件留在 upper 层 ⇒ 「fork 后能看到 base 文件」无法验证（C4 的唯一阻塞）
· `workspace init-packed-volume --manifest-key` 需要 packed-v3 manifest，
  而 manifest 由 native ingest/frozen catalog 生成，CLI 里没有对应子命令
  （src/bin 下只有两个测试用 fixture 二进制）
```

**解决路径（已下发）**：新增 `brewfs workspace seal <WORKSPACE_ID>` 子命令，
复用 `workspace_cmd` 里现成的 meta backend 构造逻辑，内部调用 `WorkspaceSealer::seal`；
agent `gseal` 正在 brewfs 包年机上实现 + 单测 + 编译（不花钱）。
拿到二进制后 C4 闭环步骤：
```
1) init-volume → 挂载 → 写 N 个文件 → umount
2) workspace seal <id>            ← 新子命令
3) workspace list 应看到 sealed_version/head_epoch 变化
4) workspace fork --from <新 revision> --count 1
5) 挂载 fork 出来的 workspace，find | wc -l 应等于 N（证明共享 lower、未复制）
6) 在 fork 里改文件 → 重新 seal/inspect base 的 root_hash 不变（写隔离）
```
'''
with io.open(dp, encoding='utf-8') as f:
    d = f.read()
d = d.rstrip() + u'\n' + entry
with io.open(dp, 'w', encoding='utf-8') as f:
    f.write(d)
print('DECISION_LOG bytes =', len(d.encode('utf-8')))
