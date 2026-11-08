# -*- coding: utf-8 -*-
import io

base = r'C:\Users\HXY\Documents\Codex\2026-10-22\https-www-usenix-org-conferences-author\work\ax-plan-review\runs\dsh-experiment-context-20261106'
mp = base + r'\METRICS.md'
dp = base + r'\DECISION_LOG.md'

# 1) METRICS：在 C2 段落里补充「补测尝试与阻塞」结论
with io.open(mp, encoding='utf-8') as f:
    text = f.read()

add = u'''
### C2 补测尝试（2026-11-08，未完成）

```
1. agent gxattr2（brewfs 包年机）已实现 TiKV MetaStore xattr：
   src/meta/stores/tikv/mod.rs +211 行（set/get/list/remove_xattr + XATTR_PREFIX + inode 删除清理）
   src/meta/stores/tikv/tests.rs +225 行（CRUD/二进制值/空值/CREATE-REPLACE/ENODATA/删除清理）
   doc/testing/xattr-backend-support.md 同步更新
   patch 已保存：工作副本 xattr.patch（27,898 字节，3 文件 +426/−56）

2. 但「把 patch 编进实验机二进制」反复失败（本机 16c64g）：
   · 第一次编译出的二进制 strings 校验不含新代码（两个 cargo 抢 target 锁，编的是旧源码）
   · 改用 codegen-units=16 → 触发 451+ 个依赖 crate 全量重编，>1h50m 仍在编 kube/aws-sdk-s3
   · 期间机器因内存压力出现 sshd 无响应，被迫 RebootInstance 一次
   ⇒ 结论：在本机把补丁编进 4.3 GB release 二进制的成本 ≈2–3 小时，已超出本轮预算，中止

3. 因此 C2 的 BrewFS 侧数据仍为「不可用」，原因不是测量方法，而是产品能力缺口：
   TiKV MetaStore 历史上没有 xattr（返回 EOPNOTSUPP 而非 ENODATA），
   而 pytest/django/rust 三条 trace 分别含 22,881 / 42,091 / 693,861 行 xattr 事件。
```

'''
anchor = u'## C3 — 百万小文件'
idx = text.index(anchor)
text = text[:idx] + add + text[idx:]
with io.open(mp, 'w', encoding='utf-8') as f:
    f.write(text)

# 2) DECISION_LOG：Round 4 结论
entry = u'''
---

## 2026-11-08 Round 2–4：补测尝试与本轮收尾

1. **C1 已完成**（72 cell，详见 METRICS C1 章节）。
2. **C2 BrewFS 侧未完成**：
   - 已由 agent 写出 TiKV xattr 实现（+426/−56，3 文件，patch 存 `xattr.patch`）
   - 但把补丁编进实验机二进制反复失败：cargo 抢锁导致编出旧代码；改 codegen-units 触发 451+ 依赖全量重编，
     >1h50m 未完成；期间机器内存压力导致 sshd 无响应，RebootInstance 一次
   - **结论**：C2 在 BrewFS 上的数据缺失是「产品能力缺口 + 构建成本」双重原因，不是测量方法问题；
     论文里应写成 limitation，并引用 xattr 缺口（ENOTSUPP vs ENODATA）
3. **C3 pilot 3 次重复未做**：需每次重建 1M workspace（30–40 分钟/次），本轮预算不足。
4. **C4 seal→可见性未做**：CLI 无 seal 入口，需走 native ingest / packed manifest 路线（未知工作量）。
5. **Redis 后端部分完成**：挂载 1 s、10k 文件 183 文件/s、元数据 7.9 MB；pilot 因 smallfiles
   `os error 2` 未跑通（与 C3 时同款，仍未定位）。
6. **成本控制**：本轮结束时 a-client/a-meta/a-obj 全部 Stopped，仅 jump Running ⇒ **≈¥0.3/h**。

### 本轮新增的可复现坑（供后续参考）
```
① 编译校验：必须 `strings target/release/brewfs | grep -c 'invalid setxattr flags'` > 0，
   否则说明编的是旧源码（cargo 锁竞争会静默编旧代码）
② 改 CARGO_PROFILE_RELEASE_CODEGEN_UNITS 会让全部依赖失效重编（451+ crate，>1.5h）
③ 16c64g 上 -j8 + cgu=256 编 brewfs 会吃 40–63 GB 内存，可能拖死 sshd
④ smallfiles 的 "os error 2" 与目录布局无关（已试 base/small/d#### 仍失败）
```
'''
with io.open(dp, encoding='utf-8') as f:
    d = f.read()
d = d.rstrip() + u'\n' + entry
with io.open(dp, 'w', encoding='utf-8') as f:
    f.write(d)

print('METRICS bytes =', len(text.encode('utf-8')), ' DECISION_LOG bytes =', len(d.encode('utf-8')))
