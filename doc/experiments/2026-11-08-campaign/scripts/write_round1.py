# -*- coding: utf-8 -*-
import io

base = r'C:\Users\HXY\Documents\Codex\2026-10-22\https-www-usenix-org-conferences-author\work\ax-plan-review\runs\dsh-experiment-context-20261106'
mp = base + r'\METRICS.md'
dp = base + r'\DECISION_LOG.md'

block = u'''
## 补充实验：Redis 元数据后端（2026-11-08）

**环境**：Redis 7.0.15 跑在 a-client 本机（`127.0.0.1:6379`，`--save '' --appendonly no --maxmemory 4gb noeviction`），
BrewFS 挂载命令：
```
brewfs-patched mount --privileged --meta-backend redis --meta-url redis://127.0.0.1:6379/1 \
  --data-backend s3 --s3-bucket redis-20261108 --s3-endpoint http://10.20.0.13:9000 \
  --s3-region us-east-1 --s3-force-path-style true /mnt/bfs-redis
```

| 指标 | 结果 |
|---|---|
| 挂载耗时 | **1 s**（mountpoint 就绪） |
| 单文件写 | OK |
| 批量建 10,000 × 4 KiB 文件 | **54.7 s ⇒ 183 文件/s** |
| Redis key 数（10k 文件后） | 30,359 |
| Redis 内存占用 | 7.90 MB |
| smallfiles pilot | ❌ 失败：`No such file or directory (os error 2)`（4 ms 即退出，与 C3 时同类错误，未定位） |

**对比 TiKV 元数据后端**：C3 导入 1M 文件 30–40 min ⇒ **420–550 文件/s**。
⇒ **Redis 元数据后端在批量小文件创建上比 TiKV 慢 ≈2.5×**（183 vs 450 文件/s），
  但元数据内存开销极低（10k 文件仅 7.9 MB）。

**踩坑**：
```
① 密码/端口：jump 上装 Redis 后 a-client 连 10.20.0.10:6379 被安全组拒绝（Connection refused）
   ⇒ 改为把 redis-server 二进制 + liblzf/libjemalloc 拷到 a-client 本机跑
② 首次批量写出现 EIO：根因不是 Redis，而是 **RustFS 没起**（S3 pipeline connection refused）
   ⇒ 日志里 `pipeline upload failed ... Connection refused (os error 111)` 是判据
③ 挂载必须用 CLI 参数（--meta-backend/--meta-url/--s3-*）；YAML 的 meta 段不被 mount 读取（会回落 sqlite::memory:）
```

---

'''

with io.open(mp, encoding='utf-8') as f:
    text = f.read()
anchor = u'## 资源指标'
idx = text.index(anchor)
text = text[:idx] + block + text[idx:]
with io.open(mp, 'w', encoding='utf-8') as f:
    f.write(text)

entry = u'''
---

## 2026-11-08 补充实验 Round 1（C1 完成 / C2 受阻 / Redis 部分完成）

1. **C1 完成**：72 个 cell（3 bs × 4 op × 3 轮 × 2 系统，含 18 个真冷读），0 错误。
   关键结论：write/randwrite JuiceFS 吞吐高 15–105% 但 BrewFS p50 低 4×；
   randrw BrewFS 全面胜（bw 8.5×、p50 12×）；**冷读 JuiceFS 6.7× 快**（2044 vs 302 MiB/s）。

2. **C2 在 BrewFS 上受阻（已定位到事件级）**：
   pytest trace 第 203 个事件 `getxattr("...","security.capability")`，期望 ENODATA(61)、
   BrewFS 返回 EOPNOTSUPP(95)。根因：TiKV MetaStore 的 xattr 四个方法都是 NotImplemented。
   影响：pytest 22,881 / django 42,091 / rust 693,861 行 xattr 事件 ⇒ 任何含 xattr 的 trace 都无法回放。
   **已在 brewfs 包年机派 agent `gxattr2` 实现 TiKV xattr（含 ENODATA 语义 + 测试 + 文档更新）**。

3. **Redis 后端部分完成**：挂载 1 s、10k 文件 183 文件/s、元数据 7.9 MB；
   pilot 因 smallfiles `os error 2` 未跑通（与 C3 时同款错误，仍未定位）。
   踩坑：外网 Redis 被安全组挡 → 本机跑；首次 EIO 实为 RustFS 未启动。

4. **未完成（下一轮继续）**：C2 待 gxattr2 完成后重跑；C3 pilot 3 次重复；
   C4 seal→可见性；Redis pilot。
'''

with io.open(dp, encoding='utf-8') as f:
    dtext = f.read()
dtext = dtext.rstrip() + u'\n' + entry
with io.open(dp, 'w', encoding='utf-8') as f:
    f.write(dtext)

print('METRICS bytes =', len(text.encode('utf-8')), ' DECISION_LOG bytes =', len(dtext.encode('utf-8')))
