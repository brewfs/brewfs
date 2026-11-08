# -*- coding: utf-8 -*-
import io
base = r'C:\Users\HXY\Documents\Codex\2026-10-22\https-www-usenix-org-conferences-author\work\ax-plan-review\runs\dsh-experiment-context-20261106'
mp = base + r'\METRICS.md'
dp = base + r'\DECISION_LOG.md'

with io.open(mp, encoding='utf-8') as f:
    text = f.read()

add = u'''
## C4 —— seal → fork 可见性（2026-11-08 21:20，新机器 ax-c1-c-build，sqlite + local-fs）

**做了什么**：agent 给 workspace 加了 `seal` 子命令（14 个文件改动 + 新文件 `src/workspace_overlay/seal.rs` 564 行），
在独立构建机（8c32g，200 GiB 盘，添加 64 GB swap、-j2、`--features fuse-io-uring-runtime,workspace-overlay`）编出
**3.3 GB release 二进制，BUILD_RC=0，耗时 62m37s**。

**端到端结果**

| 步骤 | 结果 |
|---|---|
| `workspace seal <WID>` | ✅ rc=0，**8 ms**，输出 revision `01a1bb50-…-239efa53e80b:1:dbf51fa6737b6880…` |
| `workspace fork <REV> --count 1` | ✅ rc=0，**9 ms**，产出新 workspace |
| 挂 fork 验证可见性 | ❌ **sealbase 目录 0 文件**（base 的 20 个文件看不到） |
| 写隔离 | 未能验证（因为 fork 看不到数据，写隔离无意义） |

**结论**：seal 本身工作（元数据封存 8 ms），fork 也工作（9 ms），但 **fork 出来的 workspace 挂载后没有把 sealed base 作为 lower layer 接上**，
所以"seal → fork 可见性"这条链仍差最后一环。下一步要查 CLI 里 fork/mount 如何传递 base layer（`mount --workspace` 目前只按 workspace-v1 挂 upper）。

**原始输出**：build 机 `/srv/ax-data/sealtest/`（meta.db、fork.json、mount 日志）；构建日志 `/root/build-seal3.log`

'''
anchor = u'## C3 — 百万小文件'
idx = text.index(anchor)
text = text[:idx] + add + text[idx:]
with io.open(mp, 'w', encoding='utf-8') as f:
    f.write(text)

entry = u'''
---

## 2026-11-08 21:20 任务 C（C4 seal）进展：seal/fork 通了，可见性仍缺一环

1. **编译问题全部解决**（这次是干净的路径）：
   - 新建 8c32g + 260 GiB 构建机 `ax-c1-c-build`（有公网带宽，能装 rust）
   - rust 走阿里云镜像安装（`RUSTUP_DIST_SERVER=https://mirrors.aliyun.com/rustup`），cargo 走 rsproxy 镜像
   - 需要 `protobuf-compiler`（etcd-client 的 build script 依赖 protoc）
   - **`git diff` 不会包含未跟踪的新文件** ⇒ seal.rs（564 行）要单独打包传入，否则报 `file not found for module seal`
   - 内存策略：64 GB swap + `-j2` + **不要改 LTO/cgu**（改了会触发依赖全量重编）⇒ 62m37s 编完，BUILD_RC=0
2. **端到端**：`seal` 8 ms / `fork` 9 ms 都成功，但 fork 挂载后 base 的 20 个文件**看不到（0 文件）**。
3. **下一步**：查 fork 记录里 base layer 的传递方式（fork.json 里 `upper_bucket: null`、`state: Active`），
   以及 `mount --workspace` 是否支持挂 workspace-v1 的 lower（可能需要 `--volume-format workspace-native-v2` 或显式 base 参数）。
'''
with io.open(dp, encoding='utf-8') as f:
    d = f.read()
d = d.rstrip() + u'\n' + entry
with io.open(dp, 'w', encoding='utf-8') as f:
    f.write(d)
print('ok', len(text.encode('utf-8')), len(d.encode('utf-8')))
