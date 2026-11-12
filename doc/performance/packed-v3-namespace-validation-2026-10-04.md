# Packed v3 bounded namespace validation — 2026-10-04

本批接续 PM08 root/allocated-block，实现真实目录树的有界 inventory/import。
本批完整门禁和源码身份核验已通过；签收范围仅为下述目录导入与只读修复。
全部五份 SPEC 目标见[持续清单](../superpowers/plans/2026-10-04-brewfs-all-spec-completion.md)，
S/X 未通过，本文不宣称完整 POSIX、原子源快照、packed workspace 或性能接受。

## 实现范围

Linux `V3SourceNamespaceInventory` 使用私有 SQLite spool 保存源 identity、raw paths、
hot/cold 属性、stat tokens 和目录 queue。一次读一个目录/节点，磁盘排序后按 parent/
raw-name 分页构建；source fences 每次至多128个路径，payload build 每次取一个节点，
不会把 namespace 或全部 cold attributes 同时放入 Vec。

root 和各目录保存实际属性；非目录以 dev/ino 聚合，分配稳定 snapshot inode。
symlink 保留 raw target，FIFO/socket/device 只读元数据，保留 kind/rdev，不打开设备
数据。PM08/RA05/SI05 继续认证 root/blocks，旧 PM07 和 004 解释保持原样。

调用方必须显式选择 `best-effort-detected` 与 `visible-links` 或 `reject-external`。
前者检测普通 content/xattr/path/namespace mutation，不提供原子 snapshot；后者
分别按树内 aliases 设置 non-directory nlink，或在存在树外 alias 时拒绝。源 nlink
参与 token；重复目录 identity、源内 spool、源变化均 fail closed。inventory 后、
payload capture 与 manifest finish 前后重新验证。返回 verified manifest ref 仍不是
workspace head publication；SnapshotBacked/frozen-view 协议仍待补。

同 parent 的 payload 按现有 profile 有界 co-pack。dense 小文件可在224 KiB group
inline 预算内 inline；非 inline 同 class 尾 frame 可共享并拆分 extent，超过 GM07
256 extent-record cap 的情况保持整 frame。flush 前同步所有引用的最终 raw_len，
避免共享 frame 增长后留下失效 placement。逻辑/数据计数和 group ID 具有溢出检查。

fixture 新增 `--source-directory`；要求 wire5、显式 consistency/hardlink policies，
禁止混合 generated/source-file corpus。provenance 保存 capture scope、策略、PM08、
source/group/frame/inline totals 和 peak group raw payload。规范化实际目的路径后，
检查 output、object prefix、manifest 及 `.source.json` sidecar 的源内/symlink aliases，
防止输出写回源树；私有 spool 始终在源树外。

## 发现的缺陷和失败证据

artifact：`docker/compose-xfstests/artifacts/packed-v3-completion-20261004-namespace/`。

- `namespace-cli-red.log`：原二进制缺少目录导入入口。
- `namespace-unit-red.log`：5项回归中3项失败，暴露共享 frame raw_len 失配、未填
  frame 尾部、空目录未验证 file limits；修复后全部通过，随后增加 cancel cleanup
  和200 extent 不扩大 cap 两项，共7项 namespace 回归。
- `fuse-run01/`：实际 rename 返回 EIO。FUSE rename/link custom mapper 漏掉 readonly
  错误；`readonly-rename-red.log` 用 inventory→producer→认证manifest→readonly→VFS→
  Filesystem 请求复现 Errno5/30，修复后 `readonly-rename-green.log` 通过。
- `fuse-run02/`：write open 被允许。fresh/cached VFS open 使用 `record_open`，绕过
  immutable `.open`；新增 override 在 handle 分配前拒绝 write/append。
  `readonly-open-red.log` 和 `readonly-open-green.log` 保留行为 red/green，覆盖
  O_WRONLY/O_RDWR/O_APPEND 与 cached-attr open。
- run02 的失败 oracle 未关闭意外成功 fd，造成 busy unmount。断开的 FUSE transport
  又使 `ismount` 误报卸载；原 summary 的 mount_removed=true 不作为清理证据。
  正常 fusermount 修复和 owned-directory 清理记录在 `run02-cleanup-repair.json`。
  新 oracle 立即关闭意外 fd、用 kernel mountinfo 检查卸载，先卸载再等待 daemon。
- `packed-green-compile-failure.log` 保存新增测试 enum 写法错误，修正后通过；这是
  编译修正记录，不是行为 red。全部原始失败保持独立，不覆盖失败产物。

## 当前验证

最终同迭代40项 AGENTS/受影响 feature/vendor 本地门禁在 `acceptance-gate/` 全部通过；
前两轮 `final-gate/` 和 `repaired-gate/` 保留原身份，不能替代最后一次 open 修复的 gate。
`verification.json` 已核对356个最终源码文件及实际挂载binary hashes：默认workspace
lib/bin分别1,015/1,099 passed，overlay lib1,313 passed，fixture3 passed，vendor13 passed。
源码新增1个文件、变更7个文件、删除0；无brewfs挂载/daemon或D-state任务残留。
最终源码/binary hashes、test counts 和资源检查由 `verify-evidence.py` 汇总。

`cli-acceptance/` 的目录导入、四种输出 guard 和 source unchanged 检查已通过。
`fuse-run03/` 的 raw/zstd 两个真实目录 corpus 各540 entries、六个 groups、四个 frames：

| 验证 | raw | zstd |
| --- | --- | --- |
| 完整回读 bytes | 10,691,599 | 10,691,599 |
| partial 回读 bytes | 1,349,793 | 1,349,793 |
| namespace/data/attr errors | 0 | 0 |
| 修改拒绝检查 | 九类均 EROFS | 九类均 EROFS |
| daemon exit / kernel mount 移除 | 0 / 是 | 0 / 是 |

corpus 包括520-wide目录、深目录/raw目录和文件名、跨目录hardlink及树外alias、
inline/共享/跨frame dense、8 MiB sparse、all-hole、empty、raw/internal/external symlinks、
FIFO/socket/char1:3/block7:0、binary xattrs。目录内容、hardlink inode、hot fields/blocks、
xattr值、symlink targets、完整/partial字节均对 source oracle 比对。mtime/ctime 等九个
字段在实挂载核对；atime 捕获另由库回归核对，不混写为 fresh source atime 实测。
九类动作是 mkdir/unlink/rename/setxattr/write-open/link/symlink/rmdir/chmod。

debug correctness 使用 metadata8 MiB、payload/SSD/decoded/window caches0、TTL0、
directIO/keep-cache0；未 drop host page cache，不是 strict-cold 性能结果。
没有运行云 campaign 或修改吞吐比较表；最小 gate 不代表 all-feature/operator/
严格 `-D warnings` GitHub CI。旧 warnings 保持可见。

## 未完成边界

G02 的 bounded complete-directory inventory 子项与 G04 的 specials/树内硬链接策略已有
代码和上述真实验证，但全项仍不能关闭：best-effort 检测不是原子 frozen view；
source PATH_MAX 边界仍拒绝；Linux POSIX ACL 权限/继承尚未应用；raw xattr 名称仍须
FUSE 端到端补齐，根 `.stats` 源文件还会被虚拟统计入口遮住。
G03 external-large 仍待 producer/reader，logical64 MiB/data16 MiB/256extents 仍限。

下一步先修复这些可复现 POSIX/name 边界，再按
[external-placement实施契约](../superpowers/plans/2026-10-04-packed-v3-external-placement-plan.md)
继续 G03；随后共享 budgets/metrics/pagination/executor/005 pipeline、packed binding/
fence/publication/recovery/GC、v2/operator独立出口及实验/交付仍全部必需。

后续raw xattr/.stats/removexattr子项已独立修复与签收，见
[名称边界报告](packed-v3-namespace-posix-validation-2026-10-04.md)。本报告保留该批次
的源码/测试/失败范围；后续结果不改写上述原始checkpoint证据。
