# Packed-v3 PM09 external placement 验证（2026-10-04）

状态：PM09 external placement 与 G03 源 sparse/allocated-blocks 契约已通过最终验收。
S/X仍未通过，不声明性能收益；全部 SPEC 持续目标保持 active。

## 问题、实现和兼容范围

上一批已接受的 fixture 对72 MiB all-hole、20 MiB dense、300段 sparse 数据均拒绝，
没有生成 manifest-output。独立原始行为 red 与绑定旧 binary 的哈希保存在
`docker/compose-xfstests/artifacts/packed-v3-completion-20261004-external/source-cli-red.json`。
补全不能依靠增大 GM07/整文件 Vec 限额，也不能把缺失 placement 当成 sparse hole。

新增 PM09/EX09 required selector、PS09、LE09 和 LD05。PM07/PM08/004 的字节与解释
保持兼容；只有发生 external placement 的源导入生成 PM09，所有 regular inode
必须有一个 Group 或 External selector。首次外部文件出现前已发布的 regular inode
通过磁盘逐行补 Group selector；missing/orphan/nonregular/size mismatch/unversioned
selector 阻止 finish。源 root/SI05 allocations、cold、nlink 闭合仍然必须通过。

Linux source layout 用私有 SQLite runs 和 no-follow pinned fd；每次最多读取一个
profile frame，chunk最多 raw16 MiB、stored body24 MiB、1,024 frames，帧 raw最大8 MiB。
多个 chunks 共用 manifest Containers/Frames roots，FD05 绑定 chunk digest/length/
profile/class/offset/codec/stored digest。zstd膨胀时 descriptor 显式选择 raw。
外部 extent tree 单页最多256 KiB；producer每页目标256 records，遍历不会累积全文件
extent refs。all-hole使用经过认证的空树，其 required selector 仍必须存在。

reader 对请求区间执行 overlap pagination；落在 extent 中间的请求也取得该 leaf。
用 `(container_ordinal, frame_ordinal)` 区分不同 chunks，校验 LE09 的 key/fences/inode/
EOF/raw range，然后复用统一 PackedFrame plan/executor。external hardlinks 只生产一次
payload/selector，别名 hot/cold/blocks 与反向 dentry 保留。旧 synthetic slice facade
明确拒绝 External，防止 hot-only GroupMeta 被误读为 all-hole。

单次 prepare 保留 output、metadata、extent/segment、frame bookkeeping与stored/raw预算
检查；这不证明 G07 的 mount-wide 生命周期预算或 G09 的跨请求singleflight完成。
源声明仍为 best-effort/stat revalidation，不是原子 frozen view；finish也不是workspace
head publication。孤立上传对象的系统 reachability GC 继续属于 G13。

## 开发与失败证据

- `contracts-green.log`、`layout-chunk-green.log` 保存早期51/54项 wire005 局部测试。
- `production-first-check.log` 保存函数参数和GroupInput字段接线的编译错误；修正后
  `production-check-repaired.log` 54项通过。编译错误不充当行为 red。
- `external-namespace-first.log` 保存测试 helper 的 chunk_id import 错误；修正后的
  `external-namespace-repaired.log` 保存首次混合源回读通过。
- `production-contract-green.log`、`production-contract-final.log` 保存后续57/58项
  定向结果；最终全部门禁以 `final-gate/` 为准。
- 首轮真实 single-source CLI 因内部输入误用 GM07 而拒绝；原日志在
  `source-cli-green/`。修复为内部 GM06 输入，由容器 builder 生成 stored GM07，
  并增加正式自动 admission 回归；六项最终 CLI 已通过，见 `source-cli-final/`。
- `fuse-green/`、`fuse-run02/` 保留未同步测试源的 sparse st_blocks 差异。
  run02记录导入前及导入结束源 blocks=2400，mount也为2400；延迟写回后源变为2408，
  size/mode/uid/gid/mtime/ctime/nlink不变。生成源在导入前逐fd fsync后重新验证，
  不更改 snapshot blocks 或放宽源 token 校验。
- `fuse-run03/` 在同步后完成全量/partial/属性比对，随后失败于测试错误地要求虚拟
  `.stats` 出现在 readdir。按既有虚拟入口行为修正 oracle；源真实 `.stats` 优先
  仍由上一批独立回归保证。
- `gate-run01/` 首轮40项检查全部通过，但过程中改过单文件输入与源oracle，
  不作为最终代码签收。完整40项由最后一轮独立重跑。

## 最终验收与剩余范围

最终机器可读结果为
`docker/compose-xfstests/artifacts/packed-v3-completion-20261004-external/verification.json`。

- `final-gate/` 的40项检查全部通过：fmt/scripts/report/scanner/vendor、default check/
  build、runtime features、default workspace lib/bin tests、overlay full lib、fixture
  tests、clippy及diff。default lib/bin分别1,016/1,100 passed、225 ignored；overlay
  1,328 passed、227 ignored；fixture3、vendor13 passed。clippy exit0但仍有warnings，
  不声明all-feature/operator或`-D warnings`完整CI通过。
- `source-cli-final/` 对原三个red语料各运行single-file与directory入口，共6项通过；
  provenance均为PM09，成功生成manifest-output。单文件scope仍只是source-parent属性
  加一个dentry，不能冒充完整目录inventory。
- `fuse-final/` raw/zstd各525 entries：68 MiB dense、72 MiB all-hole、300段稀疏数据、
  520个tiny文件和跨目录hardlink。每组317个external frames、6个实际LD05 objects；
  hardlink不重复生产外部payload。完整回读总计444,672,144 bytes，partial总计
  3,986,534 bytes；包括chunk/frame边界、第二extent页、EOF和hole。
- source/mount size、blocks、mode、uid/gid、mtime/ctime、nlink匹配；跨目录inode身份、
  binary xattrs/readdir与write-open/unlink/link/rename/setxattr/removexattr六类EROFS
  通过；两次外部挂载均正常卸载。
- `namespace-regression/` raw/zstd各540 entries全部通过，完整总21,383,198 bytes；
  raw paths、special/rdev、既有source属性/cold/alias和九类EROFS均保持通过。
- 四次挂载的运行期后端failures为0。最终源码集合360文件，相对上一批新增4、变更10、
  删除0；binary hashes在CLI/external/namespace之间一致且与当前文件匹配。
  最后kernel mountinfo、brewfs daemons及D-state列表均为空。

后续G04 named-user ACL grant实挂载red已独立保存在
`packed-v3-completion-20261004-acl/grant-red02/`；没有因此修改本批Rust代码或把ACL签收。
其初次启动配置错误留在`grant-red/`，不充当ACL行为red。

ACL/其他raw mutation、frozen-view/PATH_MAX、深cookie、完整metrics、共享预算、native/
packed同executor、005 pipeline、binding/fence/publication/recovery/GC、v2/operator
独立出口及实验/交付仍开放。debug/local-fs正确性结果不构成配对性能接受。
