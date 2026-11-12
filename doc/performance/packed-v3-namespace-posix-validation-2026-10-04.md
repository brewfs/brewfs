# Packed v3 source names / xattr validation — 2026-10-04

本批接续[目录inventory checkpoint](packed-v3-namespace-validation-2026-10-04.md)，
修复实际源名称与FUSE接口的四项错误。实挂载、定向测试、完整门禁和身份核验已通过；
当前不是整套POSIX、全部SPEC、S/X或性能接受。完整目标仍见
[五份SPEC清单](../superpowers/plans/2026-10-04-brewfs-all-spec-completion.md)。

## 四项真实行为 red

artifact：`docker/compose-xfstests/artifacts/packed-v3-completion-20261004-namespace-posix/`。
`initial-source-hashes.json` 绑定上一批签收的356个源码文件。`red/` 在上一批实际
mount binary上对四项检查逐一收集失败，并正常卸载；不以第一个失败代替其他问题。

1. root源文件`.stats`读到虚拟统计数据；真实大小/内容被遮住。
2. 源中同时存在`b"user.name-\xff"`与UTF8 U+FFFD名称，值各不相同。
   FUSE有损转换把前者读成后者，出现成功返回错误数据。
3. listxattr对同一个合法Linux源返回EIO，因readonly字符串接口无法表示raw名称。
4. immutable removexattr的EROFS在custom mapper中变成EIO；源值仍在，但错误码错误。

原始`fuse-red.log`、`red/summary.json`和binary hashes保留。首次新增测试helper漏掉Arc
import，编译失败保存在`fuse-unit-green.log`；修正后独立运行日志为
`fuse-unit-green-repaired.log`。编译修正不计作四项行为red。

## 修复与兼容

MetaLayer新增byte-name get/list/set/remove扩展；默认字符串后端明确拒绝无法表示的名称。
PackedReadonly从manifest认证的CA05直接按byte查找/列出，raw mutation继续返回EROFS。
既有string APIs保持UTF8兼容；String list面对raw names仍显式拒绝，不做替换或截断。
VFS/FUSE使用byte接口；`system.*`隐藏、POSIX ACL EOPNOTSUPP、只允许user namespace写入
的现有规则按byte前缀/准确名称检查。新EINVAL映射保证native字符串后端不误改UTF8 alias。
removexattr增加readonly错误映射；成功的mutable更新保持既有kernel invalidation语义。

`.stats` lookup先检查父目录search权限，再查询真实dentry；存在则返回源属性/原inode，
不存在才提供虚拟文件，metadata错误继续传播。真实file/directory都不会被遮住；
旧统计入口在没有同名源项时仍可使用。源占用该名称时，`.stats`用于源数据，
不能从这个文件读取虚拟统计；完整G06 metrics出口仍待补。
004/PM07/PM08、GM07、cold格式和producer字节保持原解释。

## 验证

定向FUSE库58项通过，包括真实inventory→producer→authenticated snapshot→readonly→VFS→
Filesystem回归；新增3项涵盖source.stats file/directory/virtual fallback、raw与UTF8 alias
独立值/size probe/ERANGE/missing ENODATA/list/String拒绝/EROFS，以及native raw mutation
EINVAL且旧UTF8 alias不变。已有内部xattr隐藏和reserved namespace回归也通过。

- `green/`：raw/zstd各一次真实FUSE，四项检查全部通过，daemon0、kernel mount移除。
- `namespace-regression/`：本批binary重跑raw/zstd各540 entries、完整21,383,198 bytes和
  partial2,699,586 bytes，namespace/attrs/blocks/xattrs/硬链接/specials/九类EROFS均通过；
  正常卸载。共享读取路径改变后进行的回归，不是重复unchanged-code性能测量。
- `final-gate/`：本批同迭代40项AGENTS与受影响features/vendor gate全部通过：
  default workspace lib/bin分别1,016/1,100 passed，overlay lib1,316 passed，fixture3、
  vendor13。`verification.json`核对356个源码文件（变更4、增删0）、实际挂载binary
  hashes与上一批red身份；无brewfs挂载/daemon/D-state残留。

debug correctness；没有drop host page cache或paired性能比较。没有云资源或吞吐表更新，
最小gate不代表all-feature/operator/严格warnings CI。生成物不进入源码提交。

## 尚未完成

G04仅关闭上述raw xattr/源.stats/removexattr子项。Linux POSIX ACL权限/继承、其他raw
namespace mutation边界、PATH_MAX和原子frozen-view仍待独立验证。G03 external-large、
G05–G09分页/metrics/共享预算/同executor/005 pipeline、G10–G14 binding/fence/发布恢复/
可达性GC/operator，以及v2独立出口和最终实验/交付仍为必需。
目标保持active，S/X未签收；系统完成后才冻结三个创新点的最终实验。
