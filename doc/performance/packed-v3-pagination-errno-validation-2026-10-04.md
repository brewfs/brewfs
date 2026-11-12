# Packed-v3 PM10/IP06、错误传播与唯一入口（2026-10-04）

状态：冻结候选的45项完整门禁和8次bounded真实FUSE回归通过；36k导入超时，
目录规模出口未签收。机器核对记录为该批次`checkpoint.json`。
仅保留v3/005；本文不声明系统完整、性能接受或全部SPEC完成。

## 具体问题与改动

旧readdir从Groups索引首部取引用，即使只请求一项，也会预取大量前置索引页。
RecordingBackend的行为red实际记录了这类range GET，保存于
`packed-v3-completion-20261004-deep-cookie/behavior-red.log`。首次草稿误把u64传给旧
usize接口的编译失败保留为`red.log`，不当成行为red。

当前唯一manifest payload改为PM10，全部索引改为IP06。索引child携带mandatory
subtree_weight，Group叶子权重由认证entry_count派生，其余索引叶子权重为1；checked
sum、所选child的weight/height/fence验证拒绝错误。manifest固定Groups总dentry weight。
分页计算目录前缀rank加u64 ordinal，直接select对应group，保留有界路径栈按需访问
后续节点；不提前收集64项refs。streaming builder仍逐层有界buffer，磁盘spool不改。
旧PM07/08/09和IP05明确拒绝，无线性扫描兼容回退。最大ordinal为i64::MAX-2；
虚拟.stats不进入readdir，真实源.stats计入普通entry，所以最后child cookie仍可表示。

索引故障另暴露了Option接口吞错。两项真实fixture red证明hot inode/group checksum
损坏被误报ENOENT；typed stat与ancestor lookup使认证/backend错误按EIO传播，真正
不存在保留ENOENT、ACL不存在保留ENODATA。四类xattr前置检查、权限helper和opendir
已迁移。getattr额外red证明已有句柄会让损坏stat成功返回nlink0；现在只有Ok(None)
才可回退合法已删除句柄属性，Err直接返回。其余Option调用仍须逐项审计。

外部产品入口不再支持packed独立v1/v2，CLI/YAML明确拒绝；main移除旧mount分发。
v3 mount在64-byte probe后拒绝004/003/错误kind/未知magic，不做全对象GET。Cargo
关闭自动bin发现，仅暴露brewfs和v3 fixture；Aliyun runner只v3并明确wire5。
native/flat工作区基线与共享input类型继续服务三创新，旧内部reader表示尚需收口。

## 当前证据和仍开放的边界

- packed相关定向147项通过，包含raw/zstd变长groups、非零prefix rank、浅/中/深
  GET预算、跨组、EOF、cookie上限、缺失/损坏payload和计数拒绝。
- typed-stat两项red/green均保存；额外getattr red/green通过。旧最终ACL binary的
  真实hot故障挂载复现fstat成功、get/list/set/remove报ENOENT，正常卸载。最终修复
  binary的raw/zstd故障实挂载均通过：checksum、missing-object、backend-read三类
  故障的fstat/get/list/access/set/remove全部返回EIO，daemon正常退出。
- v3-only四项配置/支持red、probe实际fullGET red、targets和runner red已保存，
  修复后170项packed/native相关测试及完整feature门禁通过。
- 本集成批次源码冻结为364文件，`integration-source-hashes.json`绑定完整gate；
  门禁45项，包括AGENTS、overlay/vendor/runtime、新入口和native frozen基线检查。
  45项全部通过；default lib/bin分别1,023/1,107、overlay lib1,344、fixture4、
  vendor两runtime各13项通过，ignored单列。完整源码与8次挂载binary身份均已核对。

artifact根分别为`packed-v3-completion-20261004-{deep-cookie,typed-stat,v3-only}`。
最终ACL/namespace/external及hot故障回归raw/zstd共8次通过，零残留mount/daemon/
D-state。36k首次`runs/fuse-36k-final`在fixture导入600秒超时，进程已停止，尚未挂载；
失败工作目录、SQLite spools、thread/fd诊断与超时记录保留。源条目36,003，producer
当时仅11,776个identity；观测rchar约304GB。查询计划表明nullable游标未成为有效seek，
fence仍SCAN，dentry只按非NULL parent开头搜索，继续修复后重做验收。不能将延长
timeout或减小目录当成36k通过。更大目录内存/扫描规模、hot protection、active inode、G06/G07完整计数与
共享预算仍开放；库请求预算不等于整个large-directory SPEC完成。

上批namespace曾在正常卸载后20秒未退出，失败日志保留。此后原日志级别12次复测
正常退出，仍未解释首次挂起。已加显式启用的轻量ring/session诊断，默认关闭，不
改变退出策略；安全取消buffer ownership与异常cancel/drain还在隔离草稿，尚未接入。
本批不凭成功retry声明退出问题解决，也不接受有未解释挂起的最终性能测量。
