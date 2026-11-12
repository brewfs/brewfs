# v3 source 库存有界事务验证

状态：生产修复位于`source_namespace.rs`；另同步`source_layout.rs`中的稀疏测试fixture。
行为red与167项packed green已通过，366源码冻结；首次完整gate为46/47，修正fixture后
定向测试、47项gate02和新库binary的三项真实Btrfs重验全部通过。固定binary的14次真实
FUSE、36k分页/重启与正常清理身份已核对；`checkpoint02.json`明确记录旧失败work的
保留缺失。本批源库存事务修复签收，全部SPEC、S/X和性能未签收。

## 诊断和修改

前一批producer的128项事务已通过完整gate，但相同36k的第三次600秒超时仍未挂载。
只读失败spool取证显示库存完整36,003项、snapshot_inode已编号29,571/未编号6,432，
producer的records/identities均0、integrity_check=ok。此次补的是更前面的源库存阶段。
前三次失败和work/spools/diagnostics均保留于deep-cookie runs，不覆盖或提高期限。

`SourceInventoryBatch`持有私有SQLx事务：每批最多128项insert/目录完成操作，一次只
保留一个源属性记录。目录队列、identity查询、directory fence都用同一连接，避免
单连接pool嵌套等待。commit成功后才更新inventory report；错误或取消丢弃事务回滚。
master_path/visible_links按source_id seek页128项分组，编号按master_path seek页128项
事务写入。源root-FD、stat/cold/final manifest fences、编号顺序、root保留编号和
hardlink策略不变，SQLite保持既有FULL/DELETE同步与日志配置。

私有库存不是crash-resume或workspace发布协议。中途失败可以留下已提交prefix，但
不能返回完整inventory或verified manifest。最终认证与publication要求继续独立验收。

## 已执行证据

artifact：`docker/compose-xfstests/artifacts/packed-v3-completion-20261004-source-batch/`。

- 现有API red两项实测失败：257文件+root产生260次真实SQLite commits；bad links
  在第91条发生后，留下91个已编号条目。见`active-red.log`。
- 修复后`active-green01.log`163项packed通过；补充边界后`active-green02.log`167通过、
  3项真实Btrfs测试仍ignored，需provider harness显式执行，不计为本次通过。
- 实际commit hook证明capture/assignment有界；保持同步配置；首批commit拒绝时rows/
  report均0；duplicate新identity/locator回滚；取消释放唯一连接且不删用户文件。
- late assignment错误仅保留128项已提交prefix；首grouping页错误回滚master/visible；
  跨目录130组hardlinks及目录队列闭合、RejectExternal和stable编号通过；真实capture
  第二批取消只保留128条prefix和已commit report。

固定brewfs hash与上一批相同（本次源构建代码仅fixture使用）；fixture hash为
`c98d2b143159219324db6e512e3dad13478dcb4779e45042859b5a086278227d`。
最终`final-gate02`47项全部通过：default lib/bin为1,023/1,107，overlay lib为1,364，
fixture7、vendor tokio/io-uring各13、native packed190、readonly compile-fail3。
新library executable由已执行overlay log定位并固定；`btrfs-final02`三项真实Btrfs测试
及五类CLI拒绝通过，loop/work清理正常。`production-binary-comparison02.json`证明此次
test-only同步修正后brewfs/fixture与原固定binary逐字节一致，复用本批已执行的8次
ACL/namespace/external/deep raw/zstd FUSE及6次规模挂载证据，不把复用写成新实挂载。

相同36k/600秒约束下，raw/zstd各36,001个root entries、36,003 unique inodes、72 groups
完整导入、ordinal/lseek/seekdir/telldir/rewind/cross-group/EOF与active fd通过；fresh restart
重读也通过。另两行是3-entry的真实源.stats优先检查，不是另两轮36k。四行对应6次实际
挂载，都在20秒期限内正常卸载且work删除；先前三次超时均未覆盖。验证仅为correctness，
不报告active带宽或性能胜出。`verify-checkpoint02.py`执行通过，source/binary/harness与
当前owned mounts/daemons/fixture/D-state均核对；原20秒卸载hang根因仍开放。
该大目录harness目前只能证明cache-disabled/restart refetch，actual typed GET attribution
和真实cache eviction还在G06/G07候选中；不能据此关闭完整G05。

## 测试fixture和磁盘恢复

首次47项gate中46项通过，唯一失败是既有稀疏source测试capture拒绝
`wire 005 source changed during capture/publication`。它写300段data并set_len到72MiB后
没有同步，就开始记录包含st_blocks的源token。原失败未记录变化字段，不能断言已经
证明只是delayed allocation。此次在fixture的set_len后sync_all，使测试所声明的immutable
输入稳定；生产source token/fence与拒绝逻辑未放宽。定向`green02`实际1 passed/0 failed；
原`final-gate`和失败log保留，新冻结为`source-hashes02.json`、新门禁为`final-gate02`。

磁盘故障时宿主C满，WSL出现emergency readonly/I/O；恢复复查确认ext4 recovery complete、
root rw及普通工具可用。用户授权清磁盘后，仅删除可重新构建的incremental/release/vendor
target缓存，合计12,543,689,983逻辑bytes；trim后宿主C实际增加10,567,299,072 bytes，
当时可用44,440,477,696 bytes。trim报告的788.9GiB不是宿主释放空间，不能拿它作清理量。
源码hash与五项固定binary/harness manifest核对通过；366源码另备份D。后续Cargo限并发，
门禁各项开始前要求宿主C至少18GiB空间，低于此值保留进度并停止。

首次恢复检查时，旧三次失败36k的/tmp work/spool以及代理临时源码树已不存在；本次
缓存清理没有这些路径。持久artifact中的失败logs、timeout/preserved-work JSON和source
诊断仍在，旧checkpoint保留原签收时的状态。新checkpoint必须报告原失败数据库当前缺失，
不能把旧`preserved-work.json`当作仍存在的证明。G04/observer/native持久draft均已重新
导出保护，但丢失的最后core增量需重建和重验；孤立绿不代替active签收。
