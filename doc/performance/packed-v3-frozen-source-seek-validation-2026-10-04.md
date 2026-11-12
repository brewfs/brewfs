# v3 根描述符、冻结源与导入索引定位验证

当前源码366项绑定的46项完整检查、8次真实FUSE和3项真实Btrfs库测试全部通过；
统一核对记录为`frozen-source/checkpoint.json`。36k导入两次600秒超时，规模出口未通过。
不声明全部SPEC、系统S、实验X或性能接受。用户当前范围仅v3/005。

## 缺陷与改动

两项行为red复现目录导入拒绝超过PATH_MAX的合法深路径，以及接受源根祖先的
符号链接。所有namespace、stat、readlink、cold和payload现在沿同一root FD按组件
读取，中间组件no-follow，名字保持NAME_MAX，relative path限制64KiB resident bytes。
目录用owned fdopendir/readdir，regular数据用opened descriptor/read-at；special/link
xattr通过live parent FD加一个短组件读取，不打开FIFO/socket/device payload。
跨mount/device/subvolume拒绝。显式single-file API仍为较弱的stat-revalidated捕获。

`SnapshotBacked`只接受真实Btrfs readonly snapshot root，验证filesystem magic、
inode256、FSID、UUID/parentUUID、treeID、generation/change transaction与两个独立
readonly flags域。库存持有FrozenSourceLease直到producer finish后的最终guard；
BestEffortDetected用相同root FD和stat fences，但保持非原子语义。管理员及mount
namespace属于可信冻结协议，readonly flags不是对恶意管理员的互斥锁。

首次真实Btrfs检查另暴露GET_SUBVOL_INFO成功可返回1，草稿`==0`误判unavailable。
`btrfs-preliminary02/mutable-ioctls.log`保存实际返回值；exact Microsoft
linux-msft-wsl-6.18.33.2源码说明backref搜索返回1仍复制有效identity。已改标准
nonnegative成功判断，UUID/readonly等字段校验保留。preliminary03已完成正向与拒绝
场景，失败01/02日志及owned image保留；首版harness的losetup参数错误也保留，设备
经backing identity核对后已安全detach，后续脚本修正并独立记录cleanup。

36k首次导入在600秒超时，源payload不足8KiB，却观测rchar约304GB。六个nullable
OR游标未成为索引seek，LIMIT1反复扫描前缀，总工作量呈二次增长。现在分开初始与
continuation查询，dentry用(parent,name) tuple seek，其余按现有单key或(root,key)
范围直接定位；保留LIMIT1大cold记录预算与LIMIT128属性扫描，不加载整个namespace。

## 已执行证据

G02 artifact：`docker/compose-xfstests/artifacts/packed-v3-completion-20261004-frozen-source/`。
SQL artifact：`docker/compose-xfstests/artifacts/packed-v3-completion-20261004-import-seek/`。

- `namespace-red.log`：37 passed、2 failed，明确深路径与祖先symlink行为缺陷。
  `cli-red.log`：原CLI拒绝真实snapshot provider选项。
- 修复后`source-green01.log`49 passed、3 ignored；`fixture-green01.log`5 passed。
  再接查询修复后`packed-green01.log`151 passed、2 ignored。ignored不算实际provider证据。
- 当前生产SQL的`active-red.log/json`全部12项tail/EOF复杂度子项失败，语义遍历已通过；
  `active-green.log/json`2 tests、全部子项通过。真实失败数据库副本上dentry VM步骤
  360,064→59，fence144,034→23，assignment216,039→24，migration58,907→31。
  这是数据库指令工作量，尚非完整导入wall-time/RSS或性能优势。
- `btrfs-preliminary03`及`btrfs-final`实际192MiB owned loop/private namespace验证通过：
  readonly snapshot回读不受mutable original变化影响；普通目录、可写subvolume、
  readonly非snapshot子卷、readonly bind、nested mount拒绝；guard撤销拒绝。
  manifest实际上传且认证读取成功后撤销readonly，build拒绝返回trusted ref，上传
  manifest仍合法但为orphan。三项ignored库测试均显式执行各1项，不能零测试通过。
  `cleanup.json`记录正常unmount、匹配backing后的detach、owned work清理。

## 正在验收与开放边界

`source-hashes.json`绑定366项源码，`final-gate/status.json`46项全部exit0：default
lib/bin1,023/1,107、overlay1,348、fixture7、vendor tokio/io-uring各13、native/packed174。
挂载使用`binaries/`固定副本；ACL/namespace/external/deep各raw/zstd共8次通过。
`fuse-deep-source02`验证5,474-byte深路径、raw名称、hardlinks、binary xattr、symlink/FIFO
及72MiB sparse external hole/EOF，正常20秒卸载与daemon退出、owned work清理通过。
首次`fuse-deep-source`因harness使用无效`-c`参数在挂载前失败，保留日志和work；
修正真实CLI参数后重验，不把首次失败计为生产缺陷或FUSE通过。

`btrfs-final02`使用固定fixture及本轮已执行overlay测试的固定库测试binary，SHA保存于
`btrfs-binary-harness-sha256.json`；三项真实ignored测试均显式执行1项，provider身份
及成功/拒绝/最终guard和正常cleanup再次核对。统一verifier确认source/binary身份
一致、无BrewFS mount/daemon/fixture进程和D-state任务。

第二次36k导入`deep-cookie/runs/fuse-36k-seek01`仍在600秒超时，未进入挂载；失败spools/
diagnostics/work保留。约504秒观测rchar15.5GB、write_bytes7.1GB，producer超时仅9,219
identities而source36,003已完整。六项seek减少SQLite扫描，但每项多次小事务仍阻塞导入；
下一批改私有spool的有界事务，不延长deadline、不缩减36k规模、不关闭SQLite同步。
首次`runs/fuse-36k-final`和第二次失败均不算规模通过，SQL指令减少不代替完整导入验收。

G02不实现workspace publication、journal或GC；manifest上传成功与可信ref返回只是
source guard协议。大目录完整SPEC还需要更大规模、实际eviction/refetch、hot protection、
active inode与内存证据。G06/G07完整观测与生命周期预算、可写ACL及workspace/operator
出口继续开放。此前20秒正常卸载后daemon不退出的根因仍未解释，成功重试不关闭它。
