# 小文件温态扫描：FUSE 属性/目录项 TTL 归因（2026-11-04）

只放宽挂载 TTL，原 scanner 的每文件延迟中位数从 **1.665692 ms** 降至
**0.871178 ms（1s）/ 0.853342 ms（10s）**。1s 的配对平均降幅为 **47.61%**
（95% CI **43.34%–51.88%**）。因此，#416 的 TTL=0 契约确实承担了很大一部分
往返成本。1s 与 10s 的差异没有达到这三轮可以分辨的程度。

原 scanner 的 6 次 GETATTR/文件中，**3 次是内核路径/打开期间的自动重验证，
另 3 次对应用户态 stat 类调用：显式 stat 1 次、Python 隐式 fstat 2 次**。
这两种来源都受内核 TTL 控制：保留全部 scanner 调用时，TTL=1s 已将文件级
GETATTR 几乎全部消除。不能把“scanner 调用 stat”与“TTL=0 引起 FUSE 往返”
视作互斥解释。

本分支从 `main=f5706712302435206269a253bf861a1a648761d1` 起，只增加文档和
测量证据。复用 main 06:28 的 **409,876,496 B** 二进制，SHA-256 为
`e2a308519ad1e0f5d2295e10cf3fb9eb64ce57009eb64b01c87489b51128f34f`。
没有编译、产品代码修改或已接受性能表更新。#402/#406/#414/#416 保持原状。

**实验口径。** 原实验目录为 `/mnt/brewfs-data/home/hxy/smallfile-warm/`，
新增测量脚本位于其 `ttl-20261104/` 子目录。复用 `compose.sh`、原
`run_perf_in_container.sh` scanner、packed fixture 和 `post-scan.py` 校验。
A/B/C 直接调用原 scanner；只用 `docker compose run -e BREWFS_CACHE_TTL_MS=...`
改变挂载环境。该变量在本 packed 路径同时控制 attr/entry TTL；每轮都从实际
mount 进程环境和 `.stats` 的 `brewfs_fuse_{attr,entry}_valid_ns` 核实生效。

固定项为 8,192 × 4 KiB、一个目录、字典序单读者、wire005、147 个完整 inline
group、raw codec、metadata cache 256 MiB、payload RAM/SSD=0、prefetch=off、
FUSE read direct=1、keep-cache=0、main Tokio transport、request trace sample=307。
客户端运行于 Linux `6.8.0-139-generic`。每次新容器、新 mount，先执行原
`packed-tree` 预热元数据，再计时完整扫描；对象服务器缓存不清空。
没有将完整数据扫描作为预热，也没有改用候选二进制。

D 按任务要求使用纯 `os.open(O_RDONLY|O_CLOEXEC)`、`os.read` 直到 EOF、
`os.close`；由原 scanner 定向替换 stat/buffered I/O 得到，保留目录遍历、
文件数、4 KiB 长度、checksum 以及计时后的全内容/SHA-256 校验。
D 每文件仍读取数据和 EOF 共两次。D 是负载变体，不能把 B→D 的差额称为 TTL 收益。

正式测量在 **09:33:08–09:37:37 Asia/Shanghai**（01:33:08–01:37:37 UTC）完成。
三个 block 的顺序预先固定为 **A/B/C/D → D/C/B/A → B/A/D/C**；共 12 个独立挂载，
没有并发 benchmark 或编译。额外 HTTP/内核快照在 scanner 自身计时外进行，
保留原 Compose stats sampler。计时后等 50ms 收集异步 RELEASE，与 #416
`attribute.py` 的计数方式相同；每轮最终 RELEASE 数均覆盖全部 8,192 文件。

| 组 | attr/entry TTL | scanner | 第 1 轮 ms/文件 | 第 2 轮 | 第 3 轮 | 中位数 | 均值 | 均值 95% CI |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| A | 0 / 0 | 原 stat + buffered open/read/close | 1.599197 | 1.707285 | 1.665692 | **1.665692** | 1.657391 | [1.521957, 1.792825] |
| B | 1s / 1s | 同 A | 0.828756 | 0.871178 | 0.904806 | **0.871178** | 0.868247 | [0.773576, 0.962917] |
| C | 10s / 10s | 同 A | 0.842100 | 0.853342 | 0.880237 | **0.853342** | 0.858560 | [0.809878, 0.907241] |
| D | 1s / 1s | 纯 open/read/close | 0.745689 | 0.733401 | 0.722349 | **0.733401** | 0.733813 | [0.704810, 0.762816] |

每个样本是一次全扫 wall time / 8,192，统计单位是 mount/run，不能把 24,576 个
文件当作独立轮次。中位数为描述统计；95% CI 为三轮均值的 Student t 区间，
df=2、临界值 4.302652730，依赖轮次误差近似正态的假设。仅三轮不能提供可靠的
分布无关中位数 95% CI；连 [min,max] 对总体中位数也只有 75% 覆盖率。
本表明确区分两者，不把该 t 区间冠以“中位数 CI”。

| 对照（按同 block 配对） | 平均节省 ms/文件 | 节省量 95% CI | 平均配对降幅 | 降幅 95% CI |
| --- | ---: | --- | ---: | --- |
| A→B：仅 TTL 0→1s | **0.789145** | **[0.687419, 0.890870]** | **47.61%** | **[43.34%, 51.88%]** |
| A→C：仅 TTL 0→10s | 0.798832 | [0.675149, 0.922514] | 48.17% | [44.19%, 52.15%] |
| B→C：1s→10s | 0.009687 | [−0.040561, 0.059935] | 1.05% | [−4.73%, 6.84%] |
| B→D：同 TTL，移除 stat/buffered I/O | 0.134434 | [0.010775, 0.258092] | 15.33% | [2.69%, 27.97%] |

**请求数。** 下表为正式扫描 HTTP 前后计数差 / 8,192 的三轮中位数，包含原 runner
的少量 `.stats` 轮询和目录遍历，未经手工扣减。全部原始整数计数在
[measurements.tsv](evidence/smallfile-ttl-20261104/measurements.tsv)，前后快照在
[counter-snapshots.json](evidence/smallfile-ttl-20261104/counter-snapshots.json)。
FUSE 没有单独的 CLOSE opcode；一次用户态 close 对应 FLUSH 和 RELEASE。

| FUSE 请求/文件 | A：0 | B：1s | C：10s | D：1s raw |
| --- | ---: | ---: | ---: | ---: |
| GETATTR | **6.003174** | **0.001953** | **0.001099** | **0.001465** |
| LOOKUP | 2.000977 | 0.997681 | 0.997681 | 0.997559 |
| OPEN | 1.000977 | 1.000488 | 1.000488 | 1.000366 |
| READ（含 EOF） | 2.002930 | 2.001465 | 2.001465 | 2.001099 |
| FLUSH | 1.000977 | 1.000488 | 1.000488 | 1.000366 |
| RELEASE | 1.000977 | 1.000488 | 1.000488 | 1.000366 |
| IOCTL | 1 | 1 | 1 | 0 |
| 目录操作合计 | 0.008667 | 0.008667 | 0.008667 | 0.008667 |
| 全部请求 | **14.018677** | **7.011230** | **7.010376** | **6.009888** |

目录操作每扫均为 OPENDIR=1、READDIR=68、READDIRPLUS=1、RELEASEDIR=1。
正 TTL 的 LOOKUP 略少于 1/文件，与 READDIRPLUS 为部分目录项提供属性/entry
缓存相符。A 每轮原始 GETATTR=49,178，B=15/16/16，C=9/9/9，D=13/12/12。
B→C 每扫只再少 6–7 个 GETATTR，已经没有“6 次/文件”的往返负担。
这里的 GETATTR 降幅是 FUSE 往返避免量，不能当作内核 icache 命中率。

**GETATTR 的来源。** 正式计时后，在独立 mount 上关闭轮询干扰，用 HTTP
计数逐阶段夹取，并用 sample=1 的 request trace 识别 inode。另一次 strace
记录实际 syscalls；原始诊断与 syscall 摘录分别见
[diagnostics.json](evidence/smallfile-ttl-20261104/diagnostics.json) 和
[scanner-syscalls.txt](evidence/smallfile-ttl-20261104/scanner-syscalls.txt)。
选择 `f004000`，避开初始 READDIRPLUS 填充的目录项；inode 1 是根目录，
inode 4002 是该文件。干净诊断重现原来的 14 请求/文件：

| scanner 阶段 / 实际 syscall | TTL=0 的 GETATTR | 其中内核路径/打开重验证 | 其中 stat 类调用的目标属性查询 | 同阶段其他 FUSE 请求 |
| --- | ---: | ---: | ---: | --- |
| `path.stat()` → `newfstatat(path)` | 2（root + file） | 1（root） | 1（显式 stat 的 file） | LOOKUP 1 |
| buffered open 内的 `openat(path)` | 2（root + file） | 2 | 0 | LOOKUP 1 + OPEN 1 |
| buffered open 内的 `fstat(fd)` | 1（file） | 0 | 1（库隐式） | — |
| buffered open 内的 `ioctl(TCGETS)` | 0 | 0 | 0 | IOCTL 1，返回 EOPNOTSUPP |
| read-all 内的 `fstat(fd)` | 1（file） | 0 | 1（库隐式） | — |
| 数据 read + EOF read | 0 | 0 | 0 | READ 2 |
| `close(fd)` | 0 | 0 | 0 | FLUSH 1 + RELEASE 1 |
| **合计** | **6** | **3** | **3 = 显式 1 + 隐式 2** | **其余 8 次** |

因此，按“直接触发机制”是 3/3；按“整个显式 path.stat 阶段是否删除”则一次
显式 stat 共触发 **2 GETATTR + 1 LOOKUP**，其中包含内核为了路径处理而查询根属性。
这两个口径不能混用。只去掉显式 `path.stat()`、仍用 Python buffered open/read 时，
TTL=0 的 GETATTR 是 **4**、总请求 **11**；纯 raw 模式是 GETATTR **2**、总请求 **8**。

在 TTL=1s/10s 的独立诊断中，原 scanner 的 GETATTR 为 **0**、总请求 **7**；
删除显式 stat 但保留 buffered I/O 仍为 **7**，因为首次 LOOKUP 从 stat 移到了 open。
纯 raw 模式为 **6**：LOOKUP 1 + OPEN 1 + READ 2 + FLUSH 1 + RELEASE 1。
所以 B→D 的收益包括去掉库的 IOCTL、两个 fstat 的本地 syscall 成本和 Python I/O
包装成本，不能全部归因于删除显式 stat。两个 `lseek(SEEK_CUR)` 不产生 FUSE LSEEK。

诊断还发现 strace 的 `-yy/-P` 和 IOCTL 默认解码会引起额外的路径查询；这些初次
诊断产物已保留并排除。最终用 `-e raw=ioctl`，其逐阶段 FUSE delta 与完全无 strace
的诊断逐项一致。所有 strace/逐阶段诊断耗时都不进入正式延迟表。

**配置与剩余差距。** 以任务给定的 native **0.27 ms/文件**作参考，放宽配置后的
中位数差距如下。native 没有在本轮重测，故这些是对历史参考值的算术差额/倍率，
不是同机同轮 native 对照。`1.678 / 0.27 = 6.21×`，本轮 A 为 6.17×；不能把该组
数字写成“12×”，也不能把其他历史基线的倍率套到本轮。

| 组 | 中位数 ms/文件 | 减去 native 0.27 后 | 相对 native |
| --- | ---: | ---: | ---: |
| A | 1.665692 | 1.395692 ms | 6.17× |
| B | **0.871178** | **0.601178 ms** | **3.23×** |
| C | **0.853342** | **0.583342 ms** | **3.16×** |
| D | 0.733401 | 0.463401 ms | 2.72× |

用三轮**均值**保持可加性，有：

```text
A − native = 1.387391 ms/文件
           = 0.789145（A−B，TTL 配置）
           + 0.134434（B−D，scanner/库调用结构）
           + 0.463813（D−native，未进一步分离的剩余项）
```

对这个参考差额而言，**56.88%** 可由仅改变 TTL 的配对实验解释，**9.69%** 对应
raw scanner 变体进一步减少的开销，**33.43%** 仍未分离。在已经使用 1s TTL 后的
`B−native=0.598247 ms` 差额里，B→D 占 **22.47%**，其余 **77.53%** 留在 raw 路径。
这些比例是均值差额的点估计，不是各占比的置信区间。

请求数证据给出更确定的结构结论：放宽 TTL 消除约 **7/14=50%** 的逐文件请求
（6 GETATTR + 1 LOOKUP）；其余约 7 次中，Python IOCTL 可去掉 1 次，剩 6 次。
TTL=1s→10s 没有改变这个逐文件结构。**剩余 0.463813 ms 不能全部宣称是“请求数
结构”或“实现缺陷”**：它还混合六次请求的服务/调度/往返、对象路径、scanner
以及与历史 native 环境的差异。当前四组足以量化 TTL 的影响，不能独立识别
六次请求各自的 wall-time 因果份额。

服务 handler duration 累计均值为 A/B/C/D = **0.265210 / 0.184407 / 0.180493 /
0.177508 ms/文件**。这支持剩余服务执行时间仍明显小于扫描 wall time；不过
RELEASE 等请求可能重叠，handler sums/trace durations 不构成互斥 wall 分区，
不将“wall 减 handler”直接命名为精确的内核往返时间。

**内核缓存观测。** 12 次运行均保存 `/proc/sys/fs/dentry-state`、`inode-state`、
`inode-nr` 前后快照，见
[kernel-snapshots.json](evidence/smallfile-ttl-20261104/kernel-snapshots.json)。
这些接口报告全主机对象数量/空闲数量，**不报告 per-mount dcache/icache
hit/miss**；容器的 `/sys/fs/fuse/connections` 也未枚举到连接节点，因此没有
取得命中率或该接口的 waiting 计数。明确记为不可得，不从对象数量变化推导命中率。
GETATTR 近乎消失、LOOKUP 减半和实际 reply TTL 是本次缓存生效的直接可观测证据。

**生产建议。** 对本次只读、固定 manifest 的 packed 挂载，采用
`BREWFS_CACHE_TTL_MS=1000`（attr/entry 各 1s）作为合理起点；这已经是该 legacy
策略在不显式覆盖时的默认值。TTL=0 适合需要立即重验证的诊断/一致性契约，
不宜用其小文件扫描结果描述常见缓存配置的性能。10s 在本实验没有可分辨的额外
收益，可仅在明确容许更长可见性延迟的只读/不可变或读多写少场景评估。

正 TTL 允许内核在有效期内复用属性和正目录项，远端的大小、时间、权限、
重命名/删除等变化可能延后被观察；1s 与 10s 的代价相应不同，其他缓存仍可能
影响全链路可见性。固定 manifest 本来不追踪后续发布，本轮也没有验证并发写、
权限撤销或跨客户端失效，因此不据此承诺通用的 close-to-open 一致性。
具有可写 ACL 的挂载使用单独的 `--fuse-cache-ttl-ms` 和 generation/authority
fence，默认零；不要把此环境变量在 packed 路径的结论推广为更改所有后端默认值。
相关实现依据是 `src/fuse/mod.rs` 的 `fuse_cache_ttl`、`permission_cache_ttl` 和
`fenced_metadata_read`，本分支没有更改这些策略。

**有效性与交付。** 12/12 正式扫描和完整内容校验通过，所有轮次 8,192 文件的
SHA-256 映射一致、errors=0、32 MiB 完整读出。每轮测量窗口对象 GET 都是 **9 次 /
2,159,520 B**，metadata RPC=0；TTL 收益没有通过改变对象 I/O/数据缓存换取。
实际 TTL gauges 分别为 0、1,000,000,000 和 10,000,000,000 ns。无 invalid 标记或
残留 perf 容器，独立诊断计数与最终 syscall trace 一致。未用 GitHub Actions。

这是文档与测量任务，不接受产品优化，不运行 cargo/debug 构建；本地验证覆盖
外部脚本语法、12 轮 artifact 完整性、配置/二进制身份、内容一致性、计数和统计
重算、文档 diff。仓库中的原有未跟踪文件保持不动。完整原始 artifacts 仍在
`/mnt/brewfs-data/home/hxy/smallfile-warm/artifacts/ttl-20261104-*`；紧凑证据及
[脚本档案](evidence/smallfile-ttl-20261104/reproduce.md) 随文档提交，索引与哈希见
[证据说明](evidence/smallfile-ttl-20261104/README.md)。
