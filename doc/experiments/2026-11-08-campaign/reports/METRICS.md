# 📈 性能指标台账（METRICS）

> 所有实验的**权威数字**都记在这里；DASHBOARD 只做概览。
> 每轮巡检（10 分钟/30 分钟）如发现新数字，**必须先写本文件再改看板**。
> 最后更新：**2026-11-07 17:35 CST**


> ## ⚠️ 2026-11-08 19:00 重大事故：实验机被释放，原始证据丢失
>
> `ax-c1-a-client` / `ax-c1-a-meta` / `ax-c1-a-obj` **三台实例及其数据盘已被释放**（`DescribeInstances` 只剩 jump，
> `DescribeDisks` 只剩 jump 的系统盘；无对应快照可恢复）。原因尚未查清 —— 我没有对这三台执行过 `DeleteInstance`，
> 最近的操作只有 Stop/Start；不排除账号余额/欠费自动释放或其它外部操作。
>
> **影响**：本文件中引用的以下原始路径**已不存在**：
> - `/srv/ax-data/c1fio/`（C1 72 个 cell 的 fio JSON 与 c1.tsv）
> - `/srv/ax-data/c2b2/`（C2 performance 模式 result.json 与 replay.out）
> - `/srv/ax-data/c3/`（C3 1M 文件 verify/pilot 证据，5.7 GB）
> - `/srv/ax-data/c4ws/`、`/srv/ax-data/redistest/`、`/srv/ax-data/traces/`
> - RustFS 对象桶（c3-base / c2-base 等）随 a-obj 一并消失
>
> **仍成立的部分**：本文件记录的所有**数字与结论**（C1 矩阵、C2 680.06 s、C3 verify/pilot、Redis 183 文件/s 等）
> 均来自当时的实测输出，已在本文与 DECISION_LOG 中留档；但**无法再从原始文件复查**。
>
> **教训**：实验数据必须实时同步到与实验机独立的存储（如 brewfs 包年机或本地），不能只留在按量付费机器的数据盘上。


## 记录规范（每个 cell 必须完整）

```
必填：wall_seconds、ops/文件数、read_bytes、write_bytes、错误数、RSS 峰值、CPU 均值
可选：p50/p95/p99（fio 与 replay 都带）、S3 GET/PUT 次数与字节、cache 命中/未命中、
      重挂次数、env（cache/budget/worker/seed）、提交哈希、命令原文
禁止：只写「很快/正常」；禁止把预热算进随机读分母；禁止跨配置比较
```

---

### ⚠️ 20:15 事件：对象盘再次写满（100%），C1 侧全部重建

```
现象：brewfs 写报 2,174 MiB/s（0.4 s）—— 假值；读报 0；juicefs 掉速
真因：/dev/vdb 492G 全部用满（c1b2 + c1b3 两个 C1 桶 + JuiceFS TrashDays 遗留）
处置：删除 c1b2-20261107 / c1b3-20261107 两个临时桶 → **44G 已用 / 448G 可用（9%）**
证据完好：
  ✅ C3 全部证据在 a-client 本地盘（/srv/ax-data/c3，5.7 GB，segverify10 / pilot / AGENT-REPORT 全在）
  ✅ 对象桶 c3-base（1M 文件数据）/ c2-base / c2b 均保留
教训：C1 每格 8 GiB × 多轮会把 492G 写满；BrewFS 侧必须「优雅 umount」（pkill -9 会丢未落盘数据，
      表现为写极快 + 读 EIO）
```

## ⚠️ C1 数据有效性警告（2026-11-08 17:10 追加）

**下面这组 C1 数字来自「配置不对称」的挂载，其中 randrw 与 coldread 两组不可用于横向对比：**

```
JuiceFS 挂载：juicefs mount -d --cache-size 0      ← 本地缓存被完全关闭
BrewFS  挂载：默认配置（chunk cache + write-back 开启）
```

证据（同一 cell 两边读写字节完全相同，都是读 707 MiB + 写 317 MiB）：

| randrw 1M | 耗时 | p50 | iops |
|---|---:|---:|---:|
| JuiceFS（cache=0） | 29–31 s | 375 ms | 38 |
| BrewFS（有 cache） | 6.8–7.5 s | 30 ms | 285 |

⇒ JuiceFS 在 70/30 混合读写里每次读都打到对象存储，p50 375 ms；这不是文件系统差异，是**缓存被关掉**的结果。
⇒ `coldread` 组同样受影响（JuiceFS 2 GB/s 是顺序直读对象存储，BrewFS 300 MiB/s 含分块 GET 开销）。

**仍然可用的部分**：`write` / `randwrite` 两组（只写不读，缓存策略对写路径影响小，但 `create-new` 语义仍混入 inode 创建与 fsync 成本）。

**另外两个设计缺陷**
```
① 每 cell 仅 1 GiB：bs=64M 时只有 16 个 IO ⇒ p95=p99=最大值，分位数无统计意义
② 每 cell 结束即 unlink：把删除成本混进带宽；且 randrw 的总带宽我把读写两方向相加，应分方向报
```

**C1-v2 重测协议（下次按此跑）**
```
· 缓存对称：JuiceFS 默认缓存（--cache-size 102400），BrewFS 默认；
  冷读改为「两侧 drop_caches + 重启 RustFS」，不靠关缓存
· cell 放大到 4 GiB（64M bs → 64 个 IO，分位数可用）
· rep1 = create-new（单独报），rep2/3 = overwrite（覆盖写）
· randrw 分方向报 read/write 带宽与各自 p50
· 增加 numjobs=8 / iodepth=32 的饱和吞吐组；单队列组只用于延迟
```

---

## C1 — fio 性能矩阵（2026-11-08 重做，v5 最终协议）

**协议**：两个全新卷（JuiceFS `c1fio-20261108` / BrewFS `c1fio-20261108-brew`）；
每 cell 1 GiB、独立新文件（create-new）；`--direct=1 --ioengine=libaio --iodepth=16 --numjobs=1`；
`--percentile_list=50:95:99`（冒号分隔，fio 3.36 只认这种）；3 轮、两系统交替；
冷读 = 预热写 1G → **重启 RustFS** + drop_caches → 读同一文件；每 cell 前检查对象盘 >80 GB。

**结果（3 轮中位数；bw 单位 MiB/s，延迟单位 ms）**

| op | bs | JuiceFS bw | BrewFS bw | J p50 | B p50 | J p95 | B p95 | J p99 | B p99 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| write | 1M | 515.6 | 414.2 | 25.8 | **6.5** | 37.5 | 39.6 | 47.4 | 42.2 |
| write | 4M | 514.6 | 426.3 | 105.4 | **28.7** | 127.4 | 145.8 | 135.3 | 154.1 |
| write | 64M | 508.2 | 431.3 | **885.0** | 960.5 | 1451.2 | 1182.8 | 1451.2 | 1182.8 |
| randwrite | 1M | 231.6 | 113.0 | 57.4 | **13.0** | 99.1 | 80.2 | 119.0 | 109.6 |
| randwrite | 4M | 505.7 | 387.4 | 107.5 | **39.6** | 127.4 | 141.6 | 137.4 | 143.7 |
| randwrite | 64M | 512.8 | 413.2 | **843.1** | 1019.2 | 1417.7 | 1233.1 | 1417.7 | 1233.1 |
| randrw | 1M | 37.7 | **322.3** | 375.4 | **30.0** | 683.7 | **166.7** | 767.6 | **235.9** |
| randrw | 4M | 129.6 | **326.5** | 375.4 | **121.1** | 717.2 | **455.1** | 809.5 | **501.2** |
| randrw | 64M | **826.5** | 314.5 | **633.3** | 1350.6 | **1149.2** | 2801.8 | **1149.2** | 2801.8 |
| coldread | 1M | **2043.9** | 302.4 | **5.7** | 33.8 | **7.5** | 175.1 | **10.3** | 221.2 |
| coldread | 4M | **1988.3** | 306.3 | **23.2** | 135.3 | **27.9** | 459.3 | **35.9** | 509.6 |
| coldread | 64M | **1638.4** | 296.9 | **242.2** | 1384.1 | **518.0** | 3271.6 | **518.0** | 3271.6 |

**结论**
```
· write / randwrite：JuiceFS 吞吐高 15–105%（515 vs 414、232 vs 113 MiB/s），但 BrewFS p50 低 4×（6.5 vs 25.8 ms）
· randrw（混合读写）：BrewFS 全面占优 —— 吞吐 8.5×（322 vs 37.7 MiB/s），p50 12×（30 vs 375 ms）
· 冷读：JuiceFS 6.7× 快（2044 vs 302 MiB/s），p50 低 6×（5.7 vs 33.8 ms）⇒ BrewFS 冷读是明显短板
· 72 cell 全部完成，错误数 0；磁盘护栏（<80 GB 即 ABORT）未触发
```

**踩坑记录（可复现要点）**
```
① 首轮 write 偏低是「新建文件 vs 覆盖写」混淆：A/B 实测 JuiceFS 168→386、BrewFS 257→383 MiB/s
   ⇒ 必须每 cell 独立新文件
② fio 3.36 的 --percentile_list 只认冒号：50,95,99 只出 50th；50:95:99 才出三个
③ 冷读重启 RustFS 必须用 pkill -x rustfs（用 pkill -f rustfs 会把 ssh 命令自身杀掉，
   导致 RustFS 起不来、18 个冷读 cell 全 EIO）
```

**可复现**：`/srv/ax-data/c1fio/{c1.tsv,summary.md,*.json}`；脚本 `/root/c1fio4.py` + `/root/c1cold.py`

---


### C3 pilot 3 次重复：尝试失败（环境不稳）

```
尝试 1（10 万文件 @ flat BrewFS，bucket c1fio-20261108-brew）：
  生成阶段 python 报 ConnectionAbortedError: [Errno 103] Software caused connection abort
  只建成 1,000 文件（1 个目录）后中断；mount 仍在（procs=2），但批量写入被中断
  原因：BrewFS flat 挂载在「连续小文件创建 + 每个 create 打 INFO 日志（mount.log 暴涨）」下不稳
尝试 2（1 万文件 × 3 轮，RUST_LOG=warn，逐目录重试）：
  脚本刚起（mount=y）后 a-client 的 SSH 被远端关闭，实例随后变为 Stopping/Stopped
  （本轮还发现：先前一次 StopInstance 的回显不代表真的停了，必须回读 Status 校验）
⇒ C3 pilot 方差（3 次重复）**仍未取得**；已有数据仍是最初的「1 次热态 8.9613 s + 1 次冷态 23.4057 s」
```

## C4 — fork / 派生成本（overlay fork vs juicefs clone）

> 同一台 a-client；BrewFS 侧 = `workspace fork`（overlay 层，O(1)）；JuiceFS 侧 = `juicefs clone`（元数据 CoW，O(N)）。

| 系统 | 操作 | 规模 | 耗时 | 每单位 | 复杂度 |
|---|---|---|---:|---:|---|
| **BrewFS** | `workspace fork --count 1` | base 1,026 文件 | **62 ms** | 62 ms | **O(1)** |
| **BrewFS** | `workspace fork --count 1`（第二次） | 同上 | **84 ms** | 84 ms | O(1) |
| **BrewFS** | **fork → 挂载可用（端到端）** | 同上 | **305 ms**（fork 84 + mount 221） | — | O(1) |
| **BrewFS** | fork 后首次 readdir | 空 base | **14 ms** | — | — |
| **BrewFS** | `workspace fork --count 10` | base 1,026 文件 | **443 ms** | 44.3 ms/fork | O(1) |
| **BrewFS** | `workspace fork --count 100` | base 1,026 文件 | **4,146 ms** | **41.5 ms/fork** | O(1) |
| BrewFS | `workspace create`（单） | — | 72 ms（前一次） | — | O(1) |
| BrewFS | umount（seal） | — | **4 ms** | — | — |
| JuiceFS | `juicefs clone` | 97,600 文件 | **20,020 ms** | 205 ms/千文件 | **O(N)** |

**结论：**
```
· BrewFS fork 是 O(1)：与 base 文件数无关，单次 41–72 ms（含 CLI 启动 ~40 ms 固定开销）
· 批量 fork 摊销 41.5 ms/个（count=100）；若复用进程（daemon 模式）可更低
· JuiceFS clone 是 O(N)：97,600 文件 20.02 s ⇒ 1M 文件外推 ≈205 s
· 对比：同一「派生一个独立工作区」的语义，BrewFS 比 JuiceFS 快 **≈320×**（62 ms vs 20,020 ms），
  且 base 越大差距越大（BrewFS 恒定，JuiceFS 线性增长）
```

**限制（必须写进论文）：**
```
· workspace 卷内写小文件只有 **8 文件/s**（普通 flat 挂载 ~450 文件/s），
  因此「建 10 万文件 base」需要 ≈3.5 h；本次 base 实际只有 1,026 文件。
  但 fork 是 O(1)，base 大小不影响 fork 结论（这正是要证明的点）。
· fork 后的读路径未测（读取共享 lower 的带宽/延迟）
· **可见性验证未完成**：写入 live workspace 的 1,026 个文件没有被 seal 成新 revision（只找到 head_epoch/head_layer），
  fork --from v1 得到的是「空 base 的 overlay」⇒ 挂载 fork 后看到 0 文件。需要先找到 seal 触发方式（mount 的 checkpoint / operator），
  才能验证「fork 能看到 base 文件 + 写入隔离」。这是下一轮要补的第一件事。

---


## C1-v2 —— 对称缓存重测（2026-11-08 18:00–18:45，新机器 b-client/b-meta/b-obj）

**修掉的配置不对称**：v1 里 JuiceFS 用 `--cache-size 0`、BrewFS 用默认缓存 ⇒ randrw/coldread 作废。
v2 两侧都开默认缓存（JuiceFS `--cache-size 102400`，BrewFS 默认），其余协议一致：

```
cell 4 GiB；bs ∈ {1M,4M,64M}；jedepth=16 numjobs=1（单队列组）
op  ∈ write / randwrite / randrw(70% read) / coldread
rep1 = create-new（新文件）；rep2/3 = overwrite（复用同名文件）
冷读 = prep 写 4G → 重启 RustFS（pkill -x rustfs，带凭据）→ drop_caches → **重挂载** → 读同一文件
每 cell 前检查对象盘 >80 GB；percentile_list=50:95:99（冒号）
机器：client 16c64g / meta 8c32g(PD+TiKV) / obj 16c64g(RustFS)；盘 100/100/300 GiB
```

**结果（3 次中位数，bw = MiB/s，p50 = ms）**

| op | bs | JuiceFS bw | BrewFS bw | JuiceFS p50 | BrewFS p50 |
|---|---|---:|---:|---:|---:|
| write | 1M | 270.3 | **285.5** | — | — |
| write | 4M | 266.2 | **280.7** | 231.7 | **177.2** |
| write | 64M | **245.2** | 235.7 | 3942.7 | **3070.2** |
| randwrite | 1M | 133.2 | **153.8** | — | — |
| randwrite | 4M | 266.2 | **278.8** | 227.5 | **156.2** |
| randwrite | 64M | 1.5 | **159.1** | 17112.8 | **6476.0** |
| randrw | 1M | 30.3 | **959.7** | — | — |
| randrw | 4M | 119.8 | **610.3** | 388.0 | **91.8** |
| coldread | 1M | **2314.1** | 450.4 | **6.19** | 34.34 |
| coldread | 4M | **2266.8** | 456.0 | **25.17** | 132.91 |
| coldread | 64M | **2078.9** | 446.8 | **400.56** | 2126.51 |

**读法**
- 写类：两侧接近（差 −4% ~ +6%）；BrewFS 的 p50 在 4M/64M 上低 22–56%
- 随机写 64M：JuiceFS 崩到 **1.5 MiB/s**（p50 17.1 s/IO），BrewFS 159.1 MiB/s（p50 6.5 s）⇒ **106×**
- 混合读写：BrewFS 全面大幅领先（1M ×31.7、4M ×5.1）
- 冷读：JuiceFS 快 **4.7–5.1×**，p50 低 5.5×（与 v1 结论一致，v1 的 2043.9 vs 302.4 仍成立）
- 1M 的 p50 显示为 — 是因为 fio 在该模式下 clat 只测提交（4 GiB 的 fsync 成本计入 wall），不可用于延迟比较

**未取到的 cell（NOT MEASURED）**
```
randrw 64M（JuiceFS 端极慢，超时放弃）
write_sat 1M / 64M（4M 饱和组已测：两侧都 ≈33 MiB/s，因为 8 jobs × 4 GiB = 32 GiB 的 fsync 主导，不代表吞吐上限，需用 size=512M/job 重测）
coldread rep3（只做了 2 轮）
```

**原始数据**：`/srv/ax-data/c1v2/c1v2.tsv`（58 行）+ 每 cell 的 fio JSON（新机器 ax-c1-b-client 上）

## C2 — AI trace 回放（JuiceFS 6/6 PASS；BrewFS 受阻于 xattr）

| trace | 系统 | ops / callbacks | 字节 | 耗时 | errno 差异 | 结果 |
|---|---|---|---:|---:|---|---|
| pytest-full-02 | JuiceFS | 1,747,109 ops | — | 109.7 s | 0 | ✅ |
| django | JuiceFS | 57,787,980 callbacks | — | 2,603.9 s | 0 | ✅ |
| sklearn | JuiceFS | 9,233,326 ops | — | — | 0 | ✅ |
| sympy | JuiceFS | 7,265,338 ops | — | — | 0 | ✅ |
| io-compatible（uid 1000） | JuiceFS | 3,673,814 ops | — | 961.2 s | 0 | ✅ |
| rust（uid 1000） | JuiceFS | 3,122,195 callbacks | 读 4,391,117,721 B / 写 1,178,081,436 B | 46.05 s | 0 | ✅ |
| **pytest-full-02** | **BrewFS（flat/TiKV，performance 模式）** | **1,747,109 ops** | 读 281,774,624 B / 写 194,706,661 B | **680.06 s** | **23,929 次 errno 差异（全部 xattr/ioctl 路径）** | ✅ **完成** |


### C2 的 BrewFS 侧：如何绕开 xattr 阻塞（2026-11-08 突破）

```
strict 模式永远过不去：第 203 事件 getxattr 期望 ENODATA(61)、BrewFS 返回 ENOTSUPP(95) → 直接中止
正解 = replay 工具自带的 `--performance` 模式：
  src/main.rs:398 明确对 getxattr/listxattr/ioctl 的 errno 差异容错并累加 probe_errno_differences
  ⇒ trace 不修改、事件不丢弃，完整跑完 1,747,109 个回调

pytest-full-02 on BrewFS（performance 模式）：
  operations            1,747,109（与 JuiceFS 完全一致）
  active_replay_seconds 680.06 s   （JuiceFS strict 模式 109.7 s ⇒ BrewFS 慢 6.2×）
  actual_read_bytes     281,774,624
  actual_write_bytes    194,706,661
  expected_errors       45,525
  probe_errno_differences 23,929   （22,881 个 xattr 事件 + ioctl 差异，数量级吻合）
  remaining_handles     0          （无 fd 泄漏）
  completed             true
```

**意义**：C2 的 BrewFS 数据不再是空白 —— 同一 trace、同一初始树、同一台机器，BrewFS 完成时间 680 s vs JuiceFS 109.7 s（**6.2×**），
且首次量化了 xattr 缺失的语义偏差规模（23,929 次）。

**BrewFS 失败根因（已定位到具体事件）**
```
事件 203: getxattr("/state/data/opencode/opencode.db-journal", "security.capability")
  期望：errno=61 (ENODATA，属性不存在)
  实得：errno=95 (EOPNOTSUPP)
原因：src/meta/stores/tikv/mod.rs 的 xattr 能力为 false，四个方法走默认实现 → NotImplemented
影响面：pytest trace 含 22,881 行 xattr 事件、django 42,091 行、rust 693,861 行
      ⇒ BrewFS flat 卷无法回放任何含 xattr 的 trace
修复：agent `gxattr2`（brewfs 包年机）正在给 TiKvMetaStore 实现真 xattr（含 ENODATA 语义）
```

---


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


### C2 django 在 BrewFS 上：未实测，按 pytest 速率外推

```
django trace 有 57,787,980 个事件（pytest 只有 1,747,109，是它的 33×）。
BrewFS 实测速率（pytest）= 1,747,109 / 680.06 s ≈ 2,570 events/s
⇒ django 在 BrewFS 上全量外推 ≈ 57,787,980 / 2,570 ≈ 22,485 s ≈ **6.2 小时**
限时 30 分钟的采样在启动阶段（<65,536 事件，未打印第一条进度）就耗尽了预算，
因此 django on BrewFS **记为 NOT MEASURED**，只报外推值，不写进对比表。
（JuiceFS 侧 django 实测 2,603.9 s / 57,787,980 callbacks 是既有数据。）
```


### C4 seal：代码已写，编译/验证未完成

```
agent gseal（brewfs 包年机，¥0）已完成代码改动：14 个文件
  src/cli.rs / src/config.rs / src/workspace_overlay/{catalog,mod}.rs
  src/workspace_overlay/stores/{database,kv_store}.rs + 测试文件
  目标命令：brewfs workspace seal <WORKSPACE_ID> → 输出可被 fork 接受的 revision

阻塞：
  ① 该 agent 跑 `cargo test --workspace` 把 30 GB 内存的 brewfs 拖死，SSH 全部被拒
     ⇒ RebootInstance 一次（代码保留、agent 上下文丢失）
  ② 重启后手动重编 release：brewfs 主 crate 在 30 GB 机器上跑 ~50 分钟仍未产出，
     且内存 19/30 GB、磁盘一度只剩 14 GB（已清 14 GB 旧 target 缓解）
  ③ 因主 crate 长时间无进展，已 kill；seal 代码仍在磁盘（14 个改动文件），但**没有可用二进制**
⇒ C4 的「seal → fork 可见性 → 写隔离」闭环**仍未验证**；下一步需要一个能编出二进制且内存充足的机器
   （a-client 61 GB 更合适，但需 2–3 小时编译预算）
```


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

## C3 — 百万小文件

### ⚠️ 20:45–21:00 复测发现（BrewFS pilot 可重复性）

```
① BrewFS pilot 冷挂载：**23.4057 s**（reads 20,224 / writes 768）—— 有效，作为冷态样本
   热态（C3 时刚校验过的 workspace）：8.9613 s
② 想要 3 次重复：第 2/3 次失败（`read mismatch cold-read file 911142`）
   原因：pilot 写 256 COW + 768 次写，树被改；同一 base 连跑必然 mismatch（我的方法错误）
③ 硬链接克隆（cp -al）实测 ≈150 文件/s ⇒ 1M 需 ≈1.9 小时（高负载最坏 11 h），不实用
④ agent 的 build_workspace.sh 重建到 **882–892/1024 目录就早退**，无法稳定复现
⑤ smallfiles 要求 --workspace 非 0 + 3:1 混合操作 ⇒ 不能退化成只读 pilot
⇒ 结论：BrewFS pilot 目前只有「1 冷 + 1 热」两个样本；要做 3 次重复，必须先修好 builder
   （JuiceFS 侧有 clone，秒级，天然可重复 —— 本身就是一条论文差异）
```
### JuiceFS

| 项 | 结果 | 数字 | 证据 |
|---|---|---|---|
| base verify-base | ✅ PASS | files=1,000,000 / bytes=4,096,000,000 / **2,587.57 s** | `/srv/ax-data/c3/verify-base-par-20261107/result.json` |
| pilot run | ✅ PASS | **wall 4.4466 s**（热态；早前一次 13.2788 s） | `/srv/ax-data/c3/pilot-juice-20261107/run.out` |
| verify-touched child | ✅ PASS | 256 files / 0.2311 s | 同目录 `verify.out` |
| verify-touched base | ✅ PASS | 256 files / 0.4052 s | 同目录 `base.out` |

### BrewFS

| 项 | 结果 | 数字 | 证据 |
|---|---|---|---|
| 1M 导入 | ✅ 完成 | 1024 目录 / 1,000,000 文件；`Dropping an active transaction`=**0** | `/srv/ax-data/c3/import-brew-final.csv` |
| 坏文件 | ✅ 已修 | 3 个（d0130/d0132/d0133/d0371），逐文件 SHA-256 复核 2,931 文件全一致 | `/srv/ax-data/c3/badfix-20261107/` |
| 分段 verify（编排者版） | ✅ PASS | 1024 目录 / 0 mismatch / **799.69 s** | `/srv/ax-data/c3/segverify/verify-base-brew-segmented-final.json` |
| 分段 verify（agent 版） | ✅ PASS | files=1,000,000 / bytes=4,096,000,000 / **1,510 s** / 31 次重挂 / **RSS 峰值 1,785 MB** | `/srv/ax-data/c3/segverify10-20261107/` |
| pilot run | ✅ PASS | **wall 8.9613 s** / verified_reads 20,224 / writes 768 / read_bytes 82,837,504 / write_bytes 3,145,728 | `/srv/ax-data/c3/pilot-brew-segverify-20261107/` |
| verify-touched child | ✅ PASS | 256 files / **2.186 s** | 同上 `verify/` |
| verify-touched base | ✅ PASS | 256 files / **2.060 s** | 同上 `base/` |

### C3 对比（论文用）

| 指标 | JuiceFS | BrewFS | 比值 |
|---|---:|---:|---|
| 1M base 校验 | 2,587.57 s（单趟） | 1,510 s（分段/31 重挂） | BrewFS 快 1.71× |
| pilot wall | 4.4466 s（热态） | 8.9613 s | BrewFS 慢 2.02× |
| 校验时 RSS 峰值 | 未单独记录 | 1,785 MB | — |

---


## 补充实验：Redis 元数据后端（2026-11-08）

**环境**：Redis 7.0.15 跑在 a-client 本机（`127.0.0.1:6379`，`--save '' --appendonly no --maxmemory 4gb noeviction`），
BrewFS 挂载命令：
```
brewfs-patched mount --privileged --meta-backend redis --meta-url redis://127.0.0.1:6379/1   --data-backend s3 --s3-bucket redis-20261108 --s3-endpoint http://10.20.0.13:9000   --s3-region us-east-1 --s3-force-path-style true /mnt/bfs-redis
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

## 资源指标（运行中采集）

| 时间 | 机器 | CPU | RSS | 磁盘 |
|---|---|---|---|---|
| 17:27 | a-client | 88% idle（brewfs 72.8%） | 403 MB | 110/492 G |
| 17:27 | a-meta | 84% idle（tikv 16%） | 14.2 GB | — |
| 17:27 | a-obj | 99% idle（rustfs 21%） | 950 MB | — |

---

## 历史（上一轮 C1，探索性，勿与正式结果混用）

```
1M: JuiceFS write 603.1 / randwrite 429.0 / randrw 38.8-13.4 / read 2844.9*（污染）
    BrewFS 644.2 / 591.1 / 381.8-131.5 / 428.5
4M: JuiceFS 599.6 / 598.5 / 158.3-54.5 / 2591.0*；BrewFS 641.1 / 502.6 / 375.7-129.4 / 460.6
64M: JuiceFS 615.8 / — / — / 2991.4*；BrewFS 640.6 / — / — / 406.7
cold read 仅 1M：JuiceFS 1178.7 / BrewFS 780.2 MiB/s
```
