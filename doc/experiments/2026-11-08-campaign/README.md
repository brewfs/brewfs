# BrewFS vs JuiceFS 综合实验档案（2026-11-06 → 2026-11-08）

本目录把这一轮全部实验的**协议、原始数据、汇总数字、踩坑与结论**集中存放，供论文写作与复现使用。
台账真源是 `reports/METRICS.md`；本 README 是它的完整叙述版。

```
目录
  README.md                  本文件（全部进度与数学数据）
  reports/METRICS.md         指标台账（权威数字，含作废声明）
  reports/DECISION_LOG.md    决策日志（每轮做了什么、纠错、踩坑）
  reports/DASHBOARD.md       看板（实验状态、机器与成本）
  reports/C3_RESULTS.md      C3 专项
  data/c1-legacy/            C1 第一版（72 cell；randrw/coldread 已作废，见 §3.2）
  data/c1v2/                 C1 第二版（对称缓存；c1v2.tsv 58 行 + 77 个 fio JSON）
  data/c2-juicefs/           C2 JuiceFS 侧 trace 回放结果
  data/c2-brewfs-attempts/   C2 BrewFS 侧历史失败尝试
  data/c2-rust/              C2 rust trace 报告
  data/c3-results.tar.gz     C3 百万小文件全部结果（2410 个小文件打包）
  data/c4-fork/              C4 fork 计时（fork.tsv、e2e.log）
  data/c4-seal/              C4 seal 端到端证据（meta.db、fork.json、mount 日志、构建日志）
  data/redis/                Redis 元数据后端数据
  patches/xattr.patch        TiKV MetaStore xattr 实现（+426/−56）
  patches/seal.patch         workspace seal 实现（14 文件 + 新增 seal.rs 564 行）
  scripts/                   本次用到的编排脚本（复现入口）
```

---

## 1. 环境

```
y 机型（按量付费，cn-hangzhou）
  ax-c1-jump        2c8g    ¥0.3/h   唯一公网入口（121.196.47.102 / 10.20.0.10）
  ax-c1-a-client   16c64g   ¥2.7/h   客户端（第一代，已释放）
  ax-c1-a-meta      8c32g   ¥1.3/h   PD + TiKV 8.5.3（第一代，已释放）
  ax-c1-a-obj      16c64g   ¥2.7/h   RustFS（第一代，已释放）
  ax-c1-b-*        同规格小盘版     C1-v2 用（100/100/300 GiB，已释放）
  ax-c1-c-build     8c32g    ¥1.3/h   C4 seal 编译/验证（200 GiB，已释放）
  brewfs-singapore-v2        ¥0       包年，代码机（/home/hxy/brewfs-main）
软件
  JuiceFS（format+mount，TiKV 元数据 + S3 数据）、BrewFS（brewfs-patched / workspace 版）
  RustFS 1.0.2（S3 兼容）、TiKV/PD 8.5.3、fio 3.36、full-fuse-replay、smallfiles
```

**磁盘成本教训**：第一代三台各挂 **2048 GiB** ESSD ⇒ 6,144 GiB ≈ ¥6,144/月（≈¥205/天）。
后改为 100/100/300 GiB（500 GiB ≈ ¥500/月），实验结束即归档并 `DeleteDisk`。
注意 `DeleteInstance` 不带走独立数据盘，必须单独检查孤儿盘。

---

## 2. 时间线

```
11-06  环境搭建；C1 第一版开始；fio percentile 踩坑（逗号只出 50th）
11-07  C1 第一版 72 cell 完成；C2 JuiceFS 6/6 PASS；C3 verify/pilot；C4 fork 计时；Redis 10k
11-08  发现 C1 缓存配置不对称 ⇒ 作废 randrw/coldread
       发现 TiKV xattr 缺口（EOPNOTSUPP vs ENODATA）⇒ 写 xattr 补丁
       三台一代机被释放（数据丢失事故，见 §7）
       重建小盘机跑 C1-v2（58 行）；C2 pytest 用 --performance 模式跑通（680.06 s）
       C4：实现 workspace seal，编出二进制（62m37s），seal 8 ms / fork 9 ms，
           但 fork 挂载看不到 base（0 文件）⇒ 已知缺口
       归档全部数据、删除全部实验机 ⇒ 成本回落到 ¥0.3/h
```

---

## 3. C1 —— fio 带宽与延迟

### 3.1 第一版（data/c1-legacy/，96 个 JSON + c1.tsv）

72 cell（3 bs × 6 op × 3 轮 × 2 系统，1 GiB cell，3 轮中位数）：

| op | bs | JuiceFS bw | BrewFS bw | J p50 | B p50 | J p95 | B p95 |
|---|---|---:|---:|---:|---:|---:|---:|
| write | 1M | 515.6 | 414.2 | 25.8 | 6.5 | 37.5 | 39.6 |
| write | 4M | 514.6 | 426.3 | 105.4 | 28.7 | — | — |
| write | 64M | 508.2 | 431.3 | — | — | — | — |
| randwrite | 1M | 231.6 | 113.0 | 57.4 | 13.0 | 99.1 | 80.2 |
| randwrite | 4M | 505.7 | 387.4 | 107.5 | 39.6 | — | — |
| randwrite | 64M | 512.8 | 413.2 | — | — | — | — |
| randrw | 1M | 37.7 | 322.3 | 375.4 | 30.0 | 683.7 | 166.7 |
| randrw | 4M | 129.6 | 326.5 | 375.4 | 121.1 | — | — |
| randrw | 64M | 826.5 | 314.5 | 633.3 | 1350.6 | — | — |
| coldread | 1M | 2043.9 | 302.4 | 5.7 | 33.8 | 7.5 | 175.1 |
| coldread | 4M | 1988.3 | 306.3 | 23.2 | 135.3 | — | — |
| coldread | 64M | 1638.4 | 296.9 | 242.2 | 1384.1 | — | — |

### 3.2 为什么 randrw / coldread 作废

```
JuiceFS 挂载用 --cache-size 0（本地缓存全关），BrewFS 用默认（chunk cache + write-back）
证据：randrw 1M 两边读写字节完全相同（读 707 MiB / 写 317 MiB），
      JuiceFS 29–31 s / p50 375 ms，BrewFS 6.8–7.5 s / p50 30 ms
⇒ 差异来自缓存配置，不是文件系统差异
另：1 GiB cell 在 bs=64M 时只有 16 个 IO ⇒ p95=p99=max，分位数无意义
```

### 3.3 第二版（data/c1v2/，58 行原始数据）

协议：两侧默认缓存（JuiceFS `--cache-size 102400 --cache-dir`，BrewFS 默认）；
cell 4 GiB；`irdepth=16 numjobs=1`；rep1=create-new、rep2/3=overwrite；
randrw 70% 读并分方向报；冷读 = prep 写 4 GiB → `pkill -x rustfs` 重启 → `drop_caches` → **重挂载** → 读；
每 cell 前检查对象盘 >80 GB；`--percentile_list=50:95:99`（必须冒号）。

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

**结论**
- 写类吞吐两侧接近（JuiceFS 比 BrewFS 低 4% ~ 高 6%），BrewFS 的 p50 在 4M/64M 低 22–56%
- **随机写 64M：JuiceFS 1.5 MiB/s（p50 17.1 s/IO）vs BrewFS 159.1 MiB/s（p50 6.5 s）⇒ 106×**
- **混合读写：BrewFS 领先 5–32×**（1M 32×、4M 5.1×）
- **冷读：JuiceFS 快 4.7–5.1×**，p50 低 5.5×
- 1M 的 p50 未列：fio 在 buffered+end_fsync 模式下 clat 只测提交，4 GiB 的 flush 成本落在 wall 里

**未取到（NOT MEASURED）**：randrw 64M（JuiceFS 端超时）、write_sat 1M/64M（4M 饱和组因 8 jobs × 4 GiB 的 fsync 主导，实测两侧都 ≈33 MiB/s，不代表上限）、coldread 第 3 轮。

---

## 4. C2 —— trace 回放（data/c2-*）

**JuiceFS 侧（6/6 PASS）**

| trace | 规模 | 耗时 |
|---|---|---|
| pytest-full-02 | 1,747,109 ops | 109.7 s |
| django | 57,787,980 callbacks | 2,603.9 s |
| sklearn | 9,233,326 ops | — |
| sympy | 7,265,338 ops | — |
| io-compatible | 3,673,814 ops | 961.2 s |
| rust | 3,122,195 callbacks | 46.05 s（读 4,391,117,721 B / 写 1,178,081,436 B） |

**BrewFS 侧**

```
strict 模式过不去：第 203 个事件 getxattr 期望 ENODATA(61)，BrewFS 返回 EOPNOTSUPP(95)
根因：TiKV MetaStore 没有 xattr（patch 见 patches/xattr.patch，+426/−56）

绕开方式（正解）：replay 的 --performance 模式
  源码 src/main.rs:398 对 getxattr/listxattr/ioctl 的 errno 差异容错并计数
  结果（pytest-full-02）：
    operations             1,747,109（与 JuiceFS 完全一致）
    active_replay_seconds  680.06 s   ⇒ BrewFS 慢 6.2×
    actual_read_bytes      281,774,624
    actual_write_bytes     194,706,661
    expected_errors        45,525
    probe_errno_differences 23,929（≈22,881 个 xattr 事件 + ioctl 差异）
    remaining_handles      0
  django：57,787,980 事件，按 2,570 events/s 外推 ≈22,485 s ≈ 6.2 小时 ⇒ 未实测
```

---

## 5. C3 —— 百万小文件（data/c3-results.tar.gz）

```
verify（1,000,000 文件 / 4,096,000,000 B）
  JuiceFS  2,587.57 s（单进程 32 段）
  BrewFS   1,510 s（10 段、31 次重挂、RSS 峰值 1,785 MB、0 mismatch）
  BrewFS   799.69 s（32 段 + sha256 校验）
pilot（1M 树上的元数据/小文件混合负载）
  热态：JuiceFS 4.4466 s  vs  BrewFS 8.9613 s
  冷态：BrewFS 23.4057 s
workspace 卷小文件写入：仅 8 文件/s（100k 需 ≈3.5 h，不适合做 10 万规模写入实验）
未做：pilot 3 次重复（每次需重建 1M workspace，30–40 分钟/次）
```

---

## 6. C4 —— fork / seal（data/c4-fork、data/c4-seal）

### 6.1 fork 成本（无需 seal，空 base）

```
workspace fork --count 1    = 62 ms
workspace fork --count 10   = 443 ms（44.3 ms/个）
workspace fork --count 100  = 4,146 ms（41.5 ms/个，100/100 成功）
umount / seal 阶段           = 4 ms
mount fork                  = 221 ms
端到端 fork → 可挂载可用     = 305 ms（O(1)）
对比 juicefs clone 97,600 文件 = 20,020 ms（O(N)，1M 文件外推 ≈205 s）
⇒ 65–320×
（早期 cp -a 25–27 f/s、cp -al ~150 f/s 的测量用的是错误原语，已作废）
```

### 6.2 seal（本轮新实现）

```
实现：patches/seal.patch（14 个改动文件 + 新增 src/workspace_overlay/seal.rs 564 行）
编译：8c32g 构建机，64 GB swap + -j2 + cgu=256，
      --features fuse-io-uring-runtime,workspace-overlay
      依赖 protobuf-compiler；rust 走阿里云镜像、cargo 走 rsproxy
      注意：git diff 不含未跟踪文件（seal.rs）⇒ 必须单独打包
      结果：BUILD_RC=0，3.3 GB 二进制，62m37s
实测（sqlite 元数据 + local-fs 数据）
  workspace seal <WID>        rc=0，8 ms，输出 revision <layer>:1:<root_hash>
  workspace fork <REV>        rc=0，9 ms
  workspace diff <fork>       []（元数据层面与 base 一致）
  挂 fork 读 base/sealbase    0 文件  ❌
  --volume-format workspace-native-v2 挂 fork：mount 失败
⇒ 结论：seal 与 fork 的「快」已证实（8/9 ms），但 fork 挂载不接 lower，
   「fork 能看到父层数据」这一环仍是产品缺口
```

---

## 7. Redis 元数据后端（data/redis）

```
mount 时间        1 s
10,000 × 4 KiB    54.7 s  ⇒ 183 文件/s（对比 TiKV 420–550 文件/s）
元数据占用        30,359 keys / 7.90 MB
未完成：pilot（smallfiles 报 "os error 2"，与 <root>/small/d#### 布局无关，根因未定位）
```

---

## 8. 事故与教训（写论文/复现必须知道）

```
① 数据丢失事故：第一代三台实验机在 11-08 16:16 被删除（连同数据盘），
   C1/C2/C3/C4/Redis 的原始产物（fio JSON、trace 目录、1M 文件树、RustFS 桶）全部丢失。
   现有数字来自当时输出与事前导出的 11 MB 精简包；本目录的 C1-legacy/C2/C3 数据即来自该包。
   纪律：产物必须在 10 分钟内同步到独立存储（brewfs 包年机或本地）再考虑停机。
② fio 3.36 的 --percentile_list 只认冒号 50:95:99（逗号只出 50th）。
③ 冷读必须 `pkill -x rustfs`；`pkill -f rustfs` 会杀掉 ssh 命令自身，导致 RustFS 起不来、整批 cell 全 EIO。
④ 冷读还必须**重挂载**被测文件系统：RustFS 重启会打断既有 FUSE mount，之后写入报 EIO。
⑤ RustFS 重启必须带 RUSTFS_ACCESS_KEY/SECRET，否则全部请求 InvalidAccessKeyId。
⑥ 对象盘写满会造出全线假数据（曾出现 JuiceFS 14.3 MiB/s、fsync 18 分钟）⇒ 每 cell 前查剩余 >80 GB。
⑦ 编译校验：`strings target/release/brewfs | grep -c 'invalid setxattr flags'` > 0，否则编的是旧源码。
⑧ 改 CARGO_PROFILE_RELEASE_CODEGEN_UNITS 或 LTO 会让 451+ 依赖全量失效重编（>1.5 h）；
   16c64g 上 -j8 + cgu=256 峰值 40–63 GB，会拖死 sshd（曾两次被逼 reboot）。
⑨ replay 的 sequence gap：删事件会破坏 seq 连续性 ⇒ 用 --performance 模式，不要改 trace。
⑩ 单一 TiKV/RustFS 节点上并发跑两个文件系统会互相干扰（曾把 603 MiB/s 压到 14.3 MiB/s）⇒ 一律串行。
⑪ DeleteInstance 不带走独立数据盘，必须单独 DeleteDisk 并每轮扫孤儿盘。
⑫ StopInstance 的回显不代表真的停机，必须回读 Status 校验（曾因此空转 1 小时 ≈¥7.5）。
```

---

## 9. 复现

```
1) 起机器（小盘）：client 16c64g/100 GiB、meta 8c32g/100 GiB、obj 16c64g/300 GiB，都要开公网带宽
2) meta：pd-server + tikv-server；obj：rustfs（带 RUSTFS_ACCESS_KEY/SECRET）
3) client：juicefs format/mount（默认缓存）+ brewfs mount（默认配置）
4) C1-v2：scripts/c1v2-run.sh（写类）、scripts/c1v2-cold5.sh（冷读，含重启+重挂载）
5) C2：scripts/c2-perf.sh（BrewFS performance 模式）、juicefs 侧用 strict 模式
6) C3：scripts/c3-reps2.sh（注意 workspace 卷写小文件只有 8 文件/s）
7) C4：scripts/seal-e2e-build.sh（sqlite+local-fs 即可，无需 TiKV/S3）
```

---

## 10. 已知缺口

```
① C4 fork 可见性：mount 侧未接 lower（fork 记录 base_revision 正确、diff=[]）
② C3 pilot 3 次重复
③ Redis pilot（smallfiles os error 2）
④ C2 的 django 在 BrewFS 上未实测（外推 6.2 h）；C2 若要在 BrewFS 严格模式跑，需把 xattr 补丁编进二进制
⑤ C1-v2 的 randrw 64M、write_sat 1M/64M、coldread 第 3 轮
```
