# 🧪 C1–C4 实验看板（DASHBOARD）

> 单一事实来源：本文件由**定时巡检任务每轮自动刷新**（10 分钟 / 30 分钟两条）。
> 最后更新：**2026-11-08 21:40 CST**（收尾：C1-v2 完成、C4 seal 编出二进制但 fork 可见性缺口、数据已归档、资源清零 ⇒ ¥0.3/h）
> 上下文目录：`runs/dsh-experiment-context-20261106/`（CURRENT.json / DECISION_LOG.md / C3_RESULTS.md 同步维护）

---

## 1. 总览

| **C1** fio 五模式 | 顺序/随机读写、同步写代价 | 🟢 1M 两侧同方法已测 | **JuiceFS 写 238.5 / 冷读 2,974.6 MiB/s**；**BrewFS 写 336.8 / 冷读 1,940.8 MiB/s**（同机同对象存储，RustFS 重启后测）；4M/64M/随机待测 |
|---|---|---|---|
| **C1** fio 性能 | 3 bs × 4 op × 3 轮 × 2 系统 | ✅ **完成 72/72 cell** | write/randwrite JuiceFS 吞吐高 15–105%，但 BrewFS p50 低 4×；randrw BrewFS 全面胜（bw 8.5×、p50 12×）；冷读 JuiceFS 6.7× 快；0 错误 |
| **C2** trace 回放 | 6 条 trace | ✅ pytest 双侧完成 | JuiceFS strict 109.7 s；**BrewFS performance 模式 680.06 s（6.2×）**，ops 1,747,109 完全一致，probe_errno_diff 23,929（xattr/ioctl 路径），handles 0；django 外推 6.2 h 未实测 |
| **C3** 百万小文件 | 创建/冷读/热读/COW/写 | 🟢 JuiceFS 完成；🟡 BrewFS 校验中 | JuiceFS verify-base **PASS**（1M/4.096GB/2587.57s）+ pilot **PASS**（4.4466s）；BrewFS 导入 1024/1024、分段校验进行中 |
| **C4** fork/handoff | 批量派生、跨机续跑 | 🔴 未启动 | 依赖 C3 fixture 完成 |

**一句话状态**：C3 全 PASS、C2 6/6 PASS；**C1 因对象存储磁盘写满（100%）全部作废**，已删 4 个 C1 临时 bucket（492G→44G），C1 需在干净盘上重跑；C4 未启动。

---

## 2. 现场快照（自动采集，`dashboard-status.sh`）

| 项 | 值 |
|---|---|
| 模型端点 `POST /v1/responses` | **已切 DeepSeek**（`https://api.deepseek.com/v1`，模型 `deepseek-flash`，协议 responses）；旧中转 130.33.96.80 仍 503 |
| codex agent `gbrew`（jump） | 🟢 **运行中**（DeepSeek 驱动，16:02 起；日志 339 KB） |
| a-client 内存 | 2 G used / 61 G total |
| BrewFS 挂载 | **y**（/mnt/brewfs-c3/base） |
| brewfs RSS | 1.7 GB |
| base/small 目录数 | **1024** / 1024 |
| done 标记 | 1039 |
| **分段校验（BrewFS，编排者版）** | ✅ 32/32 段、1024 目录、0 mismatch、799.69 s |
| **分段校验（agent 复核版）** | ✅ **10/10 段完成，0 mismatch，总 1,254 s，RSS 峰值 1,782 MB，27 次重挂全部 OK** |
| pilot workspace 物化 | 🔄 **agent 自建 workspace**：`brew-workspace-build-20261107`，chunk-09/32 完成（每 chunk 32 目录、重挂一次，RSS 峰值 1.35 GB）；路径 `/mnt/brewfs-c3/base/ws-c3-20261107` |
| txn drop（`Dropping an active transaction`） | **0** |
| AGENT-REPORT | 5,821 B（agent 停摆期间未增长） |

---

## 3. C3 明细（当前工作重心）

### 3.1 JuiceFS（已完成，可引用）
```json
verify-base: {"completed":true,"passed":true,"files":1000000,"bytes_checked":4096000000,"wall_seconds":2587.573239107}
pilot:       {"completed":true,"passed":true,"verified_reads":20224,"writes":768,"wall_seconds":4.446584426}
verify-touched: child 256 files PASS / base 256 files PASS
```
证据：`/srv/ax-data/c3/verify-base-par-20261107/`、`/srv/ax-data/c3/pilot-juice-20261107/`
备注：4.45 s 为 base 刚校验完的**热态**；早前一次 13.28 s。正式对比需按协议重复 3 轮。

### 3.2 BrewFS（导入完成，校验进行中）
- 导入：`/mnt/brewfs-c3/base/small/d0000..d1023`，1,000,000 × 4 KiB；分批导入 + 批间重挂（内存 ≈85 KB/文件）
- `Dropping an active transaction` = **0**（PR #435 的 TxnGuard 生效）
- 全量 audit 只发现 **3 个坏文件**（根因：旧 `done/` marker 标记了未拷完目录，产生 0 字节）
  - `d0130` 已修；`d0132`、`d0133`、`d0371` 已补拷并**逐文件 sha256 复核 977/977 全一致**
- 分段校验完成：33 个段文件（其中 seg-01 与 seg-aa 重复覆盖 d0000–d0031，去重后 **1024 个目录**、无缺失）
  - 去重汇总：`/srv/ax-data/c3/segverify/verify-base-brew-segmented-final.json`
    `{"completed":true,"passed":true,"segments_run":33,"dirs_verified":1024,"files_checked":1031264,"mismatches":0,"wall_seconds":799.69}`
  - 口径说明：files_checked 含重复段，唯一文件数应为 **1,000,000**；结论以「1024 目录全覆盖 + 0 mismatch」为准
  - 方法：16 并发读 + sha256 对比源目录，每 4 段重挂一次，RSS 1.6–6.1 GB

---

## 4. 机器与成本

| 实例 | 规格 | 状态 | ≈成本 |
|---|---|---|---|
| brewfs-singapore-v2 | 8c32g | Running（包年） | **¥0** |
| ax-c1-jump | 2c8g | Running（codex 出口；跑 DeepSeek） | ¥0.3/h |
| ax-c1-a-client | 16c64g | Running（C2 重放 + C1 前置；磁盘 110/492 G） | ¥2.7/h |
| ax-c1-a-meta | 8c32g | Running（PD/TiKV；内存 3/61 G 侧） | ¥1.6/h |
| ax-c1-a-obj | 16c64g | Running（RustFS） | ¥3.2/h |
| brewfs-build | 8c32g | Stopped | 磁盘 ≈¥0.1/h |
| tikv-xattr-test | 4c8g | Stopped | 磁盘 ≈¥0.1/h |
**当前合计 ≈ ¥0.3/h**（jump 0.3；brewfs 包年 ¥0）。21:40 已删除 ax-c1-b-*（3 台）与 ax-c1-c-build（1 台）及其盘；现存仅 jump 40 GiB ⇒ 磁盘月费 ≈¥40（旧配置 ≈¥6,144/月）。
| ax-c1-a-client | 16c64g | **Stopped（21:57）**；ws-c3 半成品 881/1024 保留 | 磁盘 ≈¥0.1/h |
| ax-c1-a-meta | 8c32g | **Stopped（20:32，空闲停机）** | 磁盘 ≈¥0.1/h |
| ax-c1-a-obj | 16c64g | **Stopped（20:32，空闲停机；RustFS 随机器停）** | 磁盘 ≈¥0.2/h |

## 5. Agent 与产出（全部用 tmux 管理）

| 会话 | 机器 | 状态 | 产出 |
|---|---|---|---|
| `gbrew` | jump | 🟢 运行中（DeepSeek `deepseek-flash`） | BrewFS C3：pilot workspace 构建（ws-c3，chunk 级重挂） |
| `gseal`（brewfs 包年机） | brewfs | 🟢 实现中 | 给 `workspace` 加 `seal` 子命令（把 live head 层封存成可 fork 的 revision），含单测与端到端验证；C4 的唯一卡点 |
| 现场 | — | ✅ 证据完好 | C3 全部证据在 a-client 本地盘（5.7 GB）；对象桶 c3-base / c2-base / c2b 保留 |
| `gxattr` | brewfs-build（已停） | ✅ 完成 | **PR #433**（xattr 矩阵）+ **PR #434**（JuiceFS xattr/attr-cache） |
| `view426` | brewfs | ⚪ 历史残留（可清） | 11-05 旧会话 |

---

| 阻塞 | 影响 | 处置 |
|---|---|---|
| **C1 两次作废：对象盘写满（100%）** | 所有 C1 数字不可用 | 已定位：每格 8 GiB × 多轮超 492G；改为每格 1–2 GiB、单轮、每轮查水位 |
| JuiceFS TrashDays=1 | 删除文件仍占空间 | 已设 `--trash-days 0` + `juicefs gc --delete`（需跑足够久） |
| BrewFS pkill -9 丢数据 | 写极快/读 EIO 假象 | 必须优雅 `umount`；冷读用「写→umount→清 cache→重挂→读同目录」 |
| 并行 lane 不可用 | JuiceFS 掉到 14.3 MiB/s | 该环境单节点 TiKV/RustFS，只能串行 |
| 对象端 page cache 无法 drop | 冷读偏快 | 真冷读需重启 RustFS |
| C2 rust chown 保真 | 曾两法皆不可行 | ✅ 已解：uid1000 + initial 树整体 chown（6/6 PASS） |

---

## 7. 下一步（按优先级）

1. **C1 重做（阻塞中）**：拉起 a-client/a-meta/a-obj，按「每格 1–2 GiB、单轮、跑完即删、每轮查 a-obj 水位」跑 1M/4M/64M × 写/冷读 + 4k 随机；冷读前重启 RustFS
2. 用同一方法测 BrewFS 侧，形成可比表格（写入 METRICS.md）
3. **C4 启动**：等 C1 收口后做 fork/handoff（需 native workspace/session executor）
4. 收尾：把 C1/C2/C3 的最终数字整理成论文用表（当前 C3、C2 已就绪）
5. 成本：实验结束立即停三台（当前已全停，¥0.6/h）
