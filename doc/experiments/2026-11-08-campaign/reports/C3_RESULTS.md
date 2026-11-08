# C3 百万小文件：当前结果（2026-11-07 更新）

状态：**JuiceFS 侧已通过（base verify + pilot）；BrewFS 侧导入完成但校验未通过，卡在 3 个坏文件与内存上限；外部模型端点 503 导致 agent 暂停。**

## JuiceFS C3（已完成，证据齐全）

### 1M base 全量校验（verify-base）
```json
{"completed":true,"passed":true,"files":1000000,
 "bytes_checked":4096000000,"wall_seconds":2587.573239107}
```
- 位置：`/srv/ax-data/c3/verify-base-par-20261107/result.json`
- 结论：100 万文件、4.096 GB 全部逐字节通过；耗时 **43.1 分钟**

### pilot（clone CoW workspace 后运行）
```json
{"completed":true,"passed":true,"files":1000000,"workers":8,
 "verified_reads":20224,"writes":768,"wall_seconds":4.446584426, ...}
```
- 位置：`/srv/ax-data/c3/pilot-juice-20261107/`
- `verify-touched`：child 256 文件 PASS、base 256 文件 PASS
- 说明：本轮 **4.45 s 为「base 刚被完整校验过」的热态**；早前一次为 13.28 s。正式对比需按协议重复 3 轮并注明冷热态。

## BrewFS C3（导入完成，校验未通过）

### 导入（已完成）
- 目标：`/mnt/brewfs-c3/base/small/d0000..d1023`，1,000,000 个 4 KiB 文件
- 方式：分批导入 + 批间重挂（内存 ≈85 KB/文件，单批控制在 ~256 目录）
- 最终编排器：`/srv/ax-data/c3/import-brew-final.sh`，CSV `import-brew-final.csv`
- 关键指标：`Dropping an active transaction` = **0**（PR #435 的 TxnGuard 修复生效）

### 已定位的三个真实问题（都有证据）
1. **事务取消丢锁** → PR #435（`src/tikv_transaction.rs`，TxnGuard 兜底回滚）已修
2. **S3 凭据**：配置结构里没有凭据字段，必须用环境变量 `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_DEFAULT_REGION`；否则每次上传失败重试，单文件创建 5 秒以上（修复后 10–30 ms）
3. **进程内存随文件数线性增长**（≈85–90 KB/文件）：43.7 万文件时 RSS 44 GB，把 64 GB 主机拖到 sshd 失联
   ⇒ 必须分批 + 批间重挂；单趟全量 verify 也会涨到 33–39 GB，需要**分段校验**

### 校验失败的具体原因（非系统性损坏）
- 全量 16 并发 audit 扫描 999,997 个文件，**只发现 3 个 mismatch**：
  - `d0130/f0103554`（index 103554）—— 已补拷并复核通过
  - `d0132`、`d0133`、`d0371` 对应文件 —— 待补拷
- 根因：旧 `done/` marker 把「未真正拷完」的目录标记为完成，产生 0 字节文件
- 处理：错误 marker 移入 `bad-markers/`，补拷坏目录后再校验；全部 attempt 目录保留

## 现场与机器

- 当前仅 `ax-c1-jump`（¥0.3/h）与包年机 `brewfs-singapore-v2`（¥0）在跑；
  `a-client` / `a-meta` / `a-obj` 为降本已 StopCharging（数据在持久盘，不丢）
- agent（codex，tmux `gbrew`，跑在 jump、通过 ssh 指挥 a-client）因**外部模型端点
  `POST /v1/responses` 持续 503** 而暂停；网关 `GET /v1/models` 正常

## 结论

- **C3 JuiceFS：完成**（base verify + pilot 均 PASS）
- **C3 BrewFS：未完成** —— 导入已完成，校验差 3 个坏文件 + 需要分段校验方法；
  端点恢复后由 agent 继续（任务书 `/root/task-brew-c3-cont.md`）
- C3 配对比较（JuiceFS vs BrewFS）目前**不可给出**，不能写成「完成」或「正式通过」
