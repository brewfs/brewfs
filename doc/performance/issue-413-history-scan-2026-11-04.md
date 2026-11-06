# #413：消除重复历史扫描

继续 PR #414 的 `fix/issue-413-write-pressure-memory`，起点 `a4ff763`。
本轮窗口为 2026-11-03 16:36–20:36 UTC；原始证据和重放脚本在
`/mnt/brewfs-data/home/hxy/issue413/history-scan-followup/`。

## 扫描归因（16:44 UTC 完成）

使用原 64 条发布的冻结诊断二进制，在既有 compose 框架运行 60 秒压力：
4 jobs × depth 32 × 4 KiB randwrite，512 MiB/job，direct=1，seed 391，
workspace-v1 + Redis + RustFS/S3。具体二进制、镜像与 argv 见
`baseline-monitor/` 中的清单。仅诊断轮打开 debug 和 Redis MONITOR；
MONITOR 只保存 extent 扫描的 Lua GET 键及 ZRANGEBYLEX 命令，不保存值。
压实使用 128 行分页，普通读取使用 32 行分页，因此可区分两条调用路径。

| 全轮扫描量 | 实测 |
|---|---:|
| 历史扫描次数 | 2,000 |
| 非空 / 空扫描 | 967 / 1,033 |
| 读取行数 | 693,451 |
| 不同 extent 键 | 51,692 |
| 已经读过的行再次读取 | 641,759（92.5457%） |
| 非空扫描平均 / 中位 / 最大行数 | 717.12 / 687 / 1,677 |

32 个压力 chunk 的 head 和空 lower layer 各被扫描 29–36 次；另有一个
预热 chunk 在两层各扫描一次。重复按 extent 键计数，统计范围为整轮，
不是只有 close 窗口。重放命令：

```bash
python3 history-scan-followup/analyze-scan-keys.py \
  history-scan-followup/baseline-monitor
```

根因是 `compact_extent_history` 在每个发布批次从头扫描涉及 chunk 的
完整历史；上次刚扫描过的旧行在下一批再次从后端读取。下层为空也重复查。
增加批次上限不是本轮首版假设。MONITOR 会显著干扰耗时，本轮扫描计数
仅用于归因；不能把它的吞吐或尾部与无 MONITOR 的验收轮比较。

## 候选设计与待验证内容

仅保留覆盖区间的并集、持久化行数、下层行数及最大 sequence，建立
generation 对应的压实证明。新增 Data extent 与旧覆盖和同批新增行均
不重叠时，旧行不可能被遮盖，压实结果就是旧行加新增行，可以跳过历史
读取。覆盖写、hole、证明缺失或 generation 变化均走完整压实。

head 和所有下层 generation 在扫描或复用之前读取，并进入最终原子 CAS。
证明只有确认提交成功后才推进；失败、取消和不确定响应丢弃本次进度。
重启或其它实例写入后重新扫描。证明不保存旧 extent 内容或 slice 标识，
最多保留 64 个 chunk，实际区间存储计入现有 metadata 预算。逐 chunk 的
`RECOVERY_MAX_ROWS=8192`、读取行数限制、ownership 和 hole 顺序保持原约束。

首版保持 64 条发布、原上传并发、缓存预算及严格排空条件。新增回归测试
在旧实现上实际失败：第二次不重叠 append 的历史分页计数从 2 增至 5。
完整本地门禁、两次连续无 debug 压力验收及分段诊断仍在执行；此处不把
定向测试或候选设计作为性能通过结论。最终结果将在本报告及现有 PR 正文
补齐，未达到条件时保持 draft。
