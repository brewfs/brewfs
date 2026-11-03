# Mixed 读 EIO：基线既有缺陷（#415）

结论：**既有缺陷**，已创建 [#415](https://github.com/brewfs/brewfs/issues/415)。
未修改的 `f5706712302435206269a253bf861a1a648761d1` 连续三轮诊断均有四个
fio job 读 EIO；#414 原有候选也捕获到相同错误链。本次只诊断，不在 #414 修复。
这些结果不能证明候选未改变故障频率，也不作为 mixed 性能验收通过的依据。

## 完整错误链

```text
read_extent_deltas_consistent：三次 before/after generation 均不一致
→ WorkspaceError::Busy
→ workspace_to_meta：MetaError::Io(EBUSY=16)
→ FileReader：anyhow::Error<MetaError>
→ VfsError::Anyhow
→ FUSE errno 转换兜底返回 EIO=5
→ fio read: Input/output error
```

`src/workspace_overlay/stores/kv_store/extent_reads.rs` 的一致性 fence 是按 layer
记录的。其他 inode/chunk 的 extent mutation 也会增加同一 generation；持续写入
可让一个合法的分页读连续三次失效。现有 reader 的 transient-error classifier
没有识别 Busy/EBUSY，最终 `src/fuse/mod.rs` 对 anyhow 变体返回 EIO。

`baseline-gdb-04` 的四次终止分支探针均记录 attempt/limit `[3, 3]`；查询分别是
inode/chunk `(8,6)`、`(5,2)`、`(7,2)`、`(6,5)`，使用相同的两个 layer-generation
键。命中时间为 2026-11-03 UTC 12:15:34.172152、12:15:35.956186、
12:15:37.735686、12:15:42.574154。GDB 随后逐一读到 errno 16、anyhow 的
`MetaError` vtable/嵌套值，以及 FUSE 转换实际返回寄存器 `EAX=5`。

## 重复复现及后端状态

| 产物 | 二进制 | fio 结果 | 完整错误链 | 实际运行时长 s | 抓包丢包 |
|---|---|---|---:|---:|---:|
| baseline-gdb-02 | 干净 f570671 | exit 4，四个读 EIO | 4 | 34.945 | 312 |
| baseline-gdb-03 | 干净 f570671 | exit 4，四个读 EIO | 4 | 41.795 | 0 |
| baseline-gdb-04 | 干净 f570671 | exit 4，四个读 EIO | 4，含源头分支 | 36.515 | 0 |
| candidate-gdb-01 | 原 #414 候选 | exit 4，四个读 EIO | 4 | 33.137 | 0 |

请求 runtime 为 60 秒；实际时长只说明失败提前结束，不能用于性能比较。第一轮
`baseline-gdb-01` 的 20 秒负载通过，但探针解析失败，不能作为 errno 证据。
旧的无 GDB 产物 `baseline-mixed-diagnostic`、`baseline-writeguard-d1` 也各有四个
基线读 EIO；另有基线通过的旧轮次，症状具有间歇性。

后三轮无抓包丢包，GET/206 请求响应对分别为 10,766 / 9,979 / 8,551，PUT/200
分别为 4,470 / 4,150 / 3,437；没有捕获到 HTTP ≥400 或 Redis 错误回复。
mixed 结束时 Redis `total_error_replies`、rejected connections、blocked clients、
evictions 均为 0，AOF/RDB 状态为 ok；Redis/RustFS 容器健康、零重启、无 OOM。
没有命中 Redis 后端错误转换探针。脚本在 mixed 前重置 Redis stats，不能把启动
时 script-cache 的 NOSCRIPT 计数与后置值相减。

这些后端观察支持已捕获的客户端 contention 错误链。抓包摘要只统计 HTTP/Redis
报文头，未完整重组 TCP；不据此推断不存在任何可能的后端故障。

## 复现与证据

沿用 compose 框架，新建普通 native `workspace-v1`，Redis + RustFS/S3，
`upload_before_commit`，无压缩，读缓存/预取关闭，写内存 512 MiB，总预算 2 GiB，
coalesce 65536 B / 0 ms，FUSE workers/background 为 16/512。预填充四个
512 MiB 文件、严格排空、cold remount 后执行：

```bash
fio --name=brewfs-randrw \
  --directory=/mnt/brewfs/issue413/.perf-fio-randrw \
  --rw=randrw --bs=4k --size=512m --numjobs=4 \
  --ioengine=libaio --iodepth=32 --direct=1 \
  --runtime=60 --time_based --group_reporting --eta=never \
  --rwmixread=70 --randseed=391 --randrepeat=1
```

完整现场与可重放脚本：
`/mnt/brewfs-data/home/hxy/issue413/close-tail-followup/mixed-eio/`。
使用新的产物名称执行 `launch.sh NAME --gdb-errors --read-errors --runtime 60`。
目录保留源码/二进制哈希、镜像 digest、有效 argv、GDB JSONL、反汇编、Redis dump、
INFO、容器状态和网络抓包。仓库中保留不含凭据的
[证据索引](evidence/issue-413-close-tail-20261103/mixed-eio-summary.json)。

- 基线二进制 SHA256：`bcf76670ea63c7e6b44e1f79be8f0c60f8e9e8fa4af08deb64fe2b7c9aa901ac`。
- 原候选 SHA256：`5d7a25617f9702dda766ba53d313c826eba5682c682160bd12199f05c18df53f`，源码映射见原 `batch-v2/committed-source.json`，对应 `708331d`。
- 源头终止分支探针限定于基线 SHA；候选只复用了符号解析的错误转换探针，不复用基线地址。

GDB 会暂停被调试进程；诊断期间 A 的编译也在运行。因此没有吞吐、延迟或故障
频率的验收结论。本项没有产品代码改动。compose 已于 12:19:30 UTC 完成
`down -v --remove-orphans`；无残留诊断进程、卷或 D-state 任务。

#415 的修复应保留有限扫描和 compaction 一致性，增加跨 inode/chunk generation
churn 的确定性测试，验证 typed error 穿过 reader/FUSE 的行为，再跑 mixed compose
及本地 CI。只把 EIO 改成 EBUSY，或取消一致性 fence，都不能证明正常读写可用。
