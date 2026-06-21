# brewfs vs juicefs 实测数据 (2026-06-21)

宿主机本地化对等基准：两端使用**相同后端**（Redis 元数据 + 本地目录对象存储，4MiB block，无压缩），
默认配置。吞吐用 fio（bs=4m, ioengine=psync, iodepth=1），元数据用自写 microbench（metabench）。
读为**冷读**（写后 unmount+remount 清空客户端缓存与内核 page cache）。机器：32 核 / 91GB / NVMe。
JuiceFS v1.3.1；brewfs 为当前源码构建。

> 注意：对象存储为本地 NVMe（非 S3）。这会**淡化**历史 S3 测试中的读缓存未命中差距（读 8–42×），
> 但**消除了此前掩盖 brewfs 写路径 CPU/串行化开销的 S3 PUT 瓶颈**，因此写差距在此暴露得更彻底。
> 读缓存相关差距请参考 `../../juicefs/07-performance-comparison.md`（rustfs S3 实测）。

纳入版本管理的文件（数据/文档）：
- `summary.tsv` — 原始结果（engine, tool, bw_MiBps, iops, p50_ms, p99_ms）
- `comparison.md` — brewfs vs juicefs 对比表（含比值）

> 注：产生上述数据的测试脚手架（`run_bench.sh`、`metabench.c`、`parse_fio.py`、
> `combine_results.py`）刻意**不纳入版本管理**；下方“复现要点”给出了等价的命令步骤。

复现要点：
1. 起 Redis（容器或本机）。
2. brewfs：`brewfs mount --meta-backend redis --meta-url redis://127.0.0.1:6379/0 --data-backend local-fs --data-dir <obj> <mnt>`
3. juicefs：`juicefs format --storage file --bucket <obj>/ --block-size 4096 --compress none redis://127.0.0.1:6379/1 <name>` 然后 `juicefs mount ...`
4. `bash run_bench.sh brewfs` 与 `bash run_bench.sh juicefs`，再 `python3 combine_results.py`。
