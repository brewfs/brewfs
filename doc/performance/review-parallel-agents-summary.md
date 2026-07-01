# BrewFS 并行代码 Review 汇总

日期：2026-06-10

本轮按模块启动 5 个并行 agent 只读审查，不直接改代码。详细意见已分别合并到 `doc/review-*.md`。

## Agent 分工

- 写回 / writer：`review-writeback-writer.md`
- 读路径 / read cache：`review-read-cache.md`
- 元数据 / metadata cache：`review-metadata-cache.md`
- 对象存储 / disk cache / compression：`review-object-store-cache.md`
- 性能脚本 / 横向对比 / gate：`review-perf-harness-config.md`

## 最高优先级改动建议

### 1. 写回正确性优先于继续调参

- `CommitBeforeUpload` 下 durable staging 不能是 best-effort，也不能让多个 upload batch 覆盖同一个 dirty slice 文件。
- `upload_complete()` 不能只看 dispatched frontier，需要区分 dispatched 与 successfully uploaded。
- early commit 后后台 upload 需要 watchdog，避免 pending/overlay 永久残留。
- 验证：故障注入 object PUT fail/hang、persist fail、多 batch slice crash recovery、`fio-randrw direct=0/1`。

### 2. 读侧先保护前台 I/O，再减少热路径 clone/copy

- 后台 VFS prefetch 和 range-triggered full-block prefetch 需要统一预算；range prefetch 饱和时应 drop/requeue low priority，而不是排队等待。
- `chunk_slices` cache hit 后仍 clone 全 slice Vec，碎片化读时会消耗 CPU。
- `read_at_into` 应接入热路径，减少 span Vec/Bytes/assemble 多层拷贝。
- LZ4 下 page cache 填入与命中策略不一致，需要 A/B 关闭填充或小读先查 page cache。
- 验证：`fio-randread`、`fio-randrw`，`direct=0/1`，记录 read p99/p99.9、S3 GET bytes、page/block hit、slice_count。

### 3. 元数据缓存要补一致性 token 和配置可验证性

- Redis `WRITE_SLICE_LUA` 必须先校验 inode 再写 slice/version，避免错误路径留下脏 slice。
- `stat_fresh` 在 Redis 下仍受 store node cache 影响，强一致语义需要 `stat_no_cache/stat_consistent` 或明确改名。
- `get_slices` cache 需要 chunk version token；open-file cache 的 TTL/写打开命中和 `cache.enabled=false` 都需要可观测验证。
- 验证：Redis unit、双客户端 stale test、metaperf cold/warm、Redis commandstats。

### 4. 对象层先修观测和低风险开销

- range background prefetch 当前是 await permit，dropped 指标会失真；应 try-acquire drop/requeue。
- LZ4 raw fallback 读会二次拷贝；`decompress` 可返回 `Cow`/`Bytes`。
- `read_range` 应防御 zero-length buffer。
- disk cache miss/hit 前多一次 metadata syscall，eviction 应跳过 tmp 文件。
- 验证：randread/randrw、compression=lz4/none、disk cache cold/hot、adapter suite。

### 5. 测试体系先统一口径再谈横向胜负

- BrewFS 已有 direct matrix，JuiceFS runner 需要补齐同名 matrix 和 report 维度。
- write workload 需要 post-write drain 账本，避免把后台上传成本推到下一个工具。
- JuiceFS artifact 需要接近 BrewFS 的 report/diagnostics：fio latency、runtime accounting、Redis commandstats、cache hygiene、drain confidence。
- 接受优化前必须跑全场景：`seqread`、`seqwrite`、`randread`、`randwrite`、`randrw`、`bigread`、`bigwrite`、`metaperf`、目录压力；吞吐提升不能伴随 `randrw` tail 或 metadata 明显回退。

## 建议下一轮实施顺序

1. 修 writer 的 pending/staging/watchdog 正确性与指标，不先碰大规模小写合并。
2. 修 range prefetch try-acquire/drop，并用 direct matrix 验证 randrw read tail。
3. 修 Redis write Lua 原子性，补独立 metadata 脚本和双客户端 stale test。
4. 补 JuiceFS direct matrix/report 对齐，保证后续 BrewFS/JuiceFS 对比可信。
5. 再做 small-write coalescing、版本化 slice cache、对象 PUT 全局优先级调度等较大优化。
