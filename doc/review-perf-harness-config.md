# BrewFS/JuiceFS 性能测试体系配置 Review

## 现状摘要

当前性能测试体系分成两条主线：

1. `docker/compose-xfstests/run_redis_perf.sh` 启动 Redis + RustFS/MinIO/local-fs，再进入 `run_perf_in_container.sh` 跑 xfstests perf 工具和 fio profiles。默认覆盖 `fio-bigwrite`、`fio-bigread`、`fio-seqread`、`fio-seqwrite`、`fio-randread`、`fio-randwrite`、`fio-randrw`、`dirstress`、`dirperf`、`metaperf`、`looptest`。`--writeback-throughput-profile` 会启用 BrewFS S3 writeback、4GiB 读写内存 buffer、12GiB memory budget、S3 并发 16、writeback upload 并发 4、pending soft/hard 1GiB/2GiB、`writeback_persist_sync=false`、`compression=lz4`、FUSE workers 6，并对读类 fio 打开 prefill drain/remount/clear cache。
2. `docker/compose-xfstests/run_juicefs_perf.sh` 与 `run_juicefs_perf_in_container.sh` 提供 JuiceFS 横向对比。`--writeback-throughput-profile` 默认启用 JuiceFS writeback、buffer 8192MiB、cache 4096MiB、upload/download concurrency 4/16、open-cache 1s/65536、compression none、backup-meta 0，并打开 prefill sync/remount/clear cache。
3. `tools/perf/run_perf.sh` 是宿主机 profiling/flamegraph 脚本，默认 Redis + RustFS + BrewFS release/profiling build，fio workload 为 `seqwrite seqread randwrite randread randrw`，使用 `sync` ioengine、`direct=0`、runtime 60s，并额外跑 on-CPU/off-CPU `perf record`。

测试产物已经在向正确方向演进：compose runner 写 `perf-summary.tsv`、fio JSON、latency log、BrewFS `.stats`、Redis diagnostics 和 `report.md`；BrewFS 侧的 prefill drain 会看 `brewfs_writeback_recent_pending_upload_bytes`、`brewfs_writeback_dirty_bytes`、`brewfs_buffer_dirty_bytes`，避免读测直接命中未落稳的写后状态。

但当前体系还不能直接支撑“单个吞吐数决定优化方向”。已知事实已经说明这一点：

- `tools/perf` 单 workload 曾被 `perf.data` 磁盘爆满影响，profile 工具链自身会改变或中断被测环境。
- `tools/perf` 单测出现过 995/452 MiB/s，而 compose `direct=0` 出现 124s 拖尾，两者矛盾说明宿主机 profile 数字、容器全套 wall seconds、fio runtime 内吞吐不是同一种指标。
- 低水位配置 artifact `perf-run-1781103886-6020` 总耗时 40s 但 write p99 2.87s，说明必须同时看 wall seconds、fio BW/IOPS、p95/p99/p99.9 tail、close/fsync/drain 拖尾，不能只看平均吞吐。

## 具体问题、风险与测试盲点

### 1. P0: BrewFS 与 JuiceFS throughput profile 不同构

- 位置/参数：`run_redis_perf.sh --writeback-throughput-profile` 设置 `BREWFS_WRITE_MEMORY_BYTES=4294967296`、`BREWFS_MEMORY_BUDGET_BYTES=12884901888`、`BREWFS_COMPRESSION=lz4`、`BREWFS_WRITEBACK_PERSIST_SYNC=false`；`run_juicefs_perf.sh --writeback-throughput-profile` 设置 `JFS_BUFFER_SIZE_MIB=8192`、`JFS_CACHE_SIZE_MIB=4096`、`JFS_COMPRESS=none`、`JFS_BACKUP_META=0`。
- 为什么会误导：BrewFS 使用 lz4，JuiceFS 使用 none；BrewFS 读写 buffer 与 JuiceFS buffer/cache 的语义也不同。若测试数据可压缩，BrewFS 可能因压缩减少 S3 字节而看似更快；若 CPU 成为瓶颈，又可能看似更慢。这样无法判断差距来自文件系统实现还是 profile 预算。
- 建议改法：定义一份显式 matrix：`compression=none/lz4`、BrewFS `read/write_memory_bytes` 与 JuiceFS `buffer/cache-size` 的等效口径、upload/download concurrency、open cache TTL、writeback durability。默认横向对比先跑 `compression=none` 的严格对齐组，再跑产品推荐组。
- 验证方式：同一 commit 下跑 BrewFS/JuiceFS 2x2 profile，检查 artifact 中 `backend.yml` 与 `juicefs-profile.env`，并比较实际 S3 put/get bytes、CPU time、fio BW、wall seconds。

### 2. P0: `direct=0` 默认混入内核 page cache 与 FUSE writeback-cache

- 位置/参数：`run_perf_in_container.sh` 和 `run_juicefs_perf_in_container.sh` 的 fio profile 默认 `--direct="${PERF_FIO_*_DIRECT:-0}"`；`tools/perf/run_perf.sh` 所有 fio 也用 `--direct=0`。
- 为什么会误导：`direct=0` 测到的是 Linux page cache、FUSE writeback-cache、BrewFS/JuiceFS 用户态 cache、对象存储的叠加结果。写 workload 可能在 fio runtime 内快速返回，真正 close/fsync/upload/drain 在脚本 wall seconds 或后续工具里体现；读 workload 可能命中内核 cache 而绕过用户态缓存差异。
- 建议改法：每个核心 fio 场景同时保留 `direct=0` 和 `direct=1` 两套结果。`direct=0` 用于产品体感和 writeback-cache 评估，`direct=1` 用于隔离用户态/后端路径。报告中必须把 direct 模式列为第一等维度。
- 验证方式：对 `seqread/seqwrite/randread/randwrite/randrw/bigread/bigwrite` 跑 direct 双轨，确认 `report.md` 和 summary 能按 direct 分组输出；读测配合 drop_caches/remount 后，direct=0 与 direct=1 的差距应可解释。

### 3. P0: fio runtime 吞吐与脚本 wall seconds 未绑定判读

- 位置/参数：`run_logged_tool()` 记录 `perf-summary.tsv` 的脚本 elapsed seconds；fio JSON 记录 runtime 内 BW/IOPS/latency。`fio-bigwrite` 使用固定 size、`--end_fsync=1`，其他 write profiles 使用 `--time_based`，未统一 close/drain 后处理口径。
- 为什么会误导：fio JSON 的 BW 可能只覆盖 runtime，脚本 elapsed 包含准备、close、end_fsync、FUSE flush、异步写回等待或挂载清理。compose `direct=0` 出现 124s 拖尾，而 tools/perf 单测 995/452 MiB/s，正是“runtime 内吞吐”和“端到端完成时间”口径冲突。
- 建议改法：报告中为每个 fio 增加 `fio_runtime_s`、`fio_elapsed_s`、`script_wall_s`、`post_fio_drain_s`、`close/fsync included` 字段。writeback profile 下，write workload 完成后应可选等待 pending dirty 归零，并把等待时间单独计入。
- 验证方式：复跑能复现 124s 拖尾的 compose 命令，确认报告同时展示 fio BW、wall seconds、drain seconds；低水位 artifact `perf-run-1781103886-6020` 这类“总 40s 但 write p99 2.87s”的结果应被标注为 tail 风险而不是单纯 pass。

### 4. P0: JuiceFS prefill drain 只做 `sync`，缺少等价 pending 观测

- 位置/参数：BrewFS `wait_for_fio_prefill_drain()` 读取 `.stats` 中 pending/dirty/buffer_dirty；JuiceFS `run_juicefs_perf_in_container.sh` 在 prefill 后只 `sync`，然后可 remount/clear cache。
- 为什么会误导：JuiceFS writeback 的后台上传状态未被 artifact 化。`sync` 对 JuiceFS writeback/cache 的语义未必等价于 BrewFS `.stats` pending bytes 归零。读测可能在 JuiceFS 侧仍受本地 staging/cache 影响，或反过来 BrewFS 因严格 drain 付出额外 wall time。
- 建议改法：JuiceFS runner 增加可观测 drain gate，例如采集 `juicefs stats`/`juicefs status`/mount log 中的 pending upload、cache bytes 或对象存储 PUT 完成计数；若无法精确等价，报告中明确标注 JuiceFS drain confidence。
- 验证方式：读类 fio prefill 后记录 S3 bucket object/bytes、JuiceFS cache dir size、mount stats；remount 前后确认预填充数据已经对象端可读，且 cache 清理真的发生。

### 5. P1: 读测清 cache 语义不完整，page cache、用户态 cache、对象存储 cache 未分层记录

- 位置/参数：`PERF_FIO_COLD_READ_CLEAR_CACHE` 清 BrewFS cache root 或 JuiceFS cache dir；`PERF_FIO_DROP_CACHES`/`PERF_FIO_COLD_READ_DROP_CACHES` 可 drop kernel cache，但默认 throughput profile 只设置 clear cache/remount，未默认 drop_caches。
- 为什么会误导：remount 会重启用户态文件系统，但不一定清宿主/容器 page cache；清 cache root 不等于清内存 cache；RustFS/MinIO 也可能受容器卷和内核缓存影响。读性能改善可能来自缓存污染，而不是读路径优化。
- 建议改法：报告每次读测前输出 cache hygiene 状态：是否 remount、是否 drop_caches、清理路径、清理前后目录大小、是否重建 mount 进程、对象存储卷是否复用。严格 cold-read profile 默认打开 drop_caches，并和 warm-read profile 分开。
- 验证方式：在 `fio-seqread/randread/bigread` 前后比较 BrewFS `.stats` cache hit ratio、S3 GET bytes、FUSE read bytes；cold run 的首次读应有可解释的 S3 GET，warm run 应有明显 cache hit。

### 6. P1: `tools/perf/run_perf.sh` 与 compose runner 参数体系分叉

- 位置/参数：`tools/perf/run_perf.sh` 使用宿主机 mount、`ioengine=sync`、`direct=0`、`PERF_FIO_WORKLOADS`，默认 `VERIFY_CACHE_CHECKSUM=full`、`WRITEBACK_MODE=commit_before_upload`、`BREWFS_S3_MAX_CONCURRENCY=16`；compose 使用容器、`io_uring`、xfstests runner 和不同 artifact/report。
- 为什么会误导：tools/perf 适合定位热点，不适合直接作为 JuiceFS 横向对比基线。`perf record` 还曾因 `perf.data` 磁盘爆满影响单 workload，说明 profile 过程本身会改变结果可信度。
- 建议改法：明确文档分工：compose 是横向 baseline 和回归门禁；tools/perf 是 profiling drilldown。tools/perf 的 fio matrix、direct/ioengine/runtime/size 应从 compose profile 导入，或者报告里禁止与 JuiceFS compose 数字直接并表。
- 验证方式：同一 workload 用 compose 和 tools/perf 各跑一次，报告列出配置 diff；只有在 direct、ioengine、size、runtime、cache hygiene、writeback profile 全部一致时才允许比较吞吐。

### 7. P1: 默认工具列表含全场景，但常用计划/命令只跑子集

- 位置/参数：入口脚本默认覆盖 11 个工具；`2026-06-04-brewfs-juicefs-perf-gap-closure.md` 的核心命令常只跑 `fio-bigwrite fio-bigread fio-seqread fio-seqwrite`，验收表也主要看四项。
- 为什么会误导：优化可能提升顺序读写，却恶化 `randread/randwrite/randrw/metaperf/dirstress`。尤其 metadata cache、rename/open/stat、目录并发错误汇总、混合读写 p99，才是 JuiceFS 差距的重要部分。
- 建议改法：门禁分层：快速 smoke 跑四项大读写；接受优化前必须跑全场景 `seqread/seqwrite/randread/randwrite/randrw/metaperf/dirstress/dirperf`，并设置 randrw p99、metaperf rename/stat/open、dirstress error pattern 的回归阈值。
- 验证方式：每个候选优化产物生成一张全场景 diff 表，要求 primary metric 改善时，randrw p99 不退化超过 25%，metaperf 关键 ops 不退化超过 15%，dirstress 非预期错误不增加。

### 8. P1: `fio-bigread/bigwrite` 与 time-based profiles 口径混杂

- 位置/参数：`run_fio_profile()` 中 `bigwrite/bigread` `runtime=0`、`use_time_based=false`、size 默认 128m、numjobs 8；`seq*`/`rand*` 默认 time_based 60s，size 512m/1g。
- 为什么会误导：big profiles 更像固定数据量微基准，seq/rand profiles 是时间窗压力测试。固定 128m 在 4GiB/8GiB cache profile 下很容易完全落入内存/cache，无法代表大工作集；time_based 则可能多轮覆盖同一小数据集。
- 建议改法：把 profile 类型标注为 `fixed-size` 或 `time-based`，并提供大工作集 variant，例如 size 至少超过 read/write memory budget 和 JuiceFS cache size。横向目标表不要把 fixed-size bigread 和 time-based seqread 混为同一类结论。
- 验证方式：增加 `working_set / cache_budget` 比例字段；跑 128m、4g、16g 三档，观察 BW、S3 GET/PUT bytes、cache hit 是否随工作集合理变化。

### 9. P2: fio 参数可覆盖但 artifact 中没有完整 env 快照

- 位置/参数：入口脚本透传大量 `PERF_FIO_*`、`BREWFS_*`、`JFS_*` 环境变量；BrewFS artifact 复制 `backend.yml`，JuiceFS 写 `juicefs-profile.env`，但没有统一保存实际命令行和完整 env allowlist。
- 为什么会误导：同名 artifact 的结果无法完全复现，特别是调用方传入 `PERF_FIO_*_ARGS` 时会绕过默认参数。后续 review 很难判断某次结果是代码变化、profile 变化还是环境变量残留。
- 建议改法：每次 run 保存 `run-env.env`、`fio-effective-args.tsv`、`compose-services.txt`、git sha、dirty state、镜像 id。对 `PERF_FIO_*_ARGS` 这种整串覆盖，报告要显示最终 fio 命令。
- 验证方式：任选一个 artifact，仅凭 artifact 内文件重放同一命令；重放结果的 fio job options 与原始 JSON 完全一致。

### 10. P2: Redis/RustFS/MinIO 状态复用与低水位配置未显式隔离

- 位置/参数：compose down 使用 `-v` 清理本次 compose 资源，但 host artifact 与端口残留会被 preclean 处理；低水位配置和默认 profile 都写入同一 artifacts 命名空间。
- 为什么会误导：低水位配置 artifact `perf-run-1781103886-6020` 总耗时 40s 但 write p99 2.87s，说明低预算能给出不错 wall time，也可能带来不可接受尾延迟。如果 artifact 没有 profile class，后续容易把低水位探索结果当推荐基线。
- 建议改法：artifact 命名或 metadata 增加 `profile_class=baseline|throughput|low-watermark|profiling|experiment`，并强制记录 memory/writeback/pending soft-hard。低水位结果只能作为探索，不进入默认目标表，除非 tail SLA 同时满足。
- 验证方式：扫描 artifacts，所有报告首页能显示 profile class；低水位配置与 throughput profile 的 p99/p99.9、wall seconds、pending bytes 均可并排比较。

### 11. P2: dirstress 错误只在日志中汇总，没有进入机器可读报告

- 位置/参数：BrewFS `run_dirstress()` 会 grep `!!`、`File exists`、`mknod Function not implemented` 并打印 info；JuiceFS runner 没有同等机器可读汇总。
- 为什么会误导：dirstress 通过/失败不能表达错误形态变化。并发目录场景下，一些 EEXIST 可能可接受，但新出现的 ENOENT/EIO/permission error 会被淹没在日志里。
- 建议改法：生成 `dirstress-summary.json`，字段包括 total errors、expected EEXIST、expected ENOSYS、unexpected errors topN，并在 `report.md` 中列出 BrewFS/JuiceFS 对比。
- 验证方式：构造包含预期和非预期错误的 dirstress log，确认 report 能把非预期错误标为失败或警告。

### 12. P2: metaperf 覆盖了操作集合，但缺少热点元数据配置对照

- 位置/参数：`metaperf` 默认 `create open stat readdir rename`、30s、200 op files、2000 bg files；BrewFS profile 可设置 `BREWFS_METADATA_OPEN_CACHE_TTL_MS=1000`、capacity 65536；JuiceFS profile 设置 `JFS_OPEN_CACHE=1s`、limit 65536。
- 为什么会误导：open-cache 参数看似对齐，但 Redis CSC、inode attr cache、handle cache 的语义不同。只看 aggregate ops/sec，无法知道 stat/open 是否命中本地 cache，还是被 Redis/脚本缓存隐藏。
- 建议改法：metaperf 报告同时列 Redis commandstats、open/stat/rename 单项延迟分布、BrewFS metadata cache hit/miss、JuiceFS open-cache 配置和状态。对 metadata 优化，必须单独跑 cache-cold 和 cache-warm 两组。
- 验证方式：metaperf 前重启 mount/清 metadata cache 跑 cold，再不清 cache 跑 warm；Redis commandstats 中 GET/HGET/EVAL 调用数应能解释 ops/sec 变化。

## 最值得优先做的 3 个测试体系改进

### A. 建立“可比 profile matrix”，先把 BrewFS/JuiceFS 参数同构

- 收益：把优化决策从“谁的默认配置更占便宜”转为“同一 compression、cache budget、direct、writeback durability、concurrency 下实现差异是什么”。这会直接降低误判读缓存、压缩、writeback 优化收益的概率。
- 回退风险：短期报告会变复杂，历史目标表需要重标口径；但可以保留旧 profile 作为 `legacy`，不影响继续看历史趋势。

### B. 把 wall seconds、fio runtime、drain/close tail 合成一张端到端报告

- 收益：能同时解释 995/452 MiB/s 单测、compose 124s 拖尾、`perf-run-1781103886-6020` 40s 总时长但 write p99 2.87s 这类矛盾。优化写路径时，团队会知道是在改善 runtime BW、close/fsync、后台 drain，还是只是把成本推迟到下一个工具。
- 回退风险：某些旧 artifact 缺字段，需要 report parser 兼容空值；新增 drain 等待如果默认启用，可能拉长 CI 时间，建议先只报告、不 gate，再逐步设阈值。

### C. 全场景门禁分层：快速四项 smoke + 接受前全套 regression

- 收益：保留快速迭代速度，同时避免只优化 `bigread/seqread/seqwrite/bigwrite` 导致 `randrw/metaperf/dirstress` 退化。对 BrewFS/JuiceFS 差距而言，metadata 与混合读写 tail 经常比平均吞吐更能指导下一步架构改动。
- 回退风险：全套 regression 成本更高；可以通过 nightly 或手动接受前门禁执行，普通开发循环只跑 smoke。

## 并行 agent 补充审查

### P0 补充：BrewFS 已支持 direct matrix，JuiceFS runner 尚未对齐

- 位置：BrewFS `run_redis_perf.sh`/`run_perf_in_container.sh` 已支持 `PERF_FIO_DIRECT_MATRIX` 与 per-workload direct matrix；JuiceFS `run_juicefs_perf.sh`/`run_juicefs_perf_in_container.sh` 仍主要是单个 `DIRECT` 值。
- 风险：BrewFS 可以在一个 artifact 中产出 `direct=0/1` 双轨，JuiceFS 需要外部跑两次，artifact 命名、summary 行和 report 口径容易不一致。横向对比时 direct 维度一旦错位，读写缓存结论会失真。
- 改动建议：JuiceFS runner 补齐与 BrewFS 同名的 `PERF_FIO_DIRECT_MATRIX`、`PERF_FIO_*_DIRECT_MATRIX` 语义；summary/report 把 direct 作为一级维度。
- 验证：同一工具列表下 BrewFS/JuiceFS 都生成 direct=0/direct=1 两组 summary 行，artifact 中可直接横向 diff。

### P0 补充：write workload 缺 post-write drain gate

- 位置：BrewFS runner 当前主要在 read/mixed prefill 后做 drain；`seqwrite/randwrite/bigwrite` 跑完多为 snapshot stats，没有可选等待 pending 归零。
- 风险：fio runtime BW 可能看起来高，但后台 upload/flush 成本推迟到下一个 workload，污染后续 `bigread/randread/randrw/metaperf`。这也是“tools/perf 高吞吐、compose wall 拖尾”不一致的常见来源。
- 改动建议：增加 `PERF_FIO_POST_WRITE_DRAIN=true`，对 write/mixed workload 记录 `post_fio_drain_s`、drain 前后 pending/dirty/S3 PUT bytes。默认可以先 report-only，接受门禁再启用 gate。
- 验证：复跑 `fio-seqwrite fio-randwrite fio-randrw`，报告同时展示 fio BW、script wall、post-write drain 秒数和 pending 变化。

### P1 补充：JuiceFS 缺 BrewFS 等价 report/diagnostics

- 位置：BrewFS 生成 `report.md`、Redis diagnostics、BrewFS `.stats`；JuiceFS runner 主要写 summary/log/fio JSON/profile env。
- 风险：BrewFS/JuiceFS 对比时，一个有 tail/cache/S3/Redis 解释，另一个只有 fio JSON 和日志，容易回到“只比吞吐”的误判。
- 改动建议：抽共享 report parser，至少让 JuiceFS 输出 fio summary、runtime accounting、latency percentiles、Redis commandstats、cache hygiene、drain confidence、cache dir size。
- 验证：同一 artifact 内 BrewFS/JuiceFS 都有 report 首页，字段能并排展示。

### P1 补充：bigread/bigwrite 的 runtime 文档与实现口径需要写清

- 位置：入口 usage 暗示 `RUNTIME` 适用于 fio profile，但 bigread/bigwrite 实际是 fixed-size、`runtime=0`。
- 风险：使用者以为改了 runtime，实际 big profiles 仍跑固定大小；对大 cache/buffer profile，128MiB fixed-size 很容易被缓存完全吸收。
- 改动建议：文档明确 big profiles 是 fixed-size；或实现 time-based big profile variant。报告加入 `profile_kind=fixed-size|time-based` 和 `working_set/cache_budget`。
- 验证：artifact report 中 bigread/bigwrite 明确显示 runtime 不生效、size 生效。

### 下一轮优化接受标准

- fio 全场景：`seqread`、`seqwrite`、`randread`、`randwrite`、`randrw`、`bigread`、`bigwrite`。
- metadata：`metaperf` 至少覆盖 `create/open/stat/readdir/rename`，同时保留 Redis commandstats 和 cache hit/miss。
- direct 矩阵：核心 fio 场景必须同时跑 `direct=0` 与 `direct=1`。
- cache hygiene：读测必须记录 prefill、drain、remount、清 BrewFS/JuiceFS cache dir、drop_caches、清理前后 cache dir size。
- drain/tail：报告必须展示 fio BW/IOPS、p95/p99/p99.9、script wall、active runtime、close/flush tail、post-write drain seconds、pending/dirty bytes。
- 横向公平性：至少一组 `compression=none` 的 BrewFS/JuiceFS 对齐 profile；推荐 profile 可以另列，不能替代公平 profile。
- 回归门禁：吞吐提升不能伴随 `randrw` p99/p99.9 明显恶化；metadata ops/sec 不应明显退化；write workload 不允许留下未解释的 pending dirty/upload tail。

## Review 结论

当前 runner 已经具备较完整的工具覆盖和 artifact 基础，但“参数可比性”和“指标口径”仍是最大风险。并行审查后最需要优先补齐的是 JuiceFS direct matrix/report 等价能力，以及 write workload 的 post-write drain 账本。近期不建议继续只用单个 fio BW 或单个 artifact 作为优化接受依据。最小可行的下一步是：统一 BrewFS/JuiceFS profile matrix，报告 direct/cache hygiene/drain/wall/tail，并把全场景回归设为优化接受前的必跑项。
