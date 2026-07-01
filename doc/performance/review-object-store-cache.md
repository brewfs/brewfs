# BrewFS 对象存储 / 缓存 / 压缩 / 校验 review

审查范围：`src/chunk/store.rs`、`src/chunk/cache.rs`、`src/chunk/cache_integrity.rs`、`src/cadapter/*`、`src/config.rs`、`src/vfs/cache/config.rs`，以及写入聚合路径中与对象 PUT 数量直接相关的 `src/chunk/writer.rs` / `src/vfs/io/writer.rs`。

## 模块现状摘要

- 对象数据以 `chunks/{slice_id}/{block_index}` 为 key 存储。`DataUploader::write_at_vectored_with_priority` 会按 block 切分 slice，每个 block 调 `BlockStore::write_fresh_vectored`，对象层仍是一 block 一 PUT。
- `ObjectBlockStore` 默认 4MiB block、`compression=Lz4`、64KiB page cache。写入时先拼出完整未压缩 block，再压缩并上传；上传后同步放入 hot cache，磁盘 clean cache 异步 best-effort 落盘。
- S3 adapter 支持 direct PUT、vectored streaming PUT、multipart upload、path-style endpoint、payload checksum disable；默认 `part_size=16MiB`、`max_concurrency=32`、`disable_payload_checksum=true`。
- 本地 clean disk cache 使用 SHA256(key) 文件名，Full 模式写 `[SFC1 header][data][crc32c per 32KiB]`，读时整文件读入并校验；`cache.verify_cache_checksum=none/full` 已有配置 knob，默认 Full。
- 当前 perf 现象必须作为优化约束：`randrw` 下 S3 PUT ops 可到数万，GET avg 约 7-11ms，PUT avg 约 12-22ms；曾尝试 checksum none 后 compose 变差，不能把 none 当默认；lz4 在 flamegraph 中是 CPU 热点，但可能减少 S3/磁盘 IO，不能仅凭热点关闭。

## 具体问题 / 风险 / 性能瓶颈

### P1: randrw 小对象 PUT 放大仍是主瓶颈

- 位置：`src/chunk/writer.rs:153-175` `DataUploader::write_at_vectored_with_priority`；`src/chunk/store.rs:587-640` `ObjectBlockStore::write_fresh_vectored_inner`。
- 原因：写入聚合到 slice 后，最终仍按 block 产生对象。`randrw` 的 4MiB IO 与 4MiB block 对齐时，几乎每次写都变成一个新 slice block PUT；非对齐写还会给对象前部补零并上传一个更大的对象。已知 PUT avg 12-22ms，PUT ops 数万时，吞吐上限会被 RTT/服务端对象创建开销卡住。
- 建议改法：优先做写入批量化实验，而不是只调 S3 multipart。可以尝试扩大 `dirty_slice_target_size/freeze_min_bytes/auto_flush_max_age` 的 workload profile；更进一步可设计 slice-pack/segment-pack，把多个小 block 以 manifest 或 framed bundle 合并成较少对象，同时保留 block 索引。
- 验证方式：在 `fio-randrw` 记录 `.stats` 中 `s3_put_ops/s3_put_bytes/avg_s3_put_lat_us`，目标是 PUT ops 至少下降 2-4 倍，同时 randrw write BW 提升且 p99 不恶化超过 25%；用 xfstests `generic/013/113/438` 回归 flush/fsync/rename/truncate 语义。

### P1: `s3_max_concurrency` 只控制 multipart part 并发，不能控制大量小 PUT 的总并发

- 位置：`src/cadapter/s3.rs:399-453` `multipart_upload_vectored`；`src/cadapter/s3.rs:517-528` `put_object_vectored`；`src/chunk/writer.rs:14-21` 上传 permit。
- 原因：默认 4MiB block 小于默认 `part_size=16MiB`，因此 `put_object_vectored` 走 simple PUT，`max_concurrency=32` 不参与限流。真正的小 PUT 并发来自 `FG_UPLOAD_PERMITS=192` 和上层任务数量，可能让 RustFS/SDK 连接池/FD 压力突增，也可能造成延迟排队。
- 建议改法：增加对象后端级别的全局 GET/PUT semaphore，默认可按 endpoint profile 配置，例如 RustFS 本地 32-64、远端 S3 64-128，并将 foreground/background/writeback 分池或带权重调度。`s3_max_concurrency` 文档需说明它是 multipart part 并发，另设 `s3_put_concurrency`。
- 验证方式：压测 `fio-randwrite/fio-randrw`，绘制 PUT 并发、PUT avg/p95/p99、FD 数、RustFS 5xx/timeout；确保降低并发后吞吐不降或 p99 明显改善。

### P1: 写入路径仍有完整 block 拼接和压缩前内存放大

- 位置：`src/chunk/store.rs:600-620` `write_fresh_vectored_inner`。
- 原因：虽然接口叫 vectored，实际会构造 `parts`，再 `flat_map(...).collect::<Vec<_>>()` 生成 `full_block`，压缩后又生成 `upload_bytes`。对 4MiB block、192 并发上传，瞬时内存和 CPU copy 可观；offset>0 还通过 `make_zero_bytes` 补零，进一步放大。
- 建议改法：短期拆出 fast path：`offset==0 && compression=None` 时直接把原 chunks 传到 `put_object_vectored` 并用同一份 Bytes 填 hot cache；`compression=Lz4` 时考虑 streaming/chunked compressor 或至少避免先补零成多个小 Bytes 再二次 collect。长期让 cache 和 upload 分别消费共享 Bytes/framed representation。
- 验证方式：用 `perf top/flamegraph` 看 `memcpy/alloc/lz4_flex` 占比；在 `.stats` 增加或临时 trace `put_prepare_lat_us`，目标是 avg prepare latency 明显下降，RSS 峰值下降。

### P1: 压缩默认 LZ4 的收益没有按 workload 分流

- 位置：`src/vfs/cache/config.rs:90` 默认 `compression=Lz4`；`src/chunk/compress.rs:50-93`；`src/chunk/store.rs:616`。
- 原因：LZ4 在 flamegraph 中有 CPU 热点。`compress()` 会先完整压缩，再比较是否变小；对不可压缩 randrw 数据，仍付出压缩 CPU 后才回退 raw。另一方面，LZ4 可能降低 PUT bytes 和磁盘 cache bytes，直接关闭可能让 IO 变差。
- 建议改法：不要改默认前先加观测：记录 `compression_attempts/compressed_blocks/raw_fallback_blocks/compress_us/saved_bytes`。再做启发式 profile：小于阈值或随机高熵样本跳过压缩；或提供 perf profile `compression=none/lz4` A/B，不把 none 默认化。
- 验证方式：同一 artifact 跑 `compression=lz4` 与 `none`，比较 CPU%、PUT bytes、PUT ops、GET/PUT latency、randrw read/write BW。只有在 CPU 下降带来的吞吐收益超过 IO 增量时才考虑调整默认。

### P1: compression=Lz4 时小范围 S3 Range GET 路径被禁用

- 位置：`src/chunk/store.rs:697-813` 小范围 range read 条件要求 `Compression::None`；`src/chunk/store.rs:815-878` 压缩时只能 full GET 后解压。
- 原因：压缩对象不能按明文 offset 做 S3 Range GET，所以默认 LZ4 下 4KiB/64KiB 随机读会退化为 full object GET + 解压。当前 GET avg 7-11ms，full GET 还叠加更多 bytes 和解压成本；page cache 只有 full read 后才铺页。
- 建议改法：评估两条路线：一是只对读密集/随机读 profile 使用 `compression=none`；二是设计 block 内分段压缩/framed index，使 64KiB/256KiB 子块可 range GET 并独立解压。后者改动大但更符合对象存储随机读。
- 验证方式：新增压缩/非压缩下 4KiB、64KiB、1MiB randread 微基准，记录 `read_range_gets/read_full_gets/get_bytes/read_page_cache_hits`；验证压缩分段不会破坏现有 `decompress()` 兼容。

### P2: disk cache Full 校验每次整文件读 + 二次拷贝，成本不可见

- 位置：`src/chunk/cache.rs:510-566` `DiskStorage::load`；`src/chunk/cache_integrity.rs:81-117` `decode`。
- 原因：磁盘缓存命中时先 `tokio::fs::read` 把完整文件读入 Vec，再 `decode()` 校验并 `data.to_vec()` 返回。Full 模式保护了本地缓存正确性，但读路径多一次内存复制和 CRC32C；none 模式虽然省校验，但历史上 compose 变差，说明不能盲目默认关闭。
- 建议改法：保留 Full 默认；增加可观测指标 `disk_cache_load_ops/load_us/verify_us/corrupt_drops`。优化实现上可让 Full decode 返回 `Bytes`/slice-backed data，或用 `read_exact_at` + mmap/BufReader 分段校验，避免整块二次复制。
- 验证方式：跑 `fio-randread` 热 cache、冷 cache、compose perf 对比 Full/None；确认 Full 优化后 checksum none 不再是唯一省 CPU 手段。

### P2: disk cache 落盘 frame 是 header/data/checksums，但写入仍不是真正 vectored file IO

- 位置：`src/chunk/cache.rs:383-420` `DiskStorage::store_with_permit`；`src/chunk/cache_integrity.rs:58-76`。
- 原因：`compute_framing()` 已分离 header/checksum，避免复制 data，但 `store_with_permit` 在 blocking task 内连续 `write_all(header)`, `write_all(data)`, `write_all(checksums)`。系统调用次数固定为 3，通常不大；但在高 cache write 并发下仍有额外调度和 flush/rename 开销，且没有记录落盘耗时。
- 建议改法：用 `write_vectored` 一次提交 header/data/checksums，或按大块 buffered writer；同时记录 disk cache store latency 与 skipped count。保留 atomic rename。
- 验证方式：热写 + 热读场景观察 `put_cache_lat_us`、disk write queue、cache hit latency；确认 `insert_opportunistic` 的 background 落盘不抢 foreground PUT。

### P2: disk cache eviction 基于 atime 扫目录，规模大时可能抖动

- 位置：`src/chunk/cache.rs:455-508` `evict_lru`；`src/chunk/cache.rs:528-554` load 后 `utimensat` touch atime。
- 原因：每次超预算会扫描目录、读 metadata、按 atime 排序并删除。缓存文件数多时，单次 eviction O(N log N)，还和正常 load/store 竞争同一磁盘。很多挂载/容器默认 noatime/relatime，atime 语义也可能不稳定，代码又手动 touch atime，增加 syscall。
- 建议改法：维护内存中的 LRU/size index，后台批量回收到 low watermark；或按分片目录 + journal 记录访问时间。至少将 eviction 耗时和删除数量暴露到 stats。
- 验证方式：构造超过 `read_ssd_bytes` 的 cache 压测，记录 cache store p99、eviction 时长、读命中延迟；检查 noatime/relatime 环境下淘汰是否符合预期。

### P2: S3 retry 策略过低，尾延迟和瞬时错误下容易把上层写路径推入重试

- 位置：`src/cadapter/s3.rs:48-61` 默认 `max_retries=1`；`src/cadapter/s3.rs:185-193`、`439-447`、`621-638`。
- 原因：S3 adapter 默认只尝试一次，瞬时 RustFS/S3 抖动会直接冒泡到 `writer.rs` 的 upload retry。上层会重跑整批 block，可能重复压缩/重复 PUT，扩大 randrw 的尾延迟。
- 建议改法：区分幂等 PUT fresh object 与 delete/get，给 5xx、timeout、connection reset 做短退避重试；重试预算要与 writer 的 `UPLOAD_MAX_RETRIES` 协同，避免两层指数放大。
- 验证方式：用 toxiproxy/netem 注入 1%-5% timeout/5xx，比较成功率、重复 PUT、flush p99；确保不可重试错误快速失败。

### P2: S3 GET 全对象读取没有预分配 content length，可能多次扩容

- 位置：`src/cadapter/s3.rs:542-559` `get_object`；`src/cadapter/s3.rs:586-600` `get_object_range`。
- 原因：`get_object` 使用 `Vec::new()` + `read_to_end`，对 4MiB block 会随 body 增长扩容；range path 虽然读入 caller buf，但没有处理 HTTP 206/Content-Range 验证和短读诊断。
- 建议改法：从 S3 response 的 `content_length` 预分配 Vec；range GET 校验返回长度、记录短读/404，并考虑使用 SDK ByteStream collect 的 bytes API 做更少拷贝。
- 验证方式：full GET 微基准观察 allocation 次数和 `get_lat_us`；对 offset 越界、对象短读、RustFS 206 响应做集成测试。

### P2: S3/RustFS 测试覆盖不足，关键兼容测试仍是 ignored/manual

- 位置：`src/cadapter/s3.rs:695-734` `rustfs_small_object_streaming_body_compat` ignored；`src/cadapter/tests/test_s3.rs:22-160` 全 ignored；`tests/native_fsstress_redis_rustfs_docker.rs:328-339` 使用临时 RustFS。
- 原因：对象层关键行为（small streaming PUT、range GET、payload checksum disable、path-style endpoint、concurrency）大多依赖手动 ignored 测试。历史上 generic/013 曾被误判为 RustFS small object 问题，说明需要更系统的端到端和 adapter 级诊断。
- 建议改法：保留 ignored live-S3，但增加可在 Docker profile 中一键跑的 RustFS adapter suite：PUT simple/vectored/multipart、range、delete idempotent、checksum flag、并发 100-1000 小对象；将结果纳入 perf artifact。
- 验证方式：`docker compose` 启 RustFS 后跑 adapter suite + `generic/013/113/438`，在 CI 或 nightly 中至少覆盖 RustFS path-style。

### P2: S3 payload checksum 默认关闭与文档示例不一致，生产安全边界需要写清

- 位置：`src/config.rs:388-391` 默认 `s3_disable_payload_checksum=true`；`src/cadapter/s3.rs:122-130` SDK checksum 设置；`doc/operations/configuration.md:56-64` 示例写 `disable_payload_checksum: false`，表格又写默认 true。
- 原因：关闭 SDK payload checksum 对 RustFS/MinIO 可省 CPU，但对跨公网 AWS S3 或合规场景，安全/完整性语义不同。文档示例与实际默认不一致，容易让测试和部署 profile 混淆。
- 建议改法：明确区分 `trusted-local-s3` 与 `aws-s3` profile：本地 RustFS 默认 true，公网/生产推荐显式评估；文档示例与代码默认对齐或注明差异。
- 验证方式：配置解析测试覆盖默认值和 YAML override；用 AWS/MinIO/RustFS 分别跑 basic PUT/GET，确认 checksum setting 不影响兼容。

### P2: 配置项存在但部分写入聚合参数没有从 `CacheConfig` 贯通

- 位置：`src/vfs/cache/config.rs:37-43` 有 `dirty_slice_target_size`、`upload_concurrency`；`src/vfs/config.rs:60-79` `WriteConfig` 有 `freeze_min_bytes/auto_flush_max_age`；`src/main.rs:376-380` 只把 block compression 传入 object store。
- 原因：对象 PUT 放大最相关的写入聚合参数在 `CacheConfig` 中可配置，但需要确认它们是否完整传入 `WriteConfig` 和实际 FileWriter。若配置只存在于 mount YAML 却未影响 writer，就会误导调参。
- 建议改法：审计 `VFSConfig::new_with_cache_config` 到 `FileWriter` 的传递链路，把 `dirty_slice_target_size` 映射到 `freeze_min_bytes`，`dirty_slice_max_age_ms` 映射到 `auto_flush_max_age`，并为 perf profile 显式打印最终值。
- 验证方式：配置解析单测 + 启动日志检查；用不同 `dirty_slice_target_size` 跑 randwrite，期望 slice 平均大小和 PUT ops 随配置变化。

## 最值得优先尝试的 3 个优化方案

### 1. 小对象 PUT 降噪：写入聚合 profile + 对象 PUT 全局限流

- 做法：先不改对象格式，增加/确认 `dirty_slice_target_size=64-128MiB`、`dirty_slice_max_age_ms=1000-2000` 可真实作用于 writer；同时新增 S3 simple PUT 全局 semaphore，默认 RustFS profile 32-64。
- 预期收益：直接降低 randrw 的 PUT ops 或降低 PUT 排队尾延迟。已知 PUT avg 12-22ms，若 PUT ops 从数万降到 1/2 或 p99 降低，randrw write BW 和混合读写抖动会更快体现。
- 回退风险：聚合时间变长可能增加 fsync/close 延迟；限流过低可能降低顺序写吞吐。必须保留默认保守 profile，作为 perf profile/配置开关逐步推进。

### 2. LZ4 自适应：保留默认，但加压缩指标和高熵跳过

- 做法：记录压缩耗时、压缩比、raw fallback 次数和 saved bytes；对明显不可压缩 block 采样跳过 LZ4，或提供 workload profile 在 randrw 中 A/B `lz4` 与 `none`。
- 预期收益：减少 flamegraph 中 LZ4 CPU 热点，同时不牺牲可压缩 workload 的 IO 节省。适合当前“lz4 热但可能省 IO”的不确定状态。
- 回退风险：跳过误判会增加 PUT bytes 和 cache bytes；关闭压缩还会禁用“压缩对象省传输”的收益。以指标驱动，不能把 `none` 设默认。

### 3. disk cache Full 模式降成本，而不是默认 checksum none

- 做法：优化 Full 校验路径，减少 `tokio::fs::read` 后 `decode().to_vec()` 的二次拷贝；加 disk cache verify/store/load latency 指标；保留 `verify_cache_checksum=full` 默认。
- 预期收益：在不牺牲缓存完整性的前提下降低热 cache 读 CPU 和内存带宽。由于 checksum none 曾让 compose 变差，这条比改默认更稳。
- 回退风险：改 framing/zero-copy 容易引入兼容问题；必须保持 legacy raw 文件读取、corrupt 后删除、atomic rename 语义。可先只加指标，再做内部实现优化。

## 建议的验证矩阵

- 基准：`fio-seqwrite`、`fio-randwrite`、`fio-randread`、`fio-randrw`，尤其固定 `fio-randrw` 的 PUT ops、GET avg 7-11ms、PUT avg 12-22ms 作为对照。
- 语义：xfstests `generic/013`、`generic/113`、`generic/438`，覆盖 S3/RustFS、flush/fsync、unlink/rename、并发写。
- 配置 A/B：`compression=lz4/none`、`verify_cache_checksum=full/none`、不同 `dirty_slice_target_size`、不同 S3 PUT concurrency。
- 观测：`.stats` 增加或导出 `put_prepare_lat_us`、`put_cache_lat_us`、`read_range_gets/read_full_gets`、disk cache load/store/verify latency、compression saved bytes。
- 环境：RustFS path-style Docker profile、MinIO 或 AWS S3 兼容检查；确保 endpoint、payload checksum、range GET、small vectored PUT 都被覆盖。

## 并行 agent 补充审查

### P1 补充：range 后台 full-block prefetch 实际会排队等待，不会按注释 drop

- 位置：`src/chunk/store.rs::prefetch_full_block_background` 的 semaphore acquire。
- 发现：当前实现使用 `acquire_owned().await`，semaphore 饱和时任务会堆积等待；只有 semaphore closed 才会计入 dropped。因此 `read_background_prefetch_dropped` 不能反映真实压力，randrw 下后台 full GET 可能持续排队并抢占前台资源。
- 改动建议：改为 `try_acquire_owned()`，拿不到 permit 立即 drop 并计数；同时把硬编码 range prefetch permit 与 `cache.prefetch_concurrency`、前台 read budget 的关系写进配置/文档。
- 验证：对比默认、`range_background_prefetch=false`、try-acquire-drop 三组 `fio-randrw/fio-randread`，观察 read p99、S3 GET bytes、background prefetch total/dropped、cache hit。

### P1 补充：`decompress()` raw fallback 会二次拷贝

- 位置：`src/chunk/compress.rs::decompress`，`ObjectBlockStore::read_range` full GET 后解压路径。
- 发现：默认 LZ4 下不可压缩对象会以 raw fallback 存储，但读取时仍走 `decompress()`，无压缩 header 的 raw data 会 `to_vec()` 返回，形成额外内存复制。对于 randrw 高熵数据，这和“先压缩再回退”共同放大 CPU/RSS。
- 改动建议：让 `decompress` 返回 `Cow<[u8]>` 或 `Bytes`；无 header 时借用/复用原 buffer，压缩对象才分配解压 buffer。短期至少记录 `raw_fallback_blocks` 与 `decompress_raw_copy_us`。
- 验证：高熵数据 `compression=lz4` 下跑 randread/randrw，观察 alloc profile、read CPU、read p99；raw fallback 读应少一次 copy。

### P1 补充：`read_range` 对 zero-length buffer 应防御

- 位置：`ObjectBlockStore::read_range` 使用 `offset + len - 1` 计算 page span。
- 发现：公共 trait 层若传入空 buffer，`len == 0` 会导致下溢风险。虽然上层正常 read 多数不会传 0，但对象层应自防御，避免后续调用者踩边界。
- 改动建议：函数开头增加 `if buf.is_empty() { return Ok(()); }`，并补单测覆盖 zero-length read。
- 验证：单测 `read_range_zero_len_is_noop`，确认不访问对象存储、不更新 GET counter。

### P2 补充：磁盘 cache miss/hit 前多一次 metadata syscall

- 位置：`DiskStorage::load_with_health` 先 `metadata()`，后续 `load()` 再 `read()`。
- 发现：冷 cache miss 或热 cache hit 都多一次 metadata syscall；在高并发 cold-read/randrw 下会给本地磁盘路径增加噪声。
- 改动建议：直接调用 `load()`，在 `NotFound` 时返回 `Ok(None)`；只有需要区分 corrupt/permission 的路径才额外 stat。
- 验证：冷 cache randread 统计 local fs syscall/IOPS、cache miss latency；热 cache hit latency 不应回退。

### P2 补充：eviction 可能扫描到临时文件

- 位置：`DiskStorage::evict_lru` 和 `store_with_permit` 的 `.tmp` 写入/rename。
- 发现：eviction 扫目录时如果没有过滤临时文件，可能把并发写入中的 `.tmp` 文件纳入候选，增加无效 metadata 处理，极端情况下影响正在落盘的 cache write。
- 改动建议：淘汰时跳过 `*.tmp`/lock 文件，或把 tmp 放独立目录；长期维护内存 LRU/size index，避免每次超预算全目录排序。
- 验证：高并发 cache store + 小 SSD budget，确认无 tmp 被删除，cache store failure 不增加，eviction duration 下降或可解释。
