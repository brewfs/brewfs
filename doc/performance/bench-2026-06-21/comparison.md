
# brewfs vs juicefs — measured (redis meta + local-fs object store, 4MiB block)

## Throughput / latency (fio, bs=4m, ioengine=psync)

| workload | brewfs BW MiB/s | juicefs BW MiB/s | BW ratio (jfs/brewfs) | brewfs p99 ms | juicefs p99 ms |
|---|--:|--:|--:|--:|--:|
| seqwrite | 222.6 | 1479.8 | 6.65x | 3271.56 | 5.47 |
| randwrite | 207.6 | 4096.0 | 19.73x | 3305.11 | 4.42 |
| bigwrite | 211.6 | 4983.0 | 23.55x | 2231.37 | 3271.56 |
| seqread | 1084.7 | 2343.2 | 2.16x | 6.26 | 2.61 |
| randread | 804.4 | 1667.8 | 2.07x | 26.61 | 41.16 |
| bigread | 852.6 | 4521.0 | 5.30x | 3305.11 | 16.91 |

## Metadata (ops/s)

| metric | brewfs ops/s | juicefs ops/s | ratio (jfs/brewfs) |
|---|--:|--:|--:|
| create_ops_per_s | 1976.0 | 1515.0 | 0.8x |
| stat_ops_per_s | 21937.0 | 49355.0 | 2.2x |
| open_ops_per_s | 9253.0 | 4574.0 | 0.5x |
| create_n | 5000.0 | 5000.0 | 1.0x |
| stat_n | 25000.0 | 25000.0 | 1.0x |
| open_n | 25000.0 | 25000.0 | 1.0x |

