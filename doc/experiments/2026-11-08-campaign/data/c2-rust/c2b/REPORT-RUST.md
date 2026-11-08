# C2 final trace — rust replay report

- 生成时间：`2026-11-07T18:00:41+08:00`
- Host：`ax-c1-a-client`（jump 经 SSH 操作）
- Trace：`/srv/ax-data/traces/extracted/rust/`
- Events：`capture/events.jsonl`（1,061,849,599 bytes）
- Blobs：`capture/blobs/`（题目给出的 `rust/blobs` 不存在；实际布局为 `capture/blobs`）
- Replay binary：`/srv/ax-data/artifacts/bin/full-fuse-replay`
- 隔离目录：`/srv/ax-data/c2b/`；未触碰 c1/c2/c3 的 brewfs、c1c2 数据、c3 数据或挂载。

## 最终结论

**PASS**（按任务指定的 pytest 同标准）

UID 1000 + 将干净 initial 副本整体 chown 到 `1000:1000` 后，strict 重放完整通过：

- `completed=true`
- `operations=3,122,195`，等于 metrics `callbacks=3,122,195`
- `remaining_handles=0`
- `actual_read_bytes=4,391,117,721`，等于 capture metrics `actual_read_bytes=4,391,117,721`
- `actual_write_bytes=1,178,081,436`，等于 capture metrics `actual_write_bytes=1,178,081,436`
- `probe_errno_differences=0`
- `expected_errors=743,429`，`valid=true`，退出码 0

没有伪造通过：原始工具 JSON 与 `time -v` 输出均原样保留；root 跑的早期 strict mismatch 也原样保留。

## 两种跑法对比

| 跑法 | 结果 | operations | read_bytes | write_bytes | wall | errno 差异 | remaining_handles |
|---|---|---:|---:|---:|---:|---|---:|
| UID 1000（initial 副本 chown 1000:1000） | PASS | 3,122,195 (= callbacks) | 4,391,117,721 (match) | 1,178,081,436 (match) | 0:46.05（execution 46.054240571 s） | `probe_errno_differences=0`；expected_errors=743,429 | 0 |
| root（独立干净 initial 副本） | strict 早停 FAIL | 处理至 seq 91,065；最后一个周期进度点为 65,536 | 周期点 34,397；早停后无最终值 | 周期点 13,261,430；早停后无最终值 | 0:01.05 | seq 91,065 的 chown 期望 errno 1，实际成功；因 strict 早停，最终 `probe_errno_differences` 未产出 | 早停，未产出 |

说明：root 行的 operations/read/write 是 strict 早停时实际可见值；不把未产出的 handles 或最终字节数伪填为 0。

## root 跑法的不可两全证据

root 独立副本在 `seq=91065` 严格失败，原始 JSON 记录：

- capture 事件：`chown("/state/data/opencode/bin/rg", 1001, 127)`
- capture 事件 `uid=0`、`gid=0`、`pid=952102`、`errno=1`（EPERM）、`result=null`
- root 跑法进程 `uid=0`、`gid=0`
- root 实际结果：`Ok(Number(0))`，即 chown 成功
- 工具报错：`mismatch at 91065: "chown" Ok(Number(0))`
- `completed=false`、`valid=false`、退出码 1

这与已知历史结论一致：root 能看见并修改内容，但会让 capture 预期的 EPERM 变成成功，strict 在 seq 91,065 mismatch。

UID 1000 跑法保持该 chown 返回 EPERM，同时为消除旧 `seq=2,400,930` 的全零读，先在 replay 前把干净 initial 副本递归 chown 到 `1000:1000`。本次该跑法越过 seq 91,065 与 seq 2,400,930，完成全部 3,122,195 callbacks，读写字节和 errno probe 全部一致。

因此本次不存在需要提交的 `PROTOCOL DEVIATION`：最终 UID 1000 跑法没有失败。root 跑法的 seq 91,065 mismatch 是刻意保留的对照证据。

## Setup 与可读性验证

- 干净副本：`/srv/ax-data/c2b/rust-root`，从 `initial/.` 完整复制后 `chown -R 1000:1000`。
- 顶层：`1000:1000 755`。
- 递归检查：root 子树中非 `1000:1000` 条目数为 0。
- 使用 `setpriv --reuid=1000 --regid=1000 --clear-groups` 抽查前 20 个文件：20/20 可 `test -r` 且完整读到 `/dev/null`。
- Replay 后 UID 1000 root 为 9,325 files / 1,445 dirs，约 1,012 MiB；root 对照副本约 34 MiB。
- 磁盘余量结束后：356 G available，未接近 10 GiB 复制上限。

首次启动曾因预先创建输出 json 被工具以 `EEXIST` 拒绝，0 秒退出且未处理 events；随后删除占位、重建干净 root 并正式重跑。最终结果来自本次正式重跑。

## 原始产物与哈希

- PASS JSON：`/srv/ax-data/c2b/rust-replay.json`
  - sha256 `9787335b3ba55114347bafa8d982eb3354793756d75b341eb3606fbc660e9ad8`
- PASS stdout/stderr：`/srv/ax-data/c2b/rust-replay.out`
  - sha256 `73fa56d1c8a1c4eb661366f6a250ceb9a7b4ac3146436f8d94770b9cf3131bf0`
- root 对照 JSON：`/srv/ax-data/c2b/rust-replay-root.json`
  - sha256 `9f9f165011e60f74da699869980660e27247ce0c9f0a227aaf6a889d0e1c648d`
- root 对照日志：`/srv/ax-data/c2b/rust-replay-root.out`
  - sha256 `3cb49516ce54d2cd954aa6df442ef5e862270593e7a2886b5ab4104c5e000f69`
- 判定基准：`/srv/ax-data/traces/extracted/rust/metrics.json`
