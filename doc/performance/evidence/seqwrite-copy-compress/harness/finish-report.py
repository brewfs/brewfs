#!/usr/bin/env python3
"""Render final evidence tables without selecting or dropping measurements."""
import datetime
import json
import pathlib
import re
import statistics

import summarize as s

root = s.ROOT
report = pathlib.Path('/home/hxy/REPORT-SEQWRITE.md')
ab = json.loads((root / 'ab-summary.json').read_text())
zero = json.loads((root / 'compressible-summary.json').read_text())
review = json.loads((root / 'guard-review.json').read_text())
audit = json.loads((root / 'final-audit.json').read_text())
correct = json.loads((root / 'correctness/candidate-summary.json').read_text())
lines = ['## 最终验收数据（自动汇总，无轮次筛选）', '',
         '汇总时间：' + datetime.datetime.now(datetime.timezone.utc).isoformat() + '。', '',
         '### 三对正式 compose A/B', '',
         (root / 'ab-table.md').read_text().strip(), '',
         '单位 MiB/s；变化与 CI 是配对 log(B/A) 的几何均值及 Student t 95% 区间，',
         '表内绝对值各自取中位数，不能用中位数比值代替配对估计。', '',
         '| 场景 | A/B 第 1 对 | A/B 第 2 对 | A/B 第 3 对 |',
         '|---|---|---|---|']
for scene in ab['workloads']:
    values = ab['workloads'][scene]['active_mib_s']
    pairs = [f'{a:.6f} / {b:.6f}' for a, b in zip(values['baseline'], values['candidate'])]
    lines.append('| ' + scene + ' | ' + ' | '.join(pairs) + ' |')
lines += ['', '| 场景 | A/B 精确 wall+drain | 候选 active+drain | ≤3% 不退化已证明 |',
          '|---|---|---:|---|']
for scene, values in ab['workloads'].items():
    exact = values['precise_wall_plus_drain_mib_s']
    active = values['active_mib_s']
    lines.append(f"| {scene} | {exact['baseline_median']:.3f} / {exact['candidate_median']:.3f} | "
                 f"{values['effective_active_plus_drain_mib_s']['candidate_median']:.3f} | "
                 f"{'是' if active['non_regression_3pct_established'] else '否'} |")
lines += ['', '| 场景/操作 | A/B p99 total，ms | 配对变化与 95% CI |',
          '|---|---|---|']
for scene, values in ab['workloads'].items():
    for op in ['read', 'write']:
        row = values.get(op + '_p99_ms')
        if row:
            lo, hi = row['paired_95_ci_pct']
            lines.append(f"| {scene}/{op} | {row['baseline_median']:.3f} / {row['candidate_median']:.3f} | "
                         f"{row['paired_geomean_change_pct']:+.2f}% [{lo:+.2f}, {hi:+.2f}]% |")
lines += ['', 'randrw 写入每轮不足 1000 次 IO，其 p99 的尾部样本有限；原始总 IO 数与',
          'p99_sample_sufficient 标记保留在逐臂 JSON，不能据此声称尾延迟已充分验证。']
lines += ['', '### 对象放大与挂载资源', '',
          '| 场景/版本 | PUT/GiB 写入 | 平均 PUT bytes | wire/raw | partial-tail/batch | RSS 峰值 MiB |',
          '|---|---:|---:|---:|---:|---:|']
for scene in ['seqwrite', 'randwrite', 'randrw']:
    for variant in ['baseline', 'candidate']:
        rows = [review['compose'][f'ab-{pair:02d}-{variant}']['workloads']['fio-' + scene]
                for pair in range(1, 4)]
        med = lambda key: statistics.median(row[key] for row in rows)
        ratios = [row['writeback_upload_partial_tail_ops_total'] /
                  row['writeback_upload_batch_ops_total'] if row['writeback_upload_batch_ops_total'] else 0
                  for row in rows]
        rss = statistics.median(row['system_window']['mount_peak_rss_mib'] for row in rows)
        lines.append(f"| {scene}/{variant} | {med('put_per_gib_written'):.3f} | "
                     f"{med('average_put_bytes'):.1f} | {med('upload_bytes_per_written_byte'):.6f} | "
                     f"{statistics.median(ratios):.6f} | {rss:.2f} |")
lines += ['', '各值分别取三轮中位数；对象窗口包括 close 和严格 drain，扣除准备阶段。',
          '最终所有 debt gauge 为零，所有实际 fusermount3 均 status=0，退出进程表无 brewfs mount。', '',
          '### 可压缩全零数据追加对照', '', (root / 'compressible-table.md').read_text().strip(), '',
          '同样三对交错、512 MiB、20 s；zero_buffers=1、scramble_buffers=0、refill_buffers=1。',
          '与不可压缩主测分开报告；隔离微基准与挂载测量各自保留。', '',
          '### Direct IO guard', '',
          '| 场景/版本 | active MiB/s | wall+drain MiB/s | read/write p99，ms | strict drain，s |',
          '|---|---:|---:|---|---:|']
for key, row in sorted(review['direct'].items()):
    lines.append(f"| {key} | {row['active_mib_s']:.3f} | {row['wall_plus_drain_mib_s']:.3f} | "
                 f"{row['p99_ms']['read']:.3f} / {row['p99_ms']['write']:.3f} | {row['drain_seconds']:.3f} |")
lines += ['', '以上为相同挂载配置的 host guard，每场景仅一对；另以仓库 compose runner',
          '执行 direct=1 的 seqwrite/randwrite/randrw，结果如下。两组各自报告，均不作为',
          '独立统计显著性证据。', '',
          '| compose direct 场景 | A/B active MiB/s | A/B 精确 wall+drain MiB/s |',
          '|---|---|---|']
direct = {variant: s.read_arm(root / 'compose' / ('direct-01-' + variant),
                             ['seqwrite', 'randwrite', 'randrw'])['workloads']
          for variant in ['baseline', 'candidate']}
for scene in ['seqwrite', 'randwrite', 'randrw']:
    before, after = direct['baseline'][scene], direct['candidate'][scene]
    lines.append(f"| {scene} | {before['active_mib_s']:.3f} / {after['active_mib_s']:.3f} | "
                 f"{before['precise_wall_plus_drain_mib_s']:.3f} / {after['precise_wall_plus_drain_mib_s']:.3f} |")
lines += ['', '### Broad compose：完整十场景单对', '',
          '两臂均 10/10 工具成功退出、无 invalid 标记、排空后债务为零且正常卸载。',
          '这只能证明运行完成；以下单对性能下降仍是未解决的风险。bigread/bigwrite',
          '为 8 jobs × 512 MiB 的完整 4 GiB 一次性传输，其余 fio 为 20 秒。', '',
          '| 场景 | A/B active MiB/s | 变化 | A/B 精确 wall+drain MiB/s |',
          '|---|---|---:|---|']
scenes = ['seqread', 'seqwrite', 'randread', 'randwrite', 'randrw', 'bigread', 'bigwrite']
broad = {variant: s.read_arm(root / 'compose' / ('broad-01-' + variant), scenes)['workloads']
         for variant in ['baseline', 'candidate']}
for scene in scenes:
    before, after = broad['baseline'][scene], broad['candidate'][scene]
    change = (after['active_mib_s'] / before['active_mib_s'] - 1) * 100
    lines.append(f"| {scene} | {before['active_mib_s']:.3f} / {after['active_mib_s']:.3f} | "
                 f"{change:+.2f}% | {before['precise_wall_plus_drain_mib_s']:.3f} / "
                 f"{after['precise_wall_plus_drain_mib_s']:.3f} |")
lines += ['', 'seqread、bigread 与 randrw 的下降尚未定位，不能由先前三对中位数上升',
          '来抵消，也不能归因于噪声后丢弃。bigread p99 命中 fio 直方图上限',
          '17,112.760 ms，不能据两臂 p99 相同推断真实尾延迟相同。', '',
          '| 冷读场景/版本 | GET/GiB 逻辑读 | GET bytes / 逻辑读 bytes | mount 平均 CPU 核 |',
          '|---|---:|---:|---:|']
for scene in ['seqread', 'bigread', 'randread', 'randrw']:
    for variant in ['baseline', 'candidate']:
        item = 'fio-' + scene
        metrics = s.reader.load_artifact(root / 'compose' / ('broad-01-' + variant))
        read_mib = metrics['fio', item, 'read_io_mib'].value
        observed = review['compose']['broad-01-' + variant]['workloads'][item]
        lines.append(f"| {scene}/{variant} | {observed['get_ops'] * 1024 / read_mib:.3f} | "
                     f"{observed['get_bytes'] / (read_mib * (1 << 20)):.3f} | "
                     f"{observed['system_window']['mount_mean_cpu_cores']:.3f} |")
lines += ['', '在无读缓存配置下，顺序读仍有约 7 倍传输放大、随机读约 900 倍；',
          '候选 GET 字节放大稍降，但 seqread/bigread 的每 GiB 请求数上升。新的 raw',
          '选择会影响现有 range/full-decode 分支，尚需分离编码布局、FUSE 分片与后端',
          '状态的贡献。Broad bigread 两臂 host IO PSI some 均约 63%，full 约 41–42%；',
          '这些是诊断线索，不能据此豁免读回归或认定其根因。', '',
          '| metaperf 操作 | A/B ops/s | 变化 |', '|---|---|---:|']
metadata = {}
for variant in ['baseline', 'candidate']:
    text = (root / 'compose' / ('broad-01-' + variant) / 'tools/metaperf.log').read_text()
    metadata[variant] = {name: float(rate) for name, rate in re.findall(
        r'^(\w+):.*ops/sec=([\d.]+)', text, re.M)}
for name, before in metadata['baseline'].items():
    after = metadata['candidate'][name]
    lines.append(f'| {name} | {before:.3f} / {after:.3f} | {(after / before - 1) * 100:+.2f}% |')
lines += ['', 'metaperf create 单对 -2.23%，其余操作上升；这里只报告观察值，没有',
          '把单对当作 ≤3% 的统计证明。metadata cache 与 Redis commandstats 保留于',
          'guard-review.json 和原始 diagnostics；完整对照为 broad-comparison.txt。']
lines += ['',
          '### 完整内容与崩溃恢复', '',
          '| 场景 | 校验字节 | 4 KiB 块数 | 整文件 SHA-256 |', '|---|---:|---:|---|']
for row in correct['checks']:
    lines.append(f"| {row['label']} | {row['bytes']} | {row['blocks']} | `{row['whole_sha256']}` |")
lines += ['', '四个场景分别写入 512 MiB，全部逐块 SHA-256 和整文件 SHA-256 一致，',
          '总计 2 GiB、524288 块；buffered/direct × fsync/close，持久性边界后立即',
          'SIGKILL，不插入 drain、等待或验证读取。重启使用新挂载和新缓存目录。',
          '空文件、单字节文件同时验证；八次清理均无残留挂载。散列列表与 SIGKILL/卸载',
          '日志保留在原始 correctness 目录，列表哈希见 guard-review.json。', '',
          '### 最低本地 CI 与 overlay 全套', '',
          '| 门禁 | exit | 命令 |', '|---|---:|---|']
for name, gate in audit['gates'].items():
    lines.append(f"| {name} | {gate['status']} | `{gate['command'] or '完整 CI 流水线'}` |")
lines += ['', '### 硬指标逐项判断', '', '| 检查 | 达标 | 实测/限制 |', '|---|---|---|']
for name, row in audit['checks'].items():
    details = json.dumps({key: value for key, value in row.items() if key != 'passed'}, ensure_ascii=False)
    lines.append(f"| {name} | {'是' if row['passed'] else '否'} | `{details}` |")
lines += ['', '完整原始目录：`/home/hxy/perf-seqwrite-20261102/`；提交保留紧凑证据、',
          '两份火焰图和 folded、原始 fio JSON、精确 phase 时间戳、协议和复现脚本。',
          '脚本中的绝对路径与独立服务端口需按环境调整，依赖 Python PyYAML/boto3、',
          'Docker Compose、fio、perf 与 inferno。历史基线和已接受 README 表均未改写。', '']
lines += ['### 发布判断', '',
          '**保持 draft，不转 ready、不合并。** CPU、顺序写 active 和包含严格排空的',
          '墙钟带宽已达标，完整内容与 SIGKILL 后重启读回通过；正式三对的 randread、',
          'seqread、randrw 95% CI 仍不能排除超过 3% 的吞吐回归，randrw read p99',
          '95% CI 上界 +3.75% 也超过 3%。Broad 单对的 seqread -32.86%、bigread',
          '-9.23%、randrw -5.76% 需要进一步诊断。样本量、冷读/后端状态和可压缩',
          '数据微基准风险尚未充分解释；本轮保留候选供 review，不宣称优化已接受。', '']
marker = '## 最终验收数据（自动汇总，无轮次筛选）'
existing = report.read_text().split(marker)[0].rstrip()
report.write_text(existing + '\n\n' + '\n'.join(lines))
print('Rendered', report)
