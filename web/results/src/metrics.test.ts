import { describe, expect, it } from 'vitest';
import { extractMetrics, statusFromMetrics } from './metrics';
import type { ArtifactFile } from './archive';

function file(path: string, content: string): ArtifactFile {
  return { path, blob: new Blob([content]), size: content.length, mtime: Date.now(), kind: 'file' };
}

describe('performance metrics', () => {
  it('combines perf summary with fio JSON and fully-drained throughput', async () => {
    const fio = JSON.stringify({ jobs: [{ job_runtime: 2000, read: { bw_bytes: 1048576, iops: 4, clat_ns: { percentile: { '99.000000': 2000000 } } }, write: { bw_bytes: 2097152, iops: 8 } }] });
    const metrics = await extractMetrics([
      file('perf-summary.tsv', 'tool\tstatus\tseconds\tlog\nfio-seqwrite\tpass\t3\ttools/fio.log\n'),
      file('results/fio-seqwrite.json', fio),
      file('fully-drained-throughput.tsv', 'tool\tactive_seconds\tdrain_seconds\tcomplete_seconds\tread_bytes\twrite_bytes\tread_mib_s\twrite_mib_s\ttotal_mib_s\nfio-seqwrite\t2\t1\t3\t0\t1\t0\t0.67\t0.67\n'),
    ]);
    expect(metrics[0]).toMatchObject({ tool: 'fio-seqwrite', seconds: 3, readMiBps: 0, writeMiBps: 0.67, totalMiBps: 0.67, writeIops: 8 });
  });

  it('marks a run as passed only when every reported tool passes', () => {
    expect(statusFromMetrics([
      { tool: 'fio-bigread', status: 'pass', seconds: 1 },
      { tool: 'metaperf', status: 'passed', seconds: 2 },
    ])).toBe('pass');
    expect(statusFromMetrics([
      { tool: 'fio-bigread', status: 'pass', seconds: 1 },
      { tool: 'metaperf', status: 'failed', seconds: 2 },
    ])).toBe('attention');
    expect(statusFromMetrics([])).toBeUndefined();
  });
});
