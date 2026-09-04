import { textPreview, type ArtifactFile } from './archive';

export type PerfMetric = {
  tool: string;
  status: string;
  seconds: number;
  readMiBps?: number;
  writeMiBps?: number;
  totalMiBps?: number;
  readIops?: number;
  writeIops?: number;
  readP99Ms?: number;
  writeP99Ms?: number;
};

function numberValue(value: unknown): number | undefined {
  const parsed = typeof value === 'number' ? value : Number.parseFloat(String(value ?? ''));
  return Number.isFinite(parsed) ? parsed : undefined;
}

function metricFromSummary(tool: string, row: Record<string, string>): PerfMetric {
  return { tool, status: row.status ?? 'unknown', seconds: numberValue(row.seconds) ?? 0 };
}

function parseTsv(value: string): Record<string, string>[] {
  const lines = value.split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) return [];
  const headers = lines[0].split('\t');
  return lines.slice(1).map((line) => {
    const cells = line.split('\t');
    return Object.fromEntries(headers.map((header, index) => [header, cells[index] ?? '']));
  });
}

function p99Ms(op: Record<string, unknown>): number | undefined {
  const percentiles = (op.clat_ns as { percentile?: Record<string, unknown> } | undefined)?.percentile;
  if (!percentiles) return undefined;
  const value = numberValue(percentiles['99.000000'] ?? percentiles['99']);
  return value === undefined ? undefined : value / 1_000_000;
}

function addFioMetric(metric: PerfMetric, data: Record<string, unknown>): PerfMetric {
  const jobs = Array.isArray(data.jobs) ? data.jobs as Record<string, unknown>[] : [];
  const total = { readBytes: 0, writeBytes: 0, readBw: 0, writeBw: 0, readIops: 0, writeIops: 0, readP99: undefined as number | undefined, writeP99: undefined as number | undefined, runtime: 0 };
  for (const job of jobs) {
    for (const kind of ['read', 'write'] as const) {
      const op = (job[kind] ?? {}) as Record<string, unknown>;
      const bytes = numberValue(op.io_bytes) ?? 0;
      const bw = numberValue(op.bw_bytes) ?? 0;
      const iops = numberValue(op.iops) ?? 0;
      if (kind === 'read') { total.readBytes += bytes; total.readBw += bw; total.readIops += iops; total.readP99 = Math.max(total.readP99 ?? 0, p99Ms(op) ?? 0); }
      else { total.writeBytes += bytes; total.writeBw += bw; total.writeIops += iops; total.writeP99 = Math.max(total.writeP99 ?? 0, p99Ms(op) ?? 0); }
      total.runtime = Math.max(total.runtime, numberValue(op.runtime) ?? 0);
    }
    total.runtime = Math.max(total.runtime, numberValue(job.job_runtime) ?? 0);
  }
  const result: PerfMetric = { ...metric };
  if (total.readBw > 0) result.readMiBps = total.readBw / (1024 * 1024);
  if (total.writeBw > 0) result.writeMiBps = total.writeBw / (1024 * 1024);
  if ((result.readMiBps ?? 0) + (result.writeMiBps ?? 0) > 0) result.totalMiBps = (total.readBw + total.writeBw) / (1024 * 1024);
  if (total.readIops > 0) result.readIops = total.readIops;
  if (total.writeIops > 0) result.writeIops = total.writeIops;
  if (total.readP99) result.readP99Ms = total.readP99;
  if (total.writeP99) result.writeP99Ms = total.writeP99;
  if (!result.seconds && total.runtime) result.seconds = total.runtime / 1000;
  return result;
}

function parseFullyDrained(value: string, metrics: Map<string, PerfMetric>): void {
  for (const row of parseTsv(value)) {
    const metric = metrics.get(row.tool);
    if (!metric) continue;
    const read = numberValue(row.read_mib_s);
    const write = numberValue(row.write_mib_s);
    const total = numberValue(row.total_mib_s);
    if (read !== undefined) metric.readMiBps = read;
    if (write !== undefined) metric.writeMiBps = write;
    if (total !== undefined) metric.totalMiBps = total;
  }
}

export async function extractMetrics(files: ArtifactFile[]): Promise<PerfMetric[]> {
  const metrics = new Map<string, PerfMetric>();
  const summary = files.find((file) => /(^|\/)perf-summary\.tsv$/i.test(file.path));
  if (summary) {
    const rows = parseTsv(await textPreview(summary, 2_000_000));
    for (const row of rows) if (row.tool) metrics.set(row.tool, metricFromSummary(row.tool, row));
  }
  for (const file of files.filter((item) => /(^|\/)fio[^\/]*\.json$/i.test(item.path))) {
    try {
      const data = JSON.parse(await textPreview(file, 8_000_000)) as Record<string, unknown>;
      const tool = file.path.split('/').pop()!.replace(/\.json$/i, '');
      metrics.set(tool, addFioMetric(metrics.get(tool) ?? { tool, status: 'unknown', seconds: 0 }, data));
    } catch { /* keep the summary row when a partial artifact is uploaded */ }
  }
  const drained = files.find((file) => /(^|\/)fully-drained-throughput\.tsv$/i.test(file.path));
  if (drained) parseFullyDrained(await textPreview(drained, 2_000_000), metrics);
  return [...metrics.values()].sort((left, right) => left.tool.localeCompare(right.tool));
}
