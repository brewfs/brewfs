import {
  AlertTriangle,
  Activity,
  Archive,
  BarChart3,
  CheckCircle2,
  ChevronRight,
  Clock3,
  Database,
  Download,
  FileText,
  FolderOpen,
  HardDrive,
  HelpCircle,
  LayoutDashboard,
  ListChecks,
  Search,
  Trash2,
  UploadCloud,
} from 'lucide-react';
import { useEffect, useMemo, useRef, useState, type ChangeEvent, type DragEvent } from 'react';
import { isZip, makeZip, readDirectory, readZip, textPreview, type ArtifactFile } from './archive';
import { deleteServerRun, listServerRuns, uploadServerRun } from './serverApi';
import { extractEnvironment, extractMetrics, type PerfMetric } from './metrics';
import { deleteRun, listRuns, saveRun, type ResultRun, type RunStatus } from './store';
import './styles.css';

const dateFormatter = new Intl.DateTimeFormat('zh-CN', {
  year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit',
});

type Page = 'overview' | 'runs' | 'compare';

const expectedTools = [
  'fio-bigwrite', 'fio-bigread', 'fio-seqread', 'fio-seqwrite', 'fio-randread',
  'fio-randwrite', 'fio-randrw', 'dirstress', 'dirperf', 'metaperf', 'looptest',
];

function pageFromHash(): Page {
  const value = window.location.hash.replace(/^#\/?/, '');
  return value === 'runs' || value === 'compare' ? value : 'overview';
}

function formatDate(timestamp: number): string {
  return timestamp ? dateFormatter.format(timestamp) : '未知';
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  const units = ['KiB', 'MiB', 'GiB', 'TiB'];
  let value = bytes;
  let unit = -1;
  while (value >= 1024 && unit < units.length - 1) { value /= 1024; unit += 1; }
  return `${value.toFixed(value >= 10 ? 0 : 1)} ${units[unit]}`;
}

function inferBackend(files: ArtifactFile[]): ResultRun['backend'] {
  const text = files.map((file) => file.path.toLowerCase()).join(' ');
  if (text.includes('tikv') || text.includes('pd-')) return 'tikv';
  if (text.includes('redis')) return 'redis';
  return 'unknown';
}

function inferDataBackend(files: ArtifactFile[]): ResultRun['dataBackend'] {
  const text = files.map((file) => file.path.toLowerCase()).join(' ');
  if (text.includes('s3') || text.includes('rustfs')) return 's3';
  if (text.includes('local-fs') || text.includes('local_fs')) return 'local-fs';
  return 'unknown';
}

async function inferStatus(files: ArtifactFile[]): Promise<RunStatus> {
  const report = files.find((file) => /(^|\/)report\.md$/i.test(file.path));
  if (!report) return 'unknown';
  const content = (await textPreview(report, 1_000_000)).toLowerCase();
  if (/\b(failed|failure|error)\b/.test(content) && !/0\s+(failed|failure|error)/.test(content)) return 'attention';
  if (/\b(pass|passed|success|succeeded)\b/.test(content)) return 'pass';
  return 'unknown';
}

function runName(sourceName: string, files: ArtifactFile[]): string {
  const top = files[0]?.path.split('/')[0];
  if (top && top !== files[0]?.path && !top.includes('.')) return top;
  return sourceName.replace(/\.zip$/i, '') || 'BrewFS run';
}

function statusLabel(status: RunStatus): string {
  return status === 'pass' ? '通过' : status === 'attention' ? '需关注' : '未判定';
}

function StatusIcon({ status }: { status: RunStatus }) {
  if (status === 'pass') return <CheckCircle2 className="status-icon pass" size={16} aria-hidden="true" />;
  if (status === 'attention') return <AlertTriangle className="status-icon attention" size={16} aria-hidden="true" />;
  return <HelpCircle className="status-icon unknown" size={16} aria-hidden="true" />;
}

export function App() {
  const [page, setPage] = useState<Page>(pageFromHash);
  const [runs, setRuns] = useState<ResultRun[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [compareIds, setCompareIds] = useState<string[]>([]);
  const [compareMetric, setCompareMetric] = useState<'totalMiBps' | 'seconds' | 'iops'>('totalMiBps');
  const [query, setQuery] = useState('');
  const [backendFilter, setBackendFilter] = useState('all');
  const [statusFilter, setStatusFilter] = useState('all');
  const [selectedPath, setSelectedPath] = useState<string | null>(null);
  const [preview, setPreview] = useState<string | null>(null);
  const [dragging, setDragging] = useState(false);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const fileInput = useRef<HTMLInputElement>(null);
  const directoryInput = useRef<HTMLInputElement>(null);

  useEffect(() => {
    const onHashChange = () => setPage(pageFromHash());
    window.addEventListener('hashchange', onHashChange);
    return () => window.removeEventListener('hashchange', onHashChange);
  }, []);

  function navigate(next: Page) {
    window.location.hash = `/${next}`;
    setPage(next);
  }

  useEffect(() => {
    let cancelled = false;
    Promise.all([listRuns(), listServerRuns().catch(() => [])]).then(([localRuns, serverRuns]) => {
      if (!cancelled) setRuns([...serverRuns, ...localRuns]);
    }).catch((error: unknown) => {
      if (!cancelled) setMessage(error instanceof Error ? error.message : '读取结果失败');
    });
    return () => { cancelled = true; };
  }, []);

  const filteredRuns = useMemo(() => {
    const needle = query.trim().toLowerCase();
    return runs.filter((run) => {
      const matchesText = !needle || [run.name, run.sourceName, run.backend, run.dataBackend].join(' ').toLowerCase().includes(needle);
      const matchesBackend = backendFilter === 'all' || run.backend === backendFilter;
      const matchesStatus = statusFilter === 'all' || run.status === statusFilter;
      return matchesText && matchesBackend && matchesStatus;
    });
  }, [backendFilter, query, runs, statusFilter]);

  const selectedRun = runs.find((run) => run.id === selectedId) ?? filteredRuns[0] ?? null;
  const comparisonRuns = useMemo(() => {
    const ids = selectedRun && !compareIds.includes(selectedRun.id) ? [selectedRun.id, ...compareIds] : compareIds;
    return ids.map((id) => runs.find((run) => run.id === id)).filter((run): run is ResultRun => Boolean(run)).slice(0, 4);
  }, [compareIds, runs, selectedRun]);
  const comparisonMetrics = useMemo(() => {
    const tools = new Set(comparisonRuns.flatMap((run) => (run.metrics ?? []).map((metric) => metric.tool)));
    return [...tools].sort().map((tool) => ({ tool, values: comparisonRuns.map((run) => ({ run, metric: (run.metrics ?? []).find((metric) => metric.tool === tool) })) }));
  }, [comparisonRuns]);
  const visibleFiles = useMemo(() => {
    if (!selectedRun) return [];
    const needle = query.trim().toLowerCase();
    return selectedRun.files.filter((file) => !needle || file.path.toLowerCase().includes(needle));
  }, [query, selectedRun]);
  const selectedFile = selectedRun?.files.find((file) => file.path === selectedPath) ?? null;

  useEffect(() => {
    let cancelled = false;
    setPreview(null);
    if (selectedFile && selectedFile.kind === 'file' && /\.(md|log|txt|tsv|json|yaml|yml|env|out)$/i.test(selectedFile.path)) {
      textPreview(selectedFile).then((value) => { if (!cancelled) setPreview(value); }).catch((error: unknown) => {
        if (!cancelled) setPreview(error instanceof Error ? error.message : '预览失败');
      });
    }
    return () => { cancelled = true; };
  }, [selectedFile]);

  async function importFiles(files: File[]) {
    if (files.length === 0) return;
    setBusy(true); setMessage(null);
    try {
      const source = files.length === 1 && isZip(files[0]) ? files[0] : null;
      const artifactFiles = source ? await readZip(source) : readDirectory(files);
      if (artifactFiles.length === 0) throw new Error('没有在上传内容中找到文件。');
      const uploadedAt = Date.now();
      const mtimes = artifactFiles.map((file) => file.mtime).filter(Boolean);
      const localRun: ResultRun = {
        id: `run-${uploadedAt}-${Math.random().toString(36).slice(2, 8)}`,
        name: runName(source?.name ?? files[0].name, artifactFiles),
        sourceName: source?.name ?? `${files.length} 个文件`,
        backend: inferBackend(artifactFiles),
        dataBackend: inferDataBackend(artifactFiles),
        status: await inferStatus(artifactFiles),
        uploadedAt,
        fileCount: artifactFiles.length,
        totalBytes: artifactFiles.reduce((sum, file) => sum + file.size, 0),
        earliestMtime: Math.min(...mtimes, uploadedAt),
        latestMtime: Math.max(...mtimes, uploadedAt),
        storage: 'browser',
        files: artifactFiles,
        metrics: await extractMetrics(artifactFiles),
        environment: await extractEnvironment(artifactFiles),
      };
      let run = localRun;
      try {
        const archive = source ?? new File([await makeZip(artifactFiles)], `${localRun.name || 'brewfs-run'}.zip`, { type: 'application/zip' });
        run = await uploadServerRun(archive);
        setMessage(`已上传服务器：${run.name}，原始时间戳已保留。`);
      } catch {
        await saveRun(localRun);
        setMessage(`服务器暂不可用，已保存在当前浏览器：${run.name}。`);
      }
      setRuns((current) => [run, ...current.filter((item) => item.id !== run.id)]);
      setSelectedId(run.id); setSelectedPath(run.files.find((file) => file.kind === 'file')?.path ?? null);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : '导入失败');
    } finally { setBusy(false); }
  }

  function onInput(event: ChangeEvent<HTMLInputElement>) {
    void importFiles(Array.from(event.target.files ?? []));
    event.target.value = '';
  }

  function onDrop(event: DragEvent<HTMLDivElement>) {
    event.preventDefault(); setDragging(false); void importFiles(Array.from(event.dataTransfer.files));
  }

  async function downloadRun(run: ResultRun) {
    const blob = run.archiveUrl ? await fetch(run.archiveUrl).then((response) => {
      if (!response.ok) throw new Error('服务器归档下载失败。');
      return response.blob();
    }) : await makeZip(run.files);
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a'); link.href = url; link.download = `${run.name || run.id}.zip`; link.click();
    URL.revokeObjectURL(url);
  }

  async function removeRun(run: ResultRun) {
    if (!window.confirm(`删除本地结果“${run.name}”？`)) return;
    if (run.storage === 'server') await deleteServerRun(run.id); else await deleteRun(run.id);
    setRuns((current) => current.filter((item) => item.id !== run.id));
    setCompareIds((current) => current.filter((id) => id !== run.id));
    if (selectedId === run.id) { setSelectedId(null); setSelectedPath(null); }
  }

  function toggleCompare(id: string) {
    setCompareIds((current) => current.includes(id) ? current.filter((item) => item !== id) : current.length < 4 ? [...current, id] : current);
  }

  function metricValue(metric: PerfMetric | undefined): number | undefined {
    if (!metric) return undefined;
    if (compareMetric === 'seconds') return metric.seconds || undefined;
    if (compareMetric === 'iops') return (metric.readIops ?? 0) + (metric.writeIops ?? 0) || undefined;
    return metric.totalMiBps ?? (((metric.readMiBps ?? 0) + (metric.writeMiBps ?? 0)) || undefined);
  }

  function metricLabel(metric: PerfMetric | undefined): string {
    const value = metricValue(metric);
    if (value === undefined) return '—';
    if (compareMetric === 'seconds') return `${value.toFixed(1)} s`;
    if (compareMetric === 'iops') return `${value.toFixed(0)} IOPS`;
    return `${value.toFixed(1)} MiB/s`;
  }

  const totalBytes = runs.reduce((sum, run) => sum + run.totalBytes, 0);
  const latestRun = runs[0] ?? null;
  const latestMetrics = latestRun?.metrics ?? [];
  const coverage = expectedTools.map((tool) => ({
    tool,
    metric: latestMetrics.find((metric) => metric.tool === tool),
  }));
  const coveredCount = coverage.filter(({ metric }) => Boolean(metric)).length;
  const failedCount = coverage.filter(({ metric }) => metric && !metric.status.startsWith('pass')).length;
  const maxLatestThroughput = Math.max(...latestMetrics.map((metric) => metric.totalMiBps ?? 0), 1);
  const pageTitle = page === 'overview' ? '性能总览' : page === 'runs' ? '测试跑次' : '结果对比';
  const pageLede = page === 'overview'
    ? '快速确认矩阵覆盖率、吞吐、延迟与运行环境。'
    : page === 'runs'
      ? '查看每次 ACK、Compose、xfstests、LTP 和性能测试的完整证据。'
      : '对齐相同工具，比较不同时间、后端或配置的指标。';

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand-mark"><Archive size={21} /><span>BrewFS</span></div>
        <p className="brand-subtitle">RESULT VAULT</p>
        <nav className="side-nav" aria-label="结果页面">
          <button className={page === 'overview' ? 'active' : ''} type="button" onClick={() => navigate('overview')}><LayoutDashboard size={16} />总览</button>
          <button className={page === 'runs' ? 'active' : ''} type="button" onClick={() => navigate('runs')}><ListChecks size={16} />跑次详情</button>
          <button className={page === 'compare' ? 'active' : ''} type="button" onClick={() => navigate('compare')}><BarChart3 size={16} />结果对比</button>
        </nav>
        <div className="side-note"><Clock3 size={15} /><span>本地优先<br />时间戳可追溯</span></div>
        <div className="side-footer">{runs.length} RUNS STORED<br /><span>server + browser fallback</span></div>
      </aside>
      <main className="workspace">
        <header className="topbar">
          <div><p className="eyebrow">BrewFS / Test evidence</p><h1>{pageTitle}</h1><p className="lede">{pageLede}</p></div>
          <div className="top-actions">
            <button className="secondary-button" type="button" onClick={() => fileInput.current?.click()}><UploadCloud size={16} />上传 ZIP</button>
            <button className="primary-button" type="button" onClick={() => directoryInput.current?.click()}><FolderOpen size={16} />上传目录</button>
            <input ref={fileInput} hidden type="file" accept=".zip,application/zip" onChange={onInput} />
            <input ref={directoryInput} hidden type="file" multiple onChange={onInput} {...({ webkitdirectory: '', directory: '' } as Record<string, string>)} />
          </div>
        </header>

        {page === 'overview' ? <><section className={`drop-zone ${dragging ? 'dragging' : ''}`} onDragOver={(event) => { event.preventDefault(); setDragging(true); }} onDragLeave={() => setDragging(false)} onDrop={onDrop}>
          <UploadCloud size={27} />
          <div><strong>{busy ? '正在解析并上传…' : '拖放结果 ZIP 到这里'}</strong><span>支持脚本导出的 .zip，也支持直接选择结果目录；所有文件原始 mtime 会被保存。</span></div>
          <button className="text-button" type="button" onClick={() => fileInput.current?.click()}>选择文件 <ChevronRight size={15} /></button>
        </section>
        {message ? <div className="notice">{message}</div> : null}

        <section className="stats-grid">
          <div className="stat-card"><span>RUNS</span><strong>{runs.length}</strong><small>本地已保存</small></div>
          <div className="stat-card"><span>FILES</span><strong>{runs.reduce((sum, run) => sum + run.fileCount, 0).toLocaleString()}</strong><small>含原始元数据</small></div>
          <div className="stat-card"><span>STORAGE</span><strong>{formatBytes(totalBytes)}</strong><small>服务器 / IndexedDB</small></div>
          <div className="stat-card accent"><span>LAST IMPORT</span><strong>{runs[0] ? formatDate(runs[0].uploadedAt).slice(5, 16) : '—'}</strong><small>{runs[0]?.name ?? '等待第一次上传'}</small></div>
        </section>

        <section className="overview-grid">
          <article className="panel overview-performance">
            <div className="panel-heading"><div><p className="eyebrow">LATEST RUN</p><h2>{latestRun?.name ?? '暂无跑次'}</h2></div>{latestRun ? <span className={`coverage-badge ${failedCount ? 'attention' : coveredCount === expectedTools.length ? 'complete' : 'partial'}`}>{coveredCount}/{expectedTools.length} 覆盖</span> : null}</div>
            {latestRun && latestMetrics.length ? <div className="throughput-list">{latestMetrics.filter((metric) => metric.totalMiBps).map((metric) => <div className="throughput-row" key={metric.tool}><div><strong>{metric.tool}</strong><span>{metric.totalMiBps?.toFixed(1)} MiB/s</span></div><div className="throughput-track"><div style={{ width: `${((metric.totalMiBps ?? 0) / maxLatestThroughput) * 100}%` }} /></div><small>{metric.readP99Ms || metric.writeP99Ms ? `p99 ${(metric.readP99Ms ?? metric.writeP99Ms)?.toFixed(1)} ms` : '无延迟数据'}</small></div>)}</div> : <p className="overview-empty">上传包含 fio JSON 的结果后显示吞吐与延迟。</p>}
          </article>
          <article className="panel coverage-panel">
            <div className="panel-heading"><div><p className="eyebrow">MATRIX</p><h2>完整矩阵覆盖</h2></div><span className="result-count">{failedCount} 失败</span></div>
            <div className="coverage-list">{coverage.map(({ tool, metric }) => <div className="coverage-row" key={tool}><span>{tool}</span><strong className={!metric ? 'missing' : metric.status.startsWith('pass') ? 'pass' : 'attention'}>{!metric ? '未采集' : metric.status.startsWith('pass') ? '通过' : '失败'}</strong><small>{metric?.seconds ? `${metric.seconds.toFixed(0)} s` : '—'}</small></div>)}</div>
          </article>
          <article className="panel environment-panel">
            <div className="panel-heading"><div><p className="eyebrow">ENVIRONMENT</p><h2>运行环境</h2></div><Activity size={18} /></div>
            {latestRun?.environment && Object.keys(latestRun.environment).length ? <dl className="environment-list"><div><dt>实例规格</dt><dd>{latestRun.environment.instanceType ?? '—'}</dd></div><div><dt>CPU / 内存</dt><dd>{latestRun.environment.cpuCapacity ?? '—'} / {latestRun.environment.memoryCapacity ?? '—'}</dd></div><div><dt>临时存储</dt><dd>{latestRun.environment.ephemeralStorageCapacity ?? '—'}</dd></div><div><dt>Kubernetes</dt><dd>{latestRun.environment.kubeletVersion ?? '—'}</dd></div><div><dt>镜像</dt><dd>{latestRun.environment.image ?? '—'}</dd></div></dl> : <p className="overview-empty">旧跑次没有环境快照；新 ACK 脚本会自动写入节点规格。</p>}
          </article>
          <article className="panel recent-panel">
            <div className="panel-heading"><div><p className="eyebrow">HISTORY</p><h2>最近跑次</h2></div><button className="text-button" type="button" onClick={() => navigate('runs')}>查看全部 <ChevronRight size={15} /></button></div>
            <div className="recent-list">{runs.slice(0, 5).map((run) => <button type="button" key={run.id} onClick={() => { setSelectedId(run.id); navigate('runs'); }}><StatusIcon status={run.status} /><span><strong>{run.name}</strong><small>{formatDate(run.uploadedAt)} · {run.metrics?.length ?? 0} metrics</small></span><ChevronRight size={15} /></button>)}</div>
          </article>
        </section></> : null}

        {page === 'runs' ? <section className="content-grid">
          <article className="panel runs-panel">
            <div className="panel-heading"><div><p className="eyebrow">INDEX</p><h2>测试跑次</h2></div><span className="result-count">{filteredRuns.length} / {runs.length}</span></div>
            <div className="filters"><label className="search-box"><Search size={16} /><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="搜索名称、文件、后端…" /></label><select value={backendFilter} onChange={(event) => setBackendFilter(event.target.value)}><option value="all">全部后端</option><option value="redis">Redis</option><option value="tikv">TiKV</option><option value="unknown">未识别</option></select><select value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}><option value="all">全部状态</option><option value="pass">通过</option><option value="attention">需关注</option><option value="unknown">未判定</option></select></div>
            {filteredRuns.length === 0 ? <div className="empty-state"><Archive size={29} /><strong>还没有匹配的跑次</strong><span>上传 ACK 导出的 zip，结果会保存在服务器（离线时回退到浏览器）。</span></div> : <div className="run-list">{filteredRuns.map((run) => <div key={run.id} className={`run-row-wrap ${selectedRun?.id === run.id ? 'selected' : ''}`}><button className={`run-row ${selectedRun?.id === run.id ? 'selected' : ''}`} type="button" onClick={() => { setSelectedId(run.id); setSelectedPath(run.files.find((file) => file.kind === 'file')?.path ?? null); }}><div className="run-icon"><StatusIcon status={run.status} /></div><div className="run-main"><strong>{run.name}</strong><span>{run.backend.toUpperCase()} · {run.dataBackend} · {run.fileCount} files{run.metrics?.length ? ` · ${run.metrics.length} metrics` : ''}</span></div><div className="run-time"><span>{formatDate(run.uploadedAt)}</span><small>{statusLabel(run.status)}</small></div><ChevronRight size={17} className="row-chevron" /></button><label className="compare-toggle"><input type="checkbox" checked={compareIds.includes(run.id)} onChange={() => toggleCompare(run.id)} />对比</label></div>)}</div>}
          </article>

          <article className="panel detail-panel">
            {!selectedRun ? <div className="empty-state detail-empty"><HardDrive size={31} /><strong>选择一个测试跑次</strong><span>报告、日志和诊断文件会在这里展示。</span></div> : <>
              <div className="panel-heading detail-heading"><div><p className="eyebrow">RUN DETAIL</p><h2>{selectedRun.name}</h2></div><div className="detail-actions"><button className="icon-button" type="button" title="下载为 ZIP" onClick={() => void downloadRun(selectedRun)}><Download size={16} /></button><button className="icon-button danger" type="button" title="删除本地结果" onClick={() => void removeRun(selectedRun)}><Trash2 size={16} /></button></div></div>
              <div className="run-meta"><span><Database size={14} />{selectedRun.backend}</span><span><HardDrive size={14} />{selectedRun.dataBackend}</span><span><Clock3 size={14} />上传 {formatDate(selectedRun.uploadedAt)}</span><span><FileText size={14} />{formatBytes(selectedRun.totalBytes)}</span></div>
              <div className="timeline"><div><span>最早文件时间</span><strong>{formatDate(selectedRun.earliestMtime)}</strong></div><div><span>最晚文件时间</span><strong>{formatDate(selectedRun.latestMtime)}</strong></div><div><span>结果状态</span><strong className={`status-text ${selectedRun.status}`}><StatusIcon status={selectedRun.status} />{statusLabel(selectedRun.status)}</strong></div></div>
              <section className="metrics-card"><div className="metrics-heading"><div><p className="eyebrow">PERFORMANCE</p><h3>性能指标</h3></div><span>{selectedRun.metrics?.length ?? 0} 项</span></div>{selectedRun.metrics?.length ? <div className="metric-table-wrap"><div className="metric-table"><div className="metric-row metric-head"><span>工具</span><span>状态</span><span>耗时</span><span>读取</span><span>写入</span><span>IOPS</span><span>p99</span></div>{selectedRun.metrics.map((metric) => <div className="metric-row" key={metric.tool}><span>{metric.tool}</span><span className={metric.status.startsWith('pass') ? 'metric-pass' : 'metric-fail'}>{metric.status}</span><span>{metric.seconds ? `${metric.seconds.toFixed(1)} s` : '—'}</span><span>{metric.readMiBps ? `${metric.readMiBps.toFixed(1)} MiB/s` : '—'}</span><span>{metric.writeMiBps ? `${metric.writeMiBps.toFixed(1)} MiB/s` : '—'}</span><span>{metric.readIops || metric.writeIops ? `${((metric.readIops ?? 0) + (metric.writeIops ?? 0)).toFixed(0)}` : '—'}</span><span>{metric.readP99Ms || metric.writeP99Ms ? `${(metric.readP99Ms ?? metric.writeP99Ms)?.toFixed(1)} ms` : '—'}</span></div>)}</div></div> : <p className="muted">该跑次没有可解析的 perf-summary.tsv 或 fio JSON；下一次使用包含 fio 的性能脚本上传后会自动显示。</p>}</section>
              <div className="file-browser"><div className="file-browser-heading"><strong>Artifacts</strong><span>{visibleFiles.length} files</span></div><div className="file-table">{visibleFiles.map((file) => <button key={file.path} type="button" className={`file-row ${selectedFile?.path === file.path ? 'selected' : ''}`} onClick={() => setSelectedPath(file.path)}><span className="file-name"><FileText size={14} />{file.path}</span><span>{formatBytes(file.size)}</span><span>{formatDate(file.mtime)}</span></button>)}</div></div>
              <div className="preview"><div className="preview-heading"><span>PREVIEW</span>{selectedFile ? <small>{selectedFile.path}</small> : null}</div>{selectedFile && preview !== null ? <pre>{preview}</pre> : <p className="muted">选择 markdown、日志或结果文件查看内容。</p>}</div>
            </>}
          </article>
        </section> : null}
        {page === 'compare' ? <section className="panel comparison-panel"><div className="panel-heading"><div><p className="eyebrow">COMPARE</p><h2>跑次对比</h2></div><div className="comparison-controls"><select value={compareMetric} onChange={(event) => setCompareMetric(event.target.value as typeof compareMetric)}><option value="totalMiBps">吞吐（MiB/s）</option><option value="iops">IOPS</option><option value="seconds">耗时（秒）</option></select><span>最多选择 4 个跑次</span></div></div>{comparisonRuns.length === 0 ? <p className="muted">先到“跑次详情”勾选最多四个跑次。</p> : comparisonMetrics.length === 0 ? <p className="muted">选中的跑次还没有解析到性能指标。</p> : <div className="comparison-chart">{comparisonMetrics.map(({ tool, values }) => { const max = Math.max(...values.map(({ metric }) => metricValue(metric) ?? 0), 1); return <div className="chart-row" key={tool}><strong>{tool}</strong><div className="chart-bars">{values.map(({ run, metric }) => { const value = metricValue(metric); return <div className="chart-bar-line" key={run.id} title={`${run.name}: ${metricLabel(metric)}`}><span>{run.name}</span><div className="chart-track"><div className="chart-bar" style={{ width: `${Math.max(0, Math.min(100, ((value ?? 0) / max) * 100))}%` }} /></div><b>{metricLabel(metric)}</b></div>})}</div></div>})}</div>}</section> : null}
        <footer className="app-footer">BrewFS Result Vault · 默认保存到服务器；服务器不可用时使用当前浏览器作为兜底。</footer>
      </main>
    </div>
  );
}
