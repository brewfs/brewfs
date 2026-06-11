import {
  Activity,
  Database,
  FileSearch,
  FolderTree,
  Gauge,
  LogIn,
  HardDrive,
  Settings,
  ShieldCheck,
  Trash2,
  type LucideIcon,
} from 'lucide-react';
import { useEffect, useMemo, useState, type FormEvent, type ReactNode } from 'react';
import {
  ApiError,
  createVolume,
  fetchHealth,
  fetchInstanceInfo,
  fetchInstances,
  fetchVolumes,
  type HealthResponse,
  type InstanceInfoResponse,
  type InstanceResponse,
  type VolumeResponse,
} from './api';

type PageKey =
  | 'overview'
  | 'filesystems'
  | 'browser'
  | 'trash'
  | 'acl'
  | 'jobs'
  | 'csi'
  | 'settings';

const navItems: Array<{ key: PageKey; label: string; icon: LucideIcon }> = [
  { key: 'overview', label: 'Overview', icon: Gauge },
  { key: 'filesystems', label: 'Filesystems', icon: HardDrive },
  { key: 'browser', label: 'Browser', icon: FileSearch },
  { key: 'trash', label: 'Trash', icon: Trash2 },
  { key: 'acl', label: 'ACL', icon: ShieldCheck },
  { key: 'jobs', label: 'Jobs', icon: Activity },
  { key: 'csi', label: 'CSI', icon: Database },
  { key: 'settings', label: 'Settings', icon: Settings },
];

const pagePaths: Record<PageKey, string> = {
  overview: '/',
  filesystems: '/filesystems',
  browser: '/browser',
  trash: '/trash',
  acl: '/acl',
  jobs: '/jobs',
  csi: '/csi',
  settings: '/settings',
};

const TOKEN_STORAGE_KEY = 'brewfs.console.token';

type VolumeFormState = {
  name: string;
  mount_point: string;
  data_backend: string;
  data_dir: string;
  meta_backend: string;
  meta_url: string;
};

const emptyVolumeForm: VolumeFormState = {
  name: '',
  mount_point: '',
  data_backend: 'local-fs',
  data_dir: '',
  meta_backend: 'sqlx',
  meta_url: '',
};

function pageTitle(page: PageKey): string {
  return navItems.find((item) => item.key === page)?.label ?? 'Overview';
}

function pageFromPathname(pathname: string): PageKey {
  const normalized = pathname === '/' ? '/' : pathname.replace(/\/+$/, '');
  return (
    (Object.entries(pagePaths).find(([, path]) => path === normalized)?.[0] as PageKey | undefined) ??
    'overview'
  );
}

export function App() {
  const [page, setPage] = useState<PageKey>(() => pageFromPathname(window.location.pathname));
  const [token, setToken] = useState<string>(() => sessionStorage.getItem(TOKEN_STORAGE_KEY) ?? '');
  const [tokenInput, setTokenInput] = useState('');
  const [authRequired, setAuthRequired] = useState(false);
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [instances, setInstances] = useState<InstanceResponse[]>([]);
  const [instanceDetails, setInstanceDetails] = useState<Record<number, InstanceInfoResponse>>({});
  const [instanceError, setInstanceError] = useState<string | null>(null);
  const [volumes, setVolumes] = useState<VolumeResponse[]>([]);
  const [volumeError, setVolumeError] = useState<string | null>(null);
  const [volumeForm, setVolumeForm] = useState<VolumeFormState>(emptyVolumeForm);
  const [creatingVolume, setCreatingVolume] = useState(false);
  const [createVolumeError, setCreateVolumeError] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function loadConsoleData() {
      try {
        const result = await fetchHealth(token);
        if (cancelled) return;
        setHealth(result);
        setError(null);
        setAuthRequired(false);

        try {
          const instanceResult = await fetchInstances(token);
          if (!cancelled) {
            setInstances(instanceResult.instances);
            setInstanceError(null);
          }
          const detailEntries = await Promise.all(
            instanceResult.instances.map(async (instance) => [
              instance.pid,
              await fetchInstanceInfo(instance.pid, token),
            ] as const),
          );
          if (!cancelled) {
            setInstanceDetails(Object.fromEntries(detailEntries));
          }
        } catch (err: unknown) {
          if (cancelled) return;
          if (err instanceof ApiError && err.status === 401) {
            setHealth(null);
            setAuthRequired(true);
          } else {
            setInstanceError(err instanceof Error ? err.message : 'instances request failed');
          }
        }

        try {
          const volumeResult = await fetchVolumes(token);
          if (!cancelled) {
            setVolumes(volumeResult.volumes);
            setVolumeError(null);
          }
        } catch (err: unknown) {
          if (cancelled) return;
          if (err instanceof ApiError && err.status === 401) {
            setHealth(null);
            setAuthRequired(true);
          } else {
            setVolumeError(err instanceof Error ? err.message : 'volume request failed');
          }
        }
      } catch (err: unknown) {
        if (cancelled) return;
        if (err instanceof ApiError && err.status === 401) {
          setHealth(null);
          setError(null);
          setAuthRequired(true);
        } else {
          setError(err instanceof Error ? err.message : 'health request failed');
        }
      }
    }

    void loadConsoleData();
    return () => {
      cancelled = true;
    };
  }, [token]);

  useEffect(() => {
    const handlePopState = () => setPage(pageFromPathname(window.location.pathname));
    window.addEventListener('popstate', handlePopState);
    return () => window.removeEventListener('popstate', handlePopState);
  }, []);

  const navigate = (nextPage: PageKey) => {
    setPage(nextPage);
    const nextPath = pagePaths[nextPage];
    if (window.location.pathname !== nextPath) {
      window.history.pushState(null, '', nextPath);
    }
  };

  const submitToken = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const nextToken = tokenInput.trim();
    if (!nextToken) return;
    sessionStorage.setItem(TOKEN_STORAGE_KEY, nextToken);
    setToken(nextToken);
    setTokenInput('');
    setError(null);
  };

  const updateVolumeForm = (field: keyof VolumeFormState, value: string) => {
    setVolumeForm((current) => ({ ...current, [field]: value }));
  };

  const submitVolume = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setCreatingVolume(true);
    setCreateVolumeError(null);
    try {
      const created = await createVolume(
        {
          name: volumeForm.name.trim(),
          mount_config: {
            mount_point: optionalField(volumeForm.mount_point),
            data_backend: volumeForm.data_backend.trim(),
            data_dir: optionalField(volumeForm.data_dir),
            meta_backend: volumeForm.meta_backend.trim(),
            meta_url: optionalField(volumeForm.meta_url),
          },
        },
        token,
      );
      setVolumes((current) => [...current, created]);
      setVolumeForm(emptyVolumeForm);
      setVolumeError(null);
    } catch (err: unknown) {
      if (err instanceof ApiError && err.status === 401) {
        setAuthRequired(true);
      } else {
        setCreateVolumeError(err instanceof Error ? err.message : 'create volume request failed');
      }
    } finally {
      setCreatingVolume(false);
    }
  };

  const status = useMemo(() => {
    if (authRequired) return { label: 'Auth required', tone: 'warn' };
    if (error) return { label: 'API unavailable', tone: 'bad' };
    if (!health) return { label: 'Connecting', tone: 'warn' };
    return { label: `BrewFS ${health.version}`, tone: 'good' };
  }, [authRequired, error, health]);

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <FolderTree size={24} aria-hidden="true" />
          <div>
            <strong>BrewFS</strong>
            <span>Console</span>
          </div>
        </div>
        <nav aria-label="Primary navigation">
          {navItems.map((item) => {
            const Icon = item.icon;
            return (
              <button
                key={item.key}
                className={page === item.key ? 'nav-item active' : 'nav-item'}
                type="button"
                onClick={() => navigate(item.key)}
              >
                <Icon size={18} aria-hidden="true" />
                <span>{item.label}</span>
              </button>
            );
          })}
        </nav>
      </aside>

      <main className="workspace">
        <header className="topbar">
          <div>
            <p className="eyebrow">Phase 1A scaffold</p>
            <h1>{pageTitle(page)}</h1>
          </div>
          <div className={`status-pill ${status.tone}`}>{status.label}</div>
        </header>

        <section className="content-grid">
          {authRequired ? (
            <AuthPanel value={tokenInput} onChange={setTokenInput} onSubmit={submitToken} />
          ) : (
            renderPage(page, {
              health,
              error,
              instances,
              instanceDetails,
              instanceError,
              volumes,
              volumeError,
              volumeForm,
              creatingVolume,
              createVolumeError,
              onVolumeFormChange: updateVolumeForm,
              onVolumeSubmit: submitVolume,
            })
          )}
        </section>
      </main>
    </div>
  );
}

function AuthPanel({
  value,
  onChange,
  onSubmit,
}: {
  value: string;
  onChange: (value: string) => void;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
}) {
  return (
    <article className="panel empty-panel">
      <h2>Console token required</h2>
      <form className="auth-form" onSubmit={onSubmit}>
        <label htmlFor="console-token">Bearer token</label>
        <div className="input-row">
          <input
            id="console-token"
            className="auth-input"
            type="password"
            value={value}
            autoComplete="current-password"
            onChange={(event) => onChange(event.target.value)}
          />
          <button className="primary-button" type="submit">
            <LogIn size={16} aria-hidden="true" />
            <span>Unlock</span>
          </button>
        </div>
      </form>
    </article>
  );
}

type RenderContext = {
  health: HealthResponse | null;
  error: string | null;
  instances: InstanceResponse[];
  instanceDetails: Record<number, InstanceInfoResponse>;
  instanceError: string | null;
  volumes: VolumeResponse[];
  volumeError: string | null;
  volumeForm: VolumeFormState;
  creatingVolume: boolean;
  createVolumeError: string | null;
  onVolumeFormChange: (field: keyof VolumeFormState, value: string) => void;
  onVolumeSubmit: (event: FormEvent<HTMLFormElement>) => void;
};

function renderPage(page: PageKey, context: RenderContext) {
  const {
    health,
    error,
    instances,
    instanceDetails,
    instanceError,
    volumes,
    volumeError,
    volumeForm,
    creatingVolume,
    createVolumeError,
    onVolumeFormChange,
    onVolumeSubmit,
  } = context;

  if (page === 'overview') {
    return (
      <>
        <Panel title="Runtime">
          <Metric label="Service" value={health?.service ?? 'waiting'} />
          <Metric label="Commit" value={health?.commit_short ?? 'unknown'} />
          <Metric label="Auth" value={health?.auth_mode ?? 'unknown'} />
          <Metric label="Live mounts" value={String(instances.length)} />
        </Panel>
        <Panel title="Live instances">
          {instanceError ? <p className="error-text">{instanceError}</p> : null}
          {instances.length === 0 ? (
            <p className="muted">No live BrewFS mount records found.</p>
          ) : (
            <div className="instance-list">
              {instances.map((instance) => (
                <div className="instance-row" key={instance.pid}>
                  <strong>{instance.mount_point}</strong>
                  <span>
                    pid {instance.pid}
                    {instanceDetails[instance.pid]
                      ? ` · ${instanceDetails[instance.pid].meta_backend} · ${instanceDetails[instance.pid].version}`
                      : ''}
                  </span>
                  <code>{instance.socket_path}</code>
                  {instanceDetails[instance.pid] ? (
                    <span>
                      capabilities:{' '}
                      {enabledCapabilities(instanceDetails[instance.pid].capabilities).join(', ') ||
                        'none'}
                    </span>
                  ) : null}
                </div>
              ))}
            </div>
          )}
          {error ? <p className="error-text">{error}</p> : null}
        </Panel>
      </>
    );
  }

  if (page === 'filesystems') {
    return (
      <FilesystemsPage
        volumes={volumes}
        volumeError={volumeError}
        form={volumeForm}
        creating={creatingVolume}
        createError={createVolumeError}
        onChange={onVolumeFormChange}
        onSubmit={onVolumeSubmit}
      />
    );
  }

  if (page === 'jobs') {
    return (
      <EmptyPanel
        title="No jobs"
        detail="Live instance discovery is available; job control-plane actions arrive next."
      />
    );
  }

  if (page === 'browser') {
    return (
      <EmptyPanel
        title="File browser unavailable"
        detail="Namespace browsing depends on mounted instance discovery and file metadata APIs."
      />
    );
  }

  if (page === 'trash') {
    return (
      <EmptyPanel
        title="Trash unavailable"
        detail="Trash listing and restore actions depend on delayed-delete metadata APIs."
      />
    );
  }

  if (page === 'acl') {
    return (
      <EmptyPanel
        title="ACL unavailable"
        detail="ACL editing will be enabled only for backends that advertise ACL support."
      />
    );
  }

  if (page === 'csi') {
    return (
      <EmptyPanel
        title="CSI dashboard disabled"
        detail="Kubernetes resource discovery is a subsequent read-only integration."
      />
    );
  }

  return (
    <EmptyPanel
      title="Settings unavailable"
      detail="Token auth and registry settings arrive in Phase 1B."
    />
  );
}

function FilesystemsPage({
  volumes,
  volumeError,
  form,
  creating,
  createError,
  onChange,
  onSubmit,
}: {
  volumes: VolumeResponse[];
  volumeError: string | null;
  form: VolumeFormState;
  creating: boolean;
  createError: string | null;
  onChange: (field: keyof VolumeFormState, value: string) => void;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
}) {
  return (
    <>
      <article className="panel table-panel">
        <h2>Registered filesystems</h2>
        {volumeError ? <p className="error-text">{volumeError}</p> : null}
        {volumes.length === 0 ? (
          <p className="muted">No registered filesystems.</p>
        ) : (
          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Data</th>
                  <th>Meta</th>
                  <th>Mount</th>
                  <th>Meta URL</th>
                </tr>
              </thead>
              <tbody>
                {volumes.map((volume) => (
                  <tr key={volume.id}>
                    <td>{volume.name}</td>
                    <td>{volume.mount_config.data_backend}</td>
                    <td>{volume.mount_config.meta_backend}</td>
                    <td>{volume.mount_config.mount_point ?? '-'}</td>
                    <td>{volume.mount_config.meta_url_redacted ?? '-'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </article>

      <article className="panel">
        <h2>Create registry entry</h2>
        <form className="volume-form" onSubmit={onSubmit}>
          <label>
            <span>Name</span>
            <input
              required
              value={form.name}
              onChange={(event) => onChange('name', event.target.value)}
            />
          </label>
          <label>
            <span>Mount point</span>
            <input
              value={form.mount_point}
              onChange={(event) => onChange('mount_point', event.target.value)}
            />
          </label>
          <label>
            <span>Data backend</span>
            <input
              required
              value={form.data_backend}
              onChange={(event) => onChange('data_backend', event.target.value)}
            />
          </label>
          <label>
            <span>Data dir</span>
            <input
              value={form.data_dir}
              onChange={(event) => onChange('data_dir', event.target.value)}
            />
          </label>
          <label>
            <span>Meta backend</span>
            <input
              required
              value={form.meta_backend}
              onChange={(event) => onChange('meta_backend', event.target.value)}
            />
          </label>
          <label>
            <span>Meta URL</span>
            <input
              type="password"
              value={form.meta_url}
              onChange={(event) => onChange('meta_url', event.target.value)}
            />
          </label>
          {createError ? <p className="error-text">{createError}</p> : null}
          <button className="primary-button" type="submit" disabled={creating}>
            <HardDrive size={16} aria-hidden="true" />
            <span>{creating ? 'Creating' : 'Create'}</span>
          </button>
        </form>
      </article>
    </>
  );
}

function Panel({ title, children }: { title: string; children: ReactNode }) {
  return (
    <article className="panel">
      <h2>{title}</h2>
      {children}
    </article>
  );
}

function EmptyPanel({ title, detail }: { title: string; detail: string }) {
  return (
    <article className="panel empty-panel">
      <h2>{title}</h2>
      <p className="muted">{detail}</p>
    </article>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="metric">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function optionalField(value: string): string | undefined {
  const trimmed = value.trim();
  return trimmed ? trimmed : undefined;
}

function enabledCapabilities(capabilities: Record<string, boolean>): string[] {
  return Object.entries(capabilities)
    .filter(([, enabled]) => enabled)
    .map(([name]) => name);
}
