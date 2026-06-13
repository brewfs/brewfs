import {
  Activity,
  ArrowUp,
  Check,
  Copy,
  Database,
  Download,
  FileSearch,
  FolderTree,
  Gauge,
  LogIn,
  HardDrive,
  Pencil,
  RefreshCw,
  RotateCcw,
  Settings,
  ShieldCheck,
  Trash2,
  X,
  type LucideIcon,
} from 'lucide-react';
import { useEffect, useMemo, useState, type FormEvent, type ReactNode } from 'react';
import {
  ApiError,
  createVolume,
  deleteAcl,
  deleteTrashEntry,
  deleteVolume as deleteVolumeRequest,
  fetchFileList,
  fetchFileStat,
  fetchJobStatus,
  fetchHealth,
  fetchInstances,
  fetchVolumes,
  fetchReadLink,
  putAcl,
  restoreTrashEntry,
  runGcJob,
  type AclUpdateRequest,
  type FileListResponse,
  type FileStatResponse,
  type HealthResponse,
  type InstanceInfoResponse,
  type InstanceResponse,
  type JobStatusResponse,
  type VolumeResponse,
  updateVolume,
} from './api';
import { formatAclDraft, parseAclDraft } from './aclDraft';
import { loadAclView, type AclViewResult } from './aclView';
import {
  browserBreadcrumbs,
  browserMvpDataActions,
  formatBrowserEntryFlags,
  formatMode,
  joinBrowserPath,
  normalizeBrowserPath,
  parentBrowserPath,
  showsBrowserDataActionsForKind,
} from './browserPath';
import {
  formatCsiItemCount,
  loadCsiDashboard,
  shouldLoadCsiDashboardForPage,
  type CsiDashboardFilters,
  type CsiDashboardResult,
} from './csiDashboard';
import { loadInstanceDetails } from './instanceDetails';
import { buildMountCommand } from './mountCommand';
import {
  overviewCapabilityWarnings,
  overviewCsiSummary,
  overviewMetrics as buildOverviewMetrics,
  overviewRecentJob,
} from './overviewSummary';
import { loadTrashView, type TrashViewResult } from './trashView';
import { buildSettingsSummary } from './settingsSummary';
import {
  aclCapabilityWarning,
  enabledCapabilityLabels,
  summarizeVolumeCapabilities,
} from './volumeCapabilities';
import { labelsFromText, labelsToText } from './volumeEdit';
import { formatVolumeRuntime } from './volumeRuntime';

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

type VolumeEditFormState = {
  name: string;
  description: string;
  labels: string;
};

const emptyVolumeForm: VolumeFormState = {
  name: '',
  mount_point: '',
  data_backend: 'local-fs',
  data_dir: '',
  meta_backend: 'sqlx',
  meta_url: '',
};

type CurrentJob = {
  pid: number;
  status: JobStatusResponse;
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
  const [editingVolumeId, setEditingVolumeId] = useState<string | null>(null);
  const [editVolumeForm, setEditVolumeForm] = useState<VolumeEditFormState | null>(null);
  const [volumeActionInFlight, setVolumeActionInFlight] = useState(false);
  const [volumeActionError, setVolumeActionError] = useState<string | null>(null);
  const [copiedMountCommandVolumeId, setCopiedMountCommandVolumeId] = useState<string | null>(null);
  const [mountCommandError, setMountCommandError] = useState<string | null>(null);
  const [selectedJobPid, setSelectedJobPid] = useState<number | null>(null);
  const [gcDryRun, setGcDryRun] = useState(true);
  const [jobRequestInFlight, setJobRequestInFlight] = useState(false);
  const [jobError, setJobError] = useState<string | null>(null);
  const [currentJob, setCurrentJob] = useState<CurrentJob | null>(null);
  const [selectedBrowserVolumeId, setSelectedBrowserVolumeId] = useState<string | null>(null);
  const [browserPath, setBrowserPath] = useState('/');
  const [browserPathInput, setBrowserPathInput] = useState('/');
  const [browserReloadKey, setBrowserReloadKey] = useState(0);
  const [browserResult, setBrowserResult] = useState<FileListResponse | null>(null);
  const [browserLoading, setBrowserLoading] = useState(false);
  const [browserError, setBrowserError] = useState<string | null>(null);
  const [browserMetadata, setBrowserMetadata] = useState<FileStatResponse | null>(null);
  const [browserLinkTarget, setBrowserLinkTarget] = useState<string | null>(null);
  const [browserMetadataLoading, setBrowserMetadataLoading] = useState(false);
  const [browserMetadataError, setBrowserMetadataError] = useState<string | null>(null);
  const [selectedTrashVolumeId, setSelectedTrashVolumeId] = useState<string | null>(null);
  const [trashResult, setTrashResult] = useState<TrashViewResult | null>(null);
  const [trashLoading, setTrashLoading] = useState(false);
  const [trashError, setTrashError] = useState<string | null>(null);
  const [trashReloadKey, setTrashReloadKey] = useState(0);
  const [trashActionInFlight, setTrashActionInFlight] = useState<string | null>(null);
  const [selectedAclVolumeId, setSelectedAclVolumeId] = useState<string | null>(null);
  const [aclPath, setAclPath] = useState('/');
  const [aclPathInput, setAclPathInput] = useState('/');
  const [aclResult, setAclResult] = useState<AclViewResult | null>(null);
  const [aclLoading, setAclLoading] = useState(false);
  const [aclError, setAclError] = useState<string | null>(null);
  const [aclReloadKey, setAclReloadKey] = useState(0);
  const [aclDraft, setAclDraft] = useState('');
  const [aclDraftError, setAclDraftError] = useState<string | null>(null);
  const [aclActionInFlight, setAclActionInFlight] = useState<string | null>(null);
  const [csiDashboard, setCsiDashboard] = useState<CsiDashboardResult | null>(null);
  const [csiLoading, setCsiLoading] = useState(false);
  const [csiError, setCsiError] = useState<string | null>(null);
  const [csiNamespaceInput, setCsiNamespaceInput] = useState('');
  const [csiVolumeInput, setCsiVolumeInput] = useState('');
  const [csiFilters, setCsiFilters] = useState<CsiDashboardFilters>({});
  const [csiReloadKey, setCsiReloadKey] = useState(0);
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
            setInstanceDetails({});
          }
          const detailState = await loadInstanceDetails(instanceResult.instances, token);
          if (!cancelled) {
            setInstanceDetails(detailState.details);
            if (detailState.authRequired) {
              setHealth(null);
              setAuthRequired(true);
            } else {
              setInstanceError(detailState.error);
            }
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
    if (instances.length === 0) {
      setSelectedJobPid(null);
      return;
    }

    setSelectedJobPid((current) =>
      current !== null && instances.some((instance) => instance.pid === current)
        ? current
        : instances[0].pid,
    );
  }, [instances]);

  useEffect(() => {
    setSelectedBrowserVolumeId((current) =>
      current !== null && volumes.some((volume) => volume.id === current)
        ? current
        : (volumes[0]?.id ?? null),
    );
  }, [volumes]);

  useEffect(() => {
    setSelectedTrashVolumeId((current) =>
      current !== null && volumes.some((volume) => volume.id === current)
        ? current
        : (volumes[0]?.id ?? null),
    );
  }, [volumes]);

  useEffect(() => {
    setSelectedAclVolumeId((current) =>
      current !== null && volumes.some((volume) => volume.id === current)
        ? current
        : (volumes[0]?.id ?? null),
    );
  }, [volumes]);

  useEffect(() => {
    setBrowserResult(null);
    setBrowserError(null);
    setBrowserMetadata(null);
    setBrowserLinkTarget(null);
    setBrowserMetadataError(null);
  }, [browserPath, selectedBrowserVolumeId]);

  useEffect(() => {
    if (page !== 'browser') return;
    if (!selectedBrowserVolumeId) {
      setBrowserResult(null);
      setBrowserError(null);
      setBrowserLoading(false);
      return;
    }

    let cancelled = false;
    setBrowserLoading(true);
    setBrowserError(null);

    void fetchFileList(selectedBrowserVolumeId, browserPath, token)
      .then((result) => {
        if (!cancelled) {
          setBrowserResult(result);
        }
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        if (err instanceof ApiError && err.status === 401) {
          setAuthRequired(true);
        } else {
          setBrowserResult(null);
          setBrowserError(browserErrorMessage(err));
        }
      })
      .finally(() => {
        if (!cancelled) {
          setBrowserLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [browserPath, browserReloadKey, page, selectedBrowserVolumeId, token]);

  useEffect(() => {
    const handlePopState = () => setPage(pageFromPathname(window.location.pathname));
    window.addEventListener('popstate', handlePopState);
    return () => window.removeEventListener('popstate', handlePopState);
  }, []);

  useEffect(() => {
    if (page !== 'trash') return;
    let cancelled = false;
    const volume = volumes.find((entry) => entry.id === selectedTrashVolumeId) ?? null;

    setTrashLoading(true);
    setTrashError(null);
    void loadTrashView(volume, token)
      .then((result) => {
        if (!cancelled) {
          setTrashResult(result);
        }
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        if (err instanceof ApiError && err.status === 401) {
          setAuthRequired(true);
        } else {
          setTrashError(err instanceof Error ? err.message : 'trash request failed');
        }
      })
      .finally(() => {
        if (!cancelled) {
          setTrashLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [page, selectedTrashVolumeId, token, trashReloadKey, volumes]);

  useEffect(() => {
    if (page !== 'acl') return;
    let cancelled = false;
    const volume = volumes.find((entry) => entry.id === selectedAclVolumeId) ?? null;

    setAclLoading(true);
    setAclError(null);
    void loadAclView(volume, aclPath, token)
      .then((result) => {
        if (!cancelled) {
          setAclResult(result);
        }
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        if (err instanceof ApiError && err.status === 401) {
          setAuthRequired(true);
        } else {
          setAclError(err instanceof Error ? err.message : 'ACL request failed');
        }
      })
      .finally(() => {
        if (!cancelled) {
          setAclLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [aclPath, aclReloadKey, page, selectedAclVolumeId, token, volumes]);

  useEffect(() => {
    if (aclResult?.state === 'ready') {
      setAclDraft(formatAclDraft(aclResult.entries));
    } else {
      setAclDraft('');
    }
    setAclDraftError(null);
  }, [aclResult]);

  useEffect(() => {
    if (!shouldLoadCsiDashboardForPage(page, health?.integrations.csi_dashboard === true)) return;
    let cancelled = false;

    setCsiLoading(true);
    setCsiError(null);
    setCsiDashboard(null);
    void loadCsiDashboard(token, csiFilters)
      .then((result) => {
        if (!cancelled) {
          setCsiDashboard(result);
        }
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        if (err instanceof ApiError && err.status === 401) {
          setAuthRequired(true);
        } else {
          setCsiError(err instanceof Error ? err.message : 'CSI dashboard request failed');
        }
      })
      .finally(() => {
        if (!cancelled) {
          setCsiLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [csiFilters, csiReloadKey, health?.integrations.csi_dashboard, page, token]);

  const navigate = (nextPage: PageKey) => {
    setPage(nextPage);
    const nextPath = pagePaths[nextPage];
    if (window.location.pathname !== nextPath) {
      window.history.pushState(null, '', nextPath);
    }
  };

  const selectedBrowserVolume = useMemo(
    () => volumes.find((volume) => volume.id === selectedBrowserVolumeId) ?? null,
    [selectedBrowserVolumeId, volumes],
  );

  const selectedTrashVolume = useMemo(
    () => volumes.find((volume) => volume.id === selectedTrashVolumeId) ?? null,
    [selectedTrashVolumeId, volumes],
  );

  const selectedAclVolume = useMemo(
    () => volumes.find((volume) => volume.id === selectedAclVolumeId) ?? null,
    [selectedAclVolumeId, volumes],
  );

  const submitBrowserPath = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const normalized = normalizeBrowserPath(browserPathInput);
    setBrowserPathInput(normalized);
    setBrowserPath(normalized);
  };

  const navigateBrowserPath = (path: string) => {
    const normalized = normalizeBrowserPath(path);
    setBrowserPathInput(normalized);
    setBrowserPath(normalized);
  };

  const changeBrowserVolume = (volumeId: string) => {
    setSelectedBrowserVolumeId(volumeId);
    setBrowserPath('/');
    setBrowserPathInput('/');
    setBrowserResult(null);
  };

  const changeTrashVolume = (volumeId: string) => {
    setSelectedTrashVolumeId(volumeId);
    setTrashResult(null);
    setTrashActionInFlight(null);
    setTrashError(null);
  };

  const restoreTrashEntryFromPage = async (entryId: string) => {
    if (!selectedTrashVolume) return;

    setTrashActionInFlight(`restore:${entryId}`);
    setTrashError(null);
    try {
      await restoreTrashEntry(selectedTrashVolume.id, entryId, token);
      setTrashReloadKey((current) => current + 1);
    } catch (err: unknown) {
      if (err instanceof ApiError && err.status === 401) {
        setAuthRequired(true);
      } else {
        setTrashError(err instanceof Error ? err.message : 'trash restore request failed');
      }
    } finally {
      setTrashActionInFlight(null);
    }
  };

  const deleteTrashEntryFromPage = async (entryId: string) => {
    if (!selectedTrashVolume) return;
    if (!window.confirm(`Delete trash entry ${entryId}?`)) return;

    setTrashActionInFlight(`delete:${entryId}`);
    setTrashError(null);
    try {
      await deleteTrashEntry(selectedTrashVolume.id, entryId, token);
      setTrashReloadKey((current) => current + 1);
    } catch (err: unknown) {
      if (err instanceof ApiError && err.status === 401) {
        setAuthRequired(true);
      } else {
        setTrashError(err instanceof Error ? err.message : 'trash delete request failed');
      }
    } finally {
      setTrashActionInFlight(null);
    }
  };

  const submitAclPath = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const normalized = normalizeBrowserPath(aclPathInput);
    setAclPathInput(normalized);
    setAclPath(normalized);
    setAclResult(null);
    setAclActionInFlight(null);
    setAclDraftError(null);
  };

  const changeAclVolume = (volumeId: string) => {
    setSelectedAclVolumeId(volumeId);
    setAclPath('/');
    setAclPathInput('/');
    setAclResult(null);
    setAclActionInFlight(null);
    setAclDraftError(null);
  };

  const replaceAclFromPage = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!selectedAclVolume) return;

    let request: AclUpdateRequest;
    try {
      request = parseAclDraft(aclDraft);
      setAclDraftError(null);
    } catch (err: unknown) {
      setAclDraftError(err instanceof Error ? err.message : 'ACL draft is invalid.');
      return;
    }

    setAclActionInFlight('replace');
    setAclError(null);
    try {
      await putAcl(selectedAclVolume.id, aclResult?.path ?? aclPath, request, token);
      setAclReloadKey((current) => current + 1);
    } catch (err: unknown) {
      if (err instanceof ApiError && err.status === 401) {
        setAuthRequired(true);
      } else {
        setAclError(err instanceof Error ? err.message : 'ACL update request failed');
      }
    } finally {
      setAclActionInFlight(null);
    }
  };

  const clearAclFromPage = async () => {
    if (!selectedAclVolume) return;
    const path = aclResult?.path ?? aclPath;
    if (!window.confirm(`Clear extended ACL for ${path}?`)) return;

    setAclActionInFlight('clear');
    setAclError(null);
    setAclDraftError(null);
    try {
      await deleteAcl(selectedAclVolume.id, path, token);
      setAclReloadKey((current) => current + 1);
    } catch (err: unknown) {
      if (err instanceof ApiError && err.status === 401) {
        setAuthRequired(true);
      } else {
        setAclError(err instanceof Error ? err.message : 'ACL delete request failed');
      }
    } finally {
      setAclActionInFlight(null);
    }
  };

  const refreshBrowser = () => {
    setBrowserReloadKey((current) => current + 1);
  };

  const inspectBrowserPath = async (path: string) => {
    if (!selectedBrowserVolumeId) return;

    const normalized = normalizeBrowserPath(path);
    setBrowserMetadataLoading(true);
    setBrowserMetadataError(null);
    setBrowserMetadata(null);
    setBrowserLinkTarget(null);
    try {
      const stat = await fetchFileStat(selectedBrowserVolumeId, normalized, token);
      let target: string | null = null;
      if (stat.kind === 'symlink') {
        const link = await fetchReadLink(selectedBrowserVolumeId, normalized, token);
        target = link.target;
      }
      setBrowserMetadata(stat);
      setBrowserLinkTarget(target);
    } catch (err: unknown) {
      if (err instanceof ApiError && err.status === 401) {
        setAuthRequired(true);
      } else {
        setBrowserMetadata(null);
        setBrowserMetadataError(browserErrorMessage(err));
      }
    } finally {
      setBrowserMetadataLoading(false);
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

  const startVolumeEdit = (volume: VolumeResponse) => {
    setEditingVolumeId(volume.id);
    setEditVolumeForm({
      name: volume.name,
      description: volume.description ?? '',
      labels: labelsToText(volume.labels),
    });
    setVolumeActionError(null);
  };

  const updateVolumeEditForm = (field: keyof VolumeEditFormState, value: string) => {
    setEditVolumeForm((current) => (current ? { ...current, [field]: value } : current));
  };

  const cancelVolumeEdit = () => {
    setEditingVolumeId(null);
    setEditVolumeForm(null);
    setVolumeActionError(null);
  };

  const submitVolumeEdit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!editingVolumeId || !editVolumeForm) return;

    setVolumeActionInFlight(true);
    setVolumeActionError(null);
    try {
      const labels = labelsFromText(editVolumeForm.labels);
      const updated = await updateVolume(
        editingVolumeId,
        {
          name: editVolumeForm.name.trim(),
          description: editVolumeForm.description.trim() || null,
          labels,
        },
        token,
      );
      setVolumes((current) =>
        current.map((volume) => (volume.id === updated.id ? updated : volume)),
      );
      setEditingVolumeId(null);
      setEditVolumeForm(null);
    } catch (err: unknown) {
      if (err instanceof ApiError && err.status === 401) {
        setAuthRequired(true);
      } else {
        setVolumeActionError(err instanceof Error ? err.message : 'volume update failed');
      }
    } finally {
      setVolumeActionInFlight(false);
    }
  };

  const deleteVolumeEntry = async (volume: VolumeResponse) => {
    if (!window.confirm(`Delete registry entry ${volume.name}?`)) return;

    setVolumeActionInFlight(true);
    setVolumeActionError(null);
    try {
      await deleteVolumeRequest(volume.id, token);
      setVolumes((current) => current.filter((entry) => entry.id !== volume.id));
      if (editingVolumeId === volume.id) {
        setEditingVolumeId(null);
        setEditVolumeForm(null);
      }
    } catch (err: unknown) {
      if (err instanceof ApiError && err.status === 401) {
        setAuthRequired(true);
      } else {
        setVolumeActionError(err instanceof Error ? err.message : 'volume delete failed');
      }
    } finally {
      setVolumeActionInFlight(false);
    }
  };

  const copyMountCommand = async (volume: VolumeResponse) => {
    const { command } = buildMountCommand(volume);
    if (!navigator.clipboard) {
      setCopiedMountCommandVolumeId(null);
      setMountCommandError('Clipboard API is unavailable in this browser.');
      return;
    }

    try {
      await navigator.clipboard.writeText(command);
      setMountCommandError(null);
      setCopiedMountCommandVolumeId(volume.id);
      window.setTimeout(() => {
        setCopiedMountCommandVolumeId((current) => (current === volume.id ? null : current));
      }, 1500);
    } catch (err: unknown) {
      setCopiedMountCommandVolumeId(null);
      setMountCommandError(
        err instanceof Error ? `Unable to copy mount command: ${err.message}` : 'Unable to copy mount command.',
      );
    }
  };

  const submitGcJob = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (selectedJobPid === null) return;

    setJobRequestInFlight(true);
    setJobError(null);
    try {
      const accepted = await runGcJob(selectedJobPid, { dry_run: gcDryRun }, token);
      const status = await fetchJobStatus(selectedJobPid, accepted.job_id, token);
      setCurrentJob({ pid: selectedJobPid, status });
    } catch (err: unknown) {
      if (err instanceof ApiError && err.status === 401) {
        setAuthRequired(true);
      } else {
        setJobError(err instanceof Error ? err.message : 'GC job request failed');
      }
    } finally {
      setJobRequestInFlight(false);
    }
  };

  const refreshCurrentJob = async () => {
    if (!currentJob) return;

    setJobRequestInFlight(true);
    setJobError(null);
    try {
      const status = await fetchJobStatus(currentJob.pid, currentJob.status.job_id, token);
      setCurrentJob({ pid: currentJob.pid, status });
    } catch (err: unknown) {
      if (err instanceof ApiError && err.status === 401) {
        setAuthRequired(true);
      } else {
        setJobError(err instanceof Error ? err.message : 'job status request failed');
      }
    } finally {
      setJobRequestInFlight(false);
    }
  };

  const submitCsiFilters = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setCsiFilters({
      namespace: optionalField(csiNamespaceInput),
      volume: optionalField(csiVolumeInput),
    });
  };

  const refreshCsiDashboard = () => {
    setCsiReloadKey((current) => current + 1);
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
            <p className="eyebrow">Control plane</p>
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
              editingVolumeId,
              editVolumeForm,
              volumeActionInFlight,
              volumeActionError,
              copiedMountCommandVolumeId,
              mountCommandError,
              selectedJobPid,
              gcDryRun,
              jobRequestInFlight,
              jobError,
              currentJob,
              selectedBrowserVolume,
              browserPathInput,
              browserResult,
              browserLoading,
              browserError,
              browserMetadata,
              browserLinkTarget,
              browserMetadataLoading,
              browserMetadataError,
              selectedTrashVolume,
              trashResult,
              trashLoading,
              trashError,
              trashActionInFlight,
              selectedAclVolume,
              aclPathInput,
              aclResult,
              aclLoading,
              aclError,
              aclDraft,
              aclDraftError,
              aclActionInFlight,
              csiDashboard,
              csiLoading,
              csiError,
              csiNamespaceInput,
              csiVolumeInput,
              onVolumeFormChange: updateVolumeForm,
              onVolumeSubmit: submitVolume,
              onVolumeEditStart: startVolumeEdit,
              onVolumeEditFormChange: updateVolumeEditForm,
              onVolumeEditSubmit: submitVolumeEdit,
              onVolumeEditCancel: cancelVolumeEdit,
              onVolumeDelete: deleteVolumeEntry,
              onMountCommandCopy: copyMountCommand,
              onSelectedJobPidChange: setSelectedJobPid,
              onGcDryRunChange: setGcDryRun,
              onGcJobSubmit: submitGcJob,
              onRefreshCurrentJob: refreshCurrentJob,
              onBrowserVolumeChange: changeBrowserVolume,
              onBrowserPathInputChange: setBrowserPathInput,
              onBrowserPathSubmit: submitBrowserPath,
              onBrowserRefresh: refreshBrowser,
              onBrowserNavigate: navigateBrowserPath,
              onBrowserInspect: inspectBrowserPath,
              onTrashVolumeChange: changeTrashVolume,
              onTrashRestore: restoreTrashEntryFromPage,
              onTrashDelete: deleteTrashEntryFromPage,
              onAclVolumeChange: changeAclVolume,
              onAclPathInputChange: setAclPathInput,
              onAclPathSubmit: submitAclPath,
              onAclDraftChange: setAclDraft,
              onAclReplace: replaceAclFromPage,
              onAclClear: clearAclFromPage,
              onCsiNamespaceChange: setCsiNamespaceInput,
              onCsiVolumeChange: setCsiVolumeInput,
              onCsiFilterSubmit: submitCsiFilters,
              onCsiRefresh: refreshCsiDashboard,
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
  editingVolumeId: string | null;
  editVolumeForm: VolumeEditFormState | null;
  volumeActionInFlight: boolean;
  volumeActionError: string | null;
  copiedMountCommandVolumeId: string | null;
  mountCommandError: string | null;
  selectedJobPid: number | null;
  gcDryRun: boolean;
  jobRequestInFlight: boolean;
  jobError: string | null;
  currentJob: CurrentJob | null;
  selectedBrowserVolume: VolumeResponse | null;
  browserPathInput: string;
  browserResult: FileListResponse | null;
  browserLoading: boolean;
  browserError: string | null;
  browserMetadata: FileStatResponse | null;
  browserLinkTarget: string | null;
  browserMetadataLoading: boolean;
  browserMetadataError: string | null;
  selectedTrashVolume: VolumeResponse | null;
  trashResult: TrashViewResult | null;
  trashLoading: boolean;
  trashError: string | null;
  trashActionInFlight: string | null;
  selectedAclVolume: VolumeResponse | null;
  aclPathInput: string;
  aclResult: AclViewResult | null;
  aclLoading: boolean;
  aclError: string | null;
  aclDraft: string;
  aclDraftError: string | null;
  aclActionInFlight: string | null;
  csiDashboard: CsiDashboardResult | null;
  csiLoading: boolean;
  csiError: string | null;
  csiNamespaceInput: string;
  csiVolumeInput: string;
  onVolumeFormChange: (field: keyof VolumeFormState, value: string) => void;
  onVolumeSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onVolumeEditStart: (volume: VolumeResponse) => void;
  onVolumeEditFormChange: (field: keyof VolumeEditFormState, value: string) => void;
  onVolumeEditSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onVolumeEditCancel: () => void;
  onVolumeDelete: (volume: VolumeResponse) => void;
  onMountCommandCopy: (volume: VolumeResponse) => void;
  onSelectedJobPidChange: (pid: number) => void;
  onGcDryRunChange: (enabled: boolean) => void;
  onGcJobSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onRefreshCurrentJob: () => void;
  onBrowserVolumeChange: (volumeId: string) => void;
  onBrowserPathInputChange: (path: string) => void;
  onBrowserPathSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onBrowserRefresh: () => void;
  onBrowserNavigate: (path: string) => void;
  onBrowserInspect: (path: string) => void;
  onTrashVolumeChange: (volumeId: string) => void;
  onTrashRestore: (entryId: string) => void;
  onTrashDelete: (entryId: string) => void;
  onAclVolumeChange: (volumeId: string) => void;
  onAclPathInputChange: (path: string) => void;
  onAclPathSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onAclDraftChange: (value: string) => void;
  onAclReplace: (event: FormEvent<HTMLFormElement>) => void;
  onAclClear: () => void;
  onCsiNamespaceChange: (value: string) => void;
  onCsiVolumeChange: (value: string) => void;
  onCsiFilterSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onCsiRefresh: () => void;
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
    editingVolumeId,
    editVolumeForm,
    volumeActionInFlight,
    volumeActionError,
    copiedMountCommandVolumeId,
    mountCommandError,
    selectedJobPid,
    gcDryRun,
    jobRequestInFlight,
    jobError,
    currentJob,
    selectedBrowserVolume,
    browserPathInput,
    browserResult,
    browserLoading,
    browserError,
    browserMetadata,
    browserLinkTarget,
    browserMetadataLoading,
    browserMetadataError,
    selectedTrashVolume,
    trashResult,
    trashLoading,
    trashError,
    trashActionInFlight,
    selectedAclVolume,
    aclPathInput,
    aclResult,
    aclLoading,
    aclError,
    aclDraft,
    aclDraftError,
    aclActionInFlight,
    csiDashboard,
    csiLoading,
    csiError,
    csiNamespaceInput,
    csiVolumeInput,
    onVolumeFormChange,
    onVolumeSubmit,
    onVolumeEditStart,
    onVolumeEditFormChange,
    onVolumeEditSubmit,
    onVolumeEditCancel,
    onVolumeDelete,
    onMountCommandCopy,
    onSelectedJobPidChange,
    onGcDryRunChange,
    onGcJobSubmit,
    onRefreshCurrentJob,
    onBrowserVolumeChange,
    onBrowserPathInputChange,
    onBrowserPathSubmit,
    onBrowserRefresh,
    onBrowserNavigate,
    onBrowserInspect,
    onTrashVolumeChange,
    onTrashRestore,
    onTrashDelete,
    onAclVolumeChange,
    onAclPathInputChange,
    onAclPathSubmit,
    onAclDraftChange,
    onAclReplace,
    onAclClear,
    onCsiNamespaceChange,
    onCsiVolumeChange,
    onCsiFilterSubmit,
    onCsiRefresh,
  } = context;

  if (page === 'overview') {
    const metrics = buildOverviewMetrics({ health, volumes, instances });
    const recentJob = overviewRecentJob(currentJob);
    const capabilityWarnings = overviewCapabilityWarnings({ volumes, instanceDetails });
    const csiSummary = overviewCsiSummary({
      health,
      dashboard: csiDashboard,
      loading: csiLoading,
      error: csiError,
    });

    return (
      <>
        <Panel title="Runtime">
          {metrics.map((metric) => (
            <Metric key={metric.label} label={metric.label} value={metric.value} />
          ))}
        </Panel>
        <Panel title="Recent job">
          <h3 className="panel-subtitle">{recentJob.title}</h3>
          <p className="muted job-detail">{recentJob.detail}</p>
          {recentJob.metrics.length > 0 ? (
            <div className="job-metrics">
              {recentJob.metrics.map((metric) => (
                <Metric key={metric.label} label={metric.label} value={metric.value} />
              ))}
            </div>
          ) : null}
        </Panel>
        <Panel title="Backend capabilities">
          {capabilityWarnings.length === 0 ? (
            <p className="muted">No capability warnings.</p>
          ) : (
            <div className="warning-list">
              {capabilityWarnings.map((warning) => (
                <p className="warning-text" key={warning}>
                  {warning}
                </p>
              ))}
            </div>
          )}
        </Panel>
        <Panel title="CSI health">
          <Metric label={csiSummary.label} value={csiSummary.value} />
          <p className="muted job-detail">{csiSummary.detail}</p>
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
                      {enabledCapabilityLabels(instanceDetails[instance.pid].capabilities).join(', ') ||
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
        instanceDetails={instanceDetails}
        form={volumeForm}
        creating={creatingVolume}
        createError={createVolumeError}
        editingVolumeId={editingVolumeId}
        editForm={editVolumeForm}
        actionInFlight={volumeActionInFlight}
        actionError={volumeActionError}
        copiedMountCommandVolumeId={copiedMountCommandVolumeId}
        mountCommandError={mountCommandError}
        onChange={onVolumeFormChange}
        onSubmit={onVolumeSubmit}
        onEditStart={onVolumeEditStart}
        onEditFormChange={onVolumeEditFormChange}
        onEditSubmit={onVolumeEditSubmit}
        onEditCancel={onVolumeEditCancel}
        onDelete={onVolumeDelete}
        onMountCommandCopy={onMountCommandCopy}
      />
    );
  }

  if (page === 'jobs') {
    return (
      <JobsPage
        instances={instances}
        selectedPid={selectedJobPid}
        dryRun={gcDryRun}
        requestInFlight={jobRequestInFlight}
        error={jobError}
        currentJob={currentJob}
        onSelectedPidChange={onSelectedJobPidChange}
        onDryRunChange={onGcDryRunChange}
        onSubmit={onGcJobSubmit}
        onRefresh={onRefreshCurrentJob}
      />
    );
  }

  if (page === 'browser') {
    return (
      <BrowserPage
        volumes={volumes}
        selectedVolume={selectedBrowserVolume}
        pathInput={browserPathInput}
        result={browserResult}
        loading={browserLoading}
        error={browserError}
        metadata={browserMetadata}
        linkTarget={browserLinkTarget}
        metadataLoading={browserMetadataLoading}
        metadataError={browserMetadataError}
        onVolumeChange={onBrowserVolumeChange}
        onPathInputChange={onBrowserPathInputChange}
        onPathSubmit={onBrowserPathSubmit}
        onRefresh={onBrowserRefresh}
        onNavigate={onBrowserNavigate}
        onInspect={onBrowserInspect}
      />
    );
  }

  if (page === 'trash') {
    return (
      <TrashPage
        volumes={volumes}
        selectedVolume={selectedTrashVolume}
        result={trashResult}
        loading={trashLoading}
        error={trashError}
        actionInFlight={trashActionInFlight}
        onVolumeChange={onTrashVolumeChange}
        onRestore={onTrashRestore}
        onDelete={onTrashDelete}
      />
    );
  }

  if (page === 'acl') {
    return (
      <AclPage
        volumes={volumes}
        selectedVolume={selectedAclVolume}
        capabilityWarning={aclCapabilityWarning(selectedAclVolume, instanceDetails)}
        pathInput={aclPathInput}
        result={aclResult}
        loading={aclLoading}
        error={aclError}
        draft={aclDraft}
        draftError={aclDraftError}
        actionInFlight={aclActionInFlight}
        onVolumeChange={onAclVolumeChange}
        onPathInputChange={onAclPathInputChange}
        onPathSubmit={onAclPathSubmit}
        onDraftChange={onAclDraftChange}
        onReplace={onAclReplace}
        onClear={onAclClear}
      />
    );
  }

  if (page === 'csi') {
    return (
      <CsiDashboardPage
        result={csiDashboard}
        loading={csiLoading}
        error={csiError}
        namespaceInput={csiNamespaceInput}
        volumeInput={csiVolumeInput}
        onNamespaceChange={onCsiNamespaceChange}
        onVolumeChange={onCsiVolumeChange}
        onFilterSubmit={onCsiFilterSubmit}
        onRefresh={onCsiRefresh}
      />
    );
  }

  return <SettingsPage health={health} volumes={volumes} instances={instances} />;
}

function TrashPage({
  volumes,
  selectedVolume,
  result,
  loading,
  error,
  actionInFlight,
  onVolumeChange,
  onRestore,
  onDelete,
}: {
  volumes: VolumeResponse[];
  selectedVolume: VolumeResponse | null;
  result: TrashViewResult | null;
  loading: boolean;
  error: string | null;
  actionInFlight: string | null;
  onVolumeChange: (volumeId: string) => void;
  onRestore: (entryId: string) => void;
  onDelete: (entryId: string) => void;
}) {
  return (
    <article className="panel table-panel">
      <h2>{result?.title ?? 'Trash'}</h2>
      {volumes.length > 0 ? (
        <div className="page-toolbar">
          <label>
            Filesystem
            <select
              value={selectedVolume?.id ?? ''}
              onChange={(event) => onVolumeChange(event.target.value)}
            >
              {volumes.map((volume) => (
                <option key={volume.id} value={volume.id}>
                  {volume.name}
                </option>
              ))}
            </select>
          </label>
        </div>
      ) : null}
      {loading && !result ? <p className="muted">Loading trash.</p> : null}
      {error ? <p className="error-text">{error}</p> : null}
      {result ? (
        <>
          <Metric label="State" value={result.state} />
          {result.volumeName ? <Metric label="Filesystem" value={result.volumeName} /> : null}
          <p className="muted feature-message">{result.message}</p>
          {result.entries.length > 0 ? (
            <div className="table-wrap">
              <table className="data-table compact-data-table">
                <thead>
                  <tr>
                    <th>ID</th>
                    <th>Original path</th>
                    <th>Size</th>
                    <th>Deleted</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {result.entries.map((entry) => (
                    <tr key={entry.id}>
                      <td>{entry.id}</td>
                      <td>{entry.path}</td>
                      <td>{entry.size}</td>
                      <td>{entry.deletedAt}</td>
                      <td>
                        <div className="table-actions">
                          <button
                            className="secondary-button compact-button"
                            type="button"
                            disabled={Boolean(actionInFlight) || !result.actions.restoreSupported}
                            onClick={() => onRestore(entry.id)}
                          >
                            <RotateCcw size={14} aria-hidden="true" />
                            <span>
                              {actionInFlight === `restore:${entry.id}` ? 'Restoring' : 'Restore'}
                            </span>
                          </button>
                          <button
                            className="danger-button compact-button"
                            type="button"
                            disabled={Boolean(actionInFlight) || !result.actions.deleteSupported}
                            title={result.actions.deleteDisabledReason ?? undefined}
                            onClick={() => onDelete(entry.id)}
                          >
                            <Trash2 size={14} aria-hidden="true" />
                            <span>
                              {actionInFlight === `delete:${entry.id}` ? 'Deleting' : 'Delete'}
                            </span>
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : null}
        </>
      ) : null}
    </article>
  );
}

function AclPage({
  volumes,
  selectedVolume,
  capabilityWarning,
  pathInput,
  result,
  loading,
  error,
  draft,
  draftError,
  actionInFlight,
  onVolumeChange,
  onPathInputChange,
  onPathSubmit,
  onDraftChange,
  onReplace,
  onClear,
}: {
  volumes: VolumeResponse[];
  selectedVolume: VolumeResponse | null;
  capabilityWarning: string | null;
  pathInput: string;
  result: AclViewResult | null;
  loading: boolean;
  error: string | null;
  draft: string;
  draftError: string | null;
  actionInFlight: string | null;
  onVolumeChange: (volumeId: string) => void;
  onPathInputChange: (path: string) => void;
  onPathSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onDraftChange: (value: string) => void;
  onReplace: (event: FormEvent<HTMLFormElement>) => void;
  onClear: () => void;
}) {
  return (
    <article className="panel table-panel">
      <h2>{result?.title ?? 'ACL'}</h2>
      <div className="page-toolbar">
        {volumes.length > 0 ? (
          <label>
            Filesystem
            <select
              value={selectedVolume?.id ?? ''}
              onChange={(event) => onVolumeChange(event.target.value)}
            >
              {volumes.map((volume) => (
                <option key={volume.id} value={volume.id}>
                  {volume.name}
                </option>
              ))}
            </select>
          </label>
        ) : null}
        <form className="browser-path-form" onSubmit={onPathSubmit}>
          <label>
            Path
            <input value={pathInput} onChange={(event) => onPathInputChange(event.target.value)} />
          </label>
          <button className="secondary-button compact-button" type="submit">
            <FileSearch size={14} aria-hidden="true" />
            <span>Load</span>
          </button>
        </form>
      </div>
      {capabilityWarning ? <p className="warning-text">{capabilityWarning}</p> : null}
      {loading && !result ? <p className="muted">Loading ACL.</p> : null}
      {error ? <p className="error-text">{error}</p> : null}
      {result ? (
        <>
          <Metric label="State" value={result.state} />
          {result.volumeName ? <Metric label="Filesystem" value={result.volumeName} /> : null}
          <Metric label="Path" value={result.path} />
          <p className="muted feature-message">{result.message}</p>
          {result.entries.length > 0 ? (
            <div className="table-wrap">
              <table className="data-table compact-data-table">
                <thead>
                  <tr>
                    <th>Scope</th>
                    <th>Tag</th>
                    <th>ID</th>
                    <th>Perm</th>
                  </tr>
                </thead>
                <tbody>
                  {result.entries.map((entry) => (
                    <tr key={`${entry.scope}:${entry.tag}:${entry.id}`}>
                      <td>{entry.scope}</td>
                      <td>{entry.tag}</td>
                      <td>{entry.id}</td>
                      <td>{entry.perm}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : null}
          {result.state === 'ready' ? (
            <form className="acl-editor" onSubmit={onReplace}>
              <label>
                <span>Entries JSON</span>
                <textarea
                  value={draft}
                  onChange={(event) => onDraftChange(event.target.value)}
                  spellCheck={false}
                />
              </label>
              {draftError ? <p className="error-text">{draftError}</p> : null}
              <div className="form-actions">
                <button
                  className="primary-button"
                  type="submit"
                  disabled={Boolean(actionInFlight)}
                >
                  <ShieldCheck size={16} aria-hidden="true" />
                  <span>{actionInFlight === 'replace' ? 'Saving' : 'Replace ACL'}</span>
                </button>
                <button
                  className="danger-button"
                  type="button"
                  disabled={Boolean(actionInFlight)}
                  onClick={onClear}
                >
                  <Trash2 size={16} aria-hidden="true" />
                  <span>{actionInFlight === 'clear' ? 'Clearing' : 'Clear ACL'}</span>
                </button>
              </div>
            </form>
          ) : null}
        </>
      ) : null}
    </article>
  );
}

function CsiDashboardPage({
  result,
  loading,
  error,
  namespaceInput,
  volumeInput,
  onNamespaceChange,
  onVolumeChange,
  onFilterSubmit,
  onRefresh,
}: {
  result: CsiDashboardResult | null;
  loading: boolean;
  error: string | null;
  namespaceInput: string;
  volumeInput: string;
  onNamespaceChange: (value: string) => void;
  onVolumeChange: (value: string) => void;
  onFilterSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onRefresh: () => void;
}) {
  return (
    <article className="panel table-panel">
      <h2>{result?.title ?? 'CSI dashboard'}</h2>
      <form className="csi-filter-form" onSubmit={onFilterSubmit}>
        <label>
          <span>Namespace</span>
          <input
            value={namespaceInput}
            onChange={(event) => onNamespaceChange(event.target.value)}
          />
        </label>
        <label>
          <span>Volume</span>
          <input value={volumeInput} onChange={(event) => onVolumeChange(event.target.value)} />
        </label>
        <button className="primary-button" type="submit" disabled={loading}>
          <Database size={16} aria-hidden="true" />
          <span>Apply</span>
        </button>
        <button className="secondary-button" type="button" onClick={onRefresh} disabled={loading}>
          <RefreshCw size={16} aria-hidden="true" />
          <span>Refresh</span>
        </button>
      </form>
      {loading && !result ? <p className="muted">Loading CSI resources.</p> : null}
      {error ? <p className="error-text">{error}</p> : null}
      {result ? (
        <>
          <Metric label="State" value={result.state} />
          <p className="muted feature-message">{result.message}</p>
          {result.warnings.length > 0 ? (
            <div className="warning-list">
              {result.warnings.map((warning) => (
                <p className="warning-text" key={warning}>
                  {warning}
                </p>
              ))}
            </div>
          ) : null}
          {result.summaryMetrics.length > 0 ? (
            <div className="metadata-grid">
              {result.summaryMetrics.map((metric) => (
                <Metric key={metric.label} label={metric.label} value={metric.value} />
              ))}
            </div>
          ) : null}
          {result.resources.length > 0 ? (
            <div className="csi-resource-list">
              {result.resources.map((resource) => (
                <section className="csi-resource-section" key={resource.key}>
                  <div className="resource-heading">
                    <h3>{resource.title}</h3>
                    <span>
                      {resource.state} · {formatCsiItemCount(resource.count)}
                    </span>
                  </div>
                  <p className="muted feature-message">{resource.message}</p>
                  {resource.rows.length > 0 ? (
                    <div className="table-wrap">
                      <table className="data-table csi-resource-table">
                        <thead>
                          <tr>
                            <th>Namespace</th>
                            <th>Name</th>
                            <th>Status</th>
                            <th>Detail</th>
                          </tr>
                        </thead>
                        <tbody>
                          {resource.rows.map((row) => (
                            <tr key={`${resource.key}:${row.namespace}:${row.name}:${row.detail}`}>
                              <td>{row.namespace}</td>
                              <td>{row.name}</td>
                              <td>{row.status}</td>
                              <td>{row.detail}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : null}
                </section>
              ))}
            </div>
          ) : null}
        </>
      ) : null}
    </article>
  );
}

function SettingsPage({
  health,
  volumes,
  instances,
}: {
  health: HealthResponse | null;
  volumes: VolumeResponse[];
  instances: InstanceResponse[];
}) {
  const summary = buildSettingsSummary(health, volumes, instances);

  return (
    <article className="panel">
      <h2>Settings</h2>
      <div className="metadata-grid">
        {summary.metrics.map((metric) => (
          <Metric key={metric.label} label={metric.label} value={metric.value} />
        ))}
      </div>
    </article>
  );
}

function BrowserPage({
  volumes,
  selectedVolume,
  pathInput,
  result,
  loading,
  error,
  metadata,
  linkTarget,
  metadataLoading,
  metadataError,
  onVolumeChange,
  onPathInputChange,
  onPathSubmit,
  onRefresh,
  onNavigate,
  onInspect,
}: {
  volumes: VolumeResponse[];
  selectedVolume: VolumeResponse | null;
  pathInput: string;
  result: FileListResponse | null;
  loading: boolean;
  error: string | null;
  metadata: FileStatResponse | null;
  linkTarget: string | null;
  metadataLoading: boolean;
  metadataError: string | null;
  onVolumeChange: (volumeId: string) => void;
  onPathInputChange: (path: string) => void;
  onPathSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onRefresh: () => void;
  onNavigate: (path: string) => void;
  onInspect: (path: string) => void;
}) {
  if (volumes.length === 0) {
    return <EmptyPanel title="No registered filesystems" detail="Register a filesystem first." />;
  }

  const currentPath = result?.path ?? normalizeBrowserPath(pathInput);
  const breadcrumbs = browserBreadcrumbs(currentPath);
  const dataActions = browserMvpDataActions();

  return (
    <>
      <article className="panel table-panel">
        <div className="browser-toolbar">
          <label>
            <span>Filesystem</span>
            <select
              value={selectedVolume?.id ?? ''}
              onChange={(event) => onVolumeChange(event.target.value)}
            >
              {volumes.map((volume) => (
                <option key={volume.id} value={volume.id}>
                  {volume.name}
                </option>
              ))}
            </select>
          </label>
          <form className="browser-path-form" onSubmit={onPathSubmit}>
            <label>
              <span>Path</span>
              <input
                value={pathInput}
                onChange={(event) => onPathInputChange(event.target.value)}
                placeholder="/"
              />
            </label>
            <button className="primary-button" type="submit" disabled={loading}>
              <FileSearch size={16} aria-hidden="true" />
              <span>Open</span>
            </button>
          </form>
          <button
            className="secondary-button"
            type="button"
            onClick={onRefresh}
            disabled={loading || !selectedVolume}
          >
            <RefreshCw size={16} aria-hidden="true" />
            <span>Refresh</span>
          </button>
        </div>
        {selectedVolume?.mount_config.mount_point ? (
          <p className="muted browser-context">
            {selectedVolume.mount_config.mount_point} · {currentPath}
          </p>
        ) : null}
        <nav className="browser-breadcrumbs" aria-label="Browser path">
          {breadcrumbs.map((crumb, index) => (
            <span className="browser-breadcrumb-item" key={crumb.path}>
              {index > 0 ? (
                <span className="browser-breadcrumb-separator" aria-hidden="true">
                  /
                </span>
              ) : null}
              <button
                className="browser-breadcrumb-button"
                type="button"
                onClick={() => onNavigate(crumb.path)}
                disabled={loading || crumb.current}
                aria-current={crumb.current ? 'page' : undefined}
              >
                <span>{crumb.path === '/' ? 'Root' : crumb.label}</span>
              </button>
            </span>
          ))}
        </nav>
        {error ? <p className="error-text">{error}</p> : null}
      </article>

      <article className="panel table-panel">
        <div className="table-actions browser-actions">
          <button
            className="secondary-button compact-button"
            type="button"
            onClick={() => onNavigate(parentBrowserPath(currentPath))}
            disabled={loading || currentPath === '/'}
          >
            <ArrowUp size={14} aria-hidden="true" />
            <span>Parent</span>
          </button>
        </div>
        {loading && !result ? <p className="muted">Loading directory.</p> : null}
        {result && result.entries.length === 0 ? (
          <p className="muted">Directory is empty.</p>
        ) : null}
        {result && result.entries.length > 0 ? (
          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Type</th>
                  <th>Inode</th>
                  <th>Size</th>
                  <th>Mode</th>
                  <th>Flags</th>
                  <th>Owner</th>
                  <th>Modified</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>
                {result.entries.map((entry) => {
                  const entryPath = joinBrowserPath(result.path, entry.name);
                  return (
                    <tr key={`${entry.inode}-${entry.name}`}>
                      <td>{entry.name}</td>
                      <td>{entry.kind}</td>
                      <td>{entry.inode}</td>
                      <td>{entry.size}</td>
                      <td>{formatMode(entry.mode)}</td>
                      <td>{formatBrowserEntryFlags(entry)}</td>
                      <td>
                        {entry.uid}:{entry.gid}
                      </td>
                      <td>{entry.mtime}</td>
                      <td>
                        <div className="table-actions">
                          <button
                            className="secondary-button compact-button"
                            type="button"
                            onClick={() => onInspect(entryPath)}
                            disabled={loading || metadataLoading}
                          >
                            <FileSearch size={14} aria-hidden="true" />
                            <span>Inspect</span>
                          </button>
                          {entry.kind === 'directory' ? (
                            <button
                              className="secondary-button compact-button"
                              type="button"
                              onClick={() => onNavigate(entryPath)}
                              disabled={loading || metadataLoading}
                            >
                              <FolderTree size={14} aria-hidden="true" />
                              <span>Open</span>
                            </button>
                          ) : null}
                          {showsBrowserDataActionsForKind(entry.kind)
                            ? dataActions.map((action) => {
                                const DataActionIcon =
                                  action.key === 'download' ? Download : Pencil;
                                return (
                                  <button
                                    key={action.key}
                                    className="secondary-button compact-button"
                                    type="button"
                                    disabled={!action.enabled}
                                    title={action.reason}
                                    aria-label={`${action.label}: ${action.reason}`}
                                  >
                                    <DataActionIcon size={14} aria-hidden="true" />
                                    <span>{action.label}</span>
                                  </button>
                                );
                              })
                            : null}
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : null}
      </article>

      <article className="panel">
        <h2>Metadata</h2>
        {metadataLoading ? <p className="muted">Loading metadata.</p> : null}
        {metadataError ? <p className="error-text">{metadataError}</p> : null}
        {metadata ? (
          <>
            <p className="muted metadata-path">{metadata.path}</p>
            <div className="metadata-grid">
              <Metric label="Type" value={metadata.kind} />
              <Metric label="Inode" value={String(metadata.inode)} />
              <Metric label="Size" value={String(metadata.size)} />
              <Metric label="Mode" value={formatMode(metadata.mode)} />
              <Metric label="Owner" value={`${metadata.uid}:${metadata.gid}`} />
              <Metric label="Modified" value={metadata.mtime} />
            </div>
            {linkTarget ? (
              <p className="muted metadata-target">target: {linkTarget}</p>
            ) : null}
          </>
        ) : metadataLoading || metadataError ? null : (
          <p className="muted">No entry selected.</p>
        )}
      </article>
    </>
  );
}

function JobsPage({
  instances,
  selectedPid,
  dryRun,
  requestInFlight,
  error,
  currentJob,
  onSelectedPidChange,
  onDryRunChange,
  onSubmit,
  onRefresh,
}: {
  instances: InstanceResponse[];
  selectedPid: number | null;
  dryRun: boolean;
  requestInFlight: boolean;
  error: string | null;
  currentJob: CurrentJob | null;
  onSelectedPidChange: (pid: number) => void;
  onDryRunChange: (enabled: boolean) => void;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onRefresh: () => void;
}) {
  if (instances.length === 0) {
    return <EmptyPanel title="No live instances" detail="GC jobs require a mounted BrewFS instance." />;
  }

  const gc = currentJob?.status.outcome?.Gc;

  return (
    <>
      <article className="panel">
        <h2>Run GC</h2>
        <form className="job-form" onSubmit={onSubmit}>
          <label>
            <span>Instance</span>
            <select
              value={selectedPid ?? ''}
              onChange={(event) => onSelectedPidChange(Number(event.target.value))}
            >
              {instances.map((instance) => (
                <option key={instance.pid} value={instance.pid}>
                  {instance.mount_point} · pid {instance.pid}
                </option>
              ))}
            </select>
          </label>
          <label className="check-row">
            <input
              type="checkbox"
              checked={dryRun}
              onChange={(event) => onDryRunChange(event.target.checked)}
            />
            <span>Dry run</span>
          </label>
          {error ? <p className="error-text">{error}</p> : null}
          <button className="primary-button" type="submit" disabled={requestInFlight}>
            <Activity size={16} aria-hidden="true" />
            <span>{requestInFlight ? 'Working' : 'Start GC'}</span>
          </button>
        </form>
      </article>

      <article className="panel">
        <h2>Latest job</h2>
        {currentJob ? (
          <>
            <Metric label="Instance" value={`pid ${currentJob.pid}`} />
            <Metric label="Job ID" value={currentJob.status.job_id} />
            <Metric label="State" value={currentJob.status.state} />
            {currentJob.status.detail ? (
              <p className="muted job-detail">{currentJob.status.detail}</p>
            ) : null}
            {gc ? (
              <div className="job-metrics">
                <Metric label="Orphan slices" value={String(gc.orphan_slice_count)} />
                <Metric label="Orphan objects" value={String(gc.orphan_object_count)} />
                <Metric label="Deleted objects" value={String(gc.deleted_object_count)} />
                <Metric label="Errors" value={String(gc.error_count)} />
              </div>
            ) : null}
            <button
              className="secondary-button"
              type="button"
              onClick={onRefresh}
              disabled={requestInFlight}
            >
              <RefreshCw size={16} aria-hidden="true" />
              <span>Refresh</span>
            </button>
          </>
        ) : (
          <p className="muted">No job has been started from this console session.</p>
        )}
      </article>
    </>
  );
}

function FilesystemsPage({
  volumes,
  volumeError,
  instanceDetails,
  form,
  creating,
  createError,
  editingVolumeId,
  editForm,
  actionInFlight,
  actionError,
  copiedMountCommandVolumeId,
  mountCommandError,
  onChange,
  onSubmit,
  onEditStart,
  onEditFormChange,
  onEditSubmit,
  onEditCancel,
  onDelete,
  onMountCommandCopy,
}: {
  volumes: VolumeResponse[];
  volumeError: string | null;
  instanceDetails: Record<number, InstanceInfoResponse>;
  form: VolumeFormState;
  creating: boolean;
  createError: string | null;
  editingVolumeId: string | null;
  editForm: VolumeEditFormState | null;
  actionInFlight: boolean;
  actionError: string | null;
  copiedMountCommandVolumeId: string | null;
  mountCommandError: string | null;
  onChange: (field: keyof VolumeFormState, value: string) => void;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onEditStart: (volume: VolumeResponse) => void;
  onEditFormChange: (field: keyof VolumeEditFormState, value: string) => void;
  onEditSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onEditCancel: () => void;
  onDelete: (volume: VolumeResponse) => void;
  onMountCommandCopy: (volume: VolumeResponse) => void;
}) {
  return (
    <>
      <article className="panel table-panel">
        <h2>Registered filesystems</h2>
        {volumeError ? <p className="error-text">{volumeError}</p> : null}
        {actionError ? <p className="error-text">{actionError}</p> : null}
        {mountCommandError ? <p className="error-text">{mountCommandError}</p> : null}
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
                  <th>Runtime</th>
                  <th>Capabilities</th>
                  <th>Meta URL</th>
                  <th>Command</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {volumes.map((volume) => {
                  const mountCommand = buildMountCommand(volume);
                  const capabilitySummary = summarizeVolumeCapabilities(volume, instanceDetails);
                  const copied = copiedMountCommandVolumeId === volume.id;
                  return (
                    <tr key={volume.id}>
                      <td>{volume.name}</td>
                      <td>{volume.mount_config.data_backend}</td>
                      <td>{volume.mount_config.meta_backend}</td>
                      <td>{volume.mount_config.mount_point ?? '-'}</td>
                      <td>{formatVolumeRuntime(volume.runtime)}</td>
                      <td>
                        <div className="capability-summary">
                          <strong>{capabilitySummary.label}</strong>
                          {capabilitySummary.state === 'ready' ? (
                            <>
                              <span>on: {capabilitySummary.enabled.join(', ') || 'none'}</span>
                              <span>off: {capabilitySummary.disabled.join(', ') || 'none'}</span>
                            </>
                          ) : (
                            <span>
                              {capabilitySummary.state === 'offline'
                                ? 'mount filesystem to inspect capabilities'
                                : 'runtime details unavailable'}
                            </span>
                          )}
                        </div>
                      </td>
                      <td>{volume.mount_config.meta_url_redacted ?? '-'}</td>
                      <td>
                        <div className="mount-command">
                          <code>{mountCommand.command}</code>
                          {mountCommand.warnings.map((warning) => (
                            <span className="muted" key={warning}>
                              {warning}
                            </span>
                          ))}
                        </div>
                      </td>
                      <td>
                        <div className="table-actions">
                          <button
                            className="secondary-button compact-button"
                            type="button"
                            onClick={() => onMountCommandCopy(volume)}
                            disabled={actionInFlight}
                          >
                            {copied ? (
                              <Check size={14} aria-hidden="true" />
                            ) : (
                              <Copy size={14} aria-hidden="true" />
                            )}
                            <span>{copied ? 'Copied' : 'Copy'}</span>
                          </button>
                          <button
                            className="secondary-button compact-button"
                            type="button"
                            onClick={() => onEditStart(volume)}
                            disabled={actionInFlight}
                          >
                            <Pencil size={14} aria-hidden="true" />
                            <span>Edit</span>
                          </button>
                          <button
                            className="danger-button compact-button"
                            type="button"
                            onClick={() => onDelete(volume)}
                            disabled={actionInFlight}
                          >
                            <Trash2 size={14} aria-hidden="true" />
                            <span>Delete</span>
                          </button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
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

      {editingVolumeId && editForm ? (
        <article className="panel">
          <h2>Edit registry entry</h2>
          <form className="volume-form" onSubmit={onEditSubmit}>
            <label>
              <span>Name</span>
              <input
                required
                value={editForm.name}
                onChange={(event) => onEditFormChange('name', event.target.value)}
              />
            </label>
            <label>
              <span>Description</span>
              <input
                value={editForm.description}
                onChange={(event) => onEditFormChange('description', event.target.value)}
              />
            </label>
            <label>
              <span>Labels</span>
              <textarea
                value={editForm.labels}
                onChange={(event) => onEditFormChange('labels', event.target.value)}
              />
            </label>
            <div className="form-actions">
              <button className="primary-button" type="submit" disabled={actionInFlight}>
                <HardDrive size={16} aria-hidden="true" />
                <span>{actionInFlight ? 'Saving' : 'Save'}</span>
              </button>
              <button
                className="secondary-button"
                type="button"
                onClick={onEditCancel}
                disabled={actionInFlight}
              >
                <X size={16} aria-hidden="true" />
                <span>Cancel</span>
              </button>
            </div>
          </form>
        </article>
      ) : null}
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

function browserErrorMessage(err: unknown): string {
  if (err instanceof ApiError) {
    if (err.status === 400) return 'Path must be absolute.';
    if (err.status === 404) return 'Directory was not found.';
    if (err.status === 409) return 'Filesystem is registered but not mounted.';
  }
  return err instanceof Error ? err.message : 'file list request failed';
}

function optionalField(value: string): string | undefined {
  const trimmed = value.trim();
  return trimmed ? trimmed : undefined;
}
