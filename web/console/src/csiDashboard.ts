import {
  ApiError,
  fetchCsiPersistentVolumeClaims,
  fetchCsiPersistentVolumes,
  fetchCsiPods,
  fetchCsiStorageClasses,
  fetchCsiSummary,
  type CsiResourceListResponse,
} from './api';

export type CsiDashboardState = 'ready' | 'unsupported' | 'unavailable';
export type CsiResourceKey =
  | 'storageclasses'
  | 'persistentvolumes'
  | 'persistentvolumeclaims'
  | 'pods';

export interface CsiDashboardMetric {
  label: string;
  value: string;
}

export interface CsiResourceRow {
  namespace: string;
  name: string;
  status: string;
  detail: string;
}

export interface CsiDashboardFilters {
  namespace?: string;
  volume?: string;
}

export interface CsiResourceStatus {
  key: CsiResourceKey;
  title: string;
  state: CsiDashboardState;
  count: number | null;
  message: string;
  items: unknown[];
  rows: CsiResourceRow[];
}

export interface CsiDashboardResult {
  state: CsiDashboardState;
  title: string;
  message: string;
  warnings: string[];
  summaryMetrics: CsiDashboardMetric[];
  resources: CsiResourceStatus[];
}

export function formatCsiItemCount(count: number | null): string {
  if (count === null) return 'items unavailable';
  return count === 1 ? '1 item' : `${count} items`;
}

export function shouldLoadCsiDashboardForPage(page: string, csiDashboardEnabled: boolean): boolean {
  return page === 'csi' || (page === 'overview' && csiDashboardEnabled);
}

type CsiResourceDescriptor = {
  key: CsiResourceKey;
  title: string;
  load: (
    filters: CsiDashboardFilters,
    token?: string | null,
  ) => Promise<CsiResourceListResponse>;
};

const resourceDescriptors: CsiResourceDescriptor[] = [
  {
    key: 'storageclasses',
    title: 'StorageClasses',
    load: (_filters, token) => fetchCsiStorageClasses(token),
  },
  {
    key: 'persistentvolumes',
    title: 'PersistentVolumes',
    load: (_filters, token) => fetchCsiPersistentVolumes(token),
  },
  {
    key: 'persistentvolumeclaims',
    title: 'PersistentVolumeClaims',
    load: (filters, token) => fetchCsiPersistentVolumeClaims(filters.namespace, token),
  },
  {
    key: 'pods',
    title: 'Pods',
    load: (filters, token) => fetchCsiPods(filters, token),
  },
];

export async function loadCsiDashboard(
  token?: string | null,
  filters: CsiDashboardFilters = {},
): Promise<CsiDashboardResult> {
  try {
    const summary = await fetchCsiSummary(token);
    const resources = await Promise.all(
      resourceDescriptors.map((descriptor) => loadResourceStatus(descriptor, filters, token)),
    );

    return {
      state: 'ready',
      title: 'CSI dashboard',
      message: `${summary.pods} pods reference BrewFS volumes; ${summary.unhealthy_mounts} mounts need attention.`,
      warnings: csiWarnings(resources),
      summaryMetrics: [
        { label: 'StorageClasses', value: String(summary.storageclasses) },
        { label: 'PersistentVolumes', value: String(summary.persistentvolumes) },
        { label: 'PersistentVolumeClaims', value: String(summary.persistentvolumeclaims) },
        { label: 'Pods', value: String(summary.pods) },
        { label: 'Unhealthy mounts', value: String(summary.unhealthy_mounts) },
      ],
      resources,
    };
  } catch (err: unknown) {
    return dashboardErrorOrThrow(err);
  }
}

async function loadResourceStatus(
  descriptor: CsiResourceDescriptor,
  filters: CsiDashboardFilters,
  token?: string | null,
): Promise<CsiResourceStatus> {
  try {
    const response = await descriptor.load(filters, token);
    return {
      key: descriptor.key,
      title: descriptor.title,
      state: 'ready',
      count: response.items.length,
      message: `${response.items.length} items`,
      items: response.items,
      rows: response.items.map((item) => resourceRow(descriptor.key, item)),
    };
  } catch (err: unknown) {
    return resourceErrorOrThrow(descriptor, err);
  }
}

function dashboardErrorOrThrow(err: unknown): CsiDashboardResult {
  if (err instanceof ApiError && err.status === 409) {
    return {
      state: 'unavailable',
      title: 'CSI dashboard unavailable',
      message: 'Enable the CSI dashboard integration before loading Kubernetes resources.',
      warnings: [],
      summaryMetrics: [],
      resources: [],
    };
  }
  if (isKubernetesError(err)) {
    return {
      state: 'unavailable',
      title: 'Kubernetes CSI unavailable',
      message: err.detail ?? 'Kubernetes resource discovery failed.',
      warnings: [],
      summaryMetrics: [],
      resources: [],
    };
  }
  if (err instanceof ApiError && (err.code === 'unsupported' || err.status === 422)) {
    return {
      state: 'unsupported',
      title: 'CSI dashboard unsupported',
      message: 'The server exposes CSI endpoints but no Kubernetes adapter is connected yet.',
      warnings: [],
      summaryMetrics: [],
      resources: [],
    };
  }
  throw err;
}

function resourceErrorOrThrow(descriptor: CsiResourceDescriptor, err: unknown): CsiResourceStatus {
  if (err instanceof ApiError && err.status === 409) {
    return {
      key: descriptor.key,
      title: descriptor.title,
      state: 'unavailable',
      count: null,
      message: 'CSI dashboard integration is disabled.',
      items: [],
      rows: [],
    };
  }
  if (isKubernetesError(err)) {
    return {
      key: descriptor.key,
      title: descriptor.title,
      state: 'unavailable',
      count: null,
      message: err.detail ?? 'Kubernetes resource discovery failed.',
      items: [],
      rows: [],
    };
  }
  if (err instanceof ApiError && (err.code === 'unsupported' || err.status === 422)) {
    return {
      key: descriptor.key,
      title: descriptor.title,
      state: 'unsupported',
      count: null,
      message: 'Kubernetes adapter support is not implemented for this resource yet.',
      items: [],
      rows: [],
    };
  }
  throw err;
}

function isKubernetesError(err: unknown): err is ApiError {
  return err instanceof ApiError && err.status === 502 && err.code === 'kubernetes_error';
}

function csiWarnings(resources: CsiResourceStatus[]): string[] {
  return resources.flatMap((resource) => {
    if (resource.key === 'persistentvolumes') {
      return resource.rows
        .filter((row) => row.status !== '-' && row.status !== 'Bound')
        .map((row) => `PersistentVolume ${row.name} is ${row.status}; inspect claim and reclaim state.`);
    }

    if (resource.key === 'pods') {
      return resource.rows
        .filter((row) => row.status !== 'Running · Ready')
        .map(
          (row) =>
            `Pod ${row.namespace}/${row.name} is ${row.status}; inspect node mount and PVC attachment.`,
        );
    }

    return [];
  });
}

function resourceRow(key: CsiResourceKey, item: unknown): CsiResourceRow {
  return {
    namespace: resourceNamespace(item),
    name: resourceName(item),
    ...resourceStatus(key, item),
  };
}

function resourceName(item: unknown): string {
  return nestedString(item, ['metadata', 'name']) ?? '-';
}

function resourceNamespace(item: unknown): string {
  return nestedString(item, ['metadata', 'namespace']) ?? '-';
}

function resourceStatus(
  key: CsiResourceKey,
  item: unknown,
): Pick<CsiResourceRow, 'status' | 'detail'> {
  if (key === 'storageclasses') {
    const provisioner = nestedString(item, ['provisioner']) ?? '-';
    return {
      status: provisioner,
      detail: provisioner === '-' ? '-' : `provisioner ${provisioner}`,
    };
  }

  if (key === 'persistentvolumes') {
    const phase = nestedString(item, ['status', 'phase']) ?? '-';
    const storageClass = nestedString(item, ['spec', 'storageClassName']);
    const claimNamespace = nestedString(item, ['spec', 'claimRef', 'namespace']);
    const claimName = nestedString(item, ['spec', 'claimRef', 'name']);
    const handle = nestedString(item, ['spec', 'csi', 'volumeHandle']);
    const claim = claimName
      ? `claim ${claimNamespace ? `${claimNamespace}/` : ''}${claimName}`
      : null;
    return {
      status: phase,
      detail: joinDetails([
        storageClass ? `storageClass ${storageClass}` : null,
        claim,
        handle ? `handle ${handle}` : null,
      ]),
    };
  }

  if (key === 'persistentvolumeclaims') {
    const phase = nestedString(item, ['status', 'phase']);
    const storageClass = nestedString(item, ['spec', 'storageClassName']);
    const volumeName = nestedString(item, ['spec', 'volumeName']);
    return {
      status: phase ?? (volumeName ? 'Bound' : '-'),
      detail: joinDetails([
        storageClass ? `storageClass ${storageClass}` : null,
        volumeName ? `volume ${volumeName}` : null,
      ]),
    };
  }

  const phase = nestedString(item, ['status', 'phase']) ?? '-';
  const ready = podReadyStatus(item);
  return {
    status: joinDetails([phase, ready]),
    detail: joinDetails(podVolumeDetails(item)),
  };
}

function podReadyStatus(item: unknown): string | null {
  const ready = nestedArray(item, ['status', 'conditions'])?.find(
    (condition) => nestedString(condition, ['type']) === 'Ready',
  );
  const status = nestedString(ready, ['status']);
  if (status === 'True') return 'Ready';
  if (status === 'False') return 'NotReady';
  return null;
}

function podVolumeDetails(item: unknown): string[] {
  return (
    nestedArray(item, ['spec', 'volumes'])
      ?.map((volume) => {
        const pvc = nestedString(volume, ['persistentVolumeClaim', 'claimName']);
        if (pvc) return `pvc ${pvc}`;
        const driver = nestedString(volume, ['csi', 'driver']);
        if (driver) return `csi ${driver}`;
        const name = nestedString(volume, ['name']);
        return name ? `volume ${name}` : null;
      })
      .filter((value): value is string => Boolean(value)) ?? []
  );
}

function joinDetails(parts: Array<string | null | undefined>): string {
  const values = parts.filter((part): part is string => Boolean(part));
  return values.length > 0 ? values.join(' · ') : '-';
}

function nestedString(item: unknown, path: string[]): string | null {
  const value = nestedValue(item, path);
  return typeof value === 'string' && value.length > 0 ? value : null;
}

function nestedArray(item: unknown, path: string[]): unknown[] | null {
  const value = nestedValue(item, path);
  return Array.isArray(value) ? value : null;
}

function nestedValue(item: unknown, path: string[]): unknown {
  let current = item;
  for (const segment of path) {
    if (!isRecord(current)) return null;
    current = current[segment];
  }
  return current;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
