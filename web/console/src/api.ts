export type AuthMode = 'disabled' | 'token';

export interface HealthResponse {
  service: 'brewfs-console';
  version: string;
  commit_short: string;
  auth_mode: AuthMode;
  integrations: {
    csi_dashboard: boolean;
  };
  static_assets_available: boolean;
}

export interface VolumeResponse {
  id: string;
  name: string;
  description: string | null;
  labels: Record<string, string>;
  created_at: string;
  updated_at: string;
  mount_config: VolumeMountConfigResponse;
  runtime: VolumeRuntimeResponse;
}

export interface VolumeMountConfigResponse {
  mount_point: string | null;
  data_backend: string;
  data_dir: string | null;
  meta_backend: string;
  meta_url_redacted: string | null;
  chunk_size: number | null;
  block_size: number | null;
}

export interface VolumeRuntimeResponse {
  mounted: boolean;
  pid: number | null;
  mount_point: string | null;
  started_at: string | null;
}

export interface ListVolumesResponse {
  volumes: VolumeResponse[];
}

export interface InstanceResponse {
  pid: number;
  mount_point: string;
  socket_path: string;
  started_at: string;
}

export interface InstanceInfoResponse {
  pid: number;
  mount_point: string;
  started_at: number;
  version: string;
  meta_backend: string;
  capabilities: Record<string, boolean>;
}

export interface RunGcJobRequest {
  dry_run: boolean;
}

export interface AcceptedJobResponse {
  job_id: string;
}

export type JobState = 'Pending' | 'Running' | 'Succeeded' | 'Failed';

export interface GcJobResult {
  dry_run: boolean;
  orphan_slice_count: number;
  orphan_object_count: number;
  deleted_object_count: number;
  error_count: number;
  detail: string | null;
}

export interface JobOutcome {
  Gc?: GcJobResult;
}

export interface JobStatusResponse {
  job_id: string;
  state: JobState;
  detail: string | null;
  outcome: JobOutcome | null;
}

export interface FileEntryResponse {
  name: string;
  inode: number;
  kind: string;
  size: number;
  mode: number;
  uid: number;
  gid: number;
  mtime: string;
  has_acl?: boolean;
}

export interface FileListResponse {
  path: string;
  entries: FileEntryResponse[];
}

export interface FileStatResponse {
  path: string;
  inode: number;
  kind: string;
  size: number;
  mode: number;
  uid: number;
  gid: number;
  mtime: string;
}

export interface ReadLinkResponse {
  path: string;
  target: string;
}

export interface TrashResponse {
  entries: unknown[];
}

export interface AclEntry {
  scope: string;
  tag: string;
  id?: number;
  perm: string;
}

export interface AclResponse {
  entries: AclEntry[];
}

export interface AclUpdateRequest {
  entries: AclEntry[];
}

export interface CsiSummaryResponse {
  storageclasses: number;
  persistentvolumes: number;
  persistentvolumeclaims: number;
  pods: number;
  unhealthy_mounts: number;
}

export interface CsiResourceListResponse {
  items: unknown[];
}

export interface CsiPodsQuery {
  namespace?: string;
  volume?: string;
}

export interface ListInstancesResponse {
  instances: InstanceResponse[];
}

export interface CreateVolumeRequest {
  name: string;
  description?: string;
  labels?: Record<string, string>;
  mount_config: {
    mount_point?: string;
    data_backend: string;
    data_dir?: string;
    meta_backend: string;
    meta_url?: string;
    chunk_size?: number;
    block_size?: number;
  };
}

export interface UpdateVolumeRequest {
  name?: string;
  description?: string | null;
  labels?: Record<string, string>;
}

export class ApiError extends Error {
  readonly status: number;
  readonly code: string | null;
  readonly detail: string | null;

  constructor(
    message: string,
    status: number,
    code: string | null = null,
    detail: string | null = null,
  ) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.code = code;
    this.detail = detail;
  }
}

export async function fetchHealth(token?: string | null): Promise<HealthResponse> {
  const response = await fetch('/api/health', {
    headers: apiHeaders(token),
  });

  await assertOk(response, 'health request failed');

  return (await response.json()) as HealthResponse;
}

export async function fetchVolumes(token?: string | null): Promise<ListVolumesResponse> {
  const response = await fetch('/api/volumes', {
    headers: apiHeaders(token),
  });

  await assertOk(response, 'volumes request failed');

  return (await response.json()) as ListVolumesResponse;
}

export async function fetchVolume(
  volumeId: string,
  token?: string | null,
): Promise<VolumeResponse> {
  const response = await fetch(`/api/volumes/${encodeURIComponent(volumeId)}`, {
    headers: apiHeaders(token),
  });

  await assertOk(response, 'volume request failed');

  return (await response.json()) as VolumeResponse;
}

export async function updateVolume(
  volumeId: string,
  request: UpdateVolumeRequest,
  token?: string | null,
): Promise<VolumeResponse> {
  const response = await fetch(`/api/volumes/${encodeURIComponent(volumeId)}`, {
    method: 'PATCH',
    headers: apiHeaders(token, true),
    body: JSON.stringify(request),
  });

  await assertOk(response, 'volume update request failed');

  return (await response.json()) as VolumeResponse;
}

export async function deleteVolume(volumeId: string, token?: string | null): Promise<void> {
  const response = await fetch(`/api/volumes/${encodeURIComponent(volumeId)}`, {
    method: 'DELETE',
    headers: apiHeaders(token),
  });

  await assertOk(response, 'volume delete request failed');
}

export async function fetchInstances(token?: string | null): Promise<ListInstancesResponse> {
  const response = await fetch('/api/instances', {
    headers: apiHeaders(token),
  });

  await assertOk(response, 'instances request failed');

  return (await response.json()) as ListInstancesResponse;
}

export async function fetchInstanceInfo(
  pid: number,
  token?: string | null,
): Promise<InstanceInfoResponse> {
  const response = await fetch(`/api/instances/${pid}`, {
    headers: apiHeaders(token),
  });

  await assertOk(response, 'instance info request failed');

  return (await response.json()) as InstanceInfoResponse;
}

export async function runGcJob(
  pid: number,
  request: RunGcJobRequest,
  token?: string | null,
): Promise<AcceptedJobResponse> {
  const response = await fetch(`/api/instances/${pid}/jobs/gc`, {
    method: 'POST',
    headers: apiHeaders(token, true),
    body: JSON.stringify(request),
  });

  await assertOk(response, 'start GC job request failed');

  return (await response.json()) as AcceptedJobResponse;
}

export async function fetchJobStatus(
  pid: number,
  jobId: string,
  token?: string | null,
): Promise<JobStatusResponse> {
  const response = await fetch(`/api/instances/${pid}/jobs/${encodeURIComponent(jobId)}`, {
    headers: apiHeaders(token),
  });

  await assertOk(response, 'job status request failed');

  return (await response.json()) as JobStatusResponse;
}

export async function fetchFileList(
  volumeId: string,
  path: string,
  token?: string | null,
): Promise<FileListResponse> {
  const response = await fetch(
    `/api/volumes/${encodeURIComponent(volumeId)}/files?${pathSearch(path)}`,
    {
      headers: apiHeaders(token),
    },
  );

  await assertOk(response, 'file list request failed');

  return (await response.json()) as FileListResponse;
}

export async function fetchFileStat(
  volumeId: string,
  path: string,
  token?: string | null,
): Promise<FileStatResponse> {
  const response = await fetch(
    `/api/volumes/${encodeURIComponent(volumeId)}/files/stat?${pathSearch(path)}`,
    {
      headers: apiHeaders(token),
    },
  );

  await assertOk(response, 'file stat request failed');

  return (await response.json()) as FileStatResponse;
}

export async function fetchReadLink(
  volumeId: string,
  path: string,
  token?: string | null,
): Promise<ReadLinkResponse> {
  const response = await fetch(
    `/api/volumes/${encodeURIComponent(volumeId)}/files/readlink?${pathSearch(path)}`,
    {
      headers: apiHeaders(token),
    },
  );

  await assertOk(response, 'readlink request failed');

  return (await response.json()) as ReadLinkResponse;
}

export async function fetchTrash(
  volumeId: string,
  token?: string | null,
): Promise<TrashResponse> {
  const response = await fetch(`/api/volumes/${encodeURIComponent(volumeId)}/trash`, {
    headers: apiHeaders(token),
  });

  await assertOk(response, 'trash request failed');

  return (await response.json()) as TrashResponse;
}

export async function restoreTrashEntry(
  volumeId: string,
  entryId: string,
  token?: string | null,
): Promise<void> {
  const response = await fetch(
    `/api/volumes/${encodeURIComponent(volumeId)}/trash/${encodeURIComponent(entryId)}/restore`,
    {
      method: 'POST',
      headers: apiHeaders(token),
    },
  );

  await assertOk(response, 'trash restore request failed');
}

export async function deleteTrashEntry(
  volumeId: string,
  entryId: string,
  token?: string | null,
): Promise<void> {
  const response = await fetch(
    `/api/volumes/${encodeURIComponent(volumeId)}/trash/${encodeURIComponent(entryId)}`,
    {
      method: 'DELETE',
      headers: apiHeaders(token),
    },
  );

  await assertOk(response, 'trash delete request failed');
}

export async function fetchAcl(
  volumeId: string,
  path: string,
  token?: string | null,
): Promise<AclResponse> {
  const response = await fetch(
    `/api/volumes/${encodeURIComponent(volumeId)}/acl?${pathSearch(path)}`,
    {
      headers: apiHeaders(token),
    },
  );

  await assertOk(response, 'ACL request failed');

  return (await response.json()) as AclResponse;
}

export async function putAcl(
  volumeId: string,
  path: string,
  request: AclUpdateRequest,
  token?: string | null,
): Promise<AclResponse> {
  const response = await fetch(
    `/api/volumes/${encodeURIComponent(volumeId)}/acl?${pathSearch(path)}`,
    {
      method: 'PUT',
      headers: apiHeaders(token, true),
      body: JSON.stringify(request),
    },
  );

  await assertOk(response, 'ACL update request failed');

  return (await response.json()) as AclResponse;
}

export async function deleteAcl(
  volumeId: string,
  path: string,
  token?: string | null,
): Promise<void> {
  const response = await fetch(
    `/api/volumes/${encodeURIComponent(volumeId)}/acl?${pathSearch(path)}`,
    {
      method: 'DELETE',
      headers: apiHeaders(token),
    },
  );

  await assertOk(response, 'ACL delete request failed');
}

export async function fetchCsiSummary(token?: string | null): Promise<CsiSummaryResponse> {
  const response = await fetch('/api/csi/summary', {
    headers: apiHeaders(token),
  });

  await assertOk(response, 'CSI summary request failed');

  return (await response.json()) as CsiSummaryResponse;
}

export async function fetchCsiStorageClasses(
  token?: string | null,
): Promise<CsiResourceListResponse> {
  return fetchCsiResourceList('/api/csi/storageclasses', token);
}

export async function fetchCsiPersistentVolumes(
  token?: string | null,
): Promise<CsiResourceListResponse> {
  return fetchCsiResourceList('/api/csi/persistentvolumes', token);
}

export async function fetchCsiPersistentVolumeClaims(
  namespace?: string,
  token?: string | null,
): Promise<CsiResourceListResponse> {
  return fetchCsiResourceList(
    `/api/csi/persistentvolumeclaims${optionalSearch([['namespace', namespace]])}`,
    token,
  );
}

export async function fetchCsiPods(
  query: CsiPodsQuery = {},
  token?: string | null,
): Promise<CsiResourceListResponse> {
  return fetchCsiResourceList(
    `/api/csi/pods${optionalSearch([
      ['namespace', query.namespace],
      ['volume', query.volume],
    ])}`,
    token,
  );
}

export async function createVolume(
  request: CreateVolumeRequest,
  token?: string | null,
): Promise<VolumeResponse> {
  const response = await fetch('/api/volumes', {
    method: 'POST',
    headers: apiHeaders(token, true),
    body: JSON.stringify(request),
  });

  await assertOk(response, 'create volume request failed');

  return (await response.json()) as VolumeResponse;
}

function apiHeaders(token?: string | null, json = false): Record<string, string> {
  const headers: Record<string, string> = { Accept: 'application/json' };
  const trimmedToken = token?.trim();
  if (trimmedToken) {
    headers.Authorization = `Bearer ${trimmedToken}`;
  }
  if (json) {
    headers['Content-Type'] = 'application/json';
  }
  return headers;
}

function pathSearch(path: string): string {
  return new URLSearchParams({ path }).toString();
}

function optionalSearch(params: Array<[string, string | undefined]>): string {
  const search = new URLSearchParams();
  for (const [key, value] of params) {
    if (value) search.set(key, value);
  }
  const encoded = search.toString();
  return encoded ? `?${encoded}` : '';
}

async function fetchCsiResourceList(
  url: string,
  token?: string | null,
): Promise<CsiResourceListResponse> {
  const response = await fetch(url, {
    headers: apiHeaders(token),
  });

  await assertOk(response, 'CSI resource request failed');

  return (await response.json()) as CsiResourceListResponse;
}

async function assertOk(response: Response, message: string) {
  if (!response.ok) {
    const error = await readApiError(response);
    const detail = error.detail ? `${response.status}: ${error.detail}` : String(response.status);
    throw new ApiError(`${message}: ${detail}`, response.status, error.code, error.detail);
  }
}

async function readApiError(
  response: Response,
): Promise<{ code: string | null; detail: string | null }> {
  if (!response.headers.get('content-type')?.includes('application/json')) {
    return { code: null, detail: null };
  }

  try {
    const payload: unknown = await response.json();
    if (!isRecord(payload) || !isRecord(payload.error)) {
      return { code: null, detail: null };
    }
    return {
      code: stringOrNull(payload.error.code),
      detail: stringOrNull(payload.error.message),
    };
  } catch {
    return { code: null, detail: null };
  }
}

function stringOrNull(value: unknown): string | null {
  return typeof value === 'string' && value.length > 0 ? value : null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
