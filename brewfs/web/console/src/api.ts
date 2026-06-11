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

export class ApiError extends Error {
  readonly status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
  }
}

export async function fetchHealth(token?: string | null): Promise<HealthResponse> {
  const response = await fetch('/api/health', {
    headers: apiHeaders(token),
  });

  assertOk(response, 'health request failed');

  return (await response.json()) as HealthResponse;
}

export async function fetchVolumes(token?: string | null): Promise<ListVolumesResponse> {
  const response = await fetch('/api/volumes', {
    headers: apiHeaders(token),
  });

  assertOk(response, 'volumes request failed');

  return (await response.json()) as ListVolumesResponse;
}

export async function fetchInstances(token?: string | null): Promise<ListInstancesResponse> {
  const response = await fetch('/api/instances', {
    headers: apiHeaders(token),
  });

  assertOk(response, 'instances request failed');

  return (await response.json()) as ListInstancesResponse;
}

export async function fetchInstanceInfo(
  pid: number,
  token?: string | null,
): Promise<InstanceInfoResponse> {
  const response = await fetch(`/api/instances/${pid}`, {
    headers: apiHeaders(token),
  });

  assertOk(response, 'instance info request failed');

  return (await response.json()) as InstanceInfoResponse;
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

  assertOk(response, 'create volume request failed');

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

function assertOk(response: Response, message: string) {
  if (!response.ok) {
    throw new ApiError(`${message}: ${response.status}`, response.status);
  }
}
