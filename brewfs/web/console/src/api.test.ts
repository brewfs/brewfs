import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  ApiError,
  createVolume,
  deleteAcl,
  deleteVolume,
  deleteTrashEntry,
  fetchAcl,
  fetchCsiPersistentVolumeClaims,
  fetchCsiPersistentVolumes,
  fetchCsiPods,
  fetchCsiStorageClasses,
  fetchCsiSummary,
  fetchFileList,
  fetchFileStat,
  fetchHealth,
  fetchInstanceInfo,
  fetchInstances,
  fetchJobStatus,
  fetchReadLink,
  fetchTrash,
  fetchVolume,
  fetchVolumes,
  putAcl,
  restoreTrashEntry,
  runGcJob,
  updateVolume,
} from './api';

const healthResponse = {
  service: 'brewfs-console',
  version: '0.1.0',
  commit_short: 'abcdef1',
  auth_mode: 'token',
  integrations: {
    csi_dashboard: false,
  },
  static_assets_available: true,
};

describe('fetchHealth', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('sends a bearer token when one is provided', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(JSON.stringify(healthResponse), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      }),
    );

    await fetchHealth('secret-token');

    expect(fetch).toHaveBeenCalledWith('/api/health', {
      headers: {
        Accept: 'application/json',
        Authorization: 'Bearer secret-token',
      },
    });
  });

  it('throws an ApiError with status for unauthorized responses', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ error: { code: 'unauthorized' } }), {
        status: 401,
        headers: { 'content-type': 'application/json' },
      }),
    );

    await expect(fetchHealth()).rejects.toMatchObject({
      name: 'ApiError',
      status: 401,
    } satisfies Partial<ApiError>);
  });
});

describe('volume registry API', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('fetches volumes with a bearer token', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(
        JSON.stringify({
          volumes: [
            {
              id: 'vol-1',
              name: 'dev-local',
              description: null,
              labels: { env: 'dev' },
              created_at: '2026-06-11T00:00:00Z',
              updated_at: '2026-06-11T00:00:00Z',
              mount_config: {
                mount_point: '/mnt/brewfs',
                data_backend: 'local-fs',
                data_dir: '/var/lib/brewfs/data',
                meta_backend: 'sqlx',
                meta_url_redacted: 'postgres://brewfs:<redacted>@db.example/brewfs',
                chunk_size: 67108864,
                block_size: 4194304,
              },
            },
          ],
        }),
        {
          status: 200,
          headers: { 'content-type': 'application/json' },
        },
      ),
    );

    const result = await fetchVolumes('secret-token');

    expect(fetch).toHaveBeenCalledWith('/api/volumes', {
      headers: {
        Accept: 'application/json',
        Authorization: 'Bearer secret-token',
      },
    });
    expect(result.volumes[0].name).toBe('dev-local');
  });

  it('creates a volume with JSON and bearer token', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(
        JSON.stringify({
          id: 'vol-1',
          name: 'dev-local',
          description: null,
          labels: {},
          created_at: '2026-06-11T00:00:00Z',
          updated_at: '2026-06-11T00:00:00Z',
          mount_config: {
            mount_point: '/mnt/brewfs',
            data_backend: 'local-fs',
            data_dir: '/var/lib/brewfs/data',
            meta_backend: 'sqlx',
            meta_url_redacted: 'postgres://brewfs:<redacted>@db.example/brewfs',
            chunk_size: null,
            block_size: null,
          },
        }),
        {
          status: 201,
          headers: { 'content-type': 'application/json' },
        },
      ),
    );

    const result = await createVolume(
      {
        name: 'dev-local',
        mount_config: {
          mount_point: '/mnt/brewfs',
          data_backend: 'local-fs',
          data_dir: '/var/lib/brewfs/data',
          meta_backend: 'sqlx',
          meta_url: 'postgres://brewfs:secret@db.example/brewfs',
        },
      },
      'secret-token',
    );

    expect(fetch).toHaveBeenCalledWith('/api/volumes', {
      method: 'POST',
      headers: {
        Accept: 'application/json',
        Authorization: 'Bearer secret-token',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        name: 'dev-local',
        mount_config: {
          mount_point: '/mnt/brewfs',
          data_backend: 'local-fs',
          data_dir: '/var/lib/brewfs/data',
          meta_backend: 'sqlx',
          meta_url: 'postgres://brewfs:secret@db.example/brewfs',
        },
      }),
    });
    expect(JSON.stringify(result)).not.toContain('secret');
  });

  it('gets updates and deletes a volume with bearer token', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch');
    fetch
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            id: 'vol-1',
            name: 'dev-local',
            description: null,
            labels: {},
            created_at: '2026-06-11T00:00:00Z',
            updated_at: '2026-06-11T00:00:00Z',
            mount_config: {
              mount_point: null,
              data_backend: 'local-fs',
              data_dir: null,
              meta_backend: 'sqlx',
              meta_url_redacted: null,
              chunk_size: null,
              block_size: null,
            },
          }),
          { status: 200, headers: { 'content-type': 'application/json' } },
        ),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            id: 'vol-1',
            name: 'prod-local',
            description: null,
            labels: { env: 'prod' },
            created_at: '2026-06-11T00:00:00Z',
            updated_at: '2026-06-11T00:00:01Z',
            mount_config: {
              mount_point: null,
              data_backend: 'local-fs',
              data_dir: null,
              meta_backend: 'sqlx',
              meta_url_redacted: null,
              chunk_size: null,
              block_size: null,
            },
          }),
          { status: 200, headers: { 'content-type': 'application/json' } },
        ),
      )
      .mockResolvedValueOnce(new Response(null, { status: 204 }));

    await expect(fetchVolume('vol-1', 'secret-token')).resolves.toMatchObject({
      name: 'dev-local',
    });
    await expect(
      updateVolume('vol-1', { name: 'prod-local', description: null, labels: { env: 'prod' } }, 'secret-token'),
    ).resolves.toMatchObject({ name: 'prod-local' });
    await expect(deleteVolume('vol-1', 'secret-token')).resolves.toBeUndefined();

    expect(fetch).toHaveBeenNthCalledWith(1, '/api/volumes/vol-1', {
      headers: {
        Accept: 'application/json',
        Authorization: 'Bearer secret-token',
      },
    });
    expect(fetch).toHaveBeenNthCalledWith(2, '/api/volumes/vol-1', {
      method: 'PATCH',
      headers: {
        Accept: 'application/json',
        Authorization: 'Bearer secret-token',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        name: 'prod-local',
        description: null,
        labels: { env: 'prod' },
      }),
    });
    expect(fetch).toHaveBeenNthCalledWith(3, '/api/volumes/vol-1', {
      method: 'DELETE',
      headers: {
        Accept: 'application/json',
        Authorization: 'Bearer secret-token',
      },
    });
  });
});

describe('runtime instances API', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('fetches runtime instances with a bearer token', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(
        JSON.stringify({
          instances: [
            {
              pid: 42,
              mount_point: '/mnt/brewfs',
              socket_path: '/run/user/1000/brewfs/42.sock',
              started_at: '2026-06-11T00:00:00Z',
            },
          ],
        }),
        {
          status: 200,
          headers: { 'content-type': 'application/json' },
        },
      ),
    );

    const result = await fetchInstances('secret-token');

    expect(fetch).toHaveBeenCalledWith('/api/instances', {
      headers: {
        Accept: 'application/json',
        Authorization: 'Bearer secret-token',
      },
    });
    expect(result.instances[0].mount_point).toBe('/mnt/brewfs');
  });

  it('fetches runtime instance detail with a bearer token', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(
        JSON.stringify({
          pid: 42,
          mount_point: '/mnt/brewfs',
          started_at: 1786000000000,
          version: '0.1.0-test',
          meta_backend: 'sqlx',
          capabilities: {
            namespace: true,
            file_data: true,
            acl: false,
          },
        }),
        {
          status: 200,
          headers: { 'content-type': 'application/json' },
        },
      ),
    );

    const result = await fetchInstanceInfo(42, 'secret-token');

    expect(fetch).toHaveBeenCalledWith('/api/instances/42', {
      headers: {
        Accept: 'application/json',
        Authorization: 'Bearer secret-token',
      },
    });
    expect(result.meta_backend).toBe('sqlx');
    expect(result.capabilities.namespace).toBe(true);
  });

  it('starts a GC job with JSON and bearer token', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ job_id: 'job-gc-1' }), {
        status: 202,
        headers: { 'content-type': 'application/json' },
      }),
    );

    const result = await runGcJob(42, { dry_run: true }, 'secret-token');

    expect(fetch).toHaveBeenCalledWith('/api/instances/42/jobs/gc', {
      method: 'POST',
      headers: {
        Accept: 'application/json',
        Authorization: 'Bearer secret-token',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ dry_run: true }),
    });
    expect(result.job_id).toBe('job-gc-1');
  });

  it('fetches a runtime job status with a bearer token', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(
        JSON.stringify({
          job_id: 'job-gc-1',
          state: 'Succeeded',
          detail: 'gc complete',
          outcome: {
            Gc: {
              dry_run: true,
              orphan_slice_count: 3,
              orphan_object_count: 2,
              deleted_object_count: 0,
              error_count: 0,
              detail: 'gc complete',
            },
          },
        }),
        {
          status: 200,
          headers: { 'content-type': 'application/json' },
        },
      ),
    );

    const result = await fetchJobStatus(42, 'job-gc-1', 'secret-token');

    expect(fetch).toHaveBeenCalledWith('/api/instances/42/jobs/job-gc-1', {
      headers: {
        Accept: 'application/json',
        Authorization: 'Bearer secret-token',
      },
    });
    expect(result.state).toBe('Succeeded');
    expect(result.outcome?.Gc?.orphan_slice_count).toBe(3);
  });
});

describe('unsupported feature API contracts', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('throws ApiError for unsupported file browser responses', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ error: { code: 'unsupported' } }), {
        status: 422,
        headers: { 'content-type': 'application/json' },
      }),
    );

    await expect(fetchFileList('vol-1', '/', 'secret-token')).rejects.toMatchObject({
      status: 422,
    } satisfies Partial<ApiError>);
    expect(fetch).toHaveBeenCalledWith('/api/volumes/vol-1/files?path=%2F', {
      headers: {
        Accept: 'application/json',
        Authorization: 'Bearer secret-token',
      },
    });
  });

  it('fetches file stat and readlink metadata', async () => {
    const fetch = vi
      .spyOn(globalThis, 'fetch')
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            path: '/projects/readme.md',
            inode: 42,
            kind: 'file',
            size: 128,
            mode: 420,
            uid: 1000,
            gid: 1000,
            mtime: '2026-06-11T00:00:00Z',
          }),
          {
            status: 200,
            headers: { 'content-type': 'application/json' },
          },
        ),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            path: '/latest',
            target: '/projects/readme.md',
          }),
          {
            status: 200,
            headers: { 'content-type': 'application/json' },
          },
        ),
      );

    const stat = await fetchFileStat('vol-1', '/projects/readme.md', 'secret-token');
    const link = await fetchReadLink('vol-1', '/latest', 'secret-token');

    expect(fetch).toHaveBeenNthCalledWith(1, '/api/volumes/vol-1/files/stat?path=%2Fprojects%2Freadme.md', {
      headers: {
        Accept: 'application/json',
        Authorization: 'Bearer secret-token',
      },
    });
    expect(fetch).toHaveBeenNthCalledWith(2, '/api/volumes/vol-1/files/readlink?path=%2Flatest', {
      headers: {
        Accept: 'application/json',
        Authorization: 'Bearer secret-token',
      },
    });
    expect(stat.inode).toBe(42);
    expect(link.target).toBe('/projects/readme.md');
  });

  it('throws ApiError for unsupported trash responses', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ error: { code: 'unsupported' } }), {
        status: 422,
        headers: { 'content-type': 'application/json' },
      }),
    );

    await expect(fetchTrash('vol-1', 'secret-token')).rejects.toMatchObject({
      status: 422,
    } satisfies Partial<ApiError>);
    expect(fetch).toHaveBeenCalledWith('/api/volumes/vol-1/trash', {
      headers: {
        Accept: 'application/json',
        Authorization: 'Bearer secret-token',
      },
    });

    await expect(restoreTrashEntry('vol-1', 'trash-1', 'secret-token')).rejects.toMatchObject({
      status: 422,
    } satisfies Partial<ApiError>);
    expect(fetch).toHaveBeenLastCalledWith('/api/volumes/vol-1/trash/trash-1/restore', {
      method: 'POST',
      headers: {
        Accept: 'application/json',
        Authorization: 'Bearer secret-token',
      },
    });

    await expect(deleteTrashEntry('vol-1', 'trash-1', 'secret-token')).rejects.toMatchObject({
      status: 422,
    } satisfies Partial<ApiError>);
    expect(fetch).toHaveBeenLastCalledWith('/api/volumes/vol-1/trash/trash-1', {
      method: 'DELETE',
      headers: {
        Accept: 'application/json',
        Authorization: 'Bearer secret-token',
      },
    });
  });

  it('preserves backend error codes on ApiError', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(
        JSON.stringify({
          error: {
            code: 'control_plane_error',
            message: 'control-plane request timed out',
          },
        }),
        {
          status: 502,
          headers: { 'content-type': 'application/json' },
        },
      ),
    );

    await expect(fetchTrash('vol-1', 'secret-token')).rejects.toMatchObject({
      status: 502,
      code: 'control_plane_error',
      detail: 'control-plane request timed out',
    } satisfies Partial<ApiError>);
  });

  it('sends ACL reads and writes through stable endpoints', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ error: { code: 'unsupported' } }), {
        status: 422,
        headers: { 'content-type': 'application/json' },
      }),
    );

    await expect(fetchAcl('vol-1', '/', 'secret-token')).rejects.toMatchObject({
      status: 422,
    } satisfies Partial<ApiError>);

    expect(fetch).toHaveBeenCalledWith('/api/volumes/vol-1/acl?path=%2F', {
      headers: {
        Accept: 'application/json',
        Authorization: 'Bearer secret-token',
      },
    });

    await expect(putAcl('vol-1', '/', { entries: [] }, 'secret-token')).rejects.toMatchObject({
      status: 422,
    } satisfies Partial<ApiError>);

    expect(fetch).toHaveBeenLastCalledWith('/api/volumes/vol-1/acl?path=%2F', {
      method: 'PUT',
      headers: {
        Accept: 'application/json',
        Authorization: 'Bearer secret-token',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ entries: [] }),
    });

    await expect(deleteAcl('vol-1', '/', 'secret-token')).rejects.toMatchObject({
      status: 422,
    } satisfies Partial<ApiError>);

    expect(fetch).toHaveBeenLastCalledWith('/api/volumes/vol-1/acl?path=%2F', {
      method: 'DELETE',
      headers: {
        Accept: 'application/json',
        Authorization: 'Bearer secret-token',
      },
    });
  });

  it('throws ApiError for unsupported CSI summary responses', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ error: { code: 'unsupported' } }), {
        status: 422,
        headers: { 'content-type': 'application/json' },
      }),
    );

    await expect(fetchCsiSummary('secret-token')).rejects.toMatchObject({
      status: 422,
    } satisfies Partial<ApiError>);
    expect(fetch).toHaveBeenCalledWith('/api/csi/summary', {
      headers: {
        Accept: 'application/json',
        Authorization: 'Bearer secret-token',
      },
    });
  });

  it('sends CSI resource table requests through stable endpoints', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch').mockImplementation(async () =>
      new Response(JSON.stringify({ items: [] }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      }),
    );

    await fetchCsiStorageClasses('secret-token');
    expect(fetch).toHaveBeenLastCalledWith('/api/csi/storageclasses', {
      headers: {
        Accept: 'application/json',
        Authorization: 'Bearer secret-token',
      },
    });

    await fetchCsiPersistentVolumes('secret-token');
    expect(fetch).toHaveBeenLastCalledWith('/api/csi/persistentvolumes', {
      headers: {
        Accept: 'application/json',
        Authorization: 'Bearer secret-token',
      },
    });

    await fetchCsiPersistentVolumeClaims('default', 'secret-token');
    expect(fetch).toHaveBeenLastCalledWith('/api/csi/persistentvolumeclaims?namespace=default', {
      headers: {
        Accept: 'application/json',
        Authorization: 'Bearer secret-token',
      },
    });

    await fetchCsiPods({ namespace: 'default', volume: 'data' }, 'secret-token');
    expect(fetch).toHaveBeenLastCalledWith('/api/csi/pods?namespace=default&volume=data', {
      headers: {
        Accept: 'application/json',
        Authorization: 'Bearer secret-token',
      },
    });
  });
});
