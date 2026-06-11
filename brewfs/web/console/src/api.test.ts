import { afterEach, describe, expect, it, vi } from 'vitest';
import { ApiError, createVolume, fetchHealth, fetchVolumes } from './api';

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
});
