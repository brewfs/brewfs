import { afterEach, describe, expect, it, vi } from 'vitest';
import { loadTrashView } from './trashView';
import type { VolumeResponse } from './api';

const volume: VolumeResponse = {
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
    meta_url_redacted: null,
    chunk_size: null,
    block_size: null,
  },
  runtime: {
    mounted: false,
    pid: null,
    mount_point: null,
    started_at: null,
  },
};

describe('loadTrashView', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('normalizes trash entries for table display', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(
        JSON.stringify({
          entries: [
            {
              id: 'trash-1',
              original_path: '/docs/report.txt',
              size: 42,
              deleted_at: '2026-06-11T12:00:00Z',
            },
          ],
        }),
        { status: 200, headers: { 'content-type': 'application/json' } },
      ),
    );

    const result = await loadTrashView(volume, 'secret-token');

    expect(result.state).toBe('ready');
    expect(result.actions).toEqual({
      restoreSupported: true,
      deleteSupported: true,
      deleteDisabledReason: null,
    });
    expect(result.entries).toEqual([
      {
        id: 'trash-1',
        path: '/docs/report.txt',
        size: '42',
        deletedAt: '2026-06-11T12:00:00Z',
      },
    ]);
  });

  it('returns unavailable without calling the API when no volume is selected', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch');

    const result = await loadTrashView(null, 'secret-token');

    expect(result.state).toBe('unavailable');
    expect(fetch).not.toHaveBeenCalled();
  });

  it('maps unsupported trash APIs to a visible page state', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ error: { code: 'unsupported' } }), {
        status: 422,
        headers: { 'content-type': 'application/json' },
      }),
    );

    const result = await loadTrashView(volume, 'secret-token');

    expect(result.state).toBe('unsupported');
    expect(result.volumeName).toBe('dev-local');
  });

  it('maps control-plane trash errors to a visible unavailable state', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ error: { code: 'control_plane_error' } }), {
        status: 502,
        headers: { 'content-type': 'application/json' },
      }),
    );

    const result = await loadTrashView(volume, 'secret-token');

    expect(result.state).toBe('unavailable');
    expect(result.title).toBe('Trash unavailable');
    expect(result.volumeName).toBe('dev-local');
  });

  it('does not hide non-control-plane bad gateway trash errors', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ error: { code: 'bad_gateway' } }), {
        status: 502,
        headers: { 'content-type': 'application/json' },
      }),
    );

    await expect(loadTrashView(volume, 'secret-token')).rejects.toMatchObject({
      status: 502,
      code: 'bad_gateway',
    });
  });
});
