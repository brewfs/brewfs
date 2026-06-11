import { afterEach, describe, expect, it, vi } from 'vitest';
import { loadAclView } from './aclView';
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

describe('loadAclView', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('loads ACL entries for a normalized path', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(
        JSON.stringify({
          entries: [{ scope: 'access', tag: 'user_obj', perm: 'rwx' }],
        }),
        { status: 200, headers: { 'content-type': 'application/json' } },
      ),
    );

    const result = await loadAclView(volume, 'docs/report.txt', 'secret-token');

    expect(fetch).toHaveBeenCalledWith('/api/volumes/vol-1/acl?path=%2Fdocs%2Freport.txt', {
      headers: {
        Accept: 'application/json',
        Authorization: 'Bearer secret-token',
      },
    });
    expect(result.state).toBe('ready');
    expect(result.path).toBe('/docs/report.txt');
    expect(result.entries[0]).toEqual({ scope: 'access', tag: 'user_obj', id: '-', perm: 'rwx' });
  });

  it('returns unavailable without calling the API when no volume is selected', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch');

    const result = await loadAclView(null, '/', 'secret-token');

    expect(result.state).toBe('unavailable');
    expect(fetch).not.toHaveBeenCalled();
  });

  it('maps unsupported ACL APIs to a visible page state', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ error: { code: 'unsupported' } }), {
        status: 501,
        headers: { 'content-type': 'application/json' },
      }),
    );

    const result = await loadAclView(volume, '/', 'secret-token');

    expect(result.state).toBe('unsupported');
    expect(result.volumeName).toBe('dev-local');
  });
});
