import { afterEach, describe, expect, it, vi } from 'vitest';
import { loadInstanceDetails } from './instanceDetails';
import type { InstanceResponse } from './api';

const instances: InstanceResponse[] = [
  {
    pid: 1,
    mount_point: '/mnt/one',
    socket_path: '/run/brewfs/1.sock',
    started_at: '2026-06-11T00:00:00Z',
  },
  {
    pid: 2,
    mount_point: '/mnt/two',
    socket_path: '/run/brewfs/2.sock',
    started_at: '2026-06-11T00:00:00Z',
  },
];

describe('loadInstanceDetails', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('keeps successful details when another instance detail request fails', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(async (input) => {
      if (input === '/api/instances/1') {
        return new Response(
          JSON.stringify({
            pid: 1,
            mount_point: '/mnt/one',
            started_at: 1786000000000,
            version: '0.1.0-test',
            meta_backend: 'sqlx',
            capabilities: { namespace: true },
          }),
          { status: 200, headers: { 'content-type': 'application/json' } },
        );
      }

      return new Response(JSON.stringify({ error: { code: 'control_plane_error' } }), {
        status: 502,
        headers: { 'content-type': 'application/json' },
      });
    });

    const result = await loadInstanceDetails(instances, 'secret-token');

    expect(result.authRequired).toBe(false);
    expect(result.error).toBe('1 instance detail request failed');
    expect(result.details[1]?.meta_backend).toBe('sqlx');
    expect(result.details[2]).toBeUndefined();
  });
});
