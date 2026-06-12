import { afterEach, describe, expect, it, vi } from 'vitest';
import { formatCsiItemCount, loadCsiDashboard, shouldLoadCsiDashboardForPage } from './csiDashboard';

describe('loadCsiDashboard', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('summarizes ready CSI counts and resource tables', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch');
    fetch
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            storageclasses: 1,
            persistentvolumes: 2,
            persistentvolumeclaims: 3,
            pods: 4,
            unhealthy_mounts: 0,
          }),
          { status: 200, headers: { 'content-type': 'application/json' } },
        ),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            items: [
              {
                metadata: { name: 'brewfs-sc' },
                provisioner: 'csi.brewfs.io',
              },
            ],
          }),
          { status: 200 },
        ),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            items: [
              {
                metadata: { name: 'pv-a' },
                spec: {
                  storageClassName: 'brewfs-sc',
                  claimRef: { namespace: 'prod', name: 'data' },
                },
                status: { phase: 'Bound' },
              },
              {
                metadata: { name: 'pv-b' },
                spec: { csi: { volumeHandle: 'cache' } },
                status: { phase: 'Available' },
              },
            ],
          }),
          { status: 200 },
        ),
      )
      .mockResolvedValueOnce(new Response(JSON.stringify({ items: [] }), { status: 200 }))
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            items: [
              {
                metadata: { namespace: 'prod', name: 'pod-a' },
                spec: {
                  volumes: [
                    { name: 'data', persistentVolumeClaim: { claimName: 'data' } },
                  ],
                },
                status: {
                  phase: 'Running',
                  conditions: [{ type: 'Ready', status: 'True' }],
                },
              },
            ],
          }),
          { status: 200 },
        ),
      );

    const result = await loadCsiDashboard('secret-token', {
      namespace: 'prod',
      volume: 'data',
    });

    expect(result.state).toBe('ready');
    expect(fetch).toHaveBeenNthCalledWith(4, '/api/csi/persistentvolumeclaims?namespace=prod', {
      headers: {
        Accept: 'application/json',
        Authorization: 'Bearer secret-token',
      },
    });
    expect(fetch).toHaveBeenNthCalledWith(5, '/api/csi/pods?namespace=prod&volume=data', {
      headers: {
        Accept: 'application/json',
        Authorization: 'Bearer secret-token',
      },
    });
    expect(result.summaryMetrics).toContainEqual({ label: 'Pods', value: '4' });
    expect(result.resources.map((resource) => [resource.key, resource.state, resource.count])).toEqual([
      ['storageclasses', 'ready', 1],
      ['persistentvolumes', 'ready', 2],
      ['persistentvolumeclaims', 'ready', 0],
      ['pods', 'ready', 1],
    ]);
    expect(result.resources[0].rows).toEqual([
      {
        namespace: '-',
        name: 'brewfs-sc',
        status: 'csi.brewfs.io',
        detail: 'provisioner csi.brewfs.io',
      },
    ]);
    expect(result.resources[1].rows[0]).toEqual({
      namespace: '-',
      name: 'pv-a',
      status: 'Bound',
      detail: 'storageClass brewfs-sc · claim prod/data',
    });
    expect(result.resources[3].rows).toEqual([
      {
        namespace: 'prod',
        name: 'pod-a',
        status: 'Running · Ready',
        detail: 'pvc data',
      },
    ]);
  });

  it('returns unavailable when the CSI dashboard is disabled', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ error: { code: 'unavailable' } }), {
        status: 409,
        headers: { 'content-type': 'application/json' },
      }),
    );

    const result = await loadCsiDashboard('secret-token');

    expect(result.state).toBe('unavailable');
    expect(result.title).toBe('CSI dashboard unavailable');
    expect(result.resources).toEqual([]);
  });

  it('returns unavailable when Kubernetes discovery fails', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(
        JSON.stringify({
          error: {
            code: 'kubernetes_error',
            message: 'failed to read kubeconfig /missing: not found',
          },
        }),
        {
          status: 502,
          headers: { 'content-type': 'application/json' },
        },
      ),
    );

    const result = await loadCsiDashboard('secret-token');

    expect(result.state).toBe('unavailable');
    expect(result.title).toBe('Kubernetes CSI unavailable');
    expect(result.message).toContain('failed to read kubeconfig');
  });

  it('keeps resource-level unsupported states visible after summary loads', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch');
    fetch
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            storageclasses: 0,
            persistentvolumes: 0,
            persistentvolumeclaims: 0,
            pods: 0,
            unhealthy_mounts: 0,
          }),
          { status: 200, headers: { 'content-type': 'application/json' } },
        ),
      )
      .mockResolvedValue(
        new Response(JSON.stringify({ error: { code: 'unsupported' } }), {
          status: 422,
          headers: { 'content-type': 'application/json' },
        }),
      );

    const result = await loadCsiDashboard('secret-token');

    expect(result.state).toBe('ready');
    expect(result.resources).toHaveLength(4);
    expect(result.resources.every((resource) => resource.state === 'unsupported')).toBe(true);
  });

  it('surfaces warnings for stale PVs and unhealthy pod mounts', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch');
    fetch
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            storageclasses: 1,
            persistentvolumes: 1,
            persistentvolumeclaims: 1,
            pods: 1,
            unhealthy_mounts: 2,
          }),
          { status: 200, headers: { 'content-type': 'application/json' } },
        ),
      )
      .mockResolvedValueOnce(new Response(JSON.stringify({ items: [] }), { status: 200 }))
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            items: [
              {
                metadata: { name: 'pv-stale' },
                status: { phase: 'Released' },
              },
            ],
          }),
          { status: 200 },
        ),
      )
      .mockResolvedValueOnce(new Response(JSON.stringify({ items: [] }), { status: 200 }))
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            items: [
              {
                metadata: { namespace: 'prod', name: 'pod-stuck' },
                spec: { volumes: [{ name: 'data', persistentVolumeClaim: { claimName: 'data' } }] },
                status: {
                  phase: 'Pending',
                  conditions: [{ type: 'Ready', status: 'False' }],
                },
              },
            ],
          }),
          { status: 200 },
        ),
      );

    const result = await loadCsiDashboard('secret-token');

    expect(result.warnings).toEqual([
      'PersistentVolume pv-stale is Released; inspect claim and reclaim state.',
      'Pod prod/pod-stuck is Pending · NotReady; inspect node mount and PVC attachment.',
    ]);
  });
});

describe('shouldLoadCsiDashboardForPage', () => {
  it('loads on the CSI page and on overview only when enabled', () => {
    expect(shouldLoadCsiDashboardForPage('csi', false)).toBe(true);
    expect(shouldLoadCsiDashboardForPage('overview', true)).toBe(true);
    expect(shouldLoadCsiDashboardForPage('overview', false)).toBe(false);
    expect(shouldLoadCsiDashboardForPage('filesystems', true)).toBe(false);
  });
});

describe('formatCsiItemCount', () => {
  it('labels unavailable counts without implying a numeric item total', () => {
    expect(formatCsiItemCount(null)).toBe('items unavailable');
    expect(formatCsiItemCount(1)).toBe('1 item');
    expect(formatCsiItemCount(2)).toBe('2 items');
  });
});
