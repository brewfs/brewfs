import { describe, expect, it } from 'vitest';
import type {
  HealthResponse,
  InstanceInfoResponse,
  InstanceResponse,
  JobStatusResponse,
  VolumeResponse,
} from './api';
import type { CsiDashboardResult } from './csiDashboard';
import {
  overviewCapabilityWarnings,
  overviewCsiSummary,
  overviewMetrics,
  overviewRecentJob,
} from './overviewSummary';

describe('overview summary helpers', () => {
  it('summarizes registered volumes, live mounts, auth, and CSI state', () => {
    expect(
      overviewMetrics({
        health: health({ csi_dashboard: true }),
        volumes: [volume({ id: 'one' }), volume({ id: 'two' })],
        instances: [instance(41)],
      }),
    ).toEqual([
      { label: 'Service', value: 'brewfs-console' },
      { label: 'Commit', value: 'abc1234' },
      { label: 'Auth', value: 'token' },
      { label: 'Registered volumes', value: '2' },
      { label: 'Live mounts', value: '1' },
      { label: 'CSI dashboard', value: 'enabled' },
    ]);
  });

  it('summarizes the most recent console job', () => {
    expect(overviewRecentJob(null)).toEqual({
      title: 'No recent job',
      detail: 'No job has been started from this console session.',
      metrics: [],
    });

    expect(
      overviewRecentJob({
        pid: 42,
        status: jobStatus({ state: 'Succeeded', detail: 'GC completed' }),
      }),
    ).toEqual({
      title: 'Succeeded GC job',
      detail: 'pid 42 - job-gc-1 - GC completed',
      metrics: [
        { label: 'Orphan slices', value: '2' },
        { label: 'Orphan objects', value: '3' },
        { label: 'Deleted objects', value: '1' },
        { label: 'Errors', value: '0' },
      ],
    });
  });

  it('surfaces backend capability warnings for offline, loading, and disabled capabilities', () => {
    expect(
      overviewCapabilityWarnings({
        volumes: [
          volume({ id: 'offline', name: 'offline-vol' }),
          volume({ id: 'loading', name: 'loading-vol', mounted: true, pid: 7 }),
          volume({ id: 'ready', name: 'ready-vol', mounted: true, pid: 9 }),
        ],
        instanceDetails: {
          9: instanceDetails({ acl: false, trash: true, quota: false }),
        },
      }),
    ).toEqual([
      'offline-vol: capability matrix unavailable while filesystem is offline.',
      'loading-vol: runtime capabilities are still loading.',
      'ready-vol: ACL, Quota disabled.',
    ]);
  });

  it('summarizes CSI health only when the integration is enabled', () => {
    expect(overviewCsiSummary({ health: health({ csi_dashboard: false }), dashboard: null })).toEqual({
      label: 'CSI dashboard',
      value: 'disabled',
      detail: 'CSI dashboard integration is disabled.',
    });

    expect(
      overviewCsiSummary({
        health: health({ csi_dashboard: true }),
        dashboard: csiDashboard({ message: '4 pods reference BrewFS volumes; 1 mounts need attention.' }),
      }),
    ).toEqual({
      label: 'CSI dashboard',
      value: 'ready',
      detail: '4 pods reference BrewFS volumes; 1 mounts need attention.',
    });
  });
});

function health(integrations: HealthResponse['integrations']): HealthResponse {
  return {
    service: 'brewfs-console',
    version: '0.1.0',
    commit_short: 'abc1234',
    auth_mode: 'token',
    integrations,
    static_assets_available: true,
  };
}

function instance(pid: number): InstanceResponse {
  return {
    pid,
    mount_point: `/mnt/brewfs-${pid}`,
    socket_path: `/run/brewfs/${pid}.sock`,
    started_at: '2026-06-12T00:00:00Z',
  };
}

function volume({
  id,
  name = id,
  mounted = false,
  pid = null,
}: {
  id: string;
  name?: string;
  mounted?: boolean;
  pid?: number | null;
}): VolumeResponse {
  return {
    id,
    name,
    description: null,
    labels: {},
    created_at: '2026-06-12T00:00:00Z',
    updated_at: '2026-06-12T00:00:00Z',
    mount_config: {
      mount_point: `/mnt/${id}`,
      data_backend: 'local-fs',
      data_dir: `/var/lib/${id}`,
      meta_backend: 'sqlx',
      meta_url_redacted: null,
      chunk_size: null,
      block_size: null,
    },
    runtime: {
      mounted,
      pid,
      mount_point: mounted ? `/mnt/${id}` : null,
      started_at: mounted ? '2026-06-12T00:00:00Z' : null,
    },
  };
}

function instanceDetails(capabilities: Record<string, boolean>): InstanceInfoResponse {
  return {
    pid: 9,
    mount_point: '/mnt/ready',
    started_at: 1_781_241_600,
    version: '0.1.0',
    meta_backend: 'sqlx',
    capabilities,
  };
}

function jobStatus(overrides: Partial<JobStatusResponse>): JobStatusResponse {
  return {
    job_id: 'job-gc-1',
    state: 'Running',
    detail: null,
    outcome: {
      Gc: {
        dry_run: true,
        orphan_slice_count: 2,
        orphan_object_count: 3,
        deleted_object_count: 1,
        error_count: 0,
        detail: null,
      },
    },
    ...overrides,
  };
}

function csiDashboard(overrides: Partial<CsiDashboardResult>): CsiDashboardResult {
  return {
    state: 'ready',
    title: 'CSI dashboard',
    message: '0 pods reference BrewFS volumes; 0 mounts need attention.',
    warnings: [],
    summaryMetrics: [],
    resources: [],
    ...overrides,
  };
}
