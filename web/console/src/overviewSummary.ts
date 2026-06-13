import type {
  HealthResponse,
  InstanceInfoResponse,
  InstanceResponse,
  JobStatusResponse,
  VolumeResponse,
} from './api';
import type { CsiDashboardResult } from './csiDashboard';
import { summarizeVolumeCapabilities } from './volumeCapabilities';

export interface OverviewMetric {
  label: string;
  value: string;
}

export interface OverviewCurrentJob {
  pid: number;
  status: JobStatusResponse;
}

export interface OverviewJobSummary {
  title: string;
  detail: string;
  metrics: OverviewMetric[];
}

export interface OverviewCsiSummary {
  label: string;
  value: string;
  detail: string;
}

export function overviewMetrics({
  health,
  volumes,
  instances,
}: {
  health: HealthResponse | null;
  volumes: VolumeResponse[];
  instances: InstanceResponse[];
}): OverviewMetric[] {
  return [
    { label: 'Service', value: health?.service ?? 'waiting' },
    { label: 'Commit', value: health?.commit_short ?? 'unknown' },
    { label: 'Auth', value: health?.auth_mode ?? 'unknown' },
    { label: 'Registered volumes', value: String(volumes.length) },
    { label: 'Live mounts', value: String(instances.length) },
    {
      label: 'CSI dashboard',
      value: health ? (health.integrations.csi_dashboard ? 'enabled' : 'disabled') : 'unknown',
    },
  ];
}

export function overviewRecentJob(currentJob: OverviewCurrentJob | null): OverviewJobSummary {
  if (!currentJob) {
    return {
      title: 'No recent job',
      detail: 'No job has been started from this console session.',
      metrics: [],
    };
  }

  const detail = [`pid ${currentJob.pid}`, currentJob.status.job_id, currentJob.status.detail]
    .filter(Boolean)
    .join(' - ');
  const gc = currentJob.status.outcome?.Gc;

  return {
    title: `${currentJob.status.state} GC job`,
    detail,
    metrics: gc
      ? [
          { label: 'Orphan slices', value: String(gc.orphan_slice_count) },
          { label: 'Orphan objects', value: String(gc.orphan_object_count) },
          { label: 'Deleted objects', value: String(gc.deleted_object_count) },
          { label: 'Errors', value: String(gc.error_count) },
        ]
      : [],
  };
}

export function overviewCapabilityWarnings({
  volumes,
  instanceDetails,
}: {
  volumes: VolumeResponse[];
  instanceDetails: Record<number, InstanceInfoResponse>;
}): string[] {
  return volumes.flatMap((volume) => {
    const summary = summarizeVolumeCapabilities(volume, instanceDetails);
    if (summary.state === 'offline') {
      return [`${volume.name}: capability matrix unavailable while filesystem is offline.`];
    }
    if (summary.state === 'unknown') {
      return [`${volume.name}: runtime capabilities are still loading.`];
    }
    if (summary.disabled.length > 0) {
      return [`${volume.name}: ${summary.disabled.join(', ')} disabled.`];
    }
    return [];
  });
}

export function overviewCsiSummary({
  health,
  dashboard,
  loading = false,
  error = null,
}: {
  health: HealthResponse | null;
  dashboard: CsiDashboardResult | null;
  loading?: boolean;
  error?: string | null;
}): OverviewCsiSummary {
  if (!health) {
    return {
      label: 'CSI dashboard',
      value: 'unknown',
      detail: 'Console health is still loading.',
    };
  }

  if (!health.integrations.csi_dashboard) {
    return {
      label: 'CSI dashboard',
      value: 'disabled',
      detail: 'CSI dashboard integration is disabled.',
    };
  }

  if (loading) {
    return {
      label: 'CSI dashboard',
      value: 'loading',
      detail: 'CSI dashboard summary is loading.',
    };
  }

  if (error) {
    return {
      label: 'CSI dashboard',
      value: 'error',
      detail: error,
    };
  }

  if (dashboard) {
    return {
      label: 'CSI dashboard',
      value: dashboard.state,
      detail: dashboard.message,
    };
  }

  return {
    label: 'CSI dashboard',
    value: 'enabled',
    detail: 'CSI dashboard integration is enabled.',
  };
}
