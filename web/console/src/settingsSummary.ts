import type { HealthResponse, InstanceResponse, VolumeResponse } from './api';

export interface SettingsMetric {
  label: string;
  value: string;
}

export interface SettingsSummary {
  metrics: SettingsMetric[];
}

export function buildSettingsSummary(
  health: HealthResponse | null,
  volumes: VolumeResponse[],
  instances: InstanceResponse[],
): SettingsSummary {
  return {
    metrics: [
      { label: 'Version', value: health?.version ?? '-' },
      { label: 'Commit', value: health?.commit_short ?? '-' },
      { label: 'Auth', value: health?.auth_mode ?? '-' },
      {
        label: 'Static assets',
        value: health ? (health.static_assets_available ? 'available' : 'missing') : '-',
      },
      {
        label: 'CSI dashboard',
        value: health ? (health.integrations.csi_dashboard ? 'enabled' : 'disabled') : '-',
      },
      { label: 'Registered filesystems', value: String(volumes.length) },
      { label: 'Live instances', value: String(instances.length) },
    ],
  };
}
