import { describe, expect, it } from 'vitest';
import { buildSettingsSummary } from './settingsSummary';
import type { HealthResponse, InstanceResponse, VolumeResponse } from './api';

const health: HealthResponse = {
  service: 'brewfs-console',
  version: '0.1.0',
  commit_short: 'abcdef1',
  auth_mode: 'token',
  integrations: {
    csi_dashboard: true,
  },
  static_assets_available: true,
};

const volumes = [{ id: 'vol-1' }, { id: 'vol-2' }] as VolumeResponse[];
const instances = [{ pid: 101 }] as InstanceResponse[];

describe('buildSettingsSummary', () => {
  it('summarizes console runtime and registry state', () => {
    const summary = buildSettingsSummary(health, volumes, instances);

    expect(summary.metrics).toEqual([
      { label: 'Version', value: '0.1.0' },
      { label: 'Commit', value: 'abcdef1' },
      { label: 'Auth', value: 'token' },
      { label: 'Static assets', value: 'available' },
      { label: 'CSI dashboard', value: 'enabled' },
      { label: 'Registered filesystems', value: '2' },
      { label: 'Live instances', value: '1' },
    ]);
  });

  it('returns unavailable labels before health loads', () => {
    const summary = buildSettingsSummary(null, [], []);

    expect(summary.metrics[0]).toEqual({ label: 'Version', value: '-' });
    expect(summary.metrics[3]).toEqual({ label: 'Static assets', value: '-' });
  });
});
