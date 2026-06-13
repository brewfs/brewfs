import { describe, expect, it } from 'vitest';
import type { InstanceInfoResponse, VolumeResponse } from './api';
import {
  aclCapabilityWarning,
  enabledCapabilityLabels,
  summarizeVolumeCapabilities,
} from './volumeCapabilities';

const mountedVolume: VolumeResponse = {
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
    mounted: true,
    pid: 42,
    mount_point: '/mnt/brewfs',
    started_at: '2026-06-11T12:00:00Z',
  },
};

const instanceInfo: InstanceInfoResponse = {
  pid: 42,
  mount_point: '/mnt/brewfs',
  started_at: 1781179200,
  version: '0.1.0',
  meta_backend: 'sqlx',
  capabilities: {
    namespace: true,
    file_data: true,
    batch_stat: false,
    hardlinks: true,
    symlinks: true,
    rename_exchange: false,
    xattr: true,
    acl: false,
    custom_future_flag: true,
  },
};

describe('summarizeVolumeCapabilities', () => {
  it('summarizes mounted volumes with stable labels and counts', () => {
    const summary = summarizeVolumeCapabilities(mountedVolume, { 42: instanceInfo });

    expect(summary.state).toBe('ready');
    expect(summary.label).toBe('6/9 enabled');
    expect(summary.enabled).toEqual([
      'Namespace',
      'File data',
      'Hardlinks',
      'Symlinks',
      'Xattr',
      'custom_future_flag',
    ]);
    expect(summary.disabled).toEqual(['Batch stat', 'Rename exchange', 'ACL']);
  });

  it('distinguishes offline volumes from mounted volumes with missing details', () => {
    expect(
      summarizeVolumeCapabilities(
        {
          ...mountedVolume,
          runtime: {
            mounted: false,
            pid: null,
            mount_point: null,
            started_at: null,
          },
        },
        {},
      ),
    ).toMatchObject({ state: 'offline', label: 'offline' });

    expect(summarizeVolumeCapabilities(mountedVolume, {})).toMatchObject({
      state: 'unknown',
      label: 'unknown',
    });
  });

  it('formats enabled capability labels for instance summaries', () => {
    expect(
      enabledCapabilityLabels({
        acl: false,
        namespace: true,
        custom_future_flag: true,
      }),
    ).toEqual(['Namespace', 'custom_future_flag']);
  });

  it('builds ACL capability warnings for the ACL page', () => {
    expect(aclCapabilityWarning(null, {})).toBe('Register a filesystem before editing ACLs.');
    expect(
      aclCapabilityWarning(
        {
          ...mountedVolume,
          runtime: {
            mounted: false,
            pid: null,
            mount_point: null,
            started_at: null,
          },
        },
        {},
      ),
    ).toBe('Mount this filesystem to inspect ACL capability.');
    expect(aclCapabilityWarning(mountedVolume, {})).toBe(
      'ACL capability is unknown until instance details finish loading.',
    );
    expect(aclCapabilityWarning(mountedVolume, { 42: instanceInfo })).toBe(
      'Mounted metadata backend reports ACL unsupported; saving changes will be rejected.',
    );
    expect(
      aclCapabilityWarning(mountedVolume, {
        42: {
          ...instanceInfo,
          capabilities: { acl: true },
        },
      }),
    ).toBeNull();
  });
});
