import { describe, expect, it } from 'vitest';
import { buildMountCommand } from './mountCommand';
import type { VolumeResponse } from './api';

function volume(overrides: Partial<VolumeResponse['mount_config']> = {}): VolumeResponse {
  return {
    id: 'vol-1',
    name: 'dev-local',
    description: null,
    labels: {},
    created_at: '2026-06-11T00:00:00Z',
    updated_at: '2026-06-11T00:00:00Z',
    mount_config: {
      mount_point: '/mnt/brewfs',
      data_backend: 'local-fs',
      data_dir: '/var/lib/brewfs data',
      meta_backend: 'sqlx',
      meta_url_redacted: null,
      chunk_size: 67108864,
      block_size: 4096,
      ...overrides,
    },
    runtime: {
      mounted: false,
      pid: null,
      mount_point: null,
      started_at: null,
    },
  };
}

describe('buildMountCommand', () => {
  it('builds a copyable mount command from safe registry fields', () => {
    const result = buildMountCommand(volume());

    expect(result.command).toBe(
      "brewfs mount --data-backend local-fs --data-dir '/var/lib/brewfs data' --meta-backend sqlx --chunk-size 67108864 --block-size 4096 /mnt/brewfs",
    );
    expect(result.warnings).toEqual([]);
  });

  it('uses a placeholder when the metadata URL is redacted', () => {
    const result = buildMountCommand(
      volume({
        meta_backend: 'redis',
        meta_url_redacted: 'redis://:***@localhost:6379/0',
      }),
    );

    expect(result.command).toContain("--meta-url '<redacted-meta-url>'");
    expect(result.warnings).toEqual(['Meta URL is redacted; provide the real value before running.']);
  });

  it('uses backend-specific placeholders for redacted etcd and tikv endpoints', () => {
    const etcdResult = buildMountCommand(
      volume({
        meta_backend: 'etcd',
        meta_url_redacted: 'http://127.0.0.1:2379',
      }),
    );
    const tikvResult = buildMountCommand(
      volume({
        meta_backend: 'tikv',
        meta_url_redacted: '127.0.0.1:2379',
      }),
    );

    expect(etcdResult.command).toContain("--meta-etcd-urls '<redacted-etcd-urls>'");
    expect(tikvResult.command).toContain("--meta-tikv-pd-endpoints '<redacted-tikv-pd-endpoints>'");
  });

  it('warns when the stored fields are not enough to mount an S3 data backend', () => {
    const result = buildMountCommand(
      volume({
        data_backend: 's3',
        data_dir: null,
      }),
    );

    expect(result.command).toContain('--data-backend s3');
    expect(result.warnings).toContain(
      'S3 bucket and endpoint options are not stored yet; add them before running.',
    );
  });

  it('quotes shell arguments containing single quotes', () => {
    const result = buildMountCommand(
      volume({
        mount_point: "/mnt/brewfs team's data",
        data_dir: "/srv/brewfs team's data",
      }),
    );

    expect(result.command).toContain("--data-dir '/srv/brewfs team'\\''s data'");
    expect(result.command).toContain("'/mnt/brewfs team'\\''s data'");
  });
});
