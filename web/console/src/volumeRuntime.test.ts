import { describe, expect, it } from 'vitest';
import { formatVolumeRuntime } from './volumeRuntime';

describe('formatVolumeRuntime', () => {
  it('formats mounted runtime records with pid and mount point', () => {
    expect(
      formatVolumeRuntime({
        mounted: true,
        pid: 42,
        mount_point: '/mnt/brewfs',
        started_at: '2026-06-11T12:00:00Z',
      }),
    ).toBe('mounted · pid 42 · /mnt/brewfs');
  });

  it('formats offline and missing runtime records conservatively', () => {
    expect(
      formatVolumeRuntime({
        mounted: false,
        pid: null,
        mount_point: null,
        started_at: null,
      }),
    ).toBe('offline');
    expect(formatVolumeRuntime(undefined)).toBe('unknown');
  });
});
