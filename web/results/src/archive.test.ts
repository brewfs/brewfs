import { describe, expect, it } from 'vitest';
import { zipSync } from 'fflate';
import { makeZip, readZip } from './archive';

describe('result archive metadata', () => {
  it('preserves ZIP modification time across import and export', async () => {
    const originalMtime = new Date('2026-08-31T12:34:56.000Z');
    const archive = zipSync({
      'brewfs-run/report.md': [new TextEncoder().encode('# pass'), { mtime: originalMtime }],
    });
    const imported = await readZip(new File([archive], 'brewfs-run.zip', { type: 'application/zip' }));
    expect(imported).toHaveLength(1);
    // ZIP stores timestamps at two-second DOS precision.
    expect(imported[0].mtime).toBe(new Date('2026-08-31T12:34:56.000Z').getTime());

    const exported = await makeZip(imported);
    const roundTripped = await readZip(new File([exported], 'round-trip.zip', { type: 'application/zip' }));
    expect(roundTripped[0].mtime).toBe(imported[0].mtime);
  });
});
