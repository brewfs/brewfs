import { describe, expect, it } from 'vitest';
import { labelsFromText, labelsToText } from './volumeEdit';

describe('volume label editing helpers', () => {
  it('serializes labels into stable key-value lines', () => {
    expect(labelsToText({ tier: 'gold', env: 'prod' })).toBe('env=prod\ntier=gold');
  });

  it('parses key-value lines and ignores blank lines', () => {
    expect(labelsFromText('env=prod\n\n tier = gold ')).toEqual({
      env: 'prod',
      tier: 'gold',
    });
  });

  it('rejects label lines without equals separators', () => {
    expect(() => labelsFromText('env')).toThrow('invalid label');
  });
});
