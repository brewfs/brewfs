import { describe, expect, it } from 'vitest';
import { formatAclDraft, parseAclDraft } from './aclDraft';
import type { AclEntryRow } from './aclView';

const rows: AclEntryRow[] = [
  { scope: 'access', tag: 'user_obj', id: '-', perm: 'rwx' },
  { scope: 'access', tag: 'user', id: '1001', perm: 'rw-' },
];

describe('aclDraft', () => {
  it('formats ACL rows as editable JSON entries', () => {
    expect(formatAclDraft(rows)).toBe(
      JSON.stringify(
        [
          { scope: 'access', tag: 'user_obj', perm: 'rwx' },
          { scope: 'access', tag: 'user', id: 1001, perm: 'rw-' },
        ],
        null,
        2,
      ),
    );
  });

  it('parses ACL entry arrays into update requests', () => {
    expect(
      parseAclDraft(
        '[{"scope":"access","tag":"user_obj","perm":"rwx"},{"scope":"access","tag":"group_obj","perm":"r-x"},{"scope":"access","tag":"other","perm":"---"},{"scope":"access","tag":"group","id":1002,"perm":"r-x"}]',
      ),
    ).toEqual({
      entries: [
        { scope: 'access', tag: 'user_obj', perm: 'rwx' },
        { scope: 'access', tag: 'group_obj', perm: 'r-x' },
        { scope: 'access', tag: 'other', perm: '---' },
        { scope: 'access', tag: 'group', id: 1002, perm: 'r-x' },
      ],
    });
  });

  it('rejects malformed ACL drafts', () => {
    expect(() => parseAclDraft('{"entries":[]}')).toThrow('ACL draft must be a JSON array.');
    expect(() => parseAclDraft('[{"scope":"access","tag":"user_obj"}]')).toThrow(
      'ACL entry 1 is missing perm.',
    );
    expect(() => parseAclDraft('[{"scope":"access","tag":"user","id":"1001","perm":"rwx"}]')).toThrow(
      'ACL entry 1 id must be a number.',
    );
    expect(() => parseAclDraft('[{"scope":"access","tag":"group","id":-1,"perm":"rwx"}]')).toThrow(
      'ACL entry 1 id must be a non-negative number.',
    );
  });

  it('rejects ACL entries that fail POSIX-oriented preflight validation', () => {
    expect(() => parseAclDraft('[{"scope":"mask","tag":"user_obj","perm":"rwx"}]')).toThrow(
      'ACL entry 1 scope must be access or default.',
    );
    expect(() => parseAclDraft('[{"scope":"access","tag":"owner","perm":"rwx"}]')).toThrow(
      'ACL entry 1 tag is not supported.',
    );
    expect(() => parseAclDraft('[{"scope":"access","tag":"user","perm":"rw-"}]')).toThrow(
      'ACL entry 1 tag user requires id.',
    );
    expect(() => parseAclDraft('[{"scope":"access","tag":"other","id":1000,"perm":"r--"}]')).toThrow(
      'ACL entry 1 tag other must not include id.',
    );
    expect(() => parseAclDraft('[{"scope":"access","tag":"group_obj","perm":"read"}]')).toThrow(
      'ACL entry 1 perm must use rwx characters like rw- or r-x.',
    );
  });

  it('rejects access ACL drafts without base entries', () => {
    expect(() =>
      parseAclDraft(
        '[{"scope":"access","tag":"user_obj","perm":"rwx"},{"scope":"access","tag":"group_obj","perm":"r-x"}]',
      ),
    ).toThrow('access ACL must include user_obj, group_obj, and other entries.');
  });
});
