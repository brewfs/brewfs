import type { AclEntry, AclUpdateRequest } from './api';
import type { AclEntryRow } from './aclView';

const SUPPORTED_ACL_TAGS = new Set(['user_obj', 'user', 'group_obj', 'group', 'mask', 'other']);

export function formatAclDraft(rows: AclEntryRow[]): string {
  return JSON.stringify(
    rows.map((row) => {
      const id = Number(row.id);
      if (row.id !== '-' && Number.isInteger(id)) {
        return {
          scope: row.scope,
          tag: row.tag,
          id,
          perm: row.perm,
        };
      }
      return {
        scope: row.scope,
        tag: row.tag,
        perm: row.perm,
      };
    }),
    null,
    2,
  );
}

export function parseAclDraft(value: string): AclUpdateRequest {
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch (err: unknown) {
    throw new Error(err instanceof Error ? err.message : 'ACL draft is not valid JSON.');
  }

  if (!Array.isArray(parsed)) {
    throw new Error('ACL draft must be a JSON array.');
  }

  return {
    entries: validateAclEntrySet(parsed.map(parseEntry)),
  };
}

function validateAclEntrySet(entries: AclEntry[]): AclEntry[] {
  const hasAccessEntries = entries.some((entry) => entry.scope === 'access');
  if (!hasAccessEntries) return entries;

  const hasUserObj = hasAclEntry(entries, 'access', 'user_obj');
  const hasGroupObj = hasAclEntry(entries, 'access', 'group_obj');
  const hasOther = hasAclEntry(entries, 'access', 'other');
  if (!hasUserObj || !hasGroupObj || !hasOther) {
    throw new Error('access ACL must include user_obj, group_obj, and other entries.');
  }

  return entries;
}

function hasAclEntry(entries: AclEntry[], scope: string, tag: string): boolean {
  return entries.some((entry) => entry.scope === scope && entry.tag === tag);
}

function parseEntry(value: unknown, index: number): AclEntry {
  if (!isRecord(value)) {
    throw new Error(`ACL entry ${index + 1} must be an object.`);
  }

  const scope = requiredString(value, 'scope', index);
  const tag = requiredString(value, 'tag', index);
  const perm = requiredString(value, 'perm', index);
  const id = value.id;

  if (id === undefined || id === null) {
    validateAclEntry({ scope, tag, perm }, index);
    return { scope, tag, perm };
  }
  if (typeof id !== 'number' || !Number.isInteger(id)) {
    throw new Error(`ACL entry ${index + 1} id must be a number.`);
  }
  if (id < 0) {
    throw new Error(`ACL entry ${index + 1} id must be a non-negative number.`);
  }

  validateAclEntry({ scope, tag, id, perm }, index);
  return { scope, tag, id, perm };
}

function validateAclEntry(entry: AclEntry, index: number): void {
  const entryNumber = index + 1;
  if (entry.scope !== 'access' && entry.scope !== 'default') {
    throw new Error(`ACL entry ${entryNumber} scope must be access or default.`);
  }

  if (!SUPPORTED_ACL_TAGS.has(entry.tag)) {
    throw new Error(`ACL entry ${entryNumber} tag is not supported.`);
  }

  if (!/^[r-][w-][x-]$/.test(entry.perm)) {
    throw new Error(`ACL entry ${entryNumber} perm must use rwx characters like rw- or r-x.`);
  }

  if ((entry.tag === 'user' || entry.tag === 'group') && entry.id === undefined) {
    throw new Error(`ACL entry ${entryNumber} tag ${entry.tag} requires id.`);
  }

  if (entry.tag !== 'user' && entry.tag !== 'group' && entry.id !== undefined) {
    throw new Error(`ACL entry ${entryNumber} tag ${entry.tag} must not include id.`);
  }
}

function requiredString(record: Record<string, unknown>, key: keyof AclEntry, index: number): string {
  const value = record[key];
  if (typeof value !== 'string' || value.length === 0) {
    throw new Error(`ACL entry ${index + 1} is missing ${key}.`);
  }
  return value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
