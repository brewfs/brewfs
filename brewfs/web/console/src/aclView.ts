import { ApiError, fetchAcl, type AclEntry, type VolumeResponse } from './api';
import { normalizeBrowserPath } from './browserPath';

export type AclViewState = 'ready' | 'unsupported' | 'unavailable';

export interface AclEntryRow {
  scope: string;
  tag: string;
  id: string;
  perm: string;
}

export interface AclViewResult {
  state: AclViewState;
  title: string;
  message: string;
  path: string;
  volumeName?: string;
  entries: AclEntryRow[];
}

export async function loadAclView(
  volume: VolumeResponse | null,
  path: string,
  token?: string | null,
): Promise<AclViewResult> {
  const normalizedPath = normalizeBrowserPath(path);
  if (!volume) {
    return {
      state: 'unavailable',
      title: 'No registered filesystems',
      message: 'Register a filesystem before using the ACL view.',
      path: normalizedPath,
      entries: [],
    };
  }

  try {
    const response = await fetchAcl(volume.id, normalizedPath, token);
    const entries = response.entries.map(formatAclEntry);
    return {
      state: 'ready',
      title: 'ACL',
      message: entries.length === 0 ? 'No extended ACL entries.' : `${entries.length} ACL entries found.`,
      path: normalizedPath,
      volumeName: volume.name,
      entries,
    };
  } catch (err: unknown) {
    return aclErrorOrThrow(volume, normalizedPath, err);
  }
}

function formatAclEntry(entry: AclEntry): AclEntryRow {
  return {
    scope: entry.scope,
    tag: entry.tag,
    id: entry.id === undefined ? '-' : String(entry.id),
    perm: entry.perm,
  };
}

function aclErrorOrThrow(
  volume: VolumeResponse,
  path: string,
  err: unknown,
): AclViewResult {
  if (err instanceof ApiError && err.status === 409) {
    return {
      state: 'unavailable',
      title: 'ACL unavailable',
      message: 'The filesystem is registered but is not mounted or runtime access is unavailable.',
      path,
      volumeName: volume.name,
      entries: [],
    };
  }
  if (err instanceof ApiError && err.status === 501) {
    return {
      state: 'unsupported',
      title: 'ACL unsupported',
      message: 'The mounted metadata backend or console adapter does not support ACL editing yet.',
      path,
      volumeName: volume.name,
      entries: [],
    };
  }
  throw err;
}
