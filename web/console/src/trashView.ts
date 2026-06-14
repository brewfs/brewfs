import { ApiError, fetchTrash, type VolumeResponse } from './api';

export type TrashViewState = 'ready' | 'unsupported' | 'unavailable';

export interface TrashEntryRow {
  id: string;
  path: string;
  size: string;
  deletedAt: string;
}

export interface TrashActions {
  restoreSupported: boolean;
  deleteSupported: boolean;
  deleteDisabledReason: string | null;
}

export interface TrashViewResult {
  state: TrashViewState;
  title: string;
  message: string;
  volumeName?: string;
  entries: TrashEntryRow[];
  actions: TrashActions;
}

export const DEFAULT_TRASH_ACTIONS: TrashActions = {
  restoreSupported: true,
  deleteSupported: true,
  deleteDisabledReason: null,
};

export async function loadTrashView(
  volume: VolumeResponse | null,
  token?: string | null,
): Promise<TrashViewResult> {
  if (!volume) {
    return {
      state: 'unavailable',
      title: 'No registered filesystems',
      message: 'Register a filesystem before using the trash view.',
      entries: [],
      actions: disabledTrashActions(),
    };
  }

  try {
    const response = await fetchTrash(volume.id, token);
    const entries = normalizeTrashEntries(response.entries);
    return {
      state: 'ready',
      title: 'Trash',
      message: entries.length === 0 ? 'Trash is empty.' : `${entries.length} trash entries found.`,
      volumeName: volume.name,
      entries,
      actions: DEFAULT_TRASH_ACTIONS,
    };
  } catch (err: unknown) {
    return trashErrorOrThrow(volume, err);
  }
}

function normalizeTrashEntries(entries: unknown[]): TrashEntryRow[] {
  return entries.map((entry, index) => {
    if (!isRecord(entry)) {
      return {
        id: String(index + 1),
        path: String(entry),
        size: '-',
        deletedAt: '-',
      };
    }

    return {
      id: stringField(entry, ['id', 'entry_id', 'trash_id']) ?? String(index + 1),
      path: stringField(entry, ['original_path', 'path', 'name']) ?? '-',
      size: stringField(entry, ['size', 'bytes']) ?? '-',
      deletedAt: stringField(entry, ['deleted_at', 'deletedAt', 'mtime']) ?? '-',
    };
  });
}

function trashErrorOrThrow(volume: VolumeResponse, err: unknown): TrashViewResult {
  if (err instanceof ApiError && (err.status === 409 || err.code === 'control_plane_error')) {
    return {
      state: 'unavailable',
      title: 'Trash unavailable',
      message: 'The filesystem is registered but is not mounted or runtime access is unavailable.',
      volumeName: volume.name,
      entries: [],
      actions: disabledTrashActions(),
    };
  }
  if (err instanceof ApiError && (err.code === 'unsupported' || err.status === 422)) {
    return {
      state: 'unsupported',
      title: 'Trash unsupported',
      message: 'The server exposes trash endpoints but BrewFS trash support is not implemented yet.',
      volumeName: volume.name,
      entries: [],
      actions: disabledTrashActions(),
    };
  }
  throw err;
}

function disabledTrashActions(): TrashActions {
  return {
    restoreSupported: false,
    deleteSupported: false,
    deleteDisabledReason: 'Trash actions are unavailable for this filesystem.',
  };
}

function stringField(record: Record<string, unknown>, keys: string[]): string | null {
  for (const key of keys) {
    const value = record[key];
    if (value !== null && value !== undefined && value !== '') return String(value);
  }
  return null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
