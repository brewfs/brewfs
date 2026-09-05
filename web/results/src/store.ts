import type { ArtifactFile } from './archive';
import type { PerfMetric } from './metrics';

export type RunStatus = 'pass' | 'attention' | 'unknown';

export type RunEnvironment = {
  jobName?: string;
  namespace?: string;
  image?: string;
  nodeName?: string;
  instanceType?: string;
  cpuCapacity?: string;
  memoryCapacity?: string;
  ephemeralStorageCapacity?: string;
  osImage?: string;
  kernelVersion?: string;
  kubeletVersion?: string;
  startedAt?: string;
  completedAt?: string;
  perfTools?: string[];
};

export type ResultRun = {
  id: string;
  name: string;
  sourceName: string;
  backend: 'redis' | 'tikv' | 'unknown';
  dataBackend: 's3' | 'local-fs' | 'unknown';
  status: RunStatus;
  uploadedAt: number;
  fileCount: number;
  totalBytes: number;
  earliestMtime: number;
  latestMtime: number;
  storage: 'browser' | 'server';
  archiveUrl?: string;
  metrics?: PerfMetric[];
  environment?: RunEnvironment;
  files: ArtifactFile[];
};

const DB_NAME = 'brewfs-result-vault';
const STORE_NAME = 'runs';
const DB_VERSION = 1;

function openDatabase(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);
    request.onerror = () => reject(request.error ?? new Error('Unable to open local result database.'));
    request.onupgradeneeded = () => {
      const database = request.result;
      if (!database.objectStoreNames.contains(STORE_NAME)) {
        database.createObjectStore(STORE_NAME, { keyPath: 'id' });
      }
    };
    request.onsuccess = () => resolve(request.result);
  });
}

function transaction<T>(mode: IDBTransactionMode, action: (store: IDBObjectStore) => IDBRequest<T>): Promise<T> {
  return openDatabase().then(
    (database) =>
      new Promise((resolve, reject) => {
        const request = action(database.transaction(STORE_NAME, mode).objectStore(STORE_NAME));
        request.onerror = () => reject(request.error ?? new Error('Local result database request failed.'));
        request.onsuccess = () => resolve(request.result);
        request.addEventListener('loadend', () => database.close(), { once: true });
      }),
  );
}

export async function listRuns(): Promise<ResultRun[]> {
  const runs = await transaction<ResultRun[]>('readonly', (store) => store.getAll());
  return runs.sort((left, right) => right.uploadedAt - left.uploadedAt);
}

export async function saveRun(run: ResultRun): Promise<void> {
  await transaction<IDBValidKey>('readwrite', (store) => store.put(run));
}

export async function deleteRun(id: string): Promise<void> {
  await transaction<undefined>('readwrite', (store) => store.delete(id));
}
