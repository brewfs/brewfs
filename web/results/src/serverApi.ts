import type { ArtifactFile } from './archive';
import type { ResultRun } from './store';

type ServerFile = Omit<ArtifactFile, 'blob' | 'remoteUrl'>;
type ServerRun = Omit<ResultRun, 'files' | 'storage' | 'archiveUrl'> & { files: ServerFile[] };

const API_PREFIX = '/api';

function withRemoteFiles(run: ServerRun): ResultRun {
  return {
    ...run,
    storage: 'server',
    archiveUrl: `${API_PREFIX}/runs/${encodeURIComponent(run.id)}/archive`,
    files: run.files.map((file) => ({
      ...file,
      remoteUrl: `${API_PREFIX}/runs/${encodeURIComponent(run.id)}/files/${file.path.split('/').map(encodeURIComponent).join('/')}`,
    })),
  };
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_PREFIX}${path}`, init);
  if (!response.ok) throw new Error((await response.text()) || `Server request failed (${response.status}).`);
  return response.json() as Promise<T>;
}

export async function listServerRuns(): Promise<ResultRun[]> {
  const runs = await request<ServerRun[]>('/runs');
  return runs.map(withRemoteFiles);
}

export async function uploadServerRun(archive: File): Promise<ResultRun> {
  const form = new FormData();
  form.append('archive', archive, archive.name);
  return withRemoteFiles(await request<ServerRun>('/runs', { method: 'POST', body: form }));
}

export async function deleteServerRun(id: string): Promise<void> {
  const response = await fetch(`${API_PREFIX}/runs/${encodeURIComponent(id)}`, { method: 'DELETE' });
  if (!response.ok) throw new Error((await response.text()) || `Server request failed (${response.status}).`);
}
