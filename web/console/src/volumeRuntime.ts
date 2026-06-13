import type { VolumeRuntimeResponse } from './api';

export function formatVolumeRuntime(runtime: VolumeRuntimeResponse | null | undefined): string {
  if (!runtime) return 'unknown';
  if (!runtime.mounted) return 'offline';

  const parts = ['mounted'];
  if (runtime.pid !== null) parts.push(`pid ${runtime.pid}`);
  if (runtime.mount_point) parts.push(runtime.mount_point);
  return parts.join(' · ');
}
