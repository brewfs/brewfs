import {
  ApiError,
  fetchInstanceInfo,
  type InstanceInfoResponse,
  type InstanceResponse,
} from './api';

export interface InstanceDetailLoadResult {
  details: Record<number, InstanceInfoResponse>;
  error: string | null;
  authRequired: boolean;
}

export async function loadInstanceDetails(
  instances: InstanceResponse[],
  token?: string | null,
): Promise<InstanceDetailLoadResult> {
  const results = await Promise.allSettled(
    instances.map(async (instance) => ({
      pid: instance.pid,
      detail: await fetchInstanceInfo(instance.pid, token),
    })),
  );
  const details: Record<number, InstanceInfoResponse> = {};
  let failedRequests = 0;
  let authRequired = false;

  for (const result of results) {
    if (result.status === 'fulfilled') {
      details[result.value.pid] = result.value.detail;
      continue;
    }

    if (result.reason instanceof ApiError && result.reason.status === 401) {
      authRequired = true;
    } else {
      failedRequests += 1;
    }
  }

  return {
    details,
    authRequired,
    error:
      failedRequests > 0
        ? `${failedRequests} instance detail request${failedRequests === 1 ? '' : 's'} failed`
        : null,
  };
}
