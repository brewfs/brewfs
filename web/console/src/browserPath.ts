export function normalizeBrowserPath(path: string): string {
  const trimmed = path.trim();
  if (!trimmed) return '/';
  const absolute = trimmed.startsWith('/') ? trimmed : `/${trimmed}`;
  const parts: string[] = [];
  for (const part of absolute.split('/')) {
    if (!part || part === '.') continue;
    if (part === '..') {
      parts.pop();
    } else {
      parts.push(part);
    }
  }
  return parts.length === 0 ? '/' : `/${parts.join('/')}`;
}

export function joinBrowserPath(base: string, name: string): string {
  return normalizeBrowserPath(`${base === '/' ? '' : base}/${name}`);
}

export function parentBrowserPath(path: string): string {
  const normalized = normalizeBrowserPath(path);
  if (normalized === '/') return '/';
  return normalizeBrowserPath(normalized.split('/').slice(0, -1).join('/') || '/');
}

export type BrowserBreadcrumb = {
  label: string;
  path: string;
  current: boolean;
};

export function browserBreadcrumbs(path: string): BrowserBreadcrumb[] {
  const normalized = normalizeBrowserPath(path);
  if (normalized === '/') return [{ label: '/', path: '/', current: true }];

  const parts = normalized.split('/').filter(Boolean);
  const breadcrumbs: BrowserBreadcrumb[] = [{ label: '/', path: '/', current: false }];
  parts.forEach((part, index) => {
    const crumbPath = `/${parts.slice(0, index + 1).join('/')}`;
    breadcrumbs.push({
      label: part,
      path: crumbPath,
      current: index === parts.length - 1,
    });
  });
  return breadcrumbs;
}

export function formatMode(mode: number): string {
  return `0${mode.toString(8)}`;
}

export function formatBrowserEntryFlags(entry: { has_acl?: boolean }): string {
  const flags = [];
  if (entry.has_acl) flags.push('ACL');
  return flags.length === 0 ? '-' : flags.join(', ');
}

export type BrowserMvpDataAction = {
  key: 'download' | 'edit';
  label: string;
  enabled: false;
  reason: string;
};

export function browserMvpDataActions(): BrowserMvpDataAction[] {
  return [
    {
      key: 'download',
      label: 'Download',
      enabled: false,
      reason: 'File downloads are outside the metadata-only console MVP.',
    },
    {
      key: 'edit',
      label: 'Edit',
      enabled: false,
      reason: 'File editing is outside the metadata-only console MVP.',
    },
  ];
}

export function showsBrowserDataActionsForKind(kind: string): boolean {
  return kind === 'file' || kind === 'symlink';
}
