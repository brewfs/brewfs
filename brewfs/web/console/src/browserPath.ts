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

export function formatMode(mode: number): string {
  return `0${mode.toString(8)}`;
}

export function formatBrowserEntryFlags(entry: { has_acl?: boolean }): string {
  const flags = [];
  if (entry.has_acl) flags.push('ACL');
  return flags.length === 0 ? '-' : flags.join(', ');
}
