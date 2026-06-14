import { describe, expect, it } from 'vitest';
import {
  browserBreadcrumbs,
  browserMvpDataActions,
  formatBrowserEntryFlags,
  formatMode,
  joinBrowserPath,
  normalizeBrowserPath,
  parentBrowserPath,
  showsBrowserDataActionsForKind,
} from './browserPath';

describe('browser path helpers', () => {
  it('normalizes empty, relative, and parent segments', () => {
    expect(normalizeBrowserPath('')).toBe('/');
    expect(normalizeBrowserPath('projects')).toBe('/projects');
    expect(normalizeBrowserPath('/projects/../logs/./today')).toBe('/logs/today');
    expect(normalizeBrowserPath('/../../')).toBe('/');
  });

  it('joins child paths and computes parent paths', () => {
    expect(joinBrowserPath('/', 'docs')).toBe('/docs');
    expect(joinBrowserPath('/docs', 'readme')).toBe('/docs/readme');
    expect(parentBrowserPath('/docs/readme')).toBe('/docs');
    expect(parentBrowserPath('/')).toBe('/');
  });

  it('builds breadcrumb items from normalized browser paths', () => {
    expect(browserBreadcrumbs('/')).toEqual([{ label: '/', path: '/', current: true }]);
    expect(browserBreadcrumbs('/projects/logs')).toEqual([
      { label: '/', path: '/', current: false },
      { label: 'projects', path: '/projects', current: false },
      { label: 'logs', path: '/projects/logs', current: true },
    ]);
    expect(browserBreadcrumbs('projects/../logs/today')).toEqual([
      { label: '/', path: '/', current: false },
      { label: 'logs', path: '/logs', current: false },
      { label: 'today', path: '/logs/today', current: true },
    ]);
  });

  it('formats numeric modes as octal text', () => {
    expect(formatMode(0o644)).toBe('0644');
    expect(formatMode(0o755)).toBe('0755');
  });

  it('formats browser entry capability flags', () => {
    expect(formatBrowserEntryFlags({ has_acl: true })).toBe('ACL');
    expect(formatBrowserEntryFlags({ has_acl: false })).toBe('-');
    expect(formatBrowserEntryFlags({})).toBe('-');
  });

  it('describes data-path actions as disabled in the metadata-only MVP', () => {
    expect(browserMvpDataActions()).toEqual([
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
    ]);
  });

  it('shows data-path actions only for file-like entries', () => {
    expect(showsBrowserDataActionsForKind('file')).toBe(true);
    expect(showsBrowserDataActionsForKind('symlink')).toBe(true);
    expect(showsBrowserDataActionsForKind('directory')).toBe(false);
    expect(showsBrowserDataActionsForKind('unknown')).toBe(false);
  });
});
