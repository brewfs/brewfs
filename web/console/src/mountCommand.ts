import type { VolumeResponse } from './api';

export interface MountCommand {
  command: string;
  warnings: string[];
}

export function buildMountCommand(volume: VolumeResponse): MountCommand {
  const config = volume.mount_config;
  const args = ['brewfs', 'mount'];
  const warnings: string[] = [];

  pushOption(args, '--data-backend', config.data_backend);
  pushOption(args, '--data-dir', config.data_dir);
  if (normalizeBackend(config.data_backend) === 's3') {
    warnings.push('S3 bucket and endpoint options are not stored yet; add them before running.');
  }

  pushOption(args, '--meta-backend', config.meta_backend);
  if (config.meta_url_redacted) {
    pushRedactedMetaOption(args, warnings, config.meta_backend);
  }
  pushOption(args, '--chunk-size', config.chunk_size);
  pushOption(args, '--block-size', config.block_size);
  if (config.mount_point) args.push(shellQuote(config.mount_point));

  return {
    command: args.join(' '),
    warnings,
  };
}

function pushOption(args: string[], flag: string, value: string | number | null | undefined) {
  if (value === null || value === undefined || value === '') return;
  args.push(flag, shellQuote(String(value)));
}

function pushRedactedMetaOption(args: string[], warnings: string[], metaBackend: string) {
  switch (normalizeBackend(metaBackend)) {
    case 'sqlx':
    case 'redis':
      pushOption(args, '--meta-url', '<redacted-meta-url>');
      warnings.push('Meta URL is redacted; provide the real value before running.');
      return;
    case 'etcd':
      pushOption(args, '--meta-etcd-urls', '<redacted-etcd-urls>');
      warnings.push('Etcd endpoint URLs are redacted; provide the real values before running.');
      return;
    case 'tikv':
    case 'ti-kv':
      pushOption(args, '--meta-tikv-pd-endpoints', '<redacted-tikv-pd-endpoints>');
      warnings.push('TiKV PD endpoints are redacted; provide the real values before running.');
      return;
    default:
      warnings.push(`Metadata connection options for ${metaBackend} are not stored yet.`);
  }
}

function normalizeBackend(value: string): string {
  return value.trim().toLowerCase();
}

function shellQuote(value: string): string {
  if (/^[A-Za-z0-9_./:@%+=,-]+$/.test(value)) return value;
  return `'${value.replace(/'/g, "'\\''")}'`;
}
