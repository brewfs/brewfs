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
  pushOption(args, '--meta-backend', config.meta_backend);
  if (config.meta_url_redacted) {
    pushOption(args, '--meta-url', '<redacted-meta-url>');
    warnings.push('Meta URL is redacted; provide the real value before running.');
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

function shellQuote(value: string): string {
  if (/^[A-Za-z0-9_./:@%+=,-]+$/.test(value)) return value;
  return `'${value.replace(/'/g, "'\\''")}'`;
}
