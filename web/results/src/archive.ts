import { unzipSync, zipSync } from 'fflate';

export type ArtifactFile = {
  path: string;
  blob?: Blob;
  size: number;
  mtime: number;
  mode?: number;
  kind: 'file' | 'directory';
  compression?: number;
  remoteUrl?: string;
};

function u16(view: DataView, offset: number): number {
  return view.getUint16(offset, true);
}

function u32(view: DataView, offset: number): number {
  return view.getUint32(offset, true);
}

function dosTime(date: number, time: number): number {
  const year = ((date >>> 9) & 0x7f) + 1980;
  const month = (date >>> 5) & 0x0f;
  const day = date & 0x1f;
  const hours = (time >>> 11) & 0x1f;
  const minutes = (time >>> 5) & 0x3f;
  const seconds = (time & 0x1f) * 2;
  // ZIP stores DOS timestamps as local wall-clock time (there is no timezone
  // field). Constructing a local Date keeps the displayed timestamp stable
  // when a ZIP is imported and exported again with fflate.
  const value = new Date(year, Math.max(month - 1, 0), day, hours, minutes, seconds);
  return Number.isNaN(value.getTime()) ? 0 : value.getTime();
}

function normalizePath(path: string): string {
  return path.replaceAll('\\', '/').replace(/^\.\//, '');
}

function centralDirectoryMetadata(bytes: Uint8Array): Map<string, Pick<ArtifactFile, 'mtime' | 'mode' | 'compression'>> {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const start = Math.max(0, bytes.length - 22 - 0xffff);
  let eocd = -1;
  for (let offset = bytes.length - 22; offset >= start; offset -= 1) {
    if (u32(view, offset) === 0x06054b50) {
      eocd = offset;
      break;
    }
  }
  if (eocd < 0) return new Map();

  const count = u16(view, eocd + 10);
  const directoryOffset = u32(view, eocd + 16);
  const decoder = new TextDecoder();
  const metadata = new Map<string, Pick<ArtifactFile, 'mtime' | 'mode' | 'compression'>>();
  let offset = directoryOffset;
  for (let index = 0; index < count && offset + 46 <= bytes.length; index += 1) {
    if (u32(view, offset) !== 0x02014b50) break;
    const flags = u16(view, offset + 8);
    const compression = u16(view, offset + 10);
    const modifiedTime = u16(view, offset + 12);
    const modifiedDate = u16(view, offset + 14);
    const nameLength = u16(view, offset + 28);
    const extraLength = u16(view, offset + 30);
    const commentLength = u16(view, offset + 32);
    const externalAttributes = u32(view, offset + 38);
    const nameBytes = bytes.subarray(offset + 46, offset + 46 + nameLength);
    const name = normalizePath(decoder.decode(nameBytes));
    const unixMode = (externalAttributes >>> 16) & 0xffff;
    metadata.set(name, {
      mtime: dosTime(modifiedDate, modifiedTime),
      mode: unixMode || undefined,
      compression,
    });
    // Bit 11 marks UTF-8. TextDecoder's replacement behavior is preferable to
    // dropping an entry when an old archive uses a legacy filename encoding.
    void flags;
    offset += 46 + nameLength + extraLength + commentLength;
  }
  return metadata;
}

export async function readZip(file: File): Promise<ArtifactFile[]> {
  const bytes = new Uint8Array(await file.arrayBuffer());
  const metadata = centralDirectoryMetadata(bytes);
  const entries = unzipSync(bytes);
  return Object.entries(entries).map(([rawPath, data]) => {
    const path = normalizePath(rawPath);
    const info = metadata.get(path);
    const directory = path.endsWith('/') || Boolean(info?.mode && (info.mode & 0o170000) === 0o040000);
    return {
      path: directory ? path.replace(/\/$/, '') : path,
      blob: new Blob([data], { type: 'application/octet-stream' }),
      size: data.byteLength,
      mtime: info?.mtime || file.lastModified,
      mode: info?.mode,
      kind: directory ? 'directory' : 'file',
      compression: info?.compression,
    } satisfies ArtifactFile;
  });
}

export function readDirectory(files: File[]): ArtifactFile[] {
  return files.map((file) => ({
    path: normalizePath(file.webkitRelativePath || file.name),
    blob: file,
    size: file.size,
    mtime: file.lastModified,
    kind: 'file',
  }));
}

export async function makeZip(files: ArtifactFile[]): Promise<Blob> {
  const entries: Record<string, [Uint8Array, { mtime: Date }]> = {};
  for (const file of files) {
    if (file.kind === 'directory') continue;
    const blob = file.blob ?? (file.remoteUrl ? await fetch(file.remoteUrl).then((response) => {
      if (!response.ok) throw new Error(`Unable to download ${file.path}.`);
      return response.blob();
    }) : null);
    if (!blob) throw new Error(`File data is unavailable for ${file.path}.`);
    entries[file.path] = [new Uint8Array(await blob.arrayBuffer()), { mtime: new Date(file.mtime) }];
  }
  return new Blob([zipSync(entries, { level: 0 })], { type: 'application/zip' });
}

export async function textPreview(file: ArtifactFile, limit = 512_000): Promise<string> {
  if (file.kind === 'directory' || file.size > limit) return `Preview unavailable for files larger than ${limit.toLocaleString()} bytes.`;
  if (file.blob) return file.blob.text();
  if (file.remoteUrl) {
    const response = await fetch(file.remoteUrl);
    if (!response.ok) throw new Error(`Unable to load ${file.path}.`);
    return response.text();
  }
  return 'File data is unavailable.';
}

export function isZip(file: File): boolean {
  return file.name.toLowerCase().endsWith('.zip') || file.type === 'application/zip';
}
