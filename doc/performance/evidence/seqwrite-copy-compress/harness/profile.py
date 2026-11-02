#!/usr/bin/env python3
"""Offline CPU accounting; reuse the historical frame-preserving symbol pipeline."""
import argparse
import collections
import json
import pathlib
import re

ROOT = pathlib.Path('/home/hxy/perf-seqwrite-20261102')
source = (ROOT / 'harness/profile_engine.py').read_text()
pipeline = {}
exec(compile(source.split('\np=argparse.ArgumentParser()')[0], 'historical-profile-pipeline', 'exec'), pipeline)
pipeline['SYMBOL_OPTIONS'] = ['--symfs', str(ROOT / 'symbols'), '--kallsyms',
                            str(ROOT / 'environment/kallsyms.txt')]


def category(frames):
    joined = ';'.join(frames).lower()
    if any(term in joined for term in ['zstd', 'lz4', 'fse_', 'huf_', 'xxh']):
        return 'codec internals'
    if 'chunk::compress::compress' in joined:
        return 'compressed body -> persisted SF frame'
    if 'encode_persisted' in joined:
        return 'raw fragments -> contiguous persisted frame'
    if 'prepare_persisted_upload' in joined:
        return 'large-block sampling / shared fragment handle preparation'
    if 'vfs::cache::page::' in joined or 'cacheslice' in joined:
        return 'FUSE payload -> CacheSlice pages'
    if 'observed_full' in joined and ('realloc' in joined or 'finish_grow' in joined):
        return 'compaction GET aggregation / realloc'
    if 'chunk::compact::' in joined:
        return 'other compaction copies'
    if 'asyncfuse' in joined:
        return 'FUSE request/session or worker future moves'
    # Generic async future names embed S3Backend even when their actual work is
    # the FUSE dispatcher. Do not classify that embedded type as transport.
    if any(term in joined for term in ['aws_', 'hyper::', 'hyper_util', 'cadapter::s3', 'http_body']):
        return 'S3 / HTTP transport (including small future moves)'
    if 'vfs::io::writer' in joined:
        return 'writer (inlined caller unresolved)'
    return 'other / unresolved'


def analyze(directory):
    profile = pipeline['process'](directory)
    total = profile['period']
    copies = collections.Counter()
    chains = collections.Counter()
    all_compress = lz4 = zstd = checksum = legacy_zstd = compress_checksum_union = 0
    compression_module = 0
    for line in (directory / 'cpu.folded').read_text().splitlines():
        stack, weight = line.rsplit(' ', 1)
        weight = int(weight)
        frames = stack.split(';')
        lower = stack.lower()
        if re.search(r'memmove', frames[-1], re.I):
            copies[category(frames[:-1])] += weight
            chains[';'.join(frames[-9:])] += weight
        zstd_encode = any('ZSTD_compress' in frame or ('zstd[' in frame and '::bulk::compressor::' in frame)
                          for frame in frames)
        lz4_encode = any('lz4' in frame.lower() and 'compress' in frame.lower()
                         and 'decompress' not in frame.lower() for frame in frames)
        in_compress = zstd_encode or lz4_encode or any(name in stack for name in [
            'chunk::compress::compress', 'chunk::compress::zstd_samples_worth_compressing'])
        if 'chunk::compress::' in stack:
            compression_module += weight
        in_checksum = any(term in lower for term in ['xxh', 'checksum', 'crc32', 'sha256', 'sha1::', 'md5::'])
        if in_compress:
            all_compress += weight
        if lz4_encode:
            lz4 += weight
        if zstd_encode:
            zstd += weight
        if 'ZSTD_compress' in stack:
            legacy_zstd += weight
        if in_checksum:
            checksum += weight
        if in_compress or in_checksum:
            compress_checksum_union += weight
    copy_total = sum(copies.values())
    workload_summary = json.loads((directory / 'summary.json').read_text())
    written_mib = workload_summary['fio']['io_bytes'] / 1048576
    encode_union = all_compress
    result = {
        'total_cpu_ns': total,
        'memmove_memcpy_self_pct': sum(x['period'] for x in profile['all_symbols']
                                     if re.search('memmove|memcpy', x['symbol'], re.I)) * 100 / total,
        'memmove_only_self_pct': sum(x['period'] for x in profile['all_symbols']
                                  if re.search('memmove', x['symbol'], re.I)) * 100 / total,
        'compression_chain_inclusive_pct': all_compress * 100 / total,
        'compression_module_upper_bound_inclusive_pct': compression_module * 100 / total,
        'lz4_inclusive_pct': lz4 * 100 / total,
        'zstd_inclusive_pct': zstd * 100 / total,
        'zstd_historical_definition_inclusive_pct': legacy_zstd * 100 / total,
        'checksum_inclusive_pct': checksum * 100 / total,
        'compression_and_checksum_union_pct': compress_checksum_union * 100 / total,
        'written_mib': written_mib,
        'total_cpu_ms_per_mib': total / 1000000 / written_mib,
        'memmove_cpu_ms_per_mib': copy_total / 1000000 / written_mib,
        'compression_chain_cpu_ms_per_mib': encode_union / 1000000 / written_mib,
        'copy_categories': [{'category': name, 'cpu_pct': weight * 100 / total,
                             'memmove_share_pct': weight * 100 / copy_total, 'cpu_ns': weight,
                             'cpu_ms_per_mib': weight / 1000000 / written_mib}
                            for name, weight in copies.most_common()],
        'copy_chains': [{'chain': name, 'cpu_pct': weight * 100 / total,
                        'memmove_share_pct': weight * 100 / copy_total, 'cpu_ns': weight}
                       for name, weight in chains.most_common()],
        'samples': profile['samples'], 'lost_samples': profile['lost_samples'],
        'unknown_self_pct': profile['unknown_self_pct'],
        'definition': 'CPU-clock period-weighted; copy attribution denominator is memmove leaf CPU only; kernel memcpy is reported separately in combined share; inclusive groups may overlap',
    }
    (directory / 'copy-cost.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({key: value for key, value in result.items() if key != 'copy_chains'}, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('directories', nargs='+', type=pathlib.Path)
    args = parser.parse_args()
    for directory in args.directories:
        analyze(directory)
