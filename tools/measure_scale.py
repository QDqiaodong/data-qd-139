#!/usr/bin/env python3
"""Reproducible scale and peak-RSS measurement for the persistent bloom index.

Builds a database holding --items distinct items (generation 1), then replaces
the source with a new --lines manifest (generation 2, half overlapping). Each
sync runs as a real CLI subprocess; the child peak RSS (ru_maxrss) must stay
below --limit-mib. Prints a JSON summary and exits nonzero on any violation.

Usage (installed venv):   python3 tools/measure_scale.py
From a source checkout:   PYTHONPATH=src python3 tools/measure_scale.py
"""
import argparse, hashlib, json, os, resource, subprocess, sys, tempfile, time
from pathlib import Path


def write_manifest(path, start, count):
    with open(path, 'w', encoding='utf-8') as stream:
        for i in range(start, start + count):
            stream.write('{"item":"item-%08d"}\n' % i)
    digest = hashlib.sha256()
    with open(path, 'rb') as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def run(args, **kwargs):
    return subprocess.run(args, capture_output=True, text=True, **kwargs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--items', type=int, default=1_000_000, help='distinct items already in the database')
    parser.add_argument('--lines', type=int, default=1_000_000, help='manifest lines of the replacement sync')
    parser.add_argument('--bits', type=int, default=1_000_000)
    parser.add_argument('--hashes', type=int, default=16)
    parser.add_argument('--limit-mib', type=int, default=128)
    parser.add_argument('--keep', action='store_true', help='keep the working directory for inspection')
    args = parser.parse_args()
    limit_kib = args.limit_mib * 1024
    work = tempfile.mkdtemp(prefix='bloom-scale-')
    db = os.path.join(work, 'scale.db')
    cli = [sys.executable, '-m', 'bloom_index.cli']
    peaks, timings = [], {}

    def measured(step, command):
        start = time.monotonic()
        result = run(command)
        timings[step] = round(time.monotonic() - start, 2)
        if result.returncode != 0:
            print(json.dumps({'error': step, 'stderr': result.stderr}, sort_keys=True))
            raise SystemExit(2)
        peaks.append(resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)

    try:
        measured('init', cli + ['init', '--db', db, '--bits', str(args.bits), '--hashes', str(args.hashes)])
        first = os.path.join(work, 'gen1.jsonl')
        digest = write_manifest(first, 0, args.items)
        control = os.path.join(work, 'gen1.json')
        with open(control, 'w') as stream:
            json.dump({'updates': [{'source': 'scale', 'generation': 1, 'base_generation': 0,
                                    'manifest': first, 'sha256': digest}]}, stream)
        measured('sync_gen1', cli + ['sync', '--db', db, '--source', control])
        second = os.path.join(work, 'gen2.jsonl')
        digest = write_manifest(second, args.items // 2, args.lines)
        control = os.path.join(work, 'gen2.json')
        with open(control, 'w') as stream:
            json.dump({'updates': [{'source': 'scale', 'generation': 2, 'base_generation': 1,
                                    'manifest': second, 'sha256': digest}]}, stream)
        measured('sync_gen2', cli + ['sync', '--db', db, '--source', control])
        query = os.path.join(work, 'query.json')
        with open(query, 'w') as stream:
            json.dump({'queries': ['item-%08d' % (args.items // 2), 'item-%08d' % (args.items + args.lines)]}, stream)
        report_path = os.path.join(work, 'report.json')
        measured('query', cli + ['query', '--db', db, '--source', query, '--output', report_path])
        with open(report_path) as stream:
            report = json.load(stream)
        expected_active = args.lines  # replacement semantics: only generation 2 remains owned
        ok = (report['active_items'] == expected_active
              and report['queries'][0]['actual_present']
              and not report['queries'][1]['actual_present']
              and report['false_negatives'] == 0)
        summary = {'items': args.items, 'lines': args.lines, 'bits': args.bits, 'hashes': args.hashes,
                   'commit_seq': report['commit_seq'], 'active_items': report['active_items'],
                   'set_bits': report['set_bits'], 'peak_kib': max(peaks), 'limit_kib': limit_kib,
                   'seconds': timings, 'consistent': ok, 'workdir': work}
        print(json.dumps(summary, sort_keys=True))
        if not ok or max(peaks) > limit_kib:
            raise SystemExit(1)
    finally:
        if not args.keep:
            import shutil
            shutil.rmtree(work, ignore_errors=True)


if __name__ == '__main__':
    main()
