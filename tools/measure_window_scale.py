#!/usr/bin/env python3
"""Reproducible peak-RSS measurement for the window pipeline.

Only the ``window-apply`` step runs as a CLI subprocess, so the reported peak is
the child's ``ru_maxrss`` and excludes this driver. Two batches are generated
on disk with the strict JSONL contract:

* batch 1 ingests ``--events`` fresh ``put`` rows across ``--partitions``
  partitions;
* batch 2 applies ``--corrections`` corrections (new revision, new ``ts`` and
  sometimes a new ``key``/``value``) and advances every partition watermark.

Both batches are streamed from disk and validated inside the state database;
no full event list, window list or report is ever held in a Python container.
The driver then runs an independent SQLite recomputation of every window from
the persisted identities and compares it with the published report, and prints
the number of rows in each workspace table so the on-disk (not in-memory)
nature of the state is checkable.

Usage (installed venv):  python3 tools/measure_window_scale.py
From a source checkout:  PYTHONPATH=src python3 tools/measure_window_scale.py
"""
import argparse
import json
import os
import resource
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from bloom_index import window  # noqa: E402

ENCODE = lambda row: (json.dumps(row, sort_keys=True, ensure_ascii=False,
                                 separators=(',', ':')) + '\n').encode('utf-8')


def build_batch_one(path, events, partitions, width):
    with path.open('wb') as stream:
        for index in range(events):
            name = 'p%03d' % (index % partitions)
            stream.write(ENCODE({
                'partition': name, 'seq': index // partitions + 1, 'op': 'put',
                'eid': 'e%07d' % index, 'rev': 1, 'ts': (index * 7) % 100000,
                'key': 'k%04d' % (index % 997), 'value': (index % 2001) - 1000}))


def build_batch_two(path, events, corrections, partitions, width):
    with path.open('wb') as stream:
        per_partition = events // partitions
        next_seq = {name: per_partition for name in
                    ('p%03d' % i for i in range(partitions))}
        for index in range(corrections):
            name = 'p%03d' % (index % partitions)
            next_seq[name] += 1
            seq = next_seq[name]
            stream.write(ENCODE({
                'partition': name, 'seq': seq, 'op': 'put',
                'eid': 'e%07d' % index, 'rev': 2, 'ts': 200000 + index,
                'key': 'k%04d' % ((index + 1) % 997), 'value': (index % 2001) - 1000}))
        for name in sorted(next_seq):
            next_seq[name] += 1
            stream.write(ENCODE({'partition': name, 'seq': next_seq[name],
                                 'op': 'watermark', 'ts': 300000}))


def recompute(state_path, width):
    """Independent SQLite recomputation of every window from persisted identities."""
    conn = sqlite3.connect(str(state_path))
    try:
        rows = conn.execute(
            'SELECT key, ts, value FROM identities WHERE alive = 1').fetchall()
        expected = {}
        for key, ts, value in rows:
            cell = (key, (ts // width) * width)
            count, total = expected.get(cell, (0, 0))
            expected[cell] = (count + 1, total + value)
        tables = {}
        for table in ('seqs', 'identities', 'windows', 'partitions'):
            tables[table] = conn.execute('SELECT COUNT(*) FROM %s' % table).fetchone()[0]
        meta = dict(conn.execute('SELECT key, value FROM meta'))
        return expected, tables, meta
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--events', type=int, default=1_000_000)
    parser.add_argument('--corrections', type=int, default=100_000)
    parser.add_argument('--partitions', type=int, default=8)
    parser.add_argument('--width', type=int, default=10)
    parser.add_argument('--limit-mib', type=int, default=128)
    parser.add_argument('--keep', action='store_true')
    args = parser.parse_args()
    limit_kib = args.limit_mib * 1024
    work = Path(tempfile.mkdtemp(prefix='window-scale-'))
    state = work / 'state.db'
    report = work / 'report.jsonl'
    batch_one = work / 'batch1.jsonl'
    batch_two = work / 'batch2.jsonl'
    control_one = work / 'control1.json'
    control_two = work / 'control2.json'
    env = dict(os.environ)
    env['PYTHONPATH'] = str(Path(__file__).resolve().parents[1] / 'src') + os.pathsep + env.get(
        'PYTHONPATH', '')
    cli = [sys.executable, '-m', 'bloom_index.cli']
    started = time.monotonic()
    try:
        partitions = ['p%03d' % i for i in range(args.partitions)]
        build_batch_one(batch_one, args.events, args.partitions, args.width)
        build_batch_two(batch_two, args.events, args.corrections, args.partitions, args.width)
        control_one.write_text(json.dumps({'files': [batch_one.name]}), encoding='utf-8')
        control_two.write_text(json.dumps({'files': [batch_two.name]}), encoding='utf-8')
        steps = [['window-init', '--state', state, '--partitions', ','.join(partitions),
                  '--width', args.width, '--output', report]]
        for control in (control_one, control_two):
            steps.append(['window-apply', '--state', state, '--source', control])
        for step in steps:
            result = subprocess.run(cli + [str(a) for a in step], capture_output=True,
                                    text=True, env=env)
            if result.returncode != 0:
                print(json.dumps({'error': step[0], 'stderr': result.stderr}, sort_keys=True))
                raise SystemExit(2)
        seconds = round(time.monotonic() - started, 2)
        peak_kib = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
        expected, tables, meta = recompute(state, args.width)
        lines = [json.loads(line) for line in
                 report.read_text(encoding='utf-8').splitlines()]
        summary, windows = lines[0], lines[1:]
        reported = {(w['key'], w['start']): (w['count'], w['sum']) for w in windows}
        watermark = int(summary['watermark'])
        finals_ok = all((w['end'] <= watermark) == w['final'] for w in windows)
        consistent = (summary['epoch'] == 2
                      and int(meta['epoch']) == 2
                      and summary['epoch'] == int(meta['confirmed_epoch'])
                      and reported == expected
                      and finals_ok
                      and tables['seqs'] == args.events + args.corrections + args.partitions
                      and tables['identities'] == args.events)
        result = {
            'events': args.events + args.corrections,
            'corrections': args.corrections, 'partitions': args.partitions,
            'width': args.width, 'epochs': summary['epoch'],
            'windows': len(windows), 'watermark': watermark,
            'state_rows': tables, 'peak_kib': peak_kib, 'limit_kib': limit_kib,
            'report_bytes': report.stat().st_size, 'seconds': seconds,
            'workdir': str(work), 'consistent': consistent,
        }
        print(json.dumps(result, sort_keys=True))
        if not consistent or peak_kib > limit_kib:
            raise SystemExit(1)
    finally:
        if not args.keep:
            shutil.rmtree(work, ignore_errors=True)


if __name__ == '__main__':
    main()
