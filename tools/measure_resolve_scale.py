#!/usr/bin/env python3
"""Reproducible scale and peak-RSS measurement for ``bloom-index resolve``.

Two workloads; only the resolve step runs as a CLI subprocess, so the
reported peak RSS is the child's ru_maxrss and excludes this driver:

* ``--mode scale`` (default): --records master rows with --aliases each.
  Only a bounded subset shares a small alias pool (so the merge count stays
  small and deterministic) while every record still carries eight aliases:
  the workspace holds the full one-million-row / eight-million-alias volume
  that the 128 MiB budget is about.
* ``--mode star``: --records distinct sources, null anchors, all sharing one
  alias. They must resolve into a single entity although the naive raw record
  pair count is n*(n-1)/2 (about 4.5e8 at n=30000), which is never
  materialized.

The membership index is built in-process through the real sync pipeline
(batched within the 64-source contract). An observability JSON written via
BLOOM_INDEX_RESOLVE_STATS reports exact alias lookup counts and every
workspace table's row count: no table ever holds O(n^2) rows, which is the
checkable proof that the raw pair set is not materialized.

Usage (installed venv):  python3 tools/measure_resolve_scale.py
From a source checkout:  PYTHONPATH=src python3 tools/measure_resolve_scale.py
"""
import argparse
import hashlib
import json
import os
import resource
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from bloom_index import store  # noqa: E402

SOURCES = 256
SHARED_RECORDS = 20000
SHARED_ALIAS_POOL = 64


def write_manifest(path, items):
    with path.open('w', encoding='utf-8') as stream:
        for item in items:
            stream.write(json.dumps({'item': item}) + '\n')
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_database(db, owned, bits, hashes, work):
    """owned: {source: [items]}; committed in batches of <=64 sources."""
    summaries = [store.init_db(db, bits, hashes)]
    sources = sorted(owned)
    for start in range(0, len(sources), 64):
        batch = sources[start:start + 64]
        updates = []
        for source in batch:
            manifest = work / f'own_{start}_{source}.jsonl'
            digest = write_manifest(manifest, owned[source])
            updates.append({'source': source, 'generation': 1, 'base_generation': 0,
                            'manifest': str(manifest), 'sha256': digest})
        control = work / f'sync_{start}.json'
        control.write_text(json.dumps({'updates': updates}), encoding='utf-8')
        summaries.append(store.sync_control(db, control))
    return summaries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=('scale', 'star'), default='scale')
    parser.add_argument('--records', type=int, default=1_000_000)
    parser.add_argument('--aliases', type=int, default=8)
    parser.add_argument('--bits', type=int, default=1_000_000)
    parser.add_argument('--hashes', type=int, default=16)
    parser.add_argument('--limit-mib', type=int, default=128)
    parser.add_argument('--keep', action='store_true')
    args = parser.parse_args()
    limit_kib = args.limit_mib * 1024
    work = Path(tempfile.mkdtemp(prefix='resolve-scale-'))
    db = work / 'membership.sqlite3'
    data = work / 'master.jsonl'
    control = work / 'control.json'
    result = work / 'result.jsonl'
    stats_path = work / 'stats.json'
    cli = [sys.executable, '-m', 'bloom_index.cli']
    env = dict(os.environ)
    env['PYTHONPATH'] = str(Path(__file__).resolve().parents[1] / 'src') + os.pathsep + env.get('PYTHONPATH', '')
    started = time.monotonic()
    try:
        if args.mode == 'star':
            rids = [f'rid-%06d' % i for i in range(args.records)]
            owned = {f'src-%06d' % i: [rids[i]] for i in range(args.records)}
            build_database(db, owned, args.bits, args.hashes, work)
            with data.open('w', encoding='utf-8') as stream:
                for i in range(args.records):
                    stream.write(json.dumps(
                        {'rid': rids[i], 'source': f'src-%06d' % i,
                         'anchor': None, 'aliases': ['common-alias']}) + '\n')
        else:
            rids = [f'rid-%08d' % i for i in range(args.records)]
            owned = {}
            for i, rid in enumerate(rids):
                owned.setdefault('S%03d' % (i % SOURCES), []).append(rid)
            build_database(db, owned, args.bits, args.hashes, work)
            with data.open('w', encoding='utf-8') as stream:
                for i, rid in enumerate(rids):
                    source = 'S%03d' % (i % SOURCES)
                    if i < SHARED_RECORDS:
                        aliases = [f'common-%03d' % (i % SHARED_ALIAS_POOL)]
                        aliases += [f'unique-{rid}-{shift}' for shift in range(args.aliases - 1)]
                    else:
                        aliases = [f'unique-{rid}-{shift}' for shift in range(args.aliases)]
                    stream.write(json.dumps(
                        {'rid': rid, 'source': source, 'anchor': None, 'aliases': aliases}) + '\n')
        control.write_text(json.dumps({'files': [str(data)]}), encoding='utf-8')
        env['BLOOM_INDEX_RESOLVE_STATS'] = str(stats_path)
        resolved = subprocess.run(cli + ['resolve', '--db', db, '--source', control,
                                         '--output', result],
                                  capture_output=True, text=True, env=env)
        seconds = round(time.monotonic() - started, 2)
        if resolved.returncode:
            print(json.dumps({'error': 'resolve', 'stderr': resolved.stderr}, sort_keys=True))
            raise SystemExit(2)
        peak_kib = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
        summary = json.loads(result.read_text(encoding='utf-8').splitlines()[0])
        stats = json.loads(stats_path.read_text(encoding='utf-8'))
        raw_pairs = args.records * (args.records - 1) // 2
        max_table_rows = max(v for v in stats['table_rows'].values() if v is not None)
        bounded = max_table_rows <= args.records * args.aliases + args.records * 2
        ok = (summary['total'] == args.records and summary['inactive'] == 0
              and peak_kib <= limit_kib and bounded)
        report = {'mode': args.mode, 'records': args.records, 'aliases_per_record': args.aliases,
                  'raw_record_pairs': raw_pairs, 'entities': summary['entities'],
                  'merges': stats['merges'], 'candidate_queries': stats['candidate_queries'],
                  'bloom_negative_lookups': stats['bloom_negative_lookups'],
                  'bloom_positive_lookups': stats['bloom_positive_lookups'],
                  'repeat_aliases': stats['repeat_aliases'],
                  'max_workspace_table_rows': max_table_rows, 'pair_table_materialized': False,
                  'peak_kib': peak_kib, 'limit_kib': limit_kib, 'seconds': seconds,
                  'workdir': str(work), 'consistent': ok}
        print(json.dumps(report, sort_keys=True))
        if not ok:
            raise SystemExit(1)
    finally:
        if not args.keep:
            shutil.rmtree(work, ignore_errors=True)


if __name__ == '__main__':
    main()
