#!/usr/bin/env python3
"""Build the deterministic self-made snapshot fixture bundle.

The fixture is generated entirely offline from the real CLI (init/sync/
snapshot) so that ``index.sqlite3`` is a genuine product of the persistence
layer rather than hand-crafted bytes. Its manifest and the exact query report
are also written next to the bundle for regression comparison.

From a source checkout:
    PYTHONPATH=src python3 tools/build_snapshot_fixture.py fixtures/snapshot
"""
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

BITS, HASHES = 256, 4
SOURCES = {
    'fixture-a': [
        (1, ['alpha', 'beta', 'gamma', 'delta']),
        (2, ['beta', 'gamma', 'epsilon']),  # alpha/delta leave, epsilon arrives
    ],
    'fixture-b': [
        (1, ['gamma', 'delta', 'zeta']),
        (2, ['delta', 'eta', 'theta']),     # zeta leaves, eta/theta arrive
    ],
}
QUERIES = ['alpha', 'beta', 'gamma', 'delta', 'epsilon', 'zeta', 'eta', 'theta', 'absent-one']


def write_manifest(path, items):
    body = ''.join(json.dumps({'item': item}, ensure_ascii=False) + '\n' for item in items)
    path.write_text(body, encoding='utf-8')
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    out_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path('fixtures/snapshot')
    cli = [sys.executable, '-m', 'bloom_index.cli']
    env = {**os.environ, 'PYTHONPATH': str(Path('src').resolve()) + os.pathsep + os.environ.get('PYTHONPATH', '')}

    def run(args):
        result = subprocess.run(cli + [str(a) for a in args], capture_output=True, text=True, env=env)
        if result.returncode != 0:
            raise SystemExit('fixture step failed: %s\n%s' % (args, result.stderr))
        return result

    with tempfile.TemporaryDirectory(prefix='bloom-fixture-') as tmp:
        work = Path(tmp)
        db = work / 'fixture.db'
        run(['init', '--db', db, '--bits', BITS, '--hashes', HASHES])
        for generation in (1, 2):
            updates = []
            for source, gens in SOURCES.items():
                gen, items = gens[generation - 1]
                assert gen == generation
                manifest_path = work / ('%s-gen%d.jsonl' % (source, generation))
                digest = write_manifest(manifest_path, items)
                updates.append({'source': source, 'generation': generation,
                                'base_generation': generation - 1,
                                'manifest': str(manifest_path), 'sha256': digest})
            control = work / ('control-gen%d.json' % generation)
            control.write_text(json.dumps({'updates': updates}, ensure_ascii=False), encoding='utf-8')
            run(['sync', '--db', db, '--source', control])
        query_file = work / 'queries.json'
        query_file.write_text(json.dumps({'queries': QUERIES}, ensure_ascii=False), encoding='utf-8')
        report_path = work / 'report.json'
        run(['query', '--db', db, '--source', query_file, '--output', report_path])
        run(['snapshot', '--db', db, '--output', work / 'fixture-bundle'])
        check = run(['snapshot-check', '--bundle', work / 'fixture-bundle'])

        out_dir.mkdir(parents=True, exist_ok=True)
        bundle_dest = out_dir / 'fixture-bundle'
        if os.path.lexists(bundle_dest):
            raise SystemExit('refusing to overwrite existing fixture: ' + str(bundle_dest))
        import shutil
        shutil.move(str(work / 'fixture-bundle'), str(bundle_dest))
        (out_dir / 'query.json').write_text(query_file.read_text(encoding='utf-8'), encoding='utf-8')
        (out_dir / 'expected-report.json').write_text(report_path.read_text(encoding='utf-8'), encoding='utf-8')
        (out_dir / 'expected-check.json').write_text(check.stdout, encoding='utf-8')
    print(json.dumps({'fixture': str(out_dir), 'check': json.loads(check.stdout)},
                     sort_keys=True, ensure_ascii=False))


if __name__ == '__main__':
    main()
