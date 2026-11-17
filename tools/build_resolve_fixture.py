#!/usr/bin/env python3
"""Build the self-made fixtures for ``bloom-index resolve`` deterministically.

Creates a membership index through the real sync pipeline, a strict resolve
control file with several JSONL master-data files (including an empty file),
and the expected JSONL result produced by the real resolve command. Re-running
the script regenerates byte-identical outputs.

Usage from a source checkout:
    PYTHONPATH=src python3 -I tools/build_resolve_fixture.py fixtures/resolve
"""
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

BITS, HASHES = 256, 4

# rid -> (source, anchor, [raw aliases])
RECORDS = [
    # linked acceptance pattern: anchors L/R/null/null, aliases x/y/y,z/x,z
    ('a', 'A', 'L', ['x']),
    ('b', 'B', 'R', ['y']),
    ('c', 'C', None, ['y', 'z']),
    ('d', 'D', None, ['X', 'Ｚ']),          # X casefolds to x; fullwidth Z -> NFKC z
    # same-source shared alias must stay split (source constraint), with evidence
    ('s1', 'A', None, ['shared']),
    ('s2', 'A', None, ['Shared']),          # folds to the same canonical alias
    # inactive: rid q is owned by A, the record claims C
    ('q', 'C', None, ['ghost']),
    # anchor-conflict pair sharing alias k, different sources
    ('e1', 'B', 'K1', ['k']),
    ('e2', 'D', 'K2', ['k']),
]

# Split across files deliberately, including an empty file, to exercise
# cross-file rid uniqueness and the empty-input allowance.
FILE_LAYOUT = {
    'master_1.jsonl': ['a', 'b', 's1', 'e1'],
    'master_2.jsonl': ['c', 'd', 's2', 'q', 'e2'],
    'master_empty.jsonl': [],
}

# Exact (source, item) ownership committed into the membership index.
OWNERSHIP = {
    'A': ['a', 's1', 's2', 'q'],
    'B': ['b', 'e1'],
    'C': ['c'],
    'D': ['d', 'e2'],
}


def write_manifest(path, items):
    with path.open('w', encoding='utf-8') as stream:
        for item in items:
            stream.write(json.dumps({'item': item}, ensure_ascii=False) + '\n')
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main(target):
    target = Path(target)
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    work = Path(tempfile.mkdtemp(prefix='resolve-fixture-'))
    try:
        db = target / 'membership.sqlite3'
        cli = [sys.executable, '-m', 'bloom_index.cli']
        env = dict(os.environ)
        env['PYTHONPATH'] = str(Path(__file__).resolve().parents[1] / 'src')
        run = lambda args: subprocess.run(cli + args, capture_output=True, text=True, env=env, check=True)
        run(['init', '--db', db, '--bits', str(BITS), '--hashes', str(HASHES)])
        updates = []
        for source, items in OWNERSHIP.items():
            manifest = work / f'own_{source}.jsonl'
            digest = write_manifest(manifest, items)
            rel = target / f'ownership_{source}.jsonl'
            shutil.copyfile(manifest, rel)
            updates.append({'source': source, 'generation': 1, 'base_generation': 0,
                            'manifest': f'ownership_{source}.jsonl', 'sha256': digest})
        control_sync = target / 'sync_control.json'
        control_sync.write_text(json.dumps({'updates': updates}, ensure_ascii=False), encoding='utf-8')
        run(['sync', '--db', db, '--source', control_sync])

        by_rid = {rid: (source, anchor, aliases) for rid, source, anchor, aliases in RECORDS}
        files = []
        for name, rids in FILE_LAYOUT.items():
            path = target / name
            with path.open('w', encoding='utf-8') as stream:
                for rid in rids:
                    source, anchor, aliases = by_rid[rid]
                    stream.write(json.dumps(
                        {'rid': rid, 'source': source, 'anchor': anchor, 'aliases': aliases},
                        ensure_ascii=False) + '\n')
            files.append(name)
        control = target / 'control.json'
        control.write_text(json.dumps({'files': files}, ensure_ascii=False), encoding='utf-8')
        result = target / 'expected-result.jsonl'
        run(['resolve', '--db', db, '--source', control, '--output', result])
        for path in target.iterdir():
            if path.is_file():
                os.chmod(path, 0o644)
        # Pin the produced bytes and re-run: the output must be reproducible.
        first = result.read_bytes()
        rerun = work / 'rerun.jsonl'
        run(['resolve', '--db', db, '--source', control, '--output', rerun])
        if rerun.read_bytes() != first:
            raise SystemExit('resolve output is not deterministic across runs')
        (target / 'SOURCES.md').write_text(
            '本目录为原创虚构合成数据，使用MIT许可。\n\n'
            '由 tools/build_resolve_fixture.py 通过真实CLI（init/sync/resolve）离线确定性生成：'
            'membership.sqlite3 为既有成员索引，control.json 列出三个JSONL主数据文件（含一个空文件），'
            'expected-result.jsonl 为 resolve 的字节稳定期望输出。重建：\n\n'
            'PYTHONPATH=src python3 -I tools/build_resolve_fixture.py fixtures/resolve\n',
            encoding='utf-8')
        print(json.dumps({'target': str(target), 'output_lines': first.count(b'\n')},
                         sort_keys=True, ensure_ascii=False))
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == '__main__':
    main(Path(sys.argv[1]) if len(sys.argv) > 1 else Path('fixtures/resolve'))
