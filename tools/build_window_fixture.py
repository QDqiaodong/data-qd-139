#!/usr/bin/env python3
"""Build the self-made fixture for ``bloom-index window-*`` deterministically.

The fixture is generated through the real CLI so the reference report is a
genuine product of the persistence layer, not hand-written JSON:

* ``control.json`` lists two strict JSONL change files (partition p then q);
* ``expected-report.jsonl`` is the byte-stable report after that one batch;
* ``replay.json`` lists only the first file, which is a pure replay of already
  accepted seq numbers and must leave both epoch and report bytes unchanged.

Usage from a source checkout:
    PYTHONPATH=src python3 -I tools/build_window_fixture.py fixtures/window
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

PARTITIONS = ['p', 'q']
WIDTH = 10

BATCH_P = [
    {'partition': 'p', 'seq': 1, 'op': 'put', 'eid': 'a', 'rev': 1,
     'ts': 1, 'key': 'x', 'value': 3},
    {'partition': 'p', 'seq': 2, 'op': 'watermark', 'ts': 20},
]
BATCH_Q = [
    {'partition': 'q', 'seq': 1, 'op': 'put', 'eid': 'b', 'rev': 1,
     'ts': 2, 'key': 'x', 'value': 4},
    {'partition': 'q', 'seq': 2, 'op': 'put', 'eid': 'c', 'rev': 1,
     'ts': 12, 'key': 'y', 'value': 6},
    {'partition': 'q', 'seq': 3, 'op': 'watermark', 'ts': 10},
]


def encode(row):
    return json.dumps(row, sort_keys=True, ensure_ascii=False,
                      separators=(',', ':')).encode('utf-8')


def write_lines(path, rows):
    with path.open('wb') as stream:
        for row in rows:
            stream.write(encode(row) + b'\n')


def main(target):
    target = Path(target)
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    work = Path(tempfile.mkdtemp(prefix='window-fixture-'))
    env = dict(os.environ)
    env['PYTHONPATH'] = str(Path(__file__).resolve().parents[1] / 'src')
    cli = [sys.executable, '-m', 'bloom_index.cli']

    def run(args):
        result = subprocess.run(cli + [str(a) for a in args], capture_output=True,
                                text=True, env=env)
        if result.returncode != 0:
            raise SystemExit('fixture step failed: %s\n%s' % (args, result.stderr))
        return result

    try:
        write_lines(target / 'p.jsonl', BATCH_P)
        write_lines(target / 'q.jsonl', BATCH_Q)
        (target / 'control.json').write_text(
            json.dumps({'files': ['p.jsonl', 'q.jsonl']}, ensure_ascii=False),
            encoding='utf-8')
        (target / 'replay.json').write_text(
            json.dumps({'files': ['p.jsonl']}, ensure_ascii=False), encoding='utf-8')

        state = work / 'state.db'
        report = target / 'expected-report.jsonl'
        run(['window-init', '--state', state, '--partitions', ','.join(PARTITIONS),
             '--width', WIDTH, '--output', report])
        run(['window-apply', '--state', state, '--source', target / 'control.json'])
        first = report.read_bytes()
        replay = run(['window-apply', '--state', state, '--source', target / 'replay.json'])
        if report.read_bytes() != first:
            raise SystemExit('pure replay changed the report bytes')
        if json.loads(replay.stdout)['new_seqs'] != 0:
            raise SystemExit('pure replay accepted new sequence numbers')
        run(['window-recover', '--state', state])
        if report.read_bytes() != first:
            raise SystemExit('recovery changed a fully published report')
        for path in target.iterdir():
            if path.is_file():
                os.chmod(path, 0o644)
        (target / 'SOURCES.md').write_text(
            '本目录为原创虚构合成数据，使用MIT许可。\n\n'
            '由 tools/build_window_fixture.py 通过真实CLI（window-init/window-apply/'
            'window-recover）离线确定性生成：control.json 列出两个严格JSONL变更文件，'
            'p.jsonl 与 q.jsonl 为多分区变更（含更正与水位），expected-report.jsonl 为'
            '字节稳定的期望报告，replay.json 仅重放已接受的首个文件（纯重放不改变epoch或报告）。'
            '重建：\n\n'
            'PYTHONPATH=src python3 -I tools/build_window_fixture.py fixtures/window\n',
            encoding='utf-8')
        print(json.dumps({'target': str(target), 'lines': first.count(b'\n')},
                         sort_keys=True, ensure_ascii=False))
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == '__main__':
    main(Path(sys.argv[1]) if len(sys.argv) > 1 else Path('fixtures/window'))
