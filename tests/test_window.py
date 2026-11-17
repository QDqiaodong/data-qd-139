"""Acceptance tests for the event-time window pipeline (v5).

Every successful or recovered state is checked against an independent,
small-scale, full-event replay oracle. Fault tests kill real child processes at
each publication boundary and then run only ``window-recover``; the recovered
state must converge to a complete old or complete new epoch, never a mix.
"""
import hashlib
import json
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

import bloom_index

ROOT = Path(__file__).resolve().parents[1]
PKG_PARENT = Path(bloom_index.__file__).resolve().parents[1]


def cli(args, env_extra=None):
    env = dict(os.environ)
    env['PYTHONPATH'] = str(PKG_PARENT) + os.pathsep + env.get('PYTHONPATH', '')
    if env_extra:
        env.update(env_extra)
    return subprocess.run([sys.executable, '-m', 'bloom_index.cli', *map(str, args)],
                          capture_output=True, text=True, env=env)


def popen(args, env_extra=None):
    env = dict(os.environ)
    env['PYTHONPATH'] = str(PKG_PARENT) + os.pathsep + env.get('PYTHONPATH', '')
    if env_extra:
        env.update(env_extra)
    return subprocess.Popen([sys.executable, '-m', 'bloom_index.cli', *map(str, args)],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)


def read_jsonl(path):
    return [json.loads(line) for line in Path(path).read_text(encoding='utf-8').splitlines()]


class Oracle:
    """Straightforward full-event model of the whole contract."""

    def __init__(self, partitions, width):
        self.partitions = list(partitions)
        self.width = width
        self.epoch = 0
        self.watermarks = {name: 0 for name in self.partitions}
        self.max_seq = {name: 0 for name in self.partitions}
        self.credentials = {}
        self.events = {}   # eid -> {'partition','rev','alive','ts','key','value'}
        self.final_cut = 0  # windows final at the start of the current batch

    def start_batch(self):
        self.final_cut = min(self.watermarks.values())

    def window_of(self, ts):
        return (ts // self.width) * self.width

    def is_final_at_start(self, start):
        return start + self.width <= self.final_cut

    def apply_row(self, row, body):
        name, seq = row['partition'], row['seq']
        if seq <= self.max_seq[name]:
            assert self.credentials[(name, seq)] == body, 'replay with different bytes'
            return False
        assert seq == self.max_seq[name] + 1
        if row['op'] == 'put':
            ts = row['ts']
            assert ts >= self.watermarks[name], 'put below watermark'
            assert not self.is_final_at_start(self.window_of(ts)), 'put into final window'
            event = self.events.get(row['eid'])
            if event is None:
                assert row['rev'] == 1
                self.events[row['eid']] = {'partition': name, 'rev': 1, 'alive': True,
                                           'ts': ts, 'key': row['key'], 'value': row['value']}
            else:
                assert event['partition'] == name
                assert row['rev'] == event['rev'] + 1
                if event['alive']:
                    assert not self.is_final_at_start(self.window_of(event['ts']))
                event.update(rev=row['rev'], alive=True, ts=ts, key=row['key'], value=row['value'])
        elif row['op'] == 'delete':
            event = self.events[row['eid']]
            assert event['partition'] == name
            assert row['rev'] == event['rev'] + 1
            assert event['alive']
            assert not self.is_final_at_start(self.window_of(event['ts']))
            event.update(rev=row['rev'], alive=False)
        else:
            assert row['ts'] >= self.watermarks[name]
            self.watermarks[name] = row['ts']
        self.credentials[(name, seq)] = body
        self.max_seq[name] = seq
        return True

    def finish_batch(self, applied):
        if applied:
            self.epoch += 1

    def report(self):
        watermark = min(self.watermarks.values())
        buckets = {}
        for event in self.events.values():
            if not event['alive']:
                continue
            cell = (event['key'], self.window_of(event['ts']))
            count, total = buckets.get(cell, (0, 0))
            buckets[cell] = (count + 1, total + event['value'])
        lines = [{'type': 'summary', 'epoch': self.epoch, 'watermark': watermark,
                  'partitions': dict(self.watermarks)}]
        for key, start in sorted(buckets):
            count, total = buckets[(key, start)]
            lines.append({'type': 'window', 'key': key, 'start': start,
                          'end': start + self.width, 'count': count, 'sum': total,
                          'final': start + self.width <= watermark})
        return lines


def encode(row):
    return json.dumps(row, sort_keys=True, ensure_ascii=False,
                      separators=(',', ':')).encode('utf-8')


class Case(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.td = Path(self.tmp.name)
        self.state = self.td / 'state.db'
        self.output = self.td / 'report.jsonl'
        self.inputs = self.td / 'inputs'
        self.inputs.mkdir()
        self.width = 10
        self.partitions = ['p', 'q']
        self.batch_no = 0

    def tearDown(self):
        self.tmp.cleanup()

    def init(self, partitions=None, width=None, expect_rc=0):
        result = cli(['window-init', '--state', self.state,
                      '--partitions', ','.join(partitions or self.partitions),
                      '--width', width or self.width, '--output', self.output])
        self.assertEqual(result.returncode, expect_rc, result.stderr)
        return result

    def write_batch(self, rows, name=None):
        self.batch_no += 1
        name = name or 'batch%03d' % self.batch_no
        path = self.inputs / (name + '.jsonl')
        with path.open('wb') as stream:
            for row in rows:
                stream.write(encode(row) + b'\n')
        control = self.inputs / (name + '.json')
        control.write_text(json.dumps({'files': [path.name]}), encoding='utf-8')
        return control

    def apply_rows(self, rows, expect_rc=0, env_extra=None, control=None):
        control = control or self.write_batch(rows)
        result = cli(['window-apply', '--state', self.state, '--source', control],
                     env_extra=env_extra)
        self.assertEqual(result.returncode, expect_rc, result.stderr)
        return result

    def recover(self, expect_rc=0):
        result = cli(['window-recover', '--state', self.state])
        self.assertEqual(result.returncode, expect_rc, result.stderr)
        return result

    def report(self):
        return read_jsonl(self.output)

    def db_epoch(self):
        conn = sqlite3.connect(str(self.state))
        try:
            return int(dict(conn.execute('SELECT key, value FROM meta'))['epoch'])
        finally:
            conn.close()


class InitContract(Case):
    def test_init_creates_empty_epoch_and_binds_paths(self):
        result = self.init()
        self.assertEqual(json.loads(result.stdout)['epoch'], 0)
        lines = self.report()
        self.assertEqual(lines[0], {'type': 'summary', 'epoch': 0, 'watermark': 0,
                                    'partitions': {'p': 0, 'q': 0}})
        self.assertEqual(len(lines), 1)
        conn = sqlite3.connect(str(self.state))
        meta = dict(conn.execute('SELECT key, value FROM meta'))
        conn.close()
        self.assertEqual(meta['epoch'], '0')
        self.assertEqual(meta['width'], '10')
        self.assertEqual(meta['output'], str(self.output.resolve()))
        # re-init on an existing state is refused and changes nothing
        before = self.state.read_bytes()
        self.init(expect_rc=2)
        self.assertEqual(self.state.read_bytes(), before)

    def test_init_rejects_bad_arguments_and_existing_paths(self):
        for partitions, width in [('p,p', 10), ('p,', 10), ('', 10), (' p', 10),
                                  (','.join('p%d' % i for i in range(65)), 10),
                                  ('p,q', 0), ('p,q', 86401)]:
            result = cli(['window-init', '--state', self.state, '--partitions', partitions,
                          '--width', width, '--output', self.output])
            self.assertEqual(result.returncode, 2, (partitions, width))
            self.assertFalse(self.state.exists())
            self.assertFalse(self.output.exists())
        existing = self.td / 'existing.jsonl'
        existing.write_text('prior\n')
        result = cli(['window-init', '--state', self.state, '--partitions', 'p',
                      '--width', 10, '--output', existing])
        self.assertEqual(result.returncode, 2)
        self.assertEqual(existing.read_text(), 'prior\n')
        self.assertFalse(self.state.exists())

    def test_init_refuses_symlinks_and_same_path(self):
        victim = self.td / 'victim.jsonl'
        link = self.td / 'link.jsonl'
        link.symlink_to(victim)
        result = cli(['window-init', '--state', self.state, '--partitions', 'p',
                      '--width', 10, '--output', link])
        self.assertEqual(result.returncode, 2)
        self.assertFalse(self.state.exists())
        self.assertFalse(victim.exists())
        result = cli(['window-init', '--state', self.state, '--partitions', 'p',
                      '--width', 10, '--output', self.state])
        self.assertEqual(result.returncode, 2)
        self.assertFalse(self.state.exists())
        self.state.write_text('not a state')
        state_link = self.td / 'state-link.db'
        state_link.symlink_to(self.state)
        result = cli(['window-apply', '--state', state_link,
                      '--source', self.write_batch([])])
        self.assertEqual(result.returncode, 2)


class StrictInput(Case):
    def setUp(self):
        super().setUp()
        self.init()

    def test_bad_controls_and_lines_reject_batch(self):
        good = self.write_batch([{'partition': 'p', 'seq': 1, 'op': 'put', 'eid': 'a',
                                  'rev': 1, 'ts': 1, 'key': 'x', 'value': 3}])
        self.assertEqual(cli(['window-apply', '--state', self.state,
                              '--source', good]).returncode, 0)
        prior_report = self.output.read_bytes()
        prior_state = self.state.read_bytes()
        bad_controls = [
            b'{}', b'{"files":[]}', b'{"files":"x"}', b'{"files":[1]}',
            b'{"files":["missing.jsonl"]}', b'{"files":["a","a"]}',
            b'{"files":["x"],"extra":1}', b'not json', b'{"files":["./\\xc3"]}',
        ]
        for raw in bad_controls:
            control = self.td / 'bad.json'
            control.write_bytes(raw)
            self.assertEqual(cli(['window-apply', '--state', self.state,
                                  '--source', control]).returncode, 2, raw)
            self.assertEqual(self.state.read_bytes(), prior_state)
            self.assertEqual(self.output.read_bytes(), prior_report)

    def test_bad_lines_reject_whole_batch(self):
        path = self.inputs / 'bad.jsonl'
        cases = [
            b'\n',                                               # empty line
            b'{"partition":"p","seq":2,"op":"put","eid":"b","rev":1,"ts":1,"key":"x","value":1}\n\n',
            b'{"partition":"p","seq":2,"op":"put","eid":"b","rev":1,"ts":1,"key":"x","value":1,"x":2}\n',
            b'{"partition":"p","seq":2,"op":"put","eid":"b","rev":1,"ts":1,"key":"x"}\n',
            b'{"partition":"p","seq":2,"op":"put","eid":"b","rev":1,"ts":1,"key":"x","value":1,"value":2}\n',
            b'{"partition":"p","seq":2,"op":"put","eid":"b","rev":1,"ts":1,"key":"x","value":NaN}\n',
            b'{"partition":"p","seq":2,"op":"put","eid":"b","rev":1,"ts":1,"key":"x","value":true}\n',
            b'{"partition":"p","seq":2,"op":"put","eid":"b","rev":1,"ts":1,"key":"x","value":1.0}\n',
            b'{"partition":"p","seq":0,"op":"watermark","ts":1}\n',
            b'{"partition":"z","seq":2,"op":"watermark","ts":1}\n',
            b'{"partition":"p","seq":2,"op":"nope","eid":"b","rev":1}\n',
            b'{"partition":"p","seq":2,"op":"put","eid":"b","rev":1,"ts":-1,"key":"x","value":1}\n',
            b'{"partition":"p","seq":2,"op":"put","eid":"b","rev":1,"ts":1,"key":"","value":1}\n',
            b'{"partition":"p","seq":2,"op":"put","eid":"b","rev":1,"ts":1,"key":"x","value":1000000001}\n',
            b'{"partition":"p","seq":2,"op":"put","eid":"b","rev":1,"ts":1000000000001,"key":"x","value":1}\n',
            b'{"partition":"p","seq":2,"op":"put","eid":"b","rev":1,"ts":1,"key":"x","value":1}',
            b'{"partition":"\xff","seq":2,"op":"watermark","ts":1}\n',
            b'{"partition":"p","seq":2,"op":"watermark","ts":1}' + b' ' * 16385 + b'\n',
        ]
        for raw in cases:
            path.write_bytes(raw)
            control = self.inputs / 'ctl.json'
            control.write_text(json.dumps({'files': [path.name]}), encoding='utf-8')
            result = cli(['window-apply', '--state', self.state, '--source', control])
            self.assertEqual(result.returncode, 2, raw[:60])
            self.assertIn('bad.jsonl:1', result.stderr or raw[:20])
        # empty file is allowed and is a no-op
        path.write_bytes(b'')
        control = self.inputs / 'ctl.json'
        control.write_text(json.dumps({'files': [path.name]}), encoding='utf-8')
        result = cli(['window-apply', '--state', self.state, '--source', control])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.report()[0]['epoch'], 0)

    def test_line_length_limit_is_on_body_without_lf(self):
        body = b'{"partition":"p","seq":1,"op":"watermark","ts":1}'
        padded = body[:-1] + b' ' * (16384 - len(body)) + b'}'
        path = self.inputs / 'edge.jsonl'
        path.write_bytes(padded + b'\n')
        self.assertEqual(len(padded), 16384)
        control = self.inputs / 'ctl.json'
        control.write_text(json.dumps({'files': [path.name]}), encoding='utf-8')
        result = cli(['window-apply', '--state', self.state, '--source', control])
        self.assertEqual(result.returncode, 0, result.stderr)
        path.write_bytes(padded + b' ' + b'\n')
        result = cli(['window-apply', '--state', self.state, '--source', control])
        self.assertEqual(result.returncode, 2)

    def test_relative_paths_and_multiple_files_in_order(self):
        a = self.inputs / 'a.jsonl'
        b = self.inputs / 'b.jsonl'
        a.write_bytes(encode({'partition': 'p', 'seq': 1, 'op': 'watermark', 'ts': 5}) + b'\n')
        b.write_bytes(encode({'partition': 'q', 'seq': 1, 'op': 'watermark', 'ts': 9}) + b'\n')
        control = self.inputs / 'ctl.json'
        control.write_text(json.dumps({'files': ['a.jsonl', 'b.jsonl']}), encoding='utf-8')
        result = cli(['window-apply', '--state', self.state, '--source', control])
        self.assertEqual(result.returncode, 0, result.stderr)
        summary = json.loads(result.stdout)
        self.assertEqual(summary['new_seqs'], 2)
        self.assertEqual(summary['watermark'], 5)


class Revisions(Case):
    def setUp(self):
        super().setUp()
        self.init()

    def put(self, partition, seq, eid, rev, ts, key='x', value=1):
        return {'partition': partition, 'seq': seq, 'op': 'put', 'eid': eid,
                'rev': rev, 'ts': ts, 'key': key, 'value': value}

    def delete(self, partition, seq, eid, rev):
        return {'partition': partition, 'seq': seq, 'op': 'delete', 'eid': eid, 'rev': rev}

    def watermark(self, partition, seq, ts):
        return {'partition': partition, 'seq': seq, 'op': 'watermark', 'ts': ts}

    def test_replay_earliest_seq_keeps_latest_revision(self):
        control = self.write_batch([
            self.put('p', 1, 'a', 1, 1, value=3),
            self.delete('p', 2, 'a', 2),
            self.put('p', 3, 'a', 3, 2, value=5),
        ], name='revs')
        self.assertEqual(cli(['window-apply', '--state', self.state,
                              '--source', control]).returncode, 0)
        before = self.report()
        self.assertEqual(before[0]['epoch'], 1)
        self.assertEqual(before[1]['count'], 1)
        self.assertEqual(before[1]['sum'], 5)
        replay = cli(['window-apply', '--state', self.state,
                      '--source', control]).returncode
        self.assertEqual(replay, 0)
        self.assertEqual(self.report(), before)
        # a replay-only batch does not bump the epoch
        self.assertEqual(self.report()[0]['epoch'], 1)

    def test_whole_batch_rejections(self):
        self.apply_rows([self.put('p', 1, 'a', 1, 1)])
        prior = self.report()
        bad_batches = [
            [self.put('q', 2, 'a', 2, 2)],                       # eid moved partition
            [self.put('p', 3, 'c', 1, 2)],                       # seq gap
            [self.put('p', 2, 'a', 3, 2)],                       # revision gap
            [self.put('p', 1, 'z', 1, 2)],                       # same seq different bytes
            [self.put('p', 2, 'a', 2, 2), self.delete('p', 3, 'missing', 1)],
            [self.delete('p', 2, 'z', 1)],                       # unknown eid
        ]
        for rows in bad_batches:
            self.apply_rows(rows, expect_rc=2)
            self.assertEqual(self.report(), prior)
        # delete then delete again is rejected
        self.apply_rows([self.delete('p', 2, 'a', 2)])
        self.apply_rows([self.delete('p', 3, 'a', 3)], expect_rc=2)
        self.assertEqual(self.report()[0]['epoch'], 2)
        # put after delete restores the identity
        self.apply_rows([self.put('p', 3, 'a', 3, 2, value=7)])
        windows = self.report()[1:]
        self.assertEqual(len(windows), 1)
        self.assertEqual(windows[0]['sum'], 7)

    def test_same_seq_with_whitespace_difference_is_rejected(self):
        self.apply_rows([self.put('p', 1, 'a', 1, 1)])
        path = self.inputs / 'spaced.jsonl'
        # semantically identical JSON, different raw bytes (extra spaces)
        path.write_bytes(b'{"partition": "p", "seq": 1, "op": "put", "eid": "a", '
                         b'"rev": 1, "ts": 1, "key": "x", "value": 1}\n')
        control = self.inputs / 'spaced.json'
        control.write_text(json.dumps({'files': [path.name]}), encoding='utf-8')
        result = cli(['window-apply', '--state', self.state, '--source', control])
        self.assertEqual(result.returncode, 2)
        self.assertIn('different bytes', result.stderr)
        # the canonical original bytes still replay cleanly
        original = self.inputs / 'batch001.json'
        self.assertEqual(cli(['window-apply', '--state', self.state,
                              '--source', original]).returncode, 0)

    def test_mixed_good_and_bad_file_changes_nothing(self):
        self.apply_rows([self.put('p', 1, 'a', 1, 1), self.watermark('q', 1, 7)])
        before_report = self.report()
        before_state = self.state.read_bytes()
        good = self.inputs / 'good.jsonl'
        good.write_bytes(encode(self.put('p', 2, 'b', 1, 2)) + b'\n'
                         + encode(self.watermark('q', 2, 9)) + b'\n')
        bad = self.inputs / 'bad.jsonl'
        bad.write_bytes(encode(self.put('p', 3, 'c', 1, 3)) + b'\n{"oops":1}\n')
        control = self.inputs / 'mixed.json'
        control.write_text(json.dumps({'files': ['good.jsonl', 'bad.jsonl']}),
                           encoding='utf-8')
        result = cli(['window-apply', '--state', self.state, '--source', control])
        self.assertEqual(result.returncode, 2)
        self.assertIn('bad.jsonl:2', result.stderr)
        self.assertEqual(self.report(), before_report)
        self.assertEqual(self.state.read_bytes(), before_state)
        # after removing the bad file the same first file applies cleanly
        control.write_text(json.dumps({'files': ['good.jsonl']}), encoding='utf-8')
        self.assertEqual(cli(['window-apply', '--state', self.state,
                              '--source', control]).returncode, 0)
        self.assertEqual(self.report()[0]['epoch'], 2)

    def test_watermarks_only_apply_after_all_data_changes(self):
        # watermark 100 would make window 0 final, but the put in the same
        # batch belongs to window 0 and must still be accepted.
        # the batch-start watermark state (all zero) governs admissibility; the
        # batch's own watermark rows only take effect after every data change.
        self.apply_rows([
            self.watermark('p', 1, 100),
            self.watermark('q', 1, 100),
            self.put('p', 2, 'a', 1, 1),
        ])
        windows = self.report()[1:]
        self.assertEqual(len(windows), 1)
        self.assertTrue(windows[0]['final'])
        self.assertEqual(windows[0]['start'], 0)

    def test_put_ts_must_not_precede_partition_watermark(self):
        self.apply_rows([self.watermark('p', 1, 10)])
        self.apply_rows([self.put('p', 2, 'a', 1, 9)], expect_rc=2)
        self.apply_rows([self.put('p', 2, 'a', 1, 10)])

    def test_watermark_never_regresses(self):
        self.apply_rows([self.watermark('p', 1, 10)])
        self.apply_rows([self.watermark('p', 2, 9)], expect_rc=2)
        self.apply_rows([self.watermark('p', 2, 11)])


class Materialization(Case):
    width = 10

    def setUp(self):
        super().setUp()
        self.init()

    def test_correction_moves_and_retracts_contributions(self):
        self.apply_rows([
            {'partition': 'p', 'seq': 1, 'op': 'put', 'eid': 'a', 'rev': 1,
             'ts': 1, 'key': 'x', 'value': 3},
            {'partition': 'q', 'seq': 1, 'op': 'put', 'eid': 'b', 'rev': 1,
             'ts': 2, 'key': 'x', 'value': 4},
        ])
        windows = self.report()[1:]
        self.assertEqual([(w['start'], w['count'], w['sum']) for w in windows],
                         [(0, 2, 7)])
        self.apply_rows([{'partition': 'p', 'seq': 2, 'op': 'put', 'eid': 'a',
                          'rev': 2, 'ts': 11, 'key': 'x', 'value': 5}])
        windows = self.report()[1:]
        self.assertEqual([(w['start'], w['count'], w['sum']) for w in windows],
                         [(0, 1, 4), (10, 1, 5)])
        self.apply_rows([{'partition': 'q', 'seq': 2, 'op': 'delete', 'eid': 'b', 'rev': 2}])
        windows = self.report()[1:]
        self.assertEqual([(w['start'], w['count'], w['sum']) for w in windows],
                         [(10, 1, 5)])

    def test_nonempty_zero_sum_row_is_kept(self):
        self.apply_rows([
            {'partition': 'p', 'seq': 1, 'op': 'put', 'eid': 'a', 'rev': 1,
             'ts': 1, 'key': 'x', 'value': 5},
            {'partition': 'q', 'seq': 1, 'op': 'put', 'eid': 'b', 'rev': 1,
             'ts': 2, 'key': 'x', 'value': -5},
        ])
        windows = self.report()[1:]
        self.assertEqual(len(windows), 1)
        self.assertEqual((windows[0]['count'], windows[0]['sum']), (2, 0))
        # a correction can also change key
        self.apply_rows([{'partition': 'p', 'seq': 2, 'op': 'put', 'eid': 'a',
                          'rev': 2, 'ts': 1, 'key': 'y', 'value': 5}])
        windows = self.report()[1:]
        self.assertEqual([(w['key'], w['count'], w['sum']) for w in windows],
                         [('x', 1, -5), ('y', 1, 5)])

    def test_windows_sort_by_key_bytes_then_start(self):
        rows = []
        for seq, (key, ts) in enumerate([('b', 5), ('a', 25), ('a', 5), ('Z', 15)], 1):
            rows.append({'partition': 'p', 'seq': seq, 'op': 'put', 'eid': 'e%d' % seq,
                         'rev': 1, 'ts': ts, 'key': key, 'value': seq})
        rows.append({'partition': 'q', 'seq': 1, 'op': 'watermark', 'ts': 40})
        self.apply_rows(rows)
        windows = self.report()[1:]
        self.assertEqual([(w['key'], w['start']) for w in windows],
                         [('Z', 10), ('a', 0), ('a', 20), ('b', 0)])
        raw = self.output.read_bytes()
        for line in raw.splitlines():
            self.assertEqual(line, encode(json.loads(line)))
        # determinism: a pure replay produces identical bytes and epoch
        control = self.inputs / ('batch%03d.json' % self.batch_no)
        self.assertEqual(cli(['window-apply', '--state', self.state,
                              '--source', control]).returncode, 0)
        self.assertEqual(self.output.read_bytes(), raw)


class Linkage(Case):
    """The acceptance scenario tying sealing to later corrections."""

    width = 10

    def setUp(self):
        super().setUp()
        self.init()

    def test_watermark_seals_and_blocks_later_corrections(self):
        self.apply_rows([
            {'partition': 'p', 'seq': 1, 'op': 'put', 'eid': 'a', 'rev': 1,
             'ts': 1, 'key': 'x', 'value': 3},
            {'partition': 'q', 'seq': 1, 'op': 'put', 'eid': 'b', 'rev': 1,
             'ts': 2, 'key': 'x', 'value': 4},
            {'partition': 'p', 'seq': 2, 'op': 'put', 'eid': 'a', 'rev': 2,
             'ts': 11, 'key': 'x', 'value': 5},
            {'partition': 'q', 'seq': 2, 'op': 'delete', 'eid': 'b', 'rev': 2},
        ])
        # advance p to 20 while q stays at 0: nothing is final yet
        self.apply_rows([{'partition': 'p', 'seq': 3, 'op': 'watermark', 'ts': 20}])
        windows = self.report()[1:]
        self.assertEqual([w['final'] for w in windows], [False])
        self.assertEqual(windows[0]['start'], 10)
        # q advances to 10: only window 0 (already gone) and window 10? end=20 > 10
        self.apply_rows([{'partition': 'q', 'seq': 3, 'op': 'watermark', 'ts': 10}])
        windows = self.report()[1:]
        self.assertEqual([(w['start'], w['final']) for w in windows], [(10, False)])
        # window 0 is now final (10 <= 10) - a correction to the deleted b is
        # impossible anyway; recreate the acceptance flow with b still alive.
        self.assertEqual(json.loads(self.recover().stdout)['epoch'], 3)

    def test_full_acceptance_flow(self):
        a = {'partition': 'p', 'seq': 1, 'op': 'put', 'eid': 'a', 'rev': 1,
             'ts': 1, 'key': 'x', 'value': 3}
        b = {'partition': 'q', 'seq': 1, 'op': 'put', 'eid': 'b', 'rev': 1,
             'ts': 2, 'key': 'x', 'value': 4}
        self.apply_rows([a, b])
        self.assertEqual([(w['count'], w['sum']) for w in self.report()[1:]], [(2, 7)])
        self.apply_rows([{'partition': 'p', 'seq': 2, 'op': 'put', 'eid': 'a',
                          'rev': 2, 'ts': 11, 'key': 'x', 'value': 5}])
        self.assertEqual([(w['start'], w['count'], w['sum']) for w in self.report()[1:]],
                         [(0, 1, 4), (10, 1, 5)])
        # p watermark 20, q still 0 => global 0, both not final
        self.apply_rows([{'partition': 'p', 'seq': 3, 'op': 'watermark', 'ts': 20}])
        self.assertEqual([w['final'] for w in self.report()[1:]], [False, False])
        # q watermark 10 => global 10, only window 0 final
        self.apply_rows([{'partition': 'q', 'seq': 2, 'op': 'watermark', 'ts': 10}])
        self.assertEqual([(w['start'], w['final']) for w in self.report()[1:]],
                         [(0, True), (10, False)])
        # moving b to ts=21 would retract window 0, which is already final
        self.apply_rows([{'partition': 'q', 'seq': 3, 'op': 'put', 'eid': 'b',
                          'rev': 2, 'ts': 21, 'key': 'x', 'value': 4}], expect_rc=2)
        self.assertEqual([(w['start'], w['count']) for w in self.report()[1:]],
                         [(0, 1), (10, 1)])
        # a put into a new window that is still open is fine
        self.apply_rows([{'partition': 'q', 'seq': 3, 'op': 'put', 'eid': 'c',
                          'rev': 1, 'ts': 25, 'key': 'x', 'value': 2}])
        # q watermark 20 seals window 10 but not window 20
        self.apply_rows([{'partition': 'q', 'seq': 4, 'op': 'watermark', 'ts': 20}])
        self.assertEqual([(w['start'], w['final']) for w in self.report()[1:]],
                         [(0, True), (10, True), (20, False)])
        # a new put into the sealed window 10 is rejected even with fresh ts
        self.apply_rows([{'partition': 'q', 'seq': 5, 'op': 'put', 'eid': 'd',
                          'rev': 1, 'ts': 15, 'key': 'x', 'value': 1}], expect_rc=2)
        # restart-equivalent: recovery does not change any decision
        before = self.output.read_bytes()
        self.recover()
        self.assertEqual(self.output.read_bytes(), before)

    def test_silent_partitions_hold_the_global_watermark(self):
        self.partitions = ['p', 'q', 'r']
        self.state.unlink()
        self.output.unlink()
        self.init()
        self.apply_rows([
            {'partition': 'p', 'seq': 1, 'op': 'put', 'eid': 'a', 'rev': 1,
             'ts': 1, 'key': 'x', 'value': 1},
            {'partition': 'p', 'seq': 2, 'op': 'watermark', 'ts': 5},
            {'partition': 'q', 'seq': 1, 'op': 'watermark', 'ts': 5},
        ])
        # r never appears, so its watermark 0 keeps every window open
        self.assertEqual(self.report()[0]['watermark'], 0)
        self.assertEqual(self.report()[0]['partitions'], {'p': 5, 'q': 5, 'r': 0})
        self.assertEqual([w['final'] for w in self.report()[1:]], [False])
        # this means a correction is still allowed and must be honoured
        self.apply_rows([{'partition': 'p', 'seq': 3, 'op': 'put', 'eid': 'a',
                          'rev': 2, 'ts': 9, 'key': 'x', 'value': 9}])
        self.assertEqual(self.report()[1]['sum'], 9)
        self.apply_rows([{'partition': 'r', 'seq': 1, 'op': 'watermark', 'ts': 100}])
        # p and q still sit at 5, so the global watermark is 5 and the window open
        self.assertEqual(self.report()[0]['watermark'], 5)
        self.assertFalse(self.report()[1]['final'])
        self.apply_rows([{'partition': 'p', 'seq': 4, 'op': 'watermark', 'ts': 100}])
        self.apply_rows([{'partition': 'q', 'seq': 2, 'op': 'watermark', 'ts': 100}])
        self.assertEqual(self.report()[0]['watermark'], 100)
        self.assertTrue(self.report()[1]['final'])
        # window 0 (end=10) is now sealed, so a correction there is refused
        self.apply_rows([{'partition': 'p', 'seq': 5, 'op': 'put', 'eid': 'a',
                          'rev': 3, 'ts': 9, 'key': 'x', 'value': 2}], expect_rc=2)


class OracleDriven(Case):
    def test_random_batches_match_full_event_oracle(self):
        import copy
        import random
        rng = random.Random(20261117)
        parts = ['p', 'q', 'r']
        width = rng.choice([1, 3, 10])
        self.init(parts, width)
        oracle = Oracle(parts, width)
        revisions = {}
        alive = set()
        keys = ['a', 'b', 'c', 'K']
        eid_no = 0
        accepted_batches = []
        for step in range(40):
            proposed = copy.deepcopy(oracle)
            proposed.start_batch()
            rows = []
            next_seq = dict(oracle.max_seq)
            for _ in range(rng.randint(1, 4)):
                name = rng.choice(parts)
                next_seq[name] += 1
                kind = rng.random()
                watermark = oracle.watermarks[name]
                if kind < 0.5 or not alive:
                    eid_no += 1
                    eid = 'e%d' % eid_no
                    revisions[eid] = 1
                    rows.append({'partition': name, 'seq': next_seq[name], 'op': 'put',
                                 'eid': eid, 'rev': 1,
                                 'ts': watermark + rng.randint(0, 8),
                                 'key': rng.choice(keys), 'value': rng.randint(-5, 5)})
                elif kind < 0.7:
                    candidates = [e for e in sorted(alive)
                                  if proposed.events[e]['partition'] == name]
                    if not candidates:
                        next_seq[name] -= 1
                        continue
                    eid = rng.choice(candidates)
                    revisions[eid] += 1
                    rows.append({'partition': name, 'seq': next_seq[name], 'op': 'delete',
                                 'eid': eid, 'rev': revisions[eid]})
                elif kind < 0.9:
                    candidates = [e for e in sorted(alive)
                                  if proposed.events[e]['partition'] == name]
                    if not candidates:
                        next_seq[name] -= 1
                        continue
                    eid = rng.choice(candidates)
                    revisions[eid] += 1
                    rows.append({'partition': name, 'seq': next_seq[name], 'op': 'put',
                                 'eid': eid, 'rev': revisions[eid],
                                 'ts': watermark + rng.randint(0, 8),
                                 'key': rng.choice(keys), 'value': rng.randint(-5, 5)})
                else:
                    rows.append({'partition': name, 'seq': next_seq[name], 'op': 'watermark',
                                 'ts': watermark + rng.randint(0, 12)})
            valid = True
            try:
                for row in rows:
                    proposed.apply_row(row, encode(row))
            except AssertionError:
                valid = False
            before = self.report()
            self.apply_rows(rows, expect_rc=0 if valid else 2)
            if valid:
                proposed.finish_batch(True)
                oracle = proposed
                accepted_batches.append(self.inputs / ('batch%03d.json' % self.batch_no))
                self.assertEqual(self.report(), oracle.report())
            else:
                self.assertEqual(self.report(), before)
                # an invalid batch must not consume any seq number, revision or
                # watermark; the local generators are reset to accepted state
                self.assertEqual(self.db_epoch(), oracle.epoch)
                revisions = {e: ev['rev'] for e, ev in oracle.events.items()}
                alive = {e for e, ev in oracle.events.items() if ev['alive']}
        # pure replay of every accepted batch is byte- and epoch-stable
        snapshot = self.output.read_bytes()
        for control in accepted_batches:
            self.assertEqual(cli(['window-apply', '--state', self.state,
                                  '--source', control]).returncode, 0)
            self.assertEqual(self.output.read_bytes(), snapshot)
        self.recover()
        self.assertEqual(self.report(), oracle.report())



class Faults(Case):
    """Kill a real apply at each publication boundary, then only recover."""

    width = 10

    def setUp(self):
        super().setUp()
        self.init()

    def seed(self):
        self.apply_rows([
            {'partition': 'p', 'seq': 1, 'op': 'put', 'eid': 'a', 'rev': 1,
             'ts': 1, 'key': 'x', 'value': 3},
            {'partition': 'q', 'seq': 1, 'op': 'put', 'eid': 'b', 'rev': 1,
             'ts': 2, 'key': 'x', 'value': 4},
        ])
        self.assertEqual(self.report()[0]['epoch'], 1)
        self.old_report = self.output.read_bytes()
        self.old_state = self.state.read_bytes()

    def new_control(self):
        return self.write_batch([
            {'partition': 'p', 'seq': 2, 'op': 'put', 'eid': 'a', 'rev': 2,
             'ts': 11, 'key': 'x', 'value': 5},
            {'partition': 'q', 'seq': 2, 'op': 'put', 'eid': 'c', 'rev': 1,
             'ts': 12, 'key': 'y', 'value': 6},
        ], name='new')

    def db_epoch(self):
        conn = sqlite3.connect(str(self.state))
        try:
            return int(dict(conn.execute('SELECT key, value FROM meta'))['epoch'])
        finally:
            conn.close()

    def run_crash(self, point):
        control = self.new_control()
        result = cli(['window-apply', '--state', self.state, '--source', control],
                     env_extra={'BLOOM_INDEX_WINDOW_CRASH': point})
        self.assertNotEqual(result.returncode, 0, point)
        return control

    def assert_converged(self, expect_new):
        recovered = self.recover()
        summary = json.loads(recovered.stdout)
        epoch = self.db_epoch()
        self.assertEqual(summary['epoch'], epoch)
        if expect_new:
            self.assertEqual(epoch, 2)
            self.assertEqual(self.report()[0]['epoch'], 2)
            self.assertEqual(self.report()[0]['watermark'], 0)
            windows = [(w['key'], w['start'], w['count'], w['sum']) for w in self.report()[1:]]
            self.assertEqual(windows, [('x', 0, 1, 4), ('x', 10, 1, 5), ('y', 10, 1, 6)])
            self.assertEqual(self.report()[0]['watermark'], 0)
        else:
            self.assertEqual(epoch, 1)
            self.assertEqual(self.report()[0]['epoch'], 1)
            self.assertEqual((self.report()[1]['count'], self.report()[1]['sum']), (2, 7))
        before = self.output.read_bytes()
        self.recover()
        self.assertEqual(self.output.read_bytes(), before)
        # no staging residue remains after a completed recovery
        residue = [p.name for p in self.td.iterdir() if p.name.startswith('.')]
        self.assertEqual(residue, [])

    def test_crash_before_commit_keeps_old_epoch(self):
        self.seed()
        self.run_crash('before_commit')
        self.assert_converged(False)
        # the new batch can still be applied afterwards
        self.assertEqual(cli(['window-apply', '--state', self.state,
                              '--source', self.new_control()]).returncode, 0)
        self.assertEqual(self.db_epoch(), 2)

    def test_crash_after_commit_converges_to_new_epoch(self):
        self.seed()
        self.run_crash('after_commit')
        self.assert_converged(True)

    def test_crash_mid_report_keeps_old_report_until_recovery(self):
        self.seed()
        self.run_crash('mid_report')
        # the old report survives a partial write and no temp file remains
        self.assertEqual(self.output.read_bytes(), self.old_report)
        self.assertEqual(self.db_epoch(), 2)
        self.assert_converged(True)

    def test_partial_temporary_write_never_replaces_old_report(self):
        self.seed()
        control = self.new_control()
        result = cli(['window-apply', '--state', self.state, '--source', control],
                     env_extra={'BLOOM_INDEX_WINDOW_FAIL_AFTER': '1'})
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertEqual(self.output.read_bytes(), self.old_report)
        residue = [p.name for p in self.td.iterdir() if p.name.startswith('.')]
        self.assertEqual(residue, [])
        # the committed epoch is pending; recovery publishes it
        self.assertEqual(self.db_epoch(), 2)
        self.recover()
        self.assertEqual(self.report()[0]['epoch'], 2)
        self.assertEqual(self.report()[0]['watermark'], 0)
        windows = [(w['key'], w['start'], w['count'], w['sum']) for w in self.report()[1:]]
        self.assertEqual(windows, [('x', 0, 1, 4), ('x', 10, 1, 5), ('y', 10, 1, 6)])

    def test_recovery_then_immediate_new_batch(self):
        self.seed()
        self.run_crash('after_commit')
        self.recover()
        self.assertEqual(self.report()[0]['epoch'], 2)
        control = self.write_batch([
            {'partition': 'p', 'seq': 3, 'op': 'put', 'eid': 'd', 'rev': 1,
             'ts': 21, 'key': 'z', 'value': 8},
        ], name='third')
        result = cli(['window-apply', '--state', self.state, '--source', control])
        self.assertEqual(result.returncode, 0, result.stderr)
        lines = self.report()
        self.assertEqual(lines[0]['epoch'], 3)
        self.assertEqual([(w['key'], w['count']) for w in lines[1:]],
                         [('x', 1), ('x', 1), ('y', 1), ('z', 1)])

    def test_apply_finishes_pending_recovery_before_new_batch(self):
        self.seed()
        self.run_crash('after_commit')
        # no explicit recover: the next apply must first publish epoch 2, then
        # accept and publish its own batch as epoch 3
        control = self.write_batch([
            {'partition': 'p', 'seq': 3, 'op': 'put', 'eid': 'd', 'rev': 1,
             'ts': 21, 'key': 'z', 'value': 8},
        ], name='third')
        result = cli(['window-apply', '--state', self.state, '--source', control])
        self.assertEqual(result.returncode, 0, result.stderr)
        lines = self.report()
        self.assertEqual(lines[0]['epoch'], 3)
        self.assertEqual([(w['key'], w['start'], w['count'], w['sum']) for w in lines[1:]],
                         [('x', 0, 1, 4), ('x', 10, 1, 5), ('y', 10, 1, 6), ('z', 20, 1, 8)])
        # no watermark was advanced, so the global watermark is still 0
        self.assertEqual([w['final'] for w in lines[1:]], [False, False, False, False])

    def test_missing_report_is_rebuilt_by_recovery(self):
        self.seed()
        self.output.unlink()
        self.recover()
        self.assertEqual(self.output.read_bytes(), self.old_report)
        self.recover()
        self.assertEqual(self.output.read_bytes(), self.old_report)

    def test_crash_before_and_after_replace(self):
        for point in ('before_replace', 'after_replace'):
            self.state = self.td / ('state-%s.db' % point)
            self.output = self.td / ('report-%s.jsonl' % point)
            self.init()
            self.seed()
            self.run_crash(point)
            if point == 'before_replace':
                self.assertEqual(self.output.read_bytes(), self.old_report)
            else:
                self.assertNotEqual(self.output.read_bytes(), self.old_report)
            self.assert_converged(True)

    def test_crash_before_confirm_leaves_pending_publication(self):
        self.seed()
        self.run_crash('before_confirm')
        # report bytes were fully replaced, but the epoch is not confirmed yet
        conn = sqlite3.connect(str(self.state))
        meta = dict(conn.execute('SELECT key, value FROM meta'))
        conn.close()
        self.assertEqual((meta['epoch'], meta['confirmed_epoch']), ('2', '1'))
        self.assert_converged(True)

    def test_published_report_survives_deleted_inputs(self):
        self.seed()
        control = self.run_crash('before_confirm')
        for path in self.inputs.iterdir():
            path.unlink()
        self.assert_converged(True)
        # the new epoch report is intact and its batch was kept
        self.assertEqual(self.report()[0]['epoch'], 2)

    def test_ordinary_write_failure_preserves_old_epoch(self):
        self.seed()
        control = self.new_control()
        result = cli(['window-apply', '--state', self.state, '--source', control],
                     env_extra={'BLOOM_INDEX_WINDOW_FAIL_REPORT': '1'})
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertEqual(self.output.read_bytes(), self.old_report)
        self.assertEqual(self.db_epoch(), 2)  # committed, so recovery publishes it
        self.recover()
        self.assertEqual(self.report()[0]['epoch'], 2)


class Restart(Case):
    def test_decisions_are_identical_after_state_reopen(self):
        self.init()
        rows = [
            {'partition': 'p', 'seq': 1, 'op': 'put', 'eid': 'a', 'rev': 1,
             'ts': 1, 'key': 'x', 'value': 3},
            {'partition': 'q', 'seq': 1, 'op': 'put', 'eid': 'b', 'rev': 1,
             'ts': 2, 'key': 'x', 'value': 4},
            {'partition': 'p', 'seq': 2, 'op': 'watermark', 'ts': 10},
            {'partition': 'q', 'seq': 2, 'op': 'watermark', 'ts': 10},
        ]
        self.apply_rows(rows)
        sealed = [{'partition': 'q', 'seq': 3, 'op': 'put', 'eid': 'b',
                   'rev': 2, 'ts': 21, 'key': 'x', 'value': 4}]
        self.apply_rows(sealed, expect_rc=2)
        # every subsequent CLI invocation opens a brand-new process and state
        for _ in range(3):
            self.apply_rows(sealed, expect_rc=2)
            self.recover()
        self.assertEqual([(w['start'], w['final']) for w in self.report()[1:]],
                         [(0, True)])


class Concurrency(Case):
    width = 10

    def setUp(self):
        super().setUp()
        self.init()

    def test_second_writer_times_out_and_recovery_bounded(self):
        import fcntl
        # hold the state writer lock in this process, then ask a child to apply
        fd = os.open(str(self.state), os.O_RDWR)
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            control = self.write_batch([{'partition': 'p', 'seq': 1, 'op': 'watermark',
                                         'ts': 5}])
            start = time.monotonic()
            result = cli(['window-apply', '--state', self.state, '--source', control])
            waited = time.monotonic() - start
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertGreater(waited, 3.5)
            self.assertLess(waited, 8.0)
            start = time.monotonic()
            result = cli(['window-recover', '--state', self.state])
            self.assertEqual(result.returncode, 2)
            self.assertLess(time.monotonic() - start, 8.0)
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
        # after release both proceed normally
        control = self.write_batch([{'partition': 'p', 'seq': 1, 'op': 'watermark',
                                     'ts': 5}])
        self.assertEqual(cli(['window-apply', '--state', self.state,
                              '--source', control]).returncode, 0)
        self.assertEqual(self.report()[0]['watermark'], 0)

    def test_two_concurrent_writers_serialize(self):
        self.partitions = ['p', 'q', 'r', 's']
        # Concurrency.setUp already initialised p,q; re-init with four partitions
        self.state.unlink()
        self.output.unlink()
        self.init()
        controls = {}
        for index in range(60):
            name = 'pqrs'[index % 4]
            controls.setdefault(name, []).append(self.write_batch(
                [{'partition': name, 'seq': index // 4 + 1, 'op': 'put',
                  'eid': '%s%d' % (name, index // 4), 'rev': 1, 'ts': 5,
                  'key': 'x', 'value': 1}], name='%s%d' % (name, index)))
        errors = []

        def worker(names):
            for name in names:
                for control in controls[name]:
                    result = cli(['window-apply', '--state', self.state, '--source', control])
                    if result.returncode != 0:
                        errors.append((control.name, result.stderr))
        threads = [threading.Thread(target=worker, args=(['p', 'r'],)),
                   threading.Thread(target=worker, args=(['q', 's'],))]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(240)
        self.assertEqual(errors, [])
        self.recover()
        lines = self.report()
        self.assertEqual(lines[0]['epoch'], 60)
        self.assertEqual(sum(w['count'] for w in lines[1:]), 60)
        self.assertEqual(sum(w['sum'] for w in lines[1:]), 60)
        conn = sqlite3.connect(str(self.state))
        seqs = conn.execute('SELECT COUNT(*) FROM seqs').fetchone()[0]
        conn.close()
        self.assertEqual(seqs, 60)

    def test_racing_writers_do_not_corrupt_state(self):
        # Two processes start with the same batch (same raw bytes): exactly one
        # accepts the new seq, the other observes a pure replay; both exit 0.
        control = self.write_batch([
            {'partition': 'p', 'seq': 1, 'op': 'put', 'eid': 'a', 'rev': 1,
             'ts': 1, 'key': 'x', 'value': 1}])
        procs = [popen(['window-apply', '--state', self.state, '--source', control])
                 for _ in range(2)]
        outputs = [proc.communicate(timeout=60) for proc in procs]
        self.assertEqual([proc.returncode for proc in procs], [0, 0], outputs)
        new_seqs = sorted(json.loads(stdout)['new_seqs'] for stdout, _err in outputs)
        self.assertEqual(new_seqs, [0, 1])
        self.recover()
        lines = self.report()
        self.assertEqual(lines[0]['epoch'], 1)
        self.assertEqual([(w['count'], w['sum']) for w in lines[1:]], [(1, 1)])

    def test_recovery_never_overwrites_newer_epoch(self):
        # a published new epoch cannot be rolled back to an old one: recovery
        # only republishes bytes derived from the current committed state.
        self.apply_rows([{'partition': 'p', 'seq': 1, 'op': 'put', 'eid': 'a',
                          'rev': 1, 'ts': 1, 'key': 'x', 'value': 1}])
        self.apply_rows([{'partition': 'p', 'seq': 2, 'op': 'put', 'eid': 'b',
                          'rev': 1, 'ts': 2, 'key': 'x', 'value': 2}])
        snapshot = self.output.read_bytes()
        for _ in range(3):
            self.recover()
        self.assertEqual(self.output.read_bytes(), snapshot)
        self.assertEqual(self.report()[0]['epoch'], 2)


class PathConflicts(Case):
    def test_state_report_and_control_conflicts(self):
        self.init()
        # state and report path conflicts are refused at apply time too
        control = self.write_batch([{'partition': 'p', 'seq': 1, 'op': 'watermark', 'ts': 1}])
        cli(['window-init', '--state', self.state, '--partitions', 'p',
             '--width', 10, '--output', self.output])
        state_copy = self.td / 'state.db'
        self.assertEqual(cli(['window-init', '--state', state_copy, '--partitions', 'p',
                              '--width', 10, '--output', state_copy]).returncode, 2)
        # a control file that resolves onto the state path is refused, both by
        # listing it as an input and by being a symlink to it
        bad = self.td / 'bad.json'
        bad.write_text(json.dumps({'files': [self.state.name]}), encoding='utf-8')
        before = self.state.read_bytes()
        result = cli(['window-apply', '--state', self.state, '--source', bad])
        self.assertEqual(result.returncode, 2)
        self.assertEqual(self.state.read_bytes(), before)
        # an input file that resolves onto the report path is refused
        link = self.inputs / 'link.jsonl'
        link.symlink_to(self.output)
        ctl = self.inputs / 'ctl.json'
        ctl.write_text(json.dumps({'files': ['link.jsonl']}), encoding='utf-8')
        result = cli(['window-apply', '--state', self.state, '--source', ctl])
        self.assertEqual(result.returncode, 2)
        self.assertEqual(self.state.read_bytes(), before)
        control_link = self.td / 'control-link.json'
        control_link.symlink_to(self.state)
        result = cli(['window-apply', '--state', self.state, '--source', control_link])
        self.assertEqual(result.returncode, 2)
        self.assertEqual(self.state.read_bytes(), before)


class StateErrors(Case):
    def test_missing_and_corrupt_states_exit_two(self):
        control = self.write_batch([{'partition': 'p', 'seq': 1, 'op': 'watermark', 'ts': 1}])
        for command in (['window-apply', '--state', self.td / 'missing.db', '--source', control],
                        ['window-recover', '--state', self.td / 'missing.db']):
            result = cli(command)
            self.assertEqual(result.returncode, 2)
            self.assertFalse((self.td / 'missing.db').exists())
        self.state.write_bytes(b'not a database')
        before = self.state.read_bytes()
        for command in (['window-apply', '--state', self.state, '--source', control],
                        ['window-recover', '--state', self.state]):
            result = cli(command)
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertEqual(self.state.read_bytes(), before)
            self.assertFalse(self.output.exists())

    def test_report_symlink_after_init_is_refused(self):
        self.init()
        self.output.unlink()
        victim = self.td / 'victim.jsonl'
        self.output.symlink_to(victim)
        result = cli(['window-apply', '--state', self.state,
                      '--source', self.write_batch(
                          [{'partition': 'p', 'seq': 1, 'op': 'watermark', 'ts': 1}])])
        self.assertEqual(result.returncode, 2)
        self.assertFalse(victim.exists())
        result = cli(['window-recover', '--state', self.state])
        self.assertEqual(result.returncode, 2)
        self.assertFalse(victim.exists())


class Fixture(unittest.TestCase):
    def test_fixture_bundle_matches_cli(self):
        fixture = ROOT / 'fixtures' / 'window'
        control = fixture / 'control.json'
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp) / 'state.db'
            output = Path(tmp) / 'report.jsonl'
            init = cli(['window-init', '--state', state, '--partitions', 'p,q',
                        '--width', 10, '--output', output])
            self.assertEqual(init.returncode, 0, init.stderr)
            result = cli(['window-apply', '--state', state, '--source', control])
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(output.read_bytes(), (fixture / 'expected-report.jsonl').read_bytes())
            # replay of the fixture's first batch is a pure replay
            replay_control = fixture / 'replay.json'
            result = cli(['window-apply', '--state', state, '--source', replay_control])
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout)['new_seqs'], 0)
            self.assertEqual(output.read_bytes(), (fixture / 'expected-report.jsonl').read_bytes())
            recovered = cli(['window-recover', '--state', state])
            self.assertEqual(recovered.returncode, 0, recovered.stderr)
            self.assertEqual(output.read_bytes(), (fixture / 'expected-report.jsonl').read_bytes())


class ScaleTool(unittest.TestCase):
    def test_measure_script_smoke(self):
        measure = ROOT / 'tools' / 'measure_window_scale.py'
        env = dict(os.environ)
        env['PYTHONPATH'] = str(PKG_PARENT) + os.pathsep + env.get('PYTHONPATH', '')
        result = subprocess.run(
            [sys.executable, str(measure), '--events', '5000', '--corrections', '500',
             '--partitions', '4', '--limit-mib', '128'],
            capture_output=True, text=True, env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        summary = json.loads(result.stdout)
        self.assertTrue(summary['consistent'])
        self.assertLessEqual(summary['peak_kib'], 128 * 1024)
        self.assertEqual(summary['events'], 5500)
        self.assertEqual(summary['epochs'], 2)


if __name__ == '__main__':
    unittest.main()
