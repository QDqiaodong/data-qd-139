"""Acceptance tests for ``bloom-index resolve`` (record resolution v4).

The merge semantics are independently reimplemented as a small in-memory naive
oracle and compared line-by-line against the CLI's streamed JSONL output on
random inputs. All other tests pin the contract details from the brief:
strict parsing, snapshot-pinned exact ownership, Bloom prefiltering with exact
verification, constrained greedy merge feedback, deterministic bytes and
bounded on-disk workspace behavior.
"""
import hashlib
import json
import os
import random
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unicodedata
import unittest
from pathlib import Path

import bloom_index

ROOT = Path(__file__).resolve().parents[1]
PKG_PARENT = Path(bloom_index.__file__).resolve().parents[1]
FIXTURE = ROOT / 'fixtures' / 'resolve'
MEASURE_RESOLVE = ROOT / 'tools' / 'measure_resolve_scale.py'


def cli(args, env_extra=None):
    env = dict(os.environ)
    env['PYTHONPATH'] = str(PKG_PARENT) + os.pathsep + env.get('PYTHONPATH', '')
    if env_extra:
        env.update(env_extra)
    return subprocess.run([sys.executable, '-m', 'bloom_index.cli', *map(str, args)],
                          capture_output=True, text=True, env=env)


def write_manifest(path, items):
    with path.open('w', encoding='utf-8') as stream:
        for item in items:
            stream.write(json.dumps({'item': item}, ensure_ascii=False) + '\n')
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sync_source(db, source, items, td, generation=1, base=0):
    manifest = td / f'own_{source}_{generation}.jsonl'
    digest = write_manifest(manifest, items)
    control = td / f'sync_{source}_{generation}.json'
    control.write_text(json.dumps({'updates': [
        {'source': source, 'generation': generation, 'base_generation': base,
         'manifest': manifest.name, 'sha256': digest}]}, ensure_ascii=False), encoding='utf-8')
    result = cli(['sync', '--db', db, '--source', control])
    assert result.returncode == 0, result.stderr


def write_master(path, rows):
    with path.open('w', encoding='utf-8') as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False) + '\n')


def read_result(path):
    lines = path.read_text(encoding='utf-8').splitlines()
    return [json.loads(line) for line in lines]


def canon(alias):
    return unicodedata.normalize('NFKC', alias).casefold()


class Case(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.td = Path(self.tmp.name)
        self.db = self.td / 'idx.db'
        self.bits, self.hashes = 256, 4
        self.assertEqual(cli(['init', '--db', self.db, '--bits', self.bits,
                              '--hashes', self.hashes]).returncode, 0)

    def tearDown(self):
        self.tmp.cleanup()

    def resolve(self, files, expect_rc=0, env_extra=None):
        control = self.td / 'control.json'
        control.write_text(json.dumps({'files': [str(p) for p in files]}), encoding='utf-8')
        out = self.td / 'result.jsonl'
        result = cli(['resolve', '--db', self.db, '--source', control,
                      '--output', out], env_extra=env_extra)
        self.assertEqual(result.returncode, expect_rc, result.stderr)
        if expect_rc == 0:
            return read_result(out), result
        return result

    def ownership(self, mapping):
        for source, items in mapping.items():
            sync_source(self.db, source, items, self.td)


class StrictControl(Case):
    def test_control_field_and_path_rules(self):
        good = self.td / 'm.jsonl'
        write_master(good, [])
        bad_controls = [
            b'{}', b'{"files":[]}', b'{"files":["x"],"extra":1}',
            b'{"files":"m.jsonl"}', b'{"files":[1]}', b'{"files":[""]}',
            json.dumps({'files': ['m.jsonl'] * 65}).encode(),
            b'{"files":["m.jsonl","./m.jsonl"]}',
            b'{"files":null}',
        ]
        for raw in bad_controls:
            ctl = self.td / 'bad.json'
            ctl.write_bytes(raw)
            out = self.td / 'r.jsonl'
            self.assertEqual(cli(['resolve', '--db', self.db, '--source', ctl,
                                  '--output', out]).returncode, 2, raw)
            self.assertFalse(out.exists())

    def test_missing_input_and_output_conflicts(self):
        ctl = self.td / 'c.json'
        ctl.write_text(json.dumps({'files': ['missing.jsonl']}))
        self.assertEqual(cli(['resolve', '--db', self.db, '--source', ctl,
                              '--output', self.td / 'r.jsonl']).returncode, 2)
        master = self.td / 'm.jsonl'
        write_master(master, [{'rid': 'a', 'source': 'S', 'anchor': None, 'aliases': ['x']}])
        for output in (self.db, ctl, master):
            self.assertEqual(cli(['resolve', '--db', self.db, '--source', ctl,
                                  '--output', output]).returncode, 2)

    def test_db_and_output_symlinks_refused(self):
        dblink = self.td / 'dlink.db'
        dblink.symlink_to(self.db)
        master = self.td / 'm.jsonl'
        write_master(master, [{'rid': 'a', 'source': 'S', 'anchor': None, 'aliases': ['x']}])
        ctl = self.td / 'c.json'
        ctl.write_text(json.dumps({'files': [str(master)]}))
        self.assertEqual(cli(['resolve', '--db', dblink, '--source', ctl,
                              '--output', self.td / 'r.jsonl']).returncode, 2)
        outlink = self.td / 'olink.jsonl'
        outlink.symlink_to(self.td / 'victim')
        self.assertEqual(cli(['resolve', '--db', self.db, '--source', ctl,
                              '--output', outlink]).returncode, 2)
        self.assertFalse((self.td / 'victim').exists())

    def test_line_rules_report_file_and_line(self):
        self.ownership({'S': ['ok']})
        good = {'rid': 'ok', 'source': 'S', 'anchor': None, 'aliases': ['x']}
        bodies = [
            b'\n',
            b'{"rid":"a","source":"S","anchor":null,"aliases":["x"]}\n\n',
            b'{"rid":"a","rid":"b","source":"S","anchor":null,"aliases":["x"]}\n',
            b'{"rid":"a","source":"S","anchor":null,"aliases":["x"],"z":1}\n',
            b'{"rid":"a","source":"S","anchor":null,"aliases":[NaN]}\n',
            b'{"rid":7,"source":"S","anchor":null,"aliases":["x"]}\n',
            b'{"rid":"a","source":"S","anchor":1,"aliases":["x"]}\n',
            b'{"rid":"a","source":"S","anchor":null,"aliases":[]}\n',
            b'{"rid":"a","source":"S","anchor":null,"aliases":["x","y","z","q","w","e","r","t","u"]}\n',
            b'{"rid":" a","source":"S","anchor":null,"aliases":["x"]}\n',
            b'{"rid":"a","source":" S","anchor":null,"aliases":["x"]}\n',
            b'{"rid":"a","source":"S","anchor":null,"aliases":[""]}\n',
            b'{"rid":"a","source":"S","anchor":null,"aliases":["\xff"]}\n',
        ]
        long_line = (json.dumps(good)[:-2] + 'x' * 16384 + '"]}').encode() + b'\n'
        bodies.append(long_line)
        for body in bodies:
            path = self.td / 'bad.jsonl'
            path.write_bytes(b'')
            path.write_bytes(body)
            ctl = self.td / 'c.json'
            ctl.write_text(json.dumps({'files': [str(path)]}))
            result = cli(['resolve', '--db', self.db, '--source', ctl,
                          '--output', self.td / 'r.jsonl'])
            self.assertEqual(result.returncode, 2, body[:30])
            self.assertIn('bad.jsonl', result.stderr)
            self.assertRegex(result.stderr, r':\d+')

    def test_line_byte_boundary(self):
        self.ownership({'S': ['rid']})
        line = b'{"rid":"rid","source":"S","anchor":null,"aliases":["x"]}'
        pad = 16384 - len(line)
        path = self.td / 'edge.jsonl'
        path.write_bytes(line + b' ' * pad + b'\n')
        rows, _ = self.resolve([path])
        self.assertEqual(rows[0]['total'], 1)

    def test_record_limit(self):
        self.ownership({})
        path = self.td / 'big.jsonl'
        with path.open('w', encoding='utf-8') as stream:
            for i in range(1_000_001):
                stream.write(json.dumps({'rid': f'r{i}', 'source': 'S',
                                         'anchor': None, 'aliases': ['x']}) + '\n')
        result = self.resolve([path], expect_rc=2)
        self.assertIn('1000000', result.stderr)

    def test_cross_file_duplicate_rid_rejects_whole_batch(self):
        self.ownership({'S': ['a', 'b']})
        f1 = self.td / 'f1.jsonl'
        f2 = self.td / 'f2.jsonl'
        write_master(f1, [{'rid': 'a', 'source': 'S', 'anchor': None, 'aliases': ['x']}])
        write_master(f2, [
            {'rid': 'b', 'source': 'S', 'anchor': None, 'aliases': ['y']},
            {'rid': 'a', 'source': 'S', 'anchor': None, 'aliases': ['z']}])
        result = self.resolve([f1, f2], expect_rc=2)
        self.assertIn('f2.jsonl', result.stderr)
        self.assertIn('2', result.stderr)
        self.assertFalse((self.td / 'result.jsonl').exists())


class BloomContract(Case):
    def test_local_position_helper_matches_filtering_contract(self):
        from bloom_index.filtering import positions
        from bloom_index.resolve import _position_prefixes, _positions_of
        samples = ['a', 'Street', 'ＳＴＲＥＥＴ', 'café', 'x', 'common-000', 'Ｚ']
        for bits, hashes in ((1, 16), (256, 4), (1_000_000, 16), (7, 3)):
            prefixes = _position_prefixes(hashes)
            for alias in samples:
                self.assertEqual(
                    _positions_of(alias.encode('utf-8'), prefixes, bits),
                    positions(alias, bits, hashes))


class Normalization(Case):
    def test_ident_then_nfkc_casefold_and_in_record_folding(self):
        self.ownership({'S': ['r1', 'r2', 'r3']})
        path = self.td / 'm.jsonl'
        write_master(path, [
            {'rid': 'r1', 'source': 'S', 'anchor': None, 'aliases': ['Street', 'ＳＴＲＥＥＴ', 'street']},
            {'rid': 'r2', 'source': 'S', 'anchor': None, 'aliases': ['STREET']},
            {'rid': 'r3', 'source': 'S', 'anchor': None, 'aliases': ['other']},
        ])
        rows, _ = self.resolve([path])
        # r1/r2 share the canonical alias but also source S: they stay split
        # with source evidence; r3 is isolated.
        entities = {e['entity']: e for e in rows[1:]}
        self.assertEqual(set(entities), {'r1', 'r2', 'r3'})
        self.assertEqual(entities['r1']['blocked_with']['entity'], 'r2')
        self.assertEqual(entities['r1']['blocked_with']['kind'], 'source')
        self.assertIsNone(entities['r3']['blocked_with'])


class Selection(Case):
    def test_exact_ownership_marks_inactive(self):
        # q is owned by A, but its master record claims source B -> inactive
        self.ownership({'A': ['a', 'q'], 'B': ['b']})
        path = self.td / 'm.jsonl'
        write_master(path, [
            {'rid': 'a', 'source': 'A', 'anchor': None, 'aliases': ['x']},
            {'rid': 'b', 'source': 'B', 'anchor': None, 'aliases': ['x']},
            {'rid': 'q', 'source': 'B', 'anchor': None, 'aliases': ['x']},
        ])
        rows, _ = self.resolve([path])
        summary = rows[0]
        self.assertEqual((summary['total'], summary['active'],
                          summary['inactive'], summary['entities']), (3, 2, 1, 1))
        active_rids = [m['rid'] for e in rows[1:] for m in e['members']]
        self.assertEqual(sorted(active_rids), ['a', 'b'])

    def test_cleared_identity_excluded_then_restored(self):
        sync_source(self.db, 'S', ['a', 'b'], self.td, 1, 0)
        path = self.td / 'm.jsonl'
        write_master(path, [
            {'rid': 'a', 'source': 'S', 'anchor': None, 'aliases': ['x']},
            {'rid': 'b', 'source': 'S', 'anchor': None, 'aliases': ['x']}])
        rows, _ = self.resolve([path])
        self.assertEqual(rows[0]['active'], 2)
        sync_source(self.db, 'S', [], self.td, 2, 1)  # cleared by sync
        rows, _ = self.resolve([path])
        self.assertEqual((rows[0]['active'], rows[0]['inactive'], rows[0]['entities']), (0, 2, 0))
        sync_source(self.db, 'S', ['a', 'b'], self.td, 3, 2)  # restored
        rows, _ = self.resolve([path])
        self.assertEqual(rows[0]['active'], 2)

    def test_inactive_record_does_not_mask_bad_lines(self):
        sync_source(self.db, 'S', [], self.td, 1, 0)  # nothing owned -> all inactive
        path = self.td / 'm.jsonl'
        path.write_bytes(
            json.dumps({'rid': 'a', 'source': 'S', 'anchor': None, 'aliases': ['x']}).encode()
            + b'\nNOT JSON\n')
        result = self.resolve([path], expect_rc=2)
        self.assertIn('m.jsonl:2', result.stderr)

    def test_bloom_positive_alone_never_creates_candidate(self):
        # bits=1/hashes=16 forces every alias to the single bit: only exact
        # alias equality may link groups.
        self.db2 = self.td / 'onebit.db'
        self.assertEqual(cli(['init', '--db', self.db2, '--bits', 1,
                              '--hashes', 16]).returncode, 0)
        sources = {f'S{i}': [f'r{i}'] for i in range(6)}
        for source, items in sources.items():
            sync_source(self.db2, source, items, self.td)
        path = self.td / 'm.jsonl'
        write_master(path, [
            {'rid': 'r0', 'source': 'S0', 'anchor': None, 'aliases': ['alpha']},
            {'rid': 'r1', 'source': 'S1', 'anchor': None, 'aliases': ['beta']},
            {'rid': 'r2', 'source': 'S2', 'anchor': None, 'aliases': ['gamma']},
            {'rid': 'r3', 'source': 'S3', 'anchor': None, 'aliases': ['delta']},
            {'rid': 'r4', 'source': 'S4', 'anchor': None, 'aliases': ['epsilon', 'alpha']},
            {'rid': 'r5', 'source': 'S5', 'anchor': None, 'aliases': ['zeta', 'alpha']},
        ])
        ctl = self.td / 'c.json'
        ctl.write_text(json.dumps({'files': [str(path)]}))
        out = self.td / 'r.jsonl'
        self.assertEqual(cli(['resolve', '--db', self.db2, '--source', ctl,
                              '--output', out]).returncode, 0)
        rows = read_result(out)
        # only r0/r4/r5 share exact alias alpha -> one group; r1,r2,r3 isolated
        groups = {e['entity']: sorted(m['rid'] for m in e['members']) for e in rows[1:]}
        self.assertEqual(groups['r0'], ['r0', 'r4', 'r5'])
        for rid in ('r1', 'r2', 'r3'):
            self.assertEqual(groups[rid], [rid])
        self.assertIsNone(next(e for e in rows[1:] if e['entity'] == 'r0')['blocked_with'])


class ConstrainedMerge(Case):
    def test_linked_acceptance_pattern_shuffled(self):
        self.ownership({'A': ['a'], 'B': ['b'], 'C': ['c'], 'D': ['d']})
        rows_spec = [
            {'rid': 'a', 'source': 'A', 'anchor': 'L', 'aliases': ['x']},
            {'rid': 'b', 'source': 'B', 'anchor': 'R', 'aliases': ['y']},
            {'rid': 'c', 'source': 'C', 'anchor': None, 'aliases': ['y', 'z']},
            {'rid': 'd', 'source': 'D', 'anchor': None, 'aliases': ['x', 'z']},
        ]
        orderings = [
            (['f1.jsonl'], [rows_spec]),
            (['f1.jsonl', 'f2.jsonl'], [rows_spec[2:], rows_spec[:2]]),
            (['f1.jsonl', 'f2.jsonl'], [list(reversed(rows_spec[2:])),
                                        list(reversed(rows_spec[:2]))]),
        ]
        canonical_bytes = None
        for names, chunks in orderings:
            paths = []
            for name, chunk in zip(names, chunks):
                p = self.td / name
                write_master(p, chunk)
                paths.append(p)
            rows, _ = self.resolve(paths)
            partition = {e['entity']: sorted(m['rid'] for m in e['members']) for e in rows[1:]}
            self.assertEqual(partition, {'a': ['a', 'c', 'd'], 'b': ['b']})
            data = (self.td / 'result.jsonl').read_bytes()
            if canonical_bytes is None:
                canonical_bytes = data
            else:
                self.assertEqual(data, canonical_bytes)
        blocked = {e['entity']: e['blocked_with'] for e in rows[1:]}
        self.assertEqual(blocked['a'], {'entity': 'b', 'kind': 'anchor', 'value': ['L', 'R']})
        self.assertEqual(blocked['b'], {'entity': 'a', 'kind': 'anchor', 'value': ['L', 'R']})

    def test_same_source_never_merges_even_shared_anchor(self):
        sync_source(self.db, 'S', ['a', 'b'], self.td, 1, 0)
        path = self.td / 'm.jsonl'
        write_master(path, [
            {'rid': 'a', 'source': 'S', 'anchor': 'L', 'aliases': ['x']},
            {'rid': 'b', 'source': 'S', 'anchor': 'L', 'aliases': ['x']}])
        rows, _ = self.resolve([path])
        entities = {e['entity']: e for e in rows[1:]}
        self.assertEqual(set(entities), {'a', 'b'})
        self.assertEqual(entities['a']['blocked_with'],
                         {'entity': 'b', 'kind': 'source', 'value': 'S'})

    def test_different_sources_same_anchor_merge(self):
        self.ownership({'A': ['a'], 'B': ['b']})
        path = self.td / 'm.jsonl'
        write_master(path, [
            {'rid': 'a', 'source': 'A', 'anchor': 'L', 'aliases': ['x']},
            {'rid': 'b', 'source': 'B', 'anchor': 'L', 'aliases': ['x']}])
        rows, _ = self.resolve([path])
        self.assertEqual(rows[0]['entities'], 1)
        self.assertEqual(rows[1]['anchor'], 'L')

    def test_null_anchor_compatible_two_distinct_anchors_isolated(self):
        self.ownership({'A': ['a', 'b', 'c'], 'B': ['d', 'e', 'f']})
        path = self.td / 'm.jsonl'
        write_master(path, [
            {'rid': 'a', 'source': 'A', 'anchor': 'L', 'aliases': ['x']},
            {'rid': 'b', 'source': 'A', 'anchor': None, 'aliases': ['x']},
            {'rid': 'c', 'source': 'A', 'anchor': 'R', 'aliases': ['x']},
            {'rid': 'd', 'source': 'B', 'anchor': 'L', 'aliases': ['x']},
            {'rid': 'e', 'source': 'B', 'anchor': None, 'aliases': ['x']},
            {'rid': 'f', 'source': 'B', 'anchor': 'R', 'aliases': ['x']},
        ])
        rows, _ = self.resolve([path])
        groups = {e['entity']: sorted(m['rid'] for m in e['members']) for e in rows[1:]}
        # L side: a(A)+d(B) merge; null side: b(A)+e(B) merge; R side: c(A)+f(B) merge
        self.assertEqual(groups, {'a': ['a', 'd'], 'b': ['b', 'e'], 'c': ['c', 'f']})
        # every group shares alias x with the other two final groups -> evidence
        entities = {e['entity']: e for e in rows[1:]}
        for gid in ('a', 'b', 'c'):
            self.assertIsNotNone(entities[gid]['blocked_with'])
        # anchor rule wins whenever both sides carry distinct non-null anchors;
        # the smallest other group for a is b (null), which blocks on source,
        # while c (the smallest non-null mismatch) gives the anchor evidence
        self.assertEqual(entities['a']['blocked_with'],
                         {'entity': 'b', 'kind': 'source', 'value': 'A'})
        self.assertEqual(entities['c']['blocked_with'],
                         {'entity': 'a', 'kind': 'anchor', 'value': ['L', 'R']})
        self.assertEqual(entities['b']['blocked_with'],
                         {'entity': 'a', 'kind': 'source', 'value': 'A'})

    def test_multi_alias_ring_and_same_source_conflict(self):
        self.ownership({f'S{i}': [f'r{i}'] for i in range(5)})
        sync_source(self.db, 'S0', ['r0', 'r4', 'x0'], self.td, 2, 1)
        path = self.td / 'm.jsonl'
        write_master(path, [
            {'rid': 'r0', 'source': 'S0', 'anchor': None, 'aliases': ['a', 'b']},
            {'rid': 'r1', 'source': 'S1', 'anchor': None, 'aliases': ['b', 'c']},
            {'rid': 'r2', 'source': 'S2', 'anchor': None, 'aliases': ['c', 'd']},
            {'rid': 'r3', 'source': 'S3', 'anchor': None, 'aliases': ['d', 'a']},
            {'rid': 'r4', 'source': 'S0', 'anchor': None, 'aliases': ['a']},  # source clash with r0
        ])
        rows, _ = self.resolve([path])
        groups = {e['entity']: sorted(m['rid'] for m in e['members']) for e in rows[1:]}
        # r0..r3 form an alias ring and merge greedily into r0's group;
        # r4 shares alias a but source S0 is already in that group -> blocked.
        self.assertEqual(groups['r0'], ['r0', 'r1', 'r2', 'r3'])
        self.assertEqual(groups['r4'], ['r4'])
        entities = {e['entity']: e for e in rows[1:]}
        self.assertEqual(entities['r4']['blocked_with'],
                         {'entity': 'r0', 'kind': 'source', 'value': 'S0'})


class SnapshotLinkage(Case):
    def test_concurrent_sync_visible_only_on_next_run(self):
        self.ownership({'A': ['a'], 'B': ['b'], 'C': ['c'], 'D': ['d']})
        path = self.td / 'm.jsonl'
        write_master(path, [
            {'rid': 'a', 'source': 'A', 'anchor': 'L', 'aliases': ['x']},
            {'rid': 'b', 'source': 'B', 'anchor': 'R', 'aliases': ['y']},
            {'rid': 'c', 'source': 'C', 'anchor': None, 'aliases': ['y', 'z']},
            {'rid': 'd', 'source': 'D', 'anchor': None, 'aliases': ['x', 'z']},
        ])
        ctl = self.td / 'c.json'
        ctl.write_text(json.dumps({'files': [str(path)]}))
        out = self.td / 'r.jsonl'
        initial_seq = sqlite3.connect(str(self.db)).execute(
            "SELECT value FROM meta WHERE key='commit_seq'").fetchone()[0]

        def delete_d():
            # Wait until resolve has pinned its snapshot and is about to read
            # the first master record, then commit D's ownership removal.
            deadline = time.monotonic() + 20
            gate = self.td / 'gate'
            while not gate.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(gate.exists())
            sync_source(self.db, 'D', [], self.td, 2, 1)
            gate.unlink()  # release resolve; its pinned snapshot is unchanged

        thread = threading.Thread(target=delete_d)
        thread.start()
        env = {'BLOOM_INDEX_RESOLVE_GATE': str(self.td / 'gate')}
        result = cli(['resolve', '--db', self.db, '--source', ctl, '--output', out], env)
        thread.join(25)
        self.assertEqual(result.returncode, 0, result.stderr)
        rows = read_result(out)
        # the pinned snapshot predates the concurrent sync: old commit_seq
        self.assertEqual(rows[0]['commit_seq'], initial_seq)
        self.assertEqual((rows[0]['active'], rows[0]['inactive']), (4, 0))
        partition = {e['entity']: sorted(m['rid'] for m in e['members']) for e in rows[1:]}
        self.assertEqual(partition, {'a': ['a', 'c', 'd'], 'b': ['b']})
        # next run against the same master data sees the new committed state
        rows, _ = self.resolve([path])
        self.assertEqual(rows[0]['commit_seq'], initial_seq + 1)
        self.assertEqual((rows[0]['active'], rows[0]['inactive']), (3, 1))
        partition = {e['entity']: sorted(m['rid'] for m in e['members']) for e in rows[1:]}
        self.assertEqual(partition, {'a': ['a'], 'b': ['b', 'c']})
        # restore D's ownership -> original four-way partition returns
        sync_source(self.db, 'D', ['d'], self.td, 3, 2)
        rows, _ = self.resolve([path])
        self.assertEqual(rows[0]['commit_seq'], initial_seq + 2)
        self.assertEqual(rows[0]['active'], 4)
        partition = {e['entity']: sorted(m['rid'] for m in e['members']) for e in rows[1:]}
        self.assertEqual(partition, {'a': ['a', 'c', 'd'], 'b': ['b']})

    def test_database_opened_read_only(self):
        self.ownership({'A': ['a']})
        path = self.td / 'm.jsonl'
        write_master(path, [{'rid': 'a', 'source': 'A', 'anchor': None, 'aliases': ['x']}])
        before = self.db.read_bytes()
        self.resolve([path])
        # resolve must not create persistent tables or grow the member DB beyond WAL noise
        conn = sqlite3.connect(str(self.db))
        objects = {row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'")}
        conn.close()
        self.assertEqual(objects, {'meta', 'sources', 'generations', 'ownership',
                                   'ownership_by_item', 'counters'})
        self.assertEqual(self.db.read_bytes(), before)

    def test_empty_batch_and_all_inactive_still_emit_summary(self):
        sync_source(self.db, 'S', ['x'], self.td, 1, 0)
        empty = self.td / 'empty.jsonl'
        empty.write_bytes(b'')
        rows, result = self.resolve([empty])
        self.assertEqual(rows, [{'type': 'summary', 'commit_seq': 1, 'total': 0,
                                 'active': 0, 'inactive': 0, 'entities': 0}])
        path = self.td / 'm.jsonl'
        write_master(path, [{'rid': 'a', 'source': 'S', 'anchor': None, 'aliases': ['q']}])
        sync_source(self.db, 'S', [], self.td, 2, 1)
        rows, _ = self.resolve([path])
        self.assertEqual(rows[0]['entities'], 0)
        self.assertEqual((rows[0]['active'], rows[0]['inactive']), (0, 1))
        self.assertEqual(len(rows), 1)


class OutputContract(Case):
    def test_deterministic_bytes_sorted_keys_and_newlines(self):
        self.ownership({'A': ['a', 'c'], 'B': ['b', 'd']})
        path = self.td / 'm.jsonl'
        write_master(path, [
            {'rid': 'a', 'source': 'A', 'anchor': 'Ｌ', 'aliases': ['x']},
            {'rid': 'b', 'source': 'B', 'anchor': 'R', 'aliases': ['y']},
            {'rid': 'c', 'source': 'A', 'anchor': None, 'aliases': ['y', 'z']},
            {'rid': 'd', 'source': 'B', 'anchor': None, 'aliases': ['x', 'z']},
        ])
        self.resolve([path])
        raw = (self.td / 'result.jsonl').read_bytes()
        self.assertTrue(raw.endswith(b'\n'))
        for line in raw.splitlines():
            self.assertNotIn(b', ', line)
            self.assertNotIn(b': ', line)
            obj = json.loads(line)
            self.assertEqual(line, json.dumps(obj, sort_keys=True, ensure_ascii=False,
                                              separators=(',', ':')).encode())
        # every active rid appears exactly once; counts add up
        rows = read_result(self.td / 'result.jsonl')
        seen = [m['rid'] for e in rows[1:] for m in e['members']]
        self.assertEqual(sorted(seen), ['a', 'b', 'c', 'd'])
        self.assertEqual(rows[0]['total'], rows[0]['active'] + rows[0]['inactive'])
        # rerun byte-identical
        first = raw
        self.resolve([path])
        self.assertEqual((self.td / 'result.jsonl').read_bytes(), first)

    def test_failures_keep_old_output_and_remove_workspace(self):
        self.ownership({'S': ['a', 'b']})
        out = self.td / 'result.jsonl'
        good = self.td / 'good.jsonl'
        write_master(good, [{'rid': 'a', 'source': 'S', 'anchor': None, 'aliases': ['x']}])
        self.resolve([good])
        prior = out.read_bytes()
        leftovers = []

        def no_residue():
            leftovers[:] = [p.name for p in self.td.iterdir()
                            if p.name.startswith('.resolve-work-')]
            self.assertEqual(leftovers, [])

        no_residue()
        bad = self.td / 'bad.jsonl'
        bad.write_bytes(b'not json\n')
        self.assertEqual(self.resolve([bad], expect_rc=2).returncode, 2)
        self.assertEqual(out.read_bytes(), prior)
        no_residue()
        # injected workspace failure after a full successful validation+merge
        tworec = self.td / 'two.jsonl'
        write_master(tworec, [
            {'rid': 'a', 'source': 'S', 'anchor': None, 'aliases': ['x']},
            {'rid': 'b', 'source': 'S', 'anchor': None, 'aliases': ['y']}])
        result = self.resolve([tworec], expect_rc=2,
                              env_extra={'BLOOM_INDEX_RESOLVE_FAIL_WORK': '1'})
        self.assertEqual(result.returncode, 2)
        self.assertEqual(out.read_bytes(), prior)
        no_residue()
        result = self.resolve([tworec], expect_rc=2,
                              env_extra={'BLOOM_INDEX_RESOLVE_FAIL_OUTPUT': '1'})
        self.assertEqual(result.returncode, 2)
        self.assertEqual(out.read_bytes(), prior)
        no_residue()
        # retry without injected failures republishes successfully
        rows, _ = self.resolve([tworec])
        self.assertEqual(rows[0]['total'], 2)
        self.assertNotEqual(out.read_bytes(), prior)
        no_residue()


class NaiveOracle:
    """Independent straightforward implementation of the full merge contract."""

    def __init__(self, records, ownership, commit_seq):
        self.commit_seq = commit_seq
        self.total = len(records)
        self.active = [r for r in records if (r['source'], r['rid']) in ownership]
        self.inactive = self.total - len(self.active)
        self.groups = {
            r['rid']: {'members': [r['rid']], 'sources': {r['source']},
                       'anchor': r['anchor'],
                       'aliases': {canon(a) for a in r['aliases']}}
            for r in self.active}
        self.record_source = {r['rid']: r['source'] for r in records}

    def mergeable(self, g1, g2):
        if self.groups[g1]['sources'] & self.groups[g2]['sources']:
            return False
        a1, a2 = self.groups[g1]['anchor'], self.groups[g2]['anchor']
        return a1 is None or a2 is None or a1 == a2

    def run(self):
        while True:
            pair = None
            ids = sorted(self.groups)
            for i, g1 in enumerate(ids):
                for g2 in ids[i + 1:]:
                    if not (self.groups[g1]['aliases'] & self.groups[g2]['aliases']):
                        continue
                    if self.mergeable(g1, g2):
                        pair = (g1, g2)
                        break
                if pair:
                    break
            if pair is None:
                break
            keep, drop = pair
            self.groups[keep]['members'] += self.groups[drop]['members']
            self.groups[keep]['sources'] |= self.groups[drop]['sources']
            self.groups[keep]['aliases'] |= self.groups[drop]['aliases']
            if self.groups[keep]['anchor'] is None:
                self.groups[keep]['anchor'] = self.groups[drop]['anchor']
            del self.groups[drop]
        return self

    def evidence(self, gid):
        group = self.groups[gid]
        others = sorted(oid for oid, other in self.groups.items()
                        if oid != gid and other['aliases'] & group['aliases'])
        if not others:
            return None
        other_id = others[0]
        other = self.groups[other_id]
        if group['anchor'] is not None and other['anchor'] is not None \
                and group['anchor'] != other['anchor']:
            return {'entity': other_id, 'kind': 'anchor',
                    'value': sorted((group['anchor'], other['anchor']))}
        shared = min(group['sources'] & other['sources'])
        return {'entity': other_id, 'kind': 'source', 'value': shared}

    def expected_lines(self):
        lines = [{'type': 'summary', 'commit_seq': self.commit_seq, 'total': self.total,
                  'active': len(self.active), 'inactive': self.inactive,
                  'entities': len(self.groups)}]
        for gid in sorted(self.groups):
            group = self.groups[gid]
            members = sorted(({'rid': rid, 'source': self.record_source[rid]}
                              for rid in group['members']), key=lambda m: m['rid'])
            lines.append({'type': 'entity', 'entity': gid, 'members': members,
                          'anchor': group['anchor'], 'blocked_with': self.evidence(gid)})
        return lines


class OracleDriven(Case):
    def test_random_inputs_line_by_line(self):
        rng = random.Random(20261116)
        source_names = ['A', 'B', 'C', 'D', 'E']
        alias_vocab = ['x', 'y', 'z', 'w', 'v', 'u', 't', 's', 'r', 'q']
        anchor_vocab = [None, None, None, 'L', 'R', 'K']
        for trial in range(12):
            self.tearDown()  # release the unittest-created directory first
            self.setUp()
            try:
                n = rng.randint(1, 26)
                rids = [f'rid{i:02d}' for i in range(n)]
                records = []
                ownership = set()
                for rid in rids:
                    source = rng.choice(source_names)
                    anchor = rng.choice(anchor_vocab)
                    aliases = rng.sample(alias_vocab, rng.randint(1, 4))
                    records.append({'rid': rid, 'source': source,
                                    'anchor': anchor, 'aliases': aliases})
                    if rng.random() < 0.8:
                        ownership.add((source, rid))
                mapping = {}
                for source, item in ownership:
                    mapping.setdefault(source, []).append(item)
                self.ownership(mapping)
                # split records across 1..3 files, sometimes shuffled, maybe an empty file
                shuffled = records[:]
                rng.shuffle(shuffled)
                cuts = sorted(rng.sample(range(1, n), min(2, n - 1))) if n > 1 else []
                chunks = [shuffled[0:cuts[0]] if cuts else shuffled]
                if cuts:
                    chunks = [shuffled[:cuts[0]], shuffled[cuts[0]:cuts[1]] if len(cuts) > 1
                              else shuffled[cuts[0]:]]
                    if len(cuts) > 1:
                        chunks.append(shuffled[cuts[1]:])
                paths = []
                for idx, chunk in enumerate(chunks):
                    p = self.td / f'm{trial}_{idx}.jsonl'
                    write_master(p, chunk)
                    paths.append(p)
                if rng.random() < 0.3:
                    empty = self.td / f'm{trial}_empty.jsonl'
                    empty.write_bytes(b'')
                    paths.insert(rng.randrange(len(paths) + 1), empty)
                rows, _ = self.resolve(paths)
                commit_seq = rows[0]['commit_seq']
                oracle = NaiveOracle(records, ownership, commit_seq).run()
                self.assertEqual(rows, oracle.expected_lines())
            finally:
                self.tearDown()


class FixtureContract(Case):
    def test_committed_fixture_matches_cli(self):
        db = FIXTURE / 'membership.sqlite3'
        control = FIXTURE / 'control.json'
        out = self.td / 'out.jsonl'
        result = cli(['resolve', '--db', db, '--source', control, '--output', out])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(out.read_bytes(), (FIXTURE / 'expected-result.jsonl').read_bytes())
        summary = json.loads(out.read_text().splitlines()[0])
        self.assertEqual(summary, {'active': 8, 'commit_seq': 1, 'entities': 6,
                                   'inactive': 1, 'total': 9, 'type': 'summary'})


class ScaleTool(unittest.TestCase):
    def test_star_mode_smoke_bounded_and_one_entity(self):
        env = dict(os.environ)
        env['PYTHONPATH'] = str(PKG_PARENT) + os.pathsep + env.get('PYTHONPATH', '')
        result = subprocess.run(
            [sys.executable, str(MEASURE_RESOLVE), '--mode', 'star', '--records', '2000',
             '--bits', '50000', '--hashes', '8', '--limit-mib', '128'],
            capture_output=True, text=True, env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        summary = json.loads(result.stdout)
        self.assertEqual(summary['entities'], 1)
        self.assertEqual(summary['merges'], 1999)
        self.assertFalse(summary['pair_table_materialized'])
        # raw pair count is quadratic, every workspace table stays linear
        self.assertEqual(summary['raw_record_pairs'], 2000 * 1999 // 2)
        self.assertLessEqual(summary['max_workspace_table_rows'], 2000 * 8 + 4000)
        self.assertLessEqual(summary['peak_kib'], 128 * 1024)

    def test_scale_mode_smoke_bounded(self):
        env = dict(os.environ)
        env['PYTHONPATH'] = str(PKG_PARENT) + os.pathsep + env.get('PYTHONPATH', '')
        result = subprocess.run(
            [sys.executable, str(MEASURE_RESOLVE), '--mode', 'scale', '--records', '5000',
             '--bits', '100000', '--hashes', '8', '--limit-mib', '128'],
            capture_output=True, text=True, env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        summary = json.loads(result.stdout)
        self.assertEqual(summary['records'], 5000)
        self.assertLessEqual(summary['peak_kib'], 128 * 1024)
        self.assertFalse(summary['pair_table_materialized'])
        self.assertLessEqual(summary['max_workspace_table_rows'], 5000 * 8 + 10000)


if __name__ == '__main__':
    unittest.main()
