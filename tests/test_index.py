
import hashlib, json, os, random, signal, sqlite3, string, subprocess, sys, tempfile, threading, time, unittest
from fractions import Fraction
from pathlib import Path

import bloom_index
from bloom_index.filtering import positions

ROOT = Path(__file__).resolve().parents[1]
PKG_PARENT = Path(bloom_index.__file__).resolve().parents[1]
MEASURE = ROOT / 'tools' / 'measure_scale.py'


def cli(args, **kwargs):
    env = dict(os.environ)
    env['PYTHONPATH'] = str(PKG_PARENT) + os.pathsep + env.get('PYTHONPATH', '')
    return subprocess.run([sys.executable, '-m', 'bloom_index.cli', *map(str, args)],
                          capture_output=True, text=True, env=env, **kwargs)


def popen(args):
    env = dict(os.environ)
    env['PYTHONPATH'] = str(PKG_PARENT) + os.pathsep + env.get('PYTHONPATH', '')
    return subprocess.Popen([sys.executable, '-m', 'bloom_index.cli', *map(str, args)],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)


def manifest(path, items, raw=None):
    path = Path(path)
    if raw is not None:
        path.write_bytes(raw)
    else:
        with path.open('w', encoding='utf-8') as stream:
            for item in items:
                stream.write(json.dumps({'item': item}, ensure_ascii=False) + '\n')
    return hashlib.sha256(path.read_bytes()).hexdigest()


def control(path, updates):
    path = Path(path)
    path.write_text(json.dumps({'updates': updates}, ensure_ascii=False), encoding='utf-8')
    return path


def update(source, generation, base, manifest_path, sha=None, hash_path=None):
    if sha is None:
        sha = hashlib.sha256(Path(hash_path or manifest_path).read_bytes()).hexdigest()
    return {'source': source, 'generation': generation, 'base_generation': base,
            'manifest': str(manifest_path), 'sha256': sha}


class Oracle:
    """Independent exact model: item -> set of sources, bitmap rebuilt from scratch."""

    def __init__(self, bits, hashes):
        self.bits, self.hashes, self.owners, self.seq = bits, hashes, {}, 0

    def replace(self, source, items):
        items = set(items)
        old = {item for item, owners in self.owners.items() if source in owners}
        for item in old - items:
            self.owners[item].discard(source)
            if not self.owners[item]:
                del self.owners[item]
        for item in items - old:
            self.owners.setdefault(item, set()).add(source)

    def commit(self):
        self.seq += 1

    def bitmap(self):
        bitmap = bytearray((self.bits + 7) // 8)
        for item in self.owners:
            for pos in set(positions(item, self.bits, self.hashes)):
                bitmap[pos // 8] |= 1 << (pos % 8)
        return bitmap

    def expect(self, queries):
        bitmap = self.bitmap()
        result, negatives, false_positive, false_negative = [], 0, 0, 0
        for item in queries:
            source_count = len(self.owners.get(item, ()))
            maybe = all(bitmap[p // 8] & (1 << (p % 8)) for p in positions(item, self.bits, self.hashes))
            actual = source_count > 0
            negatives += not actual
            false_positive += maybe and not actual
            false_negative += actual and not maybe
            result.append({'item': item, 'maybe_present': maybe,
                           'actual_present': actual, 'source_count': source_count})
        return {'schema_version': 2, 'commit_seq': self.seq, 'bits': self.bits, 'hashes': self.hashes,
                'active_items': len(self.owners), 'set_bits': sum(b.bit_count() for b in bitmap),
                'bitmap_hex': bitmap.hex(), 'queries': result, 'negative_queries': negatives,
                'false_positives': false_positive, 'false_negatives': false_negative,
                'observed_false_positive_rate': str(Fraction(false_positive, negatives)) if negatives else None}


class Case(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.td = Path(self.tmp.name)
        self.db = self.td / 'idx.db'

    def tearDown(self):
        self.tmp.cleanup()

    def init(self, bits=64, hashes=3):
        return cli(['init', '--db', self.db, '--bits', bits, '--hashes', hashes])

    def sync(self, ctl):
        return cli(['sync', '--db', self.db, '--source', ctl])

    def query(self, queries, expect_rc=0):
        qf = self.td / 'query.json'
        qf.write_text(json.dumps({'queries': queries}, ensure_ascii=False), encoding='utf-8')
        out = self.td / 'report.json'
        result = cli(['query', '--db', self.db, '--source', qf, '--output', out])
        self.assertEqual(result.returncode, expect_rc, result.stderr)
        if expect_rc == 0:
            return json.loads(out.read_text(encoding='utf-8'))
        return result

    def apply(self, source, items, generation, base):
        path = self.td / f'm_{source}_{generation}.jsonl'
        manifest(path, items)
        ctl = control(self.td / f'ctl_{source}_{generation}.json',
                      [update(source, generation, base, Path(path).name, hash_path=path)])
        result = self.sync(ctl)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result


class Init(Case):
    def test_init_and_reinit_refused(self):
        result = self.init(64, 3)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), {'bits': 64, 'commit_seq': 0, 'hashes': 3})
        before = self.db.read_bytes()
        again = self.init(64, 3)
        self.assertEqual(again.returncode, 2)
        self.assertEqual(self.db.read_bytes(), before)
        other = cli(['init', '--db', self.db, '--bits', 8, '--hashes', 1])
        self.assertEqual(other.returncode, 2)
        self.assertEqual(self.db.read_bytes(), before)

    def test_init_bounds(self):
        for bits, hashes in [(0, 1), (1, 0), (1000001, 1), (1, 17), (-1, 1)]:
            self.assertEqual(cli(['init', '--db', self.db, '--bits', bits, '--hashes', hashes]).returncode, 2)
            self.assertFalse(self.db.exists())
        self.assertEqual(self.init(1, 16).returncode, 0)
        self.assertEqual(self.init(1000000, 16).returncode, 2)  # path now exists

    def test_init_refuses_symlink_path(self):
        target = self.td / 'real.db'
        link = self.td / 'link.db'
        link.symlink_to(target)
        self.assertEqual(cli(['init', '--db', link, '--bits', 8, '--hashes', 1]).returncode, 2)
        self.assertFalse(target.exists())


class Sync(Case):
    def test_generations_clear_and_replay(self):
        self.assertEqual(self.init().returncode, 0)
        self.apply('甲', ['alpha', 'beta', 'gamma'], 1, 0)
        report = self.query(['alpha', 'beta', 'gamma', 'delta'])
        self.assertEqual(report['commit_seq'], 1)
        self.assertEqual(report['active_items'], 3)
        self.apply('甲', [], 2, 1)  # empty manifest clears the source
        report = self.query(['alpha', 'beta'])
        self.assertEqual(report['active_items'], 0)
        self.assertEqual(report['set_bits'], 0)
        self.assertEqual(report['bitmap_hex'], '00' * 8)
        self.assertTrue(all(not q['maybe_present'] for q in report['queries']))
        # replay of generation 1 must be a no-op even though generation 2 is later
        replay = self.sync(self.td / 'ctl_甲_1.json')
        self.assertEqual(replay.returncode, 0, replay.stderr)
        self.assertEqual(json.loads(replay.stdout), {'applied': 0, 'commit_seq': 2, 'replayed': 1})
        report = self.query(['alpha'])
        self.assertEqual(report['commit_seq'], 2)
        self.assertEqual(report['active_items'], 0)
        # replay also works when the manifest file is already gone
        os.unlink(self.td / 'm_甲_1.jsonl')
        self.assertEqual(self.sync(self.td / 'ctl_甲_1.json').returncode, 0)
        self.assertEqual(self.query(['alpha'])['commit_seq'], 2)

    def test_rejections_leave_state_untouched(self):
        self.assertEqual(self.init().returncode, 0)
        self.apply('甲', ['a', 'b'], 1, 0)
        good = self.td / 'good.jsonl'
        manifest(good, ['c'])
        cases = []
        bad = self.td / 'badline.jsonl'
        manifest(bad, [], raw=b'{"item":"x"}\n\n')  # empty line
        cases.append(update('甲', 2, 1, bad))
        cases.append(update('甲', 2, 1, good, sha='0' * 64))  # digest mismatch
        cases.append(update('甲', 3, 2, good))  # skipped generation
        cases.append(update('甲', 2, 0, good))  # wrong base_generation
        cases.append(update('甲', 1, 0, good))  # same generation, other content
        for entry in cases:
            ctl = control(self.td / 'ctl_bad.json', [entry])
            self.assertEqual(self.sync(ctl).returncode, 2, entry)
            report = self.query(['a', 'b', 'c'])
            self.assertEqual(report['commit_seq'], 1)
            self.assertEqual(report['active_items'], 2)
            self.assertEqual([q['actual_present'] for q in report['queries']], [True, True, False])

    def test_batch_atomicity_mixed_valid_and_invalid(self):
        self.assertEqual(self.init().returncode, 0)
        self.apply('一', ['x1'], 1, 0)
        self.apply('二', ['y1'], 1, 0)
        m_ok = self.td / 'ok.jsonl'
        manifest(m_ok, ['x2'])
        m_bad = self.td / 'bad.jsonl'
        manifest(m_bad, [], raw=b'{"item":"y2"}\n{"item":7}\n')
        ctl = control(self.td / 'ctl_mix.json', [update('一', 2, 1, m_ok), update('二', 2, 1, m_bad)])
        self.assertEqual(self.sync(ctl).returncode, 2)
        report = self.query(['x1', 'x2', 'y1', 'y2'])
        self.assertEqual(report['commit_seq'], 2)  # two earlier single-source batches
        self.assertEqual([q['actual_present'] for q in report['queries']], [True, False, True, False])
        # and the other order fails atomically too
        ctl = control(self.td / 'ctl_mix2.json', [update('二', 2, 1, m_bad), update('一', 2, 1, m_ok)])
        self.assertEqual(self.sync(ctl).returncode, 2)
        self.assertEqual(self.query(['x1'])['commit_seq'], 2)

    def test_swap_overlap_between_sources_same_batch(self):
        self.assertEqual(self.init(256, 4).returncode, 0)
        ma, mb = self.td / 'a1.jsonl', self.td / 'b1.jsonl'
        manifest(ma, ['x'])
        manifest(mb, ['y'])
        ctl = control(self.td / 'ctl1.json', [update('A', 1, 0, ma), update('B', 1, 0, mb)])
        self.assertEqual(self.sync(ctl).returncode, 0)
        before = self.query(['x', 'y'])
        self.assertEqual([q['source_count'] for q in before['queries']], [1, 1])
        # swap the overlapping items between the two sources in one batch
        ma2, mb2 = self.td / 'a2.jsonl', self.td / 'b2.jsonl'
        manifest(ma2, ['y'])
        manifest(mb2, ['x'])
        ctl = control(self.td / 'ctl2.json', [update('A', 2, 1, ma2), update('B', 2, 1, mb2)])
        self.assertEqual(self.sync(ctl).returncode, 0)
        after = self.query(['x', 'y'])
        self.assertEqual(after['commit_seq'], 2)
        self.assertEqual(after['bitmap_hex'], before['bitmap_hex'])  # union unchanged
        self.assertEqual(after['active_items'], 2)
        self.assertTrue(all(q['actual_present'] and q['source_count'] == 1 for q in after['queries']))

    def test_manifest_dedup_single_ownership(self):
        self.assertEqual(self.init().returncode, 0)
        path = self.td / 'dup.jsonl'
        manifest(path, [], raw=b'{"item":"a"}\n{"item":"a"}\n{"item":"b"}\n{"item":"a"}\n')
        ctl = control(self.td / 'ctl.json', [update('S', 1, 0, path)])
        self.assertEqual(self.sync(ctl).returncode, 0)
        report = self.query(['a', 'b'])
        self.assertEqual(report['active_items'], 2)
        self.assertEqual([q['source_count'] for q in report['queries']], [1, 1])

    def test_source_and_update_limits(self):
        self.assertEqual(self.init().returncode, 0)
        updates = [update(f's{i}', 1, 0, 'missing.jsonl', sha='0' * 64) for i in range(65)]
        self.assertEqual(self.sync(control(self.td / 'ctl.json', updates)).returncode, 2)
        self.assertEqual(self.query(['a'])['commit_seq'], 0)
        self.assertEqual(self.sync(control(self.td / 'ctl.json', [])).returncode, 0)  # empty batch is a no-op
        self.assertEqual(self.query(['a'])['commit_seq'], 0)
        bad = control(self.td / 'ctl.json', [{'source': 'S', 'generation': 1, 'base_generation': 0,
                                              'manifest': 'm', 'sha256': 'A' * 64}])  # uppercase digest
        self.assertEqual(self.sync(bad).returncode, 2)

    def test_manifest_line_rules(self):
        self.assertEqual(self.init().returncode, 0)
        self.apply('S', ['ok'], 1, 0)
        bad_bodies = [
            b'{"item":"a"}\n\n',                       # empty line
            b'{"item":"a"}\n{"item":"b"}\n\n',         # trailing empty line
            b'{"item":"a","item":"b"}\n',              # duplicate field
            b'{"item":"a","x":1}\n',                   # unknown field
            b'{"item":NaN}\n',                         # nonfinite
            b'{"item":5}\n',                           # item not a string
            b'{"item":" a"}\n',                        # identifier not stripped
            b'{"item":"' + b'x' * 129 + b'"}\n',       # identifier too long
            b'{"item":"a"}' + b' ' * 4085 + b'\n',     # line beyond 4096 bytes
            b'{"item":"\xff"}\n',                      # invalid UTF-8
            b'{"item":"a"}\n{"item":"a"}\n{"item":Infinity}\n',
        ]
        for body in bad_bodies:
            path = self.td / 'bad.jsonl'
            manifest(path, [], raw=body)
            ctl = control(self.td / 'ctl.json', [update('S', 2, 1, path)])
            self.assertEqual(self.sync(ctl).returncode, 2, body[:40])
            self.assertEqual(self.query(['ok'])['commit_seq'], 1)
        # boundary line of exactly 4096 bytes is accepted, as is a missing final newline
        path = self.td / 'edge.jsonl'
        line = b'{"item":"padded"}' + b' ' * (4096 - len(b'{"item":"padded"}'))
        manifest(path, [], raw=line + b'\n{"item":"last"}')
        ctl = control(self.td / 'ctl.json', [update('S', 2, 1, path)])
        self.assertEqual(self.sync(ctl).returncode, 0)
        report = self.query(['padded', 'last', 'ok'])
        self.assertEqual([q['actual_present'] for q in report['queries']], [True, True, False])

    def test_manifest_line_count_limit(self):
        self.assertEqual(self.init().returncode, 0)
        path = self.td / 'huge.jsonl'
        with path.open('w') as stream:
            for i in range(1_000_001):
                stream.write('{"item":"i%d"}\n' % i)
        ctl = control(self.td / 'ctl.json', [update('S', 1, 0, path)])
        self.assertEqual(self.sync(ctl).returncode, 2)
        self.assertEqual(self.query(['i0'])['commit_seq'], 0)

    def test_relative_and_absolute_manifest_paths(self):
        self.assertEqual(self.init().returncode, 0)
        sub = self.td / 'sub'
        sub.mkdir()
        path = sub / 'm.jsonl'
        manifest(path, ['deep'])
        ctl = control(sub / 'ctl.json', [update('S', 1, 0, 'm.jsonl', hash_path=path)])  # relative to control dir
        self.assertEqual(self.sync(ctl).returncode, 0)
        manifest(path, ['deep2'])
        ctl = control(self.td / 'ctl.json', [update('S', 2, 1, path)])   # absolute
        self.assertEqual(self.sync(ctl).returncode, 0)
        self.assertEqual([q['actual_present'] for q in self.query(['deep', 'deep2'])['queries']], [False, True])


class Counting(Case):
    def test_shared_bit_cleared_only_when_last_item_leaves(self):
        self.assertEqual(self.init(1, 16).returncode, 0)  # every item hashes to the single bit
        ma, mb = self.td / 'a.jsonl', self.td / 'b.jsonl'
        manifest(ma, ['x'])
        manifest(mb, ['x', 'y'])
        ctl = control(self.td / 'ctl1.json', [update('A', 1, 0, ma), update('B', 1, 0, mb)])
        self.assertEqual(self.sync(ctl).returncode, 0)
        report = self.query(['x', 'y'])
        self.assertEqual(report['bitmap_hex'], '01')
        self.assertEqual(report['active_items'], 2)
        self.assertEqual([q['source_count'] for q in report['queries']], [2, 1])
        self.apply('A', [], 2, 1)  # x still owned by B
        report = self.query(['x', 'y'])
        self.assertEqual(report['bitmap_hex'], '01')
        self.assertEqual([q['source_count'] for q in report['queries']], [1, 1])
        self.apply('B', ['x'], 2, 1)  # y leaves, x remains: bit must stay
        report = self.query(['x', 'y'])
        self.assertEqual(report['bitmap_hex'], '01')
        self.assertEqual(report['active_items'], 1)
        self.assertEqual([q['maybe_present'] for q in report['queries']], [True, True])  # y is a forced false positive
        self.assertEqual([q['actual_present'] for q in report['queries']], [True, False])
        self.assertEqual(report['false_positives'], 1)
        self.apply('B', [], 3, 2)  # last active item leaves: bit clears
        report = self.query(['x', 'y'])
        self.assertEqual(report['bitmap_hex'], '00')
        self.assertEqual(report['set_bits'], 0)
        self.assertEqual(report['active_items'], 0)
        self.assertEqual(report['false_positives'], 0)

    def test_exact_source_count_crosscheck(self):
        self.assertEqual(self.init(1, 16).returncode, 0)
        for name in ('A', 'B', 'C'):
            self.apply(name, ['shared'], 1, 0)
        report = self.query(['shared'])
        self.assertEqual(report['queries'][0]['source_count'], 3)
        self.assertEqual(report['active_items'], 1)
        self.apply('B', [], 2, 1)
        self.apply('A', [], 2, 1)
        report = self.query(['shared'])
        self.assertEqual(report['queries'][0]['source_count'], 1)
        self.assertEqual(report['bitmap_hex'], '01')
        self.apply('C', [], 2, 1)
        self.assertEqual(self.query(['shared'])['bitmap_hex'], '00')


class QueryReport(Case):
    def test_empty_database(self):
        self.assertEqual(self.init(9, 2).returncode, 0)
        report = self.query(['a', 'b'])
        self.assertEqual(report['commit_seq'], 0)
        self.assertEqual(report['active_items'], 0)
        self.assertEqual(report['set_bits'], 0)
        self.assertEqual(report['bitmap_hex'], '00' * 2)
        self.assertEqual(report['negative_queries'], 2)
        self.assertEqual(report['false_positives'], 0)
        self.assertEqual(report['false_negatives'], 0)
        self.assertEqual(report['observed_false_positive_rate'], '0')
        self.assertTrue(all(not q['maybe_present'] and not q['actual_present'] and q['source_count'] == 0
                            for q in report['queries']))

    def test_pure_positive_denominator_zero(self):
        self.assertEqual(self.init().returncode, 0)
        self.apply('S', ['alpha', 'beta'], 1, 0)
        report = self.query(['alpha', 'beta', 'alpha'])
        self.assertEqual(report['negative_queries'], 0)
        self.assertIsNone(report['observed_false_positive_rate'])
        self.assertEqual(report['false_positives'], 0)
        self.assertEqual(report['false_negatives'], 0)

    def test_duplicate_negative_queries_counted_each_time(self):
        self.assertEqual(self.init(1, 1).returncode, 0)
        self.apply('S', ['a'], 1, 0)
        report = self.query(['z', 'z', 'a'])
        self.assertEqual(report['negative_queries'], 2)
        self.assertEqual(report['false_positives'], 2)
        self.assertEqual(report['observed_false_positive_rate'], '1')
        self.assertEqual(report['bitmap_hex'], '01')

    def test_query_input_limits(self):
        self.assertEqual(self.init().returncode, 0)
        qf = self.td / 'q.json'
        out = self.td / 'r.json'
        qf.write_text(json.dumps({'queries': ['a'] * 20001}))
        self.assertEqual(cli(['query', '--db', self.db, '--source', qf, '--output', out]).returncode, 2)
        qf.write_text(json.dumps({'queries': ['a'], 'extra': 1}))
        self.assertEqual(cli(['query', '--db', self.db, '--source', qf, '--output', out]).returncode, 2)
        qf.write_text('{"queries":["a"],"queries":["b"]}')
        self.assertEqual(cli(['query', '--db', self.db, '--source', qf, '--output', out]).returncode, 2)
        self.assertFalse(out.exists())

    def test_query_output_failure_preserves_prior_report(self):
        self.assertEqual(self.init().returncode, 0)
        self.apply('S', ['a'], 1, 0)
        out = self.td / 'report.json'
        self.assertEqual(self.query(['a'])['active_items'], 1)
        prior = out.read_bytes()
        qf = self.td / 'q.json'
        qf.write_text('NaN')
        self.assertEqual(cli(['query', '--db', self.db, '--source', qf, '--output', out]).returncode, 2)
        self.assertEqual(out.read_bytes(), prior)
        link = self.td / 'link.json'
        link.symlink_to(self.td / 'victim.json')
        qf.write_text('{"queries":["a"]}')
        self.assertEqual(cli(['query', '--db', self.db, '--source', qf, '--output', link]).returncode, 2)
        self.assertFalse((self.td / 'victim.json').exists())


class OracleDriven(Case):
    def test_random_batches_against_exact_oracle(self):
        rng = random.Random(20261116)
        bits, hashes = rng.choice([1, 8, 64, 300]), rng.choice([1, 3, 16])
        self.assertEqual(self.init(bits, hashes).returncode, 0)
        oracle = Oracle(bits, hashes)
        current = {'A': 0, 'B': 0, 'C': 0}
        controls = []
        names = [''.join(rng.choices(string.ascii_lowercase, k=rng.randint(1, 8))) for _ in range(40)]
        for step in range(24):
            kind = rng.random()
            updates = []
            if kind < 0.65:  # valid new generations
                applied = False
                for source in rng.sample(['A', 'B', 'C'], rng.randint(1, 3)):
                    items = rng.sample(names, rng.randint(0, 8))
                    path = self.td / f'm{step}_{source}.jsonl'
                    manifest(path, items)
                    updates.append(update(source, current[source] + 1, current[source], Path(path).name, hash_path=path))
                    oracle.replace(source, items)
                    current[source] += 1
                    applied = True
                ctl = control(self.td / f'ctl{step}.json', updates)
                controls.append((ctl, updates))
                self.assertEqual(self.sync(ctl).returncode, 0)
                if applied:
                    oracle.commit()
            elif kind < 0.8 and controls:  # replay an earlier batch, possibly with later generations applied
                ctl, _ = rng.choice(controls)
                result = self.sync(ctl)
                self.assertEqual(result.returncode, 0)
                self.assertEqual(json.loads(result.stdout)['applied'], 0)
            else:  # invalid batch must be rejected without touching state
                source = rng.choice(['A', 'B', 'C'])
                path = self.td / f'm{step}.jsonl'
                manifest(path, rng.sample(names, 3))
                entry = update(source, current[source] + rng.choice([0, 2, 3]), current[source], Path(path).name, hash_path=path)
                if rng.random() < 0.5:
                    entry['sha256'] = 'f' * 64
                ctl = control(self.td / f'ctl{step}.json', [entry])
                self.assertEqual(self.sync(ctl).returncode, 2)
            queries = rng.sample(names, rng.randint(1, 12)) + rng.sample(names, 2)
            self.assertEqual(self.query(queries), oracle.expect(queries))
        self.assertEqual(self.query(names)['false_negatives'], 0)


class Concurrency(Case):
    def test_parallel_read_write_single_snapshot(self):
        bits, hashes = 512, 4
        self.assertEqual(self.init(bits, hashes).returncode, 0)
        pool = [f'item-{i}' for i in range(60)]
        queries = pool[:10] + ['absent-1', 'absent-2', 'item-0']
        oracle = Oracle(bits, hashes)
        snapshots = {0: oracle.expect(queries)}
        batches = []
        for generation in range(1, 7):
            source = 'A' if generation % 2 else 'B'
            items = pool[generation * 5:generation * 5 + 30]
            path = self.td / f'w{generation}.jsonl'
            manifest(path, items)
            batches.append(control(self.td / f'w{generation}.json',
                                   [update(source, (generation + 1) // 2, (generation - 1) // 2, Path(path).name, hash_path=path)]))
            oracle.replace(source, items)
            oracle.commit()
            snapshots[generation] = oracle.expect(queries)
        errors, reports, done = [], [], threading.Event()

        def writer():
            try:
                for ctl in batches:
                    result = self.sync(ctl)
                    if result.returncode != 0:
                        errors.append(result.stderr)
                        return
                    time.sleep(0.05)
            finally:
                done.set()

        def reader():
            qf = self.td / 'rq.json'
            qf.write_text(json.dumps({'queries': queries}))
            out = self.td / 'rr.json'
            while not done.is_set():
                result = cli(['query', '--db', self.db, '--source', qf, '--output', out])
                if result.returncode != 0:
                    errors.append(result.stderr)
                    return
                reports.append(json.loads(out.read_text()))

        threads = [threading.Thread(target=writer), threading.Thread(target=reader)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(60)
        self.assertEqual(errors, [])
        self.assertGreaterEqual(len(reports), 3)
        for report in reports:  # every field of a report belongs to one committed commit_seq
            self.assertIn(report['commit_seq'], snapshots)
            self.assertEqual(report, snapshots[report['commit_seq']])
            self.assertEqual(report['false_negatives'], 0)
        self.assertEqual(self.query(queries), snapshots[6])

    def test_second_writer_times_out_and_readers_do_not_wait(self):
        self.assertEqual(self.init().returncode, 0)
        self.apply('S', ['held'], 1, 0)
        locker = subprocess.Popen(
            [sys.executable, '-c',
             'import sqlite3,sys,time;'
             'c=sqlite3.connect(sys.argv[1],isolation_level=None);'
             'c.execute("BEGIN IMMEDIATE");print("locked",flush=True);time.sleep(7)',
             str(self.db)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            self.assertEqual(locker.stdout.readline().strip(), 'locked')
            path = self.td / 'm.jsonl'
            manifest(path, ['other'])
            ctl = control(self.td / 'ctl.json', [update('S', 2, 1, path)])
            start = time.monotonic()
            result = self.sync(ctl)
            waited = time.monotonic() - start
            self.assertEqual(result.returncode, 2)  # cannot take the write lock within 5 seconds
            self.assertGreater(waited, 3.5)
            self.assertLess(waited, 6.8)
            start = time.monotonic()  # a reader must not wait for the write transaction to end
            report = self.query(['held'])
            self.assertLess(time.monotonic() - start, 4)
            self.assertEqual(report['commit_seq'], 1)
            self.assertTrue(report['queries'][0]['actual_present'])
        finally:
            locker.wait()
        self.assertEqual(self.query(['held'])['commit_seq'], 1)  # locker rolled back, nothing leaked


class Recovery(Case):
    def test_kill_mid_write_leaves_complete_old_or_new_state(self):
        bits, hashes = 4096, 8
        self.assertEqual(self.init(bits, hashes).returncode, 0)
        items_a = [f'a-{i}' for i in range(60000)]
        items_b = [f'b-{i}' for i in range(60000)]
        ma, mb = self.td / 'a1.jsonl', self.td / 'b1.jsonl'
        manifest(ma, items_a)
        manifest(mb, items_b)
        ctl = control(self.td / 'gen1.json',
                      [update('S1', 1, 0, ma), update('S2', 1, 0, mb)])
        self.assertEqual(self.sync(ctl).returncode, 0)
        # one batch swaps the overlapping pools between the two sources
        ctl = control(self.td / 'gen2.json',
                      [update('S1', 2, 1, mb), update('S2', 2, 1, ma)])
        proc = popen(['sync', '--db', self.db, '--source', ctl])
        wal = Path(str(self.db) + '-wal')
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if proc.poll() is not None or (wal.exists() and wal.stat().st_size > 200000):
                break
            time.sleep(0.05)
        if proc.poll() is None:
            proc.send_signal(signal.SIGKILL)
        proc.wait()
        conn = sqlite3.connect(str(self.db))
        state = dict(conn.execute('SELECT key, value FROM meta'))
        seq = state['commit_seq']
        self.assertIn(seq, (1, 2))
        # both sources must come from the same committed side: complete old or complete new
        expect = {'S1': items_a, 'S2': items_b} if seq == 1 else {'S1': items_b, 'S2': items_a}
        union = set()
        for source, items in expect.items():
            owned = {row[0] for row in conn.execute('SELECT item FROM ownership WHERE source = ?', (source,))}
            self.assertEqual(owned, set(items), f'{source} mixed at commit_seq={seq}')
            union |= owned
            generation = conn.execute('SELECT generation FROM sources WHERE source = ?', (source,)).fetchone()[0]
            self.assertEqual(generation, seq)
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM generations WHERE source = ?', (source,)).fetchone()[0], seq)
        self.assertEqual(state['active_items'], len(union))
        counts = {}
        for item in union:
            for pos in set(positions(item, bits, hashes)):
                counts[pos] = counts.get(pos, 0) + 1
        stored = dict(conn.execute('SELECT pos, cnt FROM counters'))
        self.assertEqual(stored, counts)  # counters exactly match ownership
        conn.close()
        report = self.query(items_a[:3] + items_b[:3])
        self.assertEqual(report['commit_seq'], seq)
        self.assertEqual(report['false_negatives'], 0)
        self.assertEqual([q['actual_present'] for q in report['queries']], [True] * 6)
        self.assertEqual([q['source_count'] for q in report['queries']], [1] * 6)


class Paths(Case):
    def test_path_conflicts_and_symlinks(self):
        self.assertEqual(self.init().returncode, 0)
        self.apply('S', ['a'], 1, 0)
        qf = self.td / 'q.json'
        qf.write_text('{"queries":["a"]}')
        out = self.td / 'r.json'
        # database path conflicts with input/output
        self.assertEqual(cli(['query', '--db', self.db, '--source', self.db, '--output', out]).returncode, 2)
        self.assertEqual(cli(['query', '--db', self.db, '--source', qf, '--output', self.db]).returncode, 2)
        self.assertEqual(cli(['sync', '--db', self.db, '--source', self.db]).returncode, 2)
        # manifest conflicting with the database file
        ctl = control(self.td / 'ctl.json', [update('S', 2, 1, self.db, sha='0' * 64)])
        self.assertEqual(self.sync(ctl).returncode, 2)
        # database behind a symlink is refused before any write
        link = self.td / 'link.db'
        link.symlink_to(self.db)
        path = self.td / 'm.jsonl'
        manifest(path, ['b'])
        ctl = control(self.td / 'ctl.json', [update('S', 2, 1, path)])
        self.assertEqual(cli(['sync', '--db', link, '--source', ctl]).returncode, 2)
        self.assertEqual(cli(['query', '--db', link, '--source', qf, '--output', out]).returncode, 2)
        self.assertEqual(self.query(['a'])['commit_seq'], 1)
        # same file through a relative spelling is still a conflict
        self.assertEqual(cli(['query', '--db', self.db, '--source', qf, '--output', qf]).returncode, 2)

    def test_missing_database_and_uninitialized(self):
        missing = self.td / 'missing.db'
        qf = self.td / 'q.json'
        qf.write_text('{"queries":["a"]}')
        self.assertEqual(cli(['query', '--db', missing, '--source', qf, '--output', self.td / 'r.json']).returncode, 2)
        self.assertFalse(missing.exists())  # query must not create the database
        plain = self.td / 'plain.db'
        plain.write_bytes(b'not a database')
        self.assertEqual(cli(['sync', '--db', plain, '--source', qf]).returncode, 2)
        empty = self.td / 'empty.db'
        sqlite3.connect(str(empty)).close()
        self.assertEqual(cli(['sync', '--db', empty, '--source', qf]).returncode, 2)
        self.assertEqual(cli(['query', '--db', empty, '--source', qf, '--output', self.td / 'r.json']).returncode, 2)


class Scale(Case):
    def test_measure_scale_script_smoke(self):
        result = subprocess.run(
            [sys.executable, str(MEASURE), '--items', '20000', '--lines', '20000',
             '--bits', '100000', '--hashes', '8', '--limit-mib', '128'],
            capture_output=True, text=True,
            env={**os.environ, 'PYTHONPATH': str(PKG_PARENT) + os.pathsep + os.environ.get('PYTHONPATH', '')})
        self.assertEqual(result.returncode, 0, result.stderr)
        summary = json.loads(result.stdout)
        self.assertLessEqual(summary['peak_kib'], 128 * 1024)
        self.assertEqual(summary['active_items'], 20000)  # replacement: only generation 2 remains


if __name__ == '__main__':
    unittest.main()
