"""Acceptance tests for verifiable snapshot export, check and restore.

Covers real CLI round trips, strict manifest validation, internal database
relationship verification, symlink/path-traversal refusal, read-only checks,
staged restore publication with injected failures, concurrent sync during
export, the self-made SQLite fixture and the unchanged v1 entry point.
"""
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path

import bloom_index

ROOT = Path(__file__).resolve().parents[1]
PKG_PARENT = Path(bloom_index.__file__).resolve().parents[1]
FIXTURE_BUNDLE = ROOT / 'fixtures' / 'snapshot' / 'fixture-bundle'
FIXTURE_QUERY = ROOT / 'fixtures' / 'snapshot' / 'query.json'
FIXTURE_REPORT = ROOT / 'fixtures' / 'snapshot' / 'expected-report.json'
FIXTURE_CHECK = ROOT / 'fixtures' / 'snapshot' / 'expected-check.json'
V1_INPUT = ROOT / 'fixtures' / 'input.json'


def cli(args, env_extra=None):
    env = dict(os.environ)
    env['PYTHONPATH'] = str(PKG_PARENT) + os.pathsep + env.get('PYTHONPATH', '')
    if env_extra:
        env.update(env_extra)
    return subprocess.run([sys.executable, '-m', 'bloom_index.cli', *map(str, args)],
                          capture_output=True, text=True, env=env)


def write_manifest(path, items):
    path = Path(path)
    with path.open('w', encoding='utf-8') as stream:
        for item in items:
            stream.write(json.dumps({'item': item}, ensure_ascii=False) + '\n')
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_control(path, updates):
    Path(path).write_text(json.dumps({'updates': updates}, ensure_ascii=False), encoding='utf-8')


def update(source, generation, base, manifest_path, sha=None):
    if sha is None:
        sha = hashlib.sha256(Path(manifest_path).read_bytes()).hexdigest()
    return {'source': source, 'generation': generation, 'base_generation': base,
            'manifest': str(manifest_path), 'sha256': sha}


def sha256_path(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def tree_hashes(directory):
    result = {}
    for root, _dirs, files in os.walk(directory):
        for name in files:
            path = Path(root) / name
            result[str(path.relative_to(directory))] = sha256_path(path)
    return result


class Case(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.td = Path(self.tmp.name)
        self.db = self.td / 'idx.db'

    def tearDown(self):
        self.tmp.cleanup()

    def init(self, bits=128, hashes=3):
        result = cli(['init', '--db', self.db, '--bits', bits, '--hashes', hashes])
        self.assertEqual(result.returncode, 0, result.stderr)

    def sync(self, source, items, generation, base, name=None):
        manifest_path = self.td / (name or ('m_%s_%d.jsonl' % (source, generation)))
        write_manifest(manifest_path, items)
        ctl = self.td / ('ctl_%s_%d.json' % (name or source, generation))
        write_control(ctl, [update(source, generation, base, manifest_path)])
        result = cli(['sync', '--db', self.db, '--source', ctl])
        self.assertEqual(result.returncode, 0, result.stderr)
        return result

    def query(self, queries, db=None):
        db = db or self.db
        qf = self.td / ('q_%d.json' % len(list(self.td.glob('q_*.json'))))
        out = self.td / ('r_%d.json' % len(list(self.td.glob('r_*.json'))))
        qf.write_text(json.dumps({'queries': queries}, ensure_ascii=False), encoding='utf-8')
        result = cli(['query', '--db', db, '--source', qf, '--output', out])
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(out.read_text(encoding='utf-8'))

    def build_live_db(self):
        self.init(128, 3)
        self.sync('甲', ['alpha', 'beta', 'gamma'], 1, 0, 'a')
        self.sync('乙', ['gamma', 'delta'], 1, 0, 'b')
        self.sync('甲', ['beta', 'gamma', 'epsilon'], 2, 1, 'a2')

    def copy_bundle(self, name='bundle'):
        bundle = self.td / name
        shutil.copytree(FIXTURE_BUNDLE, bundle)
        return bundle

    def tamper_db(self, bundle, sql):
        db_path = bundle / 'index.sqlite3'
        conn = sqlite3.connect(str(db_path))
        conn.execute(sql)
        conn.commit()
        conn.close()
        self._refresh_manifest_hashes(bundle)

    def _refresh_manifest_hashes(self, bundle):
        manifest_path = bundle / 'manifest.json'
        manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
        raw = (bundle / 'index.sqlite3').read_bytes()
        manifest['db_size_bytes'] = len(raw)
        manifest['db_sha256'] = hashlib.sha256(raw).hexdigest()
        manifest_path.write_text(json.dumps(manifest, sort_keys=True, indent=2) + '\n',
                                 encoding='utf-8')

    def edit_manifest(self, bundle, mutate):
        manifest_path = bundle / 'manifest.json'
        manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
        mutate(manifest)
        manifest_path.write_text(json.dumps(manifest, sort_keys=True, indent=2) + '\n',
                                 encoding='utf-8')

    def assert_rc2(self, result):
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertTrue(result.stderr.strip())


class FixtureBased(Case):
    def test_fixture_bundle_checks_and_restores_to_identical_query(self):
        result = cli(['snapshot-check', '--bundle', FIXTURE_BUNDLE])
        self.assertEqual(result.returncode, 0, result.stderr)
        check = json.loads(result.stdout)
        self.assertEqual(check, json.loads(FIXTURE_CHECK.read_text(encoding='utf-8')))
        self.assertEqual(check['status'], 'ok')
        new_db = self.td / 'restored.db'
        result = cli(['restore', '--bundle', FIXTURE_BUNDLE, '--db', new_db])
        self.assertEqual(result.returncode, 0, result.stderr)
        out = self.td / 'report.json'
        result = cli(['query', '--db', new_db, '--source', FIXTURE_QUERY, '--output', out])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(out.read_text(encoding='utf-8')),
                         json.loads(FIXTURE_REPORT.read_text(encoding='utf-8')))
        # restored database reports the same source count and active set
        report = json.loads(out.read_text(encoding='utf-8'))
        self.assertEqual(report['commit_seq'], check['commit_seq'])
        self.assertEqual(report['bits'], check['bits'])
        self.assertEqual(report['hashes'], check['hashes'])
        self.assertEqual(report['active_items'], check['active_items'])
        self.assertEqual({q['item'] for q in report['queries'] if q['actual_present']},
                         {q['item'] for q in json.loads(FIXTURE_REPORT.read_text())['queries']
                          if q['actual_present']})

    def test_fixture_manifest_has_exact_strict_fields(self):
        manifest = json.loads((FIXTURE_BUNDLE / 'manifest.json').read_text(encoding='utf-8'))
        self.assertEqual(set(manifest),
                         {'format_version', 'bits', 'hashes', 'commit_seq',
                          'db_size_bytes', 'db_sha256'})
        self.assertEqual(manifest['format_version'], 1)
        self.assertEqual(manifest['db_size_bytes'],
                         (FIXTURE_BUNDLE / 'index.sqlite3').stat().st_size)
        self.assertEqual(manifest['db_sha256'], sha256_path(FIXTURE_BUNDLE / 'index.sqlite3'))
        self.assertEqual(sorted(os.listdir(FIXTURE_BUNDLE)), ['index.sqlite3', 'manifest.json'])


class ExportRoundTrip(Case):
    def test_real_cli_export_check_restore_field_by_field(self):
        self.build_live_db()
        queries = ['alpha', 'beta', 'gamma', 'delta', 'epsilon', 'zeta', 'zeta']
        original = self.query(queries)
        def source_db_hashes():
            return {name: sha256_path(p) for name in ('', '-wal', '-shm')
                    if (p := Path(str(self.db) + name)).exists()}
        source_hashes = source_db_hashes()
        bundle = self.td / 'bundle'
        result = cli(['snapshot', '--db', self.db, '--output', bundle])
        self.assertEqual(result.returncode, 0, result.stderr)
        export_summary = json.loads(result.stdout)
        self.assertEqual(export_summary['commit_seq'], original['commit_seq'])
        # export does not modify the source database files and leaves no sidecars
        self.assertEqual(source_db_hashes(), source_hashes)
        self.assertEqual(sorted(name for name in source_db_hashes()), [''])
        self.assertEqual(sorted(os.listdir(bundle)), ['index.sqlite3', 'manifest.json'])
        self.assertFalse(any(name.endswith(('-wal', '-shm')) for name in os.listdir(bundle)))
        manifest = json.loads((bundle / 'manifest.json').read_text(encoding='utf-8'))
        for field in ('bits', 'hashes', 'commit_seq'):
            self.assertEqual(manifest[field], original[field])
        self.assertEqual(manifest['db_size_bytes'], (bundle / 'index.sqlite3').stat().st_size)
        self.assertEqual(manifest['db_sha256'], sha256_path(bundle / 'index.sqlite3'))
        result = cli(['snapshot-check', '--bundle', bundle])
        self.assertEqual(result.returncode, 0, result.stderr)
        new_db = self.td / 'restored.db'
        result = cli(['restore', '--bundle', bundle, '--db', new_db])
        self.assertEqual(result.returncode, 0, result.stderr)
        restored = self.query(queries, db=new_db)
        for field in ('commit_seq', 'bits', 'hashes', 'active_items', 'set_bits',
                      'bitmap_hex', 'queries', 'false_positives', 'false_negatives',
                      'negative_queries', 'observed_false_positive_rate'):
            self.assertEqual(restored[field], original[field], field)

    def test_empty_database_snapshot_commit_seq_zero(self):
        self.init(9, 2)
        bundle = self.td / 'empty-bundle'
        self.assertEqual(cli(['snapshot', '--db', self.db, '--output', bundle]).returncode, 0)
        self.assertEqual(cli(['snapshot-check', '--bundle', bundle]).returncode, 0)
        new_db = self.td / 'empty.db'
        self.assertEqual(cli(['restore', '--bundle', bundle, '--db', new_db]).returncode, 0)
        report = self.query(['a', 'b'], db=new_db)
        self.assertEqual(report['commit_seq'], 0)
        self.assertEqual(report['active_items'], 0)
        self.assertEqual(report['bitmap_hex'], '00' * 2)

    def test_snapshot_refuses_existing_conflicting_and_symlink_outputs(self):
        self.build_live_db()
        existing = self.td / 'taken'
        existing.mkdir()
        self.assert_rc2(cli(['snapshot', '--db', self.db, '--output', existing]))
        link = self.td / 'link'
        link.symlink_to(existing)
        self.assert_rc2(cli(['snapshot', '--db', self.db, '--output', link]))
        file_target = self.td / 'file.txt'
        file_target.write_text('x')
        self.assert_rc2(cli(['snapshot', '--db', self.db, '--output', file_target]))
        linked_parent = self.td / 'linkdir'
        linked_parent.symlink_to(self.td)
        self.assert_rc2(cli(['snapshot', '--db', self.db,
                             '--output', linked_parent / 'bundle']))
        self.assertEqual(sorted(os.listdir(self.td)).count('bundle'), 0)
        # source database behind a symlink is refused as well
        db_link = self.td / 'db-link'
        db_link.symlink_to(self.db)
        self.assert_rc2(cli(['snapshot', '--db', db_link, '--output', self.td / 'b2']))
        self.assertFalse((self.td / 'b2').exists())
        # missing source database
        self.assert_rc2(cli(['snapshot', '--db', self.td / 'nope.db',
                             '--output', self.td / 'b3']))


class ManifestForgery(Case):
    def test_forged_commit_seq_rejected_by_check_and_restore_without_target(self):
        bundle = self.copy_bundle()
        self.edit_manifest(bundle, lambda m: m.update(commit_seq=m['commit_seq'] + 1))
        check = cli(['snapshot-check', '--bundle', bundle])
        self.assert_rc2(check)
        self.assertIn('commit_seq', check.stderr)
        new_db = self.td / 'must-not-exist.db'
        restore = cli(['restore', '--bundle', bundle, '--db', new_db])
        self.assert_rc2(restore)
        self.assertIn('commit_seq', restore.stderr)
        self.assertFalse(new_db.exists())
        self.assertFalse(Path(str(new_db) + '-wal').exists())
        self.assertFalse(Path(str(new_db) + '-shm').exists())
        # the untouched fixture still restores fine
        self.assertEqual(cli(['restore', '--bundle', FIXTURE_BUNDLE,
                              '--db', self.td / 'ok.db']).returncode, 0)
        self.assertTrue((self.td / 'ok.db').exists())

    def test_forged_digest_and_size_rejected(self):
        bundle = self.copy_bundle('digest')
        self.edit_manifest(bundle, lambda m: m.update(db_sha256='0' * 64))
        self.assert_rc2(cli(['snapshot-check', '--bundle', bundle]))
        bundle = self.copy_bundle('size')
        self.edit_manifest(bundle, lambda m: m.update(db_size_bytes=m['db_size_bytes'] + 1))
        self.assert_rc2(cli(['snapshot-check', '--bundle', bundle]))

    def test_strict_field_set_and_types(self):
        cases = [
            lambda m: m.update(extra=1),
            lambda m: m.pop('bits'),
            lambda m: m.update(bits='256'),
            lambda m: m.update(hashes=1.5),
            lambda m: m.update(format_version=2),
            lambda m: m.update(db_sha256='Z' * 64),
            lambda m: m.update(commit_seq=-1),
        ]
        for index, mutate in enumerate(cases):
            bundle = self.copy_bundle('strict-%d' % index)
            self.edit_manifest(bundle, mutate)
            self.assert_rc2(cli(['snapshot-check', '--bundle', bundle]))
            self.assert_rc2(cli(['restore', '--bundle', bundle,
                                 '--db', self.td / ('no-%d.db' % index)]))
            self.assertFalse((self.td / ('no-%d.db' % index)).exists())
        # duplicate JSON keys and non-finite numbers
        bundle = self.copy_bundle('dup')
        raw = (bundle / 'manifest.json').read_text()
        (bundle / 'manifest.json').write_text(raw[:-1] + ',"bits":256}\n')
        self.assert_rc2(cli(['snapshot-check', '--bundle', bundle]))
        bundle = self.copy_bundle('nan')
        (bundle / 'manifest.json').write_text(
            (bundle / 'manifest.json').read_text().replace('256', 'NaN', 1))
        self.assert_rc2(cli(['snapshot-check', '--bundle', bundle]))
        # malformed JSON
        bundle = self.copy_bundle('broken')
        (bundle / 'manifest.json').write_text('{not json')
        self.assert_rc2(cli(['snapshot-check', '--bundle', bundle]))


class DatabaseCorruption(Case):
    def test_truncated_database_rejected(self):
        bundle = self.copy_bundle('trunc-size')
        db_path = bundle / 'index.sqlite3'
        db_path.write_bytes(db_path.read_bytes()[:-31])  # manifest size now stale
        self.assert_rc2(cli(['snapshot-check', '--bundle', bundle]))
        # even with a re-forged manifest, SQLite integrity must fail
        bundle = self.copy_bundle('trunc-integrity')
        db_path = bundle / 'index.sqlite3'
        db_path.write_bytes(db_path.read_bytes()[:-31])
        self._refresh_manifest_hashes(bundle)
        result = cli(['snapshot-check', '--bundle', bundle])
        self.assert_rc2(result)
        self.assertIn('integrity', result.stderr)

    def test_generation_gap_and_credentials_rejected(self):
        bundle = self.copy_bundle('gap')
        self.tamper_db(bundle, "DELETE FROM generations WHERE source='fixture-a' AND generation=1")
        result = cli(['snapshot-check', '--bundle', bundle])
        self.assert_rc2(result)
        self.assertIn('contiguous', result.stderr)

        bundle = self.copy_bundle('base')
        self.tamper_db(bundle,
                       "UPDATE generations SET base_generation=99 WHERE generation=1")
        result = cli(['snapshot-check', '--bundle', bundle])
        self.assert_rc2(result)
        self.assertIn('base_generation', result.stderr)

        bundle = self.copy_bundle('orphan')
        self.tamper_db(bundle, "DELETE FROM sources WHERE source='fixture-b'")
        result = cli(['snapshot-check', '--bundle', bundle])
        self.assert_rc2(result)
        self.assertIn('generation credentials do not match sources', result.stderr)

    def test_bad_counters_and_active_items_rejected(self):
        bundle = self.copy_bundle('counter')
        self.tamper_db(
            bundle,
            'UPDATE counters SET cnt=cnt+1 WHERE pos=(SELECT MIN(pos) FROM counters)')
        result = cli(['snapshot-check', '--bundle', bundle])
        self.assert_rc2(result)
        self.assertIn('counter mismatch', result.stderr)

        bundle = self.copy_bundle('active')
        self.tamper_db(bundle, "UPDATE meta SET value=value+1 WHERE key='active_items'")
        result = cli(['snapshot-check', '--bundle', bundle])
        self.assert_rc2(result)
        self.assertIn('active_items', result.stderr)

        bundle = self.copy_bundle('counter-pos')
        self.tamper_db(bundle, 'UPDATE counters SET pos=pos+1000000')
        self.assert_rc2(cli(['snapshot-check', '--bundle', bundle]))

    def test_corrupt_bundle_never_restores_and_target_never_appears(self):
        bundle = self.copy_bundle('no-restore')
        self.tamper_db(
            bundle,
            'UPDATE counters SET cnt=cnt+1 WHERE pos=(SELECT MIN(pos) FROM counters)')
        target = self.td / 'forbidden.db'
        self.assert_rc2(cli(['restore', '--bundle', bundle, '--db', target]))
        self.assertFalse(target.exists())
        self.assertEqual([name for name in os.listdir(self.td)
                          if '.restore-' in name or '.stage-' in name], [])


class BundleLayoutAndSymlinks(Case):
    def test_missing_extra_entries_and_symlinks_refused(self):
        bundle = self.copy_bundle('entries')
        (bundle / 'rogue.txt').write_text('x')
        self.assert_rc2(cli(['snapshot-check', '--bundle', bundle]))
        os.unlink(bundle / 'rogue.txt')
        os.unlink(bundle / 'manifest.json')
        self.assert_rc2(cli(['snapshot-check', '--bundle', bundle]))

        bundle = self.copy_bundle('db-symlink')
        db_path = bundle / 'index.sqlite3'
        db_path.unlink()
        db_path.symlink_to(FIXTURE_BUNDLE / 'index.sqlite3')
        self.assert_rc2(cli(['snapshot-check', '--bundle', bundle]))

        bundle = self.copy_bundle('manifest-symlink')
        manifest_path = bundle / 'manifest.json'
        manifest_path.unlink()
        manifest_path.symlink_to(FIXTURE_BUNDLE / 'manifest.json')
        self.assert_rc2(cli(['snapshot-check', '--bundle', bundle]))

        bundle = self.copy_bundle('subdir')
        (bundle / 'index.sqlite3').unlink()
        (bundle / 'index.sqlite3').mkdir()
        self.assert_rc2(cli(['snapshot-check', '--bundle', bundle]))

        link = self.td / 'bundle-link'
        link.symlink_to(self.copy_bundle('real'))
        self.assert_rc2(cli(['snapshot-check', '--bundle', link]))

    def test_restore_refuses_existing_target_without_changing_bytes(self):
        bundle = self.copy_bundle()
        target = self.td / 'existing.db'
        target.write_bytes(b'prior contents must survive')
        before = target.read_bytes()
        self.assert_rc2(cli(['restore', '--bundle', bundle, '--db', target]))
        self.assertEqual(target.read_bytes(), before)
        # symlink target and -wal/-shm sidecars are also conflicts
        link = self.td / 'link.db'
        link.symlink_to(target)
        self.assert_rc2(cli(['restore', '--bundle', bundle, '--db', link]))
        self.assertEqual(target.read_bytes(), before)
        wal = self.td / 'side.db-wal'
        wal.write_bytes(b'wal')
        self.assert_rc2(cli(['restore', '--bundle', bundle, '--db', self.td / 'side.db']))
        self.assertFalse((self.td / 'side.db').exists())
        self.assertEqual(wal.read_bytes(), b'wal')

    def test_restore_and_check_never_touch_bundle_bytes(self):
        bundle = self.copy_bundle()
        before = tree_hashes(bundle)
        self.assertEqual(cli(['snapshot-check', '--bundle', bundle]).returncode, 0)
        self.assertEqual(cli(['restore', '--bundle', bundle,
                              '--db', self.td / 'r1.db']).returncode, 0)
        self.assertEqual(cli(['snapshot-check', '--bundle', bundle]).returncode, 0)
        self.assertEqual(tree_hashes(bundle), before)
        # no sidecars are ever created next to the immutable database
        self.assertEqual(sorted(os.listdir(bundle)), ['index.sqlite3', 'manifest.json'])


class StagedRestoreFailure(Case):
    def test_injected_failures_publish_nothing_and_retry_succeeds(self):
        bundle = self.copy_bundle()
        for env_var in ('BLOOM_INDEX_TEST_FAIL_STAGE', 'BLOOM_INDEX_TEST_FAIL_PUBLISH'):
            target = self.td / (env_var.lower() + '.db')
            result = cli(['restore', '--bundle', bundle, '--db', target],
                         env_extra={env_var: '1'})
            self.assert_rc2(result)
            self.assertFalse(target.exists())
            self.assertEqual([name for name in os.listdir(self.td)
                              if '.restore-' in name], [])
        # after removing the injected fault, the same target path retries cleanly
        target = self.td / 'clean.db'
        self.assertEqual(cli(['restore', '--bundle', bundle, '--db', target]).returncode, 0)
        self.assertTrue(target.is_file())
        report = self.query(['beta', 'absent'], db=target)
        self.assertEqual(report['commit_seq'], 2)


class ConcurrentExport(Case):
    def test_sync_commits_during_snapshot_without_lock_failure(self):
        bits, hashes = 200_000, 4
        self.init(bits, hashes)
        first = self.td / 'gen1.jsonl'
        write_manifest(first, ['item-%07d' % i for i in range(120_000)])
        ctl1 = self.td / 'ctl1.json'
        write_control(ctl1, [update('S', 1, 0, first)])
        self.assertEqual(cli(['sync', '--db', self.db, '--source', ctl1]).returncode, 0)
        second = self.td / 'gen2.jsonl'
        write_manifest(second, ['item-%07d' % i for i in range(60_000, 200_000)])
        ctl2 = self.td / 'ctl2.json'
        write_control(ctl2, [update('S', 2, 1, second)])

        bundle = self.td / 'concurrent-bundle'
        outcomes = {}
        release = threading.Event()

        def hold_reader():
            conn = sqlite3.connect(str(self.db), timeout=10)
            conn.execute('BEGIN')
            conn.execute('SELECT COUNT(*) FROM ownership')
            release.wait(30)
            conn.execute('COMMIT')
            conn.close()

        def export():
            outcomes['snapshot'] = cli(['snapshot', '--db', self.db, '--output', bundle])

        def commit_sync():
            threading.Event().wait(0.5)
            outcomes['sync'] = cli(['sync', '--db', self.db, '--source', ctl2])
            release.set()

        threads = [threading.Thread(target=hold_reader),
                   threading.Thread(target=export), threading.Thread(target=commit_sync)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(90)
        release.set()
        self.assertEqual(outcomes['snapshot'].returncode, 0, outcomes['snapshot'].stderr)
        self.assertEqual(outcomes['sync'].returncode, 0, outcomes['sync'].stderr)
        sync_summary = json.loads(outcomes['sync'].stdout)
        self.assertEqual(sync_summary['applied'], 1)
        self.assertEqual(sync_summary['commit_seq'], 2)

        check = cli(['snapshot-check', '--bundle', bundle])
        self.assertEqual(check.returncode, 0, check.stderr)
        checked = json.loads(check.stdout)
        self.assertIn(checked['commit_seq'], (1, 2))
        restored = self.td / 'concurrent.db'
        self.assertEqual(cli(['restore', '--bundle', bundle, '--db', restored]).returncode, 0)
        report = self.query(['item-0000001', 'item-100000', 'item-199999'], db=restored)
        # every reported field belongs to the same pinned committed state
        self.assertEqual(report['commit_seq'], checked['commit_seq'])
        self.assertEqual(report['bits'], checked['bits'])
        self.assertEqual(report['hashes'], checked['hashes'])
        self.assertEqual(report['active_items'], checked['active_items'])
        if checked['commit_seq'] == 2:
            self.assertEqual(report['queries'][0]['actual_present'], False)
            self.assertEqual(report['queries'][2]['actual_present'], True)
        else:
            self.assertEqual(report['queries'][0]['actual_present'], True)
            self.assertEqual(report['queries'][2]['actual_present'], False)
        live = self.query(['item-0000001'])
        self.assertEqual(live['commit_seq'], 2)  # the competing sync did land


class CliContract(Case):
    def test_argument_errors_return_2(self):
        self.build_live_db()
        self.assert_rc2(cli(['snapshot', '--db', self.db]))                    # missing --output
        self.assert_rc2(cli(['snapshot', '--output', self.td / 'x']))          # missing --db
        self.assert_rc2(cli(['snapshot-check']))                                # missing --bundle
        self.assert_rc2(cli(['restore', '--bundle', self.td]))                  # missing --db
        self.assert_rc2(cli(['snapshot-check', '--bundle', self.td / 'absent']))

    def test_v1_legacy_entry_and_safe_publication_unchanged(self):
        out = self.td / 'v1.json'
        result = cli(['--source', V1_INPUT, '--output', out])
        self.assertEqual(result.returncode, 0, result.stderr)
        first = out.read_bytes()
        report = json.loads(first)
        self.assertEqual(report['schema_version'], 1)
        self.assertEqual(report['source_sha256'], sha256_path(V1_INPUT))
        # rerun is deterministic and byte stable
        self.assertEqual(cli(['--source', V1_INPUT, '--output', out]).returncode, 0)
        self.assertEqual(out.read_bytes(), first)
        # invalid input preserves the prior report and exits 2
        bad = self.td / 'bad.json'
        bad.write_text('{"a":1,"a":2}')
        result = cli(['--source', bad, '--output', out])
        self.assert_rc2(result)
        self.assertEqual(out.read_bytes(), first)
        # symlink output is refused and its target is not created
        target = self.td / 'victim.json'
        link = self.td / 'link.json'
        link.symlink_to(target)
        self.assert_rc2(cli(['--source', V1_INPUT, '--output', link]))
        self.assertFalse(target.exists())

    def test_existing_init_sync_query_behavior_unchanged(self):
        self.init(64, 3)
        self.sync('S', ['a', 'b'], 1, 0)
        report = self.query(['a', 'b', 'c'])
        self.assertEqual(report['schema_version'], 2)
        self.assertEqual(report['commit_seq'], 1)
        self.assertEqual(report['active_items'], 2)
        self.assertEqual([q['actual_present'] for q in report['queries']],
                         [True, True, False])
        # a rejected sync still leaves state intact (existing safe-publication behavior)
        bad_manifest = self.td / 'bad.jsonl'
        bad_manifest.write_bytes(b'{"item":"x"}\n\n')
        ctl = self.td / 'bad-ctl.json'
        write_control(ctl, [update('S', 2, 1, bad_manifest)])
        self.assert_rc2(cli(['sync', '--db', self.db, '--source', ctl]))
        self.assertEqual(self.query(['a'])['commit_seq'], 1)


if __name__ == '__main__':
    unittest.main()
