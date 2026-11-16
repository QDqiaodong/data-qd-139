"""Verifiable single-commit snapshots of the persistent membership index.

A bundle is a directory holding exactly two regular files:

* ``index.sqlite3`` -- one consistent, already-committed SQLite snapshot
  (journal mode DELETE, so the whole database state is this one file);
* ``manifest.json`` -- strict JSON describing the snapshot: format version,
  bits, hashes, commit_seq, database byte size and SHA256 of the exact
  database bytes inside the bundle.

Export never writes to the source database: it opens the WAL source
read-only, pins one committed state inside a read transaction and copies it
with the SQLite online backup API, so a ``sync`` committing concurrently
neither blocks on a long write lock nor produces a torn snapshot. Every
manifest field is cross-checked against the copied database, so the whole
bundle always describes one and the same committed state.

Check and restore open the bundled database read-only/immutable (no -wal or
-shm sidecar is ever created) and re-derive every internal relationship:
schema, meta, per-source generation credentials, ownership, active item
count and the position counters. Restore stages into a hidden sibling
directory and only publishes with an atomic rename after every write and
verification step has succeeded; failure cleans the staging area and leaves
the target path untouched.
"""
from array import array
from pathlib import Path
import hashlib
import json
import os
import shutil
import sqlite3
import tempfile

from .filtering import positions
from .store import _check_db_path

FORMAT_VERSION = 1
DB_NAME = 'index.sqlite3'
MANIFEST_NAME = 'manifest.json'
BUNDLE_ENTRIES = frozenset((DB_NAME, MANIFEST_NAME))
MANIFEST_FIELDS = frozenset(
    ('format_version', 'bits', 'hashes', 'commit_seq', 'db_size_bytes', 'db_sha256'))
HASH_CHUNK = 1 << 20
MAX_BITS = 1_000_000
MAX_HASHES = 16


class BundleError(ValueError):
    """A bundle (or the source/target path contract) is invalid."""


def _sha256_file(path):
    hasher = hashlib.sha256()
    with open(path, 'rb') as stream:
        for chunk in iter(lambda: stream.read(HASH_CHUNK), b''):
            hasher.update(chunk)
    return hasher.hexdigest()


def _is_int(value):
    return type(value) is int


def _is_sha256(value):
    return isinstance(value, str) and len(value) == 64 and all(c in '0123456789abcdef' for c in value)


def _load_manifest(manifest_path):
    """Strict manifest parse: exact fields, integer types, no dup keys/NaN."""
    try:
        raw = Path(manifest_path).read_bytes()
    except OSError as error:
        raise BundleError('manifest: cannot read: ' + str(error))

    def unique(items):
        keys = set()
        for key, _value in items:
            if key in keys:
                raise BundleError('manifest: duplicate JSON key: ' + key)
            keys.add(key)
        return dict(items)

    def invalid(token):
        raise BundleError('manifest: nonfinite JSON number: ' + token)

    try:
        manifest = json.loads(raw, object_pairs_hook=unique, parse_constant=invalid)
    except json.JSONDecodeError as error:
        raise BundleError('manifest: invalid JSON: ' + str(error))
    if not isinstance(manifest, dict):
        raise BundleError('manifest: root must be a JSON object')
    keys = set(manifest)
    if keys != MANIFEST_FIELDS:
        missing = sorted(MANIFEST_FIELDS - keys)
        extra = sorted(keys - MANIFEST_FIELDS)
        raise BundleError('manifest: wrong field set, missing=%s extra=%s' % (missing, extra))
    for key in ('format_version', 'bits', 'hashes', 'commit_seq', 'db_size_bytes'):
        if not _is_int(manifest[key]):
            raise BundleError('manifest: %s must be an integer' % key)
    if manifest['format_version'] != FORMAT_VERSION:
        raise BundleError('manifest: unsupported format_version: %r' % (manifest['format_version'],))
    if not 1 <= manifest['bits'] <= MAX_BITS:
        raise BundleError('manifest: bits outside allowed bounds')
    if not 1 <= manifest['hashes'] <= MAX_HASHES:
        raise BundleError('manifest: hashes outside allowed bounds')
    if manifest['commit_seq'] < 0:
        raise BundleError('manifest: commit_seq must be >= 0')
    if manifest['db_size_bytes'] <= 0:
        raise BundleError('manifest: db_size_bytes must be > 0')
    if not _is_sha256(manifest['db_sha256']):
        raise BundleError('manifest: db_sha256 must be 64 lowercase hex characters')
    return manifest


def _bundle_paths(bundle_dir):
    """Validate the directory contract: real dir, no symlinks, exact entries."""
    bundle = Path(bundle_dir)
    if os.path.islink(bundle):
        raise BundleError('bundle path cannot be a symlink: ' + str(bundle))
    if not bundle.is_dir():
        raise BundleError('bundle directory not found: ' + str(bundle))
    entries = []
    with os.scandir(bundle) as scanner:
        for entry in scanner:
            entries.append(entry.name)
            if entry.is_symlink():
                raise BundleError('bundle entry cannot be a symlink: ' + entry.name)
            if not entry.is_file():
                raise BundleError('bundle entry must be a regular file: ' + entry.name)
    if set(entries) != BUNDLE_ENTRIES:
        extra = sorted(set(entries) - BUNDLE_ENTRIES)
        missing = sorted(BUNDLE_ENTRIES - set(entries))
        raise BundleError('bundle: wrong entries, missing=%s extra=%s' % (missing, extra))
    return bundle / DB_NAME, bundle / MANIFEST_NAME


def _connect_immutable(db_path):
    """Open strictly read-only; immutable=1 forbids -wal/-shm and any writes."""
    conn = sqlite3.connect(Path(db_path).resolve().as_uri() + '?immutable=1',
                           uri=True, timeout=0)
    conn.execute('PRAGMA query_only=ON')
    return conn


def _connect_readonly(db_path):
    """Read-only view of a live WAL database, including committed WAL frames."""
    conn = sqlite3.connect(Path(db_path).resolve().as_uri() + '?mode=ro',
                           uri=True, timeout=5.0)
    conn.execute('PRAGMA query_only=ON')
    return conn


def _source_sidecars(db_path):
    return [Path(str(db_path) + suffix) for suffix in ('-wal', '-shm')]


def _release_readonly_artifacts(db_path, before):
    """Remove -wal/-shm created by our read-only open, but never anything a
    concurrent writer has adopted: a non-empty WAL means a live write."""
    wal = Path(str(db_path) + '-wal')
    try:
        wal_active = wal.exists() and wal.stat().st_size > 0
    except OSError:
        return
    if wal_active:
        return
    for sidecar, (existed, _size) in before.items():
        if not existed and os.path.lexists(sidecar):
            try:
                os.unlink(sidecar)
            except OSError:
                pass


_EXPECTED_OBJECTS = frozenset((
    ('table', 'meta'), ('table', 'sources'), ('table', 'generations'),
    ('table', 'ownership'), ('table', 'counters'), ('index', 'ownership_by_item'),
))


def _integrity_ok(conn, label):
    rows = conn.execute('PRAGMA integrity_check').fetchall()
    if rows != [('ok',)]:
        detail = rows[0][0] if rows else 'no result'
        raise BundleError('%s: SQLite integrity check failed: %s' % (label, detail))


def _check_schema(conn, label):
    objects = conn.execute(
        "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'").fetchall()
    if set(objects) != _EXPECTED_OBJECTS:
        raise BundleError('%s: unexpected database schema objects: %r' % (label, sorted(objects)))
    mode = conn.execute('PRAGMA journal_mode').fetchone()[0]
    if mode != 'delete':
        raise BundleError(
            '%s: bundled database must use rollback journal mode (delete), got %r' % (label, mode))


def _read_committed_state(conn, label):
    """Read and validate every derivable relationship in one read transaction."""
    conn.execute('BEGIN')
    try:
        state = _validate_state(conn, label)
    except BaseException:
        try:
            conn.execute('ROLLBACK')
        except sqlite3.Error:
            pass
        raise
    else:
        conn.execute('COMMIT')
    return state


def _validate_state(conn, label):
    """Validate the state visible inside the caller's current transaction."""
    meta_rows = conn.execute('SELECT key, value FROM meta').fetchall()
    meta = dict(meta_rows)
    if set(meta) != {'bits', 'hashes', 'commit_seq', 'active_items'}:
        raise BundleError('%s: meta table must hold exactly bits/hashes/commit_seq/active_items' % label)
    for key, value in meta_rows:
        if not _is_int(value) or value < 0:
            raise BundleError('%s: meta.%s must be a nonnegative integer' % (label, key))
    bits, hashes = meta['bits'], meta['hashes']
    if not 1 <= bits <= MAX_BITS or not 1 <= hashes <= MAX_HASHES:
        raise BundleError('%s: bits/hashes outside allowed bounds' % label)
    commit_seq, active_items = meta['commit_seq'], meta['active_items']

    sources = dict(conn.execute('SELECT source, generation FROM sources').fetchall())
    for source, generation in sources.items():
        if not isinstance(source, str) or not source:
            raise BundleError('%s: sources contains an invalid source name' % label)
        if not _is_int(generation) or generation < 1:
            raise BundleError('sources[%r]: generation must be a positive integer' % source)

    creds = {}
    for source, generation, base_generation, digest in conn.execute(
            'SELECT source, generation, base_generation, sha256 FROM generations'):
        if not isinstance(source, str) or not source:
            raise BundleError('%s: generations contains an invalid source name' % label)
        if not _is_int(generation) or generation < 1:
            raise BundleError('generations[%r]: generation must be a positive integer' % source)
        if not _is_int(base_generation) or base_generation != generation - 1:
            raise BundleError(
                'generations[%s][%d]: base_generation must equal generation-1' % (source, generation))
        if not _is_sha256(digest):
            raise BundleError(
                'generations[%s][%d]: sha256 must be 64 lowercase hex characters' % (source, generation))
        creds.setdefault(source, {})[generation] = base_generation

    if set(creds) != set(sources):
        orphan = sorted(set(creds) - set(sources))
        missing = sorted(set(sources) - set(creds))
        raise BundleError(
            'generation credentials do not match sources table, missing=%s orphan=%s' % (missing, orphan))
    total_generations = 0
    for source, current in sources.items():
        chain = creds[source]
        expected_gens = set(range(1, current + 1))
        if set(chain) != expected_gens:
            raise BundleError(
                'sources[%r]: generation credentials are not contiguous 1..%d, got %s'
                % (source, current, sorted(chain)))
        total_generations += current

    # commit_seq bounds derivable from the stored history: at least the
    # highest per-source generation, at most the number of applied generations.
    highest = max(sources.values(), default=0)
    if not highest <= commit_seq <= total_generations:
        raise BundleError(
            'commit_seq=%d is inconsistent with generation history (range %d..%d)'
            % (commit_seq, highest, total_generations))

    owned = conn.execute('SELECT COUNT(*) FROM ownership').fetchone()[0]
    distinct_pairs = conn.execute(
        'SELECT COUNT(*) FROM (SELECT DISTINCT source, item FROM ownership)').fetchone()[0]
    if owned != distinct_pairs:
        raise BundleError('ownership: duplicate (source, item) rows detected')
    distinct_items = conn.execute('SELECT COUNT(DISTINCT item) FROM ownership').fetchone()[0]
    if active_items != distinct_items:
        raise BundleError(
            'meta.active_items=%d but ownership holds %d distinct items'
            % (active_items, distinct_items))
    orphan_owned = conn.execute(
        'SELECT COUNT(*) FROM ownership o WHERE NOT EXISTS'
        ' (SELECT 1 FROM sources s WHERE s.source = o.source)').fetchone()[0]
    if orphan_owned:
        raise BundleError('ownership: %d rows reference unknown sources' % orphan_owned)

    _verify_counters(conn, label, bits, hashes)
    return {'bits': bits, 'hashes': hashes, 'commit_seq': commit_seq,
            'active_items': active_items, 'sources': len(sources)}


def _verify_counters(conn, label, bits, hashes):
    """Re-derive every position counter from ownership; streamed, bounded memory."""
    counts = array('q', [0]) * bits
    # Counters count each distinct active item once, however many sources own it.
    cursor = conn.execute('SELECT DISTINCT item FROM ownership')
    while True:
        chunk = cursor.fetchmany(10_000)
        if not chunk:
            break
        for (item,) in chunk:
            for pos in set(positions(item, bits, hashes)):
                counts[pos] += 1
    expected = 0
    for value in counts:
        if value:
            expected += 1
    stored_cursor = conn.execute('SELECT pos, cnt FROM counters ORDER BY pos')
    pos, rows_seen = 0, 0
    while True:
        chunk = stored_cursor.fetchmany(10_000)
        if not chunk:
            break
        for stored_pos, stored_cnt in chunk:
            if not _is_int(stored_pos) or not 0 <= stored_pos < bits:
                raise BundleError('%s: counter position %r outside 0..%d' % (label, stored_pos, bits - 1))
            if not _is_int(stored_cnt) or stored_cnt <= 0:
                raise BundleError('%s: counter at pos=%d must be a positive integer' % (label, stored_pos))
            while pos < bits and counts[pos] == 0:
                pos += 1
            if pos >= bits or pos != stored_pos:
                raise BundleError(
                    '%s: counter mismatch at pos=%d: stored position has no ownership support'
                    % (label, stored_pos))
            if counts[pos] != stored_cnt:
                raise BundleError(
                    '%s: counter mismatch at pos=%d: stored=%d recomputed=%d'
                    % (label, pos, stored_cnt, counts[pos]))
            pos += 1
            rows_seen += 1
    while pos < bits and counts[pos] == 0:
        pos += 1
    if pos < bits:
        raise BundleError('%s: missing counter row for position %d' % (label, pos))
    if rows_seen != expected:
        raise BundleError(
            '%s: counter row count %d != recomputed %d' % (label, rows_seen, expected))


def verify_bundle(bundle_dir):
    """Full read-only verification; returns the facts proven about the bundle."""
    db_path, manifest_path = _bundle_paths(bundle_dir)
    manifest = _load_manifest(manifest_path)
    stat_result = os.stat(db_path)
    if stat_result.st_size != manifest['db_size_bytes']:
        raise BundleError(
            'index.sqlite3: size %d != manifest db_size_bytes %d'
            % (stat_result.st_size, manifest['db_size_bytes']))
    digest = _sha256_file(db_path)
    if digest != manifest['db_sha256']:
        raise BundleError('index.sqlite3: sha256 %s != manifest db_sha256 %s'
                          % (digest, manifest['db_sha256']))
    conn = _connect_immutable(db_path)
    try:
        _integrity_ok(conn, 'index.sqlite3')
        _check_schema(conn, 'index.sqlite3')
        state = _read_committed_state(conn, 'index.sqlite3')
    finally:
        conn.close()
    for field in ('bits', 'hashes', 'commit_seq'):
        if state[field] != manifest[field]:
            raise BundleError(
                'manifest %s=%d does not match database value %d'
                % (field, manifest[field], state[field]))
    return {'manifest': manifest, 'state': state,
            'db_size_bytes': stat_result.st_size, 'db_sha256': digest}


def _make_stage(parent, prefix):
    return Path(tempfile.mkdtemp(prefix=prefix, dir=str(parent)))


def _cleanup_stage(stage):
    if stage is not None and os.path.lexists(stage):
        shutil.rmtree(stage)


def export_snapshot(db_path, output_dir):
    """Pack one committed state of ``db_path`` into a brand-new bundle directory."""
    _check_db_path(db_path)
    output = Path(output_dir)
    if os.path.islink(output) or os.path.lexists(output):
        raise BundleError('snapshot output already exists: ' + str(output))
    parent = output.parent
    parent.mkdir(parents=True, exist_ok=True)
    if os.path.islink(parent) or not parent.is_dir():
        raise BundleError('snapshot output parent is not a directory: ' + str(parent))
    stage = None
    try:
        stage = _make_stage(parent, '.' + output.name + '.stage-')
        staged_db = stage / DB_NAME
        sidecars = _source_sidecars(db_path)
        before = {path: (os.path.lexists(path),
                         os.stat(path).st_size if os.path.lexists(path) else 0)
                  for path in sidecars}
        try:
            source = _connect_readonly(db_path)
            try:
                # Pin one committed state; all reported metadata comes from this state.
                source.execute('BEGIN')
                state = _validate_state(source, 'source database')
                destination = sqlite3.connect(str(staged_db))
                try:
                    source.backup(destination)
                    # Fold any WAL into the single file and switch to DELETE journal.
                    destination.execute('PRAGMA wal_checkpoint(TRUNCATE)')
                    mode = destination.execute('PRAGMA journal_mode=DELETE').fetchone()[0]
                    if mode != 'delete':
                        raise BundleError('could not finalize bundled database journal mode: ' + str(mode))
                    _integrity_ok(destination, DB_NAME)
                    _check_schema(destination, DB_NAME)
                    copied = _read_committed_state(destination, DB_NAME)
                finally:
                    destination.close()
                if copied != state:
                    raise BundleError(
                        'snapshot is not a single committed state: copied metadata %r != pinned metadata %r'
                        % (copied, state))
                source.execute('COMMIT')
            finally:
                source.close()
        finally:
            # The export writes no source data; only remove sidecars our own
            # read-only connection created and no concurrent writer is using.
            _release_readonly_artifacts(db_path, before)
        for suffix in ('-wal', '-shm'):
            leftover = Path(str(staged_db) + suffix)
            if os.path.lexists(leftover):
                os.unlink(leftover)
        os.chmod(staged_db, 0o644)
        size = os.stat(staged_db).st_size
        digest = _sha256_file(staged_db)
        manifest = {
            'format_version': FORMAT_VERSION,
            'bits': state['bits'],
            'hashes': state['hashes'],
            'commit_seq': state['commit_seq'],
            'db_size_bytes': size,
            'db_sha256': digest,
        }
        manifest_tmp = stage / (MANIFEST_NAME + '.tmp')
        with open(manifest_tmp, 'w', encoding='utf-8') as stream:
            json.dump(manifest, stream, sort_keys=True, ensure_ascii=False,
                      allow_nan=False, indent=2)
            stream.write('\n')
        os.chmod(manifest_tmp, 0o644)
        os.replace(manifest_tmp, stage / MANIFEST_NAME)
        # Last guard before the atomic directory publication.
        if os.path.lexists(output):
            raise BundleError('snapshot output already exists: ' + str(output))
        os.rename(stage, output)
        stage = None
        return {'bundle': str(output), 'commit_seq': state['commit_seq'],
                'bits': state['bits'], 'hashes': state['hashes'],
                'db_size_bytes': size, 'db_sha256': digest}
    finally:
        _cleanup_stage(stage)


def restore_bundle(bundle_dir, new_db_path):
    """Verify the bundle and publish it at a path that does not yet exist."""
    # Verification gates every write; a forged commit_seq is rejected here and
    # no target path is ever created.
    verified = verify_bundle(bundle_dir)
    manifest = verified['manifest']
    target = Path(new_db_path)
    for path in (target, Path(str(target) + '-wal'), Path(str(target) + '-shm')):
        if os.path.islink(path) or os.path.lexists(path):
            raise BundleError('restore target already exists: ' + str(path))
    parent = target.parent
    parent.mkdir(parents=True, exist_ok=True)
    if os.path.islink(parent) or not parent.is_dir():
        raise BundleError('restore target parent is not a directory: ' + str(parent))
    stage = None
    try:
        stage = _make_stage(parent, '.' + target.name + '.restore-')
        staged_db = stage / target.name
        source_db = Path(bundle_dir) / DB_NAME
        with open(source_db, 'rb') as src, open(staged_db, 'xb') as dst:
            for chunk in iter(lambda: src.read(HASH_CHUNK), b''):
                dst.write(chunk)
        if os.environ.get('BLOOM_INDEX_TEST_FAIL_STAGE'):
            raise BundleError('injected staging failure')
        size = os.stat(staged_db).st_size
        digest = _sha256_file(staged_db)
        if size != manifest['db_size_bytes'] or digest != manifest['db_sha256']:
            raise BundleError('staged database no longer matches the verified bundle')
        conn = _connect_immutable(staged_db)
        try:
            _integrity_ok(conn, 'staged database')
            staged_state = _read_committed_state(conn, 'staged database')
        finally:
            conn.close()
        for field in ('bits', 'hashes', 'commit_seq'):
            if staged_state[field] != manifest[field]:
                raise BundleError(
                    'staged database %s diverged from manifest before publish' % field)
        if os.environ.get('BLOOM_INDEX_TEST_FAIL_PUBLISH'):
            raise BundleError('injected publish failure')
        # Re-check immediately before publication; rename refuses to overwrite.
        if os.path.lexists(target):
            raise BundleError('restore target already exists: ' + str(target))
        os.chmod(staged_db, 0o644)
        os.rename(staged_db, target)
        os.rmdir(stage)
        stage = None
        check = _connect_immutable(target)
        try:
            published = _read_committed_state(check, 'restored database')
        finally:
            check.close()
        if published != staged_state:
            raise BundleError('restored database changed during publication')
        return {'db': str(target), 'commit_seq': published['commit_seq'],
                'bits': published['bits'], 'hashes': published['hashes'],
                'active_items': published['active_items'], 'sources': published['sources']}
    finally:
        _cleanup_stage(stage)
