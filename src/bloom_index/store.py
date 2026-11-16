"""Persistent multi-source membership routing index on SQLite.

One writer / many readers (WAL). Ownership replacement, generation
credentials and position counters commit in a single transaction; query
reads one committed snapshot. Manifests are streamed and deduplicated
inside the database; no full manifest or full item set is materialized
in Python.
"""
from fractions import Fraction
from pathlib import Path
import hashlib, json, os, sqlite3

from .common import read, require, ident, number, rows
from .filtering import positions

MAX_SOURCES_PER_BATCH = 64
MAX_UPDATES = 20000
MAX_MANIFEST_LINES = 1_000_000
MAX_LINE_BYTES = 4096
LOCK_TIMEOUT_SECONDS = 5.0

SCHEMA = (
    "CREATE TABLE meta(key TEXT PRIMARY KEY, value INTEGER NOT NULL);"
    "CREATE TABLE sources(source TEXT PRIMARY KEY, generation INTEGER NOT NULL) WITHOUT ROWID;"
    "CREATE TABLE generations(source TEXT NOT NULL, generation INTEGER NOT NULL,"
    " base_generation INTEGER NOT NULL, sha256 TEXT NOT NULL,"
    " PRIMARY KEY(source, generation)) WITHOUT ROWID;"
    "CREATE TABLE ownership(source TEXT NOT NULL, item TEXT NOT NULL,"
    " PRIMARY KEY(source, item)) WITHOUT ROWID;"
    "CREATE INDEX ownership_by_item ON ownership(item);"
    "CREATE TABLE counters(pos INTEGER PRIMARY KEY, cnt INTEGER NOT NULL);"
)

COUNTER_UPSERT = (
    "INSERT INTO counters(pos, cnt) VALUES(?, ?)"
    " ON CONFLICT(pos) DO UPDATE SET cnt = cnt + excluded.cnt"
)


def _unique(pairs):
    result = {}
    for k, v in pairs:
        if k in result:
            raise ValueError('duplicate JSON key: ' + k)
        result[k] = v
    return result


def _invalid(value):
    raise ValueError('nonfinite JSON number: ' + value)


def _connect(db_path):
    return sqlite3.connect(str(db_path), timeout=LOCK_TIMEOUT_SECONDS, isolation_level=None)


def _meta(conn):
    try:
        pairs = conn.execute("SELECT key, value FROM meta").fetchall()
    except sqlite3.Error:
        raise ValueError('database is not initialized')
    state = dict(pairs)
    for key in ('bits', 'hashes', 'commit_seq', 'active_items'):
        if key not in state:
            raise ValueError('database is not initialized')
    return state


def _check_db_path(db):
    if os.path.islink(db):
        raise ValueError('database cannot be a symlink')
    if not Path(db).is_file():
        raise ValueError('database not found: ' + str(db))


def init_db(db_path, bits, hashes):
    """Create an empty index; bits/hashes are fixed forever. Refuses to overwrite."""
    bits = number(bits, 1, 1000000)
    hashes = number(hashes, 1, 16)
    dest = Path(db_path)
    if os.path.lexists(dest):
        raise ValueError('database path already exists: ' + str(dest))
    dest.parent.mkdir(parents=True, exist_ok=True)
    conn = _connect(dest)
    try:
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('BEGIN IMMEDIATE')
        for statement in SCHEMA.split(';'):
            if statement.strip():
                conn.execute(statement)
        conn.executemany(
            'INSERT INTO meta(key, value) VALUES(?, ?)',
            [('bits', bits), ('hashes', hashes), ('commit_seq', 0), ('active_items', 0)],
        )
        conn.execute('COMMIT')
    except BaseException:
        try:
            conn.execute('ROLLBACK')
        except sqlite3.Error:
            pass
        conn.close()
        for suffix in ('', '-wal', '-shm'):
            try:
                os.unlink(str(dest) + suffix)
            except OSError:
                pass
        raise
    conn.close()
    return {'bits': bits, 'hashes': hashes, 'commit_seq': 0}


def _validate_update(entry):
    require(entry, 'source generation base_generation manifest sha256')
    source = ident(entry['source'])
    generation = number(entry['generation'], 0, 10**9)
    base_generation = number(entry['base_generation'], 0, 10**9)
    manifest = entry['manifest']
    if not isinstance(manifest, str) or not manifest:
        raise ValueError('manifest must be a nonempty path string')
    digest = entry['sha256']
    if not isinstance(digest, str) or len(digest) != 64 or any(c not in '0123456789abcdef' for c in digest):
        raise ValueError('sha256 must be 64 lowercase hex characters')
    return {'source': source, 'generation': generation, 'base_generation': base_generation,
            'manifest': manifest, 'sha256': digest}


def _stage_manifest(conn, path, expected_sha):
    """Stream a UTF-8 JSONL manifest into a temp table; hash the exact raw bytes read."""
    conn.execute('CREATE TEMPORARY TABLE staging(item TEXT PRIMARY KEY)')
    hasher = hashlib.sha256()
    count = 0
    batch = []
    with Path(path).open('rb') as stream:
        for raw in stream:
            hasher.update(raw)
            count += 1
            if count > MAX_MANIFEST_LINES:
                raise ValueError('manifest exceeds 1000000 lines: ' + str(path))
            line = raw[:-1] if raw.endswith(b'\n') else raw
            if len(line) > MAX_LINE_BYTES:
                raise ValueError('manifest line exceeds 4096 bytes: ' + str(path))
            if not line:
                raise ValueError('empty manifest line: ' + str(path))
            obj = json.loads(line.decode('utf-8'), object_pairs_hook=_unique, parse_constant=_invalid)
            require(obj, 'item')
            batch.append((ident(obj['item']),))
            if len(batch) >= 1000:
                conn.executemany('INSERT OR IGNORE INTO staging(item) VALUES(?)', batch)
                batch.clear()
    if batch:
        conn.executemany('INSERT OR IGNORE INTO staging(item) VALUES(?)', batch)
    if hasher.hexdigest() != expected_sha:
        raise ValueError('manifest digest mismatch: ' + str(path))


def _bump_counters(conn, bits, hashes, sql, delta):
    """Stream items crossing the 0<->1 ownership boundary; adjust each distinct position once."""
    total = 0
    batch = []
    cursor = conn.execute(sql)
    while True:
        chunk = cursor.fetchmany(1000)
        if not chunk:
            break
        for (item,) in chunk:
            total += 1
            for pos in set(positions(item, bits, hashes)):
                batch.append((pos, delta))
        if len(batch) >= 10000:
            conn.executemany(COUNTER_UPSERT, batch)
            batch.clear()
    if batch:
        conn.executemany(COUNTER_UPSERT, batch)
    return total


def _apply_staged(conn, source, bits, hashes):
    """Replace the source's ownership with the staged manifest; return active_items delta."""
    conn.execute(
        'CREATE TEMPORARY TABLE to_add AS SELECT s.item AS item FROM staging s'
        ' WHERE NOT EXISTS(SELECT 1 FROM ownership o WHERE o.source = ? AND o.item = s.item)',
        (source,))
    conn.execute(
        'CREATE TEMPORARY TABLE to_remove AS SELECT o.item AS item FROM ownership o'
        ' WHERE o.source = ? AND NOT EXISTS(SELECT 1 FROM staging s WHERE s.item = o.item)',
        (source,))
    added = _bump_counters(
        conn, bits, hashes,
        'SELECT t.item FROM to_add t WHERE NOT EXISTS(SELECT 1 FROM ownership o WHERE o.item = t.item)',
        +1)
    removed = _bump_counters(
        conn, bits, hashes,
        'SELECT t.item FROM to_remove t WHERE (SELECT COUNT(*) FROM ownership o WHERE o.item = t.item) = 1',
        -1)
    conn.execute('DELETE FROM ownership WHERE source = ? AND item IN (SELECT item FROM to_remove)', (source,))
    conn.execute('INSERT INTO ownership(source, item) SELECT ?, item FROM to_add', (source,))
    conn.execute('DROP TABLE to_add')
    conn.execute('DROP TABLE to_remove')
    conn.execute('DROP TABLE staging')
    return added - removed


def _sync_one(conn, db_resolved, control_dir, spec, bits, hashes):
    row = conn.execute('SELECT generation FROM sources WHERE source = ?', (spec['source'],)).fetchone()
    current = row[0] if row else 0
    replay = conn.execute(
        'SELECT 1 FROM generations WHERE source = ? AND generation = ?'
        ' AND base_generation = ? AND sha256 = ?',
        (spec['source'], spec['generation'], spec['base_generation'], spec['sha256'])).fetchone()
    if replay:
        return False
    if spec['generation'] != current + 1 or spec['base_generation'] != current:
        raise ValueError(
            'generation mismatch for source %r: expected generation=%d base_generation=%d, got %d/%d'
            % (spec['source'], current + 1, current, spec['generation'], spec['base_generation']))
    manifest = Path(spec['manifest'])
    if not manifest.is_absolute():
        manifest = control_dir / manifest
    if manifest.resolve() == db_resolved:
        raise ValueError('manifest path conflicts with database path')
    _stage_manifest(conn, manifest, spec['sha256'])
    delta = _apply_staged(conn, spec['source'], bits, hashes)
    conn.execute(
        'INSERT INTO generations(source, generation, base_generation, sha256) VALUES(?, ?, ?, ?)',
        (spec['source'], spec['generation'], spec['base_generation'], spec['sha256']))
    conn.execute(
        'INSERT INTO sources(source, generation) VALUES(?, ?)'
        ' ON CONFLICT(source) DO UPDATE SET generation = excluded.generation',
        (spec['source'], spec['generation']))
    if delta:
        conn.execute("UPDATE meta SET value = value + ? WHERE key = 'active_items'", (delta,))
    return True


def sync_control(db_path, control_path):
    """Apply one control file atomically; commit_seq +1 only when a new generation lands."""
    _check_db_path(db_path)
    control = Path(control_path)
    if not control.is_file():
        raise ValueError('control file not found: ' + str(control))
    db_resolved = Path(db_path).resolve()
    if db_resolved == control.resolve():
        raise ValueError('database and control file must differ')
    data, _ = read(control)
    require(data, 'updates')
    specs = [_validate_update(entry) for entry in rows(data['updates'], MAX_UPDATES)]
    if len({spec['source'] for spec in specs}) > MAX_SOURCES_PER_BATCH:
        raise ValueError('more than 64 distinct sources in one control file')
    conn = _connect(db_path)
    try:
        conn.execute('BEGIN IMMEDIATE')
        try:
            state = _meta(conn)
            applied = replayed = 0
            for spec in specs:
                if _sync_one(conn, db_resolved, control.resolve().parent, spec, state['bits'], state['hashes']):
                    applied += 1
                else:
                    replayed += 1
            if applied:
                conn.execute("UPDATE meta SET value = value + 1 WHERE key = 'commit_seq'")
                conn.execute('DELETE FROM counters WHERE cnt <= 0')
            commit_seq = conn.execute("SELECT value FROM meta WHERE key = 'commit_seq'").fetchone()[0]
            conn.execute('COMMIT')
        except BaseException:
            try:
                conn.execute('ROLLBACK')
            except sqlite3.Error:
                pass
            raise
    finally:
        conn.close()
    return {'commit_seq': commit_seq, 'applied': applied, 'replayed': replayed}


def query_db(db_path, query_path):
    """Audit one committed snapshot: ownership and counters read in a single transaction."""
    _check_db_path(db_path)
    source = Path(query_path)
    if not source.is_file():
        raise ValueError('query file not found: ' + str(source))
    if db_resolved_conflict(db_path, source):
        raise ValueError('database and query file must differ')
    data, _ = read(source)
    require(data, 'queries')
    queries = [ident(q) for q in rows(data['queries'], 20000)]
    conn = _connect(db_path)
    try:
        conn.execute('BEGIN')
        try:
            state = _meta(conn)
            bits, hashes = state['bits'], state['hashes']
            bitmap = bytearray((bits + 7) // 8)
            for (pos,) in conn.execute('SELECT pos FROM counters WHERE cnt > 0'):
                bitmap[pos // 8] |= 1 << (pos % 8)
            result = []
            negatives = false_positive = false_negative = 0
            for item in queries:
                maybe = all(bitmap[p // 8] & (1 << (p % 8)) for p in positions(item, bits, hashes))
                source_count = conn.execute(
                    'SELECT COUNT(*) FROM ownership WHERE item = ?', (item,)).fetchone()[0]
                actual = source_count > 0
                negatives += not actual
                false_positive += maybe and not actual
                false_negative += actual and not maybe
                result.append({'item': item, 'maybe_present': maybe,
                               'actual_present': actual, 'source_count': source_count})
            conn.execute('COMMIT')
        except BaseException:
            try:
                conn.execute('ROLLBACK')
            except sqlite3.Error:
                pass
            raise
    finally:
        conn.close()
    return {'schema_version': 2, 'commit_seq': state['commit_seq'], 'bits': bits, 'hashes': hashes,
            'active_items': state['active_items'],
            'set_bits': sum(b.bit_count() for b in bitmap), 'bitmap_hex': bitmap.hex(),
            'queries': result, 'negative_queries': negatives,
            'false_positives': false_positive, 'false_negatives': false_negative,
            'observed_false_positive_rate': str(Fraction(false_positive, negatives)) if negatives else None}


def db_resolved_conflict(db_path, other):
    return Path(db_path).resolve() == Path(other).resolve()
