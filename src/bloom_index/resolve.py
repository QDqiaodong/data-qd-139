"""Record-resolution pipeline: supplier master aliases -> constrained entity groups.

The pipeline has four strictly separated responsibilities and never materializes
the full record set, alias set, candidate pair set or the full result in Python:

1. **Ingestion/validation** -- strict JSONL streamed into a private on-disk
   SQLite workspace (never the membership database; its schema is untouched).
2. **Directory selection** -- one read-committed snapshot of the membership
   index is pinned before the first master record is read; exact
   ``(source, rid)`` ownership in that snapshot alone decides active/inactive.
3. **Candidate scheduling** -- canonical aliases back a Bloom prefilter built
   with the ``filtering.positions`` hash contract; only Bloom-positive aliases
   reach exact alias lookups, and SQL primary keys deduplicate group pairs.
4. **Constrained merging** -- each record starts as its own group; the
   smallest mergeable group-ID pair wins every round, and every merge feeds
   the new group state (union aliases/sources/anchor) back into the scheduler.

The membership database is opened strictly read-only and never holds a write
lock; every reported fact comes from the one pinned ``commit_seq``.
"""
import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
import time
import unicodedata
from pathlib import Path

from .common import ident, jsonl_sink
from .snapshot import _release_readonly_artifacts, _source_sidecars

_SHA256 = hashlib.sha256
_FROM_BYTES = int.from_bytes

MAX_FILES = 64
MAX_RECORDS = 1_000_000
MAX_ALIASES = 8
MAX_LINE_BYTES = 16384
MAX_IDENT = 128
ROW_FIELDS = ('rid', 'source', 'anchor', 'aliases')

# Tunables chosen so the process stays well inside a 128 MiB RSS budget even
# at one million rows; the workspace database absorbs the data volume.
CACHE_PAGES = 1500            # ~6 MiB page cache (4 KiB pages)
INSERT_FLUSH = 10000
OWNERSHIP_BATCH = 500


class ResolveError(ValueError):
    """Any input or state defect; the CLI maps this family to exit code 2."""


# --------------------------------------------------------------------------- #
# Strict JSON helpers
# --------------------------------------------------------------------------- #

def _unique(pairs):
    seen = set()
    for key, value in pairs:
        if key in seen:
            raise ResolveError('duplicate JSON key: ' + key)
        seen.add(key)
    return dict(pairs)


def _line_hook(where):
    def hook(pairs):
        seen = set()
        for key, value in pairs:
            if key in seen:
                raise ResolveError('%s: duplicate JSON key: %s' % (where, key))
            seen.add(key)
        return dict(pairs)
    return hook


def _line_constant(where):
    def invalid(token):
        raise ResolveError('%s: nonfinite JSON number: %s' % (where, token))
    return invalid


def _invalid(token):
    raise ResolveError('nonfinite JSON number: ' + token)


def _load_control(control_path):
    control = Path(control_path)
    if not control.is_file():
        raise ResolveError('control file not found: ' + str(control))
    try:
        raw = control.read_bytes()
    except OSError as error:
        raise ResolveError('control file cannot be read: ' + str(error))
    try:
        obj = json.loads(raw.decode('utf-8'), object_pairs_hook=_unique,
                         parse_constant=_invalid)
    except UnicodeDecodeError as error:
        raise ResolveError('control file is not valid UTF-8: ' + str(error))
    except json.JSONDecodeError as error:
        raise ResolveError('control file is not valid JSON: ' + str(error))
    if not isinstance(obj, dict) or set(obj) != {'files'}:
        raise ResolveError('control must be an object with exactly one field: files')
    files = obj['files']
    if not isinstance(files, list) or not 1 <= len(files) <= MAX_FILES:
        raise ResolveError('files must be a list of 1..%d paths' % MAX_FILES)
    paths = []
    seen_text, seen_resolved = set(), set()
    base = control.resolve().parent
    for entry in files:
        if not isinstance(entry, str) or not entry:
            raise ResolveError('each files entry must be a nonempty path string')
        if entry in seen_text:
            raise ResolveError('duplicate input file in control: ' + entry)
        candidate = Path(entry)
        resolved = (candidate if candidate.is_absolute() else base / candidate).resolve()
        if resolved in seen_resolved:
            raise ResolveError('duplicate input file in control: ' + entry)
        seen_text.add(entry)
        seen_resolved.add(resolved)
        paths.append((entry, resolved))
    return control.resolve(), paths


def _canonical_alias(raw, where):
    """ident first, then NFKC + casefold; re-validated as an identifier."""
    try:
        ident(raw)  # nonblank, stripped, <=128 chars, no control characters
        normalized = unicodedata.normalize('NFKC', raw).casefold()
        return ident(normalized)
    except (ValueError, TypeError) as error:
        raise ResolveError('%s: invalid alias: %s' % (where, error))


def _ident(value, where, field):
    try:
        return ident(value)
    except (ValueError, TypeError):
        raise ResolveError('%s: %s must be a nonblank identifier, <=%d characters'
                           % (where, field, MAX_IDENT))


def _validate_row(obj, where):
    if not isinstance(obj, dict) or tuple(sorted(obj)) != tuple(sorted(ROW_FIELDS)):
        raise ResolveError('%s: expected object with exactly fields rid, source, anchor, aliases' % where)
    rid = _ident(obj['rid'], where, 'rid')
    source = _ident(obj['source'], where, 'source')
    anchor = obj['anchor']
    if anchor is not None:
        anchor = _ident(anchor, where, 'anchor')
    aliases = obj['aliases']
    if not isinstance(aliases, list) or not 1 <= len(aliases) <= MAX_ALIASES:
        raise ResolveError('%s: aliases must be a list of 1..%d strings' % (where, MAX_ALIASES))
    canonical, folded = [], set()
    for raw_alias in aliases:
        norm = _canonical_alias(raw_alias, where)
        if norm not in folded:
            folded.add(norm)
            canonical.append(norm)
    return rid, source, anchor, canonical


# --------------------------------------------------------------------------- #
# Workspace database
# --------------------------------------------------------------------------- #

SCHEMA = """
CREATE TABLE records(
  rid TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  anchor TEXT,
  active INTEGER NOT NULL,
  location TEXT NOT NULL
) WITHOUT ROWID;
CREATE TABLE rec_aliases(
  rid TEXT NOT NULL,
  alias TEXT NOT NULL,
  PRIMARY KEY(rid, alias)
) WITHOUT ROWID;
CREATE INDEX rec_aliases_by_alias ON rec_aliases(alias);
CREATE TABLE repeat_alias(
  alias TEXT PRIMARY KEY
) WITHOUT ROWID;
CREATE TABLE groups(
  gid TEXT PRIMARY KEY,
  anchor TEXT
) WITHOUT ROWID;
CREATE TABLE live(
  gid TEXT PRIMARY KEY
) WITHOUT ROWID;
CREATE TABLE group_sources(
  gid TEXT NOT NULL,
  source TEXT NOT NULL,
  PRIMARY KEY(gid, source)
) WITHOUT ROWID;
CREATE TABLE alias_group(
  alias TEXT NOT NULL,
  gid TEXT NOT NULL,
  PRIMARY KEY(alias, gid)
) WITHOUT ROWID;
CREATE INDEX alias_group_by_gid ON alias_group(gid);
CREATE TABLE members(
  rid TEXT PRIMARY KEY,
  gid TEXT NOT NULL
) WITHOUT ROWID;
CREATE INDEX members_by_gid ON members(gid);
CREATE INDEX group_sources_by_source ON group_sources(source);
"""


def _open_workspace(workdir):
    conn = sqlite3.connect(str(workdir / 'work.sqlite3'), isolation_level=None)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=OFF')
    conn.execute('PRAGMA temp_store=FILE')
    conn.execute('PRAGMA cache_size=-%d' % (CACHE_PAGES * 4))
    conn.execute('PRAGMA mmap_size=0')
    conn.execute('PRAGMA foreign_keys=ON')
    conn.execute('BEGIN')
    for statement in SCHEMA.split(';'):
        if statement.strip():
            conn.execute(statement)
    return conn


def _open_member_snapshot(db_path):
    """Read-only connection; a BEGIN transaction pins one committed snapshot."""
    uri = Path(db_path).resolve().as_uri() + '?mode=ro'
    conn = sqlite3.connect(uri, uri=True, timeout=5.0)
    conn.execute('PRAGMA query_only=ON')
    conn.execute('BEGIN')
    rows = conn.execute('SELECT key, value FROM meta').fetchall()
    meta = dict(rows)
    for key in ('bits', 'hashes', 'commit_seq'):
        if key not in meta or type(meta[key]) is not int:
            raise ResolveError('database is not initialized')
    if not 1 <= meta['bits'] <= 1_000_000 or not 1 <= meta['hashes'] <= 16:
        raise ResolveError('database bits/hashes outside allowed bounds')
    return conn, meta


# --------------------------------------------------------------------------- #
# Phase 1+2: streamed validation with exact snapshot ownership
# --------------------------------------------------------------------------- #

def _track_repeat_aliases(work, alias_batch):
    """Record aliases appearing in at least two records, incrementally.

    Runs after the batch's rec_aliases rows are inserted. A single index-driven
    self-join through the alias-leading index finds batch aliases that occur
    on more than one rid; in-memory folding inside a record already collapsed
    its own duplicate canonical aliases, so a count above one always means a
    second record. No full-alias GROUP BY sort and no Python alias set.
    """
    work.execute(
        'CREATE TEMPORARY TABLE _alias_batch(alias TEXT PRIMARY KEY) WITHOUT ROWID')
    work.executemany('INSERT OR IGNORE INTO _alias_batch(alias) VALUES(?)',
                     [(alias,) for _rid, alias in alias_batch])
    work.execute(
        'INSERT OR IGNORE INTO repeat_alias(alias)'
        ' SELECT DISTINCT b.alias FROM _alias_batch b'
        ' WHERE (SELECT COUNT(*) FROM rec_aliases a WHERE a.alias = b.alias) > 1')
    work.execute('DROP TABLE _alias_batch')


def _ingest(work, member, paths):
    """Stream every line through full validation; exact ownership per record.

    No full-record Python container is kept: the workspace database holds the
    batch, ownership is checked in bounded chunks, and rid uniqueness is
    enforced by the table's primary key rather than a Python set.
    """
    total = 0
    alias_batch, pending = [], []

    def flush_ownership():
        if not pending:
            return
        marks = {}
        for start in range(0, len(pending), OWNERSHIP_BATCH):
            chunk = pending[start:start + OWNERSHIP_BATCH]
            placeholders = ','.join('(?,?)' for _ in chunk)
            values = [v for pair in chunk for v in pair[:2]]
            rows = member.execute(
                'SELECT source, item FROM ownership WHERE (source, item) IN (%s)' % placeholders,
                values).fetchall()
            for source, item in rows:
                marks[(source, item)] = True
        for source, rid, record in pending:
            record[3] = 1 if marks.get((source, rid)) else 0
        pending.clear()

    def flush_records(record_batch):
        if not record_batch:
            return
        seen_in_batch = set()
        for record in record_batch:
            if record[0] in seen_in_batch:
                raise ResolveError(
                    'duplicate rid across batch %r at %s' % (record[0], record[4]))
            seen_in_batch.add(record[0])
        placeholders = ','.join('?' for _ in record_batch)
        existing = {row[0] for row in work.execute(
            'SELECT rid FROM records WHERE rid IN (%s)' % placeholders,
            [r[0] for r in record_batch]).fetchall()}
        if existing:
            # report the second (rejected) occurrence, with its file and line
            record = next(r for r in record_batch if r[0] in existing)
            raise ResolveError('duplicate rid across batch %r at %s' % (record[0], record[4]))
        work.executemany(
            'INSERT INTO records(rid, source, anchor, active, location) VALUES(?,?,?,?,?)',
            record_batch)
        record_batch.clear()
    record_batch = []
    for label, resolved in paths:
        try:
            stream = open(resolved, 'rb')
        except OSError as error:
            raise ResolveError('%s: cannot open input file: %s' % (label, error))
        line_no = 0
        with stream:
            for raw in stream:
                line_no += 1
                if raw.endswith(b'\n'):
                    raw = raw[:-1]
                where = '%s:%d' % (label, line_no)
                if len(raw) > MAX_LINE_BYTES:
                    raise ResolveError('%s: line exceeds %d bytes' % (where, MAX_LINE_BYTES))
                if not raw:
                    raise ResolveError('%s: empty line' % where)
                try:
                    text = raw.decode('utf-8')
                except UnicodeDecodeError:
                    raise ResolveError('%s: line is not valid UTF-8' % where)
                try:
                    obj = json.loads(text, object_pairs_hook=_line_hook(where),
                                     parse_constant=_line_constant(where))
                except json.JSONDecodeError as error:
                    raise ResolveError('%s: invalid JSON: %s' % (where, error))
                rid, source, anchor, canonical = _validate_row(obj, where)
                total += 1
                if total > MAX_RECORDS:
                    raise ResolveError('batch exceeds %d records' % MAX_RECORDS)
                record = [rid, source, anchor, -1, where]  # active filled below
                pending.append((source, rid, record))
                if len(pending) >= OWNERSHIP_BATCH:
                    flush_ownership()
                record_batch.append(record)
                for norm in canonical:
                    alias_batch.append((rid, norm))
                if len(record_batch) >= INSERT_FLUSH:
                    flush_ownership()
                    flush_records(record_batch)
                    work.executemany(
                        'INSERT OR IGNORE INTO rec_aliases(rid, alias) VALUES(?,?)', alias_batch)
                    _track_repeat_aliases(work, alias_batch)
                    alias_batch.clear()
    flush_ownership()
    flush_records(record_batch)
    if alias_batch:
        work.executemany(
            'INSERT OR IGNORE INTO rec_aliases(rid, alias) VALUES(?,?)', alias_batch)
        _track_repeat_aliases(work, alias_batch)
    active = work.execute('SELECT COUNT(*) FROM records WHERE active = 1').fetchone()[0]
    return total, active


# --------------------------------------------------------------------------- #
# Phase 3: Bloom prefilter over canonical aliases
# --------------------------------------------------------------------------- #

def _position_prefixes(hashes):
    """Precompute the per-hash byte prefixes used by filtering.positions."""
    return [str(i).encode('ascii') + b'\x00' for i in range(hashes)]


def _positions_of(encoded, prefixes, bits):
    """Bit-identical to filtering.positions, but with one UTF-8 encode per alias."""
    return [_FROM_BYTES(_SHA256(prefix + encoded).digest(), 'big') % bits
            for prefix in prefixes]


def _build_alias_bloom(work, bits, hashes):
    """Bloom prefilter over the repeat-alias universe, plus exact repeat set.

    Only an alias carried by at least two records can connect two groups, so
    the candidate scheduler only needs repeat aliases to be Bloom-positive.
    The bitmap is built over precisely that universe (materialized
    incrementally during ingestion in ``repeat_alias``): every queried repeat
    alias sets all its own positions, ruling out false negatives, while the
    much larger singleton-alias set is never hashed. At scheduling time a
    group's singleton aliases are Bloom-negative and skip exact lookup; a
    positive result (a repeat alias, or a collision in a tiny bitmap) is
    always exact-verified.
    """
    bitmap = bytearray((bits + 7) // 8)
    work.execute('CREATE TABLE hot_alias(alias TEXT PRIMARY KEY) WITHOUT ROWID')
    prefixes = _position_prefixes(hashes)
    # alias-leading index makes both counts ordered scans without a sort
    distinct_count = work.execute(
        'SELECT COUNT(*) FROM (SELECT DISTINCT alias FROM rec_aliases)').fetchone()[0]
    repeat_count = work.execute('SELECT COUNT(*) FROM repeat_alias').fetchone()[0]
    cursor = work.execute('SELECT alias FROM repeat_alias')
    hot_batch = []
    while True:
        chunk = cursor.fetchmany(INSERT_FLUSH)
        if not chunk:
            break
        for (alias,) in chunk:
            for pos in _positions_of(alias.encode('utf-8'), prefixes, bits):
                bitmap[pos // 8] |= 1 << (pos % 8)
            hot_batch.append((alias,))
            if len(hot_batch) >= INSERT_FLUSH:
                work.executemany('INSERT INTO hot_alias(alias) VALUES(?)', hot_batch)
                hot_batch.clear()
    if hot_batch:
        work.executemany('INSERT INTO hot_alias(alias) VALUES(?)', hot_batch)
    return bitmap, distinct_count, repeat_count, prefixes


# --------------------------------------------------------------------------- #
# Phase 4: constrained greedy merge with scheduler feedback
# --------------------------------------------------------------------------- #

def _seed_groups(work):
    work.execute(
        'INSERT INTO groups(gid, anchor)'
        ' SELECT rid, anchor FROM records WHERE active = 1')
    work.execute('INSERT INTO live(gid) SELECT rid FROM records WHERE active = 1')
    work.execute(
        'INSERT INTO group_sources(gid, source)'
        ' SELECT rid, source FROM records WHERE active = 1')
    work.execute(
        'INSERT OR IGNORE INTO alias_group(alias, gid)'
        ' SELECT alias, rid FROM rec_aliases a WHERE EXISTS'
        ' (SELECT 1 FROM records r WHERE r.rid = a.rid AND r.active = 1)')
    work.execute(
        'INSERT INTO members(rid, gid)'
        ' SELECT rid, rid FROM records WHERE active = 1')


def _bloom_contains(bitmap, encoded, prefixes, bits):
    """Membership test with the filtering.positions hash contract."""
    for pos in _positions_of(encoded, prefixes, bits):
        if not (bitmap[pos // 8] & (1 << (pos % 8))):
            return False
    return True


def _smallest_mergeable(work, gid, anchor, bitmap, prefixes, bits, stats):
    """Smallest current mergeable neighbour of group ``gid`` (exact alias).

    Every alias of the group is streamed through the Bloom prefilter first:
    Bloom-negative aliases (the singleton universe) skip exact lookup; only
    Bloom-positive aliases reach an exact ``alias_group`` probe, which is what
    prevents a Bloom false positive from creating a candidate. Each exact
    probe walks the ``(alias, gid)`` primary key in gid order and stops at the
    first constraint-passing row (``LIMIT 1``). The Python-side minimum over
    the streamed aliases is one deduped candidate pair, however many aliases
    connect the two groups. Alias rows are streamed in chunks, so even a
    heavily merged group's alias union never sits in Python.
    """
    stats['candidate_queries'] += 1
    alias_cursor = work.execute('SELECT alias FROM alias_group WHERE gid = ?', (gid,))
    winner = None
    while True:
        aliases = alias_cursor.fetchmany(64)
        if not aliases:
            break
        for (alias,) in aliases:
            if not _bloom_contains(bitmap, alias.encode('utf-8'), prefixes, bits):
                stats['bloom_negative_lookups'] += 1
                continue  # negative: skip the exact alias lookup
            stats['bloom_positive_lookups'] += 1
            # Candidates must be scheduler-live: retired (finalized) groups
            # keep alias rows for the Bloom gate but can never merge again.
            row = work.execute(
                'SELECT ag.gid FROM alias_group ag JOIN groups gr ON gr.gid = ag.gid'
                ' WHERE ag.alias = ? AND ag.gid > ?'
                ' AND EXISTS (SELECT 1 FROM live l WHERE l.gid = ag.gid)'
                ' AND (gr.anchor IS NULL OR ? IS NULL OR gr.anchor = ?)'
                ' AND NOT EXISTS (SELECT 1 FROM group_sources cand'
                '                 WHERE cand.gid = ag.gid AND EXISTS'
                '                 (SELECT 1 FROM group_sources ours'
                '                  WHERE ours.gid = ? AND ours.source = cand.source))'
                ' ORDER BY ag.gid LIMIT 1',
                (alias, gid, anchor, anchor, gid)).fetchone()
            if row and (winner is None or row[0] < winner):
                winner = row[0]
    return winner


def _merge(work, keep, absorb):
    """Fold group ``absorb`` into ``keep`` and feed the union state back."""
    work.execute('UPDATE members SET gid = ? WHERE gid = ?', (keep, absorb))
    work.execute(
        'INSERT OR IGNORE INTO group_sources(gid, source) SELECT ?, source FROM group_sources WHERE gid = ?',
        (keep, absorb))
    work.execute(
        'INSERT OR IGNORE INTO alias_group(alias, gid) SELECT alias, ? FROM alias_group WHERE gid = ?',
        (keep, absorb))
    # A non-null anchor propagates into a still-null group; conflicting
    # non-null anchors are never mergeable so they never reach this point.
    work.execute('UPDATE groups SET anchor = (SELECT anchor FROM groups WHERE gid = ?)'
                 ' WHERE gid = ? AND anchor IS NULL', (absorb, keep))
    work.execute('DELETE FROM group_sources WHERE gid = ?', (absorb,))
    work.execute('DELETE FROM alias_group WHERE gid = ?', (absorb,))
    work.execute('DELETE FROM live WHERE gid = ?', (absorb,))
    work.execute('DELETE FROM groups WHERE gid = ?', (absorb,))


def _bulk_retire_unschedulable(work):
    """Drop scheduler state for groups that can never share an alias.

    A group with no repeat (hot) alias has no exact alias partner anywhere, so
    it can never become a candidate: such groups leave the ``live`` walk set
    and the source-constraint table in set-based statements instead of costing
    a scheduler round each. Their ``alias_group`` rows are retained so the
    Bloom gate still observes (and skips) singleton aliases for surviving
    groups; ``groups``/``members`` rows are always retained as final entities.
    """
    work.execute(
        'DELETE FROM group_sources WHERE gid NOT IN'
        ' (SELECT DISTINCT gid FROM alias_group ag'
        '  WHERE EXISTS (SELECT 1 FROM hot_alias h WHERE h.alias = ag.alias))')
    work.execute(
        'DELETE FROM live WHERE gid NOT IN'
        ' (SELECT DISTINCT gid FROM alias_group ag'
        '  WHERE EXISTS (SELECT 1 FROM hot_alias h WHERE h.alias = ag.alias))')


def _retire(work, gid):
    """The smallest group can never gain new mergeable neighbours later."""
    work.execute('DELETE FROM group_sources WHERE gid = ?', (gid,))
    work.execute('DELETE FROM live WHERE gid = ?', (gid,))


def _merge_all(work, bitmap, prefixes, bits, stats):
    stats['merges'] = 0
    stats['candidate_queries'] = 0
    stats['bloom_positive_lookups'] = 0
    stats['bloom_negative_lookups'] = 0
    _bulk_retire_unschedulable(work)
    # Walk only scheduler-live group IDs. ``live`` is keyed by gid, so the
    # keyset query is an index seek with no scan over finalized groups.
    last = ''
    while True:
        row = work.execute(
            'SELECT l.gid, gr.anchor FROM live l JOIN groups gr ON gr.gid = l.gid'
            ' WHERE l.gid > ? ORDER BY l.gid LIMIT 1', (last,)).fetchone()
        if row is None:
            break
        gid, anchor = row
        other = _smallest_mergeable(work, gid, anchor, bitmap, prefixes, bits, stats)
        if other is None:
            _retire(work, gid)
            last = gid
            continue
        _merge(work, gid, other)
        stats['merges'] += 1
        # Stay on the same (still smallest) group; its merged state decides the
        # next round's scheduling and mergeability, never a precomputed edge.


# --------------------------------------------------------------------------- #
# Phase 5: final entities, blocking evidence and streaming output
# --------------------------------------------------------------------------- #

def _finalize_entities(work):
    work.execute('CREATE TABLE entity_source(gid TEXT NOT NULL, source TEXT NOT NULL,'
                 ' PRIMARY KEY(gid, source)) WITHOUT ROWID')
    work.execute(
        'INSERT INTO entity_source(gid, source)'
        ' SELECT DISTINCT m.gid, r.source FROM members m JOIN records r ON r.rid = m.rid')
    work.execute('CREATE TABLE entity_alias(gid TEXT NOT NULL, alias TEXT NOT NULL,'
                 ' PRIMARY KEY(gid, alias)) WITHOUT ROWID')
    work.execute(
        'INSERT OR IGNORE INTO entity_alias(gid, alias)'
        ' SELECT DISTINCT m.gid, a.alias FROM members m'
        ' JOIN rec_aliases a ON a.rid = m.rid')
    # The blocking-evidence self-join looks up partners by alias; the primary
    # key leads with gid, so an alias-leading index keeps it an index seek
    # instead of an O(n^2) cross scan on high-frequency aliases.
    work.execute('CREATE INDEX entity_alias_by_alias ON entity_alias(alias)')
    work.execute('CREATE INDEX entity_source_by_source ON entity_source(source)')


def _blocking_evidence(work, gid, anchor, other):
    """Evidence for the already-chosen smallest alias-sharing final group."""
    other_anchor = work.execute('SELECT anchor FROM groups WHERE gid = ?', (other,)).fetchone()[0]
    if anchor is not None and other_anchor is not None and anchor != other_anchor:
        return {'entity': other, 'kind': 'anchor',
                'value': sorted((anchor, other_anchor))}
    shared = work.execute(
        'SELECT MIN(s1.source) FROM entity_source s1 JOIN entity_source s2'
        ' ON s2.source = s1.source WHERE s1.gid = ? AND s2.gid = ?',
        (gid, other)).fetchone()[0]
    return {'entity': other, 'kind': 'source', 'value': shared}


def _write_result(output, work, meta, total, active, inactive, stats):
    entity_count = work.execute('SELECT COUNT(*) FROM groups').fetchone()[0]
    # One set-based statement computes each group's smallest alias-sharing
    # final group (the entity_alias primary key drives the ordered join);
    # isolated groups simply get no blocker row and report null evidence.
    work.execute('CREATE TEMPORARY TABLE blocked(gid TEXT PRIMARY KEY, other TEXT NOT NULL)'
                 ' WITHOUT ROWID')
    if entity_count:
        work.execute(
            'INSERT INTO blocked(gid, other)'
            ' SELECT ea1.gid, MIN(ea2.gid) FROM entity_alias ea1 JOIN entity_alias ea2'
            ' ON ea2.alias = ea1.alias AND ea2.gid != ea1.gid GROUP BY ea1.gid')
    # Members stream from one (gid, rid)-ordered cursor alongside the
    # gid-ordered entity cursor: only the current group's members are held in
    # Python, so the full result is never materialized in a container.
    member_cursor = work.execute(
        'SELECT m.gid, m.rid, r.source FROM members m JOIN records r ON r.rid = m.rid'
        ' ORDER BY m.gid, m.rid')
    member_buffer, member_index = member_cursor.fetchmany(500), 0

    def next_member():
        nonlocal member_buffer, member_index
        while member_index >= len(member_buffer):
            member_buffer = member_cursor.fetchmany(500)
            member_index = 0
            if not member_buffer:
                return None
        row = member_buffer[member_index]
        member_index += 1
        return row

    with jsonl_sink(output) as emit:
        emit({'type': 'summary', 'commit_seq': meta['commit_seq'], 'total': total,
              'active': active, 'inactive': inactive, 'entities': entity_count})
        entity_cursor = work.execute('SELECT gid, anchor FROM groups ORDER BY gid')
        member_row = next_member()
        while True:
            entities = entity_cursor.fetchmany(500)
            if not entities:
                break
            for gid, anchor in entities:
                members = []
                while member_row is not None and member_row[0] == gid:
                    members.append({'rid': member_row[1], 'source': member_row[2]})
                    member_row = next_member()
                blocked = work.execute(
                    'SELECT other FROM blocked WHERE gid = ?', (gid,)).fetchone()
                evidence = _blocking_evidence(work, gid, anchor, blocked[0]) if blocked else None
                emit({'type': 'entity', 'entity': gid, 'members': members,
                      'anchor': anchor, 'blocked_with': evidence})
    stats['entities'] = entity_count


def _table_sizes(work):
    sizes = {}
    for table in ('records', 'rec_aliases', 'repeat_alias', 'groups', 'live',
                  'group_sources', 'alias_group', 'members',
                  'entity_alias', 'entity_source', 'hot_alias'):
        try:
            sizes[table] = work.execute('SELECT COUNT(*) FROM %s' % table).fetchone()[0]
        except sqlite3.Error:
            sizes[table] = None
    return sizes


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #

def resolve(db_path, control_path, output_path, stats_path=None):
    """Run the full pipeline; raises ResolveError on any contract violation."""
    if os.path.islink(db_path):
        raise ResolveError('database cannot be a symlink')
    if not Path(db_path).is_file():
        raise ResolveError('database not found: ' + str(db_path))
    if os.path.islink(output_path):
        raise ResolveError('output cannot be a symlink')
    control, paths = _load_control(control_path)
    db_resolved = Path(db_path).resolve()
    output_resolved = Path(output_path).resolve()
    if output_resolved == db_resolved or output_resolved == control:
        raise ResolveError('output path must differ from the database and control file')
    for _label, resolved in paths:
        if output_resolved == resolved:
            raise ResolveError('output path must differ from every input file')
        if db_resolved == resolved:
            raise ResolveError('database path must differ from every input file')
        if not resolved.is_file():
            raise ResolveError('input file not found: ' + str(resolved))

    workdir = Path(tempfile.mkdtemp(prefix='.resolve-work-', dir=output_resolved.parent))
    sidecars = _source_sidecars(db_path)
    before = {path: (os.path.lexists(path),
                     os.stat(path).st_size if os.path.lexists(path) else 0)
              for path in sidecars}
    member = work = None
    try:
        member, meta = _open_member_snapshot(db_path)  # snapshot pinned pre-ingestion
        work = _open_workspace(workdir)
        gate = os.environ.get('BLOOM_INDEX_RESOLVE_GATE')
        if gate:
            # Deterministic test hook: the read transaction above already
            # pinned one committed state. Signal an external actor (e.g. a
            # concurrent sync) and wait for it to commit; the pinned snapshot
            # must remain unchanged for the whole run.
            Path(gate).write_text('ready', encoding='utf-8')
            deadline = time.monotonic() + 30
            while Path(gate).exists() and time.monotonic() < deadline:
                time.sleep(0.02)
            if Path(gate).exists():
                raise ResolveError('resolve gate timed out')
        stats = {}
        total, active = _ingest(work, member, paths)
        if os.environ.get('BLOOM_INDEX_RESOLVE_AFTER_FIRST_DELAY'):
            time.sleep(float(os.environ['BLOOM_INDEX_RESOLVE_AFTER_FIRST_DELAY']))
        inactive = total - active
        if active:
            bitmap, distinct_count, repeat_count, prefixes = _build_alias_bloom(
                work, meta['bits'], meta['hashes'])
            stats['distinct_aliases'] = distinct_count
            stats['repeat_aliases'] = repeat_count
            stats['alias_candidates'] = repeat_count
            stats['bloom_positive_aliases'] = repeat_count  # each repeat alias sets all its bits
            stats['bloom_negative_aliases'] = 0  # negatives arise per scheduling lookup instead
            _seed_groups(work)
            _merge_all(work, bitmap, prefixes, meta['bits'], stats)
            _finalize_entities(work)
        else:
            stats.update(alias_candidates=0, bloom_positive_aliases=0,
                         bloom_negative_aliases=0, distinct_aliases=0,
                         repeat_aliases=0, bloom_positive_lookups=0,
                         bloom_negative_lookups=0, merges=0, candidate_queries=0)
            work.execute('CREATE TABLE hot_alias(alias TEXT PRIMARY KEY) WITHOUT ROWID')
        if os.environ.get('BLOOM_INDEX_RESOLVE_FAIL_WORK'):
            raise ResolveError('injected workspace write failure')
        work.execute('COMMIT')
        stats.update(commit_seq=meta['commit_seq'], total=total, active=active,
                     inactive=inactive, bits=meta['bits'], hashes=meta['hashes'],
                     table_rows=_table_sizes(work))
        if os.environ.get('BLOOM_INDEX_RESOLVE_FAIL_OUTPUT'):
            raise ResolveError('injected output write failure')
        # Observability is published before the atomic result replacement, so a
        # failure here still leaves any previous output byte-for-byte intact.
        if stats_path is not None:
            Path(stats_path).write_text(
                json.dumps(stats, sort_keys=True, ensure_ascii=False, allow_nan=False, indent=2)
                + '\n', encoding='utf-8')
        _write_result(output_path, work, meta, total, active, inactive, stats)
        return stats
    finally:
        if work is not None:
            work.close()
        if member is not None:
            member.close()
            _release_readonly_artifacts(db_path, before)
        shutil.rmtree(workdir, ignore_errors=True)
