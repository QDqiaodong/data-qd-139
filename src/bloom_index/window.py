"""Event-time window materialization with correctable multi-partition streams.

The state is a standalone SQLite database (``STATE``) that never reads the
membership index or any entity-resolution result.  It permanently binds the
configured partition set, the window width and the absolute report path when
it is created, and owns four strictly separated responsibilities:

1. **Change ingestion / identity revision** -- strict JSONL rows are parsed and
   applied inside one SQLite transaction.  Each partition's sequence numbers
   are contiguous credentials: a historical sequence is only skipped when the
   exact raw line bytes (without the trailing LF) equal the accepted
   credential; anything else rejects the whole batch.  An ``eid`` is bound to
   the partition of its first ``put`` forever and every revision must be the
   previous revision plus one.
2. **Window materialization / sealing** -- alive events aggregate into exact
   integer ``(key, floor(ts/width)*width)`` buckets.  A correction retracts the
   old contribution before adding the new one; a delete retracts the current
   contribution.  A batch is rejected if it touches a window that was already
   final at the batch's start (global watermark = minimum over every
   configured partition, including silent ones).
3. **Recoverable report publication** -- the committed epoch, sequence
   credentials, watermarks and materialized windows live in the same database
   transaction.  The report is a pure, deterministic function of that
   committed state, so an interrupted publication is finished by rebuilding
   the same bytes from the database.
4. **Single-writer recovery** -- an advisory ``flock`` on the state file spans
   recovery, the business commit and the file publication, so a stale recovery
   can never overwrite a newer report.
"""
import fcntl
import hashlib
import json
import os
import shutil
import signal
import sqlite3
import tempfile
import time
from pathlib import Path

from .common import ident, jsonl_line, number

FORMAT_VERSION = 1
MAX_PARTITIONS = 64
MAX_FILES = 64
MAX_WIDTH = 86400
MAX_LINE_BYTES = 16384
MAX_LINES = 1_000_000
MAX_CONTROL_BYTES = 1 << 20
MAX_SEQ = 10**9
MAX_TS = 10**12
MAX_VALUE = 10**9
LOCK_TIMEOUT = 5.0
CACHE_KIB = 8000

PUT_FIELDS = ('partition', 'seq', 'op', 'eid', 'rev', 'ts', 'key', 'value')
DELETE_FIELDS = ('partition', 'seq', 'op', 'eid', 'rev')
WATERMARK_FIELDS = ('partition', 'seq', 'op', 'ts')
META_FIELDS = ('format_version', 'epoch', 'confirmed_epoch', 'width', 'output',
               'report_sha256', 'report_size')

SCHEMA = (
    "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);"
    "CREATE TABLE partitions(name TEXT PRIMARY KEY, watermark INTEGER NOT NULL,"
    " max_seq INTEGER NOT NULL) WITHOUT ROWID;"
    "CREATE TABLE seqs(partition TEXT NOT NULL, seq INTEGER NOT NULL,"
    " line BLOB NOT NULL, PRIMARY KEY(partition, seq)) WITHOUT ROWID;"
    "CREATE TABLE identities(eid TEXT PRIMARY KEY, partition TEXT NOT NULL,"
    " last_rev INTEGER NOT NULL, alive INTEGER NOT NULL, ts INTEGER,"
    " key TEXT, value INTEGER) WITHOUT ROWID;"
    "CREATE TABLE windows(key TEXT NOT NULL, start INTEGER NOT NULL,"
    " count INTEGER NOT NULL, sum INTEGER NOT NULL, PRIMARY KEY(key, start)) WITHOUT ROWID;"
)


class WindowError(ValueError):
    """Any input, state or publication defect; the CLI maps it to exit code 2."""


# --------------------------------------------------------------------------- #
# Strict parsing helpers
# --------------------------------------------------------------------------- #

def _hook(where):
    def pairs_hook(pairs):
        seen = set()
        for key, _value in pairs:
            if key in seen:
                raise WindowError('%s: duplicate JSON field: %s' % (where, key))
            seen.add(key)
        return dict(pairs)
    return pairs_hook


def _constant(where):
    def invalid(token):
        raise WindowError('%s: nonfinite JSON number: %s' % (where, token))
    return invalid


def _integer(value, where, field, minimum, maximum):
    if type(value) is not int or not minimum <= value <= maximum:
        raise WindowError('%s: %s must be an integer in %d..%d'
                          % (where, field, minimum, maximum))
    return value


def _identifier(value, where, field):
    try:
        return ident(value)
    except (ValueError, TypeError):
        raise WindowError('%s: %s must be a nonblank identifier, <=128 characters'
                          % (where, field))


def _exact(obj, fields, where):
    if not isinstance(obj, dict) or set(obj) != set(fields):
        raise WindowError('%s: expected exactly fields %s'
                          % (where, ', '.join(fields)))


def _parse_partitions(text):
    if not isinstance(text, str):
        raise WindowError('partitions must be a comma separated identifier list')
    names = text.split(',')
    if not 1 <= len(names) <= MAX_PARTITIONS:
        raise WindowError('partitions must contain 1..%d identifiers' % MAX_PARTITIONS)
    result = []
    for name in names:
        try:
            ident(name)
        except (ValueError, TypeError):
            raise WindowError('invalid partition identifier: %r' % (name,))
        if name in result:
            raise WindowError('duplicate partition identifier: %s' % name)
        result.append(name)
    return result


def _load_control(control_path):
    control = Path(control_path)
    if not control.is_file():
        raise WindowError('control file not found: ' + str(control))

    def unique(pairs):
        seen = set()
        for key, _value in pairs:
            if key in seen:
                raise WindowError('control: duplicate JSON key: ' + key)
            seen.add(key)
        return dict(pairs)

    def invalid(token):
        raise WindowError('control: nonfinite JSON number: ' + token)

    try:
        with control.open('rb') as stream:
            raw = stream.read(MAX_CONTROL_BYTES + 1)
    except OSError as error:
        raise WindowError('control file cannot be read: ' + str(error))
    if len(raw) > MAX_CONTROL_BYTES:
        raise WindowError('control file exceeds %d bytes' % MAX_CONTROL_BYTES)
    try:
        obj = json.loads(raw.decode('utf-8'), object_pairs_hook=unique,
                         parse_constant=invalid)
    except UnicodeDecodeError as error:
        raise WindowError('control file is not valid UTF-8: ' + str(error))
    except json.JSONDecodeError as error:
        raise WindowError('control file is not valid JSON: ' + str(error))
    if not isinstance(obj, dict) or set(obj) != {'files'}:
        raise WindowError('control must be an object with exactly one field: files')
    files = obj['files']
    if not isinstance(files, list) or not 1 <= len(files) <= MAX_FILES:
        raise WindowError('files must be a list of 1..%d paths' % MAX_FILES)
    base = control.resolve().parent
    seen_text, seen_resolved, paths = set(), set(), []
    for entry in files:
        if not isinstance(entry, str) or not entry:
            raise WindowError('each files entry must be a nonempty path string')
        if entry in seen_text:
            raise WindowError('duplicate input file in control: ' + entry)
        candidate = Path(entry)
        resolved = (candidate if candidate.is_absolute() else base / candidate).resolve()
        if resolved in seen_resolved:
            raise WindowError('duplicate input file in control: ' + entry)
        if not resolved.is_file():
            raise WindowError('input file not found: ' + str(resolved))
        seen_text.add(entry)
        seen_resolved.add(resolved)
        paths.append((entry, resolved))
    return control.resolve(), paths


# --------------------------------------------------------------------------- #
# State database
# --------------------------------------------------------------------------- #

def _connect(path):
    conn = sqlite3.connect(str(path), timeout=LOCK_TIMEOUT, isolation_level=None)
    conn.execute('PRAGMA journal_mode=DELETE')
    conn.execute('PRAGMA synchronous=FULL')
    conn.execute('PRAGMA temp_store=FILE')
    conn.execute('PRAGMA cache_size=-%d' % CACHE_KIB)
    conn.execute('PRAGMA mmap_size=0')
    return conn


def _begin(conn):
    conn.execute('BEGIN IMMEDIATE')


def _commit(conn):
    conn.execute('COMMIT')


def _rollback(conn):
    try:
        conn.execute('ROLLBACK')
    except sqlite3.Error:
        pass


def _meta_int(rows, key, minimum, maximum):
    value = rows[key]
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise WindowError('state meta.%s is not an integer' % key)
    if str(parsed) != str(value) or not minimum <= parsed <= maximum:
        raise WindowError('state meta.%s is outside allowed bounds' % key)
    return parsed


def _read_meta(conn):
    try:
        rows = dict(conn.execute('SELECT key, value FROM meta').fetchall())
    except sqlite3.Error:
        raise WindowError('state is not a window state database')
    if set(rows) != set(META_FIELDS):
        raise WindowError('state meta must hold exactly %s' % ', '.join(META_FIELDS))
    if rows['format_version'] != str(FORMAT_VERSION):
        raise WindowError('unsupported state format_version: %r' % (rows['format_version'],))
    epoch = _meta_int(rows, 'epoch', 0, 10**15)
    confirmed = _meta_int(rows, 'confirmed_epoch', 0, 10**15)
    if confirmed > epoch:
        raise WindowError('state confirmed_epoch is ahead of epoch')
    width = _meta_int(rows, 'width', 1, MAX_WIDTH)
    output = rows['output']
    if not isinstance(output, str) or not output or not os.path.isabs(output):
        raise WindowError('state output must be an absolute path')
    digest = rows['report_sha256']
    if not isinstance(digest, str) or (digest and (len(digest) != 64
                                                   or any(c not in '0123456789abcdef' for c in digest))):
        raise WindowError('state report_sha256 is invalid')
    size = _meta_int(rows, 'report_size', 0, 10**15)
    partitions = conn.execute(
        'SELECT name, watermark, max_seq FROM partitions ORDER BY name').fetchall()
    if not 1 <= len(partitions) <= MAX_PARTITIONS:
        raise WindowError('state must configure 1..%d partitions' % MAX_PARTITIONS)
    watermarks = {}
    for name, watermark, max_seq in partitions:
        try:
            ident(name)
        except (ValueError, TypeError):
            raise WindowError('state holds an invalid partition identifier')
        if type(watermark) is not int or not 0 <= watermark <= MAX_TS:
            raise WindowError('partition %s watermark is invalid' % name)
        if type(max_seq) is not int or not 0 <= max_seq <= MAX_SEQ:
            raise WindowError('partition %s max_seq is invalid' % name)
        watermarks[name] = (watermark, max_seq)
    return {'format_version': FORMAT_VERSION, 'epoch': epoch,
            'confirmed_epoch': confirmed, 'width': width, 'output': output,
            'report_sha256': digest, 'report_size': size, 'partitions': watermarks}


def _open_state(state_path):
    state = Path(state_path)
    if os.path.islink(state):
        raise WindowError('state cannot be a symlink')
    if not state.is_file():
        raise WindowError('state not found: ' + str(state))
    return state


def _lock_state(state, timeout=LOCK_TIMEOUT):
    """Exclusive advisory writer lock spanning recovery, commit and publication."""
    fd = os.open(str(state), os.O_RDWR)
    deadline = time.monotonic() + timeout
    try:
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return fd
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise WindowError('another window writer holds the state lock (timeout)')
                time.sleep(0.05)
    except BaseException:
        os.close(fd)
        raise


def _unlock_state(fd):
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


# --------------------------------------------------------------------------- #
# Report publication (recoverable lifecycle)
# --------------------------------------------------------------------------- #

def _crash(point, enabled=True):
    if enabled and os.environ.get('BLOOM_INDEX_WINDOW_CRASH') == point:
        os.kill(os.getpid(), signal.SIGKILL)


def _sha256_file(path):
    hasher = hashlib.sha256()
    with open(path, 'rb') as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b''):
            hasher.update(chunk)
    return hasher.hexdigest()


def _fsync_dir(path):
    try:
        fd = os.open(str(path), os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _cleanup_temps(dest):
    """Remove stale hidden report staging files left by a killed publication."""
    if not dest.parent.is_dir():
        return
    pattern = '.' + dest.name + '.window-*'
    for stale in dest.parent.glob(pattern):
        if stale.is_dir() and not stale.is_symlink():
            shutil.rmtree(stale, ignore_errors=True)
        else:
            try:
                os.unlink(stale)
            except OSError:
                pass


def _publish_report(conn, meta, allow_crash=True):
    """Write the report for the committed state to its bound absolute path.

    Returns the SHA256 of the exact bytes published.  The report is purely
    derived from the committed database, so calling this again always produces
    identical bytes.  Any ordinary exception removes this invocation's staging
    file and leaves the previous report byte-for-byte intact.
    """
    dest = Path(meta['output'])
    if dest.is_symlink():
        raise WindowError('report cannot be a symlink')
    dest.parent.mkdir(parents=True, exist_ok=True)
    _cleanup_temps(dest)
    fd, temp = tempfile.mkstemp(prefix='.' + dest.name + '.window-', dir=dest.parent)
    digest = hashlib.sha256()
    replaced = False
    try:
        with os.fdopen(fd, 'w', encoding='utf-8', newline='\n') as handle:
            def emit(obj):
                text = jsonl_line(obj)
                handle.write(text)
                digest.update(text.encode('utf-8'))

            partitions = conn.execute(
                'SELECT name, watermark FROM partitions ORDER BY name').fetchall()
            if not partitions:
                raise WindowError('state configures no partitions')
            watermark = min(value for _name, value in partitions)
            emit({'type': 'summary', 'epoch': meta['epoch'], 'watermark': watermark,
                  'partitions': {name: value for name, value in partitions}})
            cursor = conn.execute(
                'SELECT key, start, count, sum FROM windows ORDER BY key, start')
            fail_after = int(os.environ.get('BLOOM_INDEX_WINDOW_FAIL_AFTER', '0') or 0)
            emitted = 0
            for key, start, count, total in cursor:
                emit({'type': 'window', 'key': key, 'start': start,
                      'end': start + meta['width'], 'count': count, 'sum': total,
                      'final': start + meta['width'] <= watermark})
                emitted += 1
                if emitted == 1:
                    handle.flush()
                    _crash('mid_report')
                if fail_after and emitted > fail_after:
                    handle.flush()
                    raise OSError('injected report write failure after %d windows' % fail_after)
                if not fail_after and os.environ.get('BLOOM_INDEX_WINDOW_FAIL_REPORT'):
                    handle.flush()
                    raise OSError('injected report write failure')
            handle.flush()
            os.fsync(handle.fileno())
        _crash('before_replace', allow_crash)
        os.chmod(temp, 0o644)
        os.replace(temp, dest)
        replaced = True
        _crash('after_replace', allow_crash)
        _fsync_dir(dest.parent)
    except BaseException:
        if not replaced and os.path.exists(temp):
            try:
                os.unlink(temp)
            except OSError:
                pass
        raise
    return digest.hexdigest(), os.path.getsize(dest)


def _confirm(conn, epoch, digest, size, allow_crash=True):
    _crash('before_confirm', allow_crash)
    conn.execute('BEGIN IMMEDIATE')
    try:
        conn.execute("UPDATE meta SET value=? WHERE key='confirmed_epoch'", (str(epoch),))
        conn.execute("UPDATE meta SET value=? WHERE key='report_sha256'", (digest,))
        conn.execute("UPDATE meta SET value=? WHERE key='report_size'", (str(size),))
        conn.execute('COMMIT')
    except BaseException:
        _rollback(conn)
        raise


def _recovery_pending(meta):
    """True when the committed epoch is not yet represented by the report file.

    Publication is confirmed inside the same database only after the atomic
    replacement, so ``epoch > confirmed_epoch`` always means a pending batch.
    A missing or size-divergent file also forces a rebuild; detecting external
    byte tampering beyond that is not part of this contract.
    """
    dest = Path(meta['output'])
    if dest.is_symlink():
        raise WindowError('report cannot be a symlink')
    if meta['epoch'] != meta['confirmed_epoch']:
        return True
    try:
        return (not dest.is_file()) or dest.stat().st_size != meta['report_size']
    except OSError:
        return True


def _recover_locked(conn, meta):
    """Finish an interrupted publication; idempotent when nothing is pending."""
    dest = Path(meta['output'])
    if dest.is_symlink():
        raise WindowError('report cannot be a symlink')
    needed = _recovery_pending(meta)
    if not needed:
        _cleanup_temps(dest)
        return False
    digest, size = _publish_report(conn, meta)
    _confirm(conn, meta['epoch'], digest, size)
    return True


# --------------------------------------------------------------------------- #
# Change ingestion and window materialization
# --------------------------------------------------------------------------- #

def _window_start(ts, width):
    return (ts // width) * width


def _add_contribution(conn, key, start, value):
    conn.execute(
        'INSERT INTO windows(key, start, count, sum) VALUES(?, ?, 1, ?)'
        ' ON CONFLICT(key, start) DO UPDATE SET count = count + 1, sum = sum + excluded.sum',
        (key, start, value))


def _remove_contribution(conn, key, start, value):
    conn.execute('UPDATE windows SET count = count - 1, sum = sum - ? WHERE key = ? AND start = ?',
                 (value, key, start))
    conn.execute('DELETE FROM windows WHERE key = ? AND start = ? AND count = 0', (key, start))


def _apply_put(conn, where, partition, eid, rev, ts, key, value,
               width, start_watermark, final_at_start):
    if ts < start_watermark:
        raise WindowError('%s: new put ts %d is below partition watermark %d'
                          % (where, ts, start_watermark))
    new_start = _window_start(ts, width)
    if final_at_start(new_start):
        raise WindowError('%s: new window start=%d is already final at batch start'
                          % (where, new_start))
    row = conn.execute(
        'SELECT partition, last_rev, alive, ts, key, value FROM identities WHERE eid = ?',
        (eid,)).fetchone()
    if row is None:
        if rev != 1:
            raise WindowError('%s: first revision for eid %r must be 1, got %d'
                              % (where, eid, rev))
        conn.execute(
            'INSERT INTO identities(eid, partition, last_rev, alive, ts, key, value)'
            ' VALUES(?, ?, 1, 1, ?, ?, ?)', (eid, partition, ts, key, value))
        _add_contribution(conn, key, new_start, value)
        return
    bound, last_rev, alive, old_ts, old_key, old_value = row
    if bound != partition:
        raise WindowError('%s: eid %r is bound to partition %r, not %r'
                          % (where, eid, bound, partition))
    if rev != last_rev + 1:
        raise WindowError('%s: eid %r expects revision %d, got %d'
                          % (where, eid, last_rev + 1, rev))
    if alive:
        old_start = _window_start(old_ts, width)
        if final_at_start(old_start):
            raise WindowError('%s: correction touches already final window start=%d'
                              % (where, old_start))
        _remove_contribution(conn, old_key, old_start, old_value)
    _add_contribution(conn, key, new_start, value)
    conn.execute(
        'UPDATE identities SET last_rev=?, alive=1, ts=?, key=?, value=? WHERE eid=?',
        (rev, ts, key, value, eid))


def _apply_delete(conn, where, partition, eid, rev, width, final_at_start):
    row = conn.execute(
        'SELECT partition, last_rev, alive, ts, key, value FROM identities WHERE eid = ?',
        (eid,)).fetchone()
    if row is None:
        raise WindowError('%s: delete references unknown eid %r' % (where, eid))
    bound, last_rev, alive, ts, key, value = row
    if bound != partition:
        raise WindowError('%s: eid %r is bound to partition %r, not %r'
                          % (where, eid, bound, partition))
    if rev != last_rev + 1:
        raise WindowError('%s: eid %r expects revision %d, got %d'
                          % (where, eid, last_rev + 1, rev))
    if not alive:
        raise WindowError('%s: eid %r is not alive and cannot be deleted again'
                          % (where, eid))
    old_start = _window_start(ts, width)
    if final_at_start(old_start):
        raise WindowError('%s: delete touches already final window start=%d'
                          % (where, old_start))
    _remove_contribution(conn, key, old_start, value)
    conn.execute('UPDATE identities SET last_rev=?, alive=0 WHERE eid=?', (rev, eid))


def _validate_row(obj, where, partitions):
    if not isinstance(obj, dict):
        raise WindowError('%s: expected a JSON object' % where)
    op = obj.get('op')
    if op == 'put':
        fields = PUT_FIELDS
    elif op == 'delete':
        fields = DELETE_FIELDS
    elif op == 'watermark':
        fields = WATERMARK_FIELDS
    else:
        raise WindowError('%s: op must be "put", "delete" or "watermark"' % where)
    _exact(obj, fields, where)
    partition = _identifier(obj['partition'], where, 'partition')
    if partition not in partitions:
        raise WindowError('%s: partition %r is not configured in this state'
                          % (where, partition))
    seq = _integer(obj['seq'], where, 'seq', 1, MAX_SEQ)
    if op == 'watermark':
        ts = _integer(obj['ts'], where, 'ts', 0, MAX_TS)
        return {'op': op, 'partition': partition, 'seq': seq, 'ts': ts}
    eid = _identifier(obj['eid'], where, 'eid')
    rev = _integer(obj['rev'], where, 'rev', 1, MAX_SEQ)
    if op == 'delete':
        return {'op': op, 'partition': partition, 'seq': seq, 'eid': eid, 'rev': rev}
    ts = _integer(obj['ts'], where, 'ts', 0, MAX_TS)
    key = _identifier(obj['key'], where, 'key')
    value = _integer(obj['value'], where, 'value', -MAX_VALUE, MAX_VALUE)
    return {'op': op, 'partition': partition, 'seq': seq, 'eid': eid,
            'rev': rev, 'ts': ts, 'key': key, 'value': value}


def _bounded_lines(stream, limit):
    """Yield raw lines without ever buffering more than ``limit`` bytes.

    A line whose body exceeds the contract is rejected before it can grow the
    process; normal lines are returned including their trailing LF, if any.
    """
    buffer = b''
    while True:
        chunk = stream.readline(limit + 2)
        if not chunk:
            if buffer:
                yield buffer
            return
        buffer += chunk
        if buffer.endswith(b'\n'):
            yield buffer
            buffer = b''
        elif len(buffer) > limit + 1:
            yield buffer
            buffer = b''


def _apply_batch(conn, meta, paths):
    """Stream one batch inside the caller's transaction; raise on any defect."""
    width = meta['width']
    partitions = {name: max_seq for name, (_wm, max_seq) in meta['partitions'].items()}
    start_watermarks = {name: wm for name, (wm, _seq) in meta['partitions'].items()}
    global_start = min(start_watermarks.values())

    def final_at_start(start):
        return start + width <= global_start

    pending_watermarks = {}
    new_seqs = replays = 0
    total = 0
    for label, resolved in paths:
        try:
            stream = open(resolved, 'rb')
        except OSError as error:
            raise WindowError('%s: cannot open input file: %s' % (label, error))
        line_no = 0
        with stream:
            for raw in _bounded_lines(stream, MAX_LINE_BYTES):
                line_no += 1
                total += 1
                where = '%s:%d' % (label, line_no)
                if total > MAX_LINES:
                    raise WindowError('%s: batch exceeds %d lines' % (where, MAX_LINES))
                body = raw[:-1] if raw.endswith(b'\n') else raw
                if len(body) > MAX_LINE_BYTES:
                    raise WindowError('%s: line exceeds %d bytes' % (where, MAX_LINE_BYTES))
                if not body:
                    raise WindowError('%s: empty line' % where)
                try:
                    text = body.decode('utf-8')
                except UnicodeDecodeError:
                    raise WindowError('%s: line is not valid UTF-8' % where)
                try:
                    obj = json.loads(text, object_pairs_hook=_hook(where),
                                     parse_constant=_constant(where))
                except json.JSONDecodeError as error:
                    raise WindowError('%s: invalid JSON: %s' % (where, error))
                row = _validate_row(obj, where, start_watermarks)
                partition, seq = row['partition'], row['seq']
                expected = partitions[partition] + 1
                if seq < expected:
                    stored = conn.execute(
                        'SELECT line FROM seqs WHERE partition = ? AND seq = ?',
                        (partition, seq)).fetchone()
                    if stored is None:
                        raise WindowError(
                            '%s: partition %r skips seq %d (expected %d)'
                            % (where, partition, seq, expected))
                    if stored[0] != body:
                        raise WindowError(
                            '%s: seq %d for partition %r was already accepted with different bytes'
                            % (where, seq, partition))
                    replays += 1
                    continue
                if seq > expected:
                    raise WindowError(
                        '%s: partition %r expects seq %d, got %d'
                        % (where, partition, expected, seq))
                if row['op'] == 'put':
                    _apply_put(conn, where, partition, row['eid'], row['rev'],
                               row['ts'], row['key'], row['value'], width,
                               start_watermarks[partition], final_at_start)
                elif row['op'] == 'delete':
                    _apply_delete(conn, where, partition, row['eid'], row['rev'],
                                  width, final_at_start)
                else:
                    watermark = row['ts']
                    floor = pending_watermarks.get(partition, start_watermarks[partition])
                    if watermark < floor:
                        raise WindowError(
                            '%s: watermark %d regresses below %d for partition %r'
                            % (where, watermark, floor, partition))
                    pending_watermarks[partition] = watermark
                conn.execute('INSERT INTO seqs(partition, seq, line) VALUES(?, ?, ?)',
                             (partition, seq, body))
                partitions[partition] = seq
                new_seqs += 1
    for name, watermark in pending_watermarks.items():
        conn.execute('UPDATE partitions SET watermark = ? WHERE name = ?', (watermark, name))
    for name, max_seq in partitions.items():
        conn.execute('UPDATE partitions SET max_seq = ? WHERE name = ? AND max_seq < ?',
                     (max_seq, name, max_seq))
    return {'new_seqs': new_seqs, 'replayed': replays, 'lines': total,
            'watermark': min(w for (w, _seq) in _partition_rows(conn))}


def _partition_rows(conn):
    return conn.execute('SELECT watermark, max_seq FROM partitions').fetchall()


# --------------------------------------------------------------------------- #
# Entry points
# --------------------------------------------------------------------------- #

def _same_path(first, second):
    return os.path.abspath(str(first)) == os.path.abspath(str(second))


def init_state(state_path, partitions_arg, width, output_path):
    width = number(width, 1, MAX_WIDTH)
    names = _parse_partitions(partitions_arg)
    state = Path(state_path)
    if os.path.islink(state) or os.path.lexists(state):
        raise WindowError('state path already exists: ' + str(state))
    output = Path(os.path.realpath(str(output_path)))
    raw_output = Path(output_path)
    for candidate in (raw_output, output):
        if os.path.islink(candidate) or os.path.lexists(candidate):
            raise WindowError('report path already exists: ' + str(candidate))
    if _same_path(state, output) or state.resolve() == output.resolve():
        raise WindowError('state and report paths must differ')
    state.parent.mkdir(parents=True, exist_ok=True)
    output.parent.mkdir(parents=True, exist_ok=True)
    if not state.parent.is_dir():
        raise WindowError('state parent is not a directory: ' + str(state.parent))
    for stale in state.parent.glob('.' + state.name + '.init-*'):
        shutil.rmtree(stale, ignore_errors=True)
    stage = Path(tempfile.mkdtemp(prefix='.' + state.name + '.init-', dir=state.parent))
    temp_db = stage / 'state.sqlite3'
    published = False
    try:
        conn = _connect(temp_db)
        try:
            _begin(conn)
            try:
                for statement in SCHEMA.split(';'):
                    if statement.strip():
                        conn.execute(statement)
                conn.executemany(
                    'INSERT INTO meta(key, value) VALUES(?, ?)',
                    [('format_version', str(FORMAT_VERSION)), ('epoch', '0'),
                     ('confirmed_epoch', '0'), ('width', str(width)),
                     ('output', str(output)), ('report_sha256', ''),
                     ('report_size', '0')])
                conn.executemany(
                    'INSERT INTO partitions(name, watermark, max_seq) VALUES(?, 0, 0)',
                    [(name,) for name in names])
                _commit(conn)
            except BaseException:
                _rollback(conn)
                raise
            meta = _read_meta(conn)
            digest, size = _publish_report(conn, meta, allow_crash=False)
            published = True
            _begin(conn)
            try:
                conn.execute("UPDATE meta SET value=? WHERE key='report_sha256'", (digest,))
                conn.execute("UPDATE meta SET value=? WHERE key='report_size'", (str(size),))
                _commit(conn)
            except BaseException:
                _rollback(conn)
                raise
        finally:
            conn.close()
        if os.path.lexists(state) or os.path.islink(state):
            raise WindowError('state path already exists: ' + str(state))
        try:
            # ``link`` publishes atomically and refuses an existing path with
            # EEXIST, unlike ``replace`` which would silently overwrite a state
            # created by a concurrent initializer.
            os.link(temp_db, state)
        except FileExistsError:
            raise WindowError('state path already exists: ' + str(state))
        _fsync_dir(state.parent)
    except BaseException:
        if published:
            try:
                if os.path.lexists(output) and not output.is_symlink():
                    os.unlink(output)
            except OSError:
                pass
        raise
    finally:
        shutil.rmtree(stage, ignore_errors=True)
    return {'epoch': 0, 'watermark': 0, 'width': width,
            'partitions': names, 'report': str(output)}


def recover_state(state_path):
    state = _open_state(state_path)
    fd = _lock_state(state)
    try:
        conn = _connect(state)
        try:
            meta = _read_meta(conn)
            recovered = _recover_locked(conn, meta)
            meta = _read_meta(conn)
            watermark = min(wm for wm, _seq in meta['partitions'].values())
        finally:
            conn.close()
    finally:
        _unlock_state(fd)
    return {'epoch': meta['epoch'], 'watermark': watermark,
            'recovered': recovered, 'partitions': sorted(meta['partitions']),
            'output': meta['output']}


def apply_control(state_path, control_path):
    state = _open_state(state_path)
    fd = _lock_state(state)
    try:
        conn = _connect(state)
        try:
            meta = _read_meta(conn)
            if Path(meta['output']).is_symlink():
                raise WindowError('report cannot be a symlink')
            # "window-apply 在接受新批前先完成恢复" and the only sanctioned
            # exception to preserving old state/report on input errors.
            _recover_locked(conn, meta)
            meta = _read_meta(conn)
            control, paths = _load_control(control_path)
            state_resolved = state.resolve()
            report_resolved = Path(meta['output']).resolve()
            if control in (state_resolved, report_resolved):
                raise WindowError('control file conflicts with the state or report path')
            for label, resolved in paths:
                if resolved in (state_resolved, report_resolved, control):
                    raise WindowError('%s: input file conflicts with the state, report or control path'
                                      % label)
            _begin(conn)
            try:
                counts = _apply_batch(conn, meta, paths)
                epoch = meta['epoch'] + 1 if counts['new_seqs'] else meta['epoch']
                if counts['new_seqs']:
                    conn.execute("UPDATE meta SET value=? WHERE key='epoch'", (str(epoch),))
                _crash('before_commit')
                _commit(conn)
            except BaseException:
                _rollback(conn)
                raise
            _crash('after_commit')
            watermark = counts['watermark']
            if counts['new_seqs']:
                meta['epoch'] = epoch
                digest, size = _publish_report(conn, meta)
                _confirm(conn, epoch, digest, size)
            counts.update(epoch=epoch, watermark=watermark)
            return counts
        finally:
            conn.close()
    finally:
        _unlock_state(fd)
