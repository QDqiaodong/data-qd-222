"""Versioned configuration consumed by service instances through one active pointer."""
from contextlib import closing
import errno
import json
import os
import re
import secrets
import sqlite3
import tempfile


class ConflictingPromotion(Exception):
    pass


_IDENT_RE = re.compile(r"[A-Za-z0-9_-]{1,64}\Z")
_BRANCH_RE = re.compile(r"[0-9a-f]{32}\Z")  # library branch identifiers
_MAX_ID = (1 << 63) - 1      # revision, pin and cursor ids are positive 63-bit integers
_BATCH_MAX = 32              # retire/pin batches accept at most this many ids
_SCAN_MAX = 64               # collect_retired inspects at most this many ids per batch
_GROUP_WINDOW = 256          # group epochs kept reconstructable, including the current one
_GROUP_MAX_COMPONENTS = 2048
_GROUP_MAX_CHANGES = 32
_GROUP_MAX_NEEDS = 8
_GROUP_MAX_ROOTS = 16
_MAX_ROSTER = 2400
_MAX_ZONES = 24
_MAX_RUNTIMES = 10
_MAX_LOAD = 1_000_000
_MAX_BUDGET = 24_000_000
# Hot reload: in-memory switchover jobs built from two retained group epochs.
_HOT_MAX_SLOTS = 128
_HOT_MAX_BODY_BYTES = 256 * 1024
_HOT_MIN_WEIGHT = 1
_HOT_MAX_WEIGHT = 1_000_000
_HOT_MAX_CAPACITY = 128_000_000


def _validate_roster(roster, max_load):
    """Validate a canary roster and return a normalized copy owned by this module."""
    if type(max_load) is not int or not (0 <= max_load <= _MAX_BUDGET):
        raise ValueError("max_load must be an integer in [0, %d]" % _MAX_BUDGET)
    if type(roster) is not list or not roster:
        raise ValueError("roster must be a non-empty list of instance tuples")
    if len(roster) > _MAX_ROSTER:
        raise ValueError("roster may contain at most %d instances" % _MAX_ROSTER)
    normalized = []
    seen_ids = set()
    zones = set()
    runtimes = set()
    for row in roster:
        if type(row) is not tuple or len(row) != 4:
            raise ValueError("each roster entry must be a (instance_id, zone, runtime, load) tuple")
        instance_id, zone, runtime, load = row
        for identifier in (instance_id, zone, runtime):
            if type(identifier) is not str or _IDENT_RE.fullmatch(identifier) is None:
                raise ValueError(
                    "identifiers must be 1-64 ASCII characters of letters, digits, '-' or '_'")
        if type(load) is not int or not (1 <= load <= _MAX_LOAD):
            raise ValueError("load must be an integer in [1, %d]" % _MAX_LOAD)
        if instance_id in seen_ids:
            raise ValueError("duplicate instance id: %r" % instance_id)
        seen_ids.add(instance_id)
        zones.add(zone)
        runtimes.add(runtime)
        normalized.append((instance_id, zone, runtime, load))
    if len(zones) > _MAX_ZONES:
        raise ValueError("roster may span at most %d zones" % _MAX_ZONES)
    if len(runtimes) > _MAX_RUNTIMES:
        raise ValueError("roster may use at most %d runtimes" % _MAX_RUNTIMES)
    return normalized


def _choose_members(roster, max_load):
    """Pick one instance per zone covering every runtime within the budget.

    Returns the selected instance ids ordered by ascending zone name, or raises
    ValueError when no feasible combination exists. Optimization order is
    minimum total load first, then the lexicographically smallest id tuple.
    """
    zones = sorted({row[1] for row in roster})
    runtimes = sorted({row[2] for row in roster})
    runtime_bit = {runtime: 1 << index for index, runtime in enumerate(runtimes)}
    full_mask = (1 << len(runtimes)) - 1

    # zone -> runtime -> Pareto front of (load, instance_id); an option is
    # dropped when another option in the same cell has both no greater load and
    # no greater instance id, since it can then never win for any predecessor.
    cells = {zone: {} for zone in zones}
    for instance_id, zone, runtime, load in roster:
        cells[zone].setdefault(runtime, []).append((load, instance_id))
    fronts = {}
    for zone in zones:
        fronts[zone] = {}
        for runtime, options in cells[zone].items():
            options.sort()  # by load, then instance id
            front = []
            smallest_id = None
            for option in options:
                instance_id = option[1]
                if smallest_id is None or instance_id < smallest_id:
                    front.append(option)
                    smallest_id = instance_id
            fronts[zone][runtime] = front

    # mask -> (total load, tuple of instance ids ordered by zone name).
    # Each zone contributes exactly one chosen instance, so every transition
    # takes one prior-zone state and one option of the current zone; runtime
    # coverage is simply unioned via the mask bit.
    dp = {0: (0, ())}
    for zone in zones:
        zone_options = []
        for runtime, options in fronts[zone].items():
            bit = runtime_bit[runtime]
            for load, instance_id in options:
                zone_options.append((bit, load, instance_id))
        next_dp = {}
        for mask, (total_load, ids) in dp.items():
            for bit, load, instance_id in zone_options:
                new_mask = mask | bit
                candidate = (total_load + load, ids + (instance_id,))
                current = next_dp.get(new_mask)
                if current is None or candidate < current:
                    next_dp[new_mask] = candidate
        dp = next_dp

    best = dp.get(full_mask)
    if best is None or best[0] > max_load:
        raise ValueError("no instance combination covers every runtime within the load budget")
    return best[1]


def _is_id(value):
    """True for an integer in [1, 2**63-1]; bools and floats are not ids."""
    return type(value) is int and 1 <= value <= _MAX_ID


def _validate_batch(revisions):
    """Normalize a retire/pin batch to a sorted tuple of distinct ids.

    The shape is checked in full before any database access, so a malformed
    batch can never leave a partial registration behind.
    """
    if type(revisions) is not tuple or not (1 <= len(revisions) <= _BATCH_MAX):
        raise ValueError(
            "revisions must be a tuple of 1-%d distinct revision ids" % _BATCH_MAX)
    for revision in revisions:
        if not _is_id(revision):
            raise ValueError(
                "revision ids must be integers in [1, %d]" % _MAX_ID)
    if len(set(revisions)) != len(revisions):
        raise ValueError("revision ids in a batch must be distinct")
    return tuple(sorted(revisions))


def _check_id(value, label):
    if not _is_id(value):
        raise ValueError("%s must be an integer in [1, %d]" % (label, _MAX_ID))
    return value


def _revision_high_water(db):
    """Highest version id ever issued, including ids no longer present.

    ``revision_seq`` is persisted (and re-seeded from live rows on open), so
    the counter survives emptying the revision table and reopening the file.
    """
    high = db.execute("SELECT high FROM revision_seq WHERE slot=1").fetchone()[0]
    top = db.execute("SELECT MAX(id) FROM revision").fetchone()[0] or 0
    return max(high, top)


class Distribution:
    def __init__(self, path):
        self.path = path
        with closing(sqlite3.connect(path)) as db, db:
            db.executescript("""CREATE TABLE IF NOT EXISTS revision (
                id INTEGER PRIMARY KEY, config TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS channel (
                slot INTEGER PRIMARY KEY CHECK (slot=1), revision INTEGER, generation INTEGER NOT NULL);
                INSERT OR IGNORE INTO channel VALUES (1, NULL, 0);
                CREATE TABLE IF NOT EXISTS canary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                slot INTEGER NOT NULL DEFAULT 1 CHECK (slot=1) UNIQUE,
                revision INTEGER NOT NULL,
                base_generation INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS canary_member (
                trial_id INTEGER NOT NULL,
                instance_id TEXT NOT NULL,
                zone TEXT NOT NULL,
                runtime TEXT NOT NULL,
                delivered INTEGER NOT NULL DEFAULT 0,
                acked INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (trial_id, instance_id));""")
            # Configuration-group path. All tables are additive: an existing
            # database opens unchanged and gains the group path without a rebuild.
            # Group rows reference the same immutable revisions as the single
            # channel; configuration bodies are never copied into group tables.
            db.executescript("""CREATE TABLE IF NOT EXISTS group_meta (
                slot INTEGER PRIMARY KEY CHECK (slot=1),
                epoch INTEGER NOT NULL,
                base_epoch INTEGER NOT NULL);
                INSERT OR IGNORE INTO group_meta VALUES (1, 0, 0);
                CREATE TABLE IF NOT EXISTS group_binding (
                name TEXT PRIMARY KEY,
                revision INTEGER NOT NULL,
                needs TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS group_event (
                epoch INTEGER NOT NULL,
                name TEXT NOT NULL,
                revision INTEGER,
                needs TEXT,
                PRIMARY KEY (epoch, name)) WITHOUT ROWID;
                CREATE TABLE IF NOT EXISTS group_base (
                name TEXT PRIMARY KEY,
                revision INTEGER NOT NULL,
                needs TEXT NOT NULL) WITHOUT ROWID;""")
            # Staged-version reclamation. Registration and pin membership are
            # kept in their own tables; revision bodies are never copied here.
            db.executescript("""CREATE TABLE IF NOT EXISTS retired_revision (
                revision INTEGER PRIMARY KEY);
                CREATE TABLE IF NOT EXISTS pin (
                id INTEGER PRIMARY KEY AUTOINCREMENT);
                CREATE TABLE IF NOT EXISTS pin_member (
                pin_id INTEGER NOT NULL,
                revision INTEGER NOT NULL,
                PRIMARY KEY (pin_id, revision)) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS pin_member_revision
                ON pin_member (revision);
                CREATE TABLE IF NOT EXISTS revision_seq (
                slot INTEGER PRIMARY KEY CHECK (slot=1), high INTEGER NOT NULL);
                INSERT OR IGNORE INTO revision_seq VALUES (1, 0);""")
            # Seed the version high-water mark from any rows written by an older
            # build, so an upgrade followed by reclaiming every staged body still
            # remembers which ids were issued and never recycles them.
            db.execute("UPDATE revision_seq SET high = MAX(high, "
                       "COALESCE((SELECT MAX(id) FROM revision), 0)) WHERE slot=1")
            # Library branch identity for recovery drills. One stable random
            # identifier per database file: seeded on first open after upgrade,
            # never changed in place afterwards; fork_recovery gives each copy
            # a fresh one. Competing first-opens race a single INSERT OR IGNORE,
            # and every later reader sees the stored winner.
            db.executescript("""CREATE TABLE IF NOT EXISTS library_meta (
                slot INTEGER PRIMARY KEY CHECK (slot=1),
                branch TEXT NOT NULL);""")
            db.execute("INSERT OR IGNORE INTO library_meta VALUES (1, ?)",
                       (secrets.token_hex(16),))

    def stage(self, config):
        if (type(config) is not dict or set(config) != {"upstream", "timeout_ms"}
                or not isinstance(config["upstream"], str) or not config["upstream"].strip()
                or type(config["timeout_ms"]) is not int or config["timeout_ms"] <= 0):
            raise ValueError("configuration requires upstream and a positive integer timeout_ms")
        with closing(sqlite3.connect(self.path, timeout=30)) as db, db:
            # A separate high-water mark survives emptying the revision table
            # (and works on databases created before this table existed):
            # SQLite on its own resets rowid allocation once a table is empty,
            # which would let a new version reuse an id a stale promotion or
            # pin still expects to identify old content.
            db.execute("BEGIN IMMEDIATE")
            revision = _revision_high_water(db) + 1
            db.execute("INSERT INTO revision(id, config) VALUES (?, ?)",
                       (revision, json.dumps(config, sort_keys=True)))
            db.execute("UPDATE revision_seq SET high=? WHERE slot=1", (revision,))
            return revision

    def promote(self, revision, *, expected_generation):
        with closing(sqlite3.connect(self.path, timeout=30)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            if not db.execute("SELECT 1 FROM revision WHERE id=?", (revision,)).fetchone():
                raise KeyError(revision)
            if db.execute("SELECT 1 FROM canary LIMIT 1").fetchone():
                raise ConflictingPromotion("a canary trial is active")
            if db.execute("""UPDATE channel SET revision=?, generation=generation+1
                             WHERE slot=1 AND generation=?""",
                          (revision, expected_generation)).rowcount != 1:
                raise ConflictingPromotion(expected_generation)
            return expected_generation + 1

    def poll(self, known_generation=None):
        with closing(sqlite3.connect(self.path)) as db:
            generation, revision, data = db.execute("""
                SELECT c.generation,c.revision,r.config FROM channel c
                LEFT JOIN revision r ON r.id=c.revision WHERE c.slot=1""").fetchone()
        if revision is None or generation == known_generation:
            return None
        return generation, revision, json.loads(data)

    def begin_canary(self, revision, *, expected_generation, roster, max_load):
        # Pure validation and optimization first: a rejected roster must never
        # take the single active-canary seat or open a write transaction.
        normalized = _validate_roster(roster, max_load)
        selected_ids = _choose_members(normalized, max_load)
        details = {row[0]: (row[1], row[2]) for row in normalized}
        with closing(sqlite3.connect(self.path, timeout=30)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            if not db.execute("SELECT 1 FROM revision WHERE id=?", (revision,)).fetchone():
                raise KeyError(revision)
            generation, current_revision = db.execute(
                "SELECT generation, revision FROM channel WHERE slot=1").fetchone()
            if generation != expected_generation:
                raise ConflictingPromotion(expected_generation)
            if db.execute("SELECT 1 FROM canary LIMIT 1").fetchone():
                raise ConflictingPromotion("a canary trial is already active")
            if current_revision == revision:
                raise ValueError("revision is already the active full configuration")
            trial_id = db.execute(
                "INSERT INTO canary(revision, base_generation) VALUES (?, ?)",
                (revision, generation)).lastrowid
            db.executemany("""INSERT INTO canary_member
                              (trial_id, instance_id, zone, runtime) VALUES (?, ?, ?, ?)""",
                           [(trial_id, instance_id, details[instance_id][0],
                             details[instance_id][1]) for instance_id in selected_ids])
            return trial_id, tuple(selected_ids)

    def poll_canary(self, trial_id, instance_id):
        with closing(sqlite3.connect(self.path, timeout=30)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("""SELECT c.revision, r.config FROM canary c
                                JOIN canary_member m ON m.trial_id = c.id
                                JOIN revision r ON r.id = c.revision
                                WHERE c.id = ? AND m.instance_id = ?""",
                             (trial_id, instance_id)).fetchone()
            if row is None:
                return None
            db.execute("""UPDATE canary_member SET delivered = 1
                          WHERE trial_id = ? AND instance_id = ?""",
                       (trial_id, instance_id))
            revision, config = row
            return trial_id, revision, json.loads(config)

    def ack_canary(self, trial_id, instance_id):
        with closing(sqlite3.connect(self.path, timeout=30)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("""SELECT m.delivered FROM canary c
                                JOIN canary_member m ON m.trial_id = c.id
                                WHERE c.id = ? AND m.instance_id = ?""",
                             (trial_id, instance_id)).fetchone()
            if row is None or not row[0]:
                return False
            db.execute("""UPDATE canary_member SET acked = 1
                          WHERE trial_id = ? AND instance_id = ?""",
                       (trial_id, instance_id))
            return True

    def finish_canary(self, trial_id):
        with closing(sqlite3.connect(self.path, timeout=30)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT revision FROM canary WHERE id = ?", (trial_id,)).fetchone()
            if row is None:
                return None
            revision = row[0]
            if db.execute("SELECT COUNT(*) FROM canary_member WHERE trial_id = ? AND acked = 0",
                          (trial_id,)).fetchone()[0]:
                return None
            generation = db.execute(
                "SELECT generation FROM channel WHERE slot=1").fetchone()[0]
            # Single atomic change: close the trial and move the full pointer.
            db.execute("DELETE FROM canary_member WHERE trial_id = ?", (trial_id,))
            db.execute("DELETE FROM canary WHERE id = ?", (trial_id,))
            db.execute("""UPDATE channel SET revision = ?, generation = generation + 1
                          WHERE slot = 1""", (revision,))
            return generation + 1

    def abort_canary(self, trial_id):
        with closing(sqlite3.connect(self.path, timeout=30)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            if not db.execute("SELECT 1 FROM canary WHERE id = ?", (trial_id,)).fetchone():
                return False
            db.execute("DELETE FROM canary_member WHERE trial_id = ?", (trial_id,))
            db.execute("DELETE FROM canary WHERE id = ?", (trial_id,))
            return True

    # ------------------------------------------------------------------
    # Staged-version reclamation. Registering a revision for collection never
    # removes it; collection deletes bodies only once every legitimate live
    # reference (full pointer, canary candidate, pin, retained group window)
    # is gone. Space freed by a delete stays with SQLite for reuse.
    # ------------------------------------------------------------------

    def retire_revisions(self, revisions):
        """Register revisions as collectable; never deletes a body now."""
        batch = _validate_batch(revisions)
        with closing(sqlite3.connect(self.path, timeout=30)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            staged = {row[0] for row in db.execute("SELECT id FROM revision")}
            for revision in batch:
                if revision not in staged:
                    raise KeyError(revision)
            db.executemany(
                "INSERT OR IGNORE INTO retired_revision(revision) VALUES (?)",
                [(revision,) for revision in batch])
            return None

    def pin_revisions(self, revisions):
        """Create one immutable pin covering every revision; return its id."""
        batch = _validate_batch(revisions)
        with closing(sqlite3.connect(self.path, timeout=30)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            staged = {row[0] for row in db.execute("SELECT id FROM revision")}
            for revision in batch:
                if revision not in staged:
                    raise KeyError(revision)
            pin_id = db.execute("INSERT INTO pin DEFAULT VALUES").lastrowid
            db.executemany("INSERT INTO pin_member(pin_id, revision) VALUES (?, ?)",
                           [(pin_id, revision) for revision in batch])
            return pin_id

    def read_pin(self, pin_id):
        """Return {revision: config} for a live pin, or None if unknown/released."""
        _check_id(pin_id, "pin_id")
        with closing(sqlite3.connect(self.path)) as db, db:
            # One read snapshot: an unpin cannot slip between the existence
            # check and the member lookup, so a pin is either fully readable or
            # reported as unknown/released, never half-empty.
            db.execute("BEGIN")
            if not db.execute("SELECT 1 FROM pin WHERE id=?", (pin_id,)).fetchone():
                return None
            return {
                revision: json.loads(config)
                for revision, config in db.execute(
                    "SELECT m.revision, r.config FROM pin_member m "
                    "JOIN revision r ON r.id = m.revision WHERE m.pin_id=? "
                    "ORDER BY m.revision", (pin_id,))}

    def unpin_revisions(self, pin_id):
        """Release a pin once; return False for an unknown or already released pin."""
        _check_id(pin_id, "pin_id")
        with closing(sqlite3.connect(self.path, timeout=30)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            if not db.execute("SELECT 1 FROM pin WHERE id=?", (pin_id,)).fetchone():
                return False
            db.execute("DELETE FROM pin_member WHERE pin_id=?", (pin_id,))
            db.execute("DELETE FROM pin WHERE id=?", (pin_id,))
            return True

    def collect_retired(self, *, after=0, scan_limit=64):
        """Delete unprotected retired revisions in one ascending cursor batch."""
        if type(after) is not int or not (0 <= after <= _MAX_ID):
            raise ValueError("after must be an integer in [0, %d]" % _MAX_ID)
        if type(scan_limit) is not int or not (1 <= scan_limit <= _SCAN_MAX):
            raise ValueError("scan_limit must be an integer in [1, %d]" % _SCAN_MAX)

        with closing(sqlite3.connect(self.path, timeout=30)) as db, db:
            # BEGIN IMMEDIATE serialises against stage/pin/publish/collect on
            # other connections: each candidate's protection test and its
            # delete observe the same snapshot, so a reference that lands first
            # keeps the body and a collected id is already gone to late callers.
            db.execute("BEGIN IMMEDIATE")
            ids = [row[0] for row in db.execute(
                "SELECT DISTINCT revision FROM retired_revision "
                "WHERE revision > ? ORDER BY revision LIMIT ?",
                (after, scan_limit + 1))]
            window = ids[:scan_limit]
            has_more = len(ids) > scan_limit
            protected = _protected_revisions(db, window)
            deleted = tuple(r for r in window if r not in protected)
            retained = tuple(r for r in window if r in protected)
            if deleted:
                placeholders = ",".join("?" for _ in deleted)
                db.execute("DELETE FROM revision WHERE id IN (%s)" % placeholders, deleted)
                db.execute("DELETE FROM retired_revision WHERE revision IN (%s)" % placeholders,
                           deleted)
            next_after = window[-1] if has_more and window else None
            return {"deleted": deleted, "retained": retained, "next_after": next_after}

    # ------------------------------------------------------------------
    # Recovery forks: an independently publishable copy of this library for
    # local disaster-recovery drills. The copy is written to a temporary file
    # in the destination directory and appears at the final path atomically
    # (hard link, no clobber), so the destination is never observable
    # half-made and a same-path race has exactly one winner.
    # ------------------------------------------------------------------

    def fork_recovery(self, destination):
        """Copy this library to a new standalone file; return its branch id.

        The copy reflects one committed state of the source observed between
        the start of this call and the appearance of the destination file:
        the full pointer and generation, group bindings with their replay
        window, staged bodies, the version high-water mark, the retirement
        registry and every live pin all come from that single point. An
        in-flight canary is NOT carried over (the source trial keeps running
        untouched); the copy may immediately open its own trial, whose id
        never reuses the dropped one, and no generation advances and no body
        is collected because of the drop.

        ``destination`` must not exist yet (FileExistsError otherwise, never
        clobbered); its parent directory must exist (OSError otherwise). The
        new file opens on its own with ``Distribution(destination)`` and may
        be moved away from the source directory by itself. On any failure the
        final path is left absent and temporary files are removed.
        """
        dest = os.fspath(destination)
        if os.path.lexists(dest):
            raise FileExistsError(errno.EEXIST, "destination already exists", dest)
        parent = os.path.dirname(dest) or "."
        # The temporary file is created next to the destination so the final
        # os.link is atomic on the same filesystem. A missing parent surfaces
        # here as the usual file error.
        fd, temp = tempfile.mkstemp(
            prefix=os.path.basename(dest) + ".", suffix=".fork", dir=parent)
        os.close(fd)
        try:
            # SQLite's online backup streams the database page by page under a
            # read transaction: the copy is transactionally consistent, memory
            # use stays independent of the library size, and on a WAL source
            # other connections keep committing while the copy runs.
            with closing(sqlite3.connect(self.path, timeout=30)) as src, \
                    closing(sqlite3.connect(temp, timeout=30)) as dst:
                src.backup(dst)
            with closing(sqlite3.connect(temp, timeout=30)) as fix, fix:
                # Detach the copy from WAL mode so the single file is fully
                # self-contained (no -wal/-shm siblings to carry along).
                mode = fix.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
                if mode != "delete":
                    raise RuntimeError(
                        "recovery copy could not be made standalone (journal_mode=%s)"
                        % mode)
                fix.execute("BEGIN IMMEDIATE")
                row = fix.execute(
                    "SELECT branch FROM library_meta WHERE slot=1").fetchone()
                if row is None:
                    raise RuntimeError("source database has no branch identifier")
                new_branch = secrets.token_hex(16)
                while new_branch == row[0]:
                    new_branch = secrets.token_hex(16)
                fix.execute("UPDATE library_meta SET branch=? WHERE slot=1",
                            (new_branch,))
                # Drop the in-flight trial only on the copy. sqlite_sequence
                # was copied along, so the next trial id stays strictly
                # greater than the dropped one; channel/group epochs and
                # staged bodies are deliberately left alone.
                fix.execute("DELETE FROM canary_member")
                fix.execute("DELETE FROM canary")
            # Atomic no-clobber publication: loser's FileExistsError leaves
            # the winner's file (and any pre-existing entry) untouched.
            os.link(temp, dest)
        except BaseException:
            for leftover in (temp, temp + "-wal", temp + "-shm", temp + "-journal"):
                try:
                    os.unlink(leftover)
                except OSError:
                    pass
            raise
        try:
            os.unlink(temp)
        except OSError:
            pass
        return new_branch

    # ------------------------------------------------------------------
    # Configuration groups: atomic multi-component publishing with an
    # independent group epoch and a fixed 256-epoch replay window.
    # ------------------------------------------------------------------

    def _current_epoch(self):
        with closing(sqlite3.connect(self.path)) as db:
            return db.execute(
                "SELECT epoch FROM group_meta WHERE slot=1").fetchone()[0]

    def publish_group(self, changes, *, expected_epoch):
        # Pure argument validation first, before any transaction or state
        # touch: every rejection leaves group content, epoch and history intact.
        if type(changes) is not dict or not (1 <= len(changes) <= _GROUP_MAX_CHANGES):
            raise ValueError(
                "changes must be a dict of 1-%d component entries" % _GROUP_MAX_CHANGES)
        if type(expected_epoch) is not int:
            raise ValueError("expected_epoch must be an integer")
        parsed = {}
        for name, binding in changes.items():
            if type(name) is not str or _IDENT_RE.fullmatch(name) is None:
                raise ValueError(
                    "component names must be 1-64 ASCII characters of letters, "
                    "digits, '-' or '_'")
            if binding is None:
                parsed[name] = None
                continue
            if type(binding) is not tuple or len(binding) != 2:
                raise ValueError("component binding must be None or a (revision, needs) tuple")
            revision, needs = binding
            if type(revision) is not int or revision <= 0:
                raise ValueError("revision must be a positive integer returned by stage()")
            if type(needs) is not dict or len(needs) > _GROUP_MAX_NEEDS:
                raise ValueError(
                    "needs must be a dict of at most %d dependency entries" % _GROUP_MAX_NEEDS)
            normalized_needs = {}
            for dep_name, dep_revision in needs.items():
                if type(dep_name) is not str or _IDENT_RE.fullmatch(dep_name) is None:
                    raise ValueError("dependency names must be valid component names")
                if type(dep_revision) is not int or dep_revision <= 0:
                    raise ValueError("dependency versions must be exact positive integers")
                normalized_needs[dep_name] = dep_revision
            parsed[name] = (revision, normalized_needs)

        with closing(sqlite3.connect(self.path, timeout=30)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            current_epoch, base_epoch = db.execute(
                "SELECT epoch, base_epoch FROM group_meta WHERE slot=1").fetchone()
            if current_epoch != expected_epoch:
                raise ConflictingPromotion(expected_epoch)

            # All directly staged revisions must exist. A missing staged
            # revision is a KeyError, distinct from structural ValueErrors.
            staged = {row[0] for row in db.execute("SELECT id FROM revision")}
            for binding in parsed.values():
                if binding is not None and binding[0] not in staged:
                    raise KeyError(binding[0])

            # Build the prospective whole group by overlaying the batch onto
            # the current bindings. Validation runs against the final group,
            # never against intermediate per-change states.
            group = {
                row[0]: (row[1], json.loads(row[2]))
                for row in db.execute("SELECT name, revision, needs FROM group_binding")}
            for name, binding in parsed.items():
                if binding is None:
                    group.pop(name, None)
                else:
                    group[name] = binding
            if len(group) > _GROUP_MAX_COMPONENTS:
                raise ValueError(
                    "a published group may contain at most %d components" % _GROUP_MAX_COMPONENTS)
            for name, (_, needs) in group.items():
                for dep_name, dep_revision in needs.items():
                    target = group.get(dep_name)
                    if target is None or target[0] != dep_revision:
                        raise ValueError(
                            "component %r requires %s@%d which is absent or bound to another "
                            "revision in the published group" % (name, dep_name, dep_revision))
            if _group_has_cycle(group):
                raise ValueError("the published group must not contain a dependency cycle")

            new_epoch = current_epoch + 1
            for name, binding in parsed.items():
                if binding is None:
                    db.execute("DELETE FROM group_binding WHERE name=?", (name,))
                    db.execute(
                        "INSERT INTO group_event(epoch, name, revision, needs) "
                        "VALUES (?, ?, NULL, NULL)", (new_epoch, name))
                else:
                    revision, needs = binding
                    db.execute(
                        "INSERT INTO group_binding(name, revision, needs) VALUES (?, ?, ?) "
                        "ON CONFLICT(name) DO UPDATE SET revision=excluded.revision, "
                        "needs=excluded.needs",
                        (name, revision, json.dumps(needs, sort_keys=True)))
                    db.execute(
                        "INSERT INTO group_event(epoch, name, revision, needs) "
                        "VALUES (?, ?, ?, ?)",
                        (new_epoch, name, revision,
                         json.dumps(needs, sort_keys=True)))
            db.execute("UPDATE group_meta SET epoch=? WHERE slot=1", (new_epoch,))

            # Keep exactly the latest _GROUP_WINDOW epochs (including the
            # current one) reconstructable. When the window fills, fold the
            # epoch falling out of the window into a moving baseline and drop
            # its events; no full per-epoch group copies are ever stored.
            if new_epoch >= _GROUP_WINDOW:
                retained_low = new_epoch - _GROUP_WINDOW + 1
                if base_epoch < retained_low:
                    snapshot = _load_group_snapshot(db, retained_low)
                    db.execute("DELETE FROM group_base")
                    db.executemany(
                        "INSERT INTO group_base(name, revision, needs) VALUES (?, ?, ?)",
                        [(name, revision, json.dumps(needs, sort_keys=True))
                         for name, (revision, needs) in sorted(snapshot.items())])
                    db.execute(
                        "DELETE FROM group_event WHERE epoch <= ?", (retained_low,))
                    db.execute(
                        "UPDATE group_meta SET base_epoch=? WHERE slot=1", (retained_low,))
            return new_epoch

    def poll_group(self, roots, *, known_epoch=None):
        if type(roots) is not tuple or not (1 <= len(roots) <= _GROUP_MAX_ROOTS):
            raise ValueError(
                "roots must be a tuple of 1-%d distinct component names" % _GROUP_MAX_ROOTS)
        for name in roots:
            if type(name) is not str or _IDENT_RE.fullmatch(name) is None:
                raise ValueError("roots must be valid component names")
        if len(set(roots)) != len(roots):
            raise ValueError("root component names must be distinct")
        if known_epoch is not None:
            if type(known_epoch) is not int or known_epoch < 0:
                raise ValueError("known_epoch must be a non-negative integer or None")

        with closing(sqlite3.connect(self.path, timeout=30)) as db, db:
            # Pin one read transaction for the whole reply: every SELECT below
            # (epoch, current bindings, historical reconstruction, config
            # bodies) shares a single snapshot, so a reply is never stitched
            # together from points on either side of a concurrent publish.
            db.execute("BEGIN")
            current_epoch, base_epoch = db.execute(
                "SELECT epoch, base_epoch FROM group_meta WHERE slot=1").fetchone()
            if known_epoch == current_epoch:
                return None
            if known_epoch is not None and known_epoch > current_epoch:
                raise ValueError("known_epoch %d is in the future" % known_epoch)

            current = _load_group_snapshot(db, current_epoch)
            current_scope = _group_scope(current, roots)

            if known_epoch is None or known_epoch < base_epoch:
                # First pull, or an epoch older than the retained window: the
                # old installed scope cannot be reconstructed, so replace the
                # whole managed set rather than guessing a delta.
                put = _load_configs(db, current, current_scope)
                return {"epoch": current_epoch, "mode": "full",
                        "put": put, "remove": ()}

            old = _load_group_snapshot(db, known_epoch)
            old_scope = _group_scope(old, roots)
            put_names = {
                name for name in current_scope
                if name not in old_scope or old[name][0] != current[name][0]}
            put = _load_configs(db, current, put_names)
            remove = tuple(sorted(old_scope - current_scope))
            return {"epoch": current_epoch, "mode": "delta",
                    "put": put, "remove": remove}

    def poll_group_cursor(self, roots, *, cursor=None):
        """poll_group variant carrying a (branch, epoch) cursor.

        Same roots and reply shape as poll_group, plus a "cursor" entry on
        every non-empty reply naming the branch and epoch the reply was
        actually built from. A cursor minted by a DIFFERENT library always
        yields a full reply for this library's current branch — the whole
        managed set is replaced, never merged, so components that only exist
        on the other library are dropped; the foreign epoch is never
        interpreted against this library's history (not even as a future
        epoch). Same-branch cursors follow the plain epoch rules: current
        epoch returns None, an in-window epoch yields an exact delta, an
        expired epoch falls back to full, a future epoch raises ValueError.
        """
        if type(roots) is not tuple or not (1 <= len(roots) <= _GROUP_MAX_ROOTS):
            raise ValueError(
                "roots must be a tuple of 1-%d distinct component names" % _GROUP_MAX_ROOTS)
        for name in roots:
            if type(name) is not str or _IDENT_RE.fullmatch(name) is None:
                raise ValueError("roots must be valid component names")
        if len(set(roots)) != len(roots):
            raise ValueError("root component names must be distinct")
        if cursor is not None:
            if (type(cursor) is not tuple or len(cursor) != 2
                    or type(cursor[0]) is not str
                    or _BRANCH_RE.fullmatch(cursor[0]) is None
                    or type(cursor[1]) is not int
                    or not (0 <= cursor[1] <= _MAX_ID)):
                raise ValueError(
                    "cursor must be None or a (branch, epoch) tuple: branch is 32 "
                    "lowercase hex characters, epoch an integer in [0, %d]" % _MAX_ID)

        with closing(sqlite3.connect(self.path, timeout=30)) as db, db:
            # One read transaction, exactly like poll_group: branch, epoch,
            # history and config bodies in a reply all come from this single
            # snapshot, so a switch-over reply is never stitched together
            # from two points in time.
            db.execute("BEGIN")
            branch, current_epoch, base_epoch = db.execute(
                "SELECT m.branch, g.epoch, g.base_epoch FROM library_meta m, "
                "group_meta g WHERE m.slot=1 AND g.slot=1").fetchone()

            if cursor is not None and cursor[0] != branch:
                # Foreign cursor: replace the entire managed installation
                # with this branch's current scope, whatever the epoch says.
                current = _load_group_snapshot(db, current_epoch)
                put = _load_configs(db, current, _group_scope(current, roots))
                return {"epoch": current_epoch, "mode": "full", "put": put,
                        "remove": (), "cursor": (branch, current_epoch)}

            known_epoch = None if cursor is None else cursor[1]
            if known_epoch == current_epoch:
                return None
            if known_epoch is not None and known_epoch > current_epoch:
                raise ValueError("cursor epoch %d is in the future" % known_epoch)

            current = _load_group_snapshot(db, current_epoch)
            current_scope = _group_scope(current, roots)

            if known_epoch is None or known_epoch < base_epoch:
                put = _load_configs(db, current, current_scope)
                return {"epoch": current_epoch, "mode": "full", "put": put,
                        "remove": (), "cursor": (branch, current_epoch)}

            old = _load_group_snapshot(db, known_epoch)
            old_scope = _group_scope(old, roots)
            put_names = {
                name for name in current_scope
                if name not in old_scope or old[name][0] != current[name][0]}
            put = _load_configs(db, current, put_names)
            remove = tuple(sorted(old_scope - current_scope))
            return {"epoch": current_epoch, "mode": "delta", "put": put,
                    "remove": remove, "cursor": (branch, current_epoch)}

    # ------------------------------------------------------------------
    # Hot reload: one in-memory job describing how a node can move a set of
    # roots between two retained group epochs one entry-point switch at a
    # time, loading new slots into spare capacity before switching and only
    # freeing an old slot once no root still serves from it. The job snapshots
    # both endpoints at preparation time; later publishes never alter it.
    # ------------------------------------------------------------------

    def prepare_hot_reload(self, roots, *, from_epoch, to_epoch, weights):
        # Pure shape validation first, mirroring poll_group's root format.
        if type(roots) is not tuple or not (1 <= len(roots) <= _GROUP_MAX_ROOTS):
            raise ValueError(
                "roots must be a tuple of 1-%d distinct component names"
                % _GROUP_MAX_ROOTS)
        for name in roots:
            if type(name) is not str or _IDENT_RE.fullmatch(name) is None:
                raise ValueError("roots must be valid component names")
        if len(set(roots)) != len(roots):
            raise ValueError("root component names must be distinct")
        if type(from_epoch) is not int or type(to_epoch) is not int:
            raise ValueError("epochs must be integers")
        if not (0 <= from_epoch < to_epoch):
            raise ValueError("require 0 <= from_epoch < to_epoch")
        if type(weights) is not dict:
            raise ValueError("weights must be a dict keyed by (component, revision) slots")

        with closing(sqlite3.connect(self.path, timeout=30)) as db, db:
            # One read snapshot for both endpoints, every binding and every
            # config body, so the job is never stitched across two publishes.
            db.execute("BEGIN")
            current_epoch, base_epoch = db.execute(
                "SELECT epoch, base_epoch FROM group_meta WHERE slot=1").fetchone()
            if not (base_epoch <= from_epoch <= current_epoch
                    and base_epoch <= to_epoch <= current_epoch):
                raise ValueError(
                    "both epochs must remain inside the retained group window "
                    "[%d, %d]" % (base_epoch, current_epoch))

            old_group = _load_group_snapshot(db, from_epoch)
            new_group = _load_group_snapshot(db, to_epoch)
            for name in roots:
                if name not in old_group:
                    raise ValueError(
                        "root %r is absent from group epoch %d" % (name, from_epoch))
                if name not in new_group:
                    raise ValueError(
                        "root %r is absent from group epoch %d" % (name, to_epoch))

            old_scopes = {}
            new_scopes = {}
            for name in roots:
                old_scopes[name] = _group_scope(old_group, (name,))
                new_scopes[name] = _group_scope(new_group, (name,))

            def slot_of(group, component):
                return component, group[component][0]

            old_slots = set()
            new_slots = set()
            for name in roots:
                old_slots.update(slot_of(old_group, c) for c in old_scopes[name])
                new_slots.update(slot_of(new_group, c) for c in new_scopes[name])
            all_slots = old_slots | new_slots
            if len(all_slots) > _HOT_MAX_SLOTS:
                raise ValueError(
                    "a hot reload job may span at most %d distinct slots; got %d"
                    % (_HOT_MAX_SLOTS, len(all_slots)))

            # Bodies are read as the exact stored JSON text and sized on their
            # UTF-8 encoding, exactly as they sit in the inventory.
            revisions = tuple(sorted({revision for _name, revision in all_slots}))
            placeholders = ",".join("?" for _ in revisions)
            rows = db.execute(
                "SELECT id, config FROM revision WHERE id IN (%s)" % placeholders,
                revisions).fetchall()
            bodies = dict(rows)
            missing = [revision for revision in revisions if revision not in bodies]
            if missing:
                raise KeyError(missing[0])
            for revision, text in rows:
                if len(text.encode("utf-8")) > _HOT_MAX_BODY_BYTES:
                    raise ValueError(
                        "config body of revision %d exceeds %d UTF-8 bytes"
                        % (revision, _HOT_MAX_BODY_BYTES))
            slot_bodies = {
                slot: bodies[slot[1]] for slot in all_slots}

            normalized_weights = {}
            for slot, weight in weights.items():
                if (type(slot) is not tuple or len(slot) != 2
                        or type(slot[0]) is not str
                        or type(slot[1]) is not int):
                    raise ValueError(
                        "weight keys must be (component name, revision) tuples")
                if type(weight) is not int or not (
                        _HOT_MIN_WEIGHT <= weight <= _HOT_MAX_WEIGHT):
                    raise ValueError(
                        "weights must be integers in [%d, %d]"
                        % (_HOT_MIN_WEIGHT, _HOT_MAX_WEIGHT))
                normalized_weights[slot] = weight
            if set(normalized_weights) != all_slots:
                missing_keys = sorted(all_slots - set(normalized_weights))
                extra_keys = sorted(set(normalized_weights) - all_slots)
                raise ValueError(
                    "weights must cover exactly the job slots; missing=%r extra=%r"
                    % (missing_keys, extra_keys))

            # Per-root transitive scopes resolved straight into slots, so the
            # job never has to revisit component graph structure again.
            old_need = {
                name: frozenset(slot_of(old_group, c) for c in old_scopes[name])
                for name in roots}
            new_need = {
                name: frozenset(slot_of(new_group, c) for c in new_scopes[name])
                for name in roots}
            return HotReloadJob(
                roots=tuple(roots),
                from_epoch=from_epoch,
                to_epoch=to_epoch,
                old_need=old_need,
                new_need=new_need,
                slots=frozenset(all_slots),
                slot_bodies=slot_bodies,
                weights=normalized_weights)


class HotReloadJob:
    """Immutable, in-memory hot-reload job between two retained group epochs.

    A job is held by its caller for the lifetime of the rollout: it never
    touches the database after construction, cannot be resumed across
    processes, and later publishes cannot change its endpoints, bodies or
    step decisions. Every accepted action is only simulated; the node itself
    is never operated on.
    """

    def __init__(self, *, roots, from_epoch, to_epoch, old_need, new_need,
                 slots, slot_bodies, weights):
        self._roots = tuple(roots)
        self._from_epoch = from_epoch
        self._to_epoch = to_epoch
        # Per-root transitive scopes expressed directly as (name, revision)
        # slots, keyed by root for either side of the switch.
        self._old_need = {root: frozenset(old_need[root]) for root in roots}
        self._new_need = {root: frozenset(new_need[root]) for root in roots}
        self._slots = frozenset(slots)
        # Stored JSON text, never parsed at preparation time; each delivery
        # re-parses a fresh object so a caller mutating a returned config
        # cannot influence any later call.
        self._bodies = dict(slot_bodies)
        self._weights = dict(weights)
        self._root_set = frozenset(roots)

    # ------------------------------------------------------------------
    # Shared state validation. A state is two duplicate-free tuples of job
    # members whose loaded set still covers every slot the roots currently
    # serve from (each unswitched root its old side, each switched root its
    # new side). Caller inputs are never mutated.
    # ------------------------------------------------------------------

    def _normalized_state(self, state):
        if type(state) is not dict or set(state) != {"switched", "loaded"}:
            raise ValueError(
                "state must be a dict with exactly 'switched' and 'loaded'")
        switched_raw, loaded_raw = state["switched"], state["loaded"]
        if type(switched_raw) is not tuple or type(loaded_raw) is not tuple:
            raise ValueError("state entries must be tuples")
        if len(set(switched_raw)) != len(switched_raw):
            raise ValueError("switched must not contain duplicates")
        if len(set(loaded_raw)) != len(loaded_raw):
            raise ValueError("loaded must not contain duplicates")
        switched = set()
        for root in switched_raw:
            if type(root) is not str or root not in self._root_set:
                raise ValueError("unknown root in state: %r" % (root,))
            switched.add(root)
        loaded = set()
        for slot in loaded_raw:
            if not self._is_known_slot(slot):
                raise ValueError("unknown slot in state: %r" % (slot,))
            loaded.add(slot)
        missing = self._active_slots(switched) - loaded
        if missing:
            raise ValueError(
                "state is missing slots required by roots in service: %r"
                % (sorted(missing),))
        return switched, loaded

    def _active_slots(self, switched):
        active = set()
        for root in self._roots:
            active.update(
                self._new_need[root] if root in switched else self._old_need[root])
        return active

    def _is_known_slot(self, slot):
        return (type(slot) is tuple and len(slot) == 2
                and type(slot[0]) is str and type(slot[1]) is int
                and slot in self._slots)

    def _loaded_weight(self, loaded):
        return sum(self._weights[slot] for slot in loaded)

    @staticmethod
    def _sorted_tuple(values):
        return tuple(sorted(values))

    def initial_state(self):
        loaded = set()
        for root in self._roots:
            loaded.update(self._old_need[root])
        return {"switched": (), "loaded": self._sorted_tuple(loaded)}

    def step(self, state, action, *, capacity):
        if type(capacity) is not int or not (0 <= capacity <= _HOT_MAX_CAPACITY):
            raise ValueError(
                "capacity must be an integer in [0, %d]" % _HOT_MAX_CAPACITY)
        switched, loaded = self._normalized_state(state)
        current_weight = self._loaded_weight(loaded)
        if current_weight > capacity:
            raise ValueError(
                "loaded weight %d already exceeds capacity %d"
                % (current_weight, capacity))

        if type(action) is not tuple or len(action) != 2:
            raise ValueError("action must be a (verb, target) tuple")
        verb, target = action

        if verb == "load":
            slot = target
            if (type(slot) is not tuple or len(slot) != 2
                    or type(slot[0]) is not str
                    or type(slot[1]) is not int
                    or slot not in self._slots):
                raise ValueError("unknown load slot: %r" % (slot,))
            if slot in loaded:
                return None
            new_weight = current_weight + self._weights[slot]
            if new_weight > capacity:
                return None
            new_loaded = loaded | {slot}
            return {
                "state": {"switched": self._sorted_tuple(switched),
                          "loaded": self._sorted_tuple(new_loaded)},
                "config": json.loads(self._bodies[slot])}

        if verb == "switch":
            root = target
            if type(root) is not str or root not in self._root_set:
                raise ValueError("unknown switch root: %r" % (root,))
            if root in switched:
                return None
            if not self._new_need[root] <= loaded:
                return None
            # A switch neither loads nor unloads slots and never touches
            # other roots; old slots stay resident until explicitly unloaded.
            new_switched = switched | {root}
            return {
                "state": {"switched": self._sorted_tuple(new_switched),
                          "loaded": self._sorted_tuple(loaded)},
                "config": None}

        if verb == "unload":
            slot = target
            if (type(slot) is not tuple or len(slot) != 2
                    or type(slot[0]) is not str
                    or type(slot[1]) is not int
                    or slot not in self._slots):
                raise ValueError("unknown unload slot: %r" % (slot,))
            if slot not in loaded:
                return None
            if slot in self._active_slots(switched):
                return None
            new_loaded = loaded - {slot}
            return {
                "state": {"switched": self._sorted_tuple(switched),
                          "loaded": self._sorted_tuple(new_loaded)},
                "config": None}

        raise ValueError("action verb must be one of 'load', 'switch', 'unload'")

    def plan(self, state, *, capacity):
        if type(capacity) is not int or not (0 <= capacity <= _HOT_MAX_CAPACITY):
            raise ValueError(
                "capacity must be an integer in [0, %d]" % _HOT_MAX_CAPACITY)
        switched, loaded = self._normalized_state(state)
        start_weight = self._loaded_weight(loaded)

        required_capacity, actions = self._solve(switched, loaded)
        if capacity < required_capacity:
            return {"required_capacity": required_capacity, "actions": None}
        return {"required_capacity": required_capacity, "actions": actions}

    def _solve(self, switched, loaded):
        """Minimum-peak rollout via subset dynamic programming.

        States are subsets of already-switched roots (at most 2**16); no root
        permutation is ever enumerated. For a fixed switch order, evicting
        every slot as early as possible and loading each new slot as late as
        possible keeps the resident weight pointwise minimal, so no plan can
        beat this transition's peak:

            resident before switching root r = active(S) union new_need(r)

        where active(S) is what the roots currently serve from. Unloads only
        shrink the resident set and are free, hence they never raise the
        peak; the start state's actual loaded weight is the first peak.
        """
        n = len(self._roots)
        index = {root: i for i, root in enumerate(self._roots)}
        full = (1 << n) - 1
        start_bits = 0
        for root in switched:
            start_bits |= 1 << index[root]
        start_weight = self._loaded_weight(loaded)

        if start_bits == full:
            # Already on the target side; the only work left is dropping
            # surplus slots, which cannot exceed the starting weight.
            return start_weight, self._closing_unloads(switched, loaded)

        old_need = self._old_need
        new_need = self._new_need
        weights = self._weights
        roots = self._roots

        # Encode every slot as one bit in a Python int so subset unions and
        # weights stay compact (at most 128 slots). Per-root old/new scopes
        # become integer masks; a slot's weight is read through a small list.
        slot_list = sorted(self._slots)
        slot_bit = {slot: 1 << i for i, slot in enumerate(slot_list)}
        slot_weight = [weights[slot] for slot in slot_list]
        old_mask = [0] * n
        new_mask = [0] * n
        for i, root in enumerate(roots):
            m_old = m_new = 0
            for slot in old_need[root]:
                m_old |= slot_bit[slot]
            for slot in new_need[root]:
                m_new |= slot_bit[slot]
            old_mask[i] = m_old
            new_mask[i] = m_new

        def mask_weight(mask):
            total = 0
            while mask:
                low = mask & -mask
                total += slot_weight[low.bit_length() - 1]
                mask ^= low
            return total

        # active_mask[bits]: live service slots once the roots in bits have
        # switched (new side for bits, old side for the rest). The union
        # naturally excludes slots no root serves from.
        size = 1 << n
        active_mask = [0] * size
        active_weight = [0] * size
        for bits in range(size):
            mask = 0
            for i in range(n):
                mask |= new_mask[i] if (bits & (1 << i)) else old_mask[i]
            active_mask[bits] = mask
            active_weight[bits] = mask_weight(mask)

        # Peak when switching root i at subset bits is the weight of
        # active(bits) union new_need(i); computed on demand from the masks,
        # so no (2**n)*n transition table is retained.
        def transition(bits, i):
            merged = active_mask[bits] | new_mask[i]
            extra = merged ^ active_mask[bits]
            return active_weight[bits] + mask_weight(extra)

        peak = [None] * size
        parent = [None] * size  # (previous_bits, root_index)
        peak[start_bits] = start_weight

        # Ascending integer order is a topological order over subset edges,
        # since every transition adds one bit and yields a larger integer.
        for bits in range(start_bits, size):
            if peak[bits] is None:
                continue
            base_peak = peak[bits]
            remaining = full ^ bits
            while remaining:
                low = remaining & -remaining
                i = low.bit_length() - 1
                remaining ^= low
                nxt = bits | low
                cand = max(base_peak, transition(bits, i))
                if peak[nxt] is None or cand < peak[nxt] or (
                        cand == peak[nxt] and roots[i] < roots[parent[nxt][1]]):
                    peak[nxt] = cand
                    parent[nxt] = (bits, i)

        # Reconstruct the chosen switch order, then expand it into concrete
        # legal load/switch/unload actions matching the DP resident sets.
        order = []
        bits = full
        while bits != start_bits:
            prev, i = parent[bits]
            order.append(i)
            bits = prev
        order.reverse()
        actions = self._replay_actions(switched, loaded, order)
        return peak[full], tuple(actions)

    def _closing_unloads(self, switched, loaded):
        surplus = loaded - self._active_slots(switched)
        return tuple(("unload", slot) for slot in sorted(surplus))

    def _replay_actions(self, switched, loaded, order):
        """Expand the chosen switch order into actions step() will accept.

        Before every switch the replay holds exactly active(S) plus the next
        root's target scope (the DP's pointwise-minimal resident set): slots
        no root serves from and the next switch does not need are unloaded
        first, the target's missing slots are loaded, then the root switches.
        Surplus slots in the start state are dropped up front; the job ends
        loaded with exactly the union of the target-side scopes.
        """
        current_switched = set(switched)
        current_loaded = set(loaded)
        pending = list(order)
        actions = []

        def trim(imminent):
            keep = self._active_slots(current_switched)
            if imminent is not None:
                keep = keep | self._new_need[self._roots[imminent]]
            for slot in sorted(current_loaded - keep):
                actions.append(("unload", slot))
                current_loaded.remove(slot)

        trim(pending[0] if pending else None)
        while pending:
            i = pending.pop(0)
            root = self._roots[i]
            for slot in sorted(self._new_need[root] - current_loaded):
                actions.append(("load", slot))
                current_loaded.add(slot)
            actions.append(("switch", root))
            current_switched.add(root)
            trim(pending[0] if pending else None)
        return actions


def _protected_revisions(db, candidates):
    """Return the subset of candidate ids still referenced by a live holder.

    Protection is computed from the live tables only, never from a permanent
    per-deleted-revision tombstone: the full pointer, an active canary, any
    unreleased pin, and every binding reachable in the retained group window.
    """
    if not candidates:
        return set()
    placeholders = ",".join("?" for _ in candidates)
    candidates = tuple(candidates)
    protected = set()

    # Single full-channel pointer.
    revision = db.execute("SELECT revision FROM channel WHERE slot=1").fetchone()[0]
    if revision in candidates:
        protected.add(revision)

    # The active canary candidate, if any.
    revision = db.execute("SELECT revision FROM canary").fetchone()
    if revision is not None and revision[0] in candidates:
        protected.add(revision[0])

    # Any unreleased pin.
    protected.update(
        row[0] for row in db.execute(
            "SELECT DISTINCT m.revision FROM pin_member m "
            "JOIN pin p ON p.id = m.pin_id WHERE m.revision IN (%s)" % placeholders,
            candidates))

    # Every revision bound at any epoch still inside the group window: the
    # moving baseline (the snapshot at the oldest retained epoch) plus all
    # retained change events, which cover every epoch up to and including the
    # current one. Bindings below the window were folded away, so an old
    # binding is only retained while the group actually still references it.
    protected.update(
        row[0] for row in db.execute(
            "SELECT DISTINCT revision FROM group_base WHERE revision IN (%s)"
            % placeholders, candidates))
    protected.update(
        row[0] for row in db.execute(
            "SELECT DISTINCT revision FROM group_event "
            "WHERE revision IN (%s)" % placeholders, candidates))
    # The live current group is always inside the window; consulting the
    # single binding table (not a per-epoch expansion) keeps protection correct
    # even for a database carried over from an earlier build.
    protected.update(
        row[0] for row in db.execute(
            "SELECT DISTINCT revision FROM group_binding "
            "WHERE revision IN (%s)" % placeholders, candidates))
    return protected


def _load_group_snapshot(db, epoch):
    """Reconstruct the whole group as bound at ``epoch`` (must be retained)."""
    base_epoch = db.execute(
        "SELECT base_epoch FROM group_meta WHERE slot=1").fetchone()[0]
    snapshot = {
        row[0]: (row[1], json.loads(row[2]))
        for row in db.execute("SELECT name, revision, needs FROM group_base")}
    rows = db.execute(
        "SELECT name, revision, needs FROM group_event WHERE ? < epoch AND epoch <= ? "
        "ORDER BY epoch, name", (base_epoch, epoch))
    for name, revision, needs in rows:
        if revision is None:
            snapshot.pop(name, None)
        else:
            snapshot[name] = (revision, json.loads(needs))
    return snapshot


def _group_has_cycle(group):
    """Iterative DFS over exact-name dependency edges; self-loops count."""
    state = {}  # 0 = visited once, 1 = on the current DFS stack
    for start in group:
        if start in state:
            continue
        stack = [(start, iter(group[start][1]))]
        state[start] = 1
        while stack:
            name, edges = stack[-1]
            advanced = False
            for dep in edges:
                if dep not in group:
                    continue  # dangling edges are reported separately
                mark = state.get(dep)
                if mark == 1:
                    return True
                if mark is None:
                    state[dep] = 1
                    stack.append((dep, iter(group[dep][1])))
                    advanced = True
                    break
            if not advanced:
                state[name] = 0
                stack.pop()
    return False


def _group_scope(group, roots):
    """Union of the roots and their transitive exact-name dependencies."""
    scope = set()
    stack = [name for name in roots if name in group]
    while stack:
        name = stack.pop()
        if name in scope:
            continue
        scope.add(name)
        for dep in group[name][1]:
            if dep in group and dep not in scope:
                stack.append(dep)
    return scope


def _load_configs(db, group, names):
    """Map component names to (revision, config) without copying bodies twice."""
    if not names:
        return {}
    revisions = tuple(sorted({group[name][0] for name in names}))
    placeholders = ",".join("?" for _ in revisions)
    configs = {
        row[0]: json.loads(row[1])
        for row in db.execute(
            "SELECT id, config FROM revision WHERE id IN (%s)" % placeholders,
            revisions)}
    return {name: (group[name][0], configs[group[name][0]]) for name in sorted(names)}
