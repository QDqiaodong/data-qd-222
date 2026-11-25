"""Versioned configuration consumed by service instances through one active pointer."""
from contextlib import closing
import json
import re
import sqlite3


class ConflictingPromotion(Exception):
    pass


_IDENT_RE = re.compile(r"[A-Za-z0-9_-]{1,64}\Z")
_MAX_ROSTER = 2400
_MAX_ZONES = 24
_MAX_RUNTIMES = 10
_MAX_LOAD = 1_000_000
_MAX_BUDGET = 24_000_000


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

    def stage(self, config):
        if (type(config) is not dict or set(config) != {"upstream", "timeout_ms"}
                or not isinstance(config["upstream"], str) or not config["upstream"].strip()
                or type(config["timeout_ms"]) is not int or config["timeout_ms"] <= 0):
            raise ValueError("configuration requires upstream and a positive integer timeout_ms")
        with closing(sqlite3.connect(self.path)) as db, db:
            return db.execute("INSERT INTO revision(config) VALUES (?)",
                              (json.dumps(config, sort_keys=True),)).lastrowid

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
