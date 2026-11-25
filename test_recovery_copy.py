"""Acceptance tests for recovery forks and cross-library cursor polling."""
from pathlib import Path
from tempfile import TemporaryDirectory
import json
import os
import re
import sqlite3
import subprocess
import sys
import threading
import unittest

from distribution import Distribution


MAX_ID = (1 << 63) - 1
BRANCH_RE = re.compile(r"[0-9a-f]{32}\Z")


def branch_of(path):
    """Read the stored library branch straight from the database file."""
    with sqlite3.connect(path) as db:
        return db.execute(
            "SELECT branch FROM library_meta WHERE slot=1").fetchone()[0]


def config(tag):
    return {"upstream": tag, "timeout_ms": 1}


class ForkRecoveryDrillTest(unittest.TestCase):
    """The duty drill: fork while publishing, switch subscribers over and back."""

    def test_switchover_and_switchback_with_moved_window(self):
        with TemporaryDirectory() as tmp:
            src_path = Path(tmp, "main.db")
            copy_path = Path(tmp, "recovery.db")
            pub = Distribution(src_path)
            route_rev = pub.stage({"upstream": "route-v1", "timeout_ms": 10})
            old_rev = pub.stage({"upstream": "old-v1", "timeout_ms": 11})
            new_rev = pub.stage({"upstream": "new-v1", "timeout_ms": 12})
            churn_a = pub.stage({"upstream": "churn-a", "timeout_ms": 13})
            churn_b = pub.stage({"upstream": "churn-b", "timeout_ms": 14})

            # Build the group through the old interface: route depends on old.
            epoch = pub.publish_group(
                {"route": (route_rev, {"old": old_rev}), "old": (old_rev, {})},
                expected_epoch=0)
            self.assertEqual(epoch, 1)

            # A route subscriber installs route and old through the old interface.
            subscriber = Distribution(src_path)
            first = subscriber.poll_group(("route",), known_epoch=None)
            self.assertEqual(first["mode"], "full")
            self.assertNotIn("cursor", first)  # old interface shape is unchanged
            installed = set(first["put"])
            self.assertEqual(installed, {"route", "old"})

            # Move the retention window at least once before the fork.
            for i in range(2, 257):
                rev = churn_a if i % 2 == 0 else churn_b
                epoch = pub.publish_group({"churn": (rev, {})}, expected_epoch=epoch)
            self.assertEqual(epoch, 256)
            with sqlite3.connect(src_path) as db:
                self.assertEqual(db.execute(
                    "SELECT base_epoch FROM group_meta").fetchone()[0], 1)

            # The old-interface subscriber still catches up with a bare epoch.
            caught = subscriber.poll_group(("route",), known_epoch=1)
            self.assertEqual(caught["mode"], "delta")
            self.assertEqual(caught["epoch"], 256)
            self.assertEqual(caught["put"], {})
            self.assertEqual(caught["remove"], ())
            self.assertNotIn("cursor", caught)

            src_branch = branch_of(src_path)
            self.assertIsNotNone(BRANCH_RE.fullmatch(src_branch))

            # Fork the recovery copy while the source keeps serving.
            copy_branch = pub.fork_recovery(copy_path)
            self.assertIsNotNone(BRANCH_RE.fullmatch(copy_branch))
            self.assertNotEqual(copy_branch, src_branch)

            # Both sides publish exactly once, landing on the same epoch with
            # different content: the source keeps old, the copy rebinds route
            # onto new (route's own revision is unchanged, old stays in the
            # copy's group but leaves route's scope).
            self.assertEqual(
                pub.publish_group({"route": (route_rev, {"old": old_rev})},
                                  expected_epoch=256), 257)
            copy = Distribution(copy_path)
            self.assertEqual(
                copy.publish_group(
                    {"route": (route_rev, {"new": new_rev}), "new": (new_rev, {})},
                    expected_epoch=256), 257)

            # The subscriber re-syncs on the source through the cursor protocol.
            synced = subscriber.poll_group_cursor(("route",), cursor=None)
            self.assertEqual(synced["mode"], "full")
            self.assertEqual(set(synced["put"]), {"route", "old"})
            self.assertEqual(synced["cursor"], (src_branch, 257))

            # Hand the source's same-epoch cursor to the copy: the foreign
            # branch forces a full replacement of the whole managed set.
            on_copy = Distribution(copy_path)
            switched = on_copy.poll_group_cursor(("route",), cursor=synced["cursor"])
            self.assertEqual(switched["mode"], "full")
            self.assertEqual(switched["epoch"], 257)
            self.assertEqual(switched["cursor"], (copy_branch, 257))
            self.assertEqual(switched["remove"], ())
            self.assertEqual(set(switched["put"]), {"route", "new"})
            self.assertEqual(switched["put"]["route"][0], route_rev)
            self.assertEqual(switched["put"]["new"],
                             (new_rev, {"upstream": "new-v1", "timeout_ms": 12}))
            # Whole-set replacement: only the put set survives, old is gone.
            installed = set(switched["put"])
            self.assertNotIn("old", installed)
            self.assertEqual(installed, {"route", "new"})

            # Steady state on the copy: the fresh cursor reports current.
            self.assertIsNone(
                on_copy.poll_group_cursor(("route",), cursor=switched["cursor"]))

            # Switch back to the source: also a full replacement.
            back = subscriber.poll_group_cursor(("route",), cursor=switched["cursor"])
            self.assertEqual(back["mode"], "full")
            self.assertEqual(back["cursor"], (src_branch, 257))
            self.assertEqual(back["remove"], ())
            self.assertEqual(set(back["put"]), {"route", "old"})
            installed = set(back["put"])
            self.assertIn("old", installed)

            # A foreign cursor from the future is still a full reply, never an
            # error: the foreign epoch is meaningless on this library.
            foreign_future = on_copy.poll_group_cursor(
                ("route",), cursor=(src_branch, 999))
            self.assertEqual(foreign_future["mode"], "full")
            self.assertEqual(foreign_future["cursor"], (copy_branch, 257))
            self.assertEqual(set(foreign_future["put"]), {"route", "new"})

            # A foreign cursor whose epoch falls inside this library's own
            # retention window is still a full replacement, never a delta.
            foreign_in_window = on_copy.poll_group_cursor(
                ("route",), cursor=(src_branch, 200))
            self.assertEqual(foreign_in_window["mode"], "full")
            self.assertEqual(foreign_in_window["cursor"], (copy_branch, 257))
            self.assertEqual(set(foreign_in_window["put"]), {"route", "new"})

            # Reopen the copy: its own cursor rules apply, history included.
            self.assertEqual(branch_of(copy_path), copy_branch)  # stable identity
            reopened = Distribution(copy_path)
            self.assertIsNone(
                reopened.poll_group_cursor(("route",), cursor=(copy_branch, 257)))
            mid = reopened.poll_group_cursor(("route",), cursor=(copy_branch, 200))
            self.assertEqual(mid["mode"], "delta")
            self.assertEqual(mid["cursor"], (copy_branch, 257))
            self.assertEqual(set(mid["put"]), {"new"})
            self.assertEqual(mid["remove"], ("old",))
            # The oldest catchable epoch still yields an exact delta, proving
            # the retained window crossed the fork; not every cursor goes full.
            with sqlite3.connect(copy_path) as db:
                copy_base = db.execute(
                    "SELECT base_epoch FROM group_meta").fetchone()[0]
            self.assertEqual(copy_base, 2)
            oldest = reopened.poll_group_cursor(
                ("route",), cursor=(copy_branch, copy_base))
            self.assertEqual(oldest["mode"], "delta")
            self.assertEqual(set(oldest["put"]), {"new"})
            self.assertEqual(oldest["remove"], ("old",))
            expired = reopened.poll_group_cursor(
                ("route",), cursor=(copy_branch, copy_base - 1))
            self.assertEqual(expired["mode"], "full")
            self.assertEqual(set(expired["put"]), {"route", "new"})
            with self.assertRaises(ValueError):
                reopened.poll_group_cursor(("route",), cursor=(copy_branch, 258))

            # The old bare-epoch interface works unchanged on the copy too.
            old_style = reopened.poll_group(("route",), known_epoch=256)
            self.assertEqual(old_style["mode"], "delta")
            self.assertNotIn("cursor", old_style)
            self.assertEqual(set(old_style["put"]), {"new"})
            self.assertEqual(old_style["remove"], ("old",))


class PollGroupCursorValidationTest(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.path = Path(self._tmp.name, "state.db")
        self.pub = Distribution(self.path)
        self.rev = self.pub.stage(config("one"))
        self.pub.publish_group({"a": (self.rev, {})}, expected_epoch=0)
        self.branch = branch_of(self.path)

    def tearDown(self):
        self._tmp.cleanup()

    def poll(self, cursor, roots=("a",)):
        return self.pub.poll_group_cursor(roots, cursor=cursor)

    def test_cursor_shape_validation(self):
        for bad in (
            ("A" * 32, 1),                 # uppercase hex is not valid
            ("g" * 32, 1),                 # not a hex character
            ("abc", 1),                    # too short
            ("a" * 33, 1),                 # too long
            ("a" * 32,),                   # missing epoch
            ("a" * 32, 1, "x"),            # too many items
            ["a" * 32, 1],                 # list, not tuple
            "a" * 32,                      # bare string
            5,                             # not a tuple at all
            (123, 1),                      # non-string branch
            (None, 1),
            ("a" * 32, True),              # bool is not an epoch
            ("a" * 32, -1),
            ("a" * 32, MAX_ID + 1),
            ("a" * 32, 1.0),
            ("a" * 32, "1"),
        ):
            with self.assertRaises(ValueError, msg=repr(bad)):
                self.poll(bad)
        # Boundary shapes are accepted (foreign branch -> full reply).
        for good in (("a" * 32, 0), ("f" * 32, MAX_ID), ("0" * 32, 1)):
            reply = self.poll(good)
            self.assertEqual(reply["mode"], "full")
            self.assertEqual(reply["cursor"], (self.branch, 1))

    def test_roots_validation_matches_poll_group(self):
        for bad_roots in (
            ["a"], (), tuple("r%d" % i for i in range(17)), ("a", "a"),
            ("bad name",), (1,), (None,),
        ):
            with self.assertRaises(ValueError, msg=repr(bad_roots)):
                self.pub.poll_group_cursor(bad_roots, cursor=None)

    def test_same_branch_cursor_rules(self):
        # Current epoch -> None; the library has only epoch 1 published.
        self.assertIsNone(self.poll((self.branch, 1)))
        older = self.poll((self.branch, 0))
        self.assertEqual(older["mode"], "delta")  # base_epoch is 0 here
        self.assertEqual(older["cursor"], (self.branch, 1))
        self.assertEqual(set(older["put"]), {"a"})
        with self.assertRaises(ValueError):
            self.poll((self.branch, 2))            # same-branch future epoch

    def test_empty_library_cursor_protocol(self):
        with TemporaryDirectory() as tmp:
            empty = Distribution(Path(tmp, "empty.db"))
            branch = branch_of(empty.path)
            first = empty.poll_group_cursor(("ghost",), cursor=None)
            self.assertEqual(
                first, {"epoch": 0, "mode": "full", "put": {}, "remove": (),
                        "cursor": (branch, 0)})
            self.assertIsNone(empty.poll_group_cursor(("ghost",), cursor=(branch, 0)))
            foreign = empty.poll_group_cursor(("ghost",), cursor=("b" * 32, 0))
            self.assertEqual(foreign["mode"], "full")
            self.assertEqual(foreign["cursor"], (branch, 0))

    def test_branch_stability_and_fork_distinctness(self):
        with TemporaryDirectory() as tmp:
            src = Path(tmp, "src.db")
            pub = Distribution(src)
            first = branch_of(src)
            self.assertIsNotNone(BRANCH_RE.fullmatch(first))
            # Reopening never changes the identity.
            self.assertEqual(branch_of(src), first)
            Distribution(src)
            self.assertEqual(branch_of(src), first)
            # Each successful fork gets a fresh, distinct branch.
            copy_a = pub.fork_recovery(Path(tmp, "a.db"))
            copy_b = pub.fork_recovery(Path(tmp, "b.db"))
            self.assertNotEqual(copy_a, first)
            self.assertNotEqual(copy_b, first)
            self.assertNotEqual(copy_a, copy_b)
            self.assertEqual(branch_of(Path(tmp, "a.db")), copy_a)
            self.assertEqual(branch_of(Path(tmp, "b.db")), copy_b)
            self.assertEqual(branch_of(src), first)  # source identity untouched
            # Copies reopen stably as well.
            self.assertEqual(branch_of(Path(tmp, "a.db")), copy_a)

    def test_upgrade_grants_a_stable_branch(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "old.db")
            # A database written by the original build: two original tables.
            with sqlite3.connect(path) as db:
                db.executescript("""
                    CREATE TABLE revision (id INTEGER PRIMARY KEY, config TEXT NOT NULL);
                    CREATE TABLE channel (slot INTEGER PRIMARY KEY CHECK (slot=1),
                        revision INTEGER, generation INTEGER NOT NULL);
                    INSERT INTO channel VALUES (1, NULL, 0);""")
                db.execute("INSERT INTO revision VALUES (1, ?)",
                           (json.dumps(config("old")),))
                db.execute("UPDATE channel SET revision=1, generation=3 WHERE slot=1")
            pub = Distribution(path)
            branch = branch_of(path)
            self.assertIsNotNone(BRANCH_RE.fullmatch(branch))
            self.assertEqual(branch_of(path), branch)  # stable across reopen
            reply = pub.poll_group_cursor(("a",), cursor=None)
            self.assertEqual(reply["cursor"], (branch, 0))
            self.assertEqual(reply["put"], {})
            copy_path = Path(tmp, "copy.db")
            copy_branch = pub.fork_recovery(copy_path)
            self.assertNotEqual(copy_branch, branch)
            self.assertEqual(branch_of(path), branch)
            # The old single-channel content survived the fork.
            self.assertEqual(Distribution(copy_path).poll()[:2], (3, 1))


class WalBoundaryTest(unittest.TestCase):
    """WAL source: writers commit while the copy is in flight."""

    def test_concurrent_commits_land_on_one_consistent_boundary(self):
        with TemporaryDirectory() as tmp:
            src_path = Path(tmp, "wal.db")
            dst_path = Path(tmp, "recovery.db")
            pub = Distribution(src_path)
            r_a = pub.stage({"upstream": "alpha", "timeout_ms": 1})
            r_b = pub.stage({"upstream": "bravo", "timeout_ms": 2})
            r_c = pub.stage({"upstream": "charlie", "timeout_ms": 3})
            r_d = pub.stage({"upstream": "delta", "timeout_ms": 4})
            r_e = pub.stage({"upstream": "echo", "timeout_ms": 5})
            r_f = pub.stage({"upstream": "foxtrot", "timeout_ms": 6})

            # Hold the database open with an active read transaction so no
            # close-checkpoint can move P1 out of the WAL before the fork
            # starts (WAL readers never block the writers below).
            keeper = sqlite3.connect(src_path)
            self.assertEqual(
                keeper.execute("PRAGMA journal_mode=WAL").fetchone()[0], "wal")
            keeper.execute("BEGIN")
            keeper.execute("SELECT count(*) FROM sqlite_master").fetchall()

            # P1: the pre-fork commit batch that must not be lost.
            self.assertEqual(pub.promote(r_a, expected_generation=0), 1)
            self.assertEqual(
                pub.publish_group({"g1marker": (r_c, {})}, expected_epoch=0), 1)
            pin_b = pub.pin_revisions((r_b,))
            pub.retire_revisions((r_c, r_f))

            # P1 still lives only in the WAL when the copy begins.
            with open(str(src_path) + "-wal", "rb") as fh:
                wal_bytes = fh.read()
            with open(src_path, "rb") as fh:
                main_bytes = fh.read()
            self.assertIn(b"g1marker", wal_bytes)
            self.assertNotIn(b"g1marker", main_bytes)

            gate = _BackupGate()
            original_connect = sqlite3.connect

            def fake_connect(path, *args, **kwargs):
                conn = original_connect(path, *args, **kwargs)
                if os.fspath(path) == str(src_path):
                    return _BackupGateProxy(conn, gate)
                return conn

            outcome = {}

            def do_fork():
                try:
                    outcome["branch"] = pub.fork_recovery(dst_path)
                except BaseException as exc:  # pragma: no cover - failure path
                    outcome["error"] = exc

            sqlite3.connect = fake_connect
            try:
                fork_thread = threading.Thread(target=do_fork)
                fork_thread.start()
                # No sleeps: the copy parks at its first progress callback,
                # provably incomplete, until the writer commits below.
                self.assertTrue(gate.copy_started.wait(15))
                self.assertTrue(fork_thread.is_alive())
                self.assertGreater(gate.remaining_at_sync, 0)
                # w1..w3 commit through the public API while the copy runs.
                self.assertEqual(pub.promote(r_d, expected_generation=1), 2)
                self.assertEqual(
                    pub.publish_group({"g2": (r_e, {})}, expected_epoch=1), 2)
                self.assertEqual(
                    pub.collect_retired(after=r_f - 1, scan_limit=1)["deleted"],
                    (r_f,))
                gate.writer_done.set()
                fork_thread.join(30)
            finally:
                sqlite3.connect = original_connect
                keeper.close()

            self.assertFalse(fork_thread.is_alive())
            self.assertNotIn("error", outcome)
            copy_branch = outcome["branch"]
            self.assertNotEqual(copy_branch, branch_of(src_path))

            # The copy must match exactly one commit boundary: P1 alone, or
            # P1 plus a prefix of the in-flight commits w1, w2, w3.
            copy = Distribution(dst_path)
            generation, active, _ = copy.poll()
            epoch = copy._current_epoch()
            with sqlite3.connect(dst_path) as db:
                r_f_body = db.execute(
                    "SELECT 1 FROM revision WHERE id=?", (r_f,)).fetchone() is not None
                r_f_retired = db.execute(
                    "SELECT 1 FROM retired_revision WHERE revision=?",
                    (r_f,)).fetchone() is not None
                g2_bound = db.execute(
                    "SELECT 1 FROM group_binding WHERE name='g2'").fetchone() is not None
                r_c_body = db.execute(
                    "SELECT 1 FROM revision WHERE id=?", (r_c,)).fetchone() is not None
            observed = (generation, active, epoch, g2_bound, r_f_body, r_f_retired)
            allowed = (
                (1, r_a, 1, False, True, True),    # P1 only
                (2, r_d, 1, False, True, True),    # + w1 promote
                (2, r_d, 2, True, True, True),     # + w2 group publish
                (2, r_d, 2, True, False, False),   # + w3 collect
            )
            self.assertIn(observed, allowed)

            # Whatever the boundary, P1's content is present and coherent:
            # pointer, group reply, bodies and reclaim state all agree.
            self.assertTrue(r_c_body)  # retired but protected by the group
            self.assertEqual(copy.read_pin(pin_b),
                             {r_b: {"upstream": "bravo", "timeout_ms": 2}})
            reply = copy.poll_group_cursor(("g1marker", "g2"), cursor=None)
            self.assertEqual(reply["cursor"], (copy_branch, epoch))
            self.assertEqual(reply["epoch"], epoch)
            expected_scope = {"g1marker"} if epoch == 1 else {"g1marker", "g2"}
            self.assertEqual(set(reply["put"]), expected_scope)

            # The source reached the final state and stays usable.
            self.assertEqual(pub.poll()[:2], (2, r_d))
            self.assertEqual(pub._current_epoch(), 2)


class _BackupGate:
    def __init__(self):
        self.copy_started = threading.Event()
        self.writer_done = threading.Event()
        self.synced = False
        self.remaining_at_sync = None


class _BackupGateProxy:
    """Wraps a real connection; forces paged backups and parks the first
    progress callback until the test's writer has committed, so a write lands
    strictly while the copy is incomplete (no sleeps involved)."""

    def __init__(self, conn, gate):
        self._conn = conn
        self._gate = gate

    def backup(self, target, **kwargs):
        kwargs["pages"] = 1
        kwargs["progress"] = self._progress
        return self._conn.backup(target, **kwargs)

    def _progress(self, status, remaining, total):
        gate = self._gate
        if not gate.synced:
            gate.synced = True
            gate.remaining_at_sync = remaining
            gate.copy_started.set()
            gate.writer_done.wait(15)

    def execute(self, sql, *args):
        return self._conn.execute(sql, *args)

    def close(self):
        return self._conn.close()

    def __enter__(self):
        self._conn.__enter__()
        return self

    def __exit__(self, *exc_info):
        return self._conn.__exit__(*exc_info)

    def __getattr__(self, name):
        return getattr(self._conn, name)


class ForkCanaryPinReclaimTest(unittest.TestCase):
    def test_inflight_canary_dropped_pins_and_high_water_kept(self):
        with TemporaryDirectory() as tmp:
            src_path = Path(tmp, "main.db")
            copy_path = Path(tmp, "copy.db")
            pub = Distribution(src_path)
            stable = pub.stage({"upstream": "stable", "timeout_ms": 1})
            candidate = pub.stage({"upstream": "candidate", "timeout_ms": 2})
            kept = pub.stage({"upstream": "kept", "timeout_ms": 3})
            doomed = pub.stage({"upstream": "doomed", "timeout_ms": 4})
            self.assertEqual(pub.promote(stable, expected_generation=0), 1)
            trial, selected = pub.begin_canary(
                candidate, expected_generation=1,
                roster=[("i1", "z", "r", 1)], max_load=1)
            self.assertEqual(selected, ("i1",))
            # Delivered but unfinished when the fork happens.
            self.assertEqual(pub.poll_canary(trial, "i1")[:2], (trial, candidate))
            pin = pub.pin_revisions((kept,))
            # A reclaimed high version id: the high-water mark must travel.
            pub.retire_revisions((doomed,))
            self.assertEqual(pub.collect_retired()["deleted"], (doomed,))

            copy_branch = pub.fork_recovery(copy_path)
            copy = Distribution(copy_path)

            # The source keeps running the old trial, undisturbed.
            self.assertTrue(pub.ack_canary(trial, "i1"))
            self.assertEqual(pub.finish_canary(trial), 2)
            self.assertEqual(pub.poll()[:2], (2, candidate))

            # The copy cannot consume the dropped trial's deliveries or acks.
            self.assertIsNone(copy.poll_canary(trial, "i1"))
            self.assertFalse(copy.ack_canary(trial, "i1"))
            self.assertIsNone(copy.finish_canary(trial))
            self.assertFalse(copy.abort_canary(trial))
            # Dropping the trial advanced nothing and collected nothing.
            self.assertEqual(copy.poll()[:2], (1, stable))
            self.assertEqual(copy._current_epoch(), 0)

            # The copy immediately runs its own trial under a fresh id.
            trial2, selected2 = copy.begin_canary(
                candidate, expected_generation=1,
                roster=[("i1", "z", "r", 1)], max_load=1)
            self.assertEqual(selected2, ("i1",))
            self.assertGreater(trial2, trial)
            self.assertEqual(copy.poll_canary(trial2, "i1")[:2], (trial2, candidate))
            self.assertTrue(copy.ack_canary(trial2, "i1"))
            self.assertEqual(copy.finish_canary(trial2), 2)

            # Duty pins read back on the copy; pin ids keep moving forward.
            self.assertEqual(copy.read_pin(pin),
                             {kept: {"upstream": "kept", "timeout_ms": 3}})
            self.assertGreater(copy.pin_revisions((kept,)), pin)

            # The reclaimed high id is never reissued on the copy.
            with self.assertRaises(KeyError):
                copy.pin_revisions((doomed,))
            self.assertGreater(copy.stage(config("fresh")), doomed)

            # The source side is equally intact.
            self.assertEqual(pub.read_pin(pin),
                             {kept: {"upstream": "kept", "timeout_ms": 3}})
            self.assertGreater(pub.stage(config("later")), doomed)
            self.assertNotEqual(branch_of(copy_path), branch_of(src_path))
            self.assertEqual(branch_of(copy_path), copy_branch)


class ForkFaultInjectionTest(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.path = Path(self._tmp.name, "main.db")
        self.pub = Distribution(self.path)
        rev = self.pub.stage(config("base"))
        self.pub.promote(rev, expected_generation=0)
        self.pub.publish_group({"c": (rev, {})}, expected_epoch=0)
        self.branch = branch_of(self.path)

    def tearDown(self):
        self._tmp.cleanup()

    def assert_source_healthy(self):
        self.assertEqual(self.pub.poll()[0], 1)
        self.assertEqual(self.pub._current_epoch(), 1)
        self.assertEqual(branch_of(self.path), self.branch)

    def assert_directory_holds(self, *names):
        self.assertEqual(sorted(os.listdir(self._tmp.name)), sorted(names))

    def test_missing_parent_directory_is_a_file_error(self):
        with self.assertRaises(FileNotFoundError):
            self.pub.fork_recovery(Path(self._tmp.name, "no-such-dir", "copy.db"))
        # A parent that is a plain file fails the same way.
        plain = Path(self._tmp.name, "plain")
        plain.write_text("not a directory")
        with self.assertRaises(OSError):
            self.pub.fork_recovery(Path(plain, "copy.db"))
        self.assert_source_healthy()
        self.assert_directory_holds("main.db", "plain")  # no temp files left

    def test_existing_destination_is_never_clobbered(self):
        taken = Path(self._tmp.name, "taken.db")
        taken.write_text("keep me")
        with self.assertRaises(FileExistsError):
            self.pub.fork_recovery(taken)
        self.assertEqual(taken.read_text(), "keep me")  # untouched
        with self.assertRaises(FileExistsError):
            self.pub.fork_recovery(self.path)           # the source itself
        with self.assertRaises(FileExistsError):
            self.pub.fork_recovery(Path(self._tmp.name))  # an existing directory
        self.assertEqual(taken.read_text(), "keep me")
        self.assert_source_healthy()
        self.assert_directory_holds("main.db", "taken.db")  # no temp files left

    def test_backup_failure_leaves_no_trace(self):
        original_connect = sqlite3.connect

        class FailingBackupProxy:
            def __init__(self, conn):
                self._conn = conn

            def backup(self, target, **kwargs):
                raise RuntimeError("injected backup failure")

            def __getattr__(self, name):
                return getattr(self._conn, name)

        def fake_connect(path, *args, **kwargs):
            conn = original_connect(path, *args, **kwargs)
            if os.fspath(path) == str(self.path):
                return FailingBackupProxy(conn)
            return conn

        dest = Path(self._tmp.name, "copy.db")
        sqlite3.connect = fake_connect
        try:
            with self.assertRaises(RuntimeError):
                self.pub.fork_recovery(dest)
        finally:
            sqlite3.connect = original_connect
        self.assertFalse(os.path.lexists(dest))
        self.assert_source_healthy()
        self.assert_directory_holds("main.db")  # temp copy fully cleaned up

    def test_destination_connect_failure_leaves_no_trace(self):
        original_connect = sqlite3.connect

        def fake_connect(path, *args, **kwargs):
            if str(path).endswith(".fork"):
                raise OSError("injected connect failure")
            return original_connect(path, *args, **kwargs)

        dest = Path(self._tmp.name, "copy.db")
        sqlite3.connect = fake_connect
        try:
            with self.assertRaises(OSError):
                self.pub.fork_recovery(dest)
        finally:
            sqlite3.connect = original_connect
        self.assertFalse(os.path.lexists(dest))
        self.assert_source_healthy()
        self.assert_directory_holds("main.db")  # temp copy fully cleaned up

    def test_fixup_failure_leaves_no_trace(self):
        original_connect = sqlite3.connect
        state = {"temp_connections": 0}

        class FixupProxy:
            def __init__(self, conn):
                self._conn = conn

            def execute(self, sql, *args):
                if sql.lstrip().upper().startswith("DELETE FROM CANARY"):
                    raise RuntimeError("injected fixup failure")
                return self._conn.execute(sql, *args)

            def close(self):
                return self._conn.close()

            def __enter__(self):
                self._conn.__enter__()
                return self

            def __exit__(self, *exc_info):
                return self._conn.__exit__(*exc_info)

            def __getattr__(self, name):
                return getattr(self._conn, name)

        def fake_connect(path, *args, **kwargs):
            conn = original_connect(path, *args, **kwargs)
            if str(path).endswith(".fork"):
                state["temp_connections"] += 1
                if state["temp_connections"] == 2:  # the fixup connection
                    return FixupProxy(conn)
            return conn

        dest = Path(self._tmp.name, "copy.db")
        sqlite3.connect = fake_connect
        try:
            with self.assertRaises(RuntimeError):
                self.pub.fork_recovery(dest)
        finally:
            sqlite3.connect = original_connect
        self.assertFalse(os.path.lexists(dest))
        self.assert_source_healthy()
        self.assert_directory_holds("main.db")  # temp copy fully cleaned up

    def test_same_destination_race_has_exactly_one_winner(self):
        dest = Path(self._tmp.name, "raced.db")
        barrier = threading.Barrier(2)
        outcomes = {}

        def worker(name):
            barrier.wait()
            try:
                outcomes[name] = ("ok", self.pub.fork_recovery(dest))
            except FileExistsError:
                outcomes[name] = ("exists", None)

        threads = [threading.Thread(target=worker, args=(name,))
                   for name in ("first", "second")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(30)
        self.assertFalse(any(thread.is_alive() for thread in threads))
        results = sorted(outcomes.values())
        self.assertEqual([kind for kind, _ in results], ["exists", "ok"])
        winner_branch = [branch for kind, branch in results if kind == "ok"][0]
        # The visible copy is complete and belongs to the single winner.
        self.assertEqual(branch_of(dest), winner_branch)
        self.assertEqual(Distribution(dest).poll()[0], 1)
        self.assert_source_healthy()
        self.assert_directory_holds("main.db", "raced.db")  # no temp files left

    def test_parallel_forks_to_distinct_paths(self):
        barrier = threading.Barrier(2)
        outcomes = {}

        def worker(name):
            barrier.wait()
            outcomes[name] = self.pub.fork_recovery(
                Path(self._tmp.name, "%s.db" % name))

        threads = [threading.Thread(target=worker, args=(name,))
                   for name in ("one", "two")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(30)
        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertNotEqual(outcomes["one"], outcomes["two"])
        self.assertNotIn(outcomes["one"], (self.branch, outcomes["two"]))
        self.assertNotIn(outcomes["two"], (self.branch,))
        self.assertEqual(branch_of(Path(self._tmp.name, "one.db")), outcomes["one"])
        self.assertEqual(branch_of(Path(self._tmp.name, "two.db")), outcomes["two"])
        self.assert_source_healthy()
        self.assert_directory_holds("main.db", "one.db", "two.db")


class ForkScaleTest(unittest.TestCase):
    """1024 staged bodies of ~160 KiB each; the fork streams within budget."""

    def test_large_library_recovery_within_rss_budget(self):
        with TemporaryDirectory() as tmp:
            src = Path(tmp, "scale.db")
            dst = Path(tmp, "scale-copy.db")
            pub = Distribution(src)

            def body(i):
                text = "u%04d-" % i + "x" * (160 * 1024 - 64)
                return {"upstream": text, "timeout_ms": i + 1}

            revisions = [pub.stage(body(i)) for i in range(1024)]
            self.assertEqual(len(set(revisions)), 1024)
            with sqlite3.connect(src) as db:
                sample = db.execute(
                    "SELECT config FROM revision WHERE id=?",
                    (revisions[0],)).fetchone()[0]
            self.assertGreaterEqual(len(sample), 150 * 1024)
            self.assertLessEqual(len(sample), 256 * 1024)

            # Publish a few through both paths; the rest stays as inventory.
            self.assertEqual(pub.promote(revisions[0], expected_generation=0), 1)
            components = tuple("comp%02d" % i for i in range(8))
            pub.publish_group(
                {name: (revisions[i + 1], {}) for i, name in enumerate(components)},
                expected_epoch=0)
            pin = pub.pin_revisions(tuple(revisions[9:41]))  # one full 32-id pin

            # The recovery runs in a dedicated process: only its peak RSS from
            # start to copy completion is measured here.
            script = (
                "import sys, resource\n"
                "sys.path.insert(0, sys.argv[1])\n"
                "from distribution import Distribution\n"
                "branch = Distribution(sys.argv[2]).fork_recovery(sys.argv[3])\n"
                "print('branch=' + branch)\n"
                "print('maxrss=%d' % resource.getrusage("
                "resource.RUSAGE_SELF).ru_maxrss)\n")
            project_dir = str(Path(__file__).resolve().parent)
            proc = subprocess.run(
                [sys.executable, "-B", "-I", "-c", script, project_dir,
                 str(src), str(dst)],
                capture_output=True, text=True, timeout=600)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            report = dict(line.split("=", 1)
                          for line in proc.stdout.strip().splitlines())
            maxrss_kib = int(report["maxrss"])
            self.assertLessEqual(
                maxrss_kib, 96 * 1024,
                "recovery peaked at %d KiB RSS" % maxrss_kib)
            copy_branch = report["branch"]
            self.assertIsNotNone(BRANCH_RE.fullmatch(copy_branch))
            self.assertNotEqual(copy_branch, branch_of(src))

            # Spot-check exact content of both published and never-published
            # bodies on the copy.
            copy = Distribution(dst)
            self.assertEqual(copy.poll()[:2], (1, revisions[0]))
            self.assertEqual(copy.poll()[2], body(0))
            reply = copy.poll_group_cursor(components, cursor=None)
            self.assertEqual(reply["cursor"], (copy_branch, 1))
            self.assertEqual(set(reply["put"]), set(components))
            for i, name in enumerate(components):
                self.assertEqual(reply["put"][name], (revisions[i + 1], body(i + 1)))
            with sqlite3.connect(dst) as db:
                for i in (500, 777, 1023):  # never published anywhere
                    stored = db.execute(
                        "SELECT config FROM revision WHERE id=?",
                        (revisions[i],)).fetchone()[0]
                    self.assertEqual(json.loads(stored), body(i))
            self.assertEqual(copy.read_pin(pin)[revisions[9]], body(9))
            self.assertEqual(copy.read_pin(pin)[revisions[40]], body(40))

            # The copy stands alone: moved away from every sibling it still
            # opens and serves exact content.
            elsewhere = Path(tmp, "elsewhere")
            elsewhere.mkdir()
            moved = Path(elsewhere, "moved.db")
            os.rename(dst, moved)
            lone = Distribution(moved)
            self.assertEqual(lone.poll()[:2], (1, revisions[0]))
            self.assertEqual(lone.read_pin(pin)[revisions[10]], body(10))
            self.assertEqual(branch_of(moved), copy_branch)

            # The source is untouched and complete.
            self.assertEqual(pub.poll()[:2], (1, revisions[0]))
            with sqlite3.connect(src) as db:
                count = db.execute("SELECT COUNT(*) FROM revision").fetchone()[0]
            self.assertEqual(count, 1024)


if __name__ == "__main__":
    unittest.main()
