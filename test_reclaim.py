"""Acceptance tests for staged-version registration, pinning and reclamation."""
from pathlib import Path
from tempfile import TemporaryDirectory
import sqlite3
import threading
import unittest

from distribution import Distribution

MAX_ID = (1 << 63) - 1


def config(tag):
    return {"upstream": tag, "timeout_ms": 1}


class ReclaimBase(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.path = Path(self._tmp.name, "state.db")
        self.pub = Distribution(self.path)

    def tearDown(self):
        self._tmp.cleanup()

    def stage(self, tag):
        return self.pub.stage(config(tag))


class RegistrationTest(ReclaimBase):
    def test_retire_only_registers_and_versions_stay_usable(self):
        stable = self.stage("stable")
        doomed = self.stage("doomed")
        never = self.stage("never")
        self.assertEqual(self.pub.promote(stable, expected_generation=0), 1)

        self.assertIsNone(self.pub.retire_revisions((doomed,)))
        self.assertIsNone(self.pub.retire_revisions((doomed,)))  # idempotent
        self.assertIsNone(self.pub.retire_revisions((stable,)))

        # Registration is not deletion: a retired revision still promotes.
        self.assertEqual(self.pub.promote(doomed, expected_generation=1), 2)
        self.assertEqual(self.pub.poll()[:2], (2, doomed))

        # The unprotected retired revision goes away; the retired one now
        # serving as the full pointer stays. The never-registered one is
        # untouched even though nothing references it.
        result = self.pub.collect_retired()
        self.assertEqual(result, {"deleted": (stable,), "retained": (doomed,),
                                  "next_after": None})
        with self.assertRaises(KeyError):
            self.pub.pin_revisions((stable,))
        self.assertGreater(self.pub.pin_revisions((never,)), 0)
        # An unregistered revision is never collected automatically.
        self.assertEqual(self.pub.collect_retired(),
                         {"deleted": (), "retained": (doomed,), "next_after": None})

    def test_full_canary_pins_and_plain_are_protected(self):
        stable = self.stage("stable")
        candidate = self.stage("candidate")
        pinned = self.stage("pinned")
        plain = self.stage("plain")
        self.assertEqual(self.pub.promote(stable, expected_generation=0), 1)
        roster = [("i1", "z", "r", 1)]
        trial, selected = self.pub.begin_canary(
            candidate, expected_generation=1, roster=roster, max_load=1)
        self.assertEqual(selected, ("i1",))
        pin_a = self.pub.pin_revisions((pinned,))
        pin_b = self.pub.pin_revisions((pinned,))
        self.assertGreater(pin_b, pin_a)

        self.pub.retire_revisions((stable, candidate, pinned))
        self.assertEqual(
            self.pub.collect_retired(scan_limit=64),
            {"deleted": (), "retained": (stable, candidate, pinned),
             "next_after": None})
        # Plain was never registered, so collection never even considers it.
        self.assertEqual(self.pub.collect_retired(scan_limit=64)["deleted"], ())

        # Finish the canary: the candidate becomes the full configuration and
        # must remain protected even though it is no longer a canary candidate.
        self.pub.poll_canary(trial, "i1")
        self.assertTrue(self.pub.ack_canary(trial, "i1"))
        self.assertEqual(self.pub.finish_canary(trial), 2)
        result = self.pub.collect_retired(scan_limit=64)
        self.assertEqual(result["deleted"], (stable,))
        self.assertEqual(result["retained"], (candidate, pinned))

        # Move the full pointer away: the old candidate now has no reference.
        self.assertEqual(self.pub.promote(plain, expected_generation=2), 3)
        result = self.pub.collect_retired(scan_limit=64)
        self.assertEqual(result["deleted"], (candidate,))
        self.assertEqual(result["retained"], (pinned,))

        # Two pins held the same body: releasing one preserves the other, and a
        # reopened connection still receives the exact body through it.
        self.assertTrue(self.pub.unpin_revisions(pin_a))
        self.assertFalse(self.pub.unpin_revisions(pin_a))
        reopened = Distribution(self.path)
        self.assertEqual(reopened.collect_retired(scan_limit=64)["deleted"], ())
        self.assertEqual(reopened.read_pin(pin_b), {pinned: config("pinned")})
        # Releasing the last pin allows collection; releasing never cancels the
        # retirement registration by itself.
        self.assertTrue(reopened.unpin_revisions(pin_b))
        self.assertIsNone(reopened.read_pin(pin_b))
        self.assertEqual(reopened.collect_retired(scan_limit=64)["deleted"], (pinned,))

        # The never-registered revision still serves normal publishing.
        self.assertEqual(reopened.promote(plain, expected_generation=3), 4)

    def test_pin_identity_and_reopen(self):
        first = self.stage("first")
        second = self.stage("second")
        pin_one = self.pub.pin_revisions((second, first))  # order irrelevant
        pin_two = self.pub.pin_revisions((second, first))
        self.assertGreater(pin_two, pin_one)
        expected = {first: config("first"), second: config("second")}
        self.assertEqual(self.pub.read_pin(pin_one), expected)

        reopened = Distribution(self.path)
        self.assertEqual(reopened.read_pin(pin_two), expected)
        self.assertTrue(reopened.unpin_revisions(pin_one))
        self.assertIsNone(reopened.read_pin(pin_one))
        pin_three = reopened.pin_revisions((second,))
        self.assertGreater(pin_three, pin_two)  # ids are never reused

        # Even with every pin released, a fresh pin id still exceeds all old
        # ones, so an old id can never resolve to a different pin.
        self.assertTrue(reopened.unpin_revisions(pin_two))
        self.assertTrue(reopened.unpin_revisions(pin_three))
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM pin").fetchone()[0], 0)
        pin_four = Distribution(self.path).pin_revisions((first,))
        self.assertGreater(pin_four, pin_three)
        self.assertIsNone(reopened.read_pin(pin_three))

    def test_batch_validation_and_atomic_failure(self):
        a, b = self.stage("a"), self.stage("b")
        for bad in ([a], (), (a, a), (True,), (0,), (MAX_ID + 1,), (1.0,),
                    ("1",), tuple(range(1, 34))):
            with self.assertRaises(ValueError):
                self.pub.retire_revisions(bad)
            with self.assertRaises(ValueError):
                self.pub.pin_revisions(bad)

        # A missing but well-formed id fails the whole retire batch: the valid
        # neighbour is not registered.
        with self.assertRaises(KeyError):
            self.pub.retire_revisions((a, 999999))
        self.assertEqual(self.pub.collect_retired(),
                         {"deleted": (), "retained": (), "next_after": None})

        # Likewise a pin batch with a missing id creates no pin and no members.
        with self.assertRaises(KeyError):
            self.pub.pin_revisions((b, 999999))
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM pin").fetchone()[0], 0)
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM pin_member").fetchone()[0], 0)

        # Single-id arguments are range checked too.
        with self.assertRaises(ValueError):
            self.pub.read_pin(0)
        with self.assertRaises(ValueError):
            self.pub.read_pin(True)
        with self.assertRaises(ValueError):
            self.pub.unpin_revisions(MAX_ID + 1)
        self.assertIsNone(self.pub.read_pin(777))
        self.assertFalse(self.pub.unpin_revisions(777))

        # After all that, both valid batches still succeed wholesale.
        self.pub.retire_revisions((b, a))
        pin = self.pub.pin_revisions((b, a))
        self.assertEqual(set(self.pub.read_pin(pin)), {a, b})


    def test_collection_leaves_publishing_state_untouched(self):
        stable = self.stage("stable")
        candidate = self.stage("candidate")
        doomed = self.stage("doomed")
        self.assertEqual(self.pub.promote(stable, expected_generation=0), 1)
        trial, _ = self.pub.begin_canary(
            candidate, expected_generation=1, roster=[("i1", "z", "r", 1)],
            max_load=1)
        epoch = self.pub.publish_group({"c": (stable, {})}, expected_epoch=0)
        self.pub.retire_revisions((doomed,))

        self.assertEqual(self.pub.collect_retired()["deleted"], (doomed,))
        # No canary / generation / group side effects.
        self.assertEqual(self.pub.poll()[:2], (1, stable))
        self.assertEqual(self.pub._current_epoch(), epoch)
        self.assertEqual(self.pub.poll_group(("c",), known_epoch=epoch - 1)["epoch"],
                         epoch)
        self.assertEqual(self.pub.poll_canary(trial, "i1")[:2], (trial, candidate))
        self.assertTrue(self.pub.abort_canary(trial))


class CollectCursorTest(ReclaimBase):
    def test_cursor_moves_past_protected_prefix(self):
        revisions = [self.stage("r%d" % i) for i in range(5)]
        pin = self.pub.pin_revisions((revisions[0], revisions[1]))
        self.pub.retire_revisions(tuple(revisions))

        # Both protected ids fill the scan window; the cursor still advances
        # instead of looping forever at the protected prefix.
        self.assertEqual(
            self.pub.collect_retired(after=0, scan_limit=2),
            {"deleted": (), "retained": tuple(revisions[:2]), "next_after": revisions[1]})
        self.assertEqual(
            self.pub.collect_retired(after=revisions[1], scan_limit=2),
            {"deleted": tuple(revisions[2:4]), "retained": (),
             "next_after": revisions[3]})
        self.assertEqual(
            self.pub.collect_retired(after=revisions[3], scan_limit=2),
            {"deleted": (revisions[4],), "retained": (), "next_after": None})
        # A fresh round may start at 0; only the still-retired pins remain.
        self.assertEqual(
            self.pub.collect_retired(after=0, scan_limit=64),
            {"deleted": (), "retained": tuple(revisions[:2]), "next_after": None})

        self.assertTrue(self.pub.unpin_revisions(pin))
        self.assertEqual(
            self.pub.collect_retired(after=0, scan_limit=64),
            {"deleted": tuple(revisions[:2]), "retained": (), "next_after": None})

    def test_argument_validation_and_empty_batch(self):
        for bad_after, bad_limit in ((True, 1), (-1, 1), (MAX_ID + 1, 1),
                                     (1.0, 1), (0, 0), (0, 65), (0, True), (0, 1.0)):
            with self.assertRaises(ValueError):
                self.pub.collect_retired(after=bad_after, scan_limit=bad_limit)
        self.assertEqual(self.pub.collect_retired(),
                         {"deleted": (), "retained": (), "next_after": None})
        # Boundary values are accepted.
        self.assertEqual(
            self.pub.collect_retired(after=MAX_ID, scan_limit=1),
            {"deleted": (), "retained": (), "next_after": None})


class GroupWindowReclaimTest(ReclaimBase):
    def publish(self, revision, epoch):
        return self.pub.publish_group({"c": (revision, {})}, expected_epoch=epoch)

    def test_window_boundary_and_polling_modes(self):
        a = self.stage("A")
        b = self.stage("B")
        c = self.stage("C")
        self.assertEqual(self.publish(a, 0), 1)
        self.assertEqual(self.publish(b, 1), 2)
        self.assertEqual(self.publish(c, 2), 3)
        self.pub.retire_revisions((a, b, c))

        epoch = 3
        while epoch < 256:
            epoch = self.publish(c, epoch)
        self.assertEqual(epoch, 256)

        # Epochs 1 and 2 are still inside the window (baseline at epoch 1).
        self.assertEqual(
            self.pub.collect_retired(scan_limit=64),
            {"deleted": (), "retained": (a, b, c), "next_after": None})
        self.assertEqual(self.pub.poll_group(("c",), known_epoch=1)["mode"], "delta")
        self.assertEqual(self.pub.poll_group(("c",), known_epoch=0)["mode"], "full")

        # At epoch 256 both old revisions can still be pinned and read back.
        pin_a = self.pub.pin_revisions((a,))
        pin_b = self.pub.pin_revisions((b,))
        self.assertEqual(self.pub.read_pin(pin_a), {a: config("A")})
        self.assertEqual(self.pub.read_pin(pin_b), {b: config("B")})
        self.assertTrue(self.pub.unpin_revisions(pin_a))
        self.assertTrue(self.pub.unpin_revisions(pin_b))
        self.assertEqual(self.pub.collect_retired(scan_limit=64)["deleted"], ())

        # Epoch 257 moves the baseline to 2: A leaves the window, B stays.
        self.assertEqual(self.publish(c, 256), 257)
        self.assertEqual(
            self.pub.collect_retired(scan_limit=64),
            {"deleted": (a,), "retained": (b, c), "next_after": None})
        with self.assertRaises(KeyError):
            self.pub.pin_revisions((a,))
        pin_b = self.pub.pin_revisions((b,))
        self.assertEqual(self.pub.read_pin(pin_b), {b: config("B")})
        self.assertTrue(self.pub.unpin_revisions(pin_b))
        # Baseline is 2, so epoch 2 is the oldest reconstructable epoch.
        self.assertEqual(self.pub.poll_group(("c",), known_epoch=2)["mode"], "delta")
        self.assertEqual(self.pub.poll_group(("c",), known_epoch=1)["mode"], "full")

        # Epoch 258 moves the baseline to 3: only B leaves, C is still bound.
        self.assertEqual(self.publish(c, 257), 258)
        self.assertEqual(
            self.pub.collect_retired(scan_limit=64),
            {"deleted": (b,), "retained": (c,), "next_after": None})
        with self.assertRaises(KeyError):
            self.pub.pin_revisions((b,))
        self.assertEqual(self.pub.poll_group(("c",), known_epoch=3)["mode"], "delta")
        self.assertEqual(self.pub.poll_group(("c",), known_epoch=2)["mode"], "full")
        # The current binding is never collected, even though it is registered.
        self.assertEqual(self.pub.collect_retired(scan_limit=64)["retained"], (c,))
        full = self.pub.poll_group(("c",), known_epoch=None)
        self.assertEqual(full["put"], {"c": (c, config("C"))})


class MaxIdTest(ReclaimBase):
    def test_empty_inventory_never_recycles_ids(self):
        a = self.stage("a")
        b = self.stage("b")
        self.pub.retire_revisions((b,))
        self.assertEqual(self.pub.collect_retired()["deleted"], (b,))
        reopened = Distribution(self.path)
        c = reopened.stage(config("c"))
        self.assertGreater(c, b)
        with self.assertRaises(KeyError):
            reopened.promote(b, expected_generation=0)
        with self.assertRaises(KeyError):
            reopened.pin_revisions((b,))

        # Empty the inventory entirely, then stage again.
        reopened.retire_revisions((a, c))
        self.assertEqual(reopened.collect_retired()["deleted"], (a, c))
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM revision").fetchone()[0], 0)
        again = Distribution(self.path)
        d = again.stage(config("d"))
        self.assertGreater(d, c)
        with self.assertRaises(KeyError):
            again.promote(c, expected_generation=0)


class _Gate:
    """Blocks one chosen thread right after it acquires SQLite's write lock."""

    def __init__(self):
        self.entered = threading.Event()
        self.release = threading.Event()
        self.ident = None
        self.active = False


class _ConnProxy:
    """Delegates to a real connection, pausing at a gated BEGIN IMMEDIATE."""

    def __init__(self, conn, gate):
        self._conn = conn
        self._gate = gate

    def execute(self, sql, *args):
        cursor = self._conn.execute(sql, *args)
        gate = self._gate
        if (gate.active and gate.ident == threading.get_ident()
                and sql.lstrip().upper().startswith("BEGIN IMMEDIATE")):
            gate.active = False
            gate.entered.set()
            gate.release.wait(15)
        return cursor

    def close(self):
        return self._conn.close()

    def __enter__(self):
        self._conn.__enter__()
        return self

    def __exit__(self, *exc_info):
        return self._conn.__exit__(*exc_info)

    def __getattr__(self, name):
        return getattr(self._conn, name)


class ReclaimRaceTest(unittest.TestCase):
    def test_sequential_orders(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "state.db")
            pinner, collector = Distribution(path), Distribution(path)
            held = pinner.stage(config("held"))
            pinner.retire_revisions((held,))
            pin_id = pinner.pin_revisions((held,))
            # Reference first: collection must leave the body alone.
            self.assertEqual(collector.collect_retired()["retained"], (held,))
            self.assertEqual(collector.read_pin(pin_id), {held: config("held")})

            gone = pinner.stage(config("gone"))
            pinner.retire_revisions((gone,))
            # Collection first: a later reference sees an unknown revision.
            self.assertEqual(collector.collect_retired()["deleted"], (gone,))
            with self.assertRaises(KeyError):
                pinner.pin_revisions((gone,))

    def _forced_race(self, winner, verify):
        """Race a pin and a collection with a lock-chosen commit order.

        The ``winner`` operation is started first and, right after acquiring
        SQLite's write lock, pauses until the loser is launched; the gate is
        then released, so the winner commits before the loser can even begin
        its own write transaction. Ordering comes from the lock, not a sleep.
        """
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "state.db")
            setup = Distribution(path)
            revision = setup.stage(config("race"))
            setup.retire_revisions((revision,))

            gate = _Gate()
            original_connect = sqlite3.connect
            sqlite3.connect = lambda *a, **k: _ConnProxy(original_connect(*a, **k), gate)
            try:
                pinner = Distribution(path)
                collector = Distribution(path)
                outcome = {}

                def pin_op():
                    try:
                        return pinner.pin_revisions((revision,))
                    except KeyError:
                        return None

                def collect_op():
                    return collector.collect_retired(scan_limit=64)

                operations = {"pin": pin_op, "collect": collect_op}

                def winning_call():
                    gate.ident = threading.get_ident()
                    gate.active = True
                    outcome[winner] = operations[winner]()

                winner_thread = threading.Thread(target=winning_call)
                winner_thread.start()
                self.assertTrue(gate.entered.wait(15))
                self.assertFalse(gate.active)  # the winner holds the write lock
                loser = "collect" if winner == "pin" else "pin"
                loser_thread = threading.Thread(
                    target=lambda: outcome.setdefault(loser, operations[loser]()))
                loser_thread.start()
                gate.release.set()
                winner_thread.join(20)
                loser_thread.join(20)
            finally:
                sqlite3.connect = original_connect

            self.assertFalse(winner_thread.is_alive())
            self.assertFalse(loser_thread.is_alive())
            return verify(pinner, revision, outcome)

    def test_reference_wins_then_collect_retains(self):
        def verify(pinner, revision, outcome):
            self.assertIsInstance(outcome["pin"], int)
            self.assertEqual(outcome["collect"]["deleted"], ())
            self.assertEqual(outcome["collect"]["retained"], (revision,))
            self.assertEqual(pinner.read_pin(outcome["pin"]),
                             {revision: config("race")})

        self._forced_race("pin", verify)

    def test_delete_wins_then_late_reference_is_unknown(self):
        def verify(pinner, revision, outcome):
            self.assertEqual(outcome["collect"]["deleted"], (revision,))
            self.assertIsNone(outcome["pin"])
            # The body is gone for every later reader too, not just the racer.
            with self.assertRaises(KeyError):
                pinner.pin_revisions((revision,))
            with self.assertRaises(KeyError):
                pinner.promote(revision, expected_generation=0)

        self._forced_race("collect", verify)


class PromoteRaceTest(unittest.TestCase):
    """The same lock-chosen ordering, with promotion as the competing reference."""

    def _race(self, winner, verify):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "state.db")
            setup = Distribution(path)
            stable = setup.stage(config("stable"))
            setup.promote(stable, expected_generation=0)
            retired = setup.stage(config("retired"))
            setup.retire_revisions((retired,))

            gate = _Gate()
            original_connect = sqlite3.connect
            sqlite3.connect = lambda *a, **k: _ConnProxy(original_connect(*a, **k), gate)
            try:
                publisher = Distribution(path)
                collector = Distribution(path)
                outcome = {}

                def promote_op():
                    try:
                        return publisher.promote(retired, expected_generation=1)
                    except KeyError:
                        return None

                def collect_op():
                    return collector.collect_retired(scan_limit=64)

                operations = {"promote": promote_op, "collect": collect_op}

                def winning_call():
                    gate.ident = threading.get_ident()
                    gate.active = True
                    outcome[winner] = operations[winner]()

                winner_thread = threading.Thread(target=winning_call)
                winner_thread.start()
                self.assertTrue(gate.entered.wait(15))
                self.assertFalse(gate.active)
                loser = "collect" if winner == "promote" else "promote"
                loser_thread = threading.Thread(
                    target=lambda: outcome.setdefault(loser, operations[loser]()))
                loser_thread.start()
                gate.release.set()
                winner_thread.join(20)
                loser_thread.join(20)
            finally:
                sqlite3.connect = original_connect

            self.assertFalse(winner_thread.is_alive())
            self.assertFalse(loser_thread.is_alive())
            return verify(publisher, retired, outcome)

    def test_promote_wins_and_body_is_retained(self):
        def verify(publisher, retired, outcome):
            self.assertEqual(outcome["promote"], 2)
            self.assertEqual(outcome["collect"]["deleted"], ())
            self.assertEqual(outcome["collect"]["retained"], (retired,))
            self.assertEqual(publisher.poll()[:2], (2, retired))

        self._race("promote", verify)

    def test_collect_wins_and_promotion_is_unknown(self):
        def verify(publisher, retired, outcome):
            self.assertEqual(outcome["collect"]["deleted"], (retired,))
            self.assertIsNone(outcome["promote"])
            # The full pointer and its generation are untouched by the loss.
            generation, revision, _ = publisher.poll()
            self.assertEqual(generation, 1)
            self.assertNotEqual(revision, retired)
            with self.assertRaises(KeyError):
                publisher.promote(retired, expected_generation=1)

        self._race("collect", verify)


class UpgradeTest(unittest.TestCase):
    def test_existing_database_is_upgraded_in_place(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "state.db")
            # Simulate a database written by the previous build: only the
            # original tables, with a staged and promoted revision.
            with sqlite3.connect(path) as db:
                db.executescript("""
                    CREATE TABLE revision (id INTEGER PRIMARY KEY, config TEXT NOT NULL);
                    CREATE TABLE channel (slot INTEGER PRIMARY KEY CHECK (slot=1),
                        revision INTEGER, generation INTEGER NOT NULL);
                    INSERT INTO channel VALUES (1, NULL, 0);""")
                db.execute("INSERT INTO revision(id, config) VALUES (1, ?)",
                           ('{"timeout_ms": 5, "upstream": "old"}',))
                db.execute("INSERT INTO revision(id, config) VALUES (2, ?)",
                           ('{"timeout_ms": 6, "upstream": "never"}',))
                db.execute("UPDATE channel SET revision=1, generation=3 WHERE slot=1")

            pub = Distribution(path)
            # The old revision and its pointer still work under the old API.
            self.assertEqual(pub.poll()[:2], (3, 1))
            pub.retire_revisions((1,))
            self.assertEqual(pub.collect_retired()["retained"], (1,))
            self.assertEqual(pub.promote(1, expected_generation=3), 4)

            new = pub.stage(config("new"))
            self.assertGreater(new, 2)
            self.assertEqual(pub.promote(new, expected_generation=4), 5)
            # Revision 2 was staged before the upgrade but never registered, so
            # collection never removes it. Reclaim revision 1, reopen and stage:
            # ids only grow and the reclaimed id stays unknown.
            self.assertEqual(pub.collect_retired()["deleted"], (1,))
            self.assertIsNotNone(pub.pin_revisions((2,)))
            reopened = Distribution(path)
            newer = reopened.stage(config("newer"))
            self.assertGreater(newer, new)
            with self.assertRaises(KeyError):
                reopened.pin_revisions((1,))


if __name__ == "__main__":
    unittest.main()
