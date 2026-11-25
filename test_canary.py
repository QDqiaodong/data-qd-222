"""Acceptance tests for the budgeted representative canary path."""
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import time
import unittest

from distribution import ConflictingPromotion, Distribution


class CanaryBaselineTest(unittest.TestCase):
    def test_small_example_full(self):
        roster = [
            ("a1", "A", "r1", 1),
            ("a2", "A", "r2", 4),
            ("a3", "A", "r2", 4),
            ("b1", "B", "r1", 1),
            ("b2", "B", "r3", 2),
            ("c1", "C", "r1", 1),
            ("c2", "C", "r2", 100),
            ("c3", "C", "r3", 100),
        ]
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "state.db")
            publisher = Distribution(path)
            candidate = publisher.stage({"upstream": "canary", "timeout_ms": 30})
            stable = publisher.stage({"upstream": "stable", "timeout_ms": 40})
            publisher.promote(stable, expected_generation=0)

            with self.assertRaises(ValueError):
                publisher.begin_canary(candidate, expected_generation=1,
                                       roster=roster, max_load=6)
            # The rejected call left no active trial, so ordinary promotion works.
            self.assertEqual(publisher.promote(stable, expected_generation=1), 2)
            self.assertEqual(publisher.promote(stable, expected_generation=2), 3)

            trial_id, selected = publisher.begin_canary(
                candidate, expected_generation=3, roster=roster, max_load=7)
            self.assertEqual(selected, ("a2", "b2", "c1"))

            # Reversing the roster must not change the result.
            publisher.abort_canary(trial_id)
            trial_id2, selected2 = publisher.begin_canary(
                candidate, expected_generation=3, roster=list(reversed(roster)),
                max_load=7)
            self.assertGreater(trial_id2, trial_id)
            self.assertEqual(selected2, ("a2", "b2", "c1"))

            # The lightest per-zone choice (a1, b1, c1) only covers r1, so the
            # coverage-aware choice above is required, not the first cover found.
            # Exact budget boundary is accepted; one less is rejected.
            publisher.abort_canary(trial_id2)
            with self.assertRaises(ValueError):
                publisher.begin_canary(candidate, expected_generation=3,
                                       roster=roster, max_load=6)
            trial_id3, _ = publisher.begin_canary(
                candidate, expected_generation=3, roster=roster, max_load=7)
            self.assertGreater(trial_id3, trial_id2)

            # Full pointer untouched while the trial is open.
            self.assertEqual(publisher.poll()[1], stable)
            publisher.abort_canary(trial_id3)

    def test_argument_and_state_validation(self):
        good = ("i1", "zone-1", "rt_1", 10)
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "state.db")
            publisher = Distribution(path)
            candidate = publisher.stage({"upstream": "c", "timeout_ms": 1})
            stable = publisher.stage({"upstream": "s", "timeout_ms": 2})
            publisher.promote(stable, expected_generation=0)

            def begin(roster, budget):
                return publisher.begin_canary(
                    candidate, expected_generation=1, roster=roster, max_load=budget)

            with self.assertRaises(ValueError):
                begin([], 10)
            with self.assertRaises(ValueError):
                begin([good, good], 10)  # duplicate instance id
            with self.assertRaises(ValueError):
                begin([("bad id", "z", "r", 1)], 10)
            with self.assertRaises(ValueError):
                begin([("", "z", "r", 1)], 10)
            with self.assertRaises(ValueError):
                begin([("x" * 65, "z", "r", 1)], 10)
            with self.assertRaises(ValueError):
                begin([("i", "z", "r", True)], 10)  # bool is not an int load
            with self.assertRaises(ValueError):
                begin([("i", "z", "r", 0)], 10)
            with self.assertRaises(ValueError):
                begin([("i", "z", "r", 1_000_001)], 10)
            with self.assertRaises(ValueError):
                begin([good], -1)
            with self.assertRaises(ValueError):
                begin([good], True)  # bool budget rejected
            with self.assertRaises(ValueError):
                begin([good], 24_000_001)
            with self.assertRaises(ValueError):
                begin([("i", "z", "r", 1.0)], 10)
            with self.assertRaises(ValueError):
                begin([("i", 1, "r", 1)], 10)
            with self.assertRaises(ValueError):
                begin([["i", "z", "r", 1]], 10)  # list, not tuple
            with self.assertRaises(ValueError):
                begin("not-a-list", 10)
            with self.assertRaises(ValueError):
                begin([("i", "z", "r", 1, 9)], 10)  # wrong arity
            with self.assertRaises(KeyError):
                publisher.begin_canary(999, expected_generation=1,
                                       roster=[good], max_load=10)

            # Candidate already being the full revision -> ValueError.
            with self.assertRaises(ValueError):
                publisher.begin_canary(stable, expected_generation=1,
                                       roster=[good], max_load=10)

            # Stale generation -> ConflictingPromotion.
            with self.assertRaises(ConflictingPromotion):
                publisher.begin_canary(candidate, expected_generation=0,
                                       roster=[good], max_load=10)

            # No active trial after all those failures.
            self.assertEqual(publisher.promote(stable, expected_generation=1), 2)

    def test_raster_limits_enforced(self):
        with TemporaryDirectory() as tmp:
            publisher = Distribution(Path(tmp, "state.db"))
            candidate = publisher.stage({"upstream": "c", "timeout_ms": 1})
            publisher.promote(publisher.stage({"upstream": "s", "timeout_ms": 2}),
                              expected_generation=0)
            too_many_zones = [("i%03d" % i, "z%02d" % i, "r", 1) for i in range(25)]
            with self.assertRaises(ValueError):
                publisher.begin_canary(candidate, expected_generation=1,
                                       roster=too_many_zones, max_load=10**9)
            many_runtimes = [("i%02d" % i, "z", "r%02d" % i, 1) for i in range(11)]
            with self.assertRaises(ValueError):
                publisher.begin_canary(candidate, expected_generation=1,
                                       roster=many_runtimes, max_load=10**9)
            big = [("i%04d" % i, "z", "r", 1) for i in range(2401)]
            with self.assertRaises(ValueError):
                publisher.begin_canary(candidate, expected_generation=1,
                                       roster=big, max_load=10**9)


class CanaryLifecycleTest(unittest.TestCase):
    def _roster(self):
        # Three zones and three runtimes so one instance per zone can cover
        # everything. Minimum feasible selection is (a2, b2, c1) at load 7;
        # the lightest per-zone choice (a1, b1, c1) only covers r1 and r3.
        return [
            ("a1", "A", "r1", 1),
            ("a2", "A", "r2", 4),
            ("b1", "B", "r1", 1),
            ("b2", "B", "r3", 2),
            ("c1", "C", "r1", 1),
            ("c2", "C", "r2", 5),
        ]

    def test_two_rounds_lifecycle_and_reconnect(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "state.db")
            publisher = Distribution(path)
            other = Distribution(path)  # second connection object
            candidate = publisher.stage({"upstream": "candidate", "timeout_ms": 7})
            stable = publisher.stage({"upstream": "stable", "timeout_ms": 60})
            publisher.promote(stable, expected_generation=0)
            roster = self._roster()
            budget = 10  # minimum feasible combination (a2, b2, c1) costs 7

            # ---- round 1 ----
            trial1, selected1 = publisher.begin_canary(
                candidate, expected_generation=1, roster=roster, max_load=budget)
            self.assertEqual(selected1, ("a2", "b2", "c1"))
            self.assertIsInstance(trial1, int)
            self.assertGreater(trial1, 0)

            # Full pointer still serves the old revision.
            self.assertEqual(other.poll()[1], stable)

            # Only selected instances can pull the trial config.
            self.assertIsNone(publisher.poll_canary(trial1, "a1"))  # not selected
            self.assertIsNone(publisher.poll_canary(trial1 + 999, "a2"))  # unknown trial
            self.assertEqual(publisher.poll_canary(trial1, "a2")[:2],
                             (trial1, candidate))
            self.assertEqual(other.poll_canary(trial1, "b2")[2]["upstream"],
                             "candidate")  # cross-connection delivery

            # Ack before delivery is rejected; unknown instance/trial rejected.
            self.assertFalse(publisher.ack_canary(trial1, "a1"))
            self.assertFalse(publisher.ack_canary(trial1, "nope"))
            self.assertFalse(publisher.ack_canary(trial1 + 999, "a2"))
            # Delivered but not yet acknowledged: finish must not move pointer.
            self.assertIsNone(publisher.finish_canary(trial1))
            self.assertEqual(other.poll()[1], stable)

            self.assertTrue(publisher.ack_canary(trial1, "a2"))
            # Repeated ack within the same active trial stays True.
            self.assertTrue(publisher.ack_canary(trial1, "a2"))
            # Still missing b2's and c1's acks: finish keeps failing, trial active.
            self.assertIsNone(publisher.finish_canary(trial1))
            self.assertEqual(other.poll()[1], stable)

            # Abort the first round without advancing the generation.
            self.assertTrue(publisher.abort_canary(trial1))
            self.assertFalse(publisher.abort_canary(trial1))  # already closed
            self.assertIsNone(publisher.poll_canary(trial1, "a2"))
            self.assertFalse(publisher.ack_canary(trial1, "a2"))
            self.assertIsNone(publisher.finish_canary(trial1))
            self.assertEqual(other.poll()[0], 1)  # generation unchanged

            # ---- round 2: identical candidate, generation and roster ----
            trial2, selected2 = publisher.begin_canary(
                candidate, expected_generation=1, roster=list(reversed(roster)),
                max_load=budget)
            self.assertGreater(trial2, trial1)  # ids are never reused
            self.assertEqual(selected2, ("a2", "b2", "c1"))

            # Round-1 delivery/ack cannot serve round 2.
            self.assertFalse(publisher.ack_canary(trial2, "a2"))
            self.assertFalse(publisher.ack_canary(trial2, "b2"))
            self.assertFalse(publisher.ack_canary(trial2, "c1"))

            # A brand-new object on the same database sees the live roster.
            reconnect = Distribution(path)
            self.assertIsNone(reconnect.poll_canary(trial1, "a2"))  # closed round
            self.assertEqual(reconnect.poll_canary(trial2, "a2")[:2],
                             (trial2, candidate))
            self.assertIsNone(reconnect.poll_canary(trial2, "a1"))  # not selected
            self.assertTrue(reconnect.ack_canary(trial2, "a2"))
            self.assertTrue(reconnect.ack_canary(trial2, "a2"))  # idempotent retry

            # Late ack from round 1 cannot count for round 2.
            self.assertFalse(publisher.ack_canary(trial1, "b2"))
            self.assertIsNone(publisher.finish_canary(trial2))  # b2, c1 pending
            # Ordinary poll keeps showing the old full value.
            self.assertEqual(publisher.poll()[1], stable)

            # While active, old-path promotion of any revision is blocked.
            with self.assertRaises(ConflictingPromotion):
                publisher.promote(candidate, expected_generation=1)
            with self.assertRaises(ConflictingPromotion):
                publisher.promote(stable, expected_generation=1)
            with self.assertRaises(ConflictingPromotion):
                other.promote(candidate, expected_generation=1)
            # A second concurrent canary is blocked too.
            with self.assertRaises(ConflictingPromotion):
                publisher.begin_canary(candidate, expected_generation=1,
                                       roster=roster, max_load=budget)

            reconnect.poll_canary(trial2, "b2")
            self.assertTrue(publisher.ack_canary(trial2, "b2"))
            # c1 still silent: a competing finish must lose for both callers.
            self.assertIsNone(publisher.finish_canary(trial2))
            reconnect.poll_canary(trial2, "c1")
            self.assertTrue(publisher.ack_canary(trial2, "c1"))

            # ---- competing finishers across two connections ----
            results = []

            def finish_on(conn):
                results.append(conn.finish_canary(trial2))

            t1 = threading.Thread(target=finish_on, args=(publisher,))
            t2 = threading.Thread(target=finish_on, args=(other,))
            t1.start()
            t2.start()
            t1.join()
            t2.join()
            self.assertCountEqual(results, [None, 2])  # exactly one promotion
            self.assertEqual(sum(1 for value in results if value is not None), 1)
            self.assertEqual(publisher.poll()[:2], (2, candidate))  # one gen step
            # Trial is closed: all trial operations now reject.
            self.assertIsNone(publisher.poll_canary(trial2, "a2"))
            self.assertFalse(publisher.ack_canary(trial2, "a2"))
            self.assertIsNone(publisher.finish_canary(trial2))
            self.assertFalse(publisher.abort_canary(trial2))

            # After closure the old path still works, including rollback to a
            # previously used revision under a fresh generation.
            self.assertEqual(publisher.promote(stable, expected_generation=2), 3)
            self.assertEqual(other.poll(2)[:2], (3, stable))

    def test_finish_requires_full_acknowledgement_set(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "state.db")
            publisher = Distribution(path)
            candidate = publisher.stage({"upstream": "c", "timeout_ms": 1})
            stable = publisher.stage({"upstream": "s", "timeout_ms": 2})
            publisher.promote(stable, expected_generation=0)
            roster = self._roster()
            trial_id, selected = publisher.begin_canary(
                candidate, expected_generation=1, roster=roster, max_load=10)
            self.assertEqual(selected, ("a2", "b2", "c1"))
            publisher.poll_canary(trial_id, "a2")
            self.assertTrue(publisher.ack_canary(trial_id, "a2"))
            for _ in range(3):
                self.assertIsNone(publisher.finish_canary(trial_id))
            self.assertEqual(publisher.poll()[1], stable)
            # Completing the remaining acks unblocks the same still-active trial.
            publisher.poll_canary(trial_id, "b2")
            self.assertTrue(publisher.ack_canary(trial_id, "b2"))
            self.assertIsNone(publisher.finish_canary(trial_id))  # c1 still pending
            publisher.poll_canary(trial_id, "c1")
            self.assertTrue(publisher.ack_canary(trial_id, "c1"))
            self.assertEqual(publisher.finish_canary(trial_id), 2)
            self.assertEqual(publisher.poll()[1], candidate)


class CanaryScaleTest(unittest.TestCase):
    def test_24_zones_10_runtimes_optimization(self):
        # 24 zones x 10 runtimes x 10 instances per cell = 2400 instances.
        roster = []
        for zone_index in range(24):
            zone = "z%02d" % zone_index
            for runtime_index in range(10):
                runtime = "r%02d" % runtime_index
                for slot in range(10):
                    load = 1 if runtime_index == 0 else 2
                    instance_id = "%s-%s-%02d" % (zone, runtime, slot)
                    roster.append((instance_id, zone, runtime, load))
        expected = []
        for zone_index in range(24):
            zone = "z%02d" % zone_index
            if zone_index < 15:
                expected.append("%s-r00-00" % zone)
            else:
                expected.append("%s-r%02d-00" % (zone, zone_index - 14))
        expected = tuple(expected)

        with TemporaryDirectory() as tmp:
            path = Path(tmp, "state.db")
            publisher = Distribution(path)
            candidate = publisher.stage({"upstream": "c", "timeout_ms": 1})
            publisher.promote(
                publisher.stage({"upstream": "s", "timeout_ms": 2}),
                expected_generation=0)

            started = time.perf_counter()
            trial_id, selected = publisher.begin_canary(
                candidate, expected_generation=1, roster=roster, max_load=33)
            elapsed = time.perf_counter() - started
            self.assertEqual(selected, expected)
            self.assertEqual(sum(1 if i < 15 else 2 for i in range(24)), 33)
            self.assertLess(elapsed, 12.0, "selection took %.2fs" % elapsed)

            # Every runtime is covered and exactly one instance per zone.
            selected_set = set(selected)
            self.assertEqual(len(selected_set), 24)
            covered = {instance.split("-")[1] for instance in selected}
            self.assertEqual(covered, {"r%02d" % i for i in range(10)})
            zones_used = {instance.split("-")[0] for instance in selected}
            self.assertEqual(len(zones_used), 24)

            # Budget 32 cannot work: any coverage-complete selection needs 33.
            with self.assertRaises(ValueError):
                publisher.begin_canary(candidate, expected_generation=1,
                                       roster=roster, max_load=32)
            publisher.abort_canary(trial_id)

            # Spot-check persistence: reopen and run a delivery/ack round trip.
            reopened = Distribution(path)
            trial2, selected2 = reopened.begin_canary(
                candidate, expected_generation=1, roster=roster, max_load=33)
            self.assertEqual(selected2, expected)
            self.assertEqual(reopened.poll_canary(trial2, expected[0])[:2],
                             (trial2, candidate))
            self.assertTrue(reopened.ack_canary(trial2, expected[0]))
            self.assertTrue(reopened.abort_canary(trial2))


if __name__ == "__main__":
    unittest.main()
