"""Acceptance tests for the configuration-group publishing path."""
from pathlib import Path
from tempfile import TemporaryDirectory
import os
import sqlite3
import threading
import time
import unittest

from distribution import ConflictingPromotion, Distribution


def stage_many(publisher, count):
    return [publisher.stage({"upstream": "u%04d" % i, "timeout_ms": i + 1})
            for i in range(count)]


class GroupValidationTest(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.path = Path(self._tmp.name, "state.db")
        self.pub = Distribution(self.path)
        self.r1, self.r2 = stage_many(self.pub, 2)

    def tearDown(self):
        self._tmp.cleanup()

    def publish(self, changes, epoch=0):
        return self.pub.publish_group(changes, expected_epoch=epoch)

    def test_name_and_batch_shape(self):
        good = {"a": (self.r1, {})}
        self.assertEqual(self.publish(good), 1)
        with self.assertRaises(ValueError):
            self.publish({})  # at least one change
        with self.assertRaises(ValueError):
            self.publish({("a",): (self.r1, {})})  # non-string name
        with self.assertRaises(ValueError):
            self.publish({"a b": (self.r1, {})})  # illegal character
        with self.assertRaises(ValueError):
            self.publish({"": (self.r1, {})})
        with self.assertRaises(ValueError):
            self.publish({"x" * 65: (self.r1, {})})
        with self.assertRaises(ValueError):
            self.publish([("a", (self.r1, {}))])  # not a dict
        with self.assertRaises(ValueError):
            self.publish({("k%d" % i): (self.r1, {}) for i in range(33)})
        # Case sensitivity: A and a are different components.
        self.assertEqual(self.publish({"A": (self.r1, {})}, epoch=1), 2)
        self.assertEqual(
            set(self.pub.poll_group(("A", "a"), known_epoch=0)["put"]), {"A", "a"})

    def test_binding_and_needs_shapes(self):
        with self.assertRaises(ValueError):
            self.publish({"a": [self.r1, {}]})  # list, not tuple
        with self.assertRaises(ValueError):
            self.publish({"a": (self.r1, {}, 1)})  # wrong arity
        with self.assertRaises(ValueError):
            self.publish({"a": (True, {})})  # bool is not a revision
        with self.assertRaises(ValueError):
            self.publish({"a": (0, {})})
        with self.assertRaises(ValueError):
            self.publish({"a": (1.0, {})})
        with self.assertRaises(ValueError):
            self.publish({"a": (self.r1, {"b": True})})  # bool dependency version
        with self.assertRaises(ValueError):
            self.publish({"a": (self.r1, {"b": "1"})})
        with self.assertRaises(ValueError):
            self.publish({"a": (self.r1, {"bad name": self.r1})})
        with self.assertRaises(ValueError):
            self.publish({"a": (self.r1, {("d%d" % i): self.r1 for i in range(9)})})
        with self.assertRaises(KeyError):
            self.publish({"a": (999999, {})})  # unstaged direct binding

    def test_epoch_arguments(self):
        self.publish({"a": (self.r1, {})})
        with self.assertRaises(ValueError):
            self.pub.publish_group({"a": (self.r2, {})}, expected_epoch=True)
        with self.assertRaises(ConflictingPromotion):
            self.pub.publish_group({"a": (self.r2, {})}, expected_epoch=0)
        with self.assertRaises(ConflictingPromotion):
            self.pub.publish_group({"a": (self.r2, {})}, expected_epoch=9)

    def test_dependency_integrity_of_the_whole_batch(self):
        # Dangling / wrong-version dependencies, including for components the
        # batch does not touch.
        with self.assertRaises(ValueError):
            self.publish({"a": (self.r1, {"b": self.r1})})  # b absent
        self.publish({"a": (self.r1, {}), "b": (self.r1, {})})
        with self.assertRaises(ValueError):
            # b stays on r1, so the exact r2 requirement cannot resolve.
            self.publish({"a": (self.r1, {"b": self.r2})}, epoch=1)
        # Make a depend on b, then deleting b strands the untouched binding.
        self.assertEqual(
            self.publish({"a": (self.r1, {"b": self.r1})}, epoch=1), 2)
        with self.assertRaises(ValueError):
            self.publish({"b": None}, epoch=2)
        # Deleting a non-existent component is itself a valid change.
        self.assertEqual(self.publish({"ghost": None}, epoch=2), 3)
        # Rewiring a and removing b together is internally consistent.
        self.assertEqual(
            self.publish({"a": (self.r2, {}), "b": None}, epoch=3), 4)
        # A no-op re-submission still advances exactly one epoch.
        self.assertEqual(self.publish({"a": (self.r2, {})}, epoch=4), 5)

    def test_rejections_change_nothing(self):
        self.publish({"a": (self.r1, {"b": self.r1}), "b": (self.r1, {})})
        snapshot = self.pub.poll_group(("a",), known_epoch=0)
        for bad in (
            {"b": (self.r2, {})},                 # strands a
            {"a": (self.r1, {"b": self.r2})},     # wrong exact version
            {"a": (self.r1, {"a": self.r1})},     # self loop
        ):
            with self.assertRaises(ValueError):
                self.pub.publish_group(bad, expected_epoch=1)
        # Epoch and content are unchanged: a subscriber on epoch 0 still
        # catches up to the same epoch 1 with the same payload.
        again = self.pub.poll_group(("a",), known_epoch=0)
        self.assertEqual(again, snapshot)
        self.assertEqual(again["epoch"], 1)

    def test_group_size_limit(self):
        revs = stage_many(self.pub, _GROUP_FULL_SIZE + 1)
        batch = {}
        for i in range(_GROUP_FULL_SIZE):
            batch["c%04d" % i] = (revs[i], {})
            if len(batch) == 32:
                self.pub.publish_group(batch, expected_epoch=self.pub._current_epoch())
                batch = {}
        if batch:
            self.pub.publish_group(batch, expected_epoch=self.pub._current_epoch())
        # One more component pushes the whole group past the limit.
        with self.assertRaises(ValueError):
            self.pub.publish_group(
                {"overflow": (revs[_GROUP_FULL_SIZE], {})},
                expected_epoch=self.pub._current_epoch())

    def test_poll_argument_validation(self):
        with self.assertRaises(ValueError):
            self.pub.poll_group(["a"])                      # list, not tuple
        with self.assertRaises(ValueError):
            self.pub.poll_group(())                         # empty
        with self.assertRaises(ValueError):
            self.pub.poll_group(tuple("r%d" % i for i in range(17)))
        with self.assertRaises(ValueError):
            self.pub.poll_group(("a", "a"))                 # duplicate
        with self.assertRaises(ValueError):
            self.pub.poll_group(("bad name",))
        with self.assertRaises(ValueError):
            self.pub.poll_group(("a",), known_epoch=-1)
        with self.assertRaises(ValueError):
            self.pub.poll_group(("a",), known_epoch=True)
        with self.assertRaises(ValueError):
            self.pub.poll_group(("a",), known_epoch=1.0)
        self.publish({"a": (self.r1, {})})
        with self.assertRaises(ValueError):
            self.pub.poll_group(("a",), known_epoch=2)      # future epoch


_GROUP_FULL_SIZE = 2048


class GroupLifecycleTest(unittest.TestCase):
    def build_three_components(self):
        pub = Distribution(self.path)
        s1 = pub.stage({"upstream": "shared-1", "timeout_ms": 10})
        s2 = pub.stage({"upstream": "shared-2", "timeout_ms": 20})
        se = pub.stage({"upstream": "extra", "timeout_ms": 5})
        epoch = pub.publish_group({
            "route": (s1, {"shared": s1}),
            "fallback": (s1, {"shared": s1}),
            "shared": (s1, {})}, expected_epoch=0)
        self.assertEqual(epoch, 1)
        return pub, s1, s2, se

    def test_batch_migration_cycle_and_competing_publishers(self):
        with TemporaryDirectory() as tmp:
            self.path = Path(tmp, "state.db")
            pub, s1, s2, _ = self.build_three_components()

            # Moving shared alone to s2 breaks the exact deps still held by
            # route and fallback; the batch must be rejected wholesale.
            with self.assertRaises(ValueError):
                pub.publish_group({"shared": (s2, {})}, expected_epoch=1)
            # Catch-up from the old epoch still works after the rejection.
            reply = pub.poll_group(("route",), known_epoch=0)
            self.assertEqual(reply["epoch"], 1)
            self.assertEqual(reply["put"]["shared"][0], s1)

            # Migrating all three in one batch is atomic and valid.
            self.assertEqual(pub.publish_group({
                "route": (s2, {"shared": s2}),
                "fallback": (s2, {"shared": s2}),
                "shared": (s2, {})}, expected_epoch=1), 2)

            # A ring whose versions all line up inside one submission is still
            # rejected; nothing is applied.
            with self.assertRaises(ValueError):
                pub.publish_group({
                    "route": (s2, {"fallback": s2}),
                    "fallback": (s2, {"route": s2})}, expected_epoch=2)
            reply = pub.poll_group(("route", "fallback"), known_epoch=1)
            self.assertEqual(reply["epoch"], 2)
            self.assertEqual(set(reply["put"]), {"route", "fallback", "shared"})
            self.assertEqual(reply["remove"], ())

            # Two connections racing on the same old epoch: exactly one wins.
            other = Distribution(self.path)
            barrier = threading.Barrier(2)
            outcomes = []

            def publish_winner(connection, binding):
                barrier.wait()
                try:
                    outcomes.append(connection.publish_group(
                        binding, expected_epoch=2))
                except ConflictingPromotion:
                    outcomes.append(None)

            t1 = threading.Thread(target=publish_winner,
                                  args=(pub, {"shared": (s2, {})}))
            t2 = threading.Thread(target=publish_winner,
                                  args=(other, {"route": (s2, {"shared": s2})}))
            t1.start()
            t2.start()
            t1.join()
            t2.join()
            self.assertEqual(sorted(outcomes, key=lambda v: v is None), [3, None])
            self.assertEqual(pub._current_epoch(), 3)
            # The loser retries against the new epoch and occupies epoch 4.
            self.assertEqual(
                other.publish_group({"route": (s2, {"shared": s2})},
                                    expected_epoch=3), 4)

    def test_scope_delta_remove_and_reconnect(self):
        with TemporaryDirectory() as tmp:
            self.path = Path(tmp, "state.db")
            pub, s1, _, se = self.build_three_components()
            route_only = Distribution(self.path)
            fallback_sub = Distribution(self.path)

            # First pulls are full replacements of the managed set.
            first = route_only.poll_group(("route",), known_epoch=None)
            self.assertEqual(first["mode"], "full")
            self.assertEqual(first["epoch"], 1)
            self.assertEqual(first["remove"], ())
            self.assertEqual(set(first["put"]), {"route", "shared"})
            fallback_first = fallback_sub.poll_group(("fallback",), known_epoch=None)
            self.assertEqual(set(fallback_first["put"]), {"fallback", "shared"})

            # Keep route's bound revision; drop its shared dependency in favor
            # of the newly published extra. shared itself stays in the group.
            self.assertEqual(pub.publish_group({
                "extra": (se, {}),
                "route": (s1, {"extra": se})}, expected_epoch=1), 2)

            delta = route_only.poll_group(("route",), known_epoch=1)
            self.assertEqual(delta["mode"], "delta")
            self.assertEqual(delta["epoch"], 2)
            self.assertEqual(set(delta["put"]), {"extra"})  # route rev unchanged
            self.assertEqual(delta["put"]["extra"],
                             (se, {"upstream": "extra", "timeout_ms": 5}))
            self.assertEqual(delta["remove"], ("shared",))

            # A subscriber whose scope still needs shared must not remove it;
            # an empty delta still carries the new epoch.
            fdelta = fallback_sub.poll_group(("fallback",), known_epoch=1)
            self.assertEqual(fdelta["mode"], "delta")
            self.assertEqual(fdelta["epoch"], 2)
            self.assertEqual(fdelta["put"], {})
            self.assertEqual(fdelta["remove"], ())

            # Reopen the database, skip an epoch, and pull: the delta is still
            # derived from the real old and current scopes via stored history.
            reopened = Distribution(self.path)
            redelta = reopened.poll_group(("route",), known_epoch=1)
            self.assertEqual(redelta, delta)
            full = reopened.poll_group(("route",), known_epoch=None)
            self.assertEqual(full["mode"], "full")
            self.assertEqual(set(full["put"]), {"route", "extra"})
            self.assertEqual(full["remove"], ())

            # Root changes are the caller's responsibility: a None re-pull is a
            # full replacement under whatever roots it now subscribes to.
            switched = reopened.poll_group(("fallback",), known_epoch=None)
            self.assertEqual(set(switched["put"]), {"fallback", "shared"})

    def test_no_mixed_epoch_replies_while_publishing(self):
        with TemporaryDirectory() as tmp:
            self.path = Path(tmp, "state.db")
            pub = Distribution(self.path)
            revisions = stage_many(pub, 40)
            pub.publish_group({"a": (revisions[0], {}),
                               "b": (revisions[0], {})}, expected_epoch=0)
            stop = threading.Event()
            errors = []

            def publisher():
                for i in range(1, 30):
                    pub.publish_group(
                        {"a": (revisions[i], {}), "b": (revisions[i], {})},
                        expected_epoch=i)

            def reader():
                conn = sqlite3.connect(self.path)
                try:
                    while not stop.is_set():
                        reply = Distribution(self.path).poll_group(
                            ("a", "b"), known_epoch=None)
                        epoch = reply["epoch"]
                        # Independently reconstruct each binding at the reply's
                        # epoch straight from the event log: every delivered
                        # revision must match that single point in time.
                        for name, (revision, _config) in reply["put"].items():
                            row = conn.execute(
                                "SELECT revision FROM group_event WHERE name=? "
                                "AND epoch <= ? ORDER BY epoch DESC LIMIT 1",
                                (name, epoch)).fetchone()
                            if row is None or row[0] != revision:
                                errors.append((epoch, name, revision, row))
                finally:
                    conn.close()

            threads = [threading.Thread(target=reader) for _ in range(4)]
            for thread in threads:
                thread.start()
            publisher()
            stop.set()
            for thread in threads:
                thread.join()
            self.assertEqual(errors, [])
            self.assertIsNone(pub.poll_group(("a", "b"), known_epoch=30))

    def test_old_channel_and_group_are_independent(self):
        with TemporaryDirectory() as tmp:
            self.path = Path(tmp, "state.db")
            pub = Distribution(self.path)
            rev = pub.stage({"upstream": "chan", "timeout_ms": 3})
            g_rev = pub.stage({"upstream": "group", "timeout_ms": 4})
            # The same staged revision may serve both paths.
            self.assertEqual(pub.promote(rev, expected_generation=0), 1)
            self.assertEqual(pub.publish_group({"c": (g_rev, {})},
                                               expected_epoch=0), 1)
            self.assertEqual(pub.promote(rev, expected_generation=1), 2)
            self.assertEqual(pub.publish_group({"c": (g_rev, {})},
                                               expected_epoch=1), 2)
            self.assertEqual(pub.poll()[0], 2)            # channel generation
            self.assertEqual(pub.poll_group(("c",), known_epoch=1)["epoch"], 2)
            self.assertIsNone(pub.poll_group(("c",), known_epoch=2))


class GroupWindowTest(unittest.TestCase):
    def test_exact_window_boundary_after_reopen(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "state.db")
            pub = Distribution(path)
            r1, r2 = stage_many(pub, 2)
            for i in range(257):
                rev = r1 if i % 2 == 0 else r2
                pub.publish_group({"c": (rev, {})}, expected_epoch=i)
            # Final epoch is 257; retained window covers epochs 2..257.
            reopened = Distribution(path)
            delta = reopened.poll_group(("c",), known_epoch=2)
            self.assertEqual(delta["mode"], "delta")
            self.assertEqual(delta["epoch"], 257)
            # 255 toggles between epochs 2 and 257: odd count, revision moved.
            self.assertEqual(set(delta["put"]), {"c"})
            self.assertEqual(delta["remove"], ())
            # Epoch 1 fell out of the window: a full replacement is required.
            full = reopened.poll_group(("c",), known_epoch=1)
            self.assertEqual(full["mode"], "full")
            self.assertEqual(full["epoch"], 257)
            self.assertEqual(set(full["put"]), {"c"})
            full0 = reopened.poll_group(("c",), known_epoch=0)
            self.assertEqual(full0["mode"], "full")
            # Events below the window have been pruned, baseline moved forward.
            with sqlite3.connect(path) as db:
                base_epoch = db.execute(
                    "SELECT base_epoch FROM group_meta").fetchone()[0]
                oldest = db.execute("SELECT MIN(epoch) FROM group_event").fetchone()[0]
            self.assertEqual(base_epoch, 2)
            self.assertEqual(oldest, 3)


class GroupScaleTest(unittest.TestCase):
    def test_2048_components_270_toggles_budget_and_window(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "state.db")
            pub = Distribution(path)
            pairs = 1024
            # Every staged version exists before any group is built.
            leaf_revs = stage_many(pub, pairs)
            root_a = stage_many(pub, pairs)
            root_b = stage_many(pub, pairs)

            def directory_size(directory):
                total = 0
                for root, _dirs, files in os.walk(directory):
                    for name in files:
                        total += os.path.getsize(os.path.join(root, name))
                return total

            size_before = directory_size(tmp)
            started = time.perf_counter()

            # 1024 disjoint root/leaf pairs, each root pinned to its exact leaf.
            batch = {}
            epoch = 0
            for i in range(pairs):
                root = "root%04d_abcdefghijklmnopqrstuvwxyz0123456789abcd" % i
                leaf = "leaf%04d_abcdefghijklmnopqrstuvwxyz0123456789abcd" % i
                self.assertGreaterEqual(len(root), 48)
                batch[root] = (root_a[i], {leaf: leaf_revs[i]})
                batch[leaf] = (leaf_revs[i], {})
                if len(batch) == 32:
                    epoch = pub.publish_group(batch, expected_epoch=epoch)
                    batch = {}
            if batch:
                epoch = pub.publish_group(batch, expected_epoch=epoch)
            # 2048 bindings in batches of 32 -> 64 group epochs.
            build_epochs = (pairs * 2) // 32
            self.assertEqual(epoch, build_epochs)

            toggled_root = "root0000_abcdefghijklmnopqrstuvwxyz0123456789abcd"
            toggled_leaf = "leaf0000_abcdefghijklmnopqrstuvwxyz0123456789abcd"
            for i in range(270):
                rev = root_b[0] if i % 2 == 0 else root_a[0]
                epoch = pub.publish_group(
                    {toggled_root: (rev, {toggled_leaf: leaf_revs[0]})},
                    expected_epoch=epoch)
            final_epoch = epoch
            self.assertEqual(final_epoch, build_epochs + 270)
            elapsed = time.perf_counter() - started

            # Close everything (connections are per-call; drop objects too)
            # and measure on-disk growth in this single directory.
            del pub
            size_after = directory_size(tmp)
            growth = size_after - size_before
            self.assertLessEqual(growth, 6 * 1024 * 1024,
                                 "group history grew %d bytes" % growth)
            self.assertLess(elapsed, 35.0, "scenario took %.2fs" % elapsed)

            subscriber = Distribution(path)
            # E-255 is still inside the retained window: an exact delta.
            near = subscriber.poll_group(
                (toggled_root,), known_epoch=final_epoch - 255)
            self.assertEqual(near["mode"], "delta")
            self.assertEqual(near["epoch"], final_epoch)
            self.assertEqual(near["remove"], ())
            # 255 toggles separate the two epochs, an odd number, so the root
            # revision differs and exactly one component is delivered.
            self.assertEqual(set(near["put"]), {toggled_root})
            expected_rev = root_b[0] if 270 % 2 == 1 else root_a[0]
            self.assertEqual(near["put"][toggled_root][0], expected_rev)

            # E-256 is one epoch older than the window: full replacement only.
            far = subscriber.poll_group(
                (toggled_root,), known_epoch=final_epoch - 256)
            self.assertEqual(far["mode"], "full")
            self.assertEqual(far["epoch"], final_epoch)
            self.assertEqual(far["remove"], ())
            self.assertEqual(set(far["put"]), {toggled_root, toggled_leaf})
            self.assertEqual(far["put"][toggled_root][0], expected_rev)

            # A fresh subscriber gets the current complete scope.
            fresh = subscriber.poll_group((toggled_root,), known_epoch=None)
            self.assertEqual(fresh["mode"], "full")
            self.assertEqual(set(fresh["put"]), {toggled_root, toggled_leaf})
            self.assertIsNone(
                subscriber.poll_group((toggled_root,), known_epoch=final_epoch))


if __name__ == "__main__":
    unittest.main()
