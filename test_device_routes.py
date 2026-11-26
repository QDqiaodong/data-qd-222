"""Acceptance tests for declarative device routing over the group path."""
from pathlib import Path
from tempfile import TemporaryDirectory
import json
import os
import sqlite3
import subprocess
import sys
import threading
import unittest

from distribution import ConflictingPromotion, Distribution


def config(tag):
    return {"upstream": tag, "timeout_ms": 1}


def route_args(axes, rules):
    """Rules as the public API wants them: tuples copied from a list."""
    return tuple(rules)


class DeviceRouteTests(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.path = Path(self._tmp.name, "routes.db")

    def equal_put(self, actual, expected):
        self.assertEqual(set(actual), set(expected))
        for name, (revision, body) in expected.items():
            self.assertEqual(actual[name][0], revision)
            self.assertEqual(actual[name][1], body)

    def tearDown(self):
        self._tmp.cleanup()

    # ------------------------------------------------------------------
    # Serving, re-binding and operand of the in-use constraint.
    # ------------------------------------------------------------------

    def test_serving_and_pruning(self):
        pub = Distribution(self.path)
        # No table installed: devices are not routed yet.
        self.assertIsNone(pub.resolve_device_config(
            {"board": "alpha", "media": "fiber"}))
        main_v1 = pub.stage(config("main-v1"))
        main_dep_v1 = pub.stage(config("main-dep-v1"))
        backup_v1 = pub.stage(config("backup-v1"))
        backup_dep_v1 = pub.stage(config("backup-dep-v1"))
        cold_v1 = pub.stage(config("cold-v1"))
        cold_dep_v1 = pub.stage(config("cold-dep-v1"))
        shared_v1 = pub.stage(config("shared-v1"))

        def publish(changes, epoch):
            return pub.publish_group(changes, expected_epoch=epoch)

        epoch = publish({
            "main": (main_v1, {"main_dep": main_dep_v1}),
            "main_dep": (main_dep_v1, {}),
            "backup": (backup_v1, {"backup_dep": backup_dep_v1}),
            "backup_dep": (backup_dep_v1, {}),
            "shared": (shared_v1, {}),
        }, 0)
        self.assertEqual(epoch, 1)

        axes = {"board": ("alpha", "beta"), "media": ("fiber", "copper")}
        # Alpha -> main. Beta -> backup. cold matches beta only, at a lower
        # priority, and is deliberately not published yet. The two priority-1
        # rules disagree but are wholly shadowed by backup, so they are legal;
        # the duplicate and overlapping top-priority rules name the same
        # target and must not be reported.
        rules = [
            (10, {"board": ("alpha",)}, "main"),
            (10, {"board": ("alpha",)}, "main"),                     # duplicate
            (10, {"board": ("alpha",), "media": ("fiber",)}, "main"),  # overlap, same target
            (5, {"board": ("beta",)}, "backup"),
            (1, {"board": ("beta",), "media": ("fiber",)}, "cold"),
            (1, {"board": ("beta",), "media": ("copper",)}, "phantomA"),
            (1, {"board": ("beta",)}, "phantomB"),                   # shadowed tie
        ]
        self.assertEqual(
            pub.install_device_routes(axes, route_args(axes, rules),
                                      expected_epoch=1),
            ("backup", "main"))
        # A successful install advances no group epoch and adds no history.
        self.assertEqual(pub._current_epoch(), 1)
        self.assertIsNone(pub.poll_group(("main",), known_epoch=1))

        alpha = pub.resolve_device_config({"board": "alpha", "media": "copper"})
        beta = pub.resolve_device_config({"board": "beta", "media": "copper"})
        self.assertEqual(alpha["root"], "main")
        self.assertEqual(alpha["epoch"], 1)
        self.assertEqual(
            set(alpha["put"]), {"main", "main_dep"})
        self.assertEqual(alpha["put"]["main"][0], main_v1)
        self.assertEqual(alpha["put"]["main"][1], config("main-v1"))
        self.assertEqual(beta["root"], "backup")
        self.assertEqual(set(beta["put"]), {"backup", "backup_dep"})
        self.assertEqual(beta["put"]["backup"][1], config("backup-v1"))
        # The delivered put is exactly what a first poll_group of that single
        # root yields at the same epoch (full transitive scope, nothing else).
        direct = pub.poll_group(("backup",), known_epoch=None)
        self.equal_put(beta["put"], direct["put"])
        self.assertEqual(beta["epoch"], direct["epoch"])
        # No cross-contamination between the two entry points.
        self.assertNotIn("backup", alpha["put"])
        self.assertNotIn("main_dep", beta["put"])
        # Authoring order carries no meaning: reversed axes values and rule
        # order install identically and deliver the same bytes.
        reordered_axes = {"media": ("copper", "fiber"),
                          "board": ("beta", "alpha")}
        self.assertEqual(
            Distribution(self.path).install_device_routes(
                reordered_axes, tuple(reversed(rules)), expected_epoch=1),
            ("backup", "main"))
        self.equal_put(
            Distribution(self.path).resolve_device_config(
                {"board": "beta", "media": "copper"})["put"], beta["put"])
        # cold is mentioned but cannot win anywhere.
        self.assertEqual(pub.resolve_device_config(
            {"board": "beta", "media": "fiber"})["root"], "backup")
        # Traits must cover exactly the declared attributes with legal values.
        for bad in (
                {"board": "alpha"},
                {"board": "alpha", "media": "fiber", "rack": "r1"},
                {"board": "gamma", "media": "fiber"},
                {"board": "alpha", "media": "fiber".upper()},
                {"board": True, "media": "fiber"},
                ["alpha", "fiber"]):
            with self.assertRaises(ValueError):
                pub.resolve_device_config(bad)

        # Re-bind the active main entry to new content with a new dependency;
        # a reopened library must deliver the new body and the new scope.
        main_v2 = pub.stage(config("main-v2"))
        main_dep_v2 = pub.stage(config("main-dep-v2"))
        epoch = publish({
            "main": (main_v2, {"main_dep": main_dep_v2}),
            "main_dep": (main_dep_v2, {}),
        }, 1)
        self.assertEqual(epoch, 2)
        reopened = Distribution(self.path)
        alpha2 = reopened.resolve_device_config(
            {"board": "alpha", "media": "fiber"})
        self.assertEqual(alpha2["root"], "main")
        self.assertEqual(alpha2["epoch"], 2)
        self.assertEqual(alpha2["put"]["main"][0], main_v2)
        self.assertEqual(alpha2["put"]["main"][1], config("main-v2"))
        self.assertEqual(set(alpha2["put"]), {"main", "main_dep"})
        self.assertEqual(alpha2["put"]["main_dep"][1], config("main-dep-v2"))
        # The rebound entry still delivers the whole new transitive scope,
        # again identical to a first poll_group of that root.
        self.equal_put(alpha2["put"],
                       Distribution(self.path).poll_group(
                           ("main",), known_epoch=None)["put"])

        # backup still wins for beta, so deleting it must be refused; the old
        # table keeps serving it and the epoch does not move.
        with self.assertRaises(ValueError) as caught:
            publish({"backup": None}, 2)
        self.assertEqual(caught.exception.args, ("in_use_root", "backup"))
        self.assertEqual(Distribution(self.path).resolve_device_config(
            {"board": "beta", "media": "copper"})["root"], "backup")
        self.assertEqual(pub._current_epoch(), 2)

        # Raise cold above backup while cold is still absent: the new table
        # must be rejected as missing its winning root, not installed broken,
        # and the previously installed table still wins.
        raised = [
            (10, {"board": ("alpha",)}, "main"),
            (10, {"board": ("alpha",)}, "main"),
            (10, {"board": ("alpha",), "media": ("fiber",)}, "main"),
            (5, {"board": ("beta",)}, "backup"),
            (20, {"board": ("beta",)}, "cold"),
            (1, {"board": ("beta",), "media": ("copper",)}, "phantomA"),
            (1, {"board": ("beta",)}, "phantomB"),
        ]
        with self.assertRaises(ValueError) as caught:
            pub.install_device_routes(axes, route_args(axes, raised),
                                      expected_epoch=2)
        self.assertEqual(caught.exception.args, ("missing_root", "cold"))
        # Two reachable top-priority rules with different targets are rejected
        # as ambiguous, with the lexicographically smallest witness.
        with self.assertRaises(ValueError) as caught:
            pub.install_device_routes(
                axes,
                ((10, {}, "backup"),
                 (10, {"board": ("alpha",)}, "main")),
                expected_epoch=2)
        self.assertEqual(
            caught.exception.args, ("ambiguous", ("alpha", "copper")))
        self.assertEqual(pub._current_epoch(), 2)  # failed install leaves epoch
        still = Distribution(self.path).resolve_device_config(
            {"board": "beta", "media": "fiber"})
        self.assertEqual(still["root"], "backup")
        self.assertEqual(still["put"]["backup"][1], config("backup-v1"))
        # An install against a stale epoch is a conflict and installs nothing.
        with self.assertRaises(ConflictingPromotion):
            pub.install_device_routes(axes, route_args(axes, rules),
                                      expected_epoch=1)

        # Publish cold, reinstall, and beta now follows cold instead.
        epoch = publish({
            "cold": (cold_v1, {"cold_dep": cold_dep_v1}),
            "cold_dep": (cold_dep_v1, {}),
        }, 2)
        self.assertEqual(epoch, 3)
        self.assertEqual(
            pub.install_device_routes(axes, route_args(axes, raised),
                                      expected_epoch=3),
            ("cold", "main"))
        beta_cold = pub.resolve_device_config(
            {"board": "beta", "media": "copper"})
        self.assertEqual(beta_cold["root"], "cold")
        self.assertEqual(beta_cold["epoch"], 3)
        self.assertEqual(set(beta_cold["put"]), {"cold", "cold_dep"})
        self.assertEqual(beta_cold["put"]["cold"][1], config("cold-v1"))

        # backup is still named by the installed table but can never win, so
        # pruning it is allowed and does not disturb delivery.
        self.assertEqual(publish({"backup": None}, 3), 4)
        reopened = Distribution(self.path)
        self.assertEqual(reopened.resolve_device_config(
            {"board": "beta", "media": "fiber"})["root"], "cold")
        self.assertEqual(reopened.resolve_device_config(
            {"board": "alpha", "media": "copper"})["root"], "main")

        # A recovery copy taken with routing installed keeps the copy's
        # time-point table and group.
        copy = Path(self._tmp.name, "copy.db")
        pub.fork_recovery(copy)
        copied = Distribution(copy).resolve_device_config(
            {"board": "beta", "media": "copper"})
        self.assertEqual(copied["root"], "cold")
        self.assertEqual(copied["epoch"], 4)

    # ------------------------------------------------------------------
    # Install and prune racing from one group epoch: never an empty table.
    # ------------------------------------------------------------------

    def test_install_remove_race(self):
        seed = Distribution(self.path)
        rev_a = seed.stage(config("A"))
        rev_cold = seed.stage(config("cold"))
        rev_priv = seed.stage(config("cold-private"))
        self.assertEqual(seed.publish_group({
            "A": (rev_a, {}),
            "cold": (rev_cold, {"priv": rev_priv}),
            "priv": (rev_priv, {}),
        }, expected_epoch=0), 1)

        axes = {"board": ("alpha", "beta"), "media": ("fiber", "copper")}
        # cold is named but shadowed everywhere, so it is not yet selectable.
        shadowed = [
            (10, {"board": ("alpha",)}, "A"),
            (10, {"board": ("beta",)}, "A"),
            (1, {"board": ("beta",)}, "cold"),
        ]
        seed.install_device_routes(axes, route_args(axes, shadowed),
                                   expected_epoch=1)
        self.assertEqual(
            seed.resolve_device_config({"board": "beta", "media": "copper"})["root"],
            "A")

        # The installer raises cold to the top for beta; the pruner removes
        # cold, which the stored table cannot reach yet.
        selecting = [
            (10, {"board": ("alpha",)}, "A"),
            (10, {"board": ("beta",)}, "cold"),
            (1, {"board": ("beta",)}, "cold"),
        ]

        install_conn = Distribution(self.path)
        remove_conn = Distribution(self.path)
        barrier = threading.Barrier(2)
        outcome = {}

        def install():
            barrier.wait()
            try:
                outcome["install"] = install_conn.install_device_routes(
                    axes, route_args(axes, selecting), expected_epoch=1)
            except ConflictingPromotion as exc:
                outcome["install"] = ("conflict", exc.args)
            except ValueError as exc:
                outcome["install"] = ("value", exc.args)

        def remove():
            barrier.wait()
            try:
                outcome["remove"] = remove_conn.publish_group(
                    {"cold": None}, expected_epoch=1)
            except ConflictingPromotion as exc:
                outcome["remove"] = ("conflict", exc.args)
            except ValueError as exc:
                outcome["remove"] = ("value", exc.args)

        threads = [threading.Thread(target=install),
                   threading.Thread(target=remove)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # Exactly one operation commits: the epoch or the routing changed, but
        # it is impossible for both an install and a prune to have succeeded.
        install_result = outcome["install"]
        remove_result = outcome["remove"]
        if remove_result == 2:
            # The prune committed first; the install then saw a stale epoch.
            self.assertEqual(install_result[0], "conflict")
            self.assertEqual(install_result[1], (1,))
            final = Distribution(self.path)
            self.assertEqual(final._current_epoch(), 2)
            self.assertEqual(
                final.resolve_device_config({"board": "beta", "media": "copper"})["root"],
                "A")
            self.assertEqual(
                set(final.resolve_device_config(
                    {"board": "beta", "media": "copper"})["put"]),
                {"A"})
            with sqlite3.connect(self.path) as db:
                roots = json.loads(db.execute(
                    "SELECT roots FROM device_route").fetchone()[0])
            self.assertEqual(roots, ["A"])
        else:
            # The install committed first; the prune was refused because cold
            # is now actually selectable.
            self.assertEqual(remove_result[0], "value")
            self.assertEqual(remove_result[1], ("in_use_root", "cold"))
            self.assertEqual(install_result, ("A", "cold"))
            final = Distribution(self.path)
            self.assertEqual(final._current_epoch(), 1)  # install never advances it
            served = final.resolve_device_config(
                {"board": "beta", "media": "copper"})
            self.assertEqual(served["root"], "cold")
            self.assertEqual(served["epoch"], 1)
            self.assertEqual(set(served["put"]), {"cold", "priv"})
            self.assertEqual(served["put"]["cold"][1], config("cold"))
            self.assertEqual(served["put"]["priv"][1], config("cold-private"))
            self.assertEqual(final.resolve_device_config(
                {"board": "alpha", "media": "fiber"})["root"], "A")
            with sqlite3.connect(self.path) as db:
                rows = db.execute(
                    "SELECT name FROM group_binding ORDER BY name").fetchall()
            self.assertEqual([row[0] for row in rows], ["A", "cold", "priv"])

        # Whatever the thread order produced, the outcome must also be reach-
        # able by these two orderings, each from the same seed state, proving
        # neither simply cannot happen (no sleeps involved).
        self.check_prune_first()
        self.check_install_first()

    def _seed(self):
        path = Path(self._tmp.name, "ordered.db")
        if os.path.exists(path):
            os.unlink(path)
        seed = Distribution(path)
        rev_a = seed.stage(config("A"))
        rev_cold = seed.stage(config("cold"))
        rev_priv = seed.stage(config("cold-private"))
        seed.publish_group({
            "A": (rev_a, {}),
            "cold": (rev_cold, {"priv": rev_priv}),
            "priv": (rev_priv, {}),
        }, expected_epoch=0)
        axes = {"board": ("alpha", "beta"), "media": ("fiber", "copper")}
        shadowed = [
            (10, {"board": ("alpha",)}, "A"),
            (10, {"board": ("beta",)}, "A"),
            (1, {"board": ("beta",)}, "cold"),
        ]
        seed.install_device_routes(axes, tuple(shadowed), expected_epoch=1)
        selecting = [
            (10, {"board": ("alpha",)}, "A"),
            (10, {"board": ("beta",)}, "cold"),
            (1, {"board": ("beta",)}, "cold"),
        ]
        return path, axes, selecting

    def check_prune_first(self):
        path, axes, selecting = self._seed()
        self.assertEqual(Distribution(path).publish_group(
            {"cold": None}, expected_epoch=1), 2)
        with self.assertRaises(ConflictingPromotion) as caught:
            Distribution(path).install_device_routes(
                axes, tuple(selecting), expected_epoch=1)
        self.assertEqual(caught.exception.args, (1,))
        final = Distribution(path)
        self.assertEqual(final._current_epoch(), 2)
        beta = final.resolve_device_config({"board": "beta", "media": "copper"})
        self.assertEqual(beta["root"], "A")
        self.assertEqual(set(beta["put"]), {"A"})
        self.assertEqual(beta["epoch"], 2)

    def check_install_first(self):
        path, axes, selecting = self._seed()
        self.assertEqual(
            Distribution(path).install_device_routes(
                axes, tuple(selecting), expected_epoch=1),
            ("A", "cold"))
        with self.assertRaises(ValueError) as caught:
            Distribution(path).publish_group({"cold": None}, expected_epoch=1)
        self.assertEqual(caught.exception.args, ("in_use_root", "cold"))
        final = Distribution(path)
        self.assertEqual(final._current_epoch(), 1)
        beta = final.resolve_device_config({"board": "beta", "media": "copper"})
        self.assertEqual(beta["root"], "cold")
        self.assertEqual(beta["epoch"], 1)
        self.assertEqual(set(beta["put"]), {"cold", "priv"})
        self.assertEqual(beta["put"]["cold"][1], config("cold"))
        self.assertEqual(beta["put"]["priv"][1], config("cold-private"))

    # ------------------------------------------------------------------
    # Scale: a huge Cartesian domain is decided symbolically, never expanded.
    # ------------------------------------------------------------------

    def test_symbolic_scale(self):
        script = r'''
import resource, sys, time
limit = 256 * 1024 * 1024
resource.setrlimit(resource.RLIMIT_AS, (limit, limit))
project_dir = sys.argv[1]
sys.path.insert(0, project_dir)
from distribution import Distribution
from pathlib import Path

def config(tag):
    return {"upstream": tag, "timeout_ms": 1}

values = tuple("%02d" % i for i in range(16))
axes = {"d%02d" % i: values for i in range(12)}

db_path = Path(sys.argv[2])
pub = Distribution(db_path)
rev_a = pub.stage(config("A"))
rev_b = pub.stage(config("B"))
pub.publish_group({"A": (rev_a, {}), "B": (rev_b, {})}, expected_epoch=0)

base = [(10, {"d00": (value,)}, "A") for value in values]
for i in range(47):
    other = 1 + i % 11
    base.append((1, {"d00": ("%02d" % (i % 15),),
                     "d%02d" % other: ("%02d" % (i % 16),)},
                 "ghost%02d" % i))

largest = tuple("15" for _ in axes)
uncovered_witness = ("15",) + tuple("00" for _ in range(len(axes) - 1))
elapsed = 0.0

started = time.perf_counter()
winners = pub.install_device_routes(axes, tuple(base), expected_epoch=1)
assert winners == ("A",), winners
served = pub.resolve_device_config(dict(zip(axes, largest)))
assert served["root"] == "A", served["root"]
assert served["epoch"] == 1
assert set(served["put"]) == {"A"}, served["put"]
elapsed += time.perf_counter() - started

without_15 = [rule for rule in base
              if not (rule[0] == 10 and rule[1] == {"d00": ("15",)})]
started = time.perf_counter()
try:
    pub.install_device_routes(axes, tuple(without_15), expected_epoch=1)
except ValueError as exc:
    assert exc.args == ("uncovered", uncovered_witness), exc.args
else:
    raise AssertionError("uncovered table was installed")
elapsed += time.perf_counter() - started

restored = list(base) + [(10, {axis: ("15",) for axis in axes}, "B")]
started = time.perf_counter()
try:
    pub.install_device_routes(axes, tuple(restored), expected_epoch=1)
except ValueError as exc:
    assert exc.args == ("ambiguous", largest), exc.args
else:
    raise AssertionError("ambiguous table was installed")
elapsed += time.perf_counter() - started

# Both rejections leave the installed table exactly as it was.
served = pub.resolve_device_config(dict(zip(axes, largest)))
assert served["root"] == "A", served["root"]
print("elapsed=%r" % elapsed)
'''
        project_dir = str(Path(__file__).resolve().parent)
        with TemporaryDirectory() as tmp:
            db_path = str(Path(tmp, "scale.db"))
            proc = subprocess.run(
                [sys.executable, "-B", "-I", "-c", script, project_dir, db_path],
                capture_output=True, text=True, timeout=60)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            report = dict(line.split("=", 1)
                          for line in proc.stdout.strip().splitlines())
            elapsed = float(report["elapsed"])
        self.assertLess(elapsed, 12.0, "symbolic scale took %.3fs" % elapsed)


if __name__ == "__main__":
    unittest.main()
