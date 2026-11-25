"""Acceptance tests for the in-memory hot-reload job path.

A hot-reload job moves a fixed set of entry-point roots between two retained
group epochs on a node that loads configuration into numbered slots
((component name, revision)) and can only switch one root's whole dependency
route at a time. An old slot cannot be freed while any root still serves from
it. The tests exercise preparation, node-side step validation, duty planning
and the minimum-peak capacity math, including a 16-root scale scenario that
must be solved without enumerating root permutations.
"""
import json
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from distribution import Distribution


LEAF_WEIGHTS = {"p": 3, "q": 6, "r": 3, "s": 6, "t": 5, "u": 5}
OLD_DEPS = {"A": ("p", "q", "r"), "B": ("u",), "C": ("p", "q")}
NEW_DEPS = {"A": ("t",), "B": ("r", "s"), "C": ("p", "s", "t")}
ROOT_NAMES = ("A", "B", "C")
LEAF_NAMES = ("p", "q", "r", "s", "t", "u")


def make_config(tag):
    return {"upstream": "upstream-%s" % tag, "timeout_ms": 1000 + len(tag)}


class HotReloadFixture:
    """Builds the canonical three-root / six-leaf library used by the tests."""

    def __init__(self, publisher=None):
        self._tmp = TemporaryDirectory()
        self.path = Path(self._tmp.name, "state.db")
        self.pub = publisher or Distribution(self.path)
        self.revisions = {}
        for name in ROOT_NAMES + LEAF_NAMES:
            self.revisions[name] = self.pub.stage(make_config(name))
        self.old_epoch = self._publish(OLD_DEPS, 0)
        self.new_epoch = self._publish(NEW_DEPS, self.old_epoch)
        self.weights = {(n, self.revisions[n]): 1 for n in ROOT_NAMES}
        self.weights.update({(n, self.revisions[n]): w
                             for n, w in LEAF_WEIGHTS.items()})

    def _publish(self, deps, epoch):
        changes = {}
        for name in ROOT_NAMES + LEAF_NAMES:
            changes[name] = (
                self.revisions[name],
                {d: self.revisions[d] for d in deps.get(name, ())})
        return self.pub.publish_group(changes, expected_epoch=epoch)

    def job(self, roots=ROOT_NAMES, *, from_epoch=None, to_epoch=None,
            weights=None):
        return self.pub.prepare_hot_reload(
            tuple(roots),
            from_epoch=self.old_epoch if from_epoch is None else from_epoch,
            to_epoch=self.new_epoch if to_epoch is None else to_epoch,
            weights=self.weights if weights is None else weights)

    def slot(self, name):
        return name, self.revisions[name]

    def cleanup(self):
        self._tmp.cleanup()


def loaded_weight(state, weights):
    return sum(weights[slot] for slot in state["loaded"])


class PreparationTest(unittest.TestCase):
    def setUp(self):
        self.fx = HotReloadFixture()

    def tearDown(self):
        self.fx.cleanup()

    def test_endpoints_scopes_slots_and_bodies_are_snapshotted(self):
        fx = self.fx
        job = fx.job()
        # Slots identify (component, revision); the union of both endpoint
        # scopes covers the three root slots and all six leaf slots.
        self.assertEqual(len(job._slots), 9)
        for name in ROOT_NAMES + LEAF_NAMES:
            self.assertIn(fx.slot(name), job._slots)
        # Each root's slot set is its full transitive scope on each side,
        # roots included -- not just the put/remove difference.
        self.assertEqual(
            job._old_need["A"],
            frozenset(fx.slot(n) for n in ("A", "p", "q", "r")))
        self.assertEqual(job._old_need["B"],
                         frozenset(fx.slot(n) for n in ("B", "u")))
        self.assertEqual(job._old_need["C"],
                         frozenset(fx.slot(n) for n in ("C", "p", "q")))
        self.assertEqual(job._new_need["A"],
                         frozenset(fx.slot(n) for n in ("A", "t")))
        self.assertEqual(job._new_need["B"],
                         frozenset(fx.slot(n) for n in ("B", "r", "s")))
        self.assertEqual(
            job._new_need["C"],
            frozenset(fx.slot(n) for n in ("C", "p", "s", "t")))
        # Bodies come from the same snapshot and are delivered as exact JSON.
        record = fx.pub.poll_group(("A",), known_epoch=0)
        self.assertIsNotNone(record)
        slot = fx.slot("t")
        result = job.step(job.initial_state(), ("load", slot), capacity=1000)
        self.assertEqual(result["config"], make_config("t"))

    def test_job_is_immune_to_later_publishes(self):
        fx = self.fx
        job = fx.job()
        before = dict(job._bodies)
        # Republish every component on top of the target epoch; the retained
        # endpoints and bodies held by the in-memory job must not move.
        rerouted = dict(NEW_DEPS)
        rerouted["A"] = ("p",)
        later_epoch = fx._publish(rerouted, fx.new_epoch)
        self.assertGreater(later_epoch, fx.new_epoch)
        self.assertEqual(job._from_epoch, fx.old_epoch)
        self.assertEqual(job._to_epoch, fx.new_epoch)
        self.assertEqual(job._bodies, before)
        # The job still switches A toward t, not the freshly published p.
        self.assertIn(fx.slot("t"), job._new_need["A"])
        # A brand-new job targets the new current epoch instead.
        newer = fx.job(from_epoch=fx.old_epoch, to_epoch=later_epoch)
        self.assertNotIn(fx.slot("t"), newer._new_need["A"])

    def test_epoch_and_root_validation(self):
        fx = self.fx
        with self.assertRaises(ValueError):
            fx.job(from_epoch=True, to_epoch=fx.new_epoch)  # bool rejected
        with self.assertRaises(ValueError):
            fx.job(from_epoch=fx.old_epoch, to_epoch=False)
        with self.assertRaises(ValueError):
            fx.job(from_epoch=1.0, to_epoch=fx.new_epoch)
        with self.assertRaises(ValueError):
            fx.job(from_epoch=fx.new_epoch, to_epoch=fx.old_epoch)  # order
        with self.assertRaises(ValueError):
            fx.job(from_epoch=-1, to_epoch=fx.new_epoch)
        with self.assertRaises(ValueError):
            fx.job(from_epoch=fx.old_epoch, to_epoch=fx.old_epoch)  # equal
        # Root format follows poll_group.
        with self.assertRaises(ValueError):
            self.fx.pub.prepare_hot_reload(
                ["A"], from_epoch=fx.old_epoch, to_epoch=fx.new_epoch,
                weights=fx.weights)
        with self.assertRaises(ValueError):
            self.fx.pub.prepare_hot_reload(
                (), from_epoch=fx.old_epoch, to_epoch=fx.new_epoch,
                weights=fx.weights)
        with self.assertRaises(ValueError):
            self.fx.pub.prepare_hot_reload(
                ("A", "A"), from_epoch=fx.old_epoch, to_epoch=fx.new_epoch,
                weights=fx.weights)
        with self.assertRaises(ValueError):
            self.fx.pub.prepare_hot_reload(
                ("ghost",), from_epoch=fx.old_epoch, to_epoch=fx.new_epoch,
                weights={("ghost", 1): 1})

    def test_weights_must_cover_exactly_the_job_slots(self):
        fx = self.fx
        good = dict(fx.weights)
        with self.assertRaises(ValueError):
            fx.job(weights={})  # covers nothing
        missing = dict(good)
        del missing[fx.slot("t")]
        with self.assertRaises(ValueError):
            fx.job(weights=missing)
        extra = dict(good)
        extra[("ghost", 999)] = 4
        with self.assertRaises(ValueError):
            fx.job(weights=extra)
        for bad in (0, -1, 1_000_001, True, 1.5, "7"):
            bad_weights = dict(good)
            bad_weights[fx.slot("t")] = bad
            with self.assertRaises(ValueError):
                fx.job(weights=bad_weights)
        # Malformed slot keys.
        bad_key = dict(good)
        del bad_key[fx.slot("t")]
        bad_key[("t",)] = 5
        with self.assertRaises(ValueError):
            fx.job(weights=bad_key)
        with self.assertRaises(ValueError):
            fx.job(weights="not-a-dict")

    def test_slot_and_body_limits(self):
        fx = self.fx
        # 128 distinct slots cap: the canonical job has 9, build a wider one.
        revs = {}
        changes = {}
        names = ["r%03d" % i for i in range(130)]
        for name in names:
            revs[name] = fx.pub.stage(make_config(name))
            changes[name] = (revs[name], {})
        wide_epoch = fx.pub.publish_group(
            dict(list(changes.items())[:32]), expected_epoch=fx.new_epoch)
        for i in range(32, len(names), 32):
            wide_epoch = fx.pub.publish_group(
                dict(list(changes.items())[i:i + 32]),
                expected_epoch=wide_epoch)
        roots = tuple(names[:16])
        weights = {(n, revs[n]): 1 for n in names}
        with self.assertRaises(ValueError):
            fx.pub.prepare_hot_reload(
                roots, from_epoch=wide_epoch, to_epoch=wide_epoch,
                weights=weights)

        # Body size cap measured on stored UTF-8 JSON text (256 KiB).
        big_rev = fx.pub.stage(
            {"upstream": "x" * (256 * 1024), "timeout_ms": 1})
        fx.pub.publish_group({"big": (big_rev, {})}, expected_epoch=wide_epoch)
        big_epoch = wide_epoch + 1
        with self.assertRaises(ValueError):
            fx.pub.prepare_hot_reload(
                ("big",), from_epoch=big_epoch, to_epoch=big_epoch,
                weights={("big", big_rev): 1})

    def test_epoch_fallen_out_of_retained_window_fails(self):
        fx = self.fx
        # 256 further publishes move the old epoch out of the 256-epoch
        # window; preparing a job anchored at it must then fail.
        epoch = fx.new_epoch
        anchor = fx.revisions["A"]
        for _ in range(256):
            epoch = fx.pub.publish_group(
                {"A": (anchor, {"t": fx.revisions["t"]})},
                expected_epoch=epoch)
        with self.assertRaises(ValueError):
            fx.job(from_epoch=fx.old_epoch, to_epoch=fx.new_epoch)

    def test_target_epoch_need_not_be_current(self):
        fx = self.fx
        # Publish one more epoch so to_epoch is retained but no longer the
        # current one; the job must still bind to the explicitly named
        # endpoints rather than substituting the current group.
        later = fx._publish(NEW_DEPS, fx.new_epoch)
        self.assertGreater(later, fx.new_epoch)
        job = fx.job(from_epoch=fx.old_epoch, to_epoch=fx.new_epoch)
        self.assertEqual(job._to_epoch, fx.new_epoch)
        self.assertEqual(
            job._new_need["B"],
            frozenset(fx.slot(n) for n in ("B", "r", "s")))

    def test_capacity_math_uses_weight_sum_not_slot_count(self):
        fx = self.fx
        # Reweight every slot to a distinct value and confirm the initial
        # loaded weight is the sum, while the slot count stays unchanged.
        weights = {}
        for i, slot in enumerate(sorted(fx.weights), start=1):
            weights[slot] = i * 2
        job = fx.job(weights=weights)
        init = job.initial_state()
        expected = sum(weights[s] for s in init["loaded"])
        self.assertEqual(loaded_weight(init, weights), expected)
        plan = job.plan(init, capacity=10_000_000)
        # Replay and verify every peak against the summed weights, never a
        # count of slots.
        state = init
        peak = expected
        for action in plan["actions"]:
            result = job.step(state, action, capacity=10_000_000)
            self.assertIsNotNone(result, action)
            state = result["state"]
            peak = max(peak, loaded_weight(state, weights))
        self.assertEqual(plan["required_capacity"], peak)

    def test_bool_revision_in_weight_key_rejected(self):
        fx = self.fx
        bad = dict(fx.weights)
        t_slot = fx.slot("t")
        del bad[t_slot]
        bad[("t", True)] = 5
        with self.assertRaises(ValueError):
            fx.job(weights=bad)


class InitialStateAndStepTest(unittest.TestCase):
    def setUp(self):
        self.fx = HotReloadFixture()
        self.job = self.fx.job()
        self.weights = self.fx.weights

    def tearDown(self):
        self.fx.cleanup()

    def test_initial_state_shape_and_sorted_tuples(self):
        state = self.job.initial_state()
        self.assertEqual(state["switched"], ())
        expected_old = set()
        for root in ROOT_NAMES:
            expected_old.update(self.job._old_need[root])
        self.assertEqual(set(state["loaded"]), expected_old)
        # Both entries are duplicate-free, lexicographically sorted tuples.
        self.assertEqual(state["loaded"], tuple(sorted(state["loaded"])))
        self.assertEqual(len(state["loaded"]), len(set(state["loaded"])))
        self.assertEqual(loaded_weight(state, self.weights), 20)

    def test_load_delivers_exact_body_only_when_accepted(self):
        fx = self.fx
        init = self.job.initial_state()
        # 20 already loaded; t weighs 5 -> 24 rejects, 25 accepts.
        self.assertIsNone(
            self.job.step(init, ("load", fx.slot("t")), capacity=24))
        accepted = self.job.step(init, ("load", fx.slot("t")), capacity=25)
        self.assertIsNotNone(accepted)
        self.assertEqual(accepted["config"], make_config("t"))
        self.assertEqual(accepted["state"]["switched"], ())
        self.assertIn(fx.slot("t"), accepted["state"]["loaded"])
        # Reloading an already loaded slot is a legal shape but cannot run.
        self.assertIsNone(
            self.job.step(accepted["state"], ("load", fx.slot("t")),
                          capacity=1000))
        # Mutating a delivered config must not poison later deliveries.
        accepted["config"]["upstream"] = "tampered"
        again = self.job.step(init, ("load", fx.slot("t")), capacity=25)
        self.assertEqual(again["config"], make_config("t"))

    def test_switch_requires_full_target_scope_and_moves_one_route(self):
        fx = self.fx
        init = self.job.initial_state()
        # B needs r (already loaded) plus s; switching before loading s fails.
        self.assertIsNone(self.job.step(init, ("switch", "B"), capacity=1000))
        loaded = self.job.step(init, ("load", fx.slot("s")),
                               capacity=1000)["state"]
        switched = self.job.step(loaded, ("switch", "B"), capacity=1000)
        self.assertIsNotNone(switched)
        self.assertEqual(switched["config"], None)
        self.assertEqual(switched["state"]["switched"], ("B",))
        # A switch neither loads nor unloads slots nor touches other roots.
        self.assertEqual(switched["state"]["loaded"], loaded["loaded"])
        # Switching the same root twice is rejected as not executable.
        self.assertIsNone(
            self.job.step(switched["state"], ("switch", "B"), capacity=1000))

    def test_unload_refused_while_any_root_still_uses_slot(self):
        fx = self.fx
        init = self.job.initial_state()
        # u is still on B's old route before B switches.
        self.assertIsNone(
            self.job.step(init, ("unload", fx.slot("u")), capacity=1000))
        # Shared slot p serves both A and C; switching A alone keeps p via C.
        loaded = self.job.step(init, ("load", fx.slot("t")),
                               capacity=1000)["state"]
        a_switched = self.job.step(loaded, ("switch", "A"),
                                   capacity=1000)["state"]
        self.assertIsNone(
            self.job.step(a_switched, ("unload", fx.slot("p")),
                          capacity=1000))
        # r becomes dead only after A switches (B's new side still needs it,
        # but B has not switched, so r currently serves nobody): it unloads.
        result = self.job.step(a_switched, ("unload", fx.slot("r")),
                               capacity=1000)
        self.assertIsNotNone(result)
        self.assertNotIn(fx.slot("r"), result["state"]["loaded"])
        # Unloading a slot that is not loaded is a rejected action, too.
        self.assertIsNone(
            self.job.step(init, ("unload", fx.slot("s")), capacity=1000))

    def test_u_freed_only_after_b_actually_switches(self):
        fx = self.fx
        state = self.job.initial_state()
        self.assertIsNone(
            self.job.step(state, ("unload", fx.slot("u")), capacity=1000))
        state = self.job.step(state, ("load", fx.slot("s")),
                              capacity=1000)["state"]
        state = self.job.step(state, ("switch", "B"),
                              capacity=1000)["state"]
        freed = self.job.step(state, ("unload", fx.slot("u")), capacity=1000)
        self.assertIsNotNone(freed)
        self.assertNotIn(fx.slot("u"), freed["state"]["loaded"])

    def test_invalid_actions_and_states_raise_value_error(self):
        fx = self.fx
        init = self.job.initial_state()
        # Capacity shape.
        for bad in (-1, 128_000_001, True, 1.0):
            with self.assertRaises(ValueError):
                self.job.step(init, ("load", fx.slot("t")), capacity=bad)
        # Unknown verbs, arity and identifiers.
        with self.assertRaises(ValueError):
            self.job.step(init, ("frobnicate", "A"), capacity=1000)
        with self.assertRaises(ValueError):
            self.job.step(init, ("load", "t"), capacity=1000)
        with self.assertRaises(ValueError):
            self.job.step(init, ("load", ("nope", 1)), capacity=1000)
        with self.assertRaises(ValueError):
            self.job.step(init, ("switch", "Z"), capacity=1000)
        with self.assertRaises(ValueError):
            self.job.step(init, ("unload", ("t", True)), capacity=1000)
        with self.assertRaises(ValueError):
            self.job.step(init, ("load",), capacity=1000)
        # Malformed state shapes.
        with self.assertRaises(ValueError):
            self.job.step({"switched": [], "loaded": ()},
                          ("switch", "A"), capacity=1000)
        with self.assertRaises(ValueError):
            self.job.step({"switched": ("A", "A"), "loaded": ()},
                          ("switch", "A"), capacity=1000)
        with self.assertRaises(ValueError):
            self.job.step({"switched": ("Z",), "loaded": ()},
                          ("switch", "A"), capacity=1000)
        with self.assertRaises(ValueError):
            self.job.step({"switched": (), "loaded": (("t", 1),)},
                          ("switch", "A"), capacity=1000)
        # State missing a slot a root in service requires.
        broken = {"switched": (),
                  "loaded": tuple(s for s in init["loaded"]
                                  if s != fx.slot("u"))}
        with self.assertRaises(ValueError):
            self.job.step(broken, ("switch", "B"), capacity=1000)
        # A live state whose loaded weight already exceeds capacity.
        with self.assertRaises(ValueError):
            self.job.step(init, ("load", fx.slot("t")), capacity=19)

    def test_inputs_are_not_mutated(self):
        fx = self.fx
        init = self.job.initial_state()
        snapshot = (init["switched"], init["loaded"])
        state_copy = {"switched": init["switched"],
                      "loaded": init["loaded"]}
        action = ("load", fx.slot("t"))
        self.job.step(state_copy, action, capacity=1000)
        self.assertEqual((init["switched"], init["loaded"]), snapshot)
        self.assertEqual(state_copy["loaded"], init["loaded"])

    def test_non_dict_and_other_malformed_states_raise(self):
        with self.assertRaises(ValueError):
            self.job.step(("switched", "loaded"),
                          ("switch", "A"), capacity=1000)
        with self.assertRaises(ValueError):
            self.job.step({"switched": None, "loaded": ()},
                          ("switch", "A"), capacity=1000)
        with self.assertRaises(ValueError):
            self.job.step({"switched": (), "loaded": None},
                          ("switch", "A"), capacity=1000)
        with self.assertRaises(ValueError):
            self.job.plan("not-a-state", capacity=1000)
        with self.assertRaises(ValueError):
            self.job.plan({"switched": (), "loaded": (), "extra": 1},
                          capacity=1000)

    def test_zero_capacity_rejects_load_but_keeps_validation(self):
        init = self.job.initial_state()
        # The start state already weighs 20, so even consulting it at
        # capacity zero surfaces the exact threshold without actions.
        advise = self.job.plan(init, capacity=0)
        self.assertEqual(advise["required_capacity"], 26)
        self.assertIsNone(advise["actions"])
        with self.assertRaises(ValueError):
            self.job.step(init, ("load", self.fx.slot("t")), capacity=0)

    def test_root_revision_unchanged_but_scope_changed_still_switches(self):
        # The canonical fixture keeps A/B/C on the same revision at both
        # epochs; a switch must nonetheless be required and executable.
        fx = self.fx
        record = fx.pub.poll_group(
            ("A",), known_epoch=fx.old_epoch)
        later = fx.pub.poll_group(("A",), known_epoch=fx.old_epoch)
        self.assertEqual(record, later)
        init = self.job.initial_state()
        state = self.job.step(init, ("load", fx.slot("t")),
                              capacity=1000)["state"]
        moved = self.job.step(state, ("switch", "A"), capacity=1000)
        self.assertIsNotNone(moved)
        self.assertEqual(moved["state"]["switched"], ("A",))
        # A's own (component, revision) slot is identical on both sides.
        self.assertEqual(
            self.job._old_need["A"] & self.job._new_need["A"],
            frozenset([fx.slot("A")]))

    def test_job_remains_usable_after_database_reopens(self):
        fx = self.fx
        job = self.job
        init = job.initial_state()
        # The job is purely in-memory: a fresh Distribution handle and even a
        # new publisher connection must not affect its decisions or bodies.
        reopened = Distribution(fx.path)
        self.assertIsNone(
            reopened.poll_group(ROOT_NAMES, known_epoch=fx.new_epoch))
        result = job.step(init, ("load", fx.slot("t")), capacity=1000)
        self.assertEqual(result["config"], make_config("t"))


class PlanningTest(unittest.TestCase):
    def setUp(self):
        self.fx = HotReloadFixture()
        self.job = self.fx.job()
        self.weights = self.fx.weights

    def tearDown(self):
        self.fx.cleanup()

    def _replay(self, plan, state, capacity):
        """Drive every planned action through step() and track the real peak."""
        self.assertIsNotNone(plan["actions"])
        peak = loaded_weight(state, self.weights)
        switched = set(state["switched"])
        active_by_root = {
            root: set(self.job._new_need[root] if root in switched
                      else self.job._old_need[root])
            for root in ROOT_NAMES}
        for action in plan["actions"]:
            result = self.job.step(state, action, capacity=capacity)
            self.assertIsNotNone(result, action)
            state = result["state"]
            peak = max(peak, loaded_weight(state, self.weights))
            if action[0] == "switch":
                root = action[1]
                active_by_root[root] = set(self.job._new_need[root])
            # After every accepted action, each root's active route must be
            # fully present in the node's loaded slots.
            loaded = set(state["loaded"])
            for root, need in active_by_root.items():
                self.assertTrue(need <= loaded, (action, root))
        return state, peak

    def test_minimum_peak_is_26_and_capacity_25_unreachable(self):
        init = self.job.initial_state()
        under = self.job.plan(init, capacity=25)
        self.assertEqual(under["required_capacity"], 26)
        self.assertIsNone(under["actions"])
        feasible = self.job.plan(init, capacity=26)
        self.assertEqual(feasible["required_capacity"], 26)
        final_state, peak = self._replay(feasible, init, 26)
        self.assertEqual(peak, 26)
        # Terminal: every root switched, only target-side union remains.
        self.assertEqual(set(final_state["switched"]), set(ROOT_NAMES))
        target = set()
        for root in ROOT_NAMES:
            target |= self.job._new_need[root]
        self.assertEqual(set(final_state["loaded"]), target)

    def test_switching_a_first_cannot_hold_the_26_threshold(self):
        init = self.job.initial_state()
        state = self.job.step(init, ("load", self.fx.slot("t")),
                              capacity=1000)["state"]
        state = self.job.step(state, ("switch", "A"),
                              capacity=1000)["state"]
        replan = self.job.plan(state, capacity=26)
        self.assertGreater(replan["required_capacity"], 26)
        self.assertIsNone(replan["actions"])

    def test_manual_partial_rollout_replans_from_real_state(self):
        fx = self.fx
        init = self.job.initial_state()
        # Hand operations: preload t, switch A, deliberately keep r loaded.
        state = self.job.step(init, ("load", fx.slot("t")),
                              capacity=1000)["state"]
        state = self.job.step(state, ("switch", "A"),
                              capacity=1000)["state"]
        self.assertIn(fx.slot("r"), state["loaded"])
        blocked = self.job.plan(state, capacity=26)
        self.assertEqual(blocked["required_capacity"], 28)
        self.assertIsNone(blocked["actions"])
        viable = self.job.plan(state, capacity=28)
        self.assertEqual(viable["required_capacity"], 28)
        final_state, peak = self._replay(viable, state, 28)
        self.assertEqual(peak, 28)
        target = set()
        for root in ROOT_NAMES:
            target |= self.job._new_need[root]
        self.assertEqual(set(final_state["loaded"]), target)

    def test_terminal_state_reports_empty_work_order(self):
        init = self.job.initial_state()
        plan = self.job.plan(init, capacity=1000)
        state, _ = self._replay(plan, init, 1000)
        done = self.job.plan(state, capacity=1000)
        self.assertEqual(done["required_capacity"],
                         loaded_weight(state, self.weights))
        self.assertEqual(done["actions"], ())

    def test_capacity_below_starting_weight_still_advises_threshold(self):
        init = self.job.initial_state()
        # Consulting scenario: validation still passes, threshold is exact,
        # actions None even though the node could not even start.
        advise = self.job.plan(init, capacity=10)
        self.assertEqual(advise["required_capacity"], 26)
        self.assertIsNone(advise["actions"])
        with self.assertRaises(ValueError):
            self.job.plan(init, capacity=True)
        with self.assertRaises(ValueError):
            self.job.plan(init, capacity=128_000_001)

    def test_plan_rejects_same_invalid_states_as_step(self):
        init = self.job.initial_state()
        broken = {"switched": (),
                  "loaded": tuple(s for s in init["loaded"]
                                  if s != self.fx.slot("p"))}
        with self.assertRaises(ValueError):
            self.job.plan(broken, capacity=1000)
        with self.assertRaises(ValueError):
            self.job.plan({"switched": ("Q",), "loaded": ()}, capacity=1000)


class StructuralSlotTest(unittest.TestCase):
    """Transitive deps, shared slots, parallel versions and shared revisions."""

    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.path = Path(self._tmp.name, "structural.db")
        self.pub = Distribution(self.path)

    def tearDown(self):
        self._tmp.cleanup()

    def _stage(self, tag):
        return self.pub.stage(make_config(tag))

    def test_transitive_dependencies_count_in_full_route(self):
        # root -> mid -> leaf: switching root must load the whole chain.
        r_root, r_mid, r_leaf = self._stage("root"), self._stage("mid"), self._stage("leaf")
        r_root2, r_mid2 = self._stage("root2"), self._stage("mid2")
        epoch1 = self.pub.publish_group({
            "root": (r_root, {"mid": r_mid}),
            "mid": (r_mid, {"leaf": r_leaf}),
            "leaf": (r_leaf, {})}, expected_epoch=0)
        epoch2 = self.pub.publish_group({
            "root": (r_root2, {"mid": r_mid2}),
            "mid": (r_mid2, {"leaf": r_leaf}),
            "leaf": (r_leaf, {})}, expected_epoch=epoch1)
        weights = {("root", r_root): 1, ("root", r_root2): 1,
                   ("mid", r_mid): 2, ("mid", r_mid2): 2,
                   ("leaf", r_leaf): 4}
        job = self.pub.prepare_hot_reload(
            ("root",), from_epoch=epoch1, to_epoch=epoch2, weights=weights)
        self.assertEqual(
            job._old_need["root"],
            frozenset((("root", r_root), ("mid", r_mid), ("leaf", r_leaf))))
        self.assertEqual(
            job._new_need["root"],
            frozenset((("root", r_root2), ("mid", r_mid2), ("leaf", r_leaf))))
        # Switching needs both new revisions staged; leaf revision is shared.
        state = job.initial_state()
        self.assertIsNone(job.step(state, ("switch", "root"), capacity=1000))
        state = job.step(state, ("load", ("mid", r_mid2)),
                         capacity=1000)["state"]
        self.assertIsNone(job.step(state, ("switch", "root"), capacity=1000))
        state = job.step(state, ("load", ("root", r_root2)),
                         capacity=1000)["state"]
        done = job.step(state, ("switch", "root"), capacity=1000)
        self.assertIsNotNone(done)
        # The shared leaf slot stays one capacity entry throughout.
        self.assertEqual(len([s for s in done["state"]["loaded"]
                              if s == ("leaf", r_leaf)]), 1)

    def test_shared_slot_last_reference_blocks_unload(self):
        # Two roots share one leaf; the leaf may only unload once both moved.
        ra, rb, leaf1, leaf2 = (self._stage(n) for n in
                                ("ra", "rb", "leaf1", "leaf2"))
        e1 = self.pub.publish_group({
            "ra": (ra, {"leaf": leaf1}), "rb": (rb, {"leaf": leaf1}),
            "leaf": (leaf1, {})}, expected_epoch=0)
        e2 = self.pub.publish_group({
            "ra": (ra, {"leaf": leaf2}), "rb": (rb, {"leaf": leaf2}),
            "leaf": (leaf2, {})}, expected_epoch=e1)
        weights = {("ra", ra): 1, ("rb", rb): 1,
                   ("leaf", leaf1): 7, ("leaf", leaf2): 7}
        job = self.pub.prepare_hot_reload(
            ("ra", "rb"), from_epoch=e1, to_epoch=e2, weights=weights)
        state = job.initial_state()
        state = job.step(state, ("load", ("leaf", leaf2)),
                         capacity=1000)["state"]
        state = job.step(state, ("switch", "ra"),
                         capacity=1000)["state"]
        # rb still serves the old leaf: last live reference, unload refused.
        self.assertIsNone(
            job.step(state, ("unload", ("leaf", leaf1)), capacity=1000))
        state = job.step(state, ("switch", "rb"),
                         capacity=1000)["state"]
        freed = job.step(state, ("unload", ("leaf", leaf1)), capacity=1000)
        self.assertIsNotNone(freed)

    def test_two_versions_of_one_component_coexist(self):
        r1, r2 = self._stage("v1"), self._stage("v2")
        e1 = self.pub.publish_group({"c": (r1, {})}, expected_epoch=0)
        e2 = self.pub.publish_group({"c": (r2, {})}, expected_epoch=e1)
        job = self.pub.prepare_hot_reload(
            ("c",), from_epoch=e1, to_epoch=e2,
            weights={("c", r1): 3, ("c", r2): 9})
        state = job.initial_state()
        self.assertEqual(state["loaded"], (("c", r1),))
        loaded = job.step(state, ("load", ("c", r2)), capacity=1000)
        # Both revisions of the same component are resident at once in
        # distinct slots, each carrying its own weight.
        self.assertEqual(
            set(loaded["state"]["loaded"]), {("c", r1), ("c", r2)})
        self.assertEqual(loaded_weight(loaded["state"], job._weights), 12)
        switched = job.step(loaded["state"], ("switch", "c"), capacity=1000)
        self.assertIsNotNone(switched)
        # After switching, the old revision can finally be unloaded.
        self.assertIsNone(
            job.step(loaded["state"], ("unload", ("c", r1)), capacity=1000))
        done = job.step(switched["state"], ("unload", ("c", r1)),
                        capacity=1000)
        self.assertEqual(done["state"]["loaded"], (("c", r2),))

    def test_two_components_reusing_one_revision_use_two_slots(self):
        shared = self._stage("shared")
        e1 = self.pub.publish_group({
            "x": (shared, {}), "y": (shared, {})}, expected_epoch=0)
        # New epoch gives each component its own staged revision, but keep one
        # shared-revision case visible at the old endpoint.
        rx, ry = self._stage("x2"), self._stage("y2")
        e2 = self.pub.publish_group({
            "x": (rx, {}), "y": (ry, {})}, expected_epoch=e1)
        job = self.pub.prepare_hot_reload(
            ("x", "y"), from_epoch=e1, to_epoch=e2,
            weights={("x", shared): 4, ("y", shared): 4,
                     ("x", rx): 4, ("y", ry): 4})
        # Same revision id, different component names -> distinct slots, both
        # counted separately despite the identical staged version.
        self.assertIn(("x", shared), job._slots)
        self.assertIn(("y", shared), job._slots)
        self.assertEqual(loaded_weight(job.initial_state(), job._weights), 8)


class ScaleTest(unittest.TestCase):
    def test_five_structures_plus_solo_root_peak_107_within_12_seconds(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "scale.db")
            pub = Distribution(path)
            revisions = {}
            weights = {}
            roots = []
            old_deps, new_deps = {}, {}
            for k in range(5):
                prefix = "g%d_" % k
                for base in ROOT_NAMES + LEAF_NAMES:
                    name = prefix + base
                    revisions[name] = pub.stage(make_config(name))
                for base in ROOT_NAMES:
                    roots.append(prefix + base)
                    weights[(prefix + base, revisions[prefix + base])] = 1
                for base, leaf_weight in LEAF_WEIGHTS.items():
                    weights[(prefix + base, revisions[prefix + base])] = leaf_weight
                old_deps[prefix + "A"] = (prefix + "p", prefix + "q", prefix + "r")
                old_deps[prefix + "B"] = (prefix + "u",)
                old_deps[prefix + "C"] = (prefix + "p", prefix + "q")
                new_deps[prefix + "A"] = (prefix + "t",)
                new_deps[prefix + "B"] = (prefix + "r", prefix + "s")
                new_deps[prefix + "C"] = (
                    prefix + "p", prefix + "s", prefix + "t")
            solo_rev = pub.stage(make_config("solo"))
            revisions["solo"] = solo_rev
            roots.append("solo")
            weights[("solo", solo_rev)] = 1

            def publish(deps, epoch):
                for k in range(5):
                    prefix = "g%d_" % k
                    batch = {
                        prefix + b: (
                            revisions[prefix + b],
                            {d: revisions[d]
                             for d in deps.get(prefix + b, ())})
                        for b in ROOT_NAMES + LEAF_NAMES}
                    epoch = pub.publish_group(batch, expected_epoch=epoch)
                return pub.publish_group(
                    {"solo": (solo_rev, {})}, expected_epoch=epoch)

            old_epoch = publish(old_deps, 0)
            new_epoch = publish(new_deps, old_epoch)
            job = pub.prepare_hot_reload(
                tuple(roots), from_epoch=old_epoch, to_epoch=new_epoch,
                weights=weights)
            self.assertEqual(len(roots), 16)

            def weight_of(state):
                return loaded_weight(state, weights)

            initial = job.initial_state()
            self.assertEqual(weight_of(initial), 101)  # 5 * 20 + solo 1

            started = time.perf_counter()
            unreachable = job.plan(initial, capacity=106)
            reachable = job.plan(initial, capacity=107)
            self.assertEqual(unreachable["required_capacity"], 107)
            self.assertIsNone(unreachable["actions"])
            self.assertEqual(reachable["required_capacity"], 107)

            # Replay the whole feasible work order through step() one action
            # at a time, verifying the real peak and the absence of leftovers.
            state = initial
            peak = weight_of(state)
            for action in reachable["actions"]:
                result = job.step(state, action, capacity=107)
                self.assertIsNotNone(result, action)
                state = result["state"]
                peak = max(peak, weight_of(state))

            # A second planning run from a genuinely partially switched node,
            # as an operator would request after manual work.
            partial = job.step(initial, ("load", ("g0_t", revisions["g0_t"])),
                               capacity=128_000_000)["state"]
            partial = job.step(partial, ("switch", "g0_A"),
                               capacity=128_000_000)["state"]
            job.plan(partial, capacity=128_000_000)
            elapsed = time.perf_counter() - started
            self.assertLess(elapsed, 12.0, "planning/replay took %.2fs" % elapsed)

            self.assertEqual(peak, 107)
            self.assertEqual(set(state["switched"]), set(roots))
            target = set()
            for root in roots:
                target |= job._new_need[root]
            self.assertEqual(set(state["loaded"]), target)


if __name__ == "__main__":
    unittest.main()
