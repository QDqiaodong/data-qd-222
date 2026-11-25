from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from distribution import ConflictingPromotion, Distribution


class DistributionTest(unittest.TestCase):
    def test_compare_and_swap_and_rollback_with_new_generation(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "state.db")
            writer, client = Distribution(path), Distribution(path)
            a = writer.stage({"upstream": "cache-a", "timeout_ms": 50})
            b = writer.stage({"upstream": "cache-b", "timeout_ms": 90})
            self.assertIsNone(client.poll())
            self.assertEqual(writer.promote(a, expected_generation=0), 1)
            self.assertEqual(client.poll()[2]["upstream"], "cache-a")
            self.assertIsNone(client.poll(1))
            with self.assertRaises(ConflictingPromotion):
                writer.promote(b, expected_generation=0)
            self.assertIsNone(client.poll(1))
            self.assertEqual(writer.promote(b, expected_generation=1), 2)
            self.assertEqual(writer.promote(a, expected_generation=2), 3)
            self.assertEqual(client.poll(1)[:2], (3, a))

    def test_invalid_config_and_unknown_revision_rejected(self):
        with TemporaryDirectory() as tmp:
            writer = Distribution(Path(tmp, "state.db"))
            with self.assertRaises(ValueError):
                writer.stage({"upstream": "a", "timeout_ms": True})
            with self.assertRaises(KeyError):
                writer.promote(1, expected_generation=0)
            self.assertIsNone(writer.poll())
