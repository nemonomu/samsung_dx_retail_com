import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from bestbuy.step14_db_load import availability_update_candidates  # noqa: E402


class Step14DbLoadTests(unittest.TestCase):
    def test_availability_update_can_be_limited_to_one_batch(self):
        rows = [
            {"batch_id": "b_20260525_040458", "item": "A"},
            {"batch_id": "b_other", "item": "B"},
            {"batch_id": "b_20260525_040458", "item": ""},
        ]

        result = availability_update_candidates(rows, "b_20260525_040458")

        self.assertEqual(result, [{"batch_id": "b_20260525_040458", "item": "A"}])


if __name__ == "__main__":
    unittest.main()
