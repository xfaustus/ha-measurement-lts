import importlib.util
from pathlib import Path
import sys
import unittest

path = Path(__file__).parents[1] / "custom_components/measurement_lts/aggregate.py"
spec = importlib.util.spec_from_file_location("backfill_aggregate", path)
aggregate = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = aggregate
spec.loader.exec_module(aggregate)


def readings(*pairs):
    return [(timestamp, value, True) for timestamp, value in pairs]


class HourlyTest(unittest.TestCase):
    def test_unavailable_ignored_inside_block(self):
        hours = aggregate.hourly(readings((0, 10), (100, None), (150, 20), (299, None)), 0, 3600)
        self.assertEqual(hours[0].values(), (15, 10, 20))
        self.assertEqual(len(hours[0].means), 1)

    def test_first_value_near_end_of_block(self):
        hours = aggregate.hourly(readings((299, 12), (300, None)), 0, 3600)
        self.assertEqual(hours[0].values(), (12, 12, 12))

    def test_hour_uses_mean_of_available_blocks(self):
        hours = aggregate.hourly(readings((299, 10), (299.5, None), (301, 20), (599, None)), 0, 3600)
        self.assertEqual(hours[0].values(), (15, 10, 20))
        self.assertEqual(len(hours[0].means), 2)

    def test_state_at_start_of_hour(self):
        hours = aggregate.hourly(readings((-5000, 20)), 0, 3600)
        self.assertEqual(hours[0].values(), (20, 20, 20))

    def test_no_valid_state_does_not_create_an_hour(self):
        self.assertEqual(aggregate.hourly(readings((-1, None)), 0, 3600), {})

    def test_attribute_only_updates_supply_next_block_start(self):
        samples = [(0, 8, True), (240, 10, False), (450, None, True)]
        hours = aggregate.hourly(samples, 0, 3600)
        self.assertEqual(hours[0].means, [8, 10])

    def test_reconstructed_blocks_can_be_saved_without_changing_hourly_values(self):
        samples = readings((10, 12), (100, None), (400, 20), (600, None))
        blocks = dict(aggregate.five_minute(samples, 0, 3600))
        self.assertEqual(blocks, {0: (12, 12, 12), 300: (20, 20, 20), 600: (20, 20, 20)})
        self.assertEqual(aggregate.combine_hours(blocks.items()), aggregate.hourly(samples, 0, 3600))

    def test_checks_all_values_in_overlap(self):
        hour = aggregate.hourly(readings((0, 20)), 0, 3600)[0]
        self.assertTrue(aggregate.matches(hour, (20.0000001, 20, 20)))
        self.assertFalse(aggregate.matches(hour, (20, 19, 20)))
        self.assertFalse(aggregate.matches(hour, (None, 20, 20)))


if __name__ == "__main__":
    unittest.main()
