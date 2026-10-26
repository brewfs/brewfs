import csv
import json
import tempfile
import unittest
from pathlib import Path

try:
    from .aggregate_observation_matrix import generate, load_campaign, load_observations, render_table
except ImportError:
    from aggregate_observation_matrix import generate, load_campaign, load_observations, render_table


PERF_DIR = Path(__file__).parent
CAMPAIGN_PATH = PERF_DIR / "fio-matrix-r1-campaign.json"
LEDGER_FIELDS = ("tool", "preset", "direct", "column", "repetition", "metric", "value", "label")


def row(preset, direct, tool, metric, direction):
    return {
        "preset": preset,
        "direct": direct,
        "tool": tool,
        "metric": metric,
        "direction": direction,
    }


def observation(identity, column, repetition, value):
    preset, direct, tool, metric = identity
    return {
        "tool": tool,
        "preset": preset,
        "direct": direct,
        "column": column,
        "repetition": repetition,
        "metric": metric,
        "value": value,
        "label": f"{preset}__{tool}__d{direct}__{column}__r{repetition}",
    }


class ObservationMatrixTests(unittest.TestCase):
    def test_campaign_catalog_reproduces_all_27_existing_row_identities(self):
        campaign = load_campaign(CAMPAIGN_PATH)
        identities = {
            (item["preset"], item["direct"], item["tool"], item["metric"])
            for item in campaign["rows"]
        }
        self.assertEqual(len(identities), 27)
        expected_tools_metrics = {
            ("0", "fio", "MiB/s"),
            ("0", "fio-bigread", "MiB/s"),
            ("0", "fio-bigwrite", "MiB/s"),
            ("0", "fio-randread", "MiB/s"),
            ("0", "fio-randrw", "MiB/s"),
            ("0", "fio-randwrite", "MiB/s"),
            ("0", "fio-seqread", "MiB/s"),
            ("1", "fio-seqread", "MiB/s"),
            ("0", "fio-seqwrite", "MiB/s"),
            ("NA", "bench2", "create_1t_ops"),
            ("NA", "bench2", "stat_1t_ops"),
            ("NA", "bench2", "unlink_1t_ops"),
            ("NA", "dirperf", "seconds"),
            ("NA", "dirstress", "seconds"),
            ("NA", "looptest", "seconds"),
            ("NA", "metaperf", "create_ops/s"),
            ("NA", "metaperf", "open_ops/s"),
            ("NA", "metaperf", "readdir_ops/s"),
            ("NA", "metaperf", "rename_ops/s"),
            ("NA", "metaperf", "stat_ops/s"),
            ("NA", "smallfiles-read", "files/s"),
            ("NA", "smallfiles-stat", "files/s"),
            ("NA", "stress-ng", "dentry_bogo_ops/s"),
            ("NA", "stress-ng", "dir_bogo_ops/s"),
            ("NA", "stress-ng", "hdd_bogo_ops/s"),
            ("NA", "stress-ng", "rename_bogo_ops/s"),
            ("NA", "stress-ng", "unlink_bogo_ops/s"),
        }
        self.assertEqual(
            {(direct, tool, metric) for _, direct, tool, metric in identities},
            expected_tools_metrics,
        )
        self.assertTrue(all(preset == "metadata-throughput-profile" for preset, *_ in identities))

    def test_zero_and_one_observation_are_not_measured_values(self):
        campaign = {
            "name": "synthetic",
            "required_observations": 3,
            "builds": {"brewfs": "test build", "juicefs": "test baseline"},
            "cache_policy": "test cache policy",
            "rows": [
                row("p", "0", "empty", "MiB/s", "rate"),
                row("p", "0", "single", "MiB/s", "rate"),
            ],
        }
        observations = {
            (("p", "0", "single", "MiB/s"), "native"): [12.0],
        }

        report = render_table(campaign, observations)

        empty_line = next(line for line in report.splitlines() if "| p | 0 | empty |" in line)
        single_line = next(line for line in report.splitlines() if "| p | 0 | single |" in line)
        for cell in empty_line[1:-1].split("|")[8:]:
            self.assertTrue(cell.strip().startswith("not-measured (n=0/3"), cell)
        self.assertIn("not-measured (n=1/3)", single_line)
        self.assertNotIn("12 (n=1/3)", report)
        self.assertNotIn("median of three", report.lower())

    def test_elapsed_ratio_is_inverted_for_lower_is_better(self):
        campaign = {
            "name": "synthetic",
            "required_observations": 3,
            "builds": {"brewfs": "test build", "juicefs": "test baseline"},
            "cache_policy": "test cache policy",
            "rows": [row("p", "NA", "dirperf", "seconds", "elapsed")],
        }
        identity = ("p", "NA", "dirperf", "seconds")
        observations = {
            (identity, "native"): [20.0, 20.0, 20.0],
            (identity, "native-overlay"): [15.0, 15.0, 15.0],
            (identity, "juicefs"): [10.0, 10.0, 10.0],
        }

        report = render_table(campaign, observations)

        self.assertIn("elapsed: lower is better", report)
        self.assertIn("0.5x (n=3/3, n=3/3)", report)
        self.assertIn("0.666667x (n=3/3, n=3/3)", report)

    def test_generator_reads_ledger_with_counts_and_rejects_duplicate_repetitions(self):
        campaign = load_campaign(CAMPAIGN_PATH)
        identity = ("metadata-throughput-profile", "NA", "dirperf", "seconds")
        with tempfile.TemporaryDirectory() as temp_dir:
            ledger = Path(temp_dir) / "observed-metrics.tsv"
            with ledger.open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=LEDGER_FIELDS, delimiter="\t")
                writer.writeheader()
                writer.writerow(observation(identity, "native", 1, 2.0))
            text = generate(ledger, CAMPAIGN_PATH)
            self.assertIn("not-measured (n=1/3)", text)

            with ledger.open("a", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=LEDGER_FIELDS, delimiter="\t")
                writer.writerow(observation(identity, "native", 1, 3.0))
            with self.assertRaisesRegex(ValueError, "duplicates repetition"):
                campaign_rows = load_observations(ledger, campaign)
                render_table(campaign, campaign_rows)


if __name__ == "__main__":
    unittest.main()
