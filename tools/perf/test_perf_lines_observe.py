"""Evidence validity regressions for the WS-0 observation report."""

import tempfile
import unittest
import json
from unittest.mock import patch
from pathlib import Path

import perf_lines_observe as observe


class ObservationEvidenceTests(unittest.TestCase):
    def test_compose_matrix_keeps_named_profiles_and_strict_writeback_defaults(self):
        for matrix in observe.MATRICES:
            env = observe.matrix_environment(matrix, "bobs-off", 128)
            self.assertFalse(any(key.endswith("_ARGS") for key in env))
            self.assertEqual(env["BREWFS_WRITEBACK_MODE"], "commit_before_upload")
            self.assertEqual(env["BREWFS_REQUEST_TRACE"], "false")
            for key in ("BREWFS_WRITEBACK_PERSIST_SYNC", "BREWFS_WRITEBACK_REQUIRE_STAGE_BEFORE_COMMIT",
                        "PERF_FIO_PREFILL_DRAIN", "PERF_FIO_PREFILL_REMOUNT"):
                self.assertEqual(env[key], "true")
            self.assertEqual(env["PERF_FIO_DIRECT"], "0")
        broad = observe.matrix_environment("p-broad", "bobs-on", 32)
        self.assertEqual(broad["PERF_FIO_BIGREAD_REPEATS"], "3")
        self.assertEqual(broad["PERF_FIO_BIGWRITE_SIZE"], "1g")
        self.assertEqual(broad["PERF_FIO_BIGWRITE_NUMJOBS"], "8")
        self.assertEqual(observe.matrix_environment("p-s", "bobs-off", 32)["PERF_FIO_BS"], "4k")

    def test_wait_breakdown_excludes_prefill_requests(self):
        with tempfile.TemporaryDirectory() as directory:
            artifact = Path(directory)
            (artifact / "phase-boundaries.tsv").write_text("fio-randrw\ttool_start\t100\nfio-randrw\ttool_return\t200\n")
            records = [{"opcode": 16, "received_at_ns": at, "total_ns": 100} for at in (99, 100, 199, 200)]
            windows = observe.measurement_windows(artifact, [{"records": records}])["windows"]
            self.assertEqual(windows[0]["request_count"], 2)
            self.assertEqual(windows[0]["write_wait_breakdown"]["ordinary"]["count"], 1)

    def test_completion_syscalls_exclude_loader_closes_and_keep_exact_time(self):
        with tempfile.TemporaryDirectory() as directory:
            artifact = Path(directory)
            (artifact / "syscall-tail").mkdir()
            path = artifact / "syscall-tail/fio-randrw.123"
            path.write_text("1793400269.873800 close(3</etc/ld.so.cache>) = 0 <0.000010>\n"
                            "1793400270.000001 fsync(4</mnt/brewfs/.perf-fio-randrw/job.0>) = 0 <0.010000>\n"
                            "1793400270.020001 close(4</mnt/brewfs/.perf-fio-randrw/job.0>) = 0 <0.030000>\n")
            row = observe.syscall_completion(artifact)["tools"]["fio-randrw"]
            self.assertEqual(row["status"], "observed")
            self.assertEqual(row["other_fd_calls"], 1)
            self.assertEqual(row["operations"]["close"]["n"], 1)
            self.assertEqual(row["records"][0]["start_ns"], 1793400270000001000)
            self.assertEqual(row["completion_call_window_ns"], 50_000_000)
            self.assertFalse(row["operations"]["close"]["p99_reliable"])
            path.write_text("1793400270.020001 close(4) = 0 <0.030000>\n")
            self.assertEqual(observe.syscall_completion(artifact)["tools"]["fio-randrw"]["status"], "unavailable")

    def fixture(self, directory, enabled=0, closed=0, retained=0):
        artifact = Path(directory)
        diagnostics = artifact / "diagnostics"
        diagnostics.mkdir(exist_ok=True)
        (artifact / "manifest.json").write_text('{"source_revision": "test"}')
        values = dict.fromkeys(observe.REQUIRED, 0)
        values.update({
            "brewfs_request_observation_enabled": enabled,
            "brewfs_request_observation_sample_rate": 1,
            "brewfs_request_observation_received_total": closed,
            "brewfs_request_observation_sampled_total": closed,
            "brewfs_request_observation_closed_total": closed,
            "brewfs_request_trace_records_dropped_total": closed - retained,
        })
        for name in observe.SEGMENTS:
            values[f"brewfs_request_{name}_ns_count"] = 0
            values[f"brewfs_request_{name}_ns_sum"] = 0
        for name in observe.OPERATIONS:
            values[f"brewfs_request_operation_{name}_ns_count"] = 0
            values[f"brewfs_request_operation_{name}_ns_sum"] = 0
        for name in observe.QUEUE_SEGMENTS:
            values[f"brewfs_request_{name}_queue_depth"] = 0
            values[f"brewfs_request_{name}_queue_oldest_age_ns"] = 0
        lines = [f"{key} {value}" for key, value in values.items()]
        for unique in range(retained):
            labels = f'unique="{unique}",opcode="16",terminal="reply"'
            lines.append(f"brewfs_request_trace_record{{{labels}}} 100")
            for segment in observe.SEGMENTS:
                duration = 100 if segment in ("handler", "flush_wait", "object_put") else 0
                lines.append(f'brewfs_request_trace_segment_ns{{{labels},segment="{segment}"}} {duration}')
        path = diagnostics / "stats-write-after.txt"
        path.write_text("\n".join(lines) + "\n")
        return artifact, path

    def test_unreadable_snapshot_keeps_parser_shape(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(len(observe.parse_stats(Path(directory) / "missing")), 3)

    def test_disabled_run_is_not_validated_trace(self):
        with tempfile.TemporaryDirectory() as directory:
            artifact, _ = self.fixture(directory)
            self.assertEqual(observe.summarize(artifact)["status"], "disabled")

    def test_missing_manifest_is_unavailable(self):
        with tempfile.TemporaryDirectory() as directory:
            artifact, _ = self.fixture(directory)
            (artifact / "manifest.json").unlink()
            self.assertEqual(observe.summarize(artifact)["status"], "unavailable")

    def test_lifecycle_counts_do_not_hide_lost_trace_records(self):
        with tempfile.TemporaryDirectory() as directory:
            _, path = self.fixture(directory, enabled=1, closed=1000, retained=1)
            self.assertEqual(observe.one_stats(path)["status"], "invalid")

    def test_inclusive_background_durations_are_not_wait_shares(self):
        with tempfile.TemporaryDirectory() as directory:
            _, path = self.fixture(directory, enabled=1, closed=1, retained=1)
            groups = observe.one_stats(path)["observation"]["write_wait_breakdown"]
            self.assertIsNone(groups["slowest_1pct"]["wait_share"])

    def test_failed_sampler_invalidates_observation_off_run(self):
        with tempfile.TemporaryDirectory() as directory:
            artifact, _ = self.fixture(directory)
            (artifact / "writeback-samples.invalid").touch()
            self.assertEqual(observe.summarize(artifact)["status"], "invalid")

    def trace_v2(self, unique=1):
        parts = dict.fromkeys(observe.SEGMENTS, 0)
        parts.update(fuse_queue_wait=10, file_lock_wait=60, dirty_merge=20, reply=10)
        return {"unique": unique, "opcode": 16, "terminal": "reply", "total_ns": 100,
                "segments": list(parts.values()), "exclusive_ns": list(parts.values()),
                "background_ns": [1000] * len(parts), "intervals_truncated": False, "ambiguous_ns": 0,
                "intervals": [[0, 10, 0], [10, 70, 1], [70, 90, 3], [90, 100, 9]]}

    def test_collection_recovers_evictions_and_separates_mounts(self):
        with tempfile.TemporaryDirectory() as directory:
            artifact, _ = self.fixture(directory)
            series = artifact / "stats-timeseries" / "fio-seqwrite"
            series.mkdir(parents=True)
            for index, (mount, unique, count) in enumerate((("mount-a", 1, 1), ("mount-a", 2, 2), ("mount-a", 2, 2), ("mount-b", 1, 1))):
                lines = [f"# brewfs_request_trace_instance {mount}",
                         "brewfs_request_observation_enabled 1",
                         f"brewfs_request_observation_sampled_total {count}",
                         f"brewfs_request_observation_closed_total {count}",
                         "brewfs_request_observation_dropped_total 0",
                         "# brewfs_request_trace_v2 " + json.dumps(self.trace_v2(unique))]
                (series / f"{index}.txt").write_text("\n".join(lines))
            collection = observe.collect_requests(artifact)
            self.assertEqual(collection["failures"], [])
            self.assertEqual(collection["status"], "valid")
            self.assertEqual([item["collected"] for item in collection["instances"]], [2, 1])
            breakdown = collection["instances"][0]["write_wait_breakdown"]
            self.assertEqual(breakdown["slowest_1pct"]["wait_share"], .8)
            self.assertFalse(breakdown["tail_reliable"])

    def test_partition_rejects_forged_sum_and_unknown_time(self):
        record = self.trace_v2()
        for key in ("segments", "exclusive_ns", "background_ns"):
            record[key] = dict(zip(observe.SEGMENTS, record[key]))
        self.assertEqual(observe.validate_partition(record), [])
        record["exclusive_ns"]["file_lock_wait"] = 900
        self.assertIn("partition_mismatch", observe.validate_partition(record))
        record["exclusive_ns"]["file_lock_wait"] = 60
        record["intervals"] = []
        self.assertIn("unattributed_over_5pct", observe.validate_partition(record))

    def test_client_probe_counts_independent_calls_and_exact_histograms(self):
        with tempfile.TemporaryDirectory() as directory:
            report = observe.client_io_probe(Path(directory), 3, 256)
        self.assertEqual(report["operations"]["fsync"]["n"], 3)
        self.assertEqual(report["operations"]["close_dirty"]["n"], 3)
        self.assertEqual(report["operations"]["close_after_fsync"]["n"], 3)
        self.assertEqual(report["operations"]["pwrite"]["n"], 768)
        for values in report["operations"].values():
            self.assertEqual(sum(count for _, count in values["histogram"]), values["n"])
            self.assertFalse(values["p99_reliable"])

    def test_backend_outcomes_are_cumulative_and_missing_is_not_zero(self):
        self.assertEqual(observe.rustfs_http_counters({}), {})
        def point(outcome, count):
            return {"attributes": [{"key": "op", "value": {"stringValue": "s3:PutObject"}},
                                   {"key": "outcome", "value": {"stringValue": outcome}}],
                    "asInt": str(count), "startTimeUnixNano": "123", "timeUnixNano": "456"}
        metric = {"name": "rustfs_s3_http_requests_total", "sum": {
            "aggregationTemporality": "AGGREGATION_TEMPORALITY_CUMULATIVE",
            "dataPoints": [point("2xx", 8), point("5xx", 2)]}}
        export = {"resourceMetrics": [{"scopeMetrics": [{"metrics": [metric]}]}]}
        put = observe.rustfs_http_counters(export)["s3:PutObject"]
        self.assertEqual((put["success"], put["completed_attempts"], put["observed_failed_attempts"]), (8, 10, 2))
        metric["sum"]["dataPoints"] = [point("5xx", 2)]
        self.assertIsNone(observe.rustfs_http_counters(export)["s3:PutObject"]["success"])
        metric["sum"]["aggregationTemporality"] = "AGGREGATION_TEMPORALITY_DELTA"
        with self.assertRaises(ValueError):
            observe.rustfs_http_counters(export)

    def test_backend_rejects_mixed_counter_epochs(self):
        points = [{"attributes": [{"key": "op", "value": {"stringValue": "s3:PutObject"}},
                                  {"key": "outcome", "value": {"stringValue": outcome}}],
                   "asInt": "1", "startTimeUnixNano": start, "timeUnixNano": "456"}
                  for outcome, start in (("2xx", "123"), ("5xx", "124"))]
        export = {"resourceMetrics": [{"scopeMetrics": [{"metrics": [{
            "name": "rustfs_s3_http_requests_total", "sum": {
                "aggregationTemporality": "AGGREGATION_TEMPORALITY_CUMULATIVE", "dataPoints": points}}]}]}]}
        with self.assertRaisesRegex(ValueError, "epoch"):
            observe.rustfs_http_counters(export)

    def test_backend_fresh_receipt_cannot_hide_stale_counter_sample(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "backend.json"
            data = {"received_at_ns": 10_000_000_000, "sequence": 1, "operations": {
                "s3:PutObject": {"start_time_ns": 1, "sample_time_ns": 1_000_000_000,
                                 "success": 1, "completed_attempts": 1}}}
            with patch("urllib.request.urlopen") as request, patch.object(observe.time, "time_ns", return_value=10_000_000_000):
                request.return_value.__enter__.return_value.read.return_value = json.dumps(data)
                with self.assertRaisesRegex(ValueError, "stale"):
                    observe.backend_snapshot(["--url", "http://collector/snapshot", "--output", str(path)])

    def test_object_ledger_uses_fio_bytes_and_includes_maintenance(self):
        with tempfile.TemporaryDirectory() as directory:
            artifact = Path(directory)
            (artifact / "results").mkdir()
            (artifact / "diagnostics").mkdir()
            (artifact / "results/fio-randrw.json").write_text(json.dumps({"jobs": [
                {"write": {"io_bytes": 2**30}, "read": {"io_bytes": 3 * 2**30}, "error": 0}]}))
            for phase, puts, uploaded in (("before", 100, 1000), ("post-write-drained-after", 110, 2000),
                                           ("maintenance-before", 110, 2000), ("maintenance-after", 113, 2300)):
                (artifact / f"diagnostics/backend-fio-randrw-{phase}.json").write_text(json.dumps({"operations": {
                    "s3:PutObject": {"start_time_ns": 1, "sample_time_ns": 100 + puts,
                                     "success": puts, "completed_attempts": puts + 2}}}))
                # Includes the mount identity, which must not change inside a window.
                (artifact / f"diagnostics/stats-fio-randrw-{phase}.txt").write_text(
                    f"# brewfs_request_trace_instance mount-a\nbrewfs_s3_put_bytes_total {uploaded}\nbrewfs_s3_put_ops_total {puts}\n")
            (artifact / "maintenance.tsv").write_text("tool\tstarted_ns\tended_ns\trequested_observation_s\nfio-randrw\t1\t60000000001\t60\n")
            report = observe.object_ledger(artifact)
            row = report["tools"]["fio-randrw"]
            self.assertEqual(row["total"]["put_success"], 13)
            self.assertEqual(row["total"]["put_per_gib_written"], 13)
            self.assertEqual(row["maintenance"]["put_success"], 3)
            self.assertEqual(row["total"]["average_put_bytes"], 100)
            (artifact / "diagnostics/backend-fio-randrw-maintenance-after.json").unlink()
            row = observe.object_ledger(artifact)["tools"]["fio-randrw"]
            self.assertEqual(row["status"], "unavailable")
            self.assertNotIn("total", row)

    def test_backend_phase_requires_counter_newer_than_phase_start(self):
        with tempfile.TemporaryDirectory() as directory:
            data = {"received_at_ns": 10_000_000_000, "sequence": 1, "operations": {
                "s3:PutObject": {"start_time_ns": 1, "sample_time_ns": 9_000_000_000,
                                 "success": 1, "completed_attempts": 1}}}
            with patch("urllib.request.urlopen") as request, patch.object(observe.time, "time_ns", return_value=10_000_000_000):
                request.return_value.__enter__.return_value.read.return_value = json.dumps(data)
                with self.assertRaisesRegex(ValueError, "precedes"):
                    observe.backend_snapshot(["--url", "http://collector/snapshot", "--output", str(Path(directory) / "backend.json"),
                                              "--after-ns", "10000000000"])

    def test_backend_waits_for_first_real_put_sample_without_synthesizing_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            empty = {"received_at_ns": 10_000_000_000, "operations": {}}
            ready = {"received_at_ns": 10_000_000_000, "operations": {"s3:PutObject": {
                "start_time_ns": 1, "sample_time_ns": 10_000_000_000, "success": 1, "completed_attempts": 1}}}
            with patch("urllib.request.urlopen") as request, patch.object(observe.time, "time_ns", return_value=10_000_000_000), \
                 patch.object(observe.time, "sleep"), patch("builtins.print"):
                request.return_value.__enter__.return_value.read.side_effect = [json.dumps(empty), json.dumps(ready)]
                self.assertEqual(observe.backend_snapshot(["--url", "http://collector/snapshot", "--output", str(Path(directory) / "backend.json"),
                                                          "--after-ns", "10000000000", "--wait-secs", "5"]), 0)


if __name__ == "__main__":
    unittest.main()
