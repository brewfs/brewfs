#!/usr/bin/env python3
"""Control-flow tests for supplemental collection; no filesystem mount needed."""

from contextlib import redirect_stderr
import io
import subprocess
import unittest
from unittest.mock import patch

import fuse_read_cache as benchmark


class NoCompilerWatch:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def report(self):
        return {"compiler_processes": [], "monitor_errors": []}


class CollectionTests(unittest.TestCase):
    def setUp(self):
        self.watcher = patch.object(benchmark, "CompilerWatch", NoCompilerWatch)
        self.watcher.start()
        self.addCleanup(self.watcher.stop)
        self.successes = []
        self.failures = []
        self.attempts = []

    def operation(self, name, error=None):
        def run():
            self.attempts.append(name)
            if error is not None:
                raise error
            return {"operation": name, "MiBps": 123.0}

        return name, run

    def run_batch(self, operations, collect=False):
        return benchmark.run_operation_batch(
            operations,
            lambda name, row: self.successes.append((name, row)),
            self.failures.append,
            collect,
        )

    def test_collects_all_repetitions_but_preserves_nonzero_exit(self):
        names = [
            "seq_read_cold", "seq_read_warm", "fio_seqread_direct0",
            "fio_seqread_direct1", "fio_randrw",
        ]
        failed = 0
        for _repetition in range(3):
            operations = [self.operation(name) for name in names[:-1]]
            operations.append(
                self.operation("fio_randrw", subprocess.CalledProcessError(1, ["fio"]))
            )
            failed += self.run_batch(operations, collect=True)
        self.assertEqual(self.attempts, names * 3)
        self.assertEqual(len(self.successes), 12)
        self.assertEqual(len(self.failures), 3)
        self.assertEqual(failed, 3)
        self.assertTrue(all(row["status"] == "rejected" for row in self.failures))
        self.assertTrue(all(row["unaccepted_result"] is None for row in self.failures))
        self.assertNotIn("fio_randrw", [name for name, _row in self.successes])
        with redirect_stderr(io.StringIO()) as messages:
            with self.assertRaises(SystemExit) as raised:
                benchmark.fail_if_guards_failed(failed)
        self.assertEqual(raised.exception.code, 1)
        self.assertIn("3 mixed-I/O guard(s)", messages.getvalue())

    def test_default_still_stops_at_first_failed_guard(self):
        operations = [
            self.operation("fio_randrw", subprocess.CalledProcessError(1, ["fio"])),
            self.operation("seq_read_cold"),
        ]
        with self.assertRaises(subprocess.CalledProcessError):
            self.run_batch(operations)
        self.assertEqual(self.attempts, ["fio_randrw"])
        self.assertEqual(len(self.failures), 1)

    def test_read_failure_always_aborts(self):
        operations = [
            self.operation("fio_seqread_direct1", subprocess.CalledProcessError(1, ["fio"])),
            self.operation("fio_randrw"),
        ]
        with self.assertRaises(subprocess.CalledProcessError):
            self.run_batch(operations, collect=True)
        self.assertEqual(self.attempts, ["fio_seqread_direct1"])

    def test_compiler_or_monitor_failure_always_aborts(self):
        for reason in ("compiler overlap", "monitor failed"):
            with self.subTest(reason=reason):
                operations = [
                    self.operation("fio_randrw", benchmark.CompilerOverlap(reason)),
                    self.operation("seq_read_cold"),
                ]
                with self.assertRaises(benchmark.CompilerOverlap):
                    self.run_batch(operations, collect=True)
        self.assertNotIn("seq_read_cold", self.attempts)

    def test_monitor_failure_overrides_collectible_guard_exit(self):
        class FailedWatch(NoCompilerWatch):
            def __exit__(self, *_args):
                raise benchmark.CompilerOverlap("monitor failed")

        with patch.object(benchmark, "CompilerWatch", FailedWatch):
            operations = [
                self.operation("fio_randrw", subprocess.CalledProcessError(1, ["fio"])),
                self.operation("seq_read_cold"),
            ]
            with self.assertRaises(benchmark.CompilerOverlap):
                self.run_batch(operations, collect=True)
        self.assertEqual(self.attempts, ["fio_randrw"])

    def test_non_fio_failure_in_guard_is_fatal(self):
        operations = [
            self.operation("fio_randrw", subprocess.CalledProcessError(1, ["mountpoint"])),
            self.operation("seq_read_cold"),
        ]
        with self.assertRaises(subprocess.CalledProcessError):
            self.run_batch(operations, collect=True)
        self.assertEqual(self.attempts, ["fio_randrw"])

    def test_successful_batch_has_no_failed_exit(self):
        failed = self.run_batch([self.operation("fio_randrw")], collect=True)
        self.assertEqual(failed, 0)
        self.assertEqual(self.failures, [])
        self.assertIsNone(benchmark.fail_if_guards_failed(failed))


if __name__ == "__main__":
    unittest.main()
