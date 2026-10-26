"""Regression coverage for the mounted diagnostic's evidence, without FUSE."""
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import workspace_metadata_cache as runner


class ReadEvidenceTests(unittest.TestCase):
    def test_trace_keeps_syscalls_eof_and_fuse_sizes(self):
        maps = {
            "@requests": {"3": 257, "15": 2048},
            "@read_sizes": {"131072": 2048},
            "@read_requested": {"1048576": 257},
            "@read_returned": {"1048576": 256, "0": 1},
            "@open_flags": {"0": 1},
        }
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "trace.jsonl"
            output.write_text(json.dumps({"type": "map", "data": maps}) + "\n")
            trace = runner.RequestCounts(Path(directory), output)
            trace.proc = Mock()
            trace.proc.wait.return_value = 0
            trace.log = io.StringIO()
            with patch.object(trace, "stop"), patch.object(runner, "checked_mount"):
                trace.__exit__()
            self.assertEqual(trace.counts, maps["@requests"])
            self.assertEqual(trace.maps, maps)

    def test_failed_tracer_cannot_be_reported_as_zero_fuse_requests(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "trace.jsonl"
            output.write_text("tracer failed\n")
            trace = runner.RequestCounts(Path(directory), output)
            trace.proc = Mock()
            trace.proc.wait.return_value = 1
            trace.log = io.StringIO()
            with patch.object(trace, "stop"), patch.object(runner, "checked_mount"):
                with self.assertRaisesRegex(RuntimeError, "BPF"):
                    trace.__exit__()

    def test_verify_rejects_same_length_corruption_after_multiple_blocks(self):
        data = bytes(range(256)) * (3 * 1024 * 1024 // 256)
        expected = hashlib.sha256(data).hexdigest()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "corpus"
            path.write_bytes(data)
            runner.verify_contents(path, expected, len(data))
            with path.open("r+b") as file:
                file.seek(2 * 1024 * 1024 + 17)
                file.write(b"X")
            with self.assertRaisesRegex(AssertionError, "content"):
                runner.verify_contents(path, expected, len(data))


if __name__ == "__main__":
    unittest.main()
