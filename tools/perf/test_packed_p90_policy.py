import copy
import json
import pathlib
import tempfile
import unittest

try:
    from .packed_p90_policy import P90PolicyError, build_from_file, build_policy, validate_policy
    from .packed_run_manifest import ArtifactError, init_manifest
except ImportError:
    from packed_p90_policy import P90PolicyError, build_from_file, build_policy, validate_policy
    from packed_run_manifest import ArtifactError, init_manifest


class PackedP90PolicyTests(unittest.TestCase):
    def _trace(self):
        return {
            "trace_sha256": "a" * 64,
            "source": "training-rustfs-20261010",
            "captured_at_utc": "2026-10-10T01:02:03Z",
            "requested_ranges_bytes": [64, 256, 256, 1024, 4096, 4096, 4096, 8192, 16384, 32768],
        }

    def test_build_freezes_histogram_and_nearest_rank_p90(self):
        policy = build_policy(self._trace())
        self.assertEqual(policy["sample_count"], 10)
        self.assertEqual(policy["p90_bytes"], 16384)
        self.assertEqual(
            policy["histogram"],
            [
                {"range_bytes": 64, "count": 1},
                {"range_bytes": 256, "count": 2},
                {"range_bytes": 1024, "count": 1},
                {"range_bytes": 4096, "count": 3},
                {"range_bytes": 8192, "count": 1},
                {"range_bytes": 16384, "count": 1},
                {"range_bytes": 32768, "count": 1},
            ],
        )
        self.assertIs(validate_policy(policy, expected_trace_sha256="a" * 64), policy)

    def test_file_builder_binds_declared_trace_digest(self):
        trace = self._trace()
        trace["trace_sha256"] = "0" * 64
        payload = {key: value for key, value in trace.items() if key != "trace_sha256"}
        import hashlib

        trace["trace_sha256"] = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            trace_path = root / "training.json"
            policy_path = root / "p90-policy.json"
            trace_path.write_text(json.dumps(trace, sort_keys=True))
            policy = build_from_file(trace_path, policy_path)
            self.assertEqual(policy["trace_sha256"], trace["trace_sha256"])
            self.assertTrue(policy_path.is_file())

    def test_policy_rejects_tampered_payload(self):
        policy = build_policy(self._trace())
        policy["p90_bytes"] = 8192
        with self.assertRaisesRegex(P90PolicyError, "digest"):
            validate_policy(policy)

    def test_policy_rejects_malformed_histogram_without_leaking_key_errors(self):
        policy = build_policy(self._trace())
        policy["histogram"] = [1]
        with self.assertRaises(P90PolicyError):
            validate_policy(policy)

    def test_policy_rejects_trace_mismatch(self):
        policy = build_policy(self._trace())
        with self.assertRaisesRegex(P90PolicyError, "does not match"):
            validate_policy(policy, expected_trace_sha256="b" * 64)

    def test_policy_requires_source_and_collection_time(self):
        for field in ("source", "captured_at_utc"):
            trace = self._trace()
            del trace[field]
            with self.assertRaises(P90PolicyError):
                build_policy(trace)

    def test_manifest_requires_authenticated_training_trace(self):
        with tempfile.TemporaryDirectory() as directory:
            artifact = pathlib.Path(directory)
            with self.assertRaisesRegex(ArtifactError, "training trace"):
                init_manifest(
                    artifact,
                    run_id="p90",
                    wire_version=5,
                    controls={"mode": "stat", "scanner_seed": 1, "frame_policy": "p90-training"},
                    fixture_prefix="local-validation",
                )


if __name__ == "__main__":
    unittest.main()
