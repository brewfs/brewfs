import ast
import pathlib
import re
import subprocess
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
RUNNER = ROOT / "tools/perf/run_packed_local.sh"


class PackedLocalRunnerTests(unittest.TestCase):
    def test_shell_and_embedded_python_syntax(self):
        subprocess.run(["bash", "-n", str(RUNNER)], check=True)
        script = RUNNER.read_text()
        bodies = re.findall(r"<<'(PY\w+)'\n(.*?)\n\1", script, re.S)
        self.assertGreaterEqual(len(bodies), 4)
        for label, body in bodies:
            with self.subTest(label=label):
                ast.parse(body)

    def test_timing_handles_multiple_epochs_and_validates_errors(self):
        script = RUNNER.read_text()
        body = re.search(r"<<'PYTIMING'\n(.*?)\nPYTIMING", script, re.S).group(1)
        import json
        import tempfile
        from unittest.mock import patch
        rows = [dict(files=100, expected_files=100, errors=0, payload_bytes=1048576)] * 2
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory)
            (path / "summary.json").write_text(json.dumps(dict(epochs=rows)))
            with patch("sys.argv", ["timing", directory, "0", "1000000000", "3000000000", "4000000000"]):
                exec(compile(body, "PYTIMING", "exec"), {})
            result = json.loads((path / "timing.json").read_text())
            self.assertEqual(result["active_bw_mib_s"], 1)
            self.assertAlmostEqual(result["effective_active_plus_drain_bw_mib_s"], 2 / 3)
            self.assertEqual(result["effective_wall_bw_mib_s"], 0.5)
            rows[0] = dict(rows[0], errors=1)
            (path / "summary.json").write_text(json.dumps(dict(epochs=rows)))
            with patch("sys.argv", ["timing", directory, "0", "1000000000", "3000000000", "4000000000"]):
                with self.assertRaises(AssertionError):
                    exec(compile(body, "PYTIMING", "exec"), {})

    def test_artifact_manifest_binds_seed_and_final_measurement_metadata(self):
        script = RUNNER.read_text()
        self.assertIn('MANIFEST_ARGS=(init', script)
        self.assertIn('packed_run_manifest.py" finalize', script)
        self.assertIn("--scanner-seed \"$SCANNER_SEED\"", script)
        self.assertIn("fixture_prefix=", script)
        self.assertIn("manifest_schema=packed-v3-run-manifest-v1", script)
        self.assertIn('"$ARTIFACT/toolchain.json"', script)
        self.assertIn('rustc", "-Vv"', script)
        self.assertIn('"git", "-C", str(root), "diff", "HEAD", "--binary"', script)
        self.assertIn('"ls-files", "--others", "--exclude-standard", "-z"', script)
        self.assertIn('"dirty_diff_sha256"', script)

    def test_resource_journal_is_initialized_and_finalized_after_cleanup(self):
        script = RUNNER.read_text()
        self.assertIn("packed_resource_journal.py\" init", script)
        self.assertIn("packed_resource_journal.py\" finalize", script)
        self.assertIn("--name compose_started", script)
        self.assertIn("--name mount_ready", script)
        self.assertIn("--name mount_unmounted", script)
        self.assertIn("--name compose_stopped", script)
        self.assertIn("--name work_removed", script)
        self.assertIn('"$ARTIFACT/resource-journal.json"', script)

    def test_layout_controls_are_forwarded_and_recorded(self):
        script = RUNNER.read_text()
        for control in ("FRAME_POLICY", "INLINE_DATA", "METADATA_CODEC", "DATA_CODEC", "ACCESS_PROFILE"):
            with self.subTest(control=control):
                self.assertIn(f"PACKED_LOCAL_{control}", script)
        self.assertIn('--frame-policy "$FRAME_POLICY"', script)
        self.assertIn('--inline-data "$INLINE_DATA"', script)
        self.assertIn('--metadata-codec "$METADATA_CODEC"', script)
        self.assertIn('--data-codec "$DATA_CODEC"', script)
        self.assertIn('--access-profile "$ACCESS_PROFILE"', script)
        self.assertIn("frame_policy=%s", script)

    def test_p90_training_requires_a_frozen_policy_and_binds_it_to_fixture(self):
        script = RUNNER.read_text()
        self.assertIn("PACKED_LOCAL_P90_TRAINING_TRACE", script)
        self.assertIn("packed_p90_policy.py\"", script)
        self.assertIn("--p90-training-trace-sha256", script)
        self.assertIn("--p90-policy \"$ARTIFACT/p90-policy.json\"", script)
        self.assertIn("p90_training_trace_sha256=%s", script)

    def test_conflicting_services_and_failed_cold_setup_stay_fatal(self):
        # Refuse an existing fixed-name Compose service instead of deleting it.
        script = RUNNER.read_text()
        self.assertIn("refusing concurrent test", script)
        self.assertIn("sudo -n sh -c 'echo 3 > /proc/sys/vm/drop_caches'", script)
        self.assertIn("set -Eeuo pipefail", script)
        self.assertIn("BREWFS_FUSE_KEEP_CACHE=0", script)
        self.assertIn("BREWFS_PACKED_DECODED_FRAME_CACHE_BYTES=0", script)
        self.assertNotIn("umount -l", script)


if __name__ == "__main__":
    unittest.main()
