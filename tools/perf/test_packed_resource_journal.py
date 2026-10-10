import json
import pathlib
import tempfile
import unittest

try:
    from .packed_resource_journal import (
        ResourceJournalError,
        append_event,
        finalize_journal,
        init_journal,
        validate_journal,
    )
except ImportError:  # direct execution from tools/perf
    from packed_resource_journal import (
        ResourceJournalError,
        append_event,
        finalize_journal,
        init_journal,
        validate_journal,
    )


class PackedResourceJournalTests(unittest.TestCase):
    @staticmethod
    def _init(path: pathlib.Path) -> None:
        init_journal(
            path,
            run_id="run-1",
            artifact=str(path.parent),
            project="project-1",
            work="/tmp/work-1",
        )

    def test_success_requires_complete_cleanup_lifecycle(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "resource-journal.json"
            self._init(path)
            for name in (
                "compose_started",
                "mount_ready",
                "mount_unmounted",
                "compose_stopped",
                "work_removed",
            ):
                append_event(path, name)
            finalize_journal(
                path,
                status=0,
                mount_present=False,
                process_alive=False,
                compose_present=False,
                work_present=False,
            )
            value = validate_journal(path, expected_status=0)
            self.assertEqual(value["status"], "passed")
            self.assertEqual(value["events"][-1]["name"], "finalize")

    def test_success_rejects_live_resource_and_failed_run_records_reason(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "resource-journal.json"
            self._init(path)
            for name in (
                "compose_started",
                "mount_ready",
                "mount_unmounted",
                "compose_stopped",
                "work_removed",
            ):
                append_event(path, name)
            with self.assertRaises(ResourceJournalError):
                finalize_journal(
                    path,
                    status=0,
                    mount_present=False,
                    process_alive=False,
                    compose_present=False,
                    work_present=True,
                )
            # A failed run may terminate before every cleanup phase, but it
            # must still have an explicit terminal record.
            finalize_journal(
                path,
                status=143,
                mount_present=True,
                process_alive=False,
                compose_present=True,
                work_present=True,
            )
            self.assertEqual(validate_journal(path, expected_status=143)["status"], "failed")

    def test_tampered_terminal_status_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "resource-journal.json"
            self._init(path)
            finalize_journal(
                path,
                status=143,
                mount_present=False,
                process_alive=False,
                compose_present=False,
                work_present=False,
            )
            value = json.loads(path.read_text())
            value["exit_status"] = 0
            path.write_text(json.dumps(value))
            with self.assertRaises(ResourceJournalError):
                validate_journal(path, expected_status=143)

    def test_journal_identity_is_bound_to_manifest_owner(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            path = root / "resource-journal.json"
            self._init(path)
            finalize_journal(
                path,
                status=143,
                mount_present=False,
                process_alive=False,
                compose_present=False,
                work_present=False,
            )
            with self.assertRaises(ResourceJournalError):
                validate_journal(path, expected_run_id="other", expected_status=143)
            with self.assertRaises(ResourceJournalError):
                validate_journal(
                    path,
                    expected_artifact=str(root / "other"),
                    expected_status=143,
                )


if __name__ == "__main__":
    unittest.main()
