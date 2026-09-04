import io
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

import server


def zip_bytes(entries):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, value in entries:
            archive.writestr(name, value)
    return output.getvalue()


class ResultServerTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.root_patch = mock.patch.object(server, "ROOT", self.root)
        self.root_patch.start()

    def tearDown(self):
        self.root_patch.stop()
        self.temporary.cleanup()

    def test_create_run_extracts_a_valid_archive(self):
        run = server.create_run(
            zip_bytes([("run/perf-summary.tsv", "tool\tstatus\tseconds\nfio-bigread\tpass\t2\n")]),
            "result.zip",
        )

        self.assertEqual(run["status"], "pass")
        self.assertEqual(run["metrics"][0]["tool"], "fio-bigread")
        self.assertTrue((self.root / run["id"] / "files" / "run" / "perf-summary.tsv").is_file())

    def test_create_run_rejects_parent_traversal_and_removes_partial_run(self):
        with self.assertRaisesRegex(ValueError, "unsafe archive path"):
            server.create_run(zip_bytes([("../outside.txt", "bad")]), "bad.zip")

        self.assertEqual(list(self.root.iterdir()), [])

    def test_create_run_rejects_extracted_size_over_limit(self):
        with mock.patch.object(server, "MAX_EXTRACTED", 3):
            with self.assertRaisesRegex(ValueError, "expands beyond"):
                server.create_run(zip_bytes([("large.txt", "four")]), "large.zip")

        self.assertEqual(list(self.root.iterdir()), [])

    def test_create_run_rejects_too_many_entries(self):
        with mock.patch.object(server, "MAX_FILES", 1):
            with self.assertRaisesRegex(ValueError, "more than 1 entries"):
                server.create_run(zip_bytes([("one.txt", "1"), ("two.txt", "2")]), "many.zip")

        self.assertEqual(list(self.root.iterdir()), [])

    def test_is_within_does_not_accept_a_sibling_with_the_same_prefix(self):
        static = self.root / "static"
        sibling = self.root / "static-private" / "secret.txt"

        self.assertFalse(server.is_within(sibling, static))


if __name__ == "__main__":
    unittest.main()
