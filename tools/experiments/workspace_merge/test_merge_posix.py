import os
from pathlib import Path
import shutil
import tempfile
import unittest

from merge_posix import merge


@unittest.skipUnless(os.name == "posix", "POSIX filesystem semantics required")
class MergeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.base, self.child, self.target = [self.root / name for name in ("base", "child", "target")]
        self.base.mkdir()
        (self.base / "directory").mkdir()
        (self.base / "file").write_bytes(b"base")
        for path in (self.child, self.target):
            shutil.copytree(self.base, path)

    def run_merge(self):
        return merge(self.base, self.child, self.target)

    def test_disjoint_changes_and_repeat(self):
        (self.child / "file").write_bytes(b"child")
        (self.target / "parent").write_bytes(b"parent")
        self.assertFalse(self.run_merge()["conflicts"])
        self.assertEqual((self.target / "file").read_bytes(), b"child")
        self.assertEqual((self.target / "parent").read_bytes(), b"parent")
        self.assertEqual(self.run_merge()["planned_edits"], 0)

    def test_file_conflict_prevents_other_changes(self):
        (self.child / "file").write_bytes(b"child")
        (self.target / "file").write_bytes(b"target")
        (self.child / "new").write_bytes(b"new")
        self.assertEqual(self.run_merge()["conflicts"], ["file"])
        self.assertFalse((self.target / "new").exists())
        self.assertEqual((self.target / "file").read_bytes(), b"target")

    def test_deleted_directory_conflicts_with_new_descendant(self):
        (self.child / "directory").rmdir()
        (self.target / "directory" / "new").write_bytes(b"target")
        self.assertEqual(self.run_merge()["conflicts"], ["directory"])
        self.assertEqual((self.target / "directory" / "new").read_bytes(), b"target")

    def test_rename_mode_symlink_and_sparse(self):
        (self.child / "file").rename(self.child / "renamed")
        (self.child / "renamed").chmod(0o700)
        (self.child / "link").symlink_to("renamed")
        with (self.child / "sparse").open("wb") as stream:
            stream.seek(8 * 1024 * 1024)
            stream.write(b"tail")
        self.assertFalse(self.run_merge()["conflicts"])
        self.assertFalse((self.target / "file").exists())
        self.assertEqual((self.target / "link").read_bytes(), b"base")
        self.assertEqual((self.target / "renamed").stat().st_mode & 0o777, 0o700)
        self.assertEqual((self.target / "sparse").read_bytes(), (self.child / "sparse").read_bytes())
        self.assertLess((self.target / "sparse").stat().st_blocks, 100)

    def test_hardlink_rejected_before_mutation(self):
        os.link(self.child / "file", self.child / "alias")
        with self.assertRaisesRegex(ValueError, "hard links"):
            self.run_merge()
        self.assertFalse((self.target / "alias").exists())


if __name__ == "__main__":
    unittest.main()
