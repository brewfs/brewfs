#!/usr/bin/env python3
"""Exercise production importer SQL with real SQLite plans and VM work.

No Rust compilation, object upload, FUSE, or live-spool mutation. Extract the
actual SQL from the selected source tree rather than maintain duplicate query
implementations. VM steps measure prefix work, not benchmark wall time.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sqlite3
import unittest


PREFIX = {
    "dentry": "SELECT p.path,p.parent,p.name,i.hot,i.cold,i.token,i.blocks,i.snapshot_inode,i.visible_links,parent.snapshot_inode",
    "fence": "SELECT p.path,i.token,i.source_id,i.snapshot_inode FROM source_paths",
    "assign": "SELECT source_id,master_path,kind,source_nlink,visible_links FROM source_inodes",
    "runs": "SELECT start,end FROM source_runs",
    "migration": "SELECT r.first_key,r.value FROM records r JOIN inode_identities",
    "placements": "SELECT p.first_key,p.last_key,p.value,i.value FROM records p JOIN records",
}
FILES = [
    "src/workspace/packed/snapshot/source_namespace.rs",
    "src/workspace/packed/snapshot/source_layout.rs",
    "src/workspace/packed/snapshot/spool.rs",
]
OBSERVATIONS: list[dict] = []
SOURCE_ROOT = Path(__file__).resolve().parents[2]


def production_queries(root: Path) -> dict[str, list[str]]:
    literals = []
    for filename in FILES:
        literals.extend(re.findall(r'"(SELECT [^"\n]+)"', (root / filename).read_text()))
    result = {}
    for name, prefix in PREFIX.items():
        found = [sql for sql in literals if sql.startswith(prefix)]
        if not found:
            raise AssertionError(f"missing production query: {name}")
        result[name] = list(dict.fromkeys(found))
    return result


def select_query(queries: list[str], continued: bool) -> str:
    nullable = [sql for sql in queries if "? IS NULL" in sql]
    if nullable:
        assert len(nullable) == 1
        return nullable[0]
    candidates = [sql for sql in queries if (">?" in sql or ">(?,?)" in sql) == continued]
    assert len(candidates) == 1, queries
    return candidates[0]


def parameters(name: str, sql: str, cursor):
    nullable = "? IS NULL" in sql
    if name == "dentry":
        if nullable:
            return (None, None, None, None) if cursor is None else (*([cursor[0]] * 3), cursor[1])
        return () if cursor is None else cursor
    prefix = (1,) if name == "migration" else (1, 6) if name == "placements" else ()
    if nullable:
        return (*prefix, cursor, cursor)
    return prefix if cursor is None else (*prefix, cursor)


def run_counted(connection: sqlite3.Connection, sql: str, params):
    steps = 0

    def count():
        nonlocal steps
        steps += 1
        return 0

    connection.set_progress_handler(count, 1)
    try:
        rows = connection.execute(sql, params).fetchall()
    finally:
        connection.set_progress_handler(None, 0)
    return rows, steps


def plan(connection, sql, params):
    return [row[3] for row in connection.execute("EXPLAIN QUERY PLAN " + sql, params)]


def fixture(size: int) -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.executescript("""
        PRAGMA cache_size=-2048;
        PRAGMA temp_store=FILE;
        CREATE TABLE source_inodes (source_id BLOB PRIMARY KEY, hot BLOB NOT NULL,
            cold BLOB NOT NULL, token BLOB NOT NULL, kind INTEGER NOT NULL,
            source_nlink BLOB NOT NULL, blocks BLOB NOT NULL, master_path BLOB,
            visible_links INTEGER, snapshot_inode INTEGER) WITHOUT ROWID;
        CREATE TABLE source_paths (path BLOB PRIMARY KEY, parent BLOB, name BLOB NOT NULL,
            source_id BLOB NOT NULL, kind INTEGER NOT NULL, scanned INTEGER NOT NULL DEFAULT 0)
            WITHOUT ROWID;
        CREATE INDEX source_path_identity ON source_paths(source_id,path);
        CREATE INDEX source_path_parent ON source_paths(parent,name);
        CREATE INDEX source_directory_work ON source_paths(kind,scanned,path);
        CREATE UNIQUE INDEX source_snapshot_inode ON source_inodes(snapshot_inode);
        CREATE UNIQUE INDEX source_master_path ON source_inodes(master_path);
        CREATE TABLE source_runs (start BLOB PRIMARY KEY,end BLOB NOT NULL) WITHOUT ROWID;
        CREATE TABLE records (root INTEGER NOT NULL,first_key BLOB NOT NULL,last_key BLOB NOT NULL,
            value BLOB NOT NULL,PRIMARY KEY(root,first_key)) WITHOUT ROWID;
        CREATE TABLE inode_identities (inode BLOB PRIMARY KEY,signature BLOB NOT NULL,
            kind INTEGER NOT NULL,expected_links INTEGER NOT NULL,observed_links INTEGER NOT NULL)
            WITHOUT ROWID;
    """)

    def insert_source(path, parent, name, identity, kind, inode, cold=b"cold", visible=1):
        connection.execute("INSERT OR IGNORE INTO source_inodes VALUES(?,?,?,?,?,?,?,?,?,?)",
            (identity, b"hot", cold, b"token", kind, visible.to_bytes(8, "little"),
             (8).to_bytes(8, "little"), path, visible, inode))
        connection.execute("INSERT INTO source_paths VALUES(?,?,?,?,?,0)",
            (path, parent, name, identity, kind))

    insert_source(b"", None, b"", b"root", 2, 1)
    insert_source(b"d", b"", b"d", b"directory", 2, 2)
    insert_source(b"\xff-dir", b"", b"\xff-dir", b"raw-directory", 2, 3)
    for i in range(size):
        name = b"n" + i.to_bytes(4, "big").hex().encode()
        parent = b"" if i % 3 == 0 else b"d" if i % 3 == 1 else b"\xff-dir"
        path = (parent + b"/" if parent else b"") + name
        key = (i + 10).to_bytes(8, "big")
        insert_source(path, parent, name, key, 1, i + 10,
            cold=b"c" * 200_000 if i == size - 1 else b"cold")
        for root in (1, 6):
            connection.execute("INSERT INTO records VALUES(?,?,?,?)", (root, key, key, b"value"))
        connection.execute("INSERT INTO inode_identities VALUES(?,?,?,?,?)", (key, b"signature", 1, 1, 1))
        start = (i * 8192).to_bytes(8, "big")
        connection.execute("INSERT INTO source_runs VALUES(?,?)", (start, (i * 8192 + 4096).to_bytes(8, "big")))
    # Raw-byte alias to one existing inode, across a parent boundary. Its hot,
    # cold, snapshot inode and visible links must remain one source identity.
    identity = (10).to_bytes(8, "big")
    connection.execute("UPDATE source_inodes SET visible_links=2,source_nlink=? WHERE source_id=?",
        ((2).to_bytes(8, "little"), identity))
    connection.execute("INSERT INTO source_paths VALUES(?,?,?,?,?,0)",
        (b"d/\x80-alias", b"d", b"\x80-alias", identity, 1))
    connection.commit()
    return connection


def tail_cursor(connection, name):
    sql = {
        "dentry": "SELECT parent,name FROM source_paths WHERE parent IS NOT NULL ORDER BY parent DESC,name DESC LIMIT 1 OFFSET 1",
        "fence": "SELECT path FROM source_paths ORDER BY path DESC LIMIT 1 OFFSET 1",
        "assign": "SELECT master_path FROM source_inodes ORDER BY master_path DESC LIMIT 1 OFFSET 1",
        "runs": "SELECT start FROM source_runs ORDER BY start DESC LIMIT 1 OFFSET 1",
        "migration": "SELECT r.first_key FROM records r JOIN inode_identities i ON i.inode=r.first_key WHERE r.root=1 AND i.kind=1 ORDER BY r.first_key DESC LIMIT 1 OFFSET 1",
        "placements": "SELECT first_key FROM records WHERE root=6 ORDER BY first_key DESC LIMIT 1 OFFSET 1",
    }[name]
    row = connection.execute(sql).fetchone()
    if row is None:
        return None
    return row if name == "dentry" else row[0]


class ImportSeekTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.queries = production_queries(SOURCE_ROOT)

    def test_late_page_and_eof_vm_work_does_not_scan_the_existing_prefix(self):
        # Two prefix sizes exercise all six real production continuations.
        # LIMIT 1/128 outputs near the tail have constant bounded work; a
        # nullable predicate that scans 4096 preceding keys fails this gate.
        for size in (1024, 4096):
            connection = fixture(size)
            try:
                for name, queries in self.queries.items():
                    with self.subTest(size=size, query=name):
                        cursor = tail_cursor(connection, name)
                        sql = select_query(queries, True)
                        rows, steps = run_counted(connection, sql, parameters(name, sql, cursor))
                        self.assertEqual(len(rows), 1)
                        if name == "dentry":
                            next_cursor = (rows[0][1], rows[0][2])
                        elif name == "fence":
                            # The production fence query returns identity and
                            # snapshot columns after the path; continuation
                            # is keyed by the first (path) column.
                            next_cursor = rows[0][0]
                        elif name == "assign":
                            next_cursor = rows[0][1]
                        else:
                            next_cursor = rows[0][0]
                        eof, eof_steps = run_counted(connection, sql, parameters(name, sql, next_cursor))
                        OBSERVATIONS.append({"rows": size, "query": name, "tail_vm_steps": steps,
                            "eof_vm_steps": eof_steps, "plan": plan(connection, sql, parameters(name, sql, cursor))})
                        self.assertEqual(eof, [])
                        self.assertLess(steps, 500, f"{name} scans the prefix: {steps} VM steps")
                        self.assertLess(eof_steps, 500, f"{name} rescans at EOF: {eof_steps} VM steps")
            finally:
                connection.close()

    def test_dentry_walk_preserves_raw_order_hardlinks_parent_and_single_cold_row(self):
        connection = fixture(96)
        try:
            expected = sorted(connection.execute("SELECT path,parent,name,source_id FROM source_paths WHERE parent IS NOT NULL"), key=lambda row: (row[1], row[2]))
            cursor = None
            seen = []
            peak_cold = 0
            aliases = []
            for _ in range(len(expected) + 1):
                sql = select_query(self.queries["dentry"], cursor is not None)
                rows = connection.execute(sql, parameters("dentry", sql, cursor)).fetchall()
                self.assertLessEqual(len(rows), 1, "large cold records must not be batched")
                if not rows:
                    break
                row = rows[0]
                seen.append((row[0], row[1], row[2]))
                cursor = (row[1], row[2])
                peak_cold = max(peak_cold, len(row[4]))
                if row[7] == 10:
                    aliases.append((row[7], row[8], row[9]))
            self.assertEqual(seen, [(row[0], row[1], row[2]) for row in expected])
            self.assertEqual(peak_cold, 200_000)
            self.assertEqual(sorted(aliases), [(10, 2, 1), (10, 2, 2)])
        finally:
            connection.close()


def actual_diagnostics(path, queries):
    connection = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    try:
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        kinds = ("dentry", "fence", "assign") if "source_paths" in tables else ("migration", "placements")
        result = {"path": str(path), "tables": {}, "queries": {}}
        for table in tables:
            result["tables"][table] = connection.execute('SELECT COUNT(*) FROM "' + table + '"').fetchone()[0]
        for name in kinds:
            cursor = tail_cursor(connection, name)
            if cursor is None:
                result["queries"][name] = {"status": "no applicable rows"}
                continue
            sql = select_query(queries[name], True)
            params = parameters(name, sql, cursor)
            rows, steps = run_counted(connection, sql, params)
            result["queries"][name] = {"output_rows": len(rows), "vm_steps": steps,
                "plan": plan(connection, sql, params)}
        return result
    finally:
        connection.close()


def main():
    global SOURCE_ROOT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=SOURCE_ROOT)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--copied-spool", type=Path, action="append", default=[])
    args = parser.parse_args()
    SOURCE_ROOT = args.source_root.resolve()
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ImportSeekTests)
    tested = unittest.TextTestRunner(verbosity=2).run(suite)
    queries = production_queries(SOURCE_ROOT)
    report = {"sqlite_version": sqlite3.sqlite_version, "source_root": str(SOURCE_ROOT),
        "passed": tested.wasSuccessful(), "tests": tested.testsRun, "observations": OBSERVATIONS,
        "copied_spools": [actual_diagnostics(path, queries) for path in args.copied_spool]}
    if args.output:
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    return 0 if tested.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
