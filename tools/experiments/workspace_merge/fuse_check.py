"""Exercise merge through independent FUSE processes and a real Redis catalog."""
import argparse
import contextlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid


def command(args):
    result = subprocess.run(args, capture_output=True, text=True, timeout=60)
    if result.returncode:
        raise RuntimeError(f"command failed: {result.stderr}")
    return result.stdout


@contextlib.contextmanager
def mount(args, case, workspace):
    run = args.root / str(uuid.uuid4())
    run.mkdir()
    path = run / "mount"
    path.mkdir()
    config = run / "mount.json"
    config.write_text(json.dumps(dict(cache=dict(cache_root=str(run / "cache"),
        compression="none", verify_cache_checksum="off", prefetch_enabled=False,
        writeback_mode="upload_before_commit"))))
    with (run / "mount.log").open("w") as log:
        process = subprocess.Popen([args.binary, "mount", "--config", str(config),
            "--volume-format", "workspace", "--workspace", workspace,
            "--workspace-namespace", case["namespace"], "--meta-backend", "redis",
            "--meta-url", args.meta_url, "--data-backend", "local-fs",
            "--data-dir", str(args.root / case["name"] / "objects"), "--fuse-workers", "2", str(path)],
            stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 40
            while not os.path.ismount(path):
                if process.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError(f"mount did not start; see {run / 'mount.log'}")
                time.sleep(0.1)
            yield path
        finally:
            if os.path.ismount(path):
                command(["fusermount3", "-u", str(path)])
            if process.poll() is None:
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.terminate()
                    process.wait(timeout=10)
                    raise RuntimeError("mount failed to exit cleanly")
            if process.returncode:
                raise RuntimeError(f"mount failed; see {run / 'mount.log'}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--meta-url", required=True)
    parser.add_argument("--fixtures", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--ax-runner", type=Path, required=True)
    args = parser.parse_args()
    args.root.mkdir(exist_ok=False)
    reports = []
    for case in json.loads(args.fixtures.read_text()):
        (args.root / case["name"]).mkdir()
        prefix = [args.binary, "workspace", "--meta-backend", "redis", "--meta-url",
                  args.meta_url, "--workspace-namespace", case["namespace"]]
        revision = json.loads(command(prefix + ["inspect", case["target"]]))["base_revision"]
        token = f"{revision['layer_id']}:{revision['sealed_version']}:{bytes(revision['root_hash']).hex()}"
        data_config = args.root / (case["name"] + "-data.json")
        data_config.write_text(json.dumps(dict(data=dict(backend="local-fs",
            localfs=dict(data_dir=str(args.root / case["name"] / "objects"))))))
        forked = json.loads(command(prefix + ["fork", token, "--data-config", str(data_config)]))
        case["child"] = forked[0]["workspace_id"]
        with mount(args, case, case["child"]) as child:
            assert (child / "baseline").read_bytes() == bytes(4)
            (child / "baseline").write_bytes(b"modified base")
            (child / "baseline").rename(child / "renamed")
            (child / "deleted").unlink()
            (child / "file").write_bytes(b"child content")
            os.link(child / "file", child / "hardlink")
            (child / "directory").mkdir()
            (child / "directory" / "nested").write_bytes(b"nested")
            (child / "link").symlink_to("file")
            (child / "file").chmod(0o700)
            with (child / "sparse").open("wb") as stream:
                stream.seek(8 * 1024 * 1024)
                stream.write(b"tail")
        if case["name"] != "unchanged":
            with mount(args, case, case["target"]) as target:
                name = "baseline" if case["name"] == "conflict" else "parent"
                (target / name).write_bytes(b"parent content")
        first = subprocess.run([sys.executable, str(args.ax_runner), "--binary", args.binary,
            "--meta-url", args.meta_url, "--namespace", case["namespace"], "--child", case["child"],
            "--target", case["target"], "--operation-file", str(args.root / (case["name"] + "-operation.json"))],
            capture_output=True, text=True, timeout=60)
        result = json.loads(first.stdout)
        merge = prefix + ["merge", case["child"], case["target"], "--operation", result["request"]["operation"]]
        if case["name"] == "conflict":
            assert first.returncode != 0 and not result["committed"], result
            with mount(args, case, case["target"]) as target:
                assert (target / "baseline").read_bytes() == b"parent content"
                assert (target / "deleted").read_bytes() == bytes(4)
                assert sorted(p.name for p in target.iterdir()) == ["baseline", "deleted"]
        else:
            assert first.returncode == 0 and result["committed"], (result, first.stderr)
            assert json.loads(command(merge)) == result
            child_head = json.loads(command(prefix + ["inspect", case["child"]]))["head_layer_id"]
            command(prefix + ["discard", case["child"]])
            with mount(args, case, case["target"]):
                key = "{brewfs-ws-v1}:" + case["namespace"] + ":ws:v1/layer/" + child_head
                deadline = time.monotonic() + 180
                while command(["redis-cli", "-u", args.meta_url, "EXISTS", key]).strip() != "0":
                    if time.monotonic() >= deadline:
                        raise RuntimeError("child metadata was not reclaimed")
                    time.sleep(0.25)
            assert json.loads(command(merge)) == result
            with mount(args, case, case["target"]) as target:
                assert (target / "file").read_bytes() == b"child content"
                assert (target / "hardlink").stat().st_ino == (target / "file").stat().st_ino
                assert (target / "renamed").read_bytes() == b"modified base"
                assert not (target / "baseline").exists()
                assert not (target / "deleted").exists()
                assert (target / "directory" / "nested").read_bytes() == b"nested"
                assert (target / "link").read_bytes() == b"child content"
                assert (target / "file").stat().st_mode & 0o777 == 0o700
                assert (target / "sparse").read_bytes() == bytes(8 * 1024 * 1024) + b"tail"
                if case["name"] == "disjoint":
                    assert (target / "parent").read_bytes() == b"parent content"
        reports.append(dict(case=case["name"], result=result))
        (args.root / "results.json").write_text(json.dumps(reports, indent=2))
    print(json.dumps(reports))


if __name__ == "__main__":
    main()
