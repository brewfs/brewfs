#!/usr/bin/env python3
"""Run the isolated HTTP byte comparison against a dedicated compose EC fixture.

Supply a Rust unit-test executable built with rustfs-ec-block-size-hint.
Requires boto3 and msgpack. Never shares the ordinary perf-runner volumes.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.request

import boto3
from botocore.config import Config

from summarize_ec_sparse_read import summarize


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    script_dir = Path(__file__).resolve().parent
    compose = ["docker", "compose", "-p", "brewfs-ec-sparse", "-f",
               str(script_dir / "docker-compose.ec-sparse.yml")]
    env = os.environ.copy()
    for name in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"]:
        env.pop(name, None)
    endpoint = f"http://127.0.0.1:{env.get('EC_SPARSE_PORT', '19300')}"
    env.update(BREWFS_S3_ENDPOINT=endpoint, BREWFS_S3_BUCKET="ec-sparse-evidence",
               BREWFS_EC_SPARSE_EVIDENCE=str(output / "results.json"))
    with args.test_binary.open("rb") as binary:
        binary_sha256 = hashlib.file_digest(binary, "sha256").hexdigest()
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        subprocess.run(compose + ["up", "-d"], check=True, env=env)
        deadline = time.monotonic() + 60
        while True:
            try:
                with opener.open(endpoint + "/health", timeout=2) as response:
                    health = json.load(response)
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(1)
        s3 = boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1",
                          aws_access_key_id="rustfsadmin", aws_secret_access_key="rustfsadmin",
                          config=Config(connect_timeout=5, read_timeout=10, proxies={}))
        try:
            s3.create_bucket(Bucket="ec-sparse-evidence")
        except s3.exceptions.BucketAlreadyOwnedByYou:
            pass
        container = subprocess.check_output(compose + ["ps", "-q", "rustfs"], text=True).strip()
        image = subprocess.check_output(["docker", "inspect", container, "--format", "{{.Image}}"], text=True).strip()
        identity = json.loads(subprocess.check_output(["docker", "image", "inspect", image], text=True))[0]
        (output / "environment.json").write_text(json.dumps({
            "health": health, "image_id": image, "image_labels": identity["Config"].get("Labels"),
            "geometry": "four directories on one local Docker disk; disk check bypassed",
            "profile": "debug=0, unoptimized Rust test executable; warm server caches",
            "requested_ec_hint_bytes": 262144,
            "test_binary_sha256": binary_sha256,
        }, indent=2))
        with (output / "test.log").open("w") as log:
            subprocess.run([str(args.test_binary.resolve()),
                            "chunk::store::read_policy_s3_evidence::ec_sparse_http_evidence", "--ignored", "--exact",
                            "--nocapture"], env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
        subprocess.run(["docker", "cp", f"{container}:/data", str(output / "data")], check=True)
        summary = summarize(output / "results.json", output / "data")
        (output / "summary.json").write_text(json.dumps(summary, indent=2))
        print(json.dumps(summary["medians"], indent=2))
    finally:
        if not (output / "data").exists():
            # Preserve persisted layout diagnostics on a failed run too.
            container = subprocess.check_output(compose + ["ps", "-a", "-q", "rustfs"], text=True).strip()
            if container:
                subprocess.run(["docker", "cp", f"{container}:/data", str(output / "data")], check=False)
        with (output / "compose.log").open("w") as log:
            subprocess.run(compose + ["logs", "--no-color"], stdout=log, stderr=subprocess.STDOUT)
        subprocess.run(compose + ["down", "-v", "--remove-orphans"], check=True)


if __name__ == "__main__":
    main()
