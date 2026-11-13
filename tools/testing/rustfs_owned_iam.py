#!/usr/bin/env python3
"""Provision and remove owned RustFS runtime/admin identities for one run.

The runtime identity receives the checked-in packed runtime policy, which
denies object deletion. The GC admin identity receives object delete rights on
the same single bucket. Credentials are written to a private 0600 env file and
are never printed. Every admin API failure is fatal; removal is verified by
listing users after deletion.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import re
import secrets
import sys
import urllib.error
import urllib.request

import boto3
import botocore.auth
import botocore.awsrequest
import botocore.config
import botocore.credentials

ROOT = pathlib.Path(__file__).resolve().parents[2]
RUNTIME_POLICY = ROOT / "operator/brewfs-operator/manifests/packed-runtime-object-policy.json"
BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9-]{2,61}[a-z0-9]$")
NAME_RE = re.compile(r"^[A-Za-z0-9]{8,64}$")


class AdminError(RuntimeError):
    pass


def _admin(endpoint: str, root: botocore.credentials.Credentials, method: str, path: str, body: bytes = b"") -> bytes:
    url = endpoint + path
    request = botocore.awsrequest.AWSRequest(
        method=method,
        url=url,
        data=body,
        headers={"x-amz-content-sha256": hashlib.sha256(body).hexdigest()},
    )
    botocore.auth.S3SigV4Auth(root, "s3", "us-east-1").add_auth(request)
    raw = urllib.request.Request(url, data=body or None, method=method, headers=dict(request.headers))
    try:
        with urllib.request.urlopen(raw, timeout=30) as response:
            if response.status != 200:
                raise AdminError(f"{method} {path.split('?')[0]} returned {response.status}")
            return response.read()
    except urllib.error.HTTPError as error:
        raise AdminError(f"{method} {path.split('?')[0]} returned {error.code}: {error.read()[:200]!r}") from error


def _s3(endpoint: str, access: str, secret: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name="us-east-1",
        config=botocore.config.Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}),
    )


def _policy(bucket: str, runtime: bool) -> bytes:
    if runtime:
        policy = json.loads(RUNTIME_POLICY.read_text(encoding="utf-8"))
        for statement in policy["Statement"]:
            statement["Resource"] = [item.replace("__BUCKET__", bucket) for item in statement["Resource"]]
    else:
        # GC admin is read+delete only: the real SID collector tests require
        # its PUT to fail with 403 so it can never mint or overwrite objects.
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "GcAdminReadDelete",
                    "Effect": "Allow",
                    "Action": ["s3:GetObject", "s3:DeleteObject", "s3:DeleteObjectVersion"],
                    "Resource": [f"arn:aws:s3:::{bucket}/*"],
                },
                {
                    "Sid": "GcAdminDenyWrite",
                    "Effect": "Deny",
                    "Action": ["s3:PutObject"],
                    "Resource": [f"arn:aws:s3:::{bucket}/*"],
                },
                {"Sid": "GcAdminList", "Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": [f"arn:aws:s3:::{bucket}"]},
            ],
        }
    return json.dumps(policy).encode()


def _users(endpoint: str, root) -> dict:
    return json.loads(_admin(endpoint, root, "GET", "/rustfs/admin/v3/list-users") or b"{}")


def provision(args) -> int:
    for bucket in args.bucket:
        if not BUCKET_RE.match(bucket):
            raise AdminError("invalid bucket name")
    root = botocore.credentials.Credentials(os.environ["RUSTFS_ROOT_ACCESS_KEY"], os.environ["RUSTFS_ROOT_SECRET_KEY"])
    root_s3 = _s3(args.endpoint, root.access_key, root.secret_key)
    lines = []
    for role in ("runtime", "admin"):
        name = f"{role}{secrets.token_hex(8)}"
        secret = secrets.token_hex(24)
        policy_name = f"brewfs-{role}-{args.owner}"
        statements = []
        for bucket in args.bucket:
            statements.extend(json.loads(_policy(bucket, role == "runtime"))["Statement"])
        document = json.dumps({"Version": "2012-10-17", "Statement": statements}).encode()
        _admin(args.endpoint, root, "PUT", f"/rustfs/admin/v3/add-user?accessKey={name}",
               json.dumps({"secretKey": secret, "status": "enabled"}).encode())
        _admin(args.endpoint, root, "PUT", f"/rustfs/admin/v3/add-canned-policy?name={policy_name}", document)
        _admin(args.endpoint, root, "PUT",
               f"/rustfs/admin/v3/set-user-or-group-policy?policyName={policy_name}&userOrGroup={name}&isGroup=false")
        lines.append((role, name, secret, policy_name))
    for bucket in args.bucket:
        root_s3.create_bucket(Bucket=bucket)
    # Fail closed unless the actual server enforces the runtime/admin split.
    runtime = next(line for line in lines if line[0] == "runtime")
    admin = next(line for line in lines if line[0] == "admin")
    probe_bucket = args.bucket[0]
    probe_key = f"iam-probe/{secrets.token_hex(8)}"
    runtime_s3 = _s3(args.endpoint, runtime[1], runtime[2])
    runtime_s3.put_object(Bucket=probe_bucket, Key=probe_key, Body=b"owned-iam-probe")
    try:
        runtime_s3.delete_object(Bucket=probe_bucket, Key=probe_key)
    except Exception as error:  # noqa: BLE001 - classify the exact service error below
        status = getattr(error, "response", {}).get("ResponseMetadata", {}).get("HTTPStatusCode")
        code = getattr(error, "response", {}).get("Error", {}).get("Code")
        if status != 403 or code != "AccessDenied":
            raise AdminError(f"runtime DELETE failed with unexpected error {status} {code}") from error
    else:
        raise AdminError("runtime identity was allowed to DELETE")
    admin_s3 = _s3(args.endpoint, admin[1], admin[2])
    try:
        admin_s3.put_object(Bucket=probe_bucket, Key=probe_key + "-admin", Body=b"must-not-exist")
    except Exception as error:  # noqa: BLE001 - classify the exact service error below
        status = getattr(error, "response", {}).get("ResponseMetadata", {}).get("HTTPStatusCode")
        code = getattr(error, "response", {}).get("Error", {}).get("Code")
        if status != 403 or code != "AccessDenied":
            raise AdminError(f"admin PUT failed with unexpected error {status} {code}") from error
    else:
        raise AdminError("GC admin identity was allowed to PUT")
    admin_s3.delete_object(Bucket=probe_bucket, Key=probe_key)
    fd = os.open(args.env_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        stream.write(f"BREWFS_TEST_ORIGINAL_RUSTFS_ACCESS_KEY={runtime[1]}\n")
        stream.write(f"BREWFS_TEST_ORIGINAL_RUSTFS_SECRET_KEY={runtime[2]}\n")
        # The actual CLI child resolves S3 credentials from the AWS chain;
        # it must run with exactly the runtime identity, never the admin one.
        stream.write(f"AWS_ACCESS_KEY_ID={runtime[1]}\n")
        stream.write(f"AWS_SECRET_ACCESS_KEY={runtime[2]}\n")
        stream.write(f"BREWFS_TEST_SID_RUSTFS_ADMIN_ACCESS_KEY={admin[1]}\n")
        stream.write(f"BREWFS_TEST_SID_RUSTFS_ADMIN_SECRET_KEY={admin[2]}\n")
        stream.write(f"RUSTFS_OWNED_USERS={runtime[1]},{admin[1]}\n")
        stream.write(f"RUSTFS_OWNED_POLICIES={runtime[3]},{admin[3]}\n")
    print(json.dumps({"provisioned_users": 2, "buckets": args.bucket, "runtime_delete": "403 AccessDenied",
                      "admin_put": "403 AccessDenied", "admin_delete": "ok"}))
    return 0


def remove(args) -> int:
    root = botocore.credentials.Credentials(os.environ["RUSTFS_ROOT_ACCESS_KEY"], os.environ["RUSTFS_ROOT_SECRET_KEY"])
    users = [name for name in os.environ.get("RUSTFS_OWNED_USERS", "").split(",") if name]
    policies = [name for name in os.environ.get("RUSTFS_OWNED_POLICIES", "").split(",") if name]
    errors = []
    for name in users:
        if not NAME_RE.match(name):
            errors.append("refusing malformed user name")
            continue
        try:
            _admin(args.endpoint, root, "DELETE", f"/rustfs/admin/v3/remove-user?accessKey={name}")
        except AdminError as error:
            errors.append(str(error))
    for name in policies:
        try:
            _admin(args.endpoint, root, "DELETE", f"/rustfs/admin/v3/remove-canned-policy?name={name}")
        except AdminError as error:
            errors.append(str(error))
    try:
        remaining = sorted(set(users) & set(_users(args.endpoint, root)))
    except AdminError as error:
        errors.append(str(error))
        remaining = users
    print(json.dumps({"removed_users": len(users) - len(remaining), "remaining_users": len(remaining), "errors": errors}))
    return 1 if errors or remaining else 0


def _object_listing(endpoint: str, access: str, secret: str, bucket: str) -> list[tuple[str, str]]:
    client = _s3(endpoint, access, secret)
    rows = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, PaginationConfig={"MaxItems": 4096}):
        rows.extend((item["Key"], item["ETag"]) for item in page.get("Contents", []))
    return sorted(rows)


def serve_fault(args) -> int:
    """Serve deny-put/assert-unchanged/restore-put for the pending-upload cases.

    deny-put swaps the runtime user's policy to a PUT-denying variant for the
    one case bucket and snapshots its object listing with the root identity;
    assert-unchanged proves no object appeared or changed; restore-put attaches
    the original runtime policy again. Each request must carry the token.
    """
    import socket

    root = botocore.credentials.Credentials(os.environ["RUSTFS_ROOT_ACCESS_KEY"], os.environ["RUSTFS_ROOT_SECRET_KEY"])
    token = os.environ["BREWFS_TEST_CLI_FAULT_TOKEN"]
    runtime_user = os.environ["BREWFS_TEST_ORIGINAL_RUSTFS_ACCESS_KEY"]
    runtime_policy = os.environ["RUSTFS_OWNED_POLICIES"].split(",")[0]
    owner = args.owner
    denied_policy = f"brewfs-runtime-denyput-{owner}"
    snapshots: dict[str, list[tuple[str, str]]] = {}
    path = pathlib.Path(args.socket)
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    old_umask = os.umask(0o177)
    try:
        server.bind(str(path))
    finally:
        os.umask(old_umask)
    server.listen(4)
    events = open(args.event_log, "a", encoding="utf-8")
    while True:
        connection, _ = server.accept()
        with connection:
            connection.settimeout(30)
            raw = b""
            while not raw.endswith(b"\n") and len(raw) <= 512:
                chunk = connection.recv(513)
                if not chunk:
                    break
                raw += chunk
            reply = {"ok": False}
            try:
                request = json.loads(raw)
                action, case = request["action"], request["case"]
                if request.get("token") != token or case not in ("rp", "tp"):
                    raise AdminError("unauthorized fault request")
                bucket = f"brewfs-v3-lifecycle-{owner}-cli-{case}"
                if action == "shutdown":
                    reply = {"ok": True, "action": action, "case": case}
                    connection.sendall(json.dumps(reply).encode())
                    events.write(json.dumps({"action": action, "case": case, "ok": True}) + "\n")
                    events.close()
                    server.close()
                    path.unlink()
                    return 0
                if action == "deny-put":
                    policy = json.loads(_policy(bucket, True))
                    policy["Statement"].append({"Sid": "FaultDenyPut", "Effect": "Deny", "Action": ["s3:PutObject"],
                                                "Resource": [f"arn:aws:s3:::{bucket}/*"]})
                    _admin(args.endpoint, root, "PUT", f"/rustfs/admin/v3/add-canned-policy?name={denied_policy}",
                           json.dumps(policy).encode())
                    _admin(args.endpoint, root, "PUT",
                           f"/rustfs/admin/v3/set-user-or-group-policy?policyName={denied_policy}&userOrGroup={runtime_user}&isGroup=false")
                    snapshots[case] = _object_listing(args.endpoint, root.access_key, root.secret_key, bucket)
                elif action == "assert-unchanged":
                    if _object_listing(args.endpoint, root.access_key, root.secret_key, bucket) != snapshots[case]:
                        raise AdminError("object listing changed while PUT was denied")
                elif action == "restore-put":
                    _admin(args.endpoint, root, "PUT",
                           f"/rustfs/admin/v3/set-user-or-group-policy?policyName={runtime_policy}&userOrGroup={runtime_user}&isGroup=false")
                    _admin(args.endpoint, root, "DELETE", f"/rustfs/admin/v3/remove-canned-policy?name={denied_policy}")
                else:
                    raise AdminError("unknown fault action")
                reply = {"ok": True, "action": action, "case": case}
            except Exception as error:  # noqa: BLE001 - every failure is reported, never swallowed
                reply = {"ok": False, "error": type(error).__name__ + ": " + str(error)[:200]}
            events.write(json.dumps({**{k: v for k, v in reply.items()}, "request_action": reply.get("action")}) + "\n")
            events.flush()
            connection.sendall(json.dumps(reply).encode())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    f = sub.add_parser("serve-fault")
    f.add_argument("--endpoint", required=True)
    f.add_argument("--owner", required=True)
    f.add_argument("--socket", required=True)
    f.add_argument("--event-log", required=True)
    p = sub.add_parser("provision")
    p.add_argument("--endpoint", required=True)
    p.add_argument("--owner", required=True)
    p.add_argument("--bucket", action="append", required=True)
    p.add_argument("--env-file", type=pathlib.Path, required=True)
    r = sub.add_parser("remove")
    r.add_argument("--endpoint", required=True)
    args = parser.parse_args()
    if not args.endpoint.startswith("http://127.0.0.1:"):
        raise SystemExit("owned RustFS endpoint must be IPv4 loopback")
    try:
        if args.command == "serve-fault":
            return serve_fault(args)
        return provision(args) if args.command == "provision" else remove(args)
    except (AdminError, KeyError) as error:
        print(json.dumps({"error": str(error)}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
