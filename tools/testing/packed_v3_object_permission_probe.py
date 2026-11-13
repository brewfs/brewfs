#!/usr/bin/env python3
"""Owned tiny S3 permission probe. It never provisions users or retries DELETE.

Requires boto3. Credentials are explicit task environment values; no AWS default
provider/profile fallback is used. Run separately for Redis and TiKV wiring.
"""
import argparse
import json
import os
from pathlib import Path
import re
import sys
import uuid


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--metadata-backend", choices=("redis", "tikv"))
    parser.add_argument("--out", type=Path)
    parser.add_argument("--render-policy", type=Path,
                        help="Render the runtime policy to this new file; performs no network calls.")
    return parser.parse_args()


def validate_bucket(bucket):
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket) or ".." in bucket:
        raise ValueError("bucket must be an ordinary DNS S3 bucket name")


def render_policy(args):
    source = Path(__file__).resolve().parents[2] / "operator/brewfs-operator/manifests/packed-runtime-object-policy.json"
    policy = json.loads(source.read_text(encoding="utf-8"))
    for statement in policy["Statement"]:
        statement["Resource"] = [value.replace("__BUCKET__", args.bucket) for value in statement["Resource"]]
    with args.render_policy.open("x", encoding="utf-8") as stream:
        json.dump(policy, stream, indent=2)
        stream.write("\n")


def credentials(role):
    prefix = f"BREWFS_TEST_{role.upper()}_"
    values = tuple(os.environ.get(prefix + suffix, "") for suffix in ("ACCESS_KEY", "SECRET_KEY"))
    if any(not value or value.strip() != value for value in values):
        raise ValueError(f"explicit {role} object credentials are missing or malformed")
    return values


def error_kind(error):
    response = getattr(error, "response", {})
    return (response.get("Error", {}).get("Code", type(error).__name__),
            response.get("ResponseMetadata", {}).get("HTTPStatusCode"))


def probe(args):
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError

    if not args.endpoint or not args.metadata_backend:
        raise ValueError("probe requires --endpoint and --metadata-backend")
    runtime_keys, admin_keys = credentials("runtime"), credentials("admin")
    if runtime_keys[0] == admin_keys[0]:
        raise ValueError("runtime and admin access-key principals must differ")
    config = Config(retries={"total_max_attempts": 1, "mode": "standard"},
                    connect_timeout=5, read_timeout=10, s3={"addressing_style": "path"})

    def client(keys):
        return boto3.client("s3", endpoint_url=args.endpoint, region_name=args.region,
                            aws_access_key_id=keys[0], aws_secret_access_key=keys[1], config=config)

    runtime, admin = client(runtime_keys), client(admin_keys)
    key = f"packed-permission-probe/{uuid.uuid4().hex}.object"
    payload = bytes(range(256))
    result = {"schema": "packed-object-permission-probe", "metadata_backend": args.metadata_backend,
              "owned_key": key, "payload_bytes": len(payload), "sdk_max_attempts": 1,
              "admin_list_prefix_positive": False, "admin_head_positive": False,
              "admin_absence_verified": False,
              "runtime_create": False, "runtime_get_range": False,
              "conditional_overwrite_rejected": False, "runtime_delete_denied": False,
              "runtime_version_delete_denied": "not-versioned",
              "bytes_unchanged_after_denial": False, "admin_delete_positive": False,
              "passed": False}
    owned_put_attempted = False
    admin_delete_attempted = False
    version = None

    def get_bytes(range_value=None):
        options = {"Bucket": args.bucket, "Key": key}
        if range_value:
            options["Range"] = range_value
        response = runtime.get_object(**options)
        with response["Body"] as body:
            return body.read(len(payload) + 1)

    def admin_delete_once():
        nonlocal admin_delete_attempted
        if admin_delete_attempted:
            raise RuntimeError("refuse to replay admin DELETE")
        admin_delete_attempted = True
        options = {"Bucket": args.bucket, "Key": key}
        if version:
            options["VersionId"] = version
        admin.delete_object(**options)

    try:
        # GetObject without ListBucket may report 403 for an absent key. Prove
        # that the independent admin has both permissions before using its
        # later HEAD 404 as evidence of deletion; never infer absence from 403.
        listed = admin.list_objects_v2(Bucket=args.bucket, Prefix=key, MaxKeys=1)
        if listed.get("Contents") or listed.get("IsTruncated", False):
            raise RuntimeError("fresh owned probe prefix is not empty")
        result["admin_list_prefix_positive"] = True
        owned_put_attempted = True
        put = runtime.put_object(Bucket=args.bucket, Key=key, Body=payload, IfNoneMatch="*")
        version = put.get("VersionId")
        result["runtime_create"] = True
        if get_bytes("bytes=17-31") != payload[17:32] or get_bytes() != payload:
            raise RuntimeError("runtime GET/range returned unexpected bytes")
        result["runtime_get_range"] = True
        try:
            runtime.put_object(Bucket=args.bucket, Key=key, Body=b"overwrite", IfNoneMatch="*")
        except ClientError as error:
            code, status = error_kind(error)
            if status != 412 or code not in ("PreconditionFailed", "412"):
                raise RuntimeError("conditional PUT failed without a precondition rejection") from None
            result["conditional_overwrite_rejected"] = True
        else:
            raise RuntimeError("existing object was overwritten by conditional PUT")
        try:
            runtime.delete_object(Bucket=args.bucket, Key=key)
        except ClientError as error:
            code, status = error_kind(error)
            result["runtime_delete_response"] = {"code": code, "status": status}
            if status != 403 or code not in ("AccessDenied", "Forbidden", "403"):
                raise RuntimeError("runtime DELETE failed without server authorization denial") from None
            result["runtime_delete_denied"] = True
        else:
            raise RuntimeError("runtime DELETE unexpectedly succeeded")
        if get_bytes() != payload:
            raise RuntimeError("probe bytes changed after runtime DELETE denial")
        result["bytes_unchanged_after_denial"] = True
        if version:
            try:
                runtime.delete_object(Bucket=args.bucket, Key=key, VersionId=version)
            except ClientError as error:
                code, status = error_kind(error)
                result["runtime_version_delete_response"] = {"code": code, "status": status}
                if status != 403 or code not in ("AccessDenied", "Forbidden", "403"):
                    raise RuntimeError("runtime version DELETE failed without server authorization denial") from None
                result["runtime_version_delete_denied"] = True
            else:
                raise RuntimeError("runtime version DELETE unexpectedly succeeded")
            if get_bytes() != payload:
                raise RuntimeError("probe bytes changed after runtime version DELETE denial")
        head = admin.head_object(Bucket=args.bucket, Key=key)
        if head.get("ContentLength") != len(payload):
            raise RuntimeError("admin HEAD did not authenticate the owned object")
        result["admin_head_positive"] = True
        admin_delete_once()
        try:
            admin.head_object(Bucket=args.bucket, Key=key)
        except ClientError as error:
            code, status = error_kind(error)
            if status != 404 or code not in ("NoSuchKey", "NotFound", "404"):
                raise RuntimeError("admin HEAD did not verify DELETE by authorized absence") from None
        else:
            raise RuntimeError("object remains present after admin DELETE")
        result["admin_absence_verified"] = True
        result["admin_delete_positive"] = True
        result["passed"] = True
    except Exception as error:
        code, status = error_kind(error)
        # Never serialize endpoint credentials, raw HTTP headers or exception
        # repr; stable error code/status is sufficient for the acceptance record.
        result["failure"] = {"code": code, "status": status, "type": type(error).__name__}
    finally:
        if owned_put_attempted and not admin_delete_attempted:
            try:
                admin_delete_once()
                result["owned_cleanup"] = "admin-delete-returned"
            except Exception as error:
                code, status = error_kind(error)
                result["owned_cleanup"] = {"code": code, "status": status}
        if admin_delete_attempted and not result["admin_delete_positive"]:
            result["cleanup_uncertain_do_not_replay_delete"] = True
        runtime.close()
        admin.close()
    destination = args.out or Path(f"packed-object-permission-{uuid.uuid4().hex}.json")
    with destination.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")
    print(json.dumps({"passed": result["passed"], "result_file": str(destination)}))
    return 0 if result["passed"] else 1


def main():
    args = arguments()
    validate_bucket(args.bucket)
    if args.render_policy:
        render_policy(args)
        return 0
    return probe(args)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        # Configuration/import failures also avoid raw messages and secrets.
        print(json.dumps({"passed": False, "failure_type": type(error).__name__}), file=sys.stderr)
        sys.exit(2)
