#!/usr/bin/env python3
"""Small, dependency-free BrewFS result server.

The API stores the original ZIP alongside an extracted, timestamped file tree.
It is intended for a private test host; put it behind the instance security
group or a reverse proxy with authentication before exposing it publicly.
"""

import email
import csv
import hashlib
import json
import mimetypes
import os
import posixpath
import re
import shutil
import time
import uuid
import zipfile
from datetime import datetime
from email import policy
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.parse import unquote, urlparse


ROOT = Path(os.environ.get("BREWFS_RESULTS_ROOT", "/var/lib/brewfs-results")).resolve()
STATIC = Path(os.environ.get("BREWFS_RESULTS_STATIC", str(Path(__file__).parent / "dist"))).resolve()
MAX_UPLOAD = int(os.environ.get("BREWFS_RESULTS_MAX_UPLOAD", str(1024 * 1024 * 1024)))


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


def now_ms() -> int:
    return int(time.time() * 1000)


def safe_relative(value: str) -> str:
    value = value.replace("\\", "/")
    value = posixpath.normpath(value).lstrip("/")
    if value in ("", ".") or value == ".." or value.startswith("../"):
        raise ValueError("unsafe archive path")
    return value


def dos_mtime(info: zipfile.ZipInfo) -> int:
    return int(datetime(*info.date_time).timestamp() * 1000)


def classify(paths: List[str], extracted: Path) -> Tuple[str, str, str]:
    haystack = " ".join(paths).lower()
    for candidate in extracted.rglob("report.md"):
        if candidate.is_file():
            try:
                haystack += " " + candidate.read_text(errors="replace")[:1_000_000].lower()
            except OSError:
                continue
    backend = "tikv" if "tikv" in haystack or "pd-" in haystack else "redis" if "redis" in haystack else "unknown"
    data_backend = "s3" if "rustfs" in haystack or "s3" in haystack else "local-fs" if "local-fs" in haystack or "local_fs" in haystack else "unknown"
    status = "attention" if re.search(r"\b(failed|failure|error)\b", haystack) and not re.search(r"0\s+(failed|failure|error)", haystack) else "pass" if re.search(r"\b(pass|passed|success|succeeded)\b", haystack) else "unknown"
    return backend, data_backend, status


def metric_number(value):
    try:
        parsed = float(value)
        return parsed if parsed == parsed and parsed not in (float("inf"), float("-inf")) else None
    except (TypeError, ValueError):
        return None


def parse_metrics(extracted: Path) -> List[dict]:
    """Extract stable, comparable metrics without requiring fio or pandas on the server."""
    metrics = {}
    summary = next(iter(extracted.rglob("perf-summary.tsv")), None)
    if summary and summary.is_file():
        try:
            with summary.open(newline="", errors="replace") as handle:
                for row in csv.DictReader(handle, delimiter="\t"):
                    tool = row.get("tool", "")
                    if tool:
                        metrics[tool] = {
                            "tool": tool,
                            "status": row.get("status", "unknown"),
                            "seconds": metric_number(row.get("seconds")) or 0,
                        }
        except (OSError, ValueError):
            pass
    drained = next(iter(extracted.rglob("fully-drained-throughput.tsv")), None)
    if drained and drained.is_file():
        try:
            with drained.open(newline="", errors="replace") as handle:
                for row in csv.DictReader(handle, delimiter="\t"):
                    metric = metrics.get(row.get("tool", ""))
                    if not metric:
                        continue
                    for key, source in (("readMiBps", "read_mib_s"), ("writeMiBps", "write_mib_s"), ("totalMiBps", "total_mib_s")):
                        value = metric_number(row.get(source))
                        if value is not None:
                            metric[key] = value
        except (OSError, ValueError):
            pass
    for path in extracted.rglob("fio*.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8", errors="replace"))
            jobs = data.get("jobs", [])
            if not jobs:
                continue
            tool = path.stem
            metric = metrics.setdefault(tool, {"tool": tool, "status": "unknown", "seconds": 0})
            read_bw = write_bw = read_iops = write_iops = 0.0
            read_p99 = write_p99 = 0.0
            runtime_ms = 0.0
            for job in jobs:
                runtime_ms = max(runtime_ms, metric_number(job.get("job_runtime")) or 0)
                for kind in ("read", "write"):
                    op = job.get(kind) or {}
                    runtime_ms = max(runtime_ms, metric_number(op.get("runtime")) or 0)
                    bw = metric_number(op.get("bw_bytes")) or 0
                    iops = metric_number(op.get("iops")) or 0
                    if kind == "read":
                        read_bw += bw; read_iops += iops
                    else:
                        write_bw += bw; write_iops += iops
                    percentiles = (op.get("clat_ns") or {}).get("percentile") or {}
                    p99 = metric_number(percentiles.get("99.000000") or percentiles.get("99")) or 0
                    if kind == "read": read_p99 = max(read_p99, p99)
                    else: write_p99 = max(write_p99, p99)
            if read_bw: metric["readMiBps"] = read_bw / (1024 * 1024)
            if write_bw: metric["writeMiBps"] = write_bw / (1024 * 1024)
            if read_bw + write_bw: metric["totalMiBps"] = (read_bw + write_bw) / (1024 * 1024)
            if read_iops: metric["readIops"] = read_iops
            if write_iops: metric["writeIops"] = write_iops
            if read_p99: metric["readP99Ms"] = read_p99 / 1000000
            if write_p99: metric["writeP99Ms"] = write_p99 / 1000000
            if not metric.get("seconds") and runtime_ms: metric["seconds"] = runtime_ms / 1000
        except (OSError, ValueError, TypeError):
            continue
    # Fully-drained throughput includes close/flush time and intentionally wins
    # over foreground fio bandwidth for write workloads.
    if drained and drained.is_file():
        try:
            with drained.open(newline="", errors="replace") as handle:
                for row in csv.DictReader(handle, delimiter="\t"):
                    metric = metrics.get(row.get("tool", ""))
                    if not metric:
                        continue
                    for key, source in (("readMiBps", "read_mib_s"), ("writeMiBps", "write_mib_s"), ("totalMiBps", "total_mib_s")):
                        value = metric_number(row.get(source))
                        if value is not None:
                            metric[key] = value
        except (OSError, ValueError):
            pass
    return [metrics[key] for key in sorted(metrics)]


def create_run(archive: bytes, source_name: str) -> dict:
    run_id = f"run-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
    run_dir = ROOT / run_id
    extracted = run_dir / "files"
    extracted.mkdir(parents=True, exist_ok=False)
    archive_path = run_dir / "original.zip"
    archive_path.write_bytes(archive)
    files: List[dict] = []
    mtimes: List[int] = []
    paths: List[str] = []
    try:
        with zipfile.ZipFile(archive_path) as archive_file:
            for info in archive_file.infolist():
                path = safe_relative(info.filename)
                paths.append(path)
                mtime = dos_mtime(info)
                mtimes.append(mtime)
                mode = (info.external_attr >> 16) & 0xFFFF or None
                is_directory = info.is_dir() or path.endswith("/")
                target = extracted / path.rstrip("/")
                if is_directory:
                    target.mkdir(parents=True, exist_ok=True)
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with archive_file.open(info) as source, target.open("wb") as destination:
                        shutil.copyfileobj(source, destination)
                    os.utime(target, (mtime / 1000, mtime / 1000))
                    if mode:
                        try:
                            os.chmod(target, mode & 0o7777)
                        except OSError:
                            pass
                files.append({
                    "path": path.rstrip("/"),
                    "size": info.file_size,
                    "mtime": mtime,
                    "mode": mode,
                    "kind": "directory" if is_directory else "file",
                    "compression": info.compress_type,
                })
    except Exception:
        shutil.rmtree(run_dir, ignore_errors=True)
        raise
    backend, data_backend, status = classify(paths, extracted)
    run = {
        "id": run_id,
        "name": Path(source_name).stem or run_id,
        "sourceName": source_name,
        "backend": backend,
        "dataBackend": data_backend,
        "status": status,
        "uploadedAt": now_ms(),
        "fileCount": len(files),
        "totalBytes": sum(item["size"] for item in files),
        "earliestMtime": min(mtimes) if mtimes else now_ms(),
        "latestMtime": max(mtimes) if mtimes else now_ms(),
        "metrics": parse_metrics(extracted),
        "files": files,
    }
    (run_dir / "metadata.json").write_text(json.dumps(run, ensure_ascii=False, indent=2), encoding="utf-8")
    return run


def load_runs() -> List[dict]:
    runs: List[dict] = []
    ROOT.mkdir(parents=True, exist_ok=True)
    for metadata in ROOT.glob("*/metadata.json"):
        try:
            run = json.loads(metadata.read_text(encoding="utf-8"))
            if "metrics" not in run:
                run["metrics"] = parse_metrics(metadata.parent / "files")
                metadata.write_text(json.dumps(run, ensure_ascii=False, indent=2), encoding="utf-8")
            runs.append(run)
        except (OSError, json.JSONDecodeError):
            continue
    return sorted(runs, key=lambda item: item.get("uploadedAt", 0), reverse=True)


def find_run(run_id: str) -> Tuple[dict, Path]:
    if "/" in run_id or "\\" in run_id or run_id in ("", ".", ".."):
        raise FileNotFoundError(run_id)
    directory = ROOT / run_id
    metadata = directory / "metadata.json"
    if not metadata.exists():
        raise FileNotFoundError(run_id)
    return json.loads(metadata.read_text(encoding="utf-8")), directory


class Handler(BaseHTTPRequestHandler):
    server_version = "BrewFSResultServer/1.0"

    def send_json(self, payload: object, status: int = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def send_error_text(self, status: int, message: str) -> None:
        self.send_json({"error": message}, status)

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        parts = [unquote(part) for part in parsed.path.split("/") if part]
        try:
            if parts == ["api", "runs"]:
                self.send_json(load_runs())
                return
            if len(parts) >= 3 and parts[:2] == ["api", "runs"]:
                run, directory = find_run(parts[2])
                if len(parts) == 4 and parts[3] == "archive":
                    self.send_file(directory / "original.zip", "application/zip", download=True)
                    return
                if len(parts) >= 4 and parts[3] == "files":
                    relative = "/".join(parts[4:])
                    target = (directory / "files" / safe_relative(relative)).resolve()
                    if not str(target).startswith(str((directory / "files").resolve())) or not target.is_file():
                        raise FileNotFoundError(relative)
                    self.send_file(target, mimetypes.guess_type(target.name)[0] or "application/octet-stream")
                    return
                self.send_json(run)
                return
            self.serve_static(parsed.path)
        except FileNotFoundError:
            self.send_error_text(HTTPStatus.NOT_FOUND, "result not found")
        except (ValueError, zipfile.BadZipFile) as error:
            self.send_error_text(HTTPStatus.BAD_REQUEST, str(error))
        except OSError as error:
            self.send_error_text(HTTPStatus.INTERNAL_SERVER_ERROR, str(error))

    def do_POST(self) -> None:  # noqa: N802
        if urlparse(self.path).path != "/api/runs":
            self.send_error_text(HTTPStatus.NOT_FOUND, "endpoint not found")
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > MAX_UPLOAD:
                raise ValueError(f"upload must be between 1 and {MAX_UPLOAD} bytes")
            content_type = self.headers.get("Content-Type", "")
            body = self.rfile.read(length)
            message = email.message_from_bytes(f"Content-Type: {content_type}\r\n\r\n".encode() + body, policy=policy.default)
            upload = next((part for part in message.iter_parts() if part.get_param("name", header="content-disposition") == "archive"), None)
            if upload is None:
                raise ValueError("multipart field 'archive' is required")
            archive = upload.get_payload(decode=True)
            if not archive:
                raise ValueError("empty archive")
            source_name = upload.get_filename() or "brewfs-run.zip"
            run = create_run(archive, source_name)
            self.send_json(run, HTTPStatus.CREATED)
        except (ValueError, zipfile.BadZipFile) as error:
            self.send_error_text(HTTPStatus.BAD_REQUEST, str(error))
        except OSError as error:
            self.send_error_text(HTTPStatus.INTERNAL_SERVER_ERROR, str(error))

    def do_DELETE(self) -> None:  # noqa: N802
        parts = [unquote(part) for part in urlparse(self.path).path.split("/") if part]
        if len(parts) != 3 or parts[:2] != ["api", "runs"]:
            self.send_error_text(HTTPStatus.NOT_FOUND, "endpoint not found")
            return
        try:
            _, directory = find_run(parts[2])
            shutil.rmtree(directory)
            self.send_response(HTTPStatus.NO_CONTENT)
            self.end_headers()
        except FileNotFoundError:
            self.send_error_text(HTTPStatus.NOT_FOUND, "result not found")

    def send_file(self, path: Path, content_type: str, download: bool = False) -> None:
        body = path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        if download:
            self.send_header("Content-Disposition", f'attachment; filename="{path.name}"')
        self.end_headers()
        self.wfile.write(body)

    def serve_static(self, path: str) -> None:
        relative = unquote(path.lstrip("/")) or "index.html"
        target = (STATIC / relative).resolve()
        if not str(target).startswith(str(STATIC)) or not target.is_file():
            target = STATIC / "index.html"
        self.send_file(target, mimetypes.guess_type(target.name)[0] or "application/octet-stream")

    def log_message(self, format: str, *args: object) -> None:
        print(f"[{self.log_date_time_string()}] {format % args}", flush=True)


def main() -> None:
    ROOT.mkdir(parents=True, exist_ok=True)
    host = os.environ.get("BREWFS_RESULTS_BIND", "127.0.0.1")
    port = int(os.environ.get("BREWFS_RESULTS_PORT", "8080"))
    with ThreadingHTTPServer((host, port), Handler) as server:
        print(f"BrewFS Result Vault listening on http://{host}:{port}", flush=True)
        server.serve_forever()


if __name__ == "__main__":
    main()
