#!/usr/bin/env python3
"""Configure and wait for a newly created single-store loopback test cluster."""
import json
import sys
import time
import urllib.request

port = int(sys.argv[1])
if not 1 < port < 65536:
    raise SystemExit("invalid task-owned PD port")
base = f"http://127.0.0.1:{port}/pd/api/v1"
for attempt in range(90):
    try:
        request = urllib.request.Request(
            base + "/config",
            data=b'{"max-replicas":1}',
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=2) as response:
            response.read()
        with urllib.request.urlopen(base + "/stores", timeout=2) as response:
            stores = json.load(response).get("stores", [])
        if len(stores) == 1 and stores[0]["store"]["state_name"] == "Up":
            break
    except (OSError, ValueError, KeyError):
        pass
    time.sleep(1)
else:
    raise SystemExit("owned TiKV cluster did not become ready")
