#!/usr/bin/env python3
import json
import sys
import time

SESSION_ID = "sess_sleepy"

def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

for raw in sys.stdin:
    raw = raw.strip()
    if not raw:
        continue
    message = json.loads(raw)
    method = message.get("method")
    msg_id = message.get("id")
    if method == "initialize":
        send({
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "protocolVersion": 1,
                "agentCapabilities": {"mcpCapabilities": {}, "promptCapabilities": {}},
                "authMethods": []
            }
        })
    elif method == "notifications/initialized":
        continue
    elif method == "session/new":
        send({"jsonrpc": "2.0", "id": msg_id, "result": {"sessionId": SESSION_ID}})
    elif method == "session/prompt":
        time.sleep(3600)
    elif method == "session/cancel":
        send({"jsonrpc": "2.0", "id": msg_id, "result": {}})
    else:
        send({"jsonrpc": "2.0", "id": msg_id, "result": {}})
