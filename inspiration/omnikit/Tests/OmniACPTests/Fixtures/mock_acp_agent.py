#!/usr/bin/env python3
import json
import sys
import time

SESSION_ID = "sess_mock_123"


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
                "agentInfo": {"name": "MockACPAgent", "version": "1.0.0"},
                "agentCapabilities": {
                    "loadSession": True,
                    "mcpCapabilities": {},
                    "promptCapabilities": {"image": True, "audio": True, "embeddedContext": True}
                },
                "authMethods": []
            }
        })
    elif method == "notifications/initialized":
        continue
    elif method == "session/new":
        send({
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {"sessionId": SESSION_ID}
        })
    elif method == "session/set_mode":
        send({"jsonrpc": "2.0", "id": msg_id, "result": {}})
    elif method == "session/cancel":
        send({"jsonrpc": "2.0", "id": msg_id, "result": {}})
    elif method == "session/prompt":
        send({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": SESSION_ID,
                "update": {
                    "sessionUpdate": "plan",
                    "entries": [
                        {"content": "Inspect repo", "priority": "high", "status": "completed"},
                        {"content": "Implement fix", "priority": "high", "status": "completed"}
                    ]
                }
            }
        })
        send({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": SESSION_ID,
                "update": {
                    "sessionUpdate": "tool_call",
                    "toolCallId": "call_1",
                    "title": "Reading file",
                    "kind": "read",
                    "status": "completed"
                }
            }
        })
        text = (
            "Mock ACP response.\n\n"
            "```json\n"
            "{\"outcome\":\"success\",\"preferred_next_label\":\"\",\"context_updates\":{\"acp_mock\":\"true\"},\"notes\":\"mock agent\"}\n"
            "```"
        )
        for chunk in [text[:20], text[20:]]:
            send({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": SESSION_ID,
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": chunk}
                    }
                }
            })
        send({
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {"stopReason": "end_turn"}
        })
    else:
        send({
            "jsonrpc": "2.0",
            "id": msg_id,
            "error": {"code": -32601, "message": f"Method not found: {method}"}
        })
