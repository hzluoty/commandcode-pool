#!/usr/bin/env python3
"""Mock of the commandcode.ai /alpha endpoints for offline testing.

GET quota endpoints (shape chosen by key substring):
  ok / exhausted / partial / snake / garbage / badkey / cancel / lowbal  — see credits()/subscriptions()

POST endpoints (also shape-by-key):
  /alpha/fingerprint/record   — 200 {"ok": true} (request echoed to stdout)
  /alpha/lifecycle-events     — 200 {"ok": true} (request echoed to stdout)
  /alpha/generate             — NDJSON event stream, shapes:
    ok         text/reasoning deltas + finish with totalUsage
    zero       finish with outputTokens = 0
    partial    stream stops mid-text (no finish event)
    tool       tool-input-* telemetry + authoritative tool-call + finish(tool-calls)
    streamerr  mid-stream {"type":"error"} with "<429> ..." message prefix
    slow       sleeps past the idle watchdog (tests 429 timeout path)
    snake      snake_case usage fields
    exhausted  HTTP 402 {"success": false, "error": {"code": "USAGE_EXCEEDED"}}
    limited    HTTP 429 + Retry-After: 3600
    badkey     HTTP 401
    forbidden  HTTP 403
    badrequest HTTP 400

Requests without a valid CLI shape (missing x-command-code-version on POST) get 401 —
that catches header regressions in manual smoke tests.

Run: python3 mock_server.py [port]   (default 18090)
"""
import json
import sys
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

NOW_MS = int(time.time() * 1000)

WHOAMI = {
    "user": {"id": "u_123", "name": "Max", "userName": "maxeagle"},
    "org": {"id": "org_9"},
}


def credits(shape="ok"):
    if shape == "exhausted":
        five = {"used": 5.0, "cap": 5.0, "exceeded": True,
                "resetAt": NOW_MS + 42 * 60 * 1000}
        limited, exceeded = True, "fiveHour"
    elif shape == "snake":
        five = {"used": 1.2, "cap": 5.0, "exceeded": False,
                "reset_at": NOW_MS + 3600 * 1000}
        limited, exceeded = False, None
    else:
        five = {"used": 1.23, "cap": 5.0, "exceeded": False,
                "resetAt": NOW_MS + 3600 * 1000}
        limited, exceeded = False, None
    weekly = {"used": 12.5, "cap": 40.0, "exceeded": False,
              "resetAt": NOW_MS + 3 * 86400 * 1000}
    monthly, below = (0.5, True) if shape == "lowbal" else (18.5, False)
    return {
        "credits": {"monthlyCredits": monthly, "purchasedCredits": 0,
                    "freeCredits": 2.0, "planId": "individual-goat",
                    "belowThreshold": below, "creditThreshold": 1.0},
        "windowLimits": {"limited": limited, "exceeded": exceeded,
                         "fiveHour": five, "weekly": weekly},
    }


def subscriptions(shape="ok"):
    return {
        "success": True,
        "data": {
            "planId": "individual-goat",
            "status": "active",
            "currentPeriodStart": NOW_MS - 18 * 86400 * 1000,
            "currentPeriodEnd": NOW_MS + 12 * 86400 * 1000,
            "cancelAtPeriodEnd": shape == "cancel",
            "pendingPhase": None,
        },
    }


USAGE = {
    "totalCount": 1883, "totalCost": 6.0011675234,
    "averageCost": 0.0031870247070631963, "successRate": 100,
    "completedCount": 1883, "failedCount": 0,
    "totalTokensIn": 487_592_979, "totalTokensOut": 1_210_434,
    "totalTokens": 488_803_413,
    "totalCredits": 6.001167523399999, "totalFreeCredits": 0,
    "totalMonthlyCredits": 6.001167523399999, "totalPurchasedCredits": 0,
    "periodBasis": "billing-period",
}


# 密钥里带 shape 名即选中该形态（方便联调）：sk-exhausted-… / cc-zero-… 等
def shape_for_key(auth):
    key = (auth or "").lower()
    for s in ("zero", "partial", "tool", "streamerr", "slow", "snake",
              "exhausted", "limited", "badkey", "forbidden", "badrequest"):
        if s in key:
            return s
    return "ok"


USAGE_DETAIL = {"inputTokens": 42, "outputTokens": 9, "totalTokens": 51,
                "cachedInputTokens": 30, "cacheWriteTokens": 12,
                "inputTokenDetails": {"cacheReadTokens": 30,
                                      "cacheWriteTokens": 12,
                                      "noCacheTokens": 12}}


def ndjson(events):
    return "".join(json.dumps(e) + "\n" for e in events).encode()


def generate_events(shape):
    """NDJSON event list for POST /alpha/generate."""
    if shape == "zero":
        return [
            {"type": "text-start", "id": "txt_1"},
            {"type": "text-delta", "id": "txt_1", "text": ""},
            {"type": "text-end", "id": "txt_1"},
            {"type": "finish", "finishReason": "stop",
             "totalUsage": {**USAGE_DETAIL, "outputTokens": 0, "totalTokens": 42}},
        ]
    if shape == "partial":
        return [
            {"type": "text-start", "id": "txt_1"},
            {"type": "text-delta", "id": "txt_1", "text": "partial "},
        ]
    if shape == "tool":
        return [
            {"type": "text-start", "id": "txt_1"},
            {"type": "tool-input-start", "id": "call_1", "toolName": "get_weather"},
            {"type": "tool-input-delta", "id": "call_1", "delta": '{"city":'},
            {"type": "tool-input-end", "id": "call_1"},
            {"type": "tool-call", "id": "call_1", "toolName": "get_weather",
             "input": '{"city":"Paris"}'},
            {"type": "finish", "finishReason": "tool-calls", "totalUsage": USAGE_DETAIL},
        ]
    if shape == "streamerr":
        return [
            {"type": "text-start", "id": "txt_1"},
            {"type": "text-delta", "id": "txt_1", "text": "before error "},
            {"type": "error", "statusCode": 429,
             "message": "<429> mid-stream rate limited"},
        ]
    if shape == "snake":
        return [
            {"type": "text-start", "id": "txt_1"},
            {"type": "text-delta", "id": "txt_1", "text": "snake "},
            {"type": "text-end", "id": "txt_1"},
            {"type": "finish", "finish_reason": "stop",
             "total_usage": {"input_tokens": 42, "output_tokens": 9,
                             "total_tokens": 51,
                             "input_token_details": {"cache_read_tokens": 30,
                                                     "cache_write_tokens": 12,
                                                     "no_cache_tokens": 12}}},
        ]
    # ok
    return [
        {"type": "text-start", "id": "txt_1"},
        {"type": "text-delta", "id": "txt_1", "text": "Hello"},
        {"type": "text-delta", "id": "txt_1", "text": " world"},
        {"type": "text-end", "id": "txt_1"},
        {"type": "reasoning-start", "id": "rsn_1"},
        {"type": "reasoning-delta", "id": "rsn_1", "text": "hmm"},
        {"type": "reasoning-end", "id": "rsn_1"},
        {"type": "finish", "finishReason": "stop", "totalUsage": USAGE_DETAIL},
    ]


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if not self.headers.get("Authorization"):
            self._send(401, {"error": "unauthorized"})
            return
        auth = self.headers.get("Authorization")
        shape = "badkey" if "badkey" in auth.lower() else shape_for_key(auth)
        path = self.path.split("?")[0]

        if path == "/alpha/whoami":
            if shape == "badkey":
                self._send(401, {"error": "unauthorized"})
                return
            self._send(200, WHOAMI)
        elif path == "/alpha/billing/credits":
            if shape == "garbage":
                body = b"<html>login page</html>"
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self._send(200, credits(shape))
        elif path == "/alpha/billing/subscriptions":
            if shape == "partial":
                self._send(500, {"error": "internal"})
                return
            self._send(200, subscriptions(shape))
        elif path == "/alpha/usage/summary":
            if shape == "partial":
                self._send(500, {"error": "internal"})
                return
            self._send(200, USAGE)
        elif path == "/provider/v1/models":
            self._send(200, {"data": [
                {"id": "deepseek/deepseek-v4-flash"},
                {"id": "deepseek/deepseek-v4-pro"},
                {"id": "claude-sonnet-4-6"},
                {"id": "gpt-5.5"},
            ]})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        path = self.path.split("?")[0]

        # 线形回归抓手：POST 必须带 CLI 版本头，否则 401
        if not self.headers.get("x-command-code-version"):
            self._send(401, {"error": "missing x-command-code-version"})
            return

        if path == "/alpha/fingerprint/record":
            print(f"[fingerprint/record] ua={self.headers.get('User-Agent')} "
                  f"ver={self.headers.get('x-command-code-version')} "
                  f"body={body.decode(errors='replace')[:300]}", flush=True)
            self._send(200, {"ok": True})
            return
        if path == "/alpha/lifecycle-events":
            print(f"[lifecycle-events] body={body.decode(errors='replace')[:300]}",
                  flush=True)
            self._send(200, {"ok": True})
            return
        if path == "/alpha/generate":
            auth = self.headers.get("Authorization") or ""
            shape = shape_for_key(auth)
            if shape == "badkey":
                self._send(401, {"error": "unauthorized"})
                return
            if shape == "forbidden":
                self._send(403, {"error": "forbidden"})
                return
            if shape == "badrequest":
                self._send(400, {"success": False,
                                 "error": {"code": "bad_request", "message": "bad messages"}})
                return
            if shape == "exhausted":
                self._send(402, {"success": False,
                                 "error": {"code": "USAGE_EXCEEDED",
                                           "message": "quota exhausted, resets at 2099-01-01T00:00:00Z"}})
                return
            if shape == "limited":
                self.send_response(429)
                self.send_header("Content-Type", "application/json")
                self.send_header("Retry-After", "3600")
                body = json.dumps({"error": "rate limited"}).encode()
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if shape == "slow":
                time.sleep(45)
                self._send(200, {"ok": True})
                return
            payload = ndjson(generate_events(shape))
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        self._send(404, {"error": "not found"})

    def _send(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 18090
    print(f"mock listening on 127.0.0.1:{port}\n"
          f"  GET  /alpha/whoami|billing/credits|billing/subscriptions|usage/summary\n"
          f"  GET  /provider/v1/models\n"
          f"  POST /alpha/fingerprint/record|lifecycle-events|generate\n"
          f"  shapes by key: zero/partial/tool/streamerr/slow/snake/exhausted/limited/"
          f"badkey/forbidden/badrequest; other=ok", flush=True)
    HTTPServer(("127.0.0.1", port), Handler).serve_forever()
