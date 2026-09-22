#!/usr/bin/env python3
"""Mock of Command Code Provider API (https://api.commandcode.ai/provider/v1) for offline testing.

Endpoints:
  GET  /provider/v1/models
  POST /provider/v1/chat/completions
  POST /provider/v1/messages
  POST /provider/v1/responses

Behavior chosen by key substring:
  ok         - normal successful response (stream or non-stream)
  zero       - response with 0 output tokens
  partial    - stream cut off / incomplete
  streamerr  - in-stream failure event
  slow       - hangs to test client abort / timeout
  exhausted  - HTTP 402 with USAGE_EXCEEDED
  limited    - HTTP 429 with Retry-After: 3600
  badkey     - HTTP 401 unauthorized
  forbidden  - HTTP 403 forbidden
  badrequest - HTTP 400 bad request

Run: python3 mock_server.py [port]   (default 18090)
"""
import json
import sys
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

MODELS = {
    "data": [
        {"id": "deepseek/deepseek-v4-flash", "object": "model"},
        {"id": "deepseek/deepseek-v4-pro", "object": "model"},
        {"id": "claude-sonnet-4-6", "object": "model"},
        {"id": "gpt-5.5", "object": "model"},
    ]
}

def shape_for_key(auth):
    key = (auth or "").lower()
    for s in ("zero", "partial", "streamerr", "slow", "exhausted", "limited", "badkey", "forbidden", "badrequest"):
        if s in key:
            return s
    return "ok"

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        auth = self.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            self._send_json(401, {"error": {"message": "missing authorization", "type": "authentication_error"}})
            return
        shape = shape_for_key(auth)
        path = self.path.split("?")[0]

        if path == "/provider/v1/models":
            if shape == "badkey":
                self._send_json(401, {"error": {"message": "invalid api key", "type": "authentication_error"}})
                return
            if shape == "limited":
                self.send_response(429)
                self.send_header("Content-Type", "application/json")
                self.send_header("Retry-After", "300")
                self.end_headers()
                self.wfile.write(json.dumps({"error": {"message": "rate limited", "type": "rate_limit_error"}}).encode())
                return
            self._send_json(200, MODELS)
            return

        self._send_json(404, {"error": {"message": "not found", "type": "invalid_request_error"}})

    def do_POST(self):
        auth = self.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            self._send_json(401, {"error": {"message": "missing authorization", "type": "authentication_error"}})
            return

        shape = shape_for_key(auth)
        if shape == "badkey":
            self._send_json(401, {"error": {"message": "invalid api key", "type": "authentication_error"}})
            return
        if shape == "forbidden":
            self._send_json(403, {"error": {"message": "forbidden", "type": "permission_error"}})
            return
        if shape == "badrequest":
            self._send_json(400, {"error": {"message": "bad request parameters", "type": "invalid_request_error"}})
            return
        if shape == "exhausted":
            self._send_json(402, {"error": {"message": "quota exhausted", "type": "rate_limit_error", "code": "USAGE_EXCEEDED"}})
            return
        if shape == "limited":
            self.send_response(429)
            self.send_header("Content-Type", "application/json")
            self.send_header("Retry-After", "3600")
            self.end_headers()
            self.wfile.write(json.dumps({"error": {"message": "rate limited", "type": "rate_limit_error"}}).encode())
            return
        if shape == "slow":
            try:
                time.sleep(30)
            except Exception:
                return
            self._send_json(200, {"ok": True})
            return

        length = int(self.headers.get("Content-Length") or 0)
        raw_body = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw_body)
        except Exception:
            body = {}

        path = self.path.split("?")[0]
        stream = body.get("stream") is True

        if path == "/provider/v1/chat/completions":
            self._handle_chat(shape, body, stream)
        elif path == "/provider/v1/messages":
            self._handle_messages(shape, body, stream)
        elif path == "/provider/v1/responses":
            self._handle_responses(shape, body, stream)
        else:
            self._send_json(404, {"error": {"message": "not found", "type": "invalid_request_error"}})

    def _handle_chat(self, shape, body, stream):
        model = body.get("model", "deepseek/deepseek-v4-flash")
        if stream:
            if shape == "partial":
                events = [
                    'data: {"id":"chatcmpl-part","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"partial content"}}]}\n\n'
                ]
                self._send_sse(events)
                return
            if shape == "streamerr":
                events = [
                    'data: {"id":"chatcmpl-err","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"before error"}}]}\n\n',
                    'data: {"error":{"message":"mid-stream chat error","type":"server_error"}}\n\n'
                ]
                self._send_sse(events)
                return
            # normal stream
            events = [
                f'data: {{"id":"chatcmpl-1","object":"chat.completion.chunk","model":"{model}","choices":[{{"index":0,"delta":{{"role":"assistant","content":""}},"finish_reason":null}}]}}\n\n',
                f'data: {{"id":"chatcmpl-1","object":"chat.completion.chunk","model":"{model}","choices":[{{"index":0,"delta":{{"content":"Hello world"}},"finish_reason":null}}]}}\n\n',
                f'data: {{"id":"chatcmpl-1","object":"chat.completion.chunk","model":"{model}","choices":[{{"index":0,"delta":{{}},"finish_reason":"stop"}}],"usage":{{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15,"prompt_tokens_details":{{"cached_tokens":4}}}}}}\n\n',
                'data: [DONE]\n\n'
            ]
            self._send_sse(events)
        else:
            resp = {
                "id": "chatcmpl-1",
                "object": "chat.completion",
                "created": int(time.time()),
                "model": model,
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "Hello world"},
                        "finish_reason": "stop"
                    }
                ],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 5,
                    "total_tokens": 15,
                    "prompt_tokens_details": {"cached_tokens": 4}
                }
            }
            self._send_json(200, resp)

    def _handle_messages(self, shape, body, stream):
        model = body.get("model", "claude-sonnet-4-6")
        if stream:
            if shape == "partial":
                events = [
                    'event: message_start\ndata: {"type":"message_start","message":{"id":"msg_part","type":"message","role":"assistant","usage":{"input_tokens":10,"output_tokens":0}}}\n\n',
                    'event: content_block_start\ndata: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}\n\n',
                    'event: content_block_delta\ndata: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"partial"}}\n\n'
                ]
                self._send_sse(events)
                return
            if shape == "streamerr":
                events = [
                    'event: message_start\ndata: {"type":"message_start","message":{"id":"msg_err","type":"message","role":"assistant","usage":{"input_tokens":10,"output_tokens":0}}}\n\n',
                    'event: error\ndata: {"type":"error","error":{"type":"server_error","message":"mid-stream anthropic error"}}\n\n'
                ]
                self._send_sse(events)
                return
            # normal stream
            events = [
                f'event: message_start\ndata: {{"type":"message_start","message":{{"id":"msg_1","type":"message","role":"assistant","model":"{model}","usage":{{"input_tokens":10,"output_tokens":1}}}}}}\n\n',
                'event: content_block_start\ndata: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}\n\n',
                'event: content_block_delta\ndata: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello world"}}\n\n',
                'event: content_block_stop\ndata: {"type":"content_block_stop","index":0}\n\n',
                'event: message_delta\ndata: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":5}}\n\n',
                'event: message_stop\ndata: {"type":"message_stop"}\n\n'
            ]
            self._send_sse(events)
        else:
            resp = {
                "id": "msg_1",
                "type": "message",
                "role": "assistant",
                "model": model,
                "content": [{"type": "text", "text": "Hello world"}],
                "stop_reason": "end_turn",
                "usage": {"input_tokens": 10, "output_tokens": 5}
            }
            self._send_json(200, resp)

    def _handle_responses(self, shape, body, stream):
        if stream:
            if shape == "partial":
                events = [
                    'event: response.created\ndata: {"type":"response.created","response":{"id":"resp_part","status":"in_progress"}}\n\n',
                    'event: response.incomplete\ndata: {"type":"response.incomplete","incomplete_details":{"reason":"max_output_tokens"}}\n\n'
                ]
                self._send_sse(events)
                return
            if shape == "streamerr":
                events = [
                    'event: response.created\ndata: {"type":"response.created","response":{"id":"resp_err","status":"in_progress"}}\n\n',
                    'event: response.failed\ndata: {"type":"response.failed","error":{"type":"server_error","message":"mid-stream response failed"}}\n\n'
                ]
                self._send_sse(events)
                return
            # normal stream
            events = [
                'event: response.created\ndata: {"type":"response.created","response":{"id":"resp_1","status":"in_progress"}}\n\n',
                'event: response.output_item.added\ndata: {"type":"response.output_item.added","output_index":0,"item":{"type":"message","role":"assistant","content":[{"type":"text","text":"Hello world"}]}}\n\n',
                'event: response.completed\ndata: {"type":"response.completed","response":{"id":"resp_1","status":"completed","usage":{"input_tokens":10,"output_tokens":5,"total_tokens":15}}}\n\n'
            ]
            self._send_sse(events)
        else:
            resp = {
                "id": "resp_1",
                "object": "response",
                "status": "completed",
                "output": [
                    {
                        "type": "message",
                        "role": "assistant",
                        "content": [{"type": "text", "text": "Hello world"}]
                    }
                ],
                "usage": {"input_tokens": 10, "output_tokens": 5, "total_tokens": 15}
            }
            self._send_json(200, resp)

    def _send_json(self, code, data):
        body = json.dumps(data).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_sse(self, event_strings):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        for evt in event_strings:
            self.wfile.write(evt.encode("utf-8"))
            self.wfile.flush()

    def log_message(self, format, *args):
        pass

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 18090
    print(f"Provider API mock listening on 127.0.0.1:{port}", flush=True)
    HTTPServer(("127.0.0.1", port), Handler).serve_forever()
