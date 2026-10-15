"""End-to-end tests: the relay runs as a real subprocess, talking stdio
to the test and HTTP to a stub MCP server on 127.0.0.1.

Run with ``python -m unittest discover -s tests``.
"""

import base64
import json
import os
import queue
import subprocess
import sys
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import fxsocket_mcp  # noqa: E402

KEY = "fxs_ro_test"
MODERN = "2026-07-28"
META = "io.modelcontextprotocol/protocolVersion"
CAPS = "io.modelcontextprotocol/clientCapabilities"


def decode(value):
    if value and value.startswith("=?base64?") and value.endswith("?="):
        return base64.b64decode(value[9:-2]).decode("utf-8")
    return value


class StubServer(BaseHTTPRequestHandler):
    """Enough of FxSocket's MCP endpoint to exercise the relay: API-key
    auth, both eras, the 2026-07-28 header checks, and a few tools that
    answer slowly, stream, or fail."""

    requests = []  # (headers, body) of every POST, for assertions

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        type(self).requests.append((dict(self.headers.items()), body))
        if self.headers.get("X-API-Key") != KEY:
            return self._json(401, {"detail": "Invalid API key."})
        if "id" not in body:
            return self._send(202, b"", None)

        msg_id, method = body["id"], body["method"]
        params = body.get("params", {})
        meta = params.get("_meta", {})
        if META in meta:
            problem = self._header_problem(body, meta)
            if problem:
                return self._json(400, {"jsonrpc": "2.0", "id": msg_id,
                                        "error": {"code": -32020, "message": problem}})

        if method == "initialize":
            self.send_response(200)
            self.send_header("Mcp-Session-Id", "session-1")
            return self._json(200, self._ok(msg_id, {"protocolVersion": "2025-11-25"}), sent_status=True)
        if method == "tools/list":
            return self._json(200, self._ok(msg_id, {"tools": [
                {"name": "echo", "inputSchema": {"type": "object"}},
                {"name": "regional", "inputSchema": {"type": "object", "properties": {
                    "region": {"type": "string", "x-mcp-header": "Region"},
                    "opts": {"type": "object", "properties": {
                        "verbose": {"type": "boolean", "x-mcp-header": "Verbose"},
                    }},
                }}},
            ]}))
        if method != "tools/call":
            return self._json(200, self._ok(msg_id, {}))

        name = params.get("name")
        if name == "slow":
            time.sleep(float(params.get("arguments", {}).get("seconds", 1)))
        if name == "throttled":
            self.send_response(429)
            self.send_header("Retry-After", "7")
            return self._json(429, {"detail": "Request was throttled."}, sent_status=True)
        if name == "bad_gateway":
            return self._send(502, b"<html><body><h1>502 Bad Gateway</h1></body></html>", "text/html")
        if name == "no_id_error":
            return self._json(400, {"jsonrpc": "2.0", "error": {"code": -32600, "message": "nope"}})
        if name == "stream":
            events = [
                {"jsonrpc": "2.0", "method": "notifications/progress", "params": {"progress": 1}},
                self._ok(msg_id, {"content": [{"type": "text", "text": "done"}]}),
            ]
            raw = b": keep-alive\n\n" + b"".join(
                b"event: message\ndata: " + json.dumps(e).encode() + b"\n\n" for e in events
            )
            return self._send(200, raw, "text/event-stream")
        # Default: echo what arrived so tests can look at the headers.
        headers = {k.lower(): v for k, v in self.headers.items()}
        return self._json(200, self._ok(msg_id, {"structuredContent": {"headers": headers, "name": name}}))

    def _header_problem(self, body, meta):
        if self.headers.get("MCP-Protocol-Version") != meta[META]:
            return "MCP-Protocol-Version mismatch"
        if self.headers.get("Mcp-Method") != body["method"]:
            return "Mcp-Method mismatch"
        if body["method"] == "tools/call" and decode(self.headers.get("Mcp-Name")) != body["params"]["name"]:
            return "Mcp-Name mismatch"
        return None

    @staticmethod
    def _ok(msg_id, result):
        return {"jsonrpc": "2.0", "id": msg_id, "result": result}

    def _json(self, status, payload, sent_status=False):
        self._send(status, json.dumps(payload).encode(), "application/json", sent_status)

    def _send(self, status, raw, content_type, sent_status=False):
        if not sent_status:
            self.send_response(status)
        if content_type:
            self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, *args):
        pass


class RelayProcess:
    """The relay as a subprocess, with its stdout read on a thread."""

    def __init__(self, url, key=KEY, extra_env=None):
        env = {k: v for k, v in os.environ.items() if not k.startswith("FXSOCKET_")}
        env.update({"FXSOCKET_MCP_URL": url, "FXSOCKET_MCP_LOG": "warning"})
        if key is not None:
            env["FXSOCKET_API_KEY"] = key
        env.update(extra_env or {})
        self.proc = subprocess.Popen(
            [sys.executable, str(ROOT / "fxsocket_mcp.py")],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env,
        )
        self.lines = queue.Queue()
        threading.Thread(target=self._pump, daemon=True).start()

    def _pump(self):
        for line in self.proc.stdout:
            self.lines.put(json.loads(line))

    def send(self, *messages):
        for message in messages:
            raw = message if isinstance(message, bytes) else json.dumps(message).encode()
            self.proc.stdin.write(raw + b"\n")
        self.proc.stdin.flush()

    def read(self, timeout=5.0):
        return self.lines.get(timeout=timeout)

    def assert_silent(self, seconds):
        try:
            extra = self.lines.get(timeout=seconds)
        except queue.Empty:
            return
        raise AssertionError(f"unexpected output: {extra}")

    def close(self):
        if not self.proc.stdin.closed:
            try:
                self.proc.stdin.close()
            except BrokenPipeError:
                pass
        if self.proc.poll() is None:
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.proc.stdout.close()
        self.proc.stderr.close()


def call(msg_id, name, arguments=None, modern=True):
    params = {"name": name, "arguments": arguments or {}}
    if modern:
        params["_meta"] = {META: MODERN, CAPS: {}}
    return {"jsonrpc": "2.0", "id": msg_id, "method": "tools/call", "params": params}


class RelayTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), StubServer)
        cls.server.daemon_threads = True
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        cls.url = f"http://127.0.0.1:{cls.server.server_address[1]}/mcp"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self):
        StubServer.requests.clear()

    def relay(self, **kwargs):
        relay = RelayProcess(self.url, **kwargs)
        self.addCleanup(relay.close)
        return relay

    def headers_of(self, method):
        return [h for h, b in StubServer.requests if b.get("method") == method]

    # Modern era

    def test_modern_request_mirrors_body_into_headers(self):
        relay = self.relay()
        relay.send(call(1, "echo"))
        reply = relay.read()
        self.assertEqual(reply["id"], 1)
        headers = reply["result"]["structuredContent"]["headers"]
        self.assertEqual(headers["mcp-protocol-version"], MODERN)
        self.assertEqual(headers["mcp-method"], "tools/call")
        self.assertEqual(headers["mcp-name"], "echo")
        self.assertEqual(headers["x-api-key"], KEY)
        self.assertIn("text/event-stream", headers["accept"])
        self.assertTrue(headers["user-agent"].startswith("fxsocket-mcp/"))

    def test_non_ascii_tool_name_is_base64_encoded(self):
        relay = self.relay()
        relay.send(call(1, "kurs_übersicht"))
        headers = relay.read()["result"]["structuredContent"]["headers"]
        self.assertTrue(headers["mcp-name"].startswith("=?base64?"))
        self.assertEqual(decode(headers["mcp-name"]), "kurs_übersicht")

    def test_x_mcp_header_params_are_mirrored_after_tools_list(self):
        relay = self.relay()
        relay.send({"jsonrpc": "2.0", "id": 1, "method": "tools/list",
                    "params": {"_meta": {META: MODERN, CAPS: {}}}})
        relay.read()
        relay.send(call(2, "regional", {"region": "eu west ", "opts": {"verbose": True}}))
        headers = relay.read()["result"]["structuredContent"]["headers"]
        self.assertEqual(decode(headers["mcp-param-region"]), "eu west ")
        self.assertTrue(headers["mcp-param-region"].startswith("=?base64?"))
        self.assertEqual(headers["mcp-param-verbose"], "true")

    # Legacy era

    def test_legacy_session_sends_negotiated_version_and_session_id(self):
        relay = self.relay()
        relay.send({"jsonrpc": "2.0", "id": 1, "method": "initialize",
                    "params": {"protocolVersion": "2025-11-25", "capabilities": {}}})
        self.assertEqual(relay.read()["result"]["protocolVersion"], "2025-11-25")
        relay.send({"jsonrpc": "2.0", "method": "notifications/initialized"},
                   call(2, "echo", modern=False))
        headers = relay.read()["result"]["structuredContent"]["headers"]
        self.assertEqual(headers["mcp-protocol-version"], "2025-11-25")
        self.assertEqual(headers["mcp-session-id"], "session-1")
        self.assertNotIn("mcp-method", headers)
        init_headers = {k.lower() for k in self.headers_of("initialize")[0]}
        self.assertNotIn("mcp-protocol-version", init_headers)
        self.assertEqual(len(self.headers_of("notifications/initialized")), 1)

    def test_batch_is_split_and_answered_as_a_batch(self):
        relay = self.relay()
        relay.send([call(1, "echo", modern=False), call(2, "echo", modern=False),
                    {"jsonrpc": "2.0", "method": "notifications/initialized"}])
        replies = relay.read()
        self.assertEqual(sorted(r["id"] for r in replies), [1, 2])

    # Errors the client can show

    def test_rejected_key_becomes_a_json_rpc_error(self):
        relay = self.relay(key="fxs_ro_wrong")
        relay.send(call("a", "echo"))
        reply = relay.read()
        self.assertEqual(reply["id"], "a")
        self.assertIn("rejected the API key", reply["error"]["message"])
        self.assertIn("Invalid API key.", reply["error"]["message"])

    def test_rate_limit_says_when_to_retry(self):
        relay = self.relay()
        relay.send(call(1, "throttled"))
        message = relay.read()["error"]["message"]
        self.assertIn("rate limit", message)
        self.assertIn("7 seconds", message)

    def test_html_error_page_is_summarised(self):
        relay = self.relay()
        relay.send(call(1, "bad_gateway"))
        message = relay.read()["error"]["message"]
        self.assertIn("HTTP 502", message)
        self.assertIn("502 Bad Gateway", message)

    def test_error_without_id_gets_the_request_id(self):
        relay = self.relay()
        relay.send(call(9, "no_id_error"))
        reply = relay.read()
        self.assertEqual(reply["id"], 9)
        self.assertEqual(reply["error"]["message"], "nope")

    def test_unreachable_server_becomes_a_json_rpc_error(self):
        relay = RelayProcess("http://127.0.0.1:9/mcp")
        self.addCleanup(relay.close)
        relay.send(call(1, "echo"))
        self.assertIn("Could not reach", relay.read()["error"]["message"])

    def test_line_that_is_not_json(self):
        relay = self.relay()
        relay.send(b"{not json")
        reply = relay.read()
        self.assertIsNone(reply["id"])
        self.assertEqual(reply["error"]["code"], -32700)

    # Streaming, concurrency, cancellation, shutdown

    def test_sse_reply_is_relayed_event_by_event(self):
        relay = self.relay()
        relay.send(call(1, "stream"))
        self.assertEqual(relay.read()["method"], "notifications/progress")
        self.assertEqual(relay.read()["result"]["content"][0]["text"], "done")

    def test_requests_run_concurrently(self):
        relay = self.relay()
        started = time.monotonic()
        relay.send(*(call(i, "slow", {"seconds": 1}) for i in range(4)))
        ids = sorted(relay.read()["id"] for _ in range(4))
        self.assertEqual(ids, [0, 1, 2, 3])
        self.assertLess(time.monotonic() - started, 2.5)

    def test_cancelled_request_gets_no_reply(self):
        relay = self.relay()
        relay.send(call(1, "slow", {"seconds": 2}))
        time.sleep(0.3)
        relay.send({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": 1}},
                   call(2, "echo"))
        self.assertEqual(relay.read()["id"], 2)
        relay.assert_silent(2.5)
        # A modern request is cancelled by closing its stream, so the
        # notification itself is not forwarded.
        self.assertEqual(self.headers_of("notifications/cancelled"), [])

    def test_eof_waits_for_replies_in_flight(self):
        relay = self.relay()
        relay.send(call(1, "slow", {"seconds": 1}))
        relay.proc.stdin.close()
        self.assertEqual(relay.read(timeout=5)["id"], 1)
        self.assertEqual(relay.proc.wait(timeout=5), 0)

    # Configuration

    def test_missing_key_exits_with_instructions(self):
        relay = RelayProcess(self.url, key=None)
        self.addCleanup(relay.close)
        self.assertEqual(relay.proc.wait(timeout=5), 1)
        self.assertIn(b"FXSOCKET_API_KEY is not set", relay.proc.stderr.read())

    def test_plain_http_to_a_remote_host_is_refused(self):
        relay = RelayProcess("http://example.com/mcp")
        self.addCleanup(relay.close)
        self.assertEqual(relay.proc.wait(timeout=5), 1)
        self.assertIn(b"plain http", relay.proc.stderr.read())


class HeaderValueTests(unittest.TestCase):
    def test_spec_examples(self):
        cases = {
            "us-west1": "us-west1",
            "Hello, 世界": "=?base64?SGVsbG8sIOS4lueVjA==?=",
            " padded ": "=?base64?IHBhZGRlZCA=?=",
            "line1\nline2": "=?base64?bGluZTEKbGluZTI=?=",
            "=?base64?literal?=": "=?base64?PT9iYXNlNjQ/bGl0ZXJhbD89?=",
        }
        for value, expected in cases.items():
            with self.subTest(value=value):
                self.assertEqual(fxsocket_mcp.header_value(value), expected)


if __name__ == "__main__":
    unittest.main()
