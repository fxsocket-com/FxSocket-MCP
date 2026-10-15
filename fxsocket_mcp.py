#!/usr/bin/env python3
"""Run FxSocket's MCP server as a local stdio server.

MCP clients that start their servers as a subprocess (Claude Desktop,
Docker's MCP Toolkit, Claude Code, Cursor) speak newline-delimited
JSON-RPC over stdin and stdout. This relays every message to
https://mcp.fxsocket.com/mcp as a Streamable HTTP POST carrying your API
key, and writes the server's replies back to stdout.

The tools themselves run on FxSocket, against the MT4/MT5 terminals
there. None of them are defined in this file, so the relay keeps working
unchanged when tools are added or changed.

Both protocol eras pass through: 2026-07-28 requests (protocol version in
``params._meta``, mirrored into headers here) and 2025-03-26 to
2025-11-25 sessions (``initialize`` first, then the negotiated version in
a header).

Standard library only. Configured through the environment:

    FXSOCKET_API_KEY      required; a read-only key (fxs_ro_...) is enough
    FXSOCKET_MCP_URL      default https://mcp.fxsocket.com/mcp
    FXSOCKET_MCP_TIMEOUT  seconds to wait for one reply, default 120
    FXSOCKET_MCP_LOG      error, warning, info (default) or debug

Logs go to stderr; stdout carries protocol messages only.
"""

from __future__ import annotations

import base64
import http.client
import json
import logging
import os
import re
import signal
import socket
import ssl
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlsplit

__version__ = "1.0.0"

DEFAULT_URL = "https://mcp.fxsocket.com/mcp"
DEFAULT_TIMEOUT = 120.0
KEYS_URL = "https://app.fxsocket.com/dashboard/api-key"
#: Hosts the key may be sent to over plain http (local testing).
LOCAL_HOSTS = ("localhost", "127.0.0.1", "::1", "host.docker.internal")
#: Requests in flight at once. Clients rarely run more than a few tool
#: calls in parallel, and the server rate-limits each key anyway.
WORKERS = 8
USER_AGENT = f"fxsocket-mcp/{__version__} (+https://github.com/fxsocket-com/FxSocket-MCP)"

META_VERSION = "io.modelcontextprotocol/protocolVersion"
#: Methods whose target goes into the ``Mcp-Name`` header, by param.
NAME_PARAMS = {"tools/call": "name", "prompts/get": "name", "resources/read": "uri"}
#: RFC 9110 token, the allowed shape of an ``x-mcp-header`` name.
_TOKEN = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")

PARSE_ERROR = -32700
INVALID_REQUEST = -32600
INTERNAL_ERROR = -32603

log = logging.getLogger("fxsocket-mcp")


# ── Configuration ───────────────────────────────────────────────────


class Config:
    def __init__(self, api_key: str, url: str, timeout: float):
        self.api_key = api_key
        self.url = url
        self.timeout = timeout


def load_config(env) -> Config:
    """Read the environment, or exit with a sentence saying what to fix."""
    api_key = env.get("FXSOCKET_API_KEY", "").strip()
    if not api_key:
        raise SystemExit(
            "fxsocket-mcp: FXSOCKET_API_KEY is not set. Create a read-only key at "
            f"{KEYS_URL} and pass it in, for example "
            "docker run -i --rm -e FXSOCKET_API_KEY fxsocket/mcp"
        )

    url = env.get("FXSOCKET_MCP_URL", "").strip() or DEFAULT_URL
    parts = urlsplit(url)
    if parts.scheme not in ("https", "http") or not parts.hostname:
        raise SystemExit(f"fxsocket-mcp: FXSOCKET_MCP_URL {url!r} is not an http(s) URL.")
    if parts.scheme == "http" and parts.hostname not in LOCAL_HOSTS:
        raise SystemExit(
            f"fxsocket-mcp: refusing to send the API key to {parts.hostname} over "
            "plain http. Use an https URL."
        )

    raw_timeout = env.get("FXSOCKET_MCP_TIMEOUT", "").strip()
    try:
        timeout = float(raw_timeout) if raw_timeout else DEFAULT_TIMEOUT
    except ValueError:
        timeout = 0
    if not timeout > 0:
        raise SystemExit(
            f"fxsocket-mcp: FXSOCKET_MCP_TIMEOUT {raw_timeout!r} must be a positive number of seconds."
        )
    return Config(api_key, url, timeout)


# ── Header values ───────────────────────────────────────────────────


def header_value(value: str) -> str:
    """Encode a value for ``Mcp-Name`` / ``Mcp-Param-*`` per the spec.

    Plain when it is visible ASCII (plus inner spaces and tabs) and not
    itself shaped like the sentinel; otherwise ``=?base64?...?=`` over
    its UTF-8 bytes.
    """
    plain = (
        value == value.strip(" \t")
        and all(c == "\t" or " " <= c <= "~" for c in value)
        and not (value.startswith("=?base64?") and value.endswith("?="))
    )
    if plain:
        return value
    return "=?base64?" + base64.b64encode(value.encode("utf-8")).decode("ascii") + "?="


def header_params(schema, path=()) -> list:
    """``(name, property path)`` for every ``x-mcp-header`` in a tool's
    ``inputSchema``, following ``properties`` keys only, as the spec
    requires."""
    found = []
    properties = schema.get("properties") if isinstance(schema, dict) else None
    if not isinstance(properties, dict):
        return found
    for key, sub in properties.items():
        if not isinstance(sub, dict):
            continue
        name = sub.get("x-mcp-header")
        if isinstance(name, str) and _TOKEN.match(name):
            found.append((name, path + (key,)))
        found.extend(header_params(sub, path + (key,)))
    return found


# ── Upstream HTTP ───────────────────────────────────────────────────


class Upstream:
    """POSTs to the MCP endpoint, one keep-alive connection per worker."""

    def __init__(self, url: str, timeout: float):
        parts = urlsplit(url)
        self.url = url
        self.secure = parts.scheme == "https"
        self.host = parts.hostname
        self.port = parts.port
        self.path = (parts.path or "/") + (f"?{parts.query}" if parts.query else "")
        self.timeout = timeout
        self._context = ssl.create_default_context() if self.secure else None
        self._local = threading.local()

    def connection(self) -> http.client.HTTPConnection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            if self.secure:
                conn = http.client.HTTPSConnection(
                    self.host, self.port, timeout=self.timeout, context=self._context,
                )
            else:
                conn = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
            self._local.conn = conn
        return conn

    def drop(self) -> None:
        """Forget this thread's connection, after an error or a cancel
        left it in an unknown state."""
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    def post(self, conn, body: bytes, headers: dict, cancelled) -> http.client.HTTPResponse:
        reused = conn.sock is not None
        try:
            conn.request("POST", self.path, body=body, headers=headers)
            return conn.getresponse()
        except (http.client.RemoteDisconnected, ConnectionResetError, BrokenPipeError):
            if not reused or cancelled():
                raise
            # The server closed the idle keep-alive connection before
            # reading the request. Sending it again on a fresh one is safe
            # because every FxSocket MCP tool is read-only.
            conn.close()
            conn.request("POST", self.path, body=body, headers=headers)
            return conn.getresponse()


def read_messages(resp: http.client.HTTPResponse):
    """Yield what the server sent: JSON values one by one (SSE events as
    they arrive), or a string for a body that is not JSON."""
    content_type = (resp.getheader("Content-Type") or "").split(";")[0].strip().lower()
    if content_type == "text/event-stream":
        yield from _sse_messages(resp)
        return
    raw = resp.read()
    if not raw.strip():
        return
    try:
        data = json.loads(raw)
    except ValueError:
        yield raw.decode("utf-8", "replace")
        return
    if isinstance(data, list):
        yield from data
    else:
        yield data


def _sse_messages(resp):
    data = []
    while True:
        line = resp.readline()
        if not line:
            break
        line = line.rstrip(b"\r\n")
        if not line:
            if data:
                yield from _sse_event("\n".join(data))
                data = []
            continue
        if line.startswith(b":"):
            continue  # comment / keep-alive
        field, _, value = line.partition(b":")
        if field == b"data":
            if value.startswith(b" "):
                value = value[1:]
            data.append(value.decode("utf-8", "replace"))
    if data:
        yield from _sse_event("\n".join(data))


def _sse_event(text: str):
    try:
        yield json.loads(text)
    except ValueError:
        log.warning("ignoring an SSE event that is not JSON: %.200s", text)


# ── The relay ───────────────────────────────────────────────────────


def _error(msg_id, code: int, message: str) -> dict:
    return {"jsonrpc": "2.0", "id": msg_id, "error": {"code": code, "message": message}}


def _is_reply(message) -> bool:
    return isinstance(message, dict) and "id" in message and ("result" in message or "error" in message)


class _InFlight:
    """A request the client is waiting on, so it can be cancelled."""

    def __init__(self, modern: bool):
        self.modern = modern
        self.cancelled = False
        self.conn = None


class Relay:
    def __init__(self, config: Config, write):
        self.api_key = config.api_key
        self.upstream = Upstream(config.url, config.timeout)
        self.write = write
        self._lock = threading.Lock()
        self._inflight = {}
        #: Legacy era: the version ``initialize`` settled on, sent as
        #: ``MCP-Protocol-Version`` from then on, and any session id.
        self._legacy_version = None
        self._session_id = None
        #: Tool name -> ``x-mcp-header`` params, learned from tools/list.
        self._header_params = {}

    # Entry point, called on the reading thread for every stdin line.

    def receive(self, line: bytes, submit) -> None:
        line = line.strip()
        if not line:
            return
        try:
            message = json.loads(line)
        except ValueError:
            self.write(_error(None, PARSE_ERROR, "Parse error: the line is not JSON"))
            return

        if isinstance(message, list):
            # JSON-RPC batches existed in 2025-03-26. The server takes one
            # message per POST, so split them here and answer as a batch.
            if not message:
                self.write(_error(None, INVALID_REQUEST, "Empty batch"))
                return
            for item in message:
                self._track(item)
            submit(self._batch, message)
            return
        if not isinstance(message, dict):
            self.write(_error(None, INVALID_REQUEST, "Not a JSON-RPC message"))
            return

        if message.get("method") == "notifications/cancelled" and not self._cancel(message):
            return
        self._track(message)
        submit(self._exchange, message, self.write)

    # Cancellation. On stdio the client sends notifications/cancelled;
    # on Streamable HTTP closing the request's stream is the signal.

    def _track(self, message) -> None:
        if isinstance(message, dict) and isinstance(message.get("method"), str) and "id" in message:
            with self._lock:
                self._inflight[_key(message["id"])] = _InFlight(_is_modern(message))

    def _cancel(self, message) -> bool:
        """Abandon the cancelled request. True when the notification
        should still be forwarded, which only legacy servers expect."""
        params = message.get("params")
        request_id = params.get("requestId") if isinstance(params, dict) else None
        with self._lock:
            entry = self._inflight.get(_key(request_id))
            if entry is None:
                return False
            entry.cancelled = True
            conn = entry.conn
        sock = conn.sock if conn is not None else None
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        log.debug("cancelled request %r", request_id)
        return not entry.modern

    # One message, one POST.

    def _batch(self, messages: list) -> None:
        replies = []

        def emit(message):
            if _is_reply(message):
                replies.append(message)
            else:
                self.write(message)

        for item in messages:
            if isinstance(item, dict):
                self._exchange(item, emit)
            else:
                replies.append(_error(None, INVALID_REQUEST, "Not a JSON-RPC message"))
        if replies:
            self.write(replies)

    def _exchange(self, message: dict, emit) -> None:
        method = message.get("method")
        is_request = isinstance(method, str) and "id" in message
        msg_id = message.get("id")
        key = _key(msg_id) if is_request else None
        with self._lock:
            entry = self._inflight.get(key) if is_request else None
        if entry is not None and entry.cancelled:
            self._untrack(key)
            return

        started = time.monotonic()
        status = None
        conn = self.upstream.connection()
        try:
            headers = self._headers(message)
            body = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            if entry is not None:
                with self._lock:
                    entry.conn = conn
                    if entry.cancelled:
                        return  # cancelled while this worker was starting
            resp = self.upstream.post(conn, body, headers, lambda: entry is not None and entry.cancelled)
            status = resp.status
            session_id = resp.getheader("Mcp-Session-Id")
            if session_id and method == "initialize":
                self._session_id = session_id

            replied = False
            detail = None
            for item in read_messages(resp):
                if entry is not None and entry.cancelled:
                    break
                if isinstance(item, dict) and item.get("jsonrpc") == "2.0":
                    if is_request and "error" in item and item.get("id") is None:
                        item["id"] = msg_id  # let the client match it
                    if is_request and item.get("id") == msg_id and "result" in item:
                        self._learn(method, item["result"])
                    replied = replied or (is_request and _is_reply(item) and item.get("id") == msg_id)
                    emit(item)
                else:
                    detail = _detail(item)

            if entry is not None and entry.cancelled:
                self.upstream.drop()
            elif is_request and not replied:
                emit(_error(msg_id, INTERNAL_ERROR, self._failure(status, detail, resp)))
            elif not is_request and status >= 400:
                log.warning("%s: %s", method or "message", self._failure(status, detail, resp))
        except ValueError as exc:
            # http.client refuses header values with control characters.
            self.upstream.drop()
            if is_request:
                emit(_error(msg_id, INVALID_REQUEST, f"Cannot send this request: {exc}"))
        except (OSError, http.client.HTTPException) as exc:
            self.upstream.drop()
            if entry is not None and entry.cancelled:
                pass
            elif is_request:
                emit(_error(msg_id, INTERNAL_ERROR, f"Could not reach {self.upstream.url}: {_reason(exc)}"))
            else:
                log.warning("could not deliver %s: %s", method, _reason(exc))
        finally:
            if is_request:
                self._untrack(key)
            log.debug(
                "%s id=%r -> %s in %d ms",
                method or "response", msg_id, status, (time.monotonic() - started) * 1000,
            )

    def _untrack(self, key) -> None:
        with self._lock:
            self._inflight.pop(key, None)

    def _headers(self, message: dict) -> dict:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "X-API-Key": self.api_key,
            "User-Agent": USER_AGENT,
        }
        method = message.get("method")
        params = message.get("params")
        if not isinstance(params, dict):
            params = {}
        meta = params.get("_meta")
        version = meta.get(META_VERSION) if isinstance(meta, dict) else None

        if isinstance(version, str):
            # 2026-07-28: mirror the body into the headers the server
            # checks against it.
            headers["MCP-Protocol-Version"] = version
            if isinstance(method, str):
                headers["Mcp-Method"] = method
                field = NAME_PARAMS.get(method)
                if field and isinstance(params.get(field), str):
                    headers["Mcp-Name"] = header_value(params[field])
                if method == "tools/call":
                    headers.update(self._param_headers(params))
        else:
            if self._legacy_version and method != "initialize":
                headers["MCP-Protocol-Version"] = self._legacy_version
            if self._session_id:
                headers["Mcp-Session-Id"] = self._session_id
        return headers

    def _param_headers(self, params: dict) -> dict:
        found = {}
        arguments = params.get("arguments")
        for name, path in self._header_params.get(params.get("name"), ()):
            value = arguments
            for step in path:
                value = value.get(step) if isinstance(value, dict) else None
            if isinstance(value, bool):
                text = "true" if value else "false"
            elif isinstance(value, int):
                text = str(value)
            elif isinstance(value, str):
                text = value
            else:
                continue
            found[f"Mcp-Param-{name}"] = header_value(text)
        return found

    def _learn(self, method, result) -> None:
        if not isinstance(result, dict):
            return
        if method == "initialize" and isinstance(result.get("protocolVersion"), str):
            self._legacy_version = result["protocolVersion"]
            log.debug("legacy session at protocol version %s", self._legacy_version)
        elif method == "tools/list" and isinstance(result.get("tools"), list):
            for tool in result["tools"]:
                if isinstance(tool, dict) and isinstance(tool.get("name"), str):
                    params = header_params(tool.get("inputSchema"))
                    if params:
                        self._header_params[tool["name"]] = params
                    else:
                        self._header_params.pop(tool["name"], None)

    def _failure(self, status, detail, resp) -> str:
        """A sentence for an answer that carried no JSON-RPC reply."""
        suffix = f": {detail}" if detail else ""
        if status in (401, 403):
            return (
                f"FxSocket rejected the API key (HTTP {status}{suffix}). Check "
                f"FXSOCKET_API_KEY; keys are at {KEYS_URL}"
            )
        if status == 429:
            retry = resp.getheader("Retry-After")
            wait = f" Try again in {retry} seconds." if retry and retry.isdigit() else " Wait a moment and try again."
            return f"FxSocket rate limit reached (HTTP 429{suffix}).{wait}"
        if status is not None and status < 300:
            return f"{self.upstream.url} answered HTTP {status} without a JSON-RPC reply."
        return f"{self.upstream.url} answered HTTP {status}{suffix}"


def _is_modern(message: dict) -> bool:
    params = message.get("params")
    meta = params.get("_meta") if isinstance(params, dict) else None
    return isinstance(meta, dict) and META_VERSION in meta


def _key(msg_id) -> str:
    # 1 and "1" are different JSON-RPC ids.
    return json.dumps(msg_id)


def _detail(item) -> str:
    if isinstance(item, dict) and isinstance(item.get("detail"), str):
        return item["detail"]  # Django REST framework errors
    text = item if isinstance(item, str) else json.dumps(item)
    text = " ".join(text.split())
    return text[:200] + ("..." if len(text) > 200 else "")


def _reason(exc: BaseException) -> str:
    if isinstance(exc, socket.timeout):
        return "timed out waiting for a reply (FXSOCKET_MCP_TIMEOUT)"
    return str(exc) or type(exc).__name__


# ── Process ─────────────────────────────────────────────────────────


class _Writer:
    """Writes whole lines to stdout, one thread at a time."""

    def __init__(self, stream):
        self._stream = stream
        self._lock = threading.Lock()

    def __call__(self, message) -> None:
        line = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
        with self._lock:
            try:
                self._stream.write(line)
                self._stream.flush()
            except BrokenPipeError:
                os._exit(0)  # the client is gone


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv in (["--version"], ["-V"]):
        print(__version__)
        return 0
    if argv:
        print(__doc__, file=sys.stderr)
        return 2

    level = os.environ.get("FXSOCKET_MCP_LOG", "info").strip().upper() or "INFO"
    logging.basicConfig(
        stream=sys.stderr,
        level=getattr(logging, level, logging.INFO),
        format="fxsocket-mcp %(levelname)s %(message)s",
    )
    config = load_config(os.environ)
    if not config.api_key.startswith("fxs_"):
        log.warning("FXSOCKET_API_KEY does not look like an FxSocket key (fxs_ro_... or fxs_live_...)")
    elif config.api_key.startswith("fxs_live_"):
        log.info("a read-only key (fxs_ro_...) is enough for MCP; the tools never trade")

    # As PID 1 in a container, SIGTERM is ignored unless handled.
    signal.signal(signal.SIGTERM, lambda *_: os._exit(0))

    relay = Relay(config, _Writer(sys.stdout.buffer))
    log.info("fxsocket-mcp %s relaying stdio to %s", __version__, config.url)
    with ThreadPoolExecutor(max_workers=WORKERS, thread_name_prefix="relay") as pool:
        for line in sys.stdin.buffer:
            relay.receive(line, pool.submit)
    # Leaving the pool waits for replies still in flight, then stdin's
    # EOF ends the process.
    return 0


if __name__ == "__main__":
    sys.exit(main())
