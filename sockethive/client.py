"""Thread-safe client with retries, automatic reconnection and heartbeats.

Delivery semantics: if the connection drops while a request is in flight, the
client reconnects and resends it once with the same idempotency key. A server
with deduplication enabled (the default) runs it only once and answers the
retry with the original response. Timeouts are not retried automatically;
call request() again with the same ``idempotency_key`` to retry one safely.
"""

from __future__ import annotations

import itertools
import logging
import random
import socket
import ssl
import threading
import time
import uuid
from typing import Any

from .protocol import (
    MAX_FRAME_SIZE,
    ConnectionClosed,
    ErrorCode,
    ProtocolError,
    make_request,
    recv_message,
    send_message,
)

log = logging.getLogger(__name__)


class RemoteError(Exception):
    """The server answered a request with an error response."""

    def __init__(self, code: str, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.details = details or {}

    @property
    def retry_after(self) -> float | None:
        """Seconds the server asked us to wait (set on RATE_LIMITED errors)."""
        value = self.details.get("retry_after")
        return float(value) if isinstance(value, (int, float)) else None


class ServerClosedConnection(RemoteError):
    """The server sent an error frame and is closing the connection."""


class AuthenticationError(RemoteError):
    """The server rejected (or requires) an auth token. Never retried."""


class RequestTimeout(TimeoutError):
    """No response arrived within the client's timeout."""


class SocketHiveClient:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9000,
        *,
        timeout: float = 10.0,
        connect_timeout: float = 5.0,
        retries: int = 5,
        backoff: float = 0.2,
        max_backoff: float = 5.0,
        auto_reconnect: bool = True,
        max_frame_size: int = MAX_FRAME_SIZE,
        ssl_context: ssl.SSLContext | None = None,
        server_hostname: str | None = None,
        auth_token: str | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
        self.connect_timeout = connect_timeout
        self.retries = retries
        self.backoff = backoff
        self.max_backoff = max_backoff
        self.auto_reconnect = auto_reconnect
        self.max_frame_size = max_frame_size
        self.ssl_context = ssl_context
        # Name the server certificate must match; defaults to ``host``.
        self.server_hostname = server_hostname or host
        self._auth_token = auth_token

        self.client_id: int | None = None
        self.server_info: dict[str, Any] = {}

        self._sock: socket.socket | None = None
        # Reentrant: request() may call connect() while holding it.
        self._lock = threading.RLock()
        self._ids = itertools.count(1)
        # Prefix for idempotency keys: unique per client, stable across reconnects.
        self.session_id = uuid.uuid4().hex
        self._last_activity = time.monotonic()
        self._heartbeat: threading.Thread | None = None
        self._heartbeat_stop = threading.Event()

    # ------------------------------------------------------------------ connection

    @property
    def connected(self) -> bool:
        return self._sock is not None

    def connect(self) -> SocketHiveClient:
        """Connect, retrying with exponential backoff and jitter."""
        with self._lock:
            if self._sock is not None:
                return self
            last_exc: Exception | None = None
            for attempt in range(self.retries + 1):
                try:
                    self._open()
                    return self
                except ssl.SSLCertVerificationError:
                    raise  # an untrusted server will not become trusted by retrying
                except (OSError, ConnectionClosed, ProtocolError, ServerClosedConnection) as exc:
                    last_exc = exc
                    if attempt == self.retries:
                        break
                    delay = min(self.max_backoff, self.backoff * 2**attempt)
                    delay *= random.uniform(0.5, 1.0)
                    log.warning(
                        "connect to %s:%d failed (%s); retry %d/%d in %.2fs",
                        self.host,
                        self.port,
                        exc,
                        attempt + 1,
                        self.retries,
                        delay,
                    )
                    time.sleep(delay)
            raise ConnectionError(
                f"could not connect to {self.host}:{self.port} "
                f"after {self.retries + 1} attempt(s): {last_exc}"
            ) from last_exc

    def _open(self) -> None:
        sock = socket.create_connection((self.host, self.port), timeout=self.connect_timeout)
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            if self.ssl_context is not None:
                sock = self.ssl_context.wrap_socket(sock, server_hostname=self.server_hostname)
            sock.settimeout(self.timeout)
            welcome = recv_message(sock, self.max_frame_size)
            if welcome.get("type") == "error":
                raise _closed_error(welcome)
            if welcome.get("type") != "welcome":
                raise ProtocolError(f"expected welcome frame, got {welcome.get('type')!r}")
            if welcome.get("auth_required"):
                self._authenticate(sock)
        except BaseException:
            sock.close()
            raise
        self._sock = sock
        self.client_id = welcome.get("client_id")
        self.server_info = welcome
        self._last_activity = time.monotonic()
        log.debug("connected to %s:%d as client #%s", self.host, self.port, self.client_id)

    def _authenticate(self, sock: socket.socket) -> None:
        if self._auth_token is None:
            raise AuthenticationError(ErrorCode.AUTH_FAILED, "server requires an auth token")
        send_message(sock, {"type": "auth", "token": self._auth_token}, self.max_frame_size)
        reply = recv_message(sock, self.max_frame_size)
        if reply.get("type") == "error":
            error = reply.get("error") or {}
            raise AuthenticationError(
                error.get("code", ErrorCode.AUTH_FAILED), error.get("message", "")
            )
        if reply.get("type") != "auth_ok":
            raise ProtocolError(f"expected auth_ok, got {reply.get('type')!r}")

    def close(self) -> None:
        self.stop_heartbeat()
        with self._lock:
            self._drop()

    def _drop(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            finally:
                self._sock = None

    def __enter__(self) -> SocketHiveClient:
        return self.connect()

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # ------------------------------------------------------------------ requests

    def request(
        self, command: str, payload: Any = None, *, idempotency_key: str | None = None
    ) -> Any:
        """Send a command and return its result.

        Raises RemoteError if the server rejects the request, RequestTimeout if
        it does not answer in time and ConnectionError if the server is
        unreachable.
        """
        with self._lock:
            # The same id and key are reused if we resend after a dropped
            # connection, so the server can recognise the retry.
            request_id = next(self._ids)
            key = idempotency_key or f"{self.session_id}:{request_id}"
            attempts = 2 if self.auto_reconnect else 1
            for attempt in range(attempts):
                self.connect()
                try:
                    return self._roundtrip(request_id, key, command, payload)
                except TimeoutError as exc:
                    # The stream may still deliver a late response; start fresh.
                    self._drop()
                    raise RequestTimeout(f"{command!r} timed out after {self.timeout}s") from exc
                except (OSError, ConnectionClosed, ProtocolError, ServerClosedConnection) as exc:
                    self._drop()
                    if attempt + 1 == attempts:
                        raise ConnectionError(f"{command!r} failed: {exc}") from exc
                    log.warning("connection lost (%s); reconnecting", exc)
            raise AssertionError("unreachable")

    def _roundtrip(self, request_id: int, key: str, command: str, payload: Any) -> Any:
        assert self._sock is not None
        request = make_request(request_id, command, payload, key=key)
        send_message(self._sock, request, self.max_frame_size)
        reply = recv_message(self._sock, self.max_frame_size)
        self._last_activity = time.monotonic()

        if reply.get("type") == "error":
            raise _closed_error(reply)
        if reply.get("type") != "response" or reply.get("id") != request_id:
            raise ProtocolError(f"unexpected reply to request {request_id}: {reply!r}")
        if reply.get("ok"):
            return reply.get("result")
        error = dict(reply.get("error") or {})
        raise RemoteError(
            error.pop("code", ErrorCode.INTERNAL), error.pop("message", ""), details=error
        )

    def ping(self) -> float:
        """Round-trip a ping and return the latency in seconds."""
        start = time.perf_counter()
        self.request("ping")
        return time.perf_counter() - start

    # ------------------------------------------------------------------ heartbeat

    def start_heartbeat(self, interval: float = 30.0) -> None:
        """Ping in the background whenever the connection has been quiet for
        ``interval`` seconds, so the server's idle timeout never fires and
        dead connections are detected (and re-established) early."""
        if self._heartbeat is not None:
            return
        self._heartbeat_stop.clear()
        self._heartbeat = threading.Thread(
            target=self._heartbeat_loop, args=(interval,), name="heartbeat", daemon=True
        )
        self._heartbeat.start()

    def stop_heartbeat(self) -> None:
        if self._heartbeat is None:
            return
        self._heartbeat_stop.set()
        if self._heartbeat is not threading.current_thread():
            self._heartbeat.join()
        self._heartbeat = None

    def _heartbeat_loop(self, interval: float) -> None:
        while not self._heartbeat_stop.wait(interval / 4):
            if time.monotonic() - self._last_activity < interval:
                continue
            try:
                self.ping()
            except Exception as exc:
                log.warning("heartbeat failed: %s", exc)
                self._last_activity = time.monotonic()  # don't hammer a dead server


def _closed_error(frame: dict[str, Any]) -> ServerClosedConnection:
    error = frame.get("error") or {}
    return ServerClosedConnection(error.get("code", ErrorCode.INTERNAL), error.get("message", ""))
