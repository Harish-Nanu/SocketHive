"""Transport-independent server logic shared by the threaded and asyncio servers.

Both servers own the sockets and the concurrency model; everything that
decides *what* to answer lives here, so the two behave identically.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import TimeoutError as FutureTimeout
from typing import Any

from . import __version__
from .config import ServerConfig
from .dedup import MAX_KEY_LENGTH, ResponseCache
from .handlers import ClientContext, CommandRegistry, InvalidPayload, default_registry
from .protocol import (
    ErrorCode,
    FrameTooLarge,
    encode,
    make_error,
    make_error_response,
    make_response,
)
from .security import RateLimiter, token_matches

log = logging.getLogger(__name__)

# How long a duplicate waits for the original request to finish.
DUPLICATE_WAIT_SECONDS = 30.0


class ServerStats:
    """Thread-safe counters describing server activity."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._started = time.monotonic()
        self.connections_total = 0
        self.connections_active = 0
        self.connections_rejected = 0
        self.requests_total = 0
        self.requests_failed = 0
        self.auth_failures = 0
        self.requests_rate_limited = 0
        self.requests_deduplicated = 0

    def connection_opened(self) -> None:
        with self._lock:
            self.connections_total += 1
            self.connections_active += 1

    def connection_closed(self) -> None:
        with self._lock:
            self.connections_active -= 1

    def connection_rejected(self) -> None:
        with self._lock:
            self.connections_rejected += 1

    def request_done(self, ok: bool) -> None:
        with self._lock:
            self.requests_total += 1
            if not ok:
                self.requests_failed += 1

    def deduplicated(self) -> None:
        with self._lock:
            self.requests_deduplicated += 1

    def rate_limited(self) -> None:
        with self._lock:
            self.requests_rate_limited += 1

    def auth_failed(self) -> None:
        with self._lock:
            self.auth_failures += 1

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "uptime_seconds": round(time.monotonic() - self._started, 3),
                "connections_total": self.connections_total,
                "connections_active": self.connections_active,
                "connections_rejected": self.connections_rejected,
                "requests_total": self.requests_total,
                "requests_failed": self.requests_failed,
                "requests_rate_limited": self.requests_rate_limited,
                "requests_deduplicated": self.requests_deduplicated,
                "auth_failures": self.auth_failures,
            }


class ServerCore:
    def __init__(self, config: ServerConfig, registry: CommandRegistry | None = None) -> None:
        self.config = config
        self.registry = registry or default_registry()
        self.stats = ServerStats()
        self.limiter = (
            RateLimiter(config.rate_limit, config.rate_burst) if config.rate_limit > 0 else None
        )
        self.dedup = (
            ResponseCache(config.dedup_ttl, config.dedup_max_entries)
            if config.dedup_ttl > 0
            else None
        )

    def context(self, client_id: int, address: tuple[str, int]) -> ClientContext:
        return ClientContext(client_id=client_id, address=address[:2], stats=self.stats.snapshot)

    @property
    def auth_required(self) -> bool:
        return bool(self.config.auth_token)

    def welcome(self, client_id: int) -> dict[str, Any]:
        return {
            "type": "welcome",
            "client_id": client_id,
            "server": "SocketHive",
            "version": __version__,
            "auth_required": self.auth_required,
        }

    def authenticate(self, frame: dict[str, Any], ctx: ClientContext) -> dict[str, Any]:
        """Check a client's auth frame and return the reply to send.

        Anything but ``{"type": "auth_ok"}`` means the connection must be closed.
        """
        if frame.get("type") == "auth" and token_matches(
            self.config.auth_token, frame.get("token")
        ):
            return {"type": "auth_ok"}
        self.stats.auth_failed()
        log.warning("client #%d from %s failed authentication", ctx.client_id, ctx.address[0])
        return make_error(ErrorCode.AUTH_FAILED, "authentication failed")

    def dispatch(self, request: dict[str, Any], ctx: ClientContext) -> dict[str, Any]:
        """Run one request through its handler and build the response.

        A request carrying an idempotency ``key`` the server has seen recently
        is not run again: it gets the stored response, waiting for it if the
        original is still running. Blocks for as long as the handler does;
        never raises.
        """
        request_id = request.get("id")
        key = request.get("key")
        if key is None or self.dedup is None:
            return self._execute(request, ctx)
        if not isinstance(key, str) or not 0 < len(key) <= MAX_KEY_LENGTH:
            return self._fail(
                request_id,
                ErrorCode.BAD_REQUEST,
                f"key must be a non-empty string of at most {MAX_KEY_LENGTH} characters",
            )

        future, owner = self.dedup.claim(key)
        if not owner:
            self.stats.deduplicated()
            try:
                original = future.result(timeout=DUPLICATE_WAIT_SECONDS)
            except FutureTimeout:
                return self._fail(
                    request_id, ErrorCode.INTERNAL, "original request is still running"
                )
            return {**original, "id": request_id, "replayed": True}

        try:
            response = self._execute(request, ctx)
        except BaseException:
            self.dedup.release(key, future, make_error_response(request_id, ErrorCode.INTERNAL, ""))
            raise
        if response.get("error", {}).get("code") == ErrorCode.RATE_LIMITED:
            # Never ran, so don't remember it: the client's retry must run.
            self.dedup.release(key, future, response)
        else:
            self.dedup.complete(future, response)
        return response

    def _execute(self, request: dict[str, Any], ctx: ClientContext) -> dict[str, Any]:
        request_id = request.get("id")
        if self.limiter is not None:
            wait = self.limiter.acquire(ctx.address[0])
            if wait:
                self.stats.rate_limited()
                return self._fail(
                    request_id,
                    ErrorCode.RATE_LIMITED,
                    "too many requests",
                    retry_after=round(wait, 3),
                )

        command = request.get("command")
        if request.get("type") != "request" or not isinstance(command, str):
            return self._fail(
                request_id, ErrorCode.BAD_REQUEST, "expected a request with a command"
            )

        handler = self.registry.get(command)
        if handler is None:
            return self._fail(request_id, ErrorCode.UNKNOWN_COMMAND, f"unknown command: {command}")

        try:
            result = handler(request.get("payload"), ctx)
        except InvalidPayload as exc:
            return self._fail(request_id, ErrorCode.INVALID_PAYLOAD, str(exc))
        except Exception:
            log.exception("handler for %r failed (client #%d)", command, ctx.client_id)
            return self._fail(request_id, ErrorCode.INTERNAL, f"command {command!r} failed")

        self.stats.request_done(ok=True)
        return make_response(request_id, result)

    def encode(self, message: dict[str, Any]) -> bytes:
        """Encode an outgoing frame, substituting an error if it cannot be sent."""
        try:
            return encode(message, self.config.max_frame_size)
        except (TypeError, ValueError, FrameTooLarge) as exc:
            log.error("cannot encode response: %s", exc)
            return encode(
                make_error_response(message.get("id"), ErrorCode.INTERNAL, "unencodable response")
            )

    def _fail(self, request_id: Any, code: str, message: str, **details: Any) -> dict[str, Any]:
        self.stats.request_done(ok=False)
        return make_error_response(request_id, code, message, **details)
