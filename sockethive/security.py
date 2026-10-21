"""Security building blocks: TLS contexts, token checks and per-IP rate limiting."""

from __future__ import annotations

import hmac
import ssl
import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from typing import Any


def server_ssl_context(certfile: str, keyfile: str) -> ssl.SSLContext:
    """TLS context for the server: TLS 1.2+ with the given certificate chain."""
    ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_cert_chain(certfile, keyfile)
    return ctx


def client_ssl_context(cafile: str | None = None, *, insecure: bool = False) -> ssl.SSLContext:
    """TLS context for the client.

    Verifies the server against ``cafile`` (or the system trust store). With
    ``insecure`` the connection is still encrypted but the server is not
    authenticated; only use that for local testing.
    """
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=cafile)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    if insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx


def token_matches(expected: str, supplied: Any) -> bool:
    """Constant-time token comparison, so response timing leaks nothing."""
    if not isinstance(supplied, str):
        return False
    return hmac.compare_digest(expected.encode(), supplied.encode())


class RateLimiter:
    """Token bucket per key (client IP): ``rate`` requests/second, bursts up to ``burst``.

    Buckets are kept in least-recently-used order and capped at ``max_keys``
    so a flood of distinct addresses cannot exhaust memory.
    """

    def __init__(
        self,
        rate: float,
        burst: int,
        *,
        max_keys: int = 10_000,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if rate <= 0 or burst < 1:
            raise ValueError("rate must be > 0 and burst >= 1")
        self.rate = rate
        self.burst = burst
        self.max_keys = max_keys
        self._clock = clock
        self._lock = threading.Lock()
        self._buckets: OrderedDict[str, tuple[float, float]] = OrderedDict()

    def acquire(self, key: str) -> float:
        """Take one token. Returns 0.0 if allowed, else seconds until one is available."""
        with self._lock:
            now = self._clock()
            tokens, last = self._buckets.pop(key, (float(self.burst), now))
            tokens = min(float(self.burst), tokens + (now - last) * self.rate)
            if tokens >= 1.0:
                tokens -= 1.0
                wait = 0.0
            else:
                wait = (1.0 - tokens) / self.rate
            self._buckets[key] = (tokens, now)
            while len(self._buckets) > self.max_keys:
                self._buckets.popitem(last=False)
            return wait
