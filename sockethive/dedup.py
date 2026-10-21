"""Server-side memory of recent requests, so a retried request runs only once.

Clients tag each request with an idempotency key. The first request with a
given key runs; any repeat within ``ttl`` seconds receives the stored
response instead, or waits for it if the original is still running (the
usual case when a connection drops mid-request and the client resends on a
fresh connection).
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from concurrent.futures import Future
from typing import Any

MAX_KEY_LENGTH = 128

Response = dict[str, Any]


class ResponseCache:
    def __init__(
        self,
        ttl: float,
        max_entries: int,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if ttl <= 0 or max_entries < 1:
            raise ValueError("ttl must be > 0 and max_entries >= 1")
        self.ttl = ttl
        self.max_entries = max_entries
        self._clock = clock
        self._lock = threading.Lock()
        # key -> (future response, time claimed); insertion order == age order.
        self._entries: OrderedDict[str, tuple[Future[Response], float]] = OrderedDict()

    def claim(self, key: str) -> tuple[Future[Response], bool]:
        """Return the key's future and whether the caller owns it.

        The owner must eventually call complete() or release(); everyone else
        waits on the future.
        """
        with self._lock:
            now = self._clock()
            self._expire(now)
            entry = self._entries.get(key)
            if entry is not None:
                return entry[0], False
            future: Future[Response] = Future()
            self._entries[key] = (future, now)
            while len(self._entries) > self.max_entries:
                self._entries.popitem(last=False)
            return future, True

    def complete(self, future: Future[Response], response: Response) -> None:
        """Store the response for later duplicates and wake current waiters."""
        future.set_result(response)

    def release(self, key: str, future: Future[Response], response: Response) -> None:
        """Wake current waiters with ``response`` but forget the key, so the
        next attempt runs again (used when the request was never executed)."""
        with self._lock:
            entry = self._entries.get(key)
            if entry is not None and entry[0] is future:
                del self._entries[key]
        future.set_result(response)

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    def _expire(self, now: float) -> None:
        while self._entries:
            _, (_, claimed) = next(iter(self._entries.items()))
            if now - claimed < self.ttl:
                break
            self._entries.popitem(last=False)
