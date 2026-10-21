"""Command registry and the built-in commands the server understands.

A handler receives the request payload and a ClientContext and returns any
JSON-serialisable result. Raising InvalidPayload produces an INVALID_PAYLOAD
error response; any other exception is reported as INTERNAL_ERROR without
dropping the client's connection.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

MAX_SLEEP_SECONDS = 5.0


class InvalidPayload(ValueError):
    """The payload is the wrong shape for the requested command."""


@dataclass
class ClientContext:
    client_id: int
    address: tuple[str, int]
    stats: Callable[[], dict[str, Any]] = field(default=dict, repr=False)


Handler = Callable[[Any, ClientContext], Any]


class CommandRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, Handler] = {}

    def register(self, name: str) -> Callable[[Handler], Handler]:
        def decorator(func: Handler) -> Handler:
            if name in self._handlers:
                raise ValueError(f"command already registered: {name}")
            self._handlers[name] = func
            return func

        return decorator

    def get(self, name: str) -> Handler | None:
        return self._handlers.get(name)

    def names(self) -> list[str]:
        return sorted(self._handlers)


def default_registry() -> CommandRegistry:
    registry = CommandRegistry()

    @registry.register("ping")
    def ping(payload: Any, ctx: ClientContext) -> str:
        return "pong"

    @registry.register("echo")
    def echo(payload: Any, ctx: ClientContext) -> Any:
        return payload

    @registry.register("reverse")
    def reverse(payload: Any, ctx: ClientContext) -> str:
        if not isinstance(payload, str):
            raise InvalidPayload("reverse expects a string")
        return payload[::-1]

    @registry.register("sum")
    def total(payload: Any, ctx: ClientContext) -> float:
        if not isinstance(payload, list) or not all(
            isinstance(x, (int, float)) and not isinstance(x, bool) for x in payload
        ):
            raise InvalidPayload("sum expects a list of numbers")
        return sum(payload)

    @registry.register("time")
    def now(payload: Any, ctx: ClientContext) -> str:
        return datetime.now(timezone.utc).isoformat()

    @registry.register("sleep")
    def sleep(payload: Any, ctx: ClientContext) -> float:
        """Simulate slow work; useful for demonstrating concurrency."""
        if not isinstance(payload, (int, float)) or isinstance(payload, bool) or payload < 0:
            raise InvalidPayload("sleep expects a non-negative number of seconds")
        seconds = min(float(payload), MAX_SLEEP_SECONDS)
        time.sleep(seconds)
        return seconds

    @registry.register("whoami")
    def whoami(payload: Any, ctx: ClientContext) -> dict[str, Any]:
        return {"client_id": ctx.client_id, "address": list(ctx.address)}

    @registry.register("stats")
    def stats(payload: Any, ctx: ClientContext) -> dict[str, Any]:
        return ctx.stats()

    @registry.register("commands")
    def commands(payload: Any, ctx: ClientContext) -> list[str]:
        return registry.names()

    return registry
