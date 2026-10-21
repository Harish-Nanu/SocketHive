"""Configuration objects, loadable from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass, fields

ENV_PREFIX = "SOCKETHIVE_"


@dataclass(frozen=True)
class ServerConfig:
    host: str = "0.0.0.0"
    port: int = 9000
    # Upper bound on simultaneously served clients; also the worker pool size.
    max_connections: int = 64
    # Seconds a client may stay silent before the server drops it.
    idle_timeout: float = 300.0
    max_frame_size: int = 1 << 20
    backlog: int = 128
    # Seconds to wait for in-flight requests to finish during shutdown.
    shutdown_grace: float = 5.0

    def __post_init__(self) -> None:
        if not 0 <= self.port <= 65535:
            raise ValueError(f"port out of range: {self.port}")
        if self.max_connections < 1:
            raise ValueError("max_connections must be >= 1")
        if self.idle_timeout <= 0:
            raise ValueError("idle_timeout must be > 0")
        if self.max_frame_size < 64:
            raise ValueError("max_frame_size must be >= 64")

    @classmethod
    def from_env(cls, environ: dict[str, str] | None = None, **overrides) -> ServerConfig:
        """Build a config from SOCKETHIVE_* variables; explicit overrides win."""
        environ = os.environ if environ is None else environ
        values: dict[str, object] = {}
        for f in fields(cls):
            raw = environ.get(ENV_PREFIX + f.name.upper())
            if raw is not None:
                values[f.name] = _coerce(raw, type(getattr(cls, f.name)))
        values.update({k: v for k, v in overrides.items() if v is not None})
        return cls(**values)


def _coerce(raw: str, kind: type) -> object:
    if kind is int:
        return int(raw)
    if kind is float:
        return float(raw)
    return raw
