"""Configuration objects, loadable from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass, field, fields

ENV_PREFIX = "SOCKETHIVE_"
SERVER_MODES = ("threaded", "async")


@dataclass(frozen=True)
class ServerConfig:
    # "threaded": one worker thread per connection.
    # "async": one asyncio event loop for all connections, handlers in a thread pool.
    mode: str = "threaded"
    host: str = "0.0.0.0"
    port: int = 9000
    # Upper bound on simultaneously served clients. In threaded mode this is
    # also the worker pool size; async mode can afford much higher values.
    max_connections: int = 64
    # Async mode only: threads available to run (possibly blocking) handlers.
    handler_workers: int = 32
    # Seconds a client may stay silent before the server drops it.
    idle_timeout: float = 300.0
    max_frame_size: int = 1 << 20
    backlog: int = 128
    # Seconds to wait for in-flight requests to finish during shutdown.
    shutdown_grace: float = 5.0
    # TLS is enabled when both a certificate and a private key are given.
    tls_cert: str = ""
    tls_key: str = ""
    # Seconds a new client has to finish the TLS handshake (and authenticate).
    handshake_timeout: float = 10.0
    # When set, clients must present this token before sending requests.
    auth_token: str = field(default="", repr=False)

    def __post_init__(self) -> None:
        if self.mode not in SERVER_MODES:
            raise ValueError(f"mode must be one of {SERVER_MODES}, got {self.mode!r}")
        if not 0 <= self.port <= 65535:
            raise ValueError(f"port out of range: {self.port}")
        if self.max_connections < 1:
            raise ValueError("max_connections must be >= 1")
        if self.handler_workers < 1:
            raise ValueError("handler_workers must be >= 1")
        if self.idle_timeout <= 0:
            raise ValueError("idle_timeout must be > 0")
        if self.max_frame_size < 64:
            raise ValueError("max_frame_size must be >= 64")
        if bool(self.tls_cert) != bool(self.tls_key):
            raise ValueError("tls_cert and tls_key must be given together")
        if self.handshake_timeout <= 0:
            raise ValueError("handshake_timeout must be > 0")

    @property
    def tls_enabled(self) -> bool:
        return bool(self.tls_cert)

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
