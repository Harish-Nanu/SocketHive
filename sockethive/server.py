"""Multithreaded TCP server.

Concurrency model
-----------------
* One acceptor thread owns the listening socket and admits new connections.
* Each admitted connection is served by a worker from a bounded thread pool.
  The pool has exactly ``max_connections`` workers and a semaphore holds one
  slot per live connection, so an admitted client never waits for a worker.
  Clients beyond the limit receive a ``SERVER_BUSY`` error frame and are
  disconnected instead of piling up unbounded threads.

Fault handling
--------------
* Malformed or oversized frames: the client gets a ``BAD_FRAME`` error and is
  disconnected; nothing else is affected.
* Handler exceptions: reported as ``INTERNAL_ERROR`` responses; the connection
  stays open.
* Silent clients are dropped after ``idle_timeout`` seconds.
* Resets / broken pipes are logged and cleaned up.
* Graceful shutdown: stop accepting, half-close every connection for reading
  so blocked workers wake up, let in-flight requests finish within
  ``shutdown_grace`` seconds, then force-close whatever is left.
"""

from __future__ import annotations

import itertools
import logging
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from . import __version__
from .config import ServerConfig
from .handlers import ClientContext, CommandRegistry, InvalidPayload, default_registry
from .protocol import (
    ConnectionClosed,
    ErrorCode,
    FrameTooLarge,
    ProtocolError,
    encode,
    make_error,
    make_error_response,
    make_response,
    recv_message,
)

log = logging.getLogger(__name__)

ACCEPT_POLL_INTERVAL = 0.5


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

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "uptime_seconds": round(time.monotonic() - self._started, 3),
                "connections_total": self.connections_total,
                "connections_active": self.connections_active,
                "connections_rejected": self.connections_rejected,
                "requests_total": self.requests_total,
                "requests_failed": self.requests_failed,
            }


class SocketHiveServer:
    def __init__(
        self, config: ServerConfig | None = None, registry: CommandRegistry | None = None
    ) -> None:
        self.config = config or ServerConfig()
        self.registry = registry or default_registry()
        self.stats = ServerStats()

        self._listener: socket.socket | None = None
        self._address: tuple[str, int] | None = None
        self._executor: ThreadPoolExecutor | None = None
        self._acceptor: threading.Thread | None = None
        self._slots = threading.BoundedSemaphore(self.config.max_connections)
        self._ids = itertools.count(1)

        # Live connections by client id; the condition fires when one closes.
        self._lock = threading.Lock()
        self._conn_closed = threading.Condition(self._lock)
        self._connections: dict[int, socket.socket] = {}

        self._stopping = threading.Event()
        self._stopped = threading.Event()
        self._shutdown_lock = threading.Lock()

    # ------------------------------------------------------------------ lifecycle

    @property
    def address(self) -> tuple[str, int]:
        if self._address is None:
            raise RuntimeError("server not started")
        return self._address

    def start(self) -> SocketHiveServer:
        """Bind, listen and start accepting in the background. Returns self."""
        if self._listener is not None:
            raise RuntimeError("server already started")
        cfg = self.config
        self._listener = socket.create_server((cfg.host, cfg.port), backlog=cfg.backlog)
        self._listener.settimeout(ACCEPT_POLL_INTERVAL)
        self._address = self._listener.getsockname()[:2]
        self._executor = ThreadPoolExecutor(
            max_workers=cfg.max_connections, thread_name_prefix="worker"
        )
        self._acceptor = threading.Thread(target=self._accept_loop, name="acceptor", daemon=True)
        self._acceptor.start()
        host, port = self.address
        log.info(
            "SocketHive %s listening on %s:%d (max_connections=%d, idle_timeout=%ss)",
            __version__,
            host,
            port,
            cfg.max_connections,
            cfg.idle_timeout,
        )
        return self

    def serve_forever(self) -> None:
        """Run until request_shutdown() is called (e.g. from a signal handler)."""
        if self._listener is None:
            self.start()
        self._stopping.wait()
        self.shutdown()

    def request_shutdown(self) -> None:
        """Ask serve_forever() to shut down. Safe to call from signal handlers."""
        self._stopping.set()

    def shutdown(self, grace: float | None = None) -> None:
        """Stop accepting, drain in-flight requests, then close everything."""
        with self._shutdown_lock:
            if self._stopped.is_set() or self._listener is None:
                return
            grace = self.config.shutdown_grace if grace is None else grace
            log.info("shutting down (grace period %ss)", grace)
            self._stopping.set()

            # Wakes a blocked accept() immediately on Linux; elsewhere the
            # acceptor notices _stopping within ACCEPT_POLL_INTERVAL.
            _quietly(self._listener.shutdown, socket.SHUT_RDWR)
            if self._acceptor is not None:
                self._acceptor.join()
            self._listener.close()

            # Half-close for reading: workers blocked in recv() wake up with EOF,
            # while a worker mid-request can still write its response.
            with self._lock:
                for conn in self._connections.values():
                    _quietly(conn.shutdown, socket.SHUT_RD)

                deadline = time.monotonic() + grace
                while self._connections:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    self._conn_closed.wait(remaining)

                if self._connections:
                    log.warning("force-closing %d connection(s)", len(self._connections))
                    for conn in self._connections.values():
                        _quietly(conn.shutdown, socket.SHUT_RDWR)

            assert self._executor is not None
            self._executor.shutdown(wait=False, cancel_futures=True)
            self._stopped.set()
            log.info("server stopped: %s", self.stats.snapshot())

    def __enter__(self) -> SocketHiveServer:
        return self.start()

    def __exit__(self, *exc_info: object) -> None:
        self.shutdown()

    # ------------------------------------------------------------------ accepting

    def _accept_loop(self) -> None:
        assert self._listener is not None
        while not self._stopping.is_set():
            try:
                conn, addr = self._listener.accept()
            except TimeoutError:
                continue
            except OSError as exc:
                if self._stopping.is_set():
                    break
                # e.g. EMFILE when out of file descriptors: back off, don't spin.
                log.error("accept failed: %s", exc)
                time.sleep(0.1)
                continue
            self._admit(conn, addr)

    def _admit(self, conn: socket.socket, addr: tuple[str, int]) -> None:
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        if not self._slots.acquire(blocking=False):
            self.stats.connection_rejected()
            log.warning("rejecting %s:%d: connection limit reached", *addr[:2])
            self._send_and_close(
                conn, make_error(ErrorCode.SERVER_BUSY, "connection limit reached")
            )
            return

        with self._lock:
            if self._stopping.is_set():
                self._slots.release()
                self._send_and_close(conn, make_error(ErrorCode.SHUTTING_DOWN, "server stopping"))
                return
            client_id = next(self._ids)
            self._connections[client_id] = conn
        self.stats.connection_opened()

        assert self._executor is not None
        try:
            self._executor.submit(self._serve_client, conn, addr, client_id)
        except RuntimeError:  # executor already shut down
            self._release(conn, client_id)

    def _send_and_close(self, conn: socket.socket, message: dict[str, Any]) -> None:
        conn.settimeout(1.0)
        _quietly(conn.sendall, encode(message))
        conn.close()

    # ------------------------------------------------------------------ serving

    def _serve_client(self, conn: socket.socket, addr: tuple[str, int], client_id: int) -> None:
        cfg = self.config
        peer = f"{addr[0]}:{addr[1]}"
        ctx = ClientContext(client_id=client_id, address=addr[:2], stats=self.stats.snapshot)
        log.info("client #%d connected from %s", client_id, peer)
        try:
            conn.settimeout(cfg.idle_timeout)
            self._send(conn, {
                "type": "welcome",
                "client_id": client_id,
                "server": "SocketHive",
                "version": __version__,
            })  # fmt: skip
            while True:
                try:
                    request = recv_message(conn, cfg.max_frame_size)
                except TimeoutError:
                    log.info("client #%d idle for %ss, disconnecting", client_id, cfg.idle_timeout)
                    self._send(conn, make_error(ErrorCode.IDLE_TIMEOUT, "idle timeout"))
                    break
                except ConnectionClosed:
                    break
                except ProtocolError as exc:
                    log.warning("client #%d sent a bad frame: %s", client_id, exc)
                    self._send(conn, make_error(ErrorCode.BAD_FRAME, str(exc)))
                    break

                self._send(conn, self._dispatch(request, ctx))
                if self._stopping.is_set():
                    break

            if self._stopping.is_set():
                _quietly(self._send, conn, make_error(ErrorCode.SHUTTING_DOWN, "server stopping"))
        except OSError as exc:
            log.info("client #%d connection error: %s", client_id, exc)
        except Exception:
            log.exception("client #%d: unexpected error", client_id)
        finally:
            self._release(conn, client_id)
            log.info("client #%d disconnected", client_id)

    def _dispatch(self, request: dict[str, Any], ctx: ClientContext) -> dict[str, Any]:
        request_id = request.get("id")
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

    def _fail(self, request_id: Any, code: str, message: str) -> dict[str, Any]:
        self.stats.request_done(ok=False)
        return make_error_response(request_id, code, message)

    def _send(self, conn: socket.socket, message: dict[str, Any]) -> None:
        try:
            data = encode(message, self.config.max_frame_size)
        except (TypeError, ValueError, FrameTooLarge) as exc:
            log.error("cannot encode response: %s", exc)
            data = encode(
                make_error_response(message.get("id"), ErrorCode.INTERNAL, "unencodable response")
            )
        conn.sendall(data)

    def _release(self, conn: socket.socket, client_id: int) -> None:
        conn.close()
        with self._lock:
            self._connections.pop(client_id, None)
            self._conn_closed.notify_all()
        self._slots.release()
        self.stats.connection_closed()


def _quietly(func: Any, *args: Any) -> None:
    """Call func, ignoring socket errors (best-effort cleanup paths)."""
    try:
        func(*args)
    except OSError:
        pass
