"""asyncio-based TCP server.

Concurrency model
-----------------
* A single event loop (in its own thread) multiplexes every connection, so an
  idle client costs a coroutine and a buffer instead of an OS thread. This
  mode scales to thousands of mostly-idle connections.
* Handlers are ordinary blocking functions, so each request is dispatched to a
  bounded thread pool (``handler_workers``) and the loop never stalls on a
  slow command.

The public interface (start, serve_forever, request_shutdown, shutdown,
address, stats, context manager) and the wire behaviour are identical to
:class:`sockethive.server.SocketHiveServer`, including connection limits,
idle timeouts, fault isolation and graceful shutdown.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import logging
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from . import __version__
from .config import ServerConfig
from .core import ServerCore
from .handlers import CommandRegistry
from .protocol import ConnectionClosed, ErrorCode, ProtocolError, make_error, read_message
from .security import server_ssl_context

log = logging.getLogger(__name__)


class _Stopping(Exception):
    """The server began shutting down while a connection was idle."""


class AsyncSocketHiveServer:
    def __init__(
        self, config: ServerConfig | None = None, registry: CommandRegistry | None = None
    ) -> None:
        self.config = config or ServerConfig(mode="async")
        self.core = ServerCore(self.config, registry)
        self.registry = self.core.registry
        self.stats = self.core.stats
        self._ssl = (
            server_ssl_context(self.config.tls_cert, self.config.tls_key)
            if self.config.tls_enabled
            else None
        )

        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._server: asyncio.base_events.Server | None = None
        self._address: tuple[str, int] | None = None
        self._executor = ThreadPoolExecutor(
            max_workers=self.config.handler_workers, thread_name_prefix="handler"
        )
        self._ids = itertools.count(1)

        # Owned by the event loop thread.
        self._tasks: set[asyncio.Task[None]] = set()
        self._stop_signal: asyncio.Future[None] | None = None

        self._stopping = threading.Event()
        self._stopped = threading.Event()
        self._shutdown_lock = threading.Lock()

    # ------------------------------------------------------------------ lifecycle

    @property
    def address(self) -> tuple[str, int]:
        if self._address is None:
            raise RuntimeError("server not started")
        return self._address

    def start(self) -> AsyncSocketHiveServer:
        """Bind, listen and run the event loop in a background thread."""
        if self._loop is not None:
            raise RuntimeError("server already started")
        self._loop = asyncio.new_event_loop()
        ready = threading.Event()
        failure: list[BaseException] = []

        def run() -> None:
            assert self._loop is not None
            asyncio.set_event_loop(self._loop)
            try:
                self._loop.run_until_complete(self._open())
            except BaseException as exc:
                failure.append(exc)
                ready.set()
                self._loop.close()
                return
            ready.set()
            self._loop.run_forever()
            self._loop.run_until_complete(self._loop.shutdown_asyncgens())
            self._loop.close()

        self._thread = threading.Thread(target=run, name="event-loop", daemon=True)
        self._thread.start()
        ready.wait()
        if failure:
            self._executor.shutdown(wait=False)
            raise failure[0]

        cfg = self.config
        host, port = self.address
        log.info(
            "SocketHive %s (async) listening on %s:%d%s "
            "(max_connections=%d, handler_workers=%d, idle_timeout=%ss)",
            __version__,
            host,
            port,
            " with TLS" if self._ssl else "",
            cfg.max_connections,
            cfg.handler_workers,
            cfg.idle_timeout,
        )
        return self

    async def _open(self) -> None:
        cfg = self.config
        self._stop_signal = asyncio.get_running_loop().create_future()
        self._server = await asyncio.start_server(
            self._handle_client,
            cfg.host,
            cfg.port,
            backlog=cfg.backlog,
            ssl=self._ssl,
            ssl_handshake_timeout=cfg.handshake_timeout if self._ssl else None,
        )
        self._address = self._server.sockets[0].getsockname()[:2]

    def serve_forever(self) -> None:
        """Run until request_shutdown() is called (e.g. from a signal handler)."""
        if self._loop is None:
            self.start()
        self._stopping.wait()
        self.shutdown()

    def request_shutdown(self) -> None:
        """Ask serve_forever() to shut down. Safe to call from signal handlers."""
        self._stopping.set()

    def shutdown(self, grace: float | None = None) -> None:
        """Stop accepting, drain in-flight requests, then close everything."""
        with self._shutdown_lock:
            if self._stopped.is_set() or self._loop is None or self._thread is None:
                return
            grace = self.config.shutdown_grace if grace is None else grace
            log.info("shutting down (grace period %ss)", grace)
            self._stopping.set()
            asyncio.run_coroutine_threadsafe(self._drain(grace), self._loop).result()
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join()
            self._executor.shutdown(wait=False, cancel_futures=True)
            self._stopped.set()
            log.info("server stopped: %s", self.stats.snapshot())

    async def _drain(self, grace: float) -> None:
        assert self._server is not None and self._stop_signal is not None
        self._server.close()  # stop accepting; existing connections stay open
        if not self._stop_signal.done():
            self._stop_signal.set_result(None)  # wakes every idle connection

        if self._tasks:
            _, pending = await asyncio.wait(set(self._tasks), timeout=grace)
            if pending:
                log.warning("force-closing %d connection(s)", len(pending))
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

        # Python 3.13+: also drop connections still in their TLS handshake,
        # which have no handler task yet.
        abort_clients = getattr(self._server, "abort_clients", None)
        if abort_clients is not None:
            abort_clients()

    def __enter__(self) -> AsyncSocketHiveServer:
        return self.start()

    def __exit__(self, *exc_info: object) -> None:
        self.shutdown()

    # ------------------------------------------------------------------ serving

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        addr = writer.get_extra_info("peername")[:2]
        sock = writer.get_extra_info("socket")
        if sock is not None:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        if self._stopping.is_set():
            await self._reject(writer, ErrorCode.SHUTTING_DOWN, "server stopping")
            return
        if len(self._tasks) >= self.config.max_connections:
            self.stats.connection_rejected()
            log.warning("rejecting %s:%d: connection limit reached", *addr)
            await self._reject(writer, ErrorCode.SERVER_BUSY, "connection limit reached")
            return

        task = asyncio.current_task()
        assert task is not None
        self._tasks.add(task)
        client_id = next(self._ids)
        self.stats.connection_opened()
        log.info("client #%d connected from %s:%d", client_id, *addr)
        try:
            await self._serve(reader, writer, client_id, addr)
        except asyncio.CancelledError:
            log.info("client #%d force-closed", client_id)
            raise
        except OSError as exc:
            log.info("client #%d connection error: %s", client_id, exc)
        except Exception:
            log.exception("client #%d: unexpected error", client_id)
        finally:
            self._tasks.discard(task)
            await _close(writer)
            self.stats.connection_closed()
            log.info("client #%d disconnected", client_id)

    async def _serve(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        client_id: int,
        addr: tuple[str, int],
    ) -> None:
        cfg = self.config
        loop = asyncio.get_running_loop()
        ctx = self.core.context(client_id, addr)
        await self._write(writer, self.core.welcome(client_id))
        while True:
            try:
                request = await self._next_request(reader)
            except TimeoutError:
                log.info("client #%d idle for %ss, disconnecting", client_id, cfg.idle_timeout)
                await self._write(writer, make_error(ErrorCode.IDLE_TIMEOUT, "idle timeout"))
                break
            except (ConnectionClosed, _Stopping):
                break
            except ProtocolError as exc:
                log.warning("client #%d sent a bad frame: %s", client_id, exc)
                await self._write(writer, make_error(ErrorCode.BAD_FRAME, str(exc)))
                break

            response = await loop.run_in_executor(self._executor, self.core.dispatch, request, ctx)
            await self._write(writer, response)
            if self._stopping.is_set():
                break

        if self._stopping.is_set():
            with contextlib.suppress(OSError):
                await self._write(writer, make_error(ErrorCode.SHUTTING_DOWN, "server stopping"))

    async def _next_request(self, reader: asyncio.StreamReader) -> dict[str, Any]:
        """Wait for the next frame, the idle timeout or shutdown, whichever comes first."""
        assert self._stop_signal is not None
        read = asyncio.ensure_future(read_message(reader, self.config.max_frame_size))
        try:
            await asyncio.wait(
                {read, self._stop_signal},
                timeout=self.config.idle_timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            if not read.done():
                read.cancel()
        if read.done() and not read.cancelled():
            return read.result()
        if self._stop_signal.done():
            raise _Stopping
        raise TimeoutError

    async def _write(self, writer: asyncio.StreamWriter, message: dict[str, Any]) -> None:
        writer.write(self.core.encode(message))
        try:
            # A client that never reads must not pin the connection forever.
            await asyncio.wait_for(writer.drain(), self.config.idle_timeout)
        except asyncio.TimeoutError as exc:  # distinct from TimeoutError before 3.11
            raise TimeoutError("timed out writing to client") from exc

    async def _reject(self, writer: asyncio.StreamWriter, code: str, message: str) -> None:
        writer.write(self.core.encode(make_error(code, message)))
        with contextlib.suppress(Exception):
            await asyncio.wait_for(writer.drain(), 1.0)
        await _close(writer)


async def _close(writer: asyncio.StreamWriter) -> None:
    """Close a stream and wait for it, so TLS close_notify finishes before the
    loop stops; abort if the peer does not cooperate."""
    writer.close()
    try:
        await asyncio.wait_for(writer.wait_closed(), 1.0)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        writer.transport.abort()
    except Exception:
        pass
