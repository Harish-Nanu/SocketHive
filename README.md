# SocketHive

A scalable, multithreaded TCP/IP client–server system written in pure Python
(standard library only). The server handles many clients at once, isolates
faults per connection and shuts down gracefully. The client retries with
backoff, reconnects on its own and can send heartbeats.

**Tech stack:** Python 3.10+, TCP/IP sockets, multithreading
(`threading` + `concurrent.futures`), Docker / Docker Compose, pytest,
GitHub Actions.

## Features

- **Concurrent connections.** An acceptor thread hands each client to a bounded
  worker pool. Slow requests on one client never block the others.
- **Backpressure.** A hard connection limit (`max_connections`). Clients over
  the limit get a `SERVER_BUSY` frame instead of piling up threads, and the
  client retries with exponential backoff and jitter.
- **Reliable framing.** Length-prefixed JSON frames with a maximum size, so
  partial reads and coalesced TCP segments are handled correctly.
- **Fault isolation.** A malformed frame, an oversized frame, a crashing
  handler, an idle client or a connection reset affects only that one
  connection.
- **Graceful shutdown.** On SIGINT/SIGTERM the server stops accepting and lets
  in-flight requests finish. Any connection still open after the grace period
  is force-closed.
- **Resilient client.** It is thread-safe, does a handshake, has request
  timeouts, reconnects automatically after drops or server restarts, and can
  send heartbeats in the background to keep an idle connection alive.
- **Observability.** Logs include the thread name, and the `stats` command
  returns live counters.
- **Load testing.** The built-in `bench` mode reports throughput and
  p50/p95/p99 latency.
- **Containerised.** Non-root Docker image with a healthcheck, plus a Compose
  setup with bench and interactive-client profiles.

## Architecture

```
                        ┌──────────────────────── SocketHiveServer ───────────────────────┐
  client ──TCP──┐       │                                                                 │
  client ──TCP──┼──────►│  acceptor thread ──► slot semaphore ──► ThreadPoolExecutor      │
  client ──TCP──┘       │   (accept loop)      (max_connections)   worker per connection  │
                        │        │                   │                   │                │
                        │        │ full?             │                   ▼                │
                        │        └──► SERVER_BUSY ◄──┘          recv frame → dispatch     │
                        │                                        → CommandRegistry        │
                        │                                        → send response          │
                        └─────────────────────────────────────────────────────────────────┘
```

| Module | Responsibility |
|---|---|
| `sockethive/protocol.py` | Frame encoding and decoding, message builders, error codes |
| `sockethive/server.py` | Accept loop, worker pool, connection limit, fault handling, shutdown, stats |
| `sockethive/client.py` | Connect with retries, request/response, reconnect, timeouts, heartbeat |
| `sockethive/handlers.py` | Command registry and built-in commands |
| `sockethive/config.py` | `ServerConfig`, loaded from `SOCKETHIVE_*` environment variables |
| `sockethive/bench.py` | Concurrent load generator |
| `sockethive/cli.py` | `sockethive server | client | bench` |

## Quick start

```bash
# Install (pip or uv)
python -m venv .venv && . .venv/bin/activate
pip install -e '.[dev]'

# Terminal 1: run the server
sockethive server --port 9000 --max-connections 64

# Terminal 2: talk to it
sockethive client ping                    # pong
sockethive client sum '[1, 2, 3.5]'       # 6.5
sockethive client stats                   # live server counters
sockethive client                         # interactive shell

# Load test: 50 concurrent clients x 200 requests each
sockethive bench -c 50 -n 200
```

### With Docker

```bash
docker compose up -d server                        # build and start (port 9000)
docker compose --profile bench run --rm bench      # load test inside the network
docker compose --profile tools run --rm client     # interactive shell
docker compose down                                # SIGTERM, graceful drain
```

## Using it as a library

```python
from sockethive.client import SocketHiveClient
from sockethive.config import ServerConfig
from sockethive.handlers import default_registry
from sockethive.server import SocketHiveServer

registry = default_registry()


@registry.register("square")
def square(payload, ctx):
    return payload * payload


with SocketHiveServer(ServerConfig(port=9000), registry):
    with SocketHiveClient("127.0.0.1", 9000) as client:
        client.start_heartbeat(interval=30)
        print(client.request("square", 12))  # 144
```

## Protocol

Every frame is a **4-byte big-endian length** followed by that many bytes of
**UTF-8 JSON** (an object). Frames larger than `max_frame_size` are rejected.

```text
server → {"type": "welcome",  "client_id": 7, "server": "SocketHive", "version": "0.1.0"}
client → {"type": "request",  "id": 1, "command": "echo", "payload": "hi"}
server → {"type": "response", "id": 1, "ok": true, "result": "hi"}
server → {"type": "response", "id": 2, "ok": false, "error": {"code": "UNKNOWN_COMMAND", "message": "..."}}
server → {"type": "error", "error": {"code": "IDLE_TIMEOUT", "message": "..."}}   (then closes)
```

| Error code | Meaning | Connection |
|---|---|---|
| `UNKNOWN_COMMAND` | No handler for this command | stays open |
| `INVALID_PAYLOAD` | Payload has the wrong shape | stays open |
| `BAD_REQUEST` | Frame is not a valid request | stays open |
| `INTERNAL_ERROR` | Handler raised or returned something that can't be encoded | stays open |
| `BAD_FRAME` | Invalid JSON, a truncated frame or an oversized frame | closed |
| `SERVER_BUSY` | Connection limit reached | closed (the client retries) |
| `IDLE_TIMEOUT` | The client was silent too long | closed (the client reconnects) |
| `SHUTTING_DOWN` | The server is stopping | closed |

### Built-in commands

`ping`, `echo`, `reverse` (string), `sum` (list of numbers), `time`,
`sleep` (seconds, capped at 5; simulates slow work), `whoami`, `stats`,
`commands`.

## Configuration

Precedence: CLI flags override environment variables, which override the defaults.

| Env var | Flag | Default | Description |
|---|---|---|---|
| `SOCKETHIVE_HOST` | `--host` | `0.0.0.0` | Bind address |
| `SOCKETHIVE_PORT` | `--port` | `9000` | TCP port |
| `SOCKETHIVE_MAX_CONNECTIONS` | `--max-connections` | `64` | Concurrent clients (and worker threads) |
| `SOCKETHIVE_IDLE_TIMEOUT` | `--idle-timeout` | `300` | Seconds before a silent client is dropped |
| `SOCKETHIVE_MAX_FRAME_SIZE` | `--max-frame-size` | `1048576` | Maximum frame body size in bytes |
| `SOCKETHIVE_BACKLOG` | – | `128` | `listen()` backlog |
| `SOCKETHIVE_SHUTDOWN_GRACE` | `--shutdown-grace` | `5` | Seconds allowed for draining on shutdown |
| `SOCKETHIVE_LOG_LEVEL` | `--log-level` | `INFO` | Logging level |

## Delivery semantics

If a connection drops while a request is in flight, the client reconnects and
resends the request **once**. This is *at-least-once* delivery, so handlers
with side effects should be idempotent. Timeouts are never retried, because
the server may still be processing the request.

## Development

```bash
pytest -q              # 51 tests: protocol, handlers, server, client, CLI
ruff check . && ruff format --check .
```

The test suite covers 50 concurrent clients, parallel slow requests,
connection-limit rejection and recovery, idle timeouts, malformed and
oversized frames, abrupt resets, handler crashes, retries until the server
comes up, reconnecting after a server restart, request timeouts, a client
shared across threads, heartbeats, and graceful versus forced shutdown.

CI (GitHub Actions) runs lint and tests on Python 3.10–3.13, then builds the
Docker image and smoke-tests it with the bench profile.
