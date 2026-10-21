import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from sockethive.client import RemoteError, ServerClosedConnection
from sockethive.handlers import default_registry
from sockethive.protocol import (
    HEADER,
    ErrorCode,
    encode,
    make_request,
    recv_message,
    send_message,
)


def raw_connect(server):
    sock = socket.create_connection(server.address, timeout=5)
    assert recv_message(sock)["type"] == "welcome"
    return sock


def wait_for(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


def test_request_response(server, make_client):
    client = make_client(server).connect()
    assert client.request("ping") == "pong"
    assert client.request("echo", {"nested": [1, "two"]}) == {"nested": [1, "two"]}
    assert client.request("reverse", "hive") == "evih"


def test_error_response_keeps_connection_open(server, make_client):
    client = make_client(server).connect()
    with pytest.raises(RemoteError) as exc:
        client.request("does-not-exist")
    assert exc.value.code == ErrorCode.UNKNOWN_COMMAND
    with pytest.raises(RemoteError) as exc:
        client.request("sum", "not a list")
    assert exc.value.code == ErrorCode.INVALID_PAYLOAD
    first_id = client.client_id
    assert client.request("ping") == "pong"
    assert client.client_id == first_id  # same connection, no reconnect


def test_handler_crash_is_isolated(make_server, make_client):
    registry = default_registry()

    @registry.register("boom")
    def boom(payload, ctx):
        raise RuntimeError("kaboom")

    server = make_server(registry)
    client = make_client(server).connect()
    with pytest.raises(RemoteError) as exc:
        client.request("boom")
    assert exc.value.code == ErrorCode.INTERNAL
    assert "kaboom" not in exc.value.message  # internals are not leaked
    assert client.request("ping") == "pong"


def test_unserialisable_result_reported(make_server, make_client):
    registry = default_registry()
    registry.register("bad")(lambda payload, ctx: object())
    client = make_client(make_server(registry)).connect()
    with pytest.raises(RemoteError) as exc:
        client.request("bad")
    assert exc.value.code == ErrorCode.INTERNAL
    assert client.request("ping") == "pong"


def test_many_concurrent_clients(make_server, make_client):
    server = make_server(max_connections=64)
    n_clients, n_requests = 50, 20

    def worker(i):
        with make_client(server) as client:
            return [client.request("echo", f"{i}-{j}") for j in range(n_requests)]

    with ThreadPoolExecutor(n_clients) as pool:
        results = list(pool.map(worker, range(n_clients)))

    for i, replies in enumerate(results):
        assert replies == [f"{i}-{j}" for j in range(n_requests)]
    assert wait_for(lambda: server.stats.snapshot()["connections_active"] == 0)
    stats = server.stats.snapshot()
    assert stats["connections_total"] == n_clients
    assert stats["requests_total"] == n_clients * n_requests


def test_slow_requests_run_in_parallel(server, make_client):
    clients = [make_client(server).connect() for _ in range(5)]
    start = time.monotonic()
    threads = [threading.Thread(target=c.request, args=("sleep", 0.5)) for c in clients]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # Five 0.5s requests served sequentially would take 2.5s.
    assert time.monotonic() - start < 1.5


def test_connection_limit_rejects_with_server_busy(make_server, make_client):
    server = make_server(max_connections=2)
    held = [make_client(server).connect() for _ in range(2)]

    extra = socket.create_connection(server.address, timeout=5)
    frame = recv_message(extra)
    assert frame["type"] == "error"
    assert frame["error"]["code"] == ErrorCode.SERVER_BUSY
    extra.close()
    assert server.stats.snapshot()["connections_rejected"] == 1

    # Once a slot frees up, new clients are admitted again.
    held[0].close()
    assert wait_for(lambda: server.stats.snapshot()["connections_active"] == 1)
    assert make_client(server, retries=0).connect().request("ping") == "pong"
    assert held[1].request("ping") == "pong"


def test_idle_client_is_disconnected(make_server):
    server = make_server(idle_timeout=0.3)
    sock = raw_connect(server)
    frame = recv_message(sock)
    assert frame["error"]["code"] == ErrorCode.IDLE_TIMEOUT
    assert sock.recv(1) == b""
    assert wait_for(lambda: server.stats.snapshot()["connections_active"] == 0)


def test_malformed_frame_drops_only_that_client(server, make_client):
    healthy = make_client(server).connect()

    bad = raw_connect(server)
    body = b"{not json"
    bad.sendall(HEADER.pack(len(body)) + body)
    assert recv_message(bad)["error"]["code"] == ErrorCode.BAD_FRAME
    assert bad.recv(1) == b""

    assert healthy.request("ping") == "pong"


def test_oversized_frame_rejected(make_server):
    server = make_server(max_frame_size=1024)
    sock = raw_connect(server)
    sock.sendall(HEADER.pack(10_000_000))
    assert recv_message(sock)["error"]["code"] == ErrorCode.BAD_FRAME


def test_bad_request_shape(server):
    sock = raw_connect(server)
    send_message(sock, {"type": "request", "id": 9, "command": 42})
    reply = recv_message(sock)
    assert reply["id"] == 9
    assert reply["error"]["code"] == ErrorCode.BAD_REQUEST


def test_abrupt_client_disconnect_is_cleaned_up(server):
    sock = raw_connect(server)
    # Send half a frame then reset the connection.
    sock.sendall(encode(make_request(1, "echo", "x"))[:5])
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, b"\x01\x00\x00\x00\x00\x00\x00\x00")
    sock.close()
    assert wait_for(lambda: server.stats.snapshot()["connections_active"] == 0)


def test_graceful_shutdown_drains_in_flight_requests(make_server, make_client):
    server = make_server()
    busy = make_client(server, auto_reconnect=False).connect()
    idle = raw_connect(server)

    result = {}
    t = threading.Thread(target=lambda: result.setdefault("v", busy.request("sleep", 0.5)))
    t.start()
    time.sleep(0.1)  # let the request reach the server

    server.shutdown(grace=3.0)
    t.join()
    assert result["v"] == 0.5  # in-flight request completed before shutdown
    assert recv_message(idle)["error"]["code"] == ErrorCode.SHUTTING_DOWN
    with pytest.raises(ConnectionRefusedError):
        socket.create_connection(server.address, timeout=1)


def test_shutdown_force_closes_after_grace(make_server, make_client):
    server = make_server()
    client = make_client(server, auto_reconnect=False).connect()
    t = threading.Thread(target=lambda: _swallow(client.request, "sleep", 3))
    t.start()
    time.sleep(0.1)
    start = time.monotonic()
    server.shutdown(grace=0.2)
    assert time.monotonic() - start < 1.0
    t.join()


def test_shutdown_notice_raises_on_client(make_server, make_client):
    server = make_server()
    client = make_client(server, auto_reconnect=False).connect()
    server.shutdown(grace=1.0)
    with pytest.raises(ConnectionError) as exc:
        client.request("ping")
    assert isinstance(exc.value.__cause__, (ServerClosedConnection, OSError))


def _swallow(func, *args):
    try:
        func(*args)
    except Exception:
        pass
