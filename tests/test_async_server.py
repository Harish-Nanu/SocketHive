import resource
import socket
import threading

import pytest

from sockethive.aio_server import AsyncSocketHiveServer
from sockethive.config import ServerConfig
from sockethive.protocol import recv_message
from sockethive.server import SocketHiveServer, create_server

IDLE_CLIENTS = 1000


def test_create_server_selects_implementation():
    assert isinstance(create_server(ServerConfig(mode="threaded")), SocketHiveServer)
    assert isinstance(create_server(ServerConfig(mode="async")), AsyncSocketHiveServer)
    with pytest.raises(ValueError):
        ServerConfig(mode="forking")


def test_many_idle_connections_use_few_threads(make_client):
    soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft < 2 * IDLE_CLIENTS + 100:
        pytest.skip(f"open-file limit {soft} too low")

    threads_before = threading.active_count()
    config = ServerConfig(mode="async", host="127.0.0.1", port=0, max_connections=IDLE_CLIENTS + 10)
    server = AsyncSocketHiveServer(config).start()
    socks = []
    try:
        for _ in range(IDLE_CLIENTS):
            sock = socket.create_connection(server.address, timeout=5)
            socks.append(sock)
            assert recv_message(sock)["type"] == "welcome"

        assert server.stats.snapshot()["connections_active"] == IDLE_CLIENTS
        # The event loop thread plus a few handler threads, not one per client.
        assert threading.active_count() - threads_before < 40
        # Still responsive to new work while holding all those connections.
        assert make_client(server).connect().request("ping") == "pong"
    finally:
        for sock in socks:
            sock.close()
        server.shutdown(grace=2.0)
