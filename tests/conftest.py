import socket

import pytest

from sockethive.client import SocketHiveClient
from sockethive.config import ServerConfig
from sockethive.server import SocketHiveServer


@pytest.fixture
def make_server():
    """Factory for servers on an ephemeral localhost port, stopped after the test."""
    servers = []

    def factory(registry=None, **overrides):
        config = ServerConfig(host="127.0.0.1", port=overrides.pop("port", 0), **overrides)
        server = SocketHiveServer(config, registry).start()
        servers.append(server)
        return server

    yield factory
    for server in servers:
        server.shutdown(grace=1.0)


@pytest.fixture
def server(make_server):
    return make_server()


@pytest.fixture
def make_client():
    clients = []

    def factory(server_or_port, **kwargs):
        port = server_or_port if isinstance(server_or_port, int) else server_or_port.address[1]
        kwargs.setdefault("timeout", 5.0)
        client = SocketHiveClient("127.0.0.1", port, **kwargs)
        clients.append(client)
        return client

    yield factory
    for client in clients:
        client.close()


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
