import socket
import time

import pytest

from sockethive.client import AuthenticationError
from sockethive.config import ServerConfig
from sockethive.protocol import ErrorCode, make_request, recv_message, send_message

TOKEN = "correct-horse-battery-staple"


@pytest.fixture
def auth_server(make_server):
    return make_server(auth_token=TOKEN, handshake_timeout=0.5)


def test_valid_token(auth_server, make_client):
    client = make_client(auth_server, auth_token=TOKEN).connect()
    assert client.server_info["auth_required"] is True
    assert client.request("ping") == "pong"


def test_reconnect_reauthenticates(make_server, make_client):
    server = make_server(auth_token=TOKEN, idle_timeout=0.2)
    client = make_client(server, auth_token=TOKEN).connect()
    time.sleep(0.4)  # dropped as idle
    assert client.request("ping") == "pong"
    assert server.stats.snapshot()["connections_total"] == 2


@pytest.mark.parametrize("token", ["wrong", "", None])
def test_bad_or_missing_token_fails_fast(auth_server, make_client, token):
    client = make_client(auth_server, auth_token=token, retries=5, backoff=0.5)
    start = time.monotonic()
    with pytest.raises(AuthenticationError) as exc:
        client.connect()
    assert exc.value.code == ErrorCode.AUTH_FAILED
    assert time.monotonic() - start < 1.0  # not retried
    assert not client.connected


def test_failures_are_counted(auth_server, make_client):
    with pytest.raises(AuthenticationError):
        make_client(auth_server, auth_token="nope").connect()
    deadline = time.monotonic() + 2
    while auth_server.stats.snapshot()["auth_failures"] < 1 and time.monotonic() < deadline:
        time.sleep(0.01)
    assert auth_server.stats.snapshot()["auth_failures"] == 1


def test_request_before_auth_is_rejected(auth_server):
    with socket.create_connection(auth_server.address, timeout=5) as sock:
        assert recv_message(sock)["auth_required"] is True
        send_message(sock, make_request(1, "stats"))
        assert recv_message(sock)["error"]["code"] == ErrorCode.AUTH_FAILED
        assert sock.recv(1) == b""


def test_silent_client_is_dropped_after_handshake_timeout(auth_server):
    with socket.create_connection(auth_server.address, timeout=5) as sock:
        recv_message(sock)
        start = time.monotonic()
        assert recv_message(sock)["error"]["code"] == ErrorCode.AUTH_FAILED
        assert time.monotonic() - start < 2.0


def test_open_server_ignores_token(server, make_client):
    client = make_client(server, auth_token="unused").connect()
    assert client.server_info["auth_required"] is False
    assert client.request("ping") == "pong"


def test_token_is_not_in_config_repr():
    assert TOKEN not in repr(ServerConfig(auth_token=TOKEN))
