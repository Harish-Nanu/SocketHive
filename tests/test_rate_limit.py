import socket
import time

import pytest

from sockethive.client import RemoteError
from sockethive.protocol import ErrorCode, make_request, recv_message, send_message


def test_requests_over_the_limit_are_rejected(make_server, make_client):
    server = make_server(rate_limit=5, rate_burst=5)
    client = make_client(server).connect()
    for _ in range(5):
        assert client.request("ping") == "pong"
    with pytest.raises(RemoteError) as exc:
        client.request("ping")
    assert exc.value.code == ErrorCode.RATE_LIMITED
    assert 0 < exc.value.retry_after <= 0.2 + 1e-6  # one token at 5/s

    # Connection stays open, and the bucket refills over time.
    time.sleep(exc.value.retry_after + 0.05)
    assert client.request("ping") == "pong"
    stats = server.stats.snapshot()
    assert stats["requests_rate_limited"] == 1
    assert stats["connections_total"] == 1


def test_limit_is_shared_across_connections_from_one_ip(make_server, make_client):
    server = make_server(rate_limit=1, rate_burst=3)
    clients = [make_client(server).connect() for _ in range(3)]
    for c in clients:
        assert c.request("ping") == "pong"
    with pytest.raises(RemoteError) as exc:
        make_client(server).connect().request("ping")
    assert exc.value.code == ErrorCode.RATE_LIMITED


def _connect_from(server, source_ip):
    sock = socket.create_connection(server.address, timeout=5, source_address=(source_ip, 0))
    assert recv_message(sock)["type"] == "welcome"
    return sock


def _ping(sock):
    send_message(sock, make_request(1, "ping"))
    return recv_message(sock)


def test_other_ips_have_their_own_budget(make_server):
    server = make_server(rate_limit=1, rate_burst=1)
    with _connect_from(server, "127.0.0.1") as first:
        assert _ping(first)["ok"]
        assert _ping(first)["error"]["code"] == ErrorCode.RATE_LIMITED
        try:
            # Any 127.x.y.z address is loopback on Linux; elsewhere, skip.
            second = _connect_from(server, "127.0.0.2")
        except OSError:
            pytest.skip("cannot use 127.0.0.2 as a source address here")
        with second:
            assert _ping(second)["ok"]


def test_disabled_by_default(server, make_client):
    client = make_client(server).connect()
    for _ in range(100):
        client.request("ping")
