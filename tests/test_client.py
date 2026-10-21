import threading
import time

import pytest

from sockethive.client import RequestTimeout

from .conftest import free_port


def test_context_manager_and_handshake(server, make_client):
    with make_client(server) as client:
        assert client.connected
        assert client.client_id == 1
        assert client.server_info["server"] == "SocketHive"
    assert not client.connected


def test_connect_retries_until_server_is_up(make_server, make_client):
    port = free_port()
    client = make_client(port, retries=8, backoff=0.05)
    starter = threading.Timer(0.3, make_server, kwargs={"port": port})
    starter.start()
    try:
        assert client.connect().request("ping") == "pong"
    finally:
        starter.join()


def test_connect_gives_up_after_retries(make_client):
    client = make_client(free_port(), retries=2, backoff=0.01)
    start = time.monotonic()
    with pytest.raises(ConnectionError, match="after 3 attempt"):
        client.connect()
    assert time.monotonic() - start < 2


def test_busy_server_is_retried(make_server, make_client):
    server = make_server(max_connections=1)
    holder = make_client(server).connect()
    waiter = make_client(server, retries=10, backoff=0.05)
    threading.Timer(0.3, holder.close).start()
    assert waiter.connect().request("ping") == "pong"


def test_reconnects_after_server_drops_connection(make_server, make_client):
    server = make_server(idle_timeout=0.2)
    client = make_client(server).connect()
    first_id = client.client_id
    time.sleep(0.4)  # server drops the idle connection
    assert client.request("ping") == "pong"
    assert client.client_id != first_id


def test_reconnects_after_server_restart(make_server, make_client):
    port = free_port()
    server = make_server(port=port)
    client = make_client(port, retries=10, backoff=0.05).connect()
    server.shutdown(grace=0.5)
    make_server(port=port)
    assert client.request("echo", "back") == "back"


def test_request_timeout(server, make_client):
    client = make_client(server, timeout=0.2).connect()
    with pytest.raises(RequestTimeout):
        client.request("sleep", 1)
    assert not client.connected
    client.timeout = 5.0
    assert client.request("ping") == "pong"


def test_shared_client_across_threads(server, make_client):
    client = make_client(server).connect()
    errors = []

    def worker(i):
        try:
            for j in range(25):
                assert client.request("echo", [i, j]) == [i, j]
        except Exception as exc:  # pragma: no cover - surfaced via assertion below
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert server.stats.snapshot()["connections_total"] == 1


def test_heartbeat_prevents_idle_disconnect(make_server, make_client):
    server = make_server(idle_timeout=0.5)
    client = make_client(server).connect()
    first_id = client.client_id
    client.start_heartbeat(interval=0.2)
    time.sleep(1.2)
    client.stop_heartbeat()
    assert client.request("whoami")["client_id"] == first_id
    assert server.stats.snapshot()["connections_total"] == 1
