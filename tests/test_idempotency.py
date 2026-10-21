import socket
import threading
import time

import pytest

from sockethive.client import RequestTimeout
from sockethive.handlers import default_registry
from sockethive.protocol import ErrorCode, make_request, recv_message, send_message


class Ledger:
    """A command with a visible side effect: every execution is counted."""

    def __init__(self):
        self.lock = threading.Lock()
        self.executions = 0

    def registry(self):
        registry = default_registry()

        @registry.register("charge")
        def charge(payload, ctx):
            with self.lock:
                self.executions += 1
                count = self.executions
            time.sleep(payload or 0)
            return count

        return registry


@pytest.fixture
def ledger():
    return Ledger()


def send(server, request):
    with socket.create_connection(server.address, timeout=5) as sock:
        recv_message(sock)  # welcome
        send_message(sock, request)
        return recv_message(sock)


def drop_connection_soon(client, delay=0.1):
    def drop():
        client._sock.shutdown(socket.SHUT_RDWR)

    timer = threading.Timer(delay, drop)
    timer.start()
    return timer


def test_retry_after_dropped_connection_runs_once(make_server, make_client, ledger):
    server = make_server(ledger.registry())
    client = make_client(server).connect()
    drop_connection_soon(client)
    # The connection dies while "charge" is still running on the server; the
    # client reconnects, resends with the same key and gets the original result.
    assert client.request("charge", 0.4) == 1
    assert ledger.executions == 1
    assert server.stats.snapshot()["requests_deduplicated"] == 1


def test_without_dedup_the_retry_runs_twice(make_server, make_client, ledger):
    server = make_server(ledger.registry(), dedup_ttl=0)
    client = make_client(server).connect()
    drop_connection_soon(client)
    client.request("charge", 0.4)
    time.sleep(0.5)  # let the orphaned original finish
    assert ledger.executions == 2


def test_completed_request_is_replayed_on_a_new_connection(make_server, ledger):
    server = make_server(ledger.registry())
    first = send(server, make_request(1, "charge", key="order-1"))
    again = send(server, make_request(1, "charge", key="order-1"))
    assert first["result"] == again["result"] == 1
    assert "replayed" not in first
    assert again["replayed"] is True

    other = send(server, make_request(2, "charge", key="order-2"))
    assert other["result"] == 2
    assert ledger.executions == 2


def test_explicit_key_is_shared_across_clients(make_server, make_client, ledger):
    server = make_server(ledger.registry())
    a = make_client(server).connect()
    b = make_client(server).connect()
    assert a.request("charge", idempotency_key="invoice-7") == 1
    assert b.request("charge", idempotency_key="invoice-7") == 1
    assert ledger.executions == 1


def test_timed_out_request_can_be_retried_safely(make_server, make_client, ledger):
    server = make_server(ledger.registry())
    client = make_client(server, timeout=0.2).connect()
    with pytest.raises(RequestTimeout):
        client.request("charge", 0.6, idempotency_key="slow-1")
    client.timeout = 5.0
    # Original is still running: the retry waits for it instead of re-running.
    assert client.request("charge", 0.6, idempotency_key="slow-1") == 1
    assert ledger.executions == 1


def test_rate_limited_request_is_not_remembered(make_server, ledger):
    server = make_server(ledger.registry(), rate_limit=2, rate_burst=1)
    assert send(server, make_request(1, "charge", key="a"))["ok"]
    limited = send(server, make_request(2, "charge", key="b"))
    assert limited["error"]["code"] == ErrorCode.RATE_LIMITED
    time.sleep(limited["error"]["retry_after"] + 0.05)
    retried = send(server, make_request(2, "charge", key="b"))
    assert retried["ok"] and "replayed" not in retried
    assert ledger.executions == 2


@pytest.mark.parametrize("key", [123, "", "x" * 129])
def test_invalid_keys_are_rejected(server, key):
    reply = send(server, make_request(1, "ping", key=key))
    assert reply["error"]["code"] == ErrorCode.BAD_REQUEST


def test_requests_without_key_are_not_deduplicated(make_server, ledger):
    server = make_server(ledger.registry())
    send(server, make_request(1, "charge"))
    send(server, make_request(1, "charge"))
    assert ledger.executions == 2
