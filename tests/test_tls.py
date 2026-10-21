import socket
import ssl
import threading
import time

import pytest

from sockethive.security import client_ssl_context


@pytest.fixture
def tls_server(make_server, tls_files):
    cert, key = tls_files
    return make_server(tls_cert=cert, tls_key=key)


def tls_client(make_client, server, tls_files, **kwargs):
    return make_client(server, ssl_context=client_ssl_context(tls_files[0]), **kwargs)


def test_tls_roundtrip(tls_server, make_client, tls_files):
    client = tls_client(make_client, tls_server, tls_files).connect()
    assert isinstance(client._sock, ssl.SSLSocket)
    assert client._sock.version() in {"TLSv1.2", "TLSv1.3"}
    assert client.request("echo", {"secret": "data"}) == {"secret": "data"}


def test_untrusted_certificate_fails_fast(tls_server, make_client):
    # Default trust store does not include our self-signed certificate.
    client = make_client(tls_server, ssl_context=client_ssl_context(), retries=5, backoff=0.5)
    start = time.monotonic()
    with pytest.raises(ssl.SSLCertVerificationError):
        client.connect()
    assert time.monotonic() - start < 1.0  # not retried


def test_hostname_must_match(tls_server, make_client, tls_files):
    client = tls_client(make_client, tls_server, tls_files, server_hostname="example.com")
    with pytest.raises(ssl.SSLCertVerificationError):
        client.connect()


def test_insecure_mode_still_encrypts(tls_server, make_client):
    client = make_client(tls_server, ssl_context=client_ssl_context(insecure=True)).connect()
    assert isinstance(client._sock, ssl.SSLSocket)
    assert client.request("ping") == "pong"


def test_plaintext_client_is_rejected(tls_server, make_client, tls_files):
    # Each side waits for the other to speak first, so keep the timeout short.
    client = make_client(tls_server, retries=1, backoff=0.01, timeout=0.3)
    with pytest.raises(ConnectionError):
        client.connect()
    # The server survives the garbage and keeps serving TLS clients.
    good = tls_client(make_client, tls_server, tls_files).connect()
    assert good.request("ping") == "pong"


def test_stalled_handshake_times_out(make_server, make_client, tls_files):
    cert, key = tls_files
    server = make_server(tls_cert=cert, tls_key=key, handshake_timeout=0.3)
    sock = socket.create_connection(server.address, timeout=5)
    try:
        # Never start the handshake; the server must give up and close.
        assert sock.recv(1) == b""
    finally:
        sock.close()
    good = make_client(server, ssl_context=client_ssl_context(cert)).connect()
    assert good.request("ping") == "pong"


def test_graceful_shutdown_over_tls(tls_server, make_client, tls_files):
    client = tls_client(make_client, tls_server, tls_files, auto_reconnect=False).connect()
    result = {}
    t = threading.Thread(target=lambda: result.setdefault("v", client.request("sleep", 0.5)))
    t.start()
    time.sleep(0.1)
    tls_server.shutdown(grace=3.0)
    t.join()
    # The in-flight response arrived intact (still encrypted, still parseable).
    assert result["v"] == 0.5


def test_tls_config_requires_cert_and_key():
    from sockethive.config import ServerConfig

    with pytest.raises(ValueError):
        ServerConfig(tls_cert="cert.pem")
    assert ServerConfig(tls_cert="c", tls_key="k").tls_enabled
