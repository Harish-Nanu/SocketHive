import socket
import threading

import pytest

from sockethive.protocol import (
    HEADER,
    ConnectionClosed,
    FrameTooLarge,
    ProtocolError,
    decode,
    encode,
    make_request,
    recv_message,
    send_message,
)


@pytest.fixture
def pair():
    a, b = socket.socketpair()
    a.settimeout(2)
    b.settimeout(2)
    yield a, b
    a.close()
    b.close()


def test_roundtrip(pair):
    a, b = pair
    msg = make_request(1, "echo", {"text": "héllo", "n": [1, 2, 3]})
    send_message(a, msg)
    assert recv_message(b) == msg


def test_multiple_frames_in_one_stream(pair):
    a, b = pair
    # Write several frames in a single sendall so they arrive coalesced.
    a.sendall(b"".join(encode({"i": i}) for i in range(50)))
    assert [recv_message(b)["i"] for _ in range(50)] == list(range(50))


def test_frame_split_across_writes(pair):
    a, b = pair
    data = encode({"payload": "x" * 1000})

    def dribble():
        for i in range(0, len(data), 7):
            a.sendall(data[i : i + 7])

    # Write from another thread: many tiny writes can fill the socket buffer.
    writer = threading.Thread(target=dribble)
    writer.start()
    assert recv_message(b)["payload"] == "x" * 1000
    writer.join()


def test_clean_close_raises_connection_closed(pair):
    a, b = pair
    a.close()
    with pytest.raises(ConnectionClosed):
        recv_message(b)


def test_close_mid_frame_is_protocol_error(pair):
    a, b = pair
    a.sendall(encode({"k": "v"})[:-2])
    a.close()
    with pytest.raises(ProtocolError):
        recv_message(b)


def test_oversized_send_rejected():
    with pytest.raises(FrameTooLarge):
        encode({"blob": "x" * 200}, max_size=100)


def test_oversized_announced_length_rejected(pair):
    a, b = pair
    a.sendall(HEADER.pack(10_000))
    with pytest.raises(FrameTooLarge):
        recv_message(b, max_size=1024)


@pytest.mark.parametrize("body", [b"not json", b"\xff\xfe", b"[1, 2, 3]", b'"str"'])
def test_invalid_bodies(body):
    with pytest.raises(ProtocolError):
        decode(body)
