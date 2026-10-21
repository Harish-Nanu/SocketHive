"""Wire protocol: length-prefixed JSON frames.

Every frame is a 4-byte unsigned big-endian length header followed by exactly
that many bytes of UTF-8 encoded JSON. The JSON value must be an object.

TCP is a byte stream, so explicit framing is what lets either side know where
one message ends and the next begins, regardless of how the kernel splits or
coalesces segments.

Message shapes::

    {"type": "welcome",  "client_id": 7, "server": "SocketHive", "version": "0.1.0"}
    {"type": "auth",     "token": "..."}          (only when welcome has auth_required)
    {"type": "auth_ok"}
    {"type": "request",  "id": 1, "command": "echo", "payload": "hi", "key": "3f2a...:1"}
    {"type": "response", "id": 1, "ok": true,  "result": "hi"}
    {"type": "response", "id": 1, "ok": false, "error": {"code": "...", "message": "..."}}
    {"type": "error",    "error": {"code": "SERVER_BUSY", "message": "..."}}

``key`` is optional. Requests sharing a key are executed at most once while
the server remembers it; repeats get the original response with
``"replayed": true``.

``error`` frames are not tied to a request; the server sends one right before
closing a connection it cannot (or will no longer) serve.
"""

from __future__ import annotations

import asyncio
import json
import socket
import struct
from typing import Any

HEADER = struct.Struct("!I")
MAX_FRAME_SIZE = 1 << 20  # 1 MiB


class ProtocolError(Exception):
    """The peer sent bytes that do not form a valid frame."""


class FrameTooLarge(ProtocolError):
    """A frame exceeds the configured size limit."""


class ConnectionClosed(Exception):
    """The peer closed the connection cleanly between frames."""


class ErrorCode:
    BAD_REQUEST = "BAD_REQUEST"
    BAD_FRAME = "BAD_FRAME"
    UNKNOWN_COMMAND = "UNKNOWN_COMMAND"
    INVALID_PAYLOAD = "INVALID_PAYLOAD"
    INTERNAL = "INTERNAL_ERROR"
    SERVER_BUSY = "SERVER_BUSY"
    IDLE_TIMEOUT = "IDLE_TIMEOUT"
    SHUTTING_DOWN = "SHUTTING_DOWN"
    AUTH_FAILED = "AUTH_FAILED"
    RATE_LIMITED = "RATE_LIMITED"


def encode(message: dict[str, Any], max_size: int = MAX_FRAME_SIZE) -> bytes:
    body = json.dumps(message, separators=(",", ":")).encode("utf-8")
    if len(body) > max_size:
        raise FrameTooLarge(f"frame of {len(body)} bytes exceeds limit of {max_size}")
    return HEADER.pack(len(body)) + body


def decode(body: bytes) -> dict[str, Any]:
    try:
        message = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError(f"invalid JSON body: {exc}") from exc
    if not isinstance(message, dict):
        raise ProtocolError("frame body must be a JSON object")
    return message


def send_message(
    sock: socket.socket, message: dict[str, Any], max_size: int = MAX_FRAME_SIZE
) -> None:
    sock.sendall(encode(message, max_size))


def recv_message(sock: socket.socket, max_size: int = MAX_FRAME_SIZE) -> dict[str, Any]:
    """Read one frame. Raises ConnectionClosed on a clean EOF before the header."""
    header = _recv_exactly(sock, HEADER.size, at_boundary=True)
    (length,) = HEADER.unpack(header)
    if length > max_size:
        raise FrameTooLarge(f"peer announced {length} bytes, limit is {max_size}")
    return decode(_recv_exactly(sock, length, at_boundary=False))


async def read_message(
    reader: asyncio.StreamReader, max_size: int = MAX_FRAME_SIZE
) -> dict[str, Any]:
    """asyncio counterpart of recv_message(), with the same error semantics."""
    try:
        header = await reader.readexactly(HEADER.size)
    except asyncio.IncompleteReadError as exc:
        if not exc.partial:
            raise ConnectionClosed("peer closed the connection") from None
        raise ProtocolError("connection closed mid-frame") from None
    (length,) = HEADER.unpack(header)
    if length > max_size:
        raise FrameTooLarge(f"peer announced {length} bytes, limit is {max_size}")
    try:
        body = await reader.readexactly(length)
    except asyncio.IncompleteReadError as exc:
        raise ProtocolError(
            f"connection closed mid-frame ({len(exc.partial)}/{length} bytes)"
        ) from None
    return decode(body)


def _recv_exactly(sock: socket.socket, n: int, *, at_boundary: bool) -> bytes:
    buf = bytearray(n)
    view = memoryview(buf)
    received = 0
    while received < n:
        chunk = sock.recv_into(view[received:], n - received)
        if chunk == 0:
            if at_boundary and received == 0:
                raise ConnectionClosed("peer closed the connection")
            raise ProtocolError(f"connection closed mid-frame ({received}/{n} bytes)")
        received += chunk
    return bytes(buf)


def make_request(
    request_id: int, command: str, payload: Any = None, key: str | None = None
) -> dict[str, Any]:
    request = {"type": "request", "id": request_id, "command": command, "payload": payload}
    if key is not None:
        request["key"] = key
    return request


def make_response(request_id: Any, result: Any) -> dict[str, Any]:
    return {"type": "response", "id": request_id, "ok": True, "result": result}


def make_error_response(request_id: Any, code: str, message: str, **details: Any) -> dict[str, Any]:
    return {
        "type": "response",
        "id": request_id,
        "ok": False,
        "error": {"code": code, "message": message, **details},
    }


def make_error(code: str, message: str) -> dict[str, Any]:
    return {"type": "error", "error": {"code": code, "message": message}}
