"""Synchronous PowDB client."""

from __future__ import annotations

import socket
from dataclasses import dataclass
from typing import Optional, Union

from .protocol import (
    Connect,
    ConnectOk,
    Disconnect,
    Error,
    Message,
    Query,
    ResultOk,
    ResultRows,
    ResultScalar,
    encode,
    try_decode,
)


class PowDBError(Exception):
    """Raised when the server returns an Error frame or a protocol error occurs."""


@dataclass
class Rows:
    columns: list[str]
    rows: list[list[str]]

    @property
    def kind(self) -> str:
        return "rows"


@dataclass
class Scalar:
    value: str

    @property
    def kind(self) -> str:
        return "scalar"


@dataclass
class Ok:
    affected: int

    @property
    def kind(self) -> str:
        return "ok"


QueryResult = Union[Rows, Scalar, Ok]


class Client:
    """Synchronous PowDB client.

    Use as a context manager to ensure the connection is closed:

        with Client.connect(host="127.0.0.1", port=5433) as c:
            result = c.query("User { .name }")
    """

    def __init__(self, sock: socket.socket, server_version: str) -> None:
        self._sock = sock
        self._buffer = bytearray()
        self._closed = False
        self.server_version = server_version

    @classmethod
    def connect(
        cls,
        host: str,
        port: int,
        *,
        db_name: str = "default",
        password: Optional[str] = None,
        connect_timeout: float = 5.0,
    ) -> "Client":
        """Open a connection, send Connect, wait for ConnectOk."""
        sock = socket.create_connection((host, port), timeout=connect_timeout)
        sock.settimeout(None)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        sock.sendall(encode(Connect(db_name=db_name, password=password)))
        reply = _read_one(sock, bytearray())
        if isinstance(reply, Error):
            sock.close()
            raise PowDBError(f"connect failed: {reply.message}")
        if not isinstance(reply, ConnectOk):
            sock.close()
            raise PowDBError(
                f"expected ConnectOk, got {type(reply).__name__}"
            )
        return cls(sock, reply.version)

    def query(self, q: str) -> QueryResult:
        """Run a PowQL statement."""
        if self._closed:
            raise PowDBError("client is closed")
        self._sock.sendall(encode(Query(query=q)))
        reply = _read_one(self._sock, self._buffer)
        if isinstance(reply, ResultRows):
            return Rows(columns=reply.columns, rows=reply.rows)
        if isinstance(reply, ResultScalar):
            return Scalar(value=reply.value)
        if isinstance(reply, ResultOk):
            return Ok(affected=reply.affected)
        if isinstance(reply, Error):
            raise PowDBError(f"query failed: {reply.message}")
        raise PowDBError(f"unexpected reply: {type(reply).__name__}")

    def close(self) -> None:
        """Send Disconnect and close the socket."""
        if self._closed:
            return
        self._closed = True
        try:
            self._sock.sendall(encode(Disconnect()))
        except OSError:
            pass
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._sock.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


def _read_one(sock: socket.socket, buffer: bytearray) -> Message:
    """Read a single frame, buffering leftover bytes for the next call."""
    while True:
        decoded = try_decode(bytes(buffer))
        if decoded is not None:
            msg, consumed = decoded
            del buffer[:consumed]
            return msg
        chunk = sock.recv(65536)
        if not chunk:
            raise PowDBError("connection closed by server")
        buffer.extend(chunk)
