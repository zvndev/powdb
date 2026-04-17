"""PowDB wire protocol.

Frame format: [type(1)][flags(1)][len(4 LE)][payload]
Strings are encoded as [len(4 LE)][utf-8 bytes].

Mirrors crates/server/src/protocol.rs.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Optional, Union

MSG_CONNECT = 0x01
MSG_CONNECT_OK = 0x02
MSG_QUERY = 0x03
MSG_RESULT_ROWS = 0x07
MSG_RESULT_SCALAR = 0x08
MSG_RESULT_OK = 0x09
MSG_ERROR = 0x0A
MSG_DISCONNECT = 0x10


@dataclass
class Connect:
    db_name: str
    password: Optional[str]


@dataclass
class ConnectOk:
    version: str


@dataclass
class Query:
    query: str


@dataclass
class ResultRows:
    columns: list[str]
    rows: list[list[str]]


@dataclass
class ResultScalar:
    value: str


@dataclass
class ResultOk:
    affected: int


@dataclass
class Error:
    message: str


@dataclass
class Disconnect:
    pass


Message = Union[
    Connect,
    ConnectOk,
    Query,
    ResultRows,
    ResultScalar,
    ResultOk,
    Error,
    Disconnect,
]


def _encode_string(s: str) -> bytes:
    data = s.encode("utf-8")
    return struct.pack("<I", len(data)) + data


def _decode_string(buf: bytes, pos: int) -> tuple[str, int]:
    if pos + 4 > len(buf):
        raise ValueError("truncated string length")
    (length,) = struct.unpack_from("<I", buf, pos)
    pos += 4
    if pos + length > len(buf):
        raise ValueError("truncated string data")
    s = buf[pos : pos + length].decode("utf-8", errors="replace")
    return s, pos + length


def encode(msg: Message) -> bytes:
    if isinstance(msg, Connect):
        payload = _encode_string(msg.db_name)
        if msg.password is None:
            payload += struct.pack("<I", 0)
        else:
            payload += _encode_string(msg.password)
        msg_type = MSG_CONNECT
    elif isinstance(msg, ConnectOk):
        payload = _encode_string(msg.version)
        msg_type = MSG_CONNECT_OK
    elif isinstance(msg, Query):
        payload = _encode_string(msg.query)
        msg_type = MSG_QUERY
    elif isinstance(msg, ResultRows):
        parts = [struct.pack("<H", len(msg.columns))]
        parts.extend(_encode_string(c) for c in msg.columns)
        parts.append(struct.pack("<I", len(msg.rows)))
        for row in msg.rows:
            for val in row:
                parts.append(_encode_string(val))
        payload = b"".join(parts)
        msg_type = MSG_RESULT_ROWS
    elif isinstance(msg, ResultScalar):
        payload = _encode_string(msg.value)
        msg_type = MSG_RESULT_SCALAR
    elif isinstance(msg, ResultOk):
        payload = struct.pack("<Q", msg.affected)
        msg_type = MSG_RESULT_OK
    elif isinstance(msg, Error):
        payload = _encode_string(msg.message)
        msg_type = MSG_ERROR
    elif isinstance(msg, Disconnect):
        payload = b""
        msg_type = MSG_DISCONNECT
    else:
        raise TypeError(f"unknown message: {type(msg)!r}")

    header = struct.pack("<BBI", msg_type, 0, len(payload))
    return header + payload


def try_decode(buf: bytes) -> Optional[tuple[Message, int]]:
    """Parse a single frame from the start of `buf`.

    Returns (message, consumed_bytes) or None if the buffer does not yet hold
    a complete frame.
    """
    if len(buf) < 6:
        return None
    msg_type = buf[0]
    # flags byte at buf[1] currently unused
    (payload_len,) = struct.unpack_from("<I", buf, 2)
    if len(buf) < 6 + payload_len:
        return None
    payload = buf[6 : 6 + payload_len]
    msg = _decode_payload(msg_type, payload)
    return msg, 6 + payload_len


def _decode_payload(msg_type: int, payload: bytes) -> Message:
    pos = 0
    if msg_type == MSG_CONNECT:
        db_name, pos = _decode_string(payload, pos)
        password: Optional[str] = None
        if pos < len(payload):
            p, pos = _decode_string(payload, pos)
            password = p if p else None
        return Connect(db_name=db_name, password=password)
    if msg_type == MSG_CONNECT_OK:
        version, _ = _decode_string(payload, 0)
        return ConnectOk(version=version)
    if msg_type == MSG_QUERY:
        q, _ = _decode_string(payload, 0)
        return Query(query=q)
    if msg_type == MSG_RESULT_ROWS:
        (col_count,) = struct.unpack_from("<H", payload, pos)
        pos += 2
        columns: list[str] = []
        for _ in range(col_count):
            c, pos = _decode_string(payload, pos)
            columns.append(c)
        (row_count,) = struct.unpack_from("<I", payload, pos)
        pos += 4
        rows: list[list[str]] = []
        for _ in range(row_count):
            row: list[str] = []
            for _ in range(col_count):
                v, pos = _decode_string(payload, pos)
                row.append(v)
            rows.append(row)
        return ResultRows(columns=columns, rows=rows)
    if msg_type == MSG_RESULT_SCALAR:
        v, _ = _decode_string(payload, 0)
        return ResultScalar(value=v)
    if msg_type == MSG_RESULT_OK:
        if len(payload) < 8:
            raise ValueError("truncated result ok payload")
        (affected,) = struct.unpack_from("<Q", payload, 0)
        return ResultOk(affected=affected)
    if msg_type == MSG_ERROR:
        m, _ = _decode_string(payload, 0)
        return Error(message=m)
    if msg_type == MSG_DISCONNECT:
        return Disconnect()
    raise ValueError(f"unknown message type: {msg_type:#x}")
