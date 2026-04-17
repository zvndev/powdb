"""Unit tests for the wire protocol (no server required)."""

import unittest

from powdb.protocol import (
    Connect,
    ConnectOk,
    Disconnect,
    Error,
    Query,
    ResultOk,
    ResultRows,
    ResultScalar,
    encode,
    try_decode,
)


def roundtrip(msg):
    decoded = try_decode(encode(msg))
    assert decoded is not None
    out, consumed = decoded
    assert consumed == len(encode(msg))
    return out


class ProtocolTests(unittest.TestCase):
    def test_connect_with_password(self):
        out = roundtrip(Connect(db_name="default", password="secret"))
        self.assertIsInstance(out, Connect)
        self.assertEqual(out.db_name, "default")
        self.assertEqual(out.password, "secret")

    def test_connect_no_password(self):
        out = roundtrip(Connect(db_name="default", password=None))
        self.assertEqual(out.password, None)

    def test_connect_ok(self):
        out = roundtrip(ConnectOk(version="0.1.2"))
        self.assertEqual(out.version, "0.1.2")

    def test_query(self):
        out = roundtrip(Query(query="User filter .age > 30"))
        self.assertEqual(out.query, "User filter .age > 30")

    def test_result_rows(self):
        out = roundtrip(
            ResultRows(
                columns=["name", "age"],
                rows=[["Alice", "30"], ["Bob", "25"]],
            )
        )
        self.assertEqual(out.columns, ["name", "age"])
        self.assertEqual(out.rows, [["Alice", "30"], ["Bob", "25"]])

    def test_result_rows_empty(self):
        out = roundtrip(ResultRows(columns=["x"], rows=[]))
        self.assertEqual(out.rows, [])

    def test_result_scalar(self):
        out = roundtrip(ResultScalar(value="42"))
        self.assertEqual(out.value, "42")

    def test_result_ok(self):
        out = roundtrip(ResultOk(affected=2**40))
        self.assertEqual(out.affected, 2**40)

    def test_error(self):
        out = roundtrip(Error(message="table not found"))
        self.assertEqual(out.message, "table not found")

    def test_disconnect(self):
        out = roundtrip(Disconnect())
        self.assertIsInstance(out, Disconnect)

    def test_partial_frame_returns_none(self):
        full = encode(Query(query="hello"))
        self.assertIsNone(try_decode(full[:3]))
        self.assertIsNone(try_decode(full[:-1]))

    def test_utf8_roundtrip(self):
        out = roundtrip(ResultScalar(value="café ☕ 日本語"))
        self.assertEqual(out.value, "café ☕ 日本語")


if __name__ == "__main__":
    unittest.main()
