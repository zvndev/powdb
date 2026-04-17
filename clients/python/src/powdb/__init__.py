"""PowDB Python client.

Synchronous client for the PowDB wire protocol.

    from powdb import Client

    with Client.connect(host="127.0.0.1", port=5433) as c:
        result = c.query("User filter .age > 27 { .name, .age }")
        for row in result.rows:
            print(row)
"""

from .client import Client, Rows, Scalar, Ok, QueryResult, PowDBError
from .protocol import Message

__all__ = [
    "Client",
    "Rows",
    "Scalar",
    "Ok",
    "QueryResult",
    "PowDBError",
    "Message",
]

__version__ = "0.1.0"
