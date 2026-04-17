# PowDB — Python client

Synchronous Python client for [PowDB](https://github.com/zvndev/powdb), speaking the PowDB wire protocol over TCP.

## Install

```bash
pip install powdb
```

From source:

```bash
pip install -e clients/python
```

## Usage

```python
from powdb import Client

with Client.connect(host="127.0.0.1", port=5433) as c:
    print("server", c.server_version)

    c.query("create table User { name: string, age: int }")
    c.query("User insert { name = 'Alice', age = 30 }")

    result = c.query("User filter .age > 27 { .name, .age }")
    if result.kind == "rows":
        print(result.columns)
        for row in result.rows:
            print(row)
```

### Result shapes

`query()` returns one of three dataclasses:

| Class    | `.kind`    | Fields                         |
|----------|------------|--------------------------------|
| `Rows`   | `"rows"`   | `columns: list[str]`, `rows: list[list[str]]` |
| `Scalar` | `"scalar"` | `value: str`                   |
| `Ok`     | `"ok"`     | `affected: int`                |

Server errors raise `PowDBError`.

## Testing

Tests require a running PowDB server:

```bash
POWDB_HOST=127.0.0.1 POWDB_PORT=15433 python -m unittest discover -s clients/python/tests
```

## License

MIT
