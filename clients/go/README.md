# PowDB — Go client

Go client for [PowDB](https://github.com/zvndev/powdb), speaking the PowDB wire protocol over TCP.

## Install

```bash
go get github.com/zvndev/powdb/clients/go
```

## Usage

```go
package main

import (
    "context"
    "fmt"
    "log"

    powdb "github.com/zvndev/powdb/clients/go"
)

func main() {
    ctx := context.Background()
    c, err := powdb.Dial(ctx, powdb.Options{Host: "127.0.0.1", Port: 5433})
    if err != nil { log.Fatal(err) }
    defer c.Close()

    fmt.Println("server", c.ServerVersion)

    c.Query("create table User { name: string, age: int }")
    c.Query("User insert { name = 'Alice', age = 30 }")

    res, err := c.Query("User filter .age > 27 { .name, .age }")
    if err != nil { log.Fatal(err) }

    switch r := res.(type) {
    case *powdb.Rows:
        fmt.Println(r.Columns)
        for _, row := range r.Rows {
            fmt.Println(row)
        }
    case *powdb.Scalar:
        fmt.Println(r.Value)
    case *powdb.Ok:
        fmt.Println("affected", r.Affected)
    }
}
```

### Concurrency

A `Client` serialises calls internally; each PowQL request waits for its reply
before the next is sent. For concurrency use one `Client` per goroutine or a
connection pool.

## Testing

Unit tests (no server):

```bash
go test ./clients/go/...
```

## License

MIT
