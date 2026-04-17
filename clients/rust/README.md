# PowDB — Rust client

Synchronous Rust client for [PowDB](https://github.com/zvndev/powdb).

## Install

```toml
[dependencies]
powdb-client = "0.1"
```

## Usage

```rust
use powdb_client::{Client, QueryResult};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut c = Client::connect("127.0.0.1", 5433)?;
    println!("server {}", c.server_version());

    c.query("create table User { name: string, age: int }")?;
    c.query("User insert { name = 'Alice', age = 30 }")?;

    match c.query("User filter .age > 27 { .name, .age }")? {
        QueryResult::Rows { columns, rows } => {
            println!("{:?}", columns);
            for r in rows { println!("{:?}", r); }
        }
        QueryResult::Scalar(v) => println!("{v}"),
        QueryResult::Ok { affected } => println!("{affected}"),
    }

    c.close()?;
    Ok(())
}
```

The crate is `std`-only — no async runtime, no third-party deps. The client is
a thin wrapper around `std::net::TcpStream`; serialise your own concurrency or
use one client per thread.

## Testing

```bash
cd clients/rust && cargo test
```

## License

MIT
