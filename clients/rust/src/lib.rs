//! Synchronous Rust client for PowDB.
//!
//! ```no_run
//! use powdb_client::{Client, QueryResult};
//!
//! # fn main() -> Result<(), powdb_client::Error> {
//! let mut c = Client::connect("127.0.0.1", 5433)?;
//! match c.query("User filter .age > 27 { .name, .age }")? {
//!     QueryResult::Rows { columns, rows } => {
//!         println!("{:?}", columns);
//!         for r in rows { println!("{:?}", r); }
//!     }
//!     QueryResult::Scalar(v) => println!("{v}"),
//!     QueryResult::Ok { affected } => println!("{affected} rows"),
//! }
//! c.close()?;
//! # Ok(())
//! # }
//! ```

pub mod protocol;

use std::fmt;
use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

pub use protocol::{Message, encode, try_decode, DecodeError};

/// Typed result of a `query()` call.
#[derive(Debug, Clone)]
pub enum QueryResult {
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
    },
    Scalar(String),
    Ok {
        affected: u64,
    },
}

/// Error raised by client operations.
#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Protocol(String),
    Server(String),
    Decode(DecodeError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "io: {e}"),
            Error::Protocol(m) => write!(f, "protocol: {m}"),
            Error::Server(m) => write!(f, "server: {m}"),
            Error::Decode(e) => write!(f, "decode: {e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::Decode(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}
impl From<DecodeError> for Error {
    fn from(e: DecodeError) -> Self {
        Error::Decode(e)
    }
}

/// Synchronous PowDB client over a TCP socket.
pub struct Client {
    stream: TcpStream,
    buf: Vec<u8>,
    server_version: String,
    closed: bool,
}

/// Optional knobs for `Client::connect_with`.
#[derive(Debug, Clone)]
pub struct ConnectOptions<'a> {
    pub db_name: &'a str,
    pub password: Option<&'a str>,
    pub connect_timeout: Duration,
}

impl Default for ConnectOptions<'_> {
    fn default() -> Self {
        Self {
            db_name: "default",
            password: None,
            connect_timeout: Duration::from_secs(5),
        }
    }
}

impl Client {
    /// Connect with default options: db "default", no password, 5s timeout.
    pub fn connect(host: &str, port: u16) -> Result<Self, Error> {
        Self::connect_with(host, port, &ConnectOptions::default())
    }

    /// Connect with custom options.
    pub fn connect_with(host: &str, port: u16, opts: &ConnectOptions) -> Result<Self, Error> {
        let addr = (host, port)
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| Error::Protocol(format!("no address for {host}:{port}")))?;
        let stream = TcpStream::connect_timeout(&addr, opts.connect_timeout)?;
        stream.set_nodelay(true)?;

        let mut c = Self {
            stream,
            buf: Vec::with_capacity(8192),
            server_version: String::new(),
            closed: false,
        };
        c.write_msg(&Message::Connect {
            db_name: opts.db_name.to_string(),
            password: opts.password.map(String::from),
        })?;
        match c.read_msg()? {
            Message::ConnectOk { version } => c.server_version = version,
            Message::Error { message } => return Err(Error::Server(message)),
            other => {
                return Err(Error::Protocol(format!(
                    "expected ConnectOk, got {other:?}"
                )));
            }
        }
        Ok(c)
    }

    /// Server-reported version string from the handshake.
    pub fn server_version(&self) -> &str {
        &self.server_version
    }

    /// Run a PowQL statement.
    pub fn query(&mut self, q: &str) -> Result<QueryResult, Error> {
        if self.closed {
            return Err(Error::Protocol("client is closed".into()));
        }
        self.write_msg(&Message::Query {
            query: q.to_string(),
        })?;
        match self.read_msg()? {
            Message::ResultRows { columns, rows } => Ok(QueryResult::Rows { columns, rows }),
            Message::ResultScalar { value } => Ok(QueryResult::Scalar(value)),
            Message::ResultOk { affected } => Ok(QueryResult::Ok { affected }),
            Message::Error { message } => Err(Error::Server(message)),
            other => Err(Error::Protocol(format!("unexpected reply: {other:?}"))),
        }
    }

    /// Send Disconnect and close the socket.
    pub fn close(mut self) -> Result<(), Error> {
        self.shutdown()
    }

    fn shutdown(&mut self) -> Result<(), Error> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        let _ = self.write_msg(&Message::Disconnect);
        let _ = self.stream.shutdown(std::net::Shutdown::Both);
        Ok(())
    }

    fn write_msg(&mut self, m: &Message) -> Result<(), Error> {
        self.stream.write_all(&encode(m))?;
        Ok(())
    }

    fn read_msg(&mut self) -> Result<Message, Error> {
        loop {
            if let Some((msg, consumed)) = try_decode(&self.buf)? {
                self.buf.drain(..consumed);
                return Ok(msg);
            }
            let mut chunk = [0u8; 65536];
            let n = self.stream.read(&mut chunk)?;
            if n == 0 {
                return Err(Error::Protocol("connection closed by server".into()));
            }
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
