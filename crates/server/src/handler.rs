use crate::protocol::Message;
use powdb_query::executor::{is_read_only_statement, Engine, READONLY_NEEDS_WRITE};
use powdb_query::parser;
use powdb_query::result::QueryResult;
use powdb_storage::types::Value;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

/// Constant-time byte comparison to prevent timing side-channel attacks
/// on password verification. Returns `true` iff `a` and `b` are identical.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Error messages that are safe to forward to the client verbatim.
const SAFE_ERROR_PREFIXES: &[&str] = &[
    "table not found",
    "column not found",
    "parse error",
    "type mismatch",
    "unknown table",
    "unknown column",
    "unknown function",
    "syntax error",
    "expected",
    "unexpected",
    "missing",
    "duplicate",
    "invalid",
    "cannot",
    "no such",
    "already exists",
];

/// Sanitize an error message before sending it to the client.
/// Known safe errors are passed through; everything else is replaced
/// with a generic message to avoid leaking internal details.
fn sanitize_error(e: &str) -> String {
    let lower = e.to_lowercase();
    for prefix in SAFE_ERROR_PREFIXES {
        if lower.starts_with(prefix) {
            return e.to_string();
        }
    }
    "query execution error".into()
}

/// Execute a query against the engine under the RwLock. Read-only
/// statements acquire `.read()` so concurrent SELECTs can scan in
/// parallel; mutations acquire `.write()`.
fn dispatch_query(engine: &Arc<RwLock<Engine>>, query: &str) -> Result<QueryResult, String> {
    let stmt_result = parser::parse(query).map_err(|e| e.to_string());

    let can_try_read = matches!(&stmt_result, Ok(s) if is_read_only_statement(s));
    if can_try_read {
        let res = {
            let eng = engine.read().map_err(|e| format!("lock poisoned: {e}"))?;
            eng.execute_powql_readonly(query)
        };
        match res {
            Ok(r) => return Ok(r),
            Err(e) if e == READONLY_NEEDS_WRITE => {
                // Escalate: fall through to the write path below.
            }
            Err(e) => return Err(e),
        }
    }

    let mut eng = engine.write().map_err(|e| format!("lock poisoned: {e}"))?;
    eng.execute_powql(query)
}

pub async fn handle_connection(
    stream: TcpStream,
    engine: Arc<RwLock<Engine>>,
    expected_password: Option<String>,
    shutdown_rx: &mut watch::Receiver<bool>,
    idle_timeout: Duration,
    query_timeout: Duration,
) {
    let peer = stream
        .peer_addr()
        .ok()
        .map(|a| a.to_string())
        .unwrap_or_else(|| "unknown".into());
    let (reader, writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut writer = BufWriter::new(writer);

    // Wait for Connect message (with idle timeout).
    let connect_msg =
        match tokio::time::timeout(idle_timeout, Message::read_from(&mut reader)).await {
            Ok(Ok(Some(msg))) => msg,
            Ok(Ok(None)) => {
                debug!(peer = %peer, "client closed before CONNECT");
                return;
            }
            Ok(Err(e)) => {
                error!(peer = %peer, error = %e, "error reading CONNECT");
                return;
            }
            Err(_) => {
                warn!(peer = %peer, "idle timeout waiting for CONNECT");
                return;
            }
        };

    match connect_msg {
        Message::Connect { db_name, password } => {
            if let Some(expected) = &expected_password {
                if !password
                    .as_deref()
                    .is_some_and(|p| constant_time_eq(p.as_bytes(), expected.as_bytes()))
                {
                    warn!(peer = %peer, db = %db_name, "auth rejected: bad password");
                    let err = Message::Error {
                        message: "authentication failed".into(),
                    };
                    err.write_to(&mut writer).await.ok();
                    writer.flush().await.ok();
                    return;
                }
            }
            info!(peer = %peer, db = %db_name, "client connected");
            let ok = Message::ConnectOk {
                version: "0.1.0".into(),
            };
            if ok.write_to(&mut writer).await.is_err() {
                return;
            }
            if writer.flush().await.is_err() {
                return;
            }
        }
        _ => {
            warn!(peer = %peer, "first message was not CONNECT");
            let err = Message::Error {
                message: "expected CONNECT".into(),
            };
            err.write_to(&mut writer).await.ok();
            writer.flush().await.ok();
            return;
        }
    }

    // Main query loop with idle timeout and shutdown awareness.
    loop {
        let msg = tokio::select! {
            // Read next message with idle timeout.
            result = tokio::time::timeout(idle_timeout, Message::read_from(&mut reader)) => {
                match result {
                    Ok(Ok(Some(msg))) => msg,
                    Ok(Ok(None)) => break,
                    Ok(Err(e)) => {
                        error!(peer = %peer, error = %e, "read error");
                        break;
                    }
                    Err(_) => {
                        info!(peer = %peer, "idle timeout, closing connection");
                        let err = Message::Error { message: "idle timeout".into() };
                        err.write_to(&mut writer).await.ok();
                        writer.flush().await.ok();
                        break;
                    }
                }
            }
            // If server is shutting down, notify client and close.
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    info!(peer = %peer, "server shutting down, closing connection");
                    let err = Message::Error { message: "server shutting down".into() };
                    err.write_to(&mut writer).await.ok();
                    writer.flush().await.ok();
                    break;
                }
                continue;
            }
        };

        let response = match msg {
            Message::Query { query } => {
                debug!(peer = %peer, query = %query, "received query");
                // Run query with timeout.
                let result = tokio::task::spawn_blocking({
                    let engine = engine.clone();
                    let query = query.clone();
                    move || dispatch_query(&engine, &query)
                });
                match tokio::time::timeout(query_timeout, result).await {
                    Ok(Ok(Ok(result))) => query_result_to_message(result),
                    Ok(Ok(Err(e))) => Message::Error {
                        message: sanitize_error(&e),
                    },
                    Ok(Err(e)) => Message::Error {
                        message: format!("internal error: {e}"),
                    },
                    Err(_) => {
                        warn!(peer = %peer, query = %query, "query timeout exceeded");
                        Message::Error {
                            message: "query timeout exceeded".into(),
                        }
                    }
                }
            }
            Message::Disconnect => {
                debug!(peer = %peer, "received DISCONNECT");
                break;
            }
            _ => Message::Error {
                message: "unexpected message type".into(),
            },
        };

        if response.write_to(&mut writer).await.is_err() {
            break;
        }
        if writer.flush().await.is_err() {
            break;
        }
    }

    info!(peer = %peer, "client disconnected");
}

fn query_result_to_message(result: QueryResult) -> Message {
    match result {
        QueryResult::Rows { columns, rows } => {
            let str_rows: Vec<Vec<String>> = rows
                .iter()
                .map(|row| row.iter().map(value_to_display).collect())
                .collect();
            Message::ResultRows {
                columns,
                rows: str_rows,
            }
        }
        QueryResult::Scalar(val) => Message::ResultScalar {
            value: value_to_display(&val),
        },
        QueryResult::Modified(n) => Message::ResultOk { affected: n },
        QueryResult::Created(_name) => Message::ResultOk { affected: 0 },
        QueryResult::Executed { .. } => Message::ResultOk { affected: 0 },
    }
}

fn value_to_display(v: &Value) -> String {
    match v {
        Value::Int(n)      => n.to_string(),
        Value::Float(n)    => format!("{n}"),
        Value::Bool(b)     => b.to_string(),
        Value::Str(s)      => s.clone(),
        Value::DateTime(t) => format!("{t}"),
        Value::Uuid(u)     => format!("{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            u[0], u[1], u[2], u[3], u[4], u[5], u[6], u[7],
            u[8], u[9], u[10], u[11], u[12], u[13], u[14], u[15]),
        Value::Bytes(b)    => format!("<{} bytes>", b.len()),
        Value::Empty       => "{}".into(),
    }
}
