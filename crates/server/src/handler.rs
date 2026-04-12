use crate::protocol::Message;
use powdb_query::executor::{Engine, is_read_only_statement, READONLY_NEEDS_WRITE};
use powdb_query::parser;
use powdb_query::result::QueryResult;
use powdb_storage::types::Value;
use std::sync::{Arc, RwLock};
use tokio::net::TcpStream;
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tracing::{info, debug, warn, error};

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
/// parallel; mutations acquire `.write()`. Parse failures, subquery
/// planning errors, and anything that needs the write lock due to dirty
/// materialized views all fall through to the write path for a uniform
/// error shape.
///
/// Mission infra-1: this is the entry point that replaces the old
/// "every query locks the whole engine" behaviour.
fn dispatch_query(engine: &Arc<RwLock<Engine>>, query: &str) -> Result<QueryResult, String> {
    // Parse once at the handler level so we can classify the statement
    // without touching the engine. This is the same lex+parse cost the
    // engine would pay anyway; we just hoist it out of the lock critical
    // section so concurrent readers don't serialise on lexing either.
    let stmt_result = parser::parse(query).map_err(|e| e.message);

    let can_try_read = matches!(&stmt_result, Ok(s) if is_read_only_statement(s));
    if can_try_read {
        // Read lock is released before we drop into the write path on
        // fallback — critical to avoid deadlock if the read fails with
        // READONLY_NEEDS_WRITE (dirty view or parser-vs-plan discrepancy).
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

    // Write path: either the statement is a mutation, it failed to parse
    // (let the authoritative planner report the error), or the read path
    // asked for escalation.
    let mut eng = engine.write().map_err(|e| format!("lock poisoned: {e}"))?;
    eng.execute_powql(query)
}

pub async fn handle_connection(stream: TcpStream, engine: Arc<RwLock<Engine>>, expected_password: Option<String>) {
    let peer = stream.peer_addr().ok().map(|a| a.to_string()).unwrap_or_else(|| "unknown".into());
    let (reader, writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut writer = BufWriter::new(writer);

    // Wait for Connect message
    match Message::read_from(&mut reader).await {
        Ok(Some(Message::Connect { db_name, password })) => {
            // Check password if server requires one
            if let Some(expected) = &expected_password {
                if !password.as_deref().map_or(false, |p| constant_time_eq(p.as_bytes(), expected.as_bytes())) {
                    warn!(peer = %peer, db = %db_name, "auth rejected: bad password");
                    let err = Message::Error { message: "authentication failed".into() };
                    err.write_to(&mut writer).await.ok();
                    writer.flush().await.ok();
                    return;
                }
            }
            info!(peer = %peer, db = %db_name, "client connected");
            let ok = Message::ConnectOk { version: "0.1.0".into() };
            if ok.write_to(&mut writer).await.is_err() { return; }
            if writer.flush().await.is_err() { return; }
        }
        Ok(Some(_)) => {
            warn!(peer = %peer, "first message was not CONNECT");
            let err = Message::Error { message: "expected CONNECT".into() };
            err.write_to(&mut writer).await.ok();
            writer.flush().await.ok();
            return;
        }
        Ok(None) => {
            debug!(peer = %peer, "client closed before CONNECT");
            return;
        }
        Err(e) => {
            error!(peer = %peer, error = %e, "error reading CONNECT");
            return;
        }
    }

    // Main query loop
    loop {
        let msg = match Message::read_from(&mut reader).await {
            Ok(Some(msg)) => msg,
            Ok(None) => break,
            Err(e) => {
                error!(peer = %peer, error = %e, "read error");
                break;
            }
        };

        let response = match msg {
            Message::Query { query } => {
                debug!(peer = %peer, query = %query, "received query");
                match dispatch_query(&engine, &query) {
                    Ok(result) => query_result_to_message(result),
                    Err(e) => Message::Error { message: sanitize_error(&e) },
                }
            }
            Message::Disconnect => {
                debug!(peer = %peer, "received DISCONNECT");
                break;
            }
            _ => Message::Error { message: "unexpected message type".into() },
        };

        if response.write_to(&mut writer).await.is_err() { break; }
        if writer.flush().await.is_err() { break; }
    }

    info!(peer = %peer, "client disconnected");
}

fn query_result_to_message(result: QueryResult) -> Message {
    match result {
        QueryResult::Rows { columns, rows } => {
            let str_rows: Vec<Vec<String>> = rows.iter().map(|row| {
                row.iter().map(value_to_display).collect()
            }).collect();
            Message::ResultRows { columns, rows: str_rows }
        }
        QueryResult::Scalar(val) => {
            Message::ResultScalar { value: value_to_display(&val) }
        }
        QueryResult::Modified(n) => {
            Message::ResultOk { affected: n }
        }
        QueryResult::Created(_name) => {
            Message::ResultOk { affected: 0 }
        }
        QueryResult::Executed { .. } => {
            Message::ResultOk { affected: 0 }
        }
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
