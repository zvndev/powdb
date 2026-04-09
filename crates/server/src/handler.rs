use crate::protocol::Message;
use powdb_query::executor::Engine;
use powdb_query::result::QueryResult;
use powdb_storage::types::Value;
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tracing::{info, debug, warn, error};

pub async fn handle_connection(stream: TcpStream, engine: Arc<Mutex<Engine>>, expected_password: Option<String>) {
    let peer = stream.peer_addr().ok().map(|a| a.to_string()).unwrap_or_else(|| "unknown".into());
    let (reader, writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut writer = BufWriter::new(writer);

    // Wait for Connect message
    match Message::read_from(&mut reader).await {
        Ok(Some(Message::Connect { db_name, password })) => {
            // Check password if server requires one
            if let Some(expected) = &expected_password {
                if password.as_deref() != Some(expected.as_str()) {
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
                let mut eng = engine.lock().unwrap();
                match eng.execute_powql(&query) {
                    Ok(result) => query_result_to_message(result),
                    Err(e) => Message::Error { message: e },
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
