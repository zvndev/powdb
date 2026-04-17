//! PowDB wire protocol.
//!
//! Frame format: `[type(1)][flags(1)][len(4 LE)][payload]`.
//! Strings are encoded as `[len(4 LE)][utf-8 bytes]`.
//!
//! Mirrors `crates/server/src/protocol.rs`.

use std::fmt;

pub const MSG_CONNECT: u8 = 0x01;
pub const MSG_CONNECT_OK: u8 = 0x02;
pub const MSG_QUERY: u8 = 0x03;
pub const MSG_RESULT_ROWS: u8 = 0x07;
pub const MSG_RESULT_SCALAR: u8 = 0x08;
pub const MSG_RESULT_OK: u8 = 0x09;
pub const MSG_ERROR: u8 = 0x0A;
pub const MSG_DISCONNECT: u8 = 0x10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    Connect {
        db_name: String,
        password: Option<String>,
    },
    ConnectOk {
        version: String,
    },
    Query {
        query: String,
    },
    ResultRows {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
    },
    ResultScalar {
        value: String,
    },
    ResultOk {
        affected: u64,
    },
    Error {
        message: String,
    },
    Disconnect,
}

#[derive(Debug)]
pub enum DecodeError {
    Truncated(&'static str),
    UnknownType(u8),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::Truncated(what) => write!(f, "truncated {what}"),
            DecodeError::UnknownType(t) => write!(f, "unknown message type: 0x{t:x}"),
        }
    }
}

impl std::error::Error for DecodeError {}

pub fn encode(msg: &Message) -> Vec<u8> {
    let (msg_type, payload) = match msg {
        Message::Connect { db_name, password } => {
            let mut buf = encode_string(db_name);
            match password {
                Some(p) => buf.extend_from_slice(&encode_string(p)),
                None => buf.extend_from_slice(&0u32.to_le_bytes()),
            }
            (MSG_CONNECT, buf)
        }
        Message::ConnectOk { version } => (MSG_CONNECT_OK, encode_string(version)),
        Message::Query { query } => (MSG_QUERY, encode_string(query)),
        Message::ResultRows { columns, rows } => {
            let mut buf = Vec::new();
            buf.extend_from_slice(&(columns.len() as u16).to_le_bytes());
            for col in columns {
                buf.extend_from_slice(&encode_string(col));
            }
            buf.extend_from_slice(&(rows.len() as u32).to_le_bytes());
            for row in rows {
                for val in row {
                    buf.extend_from_slice(&encode_string(val));
                }
            }
            (MSG_RESULT_ROWS, buf)
        }
        Message::ResultScalar { value } => (MSG_RESULT_SCALAR, encode_string(value)),
        Message::ResultOk { affected } => (MSG_RESULT_OK, affected.to_le_bytes().to_vec()),
        Message::Error { message } => (MSG_ERROR, encode_string(message)),
        Message::Disconnect => (MSG_DISCONNECT, Vec::new()),
    };

    let mut frame = Vec::with_capacity(6 + payload.len());
    frame.push(msg_type);
    frame.push(0);
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&payload);
    frame
}

/// Returns `Ok(Some((msg, consumed)))` if a full frame is present, `Ok(None)`
/// if more bytes are needed, or `Err` if the frame is malformed.
pub fn try_decode(buf: &[u8]) -> Result<Option<(Message, usize)>, DecodeError> {
    if buf.len() < 6 {
        return Ok(None);
    }
    let msg_type = buf[0];
    let payload_len = u32::from_le_bytes(buf[2..6].try_into().unwrap()) as usize;
    if buf.len() < 6 + payload_len {
        return Ok(None);
    }
    let payload = &buf[6..6 + payload_len];
    Ok(Some((decode_payload(msg_type, payload)?, 6 + payload_len)))
}

fn decode_payload(msg_type: u8, payload: &[u8]) -> Result<Message, DecodeError> {
    let mut pos = 0;
    match msg_type {
        MSG_CONNECT => {
            let db_name = decode_string(payload, &mut pos)?;
            let password = if pos < payload.len() {
                let p = decode_string(payload, &mut pos)?;
                if p.is_empty() { None } else { Some(p) }
            } else {
                None
            };
            Ok(Message::Connect { db_name, password })
        }
        MSG_CONNECT_OK => Ok(Message::ConnectOk {
            version: decode_string(payload, &mut pos)?,
        }),
        MSG_QUERY => Ok(Message::Query {
            query: decode_string(payload, &mut pos)?,
        }),
        MSG_RESULT_ROWS => {
            if payload.len() < 2 {
                return Err(DecodeError::Truncated("column count"));
            }
            let col_count = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
            pos = 2;
            let mut columns = Vec::with_capacity(col_count);
            for _ in 0..col_count {
                columns.push(decode_string(payload, &mut pos)?);
            }
            if pos + 4 > payload.len() {
                return Err(DecodeError::Truncated("row count"));
            }
            let row_count =
                u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            let mut rows = Vec::with_capacity(row_count);
            for _ in 0..row_count {
                let mut row = Vec::with_capacity(col_count);
                for _ in 0..col_count {
                    row.push(decode_string(payload, &mut pos)?);
                }
                rows.push(row);
            }
            Ok(Message::ResultRows { columns, rows })
        }
        MSG_RESULT_SCALAR => Ok(Message::ResultScalar {
            value: decode_string(payload, &mut pos)?,
        }),
        MSG_RESULT_OK => {
            if payload.len() < 8 {
                return Err(DecodeError::Truncated("result ok payload"));
            }
            Ok(Message::ResultOk {
                affected: u64::from_le_bytes(payload[0..8].try_into().unwrap()),
            })
        }
        MSG_ERROR => Ok(Message::Error {
            message: decode_string(payload, &mut pos)?,
        }),
        MSG_DISCONNECT => Ok(Message::Disconnect),
        t => Err(DecodeError::UnknownType(t)),
    }
}

fn encode_string(s: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + s.len());
    buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
    buf
}

fn decode_string(buf: &[u8], pos: &mut usize) -> Result<String, DecodeError> {
    if *pos + 4 > buf.len() {
        return Err(DecodeError::Truncated("string length"));
    }
    let len = u32::from_le_bytes(buf[*pos..*pos + 4].try_into().unwrap()) as usize;
    *pos += 4;
    if *pos + len > buf.len() {
        return Err(DecodeError::Truncated("string data"));
    }
    let s = String::from_utf8_lossy(&buf[*pos..*pos + len]).into_owned();
    *pos += len;
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(msg: Message) -> Message {
        let bytes = encode(&msg);
        let (out, consumed) = try_decode(&bytes).unwrap().unwrap();
        assert_eq!(consumed, bytes.len());
        out
    }

    #[test]
    fn connect_with_password() {
        let m = Message::Connect {
            db_name: "default".into(),
            password: Some("secret".into()),
        };
        assert_eq!(roundtrip(m.clone()), m);
    }

    #[test]
    fn connect_no_password() {
        let m = Message::Connect {
            db_name: "default".into(),
            password: None,
        };
        assert_eq!(roundtrip(m.clone()), m);
    }

    #[test]
    fn query() {
        let m = Message::Query {
            query: "User filter .age > 30".into(),
        };
        assert_eq!(roundtrip(m.clone()), m);
    }

    #[test]
    fn result_rows() {
        let m = Message::ResultRows {
            columns: vec!["name".into(), "age".into()],
            rows: vec![
                vec!["Alice".into(), "30".into()],
                vec!["Bob".into(), "25".into()],
            ],
        };
        assert_eq!(roundtrip(m.clone()), m);
    }

    #[test]
    fn result_ok_large() {
        let m = Message::ResultOk { affected: 1 << 40 };
        assert_eq!(roundtrip(m.clone()), m);
    }

    #[test]
    fn error_roundtrip() {
        let m = Message::Error {
            message: "table not found".into(),
        };
        assert_eq!(roundtrip(m.clone()), m);
    }

    #[test]
    fn disconnect() {
        assert_eq!(roundtrip(Message::Disconnect), Message::Disconnect);
    }

    #[test]
    fn partial_frame_returns_none() {
        let full = encode(&Message::Query {
            query: "hello".into(),
        });
        assert!(try_decode(&full[..3]).unwrap().is_none());
        assert!(try_decode(&full[..full.len() - 1]).unwrap().is_none());
    }

    #[test]
    fn utf8_roundtrip() {
        let m = Message::ResultScalar {
            value: "café ☕ 日本語".into(),
        };
        assert_eq!(roundtrip(m.clone()), m);
    }

    #[test]
    fn unknown_type_errors() {
        let mut bad = encode(&Message::Disconnect);
        bad[0] = 0xFF;
        assert!(try_decode(&bad).is_err());
    }
}
