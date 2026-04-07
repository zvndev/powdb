use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MSG_CONNECT: u8     = 0x01;
const MSG_CONNECT_OK: u8  = 0x02;
const MSG_QUERY: u8       = 0x03;
const MSG_RESULT_ROWS: u8 = 0x07;
const MSG_RESULT_SCALAR: u8 = 0x08;
const MSG_RESULT_OK: u8   = 0x09;
const MSG_ERROR: u8       = 0x0A;
const MSG_DISCONNECT: u8  = 0x10;

#[derive(Debug, Clone)]
pub enum Message {
    Connect { db_name: String, password: Option<String> },
    ConnectOk { version: String },
    Query { query: String },
    ResultRows {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
    },
    ResultScalar { value: String },
    ResultOk { affected: u64 },
    Error { message: String },
    Disconnect,
}

impl Message {
    /// Encode message into wire format: [type(1)][flags(1)][len(4)][payload]
    pub fn encode(&self) -> Vec<u8> {
        let (msg_type, payload) = match self {
            Message::Connect { db_name, password } => {
                let mut buf = encode_string(db_name);
                // Password is encoded as a length-prefixed string. Empty (len=0) means None.
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
        frame.push(0); // flags
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&payload);
        frame
    }

    /// Decode message from wire format.
    pub fn decode(data: &[u8]) -> Result<Message, String> {
        if data.len() < 6 {
            return Err("frame too short".into());
        }
        let msg_type = data[0];
        let _flags = data[1];
        let payload_len = u32::from_le_bytes(data[2..6].try_into().unwrap()) as usize;
        let payload = &data[6..6 + payload_len];

        match msg_type {
            MSG_CONNECT => {
                let mut pos = 0;
                let db_name = decode_string(payload, &mut pos)?;
                // Password is optional. If there are no more bytes, treat as None
                // (backwards compatible with old clients that don't send a password).
                let password = if pos < payload.len() {
                    let p = decode_string(payload, &mut pos)?;
                    if p.is_empty() { None } else { Some(p) }
                } else {
                    None
                };
                Ok(Message::Connect { db_name, password })
            }
            MSG_CONNECT_OK => {
                let version = decode_string(payload, &mut 0)?;
                Ok(Message::ConnectOk { version })
            }
            MSG_QUERY => {
                let query = decode_string(payload, &mut 0)?;
                Ok(Message::Query { query })
            }
            MSG_RESULT_ROWS => {
                let mut pos = 0;
                let col_count = u16::from_le_bytes(payload[pos..pos+2].try_into().unwrap()) as usize;
                pos += 2;
                let mut columns = Vec::with_capacity(col_count);
                for _ in 0..col_count {
                    columns.push(decode_string(payload, &mut pos)?);
                }
                let row_count = u32::from_le_bytes(payload[pos..pos+4].try_into().unwrap()) as usize;
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
            MSG_RESULT_SCALAR => {
                let value = decode_string(payload, &mut 0)?;
                Ok(Message::ResultScalar { value })
            }
            MSG_RESULT_OK => {
                let affected = u64::from_le_bytes(payload[0..8].try_into().unwrap());
                Ok(Message::ResultOk { affected })
            }
            MSG_ERROR => {
                let message = decode_string(payload, &mut 0)?;
                Ok(Message::Error { message })
            }
            MSG_DISCONNECT => Ok(Message::Disconnect),
            _ => Err(format!("unknown message type: {msg_type:#x}")),
        }
    }

    /// Write this message to an async writer.
    pub async fn write_to<W: AsyncWriteExt + Unpin>(&self, writer: &mut W) -> std::io::Result<()> {
        let bytes = self.encode();
        writer.write_all(&bytes).await
    }

    /// Read a message from an async reader.
    pub async fn read_from<R: AsyncReadExt + Unpin>(reader: &mut R) -> std::io::Result<Option<Message>> {
        let mut header = [0u8; 6];
        match reader.read_exact(&mut header).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
        let payload_len = u32::from_le_bytes(header[2..6].try_into().unwrap()) as usize;
        let mut payload = vec![0u8; payload_len];
        if payload_len > 0 {
            reader.read_exact(&mut payload).await?;
        }

        let mut full = Vec::with_capacity(6 + payload_len);
        full.extend_from_slice(&header);
        full.extend_from_slice(&payload);

        Message::decode(&full)
            .map(Some)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }
}

fn encode_string(s: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + s.len());
    buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
    buf
}

fn decode_string(data: &[u8], pos: &mut usize) -> Result<String, String> {
    if *pos + 4 > data.len() {
        return Err("truncated string length".into());
    }
    let len = u32::from_le_bytes(data[*pos..*pos+4].try_into().unwrap()) as usize;
    *pos += 4;
    if *pos + len > data.len() {
        return Err("truncated string data".into());
    }
    let s = String::from_utf8_lossy(&data[*pos..*pos+len]).into_owned();
    *pos += len;
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_query() {
        let msg = Message::Query {
            query: "User filter .age > 30".into(),
        };
        let bytes = msg.encode();
        let decoded = Message::decode(&bytes).unwrap();
        match decoded {
            Message::Query { query } => assert_eq!(query, "User filter .age > 30"),
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn test_encode_decode_result_rows() {
        let msg = Message::ResultRows {
            columns: vec!["name".into(), "age".into()],
            rows: vec![
                vec!["Alice".into(), "30".into()],
                vec!["Bob".into(), "25".into()],
            ],
        };
        let bytes = msg.encode();
        let decoded = Message::decode(&bytes).unwrap();
        match decoded {
            Message::ResultRows { columns, rows } => {
                assert_eq!(columns, vec!["name", "age"]);
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("expected ResultRows"),
        }
    }

    #[test]
    fn test_encode_decode_error() {
        let msg = Message::Error { message: "table not found".into() };
        let bytes = msg.encode();
        let decoded = Message::decode(&bytes).unwrap();
        match decoded {
            Message::Error { message } => assert_eq!(message, "table not found"),
            _ => panic!("expected Error"),
        }
    }

    #[test]
    fn test_frame_length() {
        let msg = Message::Query { query: "User".into() };
        let bytes = msg.encode();
        assert!(bytes.len() >= 6);
        let payload_len = u32::from_le_bytes(bytes[2..6].try_into().unwrap()) as usize;
        assert_eq!(bytes.len(), 6 + payload_len);
    }
}
