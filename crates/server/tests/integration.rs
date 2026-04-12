use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn encode_connect(db: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(db.len() as u32).to_le_bytes());
    payload.extend_from_slice(db.as_bytes());
    // Empty password (len=0) means None
    payload.extend_from_slice(&0u32.to_le_bytes());
    let mut frame = Vec::new();
    frame.push(0x01); // CONNECT
    frame.push(0);    // flags
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&payload);
    frame
}

fn encode_query(q: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(q.len() as u32).to_le_bytes());
    payload.extend_from_slice(q.as_bytes());
    let mut frame = Vec::new();
    frame.push(0x03); // QUERY
    frame.push(0);
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&payload);
    frame
}

async fn read_response(stream: &mut TcpStream) -> Vec<u8> {
    let mut header = [0u8; 6];
    stream.read_exact(&mut header).await.unwrap();
    let payload_len = u32::from_le_bytes(header[2..6].try_into().unwrap()) as usize;
    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        stream.read_exact(&mut payload).await.unwrap();
    }
    let mut full = Vec::new();
    full.extend_from_slice(&header);
    full.extend_from_slice(&payload);
    full
}

#[tokio::test]
async fn test_full_lifecycle() {
    // Use a unique port and temp dir to avoid conflicts with parallel tests
    let test_id = std::process::id();
    let port = 15433 + (test_id % 1000) as u16;
    let data_dir = std::env::temp_dir().join(format!("powdb_integ_{test_id}"));
    std::fs::create_dir_all(&data_dir).unwrap();
    let data_dir_str = data_dir.to_str().unwrap().to_string();

    let addr = format!("127.0.0.1:{port}");
    let bind_addr = addr.clone();

    // Start server in background
    let handle = tokio::spawn(async move {
        let engine = powdb_query::executor::Engine::new(std::path::Path::new(&data_dir_str)).unwrap();
        let engine = std::sync::Arc::new(std::sync::RwLock::new(engine));
        let listener = tokio::net::TcpListener::bind(&bind_addr).await.unwrap();

        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let eng = engine.clone();
            let (_, mut rx) = tokio::sync::watch::channel(false);
            tokio::spawn(async move {
                powdb_server::handler::handle_connection(
                    stream, eng, None, &mut rx,
                    Duration::from_secs(300),
                    Duration::from_secs(30),
                ).await;
            });
        }
    });

    // Give server time to bind
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect
    let mut stream = TcpStream::connect(&addr).await.unwrap();
    stream.write_all(&encode_connect("testdb")).await.unwrap();
    let resp = read_response(&mut stream).await;
    assert_eq!(resp[0], 0x02, "expected CONNECT_OK");

    // Create table
    stream.write_all(&encode_query("type User { required name: str, age: int }")).await.unwrap();
    let resp = read_response(&mut stream).await;
    assert_eq!(resp[0], 0x09, "expected RESULT_OK for create type");

    // Insert row
    stream.write_all(&encode_query(r#"insert User { name := "Alice", age := 30 }"#)).await.unwrap();
    let resp = read_response(&mut stream).await;
    assert_eq!(resp[0], 0x09, "expected RESULT_OK for insert");

    // Insert another row
    stream.write_all(&encode_query(r#"insert User { name := "Bob", age := 25 }"#)).await.unwrap();
    let resp = read_response(&mut stream).await;
    assert_eq!(resp[0], 0x09, "expected RESULT_OK for second insert");

    // Query all rows
    stream.write_all(&encode_query("User")).await.unwrap();
    let resp = read_response(&mut stream).await;
    assert_eq!(resp[0], 0x07, "expected RESULT_ROWS");

    // Count
    stream.write_all(&encode_query("count(User)")).await.unwrap();
    let resp = read_response(&mut stream).await;
    assert_eq!(resp[0], 0x08, "expected RESULT_SCALAR for count");

    // Filter query
    stream.write_all(&encode_query("User filter .age > 27")).await.unwrap();
    let resp = read_response(&mut stream).await;
    assert_eq!(resp[0], 0x07, "expected RESULT_ROWS for filter");

    // Decode the filtered rows to verify content
    let decoded = powdb_server::protocol::Message::decode(&resp).unwrap();
    match decoded {
        powdb_server::protocol::Message::ResultRows { columns: _, rows } => {
            assert_eq!(rows.len(), 1, "filter should return only Alice");
            assert_eq!(rows[0][0], "Alice");
        }
        other => panic!("expected ResultRows, got {other:?}"),
    }

    // Cleanup
    handle.abort();
    std::fs::remove_dir_all(&data_dir).ok();
}
