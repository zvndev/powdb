use batadb_query::executor::Engine;
use batadb_server::handler;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let port = std::env::var("BATADB_PORT").unwrap_or_else(|_| "5433".into());
    let data_dir = std::env::var("BATADB_DATA").unwrap_or_else(|_| "./batadb_data".into());

    let engine = Engine::new(std::path::Path::new(&data_dir))
        .expect("failed to initialize storage engine");
    let engine = Arc::new(Mutex::new(engine));

    let addr = format!("0.0.0.0:{port}");
    let listener = TcpListener::bind(&addr).await
        .unwrap_or_else(|_| panic!("failed to bind to {addr}"));
    eprintln!("[batadb] listening on {addr}");

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                eprintln!("[batadb] new connection from {peer}");
                let eng = engine.clone();
                tokio::spawn(async move {
                    handler::handle_connection(stream, eng).await;
                });
            }
            Err(e) => {
                eprintln!("[batadb] accept error: {e}");
            }
        }
    }
}
