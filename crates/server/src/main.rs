use powdb_query::executor::Engine;
use powdb_server::handler;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tracing::{info, error};
use tracing_subscriber::EnvFilter;

struct Args {
    port: u16,
    data_dir: String,
    password: Option<String>,
}

fn parse_args() -> Args {
    // Defaults from env vars (preserve old behavior), then overridden by CLI flags.
    let mut port: u16 = std::env::var("POWDB_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5433);
    let mut data_dir: String = std::env::var("POWDB_DATA").unwrap_or_else(|_| "./powdb_data".into());
    let mut password: Option<String> = std::env::var("POWDB_PASSWORD").ok().filter(|s| !s.is_empty());

    let argv: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < argv.len() {
        match argv[i].as_str() {
            "--port" | "-p" => {
                i += 1;
                if i >= argv.len() { eprintln!("--port requires a value"); std::process::exit(2); }
                port = argv[i].parse().unwrap_or_else(|_| { eprintln!("invalid port: {}", argv[i]); std::process::exit(2); });
            }
            "--data-dir" | "-d" => {
                i += 1;
                if i >= argv.len() { eprintln!("--data-dir requires a value"); std::process::exit(2); }
                data_dir = argv[i].clone();
            }
            "--password" => {
                i += 1;
                if i >= argv.len() { eprintln!("--password requires a value"); std::process::exit(2); }
                password = Some(argv[i].clone());
            }
            "--help" | "-h" => {
                println!("powdb-server — PowDB wire-protocol server");
                println!();
                println!("USAGE:");
                println!("    powdb-server [OPTIONS]");
                println!();
                println!("OPTIONS:");
                println!("    -p, --port <PORT>          TCP port to listen on (default: 5433)");
                println!("    -d, --data-dir <PATH>      Data directory (default: ./powdb_data)");
                println!("        --password <PW>        Require this password on CONNECT");
                println!("    -h, --help                 Print this message");
                println!();
                println!("ENVIRONMENT:");
                println!("    POWDB_PORT, POWDB_DATA, POWDB_PASSWORD");
                println!("    RUST_LOG=info|debug|trace  (defaults to info)");
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown argument: {other}");
                eprintln!("try --help");
                std::process::exit(2);
            }
        }
        i += 1;
    }

    Args { port, data_dir, password }
}

#[tokio::main]
async fn main() {
    // Initialize tracing. RUST_LOG overrides; default is info.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with_target(false)
        .init();

    let args = parse_args();

    let engine = Engine::new(std::path::Path::new(&args.data_dir))
        .expect("failed to initialize storage engine");
    let engine = Arc::new(Mutex::new(engine));

    let addr = format!("0.0.0.0:{}", args.port);
    let listener = TcpListener::bind(&addr).await
        .unwrap_or_else(|_| panic!("failed to bind to {addr}"));

    info!(addr = %addr, data_dir = %args.data_dir, auth = %args.password.is_some(), "powdb server listening");

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                info!(peer = %peer, "accepted connection");
                let eng = engine.clone();
                let pw = args.password.clone();
                tokio::spawn(async move {
                    handler::handle_connection(stream, eng, pw).await;
                });
            }
            Err(e) => {
                error!(error = %e, "accept error");
            }
        }
    }
}
