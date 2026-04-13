use powdb_query::executor::Engine;
use powdb_server::handler;
use std::sync::{Arc, RwLock};
use tokio::net::TcpListener;
use tokio::sync::{watch, Semaphore};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

/// Maximum number of concurrent connections.
const MAX_CONNECTIONS: usize = 1024;

struct Args {
    port: u16,
    bind: String,
    data_dir: String,
    password: Option<String>,
    idle_timeout_secs: u64,
    query_timeout_secs: u64,
}

fn parse_args() -> Args {
    // Defaults from env vars (preserve old behavior), then overridden by CLI flags.
    let mut port: u16 = std::env::var("POWDB_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5433);
    let mut bind: String = std::env::var("POWDB_BIND").unwrap_or_else(|_| "127.0.0.1".into());
    let mut data_dir: String =
        std::env::var("POWDB_DATA").unwrap_or_else(|_| "./powdb_data".into());
    let mut password: Option<String> = std::env::var("POWDB_PASSWORD")
        .ok()
        .filter(|s| !s.is_empty());
    let mut idle_timeout_secs: u64 = std::env::var("POWDB_IDLE_TIMEOUT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300); // 5 min default
    let mut query_timeout_secs: u64 = std::env::var("POWDB_QUERY_TIMEOUT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30); // 30s default

    let argv: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < argv.len() {
        match argv[i].as_str() {
            "--port" | "-p" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("--port requires a value");
                    std::process::exit(2);
                }
                port = argv[i].parse().unwrap_or_else(|_| {
                    eprintln!("invalid port: {}", argv[i]);
                    std::process::exit(2);
                });
            }
            "--data-dir" | "-d" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("--data-dir requires a value");
                    std::process::exit(2);
                }
                data_dir = argv[i].clone();
            }
            "--bind" | "-b" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("--bind requires a value");
                    std::process::exit(2);
                }
                bind = argv[i].clone();
            }
            "--password" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("--password requires a value");
                    std::process::exit(2);
                }
                password = Some(argv[i].clone());
            }
            "--idle-timeout" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("--idle-timeout requires a value");
                    std::process::exit(2);
                }
                idle_timeout_secs = argv[i].parse().unwrap_or_else(|_| {
                    eprintln!("invalid timeout: {}", argv[i]);
                    std::process::exit(2);
                });
            }
            "--query-timeout" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("--query-timeout requires a value");
                    std::process::exit(2);
                }
                query_timeout_secs = argv[i].parse().unwrap_or_else(|_| {
                    eprintln!("invalid timeout: {}", argv[i]);
                    std::process::exit(2);
                });
            }
            "--help" | "-h" => {
                println!("powdb-server — PowDB wire-protocol server");
                println!();
                println!("USAGE:");
                println!("    powdb-server [OPTIONS]");
                println!();
                println!("OPTIONS:");
                println!("    -p, --port <PORT>          TCP port to listen on (default: 5433)");
                println!("    -b, --bind <ADDR>          Bind address (default: 127.0.0.1)");
                println!("    -d, --data-dir <PATH>      Data directory (default: ./powdb_data)");
                println!("        --password <PW>        Require this password on CONNECT");
                println!("        --idle-timeout <SECS>  Idle connection timeout (default: 300)");
                println!(
                    "        --query-timeout <SECS> Per-query execution timeout (default: 30)"
                );
                println!("    -h, --help                 Print this message");
                println!();
                println!("ENVIRONMENT:");
                println!("    POWDB_PORT, POWDB_BIND, POWDB_DATA, POWDB_PASSWORD");
                println!("    POWDB_IDLE_TIMEOUT, POWDB_QUERY_TIMEOUT");
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

    Args {
        port,
        bind,
        data_dir,
        password,
        idle_timeout_secs,
        query_timeout_secs,
    }
}

#[tokio::main]
async fn main() {
    // Initialize tracing. RUST_LOG overrides; default is info.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let args = parse_args();

    let engine = match Engine::new(std::path::Path::new(&args.data_dir)) {
        Ok(e) => e,
        Err(e) => {
            error!(data_dir = %args.data_dir, error = %e, "failed to initialize storage engine");
            std::process::exit(1);
        }
    };
    let engine = Arc::new(RwLock::new(engine));

    let addr = format!("{}:{}", args.bind, args.port);
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            error!(addr = %addr, error = %e, "failed to bind");
            std::process::exit(1);
        }
    };

    info!(
        addr = %addr, data_dir = %args.data_dir, auth = %args.password.is_some(),
        idle_timeout = args.idle_timeout_secs, query_timeout = args.query_timeout_secs,
        "powdb server listening"
    );

    let semaphore = Arc::new(Semaphore::new(MAX_CONNECTIONS));

    // Shutdown broadcast: `false` initially, flipped to `true` on SIGINT/SIGTERM.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let idle_timeout = std::time::Duration::from_secs(args.idle_timeout_secs);
    let query_timeout = std::time::Duration::from_secs(args.query_timeout_secs);

    loop {
        tokio::select! {
            // Accept new connections.
            result = listener.accept() => {
                match result {
                    Ok((stream, peer)) => {
                        let permit = match semaphore.clone().acquire_owned().await {
                            Ok(p) => p,
                            Err(_) => break,
                        };
                        info!(peer = %peer, "accepted connection");
                        let eng = engine.clone();
                        let pw = args.password.clone();
                        let mut rx = shutdown_rx.clone();
                        let idle = idle_timeout;
                        let qtimeout = query_timeout;
                        tokio::spawn(async move {
                            handler::handle_connection(stream, eng, pw, &mut rx, idle, qtimeout).await;
                            drop(permit);
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "accept error");
                    }
                }
            }

            // Graceful shutdown on SIGINT (Ctrl-C).
            _ = tokio::signal::ctrl_c() => {
                warn!("received shutdown signal, draining connections...");
                let _ = shutdown_tx.send(true);
                break;
            }
        }
    }

    // Wait for all in-flight connections to finish. The semaphore starts
    // at MAX_CONNECTIONS; each active connection holds one permit. When
    // all connections have closed, we can acquire all permits back.
    info!(
        "waiting for {} active connection(s) to drain",
        MAX_CONNECTIONS - semaphore.available_permits()
    );
    let _ = semaphore.acquire_many(MAX_CONNECTIONS as u32).await;
    info!("all connections drained, shutting down");

    // Engine `Drop` calls `catalog.checkpoint()` which flushes heap pages
    // and truncates the WAL.
    drop(engine);
    info!("clean shutdown complete");
}
