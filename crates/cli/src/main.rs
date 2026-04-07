use batadb_query::executor::Engine;
use batadb_query::result::QueryResult;
use batadb_server::protocol::Message;
use batadb_storage::types::Value;
use rustyline::DefaultEditor;
use std::path::Path;
use tokio::io::{BufReader, BufWriter};
use tokio::net::TcpStream;
use tracing_subscriber::EnvFilter;

struct CliArgs {
    data_dir: String,
    remote: Option<String>,
    db: String,
    password: Option<String>,
}

fn parse_args() -> CliArgs {
    let mut data_dir = "./batadb_data".to_string();
    let mut remote: Option<String> = None;
    let mut db: String = "main".to_string();
    let mut password: Option<String> = std::env::var("BATADB_PASSWORD").ok().filter(|s| !s.is_empty());

    let argv: Vec<String> = std::env::args().collect();
    let mut i = 1;
    let mut saw_positional = false;
    while i < argv.len() {
        match argv[i].as_str() {
            "--remote" | "-r" => {
                i += 1;
                if i >= argv.len() { eprintln!("--remote requires host:port"); std::process::exit(2); }
                remote = Some(argv[i].clone());
            }
            "--db" => {
                i += 1;
                if i >= argv.len() { eprintln!("--db requires a name"); std::process::exit(2); }
                db = argv[i].clone();
            }
            "--password" => {
                i += 1;
                if i >= argv.len() { eprintln!("--password requires a value"); std::process::exit(2); }
                password = Some(argv[i].clone());
            }
            "--data-dir" | "-d" => {
                i += 1;
                if i >= argv.len() { eprintln!("--data-dir requires a path"); std::process::exit(2); }
                data_dir = argv[i].clone();
            }
            "--help" | "-h" => {
                println!("batadb-cli — BataQL interactive shell");
                println!();
                println!("USAGE:");
                println!("    batadb-cli [OPTIONS] [DATA_DIR]");
                println!();
                println!("OPTIONS:");
                println!("    -r, --remote <HOST:PORT>   Connect to a remote server over TCP");
                println!("        --db <NAME>            Database name (default: main)");
                println!("        --password <PW>        Password for remote auth");
                println!("    -d, --data-dir <PATH>      Embedded data dir (default: ./batadb_data)");
                println!("    -h, --help                 Print this message");
                println!();
                println!("MODES:");
                println!("    Embedded (default):  batadb-cli ./mydata");
                println!("    Remote:              batadb-cli --remote 127.0.0.1:5433 --password secret");
                std::process::exit(0);
            }
            other if !other.starts_with('-') && !saw_positional => {
                data_dir = other.to_string();
                saw_positional = true;
            }
            other => {
                eprintln!("unknown argument: {other}");
                eprintln!("try --help");
                std::process::exit(2);
            }
        }
        i += 1;
    }

    CliArgs { data_dir, remote, db, password }
}

fn main() {
    // Tracing for the CLI (mostly off by default; users can set RUST_LOG=debug).
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")))
        .with_target(false)
        .init();

    let args = parse_args();

    if let Some(remote_addr) = &args.remote {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build tokio runtime");
        rt.block_on(run_remote(remote_addr.clone(), args.db.clone(), args.password.clone()));
    } else {
        run_embedded(&args.data_dir);
    }
}

// ─── Embedded mode ──────────────────────────────────────────────────────────

fn run_embedded(data_dir: &str) {
    eprintln!("BataDB v0.1.0 — embedded mode");
    eprintln!("Data directory: {data_dir}");
    eprintln!("Type BataQL queries. Use Ctrl-D to exit.\n");

    let mut engine = Engine::new(Path::new(data_dir))
        .expect("failed to initialize engine");

    let mut rl = DefaultEditor::new().expect("failed to init readline");

    loop {
        let line = match rl.readline("bataql> ") {
            Ok(line) => line,
            Err(rustyline::error::ReadlineError::Eof) => break,
            Err(rustyline::error::ReadlineError::Interrupted) => continue,
            Err(e) => {
                eprintln!("Error: {e}");
                break;
            }
        };

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        rl.add_history_entry(trimmed).ok();

        match engine.execute_bataql(trimmed) {
            Ok(result) => print_local_result(&result),
            Err(e) => eprintln!("Error: {e}"),
        }
    }

    eprintln!("\nBye!");
}

// ─── Remote (wire protocol) mode ────────────────────────────────────────────

async fn run_remote(addr: String, db: String, password: Option<String>) {
    eprintln!("BataDB v0.1.0 — remote mode");
    eprintln!("Connecting to {addr} ...");

    let stream = match TcpStream::connect(&addr).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("connection failed: {e}");
            std::process::exit(1);
        }
    };

    let (reader, writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut writer = BufWriter::new(writer);

    // Send CONNECT
    let connect = Message::Connect { db_name: db.clone(), password };
    if let Err(e) = connect.write_to(&mut writer).await {
        eprintln!("failed to send CONNECT: {e}");
        std::process::exit(1);
    }
    if let Err(e) = tokio::io::AsyncWriteExt::flush(&mut writer).await {
        eprintln!("flush error: {e}");
        std::process::exit(1);
    }

    // Read CONNECT_OK or ERROR
    match Message::read_from(&mut reader).await {
        Ok(Some(Message::ConnectOk { version })) => {
            eprintln!("Connected to db `{db}` (server v{version})");
            eprintln!("Type BataQL queries. Use Ctrl-D to exit.\n");
        }
        Ok(Some(Message::Error { message })) => {
            eprintln!("server rejected connection: {message}");
            std::process::exit(1);
        }
        Ok(Some(other)) => {
            eprintln!("unexpected handshake reply: {other:?}");
            std::process::exit(1);
        }
        Ok(None) => {
            eprintln!("server closed connection during handshake");
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("handshake read error: {e}");
            std::process::exit(1);
        }
    }

    let mut rl = DefaultEditor::new().expect("failed to init readline");

    loop {
        let line = match rl.readline("bataql> ") {
            Ok(line) => line,
            Err(rustyline::error::ReadlineError::Eof) => break,
            Err(rustyline::error::ReadlineError::Interrupted) => continue,
            Err(e) => {
                eprintln!("Error: {e}");
                break;
            }
        };

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        rl.add_history_entry(trimmed).ok();

        let q = Message::Query { query: trimmed.to_string() };
        if q.write_to(&mut writer).await.is_err() {
            eprintln!("write error — disconnected");
            break;
        }
        if tokio::io::AsyncWriteExt::flush(&mut writer).await.is_err() {
            eprintln!("flush error — disconnected");
            break;
        }

        match Message::read_from(&mut reader).await {
            Ok(Some(msg)) => print_remote_result(&msg),
            Ok(None) => {
                eprintln!("server closed connection");
                break;
            }
            Err(e) => {
                eprintln!("read error: {e}");
                break;
            }
        }
    }

    // Best-effort goodbye
    let _ = Message::Disconnect.write_to(&mut writer).await;
    let _ = tokio::io::AsyncWriteExt::flush(&mut writer).await;

    eprintln!("\nBye!");
}

// ─── Output formatting ──────────────────────────────────────────────────────

fn print_local_result(result: &QueryResult) {
    match result {
        QueryResult::Rows { columns, rows } => {
            if rows.is_empty() {
                println!("(empty set)");
                return;
            }
            let str_rows: Vec<Vec<String>> = rows.iter()
                .map(|row| row.iter().map(format_value).collect())
                .collect();
            print_table(columns, &str_rows);
        }
        QueryResult::Scalar(val) => {
            println!("{}", format_value(val));
        }
        QueryResult::Modified(n) => {
            println!("{n} row{} affected", if *n == 1 { "" } else { "s" });
        }
        QueryResult::Created(name) => {
            println!("type {name} created");
        }
    }
}

fn print_remote_result(msg: &Message) {
    match msg {
        Message::ResultRows { columns, rows } => {
            if rows.is_empty() {
                println!("(empty set)");
                return;
            }
            print_table(columns, rows);
        }
        Message::ResultScalar { value } => {
            println!("{value}");
        }
        Message::ResultOk { affected } => {
            println!("{affected} row{} affected", if *affected == 1 { "" } else { "s" });
        }
        Message::Error { message } => {
            eprintln!("Error: {message}");
        }
        other => {
            eprintln!("unexpected response: {other:?}");
        }
    }
}

fn print_table(columns: &[String], rows: &[Vec<String>]) {
    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
    for row in rows {
        for (i, val) in row.iter().enumerate() {
            if i < widths.len() && val.len() > widths[i] {
                widths[i] = val.len();
            }
        }
    }

    let header: Vec<String> = columns.iter().enumerate()
        .map(|(i, c)| format!("{:width$}", c, width = widths[i]))
        .collect();
    println!(" {} ", header.join(" | "));
    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    println!("-{}-", sep.join("-+-"));

    for row in rows {
        let cells: Vec<String> = row.iter().enumerate()
            .map(|(i, v)| format!("{:width$}", v, width = widths[i]))
            .collect();
        println!(" {} ", cells.join(" | "));
    }

    println!("({} row{})", rows.len(), if rows.len() == 1 { "" } else { "s" });
}

fn format_value(v: &Value) -> String {
    match v {
        Value::Int(n)      => n.to_string(),
        Value::Float(n)    => format!("{n}"),
        Value::Bool(b)     => b.to_string(),
        Value::Str(s)      => s.clone(),
        Value::DateTime(t) => format!("{t}"),
        Value::Uuid(u)     => format!("{:02x}{:02x}{:02x}{:02x}-...", u[0], u[1], u[2], u[3]),
        Value::Bytes(b)    => format!("<{} bytes>", b.len()),
        Value::Empty       => "{}".into(),
    }
}
