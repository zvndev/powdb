use batadb_query::executor::Engine;
use batadb_query::result::QueryResult;
use batadb_storage::types::Value;
use rustyline::DefaultEditor;
use std::path::Path;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let data_dir = args.get(1).map(|s| s.as_str()).unwrap_or("./batadb_data");

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
            Ok(result) => print_result(&result),
            Err(e) => eprintln!("Error: {e}"),
        }
    }

    eprintln!("\nBye!");
}

fn print_result(result: &QueryResult) {
    match result {
        QueryResult::Rows { columns, rows } => {
            if rows.is_empty() {
                println!("(empty set)");
                return;
            }

            // Calculate column widths
            let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
            for row in rows {
                for (i, val) in row.iter().enumerate() {
                    let s = format_value(val);
                    if s.len() > widths[i] {
                        widths[i] = s.len();
                    }
                }
            }

            // Print header
            let header: Vec<String> = columns.iter().enumerate()
                .map(|(i, c)| format!("{:width$}", c, width = widths[i]))
                .collect();
            println!(" {} ", header.join(" | "));
            let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
            println!("-{}-", sep.join("-+-"));

            // Print rows
            for row in rows {
                let cells: Vec<String> = row.iter().enumerate()
                    .map(|(i, v)| format!("{:width$}", format_value(v), width = widths[i]))
                    .collect();
                println!(" {} ", cells.join(" | "));
            }

            println!("({} row{})", rows.len(), if rows.len() == 1 { "" } else { "s" });
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
