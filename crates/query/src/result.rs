use powdb_storage::types::Value;

/// The result of executing a query.
#[derive(Debug)]
pub enum QueryResult {
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    Scalar(Value),   // count, avg, etc.
    Modified(u64),   // insert/update/delete — number of rows affected
    Created(String), // DDL — type name created
    Executed {
        message: String,
    }, // DDL — alter/drop feedback
}

impl QueryResult {
    pub fn row_count(&self) -> usize {
        match self {
            QueryResult::Rows { rows, .. } => rows.len(),
            QueryResult::Scalar(_) => 1,
            QueryResult::Modified(n) => *n as usize,
            QueryResult::Created(_) => 0,
            QueryResult::Executed { .. } => 0,
        }
    }
}
