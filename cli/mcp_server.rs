use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::io::{self, BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use turso_core::{
    Connection, Database, DatabaseOpts, Numeric, OpenFlags, SqliteDialect, Value as DbValue,
};
use turso_parser::ast::{Cmd, Stmt};
use turso_parser::parser::Parser;

#[derive(Debug, Serialize, Deserialize)]
struct JsonRpcRequest {
    jsonrpc: String,
    id: Option<Value>,
    method: String,
    params: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
struct JsonRpcResponse {
    jsonrpc: String,
    id: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

#[derive(Debug, Serialize, Deserialize)]
struct JsonRpcError {
    code: i32,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
struct InitializeRequest {
    #[serde(rename = "protocolVersion")]
    protocol_version: String,
    capabilities: Value,
    #[serde(rename = "clientInfo")]
    client_info: Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct CallToolRequest {
    name: String,
    arguments: Option<Value>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StmtClass {
    Select,
    Insert,
    Update,
    Delete,
    Schema,
}

impl StmtClass {
    fn of(stmt: &Stmt) -> Option<Self> {
        match stmt {
            Stmt::Select(_) => Some(Self::Select),
            Stmt::Insert { .. } => Some(Self::Insert),
            Stmt::Update(_) => Some(Self::Update),
            Stmt::Delete { .. } => Some(Self::Delete),
            Stmt::AlterTable(_)
            | Stmt::CreateIndex { .. }
            | Stmt::CreateTable { .. }
            | Stmt::CreateTrigger { .. }
            | Stmt::CreateView { .. }
            | Stmt::CreateMaterializedView { .. }
            | Stmt::CreateVirtualTable(_)
            | Stmt::CreateType { .. }
            | Stmt::CreateDomain { .. }
            | Stmt::CreateSequence { .. }
            | Stmt::CreateFunction { .. }
            | Stmt::DropFunction { .. }
            | Stmt::DropIndex { .. }
            | Stmt::DropTable { .. }
            | Stmt::DropTrigger { .. }
            | Stmt::DropView { .. }
            | Stmt::DropType { .. }
            | Stmt::DropDomain { .. }
            | Stmt::DropSequence { .. } => Some(Self::Schema),
            Stmt::Analyze { .. }
            | Stmt::Attach { .. }
            | Stmt::Begin { .. }
            | Stmt::Commit { .. }
            | Stmt::Detach { .. }
            | Stmt::Pragma { .. }
            | Stmt::Reindex { .. }
            | Stmt::Release { .. }
            | Stmt::Rollback { .. }
            | Stmt::Savepoint { .. }
            | Stmt::Vacuum { .. }
            | Stmt::Optimize { .. } => None,
        }
    }

    fn single_statement_error(self) -> &'static str {
        match self {
            Self::Select => "Only a single SELECT query is allowed",
            Self::Insert => "Only a single INSERT statement is allowed",
            Self::Update => "Only a single UPDATE statement is allowed",
            Self::Delete => "Only a single DELETE statement is allowed",
            Self::Schema => "Only a single schema modification statement is allowed",
        }
    }

    fn wrong_class_error(self) -> &'static str {
        match self {
            Self::Select => "Only SELECT queries are allowed",
            Self::Insert => "Only INSERT statements are allowed",
            Self::Update => "Only UPDATE statements are allowed",
            Self::Delete => "Only DELETE statements are allowed",
            Self::Schema => "Only CREATE, ALTER, and DROP statements are allowed",
        }
    }
}

fn query_arg(arguments: &Option<Value>) -> Result<&str, String> {
    match arguments {
        Some(args) => match args.get("query") {
            Some(Value::String(q)) => Ok(q),
            _ => Err("Missing or invalid query parameter".to_string()),
        },
        None => Err("Missing query parameter".to_string()),
    }
}

fn require_single_stmt(sql: &str, class: StmtClass) -> Result<(), String> {
    let mut parser = Parser::new(sql.as_bytes());
    let cmd = match parser.next_cmd() {
        Ok(Some(cmd)) => cmd,
        Ok(None) => return Err("No SQL statement provided".to_string()),
        Err(e) => return Err(format!("Failed to parse SQL: {e}")),
    };
    match parser.next_cmd() {
        Ok(None) => {}
        Ok(Some(_)) => return Err(class.single_statement_error().to_string()),
        Err(e) => return Err(format!("Failed to parse SQL: {e}")),
    }
    match cmd {
        Cmd::Stmt(stmt) if StmtClass::of(&stmt) == Some(class) => Ok(()),
        Cmd::Stmt(_) | Cmd::Explain(_) | Cmd::ExplainQueryPlan { .. } => {
            Err(class.wrong_class_error().to_string())
        }
    }
}

fn validated_query(arguments: &Option<Value>, class: StmtClass) -> Result<&str, String> {
    let sql = query_arg(arguments)?;
    require_single_stmt(sql, class)?;
    Ok(sql)
}

pub struct TursoMcpServer {
    conn: Arc<Mutex<Arc<Connection>>>,
    interrupt_count: Arc<AtomicUsize>,
    current_db_path: Arc<Mutex<Option<String>>>,
}

impl TursoMcpServer {
    pub fn new(conn: Arc<Connection>, interrupt_count: Arc<AtomicUsize>) -> Self {
        Self {
            conn: Arc::new(Mutex::new(conn)),
            interrupt_count,
            current_db_path: Arc::new(Mutex::new(None)),
        }
    }

    pub fn run(&self) -> Result<()> {
        let stdout = io::stdout();
        let mut stdout_lock = stdout.lock();

        // Create a channel to receive lines from stdin
        let (tx, rx) = mpsc::channel();

        // Spawn a thread to read from stdin
        thread::spawn(move || {
            let stdin = io::stdin();
            let reader = BufReader::new(stdin);

            for line in reader.lines() {
                match line {
                    Ok(line) => {
                        if tx.send(Ok(line)).is_err() {
                            break; // Main thread has dropped the receiver
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e));
                        break;
                    }
                }
            }
        });

        loop {
            // Check if we've been interrupted
            if self.interrupt_count.load(Ordering::SeqCst) > 0 {
                eprintln!("MCP server interrupted, shutting down...");
                break;
            }

            // Try to receive a line with a timeout so we can check for interruption
            match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(Ok(line)) => {
                    if line.trim().is_empty() {
                        continue;
                    }

                    let request: JsonRpcRequest = match serde_json::from_str(&line) {
                        Ok(req) => req,
                        Err(e) => {
                            eprintln!("Failed to parse JSON-RPC request: {e}");
                            continue;
                        }
                    };

                    let response = self.handle_request(request);
                    // Don't send a response for notifications (when id is None)
                    if response.id.is_some() || response.error.is_some() {
                        let response_json = serde_json::to_string(&response)?;
                        writeln!(stdout_lock, "{response_json}")?;
                        stdout_lock.flush()?;
                    }
                }
                Ok(Err(_)) => {
                    // Error reading from stdin
                    break;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    // Timeout - continue loop to check for interruption
                    continue;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    // Stdin thread has finished (EOF)
                    break;
                }
            }
        }

        Ok(())
    }

    fn handle_request(&self, request: JsonRpcRequest) -> JsonRpcResponse {
        // Check if this is a notification (no id field means it's a notification)
        // Notifications should not receive a response according to JSON-RPC spec
        if request.id.is_none() {
            // For notifications, we return a special response that the caller should ignore
            return JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id: None,
                result: None,
                error: None,
            };
        }

        match request.method.as_str() {
            "initialize" => self.handle_initialize(request),
            "tools/list" => self.handle_list_tools(request),
            "tools/call" => self.handle_call_tool(request),
            _ => JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id: request.id,
                result: None,
                error: Some(JsonRpcError {
                    code: -32601,
                    message: "Method not found".to_string(),
                    data: None,
                }),
            },
        }
    }

    fn handle_initialize(&self, request: JsonRpcRequest) -> JsonRpcResponse {
        JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            id: request.id,
            result: Some(json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "tools": {}
                },
                "serverInfo": {
                    "name": "turso-mcp",
                    "version": "1.0.0"
                }
            })),
            error: None,
        }
    }

    fn handle_list_tools(&self, request: JsonRpcRequest) -> JsonRpcResponse {
        JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            id: request.id,
            result: Some(json!({
                "tools": [
                    {
                        "name": "open_database",
                        "description": "Open or create a database file. Creates parent directories if needed.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "path": {
                                    "type": "string",
                                    "description": "Path to the database file (absolute or relative). Use ':memory:' for in-memory database."
                                }
                            },
                            "required": ["path"]
                        }
                    },
                    {
                        "name": "current_database",
                        "description": "Get the path of the currently open database",
                        "inputSchema": {
                            "type": "object",
                            "properties": {},
                            "required": []
                        }
                    },
                    {
                        "name": "list_tables",
                        "description": "List all tables in the database",
                        "inputSchema": {
                            "type": "object",
                            "properties": {},
                            "required": []
                        }
                    },
                    {
                        "name": "describe_table",
                        "description": "Describe the structure of a specific table",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "table_name": {
                                    "type": "string",
                                    "description": "Name of the table to describe"
                                }
                            },
                            "required": ["table_name"]
                        }
                    },
                    {
                        "name": "execute_query",
                        "description": "Execute a read-only SELECT query",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "query": {
                                    "type": "string",
                                    "description": "The SELECT query to execute"
                                }
                            },
                            "required": ["query"]
                        }
                    },
                    {
                        "name": "insert_data",
                        "description": "Insert new data into a table",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "query": {
                                    "type": "string",
                                    "description": "The INSERT statement to execute"
                                }
                            },
                            "required": ["query"]
                        }
                    },
                    {
                        "name": "update_data",
                        "description": "Update existing data in a table",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "query": {
                                    "type": "string",
                                    "description": "The UPDATE statement to execute"
                                }
                            },
                            "required": ["query"]
                        }
                    },
                    {
                        "name": "delete_data",
                        "description": "Delete data from a table",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "query": {
                                    "type": "string",
                                    "description": "The DELETE statement to execute"
                                }
                            },
                            "required": ["query"]
                        }
                    },
                    {
                        "name": "schema_change",
                        "description": "Execute schema modification statements (CREATE TABLE, ALTER TABLE, DROP TABLE)",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "query": {
                                    "type": "string",
                                    "description": "The schema modification statement to execute"
                                }
                            },
                            "required": ["query"]
                        }
                    }
                ]
            })),
            error: None,
        }
    }

    fn handle_call_tool(&self, request: JsonRpcRequest) -> JsonRpcResponse {
        let tool_request: CallToolRequest = match request.params.as_ref() {
            Some(params) => match serde_json::from_value(params.clone()) {
                Ok(req) => req,
                Err(e) => {
                    return JsonRpcResponse {
                        jsonrpc: "2.0".to_string(),
                        id: request.id,
                        result: None,
                        error: Some(JsonRpcError {
                            code: -32602,
                            message: format!("Invalid params: {e}"),
                            data: None,
                        }),
                    };
                }
            },
            None => {
                return JsonRpcResponse {
                    jsonrpc: "2.0".to_string(),
                    id: request.id,
                    result: None,
                    error: Some(JsonRpcError {
                        code: -32602,
                        message: "Missing params".to_string(),
                        data: None,
                    }),
                };
            }
        };

        let result = match tool_request.name.as_str() {
            "open_database" => self.open_database(&tool_request.arguments),
            "current_database" => self.current_database(),
            "list_tables" => self.list_tables(),
            "describe_table" => self.describe_table(&tool_request.arguments),
            "execute_query" => self.execute_query(&tool_request.arguments),
            "insert_data" => self.insert_data(&tool_request.arguments),
            "update_data" => self.update_data(&tool_request.arguments),
            "delete_data" => self.delete_data(&tool_request.arguments),
            "schema_change" => self.schema_change(&tool_request.arguments),
            _ => {
                return JsonRpcResponse {
                    jsonrpc: "2.0".to_string(),
                    id: request.id,
                    result: None,
                    error: Some(JsonRpcError {
                        code: -32601,
                        message: format!("Unknown tool: {}", tool_request.name),
                        data: None,
                    }),
                };
            }
        };

        JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            id: request.id,
            result: Some(json!({
                "content": [{
                    "type": "text",
                    "text": result
                }]
            })),
            error: None,
        }
    }

    fn open_database(&self, arguments: &Option<Value>) -> String {
        let path = match arguments {
            Some(args) => match args.get("path") {
                Some(Value::String(p)) => p.clone(),
                _ => return "Missing or invalid path parameter".to_string(),
            },
            None => return "Missing path parameter".to_string(),
        };

        // Create parent directories if needed
        if path != ":memory:" {
            let db_path = PathBuf::from(&path);
            if let Some(parent) = db_path.parent() {
                if !parent.exists() {
                    if let Err(e) = std::fs::create_dir_all(parent) {
                        return format!("Failed to create parent directories: {e}");
                    }
                }
            }
        }

        // Open the new database connection
        let conn = if path == ":memory:" || path.contains([':', '?', '&', '#']) {
            match Connection::from_uri(&path, DatabaseOpts::default(), Arc::new(SqliteDialect)) {
                Ok((_io, c)) => c,
                Err(e) => return format!("Failed to open database '{path}': {e}"),
            }
        } else {
            match Database::open_new(
                &path,
                None::<&str>,
                OpenFlags::default(),
                DatabaseOpts::new().with_autovacuum(false),
                None,
                Arc::new(SqliteDialect),
            ) {
                Ok((_io, db)) => match db.connect() {
                    Ok(c) => c,
                    Err(e) => return format!("Failed to connect to database '{path}': {e}"),
                },
                Err(e) => return format!("Failed to open database '{path}': {e}"),
            }
        };

        // Update the connection and path
        *self.conn.lock().unwrap() = conn;
        *self.current_db_path.lock().unwrap() = Some(path.clone());

        format!("Successfully opened database: {path}")
    }

    fn current_database(&self) -> String {
        match &*self.current_db_path.lock().unwrap() {
            Some(path) => format!("Current database: {path}"),
            None => "Current database: :memory: (default)".to_string(),
        }
    }

    fn list_tables(&self) -> String {
        let query = "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY 1";

        let conn = self.conn.lock().unwrap().clone();
        match conn.query(query) {
            Ok(Some(mut rows)) => {
                let mut tables = Vec::new();

                let res = rows.run_with_row_callback(|row| {
                    if let Ok(DbValue::Text(table)) = row.get::<&DbValue>(0) {
                        tables.push(table.to_string());
                    }
                    Ok(())
                });
                if let Err(err) = res {
                    return err.to_string();
                }

                if tables.is_empty() {
                    "No tables found in the database".to_string()
                } else {
                    tables.join(", ")
                }
            }
            Ok(None) => "No results returned from the query".to_string(),
            Err(e) => format!("Error querying database: {e}"),
        }
    }

    fn describe_table(&self, arguments: &Option<Value>) -> String {
        let table_name = match arguments {
            Some(args) => match args.get("table_name") {
                Some(Value::String(name)) => name,
                _ => return "Missing or invalid table_name parameter".to_string(),
            },
            None => return "Missing table_name parameter".to_string(),
        };

        // Use table_xinfo to include generated columns (table_info hides them)
        let query = format!("PRAGMA table_xinfo({table_name})");

        let conn = self.conn.lock().unwrap().clone();
        match conn.query(&query) {
            Ok(Some(mut rows)) => {
                let mut columns = Vec::new();
                let res = rows.run_with_row_callback(|row| {
                    if let (
                        Ok(col_name),
                        Ok(col_type),
                        Ok(not_null),
                        Ok(default_value),
                        Ok(pk),
                        Ok(hidden),
                    ) = (
                        row.get::<&DbValue>(1),
                        row.get::<&DbValue>(2),
                        row.get::<&DbValue>(3),
                        row.get::<&DbValue>(4),
                        row.get::<&DbValue>(5),
                        row.get::<&DbValue>(6),
                    ) {
                        let default_str = if matches!(default_value, DbValue::Null) {
                            "".to_string()
                        } else {
                            format!("DEFAULT {default_value}")
                        };

                        let generated_str = match hidden {
                            DbValue::Numeric(Numeric::Integer(2)) => " VIRTUAL GENERATED",
                            _ => "",
                        };

                        columns.push(
                            format!(
                                "{} {} {} {} {}{}",
                                col_name,
                                col_type,
                                if matches!(not_null, DbValue::Numeric(Numeric::Integer(1))) {
                                    "NOT NULL"
                                } else {
                                    "NULL"
                                },
                                default_str,
                                if matches!(pk, DbValue::Numeric(Numeric::Integer(1))) {
                                    "PRIMARY KEY"
                                } else {
                                    ""
                                },
                                generated_str
                            )
                            .trim()
                            .to_string(),
                        );
                    }
                    Ok(())
                });

                if let Err(err) = res {
                    return err.to_string();
                }
                if columns.is_empty() {
                    format!("Table '{table_name}' not found")
                } else {
                    format!("Table '{table_name}' columns:\n{}", columns.join("\n"))
                }
            }
            Ok(None) => format!("Table '{table_name}' not found"),
            Err(e) => format!("Error querying database: {e}"),
        }
    }

    fn execute_query(&self, arguments: &Option<Value>) -> String {
        let query = match validated_query(arguments, StmtClass::Select) {
            Ok(q) => q,
            Err(e) => return e,
        };

        let conn = self.conn.lock().unwrap().clone();
        match conn.query(query) {
            Ok(Some(mut rows)) => {
                let mut results = Vec::new();

                // Get column names
                let headers: Vec<String> = (0..rows.num_columns())
                    .map(|i| rows.get_column_name(i).to_string())
                    .collect();

                // Get the data
                let res = rows.run_with_row_callback(|row| {
                    let mut row_data = Vec::new();

                    for value in row.get_values() {
                        row_data.push(value.to_string());
                    }

                    results.push(row_data);
                    Ok(())
                });

                if let Err(err) = res {
                    return err.to_string();
                }

                // Format results as text table
                let mut output = String::new();
                if !headers.is_empty() {
                    output.push_str(&headers.join(" | "));
                    output.push('\n');
                    output.push_str(&"-".repeat(headers.join(" | ").len()));
                    output.push('\n');
                }

                for row in results {
                    output.push_str(&row.join(" | "));
                    output.push('\n');
                }

                if output.is_empty() {
                    "No results returned from the query".to_string()
                } else {
                    output
                }
            }
            Ok(None) => "No results returned from the query".to_string(),
            Err(e) => format!("Error executing query: {e}"),
        }
    }

    fn insert_data(&self, arguments: &Option<Value>) -> String {
        let query = match validated_query(arguments, StmtClass::Insert) {
            Ok(q) => q,
            Err(e) => return e,
        };

        let conn = self.conn.lock().unwrap().clone();
        match conn.execute(query) {
            Ok(()) => "INSERT successful.".to_string(),
            Err(e) => format!("Error executing INSERT: {e}"),
        }
    }

    fn update_data(&self, arguments: &Option<Value>) -> String {
        let query = match validated_query(arguments, StmtClass::Update) {
            Ok(q) => q,
            Err(e) => return e,
        };

        let conn = self.conn.lock().unwrap().clone();
        match conn.execute(query) {
            Ok(()) => "UPDATE successful.".to_string(),
            Err(e) => format!("Error executing UPDATE: {e}"),
        }
    }

    fn delete_data(&self, arguments: &Option<Value>) -> String {
        let query = match validated_query(arguments, StmtClass::Delete) {
            Ok(q) => q,
            Err(e) => return e,
        };

        let conn = self.conn.lock().unwrap().clone();
        match conn.execute(query) {
            Ok(()) => "DELETE successful.".to_string(),
            Err(e) => format!("Error executing DELETE: {e}"),
        }
    }

    fn schema_change(&self, arguments: &Option<Value>) -> String {
        let query = match validated_query(arguments, StmtClass::Schema) {
            Ok(q) => q,
            Err(e) => return e,
        };

        let conn = self.conn.lock().unwrap().clone();
        match conn.execute(query) {
            Ok(()) => "Schema change successful.".to_string(),
            Err(e) => format!("Error executing schema change: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn memory_server() -> TursoMcpServer {
        let (_io, conn) =
            Connection::from_uri(":memory:", DatabaseOpts::default(), Arc::new(SqliteDialect))
                .expect("open memory database");
        TursoMcpServer::new(conn, Arc::new(AtomicUsize::new(0)))
    }

    fn query_arg(sql: &str) -> Option<Value> {
        Some(json!({ "query": sql }))
    }

    fn seed_bench_orders(server: &TursoMcpServer) {
        let conn = server.conn.lock().unwrap().clone();
        conn.execute(
            "CREATE TABLE bench_orders (
                order_id INTEGER PRIMARY KEY,
                status TEXT NOT NULL,
                priority INTEGER NOT NULL
            )",
        )
        .unwrap();
        conn.execute("INSERT INTO bench_orders VALUES (1, 'READY', 1), (2, 'HOLD', 2)")
            .unwrap();
    }

    fn orders_dump(server: &TursoMcpServer) -> String {
        server.execute_query(&query_arg(
            "SELECT order_id, status, priority FROM bench_orders ORDER BY order_id",
        ))
    }

    #[test]
    fn update_data_rejects_trailing_delete() {
        let server = memory_server();
        seed_bench_orders(&server);

        let result = server.update_data(&query_arg(
            "UPDATE bench_orders SET status='DONE' WHERE order_id=1; DELETE FROM bench_orders WHERE order_id=2",
        ));

        assert!(
            result.contains("Only a single UPDATE statement is allowed"),
            "expected single-statement rejection, got: {result}"
        );

        let dump = orders_dump(&server);
        assert!(
            dump.contains("1 | READY | 1"),
            "UPDATE must not run: {dump}"
        );
        assert!(
            dump.contains("2 | HOLD | 2"),
            "trailing DELETE must not run: {dump}"
        );
    }

    #[test]
    fn update_data_allows_semicolon_inside_string() {
        let server = memory_server();
        seed_bench_orders(&server);

        let result = server.update_data(&query_arg(
            "UPDATE bench_orders SET status='DONE; DELETE' WHERE order_id=1",
        ));
        assert_eq!(result, "UPDATE successful.");

        let dump = orders_dump(&server);
        assert!(dump.contains("1 | DONE; DELETE | 1"), "{dump}");
        assert!(dump.contains("2 | HOLD | 2"), "{dump}");
    }

    #[test]
    fn insert_data_rejects_trailing_delete() {
        let server = memory_server();
        seed_bench_orders(&server);

        let result = server.insert_data(&query_arg(
            "INSERT INTO bench_orders VALUES (3, 'NEW', 3); DELETE FROM bench_orders WHERE order_id=2",
        ));

        assert!(
            result.contains("Only a single INSERT statement is allowed"),
            "expected single-statement rejection, got: {result}"
        );
        assert!(!orders_dump(&server).contains("3 | NEW | 3"));
        assert!(orders_dump(&server).contains("2 | HOLD | 2"));
    }

    #[test]
    fn delete_data_rejects_trailing_drop() {
        let server = memory_server();
        seed_bench_orders(&server);

        let result = server.delete_data(&query_arg(
            "DELETE FROM bench_orders WHERE order_id=1; DROP TABLE bench_orders",
        ));

        assert!(
            result.contains("Only a single DELETE statement is allowed"),
            "expected single-statement rejection, got: {result}"
        );
        let dump = orders_dump(&server);
        assert!(dump.contains("1 | READY | 1"), "{dump}");
        assert!(dump.contains("2 | HOLD | 2"), "{dump}");
    }

    #[test]
    fn schema_change_rejects_trailing_delete() {
        let server = memory_server();
        seed_bench_orders(&server);

        let result = server.schema_change(&query_arg(
            "CREATE TABLE extra (id INTEGER); DELETE FROM bench_orders",
        ));

        assert!(
            result.contains("Only a single schema modification statement is allowed"),
            "expected single-statement rejection, got: {result}"
        );
        assert!(orders_dump(&server).contains("1 | READY | 1"));
        assert!(orders_dump(&server).contains("2 | HOLD | 2"));
    }

    #[test]
    fn execute_query_rejects_trailing_delete() {
        let server = memory_server();
        seed_bench_orders(&server);

        let result = server.execute_query(&query_arg(
            "SELECT order_id FROM bench_orders WHERE order_id=1; DELETE FROM bench_orders WHERE order_id=2",
        ));

        assert!(
            result.contains("Only a single SELECT query is allowed"),
            "expected single-statement rejection, got: {result}"
        );
        assert!(orders_dump(&server).contains("2 | HOLD | 2"));
    }

    #[test]
    fn update_data_accepts_single_update() {
        let server = memory_server();
        seed_bench_orders(&server);

        let result = server.update_data(&query_arg(
            "UPDATE bench_orders SET status='DONE' WHERE order_id=1",
        ));
        assert_eq!(result, "UPDATE successful.");
        assert!(orders_dump(&server).contains("1 | DONE | 1"));
        assert!(orders_dump(&server).contains("2 | HOLD | 2"));
    }
}
