use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::io::{self, BufRead, BufReader, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use turso_core::{Connection, StepResult, Value as DbValue};

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

pub struct TursoMcpServer {
    conn: Arc<Connection>,
    interrupt_count: Arc<AtomicUsize>,
}

impl TursoMcpServer {
    pub fn new(conn: Arc<Connection>, interrupt_count: Arc<AtomicUsize>) -> Self {
        Self {
            conn,
            interrupt_count,
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
                    let response_json = serde_json::to_string(&response)?;
                    writeln!(stdout_lock, "{response_json}")?;
                    stdout_lock.flush()?;
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

    fn list_tables(&self) -> String {
        let query = "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY 1";

        match self.conn.query(query) {
            Ok(Some(mut rows)) => {
                let mut tables = Vec::new();

                loop {
                    match rows.step() {
                        Ok(StepResult::Row) => {
                            let row = rows.row().unwrap();
                            if let Ok(DbValue::Text(table)) = row.get::<&DbValue>(0) {
                                tables.push(table.to_string());
                            }
                        }
                        Ok(StepResult::IO) => {
                            if rows.step().is_err() {
                                break;
                            }
                        }
                        Ok(StepResult::Done) => break,
                        Ok(StepResult::Interrupt) => break,
                        Ok(StepResult::Busy) => {
                            return "Database is busy".to_string();
                        }
                        Err(e) => {
                            return format!("Error listing tables: {e}");
                        }
                    }
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

        let query = format!("PRAGMA table_info({table_name})");

        match self.conn.query(&query) {
            Ok(Some(mut rows)) => {
                let mut columns = Vec::new();

                loop {
                    match rows.step() {
                        Ok(StepResult::Row) => {
                            let row = rows.row().unwrap();
                            if let (
                                Ok(col_name),
                                Ok(col_type),
                                Ok(not_null),
                                Ok(default_value),
                                Ok(pk),
                            ) = (
                                row.get::<&DbValue>(1),
                                row.get::<&DbValue>(2),
                                row.get::<&DbValue>(3),
                                row.get::<&DbValue>(4),
                                row.get::<&DbValue>(5),
                            ) {
                                let default_str = if matches!(default_value, DbValue::Null) {
                                    "".to_string()
                                } else {
                                    format!("DEFAULT {default_value}")
                                };

                                columns.push(
                                    format!(
                                        "{} {} {} {} {}",
                                        col_name,
                                        col_type,
                                        if matches!(not_null, DbValue::Integer(1)) {
                                            "NOT NULL"
                                        } else {
                                            "NULL"
                                        },
                                        default_str,
                                        if matches!(pk, DbValue::Integer(1)) {
                                            "PRIMARY KEY"
                                        } else {
                                            ""
                                        }
                                    )
                                    .trim()
                                    .to_string(),
                                );
                            }
                        }
                        Ok(StepResult::IO) => {
                            if rows.step().is_err() {
                                break;
                            }
                        }
                        Ok(StepResult::Done) => break,
                        Ok(StepResult::Interrupt) => break,
                        Ok(StepResult::Busy) => {
                            return "Database is busy".to_string();
                        }
                        Err(e) => {
                            return format!("Error describing table: {e}");
                        }
                    }
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
        let query = match arguments {
            Some(args) => match args.get("query") {
                Some(Value::String(q)) => q,
                _ => return "Missing or invalid query parameter".to_string(),
            },
            None => return "Missing query parameter".to_string(),
        };

        // Basic validation to ensure it's a read-only query
        let trimmed_query = query.trim().to_lowercase();
        if !trimmed_query.starts_with("select") {
            return "Only SELECT queries are allowed".to_string();
        }

        match self.conn.query(query) {
            Ok(Some(mut rows)) => {
                let mut results = Vec::new();

                // Get column names
                let headers: Vec<String> = (0..rows.num_columns())
                    .map(|i| rows.get_column_name(i).to_string())
                    .collect();

                // Get the data
                loop {
                    match rows.step() {
                        Ok(StepResult::Row) => {
                            let row = rows.row().unwrap();
                            let mut row_data = Vec::new();

                            for value in row.get_values() {
                                row_data.push(value.to_string());
                            }

                            results.push(row_data);
                        }
                        Ok(StepResult::IO) => {
                            if rows.step().is_err() {
                                break;
                            }
                        }
                        Ok(StepResult::Done) => break,
                        Ok(StepResult::Interrupt) => break,
                        Ok(StepResult::Busy) => {
                            return "Database is busy".to_string();
                        }
                        Err(e) => {
                            return format!("Error executing query: {e}");
                        }
                    }
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
        let query = match arguments {
            Some(args) => match args.get("query") {
                Some(Value::String(q)) => q,
                _ => return "Missing or invalid query parameter".to_string(),
            },
            None => return "Missing query parameter".to_string(),
        };

        // Basic validation to ensure it's an INSERT query
        let trimmed_query = query.trim().to_lowercase();
        if !trimmed_query.starts_with("insert") {
            return "Only INSERT statements are allowed".to_string();
        }

        match self.conn.execute(query) {
            Ok(()) => "INSERT successful.".to_string(),
            Err(e) => format!("Error executing INSERT: {e}"),
        }
    }

    fn update_data(&self, arguments: &Option<Value>) -> String {
        let query = match arguments {
            Some(args) => match args.get("query") {
                Some(Value::String(q)) => q,
                _ => return "Missing or invalid query parameter".to_string(),
            },
            None => return "Missing query parameter".to_string(),
        };

        // Basic validation to ensure it's an UPDATE query
        let trimmed_query = query.trim().to_lowercase();
        if !trimmed_query.starts_with("update") {
            return "Only UPDATE statements are allowed".to_string();
        }

        match self.conn.execute(query) {
            Ok(()) => "UPDATE successful.".to_string(),
            Err(e) => format!("Error executing UPDATE: {e}"),
        }
    }

    fn delete_data(&self, arguments: &Option<Value>) -> String {
        let query = match arguments {
            Some(args) => match args.get("query") {
                Some(Value::String(q)) => q,
                _ => return "Missing or invalid query parameter".to_string(),
            },
            None => return "Missing query parameter".to_string(),
        };

        // Basic validation to ensure it's a DELETE query
        let trimmed_query = query.trim().to_lowercase();
        if !trimmed_query.starts_with("delete") {
            return "Only DELETE statements are allowed".to_string();
        }

        match self.conn.execute(query) {
            Ok(()) => "DELETE successful.".to_string(),
            Err(e) => format!("Error executing DELETE: {e}"),
        }
    }

    fn schema_change(&self, arguments: &Option<Value>) -> String {
        let query = match arguments {
            Some(args) => match args.get("query") {
                Some(Value::String(q)) => q,
                _ => return "Missing or invalid query parameter".to_string(),
            },
            None => return "Missing query parameter".to_string(),
        };

        // Basic validation to ensure it's a schema modification query
        let trimmed_query = query.trim().to_lowercase();
        if !trimmed_query.starts_with("create")
            && !trimmed_query.starts_with("alter")
            && !trimmed_query.starts_with("drop")
        {
            return "Only CREATE, ALTER, and DROP statements are allowed".to_string();
        }

        match self.conn.execute(query) {
            Ok(()) => "Schema change successful.".to_string(),
            Err(e) => format!("Error executing schema change: {e}"),
        }
    }
}
