// Copyright 2023-2025 the Limbo authors. All rights reserved. MIT license.

use std::collections::HashMap;
use std::sync::Arc;
use turso_ext::*;

// ---------------------------------------------------------------------------
// Configuration types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
enum ResponseFormat {
    Json,
    Csv,
    Mcp,
}

#[derive(Clone)]
struct ColumnDef {
    name: String,
    ty: String,
    hidden: bool,
}

#[derive(Clone)]
struct HttpConfig {
    url: String,
    method: String,
    format: ResponseFormat,
    json_path: String,
    headers: Vec<(String, String)>,
    body: Option<String>,
    content_type: String,
    timeout_secs: u64,
    #[allow(dead_code)]
    cache_ttl_secs: u64,
    mcp_tool: Option<String>,
    mcp_resource: Option<String>,
    column_defs: Vec<ColumnDef>,
}

// ---------------------------------------------------------------------------
// Column definition parsing
// ---------------------------------------------------------------------------

/// Parse "id INTEGER, name TEXT, city TEXT HIDDEN" into Vec<ColumnDef>
fn parse_column_defs(s: &str) -> Result<Vec<ColumnDef>, ResultCode> {
    if s.trim().is_empty() {
        return Ok(Vec::new());
    }
    let mut defs = Vec::new();
    for part in s.split(',') {
        let tokens: Vec<&str> = part.split_whitespace().collect();
        if tokens.len() < 2 {
            return Err(ResultCode::InvalidArgs);
        }
        let name = tokens[0].to_string();
        let ty = tokens[1].to_uppercase();
        let hidden = tokens.len() > 2 && tokens[2].eq_ignore_ascii_case("HIDDEN");
        defs.push(ColumnDef { name, ty, hidden });
    }
    Ok(defs)
}

/// Generate CREATE TABLE x (...) schema string with HIDDEN markers in the type.
/// Core detects HIDDEN via ty_str.contains("HIDDEN") at core/schema.rs:2786.
fn column_defs_to_create_table_sql(column_defs: &[ColumnDef]) -> String {
    let cols: Vec<String> = column_defs
        .iter()
        .map(|c| {
            if c.hidden {
                format!("{} {} HIDDEN", c.name, c.ty)
            } else {
                format!("{} {}", c.name, c.ty)
            }
        })
        .collect();
    format!("CREATE TABLE x ({})", cols.join(", "))
}

// ---------------------------------------------------------------------------
// HttpConfig parsing
// ---------------------------------------------------------------------------

impl HttpConfig {
    fn parse(args: &[Value]) -> Result<Self, ResultCode> {
        let mut url = None;
        let mut method = "GET".to_string();
        let mut format = ResponseFormat::Json;
        let mut json_path = "$".to_string();
        let mut headers = Vec::new();
        let mut body = None;
        let mut content_type = "application/json".to_string();
        let mut timeout_secs = 30u64;
        let mut cache_ttl_secs = 0u64;
        let mut mcp_tool = None;
        let mut mcp_resource = None;
        let mut columns_str = None;

        for arg in args {
            let text = arg.to_text().ok_or(ResultCode::InvalidArgs)?;
            let parts: Vec<&str> = text.splitn(2, '=').collect();
            if parts.len() != 2 {
                return Err(ResultCode::InvalidArgs);
            }
            let (key, value) = (parts[0], parts[1]);
            match key.trim() {
                "url" => url = Some(value.trim().to_string()),
                "method" => method = value.trim().to_uppercase(),
                "format" => {
                    format = match value.trim() {
                        "csv" => ResponseFormat::Csv,
                        "mcp" => ResponseFormat::Mcp,
                        _ => ResponseFormat::Json,
                    }
                }
                "json_path" => json_path = value.trim().to_string(),
                "headers" => {
                    for line in value.split('\n') {
                        if let Some((k, v)) = line.split_once(':') {
                            headers.push((k.trim().to_string(), v.trim().to_string()));
                        }
                    }
                }
                "body" => body = Some(value.to_string()),
                "content_type" => content_type = value.trim().to_string(),
                "timeout" => timeout_secs = value.trim().parse().unwrap_or(30),
                "cache_ttl" => cache_ttl_secs = value.trim().parse().unwrap_or(0),
                "tool" => mcp_tool = Some(value.trim().to_string()),
                "resource" => mcp_resource = Some(value.trim().to_string()),
                "columns" => columns_str = Some(value.to_string()),
                _ => return Err(ResultCode::InvalidArgs),
            }
        }

        let url = url.ok_or(ResultCode::InvalidArgs)?;
        let column_defs = parse_column_defs(columns_str.as_deref().unwrap_or(""))?;
        if column_defs.is_empty() {
            return Err(ResultCode::InvalidArgs);
        }

        // Auto-set MCP defaults
        if mcp_tool.is_some() || mcp_resource.is_some() {
            format = ResponseFormat::Mcp;
            method = "POST".to_string();
        }

        Ok(HttpConfig {
            url,
            method,
            format,
            json_path,
            headers,
            body,
            content_type,
            timeout_secs,
            cache_ttl_secs,
            mcp_tool,
            mcp_resource,
            column_defs,
        })
    }

    fn to_create_table_sql(&self) -> String {
        column_defs_to_create_table_sql(&self.column_defs)
    }
}

// ---------------------------------------------------------------------------
// JSON response parsing
// ---------------------------------------------------------------------------

fn parse_json_response(body: &str, config: &HttpConfig) -> Vec<Vec<serde_json::Value>> {
    let json: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    // Apply JSONPath to extract array.
    // JsonPath::from_str() via FromStr trait (parse() does not exist).
    // find() returns a serde_json::Value (JSON array of matched items), not an iterator.
    use std::str::FromStr;
    let found = match jsonpath_rust::JsonPath::from_str(&config.json_path) {
        Ok(path) => path.find(&json),
        Err(_) => return Vec::new(),
    };

    // find() returns a JSON array of matched values. If a match is itself an
    // array (e.g. $ on [obj1, obj2]), flatten so each element becomes a row.
    let items: Vec<serde_json::Value> = match found.as_array() {
        Some(arr) => {
            let mut result = Vec::new();
            for match_item in arr {
                if let Some(inner_arr) = match_item.as_array() {
                    result.extend(inner_arr.iter().cloned());
                } else {
                    result.push(match_item.clone());
                }
            }
            result
        }
        None => vec![found],
    };

    // All columns (not just visible) — we map by column index
    let all_cols: Vec<&ColumnDef> = config.column_defs.iter().collect();

    let mut rows = Vec::new();
    for item in &items {
        match item {
            serde_json::Value::Object(obj) => {
                let row: Vec<serde_json::Value> = all_cols
                    .iter()
                    .map(|col| {
                        obj.get(&col.name)
                            .cloned()
                            .unwrap_or(serde_json::Value::Null)
                    })
                    .collect();
                rows.push(row);
            }
            serde_json::Value::Array(arr) => {
                let row: Vec<serde_json::Value> = all_cols
                    .iter()
                    .enumerate()
                    .map(|(i, _)| arr.get(i).cloned().unwrap_or(serde_json::Value::Null))
                    .collect();
                rows.push(row);
            }
            scalar => {
                rows.push(vec![scalar.clone()]);
            }
        }
    }
    rows
}

// ---------------------------------------------------------------------------
// CSV response parsing
// ---------------------------------------------------------------------------

fn parse_csv_response(body: &str, config: &HttpConfig) -> Vec<Vec<serde_json::Value>> {
    let all_cols: Vec<&ColumnDef> = config.column_defs.iter().collect();

    let mut rows = Vec::new();
    let mut lines = body.lines();

    // Skip header row
    let _header = lines.next();

    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split(',').collect();
        let row: Vec<serde_json::Value> = all_cols
            .iter()
            .enumerate()
            .map(|(i, col)| {
                let field = fields.get(i).map(|s| s.trim()).unwrap_or("");
                match col.ty.to_uppercase().as_str() {
                    "INTEGER" | "INT" => field
                        .parse::<i64>()
                        .map(|n| serde_json::Value::Number(n.into()))
                        .unwrap_or(serde_json::Value::Null),
                    "REAL" | "FLOAT" | "DOUBLE" => field
                        .parse::<f64>()
                        .ok()
                        .and_then(serde_json::Number::from_f64)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
                    _ => serde_json::Value::String(field.to_string()),
                }
            })
            .collect();
        rows.push(row);
    }
    rows
}

// ---------------------------------------------------------------------------
// MCP protocol helpers
// ---------------------------------------------------------------------------

fn build_mcp_request_body(config: &HttpConfig, params: &HashMap<String, String>) -> String {
    let arguments: serde_json::Value = params
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
        .collect::<serde_json::Map<String, serde_json::Value>>()
        .into();

    if let Some(tool) = &config.mcp_tool {
        serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": { "name": tool, "arguments": arguments }
        })
        .to_string()
    } else if let Some(resource) = &config.mcp_resource {
        serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "resources/read",
            "params": { "uri": resource }
        })
        .to_string()
    } else {
        "{}".to_string()
    }
}

fn parse_mcp_response(
    body: &str,
    config: &HttpConfig,
    _params: &HashMap<String, String>,
) -> Vec<Vec<serde_json::Value>> {
    let json: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    // MCP response: {"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"..."}]}}
    let content = json.pointer("/result/content").and_then(|v| v.as_array());
    let Some(items) = content else {
        return Vec::new();
    };

    // All columns for extraction
    let all_cols: Vec<&ColumnDef> = config.column_defs.iter().collect();

    let mut rows = Vec::new();
    for item in items {
        if item.get("type").and_then(|t| t.as_str()) == Some("text") {
            if let Some(text) = item.get("text").and_then(|t| t.as_str()) {
                if let Ok(obj) = serde_json::from_str::<serde_json::Value>(text) {
                    let row: Vec<serde_json::Value> = all_cols
                        .iter()
                        .map(|col| {
                            obj.get(&col.name)
                                .cloned()
                                .unwrap_or(serde_json::Value::Null)
                        })
                        .collect();
                    rows.push(row);
                }
            }
        }
    }
    rows
}

// ---------------------------------------------------------------------------
// VTabModule: HttpVTabModule
// ---------------------------------------------------------------------------

#[derive(Debug, VTabModuleDerive, Default)]
struct HttpVTabModule;

impl VTabModule for HttpVTabModule {
    type Table = HttpTable;
    const VTAB_KIND: VTabKind = VTabKind::VirtualTable;
    const NAME: &'static str = "http";
    const READONLY: bool = true;

    fn create(args: &[Value]) -> Result<(String, Self::Table), ResultCode> {
        let config = HttpConfig::parse(args)?;
        let schema = config.to_create_table_sql();
        Ok((schema, HttpTable { config }))
    }
}

// ---------------------------------------------------------------------------
// VTable: HttpTable
// ---------------------------------------------------------------------------

struct HttpTable {
    config: HttpConfig,
}

impl VTable for HttpTable {
    type Cursor = HttpCursor;
    type Error = ResultCode;

    fn open(&self, _conn: Option<Arc<Connection>>) -> Result<Self::Cursor, Self::Error> {
        Ok(HttpCursor {
            config: self.config.clone(),
            rows: Vec::new(),
            current_row: 0,
        })
    }

    // best_index is static — no &self, cannot access table config.
    // Strategy: accept ALL usable Eq constraints, encode column indices in idx_str.
    fn best_index(
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        let mut usages = Vec::with_capacity(constraints.len());
        let mut consumed_columns = Vec::new();
        let mut next_argv = 1u32;

        for c in constraints {
            if c.usable && c.op == ConstraintOp::Eq {
                usages.push(ConstraintUsage {
                    argv_index: Some(next_argv),
                    omit: true,
                });
                consumed_columns.push(c.column_index);
                next_argv += 1;
            } else {
                usages.push(ConstraintUsage {
                    argv_index: None,
                    omit: false,
                });
            }
        }

        let idx_str = if consumed_columns.is_empty() {
            None
        } else {
            Some(
                consumed_columns
                    .iter()
                    .map(|c| c.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            )
        };

        Ok(IndexInfo {
            idx_num: 0,
            idx_str,
            order_by_consumed: false,
            estimated_cost: if consumed_columns.is_empty() {
                1_000_000.0
            } else {
                1000.0
            },
            estimated_rows: if consumed_columns.is_empty() {
                u32::MAX
            } else {
                100
            },
            constraint_usages: usages,
        })
    }
}

// ---------------------------------------------------------------------------
// VTabCursor: HttpCursor
// ---------------------------------------------------------------------------

struct HttpCursor {
    config: HttpConfig,
    rows: Vec<Vec<serde_json::Value>>,
    current_row: usize,
}

impl HttpCursor {
    fn execute_request(
        &self,
        url: &str,
        params: &HashMap<String, String>,
    ) -> Result<String, String> {
        let timeout = std::time::Duration::from_secs(self.config.timeout_secs.max(1));
        let agent = ureq::AgentBuilder::new().timeout(timeout).build();

        let mut req = match self.config.method.as_str() {
            "POST" | "PUT" => agent.request(&self.config.method, url),
            _ => agent.get(url),
        };

        for (key, value) in &self.config.headers {
            req = req.set(key, value);
        }

        let response = if self.config.method == "POST" || self.config.method == "PUT" {
            let body = if self.config.format == ResponseFormat::Mcp {
                build_mcp_request_body(&self.config, params)
            } else {
                self.config.body.clone().unwrap_or_default()
            };
            req.set("Content-Type", &self.config.content_type)
                .send_string(&body)
                .map_err(|e| format!("HTTP request failed: {e}"))?
        } else {
            req.call()
                .map_err(|e| format!("HTTP request failed: {e}"))?
        };

        response
            .into_string()
            .map_err(|e| format!("Failed to read response body: {e}"))
    }
}

impl VTabCursor for HttpCursor {
    type Error = ResultCode;

    fn filter(&mut self, args: &[Value], idx_info: Option<(&str, i32)>) -> ResultCode {
        // Decode idx_str to know which column each arg corresponds to
        let consumed_columns: Vec<usize> = match idx_info {
            Some((idx_str, _)) => idx_str
                .split(',')
                .filter_map(|s| s.parse::<usize>().ok())
                .collect(),
            None => Vec::new(),
        };

        // Map args to column names using self.config (cursor HAS instance access).
        // Value has no Display impl — use to_text()/to_integer()/to_float().
        let mut params: HashMap<String, String> = HashMap::new();
        for (i, col_idx) in consumed_columns.iter().enumerate() {
            if let Some(arg) = args.get(i) {
                if *col_idx < self.config.column_defs.len() {
                    let col = &self.config.column_defs[*col_idx];
                    let val_str = if let Some(s) = arg.to_text() {
                        s.to_owned()
                    } else if let Some(n) = arg.to_integer() {
                        n.to_string()
                    } else if let Some(f) = arg.to_float() {
                        f.to_string()
                    } else {
                        String::new()
                    };
                    params.insert(col.name.clone(), val_str);
                }
            }
        }

        // Build HTTP request URL
        let mut url = self.config.url.clone();
        if !params.is_empty() && self.config.method == "GET" {
            // Append HIDDEN column values as query params
            let query: String = params
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("&");
            if url.contains('?') {
                url = format!("{url}&{query}");
            } else {
                url = format!("{url}?{query}");
            }
        }

        // Execute HTTP request (blocking in v1)
        let response = match self.execute_request(&url, &params) {
            Ok(r) => r,
            Err(_) => {
                self.rows.clear();
                return ResultCode::EOF;
            }
        };

        // Parse response based on format
        self.rows = match self.config.format {
            ResponseFormat::Json => parse_json_response(&response, &self.config),
            ResponseFormat::Csv => parse_csv_response(&response, &self.config),
            ResponseFormat::Mcp => parse_mcp_response(&response, &self.config, &params),
        };
        self.current_row = 0;

        if self.rows.is_empty() {
            ResultCode::EOF
        } else {
            ResultCode::OK
        }
    }

    fn column(&self, idx: u32) -> Result<Value, Self::Error> {
        // Construct fresh turso_ext::Value each call (Value is NOT Clone)
        let cell = self
            .rows
            .get(self.current_row)
            .and_then(|row| row.get(idx as usize));
        match cell {
            Some(serde_json::Value::Number(n)) if n.is_i64() => {
                Ok(Value::from_integer(n.as_i64().unwrap()))
            }
            Some(serde_json::Value::Number(n)) => Ok(Value::from_float(n.as_f64().unwrap_or(0.0))),
            Some(serde_json::Value::String(s)) => Ok(Value::from_text(s.clone())),
            Some(serde_json::Value::Bool(b)) => Ok(Value::from_integer(*b as i64)),
            Some(serde_json::Value::Null) | None => Ok(Value::null()),
            Some(other) => Ok(Value::from_text(
                serde_json::to_string(other).unwrap_or_default(),
            )),
        }
    }

    fn eof(&self) -> bool {
        self.current_row >= self.rows.len()
    }

    fn next(&mut self) -> ResultCode {
        self.current_row += 1;
        if self.current_row >= self.rows.len() {
            ResultCode::EOF
        } else {
            ResultCode::OK
        }
    }

    fn rowid(&self) -> i64 {
        self.current_row as i64
    }
}

// ---------------------------------------------------------------------------
// Extension registration
// ---------------------------------------------------------------------------

register_extension! {
    vtabs: { HttpVTabModule }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> HttpConfig {
        HttpConfig {
            url: "https://example.com".to_string(),
            method: "GET".to_string(),
            format: ResponseFormat::Json,
            json_path: "$".to_string(),
            headers: Vec::new(),
            body: None,
            content_type: "application/json".to_string(),
            timeout_secs: 30,
            cache_ttl_secs: 0,
            mcp_tool: None,
            mcp_resource: None,
            column_defs: vec![
                ColumnDef {
                    name: "id".into(),
                    ty: "INTEGER".into(),
                    hidden: false,
                },
                ColumnDef {
                    name: "name".into(),
                    ty: "TEXT".into(),
                    hidden: false,
                },
            ],
        }
    }

    // ---- Config parsing tests ----

    #[test]
    fn test_parse_http_config_basic() {
        let args = vec![
            Value::from_text("url=https://api.example.com/data".into()),
            Value::from_text("format=json".into()),
            Value::from_text("columns=id INTEGER, name TEXT".into()),
        ];
        let config = HttpConfig::parse(&args).unwrap();
        assert_eq!(config.url, "https://api.example.com/data");
        assert_eq!(config.format, ResponseFormat::Json);
        assert_eq!(config.column_defs.len(), 2);
        assert_eq!(config.column_defs[0].name, "id");
        assert_eq!(config.column_defs[0].ty, "INTEGER");
        assert!(!config.column_defs[0].hidden);
        assert_eq!(config.column_defs[1].name, "name");
        assert_eq!(config.column_defs[1].ty, "TEXT");
    }

    #[test]
    fn test_parse_hidden_columns() {
        let args = vec![
            Value::from_text("url=https://example.com".into()),
            Value::from_text("columns=city TEXT HIDDEN, temp REAL".into()),
        ];
        let config = HttpConfig::parse(&args).unwrap();
        assert!(config.column_defs[0].hidden);
        assert_eq!(config.column_defs[0].name, "city");
        assert!(!config.column_defs[1].hidden);
        assert_eq!(config.column_defs[1].name, "temp");
        assert_eq!(config.column_defs[1].ty, "REAL");
    }

    #[test]
    fn test_parse_config_with_method_and_body() {
        let args = vec![
            Value::from_text("url=https://example.com/graphql".into()),
            Value::from_text("method=POST".into()),
            Value::from_text("body={\"query\": \"{ users { id name } }\"}".into()),
            Value::from_text("columns=id INTEGER, name TEXT".into()),
        ];
        let config = HttpConfig::parse(&args).unwrap();
        assert_eq!(config.method, "POST");
        assert!(config.body.is_some());
    }

    #[test]
    fn test_parse_config_csv_format() {
        let args = vec![
            Value::from_text("url=https://example.com/data.csv".into()),
            Value::from_text("format=csv".into()),
            Value::from_text("columns=date TEXT, value REAL".into()),
        ];
        let config = HttpConfig::parse(&args).unwrap();
        assert_eq!(config.format, ResponseFormat::Csv);
    }

    #[test]
    fn test_parse_config_mcp_auto_sets_post() {
        let args = vec![
            Value::from_text("url=http://localhost:3000/mcp".into()),
            Value::from_text("tool=search_documents".into()),
            Value::from_text("columns=query TEXT HIDDEN, id INTEGER, title TEXT".into()),
        ];
        let config = HttpConfig::parse(&args).unwrap();
        assert_eq!(config.format, ResponseFormat::Mcp);
        assert_eq!(config.method, "POST");
        assert_eq!(config.mcp_tool, Some("search_documents".to_string()));
    }

    #[test]
    fn test_parse_config_missing_url_fails() {
        let args = vec![Value::from_text("columns=id INTEGER, name TEXT".into())];
        assert!(HttpConfig::parse(&args).is_err());
    }

    #[test]
    fn test_parse_config_missing_columns_fails() {
        let args = vec![Value::from_text("url=https://example.com".into())];
        assert!(HttpConfig::parse(&args).is_err());
    }

    #[test]
    fn test_parse_config_with_headers() {
        let args = vec![
            Value::from_text("url=https://example.com".into()),
            Value::from_text("headers=Authorization: Bearer token123\nX-Custom: value".into()),
            Value::from_text("columns=id INTEGER".into()),
        ];
        let config = HttpConfig::parse(&args).unwrap();
        assert_eq!(config.headers.len(), 2);
        assert_eq!(config.headers[0].0, "Authorization");
        assert_eq!(config.headers[0].1, "Bearer token123");
        assert_eq!(config.headers[1].0, "X-Custom");
        assert_eq!(config.headers[1].1, "value");
    }

    // ---- Schema generation tests ----

    #[test]
    fn test_create_table_sql() {
        let defs = vec![
            ColumnDef {
                name: "city".into(),
                ty: "TEXT".into(),
                hidden: true,
            },
            ColumnDef {
                name: "temp".into(),
                ty: "REAL".into(),
                hidden: false,
            },
        ];
        let sql = column_defs_to_create_table_sql(&defs);
        assert_eq!(sql, "CREATE TABLE x (city TEXT HIDDEN, temp REAL)");
    }

    // ---- best_index tests ----

    #[test]
    fn test_best_index_encodes_constraints() {
        let constraints = vec![
            ConstraintInfo {
                column_index: 0,
                op: ConstraintOp::Eq,
                usable: true,
                index: 0,
            },
            ConstraintInfo {
                column_index: 2,
                op: ConstraintOp::Eq,
                usable: true,
                index: 1,
            },
        ];
        let result = HttpTable::best_index(&constraints, &[]).unwrap();
        assert_eq!(result.idx_str, Some("0,2".to_string()));
        assert_eq!(result.constraint_usages[0].argv_index, Some(1));
        assert_eq!(result.constraint_usages[1].argv_index, Some(2));
        assert!(result.constraint_usages[0].omit);
        assert!(result.constraint_usages[1].omit);
    }

    #[test]
    fn test_best_index_skips_non_eq() {
        let constraints = vec![
            ConstraintInfo {
                column_index: 0,
                op: ConstraintOp::Eq,
                usable: true,
                index: 0,
            },
            ConstraintInfo {
                column_index: 1,
                op: ConstraintOp::Gt,
                usable: true,
                index: 1,
            },
        ];
        let result = HttpTable::best_index(&constraints, &[]).unwrap();
        assert_eq!(result.idx_str, Some("0".to_string()));
        assert_eq!(result.constraint_usages[0].argv_index, Some(1));
        assert_eq!(result.constraint_usages[1].argv_index, None);
        assert!(!result.constraint_usages[1].omit);
    }

    #[test]
    fn test_best_index_no_constraints() {
        let result = HttpTable::best_index(&[], &[]).unwrap();
        assert_eq!(result.idx_str, None);
        assert_eq!(result.estimated_cost, 1_000_000.0);
    }

    #[test]
    fn test_best_index_skips_unusable() {
        let constraints = vec![ConstraintInfo {
            column_index: 0,
            op: ConstraintOp::Eq,
            usable: false,
            index: 0,
        }];
        let result = HttpTable::best_index(&constraints, &[]).unwrap();
        assert_eq!(result.idx_str, None);
        assert_eq!(result.constraint_usages[0].argv_index, None);
    }

    // ---- JSON parsing tests ----

    #[test]
    fn test_json_response_parsing() {
        let body = r#"[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]"#;
        let config = test_config();
        let rows = parse_json_response(body, &config);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], serde_json::json!(1));
        assert_eq!(rows[0][1], serde_json::json!("Alice"));
        assert_eq!(rows[1][0], serde_json::json!(2));
        assert_eq!(rows[1][1], serde_json::json!("Bob"));
    }

    #[test]
    fn test_json_nested_path() {
        let body = r#"{"data": {"users": [{"id": 1, "name": "Alice"}]}}"#;
        let config = HttpConfig {
            json_path: "$.data.users".into(),
            ..test_config()
        };
        let rows = parse_json_response(body, &config);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], serde_json::json!(1));
        assert_eq!(rows[0][1], serde_json::json!("Alice"));
    }

    #[test]
    fn test_json_root_array_default_path() {
        let body = r#"[{"id": 10, "name": "Charlie"}]"#;
        let config = HttpConfig {
            json_path: "$".into(),
            ..test_config()
        };
        let rows = parse_json_response(body, &config);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], serde_json::json!(10));
    }

    #[test]
    fn test_json_missing_fields_are_null() {
        let body = r#"[{"id": 1}]"#;
        let config = test_config();
        let rows = parse_json_response(body, &config);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], serde_json::json!(1));
        assert_eq!(rows[0][1], serde_json::Value::Null);
    }

    #[test]
    fn test_json_invalid_body_returns_empty() {
        let rows = parse_json_response("not valid json", &test_config());
        assert!(rows.is_empty());
    }

    // ---- CSV parsing tests ----

    #[test]
    fn test_csv_response_parsing() {
        let body = "id,name\n1,Alice\n2,Bob\n";
        let config = test_config();
        let rows = parse_csv_response(body, &config);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], serde_json::json!(1));
        assert_eq!(rows[0][1], serde_json::json!("Alice"));
        assert_eq!(rows[1][0], serde_json::json!(2));
        assert_eq!(rows[1][1], serde_json::json!("Bob"));
    }

    #[test]
    fn test_csv_skips_empty_lines() {
        let body = "id,name\n1,Alice\n\n2,Bob\n";
        let config = test_config();
        let rows = parse_csv_response(body, &config);
        assert_eq!(rows.len(), 2);
    }

    // ---- MCP tests ----

    #[test]
    fn test_build_mcp_tool_request() {
        let config = HttpConfig {
            mcp_tool: Some("search_docs".into()),
            ..test_config()
        };
        let mut params = HashMap::new();
        params.insert("query".to_string(), "test search".to_string());
        let body = build_mcp_request_body(&config, &params);
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["method"], "tools/call");
        assert_eq!(json["params"]["name"], "search_docs");
        assert_eq!(json["params"]["arguments"]["query"], "test search");
    }

    #[test]
    fn test_build_mcp_resource_request() {
        let config = HttpConfig {
            mcp_tool: None,
            mcp_resource: Some("docs://readme".into()),
            ..test_config()
        };
        let params = HashMap::new();
        let body = build_mcp_request_body(&config, &params);
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["method"], "resources/read");
        assert_eq!(json["params"]["uri"], "docs://readme");
    }

    #[test]
    fn test_parse_mcp_response_text_content() {
        let body = r#"{
            "jsonrpc": "2.0", "id": 1,
            "result": {
                "content": [
                    {"type": "text", "text": "{\"id\": 1, \"name\": \"Alice\"}"},
                    {"type": "text", "text": "{\"id\": 2, \"name\": \"Bob\"}"}
                ]
            }
        }"#;
        let config = test_config();
        let params = HashMap::new();
        let rows = parse_mcp_response(body, &config, &params);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], serde_json::json!(1));
        assert_eq!(rows[0][1], serde_json::json!("Alice"));
    }

    #[test]
    fn test_parse_mcp_response_non_text_ignored() {
        let body = r#"{
            "jsonrpc": "2.0", "id": 1,
            "result": {
                "content": [
                    {"type": "image", "data": "base64..."},
                    {"type": "text", "text": "{\"id\": 1, \"name\": \"Alice\"}"}
                ]
            }
        }"#;
        let config = test_config();
        let rows = parse_mcp_response(body, &config, &HashMap::new());
        assert_eq!(rows.len(), 1);
    }

    // ---- Column type conversion test ----

    #[test]
    fn test_column_defs_parsing() {
        let defs = parse_column_defs("id INTEGER, name TEXT, city TEXT HIDDEN").unwrap();
        assert_eq!(defs.len(), 3);
        assert_eq!(defs[0].name, "id");
        assert_eq!(defs[0].ty, "INTEGER");
        assert!(!defs[0].hidden);
        assert_eq!(defs[2].name, "city");
        assert!(defs[2].hidden);
    }

    #[test]
    fn test_empty_column_defs() {
        let defs = parse_column_defs("").unwrap();
        assert!(defs.is_empty());
    }

    #[test]
    fn test_column_defs_single_token_fails() {
        assert!(parse_column_defs("badcolumn").is_err());
    }
}
