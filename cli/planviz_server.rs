//! Local web server for the query plan visualizer.
//!
//! Serves a single-page UI that sends SQL to `POST /plan`, gets back the
//! machine-readable EXPLAIN QUERY PLAN JSON from
//! `turso_core::Statement::query_plan_json`, and draws it as a graph.
//! Everything is self-contained: no external assets, no extra dependencies.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use tracing::{debug, error, info};

use turso_core::{Connection, Value};

const PLANVIZ_HTML: &str = include_str!("planviz.html");

pub struct PlanVizServer {
    address: String,
    db_path: String,
    conn: Arc<Connection>,
    interrupt_count: Arc<AtomicUsize>,
}

impl PlanVizServer {
    pub fn new(
        address: String,
        db_path: String,
        conn: Arc<Connection>,
        interrupt_count: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            address,
            db_path,
            conn,
            interrupt_count,
        }
    }

    pub fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.address)?;
        listener.set_nonblocking(true)?;

        let shown_address = self.address.replace("0.0.0.0", "127.0.0.1");
        println!("Turso plan visualizer for {}", self.db_path);
        println!("Open http://{shown_address}/ in your browser. Press Ctrl+C to stop.");

        loop {
            if self.interrupt_count.load(Ordering::SeqCst) > 0 {
                info!("interrupt received, stopping plan visualizer");
                break;
            }
            match listener.accept() {
                Ok((stream, addr)) => {
                    debug!("accepted connection from {addr}");
                    if let Err(e) = self.handle_connection(stream) {
                        error!("error handling request: {e}");
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                Err(e) => error!("error accepting connection: {e}"),
            }
        }
        Ok(())
    }

    fn handle_connection(&self, mut stream: TcpStream) -> Result<()> {
        stream.set_nonblocking(false)?;
        stream.set_read_timeout(Some(std::time::Duration::from_secs(10)))?;

        let request = read_http_request(&mut stream)?;
        let (method, path, body) = parse_http_request(&request)?;
        debug!("request: {method} {path}");

        let response = match (method.as_str(), path.as_str()) {
            ("GET", "/") => HttpResponse::html(PLANVIZ_HTML),
            ("GET", "/schema") => self.handle_schema(),
            ("POST", "/plan") => self.handle_plan(&body),
            _ => HttpResponse {
                status: 404,
                content_type: "text/plain",
                body: b"Not Found".to_vec(),
            },
        };

        stream.write_all(&response.serialize())?;
        stream.flush()?;
        Ok(())
    }

    /// Prepare `EXPLAIN QUERY PLAN <sql>` and return the plan JSON.
    /// Preparing never executes the statement, so this cannot modify the
    /// database even for INSERT/UPDATE/DELETE input.
    fn handle_plan(&self, body: &[u8]) -> HttpResponse {
        let sql = match serde_json::from_slice::<serde_json::Value>(body) {
            Ok(req) => match req.get("sql").and_then(|s| s.as_str()) {
                Some(sql) => sql.to_string(),
                None => return HttpResponse::error(400, "missing \"sql\" field"),
            },
            Err(e) => return HttpResponse::error(400, &format!("bad request: {e}")),
        };

        let stripped = strip_explain_prefix(&sql);
        if stripped.is_empty() {
            return HttpResponse::error(400, "empty statement");
        }
        let eqp_sql = format!("EXPLAIN QUERY PLAN {stripped}");

        let stmt = match self.conn.prepare(&eqp_sql) {
            Ok(stmt) => stmt,
            Err(e) => return HttpResponse::error(200, &format!("{e}")),
        };
        match stmt.query_plan_json() {
            Some(plan) => HttpResponse::json(plan.into_bytes()),
            None => HttpResponse::error(200, "statement has no query plan"),
        }
    }

    /// List tables, views, and indexes so the UI can show a schema browser.
    fn handle_schema(&self) -> HttpResponse {
        let mut entries = Vec::new();
        let result = (|| -> turso_core::Result<()> {
            let mut stmt = self.conn.prepare(
                "SELECT type, name, tbl_name, sql FROM sqlite_schema \
                 WHERE name NOT LIKE 'sqlite\\_%' ESCAPE '\\' \
                 ORDER BY CASE type WHEN 'table' THEN 0 WHEN 'view' THEN 1 ELSE 2 END, tbl_name, name",
            )?;
            stmt.run_with_row_callback(|row| {
                let text = |i: usize| match row.get::<&Value>(i) {
                    Ok(Value::Text(t)) => t.as_str().to_string(),
                    _ => String::new(),
                };
                entries.push(serde_json::json!({
                    "type": text(0),
                    "name": text(1),
                    "tbl_name": text(2),
                    "sql": text(3),
                }));
                Ok(())
            })
        })();
        if let Err(e) = result {
            return HttpResponse::error(200, &format!("{e}"));
        }
        let response = serde_json::json!({
            "database": self.db_path,
            "entries": entries,
        });
        HttpResponse::json(response.to_string().into_bytes())
    }
}

/// Remove a leading EXPLAIN / EXPLAIN QUERY PLAN so we can add our own,
/// and drop a trailing semicolon (prepare handles one statement).
fn strip_explain_prefix(sql: &str) -> &str {
    let mut rest = sql.trim();
    rest = rest.trim_end_matches(';').trim_end();
    let upper = rest.to_ascii_uppercase();
    let mut words = upper.split_whitespace();
    if words.next() == Some("EXPLAIN") {
        let mut offset = upper.find("EXPLAIN").unwrap() + "EXPLAIN".len();
        if let (Some("QUERY"), Some("PLAN")) = (words.next(), words.next()) {
            offset = upper[offset..].find("PLAN").unwrap() + offset + "PLAN".len();
        }
        return rest[offset..].trim_start();
    }
    rest
}

struct HttpResponse {
    status: u16,
    content_type: &'static str,
    body: Vec<u8>,
}

impl HttpResponse {
    fn html(body: &str) -> Self {
        Self {
            status: 200,
            content_type: "text/html; charset=utf-8",
            body: body.as_bytes().to_vec(),
        }
    }

    fn json(body: Vec<u8>) -> Self {
        Self {
            status: 200,
            content_type: "application/json",
            body,
        }
    }

    /// Errors also travel as JSON so the UI can show them inline.
    fn error(status: u16, message: &str) -> Self {
        let body = serde_json::json!({ "error": message });
        Self {
            status,
            content_type: "application/json",
            body: body.to_string().into_bytes(),
        }
    }

    fn serialize(&self) -> Vec<u8> {
        let reason = match self.status {
            200 => "OK",
            400 => "Bad Request",
            404 => "Not Found",
            _ => "Error",
        };
        let mut out = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
            self.status,
            reason,
            self.content_type,
            self.body.len()
        )
        .into_bytes();
        out.extend_from_slice(&self.body);
        out
    }
}

fn read_http_request(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut buffer = [0u8; 8192];
    let mut data = Vec::new();
    loop {
        let n = stream.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        data.extend_from_slice(&buffer[..n]);
        if let Some(header_end) = find_header_end(&data) {
            let headers = String::from_utf8_lossy(&data[..header_end]);
            if let Some(content_length) = parse_content_length(&headers) {
                let total_expected = header_end + 4 + content_length;
                while data.len() < total_expected {
                    let n = stream.read(&mut buffer)?;
                    if n == 0 {
                        break;
                    }
                    data.extend_from_slice(&buffer[..n]);
                }
            }
            break;
        }
    }
    Ok(data)
}

fn parse_http_request(data: &[u8]) -> Result<(String, String, Vec<u8>)> {
    let header_end = find_header_end(data).ok_or_else(|| anyhow!("incomplete HTTP request"))?;
    let headers = String::from_utf8_lossy(&data[..header_end]);
    let mut lines = headers.lines();
    let request_line = lines.next().ok_or_else(|| anyhow!("empty request"))?;
    let mut parts = request_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| anyhow!("missing method"))?
        .to_string();
    let path = parts
        .next()
        .ok_or_else(|| anyhow!("missing path"))?
        .to_string();
    let body = data[header_end + 4..].to_vec();
    Ok((method, path, body))
}

fn find_header_end(data: &[u8]) -> Option<usize> {
    data.windows(4).position(|w| w == b"\r\n\r\n")
}

fn parse_content_length(headers: &str) -> Option<usize> {
    headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.trim()
            .eq_ignore_ascii_case("content-length")
            .then(|| value.trim().parse().ok())?
    })
}

#[cfg(test)]
mod tests {
    use super::strip_explain_prefix;

    #[test]
    fn strips_explain_prefixes_and_trailing_semicolon() {
        assert_eq!(strip_explain_prefix("SELECT 1;"), "SELECT 1");
        assert_eq!(strip_explain_prefix("explain SELECT 1"), "SELECT 1");
        assert_eq!(
            strip_explain_prefix("EXPLAIN QUERY PLAN SELECT 1;"),
            "SELECT 1"
        );
        assert_eq!(
            strip_explain_prefix("  explain query plan\n  SELECT 1  "),
            "SELECT 1"
        );
        // EXPLAIN-like prefixes inside identifiers are left alone.
        assert_eq!(
            strip_explain_prefix("SELECT explain FROM t"),
            "SELECT explain FROM t"
        );
    }
}
