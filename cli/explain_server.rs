//! A local HTTP server that draws query plans.
//!
//! `tursodb mydb.db --explain-server 127.0.0.1:8080` opens a page where you
//! type SQL and see the plan as a diagram instead of a tree of text. The page
//! asks this server for [`turso_core::explain_plan::QueryPlan`] JSON and lays
//! it out; nothing here knows how the drawing works.
//!
//! The HTTP handling is deliberately small — one thread, one request at a
//! time, no dependencies. This is a developer tool that binds to loopback by
//! default, not a production server.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, Result};
use tracing::{debug, error, info};

use turso_core::{Connection, Value};

const INDEX_HTML: &str = include_str!("assets/explain/index.html");
const APP_CSS: &str = include_str!("assets/explain/app.css");
const APP_JS: &str = include_str!("assets/explain/app.js");

/// Requests larger than this are refused rather than buffered. A SQL statement
/// that does not fit in a megabyte is not something you want to look at as a
/// diagram anyway.
const MAX_BODY_BYTES: usize = 1024 * 1024;

pub struct TursoExplainServer {
    address: String,
    db_path: String,
    conn: Arc<Connection>,
    interrupt_count: Arc<AtomicUsize>,
}

struct HttpResponse {
    status: u16,
    content_type: &'static str,
    body: Vec<u8>,
}

impl HttpResponse {
    fn json(status: u16, body: String) -> Self {
        Self {
            status,
            content_type: "application/json; charset=utf-8",
            body: body.into_bytes(),
        }
    }

    fn asset(content_type: &'static str, body: &str) -> Self {
        Self {
            status: 200,
            content_type,
            body: body.as_bytes().to_vec(),
        }
    }
}

impl TursoExplainServer {
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
        let bound = listener.local_addr()?;
        listener.set_nonblocking(true)?;

        println!("Turso query plan viewer: http://{bound}/");
        println!("Database: {}", self.db_path);
        println!("Press Ctrl-C to stop.");
        info!("explain server listening on {bound}");

        let shutdown = Arc::new(AtomicBool::new(false));
        let monitor = {
            let interrupt_count = self.interrupt_count.clone();
            let shutdown = shutdown.clone();
            thread::spawn(move || loop {
                if interrupt_count.load(Ordering::SeqCst) > 0 {
                    shutdown.store(true, Ordering::SeqCst);
                    break;
                }
                thread::sleep(Duration::from_millis(100));
            })
        };

        while !shutdown.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((stream, _addr)) => {
                    if let Err(e) = self.handle_connection(stream) {
                        error!("error handling connection: {e}");
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(e) => error!("error accepting connection: {e}"),
            }
        }

        let _ = monitor.join();
        info!("explain server stopped");
        Ok(())
    }

    fn handle_connection(&self, mut stream: TcpStream) -> Result<()> {
        stream.set_nonblocking(false)?;
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;

        let (method, path, body) = read_request(&mut stream)?;
        debug!("{method} {path}");

        let response = match (method.as_str(), path.as_str()) {
            ("OPTIONS", _) => HttpResponse {
                status: 204,
                content_type: "text/plain; charset=utf-8",
                body: Vec::new(),
            },
            ("GET", "/") | ("GET", "/index.html") => {
                HttpResponse::asset("text/html; charset=utf-8", INDEX_HTML)
            }
            ("GET", "/app.css") => HttpResponse::asset("text/css; charset=utf-8", APP_CSS),
            ("GET", "/app.js") => {
                HttpResponse::asset("application/javascript; charset=utf-8", APP_JS)
            }
            ("GET", "/api/schema") => match self.schema_json() {
                Ok(json) => HttpResponse::json(200, json),
                Err(e) => HttpResponse::json(500, error_json(&e.to_string())),
            },
            ("POST", "/api/plan") => self.plan_response(&body),
            _ => HttpResponse {
                status: 404,
                content_type: "text/plain; charset=utf-8",
                body: b"Not Found".to_vec(),
            },
        };

        stream.write_all(&format_response(&response))?;
        stream.flush()?;
        Ok(())
    }

    /// Compiles the posted SQL and answers with its plan, or with the error
    /// the compiler produced. A statement that does not compile is a normal
    /// answer here, not a server failure: the page shows it next to the editor.
    fn plan_response(&self, body: &[u8]) -> HttpResponse {
        let sql = match json_string_field(body, "sql") {
            Some(sql) => sql,
            None => {
                return HttpResponse::json(400, error_json("request body needs a \"sql\" string"))
            }
        };
        match self.conn.query_plan(&sql) {
            Ok(plan) => HttpResponse::json(200, plan.to_json()),
            Err(e) => HttpResponse::json(200, error_json(&e.to_string())),
        }
    }

    /// The tables and indexes in the database, so the page can offer them
    /// while you type.
    fn schema_json(&self) -> Result<String> {
        let mut tables: Vec<(String, Vec<String>)> = Vec::new();
        for name in self.table_names()? {
            let columns = self.table_columns(&name)?;
            tables.push((name, columns));
        }

        let mut out = String::from("{\"database\":");
        push_json_string(&mut out, &self.db_path);
        out.push_str(",\"tables\":[");
        for (i, (name, columns)) in tables.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str("{\"name\":");
            push_json_string(&mut out, name);
            out.push_str(",\"columns\":[");
            for (j, column) in columns.iter().enumerate() {
                if j > 0 {
                    out.push(',');
                }
                push_json_string(&mut out, column);
            }
            out.push_str("]}");
        }
        out.push_str("]}");
        Ok(out)
    }

    fn table_names(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT name FROM sqlite_schema \
             WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
        )?;
        Ok(stmt
            .run_collect_rows()?
            .iter()
            .filter_map(|row| row.first().map(value_to_string))
            .collect())
    }

    fn table_columns(&self, table: &str) -> Result<Vec<String>> {
        // `table` comes from sqlite_schema, so it names a real table, but it
        // still gets quoted: a table can be called `my"table`.
        let mut stmt = self.conn.prepare(format!(
            "PRAGMA table_info(\"{}\")",
            table.replace('"', "\"\"")
        ))?;
        Ok(stmt
            .run_collect_rows()?
            .iter()
            .filter_map(|row| row.get(1).map(value_to_string))
            .collect())
    }
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::Text(text) => text.value.to_string(),
        other => other.to_string(),
    }
}

fn read_request(stream: &mut TcpStream) -> Result<(String, String, Vec<u8>)> {
    let mut buffer = [0u8; 8192];
    let mut data = Vec::new();

    let header_end = loop {
        if let Some(end) = find_header_end(&data) {
            break end;
        }
        if data.len() > MAX_BODY_BYTES {
            return Err(anyhow!("request headers too large"));
        }
        let n = stream.read(&mut buffer)?;
        if n == 0 {
            return Err(anyhow!("connection closed before the request finished"));
        }
        data.extend_from_slice(&buffer[..n]);
    };

    let headers = String::from_utf8_lossy(&data[..header_end]).to_string();
    let mut lines = headers.lines();
    let request_line = lines.next().ok_or_else(|| anyhow!("empty request"))?;
    let mut parts = request_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| anyhow!("no method in request line"))?
        .to_string();
    let path = parts
        .next()
        .ok_or_else(|| anyhow!("no path in request line"))?
        .to_string();

    let content_length = lines
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.trim()
                .eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())?
        })
        .unwrap_or(0);
    if content_length > MAX_BODY_BYTES {
        return Err(anyhow!("request body too large"));
    }

    let body_start = header_end + 4;
    while data.len() < body_start + content_length {
        let n = stream.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        data.extend_from_slice(&buffer[..n]);
    }

    let body = data
        .get(body_start..body_start + content_length)
        .unwrap_or_default()
        .to_vec();
    Ok((method, path, body))
}

fn find_header_end(data: &[u8]) -> Option<usize> {
    data.windows(4).position(|w| w == b"\r\n\r\n")
}

fn format_response(resp: &HttpResponse) -> Vec<u8> {
    let status_text = match resp.status {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        _ => "Internal Server Error",
    };
    let mut out = format!(
        "HTTP/1.1 {} {}\r\n\
         Content-Type: {}\r\n\
         Content-Length: {}\r\n\
         Cache-Control: no-store\r\n\
         Connection: close\r\n\
         \r\n",
        resp.status,
        status_text,
        resp.content_type,
        resp.body.len()
    )
    .into_bytes();
    out.extend_from_slice(&resp.body);
    out
}

fn error_json(message: &str) -> String {
    let mut out = String::from("{\"error\":");
    push_json_string(&mut out, message);
    out.push('}');
    out
}

/// Reads one top-level string field out of a small JSON object.
///
/// The only request body this server takes is `{"sql": "..."}`, so a full JSON
/// parser would be more machinery than the job needs. Anything that is not
/// that shape returns `None` and the caller answers with a 400.
fn json_string_field(body: &[u8], field: &str) -> Option<String> {
    let text = std::str::from_utf8(body).ok()?;
    let key = format!("\"{field}\"");
    let after_key = &text[text.find(&key)? + key.len()..];
    let after_colon = after_key.trim_start().strip_prefix(':')?.trim_start();
    let mut chars = after_colon.strip_prefix('"')?.chars();
    let mut value = String::new();
    loop {
        match chars.next()? {
            '"' => return Some(value),
            '\\' => match chars.next()? {
                'n' => value.push('\n'),
                'r' => value.push('\r'),
                't' => value.push('\t'),
                'b' => value.push('\u{8}'),
                'f' => value.push('\u{c}'),
                'u' => {
                    let hex: String = (0..4).filter_map(|_| chars.next()).collect();
                    let code = u32::from_str_radix(&hex, 16).ok()?;
                    value.push(char::from_u32(code)?);
                }
                escaped => value.push(escaped),
            },
            c => value.push(c),
        }
    }
}

fn push_json_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_sql_field_out_of_a_request_body() {
        let body = br#"{"sql": "SELECT 1"}"#;
        assert_eq!(json_string_field(body, "sql").as_deref(), Some("SELECT 1"));
    }

    #[test]
    fn unescapes_quotes_newlines_and_unicode() {
        let body = r#"{"sql":"SELECT \"a\"\nFROM t -- é"}"#.as_bytes();
        assert_eq!(
            json_string_field(body, "sql").as_deref(),
            Some("SELECT \"a\"\nFROM t -- é")
        );
    }

    #[test]
    fn a_body_without_the_field_reads_as_missing() {
        assert_eq!(json_string_field(b"{\"other\":1}", "sql"), None);
        assert_eq!(json_string_field(b"not json", "sql"), None);
        assert_eq!(json_string_field(b"{\"sql\":42}", "sql"), None);
    }

    #[test]
    fn header_end_is_the_blank_line() {
        assert_eq!(find_header_end(b"GET / HTTP/1.1\r\n\r\nbody"), Some(14));
        assert_eq!(find_header_end(b"GET / HTTP/1.1\r\n"), None);
    }
}
