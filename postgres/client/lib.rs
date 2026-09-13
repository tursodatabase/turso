// Copyright 2023-2026 the Turso authors. All rights reserved. MIT license.

//! Minimal synchronous PostgreSQL wire-protocol client.
//!
//! This crate exists for Turso's test tooling: the sqltest runner's `pg`
//! backend and the upstream regression-test runner both drive `tursopg`
//! (or a real PostgreSQL server) over the simple query protocol. It speaks
//! protocol 3.0 over TCP, handles the startup handshake with optional
//! cleartext password authentication, and decodes the backend messages the
//! simple query protocol produces. Values are returned in the text format
//! the server sent them in; no type conversion is attempted.
//!
//! The API has two levels: [`PgConn::read_message`] returns raw
//! `(tag, body)` frames for callers that need full control over rendering
//! (the regression runner reproduces psql transcripts byte-for-byte), and
//! [`PgConn::read_event`] / [`PgConn::simple_query`] decode the frames for
//! callers that just want rows and errors.

use std::collections::HashMap;
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// Errors produced by the client.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("connection rejected: {0}")]
    Rejected(String),

    #[error("invalid DSN: {0}")]
    Dsn(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Error and notice fields keyed by their single-byte field code
/// (`'S'` severity, `'M'` message, `'P'` position, ...).
pub type ErrorFields = HashMap<u8, String>;

/// Returns the human-readable message of an error response, if present.
pub fn error_message(fields: &ErrorFields) -> &str {
    fields.get(&b'M').map(String::as_str).unwrap_or("unknown")
}

/// One column of a `RowDescription` message.
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub type_oid: u32,
}

/// A decoded backend message.
#[derive(Debug)]
pub enum BackendEvent {
    RowDescription(Vec<Column>),
    DataRow(Vec<Option<String>>),
    CommandComplete(String),
    EmptyQueryResponse,
    ErrorResponse(ErrorFields),
    NoticeResponse(ErrorFields),
    /// Transaction status byte: `'I'` idle, `'T'` in transaction, `'E'` failed.
    ReadyForQuery(u8),
    /// A message this client does not interpret (ParameterStatus,
    /// BackendKeyData, notifications, ...), by tag.
    Other(u8),
}

/// Connection parameters, typically parsed from a DSN.
#[derive(Debug, Clone)]
pub struct ConnParams {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub database: String,
}

impl ConnParams {
    /// Parses a `postgres://[user[:password]@]host[:port][/database]` DSN.
    pub fn parse(dsn: &str) -> Result<ConnParams> {
        let rest = dsn
            .strip_prefix("postgres://")
            .or_else(|| dsn.strip_prefix("postgresql://"))
            .ok_or_else(|| Error::Dsn(format!("DSN must start with postgres://, got {dsn}")))?;
        let (authority, database) = match rest.split_once('/') {
            Some((a, db)) => (a, db.split('?').next().unwrap_or(db)),
            None => (rest, "postgres"),
        };
        let (userinfo, hostport) = match authority.rsplit_once('@') {
            Some((u, h)) => (Some(u), h),
            None => (None, authority),
        };
        let (user, password) = match userinfo {
            Some(u) => match u.split_once(':') {
                Some((user, pass)) => (user.to_string(), Some(pass.to_string())),
                None => (u.to_string(), None),
            },
            None => (
                std::env::var("USER").unwrap_or_else(|_| "postgres".to_string()),
                None,
            ),
        };
        let (host, port) = match hostport.rsplit_once(':') {
            Some((h, p)) => (
                h.to_string(),
                p.parse()
                    .map_err(|_| Error::Dsn(format!("bad port in DSN: {p}")))?,
            ),
            None => (hostport.to_string(), 5432),
        };
        Ok(ConnParams {
            host,
            port,
            user,
            password,
            database: database.to_string(),
        })
    }
}

/// A connection to a PostgreSQL-protocol server.
pub struct PgConn {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
}

impl PgConn {
    /// Connects and performs the startup handshake. `extra_startup` entries
    /// are sent as additional startup parameters after `user` and `database`
    /// (e.g. `application_name`, `datestyle`, or `options`).
    pub fn connect(params: &ConnParams, extra_startup: &[(&str, &str)]) -> Result<PgConn> {
        let stream = TcpStream::connect((params.host.as_str(), params.port))?;
        let reader = BufReader::new(stream.try_clone()?);
        let writer = BufWriter::new(stream);
        let mut conn = PgConn { reader, writer };

        let mut startup = Vec::new();
        startup.extend_from_slice(&196608u32.to_be_bytes()); // protocol 3.0
        let base: &[(&str, &str)] = &[
            ("user", params.user.as_str()),
            ("database", params.database.as_str()),
        ];
        for (k, v) in base.iter().chain(extra_startup) {
            startup.extend_from_slice(k.as_bytes());
            startup.push(0);
            startup.extend_from_slice(v.as_bytes());
            startup.push(0);
        }
        startup.push(0);
        conn.writer
            .write_all(&((startup.len() as u32 + 4).to_be_bytes()))?;
        conn.writer.write_all(&startup)?;
        conn.writer.flush()?;

        loop {
            let (tag, body) = conn.read_message()?;
            match tag {
                b'R' => {
                    let mut r = Reader::new(&body);
                    match r.u32()? {
                        0 => {} // AuthenticationOk
                        3 => {
                            // Cleartext password.
                            let password = params.password.as_deref().ok_or_else(|| {
                                Error::Rejected(
                                    "server requested a password but none was given".to_string(),
                                )
                            })?;
                            let mut msg = Vec::from(password.as_bytes());
                            msg.push(0);
                            conn.write_message(b'p', &msg)?;
                        }
                        method => {
                            return Err(Error::Protocol(format!(
                                "unsupported authentication method {method}"
                            )));
                        }
                    }
                }
                b'S' | b'K' | b'N' => {} // ParameterStatus, BackendKeyData, notices
                b'Z' => return Ok(conn),
                b'E' => {
                    let fields = parse_error_fields(&body)?;
                    return Err(Error::Rejected(error_message(&fields).to_string()));
                }
                other => {
                    return Err(Error::Protocol(format!(
                        "unexpected message {:?} during startup",
                        other as char
                    )));
                }
            }
        }
    }

    /// Sets a read timeout on the underlying socket. Blocking reads past the
    /// deadline fail with an I/O error, after which the connection's protocol
    /// state is indeterminate and it should be discarded.
    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> Result<()> {
        self.reader.get_ref().set_read_timeout(timeout)?;
        Ok(())
    }

    /// Sends one simple-query message. The string may contain multiple
    /// semicolon-separated statements; the server executes them in order.
    pub fn send_query(&mut self, sql: &str) -> Result<()> {
        let mut body = Vec::from(sql.as_bytes());
        body.push(0);
        self.write_message(b'Q', &body)
    }

    /// Reads one raw backend message frame.
    pub fn read_message(&mut self) -> Result<(u8, Vec<u8>)> {
        let mut header = [0u8; 5];
        self.reader.read_exact(&mut header).map_err(|e| {
            Error::Protocol(format!(
                "reading message header (server closed the connection?): {e}"
            ))
        })?;
        let len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
        if len < 4 {
            return Err(Error::Protocol(format!("invalid message length {len}")));
        }
        let mut body = vec![0u8; len - 4];
        self.reader
            .read_exact(&mut body)
            .map_err(|e| Error::Protocol(format!("reading message body: {e}")))?;
        Ok((header[0], body))
    }

    /// Reads and decodes one backend message.
    pub fn read_event(&mut self) -> Result<BackendEvent> {
        let (tag, body) = self.read_message()?;
        Ok(match tag {
            b'T' => BackendEvent::RowDescription(parse_row_description(&body)?),
            b'D' => BackendEvent::DataRow(parse_data_row(&body)?),
            b'C' => {
                let mut r = Reader::new(&body);
                BackendEvent::CommandComplete(r.cstring()?)
            }
            b'I' => BackendEvent::EmptyQueryResponse,
            b'E' => BackendEvent::ErrorResponse(parse_error_fields(&body)?),
            b'N' => BackendEvent::NoticeResponse(parse_error_fields(&body)?),
            b'Z' => {
                let mut r = Reader::new(&body);
                BackendEvent::ReadyForQuery(r.u8()?)
            }
            other => BackendEvent::Other(other),
        })
    }

    /// Sends `sql` as one simple query and collects every event up to and
    /// including `ReadyForQuery`.
    pub fn simple_query(&mut self, sql: &str) -> Result<Vec<BackendEvent>> {
        self.send_query(sql)?;
        let mut events = Vec::new();
        loop {
            let event = self.read_event()?;
            let done = matches!(event, BackendEvent::ReadyForQuery(_));
            events.push(event);
            if done {
                return Ok(events);
            }
        }
    }

    fn write_message(&mut self, tag: u8, body: &[u8]) -> Result<()> {
        self.writer.write_all(&[tag])?;
        self.writer
            .write_all(&((body.len() as u32 + 4).to_be_bytes()))?;
        self.writer.write_all(body)?;
        self.writer.flush()?;
        Ok(())
    }
}

/// Parses a `RowDescription` message body.
pub fn parse_row_description(body: &[u8]) -> Result<Vec<Column>> {
    let mut r = Reader::new(body);
    let nfields = r.u16()? as usize;
    let mut columns = Vec::with_capacity(nfields);
    for _ in 0..nfields {
        let name = r.cstring()?;
        r.skip(4 + 2)?; // table oid, attnum
        let type_oid = r.u32()?;
        r.skip(2 + 4 + 2)?; // typlen, typmod, format
        columns.push(Column { name, type_oid });
    }
    Ok(columns)
}

/// Parses a `DataRow` message body. `None` is SQL NULL; values are returned
/// as the text the server sent.
pub fn parse_data_row(body: &[u8]) -> Result<Vec<Option<String>>> {
    let mut r = Reader::new(body);
    let ncols = r.u16()? as usize;
    let mut row = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        let len = r.i32()?;
        if len < 0 {
            row.push(None);
        } else {
            let bytes = r.bytes(len as usize)?;
            row.push(Some(String::from_utf8_lossy(bytes).into_owned()));
        }
    }
    Ok(row)
}

/// Parses an `ErrorResponse` or `NoticeResponse` message body.
pub fn parse_error_fields(body: &[u8]) -> Result<ErrorFields> {
    let mut r = Reader::new(body);
    let mut fields = HashMap::new();
    loop {
        let code = r.u8()?;
        if code == 0 {
            return Ok(fields);
        }
        fields.insert(code, r.cstring()?);
    }
}

/// Cursor over a message body.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Reader<'a> {
        Reader { buf, pos: 0 }
    }

    fn bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).filter(|&e| e <= self.buf.len());
        let end = end.ok_or_else(|| Error::Protocol("truncated message".to_string()))?;
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn skip(&mut self, n: usize) -> Result<()> {
        self.bytes(n).map(|_| ())
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.bytes(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.bytes(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.bytes(4)?.try_into().unwrap()))
    }

    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.bytes(4)?.try_into().unwrap()))
    }

    fn cstring(&mut self) -> Result<String> {
        let start = self.pos;
        let nul = self.buf[start..]
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| Error::Protocol("unterminated string in message".to_string()))?;
        let s = String::from_utf8_lossy(&self.buf[start..start + nul]).into_owned();
        self.pos = start + nul + 1;
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_dsn_full() {
        let p = ConnParams::parse("postgres://alice:secret@db.example.com:5433/mydb").unwrap();
        assert_eq!(p.host, "db.example.com");
        assert_eq!(p.port, 5433);
        assert_eq!(p.user, "alice");
        assert_eq!(p.password.as_deref(), Some("secret"));
        assert_eq!(p.database, "mydb");
    }

    #[test]
    fn parse_dsn_minimal() {
        let p = ConnParams::parse("postgres://localhost").unwrap();
        assert_eq!(p.host, "localhost");
        assert_eq!(p.port, 5432);
        assert_eq!(p.database, "postgres");
        assert_eq!(p.password, None);
    }

    #[test]
    fn parse_dsn_query_string_ignored() {
        let p = ConnParams::parse("postgresql://h/db?sslmode=disable").unwrap();
        assert_eq!(p.host, "h");
        assert_eq!(p.database, "db");
    }

    #[test]
    fn parse_dsn_rejects_other_schemes() {
        assert!(ConnParams::parse("mysql://localhost").is_err());
    }

    #[test]
    fn parse_row_description_body() {
        // Two columns: "id" (int4, oid 23) and "name" (text, oid 25).
        let mut body = Vec::new();
        body.extend_from_slice(&2u16.to_be_bytes());
        for (name, oid) in [("id", 23u32), ("name", 25u32)] {
            body.extend_from_slice(name.as_bytes());
            body.push(0);
            body.extend_from_slice(&0u32.to_be_bytes()); // table oid
            body.extend_from_slice(&0u16.to_be_bytes()); // attnum
            body.extend_from_slice(&oid.to_be_bytes());
            body.extend_from_slice(&0u16.to_be_bytes()); // typlen
            body.extend_from_slice(&0u32.to_be_bytes()); // typmod
            body.extend_from_slice(&0u16.to_be_bytes()); // format
        }
        let cols = parse_row_description(&body).unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].type_oid, 23);
        assert_eq!(cols[1].name, "name");
        assert_eq!(cols[1].type_oid, 25);
    }

    #[test]
    fn parse_data_row_with_null() {
        let mut body = Vec::new();
        body.extend_from_slice(&2u16.to_be_bytes());
        body.extend_from_slice(&2i32.to_be_bytes());
        body.extend_from_slice(b"42");
        body.extend_from_slice(&(-1i32).to_be_bytes());
        let row = parse_data_row(&body).unwrap();
        assert_eq!(row, vec![Some("42".to_string()), None]);
    }

    #[test]
    fn parse_data_row_truncated() {
        let mut body = Vec::new();
        body.extend_from_slice(&1u16.to_be_bytes());
        body.extend_from_slice(&10i32.to_be_bytes());
        body.extend_from_slice(b"ab");
        assert!(parse_data_row(&body).is_err());
    }

    #[test]
    fn parse_error_fields_body() {
        let mut body = Vec::new();
        body.push(b'S');
        body.extend_from_slice(b"ERROR\0");
        body.push(b'M');
        body.extend_from_slice(b"relation \"t\" does not exist\0");
        body.push(0);
        let fields = parse_error_fields(&body).unwrap();
        assert_eq!(fields[&b'S'], "ERROR");
        assert_eq!(error_message(&fields), "relation \"t\" does not exist");
    }
}
