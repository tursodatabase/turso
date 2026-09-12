//! Port of SQLite's CSV virtual table extension: <https://www.sqlite.org/csv.html>
//!
//! This extension allows querying CSV files as if they were database tables,
//! using the virtual table mechanism.
//!
//! It supports specifying the CSV input via a filename or raw data string, optional headers,
//! and customizable schema generation.
//!
//! ## Example usage:
//!
//! ```sql
//! CREATE VIRTUAL TABLE temp.my_csv USING csv(filename='data.csv', header=yes);
//! SELECT * FROM my_csv;
//! ```
//!
//! ## Parameters:
//! - `filename` — path to the CSV file (mutually exclusive with `data=`)
//! - `data` — inline CSV content as a string
//! - `header` — whether the first row contains column names;
//!   accepts `yes`/`no`, `on`/`off`, `true`/`false`, or `1`/`0`
//! - `columns` — number of columns
//! - `schema` — optional custom SQL `CREATE TABLE` schema
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
use turso_ext::{
    register_extension, Connection, ResultCode, VTabCursor, VTabKind, VTabModule, VTabModuleDerive,
    VTable, Value,
};

register_extension! {
    vtabs: { CsvVTabModule }
}

#[derive(Debug, VTabModuleDerive, Default)]
struct CsvVTabModule;

impl CsvVTabModule {
    fn parse_arg(arg: &Value) -> Result<(&str, &str), ResultCode> {
        if let Some(text) = arg.to_text() {
            let mut split = text.splitn(2, '=');
            if let Some(name) = split.next() {
                if let Some(value) = split.next() {
                    let name = name.trim();
                    let value = value.trim();
                    return Ok((name, value));
                }
            }
        }
        Err(ResultCode::InvalidArgs)
    }

    fn parse_string(s: &str) -> Result<String, ResultCode> {
        let chars: Vec<char> = s.chars().collect();
        let len = chars.len();

        if len >= 2 && (chars[0] == '"' || chars[0] == '\'') {
            let quote = chars[0];

            if quote != chars[len - 1] {
                return Err(ResultCode::InvalidArgs);
            }

            let mut result = String::new();
            let mut i = 1;

            while i < len - 1 {
                if chars[i] == quote && i + 1 < len - 1 && chars[i + 1] == quote {
                    // Escaped quote ("" or '')
                    result.push(quote);
                    i += 2;
                } else {
                    result.push(chars[i]);
                    i += 1;
                }
            }

            Ok(result)
        } else {
            Ok(s.to_owned())
        }
    }

    fn parse_boolean(s: &str) -> Option<bool> {
        if s.eq_ignore_ascii_case("yes")
            || s.eq_ignore_ascii_case("on")
            || s.eq_ignore_ascii_case("true")
            || s.eq("1")
        {
            Some(true)
        } else if s.eq_ignore_ascii_case("no")
            || s.eq_ignore_ascii_case("off")
            || s.eq_ignore_ascii_case("false")
            || s.eq("0")
        {
            Some(false)
        } else {
            None
        }
    }

    fn escape_double_quote(identifier: &str) -> String {
        identifier.replace('"', "\"\"")
    }
}

impl VTabModule for CsvVTabModule {
    type Table = CsvTable;
    const VTAB_KIND: VTabKind = VTabKind::VirtualTable;
    const NAME: &'static str = "csv";
    const READONLY: bool = true;

    fn create(args: &[Value]) -> Result<(String, Self::Table), ResultCode> {
        if args.is_empty() {
            return Err(ResultCode::InvalidArgs);
        }

        let mut filename = None;
        let mut data = None;
        let mut schema = None;
        let mut column_count = None;
        let mut header = None;

        for arg in args {
            let (name, value) = Self::parse_arg(arg)?;
            match name {
                "filename" => {
                    if filename.is_some() {
                        return Err(ResultCode::InvalidArgs);
                    }
                    filename = Some(Self::parse_string(value)?);
                }
                "data" => {
                    if data.is_some() {
                        return Err(ResultCode::InvalidArgs);
                    }
                    data = Some(Self::parse_string(value)?);
                }
                "schema" => {
                    if schema.is_some() {
                        return Err(ResultCode::InvalidArgs);
                    }
                    schema = Some(Self::parse_string(value)?);
                }
                "columns" => {
                    if column_count.is_some() {
                        return Err(ResultCode::InvalidArgs);
                    }
                    let n: u32 = value.parse().map_err(|_| ResultCode::InvalidArgs)?;
                    if n == 0 {
                        return Err(ResultCode::InvalidArgs);
                    }
                    column_count = Some(n);
                }
                "header" => {
                    if header.is_some() {
                        return Err(ResultCode::InvalidArgs);
                    }
                    header = Some(Self::parse_boolean(value).ok_or(ResultCode::InvalidArgs)?);
                }
                _ => {
                    return Err(ResultCode::InvalidArgs);
                }
            }
        }

        if filename.is_some() == data.is_some() {
            return Err(ResultCode::InvalidArgs);
        }

        let mut columns: Vec<String> = Vec::new();

        let mut table = CsvTable {
            column_count,
            filename,
            data,
            header: header.unwrap_or(false),
            first_row_position: csv::Position::new(),
        };

        if table.header || (column_count.is_none() && schema.is_none()) {
            let mut reader = table.new_reader()?;
            if table.header {
                let headers = reader.headers().map_err(|_| ResultCode::Error)?;
                if column_count.is_none() && schema.is_none() {
                    columns = headers.into_iter().map(Self::escape_double_quote).collect();
                }
                if columns.is_empty() {
                    columns.push("(NULL)".to_owned());
                }
                table.first_row_position = reader.position().clone();
            } else {
                let mut record = csv::ByteRecord::new();
                if reader
                    .read_byte_record(&mut record)
                    .map_err(|_| ResultCode::Error)?
                {
                    for (i, _) in record.iter().enumerate() {
                        columns.push(format!("c{i}"));
                    }
                }
                if columns.is_empty() {
                    columns.push("c0".to_owned());
                }
            }
        } else if let Some(count) = column_count {
            for i in 0..count {
                columns.push(format!("c{i}"));
            }
        }

        if schema.is_none() {
            let mut sql = String::from("CREATE TABLE x (");
            for (i, col) in columns.iter().enumerate() {
                sql.push('"');
                sql.push_str(col);
                sql.push_str("\" TEXT");
                if i < columns.len() - 1 {
                    sql.push_str(", ");
                }
            }
            sql.push(')');
            schema = Some(sql);
        }

        Ok((schema.ok_or(ResultCode::Error)?, table))
    }
}

struct CsvTable {
    filename: Option<String>,
    data: Option<String>,
    header: bool,
    column_count: Option<u32>,
    first_row_position: csv::Position,
}

impl CsvTable {
    fn new_reader(&self) -> Result<csv::Reader<ReadSource>, ResultCode> {
        let mut builder = csv::ReaderBuilder::new();
        builder.has_headers(self.header).delimiter(b',').quote(b'"');

        match (&self.filename, &self.data) {
            (Some(path), None) => {
                let file = File::open(path).map_err(|_| ResultCode::Error)?;
                Ok(builder.from_reader(ReadSource::File(file)))
            }
            (None, Some(data)) => {
                let cursor = std::io::Cursor::new(data.clone().into_bytes());
                Ok(builder.from_reader(ReadSource::Memory(cursor)))
            }
            _ => Err(ResultCode::Internal),
        }
    }
}

impl VTable for CsvTable {
    type Cursor = CsvCursor;
    type Error = ResultCode;

    fn open(&self, _conn: Option<Arc<Connection>>) -> Result<Self::Cursor, Self::Error> {
        match self.new_reader() {
            Ok(reader) => Ok(CsvCursor::new(reader, self)),
            Err(_) => Err(ResultCode::Error),
        }
    }
}

enum ReadSource {
    File(File),
    Memory(std::io::Cursor<Vec<u8>>),
}

impl Read for ReadSource {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            ReadSource::File(f) => f.read(buf),
            ReadSource::Memory(c) => c.read(buf),
        }
    }
}

impl Seek for ReadSource {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        match self {
            ReadSource::File(f) => f.seek(pos),
            ReadSource::Memory(c) => c.seek(pos),
        }
    }
}

struct CsvCursor {
    column_count: Option<u32>,
    reader: csv::Reader<ReadSource>,
    row_number: usize,
    current_row: csv::StringRecord,
    eof: bool,
    first_row_position: csv::Position,
}

impl CsvCursor {
    fn new(reader: csv::Reader<ReadSource>, table: &CsvTable) -> Self {
        CsvCursor {
            column_count: table.column_count,
            reader,
            row_number: 0,
            current_row: csv::StringRecord::new(),
            eof: false,
            first_row_position: table.first_row_position.clone(),
        }
    }
}

impl VTabCursor for CsvCursor {
    type Error = ResultCode;

    fn filter(&mut self, _args: &[Value], _idx_info: Option<(&str, i32)>) -> ResultCode {
        let offset_first_row = self.first_row_position.clone();
        if self.reader.seek(offset_first_row).is_err() {
            return ResultCode::Error;
        };
        self.row_number = 0;
        self.next()
    }

    fn rowid(&self) -> i64 {
        self.row_number as i64
    }

    fn column(&self, idx: u32) -> Result<Value, Self::Error> {
        if let Some(count) = self.column_count {
            if idx >= count {
                return Ok(Value::null());
            }
        }
        let value = self
            .current_row
            .get(idx as usize)
            .map_or(Value::null(), |s| Value::from_text(s.to_owned()));
        Ok(value)
    }

    fn eof(&self) -> bool {
        self.eof
    }

    fn next(&mut self) -> ResultCode {
        {
            self.eof = self.reader.is_done();
            if self.eof {
                return ResultCode::EOF;
            }

            match self.reader.read_record(&mut self.current_row) {
                Ok(more) => {
                    self.eof = !more;
                    if self.eof {
                        return ResultCode::EOF;
                    }
                }
                Err(_) => return ResultCode::Error,
            }
        }

        self.row_number += 1;
        ResultCode::OK
    }
}

#[cfg(test)]
#[path = "../tests/unit/tests.rs"]
mod tests;
