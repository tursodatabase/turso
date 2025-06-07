use crate::{Connection, LimboError, Statement, StepResult, Value};
use bitflags::bitflags;
use limbo_sqlite3_parser::ast::PragmaName;
use std::rc::{Rc, Weak};
use std::str::FromStr;

bitflags! {
    // Flag names match those used in SQLite:
    // https://github.com/sqlite/sqlite/blob/b3c1884b65400da85636458298bd77cbbfdfb401/tool/mkpragmatab.tcl#L22-L29
    struct PragmaFlags: u8 {
        const NeedSchema = 0x01;
        const NoColumns  = 0x02;
        const NoColumns1 = 0x04;
        const ReadOnly   = 0x08;
        const Result0    = 0x10;
        const Result1    = 0x20;
        const SchemaOpt  = 0x40;
        const SchemaReq  = 0x80;
    }
}

struct Pragma {
    flags: PragmaFlags,
    columns: &'static [&'static str],
}

impl Pragma {
    const fn new(flags: PragmaFlags, columns: &'static [&'static str]) -> Self {
        Self { flags, columns }
    }
}

fn pragma_for(pragma: PragmaName) -> Pragma {
    use PragmaName::*;

    match pragma {
        CacheSize => Pragma::new(
            PragmaFlags::NeedSchema
                | PragmaFlags::Result0
                | PragmaFlags::SchemaReq
                | PragmaFlags::NoColumns1,
            &["cache_size"],
        ),
        JournalMode => Pragma::new(
            PragmaFlags::NeedSchema | PragmaFlags::Result0 | PragmaFlags::SchemaReq,
            &["journal_mode"],
        ),
        LegacyFileFormat => {
            unreachable!("pragma_for() called with LegacyFileFormat, which is unsupported")
        }
        PageCount => Pragma::new(
            PragmaFlags::NeedSchema | PragmaFlags::Result0 | PragmaFlags::SchemaReq,
            &["page_count"],
        ),
        PageSize => Pragma::new(
            PragmaFlags::Result0 | PragmaFlags::SchemaReq | PragmaFlags::NoColumns1,
            &["page_size"],
        ),
        SchemaVersion => Pragma::new(
            PragmaFlags::NoColumns1 | PragmaFlags::Result0,
            &["schema_version"],
        ),
        TableInfo => Pragma::new(
            PragmaFlags::NeedSchema | PragmaFlags::Result1 | PragmaFlags::SchemaOpt,
            &["cid", "name", "type", "notnull", "dflt_value", "pk"],
        ),
        UserVersion => Pragma::new(
            PragmaFlags::NoColumns1 | PragmaFlags::Result0,
            &["user_version"],
        ),
        WalCheckpoint => Pragma::new(PragmaFlags::NeedSchema, &["busy", "log", "checkpointed"]),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PragmaVirtualTable {
    pragma_name: String,
    visible_column_count: usize,
    max_arg_count: usize,
    has_pragma_arg: bool,
}

impl PragmaVirtualTable {
    pub(crate) fn create(pragma_name: &str) -> crate::Result<(Self, String)> {
        if let Ok(pragma) = PragmaName::from_str(pragma_name) {
            if pragma == PragmaName::LegacyFileFormat {
                return Err(Self::no_such_pragma(pragma_name));
            }
            let pragma = pragma_for(pragma);
            if pragma
                .flags
                .intersects(PragmaFlags::Result0 | PragmaFlags::Result1)
            {
                let mut max_arg_count = 0;
                let mut has_pragma_arg = false;

                let mut sql = String::from("CREATE TABLE x(");
                let col_defs = pragma
                    .columns
                    .iter()
                    .map(|col| format!("\"{col}\""))
                    .collect::<Vec<_>>()
                    .join(", ");
                sql.push_str(&col_defs);
                if pragma.flags.contains(PragmaFlags::Result1) {
                    sql.push_str(", arg HIDDEN");
                    max_arg_count += 1;
                    has_pragma_arg = true;
                }
                if pragma
                    .flags
                    .intersects(PragmaFlags::SchemaOpt | PragmaFlags::SchemaReq)
                {
                    sql.push_str(", schema HIDDEN");
                    max_arg_count += 1;
                }
                sql.push(')');

                return Ok((
                    PragmaVirtualTable {
                        pragma_name: pragma_name.to_owned(),
                        visible_column_count: pragma.columns.len(),
                        max_arg_count,
                        has_pragma_arg,
                    },
                    sql,
                ));
            }
        }
        Err(Self::no_such_pragma(pragma_name))
    }

    fn no_such_pragma(pragma_name: &str) -> LimboError {
        LimboError::ParseError(format!(
            "No such table-valued function: pragma_{}",
            pragma_name
        ))
    }

    pub(crate) fn open(&self, conn: Weak<Connection>) -> crate::Result<PragmaVirtualTableCursor> {
        Ok(PragmaVirtualTableCursor {
            pragma_name: self.pragma_name.clone(),
            pos: 0,
            conn: conn
                .upgrade()
                .ok_or_else(|| LimboError::InternalError("Connection was dropped".into()))?,
            stmt: None,
            arg: None,
            visible_column_count: self.visible_column_count,
            max_arg_count: self.max_arg_count,
            has_pragma_arg: self.has_pragma_arg,
        })
    }
}

pub struct PragmaVirtualTableCursor {
    pragma_name: String,
    pos: usize,
    conn: Rc<Connection>,
    stmt: Option<Statement>,
    arg: Option<String>,
    visible_column_count: usize,
    max_arg_count: usize,
    has_pragma_arg: bool,
}

impl PragmaVirtualTableCursor {
    pub(crate) fn rowid(&self) -> i64 {
        self.pos as i64
    }

    pub(crate) fn next(&mut self) -> crate::Result<bool> {
        let stmt = self
            .stmt
            .as_mut()
            .ok_or_else(|| LimboError::InternalError("Statement is missing".into()))?;
        let result = stmt.step()?;
        match result {
            StepResult::Done => Ok(false),
            _ => {
                self.pos += 1;
                Ok(true)
            }
        }
    }

    pub(crate) fn column(&self, idx: usize) -> crate::Result<Value> {
        if idx < self.visible_column_count {
            let value = self
                .stmt
                .as_ref()
                .ok_or_else(|| LimboError::InternalError("Statement is missing".into()))?
                .row()
                .ok_or_else(|| LimboError::InternalError("No row available".into()))?
                .get_value(idx)
                .clone();
            return Ok(value);
        }

        let value = match idx - self.visible_column_count {
            0 => self
                .arg
                .as_ref()
                .map_or(Value::Null, |arg| Value::from_text(arg)),
            _ => Value::Null,
        };
        Ok(value)
    }

    pub(crate) fn filter(&mut self, args: Vec<Value>) -> crate::Result<bool> {
        if args.len() > self.max_arg_count {
            return Err(LimboError::ParseError(format!(
                "Too many arguments for pragma {}: expected at most {}, got {}",
                self.pragma_name,
                self.max_arg_count,
                args.len()
            )));
        }

        let to_text = |v: &Value| v.to_text().map(str::to_owned);
        let (arg, schema) = match args.as_slice() {
            [arg0] if self.has_pragma_arg => (to_text(arg0), None),
            [arg0] => (None, to_text(arg0)),
            [arg0, arg1] => (to_text(arg0), to_text(arg1)),
            _ => (None, None),
        };

        self.arg = arg;

        if let Some(schema) = schema {
            // Schema-qualified PRAGMA statements are not supported yet
            return Err(LimboError::ParseError(format!(
                "Schema argument is not supported yet (got schema: '{schema}')"
            )));
        }

        let mut sql = format!("PRAGMA {}", self.pragma_name);
        if let Some(arg) = &self.arg {
            sql.push_str(&format!("=\"{}\"", arg));
        }

        self.stmt = Some(self.conn.prepare(sql)?);

        self.next()
    }
}
