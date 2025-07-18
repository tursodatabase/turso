use crate::{Connection, LimboError, Statement, StepResult, Value};
use bitflags::bitflags;
use std::sync::Arc;
use strum::IntoEnumIterator;
use turso_ext::{ConstraintInfo, ConstraintOp, ConstraintUsage, IndexInfo};
use turso_sqlite3_parser::ast::PragmaName;

bitflags! {
    // Flag names match those used in SQLite:
    // https://github.com/sqlite/sqlite/blob/b3c1884b65400da85636458298bd77cbbfdfb401/tool/mkpragmatab.tcl#L22-L29
    pub struct PragmaFlags: u8 {
        const NeedSchema = 0x01; /* Force schema load before running */
        const NoColumns  = 0x02; /* OP_ResultRow called with zero columns */
        const NoColumns1 = 0x04; /* zero columns if RHS argument is present */
        const ReadOnly   = 0x08; /* Read-only HEADER_VALUE */
        const Result0    = 0x10; /* Acts as query when no argument */
        const Result1    = 0x20; /* Acts as query when has one argument */
        const SchemaOpt  = 0x40; /* Schema restricts name search if present */
        const SchemaReq  = 0x80; /* Schema required - "main" is default */
    }
}

pub struct Pragma {
    pub flags: PragmaFlags,
    pub columns: &'static [&'static str],
}

impl Pragma {
    const fn new(flags: PragmaFlags, columns: &'static [&'static str]) -> Self {
        Self { flags, columns }
    }
}

pub fn pragma_for(pragma: &PragmaName) -> Pragma {
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
        AutoVacuum => Pragma::new(
            PragmaFlags::NoColumns1 | PragmaFlags::Result0,
            &["auto_vacuum"],
        ),
        IntegrityCheck => Pragma::new(
            PragmaFlags::NeedSchema | PragmaFlags::ReadOnly | PragmaFlags::Result0,
            &["message"],
        ),
        UnstableCaptureDataChangesConn => Pragma::new(
            PragmaFlags::NeedSchema | PragmaFlags::Result0 | PragmaFlags::SchemaReq,
            &["mode", "table"],
        ),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PragmaVirtualTable {
    pub(crate) pragma_name: String,
    visible_column_count: usize,
    max_arg_count: usize,
    has_pragma_arg: bool,
}

impl PragmaVirtualTable {
    pub(crate) fn functions() -> Vec<(PragmaVirtualTable, String)> {
        PragmaName::iter()
            .filter(|name| *name != PragmaName::LegacyFileFormat)
            .filter_map(|name| {
                let pragma = pragma_for(&name);
                if pragma
                    .flags
                    .intersects(PragmaFlags::Result0 | PragmaFlags::Result1)
                {
                    Some(Self::create(name.to_string(), pragma))
                } else {
                    None
                }
            })
            .collect()
    }

    fn create(pragma_name: String, pragma: Pragma) -> (Self, String) {
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

        (
            PragmaVirtualTable {
                pragma_name,
                visible_column_count: pragma.columns.len(),
                max_arg_count,
                has_pragma_arg,
            },
            sql,
        )
    }

    pub(crate) fn open(&self, conn: Arc<Connection>) -> crate::Result<PragmaVirtualTableCursor> {
        Ok(PragmaVirtualTableCursor {
            pragma_name: self.pragma_name.clone(),
            pos: 0,
            conn,
            stmt: None,
            arg: None,
            visible_column_count: self.visible_column_count,
            max_arg_count: self.max_arg_count,
            has_pragma_arg: self.has_pragma_arg,
        })
    }

    pub(crate) fn best_index(&self, constraints: &[ConstraintInfo]) -> IndexInfo {
        let mut arg0_idx = None;
        let mut arg1_idx = None;

        for (i, c) in constraints.iter().enumerate() {
            if !c.usable || c.op != ConstraintOp::Eq {
                continue;
            }
            let visible_count = self.visible_column_count as u32;
            if c.column_index < visible_count {
                continue;
            }
            let hidden_idx = c.column_index - visible_count;
            match hidden_idx {
                0 => arg0_idx = Some(i),
                1 => arg1_idx = Some(i),
                _ => unreachable!("Unexpected hidden column index: {}", hidden_idx),
            }
        }

        let mut argv_idx = 1;
        let constraint_usages = constraints
            .iter()
            .enumerate()
            .map(|(i, _)| {
                if Some(i) == arg0_idx || Some(i) == arg1_idx {
                    let usage = ConstraintUsage {
                        argv_index: Some(argv_idx),
                        omit: true,
                    };
                    argv_idx += 1;
                    usage
                } else {
                    ConstraintUsage {
                        argv_index: Some(0),
                        omit: false,
                    }
                }
            })
            .collect();

        IndexInfo {
            constraint_usages,
            ..Default::default()
        }
    }
}

pub struct PragmaVirtualTableCursor {
    pragma_name: String,
    pos: usize,
    conn: Arc<Connection>,
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
            sql.push_str(&format!("=\"{arg}\""));
        }

        self.stmt = Some(self.conn.prepare(sql)?);

        self.next()
    }
}
