//! Reference for implementation
//! https://github.com/sqlite/sqlite/blob/a80089c5167856f0aadc9c878bd65843df724c06/ext/misc/completion.c

mod keywords;

use std::sync::Arc;

use keywords::KEYWORDS;
use limbo_ext::{
    register_extension, Connection, ResultCode, VTabCursor, VTabModule, VTabModuleDerive, VTable,
    Value,
};

register_extension! {
    vtabs: { CompletionVTabModule }
}

macro_rules! try_option {
    ($expr:expr, $err:expr) => {
        match $expr {
            Some(val) => val,
            None => return $err,
        }
    };
}

#[derive(Debug, Default, PartialEq, Clone)]
enum CompletionPhase {
    #[default]
    Keywords = 1,
    // TODO other options now implemented for now
    // Pragmas = 2,
    // Functions = 3,
    // Collations = 4,
    // Indexes = 5,
    // Triggers = 6,
    // Databases = 7,
    // Tables = 8, // Also VIEWs and TRIGGERs
    // Columns = 9,
    // Modules = 10,
    Eof = 11,
}

impl Into<i64> for CompletionPhase {
    fn into(self) -> i64 {
        use self::CompletionPhase::*;
        match self {
            Keywords => 1,
            // Pragmas => 2,
            // Functions => 3,
            // Collations => 4,
            // Indexes => 5,
            // Triggers => 6,
            // Databases => 7,
            // Tables => 8,
            // Columns => 9,
            // Modules => 10,
            Eof => 11,
        }
    }
}

/// A virtual table that generates candidate completions
#[derive(Debug, Default, VTabModuleDerive)]
struct CompletionVTabModule {}

impl VTabModule for CompletionVTabModule {
    type Table = CompletionTable;
    const NAME: &'static str = "completion";
    const VTAB_KIND: limbo_ext::VTabKind = limbo_ext::VTabKind::TableValuedFunction;

    fn create(_args: &[Value]) -> Result<(String, Self::Table), ResultCode> {
        let schema = "CREATE TABLE completion(
            candidate TEXT,
            prefix TEXT HIDDEN,
            wholeline TEXT HIDDEN,
            phase INT HIDDEN     
        )"
        .to_string();
        Ok((schema, CompletionTable {}))
    }
}

struct CompletionTable {}

impl VTable for CompletionTable {
    type Cursor = CompletionCursor;
    type Error = ResultCode;

    fn open(&self, _conn: Option<Arc<Connection>>) -> Result<Self::Cursor, Self::Error> {
        Ok(CompletionCursor::default())
    }
}

/// The cursor for iterating over the completions
#[derive(Debug, Default)]
struct CompletionCursor {
    line: String,
    prefix: String,
    curr_row: String,
    rowid: i64,
    phase: CompletionPhase,
    inter_phase_counter: usize,
    // stmt: Statement
    // conn: Connection
}

impl CompletionCursor {
    fn reset(&mut self) {
        self.line.clear();
        self.prefix.clear();
        self.inter_phase_counter = 0;
    }
}

impl VTabCursor for CompletionCursor {
    type Error = ResultCode;

    fn filter(&mut self, args: &[Value], _: Option<(&str, i32)>) -> ResultCode {
        if args.is_empty() || args.len() > 2 {
            return ResultCode::InvalidArgs;
        }
        self.reset();
        let prefix = try_option!(args[0].to_text(), ResultCode::InvalidArgs);

        let wholeline = args.get(1).map(|v| v.to_text().unwrap_or("")).unwrap_or("");

        self.line = wholeline.to_string();
        self.prefix = prefix.to_string();

        // Currently best index is not implemented so the correct arg parsing is not done here
        if !self.line.is_empty() && self.prefix.is_empty() {
            let mut i = self.line.len();
            while let Some(ch) = self.line.chars().next() {
                if i > 0 && (ch.is_alphanumeric() || ch == '_') {
                    i -= 1;
                } else {
                    break;
                }
            }
            if self.line.len() - i > 0 {
                // TODO see if need to inclusive range
                self.prefix = self.line[..i].to_string();
            }
        }

        self.rowid = 0;
        self.phase = CompletionPhase::Keywords;

        self.next()
    }

    fn next(&mut self) -> ResultCode {
        self.rowid += 1;

        while self.phase != CompletionPhase::Eof {
            // dbg!(&self.phase, &self.prefix, &self.curr_row);
            match self.phase {
                CompletionPhase::Keywords => {
                    if self.inter_phase_counter >= KEYWORDS.len() {
                        self.curr_row.clear();
                        self.phase = CompletionPhase::Eof;
                    } else {
                        self.curr_row.clear();
                        self.curr_row.push_str(KEYWORDS[self.inter_phase_counter]);
                        self.inter_phase_counter += 1;
                    }
                }
                // TODO implement this when db conn is available
                // CompletionPhase::Databases => {
                //
                //     // self.stmt = self.conn.prepare("PRAGMA database_list")
                //     curr_col = 1;
                //     next_phase = CompletionPhase::Tables;
                //     self.phase = CompletionPhase::Eof; // for now skip other phases
                // }
                _ => {
                    return ResultCode::EOF;
                }
            }
            if self.prefix.is_empty() {
                break;
            }
            if self.prefix.len() <= self.curr_row.len()
                && self.prefix.to_lowercase() == self.curr_row.to_lowercase()[..self.prefix.len()]
            {
                break;
            }
        }
        if self.phase == CompletionPhase::Eof {
            return ResultCode::EOF;
        }
        ResultCode::OK
    }

    fn eof(&self) -> bool {
        self.phase == CompletionPhase::Eof
    }

    fn column(&self, idx: u32) -> Result<Value, Self::Error> {
        let val = match idx {
            0 => Value::from_text(self.curr_row.clone()), // COMPLETION_COLUMN_CANDIDATE
            1 => Value::from_text(self.prefix.clone()),   // COMPLETION_COLUMN_PREFIX
            2 => Value::from_text(self.line.clone()),     // COMPLETION_COLUMN_WHOLELINE
            3 => Value::from_integer(self.phase.clone().into()), // COMPLETION_COLUMN_PHASE
            _ => Value::null(),
        };
        Ok(val)
    }

    fn rowid(&self) -> i64 {
        self.rowid
    }
}

#[cfg(test)]
mod tests {}
