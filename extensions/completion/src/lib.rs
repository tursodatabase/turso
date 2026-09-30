//! Reference for implementation
//! https://github.com/sqlite/sqlite/blob/a80089c5167856f0aadc9c878bd65843df724c06/ext/misc/completion.c

mod keywords;

use std::sync::Arc;

use keywords::KEYWORDS;
use turso_ext::{
    register_extension, Connection, ConstraintInfo, ConstraintOp, ConstraintUsage, IndexInfo,
    OrderByInfo, ResultCode, VTabCursor, VTabModule, VTabModuleDerive, VTable, Value,
};

register_extension! {
    vtabs: { CompletionVTabModule }
}

macro_rules! extract_arg_text {
    ($args:expr, $idx:expr) => {
        $args
            .get($idx)
            .map(|v| v.to_text().unwrap_or(""))
            .unwrap_or("")
            .to_string()
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

impl From<CompletionPhase> for i64 {
    fn from(val: CompletionPhase) -> Self {
        use self::CompletionPhase::*;
        match val {
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
    const VTAB_KIND: turso_ext::VTabKind = turso_ext::VTabKind::TableValuedFunction;

    fn create(_args: &[Value]) -> Result<(String, Self::Table), ResultCode> {
        let schema = "CREATE TABLE completion (
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

    fn best_index(
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        // The bits of `idx_num` are used to indicate which arguments are available to the filter method:
        // - Bit 0 set -> 'prefix' is available
        // - Bit 1 set -> 'wholeline' is available
        let mut idx_num = 0;
        let mut prefix_idx = None;
        let mut wholeline_idx = None;

        for (i, c) in constraints.iter().enumerate() {
            if !c.usable || c.op != ConstraintOp::Eq {
                continue;
            }
            match c.column_index {
                1 => {
                    prefix_idx = Some(i);
                    idx_num |= 1;
                }
                2 => {
                    wholeline_idx = Some(i);
                    idx_num |= 2;
                }
                _ => {}
            }
        }

        let argv_prefix_idx = prefix_idx.map_or(0, |_| 1);
        let argv_wholeline_idx = wholeline_idx.map_or(argv_prefix_idx, |_| argv_prefix_idx + 1);

        let constraint_usages = constraints
            .iter()
            .enumerate()
            .map(|(i, _)| {
                let argv_index = if Some(i) == prefix_idx {
                    Some(argv_prefix_idx)
                } else if Some(i) == wholeline_idx {
                    Some(argv_wholeline_idx)
                } else {
                    None
                };
                ConstraintUsage {
                    argv_index,
                    omit: argv_index.is_some(),
                }
            })
            .collect();

        Ok(IndexInfo {
            idx_num,
            idx_str: Some(idx_num.to_string()),
            constraint_usages,
            ..Default::default()
        })
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

    fn filter(&mut self, args: &[Value], idx_info: Option<(&str, i32)>) -> ResultCode {
        self.reset();

        if let Some((_, idx_num)) = idx_info {
            let mut arg_idx = 0;
            // For the semantics of `idx_num`, see the comment in the `best_index` method.
            if idx_num & 1 != 0 {
                self.prefix = extract_arg_text!(args, arg_idx);
                arg_idx += 1;
            }
            if idx_num & 2 != 0 {
                self.line = extract_arg_text!(args, arg_idx);
            }
        }

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
mod tests {
    use super::*;

    #[test]
    fn test_best_index_argv_order_both_constraints() {
        // Test when both prefix and wholeline constraints are present
        let constraints = vec![
            usable_constraint(1), // prefix
            usable_constraint(2), // wholeline
        ];

        let index_info = CompletionTable::best_index(&constraints, &[]).unwrap();

        // Verify prefix gets argv_index 1 and wholeline gets argv_index 2
        assert_eq!(index_info.constraint_usages[0].argv_index, Some(1)); // prefix
        assert_eq!(index_info.constraint_usages[1].argv_index, Some(2)); // wholeline
        assert_eq!(index_info.idx_num, 3); // Both bits set (1 | 2)
    }

    #[test]
    fn test_best_index_argv_order_only_wholeline() {
        let constraints = vec![
            usable_constraint(2), // wholeline
        ];

        let index_info = CompletionTable::best_index(&constraints, &[]).unwrap();

        // Verify wholeline gets argv_index 1 when prefix is missing
        assert_eq!(index_info.constraint_usages[0].argv_index, Some(1)); // wholeline
        assert_eq!(index_info.idx_num, 2); // Only bit 1 set
    }

    #[test]
    fn test_best_index_argv_order_only_prefix() {
        let constraints = vec![
            usable_constraint(1), // prefix
        ];

        let index_info = CompletionTable::best_index(&constraints, &[]).unwrap();

        // Verify prefix gets argv_index 1
        assert_eq!(index_info.constraint_usages[0].argv_index, Some(1)); // prefix
        assert_eq!(index_info.idx_num, 1); // Only bit 0 set
    }

    #[test]
    fn test_best_index_argv_order_reverse_constraint_order() {
        // Test when constraints are provided in reverse order (wholeline first, then prefix)
        let constraints = vec![
            usable_constraint(2), // wholeline
            usable_constraint(1), // prefix
        ];

        let index_info = CompletionTable::best_index(&constraints, &[]).unwrap();

        // Verify prefix still gets argv_index 1 and wholeline gets argv_index 2 regardless of constraint order
        assert_eq!(index_info.constraint_usages[0].argv_index, Some(2)); // wholeline
        assert_eq!(index_info.constraint_usages[1].argv_index, Some(1)); // prefix
        assert_eq!(index_info.idx_num, 3); // Both bits set (1 | 2)
    }

    #[test]
    fn test_best_index_no_usable_constraints() {
        let constraints = vec![ConstraintInfo {
            column_index: 1,
            op: ConstraintOp::Eq,
            usable: false,
            index: 0,
        }];

        let index_info = CompletionTable::best_index(&constraints, &[]).unwrap();

        // Verify no argv_index is assigned
        assert_eq!(index_info.constraint_usages[0].argv_index, None);
        assert_eq!(index_info.idx_num, 0); // No bits set
    }

    fn usable_constraint(column_index: u32) -> ConstraintInfo {
        ConstraintInfo {
            column_index,
            op: ConstraintOp::Eq,
            usable: true,
            index: 0,
        }
    }
}
