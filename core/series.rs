use crate::sync::Arc;

use turso_ext::{
    Connection, ConstraintInfo, ConstraintOp, ConstraintUsage, ExtensionApi, IndexInfo,
    OrderByInfo, ResultCode, VTabCursor, VTabKind, VTabModule, VTabModuleDerive, VTable, Value,
    ValueType,
};

pub fn register_extension(ext_api: &mut ExtensionApi) {
    // FIXME: Add macro magic to register functions automatically.
    unsafe {
        GenerateSeriesVTabModule::register_GenerateSeriesVTabModule(ext_api);
    }
}

macro_rules! extract_arg_integer {
    ($args:expr, $idx:expr) => {
        $args.get($idx).and_then(|v| v.to_integer())
    };
}

/// A virtual table that generates a sequence of integers
#[derive(Debug, VTabModuleDerive, Default)]
struct GenerateSeriesVTabModule;

impl VTabModule for GenerateSeriesVTabModule {
    type Table = GenerateSeriesTable;
    const NAME: &'static str = "generate_series";
    const VTAB_KIND: VTabKind = VTabKind::TableValuedFunction;

    fn create(_args: &[Value]) -> Result<(String, Self::Table), ResultCode> {
        let schema = "CREATE TABLE generate_series (
            value INTEGER,
            start INTEGER HIDDEN,
            stop INTEGER HIDDEN,
            step INTEGER HIDDEN
        )"
        .into();
        Ok((schema, GenerateSeriesTable {}))
    }
}

struct GenerateSeriesTable {}

impl VTable for GenerateSeriesTable {
    type Cursor = GenerateSeriesCursor;
    type Error = ResultCode;

    fn open(&self, _conn: Option<Arc<Connection>>) -> Result<Self::Cursor, Self::Error> {
        Ok(GenerateSeriesCursor {
            start: 0,
            stop: 0,
            step: 0,
            current: 0,
        })
    }

    fn best_index(
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        const START_COLUMN_INDEX: u32 = 1;
        const STEP_COLUMN_INDEX: u32 = 3;

        // The bits of `idx_num` are used to indicate which arguments are available to the filter method:
        // - Bit 0 set -> 'start' is available
        // - Bit 1 set -> 'stop' is available
        // - Bit 2 set -> 'step' is available
        let mut idx_num = 0;
        let mut positions = [None; 4]; // maps column index to constraint position
        let mut start_exists = false;
        let mut usable = true;

        for (i, c) in constraints.iter().enumerate() {
            if c.column_index == START_COLUMN_INDEX && c.op == ConstraintOp::Eq {
                start_exists = true;
            }
            if c.column_index >= START_COLUMN_INDEX && c.column_index <= STEP_COLUMN_INDEX {
                if !c.usable {
                    usable = false;
                } else if c.op == ConstraintOp::Eq {
                    let bit = 1 << (c.column_index - 1);
                    idx_num |= bit;
                    positions[c.column_index as usize] = Some(i);
                }
            }
        }

        if !start_exists {
            return Err(ResultCode::InvalidArgs);
        }
        if !usable {
            return Err(ResultCode::ConstraintViolation);
        }

        // Assign argv indexes contiguously
        let mut argv_idx = 1;
        let mut argv_indexes = [None; 4];

        for (i, pos) in positions.iter().enumerate() {
            if pos.is_some() {
                argv_indexes[i] = Some(argv_idx);
                argv_idx += 1;
            }
        }

        let constraint_usages = constraints
            .iter()
            .enumerate()
            .map(|(idx, c)| {
                let argv_index = positions.get(c.column_index as usize).and_then(|&pos| {
                    pos.filter(|&i| i == idx)
                        .and_then(|_| argv_indexes[c.column_index as usize])
                });

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

/// The cursor for iterating over the generated sequence
#[derive(Debug)]
struct GenerateSeriesCursor {
    start: i64,
    stop: i64,
    step: i64,
    current: i64,
}

impl GenerateSeriesCursor {
    /// Returns true if this is an ascending series (positive step) but start > stop
    fn is_invalid_ascending_series(&self) -> bool {
        self.step > 0 && self.start > self.stop
    }

    /// Returns true if this is a descending series (negative step) but start < stop
    fn is_invalid_descending_series(&self) -> bool {
        self.step < 0 && self.start < self.stop
    }

    /// Returns true if this is an invalid range that should produce an empty series
    fn is_invalid_range(&self) -> bool {
        self.is_invalid_ascending_series() || self.is_invalid_descending_series()
    }

    /// Returns true if we would exceed the stop value in the current direction
    fn would_exceed(&self) -> bool {
        (self.step > 0 && self.current.saturating_add(self.step) > self.stop)
            || (self.step < 0 && self.current.saturating_add(self.step) < self.stop)
    }
}

impl VTabCursor for GenerateSeriesCursor {
    type Error = ResultCode;

    fn filter(&mut self, args: &[Value], idx_info: Option<(&str, i32)>) -> ResultCode {
        // SQLite returns no rows when any selected constraint has a NULL value.
        if args.iter().any(|arg| arg.value_type() == ValueType::Null) {
            return ResultCode::EOF;
        }
        let mut start: Option<i64> = None;
        let mut stop: Option<i64> = None;
        let mut step = 1;
        // SQLite default for stop when it is omitted
        const DEFAULT_STOP_OMITTED: Option<i64> = Some(u32::MAX as i64);

        if let Some((_, idx_num)) = idx_info {
            let mut arg_idx = 0;
            // For the semantics of `idx_num`, see the comment in the `best_index` method.
            if idx_num & 1 != 0 {
                start = extract_arg_integer!(args, arg_idx);
                arg_idx += 1;
            }
            if idx_num & 2 != 0 {
                stop = extract_arg_integer!(args, arg_idx);
                arg_idx += 1;
            } else {
                stop = DEFAULT_STOP_OMITTED;
            }
            if idx_num & 4 != 0 {
                step = args
                    .get(arg_idx)
                    .map(|v| v.to_integer().unwrap_or(1))
                    .unwrap_or(1);
            }
        }

        if start.is_none() {
            return ResultCode::InvalidArgs;
        }
        if stop.is_none() {
            return ResultCode::EOF; // Sqlite returns an empty series for wacky args
        }

        // Convert zero step to 1, matching SQLite behavior
        if step == 0 {
            step = 1;
        }

        self.start = start.unwrap();
        self.step = step;
        self.stop = stop.unwrap();

        // Set initial value based on range validity
        // For invalid input SQLite returns an empty series
        self.current = if self.is_invalid_range() {
            return ResultCode::EOF;
        } else {
            self.start
        };

        ResultCode::OK
    }

    fn next(&mut self) -> ResultCode {
        if self.eof() {
            return ResultCode::EOF;
        }

        self.current = match self.current.checked_add(self.step) {
            Some(val) => val,
            None => {
                return ResultCode::EOF;
            }
        };

        ResultCode::OK
    }

    fn eof(&self) -> bool {
        // Check for invalid ranges (empty series) first
        if self.is_invalid_range() {
            return true;
        }

        // Check if we would exceed the stop value in the current direction
        if self.would_exceed() {
            return true;
        }

        if self.current == i64::MAX && self.step > 0 {
            return true;
        }

        if self.current == i64::MIN && self.step < 0 {
            return true;
        }

        false
    }

    fn column(&self, idx: u32) -> Result<Value, Self::Error> {
        Ok(match idx {
            0 => Value::from_integer(self.current),
            1 => Value::from_integer(self.start),
            2 => Value::from_integer(self.stop),
            3 => Value::from_integer(self.step),
            _ => Value::null(),
        })
    }

    fn rowid(&self) -> i64 {
        // SQLite uses each generated value as its rowid. Thus, a value keeps
        // the same identity across scans that use different arguments.
        self.current
    }
}

#[cfg(test)]
#[path = "tests/unit/series/tests.rs"]
mod tests;
