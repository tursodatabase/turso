use limbo_ext::{ResultCode, VTabCursor, VTabKind, VTabModule, VTabModuleDerive, Value};

use crate::json::Val;

use super::{filter, InPlaceJsonPath, ValExt as _, VecExt as _};

/// A virtual table that generates a sequence of integers
#[derive(Debug, VTabModuleDerive, Default)]
pub struct JsonEachVTab;

impl VTabModule for JsonEachVTab {
    type VCursor = JsonEachCursor;
    type Error = ResultCode;
    const NAME: &'static str = "json_each";
    const VTAB_KIND: VTabKind = VTabKind::TableValuedFunction;

    fn create_schema(_args: &[Value]) -> String {
        // Create table schema
        "CREATE TABLE json_each(
            key ANY,             -- key for current element relative to its parent
            value ANY,           -- value for the current element
            type TEXT,           -- 'object','array','string','integer', etc.
            atom ANY,            -- value for primitive types, null for array & object
            id INTEGER,          -- integer ID for this element
            parent INTEGER,      -- integer ID for the parent of this element
            fullkey TEXT,        -- full path describing the current element
            path TEXT,           -- path to the container of the current row
            json JSON HIDDEN,    -- 1st input parameter: the raw JSON
            root TEXT HIDDEN     -- 2nd input parameter: the PATH at which to start
        );"
        .into()
    }

    fn open(&self) -> Result<Self::VCursor, Self::Error> {
        Ok(JsonEachCursor::default())
    }

    fn filter(cursor: &mut Self::VCursor, args: &[Value]) -> ResultCode {
        let (json_val, items_advanced, path) = {
            match filter(args) {
                Ok(json_val) => json_val,
                Err(rc) => return rc,
            }
        };

        cursor.init(path, json_val, items_advanced);

        cursor.next()
    }

    fn column(cursor: &Self::VCursor, idx: u32) -> Result<Value, Self::Error> {
        cursor.column(idx)
    }

    fn next(cursor: &mut Self::VCursor) -> ResultCode {
        cursor.next()
    }

    fn eof(cursor: &Self::VCursor) -> bool {
        cursor.eof()
    }
}

/// The cursor for iterating over the generated sequence
#[derive(Debug)]
pub struct JsonEachCursor {
    rowid: i64,
    json_val: Val,  // Initial Val
    key: String,    // Current key
    val: Val,       // Current Json Val
    id: i64,        // Arbitrary id of the value,
    increment: i64, // Value to increment id
    eof: bool,
    array_idx_stack: Vec<usize>,
    path: String, // Requested Path
    curr_path: InPlaceJsonPath,
}

impl Default for JsonEachCursor {
    fn default() -> Self {
        Self {
            rowid: i64::default(),
            json_val: Val::Null,
            id: 0,
            increment: 1,
            key: "".to_string(),
            val: Val::Null,
            eof: false,
            array_idx_stack: Vec::new(),
            path: "".to_string(),
            curr_path: InPlaceJsonPath::default(),
        }
    }
}

impl JsonEachCursor {
    /// Initializes the cursor and necessary base cases
    fn init(&mut self, mut path: InPlaceJsonPath, json_val: Val, items_advanced: usize) {
        self.json_val = json_val;

        path.push("".to_string()); // Add base case so that code is cleaner in next
        self.path = path.path.clone();
        self.curr_path = path;

        // This increment shenanigan is done to be compatible with the id assignment in sqlite
        if matches!(self.json_val, Val::Array(_)) {
            self.increment = 1 + items_advanced as i64;
        } else if matches!(self.json_val, Val::Object(_)) {
            self.increment = 2 + items_advanced as i64;
        } else {
            self.increment = 0 + items_advanced as i64;
        }
    }
}

impl JsonEachCursor {
    fn push_new_val(&mut self, val: Val, key: String, is_array_locator: bool) {
        self.key = key;
        self.val = val;
        if !is_array_locator {
            self.curr_path.push_key(&self.key);
            self.increment = self.val.key_value_count() as i64 + 1;
        } else {
            self.curr_path.push_array_locator(&self.key);
            self.increment = self.val.key_value_count() as i64;
            if matches!(self.val, Val::Object(_)) {
                self.increment += 1;
            }
        }
    }
}

impl VTabCursor for JsonEachCursor {
    type Error = ResultCode;

    fn next(&mut self) -> ResultCode {
        if self.eof() {
            return ResultCode::EOF;
        }

        self.rowid += 1;
        self.id += self.increment;
        let _ = self.curr_path.pop(); // Pop to remove last element in path

        // TODO Improvement: see a way to first sort the elements so that we can pop from last instead of
        // remove_first and as the Vec shifts every time we remove_first
        match &mut self.json_val {
            Val::Array(v) => {
                if let Some(val) = v.remove_first() {
                    let key = {
                        if let Some(idx) = self.array_idx_stack.last_mut() {
                            *idx += 1;
                            idx.to_string()
                        } else {
                            self.array_idx_stack.push(0);
                            0.to_string()
                        }
                    };
                    self.push_new_val(val, key, true);
                } else {
                    let _ = self.array_idx_stack.pop();
                    self.eof = true;
                    return ResultCode::EOF;
                }
            }
            Val::Object(v) => {
                if let Some((key, val)) = v.remove_first() {
                    self.push_new_val(val, key, false);
                } else {
                    self.eof = true;
                    return ResultCode::EOF;
                }
            }
            Val::Removed => unreachable!(),
            _ => {
                // This means to return the self.json_val in column()
                // Doing this avoids a self.val = self.json_val.clone()
                self.increment = self.val.key_value_count() as i64;
                self.eof = true;
            }
        };

        ResultCode::OK
    }

    fn eof(&self) -> bool {
        self.eof
    }

    fn column(&self, idx: u32) -> Result<Value, Self::Error> {
        let ret_val = {
            if self.eof() {
                &self.json_val
            } else {
                &self.val
            }
        };

        let result = match idx {
            0 => Value::from_text(self.key.to_owned()), // Key
            1 => ret_val.to_value(),                    // Value
            2 => Value::from_text(ret_val.type_name()), // Type
            3 => ret_val.atom_value(),                  // Atom
            4 => Value::from_integer(self.id),          // Id
            5 => Value::null(),                         // Parent (null for json_each)
            6 => Value::from_text(self.curr_path.path.to_owned()), // FullKey
            7 => Value::from_text(self.path.clone()),   // Path
            _ => Value::null(),
        };
        Ok(result)
    }

    fn rowid(&self) -> i64 {
        self.rowid
    }
}

#[cfg(test)]
mod tests {}
