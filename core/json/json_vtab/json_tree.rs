use std::cell::UnsafeCell;

use limbo_ext::{ResultCode, VTabCursor, VTabKind, VTabModule, VTabModuleDerive, Value};

use crate::json::Val;

use super::{filter, InPlaceJsonPath, ValExt as _, VecExt as _};

/// A virtual table that generates a sequence of integers
#[derive(Debug, VTabModuleDerive, Default)]
pub struct JsonTreeVTab;

impl VTabModule for JsonTreeVTab {
    type VCursor = JsonTreeCursor;
    type Error = ResultCode;
    const NAME: &'static str = "json_tree";
    const VTAB_KIND: VTabKind = VTabKind::TableValuedFunction;

    fn create_schema(_args: &[Value]) -> String {
        // Create table schema
        "CREATE TABLE json_tree(
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
        Ok(JsonTreeCursor::default())
    }

    fn filter(cursor: &mut Self::VCursor, args: &[Value]) -> ResultCode {
        let (json_val, mut path) = {
            match filter(args) {
                Ok(json_val) => json_val,
                Err(rc) => return rc,
            }
        };

        // TODO create a base case init function to have all basecases in one place

        cursor.start = UnsafeCell::new(true);

        cursor.val_stack = vec![json_val.clone(), Val::Null]; // Add Val::Null for base case for next
        cursor.json_val = json_val;

        path.push("".to_string()); // Add base case so that code is cleaner in next
        cursor.full_key = path;

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
pub struct JsonTreeCursor {
    rowid: i64,
    json_val: Val, // Initial Val
    key: String,   // Current key
    val_stack: Vec<Val>,
    id: i64, // Arbitrary id of the value,
    parent_stack: Vec<i64>,
    eof: bool,
    array_idx_stack: Vec<isize>,
    full_key: InPlaceJsonPath, // Requested Path that reflects the path to the current container + the curr key
    start: UnsafeCell<bool>,
    new_container: bool,
}

impl Default for JsonTreeCursor {
    fn default() -> Self {
        Self {
            rowid: i64::default(),
            json_val: Val::Null,
            id: -1,
            parent_stack: vec![0],
            key: "".to_string(),
            val_stack: Vec::new(),
            eof: false,
            array_idx_stack: Vec::new(),
            full_key: InPlaceJsonPath::default(),
            start: UnsafeCell::new(false),
            new_container: false,
        }
    }
}

impl JsonTreeCursor {
    fn check_empty_container_or_atom(&mut self) {
        if let Some(v) = self.val_stack.last_mut() {
            let mut pop = false;
            match v {
                Val::Array(v) => {
                    if v.is_empty() {
                        pop = true;
                    }
                }
                Val::Object(v) => {
                    if v.is_empty() {
                        pop = true;
                    }
                }
                Val::Removed => unreachable!(),
                _ => {
                    pop = true;
                }
            }
            if pop {
                // Pop the value as it has already been returned in column or the object or array is empty
                let _ = self.val_stack.pop();
                let _ = self.full_key.pop();
            }
        }
    }
}

impl VTabCursor for JsonTreeCursor {
    type Error = ResultCode;

    fn next(&mut self) -> ResultCode {
        if self.eof() {
            return ResultCode::EOF;
        }

        self.rowid += 1;
        self.id += 1;
        if unsafe { *self.start.get() } {
            if !matches!(self.json_val, Val::Array(_) | Val::Object(_)) {
                self.eof = true;
                let _ = self.full_key.pop();
            }
            return ResultCode::OK;
        }

        self.check_empty_container_or_atom();

        if self.new_container {
            self.parent_stack.push(self.id - 1);
            self.new_container = false;
        }

        loop {
            let curr_val = {
                match self.val_stack.last_mut() {
                    Some(curr_val) => curr_val,
                    None => {
                        self.eof = true;
                        return ResultCode::EOF;
                    }
                }
            };
            // TODO Improvement: see a way to first sort the elements so that we can pop from last instead of
            // remove_first and as the Vec shifts every time we remove_first
            match curr_val {
                Val::Array(v) => {
                    if let Some(val) = v.remove_first() {
                        self.key = {
                            if let Some(idx) = self.array_idx_stack.last_mut() {
                                *idx += 1;
                                idx.to_string()
                            } else {
                                self.array_idx_stack.push(0);
                                0.to_string()
                            }
                        };
                        if matches!(val, Val::Array(_)) {
                            // -1 here as the index is incremented in the later calls
                            self.array_idx_stack.push(-1);
                        }
                        if matches!(val, Val::Array(_) | Val::Object(_)) {
                            self.new_container = true;
                        }

                        self.full_key.push_array_locator(&self.key);
                        self.val_stack.push(val);
                        break;
                    } else {
                        let _ = self.array_idx_stack.pop();
                        let _ = self.full_key.pop();
                        let _ = self.val_stack.pop();
                        let _ = self.parent_stack.pop();
                    }
                }
                Val::Object(v) => {
                    if let Some((key, val)) = v.remove_first() {
                        if matches!(val, Val::Array(_)) {
                            self.array_idx_stack.push(-1);
                        }
                        if matches!(val, Val::Array(_) | Val::Object(_)) {
                            self.new_container = true;
                        }

                        self.key = key;
                        self.val_stack.push(val);
                        self.full_key.push_key(&self.key);
                        break;
                    } else {
                        let _ = self.full_key.pop();
                        let _ = self.val_stack.pop();
                        let _ = self.parent_stack.pop();
                    }
                }
                Val::Removed => unreachable!(),
                _ => {
                    break;
                }
            };
        }

        ResultCode::OK
    }

    fn eof(&self) -> bool {
        self.eof || self.val_stack.is_empty()
    }

    fn column(&self, idx: u32) -> Result<Value, Self::Error> {
        let start = unsafe { &mut *self.start.get() };
        let ret_val = {
            if *start {
                &self.json_val
            } else {
                &self.val_stack.last().unwrap() // Should never error here
            }
        };

        let parent = {
            if *start {
                Value::null()
            } else {
                Value::from_integer(self.parent_stack.last().unwrap().clone())
            }
        };

        let result = match idx {
            0 => Value::from_text(self.key.to_owned()), // Key
            1 => ret_val.to_value(),                    // Value
            2 => Value::from_text(ret_val.type_name()), // Type
            3 => ret_val.atom_value(),                  // Atom
            4 => Value::from_integer(self.id),          // Id
            5 => parent,                                // Parent (null for json_each)
            6 => Value::from_text(self.full_key.path.to_owned()), // FullKey
            7 => {
                let mut path = self.full_key.clone();

                let path = {
                    if *start && matches!(ret_val, Val::Array(_) | Val::Object(_)) {
                        path
                    } else {
                        let _ = path.pop();
                        path
                    }
                };
                *start = false;
                Value::from_text(path.path)
            } // Path
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
