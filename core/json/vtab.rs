use std::{cell::RefCell, result::Result, sync::Arc};

use turso_ext::{ConstraintOp, ConstraintUsage, ResultCode};

use crate::{
    json::{
        convert_dbtype_to_jsonb, json_path_from_db_value,
        jsonb::{ArrayIteratorState, Jsonb, ObjectIteratorState, SearchOperation},
        vtab::columns::Columns,
        Conv,
    },
    types::Text,
    vtab::{InternalVirtualTable, InternalVirtualTableCursor},
    Connection, LimboError, Value,
};

use super::jsonb;

pub struct JsonEachVirtualTable;

const COL_KEY: usize = 0;
const COL_VALUE: usize = 1;
const COL_TYPE: usize = 2;
const COL_ATOM: usize = 3;
const COL_ID: usize = 4;
const COL_PARENT: usize = 5;
const COL_FULLKEY: usize = 6;
const COL_PATH: usize = 7;
const COL_JSON: usize = 8;
const COL_ROOT: usize = 9;

impl InternalVirtualTable for JsonEachVirtualTable {
    fn name(&self) -> String {
        "json_each".to_owned()
    }

    fn open(
        &self,
        _conn: Arc<Connection>,
    ) -> crate::Result<std::sync::Arc<RefCell<(dyn InternalVirtualTableCursor + 'static)>>> {
        Ok(Arc::new(RefCell::new(JsonEachCursor::default())))
    }

    fn best_index(
        &self,
        constraints: &[turso_ext::ConstraintInfo],
        _order_by: &[turso_ext::OrderByInfo],
    ) -> Result<turso_ext::IndexInfo, ResultCode> {
        let mut usages = vec![
            ConstraintUsage {
                argv_index: None,
                omit: false
            };
            constraints.len()
        ];

        let mut json_idx: Option<usize> = None;
        let mut path_idx: Option<usize> = None;
        for (i, c) in constraints.iter().enumerate() {
            if !c.usable || c.op != ConstraintOp::Eq {
                continue;
            }
            match c.column_index as usize {
                COL_JSON => json_idx = Some(i),
                COL_ROOT => path_idx = Some(i),
                _ => {}
            }
        }

        let argc = match (json_idx, path_idx) {
            (Some(_), Some(_)) => 2,
            (Some(_), None) => 1,
            _ => 0,
        };

        if argc >= 1 {
            usages[json_idx.unwrap()] = ConstraintUsage {
                argv_index: Some(1),
                omit: true,
            };
        }
        if argc == 2 {
            usages[path_idx.unwrap()] = ConstraintUsage {
                argv_index: Some(2),
                omit: true,
            };
        }

        let (cost, rows) = match argc {
            1 => (1., 25),
            2 => (1., 25),
            _ => (f64::MAX, 25),
        };

        Ok(turso_ext::IndexInfo {
            idx_num: -1,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: cost,
            estimated_rows: rows,
            constraint_usages: usages,
        })
    }

    fn sql(&self) -> String {
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
        .to_owned()
    }
}

impl std::fmt::Debug for JsonEachVirtualTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonEachVirtualTable").finish()
    }
}

enum IteratorState {
    Array(ArrayIteratorState),
    Object(ObjectIteratorState),
    Primitive,
    None,
}

pub struct JsonEachCursor {
    rowid: i64,
    no_more_rows: bool,
    json: Jsonb,
    root_path: Option<String>,
    iterator_state: IteratorState,
    columns: Columns,
}

impl Default for JsonEachCursor {
    fn default() -> Self {
        Self {
            rowid: 0,
            no_more_rows: false,
            json: Jsonb::new(0, None),
            root_path: None,
            iterator_state: IteratorState::None,
            columns: Columns::default(),
        }
    }
}

impl InternalVirtualTableCursor for JsonEachCursor {
    fn filter(
        &mut self,
        args: &[Value],
        _idx_str: Option<String>,
        _idx_num: i32,
    ) -> Result<bool, LimboError> {
        if args.is_empty() {
            return Ok(false);
        }
        if args.len() != 1 && args.len() != 2 {
            return Err(LimboError::InvalidArgument(
                "json_each accepts 1 or 2 arguments".to_owned(),
            ));
        }

        let mut jsonb = convert_dbtype_to_jsonb(&args[0], Conv::Strict)?;
        if args.len() == 1 {
            self.json = jsonb;
        } else if args.len() == 2 {
            let Value::Text(root_path) = &args[1] else {
                return Err(LimboError::InvalidArgument(
                    "root path should be text".to_owned(),
                ));
            };
            self.root_path = Some(root_path.as_str().to_owned());
            self.json = if let Some(json) = navigate_to_path(&mut jsonb, &args[1])? {
                json
            } else {
                return Ok(false);
            };
        }
        let json_element_type = self.json.element_type()?;

        match json_element_type {
            jsonb::ElementType::ARRAY => {
                let iter = self.json.array_iterator()?;
                self.iterator_state = IteratorState::Array(iter);
            }
            jsonb::ElementType::OBJECT => {
                let iter = self.json.object_iterator()?;
                self.iterator_state = IteratorState::Object(iter);
            }
            jsonb::ElementType::NULL
            | jsonb::ElementType::TRUE
            | jsonb::ElementType::FALSE
            | jsonb::ElementType::INT
            | jsonb::ElementType::INT5
            | jsonb::ElementType::FLOAT
            | jsonb::ElementType::FLOAT5
            | jsonb::ElementType::TEXT
            | jsonb::ElementType::TEXT5
            | jsonb::ElementType::TEXTJ
            | jsonb::ElementType::TEXTRAW => {
                self.iterator_state = IteratorState::Primitive;
            }
            jsonb::ElementType::RESERVED1
            | jsonb::ElementType::RESERVED2
            | jsonb::ElementType::RESERVED3 => {
                unreachable!("element type not supported: {json_element_type:?}");
            }
        };

        self.next()
    }

    fn next(&mut self) -> Result<bool, LimboError> {
        self.rowid += 1;
        if self.no_more_rows {
            return Ok(false);
        }

        match &self.iterator_state {
            IteratorState::Array(state) => {
                let Some(((idx, jsonb), new_state)) = self.json.array_iterator_next(state) else {
                    self.no_more_rows = true;
                    return Ok(false);
                };
                self.iterator_state = IteratorState::Array(new_state);
                self.columns = Columns::new(
                    columns::Key::Integer(idx as i64),
                    jsonb,
                    self.root_path.clone(),
                );
            }
            IteratorState::Object(state) => {
                let Some(((_idx, key, value), new_state)): Option<(
                    (usize, Jsonb, Jsonb),
                    ObjectIteratorState,
                )> = self.json.object_iterator_next(state) else {
                    self.no_more_rows = true;
                    return Ok(false);
                };

                self.iterator_state = IteratorState::Object(new_state);
                let key = key.to_string();
                self.columns =
                    Columns::new(columns::Key::String(key), value, self.root_path.clone());
            }
            IteratorState::Primitive => {
                let json = std::mem::replace(&mut self.json, Jsonb::new(0, None));
                self.columns = Columns::new_from_primitive(json, self.root_path.clone());
                self.no_more_rows = true;
            }
            IteratorState::None => unreachable!(),
        };

        Ok(true)
    }

    fn rowid(&self) -> i64 {
        self.rowid
    }

    fn column(&self, idx: usize) -> Result<Value, LimboError> {
        Ok(match idx {
            COL_KEY => self.columns.key(),
            COL_VALUE => self.columns.value()?,
            COL_TYPE => self.columns.ttype(),
            COL_ATOM => self.columns.atom()?,
            COL_ID => Value::Integer(self.rowid),
            COL_PARENT => self.columns.parent(),
            COL_FULLKEY => self.columns.fullkey(),
            COL_PATH => self.columns.path(),
            COL_ROOT => Value::Text(Text::new("json, todo")),
            _ => Value::Null,
        })
    }
}

fn navigate_to_path(jsonb: &mut Jsonb, path: &Value) -> Result<Option<Jsonb>, LimboError> {
    let json_path = json_path_from_db_value(path, true)?.ok_or_else(|| {
        LimboError::InvalidArgument(format!("path '{path}' is not a valid json path"))
    })?;
    let mut search_operation = SearchOperation::new(jsonb.len() / 2);
    if jsonb
        .operate_on_path(&json_path, &mut search_operation)
        .is_err()
    {
        return Ok(None);
    }
    Ok(Some(search_operation.result()))
}

mod columns {
    use crate::{
        json::{
            json_string_to_db_type,
            jsonb::{self, ElementType, Jsonb},
            OutputVariant,
        },
        types::Text,
        LimboError, Value,
    };

    #[derive(Debug)]
    pub(super) enum Key {
        Integer(i64),
        String(String),
        None,
    }

    impl Key {
        fn empty() -> Self {
            Self::None
        }

        fn fullkey_representation(&self, root_path: &str) -> Value {
            match self {
                Key::Integer(ref i) => Value::Text(Text::new(&format!("{root_path}[{i}]"))),
                Key::String(ref text) => {
                    let mut needs_quoting: bool = false;

                    let mut text = (text[1..text.len() - 1]).to_owned();
                    if text.contains('.') || text.contains(" ") || text.contains('"') {
                        needs_quoting = true;
                    }

                    if needs_quoting {
                        text = format!("\"{text}\"");
                    }
                    let s = format!("{root_path}.{text}");

                    Value::Text(Text::new(&s))
                }
                Key::None => Value::Text(Text::new(root_path)),
            }
        }

        fn key_representation(&self) -> Value {
            match self {
                Key::Integer(ref i) => Value::Integer(*i),
                Key::String(ref s) => Value::Text(Text::new(
                    &s[1..s.len() - 1].to_owned().replace("\\\"", "\""),
                )),
                Key::None => Value::Null,
            }
        }
    }

    pub(super) struct Columns {
        key: Key,
        value: Jsonb,
        root_path: String,
    }

    impl Default for Columns {
        fn default() -> Columns {
            Self {
                key: Key::empty(),
                value: Jsonb::new(0, None),
                root_path: String::new(),
            }
        }
    }

    impl Columns {
        pub(super) fn new(key: Key, value: Jsonb, root_path: Option<String>) -> Self {
            Self {
                key,
                value,
                root_path: root_path.unwrap_or_else(|| "$".to_owned()),
            }
        }

        pub(super) fn new_from_primitive(value: Jsonb, root_path: Option<String>) -> Self {
            Self {
                key: Key::empty(),
                value,
                root_path: root_path.unwrap_or_else(|| "$".to_owned()),
            }
        }

        pub(super) fn atom(&self) -> Result<Value, LimboError> {
            Self::atom_from_value(&self.value)
        }

        pub(super) fn value(&self) -> Result<Value, LimboError> {
            let element_type = self.value.element_type()?;
            Ok(match element_type {
                ElementType::ARRAY | ElementType::OBJECT => {
                    json_string_to_db_type(self.value.clone(), element_type, OutputVariant::String)?
                }
                _ => Self::atom_from_value(&self.value)?,
            })
        }

        pub(super) fn key(&self) -> Value {
            self.key.key_representation()
        }

        fn atom_from_value(value: &Jsonb) -> Result<Value, LimboError> {
            let element_type = value.element_type().expect("invalid value");
            let string: Result<Value, LimboError> = match element_type {
                jsonb::ElementType::NULL => Ok(Value::Null),
                jsonb::ElementType::TRUE => Ok(Value::Integer(1)),
                jsonb::ElementType::FALSE => Ok(Value::Integer(0)),
                jsonb::ElementType::INT | jsonb::ElementType::INT5 => Self::jsonb_to_integer(value),
                jsonb::ElementType::FLOAT | jsonb::ElementType::FLOAT5 => {
                    Self::jsonb_to_float(value)
                }
                jsonb::ElementType::TEXT
                | jsonb::ElementType::TEXTJ
                | jsonb::ElementType::TEXT5
                | jsonb::ElementType::TEXTRAW => {
                    let s = value.to_string();
                    let s = (s[1..s.len() - 1]).to_string();
                    Ok(Value::Text(Text::new(&s)))
                }
                jsonb::ElementType::ARRAY => Ok(Value::Null),
                jsonb::ElementType::OBJECT => Ok(Value::Null),
                jsonb::ElementType::RESERVED1 => Ok(Value::Null),
                jsonb::ElementType::RESERVED2 => Ok(Value::Null),
                jsonb::ElementType::RESERVED3 => Ok(Value::Null),
            };

            string
        }

        fn jsonb_to_integer(value: &Jsonb) -> Result<Value, LimboError> {
            let string = value.to_string();
            let int = string.parse::<i64>()?;

            Ok(Value::Integer(int))
        }

        fn jsonb_to_float(value: &Jsonb) -> Result<Value, LimboError> {
            let string = value.to_string();
            let float = string.parse::<f64>()?;

            Ok(Value::Float(float))
        }

        pub(super) fn fullkey(&self) -> Value {
            self.key.fullkey_representation(&self.root_path)
        }

        pub(super) fn path(&self) -> Value {
            Value::Text(Text::new(&self.root_path))
        }

        pub(super) fn parent(&self) -> Value {
            Value::Null
        }

        pub(super) fn ttype(&self) -> Value {
            let element_type = self.value.element_type().expect("invalid value");
            let ttype = match element_type {
                jsonb::ElementType::NULL => "null",
                jsonb::ElementType::TRUE => "true",
                jsonb::ElementType::FALSE => "false",
                jsonb::ElementType::INT | jsonb::ElementType::INT5 => "integer",
                jsonb::ElementType::FLOAT | jsonb::ElementType::FLOAT5 => "real",
                jsonb::ElementType::TEXT
                | jsonb::ElementType::TEXTJ
                | jsonb::ElementType::TEXT5
                | jsonb::ElementType::TEXTRAW => "text",
                jsonb::ElementType::ARRAY => "array",
                jsonb::ElementType::OBJECT => "object",
                jsonb::ElementType::RESERVED1
                | jsonb::ElementType::RESERVED2
                | jsonb::ElementType::RESERVED3 => unreachable!(),
            };

            Value::Text(Text::new(ttype))
        }
    }
}
