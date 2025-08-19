use turso_ext::{
    ConstraintUsage, ResultCode, VTabCursor, VTabModule, VTabModuleDerive, VTable, Value,
};

use crate::json::{
    convert_dbtype_to_jsonb,
    jsonb::{ArrayIteratorState, Jsonb, ObjectIteratorState},
    vtab::columns::Columns,
    Conv,
};

use super::jsonb;

macro_rules! try_result {
    ($msg:expr, $expr:expr) => {
        try_result!($msg, $expr, |x| x)
    };
    ($msg:expr, $expr:expr, $fn:expr) => {
        match $expr {
            Ok(val) => val,
            Err(err) => {
                eprintln!("Error: {}: {}", $msg, err);
                return $fn(ResultCode::Error);
            }
        }
    };
}

#[derive(VTabModuleDerive)]
pub struct JsonEachVirtualTable;

const COL_KEY: u32 = 0;
const COL_VALUE: u32 = 1;
const COL_TYPE: u32 = 2;
const COL_ATOM: u32 = 3;
const COL_ID: u32 = 4;
const COL_PARENT: u32 = 5;
const COL_FULLKEY: u32 = 6;
const COL_PATH: u32 = 7;
const COL_JSON: u32 = 8;
const COL_ROOT: u32 = 9;

impl VTabModule for JsonEachVirtualTable {
    type Table = JsonEachVirtualTable;

    const VTAB_KIND: turso_ext::VTabKind = turso_ext::VTabKind::TableValuedFunction;

    const NAME: &'static str = "json_each";

    fn create(_args: &[Value]) -> Result<(String, Self::Table), turso_ext::ResultCode> {
        Ok((
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
            .to_owned(),
            JsonEachVirtualTable {},
        ))
    }
}

impl VTable for JsonEachVirtualTable {
    type Cursor = JsonEachCursor;

    type Error = ResultCode;

    fn open(
        &self,
        _conn: Option<std::sync::Arc<turso_ext::Connection>>,
    ) -> Result<Self::Cursor, Self::Error> {
        Ok(JsonEachCursor::default())
    }

    fn best_index(
        _constraints: &[turso_ext::ConstraintInfo],
        _order_by: &[turso_ext::OrderByInfo],
    ) -> Result<turso_ext::IndexInfo, ResultCode> {
        use turso_ext::ConstraintOp;

        let mut usages = vec![
            ConstraintUsage {
                argv_index: None,
                omit: false
            };
            _constraints.len()
        ];
        let mut have_json = false;

        for (i, c) in _constraints.iter().enumerate() {
            if c.usable && c.op == ConstraintOp::Eq && c.column_index == COL_JSON {
                usages[i] = ConstraintUsage {
                    argv_index: Some(1),
                    omit: true,
                };
                have_json = true;
                break;
            }
        }

        Ok(turso_ext::IndexInfo {
            idx_num: i32::from(have_json),
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: if have_json { 10.0 } else { 1_000_000.0 },
            estimated_rows: if have_json { 100 } else { u32::MAX },
            constraint_usages: usages,
        })
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
    iterator_state: IteratorState,
    columns: Columns,
}

impl Default for JsonEachCursor {
    fn default() -> Self {
        Self {
            rowid: 0,
            no_more_rows: false,
            json: Jsonb::new(0, None),
            iterator_state: IteratorState::None,
            columns: Columns::default(),
        }
    }
}

impl VTabCursor for JsonEachCursor {
    type Error = ResultCode;

    fn filter(&mut self, args: &[Value], _idx_info: Option<(&str, i32)>) -> ResultCode {
        if args.is_empty() {
            return ResultCode::EOF;
        }
        if args.len() != 1 && args.len() != 2 {
            return ResultCode::InvalidArgs;
        }

        let db_value = try_result!(
            "converting ext value to db value",
            convert_ext_value_to_db_value(&args[0])
        );

        let jsonb = try_result!(
            "converting to jsonb",
            convert_dbtype_to_jsonb(&db_value, Conv::Strict)
        );

        let element_type = try_result!("checking jsonb element type", jsonb.element_type());
        self.json = jsonb;

        match element_type {
            jsonb::ElementType::ARRAY => {
                let iter = try_result!("getting array iterator", self.json.array_iterator());
                self.iterator_state = IteratorState::Array(iter);
            }
            jsonb::ElementType::OBJECT => {
                let iter = try_result!("getting object iterator", self.json.object_iterator());
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
                eprintln!("Error: received unexpected element type {element_type:?}");
                return ResultCode::Error;
            }
        }

        self.next()
    }

    fn next(&mut self) -> ResultCode {
        self.rowid += 1;
        if self.no_more_rows {
            return ResultCode::EOF;
        }

        match &self.iterator_state {
            IteratorState::Array(state) => {
                let Some(((idx, jsonb), new_state)) = self.json.array_iterator_next(state) else {
                    self.no_more_rows = true;
                    return ResultCode::EOF;
                };
                self.iterator_state = IteratorState::Array(new_state);
                self.columns = Columns::new(columns::Key::Integer(idx as i64), jsonb);
            }
            IteratorState::Object(state) => {
                let Some(((_idx, key, value), new_state)): Option<(
                    (usize, Jsonb, Jsonb),
                    ObjectIteratorState,
                )> = self.json.object_iterator_next(state) else {
                    self.no_more_rows = true;
                    return ResultCode::EOF;
                };

                self.iterator_state = IteratorState::Object(new_state);
                let key = try_result!("converting key to db value", key.to_string());
                self.columns = Columns::new(columns::Key::String(key), value);
            }
            IteratorState::Primitive => {
                let json = std::mem::replace(&mut self.json, Jsonb::new(0, None));
                self.columns = Columns::new_from_primitive(json);
                self.no_more_rows = true;
            }
            IteratorState::None => unreachable!(),
        };

        ResultCode::OK
    }

    fn rowid(&self) -> i64 {
        self.rowid
    }

    fn column(&self, idx: u32) -> Result<Value, Self::Error> {
        Ok(match idx {
            COL_KEY => self.columns.key(),
            COL_VALUE => {
                try_result!(
                    "converting value column to ext value",
                    self.columns.value(),
                    Err
                )
            }
            COL_TYPE => self.columns.ttype(),
            COL_ATOM => self.columns.atom()?,
            COL_ID => Value::from_integer(self.rowid),
            COL_PARENT => self.columns.parent(),
            COL_FULLKEY => self.columns.fullkey(),
            COL_PATH => self.columns.path(),
            COL_ROOT => Value::from_text("json, todo".to_owned()),
            num => {
                println!("was asked for column {num}");
                Value::null()
            }
        })
    }

    fn eof(&self) -> bool {
        self.no_more_rows
    }
}

fn convert_ext_value_to_db_value(ext_value: &turso_ext::Value) -> Result<crate::Value, ResultCode> {
    Ok(match ext_value.value_type() {
        turso_ext::ValueType::Null => crate::Value::Null,
        turso_ext::ValueType::Text if ext_value.is_json() => crate::Value::Text(
            crate::types::Text::json(ext_value.to_text().unwrap().to_owned()),
        ),
        turso_ext::ValueType::Text => {
            crate::Value::Text(crate::types::Text::new(ext_value.to_text().unwrap()))
        }
        turso_ext::ValueType::Integer => crate::Value::Integer(ext_value.to_integer().unwrap()),
        turso_ext::ValueType::Float => crate::Value::Float(ext_value.to_float().unwrap()),
        turso_ext::ValueType::Blob => crate::Value::Blob(ext_value.to_blob().unwrap()),
        turso_ext::ValueType::Error => return Err(ResultCode::InvalidArgs),
    })
}

mod columns {
    use turso_ext::{ResultCode, Value};

    use crate::json::{
        json_string_to_ext_value,
        jsonb::{self, ElementType, Jsonb},
        OutputVariant,
    };

    #[derive(Debug)]
    pub(super) enum Key {
        Integer(i64),
        String(String),
    }

    impl Key {
        fn empty() -> Self {
            Self::Integer(0)
        }

        fn fullkey_representation(&self) -> Value {
            match self {
                Key::Integer(ref i) => Value::from_text(format!("$[{i}]")),
                Key::String(ref text) => {
                    let mut needs_quoting: bool = false;

                    let mut text = (text[1..text.len() - 1]).to_owned();
                    if text.contains('.') || text.contains(" ") || text.contains('"') {
                        needs_quoting = true;
                    }

                    if needs_quoting {
                        text = format!("\"{text}\"");
                    }
                    let s = format!("$.{text}");

                    Value::from_text(s)
                }
            }
        }

        fn key_representation(&self) -> Value {
            match self {
                Key::Integer(ref i) => Value::from_integer(*i),
                Key::String(ref s) => {
                    Value::from_text(s[1..s.len() - 1].to_owned().replace("\\\"", "\""))
                }
            }
        }
    }

    pub(super) struct Columns {
        key: Key,
        value: Jsonb,
        is_primitive: bool,
    }

    impl Default for Columns {
        fn default() -> Columns {
            Self {
                key: Key::empty(),
                value: Jsonb::new(0, None),
                is_primitive: false,
            }
        }
    }

    impl Columns {
        pub(super) fn new(key: Key, value: Jsonb) -> Self {
            Self {
                key,
                value,
                is_primitive: false,
            }
        }

        pub(super) fn new_from_primitive(value: Jsonb) -> Self {
            Self {
                key: Key::empty(),
                value,
                is_primitive: true,
            }
        }

        pub(super) fn atom(&self) -> Result<Value, ResultCode> {
            Self::atom_from_value(&self.value)
        }

        pub(super) fn value(&self) -> Result<Value, ResultCode> {
            Ok(
                match try_result!("reading element type", self.value.element_type(), Err) {
                    ElementType::ARRAY | ElementType::OBJECT => {
                        let value = try_result!(
                            "converting value to extension value",
                            json_string_to_ext_value(self.value.clone(), OutputVariant::String),
                            Err
                        );
                        value
                    }
                    _ => Self::atom_from_value(&self.value)?,
                },
            )
        }

        pub(super) fn key(&self) -> Value {
            if self.is_primitive {
                return Value::null();
            }
            self.key.key_representation()
        }

        fn atom_from_value(value: &Jsonb) -> Result<Value, ResultCode> {
            let element_type = value.element_type().expect("invalid value");
            let string: Result<Value, turso_ext::ResultCode> = match element_type {
                jsonb::ElementType::NULL => Ok(Value::null()),
                jsonb::ElementType::TRUE => Ok(Value::from_integer(1)),
                jsonb::ElementType::FALSE => Ok(Value::from_integer(0)),
                jsonb::ElementType::INT | jsonb::ElementType::INT5 => Self::jsonb_to_integer(value),
                jsonb::ElementType::FLOAT | jsonb::ElementType::FLOAT5 => {
                    Self::jsonb_to_float(value)
                }
                jsonb::ElementType::TEXT
                | jsonb::ElementType::TEXTJ
                | jsonb::ElementType::TEXT5
                | jsonb::ElementType::TEXTRAW => {
                    let s = try_result!("converting value to string", value.to_string(), Err);
                    let s = (s[1..s.len() - 1]).to_string();
                    Ok(Value::from_text(s))
                }
                jsonb::ElementType::ARRAY => Ok(Value::null()),
                jsonb::ElementType::OBJECT => Ok(Value::null()),
                jsonb::ElementType::RESERVED1 => Ok(Value::null()),
                jsonb::ElementType::RESERVED2 => Ok(Value::null()),
                jsonb::ElementType::RESERVED3 => Ok(Value::null()),
            };

            string.map_err(|_| ResultCode::Error)
        }

        fn jsonb_to_integer(value: &Jsonb) -> Result<Value, ResultCode> {
            let string = try_result!("converting jsonb to integer", value.to_string(), Err);
            let int = try_result!("parsing int", string.parse::<i64>(), Err);

            Ok(Value::from_integer(int))
        }

        fn jsonb_to_float(value: &Jsonb) -> Result<Value, ResultCode> {
            let string = try_result!("converting jsonb to integer", value.to_string(), Err);
            let float = try_result!("parsing float", string.parse::<f64>(), Err);

            Ok(Value::from_float(float))
        }

        pub(super) fn fullkey(&self) -> Value {
            if self.is_primitive {
                return Value::from_text("$".to_owned());
            }
            self.key.fullkey_representation()
        }

        pub(super) fn path(&self) -> Value {
            Value::from_text("$".to_owned())
        }

        pub(super) fn parent(&self) -> Value {
            Value::null()
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

            Value::from_text(ttype.to_owned())
        }
    }
}
