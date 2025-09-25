use parking_lot::RwLock;
use std::iter::successors;
use std::{result::Result, sync::Arc};

use turso_ext::{ConstraintOp, ConstraintUsage, ResultCode};

use crate::{
    json::{
        convert_dbtype_to_jsonb, json_path_from_db_value,
        jsonb::{IteratorState, Jsonb, SearchOperation},
        path::{json_path, JsonPath, PathElement},
        vtab::columns::{Columns, Key},
        Conv,
    },
    vtab::{InternalVirtualTable, InternalVirtualTableCursor},
    Connection, LimboError, Value,
};

use super::jsonb;

#[derive(Clone)]
enum JsonTraversalMode {
    /// Walk top-level keys/indices, but don't recurse. Used in `json_each`.
    Each,
    /// Walk keys/indices recursively. Used in `json_tree`.
    Tree,
}

impl JsonTraversalMode {
    fn function_name(&self) -> &'static str {
        match self {
            JsonTraversalMode::Each => "json_each",
            JsonTraversalMode::Tree => "json_tree",
        }
    }
}

pub struct JsonVirtualTable {
    traversal_mode: JsonTraversalMode,
}

impl JsonVirtualTable {
    pub fn json_each() -> Self {
        Self {
            traversal_mode: JsonTraversalMode::Each,
        }
    }

    pub fn json_tree() -> Self {
        Self {
            traversal_mode: JsonTraversalMode::Tree,
        }
    }
}

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

impl InternalVirtualTable for JsonVirtualTable {
    fn name(&self) -> String {
        self.traversal_mode.function_name().to_owned()
    }

    fn open(
        &self,
        _conn: Arc<Connection>,
    ) -> crate::Result<std::sync::Arc<RwLock<dyn InternalVirtualTableCursor + 'static>>> {
        Ok(Arc::new(RwLock::new(JsonEachCursor::empty(
            self.traversal_mode.clone(),
        ))))
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
        "CREATE TABLE x(
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

impl std::fmt::Debug for JsonVirtualTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonEachVirtualTable").finish()
    }
}

pub struct JsonEachCursor {
    rowid: i64,
    json: Jsonb,
    path_to_current_value: InPlaceJsonPath,
    traversal_states: Vec<TraversalState>,
    columns: Columns,
    traversal_mode: JsonTraversalMode,
}

struct TraversalState {
    iterator_state: IteratorState,
    parent_id: Option<i64>,
    innermost_container_id: Option<i64>,
    innermost_container_cursor: InPlaceJsonPathCursor,
}

impl JsonEachCursor {
    fn empty(traversal_mode: JsonTraversalMode) -> Self {
        Self {
            rowid: 0,
            json: Jsonb::new(0, None),
            traversal_states: Vec::new(),
            path_to_current_value: InPlaceJsonPath::new_root(),
            columns: Columns::default(),
            traversal_mode,
        }
    }

    fn push_state(
        &mut self,
        iterator_state: IteratorState,
        innermost_container_cursor: InPlaceJsonPathCursor,
    ) {
        let parent_id = self
            .traversal_states
            .last()
            .and_then(|state| state.innermost_container_id)
            .or(Some(0));

        let innermost_container = match iterator_state {
            IteratorState::Object(_) | IteratorState::Array(_) => Some(self.rowid),
            _ => parent_id,
        };

        self.traversal_states.push(TraversalState {
            iterator_state,
            parent_id,
            innermost_container_id: innermost_container,
            innermost_container_cursor,
        });
    }

    fn peek_state(&self) -> Option<&TraversalState> {
        self.traversal_states.last()
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
        if args.len() == 2 && matches!(self.traversal_mode, JsonTraversalMode::Tree) {
            if let Value::Text(ref text) = args[1] {
                if !text.value.is_empty() && text.value.windows(3).any(|chars| chars == b"[#-") {
                    return Err(LimboError::InvalidArgument(
                        "Json paths with negative indices in json_tree are not supported yet"
                            .to_owned(),
                    ));
                }
            }
        }

        let mut jsonb = convert_dbtype_to_jsonb(&args[0], Conv::Strict)?;

        let (path, root_json) = if args.len() == 1 {
            let path = "$";
            (path, jsonb)
        } else {
            let Value::Text(path) = &args[1] else {
                return Err(LimboError::InvalidArgument(
                    "root path should be text".to_owned(),
                ));
            };
            let root_json = if let Some(json) = navigate_to_path(&mut jsonb, &args[1])? {
                json
            } else {
                return Ok(false);
            };
            (path.as_str(), root_json)
        };

        self.json = root_json;
        self.path_to_current_value =
            InPlaceJsonPath::from_json_path(path.to_owned(), json_path(path)?);
        let iterator_state = json_iterator_from(&self.json)?;
        let innermost_container_path = if matches!(self.traversal_mode, JsonTraversalMode::Tree)
            && matches!(iterator_state, IteratorState::Primitive(_))
        {
            self.path_to_current_value.cursor_before_last_element()
        } else {
            self.path_to_current_value.cursor()
        };
        self.push_state(iterator_state, innermost_container_path);

        let key = self.path_to_current_value.key().to_owned();
        match self.traversal_mode {
            JsonTraversalMode::Each => self.next(),
            JsonTraversalMode::Tree => {
                if matches!(
                    self.peek_state().unwrap().iterator_state,
                    IteratorState::Primitive(_)
                ) {
                    self.next()
                } else {
                    self.columns = Columns::new(
                        key,
                        self.json.clone(),
                        self.path_to_current_value.string.clone(),
                        None,
                        self.path_to_current_value
                            .read(self.path_to_current_value.cursor_before_last_element())
                            .to_owned(),
                    );
                    Ok(true)
                }
            }
        }
    }

    fn next(&mut self) -> Result<bool, LimboError> {
        self.rowid += 1;
        if self.traversal_states.is_empty() {
            return Ok(false);
        }

        let traversal_state = self
            .traversal_states
            .pop()
            .expect("traversal state stack is empty");

        let parent_id = if matches!(self.traversal_mode, JsonTraversalMode::Tree) {
            traversal_state.parent_id
        } else {
            None
        };
        match traversal_state.iterator_state {
            IteratorState::Array(state) => {
                let Some(((idx, value), new_state)) = self.json.array_iterator_next(&state) else {
                    self.path_to_current_value.pop();
                    return self.next();
                };

                let recursing_iterator = if matches!(self.traversal_mode, JsonTraversalMode::Tree) {
                    self.json
                        .container_property_iterator(&IteratorState::Array(state))
                } else {
                    None
                };
                self.push_state(
                    IteratorState::Array(new_state),
                    self.path_to_current_value.cursor(),
                );
                let recurses = recursing_iterator.is_some();
                self.path_to_current_value.push_array_index(&idx);
                if let Some(it) = recursing_iterator {
                    self.push_state(it, self.path_to_current_value.cursor());
                }

                let key = self.path_to_current_value.key().to_owned();
                self.columns = Columns::new(
                    key,
                    value,
                    self.path_to_current_value.string.clone(),
                    parent_id,
                    self.path_to_current_value
                        .read(traversal_state.innermost_container_cursor)
                        .to_owned(),
                );

                if !recurses {
                    self.path_to_current_value.pop();
                }
            }
            IteratorState::Object(state) => {
                let Some(((_idx, key, value), new_state)) = self.json.object_iterator_next(&state)
                else {
                    self.path_to_current_value.pop();
                    return self.next();
                };

                self.push_state(
                    IteratorState::Object(new_state),
                    self.path_to_current_value.cursor(),
                );
                self.path_to_current_value.push_object_key(&key.to_string());
                let recursing = matches!(self.traversal_mode, JsonTraversalMode::Tree)
                    && self
                        .json
                        .container_property_iterator(&IteratorState::Object(state))
                        .is_some_and(|it| {
                            self.push_state(it, self.path_to_current_value.cursor());
                            true
                        });

                self.columns = Columns::new(
                    self.path_to_current_value.key().to_owned(),
                    value,
                    self.path_to_current_value.string.clone(),
                    parent_id,
                    self.path_to_current_value
                        .read(traversal_state.innermost_container_cursor)
                        .to_owned(),
                );

                if !recursing {
                    self.path_to_current_value.pop();
                }
            }
            IteratorState::Primitive(jsonb) => {
                let key = match self.traversal_mode {
                    JsonTraversalMode::Each => Key::None,
                    JsonTraversalMode::Tree => self.path_to_current_value.key().to_owned(),
                };
                self.columns = Columns::new(
                    key,
                    jsonb,
                    self.path_to_current_value.string.clone(),
                    parent_id,
                    self.path_to_current_value
                        .read(traversal_state.innermost_container_cursor)
                        .to_owned(),
                );
            }
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
            _ => Value::Null,
        })
    }
}

fn json_iterator_from(json: &Jsonb) -> crate::Result<IteratorState> {
    let json_element_type = json.element_type()?;
    match json_element_type {
        jsonb::ElementType::ARRAY => {
            let iter = json.array_iterator()?;
            Ok(IteratorState::Array(iter))
        }

        jsonb::ElementType::OBJECT => {
            let iter = json.object_iterator()?;
            Ok(IteratorState::Object(iter))
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
        | jsonb::ElementType::TEXTRAW => Ok(IteratorState::Primitive(json.clone())),
        jsonb::ElementType::RESERVED1
        | jsonb::ElementType::RESERVED2
        | jsonb::ElementType::RESERVED3 => {
            unreachable!("element type not supported: {json_element_type:?}");
        }
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

    #[derive(Debug, Clone)]
    pub(super) enum Key {
        Integer(i64),
        String(String),
        None,
    }

    impl Key {
        fn empty() -> Self {
            Self::None
        }

        fn key_representation(&self) -> Value {
            match self {
                Key::Integer(ref i) => Value::Integer(*i),
                Key::String(ref s) => Value::Text(Text::new(&s.to_owned().replace("\\\"", "\""))),
                Key::None => Value::Null,
            }
        }
    }

    pub(super) struct Columns {
        key: Key,
        value: Jsonb,
        fullkey: String,
        parent_id: Option<i64>,
        innermost_container_path: String,
    }

    impl Default for Columns {
        fn default() -> Columns {
            Self {
                key: Key::empty(),
                value: Jsonb::new(0, None),
                fullkey: "".to_owned(),
                parent_id: None,
                innermost_container_path: "".to_owned(),
            }
        }
    }

    impl Columns {
        pub(super) fn new(
            key: Key,
            value: Jsonb,
            fullkey: String,
            parent_id: Option<i64>,
            innermost_container_path: String,
        ) -> Self {
            Self {
                key,
                value,
                parent_id,
                fullkey,
                innermost_container_path,
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
            Value::Text(Text::new(&self.fullkey))
        }

        pub(super) fn path(&self) -> Value {
            Value::Text(Text::new(&self.innermost_container_path))
        }

        pub(super) fn parent(&self) -> Value {
            match self.parent_id {
                Some(id) => Value::Integer(id),
                None => Value::Null,
            }
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

struct InPlaceJsonPath {
    string: String,
    element_lengths: Vec<usize>,
    last_element: Key,
}

type InPlaceJsonPathCursor = usize;

impl InPlaceJsonPath {
    fn new_root() -> Self {
        Self {
            string: "$".to_owned(),
            element_lengths: vec![1],
            last_element: Key::None,
        }
    }

    fn pop(&mut self) {
        if let Some(len) = self.element_lengths.pop() {
            if len != 0 {
                self.string.truncate(self.string.len() - len);
            }
        }
    }

    fn push_array_index(&mut self, idx: &usize) {
        self.last_element = Key::Integer(*idx as i64);
        self.push(format!("[{idx}]"));
    }

    fn push_object_key(&mut self, key: &str) {
        // This follows SQLite's current quoting scheme, but it is not part of the stable API.
        // See https://sqlite.org/forum/forumpost?udc=1&name=be212a295ed8df4c
        let unquoted_if_necessary = if (key[1..key.len() - 1])
            .chars()
            .any(|c| c == '.' || c == ' ' || c == '"' || c == '_')
        {
            key
        } else {
            &key[1..key.len() - 1]
        };
        let always_unquoted = &key[1..key.len() - 1];
        self.last_element = Key::String(always_unquoted.to_owned());
        self.push(format!(".{unquoted_if_necessary}"));
    }

    fn push(&mut self, element: String) {
        self.element_lengths.push(element.len());
        self.string.push_str(&element);
    }

    fn cursor(&self) -> InPlaceJsonPathCursor {
        self.string.len()
    }

    fn read(&self, cursor: InPlaceJsonPathCursor) -> &str {
        &self.string[0..cursor]
    }

    fn from_json_path(path: String, json_path: JsonPath<'_>) -> Self {
        let (json_path, last_element) = if json_path.elements.is_empty() {
            (
                JsonPath {
                    elements: vec![PathElement::Root()],
                },
                Key::None,
            )
        } else {
            let last_element = json_path
                .elements
                .last()
                .and_then(|path_element| match path_element {
                    PathElement::Key(cow, _) => Some(Key::String(cow.to_string())),
                    PathElement::ArrayLocator(Some(idx)) => Some(Key::Integer(*idx as i64)),
                    _ => None,
                })
                .unwrap_or(Key::None);

            (json_path, last_element)
        };

        let element_lengths = json_path
            .elements
            .iter()
            .map(Self::element_length)
            .collect();

        Self {
            string: path.to_owned(),
            element_lengths,
            last_element,
        }
    }

    fn element_length(element: &PathElement) -> usize {
        match element {
            PathElement::Root() => 1,
            PathElement::Key(key, _) => key.len() + 1,
            PathElement::ArrayLocator(idx) => {
                let digit_count = successors(*idx, |&n| (n >= 10).then_some(n / 10)).count();
                let bracket_count = 2; // []

                digit_count + bracket_count
            }
        }
    }

    fn cursor_before_last_element(&self) -> InPlaceJsonPathCursor {
        if self.element_lengths.len() == 1 {
            self.cursor()
        } else {
            self.cursor() - self.element_lengths.last().unwrap()
        }
    }

    fn key(&self) -> &Key {
        &self.last_element
    }
}
