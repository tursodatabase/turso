use crate::sync::{Arc, RwLock};
use std::iter::successors;
use std::result::Result;

use turso_ext::{ConstraintOp, ConstraintUsage, ResultCode};

use crate::{
    json::{
        jsonb::{IteratorState, Jsonb},
        parse_strict_into,
        path::{json_path, JsonPath, PathElement},
        vtab::columns::{Columns, Key},
    },
    types::AsValueRef,
    vdbe::Register,
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
    ) -> crate::Result<Arc<RwLock<dyn InternalVirtualTableCursor + 'static>>> {
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
        let mut has_json_eq_constraint = false;
        let mut has_root_eq_constraint = false;
        for (i, c) in constraints.iter().enumerate() {
            if c.op != ConstraintOp::Eq {
                continue;
            }
            match c.column_index as usize {
                COL_JSON => {
                    has_json_eq_constraint = true;
                    if c.usable {
                        json_idx = Some(i);
                    }
                }
                COL_ROOT => {
                    has_root_eq_constraint = true;
                    if c.usable {
                        path_idx = Some(i);
                    }
                }
                _ => {}
            }
        }

        // Hidden arguments supplied in SQL must be usable in the chosen loop.
        // If they are present but unusable, reject this access shape so the
        // optimizer can pick a join order where argument registers are available.
        if has_json_eq_constraint && json_idx.is_none() {
            return Err(ResultCode::ConstraintViolation);
        }
        if has_root_eq_constraint && path_idx.is_none() {
            return Err(ResultCode::ConstraintViolation);
        }

        let argc = match (json_idx, path_idx) {
            (Some(_), Some(_)) => 2,
            (Some(_), None) => 1,
            _ => 0,
        };

        if argc >= 1 {
            let idx = json_idx.expect("json_idx should be Some when argc >= 1");
            usages[idx] = ConstraintUsage {
                argv_index: Some(1),
                omit: true,
            };
        }
        if argc == 2 {
            let idx = path_idx.expect("path_idx should be Some when argc == 2");
            usages[idx] = ConstraintUsage {
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
        "CREATE TABLE x(key, value, type, atom, id, parent, fullkey, path, json HIDDEN, root HIDDEN)"
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
    object_key: String,
    root_path: Option<RootPath>,
    pop_path_before_next: bool,
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
            json: Jsonb::empty(),
            traversal_states: Vec::new(),
            path_to_current_value: InPlaceJsonPath::new_root(),
            columns: Columns::default(),
            traversal_mode,
            object_key: String::new(),
            root_path: None,
            pop_path_before_next: false,
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
        self.traversal_states.clear();
        self.rowid = 0;
        self.pop_path_before_next = false;

        if args.is_empty() || args[0] == Value::Null {
            return Ok(false);
        }
        if args.len() == 2 && matches!(self.traversal_mode, JsonTraversalMode::Tree) {
            if let Value::Text(ref text) = args[1] {
                if !text.value.is_empty()
                    && text
                        .value
                        .as_bytes()
                        .windows(3)
                        .any(|chars| chars == b"[#-")
                {
                    return Err(LimboError::InvalidArgument(
                        "Json paths with negative indices in json_tree are not supported yet"
                            .to_owned(),
                    ));
                }
            }
        }

        parse_strict_into(args[0].as_value_ref(), &mut self.json)?;

        let root = if args.len() == 1 {
            self.path_to_current_value.reset_to_root();
            0
        } else {
            let Value::Text(path) = &args[1] else {
                return Err(LimboError::InvalidArgument(
                    "root path should be text".to_owned(),
                ));
            };
            let root_path = match self.root_path.take() {
                Some(root_path) if root_path.text == path.as_str() => root_path,
                _ => RootPath::parse(path.as_str())?,
            };
            let found = jsonb::find_path_element(self.json.as_slice(), &root_path.path);
            self.path_to_current_value.clone_from(&root_path.start);
            self.root_path = Some(root_path);
            let Ok(Some(root)) = found else {
                return Ok(false);
            };
            root
        };

        let iterator_state = json_iterator_from(&self.json, root)?;
        let innermost_container_path = if matches!(self.traversal_mode, JsonTraversalMode::Tree)
            && matches!(iterator_state, IteratorState::Primitive(_))
        {
            self.path_to_current_value.cursor_before_last_element()
        } else {
            self.path_to_current_value.cursor()
        };
        self.push_state(iterator_state, innermost_container_path);

        match self.traversal_mode {
            JsonTraversalMode::Each => self.next(),
            JsonTraversalMode::Tree => {
                let state = self.peek_state().ok_or_else(|| {
                    crate::LimboError::InternalError("state stack should not be empty".to_string())
                })?;
                if matches!(state.iterator_state, IteratorState::Primitive(_)) {
                    self.next()
                } else {
                    self.columns.set(
                        self.path_to_current_value.key(),
                        root,
                        self.path_to_current_value.cursor(),
                        None,
                        self.path_to_current_value.cursor_before_last_element(),
                    );
                    Ok(true)
                }
            }
        }
    }

    fn next(&mut self) -> Result<bool, LimboError> {
        if std::mem::take(&mut self.pop_path_before_next) {
            self.path_to_current_value.pop();
        }
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
                self.path_to_current_value.push_array_index(idx);
                if let Some(it) = recursing_iterator {
                    self.push_state(it, self.path_to_current_value.cursor());
                }

                self.columns.set(
                    self.path_to_current_value.key(),
                    value,
                    self.path_to_current_value.cursor(),
                    parent_id,
                    traversal_state.innermost_container_cursor,
                );
                self.pop_path_before_next = !recurses;
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
                self.object_key.clear();
                self.json.write_element_text(key, &mut self.object_key)?;
                self.path_to_current_value
                    .push_object_key(&self.object_key)?;
                let recursing = matches!(self.traversal_mode, JsonTraversalMode::Tree)
                    && self
                        .json
                        .container_property_iterator(&IteratorState::Object(state))
                        .is_some_and(|it| {
                            self.push_state(it, self.path_to_current_value.cursor());
                            true
                        });

                self.columns.set(
                    self.path_to_current_value.key(),
                    value,
                    self.path_to_current_value.cursor(),
                    parent_id,
                    traversal_state.innermost_container_cursor,
                );
                self.pop_path_before_next = !recursing;
            }
            IteratorState::Primitive(value) => {
                let key = match self.traversal_mode {
                    JsonTraversalMode::Each => &Key::None,
                    JsonTraversalMode::Tree => self.path_to_current_value.key(),
                };
                self.columns.set(
                    key,
                    value,
                    self.path_to_current_value.cursor(),
                    None,
                    traversal_state.innermost_container_cursor,
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
            COL_VALUE => self.columns.value(&self.json)?,
            COL_TYPE => self.columns.ttype(&self.json),
            COL_ATOM => self.columns.atom(&self.json)?,
            COL_ID => Value::from_i64(self.rowid),
            COL_PARENT => self.columns.parent(),
            COL_FULLKEY => self.columns.fullkey(&self.path_to_current_value.string),
            COL_PATH => self.columns.path(&self.path_to_current_value.string),
            _ => Value::Null,
        })
    }

    fn column_into(&self, idx: usize, dest: &mut Register) -> Result<(), LimboError> {
        if matches!(idx, COL_VALUE | COL_ATOM) && self.columns.write_text_atom(&self.json, dest)? {
            return Ok(());
        }
        dest.set_value(self.column(idx)?);
        Ok(())
    }
}

fn json_iterator_from(json: &Jsonb, pos: usize) -> crate::Result<IteratorState> {
    let json_element_type = json.element_type_at(pos)?;
    match json_element_type {
        jsonb::ElementType::ARRAY => {
            let iter = json.array_iterator(pos)?;
            Ok(IteratorState::Array(iter))
        }

        jsonb::ElementType::OBJECT => {
            let iter = json.object_iterator(pos)?;
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
        | jsonb::ElementType::TEXTRAW => Ok(IteratorState::Primitive(pos)),
        jsonb::ElementType::RESERVED1
        | jsonb::ElementType::RESERVED2
        | jsonb::ElementType::RESERVED3 => {
            unreachable!("element type not supported: {json_element_type:?}");
        }
    }
}

mod columns {
    use crate::{
        json::{
            element_to_db_type,
            jsonb::{self, ElementType, Jsonb},
            OutputVariant,
        },
        types::Text,
        vdbe::Register,
        LimboError, Value,
    };

    #[derive(Debug)]
    pub(super) enum Key {
        Integer(i64),
        String(String),
        None,
    }

    impl Clone for Key {
        fn clone(&self) -> Self {
            match self {
                Key::Integer(i) => Key::Integer(*i),
                Key::String(s) => Key::String(s.clone()),
                Key::None => Key::None,
            }
        }

        fn clone_from(&mut self, source: &Self) {
            if let (Key::String(target), Key::String(source)) = (&mut *self, source) {
                target.clone_from(source);
                return;
            }
            *self = source.clone();
        }
    }

    impl Key {
        fn empty() -> Self {
            Self::None
        }

        fn key_representation(&self) -> Value {
            match self {
                Key::Integer(ref i) => Value::from_i64(*i),
                Key::String(ref s) => Value::Text(Text::new(s.to_owned().replace("\\\"", "\""))),
                Key::None => Value::Null,
            }
        }
    }

    pub(super) struct Columns {
        key: Key,
        value: usize,
        fullkey_len: usize,
        parent_id: Option<i64>,
        innermost_container_path_len: usize,
    }

    impl Default for Columns {
        fn default() -> Columns {
            Self {
                key: Key::empty(),
                value: 0,
                fullkey_len: 0,
                parent_id: None,
                innermost_container_path_len: 0,
            }
        }
    }

    impl Columns {
        pub(super) fn set(
            &mut self,
            key: &Key,
            value: usize,
            fullkey_len: usize,
            parent_id: Option<i64>,
            innermost_container_path_len: usize,
        ) {
            self.key.clone_from(key);
            self.value = value;
            self.fullkey_len = fullkey_len;
            self.parent_id = parent_id;
            self.innermost_container_path_len = innermost_container_path_len;
        }

        pub(super) fn atom(&self, json: &Jsonb) -> Result<Value, LimboError> {
            Self::atom_at(json, self.value)
        }

        pub(super) fn value(&self, json: &Jsonb) -> Result<Value, LimboError> {
            let element_type = json.element_type_at(self.value)?;
            Ok(match element_type {
                ElementType::ARRAY | ElementType::OBJECT => {
                    let mut text = String::new();
                    json.write_element_text(self.value, &mut text)?;
                    Value::Text(Text::json(text))
                }
                _ => Self::atom_at(json, self.value)?,
            })
        }

        pub(super) fn key(&self) -> Value {
            self.key.key_representation()
        }

        pub(super) fn write_text_atom(
            &self,
            json: &Jsonb,
            dest: &mut Register,
        ) -> Result<bool, LimboError> {
            let Ok((element_type, payload)) = jsonb::element_payload(json.as_slice(), self.value)
            else {
                return Ok(false);
            };
            if !matches!(element_type, ElementType::TEXT | ElementType::TEXTRAW) {
                return Ok(false);
            }
            let Ok(text) = std::str::from_utf8(payload) else {
                return Ok(false);
            };
            match dest {
                Register::Value(Value::Text(existing)) => {
                    existing.replace_with_bytes(text.as_bytes())?
                }
                _ => dest.set_text(Text::new(text.to_string()))?,
            }
            Ok(true)
        }

        fn atom_at(json: &Jsonb, pos: usize) -> Result<Value, LimboError> {
            let (element_type, payload) = jsonb::element_payload(json.as_slice(), pos)?;
            match element_type {
                ElementType::NULL => Ok(Value::Null),
                ElementType::TRUE => Ok(Value::from_i64(1)),
                ElementType::FALSE => Ok(Value::from_i64(0)),
                ElementType::INT => Ok(Value::from_i64(number_text(payload)?.parse::<i64>()?)),
                ElementType::FLOAT => Ok(Value::from_f64(number_text(payload)?.parse::<f64>()?)),
                ElementType::INT5 => {
                    let string = jsonb::element_at(json.as_slice(), pos)?.to_string()?;
                    Ok(Value::from_i64(string.parse::<i64>()?))
                }
                ElementType::FLOAT5 => {
                    let string = jsonb::element_at(json.as_slice(), pos)?.to_string()?;
                    Ok(Value::from_f64(string.parse::<f64>()?))
                }
                ElementType::TEXT
                | ElementType::TEXTJ
                | ElementType::TEXT5
                | ElementType::TEXTRAW => {
                    element_to_db_type(json.as_slice(), pos, OutputVariant::ElementTypePlain)
                }
                ElementType::ARRAY
                | ElementType::OBJECT
                | ElementType::RESERVED1
                | ElementType::RESERVED2
                | ElementType::RESERVED3 => Ok(Value::Null),
            }
        }

        pub(super) fn fullkey(&self, path: &str) -> Value {
            Value::Text(Text::new(path[..self.fullkey_len].to_owned()))
        }

        pub(super) fn path(&self, path: &str) -> Value {
            Value::Text(Text::new(
                path[..self.innermost_container_path_len].to_owned(),
            ))
        }

        pub(super) fn parent(&self) -> Value {
            match self.parent_id {
                Some(id) => Value::from_i64(id),
                None => Value::Null,
            }
        }

        pub(super) fn ttype(&self, json: &Jsonb) -> Value {
            let element_type = json.element_type_at(self.value).expect("invalid value");
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

    fn number_text(payload: &[u8]) -> Result<&str, LimboError> {
        std::str::from_utf8(payload)
            .map_err(|_| LimboError::ParseError("Failed to parse integer".to_string()))
    }
}

struct RootPath {
    text: String,
    path: JsonPath<'static>,
    start: InPlaceJsonPath,
}

impl RootPath {
    fn parse(text: &str) -> crate::Result<Self> {
        let path = json_path(text)?.into_owned();
        let mut start = InPlaceJsonPath::new_root();
        start.reset_to(text, &path);
        Ok(Self {
            text: text.to_owned(),
            path,
            start,
        })
    }
}

struct InPlaceJsonPath {
    string: String,
    element_lengths: Vec<usize>,
    last_element: Key,
}

impl Clone for InPlaceJsonPath {
    fn clone(&self) -> Self {
        Self {
            string: self.string.clone(),
            element_lengths: self.element_lengths.clone(),
            last_element: self.last_element.clone(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.string.clone_from(&source.string);
        self.element_lengths.clone_from(&source.element_lengths);
        self.last_element.clone_from(&source.last_element);
    }
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

    fn push_array_index(&mut self, idx: usize) {
        self.last_element = Key::Integer(idx as i64);
        let start = self.string.len();
        self.string.push('[');
        push_decimal(&mut self.string, idx);
        self.string.push(']');
        self.element_lengths.push(self.string.len() - start);

        fn push_decimal(string: &mut String, mut value: usize) {
            let mut digits = [0u8; 20];
            let mut first = digits.len();
            loop {
                first -= 1;
                digits[first] = b'0' + (value % 10) as u8;
                value /= 10;
                if value == 0 {
                    break;
                }
            }
            string
                .push_str(std::str::from_utf8(&digits[first..]).expect("decimal digits are ASCII"));
        }
    }

    fn push_object_key(&mut self, key: &str) -> crate::Result<()> {
        // This follows SQLite's current quoting scheme, but it is not part of the stable API.
        // See https://sqlite.org/forum/forumpost?udc=1&name=be212a295ed8df4c
        // Keys must be properly quoted strings
        let inner = key
            .strip_prefix('"')
            .and_then(|s| s.strip_suffix('"'))
            .ok_or_else(|| crate::LimboError::ParseError("malformed JSON".to_string()))?;

        let mut chars = inner.chars();
        let needs_quotes = match chars.next() {
            None => true,
            Some(first) => {
                !first.is_ascii_alphabetic() || chars.any(|c| !c.is_ascii_alphanumeric())
            }
        };
        let unquoted_if_necessary = if needs_quotes { key } else { inner };
        match &mut self.last_element {
            Key::String(last_key) => {
                last_key.clear();
                last_key.push_str(inner);
            }
            last_element => *last_element = Key::String(inner.to_owned()),
        }
        let start = self.string.len();
        self.string.push('.');
        self.string.push_str(unquoted_if_necessary);
        self.element_lengths.push(self.string.len() - start);
        Ok(())
    }

    fn cursor(&self) -> InPlaceJsonPathCursor {
        self.string.len()
    }

    fn reset_to_root(&mut self) {
        self.string.clear();
        self.string.push('$');
        self.element_lengths.clear();
        self.element_lengths.push(1);
        self.last_element = Key::None;
    }

    fn reset_to(&mut self, path: &str, json_path: &JsonPath<'_>) {
        self.string.clear();
        self.string.push_str(path);
        self.element_lengths.clear();
        if json_path.elements.is_empty() {
            self.element_lengths.push(1);
            self.last_element = Key::None;
            return;
        }
        self.element_lengths
            .extend(json_path.elements.iter().map(Self::element_length));
        self.last_element = json_path
            .elements
            .last()
            .and_then(|path_element| match path_element {
                PathElement::Key(cow, _) => Some(Key::String(cow.to_string())),
                PathElement::ArrayLocator(Some(idx)) => Some(Key::Integer(*idx as i64)),
                _ => None,
            })
            .unwrap_or(Key::None);
    }

    fn element_length(element: &PathElement) -> usize {
        match element {
            PathElement::Root() => 1,
            PathElement::Key(key, true) => key.len() + 3,
            PathElement::Key(key, false) => key.len() + 1,
            PathElement::ArrayLocator(idx) => {
                let digit_count = successors(*idx, |&n| (n >= 10).then_some(n / 10)).count();
                let bracket_count = 2; // []

                digit_count + bracket_count
            }
            PathElement::BracketQuotedKey(key) => key.len() + 4, // ["..."]
        }
    }

    fn cursor_before_last_element(&self) -> InPlaceJsonPathCursor {
        if self.element_lengths.len() == 1 {
            self.cursor()
        } else {
            self.cursor()
                - self
                    .element_lengths
                    .last()
                    .expect("element_lengths should not be empty in else branch")
        }
    }

    fn key(&self) -> &Key {
        &self.last_element
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_each_writes_text_values_into_the_register_buffer() {
        let mut cursor = JsonEachCursor::empty(JsonTraversalMode::Each);
        assert!(cursor
            .filter(&[Value::build_text(r#"["first","second"]"#)], None, 0)
            .unwrap());
        let mut dest = Register::Value(Value::build_text(String::with_capacity(64)));
        cursor.column_into(COL_VALUE, &mut dest).unwrap();
        assert_eq!(dest.get_value(), &Value::build_text("first"));
        let first_buffer = text_buffer(&dest);

        assert!(cursor.next().unwrap());
        cursor.column_into(COL_VALUE, &mut dest).unwrap();
        assert_eq!(dest.get_value(), &Value::build_text("second"));
        assert_eq!(text_buffer(&dest), first_buffer);
    }

    fn text_buffer(register: &Register) -> *const u8 {
        let Value::Text(text) = register.get_value() else {
            panic!("the register holds text");
        };
        text.as_str().as_ptr()
    }
}
