use crate::sync::{Arc, RwLock};
use std::borrow::Cow;
use std::iter::successors;
use std::result::Result;

use turso_ext::{ConstraintOp, ConstraintUsage, ResultCode};

use crate::{
    json::{
        convert_dbtype_to_jsonb, json_path_text,
        jsonb::{IteratorState, Jsonb, SearchOperation},
        path::{json_path, JsonPath, PathElement},
        vtab::columns::{Columns, Key},
        Conv,
    },
    types::AsValueRef,
    vtab::{column_definition, InternalVirtualTable, InternalVirtualTableCursor},
    Connection, LimboError, Value,
};
use turso_parser::ast;

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

    fn columns(&self) -> Vec<ast::ColumnDefinition> {
        vec![
            // key for current element relative to its parent
            column_definition("key", ""),
            // value for the current element
            column_definition("value", ""),
            // 'object','array','string','integer', etc.
            column_definition("type", ""),
            // value for primitive types, null for array & object
            column_definition("atom", ""),
            // integer ID for this element
            column_definition("id", ""),
            // integer ID for the parent of this element
            column_definition("parent", ""),
            // full path describing the current element
            column_definition("fullkey", ""),
            // path to the container of the current row
            column_definition("path", ""),
            // 1st input parameter: the raw JSON
            column_definition("json", "HIDDEN"),
            // 2nd input parameter: the PATH at which to start
            column_definition("root", "HIDDEN"),
        ]
    }

    fn superuser_only(&self) -> bool {
        false
    }
}

impl std::fmt::Debug for JsonVirtualTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonEachVirtualTable").finish()
    }
}

pub struct JsonEachCursor {
    rowid: i64,
    id: i64,
    root_offset: i64,
    json: Jsonb,
    path_to_current_value: InPlaceJsonPath,
    traversal_states: Vec<TraversalState>,
    columns: Columns,
    traversal_mode: JsonTraversalMode,
    /// Whether the last element of `path_to_current_value` is the element of
    /// the current row, which the next call of `advance` removes. It stays
    /// until then, because the fullkey and path columns read the path.
    pop_path_before_advance: bool,
    /// The key of the current object member. It is a field so that each
    /// member uses the same buffer.
    key_element: Jsonb,
    /// The text of the last root path and the path that it parses to. The
    /// calls of a statement usually have the same root path, which is then
    /// parsed once.
    root_path: Option<(String, JsonPath<'static>)>,
}

struct TraversalState {
    iterator_state: IteratorState,
    parent_id: Option<i64>,
    innermost_container_cursor: InPlaceJsonPathCursor,
}

impl JsonEachCursor {
    fn empty(traversal_mode: JsonTraversalMode) -> Self {
        Self {
            rowid: 0,
            id: 0,
            root_offset: 0,
            json: Jsonb::empty(),
            traversal_states: Vec::new(),
            path_to_current_value: InPlaceJsonPath::new_root(),
            columns: Columns::default(),
            traversal_mode,
            pop_path_before_advance: false,
            key_element: Jsonb::empty(),
            root_path: None,
        }
    }

    fn push_state(
        &mut self,
        iterator_state: IteratorState,
        parent_id: Option<i64>,
        innermost_container_cursor: InPlaceJsonPathCursor,
    ) {
        self.traversal_states.push(TraversalState {
            iterator_state,
            parent_id,
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
        self.pop_path_before_advance = false;
        self.rowid = 0;

        if args.is_empty() || args[0] == Value::Null {
            return Ok(false);
        }
        let root_path = match args.get(1) {
            None => Cow::Borrowed("$"),
            // Like SQLite, a NULL path is an empty result once the JSON has parsed.
            Some(Value::Null) => {
                convert_dbtype_to_jsonb(&args[0], Conv::Strict)?;
                return Ok(false);
            }
            Some(path) => json_path_text(path.as_value_ref()).ok_or_else(|| {
                LimboError::InvalidArgument("root path should be text".to_owned())
            })?,
        };
        if matches!(self.traversal_mode, JsonTraversalMode::Tree)
            && root_path.as_bytes().windows(3).any(|chars| chars == b"[#-")
        {
            return Err(LimboError::InvalidArgument(
                "Json paths with negative indices in json_tree are not supported yet".to_owned(),
            ));
        }

        let jsonb = convert_dbtype_to_jsonb(&args[0], Conv::Strict)?;
        // The path is parsed after the document, so that a bad document
        // gives its error first. The parsed path is for the search and for
        // the path of the cursor.
        if self
            .root_path
            .as_ref()
            .is_none_or(|(text, _)| *text != *root_path)
        {
            let parsed = json_path(&root_path)?.into_owned();
            self.root_path = Some((root_path.to_string(), parsed));
        }
        let Some((_, root_json_path)) = &self.root_path else {
            unreachable!("the root path is set above");
        };

        let (root_json, root_offset, root_id) = if args.len() == 1 {
            (jsonb, 0, 0)
        } else if let Some(found) = navigate_to_path(jsonb, root_json_path)? {
            found
        } else {
            return Ok(false);
        };

        self.json = root_json;
        self.root_offset = root_offset;
        self.id = root_id;
        self.path_to_current_value.set(&root_path, root_json_path);
        let iterator_state = json_iterator_from(&self.json)?;
        let innermost_container_path = if matches!(self.traversal_mode, JsonTraversalMode::Tree)
            && matches!(iterator_state, IteratorState::Primitive(_))
        {
            self.path_to_current_value.cursor_before_last_element()
        } else {
            self.path_to_current_value.cursor()
        };
        self.push_state(iterator_state, Some(root_id), innermost_container_path);

        match self.traversal_mode {
            JsonTraversalMode::Each => self.advance(),
            JsonTraversalMode::Tree => {
                let state = self.peek_state().ok_or_else(|| {
                    crate::LimboError::InternalError("state stack should not be empty".to_string())
                })?;
                if matches!(state.iterator_state, IteratorState::Primitive(_)) {
                    self.advance()
                } else {
                    self.columns.value_mut().copy_from(&self.json)?;
                    self.columns.set_row(
                        true,
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
        self.rowid += 1;
        self.advance()
    }

    fn rowid(&self) -> i64 {
        self.rowid
    }

    fn column(&self, idx: usize) -> Result<Value, LimboError> {
        Ok(match idx {
            COL_KEY => self.columns.key(self.path_to_current_value.key()),
            COL_VALUE => self.columns.value()?,
            COL_TYPE => self.columns.ttype(),
            COL_ATOM => self.columns.atom()?,
            COL_ID => Value::from_i64(self.id),
            COL_PARENT => self.columns.parent(),
            COL_FULLKEY => self.columns.fullkey(&self.path_to_current_value.string),
            COL_PATH => self.columns.path(&self.path_to_current_value.string),
            _ => Value::Null,
        })
    }
}

impl JsonEachCursor {
    fn advance(&mut self) -> Result<bool, LimboError> {
        if std::mem::take(&mut self.pop_path_before_advance) {
            self.path_to_current_value.pop();
        }
        let Some(traversal_state) = self.traversal_states.pop() else {
            return Ok(false);
        };

        let parent_id = if matches!(self.traversal_mode, JsonTraversalMode::Tree) {
            traversal_state.parent_id
        } else {
            None
        };
        match traversal_state.iterator_state {
            IteratorState::Array(state) => {
                let Some((idx, new_state)) = self
                    .json
                    .array_iterator_next(&state, self.columns.value_mut())?
                else {
                    self.path_to_current_value.pop();
                    return self.advance();
                };
                self.id = self.root_offset + state.position() as i64;

                let recursing_iterator = if matches!(self.traversal_mode, JsonTraversalMode::Tree) {
                    self.json
                        .container_property_iterator(&IteratorState::Array(state))
                } else {
                    None
                };
                self.push_state(
                    IteratorState::Array(new_state),
                    traversal_state.parent_id,
                    self.path_to_current_value.cursor(),
                );
                let recurses = recursing_iterator.is_some();
                self.path_to_current_value.push_array_index(&idx);
                if let Some(it) = recursing_iterator {
                    self.push_state(it, Some(self.id), self.path_to_current_value.cursor());
                }

                self.columns.set_row(
                    true,
                    self.path_to_current_value.cursor(),
                    parent_id,
                    traversal_state.innermost_container_cursor,
                );

                self.pop_path_before_advance = !recurses;
            }
            IteratorState::Object(state) => {
                let Some((_idx, new_state)) = self.json.object_iterator_next(
                    &state,
                    &mut self.key_element,
                    self.columns.value_mut(),
                )?
                else {
                    self.path_to_current_value.pop();
                    return self.advance();
                };
                self.id = self.root_offset + state.position() as i64;

                self.push_state(
                    IteratorState::Object(new_state),
                    traversal_state.parent_id,
                    self.path_to_current_value.cursor(),
                );
                self.path_to_current_value
                    .push_object_key(&self.key_element)?;
                let recursing = matches!(self.traversal_mode, JsonTraversalMode::Tree)
                    && self
                        .json
                        .container_property_iterator(&IteratorState::Object(state))
                        .is_some_and(|it| {
                            self.push_state(it, Some(self.id), self.path_to_current_value.cursor());
                            true
                        });

                self.columns.set_row(
                    true,
                    self.path_to_current_value.cursor(),
                    parent_id,
                    traversal_state.innermost_container_cursor,
                );

                self.pop_path_before_advance = !recursing;
            }
            IteratorState::Primitive(jsonb) => {
                // The root of json_each() has no key. The root of
                // json_tree() has the last element of the root path.
                let has_key = matches!(self.traversal_mode, JsonTraversalMode::Tree);
                *self.columns.value_mut() = jsonb;
                self.columns.set_row(
                    has_key,
                    self.path_to_current_value.cursor(),
                    None,
                    traversal_state.innermost_container_cursor,
                );
            }
        };

        Ok(true)
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
/// Returns the element of `jsonb` at `json_path`, in the buffer of `jsonb`,
/// with its offset and its id.
fn navigate_to_path(
    mut jsonb: Jsonb,
    json_path: &JsonPath,
) -> Result<Option<(Jsonb, i64, i64)>, LimboError> {
    let mut search_operation = SearchOperation::locate();
    if jsonb
        .operate_on_path(json_path, &mut search_operation)
        .is_err()
    {
        return Ok(None);
    }
    let value_offset = search_operation.value_offset() as i64;
    let root_id = search_operation
        .label_offset()
        .map_or(value_offset, |offset| offset as i64);
    Ok(Some((
        search_operation.into_element(jsonb),
        value_offset,
        root_id,
    )))
}

mod columns {
    use crate::{
        json::{
            json_ref_to_db_type,
            jsonb::{self, ElementType, Jsonb},
            OutputVariant,
        },
        types::Text,
        LimboError, Value,
    };

    #[derive(Debug, Clone, Copy)]
    pub(super) enum Key<'a> {
        Integer(i64),
        String(&'a str),
        None,
    }

    impl Key<'_> {
        fn key_representation(self) -> Value {
            match self {
                Key::Integer(i) => Value::from_i64(i),
                Key::String(s) => Value::Text(Text::new(s.to_owned())),
                Key::None => Value::Null,
            }
        }
    }

    /// The columns of the current row. The key column is the last element of
    /// the path of the cursor, and the fullkey and path columns are prefixes
    /// of the path. The path stays the same until the cursor advances, so
    /// only their lengths are kept.
    pub(super) struct Columns {
        /// False for the row of a json_each() root, which has no key.
        has_key: bool,
        value: Jsonb,
        fullkey_len: usize,
        parent_id: Option<i64>,
        innermost_container_path_len: usize,
    }

    impl Default for Columns {
        fn default() -> Columns {
            Self {
                has_key: false,
                value: Jsonb::empty(),
                fullkey_len: 0,
                parent_id: None,
                innermost_container_path_len: 0,
            }
        }
    }

    impl Columns {
        /// The value of the current row. The cursor writes each value in
        /// this buffer, so that the rows do not allocate a buffer each.
        pub(super) fn value_mut(&mut self) -> &mut Jsonb {
            &mut self.value
        }

        /// Sets the columns of the current row other than the value.
        pub(super) fn set_row(
            &mut self,
            has_key: bool,
            fullkey_len: usize,
            parent_id: Option<i64>,
            innermost_container_path_len: usize,
        ) {
            self.has_key = has_key;
            self.fullkey_len = fullkey_len;
            self.parent_id = parent_id;
            self.innermost_container_path_len = innermost_container_path_len;
        }

        pub(super) fn atom(&self) -> Result<Value, LimboError> {
            let element_type = self.value.element_type().expect("invalid value");
            Self::atom_from_value(&self.value, element_type)
        }

        pub(super) fn value(&self) -> Result<Value, LimboError> {
            let element_type = self.value.element_type()?;
            Ok(match element_type {
                ElementType::ARRAY | ElementType::OBJECT => {
                    json_ref_to_db_type(&self.value, element_type, OutputVariant::String)?
                }
                _ => Self::atom_from_value(&self.value, element_type)?,
            })
        }

        /// `last_element` is the last element of the path of the cursor.
        pub(super) fn key(&self, last_element: Key<'_>) -> Value {
            if self.has_key {
                last_element.key_representation()
            } else {
                Value::Null
            }
        }

        /// `element_type` is the element type of `value`.
        fn atom_from_value(value: &Jsonb, element_type: ElementType) -> Result<Value, LimboError> {
            let string: Result<Value, LimboError> = match element_type {
                jsonb::ElementType::NULL => Ok(Value::Null),
                jsonb::ElementType::TRUE => Ok(Value::from_i64(1)),
                jsonb::ElementType::FALSE => Ok(Value::from_i64(0)),
                jsonb::ElementType::INT | jsonb::ElementType::INT5 => {
                    json_ref_to_db_type(value, element_type, OutputVariant::ElementType)
                }
                jsonb::ElementType::FLOAT | jsonb::ElementType::FLOAT5 => {
                    Self::jsonb_to_float(value)
                }
                jsonb::ElementType::TEXT
                | jsonb::ElementType::TEXTJ
                | jsonb::ElementType::TEXT5
                | jsonb::ElementType::TEXTRAW => {
                    json_ref_to_db_type(value, element_type, OutputVariant::ElementTypePlain)
                }
                jsonb::ElementType::ARRAY => Ok(Value::Null),
                jsonb::ElementType::OBJECT => Ok(Value::Null),
                jsonb::ElementType::RESERVED1 => Ok(Value::Null),
                jsonb::ElementType::RESERVED2 => Ok(Value::Null),
                jsonb::ElementType::RESERVED3 => Ok(Value::Null),
            };

            string
        }

        fn jsonb_to_float(value: &Jsonb) -> Result<Value, LimboError> {
            // The JSON text of a FLOAT element is its payload, so a UTF-8
            // payload is parsed without the text.
            if value.element_type_at(0).ok() == Some(ElementType::FLOAT) {
                if let Some(text) = value.scalar_payload().and_then(crate::types::validate_utf8) {
                    return Ok(Value::from_f64(text.parse::<f64>()?));
                }
            }
            let string = value.to_string()?;
            let float = string.parse::<f64>()?;

            Ok(Value::from_f64(float))
        }

        /// `path` is the path of the cursor.
        pub(super) fn fullkey(&self, path: &str) -> Value {
            Value::Text(Text::new(path_prefix(path, self.fullkey_len)))
        }

        /// `path` is the path of the cursor.
        pub(super) fn path(&self, path: &str) -> Value {
            Value::Text(Text::new(path_prefix(
                path,
                self.innermost_container_path_len,
            )))
        }

        pub(super) fn parent(&self) -> Value {
            match self.parent_id {
                Some(id) => Value::from_i64(id),
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

    /// The first `len` bytes of the path of the cursor. The lengths come from
    /// the path itself, so they are in range while the cursor is on a row.
    fn path_prefix(path: &str, len: usize) -> String {
        path.get(..len).unwrap_or_default().to_owned()
    }
}

struct InPlaceJsonPath {
    string: String,
    element_lengths: Vec<usize>,
    last_element: LastElement,
    /// The unescaped name of the last element when it is an object member. The buffer stays
    /// when the last element becomes an array index, so that the next name does not allocate.
    last_name: String,
}

/// The kind of the last element of an [`InPlaceJsonPath`].
#[derive(Clone, Copy)]
enum LastElement {
    Index(i64),
    /// An object member, with its name in `last_name`.
    Name,
    None,
}

type InPlaceJsonPathCursor = usize;

impl InPlaceJsonPath {
    fn new_root() -> Self {
        Self {
            string: "$".to_owned(),
            element_lengths: vec![1],
            last_element: LastElement::None,
            last_name: String::new(),
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
        self.last_element = LastElement::Index(*idx as i64);
        let start = self.string.len();
        // write! costs about 300 instructions for an index, in the formatting code.
        let mut digits = [0u8; 20];
        let digits = jsonb::decimal_digits(*idx as u64, &mut digits);
        self.string.push('[');
        self.string
            .extend(digits.iter().map(|&digit| char::from(digit)));
        self.string.push(']');
        self.element_lengths.push(self.string.len() - start);
    }

    fn push_object_key(&mut self, key: &Jsonb) -> crate::Result<()> {
        // A key with no escape is its own name, so it needs no JSON text.
        if let Some(payload) = key.plain_string_payload() {
            let name = jsonb::utf8_lossy(payload);
            self.set_last_element_name(&name);
            self.push_object_name(&name);
            return Ok(());
        }
        let key_text = key.to_string()?;
        // Keys must be properly quoted strings
        let inner = key_text
            .strip_prefix('"')
            .and_then(|s| s.strip_suffix('"'))
            .ok_or_else(|| crate::LimboError::ParseError("malformed JSON".to_string()))?;
        if key.element_type()? != jsonb::ElementType::TEXT5 {
            self.last_name = jsonb::unescape_string(inner);
            self.last_element = LastElement::Name;
            self.push_object_name(inner);
            return Ok(());
        }
        // The JSON text rendering of a key with JSON5 escapes rewrites them (\v becomes \u0009),
        // so read the key from its stored bytes, as the value column does. SQLite copies them.
        let stored = key.scalar_payload_text()?;
        self.last_name = jsonb::unescape_string(&stored);
        self.last_element = LastElement::Name;
        // A raw control character (jsonb_object() stores one where SQLite stores an escape) or a
        // raw quote keeps the rendering: SQLite's path for a raw quote cannot be read back.
        let name = if Self::needs_json_text_rendering(&stored) {
            inner
        } else {
            &stored
        };
        self.push_object_name(name);
        Ok(())
    }

    /// Makes `name` the last element. The name is written in the buffer of
    /// the name before it, if there is one, so that the members of an
    /// object do not allocate a name each.
    fn set_last_element_name(&mut self, name: &str) {
        self.last_name.clear();
        self.last_name.push_str(name);
        self.last_element = LastElement::Name;
    }

    fn push_object_name(&mut self, name: &str) {
        // This follows SQLite's current quoting scheme, but it is not part of the stable API.
        // See https://sqlite.org/forum/forumpost?udc=1&name=be212a295ed8df4c
        let mut chars = name.chars();
        let needs_quotes = match chars.next() {
            None => true,
            Some(first) => {
                !first.is_ascii_alphabetic() || chars.any(|c| !c.is_ascii_alphanumeric())
            }
        };
        let start = self.string.len();
        self.string.push('.');
        if needs_quotes {
            self.string.push('"');
            self.string.push_str(name);
            self.string.push('"');
        } else {
            self.string.push_str(name);
        }
        self.element_lengths.push(self.string.len() - start);
    }

    /// True when a stored name holds a control character or a double quote that is not part of
    /// a backslash escape.
    fn needs_json_text_rendering(name: &str) -> bool {
        let mut bytes = name.bytes().peekable();
        while let Some(byte) = bytes.next() {
            match byte {
                // The byte after a backslash belongs to the escape, even a line break.
                b'\\' => {
                    if bytes.next() == Some(b'\r') {
                        bytes.next_if_eq(&b'\n');
                    }
                }
                b'"' | 0..=0x1F => return true,
                _ => {}
            }
        }
        false
    }

    fn cursor(&self) -> InPlaceJsonPathCursor {
        self.string.len()
    }

    /// Makes this the path `path`, which parses to `json_path`. The path keeps
    /// its buffers, so that the rows of each new document do not allocate
    /// and grow new buffers.
    fn set(&mut self, path: &str, json_path: &JsonPath<'_>) {
        self.string.clear();
        self.string.push_str(path);
        self.element_lengths.clear();
        if json_path.elements.is_empty() {
            self.element_lengths
                .push(Self::element_length(&PathElement::Root()));
            self.last_element = LastElement::None;
            return;
        }
        self.element_lengths
            .extend(json_path.elements.iter().map(Self::element_length));
        match json_path.elements.last() {
            Some(PathElement::Key(name, _)) => self.set_last_element_name(name),
            Some(PathElement::ArrayLocator(Some(idx))) => {
                self.last_element = LastElement::Index(*idx as i64)
            }
            _ => self.last_element = LastElement::None,
        }
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

    fn key(&self) -> Key<'_> {
        match self.last_element {
            LastElement::Index(idx) => Key::Integer(idx),
            LastElement::Name => Key::String(&self.last_name),
            LastElement::None => Key::None,
        }
    }
}
