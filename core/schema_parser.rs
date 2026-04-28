use crate::numeric::Numeric;
use crate::schema::Schema;
use crate::storage::btree::CursorTrait;
use crate::types::IOResult;
use crate::types::ImmutableRecord;
use crate::util::UnparsedFromSqlIndex;
use crate::{LimboError, Result, SymbolTable, ValueRef};
use rustc_hash::{FxBuildHasher, FxHashMap as HashMap};
use std::str::FromStr;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    strum_macros::Display,
    strum_macros::EnumString,
    strum_macros::IntoStaticStr,
)]
#[strum(ascii_case_insensitive, serialize_all = "snake_case")]
pub enum SchemaTableType {
    Table,
    Index,
    View,
    Trigger,
}

#[derive(Debug, Clone)]
pub enum ParseSchemaFilter {
    All,
    TableNameNotTrigger { table_names: Vec<String> },
    Name { names: Vec<String> },
    NameAndType { name: String, ty: SchemaTableType },
}

impl ParseSchemaFilter {
    pub(crate) fn matches(&self, ty: SchemaTableType, name: &str, table_name: &str) -> bool {
        match self {
            Self::All => true,
            Self::TableNameNotTrigger { table_names } => {
                ty != SchemaTableType::Trigger
                    && table_names.iter().any(|candidate| candidate == table_name)
            }
            Self::Name { names } => names.iter().any(|candidate| candidate == name),
            Self::NameAndType {
                name: candidate,
                ty: candidate_ty,
            } => candidate == name && *candidate_ty == ty,
        }
    }

    pub(crate) fn matches_row(&self, row: &SchemaTableRow) -> bool {
        self.matches(row.ty, &row.name, &row.table_name)
    }

    pub fn explain(&self) -> String {
        match self {
            Self::All => "ALL".to_string(),
            Self::TableNameNotTrigger { table_names } => table_names
                .iter()
                .map(|table_name| {
                    format!(
                        "tbl_name = {} AND type != 'trigger'",
                        quote_schema_value(table_name)
                    )
                })
                .collect::<Vec<_>>()
                .join(" OR "),
            Self::Name { names } => names
                .iter()
                .map(|name| format!("name = {}", quote_schema_value(name)))
                .collect::<Vec<_>>()
                .join(" OR "),
            Self::NameAndType { name, ty } => format!(
                "name = {} AND type = {}",
                quote_schema_value(name),
                quote_schema_value((*ty).into())
            ),
        }
    }
}

fn quote_schema_value(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[derive(Clone, Copy)]
pub(crate) enum SchemaTableDecodeErrorKind {
    Conversion,
    Corrupt,
}

pub(crate) struct SchemaTableRow {
    pub ty: SchemaTableType,
    pub name: String,
    pub table_name: String,
    pub root_page: i64,
    pub sql: Option<String>,
}

impl SchemaTableRow {
    pub(crate) fn from_record(
        record: &ImmutableRecord,
        error_kind: SchemaTableDecodeErrorKind,
    ) -> Result<Self> {
        if record.column_count() < 5 {
            return Err(schema_table_error(
                error_kind,
                format!(
                    "sqlite_schema row must have at least 5 columns, got {}",
                    record.column_count()
                ),
                "Expected at least 5 columns in sqlite_schema".to_string(),
            ));
        }

        let ty = schema_type_column(record, error_kind)?;
        let name = text_column(record, 1, "name", error_kind)?;
        let table_name = text_column(record, 2, "tbl_name", error_kind)?;
        let root_page = integer_column(record, 3, "rootpage", error_kind)?;
        let sql = match record.get_value_opt(4) {
            Some(ValueRef::Text(sql)) => Some(sql.as_str()),
            _ => None,
        };

        Ok(Self {
            ty,
            name: name.to_string(),
            table_name: table_name.to_string(),
            root_page,
            sql: sql.map(ToString::to_string),
        })
    }

    pub(crate) fn has_btree(&self) -> bool {
        match self.ty {
            SchemaTableType::Index => true,
            SchemaTableType::Table => !self.is_virtual_table(),
            SchemaTableType::View | SchemaTableType::Trigger => false,
        }
    }

    fn is_virtual_table(&self) -> bool {
        self.sql
            .as_deref()
            .is_some_and(|sql| contains_ignore_ascii_case(sql.as_bytes(), b"create virtual"))
    }
}

fn contains_ignore_ascii_case(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle))
}

fn schema_type_column(
    record: &ImmutableRecord,
    error_kind: SchemaTableDecodeErrorKind,
) -> Result<SchemaTableType> {
    let value = text_column(record, 0, "type", error_kind)?;
    SchemaTableType::from_str(value).map_err(|_| {
        schema_table_error(
            error_kind,
            format!("sqlite_schema type is invalid: {value}"),
            format!("Invalid sqlite_schema.type value: {value}"),
        )
    })
}

#[derive(Debug)]
enum SchemaTableCursorPhase {
    Rewinding,
    FetchingRecord,
    Advancing,
    Done,
}

pub(crate) struct SchemaTableCursor {
    cursor: Box<dyn CursorTrait>,
    phase: SchemaTableCursorPhase,
    error_kind: SchemaTableDecodeErrorKind,
}

impl SchemaTableCursor {
    pub(crate) fn new(
        cursor: Box<dyn CursorTrait>,
        error_kind: SchemaTableDecodeErrorKind,
    ) -> Self {
        Self {
            cursor,
            phase: SchemaTableCursorPhase::Rewinding,
            error_kind,
        }
    }

    pub(crate) fn next_row(&mut self) -> Result<IOResult<Option<SchemaTableRow>>> {
        loop {
            match self.phase {
                SchemaTableCursorPhase::Rewinding => match self.cursor.rewind()? {
                    IOResult::Done(()) => self.phase = SchemaTableCursorPhase::FetchingRecord,
                    IOResult::IO(io) => return Ok(IOResult::IO(io)),
                },
                SchemaTableCursorPhase::FetchingRecord => {
                    let record = match self.cursor.record()? {
                        IOResult::Done(record) => record,
                        IOResult::IO(io) => return Ok(IOResult::IO(io)),
                    };
                    let Some(record) = record else {
                        self.phase = SchemaTableCursorPhase::Done;
                        return Ok(IOResult::Done(None));
                    };
                    let row = SchemaTableRow::from_record(record, self.error_kind)?;
                    self.phase = SchemaTableCursorPhase::Advancing;
                    return Ok(IOResult::Done(Some(row)));
                }
                SchemaTableCursorPhase::Advancing => match self.cursor.next()? {
                    IOResult::Done(()) => {
                        self.phase = if self.cursor.has_record() {
                            SchemaTableCursorPhase::FetchingRecord
                        } else {
                            SchemaTableCursorPhase::Done
                        };
                    }
                    IOResult::IO(io) => return Ok(IOResult::IO(io)),
                },
                SchemaTableCursorPhase::Done => return Ok(IOResult::Done(None)),
            }
        }
    }
}

fn text_column<'a>(
    record: &'a ImmutableRecord,
    column: usize,
    column_name: &str,
    error_kind: SchemaTableDecodeErrorKind,
) -> Result<&'a str> {
    match record.get_value_opt(column) {
        Some(ValueRef::Text(value)) => Ok(value.as_str()),
        _ => Err(schema_table_error(
            error_kind,
            format!("sqlite_schema {column_name} must be text"),
            format!("Expected text value for sqlite_schema.{column_name}"),
        )),
    }
}

fn integer_column(
    record: &ImmutableRecord,
    column: usize,
    column_name: &str,
    error_kind: SchemaTableDecodeErrorKind,
) -> Result<i64> {
    match record.get_value_opt(column) {
        Some(ValueRef::Numeric(Numeric::Integer(value))) => Ok(value),
        _ => Err(schema_table_error(
            error_kind,
            format!("sqlite_schema {column_name} must be integer"),
            format!("Expected integer value for sqlite_schema.{column_name}"),
        )),
    }
}

fn schema_table_error(
    error_kind: SchemaTableDecodeErrorKind,
    corrupt_message: String,
    conversion_message: String,
) -> LimboError {
    match error_kind {
        SchemaTableDecodeErrorKind::Conversion => LimboError::ConversionError(conversion_message),
        SchemaTableDecodeErrorKind::Corrupt => LimboError::Corrupt(corrupt_message),
    }
}

/// Accumulates schema rows whose final objects depend on entries parsed later in sqlite_schema.
pub(crate) struct SchemaTableParser {
    from_sql_indexes: Vec<UnparsedFromSqlIndex>,
    automatic_indices: HashMap<String, Vec<(String, i64)>>,
    dbsp_state_roots: HashMap<String, i64>,
    dbsp_state_index_roots: HashMap<String, i64>,
    materialized_view_info: HashMap<String, (String, i64)>,
}

impl Default for SchemaTableParser {
    fn default() -> Self {
        Self {
            from_sql_indexes: Vec::with_capacity(10),
            automatic_indices: HashMap::with_capacity_and_hasher(10, FxBuildHasher),
            dbsp_state_roots: HashMap::default(),
            dbsp_state_index_roots: HashMap::default(),
            materialized_view_info: HashMap::default(),
        }
    }
}

impl SchemaTableParser {
    pub(crate) fn parse_row(
        &mut self,
        schema: &mut Schema,
        row: &SchemaTableRow,
        syms: &SymbolTable,
        resolve_attached_db: &dyn Fn(&str) -> Option<usize>,
    ) -> Result<()> {
        schema.handle_schema_row(
            row.ty,
            &row.name,
            &row.table_name,
            row.root_page,
            row.sql.as_deref(),
            syms,
            &mut self.from_sql_indexes,
            &mut self.automatic_indices,
            &mut self.dbsp_state_roots,
            &mut self.dbsp_state_index_roots,
            &mut self.materialized_view_info,
            resolve_attached_db,
        )
    }

    pub(crate) fn finish(
        self,
        schema: &mut Schema,
        syms: &SymbolTable,
        mvcc_enabled: bool,
    ) -> Result<()> {
        schema.populate_indices(
            syms,
            self.from_sql_indexes,
            self.automatic_indices,
            mvcc_enabled,
        )?;
        schema.populate_materialized_views(
            self.materialized_view_info,
            self.dbsp_state_roots,
            self.dbsp_state_index_roots,
        )
    }
}
