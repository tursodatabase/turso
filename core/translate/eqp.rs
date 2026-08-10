//! Structured EXPLAIN QUERY PLAN data.
//!
//! Every `Insn::Explain` carries an [`EqpDetail`] describing one step of the
//! query plan. The `Display` impl produces the exact human-readable string
//! shown in `EXPLAIN QUERY PLAN` output, and [`program_plan_json`] serializes
//! the whole plan as JSON so tools don't have to parse those strings.

use std::fmt::{self, Display, Formatter, Write as _};

use crate::{
    schema::Index,
    translate::plan::{
        IterationDirection, JoinInfo, JoinType, JoinedTable, Operation, Scan, Search, SeekDef,
        SeekKeyComponent, SetOperation,
    },
    types::SeekOp,
    vdbe::{insn::Insn, Program},
};

/// How a table is named in the query: real name plus the alias used to refer
/// to it, if different (e.g. `users AS u`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EqpTable {
    pub name: String,
    pub alias: Option<String>,
}

impl EqpTable {
    pub(crate) fn from_joined(table: &JoinedTable) -> Self {
        let name = table.table.get_name();
        Self {
            name: name.to_string(),
            alias: (name != table.identifier).then(|| table.identifier.clone()),
        }
    }

    /// `users AS u` form, used by SCAN and HASH JOIN details.
    fn name_with_alias(&self) -> String {
        match &self.alias {
            Some(alias) => format!("{} AS {}", self.name, alias),
            None => self.name.clone(),
        }
    }

    /// The name the query refers to the table by (alias if present).
    fn identifier(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.name)
    }
}

/// The index used by a scan or search step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EqpIndex {
    pub name: String,
    /// True when the index alone satisfies the query and the table itself is
    /// never read.
    pub covering: bool,
    /// True for synthesized in-memory indexes (e.g. over a materialized
    /// subquery), as opposed to indexes that exist in the schema.
    pub ephemeral: bool,
}

/// How a table participates in the join, as seen from the join order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EqpJoin {
    Inner,
    /// CROSS JOIN: inner join whose position the optimizer must not change.
    Cross,
    Left,
    Full,
    /// EXISTS-style join: keep the outer row once a match is found.
    Semi,
    /// NOT EXISTS-style join: keep the outer row only if no match is found.
    Anti,
}

impl EqpJoin {
    pub fn from_join_info(join_info: Option<&JoinInfo>, in_outer_join_order: bool) -> Option<Self> {
        let info = join_info?;
        Some(if in_outer_join_order {
            if info.is_full_outer() {
                Self::Full
            } else {
                Self::Left
            }
        } else {
            match info.join_type {
                JoinType::Semi => Self::Semi,
                JoinType::Anti => Self::Anti,
                _ if info.no_reorder => Self::Cross,
                _ => Self::Inner,
            }
        })
    }

    /// Whether EXPLAIN QUERY PLAN appends " LEFT-JOIN" for this join.
    fn shows_left_join_suffix(self) -> bool {
        matches!(self, Self::Left | Self::Full)
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Inner => "inner",
            Self::Cross => "cross",
            Self::Left => "left",
            Self::Full => "full",
            Self::Semi => "semi",
            Self::Anti => "anti",
        }
    }
}

/// What kind of row source a SCAN step reads from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EqpRowSource {
    BTreeTable,
    VirtualTable,
    Subquery,
    /// The one-row input consumed by the recursive part of a recursive CTE.
    RecursiveCteInput,
}

impl EqpRowSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::BTreeTable => "table",
            Self::VirtualTable => "virtual_table",
            Self::Subquery => "subquery",
            Self::RecursiveCteInput => "recursive_cte_input",
        }
    }
}

/// How a FROM-clause subquery (or CTE reference) is executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EqpSubqueryExec {
    /// Runs interleaved with the outer query, yielding one row at a time.
    Coroutine,
    /// Fully evaluated into an in-memory table before the outer query runs.
    Materialized,
    /// Fully evaluated into an in-memory index that the outer query seeks into.
    IndexedMaterialized,
    /// Reads a result someone else already materialized (shared CTE).
    MaterializedReuse,
}

impl EqpSubqueryExec {
    fn as_str(self) -> &'static str {
        match self {
            Self::Coroutine => "coroutine",
            Self::Materialized => "materialized",
            Self::IndexedMaterialized => "indexed_materialized",
            Self::MaterializedReuse => "materialized_reuse",
        }
    }
}

/// Extra information attached to scan/search steps that read from a
/// FROM-clause subquery or CTE reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EqpSubquery {
    pub exec: EqpSubqueryExec,
    /// Identity shared by all references to the same CTE, so tools can link
    /// multiple reads of one materialized CTE together.
    pub cte_id: Option<usize>,
    /// True when the subquery is a recursive CTE.
    pub recursive: bool,
}

/// Which seek shape a SEARCH step uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EqpSearchKind {
    /// Single-row lookup by exact rowid.
    RowidEq,
    /// Seek to a key and scan a range.
    Seek,
    /// Seek once per value produced by an IN (...) list or subquery.
    InSeek,
}

impl EqpSearchKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::RowidEq => "rowid_eq",
            Self::Seek => "seek",
            Self::InSeek => "in_seek",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EqpSortMethod {
    /// An in-memory B-tree that keeps rows ordered as they are inserted.
    TempBTree,
    /// An external-merge sorter.
    Sorter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EqpCompoundOp {
    UnionAll,
    Union,
    Intersect,
    Except,
    /// The first arm of a compound select.
    LeftMost,
}

impl EqpCompoundOp {
    fn as_str(self) -> &'static str {
        match self {
            Self::UnionAll => "union_all",
            Self::Union => "union",
            Self::Intersect => "intersect",
            Self::Except => "except",
            Self::LeftMost => "left_most",
        }
    }
}

/// One structured EXPLAIN QUERY PLAN step. `Display` renders the exact detail
/// string EXPLAIN QUERY PLAN prints for it.
#[derive(Debug, Clone, PartialEq)]
pub enum EqpDetail {
    /// A query with no FROM clause produces exactly one row.
    ConstantRow,
    /// Iterate all rows of a table, index, or subquery result.
    Scan {
        table: EqpTable,
        index: Option<EqpIndex>,
        source: EqpRowSource,
        backwards: bool,
        join: Option<EqpJoin>,
        subquery: Option<EqpSubquery>,
    },
    /// Seek into a table or index using key constraints.
    Search {
        table: EqpTable,
        kind: EqpSearchKind,
        /// None means the search uses the table's integer primary key.
        index: Option<EqpIndex>,
        /// Human-readable key constraints, e.g. `["x=?", "y>?"]`.
        constraints: Vec<String>,
        backwards: bool,
        join: Option<EqpJoin>,
        subquery: Option<EqpSubquery>,
    },
    /// Combine rowid sets from several indexes with OR/AND before fetching rows.
    MultiIndex {
        table: EqpTable,
        /// True combines with OR (union of rowids), false with AND (intersection).
        union: bool,
        /// Index names; "PRIMARY KEY" stands in for the table's rowid index.
        indexes: Vec<String>,
    },
    /// Delegate the access to a pluggable index method.
    IndexMethod {
        method: String,
    },
    /// Probe a hash table built from another table's rows.
    HashJoin {
        table: EqpTable,
        join: Option<EqpJoin>,
        subquery: Option<EqpSubquery>,
    },
    /// Materialize the build side of a hash join into an in-memory table.
    HashBuild {
        table: EqpTable,
    },
    /// De-duplicate result rows with an in-memory hash table.
    Distinct,
    /// De-duplicate one aggregate's input, e.g. count(DISTINCT x).
    DistinctAggregate {
        function: String,
    },
    OrderBy {
        method: EqpSortMethod,
    },
    GroupBy {
        method: EqpSortMethod,
    },
    /// Parent of the arms of a UNION/INTERSECT/EXCEPT query.
    Compound,
    /// One arm of a compound select.
    CompoundArm {
        op: EqpCompoundOp,
        /// True when the arm's rows go through a de-duplicating in-memory
        /// B-tree (UNION/INTERSECT/EXCEPT).
        temp_btree: bool,
    },
    /// An IN (SELECT ...) subquery materialized into an in-memory index.
    ListSubquery {
        id: usize,
        correlated: bool,
    },
    /// A subquery producing a single value (or row).
    ScalarSubquery {
        id: usize,
        correlated: bool,
    },
    /// The initial (non-recursive) part of a recursive CTE.
    RecursiveSetup,
    /// The recursive part of a recursive CTE; runs once per queued row.
    RecursiveStep,
}

impl Display for EqpDetail {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConstantRow => write!(f, "SCAN CONSTANT ROW"),
            Self::Scan { table, index, .. } => {
                write!(f, "SCAN {}", table.name_with_alias())?;
                if let Some(index) = index {
                    if index.covering {
                        write!(f, " USING COVERING INDEX {}", index.name)?;
                    } else {
                        write!(f, " USING INDEX {}", index.name)?;
                    }
                }
                Ok(())
            }
            Self::Search {
                table,
                index,
                constraints,
                join,
                ..
            } => {
                write!(f, "SEARCH {}", table.identifier())?;
                match index {
                    Some(index) if index.covering => {
                        write!(f, " USING COVERING INDEX {}", index.name)?
                    }
                    Some(index) => write!(f, " USING INDEX {}", index.name)?,
                    None => write!(f, " USING INTEGER PRIMARY KEY")?,
                }
                if !constraints.is_empty() {
                    write!(f, " ({})", constraints.join(" AND "))?;
                }
                if join.is_some_and(|join| join.shows_left_join_suffix()) {
                    write!(f, " LEFT-JOIN")?;
                }
                Ok(())
            }
            Self::MultiIndex {
                table,
                union,
                indexes,
            } => {
                let op = if *union { "OR" } else { "AND" };
                write!(
                    f,
                    "MULTI-INDEX {op} {} ({})",
                    table.identifier(),
                    indexes.join(", ")
                )
            }
            Self::IndexMethod { method } => write!(f, "QUERY INDEX METHOD {method}"),
            Self::HashJoin { table, .. } => write!(f, "HASH JOIN {}", table.name_with_alias()),
            Self::HashBuild { table } => write!(
                f,
                "MATERIALIZE hash build input for {}",
                table.name_with_alias()
            ),
            Self::Distinct => write!(f, "USE HASH TABLE FOR DISTINCT"),
            Self::DistinctAggregate { function } => {
                write!(f, "USE HASH TABLE FOR {function}(DISTINCT)")
            }
            Self::OrderBy { method } => match method {
                EqpSortMethod::TempBTree => write!(f, "USE TEMP B-TREE FOR ORDER BY"),
                EqpSortMethod::Sorter => write!(f, "USE SORTER FOR ORDER BY"),
            },
            Self::GroupBy { .. } => write!(f, "USE SORTER FOR GROUP BY"),
            Self::Compound => write!(f, "COMPOUND QUERY"),
            Self::CompoundArm { op, .. } => match op {
                EqpCompoundOp::UnionAll => write!(f, "UNION ALL"),
                EqpCompoundOp::Union => write!(f, "UNION USING TEMP B-TREE"),
                EqpCompoundOp::Intersect => write!(f, "INTERSECT USING TEMP B-TREE"),
                EqpCompoundOp::Except => write!(f, "EXCEPT USING TEMP B-TREE"),
                EqpCompoundOp::LeftMost => write!(f, "LEFT-MOST SUBQUERY"),
            },
            Self::ListSubquery { id, correlated } => {
                let prefix = if *correlated { "CORRELATED " } else { "" };
                write!(f, "{prefix}LIST SUBQUERY {id}")
            }
            Self::ScalarSubquery { id, correlated } => {
                let prefix = if *correlated { "CORRELATED " } else { "" };
                write!(f, "{prefix}SCALAR SUBQUERY {id}")
            }
            Self::RecursiveSetup => write!(f, "SETUP"),
            Self::RecursiveStep => write!(f, "RECURSIVE STEP"),
        }
    }
}

/// Build the human-readable key constraints for an index seek,
/// e.g. `["label=?", "fromId>?"]`.
pub(crate) fn seek_constraint_parts(index: &Index, seek_def: &SeekDef) -> Vec<String> {
    let mut parts = Vec::new();
    // Equality prefix constraints
    for (i, _constraint) in seek_def.prefix.iter().enumerate() {
        if let Some(col) = index.columns.get(i) {
            parts.push(format!("{}=?", col.name));
        }
    }
    // Range constraint from start key
    let range_col_idx = seek_def.prefix.len();
    if let SeekKeyComponent::Expr(_) = &seek_def.start.last_component {
        if let Some(col) = index.columns.get(range_col_idx) {
            let op_str = match seek_def.start.op {
                SeekOp::GE { .. } => ">=",
                SeekOp::GT => ">",
                SeekOp::LE { .. } => "<=",
                SeekOp::LT => "<",
            };
            parts.push(format!("{}{op_str}?", col.name));
        }
    }
    // Range constraint from end key.
    // The end key's SeekOp is the B-tree termination condition (the negation of the
    // user-facing SQL operator), so we reverse it for display.
    if let SeekKeyComponent::Expr(_) = &seek_def.end.last_component {
        if let Some(col) = index.columns.get(range_col_idx) {
            let op_str = match seek_def.end.op {
                SeekOp::GE { .. } => "<",
                SeekOp::GT => "<=",
                SeekOp::LE { .. } => ">",
                SeekOp::LT => ">=",
            };
            parts.push(format!("{}{op_str}?", col.name));
        }
    }
    parts
}

fn eqp_index(index: &Index, covering: bool) -> EqpIndex {
    EqpIndex {
        name: index.name.clone(),
        covering,
        ephemeral: index.ephemeral,
    }
}

/// Build the structured detail for a joined table's access step. Shared by the
/// SELECT loop emitter and the UPDATE/DELETE emitters.
pub(crate) fn eqp_detail_for_table_op(
    table: &JoinedTable,
    join: Option<EqpJoin>,
    subquery: Option<EqpSubquery>,
) -> EqpDetail {
    let eqp_table = EqpTable::from_joined(table);
    match &table.op {
        Operation::Scan(scan) => {
            let (index, source, backwards) = match scan {
                Scan::BTreeTable { iter_dir, index } => (
                    index
                        .as_ref()
                        .map(|index| eqp_index(index, table.utilizes_covering_index())),
                    EqpRowSource::BTreeTable,
                    *iter_dir == IterationDirection::Backwards,
                ),
                Scan::VirtualTable { .. } => (None, EqpRowSource::VirtualTable, false),
                Scan::Subquery { iter_dir } => (
                    None,
                    EqpRowSource::Subquery,
                    *iter_dir == IterationDirection::Backwards,
                ),
                Scan::RecursiveCteInput => (None, EqpRowSource::RecursiveCteInput, false),
            };
            EqpDetail::Scan {
                table: eqp_table,
                index,
                source,
                backwards,
                join,
                subquery,
            }
        }
        Operation::Search(search) => {
            let (kind, index, constraints, backwards) = match search {
                Search::RowidEq { .. } => (
                    EqpSearchKind::RowidEq,
                    None,
                    vec!["rowid=?".to_string()],
                    false,
                ),
                Search::Seek { index, seek_def } => (
                    EqpSearchKind::Seek,
                    index
                        .as_ref()
                        .map(|index| eqp_index(index, table.utilizes_covering_index())),
                    match index {
                        Some(index) => seek_constraint_parts(index, seek_def),
                        None => vec!["rowid=?".to_string()],
                    },
                    seek_def.iter_dir == IterationDirection::Backwards,
                ),
                Search::InSeek { index, .. } => (
                    EqpSearchKind::InSeek,
                    index
                        .as_ref()
                        .map(|index| eqp_index(index, table.utilizes_covering_index())),
                    match index {
                        Some(index) => index
                            .columns
                            .first()
                            .map(|col| vec![format!("{}=?", col.name)])
                            .unwrap_or_default(),
                        None => vec!["rowid=?".to_string()],
                    },
                    false,
                ),
            };
            EqpDetail::Search {
                table: eqp_table,
                kind,
                index,
                constraints,
                backwards,
                join,
                subquery,
            }
        }
        Operation::MultiIndexScan(multi_idx) => EqpDetail::MultiIndex {
            table: eqp_table,
            union: matches!(multi_idx.set_op, SetOperation::Union),
            indexes: multi_idx
                .branches
                .iter()
                .map(|b| {
                    b.index
                        .as_ref()
                        .map(|i| i.name.clone())
                        .unwrap_or_else(|| "PRIMARY KEY".to_string())
                })
                .collect(),
        },
        Operation::IndexMethodQuery(query) => EqpDetail::IndexMethod {
            method: query
                .index
                .index_method
                .as_ref()
                .unwrap()
                .definition()
                .method_name
                .to_string(),
        },
        Operation::HashJoin(_) => EqpDetail::HashJoin {
            table: eqp_table,
            join,
            subquery,
        },
    }
}

/// A shared CTE materialized before the main query runs. Links the plan nodes
/// that compute the CTE's rows to the `cte_id` its readers reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EqpCteMaterialization {
    pub cte_id: usize,
    pub name: String,
    /// Ids of the plan nodes (including nested ones) emitted while materializing this CTE.
    pub node_ids: Vec<usize>,
}

fn json_escape_into(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Incrementally writes one JSON object, handling commas and escaping.
struct JsonObject<'a> {
    out: &'a mut String,
    first: bool,
}

impl<'a> JsonObject<'a> {
    fn new(out: &'a mut String) -> Self {
        out.push('{');
        Self { out, first: true }
    }

    fn key(&mut self, key: &str) -> &mut String {
        if !self.first {
            self.out.push(',');
        }
        self.first = false;
        json_escape_into(self.out, key);
        self.out.push(':');
        self.out
    }

    fn str(&mut self, key: &str, value: &str) {
        let out = self.key(key);
        json_escape_into(out, value);
    }

    fn opt_str(&mut self, key: &str, value: Option<&str>) {
        if let Some(value) = value {
            self.str(key, value);
        }
    }

    fn num(&mut self, key: &str, value: usize) {
        let _ = write!(self.key(key), "{value}");
    }

    fn bool(&mut self, key: &str, value: bool) {
        let _ = write!(self.key(key), "{value}");
    }

    fn str_array(&mut self, key: &str, values: impl IntoIterator<Item = impl AsRef<str>>) {
        let out = self.key(key);
        out.push('[');
        for (i, value) in values.into_iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            json_escape_into(out, value.as_ref());
        }
        out.push(']');
    }

    fn num_array(&mut self, key: &str, values: impl IntoIterator<Item = usize>) {
        let out = self.key(key);
        out.push('[');
        for (i, value) in values.into_iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            let _ = write!(out, "{value}");
        }
        out.push(']');
    }

    fn finish(self) {
        self.out.push('}');
    }
}

impl EqpDetail {
    fn write_table_fields(
        obj: &mut JsonObject,
        table: &EqpTable,
        join: Option<EqpJoin>,
        subquery: Option<&EqpSubquery>,
    ) {
        obj.str("table", &table.name);
        obj.opt_str("alias", table.alias.as_deref());
        obj.opt_str("join", join.map(EqpJoin::as_str));
        if let Some(subquery) = subquery {
            let mut sub = JsonObject::new(obj.key("subquery"));
            sub.str("execution", subquery.exec.as_str());
            if let Some(cte_id) = subquery.cte_id {
                sub.num("cte_id", cte_id);
            }
            if subquery.recursive {
                sub.bool("recursive", true);
            }
            sub.finish();
        }
    }

    fn write_index_field(obj: &mut JsonObject, index: Option<&EqpIndex>) {
        if let Some(index) = index {
            let mut idx = JsonObject::new(obj.key("index"));
            idx.str("name", &index.name);
            idx.bool("covering", index.covering);
            idx.bool("ephemeral", index.ephemeral);
            idx.finish();
        }
    }

    /// Write this step's machine-readable fields as one JSON object.
    fn write_json(&self, out: &mut String) {
        let mut obj = JsonObject::new(out);
        match self {
            Self::ConstantRow => obj.str("type", "constant_row"),
            Self::Scan {
                table,
                index,
                source,
                backwards,
                join,
                subquery,
            } => {
                obj.str("type", "scan");
                Self::write_table_fields(&mut obj, table, *join, subquery.as_ref());
                obj.str("source", source.as_str());
                Self::write_index_field(&mut obj, index.as_ref());
                if *backwards {
                    obj.bool("backwards", true);
                }
            }
            Self::Search {
                table,
                kind,
                index,
                constraints,
                backwards,
                join,
                subquery,
            } => {
                obj.str("type", "search");
                Self::write_table_fields(&mut obj, table, *join, subquery.as_ref());
                obj.str("search_kind", kind.as_str());
                Self::write_index_field(&mut obj, index.as_ref());
                if index.is_none() {
                    obj.bool("integer_primary_key", true);
                }
                obj.str_array("constraints", constraints);
                if *backwards {
                    obj.bool("backwards", true);
                }
            }
            Self::MultiIndex {
                table,
                union,
                indexes,
            } => {
                obj.str("type", "multi_index");
                Self::write_table_fields(&mut obj, table, None, None);
                obj.str("set_op", if *union { "or" } else { "and" });
                obj.str_array("indexes", indexes);
            }
            Self::IndexMethod { method } => {
                obj.str("type", "index_method");
                obj.str("method", method);
            }
            Self::HashJoin {
                table,
                join,
                subquery,
            } => {
                obj.str("type", "hash_join");
                Self::write_table_fields(&mut obj, table, *join, subquery.as_ref());
            }
            Self::HashBuild { table } => {
                obj.str("type", "hash_build");
                Self::write_table_fields(&mut obj, table, None, None);
            }
            Self::Distinct => obj.str("type", "distinct"),
            Self::DistinctAggregate { function } => {
                obj.str("type", "distinct_aggregate");
                obj.str("function", function);
            }
            Self::OrderBy { method } => {
                obj.str("type", "order_by");
                obj.str(
                    "method",
                    match method {
                        EqpSortMethod::TempBTree => "temp_btree",
                        EqpSortMethod::Sorter => "sorter",
                    },
                );
            }
            Self::GroupBy { .. } => {
                obj.str("type", "group_by");
                obj.str("method", "sorter");
            }
            Self::Compound => obj.str("type", "compound"),
            Self::CompoundArm { op, temp_btree } => {
                obj.str("type", "compound_arm");
                obj.str("op", op.as_str());
                obj.bool("temp_btree", *temp_btree);
            }
            Self::ListSubquery { id, correlated } => {
                obj.str("type", "list_subquery");
                obj.num("subquery_id", *id);
                obj.bool("correlated", *correlated);
            }
            Self::ScalarSubquery { id, correlated } => {
                obj.str("type", "scalar_subquery");
                obj.num("subquery_id", *id);
                obj.bool("correlated", *correlated);
            }
            Self::RecursiveSetup => obj.str("type", "recursive_setup"),
            Self::RecursiveStep => obj.str("type", "recursive_step"),
        }
        obj.finish();
    }
}

/// Serialize a program's EXPLAIN QUERY PLAN tree as JSON.
///
/// The program must have been prepared in EXPLAIN QUERY PLAN mode; otherwise
/// there are no plan steps to report and the node list is empty.
///
/// Output shape:
/// ```json
/// {
///   "version": 1,
///   "sql": "...",
///   "result_columns": ["a", "b"],
///   "nodes": [
///     {"id": 3, "parent": null, "detail": "SCAN users", "op": {"type": "scan", ...}}
///   ],
///   "cte_materializations": [{"cte_id": 1, "name": "spenders", "nodes": [4, 7]}]
/// }
/// ```
/// `parent` refers to another node's `id`; `null` marks a root node.
pub fn program_plan_json(program: &Program) -> String {
    let mut out = String::with_capacity(1024);
    let mut top = JsonObject::new(&mut out);
    top.num("version", 1);
    top.str("sql", &program.sql);

    let column_names: Vec<String> = program
        .result_columns
        .iter()
        .enumerate()
        .map(|(i, col)| match col.name(&program.table_references) {
            Some(name) => name.to_string(),
            None => format!("column{}", i + 1),
        })
        .collect();
    top.str_array("result_columns", &column_names);

    let nodes = top.key("nodes");
    nodes.push('[');
    let mut first = true;
    for (insn, _) in &program.insns {
        let Insn::Explain { p1, p2, detail } = insn else {
            continue;
        };
        if !first {
            nodes.push(',');
        }
        first = false;
        let mut node = JsonObject::new(nodes);
        node.num("id", *p1);
        match p2 {
            Some(parent) => node.num("parent", *parent),
            None => node.key("parent").push_str("null"),
        }
        node.str("detail", &detail.to_string());
        detail.write_json(node.key("op"));
        node.finish();
    }
    nodes.push(']');

    if !program.cte_materializations.is_empty() {
        let ctes = top.key("cte_materializations");
        ctes.push('[');
        for (i, cte) in program.cte_materializations.iter().enumerate() {
            if i > 0 {
                ctes.push(',');
            }
            let mut obj = JsonObject::new(ctes);
            obj.num("cte_id", cte.cte_id);
            obj.str("name", &cte.name);
            obj.num_array("nodes", cte.node_ids.iter().copied());
            obj.finish();
        }
        ctes.push(']');
    }

    top.finish();
    out
}
