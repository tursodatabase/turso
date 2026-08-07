//! A machine-readable form of `EXPLAIN QUERY PLAN`.
//!
//! `EXPLAIN QUERY PLAN` returns one text line per plan step, e.g.
//! `SEARCH users AS u USING INDEX idx_age (age>?)`. That text is fine to read
//! but painful to consume from a tool: every consumer ends up re-parsing
//! English, and the wording changes break it.
//!
//! So the emitters build a [`PlanOp`] instead of a string, and the text is
//! derived from it via [`Display`]. The structured form is the source of
//! truth, the EQP text is one rendering of it, and the two cannot drift.
//! [`QueryPlan::to_json`] gives the same tree as JSON for external tools.

use std::fmt::{self, Display, Formatter, Write as _};

/// A whole query plan: the SQL it came from plus a flat list of nodes that
/// point at their parents.
///
/// The list is in emission order, which means a node always appears after its
/// parent. Building a tree is a single pass.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryPlan {
    /// The statement this plan was built for.
    pub sql: String,
    pub nodes: Vec<PlanNode>,
}

/// One step of a query plan.
#[derive(Debug, Clone, PartialEq)]
pub struct PlanNode {
    /// Address of the `Explain` instruction that produced this node. This is
    /// the same id `EXPLAIN QUERY PLAN` reports in its first column.
    pub id: usize,
    /// Id of the node this one runs under, or `None` for a top-level node.
    pub parent_id: Option<usize>,
    pub op: PlanOp,
}

/// What a plan step actually does.
#[derive(Debug, Clone, PartialEq)]
pub enum PlanOp {
    /// Read every row of a table, optionally walking an index instead of the
    /// table b-tree.
    Scan {
        access: TableAccess,
        index: Option<IndexAccess>,
        /// True when the table is the right side of a LEFT JOIN.
        left_join: bool,
    },
    /// Jump straight to the matching rows using the rowid or an index.
    Search {
        access: TableAccess,
        /// `None` means the seek went through the INTEGER PRIMARY KEY (rowid).
        index: Option<IndexAccess>,
        /// The parts of the key the seek pins down, e.g. `["label=?", "id>?"]`.
        constraints: Vec<String>,
        left_join: bool,
    },
    /// Read several indexes and combine the row ids they produce.
    MultiIndexScan {
        access: TableAccess,
        set_op: SetOp,
        /// One entry per branch. `None` means that branch used the rowid.
        indexes: Vec<Option<String>>,
    },
    /// Probe a hash table built from an earlier table in the join order.
    HashJoin { access: TableAccess },
    /// Ask a custom index method (for example full-text search) for rows.
    IndexMethodQuery { access: TableAccess, method: String },
    /// A query with no FROM clause, which produces exactly one row.
    ConstantRow,
    /// Parent of the branches of a UNION / INTERSECT / EXCEPT query.
    CompoundQuery,
    /// The first branch of a compound query.
    CompoundLeftMost,
    /// One compound operator, applied to the branch below it.
    CompoundOperator { op: CompoundOp },
    /// Put rows in order, either through a sorter or a temporary b-tree.
    Sort {
        purpose: SortPurpose,
        strategy: SortStrategy,
    },
    /// Throw away duplicate rows using a hash table.
    Distinct {
        /// Set when the DISTINCT belongs to an aggregate, e.g. `count(DISTINCT x)`.
        aggregate: Option<String>,
    },
    /// Copy a hash join's build input into an ephemeral table before building.
    MaterializeHashBuildInput { table: String },
    /// A subquery outside the FROM clause, such as `x IN (SELECT ...)`.
    Subquery {
        kind: SubqueryKind,
        /// Number used to refer to this subquery in the plan text.
        id: usize,
        /// True when the subquery reads columns of the enclosing query, and so
        /// has to run again for every outer row.
        correlated: bool,
    },
    /// One phase of a recursive CTE.
    RecursiveCte { phase: RecursiveCtePhase },
}

/// The table (or subquery) a step reads from.
#[derive(Debug, Clone, PartialEq)]
pub struct TableAccess {
    /// Name in the schema, or the subquery/CTE name.
    pub name: String,
    /// How the query refers to it: the alias when there is one, else `name`.
    pub identifier: String,
    pub kind: TableKind,
    /// How many rows the optimizer expects this step to produce per row of the
    /// tables before it in the join order. `None` when nothing estimated it.
    pub estimated_rows: Option<f64>,
}

impl TableAccess {
    /// `users`, or `users AS u` when the query renamed it.
    fn label(&self) -> String {
        if self.name == self.identifier {
            self.identifier.clone()
        } else {
            format!("{} AS {}", self.name, self.identifier)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableKind {
    /// An ordinary b-tree table.
    Table,
    /// A virtual table provided by an extension.
    VirtualTable,
    /// A subquery or CTE in the FROM clause.
    Subquery,
    /// The working table a recursive CTE reads on each iteration.
    RecursiveCteInput,
}

impl TableKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::VirtualTable => "virtual_table",
            Self::Subquery => "subquery",
            Self::RecursiveCteInput => "recursive_cte_input",
        }
    }
}

/// The index a step walks, and whether the index alone answered the query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexAccess {
    pub name: String,
    /// True when every column the query needs is in the index, so the table
    /// itself is never touched.
    pub covering: bool,
}

/// How a multi-index scan combines the row ids of its branches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOp {
    /// The branches came from `OR`, so their rows are unioned.
    Union,
    /// The branches came from `AND`, so only rows in every branch survive.
    Intersection,
}

impl SetOp {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Union => "OR",
            Self::Intersection => "AND",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompoundOp {
    UnionAll,
    Union,
    Intersect,
    Except,
}

impl CompoundOp {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnionAll => "UNION ALL",
            Self::Union => "UNION",
            Self::Intersect => "INTERSECT",
            Self::Except => "EXCEPT",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortPurpose {
    OrderBy,
    GroupBy,
}

impl SortPurpose {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OrderBy => "ORDER BY",
            Self::GroupBy => "GROUP BY",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortStrategy {
    /// An external merge sorter.
    Sorter,
    /// A temporary b-tree that keeps rows in key order as they are inserted.
    TempBTree,
}

impl SortStrategy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sorter => "SORTER",
            Self::TempBTree => "TEMP B-TREE",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubqueryKind {
    /// `x IN (SELECT ...)`: the subquery produces a list of values.
    List,
    /// A subquery used where a single value is expected.
    Scalar,
}

impl SubqueryKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::List => "LIST",
            Self::Scalar => "SCALAR",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecursiveCtePhase {
    /// The non-recursive branch, run once to seed the CTE.
    Setup,
    /// The recursive branch, run until it stops producing rows.
    RecursiveStep,
}

impl RecursiveCtePhase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Setup => "SETUP",
            Self::RecursiveStep => "RECURSIVE STEP",
        }
    }
}

impl PlanOp {
    /// A stable tag naming the kind of step. Tools should switch on this
    /// rather than on the rendered text.
    pub const fn tag(&self) -> &'static str {
        match self {
            Self::Scan { .. } => "Scan",
            Self::Search { .. } => "Search",
            Self::MultiIndexScan { .. } => "MultiIndexScan",
            Self::HashJoin { .. } => "HashJoin",
            Self::IndexMethodQuery { .. } => "IndexMethodQuery",
            Self::ConstantRow => "ConstantRow",
            Self::CompoundQuery => "CompoundQuery",
            Self::CompoundLeftMost => "CompoundLeftMost",
            Self::CompoundOperator { .. } => "CompoundOperator",
            Self::Sort { .. } => "Sort",
            Self::Distinct { .. } => "Distinct",
            Self::MaterializeHashBuildInput { .. } => "MaterializeHashBuildInput",
            Self::Subquery { .. } => "Subquery",
            Self::RecursiveCte { .. } => "RecursiveCte",
        }
    }

    /// The table this step reads, when it reads one.
    pub const fn table_access(&self) -> Option<&TableAccess> {
        match self {
            Self::Scan { access, .. }
            | Self::Search { access, .. }
            | Self::MultiIndexScan { access, .. }
            | Self::HashJoin { access }
            | Self::IndexMethodQuery { access, .. } => Some(access),
            _ => None,
        }
    }
}

/// Renders the exact text `EXPLAIN QUERY PLAN` reports for this step.
///
/// The text says less than the node knows, and on purpose: a SCAN line never
/// says LEFT-JOIN and a SEARCH line never says COVERING INDEX, because that is
/// what those lines have always printed and the conformance snapshots pin
/// them. The fields still record both, so the JSON stays complete where the
/// text is not. Widening the text means re-recording those snapshots.
impl Display for PlanOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Scan { access, index, .. } => {
                write!(f, "SCAN {}", access.label())?;
                if let Some(index) = index {
                    let kind = if index.covering {
                        "COVERING INDEX"
                    } else {
                        "INDEX"
                    };
                    write!(f, " USING {kind} {}", index.name)?;
                }
                Ok(())
            }
            Self::Search {
                access,
                index,
                constraints,
                left_join,
            } => {
                write!(f, "SEARCH {}", access.identifier)?;
                match index {
                    Some(index) => write!(f, " USING INDEX {}", index.name)?,
                    None => write!(f, " USING INTEGER PRIMARY KEY")?,
                }
                if !constraints.is_empty() {
                    write!(f, " ({})", constraints.join(" AND "))?;
                }
                if *left_join {
                    write!(f, " LEFT-JOIN")?;
                }
                Ok(())
            }
            Self::MultiIndexScan {
                access,
                set_op,
                indexes,
            } => {
                let names = indexes
                    .iter()
                    .map(|index| index.as_deref().unwrap_or("PRIMARY KEY"))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(
                    f,
                    "MULTI-INDEX {} {} ({names})",
                    set_op.as_str(),
                    access.identifier
                )
            }
            Self::HashJoin { access } => write!(f, "HASH JOIN {}", access.label()),
            Self::IndexMethodQuery { method, .. } => write!(f, "QUERY INDEX METHOD {method}"),
            Self::ConstantRow => write!(f, "SCAN CONSTANT ROW"),
            Self::CompoundQuery => write!(f, "COMPOUND QUERY"),
            Self::CompoundLeftMost => write!(f, "LEFT-MOST SUBQUERY"),
            Self::CompoundOperator { op } => match op {
                CompoundOp::UnionAll => write!(f, "UNION ALL"),
                other => write!(f, "{} USING TEMP B-TREE", other.as_str()),
            },
            Self::Sort { purpose, strategy } => {
                write!(f, "USE {} FOR {}", strategy.as_str(), purpose.as_str())
            }
            Self::Distinct { aggregate } => match aggregate {
                Some(func) => write!(f, "USE HASH TABLE FOR {func}(DISTINCT)"),
                None => write!(f, "USE HASH TABLE FOR DISTINCT"),
            },
            Self::MaterializeHashBuildInput { table } => {
                write!(f, "MATERIALIZE hash build input for {table}")
            }
            Self::Subquery {
                kind,
                id,
                correlated,
            } => {
                if *correlated {
                    write!(f, "CORRELATED ")?;
                }
                write!(f, "{} SUBQUERY {id}", kind.as_str())
            }
            Self::RecursiveCte { phase } => write!(f, "{}", phase.as_str()),
        }
    }
}

impl QueryPlan {
    /// Serializes the plan as JSON.
    ///
    /// Shape:
    /// ```json
    /// {
    ///   "sql": "SELECT ...",
    ///   "nodes": [
    ///     {
    ///       "id": 4,
    ///       "parent_id": null,
    ///       "detail": "SCAN users AS u",
    ///       "op": "Scan",
    ///       "table": {"name": "users", "identifier": "u", "kind": "table"},
    ///       "estimated_rows": 1000.0
    ///     }
    ///   ]
    /// }
    /// ```
    /// Every node always carries `id`, `parent_id`, `detail` and `op`. The
    /// remaining keys depend on `op` and are absent when they do not apply.
    pub fn to_json(&self) -> String {
        let mut out = String::with_capacity(256 + self.nodes.len() * 160);
        out.push('{');
        write_json_key(&mut out, "sql");
        write_json_string(&mut out, &self.sql);
        out.push_str(",\"nodes\":[");
        for (i, node) in self.nodes.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            node.write_json(&mut out);
        }
        out.push_str("]}");
        out
    }
}

impl PlanNode {
    fn write_json(&self, out: &mut String) {
        out.push('{');
        let _ = write!(out, "\"id\":{}", self.id);
        match self.parent_id {
            Some(parent) => {
                let _ = write!(out, ",\"parent_id\":{parent}");
            }
            None => out.push_str(",\"parent_id\":null"),
        }
        out.push(',');
        write_json_key(out, "detail");
        write_json_string(out, &self.op.to_string());
        out.push(',');
        write_json_key(out, "op");
        write_json_string(out, self.op.tag());
        self.op.write_json_fields(out);
        out.push('}');
    }
}

impl PlanOp {
    fn write_json_fields(&self, out: &mut String) {
        if let Some(access) = self.table_access() {
            out.push(',');
            write_json_key(out, "table");
            out.push('{');
            write_json_key(out, "name");
            write_json_string(out, &access.name);
            out.push(',');
            write_json_key(out, "identifier");
            write_json_string(out, &access.identifier);
            out.push(',');
            write_json_key(out, "kind");
            write_json_string(out, access.kind.as_str());
            out.push('}');
            if let Some(rows) = access.estimated_rows {
                // Row estimates are finite by construction; skip anything else
                // rather than emitting JSON no parser accepts.
                if rows.is_finite() {
                    let _ = write!(out, ",\"estimated_rows\":{rows}");
                }
            }
        }
        match self {
            Self::Scan {
                index, left_join, ..
            } => {
                write_json_index(out, index.as_ref());
                let _ = write!(out, ",\"left_join\":{left_join}");
            }
            Self::Search {
                index,
                constraints,
                left_join,
                ..
            } => {
                write_json_index(out, index.as_ref());
                out.push(',');
                write_json_key(out, "constraints");
                write_json_string_array(out, constraints.iter().map(String::as_str));
                let _ = write!(out, ",\"left_join\":{left_join}");
            }
            Self::MultiIndexScan {
                set_op, indexes, ..
            } => {
                out.push(',');
                write_json_key(out, "set_op");
                write_json_string(out, set_op.as_str());
                out.push(',');
                write_json_key(out, "indexes");
                write_json_string_array(
                    out,
                    indexes
                        .iter()
                        .map(|index| index.as_deref().unwrap_or("PRIMARY KEY")),
                );
            }
            Self::IndexMethodQuery { method, .. } => {
                out.push(',');
                write_json_key(out, "method");
                write_json_string(out, method);
            }
            Self::CompoundOperator { op } => {
                out.push(',');
                write_json_key(out, "set_op");
                write_json_string(out, op.as_str());
            }
            Self::Sort { purpose, strategy } => {
                out.push(',');
                write_json_key(out, "purpose");
                write_json_string(out, purpose.as_str());
                out.push(',');
                write_json_key(out, "strategy");
                write_json_string(out, strategy.as_str());
            }
            Self::Distinct {
                aggregate: Some(func),
            } => {
                out.push(',');
                write_json_key(out, "aggregate");
                write_json_string(out, func);
            }
            Self::MaterializeHashBuildInput { table } => {
                out.push(',');
                write_json_key(out, "build_table");
                write_json_string(out, table);
            }
            Self::Subquery {
                kind,
                id,
                correlated,
            } => {
                out.push(',');
                write_json_key(out, "subquery_kind");
                write_json_string(out, kind.as_str());
                let _ = write!(out, ",\"subquery_id\":{id},\"correlated\":{correlated}");
            }
            Self::RecursiveCte { phase } => {
                out.push(',');
                write_json_key(out, "phase");
                write_json_string(out, phase.as_str());
            }
            Self::HashJoin { .. }
            | Self::ConstantRow
            | Self::CompoundQuery
            | Self::CompoundLeftMost
            | Self::Distinct { aggregate: None } => {}
        }
    }
}

fn write_json_index(out: &mut String, index: Option<&IndexAccess>) {
    let Some(index) = index else {
        return;
    };
    out.push(',');
    write_json_key(out, "index");
    out.push('{');
    write_json_key(out, "name");
    write_json_string(out, &index.name);
    let _ = write!(out, ",\"covering\":{}", index.covering);
    out.push('}');
}

fn write_json_string_array<'a>(out: &mut String, items: impl Iterator<Item = &'a str>) {
    out.push('[');
    for (i, item) in items.enumerate() {
        if i > 0 {
            out.push(',');
        }
        write_json_string(out, item);
    }
    out.push(']');
}

fn write_json_key(out: &mut String, key: &str) {
    write_json_string(out, key);
    out.push(':');
}

fn write_json_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            // Control characters have no shorthand escape and must not go out raw.
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(name: &str, identifier: &str) -> TableAccess {
        TableAccess {
            name: name.to_string(),
            identifier: identifier.to_string(),
            kind: TableKind::Table,
            estimated_rows: None,
        }
    }

    #[test]
    fn scan_without_alias_prints_bare_table_name() {
        let op = PlanOp::Scan {
            access: table("users", "users"),
            index: None,
            left_join: false,
        };
        assert_eq!(op.to_string(), "SCAN users");
    }

    #[test]
    fn scan_with_alias_prints_both_names() {
        let op = PlanOp::Scan {
            access: table("users", "u"),
            index: None,
            left_join: false,
        };
        assert_eq!(op.to_string(), "SCAN users AS u");
    }

    #[test]
    fn covering_index_scan_says_covering() {
        let op = PlanOp::Scan {
            access: table("users", "users"),
            index: Some(IndexAccess {
                name: "idx_age".to_string(),
                covering: true,
            }),
            left_join: false,
        };
        assert_eq!(op.to_string(), "SCAN users USING COVERING INDEX idx_age");
    }

    #[test]
    fn search_without_index_uses_the_rowid() {
        let op = PlanOp::Search {
            access: table("users", "users"),
            index: None,
            constraints: vec!["rowid=?".to_string()],
            left_join: false,
        };
        assert_eq!(
            op.to_string(),
            "SEARCH users USING INTEGER PRIMARY KEY (rowid=?)"
        );
    }

    #[test]
    fn search_joins_constraints_with_and() {
        let op = PlanOp::Search {
            access: table("users", "u"),
            index: Some(IndexAccess {
                name: "idx_age".to_string(),
                covering: false,
            }),
            constraints: vec!["name=?".to_string(), "age>?".to_string()],
            left_join: true,
        };
        assert_eq!(
            op.to_string(),
            "SEARCH u USING INDEX idx_age (name=? AND age>?) LEFT-JOIN"
        );
    }

    #[test]
    fn multi_index_scan_names_a_rowid_branch_primary_key() {
        let op = PlanOp::MultiIndexScan {
            access: table("t", "t"),
            set_op: SetOp::Union,
            indexes: vec![Some("idx_a".to_string()), None],
        };
        assert_eq!(op.to_string(), "MULTI-INDEX OR t (idx_a, PRIMARY KEY)");
    }

    #[test]
    fn compound_operators_print_their_temp_btree() {
        assert_eq!(
            PlanOp::CompoundOperator {
                op: CompoundOp::UnionAll
            }
            .to_string(),
            "UNION ALL"
        );
        assert_eq!(
            PlanOp::CompoundOperator {
                op: CompoundOp::Except
            }
            .to_string(),
            "EXCEPT USING TEMP B-TREE"
        );
    }

    #[test]
    fn subquery_marks_correlation() {
        assert_eq!(
            PlanOp::Subquery {
                kind: SubqueryKind::Scalar,
                id: 1,
                correlated: true,
            }
            .to_string(),
            "CORRELATED SCALAR SUBQUERY 1"
        );
        assert_eq!(
            PlanOp::Subquery {
                kind: SubqueryKind::List,
                id: 2,
                correlated: false,
            }
            .to_string(),
            "LIST SUBQUERY 2"
        );
    }

    #[test]
    fn json_carries_both_the_text_and_the_structure() {
        let plan = QueryPlan {
            sql: "SELECT * FROM users".to_string(),
            nodes: vec![PlanNode {
                id: 2,
                parent_id: None,
                op: PlanOp::Scan {
                    access: TableAccess {
                        name: "users".to_string(),
                        identifier: "u".to_string(),
                        kind: TableKind::Table,
                        estimated_rows: Some(12.5),
                    },
                    index: Some(IndexAccess {
                        name: "idx_age".to_string(),
                        covering: true,
                    }),
                    left_join: false,
                },
            }],
        };
        assert_eq!(
            plan.to_json(),
            r#"{"sql":"SELECT * FROM users","nodes":[{"id":2,"parent_id":null,"detail":"SCAN users AS u USING COVERING INDEX idx_age","op":"Scan","table":{"name":"users","identifier":"u","kind":"table"},"estimated_rows":12.5,"index":{"name":"idx_age","covering":true},"left_join":false}]}"#
        );
    }

    #[test]
    fn json_escapes_quotes_and_control_characters() {
        let plan = QueryPlan {
            sql: "SELECT \"a\"\t--\n FROM t".to_string(),
            nodes: vec![],
        };
        assert_eq!(
            plan.to_json(),
            r#"{"sql":"SELECT \"a\"\t--\n FROM t","nodes":[]}"#
        );
    }
}
