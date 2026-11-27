use crate::alloc::TursoIteratorExt;
use crate::ast;
use crate::dialect::Dialect;
use crate::namespace::NamespaceId;
use crate::schema::ColDef;
use crate::translate::expr::{column_reference_name, skip_parens_and_collate, WalkControl};
use crate::translate::plan::{
    left_using_column_sources, star_column_uses_merged_value, stored_column_name,
    unqualified_column_sources, ColumnLookup, JoinInfo, JoinType, OutputColumnNames,
};
use crate::translate::planner::ROWID_STRS;
use crate::util::{normalize_ident, type_from_name, visit_select, AstVisitor};
use crate::vdbe::affinity::Affinity;
use crate::{
    schema::{Column, Schema, Table},
    LimboError, Result,
};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

/// Information about a table referenced in a view
#[derive(Debug, Clone)]
pub struct ViewTable {
    /// Unqualified table name, normalized.
    pub name: String,
    /// Database qualifier if present, normalized.
    pub db_name: Option<String>,
    /// Optional alias (e.g., "c" in "FROM customers c")
    pub alias: Option<String>,
}

/// Information about a column in the view's output
#[derive(Debug, Clone)]
pub struct ViewColumn {
    /// Index into ViewColumnSchema.tables indicating which table this column comes from
    /// For computed columns or constants, this will be usize::MAX
    pub table_index: usize,
    /// The actual column definition
    pub column: Column,
}

/// Schema information for a view, tracking which columns come from which tables
#[derive(Debug, Clone)]
pub struct ViewColumnSchema {
    /// All tables referenced by the view (in order of appearance)
    pub tables: Vec<ViewTable>,
    /// The view's output columns with their table associations
    pub columns: Vec<ViewColumn>,
}

impl ViewColumnSchema {
    /// Get all columns as a flat vector (without table association info)
    #[turso_macros::allocation_site(crate::alloc::SchemaAllocationSite::FlatViewColumns)]
    pub fn flat_columns(&self) -> Result<crate::alloc::Vec<Column>> {
        Ok(self
            .columns
            .iter()
            .map(|vc| vc.column.clone())
            .try_collect()?)
    }

    /// Get columns that belong to a specific table
    pub fn table_columns(&self, table_index: usize) -> Vec<Column> {
        self.columns
            .iter()
            .filter(|vc| vc.table_index == table_index)
            .map(|vc| vc.column.clone())
            .collect()
    }

    /// Name the view's columns from the column list of `CREATE VIEW v(x, y)`.
    /// See [stored_column_name] for the names true and false.
    pub fn rename_columns(&mut self, names: &[ast::IndexedColumn], dialect: &dyn Dialect) {
        for (index, (view_column, name)) in self.columns.iter_mut().zip(names).enumerate() {
            // as_str: Display would render the quoted form, embedding literal
            // quote characters in the name
            view_column.column.name = Some(stored_column_name(
                name.col_name.as_str().to_string(),
                index,
                dialect,
            ));
        }
    }
}

#[derive(Clone)]
struct ViewSource {
    qualifiers: Vec<String>,
    /// The namespace of the table or view this source reads, which a
    /// column reference with a schema qualifier must name. A subquery or
    /// CTE has none.
    namespace: Option<NamespaceId>,
    columns: Vec<ViewColumn>,
    /// The join that added this source. The first source has no join.
    join_info: Option<JoinInfo>,
}

fn append_view_column_schema(
    schema: ViewColumnSchema,
    tables: &mut Vec<ViewTable>,
) -> Vec<ViewColumn> {
    let table_offset = tables.len();
    tables.extend(schema.tables);
    schema
        .columns
        .into_iter()
        .map(|mut column| {
            if column.table_index != usize::MAX {
                column.table_index += table_offset;
            }
            column
        })
        .collect()
}

fn view_source_from_select_table(
    namespace: NamespaceId,
    select_table: &ast::SelectTable,
    schema: &Schema,
    dialect: &dyn Dialect,
    ctes: &HashMap<String, ViewColumnSchema>,
    tables: &mut Vec<ViewTable>,
) -> Result<ViewSource> {
    match select_table {
        ast::SelectTable::Table(name, alias, _) => {
            let table_name = normalize_ident(name.name.as_str());
            let table_alias = alias.as_ref().map(|a| normalize_ident(a.name().as_str()));
            let qualifiers = table_alias
                .clone()
                .map_or_else(|| vec![table_name.clone()], |alias| vec![alias]);

            if name.namespace.is_none() {
                if let Some(cte) = ctes.get(&table_name) {
                    return Ok(ViewSource {
                        qualifiers,
                        namespace: None,
                        columns: append_view_column_schema(cte.clone(), tables),
                        join_info: None,
                    });
                }
            }

            let table_index = tables.len();
            tables.push(ViewTable {
                name: table_name.clone(),
                db_name: name
                    .namespace
                    .as_ref()
                    .map(|db| normalize_ident(db.as_str())),
                alias: table_alias,
            });
            let table_namespace = view_source_namespace(schema, namespace, name.namespace.as_ref());
            let mut table = schema.get_table(table_namespace, &table_name);
            let mut view = table
                .is_none()
                .then(|| schema.get_view(table_namespace, &table_name))
                .flatten();
            // The engine's virtual tables are found from every namespace,
            // as the resolver finds them.
            if table.is_none() && view.is_none() && name.namespace.is_none() {
                if let Some(root_table) = schema
                    .get_table(NamespaceId::ROOT, &table_name)
                    .filter(|root_table| matches!(root_table.as_ref(), Table::Virtual(_)))
                {
                    table = Some(root_table);
                    view = None;
                }
            }
            let source_columns: &[Column] = match (&table, &view) {
                (Some(table), _) => table.columns(),
                (None, Some(view)) => &view.columns,
                (None, None) => &[],
            };
            let columns = source_columns
                .iter()
                .cloned()
                .map(|column| ViewColumn {
                    table_index,
                    column,
                })
                .collect();
            Ok(ViewSource {
                qualifiers,
                namespace: Some(table_namespace),
                columns,
                join_info: None,
            })
        }
        ast::SelectTable::Select { select, alias, .. } => {
            let derived = extract_view_columns_inner(
                namespace,
                select,
                schema,
                dialect,
                ctes,
                OutputColumnNames::AsWritten,
            )?;
            Ok(ViewSource {
                qualifiers: alias
                    .as_ref()
                    .map(|a| vec![normalize_ident(a.name().as_str())])
                    .unwrap_or_default(),
                namespace: None,
                columns: append_view_column_schema(derived, tables),
                join_info: None,
            })
        }
        ast::SelectTable::Sub(from, alias) if from.joins.is_empty() => {
            let mut source_table = from.select.as_ref().clone();
            match &mut source_table {
                ast::SelectTable::Table(_, source_alias, _)
                | ast::SelectTable::TableCall(_, _, source_alias, ..)
                | ast::SelectTable::Select {
                    alias: source_alias,
                    ..
                }
                | ast::SelectTable::Sub(_, source_alias) => source_alias.clone_from(alias),
            }
            view_source_from_select_table(namespace, &source_table, schema, dialect, ctes, tables)
        }
        ast::SelectTable::Sub(from, alias) => {
            let sources = view_sources_from_clause(namespace, from, schema, dialect, ctes, tables)?;
            Ok(ViewSource {
                qualifiers: alias
                    .as_ref()
                    .map(|a| vec![normalize_ident(a.name().as_str())])
                    .unwrap_or_default(),
                namespace: None,
                columns: expand_view_star(&sources),
                join_info: None,
            })
        }
        ast::SelectTable::TableCall(name, _, alias) => {
            let table_name = normalize_ident(name.name.as_str());
            let table_alias = alias.as_ref().map(|a| normalize_ident(a.name().as_str()));
            let qualifiers = table_alias
                .clone()
                .map_or_else(|| vec![table_name.clone()], |alias| vec![alias]);
            let table_index = tables.len();
            tables.push(ViewTable {
                name: table_name.clone(),
                db_name: name
                    .namespace
                    .as_ref()
                    .map(|db| normalize_ident(db.as_str())),
                alias: table_alias,
            });
            let columns = schema
                .get_table(namespace, &table_name)
                .map(|table| {
                    table
                        .columns()
                        .iter()
                        .cloned()
                        .map(|column| ViewColumn {
                            table_index,
                            column,
                        })
                        .collect()
                })
                .unwrap_or_default();
            Ok(ViewSource {
                qualifiers,
                namespace: Some(namespace),
                columns,
                join_info: None,
            })
        }
    }
}

/// The namespace a FROM item of a view body is looked up in: the one it
/// is written with, else the view's own. A namespace is named by
/// its name. Any other qualifier names a database, and since a view only
/// reads its own database, that is the root namespace of this schema.
fn view_source_namespace(
    schema: &Schema,
    view_namespace: NamespaceId,
    qualifier: Option<&ast::Name>,
) -> NamespaceId {
    let Some(qualifier) = qualifier else {
        return view_namespace;
    };
    schema
        .namespace_named(&normalize_ident(qualifier.as_str()))
        .unwrap_or(NamespaceId::ROOT)
}

fn view_column_name(column: &ViewColumn) -> Option<&str> {
    column.column.name.as_deref()
}

fn view_output_column(source: &ViewColumn) -> ViewColumn {
    ViewColumn {
        table_index: source.table_index,
        column: Column::new(
            source.column.name.clone(),
            source.column.ty_str.clone(),
            None,
            None,
            source.column.ty(),
            source.column.text_collation(),
            ColDef::default(),
        ),
    }
}

/// Collect view sources and validate their NATURAL and USING clauses.
///
/// Stored view metadata must use the same join-column rules as normal name binding.
fn view_sources_from_clause(
    namespace: NamespaceId,
    from: &ast::FromClause,
    schema: &Schema,
    dialect: &dyn Dialect,
    ctes: &HashMap<String, ViewColumnSchema>,
    tables: &mut Vec<ViewTable>,
) -> Result<Vec<ViewSource>> {
    let first =
        view_source_from_select_table(namespace, &from.select, schema, dialect, ctes, tables)?;
    let mut sources = vec![first];
    // SQLite enables strict USING checks for the full list when any join keeps right rows.
    let has_right_or_full_join = from
        .joins
        .iter()
        .any(|join| JoinType::from_join_operator(&join.operator).keeps_right_rows());

    for join in &from.joins {
        let right =
            view_source_from_select_table(namespace, &join.table, schema, dialect, ctes, tables)?;
        let natural = matches!(
            join.operator,
            ast::JoinOperator::TypedJoin(Some(join_type))
                if join_type.contains(ast::JoinType::NATURAL)
        );
        let merged = if natural {
            if join.constraint.is_some() {
                return Err(LimboError::ParseError(
                    "a NATURAL join may not have an ON or USING clause".to_string(),
                ));
            }
            let mut merged = Vec::new();
            for right_name in right
                .columns
                .iter()
                .filter(|column| !column.column.hidden())
                .filter_map(view_column_name)
            {
                if has_left_view_using_column(&sources, right_name, true, has_right_or_full_join)? {
                    merged.push(normalize_ident(right_name));
                }
            }
            merged
        } else if let Some(ast::JoinConstraint::Using(names)) = &join.constraint {
            let mut merged = Vec::with_capacity(names.len());
            for name in names {
                let normalized = normalize_ident(name.as_str());
                // SQLite checks the new right source first. This order controls the error text.
                let in_right = right
                    .columns
                    .iter()
                    .filter_map(view_column_name)
                    .any(|column| column.eq_ignore_ascii_case(&normalized));
                if !in_right
                    || !has_left_view_using_column(
                        &sources,
                        name.as_str(),
                        false,
                        has_right_or_full_join,
                    )?
                {
                    return Err(LimboError::ParseError(format!(
                        "cannot join using column {} - column not present in both tables",
                        name.as_str()
                    )));
                }
                merged.push(normalized);
            }
            merged
        } else {
            Vec::new()
        };

        sources.push(ViewSource {
            join_info: Some(JoinInfo {
                join_type: JoinType::from_join_operator(&join.operator),
                using: merged.into_iter().map(ast::Name::exact).collect(),
                no_reorder: matches!(
                    join.operator,
                    ast::JoinOperator::TypedJoin(Some(join_type))
                        if join_type.contains(ast::JoinType::CROSS)
                ),
                lateral: false,
            }),
            ..right
        });
    }
    Ok(sources)
}

/// Return true if a source on the left side of a join has this USING column.
/// The rules are the planner's; see [left_using_column_sources].
fn has_left_view_using_column(
    sources: &[ViewSource],
    column_name: &str,
    ignore_hidden_columns: bool,
    has_right_or_full_join: bool,
) -> Result<bool> {
    let found = left_using_column_sources(
        sources,
        column_name,
        has_right_or_full_join,
        |source| source.join_info.as_ref(),
        |source| {
            // NATURAL ignores hidden columns, but an explicit USING clause can name one.
            source.columns.iter().position(|column| {
                (!ignore_hidden_columns || !column.column.hidden())
                    && view_column_name(column)
                        .is_some_and(|name| name.eq_ignore_ascii_case(column_name))
            })
        },
    )?;
    Ok(!found.is_empty())
}

/// Expand a view's bare star with SQLite's visible join-column shape.
///
/// USING hides the right copy. A later RIGHT or FULL JOIN can change the
/// metadata source of the one visible copy.
fn expand_view_star(sources: &[ViewSource]) -> Vec<ViewColumn> {
    let mut columns = Vec::new();
    for (source_index, source) in sources.iter().enumerate() {
        for source_column in source
            .columns
            .iter()
            .filter(|column| !column.column.hidden())
        {
            let is_right_using_copy = view_column_name(source_column).is_some_and(|name| {
                source
                    .join_info
                    .as_ref()
                    .is_some_and(|join_info| join_info.merges_column(name))
            });
            if !is_right_using_copy {
                columns.push(view_star_column(sources, source_index, source_column));
            }
        }
    }
    columns
}

/// Return the metadata source for one `*` or `table.*` column.
///
/// SQLite binds this column as an unqualified name when a later USING clause
/// merges it and a later RIGHT or FULL JOIN exists.
fn view_star_column(
    sources: &[ViewSource],
    source_index: usize,
    source_column: &ViewColumn,
) -> ViewColumn {
    let Some(column_name) = view_column_name(source_column) else {
        return view_output_column(source_column);
    };
    if star_column_uses_merged_value(
        sources[source_index + 1..]
            .iter()
            .filter_map(|source| source.join_info.as_ref()),
        column_name,
    ) {
        return resolve_view_column(sources, column_name)
            .expect("source_column itself has this name");
    }
    view_output_column(source_column)
}

/// Resolve metadata for an unqualified view column with the same rules as
/// query binding; see [unqualified_column_sources]. A merged FULL JOIN USING
/// column takes its declared type and collation from its first source.
///
/// SQLite accepts a view whose column name is ambiguous, so this uses the
/// first copy instead of failing.
fn resolve_view_column(sources: &[ViewSource], column_name: &str) -> Option<ViewColumn> {
    let found = unqualified_column_sources(
        sources,
        column_name,
        |source| source.join_info.as_ref(),
        |source| match source.columns.iter().position(|column| {
            view_column_name(column)
                .is_some_and(|candidate| candidate.eq_ignore_ascii_case(column_name))
        }) {
            Some(column_index) => ColumnLookup::Found(column_index),
            None => ColumnLookup::Missing,
        },
    );
    let &(source_index, column_index) = found.sources.first()?;
    let first_column = &sources[source_index].columns[column_index];
    if found.sources.len() == 1 {
        return Some(view_output_column(first_column));
    }
    Some(view_merged_column(first_column))
}

/// Metadata for a FULL JOIN USING column that more than one source supplies.
///
/// The column is a merged value, not a plain column, so SQLite 3.50 reports
/// no declared type for it. It still takes the first source's affinity.
/// (SQLite 3.53 instead names the type after that affinity, such as INT.)
fn view_merged_column(first_column: &ViewColumn) -> ViewColumn {
    ViewColumn {
        table_index: first_column.table_index,
        column: Column::new(
            first_column.column.name.clone(),
            String::new(),
            None,
            None,
            first_column.column.affinity().to_type(),
            first_column.column.text_collation(),
            ColDef::default(),
        ),
    }
}

fn deduplicate_view_column_name(column: &mut ViewColumn, counts: &mut HashMap<String, usize>) {
    let name = column
        .column
        .name
        .clone()
        .unwrap_or_else(|| "?".to_string());
    let count = counts.entry(normalize_ident(&name)).or_insert(0);
    if *count > 0 {
        column.column.name = Some(format!("{name}:{count}"));
    } else {
        column.column.name = Some(name);
    }
    *count += 1;
}

/// The columns of `select_stmt`. `names` says how a result column without
/// an alias is named, as for the planner: a view names it after it
/// resolves names, a FROM subquery or a CTE before.
fn extract_view_columns_inner(
    namespace: NamespaceId,
    select_stmt: &ast::Select,
    schema: &Schema,
    dialect: &dyn Dialect,
    outer_ctes: &HashMap<String, ViewColumnSchema>,
    names: OutputColumnNames,
) -> Result<ViewColumnSchema> {
    let ctes = extract_cte_columns(namespace, &select_stmt.with, schema, dialect, outer_ctes)?;

    let mut tables = Vec::new();
    let mut columns = Vec::new();
    let mut column_name_counts = HashMap::default();

    let ast::OneSelect::Select {
        from,
        columns: select_columns,
        ..
    } = &select_stmt.body.select
    else {
        return Ok(ViewColumnSchema { tables, columns });
    };

    let sources = from
        .as_ref()
        .map(|from| view_sources_from_clause(namespace, from, schema, dialect, &ctes, &mut tables))
        .transpose()?
        .unwrap_or_default();

    for result_column in select_columns {
        match result_column {
            ast::ResultColumn::Expr(expr, alias) => {
                let name = view_result_column_name(expr, alias.as_ref(), &sources, schema, names);
                let mut column = view_result_column(expr, &sources, schema);
                column.column.name = Some(name);
                columns.push(column);
            }
            ast::ResultColumn::Star => {
                let mut expanded = expand_view_star(&sources);
                if expanded.is_empty() {
                    expanded.push(ViewColumn {
                        table_index: usize::MAX,
                        column: Column::new_default_text(
                            Some("*".to_string()),
                            "TEXT".to_string(),
                            None,
                        ),
                    });
                }
                columns.extend(expanded);
            }
            ast::ResultColumn::TableStar(table_ref) => {
                let qualifier = normalize_ident(table_ref.as_str());
                if let Some(source_index) = sources.iter().position(|source| {
                    source
                        .qualifiers
                        .iter()
                        .any(|candidate| candidate.eq_ignore_ascii_case(&qualifier))
                }) {
                    for source_column in sources[source_index]
                        .columns
                        .iter()
                        .filter(|column| !column.column.hidden())
                    {
                        columns.push(view_star_column(&sources, source_index, source_column));
                    }
                }
            }
        }
    }

    // As in SQLite's sqlite3ColumnsFromExprList(), the names true and false
    // are replaced before the names are made unique.
    for (index, column) in columns.iter_mut().enumerate() {
        column.column.name = column
            .column
            .name
            .take()
            .map(|name| stored_column_name(name, index, dialect));
        deduplicate_view_column_name(column, &mut column_name_counts);
    }

    Ok(ViewColumnSchema { tables, columns })
}

pub(crate) fn extract_cte_columns(
    namespace: NamespaceId,
    with: &Option<ast::With>,
    schema: &Schema,
    dialect: &dyn Dialect,
    outer_ctes: &HashMap<String, ViewColumnSchema>,
) -> Result<HashMap<String, ViewColumnSchema>> {
    let mut ctes = outer_ctes.clone();
    for cte in with.iter().flat_map(|with| &with.ctes) {
        let mut derived = extract_view_columns_inner(
            namespace,
            &cte.select,
            schema,
            dialect,
            &ctes,
            OutputColumnNames::AsWritten,
        )?;
        for (index, (column, explicit_name)) in
            derived.columns.iter_mut().zip(&cte.columns).enumerate()
        {
            column.column.name = Some(stored_column_name(
                explicit_name.col_name.as_str().to_string(),
                index,
                dialect,
            ));
        }
        ctes.insert(normalize_ident(cte.tbl_name.as_str()), derived);
    }
    Ok(ctes)
}

pub(crate) fn select_table_column_names(
    namespace: NamespaceId,
    select_table: &ast::SelectTable,
    schema: &Schema,
    dialect: &dyn Dialect,
    ctes: &HashMap<String, ViewColumnSchema>,
) -> Result<Vec<String>> {
    let source = view_source_from_select_table(
        namespace,
        select_table,
        schema,
        dialect,
        ctes,
        &mut Vec::new(),
    )?;
    Ok(source
        .columns
        .iter()
        .filter_map(view_column_name)
        .map(normalize_ident)
        .collect())
}

/// The name of the result column `expr` with `alias`, before
/// [stored_column_name] and before it is made unique, as SQLite's
/// sqlite3ColumnsFromExprList() gives it: the alias, else the name of a
/// column reference, else the expression as written. With
/// [OutputColumnNames::Resolved], a column reference has the name of the
/// column it reads, and a rowid reference the name of the table's
/// INTEGER PRIMARY KEY column. With [OutputColumnNames::AsWritten], it has
/// its name as written.
fn view_result_column_name(
    expr: &ast::Expr,
    alias: Option<&ast::As>,
    sources: &[ViewSource],
    schema: &Schema,
    names: OutputColumnNames,
) -> String {
    let expr_text = match alias {
        Some(ast::As::ImplicitColumnName(text)) => Some(text),
        Some(alias) => return alias.name().as_str().to_string(),
        None => None,
    };
    if names == OutputColumnNames::Resolved {
        let reference = skip_parens_and_collate(expr);
        if let Some(name) = view_referenced_column(reference, sources, schema)
            .and_then(|column| column.column.name)
            .or_else(|| view_rowid_column_name(reference, sources, schema))
        {
            return name;
        }
    }
    column_reference_name(expr)
        .or(expr_text)
        .map(|name| name.as_str().to_string())
        .unwrap_or_else(|| expr.to_string())
}

/// The name of the column that `expr` reads, if `expr` reads the rowid of
/// a source: the name of its INTEGER PRIMARY KEY column, else rowid.
fn view_rowid_column_name(
    expr: &ast::Expr,
    sources: &[ViewSource],
    schema: &Schema,
) -> Option<String> {
    let (qualifier, column_name) = match expr {
        ast::Expr::Id(column_name) => (None, column_name),
        ast::Expr::Qualified(table, column_name) => (Some((None, table)), column_name),
        ast::Expr::DoublyQualified(database, table, column_name) => {
            (Some((Some(database), table)), column_name)
        }
        _ => return None,
    };
    if !ROWID_STRS
        .iter()
        .any(|rowid| rowid.eq_ignore_ascii_case(column_name.as_str()))
    {
        return None;
    }
    // SQLite refuses an unqualified rowid when the FROM clause has more than
    // one source, so the first source is the one it reads.
    let source = match qualifier {
        None => sources.first(),
        Some((database, table)) => view_source_named(sources, schema, database, table),
    }?;
    let rowid_alias = source
        .columns
        .iter()
        .find(|column| column.column.is_rowid_alias())
        .and_then(view_column_name);
    Some(rowid_alias.unwrap_or("rowid").to_string())
}

fn view_result_column(expr: &ast::Expr, sources: &[ViewSource], schema: &Schema) -> ViewColumn {
    if let Some(column) = view_referenced_column(expr, sources, schema) {
        return column;
    }
    let ty_str = view_affinity_type_name(view_expression_affinity(expr, sources, schema));
    let (ty, _) = type_from_name(ty_str);
    ViewColumn {
        table_index: usize::MAX,
        column: Column::new(
            None,
            ty_str.to_string(),
            None,
            None,
            ty,
            None,
            ColDef::default(),
        ),
    }
}

fn view_referenced_column(
    expr: &ast::Expr,
    sources: &[ViewSource],
    schema: &Schema,
) -> Option<ViewColumn> {
    let (database, table, column_name) = match expr {
        ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => {
            return view_referenced_column(&exprs[0], sources, schema);
        }
        ast::Expr::Id(column_name) => {
            return resolve_view_column(sources, column_name.as_str());
        }
        ast::Expr::Qualified(table, column_name) => (None, table, column_name),
        ast::Expr::DoublyQualified(database, table, column_name) => {
            (Some(database), table, column_name)
        }
        _ => return None,
    };
    view_source_named(sources, schema, database, table)?
        .columns
        .iter()
        .find(|column| {
            view_column_name(column)
                .is_some_and(|candidate| candidate.eq_ignore_ascii_case(column_name.as_str()))
        })
        .map(view_output_column)
}

/// The source that a column reference qualified with `table`, and with
/// `database` if it has a schema qualifier, reads from. As in SQLite, a
/// reference with a schema qualifier reads only a table or view of that
/// schema, never a subquery or CTE.
fn view_source_named<'a>(
    sources: &'a [ViewSource],
    schema: &Schema,
    database: Option<&ast::Name>,
    table: &ast::Name,
) -> Option<&'a ViewSource> {
    sources.iter().find(|source| {
        let database_matches = database.is_none_or(|database| {
            source.namespace.is_some_and(|namespace| {
                view_source_namespace(schema, namespace, Some(database)) == namespace
            })
        });
        database_matches
            && source
                .qualifiers
                .iter()
                .any(|candidate| candidate.eq_ignore_ascii_case(table.as_str()))
    })
}

fn view_expression_affinity(expr: &ast::Expr, sources: &[ViewSource], schema: &Schema) -> Affinity {
    match expr {
        ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => {
            view_expression_affinity(&exprs[0], sources, schema)
        }
        ast::Expr::Collate(inner, _) => view_expression_affinity(inner, sources, schema),
        ast::Expr::Cast { type_name, .. } => {
            Affinity::affinity(type_name.as_ref().map_or("", |type_name| &type_name.name))
        }
        _ => view_referenced_column(expr, sources, schema).map_or(Affinity::None, |column| {
            Affinity::affinity(&column.column.ty_str)
        }),
    }
}

fn view_affinity_type_name(affinity: Affinity) -> &'static str {
    match affinity {
        Affinity::None => "",
        Affinity::Blob => "BLOB",
        Affinity::Integer => "INT",
        Affinity::Real => "REAL",
        Affinity::Text => "TEXT",
        Affinity::Numeric => "NUM",
    }
}

/// Extract column information from a SELECT statement for view creation.
///
/// Bare-star expansion follows the join's visible row shape: columns merged
/// by NATURAL or USING appear once, while `table.*` still includes every
/// column from that particular source. A later RIGHT or FULL JOIN can change
/// which source supplies a merged column.
///
/// `namespace` is the namespace of the view, where the names of its body
/// that have no qualifier are looked up. `dialect` says whether a column
/// can have the name true or false.
pub fn extract_view_columns(
    namespace: NamespaceId,
    select_stmt: &ast::Select,
    schema: &Schema,
    dialect: &dyn Dialect,
) -> Result<ViewColumnSchema> {
    extract_view_columns_inner(
        namespace,
        select_stmt,
        schema,
        dialect,
        &HashMap::default(),
        OutputColumnNames::Resolved,
    )
}

/// The tables `select` reads, each with the namespace it is written
/// with, if any. A name that is a CTE of the select is not a table.
pub fn referenced_tables(select: &ast::Select) -> std::vec::Vec<(Option<String>, String)> {
    #[derive(Default)]
    struct TablesRead {
        /// The names of the CTEs of each enclosing WITH clause.
        ctes_in_scope: Vec<Vec<String>>,
        tables: HashSet<(Option<String>, String)>,
    }

    impl AstVisitor for TablesRead {
        fn expr(&mut self, _expr: &ast::Expr) -> Result<WalkControl> {
            Ok(WalkControl::Continue)
        }

        fn table(&mut self, name: &ast::QualifiedName) -> Result<()> {
            let table = normalize_ident(name.name.as_str());
            let qualifier = name
                .namespace
                .as_ref()
                .map(|qualifier| normalize_ident(qualifier.as_str()));
            let is_cte =
                qualifier.is_none() && self.ctes_in_scope.iter().flatten().any(|cte| *cte == table);
            if !is_cte {
                self.tables.insert((qualifier, table));
            }
            Ok(())
        }

        fn enter_with(&mut self, with: &ast::With) {
            self.ctes_in_scope.push(
                with.ctes
                    .iter()
                    .map(|cte| normalize_ident(cte.tbl_name.as_str()))
                    .collect(),
            );
        }

        fn leave_with(&mut self) {
            self.ctes_in_scope.pop();
        }
    }

    let mut tables_read = TablesRead::default();
    visit_select(select, &mut tables_read).expect("collecting the tables of a query cannot fail");
    tables_read.tables.into_iter().collect()
}
