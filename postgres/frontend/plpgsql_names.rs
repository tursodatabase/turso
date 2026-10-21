use turso_core::dialect::TableCatalog;
use turso_core::{walk_expr_mut, LimboError, Result, WalkControl};
use turso_parser::ast;

/// What a name that is both a column and a PL/pgSQL variable means, as
/// `#variable_conflict` sets it.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum VariableConflict {
    Error,
    UseVariable,
    UseColumn,
}

/// The PL/pgSQL names that a query can read in place of columns.
pub(crate) trait Variables {
    /// The value of the variable `name`, or of `qualifier.name` for a
    /// variable of a labeled block. `None` when no variable has the name.
    fn variable(&self, qualifier: Option<&str>, name: &str) -> Result<Option<ast::Expr>>;

    /// Replace `NEW.x`, `OLD.x` and `TG_ARGV[i]` in `expr`.
    fn replace_trigger_value(&self, expr: &mut ast::Expr) -> Result<WalkControl>;
}

/// Replaces the PL/pgSQL names in a query with their values. As in
/// PostgreSQL, a name is a column when a table of its query level or of an
/// enclosing level has a column with the name. Names compare exactly,
/// because PostgreSQL folds the names without quotes before.
pub(crate) struct NameResolver<'a> {
    variables: &'a dyn Variables,
    tables: &'a dyn TableCatalog,
    conflict: VariableConflict,
    ctes: Vec<ScopeTable>,
    /// The first name that is both a column and a variable.
    pub(crate) ambiguous: Option<String>,
}

#[derive(Clone)]
struct ScopeTable {
    name: String,
    /// `None` when the columns are not known, as for a table function.
    columns: Option<Vec<String>>,
    /// The columns that a `USING` or `NATURAL` join merged with a column of
    /// an earlier table. `*` gives a merged column one time.
    merged: Vec<String>,
}

/// The tables that a part of a query can read columns from.
#[derive(Clone, Copy)]
struct Scope<'s> {
    tables: &'s [ScopeTable],
    parent: Option<&'s Scope<'s>>,
}

enum ColumnLookup {
    Found,
    NotFound,
    Unknown,
}

impl<'a> NameResolver<'a> {
    pub(crate) fn new(
        variables: &'a dyn Variables,
        tables: &'a dyn TableCatalog,
        conflict: VariableConflict,
    ) -> Self {
        Self {
            variables,
            tables,
            conflict,
            ctes: Vec::new(),
            ambiguous: None,
        }
    }

    /// Resolve an expression of PL/pgSQL, such as the condition of IF. It
    /// reads no table, but its subqueries can.
    pub(crate) fn expression(&mut self, expr: &mut ast::Expr) -> Result<()> {
        self.expr(expr, Scope::EMPTY)
    }

    pub(crate) fn statement(&mut self, stmt: &mut ast::Stmt) -> Result<()> {
        match stmt {
            ast::Stmt::Select(select) => self.select(select, None).map(|_| ()),
            ast::Stmt::Insert {
                with,
                tbl_name,
                body,
                returning,
                ..
            } => {
                let ctes = self.add_ctes(with.as_mut(), None)?;
                let target = self.target_table(tbl_name);
                if let ast::InsertBody::Select(select, upsert) = body {
                    self.select(select, None)?;
                    let tables = [
                        target.clone(),
                        ScopeTable {
                            name: "excluded".to_string(),
                            columns: target.columns.clone(),
                            merged: Vec::new(),
                        },
                    ];
                    let scope = Scope {
                        tables: &tables,
                        parent: None,
                    };
                    let mut upsert = upsert.as_deref_mut();
                    while let Some(clause) = upsert {
                        if let Some(index) = &mut clause.index {
                            for target in &mut index.targets {
                                self.expr(&mut target.expr, scope)?;
                            }
                            if let Some(where_clause) = &mut index.where_clause {
                                self.expr(where_clause, scope)?;
                            }
                        }
                        if let ast::UpsertDo::Set { sets, where_clause } = &mut clause.do_clause {
                            for set in sets {
                                self.expr(&mut set.expr, scope)?;
                            }
                            if let Some(where_clause) = where_clause {
                                self.expr(where_clause, scope)?;
                            }
                        }
                        upsert = clause.next.as_deref_mut();
                    }
                }
                let tables = [target];
                self.result_columns(returning, Scope::new(&tables, None))?;
                self.ctes.truncate(ctes);
                Ok(())
            }
            ast::Stmt::Update(update) => {
                let ctes = self.add_ctes(update.with.as_mut(), None)?;
                let mut tables = vec![self.target_table(&update.tbl_name)];
                if let Some(from) = &mut update.from {
                    self.from(from, None, &mut tables)?;
                }
                let scope = Scope::new(&tables, None);
                for set in &mut update.sets {
                    self.expr(&mut set.expr, scope)?;
                }
                if let Some(where_clause) = &mut update.where_clause {
                    self.expr(where_clause, scope)?;
                }
                self.result_columns(&mut update.returning, scope)?;
                self.ctes.truncate(ctes);
                Ok(())
            }
            ast::Stmt::Delete {
                with,
                tbl_name,
                where_clause,
                returning,
                ..
            } => {
                let ctes = self.add_ctes(with.as_mut(), None)?;
                let tables = [self.target_table(tbl_name)];
                let scope = Scope::new(&tables, None);
                if let Some(where_clause) = where_clause {
                    self.expr(where_clause, scope)?;
                }
                self.result_columns(returning, scope)?;
                self.ctes.truncate(ctes);
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Resolve a query, and give the number of its columns. `None` when
    /// the number is not known, as for `*` on a table function.
    pub(crate) fn query(&mut self, select: &mut ast::Select) -> Result<Option<usize>> {
        Ok(self.select(select, None)?.map(|names| names.len()))
    }

    /// Resolve a query, and give the names of its columns. `None` when the
    /// columns are not known, as for `*` on a table function.
    fn select(
        &mut self,
        select: &mut ast::Select,
        parent: Option<&Scope>,
    ) -> Result<Option<Vec<String>>> {
        let ctes = self.add_ctes(select.with.as_mut(), parent)?;
        let (names, tables) = self.one_select(&mut select.body.select, parent)?;
        for compound in &mut select.body.compounds {
            self.one_select(&mut compound.select, parent)?;
        }
        let scope = Scope::new(&tables, parent);
        for sorted in &mut select.order_by {
            if is_output_column(&sorted.expr, names.as_deref()) {
                continue;
            }
            self.expr(&mut sorted.expr, scope)?;
        }
        if let Some(limit) = &mut select.limit {
            self.expr(&mut limit.expr, scope)?;
            if let Some(offset) = &mut limit.offset {
                self.expr(offset, scope)?;
            }
        }
        self.ctes.truncate(ctes);
        Ok(names)
    }

    fn one_select(
        &mut self,
        select: &mut ast::OneSelect,
        parent: Option<&Scope>,
    ) -> Result<(Option<Vec<String>>, Vec<ScopeTable>)> {
        match select {
            ast::OneSelect::Select {
                columns,
                from,
                where_clause,
                group_by,
                window_clause,
                ..
            } => {
                let mut tables = Vec::new();
                if let Some(from) = from {
                    self.from(from, parent, &mut tables)?;
                }
                let names = output_names(columns, &tables);
                let scope = Scope::new(&tables, parent);
                self.result_columns(columns, scope)?;
                if let Some(where_clause) = where_clause {
                    self.expr(where_clause, scope)?;
                }
                if let Some(group_by) = group_by {
                    for expr in &mut group_by.exprs {
                        let is_input_column = matches!(
                            &**expr,
                            ast::Expr::Id(name)
                                if matches!(scope.find(None, name.as_str()), ColumnLookup::Found)
                        );
                        if !is_input_column && is_output_column(expr, names.as_deref()) {
                            continue;
                        }
                        self.expr(expr, scope)?;
                    }
                    if let Some(having) = &mut group_by.having {
                        self.expr(having, scope)?;
                    }
                }
                for window in window_clause {
                    for expr in &mut window.window.partition_by {
                        self.expr(expr, scope)?;
                    }
                    for sorted in &mut window.window.order_by {
                        self.expr(&mut sorted.expr, scope)?;
                    }
                }
                Ok((names, tables))
            }
            ast::OneSelect::Values(rows) => {
                let scope = Scope::new(&[], parent);
                for expr in rows.iter_mut().flatten() {
                    self.expr(expr, scope)?;
                }
                let count = rows.first().map_or(0, Vec::len);
                let names = (1..=count).map(|i| format!("column{i}")).collect();
                Ok((Some(names), Vec::new()))
            }
        }
    }

    fn from(
        &mut self,
        from: &mut ast::FromClause,
        parent: Option<&Scope>,
        tables: &mut Vec<ScopeTable>,
    ) -> Result<()> {
        self.select_table(&mut from.select, parent, tables)?;
        for join in &mut from.joins {
            let earlier = tables.len();
            self.select_table(&mut join.table, parent, tables)?;
            let merged = merged_columns(join, &tables[..earlier], &tables[earlier..]);
            for table in &mut tables[earlier..] {
                table.merged.clone_from(&merged);
            }
            if let Some(ast::JoinConstraint::On(expr)) = &mut join.constraint {
                self.expr(expr, Scope::new(tables, parent))?;
            }
        }
        Ok(())
    }

    fn select_table(
        &mut self,
        table: &mut ast::SelectTable,
        parent: Option<&Scope>,
        tables: &mut Vec<ScopeTable>,
    ) -> Result<()> {
        match table {
            ast::SelectTable::Table(name, alias, _) => {
                tables.push(ScopeTable {
                    name: alias
                        .as_ref()
                        .map_or(&name.name, ast::As::name)
                        .as_str()
                        .to_string(),
                    columns: self.table_columns(name),
                    merged: Vec::new(),
                });
            }
            ast::SelectTable::TableCall(name, args, alias) => {
                for arg in args {
                    self.expr(arg, Scope::new(tables, parent))?;
                }
                tables.push(ScopeTable {
                    name: alias
                        .as_ref()
                        .map_or(&name.name, ast::As::name)
                        .as_str()
                        .to_string(),
                    columns: None,
                    merged: Vec::new(),
                });
            }
            ast::SelectTable::Select {
                select,
                alias,
                lateral,
            } => {
                let columns = if *lateral {
                    let earlier = tables.clone();
                    self.select(select, Some(&Scope::new(&earlier, parent)))?
                } else {
                    self.select(select, parent)?
                };
                tables.push(ScopeTable {
                    name: alias
                        .as_ref()
                        .map_or("", |alias| alias.name().as_str())
                        .to_string(),
                    columns,
                    merged: Vec::new(),
                });
            }
            ast::SelectTable::Sub(from, _) => self.from(from, parent, tables)?,
        }
        Ok(())
    }

    /// Register the CTEs of `with`, and give the number of CTEs before them.
    /// A recursive CTE without a column list has the columns of its first
    /// query, as in PostgreSQL.
    fn add_ctes(&mut self, with: Option<&mut ast::With>, parent: Option<&Scope>) -> Result<usize> {
        let start = self.ctes.len();
        let Some(with) = with else {
            return Ok(start);
        };
        for cte in &mut with.ctes {
            let listed = (!cte.columns.is_empty()).then(|| {
                cte.columns
                    .iter()
                    .map(|column| column.col_name.as_str().to_string())
                    .collect::<Vec<_>>()
            });
            if with.recursive {
                let columns = match &listed {
                    Some(listed) => Some(listed.clone()),
                    None => {
                        let mut first_query = cte.select.body.select.clone();
                        self.one_select(&mut first_query, parent)?.0
                    }
                };
                self.ctes.push(ScopeTable {
                    name: cte.tbl_name.as_str().to_string(),
                    columns,
                    merged: Vec::new(),
                });
            }
            let names = self.select(&mut cte.select, parent)?;
            if with.recursive {
                self.ctes.pop();
            }
            self.ctes.push(ScopeTable {
                name: cte.tbl_name.as_str().to_string(),
                columns: listed.or(names),
                merged: Vec::new(),
            });
        }
        Ok(start)
    }

    fn target_table(&self, name: &ast::QualifiedName) -> ScopeTable {
        ScopeTable {
            name: name
                .alias
                .as_ref()
                .unwrap_or(&name.name)
                .as_str()
                .to_string(),
            columns: self.table_columns(name),
            merged: Vec::new(),
        }
    }

    fn table_columns(&self, name: &ast::QualifiedName) -> Option<Vec<String>> {
        if name.namespace.is_none() {
            if let Some(cte) = self
                .ctes
                .iter()
                .rev()
                .find(|cte| cte.name == name.name.as_str())
            {
                return cte.columns.clone();
            }
        }
        Some(self.tables.column_names(name).unwrap_or_default())
    }

    /// Resolve the result columns. A name that becomes the value of a
    /// variable keeps the name as its alias, because ORDER BY and GROUP BY
    /// can read the column by that name.
    fn result_columns(&mut self, columns: &mut [ast::ResultColumn], scope: Scope) -> Result<()> {
        for column in columns {
            let ast::ResultColumn::Expr(expr, alias) = column else {
                continue;
            };
            let name = match expr.as_ref() {
                ast::Expr::Id(name) => Some(name.clone()),
                _ => None,
            };
            self.expr(expr, scope)?;
            let replaced = !matches!(expr.as_ref(), ast::Expr::Id(_));
            if let Some(name) = name.filter(|_| replaced) {
                if matches!(alias, None | Some(ast::As::ImplicitColumnName(_))) {
                    *alias = Some(ast::As::As(name));
                }
            }
        }
        Ok(())
    }

    fn expr(&mut self, expr: &mut ast::Expr, scope: Scope) -> Result<()> {
        walk_expr_mut(expr, &mut |expr| self.resolve_node(expr, scope))?;
        Ok(())
    }

    fn resolve_node(&mut self, expr: &mut ast::Expr, scope: Scope) -> Result<WalkControl> {
        match expr {
            ast::Expr::Id(name) => {
                let name = name.as_str().to_string();
                self.replace_variable(expr, None, &name, scope)
            }
            ast::Expr::Qualified(qualifier, name) => {
                if matches!(
                    qualifier.as_str().to_ascii_lowercase().as_str(),
                    "new" | "old"
                ) {
                    return self.variables.replace_trigger_value(expr);
                }
                let (qualifier, name) = (qualifier.as_str().to_string(), name.as_str().to_string());
                self.replace_variable(expr, Some(&qualifier), &name, scope)
            }
            ast::Expr::Subquery(select) | ast::Expr::Exists(select) => {
                self.select(select, Some(&scope))?;
                Ok(WalkControl::SkipChildren)
            }
            ast::Expr::InSelect { rhs, .. } => {
                self.select(rhs, Some(&scope))?;
                Ok(WalkControl::Continue)
            }
            _ => self.variables.replace_trigger_value(expr),
        }
    }

    fn replace_variable(
        &mut self,
        expr: &mut ast::Expr,
        qualifier: Option<&str>,
        name: &str,
        scope: Scope,
    ) -> Result<WalkControl> {
        let Some(value) = self.variables.variable(qualifier, name)? else {
            return Ok(WalkControl::Continue);
        };
        let full_name = || match qualifier {
            Some(qualifier) => format!("{qualifier}.{name}"),
            None => name.to_string(),
        };
        let use_variable = match (scope.find(qualifier, name), self.conflict) {
            (ColumnLookup::NotFound, _) | (_, VariableConflict::UseVariable) => true,
            (_, VariableConflict::UseColumn) => false,
            (ColumnLookup::Found, VariableConflict::Error) => {
                self.ambiguous.get_or_insert_with(full_name);
                false
            }
            (ColumnLookup::Unknown, VariableConflict::Error) => {
                return Err(LimboError::ParseError(format!(
                    "the variable \"{}\" in a query that reads a table function is not supported yet",
                    full_name()
                )))
            }
        };
        if !use_variable {
            return Ok(WalkControl::Continue);
        }
        *expr = value;
        Ok(WalkControl::SkipChildren)
    }
}

impl<'s> Scope<'s> {
    const EMPTY: Scope<'static> = Scope {
        tables: &[],
        parent: None,
    };

    fn new(tables: &'s [ScopeTable], parent: Option<&'s Scope<'s>>) -> Self {
        Self { tables, parent }
    }

    /// Whether a table of this level or of an enclosing level has the
    /// column, or the table `table` when the name has a qualifier. A name
    /// without a qualifier can also be the name of a table, which reads its
    /// whole row.
    fn find(&self, table: Option<&str>, column: &str) -> ColumnLookup {
        let mut unknown = false;
        let mut scope = Some(self);
        while let Some(current) = scope {
            for candidate in current.tables {
                if table.is_some_and(|table| candidate.name != table) {
                    continue;
                }
                if table.is_none() && candidate.name == column {
                    return ColumnLookup::Found;
                }
                match &candidate.columns {
                    Some(columns) => {
                        if columns.iter().any(|name| name == column) {
                            return ColumnLookup::Found;
                        }
                    }
                    None => unknown = true,
                }
            }
            scope = current.parent;
        }
        if unknown {
            ColumnLookup::Unknown
        } else {
            ColumnLookup::NotFound
        }
    }
}

/// The columns that `join` merges: its `USING` columns, or for a `NATURAL`
/// join, the columns of the joined tables that an earlier table also has.
fn merged_columns(
    join: &ast::JoinedSelectTable,
    earlier: &[ScopeTable],
    joined: &[ScopeTable],
) -> Vec<String> {
    if let Some(ast::JoinConstraint::Using(names)) = &join.constraint {
        return names.iter().map(|name| name.as_str().to_string()).collect();
    }
    let natural = matches!(
        join.operator,
        ast::JoinOperator::TypedJoin(Some(join_type)) if join_type.contains(ast::JoinType::NATURAL)
    );
    if !natural {
        return Vec::new();
    }
    let earlier_columns: Vec<&String> = earlier
        .iter()
        .filter_map(|table| table.columns.as_ref())
        .flatten()
        .collect();
    joined
        .iter()
        .filter_map(|table| table.columns.as_ref())
        .flatten()
        .filter(|column| earlier_columns.contains(column))
        .cloned()
        .collect()
}

/// Whether `expr` in ORDER BY or GROUP BY is a name of an output column of
/// the query, which the clause reads before the input columns.
fn is_output_column(expr: &ast::Expr, names: Option<&[String]>) -> bool {
    match expr {
        ast::Expr::Id(name) => names.is_some_and(|names| names.iter().any(|n| n == name.as_str())),
        _ => false,
    }
}

/// The names of the columns of a query, as PostgreSQL gives them.
fn output_names(columns: &[ast::ResultColumn], tables: &[ScopeTable]) -> Option<Vec<String>> {
    let mut names = Vec::new();
    for column in columns {
        match column {
            ast::ResultColumn::Expr(expr, alias) => names.push(match alias {
                Some(ast::As::As(name) | ast::As::Elided(name)) => name.as_str().to_string(),
                _ => expression_name(expr),
            }),
            ast::ResultColumn::Star => {
                for table in tables {
                    let columns = table.columns.as_ref()?;
                    names.extend(
                        columns
                            .iter()
                            .filter(|column| !table.merged.contains(column))
                            .cloned(),
                    );
                }
            }
            ast::ResultColumn::TableStar(name) => {
                let table = tables.iter().find(|table| table.name == name.as_str())?;
                names.extend(table.columns.clone()?);
            }
        }
    }
    Some(names)
}

fn expression_name(expr: &ast::Expr) -> String {
    match expr {
        ast::Expr::Id(name)
        | ast::Expr::Qualified(_, name)
        | ast::Expr::DoublyQualified(_, _, name) => name.as_str().to_string(),
        ast::Expr::FunctionCall { name, .. } | ast::Expr::FunctionCallStar { name, .. } => {
            name.as_str().to_ascii_lowercase()
        }
        ast::Expr::Cast { expr, .. } => expression_name(expr),
        _ => "?column?".to_string(),
    }
}
