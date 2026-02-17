//! Check for additional syntax error
use crate::ast::*;

impl Cmd {
    /// Statement accessor
    pub fn stmt(&self) -> &Stmt {
        match self {
            Self::Explain(stmt) => stmt,
            Self::ExplainQueryPlan(stmt) => stmt,
            Self::Stmt(stmt) => stmt,
        }
    }
    /// Like `sqlite3_column_count` but more limited
    pub fn column_count(&self) -> ColumnCount {
        match self {
            Self::Explain(_) => ColumnCount::Fixed(8),
            Self::ExplainQueryPlan(_) => ColumnCount::Fixed(4),
            Self::Stmt(stmt) => stmt.column_count(),
        }
    }
    /// Like `sqlite3_stmt_isexplain`
    pub fn is_explain(&self) -> bool {
        matches!(self, Self::Explain(_) | Self::ExplainQueryPlan(_))
    }
    /// Like `sqlite3_stmt_readonly`
    pub fn readonly(&self) -> bool {
        self.stmt().readonly()
    }
}

/// Column count
pub enum ColumnCount {
    /// With `SELECT *` / PRAGMA
    Dynamic,
    /// Constant count
    Fixed(usize),
    /// No column
    None,
}

impl ColumnCount {
    fn incr(&mut self) {
        if let Self::Fixed(n) = self {
            *n += 1;
        }
    }
}

impl Stmt {
    /// Like `sqlite3_column_count` but more limited
    pub fn column_count(&self) -> ColumnCount {
        match self {
            Self::Delete { returning, .. } => column_count(returning),
            Self::Insert { returning, .. } => column_count(returning),
            Self::Pragma { .. } => ColumnCount::Dynamic,
            Self::Select(s) => s.column_count(),
            Self::Update(update) => {
                let Update { returning, .. } = &update;
                column_count(returning)
            }
            _ => ColumnCount::None,
        }
    }

    /// Like `sqlite3_stmt_readonly`
    pub fn readonly(&self) -> bool {
        match self {
            Self::Attach { .. } => true,
            Self::Begin { .. } => true,
            Self::Commit { .. } => true,
            Self::Detach { .. } => true,
            Self::Pragma { .. } => true, // TODO check all
            Self::Reindex { .. } => true,
            Self::Release { .. } => true,
            Self::Rollback { .. } => true,
            Self::Savepoint { .. } => true,
            Self::Select(..) => true,
            _ => false,
        }
    }
}

impl CreateTableBody {
    /// explicit primary key constraint ?
    pub fn has_primary_key(&self) -> bool {
        if let Self::ColumnsAndConstraints {
            columns,
            constraints,
            ..
        } = self
        {
            for col in columns {
                for c in col {
                    if let ColumnConstraint::PrimaryKey { .. } = c {
                        return true;
                    }
                }
            }
            for c in constraints {
                if let TableConstraint::PrimaryKey { .. } = c.constraint {
                    return true;
                }
            }
        }
        false
    }
}

impl<'a> IntoIterator for &'a ColumnDefinition {
    type Item = &'a ColumnConstraint;
    type IntoIter = std::iter::Map<
        std::slice::Iter<'a, NamedColumnConstraint>,
        fn(&'a NamedColumnConstraint) -> &'a ColumnConstraint,
    >;

    fn into_iter(self) -> Self::IntoIter {
        self.constraints.iter().map(|nc| &nc.constraint)
    }
}

impl Select {
    /// Like `sqlite3_column_count` but more limited
    pub fn column_count(&self) -> ColumnCount {
        self.body.select.column_count()
    }
}

impl OneSelect {
    /// Like `sqlite3_column_count` but more limited
    pub fn column_count(&self) -> ColumnCount {
        match self {
            Self::Select { columns, .. } => column_count(columns),
            Self::Values(values) => {
                debug_assert!(!values.is_empty());

                #[cfg(debug_assertions)]
                for row in values {
                    debug_assert!(!row.is_empty(), "Values row should not be empty");
                    debug_assert_eq!(
                        row.len(),
                        values[0].len(),
                        "All rows in VALUES should have the same length"
                    );
                }

                ColumnCount::Fixed(values[0].len())
            }
        }
    }
}

impl ResultColumn {
    pub fn column_count(&self) -> ColumnCount {
        match self {
            Self::Expr(..) => ColumnCount::Fixed(1),
            _ => ColumnCount::Dynamic,
        }
    }
}

fn column_count(cols: &[ResultColumn]) -> ColumnCount {
    assert!(!cols.is_empty());
    let mut count = ColumnCount::Fixed(0);
    for col in cols {
        match col.column_count() {
            ColumnCount::Fixed(_) => count.incr(),
            _ => return ColumnCount::Dynamic,
        }
    }
    count
}
