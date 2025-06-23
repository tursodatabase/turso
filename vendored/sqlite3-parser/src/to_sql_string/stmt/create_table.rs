use std::fmt::Display;

use crate::{ast, to_sql_string::ToSqlString};

impl ToSqlString for ast::CreateTableBody {
    fn to_sql_string<C: crate::to_sql_string::ToSqlContext>(&self, context: &C) -> String {
        match self {
            Self::AsSelect(select) => format!("AS {}", select.to_sql_string(context)),
            Self::ColumnsAndConstraints {
                columns,
                constraints,
                options,
            } => {
                format!(
                    "({}{}){}",
                    columns
                        .iter()
                        .map(|(_, col)| col.to_sql_string(context))
                        .collect::<Vec<_>>()
                        .join(", "),
                    constraints
                        .as_ref()
                        .map_or("".to_string(), |constraints| format!(
                            ", {}",
                            constraints
                                .iter()
                                .map(|constraint| constraint.to_sql_string(context))
                                .collect::<Vec<_>>()
                                .join(", ")
                        )),
                    options
                )
            }
        }
    }
}

impl ToSqlString for ast::NamedTableConstraint {
    fn to_sql_string<C: crate::to_sql_string::ToSqlContext>(&self, context: &C) -> String {
        if let Some(name) = &self.name {
            format!(
                "CONSTRAINT {} {}",
                name.0,
                self.constraint.to_sql_string(context)
            )
        } else {
            self.constraint.to_sql_string(context).to_string()
        }
    }
}

impl ToSqlString for ast::TableConstraint {
    fn to_sql_string<C: crate::to_sql_string::ToSqlContext>(&self, context: &C) -> String {
        match self {
            Self::Check(expr) => format!("CHECK ({})", expr.to_sql_string(context)),
            Self::ForeignKey {
                columns,
                clause,
                deref_clause,
            } => format!(
                "FOREIGN KEY ({}) {}{}",
                columns
                    .iter()
                    .map(|col| col.to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
                clause,
                if let Some(deref) = deref_clause {
                    deref.to_string()
                } else {
                    "".to_string()
                }
            ),
            Self::PrimaryKey {
                columns,
                auto_increment,
                conflict_clause,
            } => format!(
                "PRIMARY KEY ({}){}{}",
                columns
                    .iter()
                    .map(|col| col.to_sql_string(context))
                    .collect::<Vec<_>>()
                    .join(", "),
                conflict_clause.map_or("".to_string(), |conflict| format!(" {conflict}")),
                auto_increment.then_some(" AUTOINCREMENT").unwrap_or("")
            ),
            Self::Unique {
                columns,
                conflict_clause,
            } => format!(
                "UNIQUE ({}){}",
                columns
                    .iter()
                    .map(|col| col.to_sql_string(context))
                    .collect::<Vec<_>>()
                    .join(", "),
                conflict_clause.map_or("".to_string(), |conflict| format!(" {conflict}"))
            ),
        }
    }
}

impl Display for ast::TableOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            if *self == Self::NONE {
                ""
            } else if *self == Self::STRICT {
                " STRICT"
            } else {
                " WITHOUT ROWID"
            }
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::to_sql_string_test;

    to_sql_string_test!(
        test_create_table_simple,
        "CREATE TABLE t (a INTEGER, b TEXT);"
    );

    to_sql_string_test!(
        test_create_table_primary_key,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);"
    );

    to_sql_string_test!(
        test_create_table_multi_primary_key,
        "CREATE TABLE t (a INTEGER, b TEXT, PRIMARY KEY (a, b));"
    );

    to_sql_string_test!(
        test_create_table_data_types,
        "CREATE TABLE t (a INTEGER, b TEXT, c REAL, d BLOB, e NUMERIC);"
    );

    to_sql_string_test!(
        test_create_table_foreign_key,
        "CREATE TABLE t2 (id INTEGER PRIMARY KEY, t_id INTEGER, FOREIGN KEY (t_id) REFERENCES t(id));"
    );

    to_sql_string_test!(
        test_create_table_foreign_key_cascade,
        "CREATE TABLE t2 (id INTEGER PRIMARY KEY, t_id INTEGER, FOREIGN KEY (t_id) REFERENCES t(id) ON DELETE CASCADE);"
    );

    to_sql_string_test!(
        test_create_table_unique,
        "CREATE TABLE t (a INTEGER UNIQUE, b TEXT);"
    );

    to_sql_string_test!(
        test_create_table_not_null,
        "CREATE TABLE t (a INTEGER NOT NULL, b TEXT);"
    );

    to_sql_string_test!(
        test_create_table_check,
        "CREATE TABLE t (a INTEGER CHECK (a > 0), b TEXT);"
    );

    to_sql_string_test!(
        test_create_table_default,
        "CREATE TABLE t (a INTEGER DEFAULT 0, b TEXT);"
    );

    to_sql_string_test!(
        test_create_table_multiple_constraints,
        "CREATE TABLE t (a INTEGER NOT NULL UNIQUE, b TEXT DEFAULT 'default');"
    );

    to_sql_string_test!(
        test_create_table_generated_column,
        "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER AS (a + b));"
    );

    to_sql_string_test!(
        test_create_table_generated_stored,
        "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER AS (a + b) STORED);"
    );

    to_sql_string_test!(
        test_create_table_generated_virtual,
        "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER AS (a + b) VIRTUAL);"
    );

    to_sql_string_test!(
        test_create_table_quoted_columns,
        "CREATE TABLE t (\"select\" INTEGER, \"from\" TEXT);"
    );

    to_sql_string_test!(
        test_create_table_quoted_table,
        "CREATE TABLE \"my table\" (a INTEGER);"
    );

    to_sql_string_test!(
        test_create_table_if_not_exists,
        "CREATE TABLE IF NOT EXISTS t (a INTEGER);"
    );

    to_sql_string_test!(test_create_temp_table, "CREATE TEMP TABLE t (a INTEGER);");

    to_sql_string_test!(
        test_create_table_without_rowid,
        "CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT) WITHOUT ROWID;"
    );

    to_sql_string_test!(
        test_create_table_named_primary_key,
        "CREATE TABLE t (a INTEGER CONSTRAINT pk_a PRIMARY KEY);"
    );

    to_sql_string_test!(
        test_create_table_named_unique,
        "CREATE TABLE t (a INTEGER, CONSTRAINT unique_a UNIQUE (a));"
    );

    to_sql_string_test!(
        test_create_table_named_foreign_key,
        "CREATE TABLE t2 (id INTEGER, t_id INTEGER, CONSTRAINT fk_t FOREIGN KEY (t_id) REFERENCES t(id));"
    );

    to_sql_string_test!(
        test_create_table_complex,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b TEXT DEFAULT 'default', c INTEGER AS (a * 2), CONSTRAINT unique_a UNIQUE (a));"
    );

    to_sql_string_test!(
        test_create_table_multiple_foreign_keys,
        "CREATE TABLE t3 (id INTEGER PRIMARY KEY, t1_id INTEGER, t2_id INTEGER, FOREIGN KEY (t1_id) REFERENCES t1(id), FOREIGN KEY (t2_id) REFERENCES t2(id));"
    );
}
