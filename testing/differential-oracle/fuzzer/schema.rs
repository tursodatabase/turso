//! Schema introspection from a live database.
//!
//! Uses PRAGMA commands to query the current database schema and converts it
//! to the `sql_gen::Schema` format for query generation.

use std::sync::Arc;

use anyhow::{Context, Result};
use sql_gen::{ColumnDef, DataType, Schema, SchemaBuilder, Table};

/// Introspects schema from a database connection.
pub struct SchemaIntrospector;

impl SchemaIntrospector {
    /// Introspect schema from a Turso connection.
    pub fn from_turso(conn: &Arc<turso_core::Connection>) -> Result<Schema> {
        let table_info = Self::get_table_info_turso(conn, None)?;
        let mut builder = SchemaBuilder::new();

        for (table_name, strict) in table_info {
            let columns = Self::get_columns_turso(conn, &table_name)?;
            if !columns.is_empty() {
                let table = if strict {
                    Table::new_strict(table_name, columns)
                } else {
                    Table::new(table_name, columns)
                };
                builder = builder.table(table);
            }
        }

        Ok(builder.build())
    }

    /// Introspect schema from a SQLite connection.
    pub fn from_sqlite(conn: &rusqlite::Connection) -> Result<Schema> {
        let table_info = Self::get_table_info_sqlite(conn, None)?;
        let mut builder = SchemaBuilder::new();

        for (table_name, strict) in table_info {
            let columns = Self::get_columns_sqlite(conn, &table_name)?;
            if !columns.is_empty() {
                let table = if strict {
                    Table::new_strict(table_name, columns)
                } else {
                    Table::new(table_name, columns)
                };
                builder = builder.table(table);
            }
        }

        Ok(builder.build())
    }

    /// Get table names and STRICT flags from a Turso connection.
    /// If `db_name` is Some, queries that attached database; otherwise queries `sqlite_master`.
    fn get_table_info_turso(
        conn: &Arc<turso_core::Connection>,
        db_name: Option<&str>,
    ) -> Result<Vec<(String, bool)>> {
        let mut tables = Vec::new();
        let prefix = match db_name {
            Some(db) => format!("{db}."),
            None => String::new(),
        };
        let query = format!(
            "SELECT name, sql FROM {prefix}sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__turso_internal_%' ORDER BY name"
        );
        let mut rows = conn
            .query(&query)
            .context("Failed to query table info")?
            .context("Expected rows from query")?;

        rows.run_with_row_callback(|row| {
            if let turso_core::Value::Text(name) = row.get_value(0) {
                let strict = match row.get_value(1) {
                    turso_core::Value::Text(sql) => Self::sql_is_strict(sql.as_str()),
                    _ => false,
                };
                tables.push((name.as_str().to_string(), strict));
            }
            Ok(())
        })
        .context("Failed to iterate table info")?;

        Ok(tables)
    }

    /// Get table names and STRICT flags from a SQLite connection.
    /// If `db_name` is Some, queries that attached database; otherwise queries `sqlite_master`.
    fn get_table_info_sqlite(
        conn: &rusqlite::Connection,
        db_name: Option<&str>,
    ) -> Result<Vec<(String, bool)>> {
        let prefix = match db_name {
            Some(db) => format!("{db}."),
            None => String::new(),
        };
        let query = format!(
            "SELECT name, sql FROM {prefix}sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__turso_internal_%' ORDER BY name"
        );
        let mut stmt = conn
            .prepare(&query)
            .context("Failed to prepare table query")?;

        let tables = stmt
            .query_map([], |row| {
                let name: String = row.get(0)?;
                let sql: Option<String> = row.get(1)?;
                let strict = sql.is_some_and(|s| Self::sql_is_strict(&s));
                Ok((name, strict))
            })
            .context("Failed to query tables")?
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("Failed to collect table info")?;

        Ok(tables)
    }

    /// Determine if a CREATE TABLE SQL statement declares a STRICT table.
    fn sql_is_strict(sql: &str) -> bool {
        let trimmed = sql.trim_end().to_uppercase();
        if let Some(pos) = trimmed.rfind(')') {
            let after_paren = trimmed[pos + 1..].trim();
            after_paren == "STRICT"
        } else {
            false
        }
    }

    fn get_columns_turso(
        conn: &Arc<turso_core::Connection>,
        table_name: &str,
    ) -> Result<Vec<ColumnDef>> {
        let mut columns = Vec::new();
        // Use PRAGMA table_info to get column information
        let query = format!("PRAGMA table_info(\"{table_name}\")");
        let mut rows = conn
            .query(&query)
            .context("Failed to query column info")?
            .context("Expected rows from PRAGMA")?;

        rows.run_with_row_callback(|row| {
            // PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
            let name = match row.get_value(1) {
                turso_core::Value::Text(s) => s.as_str().to_string(),
                _ => return Ok(()),
            };

            let type_str = match row.get_value(2) {
                turso_core::Value::Text(s) => s.as_str().to_uppercase(),
                _ => "TEXT".to_string(),
            };

            let notnull = match row.get_value(3) {
                turso_core::Value::Numeric(turso_core::Numeric::Integer(i)) => *i != 0,
                _ => false,
            };

            let pk = match row.get_value(5) {
                turso_core::Value::Numeric(turso_core::Numeric::Integer(i)) => *i != 0,
                _ => false,
            };

            let data_type = Self::parse_type(&type_str);
            let mut column = ColumnDef::new(name, data_type);

            if !notnull && !pk {
                // Column is nullable (default)
            } else {
                column = column.not_null();
            }

            if pk {
                column = column.primary_key();
            }

            columns.push(column);
            Ok(())
        })
        .context("Failed to iterate columns")?;

        Ok(columns)
    }

    fn get_columns_sqlite(conn: &rusqlite::Connection, table_name: &str) -> Result<Vec<ColumnDef>> {
        let query = format!("PRAGMA table_info(\"{table_name}\")");
        let mut stmt = conn.prepare(&query).context("Failed to prepare PRAGMA")?;

        let columns = stmt
            .query_map([], |row| {
                let name: String = row.get(1)?;
                let type_str: String = row.get::<_, String>(2).unwrap_or_else(|_| "TEXT".into());
                let notnull: i64 = row.get(3)?;
                let pk: i64 = row.get(5)?;

                Ok((name, type_str, notnull != 0, pk != 0))
            })
            .context("Failed to query columns")?
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("Failed to collect columns")?;

        let mut result = Vec::new();
        for (name, type_str, notnull, pk) in columns {
            let data_type = Self::parse_type(&type_str.to_uppercase());
            let mut column = ColumnDef::new(name, data_type);

            if notnull || pk {
                column = column.not_null();
            }

            if pk {
                column = column.primary_key();
            }

            result.push(column);
        }

        Ok(result)
    }

    /// Introspect schema from a Turso connection, including attached databases.
    pub fn from_turso_with_attached(conn: &Arc<turso_core::Connection>) -> Result<Schema> {
        let table_info = Self::get_table_info_turso(conn, None)?;
        let mut builder = SchemaBuilder::new();

        for (table_name, strict) in table_info {
            let columns = Self::get_columns_turso(conn, &table_name)?;
            if !columns.is_empty() {
                let table = if strict {
                    Table::new_strict(table_name, columns)
                } else {
                    Table::new(table_name, columns)
                };
                builder = builder.table(table);
            }
        }

        // Discover attached databases
        let attached_dbs = Self::get_attached_databases_turso(conn)?;
        for db_name in &attached_dbs {
            builder = builder.database(db_name.clone());
            let attached_tables = Self::get_table_info_turso(conn, Some(db_name))?;
            for (table_name, strict) in attached_tables {
                let query = format!("PRAGMA {db_name}.table_info(\"{table_name}\")");
                let columns = Self::get_columns_turso_query(conn, &query)?;
                if !columns.is_empty() {
                    let table = if strict {
                        Table::new_strict(table_name, columns)
                    } else {
                        Table::new(table_name, columns)
                    };
                    builder = builder.table(table.in_database(db_name));
                }
            }
        }

        Ok(builder.build())
    }

    /// Introspect schema from a SQLite connection, including attached databases.
    pub fn from_sqlite_with_attached(conn: &rusqlite::Connection) -> Result<Schema> {
        let table_info = Self::get_table_info_sqlite(conn, None)?;
        let mut builder = SchemaBuilder::new();

        for (table_name, strict) in table_info {
            let columns = Self::get_columns_sqlite(conn, &table_name)?;
            if !columns.is_empty() {
                let table = if strict {
                    Table::new_strict(table_name, columns)
                } else {
                    Table::new(table_name, columns)
                };
                builder = builder.table(table);
            }
        }

        // Discover attached databases
        let attached_dbs = Self::get_attached_databases_sqlite(conn)?;
        for db_name in &attached_dbs {
            builder = builder.database(db_name.clone());
            let attached_tables = Self::get_table_info_sqlite(conn, Some(db_name))?;

            for (table_name, strict) in attached_tables {
                let col_query = format!("PRAGMA {db_name}.table_info(\"{table_name}\")");
                let mut col_stmt = conn
                    .prepare(&col_query)
                    .context("Failed to prepare attached PRAGMA")?;
                let columns = col_stmt
                    .query_map([], |row| {
                        let name: String = row.get(1)?;
                        let type_str: String =
                            row.get::<_, String>(2).unwrap_or_else(|_| "TEXT".into());
                        let notnull: i64 = row.get(3)?;
                        let pk: i64 = row.get(5)?;
                        Ok((name, type_str, notnull != 0, pk != 0))
                    })
                    .context("Failed to query attached columns")?
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .context("Failed to collect attached columns")?;

                let mut result = Vec::new();
                for (name, type_str, notnull, pk) in columns {
                    let data_type = Self::parse_type(&type_str.to_uppercase());
                    let mut column = ColumnDef::new(name, data_type);
                    if notnull || pk {
                        column = column.not_null();
                    }
                    if pk {
                        column = column.primary_key();
                    }
                    result.push(column);
                }

                if !result.is_empty() {
                    let table = if strict {
                        Table::new_strict(table_name, result)
                    } else {
                        Table::new(table_name, result)
                    };
                    builder = builder.table(table.in_database(db_name));
                }
            }
        }

        Ok(builder.build())
    }

    /// Get names of attached databases (excluding "main" and "temp") from Turso.
    fn get_attached_databases_turso(conn: &Arc<turso_core::Connection>) -> Result<Vec<String>> {
        let mut databases = Vec::new();
        let mut rows = conn
            .query("PRAGMA database_list")
            .context("Failed to query database_list")?
            .context("Expected rows from PRAGMA database_list")?;

        rows.run_with_row_callback(|row| {
            if let turso_core::Value::Text(name) = row.get_value(1) {
                let name = name.as_str();
                if name != "main" && name != "temp" {
                    databases.push(name.to_string());
                }
            }
            Ok(())
        })
        .context("Failed to iterate database_list")?;

        Ok(databases)
    }

    /// Get names of attached databases (excluding "main" and "temp") from SQLite.
    fn get_attached_databases_sqlite(conn: &rusqlite::Connection) -> Result<Vec<String>> {
        let mut stmt = conn
            .prepare("PRAGMA database_list")
            .context("Failed to prepare database_list query")?;

        let databases = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .context("Failed to query database_list")?
            .filter_map(|r| r.ok())
            .filter(|name| name != "main" && name != "temp")
            .collect();

        Ok(databases)
    }

    /// Get columns from a Turso connection using a custom PRAGMA query.
    fn get_columns_turso_query(
        conn: &Arc<turso_core::Connection>,
        query: &str,
    ) -> Result<Vec<ColumnDef>> {
        let mut columns = Vec::new();
        let mut rows = conn
            .query(query)
            .context("Failed to query column info")?
            .context("Expected rows from PRAGMA")?;

        rows.run_with_row_callback(|row| {
            let name = match row.get_value(1) {
                turso_core::Value::Text(s) => s.as_str().to_string(),
                _ => return Ok(()),
            };

            let type_str = match row.get_value(2) {
                turso_core::Value::Text(s) => s.as_str().to_uppercase(),
                _ => "TEXT".to_string(),
            };

            let notnull = match row.get_value(3) {
                turso_core::Value::Numeric(turso_core::Numeric::Integer(i)) => *i != 0,
                _ => false,
            };

            let pk = match row.get_value(5) {
                turso_core::Value::Numeric(turso_core::Numeric::Integer(i)) => *i != 0,
                _ => false,
            };

            let data_type = Self::parse_type(&type_str);
            let mut column = ColumnDef::new(name, data_type);

            if !notnull && !pk {
                // Column is nullable (default)
            } else {
                column = column.not_null();
            }

            if pk {
                column = column.primary_key();
            }

            columns.push(column);
            Ok(())
        })
        .context("Failed to iterate columns")?;

        Ok(columns)
    }

    fn parse_type(type_str: &str) -> DataType {
        // SQLite type affinity rules (simplified)
        let upper = type_str.to_uppercase();
        if upper.contains("INT") {
            DataType::Integer
        } else if upper.contains("REAL")
            || upper.contains("FLOA")
            || upper.contains("DOUB")
            || upper.contains("NUMERIC")
        {
            DataType::Real
        } else if upper.contains("BLOB") || upper.is_empty() {
            DataType::Blob
        } else {
            // TEXT affinity for everything else (CHAR, CLOB, TEXT, VARCHAR, etc.)
            DataType::Text
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_type() {
        assert!(matches!(
            SchemaIntrospector::parse_type("INTEGER"),
            DataType::Integer
        ));
        assert!(matches!(
            SchemaIntrospector::parse_type("INT"),
            DataType::Integer
        ));
        assert!(matches!(
            SchemaIntrospector::parse_type("BIGINT"),
            DataType::Integer
        ));
        assert!(matches!(
            SchemaIntrospector::parse_type("REAL"),
            DataType::Real
        ));
        assert!(matches!(
            SchemaIntrospector::parse_type("FLOAT"),
            DataType::Real
        ));
        assert!(matches!(
            SchemaIntrospector::parse_type("DOUBLE"),
            DataType::Real
        ));
        assert!(matches!(
            SchemaIntrospector::parse_type("TEXT"),
            DataType::Text
        ));
        assert!(matches!(
            SchemaIntrospector::parse_type("VARCHAR(255)"),
            DataType::Text
        ));
        assert!(matches!(
            SchemaIntrospector::parse_type("BLOB"),
            DataType::Blob
        ));
    }
}
