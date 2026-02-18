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
        let table_names = Self::get_table_names_turso(conn)?;
        let mut builder = SchemaBuilder::new();

        for table_name in table_names {
            let columns = Self::get_columns_turso(conn, &table_name)?;
            if !columns.is_empty() {
                builder = builder.table(Table::new(table_name, columns));
            }
        }

        Ok(builder.build())
    }

    /// Introspect schema from a SQLite connection.
    pub fn from_sqlite(conn: &rusqlite::Connection) -> Result<Schema> {
        let table_names = Self::get_table_names_sqlite(conn)?;
        let mut builder = SchemaBuilder::new();

        for table_name in table_names {
            let columns = Self::get_columns_sqlite(conn, &table_name)?;
            if !columns.is_empty() {
                builder = builder.table(Table::new(table_name, columns));
            }
        }

        Ok(builder.build())
    }

    fn get_table_names_turso(conn: &Arc<turso_core::Connection>) -> Result<Vec<String>> {
        let mut tables = Vec::new();
        let mut rows = conn
            .query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__turso_internal_%' ORDER BY name")
            .context("Failed to query table names")?
            .context("Expected rows from query")?;

        rows.run_with_row_callback(|row| {
            if let turso_core::Value::Text(name) = row.get_value(0) {
                tables.push(name.as_str().to_string());
            }
            Ok(())
        })
        .context("Failed to iterate table names")?;

        Ok(tables)
    }

    fn get_table_names_sqlite(conn: &rusqlite::Connection) -> Result<Vec<String>> {
        let mut stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__turso_internal_%' ORDER BY name")
            .context("Failed to prepare table query")?;

        let tables = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .context("Failed to query tables")?
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("Failed to collect table names")?;

        Ok(tables)
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
        let table_names = Self::get_table_names_turso(conn)?;
        let mut builder = SchemaBuilder::new();

        for table_name in table_names {
            let columns = Self::get_columns_turso(conn, &table_name)?;
            if !columns.is_empty() {
                builder = builder.table(Table::new(table_name, columns));
            }
        }

        // Discover attached databases
        let attached_dbs = Self::get_attached_databases_turso(conn)?;
        for db_name in &attached_dbs {
            builder = builder.database(db_name.clone());
            let tables = Self::get_table_names_turso_db(conn, db_name)?;
            for table_name in tables {
                let query = format!("PRAGMA {db_name}.table_info(\"{table_name}\")");
                let columns = Self::get_columns_turso_query(conn, &query)?;
                if !columns.is_empty() {
                    builder = builder.table(Table::new(table_name, columns).in_database(db_name));
                }
            }
        }

        Ok(builder.build())
    }

    /// Introspect schema from a SQLite connection, including attached databases.
    pub fn from_sqlite_with_attached(conn: &rusqlite::Connection) -> Result<Schema> {
        let table_names = Self::get_table_names_sqlite(conn)?;
        let mut builder = SchemaBuilder::new();

        for table_name in table_names {
            let columns = Self::get_columns_sqlite(conn, &table_name)?;
            if !columns.is_empty() {
                builder = builder.table(Table::new(table_name, columns));
            }
        }

        // Discover attached databases
        let attached_dbs = Self::get_attached_databases_sqlite(conn)?;
        for db_name in &attached_dbs {
            builder = builder.database(db_name.clone());
            let query = format!(
                "SELECT name FROM {db_name}.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__turso_internal_%' ORDER BY name"
            );
            let mut stmt = conn
                .prepare(&query)
                .context("Failed to query attached table names")?;
            let tables = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .context("Failed to query attached tables")?
                .collect::<std::result::Result<Vec<_>, _>>()
                .context("Failed to collect attached table names")?;

            for table_name in tables {
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
                    builder = builder.table(Table::new(table_name, result).in_database(db_name));
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

    /// Get table names from a specific attached database in Turso.
    fn get_table_names_turso_db(
        conn: &Arc<turso_core::Connection>,
        db_name: &str,
    ) -> Result<Vec<String>> {
        let mut tables = Vec::new();
        let query = format!(
            "SELECT name FROM {db_name}.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__turso_internal_%' ORDER BY name"
        );
        let mut rows = conn
            .query(&query)
            .context("Failed to query attached table names")?
            .context("Expected rows from query")?;

        rows.run_with_row_callback(|row| {
            if let turso_core::Value::Text(name) = row.get_value(0) {
                tables.push(name.as_str().to_string());
            }
            Ok(())
        })
        .context("Failed to iterate attached table names")?;

        Ok(tables)
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
