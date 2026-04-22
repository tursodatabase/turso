//! Schema introspection from a live database.
//!
//! Uses PRAGMA commands to query the current database schema and converts it
//! to the `sql_gen::Schema` format for query generation.

use std::sync::Arc;

use anyhow::{Context, Result};
use sql_gen::{ColumnDef, DataType, Index, Schema, SchemaBuilder, Table};

/// Introspects schema from a database connection.
pub struct SchemaIntrospector;

impl SchemaIntrospector {
    /// Introspect schema from a Turso connection.
    pub fn from_turso(conn: &Arc<turso_core::Connection>) -> Result<Schema> {
        Self::populate_turso_schema(conn, SchemaBuilder::new(), None).map(SchemaBuilder::build)
    }

    /// Introspect schema from a SQLite connection.
    pub fn from_sqlite(conn: &rusqlite::Connection) -> Result<Schema> {
        Self::populate_sqlite_schema(conn, SchemaBuilder::new(), None).map(SchemaBuilder::build)
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

    /// Introspect schema from a Turso connection, including attached databases.
    pub fn from_turso_with_attached(conn: &Arc<turso_core::Connection>) -> Result<Schema> {
        let mut builder = SchemaBuilder::new();
        builder = Self::populate_turso_schema(conn, builder, None)?;
        builder = Self::populate_turso_schema(conn, builder, Some("temp"))?;

        for db_name in Self::get_attached_databases_turso(conn)? {
            if db_name == "temp" {
                continue;
            }
            builder = Self::populate_turso_schema(conn, builder, Some(&db_name))?;
        }

        Ok(builder.build())
    }

    /// Introspect schema from a SQLite connection, including attached databases.
    pub fn from_sqlite_with_attached(conn: &rusqlite::Connection) -> Result<Schema> {
        let mut builder = SchemaBuilder::new();
        builder = Self::populate_sqlite_schema(conn, builder, None)?;
        builder = Self::populate_sqlite_schema(conn, builder, Some("temp"))?;

        for db_name in Self::get_attached_databases_sqlite(conn)? {
            if db_name == "temp" {
                continue;
            }
            builder = Self::populate_sqlite_schema(conn, builder, Some(&db_name))?;
        }

        Ok(builder.build())
    }

    /// Get names of non-main databases, including `temp`, from Turso.
    fn get_attached_databases_turso(conn: &Arc<turso_core::Connection>) -> Result<Vec<String>> {
        let mut databases = Vec::new();
        let mut rows = conn
            .query("PRAGMA database_list")
            .context("Failed to query database_list")?
            .context("Expected rows from PRAGMA database_list")?;

        rows.run_with_row_callback(|row| {
            if let turso_core::Value::Text(name) = row.get_value(1) {
                let name = name.as_str();
                if name != "main" {
                    databases.push(name.to_string());
                }
            }
            Ok(())
        })
        .context("Failed to iterate database_list")?;

        Ok(databases)
    }

    /// Get names of non-main databases, including `temp`, from SQLite.
    fn get_attached_databases_sqlite(conn: &rusqlite::Connection) -> Result<Vec<String>> {
        let mut stmt = conn
            .prepare("PRAGMA database_list")
            .context("Failed to prepare database_list query")?;

        let databases = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .context("Failed to query database_list")?
            .filter_map(|r| r.ok())
            .filter(|name| name != "main")
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

    fn populate_turso_schema(
        conn: &Arc<turso_core::Connection>,
        mut builder: SchemaBuilder,
        db_name: Option<&str>,
    ) -> Result<SchemaBuilder> {
        if let Some(db_name) = db_name {
            builder = builder.database(db_name.to_string());
        }

        for (table_name, strict) in Self::get_table_info_turso(conn, db_name)? {
            let columns_query = match db_name {
                Some(db_name) => format!("PRAGMA {db_name}.table_info(\"{table_name}\")"),
                None => format!("PRAGMA table_info(\"{table_name}\")"),
            };
            let columns = Self::get_columns_turso_query(conn, &columns_query)?;
            if !columns.is_empty() {
                let table = if strict {
                    Table::new_strict(table_name.clone(), columns)
                } else {
                    Table::new(table_name.clone(), columns)
                };
                let table = match db_name {
                    Some(db_name) => table.in_database(db_name.to_string()),
                    None => table,
                };
                builder = builder.table(table);
            }

            for index in Self::get_indexes_turso(conn, db_name, &table_name)? {
                builder = builder.index(index);
            }
        }

        Ok(builder)
    }

    fn populate_sqlite_schema(
        conn: &rusqlite::Connection,
        mut builder: SchemaBuilder,
        db_name: Option<&str>,
    ) -> Result<SchemaBuilder> {
        if let Some(db_name) = db_name {
            builder = builder.database(db_name.to_string());
        }

        for (table_name, strict) in Self::get_table_info_sqlite(conn, db_name)? {
            let columns = Self::get_columns_sqlite_query(conn, db_name, &table_name)?;
            if !columns.is_empty() {
                let table = if strict {
                    Table::new_strict(table_name.clone(), columns)
                } else {
                    Table::new(table_name.clone(), columns)
                };
                let table = match db_name {
                    Some(db_name) => table.in_database(db_name.to_string()),
                    None => table,
                };
                builder = builder.table(table);
            }

            for index in Self::get_indexes_sqlite(conn, db_name, &table_name)? {
                builder = builder.index(index);
            }
        }

        Ok(builder)
    }

    fn get_columns_sqlite_query(
        conn: &rusqlite::Connection,
        db_name: Option<&str>,
        table_name: &str,
    ) -> Result<Vec<ColumnDef>> {
        let query = match db_name {
            Some(db_name) => format!("PRAGMA {db_name}.table_info(\"{table_name}\")"),
            None => format!("PRAGMA table_info(\"{table_name}\")"),
        };
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

    fn get_indexes_turso(
        conn: &Arc<turso_core::Connection>,
        db_name: Option<&str>,
        table_name: &str,
    ) -> Result<Vec<Index>> {
        let query = match db_name {
            Some(db_name) => format!("PRAGMA {db_name}.index_list(\"{table_name}\")"),
            None => format!("PRAGMA index_list(\"{table_name}\")"),
        };
        let mut indexes = Vec::new();
        let mut rows = conn
            .query(&query)
            .context("Failed to query index info")?
            .context("Expected rows from PRAGMA index_list")?;

        rows.run_with_row_callback(|row| {
            let name = match row.get_value(1) {
                turso_core::Value::Text(s) => s.as_str().to_string(),
                _ => return Ok(()),
            };
            let unique = match row.get_value(2) {
                turso_core::Value::Numeric(turso_core::Numeric::Integer(i)) => *i != 0,
                _ => false,
            };
            let origin = match row.get_value(3) {
                turso_core::Value::Text(s) => Some(s.as_str().to_string()),
                _ => None,
            };
            if origin.as_deref().is_some_and(|origin| origin != "c")
                || name.starts_with("sqlite_autoindex_")
            {
                return Ok(());
            }

            let columns = Self::get_index_columns_turso(conn, db_name, &name).map_err(|err| {
                turso_core::LimboError::InternalError(format!(
                    "Failed to introspect index columns for {name}: {err}"
                ))
            })?;
            let mut index = Index::new(name, table_name.to_string(), columns);
            if unique {
                index = index.unique();
            }
            if let Some(db_name) = db_name {
                index = index.in_database(db_name.to_string());
            }
            indexes.push(index);
            Ok(())
        })
        .context("Failed to iterate index_list")?;

        Ok(indexes)
    }

    fn get_indexes_sqlite(
        conn: &rusqlite::Connection,
        db_name: Option<&str>,
        table_name: &str,
    ) -> Result<Vec<Index>> {
        let query = match db_name {
            Some(db_name) => format!("PRAGMA {db_name}.index_list(\"{table_name}\")"),
            None => format!("PRAGMA index_list(\"{table_name}\")"),
        };
        let mut stmt = conn
            .prepare(&query)
            .context("Failed to prepare PRAGMA index_list")?;

        let mut indexes = Vec::new();
        let rows = stmt
            .query_map([], |row| {
                let name: String = row.get(1)?;
                let unique: i64 = row.get(2)?;
                let origin: String = row.get::<_, String>(3)?;
                Ok((name, unique != 0, origin))
            })
            .context("Failed to query index_list")?;

        for row in rows {
            let (name, unique, origin) = row.context("Failed to read index_list row")?;
            if origin != "c" || name.starts_with("sqlite_autoindex_") {
                continue;
            }
            let columns = Self::get_index_columns_sqlite(conn, db_name, &name)?;
            let mut index = Index::new(name, table_name.to_string(), columns);
            if unique {
                index = index.unique();
            }
            if let Some(db_name) = db_name {
                index = index.in_database(db_name.to_string());
            }
            indexes.push(index);
        }

        Ok(indexes)
    }

    fn get_index_columns_turso(
        conn: &Arc<turso_core::Connection>,
        db_name: Option<&str>,
        index_name: &str,
    ) -> Result<Vec<String>> {
        let query = match db_name {
            Some(db_name) => format!("PRAGMA {db_name}.index_info(\"{index_name}\")"),
            None => format!("PRAGMA index_info(\"{index_name}\")"),
        };
        let mut columns = Vec::new();
        let mut rows = conn
            .query(&query)
            .context("Failed to query index columns")?
            .context("Expected rows from PRAGMA index_info")?;

        rows.run_with_row_callback(|row| {
            // Expression index columns have cid < 0 (typically -2).
            // Skip them since they don't map to a named column.
            let cid = match row.get_value(1) {
                turso_core::Value::Numeric(turso_core::Numeric::Integer(i)) => *i,
                _ => return Ok(()),
            };
            if cid < 0 {
                return Ok(());
            }
            if let turso_core::Value::Text(name) = row.get_value(2) {
                columns.push(name.as_str().to_string());
            }
            Ok(())
        })
        .context("Failed to iterate index_info")?;

        Ok(columns)
    }

    fn get_index_columns_sqlite(
        conn: &rusqlite::Connection,
        db_name: Option<&str>,
        index_name: &str,
    ) -> Result<Vec<String>> {
        let query = match db_name {
            Some(db_name) => format!("PRAGMA {db_name}.index_info(\"{index_name}\")"),
            None => format!("PRAGMA index_info(\"{index_name}\")"),
        };
        let mut stmt = conn
            .prepare(&query)
            .context("Failed to prepare PRAGMA index_info")?;

        // Expression index columns return NULL for the column name.
        // We skip them since they don't map to a named column.
        let columns = stmt
            .query_map([], |row| row.get::<_, Option<String>>(2))
            .context("Failed to query index_info")?
            .filter_map(|r| r.ok().flatten())
            .collect();
        Ok(columns)
    }

    fn parse_type(type_str: &str) -> DataType {
        // SQLite type affinity rules (simplified)
        let upper = type_str.to_uppercase();
        // Check for array types first (e.g., "INTEGER[]", "TEXT[]", "REAL[]")
        if upper.ends_with("[]") {
            let base = upper.trim_end_matches("[]").trim();
            return if base.contains("INT") {
                DataType::IntegerArray
            } else if base.contains("REAL") || base.contains("FLOA") || base.contains("DOUB") {
                DataType::RealArray
            } else {
                DataType::TextArray
            };
        }
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
    use std::collections::HashSet;

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

    #[test]
    fn test_sqlite_with_attached_always_includes_temp_database() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();

        let schema = SchemaIntrospector::from_sqlite_with_attached(&conn).unwrap();

        assert!(
            schema.attached_databases.contains(&"temp".to_string()),
            "temp should be available as a fuzz target even before temp objects exist"
        );
    }

    #[test]
    fn test_sqlite_with_attached_discovers_temp_tables() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute("CREATE TEMP TABLE t(x INTEGER)", []).unwrap();

        let schema = SchemaIntrospector::from_sqlite_with_attached(&conn).unwrap();
        let temp_tables = schema.table_names_in_database(Some("temp"));

        assert_eq!(temp_tables, HashSet::from([String::from("t")]));
    }
}
