//! Schema introspection from a live database.
//!
//! Uses PRAGMA commands to query the current database schema and converts it
//! to the `sql_gen_prop::Schema` format for query generation.

use std::sync::Arc;

use anyhow::{Context, Result};
use sql_gen_prop::schema::{GeneratedColumn, GeneratedStorage};
use sql_gen_prop::{ColumnDef, DataType, Schema, SchemaBuilder, Table};

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
                builder = builder.add_table(Table::new(table_name, columns));
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
                builder = builder.add_table(Table::new(table_name, columns));
            }
        }

        Ok(builder.build())
    }

    fn get_table_names_turso(conn: &Arc<turso_core::Connection>) -> Result<Vec<String>> {
        let mut tables = Vec::new();
        let mut rows = conn
            .query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
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
            .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
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
        // Use PRAGMA table_xinfo to get column information including generated column status.
        // table_xinfo returns: cid, name, type, notnull, dflt_value, pk, hidden
        // hidden: 0=normal, 2=VIRTUAL generated, 3=STORED generated
        let query = format!("PRAGMA table_xinfo(\"{table_name}\")");
        let mut rows = conn
            .query(&query)
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
                turso_core::Value::Integer(i) => *i != 0,
                _ => false,
            };

            let pk = match row.get_value(5) {
                turso_core::Value::Integer(i) => *i != 0,
                _ => false,
            };

            // hidden column: 0=normal, 2=VIRTUAL generated, 3=STORED generated
            let hidden = match row.get_value(6) {
                turso_core::Value::Integer(i) => *i,
                _ => 0,
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

            // Set generated column info if this is a generated column.
            // Note: We cannot introspect the actual expression from PRAGMA,
            // but we record that it's generated so INSERT/UPDATE exclude it.
            match hidden {
                2 => {
                    column.generated = Some(GeneratedColumn {
                        expr: "/* introspected */".to_string(),
                        storage: GeneratedStorage::Virtual,
                        collation: None,
                    });
                }
                3 => {
                    column.generated = Some(GeneratedColumn {
                        expr: "/* introspected */".to_string(),
                        storage: GeneratedStorage::Stored,
                        collation: None,
                    });
                }
                _ => {}
            }

            columns.push(column);
            Ok(())
        })
        .context("Failed to iterate columns")?;

        Ok(columns)
    }

    fn get_columns_sqlite(conn: &rusqlite::Connection, table_name: &str) -> Result<Vec<ColumnDef>> {
        // Use PRAGMA table_xinfo to get column information including generated column status.
        // table_xinfo returns: cid, name, type, notnull, dflt_value, pk, hidden
        // hidden: 0=normal, 2=VIRTUAL generated, 3=STORED generated
        let query = format!("PRAGMA table_xinfo(\"{table_name}\")");
        let mut stmt = conn.prepare(&query).context("Failed to prepare PRAGMA")?;

        let columns = stmt
            .query_map([], |row| {
                let name: String = row.get(1)?;
                let type_str: String = row.get::<_, String>(2).unwrap_or_else(|_| "TEXT".into());
                let notnull: i64 = row.get(3)?;
                let pk: i64 = row.get(5)?;
                let hidden: i64 = row.get(6)?;

                Ok((name, type_str, notnull != 0, pk != 0, hidden))
            })
            .context("Failed to query columns")?
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("Failed to collect columns")?;

        let mut result = Vec::new();
        for (name, type_str, notnull, pk, hidden) in columns {
            let data_type = Self::parse_type(&type_str.to_uppercase());
            let mut column = ColumnDef::new(name, data_type);

            if notnull || pk {
                column = column.not_null();
            }

            if pk {
                column = column.primary_key();
            }

            // Set generated column info if this is a generated column.
            // Note: We cannot introspect the actual expression from PRAGMA,
            // but we record that it's generated so INSERT/UPDATE exclude it.
            match hidden {
                2 => {
                    column.generated = Some(GeneratedColumn {
                        expr: "/* introspected */".to_string(),
                        storage: GeneratedStorage::Virtual,
                        collation: None,
                    });
                }
                3 => {
                    column.generated = Some(GeneratedColumn {
                        expr: "/* introspected */".to_string(),
                        storage: GeneratedStorage::Stored,
                        collation: None,
                    });
                }
                _ => {}
            }

            result.push(column);
        }

        Ok(result)
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
