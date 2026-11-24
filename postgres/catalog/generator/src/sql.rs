//! The SQL of a catalog as a SQLite table: its `CREATE TABLE` statement and
//! its `CREATE INDEX` statements.
//!
//! The crate documentation gives the column type of each bki type.

use crate::bki::Catalog;
use crate::column::Field;

/// The SQL that stores a catalog in a SQLite table.
pub(crate) struct TableSql {
    /// The `CREATE TABLE` statement.
    pub(crate) create_sql: String,
    /// One `CREATE INDEX` statement for each index of the catalog, except
    /// the unique index on the rowid column, in the order of the file.
    pub(crate) index_sql: Vec<String>,
}

impl TableSql {
    /// The SQL of `catalog`, whose columns are `fields`. `rowid_column` is
    /// the column that is the `INTEGER PRIMARY KEY`, if there is one.
    pub(crate) fn of_catalog(
        catalog: &Catalog,
        fields: &[Field],
        rowid_column: Option<&str>,
    ) -> Result<Self, String> {
        let mut columns = Vec::with_capacity(fields.len());
        for field in fields {
            let column_type = column_type(field.bki_type).ok_or_else(|| {
                format!(
                    "column {} has type {}, which has no SQLite column type",
                    field.name, field.bki_type
                )
            })?;
            columns.push(if Some(field.name) == rowid_column {
                // A NULL in an INTEGER PRIMARY KEY gives a new rowid, so
                // NOT NULL does not apply.
                format!("{} {column_type} PRIMARY KEY", field.name)
            } else if field.nullable {
                format!("{} {column_type}", field.name)
            } else {
                format!("{} {column_type} NOT NULL", field.name)
            });
        }
        let create_sql = format!("CREATE TABLE {}({})", catalog.name, columns.join(", "));

        let mut index_sql = Vec::with_capacity(catalog.indexes.len());
        for index in &catalog.indexes {
            if index.unique && rowid_column.is_some_and(|rowid| index.columns == [rowid]) {
                // The rowid is unique, and the table is in rowid order.
                continue;
            }
            for column in &index.columns {
                if !fields.iter().any(|field| field.name == column) {
                    return Err(format!(
                        "index {} is on {column}, which is not a column",
                        index.name
                    ));
                }
            }
            index_sql.push(format!(
                "CREATE {}INDEX {} ON {}({})",
                if index.unique { "UNIQUE " } else { "" },
                index.name,
                catalog.name,
                index.columns.join(", ")
            ));
        }
        Ok(Self {
            create_sql,
            index_sql,
        })
    }
}

/// The SQLite column type of a bki type. These are the types that the
/// PostgreSQL frontend gives its catalog tables. An array is TEXT, because
/// SQLite has no array type and core decodes arrays only in STRICT tables,
/// which a seeded table cannot be.
pub(crate) fn column_type(bki_type: &str) -> Option<&'static str> {
    Some(match bki_type {
        "oid" | "regproc" | "xid" | "int2" | "int4" | "int8" => "INTEGER",
        "bool" => "BOOLEAN",
        "float4" => "REAL",
        "char" | "name" | "text" | "pg_node_tree" => "TEXT",
        "oidvector" | "int2vector" | "_oid" | "_int2" | "_float4" | "_char" | "_text"
        | "_aclitem" => "TEXT",
        "timestamptz" | "pg_lsn" | "anyarray" | "_pg_statistic" | "pg_ndistinct"
        | "pg_dependencies" | "pg_mcv_list" => "TEXT",
        "bytea" => "BLOB",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bki::parse;
    use crate::column::ColumnKind;

    const PG_CAST: &str = "\
create pg_cast 2605
 (
 oid = oid ,
 castsource = oid ,
 casttarget = oid ,
 castfunc = oid ,
 castcontext = char ,
 castmethod = char
 )
open pg_cast
close pg_cast
declare unique index pg_cast_oid_index 2660 on pg_cast using btree(oid oid_ops)
declare unique index pg_cast_source_target_index 2661 on pg_cast using btree(castsource oid_ops, casttarget oid_ops)
declare index pg_cast_func_index 9999 on pg_cast using btree(castfunc oid_ops)
";

    fn table_sql(bki: &str, rowid_column: Option<&str>) -> Result<TableSql, String> {
        let catalogs = parse(bki).unwrap();
        let fields = Field::of_catalog(&catalogs[0]).unwrap();
        TableSql::of_catalog(&catalogs[0], &fields, rowid_column)
    }

    #[test]
    fn the_oid_is_the_integer_primary_key_and_its_index_is_left_out() {
        let sql = table_sql(PG_CAST, Some("oid")).unwrap();
        assert_eq!(
            sql.create_sql,
            "CREATE TABLE pg_cast(oid INTEGER PRIMARY KEY, castsource INTEGER NOT NULL, \
             casttarget INTEGER NOT NULL, castfunc INTEGER NOT NULL, castcontext TEXT NOT NULL, \
             castmethod TEXT NOT NULL)"
        );
        assert_eq!(
            sql.index_sql,
            [
                "CREATE UNIQUE INDEX pg_cast_source_target_index ON pg_cast(castsource, casttarget)",
                "CREATE INDEX pg_cast_func_index ON pg_cast(castfunc)",
            ]
        );
    }

    #[test]
    fn without_a_rowid_column_each_index_is_kept() {
        let sql = table_sql(PG_CAST, None).unwrap();
        assert!(sql.create_sql.starts_with(
            "CREATE TABLE pg_cast(oid INTEGER NOT NULL, castsource INTEGER NOT NULL,"
        ));
        assert_eq!(
            sql.index_sql[0],
            "CREATE UNIQUE INDEX pg_cast_oid_index ON pg_cast(oid)"
        );
        assert_eq!(sql.index_sql.len(), 3);
    }

    #[test]
    fn a_column_that_can_be_null_has_no_not_null() {
        let bki = "create pg_proc 1255 bootstrap rowtype_oid 81\n (\n oid = oid ,\n \
                   proname = name ,\n procost = float4 ,\n prosecdef = bool ,\n \
                   proargtypes = oidvector FORCE NOT NULL ,\n proargnames = _text ,\n \
                   proargdefaults = pg_node_tree FORCE NULL ,\n probin = text\n )\n";
        let sql = table_sql(bki, None).unwrap();
        assert_eq!(
            sql.create_sql,
            "CREATE TABLE pg_proc(oid INTEGER NOT NULL, proname TEXT NOT NULL, \
             procost REAL NOT NULL, prosecdef BOOLEAN NOT NULL, proargtypes TEXT NOT NULL, \
             proargnames TEXT, proargdefaults TEXT, probin TEXT)"
        );
        assert!(sql.index_sql.is_empty());
    }

    #[test]
    fn an_index_on_a_column_that_does_not_exist_stops_the_generation() {
        let bki = "create pg_am 2601\n (\n oid = oid\n )\n\
                   declare index pg_am_name_index 2651 on pg_am using btree(amname name_ops)\n";
        let error = table_sql(bki, None).err().unwrap();
        assert_eq!(
            error,
            "index pg_am_name_index is on amname, which is not a column"
        );
    }

    /// Each bki type that the generator gives a Rust type also has a SQLite
    /// column type.
    #[test]
    fn each_known_type_has_a_column_type() {
        let types = [
            ("oid", "INTEGER"),
            ("regproc", "INTEGER"),
            ("xid", "INTEGER"),
            ("int2", "INTEGER"),
            ("int4", "INTEGER"),
            ("int8", "INTEGER"),
            ("bool", "BOOLEAN"),
            ("float4", "REAL"),
            ("char", "TEXT"),
            ("name", "TEXT"),
            ("text", "TEXT"),
            ("pg_node_tree", "TEXT"),
            ("oidvector", "TEXT"),
            ("int2vector", "TEXT"),
            ("_oid", "TEXT"),
            ("_int2", "TEXT"),
            ("_float4", "TEXT"),
            ("_char", "TEXT"),
            ("_text", "TEXT"),
            ("_aclitem", "TEXT"),
            ("timestamptz", "TEXT"),
            ("pg_lsn", "TEXT"),
            ("anyarray", "TEXT"),
            ("_pg_statistic", "TEXT"),
            ("pg_ndistinct", "TEXT"),
            ("pg_dependencies", "TEXT"),
            ("pg_mcv_list", "TEXT"),
            ("bytea", "BLOB"),
        ];
        for (bki_type, expected) in types {
            assert!(ColumnKind::of(bki_type).is_some(), "{bki_type}");
            assert_eq!(column_type(bki_type), Some(expected), "{bki_type}");
        }
        assert_eq!(column_type("float8"), None);

        // The file of PostgreSQL has no other type.
        let catalogs = parse(include_str!("../../upstream/postgres.bki")).unwrap();
        for catalog in &catalogs {
            for column in &catalog.columns {
                assert!(
                    types.iter().any(|(known, _)| *known == column.bki_type),
                    "{}.{} has type {}",
                    catalog.name,
                    column.name,
                    column.bki_type
                );
            }
        }
    }
}
