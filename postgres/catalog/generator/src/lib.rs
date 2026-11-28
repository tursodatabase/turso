//! Generates the Rust tables of `turso_pg_catalog` from `postgres.bki`.
//!
//! `postgres.bki` is what PostgreSQL's `genbki.pl` writes from the catalog
//! data files of its source tree: every built-in row of every system
//! catalog, with each reference to another row already an oid and each
//! default filled in. `initdb` reads it to fill a new cluster.
//!
//! The file has one block per catalog:
//!
//! ```text
//! create pg_cast 2605
//!  (
//!  oid = oid ,
//!  castsource = oid ,
//!  castcontext = char
//!  )
//! open pg_cast
//! insert ( 10000 20 21 714 a f )
//! close pg_cast
//! ```
//!
//! A catalog that `create` marks `bootstrap` has no `open` line: its rows
//! follow the column list. The values of a row are separated by spaces.
//! `_null_` is NULL. A value with a space or punctuation is in single
//! quotes, and a quote inside it is written twice.
//!
//! The generator has these parts, one in each module:
//!
//! - `grammar` reads the commands of the file with chumsky parsers.
//! - `bki` puts the rows of each catalog together.
//! - `column` gives a column its Rust type, and a value its Rust expression
//!   and its SQL value.
//! - `sql` writes the `CREATE TABLE` and `CREATE INDEX` statements of a
//!   catalog.
//! - `table` works out the columns, the key, the SQL and the order of the
//!   rows of a catalog once, for all that the generator writes.
//! - `version` computes the version of the built-in rows of a catalog.
//! - `emit` writes the struct, the table, the lookup, the SQL and the
//!   built-in catalog of a catalog.
//! - `text_out`, a file of `turso_pg_catalog`, gives the text that
//!   PostgreSQL prints for a vector, an array and a `float4`.
//!
//! Each catalog of the file gets a struct and a table. A catalog without
//! built-in rows gets an empty table. A catalog gets a lookup by oid when
//! a unique index of the file is on its first column alone.
//!
//! # The catalog as a SQLite table
//!
//! Each catalog also gets the SQL that stores it in a SQLite table:
//! `CREATE_SQL`, a `CREATE TABLE` statement with the columns of the file
//! in their order, and `INDEX_SQL`, a `CREATE INDEX` statement for each
//! `declare index` and `declare unique index` line of the catalog, with
//! the name of the index in PostgreSQL. The statements have no collations
//! and no operator classes.
//!
//! A column is `NOT NULL` when PostgreSQL makes it NOT NULL: the column
//! list forces it with `FORCE NOT NULL`, or the column and each column
//! before it have a type of fixed width, as `genbki.pl` decides. When a
//! unique index is on the column `oid` alone, `oid` is the
//! `INTEGER PRIMARY KEY`, and `INDEX_SQL` leaves out that index.
//!
//! The column types are those that the PostgreSQL frontend gives its
//! catalog tables:
//!
//! | bki type                                          | SQLite type |
//! |---------------------------------------------------|-------------|
//! | `oid`, `regproc`, `xid`, `int2`, `int4`, `int8`   | `INTEGER`   |
//! | `bool`                                            | `BOOLEAN`   |
//! | `float4`                                          | `REAL`      |
//! | `char`, `name`, `text`, `pg_node_tree`            | `TEXT`      |
//! | `oidvector`, `int2vector` and each array type     | `TEXT`      |
//! | `timestamptz`, `pg_lsn`, `anyarray`, `_pg_statistic`, `pg_ndistinct`, `pg_dependencies`, `pg_mcv_list` | `TEXT` |
//! | `bytea`                                           | `BLOB`      |
//!
//! An array is `TEXT` and not an array type such as `TEXT[]`, because core
//! decodes arrays only in STRICT tables, and a built-in catalog cannot be
//! STRICT. A `regproc` holds the oid of a function, as PostgreSQL stores
//! it.

//!
//! # The built-in rows of a catalog
//!
//! Each catalog gets a `BuiltinCatalog` of `turso_pg_catalog`: `PG_CAST_CATALOG`
//! for `pg_cast`, with `CREATE_SQL`, `INDEX_SQL` and the built-in rows of the
//! table of the catalog. `BUILTIN_CATALOGS` has them all. The built-in rows write the
//! values of a row in the order of the columns of `CREATE_SQL`, as
//! PostgreSQL 18 prints them in its text output format:
//!
//! | bki type                                    | SQL value                         |
//! |---------------------------------------------|-----------------------------------|
//! | `oid`, `regproc`, `xid`, `int2`, `int4`, `int8` | integer                       |
//! | `bool`                                      | integer 1 or 0                    |
//! | `float4`                                    | real: the `f64` of the shortest text of the `f32`, so `0.01` and not `0.009999999776482582` |
//! | `char`                                      | text of one ASCII character       |
//! | `name`, `text`, `pg_node_tree`              | text, as in the file              |
//! | `oidvector`, `int2vector`                   | text: the elements separated by spaces, such as `23 25`, and an empty text for no elements |
//! | `_oid`, `_int2`, `_float4`, `_char`, `_text`, `_aclitem` | text: the elements in braces, separated by commas, such as `{i,o}`, with the quotes of `array_out` |
//! | the other types                             | none: a value stops the generation |
//!
//! NULL is NULL. A `float4` in an array has the text of `float4out`, such
//! as `1e+10`. The rowid of a row is its oid when `oid` is the
//! `INTEGER PRIMARY KEY`, and otherwise its position in the table plus 1.
//! The version of the built-in rows is computed when the file is generated: the
//! module `version` describes it.

mod bki;
mod column;
mod emit;
mod grammar;
mod sql;
mod table;
#[path = "../../src/text_out.rs"]
mod text_out;
mod version;

use table::Table;

/// A value of a column of a built-in catalog, as the built-in rows of the catalog
/// write it.
#[derive(Clone, Debug, PartialEq)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
}

/// The Rust source of a struct, a table and a built-in catalog for each
/// catalog of the file, from the text of `postgres.bki`. Fails when the
/// file has something this generator does not know: a command, a column
/// type, a symbol or a quoting form. A new PostgreSQL release then stops
/// the generation instead of producing wrong rows.
pub fn generate(bki: &str) -> Result<String, String> {
    let catalogs = bki::parse(bki)?;
    let mut code = String::from(emit::HEADER);
    let mut tables = Vec::with_capacity(catalogs.len());
    for catalog in &catalogs {
        let table = Table::of_catalog(catalog)?;
        emit::emit_catalog(&mut code, &table)?;
        tables.push(table);
    }
    emit::emit_builtin_catalogs(&mut code, &tables);
    Ok(code)
}

/// The built-in rows of a catalog: the values of each row, in the order of the table.
pub type BuiltinValues = Vec<Vec<SqlValue>>;

/// The SQL values of the rows of each catalog of the file, under the name
/// of the catalog, in the order of its table: what its built-in rows
/// write. A test of `turso_pg_catalog` compares the built-in rows with them.
pub fn builtin_values(bki: &str) -> Result<Vec<(String, BuiltinValues)>, String> {
    let catalogs = bki::parse(bki)?;
    let mut values = Vec::with_capacity(catalogs.len());
    for catalog in &catalogs {
        let table = Table::of_catalog(catalog)?;
        values.push((catalog.name.clone(), table.sql_rows()?));
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_file_of_postgresql_has_a_table_for_each_catalog() {
        let code = generate(include_str!("../../upstream/postgres.bki")).unwrap();
        // PostgreSQL 18 has 64 catalogs and 3,397 built-in functions.
        assert_eq!(code.matches("\npub static PG_").count(), 64);
        assert!(code.contains("pub static PG_PROC: [PgProc; 3397] = ["));
        // A catalog without built-in rows has an empty table.
        assert!(code.contains("pub static PG_TRIGGER: [PgTrigger; 0] = ["));
    }

    #[test]
    fn a_catalog_whose_rows_are_left_out_says_why() {
        let code = generate(include_str!("../../upstream/postgres.bki")).unwrap();
        assert!(code.contains(
            "/// `pg_database` without rows: its row has the encoding and the locale of the \
             cluster, which only `initdb` knows.\n\
             pub static PG_DATABASE: [PgDatabase; 0] = ["
        ));
        assert!(code.contains("pub static PG_CLASS: [PgClass; 0] = ["));
        assert!(code.contains("pub static PG_ATTRIBUTE: [PgAttribute; 0] = ["));
    }

    #[test]
    fn each_catalog_is_a_builtin_catalog() {
        let code = generate(include_str!("../../upstream/postgres.bki")).unwrap();
        assert!(code.contains("pub static BUILTIN_CATALOGS: [BuiltinCatalog; 64] = ["));
        assert_eq!(code.matches("\npub const PG_").count(), 64);
        assert!(code.contains("    PG_PROC_CATALOG,\n"));
    }

    #[test]
    fn the_builtin_values_are_in_the_order_of_the_table() {
        let values = builtin_values(include_str!("../../upstream/postgres.bki")).unwrap();
        assert_eq!(values.len(), 64);
        let (_, pg_cast) = values.iter().find(|(name, _)| name == "pg_cast").unwrap();
        // pg_cast is in the order of its oid.
        assert_eq!(
            pg_cast[0],
            [
                SqlValue::Integer(10000),
                SqlValue::Integer(20),
                SqlValue::Integer(21),
                SqlValue::Integer(714),
                SqlValue::Text("a".to_string()),
                SqlValue::Text("f".to_string()),
            ]
        );
        let (_, pg_class) = values.iter().find(|(name, _)| name == "pg_class").unwrap();
        assert!(pg_class.is_empty());
    }
}
