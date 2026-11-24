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
//! The generator has four parts, one in each module:
//!
//! - `grammar` reads the commands of the file with chumsky parsers.
//! - `bki` puts the rows of each catalog together.
//! - `column` gives a column its Rust type and a value its Rust expression.
//! - `emit` writes the struct, the table and the lookup of a catalog.
//!
//! Each catalog of the file gets a struct and a table. A catalog without
//! built-in rows gets an empty table. A catalog gets a lookup by oid when
//! a unique index of the file is on its first column alone.

mod bki;
mod column;
mod emit;
mod grammar;

/// The Rust source of a struct and a table for each catalog of the file,
/// from the text of `postgres.bki`. Fails when the file has something this
/// generator does not know: a command, a column type, a symbol or a quoting
/// form. A new PostgreSQL release then stops the generation instead of
/// producing wrong rows.
pub fn generate(bki: &str) -> Result<String, String> {
    let catalogs = bki::parse(bki)?;
    let mut code = String::from(emit::HEADER);
    for catalog in &catalogs {
        emit::emit_catalog(&mut code, catalog)?;
    }
    Ok(code)
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
}
