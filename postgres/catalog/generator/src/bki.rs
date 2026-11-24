//! The catalogs of a `postgres.bki` file: their columns and their rows.
//!
//! [`crate::grammar`] reads the commands of the file. This module puts the
//! rows of each catalog together.

use chumsky::Parser;

use crate::grammar::{file_parser, Statement};

pub(crate) struct Catalog {
    pub(crate) name: String,
    pub(crate) columns: Vec<Column>,
    pub(crate) rows: Vec<Vec<Token>>,
    /// The columns of each unique index of the catalog.
    pub(crate) unique_indexes: Vec<Vec<String>>,
}

pub(crate) struct Column {
    pub(crate) name: String,
    /// The type name as the file spells it, such as `oid` or `_text`.
    pub(crate) bki_type: String,
    pub(crate) nullness: Nullness,
}

/// What the column list says about NULL in a column.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Nullness {
    /// Nothing. PostgreSQL then decides from the type of the column and
    /// from the columns before it.
    Auto,
    ForceNotNull,
    ForceNull,
}

/// One value of an `insert` line.
#[derive(Clone, PartialEq)]
pub(crate) enum Token {
    Bare(String),
    Quoted(String),
}

impl Token {
    pub(crate) fn text(&self) -> &str {
        match self {
            Token::Bare(text) | Token::Quoted(text) => text,
        }
    }

    pub(crate) fn is_null(&self) -> bool {
        *self == Token::Bare("_null_".to_string())
    }
}

/// Reads the catalogs of the file: their columns and their rows.
pub(crate) fn parse(bki: &str) -> Result<Vec<Catalog>, String> {
    // The number of the line that the byte at `offset` is in.
    let line_of = |offset: usize| bki[..offset].matches('\n').count() + 1;

    let statements = file_parser().parse(bki).into_result().map_err(|errors| {
        let error = errors.first().expect("a failed parse has an error");
        format!("line {}: {error}", line_of(error.span().start))
    })?;

    let mut catalogs: Vec<Catalog> = Vec::new();
    // The index in `catalogs` of the catalog that `insert` lines add to.
    let mut open: Option<usize> = None;
    for (statement, span) in statements {
        let at = |message: String| format!("line {}: {message}", line_of(span.start));
        match statement {
            Statement::Create { name, columns } => {
                catalogs.push(Catalog {
                    name: name.to_string(),
                    columns,
                    rows: Vec::new(),
                    unique_indexes: Vec::new(),
                });
                // A bootstrap catalog has no `open` line.
                open = Some(catalogs.len() - 1);
            }
            Statement::Open(name) => {
                let position = catalogs
                    .iter()
                    .position(|catalog| catalog.name == name)
                    .ok_or_else(|| at(format!("open of {name}, which no create declared")))?;
                open = Some(position);
            }
            Statement::Close => open = None,
            Statement::Insert(row) => {
                let position = open.ok_or_else(|| at("insert outside of a catalog".to_string()))?;
                let catalog = &mut catalogs[position];
                if row.len() != catalog.columns.len() {
                    return Err(at(format!(
                        "{} has {} columns and the row has {} values",
                        catalog.name,
                        catalog.columns.len(),
                        row.len()
                    )));
                }
                catalog.rows.push(row);
            }
            Statement::UniqueIndex { catalog, columns } => {
                catalogs
                    .iter_mut()
                    .find(|candidate| candidate.name == catalog)
                    .ok_or_else(|| at(format!("index on {catalog}, which no create declared")))?
                    .unique_indexes
                    .push(columns.into_iter().map(str::to_string).collect());
            }
            Statement::Other => {}
        }
    }
    Ok(catalogs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bootstrap_catalog_has_rows_without_an_open_line() {
        let bki =
            "create pg_type 1247 bootstrap rowtype_oid 71\n (\n oid = oid\n )\ninsert ( 16 )\n";
        let catalogs = parse(bki).unwrap();
        assert_eq!(catalogs[0].rows.len(), 1);
    }

    #[test]
    fn a_row_with_the_wrong_number_of_values_stops_the_parse() {
        let bki = "create pg_am 2601\n (\n oid = oid\n )\nopen pg_am\ninsert ( 2 3 )\n";
        let error = parse(bki).err().unwrap();
        assert_eq!(
            error,
            "line 6: pg_am has 1 columns and the row has 2 values"
        );
    }

    #[test]
    fn a_column_can_be_forced_to_be_null_or_not_null() {
        let bki = "create pg_proc 1255 bootstrap rowtype_oid 81\n (\n oid = oid ,\n \
                   proargtypes = oidvector FORCE NOT NULL ,\n probin = text FORCE NULL\n )\n";
        let catalogs = parse(bki).unwrap();
        let columns: Vec<(&str, &str, Nullness)> = catalogs[0]
            .columns
            .iter()
            .map(|column| {
                (
                    column.name.as_str(),
                    column.bki_type.as_str(),
                    column.nullness,
                )
            })
            .collect();
        assert_eq!(
            columns,
            [
                ("oid", "oid", Nullness::Auto),
                ("proargtypes", "oidvector", Nullness::ForceNotNull),
                ("probin", "text", Nullness::ForceNull)
            ]
        );
    }

    #[test]
    fn a_line_that_is_not_a_command_stops_the_parse() {
        let bki = "create pg_am 2601\n (\n oid = oid\n )\nopen pg_am\ninsert ( 2 )\ninsert 3\n";
        let error = parse(bki).err().unwrap();
        assert!(error.starts_with("line 7: "), "{error}");
    }

    #[test]
    fn a_quoted_value_that_does_not_end_stops_the_parse() {
        let bki = "create pg_am 2601\n (\n oid = oid\n )\nopen pg_am\ninsert ( 'heap )\n";
        let error = parse(bki).err().unwrap();
        assert!(error.starts_with("line 6: "), "{error}");
    }

    #[test]
    fn a_unique_index_is_kept_with_its_catalog_and_other_indexes_are_not() {
        let bki = "create pg_amop 2602\n (\n oid = oid\n )\nopen pg_amop\nclose pg_amop\n\
                   declare unique index pg_amop_fam_strat_index 2653 on pg_amop using btree(amopfamily oid_ops, amopstrategy int2_ops)\n\
                   declare unique index pg_amop_oid_index 2756 on pg_amop using btree(oid oid_ops)\n\
                   declare index pg_amop_other_index 2757 on pg_amop using btree(amopmethod oid_ops)\n\
                   declare toast 2836 2837 on pg_amop\n";
        let catalogs = parse(bki).unwrap();
        assert_eq!(
            catalogs[0].unique_indexes,
            [vec!["amopfamily", "amopstrategy"], vec!["oid"]]
        );
    }
}
