//! Ways for a DML statement to name its table that differ from how CREATE TABLE
//! wrote the name. SQLite resolves all of them to the same table.

use proptest::prelude::*;

use crate::schema::TableRef;

/// Which other spellings INSERT, UPDATE and DELETE use for their table.
#[derive(Debug, Clone, Default)]
pub struct TableSpellingProfile {
    /// Write the table name in quotes: `"msg"`, `[msg]` or `` `msg` ``.
    pub quoted: bool,
}

/// The name of `table` as a DML statement writes it.
pub fn table_name_spelling(
    table: &TableRef,
    profile: &TableSpellingProfile,
) -> BoxedStrategy<String> {
    let spellings = table_name_spellings(table.unqualified_name(), profile);
    if spellings.len() == 1 {
        return Just(table.qualified_name()).boxed();
    }
    let prefix = match &table.database {
        Some(db) => format!("{db}."),
        None => String::new(),
    };
    proptest::sample::select(spellings)
        .prop_map(move |name| format!("{prefix}{name}"))
        .boxed()
}

fn table_name_spellings(name: &str, profile: &TableSpellingProfile) -> Vec<String> {
    let mut spellings = vec![name.to_string()];
    if profile.quoted {
        spellings.extend(quoted_forms(name));
    }
    spellings
}

fn quoted_forms(name: &str) -> Vec<String> {
    assert!(
        !name.contains(['"', '[', ']', '`']),
        "table name {name} contains a quote character"
    );
    vec![
        format!("\"{name}\""),
        format!("[{name}]"),
        format!("`{name}`"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnDef, DataType, Table};
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;
    use std::collections::BTreeSet;

    fn drawn_spellings(table: Table, profile: &TableSpellingProfile) -> BTreeSet<String> {
        let table: TableRef = table.into();
        let strategy = table_name_spelling(&table, profile);
        let mut runner = TestRunner::deterministic();
        (0..100)
            .map(|_| strategy.new_tree(&mut runner).unwrap().current())
            .collect()
    }

    fn msg() -> Table {
        Table::new("msg", vec![ColumnDef::new("id", DataType::Integer)])
    }

    #[test]
    fn without_a_spelling_option_the_table_keeps_its_created_name() {
        assert_eq!(
            drawn_spellings(msg(), &TableSpellingProfile::default()),
            BTreeSet::from(["msg".to_string()])
        );
    }

    #[test]
    fn quoted_writes_the_name_in_all_three_quote_styles() {
        let profile = TableSpellingProfile { quoted: true };
        assert_eq!(
            drawn_spellings(msg(), &profile),
            BTreeSet::from([
                "\"msg\"".to_string(),
                "[msg]".to_string(),
                "`msg`".to_string(),
                "msg".to_string()
            ])
        );
    }

    #[test]
    fn quoted_keeps_the_database_prefix_as_it_is() {
        let profile = TableSpellingProfile { quoted: true };
        assert!(
            drawn_spellings(msg().in_database("aux"), &profile)
                .iter()
                .all(|spelling| spelling.starts_with("aux."))
        );
    }
}
