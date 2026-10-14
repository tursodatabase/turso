//! Ways for a DML statement to name its table that differ from how CREATE TABLE
//! wrote the name. SQLite resolves all of them to the same table.

use proptest::prelude::*;

use crate::schema::TableRef;

/// Which other spellings INSERT, UPDATE and DELETE use for their table.
#[derive(Debug, Clone, Default)]
pub struct TableSpellingProfile {
    /// Write the table name in other letter cases, for example `MSG` for `msg`.
    pub other_case: bool,
    /// Give the table of UPDATE and DELETE an alias: `UPDATE msg AS tgt ...`.
    pub target_alias: bool,
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

/// The alias of the table of an UPDATE or DELETE, if it has one.
pub fn target_alias(profile: &TableSpellingProfile) -> BoxedStrategy<Option<String>> {
    if !profile.target_alias {
        return Just(None).boxed();
    }
    prop_oneof![Just(None), Just(Some(TARGET_ALIAS.to_string()))].boxed()
}

const TARGET_ALIAS: &str = "tgt";

fn table_name_spellings(name: &str, profile: &TableSpellingProfile) -> Vec<String> {
    let mut spellings = vec![name.to_string()];
    if profile.other_case {
        spellings.extend(other_letter_cases(name));
    }
    spellings
}

fn other_letter_cases(name: &str) -> Vec<String> {
    let upper = name.to_ascii_uppercase();
    let mut capitalized = name.to_string();
    capitalized[..1].make_ascii_uppercase();
    let mut cases = vec![upper];
    if !cases.contains(&capitalized) {
        cases.push(capitalized);
    }
    cases.retain(|case| case != name);
    cases
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
    fn other_case_writes_the_name_in_upper_and_capitalized_case() {
        let profile = TableSpellingProfile {
            other_case: true,
            ..Default::default()
        };
        assert_eq!(
            drawn_spellings(msg(), &profile),
            BTreeSet::from(["MSG".to_string(), "Msg".to_string(), "msg".to_string()])
        );
    }

    #[test]
    fn other_case_keeps_the_database_prefix_as_it_is() {
        let profile = TableSpellingProfile {
            other_case: true,
            ..Default::default()
        };
        assert_eq!(
            drawn_spellings(msg().in_database("aux"), &profile),
            BTreeSet::from([
                "aux.MSG".to_string(),
                "aux.Msg".to_string(),
                "aux.msg".to_string()
            ])
        );
    }

    #[test]
    fn a_name_without_letters_has_no_other_case() {
        assert!(other_letter_cases("_1").is_empty());
    }
}
