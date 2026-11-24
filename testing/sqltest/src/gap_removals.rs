//! The check that a change keeps every gap test.
//!
//! A gap test records a known difference from PostgreSQL. When a change
//! fixes the gap, the test stays and becomes a plain test: the change removes
//! only `@compat-gap` and the `expect @tursopg` block, or keeps that block when it
//! only changes the error message. The test may move to another file. A change
//! that deletes a gap test, renames it, or edits its SQL, setups or PostgreSQL
//! answer while it removes the gap loses the record that the gap was fixed.
//! This check compares the gap tests of a base tree with a head tree and
//! reports each one that the head tree does not keep.

use crate::parser::ast::{Backend, TestCase, TestFile};
use std::path::{Path, PathBuf};

/// The test files of one tree, with paths relative to the root of the tree.
pub type Tree<'a> = &'a [(PathBuf, TestFile)];

/// Returns one message per gap test of `base` that `head` does not keep.
/// A test is kept when some file of `head` has a test with the same name
/// that is still a gap test, or that is a plain test with the same SQL,
/// setups and default `expect` block as the gap test in `base`. The file may
/// differ, so a change can move a test to another file when it fixes the gap.
pub fn removed_gaps(base: Tree<'_>, head: Tree<'_>) -> Vec<String> {
    let mut errors = Vec::new();
    for (path, base_file) in base {
        for base_test in &base_file.tests {
            let Some(label) = &base_test.gap else {
                continue;
            };
            // The test in the same file comes first, so its problem is the
            // one reported when no test of `head` keeps the gap test.
            let mut candidates: Vec<(&Path, &TestFile, &TestCase)> = head
                .iter()
                .flat_map(|(head_path, head_file)| {
                    head_file
                        .tests
                        .iter()
                        .filter(|t| t.name == base_test.name)
                        .map(move |t| (head_path.as_path(), head_file, t))
                })
                .collect();
            candidates.sort_by_key(|(head_path, _, _)| *head_path != path.as_path());
            let results: Vec<_> = candidates
                .iter()
                .map(|(head_path, head_file, head_test)| {
                    conversion_problem(base_file, base_test, head_file, head_test).map_err(|p| {
                        if *head_path == path.as_path() {
                            p.to_string()
                        } else {
                            format!("moved to {}, but {p}", head_path.display())
                        }
                    })
                })
                .collect();
            if results.iter().any(Result::is_ok) {
                continue;
            }
            let problems: Vec<&String> = results.iter().filter_map(|r| r.as_ref().err()).collect();
            let mut message = format!(
                "{}: gap test {} ({label}) removed: if the gap is fixed, keep the test and \
                 remove only the @compat-gap and the expect @tursopg override",
                path.display(),
                base_test.name
            );
            if let Some(problem) = problems.first() {
                message.push_str(&format!(" ({problem})"));
            }
            errors.push(message);
        }
    }
    errors
}

/// Loads the test files under `root` for the check, with paths relative to
/// `root`. It fails when `root` is not a directory, when a file does not
/// parse (its gap tests would be missing from the check) and, for the base
/// tree, when `root` holds no test files: a wrong `--base` path would
/// otherwise pass the check.
///
/// The files are parsed without the gap lints: each tree passed them in its
/// own CI run.
pub async fn load_tree(root: &Path, is_base: bool) -> Result<Vec<(PathBuf, TestFile)>, String> {
    if !root.is_dir() {
        return Err(format!("{} is not a directory", root.display()));
    }
    let loaded = crate::load_test_files(std::slice::from_ref(&root.to_path_buf()), &|_, _| Ok(()))
        .await
        .map_err(|e| format!("cannot load the test files under {}: {e}", root.display()))?;
    if let Some(error) = loaded.errors.first() {
        return Err(format!(
            "{} does not parse; run `sqltest check` on it",
            error.file.display()
        ));
    }
    if is_base && loaded.files.is_empty() {
        return Err(format!(
            "the base tree {} has no .sqltest files",
            root.display()
        ));
    }
    Ok(loaded
        .files
        .into_iter()
        .map(|(path, file)| {
            let relative = path.strip_prefix(root).unwrap_or(&path).to_path_buf();
            (relative, file)
        })
        .collect())
}

/// Checks that `head_test` keeps the gap test `base_test`: it is still a gap
/// test, or it is the same test without `@compat-gap` and the `expect
/// @tursopg` block, or with an `expect @tursopg` block that
/// only changes the error message.
fn conversion_problem(
    base_file: &TestFile,
    base_test: &TestCase,
    head_file: &TestFile,
    head_test: &TestCase,
) -> Result<(), &'static str> {
    if head_test.gap.is_some() {
        return Ok(());
    }
    // When tursopg returns the PostgreSQL SQLSTATE with another message, the
    // converted test keeps an `expect @tursopg` block with that message.
    if head_test
        .expectations
        .overrides
        .contains_key(&Backend::Tursopg)
        && crate::gap::check_tursopg_override(head_test).is_err()
    {
        return Err(
            "the test keeps an expect @tursopg block that changes more than the error message",
        );
    }
    if head_test
        .expectations
        .overrides
        .keys()
        .any(|backend| *backend != Backend::Tursopg)
    {
        return Err("the test has an expect block for another backend");
    }
    if base_test.sql.trim() != head_test.sql.trim() {
        return Err("its SQL changed");
    }
    if setups(base_file, base_test) != setups(head_file, head_test) {
        return Err("its setups changed");
    }
    // The parser already removes the indentation of each row, except in
    // `expect raw` blocks, where whitespace is part of the answer. Any other
    // difference can change which output the block accepts.
    if base_test.expectations.default != head_test.expectations.default {
        return Err("its PostgreSQL answer changed");
    }
    // A skip, backend or requirement that the gap test did not have would
    // stop the converted test from running on a backend where it ran.
    let (base_mods, head_mods) = (&base_test.modifiers, &head_test.modifiers);
    if head_mods.skip != base_mods.skip
        || head_mods.backend != base_mods.backend
        || head_mods.requires != base_mods.requires
    {
        return Err("its skip, backend or requires modifiers changed");
    }
    if head_file.global_skip != base_file.global_skip
        || head_file.global_requires != base_file.global_requires
    {
        return Err("its file's file-level skip or requires directives changed");
    }
    Ok(())
}

/// The setups a test uses, in order, as (name, trimmed SQL, backend).
fn setups<'a>(file: &'a TestFile, test: &'a TestCase) -> Vec<(&'a str, Option<&'a str>, String)> {
    test.modifiers
        .setups
        .iter()
        .map(|setup_ref| {
            let setup = file.setups.get(&setup_ref.name);
            (
                setup_ref.name.as_str(),
                setup.map(|s| s.sql.trim()),
                format!("{:?}", setup.and_then(|s| s.backend)),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = r#"@database :memory:

setup numbers {
    CREATE TABLE t (x int);
}

@setup numbers
@compat-gap "type:bool"
test g {
    SELECT true;
}
expect {
    t
}
expect @tursopg {
    1
}
"#;

    /// Parses without the gap lints, as `load_tree` does.
    fn tree(text: &str) -> Vec<(PathBuf, TestFile)> {
        vec![(
            PathBuf::from("a.sqltest"),
            crate::parser::parse_with_lint(text, &|_, _| Ok(())).unwrap(),
        )]
    }

    fn check(head: &str) -> Vec<String> {
        removed_gaps(&tree(BASE), &tree(head))
    }

    /// The base test with `@compat-gap` and the `expect @tursopg` block removed.
    fn converted() -> String {
        BASE.replace("@compat-gap \"type:bool\"\n", "")
            .replace("expect @tursopg {\n    1\n}\n", "")
    }

    #[test]
    fn a_kept_gap_test_passes() {
        assert!(check(BASE).is_empty());
    }

    #[test]
    fn a_converted_gap_test_passes() {
        assert!(check(&converted()).is_empty());
    }

    #[test]
    fn a_converted_gap_test_may_change_whitespace() {
        let head = converted().replace("    SELECT true;", "  SELECT true;  ");
        assert!(check(&head).is_empty());
    }

    #[test]
    fn a_deleted_gap_test_fails() {
        let head = "@database :memory:\n";
        let errors = check(head);
        assert_eq!(
            errors,
            vec![
                "a.sqltest: gap test g (type:bool) removed: if the gap is fixed, keep the test \
                 and remove only the @compat-gap and the expect @tursopg override"
            ]
        );
    }

    #[test]
    fn a_renamed_gap_test_fails() {
        let head = converted().replace("test g {", "test h {");
        assert_eq!(check(&head).len(), 1);
    }

    #[test]
    fn a_gap_test_moved_to_another_file_passes() {
        // a.sqltest has no test g; b.sqltest has it without the gap.
        let head = vec![
            (
                PathBuf::from("a.sqltest"),
                tree("@database :memory:\n").remove(0).1,
            ),
            (PathBuf::from("b.sqltest"), tree(&converted()).remove(0).1),
        ];
        assert!(removed_gaps(&tree(BASE), &head).is_empty());
        // b.sqltest has test g with other SQL.
        let head = vec![(
            PathBuf::from("b.sqltest"),
            tree(&converted().replace("SELECT true;", "SELECT 't'::bool;"))
                .remove(0)
                .1,
        )];
        let errors = removed_gaps(&tree(BASE), &head);
        assert!(
            errors[0].ends_with("(moved to b.sqltest, but its SQL changed)"),
            "{errors:?}"
        );
    }

    #[test]
    fn a_converted_test_with_other_sql_fails() {
        let head = converted().replace("SELECT true;", "SELECT 't'::bool;");
        let errors = check(&head);
        assert!(errors[0].ends_with("(its SQL changed)"), "{errors:?}");
    }

    #[test]
    fn a_converted_test_with_another_setup_fails() {
        let head = converted().replace("CREATE TABLE t (x int);", "CREATE TABLE t (x text);");
        let errors = check(&head);
        assert!(errors[0].ends_with("(its setups changed)"), "{errors:?}");
    }

    #[test]
    fn a_converted_test_with_another_answer_fails() {
        let head = converted().replace("expect {\n    t\n}", "expect {\n    1\n}");
        let errors = check(&head);
        assert!(
            errors[0].ends_with("(its PostgreSQL answer changed)"),
            "{errors:?}"
        );
    }

    #[test]
    fn a_converted_test_with_whitespace_changed_in_a_raw_block_fails() {
        // exact::compare does not trim the rows of an `expect raw` block.
        let head = converted().replace("expect {\n    t\n}", "expect raw {\n    t \n}");
        let errors = check(&head);
        assert!(
            errors[0].ends_with("(its PostgreSQL answer changed)"),
            "{errors:?}"
        );
    }

    /// A gap test whose PostgreSQL answer is an error with a message.
    const ERROR_BASE: &str = r#"@database :memory:

@compat-gap "type:int4"
test e {
    SELECT 'x'::int4;
}
@sqlstate 22P02
expect error {
    invalid input syntax
}
@sqlstate 0A000
expect @tursopg error {
}
"#;

    #[test]
    fn a_converted_test_that_keeps_a_message_only_tursopg_block_passes() {
        // tursopg returns the PostgreSQL SQLSTATE with another message.
        let head = ERROR_BASE
            .replace("@compat-gap \"type:int4\"\n", "")
            .replace(
                "@sqlstate 0A000\nexpect @tursopg error {\n}",
                "@sqlstate 22P02\nexpect @tursopg error {\n    invalid value\n}",
            );
        assert!(removed_gaps(&tree(ERROR_BASE), &tree(&head)).is_empty());
        // Another SQLSTATE is more than a message change.
        let head = head.replace(
            "@sqlstate 22P02\nexpect @tursopg",
            "@sqlstate 22003\nexpect @tursopg",
        );
        assert_eq!(removed_gaps(&tree(ERROR_BASE), &tree(&head)).len(), 1);
    }

    #[test]
    fn a_test_that_keeps_the_tursopg_block_without_the_label_fails() {
        let head = BASE.replace("@compat-gap \"type:bool\"\n", "");
        let errors = removed_gaps(&tree(BASE), &tree(&head));
        assert!(
            errors[0].ends_with(
                "(the test keeps an expect @tursopg block that changes more than the error message)"
            ),
            "{errors:?}"
        );
    }

    #[test]
    fn a_converted_test_that_is_skipped_fails() {
        for modifier in [
            "@skip \"slow\"\n",
            "@skip-if mvcc \"slow\"\n",
            "@backend tursopg\n",
            "@requires trigger \"needs triggers\"\n",
        ] {
            let head = converted().replace("test g {", &format!("{modifier}test g {{"));
            let errors = check(&head);
            assert!(
                errors[0].ends_with("(its skip, backend or requires modifiers changed)"),
                "{modifier}: {errors:?}"
            );
        }
        let head = converted().replace(
            "@database :memory:\n",
            "@database :memory:\n@skip-file \"slow\"\n",
        );
        let errors = check(&head);
        assert!(
            errors[0].ends_with("(its file's file-level skip or requires directives changed)"),
            "{errors:?}"
        );
        // A modifier that the gap test had may stay.
        let base = BASE.replace(
            "@setup numbers\n",
            "@setup numbers\n@skip-if mvcc \"slow\"\n",
        );
        let head = converted().replace(
            "@setup numbers\n",
            "@setup numbers\n@skip-if mvcc \"slow\"\n",
        );
        assert!(removed_gaps(&tree(&base), &tree(&head)).is_empty());
    }

    #[tokio::test]
    async fn load_tree_fails_on_a_missing_root_or_an_empty_base() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing");
        let err = load_tree(&missing, true).await.unwrap_err();
        assert!(err.contains("is not a directory"), "{err}");
        let err = load_tree(&missing, false).await.unwrap_err();
        assert!(err.contains("is not a directory"), "{err}");

        let err = load_tree(dir.path(), true).await.unwrap_err();
        assert!(err.contains("has no .sqltest files"), "{err}");
        // An empty head tree loads; the check then reports each removed gap.
        assert!(load_tree(dir.path(), false).await.unwrap().is_empty());

        std::fs::create_dir(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("sub/a.sqltest"), BASE).unwrap();
        let tree = load_tree(dir.path(), true).await.unwrap();
        assert_eq!(tree.len(), 1);
        assert_eq!(tree[0].0, Path::new("sub/a.sqltest"));
        assert!(tree[0].1.tests[0].gap.is_some());
    }
}
