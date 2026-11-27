//! Gap tests.
//!
//! A gap test records a known difference between tursopg and PostgreSQL 18.
//! It is a test with a `@compat-gap "<kind>:<object>"` decorator, a default `expect`
//! block that holds the PostgreSQL answer and an `expect @tursopg` block that
//! holds the current tursopg answer. The postgres backend checks the default
//! block, which proves the recorded PostgreSQL answer. The tursopg backend
//! passes when tursopg still gives the recorded answer and fails when tursopg
//! gives the PostgreSQL answer, so that a fixed gap cannot stay recorded.
//!
//! This module validates gap tests, derives their class from the two
//! expectations and counts them per label.

use crate::backends::QueryResult;
use crate::parser::ast::{
    Backend, Expectation, Requirement, Skip, SkipCondition, TestCase, TestFile,
};
use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

/// What a gap label names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum GapKind {
    Function,
    Type,
    Operator,
    Cast,
    Syntax,
}

impl GapKind {
    pub const ALL: &'static [GapKind] = &[
        GapKind::Function,
        GapKind::Type,
        GapKind::Operator,
        GapKind::Cast,
        GapKind::Syntax,
    ];

    pub fn name(self) -> &'static str {
        match self {
            GapKind::Function => "function",
            GapKind::Type => "type",
            GapKind::Operator => "operator",
            GapKind::Cast => "cast",
            GapKind::Syntax => "syntax",
        }
    }
}

impl fmt::Display for GapKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// The longest answer that messages and the summary print before they cut it.
const MAX_ANSWER_LEN: usize = 80;

/// One line that describes the answer an expectation records, such as
/// `2`, `1|t; 2|f`, `no rows` or `error 22003`.
pub fn describe_expectation(expectation: &Expectation) -> String {
    let text = match expectation {
        Expectation::Exact(rows) => describe_rows(trimmed(rows)),
        Expectation::Unordered(rows) => format!("{} (any order)", describe_rows(trimmed(rows))),
        Expectation::Pattern(pattern) => format!("output matching /{pattern}/"),
        Expectation::Error { sqlstate, .. } => match sqlstate {
            Some(sqlstate) => format!("error {sqlstate}"),
            None => "an error".to_string(),
        },
    };
    shorten(text)
}

/// One line that describes what a backend returned, in the form of
/// [`describe_expectation`].
pub fn describe_result(result: &QueryResult) -> String {
    let text = if result.is_error() {
        match &result.sqlstate {
            Some(sqlstate) => format!("error {sqlstate}"),
            None => format!(
                "error \"{}\"",
                result
                    .error
                    .as_deref()
                    .unwrap_or("")
                    .lines()
                    .next()
                    .unwrap_or("")
            ),
        }
    } else {
        let rows: Vec<String> = result.rows.iter().map(|row| row.join("|")).collect();
        describe_rows(rows.iter().map(String::as_str).collect())
    };
    shorten(text)
}

fn describe_rows(rows: Vec<&str>) -> String {
    if rows.is_empty() {
        "no rows".to_string()
    } else {
        rows.iter()
            .map(|row| row.trim())
            .collect::<Vec<_>>()
            .join("; ")
    }
}

fn shorten(text: String) -> String {
    match text.char_indices().nth(MAX_ANSWER_LEN) {
        Some((end, _)) => format!("{}...", &text[..end]),
        None => text,
    }
}

/// Checks the syntax of a gap label and returns its kind. The label is
/// `<kind>:<object>`. This does not check that the object exists in
/// PostgreSQL.
fn label_kind(label: &str) -> Result<GapKind, String> {
    let Some((kind, object)) = label.split_once(':') else {
        return Err(format!(
            "gap label '{label}' has no kind: write it as <kind>:<object>, for example function:round"
        ));
    };
    let kind = GapKind::ALL
        .iter()
        .copied()
        .find(|k| k.name() == kind)
        .ok_or_else(|| {
            format!(
                "gap label '{label}' has the unknown kind '{kind}'; the kinds are {}",
                GapKind::ALL
                    .iter()
                    .map(|k| k.name())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })?;
    if object.is_empty() || object.trim() != object {
        return Err(format!(
            "gap label '{label}' needs an object after '{kind}:' with no leading or trailing spaces"
        ));
    }
    if kind == GapKind::Cast && !object.contains(" -> ") {
        return Err(format!(
            "gap label '{label}' names a cast as '<source type> -> <target type>', for example cast:text -> timestamptz"
        ));
    }
    Ok(kind)
}

/// How tursopg differs from PostgreSQL in a gap test. The runner derives it
/// from the two expectations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum GapClass {
    /// tursopg fails with any SQLSTATE and PostgreSQL returns rows. The user
    /// sees an error, not a wrong value.
    Refused,
    /// Both fail, with different SQLSTATEs.
    ErrorMismatch,
    /// PostgreSQL fails and tursopg returns rows.
    AcceptsInvalid,
    /// Any other difference, for example different rows.
    Wrong,
}

impl GapClass {
    pub const ALL: &'static [GapClass] = &[
        GapClass::Refused,
        GapClass::ErrorMismatch,
        GapClass::AcceptsInvalid,
        GapClass::Wrong,
    ];

    pub fn of(postgres: &Expectation, tursopg: &Expectation) -> GapClass {
        match (postgres, tursopg) {
            (Expectation::Error { .. }, Expectation::Error { .. }) => GapClass::ErrorMismatch,
            (Expectation::Error { .. }, _) => GapClass::AcceptsInvalid,
            (_, Expectation::Error { .. }) => GapClass::Refused,
            _ => GapClass::Wrong,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            GapClass::Refused => "refused",
            GapClass::ErrorMismatch => "error-mismatch",
            GapClass::AcceptsInvalid => "accepts-invalid",
            GapClass::Wrong => "wrong",
        }
    }
}

impl fmt::Display for GapClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Checks the rules for a test with a `@compat-gap` decorator. `global_skip` are the
/// `@skip-file` and `@skip-file-if` directives of its file, and `global_requires` are
/// its `@requires-file` directives.
fn check_gap_test(
    test: &TestCase,
    label: &str,
    global_skip: &[Skip],
    global_requires: &[Requirement],
) -> Result<(), String> {
    label_kind(label)?;
    let Some(tursopg) = test.expectations.overrides.get(&Backend::Tursopg) else {
        return Err(
            "a @compat-gap test needs an `expect @tursopg` block with the current tursopg answer"
                .to_string(),
        );
    };
    if let Some(backend) = test
        .expectations
        .overrides
        .keys()
        .find(|b| **b != Backend::Tursopg)
    {
        return Err(format!(
            "a @compat-gap test takes no `expect @{backend}` block: the default block is the PostgreSQL answer"
        ));
    }
    if let Some(backend) = test.modifiers.backend {
        return Err(format!(
            "a @compat-gap test cannot have @backend {backend}: it must run on both the tursopg and the postgres backend"
        ));
    }
    for skip in test.modifiers.skip.iter().chain(global_skip) {
        match skip.condition {
            None => {
                return Err(
                    "a @compat-gap test cannot be skipped: it must run on the postgres backend"
                        .to_string(),
                );
            }
            Some(SkipCondition::Postgres) => {
                return Err(
                    "a @compat-gap test cannot have @skip-if postgres: the postgres backend proves its PostgreSQL answer"
                        .to_string(),
                );
            }
            Some(
                SkipCondition::Mvcc
                | SkipCondition::Sqlite
                | SkipCondition::Debug
                | SkipCondition::Model,
            ) => {}
        }
    }
    // The postgres and tursopg backends have no capabilities, so a requirement
    // skips the test on both, and `sqltest gaps` would count a gap that no run
    // checks.
    if let Some(requirement) = test.modifiers.requires.iter().chain(global_requires).next() {
        return Err(format!(
            "a @compat-gap test cannot have @requires or @requires-file ({}): the postgres and tursopg backends skip it, so no run checks the gap",
            requirement.reason
        ));
    }
    let postgres = &test.expectations.default;
    for (side, expectation) in [("default", postgres), ("@tursopg", tursopg)] {
        match expectation {
            Expectation::Pattern(_) => {
                return Err(format!(
                    "the {side} block of a @compat-gap test cannot be `expect pattern`: a pattern can match both answers"
                ));
            }
            Expectation::Error { sqlstate, .. } => {
                if sqlstate.is_none() {
                    return Err(format!(
                        "the {side} error block of a @compat-gap test needs @sqlstate: error gaps compare the SQLSTATE"
                    ));
                }
            }
            Expectation::Exact(_) | Expectation::Unordered(_) => {}
        }
    }
    if crate::comparison::may_match_same_output(postgres, tursopg) {
        return Err(
            "the default block and the `expect @tursopg` block of this @compat-gap test accept the same answer"
                .to_string(),
        );
    }
    Ok(())
}

/// The rows of an exact expectation without leading and trailing empty
/// lines, for display.
fn trimmed(rows: &[String]) -> Vec<&str> {
    let mut rows: Vec<&str> = rows.iter().map(String::as_str).collect();
    while rows.first().is_some_and(|r| r.is_empty()) {
        rows.remove(0);
    }
    while rows.last().is_some_and(|r| r.is_empty()) {
        rows.pop();
    }
    rows
}

/// Checks a test that has an `expect @tursopg` block and no `@compat-gap`
/// decorator. Such a block may only record a tursopg error message that
/// differs from the PostgreSQL message for the same SQLSTATE. A different
/// answer is a gap and needs `@compat-gap`.
pub(crate) fn check_tursopg_override(test: &TestCase) -> Result<(), String> {
    let Some(tursopg) = test.expectations.overrides.get(&Backend::Tursopg) else {
        return Ok(());
    };
    let postgres = test.expectations.for_backend(Backend::Postgres);
    match (postgres.sqlstate(), tursopg.sqlstate()) {
        (Some(pg), Some(turso)) if pg == turso => Ok(()),
        _ => Err(
            "an `expect @tursopg` block without @compat-gap may only change the error message for the \
             same @sqlstate; tursopg gives a different answer here, so record it as a gap with \
             @compat-gap \"<kind>:<object>\""
                .to_string(),
        ),
    }
}

/// One gap test in a report.
#[derive(Debug, Clone)]
pub struct GapEntry {
    pub file: PathBuf,
    pub test: String,
    pub label: String,
    pub kind: GapKind,
    pub class: GapClass,
    /// The answer PostgreSQL gives, from the default `expect` block.
    pub postgres: String,
    /// The answer tursopg gives, from the `expect @tursopg` block.
    pub tursopg: String,
}

/// The gap tests of a set of test files. Each test counts
/// once, however many databases its file runs it on.
#[derive(Debug, Default)]
pub struct GapReport {
    pub gaps: Vec<GapEntry>,
}

impl GapReport {
    /// The report of files that the gap lints accepted.
    pub fn from_files<'a>(files: impl IntoIterator<Item = (&'a Path, &'a TestFile)>) -> Self {
        let mut report = GapReport::default();
        for (path, file) in files {
            for test in &file.tests {
                if let Some(label) = &test.gap {
                    let tursopg = test.expectations.overrides.get(&Backend::Tursopg).expect(
                        "the gap lints reject a @compat-gap test without an `expect @tursopg` block",
                    );
                    report.gaps.push(GapEntry {
                        file: path.to_path_buf(),
                        test: test.name.clone(),
                        label: label.clone(),
                        kind: label_kind(label).expect("the gap lints check gap labels"),
                        class: GapClass::of(&test.expectations.default, tursopg),
                        postgres: describe_expectation(&test.expectations.default),
                        tursopg: describe_expectation(tursopg),
                    });
                }
            }
        }
        report
    }

    pub fn is_empty(&self) -> bool {
        self.gaps.is_empty()
    }

    /// The counts as Markdown tables: by kind, by class, by label (with the
    /// count of each class).
    pub fn to_markdown(&self) -> String {
        let mut out = String::new();
        let total = self.gaps.len();
        out.push_str(&format!(
            "### Compatibility gaps: {total} known differences from PostgreSQL\n\n"
        ));

        let mut by_kind: BTreeMap<GapKind, usize> = BTreeMap::new();
        let mut by_class: BTreeMap<GapClass, usize> = BTreeMap::new();
        let mut by_label: BTreeMap<&str, BTreeMap<GapClass, usize>> = BTreeMap::new();
        for gap in &self.gaps {
            *by_kind.entry(gap.kind).or_default() += 1;
            *by_class.entry(gap.class).or_default() += 1;
            *by_label
                .entry(gap.label.as_str())
                .or_default()
                .entry(gap.class)
                .or_default() += 1;
        }

        out.push_str("| kind | gaps |\n|---|---:|\n");
        for (kind, n) in &by_kind {
            out.push_str(&format!("| {kind} | {n} |\n"));
        }
        out.push_str("\n| class | gaps |\n|---|---:|\n");
        for (class, n) in &by_class {
            out.push_str(&format!("| {class} | {n} |\n"));
        }
        out.push_str("\n| label | gaps |");
        for class in GapClass::ALL {
            out.push_str(&format!(" {class} |"));
        }
        out.push_str("\n|---|---:|");
        for _ in GapClass::ALL {
            out.push_str("---:|");
        }
        out.push('\n');
        for (label, classes) in &by_label {
            let n: usize = classes.values().sum();
            out.push_str(&format!("| `{label}` | {n} |"));
            for class in GapClass::ALL {
                out.push_str(&format!(" {} |", classes.get(class).copied().unwrap_or(0)));
            }
            out.push('\n');
        }

        out.push_str(&format!(
            "\n<details><summary>All {total} gaps with both answers</summary>\n\n\
             | label | test | tursopg returns | PostgreSQL returns |\n|---|---|---|---|\n"
        ));
        let mut gaps: Vec<&GapEntry> = self.gaps.iter().collect();
        gaps.sort_by(|a, b| (&a.label, &a.file, &a.test).cmp(&(&b.label, &b.file, &b.test)));
        for gap in gaps {
            out.push_str(&format!(
                "| `{}` | {} `{}` | {} | {} |\n",
                gap.label,
                gap.file.display(),
                gap.test,
                markdown_cell(&gap.tursopg),
                markdown_cell(&gap.postgres),
            ));
        }
        out.push_str("\n</details>\n");

        out
    }
}

/// Escapes text for one cell of a markdown table.
fn markdown_cell(text: &str) -> String {
    text.replace('|', "\\|")
}

/// The gap lints for one test of a file: the rules of a `@compat-gap` test
/// and of an `expect @tursopg` block without `@compat-gap`.
pub fn lint_test(file: &TestFile, test: &TestCase) -> Result<(), String> {
    match &test.gap {
        Some(label) => check_gap_test(test, label, &file.global_skip, &file.global_requires),
        None => check_tursopg_override(test),
    }
}

/// The lint that the sqltest commands run on each test: the gap lints when
/// `compat_gaps` is set (`--compat-gaps`), or else the parser's rejection of
/// `@compat-gap`.
pub fn lint(compat_gaps: bool) -> impl Fn(&TestFile, &TestCase) -> Result<(), String> + Sync {
    move |file, test| {
        if compat_gaps {
            lint_test(file, test)
        } else {
            crate::parser::reject_compat_gap(file, test)
        }
    }
}

/// Parses a test file with the gap lints, for unit tests.
#[cfg(test)]
pub(crate) fn parse_with_gap_lints(input: &str) -> Result<TestFile, crate::ParseError> {
    crate::parser::parse_with_lint(input, &lint_test)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parses a file with the given tests after a `@database :memory:` line,
    /// with the gap lints on.
    fn parse_tests(tests: &str) -> Result<TestFile, String> {
        parse_with_gap_lints(&format!("@database :memory:\n\n{tests}"))
            .map_err(|e| format!("{e:?}"))
    }

    fn assert_rejected(tests: &str, message: &str) {
        let err = parse_tests(tests).expect_err("the file must not parse");
        assert!(err.contains(message), "expected `{message}` in: {err}");
    }

    const WRONG: &str = r#"
@compat-gap "type:bool"
test wrong {
    SELECT true;
}
expect {
    t
}
expect @tursopg {
    1
}
"#;

    #[test]
    fn compat_gap_without_the_flag_is_an_error() {
        let err = crate::parse(&format!("@database :memory:\n\n{WRONG}"))
            .expect_err("a gap test without --compat-gaps must not parse");
        assert!(format!("{err:?}").contains("--compat-gaps"), "{err:?}");
        // Without --compat-gaps, the lints for `expect @tursopg` blocks do not run.
        let plain = WRONG.replace("@compat-gap \"type:bool\"\n", "");
        crate::parse(&format!("@database :memory:\n\n{plain}")).unwrap();
    }

    #[test]
    fn label_kinds() {
        assert_eq!(label_kind("function:age"), Ok(GapKind::Function));
        assert_eq!(label_kind("type:_int4"), Ok(GapKind::Type));
        assert_eq!(label_kind("operator:jsonb @> jsonb"), Ok(GapKind::Operator));
        assert_eq!(label_kind("cast:timestamptz -> date"), Ok(GapKind::Cast));
        assert_eq!(label_kind("syntax:on-conflict"), Ok(GapKind::Syntax));
        assert!(label_kind("relation:pg_class").is_err());
        assert!(label_kind("age").is_err());
        assert!(label_kind("function:").is_err());
        assert!(label_kind("cast:timestamptz").is_err());
        assert_eq!(label_kind("syntax:any-form"), Ok(GapKind::Syntax));
    }

    #[test]
    fn valid_gap_test_parses() {
        let file = parse_tests(WRONG).unwrap();
        assert_eq!(file.tests[0].gap.as_deref(), Some("type:bool"));
    }

    #[test]
    fn lint_unknown_label_kind() {
        assert_rejected(&WRONG.replace("type:bool", "relation:pg_class"), "relation");
    }

    #[test]
    fn lint_gap_without_tursopg_override() {
        assert_rejected(
            "@compat-gap \"type:bool\"\ntest t {\n    SELECT true;\n}\nexpect {\n    t\n}\n",
            "expect @tursopg",
        );
    }

    #[test]
    fn lint_gap_that_does_not_run_on_postgres() {
        assert_rejected(
            &WRONG.replace("@compat-gap", "@skip-if postgres \"x\"\n@compat-gap"),
            "postgres backend",
        );
        assert_rejected(
            &WRONG.replace("@compat-gap", "@skip \"x\"\n@compat-gap"),
            "postgres backend",
        );
        assert_rejected(
            &WRONG.replace("@compat-gap", "@backend tursopg\n@compat-gap"),
            "@backend",
        );
        let err = parse_with_gap_lints(&format!(
            "@database :memory:\n@skip-file-if postgres \"x\"\n{WRONG}"
        ))
        .expect_err("the file must not parse");
        assert!(format!("{err:?}").contains("postgres"), "{err:?}");
    }

    #[test]
    fn lint_gap_that_requires_a_capability() {
        assert_rejected(
            &WRONG.replace("@compat-gap", "@requires strict \"x\"\n@compat-gap"),
            "@requires",
        );
        let err = parse_with_gap_lints(&format!(
            "@database :memory:\n@requires-file strict \"x\"\n{WRONG}"
        ))
        .expect_err("the file must not parse");
        assert!(format!("{err:?}").contains("@requires-file"), "{err:?}");
    }

    #[test]
    fn lint_pattern_in_gap_test() {
        assert_rejected(
            &WRONG.replace("expect @tursopg {", "expect @tursopg pattern {"),
            "pattern",
        );
    }

    #[test]
    fn lint_error_block_with_message_or_without_sqlstate() {
        let no_sqlstate = "@compat-gap \"type:int4\"\ntest t {\n    SELECT 1;\n}\nexpect error {\n}\nexpect @tursopg {\n    1\n}\n";
        assert_rejected(no_sqlstate, "@sqlstate");
        // The default block holds the PostgreSQL message, which the postgres
        // backend checks.
        let postgres_message = "@compat-gap \"type:int4\"\ntest t {\n    SELECT 1;\n}\n@sqlstate 22003\nexpect error {\n    out of range\n}\nexpect @tursopg {\n    1\n}\n";
        parse_tests(postgres_message).unwrap();
        // The @tursopg block may give the tursopg message, which the tursopg
        // run compares.
        parse_tests("@compat-gap \"type:int4\"\ntest t {\n    SELECT 1;\n}\nexpect {\n    1\n}\n@sqlstate 0A000\nexpect @tursopg error {\n    not supported\n}\n").unwrap();
        // Messages do not make two errors with the same SQLSTATE different.
        let same_sqlstate = "@compat-gap \"type:int4\"\ntest t {\n    SELECT 1;\n}\n@sqlstate 22003\nexpect error {\n    out of range\n}\n@sqlstate 22003\nexpect @tursopg error {\n}\n";
        assert_rejected(same_sqlstate, "same answer");
    }

    #[test]
    fn lint_same_answer_in_both_blocks() {
        assert_rejected(&WRONG.replace("    1\n}", "    t\n}"), "same");
        // Unordered blocks compare row sets, so repeated rows count once.
        assert_rejected(
            "@compat-gap \"type:int4\"\ntest t {\n    SELECT 1;\n}\nexpect unordered {\n    1\n    1\n}\nexpect @tursopg unordered {\n    1\n}\n",
            "same",
        );
        // An empty line in an exact block is a NULL row.
        parse_tests(
            "@compat-gap \"type:int4\"\ntest t {\n    SELECT 1;\n}\nexpect {\n    1\n\n    2\n}\nexpect @tursopg {\n    1\n    2\n}\n",
        )
        .unwrap();
    }

    #[test]
    fn lint_two_gap_labels() {
        assert_rejected(
            &WRONG.replace("@compat-gap", "@compat-gap \"type:int4\"\n@compat-gap"),
            "one @compat-gap label",
        );
    }

    #[test]
    fn override_without_gap() {
        // Only the message differs: allowed.
        parse_tests(
            "test t {\n    SELECT x;\n}\n@sqlstate 42703\nexpect error {\n    column \"x\" does not exist\n}\n@sqlstate 42703\nexpect @tursopg error {\n    no such column: x\n}\n",
        )
        .unwrap();
        // A different answer needs @compat-gap.
        assert_rejected(
            &WRONG.replace("@compat-gap \"type:bool\"\n", ""),
            "@compat-gap",
        );
    }

    fn rows(lines: &[&str]) -> Expectation {
        Expectation::Exact(lines.iter().map(|l| l.to_string()).collect())
    }

    fn error(sqlstate: &str) -> Expectation {
        Expectation::Error {
            sqlstate: Some(sqlstate.to_string()),
            pattern: None,
        }
    }

    #[test]
    fn classes() {
        assert_eq!(
            GapClass::of(&rows(&["1"]), &error("0A000")),
            GapClass::Refused
        );
        assert_eq!(
            GapClass::of(&error("22003"), &error("0A000")),
            GapClass::ErrorMismatch
        );
        assert_eq!(
            GapClass::of(&error("22003"), &error("22P02")),
            GapClass::ErrorMismatch
        );
        assert_eq!(
            GapClass::of(&error("22003"), &rows(&["1"])),
            GapClass::AcceptsInvalid
        );
        assert_eq!(GapClass::of(&rows(&["t"]), &rows(&["1"])), GapClass::Wrong);
        assert_eq!(
            GapClass::of(&rows(&["1"]), &error("42883")),
            GapClass::Refused
        );
    }

    const REPORT_FILE: &str = r#"
@compat-gap "type:bool"
test wrong {
    SELECT true;
}
expect {
    t
}
expect @tursopg {
    1
}

@compat-gap "type:int4"
test accepts-invalid {
    SELECT '2147483648'::int4;
}
@sqlstate 22003
expect error {
}
expect @tursopg {
    2147483648
}

@compat-gap "syntax:unsupported-statement"
test refused {
    LISTEN c;
}
expect {
}
@sqlstate 0A000
expect @tursopg error {
}

"#;

    fn report() -> GapReport {
        // Two databases: each test still counts once.
        let file = parse_with_gap_lints(&format!(
            "@database :memory:\n@database :temp:\n{REPORT_FILE}"
        ))
        .unwrap();
        GapReport::from_files([(Path::new("f.sqltest"), &file)])
    }

    #[test]
    fn report_counts_each_test_once() {
        let report = report();
        let classes: Vec<_> = report.gaps.iter().map(|g| g.class).collect();
        assert_eq!(
            classes,
            [GapClass::Wrong, GapClass::AcceptsInvalid, GapClass::Refused]
        );
        let md = report.to_markdown();
        assert!(
            md.contains("### Compatibility gaps: 3 known differences"),
            "{md}"
        );
        assert!(md.contains("| type | 2 |"), "{md}");
        assert!(md.contains("| `type:int4` | 1 | 0 | 0 | 1 | 0 |"), "{md}");
    }
}
