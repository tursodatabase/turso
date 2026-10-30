use hegel::{generators as gs, TestCase};
use turso_pg_dump_fuzzer::dump_ast;

#[hegel::test(test_cases = 2000)]
fn formatting_does_not_change_ast(tc: TestCase) {
    let n = tc.draw(gs::integers::<i32>());
    assert_eq!(
        dump_ast(&format!("SELECT {n};")).unwrap(),
        dump_ast(&format!("-- header\r\n select /* comment */ {n} ;\r\n")).unwrap()
    );
}

#[hegel::test(test_cases = 2000)]
fn row_order_does_not_change_ast(tc: TestCase) {
    let values = tc.draw(gs::vecs(gs::integers::<i32>()).max_size(30));
    let rows: Vec<_> = values
        .iter()
        .map(|n| format!("INSERT INTO t VALUES ({n});\n"))
        .collect();
    assert_eq!(
        dump_ast(&rows.concat()).unwrap(),
        dump_ast(&rows.into_iter().rev().collect::<String>()).unwrap()
    );
}

#[hegel::test(test_cases = 2000)]
fn missing_duplicate_row_changes_ast(tc: TestCase) {
    let n = tc.draw(gs::integers::<i32>());
    let row = format!("INSERT INTO t VALUES ({n});");
    assert_ne!(dump_ast(&row).unwrap(), dump_ast(&row.repeat(2)).unwrap());
}

#[hegel::test(test_cases = 2000)]
fn changed_value_changes_ast(tc: TestCase) {
    let n = tc.draw(gs::integers::<i64>());
    assert_ne!(
        dump_ast(&format!("INSERT INTO t VALUES ({n});")).unwrap(),
        dump_ast(&format!("INSERT INTO t VALUES ({});", n.wrapping_add(1))).unwrap()
    );
}

#[hegel::test(test_cases = 2000)]
fn changed_schema_changes_ast(tc: TestCase) {
    let n = tc.draw(gs::integers::<u16>());
    assert_ne!(
        dump_ast(&format!("CREATE TABLE t{n} (x INTEGER);")).unwrap(),
        dump_ast(&format!("CREATE TABLE t{n} (x BIGINT);")).unwrap()
    );
}

#[hegel::test(test_cases = 2000)]
fn invalid_sql_is_rejected(tc: TestCase) {
    let n = tc.draw(gs::integers::<u32>());
    assert!(dump_ast(&format!("CREATE TABLE t{n} (;")).is_err());
}

#[hegel::test(test_cases = 2000)]
fn restriction_wrapper_preserves_sql(tc: TestCase) {
    let n = tc.draw(gs::integers::<i32>());
    let sql = format!("SELECT {n};\n");
    let wrapped = format!("-- PostgreSQL dump\n\\restrict TursoPgdumpConformance\n{sql}\\unrestrict TursoPgdumpConformance\n-- complete\n");
    assert_eq!(dump_ast(&sql).unwrap(), dump_ast(&wrapped).unwrap());
}

#[hegel::test(test_cases = 2000)]
fn literal_text_is_preserved(tc: TestCase) {
    let value = tc.draw(gs::text().max_size(100));
    let value = value.replace('\0', "").replace('\'', "''");
    let sql = format!("SELECT '{value}';");
    assert_eq!(
        dump_ast(&sql).unwrap(),
        dump_ast(&format!("-- header\n{sql}")).unwrap()
    );
}

#[hegel::test(test_cases = 2000)]
fn rows_cannot_move_across_other_statements(tc: TestCase) {
    let n = tc.draw(gs::integers::<i32>());
    let row = format!("INSERT INTO t VALUES ({n});");
    assert_ne!(
        dump_ast(&format!("{row}SET search_path = public;")).unwrap(),
        dump_ast(&format!("SET search_path = public;{row}")).unwrap()
    );
}

#[hegel::test(test_cases = 2000)]
fn null_and_empty_text_differ(tc: TestCase) {
    let n = tc.draw(gs::integers::<u32>());
    assert_ne!(
        dump_ast(&format!("INSERT INTO t{n} VALUES (NULL);")).unwrap(),
        dump_ast(&format!("INSERT INTO t{n} VALUES ('');")).unwrap()
    );
}

#[hegel::test(test_cases = 2000)]
fn wrapper_words_in_comments_and_strings_survive(tc: TestCase) {
    let n = tc.draw(gs::integers::<u32>());
    let sql = format!(
        "SELECT '{n}\n\\restrict TursoPgdumpConformance\n\\unrestrict TursoPgdumpConformance';\n"
    );
    let dump = format!("-- \\restrict TursoPgdumpConformance\n\\restrict TursoPgdumpConformance\n{sql}\\unrestrict TursoPgdumpConformance\n-- \\unrestrict TursoPgdumpConformance\n");
    assert_eq!(dump_ast(&sql).unwrap(), dump_ast(&dump).unwrap());
}

#[hegel::test(test_cases = 2000)]
fn sql_after_wrapper_is_rejected(tc: TestCase) {
    let n = tc.draw(gs::integers::<u32>());
    assert!(dump_ast(&format!("\\restrict TursoPgdumpConformance\nSELECT 1;\n\\unrestrict TursoPgdumpConformance\nSELECT {n};")).is_err());
}

#[hegel::test(test_cases = 2000)]
fn copy_without_terminator_is_rejected(tc: TestCase) {
    let n = tc.draw(gs::integers::<u32>());
    assert!(dump_ast(&format!("COPY t{n} FROM STDIN;")).is_err());
}

#[hegel::test(test_cases = 2000)]
fn wrapper_tokens_inside_literals_are_data(tc: TestCase) {
    let n = tc.draw(gs::integers::<u32>());
    let sql = format!("SELECT '{n}\n\\restrict TursoPgdumpConformance';");
    assert!(dump_ast(&sql).is_ok());
}
