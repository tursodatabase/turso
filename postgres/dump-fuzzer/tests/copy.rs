use hegel::{generators as gs, TestCase};
use serde_json::json;
use turso_pg_dump_fuzzer::dump_ast;

#[hegel::test(test_cases = 2000)]
fn decoded_copy_rows_match_input(tc: TestCase) {
    let value = tc.draw(gs::text().max_size(100));
    let value = value.replace('\0', "");
    let encoded = value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t");
    let ast = dump_ast(&format!("COPY t (x, y) FROM stdin;\n{encoded}\t\\N\n\\.\n")).unwrap();
    assert_eq!(ast[1], json!({"CopyData": [[value, null]]}));
}

#[hegel::test(test_cases = 2000)]
fn copy_rows_are_unordered_with_duplicates(tc: TestCase) {
    let values = tc.draw(gs::vecs(gs::integers::<i32>()).max_size(30));
    let rows: Vec<_> = values.iter().map(|v| format!("{v}\n")).collect();
    let a = format!("COPY t (x) FROM stdin;\n{}\\.\n", rows.concat());
    let b = format!(
        "COPY t (x) FROM stdin;\n{}\\.\n",
        rows.into_iter().rev().collect::<String>()
    );
    assert_eq!(dump_ast(&a).unwrap(), dump_ast(&b).unwrap());
}

#[hegel::test(test_cases = 2000)]
fn missing_copy_duplicate_changes_ast(tc: TestCase) {
    let n = tc.draw(gs::integers::<i32>());
    assert_ne!(
        dump_ast(&format!("COPY t FROM stdin;\n{n}\n\\.\n")).unwrap(),
        dump_ast(&format!("COPY t FROM stdin;\n{n}\n{n}\n\\.\n")).unwrap()
    );
}

#[hegel::test(test_cases = 2000)]
fn equivalent_copy_escapes_agree(tc: TestCase) {
    let byte = tc.draw(gs::integers::<u8>().min_value(1).max_value(127));
    let octal = format!("COPY t FROM stdin;\n\\{byte:03o}\n\\.\n");
    let hex = format!("COPY t FROM stdin;\n\\x{byte:02x}\n\\.\n");
    assert_eq!(dump_ast(&octal).unwrap(), dump_ast(&hex).unwrap());
}

#[hegel::test(test_cases = 2000)]
fn copy_text_null_and_empty_remain_distinct(tc: TestCase) {
    let n = tc.draw(gs::integers::<i32>());
    let ast = dump_ast(&format!("COPY t FROM stdin;\n{n}\t\\N\t\\\\N\t\n\\.\n")).unwrap();
    assert_eq!(
        ast[1],
        json!({"CopyData": [[n.to_string(), null, "\\N", ""]]})
    );
}

#[hegel::test(test_cases = 2000)]
fn malformed_copy_rows_are_rejected(tc: TestCase) {
    let n = tc.draw(gs::integers::<i32>());
    assert!(dump_ast(&format!("COPY t (x, y) FROM stdin;\n{n}\n\\.\n")).is_err());
}

#[hegel::test(test_cases = 2000)]
fn copy_text_in_sql_literal_is_not_a_copy_header(tc: TestCase) {
    let n = tc.draw(gs::integers::<i32>());
    let sql = format!("SELECT '{n}\nCOPY t FROM stdin;\n\\.\n';\n");
    assert_eq!(
        dump_ast(&sql).unwrap(),
        dump_ast(&format!("-- header\n{sql}")).unwrap()
    );
}

#[hegel::test(test_cases = 2000)]
fn copy_header_comments_do_not_change_ast(tc: TestCase) {
    let n = tc.draw(gs::integers::<i32>());
    assert_eq!(
        dump_ast(&format!("COPY t FROM stdin;\n{n}\n\\.\n")).unwrap(),
        dump_ast(&format!("COPY t FROM stdin; -- header\n{n}\n\\.\n")).unwrap()
    );
}
