#[test]
fn tests() {
    let t = trybuild::TestCases::new();
    t.pass("tests/explain-name.rs");
    t.pass("tests/explain-p-vals.rs");
    t.pass("tests/explain-desc.rs");
    t.pass("tests/explain-struct.rs");
}
