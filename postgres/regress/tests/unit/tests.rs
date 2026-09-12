use super::*;

fn vars(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn feed(scanner: &mut Scanner, line: &str, vars: &HashMap<String, String>) -> Vec<Item> {
    scanner.feed_line(line, vars).unwrap()
}

#[test]
fn statement_interpolates_plain_quoted_and_identifier_variables() {
    let vars = vars(&[("filename", "/tmp/agg.data"), ("col", "f1")]);
    let mut scanner = Scanner::default();
    let items = feed(&mut scanner, "COPY onek FROM :'filename';", &vars);
    match items.as_slice() {
        [Item::Statement(s)] => assert_eq!(s, "COPY onek FROM '/tmp/agg.data';"),
        _ => panic!("expected one statement"),
    }
    let items = feed(&mut scanner, "SELECT :\"col\" FROM t;", &vars);
    match items.as_slice() {
        [Item::Statement(s)] => assert_eq!(s, "SELECT \"f1\" FROM t;"),
        _ => panic!("expected one statement"),
    }
}

#[test]
fn double_colon_casts_and_unknown_variables_stay_literal() {
    let vars = vars(&[("int", "BOOM")]);
    let mut scanner = Scanner::default();
    let items = feed(&mut scanner, "SELECT 1::int, :missing;", &vars);
    match items.as_slice() {
        [Item::Statement(s)] => assert_eq!(s, "SELECT 1::int, :missing;"),
        _ => panic!("expected one statement"),
    }
}

#[test]
fn variables_inside_string_literals_are_not_interpolated() {
    let vars = vars(&[("x", "BOOM")]);
    let mut scanner = Scanner::default();
    let items = feed(&mut scanner, "SELECT ':x';", &vars);
    match items.as_slice() {
        [Item::Statement(s)] => assert_eq!(s, "SELECT ':x';"),
        _ => panic!("expected one statement"),
    }
}

#[test]
fn trailing_gset_becomes_meta_command_and_keeps_the_buffer() {
    let vars = HashMap::new();
    let mut scanner = Scanner::default();
    let items = feed(&mut scanner, "SELECT 1 AS x \\gset", &vars);
    match items.as_slice() {
        [Item::MetaCommand(cmd)] => assert_eq!(cmd, "\\gset"),
        _ => panic!("expected one meta-command"),
    }
    assert_eq!(scanner.take_buffer(), "SELECT 1 AS x");
}

#[test]
fn backslash_inside_quotes_does_not_start_a_meta_command() {
    let vars = HashMap::new();
    let mut scanner = Scanner::default();
    let items = feed(&mut scanner, "SELECT E'a\\n', '\\x';", &vars);
    match items.as_slice() {
        [Item::Statement(s)] => assert_eq!(s, "SELECT E'a\\n', '\\x';"),
        _ => panic!("expected one statement"),
    }
}

#[test]
fn set_concatenates_value_arguments_without_separator() {
    let vars = vars(&[("libdir", "/lib"), ("dlsuffix", ".so")]);
    let args = split_meta_args(":libdir '/regress' :dlsuffix", &vars);
    assert_eq!(args, ["/lib", "/regress", ".so"]);
    assert_eq!(args.concat(), "/lib/regress.so");
}

#[test]
fn meta_args_unescape_single_quoted_strings() {
    let none = HashMap::new();
    assert_eq!(split_meta_args("null '\\\\N'", &none), ["null", "\\N"]);
    assert_eq!(split_meta_args("null ''", &none), ["null", ""]);
    assert_eq!(split_meta_args("null NULL", &none), ["null", "NULL"]);
}

#[test]
fn trailing_comment_only_buffer_is_not_sent_at_eof() {
    let vars = HashMap::new();
    let mut scanner = Scanner::default();
    feed(&mut scanner, "/* and this is", &vars);
    feed(&mut scanner, "the end of the file */", &vars);
    assert_eq!(scanner.take_rest(), None);

    let mut scanner = Scanner::default();
    feed(&mut scanner, "SELECT 1 /* trailing */", &vars);
    assert_eq!(
        scanner.take_rest().as_deref(),
        Some("SELECT 1 /* trailing */")
    );
}

#[test]
fn parse_bool_accepts_psql_prefixes() {
    assert_eq!(parse_bool("t"), Some(true));
    assert_eq!(parse_bool("f"), Some(false));
    assert_eq!(parse_bool("on"), Some(true));
    assert_eq!(parse_bool("off"), Some(false));
    assert_eq!(parse_bool("1"), Some(true));
    assert_eq!(parse_bool("0"), Some(false));
    assert_eq!(parse_bool("yes"), Some(true));
    assert_eq!(parse_bool(":unset_var"), None);
}

#[test]
fn copy_spec_splits_table_query_and_options() {
    let (before, dir, target, options) =
        split_copy_spec("y TO stdout (FORMAT CSV, DELIMITER '|')").unwrap();
    assert_eq!((before.as_str(), dir.as_str()), ("y", "TO"));
    assert_eq!(target, "stdout");
    assert_eq!(options, "(FORMAT CSV, DELIMITER '|')");

    let (before, dir, target, _) =
        split_copy_spec("(select * from t where 'x from y' <> a) to stdout").unwrap();
    assert_eq!(before, "(select * from t where 'x from y' <> a)");
    assert_eq!(dir, "to");
    assert_eq!(target, "stdout");

    let (_, dir, target, _) = split_copy_spec("t from '/tmp/data.csv' with csv").unwrap();
    assert_eq!(dir, "from");
    assert_eq!(target, "/tmp/data.csv");
}
