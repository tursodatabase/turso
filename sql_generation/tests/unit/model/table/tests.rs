use crate::model::table::{escape_singlequotes, unescape_singlequotes};

#[test]
fn test_unescape_singlequotes() {
    assert_eq!(unescape_singlequotes("'hello'"), "hello");
    assert_eq!(unescape_singlequotes("'O''Reilly'"), "O'Reilly");
    assert_eq!(
        unescape_singlequotes("'multiple''single''quotes'"),
        "multiple'single'quotes"
    );
    assert_eq!(unescape_singlequotes("'test''''test'"), "test''test");
    assert_eq!(unescape_singlequotes("'many''''''quotes'"), "many'''quotes");
}

#[test]
fn test_escape_singlequotes() {
    assert_eq!(escape_singlequotes("hello"), "'hello'");
    assert_eq!(escape_singlequotes("O'Reilly"), "'O''Reilly'");
    assert_eq!(
        escape_singlequotes("multiple'single'quotes"),
        "'multiple''single''quotes'"
    );
    assert_eq!(escape_singlequotes("test''test"), "'test''''test'");
    assert_eq!(escape_singlequotes("many'''quotes"), "'many''''''quotes'");
}
