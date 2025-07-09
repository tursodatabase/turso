//! SQLite dialect

use std::fmt::Formatter;
use std::str;

mod token;
pub use token::TokenType;

/// Token value (lexeme)
#[derive(Clone, Copy)]
pub struct Token<'i>(pub usize, pub &'i [u8], pub usize);

pub(crate) fn sentinel(start: usize) -> Token<'static> {
    Token(start, b"", start)
}

impl Token<'_> {
    /// Access token value
    pub fn unwrap(self) -> String {
        from_bytes(self.1)
    }
}

impl std::fmt::Debug for Token<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Token").field(&self.1).finish()
    }
}

impl TokenType {
    // TODO try Cow<&'static, str> (Borrowed<&'static str> for keyword and Owned<String> for below),
    // => Syntax error on keyword will be better
    // => `from_token` will become unnecessary
    pub(crate) fn to_token(self, start: usize, value: &[u8], end: usize) -> Token<'_> {
        Token(start, value, end)
    }
}

pub(crate) fn from_bytes(bytes: &[u8]) -> String {
    unsafe { str::from_utf8_unchecked(bytes).to_owned() }
}

include!(concat!(env!("OUT_DIR"), "/keywords.rs"));

pub(crate) fn is_identifier(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    let bytes = name.as_bytes();
    is_identifier_start(bytes[0])
        && (bytes.len() == 1 || bytes[1..].iter().all(|b| is_identifier_continue(*b)))
}

pub(crate) fn is_identifier_start(b: u8) -> bool {
    b.is_ascii_uppercase() || b == b'_' || b.is_ascii_lowercase() || b > b'\x7F'
}

pub(crate) fn is_identifier_continue(b: u8) -> bool {
    b == b'$'
        || b.is_ascii_digit()
        || b.is_ascii_uppercase()
        || b == b'_'
        || b.is_ascii_lowercase()
        || b > b'\x7F'
}

// keyword may become an identifier
// see %fallback in parse.y
pub(crate) fn from_token(_ty: u16, value: Token) -> String {
    from_bytes(value.1)
}

impl TokenType {
    /// Return the associated string (mainly for testing)
    pub const fn as_str(&self) -> Option<&'static str> {
        use TokenType::*;
        match self {
            TK_ABORT => Some("ABORT"),
            TK_ACTION => Some("ACTION"),
            TK_ADD => Some("ADD"),
            TK_AFTER => Some("AFTER"),
            TK_ALL => Some("ALL"),
            TK_ALTER => Some("ALTER"),
            TK_ANALYZE => Some("ANALYZE"),
            TK_ALWAYS => Some("ALWAYS"),
            TK_AND => Some("AND"),
            TK_AS => Some("AS"),
            TK_ASC => Some("ASC"),
            TK_ATTACH => Some("ATTACH"),
            TK_AUTOINCR => Some("AUTOINCREMENT"),
            TK_BEFORE => Some("BEFORE"),
            TK_BEGIN => Some("BEGIN"),
            TK_BETWEEN => Some("BETWEEN"),
            TK_BY => Some("BY"),
            TK_CASCADE => Some("CASCADE"),
            TK_CASE => Some("CASE"),
            TK_CAST => Some("CAST"),
            TK_CHECK => Some("CHECK"),
            TK_COLLATE => Some("COLLATE"),
            TK_COLUMNKW => Some("COLUMN"),
            TK_COMMIT => Some("COMMIT"),
            TK_CONFLICT => Some("CONFLICT"),
            TK_CONSTRAINT => Some("CONSTRAINT"),
            TK_CREATE => Some("CREATE"),
            TK_CURRENT => Some("CURRENT"),
            TK_DATABASE => Some("DATABASE"),
            TK_DEFAULT => Some("DEFAULT"),
            TK_DEFERRABLE => Some("DEFERRABLE"),
            TK_DEFERRED => Some("DEFERRED"),
            TK_DELETE => Some("DELETE"),
            TK_DESC => Some("DESC"),
            TK_DETACH => Some("DETACH"),
            TK_DISTINCT => Some("DISTINCT"),
            TK_DO => Some("DO"),
            TK_DROP => Some("DROP"),
            TK_EACH => Some("EACH"),
            TK_ELSE => Some("ELSE"),
            TK_END => Some("END"),
            TK_ESCAPE => Some("ESCAPE"),
            TK_EXCEPT => Some("EXCEPT"),
            TK_EXCLUDE => Some("EXCLUDE"),
            TK_EXCLUSIVE => Some("EXCLUSIVE"),
            TK_EXISTS => Some("EXISTS"),
            TK_EXPLAIN => Some("EXPLAIN"),
            TK_FAIL => Some("FAIL"),
            TK_FILTER => Some("FILTER"),
            TK_FIRST => Some("FIRST"),
            TK_FOLLOWING => Some("FOLLOWING"),
            TK_FOR => Some("FOR"),
            TK_FOREIGN => Some("FOREIGN"),
            TK_FROM => Some("FROM"),
            TK_GENERATED => Some("GENERATED"),
            TK_GROUP => Some("GROUP"),
            TK_GROUPS => Some("GROUPS"),
            TK_HAVING => Some("HAVING"),
            TK_IF => Some("IF"),
            TK_IGNORE => Some("IGNORE"),
            TK_IMMEDIATE => Some("IMMEDIATE"),
            TK_IN => Some("IN"),
            TK_INDEX => Some("INDEX"),
            TK_INDEXED => Some("INDEXED"),
            TK_INITIALLY => Some("INITIALLY"),
            TK_INSERT => Some("INSERT"),
            TK_INSTEAD => Some("INSTEAD"),
            TK_INTERSECT => Some("INTERSECT"),
            TK_INTO => Some("INTO"),
            TK_IS => Some("IS"),
            TK_ISNULL => Some("ISNULL"),
            TK_JOIN => Some("JOIN"),
            TK_KEY => Some("KEY"),
            TK_LAST => Some("LAST"),
            TK_LIMIT => Some("LIMIT"),
            TK_MATCH => Some("MATCH"),
            TK_MATERIALIZED => Some("MATERIALIZED"),
            TK_NO => Some("NO"),
            TK_NOT => Some("NOT"),
            TK_NOTHING => Some("NOTHING"),
            TK_NOTNULL => Some("NOTNULL"),
            TK_NULL => Some("NULL"),
            TK_NULLS => Some("NULLS"),
            TK_OF => Some("OF"),
            TK_OFFSET => Some("OFFSET"),
            TK_ON => Some("ON"),
            TK_OR => Some("OR"),
            TK_ORDER => Some("ORDER"),
            TK_OTHERS => Some("OTHERS"),
            TK_OVER => Some("OVER"),
            TK_PARTITION => Some("PARTITION"),
            TK_PLAN => Some("PLAN"),
            TK_PRAGMA => Some("PRAGMA"),
            TK_PRECEDING => Some("PRECEDING"),
            TK_PRIMARY => Some("PRIMARY"),
            TK_QUERY => Some("QUERY"),
            TK_RAISE => Some("RAISE"),
            TK_RANGE => Some("RANGE"),
            TK_RECURSIVE => Some("RECURSIVE"),
            TK_REFERENCES => Some("REFERENCES"),
            TK_REINDEX => Some("REINDEX"),
            TK_RELEASE => Some("RELEASE"),
            TK_RENAME => Some("RENAME"),
            TK_REPLACE => Some("REPLACE"),
            TK_RETURNING => Some("RETURNING"),
            TK_RESTRICT => Some("RESTRICT"),
            TK_ROLLBACK => Some("ROLLBACK"),
            TK_ROW => Some("ROW"),
            TK_ROWS => Some("ROWS"),
            TK_SAVEPOINT => Some("SAVEPOINT"),
            TK_SELECT => Some("SELECT"),
            TK_SET => Some("SET"),
            TK_TABLE => Some("TABLE"),
            TK_TEMP => Some("TEMP"), // or TEMPORARY
            TK_TIES => Some("TIES"),
            TK_THEN => Some("THEN"),
            TK_TO => Some("TO"),
            TK_TRANSACTION => Some("TRANSACTION"),
            TK_TRIGGER => Some("TRIGGER"),
            TK_UNBOUNDED => Some("UNBOUNDED"),
            TK_UNION => Some("UNION"),
            TK_UNIQUE => Some("UNIQUE"),
            TK_UPDATE => Some("UPDATE"),
            TK_USING => Some("USING"),
            TK_VACUUM => Some("VACUUM"),
            TK_VALUES => Some("VALUES"),
            TK_VIEW => Some("VIEW"),
            TK_VIRTUAL => Some("VIRTUAL"),
            TK_WHEN => Some("WHEN"),
            TK_WHERE => Some("WHERE"),
            TK_WINDOW => Some("WINDOW"),
            TK_WITH => Some("WITH"),
            TK_WITHOUT => Some("WITHOUT"),
            TK_BITAND => Some("&"),
            TK_BITNOT => Some("~"),
            TK_BITOR => Some("|"),
            TK_COMMA => Some(","),
            TK_CONCAT => Some("||"),
            TK_DOT => Some("."),
            TK_EQ => Some("="), // or ==
            TK_GT => Some(">"),
            TK_GE => Some(">="),
            TK_LP => Some("("),
            TK_LSHIFT => Some("<<"),
            TK_LE => Some("<="),
            TK_LT => Some("<"),
            TK_MINUS => Some("-"),
            TK_NE => Some("!="), // or <>
            TK_PLUS => Some("+"),
            TK_REM => Some("%"),
            TK_RP => Some(")"),
            TK_RSHIFT => Some(">>"),
            TK_SEMI => Some(";"),
            TK_SLASH => Some("/"),
            TK_STAR => Some("*"),
            _ => None,
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_keyword_token() {
        let values = HashMap::from([
            ("ABORT", TokenType::TK_ABORT),
            ("ACTION", TokenType::TK_ACTION),
            ("ADD", TokenType::TK_ADD),
            ("AFTER", TokenType::TK_AFTER),
            ("ALL", TokenType::TK_ALL),
            ("ALTER", TokenType::TK_ALTER),
            ("ALWAYS", TokenType::TK_ALWAYS),
            ("ANALYZE", TokenType::TK_ANALYZE),
            ("AND", TokenType::TK_AND),
            ("AS", TokenType::TK_AS),
            ("ASC", TokenType::TK_ASC),
            ("ATTACH", TokenType::TK_ATTACH),
            ("AUTOINCREMENT", TokenType::TK_AUTOINCR),
            ("BEFORE", TokenType::TK_BEFORE),
            ("BEGIN", TokenType::TK_BEGIN),
            ("BETWEEN", TokenType::TK_BETWEEN),
            ("BY", TokenType::TK_BY),
            ("CASCADE", TokenType::TK_CASCADE),
            ("CASE", TokenType::TK_CASE),
            ("CAST", TokenType::TK_CAST),
            ("CHECK", TokenType::TK_CHECK),
            ("COLLATE", TokenType::TK_COLLATE),
            ("COLUMN", TokenType::TK_COLUMNKW),
            ("COMMIT", TokenType::TK_COMMIT),
            ("CONFLICT", TokenType::TK_CONFLICT),
            ("CONSTRAINT", TokenType::TK_CONSTRAINT),
            ("CREATE", TokenType::TK_CREATE),
            ("CROSS", TokenType::TK_JOIN_KW),
            ("CURRENT", TokenType::TK_CURRENT),
            ("CURRENT_DATE", TokenType::TK_CTIME_KW),
            ("CURRENT_TIME", TokenType::TK_CTIME_KW),
            ("CURRENT_TIMESTAMP", TokenType::TK_CTIME_KW),
            ("DATABASE", TokenType::TK_DATABASE),
            ("DEFAULT", TokenType::TK_DEFAULT),
            ("DEFERRABLE", TokenType::TK_DEFERRABLE),
            ("DEFERRED", TokenType::TK_DEFERRED),
            ("DELETE", TokenType::TK_DELETE),
            ("DESC", TokenType::TK_DESC),
            ("DETACH", TokenType::TK_DETACH),
            ("DISTINCT", TokenType::TK_DISTINCT),
            ("DO", TokenType::TK_DO),
            ("DROP", TokenType::TK_DROP),
            ("EACH", TokenType::TK_EACH),
            ("ELSE", TokenType::TK_ELSE),
            ("END", TokenType::TK_END),
            ("ESCAPE", TokenType::TK_ESCAPE),
            ("EXCEPT", TokenType::TK_EXCEPT),
            ("EXCLUDE", TokenType::TK_EXCLUDE),
            ("EXCLUSIVE", TokenType::TK_EXCLUSIVE),
            ("EXISTS", TokenType::TK_EXISTS),
            ("EXPLAIN", TokenType::TK_EXPLAIN),
            ("FAIL", TokenType::TK_FAIL),
            ("FILTER", TokenType::TK_FILTER),
            ("FIRST", TokenType::TK_FIRST),
            ("FOLLOWING", TokenType::TK_FOLLOWING),
            ("FOR", TokenType::TK_FOR),
            ("FOREIGN", TokenType::TK_FOREIGN),
            ("FROM", TokenType::TK_FROM),
            ("FULL", TokenType::TK_JOIN_KW),
            ("GENERATED", TokenType::TK_GENERATED),
            ("GLOB", TokenType::TK_LIKE_KW),
            ("GROUP", TokenType::TK_GROUP),
            ("GROUPS", TokenType::TK_GROUPS),
            ("HAVING", TokenType::TK_HAVING),
            ("IF", TokenType::TK_IF),
            ("IGNORE", TokenType::TK_IGNORE),
            ("IMMEDIATE", TokenType::TK_IMMEDIATE),
            ("IN", TokenType::TK_IN),
            ("INDEX", TokenType::TK_INDEX),
            ("INDEXED", TokenType::TK_INDEXED),
            ("INITIALLY", TokenType::TK_INITIALLY),
            ("INNER", TokenType::TK_JOIN_KW),
            ("INSERT", TokenType::TK_INSERT),
            ("INSTEAD", TokenType::TK_INSTEAD),
            ("INTERSECT", TokenType::TK_INTERSECT),
            ("INTO", TokenType::TK_INTO),
            ("IS", TokenType::TK_IS),
            ("ISNULL", TokenType::TK_ISNULL),
            ("JOIN", TokenType::TK_JOIN),
            ("KEY", TokenType::TK_KEY),
            ("LAST", TokenType::TK_LAST),
            ("LEFT", TokenType::TK_JOIN_KW),
            ("LIKE", TokenType::TK_LIKE_KW),
            ("LIMIT", TokenType::TK_LIMIT),
            ("MATCH", TokenType::TK_MATCH),
            ("MATERIALIZED", TokenType::TK_MATERIALIZED),
            ("NATURAL", TokenType::TK_JOIN_KW),
            ("NO", TokenType::TK_NO),
            ("NOT", TokenType::TK_NOT),
            ("NOTHING", TokenType::TK_NOTHING),
            ("NOTNULL", TokenType::TK_NOTNULL),
            ("NULL", TokenType::TK_NULL),
            ("NULLS", TokenType::TK_NULLS),
            ("OF", TokenType::TK_OF),
            ("OFFSET", TokenType::TK_OFFSET),
            ("ON", TokenType::TK_ON),
            ("OR", TokenType::TK_OR),
            ("ORDER", TokenType::TK_ORDER),
            ("OTHERS", TokenType::TK_OTHERS),
            ("OUTER", TokenType::TK_JOIN_KW),
            ("OVER", TokenType::TK_OVER),
            ("PARTITION", TokenType::TK_PARTITION),
            ("PLAN", TokenType::TK_PLAN),
            ("PRAGMA", TokenType::TK_PRAGMA),
            ("PRECEDING", TokenType::TK_PRECEDING),
            ("PRIMARY", TokenType::TK_PRIMARY),
            ("QUERY", TokenType::TK_QUERY),
            ("RAISE", TokenType::TK_RAISE),
            ("RANGE", TokenType::TK_RANGE),
            ("RECURSIVE", TokenType::TK_RECURSIVE),
            ("REFERENCES", TokenType::TK_REFERENCES),
            ("REGEXP", TokenType::TK_LIKE_KW),
            ("REINDEX", TokenType::TK_REINDEX),
            ("RELEASE", TokenType::TK_RELEASE),
            ("RENAME", TokenType::TK_RENAME),
            ("REPLACE", TokenType::TK_REPLACE),
            ("RETURNING", TokenType::TK_RETURNING),
            ("RESTRICT", TokenType::TK_RESTRICT),
            ("RIGHT", TokenType::TK_JOIN_KW),
            ("ROLLBACK", TokenType::TK_ROLLBACK),
            ("ROW", TokenType::TK_ROW),
            ("ROWS", TokenType::TK_ROWS),
            ("SAVEPOINT", TokenType::TK_SAVEPOINT),
            ("SELECT", TokenType::TK_SELECT),
            ("SET", TokenType::TK_SET),
            ("TABLE", TokenType::TK_TABLE),
            ("TEMP", TokenType::TK_TEMP),
            ("TEMPORARY", TokenType::TK_TEMP),
            ("THEN", TokenType::TK_THEN),
            ("TIES", TokenType::TK_TIES),
            ("TO", TokenType::TK_TO),
            ("TRANSACTION", TokenType::TK_TRANSACTION),
            ("TRIGGER", TokenType::TK_TRIGGER),
            ("UNBOUNDED", TokenType::TK_UNBOUNDED),
            ("UNION", TokenType::TK_UNION),
            ("UNIQUE", TokenType::TK_UNIQUE),
            ("UPDATE", TokenType::TK_UPDATE),
            ("USING", TokenType::TK_USING),
            ("VACUUM", TokenType::TK_VACUUM),
            ("VALUES", TokenType::TK_VALUES),
            ("VIEW", TokenType::TK_VIEW),
            ("VIRTUAL", TokenType::TK_VIRTUAL),
            ("WHEN", TokenType::TK_WHEN),
            ("WHERE", TokenType::TK_WHERE),
            ("WINDOW", TokenType::TK_WINDOW),
            ("WITH", TokenType::TK_WITH),
            ("WITHOUT", TokenType::TK_WITHOUT),
        ]);

        for (key, value) in &values {
            assert!(keyword_token(key.as_bytes()).unwrap() == *value);
            assert!(
                keyword_token(key.as_bytes().to_ascii_lowercase().as_slice()).unwrap() == *value
            );
        }

        assert!(keyword_token(b"").is_none());
        assert!(keyword_token(b"wrong").is_none());
        assert!(keyword_token(b"super wrong").is_none());
        assert!(keyword_token(b"super_wrong").is_none());
        assert!(keyword_token(b"aae26e78-3ba7-4627-8f8f-02623302495a").is_none());
        assert!(keyword_token("Crème Brulée".as_bytes()).is_none());
        assert!(keyword_token("fróm".as_bytes()).is_none());
    }
}
