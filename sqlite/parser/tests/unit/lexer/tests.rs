use super::*;
use std::collections::HashMap;

#[test]
fn test_lexer_one_tok() {
    let test_cases = vec![
        (b"    ".as_slice(), Token::new(b"    ", TokenType::TK_NONE)),
        (
            b"-- This is a comment\n".as_slice(),
            Token::new(b"-- This is a comment\n", TokenType::TK_NONE), // comment
        ),
        (b"-".as_slice(), Token::new(b"-", TokenType::TK_MINUS)),
        (b"->".as_slice(), Token::new(b"->", TokenType::TK_PTR)),
        (b"->>".as_slice(), Token::new(b"->>", TokenType::TK_PTR)),
        (b"(".as_slice(), Token::new(b"(", TokenType::TK_LP)),
        (b")".as_slice(), Token::new(b")", TokenType::TK_RP)),
        (b";".as_slice(), Token::new(b";", TokenType::TK_SEMI)),
        (b"+".as_slice(), Token::new(b"+", TokenType::TK_PLUS)),
        (b"*".as_slice(), Token::new(b"*", TokenType::TK_STAR)),
        (b"/".as_slice(), Token::new(b"/", TokenType::TK_SLASH)),
        (
            b"/* This is a block comment */".as_slice(),
            Token::new(b"/* This is a block comment */", TokenType::TK_NONE), // comment
        ),
        (
            b"/* This is a\n\n block comment */".as_slice(),
            Token::new(b"/* This is a\n\n block comment */", TokenType::TK_NONE), // comment
        ),
        (
            b"/* This is a** block* comment */".as_slice(),
            Token::new(b"/* This is a** block* comment */", TokenType::TK_NONE), // comment
        ),
        (b"=".as_slice(), Token::new(b"=", TokenType::TK_EQ)),
        (b"==".as_slice(), Token::new(b"==", TokenType::TK_EQ)),
        (b"<".as_slice(), Token::new(b"<", TokenType::TK_LT)),
        (b"<>".as_slice(), Token::new(b"<>", TokenType::TK_NE)),
        (b"<=".as_slice(), Token::new(b"<=", TokenType::TK_LE)),
        (b"<<".as_slice(), Token::new(b"<<", TokenType::TK_LSHIFT)),
        (b">".as_slice(), Token::new(b">", TokenType::TK_GT)),
        (b">=".as_slice(), Token::new(b">=", TokenType::TK_GE)),
        (b">>".as_slice(), Token::new(b">>", TokenType::TK_RSHIFT)),
        (b"!=".as_slice(), Token::new(b"!=", TokenType::TK_NE)),
        (b"|".as_slice(), Token::new(b"|", TokenType::TK_BITOR)),
        (b"||".as_slice(), Token::new(b"||", TokenType::TK_CONCAT)),
        (b",".as_slice(), Token::new(b",", TokenType::TK_COMMA)),
        (b"&".as_slice(), Token::new(b"&", TokenType::TK_BITAND)),
        (b"~".as_slice(), Token::new(b"~", TokenType::TK_BITNOT)),
        (
            b"'string'".as_slice(),
            Token::new(b"'string'", TokenType::TK_STRING),
        ),
        (
            b"`identifier`".as_slice(),
            Token::new(b"`identifier`", TokenType::TK_ID),
        ),
        (
            b"\"quoted string\"".as_slice(),
            Token::new(b"\"quoted string\"", TokenType::TK_ID),
        ),
        (
            b"\"\"\"triple \"\"quoted string\"\"\"".as_slice(),
            Token::new(b"\"\"\"triple \"\"quoted string\"\"\"", TokenType::TK_ID),
        ),
        (
            b"```triple ``quoted string```".as_slice(),
            Token::new(b"```triple ``quoted string```", TokenType::TK_ID),
        ),
        (
            b"'''triple ''quoted string'''".as_slice(),
            Token::new(b"'''triple ''quoted string'''", TokenType::TK_STRING),
        ),
        (b".".as_slice(), Token::new(b".", TokenType::TK_DOT)),
        (b".123".as_slice(), Token::new(b".123", TokenType::TK_FLOAT)),
        (b".456".as_slice(), Token::new(b".456", TokenType::TK_FLOAT)),
        (
            b".456e789".as_slice(),
            Token::new(b".456e789", TokenType::TK_FLOAT),
        ),
        (
            b".456E-789".as_slice(),
            Token::new(b".456E-789", TokenType::TK_FLOAT),
        ),
        (b"123".as_slice(), Token::new(b"123", TokenType::TK_INTEGER)),
        (
            b"9_223_372_036_854_775_807".as_slice(),
            Token::new(b"9_223_372_036_854_775_807", TokenType::TK_INTEGER),
        ),
        (
            b"123.456".as_slice(),
            Token::new(b"123.456", TokenType::TK_FLOAT),
        ),
        (
            b"123e456".as_slice(),
            Token::new(b"123e456", TokenType::TK_FLOAT),
        ),
        (
            b"123E-456".as_slice(),
            Token::new(b"123E-456", TokenType::TK_FLOAT),
        ),
        (
            b"0x1A3F".as_slice(),
            Token::new(b"0x1A3F", TokenType::TK_INTEGER),
        ),
        (
            b"0x1A3F_5678".as_slice(),
            Token::new(b"0x1A3F_5678", TokenType::TK_INTEGER),
        ),
        (
            b"0x1A3F_5678e9".as_slice(),
            Token::new(b"0x1A3F_5678e9", TokenType::TK_INTEGER),
        ),
        (b"[".as_slice(), Token::new(b"[", TokenType::TK_LBRACKET)),
        (b"]".as_slice(), Token::new(b"]", TokenType::TK_RBRACKET)),
        (
            b"?123".as_slice(),
            Token::new(b"?123", TokenType::TK_VARIABLE),
        ),
        (b"?".as_slice(), Token::new(b"?", TokenType::TK_VARIABLE)),
        (
            b"$var_name".as_slice(),
            Token::new(b"$var_name", TokenType::TK_VARIABLE),
        ),
        (
            b"@param".as_slice(),
            Token::new(b"@param", TokenType::TK_VARIABLE),
        ),
        (
            b":named_param".as_slice(),
            Token::new(b":named_param", TokenType::TK_VARIABLE),
        ),
        // TCL-style names, lexed as SQLite does: "::" pairs belong to
        // the name wherever they sit.
        (
            b"$::global_var".as_slice(),
            Token::new(b"$::global_var", TokenType::TK_VARIABLE),
        ),
        (
            b"$ns::var".as_slice(),
            Token::new(b"$ns::var", TokenType::TK_VARIABLE),
        ),
        (
            b"@a::b::c".as_slice(),
            Token::new(b"@a::b::c", TokenType::TK_VARIABLE),
        ),
        (
            b":a::b::c".as_slice(),
            Token::new(b":a::b::c", TokenType::TK_VARIABLE),
        ),
        (
            b":::global_var".as_slice(),
            Token::new(b":::global_var", TokenType::TK_VARIABLE),
        ),
        (
            b"$a::".as_slice(),
            Token::new(b"$a::", TokenType::TK_VARIABLE),
        ),
        (
            b"$a::::b".as_slice(),
            Token::new(b"$a::::b", TokenType::TK_VARIABLE),
        ),
        (
            b"$1::2".as_slice(),
            Token::new(b"$1::2", TokenType::TK_VARIABLE),
        ),
        // TCL array elements: one "(...)" suffix, no whitespace inside.
        (
            b"$arr(elem)".as_slice(),
            Token::new(b"$arr(elem)", TokenType::TK_VARIABLE),
        ),
        (
            b"$arr(12x)".as_slice(),
            Token::new(b"$arr(12x)", TokenType::TK_VARIABLE),
        ),
        (
            b"$a([b])".as_slice(),
            Token::new(b"$a([b])", TokenType::TK_VARIABLE),
        ),
        (
            b"$a('b')".as_slice(),
            Token::new(b"$a('b')", TokenType::TK_VARIABLE),
        ),
        (
            b"$ns::arr(k)".as_slice(),
            Token::new(b"$ns::arr(k)", TokenType::TK_VARIABLE),
        ),
        (
            b"x'1234567890abcdef'".as_slice(),
            Token::new(b"x'1234567890abcdef'", TokenType::TK_BLOB),
        ),
        (
            b"X'1234567890abcdef'".as_slice(),
            Token::new(b"X'1234567890abcdef'", TokenType::TK_BLOB),
        ),
        (b"x''".as_slice(), Token::new(b"x''", TokenType::TK_BLOB)),
        (b"X''".as_slice(), Token::new(b"X''", TokenType::TK_BLOB)),
        (
            b"wHeRe".as_slice(),
            Token::new(b"wHeRe", TokenType::TK_WHERE),
        ),
        (
            b"wHeRe123".as_slice(),
            Token::new(b"wHeRe123", TokenType::TK_ID),
        ),
        (
            b"wHeRe_123".as_slice(),
            Token::new(b"wHeRe_123", TokenType::TK_ID),
        ),
        // issue 2933
        (b"1.e5".as_slice(), Token::new(b"1.e5", TokenType::TK_FLOAT)),
        // issue 3425
        (b"/*".as_slice(), Token::new(b"/*", TokenType::TK_NONE)),
        (b"/**".as_slice(), Token::new(b"/**", TokenType::TK_NONE)),
    ];

    for (input, expected) in test_cases {
        let mut lexer = Lexer::new(input);
        let token = lexer.next().unwrap().unwrap();
        let expect_value = unsafe { expected.to_utf8_unchecked() };
        let got_value = unsafe { token.to_utf8_unchecked() };
        println!("Input: {input:?}, Expected: {expect_value:?}, Got: {got_value:?}");
        assert_eq!(got_value, expect_value);
        assert_eq!(token.token_type, expected.token_type);
    }
}

#[test]
fn test_lexer_bad_variable_names() {
    // A named parameter needs at least one identifier byte; "::" pairs
    // alone do not make a name, and a "(...)" suffix must be closed with
    // no whitespace inside. Same as SQLite, which reports these as
    // unrecognized tokens.
    let bad_inputs: Vec<&[u8]> = vec![
        b"$",
        b"$::",
        b"@",
        b"::::a",
        b"$(",
        b"$(elem)",
        b"$a(unclosed",
        b"$a(x y)",
        b"$a(b;",
    ];
    for input in bad_inputs {
        let mut lexer = Lexer::new(input);
        let result = lexer.next().unwrap();
        assert!(
            matches!(result, Err(Error::UnrecognizedToken { .. })),
            "expected UnrecognizedToken for {:?}, got {result:?}",
            String::from_utf8_lossy(input)
        );
    }
}

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
        ("CONCURRENT", TokenType::TK_CONCURRENT),
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
        ("OPTIMIZE", TokenType::TK_OPTIMIZE),
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
        ("WITHIN", TokenType::TK_WITHIN),
        ("WITHOUT", TokenType::TK_WITHOUT),
    ]);

    for (key, value) in &values {
        assert!(keyword_or_id_token(key.as_bytes()) == *value);
        assert!(keyword_or_id_token(key.as_bytes().to_ascii_lowercase().as_slice()) == *value);
    }

    assert_eq!(keyword_or_id_token(b""), TokenType::TK_ID);
    assert_eq!(keyword_or_id_token(b"wrong"), TokenType::TK_ID);
    assert_eq!(keyword_or_id_token(b"super wrong"), TokenType::TK_ID);
    assert_eq!(keyword_or_id_token(b"super_wrong"), TokenType::TK_ID);
    assert_eq!(
        keyword_or_id_token(b"aae26e78-3ba7-4627-8f8f-02623302495a"),
        TokenType::TK_ID
    );
    assert_eq!(
        keyword_or_id_token("Crème Brulée".as_bytes()),
        TokenType::TK_ID
    );
    assert_eq!(keyword_or_id_token("fróm".as_bytes()), TokenType::TK_ID);
}

#[test]
fn test_lexer_multi_tok() {
    let test_cases = vec![
        (
            b"    SELECT 1".as_slice(),
            vec![
                Token::new(b"    ", TokenType::TK_NONE),
                Token::new(b"SELECT", TokenType::TK_SELECT),
                Token::new(b" ", TokenType::TK_NONE),
                Token::new(b"1", TokenType::TK_INTEGER),
            ],
        ),
        (
            b"INSERT INTO users VALUES (1,2,3)".as_slice(),
            vec![
                Token::new(b"INSERT", TokenType::TK_INSERT),
                Token::new(b" ", TokenType::TK_NONE),
                Token::new(b"INTO", TokenType::TK_INTO),
                Token::new(b" ", TokenType::TK_NONE),
                Token::new(b"users", TokenType::TK_ID),
                Token::new(b" ", TokenType::TK_NONE),
                Token::new(b"VALUES", TokenType::TK_VALUES),
                Token::new(b" ", TokenType::TK_NONE),
                Token::new(b"(", TokenType::TK_LP),
                Token::new(b"1", TokenType::TK_INTEGER),
                Token::new(b",", TokenType::TK_COMMA),
                Token::new(b"2", TokenType::TK_INTEGER),
                Token::new(b",", TokenType::TK_COMMA),
                Token::new(b"3", TokenType::TK_INTEGER),
                Token::new(b")", TokenType::TK_RP),
            ],
        ),
        // issue 2933
        (
            b"u.email".as_slice(),
            vec![
                Token::new(b"u", TokenType::TK_ID),
                Token::new(b".", TokenType::TK_DOT),
                Token::new(b"email", TokenType::TK_ID),
            ],
        ),
        // Slices keep their standalone colons: a `:` is only a
        // parameter when an identifier byte or a "::" pair follows it.
        (
            b"a[1:2]".as_slice(),
            vec![
                Token::new(b"a", TokenType::TK_ID),
                Token::new(b"[", TokenType::TK_LBRACKET),
                Token::new(b"1", TokenType::TK_INTEGER),
                Token::new(b":", TokenType::TK_COLON),
                Token::new(b"2", TokenType::TK_INTEGER),
                Token::new(b"]", TokenType::TK_RBRACKET),
            ],
        ),
        (
            b"a[:2]".as_slice(),
            vec![
                Token::new(b"a", TokenType::TK_ID),
                Token::new(b"[", TokenType::TK_LBRACKET),
                Token::new(b":", TokenType::TK_COLON),
                Token::new(b"2", TokenType::TK_INTEGER),
                Token::new(b"]", TokenType::TK_RBRACKET),
            ],
        ),
        (
            b"a[1:]".as_slice(),
            vec![
                Token::new(b"a", TokenType::TK_ID),
                Token::new(b"[", TokenType::TK_LBRACKET),
                Token::new(b"1", TokenType::TK_INTEGER),
                Token::new(b":", TokenType::TK_COLON),
                Token::new(b"]", TokenType::TK_RBRACKET),
            ],
        ),
        (
            b"a[:]".as_slice(),
            vec![
                Token::new(b"a", TokenType::TK_ID),
                Token::new(b"[", TokenType::TK_LBRACKET),
                Token::new(b":", TokenType::TK_COLON),
                Token::new(b"]", TokenType::TK_RBRACKET),
            ],
        ),
        // `1::hi` is the slice colon followed by the parameter `:hi`:
        // the first colon has no identifier byte or "::" pair after it.
        (
            b"a[1::hi]".as_slice(),
            vec![
                Token::new(b"a", TokenType::TK_ID),
                Token::new(b"[", TokenType::TK_LBRACKET),
                Token::new(b"1", TokenType::TK_INTEGER),
                Token::new(b":", TokenType::TK_COLON),
                Token::new(b":hi", TokenType::TK_VARIABLE),
                Token::new(b"]", TokenType::TK_RBRACKET),
            ],
        ),
        // A bare "::" between identifiers is two colons; only ":::name"
        // is a parameter.
        (
            b"a::b".as_slice(),
            vec![
                Token::new(b"a", TokenType::TK_ID),
                Token::new(b":", TokenType::TK_COLON),
                Token::new(b":b", TokenType::TK_VARIABLE),
            ],
        ),
        // Like SQLite, the name is greedy: an unspaced slice whose lower
        // bound is a parameter is one name. Write `a[$lo : :hi]` or
        // `a[$lo:$hi]` for a slice.
        (
            b"a[$lo::hi]".as_slice(),
            vec![
                Token::new(b"a", TokenType::TK_ID),
                Token::new(b"[", TokenType::TK_LBRACKET),
                Token::new(b"$lo::hi", TokenType::TK_VARIABLE),
                Token::new(b"]", TokenType::TK_RBRACKET),
            ],
        ),
        (
            b"a[$lo:$hi]".as_slice(),
            vec![
                Token::new(b"a", TokenType::TK_ID),
                Token::new(b"[", TokenType::TK_LBRACKET),
                Token::new(b"$lo", TokenType::TK_VARIABLE),
                Token::new(b":", TokenType::TK_COLON),
                Token::new(b"$hi", TokenType::TK_VARIABLE),
                Token::new(b"]", TokenType::TK_RBRACKET),
            ],
        ),
        (
            b"$x:: y".as_slice(),
            vec![
                Token::new(b"$x::", TokenType::TK_VARIABLE),
                Token::new(b" ", TokenType::TK_NONE),
                Token::new(b"y", TokenType::TK_ID),
            ],
        ),
        // The "(...)" suffix ends the name: what follows is a new token.
        (
            b"$a(b)c".as_slice(),
            vec![
                Token::new(b"$a(b)", TokenType::TK_VARIABLE),
                Token::new(b"c", TokenType::TK_ID),
            ],
        ),
        (
            b"$a(b)+$c(d)".as_slice(),
            vec![
                Token::new(b"$a(b)", TokenType::TK_VARIABLE),
                Token::new(b"+", TokenType::TK_PLUS),
                Token::new(b"$c(d)", TokenType::TK_VARIABLE),
            ],
        ),
        (
            b"$a(b)(c)".as_slice(),
            vec![
                Token::new(b"$a(b)", TokenType::TK_VARIABLE),
                Token::new(b"(", TokenType::TK_LP),
                Token::new(b"c", TokenType::TK_ID),
                Token::new(b")", TokenType::TK_RP),
            ],
        ),
    ];

    for (input, expected_tokens) in test_cases {
        let lexer = Lexer::new(input);
        let mut tokens = Vec::new();

        for token in lexer {
            tokens.push(token.unwrap());
        }

        assert_eq!(tokens.len(), expected_tokens.len());

        for (i, token) in tokens.iter().enumerate() {
            let expect_value =
                unsafe { String::from_utf8_unchecked(expected_tokens[i].value.to_vec()) };
            let got_value = unsafe { String::from_utf8_unchecked(token.value.to_vec()) };
            assert_eq!(got_value, expect_value);
            assert_eq!(token.token_type, expected_tokens[i].token_type);
        }
    }
}
