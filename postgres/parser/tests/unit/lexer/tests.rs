use super::*;

#[test]
fn test_basic_tokens() {
    let input = b"SELECT * FROM users WHERE id = 1";
    let mut lexer = Lexer::new(input);

    assert_eq!(lexer.next_token().unwrap().token_type, TokenType::Select);
    assert_eq!(lexer.next_token().unwrap().token_type, TokenType::Star);
    assert_eq!(lexer.next_token().unwrap().token_type, TokenType::From);
    assert_eq!(
        lexer.next_token().unwrap().token_type,
        TokenType::Identifier
    );
    assert_eq!(lexer.next_token().unwrap().token_type, TokenType::Where);
    assert_eq!(
        lexer.next_token().unwrap().token_type,
        TokenType::Identifier
    );
    assert_eq!(lexer.next_token().unwrap().token_type, TokenType::Equal);
    assert_eq!(
        lexer.next_token().unwrap().token_type,
        TokenType::IntegerLiteral
    );
}

#[test]
fn test_dollar_parameters() {
    let input = b"SELECT * FROM users WHERE id = $1 AND name = $2";
    let mut lexer = Lexer::new(input);

    // Skip to the dollar parameters
    for _ in 0..7 {
        lexer.next_token().unwrap();
    }

    let token = lexer.next_token().unwrap();
    assert_eq!(token.token_type, TokenType::DollarParameter);
    assert_eq!(token.value, "$1");
}

#[test]
fn test_type_cast() {
    let input = b"SELECT '123'::integer";
    let mut lexer = Lexer::new(input);

    lexer.next_token().unwrap(); // SELECT
    let string_token = lexer.next_token().unwrap();
    assert_eq!(string_token.token_type, TokenType::String);
    assert_eq!(string_token.value, "123");

    let cast_token = lexer.next_token().unwrap();
    assert_eq!(cast_token.token_type, TokenType::TypeCast);
    assert_eq!(cast_token.value, "::");
}

#[test]
fn test_dollar_quoted_string() {
    let input = b"$$Hello World$$";
    let mut lexer = Lexer::new(input);

    let token = lexer.next_token().unwrap();
    assert_eq!(token.token_type, TokenType::DollarQuotedString);
    assert_eq!(token.value, "Hello World");
}

#[test]
fn test_dollar_quoted_string_with_tag() {
    let input = b"$tag$Content with $ and ' characters$tag$";
    let mut lexer = Lexer::new(input);

    let token = lexer.next_token().unwrap();
    assert_eq!(token.token_type, TokenType::DollarQuotedString);
    assert_eq!(token.value, "Content with $ and ' characters");
}

#[test]
fn test_json_operators() {
    let tests = vec![
        (&b"->"[..], TokenType::Arrow),
        (&b"->>"[..], TokenType::LongArrow),
        (&b"#>"[..], TokenType::HashArrow),
        (&b"#>>"[..], TokenType::HashLongArrow),
        (&b"@>"[..], TokenType::Contains),
        (&b"<@"[..], TokenType::ContainedBy),
    ];

    for (input, expected) in tests {
        let mut lexer = Lexer::new(input);
        let token = lexer.next_token().unwrap();
        assert_eq!(token.token_type, expected);
    }
}
