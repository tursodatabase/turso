//! Break a rule file into tokens.

use std::str::Chars;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Token {
    Illegal,
    Eof,
    Ident,
    Str,
    Number,
    Whitespace,
    Comment,
    LParen,
    RParen,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    Dollar,
    Colon,
    Asterisk,
    Equals,
    Arrow,
    Ampersand,
    Comma,
    Caret,
    Dot,
    Ellipses,
    Pipe,
}

pub(crate) struct Scanner<'a> {
    rest: Chars<'a>,
    token: Token,
    literal: String,
    line: usize,
    pos: usize,
    token_line: usize,
    token_pos: usize,
}

impl<'a> Scanner<'a> {
    pub fn new(source: &'a str) -> Self {
        Self {
            rest: source.chars(),
            token: Token::Illegal,
            literal: String::new(),
            line: 0,
            pos: 0,
            token_line: 0,
            token_pos: 0,
        }
    }

    pub fn token(&self) -> Token {
        self.token
    }

    pub fn literal(&self) -> &str {
        &self.literal
    }

    /// The 0-based line and column where the last token starts.
    pub fn token_location(&self) -> (usize, usize) {
        (self.token_line, self.token_pos)
    }

    pub fn scan(&mut self) -> Token {
        self.token_line = self.line;
        self.token_pos = self.pos;
        self.literal.clear();
        let Some(ch) = self.peek() else {
            return self.set(Token::Eof);
        };
        if ch.is_whitespace() {
            self.take_while(char::is_whitespace);
            return self.set(Token::Whitespace);
        }
        if ch.is_alphabetic() || ch == '_' {
            self.take_while(|ch| ch.is_alphanumeric() || ch == '_');
            return self.set(Token::Ident);
        }
        if ch.is_numeric() {
            self.take_while(char::is_numeric);
            return self.set(Token::Number);
        }
        self.advance();
        let token = match ch {
            '(' => Token::LParen,
            ')' => Token::RParen,
            '[' => Token::LBracket,
            ']' => Token::RBracket,
            '{' => Token::LBrace,
            '}' => Token::RBrace,
            '$' => Token::Dollar,
            ':' => Token::Colon,
            '*' => Token::Asterisk,
            ',' => Token::Comma,
            '^' => Token::Caret,
            '|' => Token::Pipe,
            '&' => Token::Ampersand,
            '=' => {
                if self.peek() == Some('>') {
                    self.advance();
                    Token::Arrow
                } else {
                    Token::Equals
                }
            }
            '.' => {
                if self.peek() == Some('.') {
                    self.advance();
                    if self.peek() == Some('.') {
                        self.advance();
                        Token::Ellipses
                    } else {
                        Token::Illegal
                    }
                } else {
                    Token::Dot
                }
            }
            '"' => return self.scan_string('"', false),
            '`' => return self.scan_string('`', true),
            '#' => {
                self.take_while(|ch| ch != '\n');
                Token::Comment
            }
            _ => Token::Illegal,
        };
        self.set(token)
    }

    fn scan_string(&mut self, end: char, multi_line: bool) -> Token {
        loop {
            match self.peek() {
                None => return self.set(Token::Illegal),
                Some('\n') if !multi_line => return self.set(Token::Illegal),
                Some(ch) => {
                    self.advance();
                    if ch == end {
                        return self.set(Token::Str);
                    }
                }
            }
        }
    }

    fn take_while(&mut self, keep: impl Fn(char) -> bool) {
        while let Some(ch) = self.peek() {
            if !keep(ch) {
                break;
            }
            self.advance();
        }
    }

    fn set(&mut self, token: Token) -> Token {
        self.token = token;
        token
    }

    fn peek(&self) -> Option<char> {
        self.rest.clone().next()
    }

    fn advance(&mut self) {
        let Some(ch) = self.rest.next() else {
            return;
        };
        self.literal.push(ch);
        if ch == '\n' {
            self.line += 1;
            self.pos = 0;
        } else {
            self.pos += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokens(source: &str) -> Vec<(Token, String)> {
        let mut scanner = Scanner::new(source);
        let mut out = Vec::new();
        loop {
            let token = scanner.scan();
            if token == Token::Eof {
                return out;
            }
            out.push((token, scanner.literal().to_string()));
        }
    }

    #[test]
    fn scans_every_token_kind() {
        let scanned = tokens("(Op $x:* & ^(Sub) [ ... ] \"s\" 12 => a | b , . { } ) # c");
        let kinds: Vec<Token> = scanned
            .iter()
            .filter(|(token, _)| *token != Token::Whitespace)
            .map(|(token, _)| *token)
            .collect();
        assert_eq!(
            kinds,
            vec![
                Token::LParen,
                Token::Ident,
                Token::Dollar,
                Token::Ident,
                Token::Colon,
                Token::Asterisk,
                Token::Ampersand,
                Token::Caret,
                Token::LParen,
                Token::Ident,
                Token::RParen,
                Token::LBracket,
                Token::Ellipses,
                Token::RBracket,
                Token::Str,
                Token::Number,
                Token::Arrow,
                Token::Ident,
                Token::Pipe,
                Token::Ident,
                Token::Comma,
                Token::Dot,
                Token::LBrace,
                Token::RBrace,
                Token::RParen,
                Token::Comment,
            ]
        );
        assert_eq!(scanned.last().unwrap().1, "# c");
    }

    #[test]
    fn string_and_comment_literals_keep_their_delimiters() {
        let scanned = tokens("\"hello\" `multi\nline` # note\n");
        assert_eq!(scanned[0], (Token::Str, "\"hello\"".to_string()));
        assert_eq!(scanned[2], (Token::Str, "`multi\nline`".to_string()));
        assert_eq!(scanned[4], (Token::Comment, "# note".to_string()));
    }

    #[test]
    fn unterminated_string_and_bad_dots_are_illegal() {
        assert_eq!(tokens("\"open\n")[0].0, Token::Illegal);
        assert_eq!(tokens("..")[0], (Token::Illegal, "..".to_string()));
        assert_eq!(tokens("=")[0], (Token::Equals, "=".to_string()));
    }

    #[test]
    fn token_location_is_the_start_of_the_token() {
        let mut scanner = Scanner::new("ab\n  cd");
        scanner.scan();
        assert_eq!(scanner.token_location(), (0, 0));
        scanner.scan();
        scanner.scan();
        assert_eq!(scanner.token(), Token::Ident);
        assert_eq!(scanner.literal(), "cd");
        assert_eq!(scanner.token_location(), (1, 2));
    }
}
