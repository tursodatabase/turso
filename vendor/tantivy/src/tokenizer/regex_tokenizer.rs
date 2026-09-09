use regex::Regex;

use super::{Token, TokenStream, Tokenizer};
use crate::TantivyError;

/// Tokenize the text by using a regex pattern to split.
///
/// Each match of the regex emits a distinct token, empty tokens will not be emitted. Anchors such
/// as `\A` will match the text from the part where the last token was emitted or the beginning of
/// the complete text if no token was emitted yet.
///
/// Example: `` 'aaa' bbb 'ccc' 'ddd' `` with the pattern `` '(?:\w*)' `` will be tokenized as
/// followed:
///
/// | Term     | aaa  | ccc    | ddd   |
/// |----------|------|--------|-------|
/// | Position | 1    | 2      | 3     |
/// | Offsets  |0,5   | 10,15  | 16,21 |
///
///
/// # Example
///
/// ```rust
/// use tantivy::tokenizer::*;
///
/// let mut tokenizer = RegexTokenizer::new(r"'(?:\w*)'").unwrap();
/// let mut stream = tokenizer.token_stream("'aaa' bbb 'ccc' 'ddd'");
/// {
///     let token = stream.next().unwrap();
///     assert_eq!(token.text, "'aaa'");
///     assert_eq!(token.offset_from, 0);
///     assert_eq!(token.offset_to, 5);
/// }
/// {
///   let token = stream.next().unwrap();
///     assert_eq!(token.text, "'ccc'");
///     assert_eq!(token.offset_from, 10);
///     assert_eq!(token.offset_to, 15);
/// }
/// {
///   let token = stream.next().unwrap();
///     assert_eq!(token.text, "'ddd'");
///     assert_eq!(token.offset_from, 16);
///     assert_eq!(token.offset_to, 21);
/// }
/// assert!(stream.next().is_none());
/// ```

#[derive(Clone)]
pub struct RegexTokenizer {
    regex: Regex,
    token: Token,
}

impl RegexTokenizer {
    /// Creates a new RegexTokenizer.
    pub fn new(regex_pattern: &str) -> crate::Result<RegexTokenizer> {
        Regex::new(regex_pattern)
            .map_err(|_| TantivyError::InvalidArgument(regex_pattern.to_owned()))
            .map(|regex| Self {
                regex,
                token: Token::default(),
            })
    }
}

impl Tokenizer for RegexTokenizer {
    type TokenStream<'a> = RegexTokenStream<'a>;
    fn token_stream<'a>(&'a mut self, text: &'a str) -> RegexTokenStream<'a> {
        self.token.reset();
        RegexTokenStream {
            regex: self.regex.clone(),
            text,
            token: &mut self.token,
            cursor: 0,
        }
    }
}

pub struct RegexTokenStream<'a> {
    regex: Regex,
    text: &'a str,
    token: &'a mut Token,
    cursor: usize,
}

impl TokenStream for RegexTokenStream<'_> {
    fn advance(&mut self) -> bool {
        let Some(regex_match) = self.regex.find(self.text) else {
            return false;
        };
        if regex_match.as_str().is_empty() {
            return false;
        }
        self.token.text.clear();
        self.token.text.push_str(regex_match.as_str());

        self.token.offset_from = self.cursor + regex_match.start();
        self.cursor += regex_match.end();
        self.token.offset_to = self.cursor;

        self.token.position = self.token.position.wrapping_add(1);

        self.text = &self.text[regex_match.end()..];
        true
    }

    fn token(&self) -> &Token {
        self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        self.token
    }
}

#[cfg(test)]
mod tests {
    use crate::tokenizer::regex_tokenizer::RegexTokenizer;
    use crate::tokenizer::tests::assert_token;
    use crate::tokenizer::{TextAnalyzer, Token};

    #[test]
    fn test_regex_tokenizer() {
        let tokens = token_stream_helper("'aaa' bbb 'ccc' 'ddd'", r"'(?:\w*)'");
        assert_eq!(tokens.len(), 3);
        assert_token(&tokens[0], 0, "'aaa'", 0, 5);
        assert_token(&tokens[1], 1, "'ccc'", 10, 15);
        assert_token(&tokens[2], 2, "'ddd'", 16, 21);
    }

    #[test]
    fn test_regexp_tokenizer_no_match_on_input_data() {
        let tokens = token_stream_helper("aaa", r"'(?:\w*)'");
        assert_eq!(tokens.len(), 0);
    }

    #[test]
    fn test_regexp_tokenizer_no_input_data() {
        let tokens = token_stream_helper("", r"'(?:\w*)'");
        assert_eq!(tokens.len(), 0);
    }

    #[test]
    fn test_regexp_tokenizer_error_on_invalid_regex() {
        let tokenizer = RegexTokenizer::new(r"\@(");
        assert_eq!(tokenizer.is_err(), true);
        assert_eq!(
            tokenizer.err().unwrap().to_string(),
            "An invalid argument was passed: '\\@('"
        );
    }

    fn token_stream_helper(text: &str, pattern: &str) -> Vec<Token> {
        let r = RegexTokenizer::new(pattern).unwrap();
        let mut a = TextAnalyzer::from(r);
        let mut token_stream = a.token_stream(text);
        let mut tokens: Vec<Token> = vec![];
        let mut add_token = |token: &Token| {
            tokens.push(token.clone());
        };
        token_stream.process(&mut add_token);
        tokens
    }
}
