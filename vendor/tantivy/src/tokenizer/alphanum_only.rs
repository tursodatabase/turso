//! # Example
//! ```rust
//! use tantivy::tokenizer::*;
//!
//! let mut tokenizer = TextAnalyzer::builder(RawTokenizer::default())
//!   .filter(AlphaNumOnlyFilter)
//!   .build();
//!
//! let mut stream = tokenizer.token_stream("hello there");
//! // is none because the raw filter emits one token that
//! // contains a space
//! assert!(stream.next().is_none());
//!
//! let mut tokenizer = TextAnalyzer::builder(SimpleTokenizer::default())
//!   .filter(AlphaNumOnlyFilter)
//!   .build();
//!
//! let mut stream = tokenizer.token_stream("hello there 💣");
//! assert!(stream.next().is_some());
//! assert!(stream.next().is_some());
//! // the "emoji" is dropped because its not an alphanum
//! assert!(stream.next().is_none());
//! ```
use super::{Token, TokenFilter, TokenStream, Tokenizer};

/// `TokenFilter` that removes all tokens that contain non
/// ascii alphanumeric characters.
#[derive(Clone)]
pub struct AlphaNumOnlyFilter;

pub struct AlphaNumOnlyFilterStream<T> {
    tail: T,
}

impl<T> AlphaNumOnlyFilterStream<T> {
    fn predicate(&self, token: &Token) -> bool {
        token.text.chars().all(|c| c.is_ascii_alphanumeric())
    }
}

impl TokenFilter for AlphaNumOnlyFilter {
    type Tokenizer<T: Tokenizer> = AlphaNumOnlyFilterWrapper<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> AlphaNumOnlyFilterWrapper<T> {
        AlphaNumOnlyFilterWrapper(tokenizer)
    }
}

#[derive(Clone)]
pub struct AlphaNumOnlyFilterWrapper<T>(T);

impl<T: Tokenizer> Tokenizer for AlphaNumOnlyFilterWrapper<T> {
    type TokenStream<'a> = AlphaNumOnlyFilterStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        AlphaNumOnlyFilterStream {
            tail: self.0.token_stream(text),
        }
    }
}

impl<T: TokenStream> TokenStream for AlphaNumOnlyFilterStream<T> {
    fn advance(&mut self) -> bool {
        while self.tail.advance() {
            if self.predicate(self.tail.token()) {
                return true;
            }
        }

        false
    }

    fn token(&self) -> &Token {
        self.tail.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }
}

#[cfg(test)]
mod tests {
    use crate::tokenizer::tests::assert_token;
    use crate::tokenizer::{AlphaNumOnlyFilter, SimpleTokenizer, TextAnalyzer, Token};

    #[test]
    fn test_alphanum_only() {
        let tokens = token_stream_helper("I am a cat. 我輩は猫である。(1906)");
        assert_eq!(tokens.len(), 5);
        assert_token(&tokens[0], 0, "I", 0, 1);
        assert_token(&tokens[1], 1, "am", 2, 4);
        assert_token(&tokens[2], 2, "a", 5, 6);
        assert_token(&tokens[3], 3, "cat", 7, 10);
        assert_token(&tokens[4], 5, "1906", 37, 41);
    }

    fn token_stream_helper(text: &str) -> Vec<Token> {
        let mut a = TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(AlphaNumOnlyFilter)
            .build();
        let mut token_stream = a.token_stream(text);
        let mut tokens: Vec<Token> = vec![];
        let mut add_token = |token: &Token| {
            tokens.push(token.clone());
        };
        token_stream.process(&mut add_token);
        tokens
    }
}
