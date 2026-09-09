//! Tokenizer are in charge of chopping text into a stream of tokens
//! ready for indexing. This is an separate crate from tantivy, so implementers don't need to update
//! for each new tantivy version.
//!
//! To add support for a tokenizer, implement the [`Tokenizer`] trait.
//! Checkout the [tantivy repo](https://github.com/quickwit-oss/tantivy/tree/main/src/tokenizer) for some examples.

use std::borrow::{Borrow, BorrowMut};
use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

/// Token
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct Token {
    /// Offset (byte index) of the first character of the token.
    /// Offsets shall not be modified by token filters.
    pub offset_from: usize,
    /// Offset (byte index) of the last character of the token + 1.
    /// The text that generated the token should be obtained by
    /// &text[token.offset_from..token.offset_to]
    pub offset_to: usize,
    /// Position, expressed in number of tokens.
    pub position: usize,
    /// Actual text content of the token.
    pub text: String,
    /// Is the length expressed in term of number of original tokens.
    pub position_length: usize,
}

impl Default for Token {
    fn default() -> Token {
        Token {
            offset_from: 0,
            offset_to: 0,
            position: usize::MAX,
            text: String::new(),
            position_length: 1,
        }
    }
}

impl Token {
    /// reset to default
    pub fn reset(&mut self) {
        self.offset_from = 0;
        self.offset_to = 0;
        self.position = usize::MAX;
        self.text.clear();
        self.position_length = 1;
    }
}

/// `Tokenizer` are in charge of splitting text into a stream of token
/// before indexing.
pub trait Tokenizer: 'static + Clone + Send + Sync {
    /// The token stream returned by this Tokenizer.
    type TokenStream<'a>: TokenStream;
    /// Creates a token stream for a given `str`.
    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a>;
}

/// Simple wrapper of `Box<dyn TokenStream + 'a>`.
pub struct BoxTokenStream<'a>(Box<dyn TokenStream + 'a>);

impl TokenStream for BoxTokenStream<'_> {
    fn advance(&mut self) -> bool {
        self.0.advance()
    }

    fn token(&self) -> &Token {
        self.0.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.0.token_mut()
    }
}

impl<'a> BoxTokenStream<'a> {
    pub fn new<T: TokenStream + 'a>(token_stream: T) -> BoxTokenStream<'a> {
        BoxTokenStream(Box::new(token_stream))
    }
}

impl<'a> Deref for BoxTokenStream<'a> {
    type Target = dyn TokenStream + 'a;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}
impl DerefMut for BoxTokenStream<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.0
    }
}

impl<'a> TokenStream for Box<dyn TokenStream + 'a> {
    fn advance(&mut self) -> bool {
        let token_stream: &mut dyn TokenStream = self.borrow_mut();
        token_stream.advance()
    }

    fn token<'b>(&'b self) -> &'b Token {
        let token_stream: &'b (dyn TokenStream + 'a) = self.borrow();
        token_stream.token()
    }

    fn token_mut<'b>(&'b mut self) -> &'b mut Token {
        let token_stream: &'b mut (dyn TokenStream + 'a) = self.borrow_mut();
        token_stream.token_mut()
    }
}

/// `TokenStream` is the result of the tokenization.
///
/// It consists consumable stream of `Token`s.
pub trait TokenStream {
    /// Advance to the next token
    ///
    /// Returns false if there are no other tokens.
    fn advance(&mut self) -> bool;

    /// Returns a reference to the current token.
    fn token(&self) -> &Token;

    /// Returns a mutable reference to the current token.
    fn token_mut(&mut self) -> &mut Token;

    /// Helper to iterate over tokens. It
    /// simply combines a call to `.advance()`
    /// and `.token()`.
    fn next(&mut self) -> Option<&Token> {
        if self.advance() {
            Some(self.token())
        } else {
            None
        }
    }

    /// Helper function to consume the entire `TokenStream`
    /// and push the tokens to a sink function.
    fn process(&mut self, sink: &mut dyn FnMut(&Token)) {
        while self.advance() {
            sink(self.token());
        }
    }
}

/// Trait for the pluggable components of `Tokenizer`s.
pub trait TokenFilter: 'static + Send + Sync {
    /// The Tokenizer type returned by this filter, typically parametrized by the underlying
    /// Tokenizer.
    type Tokenizer<T: Tokenizer>: Tokenizer;
    /// Wraps a Tokenizer and returns a new one.
    fn transform<T: Tokenizer>(self, tokenizer: T) -> Self::Tokenizer<T>;
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn clone() {
        let t1 = Token {
            position: 1,
            offset_from: 2,
            offset_to: 3,
            text: "abc".to_string(),
            position_length: 1,
        };
        let t2 = t1.clone();

        assert_eq!(t1.position, t2.position);
        assert_eq!(t1.offset_from, t2.offset_from);
        assert_eq!(t1.offset_to, t2.offset_to);
        assert_eq!(t1.text, t2.text);
    }
}
