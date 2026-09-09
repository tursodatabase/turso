//! # Example
//! ```rust
//! use tantivy::tokenizer::*;
//!
//! let mut tokenizer = TextAnalyzer::builder(SimpleTokenizer::default())
//!   .filter(StopWordFilter::remove(vec!["the".to_string(), "is".to_string()]))
//!   .build();
//!
//! let mut stream = tokenizer.token_stream("the fox is crafty");
//! assert_eq!(stream.next().unwrap().text, "fox");
//! assert_eq!(stream.next().unwrap().text, "crafty");
//! assert!(stream.next().is_none());
//! ```
#[cfg(feature = "stopwords")]
#[rustfmt::skip]
mod stopwords;

use std::sync::Arc;

use rustc_hash::FxHashSet;

#[cfg(feature = "stopwords")]
use super::Language;
use super::{Token, TokenFilter, TokenStream, Tokenizer};

/// `TokenFilter` that removes stop words from a token stream
#[derive(Clone)]
pub struct StopWordFilter {
    words: Arc<FxHashSet<String>>,
}

impl StopWordFilter {
    /// Creates a new [`StopWordFilter`] for the given [`Language`]
    ///
    /// Returns `Some` if a list of stop words is available and `None` otherwise.
    #[cfg(feature = "stopwords")]
    pub fn new(language: Language) -> Option<Self> {
        let words = match language {
            Language::Danish => stopwords::DANISH,
            Language::Dutch => stopwords::DUTCH,
            Language::English => {
                // This is the same list of words used by the Apache-licensed Lucene project,
                // c.f. https://github.com/apache/lucene/blob/d5d6dc079395c47cd6d12dcce3bcfdd2c7d9dc63/lucene/analysis/common/src/java/org/apache/lucene/analysis/en/EnglishAnalyzer.java#L46
                &[
                    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in",
                    "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the",
                    "their", "then", "there", "these", "they", "this", "to", "was", "will", "with",
                ]
            }
            Language::Finnish => stopwords::FINNISH,
            Language::French => stopwords::FRENCH,
            Language::German => stopwords::GERMAN,
            Language::Hungarian => stopwords::HUNGARIAN,
            Language::Italian => stopwords::ITALIAN,
            Language::Norwegian => stopwords::NORWEGIAN,
            Language::Portuguese => stopwords::PORTUGUESE,
            Language::Russian => stopwords::RUSSIAN,
            Language::Spanish => stopwords::SPANISH,
            Language::Swedish => stopwords::SWEDISH,
            _ => return None,
        };

        Some(Self::remove(words.iter().map(|&word| word.to_owned())))
    }

    /// Creates a `StopWordFilter` given a list of words to remove
    pub fn remove<W: IntoIterator<Item = String>>(words: W) -> StopWordFilter {
        StopWordFilter {
            words: Arc::new(words.into_iter().collect()),
        }
    }
}

impl TokenFilter for StopWordFilter {
    type Tokenizer<T: Tokenizer> = StopWordFilterWrapper<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> StopWordFilterWrapper<T> {
        StopWordFilterWrapper {
            words: self.words,
            inner: tokenizer,
        }
    }
}

#[derive(Clone)]
pub struct StopWordFilterWrapper<T> {
    words: Arc<FxHashSet<String>>,
    inner: T,
}

impl<T: Tokenizer> Tokenizer for StopWordFilterWrapper<T> {
    type TokenStream<'a> = StopWordFilterStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        StopWordFilterStream {
            words: self.words.clone(),
            tail: self.inner.token_stream(text),
        }
    }
}

pub struct StopWordFilterStream<T> {
    words: Arc<FxHashSet<String>>,
    tail: T,
}

impl<T> StopWordFilterStream<T> {
    fn predicate(&self, token: &Token) -> bool {
        !self.words.contains(&token.text)
    }
}

impl<T: TokenStream> TokenStream for StopWordFilterStream<T> {
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
    use crate::tokenizer::{SimpleTokenizer, StopWordFilter, TextAnalyzer, Token};

    #[test]
    fn test_stop_word() {
        let tokens = token_stream_helper("i am a cat. as yet i have no name.");
        assert_eq!(tokens.len(), 5);
        assert_token(&tokens[0], 3, "cat", 7, 10);
        assert_token(&tokens[1], 5, "yet", 15, 18);
        assert_token(&tokens[2], 7, "have", 21, 25);
        assert_token(&tokens[3], 8, "no", 26, 28);
        assert_token(&tokens[4], 9, "name", 29, 33);
    }

    fn token_stream_helper(text: &str) -> Vec<Token> {
        let stops = vec![
            "a".to_string(),
            "as".to_string(),
            "am".to_string(),
            "i".to_string(),
        ];
        let mut a = TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(StopWordFilter::remove(stops))
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
