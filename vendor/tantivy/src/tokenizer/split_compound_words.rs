use aho_corasick::{AhoCorasick, AhoCorasickBuilder, MatchKind};

use super::{Token, TokenFilter, TokenStream, Tokenizer};

/// A [`TokenFilter`] which splits compound words into their parts
/// based on a given dictionary.
///
/// Words only will be split if they can be fully decomposed into
/// consecutive matches into the given dictionary.
///
/// This is mostly useful to split [compound nouns][compound] common to many
/// Germanic languages into their constituents.
///
/// # Example
///
/// The quality of the dictionary determines the quality of the splits,
/// e.g. the missing stem "back" of "backen" implies that "brotbackautomat"
/// is not split in the following example.
///
/// ```rust
/// use tantivy::tokenizer::{SimpleTokenizer, SplitCompoundWords, TextAnalyzer};
///
/// let mut tokenizer =
///        TextAnalyzer::builder(SimpleTokenizer::default())
///        .filter(
///            SplitCompoundWords::from_dictionary([
///                 "dampf", "schiff", "fahrt", "brot", "backen", "automat",
///            ])
///            .unwrap()
///        )
///        .build();
/// {
///     let mut stream = tokenizer.token_stream("dampfschifffahrt");
///     assert_eq!(stream.next().unwrap().text, "dampf");
///     assert_eq!(stream.next().unwrap().text, "schiff");
///     assert_eq!(stream.next().unwrap().text, "fahrt");
///     assert_eq!(stream.next(), None);
/// }
/// let mut stream = tokenizer.token_stream("brotbackautomat");
/// assert_eq!(stream.next().unwrap().text, "brotbackautomat");
/// assert_eq!(stream.next(), None);
/// ```
///
/// [compound]: https://en.wikipedia.org/wiki/Compound_(linguistics)
#[derive(Clone)]
pub struct SplitCompoundWords {
    dict: AhoCorasick,
}

impl SplitCompoundWords {
    /// Create a filter from a given dictionary.
    ///
    /// The dictionary will be used to construct an [`AhoCorasick`] automaton
    /// with reasonable defaults. See [`from_automaton`][Self::from_automaton] if
    /// more control over its construction is required.
    pub fn from_dictionary<I, P>(dict: I) -> crate::Result<Self>
    where
        I: IntoIterator<Item = P>,
        P: AsRef<[u8]>,
    {
        let dict = AhoCorasickBuilder::new()
            .match_kind(MatchKind::LeftmostLongest)
            .build(dict)
            .map_err(|err| {
                crate::TantivyError::InvalidArgument(format!(
                    "Failed to build Aho-Corasick automaton from dictionary: {err}"
                ))
            })?;

        Ok(Self::from_automaton(dict))
    }

    /// Create a filter from a given automaton.
    ///
    /// The automaton should use one of the leftmost-first match kinds
    /// and it should not be anchored.
    pub fn from_automaton(dict: AhoCorasick) -> Self {
        Self { dict }
    }
}

impl TokenFilter for SplitCompoundWords {
    type Tokenizer<T: Tokenizer> = SplitCompoundWordsFilter<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> SplitCompoundWordsFilter<T> {
        SplitCompoundWordsFilter {
            dict: self.dict,
            inner: tokenizer,
            cuts: Vec::new(),
            parts: Vec::new(),
        }
    }
}

#[derive(Clone)]
pub struct SplitCompoundWordsFilter<T> {
    dict: AhoCorasick,
    inner: T,
    cuts: Vec<usize>,
    parts: Vec<Token>,
}

impl<T: Tokenizer> Tokenizer for SplitCompoundWordsFilter<T> {
    type TokenStream<'a> = SplitCompoundWordsTokenStream<'a, T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        self.cuts.clear();
        self.parts.clear();
        SplitCompoundWordsTokenStream {
            dict: self.dict.clone(),
            tail: self.inner.token_stream(text),
            cuts: &mut self.cuts,
            parts: &mut self.parts,
        }
    }
}

pub struct SplitCompoundWordsTokenStream<'a, T> {
    dict: AhoCorasick,
    tail: T,
    cuts: &'a mut Vec<usize>,
    parts: &'a mut Vec<Token>,
}

impl<T: TokenStream> SplitCompoundWordsTokenStream<'_, T> {
    // Will use `self.cuts` to fill `self.parts` if `self.tail.token()`
    // can fully be split into consecutive matches against `self.dict`.
    fn split(&mut self) {
        let token = self.tail.token();
        let mut text = token.text.as_str();

        self.cuts.clear();
        let mut pos = 0;

        for match_ in self.dict.find_iter(text) {
            if pos != match_.start() {
                break;
            }

            self.cuts.push(pos);
            pos = match_.end();
        }

        if pos == token.text.len() {
            // Fill `self.parts` in reverse order,
            // so that `self.parts.pop()` yields
            // the tokens in their original order.
            for pos in self.cuts.iter().rev() {
                let (head, tail) = text.split_at(*pos);

                text = head;
                self.parts.push(Token {
                    text: tail.to_owned(),
                    ..*token
                });
            }
        }
    }
}

impl<T: TokenStream> TokenStream for SplitCompoundWordsTokenStream<'_, T> {
    fn advance(&mut self) -> bool {
        self.parts.pop();

        if !self.parts.is_empty() {
            return true;
        }

        if !self.tail.advance() {
            return false;
        }

        // Will yield either `self.parts.last()` or
        // `self.tail.token()` if it could not be split.
        self.split();
        true
    }

    fn token(&self) -> &Token {
        self.parts.last().unwrap_or_else(|| self.tail.token())
    }

    fn token_mut(&mut self) -> &mut Token {
        self.parts
            .last_mut()
            .unwrap_or_else(|| self.tail.token_mut())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokenizer::{SimpleTokenizer, TextAnalyzer};

    #[test]
    fn splitting_compound_words_works() {
        let mut tokenizer = TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(SplitCompoundWords::from_dictionary(["foo", "bar"]).unwrap())
            .build();

        {
            let mut stream = tokenizer.token_stream("");
            assert_eq!(stream.next(), None);
        }

        {
            let mut stream = tokenizer.token_stream("foo bar");
            assert_eq!(stream.next().unwrap().text, "foo");
            assert_eq!(stream.next().unwrap().text, "bar");
            assert_eq!(stream.next(), None);
        }

        {
            let mut stream = tokenizer.token_stream("foobar");
            assert_eq!(stream.next().unwrap().text, "foo");
            assert_eq!(stream.next().unwrap().text, "bar");
            assert_eq!(stream.next(), None);
        }

        {
            let mut stream = tokenizer.token_stream("foobarbaz");
            assert_eq!(stream.next().unwrap().text, "foobarbaz");
            assert_eq!(stream.next(), None);
        }

        {
            let mut stream = tokenizer.token_stream("baz foobar qux");
            assert_eq!(stream.next().unwrap().text, "baz");
            assert_eq!(stream.next().unwrap().text, "foo");
            assert_eq!(stream.next().unwrap().text, "bar");
            assert_eq!(stream.next().unwrap().text, "qux");
            assert_eq!(stream.next(), None);
        }

        {
            let mut stream = tokenizer.token_stream("foobar foobar");
            assert_eq!(stream.next().unwrap().text, "foo");
            assert_eq!(stream.next().unwrap().text, "bar");
            assert_eq!(stream.next().unwrap().text, "foo");
            assert_eq!(stream.next().unwrap().text, "bar");
            assert_eq!(stream.next(), None);
        }

        {
            let mut stream = tokenizer.token_stream("foobar foo bar foobar");
            assert_eq!(stream.next().unwrap().text, "foo");
            assert_eq!(stream.next().unwrap().text, "bar");
            assert_eq!(stream.next().unwrap().text, "foo");
            assert_eq!(stream.next().unwrap().text, "bar");
            assert_eq!(stream.next().unwrap().text, "foo");
            assert_eq!(stream.next().unwrap().text, "bar");
            assert_eq!(stream.next(), None);
        }

        {
            let mut stream = tokenizer.token_stream("foobazbar foo bar foobar");
            assert_eq!(stream.next().unwrap().text, "foobazbar");
            assert_eq!(stream.next().unwrap().text, "foo");
            assert_eq!(stream.next().unwrap().text, "bar");
            assert_eq!(stream.next().unwrap().text, "foo");
            assert_eq!(stream.next().unwrap().text, "bar");
            assert_eq!(stream.next(), None);
        }

        {
            let mut stream = tokenizer.token_stream("foobar qux foobar");
            assert_eq!(stream.next().unwrap().text, "foo");
            assert_eq!(stream.next().unwrap().text, "bar");
            assert_eq!(stream.next().unwrap().text, "qux");
            assert_eq!(stream.next().unwrap().text, "foo");
            assert_eq!(stream.next().unwrap().text, "bar");
            assert_eq!(stream.next(), None);
        }

        {
            let mut stream = tokenizer.token_stream("barfoo");
            assert_eq!(stream.next().unwrap().text, "bar");
            assert_eq!(stream.next().unwrap().text, "foo");
            assert_eq!(stream.next(), None);
        }
    }
}
