use super::{Token, TokenStream, Tokenizer};
use crate::TantivyError;

/// Tokenize the text by splitting words into n-grams of the given size(s)
///
/// With this tokenizer, the `position` is always 0.
/// Beware however, in presence of multiple value for the same field,
/// the position will be `POSITION_GAP * index of value`.
///
/// Example 1: `hello` would be tokenized as (min_gram: 2, max_gram: 3, prefix_only: false)
///
/// | Term     | he  | hel | el  | ell | ll  | llo | lo |
/// |----------|-----|-----|-----|-----|-----|-----|----|
/// | Position | 0   | 0   | 0   | 0   | 0   | 0   | 0  |
/// | Offsets  | 0,2 | 0,3 | 1,3 | 1,4 | 2,4 | 2,5 | 3,5|
///
/// Example 2: `hello` would be tokenized as (min_gram: 2, max_gram: 5, prefix_only: **true**)
///
/// | Term     | he  | hel | hell  | hello |
/// |----------|-----|-----|-------|-------|
/// | Position | 0   | 0   | 0     | 0     |
/// | Offsets  | 0,2 | 0,3 | 0,4   | 0,5   |
///
/// Example 3: `hεllo` (non-ascii) would be tokenized as (min_gram: 2, max_gram: 5, prefix_only:
/// **true**)
///
/// | Term     | hε  | hεl | hεll  | hεllo |
/// |----------|-----|-----|-------|-------|
/// | Position | 0   | 0   | 0     | 0     |
/// | Offsets  | 0,3 | 0,4 | 0,5   | 0,6   |
///
/// # Example
///
/// ```rust
/// use tantivy::tokenizer::*;
///
/// let mut tokenizer = NgramTokenizer::new(2, 3, false).unwrap();
/// let mut stream = tokenizer.token_stream("hello");
/// {
///     let token = stream.next().unwrap();
///     assert_eq!(token.text, "he");
///     assert_eq!(token.offset_from, 0);
///     assert_eq!(token.offset_to, 2);
/// }
/// {
///   let token = stream.next().unwrap();
///     assert_eq!(token.text, "hel");
///     assert_eq!(token.offset_from, 0);
///     assert_eq!(token.offset_to, 3);
/// }
/// {
///   let token = stream.next().unwrap();
///     assert_eq!(token.text, "el");
///     assert_eq!(token.offset_from, 1);
///     assert_eq!(token.offset_to, 3);
/// }
/// {
///   let token = stream.next().unwrap();
///     assert_eq!(token.text, "ell");
///     assert_eq!(token.offset_from, 1);
///     assert_eq!(token.offset_to, 4);
/// }
/// {
///   let token = stream.next().unwrap();
///     assert_eq!(token.text, "ll");
///     assert_eq!(token.offset_from, 2);
///     assert_eq!(token.offset_to, 4);
/// }
/// {
///   let token = stream.next().unwrap();
///     assert_eq!(token.text, "llo");
///     assert_eq!(token.offset_from, 2);
///     assert_eq!(token.offset_to, 5);
/// }
/// {
///   let token = stream.next().unwrap();
///   assert_eq!(token.text, "lo");
///   assert_eq!(token.offset_from, 3);
///   assert_eq!(token.offset_to, 5);
/// }
/// assert!(stream.next().is_none());
/// ```
#[derive(Clone, Debug)]
pub struct NgramTokenizer {
    /// min size of the n-gram
    min_gram: usize,
    /// max size of the n-gram
    max_gram: usize,
    /// if true, will only parse the leading edge of the input
    prefix_only: bool,
    token: Token,
}

impl NgramTokenizer {
    /// Configures a new Ngram tokenizer
    pub fn new(
        min_gram: usize,
        max_gram: usize,
        prefix_only: bool,
    ) -> crate::Result<NgramTokenizer> {
        if min_gram == 0 {
            return Err(TantivyError::InvalidArgument(
                "min_gram must be greater than 0".to_string(),
            ));
        }
        if min_gram > max_gram {
            return Err(TantivyError::InvalidArgument(
                "min_gram must not be greater than max_gram".to_string(),
            ));
        }
        Ok(NgramTokenizer {
            min_gram,
            max_gram,
            prefix_only,
            token: Token::default(),
        })
    }

    /// Create a `NGramTokenizer` which generates tokens for all inner ngrams.
    ///
    /// This is as opposed to only prefix ngrams    .
    pub fn all_ngrams(min_gram: usize, max_gram: usize) -> crate::Result<NgramTokenizer> {
        Self::new(min_gram, max_gram, false)
    }

    /// Create a `NGramTokenizer` which only generates tokens for the
    /// prefix ngrams.
    pub fn prefix_only(min_gram: usize, max_gram: usize) -> crate::Result<NgramTokenizer> {
        Self::new(min_gram, max_gram, true)
    }
}

/// TokenStream associate to the `NgramTokenizer`
pub struct NgramTokenStream<'a> {
    /// parameters
    ngram_charidx_iterator: StutteringIterator<CodepointFrontiers<'a>>,
    /// true if the NgramTokenStream is in prefix mode.
    prefix_only: bool,
    /// input
    text: &'a str,
    /// output
    token: &'a mut Token,
}

impl Tokenizer for NgramTokenizer {
    type TokenStream<'a> = NgramTokenStream<'a>;
    fn token_stream<'a>(&'a mut self, text: &'a str) -> NgramTokenStream<'a> {
        self.token.reset();
        NgramTokenStream {
            ngram_charidx_iterator: StutteringIterator::new(
                CodepointFrontiers::for_str(text),
                self.min_gram,
                self.max_gram,
            ),
            prefix_only: self.prefix_only,
            text,
            token: &mut self.token,
        }
    }
}

impl TokenStream for NgramTokenStream<'_> {
    fn advance(&mut self) -> bool {
        if let Some((offset_from, offset_to)) = self.ngram_charidx_iterator.next() {
            if self.prefix_only && offset_from > 0 {
                return false;
            }
            self.token.position = 0;
            self.token.offset_from = offset_from;
            self.token.offset_to = offset_to;
            self.token.text.clear();
            self.token.text.push_str(&self.text[offset_from..offset_to]);
            true
        } else {
            false
        }
    }

    fn token(&self) -> &Token {
        self.token
    }
    fn token_mut(&mut self) -> &mut Token {
        self.token
    }
}

/// This iterator takes an underlying Iterator
/// and emits all of the pairs `(a,b)` such that
/// a and b are items emitted by the iterator at
/// an interval between `min_gram` and `max_gram`.
///
/// The elements are emitted in the order of appearance
/// of `a` first, `b` then.
///
/// See `test_stuttering_iterator` for an example of its
/// output.
struct StutteringIterator<T> {
    underlying: T,
    min_gram: usize,
    max_gram: usize,

    memory: Vec<usize>,
    cursor: usize,
    gram_len: usize,
}

impl<T> StutteringIterator<T>
where T: Iterator<Item = usize>
{
    pub fn new(mut underlying: T, min_gram: usize, max_gram: usize) -> StutteringIterator<T> {
        assert!(min_gram > 0);
        let memory: Vec<usize> = (&mut underlying).take(max_gram + 1).collect();
        if memory.len() <= min_gram {
            // returns an empty iterator
            StutteringIterator {
                underlying,
                min_gram: 1,
                max_gram: 0,
                memory,
                cursor: 0,
                gram_len: 0,
            }
        } else {
            StutteringIterator {
                underlying,
                min_gram,
                max_gram: memory.len() - 1,
                memory,
                cursor: 0,
                gram_len: min_gram,
            }
        }
    }
}

impl<T> Iterator for StutteringIterator<T>
where T: Iterator<Item = usize>
{
    type Item = (usize, usize);

    fn next(&mut self) -> Option<(usize, usize)> {
        if self.gram_len > self.max_gram {
            // we have exhausted all options
            // starting at `self.memory[self.cursor]`.
            //
            // Time to advance.
            self.gram_len = self.min_gram;
            if let Some(next_val) = self.underlying.next() {
                self.memory[self.cursor] = next_val;
            } else {
                self.max_gram -= 1;
            }
            self.cursor += 1;
            if self.cursor >= self.memory.len() {
                self.cursor = 0;
            }
        }
        if self.max_gram < self.min_gram {
            return None;
        }
        let start = self.memory[self.cursor % self.memory.len()];
        let stop = self.memory[(self.cursor + self.gram_len) % self.memory.len()];
        self.gram_len += 1;
        Some((start, stop))
    }
}

/// Emits all of the offsets where a codepoint starts
/// or a codepoint ends.
///
/// By convention, we emit `[0]` for the empty string.
struct CodepointFrontiers<'a> {
    s: &'a str,
    next_el: Option<usize>,
}

impl<'a> CodepointFrontiers<'a> {
    fn for_str(s: &'a str) -> Self {
        CodepointFrontiers {
            s,
            next_el: Some(0),
        }
    }
}

impl Iterator for CodepointFrontiers<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        self.next_el.inspect(|&offset| {
            if self.s.is_empty() {
                self.next_el = None;
            } else {
                let first_codepoint_width = utf8_codepoint_width(self.s.as_bytes()[0]);
                self.s = &self.s[first_codepoint_width..];
                self.next_el = Some(offset + first_codepoint_width);
            }
        })
    }
}

const CODEPOINT_UTF8_WIDTH: [u8; 16] = [1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 4];

// Number of bytes to encode a codepoint in UTF-8 given
// the first byte.
//
// To do that we count the number of higher significant bits set to `1`.
fn utf8_codepoint_width(b: u8) -> usize {
    let higher_4_bits = (b as usize) >> 4;
    CODEPOINT_UTF8_WIDTH[higher_4_bits] as usize
}

#[cfg(test)]
mod tests {

    use super::{utf8_codepoint_width, CodepointFrontiers, NgramTokenizer, StutteringIterator};
    use crate::tokenizer::tests::assert_token;
    use crate::tokenizer::{Token, TokenStream, Tokenizer};

    fn test_helper<T: TokenStream>(mut tokenizer: T) -> Vec<Token> {
        let mut tokens: Vec<Token> = vec![];
        tokenizer.process(&mut |token: &Token| tokens.push(token.clone()));
        tokens
    }

    #[test]
    fn test_utf8_codepoint_width() {
        // 0xxx
        for i in 0..128 {
            assert_eq!(utf8_codepoint_width(i), 1);
        }
        // 110xx
        for i in (128 | 64)..(128 | 64 | 32) {
            assert_eq!(utf8_codepoint_width(i), 2);
        }
        // 1110xx
        for i in (128 | 64 | 32)..(128 | 64 | 32 | 16) {
            assert_eq!(utf8_codepoint_width(i), 3);
        }
        // 1111xx
        for i in (128 | 64 | 32 | 16)..256 {
            assert_eq!(utf8_codepoint_width(i as u8), 4);
        }
    }

    #[test]
    fn test_codepoint_frontiers() {
        assert_eq!(CodepointFrontiers::for_str("").collect::<Vec<_>>(), vec![0]);
        assert_eq!(
            CodepointFrontiers::for_str("abcd").collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4]
        );
        assert_eq!(
            CodepointFrontiers::for_str("aあ").collect::<Vec<_>>(),
            vec![0, 1, 4]
        );
    }

    #[test]
    fn test_ngram_tokenizer_1_2_false() {
        let tokens = test_helper(
            NgramTokenizer::all_ngrams(1, 2)
                .unwrap()
                .token_stream("hello"),
        );
        assert_eq!(tokens.len(), 9);
        assert_token(&tokens[0], 0, "h", 0, 1);
        assert_token(&tokens[1], 0, "he", 0, 2);
        assert_token(&tokens[2], 0, "e", 1, 2);
        assert_token(&tokens[3], 0, "el", 1, 3);
        assert_token(&tokens[4], 0, "l", 2, 3);
        assert_token(&tokens[5], 0, "ll", 2, 4);
        assert_token(&tokens[6], 0, "l", 3, 4);
        assert_token(&tokens[7], 0, "lo", 3, 5);
        assert_token(&tokens[8], 0, "o", 4, 5);
    }

    #[test]
    fn test_ngram_tokenizer_min_max_equal() {
        let tokens = test_helper(
            NgramTokenizer::all_ngrams(3, 3)
                .unwrap()
                .token_stream("hello"),
        );
        assert_eq!(tokens.len(), 3);
        assert_token(&tokens[0], 0, "hel", 0, 3);
        assert_token(&tokens[1], 0, "ell", 1, 4);
        assert_token(&tokens[2], 0, "llo", 2, 5);
    }

    #[test]
    fn test_ngram_tokenizer_2_5_prefix() {
        let tokens = test_helper(
            NgramTokenizer::prefix_only(2, 5)
                .unwrap()
                .token_stream("frankenstein"),
        );
        assert_eq!(tokens.len(), 4);
        assert_token(&tokens[0], 0, "fr", 0, 2);
        assert_token(&tokens[1], 0, "fra", 0, 3);
        assert_token(&tokens[2], 0, "fran", 0, 4);
        assert_token(&tokens[3], 0, "frank", 0, 5);
    }

    #[test]
    fn test_ngram_non_ascii_1_2() {
        let tokens = test_helper(
            NgramTokenizer::all_ngrams(1, 2)
                .unwrap()
                .token_stream("hεllo"),
        );
        assert_eq!(tokens.len(), 9);
        assert_token(&tokens[0], 0, "h", 0, 1);
        assert_token(&tokens[1], 0, "hε", 0, 3);
        assert_token(&tokens[2], 0, "ε", 1, 3);
        assert_token(&tokens[3], 0, "εl", 1, 4);
        assert_token(&tokens[4], 0, "l", 3, 4);
        assert_token(&tokens[5], 0, "ll", 3, 5);
        assert_token(&tokens[6], 0, "l", 4, 5);
        assert_token(&tokens[7], 0, "lo", 4, 6);
        assert_token(&tokens[8], 0, "o", 5, 6);
    }

    #[test]
    fn test_ngram_non_ascii_2_5_prefix() {
        let tokens = test_helper(
            NgramTokenizer::prefix_only(2, 5)
                .unwrap()
                .token_stream("hεllo"),
        );
        assert_eq!(tokens.len(), 4);
        assert_token(&tokens[0], 0, "hε", 0, 3);
        assert_token(&tokens[1], 0, "hεl", 0, 4);
        assert_token(&tokens[2], 0, "hεll", 0, 5);
        assert_token(&tokens[3], 0, "hεllo", 0, 6);
    }

    #[test]
    fn test_ngram_empty() {
        let tokens = test_helper(NgramTokenizer::all_ngrams(1, 5).unwrap().token_stream(""));
        assert!(tokens.is_empty());
        let tokens = test_helper(NgramTokenizer::all_ngrams(2, 5).unwrap().token_stream(""));
        assert!(tokens.is_empty());
    }

    #[test]
    #[should_panic(expected = "min_gram must be greater than 0")]
    fn test_ngram_min_max_interval_empty() {
        test_helper(
            NgramTokenizer::all_ngrams(0, 2)
                .unwrap()
                .token_stream("hellossss"),
        );
    }

    #[test]
    #[should_panic(expected = "min_gram must not be greater than max_gram")]
    fn test_invalid_interval_should_panic_if_smaller() {
        NgramTokenizer::all_ngrams(2, 1).unwrap();
    }

    #[test]
    fn test_stuttering_iterator_empty() {
        let rg: Vec<usize> = vec![0];
        let mut it = StutteringIterator::new(rg.into_iter(), 1, 2);
        assert_eq!(it.next(), None);
    }

    #[test]
    fn test_stuterring_iterator() {
        let mut it = StutteringIterator::new(0..10, 1, 2);
        assert_eq!(it.next(), Some((0, 1)));
        assert_eq!(it.next(), Some((0, 2)));
        assert_eq!(it.next(), Some((1, 2)));
        assert_eq!(it.next(), Some((1, 3)));
        assert_eq!(it.next(), Some((2, 3)));
        assert_eq!(it.next(), Some((2, 4)));
        assert_eq!(it.next(), Some((3, 4)));
        assert_eq!(it.next(), Some((3, 5)));
        assert_eq!(it.next(), Some((4, 5)));
        assert_eq!(it.next(), Some((4, 6)));
        assert_eq!(it.next(), Some((5, 6)));
        assert_eq!(it.next(), Some((5, 7)));
        assert_eq!(it.next(), Some((6, 7)));
        assert_eq!(it.next(), Some((6, 8)));
        assert_eq!(it.next(), Some((7, 8)));
        assert_eq!(it.next(), Some((7, 9)));
        assert_eq!(it.next(), Some((8, 9)));
        assert_eq!(it.next(), None);
    }
}
