use super::{Token, TokenStream, Tokenizer};

/// For each value of the field, emit a single unprocessed token.
#[derive(Clone, Default)]
pub struct RawTokenizer {
    token: Token,
}

pub struct RawTokenStream<'a> {
    token: &'a mut Token,
    has_token: bool,
}

impl Tokenizer for RawTokenizer {
    type TokenStream<'a> = RawTokenStream<'a>;
    fn token_stream<'a>(&'a mut self, text: &str) -> RawTokenStream<'a> {
        self.token.reset();
        self.token.position = 0;
        self.token.position_length = 1;
        self.token.offset_from = 0;
        self.token.offset_to = text.len();
        self.token.text.clear();
        self.token.text.push_str(text);
        RawTokenStream {
            token: &mut self.token,
            has_token: true,
        }
    }
}

impl TokenStream for RawTokenStream<'_> {
    fn advance(&mut self) -> bool {
        let result = self.has_token;
        self.has_token = false;
        result
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
    use crate::tokenizer::tests::assert_token;
    use crate::tokenizer::{RawTokenizer, TextAnalyzer, Token};

    #[test]
    fn test_raw_tokenizer() {
        let tokens = token_stream_helper("Hello, happy tax payer!");
        assert_eq!(tokens.len(), 1);
        assert_token(&tokens[0], 0, "Hello, happy tax payer!", 0, 23);
    }

    fn token_stream_helper(text: &str) -> Vec<Token> {
        let mut a = TextAnalyzer::from(RawTokenizer::default());
        let mut token_stream = a.token_stream(text);
        let mut tokens: Vec<Token> = vec![];
        let mut add_token = |token: &Token| {
            tokens.push(token.clone());
        };
        token_stream.process(&mut add_token);
        tokens
    }
}
