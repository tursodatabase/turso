use turso_core::{LimboError, Result};

const LARGEST_EXPONENT: i64 = 1_073_741_823;
const MAX_DIGITS_BEFORE_DECIMAL_POINT: i64 = 131_072;
const MAX_DIGITS_AFTER_DECIMAL_POINT: i64 = 16_383;

pub(crate) struct Jsonb {
    values: Vec<Value>,
}

enum Value {
    Null,
    Bool(bool),
    Number(Numeric),
    String(String),
    Array(Vec<usize>),
    Object(Vec<(String, usize)>),
}

impl Jsonb {
    pub(crate) fn parse(text: &str) -> Result<Self> {
        let mut parser = Parser::new(text)?;
        parser.parse()?;
        Ok(Self {
            values: parser.values,
        })
    }

    pub(crate) fn object_field_text(&self, name: &str) -> Option<String> {
        let Value::Object(fields) = &self.values[self.root()] else {
            return None;
        };
        let (_, field) = fields.iter().find(|(field_name, _)| field_name == name)?;
        match &self.values[*field] {
            Value::Null => None,
            Value::String(text) => Some(text.clone()),
            _ => Some(self.value_text(*field)),
        }
    }

    pub(crate) fn text(&self) -> String {
        self.value_text(self.root())
    }

    fn root(&self) -> usize {
        self.values.len() - 1
    }

    fn value_text(&self, value: usize) -> String {
        let mut text = String::new();
        let mut pieces = vec![Piece::Value(value)];
        while let Some(piece) = pieces.pop() {
            match piece {
                Piece::Punctuation(punctuation) => text.push_str(punctuation),
                Piece::FieldName(name) => {
                    write_string(&mut text, name);
                    text.push_str(": ");
                }
                Piece::Value(value) => match &self.values[value] {
                    Value::Null => text.push_str("null"),
                    Value::Bool(true) => text.push_str("true"),
                    Value::Bool(false) => text.push_str("false"),
                    Value::Number(number) => number.write(&mut text),
                    Value::String(string) => write_string(&mut text, string),
                    Value::Array(items) => {
                        text.push('[');
                        pieces.push(Piece::Punctuation("]"));
                        for (position, item) in items.iter().enumerate().rev() {
                            pieces.push(Piece::Value(*item));
                            if position > 0 {
                                pieces.push(Piece::Punctuation(", "));
                            }
                        }
                    }
                    Value::Object(fields) => {
                        text.push('{');
                        pieces.push(Piece::Punctuation("}"));
                        for (position, (name, value)) in fields.iter().enumerate().rev() {
                            pieces.push(Piece::Value(*value));
                            pieces.push(Piece::FieldName(name));
                            if position > 0 {
                                pieces.push(Piece::Punctuation(", "));
                            }
                        }
                    }
                },
            }
        }
        text
    }
}

enum Piece<'a> {
    Punctuation(&'static str),
    FieldName(&'a str),
    Value(usize),
}

fn write_string(text: &mut String, string: &str) {
    text.push('"');
    for character in string.chars() {
        match character {
            '"' => text.push_str("\\\""),
            '\\' => text.push_str("\\\\"),
            '\u{8}' => text.push_str("\\b"),
            '\u{c}' => text.push_str("\\f"),
            '\n' => text.push_str("\\n"),
            '\r' => text.push_str("\\r"),
            '\t' => text.push_str("\\t"),
            '\0'..='\u{1f}' => text.push_str(&format!("\\u{:04x}", u32::from(character))),
            _ => text.push(character),
        }
    }
    text.push('"');
}

struct Parser<'a> {
    lexer: Lexer<'a>,
    token: Token<'a>,
    values: Vec<Value>,
    open_containers: Vec<OpenContainer>,
}

enum OpenContainer {
    Array(Vec<usize>),
    Object {
        fields: Vec<(String, usize)>,
        next_field_name: String,
    },
}

impl<'a> Parser<'a> {
    fn new(text: &'a str) -> Result<Self> {
        let mut lexer = Lexer { text, position: 0 };
        let token = lexer.next_token()?;
        Ok(Self {
            lexer,
            token,
            values: Vec::new(),
            open_containers: Vec::new(),
        })
    }

    fn parse(&mut self) -> Result<()> {
        let mut finished_value = None;
        loop {
            finished_value = match finished_value {
                None => self.start_value()?,
                Some(value) => match self.open_containers.pop() {
                    None => return self.expect_end(),
                    Some(container) => self.continue_container(container, value)?,
                },
            };
        }
    }

    fn start_value(&mut self) -> Result<Option<usize>> {
        let scalar = match &mut self.token {
            Token::LeftBracket => return self.start_array(),
            Token::LeftBrace => return self.start_object(),
            Token::Scalar(scalar) => std::mem::replace(scalar, Scalar::Null),
            Token::RightBracket | Token::RightBrace | Token::Comma | Token::Colon | Token::End => {
                return Err(invalid_json());
            }
        };
        self.advance()?;
        let value = match scalar {
            Scalar::Null => Value::Null,
            Scalar::Bool(value) => Value::Bool(value),
            Scalar::Number(text) => Value::Number(Numeric::parse(text)?),
            Scalar::String(text) => Value::String(text),
        };
        Ok(Some(self.push(value)))
    }

    fn start_array(&mut self) -> Result<Option<usize>> {
        self.advance()?;
        if matches!(self.token, Token::RightBracket) {
            self.advance()?;
            return Ok(Some(self.push(Value::Array(Vec::new()))));
        }
        self.open_containers.push(OpenContainer::Array(Vec::new()));
        Ok(None)
    }

    fn start_object(&mut self) -> Result<Option<usize>> {
        self.advance()?;
        if matches!(self.token, Token::RightBrace) {
            self.advance()?;
            return Ok(Some(self.push(Value::Object(Vec::new()))));
        }
        let next_field_name = self.field_name()?;
        self.open_containers.push(OpenContainer::Object {
            fields: Vec::new(),
            next_field_name,
        });
        Ok(None)
    }

    fn continue_container(
        &mut self,
        container: OpenContainer,
        value: usize,
    ) -> Result<Option<usize>> {
        match container {
            OpenContainer::Array(mut items) => {
                items.push(value);
                match self.token {
                    Token::Comma => {
                        self.advance()?;
                        self.open_containers.push(OpenContainer::Array(items));
                        Ok(None)
                    }
                    Token::RightBracket => {
                        self.advance()?;
                        Ok(Some(self.push(Value::Array(items))))
                    }
                    _ => Err(invalid_json()),
                }
            }
            OpenContainer::Object {
                mut fields,
                next_field_name,
            } => {
                fields.push((next_field_name, value));
                match self.token {
                    Token::Comma => {
                        self.advance()?;
                        let next_field_name = self.field_name()?;
                        self.open_containers.push(OpenContainer::Object {
                            fields,
                            next_field_name,
                        });
                        Ok(None)
                    }
                    Token::RightBrace => {
                        self.advance()?;
                        let fields = sort_and_remove_duplicate_names(fields);
                        Ok(Some(self.push(Value::Object(fields))))
                    }
                    _ => Err(invalid_json()),
                }
            }
        }
    }

    fn field_name(&mut self) -> Result<String> {
        let Token::Scalar(Scalar::String(name)) = &mut self.token else {
            return Err(invalid_json());
        };
        let name = std::mem::take(name);
        self.advance()?;
        if !matches!(self.token, Token::Colon) {
            return Err(invalid_json());
        }
        self.advance()?;
        Ok(name)
    }

    fn expect_end(&self) -> Result<()> {
        match self.token {
            Token::End => Ok(()),
            _ => Err(invalid_json()),
        }
    }

    fn advance(&mut self) -> Result<()> {
        self.token = self.lexer.next_token()?;
        Ok(())
    }

    fn push(&mut self, value: Value) -> usize {
        self.values.push(value);
        self.values.len() - 1
    }
}

fn sort_and_remove_duplicate_names(mut fields: Vec<(String, usize)>) -> Vec<(String, usize)> {
    fields.reverse();
    fields.sort_by(|(left, _), (right, _)| {
        left.len().cmp(&right.len()).then_with(|| left.cmp(right))
    });
    fields.dedup_by(|(name, _), (kept_name, _)| name == kept_name);
    fields
}

struct Lexer<'a> {
    text: &'a str,
    position: usize,
}

enum Token<'a> {
    LeftBracket,
    RightBracket,
    LeftBrace,
    RightBrace,
    Comma,
    Colon,
    Scalar(Scalar<'a>),
    End,
}

enum Scalar<'a> {
    Null,
    Bool(bool),
    Number(&'a str),
    String(String),
}

impl<'a> Lexer<'a> {
    fn next_token(&mut self) -> Result<Token<'a>> {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.position += 1;
        }
        let punctuation = match self.peek() {
            None => return Ok(Token::End),
            Some(b'[') => Token::LeftBracket,
            Some(b']') => Token::RightBracket,
            Some(b'{') => Token::LeftBrace,
            Some(b'}') => Token::RightBrace,
            Some(b',') => Token::Comma,
            Some(b':') => Token::Colon,
            Some(b'"') => return Ok(Token::Scalar(Scalar::String(self.string()?))),
            Some(b'-' | b'0'..=b'9') => return Ok(Token::Scalar(Scalar::Number(self.number()?))),
            Some(_) => return Ok(Token::Scalar(self.keyword()?)),
        };
        self.position += 1;
        Ok(punctuation)
    }

    fn string(&mut self) -> Result<String> {
        let mut string = String::new();
        let mut high_surrogate = None;
        self.position += 1;
        loop {
            match self.peek() {
                None | Some(0x00..=0x1f) => return Err(invalid_json()),
                Some(b'"') => break,
                Some(b'\\') if self.byte_at(self.position + 1) == Some(b'u') => {
                    self.position += 2;
                    let mut code = self.hexadecimal_code()?;
                    match (code, high_surrogate.take()) {
                        (0xd800..=0xdbff, None) => {
                            high_surrogate = Some(code);
                            continue;
                        }
                        (0xdc00..=0xdfff, Some(high)) => {
                            code = 0x10000 + ((high - 0xd800) << 10) + (code - 0xdc00);
                        }
                        (0xd800..=0xdfff, _) | (_, Some(_)) => return Err(invalid_json()),
                        (_, None) => {}
                    }
                    if code == 0 {
                        return Err(LimboError::InvalidArgument(
                            "unsupported Unicode escape sequence".to_string(),
                        ));
                    }
                    string
                        .push(char::from_u32(code).expect("a code point that is not a surrogate"));
                }
                Some(b'\\') => {
                    if high_surrogate.is_some() {
                        return Err(invalid_json());
                    }
                    string.push(match self.byte_at(self.position + 1) {
                        Some(b'"') => '"',
                        Some(b'\\') => '\\',
                        Some(b'/') => '/',
                        Some(b'b') => '\u{8}',
                        Some(b'f') => '\u{c}',
                        Some(b'n') => '\n',
                        Some(b'r') => '\r',
                        Some(b't') => '\t',
                        _ => return Err(invalid_json()),
                    });
                    self.position += 2;
                }
                Some(_) => {
                    if high_surrogate.is_some() {
                        return Err(invalid_json());
                    }
                    let start = self.position;
                    while self
                        .peek()
                        .is_some_and(|byte| !matches!(byte, b'"' | b'\\' | 0x00..=0x1f))
                    {
                        self.position += 1;
                    }
                    string.push_str(&self.text[start..self.position]);
                }
            }
        }
        if high_surrogate.is_some() {
            return Err(invalid_json());
        }
        self.position += 1;
        Ok(string)
    }

    fn hexadecimal_code(&mut self) -> Result<u32> {
        let digits = self
            .text
            .as_bytes()
            .get(self.position..self.position + 4)
            .filter(|digits| digits.iter().all(u8::is_ascii_hexdigit))
            .ok_or_else(invalid_json)?;
        let code = digits.iter().fold(0, |code, digit| {
            code * 16
                + char::from(*digit)
                    .to_digit(16)
                    .expect("a hexadecimal digit")
        });
        self.position += 4;
        Ok(code)
    }

    fn number(&mut self) -> Result<&'a str> {
        let start = self.position;
        self.skip_one_of(b"-");
        match self.peek() {
            Some(b'0') => self.position += 1,
            Some(b'1'..=b'9') => {
                self.skip_digits();
            }
            _ => return Err(invalid_json()),
        }
        if self.skip_one_of(b".") && self.skip_digits() == 0 {
            return Err(invalid_json());
        }
        if self.skip_one_of(b"eE") {
            self.skip_one_of(b"+-");
            if self.skip_digits() == 0 {
                return Err(invalid_json());
            }
        }
        if self.peek().is_some_and(is_alphanumeric) {
            return Err(invalid_json());
        }
        let text = self.text;
        Ok(&text[start..self.position])
    }

    fn keyword(&mut self) -> Result<Scalar<'a>> {
        let start = self.position;
        while self.peek().is_some_and(is_alphanumeric) {
            self.position += 1;
        }
        match &self.text[start..self.position] {
            "null" => Ok(Scalar::Null),
            "true" => Ok(Scalar::Bool(true)),
            "false" => Ok(Scalar::Bool(false)),
            _ => Err(invalid_json()),
        }
    }

    fn skip_one_of(&mut self, bytes: &[u8]) -> bool {
        let found = self.peek().is_some_and(|byte| bytes.contains(&byte));
        if found {
            self.position += 1;
        }
        found
    }

    fn skip_digits(&mut self) -> usize {
        let start = self.position;
        while self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
            self.position += 1;
        }
        self.position - start
    }

    fn peek(&self) -> Option<u8> {
        self.byte_at(self.position)
    }

    fn byte_at(&self, position: usize) -> Option<u8> {
        self.text.as_bytes().get(position).copied()
    }
}

fn is_alphanumeric(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || !byte.is_ascii()
}

struct Numeric {
    negative: bool,
    significant_digits: String,
    leading_digit_power: i64,
    scale: i64,
}

impl Numeric {
    fn parse(text: &str) -> Result<Self> {
        let (negative, unsigned) = match text.strip_prefix('-') {
            Some(unsigned) => (true, unsigned),
            None => (false, text),
        };
        let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
            Some((mantissa, exponent)) => (mantissa, parse_exponent(exponent)?),
            None => (unsigned, 0),
        };
        let (integer_digits, fraction_digits) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        let digits = format!("{integer_digits}{fraction_digits}");
        let significant_digits = digits.trim_start_matches('0');
        let leading_zeros = (digits.len() - significant_digits.len()) as i64;
        let leading_digit_power = if significant_digits.is_empty() {
            0
        } else {
            integer_digits.len() as i64 - 1 + exponent - leading_zeros
        };
        let scale = (fraction_digits.len() as i64 - exponent).max(0);
        if leading_digit_power >= MAX_DIGITS_BEFORE_DECIMAL_POINT
            || scale > MAX_DIGITS_AFTER_DECIMAL_POINT
        {
            return Err(numeric_overflow());
        }
        Ok(Self {
            negative: negative && !significant_digits.is_empty(),
            significant_digits: significant_digits.to_string(),
            leading_digit_power,
            scale,
        })
    }

    fn write(&self, text: &mut String) {
        if self.negative {
            text.push('-');
        }
        if self.leading_digit_power < 0 {
            text.push('0');
        } else {
            text.extend(
                (0..=self.leading_digit_power)
                    .rev()
                    .map(|power| self.digit(power)),
            );
        }
        if self.scale > 0 {
            text.push('.');
            text.extend((1..=self.scale).map(|position| self.digit(-position)));
        }
    }

    fn digit(&self, power: i64) -> char {
        usize::try_from(self.leading_digit_power - power)
            .ok()
            .and_then(|index| self.significant_digits.as_bytes().get(index))
            .map_or('0', |digit| char::from(*digit))
    }
}

fn parse_exponent(text: &str) -> Result<i64> {
    let (negative, digits) = match text.strip_prefix('-') {
        Some(digits) => (true, digits),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let mut exponent = 0;
    for digit in digits.bytes() {
        exponent = exponent * 10 + i64::from(digit - b'0');
        if exponent > LARGEST_EXPONENT {
            return Err(numeric_overflow());
        }
    }
    Ok(if negative { -exponent } else { exponent })
}

fn invalid_json() -> LimboError {
    LimboError::InvalidArgument("invalid input syntax for type json".to_string())
}

fn numeric_overflow() -> LimboError {
    LimboError::InvalidArgument("value overflows numeric format".to_string())
}
