//! PostgreSQL SQL statement scanner.
//!
//! Splits psql input into statements with the rules of psql's lexer
//! (`src/fe_utils/psqlscan.l` in PostgreSQL). The tursopg shell and the
//! regression test runner both use it.
//!
//! A semicolon ends a statement when it is outside quoted strings, quoted
//! identifiers and comments, outside parentheses, and, in `CREATE [OR
//! REPLACE] FUNCTION` or `PROCEDURE`, outside a `BEGIN ... END` body. As in
//! psql:
//!
//! - block comments nest, so `/* a /* b */ ; */` is one comment;
//! - a backslash escapes the next character in an `E'...'` string;
//! - a string does not continue on the next line: psql scans each line
//!   without the newline that ends it, so it never sees PostgreSQL's
//!   continuation of a string after a newline, and neither does this;
//! - an identifier can contain `$`, so `a$b$` is not the start of a
//!   dollar-quoted string;
//! - `:name`, `:'name'` and `:"name"` outside quotes are references to psql
//!   variables, and `::` is a cast;
//! - a backslash outside quotes starts a meta-command, which runs to the end
//!   of the line.
//!
//! The input comes in lines. A quoted string or a block comment can start
//! in one line and end in a later one. A `--` comment ends at the end of its
//! line, with or without the newline, as psql scans each line on its own.
//! Any other token ends inside the line where it starts.

/// What the input read so far ends inside, and whether it ends a statement.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Scanner {
    quote: Quote,
    /// Whether a semicolon that ends a statement came after the last token
    /// that is not whitespace or a comment.
    ends_statement: bool,
    paren_depth: usize,
    /// How many `BEGIN`s of a function body are still open, counted only in
    /// `CREATE [OR REPLACE] FUNCTION` or `PROCEDURE`.
    begin_depth: usize,
    /// The first letters of the first four identifiers of the statement, of
    /// those that are `create`, `or`, `replace`, `function` or `procedure`,
    /// and 0 for any other, as psql keeps them.
    identifiers: [u8; 4],
    identifier_count: usize,
}

/// The state of a [`Scanner`] that [`Scanner::lex_state`] saves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LexState {
    paren_depth: usize,
    begin_depth: usize,
    identifiers: [u8; 4],
    identifier_count: usize,
}

/// A part of a piece of the input, in order. The text of the parts of a
/// piece, with `;` for [`Segment::End`], is the text of the piece.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Segment<'a> {
    /// Text of a statement outside quoted strings, quoted identifiers and
    /// comments.
    Code(&'a str),
    /// A quoted string, a quoted identifier or a block comment, or the part
    /// of one in this piece, with its quotes.
    Quoted(&'a str),
    /// A `--` comment, or the part of one in this piece, without the newline
    /// that ends it.
    LineComment(&'a str),
    /// The semicolon that ends a statement.
    End,
    /// A reference to a psql variable, such as `:name`.
    Variable {
        /// The reference as written, such as `:'name'`.
        text: &'a str,
        name: &'a str,
        quoting: VariableQuoting,
    },
    /// A meta-command: a backslash outside quotes and comments, its name
    /// and its arguments. As in psql, the arguments end at a backslash
    /// outside quotes, except for the commands that take the rest of the
    /// line, such as `\copy`.
    MetaCommand(&'a str),
}

/// How a reference to a psql variable quotes its value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VariableQuoting {
    /// `:name`: the value as it is.
    None,
    /// `:'name'`: the value as a string literal.
    Literal,
    /// `:"name"`: the value as a quoted identifier.
    Identifier,
}

/// The quoted string, quoted identifier or comment that the input read so
/// far ends inside.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
enum Quote {
    #[default]
    None,
    /// A string in single quotes. In an `E'...'` string, `escapes` is set
    /// and a backslash escapes the next character; `after_backslash` says
    /// that the input ends right after such a backslash.
    String {
        escapes: bool,
        after_backslash: bool,
    },
    /// The input ends right after the closing quote of a string. Another
    /// quote right after it is a doubled quote, which stands for one quote,
    /// and the string goes on.
    AfterString {
        escapes: bool,
    },
    /// An identifier in double quotes. A doubled quote closes it and opens
    /// it again, which comes to the same.
    Identifier,
    /// A dollar-quoted string that `tag`, such as `$$` or `$body$`, closes.
    /// `matched` is how many characters of the tag the input ends with.
    Dollar {
        tag: Vec<char>,
        matched: usize,
    },
    LineComment,
    /// A block comment, `depth` levels deep. `pending` is the last
    /// character, `/` or `*`, when it can start `/*` or `*/` with the next.
    BlockComment {
        depth: usize,
        pending: Option<char>,
    },
}

type Chars<'a> = std::iter::Peekable<std::str::CharIndices<'a>>;

impl Scanner {
    /// Whether the input read so far ends with a statement: its last token
    /// is a semicolon that ends a statement, followed by nothing but
    /// whitespace and comments.
    pub fn is_complete(&self) -> bool {
        // A line comment that the input ends inside cannot hide a semicolon
        // that is still to come.
        let outside_quote = matches!(
            self.quote,
            Quote::None | Quote::LineComment | Quote::AfterString { .. }
        );
        outside_quote && self.ends_statement
    }

    /// Whether the input read so far ends inside a quoted string, a quoted
    /// identifier or a block comment, as psql's `psql_scan_in_quote` says.
    pub fn in_quote(&self) -> bool {
        !matches!(
            self.quote,
            Quote::None | Quote::LineComment | Quote::AfterString { .. }
        )
    }

    /// What the input read so far leaves open, apart from a quote: the
    /// depth of parentheses and of `BEGIN`, and the first keywords of the
    /// statement.
    pub fn lex_state(&self) -> LexState {
        LexState {
            paren_depth: self.paren_depth,
            begin_depth: self.begin_depth,
            identifiers: self.identifiers,
            identifier_count: self.identifier_count,
        }
    }

    /// Goes back to `state`, as psql does when it discards the text of a
    /// branch of `\if` that does not run.
    pub fn set_lex_state(&mut self, state: LexState) {
        self.paren_depth = state.paren_depth;
        self.begin_depth = state.begin_depth;
        self.identifiers = state.identifiers;
        self.identifier_count = state.identifier_count;
    }

    /// Reads the next piece of the input up to the end of its first
    /// meta-command, and gives `emit` its parts, in order. Returns the byte
    /// where the rest of the piece starts, or None when it read all of it.
    /// psql runs a meta-command before it reads the text after it, which a
    /// meta-command such as `\if` can change the meaning of.
    pub fn scan<'a>(&mut self, piece: &'a str, mut emit: impl FnMut(Segment<'a>)) -> Option<usize> {
        let kind = match self.quote {
            Quote::None | Quote::AfterString { .. } => Kind::Code,
            Quote::LineComment => Kind::LineComment,
            Quote::String { .. }
            | Quote::Identifier
            | Quote::Dollar { .. }
            | Quote::BlockComment { .. } => Kind::Quoted,
        };
        let mut out = Emitter {
            piece,
            start: 0,
            kind,
            emit: &mut emit,
        };
        let mut chars = piece.char_indices().peekable();
        loop {
            if !self.skip_quote(&mut chars, &mut out) {
                break;
            }
            let Some((at, c)) = chars.next() else {
                break;
            };
            if let Read::MetaCommand { resume } = self.token(at, c, &mut chars, &mut out) {
                return (resume < piece.len()).then_some(resume);
            }
        }
        out.flush(piece.len());
        if self.quote == Quote::LineComment {
            self.quote = Quote::None;
        }
        None
    }

    /// Reads the token that starts with `c` at byte `at`, outside any quote.
    fn token(
        &mut self,
        at: usize,
        c: char,
        chars: &mut Chars<'_>,
        out: &mut Emitter<'_, '_>,
    ) -> Read {
        match c {
            c if is_whitespace(c) => {}
            '\'' => self.open(
                at,
                out,
                Quote::String {
                    escapes: false,
                    after_backslash: false,
                },
            ),
            '"' => self.open(at, out, Quote::Identifier),
            '-' if next_char(chars) == Some('-') => {
                chars.next();
                out.switch(at, Kind::LineComment);
                self.quote = Quote::LineComment;
            }
            '/' if next_char(chars) == Some('*') => {
                chars.next();
                out.switch(at, Kind::Quoted);
                self.quote = Quote::BlockComment {
                    depth: 1,
                    pending: None,
                };
            }
            '$' => match read_dollar_tag(chars) {
                Some(tag) => self.open(at, out, Quote::Dollar { tag, matched: 0 }),
                // A parameter such as `$1`, or a `$` that starts no tag.
                None => self.other(),
            },
            '(' => {
                self.paren_depth += 1;
                self.other();
            }
            ')' => {
                self.paren_depth = self.paren_depth.saturating_sub(1);
                self.other();
            }
            ';' if self.paren_depth == 0 && self.begin_depth == 0 => {
                self.ends_statement = true;
                self.identifiers = [0; 4];
                self.identifier_count = 0;
                out.token(at, at + 1, Segment::End);
            }
            '\\' if matches!(next_char(chars), Some(';' | ':')) => {
                // `\;` and `\:` put a `;` or a `:` into the statement, as
                // psql does, and the `;` does not end the statement. At the
                // outer level, the next keywords start a new statement for
                // the BEGIN tracking.
                let (at_char, c) = chars.next().expect("the character was peeked");
                self.other();
                if c == ';' && self.paren_depth == 0 && self.begin_depth == 0 {
                    self.identifiers = [0; 4];
                    self.identifier_count = 0;
                }
                out.token(
                    at,
                    at_char + 1,
                    Segment::Code(&out.piece[at_char..at_char + 1]),
                );
            }
            '\\' => {
                // The meta-command is not part of the statement.
                let (end, resume) = meta_command_end(out.piece, at);
                out.token(at, resume, Segment::MetaCommand(&out.piece[at..end]));
                return Read::MetaCommand { resume };
            }
            ':' if next_char(chars) == Some(':') => {
                chars.next();
                self.other();
            }
            ':' => {
                self.other();
                if let Some((end, name, quoting)) = variable_reference(out.piece, at) {
                    while chars.peek().is_some_and(|&(i, _)| i < end) {
                        chars.next();
                    }
                    let text = &out.piece[at..end];
                    out.token(
                        at,
                        end,
                        Segment::Variable {
                            text,
                            name,
                            quoting,
                        },
                    );
                }
            }
            c if is_identifier_start(c) => {
                let mut end = at + c.len_utf8();
                while let Some(&(i, c)) = chars.peek() {
                    if !is_identifier_continuation(c) {
                        break;
                    }
                    end = i + c.len_utf8();
                    chars.next();
                }
                let word = &out.piece[at..end];
                if word.eq_ignore_ascii_case("e") && next_char(chars) == Some('\'') {
                    let (quote_at, _) = chars.next().expect("the quote was peeked");
                    self.open(
                        quote_at,
                        out,
                        Quote::String {
                            escapes: true,
                            after_backslash: false,
                        },
                    );
                } else {
                    self.identifier(word);
                }
            }
            c if c.is_ascii_digit() => {
                // A number, and an identifier right after it, such as the
                // exponent of `1e5`, is one token, as in psql. A `$` after
                // the digits starts no identifier, so `1$$` is a number and
                // a dollar quote.
                while next_char(chars).is_some_and(|c| c.is_ascii_digit()) {
                    chars.next();
                }
                if next_char(chars).is_some_and(is_identifier_start) {
                    while next_char(chars).is_some_and(is_identifier_continuation) {
                        chars.next();
                    }
                }
                self.other();
            }
            _ => self.other(),
        }
        Read::Token
    }

    /// Starts a quoted string, a quoted identifier or a dollar-quoted string
    /// at byte `at`.
    fn open(&mut self, at: usize, out: &mut Emitter<'_, '_>, quote: Quote) {
        self.other();
        out.switch(at, Kind::Quoted);
        self.quote = quote;
    }

    /// Reads a token that does not end a statement.
    fn other(&mut self) {
        self.ends_statement = false;
    }

    /// Reads an identifier or a keyword. psql counts `BEGIN`, and `CASE`
    /// inside a `BEGIN`, against `END` in a statement that starts with
    /// `CREATE [OR REPLACE] FUNCTION` or `PROCEDURE`, outside parentheses,
    /// so that the semicolons of a `BEGIN ATOMIC` body do not end it.
    fn identifier(&mut self, word: &str) {
        self.other();
        // Keywords inside parentheses do not count, as in psql's
        // psqlscan_track_identifier.
        if self.paren_depth != 0 {
            return;
        }
        let word = word.to_ascii_lowercase();
        if let Some(slot) = self.identifiers.get_mut(self.identifier_count) {
            if matches!(
                word.as_str(),
                "create" | "function" | "procedure" | "or" | "replace"
            ) {
                *slot = word.as_bytes()[0];
            }
        }
        self.identifier_count += 1;

        let ids = self.identifiers;
        let creates_routine = ids[0] == b'c'
            && (ids[1] == b'f'
                || ids[1] == b'p'
                || (ids[1] == b'o' && ids[2] == b'r' && (ids[3] == b'f' || ids[3] == b'p')));
        if !creates_routine {
            return;
        }
        match word.as_str() {
            "begin" => self.begin_depth += 1,
            // CASE also ends with END.
            "case" if self.begin_depth >= 1 => self.begin_depth += 1,
            "end" => self.begin_depth = self.begin_depth.saturating_sub(1),
            _ => {}
        }
    }

    /// Reads to the end of the quoted string, quoted identifier or comment
    /// that the input is inside, if any. Returns false when the input ends
    /// first.
    fn skip_quote(&mut self, chars: &mut Chars<'_>, out: &mut Emitter<'_, '_>) -> bool {
        loop {
            match &mut self.quote {
                Quote::None => return true,
                Quote::AfterString { escapes } => {
                    let escapes = *escapes;
                    match chars.peek() {
                        Some(&(at, '\'')) => {
                            chars.next();
                            out.switch(at, Kind::Quoted);
                            self.quote = Quote::String {
                                escapes,
                                after_backslash: false,
                            };
                        }
                        // The string has ended. The character is read as a
                        // token of its own.
                        Some(_) => {
                            self.quote = Quote::None;
                            return true;
                        }
                        None => return false,
                    }
                }
                Quote::String {
                    escapes,
                    after_backslash,
                } => {
                    let Some((at, c)) = chars.next() else {
                        return false;
                    };
                    if *after_backslash {
                        *after_backslash = false;
                    } else if *escapes && c == '\\' {
                        *after_backslash = true;
                    } else if c == '\'' {
                        out.switch(at + 1, Kind::Code);
                        self.quote = Quote::AfterString { escapes: *escapes };
                    }
                }
                Quote::Identifier => {
                    let Some((at, c)) = chars.next() else {
                        return false;
                    };
                    if c == '"' {
                        out.switch(at + 1, Kind::Code);
                        self.quote = Quote::None;
                    }
                }
                Quote::Dollar { tag, matched } => {
                    let Some((at, c)) = chars.next() else {
                        return false;
                    };
                    // Only the first and the last character of a tag are
                    // `$`, so a mismatch can start a new match only at a `$`.
                    if c == tag[*matched] {
                        *matched += 1;
                    } else {
                        *matched = usize::from(c == '$');
                    }
                    if *matched == tag.len() {
                        out.switch(at + 1, Kind::Code);
                        self.quote = Quote::None;
                    }
                }
                Quote::LineComment => {
                    let Some((at, c)) = chars.next() else {
                        return false;
                    };
                    if c == '\n' {
                        out.switch(at, Kind::Code);
                        self.quote = Quote::None;
                    }
                }
                Quote::BlockComment { depth, pending } => {
                    let Some((at, c)) = chars.next() else {
                        return false;
                    };
                    match (pending.take(), c) {
                        (Some('/'), '*') => *depth += 1,
                        (Some('*'), '/') => {
                            *depth -= 1;
                            if *depth == 0 {
                                out.switch(at + 1, Kind::Code);
                                self.quote = Quote::None;
                            }
                        }
                        (_, '/' | '*') => *pending = Some(c),
                        _ => {}
                    }
                }
            }
        }
    }
}

/// What [`Scanner::token`] read.
#[derive(PartialEq, Eq)]
enum Read {
    Token,
    /// A meta-command, after which the SQL text starts again at byte
    /// `resume`.
    MetaCommand {
        resume: usize,
    },
}

/// The meta-commands whose argument is the rest of the line, backslashes
/// included (OT_WHOLE_LINE in psql's command.c). psql compares `copy` without
/// case.
const WHOLE_LINE_COMMANDS: [&str; 10] = [
    "ef",
    "ev",
    "h",
    "help",
    "sf",
    "sf+",
    "sv",
    "sv+",
    "unrestrict",
    "!",
];

/// Where the meta-command whose backslash is at byte `at` of `piece` ends:
/// the end of its text, and the byte where the SQL text starts again. This
/// follows psqlscanslash.l: the name ends at whitespace or a backslash, an
/// argument ends at whitespace or a backslash outside quotes, and a `\\`
/// after the arguments ends the command and is dropped. The newline that
/// ends the line is not part of the command: psql keeps a newline between
/// the lines of a statement.
fn meta_command_end(piece: &str, at: usize) -> (usize, usize) {
    let line = piece.strip_suffix('\n').unwrap_or(piece);
    let bytes = line.as_bytes();
    let len = bytes.len();
    let mut i = at + 1;
    while i < len && bytes[i] != b'\\' && !is_whitespace_byte(bytes[i]) {
        i += 1;
    }
    let name = &piece[at + 1..i];
    if name.eq_ignore_ascii_case("copy") || WHOLE_LINE_COMMANDS.contains(&name) {
        return (len, len);
    }
    let mut end = i;
    loop {
        while i < len && is_whitespace_byte(bytes[i]) {
            i += 1;
        }
        if i == len {
            return (end, len);
        }
        if bytes[i] == b'\\' {
            let resume = if bytes.get(i + 1) == Some(&b'\\') {
                i + 2
            } else {
                i
            };
            return (end, resume);
        }
        while i < len && bytes[i] != b'\\' && !is_whitespace_byte(bytes[i]) {
            i = match bytes[i] {
                b'\'' => quoted_argument_end(bytes, i, b'\'', true),
                quote @ (b'"' | b'`') => quoted_argument_end(bytes, i, quote, false),
                _ => i + 1,
            };
        }
        end = i;
    }
}

/// The byte after the quoted part of an argument that starts with `quote`
/// at byte `at`, or the end of `bytes` when the quote does not close. In
/// single quotes, a backslash takes the next byte with it, and `''` is a
/// quote, as in psqlscanslash.l.
fn quoted_argument_end(bytes: &[u8], at: usize, quote: u8, escapes: bool) -> usize {
    let mut i = at + 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' if escapes => i += 2,
            c if c == quote => {
                if escapes && bytes.get(i + 1) == Some(&quote) {
                    i += 2;
                } else {
                    return i + 1;
                }
            }
            _ => i += 1,
        }
    }
    bytes.len()
}

/// Whitespace as psql's lexer reads it, for a byte.
fn is_whitespace_byte(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r' | b'\x0c' | b'\x0b')
}

/// The kind of the text that a [`Segment`] holds.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Code,
    Quoted,
    LineComment,
}

/// Gives the parts of a piece to the caller of [`Scanner::scan`]. The text
/// from `start` on is of kind `kind` and not given yet.
struct Emitter<'a, 'e> {
    piece: &'a str,
    start: usize,
    kind: Kind,
    emit: &'e mut dyn FnMut(Segment<'a>),
}

impl<'a> Emitter<'a, '_> {
    /// The text from byte `at` on is of kind `kind`.
    fn switch(&mut self, at: usize, kind: Kind) {
        if kind != self.kind {
            self.flush(at);
            self.kind = kind;
        }
    }

    /// Gives the text up to byte `end`.
    fn flush(&mut self, end: usize) {
        if end > self.start {
            let text = &self.piece[self.start..end];
            (self.emit)(match self.kind {
                Kind::Code => Segment::Code(text),
                Kind::Quoted => Segment::Quoted(text),
                Kind::LineComment => Segment::LineComment(text),
            });
        }
        self.start = end;
    }

    /// Gives `segment`, which is the text from byte `at` to byte `end`.
    fn token(&mut self, at: usize, end: usize, segment: Segment<'a>) {
        self.flush(at);
        (self.emit)(segment);
        self.start = end;
        self.kind = Kind::Code;
    }
}

fn next_char(chars: &mut Chars<'_>) -> Option<char> {
    chars.peek().map(|&(_, c)| c)
}

/// The reference to a psql variable that starts with the `:` at byte `at` of
/// `piece`: its end, its name and its quoting, or None when the `:` starts
/// none. The name is made of letters, digits, `_` and bytes that are not
/// ASCII, as in psql.
fn variable_reference(piece: &str, at: usize) -> Option<(usize, &str, VariableQuoting)> {
    let rest = &piece[at + 1..];
    let (quoting, name_start) = match rest.chars().next()? {
        '\'' => (VariableQuoting::Literal, 1),
        '"' => (VariableQuoting::Identifier, 1),
        _ => (VariableQuoting::None, 0),
    };
    let name_len: usize = rest[name_start..]
        .chars()
        .take_while(|&c| is_variable_char(c))
        .map(char::len_utf8)
        .sum();
    if name_len == 0 {
        return None;
    }
    let name = &rest[name_start..name_start + name_len];
    let mut end = at + 1 + name_start + name_len;
    if quoting != VariableQuoting::None {
        let close = if quoting == VariableQuoting::Literal {
            '\''
        } else {
            '"'
        };
        if !piece[end..].starts_with(close) {
            return None;
        }
        end += 1;
    }
    Some((end, name, quoting))
}

/// Whitespace as PostgreSQL's lexer reads it.
fn is_whitespace(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\r' | '\x0c' | '\x0b')
}

/// A byte that is not ASCII can be part of an identifier, as in psql.
fn is_identifier_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_' || !c.is_ascii()
}

fn is_identifier_continuation(c: char) -> bool {
    is_identifier_start(c) || c.is_ascii_digit() || c == '$'
}

fn is_variable_char(c: char) -> bool {
    is_identifier_start(c) || c.is_ascii_digit()
}

/// Reads the rest of a dollar-quote tag after its first `$`, such as `$` or
/// `body$`, and returns the whole tag. Returns None, and reads nothing more,
/// when the `$` does not start a tag, as in the parameter `$1` or in `$begin`
/// without a closing `$`: psql then reads only the `$`, and `begin` as a
/// token of its own.
fn read_dollar_tag(chars: &mut Chars<'_>) -> Option<Vec<char>> {
    let mut ahead = chars.clone();
    let mut tag = vec!['$'];
    loop {
        match next_char(&mut ahead) {
            Some('$') => {
                ahead.next();
                tag.push('$');
                *chars = ahead;
                return Some(tag);
            }
            Some(c) if is_identifier_start(c) || (tag.len() > 1 && c.is_ascii_digit()) => {
                tag.push(c);
                ahead.next();
            }
            _ => return None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Scanner {
        /// Reads the next piece of the input without looking at its parts.
        fn process(&mut self, piece: &str) {
            scan_all(self, piece, |_| {});
        }
    }

    /// Reads all of `piece`, meta-commands included.
    fn scan_all<'a>(scanner: &mut Scanner, piece: &'a str, mut emit: impl FnMut(Segment<'a>)) {
        let mut from = 0;
        while let Some(resume) = scanner.scan(&piece[from..], &mut emit) {
            from += resume;
        }
    }

    fn is_complete(sql: &str) -> bool {
        let mut state = Scanner::default();
        state.process(sql);
        state.is_complete()
    }

    /// Whether the input is complete after each piece.
    fn is_complete_after_each(pieces: &[&str]) -> Vec<bool> {
        let mut state = Scanner::default();
        pieces
            .iter()
            .map(|piece| {
                state.process(piece);
                state.is_complete()
            })
            .collect()
    }

    #[test]
    fn simple_statements() {
        assert!(is_complete("SELECT 1;"));
        assert!(is_complete("SELECT * FROM foo;"));
        assert!(is_complete("INSERT INTO foo VALUES (1, 2, 3);"));
        assert!(!is_complete("SELECT 1"));
        assert!(!is_complete("SELECT * FROM"));
    }

    #[test]
    fn multiple_statements() {
        assert!(is_complete("SELECT 1; SELECT 2;"));
        assert!(!is_complete("SELECT 1; SELECT 2"));
    }

    #[test]
    fn empty_and_whitespace() {
        assert!(!is_complete(""));
        assert!(!is_complete("   "));
        assert!(!is_complete("\n\t\n"));
        assert!(is_complete(";"));
        assert!(is_complete("  ;  "));
    }

    #[test]
    fn string_with_semicolon() {
        assert!(!is_complete("SELECT ';'"));
        assert!(is_complete("SELECT ';';"));
        assert!(!is_complete("SELECT 'test;test'"));
        assert!(is_complete("SELECT 'test;test';"));
        assert!(!is_complete("SELECT 'unterminated;"));
    }

    #[test]
    fn doubled_quotes() {
        assert!(is_complete("SELECT 'it''s';"));
        assert!(!is_complete("SELECT 'it'';"));
        assert!(is_complete(r#"SELECT "col""name";"#));
        assert!(is_complete(r#"SELECT "column;name" FROM foo;"#));
    }

    #[test]
    fn backslash_escapes_only_in_an_e_string() {
        // A backslash is an ordinary character in a standard string.
        assert!(is_complete(r"SELECT 'a\';"));
        // In an E'' string it escapes the quote, so the string goes on.
        assert!(!is_complete(r"SELECT E'a\';"));
        assert!(is_complete(r"SELECT E'a\';b';"));
        assert!(is_complete(r"SELECT e'a\\';"));
        // An identifier that ends in e is not the prefix of an E'' string.
        assert!(is_complete(r"SELECT some'a\';"));
    }

    #[test]
    fn string_does_not_continue_on_the_next_line() {
        // psql reads `'b\'` on the next line as a string of its own, a
        // standard string in which a backslash escapes nothing.
        assert!(is_complete("SELECT E'a'\n'b\\';"));
        assert_eq!(
            is_complete_after_each(&["SELECT E'a'\n", "'b\\';' AS s;\n"]),
            [false, false]
        );
        assert!(is_complete("SELECT E'a' 'b\\';"));
    }

    #[test]
    fn comments() {
        assert!(is_complete("SELECT 1; -- comment"));
        assert!(!is_complete("SELECT 1 -- comment;"));
        assert!(is_complete("SELECT /* ; */ 1;"));
        assert!(!is_complete("SELECT 1 /* ; */"));
        assert!(is_complete("SELECT 1 --- dashes; \n;"));
    }

    #[test]
    fn block_comments_nest() {
        assert!(!is_complete("SELECT /* a /* b */ ;"));
        assert!(is_complete("SELECT /* a /* b */ ; */ 1;"));
        // `/*/` opens a comment, and its `/` does not close it.
        assert!(!is_complete("SELECT /*/ ;"));
        assert!(is_complete("SELECT /**/ 1;"));
    }

    #[test]
    fn semicolon_in_parentheses() {
        assert!(!is_complete(
            "CREATE RULE r AS ON INSERT TO t DO ALSO (SELECT 1;"
        ));
        assert!(is_complete(
            "CREATE RULE r AS ON INSERT TO t DO ALSO (SELECT 1; SELECT 2);"
        ));
        assert!(is_complete("SELECT (1);"));
        // A `)` too many does not make the depth negative.
        assert!(is_complete("SELECT 1); SELECT 2;"));
    }

    #[test]
    fn begin_atomic_body() {
        let body = "CREATE FUNCTION f() RETURNS int LANGUAGE sql BEGIN ATOMIC SELECT 1; END;";
        assert!(is_complete(body));
        assert!(!is_complete(
            "CREATE OR REPLACE PROCEDURE p() LANGUAGE sql BEGIN ATOMIC SELECT 1;"
        ));
        assert!(!is_complete(
            "CREATE FUNCTION f() RETURNS int LANGUAGE sql BEGIN ATOMIC \
             SELECT CASE WHEN true THEN 1 END; END"
        ));
        assert!(is_complete(
            "CREATE FUNCTION f() RETURNS int LANGUAGE sql BEGIN ATOMIC \
             SELECT CASE WHEN true THEN 1 END; END;"
        ));
        // BEGIN in another statement is a transaction.
        assert!(is_complete("BEGIN;"));
        assert!(is_complete("CREATE TABLE t (a int); BEGIN; SELECT 1;"));
        // CASE outside a BEGIN body is not counted.
        assert!(is_complete(
            "CREATE FUNCTION f() RETURNS int LANGUAGE sql \
             RETURN CASE WHEN true THEN 1 END;"
        ));
    }

    #[test]
    fn trigger_is_an_ordinary_statement() {
        assert!(is_complete(
            "CREATE TRIGGER tg BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION f();"
        ));
        assert!(is_complete(
            "CREATE TRIGGER tg BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION f(); SELECT 1;"
        ));
    }

    #[test]
    fn brackets_and_backticks_do_not_quote() {
        assert!(is_complete("SELECT a[1] FROM t;"));
        assert!(is_complete("SELECT '{1,2}'::int[];"));
        assert!(is_complete("SELECT [x;"));
        assert!(is_complete("SELECT `x;"));
    }

    #[test]
    fn dollar_quoted_string() {
        assert!(is_complete("SELECT $$hello;world$$;"));
        assert!(!is_complete("SELECT $$hello;world$$"));
        assert!(is_complete("SELECT $body$hello;world$body$;"));
        assert!(!is_complete("SELECT $body$hello;world$body$"));
        // A different tag does not close the string.
        assert!(!is_complete("SELECT $body$hello;world$fn$;"));
        assert!(is_complete("SELECT $a$x$$a$;"));
        assert!(!is_complete("SELECT $ab$x$a$;"));
    }

    #[test]
    fn create_function_dollar_quoted() {
        let func = "CREATE FUNCTION add(a int, b int) RETURNS int AS $$\n    \
                    SELECT a + b;\n$$ LANGUAGE sql;";
        assert!(is_complete(func));
        assert!(!is_complete(
            "CREATE FUNCTION add(a int) RETURNS int AS $$\n    SELECT a;\n"
        ));
    }

    #[test]
    fn dollar_sign_that_is_not_a_quote() {
        assert!(is_complete("SELECT $1;"));
        assert!(!is_complete("SELECT $1"));
        assert!(is_complete("SELECT $1, $2;"));
        // An identifier can contain `$`, so `$b$` here is no tag.
        assert!(is_complete("SELECT a$b$ FROM t;"));
        // A number cannot, so `$$` after one starts a dollar quote.
        assert!(!is_complete("SELECT 1$$;"));
        assert!(is_complete("SELECT 1e5;"));
        // A `$` and the start of a tag without its closing `$`.
        assert!(is_complete("SELECT $ab;"));
        // psql reads `begin` after such a `$` as a keyword of its own.
        assert!(!is_complete(
            "CREATE FUNCTION f() RETURNS int LANGUAGE sql $begin ATOMIC SELECT 1;"
        ));
    }

    #[test]
    fn line_comment_ends_with_its_line() {
        // Without the newline too, as the shell may give a line.
        assert_eq!(
            is_complete_after_each(&["SELECT 1 AS a -- c ", ";"]),
            [false, true]
        );
        assert_eq!(
            is_complete_after_each(&["SELECT 1 AS a -- c\n", ";\n"]),
            [false, true]
        );
    }

    #[test]
    fn quote_over_several_pieces() {
        assert_eq!(
            is_complete_after_each(&["SELECT 'a\n", "b;\n", "c';\n"]),
            [false, false, true]
        );
        assert_eq!(
            is_complete_after_each(&["SELECT $$a\n", "b;\n", "c$$;\n"]),
            [false, false, true]
        );
        assert_eq!(
            is_complete_after_each(&["SELECT /* a\n", "b; */\n", "1;\n"]),
            [false, false, true]
        );
        assert_eq!(
            is_complete_after_each(&["SELECT \"a\n", "b;\n", "c\";\n"]),
            [false, false, true]
        );
        assert_eq!(
            is_complete_after_each(&["SELECT E'a\\\n", "\\';\n", "';\n"]),
            [false, false, true]
        );
    }

    #[test]
    fn parentheses_and_body_over_several_pieces() {
        assert_eq!(
            is_complete_after_each(&[
                "CREATE RULE r AS ON INSERT TO t DO ALSO (SELECT 1;\n",
                "SELECT 2);\n"
            ]),
            [false, true]
        );
        assert_eq!(
            is_complete_after_each(&[
                "CREATE FUNCTION f() RETURNS int LANGUAGE sql BEGIN ATOMIC\n",
                "SELECT 1;\n",
                "END;\n",
                "SELECT 2;\n",
            ]),
            [false, false, true, true]
        );
    }

    fn segments(pieces: &[&str]) -> Vec<String> {
        let mut scanner = Scanner::default();
        let mut out = Vec::new();
        for piece in pieces {
            scan_all(&mut scanner, piece, |segment| {
                out.push(match segment {
                    Segment::Code(text) => format!("code {text:?}"),
                    Segment::Quoted(text) => format!("quoted {text:?}"),
                    Segment::LineComment(text) => format!("comment {text:?}"),
                    Segment::End => "end".to_string(),
                    Segment::Variable {
                        text,
                        name,
                        quoting,
                    } => {
                        format!("variable {text} {name} {quoting:?}")
                    }
                    Segment::MetaCommand(text) => format!("meta {text:?}"),
                });
            });
        }
        out
    }

    /// As in psqlscanslash.l, a meta-command's arguments end at a backslash
    /// outside quotes: a `\\` after them is dropped and SQL follows, and
    /// any other backslash starts the next meta-command. The scanner took
    /// the rest of the line as the meta-command.
    #[test]
    fn meta_command_ends_at_a_backslash() {
        assert_eq!(
            segments(&["\\x \\\\ SELECT 1;\n"]),
            [
                "meta \"\\\\x\"",
                "code \" SELECT 1\"",
                "end",
                "code \"\\n\""
            ]
        );
        assert_eq!(
            segments(&["\\echo hi \\echo there\n"]),
            [
                "meta \"\\\\echo hi\"",
                "meta \"\\\\echo there\"",
                "code \"\\n\""
            ]
        );
        // A backslash inside quotes does not end the argument.
        assert_eq!(
            segments(&["\\echo 'a\\\\b' \"c\\d\" `e\\f`\\\\;\n"]),
            [
                "meta \"\\\\echo 'a\\\\\\\\b' \\\"c\\\\d\\\" `e\\\\f`\"",
                "end",
                "code \"\\n\""
            ]
        );
        // `\copy` and the other commands that take the whole line keep their
        // backslashes.
        assert_eq!(
            segments(&["\\copy t from stdin \\\\ x\n", "\\! echo \\x\n"]),
            [
                "meta \"\\\\copy t from stdin \\\\\\\\ x\"",
                "code \"\\n\"",
                "meta \"\\\\! echo \\\\x\"",
                "code \"\\n\""
            ]
        );
    }

    /// The newline at the end of a meta-command's line is SQL text, so a
    /// statement that goes on after the meta-command keeps its line break,
    /// as in psql. The meta-command took the newline with it.
    #[test]
    fn newline_after_a_meta_command_is_sql_text() {
        assert_eq!(
            segments(&["SELECT 7 AS v\\echo hi\n", "WHERE true;\n"]),
            [
                "code \"SELECT 7 AS v\"",
                "meta \"\\\\echo hi\"",
                "code \"\\n\"",
                "code \"WHERE true\"",
                "end",
                "code \"\\n\""
            ]
        );
        // psql reads the arguments of `\?` as it reads those of most
        // commands, so `\\` ends it.
        assert_eq!(
            segments(&["\\? variables \\\\ SELECT 1;\n"]),
            [
                "meta \"\\\\? variables\"",
                "code \" SELECT 1\"",
                "end",
                "code \"\\n\""
            ]
        );
    }

    #[test]
    fn segments_of_a_piece() {
        assert_eq!(
            segments(&["SELECT 'a;', \"b\" /* c */ -- d\n"]),
            [
                "code \"SELECT \"",
                "quoted \"'a;'\"",
                "code \", \"",
                "quoted \"\\\"b\\\"\"",
                "code \" \"",
                "quoted \"/* c */\"",
                "code \" \"",
                "comment \"-- d\"",
                "code \"\\n\"",
            ]
        );
        assert_eq!(
            segments(&["SELECT 1; SELECT 2;\n"]),
            [
                "code \"SELECT 1\"",
                "end",
                "code \" SELECT 2\"",
                "end",
                "code \"\\n\"",
            ]
        );
    }

    #[test]
    fn segments_of_a_quote_over_several_pieces() {
        assert_eq!(
            segments(&["SELECT $$a;\n", "b$$ AS x; -- c\n"]),
            [
                "code \"SELECT \"",
                "quoted \"$$a;\\n\"",
                "quoted \"b$$\"",
                "code \" AS x\"",
                "end",
                "code \" \"",
                "comment \"-- c\"",
                "code \"\\n\"",
            ]
        );
        assert_eq!(
            segments(&["SELECT 'it''s'\n", ";\n"]),
            [
                "code \"SELECT \"",
                "quoted \"'it'\"",
                "quoted \"'s'\"",
                "code \"\\n\"",
                "end",
                "code \"\\n\"",
            ]
        );
    }

    #[test]
    fn variable_references() {
        assert_eq!(
            segments(&["SELECT :a, :'b', :\"c\", 1::int, ':d';\n"]),
            [
                "code \"SELECT \"",
                "variable :a a None",
                "code \", \"",
                "variable :'b' b Literal",
                "code \", \"",
                "variable :\"c\" c Identifier",
                "code \", 1::int, \"",
                "quoted \"':d'\"",
                "end",
                "code \"\\n\"",
            ]
        );
        // `:'` without a name and its closing quote starts no reference,
        // and the quote then starts a string.
        assert!(!is_complete("SELECT :'a b;"));
        assert!(is_complete("SELECT :'a b';"));
        assert!(is_complete("SELECT :'ab';"));
    }

    #[test]
    fn backslash_semicolon_and_colon_go_into_the_statement() {
        assert_eq!(
            segments(&["SELECT 1 \\; SELECT 2;\n"]),
            [
                "code \"SELECT 1 \"",
                "code \";\"",
                "code \" SELECT 2\"",
                "end",
                "code \"\\n\"",
            ]
        );
        assert!(!is_complete("SELECT 1 \\;"));
        assert!(is_complete("SELECT 1 \\; SELECT 2;"));
        assert_eq!(
            segments(&["SELECT 1\\:\\:int;\n"]),
            [
                "code \"SELECT 1\"",
                "code \":\"",
                "code \":\"",
                "code \"int\"",
                "end",
                "code \"\\n\"",
            ]
        );
        // After `\;`, CREATE FUNCTION starts a new statement for the BEGIN
        // tracking.
        assert!(!is_complete(
            "SELECT 1 \\; CREATE FUNCTION f() RETURNS int LANGUAGE sql BEGIN ATOMIC SELECT 1;"
        ));
    }

    #[test]
    fn keywords_inside_parentheses_do_not_count() {
        // psql 18.7 ends this at `;`: `create function` inside the
        // parentheses is no CREATE FUNCTION.
        assert!(is_complete("(create function) begin;"));
    }

    #[test]
    fn meta_command_takes_its_arguments() {
        // `;` is an argument of the meta-command, not the end of a statement.
        assert_eq!(
            segments(&["SELECT 1 AS x \\gset ;\n"]),
            [
                "code \"SELECT 1 AS x \"",
                "meta \"\\\\gset ;\"",
                "code \"\\n\""
            ]
        );
        assert!(!is_complete("SELECT 1 \\gset ;\n"));
        // A backslash in a string starts no meta-command.
        assert!(is_complete("SELECT '\\';"));
        assert!(is_complete("SELECT 1; \\echo ;\n"));
    }

    #[test]
    fn in_quote_follows_psql() {
        let in_quote = |sql: &str| {
            let mut scanner = Scanner::default();
            scanner.process(sql);
            scanner.in_quote()
        };
        assert!(in_quote("SELECT 'a\n"));
        assert!(in_quote("SELECT $$a\n"));
        assert!(in_quote("SELECT \"a\n"));
        assert!(in_quote("SELECT /* a\n"));
        assert!(!in_quote("SELECT 'a'\n"));
        assert!(!in_quote("SELECT 1 -- a"));
    }
}
