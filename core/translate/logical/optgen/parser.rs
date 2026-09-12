//! Parse rule files into a syntax tree.
//!
//! The grammar, in the notation of the CockroachDB Optgen documentation:
//!
//! ```text
//! root         = tags (define | rule)
//! tags         = '[' IDENT (',' IDENT)* ']'
//! define       = 'define' define-name '{' define-field* '}'
//! define-field = field-name field-type
//! rule         = func '=>' replace
//! replace      = func | ref
//! func         = '(' func-name arg* ')'
//! func-name    = names | func
//! names        = name ('|' name)*
//! arg          = bind and | ref | and
//! and          = expr ('&' and)
//! expr         = func | not | let | list | any | name | STRING | NUMBER
//! not          = '^' expr
//! list         = '[' list-child* ']'
//! list-child   = list-any | arg
//! list-any     = '...'
//! bind         = '$' label ':' and
//! let          = '(' 'Let' '(' '$' label ('$' label)* ')' ':' func ref ')'
//! ref          = '$' label
//! any          = '*'
//! ```

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use super::compiler::DataType;
use super::scanner::{Scanner, Token};

const LET_KEYWORD: &str = "Let";

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub(crate) struct SourceLoc {
    /// Shared by every location of one file, so a location is cheap to
    /// clone.
    pub file: Arc<str>,
    pub line: usize,
    pub pos: usize,
}

impl Display for SourceLoc {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}:{}", self.file, self.line + 1, self.pos + 1)
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Root {
    pub defines: Vec<Define>,
    pub rules: Vec<Rule>,
}

#[derive(Clone, Debug)]
pub(crate) struct Define {
    pub comments: Vec<String>,
    pub tags: Vec<String>,
    pub name: String,
    pub fields: Vec<DefineField>,
    pub src: SourceLoc,
}

impl Define {
    pub fn has_tag(&self, tag: &str) -> bool {
        self.tags.iter().any(|known| known == tag)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DefineField {
    pub comments: Vec<String>,
    pub name: String,
    pub type_name: String,
    pub src: SourceLoc,
}

#[derive(Clone, Debug)]
pub(crate) struct Rule {
    pub comments: Vec<String>,
    pub name: String,
    pub tags: Vec<String>,
    /// Always an `ExprKind::Func`.
    pub match_pattern: Expr,
    pub replace: Expr,
    pub src: SourceLoc,
}

impl Rule {
    pub fn has_tag(&self, tag: &str) -> bool {
        self.tags.iter().any(|known| known == tag)
    }
}

#[derive(Clone, Debug)]
pub(crate) enum FuncName {
    /// One name, or several names separated by `|`.
    Names(Vec<String>),
    /// A name chosen at run time, such as `(OpName $input)`.
    Dynamic(Box<Expr>),
}

#[derive(Clone, Debug)]
pub(crate) enum ExprKind {
    Func {
        name: FuncName,
        args: Vec<Expr>,
    },
    /// A call of a function written in Rust. The compiler turns a `Func`
    /// whose name is not an operator into this.
    CustomFunc {
        name: String,
        args: Vec<Expr>,
    },
    Name(String),
    And(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    List(Vec<Expr>),
    ListAny,
    Bind {
        label: String,
        target: Box<Expr>,
    },
    Let {
        labels: Vec<String>,
        target: Box<Expr>,
        result: String,
    },
    Ref(String),
    Any,
    Str(String),
    Number(i64),
}

#[derive(Clone, Debug)]
pub(crate) struct Expr {
    pub kind: ExprKind,
    pub src: SourceLoc,
    pub typ: DataType,
}

impl Expr {
    pub fn new(kind: ExprKind, src: SourceLoc) -> Self {
        Self {
            kind,
            src,
            typ: DataType::Any,
        }
    }

    /// The names a function can have. Empty for a dynamic name.
    pub fn name_choice(&self) -> &[String] {
        match &self.kind {
            ExprKind::Func {
                name: FuncName::Names(names),
                ..
            } => names,
            _ => &[],
        }
    }

    /// The name of a function with exactly one name.
    pub fn single_name(&self) -> &str {
        let names = self.name_choice();
        assert!(
            names.len() == 1,
            "the function must have exactly one name, it has {}",
            names.len()
        );
        &names[0]
    }
}

pub(crate) struct Parser<'a> {
    files: Vec<(&'a str, &'a str)>,
    file: usize,
    scanner: Scanner<'a>,
    src: SourceLoc,
    save_src: SourceLoc,
    errors: Vec<String>,
    comments: Vec<String>,
    unscanned: bool,
}

impl<'a> Parser<'a> {
    /// `files` holds `(name, source)` pairs. The files are parsed as one
    /// token stream, in order.
    pub fn new(files: &[(&'a str, &'a str)]) -> Self {
        let (name, source) = files.first().copied().unwrap_or(("", ""));
        Self {
            files: files.to_vec(),
            file: 0,
            scanner: Scanner::new(source),
            src: SourceLoc {
                file: Arc::from(name),
                ..SourceLoc::default()
            },
            save_src: SourceLoc::default(),
            errors: Vec::new(),
            comments: Vec::new(),
            unscanned: false,
        }
    }

    pub fn parse(mut self) -> Result<Root, Vec<String>> {
        let root = self.parse_root();
        if self.errors.is_empty() {
            Ok(root)
        } else {
            Err(self.errors)
        }
    }

    fn parse_root(&mut self) -> Root {
        let mut root = Root::default();
        loop {
            let token = self.scan();
            let src = self.src.clone();
            match token {
                Token::Eof => return root,
                Token::LBracket => {
                    self.unscan();
                    let comments = self.take_comments();
                    let Some(tags) = self.parse_tags() else {
                        self.try_recover();
                        continue;
                    };
                    if self.scan() != Token::Ident {
                        self.unscan();
                        match self.parse_rule(comments, tags, src) {
                            Some(rule) => root.rules.push(rule),
                            None => self.try_recover(),
                        }
                        continue;
                    }
                    if !self.is_define_ident() {
                        self.add_expected_token_error("define statement");
                        self.try_recover();
                        continue;
                    }
                    self.unscan();
                    match self.parse_define(comments, tags, src) {
                        Some(define) => root.defines.push(define),
                        None => self.try_recover(),
                    }
                }
                Token::Ident => {
                    if !self.is_define_ident() {
                        self.add_expected_token_error("define statement");
                        self.try_recover();
                        continue;
                    }
                    let comments = self.take_comments();
                    self.unscan();
                    match self.parse_define(comments, Vec::new(), src) {
                        Some(define) => root.defines.push(define),
                        None => self.try_recover(),
                    }
                }
                _ => {
                    self.add_expected_token_error("define statement or rule");
                    self.try_recover();
                }
            }
        }
    }

    fn parse_define(
        &mut self,
        comments: Vec<String>,
        tags: Vec<String>,
        src: SourceLoc,
    ) -> Option<Define> {
        if !self.scan_token(Token::Ident, "define statement") || self.scanner.literal() != "define"
        {
            return None;
        }
        if !self.scan_token(Token::Ident, "define name") {
            return None;
        }
        let mut define = Define {
            comments,
            tags,
            name: self.scanner.literal().to_string(),
            fields: Vec::new(),
            src,
        };
        if !self.scan_token(Token::LBrace, "'{'") {
            return None;
        }
        loop {
            if self.scan() == Token::RBrace {
                if !self.comments.is_empty() {
                    self.add_error(&format!(
                        "comments not allowed before closing }}: {:?}",
                        self.comments
                    ));
                    return None;
                }
                return Some(define);
            }
            self.unscan();
            define.fields.push(self.parse_define_field()?);
        }
    }

    fn parse_define_field(&mut self) -> Option<DefineField> {
        if !self.scan_token(Token::Ident, "define field name") {
            return None;
        }
        let src = self.src.clone();
        let name = self.scanner.literal().to_string();
        let mut type_name = String::new();
        let mut token = self.scan();
        loop {
            if !matches!(
                token,
                Token::Ident | Token::Asterisk | Token::LBracket | Token::RBracket | Token::Dot
            ) {
                self.add_expected_token_error("define field type");
                return None;
            }
            type_name.push_str(self.scanner.literal());
            token = self.scan_internal(false);
            if matches!(token, Token::Eof | Token::Whitespace) {
                break;
            }
        }
        Some(DefineField {
            comments: self.take_comments(),
            name,
            type_name,
            src,
        })
    }

    fn parse_rule(
        &mut self,
        comments: Vec<String>,
        mut tags: Vec<String>,
        src: SourceLoc,
    ) -> Option<Rule> {
        let match_pattern = self.parse_match()?;
        if !self.scan_token(Token::Arrow, "'=>'") {
            return None;
        }
        if !self.comments.is_empty() {
            self.add_error("comments not allowed before =>");
            return None;
        }
        let replace = self.parse_replace()?;
        let name = tags.remove(0);
        Some(Rule {
            comments,
            name,
            tags,
            match_pattern,
            replace,
            src,
        })
    }

    fn parse_match(&mut self) -> Option<Expr> {
        if !self.scan_token(Token::LParen, "match pattern") {
            return None;
        }
        self.comments.clear();
        self.unscan();
        self.parse_func()
    }

    fn parse_replace(&mut self) -> Option<Expr> {
        let token = self.scan();
        self.comments.clear();
        match token {
            Token::LParen => {
                self.unscan();
                self.parse_func()
            }
            Token::Dollar => {
                self.unscan();
                self.parse_ref()
            }
            _ => {
                self.add_expected_token_error("replace pattern");
                None
            }
        }
    }

    fn parse_func_or_let(&mut self) -> Option<Expr> {
        assert_eq!(self.scan(), Token::LParen, "the caller checks for '('");
        let token = self.scan();
        let is_let = token == Token::Ident && self.scanner.literal() == LET_KEYWORD;
        self.unscan();
        if is_let {
            return self.parse_let();
        }
        self.parse_func_body()
    }

    fn parse_func(&mut self) -> Option<Expr> {
        assert_eq!(self.scan(), Token::LParen, "the caller checks for '('");
        self.parse_func_body()
    }

    fn parse_func_body(&mut self) -> Option<Expr> {
        let src = self.src.clone();
        let name = self.parse_func_name()?;
        let mut args = Vec::new();
        loop {
            if self.scan() == Token::RParen {
                if !self.comments.is_empty() {
                    self.add_error("comments not allowed before )");
                    return None;
                }
                return Some(Expr::new(ExprKind::Func { name, args }, src));
            }
            self.unscan();
            self.comments.clear();
            args.push(self.parse_arg()?);
        }
    }

    fn parse_func_name(&mut self) -> Option<FuncName> {
        let token = self.scan();
        self.comments.clear();
        match token {
            Token::Ident => {
                self.unscan();
                self.parse_names().map(FuncName::Names)
            }
            Token::LParen => {
                self.unscan();
                self.parse_func()
                    .map(|func| FuncName::Dynamic(Box::new(func)))
            }
            _ => {
                self.add_expected_token_error("name");
                None
            }
        }
    }

    fn parse_let(&mut self) -> Option<Expr> {
        let token = self.scan();
        assert!(
            token == Token::Ident && self.scanner.literal() == LET_KEYWORD,
            "the caller checks for the Let keyword"
        );
        if !self.scan_token(Token::LParen, "'('") {
            return None;
        }
        let src = self.src.clone();
        let mut labels = Vec::new();
        loop {
            let token = self.scan();
            self.unscan();
            if token == Token::RParen {
                if !self.comments.is_empty() {
                    self.add_error("comments not allowed before ')'");
                    return None;
                }
                self.scan();
                break;
            }
            if !self.scan_token(Token::Dollar, "'$'") {
                return None;
            }
            if !self.scan_token(Token::Ident, "label") {
                return None;
            }
            labels.push(self.scanner.literal().to_string());
        }
        if labels.is_empty() {
            self.add_error("let expression must assign 1 or more variables");
        }
        if !self.scan_token(Token::Colon, "':'") {
            return None;
        }
        if !self.scan_token(Token::LParen, "function") {
            return None;
        }
        self.unscan();
        let target = self.parse_func()?;
        if !self.scan_token(Token::Dollar, "ref") {
            return None;
        }
        self.unscan();
        let result = self.parse_ref()?;
        let ExprKind::Ref(result) = result.kind else {
            unreachable!("parse_ref gives a reference")
        };
        if !self.scan_token(Token::RParen, "')'") {
            return None;
        }
        Some(Expr::new(
            ExprKind::Let {
                labels,
                target: Box::new(target),
                result,
            },
            src,
        ))
    }

    fn parse_names(&mut self) -> Option<Vec<String>> {
        let mut names = Vec::new();
        loop {
            if !self.scan_token(Token::Ident, "name") {
                return None;
            }
            names.push(self.scanner.literal().to_string());
            if self.scan() != Token::Pipe {
                self.unscan();
                return Some(names);
            }
        }
    }

    fn parse_arg(&mut self) -> Option<Expr> {
        let token = self.scan();
        self.unscan();
        if token == Token::Dollar {
            return self.parse_bind_or_ref();
        }
        self.parse_and()
    }

    fn parse_bind_or_ref(&mut self) -> Option<Expr> {
        assert_eq!(self.scan(), Token::Dollar, "the caller checks for '$'");
        let src = self.src.clone();
        if !self.scan_token(Token::Ident, "label") {
            return None;
        }
        let label = self.scanner.literal().to_string();
        if self.scan() != Token::Colon {
            self.unscan();
            return Some(Expr::new(ExprKind::Ref(label), src));
        }
        let target = self.parse_and()?;
        Some(Expr::new(
            ExprKind::Bind {
                label,
                target: Box::new(target),
            },
            src,
        ))
    }

    fn parse_and(&mut self) -> Option<Expr> {
        let src = self.peek_next_source();
        let left = self.parse_expr()?;
        if self.scan() != Token::Ampersand {
            self.unscan();
            return Some(left);
        }
        let right = self.parse_and()?;
        Some(Expr::new(
            ExprKind::And(Box::new(left), Box::new(right)),
            src,
        ))
    }

    fn parse_expr(&mut self) -> Option<Expr> {
        let token = self.scan();
        self.comments.clear();
        match token {
            Token::LParen => {
                self.unscan();
                self.parse_func_or_let()
            }
            Token::Caret => {
                self.unscan();
                self.parse_not()
            }
            Token::LBracket => {
                self.unscan();
                self.parse_list()
            }
            Token::Asterisk => Some(Expr::new(ExprKind::Any, self.src.clone())),
            Token::Ident => Some(Expr::new(
                ExprKind::Name(self.scanner.literal().to_string()),
                self.src.clone(),
            )),
            Token::Str => {
                self.unscan();
                self.parse_string()
            }
            Token::Number => {
                self.unscan();
                self.parse_number()
            }
            _ => {
                self.add_expected_token_error("expression");
                None
            }
        }
    }

    fn parse_not(&mut self) -> Option<Expr> {
        assert_eq!(self.scan(), Token::Caret, "the caller checks for '^'");
        let src = self.src.clone();
        let input = self.parse_expr()?;
        Some(Expr::new(ExprKind::Not(Box::new(input)), src))
    }

    fn parse_list(&mut self) -> Option<Expr> {
        assert_eq!(self.scan(), Token::LBracket, "the caller checks for '['");
        let src = self.src.clone();
        let mut items = Vec::new();
        loop {
            if self.scan() == Token::RBracket {
                if !self.comments.is_empty() {
                    self.add_error("comments not allowed before ]");
                    return None;
                }
                return Some(Expr::new(ExprKind::List(items), src));
            }
            self.unscan();
            items.push(self.parse_list_child()?);
        }
    }

    fn parse_list_child(&mut self) -> Option<Expr> {
        let token = self.scan();
        self.comments.clear();
        if token == Token::Ellipses {
            return Some(Expr::new(ExprKind::ListAny, self.src.clone()));
        }
        self.unscan();
        self.parse_arg()
    }

    fn parse_ref(&mut self) -> Option<Expr> {
        assert_eq!(self.scan(), Token::Dollar, "the caller checks for '$'");
        let src = self.src.clone();
        if !self.scan_token(Token::Ident, "label") {
            return None;
        }
        Some(Expr::new(
            ExprKind::Ref(self.scanner.literal().to_string()),
            src,
        ))
    }

    fn parse_tags(&mut self) -> Option<Vec<String>> {
        assert_eq!(self.scan(), Token::LBracket, "the caller checks for '['");
        let mut tags = Vec::new();
        loop {
            if !self.scan_token(Token::Ident, "tag name") {
                return None;
            }
            tags.push(self.scanner.literal().to_string());
            if self.scan() == Token::RBracket {
                if !self.comments.is_empty() {
                    self.add_error("comments not allowed before ]");
                    return None;
                }
                return Some(tags);
            }
            self.unscan();
            if !self.scan_token(Token::Comma, "comma") {
                return None;
            }
        }
    }

    fn parse_string(&mut self) -> Option<Expr> {
        assert_eq!(self.scan(), Token::Str, "the caller checks for a string");
        let literal = self.scanner.literal();
        let inner = literal[1..literal.len() - 1].to_string();
        Some(Expr::new(ExprKind::Str(inner), self.src.clone()))
    }

    fn parse_number(&mut self) -> Option<Expr> {
        assert_eq!(self.scan(), Token::Number, "the caller checks for a number");
        match self.scanner.literal().parse::<i64>() {
            Ok(value) => Some(Expr::new(ExprKind::Number(value), self.src.clone())),
            Err(error) => {
                self.add_error(&error.to_string());
                None
            }
        }
    }

    fn peek_next_source(&mut self) -> SourceLoc {
        self.scan();
        let src = self.src.clone();
        self.unscan();
        src
    }

    fn scan_token(&mut self, expected: Token, what: &str) -> bool {
        if self.scan() != expected {
            self.add_expected_token_error(what);
            return false;
        }
        true
    }

    fn scan(&mut self) -> Token {
        self.scan_internal(true)
    }

    fn scan_internal(&mut self, skip_whitespace: bool) -> Token {
        if self.unscanned {
            std::mem::swap(&mut self.src, &mut self.save_src);
            self.unscanned = false;
            return self.scanner.token();
        }
        loop {
            self.save_src = self.src.clone();
            let token = self.scanner.scan();
            let (line, pos) = self.scanner.token_location();
            self.src.line = line;
            self.src.pos = pos;
            match token {
                Token::Eof => {
                    self.comments.clear();
                    if self.file + 1 >= self.files.len() {
                        return Token::Eof;
                    }
                    self.file += 1;
                    let (name, source) = self.files[self.file];
                    self.scanner = Scanner::new(source);
                    self.src = SourceLoc {
                        file: Arc::from(name),
                        ..SourceLoc::default()
                    };
                }
                Token::Comment => {
                    if !skip_whitespace {
                        return token;
                    }
                    self.comments.push(self.scanner.literal().to_string());
                }
                Token::Whitespace => {
                    if !skip_whitespace {
                        return token;
                    }
                    if self.scanner.literal().matches('\n').count() > 1 {
                        self.comments.clear();
                    }
                }
                _ => return token,
            }
        }
    }

    fn unscan(&mut self) {
        assert!(!self.unscanned, "unscan was already called");
        std::mem::swap(&mut self.src, &mut self.save_src);
        self.unscanned = true;
    }

    fn take_comments(&mut self) -> Vec<String> {
        std::mem::take(&mut self.comments)
    }

    fn add_expected_token_error(&mut self, what: &str) {
        let message = if self.scanner.token() == Token::Eof {
            format!("expected {what}, found EOF")
        } else {
            format!("expected {what}, found '{}'", self.scanner.literal())
        };
        self.add_error(&message);
    }

    fn add_error(&mut self, text: &str) {
        self.errors.push(format!("{}: {text}", self.src));
    }

    /// Skip ahead to a point where parsing can start again, so that one
    /// file can report more than one error.
    fn try_recover(&mut self) {
        loop {
            let token = self.scan();
            match token {
                Token::Eof => return,
                Token::LBracket | Token::Ident => {
                    if self.src.pos == 0 {
                        if token == Token::LBracket || self.is_define_ident() {
                            self.unscan();
                        }
                        return;
                    }
                }
                _ => {}
            }
        }
    }

    fn is_define_ident(&self) -> bool {
        self.scanner.token() == Token::Ident && self.scanner.literal() == "define"
    }
}

impl Display for Root {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for define in &self.defines {
            writeln!(f, "{define}")?;
        }
        for rule in &self.rules {
            writeln!(f, "{rule}")?;
        }
        Ok(())
    }
}

impl Display for Define {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for comment in &self.comments {
            writeln!(f, "{comment}")?;
        }
        if !self.tags.is_empty() {
            writeln!(f, "[{}]", self.tags.join(", "))?;
        }
        write!(f, "define {} {{", self.name)?;
        for field in &self.fields {
            for comment in &field.comments {
                write!(f, " {comment}")?;
            }
            write!(f, " {} {}", field.name, field.type_name)?;
        }
        write!(f, " }}")
    }
}

impl Display for Rule {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for comment in &self.comments {
            writeln!(f, "{comment}")?;
        }
        write!(f, "[{}", self.name)?;
        for tag in &self.tags {
            write!(f, ", {tag}")?;
        }
        writeln!(f, "]")?;
        write!(f, "{} => {}", self.match_pattern, self.replace)
    }
}

impl Display for Expr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ExprKind::Func { name, args } => {
                write!(f, "(")?;
                match name {
                    FuncName::Names(names) => write!(f, "{}", names.join(" | "))?,
                    FuncName::Dynamic(name) => write!(f, "{name}")?,
                }
                for arg in args {
                    write!(f, " {arg}")?;
                }
                write!(f, ")")
            }
            ExprKind::CustomFunc { name, args } => {
                write!(f, "({name}")?;
                for arg in args {
                    write!(f, " {arg}")?;
                }
                write!(f, ")")
            }
            ExprKind::Name(name) => write!(f, "{name}"),
            ExprKind::And(left, right) => write!(f, "{left} & {right}"),
            ExprKind::Not(input) => write!(f, "^{input}"),
            ExprKind::List(items) => {
                write!(f, "[")?;
                for item in items {
                    write!(f, " {item}")?;
                }
                write!(f, " ]")
            }
            ExprKind::ListAny => write!(f, "..."),
            ExprKind::Bind { label, target } => write!(f, "${label}:{target}"),
            ExprKind::Let {
                labels,
                target,
                result,
            } => {
                write!(f, "(Let (")?;
                for (index, label) in labels.iter().enumerate() {
                    if index > 0 {
                        write!(f, " ")?;
                    }
                    write!(f, "${label}")?;
                }
                write!(f, "):{target} ${result})")
            }
            ExprKind::Ref(label) => write!(f, "${label}"),
            ExprKind::Any => write!(f, "*"),
            ExprKind::Str(text) => write!(f, "\"{text}\""),
            ExprKind::Number(value) => write!(f, "{value}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(source: &str) -> Result<Root, Vec<String>> {
        Parser::new(&[("test.opt", source)]).parse()
    }

    fn parse_ok(source: &str) -> Root {
        parse(source).unwrap_or_else(|errors| panic!("parse errors: {errors:?}"))
    }

    #[test]
    fn define_with_comments_tags_and_fields() {
        let root = parse_ok(
            "# A file header.\n\n# About Lt.\n# More about Lt.\n[Comparison, Bool]\ndefine Lt {\n    # The left side.\n    Left  Scalar\n    Right Scalar\n}\n",
        );
        assert_eq!(root.defines.len(), 1);
        let define = &root.defines[0];
        assert_eq!(define.name, "Lt");
        assert_eq!(define.tags, vec!["Comparison", "Bool"]);
        assert_eq!(define.comments, vec!["# About Lt.", "# More about Lt."]);
        assert_eq!(define.fields.len(), 2);
        assert_eq!(define.fields[0].name, "Left");
        assert_eq!(define.fields[0].type_name, "Scalar");
        assert_eq!(define.fields[0].comments, vec!["# The left side."]);
        assert_eq!(define.fields[1].src.to_string(), "test.opt:9:5");
        assert_eq!(define.src.to_string(), "test.opt:5:1");
        assert_eq!(
            define.to_string(),
            "# About Lt.\n# More about Lt.\n[Comparison, Bool]\ndefine Lt { # The left side. Left Scalar Right Scalar }"
        );
    }

    #[test]
    fn define_errors_are_reported_with_recovery() {
        let errors = parse(
            "[...]\ndefine Not {}\n\n[Tag1 Tag2]\ndefine Not {}\n\n[Tag1]\ndef Not {}\n\ndefine {}\n\n}\ndefine Not Unknown\n\ndefine Not {\n    ()\n}\n\ndefine Not {\n    Input 123\n}\n",
        )
        .unwrap_err();
        assert_eq!(
            errors,
            vec![
                "test.opt:1:2: expected tag name, found '...'",
                "test.opt:4:7: expected comma, found 'Tag2'",
                "test.opt:8:1: expected define statement, found 'def'",
                "test.opt:10:8: expected define name, found '{'",
                "test.opt:13:12: expected '{', found 'Unknown'",
                "test.opt:16:5: expected define field name, found '('",
                "test.opt:20:11: expected define field type, found '123'",
            ]
        );
    }

    #[test]
    fn rules_keep_their_comments_and_order() {
        let root = parse_ok(
            "# The One rule.\n[One]\n(One) => (One)\n\n# A comment that belongs to no rule.\n\n# The Two rule.\n[Two, Normalize]\n(Two) => (Two)\n",
        );
        assert_eq!(root.rules.len(), 2);
        assert_eq!(root.rules[0].name, "One");
        assert_eq!(root.rules[0].comments, vec!["# The One rule."]);
        assert_eq!(root.rules[1].name, "Two");
        assert_eq!(root.rules[1].tags, vec!["Normalize"]);
        assert_eq!(root.rules[1].comments, vec!["# The Two rule."]);
        assert_eq!(root.rules[1].src.to_string(), "test.opt:8:1");
        assert_eq!(
            root.rules[1].to_string(),
            "# The Two rule.\n[Two, Normalize]\n(Two) => (Two)"
        );
    }

    #[test]
    fn every_match_operator_prints_back() {
        let source = "[Tag]\n(Op (SubOp *) \"hello\" 10 ^(SubOp) * [ ... * ... ] [ * ... ] [ ... * ] [ * ] []) => (Op)\n";
        let root = parse_ok(source);
        assert_eq!(
            root.rules[0].to_string(),
            "[Tag]\n(Op (SubOp *) \"hello\" 10 ^(SubOp) * [ ... * ... ] [ * ... ] [ ... * ] [ * ] [ ]) => (Op)"
        );
    }

    #[test]
    fn bindings_lets_and_boolean_expressions() {
        let root = parse_ok(
            "[Binding]\n(Op\n  $a:(Op *) &\n    (Let ($b $c $d):(Func $a) $d) &\n    (Func $g:(Func $e $f)) & ^^(Func2)\n)\n=>\n(Op (Func $a $b [$c $d]))\n",
        );
        assert_eq!(
            root.rules[0].to_string(),
            "[Binding]\n(Op $a:(Op *) & (Let ($b $c $d):(Func $a) $d) & (Func $g:(Func $e $f)) & ^^(Func2)) => (Op (Func $a $b [ $c $d ]))"
        );
        let ExprKind::Func { args, .. } = &root.rules[0].match_pattern.kind else {
            panic!("the match pattern is a function")
        };
        let ExprKind::Bind { label, target } = &args[0].kind else {
            panic!("the argument is a binding")
        };
        assert_eq!(label, "a");
        assert!(matches!(target.kind, ExprKind::And(..)));
        assert_eq!(args[0].src.to_string(), "test.opt:3:3");
    }

    #[test]
    fn multiple_names_and_dynamic_names() {
        let root = parse_ok("[Tag]\n(One | Two $x:*) => ((OpName $x) $x)\n");
        assert_eq!(root.rules[0].match_pattern.name_choice(), ["One", "Two"]);
        let ExprKind::Func { name, .. } = &root.rules[0].replace.kind else {
            panic!("the replace pattern is a function")
        };
        assert!(matches!(name, FuncName::Dynamic(_)));
        assert_eq!(
            root.rules[0].to_string(),
            "[Tag]\n(One | Two $x:*) => ((OpName $x) $x)"
        );
    }

    #[test]
    fn rule_errors() {
        let errors = parse("[Tag]\n(Op $x) =>\n").unwrap_err();
        assert_eq!(
            errors,
            vec!["test.opt:3:1: expected replace pattern, found EOF"]
        );
        let errors = parse("[Tag]\n(Op $x:) => $x\n").unwrap_err();
        assert_eq!(errors, vec!["test.opt:2:8: expected expression, found ')'"]);
        let errors = parse("[Tag]\n(Op (Let () :(F) $x)) => $x\n").unwrap_err();
        assert_eq!(
            errors,
            vec!["test.opt:2:11: let expression must assign 1 or more variables"]
        );
        let errors = parse("[Tag]\n(Op # no\n) => (Op)\n").unwrap_err();
        assert_eq!(errors, vec!["test.opt:3:1: comments not allowed before )"]);
    }

    #[test]
    fn files_are_one_token_stream() {
        let root = Parser::new(&[("a.opt", "define A {}\n"), ("b.opt", "[R]\n(A) => (A)\n")])
            .parse()
            .unwrap();
        assert_eq!(root.defines.len(), 1);
        assert_eq!(root.rules.len(), 1);
        assert_eq!(root.rules[0].src.to_string(), "b.opt:1:1");
    }

    #[test]
    fn a_number_that_does_not_fit_is_an_error() {
        let errors = parse("[Tag]\n(Op 99999999999999999999) => (Op)\n").unwrap_err();
        assert_eq!(errors.len(), 1);
        assert!(errors[0].starts_with("test.opt:2:5: "));
    }
}
