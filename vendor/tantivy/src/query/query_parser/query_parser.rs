use std::net::{AddrParseError, IpAddr};
use std::num::{ParseFloatError, ParseIntError};
use std::ops::Bound;
use std::str::{FromStr, ParseBoolError};
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use itertools::Itertools;
use query_grammar::{UserInputAst, UserInputBound, UserInputLeaf, UserInputLiteral};
use rustc_hash::FxHashMap;
use tantivy_fst::Regex;

use super::logical_ast::*;
use crate::index::Index;
use crate::json_utils::convert_to_fast_value_and_append_to_json_term;
use crate::query::range_query::{is_type_valid_for_fastfield_range_query, RangeQuery};
use crate::query::{
    AllQuery, BooleanQuery, BoostQuery, EmptyQuery, FuzzyTermQuery, Occur, PhrasePrefixQuery,
    PhraseQuery, Query, RegexQuery, TermQuery, TermSetQuery,
};
use crate::schema::{
    Facet, FacetParseError, Field, FieldType, IndexRecordOption, IntoIpv6Addr, JsonObjectOptions,
    Schema, Term, TextFieldIndexing, Type,
};
use crate::time::format_description::well_known::Rfc3339;
use crate::time::OffsetDateTime;
use crate::tokenizer::{TextAnalyzer, TokenizerManager};
use crate::{DateTime, Score};

/// Possible error that may happen when parsing a query.
#[derive(Debug, PartialEq, Eq, Error)]
pub enum QueryParserError {
    /// Error in the query syntax
    #[error("Syntax Error: {0}")]
    SyntaxError(String),
    /// This query is unsupported.
    #[error("Unsupported query: {0}")]
    UnsupportedQuery(String),
    /// The query references a field that is not in the schema
    #[error("Field does not exist: '{0}'")]
    FieldDoesNotExist(String),
    /// The query contains a term for a `u64` or `i64`-field, but the value
    /// is neither.
    #[error("Expected a valid integer: '{0:?}'")]
    ExpectedInt(#[from] ParseIntError),
    /// The query contains a term for a bytes field, but the value is not valid
    /// base64.
    #[error("Expected base64: '{0:?}'")]
    ExpectedBase64(#[from] base64::DecodeError),
    /// The query contains a term for a `f64`-field, but the value
    /// is not a f64.
    #[error("Invalid query: Only excluding terms given")]
    ExpectedFloat(#[from] ParseFloatError),
    /// The query contains a term for a bool field, but the value
    /// is not a bool.
    #[error("Expected a bool value: '{0:?}'")]
    ExpectedBool(#[from] ParseBoolError),
    /// It is forbidden queries that are only "excluding". (e.g. -title:pop)
    #[error("Invalid query: Only excluding terms given")]
    AllButQueryForbidden,
    /// If no default field is declared, running a query without any
    /// field specified is forbidden.
    #[error("No default field declared and no field specified in query")]
    NoDefaultFieldDeclared,
    /// The field searched for is not declared
    /// as indexed in the schema.
    #[error("The field '{0}' is not declared as indexed")]
    FieldNotIndexed(String),
    /// A phrase query was requested for a field that does not
    /// have any positions indexed.
    #[error("The field '{0}' does not have positions indexed")]
    FieldDoesNotHavePositionsIndexed(String),
    /// A phrase-prefix query requires at least two terms
    #[error(
        "The phrase '{phrase:?}' does not produce at least two terms using the tokenizer \
         '{tokenizer:?}'"
    )]
    PhrasePrefixRequiresAtLeastTwoTerms {
        /// The phrase which triggered the issue
        phrase: String,
        /// The tokenizer configured for the field
        tokenizer: String,
    },
    /// The tokenizer for the given field is unknown
    /// The two argument strings are the name of the field, the name of the tokenizer
    #[error("The tokenizer '{tokenizer:?}' for the field '{field:?}' is unknown")]
    UnknownTokenizer {
        /// The name of the tokenizer
        tokenizer: String,
        /// The field name
        field: String,
    },
    /// The query contains a range query with a phrase as one of the bounds.
    /// Only terms can be used as bounds.
    #[error("A range query cannot have a phrase as one of the bounds")]
    RangeMustNotHavePhrase,
    /// The format for the date field is not RFC 3339 compliant.
    #[error("The date field has an invalid format")]
    DateFormatError(#[from] time::error::Parse),
    /// The format for the facet field is invalid.
    #[error("The facet field is malformed: {0}")]
    FacetFormatError(#[from] FacetParseError),
    /// The format for the ip field is invalid.
    #[error("The ip field is malformed: {0}")]
    IpFormatError(#[from] AddrParseError),
}

/// Recursively remove empty clause from the AST
///
/// Returns `None` if and only if the `logical_ast` ended up being empty.
fn trim_ast(logical_ast: LogicalAst) -> Option<LogicalAst> {
    match logical_ast {
        LogicalAst::Clause(children) => {
            let trimmed_children = children
                .into_iter()
                .flat_map(|(occur, child)| {
                    trim_ast(child).map(|trimmed_child| (occur, trimmed_child))
                })
                .collect::<Vec<_>>();
            if trimmed_children.is_empty() {
                None
            } else {
                Some(LogicalAst::Clause(trimmed_children))
            }
        }
        _ => Some(logical_ast),
    }
}

/// Tantivy's Query parser
///
/// The language covered by the current parser is extremely simple.
///
/// * simple terms: "e.g.: `Barack Obama` will be seen as a sequence of two tokens Barack and Obama.
///   By default, the query parser will interpret this as a disjunction (see
///   `.set_conjunction_by_default()`) and will match all documents that contains either "Barack" or
///   "Obama" or both. Since we did not target a specific field, the query parser will look into the
///   so-called default fields (as set up in the constructor).
///
///   Assuming that the default fields are `body` and `title`, and the query parser is set with
///   conjunction as a default, our query will be interpreted as.
///   `(body:Barack OR title:Barack) AND (title:Obama OR body:Obama)`.
///   By default, all tokenized and indexed fields are default fields.
///
///   It is possible to explicitly target a field by prefixing the text by the `fieldname:`.
///   Note this only applies to the term directly following.
///   For instance, assuming the query parser is configured to use conjunction by default,
///   `body:Barack Obama` is not interpreted as `body:Barack AND body:Obama` but as
///   `body:Barack OR (body:Barack OR text:Obama)` .
///
/// * boolean operators `AND`, `OR`. `AND` takes precedence over `OR`, so that `a AND b OR c` is
///   interpreted as `(a AND b) OR c`.
///
/// * In addition to the boolean operators, the `-`, `+` can help define. These operators are
///   sufficient to express all queries using boolean operators. For instance `x AND y OR z` can be
///   written (`(+x +y) z`). In addition, these operators can help define "required optional"
///   queries. `(+x y)` matches the same document set as simply `x`, but `y` will help refining the
///   score.
///
/// * negative terms: By prepending a term by a `-`, a term can be excluded from the search. This is
///   useful for disambiguating a query. e.g. `apple -fruit`
///
/// * must terms: By prepending a term by a `+`, a term can be made required for the search.
///
/// * phrase terms: Quoted terms become phrase searches on fields that have positions indexed. e.g.,
///   `title:"Barack Obama"` will only find documents that have "barack" immediately followed by
///   "obama". Single quotes can also be used. If the text to be searched contains quotation mark,
///   it is possible to escape them with a `\`.
///
/// * range terms: Range searches can be done by specifying the start and end bound. These can be
///   inclusive or exclusive. e.g., `title:[a TO c}` will find all documents whose title contains a
///   word lexicographically between `a` and `c` (inclusive lower bound, exclusive upper bound).
///   Inclusive bounds are `[]`, exclusive are `{}`.
///
/// * set terms: Using the `IN` operator, a field can be matched against a set of literals, e.g.
///   `title: IN [a b cd]` will match documents where `title` is either `a`, `b` or `cd`, but do so
///   more efficiently than the alternative query `title:a OR title:b OR title:c` does.
///
/// * date values: The query parser supports rfc3339 formatted dates. For example
///   `"2002-10-02T15:00:00.05Z"` or `some_date_field:[2002-10-02T15:00:00Z TO
///   2002-10-02T18:00:00Z}`
///
/// * all docs query: A plain `*` will match all documents in the index.
///
/// Parts of the queries can be boosted by appending `^boostfactor`.
/// For instance, `"SRE"^2.0 OR devops^0.4` will boost documents containing `SRE` instead of
/// devops. Negative boosts are not allowed.
///
/// It is also possible to define a boost for a some specific field, at the query parser level.
/// (See [`set_field_boost(...)`](QueryParser::set_field_boost)). Typically you may want to boost a
/// title field.
///
/// Additionally, specific fields can be marked to use fuzzy term queries for each literal
/// via the [`QueryParser::set_field_fuzzy`] method.
///
/// Phrase terms support the `~` slop operator which allows to set the phrase's matching
/// distance in words. `"big wolf"~1` will return documents containing the phrase `"big bad wolf"`.
///
/// Phrase terms also support the `*` prefix operator which switches the phrase's matching
/// to consider all documents which contain the last term as a prefix, e.g. `"big bad wo"*` will
/// match `"big bad wolf"`.
#[derive(Clone)]
pub struct QueryParser {
    schema: Schema,
    default_fields: Vec<Field>,
    conjunction_by_default: bool,
    tokenizer_manager: TokenizerManager,
    boost: FxHashMap<Field, Score>,
    fuzzy: FxHashMap<Field, Fuzzy>,
    regexes_allowed: bool,
}

#[derive(Clone)]
struct Fuzzy {
    prefix: bool,
    distance: u8,
    transpose_cost_one: bool,
}

fn all_negative(ast: &LogicalAst) -> bool {
    match ast {
        LogicalAst::Leaf(_) => false,
        LogicalAst::Boost(ref child_ast, _) => all_negative(child_ast),
        LogicalAst::Clause(children) => children
            .iter()
            .all(|(ref occur, child)| (*occur == Occur::MustNot) || all_negative(child)),
    }
}

// Make an all-negative ast into a normal ast. Must not be used on an already okay ast.
fn make_non_negative(ast: &mut LogicalAst) {
    match ast {
        LogicalAst::Leaf(_) => (),
        LogicalAst::Boost(ref mut child_ast, _) => make_non_negative(child_ast),
        LogicalAst::Clause(children) => children.push((Occur::Should, LogicalLiteral::All.into())),
    }
}

/// Similar to the try/? macro, but returns a tuple of (None, Vec<Error>) instead of Err(Error)
macro_rules! try_tuple {
    ($expr:expr) => {{
        match $expr {
            Ok(val) => val,
            Err(e) => return (None, vec![e.into()]),
        }
    }};
}

impl QueryParser {
    /// Creates a `QueryParser`, given
    /// * schema - index Schema
    /// * default_fields - fields used to search if no field is specifically defined in the query.
    pub fn new(
        schema: Schema,
        default_fields: Vec<Field>,
        tokenizer_manager: TokenizerManager,
    ) -> QueryParser {
        QueryParser {
            schema,
            default_fields,
            tokenizer_manager,
            conjunction_by_default: false,
            boost: Default::default(),
            fuzzy: Default::default(),
            regexes_allowed: false,
        }
    }

    // Splits a full_path as written in a query, into a field name and a
    // json path.
    pub(crate) fn split_full_path<'a>(&self, full_path: &'a str) -> Option<(Field, &'a str)> {
        self.schema.find_field(full_path)
    }

    /// Creates a `QueryParser`, given
    ///  * an index
    ///  * a set of default fields used to search if no field is specifically defined in the query.
    pub fn for_index(index: &Index, default_fields: Vec<Field>) -> QueryParser {
        QueryParser::new(index.schema(), default_fields, index.tokenizers().clone())
    }

    /// Set the default way to compose queries to a conjunction.
    ///
    /// By default, the query `happy tax payer` is equivalent to the query
    /// `happy OR tax OR payer`. After calling `.set_conjunction_by_default()`
    /// `happy tax payer` will be interpreted by the parser as `happy AND tax AND payer`.
    pub fn set_conjunction_by_default(&mut self) {
        self.conjunction_by_default = true;
    }

    /// Sets a boost for a specific field.
    ///
    /// The parse query will automatically boost this field.
    ///
    /// If the query defines a query boost through the query language (e.g: `country:France^3.0`),
    /// the two boosts (the one defined in the query, and the one defined in the `QueryParser`)
    /// are multiplied together.
    pub fn set_field_boost(&mut self, field: Field, boost: Score) {
        self.boost.insert(field, boost);
    }

    /// Sets the given [field][`Field`] to use [fuzzy term queries][`FuzzyTermQuery`]
    ///
    /// If set, the parse will produce queries using fuzzy term queries
    /// with the given parameters for each literal matched against the given field.
    ///
    /// See the [`FuzzyTermQuery::new`] and [`FuzzyTermQuery::new_prefix`] methods
    /// for the meaning of the individual parameters.
    pub fn set_field_fuzzy(
        &mut self,
        field: Field,
        prefix: bool,
        distance: u8,
        transpose_cost_one: bool,
    ) {
        self.fuzzy.insert(
            field,
            Fuzzy {
                prefix,
                distance,
                transpose_cost_one,
            },
        );
    }

    /// Allow regexes in queries
    pub fn allow_regexes(&mut self) {
        self.regexes_allowed = true;
    }

    /// Parse a query
    ///
    /// Note that `parse_query` returns an error if the input
    /// is not a valid query.
    pub fn parse_query(&self, query: &str) -> Result<Box<dyn Query>, QueryParserError> {
        let logical_ast = self.parse_query_to_logical_ast(query)?;
        Ok(convert_to_query(&self.fuzzy, logical_ast))
    }

    /// Parse a query leniently
    ///
    /// This variant parses invalid query on a best effort basis. If some part of the query can't
    /// reasonably be executed (range query without field, searching on a non existing field,
    /// searching without precising field when no default field is provided...), they may get
    /// turned into a "match-nothing" subquery.
    ///
    /// In case it encountered such issues, they are reported as a Vec of errors.
    pub fn parse_query_lenient(&self, query: &str) -> (Box<dyn Query>, Vec<QueryParserError>) {
        let (logical_ast, errors) = self.parse_query_to_logical_ast_lenient(query);
        (convert_to_query(&self.fuzzy, logical_ast), errors)
    }

    /// Build a query from an already parsed user input AST
    ///
    /// This can be useful if the user input AST parsed using [`query_grammar`]
    /// needs to be inspected before the query is re-interpreted w.r.t.
    /// index specifics like field names and tokenizers.
    pub fn build_query_from_user_input_ast(
        &self,
        user_input_ast: UserInputAst,
    ) -> Result<Box<dyn Query>, QueryParserError> {
        let (logical_ast, mut err) = self.compute_logical_ast_lenient(user_input_ast);
        if !err.is_empty() {
            return Err(err.swap_remove(0));
        }
        Ok(convert_to_query(&self.fuzzy, logical_ast))
    }

    /// Build leniently a query from an already parsed user input AST.
    ///
    /// See also [`QueryParser::build_query_from_user_input_ast`]
    pub fn build_query_from_user_input_ast_lenient(
        &self,
        user_input_ast: UserInputAst,
    ) -> (Box<dyn Query>, Vec<QueryParserError>) {
        let (logical_ast, errors) = self.compute_logical_ast_lenient(user_input_ast);
        (convert_to_query(&self.fuzzy, logical_ast), errors)
    }

    /// Parse the user query into an AST.
    fn parse_query_to_logical_ast(&self, query: &str) -> Result<LogicalAst, QueryParserError> {
        let user_input_ast = query_grammar::parse_query(query)
            .map_err(|_| QueryParserError::SyntaxError(query.to_string()))?;
        let (ast, mut err) = self.compute_logical_ast_lenient(user_input_ast);
        if !err.is_empty() {
            return Err(err.swap_remove(0));
        }
        Ok(ast.simplify())
    }

    /// Parse the user query into an AST.
    fn parse_query_to_logical_ast_lenient(
        &self,
        query: &str,
    ) -> (LogicalAst, Vec<QueryParserError>) {
        let (user_input_ast, errors) = query_grammar::parse_query_lenient(query);
        let mut errors: Vec<_> = errors
            .into_iter()
            .map(|error| {
                QueryParserError::SyntaxError(format!(
                    "{} at position {}",
                    error.message, error.pos
                ))
            })
            .collect();
        let (ast, mut ast_errors) = self.compute_logical_ast_lenient(user_input_ast);
        errors.append(&mut ast_errors);
        (ast, errors)
    }

    fn compute_logical_ast_lenient(
        &self,
        user_input_ast: UserInputAst,
    ) -> (LogicalAst, Vec<QueryParserError>) {
        let (mut ast, mut err) = self.compute_logical_ast_with_occur_lenient(user_input_ast);
        if let LogicalAst::Clause(children) = &ast {
            if children.is_empty() {
                return (ast, err);
            }
        }
        if all_negative(&ast) {
            err.push(QueryParserError::AllButQueryForbidden);
            make_non_negative(&mut ast);
        }
        (ast, err)
    }

    fn compute_boundary_term(
        &self,
        field: Field,
        json_path: &str,
        phrase: &str,
    ) -> Result<Term, QueryParserError> {
        let field_entry = self.schema.get_field_entry(field);
        let field_type = field_entry.field_type();
        let field_supports_ff_range_queries = field_type.is_fast()
            && is_type_valid_for_fastfield_range_query(field_type.value_type());

        if !field_type.is_indexed() && !field_supports_ff_range_queries {
            return Err(QueryParserError::FieldNotIndexed(
                field_entry.name().to_string(),
            ));
        }
        if !json_path.is_empty() && field_type.value_type() != Type::Json {
            return Err(QueryParserError::UnsupportedQuery(format!(
                "Json path is not supported for field {:?}",
                field_entry.name()
            )));
        }
        match *field_type {
            FieldType::U64(_) => {
                let val: u64 = u64::from_str(phrase)?;
                Ok(Term::from_field_u64(field, val))
            }
            FieldType::I64(_) => {
                let val: i64 = i64::from_str(phrase)?;
                Ok(Term::from_field_i64(field, val))
            }
            FieldType::F64(_) => {
                let val: f64 = f64::from_str(phrase)?;
                Ok(Term::from_field_f64(field, val))
            }
            FieldType::Bool(_) => {
                let val: bool = bool::from_str(phrase)?;
                Ok(Term::from_field_bool(field, val))
            }
            FieldType::Date(_) => {
                let dt = OffsetDateTime::parse(phrase, &Rfc3339)?;
                Ok(Term::from_field_date(field, DateTime::from_utc(dt)))
            }
            FieldType::Str(ref str_options) => {
                let option = str_options.get_indexing_options().ok_or_else(|| {
                    // This should have been seen earlier really.
                    QueryParserError::FieldNotIndexed(field_entry.name().to_string())
                })?;
                let mut text_analyzer =
                    self.tokenizer_manager
                        .get(option.tokenizer())
                        .ok_or_else(|| QueryParserError::UnknownTokenizer {
                            field: field_entry.name().to_string(),
                            tokenizer: option.tokenizer().to_string(),
                        })?;
                let mut terms: Vec<Term> = Vec::new();
                let mut token_stream = text_analyzer.token_stream(phrase);
                token_stream.process(&mut |token| {
                    let term = Term::from_field_text(field, &token.text);
                    terms.push(term);
                });
                if terms.len() != 1 {
                    return Err(QueryParserError::UnsupportedQuery(format!(
                        "Range query boundary cannot have multiple tokens: {phrase:?} [{terms:?}]."
                    )));
                }
                Ok(terms.into_iter().next().unwrap())
            }
            FieldType::JsonObject(ref json_options) => {
                let mut term = Term::from_field_json_path(
                    field,
                    json_path,
                    json_options.is_expand_dots_enabled(),
                );
                if let Some(term) =
                    // Try to convert the phrase to a fast value
                    convert_to_fast_value_and_append_to_json_term(&term, phrase, false)
                {
                    Ok(term)
                } else {
                    term.append_type_and_str(phrase);
                    Ok(term)
                }
            }
            FieldType::Facet(_) => match Facet::from_text(phrase) {
                Ok(facet) => Ok(Term::from_facet(field, &facet)),
                Err(e) => Err(QueryParserError::from(e)),
            },
            FieldType::Bytes(_) => {
                let bytes = BASE64
                    .decode(phrase)
                    .map_err(QueryParserError::ExpectedBase64)?;
                Ok(Term::from_field_bytes(field, &bytes))
            }
            FieldType::IpAddr(_) => {
                let ip_v6 = IpAddr::from_str(phrase)?.into_ipv6_addr();
                Ok(Term::from_field_ip_addr(field, ip_v6))
            }
        }
    }

    fn compute_logical_ast_for_leaf(
        &self,
        field: Field,
        json_path: &str,
        phrase: &str,
        slop: u32,
        prefix: bool,
    ) -> Result<Vec<LogicalLiteral>, QueryParserError> {
        let field_entry = self.schema.get_field_entry(field);
        let field_type = field_entry.field_type();
        let field_name = field_entry.name();
        if !field_type.is_indexed() {
            return Err(QueryParserError::FieldNotIndexed(field_name.to_string()));
        }
        if field_type.value_type() != Type::Json && !json_path.is_empty() {
            let field_name = self.schema.get_field_name(field);
            return Err(QueryParserError::FieldDoesNotExist(format!(
                "{field_name}.{json_path}"
            )));
        }
        match *field_type {
            FieldType::U64(_) => {
                let val: u64 = u64::from_str(phrase)?;
                let i64_term = Term::from_field_u64(field, val);
                Ok(vec![LogicalLiteral::Term(i64_term)])
            }
            FieldType::I64(_) => {
                let val: i64 = i64::from_str(phrase)?;
                let i64_term = Term::from_field_i64(field, val);
                Ok(vec![LogicalLiteral::Term(i64_term)])
            }
            FieldType::F64(_) => {
                let val: f64 = f64::from_str(phrase)?;
                let f64_term = Term::from_field_f64(field, val);
                Ok(vec![LogicalLiteral::Term(f64_term)])
            }
            FieldType::Bool(_) => {
                let val: bool = bool::from_str(phrase)?;
                let bool_term = Term::from_field_bool(field, val);
                Ok(vec![LogicalLiteral::Term(bool_term)])
            }
            FieldType::Date(_) => {
                let dt = OffsetDateTime::parse(phrase, &Rfc3339)?;
                let dt_term = Term::from_field_date_for_search(field, DateTime::from_utc(dt));
                Ok(vec![LogicalLiteral::Term(dt_term)])
            }
            FieldType::Str(ref str_options) => {
                let indexing_options = str_options.get_indexing_options().ok_or_else(|| {
                    // This should have been seen earlier really.
                    QueryParserError::FieldNotIndexed(field_name.to_string())
                })?;
                let mut text_analyzer = self
                    .tokenizer_manager
                    .get(indexing_options.tokenizer())
                    .ok_or_else(|| QueryParserError::UnknownTokenizer {
                        field: field_name.to_string(),
                        tokenizer: indexing_options.tokenizer().to_string(),
                    })?;
                Ok(generate_literals_for_str(
                    field_name,
                    field,
                    phrase,
                    slop,
                    prefix,
                    indexing_options,
                    &mut text_analyzer,
                )?
                .into_iter()
                .collect())
            }
            FieldType::JsonObject(ref json_options) => generate_literals_for_json_object(
                field_name,
                field,
                json_path,
                phrase,
                &self.tokenizer_manager,
                json_options,
            ),
            FieldType::Facet(_) => match Facet::from_text(phrase) {
                Ok(facet) => {
                    let facet_term = Term::from_facet(field, &facet);
                    Ok(vec![LogicalLiteral::Term(facet_term)])
                }
                Err(e) => Err(QueryParserError::from(e)),
            },
            FieldType::Bytes(_) => {
                let bytes = BASE64
                    .decode(phrase)
                    .map_err(QueryParserError::ExpectedBase64)?;
                let bytes_term = Term::from_field_bytes(field, &bytes);
                Ok(vec![LogicalLiteral::Term(bytes_term)])
            }
            FieldType::IpAddr(_) => {
                let ip_v6 = IpAddr::from_str(phrase)?.into_ipv6_addr();
                let term = Term::from_field_ip_addr(field, ip_v6);
                Ok(vec![LogicalLiteral::Term(term)])
            }
        }
    }

    fn default_occur(&self) -> Occur {
        if self.conjunction_by_default {
            Occur::Must
        } else {
            Occur::Should
        }
    }

    fn resolve_bound(
        &self,
        field: Field,
        json_path: &str,
        bound: &UserInputBound,
    ) -> Result<Bound<Term>, QueryParserError> {
        if bound.term_str() == "*" {
            return Ok(Bound::Unbounded);
        }
        let term = self.compute_boundary_term(field, json_path, bound.term_str())?;
        match *bound {
            UserInputBound::Inclusive(_) => Ok(Bound::Included(term)),
            UserInputBound::Exclusive(_) => Ok(Bound::Excluded(term)),
            UserInputBound::Unbounded => Ok(Bound::Unbounded),
        }
    }

    fn compute_logical_ast_with_occur_lenient(
        &self,
        user_input_ast: UserInputAst,
    ) -> (LogicalAst, Vec<QueryParserError>) {
        match user_input_ast {
            UserInputAst::Clause(sub_queries) => {
                let default_occur = self.default_occur();
                let mut logical_sub_queries: Vec<(Occur, LogicalAst)> = Vec::new();
                let mut errors = Vec::new();
                for (occur_opt, sub_ast) in sub_queries {
                    let (sub_ast, mut sub_errors) =
                        self.compute_logical_ast_with_occur_lenient(sub_ast);
                    let occur = occur_opt.unwrap_or(default_occur);
                    logical_sub_queries.push((occur, sub_ast));
                    errors.append(&mut sub_errors);
                }
                (LogicalAst::Clause(logical_sub_queries), errors)
            }
            UserInputAst::Boost(ast, boost) => {
                let (ast, errors) = self.compute_logical_ast_with_occur_lenient(*ast);
                (ast.boost(boost.into_inner() as Score), errors)
            }
            UserInputAst::Leaf(leaf) => {
                let (ast, errors) = self.compute_logical_ast_from_leaf_lenient(*leaf);
                // if the error is not recoverable, replace it with an empty clause. We will end up
                // trimming those later
                (
                    ast.unwrap_or_else(|| LogicalAst::Clause(Vec::new())),
                    errors,
                )
            }
        }
    }

    fn field_boost(&self, field: Field) -> Score {
        self.boost.get(&field).cloned().unwrap_or(1.0)
    }

    fn default_indexed_json_fields(&self) -> impl Iterator<Item = Field> + '_ {
        let schema = self.schema.clone();
        self.default_fields.iter().cloned().filter(move |field| {
            let field_type = schema.get_field_entry(*field).field_type();
            field_type.value_type() == Type::Json && field_type.is_indexed()
        })
    }

    /// Given a literal, returns the list of terms that should be searched.
    ///
    /// The terms are identified by a triplet:
    /// - tantivy field
    /// - field_path: tantivy has JSON fields. It is possible to target a member of a JSON object by
    ///   naturally extending the json field name with a "." separated field_path
    /// - field_phrase: the phrase that is being searched.
    ///
    /// The literal identifies the targeted field by a so-called *full field path*,
    /// specified before the ":". (e.g. identity.username:fulmicoton).
    ///
    /// The way we split the full field path into (field_name, field_path) can be ambiguous,
    /// because field_names can contain "." themselves.
    // For instance if a field is named `one.two` and another one is named `one`,
    /// should `one.two:three` target `one.two` with field path `` or or `one` with
    /// the field path `two`.
    ///
    /// In this case tantivy, just picks the solution with the longest field name.
    ///
    /// Quirk: As a hack for quickwit, we do not split over a dot that appear escaped '\.'.
    fn compute_path_triplets_for_literal<'a>(
        &self,
        literal: &'a UserInputLiteral,
    ) -> Result<Vec<(Field, &'a str, &'a str)>, QueryParserError> {
        let full_path = if let Some(full_path) = &literal.field_name {
            full_path
        } else {
            // The user did not specify any path...
            // We simply target default fields.
            if self.default_fields.is_empty() {
                return Err(QueryParserError::NoDefaultFieldDeclared);
            }
            return Ok(self
                .default_fields
                .iter()
                .map(|default_field| (*default_field, "", literal.phrase.as_str()))
                .collect::<Vec<(Field, &str, &str)>>());
        };
        if let Some((field, path)) = self.split_full_path(full_path) {
            return Ok(vec![(field, path, literal.phrase.as_str())]);
        }
        // We need to add terms associated with json default fields.
        let triplets: Vec<(Field, &str, &str)> = self
            .default_indexed_json_fields()
            .map(|json_field| (json_field, full_path.as_str(), literal.phrase.as_str()))
            .collect();
        if triplets.is_empty() {
            return Err(QueryParserError::FieldDoesNotExist(full_path.to_string()));
        }
        Ok(triplets)
    }

    fn compute_logical_ast_from_leaf_lenient(
        &self,
        leaf: UserInputLeaf,
    ) -> (Option<LogicalAst>, Vec<QueryParserError>) {
        match leaf {
            UserInputLeaf::Literal(literal) => {
                let term_phrases: Vec<(Field, &str, &str)> =
                    try_tuple!(self.compute_path_triplets_for_literal(&literal));
                let mut asts: Vec<LogicalAst> = Vec::new();
                let mut errors: Vec<QueryParserError> = Vec::new();
                for (field, json_path, phrase) in term_phrases {
                    let unboosted_asts = match self.compute_logical_ast_for_leaf(
                        field,
                        json_path,
                        phrase,
                        literal.slop,
                        literal.prefix,
                    ) {
                        Ok(asts) => asts,
                        Err(e) => {
                            errors.push(e);
                            continue;
                        }
                    };
                    for ast in unboosted_asts {
                        // Apply some field specific boost defined at the query parser level.
                        let boost = self.field_boost(field);
                        asts.push(LogicalAst::Leaf(Box::new(ast)).boost(boost));
                    }
                }
                if !asts.is_empty() {
                    // if some fields failed but other succeeded, we consider this a success, it
                    // probably means the default_fields contains
                    // text and non-text fields, and the non-text ones failed
                    errors.clear();
                }
                let result_ast: LogicalAst = if asts.len() == 1 {
                    asts.into_iter().next().unwrap()
                } else {
                    LogicalAst::Clause(asts.into_iter().map(|ast| (Occur::Should, ast)).collect())
                };
                (Some(result_ast), errors)
            }
            UserInputLeaf::All => (
                Some(LogicalAst::Leaf(Box::new(LogicalLiteral::All))),
                Vec::new(),
            ),
            UserInputLeaf::Range {
                field: full_field_opt,
                lower,
                upper,
            } => {
                let Some(full_path) = full_field_opt else {
                    return (
                        None,
                        vec![QueryParserError::UnsupportedQuery(
                            "Range query need to target a specific field.".to_string(),
                        )],
                    );
                };
                let (field, json_path) = try_tuple!(self
                    .split_full_path(&full_path)
                    .ok_or_else(|| QueryParserError::FieldDoesNotExist(full_path.clone())));
                let mut errors = Vec::new();
                let lower = match self.resolve_bound(field, json_path, &lower) {
                    Ok(bound) => bound,
                    Err(error) => {
                        errors.push(error);
                        Bound::Unbounded
                    }
                };
                let upper = match self.resolve_bound(field, json_path, &upper) {
                    Ok(bound) => bound,
                    Err(error) => {
                        errors.push(error);
                        Bound::Unbounded
                    }
                };
                if lower == Bound::Unbounded && upper == Bound::Unbounded {
                    // this range is useless, either because a user requested [* TO *], or because
                    // we failed to parse something. Either way, there is no point emitting it
                    return (None, errors);
                }
                let logical_ast =
                    LogicalAst::Leaf(Box::new(LogicalLiteral::Range { lower, upper }));
                (Some(logical_ast), errors)
            }
            UserInputLeaf::Set {
                field: full_field_opt,
                elements,
            } => {
                let full_path = try_tuple!(full_field_opt.ok_or_else(|| {
                    QueryParserError::UnsupportedQuery(
                        "Range query need to target a specific field.".to_string(),
                    )
                }));
                let (field, json_path) = try_tuple!(self
                    .split_full_path(&full_path)
                    .ok_or_else(|| QueryParserError::FieldDoesNotExist(full_path.clone())));
                let (elements, errors) = elements
                    .into_iter()
                    .map(|element| self.compute_boundary_term(field, json_path, &element))
                    .partition_result();
                let logical_ast = LogicalAst::Leaf(Box::new(LogicalLiteral::Set { elements }));
                (Some(logical_ast), errors)
            }
            UserInputLeaf::Exists { .. } => (
                None,
                vec![QueryParserError::UnsupportedQuery(
                    "Range query need to target a specific field.".to_string(),
                )],
            ),
            UserInputLeaf::Regex { field, pattern } => {
                if !self.regexes_allowed {
                    return (
                        None,
                        vec![QueryParserError::UnsupportedQuery(
                            "Regex queries are not allowed.".to_string(),
                        )],
                    );
                }
                let full_path = try_tuple!(field.ok_or_else(|| {
                    QueryParserError::UnsupportedQuery(
                        "Regex query need to target a specific field.".to_string(),
                    )
                }));
                let (field, json_path) = try_tuple!(self
                    .split_full_path(&full_path)
                    .ok_or_else(|| QueryParserError::FieldDoesNotExist(full_path.clone())));
                if !json_path.is_empty() {
                    return (
                        None,
                        vec![QueryParserError::UnsupportedQuery(
                            "Regex query does not support json paths.".to_string(),
                        )],
                    );
                }
                if !matches!(
                    self.schema.get_field_entry(field).field_type(),
                    FieldType::Str(_)
                ) {
                    return (
                        None,
                        vec![QueryParserError::UnsupportedQuery(
                            "Regex query only supported on text fields".to_string(),
                        )],
                    );
                }
                let pattern = try_tuple!(Regex::new(&pattern).map_err(|e| {
                    QueryParserError::UnsupportedQuery(format!("Invalid regex: {e}"))
                }));
                let logical_ast = LogicalAst::Leaf(Box::new(LogicalLiteral::Regex {
                    pattern: Arc::new(pattern),
                    field,
                }));
                (Some(logical_ast), Vec::new())
            }
        }
    }
}

fn convert_literal_to_query(
    fuzzy: &FxHashMap<Field, Fuzzy>,
    logical_literal: LogicalLiteral,
) -> Box<dyn Query> {
    match logical_literal {
        LogicalLiteral::Term(term) => {
            if let Some(fuzzy) = fuzzy.get(&term.field()) {
                if fuzzy.prefix {
                    Box::new(FuzzyTermQuery::new_prefix(
                        term,
                        fuzzy.distance,
                        fuzzy.transpose_cost_one,
                    ))
                } else {
                    Box::new(FuzzyTermQuery::new(
                        term,
                        fuzzy.distance,
                        fuzzy.transpose_cost_one,
                    ))
                }
            } else {
                Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs))
            }
        }
        LogicalLiteral::Phrase {
            terms,
            slop,
            prefix,
        } => {
            if prefix {
                Box::new(PhrasePrefixQuery::new_with_offset(terms))
            } else {
                Box::new(PhraseQuery::new_with_offset_and_slop(terms, slop))
            }
        }
        LogicalLiteral::Range { lower, upper } => Box::new(RangeQuery::new(lower, upper)),
        LogicalLiteral::Set { elements, .. } => Box::new(TermSetQuery::new(elements)),
        LogicalLiteral::All => Box::new(AllQuery),
        LogicalLiteral::Regex { pattern, field } => {
            Box::new(RegexQuery::from_regex(pattern, field))
        }
    }
}

fn generate_literals_for_str(
    field_name: &str,
    field: Field,
    phrase: &str,
    slop: u32,
    prefix: bool,
    indexing_options: &TextFieldIndexing,
    text_analyzer: &mut TextAnalyzer,
) -> Result<Option<LogicalLiteral>, QueryParserError> {
    let mut terms: Vec<(usize, Term)> = Vec::new();
    let mut token_stream = text_analyzer.token_stream(phrase);
    token_stream.process(&mut |token| {
        let term = Term::from_field_text(field, &token.text);
        terms.push((token.position, term));
    });
    if terms.len() <= 1 {
        if prefix {
            return Err(QueryParserError::PhrasePrefixRequiresAtLeastTwoTerms {
                phrase: phrase.to_owned(),
                tokenizer: indexing_options.tokenizer().to_owned(),
            });
        }
        let term_literal_opt = terms
            .into_iter()
            .next()
            .map(|(_, term)| LogicalLiteral::Term(term));
        return Ok(term_literal_opt);
    }
    if !indexing_options.index_option().has_positions() {
        return Err(QueryParserError::FieldDoesNotHavePositionsIndexed(
            field_name.to_string(),
        ));
    }
    Ok(Some(LogicalLiteral::Phrase {
        terms,
        slop,
        prefix,
    }))
}

fn generate_literals_for_json_object(
    field_name: &str,
    field: Field,
    json_path: &str,
    phrase: &str,
    tokenizer_manager: &TokenizerManager,
    json_options: &JsonObjectOptions,
) -> Result<Vec<LogicalLiteral>, QueryParserError> {
    let text_options = json_options.get_text_indexing_options().ok_or_else(|| {
        // This should have been seen earlier really.
        QueryParserError::FieldNotIndexed(field_name.to_string())
    })?;
    let mut text_analyzer = tokenizer_manager
        .get(text_options.tokenizer())
        .ok_or_else(|| QueryParserError::UnknownTokenizer {
            field: field_name.to_string(),
            tokenizer: text_options.tokenizer().to_string(),
        })?;
    let index_record_option = text_options.index_option();
    let mut logical_literals = Vec::new();

    let get_term_with_path =
        || Term::from_field_json_path(field, json_path, json_options.is_expand_dots_enabled());

    // Try to convert the phrase to a fast value
    if let Some(term) =
        convert_to_fast_value_and_append_to_json_term(&get_term_with_path(), phrase, true)
    {
        logical_literals.push(LogicalLiteral::Term(term));
    }

    // Try to tokenize the phrase and create Terms.
    let mut positions_and_terms = Vec::<(usize, Term)>::new();
    let mut token_stream = text_analyzer.token_stream(phrase);
    token_stream.process(&mut |token| {
        let mut term = get_term_with_path();
        term.append_type_and_str(&token.text);
        positions_and_terms.push((token.position, term.clone()));
    });

    if positions_and_terms.len() <= 1 {
        for (_, term) in positions_and_terms {
            logical_literals.push(LogicalLiteral::Term(term));
        }
        return Ok(logical_literals);
    }
    if !index_record_option.has_positions() {
        return Err(QueryParserError::FieldDoesNotHavePositionsIndexed(
            field_name.to_string(),
        ));
    }
    logical_literals.push(LogicalLiteral::Phrase {
        terms: positions_and_terms,
        slop: 0,
        prefix: false,
    });
    Ok(logical_literals)
}

fn convert_to_query(fuzzy: &FxHashMap<Field, Fuzzy>, logical_ast: LogicalAst) -> Box<dyn Query> {
    match trim_ast(logical_ast) {
        Some(LogicalAst::Clause(trimmed_clause)) => {
            let occur_subqueries = trimmed_clause
                .into_iter()
                .map(|(occur, subquery)| (occur, convert_to_query(fuzzy, subquery)))
                .collect::<Vec<_>>();
            assert!(
                !occur_subqueries.is_empty(),
                "Should not be empty after trimming"
            );
            Box::new(BooleanQuery::new(occur_subqueries))
        }
        Some(LogicalAst::Leaf(trimmed_logical_literal)) => {
            convert_literal_to_query(fuzzy, *trimmed_logical_literal)
        }
        Some(LogicalAst::Boost(ast, boost)) => {
            let query = convert_to_query(fuzzy, *ast);
            let boosted_query = BoostQuery::new(query, boost);
            Box::new(boosted_query)
        }
        None => Box::new(EmptyQuery),
    }
}

#[cfg(test)]
mod test {
    use matches::assert_matches;

    use super::super::logical_ast::*;
    use super::{QueryParser, QueryParserError};
    use crate::query::Query;
    use crate::schema::{
        FacetOptions, Field, IndexRecordOption, Schema, Term, TextFieldIndexing, TextOptions, FAST,
        INDEXED, STORED, STRING, TEXT,
    };
    use crate::tokenizer::{
        LowerCaser, SimpleTokenizer, StopWordFilter, TextAnalyzer, TokenizerManager,
    };
    use crate::Index;

    fn make_schema() -> Schema {
        let mut schema_builder = Schema::builder();
        let text_field_indexing = TextFieldIndexing::default()
            .set_tokenizer("en_with_stop_words")
            .set_index_option(IndexRecordOption::WithFreqsAndPositions);
        let text_options = TextOptions::default()
            .set_indexing_options(text_field_indexing)
            .set_stored();
        schema_builder.add_text_field("title", TEXT);
        schema_builder.add_text_field("text", TEXT);
        schema_builder.add_i64_field("signed", INDEXED);
        schema_builder.add_u64_field("unsigned", INDEXED);
        schema_builder.add_text_field("notindexed_text", STORED);
        schema_builder.add_text_field("notindexed_u64", STORED);
        schema_builder.add_text_field("notindexed_i64", STORED);
        schema_builder.add_text_field("nottokenized", STRING);
        schema_builder.add_text_field("with_stop_words", text_options);
        schema_builder.add_date_field("date", INDEXED);
        schema_builder.add_f64_field("float", INDEXED);
        schema_builder.add_facet_field("facet", FacetOptions::default());
        schema_builder.add_bytes_field("bytes", INDEXED);
        schema_builder.add_bytes_field("bytes_not_indexed", STORED);
        schema_builder.add_json_field("json", TEXT);
        schema_builder.add_json_field("json_not_indexed", STORED);
        schema_builder.add_bool_field("bool", INDEXED);
        schema_builder.add_bool_field("notindexed_bool", STORED);
        schema_builder.add_u64_field("u64_ff", FAST);
        schema_builder.build()
    }

    fn make_query_parser_with_default_fields(default_fields: &[&'static str]) -> QueryParser {
        let schema = make_schema();
        let default_fields: Vec<Field> = default_fields
            .iter()
            .flat_map(|field_name| schema.get_field(field_name))
            .collect();
        let tokenizer_manager = TokenizerManager::default();
        tokenizer_manager.register(
            "en_with_stop_words",
            TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(LowerCaser)
                .filter(StopWordFilter::remove(vec!["the".to_string()]))
                .build(),
        );
        QueryParser::new(schema, default_fields, tokenizer_manager)
    }

    fn make_query_parser() -> QueryParser {
        make_query_parser_with_default_fields(&["title", "text"])
    }

    fn parse_query_to_logical_ast_with_default_fields(
        query: &str,
        default_conjunction: bool,
        default_fields: &[&'static str],
        allow_regexes: bool,
    ) -> Result<LogicalAst, QueryParserError> {
        let mut query_parser = make_query_parser_with_default_fields(default_fields);
        if default_conjunction {
            query_parser.set_conjunction_by_default();
        }
        if allow_regexes {
            query_parser.allow_regexes();
        }
        query_parser.parse_query_to_logical_ast(query)
    }

    fn parse_query_to_logical_ast(
        query: &str,
        default_conjunction: bool,
    ) -> Result<LogicalAst, QueryParserError> {
        parse_query_to_logical_ast_with_default_fields(
            query,
            default_conjunction,
            &["title", "text"],
            true,
        )
    }

    #[track_caller]
    fn test_parse_query_to_logical_ast_helper_with_default_fields(
        query: &str,
        expected: &str,
        default_conjunction: bool,
        default_fields: &[&'static str],
    ) {
        let query = parse_query_to_logical_ast_with_default_fields(
            query,
            default_conjunction,
            default_fields,
            true,
        )
        .unwrap();
        let query_str = format!("{query:?}");
        assert_eq!(query_str, expected);
    }

    #[track_caller]
    fn test_parse_query_to_logical_ast_helper(
        query: &str,
        expected: &str,
        default_conjunction: bool,
    ) {
        test_parse_query_to_logical_ast_helper_with_default_fields(
            query,
            expected,
            default_conjunction,
            &["title", "text"],
        )
    }

    #[test]
    pub fn test_parse_query_facet() {
        let query_parser = make_query_parser();
        let query = query_parser.parse_query("facet:/root/branch/leaf").unwrap();
        assert_eq!(
            format!("{query:?}"),
            r#"TermQuery(Term(field=11, type=Facet, Facet(/root/branch/leaf)))"#
        );
    }

    #[test]
    pub fn test_parse_query_with_boost() {
        let mut query_parser = make_query_parser();
        let schema = make_schema();
        let text_field = schema.get_field("text").unwrap();
        query_parser.set_field_boost(text_field, 2.0);
        let query = query_parser.parse_query("text:hello").unwrap();
        assert_eq!(
            format!("{query:?}"),
            r#"Boost(query=TermQuery(Term(field=1, type=Str, "hello")), boost=2)"#
        );
    }

    #[test]
    pub fn test_parse_query_range_with_boost() {
        let query = make_query_parser().parse_query("title:[A TO B]").unwrap();
        assert_eq!(
            format!("{query:?}"),
            "RangeQuery { bounds: BoundsRange { lower_bound: Included(Term(field=0, type=Str, \
             \"a\")), upper_bound: Included(Term(field=0, type=Str, \"b\")) } }"
        );
    }

    #[test]
    pub fn test_parse_query_with_default_boost_and_custom_boost() {
        let mut query_parser = make_query_parser();
        let schema = make_schema();
        let text_field = schema.get_field("text").unwrap();
        query_parser.set_field_boost(text_field, 2.0);
        let query = query_parser.parse_query("text:hello^2").unwrap();
        assert_eq!(
            format!("{query:?}"),
            r#"Boost(query=Boost(query=TermQuery(Term(field=1, type=Str, "hello")), boost=2), boost=2)"#
        );
    }

    #[test]
    pub fn test_parse_nonindexed_field_yields_error() {
        let query_parser = make_query_parser();

        let is_not_indexed_err = |query: &str| {
            let result: Result<Box<dyn Query>, QueryParserError> = query_parser.parse_query(query);
            if let Err(QueryParserError::FieldNotIndexed(field_name)) = result {
                Some(field_name)
            } else {
                None
            }
        };

        assert_eq!(
            is_not_indexed_err("notindexed_text:titi"),
            Some(String::from("notindexed_text"))
        );
        assert_eq!(
            is_not_indexed_err("notindexed_u64:23424"),
            Some(String::from("notindexed_u64"))
        );
        assert_eq!(
            is_not_indexed_err("notindexed_i64:-234324"),
            Some(String::from("notindexed_i64"))
        );
        assert_eq!(
            is_not_indexed_err("notindexed_bool:true"),
            Some(String::from("notindexed_bool"))
        );
    }

    #[test]
    pub fn test_parse_query_untokenized() {
        test_parse_query_to_logical_ast_helper(
            "nottokenized:\"wordone wordtwo\"",
            r#"Term(field=7, type=Str, "wordone wordtwo")"#,
            false,
        );
    }

    #[test]
    pub fn test_parse_query_empty() {
        test_parse_query_to_logical_ast_helper("", "<emptyclause>", false);
        test_parse_query_to_logical_ast_helper(" ", "<emptyclause>", false);
        let query_parser = make_query_parser();
        let query_result = query_parser.parse_query("");
        let query = query_result.unwrap();
        assert_eq!(format!("{query:?}"), "EmptyQuery");
    }

    #[test]
    pub fn test_parse_query_ints() {
        let query_parser = make_query_parser();
        assert!(query_parser.parse_query("signed:2324").is_ok());
        assert!(query_parser.parse_query("signed:\"22\"").is_ok());
        assert!(query_parser.parse_query("signed:\"-2234\"").is_ok());
        assert!(query_parser
            .parse_query("signed:\"-9999999999999\"")
            .is_ok());
        assert!(query_parser.parse_query("signed:\"a\"").is_err());
        assert!(query_parser.parse_query("signed:\"2a\"").is_err());
        assert!(query_parser
            .parse_query("signed:\"18446744073709551615\"")
            .is_err());
        assert!(query_parser.parse_query("unsigned:\"2\"").is_ok());
        assert!(query_parser.parse_query("unsigned:\"-2\"").is_err());
        assert!(query_parser
            .parse_query("unsigned:\"18446744073709551615\"")
            .is_ok());
        assert!(query_parser.parse_query("float:\"3.1\"").is_ok());
        assert!(query_parser.parse_query("float:\"-2.4\"").is_ok());
        assert!(query_parser.parse_query("float:\"2.1.2\"").is_err());
        assert!(query_parser.parse_query("float:\"2.1a\"").is_err());
        assert!(query_parser
            .parse_query("float:\"18446744073709551615.0\"")
            .is_ok());
        test_parse_query_to_logical_ast_helper(
            "unsigned:2324",
            "Term(field=3, type=U64, 2324)",
            false,
        );

        test_parse_query_to_logical_ast_helper(
            "signed:-2324",
            &format!(
                "{:?}",
                Term::from_field_i64(Field::from_field_id(2u32), -2324)
            ),
            false,
        );

        test_parse_query_to_logical_ast_helper(
            "float:2.5",
            &format!(
                "{:?}",
                Term::from_field_f64(Field::from_field_id(10u32), 2.5)
            ),
            false,
        );
    }

    #[test]
    fn test_parse_bytes() {
        test_parse_query_to_logical_ast_helper(
            "bytes:YnVidQ==",
            "Term(field=12, type=Bytes, [98, 117, 98, 117])",
            false,
        );
    }

    #[test]
    fn test_parse_bool() {
        test_parse_query_to_logical_ast_helper(
            "bool:true",
            &format!(
                "{:?}",
                Term::from_field_bool(Field::from_field_id(16u32), true),
            ),
            false,
        );
    }

    #[test]
    fn test_parse_bytes_not_indexed() {
        let error = parse_query_to_logical_ast("bytes_not_indexed:aaa", false).unwrap_err();
        assert!(matches!(error, QueryParserError::FieldNotIndexed(_)));
    }

    #[test]
    fn test_json_field() {
        test_parse_query_to_logical_ast_helper(
            "json.titi:hello",
            "Term(field=14, type=Json, path=titi, type=Str, \"hello\")",
            false,
        );
    }

    fn extract_query_term_json_path(query: &str) -> String {
        let LogicalAst::Leaf(literal) = parse_query_to_logical_ast(query, false).unwrap() else {
            panic!();
        };
        let LogicalLiteral::Term(term) = *literal else {
            panic!();
        };
        std::str::from_utf8(term.serialized_value_bytes())
            .unwrap()
            .to_string()
    }

    #[test]
    fn test_json_field_query_with_escaped_dot() {
        assert_eq!(
            extract_query_term_json_path(r#"json.k8s.node.name:hello"#),
            "k8s\u{1}node\u{1}name\0shello"
        );
        assert_eq!(
            extract_query_term_json_path(r"json.k8s\.node\.name:hello"),
            "k8s.node.name\0shello"
        );
    }

    #[test]
    fn test_json_field_possibly_a_number() {
        test_parse_query_to_logical_ast_helper(
            "json.titi:5",
            r#"(Term(field=14, type=Json, path=titi, type=I64, 5) Term(field=14, type=Json, path=titi, type=Str, "5"))"#,
            true,
        );
        test_parse_query_to_logical_ast_helper(
            "json.titi:-5",
            r#"(Term(field=14, type=Json, path=titi, type=I64, -5) Term(field=14, type=Json, path=titi, type=Str, "5"))"#, //< Yes this is a bit weird after going through the tokenizer we lose the "-".
            true,
        );
        test_parse_query_to_logical_ast_helper(
            "json.titi:10000000000000000000",
            r#"(Term(field=14, type=Json, path=titi, type=U64, 10000000000000000000) Term(field=14, type=Json, path=titi, type=Str, "10000000000000000000"))"#,
            true,
        );
        test_parse_query_to_logical_ast_helper(
            "json.titi:-5.2",
            r#"(Term(field=14, type=Json, path=titi, type=F64, -5.2) "[(0, Term(field=14, type=Json, path=titi, type=Str, "5")), (1, Term(field=14, type=Json, path=titi, type=Str, "2"))]")"#,
            true,
        );
    }

    #[test]
    fn test_json_field_possibly_a_date() {
        test_parse_query_to_logical_ast_helper(
            r#"json.date:"2019-10-12T07:20:50.52Z""#,
            r#"(Term(field=14, type=Json, path=date, type=Date, 2019-10-12T07:20:50Z) "[(0, Term(field=14, type=Json, path=date, type=Str, "2019")), (1, Term(field=14, type=Json, path=date, type=Str, "10")), (2, Term(field=14, type=Json, path=date, type=Str, "12t07")), (3, Term(field=14, type=Json, path=date, type=Str, "20")), (4, Term(field=14, type=Json, path=date, type=Str, "50")), (5, Term(field=14, type=Json, path=date, type=Str, "52z"))]")"#,
            true,
        );
    }

    #[test]
    fn test_json_field_possibly_a_bool() {
        test_parse_query_to_logical_ast_helper(
            "json.titi:true",
            r#"(Term(field=14, type=Json, path=titi, type=Bool, true) Term(field=14, type=Json, path=titi, type=Str, "true"))"#,
            true,
        );
    }

    #[test]
    fn test_json_field_not_indexed() {
        let error = parse_query_to_logical_ast("json_not_indexed.titi:hello", false).unwrap_err();
        assert!(matches!(error, QueryParserError::FieldNotIndexed(_)));
    }

    fn test_query_to_logical_ast_with_default_json(
        query: &str,
        expected: &str,
        default_conjunction: bool,
    ) {
        let mut query_parser = make_query_parser_with_default_fields(&["json"]);
        if default_conjunction {
            query_parser.set_conjunction_by_default();
        }
        let ast = query_parser.parse_query_to_logical_ast(query).unwrap();
        let ast_str = format!("{ast:?}");
        assert_eq!(ast_str, expected);
    }

    #[test]
    fn test_json_default() {
        test_query_to_logical_ast_with_default_json(
            "titi:4",
            "(Term(field=14, type=Json, path=titi, type=I64, 4) Term(field=14, type=Json, \
             path=titi, type=Str, \"4\"))",
            false,
        );
    }

    #[test]
    fn test_json_default_with_different_field() {
        for conjunction in [false, true] {
            test_query_to_logical_ast_with_default_json(
                "text:4",
                r#"Term(field=1, type=Str, "4")"#,
                conjunction,
            );
        }
    }

    #[test]
    fn test_json_default_with_same_field() {
        for conjunction in [false, true] {
            test_query_to_logical_ast_with_default_json(
                "json:4",
                r#"(Term(field=14, type=Json, path=, type=I64, 4) Term(field=14, type=Json, path=, type=Str, "4"))"#,
                conjunction,
            );
        }
    }

    #[test]
    fn test_parse_bytes_phrase() {
        test_parse_query_to_logical_ast_helper(
            "bytes:\"YnVidQ==\"",
            "Term(field=12, type=Bytes, [98, 117, 98, 117])",
            false,
        );
    }

    #[test]
    fn test_parse_bytes_invalid_base64() {
        let base64_err: QueryParserError =
            parse_query_to_logical_ast("bytes:aa", false).unwrap_err();
        assert!(matches!(base64_err, QueryParserError::ExpectedBase64(_)));
    }

    #[test]
    fn test_parse_query_to_ast_ab_c() {
        test_parse_query_to_logical_ast_helper(
            "(+title:a +title:b) title:c",
            r#"((+Term(field=0, type=Str, "a") +Term(field=0, type=Str, "b")) Term(field=0, type=Str, "c"))"#,
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "(+title:a +title:b) title:c",
            r#"(+Term(field=0, type=Str, "a") +Term(field=0, type=Str, "b") +Term(field=0, type=Str, "c"))"#,
            true,
        );
    }

    #[test]
    pub fn test_parse_query_to_ast_single_term() {
        test_parse_query_to_logical_ast_helper(
            "title:toto",
            r#"Term(field=0, type=Str, "toto")"#,
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "+title:toto",
            r#"Term(field=0, type=Str, "toto")"#,
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "+title:toto -titi",
            r#"(+Term(field=0, type=Str, "toto") -(Term(field=0, type=Str, "titi") Term(field=1, type=Str, "titi")))"#,
            false,
        );
    }

    #[test]
    fn test_single_negative_term() {
        assert_matches!(
            parse_query_to_logical_ast("-title:toto", false),
            Err(QueryParserError::AllButQueryForbidden)
        );
    }

    #[test]
    pub fn test_parse_query_to_ast_two_terms() {
        test_parse_query_to_logical_ast_helper(
            "title:a b",
            r#"(Term(field=0, type=Str, "a") Term(field=0, type=Str, "b") Term(field=1, type=Str, "b"))"#,
            false,
        );
        test_parse_query_to_logical_ast_helper(
            r#"title:"a b""#,
            r#""[(0, Term(field=0, type=Str, "a")), (1, Term(field=0, type=Str, "b"))]""#,
            false,
        );
    }
    #[test]
    pub fn test_parse_query_all_query() {
        let logical_ast = parse_query_to_logical_ast("*", false).unwrap();
        assert_eq!(format!("{logical_ast:?}"), "*");
    }

    #[test]
    pub fn test_parse_query_range_require_a_target_field() {
        let query_parser_error = parse_query_to_logical_ast("[A TO B]", false).err().unwrap();
        assert_eq!(
            query_parser_error.to_string(),
            "Unsupported query: Range query need to target a specific field."
        );
    }

    #[test]
    pub fn test_parse_query_to_ast_ranges() {
        test_parse_query_to_logical_ast_helper(
            "title:[a TO b]",
            r#"(Included(Term(field=0, type=Str, "a")) TO Included(Term(field=0, type=Str, "b")))"#,
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "title:{titi TO toto}",
            r#"(Excluded(Term(field=0, type=Str, "titi")) TO Excluded(Term(field=0, type=Str, "toto")))"#,
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "title:{* TO toto}",
            r#"(Unbounded TO Excluded(Term(field=0, type=Str, "toto")))"#,
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "title:{titi TO *}",
            r#"(Excluded(Term(field=0, type=Str, "titi")) TO Unbounded)"#,
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "signed:{-5 TO 3}",
            r#"(Excluded(Term(field=2, type=I64, -5)) TO Excluded(Term(field=2, type=I64, 3)))"#,
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "float:{-1.5 TO 1.5}",
            r#"(Excluded(Term(field=10, type=F64, -1.5)) TO Excluded(Term(field=10, type=F64, 1.5)))"#,
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "u64_ff:[7 TO 77]",
            r#"(Included(Term(field=18, type=U64, 7)) TO Included(Term(field=18, type=U64, 77)))"#,
            false,
        );
    }

    #[test]
    pub fn test_query_parser_field_does_not_exist() {
        let query_parser = make_query_parser();
        assert_eq!(
            query_parser
                .parse_query("boujou:\"18446744073709551615\"")
                .unwrap_err(),
            QueryParserError::FieldDoesNotExist("boujou".to_string())
        );
    }

    #[test]
    pub fn test_query_parser_field_not_indexed() {
        let query_parser = make_query_parser();
        assert_matches!(
            query_parser.parse_query("notindexed_text:\"18446744073709551615\""),
            Err(QueryParserError::FieldNotIndexed(_))
        );
    }

    #[test]
    pub fn test_unknown_tokenizer() {
        let mut schema_builder = Schema::builder();
        let text_field_indexing = TextFieldIndexing::default()
            .set_tokenizer("nonexistingtokenizer")
            .set_index_option(IndexRecordOption::Basic);
        let text_options = TextOptions::default().set_indexing_options(text_field_indexing);
        let title = schema_builder.add_text_field("title", text_options);
        let schema = schema_builder.build();
        let default_fields = vec![title];
        let tokenizer_manager = TokenizerManager::default();
        let query_parser = QueryParser::new(schema, default_fields, tokenizer_manager);

        assert_matches!(
            query_parser.parse_query("title:\"happy tax payer\""),
            Err(QueryParserError::UnknownTokenizer { .. })
        );
    }

    #[test]
    pub fn test_query_parser_no_positions() {
        let mut schema_builder = Schema::builder();
        let text_field_indexing = TextFieldIndexing::default()
            .set_tokenizer("customtokenizer")
            .set_index_option(IndexRecordOption::Basic);
        let text_options = TextOptions::default().set_indexing_options(text_field_indexing);
        let title = schema_builder.add_text_field("title", text_options);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        index
            .tokenizers()
            .register("customtokenizer", SimpleTokenizer::default());
        let query_parser = QueryParser::for_index(&index, vec![title]);
        assert_eq!(
            query_parser.parse_query("title:\"happy tax\"").unwrap_err(),
            QueryParserError::FieldDoesNotHavePositionsIndexed("title".to_string())
        );
    }

    #[test]
    pub fn test_query_parser_expected_int() {
        let query_parser = make_query_parser();
        assert_matches!(
            query_parser.parse_query("unsigned:18a"),
            Err(QueryParserError::ExpectedInt(_))
        );
        assert!(query_parser.parse_query("unsigned:\"18\"").is_ok());
        assert_matches!(
            query_parser.parse_query("signed:18b"),
            Err(QueryParserError::ExpectedInt(_))
        );
        assert!(query_parser.parse_query("float:\"1.8\"").is_ok());
        assert_matches!(
            query_parser.parse_query("float:1.8a"),
            Err(QueryParserError::ExpectedFloat(_))
        );
    }

    #[test]
    pub fn test_query_parser_expected_bool() {
        let query_parser = make_query_parser();
        assert_matches!(
            query_parser.parse_query("bool:brie"),
            Err(QueryParserError::ExpectedBool(_))
        );
        assert!(query_parser.parse_query("bool:\"true\"").is_ok());
        assert!(query_parser.parse_query("bool:\"false\"").is_ok());
    }

    #[test]
    pub fn test_query_parser_expected_date() {
        let query_parser = make_query_parser();
        assert_matches!(
            query_parser.parse_query("date:18a"),
            Err(QueryParserError::DateFormatError(_))
        );
        test_parse_query_to_logical_ast_helper(
            r#"date:"2010-11-21T09:55:06.000000000+02:00""#,
            r#"Term(field=9, type=Date, 2010-11-21T07:55:06Z)"#,
            true,
        );
        test_parse_query_to_logical_ast_helper(
            r#"date:"1985-04-12T23:20:50.52Z""#,
            r#"Term(field=9, type=Date, 1985-04-12T23:20:50Z)"#,
            true,
        );
    }

    #[test]
    pub fn test_query_parser_expected_facet() {
        let query_parser = make_query_parser();
        match query_parser.parse_query("facet:INVALID") {
            Ok(_) => panic!("should never succeed"),
            Err(e) => assert_eq!(
                "The facet field is malformed: Failed to parse the facet string: 'INVALID'",
                format!("{e}")
            ),
        }
        assert!(query_parser.parse_query("facet:\"/foo/bar\"").is_ok());
    }

    #[test]
    pub fn test_query_parser_not_empty_but_no_tokens() {
        let query_parser = make_query_parser();
        assert!(query_parser.parse_query(" !, ").is_ok());
        assert!(query_parser.parse_query("with_stop_words:the").is_ok());
    }

    #[test]
    pub fn test_parse_query_single_negative_term_through_error() {
        assert_matches!(
            parse_query_to_logical_ast("-title:toto", true),
            Err(QueryParserError::AllButQueryForbidden)
        );
        assert_matches!(
            parse_query_to_logical_ast("-title:toto", false),
            Err(QueryParserError::AllButQueryForbidden)
        );
    }

    #[test]
    pub fn test_parse_query_to_ast_conjunction() {
        test_parse_query_to_logical_ast_helper(
            "title:toto",
            r#"Term(field=0, type=Str, "toto")"#,
            true,
        );
        test_parse_query_to_logical_ast_helper(
            "+title:toto",
            r#"Term(field=0, type=Str, "toto")"#,
            true,
        );
        test_parse_query_to_logical_ast_helper(
            "+title:toto -titi",
            r#"(+Term(field=0, type=Str, "toto") -(Term(field=0, type=Str, "titi") Term(field=1, type=Str, "titi")))"#,
            true,
        );
        test_parse_query_to_logical_ast_helper(
            "title:a b",
            r#"(+Term(field=0, type=Str, "a") +(Term(field=0, type=Str, "b") Term(field=1, type=Str, "b")))"#,
            true,
        );
        test_parse_query_to_logical_ast_helper(
            "title:\"a b\"",
            r#""[(0, Term(field=0, type=Str, "a")), (1, Term(field=0, type=Str, "b"))]""#,
            true,
        );
    }

    #[test]
    pub fn test_parse_query_negative() {
        test_parse_query_to_logical_ast_helper(
            "title:b -title:a",
            r#"(+Term(field=0, type=Str, "b") -Term(field=0, type=Str, "a"))"#,
            true,
        );

        test_parse_query_to_logical_ast_helper(
            "title:b -(-title:a -title:c)",
            r#"(+Term(field=0, type=Str, "b") -(-Term(field=0, type=Str, "a") -Term(field=0, type=Str, "c")))"#,
            true,
        );
    }

    #[test]
    pub fn test_query_parser_hyphen() {
        test_parse_query_to_logical_ast_helper(
            "title:www-form-encoded",
            r#""[(0, Term(field=0, type=Str, "www")), (1, Term(field=0, type=Str, "form")), (2, Term(field=0, type=Str, "encoded"))]""#,
            false,
        );
    }

    #[test]
    fn test_and_default_regardless_of_default_conjunctive() {
        for &default_conjunction in &[false, true] {
            test_parse_query_to_logical_ast_helper(
                "title:a AND title:b",
                r#"(+Term(field=0, type=Str, "a") +Term(field=0, type=Str, "b"))"#,
                default_conjunction,
            );
        }
    }

    #[test]
    fn test_or_default_conjunctive() {
        for &default_conjunction in &[false, true] {
            test_parse_query_to_logical_ast_helper(
                "title:a OR title:b",
                r#"(Term(field=0, type=Str, "a") Term(field=0, type=Str, "b"))"#,
                default_conjunction,
            );
        }
    }

    #[test]
    fn test_space_before_value() {
        test_parse_query_to_logical_ast_helper(
            "title: a",
            r#"Term(field=0, type=Str, "a")"#,
            false,
        );
    }

    #[test]
    fn test_escaped_field() {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field(r"a\.b", STRING);
        let schema = schema_builder.build();
        let query_parser = QueryParser::new(schema, Vec::new(), TokenizerManager::default());
        let query = query_parser.parse_query(r"a\.b:hello").unwrap();
        assert_eq!(
            format!("{query:?}"),
            "TermQuery(Term(field=0, type=Str, \"hello\"))"
        );
    }

    #[test]
    fn test_split_full_path() {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("second", STRING);
        schema_builder.add_text_field("first", STRING);
        schema_builder.add_text_field("first.toto", STRING);
        schema_builder.add_text_field("first.toto.titi", STRING);
        schema_builder.add_text_field("third.a.b.c", STRING);
        let schema = schema_builder.build();
        let query_parser =
            QueryParser::new(schema.clone(), Vec::new(), TokenizerManager::default());
        assert_eq!(
            query_parser.split_full_path("first.toto"),
            Some((schema.get_field("first.toto").unwrap(), ""))
        );
        assert_eq!(
            query_parser.split_full_path("first.toto.bubu"),
            Some((schema.get_field("first.toto").unwrap(), "bubu"))
        );
        assert_eq!(
            query_parser.split_full_path("first.toto.titi"),
            Some((schema.get_field("first.toto.titi").unwrap(), ""))
        );
        assert_eq!(
            query_parser.split_full_path("first.titi"),
            Some((schema.get_field("first").unwrap(), "titi"))
        );
        assert_eq!(query_parser.split_full_path("third"), None);
        assert_eq!(query_parser.split_full_path("hello.toto"), None);
        assert_eq!(query_parser.split_full_path(""), None);
        assert_eq!(query_parser.split_full_path("firsty"), None);
    }

    #[test]
    pub fn test_phrase_slop() {
        test_parse_query_to_logical_ast_helper(
            "\"a b\"~0",
            r#"("[(0, Term(field=0, type=Str, "a")), (1, Term(field=0, type=Str, "b"))]" "[(0, Term(field=1, type=Str, "a")), (1, Term(field=1, type=Str, "b"))]")"#,
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "\"a b\"~2",
            r#"("[(0, Term(field=0, type=Str, "a")), (1, Term(field=0, type=Str, "b"))]"~2 "[(0, Term(field=1, type=Str, "a")), (1, Term(field=1, type=Str, "b"))]"~2)"#,
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "title:\"a b~4\"~2",
            r#""[(0, Term(field=0, type=Str, "a")), (1, Term(field=0, type=Str, "b")), (2, Term(field=0, type=Str, "4"))]"~2"#,
            false,
        );
    }

    #[test]
    pub fn test_phrase_prefix() {
        test_parse_query_to_logical_ast_helper(
            "\"big bad wo\"*",
            r#"("[(0, Term(field=0, type=Str, "big")), (1, Term(field=0, type=Str, "bad")), (2, Term(field=0, type=Str, "wo"))]"* "[(0, Term(field=1, type=Str, "big")), (1, Term(field=1, type=Str, "bad")), (2, Term(field=1, type=Str, "wo"))]"*)"#,
            false,
        );

        let query_parser = make_query_parser();
        let query = query_parser.parse_query("\"big bad wo\"*").unwrap();
        assert_eq!(
            format!("{query:?}"),
            "BooleanQuery { subqueries: [(Should, PhrasePrefixQuery { field: Field(0), \
             phrase_terms: [(0, Term(field=0, type=Str, \"big\")), (1, Term(field=0, type=Str, \
             \"bad\"))], prefix: (2, Term(field=0, type=Str, \"wo\")), max_expansions: 50 }), \
             (Should, PhrasePrefixQuery { field: Field(1), phrase_terms: [(0, Term(field=1, \
             type=Str, \"big\")), (1, Term(field=1, type=Str, \"bad\"))], prefix: (2, \
             Term(field=1, type=Str, \"wo\")), max_expansions: 50 })], \
             minimum_number_should_match: 1 }"
        );
    }

    #[test]
    pub fn test_phrase_prefix_too_short() {
        let err = parse_query_to_logical_ast("\"wo\"*", true).unwrap_err();
        assert_eq!(
            err,
            QueryParserError::PhrasePrefixRequiresAtLeastTwoTerms {
                phrase: "wo".to_owned(),
                tokenizer: "default".to_owned()
            }
        );

        let err = parse_query_to_logical_ast("\"\"*", true).unwrap_err();
        assert_eq!(
            err,
            QueryParserError::PhrasePrefixRequiresAtLeastTwoTerms {
                phrase: "".to_owned(),
                tokenizer: "default".to_owned()
            }
        );
    }

    #[test]
    pub fn test_term_set_query() {
        test_parse_query_to_logical_ast_helper(
            "title: IN [a b cd]",
            r#"IN [Term(field=0, type=Str, "a"), Term(field=0, type=Str, "b"), Term(field=0, type=Str, "cd")]"#,
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "bytes: IN [AA== ABA= ABCD]",
            r#"IN [Term(field=12, type=Bytes, [0]), Term(field=12, type=Bytes, [0, 16]), Term(field=12, type=Bytes, [0, 16, 131])]"#,
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "signed: IN [1 2 -3]",
            r#"IN [Term(field=2, type=I64, 1), Term(field=2, type=I64, 2), Term(field=2, type=I64, -3)]"#,
            false,
        );

        test_parse_query_to_logical_ast_helper(
            "float: IN [1.1 2.2 -3.3]",
            r#"IN [Term(field=10, type=F64, 1.1), Term(field=10, type=F64, 2.2), Term(field=10, type=F64, -3.3)]"#,
            false,
        );
    }

    #[test]
    pub fn test_set_field_fuzzy() {
        {
            let mut query_parser = make_query_parser();
            query_parser.set_field_fuzzy(
                query_parser.schema.get_field("title").unwrap(),
                false,
                1,
                true,
            );
            let query = query_parser.parse_query("abc").unwrap();
            assert_eq!(
                format!("{query:?}"),
                "BooleanQuery { subqueries: [(Should, FuzzyTermQuery { term: Term(field=0, \
                 type=Str, \"abc\"), distance: 1, transposition_cost_one: true, prefix: false }), \
                 (Should, TermQuery(Term(field=1, type=Str, \"abc\")))], \
                 minimum_number_should_match: 1 }"
            );
        }

        {
            let mut query_parser = make_query_parser();
            query_parser.set_field_fuzzy(
                query_parser.schema.get_field("text").unwrap(),
                true,
                2,
                false,
            );
            let query = query_parser.parse_query("abc").unwrap();
            assert_eq!(
                format!("{query:?}"),
                "BooleanQuery { subqueries: [(Should, TermQuery(Term(field=0, type=Str, \
                 \"abc\"))), (Should, FuzzyTermQuery { term: Term(field=1, type=Str, \"abc\"), \
                 distance: 2, transposition_cost_one: false, prefix: true })], \
                 minimum_number_should_match: 1 }"
            );
        }
    }

    #[test]
    pub fn test_set_default_field_integer() {
        test_parse_query_to_logical_ast_helper_with_default_fields(
            "2324",
            "(Term(field=0, type=Str, \"2324\") Term(field=2, type=I64, 2324))",
            false,
            &["title", "signed"],
        );

        test_parse_query_to_logical_ast_helper_with_default_fields(
            "abc",
            "Term(field=0, type=Str, \"abc\")",
            false,
            &["title", "signed"],
        );

        let query_parser = make_query_parser_with_default_fields(&["signed"]);
        assert_matches!(
            query_parser.parse_query("abc"),
            Err(QueryParserError::ExpectedInt(_))
        );
    }

    #[test]
    pub fn test_deduplication() {
        let query = "be be";
        test_parse_query_to_logical_ast_helper(
            query,
            "(Term(field=0, type=Str, \"be\") Term(field=1, type=Str, \"be\"))",
            false,
        );
    }

    #[test]
    pub fn test_regex() {
        let expected_regex = tantivy_fst::Regex::new(r".*b").unwrap();
        test_parse_query_to_logical_ast_helper(
            "title:/.*b/",
            format!("Regex(Field(0), {:#?})", expected_regex).as_str(),
            false,
        );
        let expected_regex2 = tantivy_fst::Regex::new(r".*a").unwrap();
        test_parse_query_to_logical_ast_helper(
            "title:(/.*b/ OR /.*a/)",
            format!(
                "(Regex(Field(0), {:#?}) Regex(Field(0), {:#?}))",
                expected_regex, expected_regex2
            )
            .as_str(),
            false,
        );

        // Invalid field
        let err = parse_query_to_logical_ast("float:/.*b/", false).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Unsupported query: Regex query only supported on text fields"
        );

        // No field specified
        let err = parse_query_to_logical_ast("/.*b/", false).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Unsupported query: Regex query need to target a specific field."
        );

        // Regex on a json path
        let err = parse_query_to_logical_ast("title.subpath:/.*b/", false).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Unsupported query: Regex query does not support json paths."
        );

        // Invalid regex
        let err = parse_query_to_logical_ast("title:/[A-Z*b/", false).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Unsupported query: Invalid regex: regex parse error:\n    [A-Z*b\n    ^\nerror: \
             unclosed character class"
        );

        // Regexes not allowed
        let err = parse_query_to_logical_ast_with_default_fields(
            "title:/.*b/",
            false,
            &["title", "text"],
            false,
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Unsupported query: Regex queries are not allowed."
        );
    }
}
