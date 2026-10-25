use super::*;
use tantivy::query::{BooleanQuery, BoostQuery, EmptyQuery, QueryParser, TermQuery};
use tantivy::query_grammar::{Delimiter, Occur, UserInputAst, UserInputLeaf, UserInputLiteral};

const MAX_PREFIX_EXPANSIONS: usize = 50;

pub(super) fn parse_query(
    text: &str,
    parser: &QueryParser,
    index: &Index,
    searcher: &Searcher,
    fields: &[Field],
    boosts: &[(Field, f32)],
) -> Result<Box<dyn Query>> {
    let (mut ast, errors) = tantivy::query_grammar::parse_query_lenient(text);
    if let Some(error) = errors.first() {
        return Err(LimboError::InternalError(format!(
            "FTS parse error: {error:?}"
        )));
    }
    let has_prefix = mark_prefixes(&mut ast);
    if !has_prefix {
        return build_query(parser, ast);
    }
    let mut validation_ast = ast.clone();
    remove_prefixes(&mut validation_ast);
    build_query(parser, validation_ast)?;
    Ok(expand_ast(ast, parser, index, searcher, fields, boosts)?
        .unwrap_or_else(|| Box::new(EmptyQuery)))
}

fn mark_prefixes(ast: &mut UserInputAst) -> bool {
    match ast {
        UserInputAst::Clause(children) => {
            let mut found = false;
            for (_, child) in children {
                found |= mark_prefixes(child);
            }
            found
        }
        UserInputAst::Boost(child, _) => mark_prefixes(child),
        UserInputAst::Leaf(leaf) => match leaf.as_mut() {
            UserInputLeaf::Literal(literal) => {
                if literal.delimiter == Delimiter::None
                    && literal.phrase.len() > 1
                    && literal.phrase.ends_with('*')
                    && !literal.phrase.ends_with("\\*")
                {
                    literal.phrase.pop();
                    literal.prefix = true;
                }
                literal.prefix
            }
            _ => false,
        },
    }
}

fn remove_prefixes(ast: &mut UserInputAst) {
    match ast {
        UserInputAst::Clause(children) => {
            for (_, child) in children {
                remove_prefixes(child);
            }
        }
        UserInputAst::Boost(child, _) => remove_prefixes(child),
        UserInputAst::Leaf(leaf) => {
            if let UserInputLeaf::Literal(literal) = leaf.as_mut() {
                literal.prefix = false;
            }
        }
    }
}

fn expand_ast(
    ast: UserInputAst,
    parser: &QueryParser,
    index: &Index,
    searcher: &Searcher,
    fields: &[Field],
    boosts: &[(Field, f32)],
) -> Result<Option<Box<dyn Query>>> {
    match ast {
        UserInputAst::Clause(children) => {
            let mut queries = Vec::new();
            for (occur, child) in children {
                if let Some(query) = expand_ast(child, parser, index, searcher, fields, boosts)? {
                    queries.push((occur.unwrap_or(Occur::Should), query));
                }
            }
            if queries.is_empty() {
                Ok(None)
            } else {
                Ok(Some(Box::new(BooleanQuery::new(queries))))
            }
        }
        UserInputAst::Boost(child, boost) => {
            let query = expand_ast(*child, parser, index, searcher, fields, boosts)?;
            let boost = boost.into_inner() as f32;
            if (boost - 1.0).abs() < f32::EPSILON {
                Ok(query)
            } else {
                Ok(Some(Box::new(BoostQuery::new(
                    query.unwrap_or_else(|| Box::new(EmptyQuery)),
                    boost,
                ))))
            }
        }
        UserInputAst::Leaf(leaf) => match *leaf {
            UserInputLeaf::Literal(literal) if literal.prefix => {
                expand_prefix(literal, parser, index, searcher, fields, boosts).map(Some)
            }
            leaf => {
                let query = build_query(parser, UserInputAst::Leaf(Box::new(leaf)))?;
                if query.as_any().is::<EmptyQuery>() {
                    Ok(None)
                } else {
                    Ok(Some(query))
                }
            }
        },
    }
}

fn expand_prefix(
    literal: UserInputLiteral,
    parser: &QueryParser,
    index: &Index,
    searcher: &Searcher,
    default_fields: &[Field],
    boosts: &[(Field, f32)],
) -> Result<Box<dyn Query>> {
    let schema = index.schema();
    let fields = match &literal.field_name {
        Some(name) => vec![schema
            .get_field(name)
            .map_err(|error| LimboError::InternalError(format!("FTS parse error: {error}")))?],
        None => default_fields.to_vec(),
    };
    let mut queries: Vec<(Occur, Box<dyn Query>)> = Vec::new();
    let mut expansion_count = 0;
    for field in fields {
        let mut tokenizer = index.tokenizer_for_field(field).map_err(|error| {
            LimboError::InternalError(format!("FTS prefix tokenizer error: {error}"))
        })?;
        let mut tokens = Vec::new();
        tokenizer
            .token_stream(&literal.phrase)
            .process(&mut |token| {
                tokens.push(token.text.clone());
            });
        if tokens.len() != 1 {
            let mut scoped = literal.clone();
            scoped.field_name = Some(schema.get_field_name(field).to_owned());
            queries.push((
                Occur::Should,
                build_query(
                    parser,
                    UserInputAst::Leaf(Box::new(UserInputLeaf::Literal(scoped))),
                )?,
            ));
            continue;
        }
        let prefix = tokens.pop().unwrap();
        let mut terms = BTreeSet::new();
        for segment in searcher.segment_readers() {
            let inverted = segment.inverted_index(field).map_err(|error| {
                LimboError::InternalError(format!("FTS prefix dictionary error: {error}"))
            })?;
            let mut stream = inverted
                .terms()
                .range()
                .ge(prefix.as_bytes())
                .into_stream()
                .map_err(|error| {
                    LimboError::InternalError(format!("FTS prefix dictionary error: {error}"))
                })?;
            while stream.advance() && stream.key().starts_with(prefix.as_bytes()) {
                if terms.insert(stream.key().to_vec()) {
                    expansion_count += 1;
                    if expansion_count > MAX_PREFIX_EXPANSIONS {
                        return Err(LimboError::InternalError(format!(
                            "FTS prefix expands to more than {MAX_PREFIX_EXPANSIONS} terms; use a longer prefix"
                        )));
                    }
                }
            }
        }
        let boost = boosts
            .iter()
            .find(|(f, _)| *f == field)
            .map_or(1.0, |(_, boost)| *boost);
        for bytes in terms {
            let mut term = Term::from_field_text(field, "");
            term.append_bytes(&bytes);
            let query: Box<dyn Query> =
                Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs));
            queries.push((Occur::Should, Box::new(BoostQuery::new(query, boost))));
        }
    }
    if queries.is_empty() {
        Ok(Box::new(EmptyQuery))
    } else {
        Ok(Box::new(BooleanQuery::new(queries)))
    }
}

fn build_query(parser: &QueryParser, ast: UserInputAst) -> Result<Box<dyn Query>> {
    let (query, errors) = parser.build_query_from_user_input_ast_lenient(ast);
    if let Some(error) = errors.first() {
        return Err(LimboError::InternalError(format!(
            "FTS parse error: {error:?}"
        )));
    }
    Ok(query)
}
