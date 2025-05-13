use chumsky::prelude::*;
use rusqlite::types::Value;

pub(super) fn integer<'src>() -> impl Parser<'src, &'src str, i64, extra::Err<Rich<'src, char>>> {
    let number = just('-')
        .or_not()
        .then(text::int(10))
        .to_slice()
        .map(|s: &str| s.parse().unwrap())
        .labelled("integer")
        .boxed();
    number
}

pub(super) fn real<'src>() -> impl Parser<'src, &'src str, f64, extra::Err<Rich<'src, char>>> {
    let digits = text::digits(10).to_slice();

    let frac = just('.').then(digits);

    let exp = just('e')
        .or(just('E'))
        .then(one_of("+-").or_not())
        .then(digits);

    let number = just('-')
        .or_not()
        .then(text::int(10))
        .then(frac)
        .then(exp.or_not())
        .to_slice()
        .map(|s: &str| s.parse().unwrap())
        .boxed();
    choice((
        number,
        just("Inf").to(core::f64::INFINITY),
        just("-Inf").to(core::f64::NEG_INFINITY),
    ))
    .labelled("real")
    .boxed()
}

pub(super) fn text<'src>(
) -> impl Parser<'src, &'src str, &'src str, extra::Err<Rich<'src, char>>> + Clone {
    let escape = just('\\')
        .then(choice((
            just('\\'),
            just('/'),
            just('"'),
            just('b').to('\x08'),
            just('f').to('\x0C'),
            just('n').to('\n'),
            just('r').to('\r'),
            just('t').to('\t'),
        )))
        .ignored()
        .boxed();

    none_of("\\\"")
        .ignored()
        .or(escape)
        .repeated()
        .to_slice()
        .delimited_by(just('"'), just('"'))
        .labelled("text")
        .boxed()
}

pub(super) fn blob<'src>() -> impl Parser<'src, &'src str, Vec<u8>, extra::Err<Rich<'src, char>>> {
    text::digits(16)
        .to_slice()
        .try_map(|b, span| hex::decode(b).map_err(|e| Rich::custom(span, e)))
        .delimited_by(just("x\""), just('"'))
        .labelled("blob")
        .boxed()
}

pub(super) fn value_parser<'src>(
) -> impl Parser<'src, &'src str, Value, extra::Err<Rich<'src, char>>> {
    let boolean = choice((
        just("true").to(Value::Integer(1)),
        just("false").to(Value::Integer(0)),
    ))
    .labelled("boolean")
    .boxed();

    choice((
        boolean,
        real().map(|f| Value::Real(f)),
        integer().map(|i| Value::Integer(i)),
        just("Null").to(Value::Null),
        text().map(|s| Value::Text(s.to_string())),
        blob().map(|b| Value::Blob(b)),
    ))
    .boxed()
}

pub(super) fn sqlite_values_parser<'src>(
) -> impl Parser<'src, &'src str, Vec<Vec<Value>>, extra::Err<Rich<'src, char>>> {
    let value = value_parser();

    let value_list = value
        .separated_by(just(',').padded())
        .allow_trailing()
        .collect::<Vec<_>>()
        .delimited_by(just('[').padded(), just(']').padded())
        .boxed();

    let value_list_2d = value_list
        .clone()
        .separated_by(just(',').padded())
        .allow_trailing()
        .collect::<Vec<_>>()
        .delimited_by(just('[').padded(), just(']').padded())
        .boxed();

    choice((
        just("None").to(vec![vec![]]),
        value_parser().map(|v| vec![vec![v]]),
        value_list.map(|v_list| vec![v_list]),
        value_list_2d,
    ))
}

#[cfg(test)]
mod tests {
    use chumsky::Parser;

    use crate::{
        assert_debug_snapshot_with_input,
        sqlite_values::{sqlite_values_parser, value_parser},
    };

    #[test]
    fn test_none_values() {
        let parser = sqlite_values_parser();
        let input = "None";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);
    }

    #[test]
    fn test_single_value() {
        let parser = sqlite_values_parser();
        let input = "Null";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let input = "true";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let input = "1";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let input = "1.0";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let input = r#""test""#;
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let input = r#"x"1240""#;
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);
    }

    #[test]
    fn test_single_row_value() {
        let parser = sqlite_values_parser();
        let input = "[Null, 1]";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let input = "[true, \"hi\"]";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let input = "[1, \"hi\", 1.0, x\"1240\"]";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let input = "[1]";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);
    }

    #[test]
    fn test_many_rows_value() {
        let parser = sqlite_values_parser();
        // Notice that it will parse the values here but it will not assert if the length
        // of each row is the same. This is possible to happen if we have to do different queries
        // that returns different number of cols.
        let input = "[[Null, 1], [true, \"hi\"], [1, \"hi\", 1.0, x\"1240\"], [1]]";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);
    }

    #[test]
    fn test_null_value() {
        let parser = value_parser();
        let input = "Null";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let input = "null";
        let _ = parser.parse(input).into_result().unwrap_err();
    }

    #[test]
    fn test_boolean_value() {
        let parser = value_parser();
        let input = "true";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let input = "false";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);
    }

    #[test]
    fn test_integer_value() {
        let parser = value_parser();
        let input = "1";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let input = "0";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let input = "-1";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let input = "-1000021900";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let input = "112343543009010293";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);
    }

    #[test]
    fn test_real_value() {
        let parser = value_parser();
        let input = "1.0";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let input = "0.0";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let input = "-1.0";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let input = "-1000021900.0";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let input = "112343543009010293.0";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let input = "123.3489534";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let input = "-123.3489534";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let input = "-123.3489534E5";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let input = "-123.3489534E-5";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let input = "Inf";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let input = "-Inf";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);
    }

    #[test]
    fn test_text_value() {
        let parser = value_parser();
        let input = r#""true""#;
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let input = r#""test""#;
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let input = r#""test\n""#;
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let input = "\"test\n\"";
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let input = r#"test\n""#;
        let _ = parser.parse(input).into_result().unwrap_err();
    }

    #[test]
    fn test_blob_value() {
        let parser = value_parser();
        let input = r#"x"1240""#;
        let val = parser.parse(input).unwrap();
        assert_debug_snapshot_with_input!(input, val);

        let parser = value_parser();
        // Odd number of digits
        let input = r#"x"124""#;
        let _ = parser.parse(input).into_result().unwrap_err();

        // Not Hex Digit
        let input = r#"x"12LKOPK""#;
        let _ = parser.parse(input).into_result().unwrap_err();
    }
}
