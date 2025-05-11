use chumsky::prelude::*;
use rusqlite::types::Value;

pub(super) fn integer<'src>() -> impl Parser<'src, &'src str, i64, extra::Err<Rich<'src, char>>> {
    let number = just('-')
        .or_not()
        .then(text::int(10))
        .to_slice()
        .map(|s: &str| s.parse().unwrap())
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
}

pub(super) fn value_parser<'src>(
) -> impl Parser<'src, &'src str, Value, extra::Err<Rich<'src, char>>> {
    let boolean = choice((
        just("true").to(Value::Integer(1)),
        just("false").to(Value::Integer(0)),
    ))
    .boxed();

    choice((
        boolean,
        real().map(|f| Value::Real(f)),
        integer().map(|i| Value::Integer(i)),
        just("Null").to(Value::Null),
    ))
}

pub(super) fn sqlite_values_parser<'src>(
) -> impl Parser<'src, &'src str, Vec<Vec<Value>>, extra::Err<Rich<'src, char>>> {
    let value_parser = value_parser();

    let value_list = value_parser
        .separated_by(just(',').padded())
        .allow_trailing()
        .collect::<Vec<_>>()
        .delimited_by(just('[').padded(), just(']').padded())
        .boxed();

    let value_list_2d = value_list
        .separated_by(just(',').padded())
        .allow_trailing()
        .collect::<Vec<_>>()
        .delimited_by(just('[').padded(), just(']').padded())
        .boxed();

    choice((just("None").to(vec![vec![]]), value_list_2d))
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
}
