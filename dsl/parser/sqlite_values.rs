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

pub(super) fn value_parser<'src>(
) -> impl Parser<'src, &'src str, Value, extra::Err<Rich<'src, char>>> {
    let boolean = choice((
        just("true").to(Value::Integer(1)),
        just("false").to(Value::Integer(0)),
    ))
    .boxed();

    // let real = text::digits(radix);

    choice((boolean, integer().map(|i| Value::Integer(i))))
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

    value_list_2d
}

#[cfg(test)]
mod tests {
    use chumsky::Parser;

    use crate::{assert_debug_snapshot_with_input, sqlite_values::value_parser};

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
}
