use super::*;

#[test]
fn test_mixed_type_arithmetic() {
    // Test integer - float
    let expr = TrivialExpression::Binary {
        left: Box::new(TrivialExpression::Immediate(Value::from_i64(1))),
        op: Operator::Subtract,
        right: Box::new(TrivialExpression::Immediate(Value::from_f64(0.5))),
    };
    let result = expr.evaluate(&[]);
    assert_eq!(result, Value::from_f64(0.5));

    // Test float - integer
    let expr = TrivialExpression::Binary {
        left: Box::new(TrivialExpression::Immediate(Value::from_f64(2.5))),
        op: Operator::Subtract,
        right: Box::new(TrivialExpression::Immediate(Value::from_i64(1))),
    };
    let result = expr.evaluate(&[]);
    assert_eq!(result, Value::from_f64(1.5));

    // Test integer * float
    let expr = TrivialExpression::Binary {
        left: Box::new(TrivialExpression::Immediate(Value::from_i64(10))),
        op: Operator::Multiply,
        right: Box::new(TrivialExpression::Immediate(Value::from_f64(0.1))),
    };
    let result = expr.evaluate(&[]);
    assert_eq!(result, Value::from_f64(1.0));

    // Test integer / float
    let expr = TrivialExpression::Binary {
        left: Box::new(TrivialExpression::Immediate(Value::from_i64(1))),
        op: Operator::Divide,
        right: Box::new(TrivialExpression::Immediate(Value::from_f64(2.0))),
    };
    let result = expr.evaluate(&[]);
    assert_eq!(result, Value::from_f64(0.5));

    // Test integer + float
    let expr = TrivialExpression::Binary {
        left: Box::new(TrivialExpression::Immediate(Value::from_i64(1))),
        op: Operator::Add,
        right: Box::new(TrivialExpression::Immediate(Value::from_f64(0.5))),
    };
    let result = expr.evaluate(&[]);
    assert_eq!(result, Value::from_f64(1.5));
}

#[test]
fn test_nested_mixed_type_expressions() {
    // Test nested expressions with mixed types: (1 - 0.04)
    let one_minus_float = TrivialExpression::Binary {
        left: Box::new(TrivialExpression::Immediate(Value::from_i64(1))),
        op: Operator::Subtract,
        right: Box::new(TrivialExpression::Immediate(Value::from_f64(0.04))),
    };
    let result = one_minus_float.evaluate(&[]);
    assert_eq!(result, Value::from_f64(0.96));

    // Test multiplication with nested mixed-type expression: 100.0 * (1 - 0.04)
    let nested_expr = TrivialExpression::Binary {
        left: Box::new(TrivialExpression::Immediate(Value::from_f64(100.0))),
        op: Operator::Multiply,
        right: Box::new(one_minus_float),
    };
    let result = nested_expr.evaluate(&[]);
    assert_eq!(result, Value::from_f64(96.0));
}

#[test]
fn test_text_to_numeric_coercion_in_arithmetic() {
    // Non-numeric text should coerce to 0 (SQLite behavior)
    let values = vec![Value::Text(Text::new("hello".to_string()))];

    // text - 1 => 0 - 1 = -1
    let expr = TrivialExpression::Binary {
        left: Box::new(TrivialExpression::Column(0)),
        op: Operator::Subtract,
        right: Box::new(TrivialExpression::Immediate(Value::from_i64(1))),
    };
    assert_eq!(expr.evaluate(&values), Value::from_i64(-1));

    // text + 1 => 0 + 1 = 1
    let expr = TrivialExpression::Binary {
        left: Box::new(TrivialExpression::Column(0)),
        op: Operator::Add,
        right: Box::new(TrivialExpression::Immediate(Value::from_i64(1))),
    };
    assert_eq!(expr.evaluate(&values), Value::from_i64(1));

    // text * 2 => 0 * 2 = 0
    let expr = TrivialExpression::Binary {
        left: Box::new(TrivialExpression::Column(0)),
        op: Operator::Multiply,
        right: Box::new(TrivialExpression::Immediate(Value::from_i64(2))),
    };
    assert_eq!(expr.evaluate(&values), Value::from_i64(0));

    // text / 2 => 0 / 2 = 0
    let expr = TrivialExpression::Binary {
        left: Box::new(TrivialExpression::Column(0)),
        op: Operator::Divide,
        right: Box::new(TrivialExpression::Immediate(Value::from_i64(2))),
    };
    assert_eq!(expr.evaluate(&values), Value::from_i64(0));

    // Numeric text "42" - 1 => 41
    let numeric_text_values = vec![Value::Text(Text::new("42".to_string()))];
    let expr = TrivialExpression::Binary {
        left: Box::new(TrivialExpression::Column(0)),
        op: Operator::Subtract,
        right: Box::new(TrivialExpression::Immediate(Value::from_i64(1))),
    };
    assert_eq!(expr.evaluate(&numeric_text_values), Value::from_i64(41));
}
