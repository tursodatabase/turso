use super::*;
use turso_ext::ValueType;

type PctState = (Vec<f64>, Option<f64>, Option<&'static str>);

/// Drive an aggregate over explicit `(value, percentile)` rows.
fn run_rows<A: AggFunc<State = PctState>>(rows: &[(f64, f64)]) -> Result<Value, A::Error> {
    let mut state = PctState::default();
    for (y, p) in rows {
        A::step(&mut state, &[Value::from_float(*y), Value::from_float(*p)]);
    }
    A::finalize(state)
}

/// Drive an aggregate over `values` with a constant percentile argument,
/// the way `SELECT f(x, p) FROM t` does.
fn run<A: AggFunc<State = PctState>>(values: &[f64], p: f64) -> Result<Value, A::Error> {
    let rows: Vec<(f64, f64)> = values.iter().map(|y| (*y, p)).collect();
    run_rows::<A>(&rows)
}

#[test]
fn percentile_disc_rejects_percentages() {
    // Used to inherit percentile()'s [0, 100] validation while indexing by
    // fraction, reading past the end of the sorted values.
    for p in [100.0, 2.0, 1.5, -0.5] {
        // All rows rejected, so NULL rather than an out-of-bounds read.
        assert_eq!(
            run::<PercentileDisc>(&[1.0, 2.0], p).unwrap().value_type(),
            ValueType::Null,
            "percentile_disc should not index with p = {p}"
        );
        // A leading valid row keeps values non-empty, so finalize reports
        // the rejection instead of collapsing to NULL.
        assert_eq!(
            run_rows::<PercentileDisc>(&[(1.0, 0.5), (2.0, p)]).err(),
            Some("Percentile value must be between 0.0 and 1.0 inclusive"),
            "percentile_disc should reject p = {p}"
        );
    }
}

#[test]
fn percentile_disc_selects_by_fraction() {
    let values = [10.0, 20.0, 30.0, 40.0, 50.0];
    // Discrete: floor((n - 1) * p), always an actual input value.
    for (p, expected) in [
        (0.0, 10.0),
        (0.25, 20.0),
        (0.5, 30.0),
        (0.55, 30.0),
        (1.0, 50.0),
    ] {
        assert_eq!(
            run::<PercentileDisc>(&values, p).unwrap().to_float(),
            Some(expected),
            "percentile_disc at p = {p}"
        );
    }
}

#[test]
fn percentile_keeps_percentage_scale() {
    // percentile() stays on [0, 100] and interpolates; the two must not
    // drift onto the same scale.
    let values = [10.0, 20.0, 30.0, 40.0, 50.0];
    assert_eq!(
        run::<Percentile>(&values, 100.0).unwrap().to_float(),
        Some(50.0)
    );
    assert_eq!(
        run::<Percentile>(&values, 55.0).unwrap().to_float(),
        Some(32.0)
    );
    assert_eq!(
        run_rows::<Percentile>(&[(10.0, 50.0), (20.0, 101.0)]).err(),
        Some("Invalid percentile value")
    );
}
