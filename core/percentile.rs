use turso_ext::{AggFunc, AggregateDerive, ExtensionApi, Value, ValueType};

pub fn register_extension(ext_api: &mut ExtensionApi) {
    unsafe {
        Median::register_Median(ext_api);
        Percentile::register_Percentile(ext_api);
        PercentileCont::register_PercentileCont(ext_api);
        PercentileDisc::register_PercentileDisc(ext_api);
        StandardDeviation::register_StandardDeviation(ext_api);
    }
}

/// The value of an aggregated argument: `None` for NULL, an error for TEXT or BLOB
/// (as in SQLite's ext/misc/percentile.c), the number otherwise.
fn numeric_input(value: Option<&Value>) -> Result<Option<f64>, ()> {
    match value.map(Value::value_type) {
        Some(ValueType::Text | ValueType::Blob) => Err(()),
        _ => Ok(value.and_then(Value::to_float)),
    }
}

/// Values, fraction and the first error. Like SQLite's ext/misc/percentile.c, which stops at the
/// first error, `finalize` reports it before anything else.
type PercentileState = (Vec<f64>, Option<f64>, Option<String>);

/// Step of percentile, percentile_cont and percentile_disc, in the order of SQLite's percentStep:
/// the fraction must be a number in [0, `max_fraction`] and the same on every row, then a NULL
/// value is skipped and any other non-numeric value is an error.
fn fraction_step(state: &mut PercentileState, args: &[Value], name: &str, max_fraction: f64) {
    let (values, fraction, error) = state;
    if error.is_some() {
        return;
    }
    let p = match args.get(1) {
        Some(p)
            if matches!(
                p.value_type(),
                ValueType::Integer | ValueType::Float | ValueType::Text
            ) =>
        {
            p.to_float()
        }
        _ => None,
    };
    let Some(p) = p.filter(|p| (0.0..=max_fraction).contains(p)) else {
        *error = Some(format!(
            "the fraction argument to {name}() is not between 0.0 and {max_fraction:.1}"
        ));
        return;
    };
    match *fraction {
        None => *fraction = Some(p),
        Some(first) if ((first - p) / max_fraction).abs() > 0.001 => {
            *error = Some(format!(
                "the fraction argument to {name}() is not the same for all input rows"
            ));
            return;
        }
        Some(_) => {}
    }
    match numeric_input(args.first()) {
        Ok(Some(y)) => values.push(y),
        Ok(None) => {}
        Err(()) => *error = Some(format!("input to {name}() is not numeric")),
    }
}

#[derive(AggregateDerive)]
struct Median;

impl AggFunc for Median {
    type State = (Vec<f64>, Option<Self::Error>);
    type Error = &'static str;
    const NAME: &'static str = "median";
    const ARGS: i32 = 1;

    fn step(state: &mut Self::State, args: &[Value]) {
        let (values, input_err) = state;
        match numeric_input(args.first()) {
            Ok(Some(val)) => values.push(val),
            Ok(None) => {}
            Err(()) => {
                input_err.get_or_insert("input to median() is not numeric");
            }
        }
    }

    fn finalize(state: Self::State) -> Result<Value, Self::Error> {
        let (mut sorted, input_err) = state;
        if let Some(err) = input_err {
            return Err(err);
        }
        if sorted.is_empty() {
            return Ok(Value::null());
        }

        sorted.sort_by(|a, b| a.total_cmp(b));

        let len = sorted.len();
        if len % 2 == 1 {
            Ok(Value::from_float(sorted[len / 2]))
        } else {
            let mid1 = sorted[len / 2 - 1];
            let mid2 = sorted[len / 2];
            Ok(Value::from_float((mid1 + mid2) / 2.0))
        }
    }
}

#[derive(AggregateDerive)]
struct Percentile;

impl AggFunc for Percentile {
    type State = PercentileState;
    type Error = String;
    const NAME: &'static str = "percentile";
    const ARGS: i32 = 2;

    fn step(state: &mut Self::State, args: &[Value]) {
        fraction_step(state, args, Self::NAME, 100.0);
    }

    fn finalize(state: Self::State) -> Result<Value, Self::Error> {
        let (mut values, p_value, error) = state;
        if let Some(err) = error {
            return Err(err);
        }
        if values.is_empty() {
            return Ok(Value::null());
        }
        if values.len() == 1 {
            return Ok(Value::from_float(values[0]));
        }

        let p = p_value.ok_or("percentile value must be provided")?;
        values.sort_by(|a, b| a.total_cmp(b));
        let n = values.len() as f64;
        let index = p * (n - 1.0) / 100.0;
        let lower = index.floor() as usize;
        let upper = index.ceil() as usize;

        if lower == upper {
            Ok(Value::from_float(values[lower]))
        } else {
            let weight = index - lower as f64;
            Ok(Value::from_float(
                values[lower] * (1.0 - weight) + values[upper] * weight,
            ))
        }
    }
}

#[derive(AggregateDerive)]
struct PercentileCont;

impl AggFunc for PercentileCont {
    type State = PercentileState;
    type Error = String;
    const NAME: &'static str = "percentile_cont";
    const ARGS: i32 = 2;

    fn step(state: &mut Self::State, args: &[Value]) {
        fraction_step(state, args, Self::NAME, 1.0);
    }

    fn finalize(state: Self::State) -> Result<Value, Self::Error> {
        let (mut values, p_value, error) = state;
        if let Some(err) = error {
            return Err(err);
        }
        if values.is_empty() {
            return Ok(Value::null());
        }
        if values.len() == 1 {
            return Ok(Value::from_float(values[0]));
        }

        let p = p_value.ok_or("percentile value must be provided")?;
        values.sort_by(|a, b| a.total_cmp(b));
        let n = values.len() as f64;
        let index = p * (n - 1.0);
        let lower = index.floor() as usize;
        let upper = index.ceil() as usize;

        if lower == upper {
            Ok(Value::from_float(values[lower]))
        } else {
            let weight = index - lower as f64;
            Ok(Value::from_float(
                values[lower] * (1.0 - weight) + values[upper] * weight,
            ))
        }
    }
}

#[derive(AggregateDerive)]
struct PercentileDisc;

impl AggFunc for PercentileDisc {
    type State = PercentileState;
    type Error = String;
    const NAME: &'static str = "percentile_disc";
    const ARGS: i32 = 2;

    fn step(state: &mut Self::State, args: &[Value]) {
        // Fraction in [0, 1], like percentile_cont and unlike percentile -- as
        // in SQLite's ext/misc/percentile.c and PostgreSQL. Using
        // Percentile::step admitted p up to 100 while finalize indexes by
        // fraction, so percentile_disc(x, 100) ran off the end.
        fraction_step(state, args, Self::NAME, 1.0);
    }

    fn finalize(state: Self::State) -> Result<Value, Self::Error> {
        let (mut values, p_value, error) = state;
        if let Some(err) = error {
            return Err(err);
        }
        if values.is_empty() {
            return Ok(Value::null());
        }

        let p = p_value.ok_or("percentile value must be provided")?;
        values.sort_by(|a, b| a.total_cmp(b));
        let n = values.len() as f64;
        // step guarantees p in [0, 1], so this lands in [0, n - 1].
        let index = (p * (n - 1.0)).floor() as usize;
        Ok(Value::from_float(values[index]))
    }
}

/// Standard Deviation implementation using Welford's algorithm
/// Formula:
///
/// ```text
///     s = sqrt( M2 / (n - 1) )
/// ```
///
/// Where:
/// - `n` = number of observations
/// - `M2` = sum of squared deviations
#[derive(AggregateDerive)]
struct StandardDeviation;

impl AggFunc for StandardDeviation {
    type State = (u64, f64, f64); // Tracks the count, mean and sum of squared differences from the mean
    type Error = &'static str;
    const NAME: &'static str = "stddev";
    const ARGS: i32 = 1;

    fn step(state: &mut Self::State, args: &[Value]) {
        let (count, mean, m2) = state;

        if let Some(x) = args.first().and_then(Value::to_float) {
            *count += 1;

            // compute deviation from old mean
            let delta = x - *mean;
            *mean += delta / *count as f64;

            // update sum of squared differences
            let delta_2 = x - *mean;
            *m2 += delta * delta_2;
        }
    }

    fn finalize(state: Self::State) -> Result<Value, Self::Error> {
        let (count, _mean, m2) = state;
        if count < 2 {
            return Ok(Value::null());
        }

        let variance = m2 / (count - 1) as f64;
        Ok(Value::from_float(variance.sqrt()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type PctState = PercentileState;

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
            // Rejected whether or not an earlier row was accepted, never an
            // out-of-bounds read.
            for rows in [&[(1.0, p), (2.0, p)][..], &[(1.0, 0.5), (2.0, p)][..]] {
                assert_eq!(
                    run_rows::<PercentileDisc>(rows).err().as_deref(),
                    Some("the fraction argument to percentile_disc() is not between 0.0 and 1.0"),
                    "percentile_disc should reject p = {p}"
                );
            }
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
            run_rows::<Percentile>(&[(10.0, 50.0), (20.0, 101.0)])
                .err()
                .as_deref(),
            Some("the fraction argument to percentile() is not between 0.0 and 100.0")
        );
    }
}
