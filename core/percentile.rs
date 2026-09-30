use turso_ext::{AggFunc, AggregateDerive, ExtensionApi, Value};

pub fn register_extension(ext_api: &mut ExtensionApi) {
    unsafe {
        Median::register_Median(ext_api);
        Percentile::register_Percentile(ext_api);
        PercentileCont::register_PercentileCont(ext_api);
        PercentileDisc::register_PercentileDisc(ext_api);
        StandardDeviation::register_StandardDeviation(ext_api);
    }
}

#[derive(AggregateDerive)]
struct Median;

impl AggFunc for Median {
    type State = Vec<f64>;
    type Error = &'static str;
    const NAME: &'static str = "median";
    const ARGS: i32 = 1;

    fn step(state: &mut Self::State, args: &[Value]) {
        if let Some(val) = args.first().and_then(Value::to_float) {
            state.push(val);
        }
    }

    fn finalize(state: Self::State) -> Result<Value, Self::Error> {
        if state.is_empty() {
            return Ok(Value::null());
        }

        let mut sorted = state;
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
    type State = (Vec<f64>, Option<f64>, Option<Self::Error>);
    type Error = &'static str;
    const NAME: &'static str = "percentile";
    const ARGS: i32 = 2;

    fn step(state: &mut Self::State, args: &[Value]) {
        let (values, p_value, err_value) = state;
        if let (Some(y), Some(p)) = (
            args.first().and_then(Value::to_float),
            args.get(1).and_then(Value::to_float),
        ) {
            if !(0.0..=100.0).contains(&p) {
                err_value.get_or_insert("Invalid percentile value");
                return;
            }

            if let Some(existing_p) = *p_value {
                if (existing_p - p).abs() >= 0.001 {
                    err_value.get_or_insert("Inconsistent percentile values across rows");
                    return;
                }
            } else {
                *p_value = Some(p);
            }
            values.push(y);
        }
    }

    fn finalize(state: Self::State) -> Result<Value, Self::Error> {
        let (mut values, p_value, err_value) = state;
        if values.is_empty() {
            return Ok(Value::null());
        }
        if let Some(err) = err_value {
            return Err(err);
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
    type State = (Vec<f64>, Option<f64>, Option<Self::Error>);
    type Error = &'static str;
    const NAME: &'static str = "percentile_cont";
    const ARGS: i32 = 2;

    fn step(state: &mut Self::State, args: &[Value]) {
        let (values, p_value, err_state) = state;
        if let (Some(y), Some(p)) = (
            args.first().and_then(Value::to_float),
            args.get(1).and_then(Value::to_float),
        ) {
            if !(0.0..=1.0).contains(&p) {
                err_state.get_or_insert("Percentile value must be between 0.0 and 1.0 inclusive");
                return;
            }

            if let Some(existing_p) = *p_value {
                if (existing_p - p).abs() >= 0.001 {
                    err_state.get_or_insert("Inconsistent percentile values across rows");
                    return;
                }
            } else {
                *p_value = Some(p);
            }
            values.push(y);
        }
    }

    fn finalize(state: Self::State) -> Result<Value, Self::Error> {
        let (mut values, p_value, err_state) = state;
        if values.is_empty() {
            return Ok(Value::null());
        }
        if let Some(err) = err_state {
            return Err(err);
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
    type State = (Vec<f64>, Option<f64>, Option<Self::Error>);
    type Error = &'static str;
    const NAME: &'static str = "percentile_disc";
    const ARGS: i32 = 2;

    fn step(state: &mut Self::State, args: &[Value]) {
        // Fraction in [0, 1], like percentile_cont and unlike percentile -- as
        // in SQLite's ext/misc/percentile.c and PostgreSQL. Using
        // Percentile::step admitted p up to 100 while finalize indexes by
        // fraction, so percentile_disc(x, 100) ran off the end.
        PercentileCont::step(state, args);
    }

    fn finalize(state: Self::State) -> Result<Value, Self::Error> {
        let (mut values, p_value, err_value) = state;
        if values.is_empty() {
            return Ok(Value::null());
        }
        if let Some(err) = err_value {
            return Err(err);
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
}
