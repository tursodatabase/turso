use itertools::Itertools;
use sql_generation::model::table::SimValue;

fn val_to_string(sim_val: &SimValue) -> String {
    match &sim_val.0 {
        turso_core::Value::Blob(blob) => {
            let convert_blob = || -> anyhow::Result<String> {
                let val = String::from_utf8(blob.clone())?;
                Ok(val)
            };

            convert_blob().unwrap_or_else(|_| sim_val.to_string())
        }
        _ => sim_val.to_string(),
    }
}

pub fn print_diff(
    left: &[Vec<SimValue>],
    right: &[Vec<SimValue>],
    left_label: &str,
    right_label: &str,
) {
    let left_vals: Vec<Vec<_>> = left
        .iter()
        .map(|rows| rows.iter().map(val_to_string).collect())
        .sorted()
        .collect();

    let right_vals: Vec<Vec<_>> = right
        .iter()
        .map(|rows| rows.iter().map(val_to_string).collect())
        .sorted()
        .collect();

    let simulator_string = format!("{left_vals:#?}");
    let db_string = format!("{right_vals:#?}");

    let diff = similar_asserts::SimpleDiff::from_str(
        &simulator_string,
        &db_string,
        left_label,
        right_label,
    );
    tracing::error!(%diff);
}
