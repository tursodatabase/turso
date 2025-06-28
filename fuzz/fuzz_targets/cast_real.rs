#![no_main]
use libfuzzer_sys::{fuzz_target, Corpus};
use std::error::Error;
use turso_core::numeric::StrToF64;

fn do_fuzz(text: String) -> Result<Corpus, Box<dyn Error>> {
    let expected = {
        let conn = rusqlite::Connection::open_in_memory()?;
        conn.query_row(&format!("SELECT cast(? as real)"), (&text,), |row| {
            row.get::<_, f64>(0)
        })?
    };

    let actual = turso_core::numeric::str_to_f64(&text)
        .map(|v| {
            let (StrToF64::Fractional(non_nan) | StrToF64::Decimal(non_nan)) = v;
            f64::from(non_nan)
        })
        .unwrap_or(0.0);

    assert_eq!(expected, actual);

    Ok(Corpus::Keep)
}

fuzz_target!(|blob: String| -> Corpus { do_fuzz(blob).unwrap_or(Corpus::Keep) });
