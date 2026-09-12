use super::*;

#[test]
fn test_valid_get_date_from_time_value() {
    let now = chrono::Local::now().to_utc().format("%Y-%m-%d").to_string();

    let prev_date_str = "2024-07-20";
    let test_date_str = "2024-07-21";
    let next_date_str = "2024-07-22";

    let test_cases: Vec<(Value, &str)> = vec![
        // Format 1: YYYY-MM-DD (no timezone applicable)
        (Value::build_text("2024-07-21"), test_date_str),
        // Format 2: YYYY-MM-DD HH:MM
        (Value::build_text("2024-07-21 22:30"), test_date_str),
        (Value::build_text("2024-07-21 22:30+02:00"), test_date_str),
        (Value::build_text("2024-07-21 22:30-05:00"), next_date_str),
        (Value::build_text("2024-07-21 01:30+05:00"), prev_date_str),
        (Value::build_text("2024-07-21 22:30Z"), test_date_str),
        // Format 3: YYYY-MM-DD HH:MM:SS
        (Value::build_text("2024-07-21 22:30:45"), test_date_str),
        (
            Value::build_text("2024-07-21 22:30:45+02:00"),
            test_date_str,
        ),
        (
            Value::build_text("2024-07-21 22:30:45-05:00"),
            next_date_str,
        ),
        (
            Value::build_text("2024-07-21 01:30:45+05:00"),
            prev_date_str,
        ),
        (Value::build_text("2024-07-21 22:30:45Z"), test_date_str),
        // Format 4: YYYY-MM-DD HH:MM:SS.SSS
        (Value::build_text("2024-07-21 22:30:45.123"), test_date_str),
        (
            Value::build_text("2024-07-21 22:30:45.123+02:00"),
            test_date_str,
        ),
        (
            Value::build_text("2024-07-21 22:30:45.123-05:00"),
            next_date_str,
        ),
        (
            Value::build_text("2024-07-21 01:30:45.123+05:00"),
            prev_date_str,
        ),
        (Value::build_text("2024-07-21 22:30:45.123Z"), test_date_str),
        // Format 5: YYYY-MM-DDTHH:MM
        (Value::build_text("2024-07-21T22:30"), test_date_str),
        (Value::build_text("2024-07-21T22:30+02:00"), test_date_str),
        (Value::build_text("2024-07-21T22:30-05:00"), next_date_str),
        (Value::build_text("2024-07-21T01:30+05:00"), prev_date_str),
        (Value::build_text("2024-07-21T22:30Z"), test_date_str),
        // Format 6: YYYY-MM-DDTHH:MM:SS
        (Value::build_text("2024-07-21T22:30:45"), test_date_str),
        (
            Value::build_text("2024-07-21T22:30:45+02:00"),
            test_date_str,
        ),
        (
            Value::build_text("2024-07-21T22:30:45-05:00"),
            next_date_str,
        ),
        (
            Value::build_text("2024-07-21T01:30:45+05:00"),
            prev_date_str,
        ),
        (Value::build_text("2024-07-21T22:30:45Z"), test_date_str),
        // Format 7: YYYY-MM-DDTHH:MM:SS.SSS
        (Value::build_text("2024-07-21T22:30:45.123"), test_date_str),
        (
            Value::build_text("2024-07-21T22:30:45.123+02:00"),
            test_date_str,
        ),
        (
            Value::build_text("2024-07-21T22:30:45.123-05:00"),
            next_date_str,
        ),
        (
            Value::build_text("2024-07-21T01:30:45.123+05:00"),
            prev_date_str,
        ),
        (Value::build_text("2024-07-21T22:30:45.123Z"), test_date_str),
        // Format 8: HH:MM
        (Value::build_text("22:30"), "2000-01-01"),
        (Value::build_text("22:30+02:00"), "2000-01-01"),
        (Value::build_text("22:30-05:00"), "2000-01-02"),
        (Value::build_text("01:30+05:00"), "1999-12-31"),
        (Value::build_text("22:30Z"), "2000-01-01"),
        // Format 9: HH:MM:SS
        (Value::build_text("22:30:45"), "2000-01-01"),
        (Value::build_text("22:30:45+02:00"), "2000-01-01"),
        (Value::build_text("22:30:45-05:00"), "2000-01-02"),
        (Value::build_text("01:30:45+05:00"), "1999-12-31"),
        (Value::build_text("22:30:45Z"), "2000-01-01"),
        // Format 10: HH:MM:SS.SSS
        (Value::build_text("22:30:45.123"), "2000-01-01"),
        (Value::build_text("22:30:45.123+02:00"), "2000-01-01"),
        (Value::build_text("22:30:45.123-05:00"), "2000-01-02"),
        (Value::build_text("01:30:45.123+05:00"), "1999-12-31"),
        (Value::build_text("22:30:45.123Z"), "2000-01-01"),
        // Test Format 11: 'now'
        (Value::build_text("now"), &now),
        // Format 12: DDDDDDDDDD (Julian date as float or integer)
        (Value::from_f64(2460512.5), test_date_str),
        (Value::from_i64(2460513), test_date_str),
    ];

    for (input, expected) in test_cases {
        let result = exec_date(&[input.clone()]);
        assert_eq!(
            result,
            Value::build_text(expected.to_string()),
            "Failed for input: {input:?}"
        );
    }
}

#[test]
fn test_invalid_get_date_from_time_value() {
    let invalid_cases = vec![
        Value::build_text("2024-07-21 25:00"),    // Invalid hour
        Value::build_text("2024-07-21 25:00:00"), // Invalid hour
        Value::build_text("2024-07-21 23:60:00"), // Invalid minute
        Value::build_text("2024-07-21 22:58:60"), // Invalid second
        // Note: Invalid days now overflow like SQLite (2024-07-32 -> 2024-08-01)
        Value::build_text("2024-13-01"),          // Invalid month
        Value::build_text("invalid_date"),        // Completely invalid string
        Value::build_text(""),                    // Empty string
        Value::from_i64(i64::MAX),                // Large Julian day
        Value::from_i64(-1),                      // Negative Julian day
        Value::from_f64(f64::MAX),                // Large float
        Value::from_f64(-1.0),                    // Negative Julian day as float
        Value::from_f64(f64::NAN),                // NaN
        Value::from_f64(f64::INFINITY),           // Infinity
        Value::Null,                              // Null value
        Value::Blob(crate::alloc::vec![1, 2, 3]), // Blob whose bytes are not a date
        // Invalid timezone tests
        Value::build_text("2024-07-21T12:00:00+24:00"), // Invalid timezone offset (too large)
        Value::build_text("2024-07-21T12:00:00-24:00"), // Invalid timezone offset (too small)
        Value::build_text("2024-07-21T12:00:00+00:60"), // Invalid timezone minutes
        Value::build_text("2024-07-21T12:00:00+00:00:00"), // Invalid timezone format (extra seconds)
        Value::build_text("2024-07-21T12:00:00+"),         // Incomplete timezone
        Value::build_text("2024-07-21T12:00:00+Z"),        // Invalid timezone format
        Value::build_text("2024-07-21T12:00:00+00:00Z"),   // Mixing offset and Z
        Value::build_text("2024-07-21T12:00:00UTC"),       // Named timezone (not supported)
    ];

    for case in invalid_cases.iter() {
        let result = exec_date([case]);
        assert_eq!(result, Value::Null);
    }
}

#[test]
fn test_valid_get_time_from_datetime_value() {
    let test_time_str = "22:30:45";
    let prev_time_str = "20:30:45";
    let next_time_str = "03:30:45";

    let test_cases = vec![
        // Format 1: YYYY-MM-DD (no timezone applicable)
        (Value::build_text("2024-07-21"), "00:00:00"),
        // Format 2: YYYY-MM-DD HH:MM
        (Value::build_text("2024-07-21 22:30"), "22:30:00"),
        (Value::build_text("2024-07-21 22:30+02:00"), "20:30:00"),
        (Value::build_text("2024-07-21 22:30-05:00"), "03:30:00"),
        (Value::build_text("2024-07-21 22:30Z"), "22:30:00"),
        // Format 3: YYYY-MM-DD HH:MM:SS
        (Value::build_text("2024-07-21 22:30:45"), test_time_str),
        (
            Value::build_text("2024-07-21 22:30:45+02:00"),
            prev_time_str,
        ),
        (
            Value::build_text("2024-07-21 22:30:45-05:00"),
            next_time_str,
        ),
        (Value::build_text("2024-07-21 22:30:45Z"), test_time_str),
        // Format 4: YYYY-MM-DD HH:MM:SS.SSS
        (Value::build_text("2024-07-21 22:30:45.123"), test_time_str),
        (
            Value::build_text("2024-07-21 22:30:45.123+02:00"),
            prev_time_str,
        ),
        (
            Value::build_text("2024-07-21 22:30:45.123-05:00"),
            next_time_str,
        ),
        (Value::build_text("2024-07-21 22:30:45.123Z"), test_time_str),
        // Format 5: YYYY-MM-DDTHH:MM
        (Value::build_text("2024-07-21T22:30"), "22:30:00"),
        (Value::build_text("2024-07-21T22:30+02:00"), "20:30:00"),
        (Value::build_text("2024-07-21T22:30-05:00"), "03:30:00"),
        (Value::build_text("2024-07-21T22:30Z"), "22:30:00"),
        // Format 6: YYYY-MM-DDTHH:MM:SS
        (Value::build_text("2024-07-21T22:30:45"), test_time_str),
        (
            Value::build_text("2024-07-21T22:30:45+02:00"),
            prev_time_str,
        ),
        (
            Value::build_text("2024-07-21T22:30:45-05:00"),
            next_time_str,
        ),
        (Value::build_text("2024-07-21T22:30:45Z"), test_time_str),
        // Format 7: YYYY-MM-DDTHH:MM:SS.SSS
        (Value::build_text("2024-07-21T22:30:45.123"), test_time_str),
        (
            Value::build_text("2024-07-21T22:30:45.123+02:00"),
            prev_time_str,
        ),
        (
            Value::build_text("2024-07-21T22:30:45.123-05:00"),
            next_time_str,
        ),
        (Value::build_text("2024-07-21T22:30:45.123Z"), test_time_str),
        // Format 8: HH:MM
        (Value::build_text("22:30"), "22:30:00"),
        (Value::build_text("22:30+02:00"), "20:30:00"),
        (Value::build_text("22:30-05:00"), "03:30:00"),
        (Value::build_text("22:30Z"), "22:30:00"),
        // Format 9: HH:MM:SS
        (Value::build_text("22:30:45"), test_time_str),
        (Value::build_text("22:30:45+02:00"), prev_time_str),
        (Value::build_text("22:30:45-05:00"), next_time_str),
        (Value::build_text("22:30:45Z"), test_time_str),
        // Format 10: HH:MM:SS.SSS
        (Value::build_text("22:30:45.123"), test_time_str),
        (Value::build_text("22:30:45.123+02:00"), prev_time_str),
        (Value::build_text("22:30:45.123-05:00"), next_time_str),
        (Value::build_text("22:30:45.123Z"), test_time_str),
        // Format 12: DDDDDDDDDD (Julian date as float or integer)
        (Value::from_f64(2460082.1), "14:24:00"),
        (Value::from_i64(2460082), "12:00:00"),
    ];

    for (input, expected) in test_cases {
        let result = exec_time(&[input]);
        if let Value::Text(result_str) = result {
            assert_eq!(result_str.as_str(), expected);
        } else {
            panic!("Expected Value::Text, but got: {result:?}");
        }
    }
}

#[test]
fn test_invalid_get_time_from_datetime_value() {
    let invalid_cases = vec![
        Value::build_text("2024-07-21 25:00"),    // Invalid hour
        Value::build_text("2024-07-21 25:00:00"), // Invalid hour
        Value::build_text("2024-07-21 23:60:00"), // Invalid minute
        Value::build_text("2024-07-21 22:58:60"), // Invalid second
        // Note: Invalid days now overflow like SQLite (2024-07-32 -> 2024-08-01)
        Value::build_text("2024-13-01"),          // Invalid month
        Value::build_text("invalid_date"),        // Completely invalid string
        Value::build_text(""),                    // Empty string
        Value::from_i64(i64::MAX),                // Large Julian day
        Value::from_i64(-1),                      // Negative Julian day
        Value::from_f64(f64::MAX),                // Large float
        Value::from_f64(-1.0),                    // Negative Julian day as float
        Value::from_f64(f64::NAN),                // NaN
        Value::from_f64(f64::INFINITY),           // Infinity
        Value::Null,                              // Null value
        Value::Blob(crate::alloc::vec![1, 2, 3]), // Blob whose bytes are not a date
        // Invalid timezone tests
        Value::build_text("2024-07-21T12:00:00+24:00"), // Invalid timezone offset (too large)
        Value::build_text("2024-07-21T12:00:00-24:00"), // Invalid timezone offset (too small)
        Value::build_text("2024-07-21T12:00:00+00:60"), // Invalid timezone minutes
        Value::build_text("2024-07-21T12:00:00+00:00:00"), // Invalid timezone format (extra seconds)
        Value::build_text("2024-07-21T12:00:00+"),         // Incomplete timezone
        Value::build_text("2024-07-21T12:00:00+Z"),        // Invalid timezone format
        Value::build_text("2024-07-21T12:00:00+00:00Z"),   // Mixing offset and Z
        Value::build_text("2024-07-21T12:00:00UTC"),       // Named timezone (not supported)
        // Unsupported date format tests
        Value::build_text("2024/07/21"),
        Value::build_text("2024.07.21"),
        Value::build_text("07/21/2024"),
        Value::build_text("21/07/2024"),
    ];

    for case in invalid_cases {
        let result = exec_time(&[case.clone()]);
        assert_eq!(result, Value::Null);
    }
}

#[test]
fn test_parse_modifier_overflow() {
    let modifiers = [
        "1e308 days",
        "1e308 hours",
        "1e308 minutes",
        "1e308 seconds",
        "1e308 months",
        "1e308 years",
        "-1e308 days",
        "-1e308 hours",
        "-1e308 minutes",
        "-1e308 seconds",
        "-1e308 months",
        "-1e308 years",
        "1e309 days",
        "1e309 hours",
        "1e309 minutes",
        "1e309 seconds",
        "1e309 months",
        "1e309 years",
    ];

    for modifier in modifiers {
        assert_eq!(
            exec_datetime_full(&[Value::build_text("now"), Value::build_text(modifier),]),
            Value::Null,
            "modifier: {modifier}"
        );
    }
}

#[test]
fn test_parse_days() {
    let get_days = |s: &str| -> f64 {
        let mut p = DateTime::default();
        p.compute_jd();
        let start_jd = p.i_jd;
        parse_modifier(&mut p, s, 1).expect("Failed to parse modifier");
        (p.i_jd - start_jd) as f64 / 86_400_000.0
    };

    assert_eq!(get_days("5 days"), 5.0);
    assert_eq!(get_days("-3 days"), -3.0);
    assert_eq!(get_days("+2 days"), 2.0);
    assert_eq!(get_days("4  days"), 4.0);
    assert_eq!(get_days("6   DAYS"), 6.0);
    assert_eq!(get_days("+5  DAYS"), 5.0);
    // Fractional days
    assert_eq!(get_days("1.5 days"), 1.5);
    assert_eq!(get_days("-0.25 days"), -0.25);
}

#[test]
fn test_parse_hours() {
    let get_hours = |s: &str| -> f64 {
        let mut p = DateTime::default();
        p.compute_jd();
        let start_jd = p.i_jd;
        parse_modifier(&mut p, s, 1).expect("Failed to parse modifier");
        (p.i_jd - start_jd) as f64 / 3_600_000.0
    };

    assert_eq!(get_hours("12 hours"), 12.0);
    assert_eq!(get_hours("-2 hours"), -2.0);
    assert_eq!(get_hours("+3  HOURS"), 3.0);
    // Fractional hours
    assert_eq!(get_hours("0.5 hours"), 0.5);
}

#[test]
fn test_parse_minutes() {
    let get_minutes = |s: &str| -> f64 {
        let mut p = DateTime::default();
        p.compute_jd();
        let start_jd = p.i_jd;
        parse_modifier(&mut p, s, 1).expect("Failed to parse modifier");
        (p.i_jd - start_jd) as f64 / 60_000.0
    };

    assert_eq!(get_minutes("30 minutes"), 30.0);
    assert_eq!(get_minutes("-15 minutes"), -15.0);
    assert_eq!(get_minutes("+45  MINUTES"), 45.0);
}

#[test]
fn test_parse_seconds() {
    let get_seconds = |s: &str| -> f64 {
        let mut p = DateTime::default();
        p.compute_jd();
        let start_jd = p.i_jd;
        parse_modifier(&mut p, s, 1).expect("Failed to parse modifier");
        (p.i_jd - start_jd) as f64 / 1000.0
    };

    assert_eq!(get_seconds("45 seconds"), 45.0);
    assert_eq!(get_seconds("-10 seconds"), -10.0);
    assert_eq!(get_seconds("+20  SECONDS"), 20.0);
}

#[test]
fn test_parse_months() {
    let get_months = |s: &str| -> f64 {
        let mut p = DateTime::default();
        let start_y = p.y;
        let start_m = p.m;
        parse_modifier(&mut p, s, 1).expect("Failed to parse modifier");
        ((p.y - start_y) * 12 + (p.m - start_m)) as f64
    };

    assert_eq!(get_months("3 months"), 3.0);
    assert_eq!(get_months("-1 months"), -1.0);
    assert_eq!(get_months("+6  MONTHS"), 6.0);
}

#[test]
fn test_parse_years() {
    let get_years = |s: &str| -> f64 {
        let mut p = DateTime::default();
        let start_y = p.y;
        parse_modifier(&mut p, s, 1).expect("Failed to parse modifier");
        (p.y - start_y) as f64
    };

    assert_eq!(get_years("2 years"), 2.0);
    assert_eq!(get_years("-1 years"), -1.0);
    assert_eq!(get_years("+10  YEARS"), 10.0);
}

#[test]
fn test_parse_time_offset() {
    let get_ms_change = |s: &str| -> i64 {
        let mut p = DateTime::default();
        p.compute_jd();
        let start_jd = p.i_jd;
        parse_modifier(&mut p, s, 1).expect("Failed to parse modifier");
        p.i_jd - start_jd
    };

    // +01:30 = 90 mins = 5,400,000 ms
    assert_eq!(get_ms_change("+01:30"), 5_400_000);
    // -00:45 = -45 mins = -2,700,000 ms
    assert_eq!(get_ms_change("-00:45"), -2_700_000);
    // +02:15:30 = 8,130,000 ms
    assert_eq!(get_ms_change("+02:15:30"), 8_130_000);
    // +02:15:30.250 = 8,130,250 ms
    assert_eq!(get_ms_change("+02:15:30.250"), 8_130_250);
}
#[test]
fn test_parse_date_offset() {
    let run = |modifier: &str| -> String {
        let args = vec![
            Value::build_text("2000-01-01 00:00:00".to_string()),
            Value::build_text(modifier.to_string()),
        ];
        let val = exec_datetime_full(args);
        val.to_text().unwrap().to_string()
    };

    assert_eq!(run("+2023-05-15"), "4023-06-16 00:00:00");
    assert_eq!(run("-2023-05-15"), "-0024-07-17 00:00:00");
}

#[test]
fn test_parse_date_time_offset() {
    let run = |modifier: &str| -> String {
        let args = vec![
            Value::build_text("2000-01-01 00:00:00".to_string()),
            Value::build_text(modifier.to_string()),
        ];
        let val = exec_datetime_full(args);
        val.to_text().unwrap().to_string()
    };

    assert_eq!(run("+2023-05-15 14:30"), "4023-06-16 14:30:00");
    assert_eq!(run("-0001-05-15 14:30"), "1998-07-16 09:30:00");
}
#[test]
fn test_time_offset_boundaries() {
    let valid = ["+24:59", "-24:59"];

    for modifier in valid {
        assert_ne!(
            exec_datetime_full(&[Value::build_text("now"), Value::build_text(modifier),]),
            Value::Null,
            "modifier: {modifier}"
        );
    }

    let invalid = ["+25:00", "+25:01", "-25:00", "-25:01"];

    for modifier in invalid {
        assert_eq!(
            exec_datetime_full(&[Value::build_text("now"), Value::build_text(modifier),]),
            Value::Null,
            "modifier: {modifier}"
        );
    }
}

#[test]
fn test_parse_start_of() {
    let run = |start: &str, modifier: &str| -> String {
        let args = vec![
            Value::build_text(start.to_string()),
            Value::build_text(modifier.to_string()),
        ];
        let val = exec_datetime_full(args);
        val.to_text().unwrap().to_string()
    };

    let base = "2023-06-15 12:30:45";
    assert_eq!(run(base, "start of month"), "2023-06-01 00:00:00");
    assert_eq!(run(base, "START OF MONTH"), "2023-06-01 00:00:00");
    assert_eq!(run(base, "start of year"), "2023-01-01 00:00:00");
    assert_eq!(run(base, "START OF YEAR"), "2023-01-01 00:00:00");
    assert_eq!(run(base, "start of day"), "2023-06-15 00:00:00");
    assert_eq!(run(base, "START OF DAY"), "2023-06-15 00:00:00");
}

#[test]
fn test_invalid_end_of_modifiers() {
    let modifiers = [
        "end of month",
        "END OF MONTH",
        "end of year",
        "END OF YEAR",
        "end of day",
        "END OF DAY",
    ];

    for modifier in modifiers {
        let result =
            exec_datetime_full(&[Value::build_text("2023-06-15"), Value::build_text(modifier)]);

        assert_eq!(result, Value::Null, "modifier: {modifier}");
    }
}

#[test]
fn test_parse_weekday() {
    let run = |start: &str, modifier: &str| -> String {
        let args = vec![
            Value::build_text(start.to_string()),
            Value::build_text(modifier.to_string()),
        ];
        let val = exec_date(args);
        val.to_text().unwrap().to_string()
    };

    // 2023-01-01 was a Sunday (0)
    assert_eq!(run("2023-01-01", "weekday 0"), "2023-01-01"); // No change
    assert_eq!(run("2023-01-01", "weekday 1"), "2023-01-02"); // Next Monday
    assert_eq!(run("2023-01-01", "WEEKDAY 6"), "2023-01-07"); // Next Saturday
}

#[test]
fn test_parse_ceiling_modifier() {
    let mut p = DateTime::default();
    assert!(parse_modifier(&mut p, "ceiling", 1).is_ok());
    assert!(parse_modifier(&mut p, "CEILING", 1).is_ok());
}

#[test]
fn test_parse_other_modifiers() {
    // Setup state for modifiers that require specific preconditions
    let mut p = DateTime {
        valid_jd: true,
        raw_s: true,
        ..DateTime::default()
    };

    // Modifiers that should just parse OK
    assert!(parse_modifier(&mut p, "localtime", 1).is_ok());
    assert!(parse_modifier(&mut p, "LOCALTIME", 1).is_ok());
    assert!(parse_modifier(&mut p, "utc", 1).is_ok());
    assert!(parse_modifier(&mut p, "UTC", 1).is_ok());
    assert!(parse_modifier(&mut p, "subsec", 1).is_ok());
    assert!(parse_modifier(&mut p, "SUBSEC", 1).is_ok());
    assert!(parse_modifier(&mut p, "subsecond", 1).is_ok());
    assert!(parse_modifier(&mut p, "SUBSECOND", 1).is_ok());

    // These must be at index 0 to parse validly
    assert!(parse_modifier(&mut p, "unixepoch", 0).is_ok());
    p.raw_s = true;
    assert!(parse_modifier(&mut p, "UNIXEPOCH", 0).is_ok());
    p.raw_s = true;
    assert!(parse_modifier(&mut p, "julianday", 0).is_ok());
    p.raw_s = true;
    assert!(parse_modifier(&mut p, "JULIANDAY", 0).is_ok());
    p.raw_s = true;
    assert!(parse_modifier(&mut p, "auto", 0).is_ok());
    p.raw_s = true;
    assert!(parse_modifier(&mut p, "AUTO", 0).is_ok());
}

#[test]
fn test_parse_invalid_modifier() {
    let mut p = DateTime::default();
    assert!(parse_modifier(&mut p, "invalid modifier", 1).is_err());
    assert!(parse_modifier(&mut p, "5", 1).is_err());
    assert!(parse_modifier(&mut p, "days", 1).is_err());
    assert!(parse_modifier(&mut p, "++5 days", 1).is_err());
    assert!(parse_modifier(&mut p, "weekday 7", 1).is_err());
}

#[test]
fn test_apply_modifier_days() {
    let run = |mod_str: &str| -> String {
        let args = vec![
            Value::build_text("2023-06-15 12:30:45".to_string()),
            Value::build_text(mod_str.to_string()),
        ];
        exec_datetime_full(args).to_text().unwrap().to_string()
    };

    assert_eq!(run("5 days"), "2023-06-20 12:30:45");
    assert_eq!(run("-3 days"), "2023-06-12 12:30:45");
}

#[test]
fn test_apply_modifier_hours() {
    let run = |mod_str: &str| -> String {
        let args = vec![
            Value::build_text("2023-06-15 12:30:45".to_string()),
            Value::build_text(mod_str.to_string()),
        ];
        exec_datetime_full(args).to_text().unwrap().to_string()
    };

    assert_eq!(run("6 hours"), "2023-06-15 18:30:45");
    assert_eq!(run("-2 hours"), "2023-06-15 10:30:45");
}

#[test]
fn test_apply_modifier_minutes() {
    let run = |mod_str: &str| -> String {
        let args = vec![
            Value::build_text("2023-06-15 12:30:45".to_string()),
            Value::build_text(mod_str.to_string()),
        ];
        exec_datetime_full(args).to_text().unwrap().to_string()
    };

    assert_eq!(run("45 minutes"), "2023-06-15 13:15:45");
    assert_eq!(run("-15 minutes"), "2023-06-15 12:15:45");
}

#[test]
fn test_apply_modifier_seconds() {
    let run = |mod_str: &str| -> String {
        let args = vec![
            Value::build_text("2023-06-15 12:30:45".to_string()),
            Value::build_text(mod_str.to_string()),
        ];
        exec_datetime_full(args).to_text().unwrap().to_string()
    };

    assert_eq!(run("30 seconds"), "2023-06-15 12:31:15");
    assert_eq!(run("-20 seconds"), "2023-06-15 12:30:25");
}
#[test]
fn test_datetime_boundary_arithmetic() {
    assert_eq!(
        exec_datetime_full(&[
            Value::build_text("9999-12-31 23:59:59"),
            Value::build_text("+1 second"),
        ]),
        Value::Null
    );

    assert_eq!(
        exec_datetime_full(&[
            Value::build_text("9999-12-31 23:59:59"),
            Value::build_text("+1 day"),
        ]),
        Value::Null
    );

    assert_eq!(
        exec_datetime_full(&[
            Value::build_text("9999-12-31 23:59:59"),
            Value::build_text("+1 month"),
        ]),
        Value::Null
    );

    assert_eq!(
        exec_datetime_full(&[
            Value::build_text("9999-12-31 23:59:59"),
            Value::build_text("+1 year"),
        ]),
        Value::Null
    );

    assert_eq!(
        exec_datetime_full(&[
            Value::build_text("0000-01-01 00:00:00"),
            Value::build_text("-1 second"),
        ]),
        Value::build_text("-0001-12-31 23:59:59")
    );

    assert_eq!(
        exec_datetime_full(&[
            Value::build_text("0000-01-01 00:00:00"),
            Value::build_text("-1 day"),
        ]),
        Value::build_text("-0001-12-31 00:00:00")
    );

    assert_eq!(
        exec_datetime_full(&[
            Value::build_text("0000-01-01 00:00:00"),
            Value::build_text("-1 month"),
        ]),
        Value::build_text("-0001-12-01 00:00:00")
    );

    assert_eq!(
        exec_datetime_full(&[
            Value::build_text("0000-01-01 00:00:00"),
            Value::build_text("-1 year"),
        ]),
        Value::build_text("-0001-01-01 00:00:00")
    );
}

#[test]
fn test_apply_modifier_time_offset() {
    let run = |mod_str: &str| -> String {
        let args = vec![
            Value::build_text("2023-06-15 12:30:45".to_string()),
            Value::build_text(mod_str.to_string()),
        ];
        exec_datetime_full(args).to_text().unwrap().to_string()
    };

    assert_eq!(run("+01:30"), "2023-06-15 14:00:45");
    assert_eq!(run("-00:45"), "2023-06-15 11:45:45");
}

#[test]
fn test_apply_modifier_date_time_offset() {
    let run = |mod_str: &str| -> String {
        let args = vec![
            Value::build_text("2023-06-15 12:30:45".to_string()),
            Value::build_text(mod_str.to_string()),
        ];
        exec_datetime_full(args).to_text().unwrap().to_string()
    };

    assert_eq!(run("+0001-01-01 01:01"), "2024-07-16 13:31:45");
    assert_eq!(run("-0001-01-01 01:01"), "2022-05-14 11:29:45");
    assert_eq!(run("+0002-03-04 05:06"), "2025-09-19 17:36:45");
    assert_eq!(run("-0002-03-04 05:06"), "2021-03-11 07:24:45");
}

#[test]
fn test_apply_modifier_start_of_year() {
    let res = exec_datetime_full(&[
        Value::build_text("2023-06-15 12:30:45"),
        Value::build_text("start of year"),
    ]);
    assert_eq!(res.to_text().unwrap(), "2023-01-01 00:00:00");
}

#[test]
fn test_apply_modifier_start_of_day() {
    let res = exec_datetime_full(&[
        Value::build_text("2023-06-15 12:30:45"),
        Value::build_text("start of day"),
    ]);
    assert_eq!(res.to_text().unwrap(), "2023-06-15 00:00:00");
}

#[test]
fn test_single_modifier() {
    let res = exec_datetime_full(&[
        Value::build_text("2023-06-15 12:30:45"),
        Value::build_text("-1 day"),
    ]);
    assert_eq!(res.to_text().unwrap(), "2023-06-14 12:30:45");
}

#[test]
fn test_multiple_modifiers() {
    let res = exec_datetime_full(&[
        Value::build_text("2023-06-15 12:30:45"),
        Value::build_text("-1 day"),
        Value::build_text("+3 hours"),
    ]);
    assert_eq!(res.to_text().unwrap(), "2023-06-14 15:30:45");
}

#[test]
fn test_subsec_modifier() {
    let res = exec_datetime_general(
        &[
            Value::build_text("2023-06-15 12:30:45"),
            Value::build_text("subsec"),
        ],
        "time",
    );
    assert_eq!(res.to_text().unwrap(), "12:30:45.000");
}

#[test]
fn test_start_of_day_modifier() {
    let res = exec_datetime_full(&[
        Value::build_text("2023-06-15 12:30:45"),
        Value::build_text("start of day"),
        Value::build_text("-1 day"),
    ]);
    assert_eq!(res.to_text().unwrap(), "2023-06-14 00:00:00");
}

#[test]
fn test_start_of_month_modifier() {
    let res = exec_datetime_full(&[
        Value::build_text("2023-06-15 12:30:45"),
        Value::build_text("start of month"),
        Value::build_text("+1 day"),
    ]);
    assert_eq!(res.to_text().unwrap(), "2023-06-02 00:00:00");
}

#[test]
fn test_start_of_year_modifier() {
    let res = exec_datetime_full(&[
        Value::build_text("2023-06-15 12:30:45"),
        Value::build_text("start of year"),
        Value::build_text("+30 days"),
        Value::build_text("+5 hours"),
    ]);
    assert_eq!(res.to_text().unwrap(), "2023-01-31 05:00:00");
}

#[test]
fn test_timezone_modifiers() {
    let base_str = "2023-06-15 12:30:45";
    let naive = chrono::NaiveDate::from_ymd_opt(2023, 6, 15)
        .unwrap()
        .and_hms_opt(12, 30, 45)
        .unwrap();

    // 1. Test 'localtime' modifier: Input (assumed UTC) -> Output (Local)
    let args_local = vec![
        Value::build_text(base_str.to_string()),
        Value::build_text("localtime".to_string()),
    ];
    let res_local = exec_datetime_full(args_local);

    // Expected calculation: Treat naive as UTC, convert to Local
    let utc_dt = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(naive, chrono::Utc);
    let expected_local = utc_dt
        .with_timezone(&chrono::Local)
        .format("%Y-%m-%d %H:%M:%S")
        .to_string();

    assert_eq!(
        res_local.to_text().unwrap(),
        expected_local,
        "localtime modifier mismatch"
    );

    // 2. Test 'utc' modifier: Input (assumed Local) -> Output (UTC)
    let args_utc = vec![
        Value::build_text(base_str.to_string()),
        Value::build_text("utc".to_string()),
    ];
    let res_utc = exec_datetime_full(args_utc);

    // Expected calculation: Treat naive as Local, convert to UTC
    // We handle potential Local ambiguities (though 2023-06-15 is typically safe)
    match chrono::Local.from_local_datetime(&naive) {
        chrono::LocalResult::Single(local_input) => {
            let expected_utc = local_input
                .with_timezone(&chrono::Utc)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string();
            assert_eq!(
                res_utc.to_text().unwrap(),
                expected_utc,
                "utc modifier mismatch"
            );
        }
        _ => {
            // Fallback if local time is ambiguous/invalid in test environment
            // Ensure result is at least a valid string and not Null
            assert!(res_utc.to_text().is_some());
            assert_ne!(res_utc, Value::Null);
        }
    }
}

#[test]
fn test_combined_modifiers() {
    let args = vec![
        Value::build_text("2000-01-01 00:00:00".to_string()),
        Value::build_text("-1 day".to_string()),
        Value::build_text("+5 hours".to_string()),
        Value::build_text("+30 minutes".to_string()),
        Value::build_text("+15 seconds".to_string()),
        Value::build_text("subsec".to_string()),
    ];
    let result = exec_datetime_full(args);
    assert_eq!(result.to_text().unwrap(), "1999-12-31 05:30:15.000");
}

#[test]
fn test_max_datetime_limit() {
    let args = vec![Value::build_text("9999-12-31 23:59:59".to_string())];
    let result = exec_datetime_full(args);
    assert_eq!(result.to_text().unwrap(), "9999-12-31 23:59:59");
}

#[test]
fn test_leap_second_ignored() {
    let args = vec![Value::build_text("2024-06-30 23:59:60".to_string())];
    let result = exec_datetime_full(args);
    assert_eq!(result, Value::Null);
}

#[test]
fn test_already_on_weekday_no_change() {
    let args = vec![
        Value::build_text("2023-01-01 12:00:00".to_string()),
        Value::build_text("weekday 0".to_string()),
    ];
    let result = exec_datetime_full(args);
    assert_eq!(result.to_text().unwrap(), "2023-01-01 12:00:00");
}

#[test]
fn test_move_forward_if_different() {
    let args1 = vec![
        Value::build_text("2023-01-01 12:00:00".to_string()),
        Value::build_text("weekday 1".to_string()),
    ];
    let res1 = exec_datetime_full(args1);
    assert_eq!(res1.to_text().unwrap(), "2023-01-02 12:00:00");

    let args2 = vec![
        Value::build_text("2023-01-03 12:00:00".to_string()),
        Value::build_text("weekday 5".to_string()),
    ];
    let res2 = exec_datetime_full(args2);
    assert_eq!(res2.to_text().unwrap(), "2023-01-06 12:00:00");
}

#[test]
fn test_wrap_around_weekend() {
    let args1 = vec![
        Value::build_text("2023-01-06 12:00:00".to_string()),
        Value::build_text("weekday 0".to_string()),
    ];
    let res1 = exec_datetime_full(args1);
    assert_eq!(res1.to_text().unwrap(), "2023-01-08 12:00:00");

    let args2 = vec![
        Value::build_text("2023-01-08 12:00:00".to_string()),
        Value::build_text("weekday 0".to_string()),
    ];
    let res2 = exec_datetime_full(args2);
    assert_eq!(res2.to_text().unwrap(), "2023-01-08 12:00:00");
}

#[test]
fn test_same_day_stays_put() {
    let args = vec![
        Value::build_text("2023-01-05 12:00:00".to_string()),
        Value::build_text("weekday 4".to_string()),
    ];
    let res = exec_datetime_full(args);
    assert_eq!(res.to_text().unwrap(), "2023-01-05 12:00:00");
}

#[test]
fn test_already_on_friday_no_change() {
    let args = vec![
        Value::build_text("2023-01-06 12:00:00".to_string()),
        Value::build_text("weekday 5".to_string()),
    ];
    let res = exec_datetime_full(args);
    assert_eq!(res.to_text().unwrap(), "2023-01-06 12:00:00");
}

#[test]
fn test_apply_modifier_julianday() {
    let jd_args = vec![Value::build_text("2000-01-01 12:00:00".to_string())];
    let jd_val = exec_julianday(jd_args);

    let dt_args = vec![jd_val, Value::build_text("auto".to_string())];
    let dt_res = exec_datetime_full(dt_args);
    assert_eq!(dt_res.to_text().unwrap(), "2000-01-01 12:00:00");
}

#[test]
fn test_apply_modifier_start_of_month() {
    let args = vec![
        Value::build_text("2023-06-15 12:30:45".to_string()),
        Value::build_text("start of month".to_string()),
    ];
    let res = exec_datetime_full(args);
    assert_eq!(res.to_text().unwrap(), "2023-06-01 00:00:00");
}

#[test]
fn test_apply_modifier_subsec() {
    let args = vec![
        Value::build_text("2023-06-15 12:30:45".to_string()),
        Value::build_text("subsec".to_string()),
    ];
    let res = exec_datetime_general(args, "datetime");
    assert_eq!(res.to_text().unwrap(), "2023-06-15 12:30:45.000");
}

#[test]
fn test_apply_modifier_floor_modifier_n_floor_gt_0() {
    let args = vec![
        Value::build_text("2023-01-31".to_string()),
        Value::build_text("+1 month".to_string()),
        Value::build_text("floor".to_string()),
    ];
    let res = exec_datetime_full(args);
    assert_eq!(res.to_text().unwrap(), "2023-02-28 00:00:00");
}

#[test]
fn test_apply_modifier_floor_modifier_n_floor_le_0() {
    let args = vec![
        Value::build_text("2023-01-15".to_string()),
        Value::build_text("+1 month".to_string()),
        Value::build_text("floor".to_string()),
    ];
    let res = exec_datetime_full(args);
    assert_eq!(res.to_text().unwrap(), "2023-02-15 00:00:00");
}

#[test]
fn test_apply_modifier_ceiling_modifier_sets_n_floor_to_zero() {
    let args = vec![
        Value::build_text("2023-01-31".to_string()),
        Value::build_text("ceiling".to_string()),
        Value::build_text("+1 month".to_string()),
    ];
    let res = exec_datetime_full(args);
    assert_eq!(res.to_text().unwrap(), "2023-03-03 00:00:00");
}

#[test]
fn test_apply_modifier_start_of_month_basic() {
    let args = vec![
        Value::build_text("2023-06-15 12:30:45".to_string()),
        Value::build_text("start of month".to_string()),
    ];
    let res = exec_datetime_full(args);
    assert_eq!(res.to_text().unwrap(), "2023-06-01 00:00:00");
}

#[test]
fn test_apply_modifier_start_of_month_already_at_first() {
    let args = vec![
        Value::build_text("2023-06-01 00:00:00".to_string()),
        Value::build_text("start of month".to_string()),
    ];
    let res = exec_datetime_full(args);
    assert_eq!(res.to_text().unwrap(), "2023-06-01 00:00:00");
}

#[test]
fn test_apply_modifier_start_of_month_edge_case() {
    let args = vec![
        Value::build_text("2023-07-31 23:59:59".to_string()),
        Value::build_text("start of month".to_string()),
    ];
    let res = exec_datetime_full(args);
    assert_eq!(res.to_text().unwrap(), "2023-07-01 00:00:00");
}

#[test]
fn test_apply_modifier_subsec_no_change() {
    let args = vec![
        Value::build_text("2023-06-15 12:30:45.123".to_string()),
        Value::build_text("subsec".to_string()),
    ];
    let res = exec_datetime_full(args);
    assert_eq!(res.to_text().unwrap(), "2023-06-15 12:30:45.123");
}

#[test]
fn test_apply_modifier_subsec_preserves_fractional_seconds() {
    let args = vec![
        Value::build_text("2025-01-02 04:12:21.891".to_string()),
        Value::build_text("subsec".to_string()),
    ];
    let res = exec_datetime_full(args);
    assert_eq!(res.to_text().unwrap(), "2025-01-02 04:12:21.891");
}

#[test]
fn test_apply_modifier_subsec_no_fractional_seconds() {
    let args = vec![
        Value::build_text("2025-01-02 04:12:21".to_string()),
        Value::build_text("subsec".to_string()),
    ];
    let res = exec_datetime_full(args);
    assert_eq!(res.to_text().unwrap(), "2025-01-02 04:12:21.000");
}

#[test]
fn test_apply_modifier_subsec_truncate_to_milliseconds() {
    let args = vec![
        Value::build_text("2025-01-02 04:12:21.891123456".to_string()),
        Value::build_text("subsec".to_string()),
    ];
    let res = exec_datetime_full(args);
    assert_eq!(res.to_text().unwrap(), "2025-01-02 04:12:21.891");
}

#[test]
fn test_strftime() {
    let fmt = Value::build_text("%Y-%m-%d".to_string());
    let date = Value::build_text("2023-10-25 14:30:00".to_string());
    let expected = Value::build_text("2023-10-25".to_string());
    assert_eq!(exec_strftime(&[fmt, date]), expected);

    let fmt = Value::build_text("%H:%M:%S".to_string());
    let date = Value::build_text("2023-10-25 14:30:45".to_string());
    let expected = Value::build_text("14:30:45".to_string());
    assert_eq!(exec_strftime(&[fmt, date]), expected);

    let fmt = Value::build_text("Date: %Y-%m-%d, Time: %H:%M".to_string());
    let date = Value::build_text("2023-10-25 14:30:45".to_string());
    let expected = Value::build_text("Date: 2023-10-25, Time: 14:30".to_string());
    assert_eq!(exec_strftime(&[fmt, date]), expected);

    let fmt = Value::build_text("%Y-%m-%d".to_string());
    let date = Value::build_text("2023-10-25".to_string());
    let mod1 = Value::build_text("start of month".to_string());
    let expected = Value::build_text("2023-10-01".to_string());
    assert_eq!(exec_strftime(&[fmt, date, mod1]), expected);

    let fmt = Value::build_text("%Y-%m-%d".to_string());
    let date = Value::build_text("2023-10-25".to_string());
    let mod1 = Value::build_text("+5 days".to_string());
    let expected = Value::build_text("2023-10-30".to_string());
    assert_eq!(exec_strftime(&[fmt, date, mod1]), expected);

    let fmt = Value::build_text("%J".to_string());
    let date = Value::build_text("2023-01-01 12:00:00".to_string());
    let expected = Value::build_text("2459946".to_string());
    assert_eq!(exec_strftime(&[fmt, date]), expected);

    let fmt = Value::build_text("%s".to_string());
    let date = Value::build_text("2023-01-01 00:00:00".to_string());
    let expected = Value::build_text("1672531200".to_string());
    assert_eq!(exec_strftime(&[fmt, date]), expected);

    let fmt = Value::build_text("%S.%f".to_string());
    let date = Value::build_text("2023-01-01 12:00:05.123".to_string());
    let expected = Value::build_text("05.05.123".to_string());
    assert_eq!(exec_strftime(&[fmt, date]), expected);

    let fmt = Value::build_text("%w".to_string());
    let date = Value::build_text("2023-01-01".to_string());
    let expected = Value::build_text("0".to_string());
    assert_eq!(exec_strftime(&[fmt, date]), expected);

    let fmt = Value::build_text("%j".to_string());
    let date = Value::build_text("2023-02-01".to_string());
    let expected = Value::build_text("032".to_string());
    assert_eq!(exec_strftime(&[fmt, date]), expected);

    let fmt = Value::Null;
    let date = Value::build_text("now".to_string());
    assert_eq!(exec_strftime(&[fmt, date]), Value::Null);

    let fmt = Value::build_text("%Y".to_string());
    let date = Value::Null;
    let expected = Value::Null;
    assert_eq!(exec_strftime(&[fmt, date]), expected);

    let fmt = Value::build_text("%Y".to_string());
    let date = Value::build_text("invalid-date".to_string());
    assert_eq!(exec_strftime(&[fmt, date]), Value::Null);

    let fmt = Value::build_text("100%%".to_string());
    let date = Value::build_text("2023-01-01".to_string());
    let expected = Value::build_text("100%".to_string());
    assert_eq!(exec_strftime(&[fmt, date]), expected);
}

#[test]
fn test_exec_timediff() {
    let start = Value::build_text("12:00:00");
    let end = Value::build_text("14:30:45");
    let expected = Value::build_text("-0000-00-00 02:30:45.000");
    assert_eq!(exec_timediff(&[start, end]), expected);

    let start = Value::build_text("14:30:45");
    let end = Value::build_text("12:00:00");
    let expected = Value::build_text("+0000-00-00 02:30:45.000");
    assert_eq!(exec_timediff(&[start, end]), expected);

    let start = Value::build_text("12:00:01.300");
    let end = Value::build_text("12:00:00.500");
    let expected = Value::build_text("+0000-00-00 00:00:00.800");
    assert_eq!(exec_timediff(&[start, end]), expected);

    let start = Value::build_text("13:30:00");
    let end = Value::build_text("16:45:30");
    let expected = Value::build_text("-0000-00-00 03:15:30.000");
    assert_eq!(exec_timediff(&[start, end]), expected);

    let start = Value::build_text("2023-05-10 23:30:00");
    let end = Value::build_text("2023-05-11 01:15:00");
    let expected = Value::build_text("-0000-00-00 01:45:00.000");
    assert_eq!(exec_timediff(&[start, end]), expected);

    let start = Value::Null;
    let end = Value::build_text("12:00:00");
    let expected = Value::Null;
    assert_eq!(exec_timediff(&[start, end]), expected);

    let start = Value::build_text("not a time");
    let end = Value::build_text("12:00:00");
    let expected = Value::Null;
    assert_eq!(exec_timediff(&[start, end]), expected);

    // Test identical times - should return zero duration, not Null
    let start = Value::build_text("12:00:00");
    let end = Value::build_text("12:00:00");
    let expected = Value::build_text("+0000-00-00 00:00:00.000");
    assert_eq!(exec_timediff(&[start, end]), expected);
}

#[test]
fn test_subsec_fixed_time_expansion() {
    let args = vec![
        Value::build_text("2024-01-01 12:00:00".to_string()),
        Value::build_text("subsec".to_string()),
    ];
    let result = exec_datetime_full(args);
    assert_eq!(result.to_text().unwrap(), "2024-01-01 12:00:00.000");
}

#[test]
fn test_subsec_date_only_expansion() {
    let args = vec![
        Value::build_text("2024-01-01".to_string()),
        Value::build_text("subsec".to_string()),
    ];
    let result = exec_datetime_full(args);
    assert_eq!(result.to_text().unwrap(), "2024-01-01 00:00:00.000");
}

#[test]
fn test_subsec_iso_separator() {
    let args = vec![
        Value::build_text("2024-01-01T15:30:00".to_string()),
        Value::build_text("subsec".to_string()),
    ];
    let result = exec_datetime_full(args);
    assert_eq!(result.to_text().unwrap(), "2024-01-01 15:30:00.000");
}

#[test]
fn test_subsec_chaining_before_math() {
    let args = vec![
        Value::build_text("2024-01-01 12:00:00".to_string()),
        Value::build_text("subsec".to_string()),
        Value::build_text("+1 hour".to_string()),
    ];
    let result = exec_datetime_full(args);
    assert_eq!(result.to_text().unwrap(), "2024-01-01 13:00:00.000");
}

#[test]
fn test_subsec_chaining_after_math() {
    let args = vec![
        Value::build_text("2024-01-01 12:00:00".to_string()),
        Value::build_text("+1 hour".to_string()),
        Value::build_text("subsec".to_string()),
    ];
    let result = exec_datetime_full(args);
    assert_eq!(result.to_text().unwrap(), "2024-01-01 13:00:00.000");
}

#[test]
fn test_subsec_rollover_math() {
    let args = vec![
        Value::build_text("2024-01-01 12:00:00.999".to_string()),
        Value::build_text("+1 second".to_string()),
        Value::build_text("subsec".to_string()),
    ];
    let result = exec_datetime_full(args);
    assert_eq!(result.to_text().unwrap(), "2024-01-01 12:00:01.999");
}

#[test]
fn test_subsec_case_insensitivity() {
    let args = vec![
        Value::build_text("2024-01-01 12:00:00".to_string()),
        Value::build_text("SuBsEc".to_string()),
    ];
    let result = exec_datetime_full(args);
    assert_eq!(result.to_text().unwrap(), "2024-01-01 12:00:00.000");
}

#[test]
fn test_parse_modifier_unicode_no_panic() {
    let unicode_inputs = ["!*\u{ea37}", "\u{1F600}", "日本語", "中", "\u{0080}", ""];

    for input in unicode_inputs {
        let args = vec![
            Value::build_text("now".to_string()),
            Value::build_text(input.to_string()),
        ];
        let result = exec_datetime_full(args);
        // Expect Null for invalid modifiers, but no panic
        assert_eq!(result, Value::Null);
    }
}

#[test]
fn test_unixepoch_basic_usage() {
    let result = exec_unixepoch(vec![Value::build_text("1970-01-01 00:00:00".to_string())]);
    assert_eq!(result, Value::from_i64(0));

    let result = exec_unixepoch(vec![Value::build_text("2023-01-01 00:00:00".to_string())]);
    assert_eq!(result, Value::from_i64(1672531200));

    let result = exec_unixepoch(vec![Value::build_text("1969-12-31 23:59:59".to_string())]);
    assert_eq!(result, Value::from_i64(-1));

    let result = exec_unixepoch(vec![Value::from_f64(2440587.5)]);
    assert_eq!(result, Value::from_i64(0));
}

#[test]
fn test_numeric_datetime_accepts_ascii_whitespace() {
    let expected = Value::from_i64(-210866328000);
    for input in ["5 ", " 5", " 5 ", "\t5\n"] {
        let result = exec_unixepoch(vec![Value::build_text(input.to_string())]);
        assert_eq!(result, expected, "input {input:?}");
    }

    let result = exec_julianday(vec![Value::build_text("5 ".to_string())]);
    assert_eq!(result, Value::from_f64(5.0));

    let result = exec_unixepoch(vec![Value::build_text("5 trailing".to_string())]);
    assert_eq!(result, Value::Null);
}

#[test]
fn test_unixepoch_numeric_modifiers_unixepoch() {
    let res1 = exec_unixepoch(vec![
        Value::from_i64(1672531200),
        Value::build_text("unixepoch".to_string()),
    ]);
    assert_eq!(res1, Value::from_i64(1672531200));

    let res2 = exec_unixepoch(vec![
        Value::from_i64(0),
        Value::build_text("unixepoch".to_string()),
    ]);
    assert_eq!(res2, Value::from_i64(0));

    let res3 = exec_unixepoch(vec![
        Value::from_i64(1672531200),
        Value::build_text("unixepoch".to_string()),
        Value::build_text("start of year".to_string()),
    ]);
    assert_eq!(res3, Value::from_i64(1672531200));
}

#[test]
fn test_unixepoch_numeric_modifiers_julianday() {
    let res1 = exec_unixepoch(vec![
        Value::from_f64(2440587.5),
        Value::build_text("julianday".to_string()),
    ]);
    assert_eq!(res1, Value::from_i64(0));

    let res2 = exec_unixepoch(vec![
        Value::from_f64(2460311.5),
        Value::build_text("julianday".to_string()),
    ]);
    assert_eq!(res2, Value::from_i64(1704153600));

    let res3 = exec_unixepoch(vec![
        Value::from_f64(0.0),
        Value::build_text("julianday".to_string()),
    ]);
    match res3 {
        Value::Numeric(Numeric::Integer(i)) => assert_eq!(i, -210866760000),
        _ => panic!("Expected Integer result for JD 0"),
    }
}

#[test]
fn test_unixepoch_numeric_modifiers_auto() {
    let res1 = exec_unixepoch(vec![
        Value::from_f64(2440587.5),
        Value::build_text("auto".to_string()),
    ]);
    assert_eq!(res1, Value::from_i64(0));

    let res2 = exec_unixepoch(vec![
        Value::from_i64(1672531200),
        Value::build_text("auto".to_string()),
    ]);
    assert_eq!(res2, Value::from_i64(1672531200));

    let res3 = exec_unixepoch(vec![
        Value::from_f64(0.0),
        Value::build_text("auto".to_string()),
    ]);
    match res3 {
        Value::Numeric(Numeric::Integer(i)) => assert!(i < 0),
        _ => panic!("Expected Integer result"),
    }
}

#[test]
fn test_unixepoch_invalid_usage() {
    let res1 = exec_unixepoch(vec![
        Value::from_i64(0),
        Value::build_text("start of year".to_string()),
        Value::build_text("unixepoch".to_string()),
    ]);
    assert_eq!(res1, Value::Null);

    let res2 = exec_unixepoch(vec![
        Value::build_text("2023-01-01".to_string()),
        Value::build_text("unixepoch".to_string()),
    ]);
    assert_eq!(res2, Value::Null);

    let res3 = exec_unixepoch(vec![
        Value::from_i64(0),
        Value::build_text("unixepoch".to_string()),
        Value::build_text("julianday".to_string()),
    ]);
    assert_eq!(res3, Value::Null);
}

#[test]
fn test_unixepoch_complex_calculations() {
    let res1 = exec_unixepoch(vec![
        Value::from_f64(2440587.5),
        Value::build_text("julianday".to_string()),
        Value::build_text("+1 day".to_string()),
    ]);
    assert_eq!(res1, Value::from_i64(86400));

    let res2 = exec_unixepoch(vec![
        Value::from_f64(2460311.5),
        Value::build_text("auto".to_string()),
        Value::build_text("start of month".to_string()),
        Value::build_text("+1 month".to_string()),
    ]);
    assert_eq!(res2, Value::from_i64(1706745600));
}

#[test]
fn test_unixepoch_subsecond_precision() {
    let res1 = exec_unixepoch(vec![
        Value::build_text("1970-01-01 00:00:00.0006".to_string()),
        Value::build_text("subsec".to_string()),
    ]);
    match res1 {
        Value::Numeric(Numeric::Float(f)) => {
            assert!((f64::from(f) - 0.001).abs() < f64::EPSILON)
        }
        _ => panic!("Expected Float result"),
    }

    let res2 = exec_unixepoch(vec![
        Value::build_text("1970-01-01 00:00:00.9996".to_string()),
        Value::build_text("subsec".to_string()),
    ]);
    match res2 {
        Value::Numeric(Numeric::Float(f)) => {
            assert!((f64::from(f) - 0.999).abs() < f64::EPSILON)
        }
        _ => panic!("Expected Float result"),
    }
}

#[test]
fn test_fast_path_date_only() {
    assert_eq!(
        exec_date(vec![Value::build_text("2024-01-01".to_string())])
            .to_text()
            .unwrap(),
        "2024-01-01"
    );
    assert_eq!(
        exec_date(vec![Value::build_text("0001-01-01".to_string())])
            .to_text()
            .unwrap(),
        "0001-01-01"
    );
    assert_eq!(
        exec_date(vec![Value::build_text("9999-12-31".to_string())])
            .to_text()
            .unwrap(),
        "9999-12-31"
    );
    assert_eq!(
        exec_date(vec![Value::build_text("2024-02-29".to_string())])
            .to_text()
            .unwrap(),
        "2024-02-29"
    );
    assert_eq!(
        exec_date(vec![Value::build_text("2023-02-29".to_string())])
            .to_text()
            .unwrap(),
        "2023-03-01"
    );

    assert_eq!(
        exec_date(vec![Value::build_text("2024-00-01".to_string())]),
        Value::Null
    );
    assert_eq!(
        exec_date(vec![Value::build_text("2024-13-01".to_string())]),
        Value::Null
    );
    assert_eq!(
        exec_date(vec![Value::build_text("2024-01-00".to_string())]),
        Value::Null
    );

    assert_eq!(
        exec_date(vec![Value::build_text("2024-01-32".to_string())]),
        Value::Null
    );

    assert_eq!(
        exec_date(vec![Value::build_text("2024/01/01".to_string())]),
        Value::Null
    );
    assert_eq!(
        exec_date(vec![Value::build_text("2024.01.01".to_string())]),
        Value::Null
    );
    assert_eq!(
        exec_date(vec![Value::build_text("202X-01-01".to_string())]),
        Value::Null
    );
    assert_eq!(
        exec_date(vec![Value::build_text("2024-0a-01".to_string())]),
        Value::Null
    );
}

#[test]
fn test_fast_path_datetime_formats() {
    assert_eq!(
        exec_datetime_full(vec![Value::build_text("2024-01-15 10:30".to_string())])
            .to_text()
            .unwrap(),
        "2024-01-15 10:30:00"
    );
    assert_eq!(
        exec_datetime_full(vec![Value::build_text("2024-01-15T10:30".to_string())])
            .to_text()
            .unwrap(),
        "2024-01-15 10:30:00"
    );
    assert_eq!(
        exec_datetime_full(vec![Value::build_text("2024-01-15X10:30".to_string())]),
        Value::Null
    );
    assert_eq!(
        exec_datetime_full(vec![Value::build_text("2024-01-15 10:30:45".to_string())])
            .to_text()
            .unwrap(),
        "2024-01-15 10:30:45"
    );
    assert_eq!(
        exec_datetime_full(vec![Value::build_text("2024-01-15T10:30:45".to_string())])
            .to_text()
            .unwrap(),
        "2024-01-15 10:30:45"
    );

    assert_eq!(
        exec_datetime_full(vec![Value::build_text("2024-01-15 25:30:45".to_string())]),
        Value::Null
    );
    assert_eq!(
        exec_datetime_full(vec![Value::build_text("2024-01-15 10:60:45".to_string())]),
        Value::Null
    );
    assert_eq!(
        exec_datetime_full(vec![Value::build_text("2024-01-15 10:30:60".to_string())]),
        Value::Null
    );
}

#[test]
fn test_fast_path_time_only() {
    assert_eq!(
        exec_time(vec![Value::build_text("10:30".to_string())])
            .to_text()
            .unwrap(),
        "10:30:00"
    );
    assert_eq!(
        exec_time(vec![Value::build_text("00:00".to_string())])
            .to_text()
            .unwrap(),
        "00:00:00"
    );
    assert_eq!(
        exec_time(vec![Value::build_text("23:59".to_string())])
            .to_text()
            .unwrap(),
        "23:59:00"
    );
    assert_eq!(
        exec_time(vec![Value::build_text("24:00".to_string())])
            .to_text()
            .unwrap(),
        "24:00:00"
    );
    assert_eq!(
        exec_time(vec![Value::build_text("10:60".to_string())]),
        Value::Null
    );

    assert_eq!(
        exec_time(vec![Value::build_text("10:30:45".to_string())])
            .to_text()
            .unwrap(),
        "10:30:45"
    );
    assert_eq!(
        exec_time(vec![Value::build_text("00:00:00".to_string())])
            .to_text()
            .unwrap(),
        "00:00:00"
    );
    assert_eq!(
        exec_time(vec![Value::build_text("23:59:59".to_string())])
            .to_text()
            .unwrap(),
        "23:59:59"
    );

    let res1 = exec_datetime_general(
        vec![
            Value::build_text("10:30:45.123".to_string()),
            Value::build_text("subsec".to_string()),
        ],
        "time",
    );
    assert_eq!(res1.to_text().unwrap(), "10:30:45.123");

    let res2 = exec_datetime_general(
        vec![
            Value::build_text("10:30:45.1".to_string()),
            Value::build_text("subsec".to_string()),
        ],
        "time",
    );
    assert_eq!(res2.to_text().unwrap(), "10:30:45.100");
}

#[test]
fn test_fast_path_skips_timezone_strings() {
    assert_eq!(
        exec_datetime_full(vec![Value::build_text("2024-01-15 10:30:45Z".to_string())])
            .to_text()
            .unwrap(),
        "2024-01-15 10:30:45"
    );
    assert_eq!(
        exec_datetime_full(vec![Value::build_text(
            "2024-01-15 10:30:45+02:00".to_string()
        )])
        .to_text()
        .unwrap(),
        "2024-01-15 08:30:45"
    );
    assert_eq!(
        exec_datetime_full(vec![Value::build_text(
            "2024-01-15 10:30:45-05:00".to_string()
        )])
        .to_text()
        .unwrap(),
        "2024-01-15 15:30:45"
    );
    assert_eq!(
        exec_time(vec![Value::build_text("10:30:45+02:00".to_string())])
            .to_text()
            .unwrap(),
        "08:30:45"
    );
}

#[test]
fn test_fast_path_fractional_seconds_precision() {
    let args1 = vec![
        Value::build_text("2024-01-15 10:30:45.123456789".to_string()),
        Value::build_text("subsec".to_string()),
    ];
    assert_eq!(
        exec_datetime_full(args1).to_text().unwrap(),
        "2024-01-15 10:30:45.123"
    );

    let args2 = vec![
        Value::build_text("2024-01-15 10:30:45.1".to_string()),
        Value::build_text("subsec".to_string()),
    ];
    assert_eq!(
        exec_datetime_full(args2).to_text().unwrap(),
        "2024-01-15 10:30:45.100"
    );

    let args3 = vec![
        Value::build_text("2024-01-15 10:30:45.100".to_string()),
        Value::build_text("subsec".to_string()),
    ];
    assert_eq!(
        exec_datetime_full(args3).to_text().unwrap(),
        "2024-01-15 10:30:45.100"
    );

    let args4 = vec![
        Value::build_text("2024-01-15 10:30:45.000".to_string()),
        Value::build_text("subsec".to_string()),
    ];
    assert_eq!(
        exec_datetime_full(args4).to_text().unwrap(),
        "2024-01-15 10:30:45.000"
    );
}

#[test]
fn test_fast_path_month_day_boundaries() {
    let run = |s: &str| -> String {
        exec_datetime_full(&[Value::build_text(s.to_string())])
            .to_text()
            .unwrap()
            .to_string()
    };

    assert_eq!(run("2024-04-30"), "2024-04-30 00:00:00");
    assert_eq!(run("2024-04-31"), "2024-05-01 00:00:00");
    assert_eq!(run("2024-01-31"), "2024-01-31 00:00:00");
    assert_eq!(run("2024-03-31"), "2024-03-31 00:00:00");
    assert_eq!(run("2024-02-29"), "2024-02-29 00:00:00");
    assert_eq!(run("2023-02-29"), "2023-03-01 00:00:00");
    assert_eq!(run("2024-02-28"), "2024-02-28 00:00:00");
}

#[test]
fn test_fast_path_edge_cases() {
    // Helper for cases expected to fail (return Null)
    let run = |s: &str| -> Value { exec_datetime_full(&[Value::build_text(s.to_string())]) };

    // Helper for cases expected to succeed (return String)
    let run_str = |s: &str| -> String { run(s).to_text().unwrap().to_string() };

    assert_eq!(run(""), Value::Null);
    assert_eq!(run("a"), Value::Null);
    assert_eq!(run("ab"), Value::Null);
    assert_eq!(run("abc"), Value::Null);
    assert_eq!(run("abcd"), Value::Null);
    assert_eq!(run_str("0000-01-01"), "0000-01-01 00:00:00");
    assert_eq!(run(" 2024-01-01"), Value::Null);
    assert_eq!(run_str("2024-01-01 "), "2024-01-01 00:00:00");
    assert_eq!(run_str("2024-01-15\t10:30:45"), "2024-01-15 10:30:45");
    assert_eq!(run("2024-1-01"), Value::Null);
    assert_eq!(run("2024-01-1"), Value::Null);
    assert_eq!(run("aaaa-bb-cc"), Value::Null);
    assert_eq!(run("2024-01-01abc"), Value::Null);
    assert_eq!(run_str("-2024-01-01"), "-2024-01-01 00:00:00");
    assert_eq!(run("10:30:45.12abc"), Value::Null);
    assert_eq!(run("2024-01-15 10:30:45.123xyz"), Value::Null);

    // Manual check for subsec since it requires 2 arguments
    let dt_args = &[
        Value::build_text("10:30:45.12".to_string()),
        Value::build_text("subsec".to_string()),
    ];
    assert_eq!(
        exec_datetime_general(dt_args, "time").to_text().unwrap(),
        "10:30:45.120"
    );
}

// Regression test for fuzzing crash: strftime with non-char-boundary UTF-8 modifiers
// The modifier "swww\0\u{1}\t\0\u{fffd}\u{fffd}\u{f}W" has multi-byte chars where
// byte index 9 is not a valid char boundary, causing panic on slice.
#[test]
fn test_strftime_invalid_utf8_boundary_modifier() {
    // This modifier starts with 's' so it matches the 's' => branch,
    // but byte 9 falls inside a multi-byte character
    let modifier_with_multibyte = "swww\0\u{1}\t\0\u{fffd}\u{fffd}\u{f}W";
    let args = &[
        Value::build_text("".to_string()),
        Value::from_f64(-1.8041807844761696e230),
        Value::build_text(modifier_with_multibyte.to_string()),
    ];
    // Should not panic, just return an error or null
    let _ = exec_strftime(args.iter());

    // Also test the 'w' => weekday branch with similar input
    let weekday_modifier = "weekda\u{fffd}\u{fffd}";
    let args2 = &[
        Value::build_text("".to_string()),
        Value::from_f64(0.0),
        Value::build_text(weekday_modifier.to_string()),
    ];
    let _ = exec_strftime(args2.iter());
}
