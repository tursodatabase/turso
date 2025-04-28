#[cfg(test)]
mod tests {
    use crate::db_test;

    db_test!(date_current_date, "SELECT length(date('now')) = 10", 1);

    db_test!(
        date_specific_date,
        "SELECT date('2023-05-18')",
        "2023-05-18"
    );

    db_test!(
        date_with_time,
        "SELECT date('2023-05-18 15:30:45')",
        "2023-05-18"
    );

    db_test!(
        date_iso8601,
        "SELECT date('2023-05-18T15:30:45')",
        "2023-05-18"
    );

    db_test!(
        date_with_milliseconds,
        "SELECT date('2023-05-18 15:30:45.123')",
        "2023-05-18"
    );

    db_test!(
        date_julian_day_integer,
        "SELECT date(2460082)",
        "2023-05-17"
    );

    db_test!(
        date_julian_day_float,
        "SELECT date(2460082.5)",
        "2023-05-18"
    );

    // TODO: Limbo outputs Text("")
    // db_test!(date_invalid_input, "SELECT date('not a date')", [Null]);

    // TODO: Limbo outputs Text("")
    // db_test!(date_null_input, "SELECT date(NULL)", [Null]);

    // TODO: Limbo outputs Text("")
    // db_test!(date_out_of_range, "SELECT date('10001-01-01')", [Null]);

    db_test!(date_time_only, "SELECT date('15:30:45')", "2000-01-01");

    db_test!(
        date_with_timezone_utc,
        "SELECT date('2023-05-18 15:30:45Z')",
        "2023-05-18"
    );

    db_test!(
        date_with_timezone_positive,
        "SELECT date('2023-05-18 23:30:45+02:00')",
        "2023-05-18"
    );

    db_test!(
        date_with_timezone_negative,
        "SELECT date('2023-05-19 01:30:45-05:00')",
        "2023-05-19"
    );

    db_test!(
        date_with_timezone_day_change_positive,
        "SELECT date('2023-05-18 23:30:45-03:00')",
        "2023-05-19"
    );

    db_test!(
        date_with_timezone_day_change_negative,
        "SELECT date('2023-05-19 01:30:45+03:00')",
        "2023-05-18"
    );

    db_test!(
        date_with_timezone_iso8601,
        "SELECT date('2023-05-18T15:30:45+02:00')",
        "2023-05-18"
    );

    db_test!(
        date_with_timezone_and_milliseconds,
        "SELECT date('2023-05-18 15:30:45.123+02:00')",
        "2023-05-18"
    );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     date_with_invalid_timezone,
    //     "SELECT date('2023-05-18 15:30:45+25:00')",
    //     [Null]
    // );

    db_test!(
        date_with_modifier_add_days,
        "SELECT date('2023-05-18', '+10 days')",
        "2023-05-28"
    );

    db_test!(
        date_with_modifier_subtract_days,
        "SELECT date('2023-05-18', '-10 days')",
        "2023-05-08"
    );

    db_test!(
        date_with_multiple_modifiers_1,
        "SELECT date('2023-05-18', '+1 days', '-1 days', '+10 days')",
        "2023-05-28"
    );

    // TODO: Limbo outputs TEXT("")
    // db_test!(
    //     date_with_invalid_modifier,
    //     "SELECT date('2023-05-18', 'invalid modifier')",
    //     [Null]
    // );

    db_test!(time_no_arg, "SELECT length(time()) = 8", 1);

    db_test!(time_current_time, "SELECT length(time('now')) = 8", 1);

    db_test!(time_specific_time, "SELECT time('04:02:00')", "04:02:00");

    db_test!(
        time_of_datetime,
        "SELECT time('2023-05-18 15:30:45')",
        "15:30:45"
    );

    db_test!(
        time_iso8601,
        "SELECT time('2023-05-18T15:30:45')",
        "15:30:45"
    );

    db_test!(
        time_with_milliseconds,
        "SELECT time('2023-05-18 15:30:45.123')",
        "15:30:45"
    );

    db_test!(time_julian_day_integer, "SELECT time(2460082)", "12:00:00");

    db_test!(time_julian_day_float, "SELECT time(2460082.2)", "16:48:00");

    // TODO: Limbo outputs Text("")
    // db_test!(time_invalid_input, "SELECT time('not a time')", [Null]);

    // TODO: Limbo outputs Text("")
    // db_test!(time_null_input, "SELECT time(NULL)", [Null]);

    // TODO: Limbo outputs Text("")
    // db_test!(time_out_of_range, "SELECT time('25:05:01')", [Null]);

    db_test!(time_date_only, "SELECT time('2024-02-02')", "00:00:00");

    db_test!(
        time_with_timezone_utc,
        "SELECT time('2023-05-18 15:30:45Z')",
        "15:30:45"
    );

    db_test!(
        time_with_timezone_positive,
        "SELECT time('2023-05-18 23:30:45+07:00')",
        "16:30:45"
    );

    db_test!(
        time_with_timezone_negative,
        "SELECT time('2023-05-19 01:30:45-05:00')",
        "06:30:45"
    );

    db_test!(
        time_with_timezone_day_change_positive,
        "SELECT time('2023-05-18 23:30:45-03:00')",
        "02:30:45"
    );

    db_test!(
        time_with_timezone_day_change_negative,
        "SELECT time('2023-05-19 01:30:45+03:00')",
        "22:30:45"
    );

    db_test!(
        time_with_timezone_iso8601,
        "SELECT time('2023-05-18T15:30:45+02:00')",
        "13:30:45"
    );

    db_test!(
        time_with_timezone_and_milliseconds,
        "SELECT time('2023-05-18 15:30:45.123+02:00')",
        "13:30:45"
    );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     time_with_invalid_timezone,
    //     "SELECT time('2023-05-18 15:30:45+25:00')",
    //     [Null]
    // );

    db_test!(
        time_with_modifier_start_of_day,
        "SELECT time('2023-05-18 15:30:45', 'start of day')",
        "00:00:00"
    );

    db_test!(
        time_with_modifier_add_hours_1,
        "SELECT time('2023-05-18 15:30:45', '+2 hours')",
        "17:30:45"
    );

    db_test!(
        time_with_modifier_add_minutes_1,
        "SELECT time('2023-05-18 15:30:45', '+45 minutes')",
        "16:15:45"
    );

    db_test!(
        time_with_modifier_add_seconds,
        "SELECT time('2023-05-18 15:30:45', '+30 seconds')",
        "15:31:15"
    );

    db_test!(
        time_with_modifier_subtract_hours_1,
        "SELECT time('2023-05-18 15:30:45', '-3 hours')",
        "12:30:45"
    );

    db_test!(
        time_with_modifier_subtract_minutes,
        "SELECT time('2023-05-18 15:30:45', '-15 minutes')",
        "15:15:45"
    );

    db_test!(
        time_with_modifier_subtract_seconds_1,
        "SELECT time('2023-05-18 15:30:45', '-45 seconds')",
        "15:30:00"
    );

    db_test!(
        time_with_multiple_modifiers_1,
        "SELECT time('2023-05-18 15:30:45', '+1 hours', '-30 minutes', '+15 seconds')",
        "16:01:00"
    );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     time_with_invalid_modifier_1,
    //     "SELECT time('2023-05-18 15:30:45', 'invalid modifier')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     time_with_invalid_modifier_2,
    //     "SELECT time('2023-05-18 15:30:45', '+1 hour', 'invalid modifier')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("0")
    // db_test!(unixepoch_at_start, "SELECT unixepoch('1970-01-01')", 0);

    // TODO: Limbo outputs Text("-1")
    // db_test!(
    //     unixepoch_at_1_second_before_epochtime,
    //     "SELECT unixepoch('1969-12-31 23:59:59')",
    //     -1
    // );

    // TODO: Limbo outputs Text("253402300799")
    // db_test!(
    //     unixepoch_at_future,
    //     "SELECT unixepoch('9999-12-31 23:59:59')",
    //     253402300799
    // );

    // TODO: Limbo outputs Text("-62167219200")
    // db_test!(
    //     unixepoch_at_start_of_time,
    //     "SELECT unixepoch('0000-01-01 00:00:00')",
    //     -62167219200
    // );

    // TODO: Limbo outputs Text("1643288368")
    // db_test!(
    //     unixepoch_at_millisecond_precision_input_produces_seconds_precision_output,
    //     "SELECT unixepoch('2022-01-27 12:59:28.052')",
    //     1643288368
    // );

    db_test!(
        date_with_modifier_start_of_day,
        "SELECT date('2023-05-18 15:30:45', 'start of day')",
        "2023-05-18"
    );

    db_test!(
        date_with_modifier_start_of_month,
        "SELECT date('2023-05-18', 'start of month')",
        "2023-05-01"
    );

    db_test!(
        date_with_modifier_start_of_year,
        "SELECT date('2023-05-18', 'start of year')",
        "2023-01-01"
    );

    db_test!(
        date_with_modifier_add_months_1,
        "SELECT date('2023-05-18', '+2 months')",
        "2023-07-18"
    );

    db_test!(
        date_with_modifier_subtract_months_1,
        "SELECT date('2023-05-18', '-3 months')",
        "2023-02-18"
    );

    db_test!(
        date_with_modifier_add_years,
        "SELECT date('2023-05-18', '+1 year')",
        "2024-05-18"
    );

    db_test!(
        date_with_modifier_subtract_years,
        "SELECT date('2023-05-18', '-2 years')",
        "2021-05-18"
    );

    db_test!(
        date_with_modifier_weekday,
        "SELECT date('2023-05-18', 'weekday 0')",
        "2023-05-21"
    );

    db_test!(
        date_with_multiple_modifiers_2,
        "SELECT date('2023-05-18', '+1 month', '-10 days', 'start of year')",
        "2023-01-01"
    );

    db_test!(
        date_with_subsec,
        "SELECT date('2023-05-18 15:30:45.123', 'subsec')",
        "2023-05-18"
    );

    db_test!(
        time_with_modifier_add_hours_2,
        "SELECT time('2023-05-18 15:30:45', '+5 hours')",
        "20:30:45"
    );

    db_test!(
        time_with_modifier_subtract_hours_2,
        "SELECT time('2023-05-18 15:30:45', '-2 hours')",
        "13:30:45"
    );

    db_test!(
        time_with_modifier_add_minutes_2,
        "SELECT time('2023-05-18 15:30:45', '+45 minutes')",
        "16:15:45"
    );

    db_test!(
        time_with_modifier_subtract_seconds_2,
        "SELECT time('2023-05-18 15:30:45', '-50 seconds')",
        "15:29:55"
    );

    db_test!(
        time_with_subsec,
        "SELECT time('2023-05-18 15:30:45.123', 'subsec')",
        "15:30:45.123"
    );

    db_test!(
        time_with_modifier_add,
        "SELECT time('15:30:45', '+15:30:15')",
        "07:01:00"
    );

    db_test!(
        time_with_modifier_sub,
        "SELECT time('15:30:45', '-15:30:15')",
        "00:00:30"
    );

    db_test!(
        date_with_modifier_add_months_2,
        "SELECT date('2023-01-31', '+1 month')",
        "2023-03-03"
    );

    db_test!(
        date_with_modifier_subtract_months_2,
        "SELECT date('2023-03-31', '-1 month')",
        "2023-03-03"
    );

    db_test!(
        date_with_modifier_add_months_large,
        "SELECT date('2023-01-31', '+13 months')",
        "2024-03-02"
    );

    db_test!(
        date_with_modifier_subtract_months_large,
        "SELECT date('2023-01-31', '-13 months')",
        "2021-12-31"
    );

    db_test!(
        date_with_modifier_february_leap_year,
        "SELECT date('2020-02-29', '+12 months')",
        "2021-03-01"
    );

    db_test!(
        date_with_modifier_february_non_leap_year,
        "SELECT date('2019-02-28', '+12 months')",
        "2020-02-28"
    );

    db_test!(
        date_with_modifier_invalid_date,
        "SELECT date('2023-02-15 15:30:45', '-0001-01-01 00:00')",
        "2022-01-14"
    );

    db_test!(
        date_with_modifier_date,
        "SELECT date('2023-02-15 15:30:45', '+0001-01-01')",
        "2024-03-16"
    );

    db_test!(
        datetime_with_modifier_datetime_pos,
        "SELECT datetime('2023-02-15 15:30:45', '+0001-01-01 15:30')",
        "2024-03-17 07:00:45"
    );

    db_test!(
        datetime_with_modifier_datetime_neg,
        "SELECT datetime('2023-02-15 15:30:45', '+0001-01-01 15:30')",
        "2024-03-17 07:00:45"
    );

    db_test!(
        datetime_with_modifier_datetime_large,
        "SELECT datetime('2023-02-15 15:30:45', '+7777-10-10 23:59')",
        "9800-12-26 15:29:45"
    );

    db_test!(
        datetime_with_modifier_datetime_sub_large,
        "SELECT datetime('2023-02-15 15:30:45', '-2024-10-10 23:59')",
        "-0002-04-04 15:31:45"
    );

    db_test!(
        datetime_with_timezone_utc,
        "SELECT datetime('2023-05-18 15:30:45Z')",
        "2023-05-18 15:30:45"
    );

    db_test!(
        datetime_with_modifier_sub,
        "SELECT datetime('2023-12-12', '-0002-10-10 15:30:45')",
        "2021-02-01 08:29:15"
    );

    db_test!(
        datetime_with_modifier_add,
        "SELECT datetime('2023-12-12', '+0002-10-10 15:30:45')",
        "2026-10-22 15:30:45"
    );

    db_test!(
        time_with_multiple_modifiers_2,
        "SELECT time('2023-05-18 15:30:45', '+1 hours', '-20 minutes', '+15 seconds', 'subsec')",
        "16:11:00.000"
    );

    db_test!(
        datetime_with_multiple_modifiers,
        "SELECT datetime('2024-01-31', '+1 month', '+13 hours', '+5 minutes', '+62 seconds')",
        "2024-03-02 13:06:02"
    );

    db_test!(
        datetime_with_weekday,
        "SELECT datetime('2023-05-18', 'weekday 3')",
        "2023-05-24 00:00:00"
    );

    // TODO: Limbo outputs Text("1684423845")
    // db_test!(
    //     unixepoch_subsec,
    //     "SELECT unixepoch('2023-05-18 15:30:45.123')",
    //     [1684423845]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     unixepoch_invalid_date,
    //     "SELECT unixepoch('not-a-date')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     unixepoch_leap_second,
    //     "SELECT unixepoch('2015-06-30 23:59:60')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("-1")
    // db_test!(
    //     unixepoch_negative_timestamp,
    //     "SELECT unixepoch('1969-12-31 23:59:59')",
    //     -1
    // );

    // TODO: Limbo outputs Text("253402300799")
    // db_test!(
    //     unixepoch_large_date,
    //     "SELECT unixepoch('9999-12-31 23:59:59')",
    //     253402300799
    // );

    db_test!(
        datetime_with_timezone,
        "SELECT datetime('2023-05-19 01:30:45+03:00')",
        "2023-05-18 22:30:45"
    );

    db_test!(
        julianday_fractional,
        "SELECT julianday('2023-05-18 15:30:45.123')",
        2460083.1463555903
    );

    db_test!(
        julianday_fractional_2,
        "SELECT julianday('2000-01-01 12:00:00.500')",
        2451545.0000057872
    );

    db_test!(
        julianday_rounded_up,
        "SELECT julianday('2023-05-18 15:30:45.129')",
        2460083.1463556597
    );

    //
    db_test!(
        julianday_with_timezone,
        "SELECT julianday('2023-05-18 15:30:45+02:00')",
        2460083.0630208333
    );

    db_test!(
        julianday_fractional_seconds,
        "SELECT julianday('2023-05-18 15:30:45.123')",
        2460083.1463555903
    );

    db_test!(
        julianday_time_only,
        "SELECT julianday('15:30:45')",
        2451545.146354167
    );

    db_test!(
        julianday_midnight,
        "SELECT julianday('2023-05-18 00:00:00')",
        2460082.5
    );

    db_test!(
        julianday_noon,
        "SELECT julianday('2023-05-18 12:00:00')",
        2460083.0
    );

    db_test!(
        julianday_fractional_zero,
        "SELECT julianday('2023-05-18 00:00:00.000')",
        2460082.5
    );

    db_test!(
        julianday_date_only,
        "SELECT julianday('2023-05-18')",
        2460082.5
    );

    db_test!(
        julianday_with_modifier_day,
        "SELECT julianday(2454832.5, '+1 day')",
        2454833.5
    );

    db_test!(
        julianday_with_modifier_hour,
        "SELECT julianday(2454832.5, '-3 hours')",
        2454832.375
    );

    db_test!(
        julianday_max_day,
        "SELECT julianday('9999-12-31 23:59:59')",
        5373484.4999884255
    );

    // Strftime tests
    db_test!(
        strftime_day,
        "SELECT strftime('%d', '2025-01-23T13:10:30.567')",
        "23"
    );

    db_test!(
        strftime_day_without_leading_zero_1,
        "SELECT strftime('%e', '2025-01-23T13:10:30.567')",
        "23"
    );

    db_test!(
        strftime_day_without_leading_zero_2,
        "SELECT strftime('%e', '2025-01-02T13:10:30.567')",
        " 2"
    );

    db_test!(
        strftime_fractional_seconds,
        "SELECT strftime('%f', '2025-01-02T13:10:30.567')",
        "30.567"
    );

    db_test!(
        strftime_iso_8601_date,
        "SELECT strftime('%F', '2025-01-23T13:10:30.567')",
        "2025-01-23"
    );

    db_test!(
        strftime_iso_8601_year,
        "SELECT strftime('%G', '2025-01-23T13:10:30.567')",
        "2025"
    );

    db_test!(
        strftime_iso_8601_year_2_digit,
        "SELECT strftime('%g', '2025-01-23T13:10:30.567')",
        "25"
    );

    db_test!(
        strftime_hour,
        "SELECT strftime('%H', '2025-01-23T13:10:30.567')",
        "13"
    );

    db_test!(
        strftime_hour_12_hour_clock,
        "SELECT strftime('%I', '2025-01-23T13:10:30.567')",
        "01"
    );

    db_test!(
        strftime_day_of_year,
        "SELECT strftime('%j', '2025-01-23T13:10:30.567')",
        "023"
    );

    // TODO: Limbo outputs Text("2460699.048964896")
    // db_test!(
    //     strftime_julianday_1,
    //     "SELECT strftime('%J', '2025-01-23T13:10:30.567')",
    //     2460699.048964896
    // );

    db_test!(
        strftime_hour_without_leading_zero_1,
        "SELECT strftime('%k', '2025-01-23T13:10:30.567')",
        "13"
    );

    db_test!(
        strftime_hour_without_leading_zero_2,
        "SELECT strftime('%k', '2025-01-23T02:10:30.567')",
        " 2"
    );

    db_test!(
        strftime_hour_12_hour_clock_without_leading_zero_2,
        "SELECT strftime('%l', '2025-01-23T13:10:30.567')",
        " 1"
    );

    db_test!(
        strftime_month,
        "SELECT strftime('%m', '2025-01-23T13:10:30.567')",
        "01"
    );

    db_test!(
        strftime_minute,
        "SELECT strftime('%M', '2025-01-23T13:14:30.567')",
        "14"
    );

    db_test!(
        strftime_am_pm_1,
        "SELECT strftime('%p', '2025-01-23T11:14:30.567')",
        "AM"
    );

    db_test!(
        strftime_am_pm_2,
        "SELECT strftime('%p', '2025-01-23T13:14:30.567')",
        "PM"
    );

    db_test!(
        strftime_am_pm_lower_1,
        "SELECT strftime('%P', '2025-01-23T11:14:30.567')",
        ["am"]
    );

    db_test!(
        strftime_am_pm_lower_2,
        "SELECT strftime('%P', '2025-01-23T13:14:30.567')",
        "pm"
    );

    db_test!(
        strftime_iso8601_time,
        "SELECT strftime('%R', '2025-01-23T13:14:30.567')",
        "13:14"
    );

    // TODO: Limbo outputs Text("1737638070"
    // db_test!(
    //     strftime_seconds_since_epoch,
    //     "SELECT strftime('%s', '2025-01-23T13:14:30.567')",
    //     1737638070
    // );

    db_test!(
        strftime_seconds,
        "SELECT strftime('%S', '2025-01-23T13:14:30.567')",
        "30"
    );

    db_test!(
        strftime_iso8601_with_seconds,
        "SELECT strftime('%T', '2025-01-23T13:14:30.567')",
        "13:14:30"
    );

    db_test!(
        strftime_week_year_start_sunday_1,
        "SELECT strftime('%U', '2025-01-23T13:14:30.567')",
        "03"
    );

    db_test!(
        strftime_day_week_start_monday,
        "SELECT strftime('%u', '2025-01-23T13:14:30.567')",
        "4"
    );

    db_test!(
        strftime_iso8601_week_year,
        "SELECT strftime('%V', '2025-01-23T13:14:30.567')",
        "04"
    );

    db_test!(
        strftime_day_week_start_sunday_1,
        "SELECT strftime('%w', '2025-01-23T13:14:30.567')",
        "4"
    );

    db_test!(
        strftime_day_week_start_sunday_2,
        "SELECT strftime('%w', '2025-01-23T13:14:30.567')",
        "4"
    );

    db_test!(
        strftime_week_year_start_sunday_2,
        "SELECT strftime('%W', '2025-01-23T13:14:30.567')",
        "03"
    );

    db_test!(
        strftime_year,
        "SELECT strftime('%Y', '2025-01-23T13:14:30.567')",
        "2025"
    );

    db_test!(
        strftime_percent,
        "SELECT strftime('%%', '2025-01-23T13:14:30.567')",
        "%"
    );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_s_cap_3f,
    //     "SELECT strftime('%S.%3f', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_c_cap,
    //     "SELECT strftime('%C', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_y,
    //     "SELECT strftime('%y', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_b,
    //     "SELECT strftime('%b', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_b_cap,
    //     "SELECT strftime('%B', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_h,
    //     "SELECT strftime('%h', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_a,
    //     "SELECT strftime('%a', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_a_cap,
    //     "SELECT strftime('%A', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_d_cap,
    //     "SELECT strftime('%D', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_x,
    //     "SELECT strftime('%x', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_v,
    //     "SELECT strftime('%v', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_f,
    //     "SELECT strftime('%.f', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_3f,
    //     "SELECT strftime('%.3f', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_6f,
    //     "SELECT strftime('%.6f', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_9f,
    //     "SELECT strftime('%.9f', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_3f_2,
    //     "SELECT strftime('%3f', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_6f_2,
    //     "SELECT strftime('%6f', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_9f_2,
    //     "SELECT strftime('%9f', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_x_cap,
    //     "SELECT strftime('%X', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_r,
    //     "SELECT strftime('%r', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo ouputs Text("")
    // db_test!(
    //     strftime_invalid_z_cap,
    //     "SELECT strftime('%Z', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo ouputs Text("")
    // db_test!(
    //     strftime_invalid_z,
    //     "SELECT strftime('%z', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_colon_z,
    //     "SELECT strftime('%:z', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_colon_colon_z,
    //     "SELECT strftime('%::z', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_colon_colon_colon_z,
    //     "SELECT strftime('%:::z', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_hash_z,
    //     "SELECT strftime('%#z', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_c,
    //     "SELECT strftime('%c', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_plus,
    //     "SELECT strftime('%+', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_t,
    //     "SELECT strftime('%t', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_n,
    //     "SELECT strftime('%n', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_minus_question,
    //     "SELECT strftime('%-?', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_underscore_question,
    //     "SELECT strftime('%_?', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    // TODO: Limbo outputs Text("")
    // db_test!(
    //     strftime_invalid_zero_question,
    //     "SELECT strftime('%0?', '2025-01-23T13:14:30.567')",
    //     [Null]
    // );

    db_test!(
        strftime_julianday_2,
        "SELECT strftime('%Y-%m-%d %H:%M:%fZ', 2459717.08070103)",
        "2022-05-17 13:56:12.569Z"
    );

    db_test!(
        timediff_basic_positive,
        "SELECT timediff('14:30:45', '12:00:00')",
        "+0000-00-00 02:30:45.000"
    );

    db_test!(
        timediff_basic_negative,
        "SELECT timediff('12:00:00', '14:30:45')",
        "-0000-00-00 02:30:45.000"
    );

    db_test!(
        timediff_with_milliseconds_positive,
        "SELECT timediff('12:00:01.300', '12:00:00.500')",
        "+0000-00-00 00:00:00.800"
    );

    db_test!(
        timediff_same_time,
        "SELECT timediff('12:00:00', '12:00:00')",
        "+0000-00-00 00:00:00.000"
    );

    db_test!(
        timediff_across_dates,
        "SELECT timediff('2023-05-11 01:15:00', '2023-05-10 23:30:00')",
        "+0000-00-00 01:45:00.000"
    );

    db_test!(
        timediff_across_dates_negative,
        "SELECT timediff('2023-05-10 23:30:00', '2023-05-11 01:15:00')",
        "-0000-00-00 01:45:00.000"
    );

    db_test!(
        timediff_different_formats,
        "SELECT timediff('2023-05-10T23:30:00', '2023-05-10 14:15:00')",
        "+0000-00-00 09:15:00.000"
    );

    db_test!(
        timediff_with_timezone,
        "SELECT timediff('2023-05-10 23:30:00+02:00', '2023-05-10 18:30:00Z')",
        "+0000-00-00 03:00:00.000"
    );

    db_test!(
        timediff_large_difference,
        "SELECT timediff('2023-05-12 10:00:00', '2023-05-10 08:00:00')",
        "+0000-00-02 02:00:00.000"
    );

    db_test!(
        timediff_with_seconds_precision,
        "SELECT timediff('12:30:45.123', '12:30:44.987')",
        "+0000-00-00 00:00:00.136"
    );

    db_test!(
        timediff_null_first_arg,
        "SELECT timediff(NULL, '12:00:00')",
        [Null]
    );

    db_test!(
        timediff_null_second_arg,
        "SELECT timediff('12:00:00', NULL)",
        [Null]
    );

    db_test!(
        timediff_invalid_first_arg,
        "SELECT timediff('not-a-time', '12:00:00')",
        [Null]
    );

    db_test!(
        timediff_invalid_second_arg,
        "SELECT timediff('12:00:00', 'not-a-time')",
        [Null]
    );

    db_test!(
        timediff_julian_day,
        "SELECT timediff(2460000, 2460000.5)",
        "-0000-00-00 12:00:00.000"
    );

    db_test!(
        timediff_different_time_formats,
        "SELECT timediff('23:59:59', '00:00:00')",
        "+0000-00-00 23:59:59.000"
    );
}
