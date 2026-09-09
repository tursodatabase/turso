use time::convert::{Day, Nanosecond};
use time::{Time, UtcDateTime};

const NS_IN_DAY: i64 = Nanosecond::per_t::<i128>(Day) as i64;

/// Computes the timestamp in nanoseconds corresponding to the beginning of the
/// year (January 1st at midnight UTC).
pub(super) fn try_year_bucket(timestamp_ns: i64) -> crate::Result<i64> {
    year_bucket_using_time_crate(timestamp_ns).map_err(|e| {
        crate::TantivyError::InvalidArgument(format!(
            "Failed to compute year bucket for timestamp {}: {e}",
            timestamp_ns
        ))
    })
}

/// Computes the timestamp in nanoseconds corresponding to the beginning of the
/// month (1st at midnight UTC).
pub(super) fn try_month_bucket(timestamp_ns: i64) -> crate::Result<i64> {
    month_bucket_using_time_crate(timestamp_ns).map_err(|e| {
        crate::TantivyError::InvalidArgument(format!(
            "Failed to compute month bucket for timestamp {}: {e}",
            timestamp_ns
        ))
    })
}

/// Computes the timestamp in nanoseconds corresponding to the beginning of the
/// week (Monday at midnight UTC).
pub(super) fn week_bucket(timestamp_ns: i64) -> i64 {
    // 1970-01-01 was a Thursday (weekday = 4)
    let days_since_epoch = timestamp_ns.div_euclid(NS_IN_DAY);
    // Find the weekday: 0=Monday, ..., 6=Sunday
    let weekday = (days_since_epoch + 3).rem_euclid(7);
    let monday_days_since_epoch = days_since_epoch - weekday;
    monday_days_since_epoch * NS_IN_DAY
}

fn year_bucket_using_time_crate(timestamp_ns: i64) -> Result<i64, time::Error> {
    let timestamp_ns = UtcDateTime::from_unix_timestamp_nanos(timestamp_ns as i128)?
        .replace_ordinal(1)?
        .replace_time(Time::MIDNIGHT)
        .unix_timestamp_nanos();
    Ok(timestamp_ns as i64)
}

fn month_bucket_using_time_crate(timestamp_ns: i64) -> Result<i64, time::Error> {
    let timestamp_ns = UtcDateTime::from_unix_timestamp_nanos(timestamp_ns as i128)?
        .replace_day(1)?
        .replace_time(Time::MIDNIGHT)
        .unix_timestamp_nanos();
    Ok(timestamp_ns as i64)
}

#[cfg(test)]
mod tests {
    use time::format_description::well_known::Iso8601;
    use time::UtcDateTime;

    use super::*;

    fn ts_ns(iso: &str) -> i64 {
        UtcDateTime::parse(iso, &Iso8601::DEFAULT)
            .unwrap()
            .unix_timestamp_nanos() as i64
    }

    #[test]
    fn test_year_bucket() {
        let ts = ts_ns("1970-01-01T00:00:00Z");
        let res = try_year_bucket(ts).unwrap();
        assert_eq!(res, ts_ns("1970-01-01T00:00:00Z"));

        let ts = ts_ns("1970-06-01T10:00:01.010Z");
        let res = try_year_bucket(ts).unwrap();
        assert_eq!(res, ts_ns("1970-01-01T00:00:00Z"));

        let ts = ts_ns("2008-12-31T23:59:59.999999999Z"); // leap year
        let res = try_year_bucket(ts).unwrap();
        assert_eq!(res, ts_ns("2008-01-01T00:00:00Z"));

        let ts = ts_ns("2008-01-01T00:00:00Z"); // leap year
        let res = try_year_bucket(ts).unwrap();
        assert_eq!(res, ts_ns("2008-01-01T00:00:00Z"));

        let ts = ts_ns("2010-12-31T23:59:59.999999999Z");
        let res = try_year_bucket(ts).unwrap();
        assert_eq!(res, ts_ns("2010-01-01T00:00:00Z"));

        let ts = ts_ns("1972-06-01T00:10:00Z");
        let res = try_year_bucket(ts).unwrap();
        assert_eq!(res, ts_ns("1972-01-01T00:00:00Z"));
    }

    #[test]
    fn test_month_bucket() {
        let ts = ts_ns("1970-01-15T00:00:00Z");
        let res = try_month_bucket(ts).unwrap();
        assert_eq!(res, ts_ns("1970-01-01T00:00:00Z"));

        let ts = ts_ns("1970-02-01T00:00:00Z");
        let res = try_month_bucket(ts).unwrap();
        assert_eq!(res, ts_ns("1970-02-01T00:00:00Z"));

        let ts = ts_ns("2000-01-31T23:59:59.999999999Z");
        let res = try_month_bucket(ts).unwrap();
        assert_eq!(res, ts_ns("2000-01-01T00:00:00Z"));
    }

    #[test]
    fn test_week_bucket() {
        let ts = ts_ns("1970-01-05T00:00:00Z"); // Monday
        let res = week_bucket(ts);
        assert_eq!(res, ts_ns("1970-01-05T00:00:00Z"));

        let ts = ts_ns("1970-01-05T23:59:59Z"); // Monday
        let res = week_bucket(ts);
        assert_eq!(res, ts_ns("1970-01-05T00:00:00Z"));

        let ts = ts_ns("1970-01-07T01:13:00Z"); // Wednesday
        let res = week_bucket(ts);
        assert_eq!(res, ts_ns("1970-01-05T00:00:00Z"));

        let ts = ts_ns("1970-01-11T23:59:59.999999999Z"); // Sunday
        let res = week_bucket(ts);
        assert_eq!(res, ts_ns("1970-01-05T00:00:00Z"));

        let ts = ts_ns("2025-10-16T10:41:59.010Z"); // Thursday
        let res = week_bucket(ts);
        assert_eq!(res, ts_ns("2025-10-13T00:00:00Z"));

        let ts = ts_ns("1970-01-01T00:00:00Z"); // Thursday
        let res = week_bucket(ts);
        assert_eq!(res, ts_ns("1969-12-29T00:00:00Z")); // Negative
    }
}
