/// A unit name of PostgreSQL's interval unit table (deltatktbl in
/// src/backend/utils/adt/datetime.c). PostgreSQL reads interval input and
/// date_trunc units from that one table, so both call `DatetimeUnit::parse`
/// and match on the units they support.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DatetimeUnit {
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
    Decade,
    Century,
    Millennium,
    /// `timezone`, `timezone_h` and `timezone_m`: time zone fields, not
    /// units of a duration or of truncation.
    TimeZone,
}

impl DatetimeUnit {
    /// The unit named by `name`, without regard to case. PostgreSQL matches
    /// the lowercased name on its first 10 bytes only, so "microsecondsXY"
    /// is the unit "microsecon". None for a name that is not in the table.
    pub fn parse(name: &str) -> Option<Self> {
        let lowered = name.to_ascii_lowercase();
        let key = &lowered.as_bytes()[..lowered.len().min(10)];
        Some(match key {
            b"microsecon" | b"us" | b"usec" | b"usecond" | b"useconds" | b"usecs" => {
                Self::Microsecond
            }
            b"millisecon" | b"ms" | b"msec" | b"msecond" | b"mseconds" | b"msecs" => {
                Self::Millisecond
            }
            b"second" | b"seconds" | b"s" | b"sec" | b"secs" => Self::Second,
            b"minute" | b"minutes" | b"m" | b"min" | b"mins" => Self::Minute,
            b"hour" | b"hours" | b"h" | b"hr" | b"hrs" => Self::Hour,
            b"day" | b"days" | b"d" => Self::Day,
            b"week" | b"weeks" | b"w" => Self::Week,
            b"month" | b"months" | b"mon" | b"mons" => Self::Month,
            b"quarter" | b"qtr" => Self::Quarter,
            b"year" | b"years" | b"y" | b"yr" | b"yrs" => Self::Year,
            b"decade" | b"decades" | b"dec" | b"decs" => Self::Decade,
            b"century" | b"centuries" | b"c" | b"cent" => Self::Century,
            b"millennium" | b"millennia" | b"mil" | b"mils" => Self::Millennium,
            b"timezone" | b"timezone_h" | b"timezone_m" => Self::TimeZone,
            _ => return None,
        })
    }
}
