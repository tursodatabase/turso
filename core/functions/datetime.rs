use crate::numeric::Numeric;
use crate::types::AsValueRef;
use crate::types::{TextRef, TextSubtype, Value};
use crate::LimboError::InvalidModifier;
use crate::{Result, ValueRef};
// chrono isn't used more due to incompatibility with sqlite
use chrono::{Local, Offset, TimeZone};
use std::borrow::Cow;
use std::fmt::Write;

const JD_TO_MS: i64 = 86_400_000;
const MAX_JD: i64 = 464269060799999; // 9999-12-31 23:59:59.999

#[derive(Debug, Clone, Copy)]
struct DateTime {
    i_jd: i64, // The julian day number times 86400000
    y: i32,
    m: i32,
    d: i32,
    h: i32,
    min: i32,
    s: f64,
    tz: i32, // Timezone offset in minutes
    n_floor: i32,
    valid_jd: bool,
    valid_ymd: bool,
    valid_hms: bool,
    raw_s: bool, // Raw numeric value stored in s
    is_error: bool,
    use_subsec: bool,
    is_utc: bool,
    is_local: bool,
}

impl Default for DateTime {
    fn default() -> Self {
        DateTime {
            i_jd: 0,
            y: 2000,
            m: 1,
            d: 1,
            h: 0,
            min: 0,
            s: 0.0,
            tz: 0,
            n_floor: 0,
            valid_jd: false,
            valid_ymd: false,
            valid_hms: false,
            raw_s: false,
            is_error: false,
            use_subsec: false,
            is_utc: false,
            is_local: false,
        }
    }
}

impl DateTime {
    fn set_error(&mut self) {
        *self = DateTime::default();
        self.is_error = true;
    }

    fn compute_jd(&mut self) {
        if self.valid_jd {
            return;
        }
        let mut y: i32;
        let mut m: i32;
        let d: i32;
        if self.valid_ymd {
            y = self.y;
            m = self.m;
            d = self.d;
        } else {
            y = 2000;
            m = 1;
            d = 1;
        }
        if !(-4713..=9999).contains(&y) || self.raw_s {
            self.set_error();
            return;
        }
        if m <= 2 {
            y -= 1;
            m += 12;
        }
        let a = (y + 4800) / 100;
        let b = 38 - a + (a / 4);
        let x1 = 36525 * (y + 4716) / 100;
        let x2 = 306001 * (m + 1) / 10000;
        self.i_jd = (x1 as i64 + x2 as i64 + d as i64 + b as i64) * 86400000 - 131716800000;
        self.valid_jd = true;
        if self.valid_hms {
            self.i_jd += self.h as i64 * 3_600_000
                + self.min as i64 * 60_000
                + (self.s * 1000.0 + 0.5) as i64;
            if self.tz != 0 {
                self.i_jd -= self.tz as i64 * 60_000;
                self.valid_ymd = false;
                self.valid_hms = false;
                self.tz = 0;
                self.is_utc = true;
                self.is_local = false;
            }
        }
    }

    fn compute_ymd(&mut self) {
        if self.valid_ymd {
            return;
        }
        if !self.valid_jd {
            self.y = 2000;
            self.m = 1;
            self.d = 1;
        } else if self.i_jd < 0 || self.i_jd > MAX_JD {
            self.set_error();
            return;
        } else {
            let z = ((self.i_jd + 43200000) / JD_TO_MS) as i32;
            let alpha = ((z as f64 + 32044.75) / 36524.25) as i32 - 52;
            let a = z + 1 + alpha - ((alpha + 100) / 4) + 25;
            let b = a + 1524;
            let c = ((b as f64 - 122.1) / 365.25) as i32;
            let d_calc = (36525 * (c & 32767)) / 100;
            let e = ((b - d_calc) as f64 / 30.6001) as i32;
            let x1 = (30.6001 * e as f64) as i32;

            self.d = b - d_calc - x1;
            self.m = if e < 14 { e - 1 } else { e - 13 };
            self.y = if self.m > 2 { c - 4716 } else { c - 4715 };
        }
        self.valid_ymd = true;
    }

    fn compute_hms(&mut self) {
        if self.valid_hms {
            return;
        }
        self.compute_jd();
        let day_ms = ((self.i_jd + 43200000) % 86400000) as i32;
        self.s = (day_ms % 60000) as f64 / 1000.0;
        let day_min = day_ms / 60000;
        self.min = day_min % 60;
        self.h = day_min / 60;
        self.raw_s = false;
        self.valid_hms = true;
    }

    fn compute_ymd_hms(&mut self) {
        self.compute_ymd();
        self.compute_hms();
    }

    fn clear_ymd_hms_tz(&mut self) {
        self.valid_ymd = false;
        self.valid_hms = false;
        self.tz = 0;
    }

    fn compute_floor(&mut self) {
        assert!(self.valid_ymd || self.is_error);
        assert!(self.d >= 0 && self.d <= 31);
        assert!(self.m >= 0 && self.m <= 12);
        if self.d <= 28 || ((1 << self.m) & 0x15aa) != 0 {
            self.n_floor = 0;
        } else if self.m != 2 {
            self.n_floor = if self.d == 31 { 1 } else { 0 };
        } else if self.y % 4 != 0 || (self.y % 100 == 0 && self.y % 400 != 0) {
            self.n_floor = self.d - 28;
        } else {
            self.n_floor = self.d - 29;
        }
    }
}

fn get_digits(z: &str, digits: usize, min_val: i32, max_val: i32) -> Option<(i32, &str)> {
    if z.len() < digits {
        return None;
    }
    if !z.is_char_boundary(digits) {
        return None;
    }
    let bytes = z.as_bytes();
    if !bytes.iter().take(digits).all(|b| b.is_ascii_digit()) {
        return None;
    }
    let slice = &z[..digits];
    let val = slice.parse::<i32>().ok()?;
    if val < min_val || val > max_val {
        return None;
    }
    Some((val, &z[digits..]))
}

fn set_to_current(p: &mut DateTime) {
    let now = std::time::SystemTime::now();
    let duration = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    const UNIX_EPOCH_IJD: i64 = 210866760000000;
    p.i_jd = UNIX_EPOCH_IJD + duration.as_millis() as i64;
    p.valid_jd = true;
    p.is_utc = true;
    p.is_local = false;
    p.clear_ymd_hms_tz();
}

fn parse_date_or_time(value: &str, p: &mut DateTime) -> Result<()> {
    if parse_yyyy_mm_dd(value, p) {
        return Ok(());
    }
    if parse_hh_mm_ss(value, p) {
        return Ok(());
    }
    if value.eq_ignore_ascii_case("now") {
        set_to_current(p);
        return Ok(());
    }
    let numeric_value = value.trim_matches(|c: char| c.is_ascii_whitespace());
    if let Ok(val) = numeric_value.parse::<f64>() {
        p.s = val;
        p.raw_s = true;
        if (0.0..5373484.5).contains(&val) {
            p.i_jd = (val * JD_TO_MS as f64 + 0.5) as i64;
            p.valid_jd = true;
        }
        return Ok(());
    }
    if value.eq_ignore_ascii_case("subsec") || value.eq_ignore_ascii_case("subsecond") {
        p.use_subsec = true;
        set_to_current(p);
        return Ok(());
    }
    Err(crate::LimboError::InvalidModifier("Parse Failed".into()))
}

fn parse_yyyy_mm_dd(mut z: &str, p: &mut DateTime) -> bool {
    let y: i32;
    let m: i32;
    let d: i32;
    let neg: bool;

    if z.starts_with('-') {
        z = &z[1..];
        neg = true;
    } else {
        neg = false;
    }

    if let Some((val, rem)) = get_digits(z, 4, 0, 9999) {
        y = val;
        z = rem;
    } else {
        return false;
    }

    if !z.starts_with('-') {
        return false;
    }
    z = &z[1..];

    if let Some((val, rem)) = get_digits(z, 2, 1, 12) {
        m = val;
        z = rem;
    } else {
        return false;
    }

    if !z.starts_with('-') {
        return false;
    }
    z = &z[1..];

    if let Some((val, rem)) = get_digits(z, 2, 1, 31) {
        d = val;
        z = rem;
    } else {
        return false;
    }

    while !z.is_empty() {
        let c = z.as_bytes()[0] as char;
        if c.is_ascii_whitespace() || c == 'T' {
            z = &z[1..];
        } else {
            break;
        }
    }

    if parse_hh_mm_ss(z, p) {
    } else if z.is_empty() {
        p.valid_hms = false;
    } else {
        return false;
    }

    p.valid_jd = false;
    p.valid_ymd = true;
    p.y = if neg { -y } else { y };
    p.m = m;
    p.d = d;

    p.compute_floor();

    if p.tz != 0 {
        p.compute_jd();
    }
    true
}

fn parse_hh_mm_ss(mut z: &str, p: &mut DateTime) -> bool {
    let h: i32;
    let m: i32;
    let s: i32;
    let mut ms: f64 = 0.0;

    if let Some((val, rem)) = get_digits(z, 2, 0, 24) {
        h = val;
        z = rem;
    } else {
        return false;
    }

    if !z.starts_with(':') {
        return false;
    }
    z = &z[1..];

    if let Some((val, rem)) = get_digits(z, 2, 0, 59) {
        m = val;
        z = rem;
    } else {
        return false;
    }

    if z.starts_with(':') {
        z = &z[1..];

        if let Some((val, rem)) = get_digits(z, 2, 0, 59) {
            s = val;
            z = rem;
        } else {
            return false;
        }

        if z.starts_with('.') && z.len() > 1 && z.as_bytes()[1].is_ascii_digit() {
            let mut r_scale = 1.0;
            z = &z[1..]; // Skip '.'

            while !z.is_empty() && z.as_bytes()[0].is_ascii_digit() {
                let digit = (z.as_bytes()[0] - b'0') as f64;
                ms = ms * 10.0 + digit;
                r_scale *= 10.0;
                z = &z[1..];
            }
            ms /= r_scale;

            if ms > 0.999 {
                ms = 0.999;
            }
        }
    } else {
        s = 0;
    }

    p.valid_jd = false;
    p.raw_s = false;
    p.valid_hms = true;
    p.h = h;
    p.min = m;
    p.s = s as f64 + ms;

    if parse_timezone(z, p) {
        return false;
    }
    true
}

fn parse_timezone(mut z: &str, p: &mut DateTime) -> bool {
    while !z.is_empty() {
        let c = z.as_bytes()[0] as char;
        if c.is_ascii_whitespace() {
            z = &z[1..];
        } else {
            break;
        }
    }

    p.tz = 0;

    if z.is_empty() {
        return false;
    }

    let c = z.as_bytes()[0] as char;
    let sgn: i32;

    if c == '-' {
        sgn = -1;
    } else if c == '+' {
        sgn = 1;
    } else if c == 'Z' || c == 'z' {
        z = &z[1..];
        p.is_local = false;
        p.is_utc = true;
        return check_trailing_garbage(z);
    } else {
        return true;
    }

    z = &z[1..];

    let n_hr: i32;
    if let Some((val, rem)) = get_digits(z, 2, 0, 14) {
        n_hr = val;
        z = rem;
    } else {
        return true;
    }

    if !z.starts_with(':') {
        return true;
    }
    z = &z[1..];

    let n_mn: i32;
    if let Some((val, rem)) = get_digits(z, 2, 0, 59) {
        n_mn = val;
        z = rem;
    } else {
        return true;
    }

    p.tz = sgn * (n_mn + n_hr * 60);

    if p.tz == 0 {
        p.is_local = false;
        p.is_utc = true;
    }

    check_trailing_garbage(z)
}

// Helper to mimic the "zulu_time" label logic:
// while( sqlite3Isspace(*zDate) ){ zDate++; }
// return *zDate!=0;
fn check_trailing_garbage(mut z: &str) -> bool {
    while !z.is_empty() {
        let c = z.as_bytes()[0] as char;
        if c.is_ascii_whitespace() {
            z = &z[1..];
        } else {
            break;
        }
    }
    // Return true if garbage remains (Error), false if empty (Success)
    !z.is_empty()
}

fn auto_adjust_date(p: &mut DateTime) {
    if !p.raw_s || p.valid_jd {
        p.raw_s = false;
    } else if p.s >= -210866760000.0 && p.s <= 253402300799.0 {
        let r = p.s * 1000.0 + 210866760000000.0;
        p.i_jd = (r + 0.5) as i64;
        p.valid_jd = true;
        p.raw_s = false;
        p.clear_ymd_hms_tz();
    }
}

fn parse_modifier(p: &mut DateTime, z: &str, idx: usize) -> Result<()> {
    let mut chars = z.chars();
    let first_char = match chars.next() {
        Some(c) => c.to_ascii_lowercase(),
        None => return Err(InvalidModifier(format!("Unknown modifier: {z}"))),
    };

    match first_char {
        'a' if z.eq_ignore_ascii_case("auto") => {
            if idx > 0 {
                return Err(InvalidModifier(format!(
                    "Modifier 'auto' must be first: {z}"
                )));
            }
            auto_adjust_date(p);
            Ok(())
        }
        'c' if z.eq_ignore_ascii_case("ceiling") => {
            p.compute_jd();
            p.clear_ymd_hms_tz();
            p.n_floor = 0;
            Ok(())
        }
        'f' if z.eq_ignore_ascii_case("floor") => {
            p.compute_jd();
            if p.n_floor != 0 {
                p.i_jd -= p.n_floor as i64 * JD_TO_MS;
                p.n_floor = 0;
            }
            p.clear_ymd_hms_tz();
            Ok(())
        }
        'j' if z.eq_ignore_ascii_case("julianday") => {
            if idx > 0 {
                return Err(InvalidModifier(format!(
                    "Modifier 'julianday' must be first: {z}"
                )));
            }
            if p.valid_jd && p.raw_s {
                p.raw_s = false;
                Ok(())
            } else {
                Err(InvalidModifier(format!(
                    "Invalid use of julianday modifier: {z}"
                )))
            }
        }
        'l' if z.eq_ignore_ascii_case("localtime") => {
            if !p.is_local {
                p.compute_jd();
                let timestamp = (p.i_jd - 210866760000000) / 1000;
                let offset_sec = match Local.timestamp_opt(timestamp, 0) {
                    chrono::LocalResult::Single(dt) => dt.offset().fix().local_minus_utc(),
                    _ => 0,
                };
                p.i_jd += (offset_sec as i64) * 1000;
                p.clear_ymd_hms_tz();
                p.is_local = true;
                p.is_utc = false;
            }
            Ok(())
        }
        'u' if z.eq_ignore_ascii_case("unixepoch") => {
            if idx > 0 {
                return Err(InvalidModifier(format!(
                    "Modifier 'unixepoch' must be first: {z}"
                )));
            }
            if p.raw_s {
                let r = p.s * 1000.0 + 210866760000000.0;
                // Range check before the cast, as SQLite's date.c does. `as i64`
                // saturates, so an out-of-range value would park i64::MIN in
                // `i_jd`, which 'localtime'/'utc' then subtract the epoch from.
                // The check in exec_datetime_general runs too late. Rejects NaN.
                if !(r >= 0.0 && r < (MAX_JD + 1) as f64) {
                    return Err(InvalidModifier(format!(
                        "Unixepoch value out of range: {z}"
                    )));
                }
                p.i_jd = (r + 0.5) as i64;
                p.valid_jd = true;
                p.raw_s = false;
                p.clear_ymd_hms_tz();
                Ok(())
            } else {
                Err(InvalidModifier(format!(
                    "Invalid use of unixepoch modifier: {z}"
                )))
            }
        }
        'u' if z.eq_ignore_ascii_case("utc") => {
            if !p.is_utc {
                p.compute_jd();
                let timestamp = (p.i_jd - 210866760000000) / 1000;
                let offset_sec = match Local.timestamp_opt(timestamp, 0) {
                    chrono::LocalResult::Single(dt) => dt.offset().fix().local_minus_utc(),
                    _ => 0,
                };
                p.i_jd -= (offset_sec as i64) * 1000;
                p.clear_ymd_hms_tz();
                p.is_utc = true;
                p.is_local = false;
            }
            Ok(())
        }
        'w' if z
            .get(..8)
            .is_some_and(|s| s.eq_ignore_ascii_case("weekday ")) =>
        {
            if let Ok(val) = z[8..].trim().parse::<f64>() {
                if (0.0..7.0).contains(&val) && (val as i64 as f64) == val {
                    let n = val as i64;
                    p.compute_ymd_hms();
                    p.valid_jd = false;
                    p.compute_jd();
                    let mut z = ((p.i_jd + 129600000) / 86400000) % 7;
                    if z > n {
                        z -= 7;
                    }
                    p.i_jd += (n - z) * 86400000;
                    p.clear_ymd_hms_tz();
                    return Ok(());
                }
            }
            Err(InvalidModifier(format!("Invalid weekday: {z}")))
        }
        's' => {
            if z.eq_ignore_ascii_case("subsec") || z.eq_ignore_ascii_case("subsecond") {
                p.use_subsec = true;
                Ok(())
            } else if z
                .get(..9)
                .is_some_and(|s| s.eq_ignore_ascii_case("start of "))
            {
                if !p.valid_jd && !p.valid_ymd && !p.valid_hms {
                    return Err(InvalidModifier(format!("Invalid start of: {z}")));
                }
                p.compute_ymd();
                p.valid_hms = true;
                p.h = 0;
                p.min = 0;
                p.s = 0.0;
                p.raw_s = false;
                p.valid_jd = false;
                p.tz = 0;
                p.n_floor = 0;

                let suffix = &z[9..];
                if suffix.eq_ignore_ascii_case("month") {
                    p.d = 1;
                    Ok(())
                } else if suffix.eq_ignore_ascii_case("year") {
                    p.m = 1;
                    p.d = 1;
                    Ok(())
                } else if suffix.eq_ignore_ascii_case("day") {
                    Ok(())
                } else {
                    Err(InvalidModifier(format!("Invalid start of: {z}")))
                }
            } else {
                Err(InvalidModifier(format!("Unknown modifier: {z}")))
            }
        }
        '+' | '-' | '0'..='9' => parse_arithmetic_modifier(p, z),
        _ => Err(InvalidModifier(format!("Unknown modifier: {z}"))),
    }
}

fn parse_arithmetic_modifier(p: &mut DateTime, z: &str) -> Result<()> {
    let z = z.trim();
    let is_neg = z.starts_with('-');
    let sign = if is_neg { -1 } else { 1 };

    let clean_z = if z.starts_with('+') || z.starts_with('-') {
        &z[1..]
    } else {
        z
    };

    // Case 1: YYYY-MM-DD Arithmetic
    if clean_z.len() >= 10
        && clean_z.as_bytes().get(4) == Some(&b'-')
        && clean_z.as_bytes().get(7) == Some(&b'-')
        && clean_z.is_char_boundary(4)
        && clean_z.is_char_boundary(5)
        && clean_z.is_char_boundary(7)
        && clean_z.is_char_boundary(8)
        && clean_z.is_char_boundary(10)
    {
        let y_res = get_digits(&clean_z[0..4], 4, 0, 9999);
        let m_res = get_digits(&clean_z[5..7], 2, 0, 11);
        let d_res = get_digits(&clean_z[8..10], 2, 0, 30);

        if let (Some((y, _)), Some((m, _)), Some((d, _))) = (y_res, m_res, d_res) {
            let rem = &clean_z[10..];
            let mut valid_format = true;
            let mut time_str = None;

            if !rem.is_empty() {
                if rem.starts_with(' ') {
                    time_str = Some(rem.trim_start());
                } else {
                    valid_format = false;
                }
            }

            if valid_format {
                p.compute_ymd_hms();
                p.valid_jd = false;

                let y_adj = y as i64;
                let m_adj = m as i64;
                let d_adj = d as i64;

                if is_neg {
                    p.y = p.y.wrapping_sub(y_adj as i32);
                    p.m = p.m.wrapping_sub(m_adj as i32);
                } else {
                    p.y = p.y.wrapping_add(y_adj as i32);
                    p.m = p.m.wrapping_add(m_adj as i32);
                }

                // Normalize months
                let m_current = p.m as i64;
                let x = if m_current > 0 {
                    (m_current - 1) / 12
                } else {
                    (m_current - 12) / 12
                };
                p.y = p.y.wrapping_add(x as i32);
                p.m = (m_current - x * 12) as i32;

                p.compute_floor();
                p.compute_jd();

                // Apply day offset
                let day_diff = if is_neg { -d_adj } else { d_adj };
                p.i_jd = p.i_jd.wrapping_add(day_diff.wrapping_mul(JD_TO_MS));

                // Apply time offset if present
                if let Some(t_val) = time_str {
                    let mut tx = DateTime::default();
                    if parse_hh_mm_ss(t_val, &mut tx) {
                        tx.compute_jd();
                        let ms = (tx.h as i64 * 3600000)
                            + (tx.min as i64 * 60000)
                            + (tx.s * 1000.0) as i64;
                        p.i_jd = p.i_jd.wrapping_add((sign as i64).wrapping_mul(ms));
                    } else {
                        // If time parsing failed, the whole modifier is invalid
                        return Err(InvalidModifier(format!(
                            "Invalid time in arithmetic modifier: {z}"
                        )));
                    }
                }

                p.clear_ymd_hms_tz();
                return Ok(());
            }
        }
    }

    // Case 2: HH:MM:SS Arithmetic
    if z.contains(':') {
        let mut tx = DateTime::default();
        let time_str = if z.starts_with('+') || z.starts_with('-') {
            &z[1..]
        } else {
            z
        };
        if parse_hh_mm_ss(time_str, &mut tx) {
            tx.compute_jd();
            let ms = (tx.h as i64 * 3600000) + (tx.min as i64 * 60000) + (tx.s * 1000.0) as i64;
            p.compute_jd();
            p.i_jd = p.i_jd.wrapping_add((sign as i64).wrapping_mul(ms));
            p.clear_ymd_hms_tz();
            return Ok(());
        }
    }

    // Case 3: NNN Units
    let mut parts = z.split_whitespace();
    if let Some(val_str) = parts.next() {
        if let Ok(val) = val_str.parse::<f64>() {
            if let Some(unit) = parts.next() {
                let limit_check = |v: f64, limit: f64| v.abs() < limit;
                if unit.eq_ignore_ascii_case("day") || unit.eq_ignore_ascii_case("days") {
                    if !limit_check(val, 5373485.0) {
                        return Err(InvalidModifier(format!("Modifier out of range: {z}")));
                    }
                    p.compute_jd();
                    let ms = val * 86400000.0;
                    let rounder = if ms < 0.0 { -0.5 } else { 0.5 };
                    p.i_jd = p.i_jd.wrapping_add((ms + rounder) as i64);
                    p.n_floor = 0;
                    p.clear_ymd_hms_tz();
                    return Ok(());
                } else if unit.eq_ignore_ascii_case("hour") || unit.eq_ignore_ascii_case("hours") {
                    if !limit_check(val, 1.2897e+11) {
                        return Err(InvalidModifier(format!("Modifier out of range: {z}")));
                    }
                    p.compute_jd();
                    let ms = val * 3600000.0;
                    let rounder = if ms < 0.0 { -0.5 } else { 0.5 };
                    p.i_jd = p.i_jd.wrapping_add((ms + rounder) as i64);
                    p.n_floor = 0;
                    p.clear_ymd_hms_tz();
                    return Ok(());
                } else if unit.eq_ignore_ascii_case("minute")
                    || unit.eq_ignore_ascii_case("minutes")
                {
                    if !limit_check(val, 7.7379e+12) {
                        return Err(InvalidModifier(format!("Modifier out of range: {z}")));
                    }
                    p.compute_jd();
                    let ms = val * 60000.0;
                    let rounder = if ms < 0.0 { -0.5 } else { 0.5 };
                    p.i_jd = p.i_jd.wrapping_add((ms + rounder) as i64);
                    p.n_floor = 0;
                    p.clear_ymd_hms_tz();
                    return Ok(());
                } else if unit.eq_ignore_ascii_case("second")
                    || unit.eq_ignore_ascii_case("seconds")
                {
                    if !limit_check(val, 4.6427e+14) {
                        return Err(InvalidModifier(format!("Modifier out of range: {z}")));
                    }
                    p.compute_jd();
                    let ms = val * 1000.0;
                    let rounder = if ms < 0.0 { -0.5 } else { 0.5 };
                    p.i_jd = p.i_jd.wrapping_add((ms + rounder) as i64);
                    p.n_floor = 0;
                    p.clear_ymd_hms_tz();
                    return Ok(());
                } else if unit.eq_ignore_ascii_case("month") || unit.eq_ignore_ascii_case("months")
                {
                    if !limit_check(val, 176546.0) {
                        return Err(InvalidModifier(format!("Modifier out of range: {z}")));
                    }
                    p.compute_ymd_hms();
                    let int_months = val as i64;
                    let frac_months = val - int_months as f64;

                    let total_months = (p.m as i64) + int_months;
                    let x = if total_months > 0 {
                        (total_months - 1) / 12
                    } else {
                        (total_months - 12) / 12
                    };
                    p.y = p.y.wrapping_add(x as i32);
                    p.m = (total_months - x * 12) as i32;

                    p.compute_floor();
                    p.valid_jd = false;
                    p.compute_jd();

                    if frac_months.abs() > f64::EPSILON {
                        let ms = frac_months * 30.0 * JD_TO_MS as f64;
                        let rounder = if ms < 0.0 { -0.5 } else { 0.5 };
                        p.i_jd = p.i_jd.wrapping_add((ms + rounder) as i64);
                    }
                    p.clear_ymd_hms_tz();
                    return Ok(());
                } else if unit.eq_ignore_ascii_case("year") || unit.eq_ignore_ascii_case("years") {
                    if !limit_check(val, 14713.0) {
                        return Err(InvalidModifier(format!("Modifier out of range: {z}")));
                    }
                    p.compute_ymd_hms();
                    let int_years = val as i64;
                    let frac_years = val - int_years as f64;

                    p.y = p.y.wrapping_add(int_years as i32);

                    p.compute_floor();
                    p.valid_jd = false;
                    p.compute_jd();

                    if frac_years.abs() > f64::EPSILON {
                        let ms = frac_years * 365.0 * JD_TO_MS as f64;
                        let rounder = if ms < 0.0 { -0.5 } else { 0.5 };
                        p.i_jd = p.i_jd.wrapping_add((ms + rounder) as i64);
                    }
                    p.clear_ymd_hms_tz();
                    return Ok(());
                }
            }
        }
    }

    Err(InvalidModifier(format!("Invalid arithmetic modifier: {z}")))
}

pub fn exec_datetime_general<I, E, V>(values: I, func_type: &str) -> Value
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let mut values = values.into_iter();
    let mut p = DateTime::default();
    let mut has_modifier = false;

    if values.len() == 0 {
        set_to_current(&mut p);
    } else {
        let first = values.next().unwrap();
        match blob_as_text(first.as_value_ref()) {
            ValueRef::Text(s) => {
                if parse_date_or_time(s.as_str(), &mut p).is_err() {
                    return Value::Null;
                }
            }
            ValueRef::Numeric(Numeric::Integer(i)) => {
                p.s = i as f64;
                p.raw_s = true;
                if p.s >= 0.0 && p.s < 5373484.5 {
                    p.i_jd = (p.s * JD_TO_MS as f64 + 0.5) as i64;
                    p.valid_jd = true;
                }
            }
            ValueRef::Numeric(Numeric::Float(f)) => {
                p.s = f64::from(f);
                p.raw_s = true;
                if p.s >= 0.0 && p.s < 5373484.5 {
                    p.i_jd = (p.s * JD_TO_MS as f64 + 0.5) as i64;
                    p.valid_jd = true;
                }
            }
            _ => return Value::Null,
        }
    }

    for (i, val) in values.enumerate() {
        has_modifier = true;
        if let ValueRef::Text(s) = blob_as_text(val.as_value_ref()) {
            if parse_modifier(&mut p, s.as_str(), i).is_err() {
                return Value::Null;
            }
        } else {
            return Value::Null;
        }
    }

    p.compute_jd();
    if p.is_error || p.i_jd < 0 || p.i_jd > MAX_JD {
        return Value::Null;
    }

    if !has_modifier && p.valid_ymd && p.d > 28 {
        p.valid_ymd = false;
    }

    match func_type {
        "julianday" => Value::from_f64(p.i_jd as f64 / 86400000.0),
        "unixepoch" => {
            let unix = (p.i_jd - 210866760000000) as f64 / 1000.0;
            if p.use_subsec {
                Value::from_f64(unix)
            } else {
                Value::from_i64(unix.floor() as i64)
            }
        }
        _ => {
            p.compute_ymd_hms();
            if p.is_error {
                return Value::Null;
            }

            let mut res = String::new();
            if func_type == "date" {
                if p.y < 0 {
                    write!(res, "-{:04}-{:02}-{:02}", p.y.abs(), p.m, p.d).unwrap();
                } else {
                    write!(res, "{:04}-{:02}-{:02}", p.y, p.m, p.d).unwrap();
                }
            } else if func_type == "time" {
                write!(res, "{:02}:{:02}", p.h, p.min).unwrap();
                if p.use_subsec {
                    write!(res, ":{:06.3}", p.s).unwrap();
                } else {
                    write!(res, ":{:02}", p.s as i32).unwrap();
                }
            } else {
                if p.y < 0 {
                    write!(
                        res,
                        "-{:04}-{:02}-{:02} {:02}:{:02}",
                        p.y.abs(),
                        p.m,
                        p.d,
                        p.h,
                        p.min
                    )
                    .unwrap();
                } else {
                    write!(
                        res,
                        "{:04}-{:02}-{:02} {:02}:{:02}",
                        p.y, p.m, p.d, p.h, p.min
                    )
                    .unwrap();
                }

                if p.use_subsec {
                    write!(res, ":{:06.3}", p.s).unwrap();
                } else {
                    write!(res, ":{:02}", p.s as i32).unwrap();
                }
            }
            Value::from_text(res)
        }
    }
}

pub fn exec_date<I, E, V>(values: I) -> Value
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    exec_datetime_general(values, "date")
}

pub fn exec_time<I, E, V>(values: I) -> Value
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    exec_datetime_general(values, "time")
}

pub fn exec_datetime_full<I, E, V>(values: I) -> Value
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    exec_datetime_general(values, "datetime")
}

pub fn exec_julianday<I, E, V>(values: I) -> Value
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    exec_datetime_general(values, "julianday")
}

pub fn exec_unixepoch<I, E, V>(values: I) -> Value
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    exec_datetime_general(values, "unixepoch")
}

pub fn exec_timediff<I, E, V>(values: I) -> Value
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let mut values = values.into_iter();
    if values.len() < 2 {
        return Value::Null;
    }

    let mut d1 = DateTime::default();
    let mut d2 = DateTime::default();

    // Parse first argument (d1)
    let val1 = values.next().unwrap();
    match blob_as_text(val1.as_value_ref()) {
        ValueRef::Text(s) => {
            if parse_date_or_time(s.as_str(), &mut d1).is_err() {
                return Value::Null;
            }
        }
        ValueRef::Numeric(Numeric::Integer(i)) => {
            d1.s = i as f64;
            d1.raw_s = true;
            if d1.s >= 0.0 && d1.s < 5373484.5 {
                d1.i_jd = (d1.s * JD_TO_MS as f64 + 0.5) as i64;
                d1.valid_jd = true;
            }
        }
        ValueRef::Numeric(Numeric::Float(f)) => {
            d1.s = f64::from(f);
            d1.raw_s = true;
            if d1.s >= 0.0 && d1.s < 5373484.5 {
                d1.i_jd = (d1.s * JD_TO_MS as f64 + 0.5) as i64;
                d1.valid_jd = true;
            }
        }
        _ => return Value::Null,
    }

    // Parse second argument (d2)
    let val2 = values.next().unwrap();
    match blob_as_text(val2.as_value_ref()) {
        ValueRef::Text(s) => {
            if parse_date_or_time(s.as_str(), &mut d2).is_err() {
                return Value::Null;
            }
        }
        ValueRef::Numeric(Numeric::Integer(i)) => {
            d2.s = i as f64;
            d2.raw_s = true;
            if d2.s >= 0.0 && d2.s < 5373484.5 {
                d2.i_jd = (d2.s * JD_TO_MS as f64 + 0.5) as i64;
                d2.valid_jd = true;
            }
        }
        ValueRef::Numeric(Numeric::Float(f)) => {
            d2.s = f64::from(f);
            d2.raw_s = true;
            if d2.s >= 0.0 && d2.s < 5373484.5 {
                d2.i_jd = (d2.s * JD_TO_MS as f64 + 0.5) as i64;
                d2.valid_jd = true;
            }
        }
        _ => return Value::Null,
    }

    d1.compute_jd();
    d2.compute_jd();

    // Validate inputs after computation
    if d1.is_error || d2.is_error {
        return Value::Null;
    }

    d1.compute_ymd_hms();
    d2.compute_ymd_hms();

    // Month arithmetic is not symmetric: adding a month clamps a day-of-month
    // overflow forward, so subtracting a month is not the inverse of adding
    // one. To keep datetime(B, timediff(A, B)) == datetime(A), the Y/M shift
    // must be applied to the second argument (d2) in the same direction that
    // the resulting modifier will be applied, exactly as SQLite does.
    let sign: char;
    let mut y: i32;
    let mut m: i32;
    let diff_ms: i64;
    if d1.i_jd >= d2.i_jd {
        sign = '+';
        y = d1.y - d2.y;
        if y != 0 {
            d2.y = d1.y;
            d2.valid_jd = false;
            d2.compute_jd();
        }
        m = d1.m - d2.m;
        if m < 0 {
            y -= 1;
            m += 12;
        }
        if m != 0 {
            d2.m = d1.m;
            d2.valid_jd = false;
            d2.compute_jd();
        }
        // If shifting d2 forward by Y years and M months overshot d1, back
        // off one month at a time.
        while d1.i_jd < d2.i_jd {
            m -= 1;
            if m < 0 {
                m = 11;
                y -= 1;
            }
            d2.m -= 1;
            if d2.m < 1 {
                d2.m = 12;
                d2.y -= 1;
            }
            d2.valid_jd = false;
            d2.compute_jd();
        }
        diff_ms = d1.i_jd - d2.i_jd;
    } else {
        sign = '-';
        y = d2.y - d1.y;
        if y != 0 {
            d2.y = d1.y;
            d2.valid_jd = false;
            d2.compute_jd();
        }
        m = d2.m - d1.m;
        if m < 0 {
            y -= 1;
            m += 12;
        }
        if m != 0 {
            d2.m = d1.m;
            d2.valid_jd = false;
            d2.compute_jd();
        }
        // If shifting d2 backward by Y years and M months overshot d1, move
        // forward one month at a time.
        while d1.i_jd > d2.i_jd {
            m -= 1;
            if m < 0 {
                m = 11;
                y -= 1;
            }
            d2.m += 1;
            if d2.m > 12 {
                d2.m = 1;
                d2.y += 1;
            }
            d2.valid_jd = false;
            d2.compute_jd();
        }
        diff_ms = d2.i_jd - d1.i_jd;
    }
    let days = diff_ms / 86400000;
    let rem_ms = diff_ms % 86400000;
    let hours = rem_ms / 3600000;
    let rem_ms = rem_ms % 3600000;
    let mins = rem_ms / 60000;
    let rem_ms = rem_ms % 60000;
    let secs = rem_ms as f64 / 1000.0;

    let mut res = String::new();
    write!(
        res,
        "{sign}{y:04}-{m:02}-{days:02} {hours:02}:{mins:02}:{secs:06.3}"
    )
    .unwrap();

    Value::from_text(res)
}

pub fn exec_strftime<I, E, V>(values: I) -> Value
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let mut values = values.into_iter();
    if values.len() < 1 {
        return Value::Null;
    }

    let fmt_val = values.next().unwrap();
    let fmt_str = match fmt_val.as_value_ref() {
        ValueRef::Text(s) => Cow::Borrowed(s.as_str()),
        ValueRef::Null => return Value::Null,
        val => Cow::Owned(val.to_string()),
    };

    let mut p = DateTime::default();
    if values.len() == 0 {
        set_to_current(&mut p);
    } else {
        let init_val = values.next().unwrap();
        match blob_as_text(init_val.as_value_ref()) {
            ValueRef::Text(s) => {
                let s_str = s.as_str();
                if s_str.eq_ignore_ascii_case("now") {
                    set_to_current(&mut p);
                } else if let Ok(val) = s_str.parse::<f64>() {
                    p.s = val;
                    p.raw_s = true;
                    if p.s >= 0.0 && p.s < 5373484.5 {
                        p.i_jd = (p.s * JD_TO_MS as f64 + 0.5) as i64;
                        p.valid_jd = true;
                    }
                } else {
                    let mut temp_p = DateTime::default();
                    if parse_date_or_time(s_str, &mut temp_p).is_ok() {
                        p = temp_p;
                    } else {
                        return Value::Null;
                    }
                }
            }
            ValueRef::Numeric(Numeric::Integer(i)) => {
                p.s = i as f64;
                p.raw_s = true;
                if p.s >= 0.0 && p.s < 5373484.5 {
                    p.i_jd = (p.s * JD_TO_MS as f64 + 0.5) as i64;
                    p.valid_jd = true;
                }
            }
            ValueRef::Numeric(Numeric::Float(f)) => {
                p.s = f64::from(f);
                p.raw_s = true;
                if p.s >= 0.0 && p.s < 5373484.5 {
                    p.i_jd = (p.s * JD_TO_MS as f64 + 0.5) as i64;
                    p.valid_jd = true;
                }
            }
            _ => return Value::Null,
        }

        for (i, val) in values.enumerate() {
            if let ValueRef::Text(s) = blob_as_text(val.as_value_ref()) {
                if parse_modifier(&mut p, s.as_str(), i).is_err() {
                    return Value::Null;
                }
            } else {
                return Value::Null;
            }
        }
    }

    p.compute_jd();
    if p.is_error {
        return Value::Null;
    }

    p.compute_ymd_hms();

    let mut res = String::new();
    let mut chars = fmt_str.chars().peekable();

    let days_after_jan1 = |curr: &DateTime| -> i64 {
        let jan1 = DateTime {
            y: curr.y,
            m: 1,
            d: 1,
            valid_ymd: true,
            ..Default::default()
        };
        let mut j1 = jan1;
        j1.compute_jd();
        let curr_norm = DateTime {
            y: curr.y,
            m: curr.m,
            d: curr.d,
            valid_ymd: true,
            ..Default::default()
        };
        let mut c1 = curr_norm;
        c1.compute_jd();
        (c1.i_jd - j1.i_jd) / JD_TO_MS
    };

    let days_after_mon = |curr: &DateTime| -> i64 { ((curr.i_jd + 43200000) / JD_TO_MS) % 7 };
    let days_after_sun = |curr: &DateTime| -> i64 { ((curr.i_jd + 129600000) / JD_TO_MS) % 7 };

    while let Some(c) = chars.next() {
        if c != '%' {
            res.push(c);
            continue;
        }

        match chars.next() {
            Some('d') => write!(res, "{:02}", p.d).unwrap(),
            Some('e') => write!(res, "{:2}", p.d).unwrap(),
            Some('F') => write!(res, "{:04}-{:02}-{:02}", p.y, p.m, p.d).unwrap(),
            Some('f') => {
                let mut s = p.s;
                if s > 59.999 {
                    s = 59.999;
                }
                write!(res, "{s:06.3}").unwrap()
            }
            Some('g') => {
                let mut y_iso = p;
                y_iso.i_jd += (3 - days_after_mon(&p)) * 86400000;
                y_iso.valid_ymd = false;
                y_iso.compute_ymd();
                write!(res, "{:02}", y_iso.y % 100).unwrap();
            }
            Some('G') => {
                let mut y_iso = p;
                y_iso.i_jd += (3 - days_after_mon(&p)) * 86400000;
                y_iso.valid_ymd = false;
                y_iso.compute_ymd();
                write!(res, "{:04}", y_iso.y).unwrap();
            }
            Some('H') => write!(res, "{:02}", p.h).unwrap(),
            Some('I') => {
                let h = if p.h % 12 == 0 { 12 } else { p.h % 12 };
                write!(res, "{h:02}").unwrap();
            }
            Some('j') => {
                write!(res, "{:03}", days_after_jan1(&p) + 1).unwrap();
            }
            Some('J') => {
                let val = p.i_jd as f64 / 86400000.0;
                if val.abs() >= 1_000_000.0 && val.abs() < 10_000_000.0 {
                    let s = format!("{val:.9}");
                    let trimmed = s.trim_end_matches('0').trim_end_matches('.');
                    write!(res, "{trimmed}").unwrap();
                } else {
                    write!(res, "{val}").unwrap();
                }
            }
            Some('k') => write!(res, "{:2}", p.h).unwrap(),
            Some('l') => {
                let h = if p.h % 12 == 0 { 12 } else { p.h % 12 };
                write!(res, "{h:2}").unwrap();
            }
            Some('m') => write!(res, "{:02}", p.m).unwrap(),
            Some('M') => write!(res, "{:02}", p.min).unwrap(),
            Some('p') => write!(res, "{}", if p.h >= 12 { "PM" } else { "AM" }).unwrap(),
            Some('P') => write!(res, "{}", if p.h >= 12 { "pm" } else { "am" }).unwrap(),
            Some('R') => write!(res, "{:02}:{:02}", p.h, p.min).unwrap(),
            Some('s') => {
                let s = (p.i_jd - 210866760000000) as f64 / 1000.0;
                if p.use_subsec {
                    write!(res, "{s:.3}").unwrap();
                } else {
                    write!(res, "{}", s.floor()).unwrap();
                }
            }
            Some('S') => write!(res, "{:02}", p.s as i32).unwrap(),
            Some('T') => write!(res, "{:02}:{:02}:{:02}", p.h, p.min, p.s as i32).unwrap(),
            Some('u') => {
                let mut w = days_after_sun(&p);
                if w == 0 {
                    w = 7;
                }
                write!(res, "{w}").unwrap();
            }
            Some('U') => {
                let w = (days_after_jan1(&p) - days_after_sun(&p) + 7) / 7;
                write!(res, "{w:02}").unwrap();
            }
            Some('V') => {
                let mut temp = p;
                temp.i_jd += (3 - days_after_mon(&p)) * 86400000;
                temp.valid_ymd = false;
                temp.compute_ymd();
                let w = days_after_jan1(&temp) / 7 + 1;
                write!(res, "{w:02}").unwrap();
            }
            Some('w') => {
                write!(res, "{}", days_after_sun(&p)).unwrap();
            }
            Some('W') => {
                let w = (days_after_jan1(&p) - days_after_mon(&p) + 7) / 7;
                write!(res, "{w:02}").unwrap();
            }
            Some('Y') => write!(res, "{:04}", p.y).unwrap(),
            Some('%') => res.push('%'),
            _ => return Value::Null,
        }
    }

    Value::from_text(res)
}

/// SQLite reads date/time arguments with sqlite3_value_text(), which hands back a
/// BLOB's bytes unchanged, so a blob holding '2024-01-01' parses like that text.
/// Bytes that are not UTF-8 stay a blob and the caller rejects them.
fn blob_as_text(value: ValueRef<'_>) -> ValueRef<'_> {
    let ValueRef::Blob(bytes) = value else {
        return value;
    };
    match std::str::from_utf8(bytes) {
        Ok(text) => ValueRef::Text(TextRef::new(text, TextSubtype::Text)),
        Err(_) => value,
    }
}

#[cfg(test)]
#[path = "../tests/unit/functions/datetime/tests.rs"]
mod tests;
