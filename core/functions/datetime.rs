use crate::types::AsValueRef;
use crate::types::Value;
use crate::LimboError::InvalidModifier;
use crate::{Result, ValueRef};
// chrono isn't used more due to incompatibility with sqlite
use chrono::{Local, Offset, TimeZone};
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
    if let Ok(val) = value.parse::<f64>() {
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
    let z_lower = z.to_lowercase();

    match z_lower.chars().next() {
        Some('a') if z_lower == "auto" => {
            if idx > 0 {
                return Err(InvalidModifier(format!(
                    "Modifier 'auto' must be first: {z}"
                )));
            }
            auto_adjust_date(p);
            Ok(())
        }
        Some('c') if z_lower == "ceiling" => {
            p.compute_jd();
            p.clear_ymd_hms_tz();
            p.n_floor = 0;
            Ok(())
        }
        Some('f') if z_lower == "floor" => {
            p.compute_jd();
            if p.n_floor != 0 {
                p.i_jd -= p.n_floor as i64 * JD_TO_MS;
                p.n_floor = 0;
            }
            p.clear_ymd_hms_tz();
            Ok(())
        }
        Some('j') if z_lower == "julianday" => {
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
        Some('l') if z_lower == "localtime" => {
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
        Some('u') if z_lower == "unixepoch" => {
            if idx > 0 {
                return Err(InvalidModifier(format!(
                    "Modifier 'unixepoch' must be first: {z}"
                )));
            }
            if p.raw_s {
                let r = p.s * 1000.0 + 210866760000000.0;
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
        Some('u') if z_lower == "utc" => {
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
        Some('w') if z_lower.starts_with("weekday ") => {
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
        Some('s') if z_lower.starts_with("start of ") => {
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
            let suffix = &z_lower[9..];
            if suffix == "month" {
                p.d = 1;
                Ok(())
            } else if suffix == "year" {
                p.m = 1;
                p.d = 1;
                Ok(())
            } else if suffix == "day" {
                Ok(())
            } else {
                Err(InvalidModifier(format!("Invalid start of: {z}")))
            }
        }
        Some('s') if z_lower == "subsec" || z_lower == "subsecond" => {
            p.use_subsec = true;
            Ok(())
        }
        Some('+') | Some('-') | Some('0'..='9') => parse_arithmetic_modifier(p, z),
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
    let parts: Vec<&str> = z.split_whitespace().collect();
    if parts.len() >= 2 {
        if let Ok(val) = parts[0].parse::<f64>() {
            let unit = parts[1].to_lowercase();
            let limit_check = |v: f64, limit: f64| v.abs() < limit;

            match unit.as_str() {
                "day" | "days" | "hour" | "hours" | "minute" | "minutes" | "second" | "seconds" => {
                    let limit = match unit.as_str() {
                        "day" | "days" => 5373485.0,
                        "hour" | "hours" => 1.2897e+11,
                        "minute" | "minutes" => 7.7379e+12,
                        "second" | "seconds" => 4.6427e+14,
                        _ => 0.0,
                    };
                    if !limit_check(val, limit) {
                        return Err(InvalidModifier(format!("Modifier out of range: {z}")));
                    }

                    p.compute_jd();
                    let ms = match unit.as_str() {
                        "day" | "days" => val * 86400000.0,
                        "hour" | "hours" => val * 3600000.0,
                        "minute" | "minutes" => val * 60000.0,
                        "second" | "seconds" => val * 1000.0,
                        _ => 0.0,
                    };
                    let rounder = if ms < 0.0 { -0.5 } else { 0.5 };
                    p.i_jd = p.i_jd.wrapping_add((ms + rounder) as i64);
                    p.n_floor = 0;
                    p.clear_ymd_hms_tz();
                    return Ok(());
                }
                "month" | "months" => {
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
                }
                "year" | "years" => {
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
                _ => {}
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
        match first.as_value_ref() {
            ValueRef::Text(s) => {
                if parse_date_or_time(s.as_str(), &mut p).is_err() {
                    return Value::Null;
                }
            }
            ValueRef::Integer(i) => {
                p.s = i as f64;
                p.raw_s = true;
                if p.s >= 0.0 && p.s < 5373484.5 {
                    p.i_jd = (p.s * JD_TO_MS as f64 + 0.5) as i64;
                    p.valid_jd = true;
                }
            }
            ValueRef::Float(f) => {
                p.s = f;
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
        if let ValueRef::Text(s) = val.as_value_ref() {
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
        "julianday" => Value::Float(p.i_jd as f64 / 86400000.0),
        "unixepoch" => {
            let unix = (p.i_jd - 210866760000000) / 1000;
            if p.use_subsec {
                let ms = (p.i_jd - 210866760000000) as f64 / 1000.0;
                Value::Float(ms)
            } else {
                Value::Integer(unix)
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
    match val1.as_value_ref() {
        ValueRef::Text(s) => {
            if parse_date_or_time(s.as_str(), &mut d1).is_err() {
                return Value::Null;
            }
        }
        ValueRef::Integer(i) => {
            d1.s = i as f64;
            d1.raw_s = true;
            if d1.s >= 0.0 && d1.s < 5373484.5 {
                d1.i_jd = (d1.s * JD_TO_MS as f64 + 0.5) as i64;
                d1.valid_jd = true;
            }
        }
        ValueRef::Float(f) => {
            d1.s = f;
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
    match val2.as_value_ref() {
        ValueRef::Text(s) => {
            if parse_date_or_time(s.as_str(), &mut d2).is_err() {
                return Value::Null;
            }
        }
        ValueRef::Integer(i) => {
            d2.s = i as f64;
            d2.raw_s = true;
            if d2.s >= 0.0 && d2.s < 5373484.5 {
                d2.i_jd = (d2.s * JD_TO_MS as f64 + 0.5) as i64;
                d2.valid_jd = true;
            }
        }
        ValueRef::Float(f) => {
            d2.s = f;
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

    let sign: char;
    if d1.i_jd >= d2.i_jd {
        sign = '+';
    } else {
        sign = '-';
        std::mem::swap(&mut d1, &mut d2);
    }

    let mut y = d1.y - d2.y;
    let mut m = d1.m - d2.m;

    if m < 0 {
        y -= 1;
        m += 12;
    }

    let mut temp = d2;
    temp.y += y;
    temp.m += m;

    // Normalize months
    while temp.m > 12 {
        temp.m -= 12;
        temp.y += 1;
    }
    while temp.m < 1 {
        temp.m += 12;
        temp.y -= 1;
    }

    temp.valid_jd = false;
    temp.compute_jd();

    // Adjust if the Y/M shift overshot d1
    while temp.i_jd > d1.i_jd {
        m -= 1;
        if m < 0 {
            m = 11;
            y -= 1;
        }
        temp = d2;
        temp.y += y;
        temp.m += m;
        while temp.m > 12 {
            temp.m -= 12;
            temp.y += 1;
        }
        while temp.m < 1 {
            temp.m += 12;
            temp.y -= 1;
        }
        temp.valid_jd = false;
        temp.compute_jd();
    }

    let diff_ms = d1.i_jd - temp.i_jd;
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
        ValueRef::Text(s) => s.as_str(),
        _ => return Value::Null,
    };

    let mut p = DateTime::default();
    if values.len() == 0 {
        set_to_current(&mut p);
    } else {
        let init_val = values.next().unwrap();
        match init_val.as_value_ref() {
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
            ValueRef::Integer(i) => {
                p.s = i as f64;
                p.raw_s = true;
                if p.s >= 0.0 && p.s < 5373484.5 {
                    p.i_jd = (p.s * JD_TO_MS as f64 + 0.5) as i64;
                    p.valid_jd = true;
                }
            }
            ValueRef::Float(f) => {
                p.s = f;
                p.raw_s = true;
                if p.s >= 0.0 && p.s < 5373484.5 {
                    p.i_jd = (p.s * JD_TO_MS as f64 + 0.5) as i64;
                    p.valid_jd = true;
                }
            }
            _ => return Value::Null,
        }

        for (i, val) in values.enumerate() {
            if let ValueRef::Text(s) = val.as_value_ref() {
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
                if p.use_subsec {
                    write!(res, "{:.3}", (p.i_jd - 210866760000000) as f64 / 1000.0).unwrap();
                } else {
                    write!(res, "{}", (p.i_jd - 210866760000000) / 1000).unwrap();
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

// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     #[test]
//     fn test_valid_get_date_from_time_value() {
//         let now = chrono::Local::now().to_utc().format("%Y-%m-%d").to_string();
//
//         let prev_date_str = "2024-07-20";
//         let test_date_str = "2024-07-21";
//         let next_date_str = "2024-07-22";
//
//         let test_cases: Vec<(Value, &str)> = vec![
//             // Format 1: YYYY-MM-DD (no timezone applicable)
//             (Value::build_text("2024-07-21"), test_date_str),
//             // Format 2: YYYY-MM-DD HH:MM
//             (Value::build_text("2024-07-21 22:30"), test_date_str),
//             (Value::build_text("2024-07-21 22:30+02:00"), test_date_str),
//             (Value::build_text("2024-07-21 22:30-05:00"), next_date_str),
//             (Value::build_text("2024-07-21 01:30+05:00"), prev_date_str),
//             (Value::build_text("2024-07-21 22:30Z"), test_date_str),
//             // Format 3: YYYY-MM-DD HH:MM:SS
//             (Value::build_text("2024-07-21 22:30:45"), test_date_str),
//             (
//                 Value::build_text("2024-07-21 22:30:45+02:00"),
//                 test_date_str,
//             ),
//             (
//                 Value::build_text("2024-07-21 22:30:45-05:00"),
//                 next_date_str,
//             ),
//             (
//                 Value::build_text("2024-07-21 01:30:45+05:00"),
//                 prev_date_str,
//             ),
//             (Value::build_text("2024-07-21 22:30:45Z"), test_date_str),
//             // Format 4: YYYY-MM-DD HH:MM:SS.SSS
//             (Value::build_text("2024-07-21 22:30:45.123"), test_date_str),
//             (
//                 Value::build_text("2024-07-21 22:30:45.123+02:00"),
//                 test_date_str,
//             ),
//             (
//                 Value::build_text("2024-07-21 22:30:45.123-05:00"),
//                 next_date_str,
//             ),
//             (
//                 Value::build_text("2024-07-21 01:30:45.123+05:00"),
//                 prev_date_str,
//             ),
//             (Value::build_text("2024-07-21 22:30:45.123Z"), test_date_str),
//             // Format 5: YYYY-MM-DDTHH:MM
//             (Value::build_text("2024-07-21T22:30"), test_date_str),
//             (Value::build_text("2024-07-21T22:30+02:00"), test_date_str),
//             (Value::build_text("2024-07-21T22:30-05:00"), next_date_str),
//             (Value::build_text("2024-07-21T01:30+05:00"), prev_date_str),
//             (Value::build_text("2024-07-21T22:30Z"), test_date_str),
//             // Format 6: YYYY-MM-DDTHH:MM:SS
//             (Value::build_text("2024-07-21T22:30:45"), test_date_str),
//             (
//                 Value::build_text("2024-07-21T22:30:45+02:00"),
//                 test_date_str,
//             ),
//             (
//                 Value::build_text("2024-07-21T22:30:45-05:00"),
//                 next_date_str,
//             ),
//             (
//                 Value::build_text("2024-07-21T01:30:45+05:00"),
//                 prev_date_str,
//             ),
//             (Value::build_text("2024-07-21T22:30:45Z"), test_date_str),
//             // Format 7: YYYY-MM-DDTHH:MM:SS.SSS
//             (Value::build_text("2024-07-21T22:30:45.123"), test_date_str),
//             (
//                 Value::build_text("2024-07-21T22:30:45.123+02:00"),
//                 test_date_str,
//             ),
//             (
//                 Value::build_text("2024-07-21T22:30:45.123-05:00"),
//                 next_date_str,
//             ),
//             (
//                 Value::build_text("2024-07-21T01:30:45.123+05:00"),
//                 prev_date_str,
//             ),
//             (Value::build_text("2024-07-21T22:30:45.123Z"), test_date_str),
//             // Format 8: HH:MM
//             (Value::build_text("22:30"), "2000-01-01"),
//             (Value::build_text("22:30+02:00"), "2000-01-01"),
//             (Value::build_text("22:30-05:00"), "2000-01-02"),
//             (Value::build_text("01:30+05:00"), "1999-12-31"),
//             (Value::build_text("22:30Z"), "2000-01-01"),
//             // Format 9: HH:MM:SS
//             (Value::build_text("22:30:45"), "2000-01-01"),
//             (Value::build_text("22:30:45+02:00"), "2000-01-01"),
//             (Value::build_text("22:30:45-05:00"), "2000-01-02"),
//             (Value::build_text("01:30:45+05:00"), "1999-12-31"),
//             (Value::build_text("22:30:45Z"), "2000-01-01"),
//             // Format 10: HH:MM:SS.SSS
//             (Value::build_text("22:30:45.123"), "2000-01-01"),
//             (Value::build_text("22:30:45.123+02:00"), "2000-01-01"),
//             (Value::build_text("22:30:45.123-05:00"), "2000-01-02"),
//             (Value::build_text("01:30:45.123+05:00"), "1999-12-31"),
//             (Value::build_text("22:30:45.123Z"), "2000-01-01"),
//             // Test Format 11: 'now'
//             (Value::build_text("now"), &now),
//             // Format 12: DDDDDDDDDD (Julian date as float or integer)
//             (Value::Float(2460512.5), test_date_str),
//             (Value::Integer(2460513), test_date_str),
//         ];
//
//         for (input, expected) in test_cases {
//             let result = exec_date(&[input.clone()]);
//             assert_eq!(
//                 result,
//                 Value::build_text(expected.to_string()),
//                 "Failed for input: {input:?}"
//             );
//         }
//     }
//
//     #[test]
//     fn test_invalid_get_date_from_time_value() {
//         let invalid_cases = vec![
//             Value::build_text("2024-07-21 25:00"),    // Invalid hour
//             Value::build_text("2024-07-21 25:00:00"), // Invalid hour
//             Value::build_text("2024-07-21 23:60:00"), // Invalid minute
//             Value::build_text("2024-07-21 22:58:60"), // Invalid second
//             // Note: Invalid days now overflow like SQLite (2024-07-32 -> 2024-08-01)
//             Value::build_text("2024-13-01"),   // Invalid month
//             Value::build_text("invalid_date"), // Completely invalid string
//             Value::build_text(""),             // Empty string
//             Value::Integer(i64::MAX),          // Large Julian day
//             Value::Integer(-1),                // Negative Julian day
//             Value::Float(f64::MAX),            // Large float
//             Value::Float(-1.0),                // Negative Julian day as float
//             Value::Float(f64::NAN),            // NaN
//             Value::Float(f64::INFINITY),       // Infinity
//             Value::Null,                       // Null value
//             Value::Blob(vec![1, 2, 3]),        // Blob (unsupported type)
//             // Invalid timezone tests
//             Value::build_text("2024-07-21T12:00:00+24:00"), // Invalid timezone offset (too large)
//             Value::build_text("2024-07-21T12:00:00-24:00"), // Invalid timezone offset (too small)
//             Value::build_text("2024-07-21T12:00:00+00:60"), // Invalid timezone minutes
//             Value::build_text("2024-07-21T12:00:00+00:00:00"), // Invalid timezone format (extra seconds)
//             Value::build_text("2024-07-21T12:00:00+"),         // Incomplete timezone
//             Value::build_text("2024-07-21T12:00:00+Z"),        // Invalid timezone format
//             Value::build_text("2024-07-21T12:00:00+00:00Z"),   // Mixing offset and Z
//             Value::build_text("2024-07-21T12:00:00UTC"),       // Named timezone (not supported)
//         ];
//
//         for case in invalid_cases.iter() {
//             let result = exec_date([case]);
//             assert_eq!(result, Value::Null);
//         }
//     }
//
//     #[test]
//     fn test_valid_get_time_from_datetime_value() {
//         let test_time_str = "22:30:45";
//         let prev_time_str = "20:30:45";
//         let next_time_str = "03:30:45";
//
//         let test_cases = vec![
//             // Format 1: YYYY-MM-DD (no timezone applicable)
//             (Value::build_text("2024-07-21"), "00:00:00"),
//             // Format 2: YYYY-MM-DD HH:MM
//             (Value::build_text("2024-07-21 22:30"), "22:30:00"),
//             (Value::build_text("2024-07-21 22:30+02:00"), "20:30:00"),
//             (Value::build_text("2024-07-21 22:30-05:00"), "03:30:00"),
//             (Value::build_text("2024-07-21 22:30Z"), "22:30:00"),
//             // Format 3: YYYY-MM-DD HH:MM:SS
//             (Value::build_text("2024-07-21 22:30:45"), test_time_str),
//             (
//                 Value::build_text("2024-07-21 22:30:45+02:00"),
//                 prev_time_str,
//             ),
//             (
//                 Value::build_text("2024-07-21 22:30:45-05:00"),
//                 next_time_str,
//             ),
//             (Value::build_text("2024-07-21 22:30:45Z"), test_time_str),
//             // Format 4: YYYY-MM-DD HH:MM:SS.SSS
//             (Value::build_text("2024-07-21 22:30:45.123"), test_time_str),
//             (
//                 Value::build_text("2024-07-21 22:30:45.123+02:00"),
//                 prev_time_str,
//             ),
//             (
//                 Value::build_text("2024-07-21 22:30:45.123-05:00"),
//                 next_time_str,
//             ),
//             (Value::build_text("2024-07-21 22:30:45.123Z"), test_time_str),
//             // Format 5: YYYY-MM-DDTHH:MM
//             (Value::build_text("2024-07-21T22:30"), "22:30:00"),
//             (Value::build_text("2024-07-21T22:30+02:00"), "20:30:00"),
//             (Value::build_text("2024-07-21T22:30-05:00"), "03:30:00"),
//             (Value::build_text("2024-07-21T22:30Z"), "22:30:00"),
//             // Format 6: YYYY-MM-DDTHH:MM:SS
//             (Value::build_text("2024-07-21T22:30:45"), test_time_str),
//             (
//                 Value::build_text("2024-07-21T22:30:45+02:00"),
//                 prev_time_str,
//             ),
//             (
//                 Value::build_text("2024-07-21T22:30:45-05:00"),
//                 next_time_str,
//             ),
//             (Value::build_text("2024-07-21T22:30:45Z"), test_time_str),
//             // Format 7: YYYY-MM-DDTHH:MM:SS.SSS
//             (Value::build_text("2024-07-21T22:30:45.123"), test_time_str),
//             (
//                 Value::build_text("2024-07-21T22:30:45.123+02:00"),
//                 prev_time_str,
//             ),
//             (
//                 Value::build_text("2024-07-21T22:30:45.123-05:00"),
//                 next_time_str,
//             ),
//             (Value::build_text("2024-07-21T22:30:45.123Z"), test_time_str),
//             // Format 8: HH:MM
//             (Value::build_text("22:30"), "22:30:00"),
//             (Value::build_text("22:30+02:00"), "20:30:00"),
//             (Value::build_text("22:30-05:00"), "03:30:00"),
//             (Value::build_text("22:30Z"), "22:30:00"),
//             // Format 9: HH:MM:SS
//             (Value::build_text("22:30:45"), test_time_str),
//             (Value::build_text("22:30:45+02:00"), prev_time_str),
//             (Value::build_text("22:30:45-05:00"), next_time_str),
//             (Value::build_text("22:30:45Z"), test_time_str),
//             // Format 10: HH:MM:SS.SSS
//             (Value::build_text("22:30:45.123"), test_time_str),
//             (Value::build_text("22:30:45.123+02:00"), prev_time_str),
//             (Value::build_text("22:30:45.123-05:00"), next_time_str),
//             (Value::build_text("22:30:45.123Z"), test_time_str),
//             // Format 12: DDDDDDDDDD (Julian date as float or integer)
//             (Value::Float(2460082.1), "14:24:00"),
//             (Value::Integer(2460082), "12:00:00"),
//         ];
//
//         for (input, expected) in test_cases {
//             let result = exec_time(&[input]);
//             if let Value::Text(result_str) = result {
//                 assert_eq!(result_str.as_str(), expected);
//             } else {
//                 panic!("Expected Value::Text, but got: {result:?}");
//             }
//         }
//     }
//
//     #[test]
//     fn test_invalid_get_time_from_datetime_value() {
//         let invalid_cases = vec![
//             Value::build_text("2024-07-21 25:00"),    // Invalid hour
//             Value::build_text("2024-07-21 25:00:00"), // Invalid hour
//             Value::build_text("2024-07-21 23:60:00"), // Invalid minute
//             Value::build_text("2024-07-21 22:58:60"), // Invalid second
//             // Note: Invalid days now overflow like SQLite (2024-07-32 -> 2024-08-01)
//             Value::build_text("2024-13-01"),   // Invalid month
//             Value::build_text("invalid_date"), // Completely invalid string
//             Value::build_text(""),             // Empty string
//             Value::Integer(i64::MAX),          // Large Julian day
//             Value::Integer(-1),                // Negative Julian day
//             Value::Float(f64::MAX),            // Large float
//             Value::Float(-1.0),                // Negative Julian day as float
//             Value::Float(f64::NAN),            // NaN
//             Value::Float(f64::INFINITY),       // Infinity
//             Value::Null,                       // Null value
//             Value::Blob(vec![1, 2, 3]),        // Blob (unsupported type)
//             // Invalid timezone tests
//             Value::build_text("2024-07-21T12:00:00+24:00"), // Invalid timezone offset (too large)
//             Value::build_text("2024-07-21T12:00:00-24:00"), // Invalid timezone offset (too small)
//             Value::build_text("2024-07-21T12:00:00+00:60"), // Invalid timezone minutes
//             Value::build_text("2024-07-21T12:00:00+00:00:00"), // Invalid timezone format (extra seconds)
//             Value::build_text("2024-07-21T12:00:00+"),         // Incomplete timezone
//             Value::build_text("2024-07-21T12:00:00+Z"),        // Invalid timezone format
//             Value::build_text("2024-07-21T12:00:00+00:00Z"),   // Mixing offset and Z
//             Value::build_text("2024-07-21T12:00:00UTC"),       // Named timezone (not supported)
//         ];
//
//         for case in invalid_cases {
//             let result = exec_time(&[case.clone()]);
//             assert_eq!(result, Value::Null);
//         }
//     }
//
//     #[test]
//     fn test_parse_days() {
//         assert_eq!(parse_modifier("5 days").unwrap(), Modifier::Days(5.0));
//         assert_eq!(parse_modifier("-3 days").unwrap(), Modifier::Days(-3.0));
//         assert_eq!(parse_modifier("+2 days").unwrap(), Modifier::Days(2.0));
//         assert_eq!(parse_modifier("4  days").unwrap(), Modifier::Days(4.0));
//         assert_eq!(parse_modifier("6   DAYS").unwrap(), Modifier::Days(6.0));
//         assert_eq!(parse_modifier("+5  DAYS").unwrap(), Modifier::Days(5.0));
//         // Fractional days
//         assert_eq!(parse_modifier("1.5 days").unwrap(), Modifier::Days(1.5));
//         assert_eq!(parse_modifier("-0.25 days").unwrap(), Modifier::Days(-0.25));
//     }
//
//     #[test]
//     fn test_parse_hours() {
//         assert_eq!(parse_modifier("12 hours").unwrap(), Modifier::Hours(12.0));
//         assert_eq!(parse_modifier("-2 hours").unwrap(), Modifier::Hours(-2.0));
//         assert_eq!(parse_modifier("+3  HOURS").unwrap(), Modifier::Hours(3.0));
//         // Fractional hours
//         assert_eq!(parse_modifier("0.5 hours").unwrap(), Modifier::Hours(0.5));
//     }
//
//     #[test]
//     fn test_parse_minutes() {
//         assert_eq!(
//             parse_modifier("30 minutes").unwrap(),
//             Modifier::Minutes(30.0)
//         );
//         assert_eq!(
//             parse_modifier("-15 minutes").unwrap(),
//             Modifier::Minutes(-15.0)
//         );
//         assert_eq!(
//             parse_modifier("+45  MINUTES").unwrap(),
//             Modifier::Minutes(45.0)
//         );
//     }
//
//     #[test]
//     fn test_parse_seconds() {
//         assert_eq!(
//             parse_modifier("45 seconds").unwrap(),
//             Modifier::Seconds(45.0)
//         );
//         assert_eq!(
//             parse_modifier("-10 seconds").unwrap(),
//             Modifier::Seconds(-10.0)
//         );
//         assert_eq!(
//             parse_modifier("+20  SECONDS").unwrap(),
//             Modifier::Seconds(20.0)
//         );
//     }
//
//     #[test]
//     fn test_parse_months() {
//         assert_eq!(parse_modifier("3 months").unwrap(), Modifier::Months(3.0));
//         assert_eq!(parse_modifier("-1 months").unwrap(), Modifier::Months(-1.0));
//         assert_eq!(parse_modifier("+6  MONTHS").unwrap(), Modifier::Months(6.0));
//     }
//
//     #[test]
//     fn test_parse_years() {
//         assert_eq!(parse_modifier("2 years").unwrap(), Modifier::Years(2.0));
//         assert_eq!(parse_modifier("-1 years").unwrap(), Modifier::Years(-1.0));
//         assert_eq!(parse_modifier("+10  YEARS").unwrap(), Modifier::Years(10.0));
//     }
//
//     #[test]
//     fn test_parse_time_offset() {
//         assert_eq!(
//             parse_modifier("+01:30").unwrap(),
//             Modifier::TimeOffset(TimeDelta::hours(1) + TimeDelta::minutes(30))
//         );
//         assert_eq!(
//             parse_modifier("-00:45").unwrap(),
//             Modifier::TimeOffset(TimeDelta::minutes(-45))
//         );
//         assert_eq!(
//             parse_modifier("+02:15:30").unwrap(),
//             Modifier::TimeOffset(
//                 TimeDelta::hours(2) + TimeDelta::minutes(15) + TimeDelta::seconds(30)
//             )
//         );
//         assert_eq!(
//             parse_modifier("+02:15:30.250").unwrap(),
//             Modifier::TimeOffset(
//                 TimeDelta::hours(2) + TimeDelta::minutes(15) + TimeDelta::seconds(30)
//             )
//         );
//     }
//
//     #[test]
//     fn test_parse_date_offset() {
//         assert_eq!(
//             parse_modifier("+2023-05-15").unwrap(),
//             Modifier::DateOffset {
//                 years: 2023,
//                 months: 5,
//                 days: 15,
//             }
//         );
//         assert_eq!(
//             parse_modifier("-2023-05-15").unwrap(),
//             Modifier::DateOffset {
//                 years: -2023,
//                 months: -5,
//                 days: -15,
//             }
//         );
//     }
//
//     #[test]
//     fn test_parse_date_time_offset() {
//         assert_eq!(
//             parse_modifier("+2023-05-15 14:30").unwrap(),
//             Modifier::DateTimeOffset {
//                 years: 2023,
//                 months: 5,
//                 days: 15,
//                 seconds: ((14 * 60 + 30) * 60) as f64,
//             }
//         );
//         assert_eq!(
//             parse_modifier("-0001-05-15 14:30").unwrap(),
//             Modifier::DateTimeOffset {
//                 years: -1,
//                 months: -5,
//                 days: -15,
//                 seconds: -((14 * 60 + 30) * 60) as f64,
//             }
//         );
//     }
//
//     #[test]
//     fn test_parse_start_of() {
//         assert_eq!(
//             parse_modifier("start of month").unwrap(),
//             Modifier::StartOfMonth
//         );
//         assert_eq!(
//             parse_modifier("START OF MONTH").unwrap(),
//             Modifier::StartOfMonth
//         );
//         assert_eq!(
//             parse_modifier("start of year").unwrap(),
//             Modifier::StartOfYear
//         );
//         assert_eq!(
//             parse_modifier("START OF YEAR").unwrap(),
//             Modifier::StartOfYear
//         );
//         assert_eq!(
//             parse_modifier("start of day").unwrap(),
//             Modifier::StartOfDay
//         );
//         assert_eq!(
//             parse_modifier("START OF DAY").unwrap(),
//             Modifier::StartOfDay
//         );
//     }
//
//     #[test]
//     fn test_parse_weekday() {
//         assert_eq!(parse_modifier("weekday 0").unwrap(), Modifier::Weekday(0));
//         assert_eq!(parse_modifier("WEEKDAY 6").unwrap(), Modifier::Weekday(6));
//     }
//
//     #[test]
//     fn test_parse_ceiling_modifier() {
//         assert_eq!(parse_modifier("ceiling").unwrap(), Modifier::Ceiling);
//         assert_eq!(parse_modifier("CEILING").unwrap(), Modifier::Ceiling);
//     }
//
//     #[test]
//     fn test_parse_other_modifiers() {
//         assert_eq!(parse_modifier("unixepoch").unwrap(), Modifier::UnixEpoch);
//         assert_eq!(parse_modifier("UNIXEPOCH").unwrap(), Modifier::UnixEpoch);
//         assert_eq!(parse_modifier("julianday").unwrap(), Modifier::JulianDay);
//         assert_eq!(parse_modifier("JULIANDAY").unwrap(), Modifier::JulianDay);
//         assert_eq!(parse_modifier("auto").unwrap(), Modifier::Auto);
//         assert_eq!(parse_modifier("AUTO").unwrap(), Modifier::Auto);
//         assert_eq!(parse_modifier("localtime").unwrap(), Modifier::Localtime);
//         assert_eq!(parse_modifier("LOCALTIME").unwrap(), Modifier::Localtime);
//         assert_eq!(parse_modifier("utc").unwrap(), Modifier::Utc);
//         assert_eq!(parse_modifier("UTC").unwrap(), Modifier::Utc);
//         assert_eq!(parse_modifier("subsec").unwrap(), Modifier::Subsec);
//         assert_eq!(parse_modifier("SUBSEC").unwrap(), Modifier::Subsec);
//         assert_eq!(parse_modifier("subsecond").unwrap(), Modifier::Subsec);
//         assert_eq!(parse_modifier("SUBSECOND").unwrap(), Modifier::Subsec);
//     }
//
//     #[test]
//     fn test_parse_invalid_modifier() {
//         assert!(parse_modifier("invalid modifier").is_err());
//         assert!(parse_modifier("5").is_err());
//         assert!(parse_modifier("days").is_err());
//         assert!(parse_modifier("++5 days").is_err());
//         assert!(parse_modifier("weekday 7").is_err());
//     }
//
//     fn create_datetime(
//         year: i32,
//         month: u32,
//         day: u32,
//         hour: u32,
//         min: u32,
//         sec: u32,
//     ) -> NaiveDateTime {
//         NaiveDate::from_ymd_opt(year, month, day)
//             .unwrap()
//             .and_hms_opt(hour, min, sec)
//             .unwrap()
//     }
//
//     fn setup_datetime() -> NaiveDateTime {
//         create_datetime(2023, 6, 15, 12, 30, 45)
//     }
//
//     #[test]
//     fn test_apply_modifier_days() {
//         let mut dt = setup_datetime();
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(&mut dt, "5 days", &mut n_floor, None, false, &mut is_utc).unwrap();
//         assert_eq!(dt, create_datetime(2023, 6, 20, 12, 30, 45));
//
//         dt = setup_datetime();
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(&mut dt, "-3 days", &mut n_floor, None, false, &mut is_utc).unwrap();
//         assert_eq!(dt, create_datetime(2023, 6, 12, 12, 30, 45));
//     }
//
//     #[test]
//     fn test_apply_modifier_hours() {
//         let mut dt = setup_datetime();
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(&mut dt, "6 hours", &mut n_floor, None, false, &mut is_utc).unwrap();
//         assert_eq!(dt, create_datetime(2023, 6, 15, 18, 30, 45));
//
//         dt = setup_datetime();
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(&mut dt, "-2 hours", &mut n_floor, None, false, &mut is_utc).unwrap();
//         assert_eq!(dt, create_datetime(2023, 6, 15, 10, 30, 45));
//     }
//
//     #[test]
//     fn test_apply_modifier_minutes() {
//         let mut dt = setup_datetime();
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(
//             &mut dt,
//             "45 minutes",
//             &mut n_floor,
//             None,
//             false,
//             &mut is_utc,
//         )
//         .unwrap();
//         assert_eq!(dt, create_datetime(2023, 6, 15, 13, 15, 45));
//
//         dt = setup_datetime();
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(
//             &mut dt,
//             "-15 minutes",
//             &mut n_floor,
//             None,
//             false,
//             &mut is_utc,
//         )
//         .unwrap();
//         assert_eq!(dt, create_datetime(2023, 6, 15, 12, 15, 45));
//     }
//
//     #[test]
//     fn test_apply_modifier_seconds() {
//         let mut dt = setup_datetime();
//
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(
//             &mut dt,
//             "30 seconds",
//             &mut n_floor,
//             None,
//             false,
//             &mut is_utc,
//         )
//         .unwrap();
//         assert_eq!(dt, create_datetime(2023, 6, 15, 12, 31, 15));
//
//         dt = setup_datetime();
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(
//             &mut dt,
//             "-20 seconds",
//             &mut n_floor,
//             None,
//             false,
//             &mut is_utc,
//         )
//         .unwrap();
//         assert_eq!(dt, create_datetime(2023, 6, 15, 12, 30, 25));
//     }
//
//     #[test]
//     fn test_apply_modifier_time_offset() {
//         let mut dt = setup_datetime();
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(&mut dt, "+01:30", &mut n_floor, None, false, &mut is_utc).unwrap();
//         assert_eq!(dt, create_datetime(2023, 6, 15, 14, 0, 45));
//
//         dt = setup_datetime();
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(&mut dt, "-00:45", &mut n_floor, None, false, &mut is_utc).unwrap();
//         assert_eq!(dt, create_datetime(2023, 6, 15, 11, 45, 45));
//     }
//
//     #[test]
//     fn test_apply_modifier_date_time_offset() {
//         let mut dt = setup_datetime();
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(
//             &mut dt,
//             "+0001-01-01 01:01",
//             &mut n_floor,
//             None,
//             false,
//             &mut is_utc,
//         )
//         .unwrap();
//         assert_eq!(dt, create_datetime(2024, 7, 16, 13, 31, 45));
//
//         dt = setup_datetime();
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(
//             &mut dt,
//             "-0001-01-01 01:01",
//             &mut n_floor,
//             None,
//             false,
//             &mut is_utc,
//         )
//         .unwrap();
//         assert_eq!(dt, create_datetime(2022, 5, 14, 11, 29, 45));
//
//         // Test with larger offsets
//         dt = setup_datetime();
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(
//             &mut dt,
//             "+0002-03-04 05:06",
//             &mut n_floor,
//             None,
//             false,
//             &mut is_utc,
//         )
//         .unwrap();
//         assert_eq!(dt, create_datetime(2025, 9, 19, 17, 36, 45));
//
//         dt = setup_datetime();
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(
//             &mut dt,
//             "-0002-03-04 05:06",
//             &mut n_floor,
//             None,
//             false,
//             &mut is_utc,
//         )
//         .unwrap();
//         assert_eq!(dt, create_datetime(2021, 3, 11, 7, 24, 45));
//     }
//
//     #[test]
//     fn test_apply_modifier_start_of_year() {
//         let mut dt = setup_datetime();
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(
//             &mut dt,
//             "start of year",
//             &mut n_floor,
//             None,
//             false,
//             &mut is_utc,
//         )
//         .unwrap();
//         assert_eq!(dt, create_datetime(2023, 1, 1, 0, 0, 0));
//     }
//
//     #[test]
//     fn test_apply_modifier_start_of_day() {
//         let mut dt = setup_datetime();
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(
//             &mut dt,
//             "start of day",
//             &mut n_floor,
//             None,
//             false,
//             &mut is_utc,
//         )
//         .unwrap();
//         assert_eq!(dt, create_datetime(2023, 6, 15, 0, 0, 0));
//     }
//
//     fn text(value: &str) -> Value {
//         Value::build_text(value.to_string())
//     }
//
//     fn format(dt: NaiveDateTime) -> String {
//         dt.format("%Y-%m-%d %H:%M:%S").to_string()
//     }
//     fn weekday_sunday_based(dt: &NaiveDateTime) -> u32 {
//         dt.weekday().num_days_from_sunday()
//     }
//
//     #[test]
//     fn test_single_modifier() {
//         let time = setup_datetime();
//         let expected = format(time - TimeDelta::days(1));
//         let result = exec_datetime(
//             &[text("2023-06-15 12:30:45"), text("-1 day")],
//             DateTimeOutput::DateTime,
//         );
//         assert_eq!(result, text(&expected));
//     }
//
//     #[test]
//     fn test_multiple_modifiers() {
//         let time = setup_datetime();
//         let expected = format(time - TimeDelta::days(1) + TimeDelta::hours(3));
//         let result = exec_datetime(
//             &[
//                 text("2023-06-15 12:30:45"),
//                 text("-1 day"),
//                 text("+3 hours"),
//             ],
//             DateTimeOutput::DateTime,
//         );
//         assert_eq!(result, text(&expected));
//     }
//
//     #[test]
//     fn test_subsec_modifier() {
//         let time = setup_datetime();
//         let result = exec_datetime(
//             &[text("2023-06-15 12:30:45"), text("subsec")],
//             DateTimeOutput::Time,
//         );
//         let result = NaiveTime::parse_from_str(&result.to_string(), "%H:%M:%S%.3f").unwrap();
//         assert_eq!(time.time(), result);
//     }
//
//     #[test]
//     fn test_start_of_day_modifier() {
//         let time = setup_datetime();
//         let start_of_day = time.date().and_hms_opt(0, 0, 0).unwrap();
//         let expected = format(start_of_day - TimeDelta::days(1));
//         let result = exec_datetime(
//             &[
//                 text("2023-06-15 12:30:45"),
//                 text("start of day"),
//                 text("-1 day"),
//             ],
//             DateTimeOutput::DateTime,
//         );
//         assert_eq!(result, text(&expected));
//     }
//
//     #[test]
//     fn test_start_of_month_modifier() {
//         let time = setup_datetime();
//         let start_of_month = NaiveDate::from_ymd_opt(time.year(), time.month(), 1)
//             .unwrap()
//             .and_hms_opt(0, 0, 0)
//             .unwrap();
//         let expected = format(start_of_month + TimeDelta::days(1));
//         let result = exec_datetime(
//             &[
//                 text("2023-06-15 12:30:45"),
//                 text("start of month"),
//                 text("+1 day"),
//             ],
//             DateTimeOutput::DateTime,
//         );
//         assert_eq!(result, text(&expected));
//     }
//
//     #[test]
//     fn test_start_of_year_modifier() {
//         let time = setup_datetime();
//         let start_of_year = NaiveDate::from_ymd_opt(time.year(), 1, 1)
//             .unwrap()
//             .and_hms_opt(0, 0, 0)
//             .unwrap();
//         let expected = format(start_of_year + TimeDelta::days(30) + TimeDelta::hours(5));
//         let result = exec_datetime(
//             &[
//                 text("2023-06-15 12:30:45"),
//                 text("start of year"),
//                 text("+30 days"),
//                 text("+5 hours"),
//             ],
//             DateTimeOutput::DateTime,
//         );
//         assert_eq!(result, text(&expected));
//     }
//
//     #[test]
//     fn test_timezone_modifiers() {
//         let dt = setup_datetime();
//         let result_local = exec_datetime(
//             &[text("2023-06-15 12:30:45"), text("localtime")],
//             DateTimeOutput::DateTime,
//         );
//         assert_eq!(
//             result_local,
//             text(
//                 &dt.and_utc()
//                     .with_timezone(&chrono::Local)
//                     .format("%Y-%m-%d %H:%M:%S")
//                     .to_string()
//             )
//         );
//         // TODO: utc modifier assumes time given is not already utc
//         // add test when fixed in the future
//     }
//
//     #[test]
//     fn test_combined_modifiers() {
//         let time = create_datetime(2000, 1, 1, 0, 0, 0);
//         let expected = time - TimeDelta::days(1)
//             + TimeDelta::hours(5)
//             + TimeDelta::minutes(30)
//             + TimeDelta::seconds(15);
//         let result = exec_datetime(
//             &[
//                 text("2000-01-01 00:00:00"),
//                 text("-1 day"),
//                 text("+5 hours"),
//                 text("+30 minutes"),
//                 text("+15 seconds"),
//                 text("subsec"),
//             ],
//             DateTimeOutput::DateTime,
//         );
//         let result =
//             NaiveDateTime::parse_from_str(&result.to_string(), "%Y-%m-%d %H:%M:%S%.3f").unwrap();
//         assert_eq!(expected, result);
//     }
//
//     #[test]
//     fn test_max_datetime_limit() {
//         // max datetime limit
//         let max = NaiveDate::from_ymd_opt(9999, 12, 31)
//             .unwrap()
//             .and_hms_opt(23, 59, 59)
//             .unwrap();
//         let expected = format(max);
//         let result = exec_datetime(&[text("9999-12-31 23:59:59")], DateTimeOutput::DateTime);
//         assert_eq!(result, text(&expected));
//     }
//
//     // leap second
//     #[test]
//     fn test_leap_second_ignored() {
//         // SQLite returns NULL for invalid times (like second=60 which parsing fails for ex. SELECT typeof(datetime('2023-05-18 15:30:45+25:00')); )
//         let result = exec_datetime(&[text("2024-06-30 23:59:60")], DateTimeOutput::DateTime);
//         assert_eq!(result, Value::Null);
//     }
//
//     #[test]
//     fn test_already_on_weekday_no_change() {
//         // 2023-01-01 is a Sunday => weekday 0
//         let mut dt = create_datetime(2023, 1, 1, 12, 0, 0);
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(&mut dt, "weekday 0", &mut n_floor, None, false, &mut is_utc).unwrap();
//         assert_eq!(dt, create_datetime(2023, 1, 1, 12, 0, 0));
//         assert_eq!(weekday_sunday_based(&dt), 0);
//     }
//
//     #[test]
//     fn test_move_forward_if_different() {
//         // 2023-01-01 is a Sunday => weekday 0
//         // "weekday 1" => next Monday => 2023-01-02
//         let mut dt = create_datetime(2023, 1, 1, 12, 0, 0);
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(&mut dt, "weekday 1", &mut n_floor, None, false, &mut is_utc).unwrap();
//         assert_eq!(dt, create_datetime(2023, 1, 2, 12, 0, 0));
//         assert_eq!(weekday_sunday_based(&dt), 1);
//
//         // 2023-01-03 is a Tuesday => weekday 2
//         // "weekday 5" => next Friday => 2023-01-06
//         let mut dt = create_datetime(2023, 1, 3, 12, 0, 0);
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(&mut dt, "weekday 5", &mut n_floor, None, false, &mut is_utc).unwrap();
//         assert_eq!(dt, create_datetime(2023, 1, 6, 12, 0, 0));
//         assert_eq!(weekday_sunday_based(&dt), 5);
//     }
//
//     #[test]
//     fn test_wrap_around_weekend() {
//         // 2023-01-06 is a Friday => weekday 5
//         // "weekday 0" => next Sunday => 2023-01-08
//         let mut dt = create_datetime(2023, 1, 6, 12, 0, 0);
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(&mut dt, "weekday 0", &mut n_floor, None, false, &mut is_utc).unwrap();
//         assert_eq!(dt, create_datetime(2023, 1, 8, 12, 0, 0));
//         assert_eq!(weekday_sunday_based(&dt), 0);
//
//         // Now confirm that being on Sunday (weekday 0) and asking for "weekday 0" stays put
//         let mut is_utc = false;
//         apply_modifier(&mut dt, "weekday 0", &mut n_floor, None, false, &mut is_utc).unwrap();
//         assert_eq!(dt, create_datetime(2023, 1, 8, 12, 0, 0));
//         assert_eq!(weekday_sunday_based(&dt), 0);
//     }
//
//     #[test]
//     fn test_same_day_stays_put() {
//         // 2023-01-05 is a Thursday => weekday 4
//         // Asking for weekday 4 => no change
//         let mut dt = create_datetime(2023, 1, 5, 12, 0, 0);
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(&mut dt, "weekday 4", &mut n_floor, None, false, &mut is_utc).unwrap();
//         assert_eq!(dt, create_datetime(2023, 1, 5, 12, 0, 0));
//         assert_eq!(weekday_sunday_based(&dt), 4);
//     }
//
//     #[test]
//     fn test_already_on_friday_no_change() {
//         // 2023-01-06 is a Friday => weekday 5
//         // Asking for weekday 5 => no change if already on Friday
//         let mut dt = create_datetime(2023, 1, 6, 12, 0, 0);
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(&mut dt, "weekday 5", &mut n_floor, None, false, &mut is_utc).unwrap();
//         assert_eq!(dt, create_datetime(2023, 1, 6, 12, 0, 0));
//         assert_eq!(weekday_sunday_based(&dt), 5);
//     }
//
//     #[test]
//     fn test_apply_modifier_julianday() {
//         let dt = create_datetime(2000, 1, 1, 12, 0, 0);
//
//         // Convert datetime to julian day using our implementation
//         let julian_day_value = to_julian_day_exact(&dt);
//
//         // Convert back
//         let dt_result = julian_day_to_datetime(julian_day_value).unwrap();
//         assert_eq!(dt_result, dt);
//     }
//
//     #[test]
//     fn test_apply_modifier_start_of_month() {
//         let mut dt = create_datetime(2023, 6, 15, 12, 30, 45);
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(
//             &mut dt,
//             "start of month",
//             &mut n_floor,
//             None,
//             false,
//             &mut is_utc,
//         )
//         .unwrap();
//         assert_eq!(dt, create_datetime(2023, 6, 1, 0, 0, 0));
//     }
//
//     #[test]
//     fn test_apply_modifier_subsec() {
//         let mut dt = create_datetime(2023, 6, 15, 12, 30, 45);
//         let dt_with_nanos = dt.with_nanosecond(123_456_789).unwrap();
//         dt = dt_with_nanos;
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(&mut dt, "subsec", &mut n_floor, None, false, &mut is_utc).unwrap();
//         assert_eq!(dt, dt_with_nanos);
//     }
//
//     #[test]
//     fn test_apply_modifier_floor_modifier_n_floor_gt_0() {
//         let mut dt = create_datetime(2023, 6, 15, 12, 30, 45);
//         let mut n_floor = 3;
//
//         let mut is_utc = false;
//         apply_modifier(&mut dt, "floor", &mut n_floor, None, false, &mut is_utc).unwrap();
//         assert_eq!(dt, create_datetime(2023, 6, 12, 12, 30, 45));
//     }
//
//     #[test]
//     fn test_apply_modifier_floor_modifier_n_floor_le_0() {
//         let mut dt = create_datetime(2023, 6, 15, 12, 30, 45);
//         let mut n_floor = 0;
//
//         let mut is_utc = false;
//         apply_modifier(&mut dt, "floor", &mut n_floor, None, false, &mut is_utc).unwrap();
//         assert_eq!(dt, create_datetime(2023, 6, 15, 12, 30, 45));
//
//         n_floor = 2;
//         let mut is_utc = false;
//         apply_modifier(&mut dt, "floor", &mut n_floor, None, false, &mut is_utc).unwrap();
//         assert_eq!(dt, create_datetime(2023, 6, 13, 12, 30, 45));
//     }
//
//     #[test]
//     fn test_apply_modifier_ceiling_modifier_sets_n_floor_to_zero() {
//         let mut dt = create_datetime(2023, 6, 15, 12, 30, 45);
//         let mut n_floor = 5;
//
//         let mut is_utc = false;
//         apply_modifier(&mut dt, "ceiling", &mut n_floor, None, false, &mut is_utc).unwrap();
//         assert_eq!(n_floor, 0);
//     }
//
//     #[test]
//     fn test_apply_modifier_start_of_month_basic() {
//         // Basic check: from mid-month to the 1st at 00:00:00.
//         let mut dt = create_datetime(2023, 6, 15, 12, 30, 45);
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(
//             &mut dt,
//             "start of month",
//             &mut n_floor,
//             None,
//             false,
//             &mut is_utc,
//         )
//         .unwrap();
//         assert_eq!(dt, create_datetime(2023, 6, 1, 0, 0, 0));
//     }
//
//     #[test]
//     fn test_apply_modifier_start_of_month_already_at_first() {
//         // If we're already at the start of the month, no change.
//         let mut dt = create_datetime(2023, 6, 1, 0, 0, 0);
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(
//             &mut dt,
//             "start of month",
//             &mut n_floor,
//             None,
//             false,
//             &mut is_utc,
//         )
//         .unwrap();
//         assert_eq!(dt, create_datetime(2023, 6, 1, 0, 0, 0));
//     }
//
//     #[test]
//     fn test_apply_modifier_start_of_month_edge_case() {
//         // edge case: month boundary. 2023-07-31 -> start of July.
//         let mut dt = create_datetime(2023, 7, 31, 23, 59, 59);
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(
//             &mut dt,
//             "start of month",
//             &mut n_floor,
//             None,
//             false,
//             &mut is_utc,
//         )
//         .unwrap();
//         assert_eq!(dt, create_datetime(2023, 7, 1, 0, 0, 0));
//     }
//
//     #[test]
//     fn test_apply_modifier_subsec_no_change() {
//         let mut dt = create_datetime(2023, 6, 15, 12, 30, 45);
//         let dt_with_nanos = dt.with_nanosecond(123_456_789).unwrap();
//         dt = dt_with_nanos;
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(&mut dt, "subsec", &mut n_floor, None, false, &mut is_utc).unwrap();
//         assert_eq!(dt, dt_with_nanos);
//     }
//
//     #[test]
//     fn test_apply_modifier_subsec_preserves_fractional_seconds() {
//         let mut dt = create_datetime(2025, 1, 2, 4, 12, 21)
//             .with_nanosecond(891_000_000) // 891 milliseconds
//             .unwrap();
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(&mut dt, "subsec", &mut n_floor, None, false, &mut is_utc).unwrap();
//
//         let formatted = dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
//         assert_eq!(formatted, "2025-01-02 04:12:21.891");
//     }
//
//     #[test]
//     fn test_apply_modifier_subsec_no_fractional_seconds() {
//         let mut dt = create_datetime(2025, 1, 2, 4, 12, 21);
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(&mut dt, "subsec", &mut n_floor, None, false, &mut is_utc).unwrap();
//
//         let formatted = dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
//         assert_eq!(formatted, "2025-01-02 04:12:21.000");
//     }
//
//     #[test]
//     fn test_apply_modifier_subsec_truncate_to_milliseconds() {
//         let mut dt = create_datetime(2025, 1, 2, 4, 12, 21)
//             .with_nanosecond(891_123_456)
//             .unwrap();
//         let mut n_floor = 0;
//         let mut is_utc = false;
//         apply_modifier(&mut dt, "subsec", &mut n_floor, None, false, &mut is_utc).unwrap();
//
//         let formatted = dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
//         assert_eq!(formatted, "2025-01-02 04:12:21.891");
//     }
//
//     #[test]
//     fn test_is_leap_second() {
//         let dt = DateTime::from_timestamp(1483228799, 999_999_999)
//             .unwrap()
//             .naive_utc();
//         assert!(!is_leap_second(&dt));
//
//         let dt = DateTime::from_timestamp(1483228799, 1_500_000_000)
//             .unwrap()
//             .naive_utc();
//         assert!(is_leap_second(&dt));
//     }
//
//     #[test]
//     fn test_strftime() {}
//
//     #[test]
//     fn test_exec_timediff() {
//         let start = Value::build_text("12:00:00");
//         let end = Value::build_text("14:30:45");
//         let expected = Value::build_text("-0000-00-00 02:30:45.000");
//         assert_eq!(exec_timediff(&[start, end]), expected);
//
//         let start = Value::build_text("14:30:45");
//         let end = Value::build_text("12:00:00");
//         let expected = Value::build_text("+0000-00-00 02:30:45.000");
//         assert_eq!(exec_timediff(&[start, end]), expected);
//
//         let start = Value::build_text("12:00:01.300");
//         let end = Value::build_text("12:00:00.500");
//         let expected = Value::build_text("+0000-00-00 00:00:00.800");
//         assert_eq!(exec_timediff(&[start, end]), expected);
//
//         let start = Value::build_text("13:30:00");
//         let end = Value::build_text("16:45:30");
//         let expected = Value::build_text("-0000-00-00 03:15:30.000");
//         assert_eq!(exec_timediff(&[start, end]), expected);
//
//         let start = Value::build_text("2023-05-10 23:30:00");
//         let end = Value::build_text("2023-05-11 01:15:00");
//         let expected = Value::build_text("-0000-00-00 01:45:00.000");
//         assert_eq!(exec_timediff(&[start, end]), expected);
//
//         let start = Value::Null;
//         let end = Value::build_text("12:00:00");
//         let expected = Value::Null;
//         assert_eq!(exec_timediff(&[start, end]), expected);
//
//         let start = Value::build_text("not a time");
//         let end = Value::build_text("12:00:00");
//         let expected = Value::Null;
//         assert_eq!(exec_timediff(&[start, end.clone()]), expected);
//
//         // Test identical times - should return zero duration, not Null
//         let start = Value::build_text("12:00:00");
//         let end = Value::build_text("12:00:00");
//         let expected = Value::build_text("+0000-00-00 00:00:00.000");
//         assert_eq!(exec_timediff(&[start, end]), expected);
//     }
//
//     #[test]
//     fn test_subsec_fixed_time_expansion() {
//         let result = exec_datetime(
//             &[text("2024-01-01 12:00:00"), text("subsec")],
//             DateTimeOutput::DateTime,
//         );
//         assert_eq!(
//             result,
//             text("2024-01-01 12:00:00.000"),
//             "Failed to expand zero-nanosecond time with subsec"
//         );
//     }
//
//     #[test]
//     fn test_subsec_date_only_expansion() {
//         let result = exec_datetime(
//             &[text("2024-01-01"), text("subsec")],
//             DateTimeOutput::DateTime,
//         );
//         assert_eq!(
//             result,
//             text("2024-01-01 00:00:00.000"),
//             "Failed to expand date-only input to midnight.000"
//         );
//     }
//
//     #[test]
//     fn test_subsec_iso_separator() {
//         let result = exec_datetime(
//             &[text("2024-01-01T15:30:00"), text("subsec")],
//             DateTimeOutput::DateTime,
//         );
//         assert_eq!(
//             result,
//             text("2024-01-01 15:30:00.000"),
//             "Failed to normalize ISO T separator with subsec"
//         );
//     }
//
//     #[test]
//     fn test_subsec_chaining_before_math() {
//         let result = exec_datetime(
//             &[text("2024-01-01 12:00:00"), text("subsec"), text("+1 hour")],
//             DateTimeOutput::DateTime,
//         );
//         assert_eq!(
//             result,
//             text("2024-01-01 13:00:00.000"),
//             "Subsec flag failed to persist through subsequent arithmetic"
//         );
//     }
//
//     #[test]
//     fn test_subsec_chaining_after_math() {
//         let result = exec_datetime(
//             &[text("2024-01-01 12:00:00"), text("+1 hour"), text("subsec")],
//             DateTimeOutput::DateTime,
//         );
//         assert_eq!(
//             result,
//             text("2024-01-01 13:00:00.000"),
//             "Standard chaining failed"
//         );
//     }
//
//     #[test]
//     fn test_subsec_rollover_math() {
//         let result = exec_datetime(
//             &[
//                 text("2024-01-01 12:00:00.999"),
//                 text("+1 second"),
//                 text("subsec"),
//             ],
//             DateTimeOutput::DateTime,
//         );
//         assert_eq!(
//             result,
//             text("2024-01-01 12:00:01.999"),
//             "Rollover math with milliseconds failed"
//         );
//     }
//
//     #[test]
//     fn test_subsec_case_insensitivity() {
//         let result = exec_datetime(
//             &[text("2024-01-01 12:00:00"), text("SuBsEc")],
//             DateTimeOutput::DateTime,
//         );
//         assert_eq!(
//             result,
//             text("2024-01-01 12:00:00.000"),
//             "Case insensitivity check failed"
//         );
//     }
//
//     #[test]
//     fn test_parse_modifier_unicode_no_panic() {
//         // Regression test: parse_modifier should not panic on multi-byte UTF-8 strings
//         // that are shorter than expected modifier suffixes when measured in bytes
//         let unicode_inputs = [
//             "!*\u{ea37}", // <-- this produced a crash in SQLancer :]
//             "\u{1F600}",  // Emoji (4 bytes)
//             "日本語",     // Japanese text
//             "中",         // Single Chinese character
//             "\u{0080}",   // 2-byte UTF-8
//             "",           // Empty string
//         ];
//
//         for input in unicode_inputs {
//             // Should not panic - just return an error for invalid modifiers
//             let result = parse_modifier(input);
//             assert!(
//                 result.is_err(),
//                 "Expected error for invalid modifier: {input}"
//             );
//         }
//     }
//
//     #[test]
//     fn test_unixepoch_basic_usage() {
//         let result = exec_unixepoch(&[text("1970-01-01 00:00:00")]);
//         assert_eq!(result, Value::Integer(0));
//
//         let result = exec_unixepoch(&[text("2023-01-01 00:00:00")]);
//         assert_eq!(result, Value::Integer(1672531200));
//
//         let result = exec_unixepoch(&[text("1969-12-31 23:59:59")]);
//         assert_eq!(result, Value::Integer(-1));
//
//         let result = exec_unixepoch(&[Value::Float(2440587.5)]);
//         assert_eq!(result, Value::Integer(0));
//     }
//
//     #[test]
//     fn test_unixepoch_numeric_modifiers_unixepoch() {
//         let result = exec_unixepoch(&[Value::Integer(1672531200), text("unixepoch")]);
//         assert_eq!(result, Value::Integer(1672531200));
//
//         let result = exec_unixepoch(&[Value::Integer(0), text("unixepoch")]);
//         assert_eq!(result, Value::Integer(0));
//
//         let result = exec_unixepoch(&[
//             Value::Integer(1672531200),
//             text("unixepoch"),
//             text("start of year"),
//         ]);
//         assert_eq!(result, Value::Integer(1672531200));
//     }
//
//     #[test]
//     fn test_unixepoch_numeric_modifiers_julianday() {
//         let result = exec_unixepoch(&[Value::Float(2440587.5), text("julianday")]);
//         assert_eq!(result, Value::Integer(0));
//
//         let result = exec_unixepoch(&[Value::Float(2460311.5), text("julianday")]);
//         assert_eq!(result, Value::Integer(1704153600));
//
//         let result = exec_unixepoch(&[Value::Float(0.0), text("julianday")]);
//         match result {
//             Value::Integer(i) => assert_eq!(i, -210866760000),
//             _ => panic!("Expected Integer result for JD 0"),
//         }
//     }
//
//     #[test]
//     fn test_unixepoch_numeric_modifiers_auto() {
//         let result = exec_unixepoch(&[Value::Float(2440587.5), text("auto")]);
//         assert_eq!(result, Value::Integer(0));
//
//         let result = exec_unixepoch(&[Value::Integer(1672531200), text("auto")]);
//         assert_eq!(result, Value::Integer(1672531200));
//
//         let result = exec_unixepoch(&[Value::Float(0.0), text("auto")]);
//         match result {
//             Value::Integer(i) => assert!(i < 0, "Expected JD interpretation (negative), got {i}"),
//             _ => panic!("Expected Integer result"),
//         }
//     }
//
//     #[test]
//     fn test_unixepoch_invalid_usage() {
//         let result = exec_unixepoch(&[Value::Integer(0), text("start of year"), text("unixepoch")]);
//         assert_eq!(result, Value::Null);
//
//         let result = exec_unixepoch(&[text("2023-01-01"), text("unixepoch")]);
//         assert_eq!(result, Value::Null);
//
//         let result = exec_unixepoch(&[Value::Integer(0), text("unixepoch"), text("julianday")]);
//         assert_eq!(result, Value::Null);
//     }
//
//     #[test]
//     fn test_unixepoch_complex_calculations() {
//         let result = exec_unixepoch(&[Value::Float(2440587.5), text("julianday"), text("+1 day")]);
//         assert_eq!(result, Value::Integer(86400));
//
//         let result = exec_unixepoch(&[
//             Value::Float(2460311.5),
//             text("auto"),
//             text("start of month"),
//             text("+1 month"),
//         ]);
//         assert_eq!(result, Value::Integer(1706745600));
//     }
//
//     #[test]
//     fn test_unixepoch_subsecond_precision() {
//         let result = exec_unixepoch(&[text("1970-01-01 00:00:00.0006"), text("subsec")]);
//         match result {
//             Value::Float(f) => assert!((f - 0.001).abs() < f64::EPSILON),
//             _ => panic!("Expected Float result"),
//         }
//         let result = exec_unixepoch(&[text("1970-01-01 00:00:00.9996"), text("subsec")]);
//         match result {
//             Value::Float(f) => assert!((f - 0.999).abs() < f64::EPSILON),
//             _ => panic!("Expected Float result"),
//         }
//     }
//
//     // Tests specifically for the fast path datetime parsing functions
//     #[test]
//     fn test_fast_path_date_only() {
//         // Valid dates - check exact values
//         assert_eq!(
//             parse_datetime_fast("2024-01-01"),
//             Some(create_datetime(2024, 1, 1, 0, 0, 0))
//         );
//         assert_eq!(
//             parse_datetime_fast("0001-01-01"),
//             Some(create_datetime(1, 1, 1, 0, 0, 0))
//         );
//         assert_eq!(
//             parse_datetime_fast("9999-12-31"),
//             Some(create_datetime(9999, 12, 31, 0, 0, 0))
//         );
//
//         // Leap year Feb 29
//         assert_eq!(
//             parse_datetime_fast("2024-02-29"),
//             Some(create_datetime(2024, 2, 29, 0, 0, 0))
//         );
//         // Non-leap year Feb 29 overflows to Mar 1 (SQLite compatibility)
//         assert_eq!(
//             parse_datetime_fast("2023-02-29"),
//             Some(create_datetime(2023, 3, 1, 0, 0, 0))
//         );
//
//         // Invalid months still rejected
//         assert_eq!(parse_datetime_fast("2024-00-01"), None); // Month 0
//         assert_eq!(parse_datetime_fast("2024-13-01"), None); // Month 13
//
//         // Invalid days overflow (SQLite compatibility)
//         assert_eq!(parse_datetime_fast("2024-01-00"), None); // Day 0 rejected (can't add -1 days)
//         assert_eq!(
//             parse_datetime_fast("2024-01-32"),
//             Some(create_datetime(2024, 2, 1, 0, 0, 0))
//         ); // Day 32 -> Feb 1
//
//         // Wrong separators
//         assert_eq!(parse_datetime_fast("2024/01/01"), None);
//         assert_eq!(parse_datetime_fast("2024.01.01"), None);
//
//         // Non-digits
//         assert_eq!(parse_datetime_fast("202X-01-01"), None);
//         assert_eq!(parse_datetime_fast("2024-0a-01"), None);
//     }
//
//     #[test]
//     fn test_fast_path_datetime_formats() {
//         // YYYY-MM-DD HH:MM (16 chars)
//         assert_eq!(
//             parse_datetime_fast("2024-01-15 10:30"),
//             Some(create_datetime(2024, 1, 15, 10, 30, 0))
//         );
//         assert_eq!(
//             parse_datetime_fast("2024-01-15T10:30"),
//             Some(create_datetime(2024, 1, 15, 10, 30, 0))
//         );
//         assert_eq!(parse_datetime_fast("2024-01-15X10:30"), None); // Bad separator
//
//         // YYYY-MM-DD HH:MM:SS (19 chars)
//         assert_eq!(
//             parse_datetime_fast("2024-01-15 10:30:45"),
//             Some(create_datetime(2024, 1, 15, 10, 30, 45))
//         );
//         assert_eq!(
//             parse_datetime_fast("2024-01-15T10:30:45"),
//             Some(create_datetime(2024, 1, 15, 10, 30, 45))
//         );
//
//         // Invalid time components
//         assert_eq!(parse_datetime_fast("2024-01-15 25:30:45"), None); // Hour > 23
//         assert_eq!(parse_datetime_fast("2024-01-15 10:60:45"), None); // Min > 59
//         assert_eq!(parse_datetime_fast("2024-01-15 10:30:60"), None); // Sec > 59
//     }
//
//     #[test]
//     fn test_fast_path_time_only() {
//         // Time-only formats use 2000-01-01 as the default date per SQLite spec
//         // HH:MM (5 chars)
//         assert_eq!(
//             parse_datetime_fast("10:30"),
//             Some(create_datetime(2000, 1, 1, 10, 30, 0))
//         );
//         assert_eq!(
//             parse_datetime_fast("00:00"),
//             Some(create_datetime(2000, 1, 1, 0, 0, 0))
//         );
//         assert_eq!(
//             parse_datetime_fast("23:59"),
//             Some(create_datetime(2000, 1, 1, 23, 59, 0))
//         );
//         assert_eq!(parse_datetime_fast("24:00"), None); // Hour 24 invalid
//         assert_eq!(parse_datetime_fast("10:60"), None); // Min 60 invalid
//
//         // HH:MM:SS (8 chars)
//         assert_eq!(
//             parse_datetime_fast("10:30:45"),
//             Some(create_datetime(2000, 1, 1, 10, 30, 45))
//         );
//         assert_eq!(
//             parse_datetime_fast("00:00:00"),
//             Some(create_datetime(2000, 1, 1, 0, 0, 0))
//         );
//         assert_eq!(
//             parse_datetime_fast("23:59:59"),
//             Some(create_datetime(2000, 1, 1, 23, 59, 59))
//         );
//
//         // HH:MM:SS.fff (time with fractional) - also uses 2000-01-01
//         let dt = parse_datetime_fast("10:30:45.123").unwrap();
//         assert_eq!(dt.year(), 2000);
//         assert_eq!(dt.month(), 1);
//         assert_eq!(dt.day(), 1);
//         assert_eq!(dt.hour(), 10);
//         assert_eq!(dt.minute(), 30);
//         assert_eq!(dt.second(), 45);
//         assert_eq!(dt.nanosecond(), 123000000);
//
//         let dt = parse_datetime_fast("10:30:45.1").unwrap();
//         assert_eq!(dt.nanosecond(), 100000000);
//     }
//
//     #[test]
//     fn test_fast_path_skips_timezone_strings() {
//         // These should return None (skip fast path) and be handled by slow path
//         assert_eq!(parse_datetime_fast("2024-01-15 10:30:45Z"), None);
//         assert_eq!(parse_datetime_fast("2024-01-15 10:30:45+02:00"), None);
//         assert_eq!(parse_datetime_fast("2024-01-15 10:30:45-05:00"), None);
//         assert_eq!(parse_datetime_fast("10:30:45+02:00"), None);
//     }
//
//     #[test]
//     fn test_fast_path_fractional_seconds_precision() {
//         // Test that fractional seconds are parsed correctly
//         let dt = parse_datetime_fast("2024-01-15 10:30:45.123456789").unwrap();
//         assert_eq!(dt.year(), 2024);
//         assert_eq!(dt.month(), 1);
//         assert_eq!(dt.day(), 15);
//         assert_eq!(dt.hour(), 10);
//         assert_eq!(dt.minute(), 30);
//         assert_eq!(dt.second(), 45);
//         assert_eq!(dt.nanosecond(), 123456789);
//
//         let dt = parse_datetime_fast("2024-01-15 10:30:45.1").unwrap();
//         assert_eq!(dt.nanosecond(), 100000000);
//
//         let dt = parse_datetime_fast("2024-01-15 10:30:45.100").unwrap();
//         assert_eq!(dt.nanosecond(), 100000000);
//
//         let dt = parse_datetime_fast("2024-01-15 10:30:45.000").unwrap();
//         assert_eq!(dt.nanosecond(), 0);
//     }
//
//     #[test]
//     fn test_fast_path_month_day_boundaries() {
//         // 30-day months
//         assert_eq!(
//             parse_datetime_fast("2024-04-30"),
//             Some(create_datetime(2024, 4, 30, 0, 0, 0))
//         );
//         // April 31 overflows to May 1 (SQLite compatibility)
//         assert_eq!(
//             parse_datetime_fast("2024-04-31"),
//             Some(create_datetime(2024, 5, 1, 0, 0, 0))
//         );
//
//         // 31-day months
//         assert_eq!(
//             parse_datetime_fast("2024-01-31"),
//             Some(create_datetime(2024, 1, 31, 0, 0, 0))
//         );
//         assert_eq!(
//             parse_datetime_fast("2024-03-31"),
//             Some(create_datetime(2024, 3, 31, 0, 0, 0))
//         );
//
//         // February
//         assert_eq!(
//             parse_datetime_fast("2024-02-29"),
//             Some(create_datetime(2024, 2, 29, 0, 0, 0))
//         ); // Leap year
//            // Non-leap year Feb 29 overflows to Mar 1 (SQLite compatibility)
//         assert_eq!(
//             parse_datetime_fast("2023-02-29"),
//             Some(create_datetime(2023, 3, 1, 0, 0, 0))
//         );
//         assert_eq!(
//             parse_datetime_fast("2024-02-28"),
//             Some(create_datetime(2024, 2, 28, 0, 0, 0))
//         );
//     }
//
//     #[test]
//     fn test_fast_path_edge_cases() {
//         // Empty and very short strings
//         assert_eq!(parse_datetime_fast(""), None);
//         assert_eq!(parse_datetime_fast("a"), None);
//         assert_eq!(parse_datetime_fast("ab"), None);
//         assert_eq!(parse_datetime_fast("abc"), None);
//         assert_eq!(parse_datetime_fast("abcd"), None);
//
//         // Year 0000 - valid in chrono's proleptic Gregorian calendar (year 0 = 1 BCE)
//         assert_eq!(
//             parse_datetime_fast("0000-01-01"),
//             Some(create_datetime(0, 1, 1, 0, 0, 0))
//         );
//
//         // Whitespace - fast path does NOT trim (by design, for speed)
//         assert_eq!(parse_datetime_fast(" 2024-01-01"), None); // Wrong length (11)
//         assert_eq!(parse_datetime_fast("2024-01-01 "), None); // Wrong length (11)
//
//         // Tab as separator (not space or T)
//         assert_eq!(parse_datetime_fast("2024-01-15\t10:30:45"), None);
//
//         // Wrong length strings that look like dates
//         assert_eq!(parse_datetime_fast("2024-1-01"), None); // 9 chars, month not padded
//         assert_eq!(parse_datetime_fast("2024-01-1"), None); // 9 chars, day not padded
//
//         // Correct length but wrong format
//         assert_eq!(parse_datetime_fast("aaaa-bb-cc"), None);
//         assert_eq!(parse_datetime_fast("2024-01-01abc"), None); // 13 chars, no match
//
//         // Negative years - fast path doesn't handle (falls through to slow path)
//         // 11 chars "-2024-01-01" doesn't match any fast path pattern
//         assert_eq!(parse_datetime_fast("-2024-01-01"), None);
//
//         // Trailing garbage after fractional seconds - rejected like SQLite
//         assert_eq!(parse_datetime_fast("10:30:45.12abc"), None);
//         assert_eq!(parse_datetime_fast("2024-01-15 10:30:45.123xyz"), None);
//
//         // But valid fractional seconds still work
//         let dt = parse_datetime_fast("10:30:45.12").unwrap();
//         assert_eq!(dt.nanosecond(), 120000000); // .12 = 120ms
//     }
// }
