// Adapted from https://github.com/sqlite/sqlite/blob/master/ext/misc/decimal.c
use turso_ext::{register_extension, scalar, Value, ValueType};

#[derive(Clone)]
struct Decimal {
    sign: bool,
    is_null: bool,
    n_digit: usize,
    n_frac: usize,
    a: Vec<u8>,
}

register_extension! {
    scalars: {decimal_func, decimal_func_exp}
}

#[scalar(name = "decimal")]
fn decimal_func(args: &[Value]) -> Value {
    let mut d = match Decimal::decimal_new(&args[0], false) {
        Some(d) => d,
        None => return Value::null(),
    };

    let n = if args.len() == 2 {
        args[1].to_integer().unwrap_or(0).max(0) as usize
    } else {
        0
    };

    if n > 0 {
        d.decimal_round(n);
    }

    let result = d.decimal_result();
    match result {
        Some(s) => Value::from_text(s),
        None => Value::null(),
    }
}

#[scalar(name = "decimal_exp")]
fn decimal_func_exp(args: &[Value]) -> Value {
    let mut d = match Decimal::decimal_new(&args[0], false) {
        Some(d) => d,
        None => return Value::null(),
    };

    let n = if args.len() == 2 {
        args[1].to_integer().unwrap_or(0).max(0) as usize
    } else {
        0
    };

    if n > 0 {
        d.decimal_round(n);
    }

    let result = d.decimal_result_sci(n);

    match result {
        Some(s) => Value::from_text(s),
        None => Value::null(),
    }
}

impl Decimal {
    //create a decimal object from &str
    fn from_text(z_in: &str) -> Option<Self> {
        let chars: Vec<char> = z_in.chars().collect();
        let n = chars.len();
        let mut i = 0;
        let mut i_exp: i64 = 0;

        let mut p = Decimal {
            sign: false,
            is_null: false,
            n_digit: 0,
            n_frac: 0,
            a: Vec::with_capacity(n + 1),
        };

        while i < n && chars[i].is_ascii_whitespace() {
            i += 1;
        }

        if i < n && chars[i] == '-' {
            p.sign = true;
            i += 1;
        } else if i < n && chars[i] == '+' {
            i += 1;
        }

        while i < n && chars[i] == '0' {
            i += 1;
        }

        while i < n {
            let c = chars[i];

            if c.is_ascii_digit() {
                p.a.push((c as u8) - b'0');
                p.n_digit += 1;
            } else if c == '.' {
                p.n_frac = p.n_digit + 1;
            } else if c == 'e' || c == 'E' {
                let mut j = i + 1;
                let mut neg = false;

                if j >= n {
                    break;
                }

                if chars[j] == '-' {
                    neg = true;
                    j += 1;
                } else if chars[j] == '+' {
                    j += 1;
                }

                while j < n && i_exp < 1_000_000 {
                    if chars[j].is_ascii_digit() {
                        i_exp = i_exp * 10 + (chars[j] as i64) - ('0' as i64);
                    }
                    j += 1;
                }

                if neg {
                    i_exp = -i_exp;
                }
                break;
            }

            i += 1;
        }

        // right after .
        if p.n_frac > 0 {
            p.n_frac = p.n_digit - (p.n_frac - 1);
        }

        //exponent
        if i_exp > 0 {
            let i_exp = i_exp as usize;

            // move dot to the right
            if p.n_frac > 0 {
                if i_exp <= p.n_frac {
                    p.n_frac -= i_exp;
                } else {
                    let remaining = i_exp - p.n_frac;
                    p.n_frac = 0;
                    p.a.resize(p.n_digit + remaining, 0);
                    p.n_digit += remaining;
                }
            } else {
                p.a.resize(p.n_digit + i_exp, 0);
                p.n_digit += i_exp;
            }
        } else if i_exp < 0 {
            // move dot to the left
            let i_exp = (-i_exp) as usize;
            let n_extra = if p.n_digit > p.n_frac {
                p.n_digit - p.n_frac - 1
            } else {
                0
            };

            let mut remaining_exp = i_exp;

            if n_extra > 0 {
                if n_extra >= remaining_exp {
                    p.n_frac += remaining_exp;
                    remaining_exp = 0;
                } else {
                    remaining_exp -= n_extra;
                    p.n_frac = p.n_digit - 1;
                }
            }

            if remaining_exp > 0 {
                //add leading zeros
                let old_digits = p.a.clone();
                p.a = vec![0u8; remaining_exp]; // leading zeros
                p.a.extend_from_slice(&old_digits[..p.n_digit]);
                p.n_digit += remaining_exp;
                p.n_frac += remaining_exp;
            }
        }

        // "-0" to "0"
        if p.sign {
            let all_zero = p.a[..p.n_digit].iter().all(|&d| d == 0);
            if all_zero {
                p.sign = false;
            }
        }

        Some(p)
    }

    fn from_double(r: f64) -> Option<Self> {
        let mut r = r;
        let is_neg;

        if r < 0.0 {
            is_neg = true;
            r = -r;
        } else {
            is_neg = false;
        }
        // as raw bits IEEE 754
        let a = r.to_bits() as i64;

        let (m, e): (i64, i32);

        if a == 0 || a == (0x8000000000000000u64 as i64) {
            e = 0;
            m = 0;
        } else {
            // sign = [63]
            // exponent = [62-52]
            // mantissa = [51-0]
            let mut exp = (a >> 52) as i32; // eksponent biased
            let mut man = a & ((1i64 << 52) - 1); // mantissa without implicit bit

            if exp == 0 {
                //subnormal
                man <<= 1;
            } else {
                //nomarl
                man |= 1i64 << 52;
            }

            // delete trailing zeros from mantissa
            while exp < 1075 && man > 0 && (man & 1) == 0 {
                man >>= 1;
                exp += 1;
            }

            if is_neg {
                man = -man;
            }
            //change eksponent from biased to actual
            exp -= 1075;
            if exp > 971 {
                return None;
            }

            m = man;
            e = exp;
        }

        let z_num = m.to_string();
        let mut p_a = Decimal::from_text(&z_num)?;
        let p_x = Decimal::pow2(e)?;
        p_a.mul(&p_x);

        Some(p_a)
    }

    fn pow2(n: i32) -> Option<Self> {
        if !(-20000..=20000).contains(&n) {
            return None;
        }

        let mut p_a = Decimal::from_text("1.0")?;

        if n == 0 {
            return Some(p_a);
        }

        let mut n = n;
        let mut p_x = if n > 0 {
            Decimal::from_text("2.0")?
        } else {
            n = -n;
            Decimal::from_text("0.5")?
        };

        loop {
            // rightmost bit
            if n & 1 == 1 {
                p_a.mul(&p_x);
            }

            n >>= 1;
            if n == 0 {
                break;
            }

            let p_x_clone = p_x.clone();
            p_x.mul(&p_x_clone);
        }

        Some(p_a)
    }

    fn mul(&mut self, p_b: &Decimal) {
        if self.is_null || p_b.is_null {
            return;
        }

        let acc_len = self.n_digit + p_b.n_digit + 2;
        let mut acc = vec![0i32; acc_len];

        let min_frac = self.n_frac.min(p_b.n_frac);

        for i in (0..self.n_digit).rev() {
            let f = self.a[i] as i32;
            let mut carry = 0i32;

            for j in (0..p_b.n_digit).rev() {
                let k = i + j + 3;
                let x = acc[k] + f * (p_b.a[j] as i32) + carry;
                acc[k] = x % 10;
                carry = x / 10;
            }

            let k = i + 2;
            let x = acc[k] + carry;
            acc[k] = x % 10;
            acc[k - 1] += x / 10;
        }

        self.a = acc.iter().map(|&x| x as u8).collect();
        self.n_digit += p_b.n_digit + 2;
        self.n_frac += p_b.n_frac;
        self.sign ^= p_b.sign;

        while self.n_frac > min_frac && self.n_digit > 0 && self.a[self.n_digit - 1] == 0 {
            self.n_frac -= 1;
            self.n_digit -= 1;
        }
    }

    fn decimal_result(&self) -> Option<String> {
        if self.is_null {
            return None;
        }

        let mut z = String::with_capacity(self.n_digit + 4);

        let is_zero = self.n_digit == 0 || (self.n_digit == 1 && self.a[0] == 0);
        let sign = self.sign && !is_zero;

        if sign {
            z.push('-');
        }

        let n_integer = self.n_digit as isize - self.n_frac as isize;

        if n_integer <= 0 {
            z.push('0');
        }

        // skip leading zeros (at least one digit stil written)
        let mut j = 0usize;
        let mut n = n_integer;

        while n > 1 && j < self.n_digit && self.a[j] == 0 {
            j += 1;
            n -= 1;
        }

        while n > 0 {
            if j < self.n_digit {
                z.push((b'0' + self.a[j]) as char);
                j += 1;
            }
            n -= 1;
        }

        if self.n_frac > 0 {
            z.push('.');
            while j < self.n_digit {
                z.push((b'0' + self.a[j]) as char);
                j += 1;
            }
        }

        Some(z)
    }

    fn decimal_new(arg: &Value, b_text_only: bool) -> Option<Decimal> {
        match arg.value_type() {
            ValueType::Text | ValueType::Integer => Decimal::from_text(arg.to_text()?),
            ValueType::Float if b_text_only => Decimal::from_text(arg.to_text()?),
            ValueType::Blob if b_text_only => Decimal::from_text(arg.to_text()?),
            ValueType::Float => Decimal::from_double(arg.to_float()?),
            ValueType::Blob => {
                let bytes = arg.to_blob()?;
                //IEEE-754 double -> blob with 8 byte only
                if bytes.len() != 8 {
                    return None;
                }
                let v = bytes.iter().fold(0u64, |acc, &b| (acc << 8) | (b as u64));
                Decimal::from_double(f64::from_bits(v))
            }
            _ => None,
        }
    }
    fn decimal_round(&mut self, n: usize) {
        if n < 1 {
            return;
        }
        let n_zero = self.a[..self.n_digit]
            .iter()
            .take_while(|&&d| d == 0)
            .count();

        let n = n + n_zero;

        if self.n_digit <= n {
            return;
        }

        if self.a[n] > 4 {
            self.a[n - 1] += 1;

            let mut i = n - 1;
            while i > 0 && self.a[i] > 9 {
                self.a[i] = 0;
                self.a[i - 1] += 1;
                i -= 1;
            }

            if self.a[0] > 9 {
                self.a[0] = 0;
                self.a.insert(0, 1);
                self.n_digit += 1;
                if self.n_frac > 0 {
                    self.n_frac -= 1;
                }
            }
        }

        for d in &mut self.a[n..self.n_digit] {
            *d = 0;
        }
    }
    fn decimal_result_sci(&self, n: usize) -> Option<String> {
        if self.is_null {
            return None;
        }

        let n = if n < 1 { 0 } else { n };

        let mut n_digit = self.n_digit;
        while n_digit > n && n_digit > 0 && self.a[n_digit - 1] == 0 {
            n_digit -= 1;
        }

        let n_zero = self.a[..n_digit].iter().take_while(|&&d| d == 0).count();

        let n_frac = self.n_frac as isize + (n_digit as isize - self.n_digit as isize);

        let n_digit = n_digit - n_zero;

        let (a, n_digit, n_frac): (&[u8], usize, isize) = if n_digit == 0 {
            (&[0u8][..], 1, 0)
        } else {
            (&self.a[n_zero..self.n_digit], n_digit, n_frac)
        };

        let mut z = String::with_capacity(n_digit + 20);
        if self.sign && n_digit > 0 && !(n_digit == 1 && a[0] == 0) {
            z.push('-');
        } else {
            z.push('+');
        }

        z.push((b'0' + a[0]) as char);
        z.push('.');

        if n_digit == 1 {
            z.push('0');
        } else {
            for digit in a.iter().take(n_digit).skip(1) {
                z.push((b'0' + *digit) as char);
            }
        }

        // exp = position of the decimal point relative to the first digit
        //"314.15" → a=[3,1,4,1,5], nFrac=2
        //   exp = 5 - 2 - 1 = 2  → "+3.1415e+02"
        //
        //"0.00123" → a=[1,2,3], nFrac=5
        //   exp = 3 - 5 - 1 = -3 → "+1.23e-03" ✓=
        // This implementation return +1.23e+06 instead of +1.23e+006 in https://github.com/sqlite/sqlite/blob/master/test/decimal.test
        let exp = n_digit as isize - n_frac - 1;
        if exp >= 0 {
            z.push_str(&format!("e+{exp:02}"));
        } else {
            z.push_str(&format!("e-{:02}", -exp));
        }

        Some(z)
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_text_and_result() {
        let d = Decimal::from_text("-123.45e2").expect("must be not None");
        assert!(d.sign);
        assert_eq!(d.n_digit, 5);
        assert_eq!(d.n_frac, 0);
        assert_eq!(d.a, vec![1, 2, 3, 4, 5]);
        assert_eq!(d.decimal_result(), Some("-12345".to_string()));

        let dd = Decimal::from_text("0.00123").unwrap();
        assert!(!dd.sign);
        assert_eq!(dd.n_digit, 5);
        assert_eq!(dd.n_frac, 5);
        assert_eq!(dd.a, vec![0, 0, 1, 2, 3]);
        assert_eq!(dd.decimal_result(), Some("0.00123".to_string()));
    }

    #[test]
    fn test_decimal() {
        // 1000
        assert_eq!(
            Decimal::from_text("1")
                .expect("'1' is a valid decimal")
                .decimal_result(),
            Some("1".to_string())
        );

        // 1001
        assert_eq!(
            Decimal::from_text("+0")
                .expect("'+0' is a valid decimal")
                .decimal_result(),
            Some("0".to_string())
        );
        assert_eq!(
            Decimal::from_text("-0")
                .expect("'-0' is a valid decimal")
                .decimal_result(),
            Some("0".to_string())
        );

        // 1010
        assert_eq!(
            Decimal::from_text("1.0")
                .expect("'1.0' is a valid decimal")
                .decimal_result(),
            Some("1.0".to_string())
        );

        // 1020
        assert_eq!(
            Decimal::from_text("0001.0")
                .expect("'0001.0' is a valid decimal")
                .decimal_result(),
            Some("1.0".to_string())
        );

        // 1030
        assert_eq!(
            Decimal::from_text("+0001.0")
                .expect("'+0001.0' is a valid decimal")
                .decimal_result(),
            Some("1.0".to_string())
        );

        // 1040
        assert_eq!(
            Decimal::from_text("-0001.0")
                .expect("'-0001.0' is a valid decimal")
                .decimal_result(),
            Some("-1.0".to_string())
        );

        // 1041
        assert_eq!(
            Decimal::from_text("-0000.0")
                .expect("'-0000.0' is a valid decimal")
                .decimal_result(),
            Some("0.0".to_string())
        );

        // 1042
        assert_eq!(
            Decimal::from_double(0.0)
                .expect("this is a valid decimal")
                .decimal_result(),
            Decimal::from_double(-0.0)
                .expect("this is a valid decimal")
                .decimal_result()
        );

        // 1050
        assert_eq!(
            Decimal::from_text("1.0e72")
                .expect("'1.0e72' is a valid decimal")
                .decimal_result(),
            Some(
                "1000000000000000000000000000000000000000000000000000000000000000000000000"
                    .to_string()
            )
        );

        // 1060
        assert_eq!(
            Decimal::from_text("1.0e-72")
                .expect("'1.0e-72' is a valid decimal")
                .decimal_result(),
            Some(
                "0.0000000000000000000000000000000000000000000000000000000000000000000000010"
                    .to_string()
            )
        );

        // 1070
        assert_eq!(
            Decimal::from_text("-123e-4")
                .expect("'-123e-4' is a valid decimal")
                .decimal_result(),
            Some("-0.0123".to_string())
        );

        // 1080
        assert_eq!(
            Decimal::from_text("+123e+4")
                .expect("'+123e+4' is a valid decimal")
                .decimal_result(),
            Some("1230000".to_string())
        );

        // 1081
        assert_eq!(
            Decimal::from_text("+123e+4")
                .expect("'+123e+4' is a valid decimal")
                .decimal_result_sci(0),
            Some("+1.23e+06".to_string())
        );
    }

    #[test]
    fn test_decimal_round() {
        let mut d = Decimal::from_text("999").expect("must be not None");
        assert_eq!(d.n_frac, 0);
        d.decimal_round(1);
        assert_eq!(d.decimal_result(), Some("1000".to_string()));
    }

    #[test]
    fn test_result_sci_n_param() {
        let d = Decimal::from_text("3.14000").expect("'3.14000' is valid decimal string");
        assert_eq!(d.decimal_result_sci(0), Some("+3.14e+00".to_string()));

        let d2 = Decimal::from_text("3.14000").expect("'3.14000' is valid decimal string");
        assert_eq!(d2.decimal_result_sci(5), Some("+3.1400e+00".to_string()));
    }

    #[test]
    fn test_from_double() {
        assert_eq!(
            Decimal::from_double(0.99)
                .expect("0.99 is a valid f64")
                .decimal_result(),
            Some("0.9899999999999999911182158029987476766109466552734375".to_string())
        );
        assert_eq!(
            Decimal::from_double(1.0)
                .expect("1.0 is a valid f64")
                .decimal_result(),
            Some("1".to_string())
        );
        assert_eq!(
            Decimal::from_double(-1.0)
                .expect("-1.0 is a valid f64")
                .decimal_result(),
            Some("-1".to_string())
        );
        assert_eq!(
            Decimal::from_double(0.0)
                .expect("0.0 is a valid f64")
                .decimal_result(),
            Some("0".to_string())
        );
        assert_eq!(
            Decimal::from_double(-0.0)
                .expect("-0.0 is a valid f64")
                .decimal_result(),
            Some("0".to_string())
        );
        assert_eq!(
            Decimal::from_double(0.5)
                .expect("0.5 is a valid f64")
                .decimal_result(),
            Some("0.5".to_string())
        );
        assert_eq!(
            Decimal::from_double(0.25)
                .expect("0.25 is a valid f64")
                .decimal_result(),
            Some("0.25".to_string())
        );
        assert!(Decimal::from_double(f64::NAN).is_none());
        assert!(Decimal::from_double(f64::INFINITY).is_none());
        assert!(Decimal::from_double(f64::NEG_INFINITY).is_none());
    }
}
