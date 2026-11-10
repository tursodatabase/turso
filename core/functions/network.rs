//! Input functions of the PostgreSQL network address types: inet, cidr,
//! macaddr and macaddr8.
//!
//! Each function checks a text value with PostgreSQL's input rules and
//! returns the text that PostgreSQL prints for it, so the stored value is the
//! one PostgreSQL would show. The parsers follow `network.c`, `inet_net_pton.c`,
//! `mac.c` and `mac8.c` of PostgreSQL.

use std::net::{Ipv4Addr, Ipv6Addr};

use crate::types::Value;
use crate::{LimboError, Result};

/// `validate_ipaddr(value)`: the ENCODE of the inet type.
pub fn exec_inet_in(value: &Value) -> Result<Value> {
    encode_text(value, "inet", parse_inet)
}

/// `validate_cidr(value)`: the ENCODE of the cidr type.
pub fn exec_cidr_in(value: &Value) -> Result<Value> {
    encode_text(value, "cidr", parse_cidr)
}

/// `validate_macaddr(value)`: the ENCODE of the macaddr type.
pub fn exec_macaddr_in(value: &Value) -> Result<Value> {
    encode_text(value, "macaddr", parse_macaddr)
}

/// `validate_macaddr8(value)`: the ENCODE of the macaddr8 type.
pub fn exec_macaddr8_in(value: &Value) -> Result<Value> {
    encode_text(value, "macaddr8", parse_macaddr8)
}

fn encode_text(
    value: &Value,
    type_name: &'static str,
    parse: fn(&str) -> Result<String>,
) -> Result<Value> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Text(t) => Ok(Value::build_text(parse(t.as_str())?)),
        other => Err(invalid_syntax(type_name, &other.to_string())),
    }
}

/// The text PostgreSQL prints for an inet value, or the error it gives.
pub fn parse_inet(input: &str) -> Result<String> {
    let address = parse_address(input, false)?;
    Ok(address.to_inet_text())
}

/// The text PostgreSQL prints for a cidr value, or the error it gives. A cidr
/// value has no bits set to the right of its netmask.
pub fn parse_cidr(input: &str) -> Result<String> {
    let address = parse_address(input, true)?;
    if !address.host_bits_are_zero() {
        return Err(LimboError::InvalidArgument(format!(
            "invalid cidr value: \"{input}\""
        )));
    }
    Ok(address.to_cidr_text())
}

/// The text PostgreSQL prints for a macaddr value, or the error it gives.
/// PostgreSQL reads the value with `sscanf` and tries these formats in order.
pub fn parse_macaddr(input: &str) -> Result<String> {
    use MacPart::{Hex, Sep, TwoHex};
    #[rustfmt::skip]
    const FORMATS: [&[MacPart]; 7] = [
        &[Hex, Sep(':'), Hex, Sep(':'), Hex, Sep(':'), Hex, Sep(':'), Hex, Sep(':'), Hex],
        &[Hex, Sep('-'), Hex, Sep('-'), Hex, Sep('-'), Hex, Sep('-'), Hex, Sep('-'), Hex],
        &[TwoHex, TwoHex, TwoHex, Sep(':'), TwoHex, TwoHex, TwoHex],
        &[TwoHex, TwoHex, TwoHex, Sep('-'), TwoHex, TwoHex, TwoHex],
        &[TwoHex, TwoHex, Sep('.'), TwoHex, TwoHex, Sep('.'), TwoHex, TwoHex],
        &[TwoHex, TwoHex, Sep('-'), TwoHex, TwoHex, Sep('-'), TwoHex, TwoHex],
        &[TwoHex, TwoHex, TwoHex, TwoHex, TwoHex, TwoHex],
    ];
    let octets = FORMATS
        .iter()
        .find_map(|format| scan_macaddr(input, format))
        .ok_or_else(|| invalid_syntax("macaddr", input))?;
    if octets.iter().any(|&octet| octet > 255) {
        return Err(LimboError::InvalidArgument(format!(
            "invalid octet value in \"macaddr\" value: \"{input}\""
        )));
    }
    Ok(format_mac(octets.iter().map(|&octet| octet as u8)))
}

/// The text PostgreSQL prints for a macaddr8 value, or the error it gives. A
/// six byte address is turned into an eight byte one by putting FF:FE in the
/// middle, as PostgreSQL does.
pub fn parse_macaddr8(input: &str) -> Result<String> {
    let invalid = || invalid_syntax("macaddr8", input);
    let bytes = input.as_bytes();
    let mut pos = bytes
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let mut octets = Vec::with_capacity(8);
    let mut spacer = None;
    // Like PostgreSQL, a single character left at the end is ignored.
    while pos + 1 < bytes.len() {
        if octets.len() == 8 {
            return Err(invalid());
        }
        let high = hex_digit(bytes[pos]).ok_or_else(invalid)?;
        let low = hex_digit(bytes[pos + 1]).ok_or_else(invalid)?;
        octets.push(high << 4 | low);
        pos += 2;
        if let Some(&b @ (b':' | b'-' | b'.')) = bytes.get(pos) {
            if *spacer.get_or_insert(b) != b {
                return Err(invalid());
            }
            pos += 1;
        }
        if (octets.len() == 6 || octets.len() == 8)
            && bytes.get(pos).is_some_and(u8::is_ascii_whitespace)
        {
            if bytes[pos..].iter().any(|b| !b.is_ascii_whitespace()) {
                return Err(invalid());
            }
            pos = bytes.len();
        }
    }
    match octets.len() {
        6 => {
            octets.splice(3..3, [0xff, 0xfe]);
        }
        8 => {}
        _ => return Err(invalid()),
    }
    Ok(format_mac(octets.into_iter()))
}

fn invalid_syntax(type_name: &'static str, input: &str) -> LimboError {
    LimboError::InvalidTextRepresentation {
        type_name,
        input: input.to_string(),
    }
}

/// An IPv4 or IPv6 address with the length of its netmask.
struct NetAddress {
    octets: Vec<u8>,
    bits: u8,
}

impl NetAddress {
    fn max_bits(&self) -> u8 {
        (self.octets.len() * 8) as u8
    }

    fn host_bits_are_zero(&self) -> bool {
        self.octets.iter().enumerate().all(|(i, &octet)| {
            let network_bits = (self.bits as usize).saturating_sub(i * 8).min(8);
            octet & (0xffu8.checked_shr(network_bits as u32).unwrap_or(0)) == 0
        })
    }

    /// inet leaves out the netmask when it covers the whole address.
    fn to_inet_text(&self) -> String {
        let address = self.address_text();
        if self.bits == self.max_bits() {
            address
        } else {
            format!("{address}/{}", self.bits)
        }
    }

    fn to_cidr_text(&self) -> String {
        format!("{}/{}", self.address_text(), self.bits)
    }

    fn address_text(&self) -> String {
        match <[u8; 4]>::try_from(self.octets.as_slice()) {
            Ok(v4) => Ipv4Addr::from(v4).to_string(),
            Err(_) => {
                let v6: [u8; 16] = self.octets.as_slice().try_into().expect("16 octets");
                ipv6_text(&Ipv6Addr::from(v6))
            }
        }
    }
}

/// Reads an inet or cidr value. PostgreSQL takes a value with a colon as IPv6
/// and any other value as IPv4.
fn parse_address(input: &str, is_cidr: bool) -> Result<NetAddress> {
    let type_name = if is_cidr { "cidr" } else { "inet" };
    let parsed = if input.contains(':') {
        parse_ipv6(input)
    } else if is_cidr {
        parse_ipv4_cidr(input)
    } else {
        parse_ipv4_inet(input)
    };
    parsed.ok_or_else(|| invalid_syntax(type_name, input))
}

/// An IPv6 address with an optional `/bits`. The bits have no leading zero.
fn parse_ipv6(input: &str) -> Option<NetAddress> {
    let (address, bits) = match input.split_once('/') {
        Some((address, bits)) => (address, Some(bits)),
        None => (input, None),
    };
    let address: Ipv6Addr = address.parse().ok()?;
    let bits = match bits {
        None => 128,
        Some(bits) => {
            if bits.is_empty()
                || !bits.bytes().all(|b| b.is_ascii_digit())
                || (bits.len() > 1 && bits.starts_with('0'))
            {
                return None;
            }
            bits.parse::<u8>().ok().filter(|&bits| bits <= 128)?
        }
    };
    Some(NetAddress {
        octets: address.octets().to_vec(),
        bits,
    })
}

/// An IPv4 inet value: one to four decimal octets and an optional `/bits`.
/// Without bits all four octets are needed; with bits, the given octets must
/// cover the netmask.
fn parse_ipv4_inet(input: &str) -> Option<NetAddress> {
    let (address, bits) = split_ipv4_bits(input)?;
    // PostgreSQL's inet input allows one dot after the last octet.
    let address = address.strip_suffix('.').unwrap_or(address);
    let mut octets = parse_decimal_octets(address)?;
    let bits = match bits {
        None if octets.len() == 4 => 32,
        None => return None,
        Some(bits) if bits as usize / 8 > octets.len() => return None,
        Some(bits) => bits,
    };
    octets.resize(4, 0);
    Some(NetAddress { octets, bits })
}

/// An IPv4 cidr value: decimal octets, or a `0x` hex string, and an optional
/// `/bits`. Without bits the netmask comes from the address class, as in
/// PostgreSQL.
fn parse_ipv4_cidr(input: &str) -> Option<NetAddress> {
    let (address, bits) = split_ipv4_bits(input)?;
    let mut octets = match address
        .strip_prefix("0x")
        .or_else(|| address.strip_prefix("0X"))
    {
        Some(hex) => parse_hex_octets(hex)?,
        None => parse_decimal_octets(address)?,
    };
    let bits = bits.unwrap_or_else(|| {
        let class_bits = match octets[0] {
            240.. => 32,
            224.. => 8,
            192.. => 24,
            128.. => 16,
            _ => 8,
        };
        match class_bits.max(octets.len() as u8 * 8) {
            8 if octets[0] == 224 => 4,
            bits => bits,
        }
    });
    octets.resize(4, 0);
    Some(NetAddress { octets, bits })
}

/// Splits `address/bits`. The bits are decimal and at most 32.
fn split_ipv4_bits(input: &str) -> Option<(&str, Option<u8>)> {
    let Some((address, bits)) = input.split_once('/') else {
        return Some((input, None));
    };
    let bits = parse_decimal_at_most(bits, 32)?;
    Some((address, Some(bits as u8)))
}

/// One to four dot-separated decimal octets, each at most 255.
fn parse_decimal_octets(address: &str) -> Option<Vec<u8>> {
    let octets = address
        .split('.')
        .map(|part| parse_decimal_at_most(part, 255).map(|octet| octet as u8))
        .collect::<Option<Vec<u8>>>()?;
    (octets.len() <= 4).then_some(octets)
}

/// A non-empty string of decimal digits whose value is at most `max`.
fn parse_decimal_at_most(digits: &str, max: u32) -> Option<u32> {
    if digits.is_empty() {
        return None;
    }
    digits.bytes().try_fold(0u32, |acc, b| {
        b.is_ascii_digit()
            .then(|| acc * 10 + u32::from(b - b'0'))
            .filter(|&value| value <= max)
    })
}

/// A string of hex digits, two per octet; an odd last digit is the high half
/// of its octet.
fn parse_hex_octets(hex: &str) -> Option<Vec<u8>> {
    if hex.is_empty() || hex.len() > 8 {
        return None;
    }
    hex.as_bytes()
        .chunks(2)
        .map(|pair| {
            let high = hex_digit(pair[0])?;
            let low = match pair.get(1) {
                Some(&digit) => hex_digit(digit)?,
                None => 0,
            };
            Some(high << 4 | low)
        })
        .collect()
}

/// PostgreSQL's text for an IPv6 address: the longest run of two or more zero
/// groups becomes `::`, and an address that starts with 96 zero bits, or with
/// 80 zero bits and ffff, ends in dotted IPv4.
fn ipv6_text(address: &Ipv6Addr) -> String {
    let groups = address.segments();
    let mut best = (0, 0);
    let mut i = 0;
    while i < groups.len() {
        if groups[i] != 0 {
            i += 1;
            continue;
        }
        let start = i;
        while i < groups.len() && groups[i] == 0 {
            i += 1;
        }
        if i - start > best.1 {
            best = (start, i - start);
        }
    }
    if best.1 < 2 {
        best = (0, 0);
    }
    let (zeros_start, zeros_len) = best;
    let ends_in_ipv4 =
        zeros_start == 0 && (zeros_len == 6 || (zeros_len == 5 && groups[5] == 0xffff));

    let mut text = String::new();
    let mut i = 0;
    while i < groups.len() {
        if zeros_len > 0 && i == zeros_start {
            text.push(':');
            i += zeros_len;
            if i == groups.len() {
                text.push(':');
            }
            continue;
        }
        if i != 0 {
            text.push(':');
        }
        if i == 6 && ends_in_ipv4 {
            let octets = address.octets();
            text.push_str(
                &Ipv4Addr::new(octets[12], octets[13], octets[14], octets[15]).to_string(),
            );
            break;
        }
        text.push_str(&format!("{:x}", groups[i]));
        i += 1;
    }
    text
}

#[derive(Clone, Copy)]
enum MacPart {
    /// `%x`: a hex number of any length.
    Hex,
    /// `%2x`: a hex number of one or two digits.
    TwoHex,
    /// A separator that must be there.
    Sep(char),
}

/// The six numbers of `input` read with one `sscanf` format of PostgreSQL's
/// macaddr input, or None when the format does not match. As in `sscanf`,
/// white space before each number and at the end is allowed.
fn scan_macaddr(input: &str, format: &[MacPart]) -> Option<[u64; 6]> {
    let bytes = input.as_bytes();
    let mut pos = 0;
    let mut octets = [0u64; 6];
    let mut count = 0;
    for part in format {
        match *part {
            MacPart::Sep(sep) => {
                if bytes.get(pos) != Some(&(sep as u8)) {
                    return None;
                }
                pos += 1;
            }
            MacPart::Hex | MacPart::TwoHex => {
                let (value, end) = scan_hex(bytes, pos, matches!(part, MacPart::TwoHex))?;
                octets[count] = value;
                count += 1;
                pos = end;
            }
        }
    }
    bytes[pos..]
        .iter()
        .all(u8::is_ascii_whitespace)
        .then_some(octets)
}

/// One `%x` (or `%2x` when `two_digits`) of `sscanf` at `pos`: white space,
/// an optional sign, an optional `0x` for `%x`, and hex digits. A negative
/// number wraps around like the `unsigned int` of `sscanf`, so it is too big
/// for an octet. Returns the number and the position after it.
fn scan_hex(bytes: &[u8], mut pos: usize, two_digits: bool) -> Option<(u64, usize)> {
    while bytes.get(pos).is_some_and(u8::is_ascii_whitespace) {
        pos += 1;
    }
    let mut max_digits = if two_digits { 2 } else { usize::MAX };
    let negative = bytes.get(pos) == Some(&b'-');
    if matches!(bytes.get(pos), Some(b'+' | b'-')) {
        pos += 1;
        max_digits -= 1;
    }
    if !two_digits
        && bytes.get(pos) == Some(&b'0')
        && matches!(bytes.get(pos + 1), Some(b'x' | b'X'))
        && bytes.get(pos + 2).is_some_and(u8::is_ascii_hexdigit)
    {
        pos += 2;
    }
    let digits = bytes[pos..]
        .iter()
        .take(max_digits)
        .take_while(|b| b.is_ascii_hexdigit())
        .count();
    if digits == 0 {
        return None;
    }
    let value = bytes[pos..pos + digits].iter().fold(0u64, |acc, &b| {
        acc.saturating_mul(16)
            .saturating_add(u64::from(hex_digit(b).expect("hex digit")))
    });
    let value = if negative && value != 0 {
        u64::MAX
    } else {
        value
    };
    Some((value, pos + digits))
}

fn hex_digit(b: u8) -> Option<u8> {
    (b as char).to_digit(16).map(|digit| digit as u8)
}

fn format_mac(octets: impl Iterator<Item = u8>) -> String {
    octets
        .map(|octet| format!("{octet:02x}"))
        .collect::<Vec<_>>()
        .join(":")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inet_keeps_the_netmask_and_drops_a_full_one() {
        for (input, expected) in [
            ("192.168.1.5/24", "192.168.1.5/24"),
            ("192.168.1.5/32", "192.168.1.5"),
            ("10/8", "10.0.0.0/8"),
            ("010.001.0.1", "10.1.0.1"),
            ("1.2.3.4/08", "1.2.3.4/8"),
            ("1.2.3.4.", "1.2.3.4"),
            ("1.2.3./24", "1.2.3.0/24"),
            ("::1/128", "::1"),
            ("0:0:0:0:0:0:0:1", "::1"),
            ("2001:DB8::/32", "2001:db8::/32"),
            ("::ffff:1.2.3.4", "::ffff:1.2.3.4"),
            ("::ffff:0", "::255.255.0.0"),
            ("::1.2.3.4", "::1.2.3.4"),
            ("1:0:0:1:0:0:1:1", "1::1:0:0:1:1"),
            ("0:0:1::", "0:0:1::"),
        ] {
            assert_eq!(parse_inet(input).unwrap(), expected, "{input}");
        }
        for input in [
            "",
            "garbage",
            " 1.2.3.4",
            "1.2.3",
            "1.2.3/32",
            "1.2.3.4/33",
            "1.2.3.4/",
            "/8",
            "256.1.1.1",
            "1.2.3.4.5",
            "::1/08",
            "::1/129",
            "fe80::1%eth0",
        ] {
            assert!(parse_inet(input).is_err(), "{input}");
        }
    }

    #[test]
    fn cidr_takes_the_netmask_from_the_address_class() {
        for (input, expected) in [
            ("10", "10.0.0.0/8"),
            ("192.168.1", "192.168.1.0/24"),
            ("224", "224.0.0.0/4"),
            ("224.1", "224.1.0.0/16"),
            ("240", "240.0.0.0/32"),
            ("1.2/32", "1.2.0.0/32"),
            ("1.2.3.4", "1.2.3.4/32"),
            ("0x0a", "10.0.0.0/8"),
            ("0xa", "160.0.0.0/16"),
            ("::1", "::1/128"),
            ("2001:db8::/32", "2001:db8::/32"),
        ] {
            assert_eq!(parse_cidr(input).unwrap(), expected, "{input}");
        }
        for input in [
            "garbage",
            "192.168.1.5/24",
            "10.1/8",
            "0x",
            "0x0a.1",
            "::1/64",
            "1.2.3.4.",
        ] {
            assert!(parse_cidr(input).is_err(), "{input}");
        }
    }

    #[test]
    fn macaddr_accepts_postgres_formats() {
        for input in [
            "08:00:2b:01:02:03",
            "08-00-2b-01-02-03",
            "08002b:010203",
            "08002b-010203",
            "0800.2b01.0203",
            "0800-2b01-0203",
            "08002b010203",
            " 08:00:2B:01:02:03 ",
        ] {
            assert_eq!(
                parse_macaddr(input).unwrap(),
                "08:00:2b:01:02:03",
                "{input}"
            );
        }
        for input in [
            "1:2:3:4:5:6",
            "0x1:2:3:4:5:6",
            "+1:2:3:4:5:6",
            "1: 2:3:4:5:6",
        ] {
            assert_eq!(
                parse_macaddr(input).unwrap(),
                "01:02:03:04:05:06",
                "{input}"
            );
        }
        assert!(parse_macaddr("garbage").is_err());
        assert!(parse_macaddr("1 :2:3:4:5:6").is_err());
        assert!(matches!(
            parse_macaddr("-1:2:3:4:5:6"),
            Err(LimboError::InvalidArgument(_))
        ));
        assert!(matches!(
            parse_macaddr("1:2:3:4:5:300"),
            Err(LimboError::InvalidArgument(_))
        ));
    }

    #[test]
    fn macaddr8_turns_six_bytes_into_eight() {
        for (input, expected) in [
            ("08:00:2b:01:02:03:04:05", "08:00:2b:01:02:03:04:05"),
            ("0800.2b01.0203.0405", "08:00:2b:01:02:03:04:05"),
            ("08:00:2b:01:02:03", "08:00:2b:ff:fe:01:02:03"),
            ("08002b0102030", "08:00:2b:ff:fe:01:02:03"),
            (" 08:00:2b:01:02:03 ", "08:00:2b:ff:fe:01:02:03"),
        ] {
            assert_eq!(parse_macaddr8(input).unwrap(), expected, "{input}");
        }
        for input in [
            "garbage",
            "08:00:2b:01:02:0g",
            "08:00-2b:01:02:03",
            "08:00:2b:01:02",
        ] {
            assert!(parse_macaddr8(input).is_err(), "{input}");
        }
    }
}
