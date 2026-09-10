use std::fmt::Debug;
use std::net::Ipv6Addr;

/// Monotonic maps a value to u128 value space
/// Monotonic mapping enables `PartialOrd` on u128 space without conversion to original space.
pub trait MonotonicallyMappableToU128: 'static + PartialOrd + Copy + Debug + Send + Sync {
    /// Converts a value to u128.
    ///
    /// Internally all fast field values are encoded as u64.
    fn to_u128(self) -> u128;

    /// Converts a value from u128
    ///
    /// Internally all fast field values are encoded as u64.
    /// **Note: To be used for converting encoded Term, Posting values.**
    fn from_u128(val: u128) -> Self;
}

impl MonotonicallyMappableToU128 for u128 {
    fn to_u128(self) -> u128 {
        self
    }

    fn from_u128(val: u128) -> Self {
        val
    }
}

impl MonotonicallyMappableToU128 for Ipv6Addr {
    fn to_u128(self) -> u128 {
        ip_to_u128(self)
    }

    fn from_u128(val: u128) -> Self {
        Ipv6Addr::from(val.to_be_bytes())
    }
}

fn ip_to_u128(ip_addr: Ipv6Addr) -> u128 {
    u128::from_be_bytes(ip_addr.octets())
}
