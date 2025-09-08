use std::net::{IpAddr, Ipv6Addr};

use ipnetwork::IpNetwork;
use turso_ext::{register_extension, scalar, ResultCode, Value};

#[cfg(feature = "u128-support")]
register_extension! {
    scalars: { ip_contains, ip_family, ip_host, ip_masklen, ip_network, ipv6_to_u128, u128_to_ipv6 },
}

#[cfg(not(feature = "u128-support"))]
register_extension! {
    scalars: { ip_contains, ip_family, ip_host, ip_masklen, ip_network },
}

#[cfg(feature = "u128-support")]
#[scalar(name = "ipv6_to_u128")]
fn ipv6_to_u128(args: &[Value]) -> Value {
    let Some(ip_text) = args.first().and_then(|v| v.to_text()) else {
        return Value::error(ResultCode::InvalidArgs);
    };

    match ip_text.parse::<IpAddr>() {
        Ok(IpAddr::V6(ipv6)) => Value::from_u128(u128::from(ipv6)),
        _ => Value::null(),
    }
}

#[cfg(feature = "u128-support")]
#[scalar(name = "u128_to_ipv6")]
fn u128_to_ipv6(args: &[Value]) -> Value {
    let Some(value) = args.first() else {
        return Value::error(ResultCode::InvalidArgs);
    };

    if let Some(num) = value.to_u128() {
        let ipv6 = Ipv6Addr::from(num);
        Value::from_text(ipv6.to_string())
    } else {
        Value::null()
    }
}

#[scalar(name = "ipcontains")]
fn ip_contains(args: &[Value]) -> Value {
    let Some(cidr_arg) = args.first().and_then(|v| v.to_text()) else {
        return Value::error(ResultCode::InvalidArgs);
    };

    let Ok(network) = cidr_arg.parse::<IpNetwork>() else {
        return Value::error(ResultCode::InvalidArgs);
    };

    let Some(ip_arg) = args.get(1).and_then(|v| v.to_text()) else {
        return Value::error(ResultCode::InvalidArgs);
    };

    let Ok(ip) = ip_arg.parse::<IpAddr>() else {
        return Value::error(ResultCode::InvalidArgs);
    };

    Value::from_integer(network.contains(ip) as i64)
}

#[scalar(name = "ipfamily")]
fn ip_family(args: &[Value]) -> Value {
    let Some(ip_addr) = args.first().and_then(|v| v.to_text()) else {
        return Value::error(ResultCode::InvalidArgs);
    };

    match ip_addr.parse::<IpAddr>() {
        Ok(IpAddr::V4(_)) => Value::from_integer(4),
        Ok(IpAddr::V6(_)) => Value::from_integer(6),
        Err(_) => Value::error(ResultCode::InvalidArgs),
    }
}

#[scalar(name = "iphost")]
fn ip_host(args: &[Value]) -> Value {
    let Some(cidr_arg) = args.first().and_then(|v| v.to_text()) else {
        return Value::error(ResultCode::InvalidArgs);
    };

    let Ok(network) = cidr_arg.parse::<IpNetwork>() else {
        return Value::error(ResultCode::InvalidArgs);
    };

    Value::from_text(network.ip().to_string())
}

#[scalar(name = "ipmasklen")]
fn ip_masklen(args: &[Value]) -> Value {
    let Some(cidr_arg) = args.first().and_then(|v| v.to_text()) else {
        return Value::error(ResultCode::InvalidArgs);
    };

    let Ok(network) = cidr_arg.parse::<IpNetwork>() else {
        return Value::error(ResultCode::InvalidArgs);
    };

    Value::from_integer(network.prefix() as i64)
}

#[scalar(name = "ipnetwork")]
fn ip_network(args: &[Value]) -> Value {
    let Some(cidr_arg) = args.first().and_then(|v| v.to_text()) else {
        return Value::error(ResultCode::InvalidArgs);
    };

    let Ok(network) = cidr_arg.parse::<IpNetwork>() else {
        return Value::error(ResultCode::InvalidArgs);
    };

    Value::from_text(format!("{}/{}", network.network(), network.prefix()))
}
