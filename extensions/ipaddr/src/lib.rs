use std::net::IpAddr;

use ipnetwork::IpNetwork;
use turso_ext::{register_extension, scalar, ResultCode, Value};

register_extension! {
    scalars: { ip_contains, ip_family, ip_host, ip_masklen, ip_network },
}

#[scalar(name = "ipcontains")]
fn ip_contains(args: &[Value]) -> Value {
    let Some(cidr_arg) = args[0].to_text() else {
        return Value::error(ResultCode::InvalidArgs);
    };

    let Ok(network) = cidr_arg.parse::<IpNetwork>() else {
        return Value::error(ResultCode::InvalidArgs);
    };

    let Some(ip_arg) = args[1].to_text() else {
        return Value::error(ResultCode::InvalidArgs);
    };

    let Ok(ip) = ip_arg.parse::<IpAddr>() else {
        return Value::error(ResultCode::InvalidArgs);
    };

    Value::from_integer(network.contains(ip) as i64)
}

#[scalar(name = "ipfamily")]
fn ip_family(args: &[Value]) -> Value {
    let Some(ip_addr) = args[0].to_text() else {
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
    let Some(cidr_arg) = args[0].to_text() else {
        return Value::error(ResultCode::InvalidArgs);
    };

    let Ok(network) = cidr_arg.parse::<IpNetwork>() else {
        return Value::error(ResultCode::InvalidArgs);
    };

    return Value::from_text(network.ip().to_string());
}

#[scalar(name = "ipmasklen")]
fn ip_masklen(args: &[Value]) -> Value {
    let Some(cidr_arg) = args[0].to_text() else {
        return Value::error(ResultCode::InvalidArgs);
    };

    let Ok(network) = cidr_arg.parse::<IpNetwork>() else {
        return Value::error(ResultCode::InvalidArgs);
    };

    Value::from_integer(network.prefix() as i64)
}

#[scalar(name = "ipnetwork")]
fn ip_network(args: &[Value]) -> Value {
    let Some(cidr_arg) = args[0].to_text() else {
        return Value::error(ResultCode::InvalidArgs);
    };

    let Ok(network) = cidr_arg.parse::<IpNetwork>() else {
        return Value::error(ResultCode::InvalidArgs);
    };

    Value::from_text(format!("{}/{}", network.network(), network.prefix()))
}
