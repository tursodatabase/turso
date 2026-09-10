use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::TantivyError;

pub(crate) fn format_date(val: i64) -> crate::Result<String> {
    let datetime = OffsetDateTime::from_unix_timestamp_nanos(val as i128).map_err(|err| {
        TantivyError::InvalidArgument(format!(
            "Could not convert {val:?} to OffsetDateTime, err {err:?}"
        ))
    })?;
    let key_as_string = datetime
        .format(&Rfc3339)
        .map_err(|_err| TantivyError::InvalidArgument("Could not serialize date".to_string()))?;
    Ok(key_as_string)
}
