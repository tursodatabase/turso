/// Common results that different functions can return in limbo.
#[derive(Debug)]
pub enum LimboResult {
    /// Couldn't acquire a lock
    Busy,
    Ok,
}
