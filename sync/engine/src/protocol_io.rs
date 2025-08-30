use crate::Result;

pub trait DataPollResult {
    fn data(&self) -> &[u8];
}

pub trait DataCompletion {
    type DataPollResult: DataPollResult;
    fn status(&self) -> Result<Option<u16>>;
    fn poll_data(&self) -> Result<Option<Self::DataPollResult>>;
    fn is_done(&self) -> Result<bool>;
}

pub trait ProtocolIO {
    type DataCompletion: DataCompletion;
    fn full_read(&self, path: &str) -> Result<Self::DataCompletion>;
    fn full_write(&self, path: &str, content: Vec<u8>) -> Result<Self::DataCompletion>;
    fn http(
        &self,
        method: &str,
        path: &str,
        body: Option<Vec<u8>>,
        headers: &[(&str, &str)],
    ) -> Result<Self::DataCompletion>;
}
