use std::io::Read;

use http::request;
use http_body_util::BodyExt;
use hyper::body::{Buf, Bytes};

use crate::{errors::Error, Result};

#[derive(Debug, serde::Deserialize)]
pub struct DbSyncInfo {
    pub current_generation: usize,
}

#[derive(Debug, serde::Deserialize)]
pub struct DbSyncStatus {
    pub status: String,
    pub generation: usize,
    pub max_frame_no: usize,
}

pub trait SyncServer {
    fn db_info(&self) -> impl std::future::Future<Output = Result<DbSyncInfo>> + Send;
    fn db_export(
        &self,
        generation_id: usize,
    ) -> impl std::future::Future<Output = Result<hyper::body::Incoming>> + Send;
    fn wal_pull(
        &self,
        generation_id: usize,
        start_frame: usize,
        end_frame: usize,
    ) -> impl std::future::Future<Output = Result<hyper::body::Incoming>> + Send;
    fn wal_push(
        &self,
        generation_id: usize,
        start_frame: usize,
        end_frame: usize,
        frames: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<DbSyncStatus>> + Send;
}

pub(crate) type Client = hyper_util::client::legacy::Client<
    hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
    http_body_util::Full<Bytes>,
>;

pub struct TursoSyncServerConfig {
    pub sync_url: String,
    pub auth_token: Option<String>,
    pub encryption_key: Option<String>,
}

pub struct TursoSyncServer {
    client: Client,
    auth_token_header: Option<hyper::header::HeaderValue>,
    config: TursoSyncServerConfig,
}

fn sync_server_error(status: http::StatusCode, body: impl Buf) -> Error {
    let mut body_str = String::new();
    if let Err(e) = body.reader().read_to_string(&mut body_str) {
        Error::SyncServerError(status, format!("unable to read response body: {}", e))
    } else {
        Error::SyncServerError(status, body_str)
    }
}

async fn aggregate_body(body: hyper::body::Incoming) -> Result<impl Buf> {
    let chunks = body.collect().await;
    let chunks = chunks.map_err(Error::HyperResponse)?;
    Ok(chunks.aggregate())
}

impl TursoSyncServer {
    pub fn new(client: Client, config: TursoSyncServerConfig) -> Result<Self> {
        let auth_token_header = config
            .auth_token
            .as_ref()
            .map(|token| hyper::header::HeaderValue::from_str(&format!("Bearer {}", token)))
            .transpose()
            .map_err(|e| Error::Http(e.into()))?;
        Ok(Self {
            client,
            config,
            auth_token_header,
        })
    }
    async fn send(
        &self,
        method: http::Method,
        url: &str,
        body: http_body_util::Full<Bytes>,
    ) -> Result<(http::StatusCode, hyper::body::Incoming)> {
        let url: hyper::Uri = url.parse().map_err(Error::Uri)?;
        let mut request = request::Builder::new().uri(url).method(method);
        if let Some(auth_token_header) = &self.auth_token_header {
            request = request.header("Authorization", auth_token_header);
        }
        if let Some(encryption_key) = &self.config.encryption_key {
            request = request.header("x-turso-encryption-key", encryption_key);
        }
        let request = request.body(body).map_err(Error::Http)?;
        let response = self.client.request(request).await;
        let response = response.map_err(Error::HyperRequest)?;
        let status = response.status();
        Ok((status, response.into_body()))
    }
}

impl SyncServer for TursoSyncServer {
    async fn db_info(&self) -> Result<DbSyncInfo> {
        let url = format!("{}/info", self.config.sync_url);
        let empty = http_body_util::Full::new(Bytes::new());
        let (status, body) = self.send(http::Method::GET, &url, empty).await?;
        let body = aggregate_body(body).await?;

        if !status.is_success() {
            return Err(sync_server_error(status, body));
        }

        let info = serde_json::from_reader(body.reader()).map_err(Error::JsonDecode)?;
        Ok(info)
    }

    async fn wal_push(
        &self,
        generation_id: usize,
        start_frame: usize,
        end_frame: usize,
        frames: Vec<u8>,
    ) -> Result<DbSyncStatus> {
        let url = format!(
            "{}/sync/{}/{}/{}",
            self.config.sync_url, generation_id, start_frame, end_frame
        );
        let body = http_body_util::Full::new(Bytes::from(frames));
        let (status_code, body) = self.send(http::Method::POST, &url, body).await?;
        let body = aggregate_body(body).await?;

        if !status_code.is_success() {
            return Err(sync_server_error(status_code, body));
        }

        let status: DbSyncStatus;
        status = serde_json::from_reader(body.reader()).map_err(Error::JsonDecode)?;

        match status.status.as_str() {
            "ok" => Ok(status),
            "conflict" => Err(Error::PushConflict(status)),
            "push_needed" => Err(Error::PushInconsistent(status)),
            _ => Err(Error::SyncServerUnexpectedStatus(status)),
        }
    }

    async fn db_export(&self, generation_id: usize) -> Result<hyper::body::Incoming> {
        let url = format!("{}/export/{}", self.config.sync_url, generation_id);
        let empty = http_body_util::Full::new(Bytes::new());
        let (status, body) = self.send(http::Method::GET, &url, empty).await?;
        if !status.is_success() {
            let body = aggregate_body(body).await?;
            return Err(sync_server_error(status, body));
        }
        Ok(body)
    }

    async fn wal_pull(
        &self,
        generation_id: usize,
        start_frame: usize,
        end_frame: usize,
    ) -> Result<hyper::body::Incoming> {
        let url = format!(
            "{}/sync/{}/{}/{}",
            self.config.sync_url, generation_id, start_frame, end_frame
        );
        let empty = http_body_util::Full::new(Bytes::new());
        let (status, body) = self.send(http::Method::GET, &url, empty).await?;
        if status == http::StatusCode::BAD_REQUEST {
            let body = aggregate_body(body).await?;
            let status: DbSyncStatus;
            status = serde_json::from_reader(body.reader()).map_err(Error::JsonDecode)?;
            if status.status == "checkpoint_needed" {
                return Err(Error::PullCheckpointNeeded(status));
            } else {
                return Err(Error::SyncServerUnexpectedStatus(status));
            }
        }
        if !status.is_success() {
            let body = aggregate_body(body).await?;
            return Err(sync_server_error(status, body));
        }
        Ok(body)
    }
}
