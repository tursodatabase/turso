use std::io::Read;

use http::request;
use http_body_util::BodyExt;
use hyper::body::{Buf, Bytes};
use turso_sync_protocol::{errors::Error, Result};

use crate::sync_server::{DbSyncInfo, DbSyncStatus, Stream, SyncServer};

pub type Client = hyper_util::client::legacy::Client<
    hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
    http_body_util::Full<Bytes>,
>;

const DEFAULT_PULL_BATCH_SIZE: usize = 100;

pub struct TursoSyncServerOpts {
    pub sync_url: String,
    pub auth_token: Option<String>,
    pub encryption_key: Option<String>,
    pub pull_batch_size: Option<usize>,
}

pub struct TursoSyncServer {
    client: Client,
    auth_token_header: Option<hyper::header::HeaderValue>,
    opts: TursoSyncServerOpts,
}

fn sync_server_error(status: http::StatusCode, body: impl Buf) -> Error {
    let mut body_str = String::new();
    if let Err(e) = body.reader().read_to_string(&mut body_str) {
        Error::SyncServerError(status, format!("unable to read response body: {e}"))
    } else {
        Error::SyncServerError(status, body_str)
    }
}

async fn aggregate_body(body: hyper::body::Incoming) -> Result<impl Buf> {
    let chunks = body.collect().await;
    let chunks = chunks.map_err(Error::HyperResponse)?;
    Ok(chunks.aggregate())
}

pub struct HyperStream {
    body: hyper::body::Incoming,
}

impl Stream for HyperStream {
    async fn read_chunk(&mut self) -> Result<Option<bytes::Bytes>> {
        let Some(frame) = self.body.frame().await else {
            return Ok(None);
        };
        let frame = frame.map_err(Error::HyperResponse)?;
        let frame = frame
            .into_data()
            .map_err(|_| Error::DatabaseSyncError("failed to read export chunk".to_string()))?;
        Ok(Some(frame))
    }
}

impl TursoSyncServer {
    pub fn new(client: Client, opts: TursoSyncServerOpts) -> Result<Self> {
        let auth_token_header = opts
            .auth_token
            .as_ref()
            .map(|token| hyper::header::HeaderValue::from_str(&format!("Bearer {token}")))
            .transpose()
            .map_err(|e| Error::Http(e.into()))?;
        Ok(Self {
            client,
            opts,
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
        if let Some(encryption_key) = &self.opts.encryption_key {
            request = request.header("x-turso-encryption-key", encryption_key);
        }
        let request = request.body(body).map_err(Error::Http)?;
        let response = self.client.request(request).await;
        let response = response
            .map_err(|e| Error::DatabaseSyncError(format!("hyper response failed: {e}")))?;
        let status = response.status();
        Ok((status, response.into_body()))
    }
}

impl SyncServer for TursoSyncServer {
    type Stream = HyperStream;
    async fn db_info(&self) -> Result<DbSyncInfo> {
        tracing::debug!("db_info");
        let url = format!("{}/info", self.opts.sync_url);
        let empty = http_body_util::Full::new(Bytes::new());
        let (status, body) = self.send(http::Method::GET, &url, empty).await?;
        let body = aggregate_body(body).await?;

        if !status.is_success() {
            return Err(sync_server_error(status, body));
        }

        let info = serde_json::from_reader(body.reader()).map_err(Error::JsonDecode)?;
        tracing::debug!("db_info response: {:?}", info);
        Ok(info)
    }

    async fn wal_push(
        &self,
        baton: Option<String>,
        generation_id: usize,
        start_frame: usize,
        end_frame: usize,
        frames: Vec<u8>,
    ) -> Result<DbSyncStatus> {
        tracing::debug!(
            "wal_push: {}/{}/{} (baton: {:?})",
            generation_id,
            start_frame,
            end_frame,
            baton
        );
        let url = if let Some(baton) = baton {
            format!(
                "{}/sync/{}/{}/{}/{}",
                self.opts.sync_url, generation_id, start_frame, end_frame, baton
            )
        } else {
            format!(
                "{}/sync/{}/{}/{}",
                self.opts.sync_url, generation_id, start_frame, end_frame
            )
        };
        let body = http_body_util::Full::new(Bytes::from(frames));
        let (status_code, body) = self.send(http::Method::POST, &url, body).await?;
        let body = aggregate_body(body).await?;

        if !status_code.is_success() {
            return Err(sync_server_error(status_code, body));
        }

        let status: DbSyncStatus =
            serde_json::from_reader(body.reader()).map_err(Error::JsonDecode)?;

        match status.status.as_str() {
            "ok" => Ok(status),
            "conflict" => Err(Error::PushConflict),
            "push_needed" => Err(Error::PushInconsistent(status)),
            _ => Err(Error::SyncServerUnexpectedStatus(status)),
        }
    }

    async fn db_export(&self, generation_id: usize) -> Result<HyperStream> {
        tracing::debug!("db_export: {}", generation_id);
        let url = format!("{}/export/{}", self.opts.sync_url, generation_id);
        let empty = http_body_util::Full::new(Bytes::new());
        let (status, body) = self.send(http::Method::GET, &url, empty).await?;
        if !status.is_success() {
            let body = aggregate_body(body).await?;
            return Err(sync_server_error(status, body));
        }
        Ok(HyperStream { body })
    }

    async fn wal_pull(&self, generation_id: usize, start_frame: usize) -> Result<HyperStream> {
        let batch = self.opts.pull_batch_size.unwrap_or(DEFAULT_PULL_BATCH_SIZE);
        let end_frame = start_frame + batch;
        tracing::debug!("wall_pull: {}/{}/{}", generation_id, start_frame, end_frame);
        let url = format!(
            "{}/sync/{}/{}/{}",
            self.opts.sync_url, generation_id, start_frame, end_frame
        );
        let empty = http_body_util::Full::new(Bytes::new());
        let (status, body) = self.send(http::Method::GET, &url, empty).await?;
        if status == http::StatusCode::BAD_REQUEST {
            let body = aggregate_body(body).await?;
            let status: DbSyncStatus =
                serde_json::from_reader(body.reader()).map_err(Error::JsonDecode)?;
            if status.status == "checkpoint_needed" {
                return Err(Error::PullNeedCheckpoint(status));
            } else {
                return Err(Error::SyncServerUnexpectedStatus(status));
            }
        }
        if !status.is_success() {
            let body = aggregate_body(body).await?;
            return Err(sync_server_error(status, body));
        }
        Ok(HyperStream { body })
    }
}
