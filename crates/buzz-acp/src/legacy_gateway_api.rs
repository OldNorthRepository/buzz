//! Minimal client for the legacy gateway's public `/v0` Unix HTTP contract.
//!
//! Keeping the wire client here lets a public Buzz build resolve its own
//! dependencies. It does not embed gateway runtime or credential material.

use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::time::Duration;

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

const MAX_RESPONSE_BYTES: usize = 2 * 1024 * 1024;

pub(crate) enum Endpoint {
    Socket(PathBuf),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ClientError {
    #[error("gateway I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("gateway JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("gateway returned HTTP {0}")]
    Http(u16),
    #[error("invalid gateway HTTP response")]
    InvalidResponse,
}

pub(crate) struct ControlClient {
    path: PathBuf,
}

impl ControlClient {
    pub(crate) async fn connect(endpoint: Endpoint) -> Result<Self, ClientError> {
        let Endpoint::Socket(path) = endpoint;
        Ok(Self { path })
    }

    pub(crate) async fn status(&self) -> Result<serde_json::Value, ClientError> {
        self.call("GET", "/v0/status", None).await
    }

    pub(crate) async fn routes(&self) -> Result<RouteListResponse, ClientError> {
        self.call("GET", "/v0/routes", None).await
    }

    pub(crate) async fn set_route(&self, route: SetRouteRequest) -> Result<RouteView, ClientError> {
        self.call("POST", "/v0/routes", Some(serde_json::to_vec(&route)?))
            .await
    }

    pub(crate) async fn submit(
        &self,
        job: SubmitJobRequest,
    ) -> Result<SubmitJobResponse, ClientError> {
        self.call("POST", "/v0/jobs", Some(serde_json::to_vec(&job)?))
            .await
    }

    pub(crate) async fn job(&self, id: &str) -> Result<JobView, ClientError> {
        self.call("GET", &format!("/v0/jobs/{id}"), None).await
    }

    pub(crate) async fn cancel(&self, id: &str) -> Result<serde_json::Value, ClientError> {
        self.call("POST", &format!("/v0/jobs/{id}/cancel"), None)
            .await
    }

    async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<T, ClientError> {
        let body = body.unwrap_or_default();
        let request = format!(
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        let bytes = tokio::time::timeout(Duration::from_secs(10), async {
            let mut stream = UnixStream::connect(&self.path).await?;
            stream.write_all(request.as_bytes()).await?;
            stream.write_all(&body).await?;
            let mut response = Vec::new();
            let mut chunk = [0u8; 16 * 1024];
            loop {
                let read = stream.read(&mut chunk).await?;
                if read == 0 {
                    break;
                }
                if response.len().saturating_add(read) > MAX_RESPONSE_BYTES {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "gateway response too large",
                    ));
                }
                response.extend_from_slice(&chunk[..read]);
            }
            Ok::<_, io::Error>(response)
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "gateway request timed out"))??;

        let boundary = bytes
            .windows(4)
            .position(|part| part == b"\r\n\r\n")
            .ok_or(ClientError::InvalidResponse)?;
        let head =
            std::str::from_utf8(&bytes[..boundary]).map_err(|_| ClientError::InvalidResponse)?;
        let status = head
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse::<u16>().ok())
            .ok_or(ClientError::InvalidResponse)?;
        if status != 200 {
            return Err(ClientError::Http(status));
        }
        Ok(serde_json::from_slice(&bytes[boundary + 4..])?)
    }
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct RouteListResponse {
    pub routes: Vec<RouteView>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct RouteView {
    pub id: String,
    pub agent_id: String,
    pub contexts: Vec<String>,
    pub workspace_path: String,
    pub permission_profile: String,
}

#[derive(Serialize)]
pub(crate) struct SetRouteRequest {
    pub id: String,
    pub agent_id: String,
    pub contexts: Vec<String>,
    pub runtime: String,
    pub model: Option<String>,
    pub workspace_path: String,
    pub session_mode: Option<String>,
    pub permission_profile: String,
    pub repo_write_concurrency: Option<u32>,
    pub repo_read_concurrency: Option<u32>,
    pub agent_concurrency: Option<u32>,
    pub env_allowlist: BTreeMap<String, String>,
}

#[derive(Serialize)]
pub(crate) struct SubmitJobRequest {
    pub route_id: Option<String>,
    pub agent_id: Option<String>,
    pub context_id: Option<String>,
    pub thread_id: Option<String>,
    pub task: String,
    pub permission_profile: Option<String>,
    pub side_effect_class: Option<String>,
    pub priority: Option<String>,
    pub timeout_ms: Option<i64>,
    pub idempotency_key: Option<String>,
    pub source_id: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct SubmitJobResponse {
    pub job_id: String,
}

#[derive(Deserialize)]
pub(crate) struct JobView {
    pub state: String,
    pub reason: Option<String>,
    pub result: Option<JobResult>,
}

#[derive(Deserialize)]
pub(crate) struct JobResult {
    pub summary: String,
}
