//! Authenticated loopback control surface for owner-approved local automation.
//!
//! Buzz Desktop remains the authority for managed-agent keys, storage, and
//! processes. The native CLI discovers this service through an owner-readable
//! descriptor in the app-data directory and must present its bearer token.

use std::{io::Write, path::PathBuf};

use atomic_write_file::AtomicWriteFile;
use axum::{
    extract::{Path, State},
    http::{header::AUTHORIZATION, HeaderMap, StatusCode},
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tauri::{AppHandle, Manager};
use tokio::net::TcpListener;

use crate::{
    app_state::AppState,
    commands::{
        add_channel_members, create_channel, create_managed_agent,
        delete_channel as delete_channel_command, delete_managed_agent, list_managed_agents,
        set_canvas, start_managed_agent, stop_managed_agent,
    },
    managed_agents::CreateManagedAgentRequest,
};

const CONTROL_DESCRIPTOR: &str = "desktop-control.json";

#[derive(Clone)]
struct ControlState {
    app: AppHandle,
    token: String,
}

#[derive(Debug, Serialize)]
struct ControlDescriptor {
    schema_version: u8,
    url: String,
    token: String,
    pid: u32,
}

#[derive(Debug, Deserialize)]
struct ApprovalRequest {
    #[serde(default)]
    approve: bool,
}

#[derive(Debug, Deserialize)]
struct CreateRequest {
    #[serde(default)]
    approve: bool,
    #[serde(flatten)]
    input: CreateManagedAgentRequest,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateChannelRequest {
    #[serde(default)]
    approve: bool,
    name: String,
    #[serde(default = "default_channel_type")]
    channel_type: String,
    #[serde(default = "default_channel_visibility")]
    visibility: String,
    #[serde(default)]
    description: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CanvasRequest {
    #[serde(default)]
    approve: bool,
    content: String,
}

#[derive(Debug, Deserialize)]
struct MembersRequest {
    #[serde(default)]
    approve: bool,
    pubkeys: Vec<String>,
    #[serde(default)]
    role: Option<String>,
}

type ApiError = (StatusCode, Json<Value>);

fn default_channel_type() -> String {
    "stream".to_string()
}

fn default_channel_visibility() -> String {
    "private".to_string()
}

fn api_error(status: StatusCode, message: impl Into<String>) -> ApiError {
    (
        status,
        Json(json!({
            "ok": false,
            "error": status.canonical_reason().unwrap_or("error"),
            "message": message.into(),
        })),
    )
}

fn authorize(headers: &HeaderMap, token: &str) -> Result<(), ApiError> {
    let expected = format!("Bearer {token}");
    let supplied = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    if supplied != Some(expected.as_str()) {
        return Err(api_error(
            StatusCode::UNAUTHORIZED,
            "missing or invalid desktop control token",
        ));
    }
    Ok(())
}

fn require_approval(approve: bool) -> Result<(), ApiError> {
    if !approve {
        return Err(api_error(
            StatusCode::PRECONDITION_REQUIRED,
            "managed-agent mutations require explicit approval",
        ));
    }
    Ok(())
}

async fn status(
    State(state): State<ControlState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, &state.token)?;
    Ok(Json(json!({
        "ok": true,
        "service": "buzz-desktop-control",
        "schema_version": 1,
        "pid": std::process::id(),
    })))
}

async fn list_agents(
    State(state): State<ControlState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, &state.token)?;
    let agents = list_managed_agents(state.app)
        .await
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(json!({ "ok": true, "agents": agents })))
}

async fn create_agent(
    State(state): State<ControlState>,
    headers: HeaderMap,
    Json(request): Json<CreateRequest>,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, &state.token)?;
    require_approval(request.approve)?;
    let app_state = state.app.state::<AppState>();
    let response = create_managed_agent(request.input, state.app.clone(), app_state)
        .await
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    Ok(Json(json!({
        "ok": true,
        "agent": response.agent,
        "private_key_saved": true,
        "profile_sync_error": response.profile_sync_error,
        "spawn_error": response.spawn_error,
    })))
}

async fn start_agent(
    State(state): State<ControlState>,
    headers: HeaderMap,
    Path(pubkey): Path<String>,
    Json(request): Json<ApprovalRequest>,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, &state.token)?;
    require_approval(request.approve)?;
    let app_state = state.app.state::<AppState>();
    let agent = start_managed_agent(pubkey, state.app.clone(), app_state)
        .await
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    Ok(Json(json!({ "ok": true, "agent": agent })))
}

async fn stop_agent(
    State(state): State<ControlState>,
    headers: HeaderMap,
    Path(pubkey): Path<String>,
    Json(request): Json<ApprovalRequest>,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, &state.token)?;
    require_approval(request.approve)?;
    let agent = stop_managed_agent(pubkey, state.app)
        .await
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    Ok(Json(json!({ "ok": true, "agent": agent })))
}

async fn restart_agent(
    State(state): State<ControlState>,
    headers: HeaderMap,
    Path(pubkey): Path<String>,
    Json(request): Json<ApprovalRequest>,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, &state.token)?;
    require_approval(request.approve)?;
    stop_managed_agent(pubkey.clone(), state.app.clone())
        .await
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    let app_state = state.app.state::<AppState>();
    let agent = start_managed_agent(pubkey, state.app.clone(), app_state)
        .await
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    Ok(Json(json!({ "ok": true, "agent": agent })))
}

async fn delete_agent(
    State(state): State<ControlState>,
    headers: HeaderMap,
    Path(pubkey): Path<String>,
    Json(request): Json<ApprovalRequest>,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, &state.token)?;
    require_approval(request.approve)?;
    delete_managed_agent(pubkey.clone(), Some(false), state.app)
        .await
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    Ok(Json(json!({ "ok": true, "deleted": pubkey })))
}

async fn create_local_channel(
    State(state): State<ControlState>,
    headers: HeaderMap,
    Json(request): Json<CreateChannelRequest>,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, &state.token)?;
    require_approval(request.approve)?;
    let app_state = state.app.state::<AppState>();
    let channel = create_channel(
        request.name,
        request.channel_type,
        request.visibility,
        request.description,
        None,
        app_state,
    )
    .await
    .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    Ok(Json(json!({ "ok": true, "channel": channel })))
}

async fn set_local_canvas(
    State(state): State<ControlState>,
    headers: HeaderMap,
    Path(channel_id): Path<String>,
    Json(request): Json<CanvasRequest>,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, &state.token)?;
    require_approval(request.approve)?;
    let app_state = state.app.state::<AppState>();
    let result = set_canvas(channel_id, request.content, app_state)
        .await
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    Ok(Json(json!({ "ok": true, "canvas": result })))
}

async fn add_local_members(
    State(state): State<ControlState>,
    headers: HeaderMap,
    Path(channel_id): Path<String>,
    Json(request): Json<MembersRequest>,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, &state.token)?;
    require_approval(request.approve)?;
    let app_state = state.app.state::<AppState>();
    let result = add_channel_members(channel_id, request.pubkeys, request.role, app_state)
        .await
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    let ok = result
        .get("errors")
        .and_then(Value::as_array)
        .is_none_or(Vec::is_empty);
    Ok(Json(json!({ "ok": ok, "membership": result })))
}

async fn delete_local_channel(
    State(state): State<ControlState>,
    headers: HeaderMap,
    Path(channel_id): Path<String>,
    Json(request): Json<ApprovalRequest>,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, &state.token)?;
    require_approval(request.approve)?;
    let app_state = state.app.state::<AppState>();
    delete_channel_command(channel_id.clone(), app_state)
        .await
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    Ok(Json(json!({ "ok": true, "deleted": channel_id })))
}

fn router(state: ControlState) -> Router {
    Router::new()
        .route("/v1/status", get(status))
        .route("/v1/managed-agents", get(list_agents).post(create_agent))
        .route("/v1/managed-agents/{pubkey}", delete(delete_agent))
        .route("/v1/managed-agents/{pubkey}/start", post(start_agent))
        .route("/v1/managed-agents/{pubkey}/stop", post(stop_agent))
        .route("/v1/managed-agents/{pubkey}/restart", post(restart_agent))
        .route("/v1/channels", post(create_local_channel))
        .route("/v1/channels/{channel_id}", delete(delete_local_channel))
        .route("/v1/channels/{channel_id}/canvas", post(set_local_canvas))
        .route("/v1/channels/{channel_id}/members", post(add_local_members))
        .with_state(state)
}

fn descriptor_path(app: &AppHandle) -> Result<PathBuf, String> {
    app.path()
        .app_data_dir()
        .map(|path| path.join(CONTROL_DESCRIPTOR))
        .map_err(|error| format!("resolve desktop control descriptor path: {error}"))
}

fn write_descriptor(path: &std::path::Path, descriptor: &ControlDescriptor) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(descriptor)
        .map_err(|error| format!("serialize desktop control descriptor: {error}"))?;
    let mut file = AtomicWriteFile::open(path)
        .map_err(|error| format!("open desktop control descriptor: {error}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(|error| format!("set desktop control descriptor permissions: {error}"))?;
    }
    file.write_all(&bytes)
        .map_err(|error| format!("write desktop control descriptor: {error}"))?;
    file.commit()
        .map_err(|error| format!("commit desktop control descriptor: {error}"))
}

/// Start the authenticated Desktop control service on an OS-assigned loopback
/// port and publish its connection descriptor for the native Buzz CLI.
pub async fn spawn(app: AppHandle) -> Result<(), String> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| format!("bind desktop control service: {error}"))?;
    let address = listener
        .local_addr()
        .map_err(|error| format!("read desktop control address: {error}"))?;
    let token = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let descriptor = ControlDescriptor {
        schema_version: 1,
        url: format!("http://{address}"),
        token: token.clone(),
        pid: std::process::id(),
    };
    let path = descriptor_path(&app)?;
    write_descriptor(&path, &descriptor)?;

    eprintln!("buzz-desktop: local control service listening on {address}");
    axum::serve(listener, router(ControlState { app, token }))
        .await
        .map_err(|error| format!("desktop control service stopped: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutation_approval_is_fail_closed() {
        let error = require_approval(false).expect_err("missing approval must fail");
        assert_eq!(error.0, StatusCode::PRECONDITION_REQUIRED);
        assert!(require_approval(true).is_ok());
    }

    #[test]
    fn bearer_auth_requires_exact_token() {
        let mut headers = HeaderMap::new();
        assert_eq!(
            authorize(&headers, "secret").unwrap_err().0,
            StatusCode::UNAUTHORIZED
        );
        headers.insert(AUTHORIZATION, "Bearer wrong".parse().unwrap());
        assert_eq!(
            authorize(&headers, "secret").unwrap_err().0,
            StatusCode::UNAUTHORIZED
        );
        headers.insert(AUTHORIZATION, "Bearer secret".parse().unwrap());
        assert!(authorize(&headers, "secret").is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn descriptor_is_written_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(CONTROL_DESCRIPTOR);
        let descriptor = ControlDescriptor {
            schema_version: 1,
            url: "http://127.0.0.1:1234".to_string(),
            token: "a".repeat(64),
            pid: 42,
        };
        write_descriptor(&path, &descriptor).unwrap();
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}
