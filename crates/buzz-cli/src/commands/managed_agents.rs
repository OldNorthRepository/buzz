//! Native CLI client for Buzz Desktop's authenticated local control service.

use std::path::{Path, PathBuf};

use reqwest::{Method, Url};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{error::CliError, validate::validate_hex64, ManagedAgentsCmd};

const PROD_BUNDLE_IDENTIFIER: &str = "xyz.block.buzz.app";
const CONTROL_DESCRIPTOR: &str = "desktop-control.json";

#[derive(Debug, Deserialize)]
struct ControlDescriptor {
    schema_version: u8,
    url: String,
    token: String,
}

fn resolve_descriptor_path(override_path: Option<&Path>) -> Result<PathBuf, CliError> {
    if let Some(path) = override_path {
        return Ok(path.to_path_buf());
    }
    let data_dir = dirs::data_dir().ok_or_else(|| {
        CliError::Other("could not resolve platform app-data directory".to_string())
    })?;
    Ok(data_dir
        .join(PROD_BUNDLE_IDENTIFIER)
        .join(CONTROL_DESCRIPTOR))
}

fn load_descriptor(path: &Path) -> Result<ControlDescriptor, CliError> {
    let raw = std::fs::read_to_string(path).map_err(|error| {
        CliError::NotFound(format!(
            "Buzz Desktop control service is unavailable at {}: {error}; start Buzz Desktop first",
            path.display()
        ))
    })?;
    let descriptor: ControlDescriptor = serde_json::from_str(&raw).map_err(|error| {
        CliError::Other(format!(
            "invalid Buzz Desktop control descriptor at {}: {error}",
            path.display()
        ))
    })?;
    if descriptor.schema_version != 1 {
        return Err(CliError::Other(format!(
            "unsupported Buzz Desktop control schema {}",
            descriptor.schema_version
        )));
    }
    validate_loopback_url(&descriptor.url)?;
    if descriptor.token.len() < 32 {
        return Err(CliError::Auth(
            "Buzz Desktop control descriptor contains an invalid token".to_string(),
        ));
    }
    Ok(descriptor)
}

fn validate_loopback_url(raw: &str) -> Result<Url, CliError> {
    let url = Url::parse(raw)
        .map_err(|error| CliError::Other(format!("invalid desktop control URL: {error}")))?;
    if url.scheme() != "http"
        || !matches!(
            url.host_str(),
            Some("127.0.0.1") | Some("::1") | Some("[::1]")
        )
    {
        return Err(CliError::Auth(
            "desktop control URL must use HTTP on loopback".to_string(),
        ));
    }
    Ok(url)
}

fn approval_plan(action: &str, target: Value) {
    println!(
        "{}",
        json!({
            "ok": true,
            "executed": false,
            "approval_required": true,
            "action": action,
            "target": target,
            "message": "Review this plan, then rerun the same command with --approve.",
        })
    );
}

async fn request_value(
    descriptor_path: Option<&Path>,
    method: Method,
    route: &str,
    body: Option<Value>,
) -> Result<Value, CliError> {
    let descriptor = load_descriptor(&resolve_descriptor_path(descriptor_path)?)?;
    let base = validate_loopback_url(&descriptor.url)?;
    let url = base
        .join(route)
        .map_err(|error| CliError::Other(format!("build desktop control URL: {error}")))?;
    // The descriptor contains a bearer credential. Never send it through an
    // ambient proxy and never follow a redirect away from the verified
    // loopback origin.
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| CliError::Other(format!("build desktop control client: {error}")))?;
    let mut builder = client.request(method, url).bearer_auth(&descriptor.token);
    if let Some(body) = body {
        builder = builder.json(&body);
    }
    let response = builder.send().await?;
    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        return Err(CliError::Relay {
            status: status.as_u16(),
            body: text,
        });
    }
    serde_json::from_str(&text)
        .map_err(|error| CliError::Other(format!("invalid desktop control response: {error}")))
}

async fn request(
    descriptor_path: Option<&Path>,
    method: Method,
    route: &str,
    body: Option<Value>,
) -> Result<(), CliError> {
    let value = request_value(descriptor_path, method, route, body).await?;
    println!("{value}");
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AgentSpec {
    name: String,
    command: String,
}

fn parse_agent_specs(values: &[String]) -> Result<Vec<AgentSpec>, CliError> {
    values
        .iter()
        .map(|value| {
            let (name, command) = value.split_once('=').ok_or_else(|| {
                CliError::Usage(format!("invalid --agent {value:?}; expected NAME=COMMAND"))
            })?;
            let name = name.trim();
            let command = command.trim();
            if name.is_empty() || command.is_empty() {
                return Err(CliError::Usage(format!(
                    "invalid --agent {value:?}; name and command are required"
                )));
            }
            Ok(AgentSpec {
                name: name.to_string(),
                command: command.to_string(),
            })
        })
        .collect()
}

fn resolve_context_path(path: &Path) -> Result<PathBuf, CliError> {
    let expanded = if path == Path::new("~") {
        dirs::home_dir()
            .ok_or_else(|| CliError::Other("could not resolve home directory".to_string()))?
    } else if let Ok(relative) = path.strip_prefix("~/") {
        dirs::home_dir()
            .ok_or_else(|| CliError::Other("could not resolve home directory".to_string()))?
            .join(relative)
    } else {
        path.to_path_buf()
    };
    let canonical = std::fs::canonicalize(&expanded).map_err(|error| {
        CliError::Usage(format!(
            "invalid context directory {}: {error}",
            expanded.display()
        ))
    })?;
    if !canonical.is_dir() {
        return Err(CliError::Usage(format!(
            "context must be a directory: {}",
            canonical.display()
        )));
    }
    Ok(canonical)
}

fn canvas_content(channel_name: &str, context: &Path, agents: &[AgentSpec]) -> String {
    let roster = agents
        .iter()
        .map(|agent| format!("- **{}** — `{}`", agent.name, agent.command))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "# {channel_name}\n\n## Working context\n\n`{}`\n\n## Managed agents\n\n{roster}\n",
        context.display()
    )
}

fn agent_system_prompt(base: Option<&str>, context: &Path) -> String {
    let context_instruction = format!(
        "Primary working context: {}. Treat that directory as the project root and follow its repository instructions.",
        context.display()
    );
    match base.map(str::trim).filter(|value| !value.is_empty()) {
        Some(base) => format!("{base}\n\n{context_instruction}"),
        None => context_instruction,
    }
}

pub async fn dispatch(
    command: &ManagedAgentsCmd,
    descriptor_path: Option<&Path>,
) -> Result<(), CliError> {
    match command {
        ManagedAgentsCmd::Status => request(descriptor_path, Method::GET, "v1/status", None).await,
        ManagedAgentsCmd::List => {
            request(descriptor_path, Method::GET, "v1/managed-agents", None).await
        }
        ManagedAgentsCmd::Create {
            name,
            agent_command,
            system_prompt,
            model,
            provider,
            start,
            start_on_app_launch,
            approve,
        } => {
            let body = json!({
                "approve": approve,
                "name": name,
                "agentCommand": agent_command,
                "harnessOverride": true,
                "systemPrompt": system_prompt,
                "model": model,
                "provider": provider,
                "spawnAfterCreate": start,
                "startOnAppLaunch": start_on_app_launch,
            });
            if !approve {
                approval_plan("create_managed_agent", body);
                return Ok(());
            }
            request(
                descriptor_path,
                Method::POST,
                "v1/managed-agents",
                Some(body),
            )
            .await
        }
        ManagedAgentsCmd::ProvisionChannel {
            name,
            context,
            agents,
            description,
            visibility,
            channel_type,
            system_prompt,
            start,
            start_on_app_launch,
            approve,
        } => {
            let context = resolve_context_path(context)?;
            let agents = parse_agent_specs(agents)?;
            let plan = json!({
                "name": name,
                "context": context,
                "agents": agents.iter().map(|agent| json!({
                    "name": agent.name,
                    "command": agent.command,
                    "role": "bot",
                })).collect::<Vec<_>>(),
                "description": description,
                "visibility": visibility,
                "channel_type": channel_type,
                "start": start,
                "start_on_app_launch": start_on_app_launch,
                "rollback": "delete agents created by this command, then delete the channel",
            });
            if !approve {
                approval_plan("provision_managed_channel", plan);
                return Ok(());
            }
            provision_channel(
                descriptor_path,
                name,
                &context,
                &agents,
                description.as_deref(),
                visibility,
                channel_type,
                system_prompt.as_deref(),
                *start,
                *start_on_app_launch,
            )
            .await
        }
        ManagedAgentsCmd::Start { pubkey, approve } => {
            lifecycle_request(descriptor_path, "start", pubkey, *approve).await
        }
        ManagedAgentsCmd::Stop { pubkey, approve } => {
            lifecycle_request(descriptor_path, "stop", pubkey, *approve).await
        }
        ManagedAgentsCmd::Restart { pubkey, approve } => {
            lifecycle_request(descriptor_path, "restart", pubkey, *approve).await
        }
        ManagedAgentsCmd::Delete { pubkey, approve } => {
            delete_agent(descriptor_path, pubkey, *approve).await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn provision_channel(
    descriptor_path: Option<&Path>,
    name: &str,
    context: &Path,
    agents: &[AgentSpec],
    description: Option<&str>,
    visibility: &str,
    channel_type: &str,
    system_prompt: Option<&str>,
    start: bool,
    start_on_app_launch: bool,
) -> Result<(), CliError> {
    let channel_response = request_value(
        descriptor_path,
        Method::POST,
        "v1/channels",
        Some(json!({
            "approve": true,
            "name": name,
            "channelType": channel_type,
            "visibility": visibility,
            "description": description,
        })),
    )
    .await?;
    let channel = channel_response
        .get("channel")
        .cloned()
        .ok_or_else(|| CliError::Other("desktop channel response omitted channel".to_string()))?;
    let channel_id = channel
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::Other("desktop channel response omitted channel id".to_string()))?
        .to_string();
    let mut created_pubkeys = Vec::new();

    let canvas = match request_value(
        descriptor_path,
        Method::POST,
        &format!("v1/channels/{channel_id}/canvas"),
        Some(json!({
            "approve": true,
            "content": canvas_content(name, context, agents),
        })),
    )
    .await
    {
        Ok(value) => value,
        Err(error) => {
            return provision_failure(descriptor_path, error, &created_pubkeys, Some(&channel_id))
                .await;
        }
    };

    let prompt = agent_system_prompt(system_prompt, context);
    let mut provisioned_agents = Vec::new();
    for spec in agents {
        let created = match request_value(
            descriptor_path,
            Method::POST,
            "v1/managed-agents",
            Some(json!({
                "approve": true,
                "name": spec.name,
                "agentCommand": spec.command,
                "harnessOverride": true,
                "systemPrompt": prompt,
                "spawnAfterCreate": false,
                "startOnAppLaunch": start_on_app_launch,
            })),
        )
        .await
        {
            Ok(value) => value,
            Err(error) => {
                return provision_failure(
                    descriptor_path,
                    error,
                    &created_pubkeys,
                    Some(&channel_id),
                )
                .await;
            }
        };
        let pubkey = match created
            .pointer("/agent/pubkey")
            .and_then(Value::as_str)
            .map(str::to_string)
        {
            Some(pubkey) => pubkey,
            None => {
                return provision_failure(
                    descriptor_path,
                    CliError::Other(
                        "desktop managed-agent response omitted agent pubkey".to_string(),
                    ),
                    &created_pubkeys,
                    Some(&channel_id),
                )
                .await;
            }
        };
        created_pubkeys.push(pubkey.clone());

        let membership = match request_value(
            descriptor_path,
            Method::POST,
            &format!("v1/channels/{channel_id}/members"),
            Some(json!({
                "approve": true,
                "pubkeys": [pubkey],
                "role": "bot",
            })),
        )
        .await
        {
            Ok(value) if value.get("ok").and_then(Value::as_bool) == Some(true) => value,
            Ok(value) => {
                return provision_failure(
                    descriptor_path,
                    CliError::Other(format!("channel membership failed: {value}")),
                    &created_pubkeys,
                    Some(&channel_id),
                )
                .await;
            }
            Err(error) => {
                return provision_failure(
                    descriptor_path,
                    error,
                    &created_pubkeys,
                    Some(&channel_id),
                )
                .await;
            }
        };

        let started_agent = if start {
            match request_value(
                descriptor_path,
                Method::POST,
                &format!("v1/managed-agents/{pubkey}/start"),
                Some(json!({ "approve": true })),
            )
            .await
            {
                Ok(value) => Some(value),
                Err(error) => {
                    return provision_failure(
                        descriptor_path,
                        error,
                        &created_pubkeys,
                        Some(&channel_id),
                    )
                    .await;
                }
            }
        } else {
            None
        };

        provisioned_agents.push(json!({
            "name": spec.name,
            "command": spec.command,
            "pubkey": pubkey,
            "created": created,
            "membership": membership,
            "started": started_agent,
        }));
    }

    println!(
        "{}",
        json!({
            "ok": true,
            "executed": true,
            "action": "provision_managed_channel",
            "channel": channel,
            "canvas": canvas,
            "context": context,
            "agents": provisioned_agents,
        })
    );
    Ok(())
}

async fn provision_failure(
    descriptor_path: Option<&Path>,
    cause: CliError,
    created_pubkeys: &[String],
    channel_id: Option<&str>,
) -> Result<(), CliError> {
    let mut rollback_errors = Vec::new();
    for pubkey in created_pubkeys.iter().rev() {
        if let Err(error) = request_value(
            descriptor_path,
            Method::DELETE,
            &format!("v1/managed-agents/{pubkey}"),
            Some(json!({ "approve": true })),
        )
        .await
        {
            rollback_errors.push(format!("delete agent {pubkey}: {error}"));
        }
    }
    if let Some(channel_id) = channel_id {
        if let Err(error) = request_value(
            descriptor_path,
            Method::DELETE,
            &format!("v1/channels/{channel_id}"),
            Some(json!({ "approve": true })),
        )
        .await
        {
            rollback_errors.push(format!("delete channel {channel_id}: {error}"));
        }
    }
    let rollback = if rollback_errors.is_empty() {
        "rollback completed".to_string()
    } else {
        format!("rollback incomplete: {}", rollback_errors.join("; "))
    };
    Err(CliError::Other(format!(
        "managed channel provisioning failed: {cause}; {rollback}"
    )))
}

async fn delete_agent(
    descriptor_path: Option<&Path>,
    pubkey: &str,
    approve: bool,
) -> Result<(), CliError> {
    validate_hex64(pubkey)?;
    let target = json!({ "pubkey": pubkey });
    if !approve {
        approval_plan("delete_managed_agent", target);
        return Ok(());
    }
    request(
        descriptor_path,
        Method::DELETE,
        &format!("v1/managed-agents/{pubkey}"),
        Some(json!({ "approve": true })),
    )
    .await
}

async fn lifecycle_request(
    descriptor_path: Option<&Path>,
    action: &str,
    pubkey: &str,
    approve: bool,
) -> Result<(), CliError> {
    validate_hex64(pubkey)?;
    let target = json!({ "pubkey": pubkey });
    if !approve {
        approval_plan(&format!("{action}_managed_agent"), target);
        return Ok(());
    }
    request(
        descriptor_path,
        Method::POST,
        &format!("v1/managed-agents/{pubkey}/{action}"),
        Some(json!({ "approve": true })),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_override_wins() {
        let path = Path::new("/tmp/custom-control.json");
        assert_eq!(resolve_descriptor_path(Some(path)).unwrap(), path);
    }

    #[test]
    fn control_url_is_fail_closed_to_loopback_http() {
        assert!(validate_loopback_url("http://127.0.0.1:1234").is_ok());
        assert!(validate_loopback_url("http://[::1]:1234").is_ok());
        assert!(validate_loopback_url("http://localhost:1234").is_err());
        assert!(validate_loopback_url("https://127.0.0.1:1234").is_err());
        assert!(validate_loopback_url("http://example.com:1234").is_err());
    }

    #[test]
    fn short_descriptor_tokens_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("control.json");
        std::fs::write(
            &path,
            r#"{"schema_version":1,"url":"http://127.0.0.1:1234","token":"short"}"#,
        )
        .unwrap();
        assert!(matches!(load_descriptor(&path), Err(CliError::Auth(_))));
    }

    #[test]
    fn agent_specs_require_nonempty_name_and_command() {
        let parsed = parse_agent_specs(&[
            "Dokploy-Codex=codex".to_string(),
            "Dokploy-Claude=claude".to_string(),
        ])
        .unwrap();
        assert_eq!(
            parsed,
            vec![
                AgentSpec {
                    name: "Dokploy-Codex".to_string(),
                    command: "codex".to_string(),
                },
                AgentSpec {
                    name: "Dokploy-Claude".to_string(),
                    command: "claude".to_string(),
                }
            ]
        );
        assert!(parse_agent_specs(&["codex".to_string()]).is_err());
        assert!(parse_agent_specs(&["=codex".to_string()]).is_err());
        assert!(parse_agent_specs(&["Codex=".to_string()]).is_err());
    }

    #[test]
    fn canvas_records_exact_context_and_roster() {
        let agents = vec![AgentSpec {
            name: "Dokploy-Codex".to_string(),
            command: "codex".to_string(),
        }];
        let content = canvas_content("dokploy", Path::new("/srv/dokploy"), &agents);
        assert!(content.contains("`/srv/dokploy`"));
        assert!(content.contains("**Dokploy-Codex** — `codex`"));
    }
}
