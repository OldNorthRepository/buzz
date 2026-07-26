//! GWP/0 worker mode: one buzz-acp process = one agent-gateway worker.
//!
//! The gateway daemon spawns this process from a route's runtime command and
//! drives it over stdin/stdout with newline-delimited JSON frames. Each turn's
//! task is an encoded event batch (see `turn_executor::decode_turn_payload`);
//! the worker rebuilds the `FlushBatch` and runs the *same* `run_prompt_task`
//! the local pool uses — context fetches, memory, reactions, and ACP session
//! handling included — so prompt semantics never fork between backends.
//!
//! Invariants:
//! - stdout carries only GWP frames; all diagnostics go to stderr (the
//!   caller routes `tracing` there before this runs).
//! - `ready` is emitted immediately after `hello`: the gateway's handshake
//!   window (5 s) is far shorter than an ACP agent cold start, so the agent
//!   subprocess spawns lazily under the first turn's deadline instead.
//! - One worker owns exactly one ACP agent. Session state persists across
//!   turns of the same worker, which is what makes gateway warm reuse a real
//!   session continuation.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

use crate::config::Config;
use crate::pool::{self, OwnedAgent, PromptContext, PromptOutcome, PromptResult};
use crate::relay::{RestClient, relay_ws_to_http};
use crate::turn_executor::decode_turn_payload;
use crate::{PoolStartup, build_mcp_servers, initialize_agent_pool};

pub async fn run(mut config: Config) -> anyhow::Result<()> {
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut out = tokio::io::stdout();

    let hello_line = lines
        .next_line()
        .await?
        .ok_or_else(|| anyhow::anyhow!("stdin closed before hello"))?;
    let hello: Value = serde_json::from_str(&hello_line)?;
    if hello.get("type").and_then(Value::as_str) != Some("hello")
        || hello.get("protocol").and_then(Value::as_str) != Some("gwp/0")
    {
        anyhow::bail!("first frame was not a gwp/0 hello");
    }

    let session = uuid::Uuid::new_v4().to_string();
    let runtime_name = crate::config::normalize_agent_command_identity(&config.agent_command);
    write_frame(
        &mut out,
        &json!({"type":"ready","protocol":"gwp/0","runtime":runtime_name,"session":session}),
    )
    .await?;
    tracing::info!(runtime = %runtime_name, "gwp worker ready; acp agent spawns on first turn");

    // Mirrors the harness's PromptContext, with two deliberate differences:
    // the REST client is built standalone (no relay WebSocket exists here,
    // and REST consumers are documented fail-open), and the channel-info
    // cache starts empty (the resolver fetches lazily over REST).
    let base_prompt_content = config.base_prompt_content.take();
    let rest_client = RestClient {
        http: reqwest::Client::new(),
        base_url: relay_ws_to_http(&config.relay_url),
        keys: config.keys.clone(),
        auth_tag_json: None,
    };
    let ctx = Arc::new(PromptContext {
        mcp_servers: build_mcp_servers(&config),
        initial_message: config.initial_message.clone(),
        idle_timeout: Duration::from_secs(config.idle_timeout_secs),
        max_turn_duration: Duration::from_secs(config.max_turn_duration_secs),
        turn_liveness_interval: Duration::from_secs(config.turn_liveness_secs),
        dedup_mode: config.dedup_mode,
        system_prompt: config.system_prompt.clone(),
        team_instructions: config.team_instructions.clone(),
        base_prompt: if config.no_base_prompt {
            None
        } else if let Some(content) = base_prompt_content {
            Some(Box::leak(content.into_boxed_str()))
        } else {
            Some(include_str!("base_prompt.md"))
        },
        heartbeat_prompt: config.heartbeat_prompt.clone(),
        // The gateway sets the process cwd to the route's workspace, which
        // is exactly the per-repo isolation the local pool never had.
        cwd: std::env::current_dir()
            .unwrap_or_else(|_| std::path::PathBuf::from("/"))
            .to_string_lossy()
            .to_string(),
        rest_client: rest_client.clone(),
        channel_info: pool::ChannelInfoResolver::new(std::collections::HashMap::new(), rest_client),
        context_message_limit: config.context_message_limit,
        max_turns_per_session: config.max_turns_per_session,
        permission_mode: config.permission_mode,
        agent_keys: config.keys.clone(),
        agent_owner_pubkey: config
            .agent_owner
            .as_deref()
            .and_then(|hex| nostr::PublicKey::from_hex(hex).ok()),
        memory_enabled: config.memory_enabled,
        harness_name: runtime_name.clone(),
        relay_url: config.relay_url.clone(),
        gateway_executor: None,
    });

    let startup = PoolStartup {
        // One worker, one agent — parallelism is the gateway's job now.
        agents: 1,
        command: config.agent_command.clone(),
        args: config.agent_args.clone(),
        extra_env: config.persona_env_vars.clone(),
        has_generated_codex_config: config.has_generated_codex_config,
        model: config.model.clone(),
        observer: None,
    };
    let (result_tx, mut result_rx) = mpsc::unbounded_channel::<PromptResult>();
    let mut agent: Option<OwnedAgent> = None;

    while let Some(line) = lines.next_line().await? {
        let frame: Value = serde_json::from_str(&line)?;
        match frame.get("type").and_then(Value::as_str) {
            Some("turn") => {
                let turn_id = frame
                    .get("turn_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let task = frame.get("task").and_then(Value::as_str).unwrap_or_default();
                let batch = match decode_turn_payload(task) {
                    Ok(batch) => batch,
                    Err(reason) => {
                        write_frame(
                            &mut out,
                            &json!({"type":"error","turn_id":turn_id,
                                    "message":format!("bad turn payload: {reason}"),
                                    "retryable":false}),
                        )
                        .await?;
                        continue;
                    }
                };
                let current = match agent.take() {
                    Some(existing) => existing,
                    None => match spawn_agent(&startup).await {
                        Ok(spawned) => spawned,
                        Err(error) => {
                            write_frame(
                                &mut out,
                                &json!({"type":"error","turn_id":turn_id,
                                        "message":format!("acp agent spawn failed: {error}"),
                                        "retryable":false}),
                            )
                            .await?;
                            continue;
                        }
                    },
                };
                pool::run_prompt_task(
                    current,
                    Some(batch),
                    None,
                    Arc::clone(&ctx),
                    result_tx.clone(),
                    None,
                    turn_id.clone(),
                )
                .await;
                let Some(result) = result_rx.recv().await else {
                    anyhow::bail!("prompt result channel closed");
                };
                let mut agent_unusable = false;
                let reply = match &result.outcome {
                    PromptOutcome::Ok(stop) => json!({
                        "type":"result","turn_id":turn_id,
                        "summary":format!("stop_reason={stop:?}"),
                        "structured":{"stopReason":format!("{stop:?}")},
                        "session":session,
                    }),
                    PromptOutcome::Error(error) => json!({
                        "type":"error","turn_id":turn_id,
                        "message":format!("acp error: {error}"),"retryable":false,
                    }),
                    PromptOutcome::AgentExited => {
                        agent_unusable = true;
                        json!({"type":"error","turn_id":turn_id,
                               "message":"acp agent process exited","retryable":false})
                    }
                    PromptOutcome::Timeout(kind) => json!({
                        "type":"error","turn_id":turn_id,
                        "message":format!("turn timed out: {kind:?}"),"retryable":false,
                    }),
                    PromptOutcome::Cancelled => {
                        json!({"type":"canceled","turn_id":turn_id})
                    }
                    PromptOutcome::CancelDrainTimeout(grace) => {
                        agent_unusable = true;
                        json!({"type":"error","turn_id":turn_id,
                               "message":format!("cancel drain exceeded {grace:?}; agent poisoned"),
                               "retryable":false})
                    }
                };
                write_frame(&mut out, &reply).await?;
                if agent_unusable {
                    // The gateway tears a worker down after any failed turn;
                    // exiting here just makes that teardown immediate instead
                    // of leaving a dead ACP child behind a live worker.
                    anyhow::bail!("acp agent no longer usable; exiting for gateway respawn");
                }
                agent = Some(result.agent);
            }
            Some("cancel") => {
                // Turns run serially in this loop, so a cancel frame can only
                // arrive between turns; acknowledge it as already satisfied.
                let turn_id = frame
                    .get("turn_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                write_frame(&mut out, &json!({"type":"canceled","turn_id":turn_id})).await?;
            }
            other => anyhow::bail!("unexpected frame type {other:?}"),
        }
    }

    if let Some(mut remaining) = agent {
        remaining.acp.shutdown().await;
    }
    Ok(())
}

async fn spawn_agent(startup: &PoolStartup) -> anyhow::Result<OwnedAgent> {
    let mut pool = initialize_agent_pool(startup, None).await?;
    for slot in pool.agents_mut() {
        if let Some(agent) = slot.take() {
            return Ok(agent);
        }
    }
    anyhow::bail!("acp agent failed to initialize")
}

async fn write_frame(out: &mut tokio::io::Stdout, frame: &Value) -> anyhow::Result<()> {
    let line = serde_json::to_vec(frame)?;
    out.write_all(&line).await?;
    out.write_all(b"\n").await?;
    out.flush().await?;
    Ok(())
}
