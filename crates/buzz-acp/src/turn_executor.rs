//! Execution-backend seam: where a turn actually runs.
//!
//! `local` is today's in-process ACP subprocess pool. `gateway` submits the
//! turn as a job to a host agent-gateway daemon over its authenticated Unix
//! socket and awaits the journaled terminal state. The job payload is the
//! raw queued relay events for the channel — the gateway worker (a buzz-acp
//! GWP worker mode, follow-up change) rebuilds the prompt with the same
//! assembly code the local pool uses, so prompt semantics never fork between
//! backends.
//!
//! Steering: the gateway has no mid-turn channel (GWP/0), so a steer request
//! against a gateway turn must take the existing cancel+merge fallback path.
//! `cancel_turn` maps to the gateway's cancel endpoint, whose semantics are
//! truthful for effectful jobs (a running `external_effects` job lands in
//! `reconciling`, never a fabricated clean cancel).

use std::collections::HashMap;
use std::time::Duration;

use gateway_api::client::{ClientError, ControlClient, Endpoint};
use gateway_api::protocol::SubmitJobRequest;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::config::{AutoRouteConfig, GatewayBackendConfig};
use crate::pool::PromptContext;
use crate::queue::{BatchEvent, CancelReason, FlushBatch};

/// How often the executor polls a submitted job for its terminal state.
const POLL_INTERVAL: Duration = Duration::from_millis(250);
/// Consecutive poll failures tolerated before the turn is declared unknown.
/// The job may still be running on the gateway; `UnknownOutcome` is the
/// truthful classification, mirroring the gateway's own crash semantics.
const MAX_CONSECUTIVE_POLL_FAILURES: u32 = 20;

/// Terminal result of one turn, backend-agnostic.
#[derive(Debug, Clone, PartialEq)]
pub enum TurnOutcome {
    /// The turn ran to completion. `summary` is backend metadata (for the
    /// gateway backend, the worker's result summary), not user-visible
    /// content — agents publish their replies to the relay themselves.
    Completed {
        summary: String,
    },
    Failed {
        reason: String,
    },
    Canceled,
    /// The executor cannot prove what happened (gateway unreachable mid-poll,
    /// or the job itself resolved to `unknown_outcome`). Callers must not
    /// blindly retry: the turn's effects may have happened.
    UnknownOutcome {
        reason: String,
    },
}

/// The seam `dispatch_pending` will program against once local execution is
/// also expressed as an executor. Async-in-trait keeps this simple; the
/// harness selects a backend at startup, so no trait objects are needed.
pub trait TurnExecutor: Send + Sync {
    fn run_turn(
        &self,
        channel_id: Uuid,
        turn_id: &str,
        payload: serde_json::Value,
        timeout_ms: Option<i64>,
    ) -> impl std::future::Future<Output = TurnOutcome> + Send;

    /// Best-effort cancel; `true` means the gateway acknowledged it.
    fn cancel_turn(&self, turn_id: &str) -> impl std::future::Future<Output = bool> + Send;
}

#[derive(Debug, thiserror::Error)]
pub enum GatewayExecutorError {
    #[error("gateway connection failed: {0}")]
    Connect(#[from] ClientError),
    #[error("gateway has no route '{0}' — create it with `agent-gateway routes set` first")]
    RouteMissing(String),
}

/// Completion of one gateway-dispatched turn, delivered to the main loop on
/// the receiver returned by [`GatewayExecutor::connect`].
pub(crate) struct GatewayTurnDone {
    pub channel_id: Uuid,
    pub turn_id: String,
    pub outcome: TurnOutcome,
    /// The dispatched batch, kept for failure notices.
    pub batch: FlushBatch,
}

/// Turn executor backed by a host agent-gateway daemon.
pub struct GatewayExecutor {
    client: ControlClient,
    /// Default route for channels with no explicit or auto mapping.
    route_id: String,
    /// Explicit channel -> route overrides from configuration.
    channel_routes: HashMap<Uuid, String>,
    /// Name-convention auto-routing, when enabled.
    auto_route: Option<AutoRouteConfig>,
    /// Agent principal of the default route; auto-created routes reuse it.
    agent_id: String,
    /// channel -> resolved route decisions (Some(route) or None = default),
    /// so name resolution and route creation happen once per channel.
    resolved_routes: Mutex<HashMap<Uuid, Option<String>>>,
    /// turn_id -> gateway job id, for cancellation. Entries are removed when
    /// a turn reaches a terminal outcome.
    jobs: Mutex<HashMap<String, String>>,
    /// channel_id -> in-flight turn_id, for `!cancel` routing.
    channels: Mutex<HashMap<Uuid, String>>,
    notify: tokio::sync::mpsc::UnboundedSender<GatewayTurnDone>,
}

/// Turns a channel name into a workspace directory slug: lowercased, with
/// anything outside [a-z0-9-] collapsed to single dashes.
pub(crate) fn channel_slug(name: &str) -> String {
    let mut slug = String::with_capacity(name.len());
    let mut dash = true;
    for ch in name.chars() {
        let ch = ch.to_ascii_lowercase();
        if ch.is_ascii_alphanumeric() {
            slug.push(ch);
            dash = false;
        } else if !dash {
            slug.push('-');
            dash = true;
        }
    }
    slug.trim_end_matches('-').to_owned()
}

impl GatewayExecutor {
    /// Connects, authenticates, and verifies the configured route exists so a
    /// misconfigured socket or missing route fails at startup, not at first
    /// dispatch.
    pub(crate) async fn connect(
        config: GatewayBackendConfig,
    ) -> Result<(Self, tokio::sync::mpsc::UnboundedReceiver<GatewayTurnDone>), GatewayExecutorError>
    {
        let client = ControlClient::connect(Endpoint::Socket(config.socket)).await?;
        client.status().await?;
        let routes = client.routes().await?;
        let Some(default_route) = routes
            .routes
            .iter()
            .find(|route| route.id == config.route_id)
        else {
            return Err(GatewayExecutorError::RouteMissing(config.route_id));
        };
        for route in config.channel_routes.values() {
            if !routes.routes.iter().any(|view| &view.id == route) {
                return Err(GatewayExecutorError::RouteMissing(route.clone()));
            }
        }
        let (notify, done_rx) = tokio::sync::mpsc::unbounded_channel();
        Ok((
            Self {
                client,
                agent_id: default_route.agent_id.clone(),
                route_id: config.route_id,
                channel_routes: config.channel_routes,
                auto_route: config.auto_route,
                resolved_routes: Mutex::new(HashMap::new()),
                jobs: Mutex::new(HashMap::new()),
                channels: Mutex::new(HashMap::new()),
                notify,
            },
            done_rx,
        ))
    }

    /// The route a channel's turns run on: explicit mapping first, then the
    /// name-convention auto route (created on the gateway on first use), then
    /// the default. Decisions are cached per channel.
    async fn route_for(&self, channel_id: Uuid, channel_name: Option<&str>) -> String {
        if let Some(route) = self.channel_routes.get(&channel_id) {
            return route.clone();
        }
        if let Some(decision) = self.resolved_routes.lock().await.get(&channel_id) {
            return decision.clone().unwrap_or_else(|| self.route_id.clone());
        }
        let decision = match (&self.auto_route, channel_name) {
            (Some(auto), Some(name)) => self.ensure_auto_route(channel_id, auto, name).await,
            _ => None,
        };
        self.resolved_routes
            .lock()
            .await
            .insert(channel_id, decision.clone());
        decision.unwrap_or_else(|| self.route_id.clone())
    }

    async fn ensure_auto_route(
        &self,
        channel_id: Uuid,
        auto: &AutoRouteConfig,
        name: &str,
    ) -> Option<String> {
        let slug = channel_slug(name);
        if slug.is_empty() {
            return None;
        }
        let workspace = auto.root.join(&slug);
        if !workspace.is_dir() {
            tracing::info!(
                channel = %channel_id, %slug,
                "no workspace directory for channel; using the default route"
            );
            return None;
        }
        let route_id = format!("ch-{slug}");
        match self.client.routes().await {
            Ok(routes) if routes.routes.iter().any(|route| route.id == route_id) => {
                return Some(route_id);
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(%error, "route listing failed; using the default route");
                return None;
            }
        }
        let request = gateway_api::protocol::SetRouteRequest {
            id: route_id.clone(),
            agent_id: self.agent_id.clone(),
            contexts: vec![channel_id.to_string()],
            runtime: auto.runtime.clone(),
            model: None,
            workspace_path: workspace.display().to_string(),
            session_mode: None,
            permission_profile: "repository_read".into(),
            repo_write_concurrency: None,
            repo_read_concurrency: None,
            agent_concurrency: None,
            env_allowlist: std::collections::BTreeMap::new(),
        };
        match self.client.set_route(request).await {
            Ok(_) => {
                tracing::info!(
                    channel = %channel_id, route = %route_id, workspace = %workspace.display(),
                    "auto-created gateway route for channel"
                );
                Some(route_id)
            }
            Err(error) => {
                tracing::warn!(%error, route = %route_id,
                    "auto route creation failed; using the default route");
                None
            }
        }
    }

    pub fn route_id(&self) -> &str {
        &self.route_id
    }

    /// Fire-and-forget dispatch of one flushed batch; completion arrives on
    /// the receiver returned by `connect`. Returns the turn id.
    pub(crate) fn spawn_turn(
        self: &std::sync::Arc<Self>,
        batch: FlushBatch,
        ctx: std::sync::Arc<PromptContext>,
    ) -> String {
        let turn_id = uuid::Uuid::new_v4().to_string();
        let payload = encode_turn_payload(&batch);
        let executor = std::sync::Arc::clone(self);
        let id = turn_id.clone();
        tokio::spawn(async move {
            let channel_id = batch.channel_id;
            let channel_name = ctx
                .channel_info
                .resolve(channel_id)
                .await
                .map(|info| info.name);
            let route = executor
                .route_for(channel_id, channel_name.as_deref())
                .await;
            executor
                .channels
                .lock()
                .await
                .insert(channel_id, id.clone());
            let outcome = executor
                .run_turn_on(&route, channel_id, &id, payload, None)
                .await;
            executor.channels.lock().await.remove(&channel_id);
            let _ = executor.notify.send(GatewayTurnDone {
                channel_id,
                turn_id: id,
                outcome,
                batch,
            });
        });
        turn_id
    }

    /// `!cancel` entry point: cancels the channel's in-flight gateway turn,
    /// if any. Completion still arrives through the done channel (the job
    /// resolves to canceled/reconciling and the poll loop reports it).
    pub(crate) async fn cancel_channel(&self, channel_id: Uuid) -> bool {
        let turn = self.channels.lock().await.get(&channel_id).cloned();
        match turn {
            Some(turn_id) => self.cancel_turn(&turn_id).await,
            None => false,
        }
    }
}

impl GatewayExecutor {
    async fn run_turn_on(
        &self,
        route_id: &str,
        channel_id: Uuid,
        turn_id: &str,
        payload: serde_json::Value,
        timeout_ms: Option<i64>,
    ) -> TurnOutcome {
        let request = SubmitJobRequest {
            route_id: Some(route_id.to_owned()),
            agent_id: None,
            context_id: None,
            thread_id: Some(channel_id.to_string()),
            task: payload.to_string(),
            permission_profile: None,
            // Agent turns publish to the relay: real external effects, never
            // replayable. The gateway will reconcile, not rerun, on a crash.
            side_effect_class: Some("external_effects".into()),
            priority: None,
            timeout_ms,
            // The turn id makes resubmission after a harness restart dedup
            // onto the same job instead of double-running the turn.
            idempotency_key: Some(turn_id.to_string()),
            source_id: None,
        };
        let submitted = match self.client.submit(request).await {
            Ok(response) => response,
            Err(error) => {
                return TurnOutcome::Failed {
                    reason: format!("gateway submit failed: {error}"),
                };
            }
        };
        self.jobs
            .lock()
            .await
            .insert(turn_id.to_string(), submitted.job_id.clone());

        let mut consecutive_failures = 0u32;
        let outcome = loop {
            tokio::time::sleep(POLL_INTERVAL).await;
            match self.client.job(&submitted.job_id).await {
                Ok(job) => {
                    consecutive_failures = 0;
                    if let Some(outcome) = map_job_state(
                        &job.state,
                        job.reason.as_deref(),
                        job.result.as_ref().map(|result| result.summary.as_str()),
                    ) {
                        break outcome;
                    }
                }
                Err(error) => {
                    consecutive_failures += 1;
                    if consecutive_failures >= MAX_CONSECUTIVE_POLL_FAILURES {
                        break TurnOutcome::UnknownOutcome {
                            reason: format!(
                                "lost contact with gateway while polling job {}: {error}",
                                submitted.job_id
                            ),
                        };
                    }
                }
            }
        };
        self.jobs.lock().await.remove(turn_id);
        outcome
    }
}

impl TurnExecutor for GatewayExecutor {
    async fn run_turn(
        &self,
        channel_id: Uuid,
        turn_id: &str,
        payload: serde_json::Value,
        timeout_ms: Option<i64>,
    ) -> TurnOutcome {
        let route = self.route_id.clone();
        self.run_turn_on(&route, channel_id, turn_id, payload, timeout_ms)
            .await
    }

    async fn cancel_turn(&self, turn_id: &str) -> bool {
        let job_id = self.jobs.lock().await.get(turn_id).cloned();
        match job_id {
            Some(job_id) => self.client.cancel(&job_id).await.is_ok(),
            None => false,
        }
    }
}

/// Wire form of one turn: the raw queued relay events for a channel. The GWP
/// worker rebuilds a [`FlushBatch`] from this and runs the same prompt
/// assembly as the local pool, so prompt semantics never fork.
pub(crate) fn encode_turn_payload(batch: &FlushBatch) -> serde_json::Value {
    fn event(entry: &BatchEvent) -> serde_json::Value {
        serde_json::json!({"event": entry.event, "prompt_tag": entry.prompt_tag})
    }
    serde_json::json!({
        "v": 1,
        "channel_id": batch.channel_id,
        "events": batch.events.iter().map(event).collect::<Vec<_>>(),
        "cancelled_events": batch.cancelled_events.iter().map(event).collect::<Vec<_>>(),
        "cancel_reason": batch.cancel_reason.map(|reason| match reason {
            CancelReason::Interrupt => "interrupt",
            CancelReason::Steer => "steer",
        }),
    })
}

pub(crate) fn decode_turn_payload(task: &str) -> Result<FlushBatch, String> {
    let value: serde_json::Value =
        serde_json::from_str(task).map_err(|e| format!("payload is not JSON: {e}"))?;
    if value.get("v").and_then(serde_json::Value::as_u64) != Some(1) {
        return Err("unsupported payload version".into());
    }
    let channel_id = value
        .get("channel_id")
        .and_then(serde_json::Value::as_str)
        .and_then(|raw| Uuid::parse_str(raw).ok())
        .ok_or("payload has no channel_id")?;
    fn events(value: Option<&serde_json::Value>) -> Result<Vec<BatchEvent>, String> {
        let Some(entries) = value.and_then(serde_json::Value::as_array) else {
            return Ok(Vec::new());
        };
        entries
            .iter()
            .map(|entry| {
                let event = serde_json::from_value(
                    entry.get("event").cloned().ok_or("entry has no event")?,
                )
                .map_err(|e| format!("bad nostr event: {e}"))?;
                Ok(BatchEvent {
                    event,
                    prompt_tag: entry
                        .get("prompt_tag")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    // Wall-clock queueing latency does not survive the hop;
                    // the worker measures from receipt, which is what its own
                    // metrics mean anyway.
                    received_at: std::time::Instant::now(),
                })
            })
            .collect()
    }
    let cancel_reason = match value
        .get("cancel_reason")
        .and_then(serde_json::Value::as_str)
    {
        Some("interrupt") => Some(CancelReason::Interrupt),
        Some("steer") => Some(CancelReason::Steer),
        Some(other) => return Err(format!("unknown cancel_reason '{other}'")),
        None => None,
    };
    Ok(FlushBatch {
        channel_id,
        events: events(value.get("events"))?,
        cancelled_events: events(value.get("cancelled_events"))?,
        cancel_reason,
    })
}

/// Maps a gateway job state to a turn outcome. `None` = not terminal yet.
pub fn map_job_state(
    state: &str,
    reason: Option<&str>,
    summary: Option<&str>,
) -> Option<TurnOutcome> {
    match state {
        "completed" => Some(TurnOutcome::Completed {
            summary: summary.unwrap_or_default().to_owned(),
        }),
        "failed" | "rejected" => Some(TurnOutcome::Failed {
            reason: summary
                .filter(|text| !text.is_empty())
                .or(reason)
                .unwrap_or("gateway job failed")
                .to_owned(),
        }),
        "canceled" => Some(TurnOutcome::Canceled),
        "unknown_outcome" | "reconciling" => Some(TurnOutcome::UnknownOutcome {
            reason: reason
                .unwrap_or("gateway reported unknown outcome")
                .to_owned(),
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn turn_payload_roundtrips_through_encode_and_decode() {
        let keys = nostr::Keys::generate();
        let event = nostr::EventBuilder::text_note("hello gateway")
            .sign_with_keys(&keys)
            .unwrap();
        let channel = Uuid::new_v4();
        let batch = FlushBatch {
            channel_id: channel,
            events: vec![BatchEvent {
                event: event.clone(),
                prompt_tag: "mention".into(),
                received_at: std::time::Instant::now(),
            }],
            cancelled_events: vec![],
            cancel_reason: Some(CancelReason::Steer),
        };
        let decoded = decode_turn_payload(&encode_turn_payload(&batch).to_string()).unwrap();
        assert_eq!(decoded.channel_id, channel);
        assert_eq!(decoded.events.len(), 1);
        assert_eq!(decoded.events[0].event.id, event.id);
        assert_eq!(decoded.events[0].prompt_tag, "mention");
        assert_eq!(decoded.cancel_reason, Some(CancelReason::Steer));
    }

    #[test]
    fn a_garbage_payload_is_rejected_not_panicked() {
        assert!(decode_turn_payload("not json").is_err());
        assert!(decode_turn_payload("{}").is_err());
        assert!(decode_turn_payload("{\"v\":2,\"channel_id\":\"x\"}").is_err());
    }

    #[test]
    fn channel_slugs_follow_the_directory_convention() {
        assert_eq!(channel_slug("kanban"), "kanban");
        assert_eq!(channel_slug("Agent Gateway"), "agent-gateway");
        assert_eq!(channel_slug("  Boiler!! Room  "), "boiler-room");
        assert_eq!(channel_slug("émoji 🔥 room"), "moji-room");
        assert_eq!(channel_slug("---"), "");
    }

    #[test]
    fn non_terminal_states_keep_polling() {
        for state in [
            "received",
            "queued",
            "running",
            "starting_worker",
            "completing",
        ] {
            assert_eq!(map_job_state(state, None, None), None, "state {state}");
        }
    }

    #[test]
    fn completed_carries_the_worker_summary() {
        assert_eq!(
            map_job_state("completed", None, Some("stop: end_turn")),
            Some(TurnOutcome::Completed {
                summary: "stop: end_turn".into()
            })
        );
    }

    #[test]
    fn failed_prefers_the_worker_message_over_the_transition_reason() {
        assert_eq!(
            map_job_state(
                "failed",
                Some("non-retry-safe attempt failed"),
                Some("worker reported turn failure: exit 1")
            ),
            Some(TurnOutcome::Failed {
                reason: "worker reported turn failure: exit 1".into()
            })
        );
        assert_eq!(
            map_job_state("failed", Some("route membership removed"), None),
            Some(TurnOutcome::Failed {
                reason: "route membership removed".into()
            })
        );
    }

    #[test]
    fn cancellation_and_unknown_outcomes_are_distinct() {
        assert_eq!(
            map_job_state("canceled", None, None),
            Some(TurnOutcome::Canceled)
        );
        assert!(matches!(
            map_job_state(
                "unknown_outcome",
                Some("outcome unknown after daemon crash"),
                None
            ),
            Some(TurnOutcome::UnknownOutcome { .. })
        ));
        // A cancelled effectful job parks in `reconciling`; that is an
        // unknown outcome for the harness, not a clean cancel.
        assert!(matches!(
            map_job_state("reconciling", None, None),
            Some(TurnOutcome::UnknownOutcome { .. })
        ));
    }
}
