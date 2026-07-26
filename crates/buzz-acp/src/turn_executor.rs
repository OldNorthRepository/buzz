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
use std::path::PathBuf;
use std::time::Duration;

use gateway_api::client::{ClientError, ControlClient, Endpoint};
use gateway_api::protocol::SubmitJobRequest;
use tokio::sync::Mutex;
use uuid::Uuid;

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
    Completed { summary: String },
    Failed { reason: String },
    Canceled,
    /// The executor cannot prove what happened (gateway unreachable mid-poll,
    /// or the job itself resolved to `unknown_outcome`). Callers must not
    /// blindly retry: the turn's effects may have happened.
    UnknownOutcome { reason: String },
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

/// Turn executor backed by a host agent-gateway daemon.
pub struct GatewayExecutor {
    client: ControlClient,
    route_id: String,
    /// turn_id -> gateway job id, for cancellation. Entries are removed when
    /// a turn reaches a terminal outcome.
    jobs: Mutex<HashMap<String, String>>,
}

impl GatewayExecutor {
    /// Connects, authenticates, and verifies the configured route exists so a
    /// misconfigured socket or missing route fails at startup, not at first
    /// dispatch.
    pub async fn connect(socket: PathBuf, route_id: String) -> Result<Self, GatewayExecutorError> {
        let client = ControlClient::connect(Endpoint::Socket(socket)).await?;
        client.status().await?;
        let routes = client.routes().await?;
        if !routes.routes.iter().any(|route| route.id == route_id) {
            return Err(GatewayExecutorError::RouteMissing(route_id));
        }
        Ok(Self {
            client,
            route_id,
            jobs: Mutex::new(HashMap::new()),
        })
    }

    pub fn route_id(&self) -> &str {
        &self.route_id
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
        let request = SubmitJobRequest {
            route_id: Some(self.route_id.clone()),
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

    async fn cancel_turn(&self, turn_id: &str) -> bool {
        let job_id = self.jobs.lock().await.get(turn_id).cloned();
        match job_id {
            Some(job_id) => self.client.cancel(&job_id).await.is_ok(),
            None => false,
        }
    }
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
            reason: reason.unwrap_or("gateway reported unknown outcome").to_owned(),
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_terminal_states_keep_polling() {
        for state in ["received", "queued", "running", "starting_worker", "completing"] {
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
        assert_eq!(map_job_state("canceled", None, None), Some(TurnOutcome::Canceled));
        assert!(matches!(
            map_job_state("unknown_outcome", Some("outcome unknown after daemon crash"), None),
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
