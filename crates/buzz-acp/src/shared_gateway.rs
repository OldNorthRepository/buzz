//! Buzz ingress for the daemon-owned AGW-SHARED/1 session protocol.
//!
//! The relay event IDs determine the submission ID. Once a submit may have
//! reached the daemon, this client only looks it up; it never sends it again.

use std::collections::VecDeque;
use std::io;
use std::path::Path;
use std::time::Duration;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::watch;

use crate::pool::PromptContext;
use crate::queue::{FlushBatch, FormatPromptArgs};
use crate::turn_executor::TurnOutcome;

const MAX_FRAME: usize = 1024 * 1024;
const MAX_PENDING_EVENTS: usize = 128;
const RECONNECT_ATTEMPTS: usize = 120;

pub(crate) fn submission_id(route: &str, batch: &FlushBatch) -> String {
    let mut hash = Sha256::new();
    hash.update(b"buzz-agw-shared-v1\0");
    hash.update(route.as_bytes());
    hash.update(b"\0");
    for event in &batch.events {
        hash.update(event.event.id.to_hex().as_bytes());
    }
    format!("buzz-{}", hex::encode(hash.finalize()))
}

pub(crate) struct Connection {
    stream: UnixStream,
    next_id: u64,
    pending: VecDeque<Value>,
}

impl Connection {
    pub(crate) async fn open(path: &Path, route: &str) -> io::Result<Self> {
        if route.is_empty()
            || route.len() > 128
            || !route
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid shared route",
            ));
        }
        let mut stream = tokio::time::timeout(Duration::from_secs(5), UnixStream::connect(path))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "shared connect timed out"))??;
        stream.write_all(b"AGW-SHARED/1\n").await?;
        stream.write_all(route.as_bytes()).await?;
        stream.write_all(b"\n").await?;
        let answer = tokio::time::timeout(Duration::from_secs(5), read_line(&mut stream, 128))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "shared admission timed out"))??;
        if answer != b"OK" {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "shared route refused",
            ));
        }
        Ok(Self {
            stream,
            next_id: 0,
            pending: VecDeque::new(),
        })
    }

    pub(crate) async fn request(&mut self, op: Value) -> io::Result<Value> {
        self.next_id = self.next_id.checked_add(1).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "shared request ID exhausted")
        })?;
        let id = self.next_id.to_string();
        let mut request = op;
        request
            .as_object_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid request"))?
            .insert("id".into(), Value::String(id.clone()));
        let bytes = serde_json::to_vec(&request).map_err(io::Error::other)?;
        if bytes.len() > MAX_FRAME {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "shared request too large",
            ));
        }
        self.stream.write_all(&bytes).await?;
        self.stream.write_all(b"\n").await?;
        loop {
            let frame = tokio::time::timeout(Duration::from_secs(10), self.read())
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "shared reply timed out"))??;
            match frame.get("type").and_then(Value::as_str) {
                Some("reply") if frame["id"] == id => return Ok(frame["result"].clone()),
                Some("refusal") if frame["id"] == id => {
                    let reason = frame["reason"].as_str().unwrap_or("unknown");
                    let kind = if reason == "busy" {
                        io::ErrorKind::WouldBlock
                    } else {
                        io::ErrorKind::PermissionDenied
                    };
                    return Err(io::Error::new(
                        kind,
                        format!("shared gateway refused: {reason}"),
                    ));
                }
                Some("event" | "gap") => {
                    if self.pending.len() == MAX_PENDING_EVENTS {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "shared event queue overflow",
                        ));
                    }
                    self.pending.push_back(frame);
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unexpected shared frame",
                    ))
                }
            }
        }
    }

    pub(crate) async fn next_event(&mut self) -> io::Result<Value> {
        if let Some(frame) = self.pending.pop_front() {
            Ok(frame)
        } else {
            self.read().await
        }
    }

    async fn read(&mut self) -> io::Result<Value> {
        let line = read_line(&mut self.stream, MAX_FRAME).await?;
        serde_json::from_slice(&line).map_err(io::Error::other)
    }
}

async fn read_line(stream: &mut UnixStream, limit: usize) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    loop {
        let byte = stream.read_u8().await?;
        if byte == b'\n' {
            return Ok(bytes);
        }
        if bytes.len() == limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "shared frame too large",
            ));
        }
        bytes.push(byte);
    }
}

async fn prompt(batch: &FlushBatch, ctx: &PromptContext) -> String {
    let channel = ctx
        .channel_info
        .resolve(batch.channel_id)
        .await
        .ok()
        .flatten();
    let mut sections = crate::queue::format_prompt(
        batch,
        &FormatPromptArgs {
            channel_info: channel.as_ref(),
            // The normal Buzz base prompt explicitly commands the agent to
            // publish through buzz CLI. This backend owns publication, so
            // carrying that standing context would risk a duplicate reply.
            base_prompt: None,
            system_prompt: ctx.system_prompt.as_deref(),
            team_instructions: ctx.team_instructions.as_deref(),
            harness_publishes_reply: true,
            ..FormatPromptArgs::default()
        },
    );
    sections.push("Reply in your ACP response text. Buzz will publish your reply to the originating channel. Do not send a duplicate reply with the Buzz CLI.".into());
    sections.join("\n\n")
}

fn task_id(value: &Value) -> Option<&str> {
    value.get("task_id").and_then(Value::as_str)
}

fn terminal(state: &str) -> Option<TurnOutcome> {
    match state {
        "completed" => Some(TurnOutcome::Completed {
            summary: "shared turn completed".into(),
        }),
        "canceled" => Some(TurnOutcome::Canceled),
        "failed" => Some(TurnOutcome::Failed {
            reason: "shared turn failed".into(),
        }),
        "unknown_outcome" => Some(TurnOutcome::UnknownOutcome {
            reason: "shared turn outcome unknown".into(),
        }),
        _ => None,
    }
}

async fn task_state(connection: &mut Connection, id: &str) -> io::Result<String> {
    let result = connection
        .request(json!({"op":"task", "task_id":id}))
        .await?;
    result
        .get("state")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "shared task disappeared"))
}

fn text_chunk(frame: &Value) -> Option<&str> {
    if frame.get("kind")?.as_str()? != "acp_update" {
        return None;
    }
    let update = frame.pointer("/payload/params/update")?;
    if update.get("sessionUpdate")?.as_str()? != "agent_message_chunk" {
        return None;
    }
    update.pointer("/content/text")?.as_str()
}

/// Run one admitted Buzz batch. A missing submit acknowledgment is reconciled
/// by lookup only; a completed task whose transient text was missed is unknown
/// to Buzz and is never prompted or published again.
pub(crate) async fn run(
    path: &Path,
    route: &str,
    batch: &FlushBatch,
    ctx: &PromptContext,
    mut stop: watch::Receiver<bool>,
) -> TurnOutcome {
    let submission = submission_id(route, batch);
    let prompt = prompt(batch, ctx).await;
    let mut may_have_submitted = false;
    let mut known_task: Option<String> = None;
    let mut cursor = 0u64;
    let mut reply = String::new();
    let mut attempts = 0usize;
    let mut first = true;
    let deadline = tokio::time::Instant::now() + ctx.max_turn_duration;
    while attempts < RECONNECT_ATTEMPTS && tokio::time::Instant::now() < deadline {
        if !first {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        first = false;
        attempts += 1;
        if *stop.borrow() && !may_have_submitted && known_task.is_none() {
            return TurnOutcome::Canceled;
        }
        let mut connection = match Connection::open(path, route).await {
            Ok(connection) => connection,
            Err(_) => continue,
        };
        let found = match connection
            .request(json!({"op":"submission", "submission_id":submission}))
            .await
        {
            Ok(found) => found,
            Err(_) => continue,
        };
        let (task, submitted_here) = if let Some(id) = task_id(&found) {
            (id.to_owned(), false)
        } else if may_have_submitted {
            return TurnOutcome::UnknownOutcome {
                reason: format!("submission {submission} was not found after an uncertain submit; inspect gateway journal"),
            };
        } else {
            let session = match connection.request(json!({"op":"route_session"})).await {
                Ok(Value::String(session)) => Some(session),
                Ok(Value::Null) => None,
                Ok(_) => {
                    return TurnOutcome::Failed {
                        reason: "invalid shared session reply".into(),
                    }
                }
                Err(_) => continue,
            };
            may_have_submitted = true;
            let accepted = match connection
                .request(json!({
                    "op":"submit", "submission_id":submission, "session_id":session, "prompt":prompt
                }))
                .await
            {
                Ok(accepted) => accepted,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    may_have_submitted = false;
                    attempts -= 1;
                    continue;
                }
                Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
                    return TurnOutcome::Failed {
                        reason: error.to_string(),
                    };
                }
                Err(_) => continue,
            };
            match accepted.pointer("/task/task_id").and_then(Value::as_str) {
                Some(id) => (id.to_owned(), accepted["admission"] == "accepted"),
                None => {
                    return TurnOutcome::Failed {
                        reason: "shared submit omitted task ID".into(),
                    }
                }
            }
        };
        if known_task.as_deref().is_some_and(|known| known != task) {
            return TurnOutcome::UnknownOutcome {
                reason: "submission resolved to a different task".into(),
            };
        }
        known_task = Some(task.clone());
        let state = match task_state(&mut connection, &task).await {
            Ok(state) => state,
            Err(_) => continue,
        };
        let already_terminal = terminal(&state).is_some();
        if connection
            .request(json!({"op":"attach", "task_id":task, "after_sequence":cursor}))
            .await
            .is_err()
        {
            continue;
        }
        // The submitting connection already controls the task. Reclaim is
        // needed only after reconnect, once the previous connection detached.
        if !submitted_here && !already_terminal {
            match connection
                .request(json!({"op":"reclaim", "task_id":task}))
                .await
            {
                Ok(result) if result["claimed"] == true => {}
                _ => continue,
            }
        }
        if *stop.borrow()
            && !already_terminal
            && connection
                .request(json!({"op":"stop", "task_id":task}))
                .await
                .is_err()
        {
            continue;
        }
        let mut saw_gap = false;
        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {
                    let _ = connection.request(json!({"op":"stop", "task_id":task})).await;
                    return TurnOutcome::UnknownOutcome { reason: "shared turn exceeded Buzz's configured duration; inspect gateway task".into() };
                }
                changed = stop.changed() => {
                    if changed.is_ok() && *stop.borrow()
                        && connection.request(json!({"op":"stop", "task_id":task})).await.is_err() {
                        break;
                    }
                }
                frame = connection.next_event() => {
                    let frame = match frame { Ok(frame) => frame, Err(_) => break };
                    match frame.get("type").and_then(Value::as_str) {
                        Some("gap") => { saw_gap = true; break; }
                        Some("event") if frame["task_id"] == task => {
                            let sequence = frame["sequence"].as_u64().unwrap_or(0);
                            if sequence != cursor + 1 { saw_gap = true; break; }
                            cursor = sequence;
                            if let Some(chunk) = text_chunk(&frame) {
                                if reply.len().saturating_add(chunk.len()) > MAX_FRAME {
                                    saw_gap = true;
                                    break;
                                }
                                reply.push_str(chunk);
                            }
                            if frame["kind"] == "reverse_request" {
                                if let Some(request_id) = frame["request_id"].as_str() {
                                    // Buzz has no interactive permission UI here. Cancel the
                                    // request; the gateway's Level C boundary stays authoritative.
                                    if frame.pointer("/payload/method").and_then(Value::as_str) == Some("session/request_permission") {
                                        if connection.request(json!({"op":"respond", "task_id":task,
                                            "request_id":request_id, "response":{"outcome":{"outcome":"cancelled"}}})).await.is_err() {
                                            break;
                                        }
                                    } else {
                                        let _ = connection.request(json!({"op":"stop", "task_id":task})).await;
                                        return TurnOutcome::UnknownOutcome { reason: "unsupported ACP reverse request; task stopped for reconciliation".into() };
                                    }
                                }
                            }
                            if frame["kind"] == "terminal" {
                                let state = match task_state(&mut connection, &task).await {
                                    Ok(state) => state,
                                    Err(error) => return TurnOutcome::UnknownOutcome { reason: format!("terminal state unavailable: {error}") },
                                };
                                let outcome = terminal(&state).unwrap_or(TurnOutcome::UnknownOutcome { reason: "terminal frame without terminal state".into() });
                                if matches!(outcome, TurnOutcome::Completed { .. }) && !saw_gap {
                                    return publish_reply(batch, ctx, &reply).await;
                                }
                                return outcome;
                            }
                        }
                        _ => return TurnOutcome::UnknownOutcome { reason: "invalid shared event stream".into() },
                    }
                }
            }
        }
        if saw_gap {
            return TurnOutcome::UnknownOutcome {
                reason: format!("shared stream gap for task {task}; inspect task before re-asking"),
            };
        }
    }
    TurnOutcome::UnknownOutcome {
        reason: format!(
            "shared gateway unavailable; reconcile submission {submission} before re-asking"
        ),
    }
}

async fn publish_reply(batch: &FlushBatch, ctx: &PromptContext, content: &str) -> TurnOutcome {
    if content.trim().is_empty() {
        return TurnOutcome::Completed {
            summary: "shared turn returned no reply text".into(),
        };
    }
    let last = match batch.events.last() {
        Some(last) => last,
        None => {
            return TurnOutcome::Failed {
                reason: "empty Buzz batch".into(),
            }
        }
    };
    let tags = crate::queue::parse_thread_tags(&last.event);
    let anchor = last.event.id.to_hex();
    let root = tags.root_event_id.as_deref().unwrap_or(&anchor);
    let root = match nostr::EventId::from_hex(root) {
        Ok(root) => root,
        Err(_) => {
            return TurnOutcome::Failed {
                reason: "invalid Buzz reply root".into(),
            }
        }
    };
    let thread = buzz_sdk::ThreadRef {
        root_event_id: root,
        parent_event_id: root,
    };
    let builder = match buzz_sdk::build_message(
        batch.channel_id,
        content,
        Some(&thread),
        &[],
        false,
        &[],
        &[],
    ) {
        Ok(builder) => builder,
        Err(error) => {
            return TurnOutcome::Failed {
                reason: format!("could not build Buzz reply: {error}"),
            }
        }
    };
    let event = match builder.sign_with_keys(&ctx.rest_client.keys) {
        Ok(event) => event,
        Err(error) => {
            return TurnOutcome::Failed {
                reason: format!("could not sign Buzz reply: {error}"),
            }
        }
    };
    match tokio::time::timeout(Duration::from_secs(5), ctx.rest_client.submit_event(&event)).await {
        Ok(Ok(_)) => TurnOutcome::Completed {
            summary: "shared reply published".into(),
        },
        Ok(Err(error)) => TurnOutcome::UnknownOutcome {
            reason: format!("Buzz reply publication outcome uncertain: {error}"),
        },
        Err(_) => TurnOutcome::UnknownOutcome {
            reason: "Buzz reply publication timed out; inspect channel before re-asking".into(),
        },
    }
}
