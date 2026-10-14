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
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufStream};
use tokio::sync::watch;

use crate::config::SharedPolicy;
use crate::pool::PromptContext;
use crate::queue::{FlushBatch, FormatPromptArgs};
use crate::turn_executor::TurnOutcome;

const MAX_FRAME: usize = 1024 * 1024;
const MAX_PENDING_EVENTS: usize = 128;
const RECONNECT_ATTEMPTS: usize = 120;
/// How often to check whether a task that lost live events has finished.
const RESYNC_POLL: Duration = Duration::from_secs(1);

// AGW-SHARED/1 is a same-UID Unix socket. Other targets still compile the
// client, but every connect fails.
#[cfg(unix)]
type Stream = tokio::net::UnixStream;
#[cfg(not(unix))]
type Stream = tokio::io::DuplexStream;

#[cfg(unix)]
async fn connect(path: &Path) -> io::Result<Stream> {
    Stream::connect(path).await
}

#[cfg(not(unix))]
async fn connect(_path: &Path) -> io::Result<Stream> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "AGW-SHARED/1 requires a Unix socket",
    ))
}

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
    stream: BufStream<Stream>,
    next_id: u64,
    pending: VecDeque<Value>,
    /// The bytes of a frame whose read was cancelled before its newline.
    partial: Vec<u8>,
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
        let stream = tokio::time::timeout(Duration::from_secs(5), connect(path))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "shared connect timed out"))??;
        let mut stream = BufStream::new(stream);
        stream.write_all(b"AGW-SHARED/1\n").await?;
        stream.write_all(route.as_bytes()).await?;
        stream.write_all(b"\n").await?;
        stream.flush().await?;
        let mut answer = Vec::new();
        tokio::time::timeout(
            Duration::from_secs(5),
            read_line(&mut stream, &mut answer, 128),
        )
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
            partial: Vec::new(),
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
        self.stream.flush().await?;
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

    /// Cancel-safe, so `run` can `select!` over it: an unfinished frame
    /// stays in `partial` for the next call.
    async fn read(&mut self) -> io::Result<Value> {
        read_line(&mut self.stream, &mut self.partial, MAX_FRAME).await?;
        let line = std::mem::take(&mut self.partial);
        serde_json::from_slice(&line).map_err(io::Error::other)
    }
}

/// Append bytes to `line` up to the next newline, which is not kept.
/// `read_u8` takes one byte or none, so cancelling this loses nothing that
/// `line` does not hold.
async fn read_line(
    stream: &mut BufStream<Stream>,
    line: &mut Vec<u8>,
    limit: usize,
) -> io::Result<()> {
    loop {
        let byte = stream.read_u8().await?;
        if byte == b'\n' {
            return Ok(());
        }
        if line.len() == limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "shared frame too large",
            ));
        }
        line.push(byte);
    }
}

async fn prompt(batch: &FlushBatch, ctx: &PromptContext) -> Result<String, TurnOutcome> {
    let channel = ctx
        .channel_info
        .resolve(batch.channel_id)
        .await
        .ok()
        .flatten();
    let base_prompt = ctx.base_prompt.as_deref().map(|base| {
        if let Some(suffix) = base.strip_prefix(include_str!("base_prompt.md")) {
            Ok(format!("{}{}", include_str!("base_prompt_shared.md"), suffix))
        } else if base.contains("buzz messages send") {
            Err(TurnOutcome::Failed {
                reason: "custom Buzz base prompt directs CLI message sending; supply a shared-safe base prompt".into(),
            })
        } else {
            Ok(format!("{base}\n\nThe Buzz harness publishes your ACP response. Do not call `buzz messages send` for this reply."))
        }
    }).transpose()?;
    let mut sections = crate::queue::format_prompt(
        batch,
        &FormatPromptArgs {
            channel_info: channel.as_ref(),
            base_prompt: base_prompt.as_deref(),
            system_prompt: ctx.system_prompt.as_deref(),
            team_instructions: ctx.team_instructions.as_deref(),
            harness_publishes_reply: true,
            ..FormatPromptArgs::default()
        },
    );
    sections.push("Reply in your ACP response text. Buzz will publish your reply to the originating channel. Do not send a duplicate reply with the Buzz CLI.".into());
    Ok(sections.join("\n\n"))
}

fn task_id(value: &Value) -> Option<&str> {
    value.get("task_id").and_then(Value::as_str)
}

/// The daemon refuses a submit whose declared policy differs from the route's.
fn submit_op(
    submission: &str,
    session: Option<&str>,
    prompt: &str,
    policy: &SharedPolicy,
) -> Value {
    json!({
        "op": "submit",
        "submission_id": submission,
        "session_id": session,
        "prompt": prompt,
        "permission_profile": policy.permission_profile,
        "side_effect_class": policy.side_effect_class,
    })
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
/// by lookup only. Text missed while the task ran is read back from the
/// daemon's retained replay once it finishes. The daemon bounds that replay
/// per task and in total and drops a task's events past either bound, so a
/// completed task whose text is gone from it too is unknown to Buzz and is
/// never prompted or published again. A batch the daemon provably never
/// admitted ends `Failed`, never `UnknownOutcome`.
pub(crate) async fn run(
    path: &Path,
    route: &str,
    policy: &SharedPolicy,
    batch: &FlushBatch,
    ctx: &PromptContext,
    mut stop: watch::Receiver<bool>,
) -> TurnOutcome {
    let submission = submission_id(route, batch);
    let prompt = match prompt(batch, ctx).await {
        Ok(prompt) => prompt,
        Err(outcome) => return outcome,
    };
    let mut may_have_submitted = false;
    // Set once a lookup proves no task holds this submission ID. From then
    // on, only this loop's own submits could create one. Until then an
    // earlier harness process may have submitted it.
    let mut absent = false;
    let mut busy = false;
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
            absent = true;
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
                .request(submit_op(&submission, session.as_deref(), &prompt, policy))
                .await
            {
                Ok(accepted) => accepted,
                // `busy` is decided after the duplicate lookup, so this
                // submit was definitely not admitted. Wait for the agent.
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    may_have_submitted = false;
                    busy = true;
                    attempts -= 1;
                    continue;
                }
                Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
                    return TurnOutcome::Failed {
                        reason: refused(&error),
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
        let mut already_terminal = terminal(&state).is_some();
        // Attaching after the task finished yields the daemon's retained
        // replay, where a gap cannot be recovered. Read this before reclaim,
        // which may see the task finish while this attachment is still live.
        let replaying = already_terminal;
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
                _ => {
                    // The task may have finished between the state read and
                    // reclaim. Keep consuming the replay already attached.
                    match task_state(&mut connection, &task).await {
                        Ok(state) if terminal(&state).is_some() => already_terminal = true,
                        _ => continue,
                    }
                }
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
        // Set once live events were lost: when to poll the task's state if
        // no frame arrives first. The daemon keeps the events of a finished
        // task, so wait for the terminal state, then reconnect and replay
        // from `cursor`.
        let mut resync: Option<tokio::time::Instant> = None;
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
                // A lagged stream still ends with the terminal frame, but a
                // gap with no live updates behind it (the task finished
                // between the state read and attach) is followed by nothing.
                // Poll once the stream goes quiet; polling during a burst
                // would queue the reply behind its events.
                _ = tokio::time::sleep_until(resync.unwrap_or(deadline)), if resync.is_some() => {
                    match task_state(&mut connection, &task).await {
                        Ok(state) if terminal(&state).is_none() => {
                            resync = Some(tokio::time::Instant::now() + RESYNC_POLL);
                        }
                        _ => break,
                    }
                }
                frame = connection.next_event() => {
                    let frame = match frame { Ok(frame) => frame, Err(_) => break };
                    match frame.get("type").and_then(Value::as_str) {
                        Some("gap") if replaying => { saw_gap = true; break; }
                        Some("gap") => resync = Some(tokio::time::Instant::now() + RESYNC_POLL),
                        Some("event") if frame["task_id"] == task => {
                            let sequence = frame["sequence"].as_u64().unwrap_or(0);
                            if resync.is_some() || sequence != cursor + 1 {
                                if replaying { saw_gap = true; break; }
                                // Text after a gap would land out of order.
                                // The replay from `cursor` delivers it instead.
                                resync = Some(tokio::time::Instant::now() + RESYNC_POLL);
                            } else {
                                cursor = sequence;
                                if let Some(chunk) = text_chunk(&frame) {
                                    if reply.len().saturating_add(chunk.len()) > MAX_FRAME {
                                        saw_gap = true;
                                        break;
                                    }
                                    reply.push_str(chunk);
                                }
                            }
                            if frame["kind"] == "reverse_request" && !already_terminal {
                                if let Some(request_id) = frame["request_id"].as_str() {
                                    // Buzz has no interactive permission UI here. Cancel the
                                    // request; the gateway's Level C boundary stays authoritative.
                                    if frame.pointer("/payload/method").and_then(Value::as_str) == Some("session/request_permission") {
                                        if connection.request(json!({"op":"respond", "task_id":task,
                                            "request_id":request_id, "response":{"outcome":{"outcome":"cancelled"}}})).await.is_err() {
                                            match task_state(&mut connection, &task).await {
                                                Ok(state) if terminal(&state).is_some() => already_terminal = true,
                                                _ => break,
                                            }
                                        }
                                    } else {
                                        if matches!(task_state(&mut connection, &task).await, Ok(state) if terminal(&state).is_some()) {
                                            already_terminal = true;
                                            continue;
                                        }
                                        let _ = connection.request(json!({"op":"stop", "task_id":task})).await;
                                        return TurnOutcome::UnknownOutcome { reason: "unsupported ACP reverse request; task stopped for reconciliation".into() };
                                    }
                                }
                            }
                            if frame["kind"] == "terminal" {
                                if resync.is_some() {
                                    break;
                                }
                                let state = match task_state(&mut connection, &task).await {
                                    Ok(state) => state,
                                    Err(error) => return TurnOutcome::UnknownOutcome { reason: format!("terminal state unavailable: {error}") },
                                };
                                let outcome = terminal(&state).unwrap_or(TurnOutcome::UnknownOutcome { reason: "terminal frame without terminal state".into() });
                                if matches!(outcome, TurnOutcome::Completed { .. }) {
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
    exhausted(
        &submission,
        absent && !may_have_submitted && known_task.is_none(),
        busy,
    )
}

/// A route session whose policy binding differs from the route's refuses
/// every submit. `route_session` is scoped by route generation, so raising
/// it is the operator's way to start a fresh session.
fn refused(error: &io::Error) -> String {
    let reason = error.to_string();
    if reason.ends_with(": session_policy_mismatch") {
        format!(
            "{reason}; the route's current session has a different policy binding, \
             so raise its route_generation in the gateway config to start a fresh session"
        )
    } else {
        reason
    }
}

fn exhausted(submission: &str, not_admitted: bool, busy: bool) -> TurnOutcome {
    if !not_admitted {
        return TurnOutcome::UnknownOutcome {
            reason: format!(
                "shared gateway unavailable; reconcile submission {submission} before re-asking"
            ),
        };
    }
    let cause = if busy {
        "the agent stayed busy with another turn"
    } else {
        "the shared gateway stopped answering"
    };
    TurnOutcome::Failed {
        reason: format!("{cause}; this request was not submitted"),
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

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::queue::BatchEvent;
    use crate::scope::SessionScope;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::net::UnixListener;

    fn policy() -> SharedPolicy {
        SharedPolicy {
            permission_profile: "repository_read".into(),
            side_effect_class: "external_effects".into(),
        }
    }

    fn batch() -> FlushBatch {
        let keys = nostr::Keys::generate();
        let event = nostr::EventBuilder::text_note("status?")
            .sign_with_keys(&keys)
            .unwrap();
        let channel_id = uuid::Uuid::new_v4();
        FlushBatch {
            channel_id,
            scope: SessionScope::Conversation { channel_id },
            events: vec![BatchEvent {
                event,
                prompt_tag: "@mention".into(),
                received_at: std::time::Instant::now(),
            }],
            cancelled_events: vec![],
            cancel_reason: None,
        }
    }

    /// A daemon that has no task for the submission and answers every
    /// submit with `refusal`. Each submit it sees is sent to the receiver.
    fn refusing_daemon(
        refusal: &'static str,
    ) -> (
        std::path::PathBuf,
        tokio::sync::mpsc::UnboundedReceiver<Value>,
    ) {
        let (seen, submits) = tokio::sync::mpsc::unbounded_channel();
        let path = scripted_daemon(Arc::new(move |_, request: &Value| {
            if request["op"] == "submit" {
                let _ = seen.send(request.clone());
                vec![json!({"type":"refusal", "id":request["id"], "reason":refusal})]
            } else {
                vec![reply(request, Value::Null)]
            }
        }));
        (path, submits)
    }

    #[test]
    fn submit_declares_route_policy() {
        let op = submit_op("buzz-1", None, "hello", &policy());
        assert_eq!(
            op,
            json!({
                "op": "submit",
                "submission_id": "buzz-1",
                "session_id": null,
                "prompt": "hello",
                "permission_profile": "repository_read",
                "side_effect_class": "external_effects",
            })
        );
    }

    #[test]
    fn only_a_proven_absent_submission_is_definitely_not_submitted() {
        assert!(matches!(
            exhausted("buzz-1", false, false),
            TurnOutcome::UnknownOutcome { .. }
        ));
        assert!(matches!(
            exhausted("buzz-1", true, true),
            TurnOutcome::Failed { reason } if reason.contains("busy")
        ));
        assert!(matches!(
            exhausted("buzz-1", true, false),
            TurnOutcome::Failed { reason } if reason.contains("not submitted")
        ));
    }

    #[test]
    fn session_policy_refusal_names_the_operator_fix() {
        let error = io::Error::new(
            io::ErrorKind::PermissionDenied,
            "shared gateway refused: session_policy_mismatch",
        );
        assert!(refused(&error).contains("route_generation"));
        let error = io::Error::new(
            io::ErrorKind::PermissionDenied,
            "shared gateway refused: policy_mismatch",
        );
        assert_eq!(refused(&error), "shared gateway refused: policy_mismatch");
    }

    #[tokio::test]
    async fn busy_agent_until_deadline_is_not_submitted() {
        let (path, mut submits) = refusing_daemon("busy");
        let mut ctx = crate::pool::tests::make_prompt_context_no_owner();
        ctx.max_turn_duration = Duration::from_millis(900);
        let (_stop, receiver) = watch::channel(false);
        let outcome = run(&path, "rt-test", &policy(), &batch(), &ctx, receiver).await;
        let _ = std::fs::remove_file(&path);
        assert!(
            matches!(&outcome, TurnOutcome::Failed { reason } if reason.contains("busy")),
            "{outcome:?}"
        );
        let first = submits
            .try_recv()
            .expect("at least one submit reached the daemon");
        assert_eq!(first["permission_profile"], "repository_read");
        assert_eq!(first["side_effect_class"], "external_effects");
    }

    #[tokio::test]
    async fn policy_refusal_fails_without_resubmitting() {
        let (path, mut submits) = refusing_daemon("policy_mismatch");
        let ctx = crate::pool::tests::make_prompt_context_no_owner();
        let (_stop, receiver) = watch::channel(false);
        let outcome = run(&path, "rt-test", &policy(), &batch(), &ctx, receiver).await;
        let _ = std::fs::remove_file(&path);
        assert!(
            matches!(&outcome, TurnOutcome::Failed { reason } if reason.contains("policy_mismatch")),
            "{outcome:?}"
        );
        assert!(submits.try_recv().is_ok());
        assert!(
            submits.try_recv().is_err(),
            "a refused submit was sent again"
        );
    }

    type Script = Arc<dyn Fn(usize, &Value) -> Vec<Value> + Send + Sync>;

    /// A daemon that answers each request on its `n`th connection, counting
    /// from 0, with the frames `script(n, request)` returns, in order. A
    /// `hang_up()` frame closes that connection instead.
    fn scripted_daemon(script: Script) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("buzz-agw-{}.sock", uuid::Uuid::new_v4()));
        let listener = UnixListener::bind(&path).unwrap();
        tokio::spawn(async move {
            let mut connections = 0;
            while let Ok((stream, _)) = listener.accept().await {
                let (n, script) = (connections, script.clone());
                connections += 1;
                tokio::spawn(async move {
                    let (read, mut write) = stream.into_split();
                    let mut lines = BufReader::new(read).lines();
                    let (Ok(Some(_)), Ok(Some(_))) =
                        (lines.next_line().await, lines.next_line().await)
                    else {
                        return;
                    };
                    write.write_all(b"OK\n").await.unwrap();
                    while let Ok(Some(line)) = lines.next_line().await {
                        let request: Value = serde_json::from_str(&line).unwrap();
                        for frame in script(n, &request) {
                            if frame == hang_up() {
                                return;
                            }
                            let mut bytes = serde_json::to_vec(&frame).unwrap();
                            bytes.push(b'\n');
                            if write.write_all(&bytes).await.is_err() {
                                return;
                            }
                        }
                    }
                });
            }
        });
        path
    }

    fn reply(request: &Value, result: Value) -> Value {
        json!({"type":"reply", "id":request["id"], "result":result})
    }

    fn chunk(sequence: u64, text: &str) -> Value {
        json!({"type":"event", "task_id":"t1", "sequence":sequence, "kind":"acp_update",
            "payload":{"params":{"update":{"sessionUpdate":"agent_message_chunk",
                "content":{"type":"text", "text":text}}}}})
    }

    fn gap() -> Value {
        json!({"type":"gap", "task_id":"t1", "first_available":null})
    }

    fn finished(sequence: u64) -> Value {
        json!({"type":"event", "task_id":"t1", "sequence":sequence, "kind":"terminal", "payload":null})
    }

    fn hang_up() -> Value {
        json!({"hang_up": true})
    }

    /// The daemon side of one task, `t1`. The first connection submits it,
    /// sees it running for its first `running_reads` state reads and
    /// finished after that, and gets `live` after its attach. Every later
    /// connection finds it finished and gets `retained`. Each attach's
    /// `after_sequence` goes to the receiver; the counter is the first
    /// connection's state reads.
    fn gap_daemon(
        running_reads: usize,
        live: Vec<Value>,
        retained: Vec<Value>,
    ) -> (
        std::path::PathBuf,
        tokio::sync::mpsc::UnboundedReceiver<u64>,
        Arc<AtomicUsize>,
    ) {
        let (attached, attaches) = tokio::sync::mpsc::unbounded_channel();
        let reads = Arc::new(AtomicUsize::new(0));
        let counted = reads.clone();
        let script = move |connection: usize, request: &Value| {
            let first = connection == 0;
            let result = match request["op"].as_str().unwrap_or_default() {
                "submission" if first => Value::Null,
                "submission" => json!({"task_id":"t1"}),
                "route_session" => Value::Null,
                "submit" => json!({"task":{"task_id":"t1"}, "admission":"accepted"}),
                "task" if first && counted.fetch_add(1, Ordering::SeqCst) < running_reads => {
                    json!({"state":"running"})
                }
                "task" => json!({"state":"completed"}),
                "attach" => {
                    let _ = attached.send(request["after_sequence"].as_u64().unwrap());
                    let mut frames = vec![reply(request, json!({}))];
                    frames.extend(if first { &live } else { &retained }.iter().cloned());
                    return frames;
                }
                _ => Value::Null,
            };
            vec![reply(request, result)]
        };
        (scripted_daemon(Arc::new(script)), attaches, reads)
    }

    /// A relay that accepts every `POST /events` and sends the event's
    /// content to the receiver.
    async fn recording_relay() -> (String, tokio::sync::mpsc::UnboundedReceiver<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let (seen, published) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut request = Vec::new();
                let mut buffer = [0u8; 4096];
                let mut body = None;
                while body.is_none() {
                    let n = socket.read(&mut buffer).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..n]);
                    body = request
                        .windows(4)
                        .position(|window| window == b"\r\n\r\n")
                        .map(|at| at + 4);
                }
                let Some(body) = body else { continue };
                let head = String::from_utf8_lossy(&request[..body]).to_ascii_lowercase();
                let length = head
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length:"))
                    .and_then(|value| value.trim().parse::<usize>().ok())
                    .unwrap_or(0);
                while request.len() < body + length {
                    let n = socket.read(&mut buffer).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..n]);
                }
                if let Ok(event) = serde_json::from_slice::<Value>(&request[body..]) {
                    let _ = seen.send(event["content"].as_str().unwrap_or_default().to_owned());
                }
                let _ = socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
                    )
                    .await;
            }
        });
        (base_url, published)
    }

    async fn run_publishing(
        path: &Path,
    ) -> (TurnOutcome, tokio::sync::mpsc::UnboundedReceiver<String>) {
        let (base_url, published) = recording_relay().await;
        let mut ctx = crate::pool::tests::make_prompt_context_no_owner();
        ctx.rest_client.base_url = base_url;
        let (_stop, receiver) = watch::channel(false);
        let outcome = run(path, "rt-test", &policy(), &batch(), &ctx, receiver).await;
        let _ = std::fs::remove_file(path);
        (outcome, published)
    }

    /// The retained replay of `t1` after the client's last contiguous event.
    fn retained_after_one() -> Vec<Value> {
        vec![chunk(2, "wor"), chunk(3, "ld"), chunk(4, "!"), finished(5)]
    }

    #[tokio::test]
    async fn a_live_gap_is_replayed_from_the_retained_stream() {
        // The daemon reports the lag, or the next sequence simply skips.
        for live in [
            vec![chunk(1, "Hello "), gap(), chunk(4, "!"), finished(5)],
            vec![chunk(1, "Hello "), chunk(4, "!"), finished(5)],
        ] {
            let (path, mut attaches, _) = gap_daemon(1, live, retained_after_one());
            let (outcome, mut published) = run_publishing(&path).await;
            assert!(
                matches!(&outcome, TurnOutcome::Completed { summary } if summary == "shared reply published"),
                "{outcome:?}"
            );
            assert_eq!(published.try_recv().unwrap(), "Hello world!");
            assert!(
                published.try_recv().is_err(),
                "the reply was published twice"
            );
            assert_eq!(attaches.try_recv().unwrap(), 0);
            assert_eq!(
                attaches.try_recv().unwrap(),
                1,
                "the replay must resume after the last contiguous event"
            );
        }
    }

    #[tokio::test]
    async fn a_gap_with_nothing_behind_it_is_resolved_by_polling() {
        // The first poll still sees the task running, the second sees it
        // finished.
        let (path, mut attaches, reads) =
            gap_daemon(2, vec![chunk(1, "Hello "), gap()], retained_after_one());
        let (outcome, mut published) = run_publishing(&path).await;
        assert!(
            matches!(&outcome, TurnOutcome::Completed { summary } if summary == "shared reply published"),
            "{outcome:?}"
        );
        assert_eq!(published.try_recv().unwrap(), "Hello world!");
        assert!(
            published.try_recv().is_err(),
            "the reply was published twice"
        );
        assert_eq!(
            reads.load(Ordering::SeqCst),
            3,
            "a running poll must re-arm on the same connection"
        );
        assert_eq!(attaches.try_recv().unwrap(), 0);
        assert_eq!(attaches.try_recv().unwrap(), 1);
        assert!(
            attaches.try_recv().is_err(),
            "attached again after publishing"
        );
    }

    #[tokio::test]
    async fn a_gap_in_the_retained_replay_is_final() {
        let (path, mut attaches, _) =
            gap_daemon(1, vec![chunk(1, "Hello "), gap(), finished(5)], vec![gap()]);
        let (outcome, mut published) = run_publishing(&path).await;
        assert!(
            matches!(&outcome, TurnOutcome::UnknownOutcome { reason } if reason.contains("shared stream gap")),
            "{outcome:?}"
        );
        assert!(
            published.try_recv().is_err(),
            "a partial reply was published"
        );
        assert_eq!(attaches.try_recv().unwrap(), 0);
        assert_eq!(attaches.try_recv().unwrap(), 1);
        assert!(attaches.try_recv().is_err(), "a final gap was retried");
    }

    #[tokio::test]
    async fn a_live_gap_stays_recoverable_when_reclaim_sees_the_task_finish() {
        // The first connection drops mid-turn. The second reattaches to the
        // running task, but its window has moved past `cursor`, and the task
        // finishes before reclaim. That gap is live, not retained.
        let (attached, mut attaches) = tokio::sync::mpsc::unbounded_channel();
        let second_reads = AtomicUsize::new(0);
        let script = move |connection: usize, request: &Value| {
            let result = match (connection, request["op"].as_str().unwrap_or_default()) {
                (0, "submission") | (_, "route_session") => Value::Null,
                (_, "submission") => json!({"task_id":"t1"}),
                (_, "submit") => json!({"task":{"task_id":"t1"}, "admission":"accepted"}),
                (0, "task") => json!({"state":"running"}),
                (1, "task") if second_reads.fetch_add(1, Ordering::SeqCst) == 0 => {
                    json!({"state":"running"})
                }
                (_, "task") => json!({"state":"completed"}),
                (_, "reclaim") => json!({"claimed":false}),
                (_, "attach") => {
                    let _ = attached.send(request["after_sequence"].as_u64().unwrap());
                    let mut frames = vec![reply(request, json!({}))];
                    frames.extend(match connection {
                        0 => vec![chunk(1, "Hello "), hang_up()],
                        1 => vec![
                            json!({"type":"gap", "task_id":"t1", "first_available":3}),
                            finished(5),
                        ],
                        _ => retained_after_one(),
                    });
                    return frames;
                }
                _ => Value::Null,
            };
            vec![reply(request, result)]
        };
        let path = scripted_daemon(Arc::new(script));
        let (outcome, mut published) = run_publishing(&path).await;
        assert!(
            matches!(&outcome, TurnOutcome::Completed { summary } if summary == "shared reply published"),
            "{outcome:?}"
        );
        assert_eq!(published.try_recv().unwrap(), "Hello world!");
        assert!(
            published.try_recv().is_err(),
            "the reply was published twice"
        );
        for expected in [0, 1, 1] {
            assert_eq!(attaches.try_recv().unwrap(), expected);
        }
        assert!(
            attaches.try_recv().is_err(),
            "attached again after publishing"
        );
    }

    #[tokio::test]
    async fn a_read_cancelled_mid_frame_keeps_its_bytes() {
        let (client, mut daemon) = tokio::net::UnixStream::pair().unwrap();
        let mut connection = Connection {
            stream: BufStream::new(client),
            next_id: 0,
            pending: VecDeque::new(),
            partial: Vec::new(),
        };
        daemon.write_all(br#"{"type":"gap","#).await.unwrap();
        let cut = tokio::time::timeout(Duration::from_millis(100), connection.next_event()).await;
        assert!(cut.is_err(), "half a frame was returned: {cut:?}");
        daemon.write_all(b"\"task_id\":\"t1\"}\n").await.unwrap();
        assert_eq!(
            connection.next_event().await.unwrap(),
            json!({"type":"gap", "task_id":"t1"})
        );
    }
}
