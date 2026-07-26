//! End-to-end smoke of `buzz-acp --gwp-worker`: drives the real binary over
//! GWP/0 stdin/stdout with a fake ACP agent, exactly as an agent-gateway
//! daemon would. Proves the handshake ordering (ready before agent spawn),
//! payload decoding, the full `run_prompt_task` path, and the result frame.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use nostr::{EventBuilder, Keys};

const FAKE_ACP_AGENT: &str = r#"#!/usr/bin/env python3
import json, sys
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    msg = json.loads(line)
    mid = msg.get("id")
    if mid is None:
        continue  # notification
    method = msg.get("method", "")
    if method == "initialize":
        result = {"protocolVersion": 1, "agentInfo": {"name": "fake-acp"}}
    elif method == "session/new":
        result = {"sessionId": "fake-session-1"}
    elif method == "session/prompt":
        result = {"stopReason": "end_turn"}
    else:
        result = {}
    sys.stdout.write(json.dumps({"jsonrpc": "2.0", "id": mid, "result": result}) + "\n")
    sys.stdout.flush()
"#;

struct KillOnDrop(Child);
impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn read_frame(reader: &mut impl BufRead) -> serde_json::Value {
    let mut line = String::new();
    reader.read_line(&mut line).expect("read frame");
    assert!(!line.is_empty(), "worker closed stdout unexpectedly");
    serde_json::from_str(&line).expect("frame is JSON")
}

#[test]
fn a_turn_runs_end_to_end_through_the_gwp_worker() {
    let dir = std::env::temp_dir().join(format!("gwp-smoke-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let agent_script = dir.join("fake_acp.py");
    std::fs::write(&agent_script, FAKE_ACP_AGENT).unwrap();

    let keys = Keys::generate();
    let key_file = dir.join("key");
    std::fs::write(&key_file, keys.secret_key().to_secret_hex()).unwrap();

    // The wire format the north-side dispatch will emit — written out
    // longhand here so this test pins the format independently of the
    // codec's own roundtrip test.
    let event = EventBuilder::text_note("hello from the smoke test")
        .sign_with_keys(&keys)
        .unwrap();
    let channel_id = uuid::Uuid::new_v4();
    let payload = serde_json::json!({
        "v": 1,
        "channel_id": channel_id,
        "events": [{"event": event, "prompt_tag": "smoke"}],
        "cancelled_events": [],
        "cancel_reason": null,
    });

    let mut child = Command::new(env!("CARGO_BIN_EXE_buzz-acp"))
        .args([
            "--gwp-worker",
            "--private-key-file",
            key_file.to_str().unwrap(),
            "--agent-command",
            "python3",
            "--agent-args",
            agent_script.to_str().unwrap(),
            // Port 1 refuses instantly: every best-effort REST fetch
            // (reactions, channel info) fails open without stalling.
            "--relay-url",
            "ws://127.0.0.1:1",
            "--context-message-limit",
            "0",
            "--no-memory",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn buzz-acp --gwp-worker");
    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let child = KillOnDrop(child);
    let mut reader = BufReader::new(stdout);

    // Watchdog: a hang here should fail the test, not wedge the suite.
    let pid = child.0.id();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(90));
        watchdog_kill(pid);
    });

    writeln!(
        stdin,
        r#"{{"type":"hello","protocol":"gwp/0","gateway":"smoke/0","worker_id":"w1"}}"#
    )
    .unwrap();
    let ready = read_frame(&mut reader);
    assert_eq!(ready["type"], "ready", "ready frame first: {ready}");
    assert_eq!(ready["protocol"], "gwp/0");
    assert!(ready["session"].as_str().is_some_and(|s| !s.is_empty()));

    let deadline_ms = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64)
        + 60_000;
    let turn = serde_json::json!({
        "type": "turn",
        "turn_id": "turn-1",
        "job_id": uuid::Uuid::new_v4(),
        "task": payload.to_string(),
        "deadline_ms": deadline_ms,
    });
    writeln!(stdin, "{turn}").unwrap();

    let result = read_frame(&mut reader);
    assert_eq!(result["type"], "result", "expected result frame: {result}");
    assert_eq!(result["turn_id"], "turn-1");
    assert_eq!(result["summary"], "stop_reason=EndTurn");
    assert_eq!(result["structured"]["stopReason"], "EndTurn");

    // A malformed payload must produce an in-protocol error, not kill the
    // worker: the next well-formed turn still succeeds on the same session.
    let bad_turn = serde_json::json!({
        "type": "turn", "turn_id": "turn-2", "job_id": uuid::Uuid::new_v4(),
        "task": "not json", "deadline_ms": deadline_ms,
    });
    writeln!(stdin, "{bad_turn}").unwrap();
    let error = read_frame(&mut reader);
    assert_eq!(error["type"], "error");
    assert_eq!(error["turn_id"], "turn-2");
    assert_eq!(error["retryable"], false);

    let turn3 = serde_json::json!({
        "type": "turn", "turn_id": "turn-3", "job_id": uuid::Uuid::new_v4(),
        "task": payload.to_string(), "deadline_ms": deadline_ms,
    });
    writeln!(stdin, "{turn3}").unwrap();
    let result3 = read_frame(&mut reader);
    assert_eq!(
        result3["type"], "result",
        "worker must survive a bad payload: {result3}"
    );
    assert_eq!(result3["turn_id"], "turn-3");

    drop(stdin); // EOF -> clean worker shutdown
    std::fs::remove_dir_all(&dir).ok();
}

fn watchdog_kill(pid: u32) {
    let _ = Command::new("kill").arg("-9").arg(pid.to_string()).status();
}
