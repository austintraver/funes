//! End-to-end coverage for the native GitHub Copilot `events.jsonl` source.
//!
//! This test owns its memory directory because indexing and the read surface use process-global
//! `FUNES_HOME`, and because the integration suite runs test binaries concurrently.
//!
//! Fixture shape is based on github/copilot-sdk commit
//! `0cb0050ef4a6206808c7229ee11715f01bc256b0` and the GitHub Copilot CLI
//! streaming-events and CLI config-directory references retrieved 2026-09-11.

use std::io::Write;
use std::path::Path;

use funes::traces::harness::Harness;
use serde_json::{json, Value};

fn event(id: &str, kind: &str, timestamp: &str, data: Value) -> Value {
    json!({
        "id": id,
        "parentId": "previous",
        "timestamp": timestamp,
        "type": kind,
        "data": data,
    })
}

fn write_session(root: &Path, checkout: &Path, include_appended_turn: bool) {
    let session = root.join("copilot-session-0001");
    std::fs::create_dir_all(&session).unwrap();
    std::fs::write(session.join("workspace.yaml"), "cwd: /work/funes-copilot\n").unwrap();

    let mut events = vec![
        event(
            "session-start",
            "session.start",
            "2026-09-11T00:00:00Z",
            json!({
                "sessionId":"native-copilot-0001",
                "version":1,
                "context":{"cwd":checkout,"repository":"historical/funes"}
            }),
        ),
        event(
            "user-1",
            "user.message",
            "2026-09-11T00:00:01Z",
            json!({"content":"How should Copilot transcripts be indexed?"}),
        ),
        event(
            "assistant-1",
            "assistant.message",
            "2026-09-11T00:00:02Z",
            json!({
                "content":"Index durable user, assistant, and tool events.",
                "reasoningText":"Inspect the native event stream.",
                "toolRequests":[{"toolCallId":"call-1","name":"shell","arguments":{"command":"cargo test"}}]
            }),
        ),
        event(
            "tool-1",
            "tool.execution_complete",
            "2026-09-11T00:00:03Z",
            json!({"toolCallId":"call-1","result":{"content":"brief","detailedContent":"tool-result marker"}}),
        ),
    ];
    if include_appended_turn {
        events.push(event(
            "context-change",
            "session.context_changed",
            "2026-09-11T00:00:04Z",
            json!({"cwd":checkout.join("missing-checkout"),"repository":"historical/other"}),
        ));
        events.push(event(
            "user-2",
            "user.message",
            "2026-09-11T00:00:05Z",
            json!({"content":"Then verify incremental indexing and idempotence."}),
        ));
    }

    let mut file = std::fs::File::create(session.join("events.jsonl")).unwrap();
    for record in events {
        writeln!(file, "{record}").unwrap();
    }

    let fallback = root.join("copilot-fallback");
    std::fs::create_dir_all(&fallback).unwrap();
    let mut file = std::fs::File::create(fallback.join("events.jsonl")).unwrap();
    for record in [
        event(
            "fallback-start",
            "session.start",
            "2026-09-11T00:00:00Z",
            json!({"sessionId":"native-copilot-fallback","context":{"cwd":checkout}}),
        ),
        event(
            "fallback-user",
            "user.message",
            "2026-09-11T00:00:01Z",
            json!({"content":"Resolve the checkout's remotes when no repository was recorded."}),
        ),
    ] {
        writeln!(file, "{record}").unwrap();
    }
}

async fn index_copilot(root: &Path) {
    funes::commands::index::run_index_roots(&[(root.to_path_buf(), Some(Harness::Copilot))], false, None, true)
        .await
        .unwrap();
}

async fn chunk_count() -> usize {
    let status = funes::commands::recall::status(funes::memory::Memory::local())
        .await
        .unwrap();
    status
        .lines()
        .find_map(|line| line.strip_prefix("chunks: "))
        .and_then(|n| n.trim().parse().ok())
        .expect("status reports a chunk count")
}

#[derive(Debug, PartialEq, Eq)]
struct StoredChunk {
    text: String,
    repo: String,
    workdir: String,
    turn_uuid: String,
}

async fn stored_content() -> std::collections::BTreeMap<String, StoredChunk> {
    use arrow_array::Array;
    let ds = funes::memory::Memory::local().open().await.unwrap();
    let batches = funes::memory::dataset::scan_rows(&ds, &["id", "text", "repo", "workdir", "turn_uuid"], None, None)
        .await
        .unwrap();
    let mut rows = std::collections::BTreeMap::new();
    for batch in batches {
        let column = |name: &str| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap()
        };
        let ids = column("id");
        for i in 0..ids.len() {
            rows.insert(
                ids.value(i).to_owned(),
                StoredChunk {
                    text: column("text").value(i).to_owned(),
                    repo: column("repo").value(i).to_owned(),
                    workdir: column("workdir").value(i).to_owned(),
                    turn_uuid: column("turn_uuid").value(i).to_owned(),
                },
            );
        }
    }
    rows
}

#[tokio::test]
async fn copilot_native_events_index_incrementally_and_read_back() {
    let checkout = tempfile::tempdir().unwrap();
    for args in [
        vec!["init", "-q"],
        vec!["remote", "add", "origin", "https://github.com/current/funes.git"],
        vec!["remote", "add", "upstream", "git@github.com:upstream/funes.git"],
    ] {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(checkout.path())
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let incremental_source = tempfile::tempdir().unwrap();
    let incremental_memory = tempfile::tempdir().unwrap();
    std::env::set_var("FUNES_HOME", incremental_memory.path());
    write_session(incremental_source.path(), checkout.path(), false);

    index_copilot(incremental_source.path()).await;
    let first_count = chunk_count().await;
    assert!(first_count > 0, "initial Copilot events produced no chunks");
    let initial_content = stored_content().await;
    assert!(initial_content.values().any(|chunk| chunk.turn_uuid == "fallback-user"));
    let original_workdir = funes::traces::jsonl::workdir_of_cwd(checkout.path().to_str().unwrap()).unwrap();
    for chunk in initial_content.values() {
        let expected = if chunk.turn_uuid == "fallback-user" {
            "current/funes upstream/funes"
        } else {
            "historical/funes"
        };
        assert_eq!(chunk.repo, expected, "repository attribution for {}", chunk.turn_uuid);
        assert_eq!(
            chunk.workdir, original_workdir,
            "recorded context takes precedence over workspace.yaml"
        );
    }

    write_session(incremental_source.path(), checkout.path(), true);
    index_copilot(incremental_source.path()).await;
    let incremental_count = chunk_count().await;
    assert!(
        incremental_count > first_count,
        "the appended event should add chunks: {first_count} -> {incremental_count}"
    );

    // Re-indexing the unchanged native session is idempotent.
    index_copilot(incremental_source.path()).await;
    assert_eq!(chunk_count().await, incremental_count);

    let session = "native-copilot-0001".to_string();
    let memory = funes::memory::Memory::local();
    let recalled = funes::commands::recall::recall(
        memory.clone(),
        "Copilot transcripts indexed durable events".into(),
        5,
        30,
        0.0,
        1,
        None,
        Some("copilot".into()),
    )
    .await
    .unwrap();
    assert!(
        recalled.contains(&session),
        "harness-filtered recall missed session: {recalled}"
    );
    assert!(
        recalled.contains("copilot"),
        "recall did not render the harness: {recalled}"
    );

    let listed = funes::commands::recall::sessions(memory.clone(), Default::default())
        .await
        .unwrap();
    assert!(
        listed.contains("copilot") && listed.contains(&session),
        "session listing: {listed}"
    );

    let got = funes::commands::recall::get(
        memory.clone(),
        session.clone(),
        funes::commands::recall::TurnRange::default(),
    )
    .await
    .unwrap();
    assert!(got.contains("tool-result marker"), "get omitted tool output: {got}");

    let sketched = funes::commands::sketch::run(memory.clone(), session.clone(), None, None, Some(20), Some(20_000))
        .await
        .unwrap();
    assert!(
        sketched.contains("tool_result (shell)"),
        "sketch omitted the correlated tool name: {sketched}"
    );

    let scanned = funes::commands::recall::scan(memory, "tool-result marker".into(), session, None, None, false, 40)
        .await
        .unwrap();
    assert!(
        scanned.contains("tool_result"),
        "scan omitted tool-result block type: {scanned}"
    );

    let incremental_content = stored_content().await;
    for (id, chunk) in &initial_content {
        assert_eq!(
            incremental_content.get(id),
            Some(chunk),
            "earlier content and attribution changed"
        );
    }
    assert!(!checkout.path().join("missing-checkout").exists());
    let appended = incremental_content
        .values()
        .find(|chunk| chunk.turn_uuid == "user-2")
        .unwrap();
    assert_eq!(
        appended.repo, "historical/other",
        "recorded attribution survives a missing checkout"
    );
    assert_ne!(
        appended.workdir, original_workdir,
        "the appended turn uses the changed context"
    );
    let scratch_source = tempfile::tempdir().unwrap();
    let scratch_memory = tempfile::tempdir().unwrap();
    write_session(scratch_source.path(), checkout.path(), true);
    std::env::set_var("FUNES_HOME", scratch_memory.path());
    index_copilot(scratch_source.path()).await;
    assert_eq!(
        chunk_count().await,
        incremental_count,
        "incremental Copilot indexing must match from-scratch chunk count"
    );
    assert_eq!(
        stored_content().await,
        incremental_content,
        "incremental IDs, text, and attribution must match a fresh index"
    );
}
