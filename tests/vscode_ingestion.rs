//! End-to-end coverage for VS Code's native v3 mutation-log chat sessions.
//!
//! The mutation format is pinned to microsoft/vscode commit
//! `27a1023fe171a64960235c94456ddac92e81414d` (objectMutationLog.ts). The fixture is synthetic
//! and contains no private session data.

use std::io::Write;
use std::path::Path;

use funes::traces::harness::Harness;
use serde_json::{json, Value};

fn snapshot(pending: bool) -> Value {
    let mut requests = vec![json!({
        "requestId": "q1",
        "responseId": "a1",
        "timestamp": 1_756_800_001_000i64,
        "responseTimestamp": 1_756_800_002_000i64,
        "modelState": {"value": 1},
        "message": {"text": "Index this VS Code chat session."},
        "response": [{"kind":"markdownContent","content":"The initial response is complete."}]
    })];
    requests.push(json!({
        "requestId": "q2",
        "responseId": "a2",
        "timestamp": 1_756_800_003_000i64,
        "modelState": {"value": if pending { 0 } else { 1 }},
        "message": {"text": "Verify the mutation log incrementally."},
        "response": if pending { Value::Null } else {
            json!([{"kind":"markdownContent","content":"The final response is present."}])
        }
    }));
    json!({
        "version": 3,
        "sessionId": "vscode-native-0001",
        "creationDate": 1_756_800_000_000i64,
        "workingDirectory": "file:///work/funes-vscode",
        "requests": requests
    })
}

fn write_initial(path: &Path) {
    let mut file = std::fs::File::create(path).unwrap();
    writeln!(file, "{}", json!({"kind":0,"v":snapshot(true)})).unwrap();
}

fn append_completion_patch(path: &Path) {
    let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    writeln!(
        file,
        "{}",
        json!({"kind":1,"k":["requests","1","modelState"],"v":{"value":1}})
    )
    .unwrap();
    writeln!(
        file,
        "{}",
        json!({
            "kind":1,
            "k":["requests","1","response"],
            "v":[{"kind":"markdownContent","content":"The final response is present."}]
        })
    )
    .unwrap();
}

fn write_compacted(path: &Path) {
    let mut file = std::fs::File::create(path).unwrap();
    writeln!(file, "{}", json!({"kind":0,"v":snapshot(false)})).unwrap();
}

fn chunk_count(status: &str) -> usize {
    status
        .lines()
        .find_map(|line| line.strip_prefix("chunks: "))
        .and_then(|n| n.trim().parse().ok())
        .expect("status reports a chunk count")
}

async fn status() -> String {
    funes::commands::recall::status(funes::memory::Memory::local())
        .await
        .unwrap()
}

async fn index(root: &Path) {
    funes::commands::index::run_index_roots(&[(root.to_path_buf(), Some(Harness::Vscode))], false, None, true)
        .await
        .unwrap();
}

async fn stored_content() -> std::collections::BTreeMap<String, String> {
    use arrow_array::Array;
    let ds = funes::memory::Memory::local().open().await.unwrap();
    let batches = funes::memory::dataset::scan_rows(&ds, &["id", "text"], None, None)
        .await
        .unwrap();
    let mut rows = std::collections::BTreeMap::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let text = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        for i in 0..ids.len() {
            rows.insert(ids.value(i).to_owned(), text.value(i).to_owned());
        }
    }
    rows
}

#[tokio::test]
async fn vscode_mutation_log_defers_pending_responses_and_converges() {
    let source = tempfile::tempdir().unwrap();
    let path = source.path().join("vscode-session.jsonl");
    write_initial(&path);

    let memory = tempfile::tempdir().unwrap();
    std::env::set_var("FUNES_HOME", memory.path());
    index(&path).await;
    let initial = chunk_count(&status().await);
    assert!(initial > 0, "completed initial request produced no chunks");

    // The pending second request contributes no turns until its terminal state and response exist.
    append_completion_patch(&path);
    index(&path).await;
    let incremental = chunk_count(&status().await);
    assert!(incremental > initial, "completion patch should add the deferred pair");

    // Re-running the unchanged mutation log cannot duplicate chunks.
    index(&path).await;
    assert_eq!(chunk_count(&status().await), incremental);

    // A compacted kind-0 snapshot with the same stable request/response IDs also deduplicates.
    write_compacted(&path);
    index(&path).await;
    assert_eq!(chunk_count(&status().await), incremental);

    // A corrupt trailing mutation is retried rather than stamped as indexed; repairing it keeps
    // the same rows and therefore remains idempotent.
    let state_before = std::fs::read(memory.path().join("state.json")).unwrap();
    let mut corrupt = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
    writeln!(corrupt, "{{\"kind\":1").unwrap();
    index(&path).await;
    assert_eq!(chunk_count(&status().await), incremental);
    assert_eq!(
        std::fs::read(memory.path().join("state.json")).unwrap(),
        state_before,
        "corrupt log must not advance index state"
    );
    write_compacted(&path);
    index(&path).await;
    assert_eq!(chunk_count(&status().await), incremental);

    let memory = funes::memory::Memory::local();
    let session = "vscode-native-0001".to_string();
    let recalled = funes::commands::recall::recall(
        memory.clone(),
        "final response mutation log".into(),
        5,
        30,
        0.0,
        1,
        None,
        Some("vscode".into()),
    )
    .await
    .unwrap();
    assert!(
        recalled.contains(&session),
        "harness-filtered recall missed VS Code session: {recalled}"
    );
    assert!(
        recalled.contains("vscode"),
        "recall omitted VS Code harness: {recalled}"
    );

    let listed = funes::commands::recall::sessions(memory.clone(), Default::default())
        .await
        .unwrap();
    assert!(
        listed.contains("vscode") && listed.contains(&session),
        "session listing: {listed}"
    );
    let got = funes::commands::recall::get(
        memory.clone(),
        session.clone(),
        funes::commands::recall::TurnRange::default(),
    )
    .await
    .unwrap();
    assert!(
        got.contains("The final response is present"),
        "get omitted final response: {got}"
    );
    let scanned = funes::commands::recall::scan(memory, "final response".into(), session, None, None, false, 40)
        .await
        .unwrap();
    assert!(scanned.contains("text"), "scan omitted final response block: {scanned}");

    // The final compact snapshot indexed from a fresh memory must have the same stored row count.
    let incremental_content = stored_content().await;
    let fresh_source = tempfile::tempdir().unwrap();
    let fresh_path = fresh_source.path().join("vscode-session.jsonl");
    write_compacted(&fresh_path);
    let fresh_memory = tempfile::tempdir().unwrap();
    std::env::set_var("FUNES_HOME", fresh_memory.path());
    index(&fresh_path).await;
    assert_eq!(chunk_count(&status().await), incremental);
    assert_eq!(
        stored_content().await,
        incremental_content,
        "incremental IDs and text must match fresh"
    );
}
