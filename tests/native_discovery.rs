//! Exercise real CLI root selection, both VS Code editions, profiles, and Copilot overrides.
//! Synthetic v3 snapshots / persisted Copilot events use the pinned schemas recorded in
//! vscode_ingestion.rs and copilot_ingestion.rs.

use std::path::{Path, PathBuf};
use std::process::Command;

fn run(home: &Path, memory: &Path, args: &[&str]) -> String {
    // Isolate discovery while reusing the model cache, just like the rest of the integration suite.
    let model_cache = std::env::var_os("HF_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(std::env::var_os("HOME").unwrap_or_default()).join(".cache/huggingface"));
    let output = Command::new(env!("CARGO_BIN_EXE_funes"))
        .args(args)
        .env("HOME", home)
        .env("USERPROFILE", home)
        .env("APPDATA", home.join("config"))
        .env("XDG_CONFIG_HOME", home.join("config"))
        .env("COPILOT_HOME", home.join("custom-copilot"))
        .env("FUNES_HOME", memory)
        .env("HF_HOME", model_cache)
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn all_vscode_roots_and_copilot_share_memory_without_merging_sessions() {
    let home = tempfile::tempdir().unwrap();
    let memory = tempfile::tempdir().unwrap();
    let base = if cfg!(target_os = "macos") {
        home.path().join("Library/Application Support")
    } else {
        home.path().join("config")
    };
    for (edition, store, id) in [
        ("Code", "workspaceStorage/project/chatSessions", "stable-session"),
        (
            "Code - Insiders",
            "globalStorage/emptyWindowChatSessions",
            "insiders-session",
        ),
        (
            "Code",
            "profiles/profile1/globalStorage/emptyWindowChatSessions",
            "profile-session",
        ),
    ] {
        let store = base.join(edition).join("User").join(store);
        std::fs::create_dir_all(&store).unwrap();
        let snapshot = serde_json::json!({"version":3,"sessionId":id,"workingDirectory":"file:///work/shared",
            "creationDate":1700000000000i64,"requests":[{"requestId":"q","responseId":"a",
            "modelState":{"value":1},"message":{"text":"Remember the shared workspace investigation."},
            "response":[{"value":"Each producer keeps its own session identity."}]}]});
        std::fs::write(store.join(format!("{id}.json")), snapshot.to_string()).unwrap();
        // Prefer the log but retain the same native identities as the old snapshot.
        std::fs::write(
            store.join(format!("{id}.jsonl")),
            serde_json::json!({"kind":0,"v":snapshot}).to_string(),
        )
        .unwrap();
    }
    run(home.path(), memory.path(), &["index", "--harness", "vscode", "--yes"]);
    let before = run(home.path(), memory.path(), &["sessions"]);
    for id in ["stable-session", "insiders-session", "profile-session"] {
        assert!(before.contains(id), "missing {id}: {before}");
    }
    let copilot = home.path().join("custom-copilot/session-state/copilot-session");
    std::fs::create_dir_all(&copilot).unwrap();
    std::fs::write(copilot.join("events.jsonl"), concat!(
        "{\"type\":\"session.start\",\"id\":\"start\",\"data\":{\"sessionId\":\"copilot-session\",\"context\":{\"cwd\":\"/work/shared\"}}}\n",
        "{\"type\":\"user.message\",\"id\":\"q\",\"timestamp\":\"2023-11-14T22:13:20Z\",\"data\":{\"content\":\"Remember the shared workspace investigation.\"}}\n"
    )).unwrap();
    run(home.path(), memory.path(), &["index", "--harness", "copilot", "--yes"]);
    let after = run(home.path(), memory.path(), &["sessions"]);
    for id in [
        "stable-session",
        "insiders-session",
        "profile-session",
        "copilot-session",
    ] {
        assert!(after.contains(id), "missing {id}: {after}");
    }
    assert!(after.contains("copilot") && after.contains("vscode"));
    run(home.path(), memory.path(), &["index", "--harness", "vscode", "--yes"]);
    assert_eq!(run(home.path(), memory.path(), &["sessions"]), after);
}
