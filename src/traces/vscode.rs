//! Parser for native VS Code chat session files (flat JSON and mutation-log JSONL).
//!
//! VS Code's object mutation log is intentionally generic; replay it first, then translate the
//! resulting version-3 session object into the common trace model. Semantics are pinned to
//! microsoft/vscode commit `27a1023fe171a64960235c94456ddac92e81414d`:
//! <https://github.com/microsoft/vscode/blob/27a1023fe171a64960235c94456ddac92e81414d/src/vs/workbench/contrib/chat/common/model/objectMutationLog.ts>.

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde_json::{Map, Value};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::path::PathBuf;

use super::jsonl;
use super::{Block, Turn};

/// One replayed snapshot and its source metadata, read together for consistent attribution.
pub struct ParsedSession {
    pub turns: Vec<Turn>,
    pub cwd: Option<String>,
}

/// Read the complete native snapshot and extract only terminal request/response pairs.
pub fn turns_from_file(path: &Path) -> Result<Vec<Turn>> {
    Ok(read_session(path)?.turns)
}

pub fn read_session(path: &Path) -> Result<ParsedSession> {
    let session = read_value(path)?;
    let session_id = session
        .get("sessionId")
        .and_then(Value::as_str)
        .or_else(|| session.get("id").and_then(Value::as_str))
        .unwrap_or_else(|| path.file_stem().and_then(|s| s.to_str()).unwrap_or("vscode"));
    let cwd = cwd_of_value(&session).or_else(|| metadata_cwd(path));
    let workdir = cwd.as_deref().and_then(jsonl::workdir_of_cwd).unwrap_or_default();
    let creation = timestamp_value(session.get("creationDate"));
    let requests = session
        .get("requests")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("VS Code session has no requests array"))?;
    let mut turns = Vec::new();
    for (ordinal, request) in requests.iter().enumerate() {
        let req = request
            .as_object()
            .ok_or_else(|| anyhow!("request {ordinal} is not an object"))?;
        let request_id = req
            .get("requestId")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| format!("{session_id}-request-{ordinal}"));
        let user_text = req
            .get("message")
            .and_then(|m| m.get("text").or_else(|| m.get("content")))
            .and_then(string_value)
            .unwrap_or_default();
        let response = req.get("response");
        let mut user_blocks = Vec::new();
        if !user_text.trim().is_empty() {
            user_blocks.push(text_block("text", user_text));
        }
        let req_ts = timestamp_value(req.get("timestamp"))
            .or_else(|| creation.clone())
            .unwrap_or_default();
        let state = match req.get("modelState") {
            None => None, // Legacy snapshots restore as complete/cancelled in VS Code.
            Some(v) => {
                let value = v
                    .get("value")
                    .and_then(Value::as_i64)
                    .ok_or_else(|| anyhow!("invalid VS Code modelState.value"))?;
                if !(0..=4).contains(&value) {
                    bail!("unsupported VS Code modelState.value {value}");
                }
                Some(value)
            }
        };
        if matches!(state, Some(0 | 4)) {
            continue;
        }
        if !user_blocks.is_empty() {
            turns.push(make_turn(
                session_id,
                &workdir,
                &request_id,
                ordinal as i64 * 2,
                req_ts.clone(),
                "user",
                user_blocks,
                path,
            ));
        }
        let mut assistant_blocks = Vec::new();
        if let Some(response) = response {
            if let Some(parts) = response.as_array() {
                for part in parts {
                    collect_response_blocks(part, &mut assistant_blocks);
                }
            } else {
                collect_response_blocks(response, &mut assistant_blocks);
            }
        }
        if !assistant_blocks.is_empty() {
            let response_id = req
                .get("responseId")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| format!("{session_id}-request-{ordinal}-assistant"));
            let ts = timestamp_value(req.get("responseTimestamp"))
                .or_else(|| timestamp_value(req.get("timestamp")))
                .or_else(|| creation.clone())
                .unwrap_or(req_ts);
            turns.push(make_turn(
                session_id,
                &workdir,
                &response_id,
                ordinal as i64 * 2 + 1,
                ts,
                "assistant",
                assistant_blocks,
                path,
            ));
        }
    }
    Ok(ParsedSession { turns, cwd })
}

/// The raw local cwd used by repository attribution.
pub fn cwd_of_file(path: &Path) -> Option<String> {
    read_value(path)
        .ok()
        .and_then(|v| cwd_of_value(&v))
        .or_else(|| metadata_cwd(path))
}

pub(crate) fn metadata_path(path: &Path) -> Option<PathBuf> {
    let parent = path.parent()?;
    for dir in [Some(parent), parent.parent()].into_iter().flatten() {
        let candidate = dir.join("workspace.json");
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

fn metadata_cwd(path: &Path) -> Option<String> {
    let value: Value = serde_json::from_reader(fs::File::open(metadata_path(path)?).ok()?).ok()?;
    // A multi-root workspace file is not itself a working directory. Without a recorded cwd
    // there is no single repository to attribute to the session.
    value.get("folder").and_then(Value::as_str).and_then(file_uri_to_path)
}

fn read_value(path: &Path) -> Result<Value> {
    let file = fs::File::open(path).with_context(|| format!("reading {}", path.display()))?;
    if path.extension().and_then(|s| s.to_str()) != Some("jsonl") {
        let value: Value = serde_json::from_reader(BufReader::new(file))
            .with_context(|| format!("parsing VS Code session {}", path.display()))?;
        validate_session(&value)?;
        return Ok(value);
    }
    let mut state: Option<Value> = None;
    for (line_no, line) in BufReader::new(file).lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let entry: Value =
            serde_json::from_str(&line).with_context(|| format!("parsing VS Code mutation line {}", line_no + 1))?;
        apply_entry(&mut state, &entry).with_context(|| format!("applying VS Code mutation line {}", line_no + 1))?;
    }
    let state = state.ok_or_else(|| anyhow!("empty VS Code mutation log"))?;
    validate_session(&state)?;
    Ok(state)
}

fn apply_entry(state: &mut Option<Value>, entry: &Value) -> Result<()> {
    let obj = entry
        .as_object()
        .ok_or_else(|| anyhow!("mutation entry is not an object"))?;
    let kind = obj
        .get("kind")
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow!("mutation kind missing"))?;
    match kind {
        0 => {
            *state = Some(
                obj.get("v")
                    .cloned()
                    .ok_or_else(|| anyhow!("initial mutation missing v"))?,
            );
        }
        1 => {
            let root = state
                .as_mut()
                .ok_or_else(|| anyhow!("mutation log missing initial entry"))?;
            set_path(
                root,
                path(obj)?,
                obj.get("v").cloned().ok_or_else(|| anyhow!("set mutation missing v"))?,
            )?;
        }
        2 => {
            let root = state
                .as_mut()
                .ok_or_else(|| anyhow!("mutation log missing initial entry"))?;
            push_path(root, path(obj)?, obj.get("v"), obj.get("i"))?;
        }
        3 => {
            let root = state
                .as_mut()
                .ok_or_else(|| anyhow!("mutation log missing initial entry"))?;
            delete_path(root, path(obj)?)?;
        }
        _ => bail!("unsupported VS Code mutation kind {kind}"),
    }
    Ok(())
}

fn path(obj: &Map<String, Value>) -> Result<Vec<String>> {
    obj.get("k")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("mutation path missing k"))?
        .iter()
        .map(|v| match v {
            Value::String(s) => Ok(s.clone()),
            Value::Number(n) => Ok(n.to_string()),
            _ => bail!("mutation path segment is not string/number"),
        })
        .collect()
}
fn set_path(root: &mut Value, path: Vec<String>, value: Value) -> Result<()> {
    if path.is_empty() {
        return Ok(());
    }
    let mut cur = root;
    for key in &path[..path.len() - 1] {
        cur = child_mut(cur, key)?;
    }
    match cur {
        Value::Object(o) => {
            o.insert(path[path.len() - 1].clone(), value);
            Ok(())
        }
        Value::Array(a) => {
            let i: usize = path[path.len() - 1].parse().context("array path index")?;
            if i >= a.len() {
                bail!("array path index out of bounds")
            }
            a[i] = value;
            Ok(())
        }
        _ => bail!("mutation parent is not object/array"),
    }
}
fn delete_path(root: &mut Value, path: Vec<String>) -> Result<()> {
    if path.is_empty() {
        return Ok(());
    }
    let mut cur = root;
    for key in &path[..path.len() - 1] {
        cur = child_mut(cur, key)?;
    }
    match cur {
        Value::Object(o) => {
            o.remove(&path[path.len() - 1]);
            Ok(())
        }
        Value::Array(a) => {
            let i: usize = path[path.len() - 1].parse().context("array delete index")?;
            if i >= a.len() {
                bail!("array delete index out of bounds")
            }
            // JavaScript assignment of `undefined` leaves an array hole; JSON serialization
            // later represents that hole as null and preserves the array length.
            a[i] = Value::Null;
            Ok(())
        }
        _ => bail!("delete parent is not object/array"),
    }
}
fn push_path(root: &mut Value, path: Vec<String>, values: Option<&Value>, index: Option<&Value>) -> Result<()> {
    let mut cur = root;
    for key in &path[..path.len().saturating_sub(1)] {
        cur = child_mut(cur, key)?;
    }
    let key = path.last().ok_or_else(|| anyhow!("push path empty"))?;
    if let Value::Object(object) = cur {
        if object.get(key).is_none_or(Value::is_null) {
            object.insert(key.clone(), Value::Array(Vec::new()));
        }
    }
    let arr = child_mut(cur, key)?
        .as_array_mut()
        .ok_or_else(|| anyhow!("push target is not array"))?;
    if let Some(i) = index {
        let length = usize::try_from(i.as_u64().ok_or_else(|| anyhow!("push index is not integer"))?)?;
        // A generated mutation can only truncate the already stored array. Reject corrupt
        // sparse growth instead of allocating unbounded null slots from an untrusted index.
        anyhow::ensure!(length <= arr.len(), "push index exceeds array length");
        arr.truncate(length);
    }
    if let Some(v) = values {
        for item in v.as_array().ok_or_else(|| anyhow!("push v is not array"))? {
            arr.push(item.clone());
        }
    }
    Ok(())
}

fn child_mut<'a>(value: &'a mut Value, key: &str) -> Result<&'a mut Value> {
    match value {
        Value::Object(obj) => obj
            .get_mut(key)
            .ok_or_else(|| anyhow!("mutation path not found: {key}")),
        Value::Array(arr) => {
            let index: usize = key.parse().context("array path index")?;
            arr.get_mut(index)
                .ok_or_else(|| anyhow!("array path index out of bounds"))
        }
        _ => bail!("mutation path parent is not object/array"),
    }
}

fn validate_session(v: &Value) -> Result<()> {
    let version = v
        .get("version")
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow!("VS Code session version missing or invalid"))?;
    if version != 3 {
        bail!("unsupported VS Code session version {version}");
    }
    if !v.is_object() {
        bail!("VS Code session is not an object");
    }
    Ok(())
}
fn string_value(v: &Value) -> Option<&str> {
    v.as_str().or_else(|| v.get("value").and_then(Value::as_str))
}
fn text_block(kind: &str, text: &str) -> Block {
    Block {
        block_type: kind.into(),
        text: text.into(),
        tool_name: None,
        tool_use_id: None,
    }
}
fn collect_response_blocks(v: &Value, out: &mut Vec<Block>) {
    let Some(o) = v.as_object() else {
        if let Some(s) = v.as_str() {
            if !s.trim().is_empty() {
                out.push(text_block("text", s));
            }
        }
        return;
    };
    match o.get("kind").and_then(Value::as_str).unwrap_or("") {
        "markdownContent" => {
            if let Some(s) = o.get("content").and_then(string_value) {
                if !s.trim().is_empty() {
                    out.push(text_block("text", s));
                }
            }
        }
        "thinking" => {
            if let Some(value) = o.get("value").or_else(|| o.get("text")).or_else(|| o.get("content")) {
                if let Some(text) = stored_text(value) {
                    if !text.trim().is_empty() {
                        out.push(text_block("thinking", &text));
                    }
                }
            }
        }
        "toolInvocationSerialized" => {
            let text = o
                .get("toolSpecificData")
                .and_then(tool_input_text)
                .or_else(|| o.get("resultDetails").and_then(tool_input_text))
                .or_else(|| {
                    o.get("invocationMessage").and_then(|v| {
                        string_value(v)
                            .map(str::to_string)
                            .or_else(|| serde_json::to_string(v).ok())
                    })
                })
                .unwrap_or_default();
            out.push(Block {
                block_type: "tool_use".into(),
                text,
                tool_name: o.get("toolId").and_then(Value::as_str).map(str::to_string),
                tool_use_id: o.get("toolCallId").and_then(Value::as_str).map(str::to_string),
            });
            if let Some(result) = o.get("resultDetails").and_then(tool_output_text).or_else(|| {
                o.get("toolSpecificData")
                    .and_then(|v| v.get("result"))
                    .and_then(tool_output_text)
            }) {
                if !result.trim().is_empty() {
                    out.push(Block {
                        block_type: "tool_result".into(),
                        text: result,
                        tool_name: o.get("toolId").and_then(Value::as_str).map(str::to_string),
                        tool_use_id: o.get("toolCallId").and_then(Value::as_str).map(str::to_string),
                    });
                }
            }
        }
        "" => {
            if let Some(s) = string_value(v) {
                if !s.trim().is_empty() {
                    out.push(text_block("text", s));
                }
            }
        }
        _ => {}
    }
}
fn timestamp_value(v: Option<&Value>) -> Option<String> {
    let ms = v.and_then(|x| x.as_i64())?;
    DateTime::<Utc>::from_timestamp_millis(ms).map(|d| d.to_rfc3339())
}
fn stored_text(v: &Value) -> Option<String> {
    if let Some(text) = string_value(v) {
        return Some(text.to_owned());
    }
    let parts = v.as_array()?;
    let texts: Vec<String> = parts
        .iter()
        .filter_map(|part| {
            if part.get("type").and_then(Value::as_str).is_some_and(|t| t != "text") {
                return None;
            }
            stored_text(part).or_else(|| part.get("text").and_then(stored_text))
        })
        .collect();
    (!texts.is_empty()).then(|| texts.join("\n"))
}

fn tool_input_text(v: &Value) -> Option<String> {
    if let Some(command) = v.get("commandLine") {
        return v
            .pointer("/presentationOverrides/commandLine")
            .or_else(|| command.get("forDisplay"))
            .or_else(|| command.get("userEdited"))
            .or_else(|| command.get("toolEdited"))
            .or_else(|| command.get("original"))
            .or(Some(command))
            .and_then(stored_text);
    }
    v.get("input")
        .or_else(|| v.get("prompt"))
        .map(|x| stored_text(x).unwrap_or_else(|| x.to_string()))
}
fn tool_output_text(v: &Value) -> Option<String> {
    stored_text(v.get("output").unwrap_or(v))
}
fn cwd_of_value(v: &Value) -> Option<String> {
    v.get("workingDirectory")
        .and_then(Value::as_str)
        .and_then(file_uri_to_path)
}
fn file_uri_to_path(uri: &str) -> Option<String> {
    let rest = uri.strip_prefix("file://")?;
    let rest = rest
        .strip_prefix("localhost/")
        .map(|p| format!("/{p}"))
        .unwrap_or_else(|| rest.to_owned());
    if !rest.starts_with('/') {
        return None;
    } // Remote authorities are not local git roots.
    let path = percent_decode(&rest);
    let path = if path.as_bytes().get(2) == Some(&b':') && path.as_bytes().get(1).is_some_and(u8::is_ascii_alphabetic) {
        path[1..].to_owned()
    } else {
        path
    };
    Some(path)
}
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = |b: u8| -> Option<u8> {
                match b {
                    b'0'..=b'9' => Some(b - b'0'),
                    b'a'..=b'f' => Some(b - b'a' + 10),
                    b'A'..=b'F' => Some(b - b'A' + 10),
                    _ => None,
                }
            };
            if let (Some(a), Some(b)) = (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                let n = a * 16 + b;
                out.push(n);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}
#[allow(clippy::too_many_arguments)]
fn make_turn(
    session: &str,
    workdir: &str,
    id: &str,
    seq: i64,
    ts: String,
    role: &str,
    blocks: Vec<Block>,
    path: &Path,
) -> Turn {
    Turn {
        session_id: session.into(),
        workdir: workdir.into(),
        turn_uuid: id.into(),
        parent_uuid: None,
        seq,
        ts,
        role: role.into(),
        blocks,
        source_path: path.to_string_lossy().into_owned(),
        harness: "vscode".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn file(suffix: &str, contents: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::Builder::new().suffix(suffix).tempfile().unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        f
    }

    #[test]
    fn replays_nested_array_push_set_delete_and_compact_jsonl() {
        let initial = r#"{"version":3,"sessionId":"s","requests":[{"requestId":"q","message":{"text":"hi"},"response":[],"modelState":{"value":1}}],"creationDate":1700000000000}"#;
        let lines = format!(
            "{{\"kind\":0,\"v\":{initial}}}\n{{\"kind\":1,\"k\":[\"requests\",0,\"message\",\"text\"],\"v\":\"hello\"}}\n{{\"kind\":2,\"k\":[\"requests\"],\"v\":[]}}\n{{\"kind\":3,\"k\":[\"requests\",0,\"message\",\"missing\"]}}\n"
        );
        let f = file(".jsonl", &lines);
        let turns = turns_from_file(f.path()).unwrap();
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].blocks[0].text, "hello");
    }

    #[test]
    fn defers_pending_pair_and_rejects_unknown_kind() {
        let flat = r#"{"version":3,"requests":[{"requestId":"q","message":{"text":"hi"},"response":[{"kind":"markdownContent","content":"partial"}],"modelState":{"value":4}}]}"#;
        let f = file(".json", flat);
        assert!(turns_from_file(f.path()).unwrap().is_empty());
        let bad = file(".jsonl", "{\"kind\":9}\n");
        assert!(turns_from_file(bad.path()).is_err());
    }

    #[test]
    fn stored_parts_legacy_ids_and_tiers() {
        let session = serde_json::json!({
            "version":3,"sessionId":"stable","creationDate":1700000000000i64,
            "workingDirectory":"file:///work/My%20Project",
            "requests":[{"message":{"text":"question"},"timestamp":1700000001000i64,"isCanceled":true,
                "response":[{"value":"saved markdown"},{"kind":"thinking","value":["think one","think two"]},
                    {"kind":"toolInvocationSerialized","toolId":"terminal","toolCallId":"call",
                     "invocationMessage":"Running command","toolSpecificData":{"kind":"terminal","commandLine":{"original":"cargo check","userEdited":"cargo test"}},
                     "resultDetails":{"input":"cargo test","output":[{"type":"text","value":"all tests passed"},{"type":"data","base64Data":"must not retain"}]}}
                ]}]
        });
        let f = file(".json", &session.to_string());
        let turns = turns_from_file(f.path()).unwrap();
        assert_eq!(turns.len(), 2);
        assert_eq!(turns[0].turn_uuid, "stable-request-0");
        assert_eq!(turns[1].turn_uuid, "stable-request-0-assistant");
        assert_eq!(turns[0].workdir, "-work-My-Project");
        assert_eq!(turns[1].ts, turns[0].ts);
        assert_eq!(
            turns[1]
                .blocks
                .iter()
                .map(|b| b.block_type.as_str())
                .collect::<Vec<_>>(),
            ["text", "thinking", "tool_use", "tool_result"]
        );
        assert_eq!(turns[1].blocks[2].text, "cargo test");
        assert_eq!(turns[1].blocks[3].tool_name.as_deref(), Some("terminal"));
        assert_eq!(turns[1].blocks[3].text, "all tests passed");
        let all = crate::chunk::chunks_from_turns(
            &turns,
            &[
                crate::chunk::Tier::Text,
                crate::chunk::Tier::ToolUse,
                crate::chunk::Tier::ToolResult,
            ],
            true,
        );
        let text = crate::chunk::chunks_from_turns(&turns, &[crate::chunk::Tier::Text], false);
        assert!(text.iter().all(|c| c.block_type == "text"));
        assert!(text.iter().all(|c| all.iter().any(|a| a.id == c.id)));
    }

    #[test]
    fn compacted_snapshot_and_retry_keep_native_identities() {
        let mut session = serde_json::json!({"version":3,"sessionId":"s","creationDate":1700000000000i64,
            "requests":[{"requestId":"q","responseId":"a1","message":{"text":"question"},"response":"old answer"}]});
        let first = file(".json", &session.to_string());
        let before = turns_from_file(first.path()).unwrap();
        let compact = file(".jsonl", &serde_json::json!({"kind":0,"v":session}).to_string());
        let after = turns_from_file(compact.path()).unwrap();
        assert_eq!(
            before.iter().map(|t| (&t.turn_uuid, t.seq)).collect::<Vec<_>>(),
            after.iter().map(|t| (&t.turn_uuid, t.seq)).collect::<Vec<_>>()
        );
        session["requests"][0]["responseId"] = serde_json::json!("a2");
        session["requests"][0]["response"] = serde_json::json!("new answer");
        let retry = file(".json", &session.to_string());
        let retry = turns_from_file(retry.path()).unwrap();
        assert_eq!(retry[0].turn_uuid, "q");
        assert_eq!(retry[1].turn_uuid, "a2");
    }

    #[test]
    fn mutation_truncation_initializes_absent_arrays_and_deletes_without_shifting() {
        use serde_json::json;
        let mut state = Some(json!({"items":[1,2,3]}));
        apply_entry(&mut state, &json!({"kind":2,"k":["items"],"i":1,"v":[4]})).unwrap();
        apply_entry(&mut state, &json!({"kind":2,"k":["new"],"v":["value"]})).unwrap();
        apply_entry(&mut state, &json!({"kind":3,"k":["items",0]})).unwrap();
        assert_eq!(state, Some(json!({"items":[null,4],"new":["value"]})));
        apply_entry(&mut state, &json!({"kind":1,"k":[],"v":0})).unwrap();
        assert!(state.unwrap().is_object());
    }

    #[test]
    fn malformed_versions_states_and_mutations_fail() {
        for value in [
            r#"{"requests":[]}"#,
            r#"{"version":2,"requests":[]}"#,
            r#"{"version":"3","requests":[]}"#,
            r#"{"version":3,"requests":[{"modelState":{"value":8}}]}"#,
            r#"{"version":3,"requests":[{"modelState":{"value":"pending"}}]}"#,
        ] {
            assert!(turns_from_file(file(".json", value).path()).is_err(), "{value}");
        }
        let bad = file(
            ".jsonl",
            "{\"kind\":0,\"v\":{\"version\":3,\"requests\":[]}}\n{\"kind\":",
        );
        assert!(turns_from_file(bad.path()).is_err());
        let missing = file(".jsonl", r#"{"kind":1,"k":["version"],"v":3}"#);
        assert!(turns_from_file(missing.path()).is_err());
    }

    #[test]
    fn workspace_folder_metadata_and_remote_uris() {
        let d = tempfile::tempdir().unwrap();
        let sessions = d.path().join("chatSessions");
        fs::create_dir(&sessions).unwrap();
        let p = sessions.join("s.json");
        fs::write(&p, r#"{"version":3,"requests":[]}"#).unwrap();
        fs::write(d.path().join("workspace.json"), r#"{"folder":"file:///work/project"}"#).unwrap();
        assert_eq!(cwd_of_file(&p).as_deref(), Some("/work/project"));
        assert!(file_uri_to_path("vscode-remote://ssh-remote+machine/work/project").is_none());
        assert!(file_uri_to_path("file://server/work/project").is_none());
        assert_eq!(
            file_uri_to_path("file://localhost/work/project").as_deref(),
            Some("/work/project")
        );
        assert_eq!(percent_decode("%é%20x"), "%é x");
    }
}
