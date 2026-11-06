//! Per-remote worklist of blocks held back by the push secret gate.
//!
//! Entries contain only transcript coordinates, never row text or scanner findings. Push and
//! swab serialize changes with the existing per-remote push lock; writes here replace the worklist
//! atomically so an interrupted update leaves the previous complete list.

use crate::home;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader, Write as _};
use std::path::{Path, PathBuf};

/// Stable coordinates identifying one block in a session transcript.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct BlockKey {
    pub session_id: String,
    pub turn_uuid: String,
    pub block_idx: i64,
}

impl BlockKey {
    pub fn new(session_id: impl Into<String>, turn_uuid: impl Into<String>, block_idx: i64) -> Self {
        Self {
            session_id: session_id.into(),
            turn_uuid: turn_uuid.into(),
            block_idx,
        }
    }
}

fn path(memory_uri: &str) -> PathBuf {
    home::pushed_dir().join("held").join(sanitize(memory_uri))
}

fn sanitize(uri: &str) -> String {
    uri.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn load_at(path: &Path) -> Result<HashSet<BlockKey>> {
    if !path.exists() {
        return Ok(HashSet::new());
    }
    let file = File::open(path).with_context(|| format!("opening held-block worklist at {}", path.display()))?;
    let mut keys = HashSet::new();
    for (line_no, line) in BufReader::new(file).lines().enumerate() {
        let line = line.with_context(|| format!("reading held-block worklist at {}", path.display()))?;
        if line.is_empty() {
            continue;
        }
        let key: BlockKey = serde_json::from_str(&line)
            .with_context(|| format!("parsing held-block worklist at {} line {}", path.display(), line_no + 1))?;
        keys.insert(key);
    }
    Ok(keys)
}

/// Load blocks that the secret gate has held for this remote.
pub fn load(memory_uri: &str) -> Result<HashSet<BlockKey>> {
    load_at(&path(memory_uri))
}

fn write_at(path: &Path, keys: &HashSet<BlockKey>) -> Result<()> {
    let dir = path.parent().context("held-block worklist has no parent directory")?;
    std::fs::create_dir_all(dir).context("creating the held-block worklist directory")?;
    let mut ordered: Vec<_> = keys.iter().collect();
    ordered.sort_unstable_by(|a, b| {
        (&a.session_id, &a.turn_uuid, a.block_idx).cmp(&(&b.session_id, &b.turn_uuid, b.block_idx))
    });
    let mut temp = tempfile::NamedTempFile::new_in(dir).context("creating a held-block worklist")?;
    for key in ordered {
        serde_json::to_writer(&mut temp, key).context("serializing a held-block key")?;
        temp.write_all(b"\n").context("writing a held-block key")?;
    }
    temp.flush().context("flushing the held-block worklist")?;
    temp.persist(path)
        .map_err(|e| anyhow::Error::new(e.error).context("saving the held-block worklist"))?;
    Ok(())
}

/// Add blocks the push gate held back. Caller holds the matching remote's push lock.
pub fn add(memory_uri: &str, additions: impl IntoIterator<Item = BlockKey>) -> Result<()> {
    add_at(&path(memory_uri), additions)
}

fn add_at(path: &Path, additions: impl IntoIterator<Item = BlockKey>) -> Result<()> {
    let mut keys = load_at(path)?;
    keys.extend(additions);
    write_at(path, &keys)
}

/// Retire blocks swab successfully classified and repaired. Caller holds the matching remote's
/// push lock so no in-flight push can add a newer finding while these keys are removed.
pub fn remove(memory_uri: &str, retired: &HashSet<BlockKey>) -> Result<()> {
    remove_at(&path(memory_uri), retired)
}

fn remove_at(path: &Path, retired: &HashSet<BlockKey>) -> Result<()> {
    let mut keys = load_at(path)?;
    keys.retain(|key| !retired.contains(key));
    write_at(path, &keys)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worklist_add_merges_and_remove_retires_only_selected_keys() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("held");
        let one = BlockKey::new("session-1", "turn-1", 0);
        let two = BlockKey::new("session-2", "turn-2", 1);
        let three = BlockKey::new("session-3", "turn-3", 2);

        add_at(&path, [one.clone()]).unwrap();
        add_at(&path, [two.clone()]).unwrap();
        assert_eq!(load_at(&path).unwrap(), HashSet::from([one.clone(), two.clone()]));

        remove_at(&path, &HashSet::from([one])).unwrap();
        add_at(&path, [three.clone()]).unwrap();
        assert_eq!(load_at(&path).unwrap(), HashSet::from([two, three]));
    }

    #[test]
    fn malformed_worklist_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("held");
        std::fs::write(&path, "not json\n").unwrap();
        assert!(load_at(&path).is_err());
    }
}
