//! Per-remote worklist of blocks held back by the push secret gate.
//!
//! Entries contain only transcript coordinates, never row text or scanner findings. Push and
//! swab serialize changes with the existing per-remote push lock. [Persisting the temporary file]
//! atomically replaces the worklist, so readers see either the old list or the new one.
//!
//! [Persisting the temporary file]: https://docs.rs/tempfile/3.27.0/tempfile/struct.NamedTempFile.html#method.persist

use crate::home;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader, Write as _};
use std::path::{Path, PathBuf};

/// Stable coordinates identifying one block in a session transcript.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
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
    // A slash and a literal underscore can distinguish two Hub targets.
    home::pushed_dir()
        .join("held")
        .join(hex::encode(Sha256::digest(memory_uri)))
}

fn load_at(path: &Path) -> Result<HashSet<BlockKey>> {
    let file = match File::open(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(HashSet::new()),
        result => result.with_context(|| format!("opening held-block worklist at {}", path.display()))?,
    };
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
    ordered.sort_unstable();
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
    let before = keys.len();
    keys.extend(additions);
    if keys.len() == before {
        return Ok(());
    }
    write_at(path, &keys)
}

/// Retire the complete worklist after the last batch succeeds, still under the remote's push lock.
pub fn clear(memory_uri: &str) -> Result<()> {
    let path = path(memory_uri);
    std::fs::remove_file(&path).with_context(|| format!("retiring held-block worklist at {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worklist_merges_repeated_pushes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("held");
        let one = BlockKey::new("session-1", "turn-1", 0);
        let two = BlockKey::new("session-2", "turn-2", 1);

        add_at(&path, [one.clone()]).unwrap();
        add_at(&path, [one.clone(), two.clone()]).unwrap();
        assert_eq!(load_at(&path).unwrap(), HashSet::from([one, two]));
    }

    #[test]
    fn distinct_hub_targets_have_distinct_worklists() {
        assert_ne!(path("hf://datasets/team/run_1"), path("hf://datasets/team_run/1"));
    }

    #[test]
    fn malformed_worklist_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("held");
        std::fs::write(&path, "not json\n").unwrap();
        assert!(load_at(&path).is_err());
    }
}
