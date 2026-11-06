//! Chunk ids deleted by swab, retained so indexing cannot re-add the original transcript text.

use crate::home;
use anyhow::{Context, Result};
use std::collections::HashSet;
use std::io::Write as _;
use std::path::Path;

/// Load chunk ids that indexing must continue to suppress. A missing file means no suppressions;
/// malformed state is an error so indexing never silently restores a removed source row.
pub fn load() -> Result<HashSet<String>> {
    load_at(&home::suppressed_chunks_file())
}

fn load_at(path: &Path) -> Result<HashSet<String>> {
    if !path.exists() {
        return Ok(HashSet::new());
    }
    let contents = std::fs::read_to_string(&path)
        .with_context(|| format!("reading suppressed chunk ids at {}", path.display()))?;
    let mut ids = HashSet::new();
    for (line_no, line) in contents.lines().enumerate() {
        if line.is_empty() {
            continue;
        }
        if line.len() != 16 || !line.bytes().all(|b| b.is_ascii_hexdigit()) {
            anyhow::bail!("invalid suppressed chunk id at {} line {}", path.display(), line_no + 1);
        }
        ids.insert(line.to_string());
    }
    Ok(ids)
}

/// Record ids that a swab replacement removed. Call before committing the replacement rows; if
/// the commit fails, the old dirty row stays stored and push-gated, while its source cannot be
/// re-added as a second copy.
pub fn record(ids: impl IntoIterator<Item = String>) -> Result<()> {
    let path = home::suppressed_chunks_file();
    let mut all = load()?;
    all.extend(ids);
    write_at(&path, &all)
}

fn write_at(path: &Path, ids: &HashSet<String>) -> Result<()> {
    let dir = path.parent().context("suppressed chunk file has no parent directory")?;
    std::fs::create_dir_all(dir).context("creating the suppressed chunk directory")?;
    let mut ordered: Vec<_> = ids.iter().collect();
    ordered.sort_unstable();
    let mut temp = tempfile::NamedTempFile::new_in(dir).context("creating suppressed chunk state")?;
    for id in ordered {
        writeln!(temp, "{id}").context("writing suppressed chunk state")?;
    }
    temp.flush().context("flushing suppressed chunk state")?;
    temp.persist(path)
        .map_err(|e| anyhow::Error::new(e.error).context("saving suppressed chunk state"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_is_sorted_and_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("suppressed");
        let ids = HashSet::from(["bbbbbbbbbbbbbbbb".to_string(), "aaaaaaaaaaaaaaaa".to_string()]);
        write_at(&path, &ids).unwrap();
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "aaaaaaaaaaaaaaaa\nbbbbbbbbbbbbbbbb\n"
        );
        assert_eq!(load_at(&path).unwrap(), ids);
    }

    #[test]
    fn invalid_ids_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("suppressed");
        std::fs::write(&path, "not-an-id\n").unwrap();
        assert!(load_at(&path).is_err());
    }
}
