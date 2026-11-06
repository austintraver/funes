//! Repair the local blocks the egress scanner withheld. A block key survives compaction and
//! late indexing; a fragment number or the original turn's timestamp does not identify that work.

use super::scrub;
use crate::memory::{dataset, held, lock, receipt, swab, tombstone, Memory};
use crate::scan;
use anyhow::{bail, Context, Result};
use std::collections::HashSet;

/// Drain this remote's local held-block list in batches. A crash leaves unretired keys for a
/// retry: each batch is re-read and reclassified, and an already-clean or absent block needs no write.
pub async fn run(spec: &str) -> Result<String> {
    let Memory::Remote { uri } = Memory::parse(spec) else {
        bail!("swab expects the remote memory named by push; it repairs that memory's withheld blocks locally");
    };
    let _push_lock = lock::try_lock_file(&receipt::push_lock_path(&uri))?
        .context("another push or swab is in progress for this memory")?;
    let queued: Vec<_> = held::load(&uri)?.into_iter().collect();
    if queued.is_empty() {
        return Ok(format!("{uri}: no withheld blocks queued for swab"));
    }
    let _writer = lock::MemoryLock::acquire()?;
    let mut ds = Memory::local().open().await?;
    let model = dataset::embedding_model(&ds)?;
    let scanner = scan::Trufflehog::find()?;
    let mut embedder = None;
    let (mut redacted, mut dropped, mut scanned_rows) = (0, 0, 0);
    for keys in queued.chunks(64) {
        let rows = swab::read_blocks(&ds, keys).await?;
        let chunks = dataset::chunks(&rows);
        scanned_rows += chunks.len();
        let plan = scrub::plan(&chunks, &scanner)?;
        if plan.remove.iter().any(|dirty| *dirty) {
            let vectors = plan.embed(model, &mut embedder)?;
            let replacements = dataset::build_batch(model, &plan.replacements, Some(&vectors))?;
            let fresh: HashSet<_> = plan.replacements.iter().map(|chunk| &chunk.id).collect();
            let dirty: Vec<_> = chunks.iter().zip(&plan.remove).filter_map(|(c, dirty)| dirty.then_some(c)).collect();
            let dirty_keys: HashSet<_> = dirty.iter().map(|c| held::BlockKey::new(&c.session_id, &c.turn_uuid, c.block_idx)).collect();
            // Do this before the data commit. The original rows still exist if the commit fails;
            // after it succeeds reimporting a transcript must not recreate an obsolete split.
            tombstone::record(dirty.iter().filter(|c| !fresh.contains(&c.id)).map(|c| c.id.clone()))?;
            let mut base = 0;
            let mut originals = Vec::new();
            for batch in rows {
                let selected = dataset::keep_rows(&batch, |i| plan.remove[base + i])?;
                base += batch.num_rows();
                if selected.num_rows() > 0 {
                    originals.push(selected);
                }
            }
            let dirty_keys: Vec<_> = dirty_keys.into_iter().collect();
            ds = swab::replace_blocks(&ds, &dirty_keys, &originals, replacements).await?.dataset;
            redacted += plan.redacted_blocks;
            dropped += plan.dropped_blocks;
        }
        held::remove(&uri, &keys.iter().cloned().collect())?;
    }
    Ok(format!("{uri}: swab checked {} withheld block(s) ({scanned_rows} local rows); redacted {redacted}, dropped {dropped}", queued.len()))
}
