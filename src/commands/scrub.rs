//! The `scrub` command: redact secrets from the existing local memory, in place. Works on the stored
//! rows themselves — so it cleans sessions whose source transcripts are already gone, which
//! re-indexing cannot. Operates only on the local memory; it does not touch a published remote.

use crate::home;
use crate::inference::{self, embed_batched, Embedder};
use crate::memory::dataset::{self, build_batch};
use crate::memory::Memory;
use crate::memory::{lock, receipt};
use crate::{chunk, scan, ui};
use anyhow::{bail, Context, Result};
use lance::Dataset;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::io::Write as _;
use std::time::Instant;

/// Scrub every local block, or, with `pending`, only those not fully covered by the remote's push receipt.
/// Reconstruct the selected blocks, scan them in one pass, and for any block holding a secret either
/// redact the matching values in place (re-chunked, re-embedded) or — when a value can't be matched
/// (e.g. a key stored with escaped `\n`), or the redaction doesn't scan clean — drop the block
/// whole. Fail-closed on the scanner: scrubbing is the whole point.
pub async fn run(pending: Option<&str>) -> Result<()> {
    let remote = match pending.map(Memory::parse) {
        Some(Memory::Remote { uri }) => Some(uri),
        Some(_) => bail!("--pending requires a remote memory"),
        None => None,
    };
    let _lock = lock::MemoryLock::acquire()?;
    let Some(ds) = Memory::local().dataset().await? else {
        println!("no local memory to scrub");
        return Ok(());
    };
    let model = dataset::embedding_model(&ds)?;
    let batches = if let Some(remote) = remote.as_deref() {
        let Some(rows) = pending_block_rows(&ds, remote).await? else {
            println!("no pending blocks to scrub");
            return Ok(());
        };
        vec![rows]
    } else {
        eprintln!("loading the local memory…");
        dataset::scan_rows(&ds, &[], None, None).await?
    };
    let scanner = scan::Trufflehog::find()?;
    let chunks = dataset::chunks(&batches);
    let total = chunks.len();
    if total == 0 {
        sweep(&ds).await?;
        println!("memory is empty");
        return Ok(());
    }

    // Reconstruct each block's contiguous text (so a secret split() cut across chunks is whole) and
    // scan them all in one pass.
    let blocks = chunk::reconstruct_blocks(&chunks);
    let texts: Vec<&str> = blocks.iter().map(|(_, text)| text.as_str()).collect();
    eprintln!("scanning {} block(s) for secrets…", texts.len());
    let found = scan::scan_blocks(&texts, &scanner)?;

    // From that single scan, decide each block: excise the secrets whose value matches and re-chunk
    // the result; if any can't be matched, the block can't be safely redacted, so drop it whole.
    // `remove[i]` marks an original row to drop (its block had a secret); `replacements` are the
    // re-chunked redacted blocks to re-embed.
    let mut remove = vec![false; chunks.len()];
    let mut replacements: Vec<chunk::Chunk> = Vec::new();
    let mut redacted_detectors: Vec<String> = Vec::new();
    let mut dropped_detectors: Vec<String> = Vec::new();
    let (mut redacted_blocks, mut dropped_blocks, mut dropped_rows) = (0usize, 0usize, 0usize);
    for ((idxs, text), findings) in blocks.iter().zip(&found) {
        if findings.is_empty() {
            continue;
        }
        for &i in idxs {
            remove[i] = true;
        }
        let r = scan::excise(text, findings);
        // Excising one match can expose another: removing it from a long base64 run re-aligns the
        // window the next is found in. So a redaction counts only if the result scans clean.
        let clean = r.fully_redacted && scan::scan_blocks(&[r.text.as_str()], &scanner)?[0].is_empty();
        if clean {
            replacements.extend(chunk::resplit(&chunks[idxs[0]], &r.text));
            redacted_detectors.extend(r.removed_detectors);
            redacted_blocks += 1;
        } else {
            dropped_detectors.extend(scan::detectors(findings));
            dropped_blocks += 1;
            dropped_rows += idxs.len();
        }
    }
    if !remove.iter().any(|&r| r) {
        if pending.is_some() {
            println!("pending blocks already scan clean ({total} chunks)");
        } else {
            sweep(&ds).await?;
            println!("memory is already clean ({total} chunks)");
        }
        return Ok(());
    }

    // Re-embed only the redacted replacement rows — their stored vectors were computed from the
    // secret-bearing text. Clean rows keep their existing vectors (carried below).
    let replacement_batch = if replacements.is_empty() {
        None
    } else {
        let mut embedder: Box<dyn Embedder> = inference::embedder(model)?;
        let rtexts: Vec<&str> = replacements.iter().map(|c| c.text.as_str()).collect();
        let n = rtexts.len();
        eprintln!("re-embedding {n} redacted chunk(s)…");
        let t0 = Instant::now();
        let vectors = embed_batched(embedder.as_mut(), &rtexts, |done| {
            let secs = t0.elapsed().as_secs_f64().max(0.001);
            eprint!("\r    embedded {done}/{n}  ({:.0}/s)   ", done as f64 / secs);
            let _ = std::io::stderr().flush();
        })?;
        eprintln!(
            "\r    embedded {n} chunk(s) in {:.1}s          ",
            t0.elapsed().as_secs_f64()
        );
        Some(build_batch(model, &replacements, Some(&vectors))?)
    };

    if pending.is_some() {
        let dirty_ids = chunks
            .iter()
            .zip(&remove)
            .filter_map(|(c, &dirty)| dirty.then_some(c.id.as_str()))
            .collect::<Vec<_>>();
        dataset::replace_scrubbed(&ds, &dirty_ids, replacement_batch).await?;
    } else {
        rewrite(&batches, &remove, replacement_batch, model).await?;
    }

    let mut msg = format!(
        "scrubbed {total} rows: redacted {} secret(s) in {redacted_blocks} block(s)",
        redacted_detectors.len()
    );
    if dropped_blocks > 0 {
        let _ = write!(
            msg,
            "; dropped {dropped_rows} row(s) in {dropped_blocks} block(s) that couldn't be safely redacted ({})",
            scan::summary(dropped_detectors.iter().map(String::as_str))
        );
    }
    println!("{msg}");
    Ok(())
}

/// A receipt covers a block only when it contains every split. Select by identifiers first so
/// fully covered blocks never have their text or vectors loaded for scanning.
async fn pending_block_rows(ds: &Dataset, remote: &str) -> Result<Option<dataset::RecordBatch>> {
    let pushed = receipt::load(remote).with_context(|| format!("no readable push receipt for {remote}"))?;
    let keys = dataset::scan_rows_with_ids(ds, &["id", "session_id", "turn_uuid", "block_idx"], None).await?;
    let mut blocks = HashMap::new();
    for batch in &keys {
        let ids = dataset::strs(batch, "id");
        let sessions = dataset::strs(batch, "session_id");
        let turns = dataset::strs(batch, "turn_uuid");
        let indices = dataset::ints(batch, "block_idx");
        let addresses = dataset::row_ids(batch)?;
        for i in 0..batch.num_rows() {
            let key = (sessions.get(i), turns.get(i), indices.get(i));
            let (pending, rows) = blocks.entry(key).or_insert_with(|| (false, Vec::new()));
            *pending |= !pushed.contains(ids.get(i));
            rows.push(addresses.get(i));
        }
    }
    let mut rows: Vec<_> = blocks
        .into_values()
        .filter(|(pending, _)| *pending)
        .flat_map(|(_, rows)| rows)
        .collect();
    if rows.is_empty() {
        return Ok(None);
    }
    rows.sort_unstable();
    Ok(Some(dataset::take_rows_without_vectors(ds, &rows).await?))
}

/// Overwrite the local memory with the retained rows and replacements, rebuild its indexes,
/// and delete old versions.
async fn rewrite(
    batches: &[dataset::RecordBatch],
    remove: &[bool],
    replacement_batch: Option<dataset::RecordBatch>,
    model: inference::EmbeddingModel,
) -> Result<()> {
    // Rewrite the memory in a single Overwrite commit: every clean row (with its existing vector) plus
    // the re-chunked redacted blocks. One commit is deliberate — a delete-then-append is two commits,
    // and an interrupt between them would drop the secret rows without writing their replacements,
    // which for a source-gone session is permanent loss. Append-first isn't a safe alternative either:
    // `cid` hashes only coordinates, so a same-piece-count re-split reuses the old ids and a later
    // delete couldn't tell the fresh rows from the stale ones. (Cost: scrub rewrites the whole table,
    // fine for a rare remediation.)
    let mut out = Vec::new();
    let mut base = 0usize;
    for b in batches {
        let kept = dataset::keep_rows(b, |i| !remove[base + i])?;
        if kept.num_rows() > 0 {
            out.push(kept);
        }
        base += b.num_rows();
    }
    out.extend(replacement_batch);
    eprintln!("rewriting the memory…");
    let mut ds = dataset::overwrite(&dataset::table_uri(home::memory_dir()), out, model).await?;
    dataset::build_indexes(&mut ds, ui::index_progress).await?;
    sweep(&ds).await
}

/// Leave no pre-scrub row readable on disk.
async fn sweep(ds: &Dataset) -> Result<()> {
    eprintln!("deleting old versions…");
    dataset::delete_old_versions(ds).await
}
