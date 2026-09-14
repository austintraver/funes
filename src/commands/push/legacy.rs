//! Pre-staging push implementation, retained as a reference for equivalence tests.

use super::Skipped;
use crate::{chunk, scan};
use anyhow::{Context, Result};
use arrow_array::{BooleanArray, RecordBatch, StringArray, UInt64Array};
use arrow_select::filter::filter_record_batch;
use futures::TryStreamExt;
use lance::{dataset::ROW_ID, Dataset};
use std::collections::HashSet;

/// Don't make this a scan filter. On a memory that hasn't been pushed in a while, that filter lists
/// every pending id. Lance copies the whole filter into every fragment before it reads a row. RAM
/// use climbs with both the size of the backlog and the number of fragments.
pub(super) async fn rows_with_ids(local: &Dataset, ids: &HashSet<String>) -> Result<Vec<RecordBatch>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut scan = local.scan();
    scan.project(&["id"])?;
    scan.with_row_id();
    let mut stream = scan.try_into_stream().await?;
    let mut selected = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        let chunk_ids = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .context("selecting push rows: missing or non-string id column")?;
        let row_ids = batch
            .column_by_name(ROW_ID)
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            .context("selecting push rows: missing or non-u64 row ids")?;
        let matching: Vec<u64> = chunk_ids
            .iter()
            .zip(row_ids.values())
            .filter(|(id, _)| id.is_some_and(|id| ids.contains(id)))
            .map(|(_, &row_id)| row_id)
            .collect();
        if !matching.is_empty() {
            selected.push(local.take_rows(&matching, local.schema().clone()).await?);
        }
    }
    Ok(selected)
}

/// Scan the to-push `batches` and hold back every row of any *block* that holds a secret, returning
/// the clean batches and what was held back. Detection works at block granularity: a block's chunks
/// are reconstructed into their contiguous text (so a secret `split` cut across chunks is whole and
/// detectable), scanned in one pass, and the scanner says which block each finding came from — never
/// the secret's value, which fails on text stored with escaped or quoted bytes. If any
/// chunk of a block is dirty, the whole block is held back (its other chunks carry the rest of the
/// secret). Fail-closed on the scanner — a push must scan before it uploads.
pub(super) fn drop_secret_rows(batches: Vec<RecordBatch>) -> Result<(Vec<RecordBatch>, Skipped)> {
    let scanner = scan::Trufflehog::find()?;
    // Row order across batches matches `chunks_from_batches`, so a chunk's index is its global row.
    let chunks = chunk::chunks_from_batches(&batches);
    let blocks = chunk::reconstruct_blocks(&chunks);
    let texts: Vec<&str> = blocks.iter().map(|(_, text)| text.as_str()).collect();
    let found = scan::scan_blocks(&texts, &scanner)?;

    let mut dirty = vec![false; chunks.len()];
    let mut detectors: Vec<String> = Vec::new();
    for ((idxs, _), findings) in blocks.iter().zip(&found) {
        if findings.is_empty() {
            continue;
        }
        for &i in idxs {
            dirty[i] = true;
        }
        // One tally per (block, distinct detector), so the warning reads "PrivateKey×<blocks>".
        detectors.extend(scan::detectors(findings));
    }
    let dropped = dirty.iter().filter(|&&d| d).count();
    if dropped == 0 {
        return Ok((
            batches,
            Skipped {
                rows: 0,
                summary: String::new(),
            },
        ));
    }

    // Drop the dirty rows batch by batch, mapping each global row index back via a running offset.
    let mut clean = Vec::with_capacity(batches.len());
    let mut base = 0usize;
    for b in &batches {
        let mask: BooleanArray = (0..b.num_rows()).map(|i| !dirty[base + i]).collect();
        clean.push(filter_record_batch(b, &mask)?);
        base += b.num_rows();
    }
    let summary = scan::summary(detectors.iter().map(String::as_str));
    Ok((clean, Skipped { rows: dropped, summary }))
}
