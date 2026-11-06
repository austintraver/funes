//! Targeted Lance replacement for blocks repaired by the daily secret swab.
//!
//! A replacement uses the deterministic chunk `id` as its key. Lance's `RewriteRows` merge writes
//! only matched replacement rows and inserts new splits; a block-scoped `DeleteIf` removes old
//! splits that no longer exist. The three actions commit as one dataset version.

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use arrow_array::{Array, FixedSizeListArray, RecordBatch, StringArray, UInt64Array};
use arrow_schema::Schema;
use futures::TryStreamExt;
use lance::dataset::{
    CommitBuilder, Dataset, MergeInsertBuilder, MergeInsertWriteMode, WhenMatched, WhenNotMatched,
    WhenNotMatchedBySource,
};

use super::dataset::{ints, strs};
use super::held::BlockKey;

const MAX_KEYS_PER_SCAN: usize = 64;
const ROW_ADDR: &str = "_rowaddr";

/// Read all current rows for `keys`, preserving physical row addresses for the read snapshot.
///
/// A caller can pass larger worklists; this issues separate scans with at most 64 block keys each.
/// Rows are returned in Arrow batches and retain the dataset schema plus `_rowaddr`.
pub async fn read_blocks(ds: &Dataset, keys: &[BlockKey]) -> Result<Vec<RecordBatch>> {
    let columns = vec![
        "session_id".to_string(),
        "turn_uuid".to_string(),
        "block_idx".to_string(),
    ];
    let mut batches = Vec::new();
    for key_group in keys.chunks(MAX_KEYS_PER_SCAN) {
        if key_group.is_empty() {
            continue;
        }
        let filter = block_filter(key_group);
        let mut scan = ds.scan();
        scan.project(&columns)?;
        scan.filter(&filter)?;
        scan.with_row_id();
        let mut stream = scan
            .try_into_stream()
            .await
            .context("scanning dirty memory block addresses")?;
        let mut row_ids = Vec::new();
        while let Some(batch) = stream
            .try_next()
            .await
            .context("reading dirty memory block addresses")?
        {
            let ids = batch
                .column_by_name("_rowid")
                .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| anyhow!("block address scan lacks a uint64 _rowid column"))?;
            row_ids.extend((0..ids.len()).map(|row| ids.value(row)));
        }
        if !row_ids.is_empty() {
            let mut take = Arc::new(ds.clone()).take_builder(&row_ids, Arc::new(ds.schema().clone()))?;
            take = take.with_row_address(true);
            batches.push(take.execute().await.context("loading selected dirty memory rows")?);
        }
    }
    Ok(batches)
}

/// Atomically replace all rows in `keys` with fully embedded `replacements`.
///
/// `originals` must be the complete result of [`read_blocks`] for these keys, from the dataset
/// version passed here. The original and replacement batches are checked against `keys` before a
/// write. Replacements use the full target schema so `RewriteRows` preserves every non-text field.
/// An empty full-schema replacement batch removes every row in the selected blocks.
pub async fn replace_blocks(
    ds: &Dataset,
    keys: &[BlockKey],
    originals: &[RecordBatch],
    replacements: RecordBatch,
) -> Result<Dataset> {
    if keys.is_empty() {
        bail!("a block replacement needs at least one block key");
    }
    validate_unique_keys(keys)?;
    let replacements = stamp_replacements_with_current_schema(ds, replacements)?;

    let key_set: HashSet<BlockKey> = keys.iter().cloned().collect();
    let old_ids = original_ids(originals, &key_set)?;
    let new_ids = replacement_ids(&replacements, &key_set)?;
    let removed_ids = old_ids.difference(&new_ids).cloned().collect::<Vec<_>>();
    let expected_updates = old_ids.intersection(&new_ids).count() as u64;
    let expected_inserts = new_ids.difference(&old_ids).count() as u64;
    let expected_deletes = removed_ids.len() as u64;

    let delete_filter = block_filter(keys);
    let delete_behavior =
        WhenNotMatchedBySource::delete_if(ds, &delete_filter).context("building the dirty-block deletion filter")?;
    let pending = MergeInsertBuilder::try_new(Arc::new(ds.clone()), vec!["id".to_string()])?
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .when_not_matched_by_source(delete_behavior)
        .write_mode(MergeInsertWriteMode::RewriteRows)
        .try_build()?
        .execute_uncommitted_batches(vec![replacements])
        .await
        .context("planning repaired memory blocks")?;

    if pending.stats.num_updated_rows != expected_updates
        || pending.stats.num_inserted_rows != expected_inserts
        || pending.stats.num_deleted_rows != expected_deletes
    {
        bail!(
            "Lance replaced an unexpected number of rows: expected {} updates, {} inserts, and {} deletions; got {} updates, {} inserts, and {} deletions",
            expected_updates,
            expected_inserts,
            expected_deletes,
            pending.stats.num_updated_rows,
            pending.stats.num_inserted_rows,
            pending.stats.num_deleted_rows
        );
    }

    let mut commit = CommitBuilder::new(Arc::new(ds.clone()));
    if let Some(affected_rows) = pending.affected_rows {
        commit = commit.with_affected_rows(affected_rows);
    }
    let updated = commit
        .execute(pending.transaction)
        .await
        .context("committing repaired memory blocks")?;

    Ok(updated)
}

fn validate_unique_keys(keys: &[BlockKey]) -> Result<()> {
    let set: HashSet<_> = keys.iter().collect();
    if set.len() != keys.len() {
        bail!("dirty block keys contain duplicates");
    }
    Ok(())
}

fn block_filter(keys: &[BlockKey]) -> String {
    keys.iter()
        .map(|key| {
            format!(
                "(session_id = {} AND turn_uuid = {} AND block_idx = {})",
                quote(&key.session_id),
                quote(&key.turn_uuid),
                key.block_idx
            )
        })
        .collect::<Vec<_>>()
        .join(" OR ")
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn key_at(batch: &RecordBatch, row: usize) -> Result<BlockKey> {
    let session = batch
        .column_by_name("session_id")
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| anyhow!("block batch lacks a string session_id column"))?;
    let turn = batch
        .column_by_name("turn_uuid")
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| anyhow!("block batch lacks a string turn_uuid column"))?;
    let block_idx = ints(batch, "block_idx");
    Ok(BlockKey::new(session.value(row), turn.value(row), block_idx.get(row)))
}

fn original_ids(originals: &[RecordBatch], keys: &HashSet<BlockKey>) -> Result<HashSet<String>> {
    let mut ids = HashSet::new();
    for batch in originals {
        if batch.column_by_name(ROW_ADDR).is_none() {
            bail!("original block rows must include their snapshot's _rowaddr values");
        }
        let batch_ids = strs(batch, "id");
        for row in 0..batch.num_rows() {
            let key = key_at(batch, row)?;
            if !keys.contains(&key) {
                bail!("original row belongs to a block outside the replacement scope");
            }
            if !ids.insert(batch_ids.get(row).to_string()) {
                bail!("original block rows contain a duplicate chunk id");
            }
        }
    }
    Ok(ids)
}

fn replacement_ids(replacements: &RecordBatch, keys: &HashSet<BlockKey>) -> Result<HashSet<String>> {
    let ids = strs(replacements, "id");
    let vectors = replacements
        .column_by_name("vector")
        .and_then(|column| column.as_any().downcast_ref::<FixedSizeListArray>())
        .ok_or_else(|| anyhow!("replacement rows need a fixed-size vector column"))?;
    let mut found = HashSet::new();
    for row in 0..replacements.num_rows() {
        let key = key_at(replacements, row)?;
        if !keys.contains(&key) {
            bail!("replacement row belongs to a block outside the replacement scope");
        }
        if !found.insert(ids.get(row).to_string()) {
            bail!("replacement rows contain a duplicate chunk id");
        }
        if vectors.is_null(row) || vectors.value(row).null_count() != 0 {
            bail!("replacement chunk {} has no complete embedding", ids.get(row));
        }
    }
    Ok(found)
}

fn stamp_replacements_with_current_schema(ds: &Dataset, replacements: RecordBatch) -> Result<RecordBatch> {
    let target = Arc::new(Schema::from(ds.schema()));
    if replacements.num_columns() != target.fields().len() {
        bail!("replacement rows must use the complete current memory schema");
    }
    let source_schema = replacements.schema();
    let mut columns = Vec::with_capacity(target.fields().len());
    for field in target.fields() {
        let (index, source_field) = source_schema
            .column_with_name(field.name())
            .ok_or_else(|| anyhow!("replacement rows lack current memory field {}", field.name()))?;
        if source_field.data_type() != field.data_type() {
            bail!(
                "replacement field {} has type {:?}; memory schema requires {:?}",
                field.name(),
                source_field.data_type(),
                field.data_type()
            );
        }
        columns.push(replacements.column(index).clone());
    }
    RecordBatch::try_new(target, columns).context("stamping replacement rows with the current memory schema")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::Chunk;
    use crate::inference::EmbeddingModel;
    use crate::memory::dataset;
    use arrow_array::{Float32Array, Int64Array, RecordBatchIterator};
    use arrow_schema::{DataType, Field};
    use tempfile::tempdir;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("text", DataType::Utf8, false),
            Field::new("session_id", DataType::Utf8, false),
            Field::new("turn_uuid", DataType::Utf8, false),
            Field::new("block_idx", DataType::Int64, false),
            Field::new("split_idx", DataType::Int64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                true,
            ),
        ]))
    }

    type TestRow<'a> = (&'a str, &'a str, &'a str, &'a str, i64, i64, [f32; 2]);

    fn batch(rows: &[TestRow<'_>]) -> RecordBatch {
        let vectors = rows.iter().flat_map(|row| row.6).collect::<Vec<_>>();
        let vector_values: arrow_array::ArrayRef = Arc::new(Float32Array::from(vectors));
        let item_field = Arc::new(Field::new("item", DataType::Float32, true));
        let vector_array = FixedSizeListArray::try_new(item_field, 2, vector_values, None).unwrap();
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.0))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
                Arc::new(Int64Array::from(rows.iter().map(|row| row.4).collect::<Vec<_>>())),
                Arc::new(Int64Array::from(rows.iter().map(|row| row.5).collect::<Vec<_>>())),
                Arc::new(vector_array),
            ],
        )
        .unwrap()
    }

    type BatchReader = RecordBatchIterator<std::iter::Once<std::result::Result<RecordBatch, arrow_schema::ArrowError>>>;

    fn reader(batch: RecordBatch) -> BatchReader {
        let schema = batch.schema();
        RecordBatchIterator::new(std::iter::once(Ok(batch)), schema)
    }

    #[tokio::test]
    async fn reads_only_requested_blocks_and_includes_addresses() {
        let dir = tempdir().unwrap();
        let uri = dir.path().join("chunks.lance").to_string_lossy().into_owned();
        let initial = batch(&[
            ("a", "first", "session", "turn", 0, 0, [1.0, 0.0]),
            ("b", "second", "session", "turn", 0, 1, [0.0, 1.0]),
            ("c", "unrelated", "other", "turn", 1, 0, [1.0, 1.0]),
        ]);
        let ds = Dataset::write(reader(initial), &uri, None).await.unwrap();
        let keys = [BlockKey::new("session", "turn", 0)];

        let mut address_scan = ds.scan();
        address_scan.project(&["session_id", "turn_uuid", "block_idx"]).unwrap();
        address_scan.filter(&block_filter(&keys)).unwrap();
        address_scan.with_row_id();
        let plan = address_scan.explain_plan(false).await.unwrap();
        let target_read = plan
            .lines()
            .find(|line| line.trim_start().starts_with("LanceRead:"))
            .unwrap();
        assert!(
            target_read.contains("projection=[session_id, turn_uuid, block_idx]"),
            "{target_read}"
        );
        assert!(
            !target_read.contains("text") && !target_read.contains("vector"),
            "{target_read}"
        );

        let blocks = read_blocks(&ds, &keys).await.unwrap();
        assert_eq!(blocks.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert!(blocks.iter().all(|batch| batch.column_by_name(ROW_ADDR).is_some()));
        assert_eq!(strs(&blocks[0], "id").get(0), "a");
    }

    #[tokio::test]
    async fn replaces_changed_split_count_atomically_and_preserves_other_rows() {
        let dir = tempdir().unwrap();
        let uri = dir.path().join("chunks.lance").to_string_lossy().into_owned();
        let initial = batch(&[
            ("dirty-0", "secret", "session", "turn", 0, 0, [1.0, 0.0]),
            ("dirty-1", "secret continuation", "session", "turn", 0, 1, [0.0, 1.0]),
            ("clean-0", "keep this", "other", "turn", 2, 0, [1.0, 1.0]),
        ]);
        let ds = Dataset::write(reader(initial), &uri, None).await.unwrap();
        let keys = [BlockKey::new("session", "turn", 0)];
        let originals = read_blocks(&ds, &keys).await.unwrap();
        let replacements = batch(&[
            ("dirty-0", "[REDACTED]", "session", "turn", 0, 0, [0.5, 0.5]),
            (
                "dirty-2",
                "new replacement split",
                "session",
                "turn",
                0,
                1,
                [0.25, 0.75],
            ),
        ]);

        let delete_behavior = WhenNotMatchedBySource::delete_if(&ds, &block_filter(&keys)).unwrap();
        let job = MergeInsertBuilder::try_new(Arc::new(ds.clone()), vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .when_not_matched_by_source(delete_behavior)
            .write_mode(MergeInsertWriteMode::RewriteRows)
            .try_build()
            .unwrap();
        let plan = job
            .explain_plan(Some(replacements.schema().as_ref()), false)
            .await
            .unwrap();
        let target_read = plan
            .lines()
            .find(|line| line.trim_start().starts_with("LanceRead:"))
            .unwrap();
        assert!(
            target_read.contains("projection=[id, session_id, turn_uuid, block_idx]"),
            "{target_read}"
        );
        assert!(
            !target_read.contains("text") && !target_read.contains("vector"),
            "{target_read}"
        );

        let result = replace_blocks(&ds, &keys, &originals, replacements).await.unwrap();
        assert_eq!(result.count_rows(None).await.unwrap(), 3);
        assert_eq!(result.count_rows(Some("text = 'secret'".into())).await.unwrap(), 0);
        assert_eq!(result.count_rows(Some("id = 'clean-0'".into())).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn empty_replacement_deletes_only_the_selected_block() {
        let dir = tempdir().unwrap();
        let uri = dir.path().join("chunks.lance").to_string_lossy().into_owned();
        let initial = batch(&[
            ("dirty-0", "unrecoverable", "session", "turn", 0, 0, [1.0, 0.0]),
            ("clean-0", "keep this", "other", "turn", 1, 0, [0.0, 1.0]),
        ]);
        let ds = Dataset::write(reader(initial), &uri, None).await.unwrap();
        let keys = [BlockKey::new("session", "turn", 0)];
        let originals = read_blocks(&ds, &keys).await.unwrap();
        let replacements = batch(&[]);

        let result = replace_blocks(&ds, &keys, &originals, replacements).await.unwrap();
        assert_eq!(result.count_rows(None).await.unwrap(), 1);
        assert_eq!(result.count_rows(Some("id = 'clean-0'".into())).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn empty_embedded_funes_batch_can_delete_the_last_memory_row() {
        let dir = tempdir().unwrap();
        let uri = dir.path().join("chunks.lance").to_string_lossy().into_owned();
        let model = EmbeddingModel::BgeSmallEn;
        let chunk = Chunk {
            id: "0123456789abcdef".to_string(),
            text: "unrecoverable secret-bearing block".to_string(),
            session_id: "session".to_string(),
            workdir: String::new(),
            turn_uuid: "turn".to_string(),
            parent_uuid: None,
            seq: 0,
            ts: "2026-11-05T00:00:00Z".to_string(),
            role: "assistant".to_string(),
            block_type: "text".to_string(),
            tool_name: None,
            source_path: "fixture.jsonl".to_string(),
            block_idx: 0,
            split_idx: 0,
            harness: "test".to_string(),
            repo: String::new(),
        };
        let vector = vec![0.0; dataset::DIM as usize];
        let initial = dataset::build_batch(model, &[chunk], Some(&[vector])).unwrap();
        let ds = Dataset::write(reader(initial), &uri, None).await.unwrap();
        let keys = [BlockKey::new("session", "turn", 0)];
        let originals = read_blocks(&ds, &keys).await.unwrap();

        // This is the same full-schema zero-row batch produced by swab when every selected block
        // must be dropped and no replacement chunk remains to embed.
        let replacements = dataset::build_batch(model, &[], Some(&[])).unwrap();
        assert_eq!(replacements.num_rows(), 0);
        assert_eq!(
            replacements.schema().field_with_name("vector").unwrap().data_type(),
            &DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dataset::DIM,)
        );
        let result = replace_blocks(&ds, &keys, &originals, replacements).await.unwrap();
        assert_eq!(result.count_rows(None).await.unwrap(), 0);
    }
}
