//! Local end-to-end for the targeted swab: only a push-gate work item is repaired, redacted rows
//! get current embeddings, and removed source splits stay suppressed if their transcript is indexed
//! again. No Hub write is made.

mod support;

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use arrow_array::{Array, FixedSizeListArray, Float32Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_select::concat::concat_batches;
use funes::commands::{index, swab};
use funes::home;
use funes::inference::{self, EmbeddingModel};
use funes::memory::{dataset, held, tombstone};
use lance::Dataset;

const REMOTE: &str = "hf://datasets/example/swab-fixture";
const LARGE_ROWS: usize = 5_000;
const SMALL_FRAGMENTS: usize = 257;

fn receipt_path(uri: &str) -> PathBuf {
    let name: String = uri
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();
    home::pushed_dir().join(name)
}

fn write_receipt(uri: &str, ids: &[String]) -> PathBuf {
    let path = receipt_path(uri);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, format!("{}\n", ids.join("\n"))).unwrap();
    receipt_path(uri)
}

async fn append_rows(ds: &mut Dataset, batch: RecordBatch) {
    let schema = batch.schema();
    ds.append(RecordBatchIterator::new(vec![Ok(batch)], schema), None)
        .await
        .unwrap();
}

fn large_fragment_files(ds: &Dataset, table_dir: &Path) -> Vec<(String, PathBuf, Vec<u8>)> {
    let fragment = ds
        .get_fragments()
        .into_iter()
        .find(|fragment| fragment.metadata().physical_rows.unwrap_or(0) >= LARGE_ROWS)
        .expect("one retained fragment is larger than the small-fragment compaction limit");
    fragment
        .metadata()
        .files
        .iter()
        .map(|file| {
            let path = table_dir.join("data").join(&file.path);
            let contents = fs::read(&path).unwrap_or_else(|error| panic!("reading {}: {error}", path.display()));
            (file.path.clone(), path, contents)
        })
        .collect()
}

fn key_parts(path: &Path) -> (String, String, String) {
    let made = Command::new("ssh-keygen")
        .args(["-t", "rsa", "-b", "4096", "-N", "", "-q", "-f"])
        .arg(path)
        .status()
        .is_ok_and(|status| status.success());
    assert!(
        made,
        "ssh-keygen must be available to create a throwaway scanner fixture"
    );
    let key = fs::read_to_string(path).unwrap();
    let mut lines = key.lines();
    let header = lines.next().unwrap().to_owned();
    let body = lines
        .take_while(|line| !line.starts_with("-----END"))
        .collect::<String>();
    let footer = key.lines().last().unwrap().to_owned();
    fs::remove_file(path).unwrap();
    fs::remove_file(path.with_extension("pub")).unwrap();
    (header, body, footer)
}

fn stored_text_and_vectors(batches: &[RecordBatch]) -> (Vec<String>, Vec<Vec<f32>>) {
    let mut texts = Vec::new();
    let mut vectors = Vec::new();
    for batch in batches {
        let text = batch
            .column_by_name("text")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let vector = batch
            .column_by_name("vector")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            texts.push(text.value(i).to_owned());
            let value = vector.value(i);
            let row = value.as_any().downcast_ref::<Float32Array>().unwrap();
            vectors.push(row.values().to_vec());
        }
    }
    (texts, vectors)
}

#[tokio::test]
async fn swab_repairs_only_queued_splits_after_small_fragment_compaction() {
    if funes::scan::Trufflehog::find().is_err() {
        eprintln!("skip: trufflehog not found");
        return;
    }
    if !Path::new("/usr/bin/true").exists() {
        eprintln!("skip: /usr/bin/true unavailable");
        return;
    }
    if Command::new("ssh-keygen").arg("-V").output().is_err() {
        eprintln!("skip: ssh-keygen unavailable");
        return;
    }

    let funes_home = tempfile::tempdir().unwrap();
    let source = tempfile::tempdir().unwrap();
    std::env::set_var("FUNES_HOME", funes_home.path());

    // Seed already-published clean history and make one old, large fragment plus enough newer
    // small fragments to exercise compaction. The large fragment stands for clean historical data
    // that a targeted swab must leave byte-for-byte intact.
    support::memory_of(
        EmbeddingModel::BgeSmallEn,
        &["receipt-covered clean historical marker"],
        &home::memory_dir(),
    )
    .await;
    let mut ds = support::open_memory(funes_home.path()).await;
    let clean_rows = dataset::scan_rows(&ds, &[], Some("session_id = 's-0'"), Some(1))
        .await
        .unwrap();
    let clean_schema = dataset::schema_of(&ds);
    let clean = dataset::restamped(&clean_schema, clean_rows).unwrap().remove(0);
    let copies = vec![clean.clone(); LARGE_ROWS];
    let large = concat_batches(&clean_schema, &copies).unwrap();
    append_rows(&mut ds, large).await;
    for _ in 0..SMALL_FRAGMENTS {
        append_rows(&mut ds, clean.clone()).await;
    }

    support::write_session(source.path(), "compactor", &["small fragments to compact"]);
    index::run_index(source.path(), false, None).await.unwrap();

    let historical = dataset::scan_rows(&ds, &["id", "text", "vector"], Some("session_id = 's-0'"), Some(1))
        .await
        .unwrap();
    let historical_ids = historical
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|id| id.unwrap().to_owned())
        })
        .collect::<Vec<_>>();
    let receipt = write_receipt(REMOTE, &historical_ids);
    let receipt_before = fs::read(&receipt).unwrap();
    let historical_before = stored_text_and_vectors(&historical);

    // Put an RSA private key across a chunk seam. The header newline is before the splitter's
    // midpoint, and the base64 payload is one long line, so the first chunk ends inside the key.
    let key_path = funes_home.path().join("throwaway_key");
    let (header, body, footer) = key_parts(&key_path);
    let key_text = format!("{}\n{header}\n{body}\n{footer}", "clean words ".repeat(40));
    let body_start = format!("{}\n{header}\n", "clean words ".repeat(40)).chars().count();
    assert!(body_start < 1_200 && body_start + body.chars().count() > 1_200);

    support::write_session(source.path(), "late-secret", &[key_text]);
    std::env::set_var("FUNES_TRUFFLEHOG", "/usr/bin/true");
    index::run_index(source.path(), false, None).await.unwrap();

    let table_dir = funes_home.path().join("memory/chunks.lance");
    let before_swab = dataset::open(&dataset::table_uri(home::memory_dir()), Default::default())
        .await
        .unwrap();
    let key_filter = "session_id = 'late-secret'";
    let dirty_rows = dataset::scan_rows(&before_swab, &[], Some(key_filter), None)
        .await
        .unwrap();
    let dirty_chunks = dataset::chunks(&dirty_rows);
    assert!(dirty_chunks.len() >= 3, "the long key crosses stored chunk splits");
    assert!(dirty_chunks[0].text.chars().count() > body_start);
    let dirty_ids: HashSet<_> = dirty_chunks.iter().map(|chunk| chunk.id.clone()).collect();
    let block = held::BlockKey::new("late-secret", "t0", 0);

    // The real push module test verifies that drop_secret_rows emits exactly this block key. This
    // local integration test starts where that gate leaves its durable work item, without a Hub.
    held::add(REMOTE, [block.clone()]).unwrap();
    let large_files = large_fragment_files(&before_swab, &table_dir);

    std::env::remove_var("FUNES_TRUFFLEHOG");
    let report = swab::run(REMOTE).await.unwrap();
    assert!(report.contains("redacted 1"), "swab report: {report}");
    assert!(
        held::load(REMOTE).unwrap().is_empty(),
        "successfully repaired work is retired"
    );

    let repaired = dataset::open(&dataset::table_uri(home::memory_dir()), Default::default())
        .await
        .unwrap();
    let large_names_before: HashSet<_> = large_files.iter().map(|(name, _, _)| name.clone()).collect();
    let large_names_after: HashSet<_> = repaired
        .get_fragments()
        .into_iter()
        .flat_map(|fragment| {
            fragment
                .metadata()
                .files
                .iter()
                .map(|file| file.path.clone())
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(
        large_names_before.is_subset(&large_names_after),
        "the current dataset still references every large historical data file"
    );

    let repaired_version = repaired.version().version;
    // An interrupted swab can commit its repair before retiring the worklist. Replaying that
    // snapshot must leave the repaired dataset version alone and finish draining the queue.
    held::add(REMOTE, [block]).unwrap();
    let replay = swab::run(REMOTE).await.unwrap();
    assert!(replay.contains("redacted 0, dropped 0"), "{replay}");
    let empty_report = swab::run(REMOTE).await.unwrap();
    assert!(empty_report.contains("no withheld blocks queued"), "{empty_report}");
    let after_empty_swab = dataset::open(&dataset::table_uri(home::memory_dir()), Default::default())
        .await
        .unwrap();
    assert_eq!(
        after_empty_swab.version().version,
        repaired_version,
        "replaying a repaired block and draining an empty queue create no dataset version"
    );
    let repaired_key_rows = dataset::scan_rows(&repaired, &[], Some(key_filter), None)
        .await
        .unwrap();
    let (repaired_text, repaired_vectors) = stored_text_and_vectors(&repaired_key_rows);
    assert!(repaired_text.iter().any(|text| text.contains("[REDACTED:PrivateKey]")));
    assert!(repaired_text.iter().all(|text| !text.contains(&body[..80])));
    assert!(repaired_vectors
        .iter()
        .all(|vector| vector.len() == dataset::DIM as usize));

    // Confirm the replacements carry current embeddings for the redacted text.
    let mut embedder = inference::embedder(EmbeddingModel::BgeSmallEn).unwrap();
    for (text, vector) in repaired_text.iter().zip(&repaired_vectors) {
        let expected = embedder.embed(&[text]).unwrap().remove(0);
        let cosine: f32 = vector.iter().zip(&expected).map(|(a, b)| a * b).sum();
        assert!(cosine > 0.9999, "redacted row has stale embedding ({cosine}): {text}");
    }

    // The egress scanner now sees no finding in the repaired block, and a subsequent index of the
    // original source cannot restore removed split ids or the original private-key payload.
    let scanner = funes::scan::Trufflehog::find().unwrap();
    let text_refs = repaired_text.iter().map(String::as_str).collect::<Vec<_>>();
    let findings = funes::scan::scan_blocks(&text_refs, &scanner).unwrap();
    assert!(
        findings.iter().all(Vec::is_empty),
        "the repaired rows pass the secret scanner"
    );
    let suppressed = tombstone::load().unwrap();
    assert!(
        !suppressed.is_disjoint(&dirty_ids),
        "removed source split ids are tombstoned"
    );
    std::env::set_var("FUNES_TRUFFLEHOG", "/usr/bin/true");
    index::run_index(source.path(), false, None).await.unwrap();
    std::env::remove_var("FUNES_TRUFFLEHOG");
    let reindexed = dataset::open(&dataset::table_uri(home::memory_dir()), Default::default())
        .await
        .unwrap();
    let after_reindex = dataset::scan_rows(&reindexed, &[], Some(key_filter), None)
        .await
        .unwrap();
    let (reindexed_text, _) = stored_text_and_vectors(&after_reindex);
    assert!(reindexed_text.iter().all(|text| !text.contains(&body[..80])));
    assert!(reindexed_text.iter().any(|text| text.contains("[REDACTED:PrivateKey]")));

    // Previously published clean content retains every text/vector pair, and a >4,096-row old
    // fragment remains present with its original data-file bytes after the tiny fragments compact.
    let historical_after = dataset::scan_rows(
        &reindexed,
        &["id", "text", "vector"],
        Some("session_id = 's-0'"),
        Some(1),
    )
    .await
    .unwrap();
    assert_eq!(stored_text_and_vectors(&historical_after), historical_before);
    for (_, path, bytes) in large_files {
        assert_eq!(
            fs::read(&path).unwrap(),
            bytes,
            "swab rewrote old clean data file {}",
            path.display()
        );
    }

    // Ensure the canonical local memory remains readable and the receipt still covers the old rows.
    assert_eq!(
        fs::read(receipt).unwrap(),
        receipt_before,
        "swab leaves clean push coverage unchanged"
    );
}
