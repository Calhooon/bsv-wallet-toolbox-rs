//! The A8 cycle's differential target (#556, A8-3): the `a8-beef-bytes`
//! path and the oracle path side by side in one binary (the test's thread
//! chooses the door's path, `A8_PATH`), fed the same bytes, and any
//! difference in a value, an error or a refusal is a failure. The scenes:
//! every input of the BEEF door corpus (`fuzz/corpus/beef_door`) and the
//! census's 1, 4 and 10 MB BEEFs at the verification seams
//! (`a8_differential`), and the census's BEEFs and refusals through the
//! internalize door, its answer and every stored row compared. The create
//! side is `create_action/tests/beef_read_once.rs`; the stored fixtures'
//! replay (`tests/fixtures`) is the third, run with the feature on and off.

use super::a8_differential::{differential, Headers, CENSUS_HEIGHT};
use super::internalize_action_internal;
use crate::storage::sqlx::beef_verification::A8_PATH;
use crate::storage::sqlx::{StorageSqlx, StorageSqlxOptions};
use crate::storage::traits::WalletStorageWriter;
use bsv_rs::primitives::{hash160, PrivateKey};
use bsv_rs::script::template::{SignOutputs, SigningContext};
use bsv_rs::script::templates::P2PKH;
use bsv_rs::script::{LockingScript, Script, UnlockingScript};
use bsv_rs::transaction::{Beef, MerklePath, Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{BasketInsertion, InternalizeActionArgs, InternalizeOutput};
use sqlx::{Column, Row, TypeInfo, ValueRef};
use std::collections::HashSet;
use std::fmt::Write;

const CENSUS_COIN: u64 = 1_000_000;

/// Chooses the path of this thread's door calls until dropped.
pub(crate) struct Path;

impl Path {
    pub(crate) fn choose(read_once: bool) -> Self {
        A8_PATH.with(|path| path.set(Some(read_once)));
        Self
    }
}

impl Drop for Path {
    fn drop(&mut self) {
        A8_PATH.with(|path| path.set(None));
    }
}

/// The census's scene (`examples/a8_census.rs`, `big_beef`): a proven parent
/// paying `CENSUS_COIN` to a P2PKH key, and a child spending it with one
/// ALL|FORKID P2PKH input, output 0 a 10,000-satoshi P2PKH coin and output 1
/// an `OP_FALSE OP_RETURN` of `bytes` data. Returns the child's `AtomicBEEF`,
/// its txid and the parent's.
pub(crate) fn census_beef(bytes: usize) -> (Vec<u8>, String, String) {
    census_beef_signed_by(bytes, 7)
}

/// `census_beef` with the child signed by key `signer` (the coin is key 7's).
pub(crate) fn census_beef_signed_by(bytes: usize, signer: u8) -> (Vec<u8>, String, String) {
    let key = PrivateKey::from_hex(&format!("{:064x}", 7)).expect("fixture key");
    let hash = hash160(&key.public_key().to_compressed());
    let lock = [&[0x76, 0xa9, 0x14][..], &hash[..], &[0x88, 0xac][..]].concat();
    let mut parent = Transaction::new();
    let mut funding = TransactionInput::new("11".repeat(32), 0);
    funding.unlocking_script = Some(UnlockingScript::from_binary(&[0x51]).expect("push"));
    parent.inputs.push(funding);
    parent.outputs.push(TransactionOutput {
        satoshis: Some(CENSUS_COIN),
        locking_script: LockingScript::from_binary(&lock).expect("lock"),
        change: false,
    });
    let parent_id = parent.id();

    let mut child = Transaction::new();
    child
        .inputs
        .push(TransactionInput::new(parent_id.clone(), 0));
    child.outputs.push(TransactionOutput {
        satoshis: Some(10_000),
        locking_script: LockingScript::from_binary(&lock).expect("lock"),
        change: false,
    });
    let mut data = Vec::with_capacity(bytes + 8);
    data.extend([0x00, 0x6a, 0x4e]);
    data.extend(u32::try_from(bytes).expect("data length").to_le_bytes());
    data.extend((0..=250u8).cycle().take(bytes));
    child.outputs.push(TransactionOutput {
        satoshis: Some(0),
        locking_script: LockingScript::from_binary(&data).expect("data"),
        change: false,
    });
    let script = Script::from_binary(&lock).expect("script");
    let unsigned = child.to_binary();
    let context = SigningContext::new(&unsigned, 0, CENSUS_COIN, &script);
    let signer = PrivateKey::from_hex(&format!("{signer:064x}")).expect("signer");
    let unlock = P2PKH::unlock(&signer, SignOutputs::All, false)
        .sign(&context)
        .expect("sign");
    child.inputs[0].unlocking_script = Some(unlock);
    child.invalidate_caches();
    let child_id = child.id();

    let mut beef = Beef::new();
    let index = beef.merge_bump(MerklePath::from_coinbase_txid(&parent_id, CENSUS_HEIGHT));
    beef.merge_raw_tx(parent.to_binary(), Some(index));
    beef.merge_raw_tx(child.to_binary(), None);
    let atomic = beef.to_binary_atomic(&child_id).expect("atomic");
    (atomic, child_id, parent_id)
}

/// Every input of the BEEF door corpus, by name.
pub(crate) fn corpus() -> Vec<(String, Vec<u8>)> {
    let directory = concat!(env!("CARGO_MANIFEST_DIR"), "/fuzz/corpus/beef_door");
    let mut inputs: Vec<_> = std::fs::read_dir(directory)
        .expect("the corpus")
        .map(|entry| {
            let path = entry.expect("a corpus entry").path();
            let name = path.file_name().expect("a name").to_string_lossy().into();
            (name, std::fs::read(&path).expect("a corpus input"))
        })
        .collect();
    inputs.sort();
    inputs
}

#[tokio::test]
async fn the_corpus_reads_the_same_on_both_paths() {
    let corpus = corpus();
    assert!(corpus.len() > 300, "the corpus is {} inputs", corpus.len());
    let mut accepted = 0;
    for (name, bytes) in &corpus {
        accepted += differential(name, bytes).await.unwrap();
    }
    // Most of the corpus is refused; the agreement must include acceptances.
    assert!(accepted > 0, "no corpus input was accepted on either path");
}

#[tokio::test]
async fn the_census_beefs_read_the_same_on_both_paths() {
    for megabytes in [1, 4, 10] {
        let (bytes, ..) = census_beef(megabytes * 1_000_000);
        let name = format!("census {megabytes} MB");
        let accepted = differential(&name, &bytes).await.unwrap();
        // Accepted under the tip with its root carried and with no tip;
        // refused with no root carried and under testnet's word, on both.
        assert_eq!(accepted, 2, "the census BEEF's acceptances");
    }
}

/// Every stored row of every table but the clocks and the `masked`
/// columns, each table in key order.
pub(crate) async fn stored_rows(storage: &StorageSqlx, masked: &[&str]) -> Vec<String> {
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' \
         AND name NOT LIKE '_sqlx%' ORDER BY name",
    )
    .fetch_all(storage.pool())
    .await
    .expect("tables");
    let mut rows = Vec::new();
    for table in tables {
        let query = format!("SELECT * FROM {table} ORDER BY 1");
        for row in sqlx::query(&query)
            .fetch_all(storage.pool())
            .await
            .expect("rows")
        {
            let mut line = format!("{table}:");
            for column in row.columns() {
                let name = column.name();
                if name.ends_with("_at") || masked.contains(&name) {
                    continue;
                }
                let raw = row.try_get_raw(name).expect("a column");
                let value = if raw.is_null() {
                    "null".to_string()
                } else {
                    match raw.type_info().name() {
                        "INTEGER" => row.try_get::<i64, _>(name).expect("int").to_string(),
                        "REAL" => row.try_get::<f64, _>(name).expect("real").to_string(),
                        "BLOB" => hex::encode(row.try_get::<Vec<u8>, _>(name).expect("blob")),
                        _ => row.try_get::<String, _>(name).expect("text"),
                    }
                };
                write!(line, " {name}={value}").expect("a string takes the text");
            }
            rows.push(line);
        }
    }
    rows
}

/// A seeded store whose header source carries the census parent's root.
pub(crate) async fn census_store(parent: &str) -> (StorageSqlx, i64) {
    let options = StorageSqlxOptions {
        rng_seed: Some([56; 32]),
        ..Default::default()
    };
    let storage = StorageSqlx::new_with_options("sqlite::memory:", options)
        .await
        .expect("store");
    storage.migrate("a8-census", "a8").await.expect("migrate");
    storage.make_available().await.expect("available");
    let root = MerklePath::from_coinbase_txid(parent, CENSUS_HEIGHT)
        .compute_root(None)
        .expect("root");
    let roots = HashSet::from([(CENSUS_HEIGHT, root)]);
    storage
        .set_chain_tracker(std::sync::Arc::new(Headers::census(Some(roots))))
        .await;
    crate::storage::MonitorStorage::set_max_acceptable_proof_height(&storage, CENSUS_HEIGHT)
        .await
        .expect("proof gate");
    let identity = "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798";
    let (user, _) = storage.find_or_insert_user(identity).await.expect("user");
    (storage, user.user_id)
}

fn insertion(beef: Vec<u8>) -> InternalizeActionArgs {
    InternalizeActionArgs {
        tx: beef,
        outputs: vec![InternalizeOutput {
            output_index: 0,
            protocol: "basket insertion".into(),
            payment_remittance: None,
            insertion_remittance: Some(BasketInsertion {
                basket: "census".into(),
                custom_instructions: None,
                tags: Some(vec!["a8".into()]),
            }),
        }],
        description: "census large intake".into(),
        labels: Some(vec!["a8".into()]),
        seek_permission: None,
    }
}

/// The internalize door on a fresh store, on one path: its answer and rows.
async fn internalize_on(read_once: bool, beef: &[u8], parent: &str) -> (String, Vec<String>) {
    let (storage, user_id) = census_store(parent).await;
    let _path = Path::choose(read_once);
    let result = internalize_action_internal(&storage, user_id, insertion(beef.to_vec())).await;
    let answer = match result {
        Ok(result) => format!("Ok({:?})", result.base),
        Err(e) => format!("Err({e:?} | {e})"),
    };
    // The door draws the action's reference at random, outside the seeded
    // stream: it differs between any two calls on either path.
    (answer, stored_rows(&storage, &["reference"]).await)
}

#[tokio::test]
async fn the_census_internalizes_the_same_rows_on_both_paths() {
    for megabytes in [1, 4, 10] {
        let (beef, child, parent) = census_beef(megabytes * 1_000_000);
        let oracle = internalize_on(false, &beef, &parent).await;
        let read_once = internalize_on(true, &beef, &parent).await;
        assert!(oracle.0.starts_with("Ok("), "{megabytes} MB: {}", oracle.0);
        assert!(oracle.1.iter().any(|row| row.contains(&child)));
        assert_eq!(oracle.0, read_once.0, "{megabytes} MB: the answers");
        if oracle.1 != read_once.1 {
            let shorten = |row: &String| row.chars().take(400).collect::<String>();
            for (a, b) in oracle.1.iter().zip(&read_once.1).filter(|(a, b)| a != b) {
                eprintln!("ORACLE {}\nREAD   {}", shorten(a), shorten(b));
            }
            panic!(
                "{megabytes} MB: the stored rows differ ({} and {} rows)",
                oracle.1.len(),
                read_once.1.len()
            );
        }
    }
}

#[tokio::test]
async fn the_census_refusals_are_the_same_on_both_paths() {
    // Signed by another key: the word refuses the spend.
    let (wrong_key, _, parent) = census_beef_signed_by(1_000, 8);
    // A data byte moved: the child is no longer the prefix's subject.
    let (mut moved, _, _) = census_beef(1_000);
    let at = moved.len() - 40;
    moved[at] ^= 1;
    // Cut short: the stream refuses where the bytes run out.
    let (mut short, _, _) = census_beef(1_000);
    short.truncate(short.len() - 3);
    for (scene, beef) in [("wrong key", wrong_key), ("moved", moved), ("short", short)] {
        let oracle = internalize_on(false, &beef, &parent).await;
        let read_once = internalize_on(true, &beef, &parent).await;
        assert!(oracle.0.starts_with("Err("), "{scene}: {}", oracle.0);
        assert_eq!(oracle, read_once, "{scene}");
    }
    // The parent's root not carried: the structure pass refuses the BUMP.
    let (beef, _, _) = census_beef(1_000);
    let oracle = internalize_on(false, &beef, &"22".repeat(32)).await;
    let read_once = internalize_on(true, &beef, &"22".repeat(32)).await;
    assert!(oracle.0.starts_with("Err("), "uncarried: {}", oracle.0);
    assert_eq!(oracle, read_once, "uncarried");
}
