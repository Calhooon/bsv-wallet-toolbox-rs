//! The create side of the A8 cycle's differential target (#556): the
//! caller's `inputBEEF` judge and the create door on the `a8-beef-bytes`
//! path and the oracle path in one binary, every corpus input and the
//! census's 1, 4 and 10 MB BEEFs, any difference a failure
//! (`internalize_action/a8_beef_bytes_tests.rs` holds the scenes).

use super::super::*;
use super::*;
use crate::storage::sqlx::internalize_action::a8_beef_bytes_tests::{
    census_beef, census_beef_signed_by, census_store, corpus, stored_rows, Path,
};
use crate::storage::sqlx::internalize_action::a8_differential::Headers;
use crate::storage::traits::WalletStorageWriter;
use bsv_rs::wallet::{CreateActionInput, CreateActionOptions, CreateActionOutput, Outpoint};

/// The judge on one path: its answer's form and text.
async fn judge_on(
    read_once: bool,
    conn: &mut SqliteConnection,
    assembled: &[u8],
    caller: &[u8],
    word: &CallerWord<'_>,
) -> String {
    let _path = Path::choose(read_once);
    match judge_caller_beef(conn, Some(assembled), Some(caller), word).await {
        Ok(()) => "Ok".into(),
        Err(e) => format!("Err({e:?} | {e})"),
    }
}

#[tokio::test]
async fn the_judge_reads_the_corpus_the_same_on_both_paths() {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("a8-judge", "a8").await.unwrap();
    storage.make_available().await.unwrap();
    let mut conn = storage.pool().acquire().await.unwrap();
    let mut scenes = corpus();
    for megabytes in [1, 4, 10] {
        scenes.push((
            format!("census {megabytes} MB"),
            census_beef(megabytes * 1_000_000).0,
        ));
    }
    scenes.push((
        "census, wrong key".into(),
        census_beef_signed_by(1_000, 8).0,
    ));
    // The door's precondition: the judge runs only on a caller's BEEF the
    // assembly parsed (`Beef::from_binary`) and an assembly it wrote. On
    // bytes that do not parse the oracle answers `Ok` unjudged, an answer
    // no door reaches.
    let all = scenes.len();
    scenes.retain(|(_, bytes)| Beef::from_binary(bytes).is_ok());
    assert!(
        scenes.len() * 2 > all,
        "{} of {all} scenes parse",
        scenes.len()
    );
    let mut refused = 0;
    for (tip, headers) in [
        ("tip", Headers::census(None)),
        (
            "no tip",
            Headers {
                tip: None,
                roots: None,
            },
        ),
    ] {
        for network in ["main", "test"] {
            let word = CallerWord {
                tracker: Some(&headers),
                services: None,
                network,
                verifier: None,
            };
            for (name, bytes) in &scenes {
                let oracle = judge_on(false, &mut conn, bytes, bytes, &word).await;
                let read_once = judge_on(true, &mut conn, bytes, bytes, &word).await;
                assert_eq!(oracle, read_once, "{name} ({tip}, {network})");
                refused += usize::from(oracle != "Ok");
            }
        }
    }
    // The wrong key's spend is refused under every context at least.
    assert!(refused >= 4, "{refused} refusals");
}

/// createAction spending the census child's coin from the caller's BEEF.
fn spend_of(beef: &[u8], child: &str) -> bsv_rs::wallet::CreateActionArgs {
    let mut txid = [0u8; 32];
    hex::decode_to_slice(child, &mut txid).unwrap();
    bsv_rs::wallet::CreateActionArgs {
        description: "census caller beef".into(),
        input_beef: Some(beef.to_vec()),
        inputs: Some(vec![CreateActionInput {
            outpoint: Outpoint::new(txid, 0),
            input_description: "census caller input".into(),
            unlocking_script: None,
            unlocking_script_length: Some(108),
            sequence_number: None,
        }]),
        outputs: Some(vec![CreateActionOutput {
            locking_script: vec![0x51],
            satoshis: 1_000,
            output_description: "census output".into(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: None,
        options: Some(CreateActionOptions {
            sign_and_process: Some(false),
            no_send: Some(true),
            ..Default::default()
        }),
    }
}

/// The create door on a fresh store, on one path: its answer and rows.
async fn create_on(
    read_once: bool,
    beef: &[u8],
    child: &str,
    parent: &str,
) -> (String, Vec<String>) {
    let (storage, user_id) = census_store(parent).await;
    let _path = Path::choose(read_once);
    let answer = match create_action_internal(&storage, None, user_id, spend_of(beef, child)).await
    {
        Ok(result) => format!("Ok({result:?})"),
        Err(e) => format!("Err({e:?} | {e})"),
    };
    (answer, stored_rows(&storage, &[]).await)
}

#[tokio::test]
async fn the_census_creates_the_same_rows_on_both_paths() {
    for megabytes in [1, 4, 10] {
        let (beef, child, parent) = census_beef(megabytes * 1_000_000);
        let oracle = create_on(false, &beef, &child, &parent).await;
        let read_once = create_on(true, &beef, &child, &parent).await;
        assert!(
            oracle.0.starts_with("Ok("),
            "{megabytes} MB: {}",
            &oracle.0[..oracle.0.len().min(300)]
        );
        assert!(
            oracle.0 == read_once.0,
            "{megabytes} MB: the answers differ"
        );
        assert!(
            oracle.1 == read_once.1,
            "{megabytes} MB: the stored rows differ"
        );
    }
    // The caller's unproven child signed by another key: the judge refuses.
    let (beef, child, parent) = census_beef_signed_by(1_000, 8);
    let oracle = create_on(false, &beef, &child, &parent).await;
    let read_once = create_on(true, &beef, &child, &parent).await;
    assert!(oracle.0.starts_with("Err("), "{}", oracle.0);
    assert_eq!(oracle, read_once);
}
