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
        ("no tip", Headers::no_tip()),
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

#[tokio::test]
async fn the_judge_leaves_a_held_ancestor_unjudged_on_both_paths() {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("a8-judge-held", "a8").await.unwrap();
    storage.make_available().await.unwrap();
    let (bytes, child, _) = census_beef_signed_by(1_000, 8);
    let headers = Headers::census(None);
    let word = CallerWord {
        tracker: Some(&headers),
        services: None,
        network: "main",
        verifier: None,
    };
    let mut conn = storage.pool().acquire().await.unwrap();
    let bare = judge_on(false, &mut conn, &bytes, &bytes, &word).await;
    assert_ne!(
        bare, "Ok",
        "the wrong key's spend is judged on a bare store"
    );
    assert_eq!(bare, judge_on(true, &mut conn, &bytes, &bytes, &word).await);
    // The store holds the child: `held` is not empty, the judged set is.
    sqlx::query("INSERT INTO proven_tx_reqs (txid, raw_tx) VALUES (?1, x'00')")
        .bind(&child)
        .execute(storage.writer())
        .await
        .unwrap();
    let oracle = judge_on(false, &mut conn, &bytes, &bytes, &word).await;
    assert_eq!(oracle, "Ok");
    assert_eq!(
        oracle,
        judge_on(true, &mut conn, &bytes, &bytes, &word).await
    );
}

/// createAction spending the census child's coin from the caller's BEEF,
/// signed later (`noSend`, no `signAndProcess`).
fn spend_of(beef: &[u8], child: &str) -> bsv_rs::wallet::CreateActionArgs {
    let mut args: bsv_rs::wallet::CreateActionArgs = serde_json::from_value(serde_json::json!({
        "description": "census caller beef",
        "inputs": [{
            "outpoint": format!("{child}.0"),
            "inputDescription": "census caller input",
            "unlockingScriptLength": 108
        }],
        "outputs": [{ "lockingScript": "51", "satoshis": 1000, "outputDescription": "census output" }],
        "options": { "signAndProcess": false, "noSend": true }
    }))
    .unwrap();
    args.input_beef = Some(beef.to_vec());
    args
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
