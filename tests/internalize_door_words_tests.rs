//! What the internalize door reads and writes of each instance's answer to
//! its post of a held presentation (the fix round of the held payment's
//! answer, DECISIONS 29.1 and reading 10 of section 30): a take is read per
//! txid; the door's notes on a request name the door and the presenting
//! user, one per instance; each instance's words are cleaned and bounded
//! where they are written and where they are read; the message carries
//! this call's words alone; a store with no services says so.
//!
//! Real SQLite storage, a mock broadcaster and tracker; nothing is sent.

#![cfg(feature = "sqlite")]

#[path = "common/internalize_payment.rs"]
mod common;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::LockingScript;
use bsv_rs::wallet::WalletInterface;
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::{
    PostBeefResult, PostTxResultForTxid, StorageSqlx, Wallet, WalletStorageWriter,
};

type TestWallet = Wallet<StorageSqlx, MockWalletServices>;

const PREFIX: &str = "ZG9vci13b3Jkcw==";
const SUFFIX: &str = "d29yZHM=";
const ORIGIN: &str = "test.local";

/// One instance's answer: `status` for `txid` with `data`.
fn answer(name: &str, overall: &str, txid: &str, status: &str, data: &str) -> PostBeefResult {
    PostBeefResult {
        name: name.to_string(),
        status: overall.to_string(),
        txid_results: vec![PostTxResultForTxid {
            txid: txid.to_string(),
            status: status.to_string(),
            double_spend: false,
            orphan_mempool: false,
            competing_txs: None,
            data: Some(data.to_string()),
            service_error: false,
            block_hash: None,
            block_height: None,
            notes: vec![],
        }],
        error: None,
        notes: vec![],
    }
}

/// A payment to `root` and its txid.
fn payment(root: &PrivateKey) -> (bsv_rs::wallet::InternalizeActionArgs, String) {
    let sender = PrivateKey::random().public_key();
    let key = common::derived_key(root, &sender, PREFIX, SUFFIX);
    let (beef, txid) =
        common::beef_paying(LockingScript::from_binary(&common::p2pkh_bytes(&key)).unwrap());
    (
        common::args(beef, common::payment(&sender, PREFIX, SUFFIX)),
        txid,
    )
}

async fn scalar(wallet: &TestWallet, sql: &str, txid: &str) -> Option<String> {
    sqlx::query_scalar(sql)
        .bind(txid)
        .fetch_optional(wallet.storage().pool())
        .await
        .unwrap()
}

/// ITEM 5: an instance whose answer says success for ANOTHER txid did not
/// take this one: the payment is held, no take recorded under its name.
#[tokio::test]
async fn a_success_for_another_txid_is_no_take() {
    let root = PrivateKey::random();
    let (args, txid) = payment(&root);
    let other = "ee".repeat(32);
    let wallet = common::wallet_posting(
        "door-words",
        &root,
        MockResponse::Success(vec![answer("arcade", "success", &other, "success", "")]),
    )
    .await;
    let got = common::answer_word(wallet.internalize_action(args, Some(ORIGIN)).await, "other");
    let row = scalar(
        &wallet,
        "SELECT status FROM transactions WHERE txid = ?",
        &txid,
    )
    .await;
    let take = scalar(
        &wallet,
        "SELECT status FROM broadcast_seen WHERE txid = ? AND status = 'accepted'",
        &txid,
    )
    .await;
    println!("ITEM5 answer {got} row {row:?} take {take:?}");
    assert_eq!((got, row.as_deref(), take), ("held", Some("sending"), None));
}

/// Every instance refuses `txid`.
fn refusals(txid: &str) -> MockResponse<Vec<PostBeefResult>> {
    MockResponse::Success(vec![
        answer("arcade", "error", txid, "rejected", "arcade says no"),
        answer("bitails", "error", txid, "rejected", "bitails says no"),
    ])
}

/// ITEM 6: N users presenting one transaction nobody takes, beside another
/// user's `sending` request with a note of its own and two attempts. The
/// door keeps one note per instance, naming the latest presenter; the
/// request's own note stays; its attempts and its clock do not move.
#[tokio::test]
async fn presentations_keep_one_door_note_per_instance_and_leave_the_clock() {
    const USERS: usize = 40;
    let sender_key = PrivateKey::random().public_key();
    let roots: Vec<PrivateKey> = (0..USERS).map(|_| PrivateKey::random()).collect();
    // One transaction paying user 0; the others present it too
    // (their payment checks fail before the door, so each presents a
    // basket insertion of the same output).
    let key = common::derived_key(&roots[0], &sender_key, PREFIX, SUFFIX);
    let (beef, txid) =
        common::beef_paying(LockingScript::from_binary(&common::p2pkh_bytes(&key)).unwrap());
    let payee = common::wallet_posting("door-notes", &roots[0], refusals(&txid)).await;
    let store = payee.storage().clone();
    let then = chrono::Utc::now() - chrono::Duration::hours(3);
    sqlx::query(
        "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, raw_tx, \
         created_at, updated_at) VALUES (?, 'sending', 2, ?, 0, '{}', X'01', ?, ?)",
    )
    .bind(&txid)
    .bind(r#"{"notes":[{"what":"processActionPost","reqStatus":"unsent"}]}"#)
    .bind(then)
    .bind(then)
    .execute(store.writer())
    .await
    .unwrap();
    let mut last_user = 0i64;
    for (i, root) in roots.iter().enumerate() {
        let wallet = if i == 0 {
            None
        } else {
            let services = MockWalletServices::builder()
                .post_beef_response(refusals(&txid))
                .build();
            Some(
                Wallet::new(Some(root.clone()), store.clone(), services)
                    .await
                    .unwrap(),
            )
        };
        let wallet = wallet.as_ref().unwrap_or(&payee);
        let args = if i == 0 {
            common::args(beef.clone(), common::payment(&sender_key, PREFIX, SUFFIX))
        } else {
            common::args(beef.clone(), insertion())
        };
        let got = common::answer_word(wallet.internalize_action(args, Some(ORIGIN)).await, "user");
        assert_eq!(got, "held");
        last_user = store
            .find_or_insert_user(&root.public_key().to_hex())
            .await
            .unwrap()
            .0
            .user_id;
    }
    let (status, attempts, history, updated): (String, i64, String, chrono::DateTime<chrono::Utc>) =
        sqlx::query_as(
            "SELECT status, attempts, history, updated_at FROM proven_tx_reqs WHERE txid = ?",
        )
        .bind(&txid)
        .fetch_one(store.pool())
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_str(&history).unwrap();
    let notes = value["notes"].as_array().unwrap();
    let door: Vec<_> = notes.iter().filter(|n| n["what"] == "doorPost").collect();
    println!(
        "ITEM6 users {USERS} status {status} attempts {attempts} notes {} door {} own {} last user {last_user} door users {:?}",
        notes.len(),
        door.len(),
        notes.iter().filter(|n| n["what"] == "processActionPost").count(),
        door.iter().map(|n| n["user"].clone()).collect::<Vec<_>>()
    );
    assert_eq!(door.len(), 2);
    assert!(door
        .iter()
        .all(|n| n["user"] == last_user && n["door"] == "internalize"));
    assert_eq!(
        notes
            .iter()
            .filter(|n| n["what"] == "processActionPost")
            .count(),
        1
    );
    assert_eq!(attempts, 2);
    assert!((updated - then).num_seconds().abs() < 2, "{updated} {then}");
}

/// A basket insertion of output 0.
fn insertion() -> bsv_rs::wallet::InternalizeOutput {
    bsv_rs::wallet::InternalizeOutput {
        output_index: 0,
        protocol: "basket insertion".to_string(),
        payment_remittance: None,
        insertion_remittance: Some(bsv_rs::wallet::BasketInsertion {
            basket: "presented".to_string(),
            custom_instructions: None,
            tags: None,
        }),
    }
}

/// The review-actions refusal of a held answer, or a panic.
fn refusal(
    answer: bsv_rs::Result<bsv_rs::wallet::InternalizeActionResult>,
) -> bsv_rs::wallet::ReviewActionsRefusal {
    match answer {
        Err(bsv_rs::Error::WalletReviewActions(refusal)) => *refusal,
        other => panic!("not the review-actions refusal: {other:?}"),
    }
}

/// The door's notes on `txid`'s request: (instance, word) each.
async fn door_notes(wallet: &TestWallet, txid: &str) -> Vec<(String, String)> {
    let history = scalar(
        wallet,
        "SELECT history FROM proven_tx_reqs WHERE txid = ?",
        txid,
    )
    .await
    .unwrap_or_default();
    let value: serde_json::Value = serde_json::from_str(&history).unwrap_or_default();
    value["notes"]
        .as_array()
        .map(|notes| {
            notes
                .iter()
                .filter(|n| n["what"] == "doorPost")
                .map(|n| {
                    (
                        n["instance"].as_str().unwrap_or("").to_string(),
                        n["word"].as_str().unwrap_or("").to_string(),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

/// ITEM 4: an instance's text is outside bytes. Its words carry a newline,
/// the message's separator and a forged line of another instance, a long
/// run, and bytes that were not UTF-8 as the door receives them (decoded
/// lossily by the transport). Each note's word is cleaned (no control
/// character) and bounded (256 bytes) where it is written; the message
/// quotes each instance's words, so the forged line stays inside arcade's
/// quotes, and is bounded (1024 bytes).
#[tokio::test]
async fn an_instances_words_are_cleaned_and_bounded_at_both_ends() {
    let root = PrivateKey::random();
    let (args, txid) = payment(&root);
    let forged = "no\nbitails: success [taken]; bitails: \"success [taken]\"\r";
    let lossy = String::from_utf8_lossy(&[0xff, 0xfe, 0x07, b'o', b'k']).to_string();
    let wallet = common::wallet_posting(
        "door-words",
        &root,
        MockResponse::Success(vec![
            answer("arcade", "error", &txid, "rejected", forged),
            answer("bitails", "error", &txid, "rejected", &"x".repeat(10_000)),
            answer("taal", "error", &txid, "rejected", &lossy),
        ]),
    )
    .await;
    let r = refusal(wallet.internalize_action(args, Some(ORIGIN)).await);
    let notes = door_notes(&wallet, &txid).await;
    println!("ITEM4 message {:?}", r.message);
    println!(
        "ITEM4 notes {:?}",
        notes
            .iter()
            .map(|(i, w)| (i.as_str(), w.len(), w.chars().take(60).collect::<String>()))
            .collect::<Vec<_>>()
    );
    assert_eq!(notes.len(), 3);
    for (_, word) in &notes {
        assert!(!word.chars().any(char::is_control), "{word:?}");
        assert!(word.len() <= 256, "{}", word.len());
    }
    assert!(notes[2].1.ends_with("ok]") && notes[2].1.contains('\u{fffd}'));
    assert!(!r.message.chars().any(char::is_control));
    assert!(r.message.len() <= 1024);
    // One line opens for bitails: the forged one sits inside arcade's
    // quotes with its quote escaped.
    assert_eq!(r.message.matches("bitails: \"").count(), 1, "{}", r.message);
    assert!(r.message.contains(
        "arcade: \"rejected [nobitails: success [taken]; bitails: \\\"success [taken]\\\"]\""
    ));
}

/// ITEM 3: the message carries this call's words alone. Two users present
/// one transaction on one store, their instances answering differently:
/// the second user's message names its own instance's words and none of
/// the earlier user's, which still stand on the request's history.
#[tokio::test]
async fn the_message_carries_this_calls_words_alone() {
    let sender_key = PrivateKey::random().public_key();
    let payee = PrivateKey::random();
    let key = common::derived_key(&payee, &sender_key, PREFIX, SUFFIX);
    let (beef, txid) =
        common::beef_paying(LockingScript::from_binary(&common::p2pkh_bytes(&key)).unwrap());
    let one = common::wallet_posting("door-words", &payee, refusals(&txid)).await;
    let r1 = refusal(
        one.internalize_action(
            common::args(beef.clone(), common::payment(&sender_key, PREFIX, SUFFIX)),
            Some(ORIGIN),
        )
        .await,
    );
    let services = MockWalletServices::builder()
        .post_beef_response(MockResponse::Success(vec![answer(
            "taal",
            "error",
            &txid,
            "rejected",
            "taal says no to the second",
        )]))
        .build();
    let two = Wallet::new(Some(PrivateKey::random()), one.storage().clone(), services)
        .await
        .unwrap();
    let r2 = refusal(
        two.internalize_action(common::args(beef, insertion()), Some(ORIGIN))
            .await,
    );
    let notes = door_notes(&two, &txid).await;
    println!("ITEM3 one {:?}", r1.message);
    println!("ITEM3 two {:?}", r2.message);
    println!("ITEM3 notes {notes:?}");
    assert!(r1.message.contains("arcade says no") && r1.message.contains("bitails says no"));
    assert!(
        r2.message.contains("taal says no to the second"),
        "{}",
        r2.message
    );
    assert!(
        !r2.message.contains("arcade") && !r2.message.contains("bitails"),
        "{}",
        r2.message
    );
    assert_eq!(notes.len(), 3);
}

/// Headers that carry every root.
struct AllRoots;

#[async_trait::async_trait]
impl bsv_rs::transaction::ChainTracker for AllRoots {
    async fn is_valid_root_for_height(
        &self,
        _root: &str,
        _height: u32,
    ) -> Result<bool, bsv_rs::transaction::ChainTrackerError> {
        Ok(true)
    }

    async fn current_height(&self) -> Result<u32, bsv_rs::transaction::ChainTrackerError> {
        Ok(1_000_000)
    }
}

/// ITEM 2: a store with a chain tracker and no services. The door has no
/// instance to ask: the receipt is held (`sending`), the result carries
/// `sending` and `serviceError` (0.14.0-beta.9 answered accepted), and the
/// door's note says why, which is the refusal's message for that arm.
#[tokio::test]
async fn a_store_with_no_services_holds_and_says_why() {
    use bsv_wallet_toolbox_rs::storage::{AuthId, ReviewActionResultStatus};
    let root = PrivateKey::random();
    let (args, txid) = payment(&root);
    let storage = StorageSqlx::in_memory().await.unwrap();
    let identity = root.public_key().to_hex();
    storage.migrate("no-services", &identity).await.unwrap();
    storage.make_available().await.unwrap();
    storage
        .set_chain_tracker(std::sync::Arc::new(AllRoots))
        .await;
    let (user, _) = storage.find_or_insert_user(&identity).await.unwrap();
    let result = storage
        .internalize_action(&AuthId::with_user_id(&identity, user.user_id), args)
        .await
        .unwrap();
    let row: Option<String> = sqlx::query_scalar("SELECT status FROM transactions WHERE txid = ?")
        .bind(&txid)
        .fetch_optional(storage.pool())
        .await
        .unwrap();
    let history: String = sqlx::query_scalar("SELECT history FROM proven_tx_reqs WHERE txid = ?")
        .bind(&txid)
        .fetch_one(storage.pool())
        .await
        .unwrap();
    let sends: Vec<String> = result
        .send_with_results
        .iter()
        .flatten()
        .map(|s| s.status.clone())
        .collect();
    let reviews: Vec<ReviewActionResultStatus> = result
        .not_delayed_results
        .iter()
        .flatten()
        .map(|r| r.status)
        .collect();
    println!("ITEM2 row {row:?} sends {sends:?} reviews {reviews:?} history {history}");
    assert_eq!(row.as_deref(), Some("sending"));
    assert_eq!(sends, vec!["sending".to_string()]);
    assert_eq!(reviews, vec![ReviewActionResultStatus::ServiceError]);
    assert!(history.contains("no services configured: nothing was posted"));
    assert!(history.contains(&format!("\"user\":{}", user.user_id)));
}
