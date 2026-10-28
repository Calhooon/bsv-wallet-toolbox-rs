//! #389: the create refusal carries the stored competing spend through WalletInterface.

use super::test_support::{covenant_input, p2pkh_output, seed_custom_output, synth_tx};
use super::*;
use crate::services::mock::MockWalletServices;
use crate::storage::WalletStorageWriter;
use crate::Wallet;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::{CreateActionArgs, CreateActionOptions, ReviewActionResult, WalletInterface};

struct Fixture {
    wallet: Wallet<StorageSqlx, MockWalletServices>,
    source: String,
    competitor: String,
    raw: Vec<u8>,
}

async fn fixture() -> Fixture {
    let storage = StorageSqlx::in_memory().await.unwrap();
    let key = PrivateKey::random();
    let identity = key.public_key().to_hex();
    storage.migrate("review-tests", &identity).await.unwrap();
    storage.make_available().await.unwrap();
    let (user, _) = storage.find_or_insert_user(&identity).await.unwrap();
    let source = seed_custom_output(&storage, user.user_id, 100_000, &[0x51], "389").await;
    let (raw, competitor) = synth_tx(&source, 0, 99_000);
    let spender = sqlx::query(
        "INSERT INTO transactions
         (user_id, status, reference, is_outgoing, satoshis, description, txid, raw_tx)
         VALUES (?, 'unproven', 'competing-spend', 1, 0, 'Competing spend', ?, ?)",
    )
    .bind(user.user_id)
    .bind(&competitor)
    .bind(&raw)
    .execute(storage.writer())
    .await
    .unwrap()
    .last_insert_rowid();
    sqlx::query("UPDATE outputs SET spent_by = ?, spendable = 0 WHERE txid = ?")
        .bind(spender)
        .bind(&source)
        .execute(storage.writer())
        .await
        .unwrap();
    Fixture {
        wallet: Wallet::new(Some(key), storage, MockWalletServices::new())
            .await
            .unwrap(),
        source,
        competitor,
        raw,
    }
}

impl Fixture {
    async fn snapshot(&self) -> (i64, i64, i64, i64) {
        sqlx::query_as(
            "SELECT (SELECT COUNT(*) FROM transactions), (SELECT COUNT(*) FROM outputs),
                    (SELECT COUNT(*) FROM tx_labels), (SELECT spent_by FROM outputs WHERE txid = ?)",
        )
        .bind(&self.source)
        .fetch_one(self.wallet.storage().pool())
        .await
        .unwrap()
    }

    fn args(&self) -> CreateActionArgs {
        CreateActionArgs {
            description: "Review the competing spend".into(),
            input_beef: None,
            inputs: Some(vec![covenant_input(&self.source, vec![0x51])]),
            outputs: Some(vec![p2pkh_output(1_000, "Review payment")]),
            lock_time: None,
            version: None,
            labels: Some(vec!["review-regression".into()]),
            options: Some(CreateActionOptions {
                no_send: Some(true),
                sign_and_process: Some(false),
                ..CreateActionOptions::default()
            }),
        }
    }

    async fn refuse(&self) -> ReviewActionResult {
        let before = self.snapshot().await;
        let args = self.args();
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            WalletInterface::create_action(&self.wallet, args, None),
        )
        .await
        .expect("review assembly must not wait on the held writer")
        .expect_err("a stored spender requires review");
        let bsv_rs::Error::WalletReviewActions(refusal) = error else {
            panic!("expected WalletReviewActions, got {error:?}");
        };
        assert!(refusal.send_with_results.is_empty());
        let [review] = refusal.review_action_results.as_slice() else {
            panic!("expected one competing spend: {refusal:?}");
        };
        assert_eq!(
            (review.txid, review.status),
            (
                [0; 32],
                bsv_rs::wallet::ReviewActionResultStatus::DoubleSpend
            )
        );
        assert_eq!(
            review.competing_txs,
            Some(vec![hex::decode(&self.competitor)
                .unwrap()
                .try_into()
                .unwrap()])
        );
        assert_eq!(
            self.snapshot().await,
            before,
            "refusal preserves the stored spend and action rows"
        );
        review.clone()
    }

    /// A proof request for the competitor in `status`, holding its raw bytes.
    async fn request(&self, status: &str, input_beef: Option<Vec<u8>>) {
        sqlx::query(
            "INSERT INTO proven_tx_reqs (txid, status, raw_tx, input_beef) VALUES (?, ?, ?, ?)",
        )
        .bind(&self.competitor)
        .bind(status)
        .bind(&self.raw)
        .bind(input_beef)
        .execute(self.wallet.storage().writer())
        .await
        .unwrap();
    }

    async fn beef(&self) -> Beef {
        let review = self.refuse().await;
        Beef::from_binary(
            review
                .competing_beef
                .as_deref()
                .expect("competing BEEF is present"),
        )
        .expect("competing BEEF parses")
    }
}

#[tokio::test]
async fn create_review_carries_competing_beef_with_stored_proof() {
    let f = fixture().await;
    seed_proven_tx(
        f.wallet.storage(),
        &f.competitor,
        &f.raw,
        &synth_bump(100_002, &f.competitor).to_binary(),
    )
    .await;
    let mut beef = f.beef().await;
    assert_eq!(
        beef.find_txid(&f.competitor).unwrap().raw_tx(),
        Some(f.raw.as_slice())
    );
    assert_eq!(beef.txs.len(), 1, "a proof ends the ancestry walk");
    assert!(beef.find_bump(&f.competitor).is_some());
    assert!(beef.verify_valid(false).valid);
}

#[tokio::test]
async fn create_review_carries_competing_beef_with_stored_ancestry() {
    let f = fixture().await;
    f.request("unsent", None).await;
    let mut beef = f.beef().await;
    assert_eq!(
        beef.find_txid(&f.competitor).unwrap().raw_tx(),
        Some(f.raw.as_slice())
    );
    assert!(beef.find_txid(&f.source).is_some());
    assert!(beef.find_bump(&f.source).is_some());
    assert!(beef.verify_valid(false).valid);
}

#[tokio::test]
async fn create_review_carries_competing_beef_from_request_input_beef() {
    let f = fixture().await;
    let mut ancestry = Beef::new();
    let source_raw: Vec<u8> = sqlx::query_scalar("SELECT raw_tx FROM proven_txs WHERE txid = ?")
        .bind(&f.source)
        .fetch_one(f.wallet.storage().pool())
        .await
        .unwrap();
    let bump = ancestry.merge_bump(synth_bump(100_003, &f.source));
    ancestry.merge_raw_tx(source_raw, Some(bump));
    sqlx::query("UPDATE transactions SET raw_tx = NULL WHERE txid = ?")
        .bind(&f.competitor)
        .execute(f.wallet.storage().writer())
        .await
        .unwrap();
    f.request("unmined", Some(ancestry.to_binary())).await;
    let beef = f.beef().await;
    assert_eq!(
        beef.find_txid(&f.competitor).unwrap().raw_tx(),
        Some(f.raw.as_slice())
    );
    assert_eq!(beef.find_bump(&f.source).unwrap().block_height, 100_003);
}

#[tokio::test]
async fn create_review_carries_txid_only_beef_when_raw_tx_is_missing() {
    let f = fixture().await;
    sqlx::query("UPDATE transactions SET raw_tx = NULL WHERE txid = ?")
        .bind(&f.competitor)
        .execute(f.wallet.storage().writer())
        .await
        .unwrap();
    let beef = f.beef().await;
    assert_eq!(beef.txs.len(), 1);
    assert!(beef.find_txid(&f.competitor).unwrap().is_txid_only());
    assert!(beef.bumps.is_empty());
}

/// The review's probe: `getProvenOrRawTx` (StorageKnex.ts:189-210) reads
/// `proven_txs`, then a request in its six statuses, never the action's own
/// `transactions.raw_tx`. A `doubleSpend` request with a stored row answers
/// txid-only, as the reference does when services also miss.
#[tokio::test]
async fn create_review_reads_no_transaction_row_beside_a_double_spend_request() {
    let f = fixture().await;
    f.request("doubleSpend", None).await;
    let beef = f.beef().await;
    assert_eq!(beef.txs.len(), 1);
    assert!(beef.find_txid(&f.competitor).unwrap().is_txid_only());
    assert!(beef.bumps.is_empty());
}

/// A stored action row with no proven tx and no request answers txid-only:
/// create asks no provider for the bytes.
#[tokio::test]
async fn create_review_reads_no_transaction_row_without_a_request() {
    let f = fixture().await;
    let beef = f.beef().await;
    assert_eq!(beef.txs.len(), 1);
    assert!(beef.find_txid(&f.competitor).unwrap().is_txid_only());
}

impl Fixture {
    /// After each of the first `moves` review reads, store a new competitor
    /// for the input, as another writer would between the read and the lock.
    /// Returns the read count and the competitors stored, in order.
    fn move_competitor_after_read(
        &self,
        moves: usize,
    ) -> (
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
        std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    ) {
        use std::sync::atomic::Ordering;
        let reads = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let moved = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let (storage, source) = (self.wallet.storage().clone(), self.source.clone());
        let (hook_reads, hook_moved) = (reads.clone(), moved.clone());
        review_beef::race::set_after_read(Box::new(move || {
            let read = hook_reads.fetch_add(1, Ordering::SeqCst);
            let (storage, source, moved) = (storage.clone(), source.clone(), hook_moved.clone());
            Box::pin(async move {
                if read >= moves {
                    return;
                }
                let (raw, txid) = synth_tx(&source, 0, 98_000 - read as u64);
                let spender = sqlx::query(
                    "INSERT INTO transactions
                     (user_id, status, reference, is_outgoing, satoshis, description, txid)
                     SELECT user_id, 'unproven', ?, 1, 0, 'Raced spend', ?
                     FROM outputs WHERE txid = ?",
                )
                .bind(format!("raced-{read}"))
                .bind(&txid)
                .bind(&source)
                .execute(storage.writer())
                .await
                .unwrap()
                .last_insert_rowid();
                sqlx::query("UPDATE outputs SET spent_by = ? WHERE txid = ?")
                    .bind(spender)
                    .bind(&source)
                    .execute(storage.writer())
                    .await
                    .unwrap();
                sqlx::query(
                    "INSERT INTO proven_tx_reqs (txid, status, raw_tx) VALUES (?, 'unsent', ?)",
                )
                .bind(&txid)
                .bind(&raw)
                .execute(storage.writer())
                .await
                .unwrap();
                moved.lock().unwrap().push(txid);
            })
        }));
        (reads, moved)
    }
}

/// A competitor that changes between the review read and the lock: the
/// locked check names one the read did not prepare, so the action rolls
/// back (`Again::CompetingSpend`) and repeats the pre-check, whose second
/// read carries the new competitor's BEEF.
#[tokio::test]
async fn create_review_rereads_a_competitor_that_moved_before_the_lock() {
    use std::sync::atomic::Ordering;
    let f = fixture().await;
    let (reads, moved) = f.move_competitor_after_read(1);
    let error = WalletInterface::create_action(&f.wallet, f.args(), None)
        .await
        .expect_err("a stored spender requires review");
    let bsv_rs::Error::WalletReviewActions(refusal) = error else {
        panic!("expected WalletReviewActions, got {error:?}");
    };
    assert_eq!(reads.load(Ordering::SeqCst), 2, "the pre-check repeated");
    let raced = moved.lock().unwrap()[0].clone();
    let [review] = refusal.review_action_results.as_slice() else {
        panic!("expected one competing spend: {refusal:?}");
    };
    assert_eq!(
        review.competing_txs,
        Some(vec![hex::decode(&raced).unwrap().try_into().unwrap()])
    );
    let beef = Beef::from_binary(
        review
            .competing_beef
            .as_deref()
            .expect("the repeated read prepared the new competitor's BEEF"),
    )
    .unwrap();
    assert!(beef.find_txid(&raced).is_some_and(|tx| !tx.is_txid_only()));
    assert!(beef.find_txid(&f.competitor).is_none());
}

/// The competitor moves before every lock: the repeat finds it moved again,
/// and the call answers busy, never a review without its evidence.
#[tokio::test]
async fn create_review_answers_busy_when_the_competitor_moves_twice() {
    use std::sync::atomic::Ordering;
    let f = fixture().await;
    let (reads, _) = f.move_competitor_after_read(usize::MAX);
    let error = WalletInterface::create_action(&f.wallet, f.args(), None)
        .await
        .expect_err("the store moved under the call twice");
    assert!(
        matches!(error, bsv_rs::Error::WalletBusy { .. }),
        "expected WalletBusy, got {error:?}"
    );
    assert_eq!(reads.load(Ordering::SeqCst), 2);
}
