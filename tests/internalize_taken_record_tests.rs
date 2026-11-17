//! Taken is a recorded take or a stored proof whose root was checked (the
//! owner's ruling of 2026-11-17, DECISIONS 29.1).
//!
//! At the internalize door a new received row with no checked proof is
//! taken when the door's own post drew an acceptance, when the store holds
//! a record of a take for the txid (`broadcast_seen` `accepted`, `seen` or
//! `mined`), or when a stored proof whose root was checked is held for it.
//! A request's word alone does not make it taken, nor does a stored proof
//! whose root no chain tracker checked (a row a sync chunk wrote); the door
//! records its own take whichever services a host built; the per-user
//! `review_status` heals a held row only on the same evidence; migration
//! 006 keeps the request's word for rows written before and reads the same
//! checked proof.
//!
//! Two or three users on one real SQLite store, mock broadcasters and
//! trackers; nothing is sent.

#![cfg(feature = "sqlite")]

#[path = "common/internalize_payment.rs"]
mod common;

use bsv_rs::primitives::{PrivateKey, PublicKey};
use bsv_rs::script::LockingScript;
use bsv_rs::transaction::Beef;
use bsv_rs::wallet::{
    BasketInsertion, CreateActionArgs, CreateActionOptions, CreateActionOutput,
    InternalizeActionArgs, InternalizeOutput, WalletInterface,
};
use bsv_wallet_toolbox_rs::services::mock::{MockErrorKind, MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::storage::entities::{
    ProvenTxReqStatus, TableProvenTx, TableProvenTxReq,
};
use bsv_wallet_toolbox_rs::storage::{RequestSyncChunkArgs, SyncChunk};
use bsv_wallet_toolbox_rs::{
    AuthId, PostBeefResult, StorageSqlx, Wallet, WalletStorageSync, WalletStorageWriter,
};

type TestWallet = Wallet<StorageSqlx, MockWalletServices>;

const PREFIX: &str = "dGFrZW4tcHJlZml4";
const ORIGIN: &str = "test.local";
const LABEL: &str = "takenrecord";
/// The one output the tests' BEEFs carry (`common::beef_paying`).
const PAID: u64 = 4500;

fn no_answer() -> MockResponse<Vec<PostBeefResult>> {
    MockResponse::Error(MockErrorKind::NetworkError, "no instance answered".into())
}

/// A second wallet of `root` over the same store as `on`.
async fn beside(
    on: &TestWallet,
    root: &PrivateKey,
    post: MockResponse<Vec<PostBeefResult>>,
) -> TestWallet {
    let services = MockWalletServices::builder()
        .post_beef_response(post)
        .build();
    Wallet::new(Some(root.clone()), on.storage().clone(), services)
        .await
        .expect("wallet")
}

/// A paying BEEF to `root`'s BRC-29 key from `sender`: the BEEF, the
/// payment output and the txid.
fn paying(
    root: &PrivateKey,
    sender: &PublicKey,
    suffix: &str,
) -> (Vec<u8>, InternalizeOutput, String) {
    let key = common::derived_key(root, sender, PREFIX, suffix);
    let lock = LockingScript::from_binary(&common::p2pkh_bytes(&key)).unwrap();
    let (beef, txid) = common::beef_paying(lock);
    (beef, common::payment(sender, PREFIX, suffix), txid)
}

fn args(tx: Vec<u8>, output: InternalizeOutput) -> InternalizeActionArgs {
    let mut args = common::args(tx, output);
    args.description = "a received transaction".to_string();
    args.labels = Some(vec![LABEL.to_string()]);
    args
}

/// A basket insertion of `vout` into `tokens`.
fn insertion(vout: u32) -> InternalizeOutput {
    InternalizeOutput {
        output_index: vout,
        protocol: "basket insertion".to_string(),
        payment_remittance: None,
        insertion_remittance: Some(BasketInsertion {
            basket: "tokens".to_string(),
            custom_instructions: None,
            tags: None,
        }),
    }
}

async fn internalize(wallet: &TestWallet, args: InternalizeActionArgs) {
    let answer = wallet.internalize_action(args, Some(ORIGIN)).await;
    assert!(answer.expect("accepted").accepted);
}

async fn user_id(wallet: &TestWallet, root: &PrivateKey) -> i64 {
    let (user, _) = wallet
        .storage()
        .find_or_insert_user(&root.public_key().to_hex())
        .await
        .unwrap();
    user.user_id
}

/// The status of `root`'s row for `txid`.
async fn status_of(wallet: &TestWallet, root: &PrivateKey, txid: &str) -> String {
    let user = user_id(wallet, root).await;
    sqlx::query_scalar("SELECT status FROM transactions WHERE txid = ? AND user_id = ?")
        .bind(txid)
        .bind(user)
        .fetch_one(wallet.storage().pool())
        .await
        .unwrap()
}

async fn request_word(wallet: &TestWallet, txid: &str) -> String {
    sqlx::query_scalar("SELECT status FROM proven_tx_reqs WHERE txid = ?")
        .bind(txid)
        .fetch_one(wallet.storage().pool())
        .await
        .unwrap()
}

async fn records(wallet: &TestWallet, txid: &str) -> Vec<(String, String)> {
    sqlx::query_as("SELECT provider, status FROM broadcast_seen WHERE txid = ? ORDER BY provider")
        .bind(txid)
        .fetch_all(wallet.storage().pool())
        .await
        .unwrap()
}

async fn sql(wallet: &TestWallet, statement: &str, txid: &str) {
    sqlx::query(statement)
        .bind(txid)
        .execute(wallet.storage().writer())
        .await
        .unwrap();
}

/// `Ok` when an action under the `underReceivedRule` label took a coin.
async fn offered_under_label(wallet: &TestWallet) -> Result<(), String> {
    wallet
        .create_action(spend(common::other_lock().to_binary(), 1_000), Some(ORIGIN))
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// A `noSend` spend of `satoshis` to `lock` under `underReceivedRule`.
fn spend(lock: Vec<u8>, satoshis: u64) -> CreateActionArgs {
    CreateActionArgs {
        description: "a spend".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script: lock,
            satoshis,
            output_description: "a payment".to_string(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: Some(vec!["underReceivedRule".to_string(), LABEL.to_string()]),
        options: Some(CreateActionOptions {
            no_send: Some(true),
            sign_and_process: Some(true),
            ..Default::default()
        }),
    }
}

/// Held: the row `sending`, the balance 0, nothing offered under the label.
async fn assert_held(wallet: &TestWallet, root: &PrivateKey, txid: &str, case: &str) {
    let status = status_of(wallet, root, txid).await;
    let balance = wallet.balance().await.unwrap().total;
    let label = offered_under_label(wallet).await;
    println!("{case}: status {status} balance {balance} label {label:?}");
    assert_eq!((status.as_str(), balance), ("sending", 0), "{case}");
    assert!(label.unwrap_err().contains("Insufficient funds"), "{case}");
}

/// User A's `noSend` action T paying B, merged by A's own internalize with no
/// BUMP: the request goes `unmined`. Returns A's wallet, B's
/// wallet and root, T's atomic BEEF, B's payment output and T's txid.
async fn nosend_merged_by_its_owner() -> (
    TestWallet,
    TestWallet,
    PrivateKey,
    Vec<u8>,
    InternalizeOutput,
    String,
) {
    let root_a = PrivateKey::random();
    let (fund, fund_output, fund_txid) =
        paying(&root_a, &PrivateKey::random().public_key(), "ZnVuZA==");
    let wallet_a = common::wallet(&root_a.public_key().to_hex()[..8], &root_a, &fund_txid).await;
    internalize(&wallet_a, args(fund.clone(), fund_output)).await;
    assert_eq!(wallet_a.balance().await.unwrap().total, PAID);

    let root_b = PrivateKey::random();
    let sender = root_a.public_key();
    let suffix = "Yi1wYWlk";
    let lock_b = common::p2pkh_bytes(&common::derived_key(&root_b, &sender, PREFIX, suffix));
    let made = wallet_a
        .create_action(spend(lock_b, 1_000), Some(ORIGIN))
        .await
        .expect("a noSend action");
    let t = hex::encode(made.txid.expect("txid"));
    assert_eq!(status_of(&wallet_a, &root_a, &t).await, "nosend");
    let (raw, paid_vout): (Vec<u8>, i64) = sqlx::query_as(
        "SELECT t.raw_tx, o.vout FROM transactions t JOIN outputs o \
         ON o.transaction_id = t.transaction_id WHERE t.txid = ? AND o.satoshis = 1000",
    )
    .bind(&t)
    .fetch_one(wallet_a.storage().pool())
    .await
    .unwrap();
    let paid_vout = u32::try_from(paid_vout).unwrap();
    let mut atomic = Beef::from_binary(&fund).unwrap();
    atomic.merge_raw_tx(raw, None).unwrap();
    let beef_t = atomic.to_binary_atomic(&t).unwrap();

    internalize(&wallet_a, args(beef_t.clone(), insertion(paid_vout))).await;
    assert_eq!(request_word(&wallet_a, &t).await, "unmined");
    assert!(records(&wallet_a, &t).await.is_empty(), "nobody posted T");

    let wallet_b = beside(&wallet_a, &root_b, no_answer()).await;
    let mut payment = common::payment(&sender, PREFIX, suffix);
    payment.output_index = paid_vout;
    (wallet_a, wallet_b, root_b, beef_t, payment, t)
}

/// Witness 1. RED at the base (the door read the request's `unmined`).
#[tokio::test]
async fn a_request_merged_unmined_by_its_owner_does_not_make_another_users_receipt_taken() {
    let (_a, wallet_b, root_b, beef_t, payment, t) = nosend_merged_by_its_owner().await;
    internalize(&wallet_b, args(beef_t, payment)).await;
    assert_held(
        &wallet_b,
        &root_b,
        &t,
        "B beside A's merged unmined request",
    )
    .await;
}

/// Witness 3. RED at the base (the per-user heal moved `sending` with an
/// `unmined` request to `unproven`).
#[tokio::test]
async fn the_per_user_review_status_does_not_heal_a_receipt_on_a_request_word() {
    let (_a, wallet_b, root_b, beef_t, payment, t) = nosend_merged_by_its_owner().await;
    internalize(&wallet_b, args(beef_t, payment)).await;
    sql(
        &wallet_b,
        "UPDATE transactions SET status = 'sending' WHERE txid = ?",
        &t,
    )
    .await;
    let auth = AuthId::with_user_id(
        root_b.public_key().to_hex(),
        user_id(&wallet_b, &root_b).await,
    );
    let review = wallet_b
        .storage()
        .review_status(&auth, chrono::Utc::now())
        .await
        .expect("review_status");
    println!("review: {}", review.log.trim());
    assert_held(&wallet_b, &root_b, &t, "B after its own review_status").await;

    // With a recorded take the heal moves it.
    sql(
        &wallet_b,
        "INSERT INTO broadcast_seen (txid, provider, status) VALUES (?, 'arc', 'accepted')",
        &t,
    )
    .await;
    wallet_b
        .storage()
        .review_status(&auth, chrono::Utc::now())
        .await
        .unwrap();
    assert_eq!(status_of(&wallet_b, &root_b, &t).await, "unproven");
}

/// Witness 2. A request for T written by a sync chunk, `unmined`, or
/// `completed` with no proof row: B's internalize of T with no proof is
/// held. RED at the base for both words.
#[tokio::test]
async fn a_request_word_a_sync_chunk_wrote_does_not_make_a_receipt_taken() {
    for word in [ProvenTxReqStatus::Unmined, ProvenTxReqStatus::Completed] {
        let root = PrivateKey::random();
        let (beef, output, txid) = paying(&root, &PrivateKey::random().public_key(), "c3luYw==");
        let wallet = common::wallet_posting("taken-record", &root, no_answer()).await;
        let other = PrivateKey::random().public_key().to_hex();
        let sync_args = RequestSyncChunkArgs {
            from_storage_identity_key: "elsewhere".into(),
            to_storage_identity_key: "here".into(),
            identity_key: other.clone(),
            since: None,
            max_rough_size: 100_000,
            max_items: 100,
            offsets: vec![],
        };
        let now = chrono::Utc::now();
        let chunk = SyncChunk {
            from_storage_identity_key: "elsewhere".into(),
            to_storage_identity_key: "here".into(),
            user_identity_key: other,
            proven_tx_reqs: Some(vec![TableProvenTxReq {
                proven_tx_req_id: 7,
                txid: txid.clone(),
                status: word,
                attempts: 0,
                history: "{}".into(),
                notified: false,
                notify: "{}".into(),
                raw_tx: Some(vec![1]),
                input_beef: None,
                proven_tx_id: None,
                batch: None,
                created_at: now,
                updated_at: now,
            }]),
            ..Default::default()
        };
        wallet
            .storage()
            .process_sync_chunk(sync_args, chunk)
            .await
            .expect("the chunk");
        let stored = request_word(&wallet, &txid).await;
        assert_eq!(serde_json::to_value(word).unwrap(), stored.as_str());
        internalize(&wallet, args(beef, output)).await;
        assert_held(
            &wallet,
            &root,
            &txid,
            &format!("beside a synced {stored} request"),
        )
        .await;
    }
}

/// Witness 4. A recorded take for T, then B's internalize: `unproven`,
/// counted, offered under the label.
#[tokio::test]
async fn a_recorded_take_makes_a_receipt_taken() {
    let root = PrivateKey::random();
    let (beef, output, txid) = paying(&root, &PrivateKey::random().public_key(), "cmVjb3Jk");
    let wallet = common::wallet_posting("taken-record", &root, no_answer()).await;
    sql(
        &wallet,
        "INSERT INTO broadcast_seen (txid, provider, status) VALUES (?, 'arc', 'accepted')",
        &txid,
    )
    .await;
    internalize(&wallet, args(beef, output)).await;
    assert_eq!(status_of(&wallet, &root, &txid).await, "unproven");
    assert_eq!(wallet.balance().await.unwrap().total, PAID);
    assert_eq!(offered_under_label(&wallet).await, Ok(()));
}

/// Witness 5. With services that record nothing (the mock), the door's own
/// accepted post: the row `unproven` and a record written by the door; a
/// second user's internalize of the same txid reads it taken. RED at the
/// base (no record).
#[tokio::test]
async fn the_door_records_its_own_take() {
    let root = PrivateKey::random();
    let (beef, output, txid) = paying(&root, &PrivateKey::random().public_key(), "ZG9vcg==");
    let wallet = common::wallet("taken-record", &root, &txid).await;
    internalize(&wallet, args(beef.clone(), output)).await;
    let written = records(&wallet, &txid).await;
    println!(
        "the door's post accepted: status {} records {written:?}",
        status_of(&wallet, &root, &txid).await
    );
    assert_eq!(status_of(&wallet, &root, &txid).await, "unproven");
    assert_eq!(written, vec![("arc".to_string(), "accepted".to_string())]);

    let second = PrivateKey::random();
    let wallet_c = beside(&wallet, &second, no_answer()).await;
    internalize(&wallet_c, args(beef, insertion(0))).await;
    assert_eq!(status_of(&wallet_c, &second, &txid).await, "unproven");
}

/// Witness 6. Migration 006's own rule on legacy rows (written `unproven`,
/// no record kept): a request `unmined` is left; `unsent` moves to
/// `sending`; any request with a record of a take is left.
#[tokio::test]
async fn migration_006_keeps_the_request_word_for_legacy_rows() {
    let root = PrivateKey::random();
    let wallet = common::wallet_posting("taken-record", &root, no_answer()).await;
    let mut legacy = Vec::new();
    for (suffix, word, record) in [
        ("bGVmdA==", "unmined", false),
        ("bW92ZWQ=", "unsent", false),
        ("a2VwdA==", "unsent", true),
    ] {
        let (beef, output, txid) = paying(&root, &PrivateKey::random().public_key(), suffix);
        internalize(&wallet, args(beef, output)).await;
        sql(
            &wallet,
            "UPDATE transactions SET status = 'unproven' WHERE txid = ?",
            &txid,
        )
        .await;
        sqlx::query("UPDATE proven_tx_reqs SET status = ? WHERE txid = ?")
            .bind(word)
            .bind(&txid)
            .execute(wallet.storage().writer())
            .await
            .unwrap();
        if record {
            sql(
                &wallet,
                "INSERT INTO broadcast_seen (txid, provider, status) VALUES (?, 'arc', 'accepted')",
                &txid,
            )
            .await;
        }
        legacy.push((txid, word, record));
    }
    wallet.storage().make_available().await.expect("open");
    let mut after = Vec::new();
    for (txid, word, record) in &legacy {
        after.push((*word, *record, status_of(&wallet, &root, txid).await));
    }
    println!("migration 006 on legacy rows: {after:?}");
    assert_eq!(
        after,
        vec![
            ("unmined", false, "unproven".to_string()),
            ("unsent", false, "sending".to_string()),
            ("unsent", true, "unproven".to_string()),
        ]
    );
}

/// The sync arguments and an empty chunk for `identity`, from another store.
fn chunk_for(identity: &str) -> (RequestSyncChunkArgs, SyncChunk) {
    let args = RequestSyncChunkArgs {
        from_storage_identity_key: "elsewhere".into(),
        to_storage_identity_key: "here".into(),
        identity_key: identity.to_string(),
        since: None,
        max_rough_size: 100_000,
        max_items: 100,
        offsets: vec![],
    };
    let chunk = SyncChunk {
        from_storage_identity_key: "elsewhere".into(),
        to_storage_identity_key: "here".into(),
        user_identity_key: identity.to_string(),
        ..Default::default()
    };
    (args, chunk)
}

/// Witness 7. On a shared store a sync chunk for user A carries a
/// `proven_txs` row for T, no root check recorded; user B internalizes T
/// with no proof: held, and B's own `review_status` leaves it held. RED at
/// the base (the door read any `proven_txs` row).
#[tokio::test]
async fn a_proof_row_a_sync_chunk_wrote_does_not_make_a_receipt_taken() {
    let root_b = PrivateKey::random();
    let (beef, output, txid) = paying(&root_b, &PrivateKey::random().public_key(), "cHJvb2Y=");
    let wallet_b = common::wallet_posting("taken-record", &root_b, no_answer()).await;
    let user_a = PrivateKey::random().public_key().to_hex();
    let (sync_args, mut chunk) = chunk_for(&user_a);
    let now = chrono::Utc::now();
    chunk.proven_txs = Some(vec![TableProvenTx {
        proven_tx_id: 3,
        txid: txid.clone(),
        height: 900_002,
        index: 0,
        block_hash: "ab".repeat(32),
        merkle_root: "cd".repeat(32),
        merkle_path: vec![1, 2, 3],
        raw_tx: vec![1],
        created_at: now,
        updated_at: now,
    }]);
    wallet_b
        .storage()
        .process_sync_chunk(sync_args, chunk)
        .await
        .expect("the chunk");
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM proven_txs WHERE txid = ?")
        .bind(&txid)
        .fetch_one(wallet_b.storage().pool())
        .await
        .unwrap();
    assert_eq!(rows, 1, "the chunk wrote the proof row");

    internalize(&wallet_b, args(beef, output)).await;
    assert_held(&wallet_b, &root_b, &txid, "B beside a synced proof row").await;

    // The request's word as a chunk could also write it: B's own heal
    // still finds no evidence.
    sql(
        &wallet_b,
        "UPDATE proven_tx_reqs SET status = 'unmined' WHERE txid = ?",
        &txid,
    )
    .await;
    let auth = AuthId::with_user_id(
        root_b.public_key().to_hex(),
        user_id(&wallet_b, &root_b).await,
    );
    let review = wallet_b
        .storage()
        .review_status(&auth, chrono::Utc::now())
        .await
        .expect("review_status");
    println!("review: {}", review.log.trim());
    assert_held(&wallet_b, &root_b, &txid, "B after its own review_status").await;
}
