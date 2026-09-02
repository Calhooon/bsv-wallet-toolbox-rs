//! The caller-complete funding rule, end to end through `Wallet::create_action`:
//! real SQLite storage, the real signer, a mock broadcaster.
//!
//! The shape under test is the Zanaadu V3 publish: ONE covenant input the
//! frontend signed `SIGHASH_SINGLE|ANYONECANPAY|FORKID` (0xc3), 1 sat in,
//! 1,001 sats out (the next covenant state + a 1,000-sat platform fee). The
//! wallet must ADD a funding input and a change output, exactly as MetaNet's TS
//! toolbox does, and must not touch the caller's input or its outputs. The
//! b23a960 verbatim path (fully SIGHASH_ALL, inputs cover outputs) must keep
//! working unchanged.
//!
//! All keys are random throwaway keys; nothing touches the network.

#![cfg(feature = "sqlite")]

use bsv_rs::primitives::{hash160, PrivateKey};
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{
    Counterparty, CreateActionArgs, CreateActionInput, CreateActionOutput, KeyDeriverApi, Outpoint,
    ProtoWallet, Protocol, SecurityLevel, SendWithResultStatus, WalletInterface,
};
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::WalletStorageWriter;
use bsv_wallet_toolbox_rs::{PostBeefResult, PostTxResultForTxid, StorageSqlx, Wallet};

/// BRC-29 protocol name the signer derives P2PKH funding keys with.
const BRC29_PROTOCOL: &str = "3241645161d8";
const PREFIX: &str = "dGVzdC1wcmVmaXg=";
const SUFFIX: &str = "dGVzdC1zdWZmaXg=";
/// A stand-in covenant locking script (OP_1).
const COVENANT_LOCK: &[u8] = &[0x51];
const FUNDING_SATS: u64 = 100_000;

/// A structurally valid `<DER sig><sighash>` push with 32-byte r and s.
fn fake_der_signature(sighash: u8) -> Vec<u8> {
    let mut sig = vec![0x30, 0x44, 0x02, 0x20];
    sig.extend(std::iter::repeat_n(0x11u8, 32));
    sig.extend([0x02, 0x20]);
    sig.extend(std::iter::repeat_n(0x22u8, 32));
    sig.push(sighash);
    sig
}

/// `<sig> <pubkey>` unlocking script bytes signed with `sighash`.
fn unlock_with_sighash(sighash: u8) -> Vec<u8> {
    let sig = fake_der_signature(sighash);
    let mut script = vec![sig.len() as u8];
    script.extend(sig);
    script.push(33);
    script.extend(std::iter::repeat_n(0x02u8, 33));
    script
}

fn p2pkh_lock(pubkey: &[u8]) -> Vec<u8> {
    let mut script = vec![0x76, 0xa9, 0x14];
    script.extend_from_slice(&hash160(pubkey));
    script.extend([0x88, 0xac]);
    script
}

/// The fee/payment output script (an unrelated P2PKH).
fn fee_lock() -> Vec<u8> {
    hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap()
}

/// The P2PKH script the wallet's signer will be able to unlock for
/// (PREFIX, SUFFIX, counterparty self) — derived exactly as `signer.rs` does.
fn wallet_funding_lock(root: &PrivateKey) -> Vec<u8> {
    let proto = ProtoWallet::new(Some(root.clone()));
    let protocol = Protocol::new(SecurityLevel::Counterparty, BRC29_PROTOCOL);
    let key_id = format!("{} {}", PREFIX, SUFFIX);
    let sk = proto
        .key_deriver()
        .derive_private_key(&protocol, &key_id, &Counterparty::Self_)
        .expect("derive funding key");
    p2pkh_lock(&sk.public_key().to_compressed())
}

struct Seeded {
    storage: StorageSqlx,
    root: PrivateKey,
    /// Parent tx: vout 0 = covenant UTXO (`covenant_sats`), vout 1 = wallet
    /// funding UTXO (`FUNDING_SATS`).
    parent_txid: String,
}

/// A wallet whose storage holds one parent transaction with a covenant output
/// (vout 0) and a P2PKH change output the wallet can sign for (vout 1).
async fn seed(covenant_sats: u64) -> Seeded {
    let storage = StorageSqlx::in_memory().await.expect("in-memory storage");
    let root = PrivateKey::random();
    let identity = root.public_key().to_hex();
    storage
        .migrate("caller-complete-tests", &identity)
        .await
        .expect("migrate");
    storage.make_available().await.expect("make_available");
    let (user, _) = storage.find_or_insert_user(&identity).await.expect("user");
    let basket = storage
        .find_or_create_default_basket(user.user_id)
        .await
        .expect("basket");

    let funding_lock = wallet_funding_lock(&root);
    let parent = Transaction::with_params(
        1,
        vec![TransactionInput {
            source_transaction: None,
            source_txid: Some("00".repeat(32)),
            source_output_index: 0xffff_ffff,
            unlocking_script: Some(UnlockingScript::from_hex("00").unwrap()),
            unlocking_script_template: None,
            sequence: 0xffff_ffff,
        }],
        vec![
            TransactionOutput {
                satoshis: Some(covenant_sats),
                locking_script: LockingScript::from_binary(COVENANT_LOCK).unwrap(),
                change: false,
            },
            TransactionOutput {
                satoshis: Some(FUNDING_SATS),
                locking_script: LockingScript::from_binary(&funding_lock).unwrap(),
                change: false,
            },
        ],
        0,
    );
    let parent_txid = parent.id();
    let raw = parent.to_binary();
    let now = chrono::Utc::now();

    let tx_row = sqlx::query(
        r#"
        INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at)
        VALUES (?, 'completed', 'parent-ref', 0, ?, 1, 0, 'seeded parent', ?, ?, ?, ?)
        "#,
    )
    .bind(user.user_id)
    .bind((covenant_sats + FUNDING_SATS) as i64)
    .bind(&parent_txid)
    .bind(&raw)
    .bind(now)
    .bind(now)
    .execute(storage.pool())
    .await
    .expect("insert parent tx");
    let parent_id = tx_row.last_insert_rowid();

    sqlx::query(
        r#"
        INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, locking_script,
                             txid, type, spendable, change, provided_by, purpose,
                             output_description, created_at, updated_at)
        VALUES (?, ?, NULL, 0, ?, ?, ?, 'custom', 1, 0, 'you', '', 'covenant utxo', ?, ?)
        "#,
    )
    .bind(user.user_id)
    .bind(parent_id)
    .bind(covenant_sats as i64)
    .bind(COVENANT_LOCK)
    .bind(&parent_txid)
    .bind(now)
    .bind(now)
    .execute(storage.pool())
    .await
    .expect("insert covenant output");

    sqlx::query(
        r#"
        INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, locking_script,
                             txid, type, spendable, change, derivation_prefix, derivation_suffix,
                             provided_by, purpose, output_description, created_at, updated_at)
        VALUES (?, ?, ?, 1, ?, ?, ?, 'P2PKH', 1, 1, ?, ?, 'storage', 'change', 'wallet funding', ?, ?)
        "#,
    )
    .bind(user.user_id)
    .bind(parent_id)
    .bind(basket.basket_id)
    .bind(FUNDING_SATS as i64)
    .bind(&funding_lock)
    .bind(&parent_txid)
    .bind(PREFIX)
    .bind(SUFFIX)
    .bind(now)
    .bind(now)
    .execute(storage.pool())
    .await
    .expect("insert funding output");

    Seeded {
        storage,
        root,
        parent_txid,
    }
}

fn covenant_input(parent_txid: &str, unlocking_script: Vec<u8>) -> CreateActionInput {
    CreateActionInput {
        outpoint: Outpoint::from_string(&format!("{}.0", parent_txid)).unwrap(),
        input_description: "covenant input".to_string(),
        unlocking_script: Some(unlocking_script),
        unlocking_script_length: None,
        sequence_number: Some(0xffff_ffff),
    }
}

fn output(locking_script: Vec<u8>, satoshis: u64, description: &str) -> CreateActionOutput {
    CreateActionOutput {
        locking_script,
        satoshis,
        output_description: description.to_string(),
        basket: None,
        custom_instructions: None,
        tags: None,
    }
}

/// The V3 publish args: 1 covenant input (0xc3), outputs = next state (1 sat)
/// + platform fee (1,000 sats).
fn v3_publish_args(parent_txid: &str, unlock: Vec<u8>) -> CreateActionArgs {
    CreateActionArgs {
        description: "zanaadu v3 publish".to_string(),
        input_beef: None,
        inputs: Some(vec![covenant_input(parent_txid, unlock)]),
        outputs: Some(vec![
            output(COVENANT_LOCK.to_vec(), 1, "next covenant state"),
            output(fee_lock(), 1_000, "platform fee"),
        ]),
        lock_time: None,
        version: None,
        labels: None,
        options: None,
    }
}

async fn tx_status(storage: &StorageSqlx, txid: &str) -> String {
    sqlx::query_scalar("SELECT status FROM transactions WHERE txid = ?")
        .bind(txid)
        .fetch_one(storage.pool())
        .await
        .expect("tx status")
}

#[tokio::test]
async fn anyonecanpay_shortfall_is_funded_and_the_callers_bytes_are_untouched() {
    let seeded = seed(1).await;
    let mock = MockWalletServices::new(); // post_beef → success
    let unlock = unlock_with_sighash(0xc3);
    let wallet = Wallet::new(Some(seeded.root.clone()), seeded.storage, mock)
        .await
        .expect("wallet");

    let result = wallet
        .create_action(
            v3_publish_args(&seeded.parent_txid, unlock.clone()),
            "test.local",
        )
        .await
        .expect("an ANYONECANPAY shortfall must be funded");

    let signed = Transaction::from_binary(result.tx.as_ref().expect("signed tx")).unwrap();
    assert_eq!(signed.inputs.len(), 2, "one funding input appended");
    // Change is appended after the caller's outputs. `generate_change` may split
    // it into several outputs to replenish the change-UTXO pool (the same
    // behaviour as any wallet-funded createAction, and as the TS toolbox).
    assert!(
        signed.outputs.len() >= 3,
        "change must be appended, got {} outputs",
        signed.outputs.len()
    );
    for change in &signed.outputs[2..] {
        let script = change.locking_script.to_binary();
        assert_eq!(script.len(), 25, "change is P2PKH");
        assert_eq!(&script[..3], &[0x76, 0xa9, 0x14]);
        assert!(change.satoshis.unwrap() > 0);
    }

    // The caller's input is vin 0, byte for byte: outpoint, script, sequence.
    let vin0 = &signed.inputs[0];
    assert_eq!(
        vin0.source_txid.as_deref(),
        Some(seeded.parent_txid.as_str())
    );
    assert_eq!(vin0.source_output_index, 0);
    assert_eq!(vin0.sequence, 0xffff_ffff);
    assert_eq!(
        vin0.unlocking_script.as_ref().unwrap().to_binary(),
        unlock,
        "the caller's unlocking script must be broadcast verbatim"
    );

    // The caller's outputs are vout 0 and 1, byte for byte.
    assert_eq!(signed.outputs[0].satoshis, Some(1));
    assert_eq!(signed.outputs[0].locking_script.to_binary(), COVENANT_LOCK);
    assert_eq!(signed.outputs[1].satoshis, Some(1_000));
    assert_eq!(signed.outputs[1].locking_script.to_binary(), fee_lock());

    // The wallet's funding input is vin 1 (the seeded change UTXO) and carries a
    // real P2PKH unlock the signer produced.
    let vin1 = &signed.inputs[1];
    assert_eq!(
        vin1.source_txid.as_deref(),
        Some(seeded.parent_txid.as_str())
    );
    assert_eq!(vin1.source_output_index, 1);
    let funding_unlock = vin1.unlocking_script.as_ref().unwrap().to_binary();
    assert!(
        (100..=110).contains(&funding_unlock.len()),
        "P2PKH <sig> <pubkey> expected, got {} bytes",
        funding_unlock.len()
    );
    assert_ne!(funding_unlock, unlock);

    // Change comes back to the wallet; the whole thing balances with a sane fee.
    let change: u64 = signed.outputs[2..]
        .iter()
        .map(|o| o.satoshis.unwrap())
        .sum();
    assert!(change > 0 && change < FUNDING_SATS);
    let total_out: u64 = signed.outputs.iter().map(|o| o.satoshis.unwrap()).sum();
    let total_in = FUNDING_SATS + 1;
    assert!(total_in > total_out);
    assert!(total_in - total_out < 500, "fee {}", total_in - total_out);

    // The broadcaster ACCEPTED it: sendWithResults says so, storage says so.
    let swr = result.send_with_results.expect("sendWithResults");
    assert_eq!(swr.len(), 1);
    assert!(
        matches!(swr[0].status, SendWithResultStatus::Unproven),
        "an accepted broadcast reports 'unproven', got {:?}",
        swr[0].status
    );
    let txid = hex::encode(result.txid.unwrap());
    assert_eq!(swr[0].txid, result.txid.unwrap());
    assert_eq!(tx_status(wallet.storage(), &txid).await, "unproven");
    assert_eq!(wallet.services().call_count("post_beef"), 1);
}

#[tokio::test]
async fn sighash_all_shortfall_is_insufficient_funds_at_the_wallet() {
    let seeded = seed(1).await;
    let wallet = Wallet::new(
        Some(seeded.root.clone()),
        seeded.storage,
        MockWalletServices::new(),
    )
    .await
    .expect("wallet");

    let err = wallet
        .create_action(
            v3_publish_args(&seeded.parent_txid, unlock_with_sighash(0x41)),
            "test.local",
        )
        .await
        .expect_err("SIGHASH_ALL forbids funding");
    assert!(
        err.to_string().contains("Insufficient funds"),
        "got: {}",
        err
    );
    assert_eq!(
        wallet.services().call_count("post_beef"),
        0,
        "nothing may be broadcast"
    );
}

#[tokio::test]
async fn sighash_all_self_sufficient_tx_is_broadcast_verbatim() {
    // b23a960: a caller-complete tx whose inputs cover its outputs goes out with
    // exactly the caller's inputs and outputs, even though change is available.
    let seeded = seed(5_000).await;
    let unlock = unlock_with_sighash(0x41);
    let wallet = Wallet::new(
        Some(seeded.root.clone()),
        seeded.storage,
        MockWalletServices::new(),
    )
    .await
    .expect("wallet");

    let args = CreateActionArgs {
        description: "covenant settle".to_string(),
        input_beef: None,
        inputs: Some(vec![covenant_input(&seeded.parent_txid, unlock.clone())]),
        outputs: Some(vec![
            output(fee_lock(), 2_000, "party a"),
            output(fee_lock(), 2_500, "party b"),
        ]),
        lock_time: None,
        version: None,
        labels: None,
        options: None,
    };
    let result = wallet
        .create_action(args, "test.local")
        .await
        .expect("verbatim broadcast");
    let signed = Transaction::from_binary(result.tx.as_ref().unwrap()).unwrap();
    assert_eq!(signed.inputs.len(), 1);
    assert_eq!(signed.outputs.len(), 2);
    assert_eq!(
        signed.inputs[0]
            .unlocking_script
            .as_ref()
            .unwrap()
            .to_binary(),
        unlock
    );
    assert_eq!(signed.outputs[0].satoshis, Some(2_000));
    assert_eq!(signed.outputs[1].satoshis, Some(2_500));
    assert_eq!(wallet.services().call_count("post_beef"), 1);
}

#[tokio::test]
async fn transient_broadcast_fault_leaves_the_result_sending() {
    let seeded = seed(1).await;
    let mock = MockWalletServices::builder()
        .post_beef_service_unavailable()
        .build();
    let wallet = Wallet::new(Some(seeded.root.clone()), seeded.storage, mock)
        .await
        .expect("wallet");

    let result = wallet
        .create_action(
            v3_publish_args(&seeded.parent_txid, unlock_with_sighash(0xc3)),
            "test.local",
        )
        .await
        .expect("a transient fault is not an error");
    let swr = result.send_with_results.expect("sendWithResults");
    assert!(
        matches!(swr[0].status, SendWithResultStatus::Sending),
        "ambiguous broadcast stays 'sending', got {:?}",
        swr[0].status
    );
    let txid = hex::encode(result.txid.unwrap());
    assert_eq!(tx_status(wallet.storage(), &txid).await, "sending");
}

#[tokio::test]
async fn definitive_rejection_fails_the_action_and_releases_its_inputs() {
    // ARC 465 fee-too-low on the immediate broadcast: create_action must return
    // an error, mark the tx failed and give the inputs back — never a phantom
    // txid with the inputs locked behind a retry loop.
    let seeded = seed(1).await;
    let mock = MockWalletServices::builder()
        .post_beef_response(MockResponse::Success(vec![PostBeefResult {
            name: "arc".to_string(),
            status: "error".to_string(),
            txid_results: vec![PostTxResultForTxid {
                txid: "ab".repeat(32),
                status: "465".to_string(),
                double_spend: false,
                orphan_mempool: false,
                competing_txs: None,
                data: Some("ARC rejected transaction: fee too low (HTTP 465)".to_string()),
                service_error: false,
                block_hash: None,
                block_height: None,
                notes: vec![],
            }],
            error: None,
            notes: vec![],
        }]))
        .build();
    let wallet = Wallet::new(Some(seeded.root.clone()), seeded.storage, mock)
        .await
        .expect("wallet");

    let err = wallet
        .create_action(
            v3_publish_args(&seeded.parent_txid, unlock_with_sighash(0xc3)),
            "test.local",
        )
        .await
        .expect_err("a definitive rejection is an error, not a phantom txid");
    let msg = err.to_string();
    assert!(msg.contains("Transaction broadcast failed"), "{}", msg);
    assert!(msg.contains("465"), "{}", msg);

    let storage = wallet.storage();
    let failed: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM transactions WHERE status = 'failed'")
            .fetch_one(storage.pool())
            .await
            .unwrap();
    assert_eq!(failed, 1, "the rejected tx is marked failed");
    let (sending, unproven): (i64, i64) = sqlx::query_as(
        "SELECT SUM(status = 'sending'), SUM(status = 'unproven') FROM transactions",
    )
    .fetch_one(storage.pool())
    .await
    .unwrap();
    assert_eq!((sending, unproven), (0, 0), "nothing is left in flight");

    // Both inputs (covenant + funding) are back: spendable, not spent_by anyone.
    let rows: Vec<(i64, i64, Option<i64>)> = sqlx::query_as(
        "SELECT vout, spendable, spent_by FROM outputs WHERE txid = ? ORDER BY vout",
    )
    .bind(&seeded.parent_txid)
    .fetch_all(storage.pool())
    .await
    .unwrap();
    assert_eq!(rows, vec![(0, 1, None), (1, 1, None)]);

    // The failed tx's own outputs (the change) can never fund anything.
    let phantom_spendable: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outputs o JOIN transactions t ON t.transaction_id = o.transaction_id \
         WHERE t.status = 'failed' AND o.spendable = 1",
    )
    .fetch_one(storage.pool())
    .await
    .unwrap();
    assert_eq!(phantom_spendable, 0);
    let req_status: String = sqlx::query_scalar(
        "SELECT status FROM proven_tx_reqs ORDER BY proven_tx_req_id DESC LIMIT 1",
    )
    .fetch_one(storage.pool())
    .await
    .unwrap();
    assert_eq!(req_status, "invalid");
}
