//! Known ancestor wire witness for ts-stack #660 at 022f1a2.
//! Synthetic SQLite funding and local signing only; no network or real wallet.
#![cfg(feature = "sqlite")]

use bsv_rs::primitives::{hash160, PrivateKey};
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{Beef, Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{
    Counterparty, CreateActionArgs, CreateActionOptions, CreateActionOutput, KeyDeriverApi,
    ProtoWallet, Protocol, SecurityLevel, SignActionArgs, WalletInterface,
};
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::{StorageSqlx, Wallet, WalletStorageWriter};

const BRC29_PROTOCOL: &str = "3241645161d8";
const PREFIX: &str = "dGVzdC1wcmVmaXg=";
const SUFFIX: &str = "dGVzdC1zdWZmaXg=";
const FUNDING_SATS: u64 = 100_000;

fn p2pkh_lock(pubkey: &[u8]) -> Vec<u8> {
    let mut script = vec![0x76, 0xa9, 0x14];
    script.extend_from_slice(&hash160(pubkey));
    script.extend([0x88, 0xac]);
    script
}

fn pay_lock() -> Vec<u8> {
    hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap()
}

/// A storage holding one completed parent with one P2PKH change output the
/// wallet can sign for.
async fn seed() -> (StorageSqlx, PrivateKey, String) {
    let storage = StorageSqlx::in_memory().await.expect("in-memory storage");
    let root = PrivateKey::random();
    let identity = root.public_key().to_hex();
    storage
        .migrate("known-ancestor-tests", &identity)
        .await
        .expect("migrate");
    storage.make_available().await.expect("make_available");
    let (user, _) = storage.find_or_insert_user(&identity).await.expect("user");
    let basket = storage
        .find_or_create_default_basket(user.user_id)
        .await
        .expect("basket");

    let proto = ProtoWallet::new(Some(root.clone()));
    let sk = proto
        .key_deriver()
        .derive_private_key(
            &Protocol::new(SecurityLevel::Counterparty, BRC29_PROTOCOL),
            &format!("{} {}", PREFIX, SUFFIX),
            &Counterparty::Self_,
        )
        .expect("derive funding key");
    let funding_lock = p2pkh_lock(&sk.public_key().to_compressed());
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
        vec![TransactionOutput {
            satoshis: Some(FUNDING_SATS),
            locking_script: LockingScript::from_binary(&funding_lock).unwrap(),
            change: false,
        }],
        0,
    );
    let parent_txid = parent.id();
    let now = chrono::Utc::now();
    let bump = bsv_rs::transaction::MerklePath::from_coinbase_txid(&parent_txid, 100_000);
    sqlx::query("INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx, created_at, updated_at) VALUES (?, 100000, 0, ?, ?, ?, ?, ?, ?)")
        .bind(&parent_txid).bind("aa".repeat(32)).bind(&parent_txid).bind(bump.to_binary())
        .bind(parent.to_binary()).bind(now).bind(now).execute(storage.pool()).await.expect("proven funding parent");

    let parent_id = sqlx::query(
        "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at) \
         VALUES (?, 'completed', 'parent-ref', 0, ?, 1, 0, 'seeded parent', ?, ?, ?, ?)",
    )
    .bind(user.user_id)
    .bind(FUNDING_SATS as i64)
    .bind(&parent_txid)
    .bind(parent.to_binary())
    .bind(now)
    .bind(now)
    .execute(storage.pool())
    .await
    .expect("insert parent")
    .last_insert_rowid();
    sqlx::query(
        "INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, locking_script, \
                              txid, type, spendable, change, derivation_prefix, derivation_suffix, \
                              provided_by, purpose, output_description, created_at, updated_at) \
         VALUES (?, ?, ?, 0, ?, ?, ?, 'P2PKH', 1, 1, ?, ?, 'storage', 'change', 'wallet funding', ?, ?)",
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
    (storage, root, parent_txid)
}

fn pay_args(sign_and_process: bool) -> CreateActionArgs {
    CreateActionArgs {
        description: "pay someone".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script: pay_lock(),
            satoshis: 1_000,
            output_description: "payment".to_string(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: None,
        options: Some(CreateActionOptions {
            sign_and_process: Some(sign_and_process),
            accept_delayed_broadcast: Some(false),
            ..Default::default()
        }),
    }
}

fn assert_ancestor(bytes: &[u8], ancestor: &str, subject: &str, path: &str, known: bool) {
    let beef = Beef::from_binary(bytes).expect("result BEEF parses");
    let entry = beef.txs.iter().find(|tx| tx.txid() == ancestor);
    let shape = match entry {
        None => "omitted",
        Some(tx) if tx.is_txid_only() => "txid-only",
        Some(_) => "whole",
    };
    println!("{path}: known={known}, ancestor={shape}");
    assert_eq!(shape, if known { "txid-only" } else { "whole" });
    assert!(beef
        .txs
        .iter()
        .any(|tx| tx.txid() == subject && !tx.is_txid_only()));
}

async fn witness(deferred: bool, known: bool) {
    let (storage, root, ancestor) = seed().await;
    let wallet = Wallet::new(Some(root), storage, MockWalletServices::new())
        .await
        .expect("wallet");
    let mut args = pay_args(!deferred);
    let options = args.options.as_mut().unwrap();
    options.no_send = Some(true);
    if known {
        options.known_txids = Some(vec![hex::decode(&ancestor).unwrap().try_into().unwrap()]);
    }
    let result = wallet
        .create_action(args, "test.local")
        .await
        .expect("createAction");
    if deferred {
        let signable = result.signable_transaction.expect("signable result");
        let preview = Beef::from_binary(&signable.tx).expect("preview BEEF");
        assert_ancestor(
            &signable.tx,
            &ancestor,
            preview.atomic_txid.as_deref().unwrap(),
            "createAction.signableTransaction.tx",
            known,
        );
        let signed = wallet
            .sign_action(
                SignActionArgs {
                    reference: String::from_utf8(signable.reference).expect("reference"),
                    spends: Default::default(),
                    options: None,
                },
                "test.local",
            )
            .await
            .expect("signAction");
        assert_ancestor(
            signed.tx.as_deref().unwrap(),
            &ancestor,
            &hex::encode(signed.txid.unwrap()),
            "signAction.tx",
            known,
        );
    } else {
        // This port exposes the inline result BEEF in `beef`; `tx` is raw bytes.
        assert_ancestor(
            result.beef.as_deref().expect("inline result BEEF"),
            &ancestor,
            &hex::encode(result.txid.unwrap()),
            "createAction.beef",
            known,
        );
    }
    let retained: Vec<u8> = sqlx::query_scalar("SELECT raw_tx FROM transactions WHERE txid = ?")
        .bind(&ancestor)
        .fetch_one(wallet.storage().pool())
        .await
        .unwrap();
    assert_eq!(Transaction::from_binary(&retained).unwrap().id(), ancestor);
    assert_eq!(wallet.services().call_count("post_beef"), 0);
}

#[tokio::test]
async fn create_action_known_ancestor() {
    witness(false, true).await;
}
#[tokio::test]
async fn sign_action_known_ancestor() {
    witness(true, true).await;
}
#[tokio::test]
async fn create_action_undeclared_ancestor_control() {
    witness(false, false).await;
}
#[tokio::test]
async fn sign_action_undeclared_ancestor_control() {
    witness(true, false).await;
}
