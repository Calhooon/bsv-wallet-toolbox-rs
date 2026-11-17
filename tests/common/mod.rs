//! Helpers shared by the internalize and BEEF link budget integration tests:
//! a broadcaster's acceptance, a wallet over in-memory SQLite, the count of
//! stored outputs, the P2PKH of a key, the BRC-29 key a recipient derives,
//! and a sweep of one parent past bsv-rs 0.8.11's default link budget.
//!
//! Each test file uses part of this module, hence `dead_code` allowed.

#![allow(dead_code)]

use bsv_rs::primitives::{hash160, PrivateKey, PublicKey};
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{Beef, MerklePath, Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{Counterparty, KeyDeriverApi, ProtoWallet, Protocol, SecurityLevel};
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::{
    PostBeefResult, PostTxResultForTxid, StorageSqlx, Wallet, WalletStorageWriter,
};

/// Outputs of the one parent the sweep spends: past the 702 P2PKH outputs
/// bsv-rs 0.8.11 states as the default link budget's single-sweep reach.
pub const SWEPT: u32 = 800;

/// A 25-byte P2PKH paying neither party of the tests.
pub const OTHER_HASH: &str = "dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa";

/// The P2PKH of `OTHER_HASH`.
pub fn other_lock() -> LockingScript {
    LockingScript::from_hex(&format!("76a914{OTHER_HASH}88ac")).unwrap()
}

/// The 25 bytes of the P2PKH of `pubkey`.
pub fn p2pkh_bytes(pubkey: &PublicKey) -> Vec<u8> {
    let mut script = vec![0x76, 0xa9, 0x14];
    script.extend_from_slice(&hash160(&pubkey.to_compressed()));
    script.extend([0x88, 0xac]);
    script
}

/// The key the recipient `root` derives for the remittance from `sender`
/// with the derivation `prefix` and `suffix`.
pub fn derived_key(root: &PrivateKey, sender: &PublicKey, prefix: &str, suffix: &str) -> PublicKey {
    ProtoWallet::new(Some(root.clone()))
        .key_deriver()
        .derive_public_key(
            &Protocol::new(SecurityLevel::Counterparty, "3241645161d8"),
            &format!("{prefix} {suffix}"),
            &Counterparty::Other(sender.clone()),
            true,
        )
        .unwrap()
}

/// A broadcaster's acceptance of `txid`.
pub fn accepted(txid: &str) -> PostBeefResult {
    PostBeefResult {
        name: "arc".to_string(),
        status: "success".to_string(),
        txid_results: vec![PostTxResultForTxid {
            txid: txid.to_string(),
            status: "success".to_string(),
            double_spend: false,
            orphan_mempool: false,
            competing_txs: None,
            data: None,
            service_error: false,
            block_hash: None,
            block_height: None,
            notes: vec![],
        }],
        error: None,
        notes: vec![],
    }
}

/// A wallet of `root` over in-memory SQLite named `name`, whose mock
/// broadcaster accepts `txid`.
pub async fn wallet(
    name: &str,
    root: &PrivateKey,
    txid: &str,
) -> Wallet<StorageSqlx, MockWalletServices> {
    let storage = StorageSqlx::in_memory().await.expect("in-memory storage");
    let identity = root.public_key().to_hex();
    storage.migrate(name, &identity).await.expect("migrate");
    storage.make_available().await.expect("make_available");
    let services = MockWalletServices::builder()
        .post_beef_response(MockResponse::Success(vec![accepted(txid)]))
        .build();
    Wallet::new(Some(root.clone()), storage, services)
        .await
        .expect("wallet")
}

/// The number of outputs the wallet's store holds.
pub async fn stored_outputs(wallet: &Wallet<StorageSqlx, MockWalletServices>) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM outputs")
        .fetch_one(wallet.storage().writer())
        .await
        .unwrap()
}

fn input(txid: &str, vout: u32) -> TransactionInput {
    let mut input = TransactionInput::new(txid.to_string(), vout);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    input
}

/// A proven parent of `outputs` outputs locked by `other_lock`.
pub fn proven_parent(outputs: u32) -> Transaction {
    let mut parent = Transaction::new();
    parent.inputs.push(input(&"11".repeat(32), 0));
    for _ in 0..outputs {
        parent.outputs.push(TransactionOutput {
            satoshis: Some(1_000),
            locking_script: other_lock(),
            change: false,
        });
    }
    parent
}

/// A child spending outputs 0 to `spent - 1` of `parent_txid` into one
/// output locked by `lock`.
pub fn child(parent_txid: &str, spent: u32, lock: LockingScript) -> Transaction {
    let mut child = Transaction::new();
    child.version = 1;
    for vout in 0..spent {
        child.inputs.push(input(parent_txid, vout));
    }
    child.outputs.push(TransactionOutput {
        satoshis: Some(u64::from(spent) * 1_000 - 500),
        locking_script: lock,
        change: false,
    });
    child
}

/// An AtomicBEEF of a proven parent of `SWEPT` outputs and a child sweeping
/// every one of them into one output locked by `lock`.
pub fn sweep_beef(lock: LockingScript) -> (Vec<u8>, String) {
    let parent = proven_parent(SWEPT);
    let parent_txid = parent.id();
    let child = child(&parent_txid, SWEPT, lock);
    let txid = child.id();
    let mut beef = Beef::new();
    let bump = beef.merge_bump(MerklePath::from_coinbase_txid(&parent_txid, 100_001));
    beef.merge_raw_tx(parent.to_binary(), Some(bump));
    beef.merge_raw_tx(child.to_binary(), None);
    (beef.to_binary_atomic(&txid).unwrap(), txid)
}
