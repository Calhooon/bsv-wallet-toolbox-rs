//! Helpers shared by the internalize payment integration tests: a wallet
//! over in-memory SQLite whose mock broadcaster accepts, the count of stored
//! outputs, the P2PKH of a key, the BRC-29 key a recipient derives, a
//! wallet payment's arguments, and atomic BEEFs paying one output: a single
//! spend of one proven parent, and a sweep of one parent past bsv-rs's
//! default link budget.
//!
//! Each test file uses part of this module, hence `dead_code` allowed.

#![allow(dead_code)]

use bsv_rs::primitives::{hash160, PrivateKey, PublicKey};
use bsv_rs::script::{LockingScript, ProtocolEra, UnlockingScript};
use bsv_rs::transaction::{Beef, MerklePath, Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{
    Counterparty, InternalizeActionArgs, InternalizeOutput, KeyDeriverApi, ProtoWallet, Protocol,
    SecurityLevel, WalletPayment,
};
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::{
    PostBeefResult, PostTxResultForTxid, StorageSqlx, Wallet, WalletStorageWriter,
};

/// Outputs of the one parent the sweep spends: past the reach of bsv-rs's
/// default link budget (16 MiB of wire and 56 MiB of heap re-materialized)
/// for this parent's size.
pub const SWEPT: u32 = 800;

/// A 25-byte P2PKH paying neither party of the tests.
pub const OTHER_HASH: &str = "dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa";

/// The reference's sentence for a payment output not locked by the derived
/// key's P2PKH.
pub const NOT_BRC29: &str =
    "The paymentRemittance parameter must be locked by script conforming to BRC-29";

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

/// A wallet payment of output 0 with the derivation `prefix` and `suffix`
/// from `sender`.
pub fn payment(sender: &PublicKey, prefix: &str, suffix: &str) -> InternalizeOutput {
    InternalizeOutput {
        output_index: 0,
        protocol: "wallet payment".to_string(),
        payment_remittance: Some(WalletPayment {
            derivation_prefix: prefix.to_string(),
            derivation_suffix: suffix.to_string(),
            sender_identity_key: sender.to_hex(),
        }),
        insertion_remittance: None,
    }
}

/// The arguments internalizing `output` of the atomic BEEF `tx`.
pub fn args(tx: Vec<u8>, output: InternalizeOutput) -> InternalizeActionArgs {
    InternalizeActionArgs {
        tx,
        outputs: vec![output],
        description: "an output to check".to_string(),
        labels: None,
        seek_permission: None,
    }
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
/// broadcaster accepts `txid` and whose mock tracker answers every root.
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

/// The message and class of a `WalletRefusal`, or a panic naming `case`.
pub fn refusal(answer: bsv_rs::Result<bool>, case: &str) -> (bsv_rs::WalletRefusalClass, String) {
    match answer {
        Err(bsv_rs::Error::WalletRefusal { class, message }) => (class, message),
        other => panic!("{case}: not a refusal: {other:?}"),
    }
}

fn input(txid: &str, vout: u32) -> TransactionInput {
    let mut input = TransactionInput::new(txid.to_string(), vout);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    input
}

/// A lock the input `00` spends under the node's word, 24 bytes long so
/// the parent is large: push 20 bytes, drop them, drop the empty push,
/// push one.
fn anyone_lock() -> LockingScript {
    LockingScript::from_hex(&format!("14{}757551", "00".repeat(20))).unwrap()
}

/// A parent of `outputs` outputs of 1000 satoshis each locked by
/// `anyone_lock`, proven by a coinbase BUMP after Genesis.
fn proven_parent(outputs: u32) -> (Transaction, MerklePath) {
    let mut parent = Transaction::new();
    parent.inputs_mut().push(input(&"11".repeat(32), 0));
    for _ in 0..outputs {
        parent.outputs_mut().push(TransactionOutput {
            satoshis: Some(1_000),
            locking_script: anyone_lock(),
            change: false,
        });
    }
    let bump =
        MerklePath::from_coinbase_txid(&parent.id(), ProtocolEra::MAINNET_GENESIS_HEIGHT + 101);
    (parent, bump)
}

/// An atomic BEEF of a proven parent of `spent` outputs and a child
/// spending every one of them into one output locked by `lock`.
pub fn spend_beef(spent: u32, lock: LockingScript) -> (Vec<u8>, String) {
    let (parent, bump) = proven_parent(spent);
    let parent_txid = parent.id();
    let mut child = Transaction::new();
    child.set_version(1);
    for vout in 0..spent {
        child.inputs_mut().push(input(&parent_txid, vout));
    }
    child.outputs_mut().push(TransactionOutput {
        satoshis: Some(u64::from(spent) * 1_000 - 500),
        locking_script: lock,
        change: false,
    });
    let txid = child.id();
    let mut beef = Beef::new();
    let bump = beef.merge_bump(bump);
    beef.merge_raw_tx(parent.to_binary(), Some(bump)).unwrap();
    beef.merge_raw_tx(child.to_binary(), None).unwrap();
    (beef.to_binary_atomic(&txid).unwrap(), txid)
}

/// The sweep: `spend_beef` of `SWEPT` outputs.
pub fn sweep_beef(lock: LockingScript) -> (Vec<u8>, String) {
    spend_beef(SWEPT, lock)
}

/// One output of 4500 satoshis locked by `lock`, spending five outputs of
/// one proven parent.
pub fn beef_paying(lock: LockingScript) -> (Vec<u8>, String) {
    spend_beef(5, lock)
}
