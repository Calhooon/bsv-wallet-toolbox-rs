//! `Wallet::internalize_action`'s BRC-29 check of a payment output reads
//! the BEEF's subject without linking its ancestry.
//!
//! A payment output is admitted only when its script is the P2PKH of the key
//! derived from its remittance. The check reads the subject transaction's
//! own outputs (`Transaction::from_beef_subject`), so it runs whatever the
//! ancestry's size: a sweep of one parent past bsv-rs's default link budget
//! is checked like any other. A BEEF with a payment output and no readable
//! subject is refused before anything is stored.
//!
//! Real SQLite storage, a mock broadcaster and tracker; nothing touches the
//! network.

#![cfg(feature = "sqlite")]

#[path = "common/internalize_payment.rs"]
mod common;

use bsv_rs::primitives::{PrivateKey, PublicKey};
use bsv_rs::script::LockingScript;
use bsv_rs::transaction::Transaction;
use bsv_rs::wallet::{InternalizeActionArgs, WalletInterface};
use bsv_rs::WalletRefusalClass;
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::{StorageSqlx, Wallet};

use common::{beef_paying, other_lock, p2pkh_bytes, refusal, stored_outputs, sweep_beef};

const PREFIX: &str = "c3dlZXAtcHJlZml4";
const SUFFIX: &str = "c3dlZXAtc3VmZml4";

/// The P2PKH of the key the recipient `root` derives for the remittance
/// from `sender`.
fn derived_lock(root: &PrivateKey, sender: &PublicKey) -> LockingScript {
    let key = common::derived_key(root, sender, PREFIX, SUFFIX);
    LockingScript::from_binary(&p2pkh_bytes(&key)).unwrap()
}

async fn wallet(root: &PrivateKey, txid: &str) -> Wallet<StorageSqlx, MockWalletServices> {
    common::wallet("internalize-payment-subject-tests", root, txid).await
}

fn payment_args(tx: Vec<u8>, sender: &PublicKey) -> InternalizeActionArgs {
    common::args(tx, common::payment(sender, PREFIX, SUFFIX))
}

/// Internalizes the sweep paying the derived key or another, and returns
/// the answer and the number of outputs stored after it.
async fn internalize_sweep(lock_derives: bool) -> (bsv_rs::Result<bool>, i64) {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let lock = if lock_derives {
        derived_lock(&root, &sender)
    } else {
        other_lock()
    };
    let (bytes, txid) = sweep_beef(lock);
    let wallet = wallet(&root, &txid).await;
    let answer = wallet
        .internalize_action(payment_args(bytes, &sender), Some("test.local"))
        .await
        .map(|r| r.accepted);
    (answer, stored_outputs(&wallet).await)
}

/// The sweep is past the linking door's budget, and the payment's key does
/// not derive from its remittance: refused for the derivation.
#[tokio::test]
async fn a_sweep_past_the_link_budget_with_a_payment_key_that_does_not_derive_is_refused() {
    let (beef, txid) = sweep_beef(other_lock());
    let linked = Transaction::from_beef(&beef, Some(&txid));
    assert!(linked.is_err(), "the linking door refuses the sweep");
    let (answer, stored) = internalize_sweep(false).await;
    let (class, text) = refusal(answer, "the sweep");
    assert_eq!(class, WalletRefusalClass::InvalidParameter);
    assert!(
        text.starts_with("Derivation mismatch for output 0: derived "),
        "{text}"
    );
    assert!(text.ends_with(&format!(" != script {}", common::OTHER_HASH)));
    assert_eq!(stored, 0, "nothing is stored");
}

/// The same sweep paying the derived key: admitted, its output stored.
#[tokio::test]
async fn a_sweep_past_the_link_budget_with_a_payment_key_that_derives_is_stored() {
    let (answer, stored) = internalize_sweep(true).await;
    assert!(answer.expect("the payment is internalized"));
    assert_eq!(stored, 1, "the payment output is stored");
}

/// Bytes with no readable subject and a payment output: refused before
/// anything is stored, saying the subject cannot be read. The whole text a
/// host reads is pinned for each input: bytes that are not a BEEF, and an
/// atomic BEEF naming a subject it does not carry.
#[tokio::test]
async fn a_payment_in_bytes_with_no_readable_subject_is_refused_before_storage() {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let (good, _) = beef_paying(derived_lock(&root, &sender));
    // An AtomicBEEF naming a subject it does not carry.
    let mut absent = good.clone();
    absent[4..36].copy_from_slice(&[0x5a; 32]);
    let cases = [
        (
            vec![0xde, 0xad, 0xbe, 0xef],
            "Cannot read the BEEF's subject transaction to check payment outputs: \
             BEEF error: invalid BEEF at byte 0: BadVersion { word: 4022250974 }"
                .to_string(),
        ),
        (
            absent,
            format!(
                "Cannot read the BEEF's subject transaction to check payment outputs: \
                 transaction error: Transaction {} not found in BEEF",
                "5a".repeat(32)
            ),
        ),
    ];
    for (bytes, expected) in cases {
        let wallet = wallet(&root, &"00".repeat(32)).await;
        let answer = wallet
            .internalize_action(payment_args(bytes, &sender), Some("test.local"))
            .await;
        let printed = answer.as_ref().map(|_| ()).map_err(ToString::to_string);
        let (class, text) = refusal(answer.map(|r| r.accepted), &expected);
        assert_eq!(class, WalletRefusalClass::InvalidParameter);
        assert_eq!(text, expected);
        assert_eq!(printed, Err(expected), "the carrier prints the message");
        assert_eq!(stored_outputs(&wallet).await, 0, "nothing is stored");
    }
}
