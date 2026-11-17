//! `Wallet::internalize_action` admits a wallet payment output only when its
//! locking script is the P2PKH of the key its remittance derives, byte for
//! byte, whatever the script's shape (the reference's `internalizeAction`,
//! `setupWalletPaymentForOutput`).
//!
//! A 25-byte P2PKH paying another key is refused with the derivation
//! mismatch text naming both hashes; any other script is refused with
//! "The paymentRemittance parameter must be locked by script conforming to
//! BRC-29". Nothing is stored for a refused request. A basket insertion is
//! not held to the rule.
//!
//! Real SQLite storage, a mock broadcaster; nothing touches the network.

#![cfg(feature = "sqlite")]

mod common;

use bsv_rs::primitives::{hash160, PrivateKey, PublicKey};
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{Beef, MerklePath, Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{
    BasketInsertion, InternalizeActionArgs, InternalizeOutput, WalletInterface, WalletPayment,
};
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::{StorageSqlx, Wallet};

use common::{p2pkh_bytes, stored_outputs, OTHER_HASH};

const PREFIX: &str = "bG9jay1wcmVmaXg=";
const SUFFIX: &str = "bG9jay1zdWZmaXg=";
const NOT_BRC29: &str =
    "The paymentRemittance parameter must be locked by script conforming to BRC-29";
/// Builds an output's locking script from the derived key.
type ScriptFor = fn(&PublicKey) -> Vec<u8>;

fn lock(bytes: &[u8]) -> LockingScript {
    LockingScript::from_binary(bytes).unwrap()
}

/// The key the recipient `root` derives for the remittance from `sender`.
fn derived_key(root: &PrivateKey, sender: &PublicKey) -> PublicKey {
    common::derived_key(root, sender, PREFIX, SUFFIX)
}

/// An AtomicBEEF of a child with one output locked by `lock`, spending a
/// txid-only parent proven by a BUMP the BEEF carries.
fn beef_paying(lock: LockingScript) -> (Vec<u8>, String) {
    let mut parent = Transaction::new();
    let mut parent_input = TransactionInput::new("22".repeat(32), 0);
    parent_input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    parent.inputs.push(parent_input);
    parent.outputs.push(TransactionOutput {
        satoshis: Some(10_000),
        locking_script: LockingScript::from_hex(&format!("76a914{OTHER_HASH}88ac")).unwrap(),
        change: false,
    });
    let parent_txid = parent.id();
    let mut child = Transaction::new();
    child.version = 1;
    let mut input = TransactionInput::new(parent_txid.clone(), 0);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    child.inputs.push(input);
    child.outputs.push(TransactionOutput {
        satoshis: Some(9_500),
        locking_script: lock,
        change: false,
    });
    let txid = child.id();
    let mut beef = Beef::new();
    beef.merge_bump(MerklePath::from_coinbase_txid(&parent_txid, 100_003));
    beef.merge_txid_only(parent_txid);
    beef.merge_raw_tx(child.to_binary(), None);
    (beef.to_binary_atomic(&txid).unwrap(), txid)
}

async fn wallet(root: &PrivateKey, txid: &str) -> Wallet<StorageSqlx, MockWalletServices> {
    common::wallet("internalize-payment-lock-tests", root, txid).await
}

fn args(tx: Vec<u8>, output: InternalizeOutput) -> InternalizeActionArgs {
    InternalizeActionArgs {
        tx,
        outputs: vec![output],
        description: "an output to check".to_string(),
        labels: None,
        seek_permission: None,
    }
}

fn payment(sender: &PublicKey) -> InternalizeOutput {
    InternalizeOutput {
        output_index: 0,
        protocol: "wallet payment".to_string(),
        payment_remittance: Some(WalletPayment {
            derivation_prefix: PREFIX.to_string(),
            derivation_suffix: SUFFIX.to_string(),
            sender_identity_key: sender.to_hex(),
        }),
        insertion_remittance: None,
    }
}

/// Internalizes `output` of a transaction whose one output is locked by the
/// script `script_for` builds from the derived key; returns the answer and
/// the outputs stored before and after.
async fn internalize(
    script_for: impl Fn(&PublicKey) -> Vec<u8>,
    output_for: impl Fn(&PublicKey) -> InternalizeOutput,
) -> (bsv_rs::Result<bool>, i64, i64, PublicKey) {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let derived = derived_key(&root, &sender);
    let (bytes, txid) = beef_paying(lock(&script_for(&derived)));
    let wallet = wallet(&root, &txid).await;
    let before = stored_outputs(&wallet).await;
    let answer = wallet
        .internalize_action(args(bytes, output_for(&sender)), "test.local")
        .await
        .map(|r| r.accepted);
    let after = stored_outputs(&wallet).await;
    (answer, before, after, derived)
}

/// A wallet payment output in each shape that is not the derived key's
/// P2PKH: a P2PK lock of the derived key, an OP_RETURN script, the derived
/// key's P2PKH with one byte more, an empty script. Each is refused with the
/// reference's sentence and nothing is stored.
#[tokio::test]
async fn a_wallet_payment_not_locked_by_the_derived_p2pkh_is_refused_in_every_shape() {
    let shapes: [(&str, ScriptFor); 4] = [
        ("p2pk of the derived key", |k| {
            let mut s = vec![0x21];
            s.extend_from_slice(&k.to_compressed());
            s.push(0xac);
            s
        }),
        ("op_return", |_| {
            vec![0x00, 0x6a, 0x04, 0xde, 0xad, 0xbe, 0xef]
        }),
        ("p2pkh of the derived key and one byte more", |k| {
            let mut s = p2pkh_bytes(k);
            s.push(0x51);
            s
        }),
        ("empty", |_| Vec::new()),
    ];
    for (name, script_for) in shapes {
        let (answer, before, after, _) = internalize(script_for, payment).await;
        match answer {
            Err(bsv_rs::Error::WalletError(text)) => assert_eq!(text, NOT_BRC29, "{name}"),
            other => panic!("{name}: not refused: {other:?}"),
        }
        assert_eq!(after, before, "{name}: nothing is stored");
    }
}

/// A wallet payment locked by the derived key's P2PKH: admitted and stored.
#[tokio::test]
async fn a_wallet_payment_locked_by_the_derived_p2pkh_is_stored() {
    let (answer, before, after, _) = internalize(p2pkh_bytes, payment).await;
    assert!(answer.expect("the payment is internalized"));
    assert_eq!(after, before + 1, "the payment output is stored");
}

/// A 25-byte P2PKH paying another key: refused with the derivation mismatch
/// text naming both hashes.
#[tokio::test]
async fn a_wallet_payment_to_the_p2pkh_of_another_key_names_both_hashes() {
    let (answer, before, after, derived) = internalize(
        |_| hex::decode(format!("76a914{OTHER_HASH}88ac")).unwrap(),
        payment,
    )
    .await;
    let expected = format!(
        "Derivation mismatch for output 0: derived {} != script {OTHER_HASH}",
        hex::encode(hash160(&derived.to_compressed()))
    );
    match answer {
        Err(bsv_rs::Error::WalletError(text)) => assert_eq!(text, expected),
        other => panic!("not refused: {other:?}"),
    }
    assert_eq!(after, before, "nothing is stored");
}

/// A basket insertion of an output that is not a P2PKH: admitted and stored.
#[tokio::test]
async fn a_basket_insertion_of_a_script_that_is_not_p2pkh_is_stored() {
    let (answer, before, after, _) = internalize(
        |k| {
            let mut s = vec![0x21];
            s.extend_from_slice(&k.to_compressed());
            s.push(0xac);
            s
        },
        |_| InternalizeOutput {
            output_index: 0,
            protocol: "basket insertion".to_string(),
            payment_remittance: None,
            insertion_remittance: Some(BasketInsertion {
                basket: "tokens".to_string(),
                custom_instructions: None,
                tags: None,
            }),
        },
    )
    .await;
    assert!(answer.expect("the insertion is internalized"));
    assert_eq!(after, before + 1, "the inserted output is stored");
}
