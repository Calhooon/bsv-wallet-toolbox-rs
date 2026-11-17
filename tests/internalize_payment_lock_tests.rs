//! `Wallet::internalize_action` admits a wallet payment output only when its
//! locking script is the P2PKH of the key its remittance derives, byte for
//! byte, whatever the script's shape (the reference's `internalizeAction`,
//! `setupWalletPaymentForOutput`).
//!
//! A 25-byte P2PKH paying another key is refused with the derivation
//! mismatch text naming both hashes; any other script is refused with
//! "The paymentRemittance parameter must be locked by script conforming to
//! BRC-29". Both carry the class `InvalidParameter`. Nothing is stored for
//! a refused request. A basket insertion with no payment remittance is not
//! held to the rule; one carrying a payment remittance is refused by the
//! argument validator.
//!
//! An output stored by the storage door, which compares no script, sits in
//! the default basket as change; `relinquishOutput` takes it out of change
//! selection.
//!
//! Real SQLite storage, a mock broadcaster and tracker; nothing touches the
//! network.

#![cfg(feature = "sqlite")]

#[path = "common/internalize_payment.rs"]
mod common;

use bsv_rs::primitives::{hash160, PrivateKey, PublicKey};
use bsv_rs::script::LockingScript;
use bsv_rs::wallet::{
    AbortActionArgs, BasketInsertion, CreateActionArgs, CreateActionOptions, CreateActionOutput,
    InternalizeOutput, ListOutputsArgs, Outpoint, RelinquishOutputArgs, WalletInterface,
};
use bsv_rs::WalletRefusalClass;
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::{AuthId, StorageSqlx, Wallet, WalletStorageWriter};

use common::{args, beef_paying, p2pkh_bytes, refusal, stored_outputs, NOT_BRC29, OTHER_HASH};

const PREFIX: &str = "bG9jay1wcmVmaXg=";
const SUFFIX: &str = "bG9jay1zdWZmaXg=";
/// Builds an output's locking script from the derived key.
type ScriptFor = fn(&PublicKey) -> Vec<u8>;

fn lock(bytes: &[u8]) -> LockingScript {
    LockingScript::from_binary(bytes).unwrap()
}

/// The key the recipient `root` derives for the remittance from `sender`.
fn derived_key(root: &PrivateKey, sender: &PublicKey) -> PublicKey {
    common::derived_key(root, sender, PREFIX, SUFFIX)
}

fn p2pk(key: &PublicKey) -> Vec<u8> {
    let mut s = vec![0x21];
    s.extend_from_slice(&key.to_compressed());
    s.push(0xac);
    s
}

fn op_return(_: &PublicKey) -> Vec<u8> {
    vec![0x00, 0x6a, 0x04, 0xde, 0xad, 0xbe, 0xef]
}

fn p2pkh_and_one_byte(key: &PublicKey) -> Vec<u8> {
    let mut s = p2pkh_bytes(key);
    s.push(0x51);
    s
}

async fn wallet(root: &PrivateKey, txid: &str) -> Wallet<StorageSqlx, MockWalletServices> {
    common::wallet("internalize-payment-lock-tests", root, txid).await
}

fn payment(sender: &PublicKey) -> InternalizeOutput {
    common::payment(sender, PREFIX, SUFFIX)
}

fn insertion() -> Option<BasketInsertion> {
    Some(BasketInsertion {
        basket: "tokens".to_string(),
        custom_instructions: None,
        tags: None,
    })
}

/// The answer, the outputs stored before and after, and the derived key.
type Internalized = (bsv_rs::Result<bool>, i64, i64, PublicKey);

/// Internalizes `output` of a transaction whose one output is locked by the
/// script `script_for` builds from the derived key.
async fn internalize(
    script_for: impl Fn(&PublicKey) -> Vec<u8>,
    output_for: impl Fn(&PublicKey) -> InternalizeOutput,
) -> Internalized {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let derived = derived_key(&root, &sender);
    let (bytes, txid) = beef_paying(lock(&script_for(&derived)));
    let wallet = wallet(&root, &txid).await;
    let before = stored_outputs(&wallet).await;
    let answer = wallet
        .internalize_action(args(bytes, output_for(&sender)), Some("test.local"))
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
        ("p2pk of the derived key", p2pk),
        ("op_return", op_return),
        (
            "p2pkh of the derived key and one byte more",
            p2pkh_and_one_byte,
        ),
        ("empty", |_| Vec::new()),
    ];
    for (name, script_for) in shapes {
        let (answer, before, after, _) = internalize(script_for, payment).await;
        let (class, text) = refusal(answer, name);
        assert_eq!(
            (class, text.as_str()),
            (WalletRefusalClass::InvalidParameter, NOT_BRC29),
            "{name}"
        );
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
    let (class, text) = refusal(answer, "another key");
    assert_eq!(class, WalletRefusalClass::InvalidParameter);
    assert_eq!(text, expected);
    assert_eq!(after, before, "nothing is stored");
}

/// A basket insertion of an output that is not a P2PKH: admitted and stored.
#[tokio::test]
async fn a_basket_insertion_of_a_script_that_is_not_p2pkh_is_stored() {
    let (answer, before, after, _) = internalize(p2pk, |_| InternalizeOutput {
        output_index: 0,
        protocol: "basket insertion".to_string(),
        payment_remittance: None,
        insertion_remittance: insertion(),
    })
    .await;
    assert!(answer.expect("the insertion is internalized"));
    assert_eq!(after, before + 1, "the inserted output is stored");
}

/// A basket insertion output that also carries a payment remittance is
/// refused by the argument validator's remittance exclusivity, ahead of the
/// script rule, and nothing is stored.
#[tokio::test]
async fn a_basket_insertion_carrying_a_payment_remittance_is_held_to_the_rule() {
    let (answer, before, after, _) = internalize(op_return, |sender| InternalizeOutput {
        insertion_remittance: insertion(),
        protocol: "basket insertion".to_string(),
        ..payment(sender)
    })
    .await;
    let (class, text) = refusal(answer, "insertion with a payment");
    assert_eq!(class, WalletRefusalClass::InvalidParameter);
    assert_eq!(
        text,
        "Validation error: The output remittance parameter must be only insertionRemittance \
         for basket insertion"
    );
    assert_eq!(after, before, "nothing is stored");
}

fn default_basket() -> ListOutputsArgs {
    ListOutputsArgs {
        basket: "default".to_string(),
        tags: None,
        tag_query_mode: None,
        include: None,
        include_custom_instructions: None,
        include_tags: None,
        include_labels: None,
        limit: None,
        offset: None,
        seek_permission: None,
    }
}

/// A storage `createAction` for one 1000-satoshi output, which needs the
/// wallet's change to fund it, under the rule that offers an unproven
/// receipt.
fn needs_change() -> CreateActionArgs {
    CreateActionArgs {
        description: "a payment funded by change".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script: hex::decode(format!("76a914{OTHER_HASH}88ac")).unwrap(),
            satoshis: 1_000,
            output_description: "a payment".to_string(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: Some(vec!["underReceivedRule".to_string()]),
        options: Some(CreateActionOptions {
            sign_and_process: Some(false),
            no_send: Some(true),
            ..Default::default()
        }),
    }
}

async fn default_outpoints(wallet: &Wallet<StorageSqlx, MockWalletServices>) -> Vec<String> {
    let listed = wallet
        .list_outputs(default_basket(), Some("test.local"))
        .await
        .expect("listOutputs");
    listed
        .outputs
        .iter()
        .map(|o| o.outpoint.to_string())
        .collect()
}

/// A wallet payment output that is not the derived key's P2PKH, put in the
/// store through the storage door (which compares no script), lands in the
/// default basket as change: `listOutputs` on "default" finds it and a
/// storage `createAction` that needs change selects it. `relinquishOutput`
/// on "default" takes it out of the basket, and the same `createAction`
/// no longer selects it.
#[tokio::test]
async fn a_payment_output_not_the_derived_p2pkh_leaves_change_when_relinquished() {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let derived = derived_key(&root, &sender);
    let (bytes, txid) = beef_paying(lock(&p2pkh_and_one_byte(&derived)));
    let wallet = wallet(&root, &txid).await;
    let storage = wallet.storage();
    let identity = root.public_key().to_hex();
    let (user, _) = storage.find_or_insert_user(&identity).await.unwrap();
    let auth = AuthId::with_user_id(&identity, user.user_id);

    let stored = storage
        .internalize_action(&auth, args(bytes, payment(&sender)))
        .await
        .expect("the storage door stores the payment");
    assert!(stored.base.accepted);
    let outpoint = format!("{txid}.0");
    assert_eq!(
        default_outpoints(&wallet).await,
        vec![outpoint.clone()],
        "the default basket holds it"
    );

    // Before: a createAction that needs change selects it.
    let picked = storage
        .create_action(&auth, needs_change())
        .await
        .expect("change covers the action");
    let inputs: Vec<String> = picked
        .inputs
        .iter()
        .map(|i| format!("{}.{}", i.source_txid, i.source_vout))
        .collect();
    assert_eq!(inputs, vec![outpoint.clone()], "selected as change");

    // Release it again, so only the relinquish stands between it and the
    // next pick.
    let reference = picked.reference;
    storage
        .abort_action(&auth, AbortActionArgs { reference })
        .await
        .expect("abortAction");
    let free: (i64, i64) = sqlx::query_as(
        "SELECT spendable, spent_by IS NULL FROM outputs WHERE txid = ? AND vout = 0",
    )
    .bind(&txid)
    .fetch_one(storage.writer())
    .await
    .unwrap();
    assert_eq!(free, (1, 1), "spendable and unspent again");

    let relinquished = wallet
        .relinquish_output(
            RelinquishOutputArgs {
                basket: "default".to_string(),
                output: Outpoint::from_string(&outpoint).unwrap(),
            },
            Some("test.local"),
        )
        .await
        .expect("relinquishOutput");
    assert!(relinquished.relinquished);
    assert!(
        default_outpoints(&wallet).await.is_empty(),
        "the default basket no longer holds it"
    );

    // After: the same createAction has no change to select.
    match storage.create_action(&auth, needs_change()).await {
        Err(e @ bsv_wallet_toolbox_rs::Error::InsufficientFunds { .. }) => {
            assert_eq!(e.to_string(), "Insufficient funds: need 1005, have 0");
        }
        other => panic!(
            "selected change after the relinquish: {:?}",
            other.map(|r| r.inputs.len())
        ),
    }
}
