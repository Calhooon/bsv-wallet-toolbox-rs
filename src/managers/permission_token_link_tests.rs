//! The stored permission token is read from its BEEF entry (GHSA-gvmh-f5gf-5vjj,
//! bsv-rs #749): a token transaction that spends many outputs of one parent is
//! read as any other. bsv-rs 0.9.0-beta.8's linking doors refuse such a BEEF past
//! their default link budget (16 MiB of re-materialized parents); the token's
//! reader takes only its outputs and its id, so it links nothing.

use super::token_source;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::templates::PushDrop;
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{Beef, Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{Outpoint, WalletOutput};

/// A parent of `fan` P2PKH-sized outputs, and a child spending every one of
/// them whose output 0 is a one-satoshi token of `fields` data fields and a
/// signature field: the BEEF of both, the child's txid.
fn fanned_token(fan: u32, fields: usize) -> (Beef, String) {
    let mut parent = Transaction::new();
    let mut funding = TransactionInput::new("11".repeat(32), 0);
    funding.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    parent.inputs_mut().push(funding);
    let p2pkh = LockingScript::from_hex(&format!("76a914{}88ac", "22".repeat(20))).unwrap();
    for _ in 0..fan {
        parent.outputs_mut().push(TransactionOutput {
            satoshis: Some(1_000),
            locking_script: p2pkh.clone(),
            change: false,
        });
    }
    let parent_txid = parent.id();
    let mut child = Transaction::new();
    for vout in 0..fan {
        let mut input = TransactionInput::new(parent_txid.clone(), vout);
        input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
        child.inputs_mut().push(input);
    }
    let key = PrivateKey::from_hex(&format!("{:064x}", 7))
        .unwrap()
        .public_key();
    let mut data: Vec<Vec<u8>> = (b'a'..).take(fields).map(|b| vec![b; 4]).collect();
    data.push(vec![0x30; 8]);
    child.outputs_mut().push(TransactionOutput {
        satoshis: Some(1),
        locking_script: PushDrop::new(key, data).lock(),
        change: false,
    });
    let txid = child.id();
    let mut beef = Beef::new();
    beef.merge_raw_tx(parent.to_binary(), None).unwrap();
    beef.merge_raw_tx(child.to_binary(), None).unwrap();
    (beef, txid)
}

fn output(txid: &str) -> WalletOutput {
    WalletOutput {
        satoshis: 1,
        locking_script: None,
        spendable: true,
        custom_instructions: None,
        tags: None,
        outpoint: Outpoint {
            txid: hex::decode(txid).unwrap().try_into().unwrap(),
            vout: 0,
        },
        labels: None,
    }
}

/// The witness: 800 outputs of one parent re-materialize past the default link
/// budget, so a linking reader refuses this token's BEEF at the pin.
#[test]
fn witness_a_token_past_the_link_budget_is_read_749() {
    let (beef, txid) = fanned_token(800, 2);
    assert!(
        token_source(&beef, &output(&txid), 2).is_some(),
        "the token of a transaction spending 800 outputs of one parent"
    );
}

/// The control: 2 outputs of one parent are within the default link budget, so
/// this passes at either pin and with either reader.
#[test]
fn control_a_token_within_the_link_budget_is_read_749() {
    let (beef, txid) = fanned_token(2, 2);
    assert!(token_source(&beef, &output(&txid), 2).is_some());
    assert!(
        token_source(&beef, &output(&txid), 3).is_none(),
        "the field count still binds"
    );
}
