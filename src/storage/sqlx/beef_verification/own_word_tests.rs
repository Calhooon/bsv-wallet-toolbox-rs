//! The own transaction's word (#450) at the seam: the inputs whose source
//! bytes are at hand judged, the others skipped, the coin's era from its
//! proof height.

use super::*;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::template::{SignOutputs, SigningContext};
use bsv_rs::script::templates::P2PKH;
use bsv_rs::script::{LockingScript, ProtocolEra, Script, UnlockingScript};
use bsv_rs::transaction::{Transaction, TransactionInput, TransactionOutput};

#[cfg(test)]
#[path = "own_word_host_tests.rs"]
mod host;

const GENESIS: u32 = ProtocolEra::MAINNET_GENESIS_HEIGHT;
const CHRONICLE: u32 = ProtocolEra::MAINNET_CHRONICLE_HEIGHT;
const SATOSHIS: u64 = 10_000;

/// Every judgment here is on mainnet at a tip past Chronicle.
fn tip() -> ChainTip {
    crate::services::chain_tip(
        &bsv_rs::block::ChainParams::mainnet(),
        CHRONICLE + 100,
        0,
        0,
        None,
    )
}

/// `child` judged with `parent` as its source, proven at `proven`.
fn judge(child: &Transaction, parent: &Transaction, proven: Option<u32>) -> Result<()> {
    let sources = HashMap::from([(
        parent.id(),
        SourceTx {
            raw: parent.to_binary(),
            proven_height: proven.map(u64::from),
        },
    )]);
    verify_own(&child.to_binary(), &sources, Some(tip()), "main", None)
}

fn source(lock: &[u8]) -> Transaction {
    let mut tx = Transaction::new();
    let mut input = TransactionInput::new(format!("{:064x}", 7), 0);
    input.unlocking_script = Some(UnlockingScript::from_binary(&[0x51]).unwrap());
    tx.inputs.push(input);
    tx.outputs.push(TransactionOutput {
        satoshis: Some(SATOSHIS),
        locking_script: LockingScript::from_binary(lock).unwrap(),
        change: false,
    });
    tx
}

fn spend(parent: &Transaction, vout: u32, unlock: Option<&[u8]>) -> Transaction {
    let mut tx = Transaction::new();
    let mut input = TransactionInput::new(parent.id(), vout);
    input.unlocking_script = unlock.map(|u| UnlockingScript::from_binary(u).unwrap());
    tx.inputs.push(input);
    tx.outputs.push(TransactionOutput {
        satoshis: Some(SATOSHIS - 200),
        locking_script: LockingScript::from_binary(&[0x51]).unwrap(),
        change: false,
    });
    tx
}

/// A P2PKH coin and its spend signed ALL|FORKID, with the sighash byte as
/// `sighash` (0x41 as signed, 0x01 stripped of FORKID).
fn p2pkh_spend(sighash: u8) -> (Transaction, Transaction) {
    let key = PrivateKey::random();
    let hash = bsv_rs::primitives::hash160(&key.public_key().to_compressed());
    let lock = [&[0x76, 0xa9, 0x14][..], &hash[..], &[0x88, 0xac][..]].concat();
    let parent = source(&lock);
    let unsigned = spend(&parent, 0, None);
    let mut script = P2PKH::unlock(&key, SignOutputs::All, false)
        .sign(&SigningContext::new(
            &unsigned.to_binary(),
            0,
            SATOSHIS,
            &Script::from_binary(&lock).unwrap(),
        ))
        .unwrap()
        .to_binary();
    let at = usize::from(script[0]);
    assert_eq!(script[at], 0x41);
    script[at] = sighash;
    let child = spend(&parent, 0, Some(&script));
    (parent, child)
}

#[test]
fn a_fork_id_less_signature_is_refused_at_its_input() {
    let (parent, child) = p2pkh_spend(0x01);
    match judge(&child, &parent, Some(GENESIS)).unwrap_err() {
        Error::SpendRefused {
            offset,
            txid,
            input,
            why: SpendRefusal::Script(_),
        } => {
            // version (4) and the input count (1): the input's place in the raw transaction.
            assert_eq!(offset, 5);
            assert_eq!(txid, child.id());
            assert_eq!(input, Some(0));
        }
        other => panic!("unexpected: {other}"),
    }
}

#[test]
fn a_forkid_signature_is_accepted() {
    let (parent, child) = p2pkh_spend(0x41);
    for proven in [None, Some(GENESIS)] {
        judge(&child, &parent, proven).unwrap();
    }
}

#[test]
fn an_input_without_source_bytes_is_skipped() {
    let (_, child) = p2pkh_spend(0x01);
    verify_own(
        &child.to_binary(),
        &HashMap::new(),
        Some(tip()),
        "main",
        None,
    )
    .unwrap();
}

#[test]
fn a_source_without_the_output_is_refused() {
    let parent = source(&[0x51]);
    let child = spend(&parent, 1, Some(&[0x51]));
    assert!(matches!(
        judge(&child, &parent, None),
        Err(Error::SpendRefused {
            input: Some(0),
            why: SpendRefusal::OutputNotAvailable { vout: 1 },
            ..
        })
    ));
}

/// `OP_2MUL` is enabled by the coin's era: a coin mined before Chronicle is
/// refused it, an unproven coin takes tip+1's era.
#[test]
fn the_coin_era_comes_from_its_proof_height() {
    let parent = source(&[0x8d, 0x52, 0x87]);
    let child = spend(&parent, 0, Some(&[0x51]));
    judge(&child, &parent, None).unwrap();
    judge(&child, &parent, Some(CHRONICLE)).unwrap();
    assert!(matches!(
        judge(&child, &parent, Some(GENESIS)),
        Err(Error::SpendRefused {
            input: Some(0),
            why: SpendRefusal::Script(_),
            ..
        })
    ));
}

/// The interim era policy places no coin mined before Genesis: under the
/// keeper's reading of #461 the signer skips the input, the node's verdict
/// at broadcast the backstop, where the doors refuse it; both until bsv-rs
/// #440 names the pre-Genesis coin. The fork-ID-less signature passes here
/// for that reason alone.
#[test]
fn a_coin_mined_before_genesis_is_left_to_the_node() {
    let (parent, child) = p2pkh_spend(0x01);
    judge(&child, &parent, Some(GENESIS - 1)).unwrap();
}

/// A source is read for its output alone: a mined coinbase's bytes, without
/// their BUMP, still supply the coin.
#[test]
fn a_coinbase_source_supplies_its_output() {
    let mut parent = source(&[0x8d, 0x52, 0x87]);
    parent.inputs[0] = TransactionInput::new(format!("{:064x}", 0), u32::MAX);
    parent.inputs[0].unlocking_script = Some(UnlockingScript::from_binary(&[0x51]).unwrap());
    let refused = spend(&parent, 0, Some(&[0x52]));
    assert!(matches!(
        judge(&refused, &parent, Some(CHRONICLE)),
        Err(Error::SpendRefused { input: Some(0), .. })
    ));
    let accepted = spend(&parent, 0, Some(&[0x51]));
    judge(&accepted, &parent, Some(CHRONICLE)).unwrap();
}
